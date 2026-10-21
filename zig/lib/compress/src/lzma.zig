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

//! Streaming LZMA/LZMA2 and lzip containers, using native probability/range
//! primitives with immediate output and a dynamically grown bounded ring.
const std = @import("std");
const A = std.mem.Allocator;
const Reader = std.Io.Reader;
const Writer = std.Io.Writer;
const lzma = std.compress.lzma;
pub const Check = enum(u8) { none = 0, crc32 = 1, crc64 = 4, sha256 = 10 };
pub const Stats = struct { compressed: u64, uncompressed: u64, digest: [32]u8 };
const Crc64 = @import("antfly_hash").Crc64Xz;
pub const Bridge = struct {
    allocating: Writer.Allocating,
    target: *Writer,
    check: Check,
    crc32: @import("antfly_hash").Crc32 = .init(),
    crc64: Crc64 = .init(),
    sha: std.crypto.hash.sha2.Sha256 = .init(.{}),
    fn send(self: *Bridge, bytes: []const u8) Writer.Error!void {
        try self.target.writeAll(bytes);
        switch (self.check) {
            .none => {},
            .crc32 => self.crc32.update(bytes),
            .crc64 => self.crc64.update(bytes),
            .sha256 => self.sha.update(bytes),
        }
    }
    fn drain(writer: *Writer, data: []const []const u8, splat: usize) Writer.Error!usize {
        const allocating: *Writer.Allocating = @alignCast(@fieldParentPtr("writer", writer));
        const self: *Bridge = @alignCast(@fieldParentPtr("allocating", allocating));
        try self.send(writer.buffer[0..writer.end]);
        writer.end = 0;
        var n: usize = 0;
        for (data[0 .. data.len - 1]) |bytes| {
            try self.send(bytes);
            n += bytes.len;
        }
        for (0..splat) |_| {
            const bytes = data[data.len - 1];
            try self.send(bytes);
            n += bytes.len;
        }
        return n;
    }
    pub fn digest(self: *Bridge) [32]u8 {
        var result: [32]u8 = @splat(0);
        switch (self.check) {
            .none => {},
            .crc32 => std.mem.writeInt(u32, result[0..4], self.crc32.final(), .little),
            .crc64 => std.mem.writeInt(u64, result[0..8], self.crc64.final(), .little),
            .sha256 => self.sha.final(&result),
        }
        return result;
    }
};
const Ring = struct {
    data: std.ArrayList(u8) = .empty,
    window: usize,
    limit: usize,
    len: usize = 0,
    total: usize = 0,
    pub fn appendLiteral(self: *Ring, a: A, byte: u8, writer: *Writer) !void {
        if (self.total == self.limit) return error.StreamTooLong;
        if (self.data.items.len < self.window and self.len == self.data.items.len) {
            const size = @min(self.window, @max(65536, self.data.items.len * 2));
            try self.data.resize(a, size);
        }
        try writer.writeByte(byte);
        self.data.items[self.len % self.window] = byte;
        self.len += 1;
        self.total += 1;
    }
    pub fn appendLz(self: *Ring, a: A, count: usize, distance: usize, writer: *Writer) !void {
        if (distance == 0 or distance > @min(self.len, self.window)) return error.CorruptInput;
        if (count > self.limit - self.total) return error.StreamTooLong;
        const needed = @min(self.window, self.len +| count);
        if (needed > self.data.items.len) try self.data.resize(a, @min(self.window, @max(needed, @max(65536, self.data.items.len * 2))));
        var remaining = count;
        while (remaining > 0) {
            const destination = self.len % self.window;
            const source = (self.len - distance) % self.window;
            // A chunk never reads not-yet-produced history, including overlapping
            // runs. Flush contiguous spans rather than dispatching per byte.
            const n = @min(remaining, @min(distance, @min(self.window - source, self.window - destination)));
            const bytes = self.data.items[source..][0..n];
            try writer.writeAll(bytes);
            if (destination < source) std.mem.copyForwards(u8, self.data.items[destination..][0..n], bytes) else if (destination > source) std.mem.copyBackwards(u8, self.data.items[destination..][0..n], bytes);
            self.len += n;
            self.total += n;
            remaining -= n;
        }
    }
    pub fn lastOr(self: Ring, fallback: u8) u8 {
        return if (self.len == 0) fallback else self.data.items[(self.len - 1) % self.window];
    }
    pub fn lastN(self: Ring, distance: usize) !u8 {
        if (distance == 0 or distance > @min(self.len, self.window)) return error.CorruptInput;
        return self.data.items[(self.len - distance) % self.window];
    }
};
pub fn bridge(a: A, writer: *Writer, check: Check, buffer: []u8) Bridge {
    return .{ .target = writer, .check = check, .allocating = .{ .allocator = a, .alignment = .of(u8), .writer = .{ .vtable = &.{ .drain = Bridge.drain }, .buffer = buffer } } };
}
fn decodeRange(a: A, input: *Reader, output: *Writer, limit: usize, window_limit: usize, params: lzma.Decode.Params, check: Check) !Stats {
    if (params.dict_size > window_limit) return error.WindowOversize;
    var history = Ring{ .window = @max(4096, params.dict_size), .limit = limit };
    defer history.data.deinit(a);
    var decoder = try lzma.Decode.init(a, params.properties);
    defer decoder.deinit(a);
    var buffer: [65536]u8 = undefined;
    var sink = bridge(a, output, check, &buffer);
    var consumed: u64 = 0;
    var range = try lzma.RangeDecoder.initCounting(input, &consumed);
    while (true) {
        if (params.unpacked_size) |size| {
            if (history.total >= size) break;
        } else if (range.isFinished()) break;
        switch (try decoder.process(input, &sink.allocating, &history, &range, &consumed)) {
            .more => {},
            .finished => break,
        }
    }
    if (params.unpacked_size) |size| if (history.total != size) return error.DecompressedSizeMismatch;
    try sink.allocating.writer.flush();
    return .{ .compressed = consumed, .uncompressed = history.total, .digest = sink.digest() };
}
pub fn standaloneTo(a: A, input: *Reader, output: *Writer, limit: usize, window_limit: usize) !void {
    var produced: usize = 0;
    while (try more(input)) {
        const params = try lzma.Decode.Params.readHeader(input, .{});
        const result = try decodeRange(a, input, output, limit - produced, window_limit, params, .none);
        produced += @intCast(result.uncompressed);
    }
}
pub fn lzipTo(a: A, input: *Reader, output: *Writer, limit: usize, window_limit: usize) !void {
    var produced: usize = 0;
    while (try more(input)) {
        const header = try input.takeArray(6);
        if (!std.mem.eql(u8, header[0..4], "LZIP") or header[4] != 1) return error.InvalidLzip;
        const exponent = header[5] & 31;
        if (exponent < 12 or exponent > 29) return error.InvalidLzip;
        const size: u32 = @as(u32, 1) << @intCast(exponent);
        const dictionary = size - (size / 16) * @as(u32, header[5] >> 5);
        const result = try decodeRange(a, input, output, limit - produced, window_limit, .{ .dict_size = dictionary, .properties = .{ .lc = 3, .lp = 0, .pb = 2 }, .unpacked_size = null }, .crc32);
        const trailer = try input.takeArray(20);
        if (!std.mem.eql(u8, result.digest[0..4], trailer[0..4]) or std.mem.readInt(u64, trailer[4..12], .little) != result.uncompressed or std.mem.readInt(u64, trailer[12..20], .little) != result.compressed + 26) return error.InvalidLzip;
        produced += @intCast(result.uncompressed);
    }
}
pub fn lzma2To(a: A, input: *Reader, output: *Writer, limit: usize, window: usize, check: Check) !Stats {
    if (window < 4096) return error.WindowOversize;
    var history = Ring{ .window = window, .limit = limit };
    defer history.data.deinit(a);
    var decoder = try lzma.Decode.init(a, .{ .lc = 0, .lp = 0, .pb = 0 });
    defer decoder.deinit(a);
    var buffer: [65536]u8 = undefined;
    var sink = bridge(a, output, check, &buffer);
    var dict_ready = false;
    var props_ready = false;
    var state_ready = false;
    var consumed: u64 = 0;
    while (true) {
        const status = try input.takeByte();
        consumed += 1;
        if (status == 0) break;
        if (status == 1 or status == 2) {
            const count = @as(u32, try input.takeInt(u16, .big)) + 1;
            consumed += 2;
            if (status == 1) {
                history.len = 0;
                dict_ready = true;
            }
            if (!dict_ready) return error.CorruptInput;
            state_ready = false;
            for (0..count) |_| try history.appendLiteral(a, try input.takeByte(), &sink.allocating.writer);
            consumed += count;
            continue;
        }
        if (status < 0x80) return error.CorruptInput;
        const reset = (status >> 5) & 3;
        const unpacked = (@as(u32, status & 31) << 16) + @as(u32, try input.takeInt(u16, .big)) + 1;
        const packed_size = @as(u32, try input.takeInt(u16, .big)) + 1;
        consumed += 4;
        if (reset == 3) {
            history.len = 0;
            dict_ready = true;
        }
        if (!dict_ready) return error.CorruptInput;
        if (reset > 0) {
            var properties = decoder.properties;
            if (reset >= 2) {
                var raw = try input.takeByte();
                consumed += 1;
                if (raw >= 225) return error.CorruptInput;
                const lc: u4 = @intCast(raw % 9);
                raw /= 9;
                const lp: u3 = @intCast(raw % 5);
                raw /= 5;
                const pb: u3 = @intCast(raw);
                if (@as(u8, lc) + lp > 4) return error.CorruptInput;
                properties = .{ .lc = lc, .lp = lp, .pb = pb };
                props_ready = true;
            }
            if (!props_ready) return error.CorruptInput;
            try decoder.resetState(a, properties);
            state_ready = true;
        }
        if (!state_ready or !props_ready) return error.CorruptInput;
        var packed_read: u64 = 0;
        var range = try lzma.RangeDecoder.initCounting(input, &packed_read);
        const end = history.len + unpacked;
        while (history.len < end) {
            const result = try decoder.process(input, &sink.allocating, &history, &range, &packed_read);
            if (result == .finished) break;
        }
        if (history.len != end or packed_read != packed_size or !range.isFinished()) return error.CorruptInput;
        consumed += packed_read;
    }
    try sink.allocating.writer.flush();
    return .{ .compressed = consumed, .uncompressed = history.total, .digest = sink.digest() };
}
pub fn more(input: *Reader) !bool {
    _ = input.peekByte() catch |err| switch (err) {
        error.EndOfStream => return false,
        else => return err,
    };
    return true;
}
