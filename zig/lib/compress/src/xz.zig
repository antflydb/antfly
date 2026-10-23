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

//! Streaming XZ with bounded LZMA2 history and complete block/index/footer checks.
const std = @import("std");
const lzma = @import("lzma.zig");
const Bcj = @import("bcj.zig").Filter;
const A = std.mem.Allocator;
const Reader = std.Io.Reader;
const Writer = std.Io.Writer;
const Sha256 = std.crypto.hash.sha2.Sha256;
pub fn decodeTo(a: A, input: *Reader, output: *Writer, limit: usize, window_limit: usize) !void {
    var produced: usize = 0;
    while (try lzma.more(input)) {
        const header = try input.takeArray(12);
        if (!std.mem.eql(u8, header[0..6], "\xfd7zXZ\x00") or header[6] != 0 or header[7] & 0xf0 != 0) return error.InvalidXz;
        if (@import("antfly_hash").Crc32.hash(header[6..8]) != std.mem.readInt(u32, header[8..12], .little)) return error.WrongXzChecksum;
        const flags = header[0..12].*;
        const check: lzma.Check = switch (flags[7]) {
            0 => .none,
            1 => .crc32,
            4 => .crc64,
            10 => .sha256,
            else => return error.UnsupportedXzCheck,
        };
        const check_size: usize = switch (check) {
            .none => 0,
            .crc32 => 4,
            .crc64 => 8,
            .sha256 => 32,
        };
        var blocks: u64 = 0;
        var actual_records: Sha256 = .init(.{});
        while (try input.peekByte() != 0) {
            const header_size = (@as(usize, try input.peekByte()) + 1) * 4;
            const encoded = try input.take(header_size);
            if (@import("antfly_hash").Crc32.hash(encoded[0 .. header_size - 4]) != std.mem.readInt(u32, encoded[header_size - 4 ..][0..4], .little)) return error.WrongXzChecksum;
            var block_header = Reader.fixed(encoded[0 .. header_size - 4]);
            _ = try block_header.takeByte();
            const block_flags = try block_header.takeByte();
            if (block_flags & 0x3c != 0) return error.InvalidXz;
            const filter_count: usize = (block_flags & 3) + 1;
            const declared_packed: ?u64 = if (block_flags & 0x40 != 0) try vli(&block_header, null, null) else null;
            const unpacked: ?u64 = if (block_flags & 0x80 != 0) try vli(&block_header, null, null) else null;
            var descriptors: [3]struct { id: u8, property: u32 } = undefined;
            for (0..filter_count - 1) |i| {
                const id = try vli(&block_header, null, null);
                const property_size = try vli(&block_header, null, null);
                if (id == 3) {
                    if (property_size != 1) return error.InvalidXz;
                    descriptors[i] = .{ .id = 3, .property = try block_header.takeByte() };
                } else {
                    const alignment = Bcj.alignment(id) orelse return error.UnsupportedXzFilter;
                    const start = switch (property_size) {
                        0 => 0,
                        4 => try block_header.takeInt(u32, .little),
                        else => return error.InvalidXz,
                    };
                    if (start % alignment != 0) return error.InvalidXz;
                    descriptors[i] = .{ .id = @intCast(id), .property = start };
                }
            }
            if (try vli(&block_header, null, null) != 0x21 or try vli(&block_header, null, null) != 1) return error.UnsupportedXzFilter;
            const property = try block_header.takeByte();
            if (property > 40) return error.InvalidXz;
            const window: u64 = if (property == 40) 0xffffffff else (@as(u64, 2) | (property & 1)) << @intCast((property >> 1) + 11);
            if (window > window_limit) return error.WindowOversize;
            for (block_header.buffered()) |byte| if (byte != 0) return error.InvalidXz;
            var result: lzma.Stats = undefined;
            if (filter_count == 1) {
                result = try lzma.lzma2To(a, input, output, limit - produced, @intCast(window), check);
            } else {
                // Checks cover original bytes after every inverse filter. Each
                // stage retains only its history and bounded output buffer.
                var checksum_buffer: [4096]u8 = undefined;
                var checksum = lzma.bridge(a, output, check, &checksum_buffer);
                var filters: [3]union(enum) { delta: Delta, bcj: Bcj } = undefined;
                var sink = &checksum.allocating.writer;
                for (0..filter_count - 1) |i| {
                    const descriptor = descriptors[i];
                    if (descriptor.id == 3) {
                        filters[i] = .{ .delta = .{ .target = sink, .distance = @intCast(descriptor.property), .writer = .{ .vtable = &.{ .drain = Delta.drain }, .buffer = &.{} } } };
                        sink = &filters[i].delta.writer;
                    } else {
                        filters[i] = .{ .bcj = .{ .target = sink, .id = descriptor.id, .position = descriptor.property } };
                        sink = &filters[i].bcj.writer;
                    }
                }
                result = try lzma.lzma2To(a, input, sink, limit - produced, @intCast(window), .none);
                var remaining = filter_count - 1;
                while (remaining > 0) {
                    remaining -= 1;
                    switch (filters[remaining]) {
                        .delta => {},
                        .bcj => |*filter| try filter.finish(),
                    }
                }
                try checksum.allocating.writer.flush();
                result.digest = checksum.digest();
            }
            if (declared_packed) |size| if (size != result.compressed) return error.InvalidXz;
            if (unpacked) |size| if (size != result.uncompressed) return error.InvalidXz;
            const padding: usize = @intCast((4 - result.compressed % 4) % 4);
            for (try input.take(padding)) |byte| if (byte != 0) return error.InvalidXz;
            if (!std.mem.eql(u8, try input.take(check_size), result.digest[0..check_size])) return error.WrongXzChecksum;
            record(&actual_records, header_size + result.compressed + check_size, result.uncompressed);
            blocks += 1;
            produced += @intCast(result.uncompressed);
        }
        var index_crc: @import("antfly_hash").Crc32 = .init();
        var index_size: u64 = 1;
        const marker = try input.takeByte();
        index_crc.update(&.{marker});
        const count = try vli(input, &index_crc, &index_size);
        if (count != blocks) return error.InvalidXzIndex;
        var index_records: Sha256 = .init(.{});
        for (0..@as(usize, @intCast(count))) |_| {
            const unpadded = try vli(input, &index_crc, &index_size);
            const unpacked = try vli(input, &index_crc, &index_size);
            record(&index_records, unpadded, unpacked);
        }
        var actual: [32]u8 = undefined;
        var expected: [32]u8 = undefined;
        actual_records.final(&actual);
        index_records.final(&expected);
        if (!std.mem.eql(u8, &actual, &expected)) return error.InvalidXzIndex;
        while (index_size % 4 != 0) {
            const byte = try input.takeByte();
            if (byte != 0) return error.InvalidXzIndex;
            index_crc.update(&.{byte});
            index_size += 1;
        }
        if (index_crc.final() != try input.takeInt(u32, .little)) return error.WrongXzChecksum;
        index_size += 4;
        const footer = try input.takeArray(12);
        if (!std.mem.eql(u8, footer[10..12], "YZ") or !std.mem.eql(u8, footer[8..10], flags[6..8])) return error.InvalidXzFooter;
        if (@import("antfly_hash").Crc32.hash(footer[4..10]) != std.mem.readInt(u32, footer[0..4], .little)) return error.WrongXzChecksum;
        if (@as(u64, std.mem.readInt(u32, footer[4..8], .little)) + 1 != index_size / 4) return error.InvalidXzFooter;
        var padding: usize = 0;
        while (try lzma.more(input)) {
            if (try input.peekByte() != 0) break;
            input.toss(1);
            padding += 1;
        }
        if (padding % 4 != 0) return error.InvalidXzPadding;
    }
}
fn record(hash: *Sha256, unpadded: u64, unpacked: u64) void {
    var bytes: [16]u8 = undefined;
    std.mem.writeInt(u64, bytes[0..8], unpadded, .little);
    std.mem.writeInt(u64, bytes[8..16], unpacked, .little);
    hash.update(&bytes);
}
fn vli(input: *Reader, checksum: ?*@import("antfly_hash").Crc32, count: ?*u64) !u64 {
    var result: u64 = 0;
    for (0..9) |i| {
        const byte = try input.takeByte();
        if (checksum) |crc| crc.update(&.{byte});
        if (count) |n| n.* += 1;
        if (byte == 0 and i > 0) return error.InvalidXzInteger;
        result |= @as(u64, byte & 127) << @intCast(i * 7);
        if (byte & 128 == 0) return result;
    }
    return error.InvalidXzInteger;
}

/// XZ format section 5.3.3. Histories reset independently for every block.
const Delta = struct {
    target: *Writer,
    writer: Writer,
    distance: u8,
    position: u8 = 0,
    history: [256]u8 = @splat(0),
    fn send(self: *Delta, bytes: []const u8) Writer.Error!void {
        var buffer: [4096]u8 = undefined;
        var at: usize = 0;
        while (at < bytes.len) {
            const n = @min(buffer.len, bytes.len - at);
            for (bytes[at..][0..n], buffer[0..n]) |byte, *out| {
                const decoded = byte +% self.history[self.position -% (self.distance +% 1)];
                self.history[self.position] = decoded;
                self.position +%= 1;
                out.* = decoded;
            }
            try self.target.writeAll(buffer[0..n]);
            at += n;
        }
    }
    fn drain(writer: *Writer, data: []const []const u8, splat: usize) Writer.Error!usize {
        const self: *Delta = @alignCast(@fieldParentPtr("writer", writer));
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
};
