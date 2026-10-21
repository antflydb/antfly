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

//! LZ4 frame/block decoding, including linked blocks and checksums.
const std = @import("std");
const A = std.mem.Allocator;
const E = error{ InvalidLz4, UnsupportedDictionary, DictionaryIdMismatch, StreamTooLong } || A.Error || std.Io.Reader.Error || std.Io.Writer.Error;
const Input = struct {
    bytes: []const u8,
    pos: usize = 0,
    fn take(self: *Input, n: usize) E![]const u8 {
        if (n > self.bytes.len - self.pos) return error.InvalidLz4;
        defer self.pos += n;
        return self.bytes[self.pos..][0..n];
    }
    fn u32le(self: *Input) E!u32 {
        return std.mem.readInt(u32, (try self.take(4))[0..4], .little);
    }
};
pub const Options = struct { dictionary: []const u8 = &.{}, dictionary_id: ?u32 = null };
pub fn decode(a: A, data: []const u8, limit: usize) E![]u8 {
    return decodeOptions(a, data, limit, .{});
}
pub fn decodeOptions(a: A, data: []const u8, limit: usize, options: Options) E![]u8 {
    var input = std.Io.Reader.fixed(data);
    var output: std.Io.Writer.Allocating = .init(a);
    errdefer output.deinit();
    decodeToOptions(a, &input, &output.writer, limit, options) catch |err| return if (err == error.WriteFailed) error.OutOfMemory else err;
    return output.toOwnedSlice();
}
/// Retain one bounded frame block plus the last 64 KiB of decoded history.
pub fn decodeToOptions(a: A, input: *std.Io.Reader, output: *std.Io.Writer, limit: usize, options: Options) E!void {
    var block: std.ArrayList(u8) = .empty;
    defer block.deinit(a);
    var compressed: std.ArrayList(u8) = .empty;
    defer compressed.deinit(a);
    var history: [65536]u8 = undefined;
    var history_len: usize = 0;
    var produced: usize = 0;
    while (try more(input)) {
        const magic = try input.takeInt(u32, .little);
        if (magic & 0xfffffff0 == 0x184d2a50) {
            const size = try input.takeInt(u32, .little);
            try input.discardAll(size);
            continue;
        }
        const legacy = magic == 0x184c2102;
        if (!legacy and magic != 0x184d2204) return error.InvalidLz4;
        var flags: u8 = 32;
        var expected: ?u64 = null;
        var max_block: usize = 8 * 1024 * 1024;
        const dictionary = options.dictionary[options.dictionary.len - @min(65536, options.dictionary.len) ..];
        if (!legacy) {
            var header: [14]u8 = undefined;
            flags = try input.takeByte();
            const descriptor = try input.takeByte();
            header[0] = flags;
            header[1] = descriptor;
            var header_len: usize = 2;
            if (flags >> 6 != 1 or flags & 2 != 0 or descriptor & 0x8f != 0) return error.InvalidLz4;
            const block_log = (descriptor >> 4) & 7;
            if (block_log < 4 or block_log > 7) return error.InvalidLz4;
            max_block = @as(usize, 1) << @intCast(8 + 2 * block_log);
            if (flags & 8 != 0) {
                const size = try input.takeArray(8);
                @memcpy(header[header_len..][0..8], size);
                header_len += 8;
                expected = std.mem.readInt(u64, size, .little);
            }
            if (flags & 1 != 0) {
                const id = try input.takeArray(4);
                @memcpy(header[header_len..][0..4], id);
                header_len += 4;
                if (options.dictionary.len == 0) return error.UnsupportedDictionary;
                if (options.dictionary_id) |requested| if (requested != std.mem.readInt(u32, id, .little)) return error.DictionaryIdMismatch;
            }
            const hc: u8 = @truncate(std.hash.XxHash32.hash(0, header[0..header_len]) >> 8);
            if (try input.takeByte() != hc) return error.InvalidLz4;
        }
        @memcpy(history[0..dictionary.len], dictionary);
        history_len = dictionary.len;
        const frame_start = produced;
        var checksum: std.hash.XxHash32 = .init(0);
        while (true) {
            if (legacy) {
                if (!try more(input)) break;
                const next_magic = std.mem.readInt(u32, try input.peekArray(4), .little);
                if (next_magic == 0x184c2102 or next_magic == 0x184d2204 or next_magic & 0xfffffff0 == 0x184d2a50) break;
            }
            const size = try input.takeInt(u32, .little);
            if (!legacy and size == 0) break;
            const len = if (legacy) size else size & 0x7fffffff;
            const compressed_limit = if (legacy) max_block + max_block / 255 + 16 else max_block;
            if (len > compressed_limit) return error.InvalidLz4;
            try compressed.resize(a, len);
            try input.readSliceAll(compressed.items);
            if (flags & 16 != 0 and !legacy and try input.takeInt(u32, .little) != std.hash.XxHash32.hash(0, compressed.items)) return error.InvalidLz4;
            block.clearRetainingCapacity();
            const prefix = if (flags & 32 != 0) dictionary else history[0..history_len];
            try block.appendSlice(a, prefix);
            const block_limit = @min(max_block, limit - produced);
            if (!legacy and size & 0x80000000 != 0) {
                if (compressed.items.len > block_limit) return error.StreamTooLong;
                try block.appendSlice(a, compressed.items);
            } else try decodeBlock(a, compressed.items, &block, 0, block_limit + prefix.len);
            const decoded = block.items[prefix.len..];
            try output.writeAll(decoded);
            checksum.update(decoded);
            produced += decoded.len;
            if (flags & 32 == 0) {
                history_len = @min(history.len, block.items.len);
                @memcpy(history[0..history_len], block.items[block.items.len - history_len ..]);
            }
        }
        if (expected) |n| if (n != produced - frame_start) return error.InvalidLz4;
        if (!legacy and flags & 4 != 0 and try input.takeInt(u32, .little) != checksum.final()) return error.InvalidLz4;
    }
}
fn more(input: *std.Io.Reader) E!bool {
    _ = input.peekByte() catch |err| switch (err) {
        error.EndOfStream => return false,
        else => return err,
    };
    return true;
}
fn length(input: *Input, initial: usize) E!usize {
    var n = initial;
    if (initial == 15) while (true) {
        const extra = (try input.take(1))[0];
        n = std.math.add(usize, n, extra) catch return error.InvalidLz4;
        if (extra != 255) break;
    };
    return n;
}
fn decodeBlock(a: A, data: []const u8, out: *std.ArrayList(u8), start: usize, limit: usize) E!void {
    var input = Input{ .bytes = data };
    while (input.pos < data.len) {
        const token = (try input.take(1))[0];
        const literals = try input.take(try length(&input, token >> 4));
        if (literals.len > limit - out.items.len) return error.StreamTooLong;
        try out.appendSlice(a, literals);
        if (input.pos == data.len) break;
        const distance = std.mem.readInt(u16, (try input.take(2))[0..2], .little);
        if (distance == 0 or distance > out.items.len - start) return error.InvalidLz4;
        const n = (try length(&input, token & 15)) + 4;
        if (n > limit - out.items.len) return error.StreamTooLong;
        try out.ensureUnusedCapacity(a, n);
        for (0..n) |_| out.appendAssumeCapacity(out.items[out.items.len - distance]);
    }
}
