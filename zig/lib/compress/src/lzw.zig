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

//! Unix compress (.Z) LZW stream decoder, LSB codes and width-group alignment.
const std = @import("std");
pub fn decode(a: std.mem.Allocator, data: []const u8, limit: usize) ![]u8 {
    var input = std.Io.Reader.fixed(data);
    var output: std.Io.Writer.Allocating = .init(a);
    errdefer output.deinit();
    decodeTo(a, &input, &output.writer, limit) catch |err| return if (err == error.WriteFailed) error.OutOfMemory else err;
    return output.toOwnedSlice();
}
pub fn decodeTo(a: std.mem.Allocator, input: *std.Io.Reader, output: *std.Io.Writer, limit: usize) !void {
    const header = input.takeArray(3) catch return error.InvalidCompressStream;
    if (header[0] != 0x1f or header[1] != 0x9d or header[2] & 0x60 != 0) return error.InvalidCompressStream;
    const max_bits = header[2] & 31;
    if (max_bits < 9 or max_bits > 16) return error.InvalidCompressStream;
    const block = header[2] & 128 != 0;
    const capacity: usize = @as(usize, 1) << @intCast(max_bits);
    const prefix = try a.alloc(u16, capacity);
    defer a.free(prefix);
    const suffix = try a.alloc(u8, capacity);
    defer a.free(suffix);
    const stack = try a.alloc(u8, capacity);
    defer a.free(stack);
    var produced: usize = 0;
    var bits = Bits{ .input = input };
    var width: usize = 9;
    var free: usize = if (block) 257 else 256;
    var bit: usize = 24;
    var group: usize = bit;
    var old: ?usize = null;
    var first: u8 = 0;
    while (true) {
        if (free > (@as(usize, 1) << @intCast(width)) - 1 and width < max_bits) {
            const size = width * 8;
            bit = group + (bit - group + size - 1) / size * size;
            if (!try bits.skipTo(bit)) break;
            group = bit;
            width += 1;
        }
        var code = try bits.read(width) orelse break;
        bit += width;
        if (block and code == 256) {
            const size = width * 8;
            bit = group + (bit - group + size - 1) / size * size;
            if (!try bits.skipTo(bit)) break;
            group = bit;
            width = 9;
            free = 257;
            old = null;
            continue;
        }
        if (old == null) {
            if (code > 255) return error.InvalidCompressStream;
            if (produced == limit) return error.StreamTooLong;
            first = @intCast(code);
            try output.writeByte(first);
            produced += 1;
            old = code;
            continue;
        }
        const original = code;
        var n: usize = 0;
        if (code == free) {
            stack[n] = first;
            n += 1;
            code = old.?;
        } else if (code > free) return error.InvalidCompressStream;
        while (code >= 256) {
            if (code >= free or n >= capacity) return error.InvalidCompressStream;
            stack[n] = suffix[code];
            n += 1;
            code = prefix[code];
        }
        first = @intCast(code);
        if (n == capacity) return error.InvalidCompressStream;
        stack[n] = first;
        n += 1;
        if (n > limit - produced) return error.StreamTooLong;
        produced += n;
        while (n > 0) {
            n -= 1;
            try output.writeByte(stack[n]);
        }
        if (free < capacity) {
            prefix[free] = @intCast(old.?);
            suffix[free] = first;
            free += 1;
        }
        old = original;
    }
}

const Bits = struct {
    input: *std.Io.Reader,
    byte: u8 = 0,
    left: u4 = 0,
    position: usize = 24,
    fn read(self: *Bits, count: usize) !?usize {
        var value: usize = 0;
        for (0..count) |i| {
            if (self.left == 0) {
                self.byte = self.input.takeByte() catch |err| switch (err) {
                    error.EndOfStream => return null,
                    else => return err,
                };
                self.left = 8;
            }
            value |= @as(usize, self.byte & 1) << @intCast(i);
            self.byte >>= 1;
            self.left -= 1;
            self.position += 1;
        }
        return value;
    }
    fn skipTo(self: *Bits, target: usize) !bool {
        while (self.position < target) _ = try self.read(1) orelse return false;
        return true;
    }
};
