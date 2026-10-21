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

//! Record-wise transcoding adapter: UTF code units stay aligned and source
//! records are emitted as soon as complete, without waiting for pipe EOF.
const std = @import("std");
const encoding = @import("antfly_encoding");
pub const Reader = struct {
    a: std.mem.Allocator,
    input: *std.Io.Reader,
    reader: std.Io.Reader,
    label: []const u8,
    width: usize = 1,
    big_endian: bool = false,
    first: bool = true,
    iso: bool = false,
    iso_prefix: ?[3]u8 = null,
    decoded: []u8 = &.{},
    position: usize = 0,
    max_bytes: usize,
    consumed: usize = 0,
    err: ?anyerror = null,
    pub fn init(a: std.mem.Allocator, input: *std.Io.Reader, label: []const u8, buffer: []u8, max_bytes: usize) !Reader {
        var effective = label;
        if (!std.mem.eql(u8, label, "none")) {
            const first = input.peekByte() catch |err| switch (err) {
                error.EndOfStream => 0,
                else => return err,
            };
            if (first == 0xff or first == 0xfe or first == 0 or first == 0xef) {
                _ = input.peek(4) catch &.{};
                const bytes = input.buffered();
                if (std.mem.startsWith(u8, bytes, "\xff\xfe\x00\x00")) effective = "utf-32le" else if (std.mem.startsWith(u8, bytes, "\x00\x00\xfe\xff")) effective = "utf-32be" else if (std.mem.startsWith(u8, bytes, "\xff\xfe")) effective = "utf-16le" else if (std.mem.startsWith(u8, bytes, "\xfe\xff")) effective = "utf-16be";
            }
        }
        var normalized: [64]u8 = undefined;
        const name = try encoding.normalized(effective, &normalized);
        return .{ .a = a, .input = input, .reader = .{ .vtable = &.{ .stream = stream }, .buffer = buffer, .seek = 0, .end = 0 }, .label = effective, .width = if (std.mem.startsWith(u8, name, "utf16")) 2 else if (std.mem.startsWith(u8, name, "utf32")) 4 else 1, .big_endian = std.mem.endsWith(u8, name, "be"), .max_bytes = max_bytes, .iso = std.mem.eql(u8, name, "iso2022jp") };
    }
    pub fn deinit(self: *Reader) void {
        self.a.free(self.decoded);
    }
    fn fill(self: *Reader) !bool {
        self.a.free(self.decoded);
        self.decoded = &.{};
        self.position = 0;
        var raw: std.ArrayList(u8) = .empty;
        defer raw.deinit(self.a);
        if (self.iso_prefix) |prefix| try raw.appendSlice(self.a, &prefix);
        const prefix_bytes = raw.items.len;
        while (true) {
            var unit: [4]u8 = undefined;
            var n: usize = 0;
            while (n < self.width) : (n += 1) unit[n] = self.input.takeByte() catch |err| switch (err) {
                error.EndOfStream => break,
                else => return err,
            };
            if (n == 0) break;
            if (n > self.max_bytes - self.consumed) return error.StreamTooLong;
            self.consumed += n;
            try raw.appendSlice(self.a, unit[0..n]);
            var cp: u32 = 0;
            for (unit[0..n], 0..) |ch, j| cp |= @as(u32, ch) << @intCast(8 * (if (self.big_endian) n - j - 1 else j));
            if (cp == 10 or n < self.width) break;
        }
        if (raw.items.len == prefix_bytes) return false;
        if (self.iso) {
            var i: usize = 0;
            while (i + 2 < raw.items.len) : (i += 1) {
                if (raw.items[i] == 27 and (raw.items[i + 1] == '(' or raw.items[i + 1] == '$')) self.iso_prefix = .{ raw.items[i], raw.items[i + 1], raw.items[i + 2] };
            }
        }
        self.decoded = try encoding.decodeWithBom(self.a, raw.items, self.label, self.first);
        self.first = false;
        return true;
    }
    fn stream(r: *std.Io.Reader, w: *std.Io.Writer, limit: std.Io.Limit) std.Io.Reader.StreamError!usize {
        const self: *Reader = @alignCast(@fieldParentPtr("reader", r));
        while (self.position == self.decoded.len) {
            const more = self.fill() catch |err| {
                self.err = err;
                return error.ReadFailed;
            };
            if (!more) return error.EndOfStream;
        }
        const n = limit.minInt(self.decoded.len - self.position);
        try w.writeAll(self.decoded[self.position..][0..n]);
        self.position += n;
        return n;
    }
};
