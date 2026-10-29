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

//! Merkle-authenticated, seekable input for publication continuations. Cursor
//! offsets identify input bytes, never unauthenticated transport ranges.
const std = @import("std");
const A = std.mem.Allocator;
const tree = @import("../graph_segment/page_tree.zig");
pub const chunk_bytes = 32 * 1024;
pub const Input = struct {
    bytes: u64 = 0,
    page: ?tree.Ref = null,
    pub fn create(a: A, store: tree.Store, bytes: []const u8) !Input {
        if (bytes.len > tree.max_record_bytes) return error.GraphPageRecordTooLarge;
        const Source = struct {
            bytes: []const u8,
            offset: usize = 0,
            key: [8]u8 = undefined,
            const Row = struct { key: []const u8, value: []const u8 };
            pub fn next(self: *@This()) !?Row {
                if (self.offset == self.bytes.len) return null;
                std.mem.writeInt(u64, &self.key, self.offset / chunk_bytes, .big);
                const end = @min(self.bytes.len, self.offset + chunk_bytes);
                const row = Row{ .key = &self.key, .value = self.bytes[self.offset..end] };
                self.offset = end;
                return row;
            }
        };
        var source = Source{ .bytes = bytes };
        return .{ .bytes = bytes.len, .page = try tree.buildSorted(a, store, &source) };
    }
    pub fn validate(self: Input) !void {
        const records = std.math.divCeil(u64, self.bytes, chunk_bytes) catch unreachable;
        if (self.bytes > tree.max_record_bytes or (if (self.page) |ref| ref.records else 0) != records) return error.InvalidPublicationInput;
        if (self.page) |ref| try ref.validate();
    }
    pub fn readAll(self: Input, a: A, store: tree.Store) ![]u8 {
        const bytes = try a.alloc(u8, @intCast(self.bytes));
        errdefer a.free(bytes);
        var cursor = try Cursor.init(a, store, self, 0);
        defer cursor.deinit();
        for (bytes) |*byte| byte.* = (try cursor.byte()) orelse return error.InvalidPublicationInput;
        return bytes;
    }
};
pub const Frame = struct { term: []const u8, value: []const u8 };

pub const Cursor = struct {
    a: A,
    store: tree.Store,
    input: Input,
    offset: u64,
    chunk: ?[]u8 = null,
    ordinal: u64 = std.math.maxInt(u64),
    pub fn init(a: A, store: tree.Store, input: Input, offset: u64) !Cursor {
        try input.validate();
        if (offset > input.bytes) return error.InvalidPublicationInput;
        return .{ .a = a, .store = store, .input = input, .offset = offset };
    }
    pub fn deinit(self: *Cursor) void {
        if (self.chunk) |chunk| self.a.free(chunk);
    }
    pub fn byte(self: *Cursor) !?u8 {
        if (self.offset == self.input.bytes) return null;
        const ordinal = self.offset / chunk_bytes;
        if (ordinal != self.ordinal) {
            var key: [8]u8 = undefined;
            std.mem.writeInt(u64, &key, ordinal, .big);
            const rows = try tree.lookupMany(self.a, self.store, self.input.page, &.{&key});
            defer {
                for (rows) |row| if (row) |entry| entry.deinit(self.a);
                self.a.free(rows);
            }
            const row = rows[0] orelse return error.InvalidPublicationInput;
            if (row.value.len != @min(chunk_bytes, self.input.bytes - ordinal * chunk_bytes)) return error.InvalidPublicationInput;
            const copy = try self.a.dupe(u8, row.value);
            if (self.chunk) |old| self.a.free(old);
            self.chunk = copy;
            self.ordinal = ordinal;
        }
        const value = self.chunk.?[@intCast(self.offset % chunk_bytes)];
        self.offset += 1;
        return value;
    }
    pub fn token(self: *Cursor, a: A) !?[]u8 {
        var out = std.ArrayList(u8).empty;
        errdefer out.deinit(a);
        while (try self.byte()) |value| {
            if (value == ' ') {
                if (out.items.len != 0) break;
                continue;
            }
            if (out.items.len == tree.max_key_bytes) return error.GraphPageRecordTooLarge;
            try out.append(a, value);
        }
        if (out.items.len == 0) {
            out.deinit(a);
            return null;
        }
        return try out.toOwnedSlice(a);
    }
    pub fn frame(self: *Cursor, a: A) !?Frame {
        if (self.offset == self.input.bytes) return null;
        var header: [8]u8 = undefined;
        for (&header) |*byte_| byte_.* = (try self.byte()) orelse return error.InvalidPublicationInput;
        const term_len = std.mem.readInt(u32, header[0..4], .little);
        const value_len = std.mem.readInt(u32, header[4..8], .little);
        if (term_len == 0 or term_len > tree.max_key_bytes or value_len > tree.max_record_bytes or term_len +| @as(u64, value_len) > self.input.bytes - self.offset) return error.InvalidPublicationInput;
        const bytes = try a.alloc(u8, term_len + @as(usize, value_len));
        errdefer a.free(bytes);
        for (bytes) |*byte_| byte_.* = (try self.byte()) orelse return error.InvalidPublicationInput;
        return .{ .term = bytes[0..term_len], .value = bytes[term_len..] };
    }
};
pub fn appendFrame(a: A, bytes: *std.ArrayList(u8), term: []const u8, value: []const u8) !void {
    var header: [8]u8 = undefined;
    std.mem.writeInt(u32, header[0..4], @intCast(term.len), .little);
    std.mem.writeInt(u32, header[4..8], @intCast(value.len), .little);
    try bytes.appendSlice(a, &header);
    try bytes.appendSlice(a, term);
    try bytes.appendSlice(a, value);
    if (bytes.items.len > tree.max_record_bytes) return error.GraphPageRecordTooLarge;
}

test "serverless publication input resumes authenticated tokens across chunk boundaries" {
    const a = std.testing.allocator;
    var memory = tree.testing.MemoryStore{ .alloc = a };
    defer memory.deinit();
    const source = try a.alloc(u8, chunk_bytes + 20);
    defer a.free(source);
    @memset(source, 'x');
    source[chunk_bytes - 2] = ' ';
    source[chunk_bytes + 7] = ' ';
    const input = try Input.create(a, memory.store(), source);
    var cursor = try Cursor.init(a, memory.store(), input, chunk_bytes - 1);
    defer cursor.deinit();
    const first = (try cursor.token(a)).?;
    defer a.free(first);
    try std.testing.expectEqual(@as(usize, 8), first.len);
    const offset = cursor.offset;
    var resumed = try Cursor.init(a, memory.store(), input, offset);
    defer resumed.deinit();
    const tail = (try resumed.token(a)).?;
    defer a.free(tail);
    try std.testing.expectEqual(@as(usize, 12), tail.len);
    try std.testing.expectEqual(null, try resumed.token(a));
    try std.testing.expectError(error.InvalidPublicationInput, Cursor.init(a, memory.store(), input, input.bytes + 1));
}
