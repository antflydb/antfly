// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Storage-free binary envelope for one coarse WAL scan result.
//! Entry bytes remain borrowed from the envelope while parsed descriptors are
//! caller-owned. The format is private to the versioned linked-storage ABI.

const std = @import("std");

const magic: u32 = 0x4c574641; // AFWL
const version: u32 = 1;
const header_len = 16;
const entry_header_len = 16;

pub const Entry = struct {
    lsn: u64,
    data: []const u8,
};

pub const Encoder = struct {
    bytes: std.ArrayListUnmanaged(u8) = .empty,
    count: u64 = 0,

    pub fn init(self: *Encoder, alloc: std.mem.Allocator) !void {
        std.debug.assert(self.bytes.items.len == 0);
        try self.bytes.resize(alloc, header_len);
        std.mem.writeInt(u32, self.bytes.items[0..4], magic, .little);
        std.mem.writeInt(u32, self.bytes.items[4..8], version, .little);
        std.mem.writeInt(u64, self.bytes.items[8..16], 0, .little);
    }

    pub fn deinit(self: *Encoder, alloc: std.mem.Allocator) void {
        self.bytes.deinit(alloc);
        self.* = .{};
    }

    pub fn append(self: *Encoder, alloc: std.mem.Allocator, entry: Entry) !void {
        var header: [entry_header_len]u8 = undefined;
        std.mem.writeInt(u64, header[0..8], entry.lsn, .little);
        std.mem.writeInt(u64, header[8..16], @intCast(entry.data.len), .little);
        try self.bytes.appendSlice(alloc, &header);
        try self.bytes.appendSlice(alloc, entry.data);
        self.count = try std.math.add(u64, self.count, 1);
    }

    pub fn finish(self: *Encoder, alloc: std.mem.Allocator) ![]u8 {
        std.mem.writeInt(u64, self.bytes.items[8..16], self.count, .little);
        const result = try self.bytes.toOwnedSlice(alloc);
        self.* = .{};
        return result;
    }
};

pub fn encodedEntryLen(data_len: usize) !usize {
    return std.math.add(usize, entry_header_len, data_len);
}

pub fn encodeEntriesAlloc(alloc: std.mem.Allocator, entries: anytype) ![]u8 {
    var encoder = Encoder{};
    defer encoder.deinit(alloc);
    try encoder.init(alloc);
    for (entries) |entry| try encoder.append(alloc, .{ .lsn = entry.lsn, .data = entry.data });
    return encoder.finish(alloc);
}

pub fn parseEntriesAlloc(alloc: std.mem.Allocator, bytes: []const u8) ![]Entry {
    if (bytes.len < header_len) return error.InvalidWalWire;
    if (std.mem.readInt(u32, bytes[0..4], .little) != magic) return error.InvalidWalWire;
    if (std.mem.readInt(u32, bytes[4..8], .little) != version) return error.InvalidWalWire;
    const count = std.math.cast(usize, std.mem.readInt(u64, bytes[8..16], .little)) orelse
        return error.InvalidWalWire;
    if (count > (bytes.len - header_len) / entry_header_len) return error.InvalidWalWire;

    const entries = try alloc.alloc(Entry, count);
    errdefer alloc.free(entries);
    var offset: usize = header_len;
    for (entries) |*entry| {
        const header_end = std.math.add(usize, offset, entry_header_len) catch return error.InvalidWalWire;
        if (header_end > bytes.len) return error.InvalidWalWire;
        const data_len = std.math.cast(usize, std.mem.readInt(u64, bytes[offset + 8 ..][0..8], .little)) orelse
            return error.InvalidWalWire;
        const data_end = std.math.add(usize, header_end, data_len) catch return error.InvalidWalWire;
        if (data_end > bytes.len) return error.InvalidWalWire;
        entry.* = .{
            .lsn = std.mem.readInt(u64, bytes[offset..][0..8], .little),
            .data = bytes[header_end..data_end],
        };
        offset = data_end;
    }
    if (offset != bytes.len) return error.InvalidWalWire;
    return entries;
}

test "WAL scan wire round trips exact entry identities and bytes" {
    const entries = [_]Entry{
        .{ .lsn = 7, .data = "alpha" },
        .{ .lsn = 19, .data = &.{ 0, 1, 2, 255 } },
    };
    const encoded = try encodeEntriesAlloc(std.testing.allocator, &entries);
    defer std.testing.allocator.free(encoded);
    const decoded = try parseEntriesAlloc(std.testing.allocator, encoded);
    defer std.testing.allocator.free(decoded);
    try std.testing.expectEqual(entries.len, decoded.len);
    for (entries, decoded) |expected, actual| {
        try std.testing.expectEqual(expected.lsn, actual.lsn);
        try std.testing.expectEqualSlices(u8, expected.data, actual.data);
    }
}

test "WAL scan wire rejects truncation and trailing ambiguity" {
    const encoded = try encodeEntriesAlloc(std.testing.allocator, &[_]Entry{.{ .lsn = 1, .data = "x" }});
    defer std.testing.allocator.free(encoded);
    try std.testing.expectError(error.InvalidWalWire, parseEntriesAlloc(std.testing.allocator, encoded[0 .. encoded.len - 1]));

    const trailing = try std.testing.allocator.alloc(u8, encoded.len + 1);
    defer std.testing.allocator.free(trailing);
    @memcpy(trailing[0..encoded.len], encoded);
    trailing[encoded.len] = 0;
    try std.testing.expectError(error.InvalidWalWire, parseEntriesAlloc(std.testing.allocator, trailing));
}
