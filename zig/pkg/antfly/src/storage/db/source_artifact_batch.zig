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

//! Private, certified source-copy artifacts. Unlike public portable edge
//! objects these preserve ownership manifests and contender state verbatim.
//! Decode borrows the bounded object and validates it before exposing entries.
const std = @import("std");
const graph = @import("online_graph_artifacts.zig");

pub const max_bytes: usize = 128 * 1024 * 1024;
pub const max_key_bytes: usize = 1024 * 1024;
pub const max_entries: usize = 65536;
pub const Entry = struct { key: []const u8, value: []const u8 };

/// A resumable location in an already certified immutable object. Only the
/// key is allocated: large manifests stay in the object during hashing and
/// transfer. Certification validates the entire batch with Reader first;
/// this reader is not a substitute for that validation or receiver validation.
pub const Position = struct { object: u32, offset: u64 = 0, remaining: u32 = 0 };
pub const Descriptor = struct {
    key: []u8,
    object: u32,
    value_offset: u64,
    value_len: u32,
    next_position: Position,

    pub fn read(self: Descriptor, reader: anytype, offset: u64, out: []u8) !void {
        if (offset > self.value_len or out.len > self.value_len - offset) return error.SourceSnapshotCorrupt;
        try exact(reader, self.object, self.value_offset + offset, out);
    }
};

fn exact(reader: anytype, object: u32, offset: u64, out: []u8) !void {
    var done: usize = 0;
    while (done < out.len) {
        const count = try reader.readAt(object, offset + done, out[done..]);
        if (count == 0 or count > out.len - done) return error.SourceSnapshotCorrupt;
        done += count;
    }
}

fn word(reader: anytype, object: u32, offset: u64) !u32 {
    var raw: [4]u8 = undefined;
    try exact(reader, object, offset, &raw);
    return std.mem.readInt(u32, &raw, .little);
}

pub fn descriptor(alloc: std.mem.Allocator, reader: anytype, object_size: u64, position: Position) !?Descriptor {
    if (object_size < 4 or object_size > max_bytes) return error.SourceSnapshotCorrupt;
    var next_position = position;
    if (position.offset == 0) {
        next_position.remaining = try word(reader, position.object, 0);
        if (next_position.remaining == 0 or next_position.remaining > max_entries or
            next_position.remaining > (object_size - 4) / 8) return error.SourceSnapshotCorrupt;
        next_position.offset = 4;
    }
    if (next_position.offset < 4 or next_position.offset > object_size or next_position.remaining > max_entries)
        return error.SourceSnapshotCorrupt;
    if (next_position.remaining == 0) {
        if (next_position.offset != object_size) return error.SourceSnapshotCorrupt;
        return null;
    }
    if (object_size - next_position.offset < 8) return error.SourceSnapshotCorrupt;
    const key_len = try word(reader, position.object, next_position.offset);
    if (key_len == 0 or key_len > max_key_bytes or key_len > object_size - next_position.offset - 8)
        return error.SourceSnapshotCorrupt;
    const key_offset = next_position.offset + 4;
    const value_len = try word(reader, position.object, key_offset + key_len);
    const value_offset = key_offset + key_len + 4;
    if (value_len > object_size - value_offset) return error.SourceSnapshotCorrupt;
    const end = value_offset + value_len;
    next_position.remaining -= 1;
    if ((next_position.remaining == 0 and end != object_size) or next_position.remaining > (object_size - end) / 8)
        return error.SourceSnapshotCorrupt;
    const key = try alloc.alloc(u8, key_len);
    errdefer alloc.free(key);
    try exact(reader, position.object, key_offset, key);
    if (!graph.isKey(key)) return error.SourceSnapshotCorrupt;
    next_position.offset = end;
    return .{ .key = key, .object = position.object, .value_offset = value_offset, .value_len = value_len, .next_position = next_position };
}
pub const Reader = struct {
    bytes: []const u8,
    offset: usize = 4,
    remaining: u32,

    pub fn init(bytes: []const u8) !Reader {
        if (bytes.len < 4 or bytes.len > max_bytes) return error.InvalidGraphTransfer;
        const count = std.mem.readInt(u32, bytes[0..4], .little);
        if (count == 0 or count > max_entries or count > (bytes.len - 4) / 8) return error.InvalidGraphTransfer;
        const result: Reader = .{ .bytes = bytes, .remaining = count };
        var validation = result;
        var previous: []const u8 = "";
        while (try validation.next()) |entry| {
            if (std.mem.order(u8, previous, entry.key) != .lt) return error.InvalidGraphTransfer;
            try graph.validate(entry.key, entry.value);
            previous = entry.key;
        }
        return result;
    }

    fn take(self: *Reader, count: usize) ![]const u8 {
        if (count > self.bytes.len - self.offset) return error.InvalidGraphTransfer;
        const result = self.bytes[self.offset..][0..count];
        self.offset += count;
        return result;
    }
    fn length(self: *Reader) !u32 {
        return std.mem.readInt(u32, (try self.take(4))[0..4], .little);
    }
    pub fn next(self: *Reader) !?Entry {
        if (self.remaining == 0) {
            if (self.offset != self.bytes.len) return error.InvalidGraphTransfer;
            return null;
        }
        const key_len = try self.length();
        if (key_len == 0 or key_len > max_key_bytes) return error.InvalidGraphTransfer;
        const key = try self.take(key_len);
        const value_len = try self.length();
        const value = try self.take(value_len);
        self.remaining -= 1;
        return .{ .key = key, .value = value };
    }
};

test "source artifact batch rejects impossible counts before allocation" {
    try std.testing.expectError(error.InvalidGraphTransfer, Reader.init("\xff\xff\xff\x7f"));
    try std.testing.expectError(error.InvalidGraphTransfer, Reader.init("\x00\x00\x00\x00x"));
}

fn testEncode(alloc: std.mem.Allocator, entries: []const Entry) ![]u8 {
    var bytes: std.ArrayListUnmanaged(u8) = .empty;
    errdefer bytes.deinit(alloc);
    var size: [4]u8 = undefined;
    std.mem.writeInt(u32, &size, @intCast(entries.len), .little);
    try bytes.appendSlice(alloc, &size);
    for (entries) |entry| {
        std.mem.writeInt(u32, &size, @intCast(entry.key.len), .little);
        try bytes.appendSlice(alloc, &size);
        try bytes.appendSlice(alloc, entry.key);
        std.mem.writeInt(u32, &size, @intCast(entry.value.len), .little);
        try bytes.appendSlice(alloc, &size);
        try bytes.appendSlice(alloc, entry.value);
    }
    return bytes.toOwnedSlice(alloc);
}

test "source artifact batch validates full ordered graph framing before exposing a prefix" {
    const alloc = std.testing.allocator;
    const key = try @import("../internal_keys.zig").graphEdgeArtifactKeyAlloc(alloc, "doc", "g", "links", "target");
    defer alloc.free(key);
    const value = try @import("enrichment/artifact_codec.zig").encodeGraphEdgeAlloc(alloc, null, 1, 1, 0, 0, "");
    defer alloc.free(value);
    const raw = try testEncode(alloc, &.{.{ .key = key, .value = value }});
    defer alloc.free(raw);
    var reader = try Reader.init(raw);
    const entry = (try reader.next()).?;
    try std.testing.expectEqualSlices(u8, key, entry.key);
    try std.testing.expectEqualSlices(u8, value, entry.value);
    try std.testing.expect((try reader.next()) == null);
    const duplicate = try testEncode(alloc, &.{ .{ .key = key, .value = value }, .{ .key = key, .value = value } });
    defer alloc.free(duplicate);
    try std.testing.expectError(error.InvalidGraphTransfer, Reader.init(duplicate));
    const metadata = try testEncode(alloc, &.{.{ .key = "\x00\x00__metadata__:indexes", .value = value }});
    defer alloc.free(metadata);
    try std.testing.expectError(error.InvalidGraphTransfer, Reader.init(metadata));
    const extra = try std.mem.concat(alloc, u8, &.{ raw, "x" });
    defer alloc.free(extra);
    try std.testing.expectError(error.InvalidGraphTransfer, Reader.init(extra));
    try std.testing.expectError(error.InvalidGraphTransfer, Reader.init(raw[0 .. raw.len - 1]));
}

test "source artifact descriptors resume short reads without materializing values" {
    const alloc = std.testing.allocator;
    const key = try @import("../internal_keys.zig").graphEdgeArtifactKeyAlloc(alloc, "doc", "g", "links", "target");
    defer alloc.free(key);
    const value = try @import("enrichment/artifact_codec.zig").encodeGraphEdgeAlloc(alloc, null, 1, 1, 0, 0, "");
    defer alloc.free(value);
    const raw = try testEncode(alloc, &.{.{ .key = key, .value = value }});
    defer alloc.free(raw);
    _ = try Reader.init(raw);
    const Memory = struct {
        data: []const u8,
        fn readAt(self: *@This(), object: u32, offset: u64, out: []u8) !usize {
            if (object != 7 or offset > self.data.len) return error.SourceSnapshotCorrupt;
            const count = @min(3, @min(out.len, self.data.len - @as(usize, @intCast(offset))));
            @memcpy(out[0..count], self.data[@intCast(offset)..][0..count]);
            return count;
        }
    };
    var reader: Memory = .{ .data = raw };
    var key_memory: [1024]u8 = undefined;
    var bounded = std.heap.FixedBufferAllocator.init(&key_memory);
    const entry = (try descriptor(bounded.allocator(), &reader, raw.len, .{ .object = 7 })).?;
    try std.testing.expectEqualSlices(u8, key, entry.key);
    var prefix: [8]u8 = undefined;
    try entry.read(&reader, 1, &prefix);
    try std.testing.expectEqualSlices(u8, value[1..9], &prefix);
    try std.testing.expectError(error.SourceSnapshotCorrupt, entry.read(&reader, value.len - 1, &prefix));
    var restarted: Memory = .{ .data = raw };
    try std.testing.expect((try descriptor(alloc, &restarted, raw.len, entry.next_position)) == null);
    try std.testing.expectError(error.SourceSnapshotCorrupt, descriptor(alloc, &reader, raw.len - 1, .{ .object = 7 }));
    try std.testing.expectError(error.SourceSnapshotCorrupt, descriptor(alloc, &reader, raw.len, .{ .object = 7, .offset = 4, .remaining = 0 }));
}
