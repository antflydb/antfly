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

//! Incremental metadata KV effects and bounded full-authority checkpoints.
const std = @import("std");
const capture_mod = @import("../txn_mutation_capture.zig");
pub const prefix = "\x00\x00__metadata__:standalone_ha:";
pub const source_key = prefix ++ "source";
pub const sequence_key = prefix ++ "sequence";
pub const outbox_key = prefix ++ "outbox";
pub const replay_key = prefix ++ "replay";
pub const digest_key = prefix ++ "digest";
pub const max_effect_bytes = 256 * 1024 * 1024;
pub const max_row_bytes = 128 * 1024 * 1024;
pub const Header = struct { source: [16]u8, sequence: u64, group_id: u64, count: u32 };
pub const Row = struct { key: []const u8, value: ?[]const u8 };

fn appendInt(alloc: std.mem.Allocator, out: *std.ArrayList(u8), comptime T: type, value: T) !void {
    var bytes: [@sizeOf(T)]u8 = undefined;
    std.mem.writeInt(T, &bytes, value, .little);
    try out.appendSlice(alloc, &bytes);
}

pub fn encode(alloc: std.mem.Allocator, capture: *capture_mod.Capture, txn: anytype, header: Header) ![]u8 {
    const keys = try alloc.alloc([]const u8, capture.keys.count());
    defer alloc.free(keys);
    var iterator = capture.keys.keyIterator();
    var index: usize = 0;
    while (iterator.next()) |key| : (index += 1) keys[index] = key.*;
    std.mem.sort([]const u8, keys, {}, struct {
        fn less(_: void, a: []const u8, b: []const u8) bool {
            return std.mem.lessThan(u8, a, b);
        }
    }.less);
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(alloc);
    try out.appendSlice(alloc, "AFMH");
    try out.appendSlice(alloc, &header.source);
    try appendInt(alloc, &out, u64, header.sequence);
    try appendInt(alloc, &out, u64, header.group_id);
    try appendInt(alloc, &out, u32, @intCast(keys.len));
    for (keys) |key| {
        if (std.mem.startsWith(u8, key, prefix)) return error.InvalidMetadataHAEffect;
        const value = txn.get(key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        const added = key.len +| (if (value) |bytes| bytes.len else 0) +| 12;
        // Every admitted mutation must also fit the streaming seed row
        // envelope; accepting a larger value would make future seeding fail.
        if (added > max_row_bytes + 12) return error.MetadataHAEffectTooLarge;
        if (out.items.len +| added +| 32 > max_effect_bytes) return error.MetadataHAEffectTooLarge;
        try appendInt(alloc, &out, u32, @intCast(key.len));
        try appendInt(alloc, &out, u64, if (value) |bytes| bytes.len else std.math.maxInt(u64));
        try out.appendSlice(alloc, key);
        if (value) |bytes| try out.appendSlice(alloc, bytes);
    }
    var checksum: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(out.items, &checksum, .{});
    try out.appendSlice(alloc, &checksum);
    return out.toOwnedSlice(alloc);
}

pub const Decoder = struct {
    bytes: []const u8,
    header: Header,
    offset: usize = 40,
    remaining: u32,
    previous: ?[]const u8 = null,
    pub fn init(bytes: []const u8) !Decoder {
        if (bytes.len < 72 or bytes.len > max_effect_bytes or !std.mem.eql(u8, bytes[0..4], "AFMH")) return error.InvalidMetadataHAEffect;
        var digest: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(bytes[0 .. bytes.len - 32], &digest, .{});
        if (!std.mem.eql(u8, &digest, bytes[bytes.len - 32 ..])) return error.InvalidMetadataHAEffect;
        const header: Header = .{ .source = bytes[4..20].*, .sequence = std.mem.readInt(u64, bytes[20..28], .little), .group_id = std.mem.readInt(u64, bytes[28..36], .little), .count = std.mem.readInt(u32, bytes[36..40], .little) };
        if (header.sequence == 0 or header.group_id == 0 or std.mem.allEqual(u8, &header.source, 0)) return error.InvalidMetadataHAEffect;
        return .{ .bytes = bytes[0 .. bytes.len - 32], .header = header, .remaining = header.count };
    }
    pub fn next(self: *Decoder) !?Row {
        if (self.remaining == 0) {
            if (self.offset != self.bytes.len) return error.InvalidMetadataHAEffect;
            return null;
        }
        if (self.bytes.len - self.offset < 12) return error.InvalidMetadataHAEffect;
        const key_len = std.mem.readInt(u32, self.bytes[self.offset..][0..4], .little);
        const value_len = std.mem.readInt(u64, self.bytes[self.offset + 4 ..][0..8], .little);
        self.offset += 12;
        if (key_len == 0 or key_len > self.bytes.len - self.offset) return error.InvalidMetadataHAEffect;
        const key = self.bytes[self.offset..][0..key_len];
        self.offset += key_len;
        if (std.mem.startsWith(u8, key, prefix)) return error.InvalidMetadataHAEffect;
        if (self.previous) |previous| if (!std.mem.lessThan(u8, previous, key)) return error.InvalidMetadataHAEffect;
        self.previous = key;
        const value: ?[]const u8 = if (value_len == std.math.maxInt(u64)) null else blk: {
            if (value_len > self.bytes.len - self.offset) return error.InvalidMetadataHAEffect;
            if (key_len +| value_len > max_row_bytes) return error.InvalidMetadataHAEffect;
            const value = self.bytes[self.offset..][0..@intCast(value_len)];
            self.offset += @intCast(value_len);
            break :blk value;
        };
        self.remaining -= 1;
        return .{ .key = key, .value = value };
    }
};

pub const CheckpointArtifact = struct { size_bytes: u64, sha256: [32]u8 };
pub const CheckpointWriter = struct {
    file: std.Io.File,
    io: std.Io,
    buffer: [64 * 1024]u8 = undefined,
    buffered: usize = 0,
    size: u64 = 0,
    hash: std.crypto.hash.sha2.Sha256 = .init(.{}),
    pub fn write(self: *CheckpointWriter, bytes: []const u8) !void {
        self.hash.update(bytes);
        self.size += bytes.len;
        var rest = bytes;
        while (rest.len != 0) {
            const count = @min(rest.len, self.buffer.len - self.buffered);
            @memcpy(self.buffer[self.buffered..][0..count], rest[0..count]);
            self.buffered += count;
            rest = rest[count..];
            if (self.buffered == self.buffer.len) {
                try self.file.writeStreamingAll(self.io, &self.buffer);
                self.buffered = 0;
            }
        }
    }
    pub fn finish(self: *CheckpointWriter) !CheckpointArtifact {
        if (self.buffered != 0) try self.file.writeStreamingAll(self.io, self.buffer[0..self.buffered]);
        try self.file.sync(self.io);
        var digest: [32]u8 = undefined;
        self.hash.final(&digest);
        return .{ .size_bytes = self.size, .sha256 = digest };
    }
};

pub fn checkpointHeader(key_len: usize, value_len: usize) ![12]u8 {
    if (key_len == 0 or key_len +| value_len > max_row_bytes) return error.MetadataHACheckpointTooLarge;
    var out: [12]u8 = undefined;
    std.mem.writeInt(u32, out[0..4], @intCast(key_len), .little);
    std.mem.writeInt(u64, out[4..12], value_len, .little);
    return out;
}
