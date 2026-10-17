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

//! Disposable root-field range authority, bound to immutable primary bytes.
//! Old/missing authorities fall back to a full read. No primary format changes.
const std = @import("std");
const A = std.mem.Allocator;
pub const minimum_bytes = 16 * 1024;
pub const maximum_bytes = 32 * 1024;
const header_size = 47;
const prefix = "\x00\x00__metadata__:document_projection_v1:";
pub const Key = [prefix.len + 32]u8;
pub fn keyForRevision(key: anytype) Key {
    var result: Key = undefined;
    @memcpy(result[0..prefix.len], prefix);
    @memcpy(result[prefix.len..], key[key.len - 32 ..]);
    return result;
}
pub const FieldValue = struct { name: []const u8, value: []const u8 };
pub const Source = union(enum) { bytes: []const u8, fields: []const FieldValue };
pub const Field = struct { name: []const u8, offset: u64, length: u64, digest: [32]u8 };
fn less(_: void, left: Field, right: Field) bool {
    return std.mem.order(u8, left.name, right.name) == .lt;
}

pub fn build(a: A, bytes: []const u8, kind: u8, digest: [32]u8) !?[]u8 {
    if (bytes.len < minimum_bytes) return null;
    var arena = std.heap.ArenaAllocator.init(@import("antfly_platform").allocator.reclaimingScratch(a));
    defer arena.deinit();
    const temp = arena.allocator();
    const Sink = struct {
        a: A,
        bytes: []const u8,
        fields: std.ArrayList(Field) = .empty,
        size: usize = header_size,
        limit: usize,
        fn member(raw: *anyopaque, name: []const u8, value: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.fields.items.len == 512 or name.len > std.math.maxInt(u16) or name.len + 50 > self.limit -| self.size) return error.ProjectionDirectoryTooLarge;
            var hash: [32]u8 = undefined;
            std.crypto.hash.sha2.Sha256.hash(value, &hash, .{});
            try self.fields.append(self.a, .{ .name = try self.a.dupe(u8, name), .offset = @intFromPtr(value.ptr) - @intFromPtr(self.bytes.ptr), .length = value.len, .digest = hash });
            self.size += 50 + name.len;
        }
    };
    var sink: Sink = .{ .a = temp, .bytes = bytes, .limit = @min(maximum_bytes, bytes.len / 16) };
    var scanner = std.json.Scanner.initCompleteInput(temp, bytes);
    defer scanner.deinit();
    if ((scanner.peekNextTokenType() catch |err| {
        if (err == error.OutOfMemory) return err;
        return null;
    }) != .object_begin) return null;
    @import("antfly-json").scanValue(arena.child_allocator, &scanner, bytes.len, .{ .ctx = &sink, .member_fn = Sink.member }) catch |err| {
        if (err == error.OutOfMemory) return err;
        return null;
    };
    if ((scanner.next() catch |err| {
        if (err == error.OutOfMemory) return err;
        return null;
    }) != .end_of_document) return null;
    std.mem.sort(Field, sink.fields.items, {}, less);
    const output = try a.alloc(u8, sink.size);
    @memcpy(output[0..4], "DPD1");
    std.mem.writeInt(u64, output[4..12], bytes.len, .little);
    @memcpy(output[12..44], &digest);
    output[44] = kind;
    std.mem.writeInt(u16, output[45..47], @intCast(sink.fields.items.len), .little);
    var pos: usize = header_size;
    for (sink.fields.items) |field| {
        std.mem.writeInt(u16, output[pos..][0..2], @intCast(field.name.len), .little);
        pos += 2;
        @memcpy(output[pos..][0..field.name.len], field.name);
        pos += field.name.len;
        std.mem.writeInt(u64, output[pos..][0..8], field.offset, .little);
        pos += 8;
        std.mem.writeInt(u64, output[pos..][0..8], field.length, .little);
        pos += 8;
        @memcpy(output[pos..][0..32], &field.digest);
        pos += 32;
    }
    return output;
}

/// Remember unsupported large values too, so bounded migration/restarts do
/// not repeatedly read binary values or objects exceeding directory limits.
pub fn buildAuthority(a: A, bytes: []const u8, kind: u8, digest: [32]u8) !?[]u8 {
    if (kind != @import("internal_keys.zig").primary_kind or bytes.len < minimum_bytes) return null;
    if (try build(a, bytes, kind, digest)) |encoded| return encoded;
    const output = try a.alloc(u8, header_size);
    @memcpy(output[0..4], "DPX1");
    std.mem.writeInt(u64, output[4..12], bytes.len, .little);
    @memcpy(output[12..44], &digest);
    output[44] = kind;
    std.mem.writeInt(u16, output[45..47], 0, .little);
    return output;
}
pub fn isCurrent(a: A, bytes: []const u8, proof: anytype) !bool {
    if (bytes.len == header_size and std.mem.eql(u8, bytes[0..4], "DPX1") and std.mem.readInt(u64, bytes[4..12], .little) == proof.bytes and bytes[44] == proof.kind and std.mem.eql(u8, bytes[12..44], &proof.digest) and std.mem.readInt(u16, bytes[45..47], .little) == 0) return true;
    const directory = (try Directory.open(a, bytes, proof)) orelse return false;
    a.free(directory.fields);
    return true;
}

pub const Directory = struct {
    fields: []const Field,
    pub fn open(a: A, bytes: []const u8, proof: anytype) !?Directory {
        if (bytes.len < header_size or bytes.len > maximum_bytes or bytes.len > proof.bytes / 16 or !std.mem.eql(u8, bytes[0..4], "DPD1")) return null;
        if (std.mem.readInt(u64, bytes[4..12], .little) != proof.bytes or bytes[44] != proof.kind or !std.mem.eql(u8, bytes[12..44], &proof.digest)) return null;
        const count = std.mem.readInt(u16, bytes[45..47], .little);
        if (count > 512) return null;
        // First validate extents before allocating a trusted field directory.
        var pos: usize = header_size;
        for (0..count) |_| {
            if (bytes.len -| pos < 2) return null;
            const len = std.mem.readInt(u16, bytes[pos..][0..2], .little);
            pos += 2;
            if (bytes.len -| pos < @as(usize, len) + 48) return null;
            const offset = std.mem.readInt(u64, bytes[pos + len ..][0..8], .little);
            const size = std.mem.readInt(u64, bytes[pos + len + 8 ..][0..8], .little);
            if (offset > proof.bytes or size > proof.bytes - offset) return null;
            pos += @as(usize, len) + 48;
        }
        if (pos != bytes.len) return null;
        const fields = try a.alloc(Field, count);
        errdefer a.free(fields);
        pos = header_size;
        for (fields, 0..) |*field, i| {
            const len = std.mem.readInt(u16, bytes[pos..][0..2], .little);
            pos += 2;
            field.* = .{ .name = bytes[pos..][0..len], .offset = std.mem.readInt(u64, bytes[pos + len ..][0..8], .little), .length = std.mem.readInt(u64, bytes[pos + len + 8 ..][0..8], .little), .digest = bytes[pos + len + 16 ..][0..32].* };
            if (i != 0 and !less({}, fields[i - 1], field.*)) {
                a.free(fields);
                return null;
            }
            pos += @as(usize, len) + 48;
        }
        return .{ .fields = fields };
    }
    pub fn find(self: Directory, name: []const u8) ?Field {
        var low: usize = 0;
        var high = self.fields.len;
        while (low < high) {
            const mid = low + (high - low) / 2;
            switch (std.mem.order(u8, name, self.fields[mid].name)) {
                .eq => return self.fields[mid],
                .lt => high = mid,
                .gt => low = mid + 1,
            }
        }
        return null;
    }
};
