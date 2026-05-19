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

const std = @import("std");
const Allocator = std.mem.Allocator;
const segment_types = @import("types.zig");
const api_types = @import("../api/types.zig");

const magic = "AFSG";
const version: u32 = 1;

pub fn encodeAlloc(alloc: Allocator, entries: []const segment_types.Entry) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);

    try out.appendSlice(alloc, magic);
    try appendU32(alloc, &out, version);
    try appendU32(alloc, &out, @intCast(entries.len));
    for (entries) |entry| {
        try appendU64(alloc, &out, entry.lsn);
        try appendU64(alloc, &out, entry.timestamp_ns);
        try out.append(alloc, @intFromEnum(entry.kind));
        try appendU32(alloc, &out, @intCast(entry.doc_id.len));
        try appendU32(alloc, &out, @intCast(if (entry.body) |body| body.len else 0));
        try out.appendSlice(alloc, entry.doc_id);
        if (entry.body) |body| try out.appendSlice(alloc, body);
    }
    return try out.toOwnedSlice(alloc);
}

pub fn decodeAlloc(alloc: Allocator, bytes: []const u8) ![]segment_types.Entry {
    var cursor: usize = 0;
    if (bytes.len < magic.len + 4 + 4) return error.InvalidSegment;
    if (!std.mem.eql(u8, bytes[0..magic.len], magic)) return error.InvalidSegmentMagic;
    cursor += magic.len;
    const got_version = readU32(bytes, &cursor);
    if (got_version != version) return error.UnsupportedSegmentVersion;
    const count = readU32(bytes, &cursor);

    const entries = try alloc.alloc(segment_types.Entry, count);
    errdefer alloc.free(entries);

    var initialized: usize = 0;
    errdefer {
        for (entries[0..initialized]) |*entry| entry.deinit(alloc);
    }

    for (0..count) |idx| {
        if (cursor + 8 + 8 + 1 + 4 + 4 > bytes.len) return error.InvalidSegment;
        const lsn = readU64(bytes, &cursor);
        const timestamp_ns = readU64(bytes, &cursor);
        const kind_raw = bytes[cursor];
        cursor += 1;
        const doc_id_len = readU32(bytes, &cursor);
        const body_len = readU32(bytes, &cursor);
        if (cursor + doc_id_len + body_len > bytes.len) return error.InvalidSegment;

        const kind: api_types.MutationKind = switch (kind_raw) {
            @intFromEnum(api_types.MutationKind.upsert) => .upsert,
            @intFromEnum(api_types.MutationKind.delete) => .delete,
            else => return error.InvalidSegment,
        };
        entries[idx] = .{
            .lsn = lsn,
            .timestamp_ns = timestamp_ns,
            .kind = kind,
            .doc_id = try alloc.dupe(u8, bytes[cursor .. cursor + doc_id_len]),
            .body = if (body_len == 0) null else try alloc.dupe(u8, bytes[cursor + doc_id_len .. cursor + doc_id_len + body_len]),
        };
        initialized += 1;
        cursor += doc_id_len + body_len;
    }

    if (cursor != bytes.len) return error.InvalidSegment;
    return entries;
}

fn appendU32(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: u32) !void {
    var buf: [4]u8 = undefined;
    std.mem.writeInt(u32, &buf, value, .little);
    try out.appendSlice(alloc, &buf);
}

fn appendU64(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: u64) !void {
    var buf: [8]u8 = undefined;
    std.mem.writeInt(u64, &buf, value, .little);
    try out.appendSlice(alloc, &buf);
}

fn readU32(bytes: []const u8, cursor: *usize) u32 {
    const value = std.mem.readInt(u32, bytes[cursor.*..][0..4], .little);
    cursor.* += 4;
    return value;
}

fn readU64(bytes: []const u8, cursor: *usize) u64 {
    const value = std.mem.readInt(u64, bytes[cursor.*..][0..8], .little);
    cursor.* += 8;
    return value;
}

test "mutation segment codec round-trips entries" {
    const alloc = std.testing.allocator;
    const entries = try alloc.alloc(segment_types.Entry, 2);
    defer segment_types.freeEntries(alloc, entries);
    entries[0] = .{
        .lsn = 1,
        .timestamp_ns = 100,
        .kind = .upsert,
        .doc_id = try alloc.dupe(u8, "doc-a"),
        .body = try alloc.dupe(u8, "alpha"),
    };
    entries[1] = .{
        .lsn = 2,
        .timestamp_ns = 200,
        .kind = .delete,
        .doc_id = try alloc.dupe(u8, "doc-b"),
        .body = null,
    };

    const encoded = try encodeAlloc(alloc, entries);
    defer alloc.free(encoded);
    const decoded = try decodeAlloc(alloc, encoded);
    defer segment_types.freeEntries(alloc, decoded);

    try std.testing.expectEqual(@as(usize, 2), decoded.len);
    try std.testing.expectEqual(@as(u64, 1), decoded[0].lsn);
    try std.testing.expectEqualStrings("alpha", decoded[0].body.?);
    try std.testing.expectEqual(@as(@TypeOf(decoded[1].kind), .delete), decoded[1].kind);
}
