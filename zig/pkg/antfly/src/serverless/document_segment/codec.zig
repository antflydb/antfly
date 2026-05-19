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
const document_segment = @import("types.zig");

const magic = "AFDG";
const version: u32 = 1;

pub fn encodeAlloc(alloc: Allocator, entries: []const document_segment.Entry) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);

    try out.appendSlice(alloc, magic);
    try appendU32(alloc, &out, version);
    try appendU32(alloc, &out, @intCast(entries.len));
    for (entries) |entry| {
        try appendU64(alloc, &out, entry.last_lsn);
        try appendU64(alloc, &out, entry.last_timestamp_ns);
        try appendU32(alloc, &out, @intCast(entry.doc_id.len));
        try appendU32(alloc, &out, @intCast(entry.body.len));
        try out.appendSlice(alloc, entry.doc_id);
        try out.appendSlice(alloc, entry.body);
    }
    return try out.toOwnedSlice(alloc);
}

pub fn decodeAlloc(alloc: Allocator, bytes: []const u8) ![]document_segment.Entry {
    var cursor: usize = 0;
    if (bytes.len < magic.len + 4 + 4) return error.InvalidDocumentSegment;
    if (!std.mem.eql(u8, bytes[0..magic.len], magic)) return error.InvalidDocumentSegmentMagic;
    cursor += magic.len;
    const got_version = readU32(bytes, &cursor);
    if (got_version != version) return error.UnsupportedDocumentSegmentVersion;
    const count = readU32(bytes, &cursor);

    const entries = try alloc.alloc(document_segment.Entry, count);
    errdefer alloc.free(entries);

    var initialized: usize = 0;
    errdefer {
        for (entries[0..initialized]) |*entry| entry.deinit(alloc);
    }

    for (0..count) |idx| {
        if (cursor + 8 + 8 + 4 + 4 > bytes.len) return error.InvalidDocumentSegment;
        const last_lsn = readU64(bytes, &cursor);
        const last_timestamp_ns = readU64(bytes, &cursor);
        const doc_id_len = readU32(bytes, &cursor);
        const body_len = readU32(bytes, &cursor);
        if (cursor + doc_id_len + body_len > bytes.len) return error.InvalidDocumentSegment;

        entries[idx] = .{
            .doc_id = try alloc.dupe(u8, bytes[cursor .. cursor + doc_id_len]),
            .body = try alloc.dupe(u8, bytes[cursor + doc_id_len .. cursor + doc_id_len + body_len]),
            .last_lsn = last_lsn,
            .last_timestamp_ns = last_timestamp_ns,
        };
        initialized += 1;
        cursor += doc_id_len + body_len;
    }

    if (cursor != bytes.len) return error.InvalidDocumentSegment;
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

test "published document segment codec round-trips entries" {
    const alloc = std.testing.allocator;
    const entries = try alloc.alloc(document_segment.Entry, 2);
    defer document_segment.freeEntries(alloc, entries);

    entries[0] = .{
        .doc_id = try alloc.dupe(u8, "doc-a"),
        .body = try alloc.dupe(u8, "alpha"),
        .last_lsn = 4,
        .last_timestamp_ns = 40,
    };
    entries[1] = .{
        .doc_id = try alloc.dupe(u8, "doc-b"),
        .body = try alloc.dupe(u8, "beta"),
        .last_lsn = 5,
        .last_timestamp_ns = 50,
    };

    const encoded = try encodeAlloc(alloc, entries);
    defer alloc.free(encoded);
    const decoded = try decodeAlloc(alloc, encoded);
    defer document_segment.freeEntries(alloc, decoded);

    try std.testing.expectEqual(@as(usize, 2), decoded.len);
    try std.testing.expectEqualStrings("doc-a", decoded[0].doc_id);
    try std.testing.expectEqualStrings("alpha", decoded[0].body);
    try std.testing.expectEqual(@as(u64, 5), decoded[1].last_lsn);
}
