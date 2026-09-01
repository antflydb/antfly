// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
// https://www.antfly.io/licensing/ELv2-license
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
const version: u32 = 2;
pub const header_len: usize = magic.len + 4 + 4 + 8;
const index_fixed_entry_len: usize = 8 + 8 + 8 + 4 + 4;

pub const Header = struct {
    version: u32,
    count: u32,
    index_len: u64,

    pub fn indexedBytes(self: Header) !usize {
        if (self.version != version) return error.UnsupportedDocumentSegmentVersion;
        const index_len = std.math.cast(usize, self.index_len) orelse return error.InvalidDocumentSegment;
        return std.math.add(usize, header_len, index_len) catch return error.InvalidDocumentSegment;
    }

    pub fn identityBytes(self: Header) !usize {
        if (self.version != version) return error.UnsupportedDocumentSegmentVersion;
        const index_len = std.math.cast(usize, self.index_len) orelse return error.InvalidDocumentSegment;
        const fixed_bytes = std.math.mul(usize, self.count, index_fixed_entry_len) catch
            return error.InvalidDocumentSegment;
        if (index_len < fixed_bytes) return error.InvalidDocumentSegment;
        return index_len - fixed_bytes;
    }
};

/// New publications place a compact identity directory before the body region.
/// The directory can be fetched independently through the artifact range API.
pub fn encodeAlloc(alloc: Allocator, entries: []const document_segment.Entry) ![]u8 {
    if (entries.len > std.math.maxInt(u32)) return error.DocumentSegmentTooLarge;

    var index_len: usize = 0;
    var body_len: usize = 0;
    var previous_id: ?[]const u8 = null;
    for (entries) |entry| {
        if (entry.doc_id.len == 0 or entry.doc_id.len > std.math.maxInt(u32) or entry.body.len > std.math.maxInt(u32))
            return error.DocumentSegmentTooLarge;
        if (previous_id) |previous| {
            if (std.mem.order(u8, previous, entry.doc_id) != .lt) return error.InvalidDocumentSegmentOrder;
        }
        previous_id = entry.doc_id;
        index_len = std.math.add(usize, index_len, index_fixed_entry_len) catch return error.DocumentSegmentTooLarge;
        index_len = std.math.add(usize, index_len, entry.doc_id.len) catch return error.DocumentSegmentTooLarge;
        body_len = std.math.add(usize, body_len, entry.body.len) catch return error.DocumentSegmentTooLarge;
    }
    const body_start = std.math.add(usize, header_len, index_len) catch return error.DocumentSegmentTooLarge;
    const total_len = std.math.add(usize, body_start, body_len) catch return error.DocumentSegmentTooLarge;

    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try out.ensureTotalCapacityPrecise(alloc, total_len);
    try out.appendSlice(alloc, magic);
    try appendU32(alloc, &out, version);
    try appendU32(alloc, &out, @intCast(entries.len));
    try appendU64(alloc, &out, @intCast(index_len));

    var body_offset: u64 = @intCast(body_start);
    for (entries) |entry| {
        try appendU64(alloc, &out, entry.last_lsn);
        try appendU64(alloc, &out, entry.last_timestamp_ns);
        try appendU64(alloc, &out, body_offset);
        try appendU32(alloc, &out, @intCast(entry.body.len));
        try appendU32(alloc, &out, @intCast(entry.doc_id.len));
        try out.appendSlice(alloc, entry.doc_id);
        body_offset = std.math.add(u64, body_offset, entry.body.len) catch return error.DocumentSegmentTooLarge;
    }
    for (entries) |entry| try out.appendSlice(alloc, entry.body);
    std.debug.assert(out.items.len == total_len);
    return try out.toOwnedSlice(alloc);
}

pub fn decodeHeader(bytes: []const u8) !Header {
    if (bytes.len < magic.len + 4 + 4) return error.InvalidDocumentSegment;
    if (!std.mem.eql(u8, bytes[0..magic.len], magic)) return error.InvalidDocumentSegmentMagic;
    var cursor: usize = magic.len;
    const got_version = readU32(bytes, &cursor);
    const count = readU32(bytes, &cursor);
    if (got_version != version) return error.UnsupportedDocumentSegmentVersion;
    if (bytes.len < header_len) return error.InvalidDocumentSegment;
    return .{ .version = got_version, .count = count, .index_len = readU64(bytes, &cursor) };
}

/// Decode only a v2 header and identity directory. `artifact_len` binds every
/// body range to the pinned artifact and rejects overlap, gaps, and truncation.
pub fn decodeIndexAlloc(alloc: Allocator, bytes: []const u8, artifact_len: u64) ![]document_segment.IndexEntry {
    const header = try decodeHeader(bytes);
    const indexed_len = try header.indexedBytes();
    if (bytes.len != indexed_len or artifact_len < indexed_len) return error.InvalidDocumentSegment;
    const min_index_len = std.math.mul(usize, header.count, index_fixed_entry_len) catch return error.InvalidDocumentSegment;
    if (header.index_len < min_index_len) return error.InvalidDocumentSegment;

    const entries = try alloc.alloc(document_segment.IndexEntry, header.count);
    errdefer alloc.free(entries);
    var initialized: usize = 0;
    errdefer for (entries[0..initialized]) |*entry| entry.deinit(alloc);

    var cursor: usize = header_len;
    var expected_body_offset: u64 = @intCast(indexed_len);
    var previous_id: ?[]const u8 = null;
    for (entries) |*entry| {
        if (cursor > bytes.len or bytes.len - cursor < index_fixed_entry_len) return error.InvalidDocumentSegment;
        const last_lsn = readU64(bytes, &cursor);
        const last_timestamp_ns = readU64(bytes, &cursor);
        const body_offset = readU64(bytes, &cursor);
        const body_len = readU32(bytes, &cursor);
        const doc_id_len = readU32(bytes, &cursor);
        if (doc_id_len == 0 or cursor > bytes.len or bytes.len - cursor < doc_id_len) return error.InvalidDocumentSegment;
        const doc_id = bytes[cursor .. cursor + doc_id_len];
        cursor += doc_id_len;
        if (previous_id) |previous| {
            if (std.mem.order(u8, previous, doc_id) != .lt) return error.InvalidDocumentSegment;
        }
        if (body_offset != expected_body_offset) return error.InvalidDocumentSegment;
        expected_body_offset = std.math.add(u64, expected_body_offset, body_len) catch return error.InvalidDocumentSegment;
        if (expected_body_offset > artifact_len) return error.InvalidDocumentSegment;
        entry.* = .{
            .doc_id = try alloc.dupe(u8, doc_id),
            .body_offset = body_offset,
            .body_len = body_len,
            .last_lsn = last_lsn,
            .last_timestamp_ns = last_timestamp_ns,
        };
        initialized += 1;
        previous_id = entry.doc_id;
    }
    if (cursor != indexed_len or expected_body_offset != artifact_len) return error.InvalidDocumentSegment;
    return entries;
}

pub fn decodeAlloc(alloc: Allocator, bytes: []const u8) ![]document_segment.Entry {
    const header = try decodeHeader(bytes);
    const index_bytes = try header.indexedBytes();
    if (index_bytes > bytes.len) return error.InvalidDocumentSegment;
    const index = try decodeIndexAlloc(alloc, bytes[0..index_bytes], bytes.len);
    defer document_segment.freeIndexEntries(alloc, index);
    const entries = try alloc.alloc(document_segment.Entry, index.len);
    errdefer alloc.free(entries);
    var initialized: usize = 0;
    errdefer for (entries[0..initialized]) |*entry| entry.deinit(alloc);
    for (index, entries) |source, *entry| {
        const body_start = std.math.cast(usize, source.body_offset) orelse return error.InvalidDocumentSegment;
        const body_end = std.math.add(usize, body_start, source.body_len) catch return error.InvalidDocumentSegment;
        if (body_end > bytes.len) return error.InvalidDocumentSegment;
        entry.* = .{
            .doc_id = try alloc.dupe(u8, source.doc_id),
            .body = try alloc.dupe(u8, bytes[body_start..body_end]),
            .last_lsn = source.last_lsn,
            .last_timestamp_ns = source.last_timestamp_ns,
        };
        initialized += 1;
    }
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

test "indexed document segment supports directory-only decoding" {
    const alloc = std.testing.allocator;
    const entries = try alloc.alloc(document_segment.Entry, 2);
    defer document_segment.freeEntries(alloc, entries);
    entries[0] = .{ .doc_id = try alloc.dupe(u8, "doc-a"), .body = try alloc.dupe(u8, "alpha"), .last_lsn = 4, .last_timestamp_ns = 40 };
    entries[1] = .{ .doc_id = try alloc.dupe(u8, "doc-b"), .body = try alloc.dupe(u8, "beta"), .last_lsn = 5, .last_timestamp_ns = 50 };

    const encoded = try encodeAlloc(alloc, entries);
    defer alloc.free(encoded);
    const header = try decodeHeader(encoded[0..header_len]);
    const directory_len = try header.indexedBytes();
    try std.testing.expectEqual(@as(usize, "doc-a".len + "doc-b".len), try header.identityBytes());
    try std.testing.expect(directory_len < encoded.len);
    const index = try decodeIndexAlloc(alloc, encoded[0..directory_len], encoded.len);
    defer document_segment.freeIndexEntries(alloc, index);
    try std.testing.expectEqualStrings("doc-a", index[0].doc_id);
    try std.testing.expectEqualStrings("alpha", encoded[@intCast(index[0].body_offset)..][0..index[0].body_len]);

    const decoded = try decodeAlloc(alloc, encoded);
    defer document_segment.freeEntries(alloc, decoded);
    try std.testing.expectEqualStrings("beta", decoded[1].body);
}

test "indexed document segment rejects non-canonical order" {
    const alloc = std.testing.allocator;
    var entries = [_]document_segment.Entry{
        .{ .doc_id = @constCast("doc-b"), .body = @constCast("beta"), .last_lsn = 1, .last_timestamp_ns = 1 },
        .{ .doc_id = @constCast("doc-a"), .body = @constCast("alpha"), .last_lsn = 2, .last_timestamp_ns = 2 },
    };
    try std.testing.expectError(error.InvalidDocumentSegmentOrder, encodeAlloc(alloc, &entries));
}

test "decoder rejects superseded document segment versions" {
    const alloc = std.testing.allocator;
    var encoded = std.ArrayListUnmanaged(u8).empty;
    defer encoded.deinit(alloc);
    try encoded.appendSlice(alloc, magic);
    try appendU32(alloc, &encoded, 1);
    try appendU32(alloc, &encoded, 0);
    try std.testing.expectError(error.UnsupportedDocumentSegmentVersion, decodeHeader(encoded.items));
}
