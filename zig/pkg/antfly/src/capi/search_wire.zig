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
const capi = @import("types.zig");

pub const magic: u32 = 0x41464442; // "AFDB"
pub const version: u16 = 1;

// Packed search wire responses deliberately stay minimal: opaque hit IDs plus scores.
// Callers that need artifact structure should decode artifact IDs separately.
// Responses may include an 8-byte identity_read_generation footer after the ID blob.

pub const Op = enum(u16) {
    dense_search = 1,
    text_match_search = 2,
    text_term_search = 3,
    text_match_phrase_search = 4,
};

pub const DenseRequest = struct {
    index_name: []const u8,
    vector: []f32,
    k: u32,
    limit: u32,
    offset: u32,
};

pub const PackedHit = struct {
    id_offset: u32,
    id_len: u16,
    reserved: u16 = 0,
    score: f32,
};

pub const TextMatchRequest = struct {
    index_name: []const u8,
    field: []const u8,
    text: []const u8,
    analyzer: []const u8,
    prefix: u16,
    fuzziness: u16,
    auto: bool,
    operator: u8,
    boost: f32,
    limit: u32,
    offset: u32,
};

pub const TextTermRequest = TextMatchRequest;
pub const TextMatchPhraseRequest = TextMatchRequest;

pub fn decodeDenseRequest(alloc: std.mem.Allocator, payload: []const u8) !DenseRequest {
    var reader = Reader{ .buf = payload };
    try reader.expectMagicAndVersion(.dense_search);

    _ = try reader.readInt(u32); // flags
    const k = try reader.readInt(u32);
    const limit = try reader.readInt(u32);
    const offset = try reader.readInt(u32);
    const index_name_len = try reader.readInt(u16);
    const dims = try reader.readInt(u16);

    const index_name = try alloc.dupe(u8, try reader.readBytes(index_name_len));
    errdefer alloc.free(index_name);

    const vector = try alloc.alloc(f32, dims);
    errdefer alloc.free(vector);
    for (vector) |*value| {
        value.* = @bitCast(try reader.readInt(u32));
    }
    if (!reader.done()) return error.InvalidArgument;

    return .{
        .index_name = index_name,
        .vector = vector,
        .k = k,
        .limit = limit,
        .offset = offset,
    };
}

pub fn freeDenseRequest(alloc: std.mem.Allocator, req: *DenseRequest) void {
    alloc.free(req.index_name);
    alloc.free(req.vector);
    req.* = undefined;
}

pub fn decodeTextMatchRequest(alloc: std.mem.Allocator, payload: []const u8) !TextMatchRequest {
    return decodeTextRequest(alloc, payload, .text_match_search);
}

pub fn decodeTextTermRequest(alloc: std.mem.Allocator, payload: []const u8) !TextTermRequest {
    return decodeTextRequest(alloc, payload, .text_term_search);
}

pub fn decodeTextMatchPhraseRequest(alloc: std.mem.Allocator, payload: []const u8) !TextMatchPhraseRequest {
    return decodeTextRequest(alloc, payload, .text_match_phrase_search);
}

fn decodeTextRequest(alloc: std.mem.Allocator, payload: []const u8, expected_op: Op) !TextMatchRequest {
    var reader = Reader{ .buf = payload };
    try reader.expectMagicAndVersion(expected_op);

    const flags = try reader.readInt(u32);
    const limit = try reader.readInt(u32);
    const offset = try reader.readInt(u32);
    const boost: f32 = @bitCast(try reader.readInt(u32));
    const index_name_len = try reader.readInt(u16);
    const field_len = try reader.readInt(u16);
    const text_len = try reader.readInt(u32);
    const analyzer_len = try reader.readInt(u16);
    const prefix = try reader.readInt(u16);
    const fuzziness = try reader.readInt(u16);
    const operator = try reader.readInt(u8);
    _ = try reader.readInt(u8); // reserved

    const index_name = try alloc.dupe(u8, try reader.readBytes(index_name_len));
    errdefer alloc.free(index_name);
    const field = try alloc.dupe(u8, try reader.readBytes(field_len));
    errdefer alloc.free(field);
    const text = try alloc.dupe(u8, try reader.readBytes(text_len));
    errdefer alloc.free(text);
    const analyzer = try alloc.dupe(u8, try reader.readBytes(analyzer_len));
    errdefer alloc.free(analyzer);
    if (!reader.done()) return error.InvalidArgument;

    return .{
        .index_name = index_name,
        .field = field,
        .text = text,
        .analyzer = analyzer,
        .prefix = prefix,
        .fuzziness = fuzziness,
        .auto = (flags & 1) != 0,
        .operator = operator,
        .boost = boost,
        .limit = limit,
        .offset = offset,
    };
}

pub fn freeTextMatchRequest(alloc: std.mem.Allocator, req: *TextMatchRequest) void {
    alloc.free(req.index_name);
    alloc.free(req.field);
    alloc.free(req.text);
    alloc.free(req.analyzer);
    req.* = undefined;
}

pub fn freeTextTermRequest(alloc: std.mem.Allocator, req: *TextTermRequest) void {
    freeTextMatchRequest(alloc, req);
}

pub fn freeTextMatchPhraseRequest(alloc: std.mem.Allocator, req: *TextMatchPhraseRequest) void {
    freeTextMatchRequest(alloc, req);
}

pub fn encodeDenseResponse(
    total_hits: u32,
    ids: []const []const u8,
    scores: []const f32,
) !capi.Buffer {
    return try encodeDenseResponseAtGeneration(total_hits, ids, scores, null);
}

pub fn encodeDenseResponseAtGeneration(
    total_hits: u32,
    ids: []const []const u8,
    scores: []const f32,
    identity_read_generation: ?u64,
) !capi.Buffer {
    std.debug.assert(ids.len == scores.len);

    var ids_len: usize = 0;
    for (ids) |id| ids_len += id.len;

    const header_len: usize = 4 + 2 + 2 + 4 + 4 + 4;
    const hits_len: usize = ids.len * @sizeOf(PackedHit);
    const footer_len: usize = if (identity_read_generation == null) 0 else @sizeOf(u64);
    const total_len = header_len + hits_len + ids_len + footer_len;
    const out = try std.heap.c_allocator.alloc(u8, total_len);
    errdefer std.heap.c_allocator.free(out);

    var cursor: usize = 0;
    writeInt(out, &cursor, u32, magic);
    writeInt(out, &cursor, u16, version);
    writeInt(out, &cursor, u16, @intFromEnum(Op.dense_search));
    writeInt(out, &cursor, u32, total_hits);
    writeInt(out, &cursor, u32, @intCast(ids.len));
    writeInt(out, &cursor, u32, @intCast(ids_len));

    var id_cursor: u32 = 0;
    for (ids, scores) |id, score| {
        writeInt(out, &cursor, u32, id_cursor);
        writeInt(out, &cursor, u16, @intCast(id.len));
        writeInt(out, &cursor, u16, 0);
        writeInt(out, &cursor, u32, @bitCast(score));
        id_cursor += @intCast(id.len);
    }

    for (ids) |id| {
        @memcpy(out[cursor..][0..id.len], id);
        cursor += id.len;
    }
    if (identity_read_generation) |generation| {
        writeInt(out, &cursor, u64, generation);
    }

    return .{ .ptr = out.ptr, .len = out.len };
}

pub fn denseResponseIdentityReadGeneration(payload: []const u8) !?u64 {
    var reader = Reader{ .buf = payload };
    try reader.expectMagicAndVersion(.dense_search);
    _ = try reader.readInt(u32); // total_hits
    const hit_count = try reader.readInt(u32);
    const ids_len = try reader.readInt(u32);
    const hits_len = @as(usize, @intCast(hit_count)) * @sizeOf(PackedHit);
    const payload_len = reader.pos + hits_len + @as(usize, @intCast(ids_len));
    if (payload_len > payload.len) return error.InvalidArgument;
    if (payload.len == payload_len) return null;
    if (payload.len != payload_len + @sizeOf(u64)) return error.InvalidArgument;
    return std.mem.readInt(u64, payload[payload_len..][0..@sizeOf(u64)], .little);
}

test "packed dense response exposes public ids not doc ordinals" {
    const ids = [_][]const u8{ "doc:b", "doc:a" };
    const scores = [_]f32{ 0.75, 0.25 };
    var out = try encodeDenseResponseAtGeneration(2, ids[0..], scores[0..], 99);
    defer std.heap.c_allocator.free(out.ptr.?[0..out.len]);

    const header_len: usize = 4 + 2 + 2 + 4 + 4 + 4;
    const hits_len = ids.len * @sizeOf(PackedHit);
    const ids_len = ids[0].len + ids[1].len;
    try std.testing.expectEqual(header_len + hits_len + ids_len + @sizeOf(u64), out.len);
    try std.testing.expectEqual(@as(usize, 12), @sizeOf(PackedHit));
    try std.testing.expectEqual(@as(?u64, 99), try denseResponseIdentityReadGeneration(out.ptr.?[0..out.len]));

    var cursor: usize = 0;
    try std.testing.expectEqual(magic, std.mem.readInt(u32, out.ptr.?[cursor..][0..4], .little));
    cursor += 4;
    try std.testing.expectEqual(version, std.mem.readInt(u16, out.ptr.?[cursor..][0..2], .little));
    cursor += 2;
    try std.testing.expectEqual(@intFromEnum(Op.dense_search), std.mem.readInt(u16, out.ptr.?[cursor..][0..2], .little));
    cursor += 2;
    try std.testing.expectEqual(@as(u32, 2), std.mem.readInt(u32, out.ptr.?[cursor..][0..4], .little));
    cursor += 4;
    try std.testing.expectEqual(@as(u32, 2), std.mem.readInt(u32, out.ptr.?[cursor..][0..4], .little));
    cursor += 4;
    try std.testing.expectEqual(@as(u32, @intCast(ids_len)), std.mem.readInt(u32, out.ptr.?[cursor..][0..4], .little));
    cursor += 4;

    const first_id_offset = std.mem.readInt(u32, out.ptr.?[cursor..][0..4], .little);
    const first_id_len = std.mem.readInt(u16, out.ptr.?[cursor + 4 ..][0..2], .little);
    const first_reserved = std.mem.readInt(u16, out.ptr.?[cursor + 6 ..][0..2], .little);
    const first_score: f32 = @bitCast(std.mem.readInt(u32, out.ptr.?[cursor + 8 ..][0..4], .little));
    try std.testing.expectEqual(@as(u32, 0), first_id_offset);
    try std.testing.expectEqual(@as(u16, @intCast(ids[0].len)), first_id_len);
    try std.testing.expectEqual(@as(u16, 0), first_reserved);
    try std.testing.expectEqual(scores[0], first_score);
    cursor += @sizeOf(PackedHit);

    const second_id_offset = std.mem.readInt(u32, out.ptr.?[cursor..][0..4], .little);
    const second_id_len = std.mem.readInt(u16, out.ptr.?[cursor + 4 ..][0..2], .little);
    const second_reserved = std.mem.readInt(u16, out.ptr.?[cursor + 6 ..][0..2], .little);
    const second_score: f32 = @bitCast(std.mem.readInt(u32, out.ptr.?[cursor + 8 ..][0..4], .little));
    try std.testing.expectEqual(@as(u32, @intCast(ids[0].len)), second_id_offset);
    try std.testing.expectEqual(@as(u16, @intCast(ids[1].len)), second_id_len);
    try std.testing.expectEqual(@as(u16, 0), second_reserved);
    try std.testing.expectEqual(scores[1], second_score);
    cursor += @sizeOf(PackedHit);

    try std.testing.expectEqualStrings("doc:bdoc:a", out.ptr.?[cursor..][0..ids_len]);
}

fn writeInt(buf: []u8, cursor: *usize, comptime T: type, value: T) void {
    std.mem.writeInt(T, buf[cursor.*..][0..@sizeOf(T)], value, .little);
    cursor.* += @sizeOf(T);
}

const Reader = struct {
    buf: []const u8,
    pos: usize = 0,

    fn expectMagicAndVersion(self: *Reader, expected_op: Op) !void {
        const found_magic = try self.readInt(u32);
        if (found_magic != magic) return error.InvalidArgument;
        const found_version = try self.readInt(u16);
        if (found_version != version) return error.InvalidArgument;
        const found_op: Op = @enumFromInt(try self.readInt(u16));
        if (found_op != expected_op) return error.InvalidArgument;
    }

    fn readBytes(self: *Reader, len: usize) ![]const u8 {
        if (self.pos + len > self.buf.len) return error.InvalidArgument;
        defer self.pos += len;
        return self.buf[self.pos..][0..len];
    }

    fn readInt(self: *Reader, comptime T: type) !T {
        const bytes = try self.readBytes(@sizeOf(T));
        return std.mem.readInt(T, bytes[0..@sizeOf(T)], .little);
    }

    fn done(self: *Reader) bool {
        return self.pos == self.buf.len;
    }
};

test "round-trip dense response header" {
    const alloc = std.testing.allocator;
    const ids = [_][]const u8{ "doc1", "doc-two" };
    const scores = [_]f32{ 1.25, 2.5 };
    _ = alloc;
    const buf = try encodeDenseResponse(7, &ids, &scores);
    defer std.heap.c_allocator.free(buf.ptr.?[0..buf.len]);

    var reader = Reader{ .buf = buf.ptr.?[0..buf.len] };
    try reader.expectMagicAndVersion(.dense_search);
    try std.testing.expectEqual(@as(u32, 7), try reader.readInt(u32));
    try std.testing.expectEqual(@as(u32, 2), try reader.readInt(u32));
    try std.testing.expectEqual(@as(u32, ids[0].len + ids[1].len), try reader.readInt(u32));
    try std.testing.expectEqual(@as(?u64, null), try denseResponseIdentityReadGeneration(buf.ptr.?[0..buf.len]));
}

test "dense response identity generation footer" {
    const ids = [_][]const u8{ "doc1", "doc-two" };
    const scores = [_]f32{ 1.25, 2.5 };
    const buf = try encodeDenseResponseAtGeneration(7, &ids, &scores, 42);
    defer std.heap.c_allocator.free(buf.ptr.?[0..buf.len]);

    var reader = Reader{ .buf = buf.ptr.?[0..buf.len] };
    try reader.expectMagicAndVersion(.dense_search);
    try std.testing.expectEqual(@as(u32, 7), try reader.readInt(u32));
    try std.testing.expectEqual(@as(u32, 2), try reader.readInt(u32));
    try std.testing.expectEqual(@as(u32, ids[0].len + ids[1].len), try reader.readInt(u32));
    try std.testing.expectEqual(@as(?u64, 42), try denseResponseIdentityReadGeneration(buf.ptr.?[0..buf.len]));
}

test "decode text match request" {
    const alloc = std.testing.allocator;
    const index_name = "full_text_index";
    const field = "content";
    const text = "alpha";
    const analyzer = "standard";
    const total_len = 4 + 2 + 2 + 4 + 4 + 4 + 4 + 2 + 2 + 4 + 2 + 2 + 2 + 1 + 1 + index_name.len + field.len + text.len + analyzer.len;
    const buf = try alloc.alloc(u8, total_len);
    defer alloc.free(buf);

    var cursor: usize = 0;
    writeInt(buf, &cursor, u32, magic);
    writeInt(buf, &cursor, u16, version);
    writeInt(buf, &cursor, u16, @intFromEnum(Op.text_match_search));
    writeInt(buf, &cursor, u32, 0);
    writeInt(buf, &cursor, u32, 10);
    writeInt(buf, &cursor, u32, 3);
    writeInt(buf, &cursor, u32, @as(u32, @bitCast(@as(f32, 1.25))));
    writeInt(buf, &cursor, u16, @intCast(index_name.len));
    writeInt(buf, &cursor, u16, @intCast(field.len));
    writeInt(buf, &cursor, u32, @intCast(text.len));
    writeInt(buf, &cursor, u16, @intCast(analyzer.len));
    writeInt(buf, &cursor, u16, 2);
    writeInt(buf, &cursor, u16, 1);
    writeInt(buf, &cursor, u8, 0);
    writeInt(buf, &cursor, u8, 0);
    @memcpy(buf[cursor..][0..index_name.len], index_name);
    cursor += index_name.len;
    @memcpy(buf[cursor..][0..field.len], field);
    cursor += field.len;
    @memcpy(buf[cursor..][0..text.len], text);
    cursor += text.len;
    @memcpy(buf[cursor..][0..analyzer.len], analyzer);

    var req = try decodeTextMatchRequest(alloc, buf);
    defer freeTextMatchRequest(alloc, &req);
    try std.testing.expectEqualStrings(index_name, req.index_name);
    try std.testing.expectEqualStrings(field, req.field);
    try std.testing.expectEqualStrings(text, req.text);
    try std.testing.expectEqualStrings(analyzer, req.analyzer);
    try std.testing.expectEqual(@as(f32, 1.25), req.boost);
    try std.testing.expectEqual(@as(u32, 10), req.limit);
    try std.testing.expectEqual(@as(u32, 3), req.offset);
}

test "decode text term request" {
    const alloc = std.testing.allocator;
    const index_name = "full_text_index";
    const field = "content";
    const text = "alpha";
    const analyzer = "";
    const total_len = 4 + 2 + 2 + 4 + 4 + 4 + 4 + 2 + 2 + 4 + 2 + 2 + 2 + 1 + 1 + index_name.len + field.len + text.len + analyzer.len;
    const buf = try alloc.alloc(u8, total_len);
    defer alloc.free(buf);

    var cursor: usize = 0;
    writeInt(buf, &cursor, u32, magic);
    writeInt(buf, &cursor, u16, version);
    writeInt(buf, &cursor, u16, @intFromEnum(Op.text_term_search));
    writeInt(buf, &cursor, u32, 0);
    writeInt(buf, &cursor, u32, 10);
    writeInt(buf, &cursor, u32, 3);
    writeInt(buf, &cursor, u32, @as(u32, @bitCast(@as(f32, 2.5))));
    writeInt(buf, &cursor, u16, @intCast(index_name.len));
    writeInt(buf, &cursor, u16, @intCast(field.len));
    writeInt(buf, &cursor, u32, @intCast(text.len));
    writeInt(buf, &cursor, u16, @intCast(analyzer.len));
    writeInt(buf, &cursor, u16, 0);
    writeInt(buf, &cursor, u16, 0);
    writeInt(buf, &cursor, u8, 1);
    writeInt(buf, &cursor, u8, 0);
    @memcpy(buf[cursor..][0..index_name.len], index_name);
    cursor += index_name.len;
    @memcpy(buf[cursor..][0..field.len], field);
    cursor += field.len;
    @memcpy(buf[cursor..][0..text.len], text);

    var req = try decodeTextTermRequest(alloc, buf);
    defer freeTextTermRequest(alloc, &req);
    try std.testing.expectEqualStrings(index_name, req.index_name);
    try std.testing.expectEqualStrings(field, req.field);
    try std.testing.expectEqualStrings(text, req.text);
    try std.testing.expectEqualStrings(analyzer, req.analyzer);
    try std.testing.expectEqual(@as(f32, 2.5), req.boost);
    try std.testing.expectEqual(@as(u32, 10), req.limit);
    try std.testing.expectEqual(@as(u32, 3), req.offset);
}

test "decode text match phrase request" {
    const alloc = std.testing.allocator;
    const index_name = "full_text_index";
    const field = "content";
    const text = "alpha beta";
    const analyzer = "keyword";
    const total_len = 4 + 2 + 2 + 4 + 4 + 4 + 4 + 2 + 2 + 4 + 2 + 2 + 2 + 1 + 1 + index_name.len + field.len + text.len + analyzer.len;
    const buf = try alloc.alloc(u8, total_len);
    defer alloc.free(buf);

    var cursor: usize = 0;
    writeInt(buf, &cursor, u32, magic);
    writeInt(buf, &cursor, u16, version);
    writeInt(buf, &cursor, u16, @intFromEnum(Op.text_match_phrase_search));
    writeInt(buf, &cursor, u32, 0);
    writeInt(buf, &cursor, u32, 5);
    writeInt(buf, &cursor, u32, 1);
    writeInt(buf, &cursor, u32, @as(u32, @bitCast(@as(f32, 0.75))));
    writeInt(buf, &cursor, u16, @intCast(index_name.len));
    writeInt(buf, &cursor, u16, @intCast(field.len));
    writeInt(buf, &cursor, u32, @intCast(text.len));
    writeInt(buf, &cursor, u16, @intCast(analyzer.len));
    writeInt(buf, &cursor, u16, 0);
    writeInt(buf, &cursor, u16, 2);
    writeInt(buf, &cursor, u8, 2);
    writeInt(buf, &cursor, u8, 0);
    @memcpy(buf[cursor..][0..index_name.len], index_name);
    cursor += index_name.len;
    @memcpy(buf[cursor..][0..field.len], field);
    cursor += field.len;
    @memcpy(buf[cursor..][0..text.len], text);
    cursor += text.len;
    @memcpy(buf[cursor..][0..analyzer.len], analyzer);

    var req = try decodeTextMatchPhraseRequest(alloc, buf);
    defer freeTextMatchPhraseRequest(alloc, &req);
    try std.testing.expectEqualStrings(index_name, req.index_name);
    try std.testing.expectEqualStrings(field, req.field);
    try std.testing.expectEqualStrings(text, req.text);
    try std.testing.expectEqualStrings(analyzer, req.analyzer);
    try std.testing.expectEqual(@as(f32, 0.75), req.boost);
    try std.testing.expectEqual(@as(u32, 5), req.limit);
    try std.testing.expectEqual(@as(u32, 1), req.offset);
}
