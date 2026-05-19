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
const text_types = @import("types.zig");

const v2_magic = 0x32545854;

pub fn encodeAlloc(alloc: Allocator, segment: text_types.Segment) ![]u8 {
    var total_len: usize = 20;
    total_len += 4 + segment.index_name.len;
    total_len += 4 + segment.source_name.len;
    total_len += 4 + segment.config_json.len;
    for (segment.docs) |doc| {
        total_len += 4 + doc.doc_id.len;
        total_len += 4 + doc.normalized_text.len;
        total_len += 4;
    }
    for (segment.terms) |term| {
        total_len += 4 + term.term.len;
        total_len += 4;
        total_len += term.postings.len * 8;
    }

    const buf = try alloc.alloc(u8, total_len);
    var pos: usize = 0;
    std.mem.writeInt(u32, buf[pos..][0..4], v2_magic, .little);
    pos += 4;
    std.mem.writeInt(u32, buf[pos..][0..4], 2, .little);
    pos += 4;
    std.mem.writeInt(u32, buf[pos..][0..4], @intCast(segment.index_name.len), .little);
    pos += 4;
    @memcpy(buf[pos..][0..segment.index_name.len], segment.index_name);
    pos += segment.index_name.len;
    std.mem.writeInt(u32, buf[pos..][0..4], @intCast(segment.source_name.len), .little);
    pos += 4;
    @memcpy(buf[pos..][0..segment.source_name.len], segment.source_name);
    pos += segment.source_name.len;
    std.mem.writeInt(u32, buf[pos..][0..4], @intCast(segment.config_json.len), .little);
    pos += 4;
    @memcpy(buf[pos..][0..segment.config_json.len], segment.config_json);
    pos += segment.config_json.len;
    std.mem.writeInt(u32, buf[pos..][0..4], @intCast(segment.docs.len), .little);
    pos += 4;
    std.mem.writeInt(u32, buf[pos..][0..4], @intCast(segment.terms.len), .little);
    pos += 4;

    for (segment.docs) |doc| {
        std.mem.writeInt(u32, buf[pos..][0..4], @intCast(doc.doc_id.len), .little);
        pos += 4;
        @memcpy(buf[pos..][0..doc.doc_id.len], doc.doc_id);
        pos += doc.doc_id.len;

        std.mem.writeInt(u32, buf[pos..][0..4], @intCast(doc.normalized_text.len), .little);
        pos += 4;
        @memcpy(buf[pos..][0..doc.normalized_text.len], doc.normalized_text);
        pos += doc.normalized_text.len;

        std.mem.writeInt(u32, buf[pos..][0..4], doc.token_count, .little);
        pos += 4;
    }

    for (segment.terms) |term| {
        std.mem.writeInt(u32, buf[pos..][0..4], @intCast(term.term.len), .little);
        pos += 4;
        @memcpy(buf[pos..][0..term.term.len], term.term);
        pos += term.term.len;

        std.mem.writeInt(u32, buf[pos..][0..4], @intCast(term.postings.len), .little);
        pos += 4;
        for (term.postings) |posting| {
            std.mem.writeInt(u32, buf[pos..][0..4], posting.doc_index, .little);
            pos += 4;
            std.mem.writeInt(u32, buf[pos..][0..4], posting.term_freq, .little);
            pos += 4;
        }
    }

    return buf;
}

pub fn decodeAlloc(alloc: Allocator, payload: []const u8) !text_types.Segment {
    if (payload.len >= 8 and std.mem.readInt(u32, payload[0..4], .little) == v2_magic) {
        return try decodeV2Alloc(alloc, payload);
    }
    return try decodeV1Alloc(alloc, payload);
}

fn decodeV1Alloc(alloc: Allocator, payload: []const u8) !text_types.Segment {
    if (payload.len < 8) return error.InvalidTextSegmentPayload;
    var pos: usize = 0;
    const doc_count = std.mem.readInt(u32, payload[pos..][0..4], .little);
    pos += 4;
    const term_count = std.mem.readInt(u32, payload[pos..][0..4], .little);
    pos += 4;

    const docs = try alloc.alloc(text_types.DocumentEntry, doc_count);
    errdefer alloc.free(docs);
    var docs_initialized: usize = 0;
    errdefer {
        for (docs[0..docs_initialized]) |*doc| doc.deinit(alloc);
    }

    for (0..doc_count) |idx| {
        if (pos + 4 > payload.len) return error.InvalidTextSegmentPayload;
        const doc_id_len = std.mem.readInt(u32, payload[pos..][0..4], .little);
        pos += 4;
        if (pos + doc_id_len + 4 + 4 > payload.len) return error.InvalidTextSegmentPayload;
        const doc_id = try alloc.dupe(u8, payload[pos .. pos + doc_id_len]);
        pos += doc_id_len;
        errdefer alloc.free(doc_id);

        const text_len = std.mem.readInt(u32, payload[pos..][0..4], .little);
        pos += 4;
        if (pos + text_len + 4 > payload.len) return error.InvalidTextSegmentPayload;
        const normalized_text = try alloc.dupe(u8, payload[pos .. pos + text_len]);
        pos += text_len;
        errdefer alloc.free(normalized_text);

        const token_count = std.mem.readInt(u32, payload[pos..][0..4], .little);
        pos += 4;

        docs[idx] = .{
            .doc_id = doc_id,
            .normalized_text = normalized_text,
            .token_count = token_count,
        };
        docs_initialized += 1;
    }

    const terms = try alloc.alloc(text_types.TermEntry, term_count);
    errdefer alloc.free(terms);
    var terms_initialized: usize = 0;
    errdefer {
        for (terms[0..terms_initialized]) |*term| term.deinit(alloc);
    }

    for (0..term_count) |idx| {
        if (pos + 4 > payload.len) return error.InvalidTextSegmentPayload;
        const term_len = std.mem.readInt(u32, payload[pos..][0..4], .little);
        pos += 4;
        if (pos + term_len + 4 > payload.len) return error.InvalidTextSegmentPayload;
        const term = try alloc.dupe(u8, payload[pos .. pos + term_len]);
        pos += term_len;
        errdefer alloc.free(term);

        const posting_count = std.mem.readInt(u32, payload[pos..][0..4], .little);
        pos += 4;
        if (pos + (@as(usize, posting_count) * 8) > payload.len) return error.InvalidTextSegmentPayload;
        const postings = try alloc.alloc(text_types.Posting, posting_count);
        errdefer alloc.free(postings);
        for (0..posting_count) |posting_idx| {
            postings[posting_idx] = .{
                .doc_index = std.mem.readInt(u32, payload[pos..][0..4], .little),
                .term_freq = std.mem.readInt(u32, payload[pos + 4 ..][0..4], .little),
            };
            pos += 8;
        }

        terms[idx] = .{
            .term = term,
            .postings = postings,
        };
        terms_initialized += 1;
    }

    return .{
        .index_name = &.{},
        .source_name = &.{},
        .config_json = &.{},
        .docs = docs,
        .terms = terms,
    };
}

fn decodeV2Alloc(alloc: Allocator, payload: []const u8) !text_types.Segment {
    if (payload.len < 20) return error.InvalidTextSegmentPayload;
    var pos: usize = 0;
    _ = std.mem.readInt(u32, payload[pos..][0..4], .little);
    pos += 4;
    const version = std.mem.readInt(u32, payload[pos..][0..4], .little);
    pos += 4;
    if (version != 2) return error.InvalidTextSegmentPayload;

    const index_name = try decodeOwnedBytesAlloc(alloc, payload, &pos);
    errdefer if (index_name.len > 0) alloc.free(index_name);
    const source_name = try decodeOwnedBytesAlloc(alloc, payload, &pos);
    errdefer if (source_name.len > 0) alloc.free(source_name);
    const config_json = try decodeOwnedBytesAlloc(alloc, payload, &pos);
    errdefer if (config_json.len > 0) alloc.free(config_json);

    if (pos + 8 > payload.len) return error.InvalidTextSegmentPayload;
    const doc_count = std.mem.readInt(u32, payload[pos..][0..4], .little);
    pos += 4;
    const term_count = std.mem.readInt(u32, payload[pos..][0..4], .little);
    pos += 4;

    const docs = try alloc.alloc(text_types.DocumentEntry, doc_count);
    errdefer alloc.free(docs);
    var docs_initialized: usize = 0;
    errdefer {
        for (docs[0..docs_initialized]) |*doc| doc.deinit(alloc);
    }

    for (0..doc_count) |idx| {
        const doc_id = try decodeOwnedBytesAlloc(alloc, payload, &pos);
        errdefer alloc.free(doc_id);
        const normalized_text = try decodeOwnedBytesAlloc(alloc, payload, &pos);
        errdefer alloc.free(normalized_text);
        if (pos + 4 > payload.len) return error.InvalidTextSegmentPayload;
        const token_count = std.mem.readInt(u32, payload[pos..][0..4], .little);
        pos += 4;

        docs[idx] = .{
            .doc_id = doc_id,
            .normalized_text = normalized_text,
            .token_count = token_count,
        };
        docs_initialized += 1;
    }

    const terms = try alloc.alloc(text_types.TermEntry, term_count);
    errdefer alloc.free(terms);
    var terms_initialized: usize = 0;
    errdefer {
        for (terms[0..terms_initialized]) |*term| term.deinit(alloc);
    }

    for (0..term_count) |idx| {
        const term = try decodeOwnedBytesAlloc(alloc, payload, &pos);
        errdefer alloc.free(term);
        if (pos + 4 > payload.len) return error.InvalidTextSegmentPayload;
        const posting_count = std.mem.readInt(u32, payload[pos..][0..4], .little);
        pos += 4;
        if (pos + (@as(usize, posting_count) * 8) > payload.len) return error.InvalidTextSegmentPayload;
        const postings = try alloc.alloc(text_types.Posting, posting_count);
        errdefer alloc.free(postings);
        for (0..posting_count) |posting_idx| {
            postings[posting_idx] = .{
                .doc_index = std.mem.readInt(u32, payload[pos..][0..4], .little),
                .term_freq = std.mem.readInt(u32, payload[pos + 4 ..][0..4], .little),
            };
            pos += 8;
        }

        terms[idx] = .{
            .term = term,
            .postings = postings,
        };
        terms_initialized += 1;
    }

    return .{
        .index_name = index_name,
        .source_name = source_name,
        .config_json = config_json,
        .docs = docs,
        .terms = terms,
    };
}

fn decodeOwnedBytesAlloc(alloc: Allocator, payload: []const u8, pos: *usize) ![]u8 {
    if (pos.* + 4 > payload.len) return error.InvalidTextSegmentPayload;
    const len = std.mem.readInt(u32, payload[pos.*..][0..4], .little);
    pos.* += 4;
    if (pos.* + len > payload.len) return error.InvalidTextSegmentPayload;
    const out = try alloc.dupe(u8, payload[pos.* .. pos.* + len]);
    pos.* += len;
    return out;
}

test "text segment codec round-trips indexed segment" {
    const alloc = std.testing.allocator;
    var segment = text_types.Segment{
        .index_name = try alloc.dupe(u8, "full_text_index_v0"),
        .source_name = try alloc.dupe(u8, "text"),
        .config_json = try alloc.dupe(u8, "{\"type\":\"full_text\"}"),
        .docs = try alloc.alloc(text_types.DocumentEntry, 2),
        .terms = try alloc.alloc(text_types.TermEntry, 2),
    };
    defer text_types.freeSegment(alloc, &segment);

    segment.docs[0] = .{
        .doc_id = try alloc.dupe(u8, "doc-a"),
        .normalized_text = try alloc.dupe(u8, "alpha bravo"),
        .token_count = 2,
    };
    segment.docs[1] = .{
        .doc_id = try alloc.dupe(u8, "doc-b"),
        .normalized_text = try alloc.dupe(u8, "bravo charlie"),
        .token_count = 2,
    };

    segment.terms[0] = .{
        .term = try alloc.dupe(u8, "alpha"),
        .postings = try alloc.alloc(text_types.Posting, 1),
    };
    segment.terms[0].postings[0] = .{ .doc_index = 0, .term_freq = 1 };
    segment.terms[1] = .{
        .term = try alloc.dupe(u8, "bravo"),
        .postings = try alloc.alloc(text_types.Posting, 2),
    };
    segment.terms[1].postings[0] = .{ .doc_index = 0, .term_freq = 1 };
    segment.terms[1].postings[1] = .{ .doc_index = 1, .term_freq = 1 };

    const encoded = try encodeAlloc(alloc, segment);
    defer alloc.free(encoded);

    var decoded = try decodeAlloc(alloc, encoded);
    defer text_types.freeSegment(alloc, &decoded);
    try std.testing.expectEqual(@as(usize, 2), decoded.docs.len);
    try std.testing.expectEqual(@as(usize, 2), decoded.terms.len);
    try std.testing.expectEqualStrings("full_text_index_v0", decoded.index_name);
    try std.testing.expectEqualStrings("text", decoded.source_name);
    try std.testing.expectEqualStrings("doc-a", decoded.docs[0].doc_id);
    try std.testing.expectEqualStrings("alpha bravo", decoded.docs[0].normalized_text);
    try std.testing.expectEqualStrings("bravo", decoded.terms[1].term);
    try std.testing.expectEqual(@as(u32, 1), decoded.terms[1].postings[1].doc_index);
}
