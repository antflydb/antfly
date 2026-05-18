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
const sparse_types = @import("types.zig");

pub const Header = struct {
    doc_count: u32,
    term_count: u32,
    docs_len: u32,
    terms_blob_len: u32,
};

pub const TermRecord = struct {
    term_offset: u32,
    term_len: u32,
    postings_offset: u32,
    postings_len: u32,
    doc_freq: u32,
};

const header_len: usize = 16;
const term_record_len: usize = 20;

pub fn encodeAlloc(alloc: Allocator, segment: sparse_types.Segment) ![]u8 {
    var docs_len: usize = 0;
    for (segment.docs) |doc| {
        docs_len += 4 + doc.doc_id.len;
        docs_len += 4;
    }

    var terms_blob_len: usize = 0;
    var postings_blob_len: usize = 0;
    for (segment.terms) |term| {
        terms_blob_len += term.term.len;
        postings_blob_len += term.postings.len * 8;
    }

    const table_len = segment.terms.len * term_record_len;
    const total_len = header_len + docs_len + table_len + terms_blob_len + postings_blob_len;
    const buf = try alloc.alloc(u8, total_len);

    const postings_base: usize = header_len + docs_len + table_len + terms_blob_len;

    var pos: usize = 0;
    std.mem.writeInt(u32, buf[pos..][0..4], @intCast(segment.docs.len), .little);
    pos += 4;
    std.mem.writeInt(u32, buf[pos..][0..4], @intCast(segment.terms.len), .little);
    pos += 4;
    std.mem.writeInt(u32, buf[pos..][0..4], @intCast(docs_len), .little);
    pos += 4;
    std.mem.writeInt(u32, buf[pos..][0..4], @intCast(terms_blob_len), .little);
    pos += 4;

    for (segment.docs) |doc| {
        std.mem.writeInt(u32, buf[pos..][0..4], @intCast(doc.doc_id.len), .little);
        pos += 4;
        @memcpy(buf[pos..][0..doc.doc_id.len], doc.doc_id);
        pos += doc.doc_id.len;
        std.mem.writeInt(u32, buf[pos..][0..4], doc.feature_count, .little);
        pos += 4;
    }

    const table_pos = pos;
    pos += table_len;
    const terms_blob_pos = pos;
    var term_blob_cursor: usize = 0;
    var postings_cursor: usize = 0;

    for (segment.terms, 0..) |term, term_index| {
        const record_pos = table_pos + term_index * term_record_len;
        std.mem.writeInt(u32, buf[record_pos..][0..4], @intCast(term_blob_cursor), .little);
        std.mem.writeInt(u32, buf[record_pos + 4 ..][0..4], @intCast(term.term.len), .little);
        std.mem.writeInt(u32, buf[record_pos + 8 ..][0..4], @intCast(postings_base + postings_cursor), .little);
        std.mem.writeInt(u32, buf[record_pos + 12 ..][0..4], @intCast(term.postings.len * 8), .little);
        std.mem.writeInt(u32, buf[record_pos + 16 ..][0..4], @intCast(term.postings.len), .little);

        @memcpy(buf[terms_blob_pos + term_blob_cursor ..][0..term.term.len], term.term);
        term_blob_cursor += term.term.len;

        const postings_pos = postings_base + postings_cursor;
        for (term.postings, 0..) |posting, posting_index| {
            const entry_pos = postings_pos + posting_index * 8;
            std.mem.writeInt(u32, buf[entry_pos..][0..4], posting.doc_index, .little);
            std.mem.writeInt(u32, buf[entry_pos + 4 ..][0..4], @bitCast(posting.weight), .little);
        }
        postings_cursor += term.postings.len * 8;
    }

    return buf;
}

pub fn decodeHeader(payload: []const u8) !Header {
    if (payload.len < header_len) return error.InvalidSparseSegmentPayload;
    return .{
        .doc_count = std.mem.readInt(u32, payload[0..4], .little),
        .term_count = std.mem.readInt(u32, payload[4..8], .little),
        .docs_len = std.mem.readInt(u32, payload[8..12], .little),
        .terms_blob_len = std.mem.readInt(u32, payload[12..16], .little),
    };
}

pub fn termRecordLen() usize {
    return term_record_len;
}

pub fn decodeDocsAlloc(alloc: Allocator, doc_count: u32, payload: []const u8) ![]sparse_types.DocumentEntry {
    const docs = try alloc.alloc(sparse_types.DocumentEntry, doc_count);
    errdefer alloc.free(docs);
    var initialized: usize = 0;
    errdefer {
        for (docs[0..initialized]) |*doc| doc.deinit(alloc);
    }

    var pos: usize = 0;
    for (0..doc_count) |idx| {
        if (pos + 4 > payload.len) return error.InvalidSparseSegmentPayload;
        const doc_id_len = std.mem.readInt(u32, payload[pos..][0..4], .little);
        pos += 4;
        if (pos + doc_id_len + 4 > payload.len) return error.InvalidSparseSegmentPayload;
        docs[idx] = .{
            .doc_id = try alloc.dupe(u8, payload[pos .. pos + doc_id_len]),
            .feature_count = std.mem.readInt(u32, payload[pos + doc_id_len ..][0..4], .little),
        };
        pos += doc_id_len + 4;
        initialized += 1;
    }
    if (pos != payload.len) return error.InvalidSparseSegmentPayload;
    return docs;
}

pub fn decodeTermTableAlloc(alloc: Allocator, term_count: u32, payload: []const u8) ![]TermRecord {
    if (payload.len != @as(usize, term_count) * term_record_len) return error.InvalidSparseSegmentPayload;
    const records = try alloc.alloc(TermRecord, term_count);
    errdefer alloc.free(records);
    for (0..term_count) |idx| {
        const pos = idx * term_record_len;
        records[idx] = .{
            .term_offset = std.mem.readInt(u32, payload[pos..][0..4], .little),
            .term_len = std.mem.readInt(u32, payload[pos + 4 ..][0..4], .little),
            .postings_offset = std.mem.readInt(u32, payload[pos + 8 ..][0..4], .little),
            .postings_len = std.mem.readInt(u32, payload[pos + 12 ..][0..4], .little),
            .doc_freq = std.mem.readInt(u32, payload[pos + 16 ..][0..4], .little),
        };
    }
    return records;
}

pub fn decodePostingBlockAlloc(alloc: Allocator, doc_freq: u32, payload: []const u8) ![]sparse_types.Posting {
    if (payload.len != @as(usize, doc_freq) * 8) return error.InvalidSparseSegmentPayload;
    const postings = try alloc.alloc(sparse_types.Posting, doc_freq);
    errdefer alloc.free(postings);
    for (0..doc_freq) |idx| {
        const pos = idx * 8;
        postings[idx] = .{
            .doc_index = std.mem.readInt(u32, payload[pos..][0..4], .little),
            .weight = @bitCast(std.mem.readInt(u32, payload[pos + 4 ..][0..4], .little)),
        };
    }
    return postings;
}

pub fn termBytes(terms_blob: []const u8, record: TermRecord) ![]const u8 {
    const start: usize = record.term_offset;
    const end = start + record.term_len;
    if (end > terms_blob.len) return error.InvalidSparseSegmentPayload;
    return terms_blob[start..end];
}

pub fn decodeAlloc(alloc: Allocator, payload: []const u8) !sparse_types.Segment {
    const header = try decodeHeader(payload);
    const docs_offset = header_len;
    const docs_end = docs_offset + header.docs_len;
    if (docs_end > payload.len) return error.InvalidSparseSegmentPayload;
    const docs = try decodeDocsAlloc(alloc, header.doc_count, payload[docs_offset..docs_end]);
    errdefer {
        for (docs) |*doc| doc.deinit(alloc);
        alloc.free(docs);
    }

    const table_offset = docs_end;
    const table_len = termRecordLen() * @as(usize, @intCast(header.term_count));
    const table_end = table_offset + table_len;
    if (table_end > payload.len) return error.InvalidSparseSegmentPayload;
    const records = try decodeTermTableAlloc(alloc, header.term_count, payload[table_offset..table_end]);
    defer alloc.free(records);

    const terms_blob_offset = table_end;
    const terms_blob_end = terms_blob_offset + header.terms_blob_len;
    if (terms_blob_end > payload.len) return error.InvalidSparseSegmentPayload;
    const terms_blob = payload[terms_blob_offset..terms_blob_end];

    const terms = try alloc.alloc(sparse_types.TermEntry, header.term_count);
    errdefer alloc.free(terms);
    var initialized: usize = 0;
    errdefer {
        for (terms[0..initialized]) |*term| term.deinit(alloc);
    }

    for (records, 0..) |record, idx| {
        const term = try alloc.dupe(u8, try termBytes(terms_blob, record));
        errdefer alloc.free(term);
        if (@as(usize, record.postings_offset) + record.postings_len > payload.len) return error.InvalidSparseSegmentPayload;
        const postings = try decodePostingBlockAlloc(
            alloc,
            record.doc_freq,
            payload[record.postings_offset .. @as(usize, record.postings_offset) + record.postings_len],
        );
        terms[idx] = .{
            .term = term,
            .postings = postings,
        };
        initialized += 1;
    }

    return .{
        .docs = docs,
        .terms = terms,
    };
}

test "sparse segment codec round-trips weighted postings" {
    const alloc = std.testing.allocator;
    var segment = sparse_types.Segment{
        .docs = try alloc.alloc(sparse_types.DocumentEntry, 2),
        .terms = try alloc.alloc(sparse_types.TermEntry, 2),
    };
    defer sparse_types.freeSegment(alloc, &segment);

    segment.docs[0] = .{ .doc_id = try alloc.dupe(u8, "doc-a"), .feature_count = 2 };
    segment.docs[1] = .{ .doc_id = try alloc.dupe(u8, "doc-b"), .feature_count = 1 };
    segment.terms[0] = .{
        .term = try alloc.dupe(u8, "alpha"),
        .postings = try alloc.dupe(sparse_types.Posting, &.{.{ .doc_index = 0, .weight = 1.0 }}),
    };
    segment.terms[1] = .{
        .term = try alloc.dupe(u8, "bravo"),
        .postings = try alloc.dupe(sparse_types.Posting, &.{
            .{ .doc_index = 0, .weight = 0.5 },
            .{ .doc_index = 1, .weight = 2.0 },
        }),
    };

    const encoded = try encodeAlloc(alloc, segment);
    defer alloc.free(encoded);

    const header = try decodeHeader(encoded[0..header_len]);
    try std.testing.expectEqual(@as(u32, 2), header.doc_count);
    try std.testing.expectEqual(@as(u32, 2), header.term_count);

    var decoded = try decodeAlloc(alloc, encoded);
    defer sparse_types.freeSegment(alloc, &decoded);
    try std.testing.expectEqual(@as(usize, 2), decoded.docs.len);
    try std.testing.expectEqual(@as(usize, 2), decoded.terms.len);
    try std.testing.expectEqualStrings("alpha", decoded.terms[0].term);
    try std.testing.expectEqual(@as(f32, 2.0), decoded.terms[1].postings[1].weight);
}

test "sparse segment term table decodes postings offsets" {
    const alloc = std.testing.allocator;
    var segment = sparse_types.Segment{
        .docs = try alloc.alloc(sparse_types.DocumentEntry, 1),
        .terms = try alloc.alloc(sparse_types.TermEntry, 1),
    };
    defer sparse_types.freeSegment(alloc, &segment);
    segment.docs[0] = .{ .doc_id = try alloc.dupe(u8, "doc-a"), .feature_count = 1 };
    segment.terms[0] = .{
        .term = try alloc.dupe(u8, "alpha"),
        .postings = try alloc.dupe(sparse_types.Posting, &.{.{ .doc_index = 0, .weight = 3.0 }}),
    };

    const encoded = try encodeAlloc(alloc, segment);
    defer alloc.free(encoded);

    const header = try decodeHeader(encoded[0..header_len]);
    const docs_offset = header_len;
    const table_offset = docs_offset + header.docs_len;
    const records = try decodeTermTableAlloc(alloc, header.term_count, encoded[table_offset .. table_offset + termRecordLen()]);
    defer alloc.free(records);
    const terms_blob_offset = table_offset + termRecordLen();
    const term = try termBytes(encoded[terms_blob_offset .. terms_blob_offset + header.terms_blob_len], records[0]);
    try std.testing.expectEqualStrings("alpha", term);
    const postings = try decodePostingBlockAlloc(
        alloc,
        records[0].doc_freq,
        encoded[records[0].postings_offset .. records[0].postings_offset + records[0].postings_len],
    );
    defer alloc.free(postings);
    try std.testing.expectEqual(@as(f32, 3.0), postings[0].weight);
}
