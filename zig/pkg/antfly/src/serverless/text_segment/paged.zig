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

//! Copy-on-write text projections on the existing authenticated ordered tree.
//! A manifest binds two roots: document text and (escaped term, document ID)
//! postings. Publication touches changed rows; it never loads the full corpus.
const std = @import("std");
const A = std.mem.Allocator;
const tree = @import("../graph_segment/page_tree.zig");
const PageStore = @import("../graph_segment/page_store.zig").PageStore;
const artifacts = @import("../artifacts/mod.zig");
const manifest = @import("../manifest/mod.zig");
const t = @import("types.zig");
const indexed = @import("../query/indexed_reader.zig");
const request = @import("../query/request.zig");
const runtime = @import("../query/runtime.zig");
pub const metadata_version: u16 = 2;
pub const Root = struct {
    format: u8 = 1,
    domain: [32]u8,
    docs: ?tree.Ref = null,
    postings: ?tree.Ref = null,
    index_name: []const u8,
    source_name: []const u8,
    config_json: []const u8,
};

pub fn loadRoot(a: A, pages: *PageStore, ref: manifest.ArtifactRef) !std.json.Parsed(Root) {
    if (ref.metadata_version != metadata_version or ref.kind != .text_segment or ref.byte_len > 1024 * 1024) return error.InvalidPagedTextRoot;
    const bytes = try pages.artifacts.getVerifiedAllocWithCancellationUsingAllocator(a, ref.artifact_id, ref.byte_len, ref.checksum, pages.cancellation);
    defer a.free(bytes);
    const parsed = try std.json.parseFromSlice(Root, a, bytes, .{ .allocate = .alloc_always });
    errdefer parsed.deinit();
    if (parsed.value.format != 1 or !std.mem.eql(u8, &parsed.value.domain, &pages.domain)) return error.InvalidPagedTextRoot;
    if (parsed.value.docs == null and parsed.value.postings != null) return error.InvalidPagedTextRoot;
    if (ref.name.len != 0 and !std.mem.eql(u8, ref.name, parsed.value.index_name)) return error.InvalidPagedTextRoot;
    if (parsed.value.docs) |r| try r.validate();
    if (parsed.value.postings) |r| try r.validate();
    return parsed;
}

// Zero bytes are escaped so full-term and prefix ranges are unambiguous for
// arbitrary UTF-8 text and arbitrary binary-compatible document identities.
fn termKey(a: A, term: []const u8, id: ?[]const u8) ![]u8 {
    var out = std.ArrayList(u8).empty;
    errdefer out.deinit(a);
    for (term) |byte| {
        try out.append(a, byte);
        if (byte == 0) try out.append(a, 255);
    }
    if (id) |value| {
        try out.appendSlice(a, &.{ 0, 0 });
        try out.appendSlice(a, value);
    }
    return out.toOwnedSlice(a);
}
fn documentId(key: []const u8) ![]const u8 {
    var i: usize = 0;
    while (i < key.len) : (i += 1) if (key[i] == 0) {
        if (i + 1 >= key.len) return error.InvalidPagedTextPosting;
        if (key[i + 1] == 0) return if (i + 2 < key.len) key[i + 2 ..] else error.InvalidPagedTextPosting;
        if (key[i + 1] != 255) return error.InvalidPagedTextPosting;
        i += 1;
    };
    return error.InvalidPagedTextPosting;
}
fn upperBound(a: A, prefix: []const u8) !?[]u8 {
    var i = prefix.len;
    while (i != 0) {
        i -= 1;
        if (prefix[i] == 255) continue;
        const upper = try a.dupe(u8, prefix[0 .. i + 1]);
        upper[i] += 1;
        return upper;
    }
    return null;
}

pub fn publishAlloc(a: A, pages: *PageStore, prior: ?manifest.ArtifactRef, delta: t.Segment, removed: []const []const u8) !manifest.ArtifactRef {
    const source = if (prior) |ref| try loadRoot(a, pages, ref) else null;
    defer if (source) |parsed| parsed.deinit();
    var root = if (source) |parsed| parsed.value else Root{ .domain = pages.domain, .index_name = delta.index_name, .source_name = delta.source_name, .config_json = delta.config_json };
    if (!std.mem.eql(u8, root.index_name, delta.index_name) or !std.mem.eql(u8, root.source_name, delta.source_name) or !std.mem.eql(u8, root.config_json, delta.config_json)) return error.TextIndexDefinitionChanged;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var postings = std.StringHashMapUnmanaged(?[]const u8).empty;
    var docs = std.StringHashMapUnmanaged(?[]const u8).empty;
    // Repeated term and document prefixes compress well in posting pages.
    var text_pages = pages.*;
    text_pages.prefix_keys = true;
    var cache = tree.Cache{ .alloc = a, .underlying = text_pages.store() };
    defer cache.deinit();
    // Removing old terms requires only touched document projections.
    for (removed) |id| {
        try pages.cancellation.check();
        const entry = try docs.getOrPut(scratch, id);
        if (entry.found_existing) continue;
        entry.value_ptr.* = null;
        const old = try tree.lookupMany(a, cache.store(), root.docs, &.{id});
        defer {
            for (old) |record| if (record) |r| r.deinit(a);
            a.free(old);
        }
        if (old[0]) |record| {
            var tokens = std.mem.tokenizeScalar(u8, record.value, ' ');
            while (tokens.next()) |term| try postings.put(scratch, try termKey(scratch, term, id), null);
        }
    }
    for (delta.docs) |doc| try docs.put(scratch, doc.doc_id, doc.normalized_text);
    for (delta.terms) |term| for (term.postings) |posting| {
        try pages.cancellation.check();
        if (posting.doc_index >= delta.docs.len or posting.term_freq == 0) return error.InvalidPagedTextPosting;
        const value = try scratch.alloc(u8, 4);
        std.mem.writeInt(u32, value[0..4], posting.term_freq, .little);
        try postings.put(scratch, try termKey(scratch, term.term, delta.docs[posting.doc_index].doc_id), value);
    };
    root.docs = try applyMap(a, scratch, cache.store(), root.docs, docs);
    root.postings = try applyMap(a, scratch, cache.store(), root.postings, postings);
    return publishRoot(a, pages, root);
}

pub fn publishRoot(a: A, pages: *PageStore, root: Root) !manifest.ArtifactRef {
    const payload = try std.json.Stringify.valueAlloc(a, root, .{});
    defer a.free(payload);
    if (payload.len > 1024 * 1024) return error.PagedTextRootTooLarge;
    var stored = try pages.artifacts.put(payload);
    defer stored.deinit(pages.artifacts.allocator);
    const id = try a.dupe(u8, stored.artifact_id);
    errdefer a.free(id);
    const checksum = try a.dupe(u8, stored.checksum);
    errdefer a.free(checksum);
    return .{ .kind = .text_segment, .metadata_version = metadata_version, .name = try a.dupe(u8, root.index_name), .artifact_id = id, .checksum = checksum, .byte_len = stored.byte_len };
}
fn applyMap(a: A, scratch: A, store: tree.Store, prior: ?tree.Ref, map: std.StringHashMapUnmanaged(?[]const u8)) !?tree.Ref {
    const changes = try scratch.alloc(tree.Mutation, map.count());
    var iterator = map.iterator();
    var i: usize = 0;
    while (iterator.next()) |entry| : (i += 1) changes[i] = .{ .key = entry.key_ptr.*, .value = entry.value_ptr.* };
    std.mem.sort(tree.Mutation, changes, {}, struct {
        fn less(_: void, l: tree.Mutation, r: tree.Mutation) bool {
            return std.mem.lessThan(u8, l.key, r.key);
        }
    }.less);
    return tree.apply(a, store, prior, changes);
}

pub fn searchAlloc(a: A, session: *runtime.QuerySession, ref: manifest.ArtifactRef, req: request.QueryRequest) ![]indexed.ScoredDoc {
    var reads: u64 = 32 * 1024 * 1024;
    var writes: u64 = 0;
    var pages = PageStore{ .domain = PageStore.namespaceDomain(session.manifest.namespace), .artifacts = session.artifacts, .cancellation = session.cancellation, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
    const parsed = try loadRoot(a, &pages, ref);
    defer parsed.deinit();
    const root = parsed.value;
    if ((if (root.docs) |r| r.records else 0) != session.manifest.stats.document_count) return error.DocumentFactsSourceChanged;
    const normalized = try indexed.normalizeAlloc(a, req.text);
    defer a.free(normalized);
    var terms = std.ArrayList([]const u8).empty;
    defer terms.deinit(a);
    var tokens = std.mem.tokenizeScalar(u8, normalized, ' ');
    while (tokens.next()) |term| try terms.append(a, term);
    if (terms.items.len == 0) return a.alloc(indexed.ScoredDoc, 0);
    if (req.operator == .prefix_any_term) std.mem.sort([]const u8, terms.items, {}, struct {
        fn less(_: void, l: []const u8, r: []const u8) bool {
            return std.mem.lessThan(u8, l, r);
        }
    }.less);
    const Value = struct { score: u32 = 0, matched: usize = 0 };
    var scores = std.StringHashMapUnmanaged(Value).empty;
    defer {
        var keys = scores.keyIterator();
        while (keys.next()) |key| a.free(key.*);
        scores.deinit(a);
    }
    var cache = tree.Cache{ .alloc = a, .underlying = pages.store() };
    defer cache.deinit();
    var previous_prefix: ?[]const u8 = null;
    for (terms.items, 0..) |term, term_index| {
        if (req.operator == .phrase and term_index != 0) break;
        if (req.operator == .prefix_any_term) {
            if (previous_prefix) |previous| if (std.mem.startsWith(u8, term, previous)) continue;
            previous_prefix = term;
        }
        const lower = try termKey(a, term, if (req.operator == .prefix_any_term) null else "");
        defer a.free(lower);
        const upper = try upperBound(a, lower);
        defer if (upper) |value| a.free(value);
        var cursor = try tree.Cursor.init(a, cache.store(), root.postings, lower, upper);
        defer cursor.deinit();
        while (try cursor.next()) |record| {
            try session.checkCancellation();
            if (record.value.len != 4) return error.InvalidPagedTextPosting;
            const frequency = std.mem.readInt(u32, record.value[0..4], .little);
            if (frequency == 0) return error.InvalidPagedTextPosting;
            const id = try documentId(record.key);
            if (scores.getPtr(id)) |value| {
                value.score = try std.math.add(u32, value.score, frequency);
                value.matched += 1;
            } else {
                const owned = try a.dupe(u8, id);
                errdefer a.free(owned);
                try scores.put(a, owned, .{ .score = frequency, .matched = 1 });
            }
        }
    }
    var selected = @import("../query/top_docs.zig").TopDocs{ .capacity = req.offset +| req.limit };
    defer selected.deinit(a); // identities are borrowed from the score map
    var iterator = scores.iterator();
    while (iterator.next()) |entry| {
        try session.checkCancellation();
        var score = entry.value_ptr.score;
        if (req.operator == .all_terms) {
            if (entry.value_ptr.matched != terms.items.len) continue;
            score = try std.math.add(u32, score, std.math.cast(u32, terms.items.len * 10) orelse return error.QueryCandidateBudgetExceeded);
        }
        if (req.operator == .phrase) {
            const document = try tree.lookupMany(a, cache.store(), root.docs, &.{entry.key_ptr.*});
            defer {
                for (document) |record| if (record) |r| r.deinit(a);
                a.free(document);
            }
            const row = document[0] orelse return error.InvalidPagedTextPosting;
            if (std.mem.indexOf(u8, row.value, normalized) == null) continue;
            score = try std.math.add(u32, score, 100);
        }
        if (score >= req.min_score) try selected.add(a, .{ .doc_id = entry.key_ptr.*, .score = score });
    }
    const ranked = selected.sorted();
    const start = @min(req.offset, ranked.len);
    const end = @min(ranked.len, start +| req.limit);
    const result = try a.alloc(indexed.ScoredDoc, end - start);
    var initialized: usize = 0;
    errdefer {
        for (result[0..initialized]) |hit| a.free(hit.doc_id);
        a.free(result);
    }
    for (ranked[start..end], result) |hit, *owned| {
        owned.* = .{ .doc_id = try a.dupe(u8, hit.doc_id), .score = hit.score };
        initialized += 1;
    }
    return result;
}

test "serverless paged text matches flat operators and preserves prior roots on replacement" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/paged-text", .{tmp.sub_path});
    defer a.free(path);
    var fs = try @import("../artifacts/fs_store.zig").FsStore.init(a, path);
    var store = fs.artifactStore();
    defer store.deinit();
    var reads: u64 = 16 * 1024 * 1024;
    var writes: u64 = 16 * 1024 * 1024;
    var pages = PageStore{ .domain = PageStore.namespaceDomain("docs"), .attempt = @splat(1), .artifacts = &store, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
    const builder = @import("../build/builder.zig");
    const Doc = @import("../query/materializer.zig").Document;
    const docs = [_]Doc{
        .{ .doc_id = @constCast("a"), .body = @constCast("{\"text\":\"alpha alpha bravo\"}"), .last_lsn = 1, .last_timestamp_ns = 1 },
        .{ .doc_id = @constCast("b"), .body = @constCast("{\"text\":\"alpha charlie\"}"), .last_lsn = 2, .last_timestamp_ns = 2 },
        .{ .doc_id = @constCast("c"), .body = @constCast("{\"text\":\"alpine\"}"), .last_lsn = 3, .last_timestamp_ns = 3 },
        .{ .doc_id = @constCast("n\x00id"), .body = @constCast("{\"text\":\"nul\\u0000word\"}"), .last_lsn = 4, .last_timestamp_ns = 4 },
    };
    const spec = @import("antfly_local_sources").api_full_text_indexes.FullTextIndexSpec{ .name = @constCast("fts"), .config_json = @constCast("{}") };
    var flat = try builder.allocTextSegmentAlloc(a, &docs, spec, null);
    defer flat.deinit(a);
    const ref = try publishAlloc(a, &pages, null, flat, &.{});
    defer builder.freeArtifactRef(a, ref);
    var session = runtime.QuerySession{ .alloc = a, .artifacts = &store, .owns_manifest = false, .manifest = .{ .namespace = "docs", .version = 1, .built_at_ns = 1, .wal_start_lsn = 1, .wal_end_lsn = 4, .stats = .{ .document_count = 4 }, .artifacts = &.{} } };
    for ([_]request.QueryOperator{ .any_terms, .all_terms, .phrase, .prefix_any_term }) |operator| {
        const text = if (operator == .prefix_any_term) "al alp" else "alpha bravo";
        const expected = try indexed.searchTextSegmentDocIdsWithCancellationAlloc(a, flat, text, operator, 0, 100, 0, .none);
        defer indexed.freeScoredDocs(a, expected);
        const actual = try searchAlloc(a, &session, ref, .{ .text = @constCast(text), .operator = operator, .limit = 100 });
        defer indexed.freeScoredDocs(a, actual);
        try std.testing.expectEqual(expected.len, actual.len);
        for (expected, actual) |left, right| {
            try std.testing.expectEqualStrings(left.doc_id, right.doc_id);
            try std.testing.expectEqual(left.score, right.score);
        }
    }
    const nul = try searchAlloc(a, &session, ref, .{ .text = @constCast("nul\x00word"), .limit = 10 });
    defer indexed.freeScoredDocs(a, nul);
    try std.testing.expectEqualStrings("n\x00id", nul[0].doc_id);
    const changed = [_]Doc{
        .{ .doc_id = @constCast("a"), .body = @constCast("{\"text\":\"bravo\"}"), .last_lsn = 5, .last_timestamp_ns = 5 },
        .{ .doc_id = @constCast("d"), .body = @constCast("{\"text\":\"alpha\"}"), .last_lsn = 6, .last_timestamp_ns = 6 },
    };
    var delta = try builder.allocTextSegmentAlloc(a, &changed, spec, null);
    defer delta.deinit(a);
    pages.attempt = @splat(2);
    const updated = try publishAlloc(a, &pages, ref, delta, &.{ "a", "b", "d" });
    defer builder.freeArtifactRef(a, updated);
    const fresh = try searchAlloc(a, &session, updated, .{ .text = @constCast("alpha"), .limit = 10 });
    defer indexed.freeScoredDocs(a, fresh);
    try std.testing.expectEqual(@as(usize, 1), fresh.len);
    try std.testing.expectEqualStrings("d", fresh[0].doc_id);
    const old = try searchAlloc(a, &session, ref, .{ .text = @constCast("alpha"), .limit = 10 });
    defer indexed.freeScoredDocs(a, old);
    try std.testing.expectEqual(@as(usize, 2), old.len);
}
