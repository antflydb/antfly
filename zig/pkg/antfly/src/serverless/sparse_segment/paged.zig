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

//! Sparse projections and term postings updated through native copy-on-write pages.
const std = @import("std");
const common = @import("../paged_projection.zig");
const tree = @import("../graph_segment/page_tree.zig");
const PageStore = @import("../graph_segment/page_store.zig").PageStore;
const manifest = @import("../manifest/mod.zig");
const runtime = @import("../query/runtime.zig");
const indexed = @import("../query/indexed_reader.zig");
const request = @import("../query/request.zig");
const types = @import("types.zig");
const A = std.mem.Allocator;
const Feature = struct { term: []const u8, weight: f32 };

pub fn publishAlloc(a: A, pages: *PageStore, prior: ?manifest.ArtifactRef, delta: ?types.Segment, removed: []const []const u8, name: []const u8, fingerprint: [32]u8) !manifest.ArtifactRef {
    const parsed = if (prior) |ref| try common.load(a, pages, ref) else null;
    defer if (parsed) |value| value.deinit();
    var root = if (parsed) |value| value.value else common.Root{ .kind = .sparse_segment, .domain = pages.domain, .index_name = name, .source_fingerprint = fingerprint };
    if (root.kind != .sparse_segment or !std.mem.eql(u8, root.index_name, name) or !std.mem.eql(u8, &root.source_fingerprint, &fingerprint)) return error.SparseIndexDefinitionChanged;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var docs = std.StringHashMapUnmanaged(?[]const u8).empty;
    var postings = std.StringHashMapUnmanaged(?[]const u8).empty;
    var text_pages = pages.*;
    text_pages.prefix_keys = true;
    var cache = tree.Cache{ .alloc = a, .underlying = text_pages.store() };
    defer cache.deinit();
    for (removed) |id| {
        try pages.cancellation.check();
        const slot = try docs.getOrPut(scratch, id);
        if (slot.found_existing) continue;
        slot.value_ptr.* = null;
        const old = try tree.lookupMany(a, cache.store(), root.docs, &.{id});
        defer {
            for (old) |row| if (row) |value| value.deinit(a);
            a.free(old);
        }
        if (old[0]) |record| {
            const features = try std.json.parseFromSlice([]Feature, a, record.value, .{});
            defer features.deinit();
            for (features.value) |feature| try postings.put(scratch, try common.postingKey(scratch, feature.term, id), null);
        }
    }
    if (delta) |segment| {
        const features = try scratch.alloc(std.ArrayList(Feature), segment.docs.len);
        @memset(features, .empty);
        var weights = std.StringHashMapUnmanaged(std.ArrayList(u8)).empty;
        for (segment.terms) |term| for (term.postings) |posting| {
            try pages.cancellation.check();
            if (posting.doc_index >= features.len or !std.math.isFinite(posting.weight)) return error.InvalidPagedProjectionPosting;
            const id = segment.docs[posting.doc_index].doc_id;
            try features[posting.doc_index].append(scratch, .{ .term = term.term, .weight = posting.weight });
            const slot = try weights.getOrPut(scratch, try common.postingKey(scratch, term.term, id));
            if (!slot.found_existing) slot.value_ptr.* = .empty;
            var encoded: [4]u8 = undefined;
            std.mem.writeInt(u32, &encoded, @bitCast(posting.weight), .little);
            try slot.value_ptr.appendSlice(scratch, &encoded);
        };
        for (segment.docs, features) |doc, values| {
            if (values.items.len != 0) try docs.put(scratch, doc.doc_id, try std.json.Stringify.valueAlloc(scratch, values.items, .{}));
        }
        var it = weights.iterator();
        while (it.next()) |entry| try postings.put(scratch, entry.key_ptr.*, entry.value_ptr.items);
    }
    root.docs = try common.applyMap(a, scratch, cache.store(), root.docs, docs);
    root.postings = try common.applyMap(a, scratch, cache.store(), root.postings, postings);
    return common.publish(a, pages, root);
}

pub fn searchAlloc(a: A, session: *runtime.QuerySession, ref: manifest.ArtifactRef, req: request.QueryRequest) ![]indexed.ScoredDoc {
    const query = req.sparse orelse return error.SparseQueryRequired;
    var reads: u64 = 32 * 1024 * 1024;
    var writes: u64 = 0;
    var pages = PageStore{ .domain = PageStore.namespaceDomain(session.manifest.namespace), .artifacts = session.artifacts, .cancellation = session.cancellation, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
    const parsed = try common.load(a, &pages, ref);
    defer parsed.deinit();
    var cache = tree.Cache{ .alloc = a, .underlying = pages.store() };
    defer cache.deinit();
    var scores = std.StringHashMapUnmanaged(f32).empty;
    defer {
        var it = scores.keyIterator();
        while (it.next()) |key| a.free(key.*);
        scores.deinit(a);
    }
    for (query) |feature| {
        try session.checkCancellation();
        if (!std.math.isFinite(feature.weight)) return error.InvalidSparseWeight;
        const normalized = try indexed.normalizeAlloc(a, feature.term);
        defer a.free(normalized);
        if (normalized.len == 0) continue;
        const lower = try common.postingKey(a, normalized, "");
        defer a.free(lower);
        const upper = try common.upperBound(a, lower);
        defer if (upper) |value| a.free(value);
        var cursor = try tree.Cursor.init(a, cache.store(), parsed.value.postings, lower, upper);
        defer cursor.deinit();
        while (try cursor.next()) |record| {
            try session.checkCancellation();
            if (record.value.len == 0 or record.value.len % 4 != 0) return error.InvalidPagedProjectionPosting;
            const id = try common.postingDocument(record.key);
            const slot = try scores.getOrPut(a, id);
            if (!slot.found_existing) {
                errdefer _ = scores.remove(id);
                slot.key_ptr.* = try a.dupe(u8, id);
                slot.value_ptr.* = 0;
            }
            var offset: usize = 0;
            while (offset < record.value.len) : (offset += 4) {
                const weight: f32 = @bitCast(std.mem.readInt(u32, record.value[offset..][0..4], .little));
                if (!std.math.isFinite(weight)) return error.InvalidPagedProjectionPosting;
                slot.value_ptr.* += weight * feature.weight;
            }
        }
    }
    var selected = @import("../query/top_docs.zig").TopDocs{ .capacity = req.offset +| req.limit };
    defer selected.deinit(a);
    var it = scores.iterator();
    while (it.next()) |entry| {
        try session.checkCancellation();
        const score = entry.value_ptr.*;
        if (!std.math.isFinite(score) or score * 1000 >= 4294967296.0) return error.SparseScoreOutOfRange;
        if (score <= 0) continue;
        const integral: u32 = @intFromFloat(score * 1000);
        if (integral >= req.min_score) try selected.add(a, .{ .doc_id = entry.key_ptr.*, .score = integral });
    }
    const ranked = selected.sorted();
    const start = @min(req.offset, ranked.len);
    const end = @min(ranked.len, start +| req.limit);
    const result = try a.alloc(indexed.ScoredDoc, end - start);
    var initialized: usize = 0;
    errdefer {
        for (result[0..initialized]) |row| a.free(row.doc_id);
        a.free(result);
    }
    for (ranked[start..end], result) |row, *value| {
        value.* = .{ .doc_id = try a.dupe(u8, row.doc_id), .score = row.score };
        initialized += 1;
    }
    return result;
}

test "serverless paged sparse preserves normalized duplicate weights, offsets and immutable generations" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/projections", .{tmp.sub_path});
    defer a.free(path);
    var fs = try @import("../artifacts/fs_store.zig").FsStore.init(a, path);
    var store = fs.artifactStore();
    defer store.deinit();
    var reads: u64 = 32 * 1024 * 1024;
    var writes: u64 = 32 * 1024 * 1024;
    var pages = PageStore{ .domain = PageStore.namespaceDomain("docs"), .attempt = @splat(1), .artifacts = &store, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
    const builder = @import("../build/builder.zig");
    var session = runtime.QuerySession{ .alloc = a, .artifacts = &store, .owns_manifest = false, .manifest = .{ .namespace = "docs", .version = 1, .built_at_ns = 1, .wal_start_lsn = 1, .wal_end_lsn = 3, .stats = .{ .document_count = 3 }, .artifacts = &.{} } };

    const Doc = @import("../query/materializer.zig").Document;
    const docs = [_]Doc{
        .{ .doc_id = @constCast("a"), .body = @constCast("{\"sparse_embedding\":{\"Alpha\":1.25,\"alpha\":2,\"beta\":1}}"), .last_lsn = 1, .last_timestamp_ns = 1 },
        .{ .doc_id = @constCast("b"), .body = @constCast("{\"sparse_embedding\":{\"alpha\":3.25}}"), .last_lsn = 2, .last_timestamp_ns = 2 },
        .{ .doc_id = @constCast("nul\x00id"), .body = @constCast("{\"sparse_embedding\":{\"alpha\":0.25}}"), .last_lsn = 3, .last_timestamp_ns = 3 },
    };
    var flat = (try builder.allocSparseSegmentAlloc(a, &docs, .{ .index_name = "sparse", .document_source = .sparse_embedding }, null)).?;
    defer flat.deinit(a);
    const ref = try publishAlloc(a, &pages, null, flat, &.{}, "sparse", @splat(1));
    defer builder.freeArtifactRef(a, ref);
    const query = [_]request.SparseTermWeight{ .{ .term = @constCast("ALPHA"), .weight = 0.5 }, .{ .term = @constCast("beta"), .weight = 0.25 } };
    for (0..4) |offset| {
        const expected = try indexed.searchSparseSegmentDocIdsAlloc(a, flat, &query, offset, 2, 0);
        defer indexed.freeScoredDocs(a, expected);
        const actual = try searchAlloc(a, &session, ref, .{ .text = &.{}, .sparse = @constCast(&query), .offset = offset, .limit = 2 });
        defer indexed.freeScoredDocs(a, actual);
        try std.testing.expectEqual(expected.len, actual.len);
        for (expected, actual) |l, r| {
            try std.testing.expectEqualStrings(l.doc_id, r.doc_id);
            try std.testing.expectEqual(l.score, r.score);
        }
    }
    pages.attempt = @splat(2);
    const empty = try publishAlloc(a, &pages, ref, null, &.{ "a", "b", "nul\x00id" }, "sparse", @splat(1));
    defer builder.freeArtifactRef(a, empty);
    const fresh = try searchAlloc(a, &session, empty, .{ .text = &.{}, .sparse = @constCast(&query), .limit = 10 });
    defer indexed.freeScoredDocs(a, fresh);
    try std.testing.expectEqual(@as(usize, 0), fresh.len);
    const old = try searchAlloc(a, &session, ref, .{ .text = &.{}, .sparse = @constCast(&query), .limit = 10 });
    defer indexed.freeScoredDocs(a, old);
    try std.testing.expectEqual(@as(usize, 3), old.len);
    try std.testing.expectError(error.SparseIndexDefinitionChanged, publishAlloc(a, &pages, ref, null, &.{}, "sparse", @splat(9)));
}
