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

//! Exact per-document accounting shared by publication and scheduling. No
//! namespace-wide scan is needed when the policy fingerprint remains stable.
const std = @import("std");
const Allocator = std.mem.Allocator;
const facts = @import("document_facts.zig");
const builder = @import("builder.zig");
const page_store = @import("../graph_segment/page_store.zig");
const tree = @import("../graph_segment/page_tree.zig");
const refs = @import("../manifest/artifact_ref.zig");
const materializer = @import("../query/materializer.zig");
const catalog = @import("../catalog/types.zig");
const sources = @import("../search_sources.zig");
const document_projection = @import("../document_projection.zig");
const full_text_indexes = @import("../../api/full_text_indexes.zig");
const query_reader = @import("../query/indexed_reader.zig");

pub fn fingerprint(alloc: Allocator, policy: catalog.NamespacePolicy, indexes_json: []const u8) ![32]u8 {
    // A version tag owns changes to projection/enrichment semantics. Policy is
    // a fixed struct, giving deterministic field ordering and explicit values.
    const encoded = try std.json.Stringify.valueAlloc(alloc, .{
        .enrichment_enabled = policy.enrichment_enabled,
        .enrichment_pipeline_version = if (policy.enrichment_enabled) policy.enrichment_pipeline_version else 0,
        .chunk_preview_enabled = policy.chunk_preview_enabled,
        .chunk_preview_pipeline_version = if (policy.chunk_preview_enabled) policy.chunk_preview_pipeline_version else 0,
        .chunk_embeddings_enabled = policy.chunk_embeddings_enabled,
        .chunk_embeddings_pipeline_version = if (policy.chunk_embeddings_enabled) policy.chunk_embeddings_pipeline_version else 0,
        .rerank_terms_enabled = policy.rerank_terms_enabled,
        .rerank_terms_pipeline_version = if (policy.rerank_terms_enabled) policy.rerank_terms_pipeline_version else 0,
    }, .{});
    defer alloc.free(encoded);
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly:document-facts:semantics:v2:");
    var len: [8]u8 = undefined;
    std.mem.writeInt(u64, &len, encoded.len, .little);
    hash.update(&len);
    hash.update(encoded);
    // Presence uses only chunked full-text source extraction. Graph aliases,
    // metric policies, dense index options and JSON presentation cannot change
    // any fact. Names/order/duplicates of equivalent chunk sources cannot
    // change their any-source-present result either.
    const chunked = try full_text_indexes.listChunkedFullTextSourcesAlloc(alloc, indexes_json);
    defer full_text_indexes.freeChunkedFullTextSources(alloc, chunked);
    const digests = try alloc.alloc([32]u8, chunked.len);
    defer alloc.free(digests);
    for (chunked, digests) |source, *digest| {
        var cfg = try @import("../../chunking/types.zig").parseConfigFromSlice(alloc, source.chunker_json);
        defer cfg.deinit(alloc);
        const semantic = try std.json.Stringify.valueAlloc(alloc, .{
            .field = if (source.source_template.len == 0) source.source_field else "",
            .template = source.source_template,
            .provider = cfg.provider,
            .api_url = cfg.api_url,
            .model = cfg.model,
            .max_chunks = cfg.max_chunks,
            .threshold = cfg.threshold,
            .text = cfg.text,
            .audio = cfg.audio,
        }, .{});
        defer alloc.free(semantic);
        std.crypto.hash.sha2.Sha256.hash(semantic, digest, .{});
    }
    std.mem.sort([32]u8, digests, {}, struct {
        fn less(_: void, lhs: [32]u8, rhs: [32]u8) bool {
            return std.mem.lessThan(u8, &lhs, &rhs);
        }
    }.less);
    for (digests, 0..) |digest, i| {
        if (i != 0 and std.mem.eql(u8, &digests[i - 1], &digest)) continue;
        hash.update(&digest);
    }
    return hash.finalResult();
}

pub fn needsRebuild(alloc: Allocator, root: facts.Root, policy: catalog.NamespacePolicy, indexes_json: []const u8) !bool {
    return !std.mem.eql(u8, &root.policy_fingerprint, &try fingerprint(alloc, policy, indexes_json));
}

pub const Flags = struct { present: u3, pending: u4 };
pub fn outputsFromCountsAlloc(alloc: Allocator, counts: [7]u64, requested: builder.DerivedOutputDetectionSelection) !sources.MaterializedDerivedOutputs {
    var outputs = std.ArrayListUnmanaged(sources.DerivedOutputDescriptor).empty;
    errdefer {
        for (outputs.items) |*item| sources.deinitDerivedOutputDescriptor(alloc, item);
        outputs.deinit(alloc);
    }
    const selected = [_]bool{ requested.chunk_preview, requested.chunk_embeddings, requested.rerank_terms };
    inline for (.{ sources.DerivedOutputKind.chunk_preview, sources.DerivedOutputKind.chunk_embeddings, sources.DerivedOutputKind.rerank_terms }, 0..) |kind, i| {
        if (selected[i] and counts[i] != 0) {
            try outputs.ensureUnusedCapacity(alloc, 1);
            outputs.appendAssumeCapacity(.{ .name = try alloc.dupe(u8, sources.defaultDerivedOutputName(kind)), .kind = kind });
        }
    }
    return .{ .items = if (outputs.items.len == 0) null else try outputs.toOwnedSlice(alloc) };
}

pub fn flagsForDocumentAlloc(alloc: Allocator, doc: materializer.Document, policy: catalog.NamespacePolicy, indexes_json: []const u8) !Flags {
    var context = try Context.init(alloc, policy, indexes_json);
    defer context.deinit();
    return context.flags(doc);
}

/// Parse index configuration once per operation and each document projection
/// once, regardless of how many presence/pending counters are maintained.
pub const Context = struct {
    alloc: Allocator,
    policy: catalog.NamespacePolicy,
    chunked_sources: []full_text_indexes.ChunkedFullTextSource,

    pub fn init(alloc: Allocator, policy: catalog.NamespacePolicy, indexes_json: []const u8) !Context {
        return .{ .alloc = alloc, .policy = policy, .chunked_sources = try full_text_indexes.listChunkedFullTextSourcesAlloc(alloc, indexes_json) };
    }
    pub fn deinit(self: *Context) void {
        full_text_indexes.freeChunkedFullTextSources(self.alloc, self.chunked_sources);
        self.* = undefined;
    }
    pub fn flags(self: *const Context, doc: materializer.Document) !Flags {
        var projection = try document_projection.parseAlloc(self.alloc, doc.body);
        defer projection.deinit(self.alloc);
        var has_chunk_preview = projection.chunk_preview != null;
        if (!has_chunk_preview and self.chunked_sources.len != 0) {
            const text = try full_text_indexes.synthesizeChunkedFullTextAlloc(self.alloc, doc.body, self.chunked_sources);
            defer self.alloc.free(text);
            has_chunk_preview = text.len > 0;
        }
        const policy = self.policy;
        const needs_text = policy.enrichment_enabled or policy.chunk_preview_enabled or policy.rerank_terms_enabled or
            (policy.chunk_embeddings_enabled and projection.chunk_preview == null);
        const has_text = if (needs_text) text: {
            const normalized = try query_reader.normalizeAlloc(self.alloc, projection.text);
            defer self.alloc.free(normalized);
            break :text normalized.len > 0;
        } else false;
        const has_chunk_source = if (projection.chunk_preview) |chunks| chunks.len > 0 else has_text;
        return .{
            .present = @as(u3, @intFromBool(has_chunk_preview)) |
                (@as(u3, @intFromBool(projection.chunk_embeddings != null)) << 1) |
                (@as(u3, @intFromBool(projection.rerank_terms != null)) << 2),
            .pending = @as(u4, @intFromBool(policy.enrichment_enabled and has_text and (projection.lexical_sparse_version == null or projection.lexical_sparse_version.? < policy.enrichment_pipeline_version))) |
                (@as(u4, @intFromBool(policy.chunk_preview_enabled and has_text and (projection.chunk_preview_version == null or projection.chunk_preview_version.? < policy.chunk_preview_pipeline_version))) << 1) |
                (@as(u4, @intFromBool(policy.chunk_embeddings_enabled and has_chunk_source and (projection.chunk_embeddings_version == null or projection.chunk_embeddings_version.? < policy.chunk_embeddings_pipeline_version))) << 2) |
                (@as(u4, @intFromBool(policy.rerank_terms_enabled and has_text and (projection.rerank_terms_version == null or projection.rerank_terms_version.? < policy.rerank_terms_pipeline_version))) << 3),
        };
    }
};

/// `docs` must be sorted after-images. With no prior root, no mutations, or a
/// changed fingerprint it must be the full document view. Otherwise only the
/// distinct mutation IDs are looked up, and `docs` may contain only those IDs.
pub fn publishAlloc(
    alloc: Allocator,
    pages: *page_store.PageStore,
    source_ref: ?refs.ArtifactRef,
    docs: []const materializer.Document,
    mutations: ?[]const materializer.Mutation,
    policy: catalog.NamespacePolicy,
    indexes_json: []const u8,
    wal_end_lsn: u64,
) !refs.ArtifactRef {
    const policy_hash = try fingerprint(alloc, policy, indexes_json);
    var context = try Context.init(alloc, policy, indexes_json);
    defer context.deinit();
    const previous: ?facts.Root = if (source_ref) |ref| try facts.loadRoot(alloc, pages, ref) else null;
    const rebuild = previous == null or mutations == null or !std.mem.eql(u8, &previous.?.policy_fingerprint, &policy_hash);
    if (!rebuild and mutations.?.len == 0 and previous.?.wal_end_lsn == wal_end_lsn)
        return builder.cloneArtifactRefAlloc(alloc, source_ref.?);
    const source = if (rebuild) facts.Root{ .domain = pages.domain, .policy_fingerprint = policy_hash } else previous.?;
    var cache = tree.Cache{ .alloc = alloc, .underlying = pages.store() };
    defer cache.deinit();
    var replacements = std.ArrayListUnmanaged(facts.Replacement).empty;
    defer replacements.deinit(alloc);
    var seen = std.StringHashMapUnmanaged(void).empty;
    defer seen.deinit(alloc);
    if (rebuild) {
        try replacements.ensureTotalCapacity(alloc, docs.len);
        for (docs) |doc| {
            try pages.cancellation.check();
            replacements.appendAssumeCapacity(.{ .id = doc.doc_id, .value = try makeFact(alloc, pages, cache.store(), previous, doc, &context) });
        }
    } else {
        for (mutations.?) |mutation| {
            try pages.cancellation.check();
            const entry = try seen.getOrPut(alloc, mutation.doc_id);
            if (entry.found_existing) continue;
            const doc = findDocument(docs, mutation.doc_id);
            try replacements.append(alloc, .{ .id = mutation.doc_id, .value = if (doc) |value|
                try makeFact(alloc, pages, cache.store(), previous, value, &context)
            else
                null });
        }
    }
    var plan = try facts.planAlloc(alloc, cache.store(), source, replacements.items, wal_end_lsn);
    defer plan.deinit();
    const root = try plan.publish(cache.store(), source);
    return facts.publishRoot(alloc, pages, root);
}

fn makeFact(alloc: Allocator, pages: *page_store.PageStore, store: tree.Store, prior: ?facts.Root, doc: materializer.Document, context: *const Context) !facts.Fact {
    const flags = try context.flags(doc);
    const digest = facts.bodyDigest(doc.body);
    const old = if (prior) |root| try facts.lookup(alloc, store, root, doc.doc_id) else null;
    const encoded_len = std.math.add(usize, doc.body.len, facts.body_header_bytes) catch return error.ArtifactTooLarge;
    const body = if (old != null and old.?.body.bytes == encoded_len and std.mem.eql(u8, &old.?.body.digest, &digest))
        old.?.body
    else
        try facts.putBody(alloc, pages, doc.body);
    return .{ .body = body, .last_lsn = doc.last_lsn, .last_timestamp_ns = doc.last_timestamp_ns, .present = flags.present, .pending = flags.pending };
}

fn findDocument(docs: []const materializer.Document, id: []const u8) ?materializer.Document {
    var lo: usize = 0;
    var hi = docs.len;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        switch (std.mem.order(u8, docs[mid].doc_id, id)) {
            .lt => lo = mid + 1,
            .gt => hi = mid,
            .eq => return docs[mid],
        }
    }
    return null;
}

pub const TouchedDocuments = struct {
    alloc: Allocator,
    source: facts.Root,
    before_counts: [7]u64,
    before: []materializer.Document,
    after: []materializer.Document,
    pub fn deinit(self: *TouchedDocuments) void {
        materializer.freeDocuments(self.alloc, self.before);
        materializer.freeDocuments(self.alloc, self.after);
        self.* = undefined;
    }
};

pub fn materializeTouchedAlloc(alloc: Allocator, pages: *page_store.PageStore, root: facts.Root, mutations: []const materializer.Mutation) !TouchedDocuments {
    var cache = tree.Cache{ .alloc = alloc, .underlying = pages.store() };
    defer cache.deinit();
    var seen = std.StringHashMapUnmanaged(void).empty;
    defer seen.deinit(alloc);
    var docs = std.ArrayListUnmanaged(materializer.Document).empty;
    var before_counts: [7]u64 = @splat(0);
    errdefer {
        for (docs.items) |*doc| doc.deinit(alloc);
        docs.deinit(alloc);
    }
    for (mutations) |mutation| {
        try pages.cancellation.check();
        if (mutation.lsn <= root.wal_end_lsn) return error.DocumentFactsSourceChanged;
        const slot = try seen.getOrPut(alloc, mutation.doc_id);
        if (slot.found_existing) continue;
        const fact = (try facts.lookup(alloc, cache.store(), root, mutation.doc_id)) orelse continue;
        const bits = @as(u7, fact.present) | (@as(u7, fact.pending) << 3);
        for (&before_counts, 0..) |*count, i| if (bits & (@as(u7, 1) << @intCast(i)) != 0) {
            count.* += 1;
        };
        const id = try alloc.dupe(u8, mutation.doc_id);
        errdefer alloc.free(id);
        const body = try facts.readBodyAlloc(alloc, pages, fact.body);
        errdefer alloc.free(body);
        try docs.append(alloc, .{ .doc_id = id, .body = body, .last_lsn = fact.last_lsn, .last_timestamp_ns = fact.last_timestamp_ns });
    }
    const before = try docs.toOwnedSlice(alloc);
    errdefer materializer.freeDocuments(alloc, before);
    std.mem.sort(materializer.Document, before, {}, struct {
        fn less(_: void, a: materializer.Document, b: materializer.Document) bool {
            return std.mem.order(u8, a.doc_id, b.doc_id) == .lt;
        }
    }.less);
    return .{ .alloc = alloc, .source = root, .before_counts = before_counts, .before = before, .after = try materializer.materializeOverBaseAlloc(alloc, before, mutations) };
}

/// Explicit full-view operations (bootstrap, policy rebuild, compaction) use
/// the same authoritative root. Ordinary prediction/publication uses point
/// lookup instead. Ownership remains caller-local even for a streaming walk.
pub fn materializeAllAlloc(alloc: Allocator, pages: *page_store.PageStore, root: facts.Root) ![]materializer.Document {
    if (!std.mem.eql(u8, &pages.domain, &root.domain)) return error.GraphPageDomainMismatch;
    var cursor = try tree.Cursor.init(alloc, pages.store(), root.page, "", null);
    defer cursor.deinit();
    var docs = std.ArrayListUnmanaged(materializer.Document).empty;
    errdefer {
        for (docs.items) |*doc| doc.deinit(alloc);
        docs.deinit(alloc);
    }
    while (try cursor.next()) |record| {
        const fact = try facts.Fact.decode(record.value);
        const id = try alloc.dupe(u8, record.key);
        errdefer alloc.free(id);
        const body = try facts.readBodyAlloc(alloc, pages, fact.body);
        errdefer alloc.free(body);
        try docs.append(alloc, .{ .doc_id = id, .body = body, .last_lsn = fact.last_lsn, .last_timestamp_ns = fact.last_timestamp_ns });
    }
    if (docs.items.len != root.document_count) return error.InvalidDocumentFactsRoot;
    return docs.toOwnedSlice(alloc);
}

/// Read-only scheduling updates the exact aggregate tuple using touched
/// before/after facts; it does not upload body blobs or build candidate pages.
pub fn predictCountsAlloc(alloc: Allocator, source: facts.Root, touched: TouchedDocuments, policy: catalog.NamespacePolicy, indexes_json: []const u8) ![7]u64 {
    if (try needsRebuild(alloc, source, policy, indexes_json)) return error.DocumentFactsPolicyChanged;
    if (!source.eql(touched.source)) return error.DocumentFactsSourceChanged;
    var context = try Context.init(alloc, policy, indexes_json);
    defer context.deinit();
    var counts = source.counts;
    for (&counts, touched.before_counts) |*count, removed| {
        if (count.* < removed) return error.InvalidDocumentFactsRoot;
        count.* -= removed;
    }
    for (touched.after) |doc| {
        const flags = try context.flags(doc);
        const bits: u7 = @as(u7, flags.present) | (@as(u7, flags.pending) << 3);
        for (&counts, 0..) |*count, i| if (bits & (@as(u7, 1) << @intCast(i)) != 0) {
            count.* = try std.math.add(u64, count.*, 1);
        };
    }
    return counts;
}

test "serverless document facts hydrate only touched bodies and preserve exact counters across reopen and GC" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/facts", .{tmp.sub_path});
    defer a.free(path);
    var fs = try @import("../artifacts/fs_store.zig").FsStore.init(a, path);
    var artifacts = fs.artifactStore();
    defer artifacts.deinit();
    var reads: u64 = 10 * 1024 * 1024;
    var writes: u64 = 10 * 1024 * 1024;
    var pages = page_store.PageStore{ .domain = page_store.PageStore.namespaceDomain("docs"), .attempt = @splat(1), .artifacts = &artifacts, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
    // Encoding must use the operation's admitted allocator, not the artifact
    // store's allocator. Failure must precede any upload or write-budget use.
    var denied = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    const original_write_budget = writes;
    try std.testing.expectError(error.OutOfMemory, facts.putBody(denied.allocator(), &pages, "{}"));
    try std.testing.expectEqual(original_write_budget, writes);
    const docs = [_]materializer.Document{
        .{ .doc_id = @constCast("a"), .body = @constCast("{\"text\":\"one\"}"), .last_lsn = 1, .last_timestamp_ns = 1 },
        .{ .doc_id = @constCast("b"), .body = @constCast("{\"text\":\"two\"}"), .last_lsn = 2, .last_timestamp_ns = 2 },
        .{ .doc_id = @constCast("c"), .body = @constCast("{\"text\":\"three\"}"), .last_lsn = 3, .last_timestamp_ns = 3 },
    };
    const policy = catalog.NamespacePolicy{ .enrichment_enabled = true, .chunk_preview_enabled = true, .chunk_embeddings_enabled = true, .rerank_terms_enabled = true };
    const ref = try publishAlloc(a, &pages, null, &docs, null, policy, "{}", 3);
    defer freeRef(a, ref);
    const root = try facts.loadRoot(a, &pages, ref);
    try std.testing.expectEqual(@as(u64, 3), root.document_count);
    try std.testing.expectEqual([7]u64{ 0, 0, 0, 3, 3, 3, 3 }, root.counts);
    const original_c = (try facts.lookup(a, pages.store(), root, "c")).?;
    // An arbitrary JSON/body payload must not share a content identity with
    // a routing page, or marking the body could suppress child traversal.
    const routing_id = try page_store.PageStore.identity(root.domain, root.page.?);
    const routing_bytes = try artifacts.getAlloc(&routing_id);
    defer a.free(routing_bytes);
    const page_shaped_body = try facts.putBody(a, &pages, routing_bytes);
    try std.testing.expect(!std.mem.eql(u8, &routing_id, &try page_shaped_body.identity(root.domain)));
    const page_shaped_decoded = try facts.readBodyAlloc(a, &pages, page_shaped_body);
    defer a.free(page_shaped_decoded);
    try std.testing.expectEqualStrings(routing_bytes, page_shaped_decoded);
    const mutations = [_]materializer.Mutation{
        .{ .doc_id = "a", .body = "{\"text\":\"replace\"}", .kind = .upsert, .lsn = 4, .timestamp_ns = 4 },
        .{ .doc_id = "a", .body = "{\"text\":\"\"}", .kind = .upsert, .lsn = 5, .timestamp_ns = 5 },
        .{ .doc_id = "b", .kind = .delete, .lsn = 6, .timestamp_ns = 6 },
    };
    var touched = try materializeTouchedAlloc(a, &pages, root, &mutations);
    defer touched.deinit();
    try std.testing.expectEqual(@as(usize, 2), touched.before.len);
    try std.testing.expectEqual(@as(usize, 1), touched.after.len);
    const predicted = try predictCountsAlloc(a, root, touched, policy, "{}");
    try std.testing.expectEqual([7]u64{ 0, 0, 0, 1, 1, 1, 1 }, predicted);
    pages.attempt = @splat(2);
    const updated_ref = try publishAlloc(a, &pages, ref, touched.after, &mutations, policy, "{}", 6);
    defer freeRef(a, updated_ref);
    const updated = try facts.loadRoot(a, &pages, updated_ref);
    try std.testing.expectEqual(predicted, updated.counts);
    try std.testing.expectEqual(@as(u64, 2), updated.document_count);
    const next_c = (try facts.lookup(a, pages.store(), updated, "c")).?;
    try std.testing.expectEqual(original_c.body, next_c.body);
    var retained = std.StringHashMapUnmanaged(void).empty;
    defer {
        var it = retained.keyIterator();
        while (it.next()) |key| a.free(key.*);
        retained.deinit(a);
    }
    try facts.retainRoot(a, &pages, updated_ref, &retained);
    // Replay an interrupted body-before-leaf sweep. The old root and leaf
    // remain the inventory even when one obsolete body has already gone.
    const old_a = (try facts.lookup(a, pages.store(), root, "a")).?;
    try artifacts.delete(&try old_a.body.identity(pages.domain));
    try std.testing.expect(try facts.reclaimRoot(a, &pages, ref, &retained) > 0);
    try std.testing.expectEqual(@as(usize, 0), try facts.reclaimRoot(a, &pages, ref, &retained));
    var cold_fs = try @import("../artifacts/fs_store.zig").FsStore.init(a, path);
    var cold_artifacts = cold_fs.artifactStore();
    defer cold_artifacts.deinit();
    pages.artifacts = &cold_artifacts;
    const cold_root = try facts.loadRoot(a, &pages, updated_ref);
    const all = try materializeAllAlloc(a, &pages, cold_root);
    defer materializer.freeDocuments(a, all);
    try std.testing.expectEqual(@as(usize, 2), all.len);
    try std.testing.expectEqualStrings("{\"text\":\"\"}", all[0].body);
    try std.testing.expectEqualStrings(docs[2].body, all[1].body);
    var changed_policy = policy;
    changed_policy.keep_latest_versions += 1;
    try std.testing.expect(!try needsRebuild(a, cold_root, changed_policy, "{}"));
    changed_policy.chunk_preview_pipeline_version += 1;
    try std.testing.expect(try needsRebuild(a, cold_root, changed_policy, "{}"));
    try std.testing.expectError(error.DocumentFactsPolicyChanged, predictCountsAlloc(a, cold_root, touched, changed_policy, "{}"));
    try std.testing.expectError(error.DocumentFactsSourceChanged, materializeTouchedAlloc(a, &pages, cold_root, &mutations));
}

fn freeRef(alloc: Allocator, ref: refs.ArtifactRef) void {
    if (ref.name.len != 0) alloc.free(ref.name);
    alloc.free(ref.artifact_id);
    alloc.free(ref.checksum);
}

test "serverless document facts fingerprint ignores unrelated metadata but fences projection semantics" {
    const a = std.testing.allocator;
    const empty = try fingerprint(a, .{}, "{}");
    try std.testing.expectEqual(empty, try fingerprint(a, .{},
        \\{"graph_alias":{"metrics":{"rank":{"kind":"pagerank","max_iterations":20}},"type":"graph"},"dense":{"type":"embeddings","field":"body"}}
    ));
    var disabled = catalog.NamespacePolicy{};
    disabled.enrichment_pipeline_version = 19;
    try std.testing.expectEqual(empty, try fingerprint(a, disabled, "{}"));
    disabled.enrichment_enabled = true;
    try std.testing.expect(!std.mem.eql(u8, &empty, &try fingerprint(a, disabled, "{}")));
    const enabled = try fingerprint(a, disabled, "{}");
    disabled.enrichment_pipeline_version += 1;
    try std.testing.expect(!std.mem.eql(u8, &enabled, &try fingerprint(a, disabled, "{}")));
    const source =
        \\{"dense":{"type":"embeddings","field":"body","chunker":{"provider":"mock","full_text_index":{}}}}
    ;
    const renamed =
        \\{"alias":{"chunker":{"full_text_index":{},"store_chunks":true,"provider":"mock"},"field":"body","type":"embeddings"},"duplicate":{"type":"embeddings","field":"body","chunker":{"provider":"mock","full_text_index":{}}}}
    ;
    const source_hash = try fingerprint(a, .{}, source);
    try std.testing.expect(!std.mem.eql(u8, &empty, &source_hash));
    try std.testing.expectEqual(source_hash, try fingerprint(a, .{}, renamed));
    try std.testing.expect(!std.mem.eql(u8, &source_hash, &try fingerprint(a, .{},
        \\{"dense":{"type":"embeddings","field":"title","chunker":{"provider":"mock","full_text_index":{}}}}
    )));
    try std.testing.expect(!std.mem.eql(u8, &source_hash, &try fingerprint(a, .{},
        \\{"dense":{"type":"embeddings","field":"body","chunker":{"provider":"mock","full_text_index":{},"text":{"target_tokens":8}}}}
    )));
}

test "serverless document facts compiled normalization agrees with existing status semantics" {
    const a = std.testing.allocator;
    const bodies = [_][]const u8{
        "not-json",                                                                              "{}",                                            "{\"text\":\"\"}",                                                                                                                                           "{\"body\":\"hello\"}",
        "{\"text\":\"hello\",\"chunk_preview\":[],\"chunk_embeddings\":[],\"rerank_terms\":[]}", "{\"text\":\"\",\"chunk_preview\":[\"chunk\"]}", "{\"text\":\"hello\",\"_enrichment\":{\"lexical_sparse_version\":3,\"chunk_preview_version\":1,\"chunk_embeddings_version\":2,\"rerank_terms_version\":3}}",
    };
    for (0..16) |enabled| {
        const policy = catalog.NamespacePolicy{
            .enrichment_enabled = enabled & 1 != 0,
            .chunk_preview_enabled = enabled & 2 != 0,
            .chunk_embeddings_enabled = enabled & 4 != 0,
            .rerank_terms_enabled = enabled & 8 != 0,
            .enrichment_pipeline_version = 2,
            .chunk_preview_pipeline_version = 2,
            .chunk_embeddings_pipeline_version = 2,
            .rerank_terms_pipeline_version = 2,
        };
        var context = try Context.init(a, policy, "{}");
        defer context.deinit();
        for (bodies) |body| {
            const doc = materializer.Document{ .doc_id = @constCast("doc"), .body = @constCast(body), .last_lsn = 1, .last_timestamp_ns = 0 };
            const actual = try context.flags(doc);
            var outputs = try builder.detectMaterializedDerivedOutputsAlloc(a, &.{doc}, "{}", .{});
            defer sources.deinitMaterializedDerivedOutputs(a, &outputs);
            const pending = try builder.predictPendingEnrichmentAlloc(a, &.{doc}, policy, .{});
            const expected = Flags{
                .present = @as(u3, @intFromBool(outputs.containsKind(.chunk_preview))) |
                    (@as(u3, @intFromBool(outputs.containsKind(.chunk_embeddings))) << 1) |
                    (@as(u3, @intFromBool(outputs.containsKind(.rerank_terms))) << 2),
                .pending = @as(u4, @intFromBool(pending.lexical_sparse_pending_documents != 0)) |
                    (@as(u4, @intFromBool(pending.chunk_preview_pending_documents != 0)) << 1) |
                    (@as(u4, @intFromBool(pending.chunk_embeddings_pending_documents != 0)) << 2) |
                    (@as(u4, @intFromBool(pending.rerank_terms_pending_documents != 0)) << 3),
            };
            try std.testing.expectEqual(expected, actual);
        }
    }
}
