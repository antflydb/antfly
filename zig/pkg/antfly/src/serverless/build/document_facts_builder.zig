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

//! Exact per-document accounting shared by publication and scheduling. No
//! namespace-wide scan is needed when the policy fingerprint remains stable.
const std = @import("std");
const Allocator = std.mem.Allocator;
const facts = @import("document_facts.zig");
const builder = @import("builder.zig");
const page_store = @import("../graph_segment/page_store.zig");
const tree = @import("../graph_segment/page_tree.zig");
const refs = @import("antfly_local_sources").serverless_manifest_artifact_ref;
const materializer = @import("../query/materializer.zig");
const catalog = @import("../catalog/types.zig");
const sources = @import("../search_sources.zig");
const document_projection = @import("../document_projection.zig");
const full_text_indexes = @import("antfly_local_sources").api_full_text_indexes;
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
    hash.update("antfly:document-facts:semantics:v4:");
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
        var cfg = try @import("antfly_local_sources").chunking_types.parseConfigFromSlice(alloc, source.chunker_json);
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
    const fields = try mediaFields(alloc, indexes_json);
    defer freeMediaFields(alloc, fields);
    for (fields, 0..) |field, i| {
        if (i != 0 and std.mem.eql(u8, fields[i - 1], field)) continue;
        var length: [8]u8 = undefined;
        std.mem.writeInt(u64, &length, field.len, .little);
        hash.update(&length);
        hash.update(field);
    }
    return hash.finalResult();
}

pub fn hasModelSources(a: Allocator, indexes_json: []const u8) !bool {
    const fields = try mediaFields(a, indexes_json);
    defer freeMediaFields(a, fields);
    return fields.len != 0;
}

// Only fields consumed by model-aware indexes affect media pending facts.
fn mediaFields(a: Allocator, indexes_json: []const u8) ![][]const u8 {
    if (std.mem.trim(u8, indexes_json, " \t\r\n").len == 0) return a.alloc([]const u8, 0);
    var parsed = try std.json.parseFromSlice(std.json.Value, a, indexes_json, .{});
    defer parsed.deinit();
    var fields = std.ArrayList([]const u8).empty;
    errdefer {
        for (fields.items) |field| a.free(field);
        fields.deinit(a);
    }
    if (parsed.value == .object) {
        var iter = parsed.value.object.iterator();
        while (iter.next()) |entry| {
            const index = entry.value_ptr.*;
            if (index != .object) continue;
            const embedder = index.object.get("embedder") orelse continue;
            if (embedder != .object) continue;
            const provider = embedder.object.get("provider") orelse continue;
            if (provider != .string or !std.mem.eql(u8, provider.string, "antfly")) continue;
            const field = index.object.get("field") orelse std.json.Value{ .string = "text" };
            if (field != .string) continue;
            const owned = try a.dupe(u8, field.string);
            errdefer a.free(owned);
            try fields.append(a, owned);
        }
    }
    std.mem.sort([]const u8, fields.items, {}, struct {
        fn less(_: void, lhs: []const u8, rhs: []const u8) bool {
            return std.mem.lessThan(u8, lhs, rhs);
        }
    }.less);
    return fields.toOwnedSlice(a);
}
fn freeMediaFields(a: Allocator, fields: [][]const u8) void {
    for (fields) |field| a.free(field);
    a.free(fields);
}

pub fn needsRebuild(alloc: Allocator, root: facts.Root, policy: catalog.NamespacePolicy, indexes_json: []const u8) !bool {
    return !std.mem.eql(u8, &root.policy_fingerprint, &try fingerprint(alloc, policy, indexes_json));
}

pub const Flags = struct { present: u3, pending: u4, body_has_dependencies: bool = false };
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
    media_fields: [][]const u8,

    pub fn init(alloc: Allocator, policy: catalog.NamespacePolicy, indexes_json: []const u8) !Context {
        const fields = try mediaFields(alloc, indexes_json);
        errdefer freeMediaFields(alloc, fields);
        return .{ .alloc = alloc, .policy = policy, .media_fields = fields, .chunked_sources = try full_text_indexes.listChunkedFullTextSourcesAlloc(alloc, indexes_json) };
    }
    pub fn deinit(self: *Context) void {
        freeMediaFields(self.alloc, self.media_fields);
        full_text_indexes.freeChunkedFullTextSources(self.alloc, self.chunked_sources);
        self.* = undefined;
    }
    pub fn flags(self: *const Context, doc: materializer.Document) !Flags {
        var projection = try document_projection.parseAllocWithSourceFields(self.alloc, doc.body, self.media_fields);
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
        const has_chunk_source = projection.chunk_embeddings_pending or projection.model_media_source_present or (if (projection.chunk_preview) |chunks| chunks.len > 0 else has_text);
        return .{
            .body_has_dependencies = projection.has_media_checkpoint,
            .present = @as(u3, @intFromBool(has_chunk_preview)) |
                (@as(u3, @intFromBool(projection.chunk_embeddings != null)) << 1) |
                (@as(u3, @intFromBool(projection.rerank_terms != null)) << 2),
            .pending = @as(u4, @intFromBool(policy.enrichment_enabled and has_text and (projection.lexical_sparse_version == null or projection.lexical_sparse_version.? < policy.enrichment_pipeline_version))) |
                (@as(u4, @intFromBool(policy.chunk_preview_enabled and has_text and (projection.chunk_preview_version == null or projection.chunk_preview_version.? < policy.chunk_preview_pipeline_version))) << 1) |
                (@as(u4, @intFromBool(policy.chunk_embeddings_enabled and has_chunk_source and (projection.chunk_embeddings_pending or projection.chunk_embeddings_version == null or projection.chunk_embeddings_version.? < policy.chunk_embeddings_pipeline_version))) << 2) |
                (@as(u4, @intFromBool(policy.rerank_terms_enabled and has_text and (projection.rerank_terms_version == null or projection.rerank_terms_version.? < policy.rerank_terms_pipeline_version))) << 3),
        };
    }
};

/// `docs` must be sorted after-images. With no prior root, no mutations, or a
/// changed fingerprint it must be the full document view. Otherwise only the
/// distinct mutation IDs are looked up, and `docs` may contain only those IDs.
const PendingBodies = struct {
    alloc: Allocator,
    pages: *page_store.PageStore,
    items: [8][]const u8 = undefined,
    length: usize = 0,
    bytes: usize = 0,
    seen: [64][32]u8 = undefined,
    seen_count: usize = 0,
    seen_next: usize = 0,
    packed_bodies: std.AutoHashMapUnmanaged([32]u8, facts.BodyPack) = .empty,

    fn add(self: *@This(), body: []const u8, digest: [32]u8) !void {
        for (self.seen[0..self.seen_count]) |prior| if (std.mem.eql(u8, &prior, &digest)) return;
        const encoded = try std.math.add(usize, body.len, facts.body_header_bytes);
        if (self.length == self.items.len or (self.length != 0 and encoded > 1024 * 1024 -| self.bytes)) try self.flush();
        self.items[self.length] = body;
        self.length += 1;
        self.bytes = try std.math.add(usize, self.bytes, encoded);
        self.seen[self.seen_next] = digest;
        self.seen_next = (self.seen_next + 1) % self.seen.len;
        self.seen_count = @min(self.seen.len, self.seen_count + 1);
    }
    fn flush(self: *@This()) !void {
        if (self.length != 0 and self.pages.artifacts.pack_document_bodies and self.bytes + 16 <= facts.max_body_pack_bytes) {
            var refs_out: [8]facts.BodyRef = undefined;
            try facts.putBodyPack(self.alloc, self.pages, self.items[0..self.length], refs_out[0..self.length]);
            for (refs_out[0..self.length]) |ref| try self.packed_bodies.put(self.alloc, ref.digest, ref.pack.?);
        } else try facts.putBodies(self.alloc, self.pages, self.items[0..self.length]);
        self.length = 0;
        self.bytes = 0;
    }
};

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
    const saved_pack_pages = pages.pack_pages;
    const saved_value_prefix = pages.value_prefix;
    defer {
        pages.pack_pages = saved_pack_pages;
        pages.value_prefix = saved_value_prefix;
    }
    if (pages.artifacts.canonical_point_bodies or (if (previous) |value| value.canonical_records else false)) {
        pages.pack_pages = pages.artifacts.point_read_blocks or (if (previous) |value| value.packed_records else false);
        pages.value_prefix = null;
    }
    const rebuild = previous == null or mutations == null or !std.mem.eql(u8, &previous.?.policy_fingerprint, &policy_hash);
    if (!rebuild and mutations.?.len == 0 and previous.?.wal_end_lsn == wal_end_lsn)
        return builder.cloneArtifactRefAlloc(alloc, source_ref.?);
    var source = if (rebuild) facts.Root{ .domain = pages.domain, .policy_fingerprint = policy_hash } else previous.?;
    source.packed_records = source.packed_records or (pages.artifacts.point_read_blocks and pages.artifacts.canonical_point_bodies);
    source.inline_leaf_format = source.inline_leaf_format or (source.packed_records and pages.artifacts.inline_fact_leaves);
    var cache = tree.Cache{ .alloc = alloc, .underlying = pages.store() };
    defer cache.deinit();
    var pending = PendingBodies{ .alloc = alloc, .pages = pages };
    defer pending.packed_bodies.deinit(alloc);
    var replacements = std.ArrayListUnmanaged(facts.Replacement).empty;
    defer replacements.deinit(alloc);
    var seen = std.StringHashMapUnmanaged(void).empty;
    defer seen.deinit(alloc);
    if (rebuild) {
        try replacements.ensureTotalCapacity(alloc, docs.len);
        for (docs) |doc| {
            try pages.cancellation.check();
            replacements.appendAssumeCapacity(.{ .id = doc.doc_id, .value = try makeFact(alloc, pages, cache.store(), previous, doc, &context, &pending) });
        }
    } else {
        for (mutations.?) |mutation| {
            try pages.cancellation.check();
            const entry = try seen.getOrPut(alloc, mutation.doc_id);
            if (entry.found_existing) continue;
            const doc = findDocument(docs, mutation.doc_id);
            try replacements.append(alloc, .{ .id = mutation.doc_id, .value = if (doc) |value|
                try makeFact(alloc, pages, cache.store(), previous, value, &context, &pending)
            else
                null });
        }
    }
    try pending.flush();
    for (replacements.items) |*replacement| if (replacement.value) |*fact| {
        if (fact.body.pack == null and std.mem.eql(u8, &fact.body.attempt, &pages.attempt)) fact.body.pack = pending.packed_bodies.get(fact.body.digest);
    };
    var plan = try facts.planAlloc(alloc, cache.store(), source, replacements.items, wal_end_lsn);
    defer plan.deinit();
    const root = try plan.publish(cache.store(), source);
    return facts.publishRoot(alloc, pages, root);
}

fn makeFact(alloc: Allocator, pages: *page_store.PageStore, store: tree.Store, prior: ?facts.Root, doc: materializer.Document, context: *const Context, pending: *PendingBodies) !facts.Fact {
    const flags = try context.flags(doc);
    const physical = doc.stored_body orelse doc.body;
    const digest = facts.bodyDigest(physical);
    const old = if (prior) |root| try facts.lookup(alloc, store, root, doc.doc_id) else null;
    const encoded_len = std.math.add(usize, physical.len, facts.body_header_bytes) catch return error.ArtifactTooLarge;
    const eligible = doc.doc_id.len <= 256 and physical.len <= facts.max_inline_body_bytes and doc.stored_body == null and !flags.body_has_dependencies;
    const canonical = pages.artifacts.canonical_point_bodies and eligible;
    const body = if (canonical)
        facts.BodyRef{ .digest = digest, .attempt = pages.attempt, .bytes = encoded_len }
    else if (old != null and old.?.body.bytes == encoded_len and std.mem.eql(u8, &old.?.body.digest, &digest))
        old.?.body
    else queued: {
        if (doc.stored_body != null or flags.body_has_dependencies) break :queued try facts.putBody(alloc, pages, physical);
        try pending.add(physical, digest);
        break :queued facts.BodyRef{ .digest = digest, .attempt = pages.attempt, .bytes = encoded_len };
    };
    // Recompute the witness from this exact after-image, even when public
    // callers supply enrichment metadata. Its reachability follows the fact.
    const audit_json = if (doc.stored_body == null and !flags.body_has_dependencies) try @import("../enrichment/media_audit.zig").buildAlloc(alloc, doc.body) else null;
    defer if (audit_json) |json| alloc.free(json);
    const audit = if (audit_json) |json| try facts.putBody(alloc, pages, json) else null;
    return .{ .canonical_body = canonical, .inline_payload = if (canonical or (pages.artifacts.inline_point_bodies and eligible and (if (body.pack) |pack| pack.leaf_records == 0 else true))) physical else null, .body = body, .media_audit = audit, .source_input_bytes = doc.source_input_bytes, .body_has_dependencies = doc.stored_body != null or flags.body_has_dependencies, .last_lsn = doc.last_lsn, .last_timestamp_ns = doc.last_timestamp_ns, .present = flags.present, .pending = flags.pending };
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
        const body = try facts.readBodyWithStoreAlloc(alloc, pages, cache.store(), fact.body);
        errdefer alloc.free(body);
        try docs.append(alloc, .{ .doc_id = id, .body = body, .source_input_bytes = fact.source_input_bytes, .last_lsn = fact.last_lsn, .last_timestamp_ns = fact.last_timestamp_ns });
    }
    const before = try docs.toOwnedSlice(alloc);
    errdefer materializer.freeDocuments(alloc, before);
    std.mem.sort(materializer.Document, before, {}, struct {
        fn less(_: void, a: materializer.Document, b: materializer.Document) bool {
            return std.mem.order(u8, a.doc_id, b.doc_id) == .lt;
        }
    }.less);
    return .{ .alloc = alloc, .source = root, .before_counts = before_counts, .before = before, .after = try materializer.materializeWithBodiesAlloc(alloc, before, mutations, .{ .pages = pages, .artifacts = pages.artifacts, .domain = pages.domain, .namespace = "", .cancellation = pages.cancellation }) };
}

/// Explicit full-view operations (bootstrap, policy rebuild, compaction) use
/// the same authoritative root. Ordinary prediction/publication uses point
/// lookup instead. Ownership remains caller-local even for a streaming walk.
pub fn materializeAllAlloc(alloc: Allocator, pages: *page_store.PageStore, root: facts.Root) ![]materializer.Document {
    return materializeWithSnapshotAlloc(alloc, pages, root, &.{});
}

/// A verified packed snapshot is a body cache, never the authority for row
/// membership or revision. Facts select every ID/version and authenticate any
/// reused body by digest and encoded length; stale/missing entries hydrate.
pub fn materializeWithSnapshotAlloc(alloc: Allocator, pages: *page_store.PageStore, root: facts.Root, snapshot: []const @import("../document_segment/mod.zig").Entry) ![]materializer.Document {
    for (snapshot, 0..) |entry, i| if (i != 0 and std.mem.order(u8, snapshot[i - 1].doc_id, entry.doc_id) != .lt) return error.InvalidDocumentSnapshot;
    var snapshot_index: usize = 0;
    if (!std.mem.eql(u8, &pages.domain, &root.domain)) return error.GraphPageDomainMismatch;
    var cursor = try tree.Cursor.init(alloc, pages.store(), root.page, "", null);
    defer cursor.deinit();
    var docs = std.ArrayListUnmanaged(materializer.Document).empty;
    errdefer {
        for (docs.items) |*doc| doc.deinit(alloc);
        docs.deinit(alloc);
    }
    while (try cursor.next()) |record| {
        try pages.cancellation.check();
        const fact = try facts.decodeRecord(record);
        if (fact.last_lsn > root.wal_end_lsn) return error.InvalidDocumentFact;
        while (snapshot_index < snapshot.len and std.mem.order(u8, snapshot[snapshot_index].doc_id, record.key) == .lt) snapshot_index += 1;
        const cached = if (snapshot_index < snapshot.len and std.mem.eql(u8, snapshot[snapshot_index].doc_id, record.key)) snapshot[snapshot_index].body else null;
        const reusable = if (cached) |body| fact.body.bytes == @as(u64, body.len) + facts.body_header_bytes and std.mem.eql(u8, &fact.body.digest, &facts.bodyDigest(body)) else false;
        const id = try alloc.dupe(u8, record.key);
        errdefer alloc.free(id);
        const body = if (reusable) try alloc.dupe(u8, cached.?) else try facts.readRecordBodyAlloc(alloc, pages, record);
        errdefer alloc.free(body);
        try docs.append(alloc, .{ .doc_id = id, .body = body, .source_input_bytes = fact.source_input_bytes, .last_lsn = fact.last_lsn, .last_timestamp_ns = fact.last_timestamp_ns });
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
        .{ .doc_id = @constCast("c"), .body = @constCast("{\"text\":\"three\"}"), .source_input_bytes = 7, .last_lsn = 3, .last_timestamp_ns = 3 },
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
    // GC must preserve stage trees and their shared body identities, not only
    // the main point index. All stages here contain just the unchanged tail.
    for (0..4) |stage| {
        var pending = try facts.pendingCursor(a, pages.store(), cold_root, stage, "");
        defer pending.deinit();
        const record = (try pending.next()).?;
        try std.testing.expectEqualStrings("c", record.key);
        const body = try facts.readBodyAlloc(a, &pages, (try facts.Fact.decode(record.value)).body);
        defer a.free(body);
        try std.testing.expectEqualStrings(docs[2].body, body);
        try std.testing.expectEqual(null, try pending.next());
    }
    const all = try materializeAllAlloc(a, &pages, cold_root);
    defer materializer.freeDocuments(a, all);
    try std.testing.expectEqual(@as(usize, 2), all.len);
    try std.testing.expectEqualStrings("{\"text\":\"\"}", all[0].body);
    try std.testing.expectEqualStrings(docs[2].body, all[1].body);
    try std.testing.expectEqual(@as(?u64, 7), all[1].source_input_bytes);
    var changed_policy = policy;
    changed_policy.keep_latest_versions += 1;
    try std.testing.expect(!try needsRebuild(a, cold_root, changed_policy, "{}"));
    changed_policy.chunk_preview_pipeline_version += 1;
    try std.testing.expect(try needsRebuild(a, cold_root, changed_policy, "{}"));
    try std.testing.expectError(error.DocumentFactsPolicyChanged, predictCountsAlloc(a, cold_root, touched, changed_policy, "{}"));
    try std.testing.expectError(error.DocumentFactsSourceChanged, materializeTouchedAlloc(a, &pages, cold_root, &mutations));
    pages.attempt = @splat(3);
    const rebuilt_ref = try publishAlloc(a, &pages, updated_ref, all, null, changed_policy, "{}", 6);
    defer freeRef(a, rebuilt_ref);
    const rebuilt_root = try facts.loadRoot(a, &pages, rebuilt_ref);
    try std.testing.expectEqual(@as(?u64, 7), (try facts.lookup(a, pages.store(), rebuilt_root, "c")).?.source_input_bytes);
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

test "serverless packed body snapshots hydrate only mismatches and facts retain row authority" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/snapshot-facts", .{tmp.sub_path});
    defer a.free(path);
    var fs = try @import("../artifacts/fs_store.zig").FsStore.init(a, path);
    var artifacts = fs.artifactStore();
    defer artifacts.deinit();
    var reads: u64 = 1024 * 1024;
    var writes: u64 = 1024 * 1024;
    var pages = page_store.PageStore{ .domain = page_store.PageStore.namespaceDomain("docs"), .attempt = @splat(1), .artifacts = &artifacts, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
    const docs = [_]materializer.Document{
        .{ .doc_id = @constCast("a"), .body = @constCast("{\"text\":\"one\"}"), .last_lsn = 1, .last_timestamp_ns = 10 },
        .{ .doc_id = @constCast("b"), .body = @constCast("{\"text\":\"current\"}"), .last_lsn = 2, .last_timestamp_ns = 20 },
        .{ .doc_id = @constCast("c"), .body = @constCast("{\"text\":\"new\"}"), .last_lsn = 3, .last_timestamp_ns = 30 },
    };
    const ref = try publishAlloc(a, &pages, null, &docs, null, .{}, "{}", 3);
    defer freeRef(a, ref);
    const root = try facts.loadRoot(a, &pages, ref);
    const b = (try facts.lookup(a, pages.store(), root, "b")).?;
    const c = (try facts.lookup(a, pages.store(), root, "c")).?;
    const Entry = @import("../document_segment/mod.zig").Entry;
    const snapshot = [_]Entry{
        .{ .doc_id = @constCast("a"), .body = docs[0].body, .last_lsn = 999, .last_timestamp_ns = 999 },
        .{ .doc_id = @constCast("b"), .body = @constCast("stale"), .last_lsn = 0, .last_timestamp_ns = 0 },
        .{ .doc_id = @constCast("z"), .body = @constCast("deleted"), .last_lsn = 0, .last_timestamp_ns = 0 },
    };
    const before = reads;
    const result = try materializeWithSnapshotAlloc(a, &pages, root, &snapshot);
    defer materializer.freeDocuments(a, result);
    try std.testing.expectEqual(@as(usize, 3), result.len);
    try std.testing.expectEqual(@as(u64, 1), result[0].last_lsn);
    try std.testing.expectEqualStrings(docs[1].body, result[1].body);
    try std.testing.expectEqualStrings("c", result[2].doc_id);
    try std.testing.expectEqual(@as(u64, root.page.?.bytes) + b.body.bytes + c.body.bytes, before - reads);
}

test "serverless document facts packed after-images reuse bodies and retain shared packs through GC" {
    const a = std.testing.allocator;
    var memory = @import("objectstore").MemoryClient.init(a);
    defer memory.deinit();
    var impl = try @import("../artifacts/object_store.zig").ObjectStore.initWithClient(a, memory.client(), "artifacts", "tenant");
    var artifacts = impl.artifactStore();
    defer artifacts.deinit();
    artifacts.pack_document_bodies = true;
    var reads: u64 = 16 * 1024 * 1024;
    var writes: u64 = reads;
    var pages = page_store.PageStore{ .domain = page_store.PageStore.namespaceDomain("packed-docs"), .attempt = @splat(1), .artifacts = &artifacts, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
    const docs = [_]materializer.Document{
        .{ .doc_id = @constCast("a"), .body = @constCast("{\"text\":\"first\"}"), .last_lsn = 1, .last_timestamp_ns = 1 },
        .{ .doc_id = @constCast("b"), .body = @constCast("{\"text\":\"second\"}"), .last_lsn = 2, .last_timestamp_ns = 2 },
    };
    const first = try publishAlloc(a, &pages, null, &docs, null, .{}, "{}", 2);
    defer freeRef(a, first);
    const root = try facts.loadRoot(a, &pages, first);
    const original = (try facts.lookup(a, pages.store(), root, "a")).?;
    try std.testing.expect(original.body.pack != null);
    pages.attempt = @splat(2);
    var changed = docs[0];
    changed.last_lsn = 3;
    const second = try publishAlloc(a, &pages, first, &.{changed}, &.{.{ .doc_id = "a", .body = docs[0].body, .kind = .upsert, .lsn = 3, .timestamp_ns = 3 }}, .{}, "{}", 3);
    defer freeRef(a, second);
    const next = try facts.loadRoot(a, &pages, second);
    try std.testing.expectEqual(original.body, (try facts.lookup(a, pages.store(), next, "a")).?.body);
    var retained = std.StringHashMapUnmanaged(void).empty;
    defer {
        var keys = retained.keyIterator();
        while (keys.next()) |key| a.free(key.*);
        retained.deinit(a);
    }
    try facts.retainRoot(a, &pages, second, &retained);
    _ = try facts.reclaimRoot(a, &pages, first, &retained);
    const body = try facts.readBodyAlloc(a, &pages, (try facts.lookup(a, pages.store(), next, "b")).?.body);
    defer a.free(body);
    try std.testing.expectEqualStrings(docs[1].body, body);
}

test "model aware media serverless pending facts retain empty vector failures and configured source fields" {
    const a = std.testing.allocator;
    const policy = catalog.NamespacePolicy{ .chunk_embeddings_enabled = true, .chunk_embeddings_pipeline_version = 2 };
    const retry = try flagsForDocumentAlloc(a, .{ .doc_id = @constCast("doc"), .last_lsn = 0, .last_timestamp_ns = 0, .body = @constCast("{\"text\":\"\",\"chunk_preview\":[],\"_enrichment\":{\"chunk_embeddings_pending\":true}}") }, policy, "{}");
    try std.testing.expect(retry.pending & 4 != 0);
    const initial = try flagsForDocumentAlloc(a, .{ .doc_id = @constCast("doc"), .last_lsn = 0, .last_timestamp_ns = 0, .body = @constCast("{\"text\":\"\",\"clip\":{\"uri\":\"s3://bucket/clip\"}}") }, policy, "{\"dense\":{\"field\":\"clip\",\"embedder\":{\"provider\":\"antfly\"}}}");
    try std.testing.expect(initial.pending & 4 != 0);
}

test "serverless canonical document facts bodies have one durable copy and resolved audit locators across flag disable and GC" {
    for ([_]bool{ false, true }) |use_blocks| {
        const a = std.testing.allocator;
        var memory = @import("objectstore").MemoryClient.init(a);
        defer memory.deinit();
        var impl = try @import("../artifacts/object_store.zig").ObjectStore.initWithClient(a, memory.client(), "artifacts", "canonical");
        var artifacts = impl.artifactStore();
        defer artifacts.deinit();
        artifacts.pack_document_bodies = true;
        artifacts.canonical_point_bodies = true;
        artifacts.point_read_blocks = use_blocks;
        var reads: u64 = 64 * 1024 * 1024;
        var writes: u64 = reads;
        var pages = page_store.PageStore{ .domain = page_store.PageStore.namespaceDomain("canonical"), .attempt = @splat(1), .artifacts = &artifacts, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
        const docs = [_]materializer.Document{
            .{ .doc_id = @constCast("a"), .body = @constCast("{\"text\":\"first\"}"), .last_lsn = 1, .last_timestamp_ns = 1 },
            .{ .doc_id = @constCast("b"), .body = @constCast("{\"text\":\"second\"}"), .last_lsn = 2, .last_timestamp_ns = 2 },
        };
        const first = try publishAlloc(a, &pages, null, &docs, null, .{}, "{}", 2);
        defer freeRef(a, first);
        try std.testing.expectEqual(@as(u32, if (use_blocks) 13 else 12), first.metadata_version);
        const source = try facts.loadRoot(a, &pages, first);
        try std.testing.expect(source.canonical_records);
        const fact = (try facts.lookup(a, pages.store(), source, "a")).?;
        try std.testing.expect(fact.body.pack.?.leaf_records != 0);
        const body = try facts.readBodyAlloc(a, &pages, fact.body);
        defer a.free(body);
        try std.testing.expectEqualStrings(docs[0].body, body);
        var missing = fact.body;
        missing.pack = null;
        const separate = try missing.identity(pages.domain);
        try std.testing.expectError(error.FileNotFound, artifacts.getAlloc(&separate));
        var audit = try facts.mediaCursor(a, pages.store(), source, "", null);
        defer audit.deinit();
        const scheduled = try facts.Fact.decode((try audit.next()).?.value);
        const scheduled_body = try facts.readBodyAlloc(a, &pages, scheduled.body);
        defer a.free(scheduled_body);
        try std.testing.expectEqualStrings(docs[0].body, scheduled_body);
        var invalid = fact.body;
        invalid.pack.?.offset += 1;
        try std.testing.expectError(error.InvalidDocumentBody, facts.readBodyAlloc(a, &pages, invalid));
        // Enrichment descriptors serialize the typed locator and resolve their
        // canonical source even when no standalone body object exists.
        const checkpoint = try facts.encodeCheckpointBodyAlloc(a, fact.body, "{\"media_outputs\":{\"text\":\"derived\"}}");
        defer a.free(checkpoint);
        var parsed_checkpoint = try facts.parseCheckpointBody(a, checkpoint);
        defer parsed_checkpoint.deinit();
        try std.testing.expectEqualDeep(fact.body, parsed_checkpoint.value.base);
        const resolved_checkpoint = try facts.resolveCheckpointBodyAlloc(a, &pages, checkpoint);
        defer a.free(resolved_checkpoint);
        var checkpoint_json = try std.json.parseFromSlice(std.json.Value, a, resolved_checkpoint, .{});
        defer checkpoint_json.deinit();
        try std.testing.expectEqualStrings("first", checkpoint_json.value.object.get("text").?.string);
        try std.testing.expectEqualStrings("derived", checkpoint_json.value.object.get("media_outputs").?.object.get("text").?.string);
        // Updating one member rewrites its shared leaf. The unchanged member's
        // logical audit row must resolve through this new primary root, not pin
        // the obsolete leaf through a physical locator in the scheduling index.
        var changed = docs;
        changed[0].body = @constCast("{\"text\":\"replacement\"}");
        changed[0].last_lsn = 3;
        pages.attempt = @splat(3);
        const edited = try publishAlloc(a, &pages, first, &changed, null, .{}, "{}", 3);
        defer freeRef(a, edited);
        const edited_root = try facts.loadRoot(a, &pages, edited);
        const neighbor = (try facts.lookup(a, pages.store(), edited_root, "b")).?;
        try std.testing.expect(!std.mem.eql(u8, &neighbor.body.pack.?.digest, &fact.body.pack.?.digest));
        var edited_audit = try facts.mediaCursor(a, pages.store(), edited_root, "", null);
        defer edited_audit.deinit();
        const neighbor_row = (try edited_audit.next()).?;
        try std.testing.expectEqualStrings("b", neighbor_row.key);
        const neighbor_audit = try facts.Fact.decode(neighbor_row.value);
        try std.testing.expectEqualDeep(neighbor.body, neighbor_audit.body);
        // Old pins still read their original body after the new generation exists.
        const pinned_body = try facts.readBodyAlloc(a, &pages, fact.body);
        defer a.free(pinned_body);
        try std.testing.expectEqualStrings(docs[0].body, pinned_body);
        artifacts.canonical_point_bodies = false;
        artifacts.point_read_blocks = false;
        artifacts.inline_point_bodies = true;
        pages.attempt = @splat(2);
        const next = try publishAlloc(a, &pages, first, &docs, null, .{}, "{}", 2);
        defer freeRef(a, next);
        const after = try facts.loadRoot(a, &pages, next);
        const found = (try facts.lookup(a, pages.store(), after, "b")).?;
        const restored = try facts.readBodyAlloc(a, &pages, found.body);
        defer a.free(restored);
        try std.testing.expectEqualStrings(docs[1].body, restored);
        var retained = std.StringHashMapUnmanaged(void).empty;
        defer {
            var it = retained.keyIterator();
            while (it.next()) |key| a.free(key.*);
            retained.deinit(a);
        }
        try facts.retainRoot(a, &pages, next, &retained);
        const id = try found.body.identity(pages.domain);
        try std.testing.expect(retained.contains(&id));
    }
}

test "serverless document facts packed point blocks need one physical body read after directory admission" {
    const a = std.testing.allocator;
    var memory = @import("objectstore").MemoryClient.init(a);
    defer memory.deinit();
    var impl = try @import("../artifacts/object_store.zig").ObjectStore.initWithClient(a, memory.client(), "artifacts", "point-blocks");
    var artifacts = impl.artifactStore();
    defer artifacts.deinit();
    artifacts.canonical_point_bodies = true;
    artifacts.point_read_blocks = true;
    var reads: u64 = 64 * 1024 * 1024;
    var writes: u64 = reads;
    var pages = page_store.PageStore{ .domain = page_store.PageStore.namespaceDomain("point-blocks"), .attempt = @splat(1), .artifacts = &artifacts, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const m = arena.allocator();
    const docs = try m.alloc(materializer.Document, 128);
    const body: []const u8 = "{\"text\":\"" ++ @as([512]u8, @splat('x')) ++ "\"}";
    for (docs, 0..) |*doc, i| doc.* = .{ .doc_id = try std.fmt.allocPrint(m, "doc/{d:0>4}", .{i}), .body = @constCast(body), .last_lsn = i + 1, .last_timestamp_ns = 1 };
    const source = try publishAlloc(a, &pages, null, docs, null, .{}, "{}", 128);
    defer freeRef(a, source);
    var block_cache: page_store.PackReadCache = .{ .allocator = a };
    defer block_cache.deinit();
    pages.pack_cache = &block_cache;
    const root = try facts.loadRoot(a, &pages, source);
    try std.testing.expect(root.directory_len != 0 and root.page.?.height > 0);
    for ([_]usize{ 0, 127 }) |index| {
        block_cache.clearPack();
        memory.resetOperationCount();
        var cursor = try tree.Cursor.init(a, pages.store(), root.page, docs[index].doc_id, null);
        defer cursor.deinit();
        const row = (try cursor.next()).?;
        try std.testing.expectEqualStrings(docs[index].doc_id, row.key);
        const restored = try facts.readRecordBodyAlloc(a, &pages, row);
        defer a.free(restored);
        try std.testing.expectEqualStrings(body, restored);
        try std.testing.expectEqual(@as(u64, 1), memory.operationCount());
    }
    const Probe = struct {
        fn run(alloc: std.mem.Allocator, backing: page_store.PageStore, captured: facts.Root, id: []const u8, expected: []const u8) !void {
            var allowance: u64 = 1024 * 1024;
            var no_writes: u64 = 0;
            var physical: page_store.PackReadCache = .{ .allocator = alloc };
            defer physical.deinit();
            var query = backing;
            query.remaining_read_bytes = &allowance;
            query.remaining_write_bytes = &no_writes;
            query.pack_cache = &physical;
            const snapshot = captured;
            var cursor = try tree.Cursor.init(alloc, query.store(), snapshot.page, id, null);
            defer cursor.deinit();
            const row = (try cursor.next()).?;
            const restored = try facts.readRecordBodyAlloc(alloc, &query, row);
            defer alloc.free(restored);
            try std.testing.expectEqualStrings(expected, restored);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Probe.run, .{ pages, root, docs[127].doc_id, body });
    const encoded = try artifacts.getAlloc(source.artifact_id);
    defer a.free(encoded);
    encoded[encoded.len - 1] ^= 1;
    try std.testing.expectError(error.ArtifactIntegrityMismatch, facts.Root.decode(encoded));
}

test "serverless document facts inline leaf removes a point dependency with explicit version and checksum" {
    const a = std.testing.allocator;
    var memory = @import("objectstore").MemoryClient.init(a);
    defer memory.deinit();
    var impl = try @import("../artifacts/object_store.zig").ObjectStore.initWithClient(a, memory.client(), "artifacts", "inline-leaf");
    var artifacts = impl.artifactStore();
    defer artifacts.deinit();
    artifacts.canonical_point_bodies = true;
    artifacts.point_read_blocks = true;
    artifacts.inline_fact_leaves = true;
    var reads: u64 = 4 * 1024 * 1024;
    var writes: u64 = reads;
    var pages = page_store.PageStore{ .domain = page_store.PageStore.namespaceDomain("inline-leaf"), .attempt = @splat(1), .artifacts = &artifacts, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
    const body = "{\"text\":\"inline\"}";
    const docs = [_]materializer.Document{.{ .doc_id = @constCast("one"), .body = @constCast(body), .last_lsn = 1, .last_timestamp_ns = 1 }};
    const source = try publishAlloc(a, &pages, null, &docs, null, .{}, "{}", 1);
    defer freeRef(a, source);
    try std.testing.expectEqual(@as(u32, 14), source.metadata_version);
    var cache: page_store.PackReadCache = .{ .allocator = a };
    defer cache.deinit();
    pages.pack_cache = &cache;
    const root = try facts.loadRoot(a, &pages, source);
    try std.testing.expect(root.inline_leaf_format and root.directory_len != 0 and root.page.?.height == 0);
    memory.resetOperationCount();
    var cursor = try tree.Cursor.init(a, pages.store(), root.page, "one", null);
    defer cursor.deinit();
    const record = (try cursor.next()).?;
    const restored = try facts.readRecordBodyAlloc(a, &pages, record);
    defer a.free(restored);
    try std.testing.expectEqualStrings(body, restored);
    try std.testing.expectEqual(@as(u64, 0), memory.operationCount());
    const encoded = try a.alloc(u8, facts.Root.packed_header_bytes + root.directory_len);
    defer a.free(encoded);
    @memcpy(encoded[0..facts.Root.packed_header_bytes], &root.encodePackedHeader());
    @memcpy(encoded[facts.Root.packed_header_bytes..], cache.directory.?.bytes);
    @memcpy(encoded[0..8], "AFDFACTD");
    try std.testing.expectError(error.InvalidDocumentFactsRoot, facts.Root.decode(encoded));
    @memcpy(encoded[0..8], "AFDFACTE");
    encoded[encoded.len - 1] ^= 1;
    try std.testing.expectError(error.ArtifactIntegrityMismatch, facts.Root.decode(encoded));
}
