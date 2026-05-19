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
const api_codec = @import("../api/codec.zig");
const api_types = @import("../api/types.zig");
const artifacts_mod = @import("../artifacts/mod.zig");
const catalog_mod = @import("../catalog/mod.zig");
const document_projection = @import("../document_projection.zig");
const document_segment_mod = @import("../document_segment/mod.zig");
const manifest_mod = @import("../manifest/mod.zig");
const query_mod = @import("../query/mod.zig");
const query_reader = @import("../query/indexed_reader.zig");
const segment_mod = @import("../segment/mod.zig");
const wal_mod = @import("../wal/mod.zig");
const embedder_mod = @import("../../storage/db/enrichment/embedder.zig");

pub const EnrichmentRunStats = struct {
    enriched_namespaces: usize = 0,
    enriched_documents: usize = 0,
    wal_appends: usize = 0,
    model_documents: usize = 0,
    fallback_documents: usize = 0,
    failed_documents: usize = 0,
    stage_failures: usize = 0,
    idle_namespaces: usize = 0,
};

pub const lexical_sparse_enrichment_version: u32 = 1;
pub const chunk_preview_enrichment_version: u32 = 1;
pub const chunk_embeddings_enrichment_version: u32 = 1;
pub const rerank_terms_enrichment_version: u32 = 1;

pub const SparseEnricherConfig = struct {
    batch_size: usize = 32,
    pipeline_version: u32 = lexical_sparse_enrichment_version,
    stage: catalog_mod.EnrichmentStage = .lexical_sparse,
    model_preference: catalog_mod.EnrichmentModelPreference = .prefer_model,
    failure_policy: catalog_mod.EnrichmentFailurePolicy = .skip_document,
};

const DerivedBodyResult = struct {
    body: ?[]u8 = null,
    used_model: bool = false,
    used_fallback: bool = false,
};

const EnrichmentError = error{
    MissingSparseEmbeddingName,
    MissingChunkEmbeddingName,
    RequiredSparseModelUnavailable,
    RequiredChunkEmbeddingModelUnavailable,
    SparseEmbeddingModelFailed,
    ChunkEmbeddingModelFailed,
};

pub const SparseEnricher = struct {
    alloc: Allocator,
    artifacts: *artifacts_mod.ArtifactStore,
    manifests: *manifest_mod.ManifestStore,
    progress: *catalog_mod.ProgressStore,
    wal: *wal_mod.WalStore,
    sparse_embedder: ?embedder_mod.SparseEmbedder = null,
    sparse_embedding_name: ?[]u8 = null,
    chunk_embedder: ?embedder_mod.DenseEmbedder = null,
    chunk_embedding_name: ?[]u8 = null,
    chunk_embedding_dims: u32 = 8,

    pub fn init(
        alloc: Allocator,
        artifacts: *artifacts_mod.ArtifactStore,
        manifests: *manifest_mod.ManifestStore,
        progress: *catalog_mod.ProgressStore,
        wal: *wal_mod.WalStore,
    ) SparseEnricher {
        return .{
            .alloc = alloc,
            .artifacts = artifacts,
            .manifests = manifests,
            .progress = progress,
            .wal = wal,
        };
    }

    pub fn deinit(self: *SparseEnricher) void {
        if (self.sparse_embedder) |embedder| embedder.deinit(self.alloc);
        if (self.sparse_embedding_name) |name| self.alloc.free(name);
        if (self.chunk_embedder) |embedder| embedder.deinit(self.alloc);
        if (self.chunk_embedding_name) |name| self.alloc.free(name);
        self.* = undefined;
    }

    pub fn setSparseEmbedder(self: *SparseEnricher, embedder: embedder_mod.SparseEmbedder, embedding_name: []const u8) !void {
        if (self.sparse_embedder) |current| current.deinit(self.alloc);
        if (self.sparse_embedding_name) |name| self.alloc.free(name);
        self.sparse_embedder = embedder;
        self.sparse_embedding_name = try self.alloc.dupe(u8, embedding_name);
    }

    pub fn clearSparseEmbedder(self: *SparseEnricher) void {
        if (self.sparse_embedder) |current| current.deinit(self.alloc);
        if (self.sparse_embedding_name) |name| self.alloc.free(name);
        self.sparse_embedder = null;
        self.sparse_embedding_name = null;
    }

    pub fn setChunkEmbedder(self: *SparseEnricher, embedder: embedder_mod.DenseEmbedder, embedding_name: []const u8, dims: u32) !void {
        if (self.chunk_embedder) |current| current.deinit(self.alloc);
        if (self.chunk_embedding_name) |name| self.alloc.free(name);
        self.chunk_embedder = embedder;
        self.chunk_embedding_name = try self.alloc.dupe(u8, embedding_name);
        self.chunk_embedding_dims = dims;
    }

    pub fn clearChunkEmbedder(self: *SparseEnricher) void {
        if (self.chunk_embedder) |current| current.deinit(self.alloc);
        if (self.chunk_embedding_name) |name| self.alloc.free(name);
        self.chunk_embedder = null;
        self.chunk_embedding_name = null;
    }

    pub fn runNamespace(self: *SparseEnricher, namespace: []const u8) !EnrichmentRunStats {
        return try self.runNamespaceWithConfig(namespace, .{});
    }

    pub fn runNamespaceWithConfig(self: *SparseEnricher, namespace: []const u8, cfg: SparseEnricherConfig) !EnrichmentRunStats {
        const head = self.progress.getHead(namespace) catch |err| switch (err) {
            error.FileNotFound => return .{ .idle_namespaces = 1 },
            else => return err,
        };
        var manifest = try self.manifests.getAlloc(namespace, head);
        defer manifest.deinit(self.alloc);
        const latest_lsn = try self.wal.latestLsn(namespace);
        if (latest_lsn != manifest.wal_end_lsn) {
            return .{ .idle_namespaces = 1 };
        }

        const docs = try self.loadPublishedDocsAlloc(manifest);
        defer query_mod.freeMaterializedDocuments(self.alloc, docs);

        const stored_head = try self.progress.getEnrichmentStageHeadVersion(namespace, cfg.stage);
        if (stored_head != head) {
            _ = try self.progress.compareAndSwapEnrichmentStageHeadVersion(namespace, cfg.stage, stored_head, head);
            _ = try self.progress.compareAndSwapEnrichmentStageDocOffset(
                namespace,
                cfg.stage,
                try self.progress.getEnrichmentStageDocOffset(namespace, cfg.stage),
                0,
            );
        }
        const start_offset: usize = @intCast((try self.progress.getEnrichmentStageDocOffset(namespace, cfg.stage)) orelse 0);
        if (start_offset >= docs.len) {
            return .{ .idle_namespaces = 1 };
        }

        var stats = EnrichmentRunStats{};
        var next_offset: usize = start_offset;
        for (docs[start_offset..], start_offset..) |doc, doc_index| {
            next_offset = doc_index + 1;
            const derived = buildDerivedBodyAlloc(self, cfg.stage, doc.body, cfg.pipeline_version, cfg.model_preference) catch |err| {
                if (isRecoverableEnrichmentError(err)) {
                    stats.failed_documents += 1;
                    if (cfg.failure_policy == .fail_stage) {
                        stats.stage_failures += 1;
                        return err;
                    }
                    continue;
                }
                return err;
            };
            defer if (derived.body) |body| self.alloc.free(body);
            const body = derived.body orelse continue;
            const mutation = api_types.DocumentMutation{
                .kind = .upsert,
                .doc_id = doc.doc_id,
                .body = body,
            };
            const encoded = try api_codec.encodeMutationAlloc(self.alloc, mutation);
            defer self.alloc.free(encoded);
            _ = try self.wal.append(namespace, doc.last_timestamp_ns + 1, encoded);
            stats.enriched_documents += 1;
            stats.wal_appends += 1;
            if (derived.used_model) stats.model_documents += 1;
            if (derived.used_fallback) stats.fallback_documents += 1;
            if (stats.wal_appends >= cfg.batch_size) break;
        }

        const previous_offset = (try self.progress.getEnrichmentStageDocOffset(namespace, cfg.stage)) orelse 0;
        _ = try self.progress.compareAndSwapEnrichmentStageDocOffset(namespace, cfg.stage, previous_offset, @intCast(next_offset));
        if (stats.enriched_documents == 0) {
            stats.idle_namespaces = 1;
        } else {
            stats.enriched_namespaces = 1;
        }
        return stats;
    }

    fn loadPublishedDocsAlloc(self: *SparseEnricher, manifest: manifest_mod.Manifest) ![]query_mod.QueryMaterializedDocument {
        const document_index = findArtifactIndex(manifest, .document_segment) orelse return error.DocumentSegmentNotFound;
        const payload = try self.artifacts.getAlloc(manifest.artifacts[document_index].artifact_id);
        defer self.alloc.free(payload);
        const entries = try document_segment_mod.decodeAlloc(self.alloc, payload);
        defer document_segment_mod.freeEntries(self.alloc, entries);
        const base_docs = try allocMaterializedDocuments(self.alloc, entries);
        errdefer query_mod.freeMaterializedDocuments(self.alloc, base_docs);

        const mutation_index = findArtifactIndex(manifest, .mutation_segment) orelse return base_docs;
        const mutation_payload = try self.artifacts.getAlloc(manifest.artifacts[mutation_index].artifact_id);
        defer self.alloc.free(mutation_payload);
        const mutation_entries = try segment_mod.decodeAlloc(self.alloc, mutation_payload);
        defer segment_mod.freeEntries(self.alloc, mutation_entries);
        const overlay = try allocMaterializerMutations(self.alloc, mutation_entries);
        defer freeMaterializerMutations(self.alloc, overlay);
        const docs = try query_mod.materializeDocumentsOverBaseAlloc(self.alloc, base_docs, overlay);
        query_mod.freeMaterializedDocuments(self.alloc, base_docs);
        return docs;
    }

    fn allocMaterializedDocuments(alloc: Allocator, entries: []const document_segment_mod.Entry) ![]query_mod.QueryMaterializedDocument {
        const docs = try alloc.alloc(query_mod.QueryMaterializedDocument, entries.len);
        errdefer alloc.free(docs);
        var initialized: usize = 0;
        errdefer {
            for (docs[0..initialized]) |*doc| doc.deinit(alloc);
        }
        for (entries, 0..) |entry, idx| {
            docs[idx] = .{
                .doc_id = try alloc.dupe(u8, entry.doc_id),
                .body = try alloc.dupe(u8, entry.body),
                .last_lsn = entry.last_lsn,
                .last_timestamp_ns = entry.last_timestamp_ns,
            };
            initialized += 1;
        }
        return docs;
    }

    fn allocMaterializerMutations(alloc: Allocator, entries: []const segment_mod.Entry) ![]query_mod.QueryMaterializerMutation {
        const mutations = try alloc.alloc(query_mod.QueryMaterializerMutation, entries.len);
        errdefer alloc.free(mutations);
        var initialized: usize = 0;
        errdefer freeMaterializerMutations(alloc, mutations[0..initialized]);
        for (entries, 0..) |entry, idx| {
            mutations[idx] = .{
                .lsn = entry.lsn,
                .timestamp_ns = entry.timestamp_ns,
                .kind = entry.kind,
                .doc_id = try alloc.dupe(u8, entry.doc_id),
                .body = if (entry.body) |body| try alloc.dupe(u8, body) else null,
            };
            initialized += 1;
        }
        return mutations;
    }

    fn freeMaterializerMutations(alloc: Allocator, mutations: []query_mod.QueryMaterializerMutation) void {
        for (mutations) |mutation| {
            alloc.free(mutation.doc_id);
            if (mutation.body) |body| alloc.free(body);
        }
        alloc.free(mutations);
    }
};

fn findArtifactIndex(manifest: manifest_mod.Manifest, kind: manifest_mod.ArtifactKind) ?usize {
    for (manifest.artifacts, 0..) |artifact, idx| {
        if (artifact.kind == kind) return idx;
    }
    return null;
}

fn buildDerivedBodyAlloc(
    self: *SparseEnricher,
    stage: catalog_mod.EnrichmentStage,
    body: []const u8,
    pipeline_version: u32,
    model_preference: catalog_mod.EnrichmentModelPreference,
) !DerivedBodyResult {
    return switch (stage) {
        .lexical_sparse => try buildDerivedSparseBodyAlloc(self, body, pipeline_version, model_preference),
        .chunk_preview => .{ .body = try buildDerivedChunkPreviewBodyAlloc(self.alloc, body, pipeline_version) },
        .chunk_embeddings => try buildDerivedChunkEmbeddingsBodyAlloc(self, body, pipeline_version, model_preference),
        .rerank_terms => .{ .body = try buildDerivedRerankTermsBodyAlloc(self.alloc, body, pipeline_version) },
    };
}

fn buildDerivedSparseBodyAlloc(
    self: *SparseEnricher,
    body: []const u8,
    pipeline_version: u32,
    model_preference: catalog_mod.EnrichmentModelPreference,
) !DerivedBodyResult {
    const alloc = self.alloc;
    var projection = try document_projection.parseAlloc(alloc, body);
    defer projection.deinit(alloc);
    if (projection.sparse_embedding != null and projection.lexical_sparse_version != null and projection.lexical_sparse_version.? >= pipeline_version) {
        return .{};
    }

    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try appendJSONString(alloc, &out, "{");
    try appendJSONString(alloc, &out, "\"text\":");
    try appendJSONStringValue(alloc, &out, projection.text);
    if (projection.embedding) |embedding| {
        try appendEmbeddingJSON(alloc, &out, "embedding", embedding);
    }
    if (projection.graph_edges_json) |graph_edges_json| {
        try appendJSONString(alloc, &out, ",\"graph_edges\":");
        try appendJSONString(alloc, &out, graph_edges_json);
    }

    var used_model = false;
    var used_fallback = false;
    var encoded_sparse = false;
    if (shouldAttemptModel(model_preference)) {
        switch (model_preference) {
            .prefer_model => {
                if (self.sparse_embedder) |embedder| {
                    const embedding_name = self.sparse_embedding_name orelse return EnrichmentError.MissingSparseEmbeddingName;
                    const sparse = embedder.embedSparse(alloc, embedding_name, projection.text) catch null;
                    if (sparse) |value| {
                        var owned_sparse = value;
                        defer owned_sparse.deinit(alloc);
                        used_model = true;
                        try appendJSONString(alloc, &out, ",\"sparse_embedding\":{");
                        for (owned_sparse.indices, owned_sparse.values, 0..) |index, value2, idx| {
                            if (idx != 0) try appendJSONString(alloc, &out, ",");
                            const feature = try std.fmt.allocPrint(alloc, "f{d}", .{index});
                            defer alloc.free(feature);
                            try appendJSONStringValue(alloc, &out, feature);
                            try appendJSONString(alloc, &out, ":");
                            const num = try std.fmt.allocPrint(alloc, "{d}", .{value2});
                            defer alloc.free(num);
                            try appendJSONString(alloc, &out, num);
                        }
                        encoded_sparse = true;
                    } else {
                        used_fallback = true;
                    }
                } else {
                    used_fallback = true;
                }
            },
            .require_model => {
                const embedder = self.sparse_embedder orelse return EnrichmentError.RequiredSparseModelUnavailable;
                const embedding_name = self.sparse_embedding_name orelse return EnrichmentError.MissingSparseEmbeddingName;
                var sparse = embedder.embedSparse(alloc, embedding_name, projection.text) catch return EnrichmentError.SparseEmbeddingModelFailed;
                defer sparse.deinit(alloc);
                used_model = true;
                try appendJSONString(alloc, &out, ",\"sparse_embedding\":{");
                for (sparse.indices, sparse.values, 0..) |index, value, idx| {
                    if (idx != 0) try appendJSONString(alloc, &out, ",");
                    const feature = try std.fmt.allocPrint(alloc, "f{d}", .{index});
                    defer alloc.free(feature);
                    try appendJSONStringValue(alloc, &out, feature);
                    try appendJSONString(alloc, &out, ":");
                    const num = try std.fmt.allocPrint(alloc, "{d}", .{value});
                    defer alloc.free(num);
                    try appendJSONString(alloc, &out, num);
                }
                encoded_sparse = true;
            },
            .deterministic_only => {},
        }
    }
    if (!encoded_sparse) {
        const normalized = try query_reader.normalizeAlloc(alloc, projection.text);
        defer alloc.free(normalized);
        if (normalized.len == 0) return .{};

        var counts = std.StringArrayHashMapUnmanaged(u32).empty;
        defer {
            for (counts.keys()) |term| alloc.free(term);
            counts.deinit(alloc);
        }

        var token_count: usize = 0;
        var iter = std.mem.tokenizeAny(u8, normalized, " ");
        while (iter.next()) |token| {
            token_count += 1;
            const owned = try alloc.dupe(u8, token);
            errdefer alloc.free(owned);
            const gop = try counts.getOrPut(alloc, owned);
            if (!gop.found_existing) {
                gop.value_ptr.* = 0;
            } else {
                alloc.free(owned);
            }
            gop.value_ptr.* += 1;
        }
        if (token_count == 0 or counts.count() == 0) return .{};

        try appendJSONString(alloc, &out, ",\"sparse_embedding\":{");
        for (counts.keys(), counts.values(), 0..) |term, count, idx| {
            if (idx != 0) try appendJSONString(alloc, &out, ",");
            try appendJSONStringValue(alloc, &out, term);
            try appendJSONString(alloc, &out, ":");
            const weight = @as(f32, @floatFromInt(count)) / @as(f32, @floatFromInt(token_count));
            const num = try std.fmt.allocPrint(alloc, "{d}", .{weight});
            defer alloc.free(num);
            try appendJSONString(alloc, &out, num);
        }
    }
    const version = try std.fmt.allocPrint(alloc, "{d}", .{pipeline_version});
    defer alloc.free(version);
    try appendJSONString(alloc, &out, "},\"_enrichment\":{\"lexical_sparse\":true,\"lexical_sparse_version\":");
    try appendJSONString(alloc, &out, version);
    try appendJSONString(alloc, &out, "}}");
    return .{
        .body = try out.toOwnedSlice(alloc),
        .used_model = used_model,
        .used_fallback = used_fallback,
    };
}

fn buildDerivedChunkPreviewBodyAlloc(alloc: Allocator, body: []const u8, pipeline_version: u32) !?[]u8 {
    var projection = try document_projection.parseAlloc(alloc, body);
    defer projection.deinit(alloc);
    if (projection.chunk_preview_version != null and projection.chunk_preview_version.? >= pipeline_version) {
        return null;
    }

    const normalized = try query_reader.normalizeAlloc(alloc, projection.text);
    defer alloc.free(normalized);
    if (normalized.len == 0) return null;

    const chunks = try buildChunkPreviewAlloc(alloc, normalized, 8);
    defer {
        for (chunks) |chunk| alloc.free(chunk);
        alloc.free(chunks);
    }
    if (chunks.len == 0) return null;

    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try appendJSONString(alloc, &out, "{");
    try appendJSONString(alloc, &out, "\"text\":");
    try appendJSONStringValue(alloc, &out, projection.text);
    if (projection.embedding) |embedding| {
        try appendJSONString(alloc, &out, ",\"embedding\":[");
        for (embedding, 0..) |value, idx| {
            if (idx != 0) try appendJSONString(alloc, &out, ",");
            const num = try std.fmt.allocPrint(alloc, "{d}", .{value});
            defer alloc.free(num);
            try appendJSONString(alloc, &out, num);
        }
        try appendJSONString(alloc, &out, "]");
    }
    if (projection.sparse_embedding) |weights| {
        try appendJSONString(alloc, &out, ",\"sparse_embedding\":{");
        for (weights, 0..) |weight, idx| {
            if (idx != 0) try appendJSONString(alloc, &out, ",");
            try appendJSONStringValue(alloc, &out, weight.term);
            try appendJSONString(alloc, &out, ":");
            const num = try std.fmt.allocPrint(alloc, "{d}", .{weight.weight});
            defer alloc.free(num);
            try appendJSONString(alloc, &out, num);
        }
        try appendJSONString(alloc, &out, "}");
    }
    if (projection.graph_edges_json) |graph_edges_json| {
        try appendJSONString(alloc, &out, ",\"graph_edges\":");
        try appendJSONString(alloc, &out, graph_edges_json);
    }
    try appendJSONString(alloc, &out, ",\"chunk_preview\":");
    try appendStringSliceArrayJSON(alloc, &out, chunks);
    const version = try std.fmt.allocPrint(alloc, "{d}", .{pipeline_version});
    defer alloc.free(version);
    try appendJSONString(alloc, &out, ",\"_enrichment\":{\"chunk_preview\":true,\"chunk_preview_version\":");
    try appendJSONString(alloc, &out, version);
    if (projection.lexical_sparse_version) |lexical_version| {
        const lexical = try std.fmt.allocPrint(alloc, "{d}", .{lexical_version});
        defer alloc.free(lexical);
        try appendJSONString(alloc, &out, ",\"lexical_sparse\":true,\"lexical_sparse_version\":");
        try appendJSONString(alloc, &out, lexical);
    }
    try appendJSONString(alloc, &out, "}}");
    return try out.toOwnedSlice(alloc);
}

fn buildDerivedChunkEmbeddingsBodyAlloc(
    self: *SparseEnricher,
    body: []const u8,
    pipeline_version: u32,
    model_preference: catalog_mod.EnrichmentModelPreference,
) !DerivedBodyResult {
    const alloc = self.alloc;
    var projection = try document_projection.parseAlloc(alloc, body);
    defer projection.deinit(alloc);
    if (projection.chunk_embeddings_version != null and projection.chunk_embeddings_version.? >= pipeline_version) {
        return .{};
    }

    var owned_chunks: ?[][]u8 = null;
    const chunks = if (projection.chunk_preview) |chunks|
        chunks
    else blk: {
        const normalized = try query_reader.normalizeAlloc(alloc, projection.text);
        defer alloc.free(normalized);
        if (normalized.len == 0) return .{};
        owned_chunks = try buildChunkPreviewAlloc(alloc, normalized, 8);
        break :blk owned_chunks.?;
    };
    defer if (owned_chunks) |value| {
        for (value) |chunk| alloc.free(chunk);
        alloc.free(value);
    };
    if (chunks.len == 0) return .{};

    const chunk_embedding_result = try buildChunkEmbeddingsAlloc(
        self,
        chunks,
        self.chunk_embedding_dims,
        model_preference,
    );
    const chunk_embeddings = chunk_embedding_result.embeddings;
    defer {
        for (chunk_embeddings) |*chunk_embedding| chunk_embedding.deinit(alloc);
        alloc.free(chunk_embeddings);
    }

    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try appendJSONString(alloc, &out, "{");
    try appendJSONString(alloc, &out, "\"text\":");
    try appendJSONStringValue(alloc, &out, projection.text);
    if (projection.embedding) |embedding| {
        try appendEmbeddingJSON(alloc, &out, "embedding", embedding);
    }
    if (projection.sparse_embedding) |weights| {
        try appendSparseEmbeddingJSON(alloc, &out, weights);
    }
    if (projection.graph_edges_json) |graph_edges_json| {
        try appendJSONString(alloc, &out, ",\"graph_edges\":");
        try appendJSONString(alloc, &out, graph_edges_json);
    }
    try appendJSONString(alloc, &out, ",\"chunk_preview\":");
    try appendStringSliceArrayJSON(alloc, &out, chunks);
    try appendJSONString(alloc, &out, ",\"chunk_embeddings\":");
    try appendChunkEmbeddingsJSON(alloc, &out, chunk_embeddings);
    const version = try std.fmt.allocPrint(alloc, "{d}", .{pipeline_version});
    defer alloc.free(version);
    try appendJSONString(alloc, &out, ",\"_enrichment\":{\"chunk_embeddings\":true,\"chunk_embeddings_version\":");
    try appendJSONString(alloc, &out, version);
    if (projection.lexical_sparse_version) |lexical_version| {
        const lexical = try std.fmt.allocPrint(alloc, "{d}", .{lexical_version});
        defer alloc.free(lexical);
        try appendJSONString(alloc, &out, ",\"lexical_sparse\":true,\"lexical_sparse_version\":");
        try appendJSONString(alloc, &out, lexical);
    }
    if (projection.chunk_preview_version) |chunk_version| {
        const chunk = try std.fmt.allocPrint(alloc, "{d}", .{chunk_version});
        defer alloc.free(chunk);
        try appendJSONString(alloc, &out, ",\"chunk_preview\":true,\"chunk_preview_version\":");
        try appendJSONString(alloc, &out, chunk);
    }
    try appendJSONString(alloc, &out, "}}");
    return .{
        .body = try out.toOwnedSlice(alloc),
        .used_model = chunk_embedding_result.used_model,
        .used_fallback = chunk_embedding_result.used_fallback,
    };
}

fn buildDerivedRerankTermsBodyAlloc(alloc: Allocator, body: []const u8, pipeline_version: u32) !?[]u8 {
    var projection = try document_projection.parseAlloc(alloc, body);
    defer projection.deinit(alloc);
    if (projection.rerank_terms_version != null and projection.rerank_terms_version.? >= pipeline_version) {
        return null;
    }

    const normalized = try query_reader.normalizeAlloc(alloc, projection.text);
    defer alloc.free(normalized);
    if (normalized.len == 0) return null;

    const rerank_terms = try buildRerankTermsAlloc(alloc, normalized, projection.sparse_embedding, 8);
    defer {
        for (rerank_terms) |term| alloc.free(term);
        alloc.free(rerank_terms);
    }
    if (rerank_terms.len == 0) return null;

    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try appendJSONString(alloc, &out, "{");
    try appendJSONString(alloc, &out, "\"text\":");
    try appendJSONStringValue(alloc, &out, projection.text);
    if (projection.embedding) |embedding| {
        try appendJSONString(alloc, &out, ",\"embedding\":[");
        for (embedding, 0..) |value, idx| {
            if (idx != 0) try appendJSONString(alloc, &out, ",");
            const num = try std.fmt.allocPrint(alloc, "{d}", .{value});
            defer alloc.free(num);
            try appendJSONString(alloc, &out, num);
        }
        try appendJSONString(alloc, &out, "]");
    }
    if (projection.sparse_embedding) |weights| {
        try appendSparseEmbeddingJSON(alloc, &out, weights);
    }
    if (projection.graph_edges_json) |graph_edges_json| {
        try appendJSONString(alloc, &out, ",\"graph_edges\":");
        try appendJSONString(alloc, &out, graph_edges_json);
    }
    if (projection.chunk_preview) |chunks| {
        try appendJSONString(alloc, &out, ",\"chunk_preview\":");
        try appendStringSliceArrayJSON(alloc, &out, chunks);
    }
    if (projection.chunk_embeddings) |chunk_embeddings| {
        try appendJSONString(alloc, &out, ",\"chunk_embeddings\":");
        try appendChunkEmbeddingsJSON(alloc, &out, chunk_embeddings);
    }
    try appendJSONString(alloc, &out, ",\"rerank_terms\":[");
    try appendStringSliceArrayJSON(alloc, &out, rerank_terms);
    const version = try std.fmt.allocPrint(alloc, "{d}", .{pipeline_version});
    defer alloc.free(version);
    try appendJSONString(alloc, &out, "],\"_enrichment\":{\"rerank_terms\":true,\"rerank_terms_version\":");
    try appendJSONString(alloc, &out, version);
    if (projection.lexical_sparse_version) |lexical_version| {
        const lexical = try std.fmt.allocPrint(alloc, "{d}", .{lexical_version});
        defer alloc.free(lexical);
        try appendJSONString(alloc, &out, ",\"lexical_sparse\":true,\"lexical_sparse_version\":");
        try appendJSONString(alloc, &out, lexical);
    }
    if (projection.chunk_preview_version) |chunk_version| {
        const chunk = try std.fmt.allocPrint(alloc, "{d}", .{chunk_version});
        defer alloc.free(chunk);
        try appendJSONString(alloc, &out, ",\"chunk_preview\":true,\"chunk_preview_version\":");
        try appendJSONString(alloc, &out, chunk);
    }
    if (projection.chunk_embeddings_version) |chunk_embeddings_version| {
        const chunk_embeddings = try std.fmt.allocPrint(alloc, "{d}", .{chunk_embeddings_version});
        defer alloc.free(chunk_embeddings);
        try appendJSONString(alloc, &out, ",\"chunk_embeddings\":true,\"chunk_embeddings_version\":");
        try appendJSONString(alloc, &out, chunk_embeddings);
    }
    try appendJSONString(alloc, &out, "}}");
    return try out.toOwnedSlice(alloc);
}

fn buildChunkPreviewAlloc(alloc: Allocator, normalized: []const u8, words_per_chunk: usize) ![][]u8 {
    var chunks = std.ArrayListUnmanaged([]u8).empty;
    errdefer {
        for (chunks.items) |chunk| alloc.free(chunk);
        chunks.deinit(alloc);
    }

    var iter = std.mem.tokenizeAny(u8, normalized, " ");
    var current = std.ArrayListUnmanaged(u8).empty;
    defer current.deinit(alloc);
    var word_count: usize = 0;
    while (iter.next()) |token| {
        if (word_count != 0) try current.append(alloc, ' ');
        try current.appendSlice(alloc, token);
        word_count += 1;
        if (word_count >= words_per_chunk) {
            try chunks.append(alloc, try current.toOwnedSlice(alloc));
            current = .empty;
            word_count = 0;
        }
    }
    if (current.items.len != 0) {
        try chunks.append(alloc, try current.toOwnedSlice(alloc));
    }
    return try chunks.toOwnedSlice(alloc);
}

fn buildChunkEmbeddingsAlloc(
    self: *SparseEnricher,
    chunks: []const []const u8,
    dims: usize,
    model_preference: catalog_mod.EnrichmentModelPreference,
) !struct {
    embeddings: []document_projection.ChunkEmbedding,
    used_model: bool,
    used_fallback: bool,
} {
    const alloc = self.alloc;
    const out = try alloc.alloc(document_projection.ChunkEmbedding, chunks.len);
    errdefer alloc.free(out);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*chunk_embedding| chunk_embedding.deinit(alloc);
    }
    var used_model = false;
    var used_fallback = false;
    for (chunks, 0..) |chunk, idx| {
        const embedding_result = try buildDenseEmbeddingAlloc(self, chunk, dims, model_preference);
        out[idx] = .{
            .chunk = try alloc.dupe(u8, chunk),
            .embedding = embedding_result.embedding,
        };
        used_model = used_model or embedding_result.used_model;
        used_fallback = used_fallback or embedding_result.used_fallback;
        initialized += 1;
    }
    return .{
        .embeddings = out,
        .used_model = used_model,
        .used_fallback = used_fallback,
    };
}

fn shouldAttemptModel(model_preference: catalog_mod.EnrichmentModelPreference) bool {
    return switch (model_preference) {
        .deterministic_only => false,
        .prefer_model, .require_model => true,
    };
}

fn buildDenseEmbeddingAlloc(
    self: *SparseEnricher,
    text: []const u8,
    dims: usize,
    model_preference: catalog_mod.EnrichmentModelPreference,
) !struct {
    embedding: []f32,
    used_model: bool,
    used_fallback: bool,
} {
    const alloc = self.alloc;
    switch (model_preference) {
        .deterministic_only => return .{
            .embedding = try buildDeterministicEmbeddingAlloc(alloc, text, dims),
            .used_model = false,
            .used_fallback = false,
        },
        .prefer_model => {
            if (self.chunk_embedder) |embedder| {
                const embedding_name = self.chunk_embedding_name orelse return EnrichmentError.MissingChunkEmbeddingName;
                const model_result = embedder.embedDense(alloc, embedding_name, text, @intCast(dims)) catch null;
                if (model_result) |result| {
                    return .{ .embedding = result, .used_model = true, .used_fallback = false };
                }
                return .{
                    .embedding = try buildDeterministicEmbeddingAlloc(alloc, text, dims),
                    .used_model = false,
                    .used_fallback = true,
                };
            }
            return .{
                .embedding = try buildDeterministicEmbeddingAlloc(alloc, text, dims),
                .used_model = false,
                .used_fallback = true,
            };
        },
        .require_model => {
            const embedder = self.chunk_embedder orelse return EnrichmentError.RequiredChunkEmbeddingModelUnavailable;
            const embedding_name = self.chunk_embedding_name orelse return EnrichmentError.MissingChunkEmbeddingName;
            const result = embedder.embedDense(alloc, embedding_name, text, @intCast(dims)) catch return EnrichmentError.ChunkEmbeddingModelFailed;
            return .{ .embedding = result, .used_model = true, .used_fallback = false };
        },
    }
}

fn isRecoverableEnrichmentError(err: anyerror) bool {
    return err == EnrichmentError.MissingSparseEmbeddingName or
        err == EnrichmentError.MissingChunkEmbeddingName or
        err == EnrichmentError.RequiredSparseModelUnavailable or
        err == EnrichmentError.RequiredChunkEmbeddingModelUnavailable or
        err == EnrichmentError.SparseEmbeddingModelFailed or
        err == EnrichmentError.ChunkEmbeddingModelFailed;
}

const FailingDenseEmbedder = struct {
    fn embedDense(_: *anyopaque, _: Allocator, _: []const u8, _: []const u8, _: u32) ![]f32 {
        return error.TestDenseModelFailure;
    }

    fn interface() embedder_mod.DenseEmbedder {
        return .{
            .ptr = undefined,
            .dense_embed_fn = embedDense,
            .deinit_fn = null,
        };
    }
};

const FailingSparseEmbedder = struct {
    fn embedSparse(_: *anyopaque, _: Allocator, _: []const u8, _: []const u8) !embedder_mod.SparseEmbedding {
        return error.TestSparseModelFailure;
    }

    fn interface() embedder_mod.SparseEmbedder {
        return .{
            .ptr = undefined,
            .sparse_embed_fn = embedSparse,
            .deinit_fn = null,
        };
    }
};

fn buildDeterministicEmbeddingAlloc(alloc: Allocator, text: []const u8, dims: usize) ![]f32 {
    const embedding = try alloc.alloc(f32, dims);
    @memset(embedding, 0);

    var token_count: usize = 0;
    var iter = std.mem.tokenizeAny(u8, text, " ");
    while (iter.next()) |token| {
        token_count += 1;
        for (0..dims) |dim| {
            var hasher = std.hash.Wyhash.init(@as(u64, @intCast(dim + 1)));
            hasher.update(token);
            const hashed = hasher.final();
            const low_bits: u32 = @truncate(hashed & 0xffff);
            const centered_i32: i32 = @as(i32, @intCast(low_bits)) - 32768;
            const centered = @as(f32, @floatFromInt(centered_i32));
            embedding[dim] += centered / 32768.0;
        }
    }
    if (token_count == 0) return embedding;
    const denom = @as(f32, @floatFromInt(token_count));
    var norm: f32 = 0;
    for (embedding) |*value| {
        value.* /= denom;
        norm += value.* * value.*;
    }
    if (norm > 0) {
        const scale = @as(f32, 1.0) / @sqrt(norm);
        for (embedding) |*value| value.* *= scale;
    }
    return embedding;
}

const WeightedTerm = struct {
    term: []const u8,
    weight: f32,
};

fn buildRerankTermsAlloc(
    alloc: Allocator,
    normalized: []const u8,
    sparse_embedding: ?[]const document_projection.SparseTermWeight,
    limit: usize,
) ![][]u8 {
    if (sparse_embedding) |weights| {
        var ranked = try alloc.alloc(WeightedTerm, weights.len);
        defer alloc.free(ranked);
        for (weights, 0..) |weight, idx| {
            ranked[idx] = .{ .term = weight.term, .weight = weight.weight };
        }
        std.mem.sort(WeightedTerm, ranked, {}, struct {
            fn lessThan(_: void, a: WeightedTerm, b: WeightedTerm) bool {
                if (a.weight == b.weight) return std.mem.lessThan(u8, a.term, b.term);
                return a.weight > b.weight;
            }
        }.lessThan);
        const count = @min(limit, ranked.len);
        const out = try alloc.alloc([]u8, count);
        var initialized: usize = 0;
        errdefer {
            for (out[0..initialized]) |term| alloc.free(term);
            alloc.free(out);
        }
        for (ranked[0..count], 0..) |entry, idx| {
            out[idx] = try alloc.dupe(u8, entry.term);
            initialized += 1;
        }
        return out;
    }

    var counts = std.StringArrayHashMapUnmanaged(u32).empty;
    defer {
        for (counts.keys()) |term| alloc.free(term);
        counts.deinit(alloc);
    }
    var iter = std.mem.tokenizeAny(u8, normalized, " ");
    while (iter.next()) |token| {
        const owned = try alloc.dupe(u8, token);
        errdefer alloc.free(owned);
        const gop = try counts.getOrPut(alloc, owned);
        if (!gop.found_existing) {
            gop.value_ptr.* = 0;
        } else {
            alloc.free(owned);
        }
        gop.value_ptr.* += 1;
    }
    var ranked = try alloc.alloc(WeightedTerm, counts.count());
    defer alloc.free(ranked);
    for (counts.keys(), counts.values(), 0..) |term, count, idx| {
        ranked[idx] = .{ .term = term, .weight = @floatFromInt(count) };
    }
    std.mem.sort(WeightedTerm, ranked, {}, struct {
        fn lessThan(_: void, a: WeightedTerm, b: WeightedTerm) bool {
            if (a.weight == b.weight) return std.mem.lessThan(u8, a.term, b.term);
            return a.weight > b.weight;
        }
    }.lessThan);
    const count = @min(limit, ranked.len);
    const out = try alloc.alloc([]u8, count);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |term| alloc.free(term);
        alloc.free(out);
    }
    for (ranked[0..count], 0..) |entry, idx| {
        out[idx] = try alloc.dupe(u8, entry.term);
        initialized += 1;
    }
    return out;
}

fn appendJSONString(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: []const u8) !void {
    try out.appendSlice(alloc, value);
}

fn appendJSONStringValue(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: []const u8) !void {
    var writer: std.Io.Writer.Allocating = .init(alloc);
    defer writer.deinit();
    try std.json.Stringify.value(value, .{}, &writer.writer);
    try out.appendSlice(alloc, writer.written());
}

fn appendEmbeddingJSON(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), key: []const u8, embedding: []const f32) !void {
    try appendJSONString(alloc, out, ",\"");
    try appendJSONString(alloc, out, key);
    try appendJSONString(alloc, out, "\":[");
    for (embedding, 0..) |value, idx| {
        if (idx != 0) try appendJSONString(alloc, out, ",");
        const num = try std.fmt.allocPrint(alloc, "{d}", .{value});
        defer alloc.free(num);
        try appendJSONString(alloc, out, num);
    }
    try appendJSONString(alloc, out, "]");
}

fn appendSparseEmbeddingJSON(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    weights: []const document_projection.SparseTermWeight,
) !void {
    try appendJSONString(alloc, out, ",\"sparse_embedding\":{");
    for (weights, 0..) |weight, idx| {
        if (idx != 0) try appendJSONString(alloc, out, ",");
        try appendJSONStringValue(alloc, out, weight.term);
        try appendJSONString(alloc, out, ":");
        const num = try std.fmt.allocPrint(alloc, "{d}", .{weight.weight});
        defer alloc.free(num);
        try appendJSONString(alloc, out, num);
    }
    try appendJSONString(alloc, out, "}");
}

fn appendStringSliceArrayJSON(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    values: []const []const u8,
) !void {
    try appendJSONString(alloc, out, "[");
    for (values, 0..) |value, idx| {
        if (idx != 0) try appendJSONString(alloc, out, ",");
        try appendJSONStringValue(alloc, out, value);
    }
    try appendJSONString(alloc, out, "]");
}

fn appendChunkEmbeddingsJSON(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    chunk_embeddings: []const document_projection.ChunkEmbedding,
) !void {
    try appendJSONString(alloc, out, "[");
    for (chunk_embeddings, 0..) |chunk_embedding, idx| {
        if (idx != 0) try appendJSONString(alloc, out, ",");
        try appendJSONString(alloc, out, "{\"chunk\":");
        try appendJSONStringValue(alloc, out, chunk_embedding.chunk);
        try appendJSONString(alloc, out, ",\"embedding\":[");
        for (chunk_embedding.embedding, 0..) |value, emb_idx| {
            if (emb_idx != 0) try appendJSONString(alloc, out, ",");
            const num = try std.fmt.allocPrint(alloc, "{d}", .{value});
            defer alloc.free(num);
            try appendJSONString(alloc, out, num);
        }
        try appendJSONString(alloc, out, "]}");
    }
    try appendJSONString(alloc, out, "]");
}

test "sparse enricher appends derived sparse mutation when published docs lack sparse features" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests");
    const wal_root = tmpPath(&wal_root_buf, "wal");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);

    var fs_artifacts = try artifacts_mod.FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try catalog_mod.FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try wal_mod.FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var builder = @import("../build/builder.zig").Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var api = @import("../api/service.zig").Service.init(alloc, &wal_store, &builder);

    const batch = [_]api_types.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "{\"text\":\"alpha bravo alpha\"}" },
    };
    var ingest = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 100, .mutations = &batch });
    defer ingest.deinit(alloc);
    var build = try builder.publishNamespace("docs");
    defer build.deinit(alloc);

    var enricher = SparseEnricher.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    const stats = try enricher.runNamespaceWithConfig("docs", .{
        .batch_size = 4,
        .pipeline_version = lexical_sparse_enrichment_version,
    });
    try std.testing.expectEqual(@as(usize, 1), stats.enriched_namespaces);
    try std.testing.expectEqual(@as(usize, 1), stats.enriched_documents);

    const tail = try wal_store.readFromAlloc("docs", 2);
    defer wal_mod.freeRecords(alloc, tail);
    try std.testing.expectEqual(@as(usize, 1), tail.len);
    var mutation = try api_codec.decodeMutationAlloc(alloc, tail[0].payload);
    defer mutation.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, mutation.body.?, "\"sparse_embedding\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, mutation.body.?, "\"alpha\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, mutation.body.?, "\"lexical_sparse_version\":1") != null);
}

test "sparse enricher can append derived chunk preview mutation" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-chunk-preview");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-chunk-preview");
    const wal_root = tmpPath(&wal_root_buf, "wal-chunk-preview");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);

    var fs_artifacts = try artifacts_mod.FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try catalog_mod.FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try wal_mod.FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var builder = @import("../build/builder.zig").Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var api = @import("../api/service.zig").Service.init(alloc, &wal_store, &builder);

    const batch = [_]api_types.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "{\"text\":\"alpha bravo charlie delta echo foxtrot golf hotel india\",\"graph_edges\":[{\"target\":\"doc-b\",\"edge_type\":\"cites\",\"weight\":1.0}]}" },
    };
    var ingest = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 100, .mutations = &batch });
    defer ingest.deinit(alloc);
    var build = try builder.publishNamespace("docs");
    defer build.deinit(alloc);

    var enricher = SparseEnricher.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    const stats = try enricher.runNamespaceWithConfig("docs", .{
        .batch_size = 4,
        .pipeline_version = chunk_preview_enrichment_version,
        .stage = .chunk_preview,
    });
    try std.testing.expectEqual(@as(usize, 1), stats.enriched_namespaces);
    try std.testing.expectEqual(@as(usize, 1), stats.enriched_documents);

    const tail = try wal_store.readFromAlloc("docs", 2);
    defer wal_mod.freeRecords(alloc, tail);
    try std.testing.expectEqual(@as(usize, 1), tail.len);
    var mutation = try api_codec.decodeMutationAlloc(alloc, tail[0].payload);
    defer mutation.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, mutation.body.?, "\"chunk_preview\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, mutation.body.?, "\"graph_edges\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, mutation.body.?, "\"chunk_preview_version\":1") != null);
}

test "sparse enricher can append derived rerank terms mutation" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-rerank-terms");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-rerank-terms");
    const wal_root = tmpPath(&wal_root_buf, "wal-rerank-terms");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);

    var fs_artifacts = try artifacts_mod.FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try catalog_mod.FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try wal_mod.FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var builder = @import("../build/builder.zig").Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var api = @import("../api/service.zig").Service.init(alloc, &wal_store, &builder);

    const batch = [_]api_types.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "{\"text\":\"alpha bravo alpha charlie\",\"sparse_embedding\":{\"alpha\":0.9,\"charlie\":0.4,\"bravo\":0.5},\"_enrichment\":{\"lexical_sparse\":true,\"lexical_sparse_version\":1,\"chunk_preview\":true,\"chunk_preview_version\":1}}" },
    };
    var ingest = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 100, .mutations = &batch });
    defer ingest.deinit(alloc);
    var build = try builder.publishNamespace("docs");
    defer build.deinit(alloc);

    var enricher = SparseEnricher.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    const stats = try enricher.runNamespaceWithConfig("docs", .{
        .batch_size = 4,
        .pipeline_version = rerank_terms_enrichment_version,
        .stage = .rerank_terms,
    });
    try std.testing.expectEqual(@as(usize, 1), stats.enriched_namespaces);
    try std.testing.expectEqual(@as(usize, 1), stats.enriched_documents);

    const tail = try wal_store.readFromAlloc("docs", 2);
    defer wal_mod.freeRecords(alloc, tail);
    try std.testing.expectEqual(@as(usize, 1), tail.len);
    var mutation = try api_codec.decodeMutationAlloc(alloc, tail[0].payload);
    defer mutation.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, mutation.body.?, "\"rerank_terms\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, mutation.body.?, "\"alpha\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, mutation.body.?, "\"rerank_terms_version\":1") != null);
}

test "sparse enricher can append derived chunk embeddings mutation" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-chunk-embeddings");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-chunk-embeddings");
    const wal_root = tmpPath(&wal_root_buf, "wal-chunk-embeddings");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);

    var fs_artifacts = try artifacts_mod.FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try catalog_mod.FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try wal_mod.FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var builder = @import("../build/builder.zig").Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var api = @import("../api/service.zig").Service.init(alloc, &wal_store, &builder);

    const batch = [_]api_types.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "{\"text\":\"alpha bravo charlie delta echo foxtrot golf hotel india\",\"sparse_embedding\":{\"alpha\":1.0},\"graph_edges\":[{\"target\":\"doc-b\",\"edge_type\":\"cites\",\"weight\":1.0}],\"chunk_preview\":[\"alpha bravo charlie delta echo foxtrot golf hotel\",\"india\"],\"_enrichment\":{\"lexical_sparse\":true,\"lexical_sparse_version\":1,\"chunk_preview\":true,\"chunk_preview_version\":1}}" },
    };
    var ingest = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 100, .mutations = &batch });
    defer ingest.deinit(alloc);
    var build = try builder.publishNamespace("docs");
    defer build.deinit(alloc);

    var enricher = SparseEnricher.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    const stats = try enricher.runNamespaceWithConfig("docs", .{
        .batch_size = 4,
        .pipeline_version = chunk_embeddings_enrichment_version,
        .stage = .chunk_embeddings,
    });
    try std.testing.expectEqual(@as(usize, 1), stats.enriched_namespaces);
    try std.testing.expectEqual(@as(usize, 1), stats.enriched_documents);

    const tail = try wal_store.readFromAlloc("docs", 2);
    defer wal_mod.freeRecords(alloc, tail);
    try std.testing.expectEqual(@as(usize, 1), tail.len);
    var mutation = try api_codec.decodeMutationAlloc(alloc, tail[0].payload);
    defer mutation.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, mutation.body.?, "\"chunk_embeddings\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, mutation.body.?, "\"graph_edges\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, mutation.body.?, "\"chunk_embeddings_version\":1") != null);
}

test "sparse enricher idles when unpublished tail already exists" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-tail");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-tail");
    const wal_root = tmpPath(&wal_root_buf, "wal-tail");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);

    var fs_artifacts = try artifacts_mod.FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try catalog_mod.FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try wal_mod.FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var builder = @import("../build/builder.zig").Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var api = @import("../api/service.zig").Service.init(alloc, &wal_store, &builder);

    const batch = [_]api_types.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "{\"text\":\"alpha\"}" },
    };
    var ingest = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 100, .mutations = &batch });
    defer ingest.deinit(alloc);
    var build = try builder.publishNamespace("docs");
    defer build.deinit(alloc);
    var tail_ingest = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 101, .mutations = &batch });
    defer tail_ingest.deinit(alloc);

    var enricher = SparseEnricher.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    const stats = try enricher.runNamespaceWithConfig("docs", .{
        .batch_size = 4,
        .pipeline_version = lexical_sparse_enrichment_version,
    });
    try std.testing.expectEqual(@as(usize, 0), stats.enriched_documents);
    try std.testing.expectEqual(@as(usize, 1), stats.idle_namespaces);
}

test "sparse enricher skips docs already enriched at current version" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-current");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-current");
    const wal_root = tmpPath(&wal_root_buf, "wal-current");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);

    var fs_artifacts = try artifacts_mod.FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try catalog_mod.FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try wal_mod.FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var builder = @import("../build/builder.zig").Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var api = @import("../api/service.zig").Service.init(alloc, &wal_store, &builder);

    const batch = [_]api_types.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "{\"text\":\"alpha bravo alpha\",\"sparse_embedding\":{\"alpha\":0.66},\"_enrichment\":{\"lexical_sparse\":true,\"lexical_sparse_version\":1}}" },
    };
    var ingest = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 100, .mutations = &batch });
    defer ingest.deinit(alloc);
    var build = try builder.publishNamespace("docs");
    defer build.deinit(alloc);

    var enricher = SparseEnricher.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    const stats = try enricher.runNamespaceWithConfig("docs", .{
        .batch_size = 4,
        .pipeline_version = lexical_sparse_enrichment_version,
    });
    try std.testing.expectEqual(@as(usize, 0), stats.enriched_documents);
    try std.testing.expectEqual(@as(usize, 1), stats.idle_namespaces);
    try std.testing.expectEqual(@as(u64, 1), try wal_store.latestLsn("docs"));
}

test "sparse enricher can use model-backed dense and sparse embedders" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-model-backed");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-model-backed");
    const wal_root = tmpPath(&wal_root_buf, "wal-model-backed");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);

    var fs_artifacts = try artifacts_mod.FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try catalog_mod.FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try wal_mod.FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var builder = @import("../build/builder.zig").Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var api = @import("../api/service.zig").Service.init(alloc, &wal_store, &builder);

    const batch = [_]api_types.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "{\"text\":\"alpha bravo charlie delta echo foxtrot golf hotel india\"}" },
    };
    var ingest = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 100, .mutations = &batch });
    defer ingest.deinit(alloc);
    var build = try builder.publishNamespace("docs");
    defer build.deinit(alloc);

    var deterministic_sparse = embedder_mod.DeterministicSparseEmbedder{};
    var deterministic_dense = embedder_mod.DeterministicDenseEmbedder{};
    var enricher = SparseEnricher.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    defer enricher.deinit();
    try enricher.setSparseEmbedder(deterministic_sparse.interface(), "serverless_sparse");
    try enricher.setChunkEmbedder(deterministic_dense.interface(), "serverless_chunk", 6);

    const sparse_stats = try enricher.runNamespaceWithConfig("docs", .{
        .batch_size = 4,
        .pipeline_version = lexical_sparse_enrichment_version,
        .stage = .lexical_sparse,
        .model_preference = .prefer_model,
    });
    try std.testing.expectEqual(@as(usize, 1), sparse_stats.enriched_documents);
    try std.testing.expectEqual(@as(usize, 1), sparse_stats.model_documents);
    try std.testing.expectEqual(@as(usize, 0), sparse_stats.fallback_documents);

    var build_after_sparse = try builder.publishNamespace("docs");
    defer build_after_sparse.deinit(alloc);

    const chunk_stats = try enricher.runNamespaceWithConfig("docs", .{
        .batch_size = 4,
        .pipeline_version = chunk_embeddings_enrichment_version,
        .stage = .chunk_embeddings,
        .model_preference = .prefer_model,
    });
    try std.testing.expectEqual(@as(usize, 1), chunk_stats.enriched_documents);
    try std.testing.expectEqual(@as(usize, 1), chunk_stats.model_documents);
    try std.testing.expectEqual(@as(usize, 0), chunk_stats.fallback_documents);

    const tail = try wal_store.readFromAlloc("docs", 3);
    defer wal_mod.freeRecords(alloc, tail);
    try std.testing.expectEqual(@as(usize, 1), tail.len);
    var mutation = try api_codec.decodeMutationAlloc(alloc, tail[0].payload);
    defer mutation.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, mutation.body.?, "\"chunk_embeddings\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, mutation.body.?, "\"f") != null);
}

test "sparse enricher prefers model but falls back deterministically when sparse model fails" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-sparse-fallback");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-sparse-fallback");
    const wal_root = tmpPath(&wal_root_buf, "wal-sparse-fallback");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);

    var fs_artifacts = try artifacts_mod.FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try catalog_mod.FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try wal_mod.FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var builder = @import("../build/builder.zig").Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var api = @import("../api/service.zig").Service.init(alloc, &wal_store, &builder);

    const batch = [_]api_types.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "{\"text\":\"alpha bravo\"}" },
    };
    var ingest = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 100, .mutations = &batch });
    defer ingest.deinit(alloc);
    var build = try builder.publishNamespace("docs");
    defer build.deinit(alloc);

    var enricher = SparseEnricher.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    defer enricher.deinit();
    try enricher.setSparseEmbedder(FailingSparseEmbedder.interface(), "serverless_sparse");

    const stats = try enricher.runNamespaceWithConfig("docs", .{
        .batch_size = 4,
        .pipeline_version = lexical_sparse_enrichment_version,
        .stage = .lexical_sparse,
        .model_preference = .prefer_model,
    });
    try std.testing.expectEqual(@as(usize, 1), stats.enriched_documents);
    try std.testing.expectEqual(@as(usize, 0), stats.model_documents);
    try std.testing.expectEqual(@as(usize, 1), stats.fallback_documents);
    try std.testing.expectEqual(@as(usize, 0), stats.failed_documents);
}

test "sparse enricher can require chunk embedding model and fail stage" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-require-chunk-model");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-require-chunk-model");
    const wal_root = tmpPath(&wal_root_buf, "wal-require-chunk-model");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);

    var fs_artifacts = try artifacts_mod.FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try catalog_mod.FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try wal_mod.FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var builder = @import("../build/builder.zig").Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var api = @import("../api/service.zig").Service.init(alloc, &wal_store, &builder);

    const batch = [_]api_types.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "{\"text\":\"alpha bravo charlie\",\"chunk_preview\":[\"alpha bravo\",\"charlie\"],\"_enrichment\":{\"chunk_preview\":true,\"chunk_preview_version\":1}}" },
    };
    var ingest = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 100, .mutations = &batch });
    defer ingest.deinit(alloc);
    var build = try builder.publishNamespace("docs");
    defer build.deinit(alloc);

    var enricher = SparseEnricher.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    defer enricher.deinit();
    try enricher.setChunkEmbedder(FailingDenseEmbedder.interface(), "serverless_chunk", 6);

    try std.testing.expectError(
        EnrichmentError.ChunkEmbeddingModelFailed,
        enricher.runNamespaceWithConfig("docs", .{
            .batch_size = 4,
            .pipeline_version = chunk_embeddings_enrichment_version,
            .stage = .chunk_embeddings,
            .model_preference = .require_model,
            .failure_policy = .fail_stage,
        }),
    );
}

test "sparse enricher advances progress in batches" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-batch");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-batch");
    const wal_root = tmpPath(&wal_root_buf, "wal-batch");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);

    var fs_artifacts = try artifacts_mod.FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try catalog_mod.FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try wal_mod.FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var builder = @import("../build/builder.zig").Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var api = @import("../api/service.zig").Service.init(alloc, &wal_store, &builder);

    const batch = [_]api_types.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "{\"text\":\"alpha\"}" },
        .{ .kind = .upsert, .doc_id = "doc-b", .body = "{\"text\":\"bravo\"}" },
        .{ .kind = .upsert, .doc_id = "doc-c", .body = "{\"text\":\"charlie\"}" },
    };
    var ingest = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 100, .mutations = &batch });
    defer ingest.deinit(alloc);
    var build = try builder.publishNamespace("docs");
    defer build.deinit(alloc);

    var enricher = SparseEnricher.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    const first = try enricher.runNamespaceWithConfig("docs", .{
        .batch_size = 2,
        .pipeline_version = lexical_sparse_enrichment_version,
    });
    try std.testing.expectEqual(@as(usize, 2), first.enriched_documents);
    try std.testing.expectEqual(@as(?u64, 2), try progress_store.getEnrichmentDocOffset("docs"));

    const second = try enricher.runNamespaceWithConfig("docs", .{
        .batch_size = 2,
        .pipeline_version = lexical_sparse_enrichment_version,
    });
    try std.testing.expectEqual(@as(usize, 0), second.enriched_documents);
    try std.testing.expectEqual(@as(usize, 1), second.idle_namespaces);
}

var test_nonce: std.atomic.Value(u64) = .init(0);

fn threadedIo() std.Io.Threaded {
    return std.Io.Threaded.init(std.heap.page_allocator, .{});
}

fn nowNs() u64 {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    const now = std.Io.Timestamp.now(io_impl.io(), .awake);
    return @intCast(now.toNanoseconds());
}

fn tmpPath(buf: []u8, label: []const u8) [*:0]const u8 {
    const nonce = test_nonce.fetchAdd(1, .monotonic);
    const slice = std.fmt.bufPrint(buf, "/tmp/antfly-serverless-enrichment-{s}-{d}-{d}\x00", .{ label, nowNs(), nonce }) catch unreachable;
    return @ptrCast(slice.ptr);
}

fn cleanupTmp(path: [*:0]const u8) void {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), std.mem.span(path)) catch {};
}
