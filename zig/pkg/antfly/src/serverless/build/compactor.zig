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
const artifacts_mod = @import("../artifacts/mod.zig");
const catalog_types = @import("../catalog/types.zig");
const search_sources = @import("../search_sources.zig");
const document_segment_mod = @import("../document_segment/mod.zig");
const manifest_mod = @import("../manifest/mod.zig");
const query_mod = @import("../query/mod.zig");
const segment_mod = @import("../segment/mod.zig");
const vector_segment_mod = @import("../vector_segment/mod.zig");
const api_codec = @import("../api/codec.zig");
const vector_index = @import("vector_index.zig");
const publication_plan = @import("publication_plan.zig");
const catalog_mod = @import("../catalog/mod.zig");
const shared_vector = @import("antfly_vector").vector;
const full_text_indexes = @import("../../api/full_text_indexes.zig");
const builder_mod = @import("builder.zig");

pub const CompactionResult = struct {
    namespace: []u8,
    published: bool,
    version: u64,
    artifact_count: usize,

    pub fn deinit(self: *CompactionResult, alloc: Allocator) void {
        alloc.free(self.namespace);
        self.* = undefined;
    }
};

pub const Compactor = struct {
    alloc: Allocator,
    artifacts: *artifacts_mod.ArtifactStore,
    manifests: *manifest_mod.ManifestStore,
    progress: *catalog_mod.ProgressStore,

    pub fn init(
        alloc: Allocator,
        artifacts: *artifacts_mod.ArtifactStore,
        manifests: *manifest_mod.ManifestStore,
        progress: *catalog_mod.ProgressStore,
    ) Compactor {
        return .{
            .alloc = alloc,
            .artifacts = artifacts,
            .manifests = manifests,
            .progress = progress,
        };
    }

    pub fn compactHead(self: *Compactor, namespace: []const u8) !CompactionResult {
        const current_head = try self.progress.getHead(namespace);
        var current = try self.manifests.getAlloc(namespace, current_head);
        defer current.deinit(self.alloc);
        const document_index = builder_mod.findArtifactIndex(current, .document_segment) orelse return error.DocumentSegmentNotFound;
        const mutation_index = builder_mod.findArtifactIndex(current, .mutation_segment);

        const document_payload = try self.artifacts.getAlloc(current.artifacts[document_index].artifact_id);
        defer self.alloc.free(document_payload);
        const base_entries = try document_segment_mod.decodeAlloc(self.alloc, document_payload);
        defer document_segment_mod.freeEntries(self.alloc, base_entries);

        const before_docs = try allocMaterializedDocuments(self.alloc, base_entries);
        defer query_mod.freeMaterializedDocuments(self.alloc, before_docs);

        var overlay_mutations: []query_mod.QueryMaterializerMutation = &.{};
        if (mutation_index) |idx| {
            const mutation_payload = try self.artifacts.getAlloc(current.artifacts[idx].artifact_id);
            defer self.alloc.free(mutation_payload);
            const mutation_entries = try segment_mod.decodeAlloc(self.alloc, mutation_payload);
            defer segment_mod.freeEntries(self.alloc, mutation_entries);
            overlay_mutations = try allocMaterializerMutations(self.alloc, mutation_entries);
        }
        defer if (overlay_mutations.len > 0) freeMaterializerMutations(self.alloc, overlay_mutations);

        const latest_docs = if (overlay_mutations.len > 0)
            try query_mod.materializeDocumentsOverBaseAlloc(self.alloc, before_docs, overlay_mutations)
        else
            try allocMaterializedDocuments(self.alloc, base_entries);
        defer query_mod.freeMaterializedDocuments(self.alloc, latest_docs);

        const document_entries = try allocDocumentEntries(self.alloc, latest_docs);
        defer document_segment_mod.freeEntries(self.alloc, document_entries);
        std.mem.sort(document_segment_mod.Entry, document_entries, {}, lessDocumentEntry);

        const compacted_documents = try document_segment_mod.encodeAlloc(self.alloc, document_entries);
        defer self.alloc.free(compacted_documents);
        var document_artifact = try self.artifacts.put(compacted_documents);
        defer document_artifact.deinit(self.alloc);

        const text_index_specs = try builder_mod.resolvePublishedTextIndexSpecsAlloc(self.alloc, .{
            .schema_json = current.stats.schema_json,
            .read_schema_json = current.stats.read_schema_json,
            .indexes_json = current.stats.indexes_json,
        }, &.{});
        defer full_text_indexes.freeFullTextIndexSpecs(self.alloc, text_index_specs);
        const text_refs = try builder_mod.buildTextArtifactRefsForMaterializedDocsAlloc(
            self.alloc,
            self.artifacts,
            current,
            before_docs,
            latest_docs,
            overlay_mutations,
            text_index_specs,
        );
        defer builder_mod.freeArtifactRefs(self.alloc, text_refs);
        const sparse_refs = try builder_mod.buildSparseArtifactRefsForMaterializedDocsAlloc(
            self.alloc,
            self.artifacts,
            current,
            before_docs,
            latest_docs,
            overlay_mutations,
            current.stats.published_search_sources,
        );
        defer builder_mod.freeArtifactRefs(self.alloc, sparse_refs);
        const named_vector_policies = try currentNamedVectorPoliciesAlloc(
            self.alloc,
            self.artifacts,
            current,
            current.stats.published_search_sources,
            current.stats.policy,
            document_entries.len,
        );
        defer self.alloc.free(named_vector_policies);
        const vector_refs = try builder_mod.buildVectorArtifactRefsForMaterializedDocsAlloc(
            self.alloc,
            self.artifacts,
            current,
            current.stats.policy.vector_distance_metric,
            before_docs,
            latest_docs,
            overlay_mutations,
            null,
            named_vector_policies,
            current.stats.published_search_sources,
        );
        defer builder_mod.freeArtifactRefs(self.alloc, vector_refs);
        const graph_index_names = try builder_mod.listGraphIndexNamesAlloc(self.alloc, current.stats.indexes_json);
        defer builder_mod.freeOwnedStrings(self.alloc, graph_index_names);
        const graph_refs = try builder_mod.buildGraphArtifactRefsForMaterializedDocsAlloc(
            self.alloc,
            self.artifacts,
            current,
            before_docs,
            latest_docs,
            overlay_mutations,
            graph_index_names,
            true,
        );
        defer builder_mod.freeArtifactRefs(self.alloc, graph_refs);
        var derived_outputs = try builder_mod.detectMaterializedDerivedOutputsAlloc(
            self.alloc,
            latest_docs,
            current.stats.indexes_json,
            .{},
        );
        defer search_sources.deinitMaterializedDerivedOutputs(self.alloc, &derived_outputs);

        if (mutation_index == null and artifactsMatchCompactedHead(current, document_artifact, text_refs, sparse_refs, vector_refs, graph_refs, derived_outputs)) {
            return .{
                .namespace = try self.alloc.dupe(u8, namespace),
                .published = false,
                .version = current_head,
                .artifact_count = current.artifacts.len,
            };
        }

        const next_version = current_head + 1;
        var manifest = try buildCompactedManifestAlloc(
            self.alloc,
            namespace,
            next_version,
            current.wal_end_lsn,
            document_entries.len,
            document_artifact,
            text_refs,
            sparse_refs,
            vector_refs,
            graph_refs,
            current.stats.policy,
            derived_outputs,
            .{
                .schema_json = current.stats.schema_json,
                .read_schema_json = current.stats.read_schema_json,
                .indexes_json = current.stats.indexes_json,
            },
        );
        defer manifest.deinit(self.alloc);

        try self.manifests.put(manifest);
        const published = try self.progress.compareAndSwapHead(namespace, current_head, next_version);
        if (!published) return error.HeadChanged;

        return .{
            .namespace = try self.alloc.dupe(u8, namespace),
            .published = true,
            .version = next_version,
            .artifact_count = manifest.artifacts.len,
        };
    }
};

fn buildCompactedManifestAlloc(
    alloc: Allocator,
    namespace: []const u8,
    version: u64,
    wal_end_lsn: u64,
    document_count: usize,
    document_artifact: artifacts_mod.ArtifactMetadata,
    text_refs: []const manifest_mod.ArtifactRef,
    sparse_refs: []const manifest_mod.ArtifactRef,
    vector_refs: []const manifest_mod.ArtifactRef,
    graph_refs: []const manifest_mod.ArtifactRef,
    policy: catalog_types.NamespacePolicy,
    derived_outputs: search_sources.MaterializedDerivedOutputs,
    table_definition: publication_plan.TableDefinitionSnapshot,
) !manifest_mod.Manifest {
    const text_count: usize = text_refs.len;
    const sparse_count: usize = sparse_refs.len;
    const vector_count: usize = vector_refs.len;
    const graph_count: usize = graph_refs.len;
    const artifacts = try alloc.alloc(manifest_mod.ArtifactRef, 1 + text_count + sparse_count + vector_count + graph_count);
    errdefer alloc.free(artifacts);
    artifacts[0] = .{
        .kind = .document_segment,
        .artifact_id = try alloc.dupe(u8, document_artifact.artifact_id),
        .byte_len = document_artifact.byte_len,
        .checksum = try alloc.dupe(u8, document_artifact.checksum),
    };
    var artifact_index: usize = 1;
    for (text_refs) |text_ref| {
        artifacts[artifact_index] = .{
            .kind = .text_segment,
            .name = try alloc.dupe(u8, text_ref.name),
            .artifact_id = try alloc.dupe(u8, text_ref.artifact_id),
            .byte_len = text_ref.byte_len,
            .checksum = try alloc.dupe(u8, text_ref.checksum),
        };
        artifact_index += 1;
    }
    for (sparse_refs) |sparse_ref| {
        artifacts[artifact_index] = try builder_mod.cloneArtifactRefAlloc(alloc, sparse_ref);
        artifact_index += 1;
    }
    for (vector_refs) |vector_ref| {
        artifacts[artifact_index] = try builder_mod.cloneArtifactRefAlloc(alloc, vector_ref);
        artifact_index += 1;
    }
    for (graph_refs) |graph_ref| {
        artifacts[artifact_index] = try builder_mod.cloneArtifactRefAlloc(alloc, graph_ref);
        artifact_index += 1;
    }
    const text_index_specs = try builder_mod.resolvePublishedTextIndexSpecsAlloc(alloc, table_definition, &.{});
    defer full_text_indexes.freeFullTextIndexSpecs(alloc, text_index_specs);
    var manifest_sources = try builder_mod.buildPublishedSearchSourcesForManifestAlloc(
        alloc,
        text_index_specs,
        vector_refs,
        sparse_refs,
        table_definition,
    );
    errdefer search_sources.deinitPublishedSearchSources(alloc, &manifest_sources);

    return .{
        .namespace = try alloc.dupe(u8, namespace),
        .version = version,
        .built_at_ns = walEndAsBuiltAtNs(wal_end_lsn),
        .wal_start_lsn = wal_end_lsn,
        .wal_end_lsn = wal_end_lsn,
        .stats = .{
            .document_count = @intCast(document_count),
            .text_segment_count = @intCast(text_count),
            .sparse_segment_count = @intCast(sparse_count),
            .vector_segment_count = @intCast(vector_count),
            .graph_segment_count = @intCast(graph_count),
            .published_search_sources = manifest_sources,
            .derived_outputs = try search_sources.cloneMaterializedDerivedOutputsAlloc(alloc, derived_outputs),
            .policy = policy,
            .schema_json = if (table_definition.schema_json.len == 0) &.{} else try alloc.dupe(u8, table_definition.schema_json),
            .read_schema_json = if (table_definition.read_schema_json.len == 0) &.{} else try alloc.dupe(u8, table_definition.read_schema_json),
            .indexes_json = if (table_definition.indexes_json.len == 0) &.{} else try alloc.dupe(u8, table_definition.indexes_json),
        },
        .artifacts = artifacts,
    };
}

fn loadPublishedDocumentEntriesAlloc(
    alloc: Allocator,
    artifacts: *artifacts_mod.ArtifactStore,
    manifest: manifest_mod.Manifest,
) ![]document_segment_mod.Entry {
    const document_index = builder_mod.findArtifactIndex(manifest, .document_segment) orelse return error.DocumentSegmentNotFound;
    const document_payload = try artifacts.getAlloc(manifest.artifacts[document_index].artifact_id);
    defer alloc.free(document_payload);
    const base_entries = try document_segment_mod.decodeAlloc(alloc, document_payload);
    errdefer document_segment_mod.freeEntries(alloc, base_entries);

    const mutation_index = builder_mod.findArtifactIndex(manifest, .mutation_segment) orelse return base_entries;
    const mutation_payload = try artifacts.getAlloc(manifest.artifacts[mutation_index].artifact_id);
    defer alloc.free(mutation_payload);
    const mutation_entries = try segment_mod.decodeAlloc(alloc, mutation_payload);
    defer segment_mod.freeEntries(alloc, mutation_entries);
    const base_docs = try allocMaterializedDocuments(alloc, base_entries);
    defer query_mod.freeMaterializedDocuments(alloc, base_docs);
    const overlay = try allocMaterializerMutations(alloc, mutation_entries);
    defer freeMaterializerMutations(alloc, overlay);
    const materialized = try query_mod.materializeDocumentsOverBaseAlloc(alloc, base_docs, overlay);
    defer query_mod.freeMaterializedDocuments(alloc, materialized);
    document_segment_mod.freeEntries(alloc, base_entries);
    return try allocDocumentEntries(alloc, materialized);
}

fn allocMaterializedDocuments(
    alloc: Allocator,
    entries: []const document_segment_mod.Entry,
) ![]query_mod.QueryMaterializedDocument {
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

fn allocMaterializerMutations(
    alloc: Allocator,
    entries: []const segment_mod.Entry,
) ![]query_mod.QueryMaterializerMutation {
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

fn allocDocumentEntries(
    alloc: Allocator,
    docs: []const query_mod.QueryMaterializedDocument,
) ![]document_segment_mod.Entry {
    const entries = try alloc.alloc(document_segment_mod.Entry, docs.len);
    errdefer alloc.free(entries);

    var initialized: usize = 0;
    errdefer {
        for (entries[0..initialized]) |*entry| entry.deinit(alloc);
    }

    for (docs, 0..) |doc, idx| {
        entries[idx] = .{
            .doc_id = try alloc.dupe(u8, doc.doc_id),
            .body = try alloc.dupe(u8, doc.body),
            .last_lsn = doc.last_lsn,
            .last_timestamp_ns = doc.last_timestamp_ns,
        };
        initialized += 1;
    }
    return entries;
}

fn freeMaterializerMutations(alloc: Allocator, mutations: []query_mod.QueryMaterializerMutation) void {
    for (mutations) |entry| {
        alloc.free(entry.doc_id);
        if (entry.body) |body| alloc.free(body);
    }
    alloc.free(mutations);
}

fn artifactsMatchCompactedHead(
    current: manifest_mod.Manifest,
    document_artifact: artifacts_mod.ArtifactMetadata,
    text_refs: []const manifest_mod.ArtifactRef,
    sparse_refs: []const manifest_mod.ArtifactRef,
    vector_refs: []const manifest_mod.ArtifactRef,
    graph_refs: []const manifest_mod.ArtifactRef,
    derived_outputs: search_sources.MaterializedDerivedOutputs,
) bool {
    const expected_count: usize = 1 + text_refs.len +
        sparse_refs.len +
        vector_refs.len +
        graph_refs.len;
    if (current.artifacts.len != expected_count) return false;
    if (!artifactMatches(current.artifacts[0], .document_segment, document_artifact.artifact_id)) return false;
    for (text_refs) |text_ref| {
        if (!containsArtifactRef(current.artifacts[1..], .text_segment, text_ref.name, text_ref.artifact_id)) return false;
    }
    for (sparse_refs) |sparse_ref| {
        if (!containsArtifactRef(current.artifacts[1..], .sparse_segment, sparse_ref.name, sparse_ref.artifact_id)) return false;
    }
    for (vector_refs) |vector_ref| {
        if (!containsArtifactRef(current.artifacts[1..], .vector_segment, vector_ref.name, vector_ref.artifact_id)) return false;
    }
    for (graph_refs) |graph_ref| {
        if (!containsArtifactRef(current.artifacts[1..], .graph_segment, graph_ref.name, graph_ref.artifact_id)) return false;
    }
    return derivedOutputsMatch(current.stats.derived_outputs, derived_outputs);
}

fn artifactMatches(current: manifest_mod.ArtifactRef, kind: manifest_mod.ArtifactKind, artifact_id: []const u8) bool {
    return current.kind == kind and std.mem.eql(u8, current.artifact_id, artifact_id);
}

fn artifactMatchesNamed(current: manifest_mod.ArtifactRef, kind: manifest_mod.ArtifactKind, name: []const u8, artifact_id: []const u8) bool {
    return current.kind == kind and
        std.mem.eql(u8, current.name, name) and
        std.mem.eql(u8, current.artifact_id, artifact_id);
}

fn containsArtifactRef(
    artifacts: []const manifest_mod.ArtifactRef,
    kind: manifest_mod.ArtifactKind,
    name: []const u8,
    artifact_id: []const u8,
) bool {
    for (artifacts) |artifact| {
        if (artifact.name.len == 0 and name.len == 0) {
            if (artifactMatches(artifact, kind, artifact_id)) return true;
            continue;
        }
        if (artifactMatchesNamed(artifact, kind, name, artifact_id)) return true;
    }
    return false;
}

fn derivedOutputsMatch(
    current: search_sources.MaterializedDerivedOutputs,
    expected: search_sources.MaterializedDerivedOutputs,
) bool {
    const current_items = current.items orelse &.{};
    const expected_items = expected.items orelse &.{};
    if (current_items.len != expected_items.len) return false;
    for (expected_items) |expected_item| {
        var matched = false;
        for (current_items) |current_item| {
            if (current_item.kind != expected_item.kind) continue;
            if (!std.mem.eql(u8, current_item.name, expected_item.name)) continue;
            matched = true;
            break;
        }
        if (!matched) return false;
    }
    return true;
}

fn walEndAsBuiltAtNs(wal_end_lsn: u64) u64 {
    return wal_end_lsn;
}

fn lessDocumentEntry(_: void, lhs: document_segment_mod.Entry, rhs: document_segment_mod.Entry) bool {
    const order = std.mem.order(u8, lhs.doc_id, rhs.doc_id);
    if (order != .eq) return order == .lt;
    if (lhs.last_lsn != rhs.last_lsn) return lhs.last_lsn < rhs.last_lsn;
    return lhs.last_timestamp_ns < rhs.last_timestamp_ns;
}

const NamedCurrentVectorPolicy = struct {
    index_name: []const u8,
    policy: ?vector_index.BuildPolicy = null,
};

fn currentNamedVectorPoliciesAlloc(
    alloc: Allocator,
    artifacts: *artifacts_mod.ArtifactStore,
    manifest: manifest_mod.Manifest,
    published_search_sources: search_sources.PublishedSearchSources,
    policy: catalog_types.NamespacePolicy,
    document_count: usize,
) ![]builder_mod.NamedVectorBuildPolicy {
    const vector_sources = try search_sources.listVectorSourcesAlloc(alloc, published_search_sources);
    defer search_sources.freeVectorSourceDescriptors(alloc, vector_sources);

    const current_count = builder_mod.countArtifactsByKind(manifest, .vector_segment);
    var items = std.ArrayListUnmanaged(builder_mod.NamedVectorBuildPolicy).empty;
    errdefer items.deinit(alloc);

    for (vector_sources) |source| {
        const artifact = if (builder_mod.findNamedArtifactIndex(manifest, .vector_segment, source.index_name)) |artifact_index|
            manifest.artifacts[artifact_index]
        else if (current_count == 1)
            manifest.artifacts[builder_mod.findArtifactIndex(manifest, .vector_segment).?]
        else
            continue;
        const info = try builder_mod.readVectorArtifactInfoAlloc(alloc, artifacts, artifact);
        try items.append(alloc, .{
            .index_name = source.index_name,
            .policy = builder_mod.adaptiveVectorBuildPolicyForPolicy(info, document_count, policy),
        });
    }
    return try items.toOwnedSlice(alloc);
}

test "compactor rewrites head into compacted searchable artifacts" {
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

    var fs_wal = try @import("../wal/mod.zig").FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_progress = try catalog_mod.FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var builder = @import("builder.zig").Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var api = @import("../api/service.zig").Service.init(alloc, &wal_store, &builder);
    const batch = [_]@import("../api/types.zig").DocumentMutation{
        .{
            .kind = .upsert,
            .doc_id = "doc-b",
            .body = "{\"text\":\"alpha bravo\",\"embedding\":[2,0],\"sparse_embedding\":{\"alpha\":1.0,\"bravo\":0.5}}",
        },
        .{
            .kind = .upsert,
            .doc_id = "doc-a",
            .body = "{\"text\":\"alpha charlie\",\"embedding\":[0.75,0.75],\"sparse_embedding\":{\"alpha\":0.5,\"charlie\":1.25}}",
        },
    };
    var ingest = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 100, .mutations = &batch });
    defer ingest.deinit(alloc);
    var build = try builder.publishNamespaceWithMetric("docs", .inner_product);
    defer build.deinit(alloc);

    var compactor = Compactor.init(alloc, &artifact_store, &manifest_store, &progress_store);
    var result = try compactor.compactHead("docs");
    defer result.deinit(alloc);
    try std.testing.expect(result.published);
    try std.testing.expectEqual(@as(u64, 2), result.version);
    try std.testing.expectEqual(@as(usize, 4), result.artifact_count);

    var manifest = try manifest_store.getAlloc("docs", 2);
    defer manifest.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 4), manifest.artifacts.len);
    try std.testing.expectEqual(manifest_mod.ArtifactKind.document_segment, manifest.artifacts[0].kind);
    try std.testing.expectEqual(manifest_mod.ArtifactKind.text_segment, manifest.artifacts[1].kind);
    try std.testing.expectEqual(manifest_mod.ArtifactKind.sparse_segment, manifest.artifacts[2].kind);
    try std.testing.expectEqual(manifest_mod.ArtifactKind.vector_segment, manifest.artifacts[3].kind);

    const document_payload = try artifact_store.getAlloc(manifest.artifacts[0].artifact_id);
    defer alloc.free(document_payload);
    const compacted_docs = try document_segment_mod.decodeAlloc(alloc, document_payload);
    defer document_segment_mod.freeEntries(alloc, compacted_docs);
    try std.testing.expectEqual(@as(usize, 2), compacted_docs.len);
    try std.testing.expectEqualStrings("doc-a", compacted_docs[0].doc_id);
    try std.testing.expectEqualStrings("doc-b", compacted_docs[1].doc_id);

    const vector_payload = try artifact_store.getAlloc(manifest.artifacts[3].artifact_id);
    defer alloc.free(vector_payload);
    const vector_header = try vector_segment_mod.decodeHeader(vector_payload[0..vector_segment_mod.header_len]);
    try std.testing.expectEqual(shared_vector.DistanceMetric.inner_product, vector_header.metric);
}

test "artifacts match compacted head ignores named artifact and derived output ordering" {
    const alloc = std.testing.allocator;

    var current = manifest_mod.Manifest{
        .namespace = try alloc.dupe(u8, "docs"),
        .version = 1,
        .built_at_ns = 1,
        .wal_start_lsn = 1,
        .wal_end_lsn = 1,
        .stats = .{
            .derived_outputs = .{
                .items = try alloc.alloc(search_sources.DerivedOutputDescriptor, 2),
            },
        },
        .artifacts = try alloc.alloc(manifest_mod.ArtifactRef, 5),
    };
    defer current.deinit(alloc);

    current.stats.derived_outputs.items.?[0] = .{
        .name = try alloc.dupe(u8, search_sources.default_chunk_embeddings_output_name),
        .kind = .chunk_embeddings,
    };
    current.stats.derived_outputs.items.?[1] = .{
        .name = try alloc.dupe(u8, search_sources.default_chunk_preview_output_name),
        .kind = .chunk_preview,
    };

    current.artifacts[0] = .{
        .kind = .document_segment,
        .artifact_id = try alloc.dupe(u8, "doc"),
        .byte_len = 1,
        .checksum = try alloc.dupe(u8, "doc-sum"),
    };
    current.artifacts[1] = .{
        .kind = .vector_segment,
        .name = try alloc.dupe(u8, "semantic"),
        .artifact_id = try alloc.dupe(u8, "vec"),
        .byte_len = 1,
        .checksum = try alloc.dupe(u8, "vec-sum"),
    };
    current.artifacts[2] = .{
        .kind = .text_segment,
        .name = try alloc.dupe(u8, "full_text_index_v0"),
        .artifact_id = try alloc.dupe(u8, "text"),
        .byte_len = 1,
        .checksum = try alloc.dupe(u8, "text-sum"),
    };
    current.artifacts[3] = .{
        .kind = .graph_segment,
        .name = try alloc.dupe(u8, "graph"),
        .artifact_id = try alloc.dupe(u8, "graph-id"),
        .byte_len = 1,
        .checksum = try alloc.dupe(u8, "graph-sum"),
    };
    current.artifacts[4] = .{
        .kind = .sparse_segment,
        .name = try alloc.dupe(u8, "sparse"),
        .artifact_id = try alloc.dupe(u8, "sparse-id"),
        .byte_len = 1,
        .checksum = try alloc.dupe(u8, "sparse-sum"),
    };

    const document_artifact = artifacts_mod.ArtifactMetadata{
        .artifact_id = try alloc.dupe(u8, "doc"),
        .byte_len = 1,
        .checksum = try alloc.dupe(u8, "doc-sum"),
    };
    defer {
        var owned = document_artifact;
        owned.deinit(alloc);
    }

    const text_refs = try alloc.alloc(manifest_mod.ArtifactRef, 1);
    defer {
        for (text_refs) |ref| {
            if (ref.name.len > 0) alloc.free(ref.name);
            alloc.free(ref.artifact_id);
            alloc.free(ref.checksum);
        }
        alloc.free(text_refs);
    }
    text_refs[0] = .{
        .kind = .text_segment,
        .name = try alloc.dupe(u8, "full_text_index_v0"),
        .artifact_id = try alloc.dupe(u8, "text"),
        .byte_len = 1,
        .checksum = try alloc.dupe(u8, "text-sum"),
    };

    const sparse_refs = try alloc.alloc(manifest_mod.ArtifactRef, 1);
    defer {
        for (sparse_refs) |ref| {
            if (ref.name.len > 0) alloc.free(ref.name);
            alloc.free(ref.artifact_id);
            alloc.free(ref.checksum);
        }
        alloc.free(sparse_refs);
    }
    sparse_refs[0] = .{
        .kind = .sparse_segment,
        .name = try alloc.dupe(u8, "sparse"),
        .artifact_id = try alloc.dupe(u8, "sparse-id"),
        .byte_len = 1,
        .checksum = try alloc.dupe(u8, "sparse-sum"),
    };

    const vector_refs = try alloc.alloc(manifest_mod.ArtifactRef, 1);
    defer {
        for (vector_refs) |ref| {
            if (ref.name.len > 0) alloc.free(ref.name);
            alloc.free(ref.artifact_id);
            alloc.free(ref.checksum);
        }
        alloc.free(vector_refs);
    }
    vector_refs[0] = .{
        .kind = .vector_segment,
        .name = try alloc.dupe(u8, "semantic"),
        .artifact_id = try alloc.dupe(u8, "vec"),
        .byte_len = 1,
        .checksum = try alloc.dupe(u8, "vec-sum"),
    };

    const graph_refs = try alloc.alloc(manifest_mod.ArtifactRef, 1);
    defer {
        for (graph_refs) |ref| {
            if (ref.name.len > 0) alloc.free(ref.name);
            alloc.free(ref.artifact_id);
            alloc.free(ref.checksum);
        }
        alloc.free(graph_refs);
    }
    graph_refs[0] = .{
        .kind = .graph_segment,
        .name = try alloc.dupe(u8, "graph"),
        .artifact_id = try alloc.dupe(u8, "graph-id"),
        .byte_len = 1,
        .checksum = try alloc.dupe(u8, "graph-sum"),
    };

    var derived_outputs = search_sources.MaterializedDerivedOutputs{
        .items = try alloc.alloc(search_sources.DerivedOutputDescriptor, 2),
    };
    defer search_sources.deinitMaterializedDerivedOutputs(alloc, &derived_outputs);
    derived_outputs.items.?[0] = .{
        .name = try alloc.dupe(u8, search_sources.default_chunk_preview_output_name),
        .kind = .chunk_preview,
    };
    derived_outputs.items.?[1] = .{
        .name = try alloc.dupe(u8, search_sources.default_chunk_embeddings_output_name),
        .kind = .chunk_embeddings,
    };

    try std.testing.expect(artifactsMatchCompactedHead(
        current,
        document_artifact,
        text_refs,
        sparse_refs,
        vector_refs,
        graph_refs,
        derived_outputs,
    ));
}

test "compactor preserves chunk embedding vector segments when top-level embedding is absent" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-compactor-vector-chunks");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-compactor-vector-chunks");
    const wal_root = tmpPath(&wal_root_buf, "wal-compactor-vector-chunks");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);

    var fs_artifacts = try artifacts_mod.FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_wal = try @import("../wal/mod.zig").FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_progress = try catalog_mod.FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var builder = @import("builder.zig").Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    const encoded = try api_codec.encodeMutationAlloc(alloc, .{
        .kind = .upsert,
        .doc_id = "doc-v",
        .body = "{\"text\":\"alpha\",\"chunk_embeddings\":[{\"chunk\":\"a\",\"embedding\":[1,0,0]},{\"chunk\":\"b\",\"embedding\":[0.9,0.1,0]}],\"_enrichment\":{\"chunk_embeddings\":true,\"chunk_embeddings_version\":1}}",
    });
    defer alloc.free(encoded);
    _ = try wal_store.append("docs", 100, encoded);

    var publish = try builder.publishNamespace("docs");
    defer publish.deinit(alloc);

    var compactor = Compactor.init(alloc, &artifact_store, &manifest_store, &progress_store);
    var result = try compactor.compactHead("docs");
    defer result.deinit(alloc);
    try std.testing.expect(result.published);

    var manifest = try manifest_store.getAlloc("docs", result.version);
    defer manifest.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 1), manifest.stats.vector_segment_count);
    try std.testing.expectEqualStrings(search_sources.default_chunk_embedding_index_name, manifest.stats.published_search_sources.findVector().?.index_name);
    try std.testing.expect(manifest.stats.derived_outputs.containsKind(.chunk_embeddings));
    try std.testing.expectEqualStrings(search_sources.default_chunk_embeddings_output_name, manifest.stats.derived_outputs.findByKind(.chunk_embeddings).?.name);
    const vector_ref = manifest.artifacts[2];
    try std.testing.expectEqual(manifest_mod.ArtifactKind.vector_segment, vector_ref.kind);

    const payload = try artifact_store.getAlloc(vector_ref.artifact_id);
    defer alloc.free(payload);
    var decoded = try vector_segment_mod.decodeAlloc(alloc, payload);
    defer vector_segment_mod.freeSegment(alloc, &decoded);
    try std.testing.expectEqual(@as(usize, 2), decoded.entries.len);
    try std.testing.expectEqualStrings("doc-v", decoded.entries[0].doc_id);
    try std.testing.expectEqualStrings("doc-v", decoded.entries[1].doc_id);
}

test "compactor preserves named graph segments for graph indexes" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-compactor-graph-named");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-compactor-graph-named");
    const wal_root = tmpPath(&wal_root_buf, "wal-compactor-graph-named");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);

    var fs_artifacts = try artifacts_mod.FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_wal = try @import("../wal/mod.zig").FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_progress = try catalog_mod.FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    const encoded = try api_codec.encodeMutationAlloc(alloc, .{
        .kind = .upsert,
        .doc_id = "doc-g",
        .body = "{\"text\":\"alpha\",\"graph_edges\":[{\"target\":\"doc-h\",\"edge_type\":\"cites\",\"weight\":2.0}]}",
    });
    defer alloc.free(encoded);
    _ = try wal_store.append("docs", 100, encoded);

    const indexes_json = try alloc.dupe(u8, "{\"graph_a\":{\"type\":\"graph\"},\"graph_b\":{\"type\":\"graph\"}}");
    defer alloc.free(indexes_json);

    var builder = @import("builder.zig").Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var publish = try builder.publishNamespaceWithMetricAndPlan("docs", .cosine, .{
        .targets = .{
            .published_search_sources = search_sources.defaultPublishedSearchSources(),
            .include_graph = true,
        },
        .table_definition = .{
            .indexes_json = indexes_json,
        },
    });
    defer publish.deinit(alloc);
    try std.testing.expect(publish.published);

    var compactor = Compactor.init(alloc, &artifact_store, &manifest_store, &progress_store);
    var result = try compactor.compactHead("docs");
    defer result.deinit(alloc);
    try std.testing.expect(result.published);

    var manifest = try manifest_store.getAlloc("docs", result.version);
    defer manifest.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 2), manifest.stats.graph_segment_count);
    const graph_a_index = builder_mod.findNamedArtifactIndex(manifest, .graph_segment, "graph_a").?;
    const graph_b_index = builder_mod.findNamedArtifactIndex(manifest, .graph_segment, "graph_b").?;
    try std.testing.expectEqualStrings(
        manifest.artifacts[graph_a_index].artifact_id,
        manifest.artifacts[graph_b_index].artifact_id,
    );
}

test "compactor materializes reused document heads before rewriting" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-compactor-materialized-head");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-compactor-materialized-head");
    const wal_root = tmpPath(&wal_root_buf, "wal-compactor-materialized-head");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);

    var fs_artifacts = try artifacts_mod.FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_wal = try @import("../wal/mod.zig").FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_progress = try catalog_mod.FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var builder = @import("builder.zig").Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var api = @import("../api/service.zig").Service.init(alloc, &wal_store, &builder);

    const first = [_]@import("../api/types.zig").DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "alpha" },
        .{ .kind = .upsert, .doc_id = "doc-b", .body = "beta" },
    };
    var ingest_first = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 100, .mutations = &first });
    defer ingest_first.deinit(alloc);
    var publish_first = try builder.publishNamespace("docs");
    defer publish_first.deinit(alloc);

    const second = [_]@import("../api/types.zig").DocumentMutation{
        .{ .kind = .delete, .doc_id = "doc-a", .body = null },
        .{ .kind = .upsert, .doc_id = "doc-c", .body = "gamma" },
    };
    var ingest_second = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 200, .mutations = &second });
    defer ingest_second.deinit(alloc);
    var publish_second = try builder.publishNamespace("docs");
    defer publish_second.deinit(alloc);

    var reused_head = try manifest_store.getAlloc("docs", 2);
    defer reused_head.deinit(alloc);
    try std.testing.expect(builder_mod.findArtifactIndex(reused_head, .mutation_segment) != null);

    var compactor = Compactor.init(alloc, &artifact_store, &manifest_store, &progress_store);
    var result = try compactor.compactHead("docs");
    defer result.deinit(alloc);
    try std.testing.expect(result.published);

    var manifest = try manifest_store.getAlloc("docs", result.version);
    defer manifest.deinit(alloc);
    try std.testing.expect(builder_mod.findArtifactIndex(manifest, .mutation_segment) == null);

    const document_payload = try artifact_store.getAlloc(manifest.artifacts[0].artifact_id);
    defer alloc.free(document_payload);
    const compacted_docs = try document_segment_mod.decodeAlloc(alloc, document_payload);
    defer document_segment_mod.freeEntries(alloc, compacted_docs);
    try std.testing.expectEqual(@as(usize, 2), compacted_docs.len);
    try std.testing.expectEqualStrings("doc-b", compacted_docs[0].doc_id);
    try std.testing.expectEqualStrings("beta", compacted_docs[0].body);
    try std.testing.expectEqualStrings("doc-c", compacted_docs[1].doc_id);
    try std.testing.expectEqualStrings("gamma", compacted_docs[1].body);
}

test "compactor reuses unaffected sparse vector and graph artifacts" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-compactor-reuse-unaffected");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-compactor-reuse-unaffected");
    const wal_root = tmpPath(&wal_root_buf, "wal-compactor-reuse-unaffected");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);

    var fs_artifacts = try artifacts_mod.FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_wal = try @import("../wal/mod.zig").FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_progress = try catalog_mod.FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    const indexes_json = try alloc.dupe(u8, "{\"graph_idx\":{\"type\":\"graph\"}}");
    defer alloc.free(indexes_json);

    var builder = builder_mod.Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var api = @import("../api/service.zig").Service.init(alloc, &wal_store, &builder);

    const first = [_]@import("../api/types.zig").DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "{\"text\":\"alpha\",\"embedding\":[1,0,0],\"sparse_embedding\":{\"alpha\":1.0},\"graph_edges\":[{\"target\":\"doc-b\",\"edge_type\":\"cites\",\"weight\":1.0}]}" },
    };
    var ingest_first = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 100, .mutations = &first });
    defer ingest_first.deinit(alloc);
    var publish_first = try builder.publishNamespaceWithMetricAndPlan("docs", .cosine, .{
        .targets = .{
            .published_search_sources = search_sources.defaultPublishedSearchSources(),
            .include_graph = true,
        },
        .table_definition = .{
            .indexes_json = indexes_json,
        },
    });
    defer publish_first.deinit(alloc);

    const second = [_]@import("../api/types.zig").DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "{\"text\":\"alpha updated\",\"embedding\":[1,0,0],\"sparse_embedding\":{\"alpha\":1.0},\"graph_edges\":[{\"target\":\"doc-b\",\"edge_type\":\"cites\",\"weight\":1.0}]}" },
    };
    var ingest_second = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 200, .mutations = &second });
    defer ingest_second.deinit(alloc);
    var publish_second = try builder.publishNamespaceWithMetricAndPlan("docs", .cosine, .{
        .targets = .{
            .published_search_sources = search_sources.defaultPublishedSearchSources(),
            .include_graph = true,
        },
        .table_definition = .{
            .indexes_json = indexes_json,
        },
    });
    defer publish_second.deinit(alloc);

    var reused_head = try manifest_store.getAlloc("docs", 2);
    defer reused_head.deinit(alloc);
    const vector_before = reused_head.artifacts[builder_mod.findArtifactIndex(reused_head, .vector_segment).?].artifact_id;
    const sparse_before = reused_head.artifacts[builder_mod.findArtifactIndex(reused_head, .sparse_segment).?].artifact_id;
    const graph_before = reused_head.artifacts[builder_mod.findNamedArtifactIndex(reused_head, .graph_segment, "graph_idx").?].artifact_id;

    var compactor = Compactor.init(alloc, &artifact_store, &manifest_store, &progress_store);
    var result = try compactor.compactHead("docs");
    defer result.deinit(alloc);
    try std.testing.expect(result.published);

    var manifest = try manifest_store.getAlloc("docs", result.version);
    defer manifest.deinit(alloc);
    try std.testing.expectEqualStrings(vector_before, manifest.artifacts[builder_mod.findArtifactIndex(manifest, .vector_segment).?].artifact_id);
    try std.testing.expectEqualStrings(sparse_before, manifest.artifacts[builder_mod.findArtifactIndex(manifest, .sparse_segment).?].artifact_id);
    try std.testing.expectEqualStrings(graph_before, manifest.artifacts[builder_mod.findNamedArtifactIndex(manifest, .graph_segment, "graph_idx").?].artifact_id);
}

test "compactor reuses unaffected full text artifacts" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-compactor-reuse-full-text");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-compactor-reuse-full-text");
    const wal_root = tmpPath(&wal_root_buf, "wal-compactor-reuse-full-text");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);

    var fs_artifacts = try artifacts_mod.FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_wal = try @import("../wal/mod.zig").FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_progress = try catalog_mod.FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var builder = builder_mod.Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var api = @import("../api/service.zig").Service.init(alloc, &wal_store, &builder);

    const first = [_]@import("../api/types.zig").DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "{\"text\":\"alpha\",\"embedding\":[1,0,0]}" },
    };
    var ingest_first = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 100, .mutations = &first });
    defer ingest_first.deinit(alloc);
    var publish_first = try builder.publishNamespace("docs");
    defer publish_first.deinit(alloc);

    const second = [_]@import("../api/types.zig").DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "{\"text\":\"alpha\",\"embedding\":[0,1,0]}" },
    };
    var ingest_second = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 200, .mutations = &second });
    defer ingest_second.deinit(alloc);
    var publish_second = try builder.publishNamespace("docs");
    defer publish_second.deinit(alloc);

    var reused_head = try manifest_store.getAlloc("docs", 2);
    defer reused_head.deinit(alloc);
    const text_before = reused_head.artifacts[builder_mod.findArtifactIndex(reused_head, .text_segment).?].artifact_id;

    var compactor = Compactor.init(alloc, &artifact_store, &manifest_store, &progress_store);
    var result = try compactor.compactHead("docs");
    defer result.deinit(alloc);
    try std.testing.expect(result.published);

    var manifest = try manifest_store.getAlloc("docs", result.version);
    defer manifest.deinit(alloc);
    try std.testing.expectEqualStrings(text_before, manifest.artifacts[builder_mod.findArtifactIndex(manifest, .text_segment).?].artifact_id);
}

test "compactor no-ops when head is already compacted" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-noop");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-noop");
    const wal_root = tmpPath(&wal_root_buf, "wal-noop");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);

    var fs_artifacts = try artifacts_mod.FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_wal = try @import("../wal/mod.zig").FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_progress = try catalog_mod.FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var builder = @import("builder.zig").Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var api = @import("../api/service.zig").Service.init(alloc, &wal_store, &builder);
    const batch = [_]@import("../api/types.zig").DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "alpha bravo" },
    };
    var ingest = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 100, .mutations = &batch });
    defer ingest.deinit(alloc);
    var build = try builder.publishNamespace("docs");
    defer build.deinit(alloc);

    var compactor = Compactor.init(alloc, &artifact_store, &manifest_store, &progress_store);
    var first = try compactor.compactHead("docs");
    defer first.deinit(alloc);
    try std.testing.expect(first.published);

    var second = try compactor.compactHead("docs");
    defer second.deinit(alloc);
    try std.testing.expect(!second.published);
    try std.testing.expectEqual(@as(u64, 2), second.version);
}

test "adaptive vector build policy expands poor cluster layouts" {
    const policy = builder_mod.adaptiveVectorBuildPolicy(.{
        .metric = .cosine,
        .cluster_count = 4,
        .base_probe_count = 2,
        .shortlist_multiplier = 2,
        .cluster_imbalance = 1.25,
        .distance_span_max = 1.5,
    }, 64);
    try std.testing.expectEqual(@as(?usize, 6), policy.target_cluster_count);
    try std.testing.expectEqual(@as(?u32, 3), policy.base_probe_count);
    try std.testing.expectEqual(@as(?u32, 3), policy.shortlist_multiplier);
}

test "adaptive vector build policy can shrink over-fragmented layouts" {
    const policy = builder_mod.adaptiveVectorBuildPolicy(.{
        .metric = .cosine,
        .cluster_count = 8,
        .base_probe_count = 4,
        .shortlist_multiplier = 4,
        .cluster_imbalance = 0.05,
        .distance_span_max = 0.1,
    }, 64);
    try std.testing.expectEqual(@as(?usize, 6), policy.target_cluster_count);
    try std.testing.expectEqual(@as(?u32, 3), policy.base_probe_count);
    try std.testing.expectEqual(@as(?u32, 3), policy.shortlist_multiplier);
}

test "adaptive vector build policy for policy uses namespace thresholds" {
    const policy = builder_mod.adaptiveVectorBuildPolicyForPolicy(.{
        .metric = .cosine,
        .cluster_count = 4,
        .base_probe_count = 2,
        .shortlist_multiplier = 2,
        .cluster_imbalance = 0.6,
        .distance_span_max = 0.2,
    }, 64, .{
        .vector_compaction_max_cluster_imbalance = 0.5,
        .vector_compaction_max_distance_span = 0.75,
    });
    try std.testing.expectEqual(@as(?usize, 6), policy.target_cluster_count);
    try std.testing.expectEqual(@as(?u32, 3), policy.base_probe_count);
    try std.testing.expectEqual(@as(?u32, 3), policy.shortlist_multiplier);
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
    const slice = std.fmt.bufPrint(buf, "/tmp/antfly-serverless-compactor-{s}-{d}-{d}\x00", .{ label, nowNs(), nonce }) catch unreachable;
    return @ptrCast(slice.ptr);
}

fn cleanupTmp(path: [*:0]const u8) void {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), std.mem.span(path)) catch {};
}
