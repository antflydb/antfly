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
const catalog_mod = @import("../catalog/mod.zig");
const manifest_mod = @import("../manifest/mod.zig");
const wal_mod = @import("../wal/mod.zig");
const query_mod = @import("../query/mod.zig");
const query_reader = @import("../query/indexed_reader.zig");
const document_projection = @import("../document_projection.zig");
const search_sources = @import("../search_sources.zig");
const catalog_types = @import("../catalog/types.zig");
const document_segment_mod = @import("../document_segment/mod.zig");
const graph_segment_mod = @import("../graph_segment/mod.zig");
const segment_mod = @import("../segment/mod.zig");
const text_segment_mod = @import("../text_segment/mod.zig");
const sparse_segment_mod = @import("../sparse_segment/mod.zig");
const vector_segment_mod = @import("../vector_segment/mod.zig");
const vector_index = @import("vector_index.zig");
const publication_plan = @import("publication_plan.zig");
const enrichment_pipeline = @import("../enrichment/pipeline.zig");
const api_codec = @import("../api/codec.zig");
const api_types = @import("../api/types.zig");
const full_text_indexes = @import("../../api/full_text_indexes.zig");
const tables_api = @import("../../api/tables.zig");
const shared_vector = @import("antfly_vector").vector;

const FullTextIndexSpec = full_text_indexes.FullTextIndexSpec;
const FullTextSourceMode = full_text_indexes.FullTextSourceMode;

pub const BuildResult = struct {
    namespace: []u8,
    published: bool,
    version: u64,
    wal_start_lsn: u64,
    wal_end_lsn: u64,
    artifact_count: usize,

    pub fn deinit(self: *BuildResult, alloc: Allocator) void {
        alloc.free(self.namespace);
        self.* = undefined;
    }
};

pub const PredictedPublicationActions = struct {
    artifact_actions: publication_plan.ArtifactActions = .{},
    full_text_index_actions: []publication_plan.FullTextIndexAction = &.{},
    vector_index_actions: []publication_plan.NamedArtifactAction = &.{},
    sparse_index_actions: []publication_plan.NamedArtifactAction = &.{},
    graph_index_actions: []publication_plan.NamedArtifactAction = &.{},
    derived_output_actions: publication_plan.DerivedOutputActions = .{},
    pending_enrichment_stage: ?catalog_types.EnrichmentStage = null,
    pending_enrichment_document_count: u64 = 0,

    pub fn deinit(self: *PredictedPublicationActions, alloc: Allocator) void {
        for (self.full_text_index_actions) |*entry| entry.deinit(alloc);
        if (self.full_text_index_actions.len > 0) alloc.free(self.full_text_index_actions);
        for (self.vector_index_actions) |*entry| entry.deinit(alloc);
        if (self.vector_index_actions.len > 0) alloc.free(self.vector_index_actions);
        for (self.sparse_index_actions) |*entry| entry.deinit(alloc);
        if (self.sparse_index_actions.len > 0) alloc.free(self.sparse_index_actions);
        for (self.graph_index_actions) |*entry| entry.deinit(alloc);
        if (self.graph_index_actions.len > 0) alloc.free(self.graph_index_actions);
        self.* = undefined;
    }
};

pub const NamedVectorBuildPolicy = struct {
    index_name: []const u8,
    policy: ?vector_index.BuildPolicy = null,
};

pub const Builder = struct {
    alloc: Allocator,
    artifacts: *artifacts_mod.ArtifactStore,
    manifests: *manifest_mod.ManifestStore,
    progress: *catalog_mod.ProgressStore,
    wal: *wal_mod.WalStore,

    const CurrentHeadManifest = struct {
        progress_version: u64 = 0,
        manifest_version: u64 = 0,
        manifest: ?manifest_mod.Manifest = null,

        fn deinit(self: *CurrentHeadManifest, alloc: Allocator) void {
            if (self.manifest) |*manifest| manifest.deinit(alloc);
            self.* = undefined;
        }
    };

    pub fn init(
        alloc: Allocator,
        artifacts: *artifacts_mod.ArtifactStore,
        manifests: *manifest_mod.ManifestStore,
        progress: *catalog_mod.ProgressStore,
        wal: *wal_mod.WalStore,
    ) Builder {
        return .{
            .alloc = alloc,
            .artifacts = artifacts,
            .manifests = manifests,
            .progress = progress,
            .wal = wal,
        };
    }

    fn loadCurrentHeadManifestAlloc(self: *Builder, namespace: []const u8) !CurrentHeadManifest {
        const progress_version = self.progress.getHead(namespace) catch |err| switch (err) {
            error.FileNotFound => 0,
            else => return err,
        };
        if (progress_version == 0) return .{};

        var manifest = self.manifests.getAlloc(namespace, progress_version) catch |err| switch (err) {
            error.FileNotFound => null,
            else => return err,
        };
        if (manifest != null) {
            return .{
                .progress_version = progress_version,
                .manifest_version = progress_version,
                .manifest = manifest,
            };
        }

        const versions = try self.manifests.listVersionsAlloc(namespace);
        defer self.alloc.free(versions);
        if (versions.len == 0) {
            return .{
                .progress_version = progress_version,
            };
        }

        const manifest_version = versions[versions.len - 1];
        manifest = try self.manifests.getAlloc(namespace, manifest_version);
        return .{
            .progress_version = progress_version,
            .manifest_version = manifest_version,
            .manifest = manifest,
        };
    }

    pub fn publishNamespace(self: *Builder, namespace: []const u8) !BuildResult {
        return try self.publishNamespaceWithMetric(namespace, .cosine);
    }

    pub fn publishNamespaceWithMetric(
        self: *Builder,
        namespace: []const u8,
        vector_metric: shared_vector.DistanceMetric,
    ) !BuildResult {
        return try self.publishNamespaceWithMetricAndSearchSources(
            namespace,
            vector_metric,
            search_sources.defaultPublishedSearchSources(),
        );
    }

    pub fn publishNamespaceWithMetricAndSearchSources(
        self: *Builder,
        namespace: []const u8,
        vector_metric: shared_vector.DistanceMetric,
        published_search_sources: search_sources.PublishedSearchSources,
    ) !BuildResult {
        return try self.publishNamespaceWithMetricAndTargets(namespace, vector_metric, .{
            .published_search_sources = published_search_sources,
            .include_graph = true,
        }, false);
    }

    pub const PublicationTargets = struct {
        published_search_sources: search_sources.PublishedSearchSources,
        include_graph: bool = true,
    };

    pub fn publishNamespaceWithMetricAndTargets(
        self: *Builder,
        namespace: []const u8,
        vector_metric: shared_vector.DistanceMetric,
        targets: PublicationTargets,
        force_republish_from_head: bool,
    ) !BuildResult {
        return try self.publishNamespaceWithMetricAndPlan(namespace, vector_metric, .{
            .targets = targets,
            .policy = .{},
            .table_definition = .{},
            .metadata_republish = if (force_republish_from_head)
                .{ .published_search_sources_changed = true }
            else
                .{},
            .artifact_actions = if (force_republish_from_head)
                .{
                    .document_segment = .reuse,
                    .full_text = .reuse,
                    .dense_vector = if (targets.published_search_sources.findVector() == null) .drop else .reuse,
                    .sparse_vector = if (targets.published_search_sources.findSparse() == null) .drop else .reuse,
                    .graph = if (targets.include_graph) .reuse else .drop,
                }
            else
                .{},
            .derived_output_actions = if (force_republish_from_head)
                .{
                    .chunk_preview = .reuse,
                    .chunk_embeddings = .reuse,
                    .rerank_terms = .reuse,
                }
            else
                .{},
        });
    }

    pub fn publishNamespaceWithMetricAndPlan(
        self: *Builder,
        namespace: []const u8,
        vector_metric: shared_vector.DistanceMetric,
        plan: publication_plan.TablePublicationPlan,
    ) !BuildResult {
        const targets = plan.targets;
        var head = try self.loadCurrentHeadManifestAlloc(namespace);
        defer head.deinit(self.alloc);
        const current_head = head.progress_version;
        const current_manifest = head.manifest;

        const next_version: u64 = current_head + 1;
        const start_lsn: u64 = if (current_manifest) |current| current.wal_end_lsn + 1 else 1;

        const records = try self.wal.readFromAlloc(namespace, start_lsn);
        defer wal_mod.freeRecords(self.alloc, records);

        if (records.len == 0) {
            if (current_head != 0 and plan.forceRepublishFromHead()) {
                return try self.republishHeadWithPlan(namespace, current_head, vector_metric, plan);
            }
            return .{
                .namespace = try self.alloc.dupe(u8, namespace),
                .published = false,
                .version = current_head,
                .wal_start_lsn = start_lsn,
                .wal_end_lsn = if (current_head == 0) 0 else start_lsn - 1,
                .artifact_count = 0,
            };
        }

        const inline_document_rebase = if (current_manifest) |current|
            shouldInlineDocumentRebase(current, next_version, plan.policy)
        else
            false;

        const mutation_entries = if (inline_document_rebase)
            try self.alloc.alloc(segment_mod.Entry, 0)
        else if (current_manifest) |current|
            try mergeManifestMutationEntriesWithRecordsAlloc(self.alloc, self.artifacts, current, records)
        else
            try allocMutationEntriesFromRecords(self.alloc, records);
        defer segment_mod.freeEntries(self.alloc, mutation_entries);

        const mutation_artifact = if (inline_document_rebase)
            null
        else blk: {
            const mutation_payload = try segment_mod.encodeAlloc(self.alloc, mutation_entries);
            defer self.alloc.free(mutation_payload);
            var artifact = try self.artifacts.put(mutation_payload);
            errdefer artifact.deinit(self.alloc);
            break :blk artifact;
        };
        defer if (mutation_artifact) |artifact| {
            var owned = artifact;
            owned.deinit(self.alloc);
        };

        const materialized = try materializeWalDocumentsAlloc(self, namespace, current_head, records, targets.include_graph);
        defer freeMaterializerMutations(self.alloc, materialized.mutations);
        defer query_mod.freeMaterializedDocuments(self.alloc, materialized.base_documents);
        defer query_mod.freeMaterializedDocuments(self.alloc, materialized.documents);

        const document_ref = blk: {
            const entries = try allocDocumentSegmentEntries(self.alloc, materialized.documents);
            defer document_segment_mod.freeEntries(self.alloc, entries);
            const payload = try document_segment_mod.encodeAlloc(self.alloc, entries);
            defer self.alloc.free(payload);
            var artifact = try self.artifacts.put(payload);
            defer artifact.deinit(self.alloc);
            break :blk try artifactRefFromMetadataAlloc(self.alloc, .document_segment, artifact);
        };
        defer freeArtifactRef(self.alloc, document_ref);
        const text_index_specs = try resolvePublishedTextIndexSpecsAlloc(self.alloc, plan.table_definition, plan.full_text_index_actions);
        defer full_text_indexes.freeFullTextIndexSpecs(self.alloc, text_index_specs);
        const text_refs = try buildTextArtifactRefsForMaterializedDocsAlloc(
            self.alloc,
            self.artifacts,
            current_manifest,
            materialized.base_documents,
            materialized.documents,
            materialized.mutations,
            text_index_specs,
        );
        defer freeArtifactRefs(self.alloc, text_refs);
        const sparse_refs = try buildSparseArtifactRefsForMaterializedDocsAlloc(
            self.alloc,
            self.artifacts,
            current_manifest,
            materialized.base_documents,
            materialized.documents,
            materialized.mutations,
            targets.published_search_sources,
        );
        defer freeArtifactRefs(self.alloc, sparse_refs);
        const vector_refs = try buildVectorArtifactRefsForMaterializedDocsAlloc(
            self.alloc,
            self.artifacts,
            current_manifest,
            vector_metric,
            materialized.base_documents,
            materialized.documents,
            materialized.mutations,
            null,
            &.{},
            targets.published_search_sources,
        );
        defer freeArtifactRefs(self.alloc, vector_refs);
        const graph_index_names = try listGraphIndexNamesAlloc(self.alloc, plan.table_definition.indexes_json);
        defer freeOwnedStrings(self.alloc, graph_index_names);
        const graph_refs = try buildGraphArtifactRefsForMaterializedDocsAlloc(
            self.alloc,
            self.artifacts,
            current_manifest,
            materialized.base_documents,
            materialized.documents,
            materialized.mutations,
            graph_index_names,
            targets.include_graph,
        );
        defer freeArtifactRefs(self.alloc, graph_refs);

        const wal_end_lsn = records[records.len - 1].lsn;
        const built_at_ns = records[records.len - 1].timestamp_ns;
        var derived_outputs = try detectMaterializedDerivedOutputsAlloc(
            self.alloc,
            materialized.documents,
            plan.table_definition.indexes_json,
            .{},
        );
        defer search_sources.deinitMaterializedDerivedOutputs(self.alloc, &derived_outputs);
        const document_base_version = if (!inline_document_rebase) blk: {
            if (current_manifest) |current| {
                if (findArtifactIndex(current, .document_segment) != null) {
                    break :blk if (current.stats.document_base_version != 0) current.stats.document_base_version else current.version;
                }
            }
            break :blk next_version;
        } else next_version;
        var manifest = if (inline_document_rebase)
            try buildRebasedManifestFromRefsAlloc(
                self.alloc,
                namespace,
                next_version,
                built_at_ns,
                wal_end_lsn,
                materialized.documents.len,
                document_base_version,
                .inline_rebase,
                document_ref,
                text_refs,
                sparse_refs,
                vector_refs,
                graph_refs,
                derived_outputs,
                plan.policy,
                plan.table_definition,
            )
        else
            try buildManifestAlloc(
                self.alloc,
                namespace,
                next_version,
                built_at_ns,
                start_lsn,
                wal_end_lsn,
                materialized.documents.len,
                document_base_version,
                .append_mutation_tail,
                mutation_artifact.?,
                document_ref,
                text_refs,
                sparse_refs,
                vector_refs,
                graph_refs,
                derived_outputs,
                plan.policy,
                plan.table_definition,
            );
        defer manifest.deinit(self.alloc);

        self.manifests.put(manifest) catch |err| switch (err) {
            error.ManifestVersionAlreadyExists => return error.HeadChanged,
            else => return err,
        };
        const published = try self.progress.compareAndSwapHead(namespace, if (current_head == 0) null else current_head, next_version);
        if (!published) return error.HeadChanged;

        return .{
            .namespace = try self.alloc.dupe(u8, namespace),
            .published = true,
            .version = next_version,
            .wal_start_lsn = if (inline_document_rebase) wal_end_lsn else start_lsn,
            .wal_end_lsn = wal_end_lsn,
            .artifact_count = manifest.artifacts.len,
        };
    }

    fn republishHeadWithTargets(
        self: *Builder,
        namespace: []const u8,
        current_head: u64,
        vector_metric: shared_vector.DistanceMetric,
        targets: PublicationTargets,
    ) !BuildResult {
        return try self.republishHeadWithPlan(namespace, current_head, vector_metric, .{
            .targets = targets,
            .metadata_republish = .{ .published_search_sources_changed = true },
        });
    }

    fn republishHeadWithPlan(
        self: *Builder,
        namespace: []const u8,
        current_head: u64,
        vector_metric: shared_vector.DistanceMetric,
        plan: publication_plan.TablePublicationPlan,
    ) !BuildResult {
        const targets = plan.targets;
        var current = try self.manifests.getAlloc(namespace, current_head);
        defer current.deinit(self.alloc);

        var docs_cache: ?[]query_mod.QueryMaterializedDocument = null;
        defer if (docs_cache) |docs| query_mod.freeMaterializedDocuments(self.alloc, docs);

        const document_ref = if (plan.artifact_actions.document_segment == .reuse) blk: {
            if (findArtifactIndex(current, .document_segment)) |artifact_index| {
                break :blk try cloneArtifactRefAlloc(self.alloc, current.artifacts[artifact_index]);
            }
            const docs = try ensurePublishedDocumentsAlloc(self, namespace, current_head, &docs_cache);
            const document_entries = try allocDocumentSegmentEntries(self.alloc, docs);
            defer document_segment_mod.freeEntries(self.alloc, document_entries);
            const document_payload = try document_segment_mod.encodeAlloc(self.alloc, document_entries);
            defer self.alloc.free(document_payload);
            var document_artifact = try self.artifacts.put(document_payload);
            defer document_artifact.deinit(self.alloc);
            break :blk try artifactRefFromMetadataAlloc(self.alloc, .document_segment, document_artifact);
        } else blk: {
            const docs = try ensurePublishedDocumentsAlloc(self, namespace, current_head, &docs_cache);
            const document_entries = try allocDocumentSegmentEntries(self.alloc, docs);
            defer document_segment_mod.freeEntries(self.alloc, document_entries);
            const document_payload = try document_segment_mod.encodeAlloc(self.alloc, document_entries);
            defer self.alloc.free(document_payload);
            var document_artifact = try self.artifacts.put(document_payload);
            defer document_artifact.deinit(self.alloc);
            break :blk try artifactRefFromMetadataAlloc(self.alloc, .document_segment, document_artifact);
        };
        defer freeArtifactRef(self.alloc, document_ref);

        const text_index_specs = try resolvePublishedTextIndexSpecsAlloc(self.alloc, plan.table_definition, plan.full_text_index_actions);
        defer full_text_indexes.freeFullTextIndexSpecs(self.alloc, text_index_specs);

        const effective_full_text_action = plan.effectiveFullTextAction(findArtifactIndex(current, .text_segment) != null);
        const needs_text_rebuild = blk: {
            if (text_index_specs.len == 0) break :blk false;
            if (effective_full_text_action == .rebuild) break :blk true;
            for (text_index_specs) |spec| {
                if (findNamedArtifactIndex(current, .text_segment, spec.name) == null) break :blk true;
            }
            break :blk false;
        };
        const text_refs = if (!needs_text_rebuild and text_index_specs.len == 0)
            try self.alloc.alloc(manifest_mod.ArtifactRef, 0)
        else blk: {
            const docs = if (needs_text_rebuild)
                try ensurePublishedDocumentsAlloc(self, namespace, current_head, &docs_cache)
            else
                &.{};
            break :blk try buildTextArtifactRefsForRepublishAlloc(
                self.alloc,
                self.artifacts,
                current,
                docs,
                text_index_specs,
                plan.full_text_index_actions,
                effective_full_text_action,
            );
        };
        defer freeArtifactRefs(self.alloc, text_refs);

        const republish_docs = if (plan.artifact_actions.sparse_vector == .drop and plan.artifact_actions.dense_vector == .drop)
            &.{}
        else
            try ensurePublishedDocumentsAlloc(self, namespace, current_head, &docs_cache);
        const sparse_refs = try buildSparseArtifactRefsForRepublishAlloc(
            self.alloc,
            self.artifacts,
            current,
            republish_docs,
            targets.published_search_sources,
            plan.sparse_index_actions,
            plan.artifact_actions.sparse_vector,
        );
        defer freeArtifactRefs(self.alloc, sparse_refs);

        const vector_refs = try buildVectorArtifactRefsForRepublishAlloc(
            self.alloc,
            self.artifacts,
            current,
            vector_metric,
            republish_docs,
            targets.published_search_sources,
            plan.vector_index_actions,
            plan.artifact_actions.dense_vector,
        );
        defer freeArtifactRefs(self.alloc, vector_refs);

        const graph_index_names = try listGraphIndexNamesAlloc(self.alloc, plan.table_definition.indexes_json);
        defer freeOwnedStrings(self.alloc, graph_index_names);
        const graph_docs = if (plan.artifact_actions.graph == .drop or !targets.include_graph)
            &.{}
        else
            try ensurePublishedDocumentsAlloc(self, namespace, current_head, &docs_cache);
        const graph_refs = try buildGraphArtifactRefsForRepublishAlloc(
            self.alloc,
            self.artifacts,
            current,
            graph_docs,
            graph_index_names,
            plan.artifact_actions.graph,
            targets.include_graph,
        );
        defer freeArtifactRefs(self.alloc, graph_refs);

        var scanned_derived_outputs: ?search_sources.MaterializedDerivedOutputs = null;
        defer if (scanned_derived_outputs) |*outputs| search_sources.deinitMaterializedDerivedOutputs(self.alloc, outputs);
        if (plan.derived_output_actions.chunk_preview == .recompute or
            plan.derived_output_actions.chunk_embeddings == .recompute or
            plan.derived_output_actions.rerank_terms == .recompute)
        {
            const docs = try ensurePublishedDocumentsAlloc(self, namespace, current_head, &docs_cache);
            scanned_derived_outputs = try detectMaterializedDerivedOutputsAlloc(
                self.alloc,
                docs,
                current.stats.indexes_json,
                .{
                    .chunk_preview = plan.derived_output_actions.chunk_preview == .recompute,
                    .chunk_embeddings = plan.derived_output_actions.chunk_embeddings == .recompute,
                    .rerank_terms = plan.derived_output_actions.rerank_terms == .recompute,
                },
            );
        }

        var derived_outputs = try mergeDerivedOutputsAlloc(
            self.alloc,
            current.stats.derived_outputs,
            scanned_derived_outputs orelse .{},
            plan.derived_output_actions,
        );
        defer search_sources.deinitMaterializedDerivedOutputs(self.alloc, &derived_outputs);

        const next_version = current_head + 1;
        var manifest = try buildCompactedManifestFromRefsAlloc(
            self.alloc,
            namespace,
            next_version,
            current.wal_end_lsn,
            @intCast(current.stats.document_count),
            if (plan.artifact_actions.document_segment == .reuse)
                if (current.stats.document_base_version != 0) current.stats.document_base_version else current.version
            else
                next_version,
            .head_republish,
            document_ref,
            text_refs,
            sparse_refs,
            vector_refs,
            graph_refs,
            derived_outputs,
            plan.policy,
            plan.table_definition,
        );
        defer manifest.deinit(self.alloc);

        self.manifests.put(manifest) catch |err| switch (err) {
            error.ManifestVersionAlreadyExists => return error.HeadChanged,
            else => return err,
        };
        const published = try self.progress.compareAndSwapHead(namespace, current_head, next_version);
        if (!published) return error.HeadChanged;

        return .{
            .namespace = try self.alloc.dupe(u8, namespace),
            .published = true,
            .version = next_version,
            .wal_start_lsn = current.wal_end_lsn,
            .wal_end_lsn = current.wal_end_lsn,
            .artifact_count = manifest.artifacts.len,
        };
    }

    pub fn predictPendingWalPublicationActionsAlloc(
        self: *Builder,
        namespace: []const u8,
        vector_metric: shared_vector.DistanceMetric,
        plan: publication_plan.TablePublicationPlan,
    ) !?PredictedPublicationActions {
        _ = vector_metric;
        if (plan.forceRepublishFromHead()) return null;

        const current_head = self.progress.getHead(namespace) catch |err| switch (err) {
            error.FileNotFound => 0,
            else => return err,
        };
        if (current_head == 0) return null;

        var head = try self.loadCurrentHeadManifestAlloc(namespace);
        defer head.deinit(self.alloc);
        const current = head.manifest orelse return null;

        const start_lsn = current.wal_end_lsn + 1;
        const records = try self.wal.readFromAlloc(namespace, start_lsn);
        defer wal_mod.freeRecords(self.alloc, records);
        if (records.len == 0) return null;

        const built_documents = try buildDocumentSegmentAlloc(self, namespace, current_head, records, plan.targets.include_graph);
        defer freeMaterializerMutations(self.alloc, built_documents.mutations);
        defer query_mod.freeMaterializedDocuments(self.alloc, built_documents.base_documents);
        defer query_mod.freeMaterializedDocuments(self.alloc, built_documents.documents);
        defer self.alloc.free(built_documents.payload);

        const text_index_specs = try resolvePublishedTextIndexSpecsAlloc(self.alloc, plan.table_definition, plan.full_text_index_actions);
        defer full_text_indexes.freeFullTextIndexSpecs(self.alloc, text_index_specs);
        const full_text_index_actions = try predictFullTextIndexActionsAlloc(
            self.alloc,
            current,
            built_documents.base_documents,
            built_documents.documents,
            built_documents.mutations,
            text_index_specs,
            plan.full_text_index_actions,
            plan.artifact_actions.full_text,
        );
        errdefer {
            for (full_text_index_actions) |*entry| entry.deinit(self.alloc);
            if (full_text_index_actions.len > 0) self.alloc.free(full_text_index_actions);
        }

        const vector_index_actions = try predictNamedArtifactActionsAlloc(
            self.alloc,
            current,
            .vector_segment,
            built_documents.base_documents,
            built_documents.documents,
            built_documents.mutations,
            plan.targets.published_search_sources,
            .vector,
            plan.vector_index_actions,
            plan.artifact_actions.dense_vector,
        );
        errdefer {
            for (vector_index_actions) |*entry| entry.deinit(self.alloc);
            if (vector_index_actions.len > 0) self.alloc.free(vector_index_actions);
        }

        const sparse_index_actions = try predictNamedArtifactActionsAlloc(
            self.alloc,
            current,
            .sparse_segment,
            built_documents.base_documents,
            built_documents.documents,
            built_documents.mutations,
            plan.targets.published_search_sources,
            .sparse,
            plan.sparse_index_actions,
            plan.artifact_actions.sparse_vector,
        );
        errdefer {
            for (sparse_index_actions) |*entry| entry.deinit(self.alloc);
            if (sparse_index_actions.len > 0) self.alloc.free(sparse_index_actions);
        }
        const graph_index_actions = try predictNamedArtifactActionsAlloc(
            self.alloc,
            current,
            .graph_segment,
            built_documents.base_documents,
            built_documents.documents,
            built_documents.mutations,
            plan.targets.published_search_sources,
            .graph,
            plan.graph_index_actions,
            plan.artifact_actions.graph,
        );
        errdefer {
            for (graph_index_actions) |*entry| entry.deinit(self.alloc);
            if (graph_index_actions.len > 0) self.alloc.free(graph_index_actions);
        }
        var detected_outputs = try detectMaterializedDerivedOutputsAlloc(
            self.alloc,
            built_documents.documents,
            plan.table_definition.indexes_json,
            .{
                .chunk_preview = plan.derived_output_actions.chunk_preview != .drop,
                .chunk_embeddings = plan.derived_output_actions.chunk_embeddings != .drop,
                .rerank_terms = plan.derived_output_actions.rerank_terms != .drop,
            },
        );
        defer search_sources.deinitMaterializedDerivedOutputs(self.alloc, &detected_outputs);
        const pending_enrichment = try predictPendingEnrichmentAlloc(
            self.alloc,
            built_documents.documents,
            plan.policy,
            .{
                .lexical_sparse = plan.artifact_actions.sparse_vector != .drop,
                .chunk_preview = plan.derived_output_actions.chunk_preview != .drop,
                .chunk_embeddings = plan.derived_output_actions.chunk_embeddings != .drop,
                .rerank_terms = plan.derived_output_actions.rerank_terms != .drop,
            },
        );

        return .{
            .artifact_actions = .{
                .document_segment = if (findArtifactIndex(current, .document_segment) != null) .reuse else .rebuild,
                .full_text = publication_plan.collapseFullTextArtifactAction(
                    full_text_index_actions,
                    findArtifactIndex(current, .text_segment) != null,
                    plan.artifact_actions.full_text,
                ),
                .dense_vector = publication_plan.collapseNamedArtifactAction(
                    vector_index_actions,
                    findArtifactIndex(current, .vector_segment) != null,
                    plan.artifact_actions.dense_vector,
                ),
                .sparse_vector = publication_plan.collapseNamedArtifactAction(
                    sparse_index_actions,
                    findArtifactIndex(current, .sparse_segment) != null,
                    plan.artifact_actions.sparse_vector,
                ),
                .graph = if (!plan.targets.include_graph)
                    .drop
                else
                    publication_plan.collapseNamedArtifactAction(
                        graph_index_actions,
                        findArtifactIndex(current, .graph_segment) != null,
                        if (built_documents.graph_changed) .rebuild else plan.artifact_actions.graph,
                    ),
            },
            .full_text_index_actions = full_text_index_actions,
            .vector_index_actions = vector_index_actions,
            .sparse_index_actions = sparse_index_actions,
            .graph_index_actions = graph_index_actions,
            .derived_output_actions = .{
                .chunk_preview = if (pending_enrichment.chunk_preview_pending_documents > 0)
                    .recompute
                else
                    predictDerivedOutputAction(
                        current.stats.derived_outputs,
                        detected_outputs,
                        .chunk_preview,
                        plan.derived_output_actions.chunk_preview,
                    ),
                .chunk_embeddings = if (pending_enrichment.chunk_embeddings_pending_documents > 0)
                    .recompute
                else
                    predictDerivedOutputAction(
                        current.stats.derived_outputs,
                        detected_outputs,
                        .chunk_embeddings,
                        plan.derived_output_actions.chunk_embeddings,
                    ),
                .rerank_terms = if (pending_enrichment.rerank_terms_pending_documents > 0)
                    .recompute
                else
                    predictDerivedOutputAction(
                        current.stats.derived_outputs,
                        detected_outputs,
                        .rerank_terms,
                        plan.derived_output_actions.rerank_terms,
                    ),
            },
            .pending_enrichment_stage = pending_enrichment.activeStage(),
            .pending_enrichment_document_count = pending_enrichment.activeStagePendingDocuments(),
        };
    }
};

fn buildManifestAlloc(
    alloc: Allocator,
    namespace: []const u8,
    version: u64,
    built_at_ns: u64,
    wal_start_lsn: u64,
    wal_end_lsn: u64,
    document_count: usize,
    document_base_version: u64,
    document_publish_mode: catalog_types.DocumentPublishMode,
    mutation_artifact: artifacts_mod.ArtifactMetadata,
    document_ref: manifest_mod.ArtifactRef,
    text_refs: []const manifest_mod.ArtifactRef,
    sparse_refs: []const manifest_mod.ArtifactRef,
    vector_refs: []const manifest_mod.ArtifactRef,
    graph_refs: []const manifest_mod.ArtifactRef,
    derived_outputs: search_sources.MaterializedDerivedOutputs,
    policy: @import("../catalog/types.zig").NamespacePolicy,
    table_definition: publication_plan.TableDefinitionSnapshot,
) !manifest_mod.Manifest {
    const text_count: usize = text_refs.len;
    const sparse_count: usize = sparse_refs.len;
    const vector_count: usize = vector_refs.len;
    const graph_count: usize = graph_refs.len;
    const artifacts = try alloc.alloc(manifest_mod.ArtifactRef, 2 + text_count + sparse_count + vector_count + graph_count);
    errdefer alloc.free(artifacts);
    artifacts[0] = .{
        .kind = .mutation_segment,
        .artifact_id = try alloc.dupe(u8, mutation_artifact.artifact_id),
        .byte_len = mutation_artifact.byte_len,
        .checksum = try alloc.dupe(u8, mutation_artifact.checksum),
    };
    artifacts[1] = try cloneArtifactRefAlloc(alloc, document_ref);
    var artifact_index: usize = 2;
    for (text_refs) |text_ref| {
        artifacts[artifact_index] = try cloneArtifactRefAlloc(alloc, text_ref);
        artifact_index += 1;
    }
    for (sparse_refs) |sparse_ref| {
        artifacts[artifact_index] = try cloneArtifactRefAlloc(alloc, sparse_ref);
        artifact_index += 1;
    }
    for (vector_refs) |vector_ref| {
        artifacts[artifact_index] = try cloneArtifactRefAlloc(alloc, vector_ref);
        artifact_index += 1;
    }
    for (graph_refs) |graph_ref| {
        artifacts[artifact_index] = try cloneArtifactRefAlloc(alloc, graph_ref);
        artifact_index += 1;
    }
    const text_index_names = try collectTextIndexNamesAlloc(alloc, text_refs);
    defer freeOwnedStrings(alloc, text_index_names);
    const text_index_specs = try textIndexSpecsFromNamesAlloc(alloc, text_index_names);
    defer full_text_indexes.freeFullTextIndexSpecs(alloc, text_index_specs);
    var manifest_sources = try buildPublishedSearchSourcesForManifestAlloc(
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
        .built_at_ns = built_at_ns,
        .wal_start_lsn = wal_start_lsn,
        .wal_end_lsn = wal_end_lsn,
        .stats = .{
            .document_count = @intCast(document_count),
            .document_base_version = document_base_version,
            .document_publish_mode = document_publish_mode,
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
    derived_outputs: search_sources.MaterializedDerivedOutputs,
    policy: @import("../catalog/types.zig").NamespacePolicy,
    table_definition: publication_plan.TableDefinitionSnapshot,
) !manifest_mod.Manifest {
    const document_ref = try artifactRefFromMetadataAlloc(alloc, .document_segment, document_artifact);
    defer freeArtifactRef(alloc, document_ref);

    return try buildCompactedManifestFromRefsAlloc(
        alloc,
        namespace,
        version,
        wal_end_lsn,
        document_count,
        version,
        .inline_rebase,
        document_ref,
        text_refs,
        sparse_refs,
        vector_refs,
        graph_refs,
        derived_outputs,
        policy,
        table_definition,
    );
}

fn buildRebasedManifestFromRefsAlloc(
    alloc: Allocator,
    namespace: []const u8,
    version: u64,
    built_at_ns: u64,
    wal_end_lsn: u64,
    document_count: usize,
    document_base_version: u64,
    document_publish_mode: catalog_types.DocumentPublishMode,
    document_ref: manifest_mod.ArtifactRef,
    text_refs: []const manifest_mod.ArtifactRef,
    sparse_refs: []const manifest_mod.ArtifactRef,
    vector_refs: []const manifest_mod.ArtifactRef,
    graph_refs: []const manifest_mod.ArtifactRef,
    derived_outputs: search_sources.MaterializedDerivedOutputs,
    policy: @import("../catalog/types.zig").NamespacePolicy,
    table_definition: publication_plan.TableDefinitionSnapshot,
) !manifest_mod.Manifest {
    const text_count: usize = text_refs.len;
    const sparse_count: usize = sparse_refs.len;
    const vector_count: usize = vector_refs.len;
    const graph_count: usize = graph_refs.len;
    const artifacts = try alloc.alloc(manifest_mod.ArtifactRef, 1 + text_count + sparse_count + vector_count + graph_count);
    errdefer alloc.free(artifacts);
    artifacts[0] = try cloneArtifactRefAlloc(alloc, document_ref);
    var artifact_index: usize = 1;
    for (text_refs) |text_ref| {
        artifacts[artifact_index] = try cloneArtifactRefAlloc(alloc, text_ref);
        artifact_index += 1;
    }
    for (sparse_refs) |sparse_ref| {
        artifacts[artifact_index] = try cloneArtifactRefAlloc(alloc, sparse_ref);
        artifact_index += 1;
    }
    for (vector_refs) |vector_ref| {
        artifacts[artifact_index] = try cloneArtifactRefAlloc(alloc, vector_ref);
        artifact_index += 1;
    }
    for (graph_refs) |graph_ref| {
        artifacts[artifact_index] = try cloneArtifactRefAlloc(alloc, graph_ref);
        artifact_index += 1;
    }
    const text_index_names = try collectTextIndexNamesAlloc(alloc, text_refs);
    defer freeOwnedStrings(alloc, text_index_names);
    const text_index_specs = try textIndexSpecsFromNamesAlloc(alloc, text_index_names);
    defer full_text_indexes.freeFullTextIndexSpecs(alloc, text_index_specs);
    var manifest_sources = try buildPublishedSearchSourcesForManifestAlloc(
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
        .built_at_ns = built_at_ns,
        .wal_start_lsn = wal_end_lsn,
        .wal_end_lsn = wal_end_lsn,
        .stats = .{
            .document_count = @intCast(document_count),
            .document_base_version = document_base_version,
            .document_publish_mode = document_publish_mode,
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

fn buildCompactedManifestFromRefsAlloc(
    alloc: Allocator,
    namespace: []const u8,
    version: u64,
    wal_end_lsn: u64,
    document_count: usize,
    document_base_version: u64,
    document_publish_mode: catalog_types.DocumentPublishMode,
    document_ref: manifest_mod.ArtifactRef,
    text_refs: []const manifest_mod.ArtifactRef,
    sparse_refs: []const manifest_mod.ArtifactRef,
    vector_refs: []const manifest_mod.ArtifactRef,
    graph_refs: []const manifest_mod.ArtifactRef,
    derived_outputs: search_sources.MaterializedDerivedOutputs,
    policy: @import("../catalog/types.zig").NamespacePolicy,
    table_definition: publication_plan.TableDefinitionSnapshot,
) !manifest_mod.Manifest {
    const text_count: usize = text_refs.len;
    const sparse_count: usize = sparse_refs.len;
    const vector_count: usize = vector_refs.len;
    const graph_count: usize = graph_refs.len;
    const artifacts = try alloc.alloc(manifest_mod.ArtifactRef, 1 + text_count + sparse_count + vector_count + graph_count);
    errdefer alloc.free(artifacts);
    artifacts[0] = try cloneArtifactRefAlloc(alloc, document_ref);
    var artifact_index: usize = 1;
    for (text_refs) |text_ref| {
        artifacts[artifact_index] = try cloneArtifactRefAlloc(alloc, text_ref);
        artifact_index += 1;
    }
    for (sparse_refs) |sparse_ref| {
        artifacts[artifact_index] = try cloneArtifactRefAlloc(alloc, sparse_ref);
        artifact_index += 1;
    }
    for (vector_refs) |vector_ref| {
        artifacts[artifact_index] = try cloneArtifactRefAlloc(alloc, vector_ref);
        artifact_index += 1;
    }
    for (graph_refs) |graph_ref| {
        artifacts[artifact_index] = try cloneArtifactRefAlloc(alloc, graph_ref);
        artifact_index += 1;
    }
    const text_index_names = try collectTextIndexNamesAlloc(alloc, text_refs);
    defer freeOwnedStrings(alloc, text_index_names);
    const text_index_specs = try textIndexSpecsFromNamesAlloc(alloc, text_index_names);
    defer full_text_indexes.freeFullTextIndexSpecs(alloc, text_index_specs);
    var manifest_sources = try buildPublishedSearchSourcesForManifestAlloc(
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
            .document_base_version = document_base_version,
            .document_publish_mode = document_publish_mode,
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

pub fn detectMaterializedDerivedOutputsAlloc(
    alloc: Allocator,
    docs: []const query_mod.QueryMaterializedDocument,
    indexes_json: []const u8,
    requested: DerivedOutputDetectionSelection,
) !search_sources.MaterializedDerivedOutputs {
    if (!requested.any()) return .{ .items = null };

    var chunked_sources: []full_text_indexes.ChunkedFullTextSource = &.{};
    if (requested.chunk_preview) {
        chunked_sources = try full_text_indexes.listChunkedFullTextSourcesAlloc(alloc, indexes_json);
    }
    defer if (requested.chunk_preview) full_text_indexes.freeChunkedFullTextSources(alloc, chunked_sources);

    var has_chunk_preview = false;
    var has_chunk_embeddings = false;
    var has_rerank_terms = false;
    for (docs) |doc| {
        var projection = try document_projection.parseAlloc(alloc, doc.body);
        defer projection.deinit(alloc);
        if (requested.chunk_preview and projection.chunk_preview != null) has_chunk_preview = true;
        if (requested.chunk_preview and !has_chunk_preview and chunked_sources.len > 0) {
            const chunk_text = try full_text_indexes.synthesizeChunkedFullTextAlloc(alloc, doc.body, chunked_sources);
            defer alloc.free(chunk_text);
            if (chunk_text.len > 0) has_chunk_preview = true;
        }
        if (requested.chunk_embeddings and projection.chunk_embeddings != null) has_chunk_embeddings = true;
        if (requested.rerank_terms and projection.rerank_terms != null) has_rerank_terms = true;
        if ((!requested.chunk_preview or has_chunk_preview) and
            (!requested.chunk_embeddings or has_chunk_embeddings) and
            (!requested.rerank_terms or has_rerank_terms)) break;
    }
    var outputs = std.ArrayListUnmanaged(search_sources.DerivedOutputDescriptor).empty;
    errdefer {
        for (outputs.items) |*item| search_sources.deinitDerivedOutputDescriptor(alloc, item);
        outputs.deinit(alloc);
    }
    if (requested.chunk_preview and has_chunk_preview) {
        try outputs.append(alloc, .{
            .name = try alloc.dupe(u8, search_sources.defaultDerivedOutputName(.chunk_preview)),
            .kind = .chunk_preview,
        });
    }
    if (requested.chunk_embeddings and has_chunk_embeddings) {
        try outputs.append(alloc, .{
            .name = try alloc.dupe(u8, search_sources.defaultDerivedOutputName(.chunk_embeddings)),
            .kind = .chunk_embeddings,
        });
    }
    if (requested.rerank_terms and has_rerank_terms) {
        try outputs.append(alloc, .{
            .name = try alloc.dupe(u8, search_sources.defaultDerivedOutputName(.rerank_terms)),
            .kind = .rerank_terms,
        });
    }
    return .{ .items = if (outputs.items.len == 0) null else try outputs.toOwnedSlice(alloc) };
}

pub const DerivedOutputDetectionSelection = struct {
    chunk_preview: bool = true,
    chunk_embeddings: bool = true,
    rerank_terms: bool = true,

    fn any(self: DerivedOutputDetectionSelection) bool {
        return self.chunk_preview or self.chunk_embeddings or self.rerank_terms;
    }
};

test "detect materialized derived outputs honors requested selection" {
    const alloc = std.testing.allocator;

    const docs = try alloc.alloc(query_mod.QueryMaterializedDocument, 1);
    defer {
        for (docs) |*doc| doc.deinit(alloc);
        alloc.free(docs);
    }
    docs[0] = .{
        .doc_id = try alloc.dupe(u8, "doc-a"),
        .body = try alloc.dupe(u8, "{\"text\":\"alpha\",\"chunk_preview\":[\"alpha\"],\"chunk_embeddings\":[{\"chunk\":\"alpha\",\"embedding\":[1,0]}],\"rerank_terms\":[\"alpha\"]}"),
        .last_lsn = 1,
        .last_timestamp_ns = 1,
    };

    var outputs = try detectMaterializedDerivedOutputsAlloc(
        alloc,
        docs,
        tables_api.default_indexes_json,
        .{
            .chunk_preview = false,
            .chunk_embeddings = true,
            .rerank_terms = false,
        },
    );
    defer search_sources.deinitMaterializedDerivedOutputs(alloc, &outputs);

    try std.testing.expect(!outputs.containsKind(.chunk_preview));
    try std.testing.expect(outputs.containsKind(.chunk_embeddings));
    try std.testing.expect(!outputs.containsKind(.rerank_terms));

    var none_requested = try detectMaterializedDerivedOutputsAlloc(
        alloc,
        docs,
        tables_api.default_indexes_json,
        .{
            .chunk_preview = false,
            .chunk_embeddings = false,
            .rerank_terms = false,
        },
    );
    defer search_sources.deinitMaterializedDerivedOutputs(alloc, &none_requested);

    try std.testing.expectEqual(@as(?[]search_sources.DerivedOutputDescriptor, null), none_requested.items);
}

const PendingEnrichmentPrediction = struct {
    lexical_sparse_pending_documents: u64 = 0,
    chunk_preview_pending_documents: u64 = 0,
    chunk_embeddings_pending_documents: u64 = 0,
    rerank_terms_pending_documents: u64 = 0,
    pipeline: enrichment_pipeline.BuiltinPipeline,

    fn activeStage(self: PendingEnrichmentPrediction) ?catalog_types.EnrichmentStage {
        for (self.pipeline.slice()) |spec| {
            const pending = switch (spec.stage) {
                .lexical_sparse => self.lexical_sparse_pending_documents,
                .chunk_preview => self.chunk_preview_pending_documents,
                .chunk_embeddings => self.chunk_embeddings_pending_documents,
                .rerank_terms => self.rerank_terms_pending_documents,
            };
            if (pending > 0) return spec.stage;
        }
        return null;
    }

    fn activeStagePendingDocuments(self: PendingEnrichmentPrediction) u64 {
        const stage = self.activeStage() orelse return 0;
        return switch (stage) {
            .lexical_sparse => self.lexical_sparse_pending_documents,
            .chunk_preview => self.chunk_preview_pending_documents,
            .chunk_embeddings => self.chunk_embeddings_pending_documents,
            .rerank_terms => self.rerank_terms_pending_documents,
        };
    }
};

const PendingEnrichmentSelection = struct {
    lexical_sparse: bool = true,
    chunk_preview: bool = true,
    chunk_embeddings: bool = true,
    rerank_terms: bool = true,

    fn any(self: PendingEnrichmentSelection) bool {
        return self.lexical_sparse or self.chunk_preview or self.chunk_embeddings or self.rerank_terms;
    }

    fn needsNormalizedText(self: PendingEnrichmentSelection) bool {
        return self.any();
    }

    fn needsChunkSource(self: PendingEnrichmentSelection) bool {
        return self.chunk_embeddings;
    }
};

test "predict pending enrichment honors requested selection" {
    const alloc = std.testing.allocator;

    const docs = try alloc.alloc(query_mod.QueryMaterializedDocument, 1);
    defer {
        for (docs) |*doc| doc.deinit(alloc);
        alloc.free(docs);
    }
    docs[0] = .{
        .doc_id = try alloc.dupe(u8, "doc-a"),
        .body = try alloc.dupe(
            u8,
            "{\"text\":\"alpha bravo\",\"chunk_preview\":[\"alpha bravo\"],\"_enrichment\":{\"lexical_sparse\":true,\"lexical_sparse_version\":0}}",
        ),
        .last_lsn = 1,
        .last_timestamp_ns = 1,
    };

    const prediction = try predictPendingEnrichmentAlloc(
        alloc,
        docs,
        .{
            .enrichment_enabled = true,
            .chunk_preview_enabled = true,
        },
        .{
            .lexical_sparse = false,
            .chunk_preview = true,
            .chunk_embeddings = false,
            .rerank_terms = false,
        },
    );

    try std.testing.expectEqual(@as(u64, 0), prediction.lexical_sparse_pending_documents);
    try std.testing.expectEqual(@as(u64, 1), prediction.chunk_preview_pending_documents);
    try std.testing.expectEqual(@as(u64, 0), prediction.chunk_embeddings_pending_documents);
    try std.testing.expectEqual(@as(u64, 0), prediction.rerank_terms_pending_documents);
    try std.testing.expectEqual(catalog_types.EnrichmentStage.chunk_preview, prediction.activeStage().?);

    const none_requested = try predictPendingEnrichmentAlloc(
        alloc,
        docs,
        .{
            .enrichment_enabled = true,
            .chunk_preview_enabled = true,
        },
        .{
            .lexical_sparse = false,
            .chunk_preview = false,
            .chunk_embeddings = false,
            .rerank_terms = false,
        },
    );

    try std.testing.expectEqual(@as(u64, 0), none_requested.activeStagePendingDocuments());
    try std.testing.expectEqual(@as(?catalog_types.EnrichmentStage, null), none_requested.activeStage());
}

fn predictPendingEnrichmentAlloc(
    alloc: Allocator,
    docs: []const query_mod.QueryMaterializedDocument,
    policy: catalog_types.NamespacePolicy,
    requested: PendingEnrichmentSelection,
) !PendingEnrichmentPrediction {
    var prediction: PendingEnrichmentPrediction = .{
        .pipeline = enrichment_pipeline.builtinPipelineForPolicy(policy),
    };
    if (!requested.any()) return prediction;
    for (docs) |doc| {
        var projection = try document_projection.parseAlloc(alloc, doc.body);
        defer projection.deinit(alloc);

        var normalized_text_present: ?bool = null;
        const has_normalized_text = if (requested.lexical_sparse or requested.chunk_preview or requested.rerank_terms)
            try ensureNormalizedTextPresentAlloc(alloc, projection.text, &normalized_text_present)
        else
            false;
        const has_chunk_source = blk: {
            if (!requested.needsChunkSource()) break :blk false;
            break :blk if (projection.chunk_preview) |chunks|
                chunks.len > 0
            else
                try ensureNormalizedTextPresentAlloc(alloc, projection.text, &normalized_text_present);
        };

        if (requested.lexical_sparse and policy.enrichment_enabled and has_normalized_text and
            (projection.lexical_sparse_version == null or projection.lexical_sparse_version.? < policy.enrichment_pipeline_version))
        {
            prediction.lexical_sparse_pending_documents += 1;
        }
        if (requested.chunk_preview and policy.chunk_preview_enabled and has_normalized_text and
            (projection.chunk_preview_version == null or projection.chunk_preview_version.? < policy.chunk_preview_pipeline_version))
        {
            prediction.chunk_preview_pending_documents += 1;
        }
        if (requested.chunk_embeddings and policy.chunk_embeddings_enabled and has_chunk_source and
            (projection.chunk_embeddings_version == null or projection.chunk_embeddings_version.? < policy.chunk_embeddings_pipeline_version))
        {
            prediction.chunk_embeddings_pending_documents += 1;
        }
        if (requested.rerank_terms and policy.rerank_terms_enabled and has_normalized_text and
            (projection.rerank_terms_version == null or projection.rerank_terms_version.? < policy.rerank_terms_pipeline_version))
        {
            prediction.rerank_terms_pending_documents += 1;
        }
    }
    return prediction;
}

fn ensureNormalizedTextPresentAlloc(alloc: Allocator, text: []const u8, cached: *?bool) !bool {
    if (cached.*) |present| return present;
    const normalized = try query_reader.normalizeAlloc(alloc, text);
    defer alloc.free(normalized);
    const present = normalized.len > 0;
    cached.* = present;
    return present;
}

fn mergeDerivedOutputsAlloc(
    alloc: Allocator,
    current: search_sources.MaterializedDerivedOutputs,
    recomputed: search_sources.MaterializedDerivedOutputs,
    actions: publication_plan.DerivedOutputActions,
) !search_sources.MaterializedDerivedOutputs {
    var outputs = std.ArrayListUnmanaged(search_sources.DerivedOutputDescriptor).empty;
    errdefer {
        for (outputs.items) |*item| search_sources.deinitDerivedOutputDescriptor(alloc, item);
        outputs.deinit(alloc);
    }

    switch (actions.chunk_preview) {
        .reuse => if (current.findByKind(.chunk_preview)) |descriptor| {
            try outputs.append(alloc, try search_sources.cloneDerivedOutputDescriptorAlloc(alloc, descriptor));
        },
        .recompute => if (recomputed.findByKind(.chunk_preview)) |descriptor| {
            try outputs.append(alloc, try search_sources.cloneDerivedOutputDescriptorAlloc(alloc, descriptor));
        },
        .drop => {},
    }

    switch (actions.chunk_embeddings) {
        .reuse => if (current.findByKind(.chunk_embeddings)) |descriptor| {
            try outputs.append(alloc, try search_sources.cloneDerivedOutputDescriptorAlloc(alloc, descriptor));
        },
        .recompute => if (recomputed.findByKind(.chunk_embeddings)) |descriptor| {
            try outputs.append(alloc, try search_sources.cloneDerivedOutputDescriptorAlloc(alloc, descriptor));
        },
        .drop => {},
    }

    switch (actions.rerank_terms) {
        .reuse => if (current.findByKind(.rerank_terms)) |descriptor| {
            try outputs.append(alloc, try search_sources.cloneDerivedOutputDescriptorAlloc(alloc, descriptor));
        },
        .recompute => if (recomputed.findByKind(.rerank_terms)) |descriptor| {
            try outputs.append(alloc, try search_sources.cloneDerivedOutputDescriptorAlloc(alloc, descriptor));
        },
        .drop => {},
    }

    return .{ .items = if (outputs.items.len == 0) null else try outputs.toOwnedSlice(alloc) };
}

fn buildMutationSegmentAlloc(alloc: Allocator, records: []const wal_mod.Record) ![]u8 {
    const entries = try allocMutationEntriesFromRecords(alloc, records);
    defer segment_mod.freeEntries(alloc, entries);
    return try segment_mod.encodeAlloc(alloc, entries);
}

fn allocMutationEntriesFromRecords(alloc: Allocator, records: []const wal_mod.Record) ![]segment_mod.Entry {
    const entries = try alloc.alloc(segment_mod.Entry, records.len);
    errdefer alloc.free(entries);

    var initialized: usize = 0;
    errdefer {
        for (entries[0..initialized]) |*entry| entry.deinit(alloc);
    }

    for (records, 0..) |record, idx| {
        var mutation = try api_codec.decodeMutationAlloc(alloc, record.payload);
        defer mutation.deinit(alloc);

        entries[idx] = .{
            .lsn = record.lsn,
            .timestamp_ns = record.timestamp_ns,
            .kind = mutation.kind,
            .doc_id = try alloc.dupe(u8, mutation.doc_id),
            .body = if (mutation.body) |body| try alloc.dupe(u8, body) else null,
        };
        initialized += 1;
    }

    return entries;
}

fn materializeWalDocumentsAlloc(
    self: *Builder,
    namespace: []const u8,
    current_head: u64,
    records: []const wal_mod.Record,
    check_graph_impact: bool,
) !struct {
    base_documents: []query_mod.QueryMaterializedDocument,
    documents: []query_mod.QueryMaterializedDocument,
    mutations: []query_mod.QueryMaterializerMutation,
    graph_changed: bool,
} {
    const mutations = try decodeWalMutationsAlloc(self.alloc, records);

    const base_docs = try loadPublishedDocumentsAlloc(self, namespace, current_head);

    const next_docs = try query_mod.materializeDocumentsOverBaseAlloc(self.alloc, base_docs, mutations);
    errdefer query_mod.freeMaterializedDocuments(self.alloc, next_docs);
    errdefer query_mod.freeMaterializedDocuments(self.alloc, base_docs);
    errdefer freeMaterializerMutations(self.alloc, mutations);

    const graph_changed = if (check_graph_impact)
        try graphProjectionChangedForMutationsAlloc(self.alloc, base_docs, next_docs, mutations)
    else
        false;

    return .{
        .base_documents = base_docs,
        .documents = next_docs,
        .mutations = mutations,
        .graph_changed = graph_changed,
    };
}

fn buildDocumentSegmentAlloc(
    self: *Builder,
    namespace: []const u8,
    current_head: u64,
    records: []const wal_mod.Record,
    check_graph_impact: bool,
) !struct {
    payload: []u8,
    document_count: usize,
    base_documents: []query_mod.QueryMaterializedDocument,
    documents: []query_mod.QueryMaterializedDocument,
    mutations: []query_mod.QueryMaterializerMutation,
    graph_changed: bool,
} {
    const materialized = try materializeWalDocumentsAlloc(self, namespace, current_head, records, check_graph_impact);
    errdefer freeMaterializerMutations(self.alloc, materialized.mutations);
    errdefer query_mod.freeMaterializedDocuments(self.alloc, materialized.documents);
    errdefer query_mod.freeMaterializedDocuments(self.alloc, materialized.base_documents);

    const entries = try allocDocumentSegmentEntries(self.alloc, materialized.documents);
    defer document_segment_mod.freeEntries(self.alloc, entries);

    return .{
        .payload = try document_segment_mod.encodeAlloc(self.alloc, entries),
        .document_count = materialized.documents.len,
        .base_documents = materialized.base_documents,
        .documents = materialized.documents,
        .mutations = materialized.mutations,
        .graph_changed = materialized.graph_changed,
    };
}

fn buildTextSegmentAlloc(alloc: Allocator, docs: []const query_mod.QueryMaterializedDocument, spec: FullTextIndexSpec) ![]u8 {
    var segment = try allocTextSegmentAlloc(alloc, docs, spec);
    defer text_segment_mod.freeSegment(alloc, &segment);
    return try text_segment_mod.encodeAlloc(alloc, segment);
}

fn buildSparseSegmentAlloc(alloc: Allocator, docs: []const query_mod.QueryMaterializedDocument) !struct {
    payload: ?[]u8,
    feature_count: usize,
} {
    return try buildSparseSegmentAllocForSource(alloc, docs, search_sources.defaultPublishedSearchSources().findSparse());
}

fn walEndAsBuiltAtNs(wal_end_lsn: u64) u64 {
    return wal_end_lsn;
}

fn buildSparseSegmentAllocForSource(
    alloc: Allocator,
    docs: []const query_mod.QueryMaterializedDocument,
    sparse_source: ?search_sources.SparseSourceDescriptor,
) !struct {
    payload: ?[]u8,
    feature_count: usize,
} {
    if (sparse_source == null) return .{ .payload = null, .feature_count = 0 };
    var segment = try allocSparseSegmentAlloc(alloc, docs, sparse_source.?);
    defer if (segment) |*value| sparse_segment_mod.freeSegment(alloc, value);
    if (segment) |value| {
        var total_features: usize = 0;
        for (value.docs) |doc| total_features += doc.feature_count;
        return .{
            .payload = try sparse_segment_mod.encodeAlloc(alloc, value),
            .feature_count = total_features,
        };
    }
    return .{ .payload = null, .feature_count = 0 };
}

pub fn buildSparseArtifactRefsForMaterializedDocsAlloc(
    alloc: Allocator,
    artifacts: *artifacts_mod.ArtifactStore,
    current: ?manifest_mod.Manifest,
    before_docs: []const query_mod.QueryMaterializedDocument,
    docs: []const query_mod.QueryMaterializedDocument,
    mutations: []const query_mod.QueryMaterializerMutation,
    published_search_sources: search_sources.PublishedSearchSources,
) ![]manifest_mod.ArtifactRef {
    const sparse_sources = try search_sources.listSparseSourcesAlloc(alloc, published_search_sources);
    defer search_sources.freeSparseSourceDescriptors(alloc, sparse_sources);

    var refs = std.ArrayListUnmanaged(manifest_mod.ArtifactRef).empty;
    errdefer freeArtifactRefs(alloc, refs.items);
    for (sparse_sources) |source| {
        if (current) |manifest| {
            if (!try sparseProjectionChangedForMutationsAlloc(alloc, before_docs, docs, mutations, source)) {
                if (try appendReusedNamedArtifactRefAlloc(alloc, &refs, manifest, .sparse_segment, source.index_name)) continue;
            }
        }
        const built = try buildSparseSegmentAllocForSource(alloc, docs, source);
        defer if (built.payload) |payload| alloc.free(payload);
        if (built.payload) |payload| {
            var artifact = try artifacts.put(payload);
            defer artifact.deinit(alloc);
            try refs.append(alloc, try artifactRefFromMetadataNamedAlloc(
                alloc,
                .sparse_segment,
                source.index_name,
                artifact,
            ));
        }
    }
    return try refs.toOwnedSlice(alloc);
}

fn loadPublishedDocumentsAlloc(self: *Builder, namespace: []const u8, current_head: u64) ![]query_mod.QueryMaterializedDocument {
    if (current_head == 0) return try self.alloc.alloc(query_mod.QueryMaterializedDocument, 0);

    var current = try self.manifests.getAlloc(namespace, current_head);
    defer current.deinit(self.alloc);
    if (findArtifactIndex(current, .document_segment)) |artifact_index| {
        const contents = try self.artifacts.getAlloc(current.artifacts[artifact_index].artifact_id);
        defer self.alloc.free(contents);
        const entries = try document_segment_mod.decodeAlloc(self.alloc, contents);
        defer document_segment_mod.freeEntries(self.alloc, entries);
        const base_docs = try allocMaterializedDocuments(self.alloc, entries);
        errdefer query_mod.freeMaterializedDocuments(self.alloc, base_docs);

        const mutation_index = findArtifactIndex(current, .mutation_segment) orelse return base_docs;
        const mutation_entries = try loadMutationEntriesAlloc(self.alloc, self.artifacts, current.artifacts[mutation_index].artifact_id);
        defer segment_mod.freeEntries(self.alloc, mutation_entries);
        const overlay = try allocQueryMutationsFromSegmentEntries(self.alloc, mutation_entries);
        defer freeQueryMutations(self.alloc, overlay);
        const materialized = try query_mod.materializeDocumentsOverBaseAlloc(self.alloc, base_docs, overlay);
        query_mod.freeMaterializedDocuments(self.alloc, base_docs);
        return materialized;
    }

    return try rebuildPublishedDocumentsFromMutationHistoryAlloc(self, namespace, current_head);
}

fn rebuildPublishedDocumentsFromMutationHistoryAlloc(self: *Builder, namespace: []const u8, current_head: u64) ![]query_mod.QueryMaterializedDocument {
    const versions = try self.manifests.listVersionsAlloc(namespace);
    defer self.alloc.free(versions);

    var mutations = std.ArrayListUnmanaged(query_mod.QueryMaterializerMutation).empty;
    defer freeQueryMutations(self.alloc, mutations.items);

    for (versions) |version| {
        if (version > current_head) break;
        var manifest = try self.manifests.getAlloc(namespace, version);
        defer manifest.deinit(self.alloc);

        for (manifest.artifacts, 0..) |artifact, artifact_index| {
            if (artifact.kind != .mutation_segment) continue;
            const entries = try loadMutationEntriesAlloc(self.alloc, self.artifacts, manifest.artifacts[artifact_index].artifact_id);
            defer segment_mod.freeEntries(self.alloc, entries);
            for (entries) |entry| {
                try mutations.append(self.alloc, .{
                    .lsn = entry.lsn,
                    .timestamp_ns = entry.timestamp_ns,
                    .kind = entry.kind,
                    .doc_id = try self.alloc.dupe(u8, entry.doc_id),
                    .body = if (entry.body) |body| try self.alloc.dupe(u8, body) else null,
                });
            }
        }
    }

    return try query_mod.materializeDocumentsAlloc(self.alloc, mutations.items);
}

fn allocQueryMutationsFromSegmentEntries(alloc: Allocator, entries: []const segment_mod.Entry) ![]query_mod.QueryMaterializerMutation {
    const mutations = try alloc.alloc(query_mod.QueryMaterializerMutation, entries.len);
    errdefer alloc.free(mutations);
    var initialized: usize = 0;
    errdefer freeQueryMutations(alloc, mutations[0..initialized]);

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

fn decodeWalMutationsAlloc(alloc: Allocator, records: []const wal_mod.Record) ![]query_mod.QueryMaterializerMutation {
    const mutations = try alloc.alloc(query_mod.QueryMaterializerMutation, records.len);
    errdefer alloc.free(mutations);

    var initialized: usize = 0;
    errdefer freeMaterializerMutations(alloc, mutations[0..initialized]);

    for (records, 0..) |record, idx| {
        var mutation = try api_codec.decodeMutationAlloc(alloc, record.payload);
        defer mutation.deinit(alloc);
        mutations[idx] = .{
            .lsn = record.lsn,
            .timestamp_ns = record.timestamp_ns,
            .kind = mutation.kind,
            .doc_id = try alloc.dupe(u8, mutation.doc_id),
            .body = if (mutation.body) |body| try alloc.dupe(u8, body) else null,
        };
        initialized += 1;
    }
    return mutations;
}

fn graphProjectionChangedForMutationsAlloc(
    alloc: Allocator,
    before_docs: []const query_mod.QueryMaterializedDocument,
    after_docs: []const query_mod.QueryMaterializedDocument,
    mutations: []const query_mod.QueryMaterializerMutation,
) !bool {
    var seen = std.StringHashMapUnmanaged(void).empty;
    defer seen.deinit(alloc);

    for (mutations) |mutation| {
        const gop = try seen.getOrPut(alloc, mutation.doc_id);
        if (gop.found_existing) continue;
        if (try graphProjectionChangedAlloc(
            alloc,
            findMaterializedDocument(before_docs, mutation.doc_id),
            findMaterializedDocument(after_docs, mutation.doc_id),
        )) {
            return true;
        }
    }
    return false;
}

fn findMaterializedDocument(
    docs: []const query_mod.QueryMaterializedDocument,
    doc_id: []const u8,
) ?query_mod.QueryMaterializedDocument {
    var low: usize = 0;
    var high: usize = docs.len;
    while (low < high) {
        const mid = low + (high - low) / 2;
        switch (std.mem.order(u8, docs[mid].doc_id, doc_id)) {
            .lt => low = mid + 1,
            .gt => high = mid,
            .eq => return docs[mid],
        }
    }
    return null;
}

fn graphProjectionChangedAlloc(
    alloc: Allocator,
    before_doc: ?query_mod.QueryMaterializedDocument,
    after_doc: ?query_mod.QueryMaterializedDocument,
) !bool {
    if ((before_doc == null) != (after_doc == null)) return true;
    if (before_doc == null and after_doc == null) return false;

    const before = before_doc.?;
    const after = after_doc.?;

    const before_edges = try parseGraphEdgesAlloc(alloc, before.body);
    defer freeParsedGraphEdges(alloc, before_edges);
    const after_edges = try parseGraphEdgesAlloc(alloc, after.body);
    defer freeParsedGraphEdges(alloc, after_edges);

    sortParsedGraphEdges(before_edges);
    sortParsedGraphEdges(after_edges);
    if (before_edges.len != after_edges.len) return true;
    for (before_edges, after_edges) |lhs, rhs| {
        if (!std.mem.eql(u8, lhs.target, rhs.target)) return true;
        if (!std.mem.eql(u8, lhs.edge_type, rhs.edge_type)) return true;
        if (lhs.weight != rhs.weight) return true;
    }
    return false;
}

fn vectorProjectionChangedForMutationsAlloc(
    alloc: Allocator,
    before_docs: []const query_mod.QueryMaterializedDocument,
    after_docs: []const query_mod.QueryMaterializedDocument,
    mutations: []const query_mod.QueryMaterializerMutation,
    source: search_sources.VectorSourceDescriptor,
) !bool {
    var seen = std.StringHashMapUnmanaged(void).empty;
    defer seen.deinit(alloc);

    for (mutations) |mutation| {
        const gop = try seen.getOrPut(alloc, mutation.doc_id);
        if (gop.found_existing) continue;
        if (try vectorProjectionChangedAlloc(
            alloc,
            findMaterializedDocument(before_docs, mutation.doc_id),
            findMaterializedDocument(after_docs, mutation.doc_id),
            source,
        )) {
            return true;
        }
    }
    return false;
}

fn sparseProjectionChangedForMutationsAlloc(
    alloc: Allocator,
    before_docs: []const query_mod.QueryMaterializedDocument,
    after_docs: []const query_mod.QueryMaterializedDocument,
    mutations: []const query_mod.QueryMaterializerMutation,
    source: search_sources.SparseSourceDescriptor,
) !bool {
    var seen = std.StringHashMapUnmanaged(void).empty;
    defer seen.deinit(alloc);

    for (mutations) |mutation| {
        const gop = try seen.getOrPut(alloc, mutation.doc_id);
        if (gop.found_existing) continue;
        if (try sparseProjectionChangedAlloc(
            alloc,
            findMaterializedDocument(before_docs, mutation.doc_id),
            findMaterializedDocument(after_docs, mutation.doc_id),
            source,
        )) {
            return true;
        }
    }
    return false;
}

fn textProjectionChangedForMutationsAlloc(
    alloc: Allocator,
    before_docs: []const query_mod.QueryMaterializedDocument,
    after_docs: []const query_mod.QueryMaterializedDocument,
    mutations: []const query_mod.QueryMaterializerMutation,
    spec: FullTextIndexSpec,
) !bool {
    var seen = std.StringHashMapUnmanaged(void).empty;
    defer seen.deinit(alloc);

    for (mutations) |mutation| {
        const gop = try seen.getOrPut(alloc, mutation.doc_id);
        if (gop.found_existing) continue;
        if (try textProjectionChangedAlloc(
            alloc,
            findMaterializedDocument(before_docs, mutation.doc_id),
            findMaterializedDocument(after_docs, mutation.doc_id),
            spec,
        )) {
            return true;
        }
    }
    return false;
}

fn vectorProjectionChangedAlloc(
    alloc: Allocator,
    before_doc: ?query_mod.QueryMaterializedDocument,
    after_doc: ?query_mod.QueryMaterializedDocument,
    source: search_sources.VectorSourceDescriptor,
) !bool {
    if ((before_doc == null) != (after_doc == null)) return true;
    if (before_doc == null and after_doc == null) return false;

    var before_projection = try document_projection.parseAlloc(alloc, before_doc.?.body);
    defer before_projection.deinit(alloc);
    var after_projection = try document_projection.parseAlloc(alloc, after_doc.?.body);
    defer after_projection.deinit(alloc);

    const before = search_sources.selectVectorSource(&before_projection, source);
    const after = search_sources.selectVectorSource(&after_projection, source);
    return !vectorSourcesEqual(before, after);
}

fn vectorSourcesEqual(
    lhs: document_projection.VectorSource,
    rhs: document_projection.VectorSource,
) bool {
    if (std.meta.activeTag(lhs) != std.meta.activeTag(rhs)) return false;
    return switch (lhs) {
        .none => true,
        .top_level => |lhs_vec| blk: {
            const rhs_vec = rhs.top_level;
            if (lhs_vec.len != rhs_vec.len) break :blk false;
            for (lhs_vec, rhs_vec) |l, r| {
                if (l != r) break :blk false;
            }
            break :blk true;
        },
        .chunk_embeddings => |lhs_chunks| blk: {
            const rhs_chunks = rhs.chunk_embeddings;
            if (lhs_chunks.len != rhs_chunks.len) break :blk false;
            for (lhs_chunks, rhs_chunks) |lhs_chunk, rhs_chunk| {
                if (!std.mem.eql(u8, lhs_chunk.chunk, rhs_chunk.chunk)) break :blk false;
                if (lhs_chunk.embedding.len != rhs_chunk.embedding.len) break :blk false;
                for (lhs_chunk.embedding, rhs_chunk.embedding) |l, r| {
                    if (l != r) break :blk false;
                }
            }
            break :blk true;
        },
    };
}

const NormalizedSparseWeight = struct {
    term: []u8,
    weight: f32,
};

fn sparseProjectionChangedAlloc(
    alloc: Allocator,
    before_doc: ?query_mod.QueryMaterializedDocument,
    after_doc: ?query_mod.QueryMaterializedDocument,
    source: search_sources.SparseSourceDescriptor,
) !bool {
    if ((before_doc == null) != (after_doc == null)) return true;
    if (before_doc == null and after_doc == null) return false;

    var before_projection = try document_projection.parseAlloc(alloc, before_doc.?.body);
    defer before_projection.deinit(alloc);
    var after_projection = try document_projection.parseAlloc(alloc, after_doc.?.body);
    defer after_projection.deinit(alloc);

    const before = search_sources.selectSparseSource(&before_projection, source);
    const after = search_sources.selectSparseSource(&after_projection, source);
    return !(try sparseSourcesEqualAlloc(alloc, before, after));
}

fn textProjectionChangedAlloc(
    alloc: Allocator,
    before_doc: ?query_mod.QueryMaterializedDocument,
    after_doc: ?query_mod.QueryMaterializedDocument,
    spec: FullTextIndexSpec,
) !bool {
    if ((before_doc == null) != (after_doc == null)) return true;
    if (before_doc == null and after_doc == null) return false;

    const before_text = try normalizedTextForSpecAlloc(alloc, before_doc.?.body, spec);
    defer alloc.free(before_text);
    const after_text = try normalizedTextForSpecAlloc(alloc, after_doc.?.body, spec);
    defer alloc.free(after_text);
    return !std.mem.eql(u8, before_text, after_text);
}

fn sparseSourcesEqualAlloc(
    alloc: Allocator,
    lhs: ?[]const document_projection.SparseTermWeight,
    rhs: ?[]const document_projection.SparseTermWeight,
) !bool {
    if ((lhs == null) != (rhs == null)) return false;
    if (lhs == null and rhs == null) return true;

    const lhs_norm = try normalizeSparseWeightsAlloc(alloc, lhs.?);
    defer freeNormalizedSparseWeights(alloc, lhs_norm);
    const rhs_norm = try normalizeSparseWeightsAlloc(alloc, rhs.?);
    defer freeNormalizedSparseWeights(alloc, rhs_norm);

    if (lhs_norm.len != rhs_norm.len) return false;
    for (lhs_norm, rhs_norm) |l, r| {
        if (!std.mem.eql(u8, l.term, r.term)) return false;
        if (l.weight != r.weight) return false;
    }
    return true;
}

fn normalizeSparseWeightsAlloc(
    alloc: Allocator,
    weights: []const document_projection.SparseTermWeight,
) ![]NormalizedSparseWeight {
    const out = try alloc.alloc(NormalizedSparseWeight, weights.len);
    errdefer alloc.free(out);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |item| alloc.free(item.term);
    }

    for (weights, 0..) |weight, idx| {
        out[idx] = .{
            .term = try query_mod.indexed_reader.normalizeAlloc(alloc, weight.term),
            .weight = weight.weight,
        };
        initialized = idx + 1;
    }
    std.mem.sort(NormalizedSparseWeight, out, {}, lessNormalizedSparseWeight);
    return out;
}

fn normalizedTextForSpecAlloc(
    alloc: Allocator,
    raw_doc: []const u8,
    spec: FullTextIndexSpec,
) ![]u8 {
    var projection = try document_projection.parseAlloc(alloc, raw_doc);
    defer projection.deinit(alloc);
    const source_text = try selectTextSourceAlloc(alloc, raw_doc, &projection, spec);
    defer alloc.free(source_text);
    return try query_mod.indexed_reader.normalizeAlloc(alloc, source_text);
}

fn freeNormalizedSparseWeights(alloc: Allocator, weights: []NormalizedSparseWeight) void {
    for (weights) |weight| alloc.free(weight.term);
    alloc.free(weights);
}

fn lessNormalizedSparseWeight(_: void, lhs: NormalizedSparseWeight, rhs: NormalizedSparseWeight) bool {
    const term_order = std.mem.order(u8, lhs.term, rhs.term);
    if (term_order != .eq) return term_order == .lt;
    return lhs.weight < rhs.weight;
}

fn allocDocumentSegmentEntries(alloc: Allocator, docs: []const query_mod.QueryMaterializedDocument) ![]document_segment_mod.Entry {
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

pub fn findArtifactIndex(manifest: manifest_mod.Manifest, kind: manifest_mod.ArtifactKind) ?usize {
    for (manifest.artifacts, 0..) |artifact, idx| {
        if (artifact.kind == kind) return idx;
    }
    return null;
}

pub fn findNamedArtifactIndex(manifest: manifest_mod.Manifest, kind: manifest_mod.ArtifactKind, name: []const u8) ?usize {
    var unnamed_match: ?usize = null;
    for (manifest.artifacts, 0..) |artifact, idx| {
        if (artifact.kind != kind) continue;
        if (artifact.name.len == 0) {
            if (unnamed_match == null) unnamed_match = idx;
            continue;
        }
        if (std.mem.eql(u8, artifact.name, name)) return idx;
    }
    return unnamed_match;
}

pub fn countArtifactsByKind(manifest: manifest_mod.Manifest, kind: manifest_mod.ArtifactKind) usize {
    var count: usize = 0;
    for (manifest.artifacts) |artifact| {
        if (artifact.kind == kind) count += 1;
    }
    return count;
}

pub const VectorArtifactInfo = struct {
    metric: shared_vector.DistanceMetric = .cosine,
    cluster_count: usize = 0,
    base_probe_count: u32 = 2,
    shortlist_multiplier: u32 = 2,
    cluster_imbalance: f32 = 0,
    distance_span_max: f32 = 0,
};

pub fn readVectorArtifactInfoAlloc(
    alloc: Allocator,
    artifacts: *artifacts_mod.ArtifactStore,
    artifact: manifest_mod.ArtifactRef,
) !VectorArtifactInfo {
    const header_bytes = try artifacts.getRangeAlloc(artifact.artifact_id, 0, vector_segment_mod.header_len);
    defer alloc.free(header_bytes);
    const header = try vector_segment_mod.decodeHeader(header_bytes);
    if (header.cluster_count == 0) {
        return .{
            .metric = header.metric,
            .cluster_count = 0,
            .base_probe_count = header.base_probe_count,
            .shortlist_multiplier = header.shortlist_multiplier,
        };
    }
    const table_len = vector_segment_mod.clusterRecordLen(header.dims) * @as(usize, @intCast(header.cluster_count));
    const table_bytes = try artifacts.getRangeAlloc(artifact.artifact_id, vector_segment_mod.header_len, table_len);
    defer alloc.free(table_bytes);
    const clusters = try vector_segment_mod.decodeClusterTableAlloc(alloc, header.dims, header.cluster_count, table_bytes);
    defer {
        for (clusters) |*cluster| cluster.deinit(alloc);
        alloc.free(clusters);
    }

    var total_entries: usize = 0;
    var min_entries: usize = std.math.maxInt(usize);
    var max_entries: usize = 0;
    var max_span: f32 = 0;
    for (clusters) |cluster| {
        const count: usize = @intCast(cluster.entry_count);
        total_entries += count;
        min_entries = @min(min_entries, count);
        max_entries = @max(max_entries, count);
        max_span = @max(max_span, cluster.routing_distance_max - cluster.routing_distance_min);
    }
    const avg_entries = if (clusters.len == 0) 0 else @as(f32, @floatFromInt(total_entries)) / @as(f32, @floatFromInt(clusters.len));
    const imbalance = if (avg_entries <= 0 or min_entries == std.math.maxInt(usize)) 0 else @as(f32, @floatFromInt(max_entries - min_entries)) / avg_entries;

    return .{
        .metric = header.metric,
        .cluster_count = header.cluster_count,
        .base_probe_count = header.base_probe_count,
        .shortlist_multiplier = header.shortlist_multiplier,
        .cluster_imbalance = imbalance,
        .distance_span_max = max_span,
    };
}

pub fn adaptiveVectorBuildPolicy(info: VectorArtifactInfo, document_count: usize) vector_index.BuildPolicy {
    return adaptiveVectorBuildPolicyWithThresholds(info, document_count, 0.9, 1.25);
}

pub fn adaptiveVectorBuildPolicyForPolicy(
    info: VectorArtifactInfo,
    document_count: usize,
    policy: catalog_types.NamespacePolicy,
) vector_index.BuildPolicy {
    return adaptiveVectorBuildPolicyWithThresholds(
        info,
        document_count,
        policy.vector_compaction_max_cluster_imbalance,
        policy.vector_compaction_max_distance_span,
    );
}

pub fn vectorBuildPolicyChanges(policy: vector_index.BuildPolicy) bool {
    return policy.target_cluster_count != null or policy.base_probe_count != null or policy.shortlist_multiplier != null;
}

pub const VectorBuildPolicyDelta = struct {
    cluster_count_delta: usize = 0,
    base_probe_delta: u32 = 0,
    shortlist_multiplier_delta: u32 = 0,
};

pub fn vectorBuildPolicyDelta(
    info: VectorArtifactInfo,
    policy: vector_index.BuildPolicy,
) VectorBuildPolicyDelta {
    return .{
        .cluster_count_delta = if (policy.target_cluster_count) |value|
            if (value > info.cluster_count) value - info.cluster_count else info.cluster_count - value
        else
            0,
        .base_probe_delta = if (policy.base_probe_count) |value|
            if (value > info.base_probe_count) value - info.base_probe_count else info.base_probe_count - value
        else
            0,
        .shortlist_multiplier_delta = if (policy.shortlist_multiplier) |value|
            if (value > info.shortlist_multiplier) value - info.shortlist_multiplier else info.shortlist_multiplier - value
        else
            0,
    };
}

pub fn adaptiveVectorBuildPolicyWithThresholds(
    info: VectorArtifactInfo,
    document_count: usize,
    max_cluster_imbalance: f32,
    max_distance_span: f32,
) vector_index.BuildPolicy {
    if (info.cluster_count == 0 or document_count <= 1) return .{};

    var target = info.cluster_count;
    var probe_count = info.base_probe_count;
    var shortlist_multiplier = info.shortlist_multiplier;
    if (info.cluster_imbalance > max_cluster_imbalance or info.distance_span_max > max_distance_span) {
        target = @min(document_count, @max(info.cluster_count + @max(@as(usize, 1), info.cluster_count / 2), info.cluster_count + 1));
        probe_count += 1;
        shortlist_multiplier += 1;
    } else if (info.cluster_imbalance < max_cluster_imbalance * 0.3 and info.distance_span_max < max_distance_span * 0.3 and info.cluster_count > 1) {
        target = @max(@as(usize, 1), info.cluster_count - @max(@as(usize, 1), info.cluster_count / 4));
        target = @min(target, document_count);
        probe_count = @max(@as(u32, 1), probe_count - 1);
        shortlist_multiplier = @max(@as(u32, 2), shortlist_multiplier - 1);
    }
    if (target == info.cluster_count and probe_count == info.base_probe_count and shortlist_multiplier == info.shortlist_multiplier) return .{};
    return .{
        .target_cluster_count = target,
        .base_probe_count = probe_count,
        .shortlist_multiplier = shortlist_multiplier,
    };
}

fn currentDocumentBaseVersion(manifest: manifest_mod.Manifest) u64 {
    if (manifest.stats.document_base_version != 0) return manifest.stats.document_base_version;
    if (findArtifactIndex(manifest, .document_segment) != null) return manifest.version;
    return 0;
}

fn shouldInlineDocumentRebase(
    current: manifest_mod.Manifest,
    next_version: u64,
    policy: catalog_types.NamespacePolicy,
) bool {
    if (!policy.compaction_enabled or policy.compaction_trigger_version_count == 0) return false;
    if (findArtifactIndex(current, .mutation_segment) == null) return false;
    const base_version = currentDocumentBaseVersion(current);
    if (base_version == 0) return false;
    const next_lineage_versions = (next_version - base_version) + 1;
    return next_lineage_versions > policy.compaction_trigger_version_count;
}

pub fn cloneArtifactRefAlloc(alloc: Allocator, artifact: manifest_mod.ArtifactRef) !manifest_mod.ArtifactRef {
    return .{
        .kind = artifact.kind,
        .name = if (artifact.name.len == 0) &.{} else try alloc.dupe(u8, artifact.name),
        .artifact_id = try alloc.dupe(u8, artifact.artifact_id),
        .byte_len = artifact.byte_len,
        .checksum = try alloc.dupe(u8, artifact.checksum),
    };
}

fn artifactRefFromMetadataAlloc(
    alloc: Allocator,
    kind: manifest_mod.ArtifactKind,
    artifact: artifacts_mod.ArtifactMetadata,
) !manifest_mod.ArtifactRef {
    return try artifactRefFromMetadataNamedAlloc(alloc, kind, null, artifact);
}

fn artifactRefFromMetadataNamedAlloc(
    alloc: Allocator,
    kind: manifest_mod.ArtifactKind,
    name: ?[]const u8,
    artifact: artifacts_mod.ArtifactMetadata,
) !manifest_mod.ArtifactRef {
    return .{
        .kind = kind,
        .name = if (name) |value| try alloc.dupe(u8, value) else &.{},
        .artifact_id = try alloc.dupe(u8, artifact.artifact_id),
        .byte_len = artifact.byte_len,
        .checksum = try alloc.dupe(u8, artifact.checksum),
    };
}

pub fn freeArtifactRef(alloc: Allocator, artifact: manifest_mod.ArtifactRef) void {
    if (artifact.name.len > 0) alloc.free(artifact.name);
    alloc.free(artifact.artifact_id);
    alloc.free(artifact.checksum);
}

pub fn freeArtifactRefs(alloc: Allocator, artifacts: []const manifest_mod.ArtifactRef) void {
    for (artifacts) |artifact| freeArtifactRef(alloc, artifact);
    alloc.free(artifacts);
}

pub fn freeOwnedStrings(alloc: Allocator, items: []const []u8) void {
    for (items) |item| alloc.free(item);
    alloc.free(items);
}

fn activeTextIndexNameOrNull(
    alloc: Allocator,
    text_index_specs: []const FullTextIndexSpec,
    table_definition: publication_plan.TableDefinitionSnapshot,
) !?[]u8 {
    if (text_index_specs.len == 0) return null;
    if (try full_text_indexes.selectActiveFullTextIndexNameAlloc(
        alloc,
        table_definition.schema_json,
        table_definition.read_schema_json,
        table_definition.indexes_json,
    )) |active| return active;
    return try alloc.dupe(u8, text_index_specs[0].name);
}

fn collectNamedArtifactNamesAlloc(
    alloc: Allocator,
    refs: []const manifest_mod.ArtifactRef,
) ![]const []u8 {
    const names = try alloc.alloc([]u8, refs.len);
    errdefer alloc.free(names);
    for (refs, 0..) |ref, idx| {
        names[idx] = if (ref.name.len == 0) try alloc.dupe(u8, "") else try alloc.dupe(u8, ref.name);
    }
    return names;
}

pub fn buildPublishedSearchSourcesForManifestAlloc(
    alloc: Allocator,
    text_index_specs: []const FullTextIndexSpec,
    vector_refs: []const manifest_mod.ArtifactRef,
    sparse_refs: []const manifest_mod.ArtifactRef,
    table_definition: publication_plan.TableDefinitionSnapshot,
) !search_sources.PublishedSearchSources {
    const active_text_index_name = try activeTextIndexNameOrNull(alloc, text_index_specs, table_definition);
    defer if (active_text_index_name) |name| alloc.free(name);
    const text_index_names = try alloc.alloc([]u8, text_index_specs.len);
    defer alloc.free(text_index_names);
    for (text_index_specs, 0..) |spec, idx| text_index_names[idx] = spec.name;
    const vector_index_names = try collectNamedArtifactNamesAlloc(alloc, vector_refs);
    defer freeOwnedStrings(alloc, vector_index_names);
    const sparse_index_names = try collectNamedArtifactNamesAlloc(alloc, sparse_refs);
    defer freeOwnedStrings(alloc, sparse_index_names);
    return try search_sources.publishedSearchSourcesForDefinitionListsAlloc(
        alloc,
        active_text_index_name,
        text_index_names,
        vector_index_names,
        sparse_index_names,
    );
}

pub fn resolvePublishedTextIndexSpecsAlloc(
    alloc: Allocator,
    table_definition: publication_plan.TableDefinitionSnapshot,
    planned_actions: []const publication_plan.FullTextIndexAction,
) ![]FullTextIndexSpec {
    var specs = try full_text_indexes.listFullTextIndexSpecsAlloc(alloc, table_definition.indexes_json);
    errdefer full_text_indexes.freeFullTextIndexSpecs(alloc, specs);
    if (planned_actions.len > 0) {
        var filtered = std.ArrayListUnmanaged(FullTextIndexSpec).empty;
        errdefer {
            for (filtered.items) |*spec| spec.deinit(alloc);
            filtered.deinit(alloc);
        }
        for (specs) |spec| {
            if (fullTextActionForName(planned_actions, spec.name, .reuse) == .drop) continue;
            try filtered.append(alloc, .{
                .name = try alloc.dupe(u8, spec.name),
                .config_json = try alloc.dupe(u8, spec.config_json),
                .source_artifact_name = if (spec.source_artifact_name) |name| try alloc.dupe(u8, name) else null,
                .source_mode = spec.source_mode,
                .chunked_sources = try full_text_indexes.cloneChunkedFullTextSourcesAlloc(alloc, spec.chunked_sources),
            });
        }
        full_text_indexes.freeFullTextIndexSpecs(alloc, specs);
        specs = try filtered.toOwnedSlice(alloc);
    }
    const chunked_sources = try full_text_indexes.listChunkedFullTextSourcesAlloc(alloc, table_definition.indexes_json);
    defer full_text_indexes.freeChunkedFullTextSources(alloc, chunked_sources);
    const chunk_full_text_source_name = if (chunked_sources.len > 0) search_sources.default_chunk_preview_output_name else null;
    if (specs.len == 0) {
        const fallback_specs = try alloc.alloc(FullTextIndexSpec, 1);
        errdefer alloc.free(fallback_specs);
        fallback_specs[0] = .{
            .name = try alloc.dupe(u8, search_sources.default_full_text_index_name),
            .config_json = try alloc.dupe(u8, "{\"type\":\"full_text\"}"),
            .source_artifact_name = if (chunk_full_text_source_name) |name| try alloc.dupe(u8, name) else null,
            .source_mode = if (chunk_full_text_source_name != null) .document_plus_artifact else .document,
            .chunked_sources = try full_text_indexes.cloneChunkedFullTextSourcesAlloc(alloc, chunked_sources),
        };
        return fallback_specs;
    }
    if (chunk_full_text_source_name) |name| {
        for (specs) |*spec| {
            if (spec.source_artifact_name != null) continue;
            spec.source_artifact_name = try alloc.dupe(u8, name);
            spec.source_mode = .document_plus_artifact;
            spec.chunked_sources = try full_text_indexes.cloneChunkedFullTextSourcesAlloc(alloc, chunked_sources);
        }
    }
    if (specs.len <= 1) return specs;

    const active = try full_text_indexes.selectActiveFullTextIndexNameAlloc(
        alloc,
        table_definition.schema_json,
        table_definition.read_schema_json,
        table_definition.indexes_json,
    );
    defer if (active) |name| alloc.free(name);
    if (active) |active_name| {
        for (specs, 0..) |spec, idx| {
            if (!std.mem.eql(u8, spec.name, active_name)) continue;
            if (idx != 0) std.mem.swap(FullTextIndexSpec, &specs[0], &specs[idx]);
            break;
        }
    }
    return specs;
}

fn fullTextActionForName(
    planned_actions: []const publication_plan.FullTextIndexAction,
    name: []const u8,
    fallback: publication_plan.ArtifactAction,
) publication_plan.ArtifactAction {
    for (planned_actions) |item| {
        if (std.mem.eql(u8, item.name, name)) return item.action;
    }
    return fallback;
}

pub fn buildTextArtifactRefsForMaterializedDocsAlloc(
    alloc: Allocator,
    artifacts: *artifacts_mod.ArtifactStore,
    current: ?manifest_mod.Manifest,
    before_docs: []const query_mod.QueryMaterializedDocument,
    docs: []const query_mod.QueryMaterializedDocument,
    mutations: []const query_mod.QueryMaterializerMutation,
    text_index_specs: []const FullTextIndexSpec,
) ![]manifest_mod.ArtifactRef {
    const refs = try alloc.alloc(manifest_mod.ArtifactRef, text_index_specs.len);
    errdefer alloc.free(refs);
    var initialized: usize = 0;
    errdefer {
        for (refs[0..initialized]) |ref| freeArtifactRef(alloc, ref);
    }

    for (text_index_specs, 0..) |spec, idx| {
        if (current) |manifest| {
            if (!try textProjectionChangedForMutationsAlloc(alloc, before_docs, docs, mutations, spec)) {
                if (try cloneNamedArtifactRefAlloc(alloc, manifest, .text_segment, spec.name)) |artifact| {
                    refs[idx] = artifact;
                    initialized += 1;
                    continue;
                }
            }
        }
        const payload = try buildTextSegmentAlloc(alloc, docs, spec);
        defer alloc.free(payload);
        var artifact = try artifacts.put(payload);
        defer artifact.deinit(alloc);
        refs[idx] = try artifactRefFromMetadataNamedAlloc(alloc, .text_segment, spec.name, artifact);
        initialized += 1;
    }
    return refs;
}

fn buildTextArtifactRefsForRepublishAlloc(
    alloc: Allocator,
    artifacts: *artifacts_mod.ArtifactStore,
    current: manifest_mod.Manifest,
    docs: []const query_mod.QueryMaterializedDocument,
    text_index_specs: []const FullTextIndexSpec,
    planned_actions: []const publication_plan.FullTextIndexAction,
    fallback_action: publication_plan.ArtifactAction,
) ![]manifest_mod.ArtifactRef {
    const refs = try alloc.alloc(manifest_mod.ArtifactRef, text_index_specs.len);
    errdefer alloc.free(refs);
    var initialized: usize = 0;
    errdefer {
        for (refs[0..initialized]) |ref| freeArtifactRef(alloc, ref);
    }

    for (text_index_specs, 0..) |spec, idx| {
        const action = fullTextActionForName(planned_actions, spec.name, fallback_action);
        if (action == .reuse) {
            if (findNamedArtifactIndex(current, .text_segment, spec.name)) |artifact_index| {
                refs[idx] = try cloneArtifactRefAlloc(alloc, current.artifacts[artifact_index]);
                if (refs[idx].name.len == 0) refs[idx].name = try alloc.dupe(u8, spec.name);
                initialized += 1;
                continue;
            }
        }
        const payload = try buildTextSegmentAlloc(alloc, docs, spec);
        defer alloc.free(payload);
        var artifact = try artifacts.put(payload);
        defer artifact.deinit(alloc);
        refs[idx] = try artifactRefFromMetadataNamedAlloc(alloc, .text_segment, spec.name, artifact);
        initialized += 1;
    }
    return refs;
}

fn collectTextIndexNamesAlloc(
    alloc: Allocator,
    text_refs: []const manifest_mod.ArtifactRef,
) ![][]u8 {
    const names = try alloc.alloc([]u8, text_refs.len);
    errdefer alloc.free(names);
    var initialized: usize = 0;
    errdefer {
        for (names[0..initialized]) |name| alloc.free(name);
    }
    for (text_refs, 0..) |text_ref, idx| {
        names[idx] = try alloc.dupe(u8, text_ref.name);
        initialized += 1;
    }
    return names;
}

pub fn listGraphIndexNamesAlloc(alloc: Allocator, indexes_json: []const u8) ![][]u8 {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, if (indexes_json.len == 0) "{}" else indexes_json, .{});
    defer parsed.deinit();
    const object = switch (parsed.value) {
        .object => |value| value,
        else => return error.InvalidTableIndexMetadata,
    };

    var names = std.ArrayListUnmanaged([]u8).empty;
    errdefer {
        for (names.items) |name| alloc.free(name);
        names.deinit(alloc);
    }

    var it = object.iterator();
    while (it.next()) |entry| {
        if (!isGraphIndexValue(entry.value_ptr.*)) continue;
        try names.append(alloc, try alloc.dupe(u8, entry.key_ptr.*));
    }
    return try names.toOwnedSlice(alloc);
}

fn isGraphIndexValue(value: std.json.Value) bool {
    const object = switch (value) {
        .object => |map| map,
        else => return false,
    };
    const type_value = object.get("type") orelse return false;
    return type_value == .string and std.mem.eql(u8, type_value.string, "graph");
}

fn textIndexSpecsFromNamesAlloc(
    alloc: Allocator,
    names: []const []u8,
) ![]FullTextIndexSpec {
    const specs = try alloc.alloc(FullTextIndexSpec, names.len);
    errdefer alloc.free(specs);
    var initialized: usize = 0;
    errdefer {
        for (specs[0..initialized]) |*spec| spec.deinit(alloc);
    }
    for (names, 0..) |name, idx| {
        specs[idx] = .{
            .name = try alloc.dupe(u8, name),
            .config_json = try alloc.dupe(u8, "{\"type\":\"full_text\"}"),
            .source_artifact_name = null,
            .source_mode = .document,
        };
        initialized += 1;
    }
    return specs;
}

fn ensurePublishedDocumentsAlloc(
    self: *Builder,
    namespace: []const u8,
    current_head: u64,
    docs_cache: *?[]query_mod.QueryMaterializedDocument,
) ![]query_mod.QueryMaterializedDocument {
    if (docs_cache.*) |docs| return docs;
    const docs = try loadPublishedDocumentsAlloc(self, namespace, current_head);
    docs_cache.* = docs;
    return docs;
}

fn loadMutationEntriesAlloc(alloc: Allocator, artifacts: *artifacts_mod.ArtifactStore, artifact_id: []const u8) ![]segment_mod.Entry {
    const contents = try artifacts.getAlloc(artifact_id);
    defer alloc.free(contents);
    return try segment_mod.decodeAlloc(alloc, contents);
}

fn mergeManifestMutationEntriesWithRecordsAlloc(
    alloc: Allocator,
    artifacts: *artifacts_mod.ArtifactStore,
    manifest: manifest_mod.Manifest,
    records: []const wal_mod.Record,
) ![]segment_mod.Entry {
    const appended = try allocMutationEntriesFromRecords(alloc, records);
    errdefer segment_mod.freeEntries(alloc, appended);

    const mutation_index = findArtifactIndex(manifest, .mutation_segment) orelse return appended;
    const existing = try loadMutationEntriesAlloc(alloc, artifacts, manifest.artifacts[mutation_index].artifact_id);
    defer segment_mod.freeEntries(alloc, existing);

    const merged = try alloc.alloc(segment_mod.Entry, existing.len + appended.len);
    errdefer alloc.free(merged);
    var initialized: usize = 0;
    errdefer {
        for (merged[0..initialized]) |*entry| entry.deinit(alloc);
    }

    for (existing, 0..) |entry, idx| {
        merged[idx] = .{
            .lsn = entry.lsn,
            .timestamp_ns = entry.timestamp_ns,
            .kind = entry.kind,
            .doc_id = try alloc.dupe(u8, entry.doc_id),
            .body = if (entry.body) |body| try alloc.dupe(u8, body) else null,
        };
        initialized += 1;
    }
    for (appended, 0..) |entry, idx| {
        merged[existing.len + idx] = .{
            .lsn = entry.lsn,
            .timestamp_ns = entry.timestamp_ns,
            .kind = entry.kind,
            .doc_id = try alloc.dupe(u8, entry.doc_id),
            .body = if (entry.body) |body| try alloc.dupe(u8, body) else null,
        };
        initialized += 1;
    }
    segment_mod.freeEntries(alloc, appended);
    return merged;
}

fn freeMaterializerMutations(alloc: Allocator, mutations: []query_mod.QueryMaterializerMutation) void {
    for (mutations) |mutation| {
        alloc.free(mutation.doc_id);
        if (mutation.body) |body| alloc.free(body);
    }
    alloc.free(mutations);
}

fn freeQueryMutations(alloc: Allocator, mutations: []query_mod.QueryMaterializerMutation) void {
    freeMaterializerMutations(alloc, mutations);
}

fn allocTextSegmentAlloc(alloc: Allocator, docs: []const query_mod.QueryMaterializedDocument, spec: FullTextIndexSpec) !text_segment_mod.Segment {
    const doc_entries = try alloc.alloc(text_segment_mod.DocumentEntry, docs.len);
    errdefer alloc.free(doc_entries);

    var docs_initialized: usize = 0;
    errdefer {
        for (doc_entries[0..docs_initialized]) |*doc| doc.deinit(alloc);
    }

    var term_map = std.StringArrayHashMapUnmanaged(std.ArrayListUnmanaged(text_segment_mod.Posting)).empty;
    defer {
        for (term_map.values()) |*postings| postings.deinit(alloc);
        term_map.deinit(alloc);
    }

    for (docs, 0..) |doc, doc_index| {
        var projection = try document_projection.parseAlloc(alloc, doc.body);
        defer projection.deinit(alloc);
        const source_text = try selectTextSourceAlloc(alloc, doc.body, &projection, spec);
        defer alloc.free(source_text);
        const normalized_text = try @import("../query/indexed_reader.zig").normalizeAlloc(alloc, source_text);
        errdefer alloc.free(normalized_text);

        var token_count: u32 = 0;
        var per_doc = std.StringArrayHashMapUnmanaged(u32).empty;
        defer per_doc.deinit(alloc);

        var token_iter = std.mem.tokenizeAny(u8, normalized_text, " ");
        while (token_iter.next()) |token| {
            token_count += 1;
            const gop = try per_doc.getOrPut(alloc, token);
            if (!gop.found_existing) gop.value_ptr.* = 0;
            gop.value_ptr.* += 1;
        }

        for (per_doc.keys(), per_doc.values()) |term, freq| {
            const gop = try term_map.getOrPut(alloc, term);
            if (!gop.found_existing) gop.value_ptr.* = .empty;
            try gop.value_ptr.append(alloc, .{
                .doc_index = @intCast(doc_index),
                .term_freq = freq,
            });
        }

        doc_entries[doc_index] = .{
            .doc_id = try alloc.dupe(u8, doc.doc_id),
            .normalized_text = normalized_text,
            .token_count = token_count,
        };
        docs_initialized += 1;
    }

    const term_entries = try alloc.alloc(text_segment_mod.TermEntry, term_map.count());
    errdefer alloc.free(term_entries);
    var terms_initialized: usize = 0;
    errdefer {
        for (term_entries[0..terms_initialized]) |*term| term.deinit(alloc);
    }

    for (term_map.keys(), 0..) |term, idx| {
        term_entries[idx] = .{
            .term = try alloc.dupe(u8, term),
            .postings = try term_map.values()[idx].toOwnedSlice(alloc),
        };
        terms_initialized += 1;
    }

    std.mem.sort(text_segment_mod.TermEntry, term_entries, {}, lessTermEntry);

    return .{
        .index_name = try alloc.dupe(u8, spec.name),
        .source_name = switch (spec.source_mode) {
            .document => try alloc.dupe(u8, "text"),
            .artifact_only => try alloc.dupe(u8, spec.source_artifact_name orelse "text"),
            .document_plus_artifact => if (spec.source_artifact_name) |name|
                try std.fmt.allocPrint(alloc, "text+{s}", .{name})
            else
                try alloc.dupe(u8, "text"),
        },
        .config_json = try alloc.dupe(u8, spec.config_json),
        .docs = doc_entries,
        .terms = term_entries,
    };
}

fn selectTextSourceAlloc(
    alloc: Allocator,
    raw_doc: []const u8,
    projection: *const document_projection.Projection,
    spec: FullTextIndexSpec,
) ![]u8 {
    if (spec.source_artifact_name) |source_artifact_name| {
        if (std.mem.eql(u8, source_artifact_name, search_sources.default_chunk_preview_output_name)) {
            const chunk_text = if (spec.chunked_sources.len > 0)
                try full_text_indexes.synthesizeChunkedFullTextAlloc(alloc, raw_doc, spec.chunked_sources)
            else if (projection.chunk_preview) |chunks|
                try joinChunksAlloc(alloc, chunks)
            else
                try alloc.dupe(u8, "");
            defer alloc.free(chunk_text);
            return switch (spec.source_mode) {
                .artifact_only => try alloc.dupe(u8, chunk_text),
                .document_plus_artifact => try combineTextSourcesAlloc(alloc, projection.text, chunk_text),
                .document => try alloc.dupe(u8, projection.text),
            };
        }
    }
    if (spec.source_mode == .artifact_only and spec.source_artifact_name != null) {
        return try alloc.dupe(u8, projection.text);
    }
    return try alloc.dupe(u8, projection.text);
}

fn joinChunksAlloc(alloc: Allocator, chunks: []const []u8) ![]u8 {
    if (chunks.len == 0) return try alloc.dupe(u8, "");
    var total_len: usize = 0;
    for (chunks, 0..) |chunk, idx| {
        total_len += chunk.len;
        if (idx + 1 < chunks.len) total_len += 1;
    }
    const out = try alloc.alloc(u8, total_len);
    var pos: usize = 0;
    for (chunks, 0..) |chunk, idx| {
        @memcpy(out[pos..][0..chunk.len], chunk);
        pos += chunk.len;
        if (idx + 1 < chunks.len) {
            out[pos] = ' ';
            pos += 1;
        }
    }
    return out;
}

fn combineTextSourcesAlloc(alloc: Allocator, primary: []const u8, secondary: []const u8) ![]u8 {
    if (primary.len == 0) return try alloc.dupe(u8, secondary);
    if (secondary.len == 0) return try alloc.dupe(u8, primary);
    const out = try alloc.alloc(u8, primary.len + 1 + secondary.len);
    @memcpy(out[0..primary.len], primary);
    out[primary.len] = '\n';
    @memcpy(out[primary.len + 1 ..][0..secondary.len], secondary);
    return out;
}

fn buildVectorSegmentAlloc(
    alloc: Allocator,
    fallback_metric: shared_vector.DistanceMetric,
    docs: []const query_mod.QueryMaterializedDocument,
    policy: ?vector_index.BuildPolicy,
    vector_source: ?search_sources.VectorSourceDescriptor,
) !struct {
    payload: ?[]u8,
    vector_count: usize,
} {
    if (vector_source == null) return .{ .payload = null, .vector_count = 0 };
    const metric = vector_source.?.distance_metric orelse fallback_metric;
    var dims: ?usize = null;
    var count: usize = 0;
    for (docs) |doc| {
        var projection = try document_projection.parseAlloc(alloc, doc.body);
        defer projection.deinit(alloc);
        switch (search_sources.selectVectorSource(&projection, vector_source.?)) {
            .none => {},
            .top_level => |embedding| {
                if (dims == null) dims = embedding.len else if (dims.? != embedding.len) return error.InconsistentVectorDims;
                count += 1;
            },
            .chunk_embeddings => |chunk_embeddings| {
                for (chunk_embeddings) |chunk_embedding| {
                    if (dims == null) dims = chunk_embedding.embedding.len else if (dims.? != chunk_embedding.embedding.len) return error.InconsistentVectorDims;
                    count += 1;
                }
            },
        }
    }

    if (count == 0) return .{ .payload = null, .vector_count = 0 };

    const entries = try alloc.alloc(vector_segment_mod.Entry, count);
    errdefer alloc.free(entries);
    var initialized: usize = 0;
    errdefer {
        for (entries[0..initialized]) |*entry| entry.deinit(alloc);
    }

    for (docs) |doc| {
        var projection = try document_projection.parseAlloc(alloc, doc.body);
        defer projection.deinit(alloc);
        switch (search_sources.selectVectorSource(&projection, vector_source.?)) {
            .none => continue,
            .chunk_embeddings => |chunk_embeddings| {
                for (chunk_embeddings) |chunk_embedding| {
                    entries[initialized] = .{
                        .doc_id = try alloc.dupe(u8, doc.doc_id),
                        .vector = try alloc.dupe(f32, chunk_embedding.embedding),
                    };
                    initialized += 1;
                }
            },
            .top_level => |embedding| {
                entries[initialized] = .{
                    .doc_id = try alloc.dupe(u8, doc.doc_id),
                    .vector = try alloc.dupe(f32, embedding),
                };
                initialized += 1;
            },
        }
    }

    var segment = if (policy) |value|
        try vector_index.buildClusteredSegmentWithPolicyAlloc(alloc, metric, @intCast(dims.?), entries, value)
    else
        try vector_index.buildClusteredSegmentAlloc(alloc, metric, @intCast(dims.?), entries);
    defer vector_segment_mod.freeSegment(alloc, &segment);
    return .{
        .payload = try vector_segment_mod.encodeAlloc(alloc, segment),
        .vector_count = count,
    };
}

pub fn buildVectorArtifactRefsForMaterializedDocsAlloc(
    alloc: Allocator,
    artifacts: *artifacts_mod.ArtifactStore,
    current: ?manifest_mod.Manifest,
    metric: shared_vector.DistanceMetric,
    before_docs: []const query_mod.QueryMaterializedDocument,
    docs: []const query_mod.QueryMaterializedDocument,
    mutations: []const query_mod.QueryMaterializerMutation,
    policy: ?vector_index.BuildPolicy,
    named_policies: []const NamedVectorBuildPolicy,
    published_search_sources: search_sources.PublishedSearchSources,
) ![]manifest_mod.ArtifactRef {
    const vector_sources = try search_sources.listVectorSourcesAlloc(alloc, published_search_sources);
    defer search_sources.freeVectorSourceDescriptors(alloc, vector_sources);

    var refs = std.ArrayListUnmanaged(manifest_mod.ArtifactRef).empty;
    errdefer freeArtifactRefs(alloc, refs.items);
    for (vector_sources) |source| {
        if (current) |manifest| {
            if (!try vectorProjectionChangedForMutationsAlloc(alloc, before_docs, docs, mutations, source)) {
                if (try appendReusedNamedArtifactRefAlloc(alloc, &refs, manifest, .vector_segment, source.index_name)) continue;
            }
        }
        const built = try buildVectorSegmentAlloc(
            alloc,
            metric,
            docs,
            namedVectorBuildPolicyForName(named_policies, source.index_name, policy),
            source,
        );
        defer if (built.payload) |payload| alloc.free(payload);
        if (built.payload) |payload| {
            var artifact = try artifacts.put(payload);
            defer artifact.deinit(alloc);
            try refs.append(alloc, try artifactRefFromMetadataNamedAlloc(
                alloc,
                .vector_segment,
                source.index_name,
                artifact,
            ));
        }
    }
    return try refs.toOwnedSlice(alloc);
}

fn namedVectorBuildPolicyForName(
    items: []const NamedVectorBuildPolicy,
    name: []const u8,
    fallback: ?vector_index.BuildPolicy,
) ?vector_index.BuildPolicy {
    for (items) |item| {
        if (std.mem.eql(u8, item.index_name, name)) return item.policy;
    }
    return fallback;
}

fn buildSparseArtifactRefsForRepublishAlloc(
    alloc: Allocator,
    artifacts: *artifacts_mod.ArtifactStore,
    current: manifest_mod.Manifest,
    docs: []const query_mod.QueryMaterializedDocument,
    published_search_sources: search_sources.PublishedSearchSources,
    named_actions: []const publication_plan.NamedArtifactAction,
    fallback_action: publication_plan.ArtifactAction,
) ![]manifest_mod.ArtifactRef {
    const sparse_sources = try search_sources.listSparseSourcesAlloc(alloc, published_search_sources);
    defer search_sources.freeSparseSourceDescriptors(alloc, sparse_sources);
    if (fallback_action == .drop and named_actions.len == 0) return try alloc.alloc(manifest_mod.ArtifactRef, 0);
    if (sparse_sources.len == 0) return try alloc.alloc(manifest_mod.ArtifactRef, 0);

    const current_count = countArtifactsByKind(current, .sparse_segment);
    var refs = std.ArrayListUnmanaged(manifest_mod.ArtifactRef).empty;
    errdefer freeArtifactRefs(alloc, refs.items);
    for (sparse_sources) |source| {
        const action = namedArtifactActionForName(named_actions, source.index_name, fallback_action);
        if (action == .drop) continue;
        if (action == .reuse) {
            if (findNamedArtifactIndex(current, .sparse_segment, source.index_name)) |artifact_index| {
                try refs.append(alloc, try cloneArtifactRefAlloc(alloc, current.artifacts[artifact_index]));
                continue;
            }
            if (current_count == 1) {
                if (findArtifactIndex(current, .sparse_segment)) |artifact_index| {
                    var artifact = try cloneArtifactRefAlloc(alloc, current.artifacts[artifact_index]);
                    if (!std.mem.eql(u8, artifact.name, source.index_name)) {
                        if (artifact.name.len > 0) alloc.free(artifact.name);
                        artifact.name = try alloc.dupe(u8, source.index_name);
                    }
                    try refs.append(alloc, artifact);
                    continue;
                }
            }
        }
        const built = try buildSparseSegmentAllocForSource(alloc, docs, source);
        defer if (built.payload) |payload| alloc.free(payload);
        if (built.payload) |payload| {
            var artifact = try artifacts.put(payload);
            defer artifact.deinit(alloc);
            try refs.append(alloc, try artifactRefFromMetadataNamedAlloc(
                alloc,
                .sparse_segment,
                source.index_name,
                artifact,
            ));
        }
    }
    return try refs.toOwnedSlice(alloc);
}

fn buildVectorArtifactRefsForRepublishAlloc(
    alloc: Allocator,
    artifacts: *artifacts_mod.ArtifactStore,
    current: manifest_mod.Manifest,
    metric: shared_vector.DistanceMetric,
    docs: []const query_mod.QueryMaterializedDocument,
    published_search_sources: search_sources.PublishedSearchSources,
    named_actions: []const publication_plan.NamedArtifactAction,
    fallback_action: publication_plan.ArtifactAction,
) ![]manifest_mod.ArtifactRef {
    const vector_sources = try search_sources.listVectorSourcesAlloc(alloc, published_search_sources);
    defer search_sources.freeVectorSourceDescriptors(alloc, vector_sources);
    if (fallback_action == .drop and named_actions.len == 0) return try alloc.alloc(manifest_mod.ArtifactRef, 0);
    if (vector_sources.len == 0) return try alloc.alloc(manifest_mod.ArtifactRef, 0);

    const current_count = countArtifactsByKind(current, .vector_segment);
    var refs = std.ArrayListUnmanaged(manifest_mod.ArtifactRef).empty;
    errdefer freeArtifactRefs(alloc, refs.items);
    for (vector_sources) |source| {
        const action = namedArtifactActionForName(named_actions, source.index_name, fallback_action);
        if (action == .drop) continue;
        if (action == .reuse) {
            if (findNamedArtifactIndex(current, .vector_segment, source.index_name)) |artifact_index| {
                try refs.append(alloc, try cloneArtifactRefAlloc(alloc, current.artifacts[artifact_index]));
                continue;
            }
            if (current_count == 1) {
                if (findArtifactIndex(current, .vector_segment)) |artifact_index| {
                    var artifact = try cloneArtifactRefAlloc(alloc, current.artifacts[artifact_index]);
                    if (!std.mem.eql(u8, artifact.name, source.index_name)) {
                        if (artifact.name.len > 0) alloc.free(artifact.name);
                        artifact.name = try alloc.dupe(u8, source.index_name);
                    }
                    try refs.append(alloc, artifact);
                    continue;
                }
            }
        }
        const built = try buildVectorSegmentAlloc(alloc, metric, docs, null, source);
        defer if (built.payload) |payload| alloc.free(payload);
        if (built.payload) |payload| {
            var artifact = try artifacts.put(payload);
            defer artifact.deinit(alloc);
            try refs.append(alloc, try artifactRefFromMetadataNamedAlloc(
                alloc,
                .vector_segment,
                source.index_name,
                artifact,
            ));
        }
    }
    return try refs.toOwnedSlice(alloc);
}

pub fn buildGraphArtifactRefsForMaterializedDocsAlloc(
    alloc: Allocator,
    artifacts: *artifacts_mod.ArtifactStore,
    current: ?manifest_mod.Manifest,
    before_docs: []const query_mod.QueryMaterializedDocument,
    docs: []const query_mod.QueryMaterializedDocument,
    mutations: []const query_mod.QueryMaterializerMutation,
    graph_index_names: []const []u8,
    include_graph: bool,
) ![]manifest_mod.ArtifactRef {
    if (!include_graph) return try alloc.alloc(manifest_mod.ArtifactRef, 0);

    const changed = try graphProjectionChangedForMutationsAlloc(alloc, before_docs, docs, mutations);
    if (graph_index_names.len == 0) {
        if (current) |manifest| {
            if (!changed) {
                if (findArtifactIndex(manifest, .graph_segment)) |artifact_index| {
                    const refs = try alloc.alloc(manifest_mod.ArtifactRef, 1);
                    errdefer alloc.free(refs);
                    refs[0] = try cloneArtifactRefAlloc(alloc, manifest.artifacts[artifact_index]);
                    return refs;
                }
            }
        }
        const built = try buildGraphSegmentAlloc(alloc, docs, true);
        defer if (built.payload) |payload| alloc.free(payload);
        if (built.payload) |payload| {
            var artifact = try artifacts.put(payload);
            defer artifact.deinit(alloc);
            const refs = try alloc.alloc(manifest_mod.ArtifactRef, 1);
            errdefer alloc.free(refs);
            refs[0] = try artifactRefFromMetadataAlloc(alloc, .graph_segment, artifact);
            return refs;
        }
        return try alloc.alloc(manifest_mod.ArtifactRef, 0);
    }

    if (current) |manifest| {
        if (!changed) {
            var refs = std.ArrayListUnmanaged(manifest_mod.ArtifactRef).empty;
            errdefer freeArtifactRefs(alloc, refs.items);
            for (graph_index_names) |index_name| {
                if (!(try appendReusedNamedArtifactRefAlloc(alloc, &refs, manifest, .graph_segment, index_name))) break;
            } else {
                return try refs.toOwnedSlice(alloc);
            }
            freeArtifactRefs(alloc, refs.items);
        }
    }

    const built = try buildGraphSegmentAlloc(alloc, docs, true);
    defer if (built.payload) |payload| alloc.free(payload);
    if (built.payload) |payload| {
        var artifact = try artifacts.put(payload);
        defer artifact.deinit(alloc);
        const refs = try alloc.alloc(manifest_mod.ArtifactRef, graph_index_names.len);
        errdefer alloc.free(refs);
        var initialized: usize = 0;
        errdefer {
            for (refs[0..initialized]) |ref| freeArtifactRef(alloc, ref);
        }
        for (graph_index_names, 0..) |index_name, idx| {
            refs[idx] = try artifactRefFromMetadataNamedAlloc(alloc, .graph_segment, index_name, artifact);
            initialized += 1;
        }
        return refs;
    }
    return try alloc.alloc(manifest_mod.ArtifactRef, 0);
}

fn buildGraphArtifactRefsForRepublishAlloc(
    alloc: Allocator,
    artifacts: *artifacts_mod.ArtifactStore,
    current: manifest_mod.Manifest,
    docs: []const query_mod.QueryMaterializedDocument,
    graph_index_names: []const []u8,
    action: publication_plan.ArtifactAction,
    include_graph: bool,
) ![]manifest_mod.ArtifactRef {
    if (!include_graph or action == .drop) return try alloc.alloc(manifest_mod.ArtifactRef, 0);

    if (graph_index_names.len == 0) {
        if (action == .reuse) {
            if (findArtifactIndex(current, .graph_segment)) |artifact_index| {
                const refs = try alloc.alloc(manifest_mod.ArtifactRef, 1);
                errdefer alloc.free(refs);
                refs[0] = try cloneArtifactRefAlloc(alloc, current.artifacts[artifact_index]);
                return refs;
            }
        }
        const built = try buildGraphSegmentAlloc(alloc, docs, true);
        defer if (built.payload) |payload| alloc.free(payload);
        if (built.payload) |payload| {
            var artifact = try artifacts.put(payload);
            defer artifact.deinit(alloc);
            const refs = try alloc.alloc(manifest_mod.ArtifactRef, 1);
            errdefer alloc.free(refs);
            refs[0] = try artifactRefFromMetadataAlloc(alloc, .graph_segment, artifact);
            return refs;
        }
        return try alloc.alloc(manifest_mod.ArtifactRef, 0);
    }

    if (action == .reuse) {
        var refs = std.ArrayListUnmanaged(manifest_mod.ArtifactRef).empty;
        errdefer freeArtifactRefs(alloc, refs.items);
        for (graph_index_names) |index_name| {
            if (!(try appendReusedNamedArtifactRefAlloc(alloc, &refs, current, .graph_segment, index_name))) break;
        } else {
            return try refs.toOwnedSlice(alloc);
        }
        freeArtifactRefs(alloc, refs.items);
    }

    const built = try buildGraphSegmentAlloc(alloc, docs, true);
    defer if (built.payload) |payload| alloc.free(payload);
    if (built.payload) |payload| {
        var artifact = try artifacts.put(payload);
        defer artifact.deinit(alloc);
        const refs = try alloc.alloc(manifest_mod.ArtifactRef, graph_index_names.len);
        errdefer alloc.free(refs);
        var initialized: usize = 0;
        errdefer {
            for (refs[0..initialized]) |ref| freeArtifactRef(alloc, ref);
        }
        for (graph_index_names, 0..) |index_name, idx| {
            refs[idx] = try artifactRefFromMetadataNamedAlloc(alloc, .graph_segment, index_name, artifact);
            initialized += 1;
        }
        return refs;
    }
    return try alloc.alloc(manifest_mod.ArtifactRef, 0);
}

fn appendReusedNamedArtifactRefAlloc(
    alloc: Allocator,
    refs: *std.ArrayListUnmanaged(manifest_mod.ArtifactRef),
    current: manifest_mod.Manifest,
    kind: manifest_mod.ArtifactKind,
    name: []const u8,
) !bool {
    if (findNamedArtifactIndex(current, kind, name)) |artifact_index| {
        try refs.append(alloc, try cloneArtifactRefAlloc(alloc, current.artifacts[artifact_index]));
        return true;
    }
    if (countArtifactsByKind(current, kind) == 1) {
        if (findArtifactIndex(current, kind)) |artifact_index| {
            var artifact = try cloneArtifactRefAlloc(alloc, current.artifacts[artifact_index]);
            if (!std.mem.eql(u8, artifact.name, name)) {
                if (artifact.name.len > 0) alloc.free(artifact.name);
                artifact.name = try alloc.dupe(u8, name);
            }
            try refs.append(alloc, artifact);
            return true;
        }
    }
    return false;
}

fn cloneNamedArtifactRefAlloc(
    alloc: Allocator,
    current: manifest_mod.Manifest,
    kind: manifest_mod.ArtifactKind,
    name: []const u8,
) !?manifest_mod.ArtifactRef {
    if (findNamedArtifactIndex(current, kind, name)) |artifact_index| {
        var artifact = try cloneArtifactRefAlloc(alloc, current.artifacts[artifact_index]);
        if (artifact.name.len == 0) artifact.name = try alloc.dupe(u8, name);
        return artifact;
    }
    if (countArtifactsByKind(current, kind) == 1) {
        if (findArtifactIndex(current, kind)) |artifact_index| {
            var artifact = try cloneArtifactRefAlloc(alloc, current.artifacts[artifact_index]);
            if (!std.mem.eql(u8, artifact.name, name)) {
                if (artifact.name.len > 0) alloc.free(artifact.name);
                artifact.name = try alloc.dupe(u8, name);
            }
            return artifact;
        }
    }
    return null;
}

fn namedArtifactActionForName(
    planned_actions: []const publication_plan.NamedArtifactAction,
    name: []const u8,
    fallback: publication_plan.ArtifactAction,
) publication_plan.ArtifactAction {
    for (planned_actions) |action| {
        if (std.mem.eql(u8, action.name, name)) return action.action;
    }
    return fallback;
}

const PredictedNamedSourceKind = enum {
    vector,
    sparse,
    graph,
};

fn predictFullTextIndexActionsAlloc(
    alloc: Allocator,
    current: manifest_mod.Manifest,
    before_docs: []const query_mod.QueryMaterializedDocument,
    docs: []const query_mod.QueryMaterializedDocument,
    mutations: []const query_mod.QueryMaterializerMutation,
    specs: []const FullTextIndexSpec,
    planned_actions: []const publication_plan.FullTextIndexAction,
    fallback: publication_plan.ArtifactAction,
) ![]publication_plan.FullTextIndexAction {
    var out = std.ArrayListUnmanaged(publication_plan.FullTextIndexAction).empty;
    errdefer {
        for (out.items) |*entry| entry.deinit(alloc);
        out.deinit(alloc);
    }

    for (specs) |spec| {
        const planned = fullTextActionForName(planned_actions, spec.name, fallback);
        const action = if (planned == .drop)
            publication_plan.ArtifactAction.drop
        else if (artifactAvailableForName(current, .text_segment, spec.name) and
            !try textProjectionChangedForMutationsAlloc(alloc, before_docs, docs, mutations, spec))
            publication_plan.ArtifactAction.reuse
        else
            publication_plan.ArtifactAction.rebuild;
        try out.append(alloc, .{
            .name = try alloc.dupe(u8, spec.name),
            .action = action,
            .source_mode = spec.source_mode,
            .chunked_source_count = spec.chunked_sources.len,
        });
    }

    return try out.toOwnedSlice(alloc);
}

fn predictNamedArtifactActionsAlloc(
    alloc: Allocator,
    current: manifest_mod.Manifest,
    artifact_kind: manifest_mod.ArtifactKind,
    before_docs: []const query_mod.QueryMaterializedDocument,
    docs: []const query_mod.QueryMaterializedDocument,
    mutations: []const query_mod.QueryMaterializerMutation,
    published_search_sources: search_sources.PublishedSearchSources,
    kind: PredictedNamedSourceKind,
    planned_actions: []const publication_plan.NamedArtifactAction,
    fallback: publication_plan.ArtifactAction,
) ![]publication_plan.NamedArtifactAction {
    var out = std.ArrayListUnmanaged(publication_plan.NamedArtifactAction).empty;
    errdefer {
        for (out.items) |*entry| entry.deinit(alloc);
        out.deinit(alloc);
    }

    switch (kind) {
        .vector => {
            const sources = try search_sources.listVectorSourcesAlloc(alloc, published_search_sources);
            defer search_sources.freeVectorSourceDescriptors(alloc, sources);
            for (sources) |source| {
                const planned = namedArtifactActionForName(planned_actions, source.index_name, fallback);
                const action = if (planned == .drop)
                    publication_plan.ArtifactAction.drop
                else if (artifactAvailableForName(current, artifact_kind, source.index_name) and
                    !try vectorProjectionChangedForMutationsAlloc(alloc, before_docs, docs, mutations, source))
                    publication_plan.ArtifactAction.reuse
                else
                    publication_plan.ArtifactAction.rebuild;
                try out.append(alloc, .{
                    .name = try alloc.dupe(u8, source.index_name),
                    .action = action,
                });
            }
        },
        .sparse => {
            const sources = try search_sources.listSparseSourcesAlloc(alloc, published_search_sources);
            defer search_sources.freeSparseSourceDescriptors(alloc, sources);
            for (sources) |source| {
                const planned = namedArtifactActionForName(planned_actions, source.index_name, fallback);
                const action = if (planned == .drop)
                    publication_plan.ArtifactAction.drop
                else if (artifactAvailableForName(current, artifact_kind, source.index_name) and
                    !try sparseProjectionChangedForMutationsAlloc(alloc, before_docs, docs, mutations, source))
                    publication_plan.ArtifactAction.reuse
                else
                    publication_plan.ArtifactAction.rebuild;
                try out.append(alloc, .{
                    .name = try alloc.dupe(u8, source.index_name),
                    .action = action,
                });
            }
        },
        .graph => {
            for (planned_actions) |planned_item| {
                const action = if (planned_item.action == .drop)
                    publication_plan.ArtifactAction.drop
                else if (artifactAvailableForName(current, artifact_kind, planned_item.name) and !try graphProjectionChangedForMutationsAlloc(alloc, before_docs, docs, mutations))
                    publication_plan.ArtifactAction.reuse
                else
                    publication_plan.ArtifactAction.rebuild;
                try out.append(alloc, .{
                    .name = try alloc.dupe(u8, planned_item.name),
                    .action = action,
                });
            }
        },
    }

    return try out.toOwnedSlice(alloc);
}

fn artifactAvailableForName(
    current: manifest_mod.Manifest,
    kind: manifest_mod.ArtifactKind,
    name: []const u8,
) bool {
    if (findNamedArtifactIndex(current, kind, name) != null) return true;
    return countArtifactsByKind(current, kind) == 1 and findArtifactIndex(current, kind) != null;
}

fn predictDerivedOutputAction(
    current: search_sources.MaterializedDerivedOutputs,
    predicted: search_sources.MaterializedDerivedOutputs,
    kind: search_sources.DerivedOutputKind,
    fallback: publication_plan.DerivedOutputAction,
) publication_plan.DerivedOutputAction {
    const current_present = current.containsKind(kind);
    const predicted_present = predicted.containsKind(kind);
    return switch (fallback) {
        .drop => .drop,
        .recompute => if (predicted_present) .recompute else .drop,
        .reuse => if (current_present and predicted_present)
            .reuse
        else if (!current_present and predicted_present)
            .recompute
        else if (current_present and !predicted_present)
            .drop
        else
            .reuse,
    };
}

pub fn buildGraphSegmentAlloc(
    alloc: Allocator,
    docs: []const query_mod.QueryMaterializedDocument,
    include_graph: bool,
) !struct {
    payload: ?[]u8,
    edge_count: usize,
} {
    if (!include_graph) return .{ .payload = null, .edge_count = 0 };
    var node_map = std.StringArrayHashMapUnmanaged(NodeEdges).empty;
    defer deinitNodeMap(alloc, &node_map);
    var total_edges: usize = 0;

    for (docs) |doc| {
        _ = try ensureNode(alloc, &node_map, doc.doc_id);
        const parsed_edges = try parseGraphEdgesAlloc(alloc, doc.body);
        defer freeParsedGraphEdges(alloc, parsed_edges);
        for (parsed_edges) |edge| {
            const src = try ensureNode(alloc, &node_map, doc.doc_id);
            const dst = try ensureNode(alloc, &node_map, edge.target);
            try src.out_edges.append(alloc, .{
                .neighbor_id = try alloc.dupe(u8, edge.target),
                .edge_type = try alloc.dupe(u8, edge.edge_type),
                .weight = edge.weight,
            });
            try dst.in_edges.append(alloc, .{
                .neighbor_id = try alloc.dupe(u8, doc.doc_id),
                .edge_type = try alloc.dupe(u8, edge.edge_type),
                .weight = edge.weight,
            });
            total_edges += 1;
        }
    }

    if (total_edges == 0) return .{ .payload = null, .edge_count = 0 };

    var segment = try nodeMapToSegmentAlloc(alloc, &node_map);
    defer graph_segment_mod.freeSegment(alloc, &segment);
    return .{
        .payload = try graph_segment_mod.encodeAlloc(alloc, segment),
        .edge_count = total_edges,
    };
}

const ParsedGraphEdge = struct {
    target: []u8,
    edge_type: []u8,
    weight: f32,
};

const NodeEdges = struct {
    out_edges: std.ArrayListUnmanaged(graph_segment_mod.Edge) = .empty,
    in_edges: std.ArrayListUnmanaged(graph_segment_mod.Edge) = .empty,
};

fn ensureNode(alloc: Allocator, node_map: *std.StringArrayHashMapUnmanaged(NodeEdges), node_id: []const u8) !*NodeEdges {
    const gop = try node_map.getOrPut(alloc, node_id);
    if (!gop.found_existing) {
        gop.key_ptr.* = try alloc.dupe(u8, node_id);
        gop.value_ptr.* = .{};
    }
    return gop.value_ptr;
}

fn parseGraphEdgesAlloc(alloc: Allocator, body: []const u8) ![]ParsedGraphEdge {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, body, .{}) catch return try alloc.alloc(ParsedGraphEdge, 0);
    defer parsed.deinit();
    if (parsed.value != .object) return try alloc.alloc(ParsedGraphEdge, 0);
    const raw_edges = parsed.value.object.get("graph_edges") orelse return try alloc.alloc(ParsedGraphEdge, 0);
    if (raw_edges != .array) return try alloc.alloc(ParsedGraphEdge, 0);

    var out = std.ArrayListUnmanaged(ParsedGraphEdge).empty;
    errdefer {
        for (out.items) |edge| {
            alloc.free(edge.target);
            alloc.free(edge.edge_type);
        }
        out.deinit(alloc);
    }

    for (raw_edges.array.items) |item| {
        if (item != .object) continue;
        const target_value = item.object.get("target") orelse continue;
        if (target_value != .string or target_value.string.len == 0) continue;
        const edge_type_value = item.object.get("edge_type");
        const weight_value = item.object.get("weight");
        try out.append(alloc, .{
            .target = try alloc.dupe(u8, target_value.string),
            .edge_type = if (edge_type_value != null and edge_type_value.? == .string) try alloc.dupe(u8, edge_type_value.?.string) else try alloc.dupe(u8, ""),
            .weight = if (weight_value) |weight| switch (weight) {
                .float => @floatCast(weight.float),
                .integer => @floatFromInt(weight.integer),
                .number_string => std.fmt.parseFloat(f32, weight.number_string) catch 1.0,
                else => 1.0,
            } else 1.0,
        });
    }
    return try out.toOwnedSlice(alloc);
}

fn freeParsedGraphEdges(alloc: Allocator, edges: []ParsedGraphEdge) void {
    for (edges) |edge| {
        alloc.free(edge.target);
        alloc.free(edge.edge_type);
    }
    alloc.free(edges);
}

fn sortParsedGraphEdges(edges: []ParsedGraphEdge) void {
    std.mem.sort(ParsedGraphEdge, edges, {}, lessParsedGraphEdge);
}

fn lessParsedGraphEdge(_: void, lhs: ParsedGraphEdge, rhs: ParsedGraphEdge) bool {
    const edge_type_order = std.mem.order(u8, lhs.edge_type, rhs.edge_type);
    if (edge_type_order != .eq) return edge_type_order == .lt;
    const target_order = std.mem.order(u8, lhs.target, rhs.target);
    if (target_order != .eq) return target_order == .lt;
    return lhs.weight < rhs.weight;
}

fn nodeMapToSegmentAlloc(alloc: Allocator, node_map: *std.StringArrayHashMapUnmanaged(NodeEdges)) !graph_segment_mod.Segment {
    const adjacencies = try alloc.alloc(graph_segment_mod.Adjacency, node_map.count());
    errdefer alloc.free(adjacencies);
    var initialized: usize = 0;
    errdefer {
        for (adjacencies[0..initialized]) |*adjacency| adjacency.deinit(alloc);
    }

    for (node_map.keys(), node_map.values(), 0..) |node_id, *node_edges, idx| {
        sortGraphEdges(node_edges.out_edges.items);
        sortGraphEdges(node_edges.in_edges.items);
        adjacencies[idx] = .{
            .node_id = try alloc.dupe(u8, node_id),
            .out_edges = try node_edges.out_edges.toOwnedSlice(alloc),
            .in_edges = try node_edges.in_edges.toOwnedSlice(alloc),
        };
        initialized += 1;
    }
    std.mem.sort(graph_segment_mod.Adjacency, adjacencies, {}, lessGraphAdjacency);
    return .{ .adjacencies = adjacencies };
}

fn deinitNodeMap(alloc: Allocator, node_map: *std.StringArrayHashMapUnmanaged(NodeEdges)) void {
    for (node_map.keys(), node_map.values()) |key, *value| {
        alloc.free(key);
        for (value.out_edges.items) |*edge| edge.deinit(alloc);
        value.out_edges.deinit(alloc);
        for (value.in_edges.items) |*edge| edge.deinit(alloc);
        value.in_edges.deinit(alloc);
    }
    node_map.deinit(alloc);
}

fn sortGraphEdges(edges: []graph_segment_mod.Edge) void {
    std.mem.sort(graph_segment_mod.Edge, edges, {}, lessGraphEdge);
}

fn lessGraphEdge(_: void, lhs: graph_segment_mod.Edge, rhs: graph_segment_mod.Edge) bool {
    const edge_type_order = std.mem.order(u8, lhs.edge_type, rhs.edge_type);
    if (edge_type_order != .eq) return edge_type_order == .lt;
    const neighbor_order = std.mem.order(u8, lhs.neighbor_id, rhs.neighbor_id);
    if (neighbor_order != .eq) return neighbor_order == .lt;
    return lhs.weight < rhs.weight;
}

fn lessGraphAdjacency(_: void, lhs: graph_segment_mod.Adjacency, rhs: graph_segment_mod.Adjacency) bool {
    return std.mem.order(u8, lhs.node_id, rhs.node_id) == .lt;
}

fn allocSparseSegmentAlloc(
    alloc: Allocator,
    docs: []const query_mod.QueryMaterializedDocument,
    sparse_source: search_sources.SparseSourceDescriptor,
) !?sparse_segment_mod.Segment {
    var sparse_doc_count: usize = 0;
    for (docs) |doc| {
        var projection = try document_projection.parseAlloc(alloc, doc.body);
        defer projection.deinit(alloc);
        if (search_sources.selectSparseSource(&projection, sparse_source) != null) sparse_doc_count += 1;
    }
    if (sparse_doc_count == 0) return null;

    const doc_entries = try alloc.alloc(sparse_segment_mod.DocumentEntry, sparse_doc_count);
    errdefer alloc.free(doc_entries);
    var docs_initialized: usize = 0;
    errdefer {
        for (doc_entries[0..docs_initialized]) |*doc| doc.deinit(alloc);
    }

    var term_map = std.StringArrayHashMapUnmanaged(std.ArrayListUnmanaged(sparse_segment_mod.Posting)).empty;
    defer {
        for (term_map.keys()) |term| alloc.free(term);
        for (term_map.values()) |*postings| postings.deinit(alloc);
        term_map.deinit(alloc);
    }

    for (docs) |doc| {
        var projection = try document_projection.parseAlloc(alloc, doc.body);
        defer projection.deinit(alloc);
        const sparse_embedding = search_sources.selectSparseSource(&projection, sparse_source) orelse continue;

        for (sparse_embedding) |feature| {
            const normalized_term = try query_mod.indexed_reader.normalizeAlloc(alloc, feature.term);
            if (normalized_term.len == 0) {
                alloc.free(normalized_term);
                continue;
            }
            const gop = try term_map.getOrPut(alloc, normalized_term);
            if (!gop.found_existing) {
                gop.value_ptr.* = .empty;
            } else {
                alloc.free(normalized_term);
            }
            try gop.value_ptr.append(alloc, .{
                .doc_index = @intCast(docs_initialized),
                .weight = feature.weight,
            });
        }

        doc_entries[docs_initialized] = .{
            .doc_id = try alloc.dupe(u8, doc.doc_id),
            .feature_count = @intCast(sparse_embedding.len),
        };
        docs_initialized += 1;
    }

    const term_entries = try alloc.alloc(sparse_segment_mod.TermEntry, term_map.count());
    errdefer alloc.free(term_entries);
    var terms_initialized: usize = 0;
    errdefer {
        for (term_entries[0..terms_initialized]) |*term| term.deinit(alloc);
    }

    for (term_map.keys(), 0..) |term, idx| {
        term_entries[idx] = .{
            .term = try alloc.dupe(u8, term),
            .postings = try term_map.values()[idx].toOwnedSlice(alloc),
        };
        terms_initialized += 1;
    }

    std.mem.sort(sparse_segment_mod.TermEntry, term_entries, {}, lessSparseTermEntry);
    return .{
        .docs = doc_entries,
        .terms = term_entries,
    };
}

fn lessSparseTermEntry(_: void, lhs: sparse_segment_mod.TermEntry, rhs: sparse_segment_mod.TermEntry) bool {
    return std.mem.order(u8, lhs.term, rhs.term) == .lt;
}

fn lessTermEntry(_: void, lhs: text_segment_mod.TermEntry, rhs: text_segment_mod.TermEntry) bool {
    return std.mem.order(u8, lhs.term, rhs.term) == .lt;
}

test "builder publishes first manifest from WAL and query sees mutation segment artifact" {
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

    var fs_wal = try wal_mod.FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_progress = try catalog_mod.FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    const mutation_a = api_types.DocumentMutation{ .kind = .upsert, .doc_id = "doc-a", .body = "alpha" };
    const mutation_b = api_types.DocumentMutation{ .kind = .upsert, .doc_id = "doc-b", .body = "beta" };
    const encoded_a = try api_codec.encodeMutationAlloc(alloc, mutation_a);
    defer alloc.free(encoded_a);
    const encoded_b = try api_codec.encodeMutationAlloc(alloc, mutation_b);
    defer alloc.free(encoded_b);
    _ = try wal_store.append("docs", 100, encoded_a);
    _ = try wal_store.append("docs", 200, encoded_b);

    var builder = Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var result = try builder.publishNamespace("docs");
    defer result.deinit(alloc);

    try std.testing.expect(result.published);
    try std.testing.expectEqual(@as(u64, 1), result.version);
    try std.testing.expectEqual(@as(u64, 1), result.wal_start_lsn);
    try std.testing.expectEqual(@as(u64, 2), result.wal_end_lsn);
    try std.testing.expectEqual(@as(usize, 3), result.artifact_count);
    try std.testing.expectEqual(@as(u64, 1), try progress_store.getHead("docs"));

    var runtime = query_mod.QueryRuntime.init(alloc, &artifact_store, &manifest_store, &progress_store);
    defer runtime.deinit();
    var session = try runtime.openHeadSession("docs");
    defer session.deinit();

    const built = try session.fetchArtifactAlloc(0);
    defer alloc.free(built);
    const decoded = try segment_mod.decodeAlloc(alloc, built);
    defer segment_mod.freeEntries(alloc, decoded);
    try std.testing.expectEqual(@as(usize, 2), decoded.len);
    try std.testing.expectEqual(@as(api_types.MutationKind, .upsert), decoded[0].kind);
    try std.testing.expectEqualStrings("doc-a", decoded[0].doc_id);
    try std.testing.expectEqualStrings("alpha", decoded[0].body.?);
    try std.testing.expectEqualStrings("doc-b", decoded[1].doc_id);
    try std.testing.expectEqualStrings("beta", decoded[1].body.?);
}

test "builder retries publication after manifest write without head advance" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-retry");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-retry");
    const wal_root = tmpPath(&wal_root_buf, "wal-retry");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);

    var fs_artifacts = try artifacts_mod.FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_wal = try wal_mod.FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_progress = try catalog_mod.FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    const encoded = try api_codec.encodeMutationAlloc(alloc, .{
        .kind = .upsert,
        .doc_id = "doc-a",
        .body = "alpha",
    });
    defer alloc.free(encoded);
    _ = try wal_store.append("docs", 100, encoded);

    const records = try wal_store.readFromAlloc("docs", 1);
    defer wal_mod.freeRecords(alloc, records);
    const mutation_payload = try buildMutationSegmentAlloc(alloc, records);
    defer alloc.free(mutation_payload);
    var mutation_artifact = try artifact_store.put(mutation_payload);
    defer mutation_artifact.deinit(alloc);

    var builder = Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    const document_segment = try buildDocumentSegmentAlloc(&builder, "docs", 0, records, false);
    defer freeMaterializerMutations(alloc, document_segment.mutations);
    defer query_mod.freeMaterializedDocuments(alloc, document_segment.base_documents);
    defer query_mod.freeMaterializedDocuments(alloc, document_segment.documents);
    defer alloc.free(document_segment.payload);
    var document_artifact = try artifact_store.put(document_segment.payload);
    defer document_artifact.deinit(alloc);
    const document_ref = try artifactRefFromMetadataAlloc(alloc, .document_segment, document_artifact);
    defer freeArtifactRef(alloc, document_ref);
    const text_specs = try alloc.alloc(FullTextIndexSpec, 1);
    defer full_text_indexes.freeFullTextIndexSpecs(alloc, text_specs);
    text_specs[0] = .{
        .name = try alloc.dupe(u8, search_sources.default_full_text_index_name),
        .config_json = try alloc.dupe(u8, "{\"type\":\"full_text\"}"),
        .source_artifact_name = null,
    };
    const text_refs = try buildTextArtifactRefsForMaterializedDocsAlloc(
        alloc,
        &artifact_store,
        null,
        document_segment.base_documents,
        document_segment.documents,
        document_segment.mutations,
        text_specs,
    );
    defer freeArtifactRefs(alloc, text_refs);

    var manifest = try buildManifestAlloc(
        alloc,
        "docs",
        1,
        100,
        1,
        1,
        document_segment.document_count,
        1,
        .append_mutation_tail,
        mutation_artifact,
        document_ref,
        text_refs,
        &.{},
        &.{},
        &.{},
        .{},
        .{},
        .{},
    );
    defer manifest.deinit(alloc);
    try manifest_store.put(manifest);

    var result = try builder.publishNamespace("docs");
    defer result.deinit(alloc);
    try std.testing.expect(result.published);
    try std.testing.expectEqual(@as(u64, 1), try progress_store.getHead("docs"));
}

test "builder advances version only when new WAL data exists" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-advance");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-advance");
    const wal_root = tmpPath(&wal_root_buf, "wal-advance");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);

    var fs_artifacts = try artifacts_mod.FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_wal = try wal_mod.FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_progress = try catalog_mod.FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    const first_mutation = api_types.DocumentMutation{ .kind = .upsert, .doc_id = "doc-1", .body = "one" };
    const first_encoded = try api_codec.encodeMutationAlloc(alloc, first_mutation);
    defer alloc.free(first_encoded);
    _ = try wal_store.append("docs", 100, first_encoded);

    var builder = Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);

    var first = try builder.publishNamespace("docs");
    defer first.deinit(alloc);
    try std.testing.expect(first.published);
    try std.testing.expectEqual(@as(u64, 1), first.version);

    var noop = try builder.publishNamespace("docs");
    defer noop.deinit(alloc);
    try std.testing.expect(!noop.published);
    try std.testing.expectEqual(@as(u64, 1), noop.version);

    const second_mutation = api_types.DocumentMutation{ .kind = .delete, .doc_id = "doc-1", .body = null };
    const second_encoded = try api_codec.encodeMutationAlloc(alloc, second_mutation);
    defer alloc.free(second_encoded);
    _ = try wal_store.append("docs", 200, second_encoded);
    var second = try builder.publishNamespace("docs");
    defer second.deinit(alloc);
    try std.testing.expect(second.published);
    try std.testing.expectEqual(@as(u64, 2), second.version);
    try std.testing.expectEqual(@as(u64, 2), second.wal_start_lsn);
    try std.testing.expectEqual(@as(u64, 2), second.wal_end_lsn);

    var loaded = try manifest_store.getAlloc("docs", 2);
    defer loaded.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 2), loaded.version);
    try std.testing.expectEqual(@as(u64, 2), loaded.wal_start_lsn);
    try std.testing.expectEqual(@as(u64, 2), loaded.wal_end_lsn);
}

test "resolve published text specs includes chunk preview for chunker full text config" {
    const alloc = std.testing.allocator;
    var table_definition = publication_plan.TableDefinitionSnapshot{
        .schema_json = try alloc.dupe(u8, "{\"version\":0}"),
        .read_schema_json = try alloc.dupe(u8, ""),
        .indexes_json = try alloc.dupe(
            u8,
            "{\"full_text_index_v0\":{\"type\":\"full_text\"},\"chunk_idx\":{\"type\":\"embeddings\",\"field\":\"body\",\"dimension\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"artifact_name\":\"body_chunks_v1\",\"chunker\":{\"provider\":\"antfly\",\"store_chunks\":false,\"full_text_index\":{},\"text\":{\"target_tokens\":8}}}}}",
        ),
    };
    defer table_definition.deinit(alloc);
    const specs = try resolvePublishedTextIndexSpecsAlloc(alloc, table_definition, &.{});
    defer full_text_indexes.freeFullTextIndexSpecs(alloc, specs);

    try std.testing.expectEqual(@as(usize, 1), specs.len);
    try std.testing.expectEqualStrings("full_text_index_v0", specs[0].name);
    try std.testing.expectEqual(FullTextSourceMode.document_plus_artifact, specs[0].source_mode);
    try std.testing.expectEqualStrings(search_sources.default_chunk_preview_output_name, specs[0].source_artifact_name.?);
}

test "builder reuses unchanged artifacts during metadata-only republish" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-republish-reuse");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-republish-reuse");
    const wal_root = tmpPath(&wal_root_buf, "wal-republish-reuse");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);

    var fs_artifacts = try artifacts_mod.FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_wal = try wal_mod.FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_progress = try catalog_mod.FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    const encoded = try api_codec.encodeMutationAlloc(alloc, .{
        .kind = .upsert,
        .doc_id = "doc-vs",
        .body = "{\"text\":\"alpha\",\"embedding\":[1,0,0],\"sparse_embedding\":{\"alpha\":1.0},\"chunk_embeddings\":[{\"chunk\":\"alpha\",\"embedding\":[1,0,0]}],\"_enrichment\":{\"chunk_embeddings\":true,\"chunk_embeddings_version\":1}}",
    });
    defer alloc.free(encoded);
    _ = try wal_store.append("docs", 100, encoded);

    var builder = Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var initial_sources = try search_sources.publishedSearchSourcesForNamesAlloc(alloc, "semantic_idx", "sparse_idx");
    defer search_sources.deinitPublishedSearchSources(alloc, &initial_sources);
    var initial = try builder.publishNamespaceWithMetricAndTargets("docs", .cosine, .{
        .published_search_sources = initial_sources,
        .include_graph = true,
    }, false);
    defer initial.deinit(alloc);
    try std.testing.expect(initial.published);

    var first_manifest = try manifest_store.getAlloc("docs", 1);
    defer first_manifest.deinit(alloc);
    const first_doc = first_manifest.artifacts[findArtifactIndex(first_manifest, .document_segment).?];
    const first_text = first_manifest.artifacts[findArtifactIndex(first_manifest, .text_segment).?];
    const first_vector = first_manifest.artifacts[findArtifactIndex(first_manifest, .vector_segment).?];
    const first_sparse = first_manifest.artifacts[findArtifactIndex(first_manifest, .sparse_segment).?];
    try std.testing.expectEqualStrings("semantic_idx", first_vector.name);
    try std.testing.expectEqualStrings("sparse_idx", first_sparse.name);

    var republished_sources = try search_sources.publishedSearchSourcesForNamesAlloc(alloc, "semantic_idx_v2", null);
    defer search_sources.deinitPublishedSearchSources(alloc, &republished_sources);
    var republish = try builder.publishNamespaceWithMetricAndTargets("docs", .cosine, .{
        .published_search_sources = republished_sources,
        .include_graph = true,
    }, true);
    defer republish.deinit(alloc);
    try std.testing.expect(republish.published);
    try std.testing.expectEqual(@as(u64, 2), republish.version);
    try std.testing.expectEqual(@as(u64, 1), republish.wal_end_lsn);

    var second_manifest = try manifest_store.getAlloc("docs", 2);
    defer second_manifest.deinit(alloc);
    try std.testing.expectEqualStrings(first_doc.artifact_id, second_manifest.artifacts[findArtifactIndex(second_manifest, .document_segment).?].artifact_id);
    try std.testing.expectEqualStrings(first_text.artifact_id, second_manifest.artifacts[findArtifactIndex(second_manifest, .text_segment).?].artifact_id);
    try std.testing.expectEqualStrings(first_vector.artifact_id, second_manifest.artifacts[findArtifactIndex(second_manifest, .vector_segment).?].artifact_id);
    try std.testing.expectEqualStrings("semantic_idx_v2", second_manifest.artifacts[findArtifactIndex(second_manifest, .vector_segment).?].name);
    try std.testing.expect(findArtifactIndex(second_manifest, .sparse_segment) == null);
    try std.testing.expectEqualStrings("semantic_idx_v2", second_manifest.stats.published_search_sources.findVector().?.index_name);
    try std.testing.expect(second_manifest.stats.published_search_sources.findSparse() == null);
    try std.testing.expectEqualStrings(
        search_sources.default_chunk_embeddings_output_name,
        second_manifest.stats.derived_outputs.findByKind(.chunk_embeddings).?.name,
    );
}

test "builder recomputes chunk embeddings derived output during metadata-only republish" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-republish-chunk-embeddings");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-republish-chunk-embeddings");
    const wal_root = tmpPath(&wal_root_buf, "wal-republish-chunk-embeddings");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);

    var fs_artifacts = try artifacts_mod.FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_wal = try wal_mod.FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_progress = try catalog_mod.FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    const encoded = try api_codec.encodeMutationAlloc(alloc, .{
        .kind = .upsert,
        .doc_id = "doc-ce",
        .body = "{\"text\":\"alpha\",\"chunk_embeddings\":[{\"chunk\":\"alpha\",\"embedding\":[1,0,0]}],\"_enrichment\":{\"chunk_embeddings\":true,\"chunk_embeddings_version\":1}}",
    });
    defer alloc.free(encoded);
    _ = try wal_store.append("docs", 100, encoded);

    var builder = Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var initial_sources = try search_sources.defaultPublishedSearchSourcesAlloc(alloc);
    defer search_sources.deinitPublishedSearchSources(alloc, &initial_sources);
    var initial = try builder.publishNamespaceWithMetricAndTargets("docs", .cosine, .{
        .published_search_sources = initial_sources,
        .include_graph = true,
    }, false);
    defer initial.deinit(alloc);
    try std.testing.expect(initial.published);

    var first_manifest = try manifest_store.getAlloc("docs", 1);
    defer first_manifest.deinit(alloc);
    try std.testing.expectEqualStrings(
        search_sources.default_chunk_embeddings_output_name,
        first_manifest.stats.derived_outputs.findByKind(.chunk_embeddings).?.name,
    );

    const republish_sources = try search_sources.clonePublishedSearchSourcesAlloc(alloc, first_manifest.stats.published_search_sources);
    var plan = publication_plan.TablePublicationPlan{
        .targets = .{
            .published_search_sources = republish_sources,
            .include_graph = true,
        },
        .policy = .{},
        .table_definition = .{},
        .metadata_republish = .{ .artifact_families_changed = true },
        .artifact_actions = .{
            .document_segment = .reuse,
            .full_text = .reuse,
            .dense_vector = .reuse,
            .sparse_vector = .reuse,
            .graph = .reuse,
        },
        .derived_output_actions = .{
            .chunk_preview = .reuse,
            .chunk_embeddings = .recompute,
            .rerank_terms = .reuse,
        },
    };
    defer plan.deinit(alloc);

    var republish = try builder.publishNamespaceWithMetricAndPlan("docs", .cosine, plan);
    defer republish.deinit(alloc);
    try std.testing.expect(republish.published);
    try std.testing.expectEqual(@as(u64, 2), republish.version);
    try std.testing.expectEqual(@as(u64, 1), republish.wal_end_lsn);

    var second_manifest = try manifest_store.getAlloc("docs", 2);
    defer second_manifest.deinit(alloc);
    try std.testing.expectEqualStrings(
        search_sources.default_chunk_embeddings_output_name,
        second_manifest.stats.derived_outputs.findByKind(.chunk_embeddings).?.name,
    );
}

test "builder follows named vector and sparse publication actions during metadata-only republish" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-republish-named-actions");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-republish-named-actions");
    const wal_root = tmpPath(&wal_root_buf, "wal-republish-named-actions");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);

    var fs_artifacts = try artifacts_mod.FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_wal = try wal_mod.FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_progress = try catalog_mod.FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    const encoded = try api_codec.encodeMutationAlloc(alloc, .{
        .kind = .upsert,
        .doc_id = "doc-vs",
        .body = "{\"text\":\"alpha\",\"_embeddings\":{\"semantic_a\":[1,0,0],\"sparse_a\":{\"alpha\":1.0},\"sparse_b\":{\"beta\":2.0}}}",
    });
    defer alloc.free(encoded);
    _ = try wal_store.append("docs", 100, encoded);

    var builder = Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var initial_sources = try search_sources.publishedSearchSourcesForIndexesJsonAlloc(
        alloc,
        "{\"semantic_a\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":3},\"sparse_a\":{\"type\":\"embeddings\",\"external\":true,\"sparse\":true}}",
    );
    defer search_sources.deinitPublishedSearchSources(alloc, &initial_sources);
    var initial = try builder.publishNamespaceWithMetricAndTargets("docs", .cosine, .{
        .published_search_sources = initial_sources,
        .include_graph = false,
    }, false);
    defer initial.deinit(alloc);
    try std.testing.expect(initial.published);

    var first_manifest = try manifest_store.getAlloc("docs", 1);
    defer first_manifest.deinit(alloc);
    const first_vector = first_manifest.artifacts[findNamedArtifactIndex(first_manifest, .vector_segment, "semantic_a").?];
    const first_sparse = first_manifest.artifacts[findNamedArtifactIndex(first_manifest, .sparse_segment, "sparse_a").?];

    const republished_sources = try search_sources.publishedSearchSourcesForIndexesJsonAlloc(
        alloc,
        "{\"semantic_b\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":3},\"sparse_a\":{\"type\":\"embeddings\",\"external\":true,\"sparse\":true},\"sparse_b\":{\"type\":\"embeddings\",\"external\":true,\"sparse\":true}}",
    );
    var plan = publication_plan.TablePublicationPlan{
        .targets = .{
            .published_search_sources = republished_sources,
            .include_graph = false,
        },
        .metadata_republish = .{ .published_search_sources_changed = true },
        .artifact_actions = .{
            .document_segment = .reuse,
            .full_text = .reuse,
            .dense_vector = .reuse,
            .sparse_vector = .rebuild,
            .graph = .drop,
        },
        .vector_index_actions = try alloc.alloc(publication_plan.NamedArtifactAction, 1),
        .sparse_index_actions = try alloc.alloc(publication_plan.NamedArtifactAction, 2),
    };
    defer plan.deinit(alloc);
    plan.vector_index_actions[0] = .{
        .name = try alloc.dupe(u8, "semantic_b"),
        .action = .reuse,
    };
    plan.sparse_index_actions[0] = .{
        .name = try alloc.dupe(u8, "sparse_a"),
        .action = .reuse,
    };
    plan.sparse_index_actions[1] = .{
        .name = try alloc.dupe(u8, "sparse_b"),
        .action = .rebuild,
    };

    var republish = try builder.publishNamespaceWithMetricAndPlan("docs", .cosine, plan);
    defer republish.deinit(alloc);
    try std.testing.expect(republish.published);
    try std.testing.expectEqual(@as(u64, 2), republish.version);
    try std.testing.expectEqual(@as(u64, 1), republish.wal_end_lsn);

    var second_manifest = try manifest_store.getAlloc("docs", 2);
    defer second_manifest.deinit(alloc);
    const renamed_vector = second_manifest.artifacts[findNamedArtifactIndex(second_manifest, .vector_segment, "semantic_b").?];
    const reused_sparse = second_manifest.artifacts[findNamedArtifactIndex(second_manifest, .sparse_segment, "sparse_a").?];
    const rebuilt_sparse = second_manifest.artifacts[findNamedArtifactIndex(second_manifest, .sparse_segment, "sparse_b").?];
    try std.testing.expectEqualStrings(first_vector.artifact_id, renamed_vector.artifact_id);
    try std.testing.expectEqualStrings(first_sparse.artifact_id, reused_sparse.artifact_id);
    try std.testing.expect(!std.mem.eql(u8, first_sparse.artifact_id, rebuilt_sparse.artifact_id));
}

test "builder rebuilds named vector with updated distance metric during metadata-only republish" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-republish-vector-metric");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-republish-vector-metric");
    const wal_root = tmpPath(&wal_root_buf, "wal-republish-vector-metric");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);

    var fs_artifacts = try artifacts_mod.FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_wal = try wal_mod.FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_progress = try catalog_mod.FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    const encoded = try api_codec.encodeMutationAlloc(alloc, .{
        .kind = .upsert,
        .doc_id = "doc-vm",
        .body = "{\"text\":\"alpha\",\"_embeddings\":{\"semantic_idx\":[1,0,0]}}",
    });
    defer alloc.free(encoded);
    _ = try wal_store.append("docs", 100, encoded);

    var builder = Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var initial_sources = try search_sources.publishedSearchSourcesForIndexesJsonAlloc(
        alloc,
        "{\"semantic_idx\":{\"type\":\"embeddings\",\"dimension\":3,\"distance_metric\":\"cosine\"}}",
    );
    defer search_sources.deinitPublishedSearchSources(alloc, &initial_sources);
    var initial = try builder.publishNamespaceWithMetricAndTargets("docs", .inner_product, .{
        .published_search_sources = initial_sources,
        .include_graph = false,
    }, false);
    defer initial.deinit(alloc);
    try std.testing.expect(initial.published);

    var first_manifest = try manifest_store.getAlloc("docs", 1);
    defer first_manifest.deinit(alloc);
    const first_vector = first_manifest.artifacts[findNamedArtifactIndex(first_manifest, .vector_segment, "semantic_idx").?];
    const first_payload = try artifact_store.getAlloc(first_vector.artifact_id);
    defer alloc.free(first_payload);
    const first_header = try vector_segment_mod.decodeHeader(first_payload[0..vector_segment_mod.header_len]);
    try std.testing.expectEqual(shared_vector.DistanceMetric.cosine, first_header.metric);

    const republished_sources = try search_sources.publishedSearchSourcesForIndexesJsonAlloc(
        alloc,
        "{\"semantic_idx\":{\"type\":\"embeddings\",\"dimension\":3,\"distance_metric\":\"inner_product\"}}",
    );
    var plan = publication_plan.TablePublicationPlan{
        .targets = .{
            .published_search_sources = republished_sources,
            .include_graph = false,
        },
        .metadata_republish = .{ .artifact_families_changed = true },
        .artifact_actions = .{
            .document_segment = .reuse,
            .full_text = .drop,
            .dense_vector = .rebuild,
            .sparse_vector = .drop,
            .graph = .drop,
        },
        .vector_index_actions = try alloc.alloc(publication_plan.NamedArtifactAction, 1),
    };
    defer plan.deinit(alloc);
    plan.vector_index_actions[0] = .{
        .name = try alloc.dupe(u8, "semantic_idx"),
        .action = .rebuild,
    };

    var republish = try builder.publishNamespaceWithMetricAndPlan("docs", .cosine, plan);
    defer republish.deinit(alloc);
    try std.testing.expect(republish.published);

    var second_manifest = try manifest_store.getAlloc("docs", 2);
    defer second_manifest.deinit(alloc);
    const second_vector = second_manifest.artifacts[findNamedArtifactIndex(second_manifest, .vector_segment, "semantic_idx").?];
    try std.testing.expect(!std.mem.eql(u8, first_vector.artifact_id, second_vector.artifact_id));
    const second_payload = try artifact_store.getAlloc(second_vector.artifact_id);
    defer alloc.free(second_payload);
    const second_header = try vector_segment_mod.decodeHeader(second_payload[0..vector_segment_mod.header_len]);
    try std.testing.expectEqual(shared_vector.DistanceMetric.inner_product, second_header.metric);
}

test "builder publishes vector segment when document body carries embedding" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-vector");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-vector");
    const wal_root = tmpPath(&wal_root_buf, "wal-vector");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);

    var fs_artifacts = try artifacts_mod.FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_wal = try wal_mod.FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_progress = try catalog_mod.FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    const encoded = try api_codec.encodeMutationAlloc(alloc, .{
        .kind = .upsert,
        .doc_id = "doc-v",
        .body = "{\"text\":\"alpha\",\"embedding\":[1,0,0]}",
    });
    defer alloc.free(encoded);
    _ = try wal_store.append("docs", 100, encoded);

    var builder = Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var result = try builder.publishNamespace("docs");
    defer result.deinit(alloc);
    try std.testing.expect(result.published);
    try std.testing.expectEqual(@as(usize, 4), result.artifact_count);

    var manifest = try manifest_store.getAlloc("docs", 1);
    defer manifest.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 1), manifest.stats.vector_segment_count);
    try std.testing.expectEqualStrings(search_sources.default_chunk_embedding_index_name, manifest.stats.published_search_sources.findVector().?.index_name);
    try std.testing.expectEqual(search_sources.VectorDocumentSource.chunk_embeddings_or_top_level, manifest.stats.published_search_sources.findVector().?.document_source);
    try std.testing.expect(manifest.artifacts[3].kind == .vector_segment);
    try std.testing.expectEqualStrings(search_sources.default_chunk_embedding_index_name, manifest.artifacts[3].name);
}

test "builder publishes vector segment from chunk embeddings when top-level embedding is absent" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-vector-chunks");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-vector-chunks");
    const wal_root = tmpPath(&wal_root_buf, "wal-vector-chunks");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);

    var fs_artifacts = try artifacts_mod.FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_wal = try wal_mod.FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_progress = try catalog_mod.FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    const encoded = try api_codec.encodeMutationAlloc(alloc, .{
        .kind = .upsert,
        .doc_id = "doc-v",
        .body = "{\"text\":\"alpha\",\"chunk_embeddings\":[{\"chunk\":\"a\",\"embedding\":[1,0,0]},{\"chunk\":\"b\",\"embedding\":[0.9,0.1,0]}],\"_enrichment\":{\"chunk_embeddings\":true,\"chunk_embeddings_version\":1}}",
    });
    defer alloc.free(encoded);
    _ = try wal_store.append("docs", 100, encoded);

    var builder = Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var result = try builder.publishNamespace("docs");
    defer result.deinit(alloc);
    try std.testing.expect(result.published);

    var manifest = try manifest_store.getAlloc("docs", 1);
    defer manifest.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 1), manifest.stats.vector_segment_count);
    try std.testing.expectEqualStrings(search_sources.default_chunk_embedding_index_name, manifest.stats.published_search_sources.findVector().?.index_name);
    const vector_ref = manifest.artifacts[3];
    try std.testing.expect(vector_ref.kind == .vector_segment);
    try std.testing.expectEqualStrings(search_sources.default_chunk_embedding_index_name, vector_ref.name);

    const payload = try artifact_store.getAlloc(vector_ref.artifact_id);
    defer alloc.free(payload);
    var decoded = try vector_segment_mod.decodeAlloc(alloc, payload);
    defer vector_segment_mod.freeSegment(alloc, &decoded);
    try std.testing.expectEqual(@as(usize, 2), decoded.entries.len);
    try std.testing.expectEqualStrings("doc-v", decoded.entries[0].doc_id);
    try std.testing.expectEqualStrings("doc-v", decoded.entries[1].doc_id);
}

test "builder publishes sparse segment when document body carries sparse embedding" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-sparse");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-sparse");
    const wal_root = tmpPath(&wal_root_buf, "wal-sparse");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);

    var fs_artifacts = try artifacts_mod.FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_wal = try wal_mod.FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_progress = try catalog_mod.FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    const encoded = try api_codec.encodeMutationAlloc(alloc, .{
        .kind = .upsert,
        .doc_id = "doc-s",
        .body = "{\"text\":\"alpha\",\"sparse_embedding\":{\"alpha\":1.5,\"bravo\":0.5}}",
    });
    defer alloc.free(encoded);
    _ = try wal_store.append("docs", 100, encoded);

    var builder = Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var result = try builder.publishNamespace("docs");
    defer result.deinit(alloc);
    try std.testing.expect(result.published);
    try std.testing.expectEqual(@as(usize, 4), result.artifact_count);

    var manifest = try manifest_store.getAlloc("docs", 1);
    defer manifest.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 1), manifest.stats.sparse_segment_count);
    try std.testing.expectEqualStrings(search_sources.default_sparse_embedding_index_name, manifest.stats.published_search_sources.findSparse().?.index_name);
    try std.testing.expectEqual(search_sources.SparseDocumentSource.sparse_embedding, manifest.stats.published_search_sources.findSparse().?.document_source);
    try std.testing.expect(manifest.artifacts[3].kind == .sparse_segment);
    try std.testing.expectEqualStrings(search_sources.default_sparse_embedding_index_name, manifest.artifacts[3].name);
}

test "builder publishes multiple named vector and sparse segments from named embeddings" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-multi-embeddings");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-multi-embeddings");
    const wal_root = tmpPath(&wal_root_buf, "wal-multi-embeddings");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);

    var fs_artifacts = try artifacts_mod.FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_wal = try wal_mod.FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_progress = try catalog_mod.FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    const encoded = try api_codec.encodeMutationAlloc(alloc, .{
        .kind = .upsert,
        .doc_id = "doc-multi",
        .body = "{\"text\":\"alpha\",\"_embeddings\":{\"semantic_a\":[1,0,0],\"semantic_b\":[0,1,0],\"sparse_a\":{\"alpha\":1.5},\"sparse_b\":{\"bravo\":2.0}}}",
    });
    defer alloc.free(encoded);
    _ = try wal_store.append("docs", 100, encoded);

    var sources = try search_sources.publishedSearchSourcesForIndexesJsonAlloc(
        alloc,
        "{\"full_text_index_v0\":{\"type\":\"full_text\"},\"semantic_a\":{\"type\":\"embeddings\",\"dimension\":3},\"semantic_b\":{\"type\":\"embeddings\",\"dimension\":3},\"sparse_a\":{\"type\":\"embeddings\",\"sparse\":true},\"sparse_b\":{\"type\":\"embeddings\",\"sparse\":true}}",
    );
    defer search_sources.deinitPublishedSearchSources(alloc, &sources);

    var builder = Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var result = try builder.publishNamespaceWithMetricAndSearchSources("docs", .cosine, sources);
    defer result.deinit(alloc);
    try std.testing.expect(result.published);

    var manifest = try manifest_store.getAlloc("docs", 1);
    defer manifest.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 2), manifest.stats.vector_segment_count);
    try std.testing.expectEqual(@as(u32, 2), manifest.stats.sparse_segment_count);
    try std.testing.expect(findNamedArtifactIndex(manifest, .vector_segment, "semantic_a") != null);
    try std.testing.expect(findNamedArtifactIndex(manifest, .vector_segment, "semantic_b") != null);
    try std.testing.expect(findNamedArtifactIndex(manifest, .sparse_segment, "sparse_a") != null);
    try std.testing.expect(findNamedArtifactIndex(manifest, .sparse_segment, "sparse_b") != null);
}

test "builder honors per-index vector distance metrics for named embeddings" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-multi-vector-metrics");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-multi-vector-metrics");
    const wal_root = tmpPath(&wal_root_buf, "wal-multi-vector-metrics");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);

    var fs_artifacts = try artifacts_mod.FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_wal = try wal_mod.FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_progress = try catalog_mod.FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    const encoded = try api_codec.encodeMutationAlloc(alloc, .{
        .kind = .upsert,
        .doc_id = "doc-metric",
        .body = "{\"text\":\"alpha\",\"_embeddings\":{\"semantic_ip\":[1,0,0],\"semantic_l2\":[0,1,0]}}",
    });
    defer alloc.free(encoded);
    _ = try wal_store.append("docs", 100, encoded);

    var sources = try search_sources.publishedSearchSourcesForIndexesJsonAlloc(
        alloc,
        "{\"full_text_index_v0\":{\"type\":\"full_text\"},\"semantic_ip\":{\"type\":\"embeddings\",\"dimension\":3,\"distance_metric\":\"inner_product\"},\"semantic_l2\":{\"type\":\"embeddings\",\"dimension\":3,\"distance_metric\":\"l2_squared\"}}",
    );
    defer search_sources.deinitPublishedSearchSources(alloc, &sources);

    var builder = Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var result = try builder.publishNamespaceWithMetricAndSearchSources("docs", .cosine, sources);
    defer result.deinit(alloc);
    try std.testing.expect(result.published);

    var manifest = try manifest_store.getAlloc("docs", 1);
    defer manifest.deinit(alloc);

    const ip_artifact = manifest.artifacts[findNamedArtifactIndex(manifest, .vector_segment, "semantic_ip").?];
    const l2_artifact = manifest.artifacts[findNamedArtifactIndex(manifest, .vector_segment, "semantic_l2").?];

    const ip_payload = try artifact_store.getAlloc(ip_artifact.artifact_id);
    defer alloc.free(ip_payload);
    const l2_payload = try artifact_store.getAlloc(l2_artifact.artifact_id);
    defer alloc.free(l2_payload);

    const ip_header = try vector_segment_mod.decodeHeader(ip_payload[0..vector_segment_mod.header_len]);
    const l2_header = try vector_segment_mod.decodeHeader(l2_payload[0..vector_segment_mod.header_len]);
    try std.testing.expectEqual(shared_vector.DistanceMetric.inner_product, ip_header.metric);
    try std.testing.expectEqual(shared_vector.DistanceMetric.l2_squared, l2_header.metric);
}

test "builder applies named vector build policies per source" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-named-vector-policies");
    defer cleanupTmp(artifact_root);

    var fs_artifacts = try artifacts_mod.FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    const docs = try alloc.alloc(query_mod.QueryMaterializedDocument, 6);
    defer query_mod.freeMaterializedDocuments(alloc, docs);
    for (docs, 0..) |*doc, idx| {
        doc.* = .{
            .doc_id = try std.fmt.allocPrint(alloc, "doc-{d}", .{idx}),
            .body = try std.fmt.allocPrint(
                alloc,
                "{{\"text\":\"doc {d}\",\"_embeddings\":{{\"semantic_a\":[{d}.0,0.0],\"semantic_b\":[0.0,{d}.0]}}}}",
                .{ idx, idx + 1, idx + 1 },
            ),
            .last_lsn = @intCast(idx + 1),
            .last_timestamp_ns = @intCast(100 + idx),
        };
    }

    var sources = try search_sources.publishedSearchSourcesForDefinitionListsAlloc(
        alloc,
        search_sources.default_full_text_index_name,
        &.{search_sources.default_full_text_index_name},
        &.{ "semantic_a", "semantic_b" },
        &.{},
    );
    defer search_sources.deinitPublishedSearchSources(alloc, &sources);

    const policies = [_]NamedVectorBuildPolicy{
        .{ .index_name = "semantic_a", .policy = .{ .target_cluster_count = 1 } },
        .{ .index_name = "semantic_b", .policy = .{ .target_cluster_count = 4 } },
    };

    const refs = try buildVectorArtifactRefsForMaterializedDocsAlloc(
        alloc,
        &artifact_store,
        null,
        .cosine,
        &.{},
        docs,
        &.{},
        null,
        policies[0..],
        sources,
    );
    defer freeArtifactRefs(alloc, refs);

    try std.testing.expectEqual(@as(usize, 2), refs.len);

    var semantic_a_ref: ?manifest_mod.ArtifactRef = null;
    var semantic_b_ref: ?manifest_mod.ArtifactRef = null;
    for (refs) |ref| {
        if (std.mem.eql(u8, ref.name, "semantic_a")) semantic_a_ref = ref;
        if (std.mem.eql(u8, ref.name, "semantic_b")) semantic_b_ref = ref;
    }
    try std.testing.expect(semantic_a_ref != null);
    try std.testing.expect(semantic_b_ref != null);

    const payload_a = try artifact_store.getAlloc(semantic_a_ref.?.artifact_id);
    defer alloc.free(payload_a);
    const payload_b = try artifact_store.getAlloc(semantic_b_ref.?.artifact_id);
    defer alloc.free(payload_b);
    const header_a = try vector_segment_mod.decodeHeader(payload_a[0..vector_segment_mod.header_len]);
    const header_b = try vector_segment_mod.decodeHeader(payload_b[0..vector_segment_mod.header_len]);
    try std.testing.expectEqual(@as(u32, 1), header_a.cluster_count);
    try std.testing.expectEqual(@as(u32, 4), header_b.cluster_count);
}

test "builder reuses named vector and sparse artifacts when wal updates do not change those sources" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-named-reuse");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-named-reuse");
    const wal_root = tmpPath(&wal_root_buf, "wal-named-reuse");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);

    var fs_artifacts = try artifacts_mod.FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_wal = try wal_mod.FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_progress = try catalog_mod.FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var sources = try search_sources.publishedSearchSourcesForDefinitionListsAlloc(
        alloc,
        search_sources.default_full_text_index_name,
        &.{search_sources.default_full_text_index_name},
        &.{"semantic_a"},
        &.{"sparse_a"},
    );
    defer search_sources.deinitPublishedSearchSources(alloc, &sources);

    var builder = Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);

    const first = try api_codec.encodeMutationAlloc(alloc, .{
        .kind = .upsert,
        .doc_id = "doc-a",
        .body = "{\"text\":\"alpha\",\"_embeddings\":{\"semantic_a\":[1,0,0],\"sparse_a\":{\"alpha\":1.0}}}",
    });
    defer alloc.free(first);
    _ = try wal_store.append("docs", 100, first);

    var first_result = try builder.publishNamespaceWithMetricAndSearchSources("docs", .cosine, sources);
    defer first_result.deinit(alloc);
    try std.testing.expect(first_result.published);

    var first_manifest = try manifest_store.getAlloc("docs", 1);
    defer first_manifest.deinit(alloc);
    const first_vector_id = first_manifest.artifacts[findNamedArtifactIndex(first_manifest, .vector_segment, "semantic_a").?].artifact_id;
    const first_sparse_id = first_manifest.artifacts[findNamedArtifactIndex(first_manifest, .sparse_segment, "sparse_a").?].artifact_id;

    const second = try api_codec.encodeMutationAlloc(alloc, .{
        .kind = .upsert,
        .doc_id = "doc-a",
        .body = "{\"text\":\"beta\",\"_embeddings\":{\"semantic_a\":[1,0,0],\"sparse_a\":{\"alpha\":1.0}}}",
    });
    defer alloc.free(second);
    _ = try wal_store.append("docs", 200, second);

    var second_result = try builder.publishNamespaceWithMetricAndSearchSources("docs", .cosine, sources);
    defer second_result.deinit(alloc);
    try std.testing.expect(second_result.published);

    var second_manifest = try manifest_store.getAlloc("docs", 2);
    defer second_manifest.deinit(alloc);
    try std.testing.expectEqualStrings(first_vector_id, second_manifest.artifacts[findNamedArtifactIndex(second_manifest, .vector_segment, "semantic_a").?].artifact_id);
    try std.testing.expectEqualStrings(first_sparse_id, second_manifest.artifacts[findNamedArtifactIndex(second_manifest, .sparse_segment, "sparse_a").?].artifact_id);
}

test "builder rebuilds named vector and sparse artifacts when wal updates change those sources" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-named-rebuild");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-named-rebuild");
    const wal_root = tmpPath(&wal_root_buf, "wal-named-rebuild");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);

    var fs_artifacts = try artifacts_mod.FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_wal = try wal_mod.FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_progress = try catalog_mod.FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var sources = try search_sources.publishedSearchSourcesForDefinitionListsAlloc(
        alloc,
        search_sources.default_full_text_index_name,
        &.{search_sources.default_full_text_index_name},
        &.{"semantic_a"},
        &.{"sparse_a"},
    );
    defer search_sources.deinitPublishedSearchSources(alloc, &sources);

    var builder = Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);

    const first = try api_codec.encodeMutationAlloc(alloc, .{
        .kind = .upsert,
        .doc_id = "doc-a",
        .body = "{\"text\":\"alpha\",\"_embeddings\":{\"semantic_a\":[1,0,0],\"sparse_a\":{\"alpha\":1.0}}}",
    });
    defer alloc.free(first);
    _ = try wal_store.append("docs", 100, first);

    var first_result = try builder.publishNamespaceWithMetricAndSearchSources("docs", .cosine, sources);
    defer first_result.deinit(alloc);
    try std.testing.expect(first_result.published);

    var first_manifest = try manifest_store.getAlloc("docs", 1);
    defer first_manifest.deinit(alloc);
    const first_vector_id = first_manifest.artifacts[findNamedArtifactIndex(first_manifest, .vector_segment, "semantic_a").?].artifact_id;
    const first_sparse_id = first_manifest.artifacts[findNamedArtifactIndex(first_manifest, .sparse_segment, "sparse_a").?].artifact_id;

    const second = try api_codec.encodeMutationAlloc(alloc, .{
        .kind = .upsert,
        .doc_id = "doc-a",
        .body = "{\"text\":\"beta\",\"_embeddings\":{\"semantic_a\":[0,1,0],\"sparse_a\":{\"beta\":2.0}}}",
    });
    defer alloc.free(second);
    _ = try wal_store.append("docs", 200, second);

    var second_result = try builder.publishNamespaceWithMetricAndSearchSources("docs", .cosine, sources);
    defer second_result.deinit(alloc);
    try std.testing.expect(second_result.published);

    var second_manifest = try manifest_store.getAlloc("docs", 2);
    defer second_manifest.deinit(alloc);
    try std.testing.expect(!std.mem.eql(u8, first_vector_id, second_manifest.artifacts[findNamedArtifactIndex(second_manifest, .vector_segment, "semantic_a").?].artifact_id));
    try std.testing.expect(!std.mem.eql(u8, first_sparse_id, second_manifest.artifacts[findNamedArtifactIndex(second_manifest, .sparse_segment, "sparse_a").?].artifact_id));
}

test "builder reuses full text artifact when wal updates do not change indexed text" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-text-reuse");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-text-reuse");
    const wal_root = tmpPath(&wal_root_buf, "wal-text-reuse");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);

    var fs_artifacts = try artifacts_mod.FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_wal = try wal_mod.FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_progress = try catalog_mod.FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var builder = Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);

    const first = try api_codec.encodeMutationAlloc(alloc, .{
        .kind = .upsert,
        .doc_id = "doc-a",
        .body = "{\"text\":\"alpha\",\"embedding\":[1,0,0]}",
    });
    defer alloc.free(first);
    _ = try wal_store.append("docs", 100, first);

    var first_result = try builder.publishNamespace("docs");
    defer first_result.deinit(alloc);
    try std.testing.expect(first_result.published);

    var first_manifest = try manifest_store.getAlloc("docs", 1);
    defer first_manifest.deinit(alloc);
    const first_text_id = first_manifest.artifacts[findNamedArtifactIndex(first_manifest, .text_segment, search_sources.default_full_text_index_name).?].artifact_id;

    const second = try api_codec.encodeMutationAlloc(alloc, .{
        .kind = .upsert,
        .doc_id = "doc-a",
        .body = "{\"text\":\"alpha\",\"embedding\":[0,1,0]}",
    });
    defer alloc.free(second);
    _ = try wal_store.append("docs", 200, second);

    var second_result = try builder.publishNamespace("docs");
    defer second_result.deinit(alloc);
    try std.testing.expect(second_result.published);

    var second_manifest = try manifest_store.getAlloc("docs", 2);
    defer second_manifest.deinit(alloc);
    try std.testing.expectEqualStrings(first_text_id, second_manifest.artifacts[findNamedArtifactIndex(second_manifest, .text_segment, search_sources.default_full_text_index_name).?].artifact_id);
}

test "builder rebuilds full text artifact when wal updates change indexed text" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-text-rebuild");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-text-rebuild");
    const wal_root = tmpPath(&wal_root_buf, "wal-text-rebuild");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);

    var fs_artifacts = try artifacts_mod.FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_wal = try wal_mod.FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_progress = try catalog_mod.FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var builder = Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);

    const first = try api_codec.encodeMutationAlloc(alloc, .{
        .kind = .upsert,
        .doc_id = "doc-a",
        .body = "{\"text\":\"alpha\",\"embedding\":[1,0,0]}",
    });
    defer alloc.free(first);
    _ = try wal_store.append("docs", 100, first);

    var first_result = try builder.publishNamespace("docs");
    defer first_result.deinit(alloc);
    try std.testing.expect(first_result.published);

    var first_manifest = try manifest_store.getAlloc("docs", 1);
    defer first_manifest.deinit(alloc);
    const first_text_id = first_manifest.artifacts[findNamedArtifactIndex(first_manifest, .text_segment, search_sources.default_full_text_index_name).?].artifact_id;

    const second = try api_codec.encodeMutationAlloc(alloc, .{
        .kind = .upsert,
        .doc_id = "doc-a",
        .body = "{\"text\":\"beta\",\"embedding\":[1,0,0]}",
    });
    defer alloc.free(second);
    _ = try wal_store.append("docs", 200, second);

    var second_result = try builder.publishNamespace("docs");
    defer second_result.deinit(alloc);
    try std.testing.expect(second_result.published);

    var second_manifest = try manifest_store.getAlloc("docs", 2);
    defer second_manifest.deinit(alloc);
    try std.testing.expect(!std.mem.eql(u8, first_text_id, second_manifest.artifacts[findNamedArtifactIndex(second_manifest, .text_segment, search_sources.default_full_text_index_name).?].artifact_id));
}

test "builder publishes graph segment when document body carries graph edges" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-graph");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-graph");
    const wal_root = tmpPath(&wal_root_buf, "wal-graph");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);

    var fs_artifacts = try artifacts_mod.FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_wal = try wal_mod.FsStore.init(alloc, std.mem.span(wal_root));
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

    var builder = Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var result = try builder.publishNamespace("docs");
    defer result.deinit(alloc);
    try std.testing.expect(result.published);

    var manifest = try manifest_store.getAlloc("docs", 1);
    defer manifest.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 1), manifest.stats.graph_segment_count);
    try std.testing.expect(findArtifactIndex(manifest, .graph_segment) != null);
}

test "builder publishes named graph segments for graph indexes" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-graph-named");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-graph-named");
    const wal_root = tmpPath(&wal_root_buf, "wal-graph-named");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);

    var fs_artifacts = try artifacts_mod.FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_wal = try wal_mod.FsStore.init(alloc, std.mem.span(wal_root));
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

    var builder = Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var result = try builder.publishNamespaceWithMetricAndPlan("docs", .cosine, .{
        .targets = .{
            .published_search_sources = search_sources.defaultPublishedSearchSources(),
            .include_graph = true,
        },
        .table_definition = .{
            .indexes_json = indexes_json,
        },
    });
    defer result.deinit(alloc);
    try std.testing.expect(result.published);

    var manifest = try manifest_store.getAlloc("docs", 1);
    defer manifest.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 2), manifest.stats.graph_segment_count);
    const graph_a_index = findNamedArtifactIndex(manifest, .graph_segment, "graph_a").?;
    const graph_b_index = findNamedArtifactIndex(manifest, .graph_segment, "graph_b").?;
    try std.testing.expectEqualStrings(
        manifest.artifacts[graph_a_index].artifact_id,
        manifest.artifacts[graph_b_index].artifact_id,
    );
}

test "builder reuses graph artifact when wal updates do not change graph projection" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-graph-reuse");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-graph-reuse");
    const wal_root = tmpPath(&wal_root_buf, "wal-graph-reuse");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);

    var fs_artifacts = try artifacts_mod.FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_wal = try wal_mod.FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_progress = try catalog_mod.FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var builder = Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);

    const first = try api_codec.encodeMutationAlloc(alloc, .{
        .kind = .upsert,
        .doc_id = "doc-g",
        .body = "{\"text\":\"alpha\",\"graph_edges\":[{\"target\":\"doc-h\",\"edge_type\":\"cites\",\"weight\":2.0}]}",
    });
    defer alloc.free(first);
    _ = try wal_store.append("docs", 100, first);

    var first_result = try builder.publishNamespace("docs");
    defer first_result.deinit(alloc);
    try std.testing.expect(first_result.published);

    var first_manifest = try manifest_store.getAlloc("docs", 1);
    defer first_manifest.deinit(alloc);
    const first_graph_index = findArtifactIndex(first_manifest, .graph_segment).?;
    const first_graph_id = first_manifest.artifacts[first_graph_index].artifact_id;

    const second = try api_codec.encodeMutationAlloc(alloc, .{
        .kind = .upsert,
        .doc_id = "doc-g",
        .body = "{\"text\":\"beta\",\"graph_edges\":[{\"target\":\"doc-h\",\"edge_type\":\"cites\",\"weight\":2.0}]}",
    });
    defer alloc.free(second);
    _ = try wal_store.append("docs", 200, second);

    var second_result = try builder.publishNamespace("docs");
    defer second_result.deinit(alloc);
    try std.testing.expect(second_result.published);

    var second_manifest = try manifest_store.getAlloc("docs", 2);
    defer second_manifest.deinit(alloc);
    const second_graph_index = findArtifactIndex(second_manifest, .graph_segment).?;
    try std.testing.expectEqualStrings(first_graph_id, second_manifest.artifacts[second_graph_index].artifact_id);
}

test "builder rebuilds graph artifact when wal updates change graph projection" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-graph-rebuild");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-graph-rebuild");
    const wal_root = tmpPath(&wal_root_buf, "wal-graph-rebuild");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);

    var fs_artifacts = try artifacts_mod.FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_wal = try wal_mod.FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_progress = try catalog_mod.FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var builder = Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);

    const first = try api_codec.encodeMutationAlloc(alloc, .{
        .kind = .upsert,
        .doc_id = "doc-g",
        .body = "{\"text\":\"alpha\",\"graph_edges\":[{\"target\":\"doc-h\",\"edge_type\":\"cites\",\"weight\":2.0}]}",
    });
    defer alloc.free(first);
    _ = try wal_store.append("docs", 100, first);

    var first_result = try builder.publishNamespace("docs");
    defer first_result.deinit(alloc);
    try std.testing.expect(first_result.published);

    var first_manifest = try manifest_store.getAlloc("docs", 1);
    defer first_manifest.deinit(alloc);
    const first_graph_index = findArtifactIndex(first_manifest, .graph_segment).?;
    const first_graph_id = first_manifest.artifacts[first_graph_index].artifact_id;

    const second = try api_codec.encodeMutationAlloc(alloc, .{
        .kind = .upsert,
        .doc_id = "doc-g",
        .body = "{\"text\":\"beta\",\"graph_edges\":[{\"target\":\"doc-z\",\"edge_type\":\"cites\",\"weight\":2.0}]}",
    });
    defer alloc.free(second);
    _ = try wal_store.append("docs", 200, second);

    var second_result = try builder.publishNamespace("docs");
    defer second_result.deinit(alloc);
    try std.testing.expect(second_result.published);

    var second_manifest = try manifest_store.getAlloc("docs", 2);
    defer second_manifest.deinit(alloc);
    const second_graph_index = findArtifactIndex(second_manifest, .graph_segment).?;
    try std.testing.expect(!std.mem.eql(u8, first_graph_id, second_manifest.artifacts[second_graph_index].artifact_id));
}

test "builder encodes delete mutations in published segment artifact" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-delete");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-delete");
    const wal_root = tmpPath(&wal_root_buf, "wal-delete");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);

    var fs_artifacts = try artifacts_mod.FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_wal = try wal_mod.FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_progress = try catalog_mod.FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    const encoded = try api_codec.encodeMutationAlloc(alloc, .{
        .kind = .delete,
        .doc_id = "doc-z",
        .body = null,
    });
    defer alloc.free(encoded);
    _ = try wal_store.append("docs", 123, encoded);

    var builder = Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var result = try builder.publishNamespace("docs");
    defer result.deinit(alloc);

    var runtime = query_mod.QueryRuntime.init(alloc, &artifact_store, &manifest_store, &progress_store);
    defer runtime.deinit();
    var session = try runtime.openHeadSession("docs");
    defer session.deinit();

    const built = try session.fetchArtifactAlloc(0);
    defer alloc.free(built);
    const decoded = try segment_mod.decodeAlloc(alloc, built);
    defer segment_mod.freeEntries(alloc, decoded);
    try std.testing.expectEqual(@as(usize, 1), decoded.len);
    try std.testing.expectEqual(@as(api_types.MutationKind, .delete), decoded[0].kind);
    try std.testing.expectEqualStrings("doc-z", decoded[0].doc_id);
    try std.testing.expectEqual(@as(?[]u8, null), decoded[0].body);
}

test "builder rebases document segment inline when mutation lineage exceeds threshold" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-inline-rebase");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-inline-rebase");
    const wal_root = tmpPath(&wal_root_buf, "wal-inline-rebase");
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

    var builder = Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var api = @import("../api/service.zig").Service.init(alloc, &wal_store, &builder);

    const first = [_]api_types.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "alpha" },
    };
    var ingest_first = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 100, .mutations = &first });
    defer ingest_first.deinit(alloc);
    var build_first = try builder.publishNamespaceWithMetricAndPlan("docs", .cosine, .{
        .targets = .{ .published_search_sources = search_sources.defaultPublishedSearchSources() },
        .policy = .{ .compaction_enabled = true, .compaction_trigger_version_count = 2 },
    });
    defer build_first.deinit(alloc);

    const second = [_]api_types.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-b", .body = "bravo" },
    };
    var ingest_second = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 200, .mutations = &second });
    defer ingest_second.deinit(alloc);
    var build_second = try builder.publishNamespaceWithMetricAndPlan("docs", .cosine, .{
        .targets = .{ .published_search_sources = search_sources.defaultPublishedSearchSources() },
        .policy = .{ .compaction_enabled = true, .compaction_trigger_version_count = 2 },
    });
    defer build_second.deinit(alloc);

    var second_manifest = try manifest_store.getAlloc("docs", 2);
    defer second_manifest.deinit(alloc);
    try std.testing.expect(findArtifactIndex(second_manifest, .mutation_segment) != null);
    const reused_document_ref = second_manifest.artifacts[findArtifactIndex(second_manifest, .document_segment).?];
    try std.testing.expectEqual(@as(u64, 1), second_manifest.stats.document_base_version);
    try std.testing.expectEqual(catalog_types.DocumentPublishMode.append_mutation_tail, second_manifest.stats.document_publish_mode);

    const third = [_]api_types.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-c", .body = "charlie" },
    };
    var ingest_third = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 300, .mutations = &third });
    defer ingest_third.deinit(alloc);
    var build_third = try builder.publishNamespaceWithMetricAndPlan("docs", .cosine, .{
        .targets = .{ .published_search_sources = search_sources.defaultPublishedSearchSources() },
        .policy = .{ .compaction_enabled = true, .compaction_trigger_version_count = 2 },
    });
    defer build_third.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 3), build_third.version);
    try std.testing.expectEqual(@as(u64, 3), build_third.wal_start_lsn);
    try std.testing.expectEqual(@as(u64, 3), build_third.wal_end_lsn);

    var third_manifest = try manifest_store.getAlloc("docs", 3);
    defer third_manifest.deinit(alloc);
    try std.testing.expect(findArtifactIndex(third_manifest, .mutation_segment) == null);
    try std.testing.expectEqual(@as(u64, 3), third_manifest.stats.document_base_version);
    try std.testing.expectEqual(catalog_types.DocumentPublishMode.inline_rebase, third_manifest.stats.document_publish_mode);
    try std.testing.expect(!std.mem.eql(u8, reused_document_ref.artifact_id, third_manifest.artifacts[findArtifactIndex(third_manifest, .document_segment).?].artifact_id));
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
    const slice = std.fmt.bufPrint(buf, "/tmp/antfly-serverless-build-{s}-{d}-{d}\x00", .{
        label,
        nowNs(),
        nonce,
    }) catch unreachable;
    return @ptrCast(slice.ptr);
}

fn cleanupTmp(path: [*:0]const u8) void {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), std.mem.span(path)) catch {};
}
