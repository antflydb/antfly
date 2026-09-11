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
const CancellationToken = @import("../../common/cancellation.zig").CancellationToken;
const artifacts_mod = @import("../artifacts/mod.zig");
const catalog_mod = @import("../catalog/mod.zig");
const manifest_mod = @import("../manifest/mod.zig");
const manifest_base_source = @import("../manifest/base_source.zig");
const wal_mod = @import("../wal/mod.zig");
const query_mod = @import("../query/mod.zig");
const query_reader = @import("../query/indexed_reader.zig");
const document_projection = @import("../document_projection.zig");
const search_sources = @import("../search_sources.zig");
const catalog_types = @import("../catalog/types.zig");
const document_segment_mod = @import("../document_segment/mod.zig");
const graph_segment_mod = @import("../graph_segment/mod.zig");
const graph_pages = @import("../graph_segment/page_graph.zig");
const graph_page_store = @import("../graph_segment/page_store.zig");
const graph_page_tree = @import("../graph_segment/page_tree.zig");
const graph_page_keys = @import("../graph_segment/page_keys.zig");
const graph_read_lease = @import("../manifest/read_lease.zig");
const document_facts = @import("document_facts.zig");
const document_facts_builder = @import("document_facts_builder.zig");
const graph_metric_segment_mod = @import("../graph_metric_segment/mod.zig");
const segment_mod = @import("../segment/mod.zig");
const text_segment_mod = @import("../text_segment/mod.zig");
const sparse_segment_mod = @import("../sparse_segment/mod.zig");
const vector_segment_mod = @import("../vector_segment/mod.zig");
const vector_index = @import("vector_index.zig");
const graph_metric_config = @import("graph_metric_config.zig");
const graph_metric_policy = @import("graph_metric_policy.zig");
const lake_graph_metric = @import("lake_graph_metric.zig");
const graph_build_limits = @import("lake_build_limits.zig");
pub const GraphBuildLimits = graph_build_limits.Limits;
const publication_plan = @import("publication_plan.zig");
const work_lease = @import("work_lease.zig");
const maintenance_cancellation = @import("../maintenance_cancellation.zig");
const external_source_manifest = @import("external_source_manifest.zig");
const external_source_publication = @import("external_source_publication.zig");
const enrichment_pipeline = @import("../enrichment/pipeline.zig");
const enrichment_operation_id = @import("../enrichment/operation_id.zig");
const api_codec = @import("../api/codec.zig");
const api_types = @import("../api/types.zig");
const full_text_indexes = @import("../../api/full_text_indexes.zig");
const tables_api = @import("../../api/tables.zig");
const shared_vector = @import("antfly_vector").vector;
const objectstore = @import("objectstore");
const manifest_object_store = @import("../manifest/object_store.zig");

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

/// Production-neutral suspension and observation seam for an immutable
/// manifest publication. The hook runs after the candidate manifest is
/// durable and before the progress-store compare-and-swap makes it visible.
/// It is unset in production; deterministic runtimes may park here to expose
/// a concurrent generation change without replacing either storage owner.
pub const PublicationLifecycleEvent = struct {
    namespace: []const u8,
    expected_head: ?u64,
    candidate_version: u64,
};

pub const PublicationLifecycleHook = struct {
    ptr: *anyopaque,
    reach_fn: *const fn (ptr: *anyopaque, event: PublicationLifecycleEvent) anyerror!void,

    pub fn reach(self: PublicationLifecycleHook, event: PublicationLifecycleEvent) !void {
        try self.reach_fn(self.ptr, event);
    }
};

pub const Builder = struct {
    alloc: Allocator,
    artifacts: *artifacts_mod.ArtifactStore,
    manifests: *manifest_mod.ManifestStore,
    progress: *catalog_mod.ProgressStore,
    wal: *wal_mod.WalStore,
    io: ?std.Io = null,
    graph_metric_max_parallelism: usize = lake_graph_metric.default_compute_parallelism,
    publication_lifecycle_hook: ?PublicationLifecycleHook = null,

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

    pub fn setIo(self: *Builder, io: ?std.Io) void {
        self.io = io;
    }

    pub fn setGraphMetricMaxParallelism(self: *Builder, max_parallelism: usize) !void {
        if (max_parallelism == 0 or max_parallelism > lake_graph_metric.max_compute_parallelism) return error.InvalidGraphMetricBuildOptions;
        self.graph_metric_max_parallelism = max_parallelism;
    }

    fn realtimeNs(self: *const Builder) u64 {
        if (self.io) |io| return @intCast(std.Io.Timestamp.now(io, .real).toNanoseconds());
        var io_impl = threadedIo();
        defer io_impl.deinit();
        return @intCast(std.Io.Timestamp.now(io_impl.io(), .real).toNanoseconds());
    }

    pub fn setPublicationLifecycleHook(
        self: *Builder,
        hook: ?PublicationLifecycleHook,
    ) void {
        self.publication_lifecycle_hook = hook;
    }

    fn compareAndSwapPublishedHead(
        self: *Builder,
        namespace: []const u8,
        expected: ?u64,
        candidate_version: u64,
        publication_guard: ?work_lease.PublicationGuard,
        cancellation: ?maintenance_cancellation.Token,
    ) !bool {
        if (self.publication_lifecycle_hook) |hook| try hook.reach(.{
            .namespace = namespace,
            .expected_head = expected,
            .candidate_version = candidate_version,
        });
        try maintenance_cancellation.check(cancellation);
        return try compareAndSwapHeadGuarded(
            self.progress,
            namespace,
            expected,
            candidate_version,
            publication_guard,
        );
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
        return try self.publishNamespaceWithMetricAndPlanGuarded(
            namespace,
            vector_metric,
            plan,
            null,
        );
    }

    pub fn publishNamespaceWithMetricAndPlanGuarded(
        self: *Builder,
        namespace: []const u8,
        vector_metric: shared_vector.DistanceMetric,
        plan: publication_plan.TablePublicationPlan,
        publication_guard: ?work_lease.PublicationGuard,
    ) !BuildResult {
        return try self.publishNamespaceWithMetricAndPlanGuardedUntil(
            namespace,
            vector_metric,
            plan,
            publication_guard,
            null,
        );
    }

    pub fn publishNamespaceWithMetricAndPlanWithCancellation(
        self: *Builder,
        namespace: []const u8,
        vector_metric: shared_vector.DistanceMetric,
        plan: publication_plan.TablePublicationPlan,
        cancellation: CancellationToken,
    ) !BuildResult {
        var fallback: ?std.Io.Threaded = if (self.io == null) threadedIo() else null;
        defer if (fallback) |*value| value.deinit();
        return self.publishNamespaceWithMetricAndPlanGuardedUntil(namespace, vector_metric, plan, null, .{
            .io = self.io orelse fallback.?.io(),
            .cooperative = cancellation,
        });
    }

    pub fn publishNamespaceWithMetricAndPlanGuardedUntil(
        self: *Builder,
        namespace: []const u8,
        vector_metric: shared_vector.DistanceMetric,
        plan: publication_plan.TablePublicationPlan,
        publication_guard: ?work_lease.PublicationGuard,
        cancellation: ?maintenance_cancellation.Token,
    ) !BuildResult {
        var fallback: ?std.Io.Threaded = if (self.io == null and cancellation == null) threadedIo() else null;
        defer if (fallback) |*value| value.deinit();
        const io = if (cancellation) |token| token.io else self.io orelse fallback.?.io();
        if (publication_guard == null) {
            var owner_bytes: [16]u8 = undefined;
            io.random(&owner_bytes);
            const owner = std.fmt.bytesToHex(&owner_bytes, .lower);
            var held = (try work_lease.acquireHeld(try self.progress.workLeaseProvider(), io, namespace, &owner, 30 * std.time.ns_per_s)) orelse
                return error.WorkLeaseLost;
            defer _ = held.release() catch false;
            return self.publishNamespaceWithMetricAndPlanGuardedUntil(namespace, vector_metric, plan, held.guard(), held.cancellation(cancellation orelse .{ .io = io }));
        }
        var protection = try GraphSourceProtection.init(self.progress, namespace, cancellation);
        var scoped_artifacts = self.artifacts.*;
        scoped_artifacts.upload_scope = .{
            .domain = graph_page_store.PageStore.namespaceDomain(namespace),
            .attempt = try graphPublicationAttempt(publication_guard, namespace, io),
        };
        var publisher = self.*;
        publisher.artifacts = &scoped_artifacts;
        return publisher.publishNamespaceWithMetricAndPlanPinnedUntil(namespace, vector_metric, plan, publication_guard, protection.token(io)) catch |err| return graphPublicationError(err, false);
    }

    fn publishNamespaceWithMetricAndPlanPinnedUntil(
        self: *Builder,
        namespace: []const u8,
        vector_metric: shared_vector.DistanceMetric,
        plan: publication_plan.TablePublicationPlan,
        publication_guard: ?work_lease.PublicationGuard,
        cancellation: ?maintenance_cancellation.Token,
    ) !BuildResult {
        try maintenance_cancellation.check(cancellation);
        const targets = plan.targets;
        var head = try self.loadCurrentHeadManifestAlloc(namespace);
        defer head.deinit(self.alloc);
        const current_head = head.progress_version;
        const current_manifest = head.manifest;

        const next_version: u64 = current_head + 1;
        const start_lsn: u64 = if (current_manifest) |current| current.wal_end_lsn + 1 else 1;

        const records = try self.wal.readFromAlloc(namespace, start_lsn);
        defer wal_mod.freeRecords(self.alloc, records);
        try maintenance_cancellation.check(cancellation);

        const applicable_records = try applicableWalRecordsForHeadAlloc(self.alloc, records, current_head);
        defer self.alloc.free(applicable_records);

        if (applicable_records.len != 0 and publicationPlanHasExternalBaseSource(plan)) {
            return error.ExternalTableReadOnly;
        }

        if (records.len == 0) {
            if (plan.external_source_plan) |external_plan| {
                if (current_manifest == null or !externalSourcePlanMatchesManifest(external_plan, current_manifest.?) or plan.forceRepublishFromHead()) {
                    return try self.publishExternalManifestWithoutWal(
                        namespace,
                        current_head,
                        next_version,
                        start_lsn,
                        plan,
                        publication_guard,
                        cancellation,
                        null,
                    );
                }
            } else if (current_head == 0 and plan.table_definition.base_source != null) {
                return try self.publishExternalManifestWithoutWal(
                    namespace,
                    current_head,
                    next_version,
                    start_lsn,
                    plan,
                    publication_guard,
                    cancellation,
                    null,
                );
            }
            if (current_head != 0 and plan.forceRepublishFromHead()) {
                return try self.republishHeadWithPlan(
                    namespace,
                    current_head,
                    vector_metric,
                    plan,
                    publication_guard,
                    cancellation,
                    null,
                );
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

        // A derived mutation is scoped to the immutable HEAD from which its
        // full document body was produced. Normal publishers and enrichers
        // share a namespace lease, but this durable check also covers a crash,
        // an administrative HEAD publication, or a mixed-version edge. Consume
        // stale records without rebuilding unchanged artifacts so they cannot
        // wedge an external/read-only table or overwrite a newer generation.
        if (applicable_records.len == 0) {
            const current = current_manifest orelse return error.StaleEnrichmentWithoutPublishedHead;
            const last_record = records[records.len - 1];
            if (plan.external_source_plan) |external_plan| {
                if (!externalSourcePlanMatchesManifest(external_plan, current) or plan.forceRepublishFromHead()) {
                    return try self.publishExternalManifestWithoutWal(
                        namespace,
                        current_head,
                        next_version,
                        start_lsn,
                        plan,
                        publication_guard,
                        cancellation,
                        last_record,
                    );
                }
            }
            if (plan.forceRepublishFromHead()) {
                return try self.republishHeadWithPlan(
                    namespace,
                    current_head,
                    vector_metric,
                    plan,
                    publication_guard,
                    cancellation,
                    last_record,
                );
            }
            return try self.publishHeadConsumingIgnoredWal(
                namespace,
                current_head,
                current,
                last_record,
                publication_guard,
                cancellation,
            );
        }

        const inline_document_rebase = if (current_manifest) |current|
            findArtifactIndex(current, .document_facts) == null and shouldInlineDocumentRebase(current, next_version, plan.policy)
        else
            false;

        const mutation_entries = if (inline_document_rebase)
            try self.alloc.alloc(segment_mod.Entry, 0)
        else if (current_manifest) |current|
            if (findArtifactIndex(current, .document_facts) != null)
                try allocMutationEntriesFromRecords(self.alloc, applicable_records)
            else
                try mergeManifestMutationEntriesWithRecordsAlloc(self.alloc, self.artifacts, current, applicable_records)
        else
            try allocMutationEntriesFromRecords(self.alloc, applicable_records);
        defer segment_mod.freeEntries(self.alloc, mutation_entries);
        try maintenance_cancellation.check(cancellation);

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
        try maintenance_cancellation.check(cancellation);

        // The normalized page plan is the graph impact oracle. Do not parse
        // before/after JSON here and then parse it again during publication.
        const materialized = try materializeWalDocumentsForPublicationAlloc(self, namespace, current_head, current_manifest, applicable_records, plan, cancellation);
        defer freeMaterializerMutations(self.alloc, materialized.mutations);
        defer query_mod.freeMaterializedDocuments(self.alloc, materialized.base_documents);
        defer query_mod.freeMaterializedDocuments(self.alloc, materialized.documents);
        try maintenance_cancellation.check(cancellation);

        const document_ref = blk: {
            if (materialized.partial) {
                const idx = findArtifactIndex(current_manifest.?, .document_segment) orelse return error.DocumentSegmentNotFound;
                break :blk try cloneArtifactRefAlloc(self.alloc, current_manifest.?.artifacts[idx]);
            }
            const entries = try allocDocumentSegmentEntries(self.alloc, materialized.documents);
            defer document_segment_mod.freeEntries(self.alloc, entries);
            const payload = try document_segment_mod.encodeAlloc(self.alloc, entries);
            defer self.alloc.free(payload);
            var artifact = try self.artifacts.put(payload);
            defer artifact.deinit(self.alloc);
            break :blk try artifactRefFromMetadataAlloc(self.alloc, .document_segment, artifact);
        };
        defer freeArtifactRef(self.alloc, document_ref);
        try maintenance_cancellation.check(cancellation);
        const text_index_specs = try resolvePublishedTextIndexSpecsAlloc(self.alloc, plan.table_definition, plan.full_text_index_actions);
        defer full_text_indexes.freeFullTextIndexSpecs(self.alloc, text_index_specs);
        const text_refs = try buildTextArtifactRefsForMaterializedDocsAllocUntil(
            self.alloc,
            self.artifacts,
            current_manifest,
            materialized.base_documents,
            materialized.documents,
            materialized.mutations,
            text_index_specs,
            cancellation,
        );
        defer freeArtifactRefs(self.alloc, text_refs);
        try maintenance_cancellation.check(cancellation);
        const sparse_refs = try buildSparseArtifactRefsForMaterializedDocsAllocUntil(
            self.alloc,
            self.artifacts,
            current_manifest,
            materialized.base_documents,
            materialized.documents,
            materialized.mutations,
            targets.published_search_sources,
            cancellation,
        );
        defer freeArtifactRefs(self.alloc, sparse_refs);
        try maintenance_cancellation.check(cancellation);
        const vector_refs = try buildVectorArtifactRefsForMaterializedDocsAllocUntil(
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
            cancellation,
        );
        defer freeArtifactRefs(self.alloc, vector_refs);
        try maintenance_cancellation.check(cancellation);
        const graph_index_names = try listGraphIndexNamesAlloc(self.alloc, plan.table_definition.indexes_json);
        defer freeOwnedStrings(self.alloc, graph_index_names);
        const graph_refs = try buildGraphArtifactRefsFromImpactAllocUntil(
            self.alloc,
            self.artifacts,
            namespace,
            current_manifest,
            materialized.documents,
            materialized.mutations,
            true,
            graph_index_names,
            targets.include_graph,
            try graphPublicationAttempt(publication_guard, namespace, cancellation.?.io),
            cancellation,
        );
        defer freeArtifactRefs(self.alloc, graph_refs);
        try maintenance_cancellation.check(cancellation);
        const graph_metric_specs = if (targets.include_graph)
            try graph_metric_config.parseIndexSpecsAlloc(self.alloc, plan.table_definition.indexes_json)
        else
            try self.alloc.alloc(graph_metric_config.IndexSpec, 0);
        defer graph_metric_config.freeIndexSpecs(self.alloc, graph_metric_specs);
        const built_at_ns = records[records.len - 1].timestamp_ns;
        const graph_metric_refs = try buildGraphMetricArtifactRefsAlloc(self.alloc, self.artifacts, current_manifest, graph_refs, graph_metric_specs, cancellation, .{
            .published_generation = next_version,
            .edge_generation = next_version,
            .computed_at_ms = @divTrunc(self.realtimeNs(), std.time.ns_per_ms),
        }, self.io, self.graph_metric_max_parallelism);
        defer freeArtifactRefs(self.alloc, graph_metric_refs);
        const published_graph_refs = try concatArtifactRefSlicesAlloc(self.alloc, graph_refs, graph_metric_refs);
        defer self.alloc.free(published_graph_refs);

        const wal_end_lsn = records[records.len - 1].lsn;
        var derived_outputs = if (materialized.counts) |counts|
            try derivedOutputsFromFactCountsAlloc(self.alloc, counts)
        else
            try detectMaterializedDerivedOutputsAlloc(
                self.alloc,
                materialized.documents,
                plan.table_definition.indexes_json,
                .{},
            );
        defer search_sources.deinitMaterializedDerivedOutputs(self.alloc, &derived_outputs);
        try maintenance_cancellation.check(cancellation);
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
                materialized.document_count,
                document_base_version,
                .inline_rebase,
                document_ref,
                text_refs,
                sparse_refs,
                vector_refs,
                published_graph_refs,
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
                materialized.document_count,
                document_base_version,
                .append_mutation_tail,
                mutation_artifact.?,
                document_ref,
                text_refs,
                sparse_refs,
                vector_refs,
                published_graph_refs,
                derived_outputs,
                plan.policy,
                plan.table_definition,
            );
        defer manifest.deinit(self.alloc);
        try publishDocumentFactsForManifest(self.alloc, self.artifacts, &manifest, current_manifest, materialized.documents, materialized.mutations, publication_guard, cancellation);
        try attachResolvedExternalSourcePlanIfPresent(self.alloc, &manifest, plan);
        try maintenance_cancellation.check(cancellation);

        try stampPublicationFence(&manifest, publication_guard);
        const published_version = try putManifestForPublication(
            self.manifests,
            &manifest,
            if (current_head == 0) null else current_head,
        );
        const published = try self.compareAndSwapPublishedHead(
            namespace,
            if (current_head == 0) null else current_head,
            published_version,
            publication_guard,
            cancellation,
        );
        if (!published) return error.HeadChanged;

        return .{
            .namespace = try self.alloc.dupe(u8, namespace),
            .published = true,
            .version = published_version,
            .wal_start_lsn = if (inline_document_rebase) wal_end_lsn else start_lsn,
            .wal_end_lsn = wal_end_lsn,
            .artifact_count = manifest.artifacts.len,
        };
    }

    fn publishHeadConsumingIgnoredWal(
        self: *Builder,
        namespace: []const u8,
        current_head: u64,
        current: manifest_mod.Manifest,
        last_record: wal_mod.Record,
        publication_guard: ?work_lease.PublicationGuard,
        cancellation: ?maintenance_cancellation.Token,
    ) !BuildResult {
        try maintenance_cancellation.check(cancellation);
        var manifest = try manifest_mod.cloneManifest(self.alloc, current);
        defer manifest.deinit(self.alloc);
        manifest.version = std.math.add(u64, current_head, 1) catch return error.ManifestVersionExhausted;
        manifest.built_at_ns = last_record.timestamp_ns;
        // The cloned artifacts are already a complete published snapshot and
        // do not depend on any of the ignored mutations. Advance the retained
        // WAL boundary with the consumed tail so repeated stale work cannot
        // pin the namespace's entire historical log.
        manifest.wal_start_lsn = last_record.lsn;
        manifest.wal_end_lsn = last_record.lsn;

        if (findArtifactIndex(current, .document_facts) != null)
            try publishDocumentFactsForManifest(self.alloc, self.artifacts, &manifest, current, &.{}, &.{}, publication_guard, cancellation);

        try stampPublicationFence(&manifest, publication_guard);
        const published_version = try putManifestForPublication(self.manifests, &manifest, current_head);
        const published = try self.compareAndSwapPublishedHead(
            namespace,
            current_head,
            published_version,
            publication_guard,
            cancellation,
        );
        if (!published) return error.HeadChanged;
        return .{
            .namespace = try self.alloc.dupe(u8, namespace),
            .published = true,
            .version = published_version,
            .wal_start_lsn = manifest.wal_start_lsn,
            .wal_end_lsn = manifest.wal_end_lsn,
            .artifact_count = manifest.artifacts.len,
        };
    }

    fn publishExternalManifestWithoutWal(
        self: *Builder,
        namespace: []const u8,
        current_head: u64,
        version: u64,
        start_lsn: u64,
        plan: publication_plan.TablePublicationPlan,
        publication_guard: ?work_lease.PublicationGuard,
        cancellation: ?maintenance_cancellation.Token,
        consumed_record: ?wal_mod.Record,
    ) !BuildResult {
        try maintenance_cancellation.check(cancellation);
        var manifest = try buildEmptyExternalManifestAlloc(self.alloc, namespace, version, start_lsn, plan);
        defer manifest.deinit(self.alloc);
        if (consumed_record) |record| {
            manifest.built_at_ns = record.timestamp_ns;
            manifest.wal_start_lsn = record.lsn;
            manifest.wal_end_lsn = record.lsn;
        }
        try attachResolvedExternalSourcePlanIfPresent(self.alloc, &manifest, plan);

        try maintenance_cancellation.check(cancellation);
        try stampPublicationFence(&manifest, publication_guard);
        const published_version = try putManifestForPublication(
            self.manifests,
            &manifest,
            if (current_head == 0) null else current_head,
        );
        const published = try self.compareAndSwapPublishedHead(
            namespace,
            if (current_head == 0) null else current_head,
            published_version,
            publication_guard,
            cancellation,
        );
        if (!published) return error.HeadChanged;

        return .{
            .namespace = try self.alloc.dupe(u8, namespace),
            .published = true,
            .version = published_version,
            .wal_start_lsn = manifest.wal_start_lsn,
            .wal_end_lsn = manifest.wal_end_lsn,
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
        }, null, null, null);
    }

    fn republishHeadWithPlan(
        self: *Builder,
        namespace: []const u8,
        current_head: u64,
        vector_metric: shared_vector.DistanceMetric,
        plan: publication_plan.TablePublicationPlan,
        publication_guard: ?work_lease.PublicationGuard,
        cancellation: ?maintenance_cancellation.Token,
        consumed_record: ?wal_mod.Record,
    ) !BuildResult {
        try maintenance_cancellation.check(cancellation);
        const targets = plan.targets;
        var current = try self.manifests.getAlloc(namespace, current_head);
        defer current.deinit(self.alloc);

        var docs_cache: ?[]query_mod.QueryMaterializedDocument = null;
        defer if (docs_cache) |docs| query_mod.freeMaterializedDocuments(self.alloc, docs);
        const reusable_facts = try reusableDocumentFactsAlloc(self.alloc, self.artifacts, current, plan.policy, plan.table_definition.indexes_json, cancellation);

        const document_ref = if (plan.artifact_actions.document_segment == .reuse) blk: {
            if (findArtifactIndex(current, .document_segment)) |artifact_index| {
                break :blk try cloneArtifactRefAlloc(self.alloc, current.artifacts[artifact_index]);
            }
            const docs = try ensurePublishedDocumentsAlloc(self, namespace, current_head, &docs_cache, cancellation);
            const document_entries = try allocDocumentSegmentEntries(self.alloc, docs);
            defer document_segment_mod.freeEntries(self.alloc, document_entries);
            const document_payload = try document_segment_mod.encodeAlloc(self.alloc, document_entries);
            defer self.alloc.free(document_payload);
            var document_artifact = try self.artifacts.put(document_payload);
            defer document_artifact.deinit(self.alloc);
            break :blk try artifactRefFromMetadataAlloc(self.alloc, .document_segment, document_artifact);
        } else blk: {
            const docs = try ensurePublishedDocumentsAlloc(self, namespace, current_head, &docs_cache, cancellation);
            const document_entries = try allocDocumentSegmentEntries(self.alloc, docs);
            defer document_segment_mod.freeEntries(self.alloc, document_entries);
            const document_payload = try document_segment_mod.encodeAlloc(self.alloc, document_entries);
            defer self.alloc.free(document_payload);
            var document_artifact = try self.artifacts.put(document_payload);
            defer document_artifact.deinit(self.alloc);
            break :blk try artifactRefFromMetadataAlloc(self.alloc, .document_segment, document_artifact);
        };
        defer freeArtifactRef(self.alloc, document_ref);
        try maintenance_cancellation.check(cancellation);

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
                try ensurePublishedDocumentsAlloc(self, namespace, current_head, &docs_cache, cancellation)
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
                cancellation,
            );
        };
        defer freeArtifactRefs(self.alloc, text_refs);
        try maintenance_cancellation.check(cancellation);

        const sparse_sources = try search_sources.listSparseSourcesAlloc(self.alloc, targets.published_search_sources);
        defer search_sources.freeSparseSourceDescriptors(self.alloc, sparse_sources);
        const vector_sources = try search_sources.listVectorSourcesAlloc(self.alloc, targets.published_search_sources);
        defer search_sources.freeVectorSourceDescriptors(self.alloc, vector_sources);
        const needs_flat_docs = needs: {
            for (sparse_sources) |source| {
                const action = namedArtifactActionForName(plan.sparse_index_actions, source.index_name, plan.artifact_actions.sparse_vector);
                if (action != .drop and (action != .reuse or !artifactAvailableForName(current, .sparse_segment, source.index_name))) break :needs true;
            }
            for (vector_sources) |source| {
                const action = namedArtifactActionForName(plan.vector_index_actions, source.index_name, plan.artifact_actions.dense_vector);
                if (action != .drop and (action != .reuse or !artifactAvailableForName(current, .vector_segment, source.index_name))) break :needs true;
            }
            break :needs false;
        };
        const republish_docs = if (needs_flat_docs)
            try ensurePublishedDocumentsAlloc(self, namespace, current_head, &docs_cache, cancellation)
        else
            &.{};
        const sparse_refs = try buildSparseArtifactRefsForRepublishAlloc(
            self.alloc,
            self.artifacts,
            current,
            republish_docs,
            targets.published_search_sources,
            plan.sparse_index_actions,
            plan.artifact_actions.sparse_vector,
            cancellation,
        );
        defer freeArtifactRefs(self.alloc, sparse_refs);
        try maintenance_cancellation.check(cancellation);

        const vector_refs = try buildVectorArtifactRefsForRepublishAlloc(
            self.alloc,
            self.artifacts,
            current,
            vector_metric,
            republish_docs,
            targets.published_search_sources,
            plan.vector_index_actions,
            plan.artifact_actions.dense_vector,
            cancellation,
        );
        defer freeArtifactRefs(self.alloc, vector_refs);
        try maintenance_cancellation.check(cancellation);

        const graph_index_names = try listGraphIndexNamesAlloc(self.alloc, plan.table_definition.indexes_json);
        defer freeOwnedStrings(self.alloc, graph_index_names);
        const graph_docs = if (plan.artifact_actions.graph == .drop or !targets.include_graph or
            (plan.artifact_actions.graph == .reuse and findArtifactIndex(current, .graph_segment) != null))
            &.{}
        else
            try ensurePublishedDocumentsAlloc(self, namespace, current_head, &docs_cache, cancellation);
        const graph_refs = try buildGraphArtifactRefsForRepublishAlloc(
            self.alloc,
            self.artifacts,
            namespace,
            current,
            graph_docs,
            graph_index_names,
            plan.artifact_actions.graph,
            targets.include_graph,
            try graphPublicationAttempt(publication_guard, namespace, cancellation.?.io),
            cancellation,
        );
        defer freeArtifactRefs(self.alloc, graph_refs);
        try maintenance_cancellation.check(cancellation);
        const graph_metric_specs = if (targets.include_graph)
            try graph_metric_config.parseIndexSpecsAlloc(self.alloc, plan.table_definition.indexes_json)
        else
            try self.alloc.alloc(graph_metric_config.IndexSpec, 0);
        defer graph_metric_config.freeIndexSpecs(self.alloc, graph_metric_specs);
        const next_version = current_head + 1;
        const graph_metric_refs = try buildGraphMetricArtifactRefsAlloc(self.alloc, self.artifacts, current, graph_refs, graph_metric_specs, cancellation, .{
            .published_generation = next_version,
            .edge_generation = next_version,
            .computed_at_ms = @divTrunc(self.realtimeNs(), std.time.ns_per_ms),
        }, self.io, self.graph_metric_max_parallelism);
        defer freeArtifactRefs(self.alloc, graph_metric_refs);
        const published_graph_refs = try concatArtifactRefSlicesAlloc(self.alloc, graph_refs, graph_metric_refs);
        defer self.alloc.free(published_graph_refs);

        var scanned_derived_outputs: ?search_sources.MaterializedDerivedOutputs = null;
        defer if (scanned_derived_outputs) |*outputs| search_sources.deinitMaterializedDerivedOutputs(self.alloc, outputs);
        if (plan.derived_output_actions.chunk_preview == .recompute or
            plan.derived_output_actions.chunk_embeddings == .recompute or
            plan.derived_output_actions.rerank_terms == .recompute)
        {
            const selection: DerivedOutputDetectionSelection = .{
                .chunk_preview = plan.derived_output_actions.chunk_preview == .recompute,
                .chunk_embeddings = plan.derived_output_actions.chunk_embeddings == .recompute,
                .rerank_terms = plan.derived_output_actions.rerank_terms == .recompute,
            };
            scanned_derived_outputs = if (reusable_facts) |root|
                try document_facts_builder.outputsFromCountsAlloc(self.alloc, root.counts, selection)
            else
                try detectMaterializedDerivedOutputsAlloc(self.alloc, try ensurePublishedDocumentsAlloc(self, namespace, current_head, &docs_cache, cancellation), plan.table_definition.indexes_json, selection);
        }

        var derived_outputs = try mergeDerivedOutputsAlloc(
            self.alloc,
            current.stats.derived_outputs,
            scanned_derived_outputs orelse .{},
            plan.derived_output_actions,
        );
        defer search_sources.deinitMaterializedDerivedOutputs(self.alloc, &derived_outputs);
        try maintenance_cancellation.check(cancellation);

        const wal_end_lsn = if (consumed_record) |record| record.lsn else current.wal_end_lsn;
        var manifest = try buildCompactedManifestFromRefsAlloc(
            self.alloc,
            namespace,
            next_version,
            wal_end_lsn,
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
            published_graph_refs,
            derived_outputs,
            plan.policy,
            plan.table_definition,
        );
        defer manifest.deinit(self.alloc);
        if (consumed_record) |record| manifest.built_at_ns = record.timestamp_ns;
        if (reusable_facts != null) {
            try publishDocumentFactsForManifest(self.alloc, self.artifacts, &manifest, current, &.{}, &.{}, publication_guard, cancellation);
        } else {
            const fact_docs = try ensurePublishedDocumentsAlloc(self, namespace, current_head, &docs_cache, cancellation);
            try publishDocumentFactsForManifest(self.alloc, self.artifacts, &manifest, current, fact_docs, null, publication_guard, cancellation);
        }
        try attachResolvedExternalSourcePlanIfPresent(self.alloc, &manifest, plan);

        try maintenance_cancellation.check(cancellation);
        try stampPublicationFence(&manifest, publication_guard);
        const published_version = try putManifestForPublication(
            self.manifests,
            &manifest,
            current_head,
        );
        const published = try self.compareAndSwapPublishedHead(
            namespace,
            current_head,
            published_version,
            publication_guard,
            cancellation,
        );
        if (!published) return error.HeadChanged;

        return .{
            .namespace = try self.alloc.dupe(u8, namespace),
            .published = true,
            .version = published_version,
            .wal_start_lsn = manifest.wal_start_lsn,
            .wal_end_lsn = manifest.wal_end_lsn,
            .artifact_count = manifest.artifacts.len,
        };
    }

    pub fn predictPendingWalPublicationActionsAlloc(
        self: *Builder,
        namespace: []const u8,
        vector_metric: shared_vector.DistanceMetric,
        plan: publication_plan.TablePublicationPlan,
    ) !?PredictedPublicationActions {
        return self.predictPendingWalPublicationActionsAllocUntil(namespace, vector_metric, plan, null);
    }

    pub fn predictPendingWalPublicationActionsAllocUntil(
        self: *Builder,
        namespace: []const u8,
        vector_metric: shared_vector.DistanceMetric,
        plan: publication_plan.TablePublicationPlan,
        cancellation: ?maintenance_cancellation.Token,
    ) !?PredictedPublicationActions {
        return self.predictPendingWalPublicationActionsWithLimitsAlloc(namespace, vector_metric, plan, cancellation, .{});
    }

    pub fn predictPendingWalPublicationActionsWithLimitsAlloc(
        self: *Builder,
        namespace: []const u8,
        vector_metric: shared_vector.DistanceMetric,
        plan: publication_plan.TablePublicationPlan,
        cancellation: ?maintenance_cancellation.Token,
        limits: GraphBuildLimits,
    ) !?PredictedPublicationActions {
        // Borrow storage capabilities with an operation-local result allocator.
        // Never mutate/deinit the shared store owners: concurrent queries retain
        // their allocator and lifetime. This admits WAL, manifest, document and
        // projection allocations together, before any decoded expansion.
        var working_set = try graph_build_limits.WorkingSetAllocator.init(self.alloc, limits);
        const alloc = working_set.allocator();
        var artifacts = self.artifacts.*;
        artifacts.allocator = alloc;
        var manifests = self.manifests.*;
        manifests.allocator = alloc;
        var wal = self.wal.*;
        wal.allocator = alloc;
        var bounded = self.*;
        bounded.alloc = alloc;
        bounded.artifacts = &artifacts;
        bounded.manifests = &manifests;
        bounded.wal = &wal;
        // The allocator adds no allocation header. Returned owned actions are
        // released by the caller using the original backing allocator.
        return bounded.predictPendingWalPublicationActionsBoundedAlloc(namespace, vector_metric, plan, cancellation, limits) catch |err| {
            if (err == error.OutOfMemory and working_set.limit_exceeded) return error.LakeSidecarBuildBudgetExceeded;
            return err;
        };
    }

    fn predictPendingWalPublicationActionsBoundedAlloc(
        self: *Builder,
        namespace: []const u8,
        vector_metric: shared_vector.DistanceMetric,
        plan: publication_plan.TablePublicationPlan,
        parent_cancellation: ?maintenance_cancellation.Token,
        limits: GraphBuildLimits,
    ) !?PredictedPublicationActions {
        try maintenance_cancellation.check(parent_cancellation);
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

        var fallback: ?std.Io.Threaded = if (self.io == null and parent_cancellation == null) threadedIo() else null;
        defer if (fallback) |*value| value.deinit();
        const io = if (parent_cancellation) |token| token.io else self.io orelse fallback.?.io();
        var protection = try GraphSourceProtection.initAt(self.progress, namespace, current.version, parent_cancellation);
        const cancellation = protection.token(io);
        var cancellation_bridge = maintenance_cancellation.GraphBridge{ .maintenance = cancellation };

        const start_lsn = current.wal_end_lsn + 1;
        const records = try self.wal.readFromAlloc(namespace, start_lsn);
        defer wal_mod.freeRecords(self.alloc, records);
        if (records.len == 0) return null;
        if (records.len > limits.max_rows) return error.LakeSidecarBuildBudgetExceeded;
        var input_bytes: usize = 0;
        for (records) |record| {
            try maintenance_cancellation.check(cancellation);
            input_bytes = std.math.add(usize, input_bytes, record.payload.len) catch return error.LakeSidecarBuildBudgetExceeded;
            if (input_bytes > limits.max_input_bytes) return error.LakeSidecarBuildBudgetExceeded;
        }

        const applicable_records = try applicableWalRecordsForHeadAlloc(self.alloc, records, current.version);
        defer self.alloc.free(applicable_records);
        var predicted_counts: ?[7]u64 = null;
        const BuiltDocuments = struct {
            base_documents: []query_mod.QueryMaterializedDocument,
            documents: []query_mod.QueryMaterializedDocument,
            mutations: []query_mod.QueryMaterializerMutation,
            graph_changed: bool,
        };
        // Normal scheduling touches only the IDs in this WAL interval. The
        // root carries the exact whole-corpus counters; unchanged documents do
        // not need hydration, projection parsing, or a candidate upload.
        const built_documents: BuiltDocuments = materialized: {
            if (findArtifactIndex(current, .document_facts)) |facts_index| {
                var read_remaining: u64 = limits.max_input_bytes;
                var write_remaining: u64 = 0;
                var pages = graph_segment_mod.page_store.PageStore{
                    .artifacts = self.artifacts,
                    .cancellation = cancellation_bridge.token(),
                    .remaining_read_bytes = &read_remaining,
                    .remaining_write_bytes = &write_remaining,
                };
                const source = try document_facts.loadRoot(self.alloc, &pages, current.artifacts[facts_index]);
                if (source.wal_end_lsn != current.wal_end_lsn or source.document_count != current.stats.document_count)
                    return error.DocumentFactsSourceChanged;
                if (!try document_facts_builder.needsRebuild(self.alloc, source, plan.policy, plan.table_definition.indexes_json)) {
                    const mutations = try decodeWalMutationsAlloc(self.alloc, applicable_records);
                    errdefer freeMaterializerMutations(self.alloc, mutations);
                    var touched = try document_facts_builder.materializeTouchedAlloc(self.alloc, &pages, source, mutations);
                    errdefer touched.deinit();
                    predicted_counts = try document_facts_builder.predictCountsAlloc(self.alloc, source, touched, plan.policy, plan.table_definition.indexes_json);
                    break :materialized .{
                        .base_documents = touched.before,
                        .documents = touched.after,
                        .mutations = mutations,
                        .graph_changed = plan.targets.include_graph and try graphProjectionChangedForMutationsAlloc(self.alloc, namespace, touched.before, touched.after, mutations, cancellation, limits),
                    };
                }
            }
            const full = try materializeWalDocumentsAlloc(self, namespace, current.version, applicable_records, plan.targets.include_graph, cancellation);
            break :materialized .{ .base_documents = full.base_documents, .documents = full.documents, .mutations = full.mutations, .graph_changed = full.graph_changed };
        };
        defer freeMaterializerMutations(self.alloc, built_documents.mutations);
        defer query_mod.freeMaterializedDocuments(self.alloc, built_documents.base_documents);
        defer query_mod.freeMaterializedDocuments(self.alloc, built_documents.documents);
        if (built_documents.documents.len > limits.max_rows) return error.LakeSidecarBuildBudgetExceeded;
        for (built_documents.documents) |doc| {
            try maintenance_cancellation.check(cancellation);
            input_bytes = std.math.add(usize, input_bytes, doc.body.len) catch return error.LakeSidecarBuildBudgetExceeded;
            if (input_bytes > limits.max_input_bytes) return error.LakeSidecarBuildBudgetExceeded;
        }

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
            built_documents.graph_changed,
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
            built_documents.graph_changed,
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
            built_documents.graph_changed,
        );
        errdefer {
            for (graph_index_actions) |*entry| entry.deinit(self.alloc);
            if (graph_index_actions.len > 0) self.alloc.free(graph_index_actions);
        }
        var detected_outputs = if (predicted_counts) |counts|
            try document_facts_builder.outputsFromCountsAlloc(self.alloc, counts, .{
                .chunk_preview = plan.derived_output_actions.chunk_preview != .drop,
                .chunk_embeddings = plan.derived_output_actions.chunk_embeddings != .drop,
                .rerank_terms = plan.derived_output_actions.rerank_terms != .drop,
            })
        else
            try detectMaterializedDerivedOutputsAlloc(
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
        const pending_enrichment = if (predicted_counts) |counts| PendingEnrichmentPrediction{
            .pipeline = enrichment_pipeline.builtinPipelineForPolicy(plan.policy),
            .lexical_sparse_pending_documents = if (plan.artifact_actions.sparse_vector != .drop) counts[3] else 0,
            .chunk_preview_pending_documents = if (plan.derived_output_actions.chunk_preview != .drop) counts[4] else 0,
            .chunk_embeddings_pending_documents = if (plan.derived_output_actions.chunk_embeddings != .drop) counts[5] else 0,
            .rerank_terms_pending_documents = if (plan.derived_output_actions.rerank_terms != .drop) counts[6] else 0,
        } else try predictPendingEnrichmentAlloc(
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
                .document_segment = if (findArtifactIndex(current, .document_facts) != null or findArtifactIndex(current, .document_segment) != null) .reuse else .rebuild,
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
                else if (built_documents.graph_changed or findArtifactIndex(current, .graph_segment) == null)
                    .rebuild
                else
                    .reuse,
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

pub fn compareAndSwapHeadGuarded(
    progress: *catalog_mod.ProgressStore,
    namespace: []const u8,
    expected: ?u64,
    version: u64,
    publication_guard: ?work_lease.PublicationGuard,
) !bool {
    if (publication_guard) |guard| {
        if (try guard.preparePublication(namespace)) |fence| {
            return try progress.compareAndSwapHeadFenced(namespace, expected, version, fence);
        }
    }
    return try progress.compareAndSwapHead(namespace, expected, version);
}

pub fn stampPublicationFence(manifest: *manifest_mod.Manifest, guard: ?work_lease.PublicationGuard) !void {
    const authority = guard orelse return error.GraphPublicationGuardRequired;
    const fence = (try authority.preparePublication(manifest.namespace)) orelse return error.GraphPublicationGuardRequired;
    manifest.publication_fencing_token = fence.fencing_token;
}

pub fn graphPublicationError(err: anyerror, allocation_denied: bool) anyerror {
    return switch (err) {
        error.OutOfMemory => if (allocation_denied) error.LakeSidecarBuildBudgetExceeded else err,
        error.GraphSegmentTooLarge, error.GraphPageWriteBudgetExceeded, error.ArtifactReadBudgetExceeded => error.LakeSidecarBuildBudgetExceeded,
        else => err,
    };
}

test "serverless graph publication resource limits retain actionable error categories" {
    for ([_]anyerror{ error.GraphPageWriteBudgetExceeded, error.ArtifactReadBudgetExceeded, error.GraphSegmentTooLarge }) |err|
        try std.testing.expectEqual(error.LakeSidecarBuildBudgetExceeded, graphPublicationError(err, false));
    try std.testing.expectEqual(error.LakeSidecarBuildBudgetExceeded, graphPublicationError(error.OutOfMemory, true));
    try std.testing.expectEqual(error.OutOfMemory, graphPublicationError(error.OutOfMemory, false));
    try std.testing.expectEqual(error.GraphPageRecordTooLarge, graphPublicationError(error.GraphPageRecordTooLarge, false));
}

/// A metadata-only operation can authenticate the fixed-size facts root and
/// retain its immutable tree when neither documents nor counter semantics
/// changed. The surrounding publication already owns source protection.
fn reusableDocumentFactsAlloc(alloc: Allocator, artifacts: *artifacts_mod.ArtifactStore, current: manifest_mod.Manifest, policy: catalog_mod.NamespacePolicy, indexes_json: []const u8, cancellation: ?maintenance_cancellation.Token) !?document_facts.Root {
    const idx = findArtifactIndex(current, .document_facts) orelse return null;
    var bridge = maintenance_cancellation.GraphBridge{ .maintenance = cancellation };
    var reads: u64 = document_facts.Root.encoded_bytes;
    var writes: u64 = 0;
    var pages: graph_page_store.PageStore = .{ .domain = graph_page_store.PageStore.namespaceDomain(current.namespace), .artifacts = artifacts, .cancellation = bridge.token(), .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
    const root = try document_facts.loadRoot(alloc, &pages, current.artifacts[idx]);
    if (root.wal_end_lsn != current.wal_end_lsn or root.document_count != current.stats.document_count) return error.DocumentFactsSourceChanged;
    if (try document_facts_builder.needsRebuild(alloc, root, policy, indexes_json)) return null;
    return root;
}

pub fn publishDocumentFactsForManifest(
    alloc: Allocator,
    artifacts: *artifacts_mod.ArtifactStore,
    manifest: *manifest_mod.Manifest,
    current: ?manifest_mod.Manifest,
    docs: []const query_mod.QueryMaterializedDocument,
    mutations: ?[]const query_mod.QueryMaterializerMutation,
    guard: ?work_lease.PublicationGuard,
    cancellation: ?maintenance_cancellation.Token,
) !void {
    var working_set = try graph_build_limits.WorkingSetAllocator.init(alloc, .{});
    const operation_alloc = working_set.allocator();
    var operation_artifacts = artifacts.*;
    operation_artifacts.allocator = operation_alloc;
    var bridge = maintenance_cancellation.GraphBridge{ .maintenance = cancellation };
    var read_budget: u64 = (GraphBuildLimits{}).max_input_bytes;
    var write_budget: u64 = (GraphBuildLimits{}).max_output_bytes;
    var pages = graph_page_store.PageStore{
        .domain = graph_page_store.PageStore.namespaceDomain(manifest.namespace),
        .attempt = try graphPublicationAttempt(guard, manifest.namespace, cancellation.?.io),
        .artifacts = &operation_artifacts,
        .cancellation = bridge.token(),
        .remaining_read_bytes = &read_budget,
        .remaining_write_bytes = &write_budget,
    };
    const prior_ref = if (current) |source| if (findArtifactIndex(source, .document_facts)) |idx| source.artifacts[idx] else null else null;
    if (prior_ref) |ref| {
        const root = document_facts.loadRoot(operation_alloc, &pages, ref) catch |err| return graphPublicationError(err, working_set.limit_exceeded);
        if (root.wal_end_lsn != current.?.wal_end_lsn) return error.DocumentFactsSourceChanged;
    }
    const ref = document_facts_builder.publishAlloc(operation_alloc, &pages, prior_ref, docs, mutations, manifest.stats.policy, manifest.stats.indexes_json, manifest.wal_end_lsn) catch |err| return graphPublicationError(err, working_set.limit_exceeded);
    errdefer freeArtifactRef(alloc, ref);
    if (findArtifactIndex(manifest.*, .document_facts)) |idx| {
        freeArtifactRef(alloc, manifest.artifacts[idx]);
        manifest.artifacts[idx] = ref;
    } else {
        manifest.artifacts = try alloc.realloc(manifest.artifacts, manifest.artifacts.len + 1);
        manifest.artifacts[manifest.artifacts.len - 1] = ref;
    }
}

fn applicableWalRecordsForHeadAlloc(
    alloc: Allocator,
    records: []const wal_mod.Record,
    current_head: u64,
) ![]wal_mod.Record {
    const applicable = try alloc.alloc(wal_mod.Record, records.len);
    errdefer alloc.free(applicable);
    var count: usize = 0;
    for (records) |record| {
        const source_head = try enrichment_operation_id.sourceHeadVersion(record.operation_id);
        if (source_head != null and source_head.? != current_head) continue;
        applicable[count] = record;
        count += 1;
    }
    return alloc.realloc(applicable, count);
}

/// Store an immutable manifest without letting an abandoned version wedge all
/// later publications. The common path is one PUT; listing is only needed when
/// an earlier publisher left different contents at the same version.
pub fn putManifestForPublication(
    manifests: *manifest_mod.ManifestStore,
    manifest: *manifest_mod.Manifest,
    expected_head: ?u64,
) !u64 {
    manifest.publication_lineage_tracked = true;
    manifest.publication_parent_version = expected_head;
    while (true) {
        manifests.put(manifest.*) catch |err| switch (err) {
            error.ManifestVersionAlreadyExists => {
                const versions = try manifests.listVersionsAlloc(manifest.namespace);
                defer manifests.allocator.free(versions);
                var maximum = manifest.version;
                for (versions) |version| maximum = @max(maximum, version);
                const replacement = std.math.add(u64, maximum, 1) catch
                    return error.ManifestVersionExhausted;
                if (manifest.stats.document_base_version == manifest.version) {
                    manifest.stats.document_base_version = replacement;
                }
                manifest.version = replacement;
                continue;
            },
            else => return err,
        };
        return manifest.version;
    }
}

test "serverless publication advances past a conflicting orphan manifest" {
    const alloc = std.testing.allocator;
    var memory = objectstore.MemoryClient.init(alloc);
    defer memory.deinit();
    var manifest_impl = try manifest_object_store.ObjectStore.initWithClient(
        alloc,
        memory.client(),
        "manifests",
        "tenant",
    );
    var manifests = manifest_impl.manifestStore();
    defer manifests.deinit();

    try manifests.put(.{
        .namespace = "docs",
        .version = 1,
        .built_at_ns = 100,
        .wal_start_lsn = 1,
        .wal_end_lsn = 1,
        .stats = .{ .document_count = 1, .document_base_version = 1 },
        .artifacts = &.{},
    });
    var candidate = manifest_mod.Manifest{
        .namespace = "docs",
        .version = 1,
        .built_at_ns = 200,
        .wal_start_lsn = 1,
        .wal_end_lsn = 1,
        .stats = .{ .document_count = 2, .document_base_version = 1 },
        .artifacts = &.{},
    };
    try std.testing.expectEqual(
        @as(u64, 2),
        try putManifestForPublication(&manifests, &candidate, null),
    );
    try std.testing.expectEqual(@as(u64, 2), candidate.version);
    try std.testing.expectEqual(@as(u64, 2), candidate.stats.document_base_version);
    try std.testing.expect(candidate.publication_lineage_tracked);
    try std.testing.expectEqual(@as(?u64, null), candidate.publication_parent_version);
    var stored = try manifests.getAlloc("docs", 2);
    defer stored.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 2), stored.stats.document_count);
}

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
    const graph_count: usize = countArtifactRefsByKind(graph_refs, .graph_segment);
    const artifacts = try alloc.alloc(manifest_mod.ArtifactRef, 2 + text_count + sparse_count + vector_count + graph_refs.len);
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
    const base_source = try cloneTableDefinitionBaseSourceAlloc(alloc, table_definition);
    errdefer if (base_source) |descriptor| {
        var cleanup = descriptor;
        manifest_base_source.freeOwnedDescriptor(alloc, &cleanup);
    };

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
        .base_source = base_source,
    };
}

fn buildEmptyExternalManifestAlloc(
    alloc: Allocator,
    namespace: []const u8,
    version: u64,
    start_lsn: u64,
    plan: publication_plan.TablePublicationPlan,
) !manifest_mod.Manifest {
    const namespace_copy = try alloc.dupe(u8, namespace);
    errdefer alloc.free(namespace_copy);
    const artifacts = try alloc.alloc(manifest_mod.ArtifactRef, 0);
    errdefer alloc.free(artifacts);
    var base_source = try cloneTableDefinitionBaseSourceAlloc(alloc, plan.table_definition);
    errdefer if (base_source) |*descriptor| manifest_base_source.freeOwnedDescriptor(alloc, descriptor);
    const schema_json: []u8 = if (plan.table_definition.schema_json.len == 0)
        &.{}
    else
        try alloc.dupe(u8, plan.table_definition.schema_json);
    errdefer if (schema_json.len > 0) alloc.free(schema_json);
    const read_schema_json: []u8 = if (plan.table_definition.read_schema_json.len == 0)
        &.{}
    else
        try alloc.dupe(u8, plan.table_definition.read_schema_json);
    errdefer if (read_schema_json.len > 0) alloc.free(read_schema_json);
    const indexes_json: []u8 = if (plan.table_definition.indexes_json.len == 0)
        &.{}
    else
        try alloc.dupe(u8, plan.table_definition.indexes_json);
    errdefer if (indexes_json.len > 0) alloc.free(indexes_json);

    return .{
        .namespace = namespace_copy,
        .version = version,
        .built_at_ns = 0,
        .wal_start_lsn = start_lsn,
        .wal_end_lsn = start_lsn - 1,
        .stats = .{
            .document_count = 0,
            .document_base_version = 0,
            .document_publish_mode = .append_mutation_tail,
            .published_search_sources = .{},
            .policy = plan.policy,
            .schema_json = schema_json,
            .read_schema_json = read_schema_json,
            .indexes_json = indexes_json,
        },
        .artifacts = artifacts,
        .base_source = base_source,
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
    const graph_count: usize = countArtifactRefsByKind(graph_refs, .graph_segment);
    const artifacts = try alloc.alloc(manifest_mod.ArtifactRef, 1 + text_count + sparse_count + vector_count + graph_refs.len);
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
    const base_source = try cloneTableDefinitionBaseSourceAlloc(alloc, table_definition);
    errdefer if (base_source) |descriptor| {
        var cleanup = descriptor;
        manifest_base_source.freeOwnedDescriptor(alloc, &cleanup);
    };

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
        .base_source = base_source,
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
    const graph_count: usize = countArtifactRefsByKind(graph_refs, .graph_segment);
    const artifacts = try alloc.alloc(manifest_mod.ArtifactRef, 1 + text_count + sparse_count + vector_count + graph_refs.len);
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
    const base_source = try cloneTableDefinitionBaseSourceAlloc(alloc, table_definition);
    errdefer if (base_source) |descriptor| {
        var cleanup = descriptor;
        manifest_base_source.freeOwnedDescriptor(alloc, &cleanup);
    };

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
        .base_source = base_source,
    };
}

fn cloneTableDefinitionBaseSourceAlloc(
    alloc: Allocator,
    table_definition: publication_plan.TableDefinitionSnapshot,
) !?manifest_base_source.BaseSourceDescriptor {
    return if (table_definition.base_source) |descriptor|
        try manifest_base_source.cloneDescriptorAlloc(alloc, descriptor)
    else
        null;
}

fn publicationPlanHasExternalBaseSource(plan: publication_plan.TablePublicationPlan) bool {
    if (plan.external_source_plan != null) return true;
    const base_source = plan.table_definition.base_source orelse return false;
    return switch (base_source) {
        .external_parquet, .external_iceberg, .external_lance => true,
        else => false,
    };
}

fn attachResolvedExternalSourcePlanIfPresent(
    alloc: Allocator,
    manifest: *manifest_mod.Manifest,
    plan: publication_plan.TablePublicationPlan,
) !void {
    const external_plan = plan.external_source_plan orelse return;
    _ = try external_source_publication.attachPlanToOwnedManifestAlloc(
        alloc,
        manifest,
        external_plan,
        .{},
    );
}

fn externalSourcePlanMatchesManifest(
    plan: external_source_manifest.Plan,
    manifest: manifest_mod.Manifest,
) bool {
    const current = manifest.base_source orelse return false;
    if (std.meta.activeTag(plan.base_source) != std.meta.activeTag(current)) return false;
    const sources_match = switch (plan.base_source) {
        .external_parquet => |planned| externalSourceDescriptorMatches(planned, current.external_parquet),
        .external_iceberg => |planned| externalSourceDescriptorMatches(planned, current.external_iceberg),
        .external_lance => |planned| externalSourceDescriptorMatches(planned, current.external_lance),
        else => false,
    };
    if (!sources_match or plan.artifacts.len == 0) return false;

    // External plans own the external metadata refs, while a published
    // manifest may also carry unrelated document and search artifacts. Match
    // the owned refs as an exact multiset so
    // a logical rename, removal, duplicate, or replacement cannot be mistaken
    // for an unchanged plan, while a harmless order change does not force a
    // new publication.
    for (plan.artifacts) |artifact| {
        if (artifact.kind != .external_base_source) return false;
    }
    var current_external_count: usize = 0;
    for (manifest.artifacts) |current_artifact| {
        if (current_artifact.kind == .external_base_source) current_external_count += 1;
    }
    if (current_external_count != plan.artifacts.len) return false;
    for (plan.artifacts) |planned_artifact| {
        var planned_occurrences: usize = 0;
        for (plan.artifacts) |candidate| {
            if (artifactRefEql(planned_artifact, candidate)) planned_occurrences += 1;
        }
        var current_occurrences: usize = 0;
        for (manifest.artifacts) |candidate| {
            if (artifactRefEql(planned_artifact, candidate)) current_occurrences += 1;
        }
        if (planned_occurrences != current_occurrences) return false;
    }
    return true;
}

fn artifactRefEql(left: manifest_mod.ArtifactRef, right: manifest_mod.ArtifactRef) bool {
    return left.kind == right.kind and
        std.mem.eql(u8, left.name, right.name) and
        std.mem.eql(u8, left.artifact_id, right.artifact_id) and
        left.byte_len == right.byte_len and
        std.mem.eql(u8, left.checksum, right.checksum);
}

test "serverless external source plan matching is exact over owned artifacts" {
    const base_source = manifest_mod.BaseSourceDescriptor{ .external_parquet = .{
        .format = .parquet_prefix,
        .source_uri = "s3://bucket/events",
        .snapshot_id = "snapshot-1",
        .schema_fingerprint = "schema-v1",
        .file_inventory_artifact = "inventory-1",
        .row_group_metadata_artifact = "row-groups-1",
    } };
    var planned_artifacts = [_]manifest_mod.ArtifactRef{
        .{
            .kind = .external_base_source,
            .name = "events.external-files",
            .artifact_id = "inventory-1",
            .byte_len = 128,
            .checksum = "sha256:inventory-1",
        },
        .{
            .kind = .external_base_source,
            .name = "events.row-groups",
            .artifact_id = "row-groups-1",
            .byte_len = 32,
            .checksum = "sha256:row-groups-1",
        },
    };
    var current_artifacts = [_]manifest_mod.ArtifactRef{
        .{
            .kind = .document_segment,
            .artifact_id = "documents-1",
            .byte_len = 64,
            .checksum = "sha256:documents-1",
        },
        planned_artifacts[1],
        planned_artifacts[0],
        .{
            .kind = .external_base_source,
            .name = "events.deletes",
            .artifact_id = "deletes-1",
            .byte_len = 16,
            .checksum = "sha256:deletes-1",
        },
    };
    const plan = external_source_manifest.Plan{
        .base_source = base_source,
        .artifacts = planned_artifacts[0..],
    };
    var manifest = manifest_mod.Manifest{
        .namespace = "events",
        .version = 1,
        .built_at_ns = 1,
        .wal_start_lsn = 1,
        .wal_end_lsn = 0,
        .stats = .{},
        .artifacts = current_artifacts[0..3],
        .base_source = base_source,
    };

    try std.testing.expect(externalSourcePlanMatchesManifest(plan, manifest));
    planned_artifacts[0].name = "events.renamed-files";
    try std.testing.expect(!externalSourcePlanMatchesManifest(plan, manifest));
    planned_artifacts[0].name = "events.external-files";
    manifest.artifacts = current_artifacts[0..2];
    try std.testing.expect(!externalSourcePlanMatchesManifest(plan, manifest));
    manifest.artifacts = current_artifacts[0..];
    try std.testing.expect(!externalSourcePlanMatchesManifest(plan, manifest));
}

fn externalSourceDescriptorMatches(
    planned: manifest_base_source.ExternalBaseSource,
    current: manifest_base_source.ExternalBaseSource,
) bool {
    return planned.format == current.format and
        std.mem.eql(u8, planned.source_uri, current.source_uri) and
        std.mem.eql(u8, planned.snapshot_id, current.snapshot_id) and
        std.mem.eql(u8, planned.schema_fingerprint, current.schema_fingerprint) and
        optionalStringEql(planned.file_inventory_artifact, current.file_inventory_artifact) and
        optionalStringEql(planned.row_group_metadata_artifact, current.row_group_metadata_artifact) and
        optionalStringEql(planned.delete_metadata_artifact, current.delete_metadata_artifact);
}

fn optionalStringEql(left: ?[]const u8, right: ?[]const u8) bool {
    if (left == null or right == null) return left == null and right == null;
    return std.mem.eql(u8, left.?, right.?);
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

test "serverless detect materialized derived outputs honors requested selection" {
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

pub const PendingEnrichmentPrediction = struct {
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

pub const PendingEnrichmentSelection = struct {
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

test "serverless predict pending enrichment honors requested selection" {
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

pub fn predictPendingEnrichmentAlloc(
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

const PublicationDocuments = struct {
    base_documents: []query_mod.QueryMaterializedDocument,
    documents: []query_mod.QueryMaterializedDocument,
    mutations: []query_mod.QueryMaterializerMutation,
    document_count: usize,
    partial: bool = false,
    counts: ?[7]u64 = null,
};

fn materializeWalDocumentsForPublicationAlloc(
    self: *Builder,
    namespace: []const u8,
    current_head: u64,
    current: ?manifest_mod.Manifest,
    records: []const wal_mod.Record,
    plan: publication_plan.TablePublicationPlan,
    cancellation: ?maintenance_cancellation.Token,
) !PublicationDocuments {
    if (current) |manifest| {
        if (findArtifactIndex(manifest, .document_facts)) |idx| {
            var bridge = maintenance_cancellation.GraphBridge{ .maintenance = cancellation };
            var read_budget: u64 = (GraphBuildLimits{}).max_input_bytes;
            var write_budget: u64 = 0;
            var pages = graph_page_store.PageStore{ .domain = graph_page_store.PageStore.namespaceDomain(namespace), .artifacts = self.artifacts, .cancellation = bridge.token(), .remaining_read_bytes = &read_budget, .remaining_write_bytes = &write_budget };
            const root = try document_facts.loadRoot(self.alloc, &pages, manifest.artifacts[idx]);
            if (root.wal_end_lsn != manifest.wal_end_lsn) return error.DocumentFactsSourceChanged;
            if (!try document_facts_builder.needsRebuild(self.alloc, root, plan.policy, plan.table_definition.indexes_json)) {
                const mutations = try decodeWalMutationsAlloc(self.alloc, records);
                var owned = true;
                defer if (owned) freeMaterializerMutations(self.alloc, mutations);
                var touched = try document_facts_builder.materializeTouchedAlloc(self.alloc, &pages, root, mutations);
                defer if (owned) touched.deinit();
                if (!try flatSearchProjectionChangedAlloc(self.alloc, manifest, touched.before, touched.after, mutations, plan, cancellation)) {
                    const counts = try document_facts_builder.predictCountsAlloc(self.alloc, root, touched, plan.policy, plan.table_definition.indexes_json);
                    const count = try std.math.add(u64, std.math.sub(u64, root.document_count, touched.before.len) catch return error.InvalidDocumentFactsRoot, touched.after.len);
                    const document_count = std.math.cast(usize, count) orelse return error.LakeSidecarBuildBudgetExceeded;
                    owned = false;
                    return .{ .base_documents = touched.before, .documents = touched.after, .mutations = mutations, .document_count = document_count, .partial = true, .counts = counts };
                }
            }
        }
    }
    const full = try materializeWalDocumentsAlloc(self, namespace, current_head, records, false, cancellation);
    return .{ .base_documents = full.base_documents, .documents = full.documents, .mutations = full.mutations, .document_count = full.documents.len };
}

/// Flat text/vector/sparse formats still need the complete view only when
/// their own projection changes. Graph-only updates remain point lookups and
/// copy-on-write page mutations even when unrelated search indexes coexist.
fn flatSearchProjectionChangedAlloc(
    alloc: Allocator,
    current: manifest_mod.Manifest,
    before: []const query_mod.QueryMaterializedDocument,
    after: []const query_mod.QueryMaterializedDocument,
    mutations: []const query_mod.QueryMaterializerMutation,
    plan: publication_plan.TablePublicationPlan,
    cancellation: ?maintenance_cancellation.Token,
) !bool {
    const specs = try resolvePublishedTextIndexSpecsAlloc(alloc, plan.table_definition, plan.full_text_index_actions);
    defer full_text_indexes.freeFullTextIndexSpecs(alloc, specs);
    for (specs) |spec| {
        try maintenance_cancellation.check(cancellation);
        if (!artifactAvailableForName(current, .text_segment, spec.name) or
            try textProjectionChangedForMutationsAlloc(alloc, before, after, mutations, spec)) return true;
    }
    const vectors = try search_sources.listVectorSourcesAlloc(alloc, plan.targets.published_search_sources);
    defer search_sources.freeVectorSourceDescriptors(alloc, vectors);
    for (vectors) |source| {
        try maintenance_cancellation.check(cancellation);
        if (try vectorProjectionChangedForMutationsAlloc(alloc, before, after, mutations, source)) return true;
    }
    const sparse = try search_sources.listSparseSourcesAlloc(alloc, plan.targets.published_search_sources);
    defer search_sources.freeSparseSourceDescriptors(alloc, sparse);
    for (sparse) |source| {
        try maintenance_cancellation.check(cancellation);
        if (try sparseProjectionChangedForMutationsAlloc(alloc, before, after, mutations, source)) return true;
    }
    return false;
}

pub fn derivedOutputsFromFactCountsAlloc(alloc: Allocator, counts: [7]u64) !search_sources.MaterializedDerivedOutputs {
    var outputs: std.ArrayListUnmanaged(search_sources.DerivedOutputDescriptor) = .empty;
    errdefer {
        for (outputs.items) |*item| search_sources.deinitDerivedOutputDescriptor(alloc, item);
        outputs.deinit(alloc);
    }
    inline for (.{ .chunk_preview, .chunk_embeddings, .rerank_terms }, 0..) |kind, index| {
        if (counts[index] != 0) {
            const name = try alloc.dupe(u8, search_sources.defaultDerivedOutputName(kind));
            errdefer alloc.free(name);
            try outputs.append(alloc, .{ .name = name, .kind = kind });
        }
    }
    return .{ .items = if (outputs.items.len == 0) null else try outputs.toOwnedSlice(alloc) };
}

fn materializeWalDocumentsAlloc(
    self: *Builder,
    namespace: []const u8,
    current_head: u64,
    records: []const wal_mod.Record,
    check_graph_impact: bool,
    cancellation: ?maintenance_cancellation.Token,
) !struct {
    base_documents: []query_mod.QueryMaterializedDocument,
    documents: []query_mod.QueryMaterializedDocument,
    mutations: []query_mod.QueryMaterializerMutation,
    graph_changed: bool,
} {
    try maintenance_cancellation.check(cancellation);
    const mutations = try decodeWalMutationsAlloc(self.alloc, records);
    errdefer freeMaterializerMutations(self.alloc, mutations);

    const base_docs = try loadPublishedDocumentsAlloc(self, namespace, current_head, cancellation);
    errdefer query_mod.freeMaterializedDocuments(self.alloc, base_docs);

    try maintenance_cancellation.check(cancellation);
    const next_docs = try query_mod.materializeDocumentsOverBaseAlloc(self.alloc, base_docs, mutations);
    errdefer query_mod.freeMaterializedDocuments(self.alloc, next_docs);

    const graph_changed = if (check_graph_impact)
        try graphProjectionChangedForMutationsAlloc(self.alloc, namespace, base_docs, next_docs, mutations, cancellation, .{})
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
    const materialized = try materializeWalDocumentsAlloc(self, namespace, current_head, records, check_graph_impact, null);
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

fn buildTextSegmentAlloc(alloc: Allocator, docs: []const query_mod.QueryMaterializedDocument, spec: FullTextIndexSpec, cancellation: ?maintenance_cancellation.Token) ![]u8 {
    var segment = try allocTextSegmentAlloc(alloc, docs, spec, cancellation);
    defer text_segment_mod.freeSegment(alloc, &segment);
    return try text_segment_mod.encodeAlloc(alloc, segment);
}

fn buildSparseSegmentAlloc(alloc: Allocator, docs: []const query_mod.QueryMaterializedDocument) !struct {
    payload: ?[]u8,
    feature_count: usize,
} {
    return try buildSparseSegmentAllocForSource(alloc, docs, search_sources.defaultPublishedSearchSources().findSparse(), null);
}

fn walEndAsBuiltAtNs(wal_end_lsn: u64) u64 {
    return wal_end_lsn;
}

fn buildSparseSegmentAllocForSource(
    alloc: Allocator,
    docs: []const query_mod.QueryMaterializedDocument,
    sparse_source: ?search_sources.SparseSourceDescriptor,
    cancellation: ?maintenance_cancellation.Token,
) !struct {
    payload: ?[]u8,
    feature_count: usize,
} {
    if (sparse_source == null) return .{ .payload = null, .feature_count = 0 };
    var segment = try allocSparseSegmentAlloc(alloc, docs, sparse_source.?, cancellation);
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
    return try buildSparseArtifactRefsForMaterializedDocsAllocUntil(
        alloc,
        artifacts,
        current,
        before_docs,
        docs,
        mutations,
        published_search_sources,
        null,
    );
}

pub fn buildSparseArtifactRefsForMaterializedDocsAllocUntil(
    alloc: Allocator,
    artifacts: *artifacts_mod.ArtifactStore,
    current: ?manifest_mod.Manifest,
    before_docs: []const query_mod.QueryMaterializedDocument,
    docs: []const query_mod.QueryMaterializedDocument,
    mutations: []const query_mod.QueryMaterializerMutation,
    published_search_sources: search_sources.PublishedSearchSources,
    cancellation: ?maintenance_cancellation.Token,
) ![]manifest_mod.ArtifactRef {
    const sparse_sources = try search_sources.listSparseSourcesAlloc(alloc, published_search_sources);
    defer search_sources.freeSparseSourceDescriptors(alloc, sparse_sources);

    var refs = std.ArrayListUnmanaged(manifest_mod.ArtifactRef).empty;
    errdefer freeArtifactRefs(alloc, refs.items);
    for (sparse_sources) |source| {
        try maintenance_cancellation.check(cancellation);
        if (current) |manifest| {
            if (!try sparseProjectionChangedForMutationsAlloc(alloc, before_docs, docs, mutations, source)) {
                if (try appendReusedNamedArtifactRefAlloc(alloc, &refs, manifest, .sparse_segment, source.index_name)) continue;
            }
        }
        const built = try buildSparseSegmentAllocForSource(alloc, docs, source, cancellation);
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

fn loadPublishedDocumentsAlloc(self: *Builder, namespace: []const u8, current_head: u64, cancellation: ?maintenance_cancellation.Token) ![]query_mod.QueryMaterializedDocument {
    if (current_head == 0) return try self.alloc.alloc(query_mod.QueryMaterializedDocument, 0);

    var current = try self.manifests.getAlloc(namespace, current_head);
    defer current.deinit(self.alloc);
    if (findArtifactIndex(current, .document_facts)) |idx|
        return materializeManifestFactsAlloc(self.alloc, self.artifacts, current, current.artifacts[idx], cancellation);
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

pub fn materializeManifestFactsAlloc(alloc: Allocator, artifacts: *artifacts_mod.ArtifactStore, manifest: manifest_mod.Manifest, ref: manifest_mod.ArtifactRef, cancellation: ?maintenance_cancellation.Token) ![]query_mod.QueryMaterializedDocument {
    var bridge = maintenance_cancellation.GraphBridge{ .maintenance = cancellation };
    var read_budget: u64 = (GraphBuildLimits{}).max_input_bytes;
    var write_budget: u64 = 0;
    var pages = graph_page_store.PageStore{ .domain = graph_page_store.PageStore.namespaceDomain(manifest.namespace), .artifacts = artifacts, .cancellation = bridge.token(), .remaining_read_bytes = &read_budget, .remaining_write_bytes = &write_budget };
    const root = try document_facts.loadRoot(alloc, &pages, ref);
    if (root.wal_end_lsn != manifest.wal_end_lsn) return error.DocumentFactsSourceChanged;
    return document_facts_builder.materializeAllAlloc(alloc, &pages, root);
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
    errdefer for (mutations[0..initialized]) |mutation| {
        alloc.free(mutation.doc_id);
        if (mutation.body) |body| alloc.free(body);
    };

    for (entries, 0..) |entry, idx| {
        const doc_id = try alloc.dupe(u8, entry.doc_id);
        errdefer alloc.free(doc_id);
        mutations[idx] = .{
            .lsn = entry.lsn,
            .timestamp_ns = entry.timestamp_ns,
            .kind = entry.kind,
            .doc_id = doc_id,
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
    errdefer for (mutations[0..initialized]) |mutation| {
        alloc.free(mutation.doc_id);
        if (mutation.body) |body| alloc.free(body);
    };

    for (records, 0..) |record, idx| {
        var mutation = try api_codec.decodeMutationAlloc(alloc, record.payload);
        defer mutation.deinit(alloc);
        const doc_id = try alloc.dupe(u8, mutation.doc_id);
        errdefer alloc.free(doc_id);
        mutations[idx] = .{
            .lsn = record.lsn,
            .timestamp_ns = record.timestamp_ns,
            .kind = mutation.kind,
            .doc_id = doc_id,
            .body = if (mutation.body) |body| try alloc.dupe(u8, body) else null,
        };
        initialized += 1;
    }
    return mutations;
}

pub fn graphProjectionChangedForMutationsAlloc(
    alloc: Allocator,
    source_table: []const u8,
    before_docs: []const query_mod.QueryMaterializedDocument,
    after_docs: []const query_mod.QueryMaterializedDocument,
    mutations: []const query_mod.QueryMaterializerMutation,
    cancellation: ?maintenance_cancellation.Token,
    limits: GraphBuildLimits,
) !bool {
    var working_set = try graph_build_limits.WorkingSetAllocator.init(alloc, limits);
    return graphProjectionChangedBoundedAlloc(working_set.allocator(), source_table, before_docs, after_docs, mutations, cancellation, limits) catch |err| {
        if (err == error.OutOfMemory and working_set.limit_exceeded) return error.LakeSidecarBuildBudgetExceeded;
        return err;
    };
}

fn graphProjectionChangedBoundedAlloc(
    alloc: Allocator,
    source_table: []const u8,
    before_docs: []const query_mod.QueryMaterializedDocument,
    after_docs: []const query_mod.QueryMaterializedDocument,
    mutations: []const query_mod.QueryMaterializerMutation,
    cancellation: ?maintenance_cancellation.Token,
    limits: GraphBuildLimits,
) !bool {
    try maintenance_cancellation.check(cancellation);
    if (mutations.len > limits.max_rows) return error.LakeSidecarBuildBudgetExceeded;
    var seen = std.StringHashMapUnmanaged(void).empty;
    defer seen.deinit(alloc);
    var input_bytes: usize = 0;

    for (mutations) |mutation| {
        try maintenance_cancellation.check(cancellation);
        const gop = try seen.getOrPut(alloc, mutation.doc_id);
        if (gop.found_existing) continue;
        if (seen.count() > limits.max_retained_items) return error.LakeSidecarBuildBudgetExceeded;
        const before = findMaterializedDocument(before_docs, mutation.doc_id);
        const after = findMaterializedDocument(after_docs, mutation.doc_id);
        // Byte-identical bodies cannot change topology. Avoid parsing ordinary
        // idempotent WAL replay, while still counting the admitted source bytes.
        for ([_]?query_mod.QueryMaterializedDocument{ before, after }) |doc| {
            if (doc) |value| {
                input_bytes = std.math.add(usize, input_bytes, value.body.len) catch return error.LakeSidecarBuildBudgetExceeded;
                if (input_bytes > limits.max_input_bytes) return error.LakeSidecarBuildBudgetExceeded;
            }
        }
        if (before != null and after != null and std.mem.eql(u8, before.?.body, after.?.body)) continue;
        if (try graphProjectionChangedAlloc(
            alloc,
            source_table,
            before,
            after,
            cancellation,
            limits,
        )) {
            return true;
        }
    }
    return false;
}

test "serverless builder graph impact admits scratch input and cancellation and canonicalizes local targets" {
    const a = std.testing.allocator;
    const before = [_]query_mod.QueryMaterializedDocument{.{
        .doc_id = @constCast("a"),
        .body = @constCast("{\"graph_edges\":[{\"target\":\"b\",\"edge_type\":\"link\"}]}"),
        .last_lsn = 1,
        .last_timestamp_ns = 1,
    }};
    const after = [_]query_mod.QueryMaterializedDocument{.{
        .doc_id = @constCast("a"),
        .body = @constCast("{\"graph_edges\":[{\"target\":\"b\",\"edge_type\":\"link\",\"target_table\":\"docs\"}]}"),
        .last_lsn = 2,
        .last_timestamp_ns = 2,
    }};
    const mutations = [_]query_mod.QueryMaterializerMutation{.{ .lsn = 2, .timestamp_ns = 2, .kind = .upsert, .doc_id = "a", .body = after[0].body }};
    try std.testing.expect(!try graphProjectionChangedForMutationsAlloc(a, "docs", &before, &after, &mutations, null, .{}));
    try std.testing.expect(try graphProjectionChangedForMutationsAlloc(a, "other", &before, &after, &mutations, null, .{}));
    try std.testing.expectError(error.LakeSidecarBuildBudgetExceeded, graphProjectionChangedForMutationsAlloc(a, "docs", &before, &after, &mutations, null, .{ .max_working_set_bytes = 1 }));
    try std.testing.expectError(error.LakeSidecarBuildBudgetExceeded, graphProjectionChangedForMutationsAlloc(a, "docs", &before, &after, &mutations, null, .{ .max_input_bytes = 1 }));
    var canceled = std.atomic.Value(bool).init(true);
    try std.testing.expectError(error.Canceled, graphProjectionChangedForMutationsAlloc(a, "docs", &before, &after, &mutations, .{ .io = std.testing.io, .requested = &canceled }, .{}));
    const Check = struct {
        fn run(alloc: Allocator, old: []const query_mod.QueryMaterializedDocument, new: []const query_mod.QueryMaterializedDocument, wal: []const query_mod.QueryMaterializerMutation) !void {
            try std.testing.expect(!try graphProjectionChangedForMutationsAlloc(alloc, "docs", old, new, wal, null, .{}));
        }
    };
    try std.testing.checkAllAllocationFailures(a, Check.run, .{ &before, &after, &mutations });
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
    source_table: []const u8,
    before_doc: ?query_mod.QueryMaterializedDocument,
    after_doc: ?query_mod.QueryMaterializedDocument,
    cancellation: ?maintenance_cancellation.Token,
    limits: GraphBuildLimits,
) !bool {
    if ((before_doc == null) != (after_doc == null)) return true;
    if (before_doc == null and after_doc == null) return false;

    const before = before_doc.?;
    const after = after_doc.?;

    const before_edges = try parseGraphEdgesAlloc(alloc, before.body);
    defer freeParsedGraphEdges(alloc, before_edges);
    if (before_edges.len > limits.max_retained_items) return error.LakeSidecarBuildBudgetExceeded;
    const after_edges = try parseGraphEdgesAlloc(alloc, after.body);
    defer freeParsedGraphEdges(alloc, after_edges);
    if (before_edges.len +| after_edges.len > limits.max_retained_items) return error.LakeSidecarBuildBudgetExceeded;

    // Use exactly the same local-table identity as graph construction.
    for ([_][]ParsedGraphEdge{ before_edges, after_edges }) |edges| {
        for (edges, 0..) |*edge, i| {
            if (i % 4096 == 0) try maintenance_cancellation.check(cancellation);
            if (edge.target_table) |table| if (std.mem.eql(u8, table, source_table)) {
                alloc.free(table);
                edge.target_table = null;
            };
        }
    }
    sortParsedGraphEdges(before_edges);
    sortParsedGraphEdges(after_edges);
    try maintenance_cancellation.check(cancellation);
    if (before_edges.len != after_edges.len) return true;
    for (before_edges, after_edges) |lhs, rhs| {
        if (!std.mem.eql(u8, lhs.target, rhs.target)) return true;
        if (!std.mem.eql(u8, lhs.edge_type, rhs.edge_type)) return true;
        if (!optionalStringsEqual(lhs.target_table, rhs.target_table)) return true;
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
        const doc_id = try alloc.dupe(u8, doc.doc_id);
        errdefer alloc.free(doc_id);
        entries[idx] = .{
            .doc_id = doc_id,
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
        const doc_id = try alloc.dupe(u8, entry.doc_id);
        errdefer alloc.free(doc_id);
        docs[idx] = .{
            .doc_id = doc_id,
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

pub fn countArtifactRefsByKind(refs: []const manifest_mod.ArtifactRef, kind: manifest_mod.ArtifactKind) usize {
    var count: usize = 0;
    for (refs) |artifact| if (artifact.kind == kind) {
        count += 1;
    };
    return count;
}

pub const VectorArtifactInfo = struct {
    metric: shared_vector.DistanceMetric = shared_vector.default_distance_metric,
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
        .metadata_version = artifact.metadata_version,
        .published_generation = artifact.published_generation,
        .edge_generation = artifact.edge_generation,
        .computed_at_ms = artifact.computed_at_ms,
        .materializer_fingerprint = artifact.materializer_fingerprint,
        .graph_metric_control_len = artifact.graph_metric_control_len,
        .graph_metric_routing_footer_len = artifact.graph_metric_routing_footer_len,
        .graph_metric_control_checksum = artifact.graph_metric_control_checksum,
        .graph_topology_control_checksum = artifact.graph_topology_control_checksum,
        .graph_metric_routing_checksum = artifact.graph_metric_routing_checksum,
        .graph_metric_point_index_checksum = artifact.graph_metric_point_index_checksum,
        .graph_metric_config_fingerprint = artifact.graph_metric_config_fingerprint,
        .graph_metric_source_checksum = artifact.graph_metric_source_checksum,
        .graph_metric_topology_checksum = artifact.graph_metric_topology_checksum,
        .graph_metric_materialization_state = artifact.graph_metric_materialization_state,
        .graph_metric_rejection_reason = artifact.graph_metric_rejection_reason,
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
        try filtered.ensureTotalCapacity(alloc, specs.len);
        for (specs) |*spec| {
            if (fullTextActionForName(planned_actions, spec.name, .reuse) == .drop) continue;
            filtered.appendAssumeCapacity(spec.*);
            spec.* = .{ .name = &.{}, .config_json = &.{} };
        }
        const selected = try filtered.toOwnedSlice(alloc);
        full_text_indexes.freeFullTextIndexSpecs(alloc, specs);
        specs = selected;
    }
    const chunked_sources = try full_text_indexes.listChunkedFullTextSourcesAlloc(alloc, table_definition.indexes_json);
    defer full_text_indexes.freeChunkedFullTextSources(alloc, chunked_sources);
    const chunk_full_text_source_name = if (chunked_sources.len > 0) search_sources.default_chunk_preview_output_name else null;
    if (specs.len == 0) {
        const fallback_specs = try alloc.alloc(FullTextIndexSpec, 1);
        errdefer alloc.free(fallback_specs);
        const name = try alloc.dupe(u8, search_sources.default_full_text_index_name);
        errdefer alloc.free(name);
        const config_json = try alloc.dupe(u8, "{\"type\":\"full_text\"}");
        errdefer alloc.free(config_json);
        const source_name = if (chunk_full_text_source_name) |value| try alloc.dupe(u8, value) else null;
        errdefer if (source_name) |value| alloc.free(value);
        fallback_specs[0] = .{
            .name = name,
            .config_json = config_json,
            .source_artifact_name = source_name,
            .source_mode = if (chunk_full_text_source_name != null) .document_plus_artifact else .document,
            .chunked_sources = try full_text_indexes.cloneChunkedFullTextSourcesAlloc(alloc, chunked_sources),
        };
        full_text_indexes.freeFullTextIndexSpecs(alloc, specs);
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
    return try buildTextArtifactRefsForMaterializedDocsAllocUntil(
        alloc,
        artifacts,
        current,
        before_docs,
        docs,
        mutations,
        text_index_specs,
        null,
    );
}

pub fn buildTextArtifactRefsForMaterializedDocsAllocUntil(
    alloc: Allocator,
    artifacts: *artifacts_mod.ArtifactStore,
    current: ?manifest_mod.Manifest,
    before_docs: []const query_mod.QueryMaterializedDocument,
    docs: []const query_mod.QueryMaterializedDocument,
    mutations: []const query_mod.QueryMaterializerMutation,
    text_index_specs: []const FullTextIndexSpec,
    cancellation: ?maintenance_cancellation.Token,
) ![]manifest_mod.ArtifactRef {
    const refs = try alloc.alloc(manifest_mod.ArtifactRef, text_index_specs.len);
    errdefer alloc.free(refs);
    var initialized: usize = 0;
    errdefer {
        for (refs[0..initialized]) |ref| freeArtifactRef(alloc, ref);
    }

    for (text_index_specs, 0..) |spec, idx| {
        try maintenance_cancellation.check(cancellation);
        if (current) |manifest| {
            if (!try textProjectionChangedForMutationsAlloc(alloc, before_docs, docs, mutations, spec)) {
                if (try cloneNamedArtifactRefAlloc(alloc, manifest, .text_segment, spec.name)) |artifact| {
                    refs[idx] = artifact;
                    initialized += 1;
                    continue;
                }
            }
        }
        const payload = try buildTextSegmentAlloc(alloc, docs, spec, cancellation);
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
    cancellation: ?maintenance_cancellation.Token,
) ![]manifest_mod.ArtifactRef {
    const refs = try alloc.alloc(manifest_mod.ArtifactRef, text_index_specs.len);
    errdefer alloc.free(refs);
    var initialized: usize = 0;
    errdefer {
        for (refs[0..initialized]) |ref| freeArtifactRef(alloc, ref);
    }

    for (text_index_specs, 0..) |spec, idx| {
        try maintenance_cancellation.check(cancellation);
        const action = fullTextActionForName(planned_actions, spec.name, fallback_action);
        if (action == .reuse) {
            if (findNamedArtifactIndex(current, .text_segment, spec.name)) |artifact_index| {
                refs[idx] = try cloneArtifactRefAlloc(alloc, current.artifacts[artifact_index]);
                if (refs[idx].name.len == 0) refs[idx].name = try alloc.dupe(u8, spec.name);
                initialized += 1;
                continue;
            }
        }
        const payload = try buildTextSegmentAlloc(alloc, docs, spec, cancellation);
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
    cancellation: ?maintenance_cancellation.Token,
) ![]query_mod.QueryMaterializedDocument {
    if (docs_cache.*) |docs| return docs;
    const docs = try loadPublishedDocumentsAlloc(self, namespace, current_head, cancellation);
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

fn allocTextSegmentAlloc(alloc: Allocator, docs: []const query_mod.QueryMaterializedDocument, spec: FullTextIndexSpec, cancellation: ?maintenance_cancellation.Token) !text_segment_mod.Segment {
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
        if (doc_index % 64 == 0) try maintenance_cancellation.check(cancellation);
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
            if (token_count % 256 == 0) try maintenance_cancellation.check(cancellation);
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
        if (idx % 64 == 0) try maintenance_cancellation.check(cancellation);
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
    cancellation: ?maintenance_cancellation.Token,
) !struct {
    payload: ?[]u8,
    vector_count: usize,
} {
    if (vector_source == null) return .{ .payload = null, .vector_count = 0 };
    const metric = vector_source.?.distance_metric orelse fallback_metric;
    var dims: ?usize = null;
    var count: usize = 0;
    for (docs, 0..) |doc, doc_index| {
        if (doc_index % 64 == 0) try maintenance_cancellation.check(cancellation);
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

    for (docs, 0..) |doc, doc_index| {
        if (doc_index % 64 == 0) try maintenance_cancellation.check(cancellation);
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

    var segment = try vector_index.buildClusteredSegmentWithPolicyAllocUntil(
        alloc,
        metric,
        @intCast(dims.?),
        entries,
        policy orelse .{},
        cancellation,
    );
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
    return try buildVectorArtifactRefsForMaterializedDocsAllocUntil(
        alloc,
        artifacts,
        current,
        metric,
        before_docs,
        docs,
        mutations,
        policy,
        named_policies,
        published_search_sources,
        null,
    );
}

pub fn buildVectorArtifactRefsForMaterializedDocsAllocUntil(
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
    cancellation: ?maintenance_cancellation.Token,
) ![]manifest_mod.ArtifactRef {
    const vector_sources = try search_sources.listVectorSourcesAlloc(alloc, published_search_sources);
    defer search_sources.freeVectorSourceDescriptors(alloc, vector_sources);

    var refs = std.ArrayListUnmanaged(manifest_mod.ArtifactRef).empty;
    errdefer freeArtifactRefs(alloc, refs.items);
    for (vector_sources) |source| {
        try maintenance_cancellation.check(cancellation);
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
            cancellation,
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
    cancellation: ?maintenance_cancellation.Token,
) ![]manifest_mod.ArtifactRef {
    const sparse_sources = try search_sources.listSparseSourcesAlloc(alloc, published_search_sources);
    defer search_sources.freeSparseSourceDescriptors(alloc, sparse_sources);
    if (fallback_action == .drop and named_actions.len == 0) return try alloc.alloc(manifest_mod.ArtifactRef, 0);
    if (sparse_sources.len == 0) return try alloc.alloc(manifest_mod.ArtifactRef, 0);

    const current_count = countArtifactsByKind(current, .sparse_segment);
    var refs = std.ArrayListUnmanaged(manifest_mod.ArtifactRef).empty;
    errdefer freeArtifactRefs(alloc, refs.items);
    for (sparse_sources) |source| {
        try maintenance_cancellation.check(cancellation);
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
        const built = try buildSparseSegmentAllocForSource(alloc, docs, source, cancellation);
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
    cancellation: ?maintenance_cancellation.Token,
) ![]manifest_mod.ArtifactRef {
    const vector_sources = try search_sources.listVectorSourcesAlloc(alloc, published_search_sources);
    defer search_sources.freeVectorSourceDescriptors(alloc, vector_sources);
    if (fallback_action == .drop and named_actions.len == 0) return try alloc.alloc(manifest_mod.ArtifactRef, 0);
    if (vector_sources.len == 0) return try alloc.alloc(manifest_mod.ArtifactRef, 0);

    const current_count = countArtifactsByKind(current, .vector_segment);
    var refs = std.ArrayListUnmanaged(manifest_mod.ArtifactRef).empty;
    errdefer freeArtifactRefs(alloc, refs.items);
    for (vector_sources) |source| {
        try maintenance_cancellation.check(cancellation);
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
        const built = try buildVectorSegmentAlloc(alloc, metric, docs, null, source, cancellation);
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

/// One publication's graph uploads share namespace fencing authority, while
/// a fresh nonce prevents a retired attempt's late upload from aliasing a
/// subsequent writer's identical content.
pub fn graphPublicationAttempt(guard: ?work_lease.PublicationGuard, namespace: []const u8, io: std.Io) ![16]u8 {
    const authority = guard orelse return error.GraphPublicationGuardRequired;
    const fence = (try authority.preparePublication(namespace)) orelse return error.GraphPublicationGuardRequired;
    return (try @import("../artifacts/store.zig").UploadScope.forPublication(graph_page_store.PageStore.namespaceDomain(namespace), fence.fencing_token, io)).attempt;
}

/// Builders hold the same durable source protection as queries. The lease is
/// checked at every maintenance checkpoint, including the final HEAD CAS;
/// an expired build cannot continue reading pages GC is allowed to retire.
pub const GraphSourceProtection = struct {
    parent: ?maintenance_cancellation.Token,
    lease: ?graph_read_lease.Lease,
    progress: *catalog_mod.ProgressStore,
    namespace: []const u8,
    version: u64,
    cache: graph_read_lease.Cache = .{},

    pub fn init(progress: *catalog_mod.ProgressStore, namespace: []const u8, parent: ?maintenance_cancellation.Token) !GraphSourceProtection {
        const version = progress.getHead(namespace) catch |err| switch (err) {
            error.FileNotFound => return .{ .parent = parent, .lease = null, .progress = progress, .namespace = namespace, .version = 0 },
            else => return err,
        };
        return initAt(progress, namespace, version, parent);
    }

    pub fn initAt(progress: *catalog_mod.ProgressStore, namespace: []const u8, version: u64, parent: ?maintenance_cancellation.Token) !GraphSourceProtection {
        var cache: graph_read_lease.Cache = .{};
        return .{ .parent = parent, .lease = try cache.acquire(progress, namespace, version), .progress = progress, .namespace = namespace, .version = version };
    }

    pub fn token(self: *GraphSourceProtection, io: std.Io) maintenance_cancellation.Token {
        return .{ .io = io, .cooperative = .{ .ptr = self, .check_fn = check } };
    }

    fn check(ptr: *const anyopaque) !void {
        const self: *GraphSourceProtection = @ptrCast(@alignCast(@constCast(ptr)));
        try maintenance_cancellation.check(self.parent);
        if (self.lease) |lease| {
            // Renew only existing live rights, after validating the writer's
            // authority. A fenced owner cannot reacquire a retired source.
            try lease.check();
            const now = @import("antfly_platform").time.realtimeNs();
            if (lease.unix_deadline -| now < graph_read_lease.reuse_min_ns)
                self.lease = try self.cache.acquire(self.progress, self.namespace, self.version);
        }
    }
};

pub fn buildGraphArtifactRefsForMaterializedDocsAlloc(
    alloc: Allocator,
    artifacts: *artifacts_mod.ArtifactStore,
    source_table: []const u8,
    current: ?manifest_mod.Manifest,
    before_docs: []const query_mod.QueryMaterializedDocument,
    docs: []const query_mod.QueryMaterializedDocument,
    mutations: []const query_mod.QueryMaterializerMutation,
    graph_index_names: []const []u8,
    include_graph: bool,
    attempt: [16]u8,
) ![]manifest_mod.ArtifactRef {
    return try buildGraphArtifactRefsForMaterializedDocsAllocUntil(
        alloc,
        artifacts,
        source_table,
        current,
        before_docs,
        docs,
        mutations,
        graph_index_names,
        include_graph,
        attempt,
        null,
    );
}

pub fn buildGraphArtifactRefsForMaterializedDocsAllocUntil(
    alloc: Allocator,
    artifacts: *artifacts_mod.ArtifactStore,
    source_table: []const u8,
    current: ?manifest_mod.Manifest,
    before_docs: []const query_mod.QueryMaterializedDocument,
    docs: []const query_mod.QueryMaterializedDocument,
    mutations: []const query_mod.QueryMaterializerMutation,
    graph_index_names: []const []u8,
    include_graph: bool,
    attempt: [16]u8,
    cancellation: ?maintenance_cancellation.Token,
) ![]manifest_mod.ArtifactRef {
    if (!include_graph) return try alloc.alloc(manifest_mod.ArtifactRef, 0);

    const changed = try graphProjectionChangedForMutationsAlloc(alloc, source_table, before_docs, docs, mutations, cancellation, .{});
    return buildGraphArtifactRefsFromImpactAllocUntil(alloc, artifacts, source_table, current, docs, mutations, changed, graph_index_names, include_graph, attempt, cancellation);
}

fn buildGraphArtifactRefsFromImpactAllocUntil(
    alloc: Allocator,
    artifacts: *artifacts_mod.ArtifactStore,
    source_table: []const u8,
    current: ?manifest_mod.Manifest,
    docs: []const query_mod.QueryMaterializedDocument,
    mutations: ?[]const query_mod.QueryMaterializerMutation,
    changed: bool,
    graph_index_names: []const []u8,
    include_graph: bool,
    attempt: [16]u8,
    cancellation: ?maintenance_cancellation.Token,
) ![]manifest_mod.ArtifactRef {
    try maintenance_cancellation.check(cancellation);
    if (!include_graph) return try alloc.alloc(manifest_mod.ArtifactRef, 0);
    var working_set = try graph_build_limits.WorkingSetAllocator.init(alloc, .{});
    var operation_artifacts = artifacts.*;
    operation_artifacts.allocator = working_set.allocator();
    return buildGraphPageRefsBoundedAlloc(working_set.allocator(), &operation_artifacts, source_table, current, docs, mutations, changed, graph_index_names, attempt, cancellation) catch |err| return graphPublicationError(err, working_set.limit_exceeded);
}

/// Parses one coalesced after-image at a time. The reusable plan owns only
/// normalized index mutations; no parsed JSON or edge list survives next().
const GraphDocumentReplacements = struct {
    alloc: Allocator,
    source_table: []const u8,
    docs: []const query_mod.QueryMaterializedDocument,
    touched: ?[]const []const u8,
    cancellation: CancellationToken,
    limits: GraphBuildLimits = .{},
    position: usize = 0,
    input_bytes: usize = 0,
    retained_edges: usize = 0,
    parsed: []ParsedGraphEdge = &.{},
    edges: []graph_page_keys.Edge = &.{},

    fn clear(self: *@This()) void {
        freeParsedGraphEdges(self.alloc, self.parsed);
        self.alloc.free(self.edges);
        self.parsed = &.{};
        self.edges = &.{};
    }

    pub fn next(self: *@This()) !?graph_pages.Replacement {
        self.clear();
        try self.cancellation.check();
        const count = if (self.touched) |ids| ids.len else self.docs.len;
        if (count > self.limits.max_rows) return error.LakeSidecarBuildBudgetExceeded;
        if (self.position == count) return null;
        const id = if (self.touched) |ids| ids[self.position] else self.docs[self.position].doc_id;
        const doc = if (self.touched != null) findMaterializedDocument(self.docs, id) else self.docs[self.position];
        self.position += 1;
        const value = doc orelse return .{ .id = id, .edges = null };
        self.input_bytes = std.math.add(usize, self.input_bytes, value.body.len) catch return error.LakeSidecarBuildBudgetExceeded;
        if (self.input_bytes > self.limits.max_input_bytes) return error.LakeSidecarBuildBudgetExceeded;
        self.parsed = try parseGraphEdgesAlloc(self.alloc, value.body);
        self.retained_edges = std.math.add(usize, self.retained_edges, self.parsed.len) catch return error.LakeSidecarBuildBudgetExceeded;
        if (self.retained_edges +| count > self.limits.max_retained_items) return error.LakeSidecarBuildBudgetExceeded;
        self.edges = try self.alloc.alloc(graph_page_keys.Edge, self.parsed.len);
        for (self.parsed, self.edges) |edge, *result| {
            result.* = .{
                .source = id,
                .target = edge.target,
                .kind = edge.edge_type,
                .weight = edge.weight,
                .table = if (edge.target_table) |table| if (std.mem.eql(u8, table, self.source_table)) null else table else null,
            };
        }
        return .{ .id = id, .edges = self.edges };
    }
};

fn buildGraphPageRefsBoundedAlloc(
    alloc: Allocator,
    artifacts: *artifacts_mod.ArtifactStore,
    source_table: []const u8,
    current: ?manifest_mod.Manifest,
    docs: []const query_mod.QueryMaterializedDocument,
    mutations: ?[]const query_mod.QueryMaterializerMutation,
    changed: bool,
    graph_index_names: []const []u8,
    attempt: [16]u8,
    cancellation: ?maintenance_cancellation.Token,
) ![]manifest_mod.ArtifactRef {
    var bridge = maintenance_cancellation.GraphBridge{ .maintenance = cancellation };
    var read_budget: u64 = (GraphBuildLimits{}).max_input_bytes;
    var write_budget: u64 = (GraphBuildLimits{}).max_output_bytes;
    var pages = graph_page_store.PageStore{
        .domain = graph_page_store.PageStore.namespaceDomain(source_table),
        .attempt = attempt,
        .artifacts = artifacts,
        .cancellation = bridge.token(),
        .remaining_read_bytes = &read_budget,
        .remaining_write_bytes = &write_budget,
    };
    const prior_ref: ?manifest_mod.ArtifactRef = if (current) |manifest|
        if (findArtifactIndex(manifest, .graph_segment)) |idx| manifest.artifacts[idx] else null
    else
        null;
    // Serverless is latest-only. A root's namespace is authenticated before
    // loading any child or reusing its identity for another graph alias.
    var prior: graph_pages.Root = .{};
    if (prior_ref) |ref| {
        if (ref.metadata_version != graph_pages.Root.metadata_version) return error.InvalidGraphRoot;
        prior = try pages.loadRoot(alloc, ref);
        if (!std.mem.eql(u8, &prior.domain, &graph_page_store.PageStore.namespaceDomain(source_table)))
            return error.GraphPageDomainMismatch;
    }
    var root_ref: manifest_mod.ArtifactRef = undefined;
    if (!changed and prior_ref != null) {
        root_ref = try cloneArtifactRefAlloc(alloc, prior_ref.?);
    } else {
        var touched: std.StringArrayHashMapUnmanaged(void) = .empty;
        defer touched.deinit(alloc);
        if (prior_ref != null) {
            if (mutations) |items| for (items) |mutation| {
                try bridge.token().check();
                try touched.put(alloc, mutation.doc_id, {});
            };
        }
        var source = GraphDocumentReplacements{
            .alloc = alloc,
            .source_table = source_table,
            .docs = docs,
            .touched = if (prior_ref != null and mutations != null) touched.keys() else null,
            .cancellation = bridge.token(),
        };
        defer source.clear();
        var cache = graph_page_tree.Cache{ .alloc = alloc, .underlying = pages.store() };
        defer cache.deinit();
        // Schema-only republish supplies the same complete source document
        // view. Planning against its prior root preserves content identity
        // when normalized graph facts did not change; WAL plans visit only
        // coalesced touched IDs. Only genuine bootstrap has an empty source.
        const source_root = prior;
        const next = if (source_root.page == null)
            try @import("../graph_segment/page_bootstrap.zig").buildFromSource(alloc, cache.store(), &source, .{})
        else blk: {
            var plan = try graph_pages.planFromSource(alloc, cache.store(), source_root, &source);
            defer plan.deinit();
            break :blk try plan.publish(cache.store(), source_root);
        };
        root_ref = if (prior_ref != null and next.eql(prior))
            try cloneArtifactRefAlloc(alloc, prior_ref.?)
        else
            try pages.publishRoot(alloc, next, "");
    }
    defer freeArtifactRef(alloc, root_ref);
    const refs = try alloc.alloc(manifest_mod.ArtifactRef, @max(1, graph_index_names.len));
    errdefer alloc.free(refs);
    var initialized: usize = 0;
    errdefer for (refs[0..initialized]) |ref| freeArtifactRef(alloc, ref);
    for (refs, 0..) |*ref, idx| {
        ref.* = try cloneArtifactRefAlloc(alloc, root_ref);
        initialized += 1;
        if (ref.name.len > 0) alloc.free(ref.name);
        ref.name = "";
        if (graph_index_names.len != 0) ref.name = try alloc.dupe(u8, graph_index_names[idx]);
    }
    return refs;
}

fn buildGraphArtifactRefsForRepublishAlloc(
    alloc: Allocator,
    artifacts: *artifacts_mod.ArtifactStore,
    source_table: []const u8,
    current: manifest_mod.Manifest,
    docs: []const query_mod.QueryMaterializedDocument,
    graph_index_names: []const []u8,
    action: publication_plan.ArtifactAction,
    include_graph: bool,
    attempt: [16]u8,
    cancellation: ?maintenance_cancellation.Token,
) ![]manifest_mod.ArtifactRef {
    return buildGraphArtifactRefsFromImpactAllocUntil(alloc, artifacts, source_table, current, docs, null, action != .reuse, graph_index_names, include_graph and action != .drop, attempt, cancellation);
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

pub fn concatArtifactRefSlicesAlloc(
    alloc: Allocator,
    first: []const manifest_mod.ArtifactRef,
    second: []const manifest_mod.ArtifactRef,
) ![]manifest_mod.ArtifactRef {
    const total = std.math.add(usize, first.len, second.len) catch return error.OutOfMemory;
    const refs = try alloc.alloc(manifest_mod.ArtifactRef, total);
    @memcpy(refs[0..first.len], first);
    @memcpy(refs[first.len..], second);
    return refs;
}

fn findArtifactRefByName(
    refs: []const manifest_mod.ArtifactRef,
    kind: manifest_mod.ArtifactKind,
    name: []const u8,
) ?manifest_mod.ArtifactRef {
    for (refs) |ref| if (ref.kind == kind and std.mem.eql(u8, ref.name, name)) return ref;
    if (refs.len == 1 and refs[0].kind == kind) return refs[0];
    return null;
}

fn stampGraphTopologyGenerations(
    graph_refs: []manifest_mod.ArtifactRef,
    current: ?manifest_mod.Manifest,
    next_generation: u64,
) void {
    for (graph_refs) |*graph_ref| {
        graph_ref.edge_generation = next_generation;
        const manifest = current orelse continue;
        // Aliases name a shared immutable topology, not a new edge generation.
        // A rename may have no same-name prior ref while its source is exactly
        // the authenticated root already published under another graph alias.
        const previous_graph = previous: {
            for (manifest.artifacts) |candidate| {
                if (candidate.kind == .graph_segment and artifactRefsIdentifySamePayload(candidate, graph_ref.*)) break :previous candidate;
            }
            continue;
        };

        if (previous_graph.edge_generation != 0) {
            graph_ref.edge_generation = previous_graph.edge_generation;
            continue;
        }

        // An unstamped graph first acquires metric provenance now.
    }
}

pub fn buildGraphMetricArtifactRefsAlloc(
    alloc: Allocator,
    artifacts: *artifacts_mod.ArtifactStore,
    current: ?manifest_mod.Manifest,
    graph_refs: []manifest_mod.ArtifactRef,
    specs: []const graph_metric_config.IndexSpec,
    maintenance: ?maintenance_cancellation.Token,
    provenance: lake_graph_metric.Provenance,
    io: ?std.Io,
    max_parallelism: usize,
) ![]manifest_mod.ArtifactRef {
    var bridge = maintenance_cancellation.GraphBridge{ .maintenance = maintenance };
    const cancellation = bridge.token();
    var requests = std.ArrayListUnmanaged(lake_graph_metric.PublicationRequest).empty;
    defer requests.deinit(alloc);

    // Metric-bearing publications pin a shared topology generation.
    if (specs.len > 0) {
        stampGraphTopologyGenerations(graph_refs, current, provenance.edge_generation);
    }

    const graph_metric_limits = lake_graph_metric.Limits{};
    var graph_metric_budget = graph_metric_policy.Budget{ .limits = graph_metric_limits };
    for (specs) |spec| {
        try cancellation.check();
        const graph_ref = findArtifactRefByName(graph_refs, .graph_segment, spec.index_name) orelse continue;
        var effective_provenance = provenance;
        effective_provenance.edge_generation = graph_ref.edge_generation;

        // Submit the complete desired plan. Shared prior-artifact resolution
        // happens before any dirty computation consumes the build budget.
        for (spec.configs) |config| {
            var request = lake_graph_metric.PublicationRequest{
                .graph_index_name = spec.index_name,
                .source_graph = graph_ref,
                .config = config,
                .provenance = effective_provenance,
            };
            if (current) |manifest| {
                const name = try graph_metric_segment_mod.artifactNameAlloc(alloc, spec.index_name, config.name);
                defer alloc.free(name);
                if (findNamedArtifactIndex(manifest, .graph_metric_segment, name)) |metric_index| {
                    const prior = manifest.artifacts[metric_index];
                    request.prior_artifact = prior;
                }
            }
            try requests.append(alloc, request);
        }
    }
    return try lake_graph_metric.publishRequestsWithPriorAlloc(alloc, artifacts, requests.items, if (current) |manifest| manifest.artifacts else &.{}, cancellation, graph_metric_limits, &graph_metric_budget, .{
        .io = io,
        .max_parallelism = if (io == null) 1 else max_parallelism,
    });
}

fn artifactRefsIdentifySamePayload(lhs: manifest_mod.ArtifactRef, rhs: manifest_mod.ArtifactRef) bool {
    return lhs.byte_len == rhs.byte_len and
        std.mem.eql(u8, lhs.artifact_id, rhs.artifact_id) and
        std.mem.eql(u8, lhs.checksum, rhs.checksum);
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
        try out.ensureUnusedCapacity(alloc, 1);
        out.appendAssumeCapacity(.{
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
    graph_changed: bool,
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
                try out.ensureUnusedCapacity(alloc, 1);
                out.appendAssumeCapacity(.{
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
                try out.ensureUnusedCapacity(alloc, 1);
                out.appendAssumeCapacity(.{
                    .name = try alloc.dupe(u8, source.index_name),
                    .action = action,
                });
            }
        },
        .graph => {
            for (planned_actions) |planned_item| {
                const action = if (planned_item.action == .drop)
                    publication_plan.ArtifactAction.drop
                else if (findArtifactIndex(current, artifact_kind) != null and !graph_changed)
                    publication_plan.ArtifactAction.reuse
                else
                    publication_plan.ArtifactAction.rebuild;
                try out.ensureUnusedCapacity(alloc, 1);
                out.appendAssumeCapacity(.{
                    .name = try alloc.dupe(u8, planned_item.name),
                    .action = action,
                });
            }
        },
    }

    return try out.toOwnedSlice(alloc);
}

test "serverless pending graph prediction shares existing roots across multiple aliases" {
    const a = std.testing.allocator;
    const current: manifest_mod.Manifest = .{
        .namespace = "docs",
        .version = 1,
        .built_at_ns = 1,
        .wal_start_lsn = 1,
        .wal_end_lsn = 1,
        .stats = .{},
        .artifacts = @constCast(&[_]manifest_mod.ArtifactRef{
            .{ .kind = .graph_segment, .name = "a", .artifact_id = "root", .byte_len = 1, .checksum = "checksum" },
            .{ .kind = .graph_segment, .name = "b", .artifact_id = "root", .byte_len = 1, .checksum = "checksum" },
        }),
    };
    const planned = [_]publication_plan.NamedArtifactAction{
        .{ .name = @constCast("new_alias"), .action = .reuse },
        .{ .name = @constCast("a"), .action = .drop },
    };
    for ([_]bool{ false, true }) |changed| {
        const actions = try predictNamedArtifactActionsAlloc(a, current, .graph_segment, &.{}, &.{}, &.{}, .{}, .graph, &planned, .reuse, changed);
        defer {
            for (actions) |*action| action.deinit(a);
            a.free(actions);
        }
        try std.testing.expectEqual(@as(usize, 2), actions.len);
        try std.testing.expectEqual(if (changed) publication_plan.ArtifactAction.rebuild else publication_plan.ArtifactAction.reuse, actions[0].action);
        try std.testing.expectEqual(publication_plan.ArtifactAction.drop, actions[1].action);
    }
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

pub const GraphSegmentBuildResult = struct { payload: ?[]u8, edge_count: usize };

/// Benchmark oracle only: the former string-expanded graph construction.
pub fn benchmarkReferenceGraphSegmentAlloc(
    alloc: Allocator,
    source_table: []const u8,
    docs: []const query_mod.QueryMaterializedDocument,
    include_graph: bool,
) !GraphSegmentBuildResult {
    return try benchmarkReferenceGraphSegmentAllocUntil(alloc, source_table, docs, include_graph, null);
}

fn benchmarkReferenceGraphSegmentAllocUntil(
    alloc: Allocator,
    source_table: []const u8,
    docs: []const query_mod.QueryMaterializedDocument,
    include_graph: bool,
    cancellation: ?maintenance_cancellation.Token,
) !GraphSegmentBuildResult {
    if (!include_graph) return .{ .payload = null, .edge_count = 0 };
    var node_map = std.StringArrayHashMapUnmanaged(NodeEdges).empty;
    defer deinitNodeMap(alloc, &node_map);
    var neighbor_tables = std.StringArrayHashMapUnmanaged(void).empty;
    defer deinitNeighborTableMap(alloc, &neighbor_tables);
    var total_edges: usize = 0;

    for (docs, 0..) |doc, doc_index| {
        if (doc_index % 64 == 0) try maintenance_cancellation.check(cancellation);
        _ = try ensureNode(alloc, &node_map, doc.doc_id);
        const parsed_edges = try parseGraphEdgesAlloc(alloc, doc.body);
        defer freeParsedGraphEdges(alloc, parsed_edges);
        for (parsed_edges) |edge| {
            const src = try ensureNode(alloc, &node_map, doc.doc_id);
            const target_table = if (edge.target_table) |table|
                if (std.mem.eql(u8, table, source_table)) null else table
            else
                null;
            const neighbor_table_id = if (target_table) |table|
                try internNeighborTable(alloc, &neighbor_tables, table)
            else
                null;
            try src.out_edges.append(alloc, .{
                .neighbor_id = try alloc.dupe(u8, edge.target),
                .edge_type = try alloc.dupe(u8, edge.edge_type),
                .weight = edge.weight,
                .neighbor_table_id = neighbor_table_id,
            });
            // A qualified endpoint belongs to another table's segment. Do not
            // synthesize it into this table's key space or invent a reverse
            // edge in the source-table artifact.
            if (target_table == null) {
                const dst = try ensureNode(alloc, &node_map, edge.target);
                try dst.in_edges.append(alloc, .{
                    .neighbor_id = try alloc.dupe(u8, doc.doc_id),
                    .edge_type = try alloc.dupe(u8, edge.edge_type),
                    .weight = edge.weight,
                });
            }
            total_edges += 1;
        }
    }

    if (total_edges == 0) return .{ .payload = null, .edge_count = 0 };

    var segment = try nodeMapToSegmentAlloc(alloc, &node_map, &neighbor_tables, cancellation);
    defer graph_segment_mod.freeSegment(alloc, &segment);
    return .{
        .payload = try graph_segment_mod.encodeAlloc(alloc, segment),
        .edge_count = total_edges,
    };
}

pub fn buildGraphSegmentAlloc(
    alloc: Allocator,
    source_table: []const u8,
    docs: []const query_mod.QueryMaterializedDocument,
    include_graph: bool,
) !GraphSegmentBuildResult {
    return buildGraphSegmentAllocUntil(alloc, source_table, docs, include_graph, null);
}

pub fn buildGraphSegmentAllocUntil(
    alloc: Allocator,
    source_table: []const u8,
    docs: []const query_mod.QueryMaterializedDocument,
    include_graph: bool,
    maintenance: ?maintenance_cancellation.Token,
) !GraphSegmentBuildResult {
    return buildGraphSegmentWithLimitsAlloc(alloc, source_table, docs, include_graph, maintenance, .{});
}

/// The WAL and lake paths share the same bounded construction contract.
/// Returned bytes belong to the backing allocator, never the stack limiter.
pub fn buildGraphSegmentWithLimitsAlloc(
    alloc: Allocator,
    source_table: []const u8,
    docs: []const query_mod.QueryMaterializedDocument,
    include_graph: bool,
    maintenance: ?maintenance_cancellation.Token,
    limits: GraphBuildLimits,
) !GraphSegmentBuildResult {
    var working_set = try graph_build_limits.WorkingSetAllocator.init(alloc, limits);
    return buildGraphSegmentBoundedAlloc(working_set.allocator(), source_table, docs, include_graph, maintenance, limits) catch |err| {
        if ((err == error.OutOfMemory and working_set.limit_exceeded) or err == error.GraphSegmentTooLarge)
            return error.LakeSidecarBuildBudgetExceeded;
        return err;
    };
}

fn buildGraphSegmentBoundedAlloc(
    alloc: Allocator,
    source_table: []const u8,
    docs: []const query_mod.QueryMaterializedDocument,
    include_graph: bool,
    maintenance: ?maintenance_cancellation.Token,
    limits: GraphBuildLimits,
) !GraphSegmentBuildResult {
    var bridge = maintenance_cancellation.GraphBridge{ .maintenance = maintenance };
    const cancellation = bridge.token();
    try cancellation.check();
    if (!include_graph) return .{ .payload = null, .edge_count = 0 };
    if (docs.len > limits.max_rows) return error.LakeSidecarBuildBudgetExceeded;
    var builder = graph_segment_mod.Builder{ .alloc = alloc };
    defer builder.deinit();
    var input_bytes: usize = 0;

    for (docs) |doc| {
        try cancellation.check();
        input_bytes = std.math.add(usize, input_bytes, doc.body.len) catch return error.LakeSidecarBuildBudgetExceeded;
        if (input_bytes > limits.max_input_bytes) return error.LakeSidecarBuildBudgetExceeded;
        try builder.addNode(doc.doc_id);
        const parsed_edges = try parseGraphEdgesAlloc(alloc, doc.body);
        defer freeParsedGraphEdges(alloc, parsed_edges);
        for (parsed_edges, 0..) |edge, i| {
            if (i % 4096 == 0) try cancellation.check();
            if (builder.edges.items.len >= limits.max_retained_items) return error.LakeSidecarBuildBudgetExceeded;
            const target_table = if (edge.target_table) |table|
                if (std.mem.eql(u8, table, source_table)) null else table
            else
                null;
            try builder.addEdge(doc.doc_id, edge.target, edge.edge_type, edge.weight, target_table);
        }
        if (builder.nodes.values.count() +| builder.edges.items.len > limits.max_retained_items)
            return error.LakeSidecarBuildBudgetExceeded;
    }

    if (builder.edges.items.len == 0) return .{ .payload = null, .edge_count = 0 };
    return .{
        .payload = try builder.encodeAlloc(limits.max_output_bytes, cancellation),
        .edge_count = builder.edges.items.len,
    };
}

const ParsedGraphEdge = struct {
    target: []u8,
    edge_type: []u8,
    weight: f32,
    target_table: ?[]u8,
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
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, body, .{}) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return try alloc.alloc(ParsedGraphEdge, 0),
    };
    defer parsed.deinit();
    if (parsed.value != .object) return try alloc.alloc(ParsedGraphEdge, 0);
    const raw_edges = parsed.value.object.get("graph_edges") orelse return try alloc.alloc(ParsedGraphEdge, 0);
    if (raw_edges != .array) return try alloc.alloc(ParsedGraphEdge, 0);

    var out = std.ArrayListUnmanaged(ParsedGraphEdge).empty;
    errdefer {
        for (out.items) |edge| {
            alloc.free(edge.target);
            alloc.free(edge.edge_type);
            if (edge.target_table) |table| alloc.free(table);
        }
        out.deinit(alloc);
    }

    for (raw_edges.array.items) |item| {
        if (item != .object) continue;
        const target_value = item.object.get("target") orelse continue;
        if (target_value != .string or target_value.string.len == 0) continue;
        const edge_type_value = item.object.get("edge_type");
        const weight_value = item.object.get("weight");
        const target_table_value = item.object.get("target_table");
        const owned_target = try alloc.dupe(u8, target_value.string);
        errdefer alloc.free(owned_target);
        const owned_type = try alloc.dupe(u8, if (edge_type_value != null and edge_type_value.? == .string) edge_type_value.?.string else "");
        errdefer alloc.free(owned_type);
        const owned_table = if (target_table_value != null and target_table_value.? == .string and target_table_value.?.string.len > 0)
            try alloc.dupe(u8, target_table_value.?.string)
        else
            null;
        errdefer if (owned_table) |table| alloc.free(table);
        try out.append(alloc, .{
            .target = owned_target,
            .edge_type = owned_type,
            .weight = if (weight_value) |weight| switch (weight) {
                .float => @floatCast(weight.float),
                .integer => @floatFromInt(weight.integer),
                .number_string => std.fmt.parseFloat(f32, weight.number_string) catch 1.0,
                else => 1.0,
            } else 1.0,
            .target_table = owned_table,
        });
    }
    return try out.toOwnedSlice(alloc);
}

fn freeParsedGraphEdges(alloc: Allocator, edges: []ParsedGraphEdge) void {
    for (edges) |edge| {
        alloc.free(edge.target);
        alloc.free(edge.edge_type);
        if (edge.target_table) |table| alloc.free(table);
    }
    alloc.free(edges);
}

test "serverless graph builder parser propagates allocation failure without losing edges" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, struct {
        fn run(alloc: Allocator) !void {
            const edges = try parseGraphEdgesAlloc(alloc, "{\"graph_edges\":[{\"target\":\"b\",\"edge_type\":\"link\",\"target_table\":\"other\"}]}");
            defer freeParsedGraphEdges(alloc, edges);
            try std.testing.expectEqual(@as(usize, 1), edges.len);
        }
    }.run, .{});
}

test "serverless graph builder admits input scratch identities and output before publication" {
    const a = std.testing.allocator;
    const docs = [_]query_mod.QueryMaterializedDocument{.{
        .doc_id = @constCast("a"),
        .body = @constCast("{\"graph_edges\":[{\"target\":\"b\",\"edge_type\":\"link\"}]}"),
        .last_lsn = 1,
        .last_timestamp_ns = 1,
    }};
    for ([_]GraphBuildLimits{
        .{ .max_input_bytes = 1 },
        .{ .max_working_set_bytes = 1 },
        .{ .max_retained_items = 1 },
        .{ .max_output_bytes = 1 },
    }) |limits| {
        try std.testing.expectError(error.LakeSidecarBuildBudgetExceeded, buildGraphSegmentWithLimitsAlloc(a, "docs", &docs, true, null, limits));
    }
    try std.testing.checkAllAllocationFailures(a, struct {
        fn run(alloc: Allocator, input: []const query_mod.QueryMaterializedDocument) !void {
            const built = try buildGraphSegmentWithLimitsAlloc(alloc, "docs", input, true, null, .{});
            defer if (built.payload) |payload| alloc.free(payload);
            try std.testing.expectEqual(@as(usize, 1), built.edge_count);
        }
    }.run, .{@as([]const query_mod.QueryMaterializedDocument, &docs)});
}

fn sortParsedGraphEdges(edges: []ParsedGraphEdge) void {
    std.mem.sort(ParsedGraphEdge, edges, {}, lessParsedGraphEdge);
}

fn lessParsedGraphEdge(_: void, lhs: ParsedGraphEdge, rhs: ParsedGraphEdge) bool {
    const table_order = optionalStringOrder(lhs.target_table, rhs.target_table);
    if (table_order != .eq) return table_order == .lt;
    const edge_type_order = std.mem.order(u8, lhs.edge_type, rhs.edge_type);
    if (edge_type_order != .eq) return edge_type_order == .lt;
    const target_order = std.mem.order(u8, lhs.target, rhs.target);
    if (target_order != .eq) return target_order == .lt;
    return lhs.weight < rhs.weight;
}

fn nodeMapToSegmentAlloc(
    alloc: Allocator,
    node_map: *std.StringArrayHashMapUnmanaged(NodeEdges),
    neighbor_table_map: *const std.StringArrayHashMapUnmanaged(void),
    cancellation: ?maintenance_cancellation.Token,
) !graph_segment_mod.Segment {
    const neighbor_tables = try alloc.alloc([]u8, neighbor_table_map.count());
    errdefer if (neighbor_tables.len > 0) alloc.free(neighbor_tables);
    var initialized_tables: usize = 0;
    errdefer for (neighbor_tables[0..initialized_tables]) |table| alloc.free(table);
    for (neighbor_table_map.keys(), 0..) |table, idx| {
        neighbor_tables[idx] = try alloc.dupe(u8, table);
        initialized_tables += 1;
    }
    const adjacencies = try alloc.alloc(graph_segment_mod.Adjacency, node_map.count());
    errdefer alloc.free(adjacencies);
    var initialized: usize = 0;
    errdefer {
        for (adjacencies[0..initialized]) |*adjacency| adjacency.deinit(alloc);
    }

    for (node_map.keys(), node_map.values(), 0..) |node_id, *node_edges, idx| {
        if (idx % 64 == 0) try maintenance_cancellation.check(cancellation);
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
    return .{ .neighbor_tables = neighbor_tables, .adjacencies = adjacencies };
}

fn internNeighborTable(
    alloc: Allocator,
    tables: *std.StringArrayHashMapUnmanaged(void),
    table: []const u8,
) !u32 {
    if (tables.getIndex(table)) |index| return std.math.cast(u32, index) orelse error.GraphSegmentTooLarge;
    const next_id = std.math.cast(u32, tables.count()) orelse return error.GraphSegmentTooLarge;
    const owned = try alloc.dupe(u8, table);
    errdefer alloc.free(owned);
    const gop = try tables.getOrPut(alloc, owned);
    std.debug.assert(!gop.found_existing);
    std.debug.assert(gop.index == @as(usize, next_id));
    return next_id;
}

fn deinitNeighborTableMap(alloc: Allocator, tables: *std.StringArrayHashMapUnmanaged(void)) void {
    for (tables.keys()) |table| alloc.free(table);
    tables.deinit(alloc);
}

fn optionalStringOrder(lhs: ?[]const u8, rhs: ?[]const u8) std.math.Order {
    if (lhs == null and rhs == null) return .eq;
    if (lhs == null) return .lt;
    if (rhs == null) return .gt;
    return std.mem.order(u8, lhs.?, rhs.?);
}

fn optionalStringsEqual(lhs: ?[]const u8, rhs: ?[]const u8) bool {
    if (lhs == null or rhs == null) return lhs == null and rhs == null;
    return std.mem.eql(u8, lhs.?, rhs.?);
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
    cancellation: ?maintenance_cancellation.Token,
) !?sparse_segment_mod.Segment {
    var sparse_doc_count: usize = 0;
    for (docs, 0..) |doc, doc_index| {
        if (doc_index % 64 == 0) try maintenance_cancellation.check(cancellation);
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

    for (docs, 0..) |doc, doc_index| {
        if (doc_index % 64 == 0) try maintenance_cancellation.check(cancellation);
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

test "serverless builder publishes first manifest from WAL and query sees mutation segment artifact" {
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
    try std.testing.expectEqual(@as(usize, 5), result.artifact_count);
    try std.testing.expectEqual(@as(u64, 1), try progress_store.getHead("docs"));

    var runtime = query_mod.QueryRuntime.init(alloc, &artifact_store, &manifest_store, &progress_store);
    defer runtime.deinit();
    var session = try runtime.openHeadSession("docs");
    defer session.deinit();

    for (session.manifest.artifacts) |artifact| {
        const scope = (try @import("../artifacts/store.zig").uploadScopeFromArtifactId(artifact.artifact_id)).?;
        try std.testing.expectEqual(graph_page_store.PageStore.namespaceDomain("docs"), scope.domain);
        try std.testing.expectEqual(session.manifest.publication_fencing_token, scope.fencingToken());
    }
    try std.testing.expect(artifact_store.upload_scope == null);
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

test "serverless builder retries interrupted publication with a new fenced immutable candidate" {
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

    var builder = Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    const Interrupt = struct {
        fn reach(_: *anyopaque, event: PublicationLifecycleEvent) !void {
            try std.testing.expectEqual(@as(?u64, null), event.expected_head);
            try std.testing.expectEqual(@as(u64, 1), event.candidate_version);
            return error.TestPublicationInterrupted;
        }
    };
    var hook_context: u8 = 0;
    builder.setPublicationLifecycleHook(.{ .ptr = &hook_context, .reach_fn = Interrupt.reach });
    try std.testing.expectError(error.TestPublicationInterrupted, builder.publishNamespace("docs"));
    try std.testing.expectError(error.FileNotFound, progress_store.getHead("docs"));
    var orphan = try manifest_store.getAlloc("docs", 1);
    defer orphan.deinit(alloc);
    try std.testing.expect(orphan.publication_fencing_token != 0);
    const orphan_facts = orphan.artifacts[findArtifactIndex(orphan, .document_facts).?];
    builder.setPublicationLifecycleHook(null);
    var result = try builder.publishNamespace("docs");
    defer result.deinit(alloc);
    try std.testing.expect(result.published);
    // Immutable candidates from different fencing attempts cannot overwrite or
    // adopt each other's scoped uploads, even for the same logical WAL input.
    try std.testing.expectEqual(@as(u64, 2), result.version);
    try std.testing.expectEqual(result.version, try progress_store.getHead("docs"));
    var current = try manifest_store.getAlloc("docs", result.version);
    defer current.deinit(alloc);
    try std.testing.expect(current.publication_fencing_token > orphan.publication_fencing_token);
    try std.testing.expectEqual(orphan.wal_end_lsn, current.wal_end_lsn);
    const current_facts = current.artifacts[findArtifactIndex(current, .document_facts).?];
    try std.testing.expect(!std.mem.eql(u8, orphan_facts.artifact_id, current_facts.artifact_id));
    var retained_orphan = try manifest_store.getAlloc("docs", 1);
    defer retained_orphan.deinit(alloc);
    try std.testing.expectEqual(orphan.publication_fencing_token, retained_orphan.publication_fencing_token);
    try std.testing.expectEqualStrings(orphan_facts.artifact_id, retained_orphan.artifacts[findArtifactIndex(retained_orphan, .document_facts).?].artifact_id);
}

test "serverless builder advances version only when new WAL data exists" {
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

test "serverless resolve published text specs includes chunk preview for chunker full text config" {
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

test "serverless builder reuses unchanged artifacts during metadata-only republish" {
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

test "serverless builder recomputes chunk embeddings derived output during metadata-only republish" {
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

test "serverless builder follows named vector and sparse publication actions during metadata-only republish" {
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

test "serverless builder rebuilds named vector with updated distance metric during metadata-only republish" {
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

test "serverless builder publishes vector segment when document body carries embedding" {
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
    try std.testing.expectEqual(@as(usize, 6), result.artifact_count);

    var manifest = try manifest_store.getAlloc("docs", 1);
    defer manifest.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 1), manifest.stats.vector_segment_count);
    try std.testing.expectEqualStrings(search_sources.default_chunk_embedding_index_name, manifest.stats.published_search_sources.findVector().?.index_name);
    try std.testing.expectEqual(search_sources.VectorDocumentSource.chunk_embeddings_or_top_level, manifest.stats.published_search_sources.findVector().?.document_source);
    try std.testing.expect(manifest.artifacts[3].kind == .vector_segment);
    try std.testing.expectEqualStrings(search_sources.default_chunk_embedding_index_name, manifest.artifacts[3].name);
}

test "serverless builder publishes vector segment from chunk embeddings when top-level embedding is absent" {
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

test "serverless builder publishes sparse segment when document body carries sparse embedding" {
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
    try std.testing.expectEqual(@as(usize, 6), result.artifact_count);

    var manifest = try manifest_store.getAlloc("docs", 1);
    defer manifest.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 1), manifest.stats.sparse_segment_count);
    try std.testing.expectEqualStrings(search_sources.default_sparse_embedding_index_name, manifest.stats.published_search_sources.findSparse().?.index_name);
    try std.testing.expectEqual(search_sources.SparseDocumentSource.sparse_embedding, manifest.stats.published_search_sources.findSparse().?.document_source);
    try std.testing.expect(manifest.artifacts[3].kind == .sparse_segment);
    try std.testing.expectEqualStrings(search_sources.default_sparse_embedding_index_name, manifest.artifacts[3].name);
}

test "serverless builder publishes multiple named vector and sparse segments from named embeddings" {
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

test "serverless builder honors per-index vector distance metrics for named embeddings" {
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

test "serverless builder applies named vector build policies per source" {
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

test "serverless builder reuses named vector and sparse artifacts when wal updates do not change those sources" {
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

test "serverless builder rebuilds named vector and sparse artifacts when wal updates change those sources" {
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

test "serverless builder prediction admits allocations and reuses unchanged text projection" {
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

    const prediction_plan = publication_plan.TablePublicationPlan{ .targets = .{ .published_search_sources = search_sources.defaultPublishedSearchSources() } };
    try std.testing.expectError(error.LakeSidecarBuildBudgetExceeded, builder.predictPendingWalPublicationActionsWithLimitsAlloc("docs", .cosine, prediction_plan, null, .{ .max_working_set_bytes = 1 }));
    try std.testing.expectError(error.LakeSidecarBuildBudgetExceeded, builder.predictPendingWalPublicationActionsWithLimitsAlloc("docs", .cosine, prediction_plan, null, .{ .max_input_bytes = 1 }));
    const PredictionCheck = struct {
        fn run(failing: Allocator, owner: *Builder, plan: publication_plan.TablePublicationPlan) !void {
            var borrowed = owner.*;
            borrowed.alloc = failing;
            var prediction = (try borrowed.predictPendingWalPublicationActionsAlloc("docs", .cosine, plan)).?;
            defer prediction.deinit(failing);
        }
    };
    try std.testing.checkAllAllocationFailures(alloc, PredictionCheck.run, .{ &builder, prediction_plan });

    var second_result = try builder.publishNamespace("docs");
    defer second_result.deinit(alloc);
    try std.testing.expect(second_result.published);

    var second_manifest = try manifest_store.getAlloc("docs", 2);
    defer second_manifest.deinit(alloc);
    try std.testing.expectEqualStrings(first_text_id, second_manifest.artifacts[findNamedArtifactIndex(second_manifest, .text_segment, search_sources.default_full_text_index_name).?].artifact_id);
}

test "serverless builder rebuilds full text artifact when wal updates change indexed text" {
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

test "serverless builder publishes graph segment when document body carries graph edges" {
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

test "serverless WAL graph updates use authoritative document facts and reuse flat snapshots" {
    const a = std.testing.allocator;
    var artifact_path_buf: [256]u8 = undefined;
    var manifest_path_buf: [256]u8 = undefined;
    var wal_path_buf: [256]u8 = undefined;
    const artifact_path = tmpPath(&artifact_path_buf, "facts-wal-artifacts");
    const manifest_path = tmpPath(&manifest_path_buf, "facts-wal-manifests");
    const wal_path = tmpPath(&wal_path_buf, "facts-wal-log");
    defer cleanupTmp(artifact_path);
    defer cleanupTmp(manifest_path);
    defer cleanupTmp(wal_path);
    var fs_artifacts = try artifacts_mod.FsStore.init(a, std.mem.span(artifact_path));
    var artifacts = fs_artifacts.artifactStore();
    defer artifacts.deinit();
    var fs_manifests = try manifest_mod.FsStore.init(a, std.mem.span(manifest_path));
    var manifests = fs_manifests.manifestStore();
    defer manifests.deinit();
    var fs_wal = try wal_mod.FsStore.init(a, std.mem.span(wal_path));
    var wal = fs_wal.walStore();
    defer wal.deinit();
    var fs_progress = try catalog_mod.FsProgressStore.init(a, std.mem.span(manifest_path));
    var progress = fs_progress.progressStore();
    defer progress.deinit();
    var builder = Builder.init(a, &artifacts, &manifests, &progress, &wal);
    for ([_][]const u8{ "a", "z" }) |id| {
        const encoded = try api_codec.encodeMutationAlloc(a, .{ .kind = .upsert, .doc_id = id, .body = "{\"text\":\"alpha\",\"graph_edges\":[{\"target\":\"b\",\"edge_type\":\"link\"}]}" });
        defer a.free(encoded);
        _ = try wal.append("docs", 1, encoded);
    }
    var initial = try builder.publishNamespace("docs");
    defer initial.deinit(a);
    var before = try manifests.getAlloc("docs", initial.version);
    defer before.deinit(a);
    try std.testing.expect(findArtifactIndex(before, .document_facts) != null);
    const snapshot_id = before.artifacts[findArtifactIndex(before, .document_segment).?].artifact_id;
    const encoded = try api_codec.encodeMutationAlloc(a, .{ .kind = .upsert, .doc_id = "a", .body = "{\"text\":\"alpha\",\"graph_edges\":[{\"target\":\"c\",\"edge_type\":\"link\"}]}" });
    defer a.free(encoded);
    _ = try wal.append("docs", 2, encoded);
    var updated = try builder.publishNamespace("docs");
    defer updated.deinit(a);
    var after = try manifests.getAlloc("docs", updated.version);
    defer after.deinit(a);
    try std.testing.expectEqual(@as(u64, 2), after.stats.document_count);
    try std.testing.expectEqualStrings(snapshot_id, after.artifacts[findArtifactIndex(after, .document_segment).?].artifact_id);
    try std.testing.expect(!std.mem.eql(u8, before.artifacts[findArtifactIndex(before, .graph_segment).?].artifact_id, after.artifacts[findArtifactIndex(after, .graph_segment).?].artifact_id));
    const docs = try loadPublishedDocumentsAlloc(&builder, "docs", updated.version, null);
    defer query_mod.freeMaterializedDocuments(a, docs);
    try std.testing.expectEqual(@as(usize, 2), docs.len);
    try std.testing.expect(std.mem.indexOf(u8, docs[0].body, "\"target\":\"c\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, docs[1].body, "\"target\":\"b\"") != null);
}

test "serverless graph page publication coalesces touched sources and shares alias roots" {
    const a = std.testing.allocator;
    var root_buf: [256]u8 = undefined;
    const path = tmpPath(&root_buf, "graph-pages-producer");
    defer cleanupTmp(path);
    var fs = try artifacts_mod.FsStore.init(a, std.mem.span(path));
    var artifacts = fs.artifactStore();
    defer artifacts.deinit();
    const before = [_]query_mod.QueryMaterializedDocument{
        .{ .doc_id = @constCast("a"), .body = @constCast("{\"graph_edges\":[{\"target\":\"b\",\"edge_type\":\"link\"}]}"), .last_lsn = 1, .last_timestamp_ns = 1 },
        .{ .doc_id = @constCast("z"), .body = @constCast("{\"graph_edges\":[{\"target\":\"remote\",\"target_table\":\"other\",\"edge_type\":\"link\"}]}"), .last_lsn = 2, .last_timestamp_ns = 2 },
    };
    const names = [_][]u8{ @constCast("first"), @constCast("second") };
    const first = try buildGraphArtifactRefsFromImpactAllocUntil(a, &artifacts, "docs", null, &before, null, true, &names, true, @splat(1), null);
    defer freeArtifactRefs(a, first);
    try std.testing.expectEqual(@as(usize, 2), first.len);
    try std.testing.expectEqualStrings(first[0].artifact_id, first[1].artifact_id);
    try std.testing.expectEqual(graph_pages.Root.metadata_version, first[0].metadata_version);
    const current: manifest_mod.Manifest = .{ .namespace = @constCast("docs"), .version = 1, .built_at_ns = 1, .wal_start_lsn = 1, .wal_end_lsn = 2, .stats = .{}, .artifacts = first };
    const after = [_]query_mod.QueryMaterializedDocument{
        .{ .doc_id = @constCast("a"), .body = @constCast("{}"), .last_lsn = 4, .last_timestamp_ns = 4 },
        // Deliberately poisonous if reparsed: this source is not touched.
        .{ .doc_id = @constCast("z"), .body = @constCast("{\"graph_edges\":[{\"target\":\"bad\",\"edge_type\":\"\"}]}"), .last_lsn = 2, .last_timestamp_ns = 2 },
    };
    const mutations = [_]query_mod.QueryMaterializerMutation{
        .{ .kind = .upsert, .doc_id = "a", .body = "{}", .lsn = 3, .timestamp_ns = 3 },
        .{ .kind = .upsert, .doc_id = "a", .body = "{}", .lsn = 4, .timestamp_ns = 4 },
    };
    const next = try buildGraphArtifactRefsFromImpactAllocUntil(a, &artifacts, "docs", current, &after, &mutations, true, &names, true, @splat(2), null);
    defer freeArtifactRefs(a, next);
    var reads: u64 = std.math.maxInt(u64);
    var writes: u64 = 0;
    var pages = graph_page_store.PageStore{ .artifacts = &artifacts, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
    const root = try pages.loadRoot(a, next[0]);
    try std.testing.expectEqual(@as(u64, 2), root.nodes);
    try std.testing.expectEqual(@as(u64, 1), root.edges);
    try std.testing.expectEqualStrings(next[0].artifact_id, next[1].artifact_id);
    const reused = try buildGraphArtifactRefsFromImpactAllocUntil(a, &artifacts, "docs", .{ .namespace = @constCast("docs"), .version = 2, .built_at_ns = 2, .wal_start_lsn = 3, .wal_end_lsn = 4, .stats = .{}, .artifacts = next }, &after, &.{}, false, &names, true, @splat(3), null);
    defer freeArtifactRefs(a, reused);
    try std.testing.expectEqualStrings(next[0].artifact_id, reused[0].artifact_id);
}

test "serverless graph segment preserves qualified endpoints without local key aliasing" {
    const alloc = std.testing.allocator;
    const docs = [_]query_mod.QueryMaterializedDocument{.{
        .doc_id = @constCast("doc-g"),
        .body = @constCast("{\"graph_edges\":[{\"target\":\"shared\",\"target_table\":\"entities\",\"edge_type\":\"mentions\"},{\"target\":\"doc-h\",\"target_table\":\"docs\",\"edge_type\":\"related\"}]}"),
        .last_lsn = 1,
        .last_timestamp_ns = 1,
    }};
    const built = try buildGraphSegmentAlloc(alloc, "docs", &docs, true);
    defer if (built.payload) |payload| alloc.free(payload);

    var segment = try graph_segment_mod.decodeAlloc(alloc, built.payload.?);
    defer graph_segment_mod.freeSegment(alloc, &segment);
    try std.testing.expectEqual(@as(usize, 1), segment.neighbor_tables.len);
    try std.testing.expectEqualStrings("entities", segment.neighbor_tables[0]);
    try std.testing.expectEqual(@as(usize, 2), segment.adjacencies.len);
    var adjacency_index = try graph_segment_mod.AdjacencyIndex.init(alloc, segment);
    defer adjacency_index.deinit(alloc);
    const source = adjacency_index.find(segment, "doc-g").?;
    try std.testing.expectEqual(@as(usize, 2), source.out_edges.len);
    try std.testing.expectEqualStrings("entities", segment.neighborTable(source.out_edges[0]).?);
    try std.testing.expect(adjacency_index.find(segment, "shared") == null);
    try std.testing.expectEqual(@as(usize, 1), adjacency_index.find(segment, "doc-h").?.in_edges.len);
}

test "serverless builder publishes named graph segments for graph indexes" {
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

test "serverless builder warm starts changed graphs and cold starts when prior scores disappear" {
    const alloc = std.testing.allocator;
    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-graph-metric-seed");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-graph-metric-seed");
    const wal_root = tmpPath(&wal_root_buf, "wal-graph-metric-seed");
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

    const plan = publication_plan.TablePublicationPlan{
        .targets = .{
            .published_search_sources = search_sources.defaultPublishedSearchSources(),
            .include_graph = true,
        },
        .table_definition = .{ .indexes_json = @constCast(
            \\{"graph_idx":{"type":"graph","metrics":{"rank":{"kind":"pagerank","max_iterations":1}}}}
        ) },
    };
    var builder = Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    const metric_name = try graph_metric_segment_mod.artifactNameAlloc(alloc, "graph_idx", "rank");
    defer alloc.free(metric_name);
    for ([_][]const u8{ "doc-a", "doc-c", "doc-d" }, 0..) |node, round| {
        const mutation = try api_codec.encodeMutationAlloc(alloc, .{
            .kind = .upsert,
            .doc_id = node,
            .body =
            \\{"graph_edges":[{"target":"doc-b","edge_type":"cites"}]}
            ,
        });
        defer alloc.free(mutation);
        _ = try wal_store.append("docs", @intCast(100 * (round + 1)), mutation);
        var result = try builder.publishNamespaceWithMetricAndPlan("docs", .cosine, plan);
        defer result.deinit(alloc);
        try std.testing.expect(result.published);
        var runtime = query_mod.QueryRuntime.init(alloc, &artifact_store, &manifest_store, &progress_store);
        defer runtime.deinit();
        var session = try runtime.openHeadSession("docs");
        defer session.deinit();
        var top = try query_mod.graphMetricTopAlloc(alloc, &session, "graph_idx", "rank", 10);
        defer top.deinit(alloc);
        try std.testing.expectEqual(round + 2, top.scores.len);
        try std.testing.expectEqualStrings("doc-b", top.scores[0].node_id);
        // One iteration makes use of the prior seed observable. Round 1 maps
        // [0.2875, 0.7125] onto [a, b, c], giving c zero initial mass.
        const expected_top: f64 = switch (round) {
            0 => 0.7125,
            1 => 0.49625,
            else => 0.728125,
        };
        try std.testing.expectApproxEqAbs(expected_top, top.scores[0].value, 0.0000001);
        if (round == 1) {
            var manifest = try manifest_store.getAlloc("docs", 2);
            defer manifest.deinit(alloc);
            const prior = manifest.artifacts[findNamedArtifactIndex(manifest, .graph_metric_segment, metric_name).?];
            // Missing optional acceleration cannot block the next authoritative
            // graph publication; round 2 must use the four-node cold seed.
            try artifact_store.delete(prior.artifact_id);
        }
    }
}

test "serverless builder publishes and lifecycle-binds configured graph metrics" {
    const alloc = std.testing.allocator;
    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-graph-metric-lifecycle");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-graph-metric-lifecycle");
    const wal_root = tmpPath(&wal_root_buf, "wal-graph-metric-lifecycle");
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

    const indexes_json = try alloc.dupe(u8,
        \\{"graph_idx":{"type":"graph","metrics":{"degree":{"kind":"degree"},"rank":{"kind":"pagerank","max_iterations":20}}}}
    );
    defer alloc.free(indexes_json);
    const plan = publication_plan.TablePublicationPlan{
        .targets = .{
            .published_search_sources = search_sources.defaultPublishedSearchSources(),
            .include_graph = true,
        },
        .table_definition = .{ .indexes_json = indexes_json },
    };
    var builder = Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);

    const first_mutation = try api_codec.encodeMutationAlloc(alloc, .{
        .kind = .upsert,
        .doc_id = "doc-a",
        .body = "{\"text\":\"one\",\"graph_edges\":[{\"target\":\"doc-b\",\"edge_type\":\"cites\"}]}",
    });
    defer alloc.free(first_mutation);
    _ = try wal_store.append("docs", 100, first_mutation);
    var first_result = try builder.publishNamespaceWithMetricAndPlan("docs", .cosine, plan);
    defer first_result.deinit(alloc);
    var first = try manifest_store.getAlloc("docs", 1);
    defer first.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 1), first.stats.graph_segment_count);
    const metric_name = try graph_metric_segment_mod.artifactNameAlloc(alloc, "graph_idx", "rank");
    defer alloc.free(metric_name);
    const degree_metric_name = try graph_metric_segment_mod.artifactNameAlloc(alloc, "graph_idx", "degree");
    defer alloc.free(degree_metric_name);
    const first_graph = first.artifacts[findNamedArtifactIndex(first, .graph_segment, "graph_idx").?];
    try std.testing.expectEqual(graph_pages.Root.metadata_version, first_graph.metadata_version);
    try std.testing.expectEqual(@as(u64, graph_pages.Root.encoded_bytes), first_graph.byte_len);
    const first_metric = first.artifacts[findNamedArtifactIndex(first, .graph_metric_segment, metric_name).?];
    try std.testing.expectEqual(@as(u64, 1), first_graph.edge_generation);
    try std.testing.expectEqual(graph_metric_segment_mod.wire_version, first_metric.metadata_version);
    try std.testing.expectEqual(@as(u64, 1), first_metric.published_generation);
    try std.testing.expectEqual(@as(u64, 1), first_metric.edge_generation);
    try std.testing.expectEqual(lake_graph_metric.materializerFingerprint(.{}), first_metric.materializer_fingerprint);
    try std.testing.expect(findNamedArtifactIndex(first, .graph_metric_segment, degree_metric_name) != null);
    try artifact_store.delete(first_metric.artifact_id);

    const unchanged_graph_mutation = try api_codec.encodeMutationAlloc(alloc, .{
        .kind = .upsert,
        .doc_id = "doc-a",
        .body = "{\"text\":\"two\",\"graph_edges\":[{\"target\":\"doc-b\",\"edge_type\":\"cites\"}]}",
    });
    defer alloc.free(unchanged_graph_mutation);
    _ = try wal_store.append("docs", 200, unchanged_graph_mutation);
    var second_result = try builder.publishNamespaceWithMetricAndPlan("docs", .cosine, plan);
    defer second_result.deinit(alloc);
    var second = try manifest_store.getAlloc("docs", 2);
    defer second.deinit(alloc);
    const second_graph = second.artifacts[findNamedArtifactIndex(second, .graph_segment, "graph_idx").?];
    try std.testing.expectEqualStrings(first_graph.checksum, second_graph.checksum);
    const second_metric = second.artifacts[findNamedArtifactIndex(second, .graph_metric_segment, metric_name).?];
    try std.testing.expectEqualStrings(first_graph.artifact_id, second_graph.artifact_id);
    try std.testing.expectEqual(first_graph.edge_generation, second_graph.edge_generation);
    // Recomputing a missing payload must not resurrect a retired attempt's
    // physical identity, even when the numerical bytes are identical.
    try std.testing.expectEqualStrings(first_metric.checksum, second_metric.checksum);
    try std.testing.expect(!std.mem.eql(u8, first_metric.artifact_id, second_metric.artifact_id));
    const repaired_scope = (try @import("../artifacts/store.zig").uploadScopeFromArtifactId(second_metric.artifact_id)).?;
    try std.testing.expectEqual(second.publication_fencing_token, repaired_scope.fencingToken());
    try std.testing.expectEqual(first_metric.metadata_version, second_metric.metadata_version);
    // The missing payload was recomputed; only unchanged, reusable artifacts
    // retain their previous publication/computation provenance.
    try std.testing.expectEqual(@as(u64, 2), second_metric.published_generation);
    try std.testing.expectEqual(first_metric.edge_generation, second_metric.edge_generation);
    try std.testing.expect(second_metric.computed_at_ms >= first_metric.computed_at_ms);
    const first_degree = first.artifacts[findNamedArtifactIndex(first, .graph_metric_segment, degree_metric_name).?];
    const second_degree = second.artifacts[findNamedArtifactIndex(second, .graph_metric_segment, degree_metric_name).?];
    try std.testing.expectEqualStrings(first_degree.artifact_id, second_degree.artifact_id);
    try std.testing.expectEqual(first_degree.published_generation, second_degree.published_generation);
    try std.testing.expectEqual(first_degree.computed_at_ms, second_degree.computed_at_ms);
    var restored_metric = try artifact_store.stat(second_metric.artifact_id);
    restored_metric.deinit(alloc);

    const changed_graph_mutation = try api_codec.encodeMutationAlloc(alloc, .{
        .kind = .upsert,
        .doc_id = "doc-a",
        .body = "{\"text\":\"three\",\"graph_edges\":[{\"target\":\"doc-c\",\"edge_type\":\"cites\"}]}",
    });
    defer alloc.free(changed_graph_mutation);
    _ = try wal_store.append("docs", 300, changed_graph_mutation);
    var third_result = try builder.publishNamespaceWithMetricAndPlan("docs", .cosine, plan);
    defer third_result.deinit(alloc);
    var third = try manifest_store.getAlloc("docs", 3);
    defer third.deinit(alloc);
    const third_graph = third.artifacts[findNamedArtifactIndex(third, .graph_segment, "graph_idx").?];
    try std.testing.expectEqual(graph_pages.Root.metadata_version, third_graph.metadata_version);
    const third_metric = third.artifacts[findNamedArtifactIndex(third, .graph_metric_segment, metric_name).?];
    const third_degree_metric = third.artifacts[findNamedArtifactIndex(third, .graph_metric_segment, degree_metric_name).?];
    try std.testing.expect(!std.mem.eql(u8, second_graph.artifact_id, third_graph.artifact_id));
    try std.testing.expectEqual(@as(u64, 3), third_graph.edge_generation);
    try std.testing.expect(!std.mem.eql(u8, second_metric.artifact_id, third_metric.artifact_id));
    try std.testing.expectEqual(@as(u64, 3), third_metric.published_generation);
    try std.testing.expectEqual(@as(u64, 3), third_metric.edge_generation);

    var runtime = query_mod.QueryRuntime.init(alloc, &artifact_store, &manifest_store, &progress_store);
    defer runtime.deinit();
    var session = try runtime.openHeadSession("docs");
    defer session.deinit();
    var top = try query_mod.graphMetricTopAlloc(alloc, &session, "graph_idx", "rank", 2);
    defer top.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), top.scores.len);
    try std.testing.expect(top.scores[0].value >= top.scores[1].value);
    try std.testing.expectEqual(@as(u64, 3), top.published_generation);

    const payload = try artifact_store.getVerifiedAllocWithCancellationUsingAllocator(alloc, third_metric.artifact_id, third_metric.byte_len, third_metric.checksum, .none);
    defer alloc.free(payload);
    var decoded = try graph_metric_segment_mod.decodeAlloc(alloc, payload);
    defer decoded.deinit(alloc);
    try std.testing.expectEqualStrings(third_graph.artifact_id, decoded.source_graph_artifact_id);
    try std.testing.expectEqualStrings(third_graph.checksum, decoded.source_graph_checksum);

    const updated_indexes_json = try alloc.dupe(u8,
        \\{"graph_idx":{"type":"graph","metrics":{"degree":{"kind":"degree"},"rank":{"kind":"pagerank","max_iterations":40}}}}
    );
    defer alloc.free(updated_indexes_json);
    var fourth_result = try builder.publishNamespaceWithMetricAndPlan("docs", .cosine, .{
        .targets = plan.targets,
        .table_definition = .{ .indexes_json = updated_indexes_json },
        .metadata_republish = .{ .artifact_families_changed = true },
        .artifact_actions = .{ .document_segment = .reuse, .full_text = .reuse, .dense_vector = .reuse, .sparse_vector = .reuse, .graph = .reuse },
    });
    defer fourth_result.deinit(alloc);
    var fourth = try manifest_store.getAlloc("docs", 4);
    defer fourth.deinit(alloc);
    const fourth_graph = fourth.artifacts[findNamedArtifactIndex(fourth, .graph_segment, "graph_idx").?];
    const fourth_metric = fourth.artifacts[findNamedArtifactIndex(fourth, .graph_metric_segment, metric_name).?];
    const fourth_degree_metric = fourth.artifacts[findNamedArtifactIndex(fourth, .graph_metric_segment, degree_metric_name).?];
    try std.testing.expectEqualStrings(third_graph.artifact_id, fourth_graph.artifact_id);
    try std.testing.expectEqual(third_graph.edge_generation, fourth_graph.edge_generation);
    try std.testing.expect(!std.mem.eql(u8, third_metric.artifact_id, fourth_metric.artifact_id));
    try std.testing.expectEqual(@as(u64, 4), fourth_metric.published_generation);
    try std.testing.expectEqual(third_metric.edge_generation, fourth_metric.edge_generation);
    try std.testing.expectEqualStrings(third_degree_metric.artifact_id, fourth_degree_metric.artifact_id);
    try std.testing.expectEqual(third_degree_metric.published_generation, fourth_degree_metric.published_generation);
    try std.testing.expectEqual(third_degree_metric.edge_generation, fourth_degree_metric.edge_generation);
    try std.testing.expectEqual(third_degree_metric.computed_at_ms, fourth_degree_metric.computed_at_ms);

    const expanded_indexes_json = try alloc.dupe(u8,
        \\{"graph_idx":{"type":"graph","metrics":{"degree":{"kind":"degree"},"rank":{"kind":"pagerank","max_iterations":40},"centrality":{"kind":"eigenvector"},"degree_alias":{"kind":"degree"}}}}
    );
    defer alloc.free(expanded_indexes_json);
    var fifth_result = try builder.publishNamespaceWithMetricAndPlan("docs", .cosine, .{
        .targets = plan.targets,
        .table_definition = .{ .indexes_json = expanded_indexes_json },
        .metadata_republish = .{ .artifact_families_changed = true },
        .artifact_actions = .{ .document_segment = .reuse, .full_text = .reuse, .dense_vector = .reuse, .sparse_vector = .reuse, .graph = .reuse },
    });
    defer fifth_result.deinit(alloc);
    var fifth = try manifest_store.getAlloc("docs", 5);
    defer fifth.deinit(alloc);
    const centrality_metric_name = try graph_metric_segment_mod.artifactNameAlloc(alloc, "graph_idx", "centrality");
    defer alloc.free(centrality_metric_name);
    const fifth_graph = fifth.artifacts[findNamedArtifactIndex(fifth, .graph_segment, "graph_idx").?];
    const fifth_metric = fifth.artifacts[findNamedArtifactIndex(fifth, .graph_metric_segment, metric_name).?];
    const fifth_degree_metric = fifth.artifacts[findNamedArtifactIndex(fifth, .graph_metric_segment, degree_metric_name).?];
    const fifth_centrality_metric = fifth.artifacts[findNamedArtifactIndex(fifth, .graph_metric_segment, centrality_metric_name).?];
    try std.testing.expectEqualStrings(fourth_graph.artifact_id, fifth_graph.artifact_id);
    try std.testing.expectEqual(fourth_graph.edge_generation, fifth_graph.edge_generation);
    try std.testing.expectEqualStrings(fourth_metric.artifact_id, fifth_metric.artifact_id);
    try std.testing.expectEqualStrings(fourth_degree_metric.artifact_id, fifth_degree_metric.artifact_id);
    try std.testing.expectEqual(@as(u64, 5), fifth_centrality_metric.published_generation);
    try std.testing.expectEqual(fourth_graph.edge_generation, fifth_centrality_metric.edge_generation);
    const alias_name = try graph_metric_segment_mod.artifactNameAlloc(alloc, "graph_idx", "degree_alias");
    defer alloc.free(alias_name);
    const alias = fifth.artifacts[findNamedArtifactIndex(fifth, .graph_metric_segment, alias_name).?];
    try std.testing.expectEqualStrings(fourth_degree_metric.artifact_id, alias.artifact_id);
    try std.testing.expectEqual(fourth_degree_metric.computed_at_ms, alias.computed_at_ms);
    try std.testing.expectEqual(@as(u64, 5), alias.published_generation);
    try std.testing.expectEqual(fifth_graph.edge_generation, alias.edge_generation);
}

test "serverless builder reuses graph artifact when wal updates do not change graph projection" {
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

test "serverless builder rebuilds graph artifact when wal updates change graph projection" {
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

test "serverless builder encodes delete mutations in published segment artifact" {
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

test "serverless builder facts keep mutation tails bounded without periodic flat corpus rewrites" {
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
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "{\"text\":\"alpha\",\"extra\":1}" },
    };
    var ingest_first = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 100, .mutations = &first });
    defer ingest_first.deinit(alloc);
    var build_first = try builder.publishNamespaceWithMetricAndPlan("docs", .cosine, .{
        .targets = .{ .published_search_sources = search_sources.defaultPublishedSearchSources() },
        .policy = .{ .compaction_enabled = true, .compaction_trigger_version_count = 2 },
    });
    defer build_first.deinit(alloc);

    const second = [_]api_types.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "{\"text\":\"alpha\",\"extra\":2}" },
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
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "{\"text\":\"alpha\",\"extra\":3}" },
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
    try std.testing.expectEqual(@as(u64, 1), third_manifest.stats.document_base_version);
    try std.testing.expectEqual(catalog_types.DocumentPublishMode.append_mutation_tail, third_manifest.stats.document_publish_mode);
    try std.testing.expectEqualStrings(reused_document_ref.artifact_id, third_manifest.artifacts[findArtifactIndex(third_manifest, .document_segment).?].artifact_id);
    const tail = try artifact_store.getAlloc(third_manifest.artifacts[findArtifactIndex(third_manifest, .mutation_segment).?].artifact_id);
    defer alloc.free(tail);
    const mutations = try segment_mod.decodeAlloc(alloc, tail);
    defer segment_mod.freeEntries(alloc, mutations);
    try std.testing.expectEqual(@as(usize, 1), mutations.len);
    try std.testing.expectEqualStrings("doc-a", mutations[0].doc_id);
    const facts_ref = third_manifest.artifacts[findArtifactIndex(third_manifest, .document_facts).?];
    const documents = try materializeManifestFactsAlloc(alloc, &artifact_store, third_manifest, facts_ref, null);
    defer query_mod.freeMaterializedDocuments(alloc, documents);
    try std.testing.expectEqual(@as(usize, 1), documents.len);
    try std.testing.expectEqualStrings(third[0].body.?, documents[0].body);
}

test "serverless builder publishes initial external manifest without wal records" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-initial-external");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-initial-external");
    const wal_root = tmpPath(&wal_root_buf, "wal-initial-external");
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

    var external_plan = try external_source_manifest.planAlloc(
        alloc,
        .parquet_prefix,
        "s3://bucket/events",
        "snapshot-1",
        "schema-v1",
        .{
            .artifact_id = "external-inventory-1",
            .byte_len = 128,
            .checksum = "sha256:inventory-1",
        },
    );
    defer external_plan.deinit(alloc);

    var builder = Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var result = try builder.publishNamespaceWithMetricAndPlan("events", .cosine, .{
        .targets = .{ .published_search_sources = search_sources.defaultPublishedSearchSources() },
        .external_source_plan = external_plan,
        .table_definition = .{
            .schema_json = @constCast("{\"storage_mode\":\"relational\"}"),
            .read_schema_json = @constCast("{}"),
            .indexes_json = @constCast("{}"),
        },
    });
    defer result.deinit(alloc);

    try std.testing.expect(result.published);
    try std.testing.expectEqual(@as(u64, 1), result.version);
    try std.testing.expectEqual(@as(u64, 1), result.wal_start_lsn);
    try std.testing.expectEqual(@as(u64, 0), result.wal_end_lsn);
    try std.testing.expectEqual(@as(usize, 1), result.artifact_count);

    var manifest = try manifest_store.getAlloc("events", 1);
    defer manifest.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 0), manifest.wal_end_lsn);
    try std.testing.expectEqual(@as(usize, 1), manifest.artifacts.len);
    try std.testing.expectEqual(@as(u64, 0), manifest.stats.document_base_version);
    try std.testing.expect(manifest.stats.published_search_sources.findText() == null);
    try std.testing.expect(manifest.stats.published_search_sources.findVector() == null);
    try std.testing.expect(manifest.stats.published_search_sources.findSparse() == null);
    try std.testing.expectEqual(manifest_mod.ArtifactKind.external_base_source, manifest.artifacts[0].kind);
    try std.testing.expectEqualStrings("external-inventory-1", manifest.artifacts[0].artifact_id);
    try std.testing.expect(manifest.base_source != null);
    try std.testing.expectEqualStrings("snapshot-1", manifest.base_source.?.external_parquet.snapshot_id);

    var unchanged = try builder.publishNamespaceWithMetricAndPlan("events", .cosine, .{
        .targets = .{ .published_search_sources = search_sources.defaultPublishedSearchSources() },
        .external_source_plan = external_plan,
        .table_definition = .{
            .schema_json = @constCast("{\"storage_mode\":\"relational\"}"),
            .read_schema_json = @constCast("{}"),
            .indexes_json = @constCast("{}"),
        },
    });
    defer unchanged.deinit(alloc);
    try std.testing.expect(!unchanged.published);
    try std.testing.expectEqual(@as(u64, 1), unchanged.version);

    // A content-addressed inventory can retain the same bytes while its
    // logical manifest name changes. Consume a stale enrichment record in the
    // same publication and prove the renamed external plan is not mistaken
    // for the current plan merely because its content identity is unchanged.
    var stale_operation_buffer: [128]u8 = undefined;
    try std.testing.expectEqual(
        @as(?u64, 1),
        try wal_store.appendIdempotentIfLatest(
            "events",
            99,
            "ignored-stale-enrichment",
            try @import("../enrichment/operation_id.zig").format(&stale_operation_buffer, 2, 1, 0, 1),
            0,
        ),
    );
    var renamed_plan = try external_source_manifest.planAlloc(
        alloc,
        .parquet_prefix,
        "s3://bucket/events",
        "snapshot-1",
        "schema-v1",
        .{
            .artifact_id = "external-inventory-1",
            .byte_len = 128,
            .checksum = "sha256:inventory-1",
            .name = "events.external-files",
        },
    );
    defer renamed_plan.deinit(alloc);
    var renamed = try builder.publishNamespaceWithMetricAndPlan("events", .cosine, .{
        .targets = .{ .published_search_sources = search_sources.defaultPublishedSearchSources() },
        .external_source_plan = renamed_plan,
        .table_definition = .{
            .schema_json = @constCast("{\"storage_mode\":\"relational\"}"),
            .read_schema_json = @constCast("{}"),
            .indexes_json = @constCast("{}"),
        },
    });
    defer renamed.deinit(alloc);
    try std.testing.expect(renamed.published);
    try std.testing.expectEqual(@as(u64, 2), renamed.version);
    try std.testing.expectEqual(@as(u64, 1), renamed.wal_start_lsn);
    try std.testing.expectEqual(@as(u64, 1), renamed.wal_end_lsn);
    var renamed_manifest = try manifest_store.getAlloc("events", 2);
    defer renamed_manifest.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), renamed_manifest.artifacts.len);
    try std.testing.expectEqualStrings("events.external-files", renamed_manifest.artifacts[0].name);

    var updated_plan = try external_source_manifest.planAlloc(
        alloc,
        .parquet_prefix,
        "s3://bucket/events",
        "snapshot-2",
        "schema-v1",
        .{
            .artifact_id = "external-inventory-2",
            .byte_len = 144,
            .checksum = "sha256:inventory-2",
            .name = "events.external-files",
        },
    );
    defer updated_plan.deinit(alloc);
    var updated = try builder.publishNamespaceWithMetricAndPlan("events", .cosine, .{
        .targets = .{ .published_search_sources = search_sources.defaultPublishedSearchSources() },
        .external_source_plan = updated_plan,
        .table_definition = .{
            .schema_json = @constCast("{\"storage_mode\":\"relational\"}"),
            .read_schema_json = @constCast("{}"),
            .indexes_json = @constCast("{}"),
        },
    });
    defer updated.deinit(alloc);
    try std.testing.expect(updated.published);
    try std.testing.expectEqual(@as(u64, 3), updated.version);

    var updated_manifest = try manifest_store.getAlloc("events", 3);
    defer updated_manifest.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), updated_manifest.artifacts.len);
    try std.testing.expectEqualStrings("external-inventory-2", updated_manifest.artifacts[0].artifact_id);
    try std.testing.expectEqualStrings("snapshot-2", updated_manifest.base_source.?.external_parquet.snapshot_id);

    const encoded_mutation = try api_codec.encodeMutationAlloc(alloc, .{
        .kind = .upsert,
        .doc_id = "event-1",
        .body = "should-not-publish",
    });
    defer alloc.free(encoded_mutation);
    _ = try wal_store.append("events", 100, encoded_mutation);
    try std.testing.expectError(error.ExternalTableReadOnly, builder.publishNamespaceWithMetricAndPlan("events", .cosine, .{
        .targets = .{ .published_search_sources = search_sources.defaultPublishedSearchSources() },
        .external_source_plan = updated_plan,
        .table_definition = .{
            .schema_json = @constCast("{\"storage_mode\":\"relational\"}"),
            .read_schema_json = @constCast("{}"),
            .indexes_json = @constCast("{}"),
        },
    }));
    try std.testing.expectEqual(@as(u64, 3), try progress_store.getHead("events"));
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

test "serverless metadata graph alias publication reuses facts without reading document bodies" {
    try @import("document_facts_publication_bench.zig").metadataRepublishRegression();
}

fn cleanupTmp(path: [*:0]const u8) void {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), std.mem.span(path)) catch {};
}
