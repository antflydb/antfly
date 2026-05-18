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
const catalog_types = @import("types.zig");
const catalog_store = @import("store.zig");
const progress_store_mod = @import("progress_store.zig");
const document_projection = @import("../document_projection.zig");
const document_segment_mod = @import("../document_segment/mod.zig");
const manifest_mod = @import("../manifest/mod.zig");
const query_mod = @import("../query/mod.zig");
const segment_mod = @import("../segment/mod.zig");
const wal_mod = @import("../wal/mod.zig");
const builder_mod = @import("../build/builder.zig");
const impact_planner = @import("../build/impact_planner.zig");
const publication_plan = @import("../build/publication_plan.zig");
const enrichment_pipeline = @import("../enrichment/pipeline.zig");
const api_codec = @import("../api/codec.zig");
const api_types = @import("../api/types.zig");
const search_sources = @import("../search_sources.zig");
const vector_segment_mod = @import("../vector_segment/mod.zig");
const vector_index = @import("../build/vector_index.zig");
const tables_api = @import("../../api/tables.zig");
const full_text_indexes = @import("../../api/full_text_indexes.zig");
const shared_vector = @import("antfly_vector").vector;

pub const CatalogService = struct {
    alloc: Allocator,
    artifacts: *artifacts_mod.ArtifactStore,
    manifests: *manifest_mod.ManifestStore,
    progress: *progress_store_mod.ProgressStore,
    wal: *wal_mod.WalStore,
    builder: *builder_mod.Builder,
    store: *catalog_store.CatalogStore,

    pub fn init(
        alloc: Allocator,
        artifacts: *artifacts_mod.ArtifactStore,
        manifests: *manifest_mod.ManifestStore,
        progress: *progress_store_mod.ProgressStore,
        wal: *wal_mod.WalStore,
        builder: *builder_mod.Builder,
        store: *catalog_store.CatalogStore,
    ) CatalogService {
        return .{
            .alloc = alloc,
            .artifacts = artifacts,
            .manifests = manifests,
            .progress = progress,
            .wal = wal,
            .builder = builder,
            .store = store,
        };
    }

    pub fn deinit(self: *CatalogService) void {
        self.* = undefined;
    }

    pub fn ensureNamespace(self: *CatalogService, name: []const u8, created_at_ns: u64) !bool {
        return try self.ensureNamespaceWithPolicy(name, created_at_ns, .{});
    }

    // The first public table seam keeps one table mapped to one serving namespace.
    // This can be replaced with an explicit serving map later without changing
    // higher-level table-centric callers.
    pub fn ensureTable(self: *CatalogService, table_name: []const u8, created_at_ns: u64) !bool {
        return try self.ensureTableWithPolicy(table_name, created_at_ns, .{});
    }

    pub fn ensureNamespaceWithPolicy(
        self: *CatalogService,
        name: []const u8,
        created_at_ns: u64,
        policy: catalog_types.NamespacePolicy,
    ) !bool {
        return try self.store.ensureNamespace(name, created_at_ns, policy);
    }

    pub fn ensureTableWithPolicy(
        self: *CatalogService,
        table_name: []const u8,
        created_at_ns: u64,
        policy: catalog_types.NamespacePolicy,
    ) !bool {
        return try self.ensureTableWithDefinition(
            table_name,
            created_at_ns,
            policy,
            "",
            "",
            tables_api.default_indexes_json,
        );
    }

    pub fn ensureTableWithDefinition(
        self: *CatalogService,
        table_name: []const u8,
        created_at_ns: u64,
        policy: catalog_types.NamespacePolicy,
        schema_json: []const u8,
        read_schema_json: []const u8,
        indexes_json: []const u8,
    ) !bool {
        const namespace = try self.defaultServingNamespaceAlloc(table_name);
        defer self.alloc.free(namespace);
        return try self.store.ensureTable(
            table_name,
            namespace,
            created_at_ns,
            policy,
            schema_json,
            read_schema_json,
            indexes_json,
        );
    }

    pub fn listNamespacesAlloc(self: *CatalogService, alloc: Allocator) ![]catalog_types.NamespaceRecord {
        return try self.store.listNamespacesAlloc(alloc);
    }

    pub fn listTablesAlloc(self: *CatalogService, alloc: Allocator) ![]catalog_types.TableNamespaceRecord {
        return try self.store.listTablesAlloc(alloc);
    }

    pub fn getTableAlloc(self: *CatalogService, alloc: Allocator, table_name: []const u8) !?catalog_types.TableNamespaceRecord {
        return try self.store.getTableAlloc(alloc, table_name);
    }

    pub fn getTableForNamespaceAlloc(self: *CatalogService, alloc: Allocator, namespace: []const u8) !?catalog_types.TableNamespaceRecord {
        const tables = try self.listTablesAlloc(alloc);
        defer self.freeTables(alloc, tables);

        for (tables) |table| {
            if (!std.mem.eql(u8, table.namespace, namespace)) continue;
            return .{
                .table_name = try alloc.dupe(u8, table.table_name),
                .namespace = try alloc.dupe(u8, table.namespace),
                .created_at_ns = table.created_at_ns,
                .policy = table.policy,
                .schema_json = try alloc.dupe(u8, table.schema_json),
                .read_schema_json = try alloc.dupe(u8, table.read_schema_json),
                .indexes_json = try alloc.dupe(u8, table.indexes_json),
            };
        }
        return null;
    }

    pub fn setTableDefinition(
        self: *CatalogService,
        table_name: []const u8,
        schema_json: []const u8,
        read_schema_json: []const u8,
        indexes_json: []const u8,
    ) !bool {
        return try self.store.setTableDefinition(table_name, schema_json, read_schema_json, indexes_json);
    }

    pub fn freeNamespaces(self: *CatalogService, alloc: Allocator, records: []catalog_types.NamespaceRecord) void {
        _ = self;
        for (records) |*record| record.deinit(alloc);
        alloc.free(records);
    }

    pub fn freeTables(self: *CatalogService, alloc: Allocator, records: []catalog_types.TableNamespaceRecord) void {
        _ = self;
        for (records) |*record| record.deinit(alloc);
        alloc.free(records);
    }

    const PublishedHead = struct {
        progress_version: u64 = 0,
        manifest_version: u64 = 0,
        manifest: ?manifest_mod.Manifest = null,

        fn deinit(self: *PublishedHead, alloc: Allocator) void {
            if (self.manifest) |*manifest| manifest.deinit(alloc);
            self.* = undefined;
        }
    };

    fn loadPublishedHeadAlloc(self: *CatalogService, namespace: []const u8) !PublishedHead {
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

    pub fn buildStatus(self: *CatalogService, namespace: []const u8) !catalog_types.BuildStatus {
        const policy = self.getPolicy(namespace) catch catalog_types.NamespacePolicy{};
        var plan = try self.publicationPlanForNamespaceAlloc(namespace, policy);
        defer plan.deinit(self.alloc);
        const effective_policy = plan.policy;
        var published_head = try self.loadPublishedHeadAlloc(namespace);
        defer published_head.deinit(self.alloc);
        const head_version = published_head.manifest_version;
        const published_wal_end_lsn: u64 = if (published_head.manifest) |manifest|
            manifest.wal_end_lsn
        else
            0;
        const materialized_search_sources: search_sources.PublishedSearchSources = if (published_head.manifest) |manifest|
            try search_sources.clonePublishedSearchSourcesAlloc(
                self.alloc,
                manifest.stats.published_search_sources,
            )
        else
            .{};
        const materialized_derived_outputs: search_sources.MaterializedDerivedOutputs = if (published_head.manifest) |manifest|
            try search_sources.cloneMaterializedDerivedOutputsAlloc(
                self.alloc,
                manifest.stats.derived_outputs,
            )
        else
            .{};
        const latest_wal_lsn = try self.wal.latestLsn(namespace);
        const pending_records = latest_wal_lsn -| published_wal_end_lsn;
        const versions = try self.manifests.listVersionsAlloc(namespace);
        defer self.alloc.free(versions);
        const retained_artifacts = try countRetainedArtifactsAlloc(self.alloc, self.manifests, namespace, versions);
        const head_has_mutation_segment = if (published_head.manifest) |manifest|
            findArtifactIndex(manifest, .mutation_segment) != null
        else
            false;
        const head_document_count = if (published_head.manifest) |manifest| manifest.stats.document_count else 0;
        const head_document_base_version = if (published_head.manifest) |manifest|
            if (manifest.stats.document_base_version == 0) manifest.version else manifest.stats.document_base_version
        else
            0;
        const head_document_publish_mode = if (published_head.manifest) |manifest|
            @as(?catalog_types.DocumentPublishMode, manifest.stats.document_publish_mode)
        else
            null;
        const head_document_lineage_versions: u64 =
            if (head_has_mutation_segment and head_version != 0 and head_document_base_version != 0)
                (head_version - head_document_base_version) + 1
            else
                0;
        const next_document_publish_mode =
            if (plan.forceRepublishFromHead())
                @as(?catalog_types.DocumentPublishMode, .head_republish)
            else if (pending_records > 0)
                @as(?catalog_types.DocumentPublishMode, if (head_has_mutation_segment and effective_policy.compaction_enabled and effective_policy.compaction_trigger_version_count != 0 and
                    head_document_base_version != 0 and
                    ((head_version + 1 - head_document_base_version) + 1) > effective_policy.compaction_trigger_version_count)
                    .inline_rebase
                else
                    .append_mutation_tail)
            else
                null;
        const mutation_tail_compaction_recommended =
            effective_policy.compaction_enabled and
            head_has_mutation_segment and
            head_document_lineage_versions >= effective_policy.compaction_trigger_version_count;
        const mutation_tail_resolution: catalog_types.MutationTailResolution =
            if (mutation_tail_compaction_recommended and next_document_publish_mode == .inline_rebase)
                .next_publish_inline_rebase
            else if (mutation_tail_compaction_recommended)
                .background_compaction
            else
                .none;
        const enrichment_completion = try enrichmentCompletionAlloc(self.alloc, self.artifacts, self.manifests, namespace, head_version, effective_policy);
        const pipeline = enrichment_pipeline.builtinPipelineForPolicy(effective_policy);
        const enrichment_active_stage = chooseActiveEnrichmentStage(pipeline, enrichment_completion);
        const enrichment_head_version = if (enrichment_active_stage) |stage|
            try self.progress.getEnrichmentStageHeadVersion(namespace, stage)
        else
            null;
        const enrichment_doc_offset = if (enrichment_active_stage) |stage|
            (try self.progress.getEnrichmentStageDocOffset(namespace, stage)) orelse 0
        else
            0;
        const enrichment_in_progress =
            enrichment_active_stage != null and
            enrichment_head_version != null and
            enrichment_head_version.? == head_version and
            enrichment_doc_offset < head_document_count;
        var vector_compaction = try vectorCompactionSignalAlloc(self.alloc, self.artifacts, self.manifests, namespace, head_version);
        defer vector_compaction.deinit(self.alloc);
        const vector_compaction_policy = builder_mod.adaptiveVectorBuildPolicyForPolicy(.{
            .metric = vector_compaction.metric orelse .cosine,
            .cluster_count = vector_compaction.cluster_count,
            .base_probe_count = vector_compaction.base_probe_count,
            .shortlist_multiplier = vector_compaction.shortlist_multiplier,
            .cluster_imbalance = vector_compaction.cluster_imbalance,
            .distance_span_max = vector_compaction.distance_span_max,
        }, head_document_count, effective_policy);
        const vector_compaction_recommended =
            vector_compaction.driver_index_name != null and
            builder_mod.vectorBuildPolicyChanges(vector_compaction_policy);
        const vector_target_cluster_count =
            if (vector_compaction_recommended and vector_compaction_policy.target_cluster_count != null)
                @as(?u32, @intCast(vector_compaction_policy.target_cluster_count.?))
            else
                null;
        const vector_target_base_probe_count =
            if (vector_compaction_recommended)
                vector_compaction_policy.base_probe_count
            else
                null;
        const vector_target_shortlist_multiplier =
            if (vector_compaction_recommended)
                vector_compaction_policy.shortlist_multiplier
            else
                null;
        var predicted_pending_wal_enrichment_stage: ?catalog_types.EnrichmentStage = null;
        var predicted_pending_wal_enrichment_document_count: u64 = 0;
        if (pending_records > 0 and !plan.forceRepublishFromHead()) {
            if (try self.builder.predictPendingWalPublicationActionsAlloc(
                namespace,
                effective_policy.vector_distance_metric,
                plan,
            )) |predicted_value| {
                var predicted = predicted_value;
                defer predicted.deinit(self.alloc);
                freeFullTextIndexActions(self.alloc, plan.full_text_index_actions);
                freeNamedArtifactActions(self.alloc, plan.vector_index_actions);
                freeNamedArtifactActions(self.alloc, plan.sparse_index_actions);
                freeNamedArtifactActions(self.alloc, plan.graph_index_actions);
                plan.artifact_actions = predicted.artifact_actions;
                plan.full_text_index_actions = predicted.full_text_index_actions;
                predicted.full_text_index_actions = &.{};
                plan.vector_index_actions = predicted.vector_index_actions;
                predicted.vector_index_actions = &.{};
                plan.sparse_index_actions = predicted.sparse_index_actions;
                predicted.sparse_index_actions = &.{};
                plan.graph_index_actions = predicted.graph_index_actions;
                predicted.graph_index_actions = &.{};
                plan.derived_output_actions = predicted.derived_output_actions;
                predicted_pending_wal_enrichment_stage = predicted.pending_enrichment_stage;
                predicted_pending_wal_enrichment_document_count = predicted.pending_enrichment_document_count;
            }
        }
        const pending_wal_enrichment_stage =
            if (enrichment_active_stage == null and pending_records > 0 and !plan.forceRepublishFromHead())
                predicted_pending_wal_enrichment_stage
            else
                null;
        const effective_enrichment_stage = enrichment_active_stage orelse pending_wal_enrichment_stage;
        const effective_enrichment_stage_source =
            if (enrichment_active_stage != null)
                @as(?catalog_types.EnrichmentStageSource, .current_head)
            else if (pending_wal_enrichment_stage != null)
                @as(?catalog_types.EnrichmentStageSource, .pending_wal)
            else
                null;
        const effective_enrichment_complete = effective_enrichment_stage == null;
        const stage_publish_min_pending_records = stagePublishMinPendingRecords(pipeline, effective_enrichment_stage);
        const publish_deferred_for_enrichment =
            pending_records > 0 and
            effective_enrichment_stage != null and
            pending_records < stage_publish_min_pending_records and
            (enrichment_in_progress or pending_wal_enrichment_stage != null);
        const effective_enrichment_pending_document_count =
            if (enrichment_active_stage != null)
                pendingDocumentsForStage(enrichment_completion, enrichment_active_stage)
            else if (pending_wal_enrichment_stage != null)
                predicted_pending_wal_enrichment_document_count
            else
                0;
        const effective_enrichment_stage_state =
            if (enrichment_active_stage != null)
                @as(?catalog_types.EnrichmentStageState, if (enrichment_in_progress) .executing else .awaiting_execution)
            else if (pending_wal_enrichment_stage != null)
                @as(?catalog_types.EnrichmentStageState, if (publish_deferred_for_enrichment) .deferred_for_publish_threshold else .ready_for_publish)
            else
                null;
        const pending_materialization_families = pendingMaterializationFamilies(
            plan,
            effective_enrichment_stage,
        );
        const derived_output_resolutions = derivedOutputResolutions(
            effective_policy,
            plan,
            pending_materialization_families,
        );
        const next_publish_reason =
            if (plan.forceRepublishFromHead())
                @as(?catalog_types.NextPublishReason, .head_republish)
            else if (pending_records > 0 and pending_wal_enrichment_stage != null)
                @as(?catalog_types.NextPublishReason, .wal_enrichment)
            else if (pending_records > 0)
                @as(?catalog_types.NextPublishReason, .wal_artifact_update)
            else
                null;
        var head_actions = try headPublicationActionsAlloc(self.alloc, self.manifests, namespace, head_version);
        defer head_actions.deinit(self.alloc);
        const head_republish_recommended = plan.forceRepublishFromHead();
        const pending_materialization_rebuild =
            !head_republish_recommended and
            (plan.artifact_actions.any() or plan.derived_output_actions.any());

        return .{
            .namespace = try self.alloc.dupe(u8, namespace),
            .published_search_sources = try search_sources.clonePublishedSearchSourcesAlloc(
                self.alloc,
                plan.targets.published_search_sources,
            ),
            .materialized_search_sources = materialized_search_sources,
            .materialized_derived_outputs = materialized_derived_outputs,
            .head_version = head_version,
            .published_wal_end_lsn = published_wal_end_lsn,
            .latest_wal_lsn = latest_wal_lsn,
            .freshness_lag_records = latest_wal_lsn -| published_wal_end_lsn,
            .pending_records = pending_records,
            .next_version = head_version + 1,
            .publish_admitted = pending_records <= effective_policy.max_pending_records,
            .publish_recommended = (pending_records > 0 and !publish_deferred_for_enrichment) or plan.forceRepublishFromHead(),
            .head_document_publish_mode = head_document_publish_mode,
            .next_document_publish_mode = next_document_publish_mode,
            .document_base_version = head_document_base_version,
            .document_lineage_versions = head_document_lineage_versions,
            .head_republish_recommended = head_republish_recommended,
            .pending_materialization_rebuild = pending_materialization_rebuild,
            .pending_materialization_families = pending_materialization_families,
            .head_artifact_actions = head_actions.artifact_actions,
            .head_full_text_index_actions = try cloneCatalogFullTextIndexActionsAlloc(self.alloc, head_actions.full_text_index_actions),
            .head_vector_index_actions = try cloneCatalogNamedArtifactActionsAlloc(self.alloc, head_actions.vector_index_actions),
            .head_sparse_index_actions = try cloneCatalogNamedArtifactActionsAlloc(self.alloc, head_actions.sparse_index_actions),
            .head_graph_index_actions = try cloneCatalogNamedArtifactActionsAlloc(self.alloc, head_actions.graph_index_actions),
            .head_derived_output_actions = head_actions.derived_output_actions,
            .artifact_actions = .{
                .document_segment = @enumFromInt(@intFromEnum(plan.artifact_actions.document_segment)),
                .full_text = @enumFromInt(@intFromEnum(plan.artifact_actions.full_text)),
                .dense_vector = @enumFromInt(@intFromEnum(plan.artifact_actions.dense_vector)),
                .sparse_vector = @enumFromInt(@intFromEnum(plan.artifact_actions.sparse_vector)),
                .graph = @enumFromInt(@intFromEnum(plan.artifact_actions.graph)),
            },
            .full_text_index_actions = try cloneFullTextIndexActionsAlloc(self.alloc, plan.full_text_index_actions),
            .vector_index_actions = try cloneNamedArtifactActionsAlloc(self.alloc, plan.vector_index_actions),
            .sparse_index_actions = try cloneNamedArtifactActionsAlloc(self.alloc, plan.sparse_index_actions),
            .graph_index_actions = try cloneNamedArtifactActionsAlloc(self.alloc, plan.graph_index_actions),
            .derived_output_actions = .{
                .chunk_preview = @enumFromInt(@intFromEnum(plan.derived_output_actions.chunk_preview)),
                .chunk_embeddings = @enumFromInt(@intFromEnum(plan.derived_output_actions.chunk_embeddings)),
                .rerank_terms = @enumFromInt(@intFromEnum(plan.derived_output_actions.rerank_terms)),
            },
            .derived_output_resolutions = derived_output_resolutions,
            .max_pending_records = effective_policy.max_pending_records,
            .retained_versions = versions.len,
            .retained_artifacts = retained_artifacts,
            .compaction_recommended = mutation_tail_compaction_recommended or vector_compaction_recommended,
            .mutation_tail_compaction_recommended = mutation_tail_compaction_recommended,
            .vector_compaction_recommended = vector_compaction_recommended,
            .mutation_tail_resolution = mutation_tail_resolution,
            .vector_compaction_driver_index_name = if (vector_compaction.driver_index_name) |value| try self.alloc.dupe(u8, value) else null,
            .vector_compaction_distance_metric = vector_compaction.metric,
            .vector_cluster_count = vector_compaction.cluster_count,
            .vector_target_cluster_count = vector_target_cluster_count,
            .vector_base_probe_count = vector_compaction.base_probe_count,
            .vector_target_base_probe_count = vector_target_base_probe_count,
            .vector_shortlist_multiplier = vector_compaction.shortlist_multiplier,
            .vector_target_shortlist_multiplier = vector_target_shortlist_multiplier,
            .vector_cluster_imbalance = vector_compaction.cluster_imbalance,
            .vector_cluster_distance_span_max = vector_compaction.distance_span_max,
            .enrichment_enabled = effective_policy.enrichment_enabled or effective_policy.chunk_preview_enabled or effective_policy.chunk_embeddings_enabled or effective_policy.rerank_terms_enabled,
            .next_publish_reason = next_publish_reason,
            .lexical_sparse_model_preference = effective_policy.lexical_sparse_model_preference,
            .lexical_sparse_complete = enrichment_completion.lexical_sparse_complete,
            .chunk_preview_enabled = effective_policy.chunk_preview_enabled,
            .chunk_preview_complete = enrichment_completion.chunk_preview_complete,
            .chunk_embeddings_enabled = effective_policy.chunk_embeddings_enabled,
            .chunk_embeddings_model_preference = effective_policy.chunk_embeddings_model_preference,
            .chunk_embeddings_complete = enrichment_completion.chunk_embeddings_complete,
            .rerank_terms_enabled = effective_policy.rerank_terms_enabled,
            .rerank_terms_complete = enrichment_completion.rerank_terms_complete,
            .enrichment_failure_policy = effective_policy.enrichment_failure_policy,
            .enrichment_active_stage = effective_enrichment_stage,
            .enrichment_stage_source = effective_enrichment_stage_source,
            .enrichment_stage_state = effective_enrichment_stage_state,
            .enrichment_in_progress = enrichment_in_progress,
            .enrichment_complete = effective_enrichment_complete,
            .enrichment_head_version = enrichment_head_version,
            .enrichment_doc_offset = enrichment_doc_offset,
            .enrichment_total_document_count = head_document_count,
            .enrichment_pending_document_count = effective_enrichment_pending_document_count,
            .enrichment_batch_size = effective_policy.enrichment_batch_size,
            .enrichment_publish_min_pending_records = stage_publish_min_pending_records,
            .enrichment_pipeline_version = if (effective_enrichment_stage) |stage|
                pipeline.stageSpec(stage).?.pipeline_version
            else
                effective_policy.enrichment_pipeline_version,
        };
    }

    fn pendingMaterializationFamilies(
        plan: publication_plan.TablePublicationPlan,
        stage: ?catalog_types.EnrichmentStage,
    ) catalog_types.PendingMaterializationFamilies {
        var out: catalog_types.PendingMaterializationFamilies = .{};
        switch (stage orelse return out) {
            .lexical_sparse => {
                out.sparse_vector = plan.artifact_actions.sparse_vector == .rebuild;
            },
            .chunk_preview => {
                out.chunk_preview = plan.derived_output_actions.chunk_preview == .recompute;
                out.full_text = hasChunkBackedFullTextRebuild(plan.full_text_index_actions);
            },
            .chunk_embeddings => {
                out.chunk_embeddings = plan.derived_output_actions.chunk_embeddings == .recompute;
                out.dense_vector = hasChunkEmbeddingBackedVectorRebuild(
                    plan.targets.published_search_sources,
                    plan.vector_index_actions,
                );
            },
            .rerank_terms => {
                out.rerank_terms = plan.derived_output_actions.rerank_terms == .recompute;
            },
        }
        return out;
    }

    fn derivedOutputResolutions(
        policy: catalog_types.NamespacePolicy,
        plan: publication_plan.TablePublicationPlan,
        pending: catalog_types.PendingMaterializationFamilies,
    ) catalog_types.DerivedOutputResolutions {
        return .{
            .chunk_preview = derivedOutputResolution(
                policy.chunk_preview_enabled,
                plan.metadata_republish.chunk_preview_policy_changed,
                plan.derived_output_actions.chunk_preview,
                pending.chunk_preview,
            ),
            .chunk_embeddings = derivedOutputResolution(
                policy.chunk_embeddings_enabled,
                plan.metadata_republish.chunk_embeddings_policy_changed,
                plan.derived_output_actions.chunk_embeddings,
                pending.chunk_embeddings,
            ),
            .rerank_terms = derivedOutputResolution(
                policy.rerank_terms_enabled,
                plan.metadata_republish.rerank_terms_policy_changed,
                plan.derived_output_actions.rerank_terms,
                pending.rerank_terms,
            ),
        };
    }

    fn derivedOutputResolution(
        enabled: bool,
        metadata_republish_changed: bool,
        action: publication_plan.DerivedOutputAction,
        pending_materialization: bool,
    ) catalog_types.DerivedOutputResolution {
        if (!enabled and action == .drop) return .drop_on_republish;
        if (!enabled) return .disabled;
        if (pending_materialization or action == .recompute) return .pending_materialization;
        if (metadata_republish_changed and action == .reuse) return .head_republish_reuse;
        return .ready;
    }

    fn hasChunkBackedFullTextRebuild(actions: []const publication_plan.FullTextIndexAction) bool {
        for (actions) |action| {
            if (action.action != .rebuild) continue;
            if (action.source_mode != .document or action.chunked_source_count != 0) return true;
        }
        return false;
    }

    fn hasChunkEmbeddingBackedVectorRebuild(
        sources: search_sources.PublishedSearchSources,
        actions: []const publication_plan.NamedArtifactAction,
    ) bool {
        for (actions) |action| {
            if (action.action != .rebuild) continue;
            const descriptor = findVectorSourceByIndexName(sources, action.name) orelse continue;
            switch (descriptor.document_source) {
                .chunk_embeddings, .chunk_embeddings_or_top_level => return true,
                .top_level_embedding => {},
            }
        }
        return false;
    }

    fn findVectorSourceByIndexName(
        sources: search_sources.PublishedSearchSources,
        index_name: []const u8,
    ) ?search_sources.VectorSourceDescriptor {
        if (sources.items) |items| {
            for (items) |item| switch (item) {
                .vector => |value| if (std.mem.eql(u8, value.index_name, index_name)) return value,
                else => {},
            };
        }
        if (sources.vector) |value| {
            if (std.mem.eql(u8, value.index_name, index_name)) return value;
        }
        return null;
    }

    pub fn buildNamespace(self: *CatalogService, namespace: []const u8) !builder_mod.BuildResult {
        const policy = self.getPolicy(namespace) catch catalog_types.NamespacePolicy{};
        var plan = try self.publicationPlanForNamespaceAlloc(namespace, policy);
        defer plan.deinit(self.alloc);
        return try self.builder.publishNamespaceWithMetricAndPlan(
            namespace,
            policy.vector_distance_metric,
            plan,
        );
    }

    pub fn buildTable(self: *CatalogService, table_name: []const u8) !builder_mod.BuildResult {
        const namespace = try self.resolveTableNamespaceAlloc(table_name);
        defer self.alloc.free(namespace);
        return try self.buildNamespace(namespace);
    }

    pub fn tableBuildStatus(self: *CatalogService, table_name: []const u8) !catalog_types.BuildStatus {
        const namespace = try self.resolveTableNamespaceAlloc(table_name);
        defer self.alloc.free(namespace);
        return try self.buildStatus(namespace);
    }

    pub fn getTablePolicy(self: *CatalogService, table_name: []const u8) !catalog_types.NamespacePolicy {
        const namespace = try self.resolveTableNamespaceAlloc(table_name);
        defer self.alloc.free(namespace);
        return try self.getPolicy(namespace);
    }

    pub fn setTablePolicy(self: *CatalogService, table_name: []const u8, policy: catalog_types.NamespacePolicy) !catalog_types.NamespacePolicy {
        const namespace = try self.resolveTableNamespaceAlloc(table_name);
        defer self.alloc.free(namespace);
        return try self.setPolicy(namespace, policy);
    }

    pub fn resolveTableNamespaceAlloc(self: *CatalogService, table_name: []const u8) ![]u8 {
        return try self.store.resolveNamespaceAlloc(self.alloc, table_name);
    }

    fn defaultServingNamespaceAlloc(self: *CatalogService, table_name: []const u8) ![]u8 {
        // Keep today's behavior stable while making the table->serving mapping explicit.
        // This centralizes the choice so we can switch to hidden serving namespace ids later.
        return try self.alloc.dupe(u8, table_name);
    }

    fn publicationPlanForNamespaceAlloc(
        self: *CatalogService,
        namespace: []const u8,
        policy: catalog_types.NamespacePolicy,
    ) !publication_plan.TablePublicationPlan {
        const tables = try self.listTablesAlloc(self.alloc);
        defer self.freeTables(self.alloc, tables);
        for (tables) |table| {
            if (!std.mem.eql(u8, table.namespace, namespace)) continue;
            const effective_policy = effectivePolicyForTable(policy, table.indexes_json) catch return error.InvalidTableIndexMetadata;
            const targets: builder_mod.Builder.PublicationTargets = if (table.indexes_json.len == 0 or std.mem.eql(u8, table.indexes_json, "{}"))
                .{
                    .published_search_sources = try search_sources.clonePublishedSearchSourcesAlloc(self.alloc, search_sources.defaultPublishedSearchSources()),
                    .include_graph = true,
                }
            else
                .{
                    .published_search_sources = try search_sources.publishedSearchSourcesForTableDefinitionAlloc(
                        self.alloc,
                        table.schema_json,
                        table.read_schema_json,
                        table.indexes_json,
                    ),
                    .include_graph = true,
                };

            var metadata_republish: publication_plan.MetadataRepublishReasons = .{};
            var published_head = try self.loadPublishedHeadAlloc(namespace);
            defer published_head.deinit(self.alloc);
            if (published_head.manifest) |manifest| {
                const head_version = published_head.manifest_version;

                const impact = try impact_planner.planAlloc(self.alloc, .{
                    .before_schema_json = manifest.stats.schema_json,
                    .after_schema_json = table.schema_json,
                    .before_read_schema_json = manifest.stats.read_schema_json,
                    .after_read_schema_json = table.read_schema_json,
                    .before_indexes_json = manifest.stats.indexes_json,
                    .after_indexes_json = table.indexes_json,
                    .before_policy = manifest.stats.policy,
                    .after_policy = effective_policy,
                });
                const completion = try enrichmentCompletionAlloc(self.alloc, self.artifacts, self.manifests, namespace, head_version, effective_policy);
                const can_republish_chunk_preview = impact.rebuild_chunk_preview and
                    (!effective_policy.chunk_preview_enabled or completion.chunk_preview_complete);
                const can_republish_chunk_embeddings = impact.rebuild_chunk_embeddings and
                    (!effective_policy.chunk_embeddings_enabled or completion.chunk_embeddings_complete);
                const can_republish_rerank_terms = impact.rebuild_rerank_terms and
                    (!effective_policy.rerank_terms_enabled or completion.rerank_terms_complete);

                metadata_republish.read_schema_migration = impact.migration_state_changed;
                metadata_republish.published_search_sources_changed = !publishedSearchSourcesMatch(
                    targets.published_search_sources,
                    manifest.stats.published_search_sources,
                );
                metadata_republish.artifact_families_changed = impact.requiresHeadRepublish() and
                    !metadata_republish.read_schema_migration and
                    !metadata_republish.published_search_sources_changed and
                    !impact.rebuild_chunk_preview and
                    !impact.rebuild_rerank_terms;
                metadata_republish.chunk_preview_policy_changed = can_republish_chunk_preview;
                metadata_republish.chunk_embeddings_policy_changed = can_republish_chunk_embeddings;
                metadata_republish.rerank_terms_policy_changed = can_republish_rerank_terms;

                const full_text_index_actions = try planFullTextIndexActionsAlloc(
                    self.alloc,
                    manifest.stats.schema_json,
                    table.schema_json,
                    manifest.stats.indexes_json,
                    table.indexes_json,
                );
                errdefer freeFullTextIndexActions(self.alloc, full_text_index_actions);
                const vector_index_actions = try planNamedIndexActionsAlloc(
                    self.alloc,
                    manifest.stats.indexes_json,
                    table.indexes_json,
                    .vector,
                    countManifestArtifactsOfKind(manifest, .vector_segment),
                );
                errdefer freeNamedArtifactActions(self.alloc, vector_index_actions);
                const sparse_index_actions = try planNamedIndexActionsAlloc(
                    self.alloc,
                    manifest.stats.indexes_json,
                    table.indexes_json,
                    .sparse,
                    countManifestArtifactsOfKind(manifest, .sparse_segment),
                );
                errdefer freeNamedArtifactActions(self.alloc, sparse_index_actions);
                const graph_index_actions = try planNamedIndexActionsAlloc(
                    self.alloc,
                    manifest.stats.indexes_json,
                    table.indexes_json,
                    .graph,
                    countManifestArtifactsOfKind(manifest, .graph_segment),
                );
                errdefer freeNamedArtifactActions(self.alloc, graph_index_actions);

                const artifact_actions: publication_plan.ArtifactActions = .{
                    .document_segment = if (findManifestArtifactIndex(manifest, .document_segment) != null) .reuse else .rebuild,
                    .full_text = publication_plan.collapseFullTextArtifactAction(full_text_index_actions, findManifestArtifactIndex(manifest, .text_segment) != null, .rebuild),
                    .dense_vector = if (targets.published_search_sources.findVector() == null)
                        .drop
                    else
                        publication_plan.collapseNamedArtifactAction(
                            vector_index_actions,
                            findManifestArtifactIndex(manifest, .vector_segment) != null,
                            .rebuild,
                        ),
                    .sparse_vector = if (targets.published_search_sources.findSparse() == null)
                        .drop
                    else
                        publication_plan.collapseNamedArtifactAction(
                            sparse_index_actions,
                            findManifestArtifactIndex(manifest, .sparse_segment) != null,
                            .rebuild,
                        ),
                    .graph = if (!targets.include_graph)
                        .drop
                    else
                        publication_plan.collapseNamedArtifactAction(
                            graph_index_actions,
                            findManifestArtifactIndex(manifest, .graph_segment) != null,
                            if (impact.rebuild_graph) .rebuild else .reuse,
                        ),
                };
                const derived_output_actions: publication_plan.DerivedOutputActions = .{
                    .chunk_preview = if (!effective_policy.chunk_preview_enabled)
                        .drop
                    else if (impact.rebuild_chunk_preview and !completion.chunk_preview_complete)
                        .recompute
                    else if (manifest.stats.derived_outputs.containsKind(.chunk_preview))
                        .reuse
                    else
                        .recompute,
                    .chunk_embeddings = if (!effective_policy.chunk_embeddings_enabled)
                        .drop
                    else if (impact.rebuild_chunk_embeddings and !completion.chunk_embeddings_complete)
                        .recompute
                    else if (manifest.stats.derived_outputs.containsKind(.chunk_embeddings))
                        .reuse
                    else
                        .recompute,
                    .rerank_terms = if (!effective_policy.rerank_terms_enabled)
                        .drop
                    else if (impact.rebuild_rerank_terms and !completion.rerank_terms_complete)
                        .recompute
                    else if (manifest.stats.derived_outputs.containsKind(.rerank_terms))
                        .reuse
                    else
                        .recompute,
                };

                return .{
                    .targets = targets,
                    .policy = effective_policy,
                    .table_definition = .{
                        .schema_json = try self.alloc.dupe(u8, table.schema_json),
                        .read_schema_json = try self.alloc.dupe(u8, table.read_schema_json),
                        .indexes_json = try self.alloc.dupe(u8, table.indexes_json),
                    },
                    .metadata_republish = metadata_republish,
                    .artifact_actions = artifact_actions,
                    .full_text_index_actions = full_text_index_actions,
                    .vector_index_actions = vector_index_actions,
                    .sparse_index_actions = sparse_index_actions,
                    .graph_index_actions = graph_index_actions,
                    .derived_output_actions = derived_output_actions,
                };
            }

            const full_text_index_actions = try planFullTextIndexActionsAlloc(
                self.alloc,
                "",
                table.schema_json,
                "",
                table.indexes_json,
            );
            errdefer freeFullTextIndexActions(self.alloc, full_text_index_actions);
            const vector_index_actions = try planNamedIndexActionsAlloc(
                self.alloc,
                "",
                table.indexes_json,
                .vector,
                0,
            );
            errdefer freeNamedArtifactActions(self.alloc, vector_index_actions);
            const sparse_index_actions = try planNamedIndexActionsAlloc(
                self.alloc,
                "",
                table.indexes_json,
                .sparse,
                0,
            );
            errdefer freeNamedArtifactActions(self.alloc, sparse_index_actions);
            const graph_index_actions = try planNamedIndexActionsAlloc(
                self.alloc,
                "",
                table.indexes_json,
                .graph,
                0,
            );
            errdefer freeNamedArtifactActions(self.alloc, graph_index_actions);

            return .{
                .targets = targets,
                .policy = effective_policy,
                .table_definition = .{
                    .schema_json = try self.alloc.dupe(u8, table.schema_json),
                    .read_schema_json = try self.alloc.dupe(u8, table.read_schema_json),
                    .indexes_json = try self.alloc.dupe(u8, table.indexes_json),
                },
                .metadata_republish = metadata_republish,
                .artifact_actions = .{
                    .document_segment = .rebuild,
                    .full_text = publication_plan.collapseFullTextArtifactAction(full_text_index_actions, false, .rebuild),
                    .dense_vector = if (targets.published_search_sources.findVector() == null)
                        .drop
                    else
                        publication_plan.collapseNamedArtifactAction(vector_index_actions, false, .rebuild),
                    .sparse_vector = if (targets.published_search_sources.findSparse() == null)
                        .drop
                    else
                        publication_plan.collapseNamedArtifactAction(sparse_index_actions, false, .rebuild),
                    .graph = if (!targets.include_graph)
                        .drop
                    else
                        publication_plan.collapseNamedArtifactAction(graph_index_actions, false, .rebuild),
                },
                .full_text_index_actions = full_text_index_actions,
                .vector_index_actions = vector_index_actions,
                .sparse_index_actions = sparse_index_actions,
                .graph_index_actions = graph_index_actions,
            };
        }
        return .{
            .targets = .{
                .published_search_sources = try search_sources.clonePublishedSearchSourcesAlloc(self.alloc, search_sources.defaultPublishedSearchSources()),
                .include_graph = true,
            },
            .policy = policy,
        };
    }

    fn effectivePolicyForTable(
        base_policy: catalog_types.NamespacePolicy,
        indexes_json: []const u8,
    ) !catalog_types.NamespacePolicy {
        var effective = base_policy;
        if (!effective.chunk_embeddings_enabled and try tableRequiresChunkEmbeddings(indexes_json)) {
            effective.chunk_embeddings_enabled = true;
        }
        return effective;
    }

    fn tableRequiresChunkEmbeddings(indexes_json: []const u8) !bool {
        if (indexes_json.len == 0 or std.mem.eql(u8, indexes_json, "{}")) return false;

        var parsed = try std.json.parseFromSlice(std.json.Value, std.heap.page_allocator, indexes_json, .{});
        defer parsed.deinit();
        const object = switch (parsed.value) {
            .object => |object| object,
            else => return error.InvalidTableIndexMetadata,
        };

        var it = object.iterator();
        while (it.next()) |entry| {
            if (entry.value_ptr.* != .object) continue;
            const root = entry.value_ptr.object;
            const index_type = blk: {
                const value = root.get("type") orelse break :blk "full_text";
                break :blk switch (value) {
                    .string => |kind| kind,
                    else => continue,
                };
            };
            if (!std.mem.eql(u8, index_type, "embeddings")) continue;
            const sparse = if (root.get("sparse")) |value| switch (value) {
                .bool => |enabled| enabled,
                else => false,
            } else false;
            if (sparse) continue;
            if (root.get("chunker") != null) return true;
        }
        return false;
    }

    pub fn getPolicy(self: *CatalogService, namespace: []const u8) !catalog_types.NamespacePolicy {
        return try self.store.getPolicy(namespace);
    }

    pub fn setPolicy(self: *CatalogService, namespace: []const u8, policy: catalog_types.NamespacePolicy) !catalog_types.NamespacePolicy {
        return try self.store.setPolicy(namespace, policy);
    }
};

fn cloneFullTextIndexActionsAlloc(
    alloc: Allocator,
    items: []const publication_plan.FullTextIndexAction,
) ![]catalog_types.FullTextIndexPublicationAction {
    if (items.len == 0) return &.{};
    const out = try alloc.alloc(catalog_types.FullTextIndexPublicationAction, items.len);
    errdefer alloc.free(out);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*entry| entry.deinit(alloc);
    }
    for (items, 0..) |item, idx| {
        out[idx] = .{
            .name = try alloc.dupe(u8, item.name),
            .action = @enumFromInt(@intFromEnum(item.action)),
            .source_mode = item.source_mode,
            .chunked_source_count = item.chunked_source_count,
        };
        initialized += 1;
    }
    return out;
}

fn cloneCatalogFullTextIndexActionsAlloc(
    alloc: Allocator,
    items: []const catalog_types.FullTextIndexPublicationAction,
) ![]catalog_types.FullTextIndexPublicationAction {
    if (items.len == 0) return &.{};
    const out = try alloc.alloc(catalog_types.FullTextIndexPublicationAction, items.len);
    errdefer alloc.free(out);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*entry| entry.deinit(alloc);
    }
    for (items, 0..) |item, idx| {
        out[idx] = .{
            .name = try alloc.dupe(u8, item.name),
            .action = item.action,
            .source_mode = item.source_mode,
            .chunked_source_count = item.chunked_source_count,
        };
        initialized += 1;
    }
    return out;
}

fn cloneNamedArtifactActionsAlloc(
    alloc: Allocator,
    items: []const publication_plan.NamedArtifactAction,
) ![]catalog_types.NamedArtifactPublicationAction {
    if (items.len == 0) return &.{};
    const out = try alloc.alloc(catalog_types.NamedArtifactPublicationAction, items.len);
    errdefer alloc.free(out);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*entry| entry.deinit(alloc);
    }
    for (items, 0..) |item, idx| {
        out[idx] = .{
            .name = try alloc.dupe(u8, item.name),
            .action = @enumFromInt(@intFromEnum(item.action)),
        };
        initialized += 1;
    }
    return out;
}

fn cloneCatalogNamedArtifactActionsAlloc(
    alloc: Allocator,
    items: []const catalog_types.NamedArtifactPublicationAction,
) ![]catalog_types.NamedArtifactPublicationAction {
    if (items.len == 0) return &.{};
    const out = try alloc.alloc(catalog_types.NamedArtifactPublicationAction, items.len);
    errdefer alloc.free(out);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*entry| entry.deinit(alloc);
    }
    for (items, 0..) |item, idx| {
        out[idx] = .{
            .name = try alloc.dupe(u8, item.name),
            .action = item.action,
        };
        initialized += 1;
    }
    return out;
}

fn freeFullTextIndexActions(alloc: Allocator, items: []publication_plan.FullTextIndexAction) void {
    for (items) |*item| item.deinit(alloc);
    if (items.len > 0) alloc.free(items);
}

fn freeNamedArtifactActions(alloc: Allocator, items: []publication_plan.NamedArtifactAction) void {
    for (items) |*item| item.deinit(alloc);
    if (items.len > 0) alloc.free(items);
}

fn headPublicationActionsAlloc(
    alloc: Allocator,
    manifests: *manifest_mod.ManifestStore,
    namespace: []const u8,
    head_version: u64,
) !HeadPublicationActions {
    if (head_version == 0) return .{};

    var current = try manifests.getAlloc(namespace, head_version);
    defer current.deinit(alloc);
    var previous: ?manifest_mod.Manifest = null;
    defer if (previous) |*manifest| manifest.deinit(alloc);
    if (head_version > 1) {
        previous = try manifests.getAlloc(namespace, head_version - 1);
    }

    const full_text_index_actions = try deriveHeadFullTextIndexActionsAlloc(alloc, current, previous);
    errdefer {
        for (full_text_index_actions) |*entry| entry.deinit(alloc);
        if (full_text_index_actions.len > 0) alloc.free(full_text_index_actions);
    }
    const vector_index_actions = try deriveHeadNamedArtifactActionsAlloc(alloc, current, previous, .vector_segment);
    errdefer {
        for (vector_index_actions) |*entry| entry.deinit(alloc);
        if (vector_index_actions.len > 0) alloc.free(vector_index_actions);
    }
    const sparse_index_actions = try deriveHeadNamedArtifactActionsAlloc(alloc, current, previous, .sparse_segment);
    errdefer {
        for (sparse_index_actions) |*entry| entry.deinit(alloc);
        if (sparse_index_actions.len > 0) alloc.free(sparse_index_actions);
    }
    const graph_index_actions = try deriveHeadGraphIndexActionsAlloc(alloc, current, previous);
    errdefer {
        for (graph_index_actions) |*entry| entry.deinit(alloc);
        if (graph_index_actions.len > 0) alloc.free(graph_index_actions);
    }

    return .{
        .artifact_actions = .{
            .document_segment = deriveSingleArtifactAction(current, previous, .document_segment),
            .full_text = collapseHeadNamedActions(catalog_types.FullTextIndexPublicationAction, full_text_index_actions),
            .dense_vector = collapseHeadNamedActions(catalog_types.NamedArtifactPublicationAction, vector_index_actions),
            .sparse_vector = collapseHeadNamedActions(catalog_types.NamedArtifactPublicationAction, sparse_index_actions),
            .graph = collapseHeadNamedActions(catalog_types.NamedArtifactPublicationAction, graph_index_actions),
        },
        .full_text_index_actions = full_text_index_actions,
        .vector_index_actions = vector_index_actions,
        .sparse_index_actions = sparse_index_actions,
        .graph_index_actions = graph_index_actions,
        .derived_output_actions = deriveHeadDerivedOutputActions(current, previous),
    };
}

fn deriveSingleArtifactAction(
    current: manifest_mod.Manifest,
    previous: ?manifest_mod.Manifest,
    kind: manifest_mod.ArtifactKind,
) catalog_types.ArtifactPublicationAction {
    const current_ref = findManifestArtifactByKind(current, kind);
    const previous_ref = if (previous) |manifest| findManifestArtifactByKind(manifest, kind) else null;
    if (current_ref == null) return .drop;
    if (previous_ref) |prev| {
        if (std.mem.eql(u8, current_ref.?.artifact_id, prev.artifact_id)) return .reuse;
    }
    return .rebuild;
}

fn deriveHeadFullTextIndexActionsAlloc(
    alloc: Allocator,
    current: manifest_mod.Manifest,
    previous: ?manifest_mod.Manifest,
) ![]catalog_types.FullTextIndexPublicationAction {
    const current_specs = try full_text_indexes.listFullTextIndexSpecsAlloc(alloc, current.stats.indexes_json);
    defer full_text_indexes.freeFullTextIndexSpecs(alloc, current_specs);
    const previous_specs = if (previous) |manifest|
        try full_text_indexes.listFullTextIndexSpecsAlloc(alloc, manifest.stats.indexes_json)
    else
        try alloc.alloc(full_text_indexes.FullTextIndexSpec, 0);
    defer full_text_indexes.freeFullTextIndexSpecs(alloc, previous_specs);

    var actions = std.ArrayListUnmanaged(catalog_types.FullTextIndexPublicationAction).empty;
    errdefer {
        for (actions.items) |*entry| entry.deinit(alloc);
        actions.deinit(alloc);
    }

    for (current_specs) |spec| {
        try actions.append(alloc, .{
            .name = try alloc.dupe(u8, spec.name),
            .action = if (headNamedArtifactReused(current, previous, .text_segment, spec.name)) .reuse else .rebuild,
            .source_mode = spec.source_mode,
            .chunked_source_count = spec.chunked_sources.len,
        });
    }

    for (previous_specs) |spec| {
        if (containsFullTextSpecName(current_specs, spec.name)) continue;
        try actions.append(alloc, .{
            .name = try alloc.dupe(u8, spec.name),
            .action = .drop,
            .source_mode = spec.source_mode,
            .chunked_source_count = spec.chunked_sources.len,
        });
    }

    std.mem.sort(catalog_types.FullTextIndexPublicationAction, actions.items, {}, lessCatalogFullTextIndexPublicationAction);
    return try actions.toOwnedSlice(alloc);
}

fn deriveHeadNamedArtifactActionsAlloc(
    alloc: Allocator,
    current: manifest_mod.Manifest,
    previous: ?manifest_mod.Manifest,
    kind: manifest_mod.ArtifactKind,
) ![]catalog_types.NamedArtifactPublicationAction {
    var actions = std.ArrayListUnmanaged(catalog_types.NamedArtifactPublicationAction).empty;
    errdefer {
        for (actions.items) |*entry| entry.deinit(alloc);
        actions.deinit(alloc);
    }

    for (current.artifacts) |artifact| {
        if (artifact.kind != kind) continue;
        if (artifact.name.len == 0) continue;
        try actions.append(alloc, .{
            .name = try alloc.dupe(u8, artifact.name),
            .action = if (headNamedArtifactReused(current, previous, kind, artifact.name)) .reuse else .rebuild,
        });
    }

    if (previous) |manifest| {
        for (manifest.artifacts) |artifact| {
            if (artifact.kind != kind) continue;
            if (artifact.name.len == 0) continue;
            if (findManifestNamedArtifact(current, kind, artifact.name) != null) continue;
            try actions.append(alloc, .{
                .name = try alloc.dupe(u8, artifact.name),
                .action = .drop,
            });
        }
    }

    std.mem.sort(catalog_types.NamedArtifactPublicationAction, actions.items, {}, lessCatalogNamedArtifactPublicationAction);
    return try actions.toOwnedSlice(alloc);
}

fn deriveHeadGraphIndexActionsAlloc(
    alloc: Allocator,
    current: manifest_mod.Manifest,
    previous: ?manifest_mod.Manifest,
) ![]catalog_types.NamedArtifactPublicationAction {
    const current_names = try listNamedIndexNamesAlloc(alloc, current.stats.indexes_json, .graph);
    defer freeOwnedStrings(alloc, current_names);
    const previous_names = if (previous) |manifest|
        try listNamedIndexNamesAlloc(alloc, manifest.stats.indexes_json, .graph)
    else
        try alloc.alloc([]u8, 0);
    defer freeOwnedStrings(alloc, previous_names);

    var actions = std.ArrayListUnmanaged(catalog_types.NamedArtifactPublicationAction).empty;
    errdefer {
        for (actions.items) |*entry| entry.deinit(alloc);
        actions.deinit(alloc);
    }

    for (current_names) |name| {
        try actions.append(alloc, .{
            .name = try alloc.dupe(u8, name),
            .action = if (headNamedOrSingleArtifactReused(current, previous, .graph_segment, name)) .reuse else .rebuild,
        });
    }

    for (previous_names) |name| {
        if (containsString(current_names, name)) continue;
        try actions.append(alloc, .{
            .name = try alloc.dupe(u8, name),
            .action = .drop,
        });
    }

    std.mem.sort(catalog_types.NamedArtifactPublicationAction, actions.items, {}, lessCatalogNamedArtifactPublicationAction);
    return try actions.toOwnedSlice(alloc);
}

fn deriveHeadDerivedOutputActions(
    current: manifest_mod.Manifest,
    previous: ?manifest_mod.Manifest,
) catalog_types.DerivedOutputPublicationActions {
    return .{
        .chunk_preview = deriveHeadDerivedOutputAction(current, previous, .chunk_preview),
        .chunk_embeddings = deriveHeadDerivedOutputAction(current, previous, .chunk_embeddings),
        .rerank_terms = deriveHeadDerivedOutputAction(current, previous, .rerank_terms),
    };
}

fn deriveHeadDerivedOutputAction(
    current: manifest_mod.Manifest,
    previous: ?manifest_mod.Manifest,
    kind: search_sources.DerivedOutputKind,
) catalog_types.DerivedOutputPublicationAction {
    const current_descriptor = current.stats.derived_outputs.findByKind(kind);
    const previous_descriptor = if (previous) |manifest| manifest.stats.derived_outputs.findByKind(kind) else null;
    if (current_descriptor == null) return .drop;
    if (previous_descriptor) |descriptor| {
        if (std.mem.eql(u8, current_descriptor.?.name, descriptor.name)) return .reuse;
    }
    return .recompute;
}

fn collapseHeadNamedActions(comptime T: type, items: []const T) catalog_types.ArtifactPublicationAction {
    var has_rebuild = false;
    var has_reuse = false;
    for (items) |item| switch (item.action) {
        .rebuild => has_rebuild = true,
        .reuse => has_reuse = true,
        .drop => {},
    };
    if (has_rebuild) return .rebuild;
    if (has_reuse) return .reuse;
    return .drop;
}

fn headNamedArtifactReused(
    current: manifest_mod.Manifest,
    previous: ?manifest_mod.Manifest,
    kind: manifest_mod.ArtifactKind,
    name: []const u8,
) bool {
    const current_ref = findManifestNamedArtifact(current, kind, name) orelse return false;
    const previous_ref = if (previous) |manifest| findManifestNamedArtifact(manifest, kind, name) else null;
    if (previous_ref) |artifact| {
        return std.mem.eql(u8, current_ref.artifact_id, artifact.artifact_id);
    }
    return false;
}

fn headNamedOrSingleArtifactReused(
    current: manifest_mod.Manifest,
    previous: ?manifest_mod.Manifest,
    kind: manifest_mod.ArtifactKind,
    name: []const u8,
) bool {
    const current_ref = findManifestNamedOrSingleArtifact(current, kind, name) orelse return false;
    const previous_ref = if (previous) |manifest| findManifestNamedOrSingleArtifact(manifest, kind, name) else null;
    if (previous_ref) |artifact| {
        return std.mem.eql(u8, current_ref.artifact_id, artifact.artifact_id);
    }
    return false;
}

fn findManifestArtifactByKind(
    manifest: manifest_mod.Manifest,
    kind: manifest_mod.ArtifactKind,
) ?manifest_mod.ArtifactRef {
    for (manifest.artifacts) |artifact| {
        if (artifact.kind == kind) return artifact;
    }
    return null;
}

fn findManifestNamedArtifact(
    manifest: manifest_mod.Manifest,
    kind: manifest_mod.ArtifactKind,
    name: []const u8,
) ?manifest_mod.ArtifactRef {
    for (manifest.artifacts) |artifact| {
        if (artifact.kind != kind) continue;
        if (std.mem.eql(u8, artifact.name, name)) return artifact;
    }
    return null;
}

fn findManifestNamedOrSingleArtifact(
    manifest: manifest_mod.Manifest,
    kind: manifest_mod.ArtifactKind,
    name: []const u8,
) ?manifest_mod.ArtifactRef {
    if (findManifestNamedArtifact(manifest, kind, name)) |artifact| return artifact;
    if (countManifestArtifactsOfKind(manifest, kind) == 1) return findManifestArtifactByKind(manifest, kind);
    return null;
}

fn containsFullTextSpecName(items: []const full_text_indexes.FullTextIndexSpec, name: []const u8) bool {
    for (items) |item| {
        if (std.mem.eql(u8, item.name, name)) return true;
    }
    return false;
}

fn lessCatalogFullTextIndexPublicationAction(
    _: void,
    lhs: catalog_types.FullTextIndexPublicationAction,
    rhs: catalog_types.FullTextIndexPublicationAction,
) bool {
    return std.mem.order(u8, lhs.name, rhs.name) == .lt;
}

fn lessCatalogNamedArtifactPublicationAction(
    _: void,
    lhs: catalog_types.NamedArtifactPublicationAction,
    rhs: catalog_types.NamedArtifactPublicationAction,
) bool {
    return std.mem.order(u8, lhs.name, rhs.name) == .lt;
}

const NamedSearchSourceKind = enum {
    vector,
    sparse,
    graph,
};

const HeadPublicationActions = struct {
    artifact_actions: catalog_types.ArtifactPublicationActions = .{},
    full_text_index_actions: []catalog_types.FullTextIndexPublicationAction = &.{},
    vector_index_actions: []catalog_types.NamedArtifactPublicationAction = &.{},
    sparse_index_actions: []catalog_types.NamedArtifactPublicationAction = &.{},
    graph_index_actions: []catalog_types.NamedArtifactPublicationAction = &.{},
    derived_output_actions: catalog_types.DerivedOutputPublicationActions = .{},

    fn deinit(self: *HeadPublicationActions, alloc: Allocator) void {
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

fn planNamedIndexActionsAlloc(
    alloc: Allocator,
    before_indexes_json: []const u8,
    after_indexes_json: []const u8,
    kind: NamedSearchSourceKind,
    current_artifact_count: usize,
) ![]publication_plan.NamedArtifactAction {
    var before = try std.json.parseFromSlice(std.json.Value, alloc, if (before_indexes_json.len == 0) "{}" else before_indexes_json, .{});
    defer before.deinit();
    var after = try std.json.parseFromSlice(std.json.Value, alloc, if (after_indexes_json.len == 0) "{}" else after_indexes_json, .{});
    defer after.deinit();

    const before_object = switch (before.value) {
        .object => |value| value,
        else => return error.InvalidTableIndexMetadata,
    };
    const after_object = switch (after.value) {
        .object => |value| value,
        else => return error.InvalidTableIndexMetadata,
    };

    var actions = std.ArrayListUnmanaged(publication_plan.NamedArtifactAction).empty;
    errdefer {
        for (actions.items) |*item| item.deinit(alloc);
        actions.deinit(alloc);
    }

    var after_it = after_object.iterator();
    while (after_it.next()) |entry| {
        if (!isNamedIndexKindValue(entry.value_ptr.*, kind)) continue;
        const action: publication_plan.ArtifactAction = blk: {
            if (before_object.get(entry.key_ptr.*)) |before_value| {
                if (isNamedIndexKindValue(before_value, kind) and jsonValueEql(entry.value_ptr.*, before_value)) {
                    break :blk .reuse;
                }
            }
            if (current_artifact_count == 1) {
                const rename_source = findEquivalentRenamedIndexName(before_object, after_object, kind, entry.key_ptr.*, entry.value_ptr.*);
                if (rename_source != null) break :blk .reuse;
            }
            break :blk .rebuild;
        };
        try actions.append(alloc, .{
            .name = try alloc.dupe(u8, entry.key_ptr.*),
            .action = action,
        });
    }

    var before_it = before_object.iterator();
    while (before_it.next()) |entry| {
        if (!isNamedIndexKindValue(entry.value_ptr.*, kind)) continue;
        if (after_object.get(entry.key_ptr.*) != null) continue;
        try actions.append(alloc, .{
            .name = try alloc.dupe(u8, entry.key_ptr.*),
            .action = .drop,
        });
    }

    std.mem.sort(publication_plan.NamedArtifactAction, actions.items, {}, lessNamedArtifactAction);
    return try actions.toOwnedSlice(alloc);
}

fn listNamedIndexNamesAlloc(
    alloc: Allocator,
    indexes_json: []const u8,
    kind: NamedSearchSourceKind,
) ![][]u8 {
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
        if (!isNamedIndexKindValue(entry.value_ptr.*, kind)) continue;
        try names.append(alloc, try alloc.dupe(u8, entry.key_ptr.*));
    }
    return try names.toOwnedSlice(alloc);
}

fn freeOwnedStrings(alloc: Allocator, items: []const []u8) void {
    for (items) |item| alloc.free(item);
    alloc.free(items);
}

fn containsString(items: []const []const u8, needle: []const u8) bool {
    for (items) |item| {
        if (std.mem.eql(u8, item, needle)) return true;
    }
    return false;
}

fn isNamedIndexKindValue(value: std.json.Value, kind: NamedSearchSourceKind) bool {
    const object = switch (value) {
        .object => |map| map,
        else => return false,
    };
    const type_value = object.get("type") orelse return false;
    return switch (kind) {
        .graph => type_value == .string and std.mem.eql(u8, type_value.string, "graph"),
        .vector, .sparse => blk: {
            if (type_value != .string or !std.mem.eql(u8, type_value.string, "embeddings")) break :blk false;
            const sparse = if (object.get("sparse")) |sparse_value|
                switch (sparse_value) {
                    .bool => sparse_value.bool,
                    else => return false,
                }
            else
                false;
            break :blk switch (kind) {
                .vector => !sparse,
                .sparse => sparse,
                .graph => unreachable,
            };
        },
    };
}

fn findEquivalentRenamedIndexName(
    before_object: std.json.ObjectMap,
    after_object: std.json.ObjectMap,
    kind: NamedSearchSourceKind,
    target_name: []const u8,
    target_value: std.json.Value,
) ?[]const u8 {
    var match: ?[]const u8 = null;
    var before_it = before_object.iterator();
    while (before_it.next()) |entry| {
        if (!isNamedIndexKindValue(entry.value_ptr.*, kind)) continue;
        if (after_object.get(entry.key_ptr.*) != null) continue;
        if (!jsonValueEql(entry.value_ptr.*, target_value)) continue;
        if (match != null) return null;
        match = entry.key_ptr.*;
    }
    _ = target_name;
    return match;
}

fn planFullTextIndexActionsAlloc(
    alloc: Allocator,
    before_schema_json: []const u8,
    after_schema_json: []const u8,
    before_indexes_json: []const u8,
    after_indexes_json: []const u8,
) ![]publication_plan.FullTextIndexAction {
    var before = try std.json.parseFromSlice(std.json.Value, alloc, if (before_indexes_json.len == 0) "{}" else before_indexes_json, .{});
    defer before.deinit();
    var after = try std.json.parseFromSlice(std.json.Value, alloc, if (after_indexes_json.len == 0) "{}" else after_indexes_json, .{});
    defer after.deinit();

    const before_object = switch (before.value) {
        .object => |value| value,
        else => return error.InvalidTableIndexMetadata,
    };
    const after_object = switch (after.value) {
        .object => |value| value,
        else => return error.InvalidTableIndexMetadata,
    };

    const schema_changed = !std.mem.eql(u8, before_schema_json, after_schema_json);
    const target_schema_version = parseSchemaVersionAlloc(alloc, after_schema_json) catch null;
    const chunked_sources = try full_text_indexes.listChunkedFullTextSourcesAlloc(alloc, if (after_indexes_json.len == 0) "{}" else after_indexes_json);
    defer full_text_indexes.freeChunkedFullTextSources(alloc, chunked_sources);

    var actions = std.ArrayListUnmanaged(publication_plan.FullTextIndexAction).empty;
    errdefer {
        for (actions.items) |*item| item.deinit(alloc);
        actions.deinit(alloc);
    }

    var after_it = after_object.iterator();
    while (after_it.next()) |entry| {
        if (!isFullTextIndexValue(entry.value_ptr.*)) continue;
        const action: publication_plan.ArtifactAction = blk: {
            const before_value = before_object.get(entry.key_ptr.*) orelse break :blk .rebuild;
            if (!isFullTextIndexValue(before_value)) break :blk .rebuild;
            if (!jsonValueEql(entry.value_ptr.*, before_value)) break :blk .rebuild;
            if (schema_changed and shouldRebuildFullTextIndexForSchemaChange(entry.key_ptr.*, target_schema_version)) {
                break :blk .rebuild;
            }
            break :blk .reuse;
        };
        const source_mode: full_text_indexes.FullTextSourceMode = if (hasFullTextSourceArtifact(entry.value_ptr.*))
            .artifact_only
        else if (chunked_sources.len > 0)
            .document_plus_artifact
        else
            .document;
        try actions.append(alloc, .{
            .name = try alloc.dupe(u8, entry.key_ptr.*),
            .action = action,
            .source_mode = source_mode,
            .chunked_source_count = if (source_mode == .document_plus_artifact) chunked_sources.len else 0,
        });
    }

    var before_it = before_object.iterator();
    while (before_it.next()) |entry| {
        if (!isFullTextIndexValue(entry.value_ptr.*)) continue;
        if (after_object.get(entry.key_ptr.*) != null) continue;
        try actions.append(alloc, .{
            .name = try alloc.dupe(u8, entry.key_ptr.*),
            .action = .drop,
            .source_mode = if (hasFullTextSourceArtifact(entry.value_ptr.*)) .artifact_only else .document,
            .chunked_source_count = 0,
        });
    }

    std.mem.sort(publication_plan.FullTextIndexAction, actions.items, {}, lessFullTextIndexAction);
    return try actions.toOwnedSlice(alloc);
}

fn hasFullTextSourceArtifact(value: std.json.Value) bool {
    if (value != .object) return false;
    if (value.object.get("artifact_name")) |artifact_name| {
        return artifact_name == .string and artifact_name.string.len > 0;
    }
    if (value.object.get("chunk_name")) |chunk_name| {
        return chunk_name == .string and chunk_name.string.len > 0;
    }
    return false;
}

fn lessFullTextIndexAction(_: void, lhs: publication_plan.FullTextIndexAction, rhs: publication_plan.FullTextIndexAction) bool {
    return std.mem.lessThan(u8, lhs.name, rhs.name);
}

fn lessNamedArtifactAction(_: void, lhs: publication_plan.NamedArtifactAction, rhs: publication_plan.NamedArtifactAction) bool {
    return std.mem.lessThan(u8, lhs.name, rhs.name);
}

fn countManifestArtifactsOfKind(manifest: manifest_mod.Manifest, kind: manifest_mod.ArtifactKind) usize {
    var count: usize = 0;
    for (manifest.artifacts) |artifact| {
        if (artifact.kind == kind) count += 1;
    }
    return count;
}

fn isFullTextIndexValue(value: std.json.Value) bool {
    const object = switch (value) {
        .object => |map| map,
        else => return false,
    };
    const type_value = object.get("type") orelse return false;
    return type_value == .string and std.mem.eql(u8, type_value.string, "full_text");
}

fn parseSchemaVersionAlloc(alloc: Allocator, schema_json: []const u8) !u32 {
    if (schema_json.len == 0) return 0;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, schema_json, .{});
    defer parsed.deinit();
    const object = switch (parsed.value) {
        .object => |value| value,
        else => return error.InvalidSchemaUpdateRequest,
    };
    const version_value = object.get("version") orelse return 0;
    return switch (version_value) {
        .integer => |value| std.math.cast(u32, value) orelse return error.InvalidSchemaUpdateRequest,
        else => return error.InvalidSchemaUpdateRequest,
    };
}

fn shouldRebuildFullTextIndexForSchemaChange(index_name: []const u8, target_schema_version: ?u32) bool {
    const target = target_schema_version orelse return true;
    const version = parseFullTextIndexVersion(index_name) orelse return true;
    return version == target;
}

fn parseFullTextIndexVersion(index_name: []const u8) ?u32 {
    const prefix = "full_text_index_v";
    if (!std.mem.startsWith(u8, index_name, prefix)) return null;
    return std.fmt.parseInt(u32, index_name[prefix.len..], 10) catch null;
}

fn jsonValueEql(lhs: std.json.Value, rhs: std.json.Value) bool {
    if (@intFromEnum(lhs) != @intFromEnum(rhs)) return false;
    return switch (lhs) {
        .null => true,
        .bool => |value| value == rhs.bool,
        .integer => |value| value == rhs.integer,
        .float => |value| value == rhs.float,
        .number_string => |value| std.mem.eql(u8, value, rhs.number_string),
        .string => |value| std.mem.eql(u8, value, rhs.string),
        .array => |items| blk: {
            if (items.items.len != rhs.array.items.len) break :blk false;
            for (items.items, rhs.array.items) |lhs_item, rhs_item| {
                if (!jsonValueEql(lhs_item, rhs_item)) break :blk false;
            }
            break :blk true;
        },
        .object => |object| blk: {
            if (object.count() != rhs.object.count()) break :blk false;
            var it = object.iterator();
            while (it.next()) |entry| {
                const other = rhs.object.get(entry.key_ptr.*) orelse break :blk false;
                if (!jsonValueEql(entry.value_ptr.*, other)) break :blk false;
            }
            break :blk true;
        },
    };
}

fn publishedSearchSourcesMatch(
    lhs: search_sources.PublishedSearchSources,
    rhs: search_sources.PublishedSearchSources,
) bool {
    const lhs_text = lhs.findText();
    const rhs_text = rhs.findText();
    if ((lhs_text == null) != (rhs_text == null)) return false;
    if (lhs_text) |value| {
        if (!std.mem.eql(u8, value.index_name, rhs_text.?.index_name)) return false;
    }

    const lhs_vector = lhs.findVector();
    const rhs_vector = rhs.findVector();
    if ((lhs_vector == null) != (rhs_vector == null)) return false;
    if (lhs_vector) |value| {
        if (!std.mem.eql(u8, value.index_name, rhs_vector.?.index_name)) return false;
    }

    const lhs_sparse = lhs.findSparse();
    const rhs_sparse = rhs.findSparse();
    if ((lhs_sparse == null) != (rhs_sparse == null)) return false;
    if (lhs_sparse) |value| {
        if (!std.mem.eql(u8, value.index_name, rhs_sparse.?.index_name)) return false;
    }
    return true;
}

fn findManifestArtifactIndex(manifest: manifest_mod.Manifest, kind: manifest_mod.ArtifactKind) ?usize {
    for (manifest.artifacts, 0..) |artifact, idx| {
        if (artifact.kind == kind) return idx;
    }
    return null;
}

const EnrichmentCompletion = struct {
    lexical_sparse_complete: bool = true,
    lexical_sparse_pending_documents: u64 = 0,
    chunk_preview_complete: bool = true,
    chunk_preview_pending_documents: u64 = 0,
    chunk_embeddings_complete: bool = true,
    chunk_embeddings_pending_documents: u64 = 0,
    rerank_terms_complete: bool = true,
    rerank_terms_pending_documents: u64 = 0,
};

fn chooseActiveEnrichmentStage(
    pipeline: enrichment_pipeline.BuiltinPipeline,
    completion: EnrichmentCompletion,
) ?catalog_types.EnrichmentStage {
    for (pipeline.slice()) |spec| {
        if (!isStageComplete(completion, spec.stage)) return spec.stage;
    }
    return null;
}

fn stagePublishMinPendingRecords(
    pipeline: enrichment_pipeline.BuiltinPipeline,
    active_stage: ?catalog_types.EnrichmentStage,
) u64 {
    if (active_stage) |stage| {
        if (pipeline.stageSpec(stage)) |spec| return spec.publish_min_pending_records;
    }
    return 0;
}

fn pendingDocumentsForStage(completion: EnrichmentCompletion, active_stage: ?catalog_types.EnrichmentStage) u64 {
    const stage = active_stage orelse return 0;
    return switch (stage) {
        .lexical_sparse => completion.lexical_sparse_pending_documents,
        .chunk_preview => completion.chunk_preview_pending_documents,
        .chunk_embeddings => completion.chunk_embeddings_pending_documents,
        .rerank_terms => completion.rerank_terms_pending_documents,
    };
}

fn isStageComplete(completion: EnrichmentCompletion, stage: catalog_types.EnrichmentStage) bool {
    return switch (stage) {
        .lexical_sparse => completion.lexical_sparse_complete,
        .chunk_preview => completion.chunk_preview_complete,
        .chunk_embeddings => completion.chunk_embeddings_complete,
        .rerank_terms => completion.rerank_terms_complete,
    };
}

fn enrichmentCompletionAlloc(
    alloc: Allocator,
    artifacts: *artifacts_mod.ArtifactStore,
    manifests: *manifest_mod.ManifestStore,
    namespace: []const u8,
    head_version: u64,
    policy: catalog_types.NamespacePolicy,
) !EnrichmentCompletion {
    if (head_version == 0) return .{};
    var manifest = try manifests.getAlloc(namespace, head_version);
    defer manifest.deinit(alloc);
    const docs = loadPublishedDocumentsForEnrichmentAlloc(alloc, artifacts, manifest) catch return .{};
    defer query_mod.freeMaterializedDocuments(alloc, docs);

    var lexical_pending: u64 = 0;
    var chunk_pending: u64 = 0;
    var chunk_embeddings_pending: u64 = 0;
    var rerank_pending: u64 = 0;
    for (docs) |doc| {
        var projection = document_projection.parseAlloc(alloc, doc.body) catch continue;
        defer projection.deinit(alloc);
        if (policy.enrichment_enabled and (projection.lexical_sparse_version == null or projection.lexical_sparse_version.? < policy.enrichment_pipeline_version)) {
            lexical_pending += 1;
        }
        if (policy.chunk_preview_enabled and (projection.chunk_preview_version == null or projection.chunk_preview_version.? < policy.chunk_preview_pipeline_version)) {
            chunk_pending += 1;
        }
        if (policy.chunk_embeddings_enabled and (projection.chunk_embeddings_version == null or projection.chunk_embeddings_version.? < policy.chunk_embeddings_pipeline_version)) {
            chunk_embeddings_pending += 1;
        }
        if (policy.rerank_terms_enabled and (projection.rerank_terms_version == null or projection.rerank_terms_version.? < policy.rerank_terms_pipeline_version)) {
            rerank_pending += 1;
        }
    }
    return .{
        .lexical_sparse_complete = !policy.enrichment_enabled or lexical_pending == 0,
        .lexical_sparse_pending_documents = lexical_pending,
        .chunk_preview_complete = !policy.chunk_preview_enabled or chunk_pending == 0,
        .chunk_preview_pending_documents = chunk_pending,
        .chunk_embeddings_complete = !policy.chunk_embeddings_enabled or chunk_embeddings_pending == 0,
        .chunk_embeddings_pending_documents = chunk_embeddings_pending,
        .rerank_terms_complete = !policy.rerank_terms_enabled or rerank_pending == 0,
        .rerank_terms_pending_documents = rerank_pending,
    };
}

fn loadPublishedDocumentsForEnrichmentAlloc(
    alloc: Allocator,
    artifacts: *artifacts_mod.ArtifactStore,
    manifest: manifest_mod.Manifest,
) ![]query_mod.QueryMaterializedDocument {
    const document_index = findArtifactIndex(manifest, .document_segment) orelse return error.DocumentSegmentNotFound;
    const payload = try artifacts.getAlloc(manifest.artifacts[document_index].artifact_id);
    defer alloc.free(payload);
    const entries = try document_segment_mod.decodeAlloc(alloc, payload);
    defer document_segment_mod.freeEntries(alloc, entries);
    const base_docs = try allocMaterializedDocumentsForEnrichment(alloc, entries);
    errdefer query_mod.freeMaterializedDocuments(alloc, base_docs);

    const mutation_index = findArtifactIndex(manifest, .mutation_segment) orelse return base_docs;
    const mutation_payload = try artifacts.getAlloc(manifest.artifacts[mutation_index].artifact_id);
    defer alloc.free(mutation_payload);
    const mutation_entries = try segment_mod.decodeAlloc(alloc, mutation_payload);
    defer segment_mod.freeEntries(alloc, mutation_entries);
    const overlay = try allocMaterializerMutationsForEnrichment(alloc, mutation_entries);
    defer freeMaterializerMutationsForEnrichment(alloc, overlay);
    const docs = try query_mod.materializeDocumentsOverBaseAlloc(alloc, base_docs, overlay);
    query_mod.freeMaterializedDocuments(alloc, base_docs);
    return docs;
}

fn allocMaterializedDocumentsForEnrichment(
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

fn allocMaterializerMutationsForEnrichment(
    alloc: Allocator,
    entries: []const segment_mod.Entry,
) ![]query_mod.QueryMaterializerMutation {
    const mutations = try alloc.alloc(query_mod.QueryMaterializerMutation, entries.len);
    errdefer alloc.free(mutations);
    var initialized: usize = 0;
    errdefer freeMaterializerMutationsForEnrichment(alloc, mutations[0..initialized]);
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

fn freeMaterializerMutationsForEnrichment(
    alloc: Allocator,
    mutations: []query_mod.QueryMaterializerMutation,
) void {
    for (mutations) |mutation| {
        alloc.free(mutation.doc_id);
        if (mutation.body) |body| alloc.free(body);
    }
    alloc.free(mutations);
}

const VectorCompactionSignal = struct {
    driver_index_name: ?[]u8 = null,
    metric: ?shared_vector.DistanceMetric = null,
    cluster_count: u32 = 0,
    base_probe_count: u32 = 2,
    shortlist_multiplier: u32 = 2,
    cluster_count_delta: usize = 0,
    base_probe_delta: u32 = 0,
    shortlist_multiplier_delta: u32 = 0,
    cluster_imbalance: f32 = 0,
    distance_span_max: f32 = 0,

    fn deinit(self: *VectorCompactionSignal, alloc: Allocator) void {
        if (self.driver_index_name) |value| alloc.free(value);
        self.* = undefined;
    }
};

fn vectorCompactionSignalAlloc(
    alloc: Allocator,
    artifacts: *artifacts_mod.ArtifactStore,
    manifests: *manifest_mod.ManifestStore,
    namespace: []const u8,
    head_version: u64,
) !VectorCompactionSignal {
    if (head_version == 0) return .{};
    var manifest = try manifests.getAlloc(namespace, head_version);
    defer manifest.deinit(alloc);

    var signal = VectorCompactionSignal{};
    var found = false;
    for (manifest.artifacts) |artifact| {
        if (artifact.kind != .vector_segment) continue;

        const info = builder_mod.readVectorArtifactInfoAlloc(alloc, artifacts, artifact) catch continue;
        if (info.cluster_count == 0) continue;
        const adaptive_policy = builder_mod.adaptiveVectorBuildPolicyForPolicy(
            info,
            manifest.stats.document_count,
            manifest.stats.policy,
        );
        if (!builder_mod.vectorBuildPolicyChanges(adaptive_policy)) continue;
        const delta = builder_mod.vectorBuildPolicyDelta(info, adaptive_policy);
        const should_replace_driver =
            !found or
            delta.cluster_count_delta > signal.cluster_count_delta or
            (delta.cluster_count_delta == signal.cluster_count_delta and delta.base_probe_delta > signal.base_probe_delta) or
            (delta.cluster_count_delta == signal.cluster_count_delta and delta.base_probe_delta == signal.base_probe_delta and delta.shortlist_multiplier_delta > signal.shortlist_multiplier_delta) or
            (delta.cluster_count_delta == signal.cluster_count_delta and delta.base_probe_delta == signal.base_probe_delta and delta.shortlist_multiplier_delta == signal.shortlist_multiplier_delta and info.cluster_imbalance > signal.cluster_imbalance) or
            (delta.cluster_count_delta == signal.cluster_count_delta and delta.base_probe_delta == signal.base_probe_delta and delta.shortlist_multiplier_delta == signal.shortlist_multiplier_delta and info.cluster_imbalance == signal.cluster_imbalance and info.distance_span_max > signal.distance_span_max);
        if (should_replace_driver) {
            if (signal.driver_index_name) |value| alloc.free(value);
            signal.driver_index_name = if (artifact.name.len == 0) null else try alloc.dupe(u8, artifact.name);
            signal.metric = info.metric;
            signal.cluster_count = @intCast(info.cluster_count);
            signal.base_probe_count = info.base_probe_count;
            signal.shortlist_multiplier = info.shortlist_multiplier;
            signal.cluster_count_delta = delta.cluster_count_delta;
            signal.base_probe_delta = delta.base_probe_delta;
            signal.shortlist_multiplier_delta = delta.shortlist_multiplier_delta;
            signal.cluster_imbalance = info.cluster_imbalance;
            signal.distance_span_max = info.distance_span_max;
        }
        found = true;
    }
    return if (found) signal else .{};
}

test "vector compaction signal aggregates named vector artifacts" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-vector-compaction-signal");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-vector-compaction-signal");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);

    var fs_artifacts = try artifacts_mod.FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    const entries_a = try alloc.alloc(vector_segment_mod.Entry, 4);
    entries_a[0] = .{ .doc_id = try alloc.dupe(u8, "a0"), .vector = try alloc.dupe(f32, &.{ 1.0, 0.0 }) };
    entries_a[1] = .{ .doc_id = try alloc.dupe(u8, "a1"), .vector = try alloc.dupe(f32, &.{ 0.9, 0.1 }) };
    entries_a[2] = .{ .doc_id = try alloc.dupe(u8, "a2"), .vector = try alloc.dupe(f32, &.{ 1.1, 0.0 }) };
    entries_a[3] = .{ .doc_id = try alloc.dupe(u8, "a3"), .vector = try alloc.dupe(f32, &.{ 1.0, 0.2 }) };
    var segment_a = try vector_index.buildClusteredSegmentWithPolicyAlloc(alloc, .cosine, 2, entries_a, .{
        .target_cluster_count = 1,
    });
    defer vector_segment_mod.freeSegment(alloc, &segment_a);
    const payload_a = try vector_segment_mod.encodeAlloc(alloc, segment_a);
    defer alloc.free(payload_a);
    var artifact_a = try artifact_store.put(payload_a);
    defer artifact_a.deinit(alloc);

    const entries_b = try alloc.alloc(vector_segment_mod.Entry, 16);
    for (entries_b, 0..) |*entry, idx| {
        entry.* = .{
            .doc_id = try std.fmt.allocPrint(alloc, "b{d}", .{idx}),
            .vector = try alloc.dupe(f32, &.{
                @as(f32, @floatFromInt((idx % 4) * 3)),
                @as(f32, @floatFromInt((idx / 4) * 3)),
            }),
        };
    }
    var segment_b = try vector_index.buildClusteredSegmentWithPolicyAlloc(alloc, .cosine, 2, entries_b, .{
        .target_cluster_count = 4,
    });
    defer vector_segment_mod.freeSegment(alloc, &segment_b);
    const payload_b = try vector_segment_mod.encodeAlloc(alloc, segment_b);
    defer alloc.free(payload_b);
    var artifact_b = try artifact_store.put(payload_b);
    defer artifact_b.deinit(alloc);

    var manifest = manifest_mod.Manifest{
        .namespace = try alloc.dupe(u8, "docs"),
        .version = 1,
        .built_at_ns = 1,
        .wal_start_lsn = 1,
        .wal_end_lsn = 1,
        .stats = .{
            .document_count = 20,
            .vector_segment_count = 2,
            .policy = .{
                .vector_compaction_max_cluster_imbalance = 0.1,
                .vector_compaction_max_distance_span = 0.1,
            },
        },
        .artifacts = try alloc.alloc(manifest_mod.ArtifactRef, 2),
    };
    defer manifest.deinit(alloc);
    manifest.artifacts[0] = .{
        .kind = .vector_segment,
        .name = try alloc.dupe(u8, "semantic_a"),
        .artifact_id = try alloc.dupe(u8, artifact_a.artifact_id),
        .byte_len = artifact_a.byte_len,
        .checksum = try alloc.dupe(u8, artifact_a.checksum),
    };
    manifest.artifacts[1] = .{
        .kind = .vector_segment,
        .name = try alloc.dupe(u8, "semantic_b"),
        .artifact_id = try alloc.dupe(u8, artifact_b.artifact_id),
        .byte_len = artifact_b.byte_len,
        .checksum = try alloc.dupe(u8, artifact_b.checksum),
    };
    try manifest_store.put(manifest);

    var signal = try vectorCompactionSignalAlloc(alloc, &artifact_store, &manifest_store, "docs", 1);
    defer signal.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 4), signal.cluster_count);
    try std.testing.expectEqualStrings("semantic_b", signal.driver_index_name.?);
}

test "vector compaction signal uses driver artifact metrics" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-vector-compaction-driver");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-vector-compaction-driver");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);

    var fs_artifacts = try artifacts_mod.FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    const entries_a = try alloc.alloc(vector_segment_mod.Entry, 4);
    entries_a[0] = .{ .doc_id = try alloc.dupe(u8, "a0"), .vector = try alloc.dupe(f32, &.{ 1.0, 0.0 }) };
    entries_a[1] = .{ .doc_id = try alloc.dupe(u8, "a1"), .vector = try alloc.dupe(f32, &.{ 0.9, 0.1 }) };
    entries_a[2] = .{ .doc_id = try alloc.dupe(u8, "a2"), .vector = try alloc.dupe(f32, &.{ 1.1, 0.0 }) };
    entries_a[3] = .{ .doc_id = try alloc.dupe(u8, "a3"), .vector = try alloc.dupe(f32, &.{ 1.0, 0.2 }) };
    var segment_a = try vector_index.buildClusteredSegmentWithPolicyAlloc(alloc, .cosine, 2, entries_a, .{
        .target_cluster_count = 1,
        .base_probe_count = 2,
        .shortlist_multiplier = 3,
    });
    defer vector_segment_mod.freeSegment(alloc, &segment_a);
    const payload_a = try vector_segment_mod.encodeAlloc(alloc, segment_a);
    defer alloc.free(payload_a);
    var artifact_a = try artifact_store.put(payload_a);
    defer artifact_a.deinit(alloc);

    const entries_b = try alloc.alloc(vector_segment_mod.Entry, 16);
    for (entries_b, 0..) |*entry, idx| {
        entry.* = .{
            .doc_id = try std.fmt.allocPrint(alloc, "b{d}", .{idx}),
            .vector = try alloc.dupe(f32, &.{
                @as(f32, @floatFromInt((idx % 4) * 3)),
                @as(f32, @floatFromInt((idx / 4) * 3)),
            }),
        };
    }
    var segment_b = try vector_index.buildClusteredSegmentWithPolicyAlloc(alloc, .cosine, 2, entries_b, .{
        .target_cluster_count = 4,
        .base_probe_count = 4,
        .shortlist_multiplier = 5,
    });
    defer vector_segment_mod.freeSegment(alloc, &segment_b);
    const payload_b = try vector_segment_mod.encodeAlloc(alloc, segment_b);
    defer alloc.free(payload_b);
    var artifact_b = try artifact_store.put(payload_b);
    defer artifact_b.deinit(alloc);

    const entries_c = try alloc.alloc(vector_segment_mod.Entry, 64);
    for (entries_c, 0..) |*entry, idx| {
        entry.* = .{
            .doc_id = try std.fmt.allocPrint(alloc, "c{d}", .{idx}),
            .vector = try alloc.dupe(f32, &.{
                @as(f32, @floatFromInt((idx % 8) * 2)),
                @as(f32, @floatFromInt((idx / 8) * 2)),
            }),
        };
    }
    var segment_c = try vector_index.buildClusteredSegmentWithPolicyAlloc(alloc, .cosine, 2, entries_c, .{
        .target_cluster_count = 8,
        .base_probe_count = 8,
        .shortlist_multiplier = 9,
    });
    defer vector_segment_mod.freeSegment(alloc, &segment_c);
    const payload_c = try vector_segment_mod.encodeAlloc(alloc, segment_c);
    defer alloc.free(payload_c);
    var artifact_c = try artifact_store.put(payload_c);
    defer artifact_c.deinit(alloc);

    var manifest = manifest_mod.Manifest{
        .namespace = try alloc.dupe(u8, "docs"),
        .version = 1,
        .built_at_ns = 1,
        .wal_start_lsn = 1,
        .wal_end_lsn = 1,
        .stats = .{
            .document_count = 84,
            .vector_segment_count = 3,
            .policy = .{
                .vector_compaction_max_cluster_imbalance = 0.1,
                .vector_compaction_max_distance_span = 0.1,
            },
        },
        .artifacts = try alloc.alloc(manifest_mod.ArtifactRef, 3),
    };
    defer manifest.deinit(alloc);
    manifest.artifacts[0] = .{
        .kind = .vector_segment,
        .name = try alloc.dupe(u8, "semantic_a"),
        .artifact_id = try alloc.dupe(u8, artifact_a.artifact_id),
        .byte_len = artifact_a.byte_len,
        .checksum = try alloc.dupe(u8, artifact_a.checksum),
    };
    manifest.artifacts[1] = .{
        .kind = .vector_segment,
        .name = try alloc.dupe(u8, "semantic_b"),
        .artifact_id = try alloc.dupe(u8, artifact_b.artifact_id),
        .byte_len = artifact_b.byte_len,
        .checksum = try alloc.dupe(u8, artifact_b.checksum),
    };
    manifest.artifacts[2] = .{
        .kind = .vector_segment,
        .name = try alloc.dupe(u8, "semantic_c"),
        .artifact_id = try alloc.dupe(u8, artifact_c.artifact_id),
        .byte_len = artifact_c.byte_len,
        .checksum = try alloc.dupe(u8, artifact_c.checksum),
    };
    try manifest_store.put(manifest);

    var signal = try vectorCompactionSignalAlloc(alloc, &artifact_store, &manifest_store, "docs", 1);
    defer signal.deinit(alloc);
    try std.testing.expectEqualStrings("semantic_c", signal.driver_index_name.?);
    try std.testing.expectEqual(@as(u32, 8), signal.cluster_count);
    try std.testing.expectEqual(@as(u32, 8), signal.base_probe_count);
    try std.testing.expectEqual(@as(u32, 9), signal.shortlist_multiplier);
}

test "vector compaction signal ignores artifacts whose adaptive policy is a no-op" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-vector-compaction-noop");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-vector-compaction-noop");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);

    var fs_artifacts = try artifacts_mod.FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    const entries = try alloc.alloc(vector_segment_mod.Entry, 8);
    for (entries, 0..) |*entry, idx| {
        entry.* = .{
            .doc_id = try std.fmt.allocPrint(alloc, "d{d}", .{idx}),
            .vector = try alloc.dupe(f32, &.{
                @as(f32, @floatFromInt(idx % 2)),
                @as(f32, @floatFromInt(idx / 2)),
            }),
        };
    }
    var segment = try vector_index.buildClusteredSegmentWithPolicyAlloc(alloc, .cosine, 2, entries, .{
        .target_cluster_count = 1,
        .base_probe_count = 2,
        .shortlist_multiplier = 2,
    });
    defer vector_segment_mod.freeSegment(alloc, &segment);
    const payload = try vector_segment_mod.encodeAlloc(alloc, segment);
    defer alloc.free(payload);
    var artifact = try artifact_store.put(payload);
    defer artifact.deinit(alloc);

    var manifest = manifest_mod.Manifest{
        .namespace = try alloc.dupe(u8, "docs"),
        .version = 1,
        .built_at_ns = 1,
        .wal_start_lsn = 1,
        .wal_end_lsn = 1,
        .stats = .{
            .document_count = 8,
            .vector_segment_count = 1,
            .policy = .{
                .vector_compaction_max_cluster_imbalance = 100,
                .vector_compaction_max_distance_span = 100,
            },
        },
        .artifacts = try alloc.alloc(manifest_mod.ArtifactRef, 1),
    };
    defer manifest.deinit(alloc);
    manifest.artifacts[0] = .{
        .kind = .vector_segment,
        .name = try alloc.dupe(u8, "semantic"),
        .artifact_id = try alloc.dupe(u8, artifact.artifact_id),
        .byte_len = artifact.byte_len,
        .checksum = try alloc.dupe(u8, artifact.checksum),
    };
    try manifest_store.put(manifest);

    var signal = try vectorCompactionSignalAlloc(alloc, &artifact_store, &manifest_store, "docs", 1);
    defer signal.deinit(alloc);
    try std.testing.expect(signal.driver_index_name == null);
    try std.testing.expectEqual(@as(u32, 0), signal.cluster_count);
}

fn countRetainedArtifactsAlloc(
    alloc: Allocator,
    manifests: *manifest_mod.ManifestStore,
    namespace: []const u8,
    versions: []const u64,
) !usize {
    var artifact_ids = std.StringHashMapUnmanaged(void).empty;
    defer {
        var it = artifact_ids.iterator();
        while (it.next()) |entry| alloc.free(entry.key_ptr.*);
        artifact_ids.deinit(alloc);
    }

    for (versions) |version| {
        var manifest = try manifests.getAlloc(namespace, version);
        defer manifest.deinit(alloc);
        for (manifest.artifacts) |artifact| {
            if (artifact_ids.contains(artifact.artifact_id)) continue;
            const owned = try alloc.dupe(u8, artifact.artifact_id);
            errdefer alloc.free(owned);
            try artifact_ids.put(alloc, owned, {});
        }
    }
    return artifact_ids.count();
}

fn headHasArtifactKind(
    alloc: Allocator,
    manifests: *manifest_mod.ManifestStore,
    namespace: []const u8,
    head_version: u64,
    kind: manifest_mod.ArtifactKind,
) !bool {
    if (head_version == 0) return false;
    var manifest = try manifests.getAlloc(namespace, head_version);
    defer manifest.deinit(alloc);
    for (manifest.artifacts) |artifact| {
        if (artifact.kind == kind) return true;
    }
    return false;
}

fn findArtifactIndex(manifest: manifest_mod.Manifest, kind: manifest_mod.ArtifactKind) ?usize {
    for (manifest.artifacts, 0..) |artifact, idx| {
        if (artifact.kind == kind) return idx;
    }
    return null;
}

test "catalog service tracks namespaces and reports build status" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    var catalog_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests");
    const wal_root = tmpPath(&wal_root_buf, "wal");
    const catalog_root = tmpPath(&catalog_root_buf, "catalog");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);
    defer cleanupTmp(catalog_root);

    var fs_artifacts = try @import("../artifacts/mod.zig").FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try @import("fs_progress_store.zig").FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try wal_mod.FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_catalog = try @import("fs_store.zig").FsStore.init(alloc, std.mem.span(catalog_root));
    var fs_catalog_store = fs_catalog.catalogStore();
    defer fs_catalog_store.deinit();

    var builder = builder_mod.Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var catalog = CatalogService.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store, &builder, &fs_catalog_store);
    defer catalog.deinit();

    try std.testing.expect(try catalog.ensureNamespace("docs", 100));
    try std.testing.expect(!(try catalog.ensureNamespace("docs", 200)));

    const listed = try catalog.listNamespacesAlloc(alloc);
    defer catalog.freeNamespaces(alloc, listed);
    try std.testing.expectEqual(@as(usize, 1), listed.len);
    try std.testing.expectEqualStrings("docs", listed[0].name);
    try std.testing.expectEqual(catalog_types.DefaultQueryView.published, listed[0].policy.default_query_view);

    const encoded_a = try api_codec.encodeMutationAlloc(alloc, .{
        .kind = api_types.MutationKind.upsert,
        .doc_id = "doc-a",
        .body = "payload-a",
    });
    defer alloc.free(encoded_a);
    const encoded_b = try api_codec.encodeMutationAlloc(alloc, .{
        .kind = api_types.MutationKind.upsert,
        .doc_id = "doc-b",
        .body = "payload-b",
    });
    defer alloc.free(encoded_b);
    _ = try wal_store.append("docs", 111, encoded_a);
    _ = try wal_store.append("docs", 222, encoded_b);

    var before = try catalog.buildStatus("docs");
    defer before.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 0), before.head_version);
    try std.testing.expectEqual(@as(u64, 2), before.latest_wal_lsn);
    try std.testing.expectEqual(@as(u64, 2), before.freshness_lag_records);
    try std.testing.expectEqual(@as(u64, 2), before.pending_records);
    try std.testing.expectEqual(@as(u64, 1), before.next_version);
    try std.testing.expect(before.publish_admitted);
    try std.testing.expect(before.publish_recommended);
    try std.testing.expectEqual(@as(usize, 0), before.retained_versions);
    try std.testing.expectEqual(@as(usize, 0), before.retained_artifacts);
    try std.testing.expectEqual(catalog_types.MutationTailResolution.none, before.mutation_tail_resolution);
    try std.testing.expect(before.enrichment_complete);

    var build = try catalog.buildNamespace("docs");
    defer build.deinit(alloc);

    var after = try catalog.buildStatus("docs");
    defer after.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 1), after.head_version);
    try std.testing.expectEqual(@as(u64, 2), after.published_wal_end_lsn);
    try std.testing.expectEqual(@as(u64, 2), after.latest_wal_lsn);
    try std.testing.expectEqual(@as(u64, 0), after.freshness_lag_records);
    try std.testing.expectEqual(@as(u64, 0), after.pending_records);
    try std.testing.expectEqual(@as(u64, 2), after.next_version);
    try std.testing.expect(after.publish_admitted);
    try std.testing.expect(!after.publish_recommended);
    try std.testing.expectEqual(catalog_types.MutationTailResolution.none, after.mutation_tail_resolution);
    try std.testing.expectEqual(@as(usize, 1), after.retained_versions);
    try std.testing.expectEqual(@as(usize, 3), after.retained_artifacts);
    try std.testing.expect(!after.compaction_recommended);
    try std.testing.expect(after.enrichment_complete);
}

test "catalog service stores per-namespace policy" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-policy");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-policy");
    const wal_root = tmpPath(&wal_root_buf, "wal-policy");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);

    var fs_artifacts = try @import("../artifacts/mod.zig").FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try @import("fs_progress_store.zig").FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try wal_mod.FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var catalog_root_buf: [256]u8 = undefined;
    const catalog_root = tmpPath(&catalog_root_buf, "catalog-policy");
    defer cleanupTmp(catalog_root);
    var fs_catalog = try @import("fs_store.zig").FsStore.init(alloc, std.mem.span(catalog_root));
    var fs_catalog_store = fs_catalog.catalogStore();
    defer fs_catalog_store.deinit();

    var builder = builder_mod.Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var catalog = CatalogService.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store, &builder, &fs_catalog_store);
    defer catalog.deinit();

    try std.testing.expect(try catalog.ensureNamespaceWithPolicy("docs", 100, .{
        .default_query_view = .latest,
        .keep_latest_versions = 5,
    }));

    const initial = try catalog.getPolicy("docs");
    try std.testing.expectEqual(catalog_types.DefaultQueryView.latest, initial.default_query_view);
    try std.testing.expectEqual(@as(usize, 5), initial.keep_latest_versions);
    try std.testing.expectEqual(@as(u64, 1024), initial.max_pending_records);

    const updated = try catalog.setPolicy("docs", .{
        .default_query_view = .published,
        .keep_latest_versions = 3,
        .max_pending_records = 2,
        .compaction_enabled = false,
        .compaction_trigger_version_count = 4,
        .enrichment_batch_size = 8,
        .enrichment_pipeline_version = 2,
    });
    try std.testing.expectEqual(catalog_types.DefaultQueryView.published, updated.default_query_view);
    try std.testing.expectEqual(@as(usize, 3), updated.keep_latest_versions);
    try std.testing.expectEqual(@as(u64, 2), updated.max_pending_records);
    try std.testing.expectEqual(false, updated.compaction_enabled);
    try std.testing.expectEqual(@as(usize, 4), updated.compaction_trigger_version_count);
    try std.testing.expectEqual(@as(usize, 8), updated.enrichment_batch_size);
    try std.testing.expectEqual(@as(u32, 2), updated.enrichment_pipeline_version);
}

test "catalog service exposes table records over serving namespaces" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    var catalog_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-table-compat");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-table-compat");
    const wal_root = tmpPath(&wal_root_buf, "wal-table-compat");
    const catalog_root = tmpPath(&catalog_root_buf, "catalog-table-compat");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);
    defer cleanupTmp(catalog_root);

    var fs_artifacts = try @import("../artifacts/mod.zig").FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try @import("fs_progress_store.zig").FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try wal_mod.FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_catalog = try @import("fs_store.zig").FsStore.init(alloc, std.mem.span(catalog_root));
    var fs_catalog_store = fs_catalog.catalogStore();
    defer fs_catalog_store.deinit();

    var builder = builder_mod.Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var catalog = CatalogService.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store, &builder, &fs_catalog_store);
    defer catalog.deinit();

    try std.testing.expect(try catalog.ensureTableWithDefinition(
        "docs",
        100,
        .{
            .default_query_view = .latest,
        },
        "{\"default_type\":\"doc\"}",
        "",
        "{\"semantic_idx\":{\"type\":\"embeddings\",\"dimension\":3},\"sparse_idx\":{\"type\":\"embeddings\",\"sparse\":true}}",
    ));

    const tables = try catalog.listTablesAlloc(alloc);
    defer catalog.freeTables(alloc, tables);

    try std.testing.expectEqual(@as(usize, 1), tables.len);
    try std.testing.expectEqualStrings("docs", tables[0].table_name);
    try std.testing.expectEqualStrings("docs", tables[0].namespace);
    try std.testing.expectEqual(catalog_types.DefaultQueryView.latest, tables[0].policy.default_query_view);
    try std.testing.expectEqualStrings("{\"default_type\":\"doc\"}", tables[0].schema_json);
    try std.testing.expectEqualStrings("", tables[0].read_schema_json);
    try std.testing.expectEqualStrings("{\"semantic_idx\":{\"type\":\"embeddings\",\"dimension\":3},\"sparse_idx\":{\"type\":\"embeddings\",\"sparse\":true}}", tables[0].indexes_json);

    var status = try catalog.buildStatus("docs");
    defer status.deinit(alloc);
    try std.testing.expectEqualStrings("semantic_idx", status.published_search_sources.findVector().?.index_name);
    try std.testing.expectEqualStrings("sparse_idx", status.published_search_sources.findSparse().?.index_name);

    var api = @import("../api/service.zig").Service.init(alloc, &wal_store, &builder);
    const mutations = [_]api_types.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "{\"text\":\"alpha\",\"embedding\":[1,0,0],\"sparse_embedding\":{\"alpha\":1.0}}" },
    };
    var ingest = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 123, .mutations = &mutations });
    defer ingest.deinit(alloc);

    var build = try catalog.buildTable("docs");
    defer build.deinit(alloc);
    try std.testing.expect(build.published);

    var after_build = try catalog.buildStatus("docs");
    defer after_build.deinit(alloc);
    try std.testing.expectEqualStrings("semantic_idx", after_build.materialized_search_sources.findVector().?.index_name);
    try std.testing.expectEqualStrings("sparse_idx", after_build.materialized_search_sources.findSparse().?.index_name);
}

test "catalog service republishes head when table index metadata changes without new wal" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    var catalog_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-table-metadata-republish");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-table-metadata-republish");
    const wal_root = tmpPath(&wal_root_buf, "wal-table-metadata-republish");
    const catalog_root = tmpPath(&catalog_root_buf, "catalog-table-metadata-republish");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);
    defer cleanupTmp(catalog_root);

    var fs_artifacts = try @import("../artifacts/mod.zig").FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try @import("fs_progress_store.zig").FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try wal_mod.FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_catalog = try @import("fs_store.zig").FsStore.init(alloc, std.mem.span(catalog_root));
    var fs_catalog_store = fs_catalog.catalogStore();
    defer fs_catalog_store.deinit();

    var builder = builder_mod.Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var catalog = CatalogService.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store, &builder, &fs_catalog_store);
    defer catalog.deinit();

    try std.testing.expect(try catalog.ensureTableWithDefinition(
        "docs",
        100,
        .{},
        "{\"default_type\":\"doc\"}",
        "",
        "{\"semantic_idx\":{\"type\":\"embeddings\",\"dimension\":3},\"sparse_idx\":{\"type\":\"embeddings\",\"sparse\":true}}",
    ));

    var api = @import("../api/service.zig").Service.init(alloc, &wal_store, &builder);
    const mutations = [_]api_types.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "{\"text\":\"alpha\",\"embedding\":[1,0,0],\"sparse_embedding\":{\"alpha\":1.0}}" },
    };
    var ingest = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 123, .mutations = &mutations });
    defer ingest.deinit(alloc);

    var first_build = try catalog.buildTable("docs");
    defer first_build.deinit(alloc);
    try std.testing.expect(first_build.published);
    try std.testing.expectEqual(@as(u64, 1), first_build.version);
    try std.testing.expectEqual(@as(u64, 1), first_build.wal_end_lsn);

    var before = try catalog.buildStatus("docs");
    defer before.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 1), before.head_version);
    try std.testing.expectEqual(@as(u64, 1), before.published_wal_end_lsn);
    try std.testing.expectEqualStrings("semantic_idx", before.materialized_search_sources.findVector().?.index_name);
    try std.testing.expectEqualStrings("sparse_idx", before.materialized_search_sources.findSparse().?.index_name);
    try std.testing.expect(!before.publish_recommended);

    try std.testing.expect(try catalog.setTableDefinition(
        "docs",
        "{\"default_type\":\"doc\"}",
        "",
        "{\"semantic_idx_v2\":{\"type\":\"embeddings\",\"dimension\":3}}",
    ));

    var status = try catalog.buildStatus("docs");
    defer status.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 1), status.head_version);
    try std.testing.expectEqual(@as(u64, 1), status.published_wal_end_lsn);
    try std.testing.expect(status.publish_recommended);
    try std.testing.expectEqualStrings("semantic_idx", status.materialized_search_sources.findVector().?.index_name);
    try std.testing.expectEqualStrings("sparse_idx", status.materialized_search_sources.findSparse().?.index_name);

    var rebuild = try catalog.buildTable("docs");
    defer rebuild.deinit(alloc);
    try std.testing.expect(rebuild.published);
    try std.testing.expectEqual(@as(u64, 2), rebuild.version);
    try std.testing.expectEqual(@as(u64, 1), rebuild.wal_end_lsn);

    var after = try catalog.buildStatus("docs");
    defer after.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 2), after.head_version);
    try std.testing.expectEqual(@as(u64, 1), after.published_wal_end_lsn);
    try std.testing.expectEqualStrings("semantic_idx_v2", after.materialized_search_sources.findVector().?.index_name);
    try std.testing.expect(after.materialized_search_sources.findSparse() == null);
    try std.testing.expect(!after.publish_recommended);
}

test "catalog service republishes head when derived output policy changes without new wal" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    var catalog_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-derived-policy-republish");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-derived-policy-republish");
    const wal_root = tmpPath(&wal_root_buf, "wal-derived-policy-republish");
    const catalog_root = tmpPath(&catalog_root_buf, "catalog-derived-policy-republish");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);
    defer cleanupTmp(catalog_root);

    var fs_artifacts = try @import("../artifacts/mod.zig").FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try @import("fs_progress_store.zig").FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try wal_mod.FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_catalog = try @import("fs_store.zig").FsStore.init(alloc, std.mem.span(catalog_root));
    var fs_catalog_store = fs_catalog.catalogStore();
    defer fs_catalog_store.deinit();

    var builder = builder_mod.Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var catalog = CatalogService.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store, &builder, &fs_catalog_store);
    defer catalog.deinit();

    try std.testing.expect(try catalog.ensureTableWithDefinition(
        "docs",
        100,
        .{ .chunk_preview_enabled = true },
        "{\"default_type\":\"doc\"}",
        "",
        "{\"semantic_idx\":{\"type\":\"embeddings\",\"dimension\":3}}",
    ));

    var api = @import("../api/service.zig").Service.init(alloc, &wal_store, &builder);
    const mutations = [_]api_types.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "{\"text\":\"alpha\",\"embedding\":[1,0,0],\"chunk_preview\":[\"alpha\"]}" },
    };
    var ingest = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 123, .mutations = &mutations });
    defer ingest.deinit(alloc);

    var first_build = try catalog.buildTable("docs");
    defer first_build.deinit(alloc);
    try std.testing.expect(first_build.published);

    var first_manifest = try manifest_store.getAlloc("docs", 1);
    defer first_manifest.deinit(alloc);
    const first_vector = first_manifest.artifacts[builder_mod.findNamedArtifactIndex(first_manifest, .vector_segment, "semantic_idx").?];
    const first_payload = try artifact_store.getAlloc(first_vector.artifact_id);
    defer alloc.free(first_payload);
    const first_header = try vector_segment_mod.decodeHeader(first_payload[0..vector_segment_mod.header_len]);
    try std.testing.expectEqual(shared_vector.DistanceMetric.cosine, first_header.metric);

    var before = try catalog.buildStatus("docs");
    defer before.deinit(alloc);
    try std.testing.expect(before.materialized_derived_outputs.containsKind(.chunk_preview));
    try std.testing.expect(!before.publish_recommended);

    _ = try catalog.setPolicy("docs", .{ .chunk_preview_enabled = false });

    var status = try catalog.buildStatus("docs");
    defer status.deinit(alloc);
    try std.testing.expect(status.publish_recommended);
    try std.testing.expectEqual(catalog_types.NextPublishReason.head_republish, status.next_publish_reason.?);
    try std.testing.expect(status.head_republish_recommended);
    try std.testing.expect(!status.pending_materialization_rebuild);
    try std.testing.expectEqual(catalog_types.DerivedOutputPublicationAction.drop, status.derived_output_actions.chunk_preview);
    try std.testing.expect(status.materialized_derived_outputs.containsKind(.chunk_preview));

    var rebuild = try catalog.buildTable("docs");
    defer rebuild.deinit(alloc);
    try std.testing.expect(rebuild.published);
    try std.testing.expectEqual(@as(u64, 2), rebuild.version);
    try std.testing.expectEqual(@as(u64, 1), rebuild.wal_end_lsn);

    var after = try catalog.buildStatus("docs");
    defer after.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 2), after.head_version);
    try std.testing.expectEqual(@as(u64, 1), after.published_wal_end_lsn);
    try std.testing.expect(!after.materialized_derived_outputs.containsKind(.chunk_preview));
    try std.testing.expect(!after.publish_recommended);
}

test "catalog service republishes head when graph index metadata changes without new wal" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    var catalog_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-graph-index-republish");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-graph-index-republish");
    const wal_root = tmpPath(&wal_root_buf, "wal-graph-index-republish");
    const catalog_root = tmpPath(&catalog_root_buf, "catalog-graph-index-republish");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);
    defer cleanupTmp(catalog_root);

    var fs_artifacts = try @import("../artifacts/mod.zig").FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try @import("fs_progress_store.zig").FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try wal_mod.FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_catalog = try @import("fs_store.zig").FsStore.init(alloc, std.mem.span(catalog_root));
    var fs_catalog_store = fs_catalog.catalogStore();
    defer fs_catalog_store.deinit();

    var builder = builder_mod.Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var catalog = CatalogService.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store, &builder, &fs_catalog_store);
    defer catalog.deinit();

    try std.testing.expect(try catalog.ensureTableWithDefinition(
        "docs",
        100,
        .{},
        "{\"default_type\":\"doc\"}",
        "",
        "{\"semantic_idx\":{\"type\":\"embeddings\",\"dimension\":3,\"distance_metric\":\"cosine\"}}",
    ));

    var api = @import("../api/service.zig").Service.init(alloc, &wal_store, &builder);
    const mutations = [_]api_types.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "{\"text\":\"alpha\",\"embedding\":[1,0,0],\"graph_edges\":[{\"type\":\"related\",\"to\":\"doc-b\"}]}" },
    };
    var ingest = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 123, .mutations = &mutations });
    defer ingest.deinit(alloc);

    var first_build = try catalog.buildTable("docs");
    defer first_build.deinit(alloc);
    try std.testing.expect(first_build.published);

    var first_manifest = try manifest_store.getAlloc("docs", 1);
    defer first_manifest.deinit(alloc);
    const first_vector = first_manifest.artifacts[builder_mod.findNamedArtifactIndex(first_manifest, .vector_segment, "semantic_idx").?];
    const first_payload = try artifact_store.getAlloc(first_vector.artifact_id);
    defer alloc.free(first_payload);
    const first_header = try vector_segment_mod.decodeHeader(first_payload[0..vector_segment_mod.header_len]);
    try std.testing.expectEqual(shared_vector.DistanceMetric.cosine, first_header.metric);

    var before = try catalog.buildStatus("docs");
    defer before.deinit(alloc);
    try std.testing.expect(!before.publish_recommended);

    try std.testing.expect(try catalog.setTableDefinition(
        "docs",
        "{\"default_type\":\"doc\"}",
        "",
        "{\"semantic_idx\":{\"type\":\"embeddings\",\"dimension\":3},\"graph_idx\":{\"type\":\"graph\"}}",
    ));

    var status = try catalog.buildStatus("docs");
    defer status.deinit(alloc);
    try std.testing.expect(status.publish_recommended);
    try std.testing.expectEqual(catalog_types.NextPublishReason.head_republish, status.next_publish_reason.?);
    try std.testing.expect(status.head_republish_recommended);
    try std.testing.expect(!status.pending_materialization_rebuild);
    try std.testing.expectEqual(catalog_types.ArtifactPublicationAction.rebuild, status.artifact_actions.graph);
    try std.testing.expectEqual(catalog_types.ArtifactPublicationAction.rebuild, findNamedArtifactAction(status.graph_index_actions, "graph_idx").?);

    var rebuild = try catalog.buildTable("docs");
    defer rebuild.deinit(alloc);
    try std.testing.expect(rebuild.published);
    try std.testing.expectEqual(@as(u64, 2), rebuild.version);
    try std.testing.expectEqual(@as(u64, 1), rebuild.wal_end_lsn);

    var after = try catalog.buildStatus("docs");
    defer after.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 2), after.head_version);
    try std.testing.expectEqual(@as(u64, 1), after.published_wal_end_lsn);
    try std.testing.expect(!after.publish_recommended);
    try std.testing.expectEqual(catalog_types.ArtifactPublicationAction.rebuild, findNamedArtifactAction(after.head_graph_index_actions, "graph_idx").?);
}

test "catalog service republishes head when dense index config changes without renaming source" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    var catalog_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-dense-config-republish");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-dense-config-republish");
    const wal_root = tmpPath(&wal_root_buf, "wal-dense-config-republish");
    const catalog_root = tmpPath(&catalog_root_buf, "catalog-dense-config-republish");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);
    defer cleanupTmp(catalog_root);

    var fs_artifacts = try @import("../artifacts/mod.zig").FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try @import("fs_progress_store.zig").FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try wal_mod.FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_catalog = try @import("fs_store.zig").FsStore.init(alloc, std.mem.span(catalog_root));
    var fs_catalog_store = fs_catalog.catalogStore();
    defer fs_catalog_store.deinit();

    var builder = builder_mod.Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var catalog = CatalogService.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store, &builder, &fs_catalog_store);
    defer catalog.deinit();

    try std.testing.expect(try catalog.ensureTableWithDefinition(
        "docs",
        100,
        .{},
        "{\"default_type\":\"doc\"}",
        "",
        "{\"semantic_idx\":{\"type\":\"embeddings\",\"dimension\":3,\"distance_metric\":\"cosine\"}}",
    ));

    var api = @import("../api/service.zig").Service.init(alloc, &wal_store, &builder);
    const mutations = [_]api_types.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "{\"text\":\"alpha\",\"embedding\":[1,0,0]}" },
    };
    var ingest = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 123, .mutations = &mutations });
    defer ingest.deinit(alloc);

    var first_build = try catalog.buildTable("docs");
    defer first_build.deinit(alloc);
    try std.testing.expect(first_build.published);

    var first_manifest = try manifest_store.getAlloc("docs", 1);
    defer first_manifest.deinit(alloc);
    const first_vector = first_manifest.artifacts[builder_mod.findNamedArtifactIndex(first_manifest, .vector_segment, "semantic_idx").?];
    const first_payload = try artifact_store.getAlloc(first_vector.artifact_id);
    defer alloc.free(first_payload);
    const first_header = try vector_segment_mod.decodeHeader(first_payload[0..vector_segment_mod.header_len]);
    try std.testing.expectEqual(shared_vector.DistanceMetric.cosine, first_header.metric);

    var before = try catalog.buildStatus("docs");
    defer before.deinit(alloc);
    try std.testing.expectEqualStrings("semantic_idx", before.materialized_search_sources.findVector().?.index_name);
    try std.testing.expect(!before.publish_recommended);

    try std.testing.expect(try catalog.setTableDefinition(
        "docs",
        "{\"default_type\":\"doc\"}",
        "",
        "{\"semantic_idx\":{\"type\":\"embeddings\",\"dimension\":3,\"distance_metric\":\"inner_product\"}}",
    ));

    var status = try catalog.buildStatus("docs");
    defer status.deinit(alloc);
    try std.testing.expect(status.publish_recommended);
    try std.testing.expectEqual(catalog_types.NextPublishReason.head_republish, status.next_publish_reason.?);
    try std.testing.expect(status.head_republish_recommended);
    try std.testing.expect(!status.pending_materialization_rebuild);
    try std.testing.expectEqual(catalog_types.ArtifactPublicationAction.rebuild, status.artifact_actions.dense_vector);
    try std.testing.expectEqualStrings("semantic_idx", status.materialized_search_sources.findVector().?.index_name);

    var rebuild = try catalog.buildTable("docs");
    defer rebuild.deinit(alloc);
    try std.testing.expect(rebuild.published);
    try std.testing.expectEqual(@as(u64, 2), rebuild.version);
    try std.testing.expectEqual(@as(u64, 1), rebuild.wal_end_lsn);

    var after = try catalog.buildStatus("docs");
    defer after.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 2), after.head_version);
    try std.testing.expectEqual(@as(u64, 1), after.published_wal_end_lsn);
    try std.testing.expectEqualStrings("semantic_idx", after.materialized_search_sources.findVector().?.index_name);
    try std.testing.expect(!after.publish_recommended);

    var second_manifest = try manifest_store.getAlloc("docs", 2);
    defer second_manifest.deinit(alloc);
    const second_vector = second_manifest.artifacts[builder_mod.findNamedArtifactIndex(second_manifest, .vector_segment, "semantic_idx").?];
    const second_payload = try artifact_store.getAlloc(second_vector.artifact_id);
    defer alloc.free(second_payload);
    const second_header = try vector_segment_mod.decodeHeader(second_payload[0..vector_segment_mod.header_len]);
    try std.testing.expectEqual(shared_vector.DistanceMetric.inner_product, second_header.metric);
    try std.testing.expect(!std.mem.eql(u8, first_vector.artifact_id, second_vector.artifact_id));
}

test "catalog service reports chunk embeddings changes as pending materialization rebuilds" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    var catalog_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-chunk-embeddings-status");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-chunk-embeddings-status");
    const wal_root = tmpPath(&wal_root_buf, "wal-chunk-embeddings-status");
    const catalog_root = tmpPath(&catalog_root_buf, "catalog-chunk-embeddings-status");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);
    defer cleanupTmp(catalog_root);

    var fs_artifacts = try @import("../artifacts/mod.zig").FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try @import("fs_progress_store.zig").FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try wal_mod.FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_catalog = try @import("fs_store.zig").FsStore.init(alloc, std.mem.span(catalog_root));
    var fs_catalog_store = fs_catalog.catalogStore();
    defer fs_catalog_store.deinit();

    var builder = builder_mod.Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var catalog = CatalogService.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store, &builder, &fs_catalog_store);
    defer catalog.deinit();

    try std.testing.expect(try catalog.ensureTableWithDefinition(
        "docs",
        100,
        .{},
        "{\"default_type\":\"doc\"}",
        "",
        "{\"semantic_idx\":{\"type\":\"embeddings\",\"dimension\":3}}",
    ));

    var api = @import("../api/service.zig").Service.init(alloc, &wal_store, &builder);
    const mutations = [_]api_types.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "{\"text\":\"alpha\",\"embedding\":[1,0,0]}" },
    };
    var ingest = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 123, .mutations = &mutations });
    defer ingest.deinit(alloc);

    var first_build = try catalog.buildTable("docs");
    defer first_build.deinit(alloc);
    try std.testing.expect(first_build.published);

    _ = try catalog.setPolicy("docs", .{ .chunk_embeddings_enabled = true });

    var status = try catalog.buildStatus("docs");
    defer status.deinit(alloc);
    try std.testing.expect(!status.head_republish_recommended);
    try std.testing.expect(status.pending_materialization_rebuild);
    try std.testing.expect(!status.publish_recommended);
    try std.testing.expectEqual(catalog_types.ArtifactPublicationAction.reuse, status.artifact_actions.dense_vector);
    try std.testing.expectEqual(catalog_types.DerivedOutputPublicationAction.recompute, status.derived_output_actions.chunk_embeddings);
    try std.testing.expectEqual(catalog_types.DerivedOutputResolution.pending_materialization, status.derived_output_resolutions.chunk_embeddings);
    try std.testing.expect(status.pending_materialization_families.chunk_embeddings);
    try std.testing.expect(!status.pending_materialization_families.dense_vector);
}

test "catalog service republishes head when chunk embeddings are already materialized" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    var catalog_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-chunk-embeddings-republish");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-chunk-embeddings-republish");
    const wal_root = tmpPath(&wal_root_buf, "wal-chunk-embeddings-republish");
    const catalog_root = tmpPath(&catalog_root_buf, "catalog-chunk-embeddings-republish");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);
    defer cleanupTmp(catalog_root);

    var fs_artifacts = try @import("../artifacts/mod.zig").FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try @import("fs_progress_store.zig").FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try wal_mod.FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_catalog = try @import("fs_store.zig").FsStore.init(alloc, std.mem.span(catalog_root));
    var fs_catalog_store = fs_catalog.catalogStore();
    defer fs_catalog_store.deinit();

    var builder = builder_mod.Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var catalog = CatalogService.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store, &builder, &fs_catalog_store);
    defer catalog.deinit();

    try std.testing.expect(try catalog.ensureTableWithDefinition(
        "docs",
        100,
        .{},
        "{\"default_type\":\"doc\"}",
        "",
        "{\"semantic_idx\":{\"type\":\"embeddings\",\"dimension\":3}}",
    ));

    var api = @import("../api/service.zig").Service.init(alloc, &wal_store, &builder);
    const mutations = [_]api_types.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "{\"text\":\"alpha\",\"chunk_embeddings\":[{\"chunk\":\"alpha\",\"embedding\":[1,0,0]}],\"_enrichment\":{\"chunk_embeddings\":true,\"chunk_embeddings_version\":1}}" },
    };
    var ingest = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 123, .mutations = &mutations });
    defer ingest.deinit(alloc);

    var first_build = try catalog.buildTable("docs");
    defer first_build.deinit(alloc);
    try std.testing.expect(first_build.published);

    var before = try catalog.buildStatus("docs");
    defer before.deinit(alloc);
    try std.testing.expect(before.materialized_derived_outputs.containsKind(.chunk_embeddings));
    try std.testing.expect(!before.publish_recommended);

    _ = try catalog.setPolicy("docs", .{ .chunk_embeddings_enabled = true });

    var status = try catalog.buildStatus("docs");
    defer status.deinit(alloc);
    try std.testing.expect(status.publish_recommended);
    try std.testing.expectEqual(catalog_types.NextPublishReason.head_republish, status.next_publish_reason.?);
    try std.testing.expect(status.head_republish_recommended);
    try std.testing.expect(!status.pending_materialization_rebuild);
    try std.testing.expectEqual(catalog_types.DerivedOutputPublicationAction.reuse, status.derived_output_actions.chunk_embeddings);
    try std.testing.expectEqual(catalog_types.DerivedOutputResolution.head_republish_reuse, status.derived_output_resolutions.chunk_embeddings);
    try std.testing.expect(!status.pending_materialization_families.chunk_embeddings);
}

test "catalog service republishes head when chunk preview is already materialized" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    var catalog_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-chunk-preview-republish");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-chunk-preview-republish");
    const wal_root = tmpPath(&wal_root_buf, "wal-chunk-preview-republish");
    const catalog_root = tmpPath(&catalog_root_buf, "catalog-chunk-preview-republish");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);
    defer cleanupTmp(catalog_root);

    var fs_artifacts = try @import("../artifacts/mod.zig").FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try @import("fs_progress_store.zig").FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try wal_mod.FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_catalog = try @import("fs_store.zig").FsStore.init(alloc, std.mem.span(catalog_root));
    var fs_catalog_store = fs_catalog.catalogStore();
    defer fs_catalog_store.deinit();

    var builder = builder_mod.Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var catalog = CatalogService.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store, &builder, &fs_catalog_store);
    defer catalog.deinit();

    try std.testing.expect(try catalog.ensureTableWithDefinition(
        "docs",
        100,
        .{},
        "{\"default_type\":\"doc\"}",
        "",
        tables_api.default_indexes_json,
    ));

    var api = @import("../api/service.zig").Service.init(alloc, &wal_store, &builder);
    const mutations = [_]api_types.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "{\"text\":\"alpha\",\"chunk_preview\":[\"alpha\"],\"_enrichment\":{\"chunk_preview\":true,\"chunk_preview_version\":1}}" },
    };
    var ingest = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 123, .mutations = &mutations });
    defer ingest.deinit(alloc);

    var first_build = try catalog.buildTable("docs");
    defer first_build.deinit(alloc);
    try std.testing.expect(first_build.published);

    var before = try catalog.buildStatus("docs");
    defer before.deinit(alloc);
    try std.testing.expect(before.materialized_derived_outputs.containsKind(.chunk_preview));
    try std.testing.expect(!before.publish_recommended);

    _ = try catalog.setPolicy("docs", .{ .chunk_preview_enabled = true });

    var status = try catalog.buildStatus("docs");
    defer status.deinit(alloc);
    try std.testing.expect(status.publish_recommended);
    try std.testing.expectEqual(catalog_types.NextPublishReason.head_republish, status.next_publish_reason.?);
    try std.testing.expect(status.head_republish_recommended);
    try std.testing.expect(!status.pending_materialization_rebuild);
    try std.testing.expectEqual(catalog_types.DerivedOutputPublicationAction.reuse, status.derived_output_actions.chunk_preview);
    try std.testing.expectEqual(catalog_types.DerivedOutputResolution.head_republish_reuse, status.derived_output_resolutions.chunk_preview);
    try std.testing.expect(!status.pending_materialization_families.chunk_preview);
}

test "catalog service republishes head when rerank terms are already materialized" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    var catalog_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-rerank-republish");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-rerank-republish");
    const wal_root = tmpPath(&wal_root_buf, "wal-rerank-republish");
    const catalog_root = tmpPath(&catalog_root_buf, "catalog-rerank-republish");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);
    defer cleanupTmp(catalog_root);

    var fs_artifacts = try @import("../artifacts/mod.zig").FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try @import("fs_progress_store.zig").FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try wal_mod.FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_catalog = try @import("fs_store.zig").FsStore.init(alloc, std.mem.span(catalog_root));
    var fs_catalog_store = fs_catalog.catalogStore();
    defer fs_catalog_store.deinit();

    var builder = builder_mod.Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var catalog = CatalogService.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store, &builder, &fs_catalog_store);
    defer catalog.deinit();

    try std.testing.expect(try catalog.ensureTableWithDefinition(
        "docs",
        100,
        .{},
        "{\"default_type\":\"doc\"}",
        "",
        tables_api.default_indexes_json,
    ));

    var api = @import("../api/service.zig").Service.init(alloc, &wal_store, &builder);
    const mutations = [_]api_types.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "{\"text\":\"alpha bravo\",\"rerank_terms\":[\"alpha\",\"bravo\"],\"_enrichment\":{\"rerank_terms\":true,\"rerank_terms_version\":1}}" },
    };
    var ingest = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 123, .mutations = &mutations });
    defer ingest.deinit(alloc);

    var first_build = try catalog.buildTable("docs");
    defer first_build.deinit(alloc);
    try std.testing.expect(first_build.published);

    var before = try catalog.buildStatus("docs");
    defer before.deinit(alloc);
    try std.testing.expect(before.materialized_derived_outputs.containsKind(.rerank_terms));
    try std.testing.expect(!before.publish_recommended);

    _ = try catalog.setPolicy("docs", .{ .rerank_terms_enabled = true });

    var status = try catalog.buildStatus("docs");
    defer status.deinit(alloc);
    try std.testing.expect(status.publish_recommended);
    try std.testing.expectEqual(catalog_types.NextPublishReason.head_republish, status.next_publish_reason.?);
    try std.testing.expect(status.head_republish_recommended);
    try std.testing.expect(!status.pending_materialization_rebuild);
    try std.testing.expectEqual(catalog_types.DerivedOutputPublicationAction.reuse, status.derived_output_actions.rerank_terms);
    try std.testing.expectEqual(catalog_types.DerivedOutputResolution.head_republish_reuse, status.derived_output_resolutions.rerank_terms);
    try std.testing.expect(!status.pending_materialization_families.rerank_terms);
}

test "catalog service reports named vector and sparse publication actions" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    var catalog_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-named-embedding-actions");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-named-embedding-actions");
    const wal_root = tmpPath(&wal_root_buf, "wal-named-embedding-actions");
    const catalog_root = tmpPath(&catalog_root_buf, "catalog-named-embedding-actions");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);
    defer cleanupTmp(catalog_root);

    var fs_artifacts = try @import("../artifacts/mod.zig").FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try @import("fs_progress_store.zig").FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try wal_mod.FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_catalog = try @import("fs_store.zig").FsStore.init(alloc, std.mem.span(catalog_root));
    var fs_catalog_store = fs_catalog.catalogStore();
    defer fs_catalog_store.deinit();

    var builder = builder_mod.Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var catalog = CatalogService.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store, &builder, &fs_catalog_store);
    defer catalog.deinit();

    try std.testing.expect(try catalog.ensureTableWithDefinition(
        "docs",
        100,
        .{},
        "{\"default_type\":\"doc\"}",
        "",
        "{\"semantic_a\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":3},\"sparse_a\":{\"type\":\"embeddings\",\"external\":true,\"sparse\":true}}",
    ));

    var api = @import("../api/service.zig").Service.init(alloc, &wal_store, &builder);
    const mutations = [_]api_types.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "{\"text\":\"alpha\",\"embedding\":[1,0,0],\"sparse_embedding\":{\"11\":1.5}}" },
    };
    var ingest = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 123, .mutations = &mutations });
    defer ingest.deinit(alloc);

    var first_build = try catalog.buildTable("docs");
    defer first_build.deinit(alloc);
    try std.testing.expect(first_build.published);

    try std.testing.expect(try catalog.setTableDefinition(
        "docs",
        "{\"default_type\":\"doc\"}",
        "",
        "{\"semantic_b\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":3},\"sparse_a\":{\"type\":\"embeddings\",\"external\":true,\"sparse\":true},\"sparse_b\":{\"type\":\"embeddings\",\"external\":true,\"sparse\":true}}",
    ));

    var status = try catalog.buildStatus("docs");
    defer status.deinit(alloc);
    try std.testing.expectEqual(catalog_types.ArtifactPublicationAction.reuse, status.artifact_actions.dense_vector);
    try std.testing.expectEqual(catalog_types.ArtifactPublicationAction.rebuild, status.artifact_actions.sparse_vector);
    try std.testing.expectEqual(catalog_types.ArtifactPublicationAction.drop, findNamedArtifactAction(status.vector_index_actions, "semantic_a").?);
    try std.testing.expectEqual(catalog_types.ArtifactPublicationAction.reuse, findNamedArtifactAction(status.vector_index_actions, "semantic_b").?);
    try std.testing.expectEqual(catalog_types.ArtifactPublicationAction.reuse, findNamedArtifactAction(status.sparse_index_actions, "sparse_a").?);
    try std.testing.expectEqual(catalog_types.ArtifactPublicationAction.rebuild, findNamedArtifactAction(status.sparse_index_actions, "sparse_b").?);
}

test "catalog service defers small publish tails while enrichment is still in progress" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    var catalog_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-enrichment-defer");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-enrichment-defer");
    const wal_root = tmpPath(&wal_root_buf, "wal-enrichment-defer");
    const catalog_root = tmpPath(&catalog_root_buf, "catalog-enrichment-defer");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);
    defer cleanupTmp(catalog_root);

    var fs_artifacts = try @import("../artifacts/mod.zig").FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try @import("fs_progress_store.zig").FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try wal_mod.FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_catalog = try @import("fs_store.zig").FsStore.init(alloc, std.mem.span(catalog_root));
    var fs_catalog_store = fs_catalog.catalogStore();
    defer fs_catalog_store.deinit();

    var builder = builder_mod.Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var catalog = CatalogService.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store, &builder, &fs_catalog_store);
    defer catalog.deinit();
    try std.testing.expect(try catalog.ensureNamespaceWithPolicy("docs", 100, .{
        .enrichment_enabled = true,
        .enrichment_batch_size = 8,
        .enrichment_publish_min_pending_records = 4,
    }));

    var api = @import("../api/service.zig").Service.init(alloc, &wal_store, &builder);
    const first = [_]api_types.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "{\"text\":\"alpha bravo\"}" },
    };
    var ingest_first = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 100, .mutations = &first });
    defer ingest_first.deinit(alloc);
    var build_first = try builder.publishNamespace("docs");
    defer build_first.deinit(alloc);

    try std.testing.expect(try progress_store.compareAndSwapEnrichmentHeadVersion("docs", null, 1));
    try std.testing.expect(try progress_store.compareAndSwapEnrichmentDocOffset("docs", null, 0));

    const derived = [_]api_types.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "{\"text\":\"alpha bravo\",\"sparse_embedding\":{\"alpha\":0.5,\"bravo\":0.5},\"_enrichment\":{\"lexical_sparse\":true,\"lexical_sparse_version\":1}}" },
    };
    var ingest_derived = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 101, .mutations = &derived });
    defer ingest_derived.deinit(alloc);

    var status = try catalog.buildStatus("docs");
    defer status.deinit(alloc);
    try std.testing.expect(status.enrichment_enabled);
    try std.testing.expect(status.enrichment_in_progress);
    try std.testing.expectEqual(catalog_types.EnrichmentStageSource.current_head, status.enrichment_stage_source.?);
    try std.testing.expectEqual(catalog_types.EnrichmentStageState.executing, status.enrichment_stage_state.?);
    try std.testing.expectEqual(@as(u64, 1), status.pending_records);
    try std.testing.expectEqualStrings(search_sources.default_chunk_embedding_index_name, status.published_search_sources.findVector().?.index_name);
    try std.testing.expectEqual(search_sources.VectorDocumentSource.chunk_embeddings_or_top_level, status.published_search_sources.findVector().?.document_source);
    try std.testing.expectEqualStrings(search_sources.default_sparse_embedding_index_name, status.published_search_sources.findSparse().?.index_name);
    try std.testing.expectEqual(search_sources.SparseDocumentSource.sparse_embedding, status.published_search_sources.findSparse().?.document_source);
    try std.testing.expect(status.materialized_search_sources.findVector() == null);
    try std.testing.expect(status.materialized_search_sources.findSparse() == null);
    try std.testing.expect(!status.materialized_derived_outputs.containsKind(.chunk_preview));
    try std.testing.expect(!status.materialized_derived_outputs.containsKind(.rerank_terms));
    try std.testing.expect(!status.publish_recommended);
    try std.testing.expectEqual(@as(?u64, 1), status.enrichment_head_version);
    try std.testing.expectEqual(@as(u64, 0), status.enrichment_doc_offset);
}

test "catalog service advances active enrichment stage to rerank terms" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    var catalog_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-rerank-status");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-rerank-status");
    const wal_root = tmpPath(&wal_root_buf, "wal-rerank-status");
    const catalog_root = tmpPath(&catalog_root_buf, "catalog-rerank-status");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);
    defer cleanupTmp(catalog_root);

    var fs_artifacts = try @import("../artifacts/mod.zig").FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try @import("fs_progress_store.zig").FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try wal_mod.FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_catalog = try @import("fs_store.zig").FsStore.init(alloc, std.mem.span(catalog_root));
    var fs_catalog_store = fs_catalog.catalogStore();
    defer fs_catalog_store.deinit();

    var builder = builder_mod.Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var catalog = CatalogService.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store, &builder, &fs_catalog_store);
    defer catalog.deinit();

    try std.testing.expect(try catalog.ensureNamespaceWithPolicy("docs", 100, .{
        .enrichment_enabled = true,
        .chunk_preview_enabled = true,
        .rerank_terms_enabled = true,
    }));

    var api = @import("../api/service.zig").Service.init(alloc, &wal_store, &builder);
    const batch = [_]api_types.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "{\"text\":\"alpha bravo charlie\",\"sparse_embedding\":{\"alpha\":0.7,\"bravo\":0.3},\"chunk_preview\":[\"alpha bravo\",\"charlie\"],\"_enrichment\":{\"lexical_sparse\":true,\"lexical_sparse_version\":1,\"chunk_preview\":true,\"chunk_preview_version\":1}}" },
    };
    var ingest = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 100, .mutations = &batch });
    defer ingest.deinit(alloc);
    var build = try builder.publishNamespace("docs");
    defer build.deinit(alloc);

    var status = try catalog.buildStatus("docs");
    defer status.deinit(alloc);
    try std.testing.expect(status.enrichment_enabled);
    try std.testing.expect(status.lexical_sparse_complete);
    try std.testing.expect(status.chunk_preview_complete);
    try std.testing.expect(status.rerank_terms_enabled);
    try std.testing.expect(!status.rerank_terms_complete);
    try std.testing.expectEqual(catalog_types.EnrichmentStageState.awaiting_execution, status.enrichment_stage_state.?);
    try std.testing.expectEqualStrings(search_sources.default_chunk_preview_output_name, status.materialized_derived_outputs.findByKind(.chunk_preview).?.name);
    try std.testing.expect(!status.materialized_derived_outputs.containsKind(.rerank_terms));
    try std.testing.expectEqual(catalog_types.EnrichmentStage.rerank_terms, status.enrichment_active_stage.?);
}

test "catalog service uses stage-specific publish thresholds for later enrichment stages" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    var catalog_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-rerank-threshold");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-rerank-threshold");
    const wal_root = tmpPath(&wal_root_buf, "wal-rerank-threshold");
    const catalog_root = tmpPath(&catalog_root_buf, "catalog-rerank-threshold");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);
    defer cleanupTmp(catalog_root);

    var fs_artifacts = try @import("../artifacts/mod.zig").FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try @import("fs_progress_store.zig").FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try wal_mod.FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_catalog = try @import("fs_store.zig").FsStore.init(alloc, std.mem.span(catalog_root));
    var fs_catalog_store = fs_catalog.catalogStore();
    defer fs_catalog_store.deinit();

    var builder = builder_mod.Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var catalog = CatalogService.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store, &builder, &fs_catalog_store);
    defer catalog.deinit();
    try std.testing.expect(try catalog.ensureNamespaceWithPolicy("docs", 100, .{
        .enrichment_enabled = true,
        .chunk_preview_enabled = true,
        .chunk_embeddings_enabled = true,
        .rerank_terms_enabled = true,
        .enrichment_publish_min_pending_records = 2,
        .chunk_preview_publish_min_pending_records = 4,
        .chunk_embeddings_publish_min_pending_records = 6,
        .rerank_terms_publish_min_pending_records = 8,
    }));

    var api = @import("../api/service.zig").Service.init(alloc, &wal_store, &builder);
    const batch = [_]api_types.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "{\"text\":\"alpha bravo charlie\",\"sparse_embedding\":{\"alpha\":0.7,\"bravo\":0.3},\"chunk_preview\":[\"alpha bravo\",\"charlie\"],\"chunk_embeddings\":[{\"chunk\":\"alpha bravo\",\"embedding\":[1,0]},{\"chunk\":\"charlie\",\"embedding\":[0,1]}],\"_enrichment\":{\"lexical_sparse\":true,\"lexical_sparse_version\":1,\"chunk_preview\":true,\"chunk_preview_version\":1,\"chunk_embeddings\":true,\"chunk_embeddings_version\":1}}" },
    };
    var ingest = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 100, .mutations = &batch });
    defer ingest.deinit(alloc);
    var build = try builder.publishNamespace("docs");
    defer build.deinit(alloc);

    const derived = [_]api_types.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "{\"text\":\"alpha bravo charlie\",\"sparse_embedding\":{\"alpha\":0.7,\"bravo\":0.3},\"chunk_preview\":[\"alpha bravo\",\"charlie\"],\"chunk_embeddings\":[{\"chunk\":\"alpha bravo\",\"embedding\":[1,0]},{\"chunk\":\"charlie\",\"embedding\":[0,1]}],\"rerank_terms\":[\"alpha\",\"bravo\"],\"_enrichment\":{\"lexical_sparse\":true,\"lexical_sparse_version\":1,\"chunk_preview\":true,\"chunk_preview_version\":1,\"chunk_embeddings\":true,\"chunk_embeddings_version\":1,\"rerank_terms\":true,\"rerank_terms_version\":1}}" },
    };
    var ingest_derived = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 101, .mutations = &derived });
    defer ingest_derived.deinit(alloc);

    var status = try catalog.buildStatus("docs");
    defer status.deinit(alloc);
    try std.testing.expectEqual(catalog_types.EnrichmentStage.rerank_terms, status.enrichment_active_stage.?);
    try std.testing.expectEqual(@as(u64, 8), status.enrichment_publish_min_pending_records);
    try std.testing.expectEqual(@as(u64, 1), status.pending_records);
    try std.testing.expectEqualStrings(search_sources.default_chunk_preview_output_name, status.materialized_derived_outputs.findByKind(.chunk_preview).?.name);
    try std.testing.expectEqualStrings(search_sources.default_chunk_embeddings_output_name, status.materialized_derived_outputs.findByKind(.chunk_embeddings).?.name);
    try std.testing.expect(!status.materialized_derived_outputs.containsKind(.rerank_terms));
}

test "catalog service recommends compaction only while head still contains mutation segments" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    var catalog_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-compact-status");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-compact-status");
    const wal_root = tmpPath(&wal_root_buf, "wal-compact-status");
    const catalog_root = tmpPath(&catalog_root_buf, "catalog-compact-status");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);
    defer cleanupTmp(catalog_root);

    var fs_artifacts = try @import("../artifacts/mod.zig").FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try @import("fs_progress_store.zig").FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try wal_mod.FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_catalog = try @import("fs_store.zig").FsStore.init(alloc, std.mem.span(catalog_root));
    var fs_catalog_store = fs_catalog.catalogStore();
    defer fs_catalog_store.deinit();

    var builder = builder_mod.Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var catalog = CatalogService.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store, &builder, &fs_catalog_store);
    defer catalog.deinit();
    try std.testing.expect(try catalog.ensureNamespaceWithPolicy("docs", 100, .{
        .compaction_trigger_version_count = 2,
    }));

    var api = @import("../api/service.zig").Service.init(alloc, &wal_store, &builder);
    const first = [_]api_types.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "alpha" },
    };
    var ingest_first = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 100, .mutations = &first });
    defer ingest_first.deinit(alloc);
    var build_first = try builder.publishNamespace("docs");
    defer build_first.deinit(alloc);

    const second = [_]api_types.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-b", .body = "bravo" },
    };
    var ingest_second = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 200, .mutations = &second });
    defer ingest_second.deinit(alloc);
    var build_second = try builder.publishNamespace("docs");
    defer build_second.deinit(alloc);

    var before = try catalog.buildStatus("docs");
    defer before.deinit(alloc);
    try std.testing.expect(before.compaction_recommended);
    try std.testing.expectEqual(catalog_types.MutationTailResolution.background_compaction, before.mutation_tail_resolution);

    var compactor = @import("../build/compactor.zig").Compactor.init(alloc, &artifact_store, &manifest_store, &progress_store);
    var compacted = try compactor.compactHead("docs");
    defer compacted.deinit(alloc);
    try std.testing.expect(compacted.published);

    var after = try catalog.buildStatus("docs");
    defer after.deinit(alloc);
    try std.testing.expect(!after.compaction_recommended);
}

test "catalog service recommends compaction based on document base lineage after pruning" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    var catalog_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-compact-lineage");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-compact-lineage");
    const wal_root = tmpPath(&wal_root_buf, "wal-compact-lineage");
    const catalog_root = tmpPath(&catalog_root_buf, "catalog-compact-lineage");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);
    defer cleanupTmp(catalog_root);

    var fs_artifacts = try @import("../artifacts/mod.zig").FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try @import("fs_progress_store.zig").FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try wal_mod.FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_catalog = try @import("fs_store.zig").FsStore.init(alloc, std.mem.span(catalog_root));
    var fs_catalog_store = fs_catalog.catalogStore();
    defer fs_catalog_store.deinit();

    var builder = builder_mod.Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var catalog = CatalogService.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store, &builder, &fs_catalog_store);
    defer catalog.deinit();
    try std.testing.expect(try catalog.ensureNamespaceWithPolicy("docs", 100, .{
        .keep_latest_versions = 2,
        .compaction_trigger_version_count = 3,
    }));

    var api = @import("../api/service.zig").Service.init(alloc, &wal_store, &builder);
    const batches = [_][]const api_types.DocumentMutation{
        &.{.{ .kind = .upsert, .doc_id = "doc-a", .body = "alpha" }},
        &.{.{ .kind = .upsert, .doc_id = "doc-b", .body = "bravo" }},
        &.{.{ .kind = .upsert, .doc_id = "doc-c", .body = "charlie" }},
    };
    for (batches, 0..) |batch, idx| {
        var ingest = try api.ingestBatch(.{
            .namespace = "docs",
            .timestamp_ns = 100 + @as(u64, @intCast(idx)) * 100,
            .mutations = batch,
        });
        defer ingest.deinit(alloc);
        var build = try builder.publishNamespace("docs");
        defer build.deinit(alloc);
    }

    var pruner = @import("../build/retention.zig").Pruner.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var result = try pruner.pruneNamespace("docs", 2);
    defer result.deinit(alloc);

    const versions = try manifest_store.listVersionsAlloc("docs");
    defer alloc.free(versions);
    try std.testing.expectEqualSlices(u64, &.{ 2, 3 }, versions);

    var status = try catalog.buildStatus("docs");
    defer status.deinit(alloc);
    try std.testing.expect(status.compaction_recommended);
    try std.testing.expectEqual(@as(u64, 1), status.document_base_version);
    try std.testing.expectEqual(@as(u64, 3), status.document_lineage_versions);
    try std.testing.expectEqual(catalog_types.MutationTailResolution.background_compaction, status.mutation_tail_resolution);
    try std.testing.expectEqual(catalog_types.DocumentPublishMode.append_mutation_tail, status.head_document_publish_mode.?);
    try std.testing.expect(status.next_document_publish_mode == null);
}

test "catalog service reports mutation tail resolved by next inline rebase publish" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    var catalog_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-inline-rebase-status");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-inline-rebase-status");
    const wal_root = tmpPath(&wal_root_buf, "wal-inline-rebase-status");
    const catalog_root = tmpPath(&catalog_root_buf, "catalog-inline-rebase-status");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);
    defer cleanupTmp(catalog_root);

    var fs_artifacts = try @import("../artifacts/mod.zig").FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try @import("fs_progress_store.zig").FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try wal_mod.FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_catalog = try @import("fs_store.zig").FsStore.init(alloc, std.mem.span(catalog_root));
    var fs_catalog_store = fs_catalog.catalogStore();
    defer fs_catalog_store.deinit();

    var builder = builder_mod.Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var catalog = CatalogService.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store, &builder, &fs_catalog_store);
    defer catalog.deinit();
    try std.testing.expect(try catalog.ensureNamespaceWithPolicy("docs", 100, .{
        .compaction_enabled = true,
        .compaction_trigger_version_count = 2,
    }));

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

    const third = [_]api_types.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-c", .body = "charlie" },
    };
    var ingest_third = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 300, .mutations = &third });
    defer ingest_third.deinit(alloc);

    var status = try catalog.buildStatus("docs");
    defer status.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 1), status.pending_records);
    try std.testing.expectEqual(catalog_types.DocumentPublishMode.append_mutation_tail, status.head_document_publish_mode.?);
    try std.testing.expectEqual(catalog_types.DocumentPublishMode.inline_rebase, status.next_document_publish_mode.?);
    try std.testing.expectEqual(catalog_types.MutationTailResolution.next_publish_inline_rebase, status.mutation_tail_resolution);
}

test "catalog service reports versioned full text migration actions" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    var catalog_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-ft-migration-actions");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-ft-migration-actions");
    const wal_root = tmpPath(&wal_root_buf, "wal-ft-migration-actions");
    const catalog_root = tmpPath(&catalog_root_buf, "catalog-ft-migration-actions");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);
    defer cleanupTmp(catalog_root);

    var fs_artifacts = try @import("../artifacts/mod.zig").FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try @import("fs_progress_store.zig").FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try wal_mod.FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_catalog = try @import("fs_store.zig").FsStore.init(alloc, std.mem.span(catalog_root));
    var fs_catalog_store = fs_catalog.catalogStore();
    defer fs_catalog_store.deinit();

    var builder = builder_mod.Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var catalog = CatalogService.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store, &builder, &fs_catalog_store);
    defer catalog.deinit();

    try std.testing.expect(try catalog.ensureTableWithDefinition(
        "docs",
        100,
        .{},
        "{\"version\":0}",
        "",
        "{\"full_text_index_v0\":{\"type\":\"full_text\"}}",
    ));

    var api = @import("../api/service.zig").Service.init(alloc, &wal_store, &builder);
    const batch = [_]api_types.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "{\"title\":\"alpha\"}" },
    };
    var ingest = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 100, .mutations = &batch });
    defer ingest.deinit(alloc);
    var build = try catalog.buildTable("docs");
    defer build.deinit(alloc);

    try std.testing.expect(try catalog.setTableDefinition(
        "docs",
        "{\"version\":1}",
        "{\"version\":0}",
        "{\"full_text_index_v0\":{\"type\":\"full_text\"},\"full_text_index_v1\":{\"type\":\"full_text\"}}",
    ));

    var status = try catalog.tableBuildStatus("docs");
    defer status.deinit(alloc);
    try std.testing.expectEqual(catalog_types.ArtifactPublicationAction.rebuild, status.artifact_actions.full_text);
    try std.testing.expectEqual(catalog_types.ArtifactPublicationAction.reuse, findFullTextIndexAction(status.full_text_index_actions, "full_text_index_v0").?);
    try std.testing.expectEqual(catalog_types.ArtifactPublicationAction.rebuild, findFullTextIndexAction(status.full_text_index_actions, "full_text_index_v1").?);
}

test "catalog service reports versioned full text cutover drop actions" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    var catalog_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-ft-cutover-actions");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-ft-cutover-actions");
    const wal_root = tmpPath(&wal_root_buf, "wal-ft-cutover-actions");
    const catalog_root = tmpPath(&catalog_root_buf, "catalog-ft-cutover-actions");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);
    defer cleanupTmp(catalog_root);

    var fs_artifacts = try @import("../artifacts/mod.zig").FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try @import("fs_progress_store.zig").FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try wal_mod.FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_catalog = try @import("fs_store.zig").FsStore.init(alloc, std.mem.span(catalog_root));
    var fs_catalog_store = fs_catalog.catalogStore();
    defer fs_catalog_store.deinit();

    var builder = builder_mod.Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var catalog = CatalogService.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store, &builder, &fs_catalog_store);
    defer catalog.deinit();

    try std.testing.expect(try catalog.ensureTableWithDefinition(
        "docs",
        100,
        .{},
        "{\"version\":1}",
        "{\"version\":0}",
        "{\"full_text_index_v0\":{\"type\":\"full_text\"},\"full_text_index_v1\":{\"type\":\"full_text\"}}",
    ));

    var api = @import("../api/service.zig").Service.init(alloc, &wal_store, &builder);
    const batch = [_]api_types.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "{\"title\":\"alpha\"}" },
    };
    var ingest = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 100, .mutations = &batch });
    defer ingest.deinit(alloc);
    var build = try catalog.buildTable("docs");
    defer build.deinit(alloc);

    try std.testing.expect(try catalog.setTableDefinition(
        "docs",
        "{\"version\":1}",
        "",
        "{\"full_text_index_v1\":{\"type\":\"full_text\"}}",
    ));

    var status = try catalog.tableBuildStatus("docs");
    defer status.deinit(alloc);
    try std.testing.expectEqual(catalog_types.ArtifactPublicationAction.reuse, status.artifact_actions.full_text);
    try std.testing.expectEqual(catalog_types.ArtifactPublicationAction.drop, findFullTextIndexAction(status.full_text_index_actions, "full_text_index_v0").?);
    try std.testing.expectEqual(catalog_types.ArtifactPublicationAction.reuse, findFullTextIndexAction(status.full_text_index_actions, "full_text_index_v1").?);
}

test "catalog service reports head publication actions for wal partial reuse" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    var catalog_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-head-publication-actions");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-head-publication-actions");
    const wal_root = tmpPath(&wal_root_buf, "wal-head-publication-actions");
    const catalog_root = tmpPath(&catalog_root_buf, "catalog-head-publication-actions");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);
    defer cleanupTmp(catalog_root);

    var fs_artifacts = try @import("../artifacts/mod.zig").FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try @import("fs_progress_store.zig").FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try wal_mod.FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_catalog = try @import("fs_store.zig").FsStore.init(alloc, std.mem.span(catalog_root));
    var fs_catalog_store = fs_catalog.catalogStore();
    defer fs_catalog_store.deinit();

    var builder = builder_mod.Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var catalog = CatalogService.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store, &builder, &fs_catalog_store);
    defer catalog.deinit();

    try std.testing.expect(try catalog.ensureTableWithDefinition(
        "docs",
        100,
        .{},
        "{\"default_type\":\"doc\"}",
        "",
        "{\"full_text_index_v0\":{\"type\":\"full_text\"},\"semantic_idx\":{\"type\":\"embeddings\",\"dimension\":3},\"sparse_idx\":{\"type\":\"embeddings\",\"sparse\":true}}",
    ));

    var api = @import("../api/service.zig").Service.init(alloc, &wal_store, &builder);
    const first = [_]api_types.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "{\"text\":\"alpha\",\"embedding\":[1,0,0],\"sparse_embedding\":{\"alpha\":1.0}}" },
    };
    var ingest_first = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 100, .mutations = &first });
    defer ingest_first.deinit(alloc);
    var build_first = try catalog.buildTable("docs");
    defer build_first.deinit(alloc);

    const second = [_]api_types.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "{\"text\":\"bravo\",\"embedding\":[1,0,0],\"sparse_embedding\":{\"alpha\":1.0}}" },
    };
    var ingest_second = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 200, .mutations = &second });
    defer ingest_second.deinit(alloc);
    var build_second = try catalog.buildTable("docs");
    defer build_second.deinit(alloc);

    var status = try catalog.tableBuildStatus("docs");
    defer status.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 2), status.head_version);
    try std.testing.expectEqual(catalog_types.ArtifactPublicationAction.rebuild, status.head_artifact_actions.document_segment);
    try std.testing.expectEqual(catalog_types.ArtifactPublicationAction.rebuild, status.head_artifact_actions.full_text);
    try std.testing.expectEqual(catalog_types.ArtifactPublicationAction.reuse, status.head_artifact_actions.dense_vector);
    try std.testing.expectEqual(catalog_types.ArtifactPublicationAction.reuse, status.head_artifact_actions.sparse_vector);
    try std.testing.expectEqual(catalog_types.ArtifactPublicationAction.rebuild, findFullTextIndexAction(status.head_full_text_index_actions, "full_text_index_v0").?);
    try std.testing.expectEqual(catalog_types.ArtifactPublicationAction.reuse, findNamedArtifactAction(status.head_vector_index_actions, "semantic_idx").?);
    try std.testing.expectEqual(catalog_types.ArtifactPublicationAction.reuse, findNamedArtifactAction(status.head_sparse_index_actions, "sparse_idx").?);
}

test "catalog service predicts wal partial reuse before publish" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    var catalog_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-predict-wal-reuse");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-predict-wal-reuse");
    const wal_root = tmpPath(&wal_root_buf, "wal-predict-wal-reuse");
    const catalog_root = tmpPath(&catalog_root_buf, "catalog-predict-wal-reuse");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);
    defer cleanupTmp(catalog_root);

    var fs_artifacts = try @import("../artifacts/mod.zig").FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try @import("fs_progress_store.zig").FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try wal_mod.FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_catalog = try @import("fs_store.zig").FsStore.init(alloc, std.mem.span(catalog_root));
    var fs_catalog_store = fs_catalog.catalogStore();
    defer fs_catalog_store.deinit();

    var builder = builder_mod.Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var catalog = CatalogService.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store, &builder, &fs_catalog_store);
    defer catalog.deinit();

    try std.testing.expect(try catalog.ensureTableWithDefinition(
        "docs",
        100,
        .{},
        "{\"default_type\":\"doc\"}",
        "",
        "{\"full_text_index_v0\":{\"type\":\"full_text\"},\"semantic_idx\":{\"type\":\"embeddings\",\"dimension\":3},\"sparse_idx\":{\"type\":\"embeddings\",\"sparse\":true}}",
    ));

    var api = @import("../api/service.zig").Service.init(alloc, &wal_store, &builder);
    const first = [_]api_types.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "{\"text\":\"alpha\",\"embedding\":[1,0,0],\"sparse_embedding\":{\"alpha\":1.0}}" },
    };
    var ingest_first = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 100, .mutations = &first });
    defer ingest_first.deinit(alloc);
    var build_first = try catalog.buildTable("docs");
    defer build_first.deinit(alloc);

    const second = [_]api_types.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "{\"text\":\"bravo\",\"embedding\":[1,0,0],\"sparse_embedding\":{\"alpha\":1.0}}" },
    };
    var ingest_second = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 200, .mutations = &second });
    defer ingest_second.deinit(alloc);

    var status = try catalog.tableBuildStatus("docs");
    defer status.deinit(alloc);
    try std.testing.expect(status.publish_recommended);
    try std.testing.expectEqual(catalog_types.NextPublishReason.wal_artifact_update, status.next_publish_reason.?);
    try std.testing.expect(!status.head_republish_recommended);
    try std.testing.expectEqual(catalog_types.ArtifactPublicationAction.reuse, status.artifact_actions.document_segment);
    try std.testing.expectEqual(catalog_types.ArtifactPublicationAction.rebuild, status.artifact_actions.full_text);
    try std.testing.expectEqual(catalog_types.ArtifactPublicationAction.reuse, status.artifact_actions.dense_vector);
    try std.testing.expectEqual(catalog_types.ArtifactPublicationAction.reuse, status.artifact_actions.sparse_vector);
    try std.testing.expectEqual(catalog_types.ArtifactPublicationAction.rebuild, findFullTextIndexAction(status.full_text_index_actions, "full_text_index_v0").?);
    try std.testing.expectEqual(catalog_types.ArtifactPublicationAction.reuse, findNamedArtifactAction(status.vector_index_actions, "semantic_idx").?);
    try std.testing.expectEqual(catalog_types.ArtifactPublicationAction.reuse, findNamedArtifactAction(status.sparse_index_actions, "sparse_idx").?);
}

test "catalog service predicts graph index reuse before publish when graph projection is unchanged" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    var catalog_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-predict-graph-reuse");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-predict-graph-reuse");
    const wal_root = tmpPath(&wal_root_buf, "wal-predict-graph-reuse");
    const catalog_root = tmpPath(&catalog_root_buf, "catalog-predict-graph-reuse");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);
    defer cleanupTmp(catalog_root);

    var fs_artifacts = try @import("../artifacts/mod.zig").FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try @import("fs_progress_store.zig").FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try wal_mod.FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_catalog = try @import("fs_store.zig").FsStore.init(alloc, std.mem.span(catalog_root));
    var fs_catalog_store = fs_catalog.catalogStore();
    defer fs_catalog_store.deinit();

    var builder = builder_mod.Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var catalog = CatalogService.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store, &builder, &fs_catalog_store);
    defer catalog.deinit();

    try std.testing.expect(try catalog.ensureTableWithDefinition(
        "docs",
        100,
        .{},
        "{\"default_type\":\"doc\"}",
        "",
        "{\"graph_idx\":{\"type\":\"graph\"}}",
    ));

    var api = @import("../api/service.zig").Service.init(alloc, &wal_store, &builder);
    const first = [_]api_types.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "{\"text\":\"alpha\",\"graph_edges\":[{\"edge_type\":\"related\",\"target\":\"doc-b\"}]}" },
    };
    var ingest_first = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 100, .mutations = &first });
    defer ingest_first.deinit(alloc);
    var build_first = try catalog.buildTable("docs");
    defer build_first.deinit(alloc);

    const second = [_]api_types.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "{\"text\":\"bravo\",\"graph_edges\":[{\"edge_type\":\"related\",\"target\":\"doc-b\"}]}" },
    };
    var ingest_second = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 200, .mutations = &second });
    defer ingest_second.deinit(alloc);

    var status = try catalog.tableBuildStatus("docs");
    defer status.deinit(alloc);
    try std.testing.expect(status.publish_recommended);
    try std.testing.expectEqual(catalog_types.NextPublishReason.wal_artifact_update, status.next_publish_reason.?);
    try std.testing.expectEqual(catalog_types.ArtifactPublicationAction.reuse, status.artifact_actions.graph);
    try std.testing.expectEqual(catalog_types.ArtifactPublicationAction.reuse, findNamedArtifactAction(status.graph_index_actions, "graph_idx").?);
}

test "catalog service predicts graph index rebuild before publish when graph projection changes" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    var catalog_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-predict-graph-rebuild");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-predict-graph-rebuild");
    const wal_root = tmpPath(&wal_root_buf, "wal-predict-graph-rebuild");
    const catalog_root = tmpPath(&catalog_root_buf, "catalog-predict-graph-rebuild");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);
    defer cleanupTmp(catalog_root);

    var fs_artifacts = try @import("../artifacts/mod.zig").FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try @import("fs_progress_store.zig").FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try wal_mod.FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_catalog = try @import("fs_store.zig").FsStore.init(alloc, std.mem.span(catalog_root));
    var fs_catalog_store = fs_catalog.catalogStore();
    defer fs_catalog_store.deinit();

    var builder = builder_mod.Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var catalog = CatalogService.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store, &builder, &fs_catalog_store);
    defer catalog.deinit();

    try std.testing.expect(try catalog.ensureTableWithDefinition(
        "docs",
        100,
        .{},
        "{\"default_type\":\"doc\"}",
        "",
        "{\"graph_idx\":{\"type\":\"graph\"}}",
    ));

    var api = @import("../api/service.zig").Service.init(alloc, &wal_store, &builder);
    const first = [_]api_types.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "{\"text\":\"alpha\",\"graph_edges\":[{\"edge_type\":\"related\",\"target\":\"doc-b\"}]}" },
    };
    var ingest_first = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 100, .mutations = &first });
    defer ingest_first.deinit(alloc);
    var build_first = try catalog.buildTable("docs");
    defer build_first.deinit(alloc);

    const second = [_]api_types.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "{\"text\":\"alpha\",\"graph_edges\":[{\"edge_type\":\"related\",\"target\":\"doc-c\"}]}" },
    };
    var ingest_second = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 200, .mutations = &second });
    defer ingest_second.deinit(alloc);

    var status = try catalog.tableBuildStatus("docs");
    defer status.deinit(alloc);
    try std.testing.expect(status.publish_recommended);
    try std.testing.expectEqual(catalog_types.NextPublishReason.wal_artifact_update, status.next_publish_reason.?);
    try std.testing.expectEqual(catalog_types.ArtifactPublicationAction.rebuild, status.artifact_actions.graph);
    try std.testing.expectEqual(catalog_types.ArtifactPublicationAction.rebuild, findNamedArtifactAction(status.graph_index_actions, "graph_idx").?);
}

test "catalog service predicts derived output recomputes from pending wal when enrichment is enabled" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    var catalog_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-predict-derived-output");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-predict-derived-output");
    const wal_root = tmpPath(&wal_root_buf, "wal-predict-derived-output");
    const catalog_root = tmpPath(&catalog_root_buf, "catalog-predict-derived-output");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);
    defer cleanupTmp(catalog_root);

    var fs_artifacts = try @import("../artifacts/mod.zig").FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try @import("fs_progress_store.zig").FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try wal_mod.FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_catalog = try @import("fs_store.zig").FsStore.init(alloc, std.mem.span(catalog_root));
    var fs_catalog_store = fs_catalog.catalogStore();
    defer fs_catalog_store.deinit();

    var builder = builder_mod.Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var catalog = CatalogService.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store, &builder, &fs_catalog_store);
    defer catalog.deinit();

    try std.testing.expect(try catalog.ensureTableWithDefinition(
        "docs",
        100,
        .{
            .chunk_preview_enabled = true,
        },
        "",
        "",
        tables_api.default_indexes_json,
    ));

    var api = @import("../api/service.zig").Service.init(alloc, &wal_store, &builder);
    const first = [_]api_types.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "{\"text\":\"alpha bravo\",\"chunk_preview\":[\"alpha bravo\"],\"_enrichment\":{\"chunk_preview\":true,\"chunk_preview_version\":1}}" },
    };
    var ingest_first = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 100, .mutations = &first });
    defer ingest_first.deinit(alloc);
    var build_first = try catalog.buildTable("docs");
    defer build_first.deinit(alloc);

    const second = [_]api_types.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "{\"text\":\"charlie delta\"}" },
    };
    var ingest_second = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 200, .mutations = &second });
    defer ingest_second.deinit(alloc);

    var status = try catalog.buildStatus("docs");
    defer status.deinit(alloc);
    try std.testing.expectEqual(catalog_types.NextPublishReason.wal_enrichment, status.next_publish_reason.?);
    try std.testing.expectEqual(catalog_types.DerivedOutputPublicationAction.recompute, status.derived_output_actions.chunk_preview);
    try std.testing.expectEqual(catalog_types.EnrichmentStage.chunk_preview, status.enrichment_active_stage.?);
    try std.testing.expectEqual(catalog_types.EnrichmentStageSource.pending_wal, status.enrichment_stage_source.?);
    try std.testing.expectEqual(catalog_types.EnrichmentStageState.deferred_for_publish_threshold, status.enrichment_stage_state.?);
    try std.testing.expectEqual(@as(u64, 1), status.enrichment_pending_document_count);
    try std.testing.expectEqual(@as(u64, 32), status.enrichment_publish_min_pending_records);
    try std.testing.expect(!status.publish_recommended);
    try std.testing.expect(status.pending_materialization_families.chunk_preview);
    try std.testing.expect(!status.pending_materialization_families.sparse_vector);
}

test "catalog service predicts lexical sparse enrichment stage from pending wal" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    var catalog_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-predict-lexical-stage");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-predict-lexical-stage");
    const wal_root = tmpPath(&wal_root_buf, "wal-predict-lexical-stage");
    const catalog_root = tmpPath(&catalog_root_buf, "catalog-predict-lexical-stage");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);
    defer cleanupTmp(catalog_root);

    var fs_artifacts = try @import("../artifacts/mod.zig").FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try @import("fs_progress_store.zig").FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try wal_mod.FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_catalog = try @import("fs_store.zig").FsStore.init(alloc, std.mem.span(catalog_root));
    var fs_catalog_store = fs_catalog.catalogStore();
    defer fs_catalog_store.deinit();

    var builder = builder_mod.Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var catalog = CatalogService.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store, &builder, &fs_catalog_store);
    defer catalog.deinit();

    try std.testing.expect(try catalog.ensureTableWithDefinition(
        "docs",
        100,
        .{
            .enrichment_enabled = true,
        },
        "",
        "",
        "{\"full_text_index_v0\":{\"type\":\"full_text\"},\"sparse_idx\":{\"type\":\"embeddings\",\"sparse\":true}}",
    ));

    var api = @import("../api/service.zig").Service.init(alloc, &wal_store, &builder);
    const first = [_]api_types.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "{\"text\":\"alpha bravo\",\"sparse_embedding\":{\"alpha\":0.5,\"bravo\":0.5},\"_enrichment\":{\"lexical_sparse\":true,\"lexical_sparse_version\":1}}" },
    };
    var ingest_first = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 100, .mutations = &first });
    defer ingest_first.deinit(alloc);
    var build_first = try catalog.buildTable("docs");
    defer build_first.deinit(alloc);

    const second = [_]api_types.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "{\"text\":\"charlie delta\"}" },
    };
    var ingest_second = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 200, .mutations = &second });
    defer ingest_second.deinit(alloc);

    var status = try catalog.buildStatus("docs");
    defer status.deinit(alloc);
    try std.testing.expectEqual(catalog_types.NextPublishReason.wal_enrichment, status.next_publish_reason.?);
    try std.testing.expectEqual(catalog_types.EnrichmentStage.lexical_sparse, status.enrichment_active_stage.?);
    try std.testing.expectEqual(catalog_types.EnrichmentStageSource.pending_wal, status.enrichment_stage_source.?);
    try std.testing.expectEqual(catalog_types.EnrichmentStageState.deferred_for_publish_threshold, status.enrichment_stage_state.?);
    try std.testing.expectEqual(@as(u64, 1), status.enrichment_pending_document_count);
    try std.testing.expectEqual(@as(u64, 16), status.enrichment_publish_min_pending_records);
    try std.testing.expect(!status.publish_recommended);
    try std.testing.expect(status.pending_materialization_rebuild);
    try std.testing.expectEqual(catalog_types.ArtifactPublicationAction.rebuild, status.artifact_actions.sparse_vector);
    try std.testing.expect(status.pending_materialization_families.sparse_vector);
    try std.testing.expect(!status.pending_materialization_families.chunk_preview);
}

test "catalog service marks pending wal enrichment as ready to publish when threshold is met" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    var catalog_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-predict-ready-stage");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-predict-ready-stage");
    const wal_root = tmpPath(&wal_root_buf, "wal-predict-ready-stage");
    const catalog_root = tmpPath(&catalog_root_buf, "catalog-predict-ready-stage");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);
    defer cleanupTmp(catalog_root);

    var fs_artifacts = try @import("../artifacts/mod.zig").FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try @import("fs_progress_store.zig").FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try wal_mod.FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_catalog = try @import("fs_store.zig").FsStore.init(alloc, std.mem.span(catalog_root));
    var fs_catalog_store = fs_catalog.catalogStore();
    defer fs_catalog_store.deinit();

    var builder = builder_mod.Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var catalog = CatalogService.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store, &builder, &fs_catalog_store);
    defer catalog.deinit();

    try std.testing.expect(try catalog.ensureTableWithDefinition(
        "docs",
        100,
        .{
            .chunk_preview_enabled = true,
            .chunk_preview_publish_min_pending_records = 1,
        },
        "",
        "",
        tables_api.default_indexes_json,
    ));

    var api = @import("../api/service.zig").Service.init(alloc, &wal_store, &builder);
    const first = [_]api_types.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "{\"text\":\"alpha bravo\",\"chunk_preview\":[\"alpha bravo\"],\"_enrichment\":{\"chunk_preview\":true,\"chunk_preview_version\":1}}" },
    };
    var ingest_first = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 100, .mutations = &first });
    defer ingest_first.deinit(alloc);
    var build_first = try catalog.buildTable("docs");
    defer build_first.deinit(alloc);

    const second = [_]api_types.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "{\"text\":\"charlie delta\"}" },
    };
    var ingest_second = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 200, .mutations = &second });
    defer ingest_second.deinit(alloc);

    var status = try catalog.buildStatus("docs");
    defer status.deinit(alloc);
    try std.testing.expectEqual(catalog_types.NextPublishReason.wal_enrichment, status.next_publish_reason.?);
    try std.testing.expectEqual(catalog_types.EnrichmentStage.chunk_preview, status.enrichment_active_stage.?);
    try std.testing.expectEqual(catalog_types.EnrichmentStageSource.pending_wal, status.enrichment_stage_source.?);
    try std.testing.expectEqual(catalog_types.EnrichmentStageState.ready_for_publish, status.enrichment_stage_state.?);
    try std.testing.expect(status.publish_recommended);
    try std.testing.expect(status.pending_materialization_families.chunk_preview);
}

test "catalog service reports chunk-augmented full text status without chunk preview policy" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    var catalog_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-ft-chunk-routing");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-ft-chunk-routing");
    const wal_root = tmpPath(&wal_root_buf, "wal-ft-chunk-routing");
    const catalog_root = tmpPath(&catalog_root_buf, "catalog-ft-chunk-routing");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);
    defer cleanupTmp(catalog_root);

    var fs_artifacts = try @import("../artifacts/mod.zig").FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try @import("fs_progress_store.zig").FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try wal_mod.FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_catalog = try @import("fs_store.zig").FsStore.init(alloc, std.mem.span(catalog_root));
    var fs_catalog_store = fs_catalog.catalogStore();
    defer fs_catalog_store.deinit();

    var builder = builder_mod.Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var catalog = CatalogService.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store, &builder, &fs_catalog_store);
    defer catalog.deinit();

    try std.testing.expect(try catalog.ensureTableWithDefinition(
        "docs",
        100,
        .{},
        "{\"version\":0}",
        "",
        "{\"full_text_index_v0\":{\"type\":\"full_text\"},\"semantic_chunked_idx\":{\"field\":\"body\",\"dimension\":3,\"chunker\":{\"provider\":\"antfly\",\"store_chunks\":false,\"full_text_index\":{},\"text\":{\"target_tokens\":4}}}}",
    ));

    var status = try catalog.tableBuildStatus("docs");
    defer status.deinit(alloc);
    const full_text = findFullTextIndexEntry(status.full_text_index_actions, "full_text_index_v0").?;
    try std.testing.expectEqual(full_text_indexes.FullTextSourceMode.document_plus_artifact, full_text.source_mode);
    try std.testing.expectEqual(@as(usize, 1), full_text.chunked_source_count);
    try std.testing.expectEqual(false, status.chunk_preview_enabled);
}

test "catalog service marks chunk-backed full text as waiting on chunk preview materialization" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    var catalog_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-ft-chunk-blocked");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-ft-chunk-blocked");
    const wal_root = tmpPath(&wal_root_buf, "wal-ft-chunk-blocked");
    const catalog_root = tmpPath(&catalog_root_buf, "catalog-ft-chunk-blocked");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);
    defer cleanupTmp(catalog_root);

    var fs_artifacts = try @import("../artifacts/mod.zig").FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try @import("fs_progress_store.zig").FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try wal_mod.FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_catalog = try @import("fs_store.zig").FsStore.init(alloc, std.mem.span(catalog_root));
    var fs_catalog_store = fs_catalog.catalogStore();
    defer fs_catalog_store.deinit();

    var builder = builder_mod.Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var catalog = CatalogService.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store, &builder, &fs_catalog_store);
    defer catalog.deinit();

    try std.testing.expect(try catalog.ensureTableWithDefinition(
        "docs",
        100,
        .{
            .chunk_preview_enabled = true,
            .chunk_preview_publish_min_pending_records = 32,
        },
        "{\"version\":0}",
        "",
        "{\"full_text_index_v0\":{\"type\":\"full_text\"},\"semantic_chunked_idx\":{\"field\":\"body\",\"dimension\":3,\"chunker\":{\"provider\":\"antfly\",\"store_chunks\":false,\"full_text_index\":{},\"text\":{\"target_tokens\":4}}}}",
    ));

    var api = @import("../api/service.zig").Service.init(alloc, &wal_store, &builder);
    const first = [_]api_types.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "{\"body\":\"alpha bravo\",\"chunk_preview\":[\"alpha bravo\"],\"_enrichment\":{\"chunk_preview\":true,\"chunk_preview_version\":1}}" },
    };
    var ingest_first = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 100, .mutations = &first });
    defer ingest_first.deinit(alloc);
    var build_first = try catalog.buildTable("docs");
    defer build_first.deinit(alloc);

    const second = [_]api_types.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-b", .body = "{\"body\":\"charlie delta echo foxtrot golf\"}" },
    };
    var ingest_second = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 200, .mutations = &second });
    defer ingest_second.deinit(alloc);

    var status = try catalog.buildStatus("docs");
    defer status.deinit(alloc);
    try std.testing.expectEqual(catalog_types.NextPublishReason.wal_enrichment, status.next_publish_reason.?);
    try std.testing.expectEqual(catalog_types.EnrichmentStage.chunk_preview, status.enrichment_active_stage.?);
    try std.testing.expect(status.pending_materialization_families.chunk_preview);
    try std.testing.expect(status.pending_materialization_families.full_text);
}

test "catalog service auto-enables chunk embeddings for chunked embedding indexes" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    var catalog_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-auto-chunk-embeddings");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-auto-chunk-embeddings");
    const wal_root = tmpPath(&wal_root_buf, "wal-auto-chunk-embeddings");
    const catalog_root = tmpPath(&catalog_root_buf, "catalog-auto-chunk-embeddings");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);
    defer cleanupTmp(catalog_root);

    var fs_artifacts = try @import("../artifacts/mod.zig").FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try @import("fs_progress_store.zig").FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try wal_mod.FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_catalog = try @import("fs_store.zig").FsStore.init(alloc, std.mem.span(catalog_root));
    var fs_catalog_store = fs_catalog.catalogStore();
    defer fs_catalog_store.deinit();

    var builder = builder_mod.Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var catalog = CatalogService.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store, &builder, &fs_catalog_store);
    defer catalog.deinit();
    try std.testing.expect(try catalog.ensureTableWithDefinition(
        "docs",
        100,
        .{},
        "{\"version\":0}",
        "",
        "{\"semantic_chunked_idx\":{\"type\":\"embeddings\",\"field\":\"body\",\"dimension\":3,\"chunker\":{\"provider\":\"antfly\",\"text\":{\"target_tokens\":4}}}}",
    ));

    var status = try catalog.tableBuildStatus("docs");
    defer status.deinit(alloc);
    try std.testing.expect(status.chunk_embeddings_enabled);
    try std.testing.expectEqual(catalog_types.DerivedOutputPublicationAction.reuse, status.derived_output_actions.chunk_embeddings);
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
    const slice = std.fmt.bufPrint(buf, "/tmp/antfly-serverless-catalog-{s}-{d}-{d}\x00", .{
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

fn findFullTextIndexAction(
    actions: []const catalog_types.FullTextIndexPublicationAction,
    name: []const u8,
) ?catalog_types.ArtifactPublicationAction {
    for (actions) |action| {
        if (std.mem.eql(u8, action.name, name)) return action.action;
    }
    return null;
}

fn findFullTextIndexEntry(
    actions: []const catalog_types.FullTextIndexPublicationAction,
    name: []const u8,
) ?catalog_types.FullTextIndexPublicationAction {
    for (actions) |action| {
        if (std.mem.eql(u8, action.name, name)) return action;
    }
    return null;
}

fn findNamedArtifactAction(
    actions: []const catalog_types.NamedArtifactPublicationAction,
    name: []const u8,
) ?catalog_types.ArtifactPublicationAction {
    for (actions) |action| {
        if (std.mem.eql(u8, action.name, name)) return action.action;
    }
    return null;
}
