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
const catalog_types = @import("types.zig");
const catalog_store = @import("store.zig");
const progress_store_mod = @import("progress_store.zig");
const document_facts = @import("../build/document_facts.zig");
const document_facts_builder = @import("../build/document_facts_builder.zig");
const graph_page_store = @import("../graph_segment/page_store.zig");
const graph_page_tree = @import("../graph_segment/page_tree.zig");
const read_lease = @import("../manifest/read_lease.zig");
const manifest_mod = @import("../manifest/mod.zig");
const query_mod = @import("../query/mod.zig");
const wal_mod = @import("../wal/mod.zig");
const builder_mod = @import("../build/builder.zig");
const graph_metric_policy = @import("../build/graph_metric_policy.zig");
const graph_metric_config = @import("../build/graph_metric_config.zig");
const graph_metric_segment = @import("../graph_metric_segment/mod.zig");
const lake_graph_metric = @import("../build/lake_graph_metric.zig");
const impact_planner = @import("../build/impact_planner.zig");
const external_source_manifest = @import("../build/external_source_manifest.zig");
const external_metadata = @import("../build/external_publication_metadata.zig");
const publication_plan = @import("../build/publication_plan.zig");
const enrichment_pipeline = @import("../enrichment/pipeline.zig");
const api_codec = @import("../api/codec.zig");
const api_types = @import("../api/types.zig");
const search_sources = @import("../search_sources.zig");
const maintenance_cancellation = @import("../maintenance_cancellation.zig");
const work_lease = @import("../build/work_lease.zig");
const InventoryPublication = struct {
    artifacts: *artifacts_mod.ArtifactStore,
    cancellation: CancellationToken,
    maintenance: ?maintenance_cancellation.Token = null,
};
const vector_segment_mod = @import("../vector_segment/mod.zig");
const vector_index = @import("../build/vector_index.zig");
const tables_api = @import("../../api/tables.zig");
const full_text_indexes = @import("../../api/full_text_indexes.zig");
const coverage_policy = @import("../../api/coverage_policy.zig");
const shared_vector = @import("antfly_vector").vector;

const PublicationPlanPurpose = enum {
    status,
    publication,
};

const GraphMetricReadiness = struct {
    configured: usize = 0,
    pending: usize = 0,
    rejected: usize = 0,
};

fn graphMetricReadinessAlloc(alloc: Allocator, manifest: ?manifest_mod.Manifest, indexes_json: []const u8) !GraphMetricReadiness {
    const current = graph_metric_policy.materializerFingerprint(.{});
    const specs = try graph_metric_config.parseIndexSpecsAlloc(alloc, indexes_json);
    defer graph_metric_config.freeIndexSpecs(alloc, specs);
    var readiness = GraphMetricReadiness{};
    for (specs) |spec| {
        const graph_artifact = if (manifest) |value|
            findManifestNamedArtifact(value, .graph_segment, spec.index_name)
        else
            null;
        for (spec.configs) |config| {
            readiness.configured += 1;
            const value = manifest orelse {
                readiness.pending += 1;
                continue;
            };
            const name = try graph_metric_segment.artifactNameAlloc(alloc, spec.index_name, config.name);
            defer alloc.free(name);
            const artifact = findManifestNamedArtifact(value, .graph_metric_segment, name) orelse {
                readiness.pending += 1;
                continue;
            };
            const source_digest = if (graph_artifact) |graph_ref| blk: {
                artifacts_mod.validateSha256ArtifactIdentity(graph_ref.artifact_id, graph_ref.checksum) catch break :blk null;
                break :blk artifacts_mod.sha256DigestFromChecksum(graph_ref.checksum) catch null;
            } else null;
            const current_artifact = artifact.metadata_version == graph_metric_segment.wire_version and
                artifact.graph_metric_control_len != 0 and
                artifact.graph_metric_routing_footer_len != 0 and
                artifact.materializer_fingerprint == current and
                artifact.graph_metric_config_fingerprint == lake_graph_metric.configFingerprint(config) and
                source_digest != null and
                std.mem.eql(u8, &source_digest.?, &artifact.graph_metric_source_checksum);
            if (!current_artifact) {
                readiness.pending += 1;
                continue;
            }
            if (artifact.graph_metric_materialization_state == .rejected) readiness.rejected += 1;
        }
    }
    return readiness;
}

fn graphMetricMaterializationStaleAlloc(alloc: Allocator, manifest: manifest_mod.Manifest, indexes_json: []const u8) !bool {
    return (try graphMetricReadinessAlloc(alloc, manifest, indexes_json)).pending != 0;
}

test "serverless catalog schedules idle graph metric policy upgrades from manifest metadata" {
    var artifacts = [_]manifest_mod.ArtifactRef{
        .{
            .kind = .graph_segment,
            .name = "graph_idx",
            .artifact_id = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            .byte_len = 1,
            .checksum = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        },
        .{
            .kind = .graph_metric_segment,
            .name = "9:graph_idx4:rank",
            .artifact_id = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            .byte_len = 1,
            .checksum = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            .materializer_fingerprint = 0,
        },
    };
    var manifest = manifest_mod.Manifest{
        .namespace = "docs",
        .version = 1,
        .built_at_ns = 1,
        .wal_start_lsn = 0,
        .wal_end_lsn = 0,
        .stats = .{},
        .artifacts = &artifacts,
    };
    const indexes_json = "{\"graph_idx\":{\"type\":\"graph\",\"metrics\":{\"rank\":{\"kind\":\"pagerank\"}}}}";
    try std.testing.expect(try graphMetricMaterializationStaleAlloc(std.testing.allocator, manifest, indexes_json));
    const specs = try graph_metric_config.parseIndexSpecsAlloc(std.testing.allocator, indexes_json);
    defer graph_metric_config.freeIndexSpecs(std.testing.allocator, specs);
    artifacts[1].metadata_version = graph_metric_segment.wire_version;
    artifacts[1].materializer_fingerprint = graph_metric_policy.materializerFingerprint(.{});
    artifacts[1].graph_metric_control_len = 1;
    artifacts[1].graph_metric_routing_footer_len = 1;
    artifacts[1].graph_metric_config_fingerprint = lake_graph_metric.configFingerprint(specs[0].configs[0]);
    artifacts[1].graph_metric_source_checksum = @splat(0xaa);
    try std.testing.expect(!try graphMetricMaterializationStaleAlloc(std.testing.allocator, manifest, indexes_json));
    manifest.artifacts = manifest.artifacts[0..0];
    try std.testing.expect(try graphMetricMaterializationStaleAlloc(std.testing.allocator, manifest, indexes_json));
}

fn ensureSchemaWritesAllowedAlloc(alloc: Allocator, schema_json: []const u8) !void {
    var binding = (try publication_plan.externalBindingFromSchemaJsonAlloc(alloc, schema_json)) orelse return;
    defer binding.deinit(alloc);
    if (binding.binding.write_policy == .read_only) return error.ExternalTableReadOnly;
}

pub const CatalogService = struct {
    alloc: Allocator,
    artifacts: *artifacts_mod.ArtifactStore,
    manifests: *manifest_mod.ManifestStore,
    progress: *progress_store_mod.ProgressStore,
    wal: *wal_mod.WalStore,
    builder: *builder_mod.Builder,
    store: *catalog_store.CatalogStore,
    external_source_plan_resolver: ?publication_plan.ExternalSourcePlanResolver = null,
    facts_read_leases: read_lease.Cache = .{},

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
            .external_source_plan_resolver = null,
        };
    }

    pub fn deinit(self: *CatalogService) void {
        self.* = undefined;
    }

    pub fn setExternalSourcePlanResolver(
        self: *CatalogService,
        resolver: ?publication_plan.ExternalSourcePlanResolver,
    ) void {
        self.external_source_plan_resolver = resolver;
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

    pub fn ensureTableWritesAllowed(self: *CatalogService, table_name: []const u8) !void {
        var table = (try self.getTableAlloc(self.alloc, table_name)) orelse return error.NamespaceNotFound;
        defer table.deinit(self.alloc);
        try ensureSchemaWritesAllowedAlloc(self.alloc, table.schema_json);
    }

    pub fn ensureNamespaceWritesAllowed(self: *CatalogService, namespace: []const u8) !void {
        var table = (try self.getTableForNamespaceAlloc(self.alloc, namespace)) orelse return;
        defer table.deinit(self.alloc);
        try ensureSchemaWritesAllowedAlloc(self.alloc, table.schema_json);
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
        return self.buildStatusUntil(namespace, null);
    }

    pub fn buildStatusUntil(self: *CatalogService, namespace: []const u8, cancellation: ?maintenance_cancellation.Token) !catalog_types.BuildStatus {
        try maintenance_cancellation.check(cancellation);
        const policy = self.getPolicy(namespace) catch catalog_types.NamespacePolicy{};
        var plan = try self.publicationPlanForNamespaceAlloc(namespace, policy, .status, null);
        defer plan.deinit(self.alloc);
        const effective_policy = plan.policy;
        var published_head = try self.loadPublishedHeadAlloc(namespace);
        defer published_head.deinit(self.alloc);
        const head_version = published_head.manifest_version;
        const published_wal_end_lsn: u64 = if (published_head.manifest) |manifest|
            manifest.wal_end_lsn
        else
            0;
        var materialized_search_sources: search_sources.PublishedSearchSources = if (published_head.manifest) |manifest|
            try search_sources.clonePublishedSearchSourcesAlloc(
                self.alloc,
                manifest.stats.published_search_sources,
            )
        else
            .{};
        errdefer search_sources.deinitPublishedSearchSources(self.alloc, &materialized_search_sources);
        var materialized_derived_outputs: search_sources.MaterializedDerivedOutputs = if (published_head.manifest) |manifest|
            try search_sources.cloneMaterializedDerivedOutputsAlloc(
                self.alloc,
                manifest.stats.derived_outputs,
            )
        else
            .{};
        errdefer search_sources.deinitMaterializedDerivedOutputs(self.alloc, &materialized_derived_outputs);
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
        const enrichment_completion = try enrichmentCompletionAlloc(self, namespace, head_version, effective_policy, plan.table_definition.indexes_json, cancellation);
        const pipeline = enrichment_pipeline.builtinPipelineForPolicy(effective_policy);
        const enrichment_active_stage = chooseActiveEnrichmentStage(pipeline, enrichment_completion);
        var atomic_enrichment_progress = if (enrichment_active_stage) |stage|
            try self.progress.getEnrichmentStageProgress(namespace, stage)
        else
            null;
        defer if (atomic_enrichment_progress) |*value| value.deinit(self.progress.allocator);
        const enrichment_head_version = if (atomic_enrichment_progress) |value|
            value.head_version
        else
            null;
        const enrichment_doc_offset = if (atomic_enrichment_progress) |value|
            value.doc_offset
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
            .metric = vector_compaction.metric orelse effective_policy.vector_distance_metric,
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
            if (try self.builder.predictPendingWalPublicationActionsAllocUntil(
                namespace,
                effective_policy.vector_distance_metric,
                plan,
                cancellation,
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
        const head_indexes_json = if (published_head.manifest) |manifest| manifest.stats.indexes_json else "";
        const index_config_actions = try planIndexConfigActionsAlloc(
            self.alloc,
            head_indexes_json,
            plan.table_definition.indexes_json,
        );
        defer freeIndexConfigPublicationStatuses(self.alloc, index_config_actions);
        const head_republish_recommended = plan.forceRepublishFromHead();
        const pending_materialization_rebuild =
            pending_materialization_families.any() or
            (!head_republish_recommended and
                hasPendingMaterialization(plan, published_head.manifest));
        const graph_metric_readiness: GraphMetricReadiness = if (plan.external_materialization) |external| .{
            .configured = external.graph_metrics_configured,
            .pending = external.graph_metrics_pending,
            .rejected = external.graph_metrics_rejected,
        } else try graphMetricReadinessAlloc(
            self.alloc,
            published_head.manifest,
            plan.table_definition.indexes_json,
        );

        const owned_namespace = try self.alloc.dupe(u8, namespace);
        errdefer self.alloc.free(owned_namespace);
        var published_search_sources = try search_sources.clonePublishedSearchSourcesAlloc(
            self.alloc,
            plan.targets.published_search_sources,
        );
        errdefer search_sources.deinitPublishedSearchSources(self.alloc, &published_search_sources);
        const owned_index_config_actions = try cloneIndexConfigPublicationStatusesAlloc(self.alloc, index_config_actions);
        errdefer freeIndexConfigPublicationStatuses(self.alloc, owned_index_config_actions);
        const head_full_text_index_actions = try cloneCatalogFullTextIndexActionsAlloc(self.alloc, head_actions.full_text_index_actions);
        errdefer freeCatalogActions(self.alloc, head_full_text_index_actions);
        const head_vector_index_actions = try cloneCatalogNamedArtifactActionsAlloc(self.alloc, head_actions.vector_index_actions);
        errdefer freeCatalogActions(self.alloc, head_vector_index_actions);
        const head_sparse_index_actions = try cloneCatalogNamedArtifactActionsAlloc(self.alloc, head_actions.sparse_index_actions);
        errdefer freeCatalogActions(self.alloc, head_sparse_index_actions);
        const head_graph_index_actions = try cloneCatalogNamedArtifactActionsAlloc(self.alloc, head_actions.graph_index_actions);
        errdefer freeCatalogActions(self.alloc, head_graph_index_actions);
        const full_text_index_actions = try cloneFullTextIndexActionsAlloc(self.alloc, plan.full_text_index_actions);
        errdefer freeCatalogActions(self.alloc, full_text_index_actions);
        const vector_index_actions = try cloneNamedArtifactActionsAlloc(self.alloc, plan.vector_index_actions);
        errdefer freeCatalogActions(self.alloc, vector_index_actions);
        const sparse_index_actions = try cloneNamedArtifactActionsAlloc(self.alloc, plan.sparse_index_actions);
        errdefer freeCatalogActions(self.alloc, sparse_index_actions);
        const graph_index_actions = try cloneNamedArtifactActionsAlloc(self.alloc, plan.graph_index_actions);
        errdefer freeCatalogActions(self.alloc, graph_index_actions);
        const vector_compaction_driver_index_name = if (vector_compaction.driver_index_name) |value|
            try self.alloc.dupe(u8, value)
        else
            null;
        errdefer if (vector_compaction_driver_index_name) |value| self.alloc.free(value);

        return .{
            .namespace = owned_namespace,
            .published_search_sources = published_search_sources,
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
            .graph_metrics_configured = graph_metric_readiness.configured,
            .graph_metrics_pending = graph_metric_readiness.pending,
            .graph_metrics_rejected = graph_metric_readiness.rejected,
            .pending_materialization_families = pending_materialization_families,
            .head_artifact_actions = head_actions.artifact_actions,
            .head_full_text_index_actions = head_full_text_index_actions,
            .head_vector_index_actions = head_vector_index_actions,
            .head_sparse_index_actions = head_sparse_index_actions,
            .head_graph_index_actions = head_graph_index_actions,
            .head_derived_output_actions = head_actions.derived_output_actions,
            .artifact_actions = .{
                .document_segment = @enumFromInt(@intFromEnum(plan.artifact_actions.document_segment)),
                .full_text = @enumFromInt(@intFromEnum(plan.artifact_actions.full_text)),
                .dense_vector = @enumFromInt(@intFromEnum(plan.artifact_actions.dense_vector)),
                .sparse_vector = @enumFromInt(@intFromEnum(plan.artifact_actions.sparse_vector)),
                .graph = @enumFromInt(@intFromEnum(plan.artifact_actions.graph)),
            },
            .index_config_actions = owned_index_config_actions,
            .full_text_index_actions = full_text_index_actions,
            .vector_index_actions = vector_index_actions,
            .sparse_index_actions = sparse_index_actions,
            .graph_index_actions = graph_index_actions,
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
            .vector_compaction_driver_index_name = vector_compaction_driver_index_name,
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
        return try self.buildNamespaceGuarded(namespace, null);
    }

    pub fn buildNamespaceWithCancellation(self: *CatalogService, namespace: []const u8, cancellation: CancellationToken) !builder_mod.BuildResult {
        try cancellation.check();
        var fallback: ?std.Io.Threaded = if (self.builder.io == null) std.Io.Threaded.init(self.alloc, .{}) else null;
        defer if (fallback) |*value| value.deinit();
        return self.buildNamespaceGuardedUntil(namespace, null, .{ .io = self.builder.io orelse fallback.?.io(), .cooperative = cancellation });
    }

    pub fn buildNamespaceGuarded(
        self: *CatalogService,
        namespace: []const u8,
        publication_guard: ?@import("../build/work_lease.zig").PublicationGuard,
    ) !builder_mod.BuildResult {
        return try self.buildNamespaceGuardedUntil(namespace, publication_guard, null);
    }

    pub fn buildNamespaceGuardedUntil(
        self: *CatalogService,
        namespace: []const u8,
        publication_guard: ?@import("../build/work_lease.zig").PublicationGuard,
        cancellation: ?maintenance_cancellation.Token,
    ) !builder_mod.BuildResult {
        var fallback: ?std.Io.Threaded = if (self.builder.io == null and cancellation == null) std.Io.Threaded.init(self.alloc, .{}) else null;
        defer if (fallback) |*value| value.deinit();
        const io = if (cancellation) |token| token.io else self.builder.io orelse fallback.?.io();
        try maintenance_cancellation.check(cancellation);
        if (publication_guard == null) {
            var nonce: [16]u8 = undefined;
            io.random(&nonce);
            const owner = std.fmt.bytesToHex(&nonce, .lower);
            var held = (try work_lease.acquireHeld(try self.progress.workLeaseProvider(), io, namespace, &owner, 30 * std.time.ns_per_s)) orelse return error.WorkLeaseLost;
            defer _ = held.release() catch false;
            return self.buildNamespaceGuardedUntil(namespace, held.guard(), held.cancellation(cancellation orelse .{ .io = io }));
        }
        var protection = try builder_mod.GraphSourceProtection.init(self.progress, namespace, cancellation);
        const protected = protection.token(io);
        var bridge = maintenance_cancellation.GraphBridge{ .maintenance = protected };
        var scoped = self.artifacts.*;
        scoped.upload_scope = .{ .domain = graph_page_store.PageStore.namespaceDomain(namespace), .attempt = try builder_mod.graphPublicationAttempt(publication_guard, namespace, io) };
        const policy = self.getPolicy(namespace) catch catalog_types.NamespacePolicy{};
        var plan = try self.publicationPlanForNamespaceAlloc(namespace, policy, .publication, .{ .artifacts = &scoped, .cancellation = bridge.token(), .maintenance = protected });
        defer plan.deinit(self.alloc);
        return try self.builder.publishNamespaceWithMetricAndPlanGuardedUntil(
            namespace,
            policy.vector_distance_metric,
            plan,
            publication_guard,
            protected,
        );
    }

    pub fn buildTable(self: *CatalogService, table_name: []const u8) !builder_mod.BuildResult {
        return try self.buildTableWithCancellation(table_name, .none);
    }

    pub fn buildTableWithCancellation(
        self: *CatalogService,
        table_name: []const u8,
        cancellation: CancellationToken,
    ) !builder_mod.BuildResult {
        try cancellation.check();
        const namespace = try self.resolveTableNamespaceAlloc(table_name);
        defer self.alloc.free(namespace);
        return try self.buildNamespaceWithCancellation(namespace, cancellation);
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

    fn externalSourcePlanForTableAlloc(
        self: *CatalogService,
        namespace: []const u8,
        table: catalog_types.TableNamespaceRecord,
        publication: InventoryPublication,
        previous_artifacts: []const manifest_mod.ArtifactRef,
    ) !?external_source_manifest.Plan {
        var binding = (try publication_plan.externalBindingFromSchemaJsonAlloc(self.alloc, table.schema_json)) orelse return null;
        defer binding.deinit(self.alloc);
        // A user pin selects data; it does not supply the immutable inventory
        // required to publish it. Every external binding needs a resolved plan.
        const resolver = self.external_source_plan_resolver orelse return error.ExternalSourcePlanResolverUnavailable;
        return try resolver.resolveAlloc(self.alloc, .{
            .namespace = namespace,
            .table_name = table.table_name,
            .binding = binding.binding,
            .artifacts = publication.artifacts,
            .previous_artifacts = previous_artifacts,
            .cancellation = publication.cancellation,
        });
    }

    fn publicationPlanForNamespaceAlloc(
        self: *CatalogService,
        namespace: []const u8,
        policy: catalog_types.NamespacePolicy,
        purpose: PublicationPlanPurpose,
        publication: ?InventoryPublication,
    ) !publication_plan.TablePublicationPlan {
        const tables = try self.listTablesAlloc(self.alloc);
        defer self.freeTables(self.alloc, tables);
        for (tables) |table| {
            if (!std.mem.eql(u8, table.namespace, namespace)) continue;
            const effective_policy = effectivePolicyForTable(policy, table.indexes_json) catch return error.InvalidTableIndexMetadata;
            var external_binding = try publication_plan.externalBindingFromSchemaJsonAlloc(self.alloc, table.schema_json);
            defer if (external_binding) |*binding| binding.deinit(self.alloc);
            const default_indexes = table.indexes_json.len == 0 or std.mem.eql(u8, table.indexes_json, "{}");
            // External targets always come from explicit declarations,
            // including graph-only and whitespace-empty configurations.
            var targets: builder_mod.Builder.PublicationTargets = if (external_binding != null) external: {
                const graph_names = try builder_mod.listGraphIndexNamesAlloc(self.alloc, table.indexes_json);
                defer {
                    for (graph_names) |name| self.alloc.free(name);
                    self.alloc.free(graph_names);
                }
                break :external .{
                    .published_search_sources = try search_sources.publishedSearchSourcesForTableDefinitionWithDefaultsAlloc(
                        self.alloc,
                        table.schema_json,
                        table.read_schema_json,
                        table.indexes_json,
                        .explicit_only,
                    ),
                    .include_graph = graph_names.len != 0,
                };
            } else if (default_indexes)
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
            errdefer search_sources.deinitPublishedSearchSources(self.alloc, &targets.published_search_sources);

            var metadata_republish: publication_plan.MetadataRepublishReasons = .{};
            var published_head = try self.loadPublishedHeadAlloc(namespace);
            defer published_head.deinit(self.alloc);
            if (published_head.manifest) |manifest| {
                const head_version = published_head.manifest_version;
                metadata_republish.external_schema_changed = external_binding != null and
                    !std.mem.eql(u8, manifest.stats.schema_json, table.schema_json);

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
                const completion = try enrichmentCompletionAlloc(self, namespace, head_version, effective_policy, table.indexes_json, if (publication) |context| context.maintenance else null);
                const can_republish_chunk_preview = impact.rebuild_chunk_preview and
                    (!effective_policy.chunk_preview_enabled or completion.chunk_preview_complete);
                const can_republish_chunk_embeddings = impact.rebuild_chunk_embeddings and
                    (!effective_policy.chunk_embeddings_enabled or completion.chunk_embeddings_complete);
                const can_republish_rerank_terms = impact.rebuild_rerank_terms and
                    (!effective_policy.rerank_terms_enabled or completion.rerank_terms_complete);

                metadata_republish.read_schema_migration = impact.migration_state_changed;
                metadata_republish.index_definitions_changed = !std.mem.eql(
                    u8,
                    manifest.stats.indexes_json,
                    table.indexes_json,
                );
                // Inventory publication cannot fulfill managed sidecar
                // readiness. Preserve configured targets and pending status,
                // but let the lake sidecar workflow materialize them instead
                // of repeatedly publishing an identical remote inventory.
                metadata_republish.published_search_sources_changed = !isExternalManifest(manifest) and !publishedSearchSourcesMatch(
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
                metadata_republish.graph_metric_policy_changed = !isExternalManifest(manifest) and
                    try graphMetricMaterializationStaleAlloc(self.alloc, manifest, table.indexes_json);
                // Pending indexes are interpreted under their published policy.
                // Even an incomplete newly enabled stage needs this publication
                // before its worker can discover work. Derived-output readiness
                // must not gate the policy/index transition itself.
                const before_facts_policy = try document_facts_builder.fingerprint(self.alloc, manifest.stats.policy, manifest.stats.indexes_json);
                const after_facts_policy = try document_facts_builder.fingerprint(self.alloc, effective_policy, table.indexes_json);
                metadata_republish.document_facts_policy_changed = !isExternalManifest(manifest) and
                    !std.mem.eql(u8, &before_facts_policy, &after_facts_policy);

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
                    .full_text = if (external_binding != null and targets.published_search_sources.findText() == null)
                        .drop
                    else
                        publication_plan.collapseFullTextArtifactAction(full_text_index_actions, findManifestArtifactIndex(manifest, .text_segment) != null, .rebuild),
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
                    else if (findManifestArtifactIndex(manifest, .graph_segment) != null)
                        .reuse
                    else
                        .rebuild,
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
                var table_definition = try publication_plan.tableDefinitionSnapshotAlloc(
                    self.alloc,
                    table.schema_json,
                    table.read_schema_json,
                    table.indexes_json,
                );
                errdefer table_definition.deinit(self.alloc);
                var external_source_plan = if (purpose == .publication)
                    try self.externalSourcePlanForTableAlloc(namespace, table, publication.?, manifest.artifacts)
                else
                    null;
                errdefer if (external_source_plan) |*plan| plan.deinit(self.alloc);

                var plan: publication_plan.TablePublicationPlan = .{
                    .targets = targets,
                    .policy = effective_policy,
                    .table_definition = table_definition,
                    .external_source_plan = external_source_plan,
                    .metadata_republish = metadata_republish,
                    .artifact_actions = artifact_actions,
                    .full_text_index_actions = full_text_index_actions,
                    .vector_index_actions = vector_index_actions,
                    .sparse_index_actions = sparse_index_actions,
                    .graph_index_actions = graph_index_actions,
                    .derived_output_actions = derived_output_actions,
                };
                if (external_binding != null) try applyExternalReadinessAlloc(self.alloc, &plan, manifest);
                return plan;
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
            var table_definition = try publication_plan.tableDefinitionSnapshotAlloc(
                self.alloc,
                table.schema_json,
                table.read_schema_json,
                table.indexes_json,
            );
            errdefer table_definition.deinit(self.alloc);
            var external_source_plan = if (purpose == .publication)
                try self.externalSourcePlanForTableAlloc(namespace, table, publication.?, &.{})
            else
                null;
            errdefer if (external_source_plan) |*plan| plan.deinit(self.alloc);

            var plan: publication_plan.TablePublicationPlan = .{
                .targets = targets,
                .policy = effective_policy,
                .table_definition = table_definition,
                .external_source_plan = external_source_plan,
                .metadata_republish = metadata_republish,
                .artifact_actions = .{
                    .document_segment = .rebuild,
                    .full_text = if (external_binding != null and targets.published_search_sources.findText() == null)
                        .drop
                    else
                        publication_plan.collapseFullTextArtifactAction(full_text_index_actions, false, .rebuild),
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
            if (external_binding != null) try applyExternalReadinessAlloc(self.alloc, &plan, null);
            return plan;
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

/// Status and publication share the same metadata-only external reconciliation.
/// Managed namespace adjacency aliases never stand in for external projections.
fn applyExternalReadinessAlloc(alloc: Allocator, plan: *publication_plan.TablePublicationPlan, current: ?manifest_mod.Manifest) !void {
    var reconciliation = try external_metadata.planAlloc(alloc, current, plan.*);
    defer reconciliation.deinit(alloc);
    for (plan.full_text_index_actions) |*entry| {
        entry.action = reconciliation.action(.text_segment, entry.name);
    }
    inline for (.{ .{ "vector_index_actions", manifest_mod.ArtifactKind.vector_segment }, .{ "sparse_index_actions", manifest_mod.ArtifactKind.sparse_segment }, .{ "graph_index_actions", manifest_mod.ArtifactKind.graph_segment } }) |field| {
        for (@field(plan, field[0])) |*entry| {
            entry.action = reconciliation.action(field[1], entry.name);
        }
    }
    plan.artifact_actions = .{
        .document_segment = .reuse,
        .full_text = reconciliation.familyAction(.text_segment),
        .dense_vector = reconciliation.familyAction(.vector_segment),
        .sparse_vector = reconciliation.familyAction(.sparse_segment),
        .graph = reconciliation.familyAction(.graph_segment),
    };
    // External row sources do not run the managed document enrichment queue.
    // Its policy defaults must not invent chunk/rerank recomputations here;
    // the external dependency planner owns sidecar readiness instead.
    plan.derived_output_actions = .{};
    // Validate metric readiness against the compatible retained graph, not a
    // same-named old projection which metadata publication would discard.
    var retained_manifest = current;
    if (retained_manifest) |*manifest| manifest.artifacts = reconciliation.retained_refs;
    const metrics_ready = try graphMetricReadinessAlloc(alloc, retained_manifest, plan.table_definition.indexes_json);
    plan.external_materialization = .{
        .pending = reconciliation.hasOutstandingWork() or metrics_ready.pending != 0,
        .graph_metrics_configured = metrics_ready.configured,
        .graph_metrics_pending = metrics_ready.pending,
        .graph_metrics_rejected = metrics_ready.rejected,
    };
}

/// Actions describe intended publication; a declarative drop is outstanding
/// work only while there is something to remove. Keep this separate from
/// ArtifactActions.any(), which is also used as an execution summary.
fn hasPendingMaterialization(plan: publication_plan.TablePublicationPlan, current: ?manifest_mod.Manifest) bool {
    if (plan.external_materialization) |external| {
        if (external.pending) return true;
    } else {
        inline for (.{ .{ "document_segment", manifest_mod.ArtifactKind.document_segment }, .{ "full_text", manifest_mod.ArtifactKind.text_segment }, .{ "dense_vector", manifest_mod.ArtifactKind.vector_segment }, .{ "sparse_vector", manifest_mod.ArtifactKind.sparse_segment }, .{ "graph", manifest_mod.ArtifactKind.graph_segment } }) |field| {
            switch (@field(plan.artifact_actions, field[0])) {
                .rebuild => return true,
                .drop => if (current) |manifest| {
                    if (findManifestArtifactIndex(manifest, field[1]) != null) return true;
                },
                .reuse => {},
            }
        }
    }
    inline for (.{ "chunk_preview", "chunk_embeddings", "rerank_terms" }) |field| {
        switch (@field(plan.derived_output_actions, field)) {
            .recompute => return true,
            .drop => if (current) |manifest| {
                if (manifest.stats.derived_outputs.containsKind(@field(search_sources.DerivedOutputKind, field))) return true;
            },
            .reuse => {},
        }
    }
    return false;
}

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

fn cloneIndexConfigPublicationStatusesAlloc(
    alloc: Allocator,
    items: []const catalog_types.IndexConfigPublicationStatus,
) ![]catalog_types.IndexConfigPublicationStatus {
    if (items.len == 0) return &.{};
    const out = try alloc.alloc(catalog_types.IndexConfigPublicationStatus, items.len);
    errdefer alloc.free(out);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*entry| entry.deinit(alloc);
    }
    for (items, 0..) |item, idx| {
        out[idx] = .{
            .name = try alloc.dupe(u8, item.name),
            .action = item.action,
            .incarnation = item.incarnation,
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

fn freeIndexConfigPublicationStatuses(alloc: Allocator, items: []catalog_types.IndexConfigPublicationStatus) void {
    for (items) |*item| item.deinit(alloc);
    if (items.len > 0) alloc.free(items);
}

fn freeCatalogActions(alloc: Allocator, items: anytype) void {
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
            // Graph aliases select the same canonical namespace adjacency.
            // Index definitions/metric policy still require a manifest update,
            // but cannot invalidate its document-derived physical root. WAL
            // prediction separately promotes this action for changed facts.
            if (kind == .graph) break :blk if (current_artifact_count != 0) .reuse else .rebuild;
            if (before_object.get(entry.key_ptr.*)) |before_value| {
                if (isNamedIndexKindValue(before_value, kind) and namedIndexConfigEql(entry.value_ptr.*, before_value, kind)) {
                    break :blk .reuse;
                }
            }
            if (current_artifact_count == 1) {
                const rename_source = findEquivalentRenamedIndexName(before_object, after_object, kind, entry.key_ptr.*, entry.value_ptr.*);
                if (rename_source != null) break :blk .reuse;
            }
            break :blk .rebuild;
        };
        try actions.ensureUnusedCapacity(alloc, 1);
        actions.appendAssumeCapacity(.{
            .name = try alloc.dupe(u8, entry.key_ptr.*),
            .action = action,
        });
    }

    var before_it = before_object.iterator();
    while (before_it.next()) |entry| {
        if (!isNamedIndexKindValue(entry.value_ptr.*, kind)) continue;
        if (after_object.get(entry.key_ptr.*) != null) continue;
        try actions.ensureUnusedCapacity(alloc, 1);
        actions.appendAssumeCapacity(.{
            .name = try alloc.dupe(u8, entry.key_ptr.*),
            .action = .drop,
        });
    }

    std.mem.sort(publication_plan.NamedArtifactAction, actions.items, {}, lessNamedArtifactAction);
    return try actions.toOwnedSlice(alloc);
}

/// Plan exact public index-definition publication separately from physical
/// artifact reuse. A rename or equivalent config may reuse an artifact, but it
/// is not query-visible under the desired name until a manifest containing that
/// exact name/config pair becomes the head.
fn planIndexConfigActionsAlloc(
    alloc: Allocator,
    before_indexes_json: []const u8,
    after_indexes_json: []const u8,
) ![]catalog_types.IndexConfigPublicationStatus {
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

    var actions = std.ArrayListUnmanaged(catalog_types.IndexConfigPublicationStatus).empty;
    errdefer {
        for (actions.items) |*item| item.deinit(alloc);
        actions.deinit(alloc);
    }

    var after_it = after_object.iterator();
    while (after_it.next()) |entry| {
        const action: catalog_types.ArtifactPublicationAction = if (before_object.get(entry.key_ptr.*)) |before_value|
            if (jsonValueEql(entry.value_ptr.*, before_value)) .reuse else .rebuild
        else
            .rebuild;
        try actions.append(alloc, .{
            .name = try alloc.dupe(u8, entry.key_ptr.*),
            .action = action,
            .incarnation = coverage_policy.incarnation(entry.value_ptr.*),
        });
    }

    var before_it = before_object.iterator();
    while (before_it.next()) |entry| {
        if (after_object.get(entry.key_ptr.*) != null) continue;
        try actions.append(alloc, .{
            .name = try alloc.dupe(u8, entry.key_ptr.*),
            .action = .drop,
            .incarnation = coverage_policy.incarnation(entry.value_ptr.*),
        });
    }

    std.mem.sort(catalog_types.IndexConfigPublicationStatus, actions.items, {}, lessIndexConfigPublicationStatus);
    return try actions.toOwnedSlice(alloc);
}

fn lessIndexConfigPublicationStatus(
    _: void,
    lhs: catalog_types.IndexConfigPublicationStatus,
    rhs: catalog_types.IndexConfigPublicationStatus,
) bool {
    return std.mem.lessThan(u8, lhs.name, rhs.name);
}

test "serverless index config publication status carries durable embedding incarnation" {
    const alloc = std.testing.allocator;
    const actions = try planIndexConfigActionsAlloc(
        alloc,
        "{}",
        "{\"semantic_idx\":{\"type\":\"embeddings\",\"dimension\":3,\"_coverage_incarnation\":42}}",
    );
    defer freeIndexConfigPublicationStatuses(alloc, actions);

    try std.testing.expectEqual(@as(usize, 1), actions.len);
    try std.testing.expectEqualStrings("semantic_idx", actions[0].name);
    try std.testing.expectEqual(catalog_types.ArtifactPublicationAction.rebuild, actions[0].action);
    try std.testing.expectEqual(@as(?u64, 42), actions[0].incarnation);

    const recreated = try planIndexConfigActionsAlloc(
        alloc,
        "{\"semantic_idx\":{\"type\":\"embeddings\",\"dimension\":3,\"_coverage_incarnation\":41}}",
        "{\"semantic_idx\":{\"type\":\"embeddings\",\"dimension\":3,\"_coverage_incarnation\":42}}",
    );
    defer freeIndexConfigPublicationStatuses(alloc, recreated);
    try std.testing.expectEqual(catalog_types.ArtifactPublicationAction.rebuild, recreated[0].action);
    try std.testing.expectEqual(@as(?u64, 42), recreated[0].incarnation);
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
        if (!namedIndexConfigEql(entry.value_ptr.*, target_value, kind)) continue;
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
        try actions.ensureUnusedCapacity(alloc, 1);
        actions.appendAssumeCapacity(.{
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
        try actions.ensureUnusedCapacity(alloc, 1);
        actions.appendAssumeCapacity(.{
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

fn namedIndexConfigEql(lhs: std.json.Value, rhs: std.json.Value, kind: NamedSearchSourceKind) bool {
    if (kind != .graph) return jsonValueEql(lhs, rhs);
    if (lhs != .object or rhs != .object) return false;
    var lhs_count: usize = 0;
    var lhs_it = lhs.object.iterator();
    while (lhs_it.next()) |entry| {
        if (std.mem.eql(u8, entry.key_ptr.*, "metrics")) continue;
        lhs_count += 1;
        const other = rhs.object.get(entry.key_ptr.*) orelse return false;
        if (!jsonValueEql(entry.value_ptr.*, other)) return false;
    }
    var rhs_count: usize = 0;
    var rhs_it = rhs.object.iterator();
    while (rhs_it.next()) |entry| {
        if (!std.mem.eql(u8, entry.key_ptr.*, "metrics")) rhs_count += 1;
    }
    return lhs_count == rhs_count;
}

test "serverless named graph planning reuses topology for metric-only changes" {
    const actions = try planNamedIndexActionsAlloc(
        std.testing.allocator,
        "{\"graph_idx\":{\"type\":\"graph\",\"field\":\"edges\",\"metrics\":{\"rank\":{\"kind\":\"pagerank\",\"max_iterations\":20}}}}",
        "{\"graph_idx\":{\"type\":\"graph\",\"field\":\"edges\",\"metrics\":{\"rank\":{\"kind\":\"pagerank\",\"max_iterations\":40}}}}",
        .graph,
        1,
    );
    defer freeNamedArtifactActions(std.testing.allocator, actions);
    try std.testing.expectEqual(@as(usize, 1), actions.len);
    try std.testing.expectEqual(publication_plan.ArtifactAction.reuse, actions[0].action);
}

test "serverless named graph planning unwinds every failed allocation" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, struct {
        fn run(a: Allocator) !void {
            const actions = try planNamedIndexActionsAlloc(a, "{\"old\":{\"type\":\"graph\"}}", "{\"a\":{\"type\":\"graph\"},\"b\":{\"type\":\"graph\"},\"c\":{\"type\":\"graph\"},\"d\":{\"type\":\"graph\"},\"e\":{\"type\":\"graph\"},\"f\":{\"type\":\"graph\"},\"g\":{\"type\":\"graph\"},\"h\":{\"type\":\"graph\"}}", .graph, 1);
            defer freeNamedArtifactActions(a, actions);
        }
    }.run, .{});
}

test "serverless named graph planning treats aliases separately from canonical storage" {
    const a = std.testing.allocator;
    const graph = "{\"g\":{\"type\":\"graph\"}}";
    const two = "{\"g\":{\"type\":\"graph\"},\"alias\":{\"type\":\"graph\",\"edge_types\":[\"links\"]}}";
    for ([_]struct { before: []const u8, after: []const u8, roots: usize, expected: publication_plan.ArtifactAction }{
        .{ .before = "{}", .after = graph, .roots = 1, .expected = .reuse },
        .{ .before = graph, .after = two, .roots = 1, .expected = .reuse },
        .{ .before = graph, .after = "{\"renamed\":{\"type\":\"graph\",\"edge_types\":[\"other\"]}}", .roots = 2, .expected = .reuse },
        .{ .before = graph, .after = graph, .roots = 0, .expected = .rebuild },
        .{ .before = "{}", .after = graph, .roots = 0, .expected = .rebuild },
    }) |case| {
        const actions = try planNamedIndexActionsAlloc(a, case.before, case.after, .graph, case.roots);
        defer freeNamedArtifactActions(a, actions);
        var live: usize = 0;
        for (actions) |action| {
            if (action.action == .drop) continue;
            live += 1;
            try std.testing.expectEqual(case.expected, action.action);
        }
        try std.testing.expect(live != 0);
    }
    const removed = try planNamedIndexActionsAlloc(a, graph, "{}", .graph, 1);
    defer freeNamedArtifactActions(a, removed);
    try std.testing.expectEqual(@as(usize, 1), removed.len);
    try std.testing.expectEqual(publication_plan.ArtifactAction.drop, removed[0].action);
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
    self: *CatalogService,
    namespace: []const u8,
    head_version: u64,
    policy: catalog_types.NamespacePolicy,
    indexes_json: []const u8,
    cancellation: ?maintenance_cancellation.Token,
) !EnrichmentCompletion {
    if (head_version == 0) return .{};
    // Pin before resolving the manifest; a concurrent retirement must either
    // observe this reader or reject it. Never turn an unreadable source into
    // "enrichment complete".
    const pin = try self.facts_read_leases.acquire(self.progress, namespace, head_version);
    const ReadCheck = struct {
        pin: read_lease.Lease,
        cancellation: ?maintenance_cancellation.Token,
        fn check(ptr: *const anyopaque) !void {
            const state: *const @This() = @ptrCast(@alignCast(ptr));
            try state.pin.check();
            try maintenance_cancellation.check(state.cancellation);
        }
        fn cancelled(ptr: *const anyopaque) bool {
            check(ptr) catch return true;
            return false;
        }
    };
    const check = ReadCheck{ .pin = pin, .cancellation = cancellation };
    const token = CancellationToken{ .ptr = &check, .check_fn = ReadCheck.check, .is_cancelled_fn = ReadCheck.cancelled };
    try token.check();
    var manifest = try self.manifests.getAlloc(namespace, head_version);
    defer manifest.deinit(self.alloc);
    // Read-only external inventories have no managed document bodies or WAL
    // enrichment queue. Their lake sidecars are built from the pinned source,
    // not from document facts. Do not mistake that valid layout for corruption.
    if (isExternalManifest(manifest)) return .{};
    const idx = findArtifactIndex(manifest, .document_facts) orelse return error.DocumentFactsNotFound;
    for (manifest.artifacts[idx + 1 ..]) |artifact| if (artifact.kind == .document_facts) return error.InvalidDocumentFactsRoot;
    var reads: u64 = (builder_mod.GraphBuildLimits{}).max_input_bytes;
    var writes: u64 = 0;
    var pages = graph_page_store.PageStore{
        .domain = graph_page_store.PageStore.namespaceDomain(namespace),
        .artifacts = self.artifacts,
        .remaining_read_bytes = &reads,
        .remaining_write_bytes = &writes,
        .cancellation = token,
    };
    const root = try document_facts.loadRoot(self.alloc, &pages, manifest.artifacts[idx]);
    if (root.wal_end_lsn != manifest.wal_end_lsn or root.document_count != manifest.stats.document_count) return error.DocumentFactsSourceChanged;
    const result = try enrichmentCompletionFromFactsAlloc(self.alloc, &pages, root, policy, indexes_json);
    try token.check();
    return result;
}

fn isExternalManifest(manifest: manifest_mod.Manifest) bool {
    const source = manifest.base_source orelse return false;
    return switch (source) {
        .external_parquet, .external_iceberg, .external_lance => true,
        else => false,
    };
}

fn enrichmentCompletionFromFactsAlloc(
    alloc: Allocator,
    pages: *graph_page_store.PageStore,
    root: document_facts.Root,
    policy: catalog_types.NamespacePolicy,
    indexes_json: []const u8,
) !EnrichmentCompletion {
    if (!policy.enrichment_enabled and !policy.chunk_preview_enabled and
        !policy.chunk_embeddings_enabled and !policy.rerank_terms_enabled) return .{};
    var pending: [4]u64 = root.counts[3..7].*;
    if (try document_facts_builder.needsRebuild(alloc, root, policy, indexes_json)) {
        // Only a real change to facts semantics needs bodies. Stream that
        // exceptional read with bounded memory and the same admission budget
        // as publication, rather than rebuilding a second document array.
        pending = @splat(0);
        var context = try document_facts_builder.Context.init(alloc, policy, indexes_json);
        defer context.deinit();
        var cursor = try graph_page_tree.Cursor.init(alloc, pages.store(), root.page, "", null);
        defer cursor.deinit();
        var count: u64 = 0;
        while (try cursor.next()) |record| {
            try pages.cancellation.check();
            const fact = try document_facts.Fact.decode(record.value);
            const body = try document_facts.readBodyAlloc(alloc, pages, fact.body);
            defer alloc.free(body);
            const flags = try context.flags(.{
                .doc_id = @constCast(record.key),
                .body = body,
                .last_lsn = fact.last_lsn,
                .last_timestamp_ns = fact.last_timestamp_ns,
            });
            for (&pending, 0..) |*value, bit| {
                if (flags.pending & (@as(u4, 1) << @intCast(bit)) != 0) value.* += 1;
            }
            count += 1;
        }
        if (count != root.document_count) return error.InvalidDocumentFactsRoot;
    }
    return .{
        .lexical_sparse_complete = pending[0] == 0,
        .lexical_sparse_pending_documents = pending[0],
        .chunk_preview_complete = pending[1] == 0,
        .chunk_preview_pending_documents = pending[1],
        .chunk_embeddings_complete = pending[2] == 0,
        .chunk_embeddings_pending_documents = pending[2],
        .rerank_terms_complete = pending[3] == 0,
        .rerank_terms_pending_documents = pending[3],
    };
}

test "serverless catalog facts completion uses exact counters and authoritative policy refresh" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/facts-completion", .{tmp.sub_path});
    defer alloc.free(path);
    var fs = try artifacts_mod.FsStore.init(alloc, path);
    var artifacts = fs.artifactStore();
    defer artifacts.deinit();
    var reads: u64 = 1024 * 1024;
    var writes: u64 = 1024 * 1024;
    var pages = graph_page_store.PageStore{
        .domain = graph_page_store.PageStore.namespaceDomain("docs"),
        .attempt = @splat(1),
        .artifacts = &artifacts,
        .remaining_read_bytes = &reads,
        .remaining_write_bytes = &writes,
    };
    const policy = catalog_types.NamespacePolicy{ .enrichment_enabled = true };
    const initial = [_]query_mod.QueryMaterializedDocument{
        .{ .doc_id = @constCast("a"), .body = @constCast("{\"text\":\"alpha\"}"), .last_lsn = 1, .last_timestamp_ns = 1 },
        .{ .doc_id = @constCast("b"), .body = @constCast("{\"text\":\"bravo\"}"), .last_lsn = 2, .last_timestamp_ns = 2 },
    };
    const first = try document_facts_builder.publishAlloc(alloc, &pages, null, &initial, null, policy, "{}", 2);
    defer alloc.free(first.artifact_id);
    defer alloc.free(first.checksum);
    const a_body = "{\"text\":\"alpha\",\"_enrichment\":{\"lexical_sparse_version\":1}}";
    const a_docs = [_]query_mod.QueryMaterializedDocument{.{ .doc_id = @constCast("a"), .body = @constCast(a_body), .last_lsn = 3, .last_timestamp_ns = 3 }};
    const a_mutations = [_]query_mod.QueryMaterializerMutation{.{ .doc_id = @constCast("a"), .body = @constCast(a_body), .kind = .upsert, .lsn = 3, .timestamp_ns = 3 }};
    const second = try document_facts_builder.publishAlloc(alloc, &pages, first, &a_docs, &a_mutations, policy, "{}", 3);
    defer alloc.free(second.artifact_id);
    defer alloc.free(second.checksum);
    const b_body = "{\"text\":\"bravo\",\"other\":\"new\"}";
    const b_docs = [_]query_mod.QueryMaterializedDocument{.{ .doc_id = @constCast("b"), .body = @constCast(b_body), .last_lsn = 4, .last_timestamp_ns = 4 }};
    const b_mutations = [_]query_mod.QueryMaterializerMutation{.{ .doc_id = @constCast("b"), .body = @constCast(b_body), .kind = .upsert, .lsn = 4, .timestamp_ns = 4 }};
    const third = try document_facts_builder.publishAlloc(alloc, &pages, second, &b_docs, &b_mutations, policy, "{}", 4);
    defer alloc.free(third.artifact_id);
    defer alloc.free(third.checksum);
    const root = try document_facts.loadRoot(alloc, &pages, third);

    // No flat document segment or mutation segment exists, and no page/body
    // reads are admitted: unchanged semantics must use the exact root tuple.
    reads = 0;
    const current = try enrichmentCompletionFromFactsAlloc(alloc, &pages, root, policy, "{}");
    try std.testing.expectEqual(@as(u64, 1), current.lexical_sparse_pending_documents);
    try std.testing.expect(!current.lexical_sparse_complete);
    var upgraded = policy;
    upgraded.enrichment_pipeline_version = 2;
    try std.testing.expectError(error.ArtifactReadBudgetExceeded, enrichmentCompletionFromFactsAlloc(alloc, &pages, root, upgraded, "{}"));
    reads = 1024 * 1024;
    const refreshed = try enrichmentCompletionFromFactsAlloc(alloc, &pages, root, upgraded, "{}");
    try std.testing.expectEqual(@as(u64, 2), refreshed.lexical_sparse_pending_documents);
    var disabled = policy;
    disabled.enrichment_enabled = false;
    const complete = try enrichmentCompletionFromFactsAlloc(alloc, &pages, root, disabled, "{}");
    try std.testing.expect(complete.lexical_sparse_complete);
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

test "serverless vector compaction signal aggregates named vector artifacts" {
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

test "serverless vector compaction signal uses driver artifact metrics" {
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

test "serverless vector compaction signal ignores artifacts whose adaptive policy is a no-op" {
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

test "serverless catalog service tracks namespaces and reports build status" {
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
    var published = try manifest_store.getAlloc("docs", after.head_version);
    defer published.deinit(alloc);
    try std.testing.expect(findManifestArtifactIndex(published, .document_facts) != null);
    try std.testing.expect(findManifestArtifactIndex(published, .graph_segment) != null);
    try std.testing.expectEqual(published.artifacts.len, after.retained_artifacts);
    try std.testing.expect(!after.compaction_recommended);
    try std.testing.expect(after.enrichment_complete);
}

test "serverless catalog service stores per-namespace policy" {
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

test "serverless catalog service exposes table records over serving namespaces" {
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

test "serverless catalog service republishes head when table index metadata changes without new wal" {
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

test "serverless catalog service republishes head when derived output policy changes without new wal" {
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
        "{\"semantic_idx\":{\"type\":\"embeddings\",\"dimension\":3,\"distance_metric\":\"cosine\"}}",
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

    // Incomplete stages must refresh the maintained pending indexes too. In
    // particular lexical sparse has no derived-output republish flag of its
    // own, and waiting for a stage to complete before publishing its policy
    // would deadlock its worker against the old empty pending index.
    _ = try catalog.setPolicy("docs", .{ .enrichment_enabled = true, .enrichment_pipeline_version = 2, .chunk_preview_enabled = true, .chunk_preview_pipeline_version = 2 });
    var enabled = try catalog.buildStatus("docs");
    defer enabled.deinit(alloc);
    try std.testing.expect(enabled.publish_recommended);
    var refresh = try catalog.buildTable("docs");
    defer refresh.deinit(alloc);
    try std.testing.expect(refresh.published);
    try std.testing.expectEqual(@as(u64, 1), refresh.wal_end_lsn);
    var refreshed = try manifest_store.getAlloc("docs", refresh.version);
    defer refreshed.deinit(alloc);
    var reads: u64 = 4096;
    var writes: u64 = 0;
    var pages = graph_page_store.PageStore{
        .domain = graph_page_store.PageStore.namespaceDomain("docs"),
        .artifacts = &artifact_store,
        .remaining_read_bytes = &reads,
        .remaining_write_bytes = &writes,
    };
    const root = try document_facts.loadRoot(alloc, &pages, refreshed.artifacts[findArtifactIndex(refreshed, .document_facts).?]);
    try std.testing.expectEqual(@as(u64, 1), root.counts[3]);
    try std.testing.expectEqual(@as(u64, 1), root.counts[4]);
    try std.testing.expect(root.pending_pages[0] != null and root.pending_pages[1] != null);
    var stable = try catalog.buildStatus("docs");
    defer stable.deinit(alloc);
    try std.testing.expect(!stable.publish_recommended);
}

test "serverless catalog service republishes head when graph index metadata changes without new wal" {
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
        "{\"semantic_idx\":{\"type\":\"embeddings\",\"dimension\":3,\"distance_metric\":\"cosine\"},\"graph_idx\":{\"type\":\"graph\"}}",
    ));

    var status = try catalog.buildStatus("docs");
    defer status.deinit(alloc);
    try std.testing.expect(status.publish_recommended);
    try std.testing.expectEqual(catalog_types.NextPublishReason.head_republish, status.next_publish_reason.?);
    try std.testing.expect(status.head_republish_recommended);
    try std.testing.expect(!status.pending_materialization_rebuild);
    try std.testing.expectEqual(catalog_types.ArtifactPublicationAction.reuse, status.artifact_actions.graph);
    try std.testing.expectEqual(catalog_types.ArtifactPublicationAction.reuse, findNamedArtifactAction(status.graph_index_actions, "graph_idx").?);

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
    try std.testing.expectEqual(catalog_types.ArtifactPublicationAction.reuse, findNamedArtifactAction(after.head_graph_index_actions, "graph_idx").?);

    var aliased = try manifest_store.getAlloc("docs", 2);
    defer aliased.deinit(alloc);
    const canonical_id = first_manifest.artifacts[findManifestArtifactIndex(first_manifest, .graph_segment).?].artifact_id;
    try std.testing.expectEqualStrings(canonical_id, findManifestNamedArtifact(aliased, .graph_segment, "graph_idx").?.artifact_id);

    // Removing the final public alias does not remove the namespace's default
    // graph: aliases and canonical physical storage have separate lifetimes.
    try std.testing.expect(try catalog.setTableDefinition(
        "docs",
        "{\"default_type\":\"doc\"}",
        "",
        "{\"semantic_idx\":{\"type\":\"embeddings\",\"dimension\":3,\"distance_metric\":\"cosine\"}}",
    ));
    var dropping = try catalog.buildStatus("docs");
    defer dropping.deinit(alloc);
    try std.testing.expect(dropping.head_republish_recommended);
    try std.testing.expectEqual(catalog_types.ArtifactPublicationAction.reuse, dropping.artifact_actions.graph);
    try std.testing.expectEqual(catalog_types.ArtifactPublicationAction.drop, findNamedArtifactAction(dropping.graph_index_actions, "graph_idx").?);
    var dropped = try catalog.buildTable("docs");
    defer dropped.deinit(alloc);
    try std.testing.expect(dropped.published);
    var unnamed = try manifest_store.getAlloc("docs", dropped.version);
    defer unnamed.deinit(alloc);
    const retained_graph = unnamed.artifacts[findManifestArtifactIndex(unnamed, .graph_segment).?];
    try std.testing.expectEqualStrings(canonical_id, retained_graph.artifact_id);
    try std.testing.expectEqualStrings("", retained_graph.name);
}

test "serverless catalog service republishes head when dense index config changes without renaming source" {
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

test "serverless catalog service reports chunk embeddings changes as pending materialization rebuilds" {
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
    // Publish the new pending index first, while keeping derived readiness
    // explicitly pending until the worker has produced embeddings.
    try std.testing.expect(status.head_republish_recommended);
    try std.testing.expect(status.pending_materialization_rebuild);
    try std.testing.expect(status.publish_recommended);
    try std.testing.expectEqual(catalog_types.ArtifactPublicationAction.reuse, status.artifact_actions.dense_vector);
    try std.testing.expectEqual(catalog_types.DerivedOutputPublicationAction.recompute, status.derived_output_actions.chunk_embeddings);
    try std.testing.expectEqual(catalog_types.DerivedOutputResolution.pending_materialization, status.derived_output_resolutions.chunk_embeddings);
    try std.testing.expect(status.pending_materialization_families.chunk_embeddings);
    try std.testing.expect(!status.pending_materialization_families.dense_vector);
}

test "serverless catalog service republishes head when chunk embeddings are already materialized" {
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

test "serverless catalog service republishes head when chunk preview is already materialized" {
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

test "serverless catalog service republishes head when rerank terms are already materialized" {
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

test "serverless catalog service reports named vector and sparse publication actions" {
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

test "serverless catalog service defers small publish tails while enrichment is still in progress" {
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

    try std.testing.expect(try progress_store.compareAndSwapEnrichmentStageProgress("docs", .lexical_sparse, null, .{ .head_version = 1, .doc_offset = 0, .pipeline_version = 1 }));

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

test "serverless catalog service advances active enrichment stage to rerank terms" {
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

test "serverless catalog service uses stage-specific publish thresholds for later enrichment stages" {
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

test "serverless catalog service recommends compaction only while head still contains mutation segments" {
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

test "serverless catalog service recommends compaction based on document base lineage after pruning" {
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
    // Publication pins are shared read rights and remain valid after the
    // writer returns. Test eventual pruning once those rights have expired.
    const lease = @import("../manifest/read_lease.zig");
    const gc_now = @import("antfly_platform").time.realtimeNs() + lease.duration_ns + lease.gc_grace_ns + 1;
    pruner.read_lease_clock = .{ .ptr = &gc_now, .unix_fn = struct {
        fn now(ptr: *const anyopaque) u64 {
            return @as(*const u64, @ptrCast(@alignCast(ptr))).*;
        }
    }.now };
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

test "serverless catalog service reports mutation tail resolved by next inline rebase publish" {
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

test "serverless catalog service reports versioned full text migration actions" {
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

test "serverless catalog service reports versioned full text cutover drop actions" {
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

test "serverless catalog service reports head publication actions for wal partial reuse" {
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

test "serverless catalog service predicts wal partial reuse before publish" {
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

test "serverless catalog service predicts graph index reuse before publish when graph projection is unchanged" {
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

test "serverless catalog service predicts graph index rebuild before publish when graph projection changes" {
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

test "serverless catalog service predicts derived output recomputes from pending wal when enrichment is enabled" {
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

test "serverless catalog service predicts lexical sparse enrichment stage from pending wal" {
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

test "serverless catalog service marks pending wal enrichment as ready to publish when threshold is met" {
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

test "serverless catalog service reports chunk-augmented full text status without chunk preview policy" {
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
        "{\"full_text_index_v0\":{\"type\":\"full_text\"},\"semantic_chunked_idx\":{\"field\":\"body\",\"dimension\":3,\"chunker\":{\"provider\":\"antfly\",\"store_chunks\":false,\"full_text_index\":{},\"text\":{\"target_tokens\":4,\"overlap_tokens\":0}}}}",
    ));

    var status = try catalog.tableBuildStatus("docs");
    defer status.deinit(alloc);
    const full_text = findFullTextIndexEntry(status.full_text_index_actions, "full_text_index_v0").?;
    try std.testing.expectEqual(full_text_indexes.FullTextSourceMode.document_plus_artifact, full_text.source_mode);
    try std.testing.expectEqual(@as(usize, 1), full_text.chunked_source_count);
    try std.testing.expectEqual(false, status.chunk_preview_enabled);
}

test "serverless catalog service marks chunk-backed full text as waiting on chunk preview materialization" {
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
        "{\"full_text_index_v0\":{\"type\":\"full_text\"},\"semantic_chunked_idx\":{\"field\":\"body\",\"dimension\":3,\"chunker\":{\"provider\":\"antfly\",\"store_chunks\":false,\"full_text_index\":{},\"text\":{\"target_tokens\":4,\"overlap_tokens\":0}}}}",
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

test "serverless catalog service auto-enables chunk embeddings for chunked embedding indexes" {
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
        "{\"semantic_chunked_idx\":{\"type\":\"embeddings\",\"field\":\"body\",\"dimension\":3,\"chunker\":{\"provider\":\"antfly\",\"text\":{\"target_tokens\":4,\"overlap_tokens\":0}}}}",
    ));

    var status = try catalog.tableBuildStatus("docs");
    defer status.deinit(alloc);
    try std.testing.expect(status.chunk_embeddings_enabled);
    try std.testing.expectEqual(catalog_types.DerivedOutputPublicationAction.reuse, status.derived_output_actions.chunk_embeddings);
}

test "serverless catalog service fails closed for current external binding without resolver" {
    const alloc = std.testing.allocator;
    const current_schema =
        \\{"version":5,"storage_mode":"relational","default_type":"row","enforce_types":true,"base_source":{"kind":"external","table_id":"events","format":"parquet","uri":"s3://bucket/events","snapshot":"current","schema_fingerprint":"schema-v5","write_policy":"read_only"},"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var table = catalog_types.TableNamespaceRecord{
        .table_name = try alloc.dupe(u8, "events"),
        .namespace = try alloc.dupe(u8, "events"),
        .created_at_ns = 1,
        .schema_json = try alloc.dupe(u8, current_schema),
        .read_schema_json = try alloc.dupe(u8, ""),
        .indexes_json = try alloc.dupe(u8, "{}"),
    };
    defer table.deinit(alloc);

    var catalog: CatalogService = undefined;
    catalog.alloc = alloc;
    catalog.external_source_plan_resolver = null;
    var unused_artifacts: artifacts_mod.ArtifactStore = undefined;
    try std.testing.expectError(
        error.ExternalSourcePlanResolverUnavailable,
        catalog.externalSourcePlanForTableAlloc("events", table, .{ .artifacts = &unused_artifacts, .cancellation = .none }, &.{}),
    );
}

test "serverless external readiness shares exact publication bindings and actual presence" {
    const a = std.testing.allocator;
    var current = try external_metadata.testing.fixtureAlloc(a, 16384);
    defer current.deinit(a);
    for (current.artifacts) |*ref| {
        if (ref.kind == .graph_metric_segment) ref.materializer_fingerprint = graph_metric_policy.materializerFingerprint(.{});
    }
    const Helpers = struct {
        fn plan(alloc: Allocator, manifest: manifest_mod.Manifest, indexes: []const u8) !publication_plan.TablePublicationPlan {
            var out = publication_plan.TablePublicationPlan{ .targets = .{ .published_search_sources = .{} } };
            errdefer out.deinit(alloc);
            out.policy = manifest.stats.policy;
            out.table_definition = try publication_plan.tableDefinitionSnapshotAlloc(alloc, manifest.stats.schema_json, manifest.stats.read_schema_json, indexes);
            out.full_text_index_actions = try planFullTextIndexActionsAlloc(alloc, manifest.stats.schema_json, manifest.stats.schema_json, manifest.stats.indexes_json, indexes);
            out.vector_index_actions = try planNamedIndexActionsAlloc(alloc, manifest.stats.indexes_json, indexes, .vector, countManifestArtifactsOfKind(manifest, .vector_segment));
            out.graph_index_actions = try planNamedIndexActionsAlloc(alloc, manifest.stats.indexes_json, indexes, .graph, countManifestArtifactsOfKind(manifest, .graph_segment));
            try applyExternalReadinessAlloc(alloc, &out, manifest);
            return out;
        }
    };
    var ready = try Helpers.plan(a, current, current.stats.indexes_json);
    defer ready.deinit(a);
    try std.testing.expect(!hasPendingMaterialization(ready, current));
    try std.testing.expectEqual(publication_plan.ArtifactAction.reuse, ready.full_text_index_actions[0].action);
    try std.testing.expectEqual(publication_plan.ArtifactAction.reuse, ready.vector_index_actions[0].action);
    try std.testing.expectEqual(publication_plan.ArtifactAction.reuse, ready.graph_index_actions[0].action);
    try std.testing.expectEqual(@as(usize, 0), ready.external_materialization.?.graph_metrics_pending);
    ready.policy.chunk_preview_enabled = true;
    ready.policy.chunk_embeddings_enabled = true;
    ready.policy.rerank_terms_enabled = true;
    ready.derived_output_actions = .{ .chunk_preview = .recompute, .chunk_embeddings = .recompute, .rerank_terms = .recompute };
    try applyExternalReadinessAlloc(a, &ready, current);
    try std.testing.expect(!hasPendingMaterialization(ready, current));
    try std.testing.expect(!ready.derived_output_actions.any());

    // Metadata can be current while individual physical indexes are missing.
    // Another graph projection must not satisfy the newly configured graph.
    const changed = "{\"body_text\":{\"type\":\"full_text\",\"field\":\"other\"},\"vec\":{\"type\":\"embeddings\",\"field\":\"other_vector\",\"dimension\":3},\"graph_idx\":{\"type\":\"graph\",\"field\":\"graph_edges\"},\"g2\":{\"type\":\"graph\",\"field\":\"other_edges\",\"metrics\":{\"rank\":{\"kind\":\"pagerank\"}}}}";
    var changing = try Helpers.plan(a, current, changed);
    defer changing.deinit(a);
    var metadata_only = try external_metadata.reconcileAlloc(a, current, changing);
    defer metadata_only.deinit(a);
    var missing = try Helpers.plan(a, metadata_only, changed);
    defer missing.deinit(a);
    try std.testing.expect(hasPendingMaterialization(missing, metadata_only));
    try std.testing.expectEqual(publication_plan.ArtifactAction.rebuild, missing.full_text_index_actions[0].action);
    try std.testing.expectEqual(publication_plan.ArtifactAction.rebuild, missing.vector_index_actions[0].action);
    for (missing.graph_index_actions) |entry| {
        try std.testing.expectEqual(if (std.mem.eql(u8, entry.name, "graph_idx")) publication_plan.ArtifactAction.reuse else .rebuild, entry.action);
    }
    try std.testing.expectEqual(publication_plan.ArtifactAction.rebuild, missing.artifact_actions.graph);
    try std.testing.expectEqual(@as(usize, 1), missing.external_materialization.?.graph_metrics_pending);

    var dropping = try Helpers.plan(a, current, "{}");
    defer dropping.deinit(a);
    try std.testing.expect(hasPendingMaterialization(dropping, current));
    var empty = try external_metadata.reconcileAlloc(a, current, dropping);
    defer empty.deinit(a);
    var converged = try Helpers.plan(a, empty, "{}");
    defer converged.deinit(a);
    try std.testing.expect(!hasPendingMaterialization(converged, empty));
    try std.testing.expectEqual(publication_plan.ArtifactAction.reuse, converged.artifact_actions.graph);

    const AllocationExercise = struct {
        fn run(alloc: Allocator, manifest: manifest_mod.Manifest, indexes: []const u8) !void {
            var plan = try Helpers.plan(alloc, manifest, indexes);
            defer plan.deinit(alloc);
        }
    };
    try std.testing.checkAllAllocationFailures(a, AllocationExercise.run, .{ current, current.stats.indexes_json });
    try std.testing.checkAllAllocationFailures(a, AllocationExercise.run, .{ current, "{}" });
}

test "serverless materialization readiness distinguishes absent drops from real work" {
    var plan = publication_plan.TablePublicationPlan{
        .targets = .{ .published_search_sources = .{} },
        .artifact_actions = .{ .document_segment = .reuse, .full_text = .reuse, .dense_vector = .drop, .sparse_vector = .drop, .graph = .drop },
        .derived_output_actions = .{ .chunk_preview = .drop, .chunk_embeddings = .drop, .rerank_terms = .drop },
    };
    try std.testing.expect(!hasPendingMaterialization(plan, null));
    var current = try external_metadata.testing.fixtureAlloc(std.testing.allocator, 1);
    defer current.deinit(std.testing.allocator);
    try std.testing.expect(hasPendingMaterialization(plan, current));
    plan.artifact_actions.dense_vector = .reuse;
    plan.artifact_actions.graph = .reuse;
    try std.testing.expect(!hasPendingMaterialization(plan, current));
    plan.derived_output_actions.chunk_preview = .recompute;
    try std.testing.expect(hasPendingMaterialization(plan, current));
}

test "serverless catalog status stays local and write admission rejects read-only external tables" {
    const alloc = std.testing.allocator;
    const NoArtifactAccess = struct {
        calls: usize = 0,
        fn denied(ptr: *anyopaque) error{UnexpectedArtifactAccess} {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            return error.UnexpectedArtifactAccess;
        }
        fn deinit(_: Allocator, _: *anyopaque) void {}
        fn put(ptr: *anyopaque, _: Allocator, _: []const u8) !artifacts_mod.store.ArtifactMetadata {
            return denied(ptr);
        }
        fn putScoped(ptr: *anyopaque, _: Allocator, _: artifacts_mod.store.UploadScope, _: []const u8, _: @import("../../common/cancellation.zig").CancellationToken) !artifacts_mod.store.ArtifactMetadata {
            return denied(ptr);
        }
        fn get(ptr: *anyopaque, _: Allocator, _: []const u8) ![]u8 {
            return denied(ptr);
        }
        fn range(ptr: *anyopaque, _: Allocator, _: []const u8, _: u64, _: usize) ![]u8 {
            return denied(ptr);
        }
        fn stat(ptr: *anyopaque, _: Allocator, _: []const u8) !artifacts_mod.store.ArtifactMetadata {
            return denied(ptr);
        }
        fn delete(ptr: *anyopaque, _: []const u8) !void {
            return denied(ptr);
        }
        fn store(self: *@This(), a: Allocator) artifacts_mod.ArtifactStore {
            return .{ .allocator = a, .ptr = self, .vtable = &.{ .deinit = deinit, .put = put, .put_scoped = putScoped, .get_alloc = get, .get_range_alloc = range, .stat = stat, .delete = delete } };
        }
    };
    const current_schema =
        \\{"version":5,"storage_mode":"relational","default_type":"row","enforce_types":true,"base_source":{"kind":"external","table_id":"events","format":"parquet","uri":"s3://bucket/events","snapshot":"current","schema_fingerprint":"schema-v5","write_policy":"read_only"},"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    const pinned_schema = try std.mem.replaceOwned(u8, alloc, current_schema, "\"snapshot\":\"current\"", "\"snapshot\":{\"mode\":\"object_version_digest\",\"digest\":\"discovered-snapshot\"}");
    defer alloc.free(pinned_schema);
    const iceberg_current = try std.mem.replaceOwned(u8, alloc, current_schema, "\"format\":\"parquet\"", "\"format\":\"iceberg\"");
    defer alloc.free(iceberg_current);
    const iceberg_pinned = try std.mem.replaceOwned(u8, alloc, iceberg_current, "\"snapshot\":\"current\"", "\"snapshot\":{\"mode\":\"snapshot_id\",\"id\":\"31\"}");
    defer alloc.free(iceberg_pinned);

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    var catalog_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-external-status");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-external-status");
    const wal_root = tmpPath(&wal_root_buf, "wal-external-status");
    const catalog_root = tmpPath(&catalog_root_buf, "catalog-external-status");
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
    var test_catalog_store = fs_catalog.catalogStore();
    defer test_catalog_store.deinit();

    var builder = builder_mod.Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var catalog = CatalogService.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store, &builder, &test_catalog_store);
    defer catalog.deinit();
    try std.testing.expect(try catalog.ensureTableWithDefinition("events", 1, .{}, current_schema, "", "{}"));

    var initial_targets = try catalog.publicationPlanForNamespaceAlloc("events", .{}, .status, null);
    defer initial_targets.deinit(alloc);
    try std.testing.expect(initial_targets.targets.published_search_sources.findText() == null);
    try std.testing.expect(!initial_targets.targets.include_graph);
    try std.testing.expectEqual(publication_plan.ArtifactAction.reuse, initial_targets.artifact_actions.full_text);
    try std.testing.expectEqual(publication_plan.ArtifactAction.reuse, initial_targets.artifact_actions.document_segment);
    try std.testing.expect(!initial_targets.external_materialization.?.pending);

    var status = try catalog.tableBuildStatus("events");
    defer status.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 0), status.latest_wal_lsn);
    try std.testing.expectError(error.ExternalTableReadOnly, catalog.ensureTableWritesAllowed("events"));
    try std.testing.expectError(error.ExternalTableReadOnly, catalog.ensureNamespaceWritesAllowed("events"));
    try std.testing.expectError(error.ExternalSourcePlanResolverUnavailable, catalog.buildTable("events"));

    // A selector is not a resolved inventory. Neither current nor explicit
    // pins may create a partial external HEAD without the resolver capability.
    {
        const actual_artifacts = artifact_store;
        defer artifact_store = actual_artifacts;
        var denied: NoArtifactAccess = .{};
        artifact_store = denied.store(alloc);
        for ([_][]const u8{ current_schema, pinned_schema, iceberg_current, iceberg_pinned }) |schema| {
            _ = try catalog.setTableDefinition("events", schema, "", "{}");
            var local_status = try catalog.tableBuildStatus("events");
            defer local_status.deinit(alloc);
            try std.testing.expectError(error.ExternalSourcePlanResolverUnavailable, catalog.buildTable("events"));
            const versions = try manifest_store.listVersionsAlloc("events");
            defer alloc.free(versions);
            try std.testing.expectEqual(@as(usize, 0), versions.len);
            try std.testing.expectError(error.FileNotFound, progress_store.getHead("events"));
        }
        try std.testing.expectEqual(@as(usize, 0), denied.calls);
    }
    _ = try catalog.setTableDefinition("events", current_schema, "", "{}");

    const ScopedResolver = struct {
        progress: *progress_store_mod.ProgressStore,
        shared_artifacts: *artifacts_mod.ArtifactStore,
        io: std.Io,
        fn resolve(ptr: *anyopaque, a: Allocator, request: publication_plan.ExternalSourcePlanResolveRequest) !external_source_manifest.Plan {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expect(self.shared_artifacts.upload_scope == null);
            const scope = request.artifacts.upload_scope orelse return error.ExternalInventoryPublicationScopeRequired;
            try std.testing.expectEqual(graph_page_store.PageStore.namespaceDomain(request.namespace), scope.domain);
            var contender = try work_lease.acquireHeld(try self.progress.workLeaseProvider(), self.io, request.namespace, "discovery-contender", 30 * std.time.ns_per_s);
            defer if (contender) |*held| {
                _ = held.release() catch false;
            };
            if (contender != null) return error.ExternalInventoryResolvedWithoutLease;
            try request.cancellation.check();
            const published = try @import("../build/external_source_publish.zig").publishInventoryAlloc(a, request.artifacts, request.binding, .{
                .format = .parquet,
                .source_id = @constCast(request.binding.table_id),
                .source_uri = @constCast(request.binding.source_uri),
                .snapshot_id = @constCast("discovered-snapshot"),
                .schema_fingerprint = @constCast(request.binding.schema_fingerprint),
                .files = &.{},
            }, .{ .artifact_name = "events.external-files", .previous_artifacts = request.previous_artifacts, .cancellation = request.cancellation });
            errdefer {
                var owned = published;
                owned.deinit(a);
            }
            for (request.previous_artifacts) |prior| {
                if (prior.kind == .external_base_source) try std.testing.expectEqualStrings(prior.artifact_id, published.plan.artifacts[0].artifact_id);
            }
            return published.plan;
        }
    };
    var io_impl = threadedIo();
    defer io_impl.deinit();
    builder.setIo(io_impl.io());
    var resolver = ScopedResolver{ .progress = &progress_store, .shared_artifacts = &artifact_store, .io = io_impl.io() };
    catalog.setExternalSourcePlanResolver(.{ .ptr = &resolver, .vtable = &.{ .resolve = ScopedResolver.resolve } });
    var published = try catalog.buildTable("events");
    defer published.deinit(alloc);
    try std.testing.expect(published.published);
    var head = try manifest_store.getAlloc("events", published.version);
    defer head.deinit(alloc);
    const inventory = head.artifacts[findManifestArtifactIndex(head, .external_base_source).?];
    const scope = (try artifacts_mod.store.uploadScopeFromArtifactId(inventory.artifact_id)).?;
    try std.testing.expectEqual(head.publication_fencing_token, scope.fencingToken());
    var unchanged = try catalog.buildTable("events");
    defer unchanged.deinit(alloc);
    if (unchanged.published) return error.UnexpectedExternalRepublish;
    // Metadata changes still publish, but must not manufacture an empty local
    // document/facts snapshot or lose the authenticated remote inventory.
    try std.testing.expect(try catalog.setTableDefinition("events", current_schema, "{}", "{}"));
    var metadata = try catalog.buildTable("events");
    defer metadata.deinit(alloc);
    try std.testing.expect(metadata.published);
    var metadata_head = try manifest_store.getAlloc("events", metadata.version);
    defer metadata_head.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), metadata_head.artifacts.len);
    try std.testing.expectEqual(manifest_mod.ArtifactKind.external_base_source, metadata_head.artifacts[0].kind);
    try std.testing.expectEqualStrings(inventory.artifact_id, metadata_head.artifacts[0].artifact_id);
    try std.testing.expectEqualStrings("{}", metadata_head.stats.read_schema_json);
    var metadata_unchanged = try catalog.buildTable("events");
    defer metadata_unchanged.deinit(alloc);
    try std.testing.expect(!metadata_unchanged.published);
    var converged_status = try catalog.tableBuildStatus("events");
    defer converged_status.deinit(alloc);
    try std.testing.expect(!converged_status.head_republish_recommended);
    try std.testing.expect(!converged_status.pending_materialization_rebuild);
    try std.testing.expectEqual(catalog_types.ArtifactPublicationAction.reuse, converged_status.artifact_actions.document_segment);

    const graph_indexes = "{\"graph_idx\":{\"type\":\"graph\",\"metrics\":{\"rank\":{\"kind\":\"pagerank\"}}}}";
    // Selector intent must publish even without a text index to incidentally
    // trigger schema migration. It then converges without inventory churn.
    for ([_][]const u8{ "{}", graph_indexes }) |indexes| {
        try std.testing.expect(try catalog.setTableDefinition("events", current_schema, "{}", indexes));
        var initial_selection = try catalog.buildTable("events");
        defer initial_selection.deinit(alloc);
        for ([_][]const u8{ pinned_schema, current_schema }) |schema| {
            try std.testing.expect(try catalog.setTableDefinition("events", schema, "{}", indexes));
            var selection_plan = try catalog.publicationPlanForNamespaceAlloc("events", .{}, .status, null);
            defer selection_plan.deinit(alloc);
            try std.testing.expect(selection_plan.metadata_republish.external_schema_changed);
            try std.testing.expect(selection_plan.forceRepublishFromHead());
            var selection = try catalog.buildTable("events");
            defer selection.deinit(alloc);
            try std.testing.expect(selection.published);
            var selected_head = try manifest_store.getAlloc("events", selection.version);
            defer selected_head.deinit(alloc);
            try std.testing.expectEqualStrings(schema, selected_head.stats.schema_json);
            try std.testing.expectEqualStrings(inventory.artifact_id, selected_head.artifacts[findManifestArtifactIndex(selected_head, .external_base_source).?].artifact_id);
            var selection_unchanged = try catalog.buildTable("events");
            defer selection_unchanged.deinit(alloc);
            try std.testing.expect(!selection_unchanged.published);
        }
    }
    try std.testing.expect(try catalog.setTableDefinition("events", current_schema, "{}", "{}"));
    var reset_indexes = try catalog.buildTable("events");
    defer reset_indexes.deinit(alloc);
    try std.testing.expect(try catalog.setTableDefinition("events", current_schema, "{}", graph_indexes));
    var configured = try catalog.buildTable("events");
    defer configured.deinit(alloc);
    try std.testing.expect(configured.published);
    var configured_unchanged = try catalog.buildTable("events");
    defer configured_unchanged.deinit(alloc);
    try std.testing.expect(!configured_unchanged.published);
    var configured_status = try catalog.tableBuildStatus("events");
    defer configured_status.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), configured_status.graph_metrics_configured);
    try std.testing.expectEqual(@as(usize, 1), configured_status.graph_metrics_pending);
    // Target planning must use the same explicit-index contract as lake
    // reconciliation, independent of JSON whitespace or unrelated indexes.
    for ([_]struct { json: []const u8, graph: bool }{
        .{ .json = "{}", .graph = false },
        .{ .json = "{ }", .graph = false },
        .{ .json = graph_indexes, .graph = true },
    }) |case| {
        try std.testing.expect(try catalog.setTableDefinition("events", current_schema, "{}", case.json));
        var target_plan = try catalog.publicationPlanForNamespaceAlloc("events", .{}, .status, null);
        defer target_plan.deinit(alloc);
        try std.testing.expect(target_plan.targets.published_search_sources.findText() == null);
        try std.testing.expectEqual(case.graph, target_plan.targets.include_graph);
        try std.testing.expectEqual(publication_plan.ArtifactAction.reuse, target_plan.artifact_actions.full_text);
        try std.testing.expectEqual(case.graph, target_plan.external_materialization.?.pending);
    }

    // The same capability requirement holds for metadata republishing an
    // existing external HEAD. In particular, never turn its absent WAL into
    // an empty managed document snapshot or drop its authenticated inventory.
    {
        const actual_artifacts = artifact_store;
        defer artifact_store = actual_artifacts;
        var denied: NoArtifactAccess = .{};
        artifact_store = denied.store(alloc);
        catalog.external_source_plan_resolver = null;
        const before_head = try progress_store.getHead("events");
        const before_versions = try manifest_store.listVersionsAlloc("events");
        defer alloc.free(before_versions);
        for ([_][]const u8{ current_schema, pinned_schema, iceberg_current, iceberg_pinned }) |schema| {
            _ = try catalog.setTableDefinition("events", schema, "", "{}");
            var local_status = try catalog.tableBuildStatus("events");
            defer local_status.deinit(alloc);
            try std.testing.expect(local_status.head_republish_recommended);
            try std.testing.expectError(error.ExternalSourcePlanResolverUnavailable, catalog.buildTable("events"));
            try std.testing.expectEqual(before_head, try progress_store.getHead("events"));
            const after_versions = try manifest_store.listVersionsAlloc("events");
            defer alloc.free(after_versions);
            try std.testing.expectEqualSlices(u64, before_versions, after_versions);
        }
        try std.testing.expectEqual(@as(usize, 0), denied.calls);
    }
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
