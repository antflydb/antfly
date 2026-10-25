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

//! Search operations of the local storage DB. Methods take `*DB` and
//! are re-exported from `DB` in db.zig, so callers keep writing `db.method()`.

const db_zig = @import("db.zig");
const DB = db_zig.DB;
const allDocsVisibleFastCallback = DB.allDocsVisibleFastCallback;
const textLiveFilterDocSetControlledCallback = DB.textLiveFilterDocSetControlledCallback;
const schema_mod = @import("../schema.zig");
const range_cardinality = @import("range_cardinality.zig");
const apply_state = @import("derived/apply_state.zig");
const openStoredLogicalSourcesCallback = db_zig.openStoredLogicalSourcesCallback;
const requestHasLogicalValues = db_zig.requestHasLogicalValues;
const textQueryHasLogicalValues = db_zig.textQueryHasLogicalValues;
const TestDirectory = @import("../../common/test_directory.zig").TestDirectory;
const algebraic_mod = @import("algebraic/mod.zig");
const ant_json = @import("antfly-json");
const artifact_ids = @import("artifact/ids.zig");
const asset_producer_mod = @import("enrichment/asset_producer.zig");
const builtin = @import("builtin");
const db_query_metrics = @import("query_metrics.zig");
const db_query_projection = @import("query/projection.zig");
const db_query_result_shape = @import("query/result_shape.zig");
const db_query_search = @import("query/search_exec.zig");
const distributed_stats_mod = @import("../../search/distributed_stats.zig");
const doc_identity = @import("doc_identity.zig");
const doc_set = @import("doc_set.zig");
const document_query = @import("document/query.zig");
const enrichment_runtime_mod = @import("enrichment/enrichment_runtime.zig");
const index_manager_mod = @import("catalog/index_manager.zig");
const internal_keys = @import("../internal_keys.zig");
const native_query_cut = @import("native/query_cut.zig");
const planning_stats_mod = @import("planning_stats.zig");
const platform = @import("antfly_platform");
const search_mod = @import("../../search/search.zig");
const std = @import("std");
const storage_engine = @import("../engine.zig");
const types = @import("types.zig");
const vectorindex_mod = @import("antfly_vectorindex");
const Allocator = db_zig.Allocator;
const GateAssetProducer = db_zig.GateAssetProducer;
const SearchWithCapturedRequestResult = db_zig.SearchWithCapturedRequestResult;
const VectorSearchPresenceContext = db_zig.VectorSearchPresenceContext;
const benchQueryProfileEnabled = db_zig.benchQueryProfileEnabled;
const currentTimeNs = db_zig.currentTimeNs;
const encodeStoreLookupKeyAlloc = db_zig.encodeStoreLookupKeyAlloc;
const expectJsonNumber = db_zig.expectJsonNumber;
const expectedChunkArtifactKeyAlloc = db_zig.expectedChunkArtifactKeyAlloc;
const expectedDocumentEmbeddingArtifactKeyAlloc = db_zig.expectedDocumentEmbeddingArtifactKeyAlloc;
const externalizeSearchResultArtifactIds = db_zig.externalizeSearchResultArtifactIds;
const filterPresentSearchHitsMany = db_zig.filterPresentSearchHitsMany;
const filterStoredSearchCandidatesManyCallback = db_zig.filterStoredSearchCandidatesManyCallback;
const filterVectorSearchCandidatesManyCallback = db_zig.filterVectorSearchCandidatesManyCallback;
const freeConstDocIds = db_zig.freeConstDocIds;
const freeOptionalOwnedBytes = db_zig.freeOptionalOwnedBytes;
const isExpiredDocumentKeyCallback = db_zig.isExpiredDocumentKeyCallback;
const isNonvisibleStoredSearchKeyCallback = db_zig.isNonvisibleStoredSearchKeyCallback;
const isVisibleNonChunkSearchHitCallback = db_zig.isVisibleNonChunkSearchHitCallback;
const isVisibleSearchHitCallback = db_zig.isVisibleSearchHitCallback;
const loadParentStoredForSearchCallback = db_zig.loadParentStoredForSearchCallback;
const loadParentStoredForSearchManyCallback = db_zig.loadParentStoredForSearchManyCallback;
const loadStoredSearchDocumentCallback = db_zig.loadStoredSearchDocumentCallback;
const loadStoredSearchDocumentManyCallback = db_zig.loadStoredSearchDocumentManyCallback;
const loadStoredSearchDocumentsMany = db_zig.loadStoredSearchDocumentsMany;
const observeSearchFailureMetric = db_zig.observeSearchFailureMetric;
const ordinalProjectionFields = db_zig.ordinalProjectionFields;
const platform_time = db_zig.platform_time;
const primaryParentMatchAllEligible = db_zig.primaryParentMatchAllEligible;
const projectOwnedStoredBytesForSearch = db_zig.projectOwnedStoredBytesForSearch;
const projectStoredBytesForSearch = db_zig.projectStoredBytesForSearch;
const putDenseEmbeddingArtifactForTest = db_zig.putDenseEmbeddingArtifactForTest;
const resolveChunkParentIdCallback = db_zig.resolveChunkParentIdCallback;
const resolvedDocSetFromSearchHitOrdinalsAlloc = db_zig.resolvedDocSetFromSearchHitOrdinalsAlloc;
const AlgebraicDocFilterRequest = DB.AlgebraicDocFilterRequest;
const ChunkBoolFilterLowering = DB.ChunkBoolFilterLowering;
const IdentityPrefixFilterRequest = DB.IdentityPrefixFilterRequest;
const PrimaryMatchAllIdentityScan = DB.PrimaryMatchAllIdentityScan;
const allDocsVisibleCallback = DB.allDocsVisibleCallback;
const applyResolvedFilterIncludeAlloc = DB.applyResolvedFilterIncludeAlloc;
const attachGraphResultsCallback = DB.attachGraphResultsCallback;
const cloneNamedSetCallback = DB.cloneNamedSetCallback;
const composedQueryMetricIndexName = DB.composedQueryMetricIndexName;
const directSingleVectorRequest = DB.directSingleVectorRequest;
const dupeAlgebraicDocIds = DB.dupeAlgebraicDocIds;
const fuseNamedSetsCallback = DB.fuseNamedSetsCallback;
const hasNativeDocIdConstraints = DB.hasNativeDocIdConstraints;
const intersectAlgebraicDocIds = DB.intersectAlgebraicDocIds;
const liftChunkBoolFilterClausesAlloc = DB.liftChunkBoolFilterClausesAlloc;
const liveFilterDocSetCallback = DB.liveFilterDocSetCallback;
const lookupLiveDocOrdinalNoLockCallback = DB.lookupLiveDocOrdinalNoLockCallback;
const lookupLiveDocOrdinalsNoLockCallback = DB.lookupLiveDocOrdinalsNoLockCallback;
const nonVisibleDocSetCallback = DB.nonVisibleDocSetCallback;
const requiresFullCandidateVisibilityFilterCallback = DB.requiresFullCandidateVisibilityFilterCallback;
const resolveDocIdsToDocSetCallback = DB.resolveDocIdsToDocSetCallback;
const resolveDocSetDocIdsCallback = DB.resolveDocSetDocIdsCallback;
const resolveStructuredDocFilterForComposedCallback = DB.resolveStructuredDocFilterForComposedCallback;
const resolveStructuredTextDocFilterForComposedCallback = DB.resolveStructuredTextDocFilterForComposedCallback;
const resolvedDocSetStatCount = DB.resolvedDocSetStatCount;
const scanStoreRangeCallback = DB.scanStoreRangeCallback;
const scanStoreRangeWithContextCallback = DB.scanStoreRangeWithContextCallback;
const searchDenseComposedCallback = DB.searchDenseComposedCallback;
const searchDenseComposedProfiledCallback = DB.searchDenseComposedProfiledCallback;
const sparseDocNumsForOrdinalsCallback = DB.sparseDocNumsForOrdinalsCallback;
const sparseIndexCallback = DB.sparseIndexCallback;
const textIndexEntryCallback = DB.textIndexEntryCallback;
const textIndexIsChunkBackedCallback = DB.textIndexIsChunkBackedCallback;
const textIndexSupportsUnitGroupingCallback = DB.textIndexSupportsUnitGroupingCallback;
const unionAlgebraicDocIds = DB.unionAlgebraicDocIds;
const unionResolvedFilterExcludeAlloc = DB.unionResolvedFilterExcludeAlloc;

/// Execute directly against an immutable production text-index snapshot.
/// This deliberately bypasses DB query envelopes, MVCC constraint
/// derivation, public projection, and stored-document loading. It is an
/// internal engineering/benchmark boundary, not a public query API.
pub fn searchTextKernel(
    self: *DB,
    alloc: Allocator,
    index_name: []const u8,
    text_query: types.TextQuery,
    options: types.TextKernelSearchOptions,
) !types.TextKernelResult {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc, index_name, text_query, options });
    var row_policy_lease = try self.local_execution.row_policy_gate.enterUnsupportedSearch();
    defer row_policy_lease.release();
    try self.lockApplySharedForPortableRuntime();
    defer self.core.unlockApplyShared();

    const entry = self.core.textIndexEntry(index_name) orelse return error.IndexNotFound;
    entry.lockAnalysisShared();
    defer entry.unlockAnalysisShared();
    const text_snapshot = entry.persistent.acquireSnapshot();
    defer text_snapshot.release();

    var arena_state = std.heap.ArenaAllocator.init(alloc);
    defer arena_state.deinit();
    const query = try db_query_search.textQueryToSearchQuery(
        arena_state.allocator(),
        text_query,
        entry.text_analysis,
        entry.runtime_schema,
    );
    var search_diagnostics = search_mod.SearchDiagnostics{};
    var result = try search_mod.execute(alloc, text_snapshot, .{
        .query = query,
        .k = options.limit,
        .include_stored = false,
        .bm25_config = .{ .k1 = options.bm25.k1, .b = options.bm25.b },
        .diagnostics = if (options.collect_diagnostics) &search_diagnostics else null,
    });
    defer result.deinit();

    const hits = try alloc.alloc(types.TextKernelHit, result.hits.len);
    errdefer alloc.free(hits);
    for (result.hits, 0..) |hit, i| {
        hits[i] = .{
            .doc_ordinal = (try text_snapshot.docOrdinal(hit.doc_id)) orelse return error.MissingTextDocOrdinal,
            .score = hit.score,
        };
    }
    return .{
        .hits = hits,
        .total_hits = result.total_hits,
        .total_hits_relation = switch (result.total_hits_relation) {
            .exact => .exact,
            .gte => .gte,
        },
        .diagnostics = .{
            .segments_considered = search_diagnostics.segments_considered,
            .segments_searched = search_diagnostics.segments_searched,
            .segments_pruned = search_diagnostics.segments_pruned,
            .postings_iterators_opened = search_diagnostics.postings_iterators_opened,
            .wand_next_in_score = search_diagnostics.wand_next_in_score,
            .wand_next_in_advance = search_diagnostics.wand_next_in_advance,
            .wand_pivots_scored = search_diagnostics.wand_pivots_scored,
            .wand_pivots_advanced = search_diagnostics.wand_pivots_advanced,
            .wand_chunks_skipped = search_diagnostics.wand_chunks_skipped,
            .boolean_candidates_scored = search_diagnostics.boolean_candidates_scored,
            .boolean_chunks_skipped = search_diagnostics.boolean_chunks_skipped,
            .phrase_candidates_verified = search_diagnostics.phrase_candidates_verified,
            .phrase_position_records_decoded = search_diagnostics.phrase_position_records_decoded,
            .phrase_matches_scored = search_diagnostics.phrase_matches_scored,
        },
    };
}

pub fn search(self: *DB, alloc: Allocator, req: types.SearchRequest) !types.SearchResult {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc, req });
    return try searchWithExecutionContext(self, alloc, req, .{});
}

/// Returns the identity generation captured after entering the DB search
/// lease together with the result produced under that lease. Public read
/// adapters must use this operation instead of sampling the generation and
/// then calling `search`, which leaves a writer race between the two calls.
pub fn searchWithCapturedRequest(
    self: *DB,
    alloc: Allocator,
    req: types.SearchRequest,
) !SearchWithCapturedRequestResult {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc, req });
    return try self.searchWithCapturedRequestAndExecutionContext(alloc, req, .{});
}

pub fn searchWithExecutionContext(
    self: *DB,
    alloc: Allocator,
    req: types.SearchRequest,
    exec_ctx: types.ExecutionContext,
) !types.SearchResult {
    return (try self.searchWithCapturedRequestAndExecutionContext(alloc, req, exec_ctx)).result;
}

pub fn searchWithCapturedRequestAndExecutionContext(
    self: *DB,
    alloc: Allocator,
    req: types.SearchRequest,
    exec_ctx: types.ExecutionContext,
) !SearchWithCapturedRequestResult {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc, req, exec_ctx });
    if (self.local_execution.initial_child_hidden.load(.acquire)) return error.InitialChildNotPublished;
    var row_policy_lease = try self.local_execution.row_policy_gate.enterUnsupportedSearch();
    defer row_policy_lease.release();
    if (self.async_context.resource_manager) |manager| manager.beginForegroundQuery();
    defer if (self.async_context.resource_manager) |manager| manager.finishForegroundQuery();
    // Keep degraded requests out of catalog/apply lock queues. Revalidate
    // after admission below to close publication's check-to-use race.
    try self.enforcePortableRuntimeGate();
    if (std.meta.eql(exec_ctx, types.ExecutionContext{})) {
        const snapshot_input = directSingleVectorRequest(req) orelse req;
        const dense = snapshot_input.dense orelse if (snapshot_input.query == .dense_knn) snapshot_input.query.dense_knn else null;
        if (dense) |query| if (try self.trySnapshotDenseSearch(alloc, snapshot_input, query, false)) |captured| {
            var response_req = req;
            response_req.identity_read_generation = captured.request.identity_read_generation;
            return .{ .request = response_req, .result = captured.result };
        };
    }
    const bench_profile = benchQueryProfileEnabled();
    const total_start_ns = if (bench_profile) platform_time.monotonicNs() else 0;
    var generation_ns: u64 = 0;
    var lock_wait_ns: u64 = 0;
    var locked_search_ns: u64 = 0;
    const lock_start_ns = if (bench_profile) platform_time.monotonicNs() else 0;
    const search_access = self.beginDenseSearchAccess(req);
    if (bench_profile) lock_wait_ns = platform_time.monotonicNs() - lock_start_ns;
    defer self.endDenseSearchAccess(search_access);
    // beginDenseSearchAccess either owns apply-shared or a catalog lease
    // drained by portable activation. Validate only after that ownership is
    // established so publication cannot slip between the check and use.
    try self.enforcePortableRuntimeGate();
    const generation_start_ns = if (bench_profile) platform_time.monotonicNs() else 0;
    const snapshot_req = try self.searchRequestAtCurrentIdentityGeneration(req);
    if (bench_profile) generation_ns = platform_time.monotonicNs() - generation_start_ns;
    const search_start_ns = if (bench_profile) platform_time.monotonicNs() else 0;
    const result = try searchLockedWithExecutionContext(self, alloc, snapshot_req, exec_ctx);
    if (bench_profile) {
        locked_search_ns = platform_time.monotonicNs() - search_start_ns;
        std.log.info(
            "antfly_bench_db_search_wrapper total_us={d} generation_us={d} lock_wait_us={d} locked_search_us={d} published_dense={}",
            .{ (platform_time.monotonicNs() - total_start_ns) / 1000, generation_ns / 1000, lock_wait_ns / 1000, locked_search_ns / 1000, search_access == .published },
        );
    }
    return .{ .request = snapshot_req, .result = result };
}

pub fn searchLocked(self: *DB, alloc: Allocator, req: types.SearchRequest) !types.SearchResult {
    return try searchLockedWithExecutionContext(self, alloc, try self.searchRequestAtCurrentIdentityGeneration(req), .{});
}

pub fn searchRequestAtCurrentIdentityGeneration(self: *DB, req: types.SearchRequest) !types.SearchRequest {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, req });
    var snapshot_req = req;
    snapshot_req.identity_read_generation = try self.currentIdentityReadGenerationForRequest(snapshot_req.identity_read_generation);
    try self.validateResolvedDocFilterWireContext(snapshot_req);
    return snapshot_req;
}

pub fn searchLockedWithExecutionContext(
    self: *DB,
    alloc: Allocator,
    req: types.SearchRequest,
    exec_ctx: types.ExecutionContext,
) !types.SearchResult {
    if ((req.query == .graph or req.graph_queries.len != 0 or req.graph_metric_queries.len != 0 or req.graph_metric_rerank != null or req.graph_query_transport != null) and
        !self.core.index_manager.graphRetirementAdmissionOpen())
        return error.IntegrityTopologyBusy;
    // Validate canonical collapse-and-expand work before executing the
    // primary selection query. This protects direct storage callers and
    // internal worker envelopes in addition to the public API boundary.
    if (!types.canonicalHierarchyExecutionWithinBudget(req)) return error.InvalidQueryRequest;
    var identity_prefix_filter = try self.searchRequestWithIdentityPrefixFilterAlloc(req);
    defer identity_prefix_filter.deinit();
    return try self.searchLockedWithExecutionContextImpl(alloc, identity_prefix_filter.req, exec_ctx, true, null);
}

pub fn searchLockedWithExecutionContextImpl(self: *DB, alloc: Allocator, req: types.SearchRequest, exec_ctx: types.ExecutionContext, externalize_artifact_ids: bool, dense_profile_sink: ?*db_query_search.DenseSearchProfile) !types.SearchResult {
    // Trusted whole-source hydration is a primary read, not a ranked live
    // relation. Memory/control/support rows must agree with point lookup
    // even when their ordinary search visibility differs. The outer read
    // lease and routed read barrier still own admission and consistency.
    if (req.document_authority_mode == .lookup and req.include_stored and req.query == .match_all and req.filter_doc_ids_positive and req.filter_doc_ids.len <= 64 and req.index_name == null and req.fields.len == 0 and req.include_all_fields and req.offset == 0 and req.limit >= req.filter_doc_ids.len and req.full_text == null and req.full_text_queries.len == 0 and req.dense == null and req.dense_queries.len == 0 and req.sparse == null and req.sparse_queries.len == 0 and req.graph_queries.len == 0 and req.filter_query_json.len == 0 and req.exclusion_query_json.len == 0 and req.resolved_doc_filter == null and req.exclude_doc_ids.len == 0 and req.filter_prefix.len == 0 and req.order_by.len == 0 and !req.count_only) {
        return self.hydratePrimaryDocuments(alloc, req);
    }
    if (req.document_authority_mode == .lookup) return error.InvalidQueryRequest;
    var request = req;
    // A caller cannot promote a mutable DB into an immutable cache scope.
    request.value_search_scope = self.retained_value_scope;
    if (requestHasLogicalValues(req)) if (self.core.textIndexEntry(req.index_name)) |entry| {
        // A declared mapping is not a completeness proof. Do not let a
        // lagging or partial projection exclude a newer primary value.
        if (!entry.coversPrimaryDocuments() or entry.backfill_active.load(.acquire) != 0) return error.IndexRebuilding;
        // Retained/restored owners may have a persisted checkpoint newer
        // than a newly attached worker's in-memory observation.
        var applied = try self.core.loadAppliedSequence(alloc, entry.config.name);
        if (self.executor.appliedSequence(entry.config.name)) |live| applied = @max(applied, live);
        if (applied < self.core.nextDerivedSequence() and self.retained_remote_query_lease != null) {
            if (self.primary_lsm_storage) |storage| {
                const checkpoint_path = try apply_state.checkpointPathAlloc(alloc, self.core.path);
                defer alloc.free(checkpoint_path);
                const retained = apply_state.loadProjectionCheckpointFromStorage(alloc, storage, checkpoint_path, entry.config.name) catch |err| switch (err) {
                    error.FileNotFound => null,
                    else => return err,
                };
                if (retained) |checkpoint| applied = @max(applied, checkpoint.applied_sequence);
            }
        }
        // The global tail also includes work for unrelated projections.
        // Match the existing readiness/debt contract: inspect replay hints
        // only on the slow path where a global sequence gap exists.
        const target = if (applied >= self.core.nextDerivedSequence()) applied else try self.probeDerivedReplayTargetSequence(
            alloc,
            self.core.replaySource(),
            .{ .name = entry.config.name, .kind = .full_text },
            applied,
        );
        if (applied < target) {
            // Empty replicated batches can advance a text replay hint
            // without producing an index commit/checkpoint. Require both
            // source and index emptiness; a stale index with deleted rows
            // must still fail closed, including under boolean negation.
            const primary_count = try range_cardinality.loadOrProveEmpty(alloc, self.core.store);
            const empty_projection = if (primary_count != null and primary_count.? == 0) empty: {
                const empty_snapshot = entry.persistent.acquireSnapshot();
                defer empty_snapshot.release();
                break :empty empty_snapshot.liveDocCount() == 0;
            } else false;
            if (!empty_projection) {
                std.log.debug("logical value index checkpoint behind index={s} applied={d} target={d}", .{ entry.config.name, applied, target });
                return error.IndexRebuilding;
            }
        }
    };
    var logical_planning = std.heap.ArenaAllocator.init(alloc);
    defer logical_planning.deinit();
    if (self.core.schema) |schema| {
        var changed = false;
        for (req.order_by) |field| if (schema_mod.resolveLogicalDocValues(schema, field.field)) |resolved| {
            if (!std.mem.eql(u8, resolved.field, field.field)) changed = true;
        };
        if (changed) {
            const fields = try logical_planning.allocator().dupe(types.SortField, req.order_by);
            for (fields) |*field| if (schema_mod.resolveLogicalDocValues(schema, field.field)) |resolved| {
                field.field = resolved.field;
            };
            request.order_by = fields;
        }
    }
    var value_work: @import("../../search/value_query.zig").Work = .{};
    defer value_work.deinit(alloc);
    if (request.value_search_work == null) request.value_search_work = &value_work;
    var projection: ?document_query.SpanProjectionPlan = null;
    defer if (projection) |*plan| plan.deinit();
    const projection_opts: types.LookupOptions = .{ .fields = req.fields, .include_all_fields = req.include_all_fields };
    const shared_matches = if (req.document_projection_plan) |shared| shared.matches(projection_opts) else false;
    if (req.include_stored and !req.count_only and req.limit != 0 and !req.defer_stored_projection and db_query_projection.shouldProjectSearchStored(req) and !shared_matches) {
        projection = try document_query.SpanProjectionPlan.init(alloc, projection_opts);
        if (projection) |*plan| request.document_projection_plan = .{ .plan = plan, .fields = req.fields, .include_all_fields = req.include_all_fields };
    }
    var result = try searchLockedWithExecutionContextRaw(self, alloc, request, exec_ctx, externalize_artifact_ids, dense_profile_sink);
    errdefer result.deinit();
    if (req.profile and req.value_search_work == null) {
        result.value_search_profile_json = try value_work.profileAlloc(alloc);
    }
    try self.attachDistributedDocumentAuthority(alloc, req, &result);
    return result;
}

pub fn searchLockedWithExecutionContextRaw(
    self: *DB,
    alloc: Allocator,
    req: types.SearchRequest,
    exec_ctx: types.ExecutionContext,
    externalize_artifact_ids: bool,
    dense_profile_sink: ?*db_query_search.DenseSearchProfile,
) !types.SearchResult {
    const execution_req = directSingleVectorRequest(req) orelse req;
    if (execution_req.hierarchy_children != null) {
        var children = try self.navigateHierarchyChildren(alloc, execution_req);
        errdefer children.deinit();
        if (externalize_artifact_ids) try externalizeSearchResultArtifactIds(alloc, &children);
        return children;
    }
    var selection_req = types.canonicalGroupedMatchSelectionRequest(execution_req);
    if (execution_req.graph_metric_rerank) |rerank| {
        try types.validateGraphMetricRerankWindow(rerank, execution_req.offset, execution_req.limit);
        selection_req.offset = 0;
        selection_req.limit = types.graphMetricRerankCandidateCount(rerank, execution_req.offset, execution_req.limit);
    }
    if (searchRequestRequiresComposedSearch(selection_req)) {
        var composed = try self.searchComposed(alloc, selection_req, exec_ctx, dense_profile_sink);
        errdefer composed.deinit();
        try self.populateCanonicalGroupedMatches(alloc, execution_req, exec_ctx, &composed);
        try self.applyGraphMetricRerank(&composed, execution_req);
        try attachSearchHighlights(self, alloc, execution_req, &composed);
        if (externalize_artifact_ids) try externalizeSearchResultArtifactIds(alloc, &composed);
        return composed;
    }

    const has_primary = selection_req.full_text != null or selection_req.dense != null or selection_req.sparse != null or !db_query_search.isDefaultMatchAll(selection_req.query) or (selection_req.graph_queries.len == 0 and selection_req.graph_metric_queries.len == 0);

    var base = if (!has_primary and (selection_req.graph_queries.len > 0 or selection_req.graph_metric_queries.len > 0))
        try db_query_search.emptySearchResult(alloc)
    else if (selection_req.full_text) |text|
        try self.searchTextQuery(alloc, selection_req, text)
    else if (selection_req.dense) |dense|
        try self.searchDenseWithProfileSink(alloc, selection_req, dense, dense_profile_sink)
    else if (selection_req.sparse) |sparse|
        try self.searchSparse(alloc, selection_req, sparse)
    else switch (selection_req.query) {
        .match_none,
        .match_all,
        .phrase,
        .multi_phrase,
        .term,
        .fuzzy,
        .numeric_range,
        .date_range,
        .doc_id,
        .bool_field,
        .geo_distance,
        .geo_bbox,
        .term_range,
        .ip_range,
        .geo_shape,
        .match,
        .match_phrase,
        .prefix,
        .wildcard,
        .regexp,
        => try self.searchText(alloc, selection_req),
        .dense_knn => |dense| try self.searchDenseWithProfileSink(alloc, selection_req, dense, dense_profile_sink),
        .sparse_knn => |sparse| try self.searchSparse(alloc, selection_req, sparse),
        .graph => |graph| try self.searchGraph(alloc, selection_req, graph, null),
    };
    errdefer base.deinit();
    try self.populateCanonicalGroupedMatches(alloc, execution_req, exec_ctx, &base);
    try attachSearchHighlights(self, alloc, execution_req, &base);

    if (execution_req.graph_metric_queries.len > 0) {
        base.graph_metric_results = try self.executeGraphMetricQueries(alloc, execution_req.graph_metric_queries, execution_req.cancellation);
    }
    try self.applyGraphMetricRerank(&base, execution_req);

    if (execution_req.graph_queries.len == 0) {
        if (externalize_artifact_ids) try externalizeSearchResultArtifactIds(alloc, &base);
        return base;
    }

    base.graph_results = try self.executeGraphQueries(alloc, execution_req, execution_req.graph_queries, base.hits, base.total_hits);
    try self.applyGraphExpandStrategy(alloc, &base, execution_req.expand_strategy);
    if (externalize_artifact_ids) try externalizeSearchResultArtifactIds(alloc, &base);
    return base;
}

/// Attach `_highlights` to hits that carry stored source when the request
/// asked for them. This runs on the node that owns the stored documents;
/// a distributed coordinator only relays what its shards computed.
pub fn attachSearchHighlights(self: *DB, alloc: Allocator, req: types.SearchRequest, result: *types.SearchResult) !void {
    const options = req.highlight orelse return;
    // Deferred unit grouping carries a private revision envelope in
    // `_source`, not the document.
    if (req.defer_hierarchy_child_hydration) return;
    if (result.hits.len == 0) return;

    if (req.full_text == null and req.full_text_queries.len == 0) return;

    // `_source` may already be projected down to the requested fields (or
    // omitted entirely), so highlight from the unprojected documents in
    // that case rather than from whatever survived the projection.
    const needs_full_documents = !req.include_stored or (!req.include_all_fields and !req.defer_stored_projection);
    var sources: ?[]?[]u8 = null;
    defer if (sources) |items| freeOptionalOwnedBytes(alloc, items);
    if (needs_full_documents) {
        const keys = try alloc.alloc([]const u8, result.hits.len);
        defer alloc.free(keys);
        for (result.hits, keys) |hit, *key| key.* = hit.id;
        const loaded = try loadStoredSearchDocumentsMany(self, alloc, keys, null);
        sources = loaded;
        // Graph hydration can surface hits that live in another table;
        // their ids must not be resolved against this table's store.
        for (result.hits, loaded) |hit, *source| {
            if (hit.source_table == null) continue;
            if (source.*) |owned| alloc.free(owned);
            source.* = null;
        }
    }

    var indexed_queries = std.ArrayListUnmanaged(db_query_search.HighlightQuery).empty;
    defer indexed_queries.deinit(alloc);
    var locked_entries = std.ArrayListUnmanaged(*index_manager_mod.IndexManager.TextIndex).empty;
    defer {
        for (locked_entries.items) |entry| entry.unlockAnalysisShared();
        locked_entries.deinit(alloc);
    }
    if (req.full_text_queries.len > 0) {
        // Composed search executes named queries on their own indexes.
        for (req.full_text_queries) |named| {
            const entry = self.core.textIndexEntry(named.index_name) orelse continue;
            var already_locked = false;
            for (locked_entries.items) |locked| {
                if (locked == entry) already_locked = true;
            }
            if (!already_locked) {
                try locked_entries.append(alloc, entry);
                entry.lockAnalysisShared();
            }
            try indexed_queries.append(alloc, .{
                .query = named.query,
                .text_analysis = entry.text_analysis,
                .runtime_schema = entry.runtime_schema,
            });
        }
    } else if (req.full_text) |query| {
        const entry = self.core.textIndexEntry(req.primary_text_index_name orelse req.index_name) orelse
            self.core.textIndexEntry(null) orelse return;
        try locked_entries.append(alloc, entry);
        entry.lockAnalysisShared();
        try indexed_queries.append(alloc, .{
            .query = query,
            .text_analysis = entry.text_analysis,
            .runtime_schema = entry.runtime_schema,
        });
    }
    if (indexed_queries.items.len == 0) return;
    try db_query_search.attachHighlightsWithIndexQueries(
        alloc,
        options,
        indexed_queries.items,
        result.hits,
        sources,
    );
}

pub fn chunkHitFromSearchHitAlloc(alloc: Allocator, hit: types.SearchHit) !types.ChunkHit {
    var out = types.ChunkHit{
        .id = try alloc.dupe(u8, hit.id),
        .score = hit.score,
        .distance = hit.distance,
    };
    errdefer out.deinit(alloc);
    out.stored_data = if (hit.stored_data) |data| try alloc.dupe(u8, data) else null;
    out.ancestor_source_data = if (hit.ancestor_source_data) |data| try alloc.dupe(u8, data) else null;
    out.ancestor_unit_data = if (hit.ancestor_unit_data) |data| try alloc.dupe(u8, data) else null;
    out.artifact_ref = if (hit.artifact_ref) |artifact_ref| try artifact_ref.clone(alloc) else null;
    return out;
}

pub fn searchRequestRequiresComposedSearch(req: types.SearchRequest) bool {
    if (req.full_text_queries.len > 0 or req.dense_queries.len > 0 or req.sparse_queries.len > 0) return true;

    var base_result_sets: u32 = 0;
    if (req.full_text != null) base_result_sets += 1;
    if (req.dense != null) base_result_sets += 1;
    if (req.sparse != null) base_result_sets += 1;
    switch (req.query) {
        .dense_knn => base_result_sets += 1,
        .sparse_knn => base_result_sets += 1,
        else => {},
    }
    return base_result_sets > 1;
}

pub fn searchComposed(
    self: *DB,
    alloc: Allocator,
    req: types.SearchRequest,
    exec_ctx: types.ExecutionContext,
    dense_profile_sink: ?*db_query_search.DenseSearchProfile,
) !types.SearchResult {
    _ = exec_ctx;
    const metric_name = composedQueryMetricIndexName(req);
    const start_ns = platform_time.monotonicNs();
    errdefer if (db_query_search.peekLastSortRejectionDiagnostic()) |diagnostic| {
        db_query_metrics.observeSortRejection(metric_name, .search, platform_time.monotonicNs() -| start_ns, diagnostic.reason, diagnostic.detail);
    };
    const result = try db_query_search.searchComposed(alloc, req, .{
        .ctx = self,
        .resolve_structured_doc_filter = resolveStructuredDocFilterForComposedCallback,
        .resolve_structured_text_doc_filter = resolveStructuredTextDocFilterForComposedCallback,
        .search_text_query = searchTextQueryCallback,
        .search_text = searchTextComposedCallback,
        .search_dense = searchDenseComposedCallback,
        .search_dense_profiled = searchDenseComposedProfiledCallback,
        .dense_profile_sink = dense_profile_sink,
        .search_sparse = searchSparseComposedCallback,
        .clone_named_set = cloneNamedSetCallback,
        .fuse_named_sets = fuseNamedSetsCallback,
        .resolve_hits_to_doc_set = resolveSearchHitsToDocSetCallback,
        .attach_graph_results = attachGraphResultsCallback,
    });
    if (result.sort_profile != null) {
        db_query_metrics.observeSortProfile(metric_name, .search, platform_time.monotonicNs() -| start_ns, result.sort_profile);
    }
    return result;
}

pub fn searchText(self: *DB, alloc: Allocator, req: types.SearchRequest) !types.SearchResult {
    return try db_query_search.searchText(alloc, req, .{
        .ctx = self,
        .func = searchTextQueryCallback,
    });
}

pub fn searchTextQuery(self: *DB, alloc: Allocator, req: types.SearchRequest, text_query: types.TextQuery) !types.SearchResult {
    if (self.core.textIndexEntry(req.index_name) == null and (textQueryHasLogicalValues(text_query) or (if (req.filter_text) |filter| textQueryHasLogicalValues(filter) else false) or (if (req.exclusion_text) |filter| textQueryHasLogicalValues(filter) else false))) {
        return self.searchLogicalValuesWithoutTextIndex(alloc, req, text_query);
    }

    // A primary-document projection may also contain generated members.
    // For a plain source-parent match_all those members cannot add a parent
    // or improve its constant score. Read the primary population directly,
    // before resolving filters in the member index's document-number space.
    if (text_query == .match_all and primaryParentMatchAllEligible(req)) {
        if (self.core.textIndexEntry(req.index_name)) |entry| {
            if (entry.coversPrimaryDocuments() and
                try self.core.textIndexIsChunkBacked(alloc, entry.config.name))
            {
                var executor = self.matchAllExecutor();
                executor.bounded_unordered_page = true;
                var primary_req = req;
                primary_req.index_name = entry.config.name;
                // The population proof replaces the retrieval predicate.
                // Leaving it here would resolve match_all as a full-text
                // filter and enumerate the very members we eliminated.
                primary_req.full_text = null;
                const metric_name = self.textQueryMetricIndexName(req);
                const start_ns = platform_time.monotonicNs();
                errdefer observeSearchFailureMetric(metric_name, .search, platform_time.monotonicNs() -| start_ns);
                const result = try db_query_search.searchMatchAll(alloc, primary_req, executor);
                db_query_metrics.observeSortProfile(metric_name, .search, platform_time.monotonicNs() -| start_ns, result.sort_profile);
                return result;
            }
        }
    }
    const needs_algebraic_doc_filter = req.doc_filter_bindings.len > 0 or req.require_algebraic_filter_resolution;
    var algebraic_filter = if (needs_algebraic_doc_filter)
        try self.searchRequestWithAlgebraicDocFilterAlloc(req)
    else
        AlgebraicDocFilterRequest{ .req = req };
    defer algebraic_filter.deinit();
    var execution_req = algebraic_filter.req;
    // Chunk members are separate full-text documents from their parent
    // row and have their own doc numbers, so resolving filter_query/
    // exclusion_query into parent-row doc-number sets here can never
    // match a chunk (positive clause: 0 hits) or exclude one (negative
    // clause: no effect). Leave the JSON filters unresolved for
    // chunk-backed member/chunk-mode text queries so they survive into
    // applyStoredSearchPatternFilters, which matches them against the
    // resolved parent row instead (issue #931).
    const chunk_backed_for_filter = try self.core.textIndexIsChunkBacked(alloc, execution_req.index_name);
    const member_mode_for_filter = execution_req.return_mode == .member or execution_req.return_mode == .chunk;
    const suppress_text_doc_num_filter = chunk_backed_for_filter and member_mode_for_filter;
    var lifted_bool_filter = ChunkBoolFilterLowering{};
    defer lifted_bool_filter.deinit(alloc);
    if (suppress_text_doc_num_filter) {
        // Discard the composed-search doc-number/ordinal resolution a
        // shared filter step may already have attached upstream
        // (searchComposed resolves these unconditionally before
        // dispatching to per-arm searches); it would be just as wrong
        // here. resolved_doc_filter -- the generic doc-identity form
        // filter_prefix resolves into (searchRequestWithIdentityPrefixFilterAlloc)
        // -- is left intact: db_query_search.searchTextQuery itself
        // skips using it natively for a chunk-backed member/chunk-mode
        // query and instead carries it through to
        // applyStoredSearchPatternFilters' parent-aware matcher.
        execution_req.resolved_text_doc_filter = null;
        // query.bool.filter/must_not (native TextQuery filter_text/
        // exclusion_text) are resolved the same wrong way: against the
        // chunk-backed index's own doc-number/ordinal space, which never
        // covers a parent-only field. Lift representable clauses into
        // filter_query_json/exclusion_query_json instead, so they reach
        // applyStoredSearchPatternFilters' parent-aware matcher (issue
        // #931). Unrepresentable shapes fail closed with a clear error
        // rather than silently matching nothing or everything.
        lifted_bool_filter = try liftChunkBoolFilterClausesAlloc(
            alloc,
            execution_req.filter_text,
            execution_req.exclusion_text,
            execution_req.filter_query_json,
            execution_req.exclusion_query_json,
        );
        if (lifted_bool_filter.filter_query_json) |json| execution_req.filter_query_json = json;
        if (lifted_bool_filter.exclusion_query_json) |json| execution_req.exclusion_query_json = json;
        if (execution_req.filter_text != null) execution_req.filter_text = null;
        if (execution_req.exclusion_text != null) execution_req.exclusion_text = null;
    }
    const maybe_resolved_text_filter = if (suppress_text_doc_num_filter)
        null
    else
        try db_query_search.resolveStructuredTextDocNumFilterForComposedAlloc(alloc, execution_req, .{
            .load_stored = loadStoredSearchDocumentCallback,
            .load_many_stored_values = loadStoredSearchDocumentManyCallback,
            .open_logical_sources = openStoredLogicalSourcesCallback,
            .ctx = self,
            .text_index_entry = textIndexEntryCallback,
            .resolve_doc_set_doc_ids = resolveDocSetDocIdsCallback,
            .resolve_doc_ids_to_doc_set = resolveDocIdsToDocSetCallback,
            .live_filter_doc_set = liveFilterDocSetCallback,
            .all_docs_visible = allDocsVisibleCallback,
            .project_ordinals_to_doc_ids = false,
            .identity_read_generation = execution_req.identity_read_generation,
        });
    var resolved_text_filter_storage: db_query_search.ResolvedTextDocNumFilter = undefined;
    var has_resolved_text_filter = false;
    defer {
        if (has_resolved_text_filter) resolved_text_filter_storage.deinit(alloc);
    }
    if (maybe_resolved_text_filter) |filter| {
        resolved_text_filter_storage = filter;
        has_resolved_text_filter = true;
        execution_req.resolved_text_doc_filter = &resolved_text_filter_storage;
        execution_req.filter_query_json = "";
        execution_req.exclusion_query_json = "";
    }
    try self.proveTextQueryAccessPaths(execution_req.index_name, text_query);
    const metric_name = self.textQueryMetricIndexName(execution_req);
    const start_ns = platform_time.monotonicNs();
    errdefer observeSearchFailureMetric(metric_name, .search, platform_time.monotonicNs() -| start_ns);
    const result = try db_query_search.searchTextQuery(alloc, execution_req, text_query, .{
        .filter_candidate_presence = true,
        .ctx = self,
        .text_index_entry = textIndexEntryCallback,
        .text_index_is_chunk_backed = textIndexIsChunkBackedCallback,
        .text_index_supports_unit_grouping = textIndexSupportsUnitGroupingCallback,
        .search_match_all = searchMatchAllCallback,
        .project_stored_search = projectStoredBytesForSearchCallback,
        .load_stored = loadStoredSearchDocumentCallback,
        .load_many_stored_values = loadStoredSearchDocumentManyCallback,
        .open_logical_sources = openStoredLogicalSourcesCallback,
        .load_projected_documents = loadProjectedSearchDocumentManyCallback,
        .is_expired_key = isNonvisibleStoredSearchKeyCallback,
        .resolve_doc_set_doc_ids = resolveDocSetDocIdsCallback,
        .resolve_doc_ids_to_doc_set = resolveDocIdsToDocSetCallback,
        .live_filter_doc_set = liveFilterDocSetCallback,
        .live_filter_doc_set_controlled = textLiveFilterDocSetControlledCallback,
        // An absent legacy summary is unknown, not a reason to scan the
        // primary population before searching selective postings.
        .all_docs_visible = allDocsVisibleFastCallback,
        .requires_full_candidate_visibility_filter = requiresFullCandidateVisibilityFilterCallback,
        .nonvisible_doc_set = nonVisibleDocSetCallback,
        .postprocess = postprocessTextSearchResultCallback,
    });
    db_query_metrics.observeSortProfile(metric_name, .search, platform_time.monotonicNs() -| start_ns, result.sort_profile);
    return result;
}

pub fn isSearchAsYouTypeGeneratedFieldName(field: []const u8) bool {
    return std.mem.endsWith(u8, field, "._2gram") or
        std.mem.endsWith(u8, field, "._3gram") or
        std.mem.endsWith(u8, field, "._index_prefix");
}

pub fn collectSearchRequestTextStats(self: *DB, alloc: Allocator, req: types.SearchRequest) ![]const distributed_stats_mod.TextFieldStats {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc, req });
    var row_policy_lease = try self.local_execution.row_policy_gate.enterUnsupportedSearch();
    defer row_policy_lease.release();
    try self.lockApplySharedForPortableRuntime();
    defer self.core.unlockApplyShared();
    return try db_query_search.collectSearchRequestTextStats(alloc, try self.searchRequestAtCurrentIdentityGeneration(req), .{
        .ctx = self,
        .text_index_entry = textIndexEntryCallback,
    });
}

pub fn preflightSearchRequest(self: *DB, alloc: Allocator, req: types.SearchRequest, max_work: u32) !db_query_search.RuntimePreflightSummary {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc, req, max_work });
    return try self.preflightSearchRequestWithExecutionContext(alloc, req, max_work, .{});
}

pub fn preflightSearchRequestWithExecutionContext(
    self: *DB,
    alloc: Allocator,
    req: types.SearchRequest,
    max_work: u32,
    exec_ctx: types.ExecutionContext,
) !db_query_search.RuntimePreflightSummary {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc, req, max_work, exec_ctx });
    return try self.collectPlanningStatsWithExecutionContext(alloc, req, max_work, exec_ctx);
}

pub fn planningStatsProviderCollectSearchRequestStats(
    ptr: *anyopaque,
    alloc: Allocator,
    req: types.SearchRequest,
    max_work: u32,
) !planning_stats_mod.PlanningStatsSummary {
    const self: *DB = @ptrCast(@alignCast(ptr));
    return try self.collectPlanningStats(alloc, req, max_work);
}

pub fn planningStatsSearchRequestCallback(
    ptr: *anyopaque,
    alloc: Allocator,
    req: types.SearchRequest,
) !types.SearchResult {
    const self: *DB = @ptrCast(@alignCast(ptr));
    return try searchLocked(self, alloc, req);
}

pub fn searchMatchAllCallback(
    ctx: ?*anyopaque,
    alloc: Allocator,
    req: types.SearchRequest,
) anyerror!types.SearchResult {
    const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
    return try self.searchMatchAll(alloc, req);
}

pub fn projectStoredBytesForSearchCallback(
    ctx: ?*anyopaque,
    alloc: Allocator,
    req: types.SearchRequest,
    doc_key: []const u8,
    raw: []const u8,
) anyerror![]u8 {
    const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
    return try projectStoredBytesForSearch(self, alloc, req, doc_key, raw);
}

pub fn postprocessTextSearchResultCallback(
    ctx: ?*anyopaque,
    alloc: Allocator,
    req: types.SearchRequest,
    raw: types.SearchResult,
    chunk_backed: bool,
) anyerror!types.SearchResult {
    const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
    return try db_query_result_shape.postprocessTextSearchResult(alloc, req, raw, chunk_backed, .{
        .ctx = self,
        .is_visible = isVisibleSearchHitCallback,
        .filter_visible_many = filterStoredSearchCandidatesManyCallback,
        .resolve_parent_id = resolveChunkParentIdCallback,
        .load_parent_stored = loadParentStoredForSearchCallback,
        .load_stored = loadStoredSearchDocumentCallback,
        .resolve_doc_set_doc_ids = resolveDocSetDocIdsCallback,
        .resolve_doc_ids_to_doc_set = resolveDocIdsToDocSetCallback,
        .load_many_parent_stored = loadParentStoredForSearchManyCallback,
        .load_many_stored = loadStoredSearchDocumentManyCallback,
        .load_projected_stored = loadProjectedSearchDocumentCallback,
        .load_many_projected_stored = loadProjectedSearchDocumentManyCallback,
    });
}

pub fn searchTextQueryCallback(
    ctx: ?*anyopaque,
    alloc: Allocator,
    req: types.SearchRequest,
    text_query: types.TextQuery,
) anyerror!types.SearchResult {
    const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
    return try self.searchTextQuery(alloc, req, text_query);
}

pub fn searchTextComposedCallback(
    ctx: ?*anyopaque,
    alloc: Allocator,
    req: types.SearchRequest,
) anyerror!types.SearchResult {
    const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
    return try self.searchText(alloc, req);
}

pub fn searchSparseComposedCallback(
    ctx: ?*anyopaque,
    alloc: Allocator,
    req: types.SearchRequest,
    sparse: types.SparseKnnQuery,
) anyerror!types.SearchResult {
    const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
    return try self.searchSparse(alloc, req, sparse);
}

pub fn hbcSearchCallback(
    ctx: ?*anyopaque,
    entry: *index_manager_mod.IndexManager.DenseIndex,
    req: vectorindex_mod.SearchRequest,
) anyerror!vectorindex_mod.SearchResults {
    const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
    return self.core.index_manager.searchDenseEntryWithRequest(entry, req) catch |err| switch (err) {
        error.IncompletePublishedSnapshot => {
            if (!try self.persistIncompleteHbcSnapshotRepair(entry)) return err;
            return error.IndexRebuilding;
        },
        else => return err,
    };
}

pub fn hbcSearchProfiledCallback(
    ctx: ?*anyopaque,
    entry: *index_manager_mod.IndexManager.DenseIndex,
    req: vectorindex_mod.SearchRequest,
) anyerror!vectorindex_mod.ProfiledSearchResults {
    const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
    return self.core.index_manager.searchDenseEntryProfiledWithRequest(entry, req) catch |err| switch (err) {
        error.IncompletePublishedSnapshot => {
            if (!try self.persistIncompleteHbcSnapshotRepair(entry)) return err;
            return error.IndexRebuilding;
        },
        else => return err,
    };
}

pub fn searchSparse(self: *DB, alloc: Allocator, req: types.SearchRequest, sparse: types.SparseKnnQuery) !types.SearchResult {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc, req, sparse });
    if (builtin.os.tag == .freestanding) return error.UnsupportedPlatform;
    const metric_name = self.sparseQueryMetricIndexName(req);
    const start_ns = platform_time.monotonicNs();
    errdefer observeSearchFailureMetric(metric_name, .vector, platform_time.monotonicNs() -| start_ns);
    const bench_profile = benchQueryProfileEnabled();
    const total_start_ns = if (bench_profile) platform_time.monotonicNs() else 0;
    var algebraic_ns: u64 = 0;
    var prove_ns: u64 = 0;
    var inner_ns: u64 = 0;
    const algebraic_start_ns = if (bench_profile) platform_time.monotonicNs() else 0;
    var algebraic_filter = try self.searchRequestWithAlgebraicDocFilterAlloc(req);
    defer algebraic_filter.deinit();
    if (bench_profile) algebraic_ns = platform_time.monotonicNs() - algebraic_start_ns;
    const prove_start_ns = if (bench_profile) platform_time.monotonicNs() else 0;
    try self.proveVectorSearchAccessPath(algebraic_filter.req.index_name, .sparse_vector, hasNativeDocIdConstraints(algebraic_filter.req));
    if (bench_profile) prove_ns = platform_time.monotonicNs() - prove_start_ns;
    const inner_start_ns = if (bench_profile) platform_time.monotonicNs() else 0;
    const result = try db_query_search.searchSparse(alloc, algebraic_filter.req, sparse, .{
        .score_spill = if (self.backend_runtime.filesystemIo()) |io| .{ .io = io, .directory = "/tmp", .resource_manager = self.core.index_manager.resource_manager } else null,
        .filter_candidate_presence = true,
        .ctx = self,
        .text_index_entry = textIndexEntryCallback,
        .sparse_index = sparseIndexCallback,
        .resolve_doc_set_doc_ids = resolveDocSetDocIdsCallback,
        .resolve_doc_ids_to_doc_set = resolveDocIdsToDocSetCallback,
        .live_filter_doc_set = liveFilterDocSetCallback,
        .nonvisible_doc_set = nonVisibleDocSetCallback,
        .lookup_doc_nums_for_ordinals = sparseDocNumsForOrdinalsCallback,
        .lookup_doc_ordinal = lookupLiveDocOrdinalNoLockCallback,
        .lookup_doc_ordinals = lookupLiveDocOrdinalsNoLockCallback,
        .load_projected_document = loadRequiredProjectedSearchDocumentCallback,
        .load_projected_documents = loadProjectedSearchDocumentManyCallback,
        .postprocess = postprocessVectorSearchResultCallback,
    });
    if (bench_profile) {
        inner_ns = platform_time.monotonicNs() - inner_start_ns;
        std.log.info(
            "antfly_bench_db_sparse_wrapper total_us={d} algebraic_us={d} prove_us={d} inner_us={d}",
            .{ (platform_time.monotonicNs() - total_start_ns) / 1000, algebraic_ns / 1000, prove_ns / 1000, inner_ns / 1000 },
        );
    }
    db_query_metrics.observeSortProfile(metric_name, .vector, platform_time.monotonicNs() -| start_ns, result.sort_profile);
    return result;
}

pub fn proveVectorSearchAccessPath(self: *DB, index_name: ?[]const u8, layout: algebraic_mod.ir.PhysicalLayout, constrained: bool) !void {
    // Native vector routes use the access-path owner directly and may
    // bypass the callback lookup. An installed physical index still has
    // to satisfy its durable admission gate before it can serve.
    try self.failIfIndexQuarantined(index_name);
    const selected_path = switch (layout) {
        .dense_vector => self.core.index_manager.denseVectorAccessPath(index_name),
        .sparse_vector => self.core.index_manager.sparseVectorAccessPath(index_name),
        else => null,
    };
    const access_path = selected_path orelse {
        try self.failIfIndexQuarantined(index_name);
        return error.IndexNotFound;
    };
    var planned = (try algebraic_mod.planner.planVectorSearchTensorProgramAlloc(self.alloc, access_path.owner, layout, constrained)) orelse return error.InvalidIndexConfig;
    defer planned.deinit(self.alloc);
    if (planned.access_paths.len != 1 or
        planned.access_paths[0].layout != layout or
        !std.mem.eql(u8, planned.access_paths[0].owner, access_path.owner) or
        !std.mem.eql(algebraic_mod.ir.Dimension, planned.access_paths[0].output_dims, access_path.output_dims))
    {
        return error.InvalidIndexConfig;
    }
    if (!algebraic_mod.ir.vectorSearchProgramMatchesTarget(planned.asProgram(), access_path.owner, layout, constrained)) return error.InvalidIndexConfig;
}

/// Resolve document-key prefixes once, before composed search fans out to
/// dense, sparse, and full-text lanes. Artifact-backed indexes use encoded
/// member identities internally, so forwarding the raw prefix to a physical
/// backend is both incorrect and inconsistent across index kinds.
pub fn searchRequestWithIdentityPrefixFilterAlloc(self: *DB, req: types.SearchRequest) !IdentityPrefixFilterRequest {
    if (req.filter_prefix.len == 0) return .{ .req = req };

    var filter = if (req.resolved_doc_filter) |ptr|
        try self.normalizeResolvedDocFilterDocKeysNoLockAtGenerationAlloc(
            self.alloc,
            @ptrCast(@alignCast(ptr)),
            req.identity_read_generation,
        )
    else
        doc_set.ResolvedDocFilter{};
    errdefer filter.deinit(self.alloc);

    switch (filter.include) {
        // A partial-coverage identity set cannot be intersected with
        // ordinals. Filtering its canonical document keys directly
        // preserves prefix semantics while allowing downstream projection
        // to use its normal fallback behavior.
        .doc_keys => |keys| {
            var matching = std.ArrayListUnmanaged([]const u8).empty;
            defer matching.deinit(self.alloc);
            for (keys) |key| {
                if (std.mem.startsWith(u8, key, req.filter_prefix)) try matching.append(self.alloc, key);
            }
            const include = try doc_set.cloneDocKeysAlloc(self.alloc, matching.items);
            filter.include.deinit(self.alloc);
            filter.include = include;
        },
        else => {
            var prefix_set = try doc_identity.visibleDocSetForPrefixFromStoreAlloc(
                self.alloc,
                self.core.store,
                req.filter_prefix,
                req.identity_read_generation,
            );
            defer prefix_set.deinit(self.alloc);
            const include = (try doc_set.intersectAlloc(self.alloc, &filter.include, &prefix_set)) orelse
                return error.InvalidDocIdentity;
            filter.include.deinit(self.alloc);
            filter.include = include;
        },
    }

    const resolved_filter = try self.alloc.create(doc_set.ResolvedDocFilter);
    errdefer self.alloc.destroy(resolved_filter);
    resolved_filter.* = filter;
    filter = .{};

    var next = req;
    next.filter_prefix = "";
    next.resolved_doc_filter = resolved_filter;
    next.resolved_doc_filter_owned = false;
    return .{
        .req = next,
        .resolved_doc_filter = resolved_filter,
        .alloc = self.alloc,
    };
}

pub fn searchRequestWithAlgebraicDocFilterAlloc(self: *DB, req: types.SearchRequest) !AlgebraicDocFilterRequest {
    if (req.filter_query_json.len == 0 and req.exclusion_query_json.len == 0) return .{ .req = req };
    // `_id` is the primary identity access path, not a user-provisioned
    // secondary index. Resolve a closed ID union directly even for callers
    // that require exact native resolution (notably canonical MATCH anchor
    // scans). This keeps bounded point-source queries exact on every table
    // while arbitrary predicates still fail closed without index coverage.
    if (req.doc_filter_bindings.len == 0 and
        req.filter_query_json.len > 0 and
        req.exclusion_query_json.len == 0)
    {
        if (try db_query_search.exactStructuredFilterDocIdsAlloc(self.alloc, req.filter_query_json)) |doc_ids| {
            defer freeConstDocIds(self.alloc, doc_ids);
            const resolved_filter = try self.alloc.create(doc_set.ResolvedDocFilter);
            errdefer self.alloc.destroy(resolved_filter);
            resolved_filter.* = try self.resolvedDocFilterForIdsAlloc(true, doc_ids, &.{}, req.identity_read_generation);
            errdefer resolved_filter.deinit(self.alloc);
            var next = req;
            next.filter_query_json = "";
            next.resolved_doc_filter = resolved_filter;
            return .{
                .req = next,
                .resolved_doc_filter = resolved_filter,
                .resolved_doc_filter_alloc = self.alloc,
            };
        }
    }
    if (!req.require_algebraic_filter_resolution and req.doc_filter_bindings.len == 0) {
        if (try searchRequestWithDynamicStructuredDocFilterAlloc(self, req)) |direct| return direct;
    }
    const entry = self.core.index_manager.algebraicIndex(null) orelse {
        self.recordUnsupportedDocSetFilterShape();
        if (req.require_algebraic_filter_resolution) return error.UnsupportedQueryRequest;
        return .{ .req = req };
    };
    entry.index.recordVectorFilterAttempt();
    if (entry.index.hasErrors() or !entry.index.plannerLifecycleReady()) {
        entry.index.recordVectorFilterUnsupported(req.require_algebraic_filter_resolution);
        self.recordUnsupportedDocSetFilterShape();
        if (req.require_algebraic_filter_resolution) return error.UnsupportedQueryRequest;
        return .{ .req = req };
    }
    if (try searchRequestWithDirectAlgebraicDocFilterAlloc(self, req, entry.index)) |direct| return direct;
    var filter_doc_ids: [][]u8 = &.{};
    errdefer entry.index.freeDocIds(filter_doc_ids);
    var exclude_doc_ids: [][]u8 = &.{};
    errdefer entry.index.freeDocIds(exclude_doc_ids);
    var changed = false;
    var filter_json_resolved = false;
    var exclusion_json_resolved = false;
    var filter_supported = req.filter_doc_ids_positive or req.filter_doc_ids.len > 0;
    var filter_bindings = std.ArrayListUnmanaged(@import("algebraic/index.zig").Index.FilterBinding).empty;
    defer {
        for (filter_bindings.items) |*binding| binding.set.deinit(entry.index);
        filter_bindings.deinit(entry.index.alloc);
    }

    if (filter_supported) filter_doc_ids = try dupeAlgebraicDocIds(entry.index.alloc, req.filter_doc_ids);
    if (req.exclude_doc_ids.len > 0) exclude_doc_ids = try dupeAlgebraicDocIds(entry.index.alloc, req.exclude_doc_ids);

    for (req.doc_filter_bindings) |binding| {
        if (binding.name.len == 0 or binding.filter_query_json.len == 0) return error.InvalidArgument;
        for (filter_bindings.items) |existing| {
            if (std.mem.eql(u8, existing.name, binding.name)) return error.InvalidArgument;
        }
        var set = (try entry.index.docIdSetForFilterJsonWithBindingsAlloc(
            self.core.store,
            binding.filter_query_json,
            filter_bindings.items,
        )) orelse {
            entry.index.recordVectorFilterUnsupported(req.require_algebraic_filter_resolution);
            self.recordUnsupportedDocSetFilterShape();
            if (req.require_algebraic_filter_resolution) return error.UnsupportedQueryRequest;
            return .{ .req = req };
        };
        errdefer set.deinit(entry.index);
        try filter_bindings.append(entry.index.alloc, .{
            .name = binding.name,
            .set = set,
        });
        set = .{};
    }

    if (req.filter_query_json.len > 0) {
        if (try entry.index.docIdSetForFilterJsonWithBindingsAlloc(self.core.store, req.filter_query_json, filter_bindings.items)) |set| {
            var owned_set = set;
            defer owned_set.deinit(entry.index);
            if (owned_set.include) |ids| {
                if (filter_supported) {
                    const intersected = try intersectAlgebraicDocIds(entry.index.alloc, filter_doc_ids, ids);
                    entry.index.freeDocIds(filter_doc_ids);
                    filter_doc_ids = intersected;
                } else {
                    filter_doc_ids = try dupeAlgebraicDocIds(entry.index.alloc, ids);
                }
                filter_supported = true;
                changed = true;
            }
            if (owned_set.exclude.len > 0) {
                const merged = try unionAlgebraicDocIds(entry.index.alloc, exclude_doc_ids, owned_set.exclude);
                entry.index.freeDocIds(exclude_doc_ids);
                exclude_doc_ids = merged;
                changed = true;
            }
            filter_json_resolved = true;
        }
    }
    if (req.exclusion_query_json.len > 0) {
        if (try entry.index.docIdSetForFilterJsonWithBindingsAlloc(self.core.store, req.exclusion_query_json, filter_bindings.items)) |set| {
            var owned_set = set;
            defer owned_set.deinit(entry.index);
            if (owned_set.include) |ids| {
                const merged = try unionAlgebraicDocIds(entry.index.alloc, exclude_doc_ids, ids);
                entry.index.freeDocIds(exclude_doc_ids);
                exclude_doc_ids = merged;
                changed = true;
            }
            if (owned_set.exclude.len > 0) {
                const merged = try unionAlgebraicDocIds(entry.index.alloc, exclude_doc_ids, owned_set.exclude);
                entry.index.freeDocIds(exclude_doc_ids);
                exclude_doc_ids = merged;
                changed = true;
            }
            exclusion_json_resolved = true;
        }
    }
    if (!changed) {
        entry.index.recordVectorFilterUnsupported(req.require_algebraic_filter_resolution);
        self.recordUnsupportedDocSetFilterShape();
        if (req.require_algebraic_filter_resolution) return error.UnsupportedQueryRequest;
        return .{ .req = req };
    }

    var next = req;
    if (filter_supported) {
        next.filter_doc_ids = filter_doc_ids;
        next.filter_doc_ids_positive = true;
    }
    if (exclude_doc_ids.len > 0) next.exclude_doc_ids = exclude_doc_ids;
    if (filter_json_resolved) next.filter_query_json = "";
    if (exclusion_json_resolved) next.exclusion_query_json = "";
    if (filter_bindings.items.len > 0) next.doc_filter_bindings = &.{};
    if (next.require_algebraic_filter_resolution and (next.filter_query_json.len > 0 or next.exclusion_query_json.len > 0)) {
        entry.index.recordVectorFilterUnsupported(true);
        self.recordUnsupportedDocSetFilterShape();
        return error.UnsupportedQueryRequest;
    }
    const resolved_filter = try self.alloc.create(doc_set.ResolvedDocFilter);
    errdefer self.alloc.destroy(resolved_filter);
    resolved_filter.* = try self.resolvedDocFilterForIdsAlloc(filter_supported, filter_doc_ids, exclude_doc_ids, req.identity_read_generation);
    errdefer resolved_filter.deinit(self.alloc);
    next.resolved_doc_filter = resolved_filter;
    entry.index.recordVectorFilterResolved(filter_doc_ids.len, exclude_doc_ids.len);
    return .{
        .req = next,
        .index = entry.index,
        .filter_doc_ids = filter_doc_ids,
        .exclude_doc_ids = exclude_doc_ids,
        .resolved_doc_filter = resolved_filter,
        .resolved_doc_filter_alloc = self.alloc,
    };
}

pub fn searchRequestWithDynamicStructuredDocFilterAlloc(self: *DB, req: types.SearchRequest) !?AlgebraicDocFilterRequest {
    var resolved = (try db_query_search.resolveStructuredDocFilterForComposedAlloc(self.alloc, req, .{
        .load_stored = loadStoredSearchDocumentCallback,
        .load_many_stored_values = loadStoredSearchDocumentManyCallback,
        .open_logical_sources = openStoredLogicalSourcesCallback,
        .ctx = self,
        .text_index_entry = textIndexEntryCallback,
        .resolve_doc_set_doc_ids = resolveDocSetDocIdsCallback,
        .resolve_doc_ids_to_doc_set = resolveDocIdsToDocSetCallback,
        .live_filter_doc_set = liveFilterDocSetCallback,
        .nonvisible_doc_set = nonVisibleDocSetCallback,
        .project_ordinals_to_doc_ids = false,
        .identity_read_generation = req.identity_read_generation,
    })) orelse return null;
    errdefer resolved.deinit(self.alloc);

    const resolved_filter = try self.alloc.create(doc_set.ResolvedDocFilter);
    errdefer self.alloc.destroy(resolved_filter);
    resolved_filter.* = resolved;
    resolved = .{};

    var next = req;
    next.resolved_doc_filter = resolved_filter;
    next.filter_text = null;
    next.exclusion_text = null;
    next.filter_query_json = "";
    next.exclusion_query_json = "";
    return .{
        .req = next,
        .resolved_doc_filter = resolved_filter,
        .resolved_doc_filter_alloc = self.alloc,
    };
}

pub fn searchRequestWithDirectAlgebraicDocFilterAlloc(self: *DB, req: types.SearchRequest, index: *@import("algebraic/index.zig").Index) !?AlgebraicDocFilterRequest {
    // Search requests are normalized to the current identity generation before
    // reaching this path. Algebraic ordinal postings represent the current
    // live row set, so using them directly avoids a broad identity-state scan
    // before every vector filter query.
    const algebraic_filter_rows_are_visible = true;
    var resolved_bindings = std.ArrayListUnmanaged(@import("algebraic/index.zig").Index.ResolvedFilterBinding).empty;
    defer {
        for (resolved_bindings.items) |*binding| binding.filter.deinit(index.alloc);
        resolved_bindings.deinit(index.alloc);
    }
    for (req.doc_filter_bindings) |binding| {
        if (binding.name.len == 0 or binding.filter_query_json.len == 0) return error.InvalidArgument;
        for (resolved_bindings.items) |existing| {
            if (std.mem.eql(u8, existing.name, binding.name)) return error.InvalidArgument;
        }
        var binding_filter = (if (algebraic_filter_rows_are_visible)
            try index.resolvedDocFilterForFilterJsonUncheckedAlloc(
                self.core.store,
                binding.filter_query_json,
                resolved_bindings.items,
            )
        else
            try index.resolvedDocFilterForFilterJsonWithBindingsAtGenerationAlloc(
                self.core.store,
                binding.filter_query_json,
                req.identity_read_generation,
                resolved_bindings.items,
            )) orelse return null;
        errdefer binding_filter.deinit(index.alloc);
        try resolved_bindings.append(index.alloc, .{
            .name = binding.name,
            .filter = binding_filter,
        });
        binding_filter = .{};
    }

    var filter = doc_set.ResolvedDocFilter{};
    errdefer filter.deinit(index.alloc);
    var changed = false;
    var request_constraints_resolved = false;
    var filter_json_resolved = false;
    var exclusion_json_resolved = false;

    if (try self.resolvedDocFilterForRequestNativeConstraintsAlloc(index.alloc, req)) |initial| {
        filter = initial;
        changed = true;
        request_constraints_resolved = true;
    }

    if (req.filter_query_json.len > 0) {
        var query_filter = (if (algebraic_filter_rows_are_visible)
            try index.resolvedDocFilterForFilterJsonUncheckedAlloc(
                self.core.store,
                req.filter_query_json,
                resolved_bindings.items,
            )
        else if (resolved_bindings.items.len > 0)
            try index.resolvedDocFilterForFilterJsonWithBindingsAtGenerationAlloc(
                self.core.store,
                req.filter_query_json,
                req.identity_read_generation,
                resolved_bindings.items,
            )
        else
            try index.resolvedDocFilterForFilterJsonAtGenerationAlloc(self.core.store, req.filter_query_json, req.identity_read_generation)) orelse {
            filter.deinit(index.alloc);
            return null;
        };
        defer query_filter.deinit(index.alloc);
        if (!(try applyResolvedFilterIncludeAlloc(index.alloc, &filter, &query_filter))) {
            filter.deinit(index.alloc);
            return null;
        }
        changed = true;
        filter_json_resolved = true;
    }
    if (req.exclusion_query_json.len > 0) {
        var exclusion = (if (algebraic_filter_rows_are_visible)
            try index.resolvedDocFilterForFilterJsonUncheckedAlloc(
                self.core.store,
                req.exclusion_query_json,
                resolved_bindings.items,
            )
        else if (resolved_bindings.items.len > 0)
            try index.resolvedDocFilterForFilterJsonWithBindingsAtGenerationAlloc(
                self.core.store,
                req.exclusion_query_json,
                req.identity_read_generation,
                resolved_bindings.items,
            )
        else
            try index.resolvedDocFilterForFilterJsonAtGenerationAlloc(self.core.store, req.exclusion_query_json, req.identity_read_generation)) orelse {
            filter.deinit(index.alloc);
            return null;
        };
        defer exclusion.deinit(index.alloc);

        if (!(try unionResolvedFilterExcludeAlloc(index.alloc, &filter, &exclusion.include))) {
            filter.deinit(index.alloc);
            return null;
        }
        if (!(try unionResolvedFilterExcludeAlloc(index.alloc, &filter, &exclusion.exclude))) {
            filter.deinit(index.alloc);
            return null;
        }
        changed = true;
        exclusion_json_resolved = true;
    }
    if (!changed) return null;

    const resolved_filter = try index.alloc.create(doc_set.ResolvedDocFilter);
    errdefer index.alloc.destroy(resolved_filter);
    resolved_filter.* = filter;
    filter = .{};

    var next = req;
    if (request_constraints_resolved) {
        next.filter_doc_ids = &.{};
        next.filter_doc_ids_positive = false;
        next.exclude_doc_ids = &.{};
    }
    if (filter_json_resolved) next.filter_query_json = "";
    if (exclusion_json_resolved) next.exclusion_query_json = "";
    if (resolved_bindings.items.len > 0) next.doc_filter_bindings = &.{};
    next.resolved_doc_filter = resolved_filter;

    index.recordVectorFilterResolved(
        resolvedDocSetStatCount(&resolved_filter.include),
        resolvedDocSetStatCount(&resolved_filter.exclude),
    );
    return .{
        .req = next,
        .resolved_doc_filter = resolved_filter,
        .resolved_doc_filter_alloc = index.alloc,
    };
}

pub fn loadRequiredProjectedSearchDocumentCallback(
    ctx: ?*anyopaque,
    alloc: Allocator,
    req: types.SearchRequest,
    key: []const u8,
) anyerror![]u8 {
    const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
    return try projectOwnedStoredBytesForSearch(
        self,
        alloc,
        req,
        key,
        (try self.loadStoredSearchDocumentProbe(alloc, key)) orelse return error.StoredDocMissing,
    );
}

pub fn postprocessVectorSearchResultCallback(
    ctx: ?*anyopaque,
    alloc: Allocator,
    req: types.SearchRequest,
    raw: types.SearchResult,
    chunk_backed: bool,
) anyerror!types.SearchResult {
    const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
    var presence = VectorSearchPresenceContext{ .db = self };
    var preparing = true;
    errdefer if (preparing) {
        var owned = raw;
        owned.deinit();
    };
    // The vector’s stored row is its presence authority; public chunk
    // identity stays unchanged so text/vector fusion merges the same member.
    inline for (.{ self.core.index_manager.denseIndex(req.index_name), self.core.index_manager.sparseIndex(req.index_name) }) |selected| {
        if (selected) |entry| {
            if (chunk_backed and entry.embedding_names.len == 0 and entry.chunk_name != null)
                presence.embedding_presence_name = entry.embedding_name orelse entry.config.name;
        }
    }
    preparing = false;
    return try db_query_result_shape.postprocessVectorSearchResult(alloc, req, raw, chunk_backed, .{
        .ctx = self,
        .is_visible = if (chunk_backed) isVisibleSearchHitCallback else isVisibleNonChunkSearchHitCallback,
        .filter_visible_many = filterVectorSearchCandidatesManyCallback,
        .filter_visible_many_ctx = &presence,
        .resolve_parent_id = resolveChunkParentIdCallback,
        .load_parent_stored = loadParentStoredForSearchCallback,
        .load_stored = loadStoredSearchDocumentCallback,
        .resolve_doc_set_doc_ids = resolveDocSetDocIdsCallback,
        .resolve_doc_ids_to_doc_set = resolveDocIdsToDocSetCallback,
        .load_many_parent_stored = loadParentStoredForSearchManyCallback,
        .load_many_stored = loadStoredSearchDocumentManyCallback,
        .load_projected_stored = loadProjectedSearchDocumentCallback,
        .load_many_projected_stored = loadProjectedSearchDocumentManyCallback,
    });
}

pub fn searchMatchAll(self: *DB, alloc: Allocator, req: types.SearchRequest) !types.SearchResult {
    return try db_query_search.searchMatchAll(alloc, req, self.matchAllExecutor());
}

pub fn collectSearchMatchAllCandidatesCallback(
    ctx: ?*anyopaque,
    alloc: Allocator,
    req: types.SearchRequest,
    options: db_query_search.MatchAllCandidateCollectOptions,
) anyerror!db_query_search.MatchAllCandidates {
    const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
    return try db_query_search.collectMatchAllCandidatesWithOptions(alloc, req, .{
        .ctx = self,
        .primary_key_range = self.core.byteRange(),
        .scan_store_range = scanStoreRangeCallback,
        .scan_store_range_with_context = scanStoreRangeWithContextCallback,
        .is_expired_key = isExpiredDocumentKeyCallback,
        // searchMatchAll is entered under the outer DB apply lease. The
        // lock is not reentrant once an exclusive writer closes its reader
        // gate, so callbacks in this call tree must use lock-assuming APIs.
        .lookup_doc_ordinal = lookupLiveDocOrdinalNoLockCallback,
    }, options);
}

pub fn streamSearchMatchAllCandidatesCallback(
    ctx: ?*anyopaque,
    alloc: Allocator,
    req: types.SearchRequest,
    options: db_query_search.MatchAllCandidateCollectOptions,
    consumer_ctx: ?*anyopaque,
    consumer: db_query_search.MatchAllCandidateConsumer,
) anyerror!db_query_search.MatchAllCandidateStreamStats {
    const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
    var identity_scan = PrimaryMatchAllIdentityScan{ .db = self, .req = req };
    return try db_query_search.streamMatchAllCandidatesWithOptions(alloc, req, .{
        .ctx = self,
        .primary_key_range = self.core.byteRange(),
        .scan_ids_ctx = &identity_scan,
        .scan_ids = PrimaryMatchAllIdentityScan.scan,
        .scan_store_range = scanStoreRangeCallback,
        .scan_store_range_with_context = scanStoreRangeWithContextCallback,
        .is_expired_key = isExpiredDocumentKeyCallback,
        // See collectSearchMatchAllCandidatesCallback: the caller owns the
        // apply lease for the full streaming traversal.
        .lookup_doc_ordinal = lookupLiveDocOrdinalNoLockCallback,
    }, options, consumer_ctx, consumer);
}

pub fn annotateSearchHitOrdinalsNoLock(
    self: *DB,
    alloc: Allocator,
    req: types.SearchRequest,
    hits: []types.SearchHit,
) !void {
    var missing_ids = std.ArrayListUnmanaged([]const u8).empty;
    defer missing_ids.deinit(alloc);
    for (hits) |hit| {
        // Ordinals belong to this DB's identity namespace. A qualified
        // graph hit may use the same key in another table and must retain
        // its typed identity instead of acquiring a local ordinal.
        if (hit.doc_ordinal == null and hit.source_table == null)
            try missing_ids.append(alloc, hit.id);
    }
    if (missing_ids.items.len == 0) return;

    const ordinals = try self.lookupLiveDocOrdinalsNoLock(
        alloc,
        missing_ids.items,
        req.identity_read_generation,
    );
    defer alloc.free(ordinals);

    var ordinal_index: usize = 0;
    for (hits) |*hit| {
        if (hit.doc_ordinal != null or hit.source_table != null) continue;
        hit.doc_ordinal = ordinals[ordinal_index];
        ordinal_index += 1;
    }
}

pub fn resolveSearchHitsToDocSetCallback(
    ctx: ?*anyopaque,
    alloc: Allocator,
    req: types.SearchRequest,
    hits: []const types.SearchHit,
) anyerror!doc_set.ResolvedDocSet {
    const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
    if (try resolvedDocSetFromSearchHitOrdinalsAlloc(alloc, hits)) |resolved| {
        self.recordResolvedDocSet(&resolved, false);
        return resolved;
    }
    var doc_ids = try alloc.alloc([]const u8, hits.len);
    defer alloc.free(doc_ids);
    for (hits, 0..) |hit, i| doc_ids[i] = hit.id;
    return try self.resolveDocSetForIdsNoLockAtGenerationAlloc(alloc, doc_ids, req.identity_read_generation);
}

pub fn loadProjectedSearchDocumentCallback(
    ctx: ?*anyopaque,
    alloc: Allocator,
    req: types.SearchRequest,
    key: []const u8,
) anyerror!?[]u8 {
    const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
    return if (try self.loadStoredSearchDocumentProbe(alloc, key)) |stored|
        try projectOwnedStoredBytesForSearch(self, alloc, req, key, stored)
    else
        null;
}

pub fn loadProjectedSearchDocumentManyCallback(
    ctx: ?*anyopaque,
    alloc: Allocator,
    req: types.SearchRequest,
    keys: []const []const u8,
) anyerror![]?[]u8 {
    const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
    return try self.loadProjectedSearchDocumentMany(alloc, req, keys);
}

pub fn loadStoredSearchDocumentProbe(self: *DB, alloc: Allocator, key: []const u8) !?[]u8 {
    const store_key = try encodeStoreLookupKeyAlloc(self, alloc, key);
    defer alloc.free(store_key);
    var txn = try self.core.store.beginProbeTxn();
    defer txn.abort();
    const raw = txn.get(store_key) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    return try self.core.index_manager.materializeStoredValueAlloc(alloc, store_key, raw);
}

pub fn loadProjectedSearchDocumentMany(
    self: *DB,
    alloc: Allocator,
    req: types.SearchRequest,
    keys: []const []const u8,
) ![]?[]u8 {
    const bench_profile = benchQueryProfileEnabled();
    const total_start_ns = if (bench_profile) platform_time.monotonicNs() else 0;
    const direct_fields = if (req.defer_stored_projection) null else ordinalProjectionFields(req.fields, req.include_all_fields);
    const loaded = try loadStoredSearchDocumentsMany(self, alloc, keys, direct_fields);
    errdefer freeOptionalOwnedBytes(alloc, loaded);

    var project_ns: u64 = 0;
    for (loaded, 0..) |maybe_stored, i| {
        if (maybe_stored) |stored| {
            const project_start_ns = if (bench_profile) platform_time.monotonicNs() else 0;
            loaded[i] = try projectOwnedStoredBytesForSearch(self, alloc, req, keys[i], stored);
            if (bench_profile) project_ns += platform_time.monotonicNs() - project_start_ns;
        }
    }
    if (bench_profile) {
        std.log.info(
            "antfly_bench_projected_many total_us={d} project_us={d} keys={d}",
            .{ (platform_time.monotonicNs() - total_start_ns) / 1000, project_ns / 1000, keys.len },
        );
    }
    return loaded;
}

test "db search requests default to current identity generation snapshot" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var db = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = .{ .mem = .{} },
    });
    defer db.close();

    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"name\":\"alpha\"}" }},
    });

    const current_generation = db.core.nextDerivedSequence();
    const current = try db.searchRequestAtCurrentIdentityGeneration(.{});
    try std.testing.expectEqual(@as(?u64, db.core.nextDerivedSequence()), current.identity_read_generation);

    var captured = try db.searchWithCapturedRequest(alloc, .{ .limit = 1 });
    defer captured.result.deinit();
    try std.testing.expectEqual(
        @as(?u64, current_generation),
        captured.request.identity_read_generation,
    );
    try std.testing.expectEqual(@as(u32, 1), captured.result.total_hits);

    const explicit = try db.searchRequestAtCurrentIdentityGeneration(.{ .identity_read_generation = current_generation });
    try std.testing.expectEqual(@as(?u64, current_generation), explicit.identity_read_generation);

    try std.testing.expectError(error.IdentityReadGenerationChanged, db.searchRequestAtCurrentIdentityGeneration(.{
        .identity_read_generation = current_generation -| 1,
    }));
    try std.testing.expectError(error.IdentityReadGenerationChanged, db.search(alloc, .{
        .identity_read_generation = current_generation + 1,
    }));

    var preflight = try db.preflightSearchRequest(alloc, .{ .identity_read_generation = current_generation }, 0);
    defer preflight.deinit(alloc);
    try std.testing.expectError(error.IdentityReadGenerationChanged, db.preflightSearchRequest(alloc, .{
        .identity_read_generation = current_generation -| 1,
    }, 0));
    try std.testing.expectError(error.IdentityReadGenerationChanged, db.collectPlanningStats(alloc, .{
        .identity_read_generation = current_generation + 1,
    }, 0));
    const text_stats = try db.collectSearchRequestTextStats(alloc, .{ .identity_read_generation = current_generation });
    defer alloc.free(text_stats);
    try std.testing.expectError(error.IdentityReadGenerationChanged, db.collectSearchRequestTextStats(alloc, .{
        .identity_read_generation = current_generation + 1,
    }));

    const stats = try db.stats(alloc);
    defer types.freeDBStats(alloc, stats);
    try std.testing.expectEqual(@as(u64, 5), stats.doc_set_planning.stale_identity_generation_rejection_count);
}

test "db query capture retains settled terminal repair debt without acknowledging visibility" {
    const Fixture = struct {
        fn produce(raw: *anyopaque, _: Allocator, _: asset_producer_mod.Request) ![]u8 {
            const calls: *u8 = @ptrCast(@alignCast(raw));
            calls.* += 1;
            return error.InvalidExtractorResponse;
        }
    };
    const alloc = std.testing.allocator;
    var tmp = try TestDirectory.init("query-terminal-debt");
    defer tmp.cleanup();
    const pins = try std.fmt.allocPrint(alloc, "{s}.query-pins", .{tmp.path()});
    defer alloc.free(pins);
    defer std.Io.Dir.cwd().deleteTree(std.testing.io, pins) catch {};
    var db = try DB.open(alloc, tmp.path(), .{
        .identity_namespace = .{ .table_id = 7, .shard_id = 1, .range_id = 1 },
        .start_optional_runtimes = false,
        .start_optional_runtime_workers = false,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    try db.addIndex(.{ .name = "text", .kind = .full_text, .config_json = "{}" });
    try db.addEnrichment(.{ .name = "broken", .kind = .asset, .field = "body", .producer_json = "{\"type\":\"generator\",\"config\":{\"generator\":{\"provider\":\"openai\",\"model\":\"fixture\",\"api_key\":\"fixture\"}}}" });
    var fixture: u8 = 0;
    var config: enrichment_runtime_mod.Config = .{ .asset_producer = .{ .ptr = &fixture, .vtable = &.{ .produce = Fixture.produce, .invocation_memory_for_requests = GateAssetProducer.invocationMemory } } };
    try db.initOptionalEnrichmentRuntime(&config);
    try db.batch(.{ .writes = &.{.{ .key = "doc", .value = "{\"body\":\"broken input\"}" }}, .sync_level = .write });
    const runtime = db.enrichment_runtime.?;
    const sequence = db.core.nextDerivedSequence();
    try std.testing.expectError(error.EnrichmentWorkerFailed, runtime.catchUpUntilForDrain(sequence));
    try std.testing.expect(runtime.stats().applied_sequence >= sequence);
    try std.testing.expectError(error.EnrichmentWorkerFailed, db.runMaintenanceUntilWithCancellation(sequence, .{}, .none, std.math.maxInt(u64)));

    const id: [64]u8 = @splat('b');
    const request: native_query_cut.Request = .{
        .id = &id,
        .table_id = 7,
        .expires_ms = native_query_cut.nowMs() + 300_000,
        .create = true,
    };
    // An applied watermark is insufficient when the owner itself is unhealthy.
    try runtime.catchUpUntil(db.core.nextDerivedSequence());
    try std.testing.expect(runtime.stats().applied_sequence >= db.core.nextDerivedSequence());
    runtime.worker_failed = true;
    try std.testing.expectError(error.EnrichmentWorkerFailed, db.captureQueryCut(request, .none));
    runtime.worker_failed = false;
    runtime.retrying = true;
    runtime.next_retry_at_ms = runtime.clock.nowRealtimeMs() + 60_000;
    try db.batch(.{ .writes = &.{.{ .key = "pending", .value = "{\"body\":\"awaiting extraction\"}" }}, .sync_level = .write });
    try std.testing.expect(runtime.stats().applied_sequence < db.core.nextDerivedSequence());
    const calls_before_capture = fixture;
    try db.captureQueryCut(request, .none);
    try std.testing.expectEqual(calls_before_capture, fixture);
    try std.testing.expect(runtime.stats().retrying);
    runtime.retrying = false;
    runtime.next_retry_at_ms = 0;

    var retained = try db.openQueryCut(request, .none);
    defer retained.close();
    var sorted = try retained.search(alloc, .{ .index_name = "text", .full_text = .{ .match_all = {} }, .order_by = &.{.{ .field = "_id" }}, .limit = 10 });
    defer sorted.deinit();
    try std.testing.expectEqual(@as(usize, 2), sorted.hits.len);
    try std.testing.expectEqualStrings("doc", sorted.hits[0].id);
    try std.testing.expectEqualStrings("pending", sorted.hits[1].id);
    const pending = (try retained.get(alloc, "pending")) orelse return error.TestUnexpectedResult;
    defer alloc.free(pending);
    try std.testing.expect(std.mem.indexOf(u8, pending, "awaiting extraction") != null);
    var document = (try retained.getDocument(alloc, "doc", .{})) orelse return error.TestUnexpectedResult;
    defer document.deinit(alloc);
    try ant_json.testing.expectSubsetJsonText(alloc, "{\"body\":\"broken input\"}", document.json);
    const issues = try retained.listArtifactRepairIssues(alloc, .asset, null, 0);
    defer types.freeArtifactRepairIssues(alloc, issues);
    try std.testing.expectEqual(@as(usize, 1), issues.len);
    try std.testing.expectEqualStrings("broken", issues[0].artifact_name);
    // Capturing reads must neither clear the debt nor turn a later write
    // visibility acknowledgement into success.
    try std.testing.expectError(error.EnrichmentWorkerFailed, runtime.catchUpUntilForDrain(sequence));
}

test "db search structured expansion filters cancel inside dictionary scans" {
    const alloc = std.testing.allocator;
    var directory = try TestDirectory.init("db");
    defer directory.cleanup();
    var db = try DB.open(alloc, directory.path(), .{ .start_optional_runtime_workers = false, .start_index_workers = false });
    defer db.close();
    try db.addIndex(.{ .name = "text", .kind = .full_text, .config_json = "{}" });
    const writes = try alloc.alloc(types.BatchWrite, 4096);
    defer alloc.free(writes);
    var initialized: usize = 0;
    defer for (writes[0..initialized]) |write| {
        alloc.free(write.key);
        alloc.free(write.value);
    };
    for (writes, 0..) |*write, index| {
        const key = try std.fmt.allocPrint(alloc, "doc:{d}", .{index});
        errdefer alloc.free(key);
        const value = try std.fmt.allocPrint(alloc, "{{\"body\":\"catalog{d}\"}}", .{index});
        write.* = .{ .key = key, .value = value };
        initialized += 1;
    }
    try db.batch(.{ .writes = writes, .sync_level = .full_index });
    const State = struct {
        checks: usize = 0,
        fn canceled(ptr: *const anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(@constCast(ptr)));
            self.checks += 1;
            return self.checks >= 128;
        }
    };
    for ([_]bool{ false, true }) |exclude| {
        var state = State{};
        var request: types.SearchRequest = .{
            .index_name = "text",
            .query = .{ .match_all = {} },
            .include_stored = false,
            .cancellation = .{ .ptr = &state, .is_cancelled_fn = State.canceled },
        };
        const filter = "{\"wildcard\":{\"field\":\"body\",\"pattern\":\"*never-present\"}}";
        if (exclude) request.exclusion_query_json = filter else request.filter_query_json = filter;
        var result = db.search(alloc, request) catch |err| {
            try std.testing.expectEqual(error.Canceled, err);
            continue;
        };
        result.deinit();
        return error.TestUnexpectedResult;
    }
}

test "db query repair gate revalidates stale debt" {
    const alloc = std.testing.allocator;
    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var db = try DB.open(alloc, std.mem.span(path), .{ .start_index_workers = false });
    defer db.close();
    try db.addIndex(.{
        .name = "stale_repair",
        .kind = .full_text,
        .config_json = "{}",
    });

    // Model a reader that loaded repair debt immediately before its durable
    // intent was removed. The first rejected request proves absence and heals
    // the process-local gate instead of wedging the reader indefinitely.
    try db.core.index_manager.markUnscopedRepairUnavailable("stale_repair");
    try db.failIfIndexQuarantined("stale_repair");
    try std.testing.expect(!db.core.index_manager.repairUnavailable("stale_repair"));
}

test "db query drops full text hits whose stored document row was deleted directly from the store" {
    // Issue #929: deleting a chunk enrichment and reprocessing (or a crash
    // mid-purge) can leave a full-text posting pointing at a stored-document
    // key whose row is gone -- the write-side fix in this checkout stops
    // *new* orphans from the enrichment-delete-then-reprocess path, but the
    // query side must still tolerate any orphaned posting that already
    // exists (e.g. from a crash, or a store mutated out of band) by
    // dropping the affected hit instead of failing the whole query with
    // StoredDocMissing/500, exactly as the issue's own repro observed for
    // plain row-level queries and match_all. Simulate that end state
    // directly on a plain (non-chunk) table: delete a row's stored KV entry
    // out from under its own full-text posting, then assert queries that
    // would have hit it still succeed with reduced, correct results.
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "ft_rows",
        .kind = .full_text,
        .config_json = "{}",
    });

    try db.addIndex(.{ .name = "dv_rows", .kind = .dense_vector, .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\"}" });
    try db.addIndex(.{ .name = "sp_rows", .kind = .sparse_vector, .config_json = "{\"field\":\"sparse\"}" });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"body\":\"alpha beta gamma\",\"embedding\":[0,0],\"sparse\":{\"indices\":[1],\"values\":[5]}}" },
            .{ .key = "doc:b", .value = "{\"body\":\"alpha delta epsilon\",\"embedding\":[1,0],\"sparse\":{\"indices\":[1],\"values\":[4]}}" },
        },
        .sync_level = .full_index,
    });
    try db.runUntilIdle();

    // Sanity check: both rows are searchable before one goes missing.
    var before_term = try db.search(alloc, .{
        .index_name = "ft_rows",
        .full_text = .{ .match = .{ .field = "body", .text = "alpha" } },
        .include_stored = true,
        .limit = 10,
    });
    defer before_term.deinit();
    try std.testing.expectEqual(@as(u32, 2), before_term.total_hits);
    try std.testing.expectEqual(@as(usize, 2), before_term.hits.len);

    const orphaned_doc_key = try internal_keys.documentKeyAlloc(alloc, "doc:a");
    defer alloc.free(orphaned_doc_key);
    // Confirm the row exists before deleting it, so a failure to delete
    // doesn't silently make this test pass for the wrong reason.
    {
        var probe = try db.core.store.beginProbeTxn();
        defer probe.abort();
        _ = try probe.get(orphaned_doc_key);
    }
    // Delete the KV row directly, bypassing db.batch's normal delete path
    // entirely -- the posting for "doc:a" in ft_rows now has no backing
    // stored row, the same end state left by #929's enrichment-delete +
    // reprocess sequence (or a crash mid-purge).
    try db.core.store.delete(orphaned_doc_key);

    // A child range can own a member while source/unit hydration is remote.
    const remote_member_key = try internal_keys.documentUnitChunkArtifactKeyAlloc(alloc, "doc:remote", "remote_chunks", "document:000001", 0);
    defer alloc.free(remote_member_key);
    try db.core.store.put(remote_member_key, "{}");
    defer db.core.store.delete(remote_member_key) catch {};

    // An embedding member can exist locally even when its source chunk,
    // unit and document belong to another range. Canonical artifact identity,
    // rather than its source/public id, must drive the probe under OOM too.
    const remote_source_key = try internal_keys.documentUnitChunkArtifactKeyAlloc(alloc, "doc:off-shard", "remote_chunks", "document:000001", 0);
    defer alloc.free(remote_source_key);
    const remote_embedding_key = try internal_keys.derivedEmbeddingArtifactKeyAlloc(alloc, remote_source_key, "remote_dense");
    defer alloc.free(remote_embedding_key);
    try db.core.store.put(remote_embedding_key, "{}");
    defer db.core.store.delete(remote_embedding_key) catch {};
    var remote_embedding_ref = (try artifact_ids.decodeArtifactRefAlloc(alloc, remote_embedding_key)).?;
    defer remote_embedding_ref.deinit(alloc);

    const PresenceAllocationCheck = struct {
        fn run(test_alloc: Allocator, active_db: *DB, remote_key: []u8, source_key: []u8, embedding_ref: types.ArtifactRef) !void {
            const keep = try filterPresentSearchHitsMany(active_db, test_alloc, &.{
                .{ .id = @constCast("doc:a") },
                .{ .id = @constCast("doc:b") },
                .{ .id = remote_key },
                .{ .id = @constCast("doc:off-shard"), .artifact_ref = embedding_ref },
                .{ .id = @constCast("doc:b"), .artifact_ref = .{ .document_id = @constCast("doc:b"), .name = @constCast("absent_embedding"), .kind = .embedding } },
            }, null);
            defer test_alloc.free(keep);
            try std.testing.expectEqualSlices(bool, &.{ false, true, true, true, false }, keep);
            // A single managed vector source uses chunk metadata even when
            // store_chunks is false. Its embedding is the persisted member.
            const single = try filterPresentSearchHitsMany(active_db, test_alloc, &.{
                .{ .id = remote_key },
                .{ .id = source_key },
            }, "remote_dense");
            defer test_alloc.free(single);
            try std.testing.expectEqualSlices(bool, &.{ false, true }, single);
        }
    };
    try platform.allocator.checkAllAllocationFailures(alloc, PresenceAllocationCheck.run, .{ &db, remote_member_key, remote_source_key, remote_embedding_ref });

    // Presence is a candidate visibility rule, including IDs-only and count
    // requests. Apply it before offset/limit, and refill an orphaned top hit.
    for ([_]bool{ false, true }) |include_stored| {
        for ([_]types.TextQuery{ .{ .match_all = {} }, .{ .match = .{ .field = "body", .text = "alpha" } } }) |query| {
            var page = try db.search(alloc, .{ .index_name = "ft_rows", .full_text = query, .include_stored = include_stored, .limit = 1 });
            defer page.deinit();
            try std.testing.expectEqual(@as(usize, 1), page.hits.len);
            try std.testing.expectEqualStrings("doc:b", page.hits[0].id);
            try std.testing.expectEqual(@as(u32, 1), page.total_hits);
            var after = try db.search(alloc, .{ .index_name = "ft_rows", .full_text = query, .include_stored = include_stored, .offset = 1, .limit = 1 });
            defer after.deinit();
            try std.testing.expectEqual(@as(usize, 0), after.hits.len);
            try std.testing.expectEqual(@as(u32, 1), after.total_hits);
            var count = try db.search(alloc, .{ .index_name = "ft_rows", .full_text = query, .count_only = true });
            defer count.deinit();
            try std.testing.expectEqual(@as(u32, 1), count.total_hits);
        }
    }

    for ([_]bool{ false, true }) |include_stored| {
        for ([_]bool{ false, true }) |is_sparse| {
            var req: types.SearchRequest = .{ .index_name = if (is_sparse) "sp_rows" else "dv_rows", .include_stored = include_stored, .limit = 1 };
            if (is_sparse) req.query = .{ .sparse_knn = .{ .indices = &.{1}, .values = &.{1}, .k = 1 } } else req.dense = .{ .vector = &.{ 0, 0 }, .k = 1 };
            var page = try db.search(alloc, req);
            defer page.deinit();
            try std.testing.expectEqual(@as(usize, 1), page.hits.len);
            try std.testing.expectEqualStrings("doc:b", page.hits[0].id);
            req.offset = 1;
            var after = try db.search(alloc, req);
            defer after.deinit();
            try std.testing.expectEqual(@as(usize, 0), after.hits.len);
        }
    }

    for ([_]bool{ false, true }) |include_stored| {
        var sorted = try db.search(alloc, .{
            .index_name = "ft_rows",
            .full_text = .{ .match_all = {} },
            .order_by = &.{.{ .field = "_id" }},
            .include_stored = include_stored,
            .limit = 1,
        });
        defer sorted.deinit();
        try std.testing.expectEqual(@as(usize, 1), sorted.hits.len);
        try std.testing.expectEqualStrings("doc:b", sorted.hits[0].id);
        try std.testing.expectEqual(@as(u32, 1), sorted.total_hits);
    }

    // A term unique to the now-orphaned row must come back empty, not fail
    // the query (matches the issue's `query {"query": "harbor"} -> 500`).
    var unique_term = try db.search(alloc, .{
        .index_name = "ft_rows",
        .full_text = .{ .match = .{ .field = "body", .text = "beta" } },
        .include_stored = true,
        .limit = 10,
    });
    defer unique_term.deinit();
    try std.testing.expectEqual(@as(u32, 0), unique_term.total_hits);
    try std.testing.expectEqual(@as(usize, 0), unique_term.hits.len);

    // A term shared by both rows must drop only the orphaned hit and adjust
    // total_hits, not fail the whole query.
    var shared_term = try db.search(alloc, .{
        .index_name = "ft_rows",
        .full_text = .{ .match = .{ .field = "body", .text = "alpha" } },
        .include_stored = true,
        .limit = 10,
    });
    defer shared_term.deinit();
    try std.testing.expectEqual(@as(u32, 1), shared_term.total_hits);
    try std.testing.expectEqual(@as(usize, 1), shared_term.hits.len);
    try std.testing.expectEqualStrings("doc:b", shared_term.hits[0].id);

    // match_all must also drop the orphaned hit rather than 500 (the
    // issue's own repro: `match_all` failed too, not just term queries).
    var all_result = try db.search(alloc, .{
        .index_name = "ft_rows",
        .full_text = .{ .match_all = {} },
        .include_stored = true,
        .limit = 10,
    });
    defer all_result.deinit();
    try std.testing.expectEqual(@as(u32, 1), all_result.total_hits);
    try std.testing.expectEqual(@as(usize, 1), all_result.hits.len);
    try std.testing.expectEqualStrings("doc:b", all_result.hits[0].id);
}

test "db search supports graph result_ref from full-text hits" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{}",
    });
    try db.addIndex(.{
        .name = "gr_v1",
        .kind = .graph,
        .config_json = "{}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "paper:1", .value = "{\"title\":\"intro\",\"body\":\"machine learning systems\",\"_edges\":{\"gr_v1\":{\"cites\":[{\"target\":\"paper:2\",\"weight\":1.0}]}}}" },
            .{ .key = "paper:2", .value = "{\"title\":\"followup\",\"body\":\"retrieval and ranking\"}" },
            .{ .key = "paper:3", .value = "{\"title\":\"other\",\"body\":\"graph traversal\"}" },
        },
        .sync_level = .full_index,
    });

    var result = try db.search(alloc, .{
        .full_text = .{ .match = .{ .field = "body", .text = "machine learning" } },
        .index_name = "ft_v1",
        .graph_queries = &.{
            .{
                .name = "citations",
                .query = .{
                    .query_type = .neighbors,
                    .index_name = "gr_v1",
                    .start_nodes = .{ .result_ref = .{ .ref = "$query_results", .limit = 0 } },
                    .params = .{ .direction = .out, .edge_types = &.{"cites"} },
                },
            },
        },
        .limit = 10,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqualStrings("paper:1", result.hits[0].id);
    try std.testing.expectEqual(@as(usize, 1), result.graph_results.len);
    try std.testing.expectEqualStrings("citations", result.graph_results[0].name);
    try std.testing.expectEqual(@as(u32, 1), result.graph_results[0].total_hits);
    try std.testing.expectEqualStrings("paper:2", result.graph_results[0].hits[0].id);
    const stats = try db.stats(alloc);
    defer types.freeDBStats(alloc, stats);
    try std.testing.expect(stats.doc_set_planning.ordinal_list_count >= 1);
}

test "db search supports graph result_ref from dense hits without public id handoff" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"cosine\"}",
    });
    try db.addIndex(.{
        .name = "gr_v1",
        .kind = .graph,
        .config_json = "{}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "paper:1", .value = "{\"embedding\":[1,0],\"_edges\":{\"gr_v1\":{\"cites\":[{\"target\":\"paper:2\",\"weight\":1.0}]}}}" },
            .{ .key = "paper:2", .value = "{\"embedding\":[0,1]}" },
            .{ .key = "paper:3", .value = "{\"embedding\":[-1,0]}" },
        },
        .sync_level = .full_index,
    });

    var result = try db.search(alloc, .{
        .index_name = "dv_v1",
        .dense = .{ .vector = &.{ 1.0, 0.0 }, .k = 1 },
        .graph_queries = &.{
            .{
                .name = "citations",
                .query = .{
                    .query_type = .neighbors,
                    .index_name = "gr_v1",
                    .start_nodes = .{ .result_ref = .{ .ref = "$query_results", .limit = 0 } },
                    .params = .{ .direction = .out, .edge_types = &.{"cites"} },
                },
            },
        },
        .limit = 10,
    });
    defer result.deinit();

    try std.testing.expect(result.total_hits >= 1);
    try std.testing.expect(result.hits.len >= 1);
    try std.testing.expectEqualStrings("paper:1", result.hits[0].id);
    var identity_txn = try db.core.store.beginProbeTxn();
    defer identity_txn.abort();
    try std.testing.expectEqual(try doc_identity.lookupOrdinalTxn(alloc, &identity_txn, "paper:1"), result.hits[0].doc_ordinal);
    try std.testing.expectEqual(@as(usize, 1), result.graph_results.len);
    try std.testing.expectEqualStrings("citations", result.graph_results[0].name);
    try std.testing.expectEqual(@as(u32, 1), result.graph_results[0].total_hits);
    try std.testing.expectEqualStrings("paper:2", result.graph_results[0].hits[0].id);
}

test "db search rejects unbounded graph result_ref when base result is paged" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{}",
    });
    try db.addIndex(.{
        .name = "gr_v1",
        .kind = .graph,
        .config_json = "{}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "paper:1", .value = "{\"title\":\"intro\",\"body\":\"machine learning systems\",\"_edges\":{\"gr_v1\":{\"cites\":[{\"target\":\"paper:3\",\"weight\":1.0}]}}}" },
            .{ .key = "paper:2", .value = "{\"title\":\"followup\",\"body\":\"machine learning ranking\",\"_edges\":{\"gr_v1\":{\"cites\":[{\"target\":\"paper:3\",\"weight\":1.0}]}}}" },
            .{ .key = "paper:3", .value = "{\"title\":\"target\",\"body\":\"graph traversal\"}" },
        },
        .sync_level = .full_index,
    });

    try std.testing.expectError(error.UnsupportedQueryRequest, db.search(alloc, .{
        .full_text = .{ .match = .{ .field = "body", .text = "machine" } },
        .index_name = "ft_v1",
        .graph_queries = &.{
            .{
                .name = "citations",
                .query = .{
                    .query_type = .neighbors,
                    .index_name = "gr_v1",
                    .start_nodes = .{ .result_ref = .{ .ref = "$query_results", .limit = 0 } },
                    .params = .{ .direction = .out, .edge_types = &.{"cites"} },
                },
            },
        },
        .limit = 1,
    }));
}

test "db search supports graph-only named queries" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "gr_v1",
        .kind = .graph,
        .config_json = "{}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "n:a", .value = "{\"title\":\"A\",\"_edges\":{\"gr_v1\":{\"links\":[{\"target\":\"n:b\"}]}}}" },
            .{ .key = "n:b", .value = "{\"title\":\"B\"}" },
        },
        .sync_level = .full_index,
    });

    var result = try db.search(alloc, .{
        .graph_queries = &.{
            .{
                .name = "neighbors",
                .query = .{
                    .query_type = .neighbors,
                    .index_name = "gr_v1",
                    .start_nodes = .{ .keys = &.{"n:a"} },
                    .params = .{ .direction = .out, .edge_types = &.{"links"} },
                },
            },
        },
        .limit = 10,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 0), result.total_hits);
    try std.testing.expectEqual(@as(usize, 0), result.hits.len);
    try std.testing.expectEqual(@as(usize, 1), result.graph_results.len);
    try std.testing.expectEqual(@as(u32, 1), result.graph_results[0].total_hits);
    try std.testing.expectEqualStrings("n:b", result.graph_results[0].hits[0].id);
    const stats = try db.stats(alloc);
    defer types.freeDBStats(alloc, stats);
    try std.testing.expect(stats.doc_set_planning.ordinal_list_count >= 1);
    try std.testing.expect(stats.doc_set_planning.ordinal_list_docs >= 1);
}

test "db search supports fused graph selectors for single-lane full-text searches" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{}",
    });
    try db.addIndex(.{
        .name = "gr_v1",
        .kind = .graph,
        .config_json = "{}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "paper:1", .value = "{\"title\":\"intro\",\"body\":\"machine learning systems\",\"_edges\":{\"gr_v1\":{\"cites\":[{\"target\":\"paper:2\",\"weight\":1.0}]}}}" },
            .{ .key = "paper:2", .value = "{\"title\":\"followup\",\"body\":\"retrieval and ranking\"}" },
            .{ .key = "paper:3", .value = "{\"title\":\"other\",\"body\":\"graph traversal\"}" },
        },
        .sync_level = .full_index,
    });

    var result = try db.search(alloc, .{
        .full_text = .{ .match = .{ .field = "body", .text = "machine learning" } },
        .index_name = "ft_v1",
        .graph_queries = &.{
            .{
                .name = "citations",
                .query = .{
                    .query_type = .neighbors,
                    .index_name = "gr_v1",
                    .start_nodes = .{ .result_ref = .{ .ref = "$query_results", .limit = 5 } },
                    .params = .{ .direction = .out, .edge_types = &.{"cites"} },
                },
            },
        },
        .limit = 10,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqualStrings("paper:1", result.hits[0].id);
    try std.testing.expectEqual(@as(usize, 1), result.graph_results.len);
    try std.testing.expectEqualStrings("citations", result.graph_results[0].name);
    try std.testing.expectEqual(@as(u32, 1), result.graph_results[0].total_hits);
    try std.testing.expectEqualStrings("paper:2", result.graph_results[0].hits[0].id);
}

test "db search fuses full_text and dense named searches before graph expansion" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{}",
    });
    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\"}",
    });
    try db.addIndex(.{
        .name = "gr_v1",
        .kind = .graph,
        .config_json = "{}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"machine learning\",\"_embeddings\":{\"dv_v1\":[1,0,0]},\"_edges\":{\"gr_v1\":{\"links\":[{\"target\":\"doc:c\"}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"body\":\"ranking systems\",\"_embeddings\":{\"dv_v1\":[0,1,0]}}" },
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"body\":\"graph target\"}" },
        },
        .sync_level = .full_index,
    });

    const profiled = try db.searchWithDenseProfile(alloc, .{
        .profile = true,
        .full_text = .{ .match = .{ .field = "body", .text = "machine learning" } },
        .index_name = "ft_v1",
        .dense_queries = &.{
            .{
                .name = "dv_v1",
                .index_name = "dv_v1",
                .query = .{
                    .vector = &.{ 1.0, 0.0, 0.0 },
                    .k = 2,
                },
            },
        },
        .merge_config = .{
            .strategy = .rrf,
            .weights = &.{
                .{ .name = "full_text", .weight = 1.0 },
                .{ .name = "dv_v1", .weight = 1.0 },
            },
        },
        .graph_queries = &.{
            .{
                .name = "neighbors",
                .query = .{
                    .query_type = .neighbors,
                    .index_name = "gr_v1",
                    .start_nodes = .{ .result_ref = .{ .ref = "$query_results", .limit = 1 } },
                    .params = .{ .direction = .out, .edge_types = &.{"links"} },
                },
            },
        },
        .limit = 10,
    });
    var result = profiled.result;
    defer result.deinit();

    const dense_profile = profiled.dense_profile orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("hbc", dense_profile.search_route);
    try std.testing.expectEqualStrings("no_native_filter", dense_profile.route_reason);
    try std.testing.expect(result.total_hits >= 1);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
    try std.testing.expectEqual(@as(usize, 1), result.graph_results.len);
    try std.testing.expectEqual(@as(u32, 1), result.graph_results[0].total_hits);
    try std.testing.expectEqualStrings("doc:c", result.graph_results[0].hits[0].id);
}

test "db search marks a truncated fused candidate union as a lower bound" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{ .name = "ft_v1", .kind = .full_text, .config_json = "{}" });
    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\"}",
    });
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"body\":\"common alpha\",\"_embeddings\":{\"dv_v1\":[1,0,0]}}" },
            .{ .key = "doc:b", .value = "{\"body\":\"common beta\",\"_embeddings\":{\"dv_v1\":[0,1,0]}}" },
            .{ .key = "doc:c", .value = "{\"body\":\"common gamma\",\"_embeddings\":{\"dv_v1\":[0,0,1]}}" },
        },
        .sync_level = .full_index,
    });

    const dense_queries = [_]types.NamedDenseQuery{.{
        .name = "dv_v1",
        .index_name = "dv_v1",
        .query = .{ .vector = &.{ 1.0, 0.0, 0.0 }, .k = 1 },
    }};
    const merge_config = types.MergeConfig{
        .strategy = .rrf,
        .weights = &.{
            .{ .name = "full_text", .weight = 1.0 },
            .{ .name = "dv_v1", .weight = 1.0 },
        },
    };

    var first_page = try db.search(alloc, .{
        .full_text = .{ .match = .{ .field = "body", .text = "common" } },
        .index_name = "ft_v1",
        .dense_queries = &dense_queries,
        .merge_config = merge_config,
        .include_stored = false,
        .limit = 1,
    });
    defer first_page.deinit();
    try std.testing.expectEqual(@as(usize, 1), first_page.hits.len);
    try std.testing.expectEqual(types.TotalHitsRelation.gte, first_page.total_hits_relation);

    var full_window = try db.search(alloc, .{
        .full_text = .{ .match = .{ .field = "body", .text = "common" } },
        .index_name = "ft_v1",
        .dense_queries = &dense_queries,
        .merge_config = merge_config,
        .include_stored = false,
        .limit = 10,
    });
    defer full_window.deinit();
    try std.testing.expectEqual(@as(usize, 3), full_window.hits.len);
    try std.testing.expectEqual(types.TotalHitsRelation.exact, full_window.total_hits_relation);
}

test "db search fuses full_text and dense named searches before graph expansion with durable lsm primary backend" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var db = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{}",
    });
    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\"}",
    });
    try db.addIndex(.{
        .name = "gr_v1",
        .kind = .graph,
        .config_json = "{}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"machine learning\",\"_embeddings\":{\"dv_v1\":[1,0,0]},\"_edges\":{\"gr_v1\":{\"links\":[{\"target\":\"doc:c\"}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"body\":\"ranking systems\",\"_embeddings\":{\"dv_v1\":[0,1,0]}}" },
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"body\":\"graph target\"}" },
        },
        .sync_level = .full_index,
    });

    var result = try db.search(alloc, .{
        .full_text = .{ .match = .{ .field = "body", .text = "machine learning" } },
        .index_name = "ft_v1",
        .dense_queries = &.{
            .{
                .name = "dv_v1",
                .index_name = "dv_v1",
                .query = .{
                    .vector = &.{ 1.0, 0.0, 0.0 },
                    .k = 2,
                },
            },
        },
        .merge_config = .{
            .strategy = .rrf,
            .weights = &.{
                .{ .name = "full_text", .weight = 1.0 },
                .{ .name = "dv_v1", .weight = 1.0 },
            },
        },
        .graph_queries = &.{
            .{
                .name = "neighbors",
                .query = .{
                    .query_type = .neighbors,
                    .index_name = "gr_v1",
                    .start_nodes = .{ .result_ref = .{ .ref = "$query_results", .limit = 1 } },
                    .params = .{ .direction = .out, .edge_types = &.{"links"} },
                },
            },
        },
        .limit = 10,
    });
    defer result.deinit();

    try std.testing.expect(result.total_hits >= 1);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
    try std.testing.expectEqual(@as(usize, 1), result.graph_results.len);
    try std.testing.expectEqual(@as(u32, 1), result.graph_results[0].total_hits);
    try std.testing.expectEqualStrings("doc:c", result.graph_results[0].hits[0].id);
}

test "db search supports named full_text queries fused with dense and sparse before graph expansion" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{}",
    });
    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\"}",
    });
    try db.addIndex(.{
        .name = "sv_v1",
        .kind = .sparse_vector,
        .config_json = "{\"field\":\"sparse_embedding\"}",
    });
    try db.addIndex(.{
        .name = "gr_v1",
        .kind = .graph,
        .config_json = "{}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"machine learning\",\"_embeddings\":{\"dv_v1\":[1,0,0],\"sv_v1\":{\"indices\":[1],\"values\":[1.0]}},\"_edges\":{\"gr_v1\":{\"links\":[{\"target\":\"doc:c\"}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"body\":\"machine learning beta\",\"_embeddings\":{\"dv_v1\":[0.7,0.3,0],\"sv_v1\":{\"indices\":[1],\"values\":[0.4]}}}" },
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"body\":\"graph target\"}" },
        },
        .sync_level = .full_index,
    });

    var result = try db.search(alloc, .{
        .index_name = "ft_v1",
        .full_text_queries = &.{
            .{
                .name = "ft_body",
                .index_name = "ft_v1",
                .query = .{ .match = .{ .field = "body", .text = "machine learning" } },
            },
        },
        .dense_queries = &.{
            .{
                .name = "dv_v1",
                .index_name = "dv_v1",
                .query = .{
                    .vector = &.{ 1.0, 0.0, 0.0 },
                    .k = 2,
                },
            },
        },
        .sparse_queries = &.{
            .{
                .name = "sv_v1",
                .index_name = "sv_v1",
                .query = .{
                    .indices = &.{1},
                    .values = &.{1.0},
                    .k = 2,
                },
            },
        },
        .merge_config = .{
            .strategy = .rrf,
            .weights = &.{
                .{ .name = "ft_body", .weight = 1.0 },
                .{ .name = "dv_v1", .weight = 2.0 },
                .{ .name = "sv_v1", .weight = 1.5 },
            },
        },
        .graph_queries = &.{
            .{
                .name = "neighbors",
                .query = .{
                    .query_type = .neighbors,
                    .index_name = "gr_v1",
                    .start_nodes = .{ .result_ref = .{ .ref = "$query_results", .limit = 1 } },
                    .params = .{ .direction = .out, .edge_types = &.{"links"} },
                },
            },
        },
        .limit = 10,
    });
    defer result.deinit();

    try std.testing.expect(result.total_hits >= 1);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
    try std.testing.expectEqual(@as(usize, 1), result.graph_results.len);
    try std.testing.expectEqual(@as(u32, 1), result.graph_results[0].total_hits);
    try std.testing.expectEqualStrings("doc:c", result.graph_results[0].hits[0].id);
}

test "db search supports named full_text queries fused with dense and sparse before graph expansion with durable lsm primary backend" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var db = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{}",
    });
    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\"}",
    });
    try db.addIndex(.{
        .name = "sv_v1",
        .kind = .sparse_vector,
        .config_json = "{\"field\":\"sparse_embedding\"}",
    });
    try db.addIndex(.{
        .name = "gr_v1",
        .kind = .graph,
        .config_json = "{}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"machine learning\",\"_embeddings\":{\"dv_v1\":[1,0,0],\"sv_v1\":{\"indices\":[1],\"values\":[1.0]}},\"_edges\":{\"gr_v1\":{\"links\":[{\"target\":\"doc:c\"}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"body\":\"machine learning beta\",\"_embeddings\":{\"dv_v1\":[0.7,0.3,0],\"sv_v1\":{\"indices\":[1],\"values\":[0.4]}}}" },
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"body\":\"graph target\"}" },
        },
        .sync_level = .full_index,
    });

    var result = try db.search(alloc, .{
        .index_name = "ft_v1",
        .full_text_queries = &.{
            .{
                .name = "ft_body",
                .index_name = "ft_v1",
                .query = .{ .match = .{ .field = "body", .text = "machine learning" } },
            },
        },
        .dense_queries = &.{
            .{
                .name = "dv_v1",
                .index_name = "dv_v1",
                .query = .{
                    .vector = &.{ 1.0, 0.0, 0.0 },
                    .k = 2,
                },
            },
        },
        .sparse_queries = &.{
            .{
                .name = "sv_v1",
                .index_name = "sv_v1",
                .query = .{
                    .indices = &.{1},
                    .values = &.{1.0},
                    .k = 2,
                },
            },
        },
        .merge_config = .{
            .strategy = .rrf,
            .weights = &.{
                .{ .name = "ft_body", .weight = 1.0 },
                .{ .name = "dv_v1", .weight = 2.0 },
                .{ .name = "sv_v1", .weight = 1.5 },
            },
        },
        .graph_queries = &.{
            .{
                .name = "neighbors",
                .query = .{
                    .query_type = .neighbors,
                    .index_name = "gr_v1",
                    .start_nodes = .{ .result_ref = .{ .ref = "$query_results", .limit = 1 } },
                    .params = .{ .direction = .out, .edge_types = &.{"links"} },
                },
            },
        },
        .limit = 10,
    });
    defer result.deinit();

    try std.testing.expect(result.total_hits >= 1);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
    try std.testing.expectEqual(@as(usize, 1), result.graph_results.len);
    try std.testing.expectEqual(@as(u32, 1), result.graph_results[0].total_hits);
    try std.testing.expectEqualStrings("doc:c", result.graph_results[0].hits[0].id);
}

test "db search sorts graph queries by dependency and resolves prior graph results" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "gr_v1",
        .kind = .graph,
        .config_json = "{}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"A\",\"_edges\":{\"gr_v1\":{\"links\":[{\"target\":\"doc:b\"}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"B\",\"_edges\":{\"gr_v1\":{\"links\":[{\"target\":\"doc:c\"}]}}}" },
            .{ .key = "doc:c", .value = "{\"title\":\"C\"}" },
        },
        .sync_level = .full_index,
    });

    var result = try db.search(alloc, .{
        .graph_queries = &.{
            .{
                .name = "second_hop",
                .query = .{
                    .query_type = .neighbors,
                    .index_name = "gr_v1",
                    .start_nodes = .{ .result_ref = .{ .ref = "$graph_results.first_hop", .limit = 5 } },
                    .params = .{ .direction = .out, .edge_types = &.{"links"} },
                },
            },
            .{
                .name = "first_hop",
                .query = .{
                    .query_type = .neighbors,
                    .index_name = "gr_v1",
                    .start_nodes = .{ .keys = &.{"doc:a"} },
                    .params = .{ .direction = .out, .edge_types = &.{"links"} },
                },
            },
        },
        .limit = 10,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 2), result.graph_results.len);
    try std.testing.expectEqualStrings("first_hop", result.graph_results[0].name);
    try std.testing.expectEqualStrings("doc:b", result.graph_results[0].hits[0].id);
    try std.testing.expectEqualStrings("second_hop", result.graph_results[1].name);
    try std.testing.expectEqual(@as(u32, 1), result.graph_results[1].total_hits);
    try std.testing.expectEqualStrings("doc:c", result.graph_results[1].hits[0].id);
}

test "db search expand_strategy union and intersection apply graph hits to top level results" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{}",
    });
    try db.addIndex(.{
        .name = "gr_v1",
        .kind = .graph,
        .config_json = "{}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"body\":\"machine learning\",\"_edges\":{\"gr_v1\":{\"links\":[{\"target\":\"doc:b\"}]}}}" },
            .{ .key = "doc:b", .value = "{\"body\":\"machine learning beta\"}" },
            .{ .key = "doc:c", .value = "{\"body\":\"other topic\"}" },
        },
        .sync_level = .full_index,
    });

    var union_result = try db.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match = .{ .field = "body", .text = "machine learning" } },
        .graph_queries = &.{
            .{
                .name = "neighbors",
                .query = .{
                    .query_type = .neighbors,
                    .index_name = "gr_v1",
                    .start_nodes = .{ .keys = &.{"doc:a"} },
                    .params = .{ .direction = .out, .edge_types = &.{"links"} },
                },
            },
        },
        .expand_strategy = .@"union",
        .limit = 10,
    });
    defer union_result.deinit();

    try std.testing.expectEqual(@as(u32, 2), union_result.total_hits);
    try std.testing.expectEqualStrings("doc:a", union_result.hits[0].id);
    try std.testing.expectEqualStrings("doc:b", union_result.hits[1].id);

    var intersection_result = try db.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match = .{ .field = "body", .text = "machine learning" } },
        .graph_queries = &.{
            .{
                .name = "neighbors",
                .query = .{
                    .query_type = .neighbors,
                    .index_name = "gr_v1",
                    .start_nodes = .{ .keys = &.{"doc:a"} },
                    .params = .{ .direction = .out, .edge_types = &.{"links"} },
                },
            },
        },
        .expand_strategy = .intersection,
        .limit = 10,
    });
    defer intersection_result.deinit();

    try std.testing.expectEqual(@as(u32, 1), intersection_result.total_hits);
    try std.testing.expectEqualStrings("doc:b", intersection_result.hits[0].id);
}

test "db search projects stored fields for hydrated hits" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{}",
    });
    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value =
            \\{"title":"alpha","author":{"name":"ann","age":42},"body":"hello world"}
            ,
        }},
        .sync_level = .full_index,
    });

    var result = try db.search(alloc, .{
        .index_name = "ft_v1",
        .query = .{ .match = .{ .field = "body", .text = "hello" } },
        .fields = &.{ "author.name", "-body" },
        .include_all_fields = false,
        .include_stored = true,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expect(result.hits[0].stored_data != null);

    const parsed = try std.json.parseFromSlice(std.json.Value, alloc, result.hits[0].stored_data.?, .{});
    defer parsed.deinit();
    try std.testing.expect(parsed.value.object.get("body") == null);
    try std.testing.expectEqualStrings("ann", parsed.value.object.get("author").?.object.get("name").?.string);
}

test "db search includes chunk artifacts on hydrated hits when _chunks is requested" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{}",
    });
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"hello world\"}" }},
        .sync_level = .full_index,
    });

    const chunk_zero = try expectedChunkArtifactKeyAlloc(alloc, "doc:a", "body_chunks_v1", 0);
    defer alloc.free(chunk_zero);
    try db.core.store.put(chunk_zero, "{\"body\":\"hello\",\"_artifact_name\":\"body_chunks_v1\",\"_chunk_id\":0}");

    var result = try db.search(alloc, .{
        .index_name = "ft_v1",
        .query = .{ .match = .{ .field = "body", .text = "hello" } },
        .fields = &.{"_chunks"},
        .include_all_fields = false,
        .include_stored = true,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expect(result.hits[0].stored_data != null);
    const parsed = try std.json.parseFromSlice(std.json.Value, alloc, result.hits[0].stored_data.?, .{});
    defer parsed.deinit();
    const chunks = parsed.value.object.get("_chunks").?.object.get("body_chunks_v1").?.array.items;
    try std.testing.expectEqual(@as(usize, 1), chunks.len);
    try std.testing.expectEqualStrings("hello", chunks[0].object.get("body").?.string);
}

test "db search includes embedding artifacts on hydrated hits when _embeddings is requested" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{}",
    });
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"hello world\"}" }},
        .sync_level = .full_index,
    });

    const dense_key = try expectedDocumentEmbeddingArtifactKeyAlloc(alloc, "doc:a", "body_dense_v1");
    defer alloc.free(dense_key);
    try putDenseEmbeddingArtifactForTest(&db, alloc, dense_key, null, &[_]f32{ 1, 0 });

    var result = try db.search(alloc, .{
        .index_name = "ft_v1",
        .query = .{ .match = .{ .field = "body", .text = "hello" } },
        .fields = &.{"_embeddings"},
        .include_all_fields = false,
        .include_stored = true,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expect(result.hits[0].stored_data != null);
    const parsed = try std.json.parseFromSlice(std.json.Value, alloc, result.hits[0].stored_data.?, .{});
    defer parsed.deinit();
    const vector = parsed.value.object.get("_embeddings").?.object.get("body_dense_v1").?.array.items;
    try std.testing.expectEqual(@as(usize, 2), vector.len);
    try expectJsonNumber(vector[0], 1);
}

test "db search filters expired documents when ttl schema is configured" {
    const alloc = std.testing.allocator;
    const ttl_duration_ns: u64 = 60 * std.time.ns_per_s;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.setSchema(.{
        .version = 1,
        .default_type = "_default",
        .ttl_duration_ns = ttl_duration_ns,
    });

    const now_ns = currentTimeNs();
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:old", .value = "{\"title\":\"alpha\",\"body\":\"common token\"}" },
            .{ .key = "doc:new", .value = "{\"title\":\"beta\",\"body\":\"common token\"}" },
        },
        .timestamp_ns = now_ns - 2 * ttl_duration_ns,
    });
    try db.batch(.{
        .writes = &.{.{ .key = "doc:fresh", .value = "{\"title\":\"fresh\",\"body\":\"common token\"}" }},
        .timestamp_ns = now_ns,
    });

    var all = try db.search(alloc, .{
        .query = .{ .match_all = {} },
        .limit = 10,
    });
    defer all.deinit();
    try std.testing.expectEqual(@as(u32, 1), all.total_hits);
    try std.testing.expectEqualStrings("doc:fresh", all.hits[0].id);

    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{}",
    });
    try db.runUntilIdle();

    var text = try db.search(alloc, .{
        .index_name = "ft_v1",
        .query = .{ .match = .{ .field = "body", .text = "common" } },
        .limit = 10,
    });
    defer text.deinit();
    try std.testing.expectEqual(@as(u32, 1), text.total_hits);
    try std.testing.expectEqualStrings("doc:fresh", text.hits[0].id);
}
