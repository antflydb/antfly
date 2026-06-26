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
const builtin = @import("builtin");
const indexes_openapi = @import("antfly_indexes_openapi");
const metadata_openapi = @import("antfly_metadata_openapi");
const scraping = @import("antfly_scraping");
const metadata_admin = @import("../metadata/admin.zig");
const metadata_api = @import("../metadata/api.zig");
const metadata_mod = @import("../metadata/mod.zig");
const metadata_reconciler = @import("../metadata/reconciler.zig");
const common_secrets = @import("../common/secrets.zig");
const metadata_table_manager = @import("../metadata/table_manager.zig");
const metadata_table_provisioner = @import("../metadata/table_provisioner.zig");
const metadata_transition_state = @import("../metadata/transition_state.zig");
const managed_embedder = @import("../inference/managed_embedder.zig");
const asset_producer_runtime = @import("../asset_producer_runtime.zig");
const raft_mod = @import("../raft/mod.zig");
const raft_reconciler = @import("../raft/reconciler.zig");
const db_mod = @import("../storage/db/mod.zig");
const storage_schema = @import("../storage/schema.zig");
const doc_set = @import("../storage/db/doc_set.zig");
const catalog_resources = @import("catalog_resources.zig");
const db_embedder = @import("../storage/db/enrichment/embedder.zig");
const asset_producer_mod = @import("../storage/db/enrichment/asset_producer.zig");
const ha_standby_mod = @import("../storage/ha/standby.zig");
const hbc_mod = @import("../storage/hbc_adapter.zig");
const lsm_backend = @import("../storage/lsm_backend/mod.zig");
const resource_manager_mod = @import("../storage/resource_manager.zig");
const db_query_search = @import("../storage/db/query/search_exec.zig");
const graph_mod = @import("../graph/graph.zig");
const graph_paths = @import("../graph/paths.zig");
const graph_query_mod = @import("../graph/query.zig");
const reranking_runtime = @import("../reranking/mod.zig");
const template_mod = @import("../template.zig");
const table_catalog = @import("table_catalog.zig");
const table_router = @import("table_router.zig");
const tables_api = @import("tables.zig");
const query_api = @import("query.zig");
const query_contract = @import("query_contract.zig");
const relational_rows_api = @import("relational_rows.zig");
const sql_adapter = @import("../sql/mod.zig");
const document_sql_runtime = @import("../sql/document_runtime.zig");
const sql_adapter_runtime = @import("../sql/runtime.zig");
const serverless_query = @import("../serverless/query/mod.zig");
const schema_api = @import("../schema/mod.zig");
const distributed_graph = @import("distributed_graph.zig");
const runtime_status = @import("runtime_status.zig");
const http_common = @import("../raft/transport/http_common.zig");
const platform_time = @import("../platform/time.zig");
const distributed_stats_mod = @import("../search/distributed_stats.zig");
const regex_mod = @import("../search/regex.zig");
const httpx = @import("httpx");
const Io = std.Io;
const json_helpers = @import("json_helpers.zig");
const table_read_core = @import("table_reads/core.zig");
const table_read_cache = @import("table_reads/cache.zig");
const table_read_document_sql = @import("table_reads/document_sql.zig");
const table_read_relational_rows = @import("table_reads/relational_rows.zig");
const table_read_external_lake = @import("table_reads/external_lake.zig");
const table_read_remote_wire = @import("table_reads/remote_wire.zig");
const table_read_fanout = @import("table_reads/fanout.zig");
const table_read_graph = @import("table_reads/graph.zig");

const nativeCatalogTableNameAlloc = table_catalog.nativeTableNameForCatalogTargetAlloc;
const ParsedJsonPathValue = json_helpers.ParsedJsonPathValue;
const parseJsonValueAlloc = json_helpers.parseJsonValueAlloc;
const parseJsonPathValueAlloc = json_helpers.parseJsonPathValueAlloc;
const algebraic_ir = db_mod.algebraic.ir;

const searchRequestHasResolvedDocFilter = table_read_remote_wire.searchRequestHasResolvedDocFilter;
const searchRequestHasUnserializableResolvedDocFilter = table_read_remote_wire.searchRequestHasUnserializableResolvedDocFilter;
const encodeLookupFields = table_read_remote_wire.encodeLookupFields;
const encodeScanRequest = table_read_remote_wire.encodeScanRequest;
const encodeQueryRequest = table_read_remote_wire.encodeQueryRequest;
const lookupRemote = table_read_remote_wire.lookupRemote;
const scanRemote = table_read_remote_wire.scanRemote;
const preflightRemote = table_read_remote_wire.preflightRemote;
const textStatsRemote = table_read_remote_wire.textStatsRemote;
const algebraicPartialsRemote = table_read_remote_wire.algebraicPartialsRemote;
const documentAlgebraicAggregateRemote = table_read_remote_wire.documentAlgebraicAggregateRemote;
const joinPartitionRemote = table_read_remote_wire.joinPartitionRemote;
const joinRowsRemote = table_read_remote_wire.joinRowsRemote;
const joinUnmatchedRemote = table_read_remote_wire.joinUnmatchedRemote;
const joinFinalizeRemote = table_read_remote_wire.joinFinalizeRemote;
const joinJobStateRemote = table_read_remote_wire.joinJobStateRemote;
const graphExpandRemote = table_read_remote_wire.graphExpandRemote;
const graphHydrateRemote = table_read_remote_wire.graphHydrateRemote;
const graphEdgesRemote = table_read_remote_wire.graphEdgesRemote;
const lookupRelationalUniqueOwnerRemote = table_read_remote_wire.lookupRelationalUniqueOwnerRemote;
const lookupRelationalTemporalUniqueOwnerRemote = table_read_remote_wire.lookupRelationalTemporalUniqueOwnerRemote;
const lookupRelationalTemporalUniqueOverlapOwnerRemote = table_read_remote_wire.lookupRelationalTemporalUniqueOverlapOwnerRemote;
const GraphMetricFanInShardRequest = table_read_graph.GraphMetricFanInShardRequest;
const prepareGraphMetricFanInShardRequest = table_read_graph.prepareGraphMetricFanInShardRequest;
const validateGraphHydrateResolvedDocFilterForDb = table_read_graph.validateGraphHydrateResolvedDocFilterForDb;
const graphHydrateResolvedDocFilterAllows = table_read_graph.graphHydrateResolvedDocFilterAllows;
const graphExpandWithSearch = table_read_graph.graphExpandWithSearch;
const graphGetEdgesLocal = table_read_graph.graphGetEdgesLocal;
const appendJsonFieldName = table_read_remote_wire.appendJsonFieldName;
const appendJsonFieldString = table_read_remote_wire.appendJsonFieldString;
const appendJsonFieldU32 = table_read_remote_wire.appendJsonFieldU32;
const appendJsonFieldU64 = table_read_remote_wire.appendJsonFieldU64;
const appendJsonFieldF32 = table_read_remote_wire.appendJsonFieldF32;
const appendJsonFieldF64 = table_read_remote_wire.appendJsonFieldF64;
const appendJsonFieldBool = table_read_remote_wire.appendJsonFieldBool;
const appendJsonFieldNames = table_read_remote_wire.appendJsonFieldNames;
const appendJsonString = table_read_remote_wire.appendJsonString;
const OwnedTextStatsFieldRequest = table_read_remote_wire.OwnedTextStatsFieldRequest;
const OwnedBackgroundTextStatsFieldRequest = table_read_remote_wire.OwnedBackgroundTextStatsFieldRequest;
const ParsedTextStatsRequest = table_read_remote_wire.ParsedTextStatsRequest;
const encodeQueryTextStatsRequest = table_read_remote_wire.encodeQueryTextStatsRequest;
const encodeExplicitTextStatsRequest = table_read_remote_wire.encodeExplicitTextStatsRequest;
const encodeExplicitTextStatsRequestForSearchRequest = table_read_remote_wire.encodeExplicitTextStatsRequestForSearchRequest;
const encodeBackgroundTextStatsRequest = table_read_remote_wire.encodeBackgroundTextStatsRequest;
const encodeBackgroundTextStatsRequestForSearchRequest = table_read_remote_wire.encodeBackgroundTextStatsRequestForSearchRequest;
const parseTextStatsRequest = table_read_remote_wire.parseTextStatsRequest;
const encodeTextStatsResponse = table_read_remote_wire.encodeTextStatsResponse;
const parseTextStatsResponse = table_read_remote_wire.parseTextStatsResponse;
const encodeBackgroundTextStatsResponse = table_read_remote_wire.encodeBackgroundTextStatsResponse;
const parseBackgroundTextStatsResponse = table_read_remote_wire.parseBackgroundTextStatsResponse;
pub const parseTextStatsHttpResponse = table_read_remote_wire.parseTextStatsHttpResponse;
const encodeAlgebraicPartialsRequest = table_read_remote_wire.encodeAlgebraicPartialsRequest;
const encodeAlgebraicPartialsRequestWithProgram = table_read_remote_wire.encodeAlgebraicPartialsRequestWithProgram;
const encodeAlgebraicPartialsRequestWithProgramAtGeneration = table_read_remote_wire.encodeAlgebraicPartialsRequestWithProgramAtGeneration;
const encodeAlgebraicExpressionPartialsRequest = table_read_remote_wire.encodeAlgebraicExpressionPartialsRequest;
const parseAlgebraicPartialsRequest = table_read_remote_wire.parseAlgebraicPartialsRequest;
const encodeAlgebraicPartialsResponse = table_read_remote_wire.encodeAlgebraicPartialsResponse;
const parseAlgebraicPartialsResponse = table_read_remote_wire.parseAlgebraicPartialsResponse;
const parsedAlgebraicTensorExpressionsAlloc = table_read_remote_wire.parsedAlgebraicTensorExpressionsAlloc;
const validateAlgebraicPartialsAccessPaths = table_read_remote_wire.validateAlgebraicPartialsAccessPaths;
const validateAlgebraicProgramPartialsAccessPaths = table_read_remote_wire.validateAlgebraicProgramPartialsAccessPaths;
const validateAlgebraicProgramPartialsProof = table_read_remote_wire.validateAlgebraicProgramPartialsProof;
const algebraicTensorProgramOutputExpressionsForIndexAlloc = table_read_remote_wire.algebraicTensorProgramOutputExpressionsForIndexAlloc;
const algebraicTensorAccessPathValuesAlloc = table_read_remote_wire.algebraicTensorAccessPathValuesAlloc;

const ParallelFanoutKind = table_read_fanout.ParallelFanoutKind;
const FanoutPlanReason = table_read_fanout.FanoutPlanReason;
const FanoutPlan = table_read_fanout.FanoutPlan;
pub const ParallelFanoutMetricsSnapshot = table_read_fanout.ParallelFanoutMetricsSnapshot;
const recordFanoutPlan = table_read_fanout.recordFanoutPlan;
const recordParallelFanout = table_read_fanout.recordParallelFanout;
const recordParallelFanoutFallback = table_read_fanout.recordParallelFanoutFallback;
pub const parallelFanoutMetricsSnapshot = table_read_fanout.parallelFanoutMetricsSnapshot;
const ioAsyncLimitCap = table_read_fanout.ioAsyncLimitCap;
const planFanout = table_read_fanout.planFanout;
const planQueryFanout = table_read_fanout.planQueryFanout;
const mergeDistributedTextStats = table_read_fanout.mergeDistributedTextStats;
const mergeDistributedBackgroundTextStats = table_read_fanout.mergeDistributedBackgroundTextStats;
const queryNeedsDistributedTextStats = table_read_fanout.queryNeedsDistributedTextStats;
const collectSignificantTermsFieldRequests = table_read_fanout.collectSignificantTermsFieldRequests;
const collectSignificantTermsBackgroundFieldRequests = table_read_fanout.collectSignificantTermsBackgroundFieldRequests;
const TextStatsFanoutSlot = table_read_fanout.TextStatsFanoutSlot;
const SearchFanoutSlot = table_read_fanout.SearchFanoutSlot;
const PreflightFanoutSlot = table_read_fanout.PreflightFanoutSlot;
const initTextStatsFanoutSlots = table_read_fanout.initTextStatsFanoutSlots;
const deinitTextStatsFanoutSlots = table_read_fanout.deinitTextStatsFanoutSlots;
const initSearchFanoutSlots = table_read_fanout.initSearchFanoutSlots;
const deinitSearchFanoutSlots = table_read_fanout.deinitSearchFanoutSlots;
const initPreflightFanoutSlots = table_read_fanout.initPreflightFanoutSlots;
const deinitPreflightFanoutSlots = table_read_fanout.deinitPreflightFanoutSlots;
const cloneRuntimePreflightSummary = table_read_fanout.cloneRuntimePreflightSummary;
const mergeRuntimePreflightSummary = table_read_fanout.mergeRuntimePreflightSummary;
const mergeRuntimePreflightSummaryNoFree = table_read_fanout.mergeRuntimePreflightSummaryNoFree;

fn benchQueryApiPhaseProfileEnabled() bool {
    return std.c.getenv("ANTFLY_BENCH_QUERY_API_PHASES\x00") != null or
        std.c.getenv("ANTFLY_BENCH_QUERY_PROFILE_EVERY\x00") != null;
}

fn nsToUsFloat(ns: u64) f64 {
    return @as(f64, @floatFromInt(ns)) / 1000.0;
}
const algebraic_law = db_mod.algebraic.law;
const algebraic_planner = db_mod.algebraic.planner;

pub const LookupResponse = table_read_core.LookupResponse;
pub const ScanResponse = table_read_core.ScanResponse;
pub const TextStatsResponse = table_read_core.TextStatsResponse;
pub const BackgroundTextStatsResponse = table_read_core.BackgroundTextStatsResponse;

pub const LoweredSqlReadPlanResult = table_read_relational_rows.LoweredSqlReadPlanResult;
pub const LoweredRelationPopulationRowsResult = table_read_relational_rows.LoweredRelationPopulationRowsResult;
const takeLoweredSqlReadRows = table_read_relational_rows.takeLoweredSqlReadRows;

pub const LsmStorageStats = table_read_core.LsmStorageStats;
pub const ParsedTextStatsHttpResponse = table_read_core.ParsedTextStatsHttpResponse;
const appendScanLine = table_read_core.appendScanLine;
pub const HAReadGate = table_read_core.HAReadGate;
pub const ReadPreparation = table_read_core.ReadPreparation;
pub const backend_current_root_generation = table_read_core.backend_current_root_generation;
pub const GroupVisibleRootGenerationSource = table_read_core.GroupVisibleRootGenerationSource;
pub const PrimaryLookupDbLease = table_read_core.PrimaryLookupDbLease;
pub const PrimaryLookupDbSource = table_read_core.PrimaryLookupDbSource;
const routePolicyForConsistency = table_read_core.routePolicyForConsistency;

pub const testing = if (builtin.is_test) struct {
    pub fn rejectResolvedDocFilterForCrossGroup(req: db_mod.types.SearchRequest, group_count: usize) !void {
        return rejectCrossGroupResolvedDocFilter(req, group_count);
    }

    pub fn rejectResolvedDocFilterForRemoteRoute(req: db_mod.types.SearchRequest, route: table_router.GroupRoute) !void {
        return rejectRemoteRouteResolvedDocFilter(req, route);
    }

    pub fn validateDocIdentityReadyForMultiGroupRead(
        alloc: std.mem.Allocator,
        catalog: table_catalog.CatalogSource,
        table_name: []const u8,
        group_count: usize,
    ) !void {
        return tableReadsValidateDocIdentityReadyForMultiGroup(alloc, catalog, table_name, group_count);
    }
} else struct {};

pub const ProvisionedTableReadCache = table_read_cache.ProvisionedTableReadCache;
const openProvisionedQueryDbForTable = table_read_cache.openProvisionedQueryDbForTable;
const openProvisionedQueryDbForTableWithRuntime = table_read_cache.openProvisionedQueryDbForTableWithRuntime;
const openProvisionedQueryDbForTableWithCache = table_read_cache.openProvisionedQueryDbForTableWithCache;
const loadTableIndexesJson = table_read_cache.loadTableIndexesJson;
const loadTableIdentityNamespaceForGroup = table_read_cache.loadTableIdentityNamespaceForGroup;
const validateProvisionedDbIdentityNamespace = table_read_cache.validateProvisionedDbIdentityNamespace;
const validateOpenedProvisionedDbIdentityNamespace = table_read_cache.validateOpenedProvisionedDbIdentityNamespace;
const DocIdentityInternalWorkerBoundary = table_read_core.DocIdentityInternalWorkerBoundary;
const DocIdentityInternalWorkerPolicy = table_read_core.DocIdentityInternalWorkerPolicy;
const docIdentityInternalWorkerPolicy = table_read_core.docIdentityInternalWorkerPolicy;

const LocalQueryExecution = struct {
    request: db_mod.types.SearchRequest,
    result: db_mod.types.SearchResult,
    dense_profile: ?query_api.QueryResponseMeta.DenseSearchProfile = null,
};

const ProfiledDenseQuery = struct {
    req: db_mod.types.SearchRequest,
    query: db_mod.types.DenseKnnQuery,
};

const ParsedAlgebraicPartialsRequest = table_read_remote_wire.ParsedAlgebraicPartialsRequest;
pub const searchRequestFromVectorWorkerEnvelope = table_read_remote_wire.searchRequestFromVectorWorkerEnvelope;

pub const RelationalRowsSourceGroupRequest = table_read_core.RelationalRowsSourceGroupRequest;
pub const TableReadSource = table_read_core.TableReadSource;

const AlgebraicVectorWorkerCandidate = struct {
    index_name: []const u8,
    layout: algebraic_ir.PhysicalLayout,
    query: query_contract.AlgebraicVectorWorkerQuery,
    k: u32,
};

fn algebraicVectorWorkerCandidateForSearchRequest(alloc: std.mem.Allocator, req: db_mod.types.SearchRequest) ?AlgebraicVectorWorkerCandidate {
    if (req.aggregations_json.len != 0 or
        req.full_text != null or
        req.full_text_queries.len != 0 or
        req.dense_queries.len != 0 or
        req.sparse_queries.len != 0 or
        req.graph_queries.len != 0 or
        req.merge_config != null or
        req.reranker != null or
        req.pruner != null or
        req.expand_strategy != null or
        req.distributed_text_stats.len != 0 or
        searchRequestHasUnserializableResolvedDocFilter(req))
    {
        return null;
    }
    if (req.filter_query_json.len != 0 and !algebraicVectorWorkerFilterJsonSupported(alloc, req.filter_query_json)) return null;
    if (req.exclusion_query_json.len != 0 and !algebraicVectorWorkerFilterJsonSupported(alloc, req.exclusion_query_json)) return null;

    if (req.dense) |dense| {
        if (req.sparse != null) return null;
        if (req.query != .match_all) return null;
        const index_name = req.index_name orelse return null;
        return .{ .index_name = index_name, .layout = .dense_vector, .query = .{ .dense = dense }, .k = dense.k };
    }
    if (req.sparse) |sparse| {
        if (req.query != .match_all) return null;
        const index_name = req.index_name orelse return null;
        return .{ .index_name = index_name, .layout = .sparse_vector, .query = .{ .sparse = sparse }, .k = sparse.k };
    }

    switch (req.query) {
        .dense_knn => |dense| {
            const index_name = req.index_name orelse return null;
            return .{ .index_name = index_name, .layout = .dense_vector, .query = .{ .dense = dense }, .k = dense.k };
        },
        .sparse_knn => |sparse| {
            const index_name = req.index_name orelse return null;
            return .{ .index_name = index_name, .layout = .sparse_vector, .query = .{ .sparse = sparse }, .k = sparse.k };
        },
        else => return null,
    }
}

fn algebraicVectorWorkerFilterJsonSupported(alloc: std.mem.Allocator, filter_query_json: []const u8) bool {
    if (filter_query_json.len == 0) return true;
    const constraints = algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .match_all = {} },
        .filter_query_json = filter_query_json,
    }) catch return false;
    const owned = constraints orelse return false;
    defer freeAlgebraicConstraints(alloc, owned);
    return true;
}

fn annotateVectorWorkerPreflight(
    alloc: std.mem.Allocator,
    summary: *db_mod.RuntimePreflightSummary,
    req: db_mod.types.SearchRequest,
) void {
    if (!searchRequestHasSingleVectorWorkerKnn(req)) return;
    summary.vector_worker_filter_constraint_count +|= vectorWorkerFilterConstraintCount(req);
    if (req.filter_query_json.len > 0 or req.exclusion_query_json.len > 0) {
        summary.vector_worker_requires_algebraic_filter_resolution = true;
    }
    if (algebraicVectorWorkerCandidateForSearchRequest(alloc, req) != null) {
        summary.vector_worker_candidate_count +|= 1;
    } else {
        summary.vector_worker_fallback_count +|= 1;
    }
}

fn searchRequestHasSingleVectorWorkerKnn(req: db_mod.types.SearchRequest) bool {
    var count: u32 = 0;
    if (req.dense != null) count += 1;
    if (req.sparse != null) count += 1;
    switch (req.query) {
        .dense_knn, .sparse_knn => count += 1,
        else => {},
    }
    return count == 1;
}

fn vectorWorkerFilterConstraintCount(req: db_mod.types.SearchRequest) u32 {
    var count: u32 = 0;
    if (req.filter_query_json.len > 0) count += 1;
    if (req.exclusion_query_json.len > 0) count += 1;
    if (req.filter_ids.len > 0) count += 1;
    if (req.exclude_ids.len > 0) count += 1;
    if (req.filter_doc_ids_positive or req.filter_doc_ids.len > 0) count += 1;
    if (req.exclude_doc_ids.len > 0) count += 1;
    if (searchRequestHasResolvedDocFilter(req)) count += 1;
    return count;
}

fn encodeAlgebraicVectorWorkerRequestForSearchRequestAlloc(
    alloc: std.mem.Allocator,
    req: db_mod.types.SearchRequest,
) !?[]u8 {
    const candidate = algebraicVectorWorkerCandidateForSearchRequest(alloc, req) orelse return null;
    const constraints = query_contract.nativeDocIdConstraintEnvelopeFromSearchRequest(req);
    var tensor_program = (try algebraic_planner.planVectorSearchTensorProgramAlloc(alloc, candidate.index_name, candidate.layout, constraints.hasConstraints())) orelse return null;
    defer tensor_program.deinit(alloc);
    return try query_contract.encodeAlgebraicVectorWorkerRequestEnvelopeAlloc(
        alloc,
        candidate.index_name,
        candidate.layout,
        candidate.query,
        .{
            .limit = req.limit,
            .offset = req.offset,
            .count_only = req.count_only,
            .profile = req.profile,
            .include_stored = req.include_stored,
            .fields = @constCast(req.fields),
            .filter_query_json = req.filter_query_json,
            .exclusion_query_json = req.exclusion_query_json,
            .filter_prefix = req.filter_prefix,
            .filter_ids = req.filter_ids,
            .exclude_ids = req.exclude_ids,
            .require_algebraic_filter_resolution = req.filter_query_json.len > 0 or req.exclusion_query_json.len > 0,
            .include_all_fields = req.include_all_fields,
            .defer_stored_projection = req.defer_stored_projection,
            .search_effort = req.search_effort,
            .distance_over = req.distance_over,
            .distance_under = req.distance_under,
            .return_mode = req.return_mode,
            .max_chunks_per_parent = req.max_chunks_per_parent,
            .identity_read_generation = req.identity_read_generation,
        },
        constraints,
        req.resolved_doc_filter,
        req.resolved_doc_filter_wire_context,
        tensor_program.access_paths,
        tensor_program.asProgram(),
    );
}

pub const BoundTableReadSource = struct {
    table_name: []const u8,
    db: *db_mod.DB,
    reads: raft_mod.FeatureDBReads,

    pub fn init(
        table_name: []const u8,
        group_id: u64,
        db: *db_mod.DB,
        requester: raft_mod.ReadableLeaseRequester,
    ) BoundTableReadSource {
        return .{
            .table_name = table_name,
            .db = db,
            .reads = raft_mod.FeatureDBReads.init(group_id, requester),
        };
    }

    pub fn source(self: *BoundTableReadSource) TableReadSource {
        return .{
            .ptr = self,
            .vtable = &.{
                .lookup = lookup,
                .scan = scan,
                .query = query,
                .document_algebraic_aggregate = BoundTableReadSource.documentAlgebraicAggregate,
                .document_algebraic_aggregate_group_local = BoundTableReadSource.documentAlgebraicAggregateGroupLocal,
                .preflight_query = preflightQuery,
                .preflight_query_group_local = preflightQueryGroupLocal,
                .lookup_group_local = lookupGroupLocal,
                .relational_unique_owner_lookup = relationalUniqueOwnerLookup,
                .relational_temporal_unique_owner_lookup = BoundTableReadSource.relationalTemporalUniqueOwnerLookup,
                .relational_temporal_unique_overlap_owner_lookup = BoundTableReadSource.relationalTemporalUniqueOverlapOwnerLookup,
                .relational_temporal_unique_owner_lookup_group_local = BoundTableReadSource.relationalTemporalUniqueOwnerLookupGroupLocal,
                .relational_temporal_unique_overlap_owner_lookup_group_local = BoundTableReadSource.relationalTemporalUniqueOverlapOwnerLookupGroupLocal,
                .rows_query_plan = rowsQueryPlan,
                .rows_query_plan_catalog = rowsQueryPlanCatalogNative,
                .rows_query_plan_system_time_as_of_sequence = rowsQueryPlanSystemTimeAsOfSequence,
                .rows_query_plan_catalog_system_time_as_of_sequence = rowsQueryPlanCatalogSystemTimeAsOfSequenceNative,
                .rows_set_operation_plan = rowsSetOperationPlan,
                .rows_aggregate_plan = rowsAggregatePlan,
                .rows_window_plan = rowsWindowPlan,
                .rows_join_plan = rowsJoinPlan,
                .rows_lateral_plan = rowsLateralPlan,
                .scan_group_local = scanGroupLocal,
                .query_group_local = queryGroupLocal,
                .search_result_group_local = searchResultGroupLocal,
                .text_stats_group_local = textStatsGroupLocal,
                .algebraic_partials_group_local = algebraicPartialsGroupLocal,
                .join_partition_group_local = null,
                .join_rows_group_local = null,
                .join_unmatched_group_local = null,
                .join_finalize_group_local = null,
                .graph_expand_group_local = graphExpandGroupLocal,
                .graph_hydrate_group_local = graphHydrateGroupLocal,
                .graph_edges_group_local = graphEdgesGroupLocal,
                .local_runtime_statuses = localRuntimeStatuses,
                .lsm_storage_stats = lsmStorageStats,
            },
        };
    }

    fn lsmStorageStats(
        ptr: *anyopaque,
        table_name: []const u8,
    ) !?LsmStorageStats {
        const self: *BoundTableReadSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table_name)) return null;
        return .{
            .maintenance = self.db.snapshotLsmMaintenanceStats(),
            .write = self.db.snapshotLsmWriteStats(),
        };
    }

    fn localRuntimeStatuses(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
    ) !?runtime_status.LocalTableRuntimeStatuses {
        const self: *BoundTableReadSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table_name)) return null;
        const items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
        items[0] = .{
            .group_id = self.reads.group_id,
            .stats = try self.db.runtimeStatusStatsConsistent(alloc),
        };
        return .{ .items = items };
    }

    fn lookup(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        key: []const u8,
        opts: db_mod.types.LookupOptions,
        consistency: raft_mod.ReadConsistency,
    ) !?LookupResponse {
        const self: *BoundTableReadSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;

        var result = (try self.reads.lookupWithConsistency(alloc, self.db, key, opts, consistency)) orelse return null;
        defer result.deinit(alloc);

        return .{
            .json = try alloc.dupe(u8, result.json),
            .version = try self.db.getTimestamp(alloc, key),
        };
    }

    fn scan(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        from_key: []const u8,
        to_key: []const u8,
        opts: db_mod.types.ScanOptions,
        consistency: raft_mod.ReadConsistency,
    ) !?ScanResponse {
        const self: *BoundTableReadSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;

        var result = try self.reads.scanWithConsistency(alloc, self.db, from_key, to_key, opts, consistency);
        defer result.deinit(alloc);

        var out = std.ArrayListUnmanaged(u8).empty;
        defer out.deinit(alloc);

        for (result.hashes, 0..) |entry, i| {
            const json = if (opts.include_documents) result.documents[i].json else null;
            try appendScanLine(alloc, &out, entry.id, json);
        }

        return .{
            .ndjson = try out.toOwnedSlice(alloc),
        };
    }

    fn query(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        req: db_mod.types.SearchRequest,
        consistency: raft_mod.ReadConsistency,
    ) !?query_api.QueryResponse {
        const self: *BoundTableReadSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;

        const start_ns = platform_time.monotonicNs();
        const phase_profile = benchQueryApiPhaseProfileEnabled();
        const prepare_start_ns = if (phase_profile) platform_time.monotonicNs() else 0;
        try self.reads.reads.prepareSearchWithConsistency(self.reads.group_id, req, consistency);
        const prepare_ns = if (phase_profile) platform_time.monotonicNs() - prepare_start_ns else 0;
        const snapshot_start_ns = if (phase_profile) platform_time.monotonicNs() else 0;
        const snapshot_req = try self.db.searchRequestAtCurrentIdentityGeneration(req);
        const snapshot_ns = if (phase_profile) platform_time.monotonicNs() - snapshot_start_ns else 0;
        var execution: LocalQueryExecution = .{ .request = snapshot_req, .result = undefined };
        const search_start_ns = if (phase_profile) platform_time.monotonicNs() else 0;
        if (profiledDenseQuery(snapshot_req)) |dense| {
            const profiled = try self.db.searchDenseProfiled(alloc, dense.req, dense.query);
            execution = .{
                .request = snapshot_req,
                .result = profiled.result,
                .dense_profile = mapDenseSearchProfile(profiled.profile),
            };
        } else {
            execution = .{
                .request = snapshot_req,
                .result = try self.db.search(alloc, snapshot_req),
            };
        }
        const search_ns = if (phase_profile) platform_time.monotonicNs() - search_start_ns else 0;
        var result = execution.result;
        defer result.deinit();
        const response_req = execution.request;
        var meta: query_api.QueryResponseMeta = .{
            .took_ms = @intCast(@divTrunc(platform_time.monotonicNs() - start_ns, std.time.ns_per_ms)),
            .shard_count = 1,
            .dense_search = execution.dense_profile,
        };
        defer meta.deinit(alloc);
        const agg_start_ns = if (phase_profile) platform_time.monotonicNs() else 0;
        try applyBoundQueryAggregations(self, alloc, response_req, &result, &meta, consistency);
        const agg_ns = if (phase_profile) platform_time.monotonicNs() - agg_start_ns else 0;
        const post_start_ns = if (phase_profile) platform_time.monotonicNs() else 0;
        try applyQueryPostProcessing(alloc, response_req, &result, &meta, null, null);
        const post_ns = if (phase_profile) platform_time.monotonicNs() - post_start_ns else 0;
        const encode_start_ns = if (phase_profile) platform_time.monotonicNs() else 0;
        const response = try query_api.encodeQueryResponses(alloc, table_name, response_req, meta, result);
        if (phase_profile) {
            const encode_ns = platform_time.monotonicNs() - encode_start_ns;
            const total_ns = platform_time.monotonicNs() - start_ns;
            std.debug.print(
                "antfly_bench_query_api_phases prepare_us={d:.3} snapshot_us={d:.3} search_us={d:.3} aggregation_us={d:.3} post_us={d:.3} encode_us={d:.3} total_us={d:.3} hits={d} total_hits={d} response_bytes={d}\n",
                .{
                    nsToUsFloat(prepare_ns),
                    nsToUsFloat(snapshot_ns),
                    nsToUsFloat(search_ns),
                    nsToUsFloat(agg_ns),
                    nsToUsFloat(post_ns),
                    nsToUsFloat(encode_ns),
                    nsToUsFloat(total_ns),
                    result.hits.len,
                    result.total_hits,
                    response.json.len,
                },
            );
        }
        return response;
    }

    fn preflightQuery(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        req: db_mod.types.SearchRequest,
        consistency: raft_mod.ReadConsistency,
        max_work: u32,
    ) !?db_mod.RuntimePreflightSummary {
        const self: *BoundTableReadSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        try self.reads.reads.prepareSearchWithConsistency(self.reads.group_id, req, consistency);
        var summary = try self.db.preflightSearchRequest(alloc, req, max_work);
        annotateVectorWorkerPreflight(alloc, &summary, req);
        return summary;
    }

    fn preflightQueryGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        _: u64,
        table_name: []const u8,
        req: db_mod.types.SearchRequest,
        consistency: raft_mod.ReadConsistency,
        max_work: u32,
    ) !?db_mod.RuntimePreflightSummary {
        return try preflightQuery(ptr, alloc, table_name, req, consistency, max_work);
    }

    fn lookupGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        _: u64,
        table_name: []const u8,
        key: []const u8,
        opts: db_mod.types.LookupOptions,
        consistency: raft_mod.ReadConsistency,
    ) !?LookupResponse {
        return try lookup(ptr, alloc, table_name, key, opts, consistency);
    }

    fn relationalUniqueOwnerLookup(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        constraint_name: []const u8,
        encoded_value: []const u8,
        consistency: raft_mod.ReadConsistency,
    ) !?[]u8 {
        const self: *BoundTableReadSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        return try lookupRelationalUniqueOwnerInDb(alloc, self.db, self.reads.group_id, self.reads.reads, constraint_name, encoded_value, consistency);
    }

    fn relationalTemporalUniqueOwnerLookup(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        constraint_name: []const u8,
        encoded_value: []const u8,
        encoded_point: []const u8,
        consistency: raft_mod.ReadConsistency,
    ) !?[]u8 {
        const self: *BoundTableReadSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        return try lookupRelationalTemporalUniqueOwnerInDb(alloc, self.db, self.reads.group_id, self.reads.reads, constraint_name, encoded_value, encoded_point, consistency);
    }

    fn relationalTemporalUniqueOverlapOwnerLookup(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        constraint_name: []const u8,
        encoded_value: []const u8,
        encoded_start: []const u8,
        encoded_end: []const u8,
        consistency: raft_mod.ReadConsistency,
    ) !?[]u8 {
        const self: *BoundTableReadSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        return try lookupRelationalTemporalUniqueOverlapOwnerInDb(alloc, self.db, self.reads.group_id, self.reads.reads, constraint_name, encoded_value, encoded_start, encoded_end, consistency);
    }

    fn relationalTemporalUniqueOwnerLookupGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        constraint_name: []const u8,
        encoded_value: []const u8,
        encoded_point: []const u8,
        consistency: raft_mod.ReadConsistency,
    ) !?[]u8 {
        const self: *BoundTableReadSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        return try lookupRelationalTemporalUniqueOwnerInDb(alloc, self.db, group_id, self.reads.reads, constraint_name, encoded_value, encoded_point, consistency);
    }

    fn relationalTemporalUniqueOverlapOwnerLookupGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        constraint_name: []const u8,
        encoded_value: []const u8,
        encoded_start: []const u8,
        encoded_end: []const u8,
        consistency: raft_mod.ReadConsistency,
    ) !?[]u8 {
        const self: *BoundTableReadSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        return try lookupRelationalTemporalUniqueOverlapOwnerInDb(alloc, self.db, group_id, self.reads.reads, constraint_name, encoded_value, encoded_start, encoded_end, consistency);
    }

    fn prepareRelationalRowsFullTableRead(self: *BoundTableReadSource, consistency: raft_mod.ReadConsistency) !void {
        try self.reads.reads.prepareScanWithConsistency(self.reads.group_id, "", "", .{}, consistency);
    }

    fn rowsQueryPlan(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        runtime_schema: storage_schema.TableSchema,
        plan: db_mod.types.RelationalRowsQueryPlan,
        consistency: raft_mod.ReadConsistency,
    ) !?db_mod.types.RelationalRowsQueryResult {
        const self: *BoundTableReadSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        try self.prepareRelationalRowsFullTableRead(consistency);
        return try self.db.queryRelationalRowsPlan(alloc, runtime_schema, plan);
    }

    fn rowsQueryPlanCatalogNative(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        target: catalog_resources.TableTarget,
        runtime_schema: storage_schema.TableSchema,
        plan: db_mod.types.RelationalRowsQueryPlan,
        consistency: raft_mod.ReadConsistency,
    ) !?db_mod.types.RelationalRowsQueryResult {
        return try rowsQueryPlan(ptr, alloc, target.table_name, runtime_schema, plan, consistency);
    }

    fn rowsQueryPlanSystemTimeAsOfSequence(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        runtime_schema: storage_schema.TableSchema,
        commit_sequence: u64,
        plan: db_mod.types.RelationalRowsQueryPlan,
        consistency: raft_mod.ReadConsistency,
    ) !?db_mod.types.RelationalRowsQueryResult {
        const self: *BoundTableReadSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        if (plan.ctes.len != 0 or plan.ranges.len != 0) return error.UnsupportedRowsQuery;
        try self.prepareRelationalRowsFullTableRead(consistency);
        return try self.db.querySystemVersionedRelationalRowsAsOfSequence(alloc, runtime_schema, commit_sequence, plan.query);
    }

    fn rowsQueryPlanCatalogSystemTimeAsOfSequenceNative(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        target: catalog_resources.TableTarget,
        runtime_schema: storage_schema.TableSchema,
        commit_sequence: u64,
        plan: db_mod.types.RelationalRowsQueryPlan,
        consistency: raft_mod.ReadConsistency,
    ) !?db_mod.types.RelationalRowsQueryResult {
        return try rowsQueryPlanSystemTimeAsOfSequence(ptr, alloc, target.table_name, runtime_schema, commit_sequence, plan, consistency);
    }

    fn rowsSetOperationPlan(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        runtime_schema: storage_schema.TableSchema,
        plan: db_mod.types.RelationalRowsSetOperationPlan,
        consistency: raft_mod.ReadConsistency,
    ) !?db_mod.types.RelationalRowsQueryResult {
        const self: *BoundTableReadSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        try self.prepareRelationalRowsFullTableRead(consistency);
        return try self.db.queryRelationalRowsSetOperationPlan(alloc, runtime_schema, plan);
    }

    fn rowsAggregatePlan(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        runtime_schema: storage_schema.TableSchema,
        plan: db_mod.types.RelationalRowsAggregatePlan,
        consistency: raft_mod.ReadConsistency,
    ) !?db_mod.types.RelationalRowsAggregateResult {
        const self: *BoundTableReadSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        try self.prepareRelationalRowsFullTableRead(consistency);
        return try self.db.aggregateRelationalRowsPlan(alloc, runtime_schema, plan);
    }

    fn documentAlgebraicAggregate(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        req: document_sql_runtime.AlgebraicAggregateRequest,
        consistency: raft_mod.ReadConsistency,
    ) !?document_sql_runtime.AlgebraicAggregateResponse {
        const self: *BoundTableReadSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        try self.prepareRelationalRowsFullTableRead(consistency);
        return try documentAlgebraicAggregateFromDbAlloc(alloc, self.db, req);
    }

    fn documentAlgebraicAggregateGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        _: u64,
        table_name: []const u8,
        req: document_sql_runtime.AlgebraicAggregateRequest,
        consistency: raft_mod.ReadConsistency,
    ) !?document_sql_runtime.AlgebraicAggregateResponse {
        return try documentAlgebraicAggregate(ptr, alloc, table_name, req, consistency);
    }

    fn rowsWindowPlan(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        runtime_schema: storage_schema.TableSchema,
        plan: db_mod.types.RelationalRowsWindowPlan,
        consistency: raft_mod.ReadConsistency,
    ) !?db_mod.types.RelationalRowsWindowResult {
        const self: *BoundTableReadSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        try self.prepareRelationalRowsFullTableRead(consistency);
        return try self.db.windowRelationalRowsPlan(alloc, runtime_schema, plan);
    }

    fn rowsJoinPlan(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        runtime_schema: storage_schema.TableSchema,
        plan: db_mod.types.RelationalRowsJoinPlan,
        consistency: raft_mod.ReadConsistency,
    ) !?db_mod.types.RelationalRowsJoinResult {
        const self: *BoundTableReadSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        try self.prepareRelationalRowsFullTableRead(consistency);
        return try self.db.joinRelationalRowsPlan(alloc, runtime_schema, plan);
    }

    fn rowsLateralPlan(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        runtime_schema: storage_schema.TableSchema,
        plan: db_mod.types.RelationalRowsLateralPlan,
        consistency: raft_mod.ReadConsistency,
    ) !?db_mod.types.RelationalRowsJoinResult {
        const self: *BoundTableReadSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        try self.prepareRelationalRowsFullTableRead(consistency);
        return try self.db.lateralRelationalRowsPlan(alloc, runtime_schema, plan);
    }

    fn scanGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        _: u64,
        table_name: []const u8,
        from_key: []const u8,
        to_key: []const u8,
        opts: db_mod.types.ScanOptions,
        consistency: raft_mod.ReadConsistency,
    ) !?ScanResponse {
        return try scan(ptr, alloc, table_name, from_key, to_key, opts, consistency);
    }

    fn queryGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        _: u64,
        table_name: []const u8,
        req: db_mod.types.SearchRequest,
        consistency: raft_mod.ReadConsistency,
    ) !?query_api.QueryResponse {
        return try query(ptr, alloc, table_name, req, consistency);
    }

    fn searchResultGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        _: u64,
        table_name: []const u8,
        req: db_mod.types.SearchRequest,
        consistency: raft_mod.ReadConsistency,
    ) !?db_mod.types.SearchResult {
        const self: *BoundTableReadSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        return try self.reads.searchWithConsistency(alloc, self.db, req, consistency);
    }

    fn textStatsGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        _: u64,
        table_name: []const u8,
        body: []const u8,
    ) !?query_api.QueryResponse {
        const self: *BoundTableReadSource = @ptrCast(@alignCast(ptr));
        return try collectBoundLocalTextStats(self, alloc, table_name, body);
    }

    fn algebraicPartialsGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        _: u64,
        table_name: []const u8,
        body: []const u8,
    ) !?query_api.QueryResponse {
        const self: *BoundTableReadSource = @ptrCast(@alignCast(ptr));
        return try collectBoundLocalAlgebraicPartials(self, alloc, table_name, body);
    }

    fn graphExpandGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        _: u64,
        table_name: []const u8,
        req: distributed_graph.GraphExpandRequest,
        consistency: raft_mod.ReadConsistency,
    ) !?distributed_graph.GraphExpandResponse {
        const self: *BoundTableReadSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;

        const Context = struct {
            source: *BoundTableReadSource,
            consistency: raft_mod.ReadConsistency,

            fn search(ctx: @This(), allocator: std.mem.Allocator, search_req: db_mod.types.SearchRequest) anyerror!db_mod.types.SearchResult {
                return try ctx.source.reads.searchWithConsistency(allocator, ctx.source.db, search_req, ctx.consistency);
            }
        };
        return try graphExpandWithSearch(Context, alloc, req, .{ .source = self, .consistency = consistency }, Context.search);
    }

    fn graphHydrateGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        _: u64,
        table_name: []const u8,
        req: distributed_graph.GraphHydrateRequest,
        consistency: raft_mod.ReadConsistency,
    ) !?distributed_graph.GraphHydrateResponse {
        const self: *BoundTableReadSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        if (req.topology_epoch != 0) return error.TopologyChanged;

        var hits = std.ArrayListUnmanaged(db_mod.types.SearchHit).empty;
        errdefer {
            for (hits.items) |*hit| hit.deinit(alloc);
            hits.deinit(alloc);
        }

        for (req.keys) |key| {
            var result = (try self.reads.lookupWithConsistency(alloc, self.db, key, .{}, consistency)) orelse continue;
            defer result.deinit(alloc);
            try hits.append(alloc, .{
                .id = try alloc.dupe(u8, key),
                .doc_ordinal = try self.db.lookupLiveDocOrdinalForInternalRead(alloc, key, req.identity_read_generation),
                .stored_data = try alloc.dupe(u8, result.json),
            });
        }
        return .{ .hits = try hits.toOwnedSlice(alloc) };
    }

    fn graphEdgesGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        _: u64,
        table_name: []const u8,
        req: distributed_graph.GraphEdgesRequest,
        _: raft_mod.ReadConsistency,
    ) !?distributed_graph.GraphEdgesResponse {
        const self: *BoundTableReadSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        if (req.topology_epoch != 0) return error.TopologyChanged;
        try distributed_graph.validateGraphEdgesTensorAccessPath(alloc, req);
        const edges = try self.db.getEdges(alloc, req.index_name, req.key, "", req.direction);
        return .{ .edges = edges };
    }
};

const documentAlgebraicAggregateFromDbAlloc = table_read_document_sql.aggregateFromDbAlloc;
const documentAlgebraicAggregateMergeResponsesAlloc = table_read_document_sql.mergeResponsesAlloc;
const documentAlgebraicAggregateProvisionedHostedLocal = table_read_document_sql.aggregateProvisionedHostedLocal;
const aggregationContextForDb = table_read_document_sql.aggregationContextForDb;
const algebraicIndexFreshEnoughForRequest = table_read_document_sql.algebraicIndexFreshEnoughForRequest;
const algebraicIndexFreshEnoughForName = table_read_document_sql.algebraicIndexFreshEnoughForName;
const canConsiderAlgebraicAggregations = table_read_document_sql.canConsiderAlgebraicAggregations;
const requestWithResultIdentityGeneration = table_read_document_sql.requestWithResultIdentityGeneration;
const identityGenerationForAggregationFullResultRerun = table_read_document_sql.identityGenerationForAggregationFullResultRerun;
const aggregationFirstPassIsComplete = table_read_document_sql.aggregationFirstPassIsComplete;
const aggregationFullScanLimit = table_read_document_sql.aggregationFullScanLimit;
const aggregationDistributedFullScanLimit = table_read_document_sql.aggregationDistributedFullScanLimit;

pub const ProvisionedTableReadSource = struct {
    replica_root_dir: []const u8,
    catalog: table_catalog.CatalogSource,
    requester: raft_mod.ReadableLeaseRequester,
    io_impl: ?*std.Io.Threaded = null,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime = null,
    cache: ?*ProvisionedTableReadCache = null,
    runtime_status_cache: ?*runtime_status.TableRuntimeSnapshotCache = null,
    prepare_for_read: ?ReadPreparation = null,
    group_visible_root_generation: ?GroupVisibleRootGenerationSource = null,
    primary_lookup_db: ?PrimaryLookupDbSource = null,
    ha_read_gate: ?HAReadGate = null,
    antfly_provider: ?managed_embedder.AntflyProvider = null,
    secret_store: ?*common_secrets.FileStore = null,
    remote_content: ?*const scraping.RemoteContentConfig = null,

    pub fn init(
        replica_root_dir: []const u8,
        catalog: table_catalog.CatalogSource,
        requester: raft_mod.ReadableLeaseRequester,
    ) ProvisionedTableReadSource {
        return .{
            .replica_root_dir = replica_root_dir,
            .catalog = catalog,
            .requester = requester,
        };
    }

    pub fn withIo(self: *ProvisionedTableReadSource, io_impl: *std.Io.Threaded) *ProvisionedTableReadSource {
        self.io_impl = io_impl;
        return self;
    }

    pub fn withAntflyProvider(
        self: *ProvisionedTableReadSource,
        provider: ?managed_embedder.AntflyProvider,
    ) *ProvisionedTableReadSource {
        self.antfly_provider = provider;
        if (self.cache) |cache| cache.antfly_provider = provider;
        return self;
    }

    pub fn withSecretStore(
        self: *ProvisionedTableReadSource,
        secret_store: ?*common_secrets.FileStore,
    ) *ProvisionedTableReadSource {
        self.secret_store = secret_store;
        if (self.cache) |cache| cache.secret_store = secret_store;
        return self;
    }

    pub fn withRemoteContent(
        self: *ProvisionedTableReadSource,
        remote_content: ?*const scraping.RemoteContentConfig,
    ) *ProvisionedTableReadSource {
        self.remote_content = remote_content;
        if (self.cache) |cache| cache.remote_content = remote_content;
        return self;
    }

    pub fn withGroupVisibleRootGeneration(
        self: *ProvisionedTableReadSource,
        generation_source: ?GroupVisibleRootGenerationSource,
    ) *ProvisionedTableReadSource {
        self.group_visible_root_generation = generation_source;
        return self;
    }

    pub fn withHAReadGate(
        self: *ProvisionedTableReadSource,
        gate: ?HAReadGate,
    ) *ProvisionedTableReadSource {
        self.ha_read_gate = gate;
        return self;
    }

    fn ensureHAReadAllowed(self: *ProvisionedTableReadSource, consistency: raft_mod.ReadConsistency) !void {
        if (self.ha_read_gate) |gate| try gate.check(consistency);
    }

    pub fn source(self: *ProvisionedTableReadSource) TableReadSource {
        return .{
            .ptr = self,
            .vtable = &.{
                .lookup = lookup,
                .lookup_catalog = lookupCatalogNative,
                .scan = scan,
                .scan_catalog = scanCatalogNative,
                .query = query,
                .query_catalog = queryCatalogNative,
                .document_algebraic_aggregate = ProvisionedTableReadSource.documentAlgebraicAggregate,
                .document_algebraic_aggregate_catalog = ProvisionedTableReadSource.documentAlgebraicAggregateCatalog,
                .document_algebraic_aggregate_group_local = ProvisionedTableReadSource.documentAlgebraicAggregateGroupLocal,
                .preflight_query = preflightQuery,
                .preflight_query_group_local = preflightQueryGroupLocal,
                .lookup_group_local = lookupGroupLocal,
                .relational_unique_owner_lookup = relationalUniqueOwnerLookup,
                .relational_temporal_unique_owner_lookup = ProvisionedTableReadSource.relationalTemporalUniqueOwnerLookup,
                .relational_temporal_unique_overlap_owner_lookup = ProvisionedTableReadSource.relationalTemporalUniqueOverlapOwnerLookup,
                .relational_temporal_unique_owner_lookup_group_local = ProvisionedTableReadSource.relationalTemporalUniqueOwnerLookupGroupLocal,
                .relational_temporal_unique_overlap_owner_lookup_group_local = ProvisionedTableReadSource.relationalTemporalUniqueOverlapOwnerLookupGroupLocal,
                .rows_query_plan = rowsQueryPlan,
                .rows_query_plan_catalog = rowsQueryPlanCatalogNative,
                .rows_query_plan_system_time_as_of_sequence = rowsQueryPlanSystemTimeAsOfSequence,
                .rows_query_plan_catalog_system_time_as_of_sequence = rowsQueryPlanCatalogSystemTimeAsOfSequenceNative,
                .rows_set_operation_plan = rowsSetOperationPlan,
                .rows_set_operation_plan_catalog = rowsSetOperationPlanCatalogNative,
                .rows_aggregate_plan = rowsAggregatePlan,
                .rows_window_plan = rowsWindowPlan,
                .rows_join_plan = rowsJoinPlan,
                .rows_lateral_plan = rowsLateralPlan,
                .scan_group_local = scanGroupLocal,
                .query_group_local = queryGroupLocal,
                .search_result_group_local = searchResultGroupLocal,
                .text_stats_group_local = textStatsGroupLocal,
                .algebraic_partials_group_local = algebraicPartialsGroupLocal,
                .join_partition_group_local = null,
                .join_rows_group_local = null,
                .join_unmatched_group_local = null,
                .join_finalize_group_local = null,
                .graph_expand_group_local = graphExpandGroupLocal,
                .graph_hydrate_group_local = graphHydrateGroupLocal,
                .graph_edges_group_local = graphEdgesGroupLocal,
                .local_runtime_statuses = localRuntimeStatuses,
                .local_runtime_statuses_catalog = localRuntimeStatusesCatalogNative,
            },
        };
    }

    pub fn warmTableGroup(self: *ProvisionedTableReadSource, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8) !void {
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);
        if (self.prepare_for_read) |prep| prep.prepareForRead(table_name, .general);

        // Warmup must not pin a full managed query handle or run metadata-driven
        // query-open reconciliation for a just-created table. That work is
        // heavier than the startup/cache warmup needs, and it can block or
        // destabilize startup before the first real read. Keep warmup to a
        // lightweight status-only open instead; actual query handles still open
        // lazily on demand.
        var db = try openProvisionedWarmStatusDbForTable(
            alloc,
            path,
            self.visibleRootGeneration(group_id),
            self.backend_runtime,
            try loadTableIdentityNamespaceForGroup(alloc, self.catalog, table_name, group_id),
        );
        db.close();
    }

    fn visibleRootGeneration(self: *const ProvisionedTableReadSource, group_id: u64) u64 {
        return if (self.group_visible_root_generation) |generation_source| generation_source.visibleRootGenerationForGroup(group_id) else backend_current_root_generation;
    }

    fn lookup(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        key: []const u8,
        opts: db_mod.types.LookupOptions,
        consistency: raft_mod.ReadConsistency,
    ) !?LookupResponse {
        const self: *ProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        try self.ensureHAReadAllowed(consistency);
        if (self.prepare_for_read) |prep| prep.prepareForRead(table_name, .general);
        const group_id = (try table_catalog.resolveGroupForKey(alloc, self.catalog, table_name, key)) orelse return null;
        return try lookupProvisionedHostedLocal(self.primary_lookup_db, self.cache, self.replica_root_dir, self.catalog, self.requester, alloc, group_id, self.visibleRootGeneration(group_id), self.backend_runtime, table_name, key, opts, consistency);
    }

    fn lookupCatalogNative(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        target: catalog_resources.TableTarget,
        key: []const u8,
        opts: db_mod.types.LookupOptions,
        consistency: raft_mod.ReadConsistency,
    ) !?LookupResponse {
        const self: *ProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        const table_name = try nativeCatalogTableNameAlloc(alloc, self.catalog, target);
        defer alloc.free(table_name);
        return try lookup(ptr, alloc, table_name, key, opts, consistency);
    }

    fn documentArtifactManifest(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        doc_key: []const u8,
        artifact_name: []const u8,
        consistency: raft_mod.ReadConsistency,
    ) !?db_mod.types.DocumentArtifactManifest {
        const self: *ProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        try self.ensureHAReadAllowed(consistency);
        if (self.prepare_for_read) |prep| prep.prepareForRead(table_name, .general);
        const group_id = (try table_catalog.resolveGroupForKey(alloc, self.catalog, table_name, doc_key)) orelse return null;
        return try documentArtifactManifestProvisionedHostedLocal(self.cache, self.replica_root_dir, self.catalog, self.requester, alloc, group_id, self.visibleRootGeneration(group_id), self.backend_runtime, table_name, doc_key, artifact_name, consistency);
    }

    fn documentArtifactManifests(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        doc_key: []const u8,
        consistency: raft_mod.ReadConsistency,
    ) !?db_mod.types.DocumentArtifactManifestList {
        const self: *ProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        try self.ensureHAReadAllowed(consistency);
        if (self.prepare_for_read) |prep| prep.prepareForRead(table_name, .general);
        const group_id = (try table_catalog.resolveGroupForKey(alloc, self.catalog, table_name, doc_key)) orelse return null;
        return try documentArtifactManifestsProvisionedHostedLocal(self.cache, self.replica_root_dir, self.catalog, self.requester, alloc, group_id, self.visibleRootGeneration(group_id), self.backend_runtime, table_name, doc_key, consistency);
    }

    fn scanCatalogNative(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        target: catalog_resources.TableTarget,
        from_key: []const u8,
        to_key: []const u8,
        opts: db_mod.types.ScanOptions,
        consistency: raft_mod.ReadConsistency,
    ) !?ScanResponse {
        const self: *ProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        const table_name = try nativeCatalogTableNameAlloc(alloc, self.catalog, target);
        defer alloc.free(table_name);
        return try scan(ptr, alloc, table_name, from_key, to_key, opts, consistency);
    }

    fn scan(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        from_key: []const u8,
        to_key: []const u8,
        opts: db_mod.types.ScanOptions,
        consistency: raft_mod.ReadConsistency,
    ) !?ScanResponse {
        const self: *ProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        try self.ensureHAReadAllowed(consistency);
        if (self.prepare_for_read) |prep| prep.prepareForRead(table_name, .general);
        const group_ids = try table_catalog.resolveGroupsForSpan(alloc, self.catalog, table_name, from_key, to_key);
        defer alloc.free(group_ids);
        if (group_ids.len == 0) return null;
        try tableReadsValidateDocIdentityReadyForMultiGroup(alloc, self.catalog, table_name, group_ids.len);
        var out = std.ArrayListUnmanaged(u8).empty;
        defer out.deinit(alloc);

        var emitted: u32 = 0;
        for (group_ids) |group_id| {
            var group_opts = opts;
            if (opts.limit > 0) {
                if (emitted >= opts.limit) break;
                group_opts.limit = opts.limit - emitted;
            }

            var result = (try scanProvisionedHostedLocal(self.cache, self.replica_root_dir, self.catalog, self.requester, alloc, group_id, self.visibleRootGeneration(group_id), self.backend_runtime, table_name, from_key, to_key, group_opts, consistency)) orelse continue;
            defer result.deinit(alloc);
            try out.appendSlice(alloc, result.ndjson);
            emitted += @intCast(std.mem.count(u8, result.ndjson, "\n"));
        }
        return .{ .ndjson = try out.toOwnedSlice(alloc) };
    }

    fn query(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        req: db_mod.types.SearchRequest,
        consistency: raft_mod.ReadConsistency,
    ) !?query_api.QueryResponse {
        const self: *ProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        try self.ensureHAReadAllowed(consistency);
        if (self.prepare_for_read) |prep| prep.prepareForRead(table_name, readPreparationKindForQuery(req));
        const group_ids = try table_catalog.resolveGroupsForSpan(alloc, self.catalog, table_name, "", "");
        defer alloc.free(group_ids);
        if (group_ids.len == 0) return null;
        try tableReadsValidateDocIdentityReadyForMultiGroup(alloc, self.catalog, table_name, group_ids.len);
        if (group_ids.len > 1) try distributed_graph.rejectUnstampedResultRefs(req);
        const start_ns = platform_time.monotonicNs();
        if (group_ids.len == 1 and !distributed_graph.supportsCrossRange(req)) {
            const execution = try queryHostedLocalDetailed(self.cache, self.replica_root_dir, self.catalog, self.requester, alloc, group_ids[0], self.visibleRootGeneration(group_ids[0]), self.backend_runtime, self.antfly_provider, self.secret_store, self.remote_content, table_name, req, consistency);
            var result = execution.result;
            defer result.deinit();
            const response_req = execution.request;
            var meta: query_api.QueryResponseMeta = .{
                .took_ms = @intCast(@divTrunc(platform_time.monotonicNs() - start_ns, std.time.ns_per_ms)),
                .shard_count = 1,
                .dense_search = execution.dense_profile,
            };
            defer meta.deinit(alloc);
            try applyProvisionedQueryAggregations(self, alloc, group_ids, table_name, response_req, &result, &meta, consistency);
            try applyQueryPostProcessing(alloc, response_req, &result, &meta, self.antfly_provider, self.secret_store);
            return try query_api.encodeQueryResponses(alloc, table_name, response_req, meta, result);
        }

        if (group_ids.len > 1 and distributed_graph.supportsCrossRange(req)) {
            var base_req = req;
            base_req.graph_queries = &.{};
            base_req.expand_strategy = null;
            var merged = try queryProvisionedAcrossGroups(self, alloc, group_ids, base_req, table_name, consistency);
            defer merged.deinit();
            const graph_req = requestWithResultIdentityGeneration(req, merged);

            const worker = provisionedGraphWorker(self);
            const graph_results = try distributed_graph.executeCrossRange(alloc, self.catalog, worker, table_name, graph_req, merged, consistency);
            merged.graph_results = graph_results;

            var meta: query_api.QueryResponseMeta = .{
                .took_ms = @intCast(@divTrunc(platform_time.monotonicNs() - start_ns, std.time.ns_per_ms)),
                .shard_count = @intCast(group_ids.len),
                .merged = true,
            };
            defer meta.deinit(alloc);
            try applyProvisionedQueryAggregations(self, alloc, group_ids, table_name, graph_req, &merged, &meta, consistency);
            try applyQueryPostProcessing(alloc, graph_req, &merged, &meta, self.antfly_provider, self.secret_store);
            return try query_api.encodeQueryResponses(alloc, table_name, graph_req, meta, merged);
        }
        var merged = try queryProvisionedAcrossGroups(self, alloc, group_ids, req, table_name, consistency);
        defer merged.deinit();
        var meta: query_api.QueryResponseMeta = .{
            .took_ms = @intCast(@divTrunc(platform_time.monotonicNs() - start_ns, std.time.ns_per_ms)),
            .shard_count = @intCast(group_ids.len),
            .merged = group_ids.len > 1,
        };
        defer meta.deinit(alloc);
        try applyProvisionedQueryAggregations(self, alloc, group_ids, table_name, req, &merged, &meta, consistency);
        try applyQueryPostProcessing(alloc, req, &merged, &meta, self.antfly_provider, self.secret_store);
        return try query_api.encodeQueryResponses(alloc, table_name, req, meta, merged);
    }

    fn queryCatalogNative(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        target: catalog_resources.TableTarget,
        req: db_mod.types.SearchRequest,
        consistency: raft_mod.ReadConsistency,
    ) !?query_api.QueryResponse {
        const self: *ProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        const table_name = try nativeCatalogTableNameAlloc(alloc, self.catalog, target);
        defer alloc.free(table_name);
        return try query(ptr, alloc, table_name, req, consistency);
    }

    fn documentAlgebraicAggregate(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        req: document_sql_runtime.AlgebraicAggregateRequest,
        consistency: raft_mod.ReadConsistency,
    ) !?document_sql_runtime.AlgebraicAggregateResponse {
        const self: *ProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        try self.ensureHAReadAllowed(consistency);
        if (self.prepare_for_read) |prep| prep.prepareForRead(table_name, .general);
        const group_ids = try table_catalog.resolveGroupsForSpan(alloc, self.catalog, table_name, "", "");
        defer alloc.free(group_ids);
        if (group_ids.len == 0) return null;
        try tableReadsValidateDocIdentityReadyForMultiGroup(alloc, self.catalog, table_name, group_ids.len);
        return try table_read_document_sql.aggregateProvisionedGroupsAlloc(
            self.replica_root_dir,
            self.catalog,
            self.requester,
            alloc,
            group_ids,
            self.group_visible_root_generation,
            self.backend_runtime,
            table_name,
            req,
            consistency,
        );
    }

    fn documentAlgebraicAggregateCatalog(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        target: catalog_resources.TableTarget,
        req: document_sql_runtime.AlgebraicAggregateRequest,
        consistency: raft_mod.ReadConsistency,
    ) !?document_sql_runtime.AlgebraicAggregateResponse {
        const self: *ProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        const table_name = try nativeCatalogTableNameAlloc(alloc, self.catalog, target);
        defer alloc.free(table_name);
        return try documentAlgebraicAggregate(ptr, alloc, table_name, req, consistency);
    }

    fn documentAlgebraicAggregateGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: document_sql_runtime.AlgebraicAggregateRequest,
        consistency: raft_mod.ReadConsistency,
    ) !?document_sql_runtime.AlgebraicAggregateResponse {
        const self: *ProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        try self.ensureHAReadAllowed(consistency);
        if (self.prepare_for_read) |prep| prep.prepareForRead(table_name, .general);
        return try documentAlgebraicAggregateProvisionedHostedLocal(
            self.replica_root_dir,
            self.catalog,
            self.requester,
            alloc,
            group_id,
            self.visibleRootGeneration(group_id),
            self.backend_runtime,
            table_name,
            req,
            consistency,
        );
    }

    fn preflightQuery(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        req: db_mod.types.SearchRequest,
        consistency: raft_mod.ReadConsistency,
        max_work: u32,
    ) !?db_mod.RuntimePreflightSummary {
        const self: *ProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        try self.ensureHAReadAllowed(consistency);
        if (self.prepare_for_read) |prep| prep.prepareForRead(table_name, readPreparationKindForQuery(req));
        const group_ids = try table_catalog.resolveGroupsForSpan(alloc, self.catalog, table_name, "", "");
        defer alloc.free(group_ids);
        if (group_ids.len == 0) return null;
        try tableReadsValidateDocIdentityReadyForMultiGroup(alloc, self.catalog, table_name, group_ids.len);
        try validateResolvedDocFilterForGroups(alloc, self.catalog, table_name, group_ids, req);
        if (group_ids.len > 1) try distributed_graph.rejectUnstampedResultRefs(req);
        const plan = planFanout(.preflight, self.io_impl, group_ids.len);
        recordFanoutPlan(.preflight, plan);
        if (plan.parallel) {
            return try preflightProvisionedGroupsParallel(self, alloc, self.io_impl.?.io(), plan.width, group_ids, table_name, req, consistency, max_work);
        }
        if (plan.reason == .no_io and group_ids.len > 1) recordParallelFanoutFallback(.preflight);
        return try preflightProvisionedGroups(self, alloc, group_ids, table_name, req, consistency, max_work);
    }

    fn lookupGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        key: []const u8,
        opts: db_mod.types.LookupOptions,
        consistency: raft_mod.ReadConsistency,
    ) !?LookupResponse {
        const self: *ProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        try self.ensureHAReadAllowed(consistency);
        if (self.prepare_for_read) |prep| prep.prepareForRead(table_name, .general);
        return try lookupProvisionedHostedLocal(self.primary_lookup_db, self.cache, self.replica_root_dir, self.catalog, self.requester, alloc, group_id, self.visibleRootGeneration(group_id), self.backend_runtime, table_name, key, opts, consistency);
    }

    fn relationalUniqueOwnerLookup(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        constraint_name: []const u8,
        encoded_value: []const u8,
        consistency: raft_mod.ReadConsistency,
    ) !?[]u8 {
        const self: *ProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        try self.ensureHAReadAllowed(consistency);
        if (self.prepare_for_read) |prep| prep.prepareForRead(table_name, .general);
        const group_id = try resolveSingleUniqueOwnerGroup(alloc, self.catalog, table_name, constraint_name, encoded_value);
        return try lookupRelationalUniqueOwnerProvisionedHostedLocal(
            self.primary_lookup_db,
            self.cache,
            self.replica_root_dir,
            self.catalog,
            self.requester,
            alloc,
            group_id,
            self.visibleRootGeneration(group_id),
            self.backend_runtime,
            table_name,
            constraint_name,
            encoded_value,
            consistency,
        );
    }

    fn relationalTemporalUniqueOwnerLookup(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        constraint_name: []const u8,
        encoded_value: []const u8,
        encoded_point: []const u8,
        consistency: raft_mod.ReadConsistency,
    ) !?[]u8 {
        const self: *ProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        if (self.prepare_for_read) |prep| prep.prepareForRead(table_name, .general);
        const group_id = try resolveSingleUniqueOwnerGroup(alloc, self.catalog, table_name, constraint_name, encoded_value);
        return try lookupRelationalTemporalUniqueOwnerProvisionedHostedLocal(
            self.primary_lookup_db,
            self.cache,
            self.replica_root_dir,
            self.catalog,
            self.requester,
            alloc,
            group_id,
            self.visibleRootGeneration(group_id),
            self.backend_runtime,
            table_name,
            constraint_name,
            encoded_value,
            encoded_point,
            consistency,
        );
    }

    fn relationalTemporalUniqueOverlapOwnerLookup(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        constraint_name: []const u8,
        encoded_value: []const u8,
        encoded_start: []const u8,
        encoded_end: []const u8,
        consistency: raft_mod.ReadConsistency,
    ) !?[]u8 {
        const self: *ProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        if (self.prepare_for_read) |prep| prep.prepareForRead(table_name, .general);
        const group_id = try resolveSingleUniqueOwnerGroup(alloc, self.catalog, table_name, constraint_name, encoded_value);
        return try lookupRelationalTemporalUniqueOverlapOwnerProvisionedHostedLocal(
            self.primary_lookup_db,
            self.cache,
            self.replica_root_dir,
            self.catalog,
            self.requester,
            alloc,
            group_id,
            self.visibleRootGeneration(group_id),
            self.backend_runtime,
            table_name,
            constraint_name,
            encoded_value,
            encoded_start,
            encoded_end,
            consistency,
        );
    }

    fn relationalTemporalUniqueOwnerLookupGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        constraint_name: []const u8,
        encoded_value: []const u8,
        encoded_point: []const u8,
        consistency: raft_mod.ReadConsistency,
    ) !?[]u8 {
        const self: *ProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        try self.ensureHAReadAllowed(consistency);
        if (self.prepare_for_read) |prep| prep.prepareForRead(table_name, .general);
        return try lookupRelationalTemporalUniqueOwnerProvisionedHostedLocal(
            self.primary_lookup_db,
            self.cache,
            self.replica_root_dir,
            self.catalog,
            self.requester,
            alloc,
            group_id,
            self.visibleRootGeneration(group_id),
            self.backend_runtime,
            table_name,
            constraint_name,
            encoded_value,
            encoded_point,
            consistency,
        );
    }

    fn relationalTemporalUniqueOverlapOwnerLookupGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        constraint_name: []const u8,
        encoded_value: []const u8,
        encoded_start: []const u8,
        encoded_end: []const u8,
        consistency: raft_mod.ReadConsistency,
    ) !?[]u8 {
        const self: *ProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        if (self.prepare_for_read) |prep| prep.prepareForRead(table_name, .general);
        return try lookupRelationalTemporalUniqueOverlapOwnerProvisionedHostedLocal(
            self.primary_lookup_db,
            self.cache,
            self.replica_root_dir,
            self.catalog,
            self.requester,
            alloc,
            group_id,
            self.visibleRootGeneration(group_id),
            self.backend_runtime,
            table_name,
            constraint_name,
            encoded_value,
            encoded_start,
            encoded_end,
            consistency,
        );
    }

    fn rowsQueryPlan(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        runtime_schema: storage_schema.TableSchema,
        plan: db_mod.types.RelationalRowsQueryPlan,
        consistency: raft_mod.ReadConsistency,
    ) !?db_mod.types.RelationalRowsQueryResult {
        const self: *ProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        if (self.prepare_for_read) |prep| prep.prepareForRead(table_name, .general);
        const routed_source = self.source();
        return try rowsQueryPlanFromRoutedScansAlloc(alloc, routed_source, table_name, runtime_schema, plan, consistency);
    }

    fn rowsQueryPlanCatalogNative(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        target: catalog_resources.TableTarget,
        runtime_schema: storage_schema.TableSchema,
        plan: db_mod.types.RelationalRowsQueryPlan,
        consistency: raft_mod.ReadConsistency,
    ) !?db_mod.types.RelationalRowsQueryResult {
        const self: *ProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        const table_name = try nativeCatalogTableNameAlloc(alloc, self.catalog, target);
        defer alloc.free(table_name);
        return try rowsQueryPlan(ptr, alloc, table_name, runtime_schema, plan, consistency);
    }

    fn rowsQueryPlanSystemTimeAsOfSequence(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        runtime_schema: storage_schema.TableSchema,
        commit_sequence: u64,
        plan: db_mod.types.RelationalRowsQueryPlan,
        consistency: raft_mod.ReadConsistency,
    ) !?db_mod.types.RelationalRowsQueryResult {
        const self: *ProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        if (self.prepare_for_read) |prep| prep.prepareForRead(table_name, .general);
        if (plan.ctes.len != 0 or plan.ranges.len != 0) return error.UnsupportedRowsQuery;
        const group_ids = try table_catalog.resolveGroupsForSpan(alloc, self.catalog, table_name, "", "");
        defer alloc.free(group_ids);
        if (group_ids.len == 0) return null;
        if (group_ids.len != 1) return error.UnsupportedRowsQuery;
        try tableReadsValidateDocIdentityReadyForMultiGroup(alloc, self.catalog, table_name, group_ids.len);
        var reads = raft_mod.FeatureDBReads.init(group_ids[0], self.requester);
        try reads.reads.prepareScanWithConsistency(group_ids[0], "", "", .{}, consistency);
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_ids[0]);
        defer alloc.free(path);
        var db = try openProvisionedQueryDbForTableWithRuntime(alloc, path, self.catalog, table_name, group_ids[0], self.visibleRootGeneration(group_ids[0]), self.backend_runtime);
        defer db.close();
        return try db.querySystemVersionedRelationalRowsAsOfSequence(alloc, runtime_schema, commit_sequence, plan.query);
    }

    fn rowsQueryPlanCatalogSystemTimeAsOfSequenceNative(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        target: catalog_resources.TableTarget,
        runtime_schema: storage_schema.TableSchema,
        commit_sequence: u64,
        plan: db_mod.types.RelationalRowsQueryPlan,
        consistency: raft_mod.ReadConsistency,
    ) !?db_mod.types.RelationalRowsQueryResult {
        const self: *ProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        const table_name = try nativeCatalogTableNameAlloc(alloc, self.catalog, target);
        defer alloc.free(table_name);
        return try rowsQueryPlanSystemTimeAsOfSequence(ptr, alloc, table_name, runtime_schema, commit_sequence, plan, consistency);
    }

    fn rowsSetOperationPlan(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        runtime_schema: storage_schema.TableSchema,
        plan: db_mod.types.RelationalRowsSetOperationPlan,
        consistency: raft_mod.ReadConsistency,
    ) !?db_mod.types.RelationalRowsQueryResult {
        const self: *ProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        if (self.prepare_for_read) |prep| prep.prepareForRead(table_name, .general);
        const routed_source = self.source();
        return try rowsSetOperationPlanFromRoutedScansAlloc(alloc, routed_source, table_name, runtime_schema, plan, consistency);
    }

    fn rowsSetOperationPlanCatalogNative(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        target: catalog_resources.TableTarget,
        runtime_schema: storage_schema.TableSchema,
        plan: db_mod.types.RelationalRowsSetOperationPlan,
        consistency: raft_mod.ReadConsistency,
    ) !?db_mod.types.RelationalRowsQueryResult {
        const self: *ProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        const table_name = try nativeCatalogTableNameAlloc(alloc, self.catalog, target);
        defer alloc.free(table_name);
        return try rowsSetOperationPlan(ptr, alloc, table_name, runtime_schema, plan, consistency);
    }

    fn rowsAggregatePlan(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        runtime_schema: storage_schema.TableSchema,
        plan: db_mod.types.RelationalRowsAggregatePlan,
        consistency: raft_mod.ReadConsistency,
    ) !?db_mod.types.RelationalRowsAggregateResult {
        const self: *ProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        if (self.prepare_for_read) |prep| prep.prepareForRead(table_name, .general);
        const routed_source = self.source();
        return try rowsAggregatePlanFromRoutedScansAlloc(alloc, routed_source, table_name, runtime_schema, plan, consistency);
    }

    fn rowsWindowPlan(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        runtime_schema: storage_schema.TableSchema,
        plan: db_mod.types.RelationalRowsWindowPlan,
        consistency: raft_mod.ReadConsistency,
    ) !?db_mod.types.RelationalRowsWindowResult {
        const self: *ProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        if (self.prepare_for_read) |prep| prep.prepareForRead(table_name, .general);
        const routed_source = self.source();
        return try rowsWindowPlanFromRoutedScansAlloc(alloc, routed_source, table_name, runtime_schema, plan, consistency);
    }

    fn rowsJoinPlan(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        runtime_schema: storage_schema.TableSchema,
        plan: db_mod.types.RelationalRowsJoinPlan,
        consistency: raft_mod.ReadConsistency,
    ) !?db_mod.types.RelationalRowsJoinResult {
        const self: *ProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        if (self.prepare_for_read) |prep| prep.prepareForRead(table_name, .general);
        const routed_source = self.source();
        return try rowsJoinPlanFromRoutedScansAlloc(alloc, routed_source, table_name, runtime_schema, plan, consistency);
    }

    fn rowsLateralPlan(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        runtime_schema: storage_schema.TableSchema,
        plan: db_mod.types.RelationalRowsLateralPlan,
        consistency: raft_mod.ReadConsistency,
    ) !?db_mod.types.RelationalRowsJoinResult {
        const self: *ProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        if (self.prepare_for_read) |prep| prep.prepareForRead(table_name, .general);
        const routed_source = self.source();
        return try rowsLateralPlanFromRoutedScansAlloc(alloc, routed_source, table_name, runtime_schema, plan, consistency);
    }

    fn preflightQueryGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: db_mod.types.SearchRequest,
        consistency: raft_mod.ReadConsistency,
        max_work: u32,
    ) !?db_mod.RuntimePreflightSummary {
        const self: *ProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        try self.ensureHAReadAllowed(consistency);
        return try preflightHostedLocal(
            self.cache,
            self.replica_root_dir,
            self.catalog,
            self.requester,
            alloc,
            group_id,
            self.visibleRootGeneration(group_id),
            self.backend_runtime,
            table_name,
            req,
            consistency,
            max_work,
        );
    }

    fn scanGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        from_key: []const u8,
        to_key: []const u8,
        opts: db_mod.types.ScanOptions,
        consistency: raft_mod.ReadConsistency,
    ) !?ScanResponse {
        const self: *ProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        try self.ensureHAReadAllowed(consistency);
        if (self.prepare_for_read) |prep| prep.prepareForRead(table_name, .general);
        return try scanProvisionedHostedLocal(self.cache, self.replica_root_dir, self.catalog, self.requester, alloc, group_id, self.visibleRootGeneration(group_id), self.backend_runtime, table_name, from_key, to_key, opts, consistency);
    }

    fn queryGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: db_mod.types.SearchRequest,
        consistency: raft_mod.ReadConsistency,
    ) !?query_api.QueryResponse {
        const self: *ProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        try self.ensureHAReadAllowed(consistency);
        if (self.prepare_for_read) |prep| prep.prepareForRead(table_name, readPreparationKindForQuery(req));
        const start_ns = platform_time.monotonicNs();
        const execution = try queryHostedLocalDetailed(self.cache, self.replica_root_dir, self.catalog, self.requester, alloc, group_id, self.visibleRootGeneration(group_id), self.backend_runtime, self.antfly_provider, self.secret_store, self.remote_content, table_name, req, consistency);
        var result = execution.result;
        defer result.deinit();
        const response_req = execution.request;
        var meta: query_api.QueryResponseMeta = .{
            .took_ms = @intCast(@divTrunc(platform_time.monotonicNs() - start_ns, std.time.ns_per_ms)),
            .shard_count = 1,
            .dense_search = execution.dense_profile,
        };
        defer meta.deinit(alloc);
        try applyProvisionedQueryAggregations(self, alloc, &.{group_id}, table_name, response_req, &result, &meta, consistency);
        try applyQueryPostProcessing(alloc, response_req, &result, &meta, self.antfly_provider, self.secret_store);
        return try query_api.encodeQueryResponses(alloc, table_name, response_req, meta, result);
    }

    fn searchResultGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: db_mod.types.SearchRequest,
        consistency: raft_mod.ReadConsistency,
    ) !?db_mod.types.SearchResult {
        const self: *ProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        try self.ensureHAReadAllowed(consistency);
        if (self.prepare_for_read) |prep| prep.prepareForRead(table_name, readPreparationKindForQuery(req));
        return try queryHostedLocal(self.cache, self.replica_root_dir, self.catalog, self.requester, alloc, group_id, self.visibleRootGeneration(group_id), self.backend_runtime, self.antfly_provider, self.secret_store, self.remote_content, table_name, req, consistency);
    }

    fn textStatsGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !?query_api.QueryResponse {
        const self: *ProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        return try collectProvisionedHostedLocalTextStats(self.cache, self.replica_root_dir, self.catalog, alloc, group_id, self.visibleRootGeneration(group_id), self.backend_runtime, table_name, body);
    }

    fn algebraicPartialsGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !?query_api.QueryResponse {
        const self: *ProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        return try collectProvisionedHostedLocalAlgebraicPartials(self.cache, self.replica_root_dir, self.catalog, alloc, group_id, self.visibleRootGeneration(group_id), self.backend_runtime, table_name, body);
    }

    fn graphExpandGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: distributed_graph.GraphExpandRequest,
        consistency: raft_mod.ReadConsistency,
    ) !?distributed_graph.GraphExpandResponse {
        const self: *ProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        try self.ensureHAReadAllowed(consistency);
        const Context = struct {
            source: *ProvisionedTableReadSource,
            group_id: u64,
            table_name: []const u8,
            consistency: raft_mod.ReadConsistency,

            fn search(ctx: @This(), allocator: std.mem.Allocator, search_req: db_mod.types.SearchRequest) anyerror!db_mod.types.SearchResult {
                return try queryHostedLocal(
                    ctx.source.cache,
                    ctx.source.replica_root_dir,
                    ctx.source.catalog,
                    ctx.source.requester,
                    allocator,
                    ctx.group_id,
                    ctx.source.visibleRootGeneration(ctx.group_id),
                    ctx.source.backend_runtime,
                    ctx.source.antfly_provider,
                    ctx.source.secret_store,
                    ctx.source.remote_content,
                    ctx.table_name,
                    search_req,
                    ctx.consistency,
                );
            }
        };
        return try graphExpandWithSearch(Context, alloc, req, .{
            .source = self,
            .group_id = group_id,
            .table_name = table_name,
            .consistency = consistency,
        }, Context.search);
    }

    fn graphHydrateGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: distributed_graph.GraphHydrateRequest,
        consistency: raft_mod.ReadConsistency,
    ) !?distributed_graph.GraphHydrateResponse {
        const self: *ProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        try self.ensureHAReadAllowed(consistency);
        try table_catalog.validateTopologyEpoch(alloc, self.catalog, table_name, req.topology_epoch);
        return try executeProvisionedGraphHydrate(ptr, alloc, group_id, table_name, req, consistency);
    }

    fn graphEdgesGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: distributed_graph.GraphEdgesRequest,
        consistency: raft_mod.ReadConsistency,
    ) !?distributed_graph.GraphEdgesResponse {
        const self: *ProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        try self.ensureHAReadAllowed(consistency);
        return try graphGetEdgesLocal(alloc, self.replica_root_dir, self.catalog, self.requester, group_id, self.visibleRootGeneration(group_id), self.backend_runtime, table_name, req, consistency);
    }

    fn localRuntimeStatuses(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
    ) !?runtime_status.LocalTableRuntimeStatuses {
        const self: *ProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        if (self.runtime_status_cache) |snapshot_cache| {
            return try snapshot_cache.snapshot(alloc, table_name);
        }
        return null;
    }

    fn localRuntimeStatusesCatalogNative(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        target: catalog_resources.TableTarget,
    ) !?runtime_status.LocalTableRuntimeStatuses {
        const self: *ProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        const table_name = try nativeCatalogTableNameAlloc(alloc, self.catalog, target);
        defer alloc.free(table_name);
        return try localRuntimeStatuses(ptr, alloc, table_name);
    }
};

pub const HostedProvisionedTableReadSource = struct {
    replica_root_dir: []const u8,
    catalog: table_catalog.CatalogSource,
    requester: raft_mod.ReadableLeaseRequester,
    router: table_router.HostedGroupRouter,
    executor: http_common.RequestExecutor,
    io_impl: ?*std.Io.Threaded = null,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime = null,
    group_visible_root_generation: ?GroupVisibleRootGenerationSource = null,

    pub fn init(
        replica_root_dir: []const u8,
        catalog: table_catalog.CatalogSource,
        requester: raft_mod.ReadableLeaseRequester,
        router: table_router.HostedGroupRouter,
        executor: http_common.RequestExecutor,
    ) HostedProvisionedTableReadSource {
        return .{
            .replica_root_dir = replica_root_dir,
            .catalog = catalog,
            .requester = requester,
            .router = router,
            .executor = executor,
        };
    }

    pub fn withIo(self: *HostedProvisionedTableReadSource, io_impl: *std.Io.Threaded) *HostedProvisionedTableReadSource {
        self.io_impl = io_impl;
        return self;
    }

    pub fn withBackendRuntime(self: *HostedProvisionedTableReadSource, backend_runtime: *db_mod.background_runtime.BackendRuntime) *HostedProvisionedTableReadSource {
        self.backend_runtime = backend_runtime;
        return self;
    }

    pub fn withGroupVisibleRootGeneration(self: *HostedProvisionedTableReadSource, generation_source: ?GroupVisibleRootGenerationSource) *HostedProvisionedTableReadSource {
        self.group_visible_root_generation = generation_source;
        return self;
    }

    fn visibleRootGeneration(self: *const HostedProvisionedTableReadSource, group_id: u64) u64 {
        return if (self.group_visible_root_generation) |generation_source| generation_source.visibleRootGenerationForGroup(group_id) else backend_current_root_generation;
    }

    pub fn source(self: *HostedProvisionedTableReadSource) TableReadSource {
        return .{
            .ptr = self,
            .vtable = &.{
                .lookup = lookup,
                .lookup_catalog = lookupCatalogNative,
                .scan = scan,
                .scan_catalog = scanCatalogNative,
                .query = query,
                .query_catalog = queryCatalogNative,
                .document_algebraic_aggregate = documentAlgebraicAggregate,
                .document_algebraic_aggregate_catalog = documentAlgebraicAggregateCatalog,
                .document_algebraic_aggregate_group_local = documentAlgebraicAggregateGroupLocal,
                .preflight_query = preflightQuery,
                .preflight_query_group_local = preflightQueryGroupLocal,
                .lookup_group_local = lookupGroupLocal,
                .relational_unique_owner_lookup = relationalUniqueOwnerLookup,
                .relational_temporal_unique_owner_lookup = HostedProvisionedTableReadSource.relationalTemporalUniqueOwnerLookup,
                .relational_temporal_unique_overlap_owner_lookup = HostedProvisionedTableReadSource.relationalTemporalUniqueOverlapOwnerLookup,
                .relational_temporal_unique_owner_lookup_group_local = HostedProvisionedTableReadSource.relationalTemporalUniqueOwnerLookupGroupLocal,
                .relational_temporal_unique_overlap_owner_lookup_group_local = HostedProvisionedTableReadSource.relationalTemporalUniqueOverlapOwnerLookupGroupLocal,
                .rows_query_plan = rowsQueryPlan,
                .rows_query_plan_catalog = HostedProvisionedTableReadSource.rowsQueryPlanCatalogNative,
                .rows_query_plan_system_time_as_of_sequence = HostedProvisionedTableReadSource.rowsQueryPlanSystemTimeAsOfSequence,
                .rows_query_plan_catalog_system_time_as_of_sequence = HostedProvisionedTableReadSource.rowsQueryPlanCatalogSystemTimeAsOfSequenceNative,
                .rows_set_operation_plan = rowsSetOperationPlan,
                .rows_set_operation_plan_catalog = HostedProvisionedTableReadSource.rowsSetOperationPlanCatalogNative,
                .rows_aggregate_plan = rowsAggregatePlan,
                .rows_window_plan = rowsWindowPlan,
                .rows_join_plan = rowsJoinPlan,
                .rows_lateral_plan = rowsLateralPlan,
                .scan_group_local = scanGroupLocal,
                .query_group_local = queryGroupLocal,
                .search_result_group_local = searchResultGroupLocal,
                .text_stats_group_local = textStatsGroupLocal,
                .algebraic_partials_group_local = algebraicPartialsGroupLocal,
                .join_partition_group_local = joinPartitionGroupLocal,
                .join_rows_group_local = joinRowsGroupLocal,
                .join_unmatched_group_local = joinUnmatchedGroupLocal,
                .join_finalize_group_local = joinFinalizeGroupLocal,
                .join_job_state_group_local = joinJobStateGroupLocal,
                .graph_expand_group_local = graphExpandGroupLocal,
                .graph_hydrate_group_local = graphHydrateGroupLocal,
                .graph_edges_group_local = graphEdgesGroupLocal,
                .local_runtime_statuses = localRuntimeStatuses,
                .local_runtime_statuses_catalog = HostedProvisionedTableReadSource.localRuntimeStatusesCatalogNative,
            },
        };
    }

    fn lookup(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        key: []const u8,
        opts: db_mod.types.LookupOptions,
        consistency: raft_mod.ReadConsistency,
    ) !?LookupResponse {
        const self: *HostedProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        const group_id = (try table_catalog.resolveGroupForKey(alloc, self.catalog, table_name, key)) orelse {
            return null;
        };
        var route = (try table_router.resolveGroupRoute(alloc, self.catalog, self.router, group_id, routePolicyForConsistency(consistency))) orelse {
            return null;
        };
        defer route.deinit(alloc);

        if (try lookupViaRoute(self, alloc, route, group_id, table_name, key, opts, consistency)) |result| return result;
        return try lookupAcrossActivePlacements(self, alloc, group_id, table_name, key, opts, consistency, route);
    }

    fn lookupCatalogNative(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        target: catalog_resources.TableTarget,
        key: []const u8,
        opts: db_mod.types.LookupOptions,
        consistency: raft_mod.ReadConsistency,
    ) !?LookupResponse {
        const self: *HostedProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        const table_name = try nativeCatalogTableNameAlloc(alloc, self.catalog, target);
        defer alloc.free(table_name);
        return try lookup(ptr, alloc, table_name, key, opts, consistency);
    }

    fn queryCatalogNative(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        target: catalog_resources.TableTarget,
        req: db_mod.types.SearchRequest,
        consistency: raft_mod.ReadConsistency,
    ) !?query_api.QueryResponse {
        const self: *HostedProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        const table_name = try nativeCatalogTableNameAlloc(alloc, self.catalog, target);
        defer alloc.free(table_name);
        return try query(ptr, alloc, table_name, req, consistency);
    }

    fn scanCatalogNative(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        target: catalog_resources.TableTarget,
        from_key: []const u8,
        to_key: []const u8,
        opts: db_mod.types.ScanOptions,
        consistency: raft_mod.ReadConsistency,
    ) !?ScanResponse {
        const self: *HostedProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        const table_name = try nativeCatalogTableNameAlloc(alloc, self.catalog, target);
        defer alloc.free(table_name);
        return try scan(ptr, alloc, table_name, from_key, to_key, opts, consistency);
    }

    fn documentAlgebraicAggregateCatalog(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        target: catalog_resources.TableTarget,
        req: document_sql_runtime.AlgebraicAggregateRequest,
        consistency: raft_mod.ReadConsistency,
    ) !?document_sql_runtime.AlgebraicAggregateResponse {
        const self: *HostedProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        const table_name = try nativeCatalogTableNameAlloc(alloc, self.catalog, target);
        defer alloc.free(table_name);
        return try documentAlgebraicAggregate(ptr, alloc, table_name, req, consistency);
    }

    fn documentAlgebraicAggregate(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        req: document_sql_runtime.AlgebraicAggregateRequest,
        consistency: raft_mod.ReadConsistency,
    ) !?document_sql_runtime.AlgebraicAggregateResponse {
        const self: *HostedProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        const group_ids = try table_catalog.resolveGroupsForSpan(alloc, self.catalog, table_name, "", "");
        defer alloc.free(group_ids);
        if (group_ids.len == 0) return null;
        try tableReadsValidateDocIdentityReadyForMultiGroup(alloc, self.catalog, table_name, group_ids.len);
        var shard_responses = std.ArrayListUnmanaged(document_sql_runtime.AlgebraicAggregateResponse).empty;
        defer {
            for (shard_responses.items) |*response| response.deinit(alloc);
            shard_responses.deinit(alloc);
        }

        var local_req = req;
        if (req.group_by != null) local_req.limit = null;

        for (group_ids) |group_id| {
            var route = (try table_router.resolveGroupRoute(alloc, self.catalog, self.router, group_id, routePolicyForConsistency(consistency))) orelse return error.TopologyChanged;
            defer route.deinit(alloc);
            var response = switch (route) {
                .local => try documentAlgebraicAggregateProvisionedHostedLocal(
                    self.replica_root_dir,
                    self.catalog,
                    self.requester,
                    alloc,
                    group_id,
                    self.visibleRootGeneration(group_id),
                    self.backend_runtime,
                    table_name,
                    local_req,
                    consistency,
                ) orelse return error.DocumentSqlIndexUnavailable,
                .remote => |remote| try documentAlgebraicAggregateRemote(
                    self.executor,
                    alloc,
                    remote.base_uri,
                    group_id,
                    table_name,
                    local_req,
                ),
            };
            errdefer response.deinit(alloc);
            try shard_responses.append(alloc, response);
        }
        if (shard_responses.items.len == 0) return null;
        return try documentAlgebraicAggregateMergeResponsesAlloc(alloc, req, shard_responses.items);
    }

    fn lookupViaRoute(
        self: *HostedProvisionedTableReadSource,
        alloc: std.mem.Allocator,
        route: table_router.GroupRoute,
        group_id: u64,
        table_name: []const u8,
        key: []const u8,
        opts: db_mod.types.LookupOptions,
        consistency: raft_mod.ReadConsistency,
    ) !?LookupResponse {
        return switch (route) {
            .local => try lookupProvisionedHostedLocal(null, null, self.replica_root_dir, self.catalog, self.requester, alloc, group_id, self.visibleRootGeneration(group_id), self.backend_runtime, table_name, key, opts, consistency),
            .remote => |remote| lookupRemote(self.executor, alloc, remote.base_uri, group_id, table_name, key, opts) catch |err| switch (err) {
                error.UnexpectedHttpStatus => null,
                else => err,
            },
        };
    }

    fn lookupAcrossActivePlacements(
        self: *HostedProvisionedTableReadSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        key: []const u8,
        opts: db_mod.types.LookupOptions,
        consistency: raft_mod.ReadConsistency,
        initial_route: table_router.GroupRoute,
    ) !?LookupResponse {
        var snapshot = try self.catalog.adminSnapshot();
        defer self.catalog.freeAdminSnapshot(&snapshot);
        const placements = try metadata_admin.listGroupPlacement(alloc, &snapshot, group_id);
        defer metadata_admin.freePlacementRefs(alloc, placements);

        const local_node_id = self.router.localNodeId();
        const tried_local = initial_route == .local;
        const tried_remote_node_id = switch (initial_route) {
            .remote => |remote| remote.node_id,
            else => 0,
        };

        for (placements) |intent| {
            const node_id = intent.record.local_node_id;
            if (node_id == local_node_id) {
                if (tried_local or self.router.localStatus(group_id) != .active) continue;
                if (try lookupProvisionedHostedLocal(null, null, self.replica_root_dir, self.catalog, self.requester, alloc, group_id, self.visibleRootGeneration(group_id), self.backend_runtime, table_name, key, opts, consistency)) |result| return result;
                continue;
            }
            if (node_id == tried_remote_node_id) continue;
            if (self.router.nodeStatus(node_id, group_id)) |status| {
                if (status != .active) continue;
            }
            const base_uri = (try self.router.nodeBaseUriForGroup(alloc, group_id, node_id)) orelse continue;
            defer alloc.free(base_uri);
            if (lookupRemote(self.executor, alloc, base_uri, group_id, table_name, key, opts)) |result| {
                return result;
            } else |err| switch (err) {
                error.UnexpectedHttpStatus => continue,
                else => return err,
            }
        }
        return null;
    }

    fn relationalUniqueOwnerLookup(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        constraint_name: []const u8,
        encoded_value: []const u8,
        consistency: raft_mod.ReadConsistency,
    ) !?[]u8 {
        const self: *HostedProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        const group_id = try resolveSingleUniqueOwnerGroup(alloc, self.catalog, table_name, constraint_name, encoded_value);
        var route = (try table_router.resolveGroupRoute(alloc, self.catalog, self.router, group_id, routePolicyForConsistency(consistency))) orelse {
            return error.UniqueOwnerTopologyUnavailable;
        };
        defer route.deinit(alloc);
        return switch (route) {
            .local => try lookupRelationalUniqueOwnerProvisionedHostedLocal(
                null,
                null,
                self.replica_root_dir,
                self.catalog,
                self.requester,
                alloc,
                group_id,
                self.visibleRootGeneration(group_id),
                self.backend_runtime,
                table_name,
                constraint_name,
                encoded_value,
                consistency,
            ),
            .remote => |remote| lookupRelationalUniqueOwnerRemote(self.executor, alloc, remote.base_uri, group_id, table_name, constraint_name, encoded_value) catch |err| switch (err) {
                error.UnexpectedHttpStatus => null,
                else => err,
            },
        };
    }

    fn relationalTemporalUniqueOwnerLookup(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        constraint_name: []const u8,
        encoded_value: []const u8,
        encoded_point: []const u8,
        consistency: raft_mod.ReadConsistency,
    ) !?[]u8 {
        const self: *HostedProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        const group_id = try resolveSingleUniqueOwnerGroup(alloc, self.catalog, table_name, constraint_name, encoded_value);
        var route = (try table_router.resolveGroupRoute(alloc, self.catalog, self.router, group_id, routePolicyForConsistency(consistency))) orelse {
            return error.UniqueOwnerTopologyUnavailable;
        };
        defer route.deinit(alloc);
        return switch (route) {
            .local => try lookupRelationalTemporalUniqueOwnerProvisionedHostedLocal(
                null,
                null,
                self.replica_root_dir,
                self.catalog,
                self.requester,
                alloc,
                group_id,
                self.visibleRootGeneration(group_id),
                self.backend_runtime,
                table_name,
                constraint_name,
                encoded_value,
                encoded_point,
                consistency,
            ),
            .remote => |remote| lookupRelationalTemporalUniqueOwnerRemote(self.executor, alloc, remote.base_uri, group_id, table_name, constraint_name, encoded_value, encoded_point) catch |err| switch (err) {
                error.UnexpectedHttpStatus => null,
                else => err,
            },
        };
    }

    fn relationalTemporalUniqueOverlapOwnerLookup(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        constraint_name: []const u8,
        encoded_value: []const u8,
        encoded_start: []const u8,
        encoded_end: []const u8,
        consistency: raft_mod.ReadConsistency,
    ) !?[]u8 {
        const self: *HostedProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        const group_id = try resolveSingleUniqueOwnerGroup(alloc, self.catalog, table_name, constraint_name, encoded_value);
        var route = (try table_router.resolveGroupRoute(alloc, self.catalog, self.router, group_id, routePolicyForConsistency(consistency))) orelse {
            return error.UniqueOwnerTopologyUnavailable;
        };
        defer route.deinit(alloc);

        return switch (route) {
            .local => try lookupRelationalTemporalUniqueOverlapOwnerProvisionedHostedLocal(
                null,
                null,
                self.replica_root_dir,
                self.catalog,
                self.requester,
                alloc,
                group_id,
                self.visibleRootGeneration(group_id),
                self.backend_runtime,
                table_name,
                constraint_name,
                encoded_value,
                encoded_start,
                encoded_end,
                consistency,
            ),
            .remote => |remote| lookupRelationalTemporalUniqueOverlapOwnerRemote(self.executor, alloc, remote.base_uri, group_id, table_name, constraint_name, encoded_value, encoded_start, encoded_end) catch |err| switch (err) {
                error.UnexpectedHttpStatus => null,
                else => err,
            },
        };
    }

    fn relationalTemporalUniqueOwnerLookupGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        constraint_name: []const u8,
        encoded_value: []const u8,
        encoded_point: []const u8,
        consistency: raft_mod.ReadConsistency,
    ) !?[]u8 {
        const self: *HostedProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        return try lookupRelationalTemporalUniqueOwnerProvisionedHostedLocal(
            null,
            null,
            self.replica_root_dir,
            self.catalog,
            self.requester,
            alloc,
            group_id,
            self.visibleRootGeneration(group_id),
            self.backend_runtime,
            table_name,
            constraint_name,
            encoded_value,
            encoded_point,
            consistency,
        );
    }

    fn relationalTemporalUniqueOverlapOwnerLookupGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        constraint_name: []const u8,
        encoded_value: []const u8,
        encoded_start: []const u8,
        encoded_end: []const u8,
        consistency: raft_mod.ReadConsistency,
    ) !?[]u8 {
        const self: *HostedProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        return try lookupRelationalTemporalUniqueOverlapOwnerProvisionedHostedLocal(
            null,
            null,
            self.replica_root_dir,
            self.catalog,
            self.requester,
            alloc,
            group_id,
            self.visibleRootGeneration(group_id),
            self.backend_runtime,
            table_name,
            constraint_name,
            encoded_value,
            encoded_start,
            encoded_end,
            consistency,
        );
    }

    fn rowsQueryPlan(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        runtime_schema: storage_schema.TableSchema,
        plan: db_mod.types.RelationalRowsQueryPlan,
        consistency: raft_mod.ReadConsistency,
    ) !?db_mod.types.RelationalRowsQueryResult {
        const self: *HostedProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        const routed_source = self.source();
        return try rowsQueryPlanFromRoutedScansAlloc(alloc, routed_source, table_name, runtime_schema, plan, consistency);
    }

    fn rowsQueryPlanCatalogNative(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        target: catalog_resources.TableTarget,
        runtime_schema: storage_schema.TableSchema,
        plan: db_mod.types.RelationalRowsQueryPlan,
        consistency: raft_mod.ReadConsistency,
    ) !?db_mod.types.RelationalRowsQueryResult {
        const self: *HostedProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        const table_name = try nativeCatalogTableNameAlloc(alloc, self.catalog, target);
        defer alloc.free(table_name);
        return try rowsQueryPlan(ptr, alloc, table_name, runtime_schema, plan, consistency);
    }

    fn rowsQueryPlanSystemTimeAsOfSequence(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        runtime_schema: storage_schema.TableSchema,
        commit_sequence: u64,
        plan: db_mod.types.RelationalRowsQueryPlan,
        consistency: raft_mod.ReadConsistency,
    ) !?db_mod.types.RelationalRowsQueryResult {
        const self: *HostedProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        if (plan.ctes.len != 0 or plan.ranges.len != 0) return error.UnsupportedRowsQuery;
        const group_ids = try table_catalog.resolveGroupsForSpan(alloc, self.catalog, table_name, "", "");
        defer alloc.free(group_ids);
        if (group_ids.len == 0) return null;
        if (group_ids.len != 1) return error.UnsupportedRowsQuery;
        try tableReadsValidateDocIdentityReadyForMultiGroup(alloc, self.catalog, table_name, group_ids.len);
        var route = (try table_router.resolveGroupRoute(alloc, self.catalog, self.router, group_ids[0], routePolicyForConsistency(consistency))) orelse return null;
        defer route.deinit(alloc);
        switch (route) {
            .local => {},
            .remote => return error.UnsupportedOperation,
        }
        var reads = raft_mod.FeatureDBReads.init(group_ids[0], self.requester);
        try reads.reads.prepareScanWithConsistency(group_ids[0], "", "", .{}, consistency);
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_ids[0]);
        defer alloc.free(path);
        var db = try openProvisionedQueryDbForTableWithRuntime(alloc, path, self.catalog, table_name, group_ids[0], self.visibleRootGeneration(group_ids[0]), self.backend_runtime);
        defer db.close();
        return try db.querySystemVersionedRelationalRowsAsOfSequence(alloc, runtime_schema, commit_sequence, plan.query);
    }

    fn rowsQueryPlanCatalogSystemTimeAsOfSequenceNative(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        target: catalog_resources.TableTarget,
        runtime_schema: storage_schema.TableSchema,
        commit_sequence: u64,
        plan: db_mod.types.RelationalRowsQueryPlan,
        consistency: raft_mod.ReadConsistency,
    ) !?db_mod.types.RelationalRowsQueryResult {
        const self: *HostedProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        const table_name = try nativeCatalogTableNameAlloc(alloc, self.catalog, target);
        defer alloc.free(table_name);
        return try rowsQueryPlanSystemTimeAsOfSequence(ptr, alloc, table_name, runtime_schema, commit_sequence, plan, consistency);
    }

    fn rowsSetOperationPlan(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        runtime_schema: storage_schema.TableSchema,
        plan: db_mod.types.RelationalRowsSetOperationPlan,
        consistency: raft_mod.ReadConsistency,
    ) !?db_mod.types.RelationalRowsQueryResult {
        const self: *HostedProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        const routed_source = self.source();
        return try rowsSetOperationPlanFromRoutedScansAlloc(alloc, routed_source, table_name, runtime_schema, plan, consistency);
    }

    fn rowsSetOperationPlanCatalogNative(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        target: catalog_resources.TableTarget,
        runtime_schema: storage_schema.TableSchema,
        plan: db_mod.types.RelationalRowsSetOperationPlan,
        consistency: raft_mod.ReadConsistency,
    ) !?db_mod.types.RelationalRowsQueryResult {
        const self: *HostedProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        const table_name = try nativeCatalogTableNameAlloc(alloc, self.catalog, target);
        defer alloc.free(table_name);
        return try rowsSetOperationPlan(ptr, alloc, table_name, runtime_schema, plan, consistency);
    }

    fn rowsAggregatePlan(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        runtime_schema: storage_schema.TableSchema,
        plan: db_mod.types.RelationalRowsAggregatePlan,
        consistency: raft_mod.ReadConsistency,
    ) !?db_mod.types.RelationalRowsAggregateResult {
        const self: *HostedProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        const routed_source = self.source();
        return try rowsAggregatePlanFromRoutedScansAlloc(alloc, routed_source, table_name, runtime_schema, plan, consistency);
    }

    fn rowsWindowPlan(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        runtime_schema: storage_schema.TableSchema,
        plan: db_mod.types.RelationalRowsWindowPlan,
        consistency: raft_mod.ReadConsistency,
    ) !?db_mod.types.RelationalRowsWindowResult {
        const self: *HostedProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        const routed_source = self.source();
        return try rowsWindowPlanFromRoutedScansAlloc(alloc, routed_source, table_name, runtime_schema, plan, consistency);
    }

    fn rowsJoinPlan(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        runtime_schema: storage_schema.TableSchema,
        plan: db_mod.types.RelationalRowsJoinPlan,
        consistency: raft_mod.ReadConsistency,
    ) !?db_mod.types.RelationalRowsJoinResult {
        const self: *HostedProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        const routed_source = self.source();
        return try rowsJoinPlanFromRoutedScansAlloc(alloc, routed_source, table_name, runtime_schema, plan, consistency);
    }

    fn rowsLateralPlan(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        runtime_schema: storage_schema.TableSchema,
        plan: db_mod.types.RelationalRowsLateralPlan,
        consistency: raft_mod.ReadConsistency,
    ) !?db_mod.types.RelationalRowsJoinResult {
        const self: *HostedProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        const routed_source = self.source();
        return try rowsLateralPlanFromRoutedScansAlloc(alloc, routed_source, table_name, runtime_schema, plan, consistency);
    }

    fn scan(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        from_key: []const u8,
        to_key: []const u8,
        opts: db_mod.types.ScanOptions,
        consistency: raft_mod.ReadConsistency,
    ) !?ScanResponse {
        const self: *HostedProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        const group_ids = try table_catalog.resolveGroupsForSpan(alloc, self.catalog, table_name, from_key, to_key);
        defer alloc.free(group_ids);
        if (group_ids.len == 0) return null;
        try tableReadsValidateDocIdentityReadyForMultiGroup(alloc, self.catalog, table_name, group_ids.len);

        var out = std.ArrayListUnmanaged(u8).empty;
        defer out.deinit(alloc);
        var emitted: u32 = 0;

        for (group_ids) |group_id| {
            var group_opts = opts;
            if (opts.limit > 0) {
                if (emitted >= opts.limit) break;
                group_opts.limit = opts.limit - emitted;
            }

            var route = (try table_router.resolveGroupRoute(alloc, self.catalog, self.router, group_id, routePolicyForConsistency(consistency))) orelse return null;
            defer route.deinit(alloc);

            var result = switch (route) {
                .local => try scanProvisionedHostedLocal(null, self.replica_root_dir, self.catalog, self.requester, alloc, group_id, self.visibleRootGeneration(group_id), self.backend_runtime, table_name, from_key, to_key, group_opts, consistency),
                .remote => |remote| try scanRemote(self.executor, alloc, remote.base_uri, group_id, table_name, from_key, to_key, group_opts),
            } orelse return null;
            defer result.deinit(alloc);

            try out.appendSlice(alloc, result.ndjson);
            emitted += @intCast(std.mem.count(u8, result.ndjson, "\n"));
        }
        return .{ .ndjson = try out.toOwnedSlice(alloc) };
    }

    fn query(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        req: db_mod.types.SearchRequest,
        consistency: raft_mod.ReadConsistency,
    ) !?query_api.QueryResponse {
        const self: *HostedProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        const group_ids = try table_catalog.resolveGroupsForSpan(alloc, self.catalog, table_name, "", "");
        defer alloc.free(group_ids);
        if (group_ids.len == 0) return null;
        try tableReadsValidateDocIdentityReadyForMultiGroup(alloc, self.catalog, table_name, group_ids.len);
        if (group_ids.len > 1) try distributed_graph.rejectUnstampedResultRefs(req);
        const start_ns = platform_time.monotonicNs();
        if (group_ids.len == 1 and !distributed_graph.supportsCrossRange(req)) {
            var route = (try table_router.resolveGroupRoute(alloc, self.catalog, self.router, group_ids[0], routePolicyForConsistency(consistency))) orelse return null;
            defer route.deinit(alloc);

            if (route == .local) {
                const execution = try queryHostedLocalDetailed(null, self.replica_root_dir, self.catalog, self.requester, alloc, group_ids[0], self.visibleRootGeneration(group_ids[0]), self.backend_runtime, null, null, null, table_name, req, consistency);
                var result = execution.result;
                defer result.deinit();
                const response_req = execution.request;
                var meta: query_api.QueryResponseMeta = .{
                    .took_ms = @intCast(@divTrunc(platform_time.monotonicNs() - start_ns, std.time.ns_per_ms)),
                    .shard_count = 1,
                    .dense_search = execution.dense_profile,
                };
                defer meta.deinit(alloc);
                try applyHostedProvisionedQueryAggregations(self, alloc, group_ids, table_name, response_req, &result, &meta, consistency);
                try applyQueryPostProcessing(alloc, response_req, &result, &meta, null, null);
                return try query_api.encodeQueryResponses(alloc, table_name, response_req, meta, result);
            }
        }

        if (group_ids.len > 1 and distributed_graph.supportsCrossRange(req)) {
            var base_req = req;
            base_req.graph_queries = &.{};
            base_req.expand_strategy = null;
            var merged = try queryHostedAcrossGroups(self, alloc, group_ids, base_req, table_name, consistency);
            defer merged.deinit();
            const graph_req = requestWithResultIdentityGeneration(req, merged);

            const worker = hostedGraphWorker(self);
            const graph_results = try distributed_graph.executeCrossRange(alloc, self.catalog, worker, table_name, graph_req, merged, consistency);
            merged.graph_results = graph_results;

            var meta: query_api.QueryResponseMeta = .{
                .took_ms = @intCast(@divTrunc(platform_time.monotonicNs() - start_ns, std.time.ns_per_ms)),
                .shard_count = @intCast(group_ids.len),
                .merged = true,
            };
            defer meta.deinit(alloc);
            try applyHostedProvisionedQueryAggregations(self, alloc, group_ids, table_name, graph_req, &merged, &meta, consistency);
            try applyQueryPostProcessing(alloc, graph_req, &merged, &meta, null, null);
            return try query_api.encodeQueryResponses(alloc, table_name, graph_req, meta, merged);
        }
        var merged = try queryHostedAcrossGroups(self, alloc, group_ids, req, table_name, consistency);
        defer merged.deinit();
        var meta: query_api.QueryResponseMeta = .{
            .took_ms = @intCast(@divTrunc(platform_time.monotonicNs() - start_ns, std.time.ns_per_ms)),
            .shard_count = @intCast(group_ids.len),
            .merged = group_ids.len > 1,
        };
        defer meta.deinit(alloc);
        try applyHostedProvisionedQueryAggregations(self, alloc, group_ids, table_name, req, &merged, &meta, consistency);
        try applyQueryPostProcessing(alloc, req, &merged, &meta, null, null);
        return try query_api.encodeQueryResponses(alloc, table_name, req, meta, merged);
    }

    fn preflightQuery(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        req: db_mod.types.SearchRequest,
        consistency: raft_mod.ReadConsistency,
        max_work: u32,
    ) !?db_mod.RuntimePreflightSummary {
        const self: *HostedProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        const group_ids = try table_catalog.resolveGroupsForSpan(alloc, self.catalog, table_name, "", "");
        defer alloc.free(group_ids);
        if (group_ids.len == 0) return null;
        try tableReadsValidateDocIdentityReadyForMultiGroup(alloc, self.catalog, table_name, group_ids.len);
        try validateResolvedDocFilterForGroups(alloc, self.catalog, table_name, group_ids, req);
        if (group_ids.len > 1) try distributed_graph.rejectUnstampedResultRefs(req);
        const plan = planFanout(.preflight, self.io_impl, group_ids.len);
        recordFanoutPlan(.preflight, plan);
        if (plan.parallel) {
            return try preflightHostedGroupsParallel(self, alloc, self.io_impl.?.io(), plan.width, group_ids, table_name, req, consistency, max_work);
        }
        if (plan.reason == .no_io and group_ids.len > 1) recordParallelFanoutFallback(.preflight);

        var first_summary: ?db_mod.RuntimePreflightSummary = null;
        var keep_summary = false;
        defer if (!keep_summary) {
            if (first_summary) |*summary| summary.deinit(alloc);
        };
        for (group_ids) |group_id| {
            var route = (try table_router.resolveGroupRoute(alloc, self.catalog, self.router, group_id, routePolicyForConsistency(consistency))) orelse {
                return null;
            };
            defer route.deinit(alloc);
            switch (route) {
                .local => {
                    const summary = try preflightHostedLocal(null, self.replica_root_dir, self.catalog, self.requester, alloc, group_id, self.visibleRootGeneration(group_id), self.backend_runtime, table_name, req, consistency, max_work);
                    if (first_summary == null) {
                        first_summary = summary;
                    } else {
                        try mergeRuntimePreflightSummary(alloc, &first_summary.?, summary);
                    }
                },
                .remote => |remote| {
                    const summary = try preflightRemote(self.executor, alloc, remote.base_uri, group_id, table_name, req, max_work);
                    if (first_summary == null) {
                        first_summary = summary;
                    } else {
                        try mergeRuntimePreflightSummary(alloc, &first_summary.?, summary);
                    }
                },
            }
        }
        keep_summary = true;
        return first_summary;
    }

    fn lookupGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        key: []const u8,
        opts: db_mod.types.LookupOptions,
        consistency: raft_mod.ReadConsistency,
    ) !?LookupResponse {
        const self: *HostedProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        return try lookupProvisionedHostedLocal(null, null, self.replica_root_dir, self.catalog, self.requester, alloc, group_id, self.visibleRootGeneration(group_id), self.backend_runtime, table_name, key, opts, consistency);
    }

    fn preflightQueryGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: db_mod.types.SearchRequest,
        consistency: raft_mod.ReadConsistency,
        max_work: u32,
    ) !?db_mod.RuntimePreflightSummary {
        const self: *HostedProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        return try preflightHostedLocal(null, self.replica_root_dir, self.catalog, self.requester, alloc, group_id, self.visibleRootGeneration(group_id), self.backend_runtime, table_name, req, consistency, max_work);
    }

    fn scanGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        from_key: []const u8,
        to_key: []const u8,
        opts: db_mod.types.ScanOptions,
        consistency: raft_mod.ReadConsistency,
    ) !?ScanResponse {
        const self: *HostedProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        return try scanProvisionedHostedLocal(null, self.replica_root_dir, self.catalog, self.requester, alloc, group_id, self.visibleRootGeneration(group_id), self.backend_runtime, table_name, from_key, to_key, opts, consistency);
    }

    fn queryGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: db_mod.types.SearchRequest,
        consistency: raft_mod.ReadConsistency,
    ) !?query_api.QueryResponse {
        const self: *HostedProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        const start_ns = platform_time.monotonicNs();
        const execution = try queryHostedLocalDetailed(null, self.replica_root_dir, self.catalog, self.requester, alloc, group_id, self.visibleRootGeneration(group_id), self.backend_runtime, null, null, null, table_name, req, consistency);
        var result = execution.result;
        defer result.deinit();
        const response_req = execution.request;
        var meta: query_api.QueryResponseMeta = .{
            .took_ms = @intCast(@divTrunc(platform_time.monotonicNs() - start_ns, std.time.ns_per_ms)),
            .shard_count = 1,
            .dense_search = execution.dense_profile,
        };
        defer meta.deinit(alloc);
        try applyHostedProvisionedQueryAggregations(self, alloc, &.{group_id}, table_name, response_req, &result, &meta, consistency);
        try applyQueryPostProcessing(alloc, response_req, &result, &meta, null, null);
        return try query_api.encodeQueryResponses(alloc, table_name, response_req, meta, result);
    }

    fn searchResultGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: db_mod.types.SearchRequest,
        consistency: raft_mod.ReadConsistency,
    ) !?db_mod.types.SearchResult {
        const self: *HostedProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        var route = (try table_router.resolveGroupRoute(alloc, self.catalog, self.router, group_id, routePolicyForConsistency(.read_index))) orelse return null;
        defer route.deinit(alloc);

        return switch (route) {
            .local => try queryHostedLocal(null, self.replica_root_dir, self.catalog, self.requester, alloc, group_id, self.visibleRootGeneration(group_id), self.backend_runtime, null, null, null, table_name, req, consistency),
            .remote => null,
        };
    }

    fn textStatsGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !?query_api.QueryResponse {
        const self: *HostedProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        return try collectProvisionedHostedLocalTextStats(null, self.replica_root_dir, self.catalog, alloc, group_id, self.visibleRootGeneration(group_id), self.backend_runtime, table_name, body);
    }

    fn algebraicPartialsGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !?query_api.QueryResponse {
        const self: *HostedProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        return try collectProvisionedHostedLocalAlgebraicPartials(null, self.replica_root_dir, self.catalog, alloc, group_id, self.visibleRootGeneration(group_id), self.backend_runtime, table_name, body);
    }

    fn documentAlgebraicAggregateGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: document_sql_runtime.AlgebraicAggregateRequest,
        consistency: raft_mod.ReadConsistency,
    ) !?document_sql_runtime.AlgebraicAggregateResponse {
        const self: *HostedProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        return try documentAlgebraicAggregateProvisionedHostedLocal(
            self.replica_root_dir,
            self.catalog,
            self.requester,
            alloc,
            group_id,
            self.visibleRootGeneration(group_id),
            self.backend_runtime,
            table_name,
            req,
            consistency,
        );
    }

    fn joinPartitionGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !?query_api.QueryResponse {
        const self: *HostedProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        var route = (try table_router.resolveGroupRoute(alloc, self.catalog, self.router, group_id, routePolicyForConsistency(.read_index))) orelse return null;
        defer route.deinit(alloc);

        return switch (route) {
            .local => null,
            .remote => |remote| joinPartitionRemote(self.executor, alloc, remote.base_uri, group_id, table_name, body) catch |err| switch (err) {
                error.UnexpectedHttpStatus => null,
                else => err,
            },
        };
    }

    fn joinRowsGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !?query_api.QueryResponse {
        const self: *HostedProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        var route = (try table_router.resolveGroupRoute(alloc, self.catalog, self.router, group_id, routePolicyForConsistency(.read_index))) orelse return null;
        defer route.deinit(alloc);

        return switch (route) {
            .local => null,
            .remote => |remote| joinRowsRemote(self.executor, alloc, remote.base_uri, group_id, table_name, body) catch |err| switch (err) {
                error.UnexpectedHttpStatus => null,
                else => err,
            },
        };
    }

    fn joinUnmatchedGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !?query_api.QueryResponse {
        const self: *HostedProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        var route = (try table_router.resolveGroupRoute(alloc, self.catalog, self.router, group_id, routePolicyForConsistency(.read_index))) orelse return null;
        defer route.deinit(alloc);

        return switch (route) {
            .local => null,
            .remote => |remote| joinUnmatchedRemote(self.executor, alloc, remote.base_uri, group_id, table_name, body) catch |err| switch (err) {
                error.UnexpectedHttpStatus => null,
                else => err,
            },
        };
    }

    fn joinFinalizeGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !?query_api.QueryResponse {
        const self: *HostedProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        var route = (try table_router.resolveGroupRoute(alloc, self.catalog, self.router, group_id, routePolicyForConsistency(.read_index))) orelse return null;
        defer route.deinit(alloc);

        return switch (route) {
            .local => null,
            .remote => |remote| joinFinalizeRemote(self.executor, alloc, remote.base_uri, group_id, table_name, body) catch |err| switch (err) {
                error.UnexpectedHttpStatus => null,
                else => err,
            },
        };
    }

    fn joinJobStateGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !?query_api.QueryResponse {
        const self: *HostedProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        var route = (try table_router.resolveGroupRoute(alloc, self.catalog, self.router, group_id, routePolicyForConsistency(.read_index))) orelse return null;
        defer route.deinit(alloc);

        return switch (route) {
            .local => null,
            .remote => |remote| joinJobStateRemote(self.executor, alloc, remote.base_uri, group_id, table_name, body) catch |err| switch (err) {
                error.UnexpectedHttpStatus => null,
                else => err,
            },
        };
    }

    fn localRuntimeStatuses(
        _: *anyopaque,
        _: std.mem.Allocator,
        _: []const u8,
    ) !?runtime_status.LocalTableRuntimeStatuses {
        return null;
    }

    fn localRuntimeStatusesCatalogNative(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        target: catalog_resources.TableTarget,
    ) !?runtime_status.LocalTableRuntimeStatuses {
        const self: *HostedProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        const table_name = try nativeCatalogTableNameAlloc(alloc, self.catalog, target);
        defer alloc.free(table_name);
        return try localRuntimeStatuses(ptr, alloc, table_name);
    }

    fn graphExpandGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: distributed_graph.GraphExpandRequest,
        consistency: raft_mod.ReadConsistency,
    ) !?distributed_graph.GraphExpandResponse {
        const self: *HostedProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        var route = (try table_router.resolveGroupRoute(alloc, self.catalog, self.router, group_id, routePolicyForConsistency(consistency))) orelse return null;
        defer route.deinit(alloc);

        return switch (route) {
            .local => blk: {
                try table_catalog.validateTopologyEpoch(alloc, self.catalog, table_name, req.topology_epoch);
                const Context = struct {
                    source: *HostedProvisionedTableReadSource,
                    group_id: u64,
                    table_name: []const u8,
                    consistency: raft_mod.ReadConsistency,

                    fn search(ctx: @This(), allocator: std.mem.Allocator, search_req: db_mod.types.SearchRequest) anyerror!db_mod.types.SearchResult {
                        return try queryHostedLocal(
                            null,
                            ctx.source.replica_root_dir,
                            ctx.source.catalog,
                            ctx.source.requester,
                            allocator,
                            ctx.group_id,
                            ctx.source.visibleRootGeneration(ctx.group_id),
                            ctx.source.backend_runtime,
                            null,
                            null,
                            null,
                            ctx.table_name,
                            search_req,
                            ctx.consistency,
                        );
                    }
                };
                break :blk try graphExpandWithSearch(Context, alloc, req, .{
                    .source = self,
                    .group_id = group_id,
                    .table_name = table_name,
                    .consistency = consistency,
                }, Context.search);
            },
            .remote => |remote| blk: {
                if (req.resolved_doc_filter != null) {
                    const ctx = req.resolved_doc_filter_wire_context orelse return error.UnsupportedQueryRequest;
                    try table_catalog.validateResolvedDocFilterContextForGroups(
                        alloc,
                        self.catalog,
                        table_name,
                        &.{group_id},
                        ctx.namespace.table_id,
                        ctx.namespace.shard_id,
                        ctx.namespace.range_id,
                    );
                }
                break :blk try graphExpandRemote(self.executor, alloc, remote.base_uri, group_id, table_name, req);
            },
        };
    }

    fn graphHydrateGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: distributed_graph.GraphHydrateRequest,
        consistency: raft_mod.ReadConsistency,
    ) !?distributed_graph.GraphHydrateResponse {
        const self: *HostedProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        try table_catalog.validateTopologyEpoch(alloc, self.catalog, table_name, req.topology_epoch);
        return try executeHostedGraphHydrate(ptr, alloc, group_id, table_name, req, consistency);
    }

    fn graphEdgesGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: distributed_graph.GraphEdgesRequest,
        consistency: raft_mod.ReadConsistency,
    ) !?distributed_graph.GraphEdgesResponse {
        const self: *HostedProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        var route = (try table_router.resolveGroupRoute(alloc, self.catalog, self.router, group_id, routePolicyForConsistency(consistency))) orelse return null;
        defer route.deinit(alloc);

        return switch (route) {
            .local => try graphGetEdgesLocal(alloc, self.replica_root_dir, self.catalog, self.requester, group_id, self.visibleRootGeneration(group_id), self.backend_runtime, table_name, req, consistency),
            .remote => |remote| try graphEdgesRemote(self.executor, alloc, remote.base_uri, group_id, table_name, req),
        };
    }
};

pub const executeLoweredSqlReadPlanAlloc = table_read_relational_rows.executeLoweredSqlReadPlanAlloc;
pub const executeLoweredSqlReadPlanWithSessionAlloc = table_read_relational_rows.executeLoweredSqlReadPlanWithSessionAlloc;
pub const executeLoweredRelationPopulationPlanAlloc = table_read_relational_rows.executeLoweredRelationPopulationPlanAlloc;
pub const RecursiveCteMaterializedRows = table_read_relational_rows.RecursiveCteMaterializedRows;
pub const materializeLoweredSqlRecursiveCteRowsAlloc = table_read_relational_rows.materializeLoweredRecursiveCteRowsAlloc;
pub const materializeLoweredSqlRecursiveCteRowsWithSessionAlloc = table_read_relational_rows.materializeLoweredRecursiveCteRowsWithSessionAlloc;

const catalogTargetForLoweredSqlTable = table_read_relational_rows.catalogTargetForLoweredSqlTable;
const loweredSqlSetOperationToRowsOperation = table_read_relational_rows.loweredSetOperationToRowsOperation;
const executeRelationalRowsSetOperationOnQueryResultsAlloc = table_read_relational_rows.executeSetOperationOnQueryResultsAlloc;

const catalogRuntimeSchemaUnlessDefaultAlloc = table_read_relational_rows.catalogRuntimeSchemaUnlessDefaultAlloc;

const loweredSqlReadJoinCteTableName = table_read_relational_rows.loweredReadJoinCteTableName;

pub const rowsInsertSourceBatchFromRoutedScansWithSchemasAlloc = table_read_relational_rows.rowsInsertSourceBatchFromRoutedScansWithSchemasAlloc;
pub const rowsInsertSourceBatchFromRecursiveCtePlanAlloc = table_read_relational_rows.rowsInsertSourceBatchFromRecursiveCtePlanAlloc;
pub const rowsInsertSourceBatchFromRecursiveCtePlanWithSessionAlloc = table_read_relational_rows.rowsInsertSourceBatchFromRecursiveCtePlanWithSessionAlloc;
pub const rowsInsertSourcePlanBatchFromRoutedScansWithSchemasAlloc = table_read_relational_rows.rowsInsertSourcePlanBatchFromRoutedScansWithSchemasAlloc;
pub const rowsMergeMutationBatchFromRoutedScansWithSchemasAlloc = table_read_relational_rows.rowsMergeMutationBatchFromRoutedScansWithSchemasAlloc;
pub const rowsRecursiveMergeMutationBatchFromRoutedScansWithSchemasAlloc = table_read_relational_rows.rowsRecursiveMergeMutationBatchFromRoutedScansWithSchemasAlloc;
pub const rowsRecursiveMergeMutationBatchFromRoutedScansWithSchemasAndSessionAlloc = table_read_relational_rows.rowsRecursiveMergeMutationBatchFromRoutedScansWithSchemasAndSessionAlloc;

fn rowsQueryPlanFromRoutedScansAlloc(
    alloc: std.mem.Allocator,
    source: TableReadSource,
    table_name: []const u8,
    runtime_schema: storage_schema.TableSchema,
    plan: db_mod.types.RelationalRowsQueryPlan,
    consistency: raft_mod.ReadConsistency,
) !?db_mod.types.RelationalRowsQueryResult {
    if (runtime_schema.external_base_source != null) {
        return try rowsQueryPlanFromLakeScanAlloc(alloc, source, table_name, runtime_schema, plan, consistency);
    }
    if (!relationalRowsScanPayloadCanStripSyntheticKey(runtime_schema)) return error.UnsupportedRowsQuery;

    var scanned_rows = (try collectRowsFromRoutedScansAlloc(alloc, source, table_name, plan.ranges, consistency)) orelse return null;
    defer scanned_rows.deinit(alloc);

    var local_plan = plan;
    local_plan.ranges = &.{};
    return try relational_rows_api.executeRowsQueryPlanOnJsonRowsAlloc(alloc, runtime_schema, local_plan, scanned_rows.rows);
}

fn rowsAggregatePlanFromRoutedScansAlloc(
    alloc: std.mem.Allocator,
    source: TableReadSource,
    table_name: []const u8,
    runtime_schema: storage_schema.TableSchema,
    plan: db_mod.types.RelationalRowsAggregatePlan,
    consistency: raft_mod.ReadConsistency,
) !?db_mod.types.RelationalRowsAggregateResult {
    if (runtime_schema.external_base_source != null) {
        return try rowsAggregatePlanFromLakeScanAlloc(alloc, source, table_name, runtime_schema, plan, consistency);
    }
    if (!relationalRowsScanPayloadCanStripSyntheticKey(runtime_schema)) return error.UnsupportedRowsQuery;

    var scanned_rows = (try collectRowsFromRoutedScansAlloc(alloc, source, table_name, plan.ranges, consistency)) orelse return null;
    defer scanned_rows.deinit(alloc);

    var local_plan = plan;
    local_plan.ranges = &.{};
    return try relational_rows_api.executeRowsAggregatePlanOnJsonRowsAlloc(alloc, runtime_schema, local_plan, scanned_rows.rows);
}

fn rowsSetOperationPlanFromRoutedScansAlloc(
    alloc: std.mem.Allocator,
    source: TableReadSource,
    table_name: []const u8,
    runtime_schema: storage_schema.TableSchema,
    plan: db_mod.types.RelationalRowsSetOperationPlan,
    consistency: raft_mod.ReadConsistency,
) !?db_mod.types.RelationalRowsQueryResult {
    var left = (try rowsQueryPlanFromRoutedScansAlloc(alloc, source, table_name, runtime_schema, plan.left, consistency)) orelse return null;
    defer left.deinit(alloc);
    var right = (try rowsQueryPlanFromRoutedScansAlloc(alloc, source, table_name, runtime_schema, plan.right, consistency)) orelse return null;
    defer right.deinit(alloc);
    return try executeRelationalRowsSetOperationOnQueryResultsAlloc(alloc, plan, left.rows, right.rows);
}

const rowsQueryPlanFromLakeScanAlloc = table_read_external_lake.rowsQueryPlanFromLakeScanAlloc;
const rowsAggregatePlanFromLakeScanAlloc = table_read_external_lake.rowsAggregatePlanFromLakeScanAlloc;
const rowsWindowPlanFromLakeScanAlloc = table_read_external_lake.rowsWindowPlanFromLakeScanAlloc;
const rowsJoinPlanFromLakeScanAlloc = table_read_external_lake.rowsJoinPlanFromLakeScanAlloc;
const rowsLateralPlanFromLakeScanAlloc = table_read_external_lake.rowsLateralPlanFromLakeScanAlloc;

pub const PinnedExternalLakeRowsScanner = table_read_external_lake.PinnedExternalLakeRowsScanner;
pub const PinnedExternalLakeSidecarContext = table_read_external_lake.PinnedExternalLakeSidecarContext;

pub const PinnedExternalObjectStorageLakeRowsScanner = table_read_external_lake.PinnedExternalObjectStorageLakeRowsScanner;

pub const PinnedExternalObjectStorageLakeRowsSource = table_read_external_lake.PinnedExternalObjectStorageLakeRowsSource;

pub const ExternalObjectStorageLakeRowsSourceOptions = table_read_external_lake.ExternalObjectStorageLakeRowsSourceOptions;
pub const ExternalLakePinnedSourceState = table_read_external_lake.ExternalLakePinnedSourceState;
pub const ExternalLakePhysicalScanSummary = table_read_external_lake.ExternalLakePhysicalScanSummary;
pub const OwnedExternalObjectStorageLakeRowsSource = table_read_external_lake.OwnedExternalObjectStorageLakeRowsSource;
pub const OpenedExternalObjectStorageLakeRowsSource = table_read_external_lake.OpenedExternalObjectStorageLakeRowsSource;
pub const ExternalLakeObjectStoreResolver = table_read_external_lake.ExternalLakeObjectStoreResolver;
pub const RemoteUriExternalLakeObjectStoreResolver = table_read_external_lake.RemoteUriExternalLakeObjectStoreResolver;
pub const ConfiguredExternalLakeObjectStoreResolver = table_read_external_lake.ConfiguredExternalLakeObjectStoreResolver;

pub const ExternalLakeRoutingTableReadSource = table_read_external_lake.ExternalLakeRoutingTableReadSource;

pub const executePinnedExternalLakeRowsScanAlloc = table_read_external_lake.executePinnedExternalLakeRowsScanAlloc;

fn rowsWindowPlanFromRoutedScansAlloc(
    alloc: std.mem.Allocator,
    source: TableReadSource,
    table_name: []const u8,
    runtime_schema: storage_schema.TableSchema,
    plan: db_mod.types.RelationalRowsWindowPlan,
    consistency: raft_mod.ReadConsistency,
) !?db_mod.types.RelationalRowsWindowResult {
    if (runtime_schema.external_base_source != null) {
        return try rowsWindowPlanFromLakeScanAlloc(alloc, source, table_name, runtime_schema, plan, consistency);
    }
    if (!relationalRowsScanPayloadCanStripSyntheticKey(runtime_schema)) return error.UnsupportedRowsQuery;

    var scanned_rows = (try collectRowsFromRoutedScansAlloc(alloc, source, table_name, plan.ranges, consistency)) orelse return null;
    defer scanned_rows.deinit(alloc);

    var local_plan = plan;
    local_plan.ranges = &.{};
    return try relational_rows_api.executeRowsWindowPlanOnJsonRowsAlloc(alloc, runtime_schema, local_plan, scanned_rows.rows);
}

fn rowsJoinPlanFromRoutedScansAlloc(
    alloc: std.mem.Allocator,
    source: TableReadSource,
    table_name: []const u8,
    runtime_schema: storage_schema.TableSchema,
    plan: db_mod.types.RelationalRowsJoinPlan,
    consistency: raft_mod.ReadConsistency,
) !?db_mod.types.RelationalRowsJoinResult {
    if (runtime_schema.external_base_source != null) {
        return try rowsJoinPlanFromLakeScanAlloc(alloc, source, table_name, runtime_schema, plan, consistency);
    }
    const left_table_name = relationalRowsEffectiveSideTable(table_name, plan.left_table);
    const right_table_name = relationalRowsEffectiveSideTable(table_name, plan.right_table);
    return try rowsJoinPlanFromRoutedScansWithSchemasAlloc(
        alloc,
        source,
        table_name,
        left_table_name,
        right_table_name,
        runtime_schema,
        runtime_schema,
        runtime_schema,
        plan,
        consistency,
    );
}

pub const rowsJoinPlanFromRoutedScansWithSchemasAlloc = table_read_relational_rows.rowsJoinPlanFromRoutedScansWithSchemasAlloc;

fn rowsLateralPlanFromRoutedScansAlloc(
    alloc: std.mem.Allocator,
    source: TableReadSource,
    table_name: []const u8,
    runtime_schema: storage_schema.TableSchema,
    plan: db_mod.types.RelationalRowsLateralPlan,
    consistency: raft_mod.ReadConsistency,
) !?db_mod.types.RelationalRowsJoinResult {
    if (runtime_schema.external_base_source != null) {
        return try rowsLateralPlanFromLakeScanAlloc(alloc, source, table_name, runtime_schema, plan, consistency);
    }
    const left_table_name = relationalRowsEffectiveSideTable(table_name, plan.left_table);
    const right_table_name = relationalRowsEffectiveSideTable(table_name, plan.right_table);
    return try rowsLateralPlanFromRoutedScansWithSchemasAlloc(
        alloc,
        source,
        table_name,
        left_table_name,
        right_table_name,
        runtime_schema,
        runtime_schema,
        runtime_schema,
        plan,
        consistency,
    );
}

pub const rowsLateralPlanFromRoutedScansWithSchemasAlloc = table_read_relational_rows.rowsLateralPlanFromRoutedScansWithSchemasAlloc;
const relationalRowsEffectiveSideTable = table_read_relational_rows.effectiveSideTable;

const RoutedRows = table_read_relational_rows.RoutedRows;
const collectMergeTargetRowsFromRoutedScansAlloc = table_read_relational_rows.collectMergeTargetRowsFromRoutedScansAlloc;
const collectMergeSourceRowsFromRoutedScansAlloc = table_read_relational_rows.collectMergeSourceRowsFromRoutedScansAlloc;
const collectRowsFromRoutedScansAlloc = table_read_relational_rows.collectRowsFromRoutedScansAlloc;
const routedRowsPlanRangesForJoinAlloc = table_read_relational_rows.routedRowsPlanRangesForJoinAlloc;
const routedRowsPlanRangesForJoinCtesAlloc = table_read_relational_rows.routedRowsPlanRangesForJoinCtesAlloc;
const relationalRowsScanPayloadCanStripSyntheticKey = table_read_relational_rows.scanPayloadCanStripSyntheticKey;
const resolveSingleUniqueOwnerGroup = table_read_relational_rows.resolveSingleUniqueOwnerGroup;
const lookupRelationalUniqueOwnerInDb = table_read_relational_rows.lookupRelationalUniqueOwnerInDb;
const lookupRelationalTemporalUniqueOwnerInDb = table_read_relational_rows.lookupRelationalTemporalUniqueOwnerInDb;
const lookupRelationalTemporalUniqueOverlapOwnerInDb = table_read_relational_rows.lookupRelationalTemporalUniqueOverlapOwnerInDb;

fn collectProvisionedSearchRequestTextStatsParallel(
    self: *ProvisionedTableReadSource,
    alloc: std.mem.Allocator,
    io: std.Io,
    width: usize,
    group_ids: []const u64,
    table_name: []const u8,
    body: []const u8,
) ![]const distributed_stats_mod.TextFieldStats {
    const start_ns = platform_time.monotonicNs();
    const slots = try initTextStatsFanoutSlots(alloc, group_ids.len);
    defer deinitTextStatsFanoutSlots(alloc, slots);

    const Fiber = struct {
        fn run(
            source: *ProvisionedTableReadSource,
            slot: *TextStatsFanoutSlot,
            group_id: u64,
            table_name_inner: []const u8,
            body_inner: []const u8,
        ) void {
            const arena = slot.arena.allocator();
            var response = collectProvisionedHostedLocalTextStats(
                source.cache,
                source.replica_root_dir,
                source.catalog,
                arena,
                group_id,
                source.visibleRootGeneration(group_id),
                source.backend_runtime,
                table_name_inner,
                body_inner,
            ) catch |err| {
                slot.err = err;
                return;
            } orelse {
                slot.err = error.TableNotFound;
                return;
            };
            defer response.deinit(arena);
            slot.fields = parseTextStatsResponse(arena, response.json) catch |err| {
                slot.err = err;
                return;
            };
        }
    };

    var start: usize = 0;
    while (start < group_ids.len) : (start += width) {
        const end = @min(start + width, group_ids.len);
        var group: std.Io.Group = .init;
        for (group_ids[start..end], start..end) |group_id, i| {
            group.async(io, Fiber.run, .{ self, &slots[i], group_id, table_name, body });
        }
        group.await(io) catch {};
    }

    for (slots) |slot| {
        if (slot.err) |err| return err;
    }

    const shard_stats = try alloc.alloc([]const distributed_stats_mod.TextFieldStats, group_ids.len);
    defer alloc.free(shard_stats);
    for (slots, 0..) |slot, i| shard_stats[i] = slot.fields;
    const merged = try mergeDistributedTextStats(alloc, shard_stats);
    recordParallelFanout(.text_stats, @intCast(platform_time.monotonicNs() - start_ns));
    return merged;
}

fn resolveHostedShardRoutes(
    self: *HostedProvisionedTableReadSource,
    alloc: std.mem.Allocator,
    group_ids: []const u64,
    consistency: raft_mod.ReadConsistency,
) ![]table_router.GroupRoute {
    const routes = try alloc.alloc(table_router.GroupRoute, group_ids.len);
    errdefer alloc.free(routes);
    var initialized: usize = 0;
    errdefer {
        for (routes[0..initialized]) |*route| route.deinit(alloc);
    }
    for (group_ids, 0..) |group_id, i| {
        routes[i] = (try table_router.resolveGroupRoute(alloc, self.catalog, self.router, group_id, routePolicyForConsistency(consistency))) orelse return error.TableNotFound;
        initialized += 1;
    }
    return routes;
}

fn deinitHostedShardRoutes(alloc: std.mem.Allocator, routes: []table_router.GroupRoute) void {
    for (routes) |*route| route.deinit(alloc);
    alloc.free(routes);
}

fn collectHostedSearchRequestTextStatsParallel(
    self: *HostedProvisionedTableReadSource,
    alloc: std.mem.Allocator,
    io: std.Io,
    width: usize,
    group_ids: []const u64,
    table_name: []const u8,
    body: []const u8,
    consistency: raft_mod.ReadConsistency,
) ![]const distributed_stats_mod.TextFieldStats {
    const start_ns = platform_time.monotonicNs();
    const routes = try resolveHostedShardRoutes(self, alloc, group_ids, consistency);
    defer deinitHostedShardRoutes(alloc, routes);

    const slots = try initTextStatsFanoutSlots(alloc, group_ids.len);
    defer deinitTextStatsFanoutSlots(alloc, slots);

    const Fiber = struct {
        fn run(
            source: *HostedProvisionedTableReadSource,
            slot: *TextStatsFanoutSlot,
            route: table_router.GroupRoute,
            group_id: u64,
            table_name_inner: []const u8,
            body_inner: []const u8,
        ) void {
            const arena = slot.arena.allocator();
            var response = switch (route) {
                .local => collectProvisionedHostedLocalTextStats(
                    null,
                    source.replica_root_dir,
                    source.catalog,
                    arena,
                    group_id,
                    0,
                    source.backend_runtime,
                    table_name_inner,
                    body_inner,
                ),
                .remote => |remote| textStatsRemote(source.executor, arena, remote.base_uri, group_id, table_name_inner, body_inner),
            } catch |err| {
                slot.err = err;
                return;
            } orelse {
                slot.err = error.TableNotFound;
                return;
            };
            defer response.deinit(arena);
            slot.fields = parseTextStatsResponse(arena, response.json) catch |err| {
                slot.err = err;
                return;
            };
        }
    };

    var start: usize = 0;
    while (start < group_ids.len) : (start += width) {
        const end = @min(start + width, group_ids.len);
        var group: std.Io.Group = .init;
        for (group_ids[start..end], start..end) |group_id, i| {
            group.async(io, Fiber.run, .{ self, &slots[i], routes[i], group_id, table_name, body });
        }
        group.await(io) catch {};
    }

    for (slots) |slot| {
        if (slot.err) |err| return err;
    }

    const shard_stats = try alloc.alloc([]const distributed_stats_mod.TextFieldStats, group_ids.len);
    defer alloc.free(shard_stats);
    for (slots, 0..) |slot, i| shard_stats[i] = slot.fields;
    const merged = try mergeDistributedTextStats(alloc, shard_stats);
    recordParallelFanout(.text_stats, @intCast(platform_time.monotonicNs() - start_ns));
    return merged;
}

fn queryProvisionedAcrossGroupsParallel(
    self: *ProvisionedTableReadSource,
    alloc: std.mem.Allocator,
    io: std.Io,
    width: usize,
    group_ids: []const u64,
    shard_req: *const db_mod.types.SearchRequest,
    req: db_mod.types.SearchRequest,
    table_name: []const u8,
    consistency: raft_mod.ReadConsistency,
) !db_mod.types.SearchResult {
    const start_ns = platform_time.monotonicNs();
    const slots = try initSearchFanoutSlots(alloc, group_ids.len);
    defer deinitSearchFanoutSlots(alloc, slots);

    const Fiber = struct {
        fn run(
            source: *ProvisionedTableReadSource,
            slot: *SearchFanoutSlot,
            group_id: u64,
            table_name_inner: []const u8,
            shard_req_inner: *const db_mod.types.SearchRequest,
            consistency_inner: raft_mod.ReadConsistency,
        ) void {
            const arena = slot.arena.allocator();
            slot.result = queryHostedLocal(
                source.cache,
                source.replica_root_dir,
                source.catalog,
                source.requester,
                arena,
                group_id,
                source.visibleRootGeneration(group_id),
                source.backend_runtime,
                source.antfly_provider,
                source.secret_store,
                source.remote_content,
                table_name_inner,
                shard_req_inner.*,
                consistency_inner,
            ) catch |err| {
                slot.err = err;
                return;
            };
        }
    };

    var start: usize = 0;
    while (start < group_ids.len) : (start += width) {
        const end = @min(start + width, group_ids.len);
        var group: std.Io.Group = .init;
        for (group_ids[start..end], start..end) |group_id, i| {
            group.async(io, Fiber.run, .{ self, &slots[i], group_id, table_name, shard_req, consistency });
        }
        group.await(io) catch {};
    }

    for (slots) |slot| {
        if (slot.err) |err| return err;
    }

    const shard_results = try alloc.alloc(db_mod.types.SearchResult, group_ids.len);
    defer alloc.free(shard_results);
    for (slots, 0..) |slot, i| shard_results[i] = slot.result.?;
    const merged = try query_api.mergeSearchResults(alloc, req, shard_results, req.offset, req.limit);
    recordParallelFanout(.query, @intCast(platform_time.monotonicNs() - start_ns));
    return merged;
}

fn queryHostedAcrossGroupsParallel(
    self: *HostedProvisionedTableReadSource,
    alloc: std.mem.Allocator,
    io: std.Io,
    width: usize,
    group_ids: []const u64,
    shard_req: *const db_mod.types.SearchRequest,
    req: db_mod.types.SearchRequest,
    table_name: []const u8,
    consistency: raft_mod.ReadConsistency,
) !db_mod.types.SearchResult {
    const start_ns = platform_time.monotonicNs();
    const routes = try resolveHostedShardRoutes(self, alloc, group_ids, consistency);
    defer deinitHostedShardRoutes(alloc, routes);

    const slots = try initSearchFanoutSlots(alloc, group_ids.len);
    defer deinitSearchFanoutSlots(alloc, slots);

    const Fiber = struct {
        fn run(
            source: *HostedProvisionedTableReadSource,
            slot: *SearchFanoutSlot,
            route: table_router.GroupRoute,
            group_id: u64,
            table_name_inner: []const u8,
            shard_req_inner: *const db_mod.types.SearchRequest,
            consistency_inner: raft_mod.ReadConsistency,
        ) void {
            const arena = slot.arena.allocator();
            slot.result = switch (route) {
                .local => queryHostedLocal(
                    null,
                    source.replica_root_dir,
                    source.catalog,
                    source.requester,
                    arena,
                    group_id,
                    0,
                    source.backend_runtime,
                    null,
                    null,
                    null,
                    table_name_inner,
                    shard_req_inner.*,
                    consistency_inner,
                ),
                .remote => |remote| queryRemote(source.executor, arena, remote.base_uri, group_id, table_name_inner, shard_req_inner.*),
            } catch |err| {
                slot.err = err;
                return;
            };
        }
    };

    var start: usize = 0;
    while (start < group_ids.len) : (start += width) {
        const end = @min(start + width, group_ids.len);
        var group: std.Io.Group = .init;
        for (group_ids[start..end], start..end) |group_id, i| {
            group.async(io, Fiber.run, .{ self, &slots[i], routes[i], group_id, table_name, shard_req, consistency });
        }
        group.await(io) catch {};
    }

    for (slots) |slot| {
        if (slot.err) |err| return err;
    }

    const shard_results = try alloc.alloc(db_mod.types.SearchResult, group_ids.len);
    defer alloc.free(shard_results);
    for (slots, 0..) |slot, i| shard_results[i] = slot.result.?;
    const merged = try query_api.mergeSearchResults(alloc, req, shard_results, req.offset, req.limit);
    recordParallelFanout(.query, @intCast(platform_time.monotonicNs() - start_ns));
    return merged;
}

fn preflightProvisionedGroupsParallel(
    self: *ProvisionedTableReadSource,
    alloc: std.mem.Allocator,
    io: std.Io,
    width: usize,
    group_ids: []const u64,
    table_name: []const u8,
    req: db_mod.types.SearchRequest,
    consistency: raft_mod.ReadConsistency,
    max_work: u32,
) !?db_mod.RuntimePreflightSummary {
    const start_ns = platform_time.monotonicNs();
    const slots = try initPreflightFanoutSlots(alloc, group_ids.len);
    defer deinitPreflightFanoutSlots(alloc, slots);

    const Fiber = struct {
        fn run(
            source: *ProvisionedTableReadSource,
            slot: *PreflightFanoutSlot,
            group_id: u64,
            table_name_inner: []const u8,
            req_inner: *const db_mod.types.SearchRequest,
            consistency_inner: raft_mod.ReadConsistency,
            max_work_inner: u32,
        ) void {
            const arena = slot.arena.allocator();
            slot.summary = preflightHostedLocal(
                source.cache,
                source.replica_root_dir,
                source.catalog,
                source.requester,
                arena,
                group_id,
                source.visibleRootGeneration(group_id),
                source.backend_runtime,
                table_name_inner,
                req_inner.*,
                consistency_inner,
                max_work_inner,
            ) catch |err| {
                slot.err = err;
                return;
            };
        }
    };

    var start: usize = 0;
    while (start < group_ids.len) : (start += width) {
        const end = @min(start + width, group_ids.len);
        var group: std.Io.Group = .init;
        for (group_ids[start..end], start..end) |group_id, i| {
            group.async(io, Fiber.run, .{ self, &slots[i], group_id, table_name, &req, consistency, max_work });
        }
        group.await(io) catch {};
    }

    for (slots) |slot| {
        if (slot.err) |err| return err;
    }

    var merged = try cloneRuntimePreflightSummary(alloc, slots[0].summary.?);
    errdefer merged.deinit(alloc);
    for (slots[1..]) |slot| {
        try mergeRuntimePreflightSummaryNoFree(alloc, &merged, slot.summary.?);
    }
    recordParallelFanout(.preflight, @intCast(platform_time.monotonicNs() - start_ns));
    return merged;
}

fn preflightHostedGroupsParallel(
    self: *HostedProvisionedTableReadSource,
    alloc: std.mem.Allocator,
    io: std.Io,
    width: usize,
    group_ids: []const u64,
    table_name: []const u8,
    req: db_mod.types.SearchRequest,
    consistency: raft_mod.ReadConsistency,
    max_work: u32,
) !?db_mod.RuntimePreflightSummary {
    const start_ns = platform_time.monotonicNs();
    const routes = try alloc.alloc(table_router.GroupRoute, group_ids.len);
    var initialized: usize = 0;
    defer {
        for (routes[0..initialized]) |*route| route.deinit(alloc);
        alloc.free(routes);
    }
    for (group_ids, 0..) |group_id, i| {
        routes[i] = (try table_router.resolveGroupRoute(alloc, self.catalog, self.router, group_id, routePolicyForConsistency(consistency))) orelse {
            return null;
        };
        initialized += 1;
    }

    const slots = try initPreflightFanoutSlots(alloc, group_ids.len);
    defer deinitPreflightFanoutSlots(alloc, slots);

    const Fiber = struct {
        fn run(
            source: *HostedProvisionedTableReadSource,
            slot: *PreflightFanoutSlot,
            route: table_router.GroupRoute,
            group_id: u64,
            table_name_inner: []const u8,
            req_inner: *const db_mod.types.SearchRequest,
            consistency_inner: raft_mod.ReadConsistency,
            max_work_inner: u32,
        ) void {
            const arena = slot.arena.allocator();
            slot.summary = switch (route) {
                .local => preflightHostedLocal(
                    null,
                    source.replica_root_dir,
                    source.catalog,
                    source.requester,
                    arena,
                    group_id,
                    0,
                    source.backend_runtime,
                    table_name_inner,
                    req_inner.*,
                    consistency_inner,
                    max_work_inner,
                ),
                .remote => |remote| preflightRemote(
                    source.executor,
                    arena,
                    remote.base_uri,
                    group_id,
                    table_name_inner,
                    req_inner.*,
                    max_work_inner,
                ),
            } catch |err| {
                slot.err = err;
                return;
            };
        }
    };

    var start: usize = 0;
    while (start < group_ids.len) : (start += width) {
        const end = @min(start + width, group_ids.len);
        var group: std.Io.Group = .init;
        for (group_ids[start..end], start..end) |group_id, i| {
            group.async(io, Fiber.run, .{ self, &slots[i], routes[i], group_id, table_name, &req, consistency, max_work });
        }
        group.await(io) catch {};
    }

    for (slots) |slot| {
        if (slot.err) |err| return err;
    }

    var merged = try cloneRuntimePreflightSummary(alloc, slots[0].summary.?);
    errdefer merged.deinit(alloc);
    for (slots[1..]) |slot| {
        try mergeRuntimePreflightSummaryNoFree(alloc, &merged, slot.summary.?);
    }
    recordParallelFanout(.preflight, @intCast(platform_time.monotonicNs() - start_ns));
    return merged;
}

fn rejectCrossGroupResolvedDocFilter(req: db_mod.types.SearchRequest, group_count: usize) !void {
    if (group_count > 1 and searchRequestHasUnserializableResolvedDocFilter(req)) return error.UnsupportedQueryRequest;
}

fn validateResolvedDocFilterForGroups(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
    group_ids: []const u64,
    req: db_mod.types.SearchRequest,
) !void {
    if (!searchRequestHasResolvedDocFilter(req)) return;
    if (searchRequestHasUnserializableResolvedDocFilter(req)) {
        if (group_ids.len > 1) return error.UnsupportedQueryRequest;
        return;
    }
    const ctx = req.resolved_doc_filter_wire_context orelse return error.UnsupportedQueryRequest;
    try table_catalog.validateResolvedDocFilterContextForGroups(
        alloc,
        catalog,
        table_name,
        group_ids,
        ctx.namespace.table_id,
        ctx.namespace.shard_id,
        ctx.namespace.range_id,
    );
}

fn tableReadsValidateDocIdentityReadyForMultiGroup(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
    group_count: usize,
) !void {
    if (group_count <= 1) return;
    try table_catalog.validateDocIdentityReadyForTable(alloc, catalog, table_name);
}

fn rejectRemoteRouteResolvedDocFilter(req: db_mod.types.SearchRequest, route: table_router.GroupRoute) !void {
    if (!searchRequestHasUnserializableResolvedDocFilter(req)) return;
    switch (route) {
        .local => {},
        .remote => return error.UnsupportedQueryRequest,
    }
}

fn validateResolvedDocFilterForRemoteRoute(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
    group_id: u64,
    req: db_mod.types.SearchRequest,
    route: table_router.GroupRoute,
) !void {
    switch (route) {
        .local => {},
        .remote => {
            if (searchRequestHasUnserializableResolvedDocFilter(req)) return error.UnsupportedQueryRequest;
            try validateResolvedDocFilterForGroups(alloc, catalog, table_name, &.{group_id}, req);
        },
    }
}

fn rejectHostedRemoteResolvedDocFilter(
    self: *HostedProvisionedTableReadSource,
    alloc: std.mem.Allocator,
    group_ids: []const u64,
    table_name: []const u8,
    req: db_mod.types.SearchRequest,
    consistency: raft_mod.ReadConsistency,
) !void {
    if (!searchRequestHasResolvedDocFilter(req) or group_ids.len != 1) return;
    var route = (try table_router.resolveGroupRoute(alloc, self.catalog, self.router, group_ids[0], routePolicyForConsistency(consistency))) orelse return error.TableNotFound;
    defer route.deinit(alloc);
    try validateResolvedDocFilterForRemoteRoute(alloc, self.catalog, table_name, group_ids[0], req, route);
}

fn queryProvisionedAcrossGroups(
    self: *ProvisionedTableReadSource,
    alloc: std.mem.Allocator,
    group_ids: []const u64,
    req: db_mod.types.SearchRequest,
    table_name: []const u8,
    consistency: raft_mod.ReadConsistency,
) !db_mod.types.SearchResult {
    try tableReadsValidateDocIdentityReadyForMultiGroup(alloc, self.catalog, table_name, group_ids.len);
    try validateResolvedDocFilterForGroups(alloc, self.catalog, table_name, group_ids, req);
    const distributed_text_stats = try collectProvisionedSearchRequestTextStats(self, alloc, group_ids, req, table_name);
    defer distributed_stats_mod.deinitTextFieldStats(alloc, distributed_text_stats);
    const shard_limit = req.limit + req.offset;
    const shard_req = blk: {
        var copy = req;
        copy.offset = 0;
        copy.limit = if (shard_limit == 0) req.limit else shard_limit;
        copy.distributed_text_stats = distributed_text_stats;
        break :blk copy;
    };
    var fan_in_shard_req = try prepareGraphMetricFanInShardRequest(alloc, shard_req);
    defer fan_in_shard_req.deinit(alloc);

    const plan = planQueryFanout(self.io_impl, group_ids.len, req);
    recordFanoutPlan(.query, plan);
    if (plan.parallel) {
        return try queryProvisionedAcrossGroupsParallel(self, alloc, self.io_impl.?.io(), plan.width, group_ids, &fan_in_shard_req.req, req, table_name, consistency);
    }
    if (plan.reason == .no_io) recordParallelFanoutFallback(.query);

    var shard_results = try alloc.alloc(db_mod.types.SearchResult, group_ids.len);
    var initialized: usize = 0;
    defer {
        for (shard_results[0..initialized]) |*result| result.deinit();
        alloc.free(shard_results);
    }

    for (group_ids, 0..) |group_id, i| {
        shard_results[i] = try queryHostedLocal(self.cache, self.replica_root_dir, self.catalog, self.requester, alloc, group_id, self.visibleRootGeneration(group_id), self.backend_runtime, self.antfly_provider, self.secret_store, self.remote_content, table_name, fan_in_shard_req.req, consistency);
        initialized += 1;
    }
    return try query_api.mergeSearchResults(alloc, req, shard_results[0..initialized], req.offset, req.limit);
}

fn queryHostedAcrossGroups(
    self: *HostedProvisionedTableReadSource,
    alloc: std.mem.Allocator,
    group_ids: []const u64,
    req: db_mod.types.SearchRequest,
    table_name: []const u8,
    consistency: raft_mod.ReadConsistency,
) !db_mod.types.SearchResult {
    try tableReadsValidateDocIdentityReadyForMultiGroup(alloc, self.catalog, table_name, group_ids.len);
    try validateResolvedDocFilterForGroups(alloc, self.catalog, table_name, group_ids, req);
    try rejectHostedRemoteResolvedDocFilter(self, alloc, group_ids, table_name, req, consistency);
    const distributed_text_stats = try collectHostedSearchRequestTextStats(self, alloc, group_ids, req, table_name, consistency);
    defer distributed_stats_mod.deinitTextFieldStats(alloc, distributed_text_stats);
    const shard_limit = req.limit + req.offset;
    const shard_req = blk: {
        var copy = req;
        copy.offset = 0;
        copy.limit = if (shard_limit == 0) req.limit else shard_limit;
        copy.distributed_text_stats = distributed_text_stats;
        break :blk copy;
    };
    var fan_in_shard_req = try prepareGraphMetricFanInShardRequest(alloc, shard_req);
    defer fan_in_shard_req.deinit(alloc);

    const plan = planQueryFanout(self.io_impl, group_ids.len, req);
    recordFanoutPlan(.query, plan);
    if (plan.parallel) {
        return try queryHostedAcrossGroupsParallel(self, alloc, self.io_impl.?.io(), plan.width, group_ids, &fan_in_shard_req.req, req, table_name, consistency);
    }
    if (plan.reason == .no_io) recordParallelFanoutFallback(.query);

    var shard_results = try alloc.alloc(db_mod.types.SearchResult, group_ids.len);
    var initialized: usize = 0;
    defer {
        for (shard_results[0..initialized]) |*result| result.deinit();
        alloc.free(shard_results);
    }

    for (group_ids, 0..) |group_id, i| {
        var route = (try table_router.resolveGroupRoute(alloc, self.catalog, self.router, group_id, routePolicyForConsistency(consistency))) orelse return error.TableNotFound;
        defer route.deinit(alloc);
        shard_results[i] = switch (route) {
            .local => try queryHostedLocal(null, self.replica_root_dir, self.catalog, self.requester, alloc, group_id, self.visibleRootGeneration(group_id), self.backend_runtime, null, null, null, table_name, fan_in_shard_req.req, consistency),
            .remote => |remote| try queryRemote(self.executor, alloc, remote.base_uri, group_id, table_name, fan_in_shard_req.req),
        };
        initialized += 1;
    }
    return try query_api.mergeSearchResults(alloc, req, shard_results[0..initialized], req.offset, req.limit);
}

fn provisionedGraphWorker(self: *ProvisionedTableReadSource) distributed_graph.Worker {
    return .{
        .ptr = self,
        .vtable = &.{
            .execute_graph_expand = executeProvisionedGraphExpand,
            .execute_graph_hydrate = executeProvisionedGraphHydrate,
            .execute_graph_get_edges = executeProvisionedGraphGetEdges,
            .fanout_io = provisionedGraphFanoutIo,
            .fanout_width_cap = provisionedGraphFanoutWidthCap,
        },
    };
}

fn hostedGraphWorker(self: *HostedProvisionedTableReadSource) distributed_graph.Worker {
    return .{
        .ptr = self,
        .vtable = &.{
            .execute_graph_expand = executeHostedGraphExpand,
            .execute_graph_hydrate = executeHostedGraphHydrate,
            .execute_graph_get_edges = executeHostedGraphGetEdges,
            .fanout_io = hostedGraphFanoutIo,
            .fanout_width_cap = hostedGraphFanoutWidthCap,
        },
    };
}

fn provisionedGraphFanoutIo(ptr: *anyopaque) ?std.Io {
    const self: *ProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
    const io_impl = self.io_impl orelse return null;
    return io_impl.io();
}

fn provisionedGraphFanoutWidthCap(ptr: *anyopaque) usize {
    const self: *ProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
    const io_impl = self.io_impl orelse return 1;
    return table_read_fanout.ioAsyncLimitCap(io_impl);
}

fn hostedGraphFanoutIo(ptr: *anyopaque) ?std.Io {
    const self: *HostedProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
    const io_impl = self.io_impl orelse return null;
    return io_impl.io();
}

fn hostedGraphFanoutWidthCap(ptr: *anyopaque) usize {
    const self: *HostedProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
    const io_impl = self.io_impl orelse return 1;
    return table_read_fanout.ioAsyncLimitCap(io_impl);
}

fn executeProvisionedGraphExpand(
    ptr: *anyopaque,
    alloc: std.mem.Allocator,
    group_id: u64,
    table_name: []const u8,
    req: distributed_graph.GraphExpandRequest,
    consistency: raft_mod.ReadConsistency,
) !distributed_graph.GraphExpandResponse {
    const self: *ProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
    try table_catalog.validateTopologyEpoch(alloc, self.catalog, table_name, req.topology_epoch);
    const Context = struct {
        source: *ProvisionedTableReadSource,
        group_id: u64,
        table_name: []const u8,
        consistency: raft_mod.ReadConsistency,

        fn search(ctx: @This(), allocator: std.mem.Allocator, search_req: db_mod.types.SearchRequest) anyerror!db_mod.types.SearchResult {
            return try queryHostedLocal(
                null,
                ctx.source.replica_root_dir,
                ctx.source.catalog,
                ctx.source.requester,
                allocator,
                ctx.group_id,
                ctx.source.visibleRootGeneration(ctx.group_id),
                ctx.source.backend_runtime,
                null,
                null,
                null,
                ctx.table_name,
                search_req,
                ctx.consistency,
            );
        }
    };
    return try graphExpandWithSearch(Context, alloc, req, .{
        .source = self,
        .group_id = group_id,
        .table_name = table_name,
        .consistency = consistency,
    }, Context.search);
}

fn executeProvisionedGraphHydrate(
    ptr: *anyopaque,
    alloc: std.mem.Allocator,
    group_id: u64,
    table_name: []const u8,
    req: distributed_graph.GraphHydrateRequest,
    consistency: raft_mod.ReadConsistency,
) !distributed_graph.GraphHydrateResponse {
    const self: *ProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
    try table_catalog.validateTopologyEpoch(alloc, self.catalog, table_name, req.topology_epoch);
    const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
    defer alloc.free(path);
    var db = try openProvisionedQueryDbForTableWithRuntime(alloc, path, self.catalog, table_name, group_id, self.visibleRootGeneration(group_id), self.backend_runtime);
    defer db.close();
    try validateGraphHydrateResolvedDocFilterForDb(req, &db);

    var reads = raft_mod.FeatureDBReads.init(group_id, self.requester);
    var hits = std.ArrayListUnmanaged(db_mod.types.SearchHit).empty;
    errdefer {
        for (hits.items) |*hit| hit.deinit(alloc);
        hits.deinit(alloc);
    }

    for (req.keys) |key| {
        var result = (reads.lookupWithConsistency(alloc, &db, key, .{}, consistency) catch |err| switch (err) {
            error.NotLeader => if (consistency == .stale) return err else try reads.lookupWithConsistency(alloc, &db, key, .{}, .stale),
            else => return err,
        }) orelse continue;
        defer result.deinit(alloc);
        const ordinal = try db.lookupLiveDocOrdinalForInternalRead(alloc, key, req.identity_read_generation);
        if (!graphHydrateResolvedDocFilterAllows(req, key, ordinal)) continue;
        try hits.append(alloc, .{
            .id = try alloc.dupe(u8, key),
            .doc_ordinal = ordinal,
            .stored_data = try alloc.dupe(u8, result.json),
        });
    }
    return .{ .hits = try hits.toOwnedSlice(alloc) };
}

fn executeHostedGraphExpand(
    ptr: *anyopaque,
    alloc: std.mem.Allocator,
    group_id: u64,
    table_name: []const u8,
    req: distributed_graph.GraphExpandRequest,
    consistency: raft_mod.ReadConsistency,
) !distributed_graph.GraphExpandResponse {
    return (try HostedProvisionedTableReadSource.graphExpandGroupLocal(ptr, alloc, group_id, table_name, req, consistency)) orelse return error.TableNotFound;
}

fn executeHostedGraphHydrate(
    ptr: *anyopaque,
    alloc: std.mem.Allocator,
    group_id: u64,
    table_name: []const u8,
    req: distributed_graph.GraphHydrateRequest,
    consistency: raft_mod.ReadConsistency,
) !distributed_graph.GraphHydrateResponse {
    const self: *HostedProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
    var route = (try table_router.resolveGroupRoute(alloc, self.catalog, self.router, group_id, routePolicyForConsistency(consistency))) orelse return error.TableNotFound;
    defer route.deinit(alloc);

    return switch (route) {
        .local => blk: {
            const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
            defer alloc.free(path);
            const identity_namespace = try loadTableIdentityNamespaceForGroup(alloc, self.catalog, table_name, group_id);
            var db = try db_mod.DB.open(alloc, path, .{
                .backend_runtime = self.backend_runtime,
                .identity_namespace = identity_namespace,
                .prefer_existing_identity_namespace = identity_namespace != null,
            });
            defer db.close();
            try validateOpenedProvisionedDbIdentityNamespace(&db, identity_namespace);
            try validateGraphHydrateResolvedDocFilterForDb(req, &db);

            var reads = raft_mod.FeatureDBReads.init(group_id, self.requester);
            var hits = std.ArrayListUnmanaged(db_mod.types.SearchHit).empty;
            errdefer {
                for (hits.items) |*hit| hit.deinit(alloc);
                hits.deinit(alloc);
            }

            for (req.keys) |key| {
                var result = (try reads.lookupWithConsistency(alloc, &db, key, .{}, consistency)) orelse continue;
                defer result.deinit(alloc);
                const ordinal = try db.lookupLiveDocOrdinalForInternalRead(alloc, key, req.identity_read_generation);
                if (!graphHydrateResolvedDocFilterAllows(req, key, ordinal)) continue;
                try hits.append(alloc, .{
                    .id = try alloc.dupe(u8, key),
                    .doc_ordinal = ordinal,
                    .stored_data = try alloc.dupe(u8, result.json),
                });
            }
            break :blk .{ .hits = try hits.toOwnedSlice(alloc) };
        },
        .remote => |remote| blk: {
            if (req.resolved_doc_filter != null) {
                const ctx = req.resolved_doc_filter_wire_context orelse return error.UnsupportedQueryRequest;
                try table_catalog.validateResolvedDocFilterContextForGroups(
                    alloc,
                    self.catalog,
                    table_name,
                    &.{group_id},
                    ctx.namespace.table_id,
                    ctx.namespace.shard_id,
                    ctx.namespace.range_id,
                );
            }
            break :blk try graphHydrateRemote(self.executor, alloc, remote.base_uri, group_id, table_name, req);
        },
    };
}

fn executeProvisionedGraphGetEdges(
    ptr: *anyopaque,
    alloc: std.mem.Allocator,
    group_id: u64,
    table_name: []const u8,
    req: distributed_graph.GraphEdgesRequest,
    consistency: raft_mod.ReadConsistency,
) anyerror!distributed_graph.GraphEdgesResponse {
    const self: *ProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
    return graphGetEdgesLocal(alloc, self.replica_root_dir, self.catalog, self.requester, group_id, self.visibleRootGeneration(group_id), self.backend_runtime, table_name, req, consistency);
}

fn executeHostedGraphGetEdges(
    ptr: *anyopaque,
    alloc: std.mem.Allocator,
    group_id: u64,
    table_name: []const u8,
    req: distributed_graph.GraphEdgesRequest,
    consistency: raft_mod.ReadConsistency,
) anyerror!distributed_graph.GraphEdgesResponse {
    const self: *HostedProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
    var route = (try table_router.resolveGroupRoute(alloc, self.catalog, self.router, group_id, routePolicyForConsistency(consistency))) orelse return error.TableNotFound;
    defer route.deinit(alloc);

    return switch (route) {
        .local => graphGetEdgesLocal(alloc, self.replica_root_dir, self.catalog, self.requester, group_id, self.visibleRootGeneration(group_id), self.backend_runtime, table_name, req, consistency),
        .remote => |remote| try graphEdgesRemote(self.executor, alloc, remote.base_uri, group_id, table_name, req),
    };
}

fn lookupLocal(
    replica_root_dir: []const u8,
    requester: raft_mod.ReadableLeaseRequester,
    alloc: std.mem.Allocator,
    group_id: u64,
    key: []const u8,
    opts: db_mod.types.LookupOptions,
    consistency: raft_mod.ReadConsistency,
) !?LookupResponse {
    const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, replica_root_dir, group_id);
    defer alloc.free(path);
    var db = try db_mod.DB.open(alloc, path, .{});
    defer db.close();

    var reads = raft_mod.FeatureDBReads.init(group_id, requester);
    var result = (try reads.lookupWithConsistency(alloc, &db, key, opts, consistency)) orelse return null;
    defer result.deinit(alloc);
    return .{
        .json = try alloc.dupe(u8, result.json),
        .version = try db.getTimestamp(alloc, key),
    };
}

fn lookupRelationalUniqueOwnerProvisionedLocal(
    primary_lookup_db: ?PrimaryLookupDbSource,
    cache: ?*ProvisionedTableReadCache,
    replica_root_dir: []const u8,
    catalog: table_catalog.CatalogSource,
    requester: raft_mod.ReadableLeaseRequester,
    alloc: std.mem.Allocator,
    group_id: u64,
    lsm_root_generation: u64,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
    table_name: []const u8,
    constraint_name: []const u8,
    encoded_value: []const u8,
    consistency: raft_mod.ReadConsistency,
) !?[]u8 {
    if (primary_lookup_db) |source| {
        if (try source.leaseGroup(alloc, table_name, group_id, lsm_root_generation)) |lease_value| {
            var lease = lease_value;
            defer lease.release(alloc);
            try validateProvisionedDbIdentityNamespace(alloc, catalog, table_name, group_id, lease.db);
            return try lookupRelationalUniqueOwnerInDb(alloc, lease.db, group_id, raft_mod.FeatureReads.init(requester), constraint_name, encoded_value, consistency);
        }
    }

    const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, replica_root_dir, group_id);
    defer alloc.free(path);
    const identity_namespace = try loadTableIdentityNamespaceForGroup(alloc, catalog, table_name, group_id);
    if (cache) |cached| {
        if (cached.getIfPresent(group_id, lsm_root_generation, identity_namespace, table_name)) |db_lease_value| {
            var db_lease = db_lease_value;
            defer db_lease.release();
            return try lookupRelationalUniqueOwnerInDb(alloc, db_lease.db, group_id, raft_mod.FeatureReads.init(requester), constraint_name, encoded_value, consistency);
        }
    }

    var db = try openProvisionedLookupDbForTable(
        alloc,
        path,
        if (cache) |cached| cached.lsm_cache else null,
        lsm_root_generation,
        if (cache) |cached| cached.resource_manager else null,
        backend_runtime,
        identity_namespace,
    );
    defer db.close();
    return try lookupRelationalUniqueOwnerInDb(alloc, &db, group_id, raft_mod.FeatureReads.init(requester), constraint_name, encoded_value, consistency);
}

fn lookupRelationalTemporalUniqueOwnerProvisionedLocal(
    primary_lookup_db: ?PrimaryLookupDbSource,
    cache: ?*ProvisionedTableReadCache,
    replica_root_dir: []const u8,
    catalog: table_catalog.CatalogSource,
    requester: raft_mod.ReadableLeaseRequester,
    alloc: std.mem.Allocator,
    group_id: u64,
    lsm_root_generation: u64,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
    table_name: []const u8,
    constraint_name: []const u8,
    encoded_value: []const u8,
    encoded_point: []const u8,
    consistency: raft_mod.ReadConsistency,
) !?[]u8 {
    if (primary_lookup_db) |source| {
        if (try source.leaseGroup(alloc, table_name, group_id, lsm_root_generation)) |lease_value| {
            var lease = lease_value;
            defer lease.release(alloc);
            try validateProvisionedDbIdentityNamespace(alloc, catalog, table_name, group_id, lease.db);
            return try lookupRelationalTemporalUniqueOwnerInDb(alloc, lease.db, group_id, raft_mod.FeatureReads.init(requester), constraint_name, encoded_value, encoded_point, consistency);
        }
    }

    const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, replica_root_dir, group_id);
    defer alloc.free(path);
    const identity_namespace = try loadTableIdentityNamespaceForGroup(alloc, catalog, table_name, group_id);
    if (cache) |cached| {
        if (cached.getIfPresent(group_id, lsm_root_generation, identity_namespace, table_name)) |db_lease_value| {
            var db_lease = db_lease_value;
            defer db_lease.release();
            return try lookupRelationalTemporalUniqueOwnerInDb(alloc, db_lease.db, group_id, raft_mod.FeatureReads.init(requester), constraint_name, encoded_value, encoded_point, consistency);
        }
    }

    var db = try openProvisionedLookupDbForTable(
        alloc,
        path,
        if (cache) |cached| cached.lsm_cache else null,
        lsm_root_generation,
        if (cache) |cached| cached.resource_manager else null,
        backend_runtime,
        identity_namespace,
    );
    defer db.close();
    return try lookupRelationalTemporalUniqueOwnerInDb(alloc, &db, group_id, raft_mod.FeatureReads.init(requester), constraint_name, encoded_value, encoded_point, consistency);
}

fn lookupRelationalTemporalUniqueOverlapOwnerProvisionedLocal(
    primary_lookup_db: ?PrimaryLookupDbSource,
    cache: ?*ProvisionedTableReadCache,
    replica_root_dir: []const u8,
    catalog: table_catalog.CatalogSource,
    requester: raft_mod.ReadableLeaseRequester,
    alloc: std.mem.Allocator,
    group_id: u64,
    lsm_root_generation: u64,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
    table_name: []const u8,
    constraint_name: []const u8,
    encoded_value: []const u8,
    encoded_start: []const u8,
    encoded_end: []const u8,
    consistency: raft_mod.ReadConsistency,
) !?[]u8 {
    if (primary_lookup_db) |source| {
        if (try source.leaseGroup(alloc, table_name, group_id, lsm_root_generation)) |lease_value| {
            var lease = lease_value;
            defer lease.release(alloc);
            try validateProvisionedDbIdentityNamespace(alloc, catalog, table_name, group_id, lease.db);
            return try lookupRelationalTemporalUniqueOverlapOwnerInDb(alloc, lease.db, group_id, raft_mod.FeatureReads.init(requester), constraint_name, encoded_value, encoded_start, encoded_end, consistency);
        }
    }

    const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, replica_root_dir, group_id);
    defer alloc.free(path);
    const identity_namespace = try loadTableIdentityNamespaceForGroup(alloc, catalog, table_name, group_id);
    if (cache) |cached| {
        if (cached.getIfPresent(group_id, lsm_root_generation, identity_namespace, table_name)) |db_lease_value| {
            var db_lease = db_lease_value;
            defer db_lease.release();
            return try lookupRelationalTemporalUniqueOverlapOwnerInDb(alloc, db_lease.db, group_id, raft_mod.FeatureReads.init(requester), constraint_name, encoded_value, encoded_start, encoded_end, consistency);
        }
    }

    var db = try openProvisionedLookupDbForTable(
        alloc,
        path,
        if (cache) |cached| cached.lsm_cache else null,
        lsm_root_generation,
        if (cache) |cached| cached.resource_manager else null,
        backend_runtime,
        identity_namespace,
    );
    defer db.close();
    return try lookupRelationalTemporalUniqueOverlapOwnerInDb(alloc, &db, group_id, raft_mod.FeatureReads.init(requester), constraint_name, encoded_value, encoded_start, encoded_end, consistency);
}

fn lookupRelationalTemporalUniqueOwnerProvisionedHostedLocal(
    primary_lookup_db: ?PrimaryLookupDbSource,
    cache: ?*ProvisionedTableReadCache,
    replica_root_dir: []const u8,
    catalog: table_catalog.CatalogSource,
    requester: raft_mod.ReadableLeaseRequester,
    alloc: std.mem.Allocator,
    group_id: u64,
    lsm_root_generation: u64,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
    table_name: []const u8,
    constraint_name: []const u8,
    encoded_value: []const u8,
    encoded_point: []const u8,
    consistency: raft_mod.ReadConsistency,
) !?[]u8 {
    return lookupRelationalTemporalUniqueOwnerProvisionedLocal(primary_lookup_db, cache, replica_root_dir, catalog, requester, alloc, group_id, lsm_root_generation, backend_runtime, table_name, constraint_name, encoded_value, encoded_point, consistency) catch |err| switch (err) {
        error.NotLeader => if (consistency == .stale) err else try lookupRelationalTemporalUniqueOwnerProvisionedLocal(primary_lookup_db, cache, replica_root_dir, catalog, requester, alloc, group_id, lsm_root_generation, backend_runtime, table_name, constraint_name, encoded_value, encoded_point, .stale),
        else => err,
    };
}

fn lookupRelationalTemporalUniqueOverlapOwnerProvisionedHostedLocal(
    primary_lookup_db: ?PrimaryLookupDbSource,
    cache: ?*ProvisionedTableReadCache,
    replica_root_dir: []const u8,
    catalog: table_catalog.CatalogSource,
    requester: raft_mod.ReadableLeaseRequester,
    alloc: std.mem.Allocator,
    group_id: u64,
    lsm_root_generation: u64,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
    table_name: []const u8,
    constraint_name: []const u8,
    encoded_value: []const u8,
    encoded_start: []const u8,
    encoded_end: []const u8,
    consistency: raft_mod.ReadConsistency,
) !?[]u8 {
    return lookupRelationalTemporalUniqueOverlapOwnerProvisionedLocal(primary_lookup_db, cache, replica_root_dir, catalog, requester, alloc, group_id, lsm_root_generation, backend_runtime, table_name, constraint_name, encoded_value, encoded_start, encoded_end, consistency) catch |err| switch (err) {
        error.NotLeader => if (consistency == .stale) err else try lookupRelationalTemporalUniqueOverlapOwnerProvisionedLocal(primary_lookup_db, cache, replica_root_dir, catalog, requester, alloc, group_id, lsm_root_generation, backend_runtime, table_name, constraint_name, encoded_value, encoded_start, encoded_end, .stale),
        else => err,
    };
}

fn lookupRelationalUniqueOwnerProvisionedHostedLocal(
    primary_lookup_db: ?PrimaryLookupDbSource,
    cache: ?*ProvisionedTableReadCache,
    replica_root_dir: []const u8,
    catalog: table_catalog.CatalogSource,
    requester: raft_mod.ReadableLeaseRequester,
    alloc: std.mem.Allocator,
    group_id: u64,
    lsm_root_generation: u64,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
    table_name: []const u8,
    constraint_name: []const u8,
    encoded_value: []const u8,
    consistency: raft_mod.ReadConsistency,
) !?[]u8 {
    return lookupRelationalUniqueOwnerProvisionedLocal(primary_lookup_db, cache, replica_root_dir, catalog, requester, alloc, group_id, lsm_root_generation, backend_runtime, table_name, constraint_name, encoded_value, consistency) catch |err| switch (err) {
        error.NotLeader => if (consistency == .stale) err else try lookupRelationalUniqueOwnerProvisionedLocal(primary_lookup_db, cache, replica_root_dir, catalog, requester, alloc, group_id, lsm_root_generation, backend_runtime, table_name, constraint_name, encoded_value, .stale),
        else => err,
    };
}

fn lookupProvisionedLocal(
    primary_lookup_db: ?PrimaryLookupDbSource,
    cache: ?*ProvisionedTableReadCache,
    replica_root_dir: []const u8,
    catalog: table_catalog.CatalogSource,
    requester: raft_mod.ReadableLeaseRequester,
    alloc: std.mem.Allocator,
    group_id: u64,
    lsm_root_generation: u64,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
    table_name: []const u8,
    key: []const u8,
    opts: db_mod.types.LookupOptions,
    consistency: raft_mod.ReadConsistency,
) !?LookupResponse {
    if (primary_lookup_db) |source| {
        if (try source.leaseGroup(alloc, table_name, group_id, lsm_root_generation)) |lease_value| {
            var lease = lease_value;
            defer lease.release(alloc);
            try validateProvisionedDbIdentityNamespace(alloc, catalog, table_name, group_id, lease.db);

            var reads = raft_mod.FeatureDBReads.init(group_id, requester);
            var result = (try reads.lookupWithConsistency(alloc, lease.db, key, opts, consistency)) orelse return null;
            defer result.deinit(alloc);
            return .{
                .json = try alloc.dupe(u8, result.json),
                .version = try lease.db.getTimestamp(alloc, key),
            };
        }
    }

    const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, replica_root_dir, group_id);
    defer alloc.free(path);
    if (cache) |cached| {
        const identity_namespace = try loadTableIdentityNamespaceForGroup(alloc, catalog, table_name, group_id);
        if (cached.getIfPresent(group_id, lsm_root_generation, identity_namespace, table_name)) |db_lease_value| {
            var db_lease = db_lease_value;
            defer db_lease.release();
            const db = db_lease.db;

            var reads = raft_mod.FeatureDBReads.init(group_id, requester);
            var result = (try reads.lookupWithConsistency(alloc, db, key, opts, consistency)) orelse return null;
            defer result.deinit(alloc);
            return .{
                .json = try alloc.dupe(u8, result.json),
                .version = try db.getTimestamp(alloc, key),
            };
        }
    }

    var db = try openProvisionedLookupDbForTable(
        alloc,
        path,
        if (cache) |cached| cached.lsm_cache else null,
        lsm_root_generation,
        if (cache) |cached| cached.resource_manager else null,
        backend_runtime,
        try loadTableIdentityNamespaceForGroup(alloc, catalog, table_name, group_id),
    );
    defer db.close();

    var reads = raft_mod.FeatureDBReads.init(group_id, requester);
    var result = (try reads.lookupWithConsistency(alloc, &db, key, opts, consistency)) orelse return null;
    defer result.deinit(alloc);
    return .{
        .json = try alloc.dupe(u8, result.json),
        .version = try db.getTimestamp(alloc, key),
    };
}

fn lookupHostedLocal(
    replica_root_dir: []const u8,
    requester: raft_mod.ReadableLeaseRequester,
    alloc: std.mem.Allocator,
    group_id: u64,
    key: []const u8,
    opts: db_mod.types.LookupOptions,
    consistency: raft_mod.ReadConsistency,
) !?LookupResponse {
    return lookupLocal(replica_root_dir, requester, alloc, group_id, key, opts, consistency) catch |err| switch (err) {
        error.NotLeader => if (consistency == .stale) err else try lookupLocal(replica_root_dir, requester, alloc, group_id, key, opts, .stale),
        else => err,
    };
}

fn lookupProvisionedHostedLocal(
    primary_lookup_db: ?PrimaryLookupDbSource,
    cache: ?*ProvisionedTableReadCache,
    replica_root_dir: []const u8,
    catalog: table_catalog.CatalogSource,
    requester: raft_mod.ReadableLeaseRequester,
    alloc: std.mem.Allocator,
    group_id: u64,
    lsm_root_generation: u64,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
    table_name: []const u8,
    key: []const u8,
    opts: db_mod.types.LookupOptions,
    consistency: raft_mod.ReadConsistency,
) !?LookupResponse {
    return lookupProvisionedLocal(primary_lookup_db, cache, replica_root_dir, catalog, requester, alloc, group_id, lsm_root_generation, backend_runtime, table_name, key, opts, consistency) catch |err| switch (err) {
        error.NotLeader => if (consistency == .stale) err else try lookupProvisionedLocal(primary_lookup_db, cache, replica_root_dir, catalog, requester, alloc, group_id, lsm_root_generation, backend_runtime, table_name, key, opts, .stale),
        else => err,
    };
}

fn documentArtifactManifestProvisionedLocal(
    cache: ?*ProvisionedTableReadCache,
    replica_root_dir: []const u8,
    catalog: table_catalog.CatalogSource,
    requester: raft_mod.ReadableLeaseRequester,
    alloc: std.mem.Allocator,
    group_id: u64,
    lsm_root_generation: u64,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
    table_name: []const u8,
    doc_key: []const u8,
    artifact_name: []const u8,
    consistency: raft_mod.ReadConsistency,
) !?db_mod.types.DocumentArtifactManifest {
    const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, replica_root_dir, group_id);
    defer alloc.free(path);
    if (cache) |cached| {
        var db_lease = try cached.getOrOpen(path, catalog, group_id, lsm_root_generation, table_name);
        defer db_lease.release();
        var reads = raft_mod.FeatureDBReads.init(group_id, requester);
        return try reads.documentArtifactManifestWithConsistency(alloc, db_lease.db, doc_key, artifact_name, consistency);
    }

    var db = try openProvisionedQueryDbForTableWithRuntime(alloc, path, catalog, table_name, group_id, lsm_root_generation, backend_runtime);
    defer db.close();
    var reads = raft_mod.FeatureDBReads.init(group_id, requester);
    return try reads.documentArtifactManifestWithConsistency(alloc, &db, doc_key, artifact_name, consistency);
}

fn documentArtifactManifestProvisionedHostedLocal(
    cache: ?*ProvisionedTableReadCache,
    replica_root_dir: []const u8,
    catalog: table_catalog.CatalogSource,
    requester: raft_mod.ReadableLeaseRequester,
    alloc: std.mem.Allocator,
    group_id: u64,
    lsm_root_generation: u64,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
    table_name: []const u8,
    doc_key: []const u8,
    artifact_name: []const u8,
    consistency: raft_mod.ReadConsistency,
) !?db_mod.types.DocumentArtifactManifest {
    return documentArtifactManifestProvisionedLocal(cache, replica_root_dir, catalog, requester, alloc, group_id, lsm_root_generation, backend_runtime, table_name, doc_key, artifact_name, consistency) catch |err| switch (err) {
        error.NotLeader => if (consistency == .stale) err else try documentArtifactManifestProvisionedLocal(cache, replica_root_dir, catalog, requester, alloc, group_id, lsm_root_generation, backend_runtime, table_name, doc_key, artifact_name, .stale),
        else => err,
    };
}

fn documentArtifactManifestsProvisionedLocal(
    cache: ?*ProvisionedTableReadCache,
    replica_root_dir: []const u8,
    catalog: table_catalog.CatalogSource,
    requester: raft_mod.ReadableLeaseRequester,
    alloc: std.mem.Allocator,
    group_id: u64,
    lsm_root_generation: u64,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
    table_name: []const u8,
    doc_key: []const u8,
    consistency: raft_mod.ReadConsistency,
) !?db_mod.types.DocumentArtifactManifestList {
    const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, replica_root_dir, group_id);
    defer alloc.free(path);
    if (cache) |cached| {
        var db_lease = try cached.getOrOpen(path, catalog, group_id, lsm_root_generation, table_name);
        defer db_lease.release();
        var reads = raft_mod.FeatureDBReads.init(group_id, requester);
        return try reads.documentArtifactManifestsWithConsistency(alloc, db_lease.db, doc_key, consistency);
    }

    var db = try openProvisionedQueryDbForTableWithRuntime(alloc, path, catalog, table_name, group_id, lsm_root_generation, backend_runtime);
    defer db.close();
    var reads = raft_mod.FeatureDBReads.init(group_id, requester);
    return try reads.documentArtifactManifestsWithConsistency(alloc, &db, doc_key, consistency);
}

fn documentArtifactManifestsProvisionedHostedLocal(
    cache: ?*ProvisionedTableReadCache,
    replica_root_dir: []const u8,
    catalog: table_catalog.CatalogSource,
    requester: raft_mod.ReadableLeaseRequester,
    alloc: std.mem.Allocator,
    group_id: u64,
    lsm_root_generation: u64,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
    table_name: []const u8,
    doc_key: []const u8,
    consistency: raft_mod.ReadConsistency,
) !?db_mod.types.DocumentArtifactManifestList {
    return documentArtifactManifestsProvisionedLocal(cache, replica_root_dir, catalog, requester, alloc, group_id, lsm_root_generation, backend_runtime, table_name, doc_key, consistency) catch |err| switch (err) {
        error.NotLeader => if (consistency == .stale) err else try documentArtifactManifestsProvisionedLocal(cache, replica_root_dir, catalog, requester, alloc, group_id, lsm_root_generation, backend_runtime, table_name, doc_key, .stale),
        else => err,
    };
}

fn scanLocal(
    replica_root_dir: []const u8,
    requester: raft_mod.ReadableLeaseRequester,
    alloc: std.mem.Allocator,
    group_id: u64,
    from_key: []const u8,
    to_key: []const u8,
    opts: db_mod.types.ScanOptions,
    consistency: raft_mod.ReadConsistency,
) !?ScanResponse {
    const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, replica_root_dir, group_id);
    defer alloc.free(path);
    var db = try db_mod.DB.open(alloc, path, .{});
    defer db.close();

    var reads = raft_mod.FeatureDBReads.init(group_id, requester);
    var result = try reads.scanWithConsistency(alloc, &db, from_key, to_key, opts, consistency);
    defer result.deinit(alloc);

    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    for (result.hashes, 0..) |entry, i| {
        const json = if (opts.include_documents) result.documents[i].json else null;
        try appendScanLine(alloc, &out, entry.id, json);
    }
    return .{ .ndjson = try out.toOwnedSlice(alloc) };
}

fn scanProvisionedLocal(
    cache: ?*ProvisionedTableReadCache,
    replica_root_dir: []const u8,
    catalog: table_catalog.CatalogSource,
    requester: raft_mod.ReadableLeaseRequester,
    alloc: std.mem.Allocator,
    group_id: u64,
    lsm_root_generation: u64,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
    table_name: []const u8,
    from_key: []const u8,
    to_key: []const u8,
    opts: db_mod.types.ScanOptions,
    consistency: raft_mod.ReadConsistency,
) !?ScanResponse {
    const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, replica_root_dir, group_id);
    defer alloc.free(path);
    if (cache) |cached| {
        var db_lease = try cached.getOrOpen(path, catalog, group_id, lsm_root_generation, table_name);
        defer db_lease.release();
        const db = db_lease.db;

        var reads = raft_mod.FeatureDBReads.init(group_id, requester);
        var result = try reads.scanWithConsistency(alloc, db, from_key, to_key, opts, consistency);
        defer result.deinit(alloc);

        var out = std.ArrayListUnmanaged(u8).empty;
        defer out.deinit(alloc);
        for (result.hashes, 0..) |entry, i| {
            const json = if (opts.include_documents) result.documents[i].json else null;
            try appendScanLine(alloc, &out, entry.id, json);
        }
        return .{ .ndjson = try out.toOwnedSlice(alloc) };
    } else {
        var db = try openProvisionedQueryDbForTableWithRuntime(alloc, path, catalog, table_name, group_id, lsm_root_generation, backend_runtime);
        defer db.close();

        var reads = raft_mod.FeatureDBReads.init(group_id, requester);
        var result = try reads.scanWithConsistency(alloc, &db, from_key, to_key, opts, consistency);
        defer result.deinit(alloc);

        var out = std.ArrayListUnmanaged(u8).empty;
        defer out.deinit(alloc);
        for (result.hashes, 0..) |entry, i| {
            const json = if (opts.include_documents) result.documents[i].json else null;
            try appendScanLine(alloc, &out, entry.id, json);
        }
        return .{ .ndjson = try out.toOwnedSlice(alloc) };
    }
}

fn scanHostedLocal(
    replica_root_dir: []const u8,
    requester: raft_mod.ReadableLeaseRequester,
    alloc: std.mem.Allocator,
    group_id: u64,
    from_key: []const u8,
    to_key: []const u8,
    opts: db_mod.types.ScanOptions,
    consistency: raft_mod.ReadConsistency,
) !?ScanResponse {
    return scanLocal(replica_root_dir, requester, alloc, group_id, from_key, to_key, opts, consistency) catch |err| switch (err) {
        error.NotLeader => if (consistency == .stale) err else try scanLocal(replica_root_dir, requester, alloc, group_id, from_key, to_key, opts, .stale),
        else => err,
    };
}

fn scanProvisionedHostedLocal(
    cache: ?*ProvisionedTableReadCache,
    replica_root_dir: []const u8,
    catalog: table_catalog.CatalogSource,
    requester: raft_mod.ReadableLeaseRequester,
    alloc: std.mem.Allocator,
    group_id: u64,
    lsm_root_generation: u64,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
    table_name: []const u8,
    from_key: []const u8,
    to_key: []const u8,
    opts: db_mod.types.ScanOptions,
    consistency: raft_mod.ReadConsistency,
) !?ScanResponse {
    return scanProvisionedLocal(cache, replica_root_dir, catalog, requester, alloc, group_id, lsm_root_generation, backend_runtime, table_name, from_key, to_key, opts, consistency) catch |err| switch (err) {
        error.NotLeader => if (consistency == .stale) err else try scanProvisionedLocal(cache, replica_root_dir, catalog, requester, alloc, group_id, lsm_root_generation, backend_runtime, table_name, from_key, to_key, opts, .stale),
        else => err,
    };
}

fn queryLocal(
    cache: ?*ProvisionedTableReadCache,
    replica_root_dir: []const u8,
    catalog: table_catalog.CatalogSource,
    requester: raft_mod.ReadableLeaseRequester,
    alloc: std.mem.Allocator,
    group_id: u64,
    lsm_root_generation: u64,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
    antfly_provider: ?managed_embedder.AntflyProvider,
    secret_store: ?*common_secrets.FileStore,
    remote_content: ?*const scraping.RemoteContentConfig,
    table_name: []const u8,
    req: db_mod.types.SearchRequest,
    consistency: raft_mod.ReadConsistency,
) !db_mod.types.SearchResult {
    const detailed = try queryLocalDetailed(cache, replica_root_dir, catalog, requester, alloc, group_id, lsm_root_generation, backend_runtime, antfly_provider, secret_store, remote_content, table_name, req, consistency);
    var result = detailed.result;
    result.identity_read_generation = detailed.request.identity_read_generation;
    return result;
}

fn mapDenseSearchProfile(profile: db_query_search.DenseSearchProfile) query_api.QueryResponseMeta.DenseSearchProfile {
    return .{
        .total_ns = profile.total_ns,
        .index_lookup_ns = profile.index_lookup_ns,
        .hbc_search_ns = profile.hbc_search_ns,
        .hbc_runtime_txn_ns = profile.hbc_runtime_txn_ns,
        .hbc_scratch_acquire_ns = profile.hbc_scratch_acquire_ns,
        .hbc_node_cache_lookup_ns = profile.hbc_node_cache_lookup_ns,
        .hbc_quantized_cache_lookup_ns = profile.hbc_quantized_cache_lookup_ns,
        .resolved_search_width = profile.resolved_search_width,
        .resolved_epsilon = profile.resolved_epsilon,
        .hbc_nodes_visited = profile.hbc_nodes_visited,
        .hbc_leaves_explored = profile.hbc_leaves_explored,
        .hbc_approx_vectors_scored = profile.hbc_approx_vectors_scored,
        .hbc_exact_vectors_scored = profile.hbc_exact_vectors_scored,
        .hbc_reranked_vectors = profile.hbc_reranked_vectors,
        .hbc_approx_candidate_count = profile.hbc_approx_candidate_count,
        .hbc_rerank_candidate_count = profile.hbc_rerank_candidate_count,
        .hbc_ambiguous_top_k_pairs = profile.hbc_ambiguous_top_k_pairs,
        .hbc_ambiguous_boundary_pairs = profile.hbc_ambiguous_boundary_pairs,
        .hbc_ambiguous_distance_over_hits = profile.hbc_ambiguous_distance_over_hits,
        .hbc_ambiguous_distance_under_hits = profile.hbc_ambiguous_distance_under_hits,
        .hbc_full_rerank_due_to_threshold = profile.hbc_full_rerank_due_to_threshold,
        .hbc_top_k_count = profile.hbc_top_k_count,
        .hbc_min_distance_gap_top_k = profile.hbc_min_distance_gap_top_k,
        .hbc_min_interval_gap_top_k = profile.hbc_min_interval_gap_top_k,
        .hbc_closest_pair_top_k = if (profile.hbc_closest_pair_top_k) |pair| mapDenseDebugPair(pair) else null,
        .hbc_boundary_pair = if (profile.hbc_boundary_pair) |pair| mapDenseDebugPair(pair) else null,
        .hbc_boundary_tail_error_avg = profile.hbc_boundary_tail_error_avg,
        .hbc_boundary_tail_error_max = profile.hbc_boundary_tail_error_max,
        .hbc_boundary_tail_distance_gap_avg = profile.hbc_boundary_tail_distance_gap_avg,
        .hbc_boundary_tail_distance_gap_min = profile.hbc_boundary_tail_distance_gap_min,
        .hbc_boundary_tail_distance_gap_max = profile.hbc_boundary_tail_distance_gap_max,
        .hbc_boundary_tail_interval_gap_avg = profile.hbc_boundary_tail_interval_gap_avg,
        .hbc_boundary_tail_interval_gap_min = profile.hbc_boundary_tail_interval_gap_min,
        .hbc_boundary_tail_interval_gap_max = profile.hbc_boundary_tail_interval_gap_max,
        .hbc_approx_top_count = profile.hbc_approx_top_count,
        .hbc_approx_top = blk: {
            var out: [5]query_api.QueryResponseMeta.DenseSearchProfile.DebugHit = .{ .{}, .{}, .{}, .{}, .{} };
            for (profile.hbc_approx_top, 0..) |hit, i| out[i] = mapDenseDebugHit(hit);
            break :blk out;
        },
        .hbc_rerank_external_score_ns = profile.hbc_rerank_external_score_ns,
        .hbc_rerank_vector_load_ns = profile.hbc_rerank_vector_load_ns,
        .hbc_rerank_metadata_lookup_ns = profile.hbc_rerank_metadata_lookup_ns,
        .hbc_rerank_artifact_key_ns = profile.hbc_rerank_artifact_key_ns,
        .hbc_rerank_artifact_read_ns = profile.hbc_rerank_artifact_read_ns,
        .hbc_rerank_artifact_decode_ns = profile.hbc_rerank_artifact_decode_ns,
        .hbc_rerank_artifact_distance_ns = profile.hbc_rerank_artifact_distance_ns,
        .hbc_rerank_lsm_cache_hits = profile.hbc_rerank_lsm_cache_hits,
        .hbc_rerank_lsm_cache_misses = profile.hbc_rerank_lsm_cache_misses,
        .hbc_rerank_distance_ns = profile.hbc_rerank_distance_ns,
        .doc_key_resolve_ns = profile.doc_key_resolve_ns,
        .doc_ordinal_lookup_ns = profile.doc_ordinal_lookup_ns,
        .load_projected_document_ns = profile.load_projected_document_ns,
        .postprocess_ns = profile.postprocess_ns,
        .raw_hit_count = profile.raw_hit_count,
        .returned_hit_count = profile.returned_hit_count,
        .inline_metadata_hits = profile.inline_metadata_hits,
        .fetched_metadata_hits = profile.fetched_metadata_hits,
        .lookup_doc_key_hits = profile.lookup_doc_key_hits,
    };
}

fn mapDenseDebugHit(hit: db_query_search.DenseSearchProfile.DebugHit) query_api.QueryResponseMeta.DenseSearchProfile.DebugHit {
    return .{
        .id = hit.id,
        .distance = hit.distance,
        .error_bound = hit.error_bound,
        .lower_bound = hit.lower_bound,
        .upper_bound = hit.upper_bound,
    };
}

fn mapDenseDebugPair(pair: db_query_search.DenseSearchProfile.DebugPair) query_api.QueryResponseMeta.DenseSearchProfile.DebugPair {
    return .{
        .left = mapDenseDebugHit(pair.left),
        .right = mapDenseDebugHit(pair.right),
        .distance_gap = pair.distance_gap,
        .interval_gap = pair.interval_gap,
        .overlaps = pair.overlaps,
    };
}

fn profiledDenseQuery(req: db_mod.types.SearchRequest) ?ProfiledDenseQuery {
    if (!req.profile) return null;
    if (req.full_text != null or req.full_text_queries.len > 0) return null;
    if (req.sparse != null or req.sparse_queries.len > 0) return null;
    if (req.graph_queries.len > 0) return null;
    if (req.dense_queries.len > 1) return null;
    if (req.merge_config != null) return null;
    if (req.reranker != null) return null;
    if (req.dense_queries.len == 1) {
        var dense_req = req;
        dense_req.index_name = req.dense_queries[0].index_name;
        return .{
            .req = dense_req,
            .query = req.dense_queries[0].query,
        };
    }
    return if (req.dense) |dense|
        .{ .req = req, .query = dense }
    else switch (req.query) {
        .dense_knn => |dense| .{ .req = req, .query = dense },
        else => null,
    };
}

fn readPreparationKindForQuery(req: db_mod.types.SearchRequest) ReadPreparation.Kind {
    return if (isDenseOnlyQuery(req)) .dense_query else .general;
}

fn isDenseOnlyQuery(req: db_mod.types.SearchRequest) bool {
    if (req.full_text != null or req.full_text_queries.len > 0) return false;
    if (req.sparse != null or req.sparse_queries.len > 0) return false;
    if (req.graph_queries.len > 0) return false;
    if (req.filter_query_json.len > 0 or req.exclusion_query_json.len > 0) return false;

    const query_is_dense_or_neutral = switch (req.query) {
        .match_all, .dense_knn => true,
        else => false,
    };
    if (!query_is_dense_or_neutral) return false;

    return req.dense != null or req.dense_queries.len > 0 or switch (req.query) {
        .dense_knn => true,
        else => false,
    };
}

fn queryLocalDetailed(
    cache: ?*ProvisionedTableReadCache,
    replica_root_dir: []const u8,
    catalog: table_catalog.CatalogSource,
    requester: raft_mod.ReadableLeaseRequester,
    alloc: std.mem.Allocator,
    group_id: u64,
    lsm_root_generation: u64,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
    antfly_provider: ?managed_embedder.AntflyProvider,
    secret_store: ?*common_secrets.FileStore,
    remote_content: ?*const scraping.RemoteContentConfig,
    table_name: []const u8,
    req: db_mod.types.SearchRequest,
    consistency: raft_mod.ReadConsistency,
) !LocalQueryExecution {
    const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, replica_root_dir, group_id);
    defer alloc.free(path);
    if (cache) |cached| {
        var db_lease = try cached.getOrOpen(path, catalog, group_id, lsm_root_generation, table_name);
        defer db_lease.release();
        const db = db_lease.db;

        var reads = raft_mod.FeatureDBReads.init(group_id, requester);
        try reads.reads.prepareSearchWithConsistency(group_id, req, consistency);
        const snapshot_req = try db.searchRequestAtCurrentIdentityGeneration(req);
        if (profiledDenseQuery(snapshot_req)) |dense| {
            const profiled = try db.searchDenseProfiled(alloc, dense.req, dense.query);
            return .{
                .request = snapshot_req,
                .result = profiled.result,
                .dense_profile = mapDenseSearchProfile(profiled.profile),
            };
        }
        return .{
            .request = snapshot_req,
            .result = try db.search(alloc, snapshot_req),
        };
    } else {
        const identity_namespace = try loadTableIdentityNamespaceForGroup(alloc, catalog, table_name, group_id);
        var db = try openProvisionedQueryDbForTableWithCache(alloc, path, catalog, table_name, null, null, lsm_root_generation, null, backend_runtime, antfly_provider, secret_store, remote_content, identity_namespace);
        defer db.close();

        var reads = raft_mod.FeatureDBReads.init(group_id, requester);
        try reads.reads.prepareSearchWithConsistency(group_id, req, consistency);
        const snapshot_req = try db.searchRequestAtCurrentIdentityGeneration(req);
        if (profiledDenseQuery(snapshot_req)) |dense| {
            const profiled = try db.searchDenseProfiled(alloc, dense.req, dense.query);
            return .{
                .request = snapshot_req,
                .result = profiled.result,
                .dense_profile = mapDenseSearchProfile(profiled.profile),
            };
        }
        return .{
            .request = snapshot_req,
            .result = try db.search(alloc, snapshot_req),
        };
    }
}

fn preflightHostedLocal(
    cache: ?*ProvisionedTableReadCache,
    replica_root_dir: []const u8,
    catalog: table_catalog.CatalogSource,
    requester: raft_mod.ReadableLeaseRequester,
    alloc: std.mem.Allocator,
    group_id: u64,
    lsm_root_generation: u64,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
    table_name: []const u8,
    req: db_mod.types.SearchRequest,
    consistency: raft_mod.ReadConsistency,
    max_work: u32,
) !db_mod.RuntimePreflightSummary {
    const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, replica_root_dir, group_id);
    defer alloc.free(path);
    if (cache) |cached| {
        var db_lease = try cached.getOrOpen(path, catalog, group_id, lsm_root_generation, table_name);
        defer db_lease.release();
        const db = db_lease.db;
        var reads = raft_mod.FeatureDBReads.init(group_id, requester);
        try reads.reads.prepareSearchWithConsistency(group_id, req, consistency);
        var summary = try db.preflightSearchRequest(alloc, req, max_work);
        annotateVectorWorkerPreflight(alloc, &summary, req);
        return summary;
    }

    var db = try openProvisionedQueryDbForTableWithRuntime(alloc, path, catalog, table_name, group_id, lsm_root_generation, backend_runtime);
    defer db.close();
    var reads = raft_mod.FeatureDBReads.init(group_id, requester);
    try reads.reads.prepareSearchWithConsistency(group_id, req, consistency);
    var summary = try db.preflightSearchRequest(alloc, req, max_work);
    annotateVectorWorkerPreflight(alloc, &summary, req);
    return summary;
}

fn preflightProvisionedGroups(
    self: *ProvisionedTableReadSource,
    alloc: std.mem.Allocator,
    group_ids: []const u64,
    table_name: []const u8,
    req: db_mod.types.SearchRequest,
    consistency: raft_mod.ReadConsistency,
    max_work: u32,
) !?db_mod.RuntimePreflightSummary {
    try tableReadsValidateDocIdentityReadyForMultiGroup(alloc, self.catalog, table_name, group_ids.len);
    var first_summary: ?db_mod.RuntimePreflightSummary = null;
    errdefer if (first_summary) |*summary| summary.deinit(alloc);
    for (group_ids) |group_id| {
        const summary = try preflightHostedLocal(
            self.cache,
            self.replica_root_dir,
            self.catalog,
            self.requester,
            alloc,
            group_id,
            self.visibleRootGeneration(group_id),
            self.backend_runtime,
            table_name,
            req,
            consistency,
            max_work,
        );
        if (first_summary == null) {
            first_summary = summary;
        } else {
            try mergeRuntimePreflightSummary(alloc, &first_summary.?, summary);
        }
    }
    return first_summary;
}

fn queryHostedLocal(
    cache: ?*ProvisionedTableReadCache,
    replica_root_dir: []const u8,
    catalog: table_catalog.CatalogSource,
    requester: raft_mod.ReadableLeaseRequester,
    alloc: std.mem.Allocator,
    group_id: u64,
    lsm_root_generation: u64,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
    antfly_provider: ?managed_embedder.AntflyProvider,
    secret_store: ?*common_secrets.FileStore,
    remote_content: ?*const scraping.RemoteContentConfig,
    table_name: []const u8,
    req: db_mod.types.SearchRequest,
    consistency: raft_mod.ReadConsistency,
) !db_mod.types.SearchResult {
    const detailed = try queryHostedLocalDetailed(cache, replica_root_dir, catalog, requester, alloc, group_id, lsm_root_generation, backend_runtime, antfly_provider, secret_store, remote_content, table_name, req, consistency);
    return detailed.result;
}

fn queryHostedLocalDetailed(
    cache: ?*ProvisionedTableReadCache,
    replica_root_dir: []const u8,
    catalog: table_catalog.CatalogSource,
    requester: raft_mod.ReadableLeaseRequester,
    alloc: std.mem.Allocator,
    group_id: u64,
    lsm_root_generation: u64,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
    antfly_provider: ?managed_embedder.AntflyProvider,
    secret_store: ?*common_secrets.FileStore,
    remote_content: ?*const scraping.RemoteContentConfig,
    table_name: []const u8,
    req: db_mod.types.SearchRequest,
    consistency: raft_mod.ReadConsistency,
) !LocalQueryExecution {
    return queryLocalDetailed(cache, replica_root_dir, catalog, requester, alloc, group_id, lsm_root_generation, backend_runtime, antfly_provider, secret_store, remote_content, table_name, req, consistency) catch |err| switch (err) {
        error.NotLeader => if (consistency == .stale) err else try queryLocalDetailed(cache, replica_root_dir, catalog, requester, alloc, group_id, lsm_root_generation, backend_runtime, antfly_provider, secret_store, remote_content, table_name, req, .stale),
        else => err,
    };
}

fn openProvisionedWarmStatusDbForTable(
    alloc: std.mem.Allocator,
    path: []const u8,
    lsm_root_generation: u64,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
    identity_namespace: ?db_mod.DocIdentityNamespace,
) !db_mod.DB {
    var db = try db_mod.DB.open(alloc, path, .{
        .open_mode = .status_only,
        .lsm_root_generation = lsm_root_generation,
        .backend_runtime = backend_runtime,
        .identity_namespace = identity_namespace,
        .prefer_existing_identity_namespace = identity_namespace != null,
    });
    errdefer db.close();
    try validateOpenedProvisionedDbIdentityNamespace(&db, identity_namespace);
    return db;
}

fn openProvisionedLookupDbForTable(
    alloc: std.mem.Allocator,
    path: []const u8,
    lsm_cache: ?*lsm_backend.Cache,
    lsm_root_generation: u64,
    resource_manager: ?*resource_manager_mod.ResourceManager,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
    identity_namespace: ?db_mod.DocIdentityNamespace,
) !db_mod.DB {
    var db = try db_mod.DB.open(alloc, path, .{
        .open_mode = .status_only,
        .lsm_cache = lsm_cache,
        .lsm_root_generation = lsm_root_generation,
        .resource_manager = resource_manager,
        .backend_runtime = backend_runtime,
        .identity_namespace = identity_namespace,
        .prefer_existing_identity_namespace = identity_namespace != null,
    });
    errdefer db.close();
    try validateOpenedProvisionedDbIdentityNamespace(&db, identity_namespace);
    return db;
}

fn algebraicConstraintsForRequestAlloc(
    alloc: std.mem.Allocator,
    req: db_mod.types.SearchRequest,
) !?[]db_mod.aggregations.FixedConstraint {
    var out = std.ArrayListUnmanaged(db_mod.aggregations.FixedConstraint).empty;
    errdefer freeAlgebraicConstraints(alloc, out.items);

    switch (req.query) {
        .match_all => {},
        .term => |term| {
            if (!std.mem.startsWith(u8, term.field, "/")) return null;
            const value_text = db_mod.algebraic.token.canonicalTupleAlloc(alloc, &.{ "string", term.term }) catch return null;
            defer alloc.free(value_text);
            appendAlgebraicConstraint(&out, alloc, term.field, value_text) catch return null;
        },
        .match => |match| {
            if (!std.mem.startsWith(u8, match.field, "/") or match.analyzer != null or match.text.len == 0) return null;
            const value_text = db_mod.algebraic.index.pathFactStringMatchConstraintValueAlloc(alloc, match.text) catch return null;
            defer alloc.free(value_text);
            appendAlgebraicConstraint(&out, alloc, match.field, value_text) catch return null;
        },
        .fuzzy => |fuzzy| {
            if (!std.mem.startsWith(u8, fuzzy.field, "/") or fuzzy.auto_fuzzy or fuzzy.prefix_len == 0) return null;
            const prefix_len: usize = @intCast(fuzzy.prefix_len);
            if (fuzzy.term.len < prefix_len) return null;
            const value_text = db_mod.algebraic.index.pathFactStringFuzzyConstraintValueAlloc(
                alloc,
                fuzzy.term,
                fuzzy.max_edits,
                fuzzy.prefix_len,
            ) catch return null;
            defer alloc.free(value_text);
            appendAlgebraicConstraint(&out, alloc, fuzzy.field, value_text) catch return null;
        },
        .prefix => |prefix| {
            if (!std.mem.startsWith(u8, prefix.field, "/")) return null;
            const value_text = db_mod.algebraic.index.pathFactStringPrefixConstraintValueAlloc(alloc, prefix.prefix) catch return null;
            defer alloc.free(value_text);
            appendAlgebraicConstraint(&out, alloc, prefix.field, value_text) catch return null;
        },
        .wildcard => |wildcard| {
            if (!std.mem.startsWith(u8, wildcard.field, "/")) return null;
            const literal_prefix = algebraicWildcardLiteralPrefix(wildcard.pattern);
            if (literal_prefix.len == 0 and algebraicWildcardPatternHasMeta(wildcard.pattern)) return null;
            const value_text = db_mod.algebraic.index.pathFactStringWildcardConstraintValueAlloc(alloc, wildcard.pattern) catch return null;
            defer alloc.free(value_text);
            appendAlgebraicConstraint(&out, alloc, wildcard.field, value_text) catch return null;
        },
        .regexp => |regexp| {
            if (!std.mem.startsWith(u8, regexp.field, "/")) return null;
            if (algebraicRegexpLiteralPrefix(regexp.pattern).len == 0) return null;
            var compiled = regex_mod.compile(alloc, regexp.pattern) catch return null;
            defer compiled.deinit();
            const value_text = db_mod.algebraic.index.pathFactStringRegexpConstraintValueAlloc(alloc, regexp.pattern) catch return null;
            defer alloc.free(value_text);
            appendAlgebraicConstraint(&out, alloc, regexp.field, value_text) catch return null;
        },
        .bool_field => |field| {
            const value_text = algebraicConstraintBoolValueAlloc(alloc, field.field, field.value) catch return null;
            defer alloc.free(value_text);
            appendAlgebraicConstraint(&out, alloc, field.field, value_text) catch return null;
        },
        .numeric_range => |range| {
            if (!std.mem.startsWith(u8, range.field, "/")) return null;
            const value_text = db_mod.algebraic.index.pathFactNumericRangeConstraintValueAlloc(
                alloc,
                range.min,
                range.max,
                range.inclusive_min,
                range.inclusive_max,
            ) catch return null;
            defer alloc.free(value_text);
            appendAlgebraicConstraint(&out, alloc, range.field, value_text) catch return null;
        },
        .term_range => |range| {
            if (!std.mem.startsWith(u8, range.field, "/") or (range.min == null and range.max == null)) return null;
            const value_text = db_mod.algebraic.index.pathFactTermRangeConstraintValueAlloc(
                alloc,
                range.min,
                range.max,
                range.inclusive_min,
                range.inclusive_max,
            ) catch return null;
            defer alloc.free(value_text);
            appendAlgebraicConstraint(&out, alloc, range.field, value_text) catch return null;
        },
        .ip_range => |range| {
            if (!std.mem.startsWith(u8, range.field, "/") or !algebraicValidIpRange(range.cidr)) return null;
            const value_text = db_mod.algebraic.index.pathFactIpRangeConstraintValueAlloc(alloc, range.cidr) catch return null;
            defer alloc.free(value_text);
            appendAlgebraicConstraint(&out, alloc, range.field, value_text) catch return null;
        },
        .geo_bbox => |bbox| {
            if (!std.mem.startsWith(u8, bbox.field, "/") or !algebraicValidGeoBBox(bbox.min_lat, bbox.min_lon, bbox.max_lat, bbox.max_lon)) return null;
            const value_text = db_mod.algebraic.index.pathFactGeoBBoxConstraintValueAlloc(
                alloc,
                bbox.min_lat,
                bbox.min_lon,
                bbox.max_lat,
                bbox.max_lon,
            ) catch return null;
            defer alloc.free(value_text);
            appendAlgebraicConstraint(&out, alloc, bbox.field, value_text) catch return null;
        },
        .geo_distance => |distance| {
            if (!std.mem.startsWith(u8, distance.field, "/") or !algebraicValidGeoDistance(distance.lat, distance.lon, distance.radius_meters)) return null;
            const value_text = db_mod.algebraic.index.pathFactGeoDistanceConstraintValueAlloc(
                alloc,
                distance.lat,
                distance.lon,
                distance.radius_meters,
            ) catch return null;
            defer alloc.free(value_text);
            appendAlgebraicConstraint(&out, alloc, distance.field, value_text) catch return null;
        },
        .geo_shape => |shape| {
            if (!std.mem.startsWith(u8, shape.field, "/") or !algebraicGeoShapeRelationSupported(shape.relation)) return null;
            const value_text = db_mod.algebraic.index.pathFactGeoShapeConstraintValueAlloc(
                alloc,
                @tagName(shape.relation),
                shape.polygons,
            ) catch return null;
            defer alloc.free(value_text);
            appendAlgebraicConstraint(&out, alloc, shape.field, value_text) catch return null;
        },
        .date_range => |range| {
            if (!std.mem.startsWith(u8, range.field, "/")) return null;
            const start_text = if (range.start_ns) |ns| std.fmt.allocPrint(alloc, "{d}", .{ns}) catch return null else null;
            defer if (start_text) |value| alloc.free(value);
            const end_text = if (range.end_ns) |ns| std.fmt.allocPrint(alloc, "{d}", .{ns}) catch return null else null;
            defer if (end_text) |value| alloc.free(value);
            const value_text = db_mod.algebraic.index.pathFactDateRangeConstraintValueAlloc(
                alloc,
                start_text,
                end_text,
                range.inclusive_start,
                range.inclusive_end,
            ) catch return null;
            defer alloc.free(value_text);
            appendAlgebraicConstraint(&out, alloc, range.field, value_text) catch return null;
        },
        else => return null,
    }

    if (req.full_text) |text_query| {
        if (!(collectAlgebraicTextQueryConstraints(alloc, text_query, &out) catch return null)) return null;
    }

    if (req.filter_query_json.len > 0) {
        var parsed = std.json.parseFromSlice(std.json.Value, alloc, req.filter_query_json, .{}) catch return null;
        defer parsed.deinit();
        if (!(collectAlgebraicFilterConstraints(alloc, parsed.value, &out) catch return null)) return null;
    }

    return try out.toOwnedSlice(alloc);
}

fn freeAlgebraicConstraints(
    alloc: std.mem.Allocator,
    constraints: []db_mod.aggregations.FixedConstraint,
) void {
    for (constraints) |constraint| {
        alloc.free(@constCast(constraint.field));
        alloc.free(@constCast(constraint.value));
    }
    if (constraints.len > 0) alloc.free(constraints);
}

const AlgebraicConstraintCollectError = std.mem.Allocator.Error || error{UnsupportedQueryRequest};

fn collectAlgebraicTextBoolQueryConstraints(
    alloc: std.mem.Allocator,
    bool_query: db_mod.types.TextBoolQuery,
    out: *std.ArrayListUnmanaged(db_mod.aggregations.FixedConstraint),
) AlgebraicConstraintCollectError!bool {
    if (bool_query.must_not.len > 0) return false;
    if (bool_query.should.len > 0) {
        if (bool_query.must.len > 0) {
            if (bool_query.min_should != 0) return false;
        } else {
            if (bool_query.min_should != 0 and bool_query.min_should != 1) return false;
            return try collectAlgebraicTextShouldTermConstraint(alloc, bool_query.should, out);
        }
    }
    if (bool_query.min_should > 0) return false;
    for (bool_query.must) |query| {
        if (!(try collectAlgebraicTextQueryConstraints(alloc, query, out))) return false;
    }
    return true;
}

fn collectAlgebraicTextShouldTermConstraint(
    alloc: std.mem.Allocator,
    queries: []const db_mod.types.TextQuery,
    out: *std.ArrayListUnmanaged(db_mod.aggregations.FixedConstraint),
) AlgebraicConstraintCollectError!bool {
    if (queries.len == 0) return false;
    var field: ?[]const u8 = null;
    const typed_values = try alloc.alloc([]const u8, queries.len);
    defer alloc.free(typed_values);
    var initialized: usize = 0;
    defer {
        for (typed_values[0..initialized]) |value| alloc.free(@constCast(value));
    }

    for (queries, 0..) |query, i| {
        const term = switch (query) {
            .term => |term| term,
            else => return false,
        };
        if (!std.mem.startsWith(u8, term.field, "/")) return false;
        if (field) |existing| {
            if (!std.mem.eql(u8, existing, term.field)) return false;
        } else {
            field = term.field;
        }
        typed_values[i] = try db_mod.algebraic.token.canonicalTupleAlloc(alloc, &.{ "string", term.term });
        initialized += 1;
    }

    try appendAlgebraicTypedAnyConstraint(out, alloc, field orelse return false, typed_values);
    return true;
}

fn collectAlgebraicTextQueryConstraints(
    alloc: std.mem.Allocator,
    query: db_mod.types.TextQuery,
    out: *std.ArrayListUnmanaged(db_mod.aggregations.FixedConstraint),
) AlgebraicConstraintCollectError!bool {
    switch (query) {
        .match_all => return true,
        .term => |term| {
            if (!std.mem.startsWith(u8, term.field, "/")) return false;
            const value_text = try db_mod.algebraic.token.canonicalTupleAlloc(alloc, &.{ "string", term.term });
            defer alloc.free(value_text);
            try appendAlgebraicConstraint(out, alloc, term.field, value_text);
            return true;
        },
        .match => |match| {
            if (!std.mem.startsWith(u8, match.field, "/") or match.analyzer != null or match.text.len == 0) return false;
            const value_text = try db_mod.algebraic.index.pathFactStringMatchConstraintValueAlloc(alloc, match.text);
            defer alloc.free(value_text);
            try appendAlgebraicConstraint(out, alloc, match.field, value_text);
            return true;
        },
        .fuzzy => |fuzzy| {
            if (!std.mem.startsWith(u8, fuzzy.field, "/") or fuzzy.auto_fuzzy or fuzzy.prefix_len == 0) return false;
            const prefix_len: usize = @intCast(fuzzy.prefix_len);
            if (fuzzy.term.len < prefix_len) return false;
            const value_text = try db_mod.algebraic.index.pathFactStringFuzzyConstraintValueAlloc(
                alloc,
                fuzzy.term,
                fuzzy.max_edits,
                fuzzy.prefix_len,
            );
            defer alloc.free(value_text);
            try appendAlgebraicConstraint(out, alloc, fuzzy.field, value_text);
            return true;
        },
        .prefix => |prefix| {
            if (!std.mem.startsWith(u8, prefix.field, "/")) return false;
            const value_text = try db_mod.algebraic.index.pathFactStringPrefixConstraintValueAlloc(alloc, prefix.prefix);
            defer alloc.free(value_text);
            try appendAlgebraicConstraint(out, alloc, prefix.field, value_text);
            return true;
        },
        .wildcard => |wildcard| {
            if (!std.mem.startsWith(u8, wildcard.field, "/")) return false;
            const literal_prefix = algebraicWildcardLiteralPrefix(wildcard.pattern);
            if (literal_prefix.len == 0 and algebraicWildcardPatternHasMeta(wildcard.pattern)) return false;
            const value_text = try db_mod.algebraic.index.pathFactStringWildcardConstraintValueAlloc(alloc, wildcard.pattern);
            defer alloc.free(value_text);
            try appendAlgebraicConstraint(out, alloc, wildcard.field, value_text);
            return true;
        },
        .regexp => |regexp| {
            if (!std.mem.startsWith(u8, regexp.field, "/")) return false;
            if (algebraicRegexpLiteralPrefix(regexp.pattern).len == 0) return false;
            var compiled = regex_mod.compile(alloc, regexp.pattern) catch return false;
            defer compiled.deinit();
            const value_text = try db_mod.algebraic.index.pathFactStringRegexpConstraintValueAlloc(alloc, regexp.pattern);
            defer alloc.free(value_text);
            try appendAlgebraicConstraint(out, alloc, regexp.field, value_text);
            return true;
        },
        .bool_field => |field| {
            const value_text = try algebraicConstraintBoolValueAlloc(alloc, field.field, field.value);
            defer alloc.free(value_text);
            try appendAlgebraicConstraint(out, alloc, field.field, value_text);
            return true;
        },
        .numeric_range => |range| {
            if (!std.mem.startsWith(u8, range.field, "/")) return false;
            const value_text = try db_mod.algebraic.index.pathFactNumericRangeConstraintValueAlloc(
                alloc,
                range.min,
                range.max,
                range.inclusive_min,
                range.inclusive_max,
            );
            defer alloc.free(value_text);
            try appendAlgebraicConstraint(out, alloc, range.field, value_text);
            return true;
        },
        .date_range => |range| {
            if (!std.mem.startsWith(u8, range.field, "/")) return false;
            const start_text = if (range.start_ns) |ns| try std.fmt.allocPrint(alloc, "{d}", .{ns}) else null;
            defer if (start_text) |value| alloc.free(value);
            const end_text = if (range.end_ns) |ns| try std.fmt.allocPrint(alloc, "{d}", .{ns}) else null;
            defer if (end_text) |value| alloc.free(value);
            const value_text = try db_mod.algebraic.index.pathFactDateRangeConstraintValueAlloc(
                alloc,
                start_text,
                end_text,
                range.inclusive_start,
                range.inclusive_end,
            );
            defer alloc.free(value_text);
            try appendAlgebraicConstraint(out, alloc, range.field, value_text);
            return true;
        },
        .term_range => |range| {
            if (!std.mem.startsWith(u8, range.field, "/") or (range.min == null and range.max == null)) return false;
            const value_text = try db_mod.algebraic.index.pathFactTermRangeConstraintValueAlloc(
                alloc,
                range.min,
                range.max,
                range.inclusive_min,
                range.inclusive_max,
            );
            defer alloc.free(value_text);
            try appendAlgebraicConstraint(out, alloc, range.field, value_text);
            return true;
        },
        .ip_range => |range| {
            if (!std.mem.startsWith(u8, range.field, "/") or !algebraicValidIpRange(range.cidr)) return false;
            const value_text = try db_mod.algebraic.index.pathFactIpRangeConstraintValueAlloc(alloc, range.cidr);
            defer alloc.free(value_text);
            try appendAlgebraicConstraint(out, alloc, range.field, value_text);
            return true;
        },
        .geo_bbox => |bbox| {
            if (!std.mem.startsWith(u8, bbox.field, "/") or !algebraicValidGeoBBox(bbox.min_lat, bbox.min_lon, bbox.max_lat, bbox.max_lon)) return false;
            const value_text = db_mod.algebraic.index.pathFactGeoBBoxConstraintValueAlloc(
                alloc,
                bbox.min_lat,
                bbox.min_lon,
                bbox.max_lat,
                bbox.max_lon,
            ) catch return false;
            defer alloc.free(value_text);
            try appendAlgebraicConstraint(out, alloc, bbox.field, value_text);
            return true;
        },
        .geo_distance => |distance| {
            if (!std.mem.startsWith(u8, distance.field, "/") or !algebraicValidGeoDistance(distance.lat, distance.lon, distance.radius_meters)) return false;
            const value_text = db_mod.algebraic.index.pathFactGeoDistanceConstraintValueAlloc(
                alloc,
                distance.lat,
                distance.lon,
                distance.radius_meters,
            ) catch return false;
            defer alloc.free(value_text);
            try appendAlgebraicConstraint(out, alloc, distance.field, value_text);
            return true;
        },
        .geo_shape => |shape| {
            if (!std.mem.startsWith(u8, shape.field, "/") or !algebraicGeoShapeRelationSupported(shape.relation)) return false;
            const value_text = db_mod.algebraic.index.pathFactGeoShapeConstraintValueAlloc(
                alloc,
                @tagName(shape.relation),
                shape.polygons,
            ) catch return false;
            defer alloc.free(value_text);
            try appendAlgebraicConstraint(out, alloc, shape.field, value_text);
            return true;
        },
        .bool_query => |nested| return try collectAlgebraicTextBoolQueryConstraints(alloc, nested, out),
        else => return false,
    }
}

fn appendAlgebraicConstraint(
    out: *std.ArrayListUnmanaged(db_mod.aggregations.FixedConstraint),
    alloc: std.mem.Allocator,
    field: []const u8,
    value: []const u8,
) AlgebraicConstraintCollectError!void {
    for (out.items) |existing| {
        if (!std.mem.eql(u8, existing.field, field)) continue;
        if (std.mem.eql(u8, existing.value, value)) return;
        return error.UnsupportedQueryRequest;
    }
    try out.append(alloc, .{
        .field = try alloc.dupe(u8, field),
        .value = try alloc.dupe(u8, value),
    });
}

fn collectAlgebraicFilterConstraints(
    alloc: std.mem.Allocator,
    filter: std.json.Value,
    out: *std.ArrayListUnmanaged(db_mod.aggregations.FixedConstraint),
) AlgebraicConstraintCollectError!bool {
    if (filter != .object) return false;
    if (filter.object.get("match_all") != null) return true;
    if (filter.object.get("term")) |term| {
        const predicate = algebraicFilterTermPredicate(term, filter.object.get("field") orelse filter.object.get("path")) orelse return false;
        const value_text = try algebraicConstraintValueTextAlloc(alloc, predicate.field, predicate.value);
        defer alloc.free(value_text);
        try appendAlgebraicConstraint(out, alloc, predicate.field, value_text);
        return true;
    }
    if (filter.object.get("terms")) |terms| {
        if (!(try collectSingleValueTermsConstraint(alloc, terms, out))) return false;
        return true;
    }
    if (filter.object.get("match")) |match| {
        const predicate = algebraicPathMatchPredicate(match, filter.object.get("field")) orelse return false;
        if (predicate.text.len == 0) return false;
        const value_text = try db_mod.algebraic.index.pathFactStringMatchConstraintValueAlloc(alloc, predicate.text);
        defer alloc.free(value_text);
        try appendAlgebraicConstraint(out, alloc, predicate.path, value_text);
        return true;
    }
    if (filter.object.get("bool_field")) |bool_field| {
        if (bool_field != .object) return false;
        const field = bool_field.object.get("field") orelse return false;
        const value = bool_field.object.get("value") orelse return false;
        if (field != .string or value != .bool) return false;
        const value_text = try algebraicConstraintBoolValueAlloc(alloc, field.string, value.bool);
        defer alloc.free(value_text);
        try appendAlgebraicConstraint(out, alloc, field.string, value_text);
        return true;
    }
    if (filter.object.get("exists")) |exists| {
        const path = algebraicExistsPath(exists) orelse return false;
        if (!std.mem.startsWith(u8, path, "/")) return false;
        try appendAlgebraicConstraint(out, alloc, path, db_mod.algebraic.index.path_fact_exists_constraint_value);
        return true;
    }
    if (filter.object.get("prefix")) |prefix| {
        const predicate = algebraicPathPrefixPredicate(prefix, filter.object.get("field")) orelse return false;
        const value_text = try db_mod.algebraic.index.pathFactStringPrefixConstraintValueAlloc(alloc, predicate.prefix);
        defer alloc.free(value_text);
        try appendAlgebraicConstraint(out, alloc, predicate.path, value_text);
        return true;
    }
    if (filter.object.get("wildcard")) |wildcard| {
        const predicate = algebraicPathPatternPredicate(wildcard, "pattern") orelse return false;
        const literal_prefix = algebraicWildcardLiteralPrefix(predicate.text);
        if (literal_prefix.len == 0 and algebraicWildcardPatternHasMeta(predicate.text)) return false;
        const value_text = try db_mod.algebraic.index.pathFactStringWildcardConstraintValueAlloc(alloc, predicate.text);
        defer alloc.free(value_text);
        try appendAlgebraicConstraint(out, alloc, predicate.path, value_text);
        return true;
    }
    if (filter.object.get("regexp")) |regexp| {
        const predicate = algebraicPathPatternPredicate(regexp, "pattern") orelse return false;
        if (algebraicRegexpLiteralPrefix(predicate.text).len == 0) return false;
        var compiled = regex_mod.compile(alloc, predicate.text) catch return false;
        defer compiled.deinit();
        const value_text = try db_mod.algebraic.index.pathFactStringRegexpConstraintValueAlloc(alloc, predicate.text);
        defer alloc.free(value_text);
        try appendAlgebraicConstraint(out, alloc, predicate.path, value_text);
        return true;
    }
    if (filter.object.get("fuzzy")) |fuzzy| {
        const predicate = algebraicPathFuzzyPredicate(fuzzy) orelse return false;
        if (predicate.query.prefix_len == 0) return false;
        const prefix_len: usize = @intCast(predicate.query.prefix_len);
        if (predicate.query.term.len < prefix_len) return false;
        const value_text = try db_mod.algebraic.index.pathFactStringFuzzyConstraintValueAlloc(
            alloc,
            predicate.query.term,
            predicate.query.max_edits,
            predicate.query.prefix_len,
        );
        defer alloc.free(value_text);
        try appendAlgebraicConstraint(out, alloc, predicate.path, value_text);
        return true;
    }
    if (filter.object.get("numeric_range")) |range| {
        const predicate = algebraicPathNumericRangePredicate(range) orelse return false;
        const value_text = try db_mod.algebraic.index.pathFactNumericRangeConstraintValueAlloc(
            alloc,
            predicate.min,
            predicate.max,
            predicate.inclusive_min,
            predicate.inclusive_max,
        );
        defer alloc.free(value_text);
        try appendAlgebraicConstraint(out, alloc, predicate.path, value_text);
        return true;
    }
    if (filter.object.get("date_range")) |range| {
        const predicate = algebraicPathDateRangePredicate(range) orelse return false;
        const start_text = try algebraicDateBoundTextAlloc(alloc, predicate.start);
        defer if (start_text) |value| alloc.free(value);
        const end_text = try algebraicDateBoundTextAlloc(alloc, predicate.end);
        defer if (end_text) |value| alloc.free(value);
        const value_text = try db_mod.algebraic.index.pathFactDateRangeConstraintValueAlloc(
            alloc,
            start_text,
            end_text,
            predicate.inclusive_start,
            predicate.inclusive_end,
        );
        defer alloc.free(value_text);
        try appendAlgebraicConstraint(out, alloc, predicate.path, value_text);
        return true;
    }
    if (filter.object.get("ip_range")) |range| {
        const predicate = algebraicPathIpRangePredicate(range) orelse return false;
        const value_text = try db_mod.algebraic.index.pathFactIpRangeConstraintValueAlloc(alloc, predicate.cidr);
        defer alloc.free(value_text);
        try appendAlgebraicConstraint(out, alloc, predicate.path, value_text);
        return true;
    }
    if (filter.object.get("geo_bbox")) |bbox| {
        const predicate = algebraicPathGeoBBoxPredicate(bbox) orelse return false;
        const value_text = db_mod.algebraic.index.pathFactGeoBBoxConstraintValueAlloc(
            alloc,
            predicate.min_lat,
            predicate.min_lon,
            predicate.max_lat,
            predicate.max_lon,
        ) catch return false;
        defer alloc.free(value_text);
        try appendAlgebraicConstraint(out, alloc, predicate.path, value_text);
        return true;
    }
    if (filter.object.get("geo_distance")) |distance| {
        const predicate = algebraicPathGeoDistancePredicate(distance) orelse return false;
        const value_text = db_mod.algebraic.index.pathFactGeoDistanceConstraintValueAlloc(
            alloc,
            predicate.lat,
            predicate.lon,
            predicate.radius_meters,
        ) catch return false;
        defer alloc.free(value_text);
        try appendAlgebraicConstraint(out, alloc, predicate.path, value_text);
        return true;
    }
    if (filter.object.get("geo_shape")) |shape| {
        var predicate = (try algebraicPathGeoShapePredicateAlloc(alloc, shape)) orelse return false;
        defer predicate.deinit(alloc);
        const value_text = db_mod.algebraic.index.pathFactGeoShapeConstraintValueAlloc(
            alloc,
            @tagName(predicate.relation),
            predicate.polygons,
        ) catch return false;
        defer alloc.free(value_text);
        try appendAlgebraicConstraint(out, alloc, predicate.path, value_text);
        return true;
    }
    if (filter.object.get("term_range")) |range| {
        const predicate = algebraicPathTermRangePredicate(range) orelse return false;
        const value_text = try db_mod.algebraic.index.pathFactTermRangeConstraintValueAlloc(
            alloc,
            predicate.min,
            predicate.max,
            predicate.inclusive_min,
            predicate.inclusive_max,
        );
        defer alloc.free(value_text);
        try appendAlgebraicConstraint(out, alloc, predicate.path, value_text);
        return true;
    }
    if (filter.object.get("range")) |range| {
        if (algebraicPathStandardNumericRangePredicate(range)) |predicate| {
            const value_text = try db_mod.algebraic.index.pathFactNumericRangeConstraintValueAlloc(
                alloc,
                predicate.min,
                predicate.max,
                predicate.inclusive_min,
                predicate.inclusive_max,
            );
            defer alloc.free(value_text);
            try appendAlgebraicConstraint(out, alloc, predicate.path, value_text);
            return true;
        }
        if (algebraicPathStandardDateRangePredicate(range)) |predicate| {
            const start_text = try algebraicDateBoundTextAlloc(alloc, predicate.start);
            defer if (start_text) |value| alloc.free(value);
            const end_text = try algebraicDateBoundTextAlloc(alloc, predicate.end);
            defer if (end_text) |value| alloc.free(value);
            const value_text = try db_mod.algebraic.index.pathFactDateRangeConstraintValueAlloc(
                alloc,
                start_text,
                end_text,
                predicate.inclusive_start,
                predicate.inclusive_end,
            );
            defer alloc.free(value_text);
            try appendAlgebraicConstraint(out, alloc, predicate.path, value_text);
            return true;
        }
        if (algebraicPathStandardTermRangePredicate(range)) |predicate| {
            const value_text = try db_mod.algebraic.index.pathFactTermRangeConstraintValueAlloc(
                alloc,
                predicate.min,
                predicate.max,
                predicate.inclusive_min,
                predicate.inclusive_max,
            );
            defer alloc.free(value_text);
            try appendAlgebraicConstraint(out, alloc, predicate.path, value_text);
            return true;
        }
        return false;
    }
    if (filter.object.get("conjuncts")) |conjuncts| {
        if (conjuncts != .array) return false;
        for (conjuncts.array.items) |item| {
            if (!(try collectAlgebraicFilterConstraints(alloc, item, out))) return false;
        }
        return true;
    }
    if (filter.object.get("disjuncts")) |disjuncts| {
        return try collectAlgebraicFilterShouldTermConstraint(alloc, disjuncts, out);
    }
    if (filter.object.get("bool")) |bool_query| {
        if (bool_query != .object) return false;
        if (bool_query.object.get("must_not") != null) return false;
        const must = bool_query.object.get("must");
        const filter_clause = bool_query.object.get("filter");
        const should = bool_query.object.get("should");
        if (should) |clause| {
            const min_should_value = bool_query.object.get("minimum_should_match") orelse bool_query.object.get("min_should");
            if (must != null or filter_clause != null) {
                if (!algebraicBoolShouldMinIsOptional(min_should_value)) return false;
            } else {
                if (!algebraicBoolShouldMinIsOne(min_should_value)) return false;
                return try collectAlgebraicFilterShouldTermConstraint(alloc, clause, out);
            }
        }
        if (must == null and filter_clause == null) return false;
        if (must) |clause| {
            if (!(try collectAlgebraicFilterConstraintClause(alloc, clause, out))) return false;
        }
        if (filter_clause) |clause| {
            if (!(try collectAlgebraicFilterConstraintClause(alloc, clause, out))) return false;
        }
        return true;
    }
    return false;
}

fn collectAlgebraicFilterConstraintClause(
    alloc: std.mem.Allocator,
    clause: std.json.Value,
    out: *std.ArrayListUnmanaged(db_mod.aggregations.FixedConstraint),
) AlgebraicConstraintCollectError!bool {
    if (clause == .array) {
        if (clause.array.items.len == 0) return false;
        for (clause.array.items) |item| {
            if (!(try collectAlgebraicFilterConstraints(alloc, item, out))) return false;
        }
        return true;
    }
    return try collectAlgebraicFilterConstraints(alloc, clause, out);
}

fn algebraicBoolShouldMinIsOptional(value: ?std.json.Value) bool {
    const actual = value orelse return true;
    return switch (actual) {
        .integer => |number| number == 0,
        .float => |number| number == 0.0,
        .string => |text| std.mem.eql(u8, text, "0"),
        else => false,
    };
}

fn algebraicBoolShouldMinIsOne(value: ?std.json.Value) bool {
    const actual = value orelse return true;
    return switch (actual) {
        .integer => |number| number == 1,
        .float => |number| number == 1.0,
        .string => |text| std.mem.eql(u8, text, "1"),
        else => false,
    };
}

fn collectAlgebraicFilterShouldTermConstraint(
    alloc: std.mem.Allocator,
    clause: std.json.Value,
    out: *std.ArrayListUnmanaged(db_mod.aggregations.FixedConstraint),
) AlgebraicConstraintCollectError!bool {
    const items = switch (clause) {
        .array => |array| array.items,
        else => return try collectAlgebraicFilterShouldTermItemsConstraint(alloc, &.{clause}, out),
    };
    return try collectAlgebraicFilterShouldTermItemsConstraint(alloc, items, out);
}

fn collectAlgebraicFilterShouldTermItemsConstraint(
    alloc: std.mem.Allocator,
    items: []const std.json.Value,
    out: *std.ArrayListUnmanaged(db_mod.aggregations.FixedConstraint),
) AlgebraicConstraintCollectError!bool {
    if (items.len == 0) return false;
    var field: ?[]const u8 = null;
    const typed_values = try alloc.alloc([]const u8, items.len);
    defer alloc.free(typed_values);
    var initialized: usize = 0;
    defer {
        for (typed_values[0..initialized]) |value| alloc.free(@constCast(value));
    }

    for (items, 0..) |item, i| {
        const object = switch (item) {
            .object => |object| object,
            else => return false,
        };
        const predicate = algebraicFilterTermPredicate(object.get("term") orelse return false, object.get("field") orelse object.get("path")) orelse return false;
        if (!std.mem.startsWith(u8, predicate.field, "/")) return false;
        if (field) |existing| {
            if (!std.mem.eql(u8, existing, predicate.field)) return false;
        } else {
            field = predicate.field;
        }
        typed_values[i] = try algebraicConstraintValueTextAlloc(alloc, predicate.field, predicate.value);
        initialized += 1;
    }

    try appendAlgebraicTypedAnyConstraint(out, alloc, field orelse return false, typed_values);
    return true;
}

const AlgebraicFilterTermPredicate = struct {
    field: []const u8,
    value: std.json.Value,
};

fn algebraicFilterTermPredicate(term: std.json.Value, sibling_field_value: ?std.json.Value) ?AlgebraicFilterTermPredicate {
    if (term == .object) {
        if (term.object.get("field") orelse term.object.get("path")) |field_value| {
            const field = algebraicJsonString(field_value) orelse return null;
            const value = term.object.get("term") orelse term.object.get("value") orelse return null;
            return .{ .field = field, .value = value };
        }
        if (term.object.count() == 1) {
            var it = term.object.iterator();
            const entry = it.next() orelse return null;
            return .{ .field = entry.key_ptr.*, .value = entry.value_ptr.* };
        }
    }
    const field = algebraicJsonString(sibling_field_value orelse return null) orelse return null;
    return .{ .field = field, .value = term };
}

fn algebraicExistsPath(value: std.json.Value) ?[]const u8 {
    return switch (value) {
        .string => |field| field,
        .object => |object| blk: {
            const path = object.get("path") orelse object.get("field") orelse break :blk null;
            if (path != .string) break :blk null;
            break :blk path.string;
        },
        else => null,
    };
}

const AlgebraicPathPrefixPredicate = struct {
    path: []const u8,
    prefix: []const u8,
};

const AlgebraicPathTextPredicate = struct {
    path: []const u8,
    text: []const u8,
};

const AlgebraicFuzzyQuery = struct {
    term: []const u8,
    max_edits: u8,
    prefix_len: u8,
};

const AlgebraicPathFuzzyPredicate = struct {
    path: []const u8,
    query: AlgebraicFuzzyQuery,
};

const AlgebraicPathNumericRangePredicate = struct {
    path: []const u8,
    min: ?f64 = null,
    max: ?f64 = null,
    inclusive_min: bool = true,
    inclusive_max: bool = false,
};

const AlgebraicPathTermRangePredicate = struct {
    path: []const u8,
    min: ?[]const u8 = null,
    max: ?[]const u8 = null,
    inclusive_min: bool = true,
    inclusive_max: bool = false,
};

const AlgebraicPathDateRangePredicate = struct {
    path: []const u8,
    start: ?std.json.Value = null,
    end: ?std.json.Value = null,
    inclusive_start: bool = true,
    inclusive_end: bool = false,
};

const AlgebraicPathIpRangePredicate = struct {
    path: []const u8,
    cidr: []const u8,
};

const AlgebraicPathGeoBBoxPredicate = struct {
    path: []const u8,
    min_lat: f64,
    min_lon: f64,
    max_lat: f64,
    max_lon: f64,
};

const AlgebraicPathGeoDistancePredicate = struct {
    path: []const u8,
    lat: f64,
    lon: f64,
    radius_meters: f64,
};

const AlgebraicPathGeoShapePredicate = struct {
    path: []const u8,
    relation: db_mod.types.GeoShapeRelation,
    polygons: []const []const db_mod.types.GeoPoint,

    fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        for (self.polygons) |polygon| {
            if (polygon.len > 0) alloc.free(@constCast(polygon));
        }
        if (self.polygons.len > 0) alloc.free(@constCast(self.polygons));
        self.* = undefined;
    }
};

fn algebraicJsonString(value: std.json.Value) ?[]const u8 {
    return switch (value) {
        .string => |text| text,
        else => null,
    };
}

fn algebraicPathPrefixPredicate(prefix_value: std.json.Value, sibling_field_value: ?std.json.Value) ?AlgebraicPathPrefixPredicate {
    return switch (prefix_value) {
        .object => |object| blk: {
            if (object.get("path")) |path_value| {
                const path = algebraicJsonString(path_value) orelse break :blk null;
                if (!std.mem.startsWith(u8, path, "/")) break :blk null;
                const prefix = algebraicJsonString(object.get("value") orelse object.get("prefix") orelse break :blk null) orelse break :blk null;
                break :blk .{ .path = path, .prefix = prefix };
            }
            if (object.get("role") == null) {
                if (object.get("field")) |field_value| {
                    const field = algebraicJsonString(field_value) orelse break :blk null;
                    if (std.mem.startsWith(u8, field, "/")) {
                        const prefix = algebraicJsonString(object.get("value") orelse object.get("prefix") orelse break :blk null) orelse break :blk null;
                        break :blk .{ .path = field, .prefix = prefix };
                    }
                }
            }
            if (object.count() == 1) {
                var it = object.iterator();
                const entry = it.next() orelse break :blk null;
                if (!std.mem.startsWith(u8, entry.key_ptr.*, "/")) break :blk null;
                const prefix = algebraicJsonString(entry.value_ptr.*) orelse break :blk null;
                break :blk .{ .path = entry.key_ptr.*, .prefix = prefix };
            }
            break :blk null;
        },
        .string => |prefix| blk: {
            const field = algebraicJsonString(sibling_field_value orelse break :blk null) orelse break :blk null;
            break :blk if (std.mem.startsWith(u8, field, "/")) .{ .path = field, .prefix = prefix } else null;
        },
        else => null,
    };
}

fn algebraicPathFromPredicateObject(object: anytype) ?[]const u8 {
    if (object.get("path")) |path_value| {
        const path = algebraicJsonString(path_value) orelse return null;
        if (!std.mem.startsWith(u8, path, "/")) return null;
        return path;
    }
    return null;
}

fn algebraicPathPatternPredicate(value: std.json.Value, pattern_field: []const u8) ?AlgebraicPathTextPredicate {
    const object = switch (value) {
        .object => |object| object,
        else => return null,
    };
    if (algebraicPathFromPredicateObject(object)) |path| {
        const text = algebraicJsonString(object.get(pattern_field) orelse object.get("value") orelse return null) orelse return null;
        return .{ .path = path, .text = text };
    }
    if (object.get("role") == null) {
        if (object.get("field")) |field_value| {
            const field = algebraicJsonString(field_value) orelse return null;
            if (std.mem.startsWith(u8, field, "/")) {
                const text = algebraicJsonString(object.get(pattern_field) orelse object.get("value") orelse return null) orelse return null;
                return .{ .path = field, .text = text };
            }
        }
    }
    if (object.count() == 1) {
        var it = object.iterator();
        const entry = it.next() orelse return null;
        if (!std.mem.startsWith(u8, entry.key_ptr.*, "/")) return null;
        const text = algebraicJsonString(entry.value_ptr.*) orelse return null;
        return .{ .path = entry.key_ptr.*, .text = text };
    }
    return null;
}

fn algebraicPathMatchPredicate(value: std.json.Value, sibling_field_value: ?std.json.Value) ?AlgebraicPathTextPredicate {
    return switch (value) {
        .object => algebraicPathPatternPredicate(value, "query"),
        .string => |text| blk: {
            const field = algebraicJsonString(sibling_field_value orelse break :blk null) orelse break :blk null;
            break :blk if (std.mem.startsWith(u8, field, "/")) .{ .path = field, .text = text } else null;
        },
        else => null,
    };
}

fn algebraicWildcardLiteralPrefix(pattern: []const u8) []const u8 {
    for (pattern, 0..) |ch, i| {
        if (ch == '*' or ch == '?') return pattern[0..i];
    }
    return pattern;
}

fn algebraicWildcardPatternHasMeta(pattern: []const u8) bool {
    return std.mem.indexOfAny(u8, pattern, "*?") != null;
}

fn algebraicRegexpLiteralPrefix(pattern: []const u8) []const u8 {
    for (pattern, 0..) |ch, i| {
        switch (ch) {
            '.', '^', '$', '*', '+', '?', '(', ')', '[', ']', '{', '}', '|', '\\' => return pattern[0..i],
            else => {},
        }
    }
    return pattern;
}

fn algebraicJsonU8(value: std.json.Value) ?u8 {
    return switch (value) {
        .integer => |number| std.math.cast(u8, number),
        .float => |number| blk: {
            if (!std.math.isFinite(number) or @round(number) != number) break :blk null;
            const parsed: i64 = @intFromFloat(number);
            break :blk std.math.cast(u8, parsed);
        },
        else => null,
    };
}

fn algebraicParseFuzzyOptions(object: anytype, out: *AlgebraicFuzzyQuery) bool {
    if (object.get("max_edits")) |edits| {
        out.max_edits = algebraicJsonU8(edits) orelse return false;
    }
    if (object.get("prefix_length")) |prefix| {
        out.prefix_len = algebraicJsonU8(prefix) orelse return false;
    }
    if (object.get("auto_fuzzy")) |auto| {
        if (auto != .bool) return false;
        if (auto.bool) out.max_edits = if (out.term.len > 5) 2 else if (out.term.len > 2) 1 else 0;
    }
    return true;
}

fn algebraicParseFuzzyQuery(value: std.json.Value) ?AlgebraicFuzzyQuery {
    return switch (value) {
        .string => |text| .{ .term = text, .max_edits = 1, .prefix_len = 0 },
        .object => |object| blk: {
            var out = AlgebraicFuzzyQuery{
                .term = algebraicJsonString(object.get("query") orelse object.get("value") orelse break :blk null) orelse break :blk null,
                .max_edits = 1,
                .prefix_len = 0,
            };
            if (!algebraicParseFuzzyOptions(object, &out)) break :blk null;
            break :blk out;
        },
        else => null,
    };
}

fn algebraicPathFuzzyPredicate(value: std.json.Value) ?AlgebraicPathFuzzyPredicate {
    const object = switch (value) {
        .object => |object| object,
        else => return null,
    };
    if (algebraicPathFromPredicateObject(object)) |path| {
        var query = AlgebraicFuzzyQuery{
            .term = algebraicJsonString(object.get("query") orelse object.get("value") orelse return null) orelse return null,
            .max_edits = 1,
            .prefix_len = 0,
        };
        if (!algebraicParseFuzzyOptions(object, &query)) return null;
        return .{ .path = path, .query = query };
    }
    if (object.get("role") == null) {
        if (object.get("field")) |field_value| {
            const field = algebraicJsonString(field_value) orelse return null;
            if (std.mem.startsWith(u8, field, "/")) {
                var query = AlgebraicFuzzyQuery{
                    .term = algebraicJsonString(object.get("query") orelse object.get("value") orelse return null) orelse return null,
                    .max_edits = 1,
                    .prefix_len = 0,
                };
                if (!algebraicParseFuzzyOptions(object, &query)) return null;
                return .{ .path = field, .query = query };
            }
        }
    }
    if (object.count() == 1) {
        var it = object.iterator();
        const entry = it.next() orelse return null;
        if (!std.mem.startsWith(u8, entry.key_ptr.*, "/")) return null;
        const query = algebraicParseFuzzyQuery(entry.value_ptr.*) orelse return null;
        return .{ .path = entry.key_ptr.*, .query = query };
    }
    return null;
}

fn algebraicOptionalBool(value: ?std.json.Value) ?bool {
    const actual = value orelse return null;
    return switch (actual) {
        .bool => |flag| flag,
        .null => null,
        else => null,
    };
}

fn algebraicOptionalF64(value: ?std.json.Value) ?f64 {
    const actual = value orelse return null;
    return switch (actual) {
        .integer => |number| @floatFromInt(number),
        .float => |number| number,
        .null => null,
        else => null,
    };
}

fn algebraicOptionalString(value: ?std.json.Value) ?[]const u8 {
    const actual = value orelse return null;
    return switch (actual) {
        .string => |text| text,
        .null => null,
        else => null,
    };
}

fn algebraicNumericJsonValue(value: std.json.Value) bool {
    return switch (value) {
        .integer, .float => true,
        else => false,
    };
}

fn algebraicStringJsonValue(value: std.json.Value) bool {
    return switch (value) {
        .string => true,
        else => false,
    };
}

fn algebraicDateJsonValue(value: std.json.Value) bool {
    return switch (value) {
        .integer => |number| number >= 0,
        .string => |text| (algebraicParseDateTimeOptionalToNs(text) catch null) != null,
        else => false,
    };
}

fn algebraicDateBoundTextAlloc(alloc: std.mem.Allocator, value: ?std.json.Value) !?[]u8 {
    const actual = value orelse return null;
    return switch (actual) {
        .integer => |number| if (number >= 0) try std.fmt.allocPrint(alloc, "{d}", .{number}) else error.UnsupportedQueryRequest,
        .string => |text| if ((try algebraicParseDateTimeOptionalToNs(text)) != null) try alloc.dupe(u8, text) else error.UnsupportedQueryRequest,
        .null => null,
        else => error.UnsupportedQueryRequest,
    };
}

fn algebraicValidIpRange(text: []const u8) bool {
    return algebraicParseIpCidr(text) != null or algebraicParseIPv4(text) != null;
}

fn algebraicValidLatitude(lat: f64) bool {
    return std.math.isFinite(lat) and lat >= -90.0 and lat <= 90.0;
}

fn algebraicValidLongitude(lon: f64) bool {
    return std.math.isFinite(lon) and lon >= -180.0 and lon <= 180.0;
}

fn algebraicValidGeoBBox(min_lat: f64, min_lon: f64, max_lat: f64, max_lon: f64) bool {
    return algebraicValidLatitude(min_lat) and
        algebraicValidLatitude(max_lat) and
        algebraicValidLongitude(min_lon) and
        algebraicValidLongitude(max_lon) and
        min_lat <= max_lat;
}

fn algebraicValidGeoDistance(lat: f64, lon: f64, radius_meters: f64) bool {
    return algebraicValidLatitude(lat) and
        algebraicValidLongitude(lon) and
        std.math.isFinite(radius_meters) and
        radius_meters >= 0;
}

fn algebraicGeoShapeRelationSupported(relation: db_mod.types.GeoShapeRelation) bool {
    return switch (relation) {
        .intersects, .within => true,
        .contains => false,
    };
}

const AlgebraicIpCidr = struct {
    network: [4]u8,
    prefix_len: u8,
};

fn algebraicParseIpCidr(text: []const u8) ?AlgebraicIpCidr {
    const slash_pos = std.mem.indexOfScalar(u8, text, '/') orelse return null;
    const ip = algebraicParseIPv4(text[0..slash_pos]) orelse return null;
    const prefix_len = std.fmt.parseInt(u8, text[slash_pos + 1 ..], 10) catch return null;
    if (prefix_len > 32) return null;
    const mask = algebraicIpMask(prefix_len);
    return .{
        .network = .{ ip[0] & mask[0], ip[1] & mask[1], ip[2] & mask[2], ip[3] & mask[3] },
        .prefix_len = prefix_len,
    };
}

fn algebraicParseIPv4(text: []const u8) ?[4]u8 {
    var parts = std.mem.splitScalar(u8, text, '.');
    var out: [4]u8 = undefined;
    var i: usize = 0;
    while (parts.next()) |part| {
        if (i >= 4 or part.len == 0) return null;
        out[i] = std.fmt.parseInt(u8, part, 10) catch return null;
        i += 1;
    }
    if (i != 4) return null;
    return out;
}

fn algebraicIpMask(prefix_len: u8) [4]u8 {
    var mask = [_]u8{ 0, 0, 0, 0 };
    var remaining = prefix_len;
    for (&mask) |*byte| {
        if (remaining >= 8) {
            byte.* = 0xff;
            remaining -= 8;
        } else if (remaining > 0) {
            byte.* = @as(u8, 0xff) << @intCast(8 - remaining);
            remaining = 0;
        }
    }
    return mask;
}

fn algebraicParseDateTimeOptionalToNs(text: []const u8) !?u64 {
    if (try algebraicParseRfc3339ToNs(text)) |ts| return ts;
    if (text.len != 10 or text[4] != '-' or text[7] != '-') return null;
    const year = std.fmt.parseInt(i64, text[0..4], 10) catch return null;
    const month = std.fmt.parseInt(i64, text[5..7], 10) catch return null;
    const day = std.fmt.parseInt(i64, text[8..10], 10) catch return null;
    return algebraicCivilDateTimeToNs(year, month, day, 0, 0, 0, 0);
}

fn algebraicParseRfc3339ToNs(text: []const u8) !?u64 {
    if (text.len < 20) return null;
    if (text[4] != '-' or text[7] != '-' or text[10] != 'T' or text[13] != ':' or text[16] != ':') return null;
    const year = std.fmt.parseInt(i64, text[0..4], 10) catch return null;
    const month = std.fmt.parseInt(i64, text[5..7], 10) catch return null;
    const day = std.fmt.parseInt(i64, text[8..10], 10) catch return null;
    const hour = std.fmt.parseInt(i64, text[11..13], 10) catch return null;
    const minute = std.fmt.parseInt(i64, text[14..16], 10) catch return null;
    const second = std.fmt.parseInt(i64, text[17..19], 10) catch return null;
    var idx: usize = 19;
    var nanos: u64 = 0;
    if (idx < text.len and text[idx] == '.') {
        idx += 1;
        const frac_start = idx;
        while (idx < text.len and text[idx] >= '0' and text[idx] <= '9') : (idx += 1) {}
        const frac = text[frac_start..idx];
        if (frac.len == 0 or frac.len > 9) return null;
        var frac_ns = std.fmt.parseInt(u64, frac, 10) catch return null;
        var scale: usize = frac.len;
        while (scale < 9) : (scale += 1) frac_ns *= 10;
        nanos = frac_ns;
    }
    if (idx >= text.len or text[idx] != 'Z' or idx + 1 != text.len) return null;
    return algebraicCivilDateTimeToNs(year, month, day, hour, minute, second, nanos);
}

fn algebraicCivilDateTimeToNs(year: i64, month: i64, day: i64, hour: i64, minute: i64, second: i64, nanos: u64) ?u64 {
    if (month < 1 or month > 12 or day < 1 or day > 31 or hour < 0 or hour > 23 or minute < 0 or minute > 59 or second < 0 or second > 60) return null;
    const days = algebraicDaysFromCivil(year, month, day);
    if (days < 0) return null;
    const secs = days * 86_400 + hour * 3_600 + minute * 60 + second;
    if (secs < 0) return null;
    return @as(u64, @intCast(secs)) * std.time.ns_per_s + nanos;
}

fn algebraicDaysFromCivil(year: i64, month: i64, day: i64) i64 {
    var y = year;
    y -= if (month <= 2) 1 else 0;
    const era = @divFloor(y, 400);
    const yoe = y - era * 400;
    const mp = month + (if (month > 2) @as(i64, -3) else @as(i64, 9));
    const doy = @divFloor(153 * mp + 2, 5) + day - 1;
    const doe = yoe * 365 + @divFloor(yoe, 4) - @divFloor(yoe, 100) + doy;
    return era * 146_097 + doe - 719_468;
}

fn algebraicPathIpRangePredicate(value: std.json.Value) ?AlgebraicPathIpRangePredicate {
    const object = switch (value) {
        .object => |object| object,
        else => return null,
    };
    const cidr = algebraicJsonString(object.get("cidr") orelse return null) orelse return null;
    if (!algebraicValidIpRange(cidr)) return null;
    if (algebraicPathFromPredicateObject(object)) |path| {
        return .{ .path = path, .cidr = cidr };
    }
    if (object.get("role") == null) {
        if (object.get("field")) |field_value| {
            const field = algebraicJsonString(field_value) orelse return null;
            if (std.mem.startsWith(u8, field, "/")) return .{ .path = field, .cidr = cidr };
        }
    }
    return null;
}

fn algebraicPathGeoBBoxPredicate(value: std.json.Value) ?AlgebraicPathGeoBBoxPredicate {
    const object = switch (value) {
        .object => |object| object,
        else => return null,
    };
    if (object.get("role") != null) return null;
    const path = if (object.get("path")) |path_value|
        algebraicJsonString(path_value) orelse return null
    else if (object.get("field")) |field_value| blk: {
        const field = algebraicJsonString(field_value) orelse return null;
        if (!std.mem.startsWith(u8, field, "/")) return null;
        break :blk field;
    } else return null;
    if (!std.mem.startsWith(u8, path, "/")) return null;
    const min_lat = algebraicOptionalF64(object.get("min_lat")) orelse return null;
    const min_lon = algebraicOptionalF64(object.get("min_lon")) orelse return null;
    const max_lat = algebraicOptionalF64(object.get("max_lat")) orelse return null;
    const max_lon = algebraicOptionalF64(object.get("max_lon")) orelse return null;
    if (!algebraicValidGeoBBox(min_lat, min_lon, max_lat, max_lon)) return null;
    return .{
        .path = path,
        .min_lat = min_lat,
        .min_lon = min_lon,
        .max_lat = max_lat,
        .max_lon = max_lon,
    };
}

fn algebraicPathGeoDistancePredicate(value: std.json.Value) ?AlgebraicPathGeoDistancePredicate {
    const object = switch (value) {
        .object => |object| object,
        else => return null,
    };
    if (object.get("role") != null) return null;
    const path = if (object.get("path")) |path_value|
        algebraicJsonString(path_value) orelse return null
    else if (object.get("field")) |field_value| blk: {
        const field = algebraicJsonString(field_value) orelse return null;
        if (!std.mem.startsWith(u8, field, "/")) return null;
        break :blk field;
    } else return null;
    if (!std.mem.startsWith(u8, path, "/")) return null;
    const lat = algebraicOptionalF64(object.get("lat")) orelse return null;
    const lon = algebraicOptionalF64(object.get("lon")) orelse return null;
    const radius_meters = algebraicOptionalF64(object.get("radius_meters")) orelse return null;
    if (!algebraicValidGeoDistance(lat, lon, radius_meters)) return null;
    return .{
        .path = path,
        .lat = lat,
        .lon = lon,
        .radius_meters = radius_meters,
    };
}

fn algebraicPathGeoShapePredicateAlloc(alloc: std.mem.Allocator, value: std.json.Value) !?AlgebraicPathGeoShapePredicate {
    const object = switch (value) {
        .object => |object| object,
        else => return null,
    };
    if (object.get("role") != null) return null;
    const path = if (object.get("path")) |path_value|
        algebraicJsonString(path_value) orelse return null
    else if (object.get("field")) |field_value| blk: {
        const field = algebraicJsonString(field_value) orelse return null;
        if (!std.mem.startsWith(u8, field, "/")) return null;
        break :blk field;
    } else return null;
    if (!std.mem.startsWith(u8, path, "/")) return null;
    const relation_text = if (object.get("relation")) |relation_value|
        algebraicJsonString(relation_value) orelse return null
    else
        "intersects";
    const relation = std.meta.stringToEnum(db_mod.types.GeoShapeRelation, relation_text) orelse return null;
    if (!algebraicGeoShapeRelationSupported(relation)) return null;
    const polygons_value = object.get("polygons") orelse object.get("polygon") orelse return null;
    const polygons = try algebraicGeoShapePolygonsAlloc(alloc, polygons_value);
    errdefer {
        for (polygons) |polygon| {
            if (polygon.len > 0) alloc.free(@constCast(polygon));
        }
        if (polygons.len > 0) alloc.free(polygons);
    }
    return .{
        .path = path,
        .relation = relation,
        .polygons = polygons,
    };
}

fn algebraicGeoShapePolygonsAlloc(alloc: std.mem.Allocator, value: std.json.Value) ![]const []const db_mod.types.GeoPoint {
    const array = switch (value) {
        .array => |array| array,
        else => return error.UnsupportedQueryRequest,
    };
    if (array.items.len == 0) return error.UnsupportedQueryRequest;
    const first_is_point = array.items[0] == .object;
    const polygon_count: usize = if (first_is_point) 1 else array.items.len;
    var out = try alloc.alloc([]const db_mod.types.GeoPoint, polygon_count);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |polygon| {
            if (polygon.len > 0) alloc.free(@constCast(polygon));
        }
        if (out.len > 0) alloc.free(out);
    }
    if (first_is_point) {
        out[0] = try algebraicGeoShapePolygonAlloc(alloc, value);
        initialized = 1;
    } else {
        for (array.items, 0..) |item, i| {
            out[i] = try algebraicGeoShapePolygonAlloc(alloc, item);
            initialized += 1;
        }
    }
    return out;
}

fn algebraicGeoShapePolygonAlloc(alloc: std.mem.Allocator, value: std.json.Value) ![]const db_mod.types.GeoPoint {
    const array = switch (value) {
        .array => |array| array,
        else => return error.UnsupportedQueryRequest,
    };
    if (array.items.len < 3) return error.UnsupportedQueryRequest;
    var out = try alloc.alloc(db_mod.types.GeoPoint, array.items.len);
    errdefer if (out.len > 0) alloc.free(out);
    for (array.items, 0..) |item, i| {
        const object = switch (item) {
            .object => |object| object,
            else => return error.UnsupportedQueryRequest,
        };
        const lat = algebraicOptionalF64(object.get("lat")) orelse return error.UnsupportedQueryRequest;
        const lon = algebraicOptionalF64(object.get("lon")) orelse return error.UnsupportedQueryRequest;
        if (!algebraicValidLatitude(lat) or !algebraicValidLongitude(lon)) return error.UnsupportedQueryRequest;
        out[i] = .{ .lat = lat, .lon = lon };
    }
    return out;
}

fn algebraicPathNumericRangePredicate(value: std.json.Value) ?AlgebraicPathNumericRangePredicate {
    const object = switch (value) {
        .object => |object| object,
        else => return null,
    };
    const path = if (object.get("path")) |path_value|
        algebraicJsonString(path_value) orelse return null
    else if (object.get("field")) |field_value| blk: {
        const field = algebraicJsonString(field_value) orelse return null;
        if (!std.mem.startsWith(u8, field, "/")) return null;
        break :blk field;
    } else return null;
    if (!std.mem.startsWith(u8, path, "/")) return null;
    const min = algebraicOptionalF64(object.get("min"));
    const max = algebraicOptionalF64(object.get("max"));
    if (min == null and max == null) return null;
    return .{
        .path = path,
        .min = min,
        .max = max,
        .inclusive_min = algebraicOptionalBool(object.get("inclusive_min")) orelse true,
        .inclusive_max = algebraicOptionalBool(object.get("inclusive_max")) orelse false,
    };
}

fn algebraicPathTermRangePredicate(value: std.json.Value) ?AlgebraicPathTermRangePredicate {
    const object = switch (value) {
        .object => |object| object,
        else => return null,
    };
    if (object.get("path") != null or object.get("field") != null) {
        const path = if (object.get("path")) |path_value|
            algebraicJsonString(path_value) orelse return null
        else blk: {
            const field = algebraicJsonString(object.get("field").?) orelse return null;
            if (!std.mem.startsWith(u8, field, "/")) return null;
            break :blk field;
        };
        if (!std.mem.startsWith(u8, path, "/")) return null;
        const min = algebraicOptionalString(object.get("min"));
        const max = algebraicOptionalString(object.get("max"));
        if (min == null and max == null) return null;
        return .{
            .path = path,
            .min = min,
            .max = max,
            .inclusive_min = algebraicOptionalBool(object.get("inclusive_min")) orelse true,
            .inclusive_max = algebraicOptionalBool(object.get("inclusive_max")) orelse false,
        };
    }
    if (object.count() != 1) return null;
    var it = object.iterator();
    const entry = it.next() orelse return null;
    if (!std.mem.startsWith(u8, entry.key_ptr.*, "/")) return null;
    return algebraicTermRangeFromBounds(entry.key_ptr.*, switch (entry.value_ptr.*) {
        .object => |inner| inner,
        else => return null,
    });
}

fn algebraicPathDateRangePredicate(value: std.json.Value) ?AlgebraicPathDateRangePredicate {
    const object = switch (value) {
        .object => |object| object,
        else => return null,
    };
    if (object.get("path") != null or object.get("field") != null) {
        const path = if (object.get("path")) |path_value|
            algebraicJsonString(path_value) orelse return null
        else blk: {
            const field = algebraicJsonString(object.get("field").?) orelse return null;
            if (!std.mem.startsWith(u8, field, "/")) return null;
            break :blk field;
        };
        if (!std.mem.startsWith(u8, path, "/")) return null;
        const start = object.get("start_ns") orelse object.get("start");
        const end = object.get("end_ns") orelse object.get("end");
        if (start == null and end == null) return null;
        if (start) |bound| if (!algebraicDateJsonValue(bound)) return null;
        if (end) |bound| if (!algebraicDateJsonValue(bound)) return null;
        return .{
            .path = path,
            .start = start,
            .end = end,
            .inclusive_start = algebraicOptionalBool(object.get("inclusive_start")) orelse true,
            .inclusive_end = algebraicOptionalBool(object.get("inclusive_end")) orelse false,
        };
    }
    if (object.count() != 1) return null;
    var it = object.iterator();
    const entry = it.next() orelse return null;
    if (!std.mem.startsWith(u8, entry.key_ptr.*, "/")) return null;
    const range_object = switch (entry.value_ptr.*) {
        .object => |inner| inner,
        else => return null,
    };
    return algebraicDateRangeFromBounds(entry.key_ptr.*, range_object);
}

const AlgebraicRangeBound = struct {
    value: std.json.Value,
    inclusive: bool,
};

fn algebraicSetRangeBound(found: *?AlgebraicRangeBound, value: std.json.Value, inclusive: bool) ?void {
    if (found.* != null) return null;
    found.* = .{ .value = value, .inclusive = inclusive };
}

fn algebraicStandardRangeLowerBound(object: std.json.ObjectMap) ?AlgebraicRangeBound {
    var found: ?AlgebraicRangeBound = null;
    if (object.get("gt")) |value| algebraicSetRangeBound(&found, value, false) orelse return null;
    if (object.get("gte")) |value| algebraicSetRangeBound(&found, value, true) orelse return null;
    return found;
}

fn algebraicStandardRangeUpperBound(object: std.json.ObjectMap) ?AlgebraicRangeBound {
    var found: ?AlgebraicRangeBound = null;
    if (object.get("lt")) |value| algebraicSetRangeBound(&found, value, false) orelse return null;
    if (object.get("lte")) |value| algebraicSetRangeBound(&found, value, true) orelse return null;
    return found;
}

fn algebraicTermRangeFromBounds(path: []const u8, object: std.json.ObjectMap) ?AlgebraicPathTermRangePredicate {
    const lower = algebraicStandardRangeLowerBound(object);
    const upper = algebraicStandardRangeUpperBound(object);
    if (lower == null and upper == null) return null;
    if (lower) |bound| if (!algebraicStringJsonValue(bound.value)) return null;
    if (upper) |bound| if (!algebraicStringJsonValue(bound.value)) return null;
    return .{
        .path = path,
        .min = if (lower) |bound| algebraicOptionalString(bound.value) else null,
        .max = if (upper) |bound| algebraicOptionalString(bound.value) else null,
        .inclusive_min = if (lower) |bound| bound.inclusive else true,
        .inclusive_max = if (upper) |bound| bound.inclusive else false,
    };
}

fn algebraicDateRangeFromBounds(path: []const u8, object: std.json.ObjectMap) ?AlgebraicPathDateRangePredicate {
    const lower = algebraicStandardRangeLowerBound(object);
    const upper = algebraicStandardRangeUpperBound(object);
    if (lower == null and upper == null) return null;
    if (lower) |bound| if (!algebraicDateJsonValue(bound.value)) return null;
    if (upper) |bound| if (!algebraicDateJsonValue(bound.value)) return null;
    return .{
        .path = path,
        .start = if (lower) |bound| bound.value else null,
        .end = if (upper) |bound| bound.value else null,
        .inclusive_start = if (lower) |bound| bound.inclusive else true,
        .inclusive_end = if (upper) |bound| bound.inclusive else false,
    };
}

fn algebraicPathStandardNumericRangePredicate(value: std.json.Value) ?AlgebraicPathNumericRangePredicate {
    const object = switch (value) {
        .object => |object| object,
        else => return null,
    };
    if (object.get("field") != null or object.get("path") != null) {
        const path = if (object.get("path")) |path_value|
            algebraicJsonString(path_value) orelse return null
        else blk: {
            const field = algebraicJsonString(object.get("field").?) orelse return null;
            if (!std.mem.startsWith(u8, field, "/")) return null;
            break :blk field;
        };
        if (!std.mem.startsWith(u8, path, "/")) return null;
        const lower = algebraicStandardRangeLowerBound(object);
        const upper = algebraicStandardRangeUpperBound(object);
        if (lower == null and upper == null) return null;
        if (lower) |bound| if (!algebraicNumericJsonValue(bound.value)) return null;
        if (upper) |bound| if (!algebraicNumericJsonValue(bound.value)) return null;
        return .{
            .path = path,
            .min = if (lower) |bound| algebraicOptionalF64(bound.value) else null,
            .max = if (upper) |bound| algebraicOptionalF64(bound.value) else null,
            .inclusive_min = if (lower) |bound| bound.inclusive else true,
            .inclusive_max = if (upper) |bound| bound.inclusive else false,
        };
    }
    if (object.count() != 1) return null;
    var it = object.iterator();
    const entry = it.next() orelse return null;
    if (!std.mem.startsWith(u8, entry.key_ptr.*, "/")) return null;
    const range_object = switch (entry.value_ptr.*) {
        .object => |inner| inner,
        else => return null,
    };
    const lower = algebraicStandardRangeLowerBound(range_object);
    const upper = algebraicStandardRangeUpperBound(range_object);
    if (lower == null and upper == null) return null;
    if (lower) |bound| if (!algebraicNumericJsonValue(bound.value)) return null;
    if (upper) |bound| if (!algebraicNumericJsonValue(bound.value)) return null;
    return .{
        .path = entry.key_ptr.*,
        .min = if (lower) |bound| algebraicOptionalF64(bound.value) else null,
        .max = if (upper) |bound| algebraicOptionalF64(bound.value) else null,
        .inclusive_min = if (lower) |bound| bound.inclusive else true,
        .inclusive_max = if (upper) |bound| bound.inclusive else false,
    };
}

fn algebraicPathStandardDateRangePredicate(value: std.json.Value) ?AlgebraicPathDateRangePredicate {
    const object = switch (value) {
        .object => |object| object,
        else => return null,
    };
    if (object.get("field") != null or object.get("path") != null) {
        const path = if (object.get("path")) |path_value|
            algebraicJsonString(path_value) orelse return null
        else blk: {
            const field = algebraicJsonString(object.get("field").?) orelse return null;
            if (!std.mem.startsWith(u8, field, "/")) return null;
            break :blk field;
        };
        if (!std.mem.startsWith(u8, path, "/")) return null;
        return algebraicDateRangeFromBounds(path, object);
    }
    if (object.count() != 1) return null;
    var it = object.iterator();
    const entry = it.next() orelse return null;
    if (!std.mem.startsWith(u8, entry.key_ptr.*, "/")) return null;
    const range_object = switch (entry.value_ptr.*) {
        .object => |inner| inner,
        else => return null,
    };
    return algebraicDateRangeFromBounds(entry.key_ptr.*, range_object);
}

fn algebraicPathStandardTermRangePredicate(value: std.json.Value) ?AlgebraicPathTermRangePredicate {
    const object = switch (value) {
        .object => |object| object,
        else => return null,
    };
    if (object.get("field") != null or object.get("path") != null) {
        const path = if (object.get("path")) |path_value|
            algebraicJsonString(path_value) orelse return null
        else blk: {
            const field = algebraicJsonString(object.get("field").?) orelse return null;
            if (!std.mem.startsWith(u8, field, "/")) return null;
            break :blk field;
        };
        if (!std.mem.startsWith(u8, path, "/")) return null;
        return algebraicTermRangeFromBounds(path, object);
    }
    if (object.count() != 1) return null;
    var it = object.iterator();
    const entry = it.next() orelse return null;
    if (!std.mem.startsWith(u8, entry.key_ptr.*, "/")) return null;
    const range_object = switch (entry.value_ptr.*) {
        .object => |inner| inner,
        else => return null,
    };
    return algebraicTermRangeFromBounds(entry.key_ptr.*, range_object);
}

fn collectSingleValueTermsConstraint(
    alloc: std.mem.Allocator,
    terms: std.json.Value,
    out: *std.ArrayListUnmanaged(db_mod.aggregations.FixedConstraint),
) AlgebraicConstraintCollectError!bool {
    if (terms != .object) return false;
    if (terms.object.count() == 1) {
        var it = terms.object.iterator();
        const entry = it.next() orelse return false;
        return try collectTermsValuesConstraint(alloc, entry.key_ptr.*, entry.value_ptr.*, out);
    }
    const field = terms.object.get("path") orelse terms.object.get("field") orelse return false;
    const values = terms.object.get("values") orelse terms.object.get("terms") orelse return false;
    if (field != .string) return false;
    return try collectTermsValuesConstraint(alloc, field.string, values, out);
}

fn collectTermsValuesConstraint(
    alloc: std.mem.Allocator,
    field: []const u8,
    values: std.json.Value,
    out: *std.ArrayListUnmanaged(db_mod.aggregations.FixedConstraint),
) AlgebraicConstraintCollectError!bool {
    if (values != .array or values.array.items.len == 0) return false;
    if (!std.mem.startsWith(u8, field, "/")) {
        if (values.array.items.len != 1) return false;
        const value_text = try algebraicConstraintValueTextAlloc(alloc, field, values.array.items[0]);
        defer alloc.free(value_text);
        try appendAlgebraicConstraint(out, alloc, field, value_text);
        return true;
    }
    if (values.array.items.len == 1) {
        const value_text = try algebraicConstraintValueTextAlloc(alloc, field, values.array.items[0]);
        defer alloc.free(value_text);
        try appendAlgebraicConstraint(out, alloc, field, value_text);
        return true;
    }
    const typed_values = try alloc.alloc([]const u8, values.array.items.len);
    defer alloc.free(typed_values);
    var initialized: usize = 0;
    defer {
        for (typed_values[0..initialized]) |value| alloc.free(@constCast(value));
    }
    for (values.array.items, 0..) |item, i| {
        typed_values[i] = try algebraicConstraintValueTextAlloc(alloc, field, item);
        initialized += 1;
    }
    try appendAlgebraicTypedAnyConstraint(out, alloc, field, typed_values);
    return true;
}

fn appendAlgebraicTypedAnyConstraint(
    out: *std.ArrayListUnmanaged(db_mod.aggregations.FixedConstraint),
    alloc: std.mem.Allocator,
    field: []const u8,
    typed_values: []const []const u8,
) AlgebraicConstraintCollectError!void {
    if (typed_values.len == 0) return error.UnsupportedQueryRequest;
    if (typed_values.len == 1) {
        try appendAlgebraicConstraint(out, alloc, field, typed_values[0]);
        return;
    }
    const any_value = db_mod.algebraic.index.pathFactAnyConstraintValueAlloc(alloc, typed_values) catch return error.UnsupportedQueryRequest;
    defer alloc.free(any_value);
    try appendAlgebraicConstraint(out, alloc, field, any_value);
}

fn algebraicConstraintValueTextAlloc(alloc: std.mem.Allocator, field: []const u8, value: std.json.Value) AlgebraicConstraintCollectError![]u8 {
    const raw = switch (value) {
        .string => |text| try alloc.dupe(u8, text),
        .integer => |number| try std.fmt.allocPrint(alloc, "{d}", .{number}),
        .float => |number| try std.fmt.allocPrint(alloc, "{d}", .{number}),
        .bool => |flag| try alloc.dupe(u8, if (flag) "true" else "false"),
        .null => if (std.mem.startsWith(u8, field, "/")) try alloc.dupe(u8, "") else return error.UnsupportedQueryRequest,
        else => return error.UnsupportedQueryRequest,
    };
    errdefer alloc.free(raw);
    if (!std.mem.startsWith(u8, field, "/")) return raw;
    const kind = switch (value) {
        .string => "string",
        .integer, .float => "number",
        .bool => "bool",
        .null => "null",
        else => unreachable,
    };
    const typed = try db_mod.algebraic.token.canonicalTupleAlloc(alloc, &.{ kind, raw });
    alloc.free(raw);
    return typed;
}

fn algebraicConstraintBoolValueAlloc(alloc: std.mem.Allocator, field: []const u8, value: bool) AlgebraicConstraintCollectError![]u8 {
    const raw = if (value) "true" else "false";
    if (!std.mem.startsWith(u8, field, "/")) return try alloc.dupe(u8, raw);
    return try db_mod.algebraic.token.canonicalTupleAlloc(alloc, &.{ "bool", raw });
}

test "algebraic constraints accept scalar bool query and structured filters" {
    const alloc = std.testing.allocator;

    const from_bool = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .bool_field = .{ .field = "published", .value = true } },
    })).?;
    defer freeAlgebraicConstraints(alloc, from_bool);
    try std.testing.expectEqual(@as(usize, 1), from_bool.len);
    try std.testing.expectEqualStrings("published", from_bool[0].field);
    try std.testing.expectEqualStrings("true", from_bool[0].value);

    const bool_must_queries = [_]db_mod.types.TextQuery{
        .{ .term = .{ .field = "/tier", .term = "gold" } },
        .{ .prefix = .{ .field = "/tenant", .prefix = "ac" } },
    };
    const from_path_bool_query = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .full_text = .{ .bool_query = .{ .must = bool_must_queries[0..] } },
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_bool_query);
    try std.testing.expectEqual(@as(usize, 2), from_path_bool_query.len);
    try std.testing.expectEqualStrings("/tier", from_path_bool_query[0].field);
    const top_level_bool_term_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_bool_query[0].value);
    defer {
        for (top_level_bool_term_parts) |part| alloc.free(part);
        alloc.free(top_level_bool_term_parts);
    }
    try std.testing.expectEqual(@as(usize, 2), top_level_bool_term_parts.len);
    try std.testing.expectEqualStrings("string", top_level_bool_term_parts[0]);
    try std.testing.expectEqualStrings("gold", top_level_bool_term_parts[1]);
    try std.testing.expectEqualStrings("/tenant", from_path_bool_query[1].field);
    const top_level_bool_prefix_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_bool_query[1].value);
    defer {
        for (top_level_bool_prefix_parts) |part| alloc.free(part);
        alloc.free(top_level_bool_prefix_parts);
    }
    try std.testing.expectEqual(@as(usize, 3), top_level_bool_prefix_parts.len);
    try std.testing.expectEqualStrings("pathfact-prefix:v1", top_level_bool_prefix_parts[0]);
    try std.testing.expectEqualStrings("string", top_level_bool_prefix_parts[1]);
    try std.testing.expectEqualStrings("ac", top_level_bool_prefix_parts[2]);

    const bool_optional_should_queries = [_]db_mod.types.TextQuery{
        .{ .term = .{ .field = "/region", .term = "west" } },
    };
    const from_path_bool_optional_should_query = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .full_text = .{ .bool_query = .{
            .must = bool_must_queries[0..],
            .should = bool_optional_should_queries[0..],
            .min_should = 0,
        } },
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_bool_optional_should_query);
    try std.testing.expectEqual(@as(usize, 2), from_path_bool_optional_should_query.len);
    try std.testing.expectEqualStrings("/tier", from_path_bool_optional_should_query[0].field);
    try std.testing.expectEqualStrings("/tenant", from_path_bool_optional_should_query[1].field);

    const required_optional_should_query = try algebraicConstraintsForRequestAlloc(alloc, .{
        .full_text = .{ .bool_query = .{
            .must = bool_must_queries[0..],
            .should = bool_optional_should_queries[0..],
            .min_should = 1,
        } },
    });
    try std.testing.expect(required_optional_should_query == null);

    const bool_should_queries = [_]db_mod.types.TextQuery{
        .{ .term = .{ .field = "/tier", .term = "gold" } },
        .{ .term = .{ .field = "/tier", .term = "silver" } },
    };
    const top_level_should_query = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .full_text = .{ .bool_query = .{ .should = bool_should_queries[0..], .min_should = 1 } },
    })).?;
    defer freeAlgebraicConstraints(alloc, top_level_should_query);
    try std.testing.expectEqual(@as(usize, 1), top_level_should_query.len);
    try std.testing.expectEqualStrings("/tier", top_level_should_query[0].field);
    const top_level_should_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, top_level_should_query[0].value);
    defer {
        for (top_level_should_parts) |part| alloc.free(part);
        alloc.free(top_level_should_parts);
    }
    try std.testing.expectEqual(@as(usize, 3), top_level_should_parts.len);
    try std.testing.expectEqualStrings("pathfact-any:v1", top_level_should_parts[0]);

    const implicit_top_level_should_query = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .full_text = .{ .bool_query = .{ .should = bool_should_queries[0..] } },
    })).?;
    defer freeAlgebraicConstraints(alloc, implicit_top_level_should_query);
    try std.testing.expectEqual(@as(usize, 1), implicit_top_level_should_query.len);
    try std.testing.expectEqualStrings("/tier", implicit_top_level_should_query[0].field);
    const implicit_top_level_should_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, implicit_top_level_should_query[0].value);
    defer {
        for (implicit_top_level_should_parts) |part| alloc.free(part);
        alloc.free(implicit_top_level_should_parts);
    }
    try std.testing.expectEqual(@as(usize, 3), implicit_top_level_should_parts.len);
    try std.testing.expectEqualStrings("pathfact-any:v1", implicit_top_level_should_parts[0]);

    const two_required_should_query = try algebraicConstraintsForRequestAlloc(alloc, .{
        .full_text = .{ .bool_query = .{ .should = bool_should_queries[0..], .min_should = 2 } },
    });
    try std.testing.expect(two_required_should_query == null);

    const mixed_field_should_queries = [_]db_mod.types.TextQuery{
        .{ .term = .{ .field = "/tier", .term = "gold" } },
        .{ .term = .{ .field = "/region", .term = "west" } },
    };
    const mixed_field_should_query = try algebraicConstraintsForRequestAlloc(alloc, .{
        .full_text = .{ .bool_query = .{ .should = mixed_field_should_queries[0..], .min_should = 1 } },
    });
    try std.testing.expect(mixed_field_should_query == null);

    const bool_must_not_queries = [_]db_mod.types.TextQuery{.{ .term = .{ .field = "/tier", .term = "gold" } }};
    const top_level_must_not_query = try algebraicConstraintsForRequestAlloc(alloc, .{
        .full_text = .{ .bool_query = .{ .must_not = bool_must_not_queries[0..] } },
    });
    try std.testing.expect(top_level_must_not_query == null);

    const from_path_term_query = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .term = .{ .field = "/tier", .term = "gold" } },
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_term_query);
    try std.testing.expectEqual(@as(usize, 1), from_path_term_query.len);
    try std.testing.expectEqualStrings("/tier", from_path_term_query[0].field);
    const top_level_term_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_term_query[0].value);
    defer {
        for (top_level_term_parts) |part| alloc.free(part);
        alloc.free(top_level_term_parts);
    }
    try std.testing.expectEqual(@as(usize, 2), top_level_term_parts.len);
    try std.testing.expectEqualStrings("string", top_level_term_parts[0]);
    try std.testing.expectEqualStrings("gold", top_level_term_parts[1]);

    const from_path_match_query = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .match = .{ .field = "/tier", .text = "OLD" } },
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_match_query);
    try std.testing.expectEqual(@as(usize, 1), from_path_match_query.len);
    try std.testing.expectEqualStrings("/tier", from_path_match_query[0].field);
    const top_level_match_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_match_query[0].value);
    defer {
        for (top_level_match_parts) |part| alloc.free(part);
        alloc.free(top_level_match_parts);
    }
    try std.testing.expectEqual(@as(usize, 3), top_level_match_parts.len);
    try std.testing.expectEqualStrings("pathfact-match:v1", top_level_match_parts[0]);
    try std.testing.expectEqualStrings("string", top_level_match_parts[1]);
    try std.testing.expectEqualStrings("OLD", top_level_match_parts[2]);

    const analyzed_path_match_query = try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .match = .{ .field = "/tier", .text = "gold", .analyzer = "default" } },
    });
    try std.testing.expect(analyzed_path_match_query == null);

    const from_path_fuzzy_query = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .fuzzy = .{ .field = "/tier", .term = "gild", .max_edits = 1, .prefix_len = 1 } },
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_fuzzy_query);
    try std.testing.expectEqual(@as(usize, 1), from_path_fuzzy_query.len);
    try std.testing.expectEqualStrings("/tier", from_path_fuzzy_query[0].field);
    const top_level_fuzzy_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_fuzzy_query[0].value);
    defer {
        for (top_level_fuzzy_parts) |part| alloc.free(part);
        alloc.free(top_level_fuzzy_parts);
    }
    try std.testing.expectEqual(@as(usize, 5), top_level_fuzzy_parts.len);
    try std.testing.expectEqualStrings("pathfact-fuzzy:v1", top_level_fuzzy_parts[0]);
    try std.testing.expectEqualStrings("string", top_level_fuzzy_parts[1]);
    try std.testing.expectEqualStrings("gild", top_level_fuzzy_parts[2]);
    try std.testing.expectEqualStrings("1", top_level_fuzzy_parts[3]);
    try std.testing.expectEqualStrings("1", top_level_fuzzy_parts[4]);

    const unbounded_path_fuzzy_query = try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .fuzzy = .{ .field = "/tier", .term = "gold", .max_edits = 1 } },
    });
    try std.testing.expect(unbounded_path_fuzzy_query == null);

    const auto_path_fuzzy_query = try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .fuzzy = .{ .field = "/tier", .term = "gold", .auto_fuzzy = true, .prefix_len = 1 } },
    });
    try std.testing.expect(auto_path_fuzzy_query == null);

    const non_path_fuzzy_query = try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .fuzzy = .{ .field = "tier", .term = "gold", .max_edits = 1, .prefix_len = 1 } },
    });
    try std.testing.expect(non_path_fuzzy_query == null);

    const from_path_prefix_query = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .prefix = .{ .field = "/tier", .prefix = "go" } },
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_prefix_query);
    try std.testing.expectEqual(@as(usize, 1), from_path_prefix_query.len);
    try std.testing.expectEqualStrings("/tier", from_path_prefix_query[0].field);
    const top_level_prefix_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_prefix_query[0].value);
    defer {
        for (top_level_prefix_parts) |part| alloc.free(part);
        alloc.free(top_level_prefix_parts);
    }
    try std.testing.expectEqual(@as(usize, 3), top_level_prefix_parts.len);
    try std.testing.expectEqualStrings("pathfact-prefix:v1", top_level_prefix_parts[0]);
    try std.testing.expectEqualStrings("string", top_level_prefix_parts[1]);
    try std.testing.expectEqualStrings("go", top_level_prefix_parts[2]);

    const non_path_prefix_query = try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .prefix = .{ .field = "tier", .prefix = "go" } },
    });
    try std.testing.expect(non_path_prefix_query == null);

    const from_path_wildcard_query = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .wildcard = .{ .field = "/tier", .pattern = "go*" } },
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_wildcard_query);
    try std.testing.expectEqual(@as(usize, 1), from_path_wildcard_query.len);
    try std.testing.expectEqualStrings("/tier", from_path_wildcard_query[0].field);
    const top_level_wildcard_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_wildcard_query[0].value);
    defer {
        for (top_level_wildcard_parts) |part| alloc.free(part);
        alloc.free(top_level_wildcard_parts);
    }
    try std.testing.expectEqual(@as(usize, 3), top_level_wildcard_parts.len);
    try std.testing.expectEqualStrings("pathfact-wildcard:v1", top_level_wildcard_parts[0]);
    try std.testing.expectEqualStrings("string", top_level_wildcard_parts[1]);
    try std.testing.expectEqualStrings("go*", top_level_wildcard_parts[2]);

    const leading_wildcard_query = try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .wildcard = .{ .field = "/tier", .pattern = "*old" } },
    });
    try std.testing.expect(leading_wildcard_query == null);

    const non_path_wildcard_query = try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .wildcard = .{ .field = "tier", .pattern = "go*" } },
    });
    try std.testing.expect(non_path_wildcard_query == null);

    const from_path_regexp_query = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .regexp = .{ .field = "/tier", .pattern = "go.*" } },
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_regexp_query);
    try std.testing.expectEqual(@as(usize, 1), from_path_regexp_query.len);
    try std.testing.expectEqualStrings("/tier", from_path_regexp_query[0].field);
    const top_level_regexp_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_regexp_query[0].value);
    defer {
        for (top_level_regexp_parts) |part| alloc.free(part);
        alloc.free(top_level_regexp_parts);
    }
    try std.testing.expectEqual(@as(usize, 3), top_level_regexp_parts.len);
    try std.testing.expectEqualStrings("pathfact-regexp:v1", top_level_regexp_parts[0]);
    try std.testing.expectEqualStrings("string", top_level_regexp_parts[1]);
    try std.testing.expectEqualStrings("go.*", top_level_regexp_parts[2]);

    const leading_regexp_query = try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .regexp = .{ .field = "/tier", .pattern = ".*old" } },
    });
    try std.testing.expect(leading_regexp_query == null);

    const invalid_regexp_query = try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .regexp = .{ .field = "/tier", .pattern = "go(" } },
    });
    try std.testing.expect(invalid_regexp_query == null);

    const non_path_regexp_query = try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .regexp = .{ .field = "tier", .pattern = "go.*" } },
    });
    try std.testing.expect(non_path_regexp_query == null);

    const from_path_numeric_query = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .numeric_range = .{ .field = "/amount", .min = 10, .max = 30, .inclusive_min = true, .inclusive_max = false } },
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_numeric_query);
    try std.testing.expectEqual(@as(usize, 1), from_path_numeric_query.len);
    try std.testing.expectEqualStrings("/amount", from_path_numeric_query[0].field);
    const top_level_amount_range_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_numeric_query[0].value);
    defer {
        for (top_level_amount_range_parts) |part| alloc.free(part);
        alloc.free(top_level_amount_range_parts);
    }
    try std.testing.expectEqual(@as(usize, 6), top_level_amount_range_parts.len);
    try std.testing.expectEqualStrings("pathfact-numeric-range:v1", top_level_amount_range_parts[0]);
    try std.testing.expectEqualStrings("number", top_level_amount_range_parts[1]);
    try std.testing.expectEqualStrings("10", top_level_amount_range_parts[2]);
    try std.testing.expectEqualStrings("30", top_level_amount_range_parts[3]);
    try std.testing.expectEqualStrings("1", top_level_amount_range_parts[4]);
    try std.testing.expectEqualStrings("0", top_level_amount_range_parts[5]);

    const non_path_numeric_query = try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .numeric_range = .{ .field = "amount", .min = 10 } },
    });
    try std.testing.expect(non_path_numeric_query == null);

    const from_path_term_range_query = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .term_range = .{ .field = "/status", .min = "active", .max = "archived", .inclusive_min = true, .inclusive_max = false } },
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_term_range_query);
    try std.testing.expectEqual(@as(usize, 1), from_path_term_range_query.len);
    try std.testing.expectEqualStrings("/status", from_path_term_range_query[0].field);
    const top_level_term_range_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_term_range_query[0].value);
    defer {
        for (top_level_term_range_parts) |part| alloc.free(part);
        alloc.free(top_level_term_range_parts);
    }
    try std.testing.expectEqual(@as(usize, 6), top_level_term_range_parts.len);
    try std.testing.expectEqualStrings("pathfact-term-range:v1", top_level_term_range_parts[0]);
    try std.testing.expectEqualStrings("string", top_level_term_range_parts[1]);
    try std.testing.expectEqualStrings("active", top_level_term_range_parts[2]);
    try std.testing.expectEqualStrings("archived", top_level_term_range_parts[3]);
    try std.testing.expectEqualStrings("1", top_level_term_range_parts[4]);
    try std.testing.expectEqualStrings("0", top_level_term_range_parts[5]);

    const unbounded_term_range_query = try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .term_range = .{ .field = "/status" } },
    });
    try std.testing.expect(unbounded_term_range_query == null);

    const non_path_term_range_query = try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .term_range = .{ .field = "status", .min = "active" } },
    });
    try std.testing.expect(non_path_term_range_query == null);

    const from_path_ip_range_query = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .ip_range = .{ .field = "/client_ip", .cidr = "10.1.0.0/16" } },
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_ip_range_query);
    try std.testing.expectEqual(@as(usize, 1), from_path_ip_range_query.len);
    try std.testing.expectEqualStrings("/client_ip", from_path_ip_range_query[0].field);
    const top_level_ip_range_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_ip_range_query[0].value);
    defer {
        for (top_level_ip_range_parts) |part| alloc.free(part);
        alloc.free(top_level_ip_range_parts);
    }
    try std.testing.expectEqual(@as(usize, 3), top_level_ip_range_parts.len);
    try std.testing.expectEqualStrings("pathfact-ip-range:v1", top_level_ip_range_parts[0]);
    try std.testing.expectEqualStrings("ipv4", top_level_ip_range_parts[1]);
    try std.testing.expectEqualStrings("10.1.0.0/16", top_level_ip_range_parts[2]);

    const invalid_ip_range_query = try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .ip_range = .{ .field = "/client_ip", .cidr = "10.999.0.0/16" } },
    });
    try std.testing.expect(invalid_ip_range_query == null);

    const non_path_ip_range_query = try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .ip_range = .{ .field = "client_ip", .cidr = "10.1.0.0/16" } },
    });
    try std.testing.expect(non_path_ip_range_query == null);

    const from_path_geo_bbox_query = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .geo_bbox = .{ .field = "/location", .min_lat = 37.70, .min_lon = -122.50, .max_lat = 37.80, .max_lon = -122.30 } },
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_geo_bbox_query);
    try std.testing.expectEqual(@as(usize, 1), from_path_geo_bbox_query.len);
    try std.testing.expectEqualStrings("/location", from_path_geo_bbox_query[0].field);
    const top_level_geo_bbox_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_geo_bbox_query[0].value);
    defer {
        for (top_level_geo_bbox_parts) |part| alloc.free(part);
        alloc.free(top_level_geo_bbox_parts);
    }
    try std.testing.expectEqual(@as(usize, 6), top_level_geo_bbox_parts.len);
    try std.testing.expectEqualStrings("pathfact-geo-bbox:v1", top_level_geo_bbox_parts[0]);
    try std.testing.expectEqualStrings("geo_point", top_level_geo_bbox_parts[1]);
    try std.testing.expectEqualStrings("37.7", top_level_geo_bbox_parts[2]);
    try std.testing.expectEqualStrings("-122.5", top_level_geo_bbox_parts[3]);
    try std.testing.expectEqualStrings("37.8", top_level_geo_bbox_parts[4]);
    try std.testing.expectEqualStrings("-122.3", top_level_geo_bbox_parts[5]);

    const from_path_geo_distance_query = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .geo_distance = .{ .field = "/location", .lat = 37.7749, .lon = -122.4194, .radius_meters = 2000 } },
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_geo_distance_query);
    try std.testing.expectEqual(@as(usize, 1), from_path_geo_distance_query.len);
    try std.testing.expectEqualStrings("/location", from_path_geo_distance_query[0].field);
    const top_level_geo_distance_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_geo_distance_query[0].value);
    defer {
        for (top_level_geo_distance_parts) |part| alloc.free(part);
        alloc.free(top_level_geo_distance_parts);
    }
    try std.testing.expectEqual(@as(usize, 5), top_level_geo_distance_parts.len);
    try std.testing.expectEqualStrings("pathfact-geo-distance:v1", top_level_geo_distance_parts[0]);
    try std.testing.expectEqualStrings("geo_point", top_level_geo_distance_parts[1]);
    try std.testing.expectEqualStrings("37.7749", top_level_geo_distance_parts[2]);
    try std.testing.expectEqualStrings("-122.4194", top_level_geo_distance_parts[3]);
    try std.testing.expectEqualStrings("2000", top_level_geo_distance_parts[4]);

    const direct_shape_polygon = [_]db_mod.types.GeoPoint{
        .{ .lat = 37.0, .lon = -123.0 },
        .{ .lat = 38.0, .lon = -123.0 },
        .{ .lat = 38.0, .lon = -122.0 },
        .{ .lat = 37.0, .lon = -122.0 },
    };
    const direct_shape_polygons = [_][]const db_mod.types.GeoPoint{direct_shape_polygon[0..]};
    const from_path_geo_shape_query = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .geo_shape = .{
            .field = "/location",
            .relation = .intersects,
            .polygons = direct_shape_polygons[0..],
        } },
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_geo_shape_query);
    try std.testing.expectEqual(@as(usize, 1), from_path_geo_shape_query.len);
    try std.testing.expectEqualStrings("/location", from_path_geo_shape_query[0].field);
    const top_level_geo_shape_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_geo_shape_query[0].value);
    defer {
        for (top_level_geo_shape_parts) |part| alloc.free(part);
        alloc.free(top_level_geo_shape_parts);
    }
    try std.testing.expectEqual(@as(usize, 13), top_level_geo_shape_parts.len);
    try std.testing.expectEqualStrings("pathfact-geo-shape:v1", top_level_geo_shape_parts[0]);
    try std.testing.expectEqualStrings("geo_point", top_level_geo_shape_parts[1]);
    try std.testing.expectEqualStrings("intersects", top_level_geo_shape_parts[2]);
    try std.testing.expectEqualStrings("1", top_level_geo_shape_parts[3]);
    try std.testing.expectEqualStrings("4", top_level_geo_shape_parts[4]);

    const contains_geo_shape_query = try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .geo_shape = .{
            .field = "/location",
            .relation = .contains,
            .polygons = direct_shape_polygons[0..],
        } },
    });
    try std.testing.expect(contains_geo_shape_query == null);

    const non_path_geo_query = try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .geo_bbox = .{ .field = "location", .min_lat = 37.70, .min_lon = -122.50, .max_lat = 37.80, .max_lon = -122.30 } },
    });
    try std.testing.expect(non_path_geo_query == null);

    const from_path_date_query = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .date_range = .{ .field = "/published_at", .start_ns = 1767225600000000000, .end_ns = 1767312000000000000, .inclusive_start = true, .inclusive_end = false } },
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_date_query);
    try std.testing.expectEqual(@as(usize, 1), from_path_date_query.len);
    try std.testing.expectEqualStrings("/published_at", from_path_date_query[0].field);
    const top_level_date_range_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_date_query[0].value);
    defer {
        for (top_level_date_range_parts) |part| alloc.free(part);
        alloc.free(top_level_date_range_parts);
    }
    try std.testing.expectEqual(@as(usize, 6), top_level_date_range_parts.len);
    try std.testing.expectEqualStrings("pathfact-date-range:v1", top_level_date_range_parts[0]);
    try std.testing.expectEqualStrings("datetime", top_level_date_range_parts[1]);
    try std.testing.expectEqualStrings("1767225600000000000", top_level_date_range_parts[2]);
    try std.testing.expectEqualStrings("1767312000000000000", top_level_date_range_parts[3]);
    try std.testing.expectEqualStrings("1", top_level_date_range_parts[4]);
    try std.testing.expectEqualStrings("0", top_level_date_range_parts[5]);

    const non_path_date_query = try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .date_range = .{ .field = "published_at", .start_ns = 1767225600000000000 } },
    });
    try std.testing.expect(non_path_date_query == null);

    const from_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"bool\":{\"must\":[{\"term\":{\"tenant\":\"t1\"}},{\"bool_field\":{\"field\":\"paid\",\"value\":false}}]}}",
    })).?;
    defer freeAlgebraicConstraints(alloc, from_filter);
    try std.testing.expectEqual(@as(usize, 2), from_filter.len);
    try std.testing.expectEqualStrings("tenant", from_filter[0].field);
    try std.testing.expectEqualStrings("t1", from_filter[0].value);
    try std.testing.expectEqualStrings("paid", from_filter[1].field);
    try std.testing.expectEqualStrings("false", from_filter[1].value);

    const from_numeric_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"term\":{\"amount\":42}}",
    })).?;
    defer freeAlgebraicConstraints(alloc, from_numeric_filter);
    try std.testing.expectEqual(@as(usize, 1), from_numeric_filter.len);
    try std.testing.expectEqualStrings("amount", from_numeric_filter[0].field);
    try std.testing.expectEqualStrings("42", from_numeric_filter[0].value);

    const from_path_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"bool\":{\"filter\":[{\"term\":{\"/active\":true}},{\"term\":{\"/tier\":\"gold\"}},{\"term\":{\"/deleted_at\":null}}]}}",
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_filter);
    try std.testing.expectEqual(@as(usize, 3), from_path_filter.len);
    try std.testing.expectEqualStrings("/active", from_path_filter[0].field);
    const active_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_filter[0].value);
    defer {
        for (active_parts) |part| alloc.free(part);
        alloc.free(active_parts);
    }
    try std.testing.expectEqual(@as(usize, 2), active_parts.len);
    try std.testing.expectEqualStrings("bool", active_parts[0]);
    try std.testing.expectEqualStrings("true", active_parts[1]);
    try std.testing.expectEqualStrings("/tier", from_path_filter[1].field);
    const tier_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_filter[1].value);
    defer {
        for (tier_parts) |part| alloc.free(part);
        alloc.free(tier_parts);
    }
    try std.testing.expectEqual(@as(usize, 2), tier_parts.len);
    try std.testing.expectEqualStrings("string", tier_parts[0]);
    try std.testing.expectEqualStrings("gold", tier_parts[1]);
    try std.testing.expectEqualStrings("/deleted_at", from_path_filter[2].field);
    const null_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_filter[2].value);
    defer {
        for (null_parts) |part| alloc.free(part);
        alloc.free(null_parts);
    }
    try std.testing.expectEqual(@as(usize, 2), null_parts.len);
    try std.testing.expectEqualStrings("null", null_parts[0]);
    try std.testing.expectEqualStrings("", null_parts[1]);

    const from_direct_path_term_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"term\":\"gold\",\"field\":\"/tier\"}",
    })).?;
    defer freeAlgebraicConstraints(alloc, from_direct_path_term_filter);
    try std.testing.expectEqual(@as(usize, 1), from_direct_path_term_filter.len);
    try std.testing.expectEqualStrings("/tier", from_direct_path_term_filter[0].field);
    const direct_path_term_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_direct_path_term_filter[0].value);
    defer {
        for (direct_path_term_parts) |part| alloc.free(part);
        alloc.free(direct_path_term_parts);
    }
    try std.testing.expectEqual(@as(usize, 2), direct_path_term_parts.len);
    try std.testing.expectEqualStrings("string", direct_path_term_parts[0]);
    try std.testing.expectEqualStrings("gold", direct_path_term_parts[1]);

    const from_direct_path_alias_term_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"term\":\"gold\",\"path\":\"/tier\"}",
    })).?;
    defer freeAlgebraicConstraints(alloc, from_direct_path_alias_term_filter);
    try std.testing.expectEqual(@as(usize, 1), from_direct_path_alias_term_filter.len);
    try std.testing.expectEqualStrings("/tier", from_direct_path_alias_term_filter[0].field);
    const direct_path_alias_term_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_direct_path_alias_term_filter[0].value);
    defer {
        for (direct_path_alias_term_parts) |part| alloc.free(part);
        alloc.free(direct_path_alias_term_parts);
    }
    try std.testing.expectEqual(@as(usize, 2), direct_path_alias_term_parts.len);
    try std.testing.expectEqualStrings("string", direct_path_alias_term_parts[0]);
    try std.testing.expectEqualStrings("gold", direct_path_alias_term_parts[1]);

    const from_field_value_path_term_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"term\":{\"field\":\"/tier\",\"value\":\"gold\"}}",
    })).?;
    defer freeAlgebraicConstraints(alloc, from_field_value_path_term_filter);
    try std.testing.expectEqual(@as(usize, 1), from_field_value_path_term_filter.len);
    try std.testing.expectEqualStrings("/tier", from_field_value_path_term_filter[0].field);
    const field_value_path_term_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_field_value_path_term_filter[0].value);
    defer {
        for (field_value_path_term_parts) |part| alloc.free(part);
        alloc.free(field_value_path_term_parts);
    }
    try std.testing.expectEqual(@as(usize, 2), field_value_path_term_parts.len);
    try std.testing.expectEqualStrings("string", field_value_path_term_parts[0]);
    try std.testing.expectEqualStrings("gold", field_value_path_term_parts[1]);

    const from_wrapped_path_alias_term_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"term\":{\"path\":\"/tier\",\"value\":\"gold\"}}",
    })).?;
    defer freeAlgebraicConstraints(alloc, from_wrapped_path_alias_term_filter);
    try std.testing.expectEqual(@as(usize, 1), from_wrapped_path_alias_term_filter.len);
    try std.testing.expectEqualStrings("/tier", from_wrapped_path_alias_term_filter[0].field);

    const from_terms_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"bool\":{\"filter\":{\"terms\":{\"tenant\":[\"t1\"]}},\"must\":{\"terms\":{\"field\":\"region\",\"values\":[\"west\"]}}}}",
    })).?;
    defer freeAlgebraicConstraints(alloc, from_terms_filter);
    try std.testing.expectEqual(@as(usize, 2), from_terms_filter.len);
    var saw_tenant_terms = false;
    var saw_region_terms = false;
    for (from_terms_filter) |constraint| {
        if (std.mem.eql(u8, constraint.field, "tenant") and std.mem.eql(u8, constraint.value, "t1")) saw_tenant_terms = true;
        if (std.mem.eql(u8, constraint.field, "region") and std.mem.eql(u8, constraint.value, "west")) saw_region_terms = true;
    }
    try std.testing.expect(saw_tenant_terms);
    try std.testing.expect(saw_region_terms);

    const from_optional_should_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"bool\":{\"filter\":{\"terms\":{\"tenant\":[\"t1\"]}},\"should\":[{\"term\":{\"region\":\"west\"}}]}}",
    })).?;
    defer freeAlgebraicConstraints(alloc, from_optional_should_filter);
    try std.testing.expectEqual(@as(usize, 1), from_optional_should_filter.len);
    try std.testing.expectEqualStrings("tenant", from_optional_should_filter[0].field);
    try std.testing.expectEqualStrings("t1", from_optional_should_filter[0].value);

    const from_explicit_optional_should_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"bool\":{\"must\":{\"terms\":{\"field\":\"region\",\"values\":[\"west\"]}},\"should\":[{\"term\":{\"tenant\":\"t1\"}}],\"minimum_should_match\":0}}",
    })).?;
    defer freeAlgebraicConstraints(alloc, from_explicit_optional_should_filter);
    try std.testing.expectEqual(@as(usize, 1), from_explicit_optional_should_filter.len);
    try std.testing.expectEqualStrings("region", from_explicit_optional_should_filter[0].field);
    try std.testing.expectEqualStrings("west", from_explicit_optional_should_filter[0].value);

    const unsupported_required_optional_should_filter = try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"bool\":{\"must\":{\"terms\":{\"field\":\"region\",\"values\":[\"west\"]}},\"should\":[{\"term\":{\"tenant\":\"t1\"}}],\"minimum_should_match\":1}}",
    });
    try std.testing.expect(unsupported_required_optional_should_filter == null);

    const from_path_should_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"bool\":{\"should\":[{\"term\":{\"/tier\":\"gold\"}},{\"term\":{\"/tier\":\"silver\"}}],\"minimum_should_match\":1}}",
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_should_filter);
    try std.testing.expectEqual(@as(usize, 1), from_path_should_filter.len);
    try std.testing.expectEqualStrings("/tier", from_path_should_filter[0].field);
    const filter_should_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_should_filter[0].value);
    defer {
        for (filter_should_parts) |part| alloc.free(part);
        alloc.free(filter_should_parts);
    }
    try std.testing.expectEqual(@as(usize, 3), filter_should_parts.len);
    try std.testing.expectEqualStrings("pathfact-any:v1", filter_should_parts[0]);

    const from_path_disjuncts_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"disjuncts\":[{\"term\":{\"/tier\":\"gold\"}},{\"term\":{\"/tier\":\"bronze\"}}]}",
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_disjuncts_filter);
    try std.testing.expectEqual(@as(usize, 1), from_path_disjuncts_filter.len);
    try std.testing.expectEqualStrings("/tier", from_path_disjuncts_filter[0].field);
    const disjuncts_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_disjuncts_filter[0].value);
    defer {
        for (disjuncts_parts) |part| alloc.free(part);
        alloc.free(disjuncts_parts);
    }
    try std.testing.expectEqual(@as(usize, 3), disjuncts_parts.len);
    try std.testing.expectEqualStrings("pathfact-any:v1", disjuncts_parts[0]);

    const from_direct_path_disjuncts_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"disjuncts\":[{\"term\":\"gold\",\"field\":\"/tier\"},{\"term\":\"bronze\",\"field\":\"/tier\"}]}",
    })).?;
    defer freeAlgebraicConstraints(alloc, from_direct_path_disjuncts_filter);
    try std.testing.expectEqual(@as(usize, 1), from_direct_path_disjuncts_filter.len);
    try std.testing.expectEqualStrings("/tier", from_direct_path_disjuncts_filter[0].field);
    const direct_disjuncts_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_direct_path_disjuncts_filter[0].value);
    defer {
        for (direct_disjuncts_parts) |part| alloc.free(part);
        alloc.free(direct_disjuncts_parts);
    }
    try std.testing.expectEqual(@as(usize, 3), direct_disjuncts_parts.len);
    try std.testing.expectEqualStrings("pathfact-any:v1", direct_disjuncts_parts[0]);

    const from_field_value_path_disjuncts_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"disjuncts\":[{\"term\":{\"field\":\"/tier\",\"term\":\"gold\"}},{\"term\":{\"field\":\"/tier\",\"value\":\"bronze\"}}]}",
    })).?;
    defer freeAlgebraicConstraints(alloc, from_field_value_path_disjuncts_filter);
    try std.testing.expectEqual(@as(usize, 1), from_field_value_path_disjuncts_filter.len);
    try std.testing.expectEqualStrings("/tier", from_field_value_path_disjuncts_filter[0].field);
    const field_value_disjuncts_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_field_value_path_disjuncts_filter[0].value);
    defer {
        for (field_value_disjuncts_parts) |part| alloc.free(part);
        alloc.free(field_value_disjuncts_parts);
    }
    try std.testing.expectEqual(@as(usize, 3), field_value_disjuncts_parts.len);
    try std.testing.expectEqualStrings("pathfact-any:v1", field_value_disjuncts_parts[0]);

    const from_path_alias_disjuncts_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"disjuncts\":[{\"term\":\"gold\",\"path\":\"/tier\"},{\"term\":{\"path\":\"/tier\",\"value\":\"bronze\"}}]}",
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_alias_disjuncts_filter);
    try std.testing.expectEqual(@as(usize, 1), from_path_alias_disjuncts_filter.len);
    try std.testing.expectEqualStrings("/tier", from_path_alias_disjuncts_filter[0].field);
    const path_alias_disjuncts_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_alias_disjuncts_filter[0].value);
    defer {
        for (path_alias_disjuncts_parts) |part| alloc.free(part);
        alloc.free(path_alias_disjuncts_parts);
    }
    try std.testing.expectEqual(@as(usize, 3), path_alias_disjuncts_parts.len);
    try std.testing.expectEqualStrings("pathfact-any:v1", path_alias_disjuncts_parts[0]);

    const mixed_path_should_filter = try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"bool\":{\"should\":[{\"term\":{\"/tier\":\"gold\"}},{\"term\":{\"/region\":\"west\"}}],\"minimum_should_match\":1}}",
    });
    try std.testing.expect(mixed_path_should_filter == null);

    const from_path_terms_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"terms\":{\"path\":\"/tier\",\"values\":[\"gold\",\"silver\",null]}}",
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_terms_filter);
    try std.testing.expectEqual(@as(usize, 1), from_path_terms_filter.len);
    try std.testing.expectEqualStrings("/tier", from_path_terms_filter[0].field);
    const any_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_terms_filter[0].value);
    defer {
        for (any_parts) |part| alloc.free(part);
        alloc.free(any_parts);
    }
    try std.testing.expectEqual(@as(usize, 4), any_parts.len);
    try std.testing.expectEqualStrings("pathfact-any:v1", any_parts[0]);
    const any_first = try db_mod.algebraic.token.decodeTupleAlloc(alloc, any_parts[1]);
    defer {
        for (any_first) |part| alloc.free(part);
        alloc.free(any_first);
    }
    try std.testing.expectEqualStrings("string", any_first[0]);
    try std.testing.expectEqualStrings("gold", any_first[1]);
    const any_null = try db_mod.algebraic.token.decodeTupleAlloc(alloc, any_parts[3]);
    defer {
        for (any_null) |part| alloc.free(part);
        alloc.free(any_null);
    }
    try std.testing.expectEqualStrings("null", any_null[0]);
    try std.testing.expectEqualStrings("", any_null[1]);

    const non_path_multi_terms_filter = try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"terms\":{\"tenant\":[\"t1\",\"t2\"]}}",
    });
    try std.testing.expect(non_path_multi_terms_filter == null);

    const from_path_exists_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"bool\":{\"filter\":[{\"exists\":{\"path\":\"/metadata/tier\"}},{\"exists\":\"/tenant\"}]}}",
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_exists_filter);
    try std.testing.expectEqual(@as(usize, 2), from_path_exists_filter.len);
    try std.testing.expectEqualStrings("/metadata/tier", from_path_exists_filter[0].field);
    try std.testing.expectEqualStrings(db_mod.algebraic.index.path_fact_exists_constraint_value, from_path_exists_filter[0].value);
    try std.testing.expectEqualStrings("/tenant", from_path_exists_filter[1].field);
    try std.testing.expectEqualStrings(db_mod.algebraic.index.path_fact_exists_constraint_value, from_path_exists_filter[1].value);

    const from_path_prefix_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"bool\":{\"filter\":[{\"prefix\":{\"path\":\"/metadata/tier\",\"prefix\":\"go\"}},{\"prefix\":{\"/tenant\":\"ac\"}}]}}",
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_prefix_filter);
    try std.testing.expectEqual(@as(usize, 2), from_path_prefix_filter.len);
    try std.testing.expectEqualStrings("/metadata/tier", from_path_prefix_filter[0].field);
    const tier_prefix_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_prefix_filter[0].value);
    defer {
        for (tier_prefix_parts) |part| alloc.free(part);
        alloc.free(tier_prefix_parts);
    }
    try std.testing.expectEqual(@as(usize, 3), tier_prefix_parts.len);
    try std.testing.expectEqualStrings("pathfact-prefix:v1", tier_prefix_parts[0]);
    try std.testing.expectEqualStrings("string", tier_prefix_parts[1]);
    try std.testing.expectEqualStrings("go", tier_prefix_parts[2]);
    try std.testing.expectEqualStrings("/tenant", from_path_prefix_filter[1].field);
    const tenant_prefix_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_prefix_filter[1].value);
    defer {
        for (tenant_prefix_parts) |part| alloc.free(part);
        alloc.free(tenant_prefix_parts);
    }
    try std.testing.expectEqual(@as(usize, 3), tenant_prefix_parts.len);
    try std.testing.expectEqualStrings("pathfact-prefix:v1", tenant_prefix_parts[0]);
    try std.testing.expectEqualStrings("string", tenant_prefix_parts[1]);
    try std.testing.expectEqualStrings("ac", tenant_prefix_parts[2]);

    const non_path_prefix_filter = try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"prefix\":{\"tenant\":\"ac\"}}",
    });
    try std.testing.expect(non_path_prefix_filter == null);

    const from_path_match_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"bool\":{\"filter\":[{\"match\":{\"path\":\"/metadata/tier\",\"value\":\"OLD\"}},{\"match\":{\"/tenant\":\"ICE\"}}]}}",
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_match_filter);
    try std.testing.expectEqual(@as(usize, 2), from_path_match_filter.len);
    try std.testing.expectEqualStrings("/metadata/tier", from_path_match_filter[0].field);
    const tier_match_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_match_filter[0].value);
    defer {
        for (tier_match_parts) |part| alloc.free(part);
        alloc.free(tier_match_parts);
    }
    try std.testing.expectEqual(@as(usize, 3), tier_match_parts.len);
    try std.testing.expectEqualStrings("pathfact-match:v1", tier_match_parts[0]);
    try std.testing.expectEqualStrings("string", tier_match_parts[1]);
    try std.testing.expectEqualStrings("OLD", tier_match_parts[2]);
    try std.testing.expectEqualStrings("/tenant", from_path_match_filter[1].field);
    const tenant_match_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_match_filter[1].value);
    defer {
        for (tenant_match_parts) |part| alloc.free(part);
        alloc.free(tenant_match_parts);
    }
    try std.testing.expectEqual(@as(usize, 3), tenant_match_parts.len);
    try std.testing.expectEqualStrings("pathfact-match:v1", tenant_match_parts[0]);
    try std.testing.expectEqualStrings("string", tenant_match_parts[1]);
    try std.testing.expectEqualStrings("ICE", tenant_match_parts[2]);

    const sibling_path_match_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"match\":\"old\",\"field\":\"/metadata/tier\"}",
    })).?;
    defer freeAlgebraicConstraints(alloc, sibling_path_match_filter);
    try std.testing.expectEqual(@as(usize, 1), sibling_path_match_filter.len);
    try std.testing.expectEqualStrings("/metadata/tier", sibling_path_match_filter[0].field);

    const non_path_match_filter = try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"match\":{\"tenant\":\"ICE\"}}",
    });
    try std.testing.expect(non_path_match_filter == null);

    const from_path_wildcard_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"bool\":{\"filter\":[{\"wildcard\":{\"path\":\"/metadata/tier\",\"pattern\":\"go*\"}},{\"wildcard\":{\"/tenant\":\"ac?e\"}}]}}",
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_wildcard_filter);
    try std.testing.expectEqual(@as(usize, 2), from_path_wildcard_filter.len);
    try std.testing.expectEqualStrings("/metadata/tier", from_path_wildcard_filter[0].field);
    const tier_wildcard_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_wildcard_filter[0].value);
    defer {
        for (tier_wildcard_parts) |part| alloc.free(part);
        alloc.free(tier_wildcard_parts);
    }
    try std.testing.expectEqual(@as(usize, 3), tier_wildcard_parts.len);
    try std.testing.expectEqualStrings("pathfact-wildcard:v1", tier_wildcard_parts[0]);
    try std.testing.expectEqualStrings("string", tier_wildcard_parts[1]);
    try std.testing.expectEqualStrings("go*", tier_wildcard_parts[2]);
    try std.testing.expectEqualStrings("/tenant", from_path_wildcard_filter[1].field);
    const tenant_wildcard_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_wildcard_filter[1].value);
    defer {
        for (tenant_wildcard_parts) |part| alloc.free(part);
        alloc.free(tenant_wildcard_parts);
    }
    try std.testing.expectEqual(@as(usize, 3), tenant_wildcard_parts.len);
    try std.testing.expectEqualStrings("pathfact-wildcard:v1", tenant_wildcard_parts[0]);
    try std.testing.expectEqualStrings("string", tenant_wildcard_parts[1]);
    try std.testing.expectEqualStrings("ac?e", tenant_wildcard_parts[2]);

    const leading_wildcard_filter = try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"wildcard\":{\"/tenant\":\"*ice\"}}",
    });
    try std.testing.expect(leading_wildcard_filter == null);

    const non_path_wildcard_filter = try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"wildcard\":{\"tenant\":\"ac*\"}}",
    });
    try std.testing.expect(non_path_wildcard_filter == null);

    const from_path_regexp_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"bool\":{\"filter\":[{\"regexp\":{\"path\":\"/metadata/tier\",\"pattern\":\"go.*\"}},{\"regexp\":{\"/tenant\":\"ac.e\"}}]}}",
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_regexp_filter);
    try std.testing.expectEqual(@as(usize, 2), from_path_regexp_filter.len);
    try std.testing.expectEqualStrings("/metadata/tier", from_path_regexp_filter[0].field);
    const tier_regexp_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_regexp_filter[0].value);
    defer {
        for (tier_regexp_parts) |part| alloc.free(part);
        alloc.free(tier_regexp_parts);
    }
    try std.testing.expectEqual(@as(usize, 3), tier_regexp_parts.len);
    try std.testing.expectEqualStrings("pathfact-regexp:v1", tier_regexp_parts[0]);
    try std.testing.expectEqualStrings("string", tier_regexp_parts[1]);
    try std.testing.expectEqualStrings("go.*", tier_regexp_parts[2]);
    try std.testing.expectEqualStrings("/tenant", from_path_regexp_filter[1].field);
    const tenant_regexp_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_regexp_filter[1].value);
    defer {
        for (tenant_regexp_parts) |part| alloc.free(part);
        alloc.free(tenant_regexp_parts);
    }
    try std.testing.expectEqual(@as(usize, 3), tenant_regexp_parts.len);
    try std.testing.expectEqualStrings("pathfact-regexp:v1", tenant_regexp_parts[0]);
    try std.testing.expectEqualStrings("string", tenant_regexp_parts[1]);
    try std.testing.expectEqualStrings("ac.e", tenant_regexp_parts[2]);

    const leading_regexp_filter = try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"regexp\":{\"/tenant\":\".*ice\"}}",
    });
    try std.testing.expect(leading_regexp_filter == null);

    const invalid_regexp_filter = try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"regexp\":{\"/tenant\":\"ac(\"}}",
    });
    try std.testing.expect(invalid_regexp_filter == null);

    const non_path_regexp_filter = try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"regexp\":{\"tenant\":\"ac.*\"}}",
    });
    try std.testing.expect(non_path_regexp_filter == null);

    const from_path_fuzzy_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"bool\":{\"filter\":[{\"fuzzy\":{\"path\":\"/metadata/tier\",\"query\":\"gild\",\"prefix_length\":1,\"max_edits\":1}},{\"fuzzy\":{\"/tenant\":{\"query\":\"alpine\",\"prefix_length\":2,\"max_edits\":1}}}]}}",
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_fuzzy_filter);
    try std.testing.expectEqual(@as(usize, 2), from_path_fuzzy_filter.len);
    try std.testing.expectEqualStrings("/metadata/tier", from_path_fuzzy_filter[0].field);
    const tier_fuzzy_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_fuzzy_filter[0].value);
    defer {
        for (tier_fuzzy_parts) |part| alloc.free(part);
        alloc.free(tier_fuzzy_parts);
    }
    try std.testing.expectEqual(@as(usize, 5), tier_fuzzy_parts.len);
    try std.testing.expectEqualStrings("pathfact-fuzzy:v1", tier_fuzzy_parts[0]);
    try std.testing.expectEqualStrings("string", tier_fuzzy_parts[1]);
    try std.testing.expectEqualStrings("gild", tier_fuzzy_parts[2]);
    try std.testing.expectEqualStrings("1", tier_fuzzy_parts[3]);
    try std.testing.expectEqualStrings("1", tier_fuzzy_parts[4]);
    try std.testing.expectEqualStrings("/tenant", from_path_fuzzy_filter[1].field);
    const tenant_fuzzy_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_fuzzy_filter[1].value);
    defer {
        for (tenant_fuzzy_parts) |part| alloc.free(part);
        alloc.free(tenant_fuzzy_parts);
    }
    try std.testing.expectEqual(@as(usize, 5), tenant_fuzzy_parts.len);
    try std.testing.expectEqualStrings("pathfact-fuzzy:v1", tenant_fuzzy_parts[0]);
    try std.testing.expectEqualStrings("string", tenant_fuzzy_parts[1]);
    try std.testing.expectEqualStrings("alpine", tenant_fuzzy_parts[2]);
    try std.testing.expectEqualStrings("1", tenant_fuzzy_parts[3]);
    try std.testing.expectEqualStrings("2", tenant_fuzzy_parts[4]);

    const unbounded_fuzzy_filter = try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"fuzzy\":{\"/tenant\":{\"query\":\"alice\",\"max_edits\":1}}}",
    });
    try std.testing.expect(unbounded_fuzzy_filter == null);

    const non_path_fuzzy_filter = try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"fuzzy\":{\"tenant\":{\"query\":\"alice\",\"prefix_length\":1}}}",
    });
    try std.testing.expect(non_path_fuzzy_filter == null);

    const from_path_numeric_range_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"bool\":{\"filter\":[{\"numeric_range\":{\"path\":\"/amount\",\"min\":10,\"max\":30,\"inclusive_min\":true,\"inclusive_max\":false}},{\"range\":{\"/score\":{\"gte\":7,\"lt\":9}}}]}}",
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_numeric_range_filter);
    try std.testing.expectEqual(@as(usize, 2), from_path_numeric_range_filter.len);
    try std.testing.expectEqualStrings("/amount", from_path_numeric_range_filter[0].field);
    const amount_range_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_numeric_range_filter[0].value);
    defer {
        for (amount_range_parts) |part| alloc.free(part);
        alloc.free(amount_range_parts);
    }
    try std.testing.expectEqual(@as(usize, 6), amount_range_parts.len);
    try std.testing.expectEqualStrings("pathfact-numeric-range:v1", amount_range_parts[0]);
    try std.testing.expectEqualStrings("number", amount_range_parts[1]);
    try std.testing.expectEqualStrings("10", amount_range_parts[2]);
    try std.testing.expectEqualStrings("30", amount_range_parts[3]);
    try std.testing.expectEqualStrings("1", amount_range_parts[4]);
    try std.testing.expectEqualStrings("0", amount_range_parts[5]);
    try std.testing.expectEqualStrings("/score", from_path_numeric_range_filter[1].field);
    const score_range_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_numeric_range_filter[1].value);
    defer {
        for (score_range_parts) |part| alloc.free(part);
        alloc.free(score_range_parts);
    }
    try std.testing.expectEqual(@as(usize, 6), score_range_parts.len);
    try std.testing.expectEqualStrings("pathfact-numeric-range:v1", score_range_parts[0]);
    try std.testing.expectEqualStrings("number", score_range_parts[1]);
    try std.testing.expectEqualStrings("7", score_range_parts[2]);
    try std.testing.expectEqualStrings("9", score_range_parts[3]);
    try std.testing.expectEqualStrings("1", score_range_parts[4]);
    try std.testing.expectEqualStrings("0", score_range_parts[5]);

    const upper_only_path_range_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"range\":{\"field\":\"/score\",\"lte\":9}}",
    })).?;
    defer freeAlgebraicConstraints(alloc, upper_only_path_range_filter);
    try std.testing.expectEqual(@as(usize, 1), upper_only_path_range_filter.len);
    const upper_range_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, upper_only_path_range_filter[0].value);
    defer {
        for (upper_range_parts) |part| alloc.free(part);
        alloc.free(upper_range_parts);
    }
    try std.testing.expectEqualStrings("", upper_range_parts[2]);
    try std.testing.expectEqualStrings("9", upper_range_parts[3]);
    try std.testing.expectEqualStrings("1", upper_range_parts[4]);
    try std.testing.expectEqualStrings("1", upper_range_parts[5]);

    const non_path_numeric_range_filter = try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"numeric_range\":{\"field\":\"amount\",\"min\":10}}",
    });
    try std.testing.expect(non_path_numeric_range_filter == null);

    const from_path_date_range_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"bool\":{\"filter\":[{\"date_range\":{\"path\":\"/published_at\",\"start\":\"2026-01-02T00:00:00Z\",\"end\":\"2026-01-03T00:00:00Z\",\"inclusive_start\":true,\"inclusive_end\":false}},{\"range\":{\"/created_at\":{\"gte\":\"2026-01-04\",\"lt\":\"2026-01-05\"}}}]}}",
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_date_range_filter);
    try std.testing.expectEqual(@as(usize, 2), from_path_date_range_filter.len);
    try std.testing.expectEqualStrings("/published_at", from_path_date_range_filter[0].field);
    const published_range_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_date_range_filter[0].value);
    defer {
        for (published_range_parts) |part| alloc.free(part);
        alloc.free(published_range_parts);
    }
    try std.testing.expectEqual(@as(usize, 6), published_range_parts.len);
    try std.testing.expectEqualStrings("pathfact-date-range:v1", published_range_parts[0]);
    try std.testing.expectEqualStrings("datetime", published_range_parts[1]);
    try std.testing.expectEqualStrings("2026-01-02T00:00:00Z", published_range_parts[2]);
    try std.testing.expectEqualStrings("2026-01-03T00:00:00Z", published_range_parts[3]);
    try std.testing.expectEqualStrings("1", published_range_parts[4]);
    try std.testing.expectEqualStrings("0", published_range_parts[5]);
    try std.testing.expectEqualStrings("/created_at", from_path_date_range_filter[1].field);
    const created_range_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_date_range_filter[1].value);
    defer {
        for (created_range_parts) |part| alloc.free(part);
        alloc.free(created_range_parts);
    }
    try std.testing.expectEqual(@as(usize, 6), created_range_parts.len);
    try std.testing.expectEqualStrings("pathfact-date-range:v1", created_range_parts[0]);
    try std.testing.expectEqualStrings("datetime", created_range_parts[1]);
    try std.testing.expectEqualStrings("2026-01-04", created_range_parts[2]);
    try std.testing.expectEqualStrings("2026-01-05", created_range_parts[3]);
    try std.testing.expectEqualStrings("1", created_range_parts[4]);
    try std.testing.expectEqualStrings("0", created_range_parts[5]);

    const non_path_date_range_filter = try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"date_range\":{\"field\":\"published_at\",\"start\":\"2026-01-02T00:00:00Z\"}}",
    });
    try std.testing.expect(non_path_date_range_filter == null);

    const from_path_ip_range_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"bool\":{\"filter\":[{\"ip_range\":{\"path\":\"/client_ip\",\"cidr\":\"10.1.0.0/16\"}},{\"ip_range\":{\"field\":\"/gateway_ip\",\"cidr\":\"192.168.1.10\"}}]}}",
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_ip_range_filter);
    try std.testing.expectEqual(@as(usize, 2), from_path_ip_range_filter.len);
    try std.testing.expectEqualStrings("/client_ip", from_path_ip_range_filter[0].field);
    const client_ip_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_ip_range_filter[0].value);
    defer {
        for (client_ip_parts) |part| alloc.free(part);
        alloc.free(client_ip_parts);
    }
    try std.testing.expectEqual(@as(usize, 3), client_ip_parts.len);
    try std.testing.expectEqualStrings("pathfact-ip-range:v1", client_ip_parts[0]);
    try std.testing.expectEqualStrings("ipv4", client_ip_parts[1]);
    try std.testing.expectEqualStrings("10.1.0.0/16", client_ip_parts[2]);
    try std.testing.expectEqualStrings("/gateway_ip", from_path_ip_range_filter[1].field);
    const gateway_ip_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_ip_range_filter[1].value);
    defer {
        for (gateway_ip_parts) |part| alloc.free(part);
        alloc.free(gateway_ip_parts);
    }
    try std.testing.expectEqual(@as(usize, 3), gateway_ip_parts.len);
    try std.testing.expectEqualStrings("pathfact-ip-range:v1", gateway_ip_parts[0]);
    try std.testing.expectEqualStrings("ipv4", gateway_ip_parts[1]);
    try std.testing.expectEqualStrings("192.168.1.10", gateway_ip_parts[2]);

    const non_path_ip_range_filter = try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"ip_range\":{\"field\":\"client_ip\",\"cidr\":\"10.1.0.0/16\"}}",
    });
    try std.testing.expect(non_path_ip_range_filter == null);

    const invalid_ip_range_filter = try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"ip_range\":{\"path\":\"/client_ip\",\"cidr\":\"10.999.0.0/16\"}}",
    });
    try std.testing.expect(invalid_ip_range_filter == null);

    const from_path_geo_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"bool\":{\"filter\":[{\"geo_bbox\":{\"path\":\"/location\",\"min_lat\":37.70,\"min_lon\":-122.50,\"max_lat\":37.80,\"max_lon\":-122.30}},{\"geo_distance\":{\"field\":\"/warehouse\",\"lat\":40.7128,\"lon\":-74.006,\"radius_meters\":5000}}]}}",
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_geo_filter);
    try std.testing.expectEqual(@as(usize, 2), from_path_geo_filter.len);
    try std.testing.expectEqualStrings("/location", from_path_geo_filter[0].field);
    const location_geo_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_geo_filter[0].value);
    defer {
        for (location_geo_parts) |part| alloc.free(part);
        alloc.free(location_geo_parts);
    }
    try std.testing.expectEqualStrings("pathfact-geo-bbox:v1", location_geo_parts[0]);
    try std.testing.expectEqualStrings("/warehouse", from_path_geo_filter[1].field);
    const warehouse_geo_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_geo_filter[1].value);
    defer {
        for (warehouse_geo_parts) |part| alloc.free(part);
        alloc.free(warehouse_geo_parts);
    }
    try std.testing.expectEqualStrings("pathfact-geo-distance:v1", warehouse_geo_parts[0]);

    const from_path_geo_shape_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"geo_shape\":{\"path\":\"/location\",\"relation\":\"within\",\"polygons\":[[{\"lat\":37.0,\"lon\":-123.0},{\"lat\":38.0,\"lon\":-123.0},{\"lat\":38.0,\"lon\":-122.0},{\"lat\":37.0,\"lon\":-122.0}]]}}",
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_geo_shape_filter);
    try std.testing.expectEqual(@as(usize, 1), from_path_geo_shape_filter.len);
    try std.testing.expectEqualStrings("/location", from_path_geo_shape_filter[0].field);
    const location_geo_shape_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_geo_shape_filter[0].value);
    defer {
        for (location_geo_shape_parts) |part| alloc.free(part);
        alloc.free(location_geo_shape_parts);
    }
    try std.testing.expectEqual(@as(usize, 13), location_geo_shape_parts.len);
    try std.testing.expectEqualStrings("pathfact-geo-shape:v1", location_geo_shape_parts[0]);
    try std.testing.expectEqualStrings("geo_point", location_geo_shape_parts[1]);
    try std.testing.expectEqualStrings("within", location_geo_shape_parts[2]);

    const unsupported_geo_shape_filter = try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"geo_shape\":{\"path\":\"/location\",\"relation\":\"contains\",\"polygons\":[[{\"lat\":37.0,\"lon\":-123.0},{\"lat\":38.0,\"lon\":-123.0},{\"lat\":38.0,\"lon\":-122.0}]]}}",
    });
    try std.testing.expect(unsupported_geo_shape_filter == null);

    const non_path_geo_filter = try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"geo_bbox\":{\"field\":\"location\",\"min_lat\":37.70,\"min_lon\":-122.50,\"max_lat\":37.80,\"max_lon\":-122.30}}",
    });
    try std.testing.expect(non_path_geo_filter == null);

    const invalid_geo_filter = try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"geo_distance\":{\"path\":\"/location\",\"lat\":95,\"lon\":-122.4194,\"radius_meters\":2000}}",
    });
    try std.testing.expect(invalid_geo_filter == null);

    const from_path_term_range_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"bool\":{\"filter\":[{\"term_range\":{\"path\":\"/tenant\",\"min\":\"alpi\",\"max\":\"alpj\",\"inclusive_min\":true,\"inclusive_max\":false}},{\"range\":{\"/status\":{\"gte\":\"active\",\"lt\":\"archived\"}}}]}}",
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_term_range_filter);
    try std.testing.expectEqual(@as(usize, 2), from_path_term_range_filter.len);
    try std.testing.expectEqualStrings("/tenant", from_path_term_range_filter[0].field);
    const tenant_range_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_term_range_filter[0].value);
    defer {
        for (tenant_range_parts) |part| alloc.free(part);
        alloc.free(tenant_range_parts);
    }
    try std.testing.expectEqual(@as(usize, 6), tenant_range_parts.len);
    try std.testing.expectEqualStrings("pathfact-term-range:v1", tenant_range_parts[0]);
    try std.testing.expectEqualStrings("string", tenant_range_parts[1]);
    try std.testing.expectEqualStrings("alpi", tenant_range_parts[2]);
    try std.testing.expectEqualStrings("alpj", tenant_range_parts[3]);
    try std.testing.expectEqualStrings("1", tenant_range_parts[4]);
    try std.testing.expectEqualStrings("0", tenant_range_parts[5]);
    try std.testing.expectEqualStrings("/status", from_path_term_range_filter[1].field);
    const status_range_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_term_range_filter[1].value);
    defer {
        for (status_range_parts) |part| alloc.free(part);
        alloc.free(status_range_parts);
    }
    try std.testing.expectEqual(@as(usize, 6), status_range_parts.len);
    try std.testing.expectEqualStrings("pathfact-term-range:v1", status_range_parts[0]);
    try std.testing.expectEqualStrings("string", status_range_parts[1]);
    try std.testing.expectEqualStrings("active", status_range_parts[2]);
    try std.testing.expectEqualStrings("archived", status_range_parts[3]);
    try std.testing.expectEqualStrings("1", status_range_parts[4]);
    try std.testing.expectEqualStrings("0", status_range_parts[5]);

    const non_path_term_range_filter = try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"term_range\":{\"field\":\"tenant\",\"min\":\"a\"}}",
    });
    try std.testing.expect(non_path_term_range_filter == null);

    const non_path_exists_filter = try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"exists\":\"tenant\"}",
    });
    try std.testing.expect(non_path_exists_filter == null);

    const multi_terms_filter = try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"terms\":{\"tenant\":[\"t1\",\"t2\"]}}",
    });
    try std.testing.expect(multi_terms_filter == null);
}

test "algebraic constraints reject top-level text term query" {
    const alloc = std.testing.allocator;
    const constraints = try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .term = .{ .field = "body", .term = "published" } },
    });
    try std.testing.expect(constraints == null);
}

fn applyAggregationResults(
    alloc: std.mem.Allocator,
    req: db_mod.types.SearchRequest,
    result: db_mod.types.SearchResult,
    ctx: db_mod.aggregations.Context,
    meta: *query_api.QueryResponseMeta,
) !void {
    if (req.aggregations_json.len == 0) return;
    const requests = try query_api.parseAggregationRequestsJson(alloc, req.aggregations_json);
    defer query_api.freeAggregationRequests(alloc, requests);
    var aggregation_ctx = ctx;
    const constraints = if (canConsiderAlgebraicAggregations(req))
        try algebraicConstraintsForRequestAlloc(alloc, req)
    else
        null;
    defer if (constraints) |items| freeAlgebraicConstraints(alloc, items);
    if (constraints) |items| if (aggregation_ctx.algebraic_available) {
        aggregation_ctx.algebraic_scope = .root;
        aggregation_ctx.algebraic_constraints = items;
    };
    const aggregation_results = try db_mod.aggregations.computeSearchAggregations(alloc, requests, result, aggregation_ctx);
    errdefer db_mod.aggregations.deinitResults(alloc, aggregation_results);
    for (aggregation_results) |*aggregation| {
        try db_mod.aggregations.cloneSearchAggregationResultLabelsDeep(alloc, aggregation);
    }
    meta.aggregation_results = aggregation_results;
}

fn applyBoundQueryAggregations(
    self: *BoundTableReadSource,
    alloc: std.mem.Allocator,
    req: db_mod.types.SearchRequest,
    result: *db_mod.types.SearchResult,
    meta: *query_api.QueryResponseMeta,
    consistency: raft_mod.ReadConsistency,
) !void {
    if (req.aggregations_json.len == 0) return;
    const aggregation_req = requestWithResultIdentityGeneration(req, result.*);
    // Aggregate over every matching document, not the (limit-truncated) page;
    // see aggregationFirstPassIsComplete / aggregationFullScanLimit.
    if (!req.count_only and aggregationFirstPassIsComplete(req, result.*)) {
        return try applyAggregationResults(alloc, aggregation_req, result.*, try aggregationContextForDb(alloc, aggregation_req, self.db), meta);
    }

    const identity_read_generation = try identityGenerationForAggregationFullResultRerun(req, result.*);
    var full_req = req;
    full_req.identity_read_generation = identity_read_generation;
    full_req.offset = 0;
    full_req.limit = try aggregationFullScanLimit(alloc, self.db, result.*);
    full_req.include_stored = true;
    full_req.count_only = false;
    var full_result = try self.reads.searchWithConsistency(alloc, self.db, full_req, consistency);
    defer full_result.deinit();
    return try applyAggregationResults(alloc, full_req, full_result, try aggregationContextForDb(alloc, full_req, self.db), meta);
}

fn applyProvisionedQueryAggregations(
    self: *ProvisionedTableReadSource,
    alloc: std.mem.Allocator,
    group_ids: []const u64,
    table_name: []const u8,
    req: db_mod.types.SearchRequest,
    result: *db_mod.types.SearchResult,
    meta: *query_api.QueryResponseMeta,
    consistency: raft_mod.ReadConsistency,
) !void {
    if (req.aggregations_json.len == 0) return;
    const aggregation_req = requestWithResultIdentityGeneration(req, result.*);
    if (group_ids.len == 1) {
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_ids[0]);
        defer alloc.free(path);
        var db = try openProvisionedQueryDbForTableWithRuntime(alloc, path, self.catalog, table_name, group_ids[0], self.visibleRootGeneration(group_ids[0]), self.backend_runtime);
        defer db.close();

        // Scan-based aggregations must run over every matching document, not the
        // returned page. total_hits is recomputed from the (limit-truncated,
        // MVCC-visible) page during postprocessing, so it cannot be trusted as
        // the true match count: re-fetch with an unbounded limit (capped by the
        // shard's primary doc count) whenever the first pass could have been
        // truncated.
        if (!req.count_only and aggregationFirstPassIsComplete(req, result.*)) {
            return try applyAggregationResults(alloc, aggregation_req, result.*, try aggregationContextForDb(alloc, aggregation_req, &db), meta);
        }

        var reads = raft_mod.FeatureDBReads.init(group_ids[0], self.requester);
        const identity_read_generation = try identityGenerationForAggregationFullResultRerun(req, result.*);
        var full_req = req;
        full_req.identity_read_generation = identity_read_generation;
        full_req.offset = 0;
        full_req.limit = try aggregationFullScanLimit(alloc, &db, result.*);
        full_req.include_stored = true;
        full_req.count_only = false;
        var full_result = try reads.searchWithConsistency(alloc, &db, full_req, consistency);
        defer full_result.deinit();
        return try applyAggregationResults(alloc, full_req, full_result, try aggregationContextForDb(alloc, full_req, &db), meta);
    }

    if (try tryApplyProvisionedAlgebraicDistributedAggregations(self, alloc, group_ids, table_name, aggregation_req, meta)) return;

    const current_agg_stats = try collectProvisionedAggregationTextStats(self, alloc, group_ids, table_name, aggregation_req, result.hits);
    defer distributed_stats_mod.deinitTextFieldStats(alloc, current_agg_stats);
    const current_bg_stats = try collectProvisionedAggregationBackgroundTextStats(self, alloc, group_ids, table_name, aggregation_req, result.hits);
    defer db_mod.aggregations.deinitDistributedBackgroundTextStats(alloc, current_bg_stats);
    // Aggregate over every matching document, not the (limit-truncated) page;
    // see aggregationFirstPassIsComplete. Across groups the per-shard doc count
    // is not readily available, so the re-fetch uses an unbounded limit.
    if (!req.count_only and aggregationFirstPassIsComplete(req, result.*)) {
        return try applyAggregationResults(alloc, aggregation_req, result.*, .{
            .distributed_text_stats = current_agg_stats,
            .distributed_background_text_stats = current_bg_stats,
        }, meta);
    }

    const identity_read_generation = try identityGenerationForAggregationFullResultRerun(req, result.*);
    var full_req = req;
    full_req.identity_read_generation = identity_read_generation;
    full_req.offset = 0;
    full_req.limit = aggregationDistributedFullScanLimit(result.*);
    full_req.include_stored = true;
    full_req.count_only = false;
    var full_result = try queryProvisionedAcrossGroups(self, alloc, group_ids, full_req, table_name, consistency);
    defer full_result.deinit();
    const full_agg_stats = try collectProvisionedAggregationTextStats(self, alloc, group_ids, table_name, full_req, full_result.hits);
    defer distributed_stats_mod.deinitTextFieldStats(alloc, full_agg_stats);
    const full_bg_stats = try collectProvisionedAggregationBackgroundTextStats(self, alloc, group_ids, table_name, full_req, full_result.hits);
    defer db_mod.aggregations.deinitDistributedBackgroundTextStats(alloc, full_bg_stats);
    return try applyAggregationResults(alloc, full_req, full_result, .{
        .distributed_text_stats = full_agg_stats,
        .distributed_background_text_stats = full_bg_stats,
    }, meta);
}

fn tryApplyProvisionedAlgebraicDistributedAggregations(
    self: *ProvisionedTableReadSource,
    alloc: std.mem.Allocator,
    group_ids: []const u64,
    table_name: []const u8,
    req: db_mod.types.SearchRequest,
    meta: *query_api.QueryResponseMeta,
) !bool {
    if (group_ids.len <= 1 or req.aggregations_json.len == 0) return false;
    if (!canConsiderAlgebraicAggregations(req)) return false;
    const constraints = (try algebraicConstraintsForRequestAlloc(alloc, req)) orelse return false;
    defer freeAlgebraicConstraints(alloc, constraints);
    const requests = try query_api.parseAggregationRequestsJson(alloc, req.aggregations_json);
    defer query_api.freeAggregationRequests(alloc, requests);
    if (requests.len == 0) return false;

    const first_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_ids[0]);
    defer alloc.free(first_path);
    var first_db = try openProvisionedQueryDbForTableWithRuntime(alloc, first_path, self.catalog, table_name, group_ids[0], self.visibleRootGeneration(group_ids[0]), self.backend_runtime);
    defer first_db.close();
    if (!(try algebraicIndexFreshEnoughForRequest(alloc, req, &first_db))) return false;
    const first_entry = if (req.index_name) |index_name|
        first_db.core.index_manager.algebraicIndex(index_name) orelse return false
    else
        first_db.core.index_manager.algebraicIndex(null) orelse return false;
    if (!first_entry.index.plannerLifecycleReady()) return false;

    var primary_count: usize = 0;
    var pipeline_count: usize = 0;
    for (requests) |request| {
        if (db_mod.aggregations.isPipelineAggregation(request.type)) pipeline_count += 1 else primary_count += 1;
    }
    if (primary_count == 0) return false;

    var primary_results = try alloc.alloc(db_mod.aggregations.SearchAggregationResult, primary_count);
    defer if (primary_results.len > 0) alloc.free(primary_results);
    var primary_filled: usize = 0;
    errdefer {
        for (primary_results[0..primary_filled]) |*result| result.deinit(alloc);
    }
    var pipeline_requests = try alloc.alloc(db_mod.aggregations.SearchAggregationRequest, pipeline_count);
    defer if (pipeline_requests.len > 0) alloc.free(pipeline_requests);
    var pipeline_filled: usize = 0;

    for (requests) |request| {
        if (db_mod.aggregations.isPipelineAggregation(request.type)) {
            pipeline_requests[pipeline_filled] = request;
            pipeline_filled += 1;
            continue;
        }
        var request_plan = (try algebraicDistributedTensorProgramForAggregationRequestAlloc(alloc, &first_entry.index, request, constraints, req.identity_read_generation)) orelse return false;
        defer request_plan.deinit(alloc);
        var merged = (try collectProvisionedAlgebraicDistributedPartials(self, alloc, group_ids, table_name, req, first_entry.index.name, request_plan.access_paths, request_plan.asProgram())) orelse return false;
        defer merged.deinit(alloc);
        var result = (try algebraicAggregationFromDistributedPartialsAlloc(alloc, &first_entry.index, request, constraints, merged)) orelse return false;
        var result_owned = true;
        errdefer if (result_owned) result.deinit(alloc);
        try db_mod.aggregations.cloneSearchAggregationResultLabelsDeep(alloc, &result);
        primary_results[primary_filled] = result;
        result_owned = false;
        primary_filled += 1;
    }

    var pipeline_results: []db_mod.aggregations.SearchAggregationResult = &.{};
    defer if (pipeline_results.len > 0) alloc.free(pipeline_results);
    var pipeline_items_owned = false;
    errdefer if (pipeline_items_owned) {
        for (pipeline_results) |*result| result.deinit(alloc);
    };
    if (pipeline_requests.len > 0) {
        pipeline_results = try db_mod.aggregations.computeRootPipelineAggregations(alloc, pipeline_requests, primary_results);
        pipeline_items_owned = true;
        for (pipeline_results) |*result| try db_mod.aggregations.cloneSearchAggregationResultLabelsDeep(alloc, result);
    }

    var results = try alloc.alloc(db_mod.aggregations.SearchAggregationResult, primary_results.len + pipeline_results.len);
    errdefer alloc.free(results);
    for (primary_results, 0..) |result, i| results[i] = result;
    primary_filled = 0;
    for (pipeline_results, 0..) |result, i| results[primary_results.len + i] = result;
    pipeline_items_owned = false;

    meta.aggregation_results = results;
    return true;
}

fn collectProvisionedAlgebraicDistributedPartials(
    self: *ProvisionedTableReadSource,
    alloc: std.mem.Allocator,
    group_ids: []const u64,
    table_name: []const u8,
    req: db_mod.types.SearchRequest,
    selected_index_name: []const u8,
    access_paths: []const algebraic_ir.PhysicalAccessPath,
    tensor_program: algebraic_ir.TensorProgram,
) !?db_mod.algebraic.distributed.MergeSet {
    try tableReadsValidateDocIdentityReadyForMultiGroup(alloc, self.catalog, table_name, group_ids.len);
    if (searchRequestHasResolvedDocFilter(req)) return null;
    const body = try encodeAlgebraicPartialsRequestWithProgramAtGeneration(alloc, req.index_name orelse selected_index_name, req.identity_read_generation, access_paths, &.{}, tensor_program);
    defer alloc.free(body);
    var partials = std.ArrayListUnmanaged(db_mod.algebraic.distributed.Partial).empty;
    errdefer {
        for (partials.items) |partial| {
            alloc.free(@constCast(partial.canonical_axis));
            if (partial.metric.len > 0) alloc.free(@constCast(partial.metric));
            alloc.free(@constCast(partial.value));
        }
        partials.deinit(alloc);
    }

    for (group_ids) |group_id| {
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);
        var db = try openProvisionedQueryDbForTableWithRuntime(alloc, path, self.catalog, table_name, group_id, self.visibleRootGeneration(group_id), self.backend_runtime);
        defer db.close();
        if (!(try algebraicIndexFreshEnoughForRequest(alloc, req, &db))) return null;
        var parsed = try parseAlgebraicPartialsRequest(alloc, body);
        defer parsed.deinit(alloc);
        const shard_partials = try collectAlgebraicPartialsFromDbForRequest(alloc, &db, parsed);
        defer if (shard_partials.len > 0) alloc.free(shard_partials);
        for (shard_partials) |partial| try partials.append(alloc, partial);
    }

    const partial_slice = try partials.toOwnedSlice(alloc);
    defer db_mod.algebraic.distributed.freePartials(alloc, partial_slice);
    return try db_mod.algebraic.distributed.mergePartialsAlloc(alloc, partial_slice);
}

fn applyHostedProvisionedQueryAggregations(
    self: *HostedProvisionedTableReadSource,
    alloc: std.mem.Allocator,
    group_ids: []const u64,
    table_name: []const u8,
    req: db_mod.types.SearchRequest,
    result: *db_mod.types.SearchResult,
    meta: *query_api.QueryResponseMeta,
    consistency: raft_mod.ReadConsistency,
) !void {
    if (req.aggregations_json.len == 0) return;
    const aggregation_req = requestWithResultIdentityGeneration(req, result.*);
    if (group_ids.len == 1) {
        var route = (try table_router.resolveGroupRoute(alloc, self.catalog, self.router, group_ids[0], routePolicyForConsistency(consistency))) orelse return error.TableNotFound;
        defer route.deinit(alloc);

        switch (route) {
            .local => {
                const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_ids[0]);
                defer alloc.free(path);
                var db = try openProvisionedQueryDbForTableWithRuntime(alloc, path, self.catalog, table_name, group_ids[0], self.visibleRootGeneration(group_ids[0]), self.backend_runtime);
                defer db.close();

                // Aggregate over every matching document, not the
                // (limit-truncated) page; see aggregationFirstPassIsComplete /
                // aggregationFullScanLimit.
                if (!req.count_only and aggregationFirstPassIsComplete(req, result.*)) {
                    return try applyAggregationResults(alloc, aggregation_req, result.*, try aggregationContextForDb(alloc, aggregation_req, &db), meta);
                }

                var reads = raft_mod.FeatureDBReads.init(group_ids[0], self.requester);
                const identity_read_generation = try identityGenerationForAggregationFullResultRerun(req, result.*);
                var full_req = req;
                full_req.identity_read_generation = identity_read_generation;
                full_req.offset = 0;
                full_req.limit = try aggregationFullScanLimit(alloc, &db, result.*);
                full_req.include_stored = true;
                full_req.count_only = false;
                var full_result = try reads.searchWithConsistency(alloc, &db, full_req, consistency);
                defer full_result.deinit();
                return try applyAggregationResults(alloc, full_req, full_result, try aggregationContextForDb(alloc, full_req, &db), meta);
            },
            .remote => {},
        }
    }

    if (try tryApplyHostedAlgebraicDistributedAggregations(self, alloc, group_ids, table_name, aggregation_req, meta, consistency)) return;

    const current_agg_stats = try collectHostedAggregationTextStats(self, alloc, group_ids, table_name, aggregation_req, result.hits, consistency);
    defer distributed_stats_mod.deinitTextFieldStats(alloc, current_agg_stats);
    const current_bg_stats = try collectHostedAggregationBackgroundTextStats(self, alloc, group_ids, table_name, aggregation_req, result.hits, consistency);
    defer db_mod.aggregations.deinitDistributedBackgroundTextStats(alloc, current_bg_stats);
    // Aggregate over every matching document, not the (limit-truncated) page;
    // see aggregationFirstPassIsComplete.
    if (!req.count_only and aggregationFirstPassIsComplete(req, result.*)) {
        return try applyAggregationResults(alloc, aggregation_req, result.*, .{
            .distributed_text_stats = current_agg_stats,
            .distributed_background_text_stats = current_bg_stats,
        }, meta);
    }

    const identity_read_generation = try identityGenerationForAggregationFullResultRerun(req, result.*);
    var full_req = req;
    full_req.identity_read_generation = identity_read_generation;
    full_req.offset = 0;
    full_req.limit = aggregationDistributedFullScanLimit(result.*);
    full_req.include_stored = true;
    full_req.count_only = false;
    var full_result = try queryHostedAcrossGroups(self, alloc, group_ids, full_req, table_name, consistency);
    defer full_result.deinit();
    const full_agg_stats = try collectHostedAggregationTextStats(self, alloc, group_ids, table_name, full_req, full_result.hits, consistency);
    defer distributed_stats_mod.deinitTextFieldStats(alloc, full_agg_stats);
    const full_bg_stats = try collectHostedAggregationBackgroundTextStats(self, alloc, group_ids, table_name, full_req, full_result.hits, consistency);
    defer db_mod.aggregations.deinitDistributedBackgroundTextStats(alloc, full_bg_stats);
    return try applyAggregationResults(alloc, full_req, full_result, .{
        .distributed_text_stats = full_agg_stats,
        .distributed_background_text_stats = full_bg_stats,
    }, meta);
}

fn tryApplyHostedAlgebraicDistributedAggregations(
    self: *HostedProvisionedTableReadSource,
    alloc: std.mem.Allocator,
    group_ids: []const u64,
    table_name: []const u8,
    req: db_mod.types.SearchRequest,
    meta: *query_api.QueryResponseMeta,
    consistency: raft_mod.ReadConsistency,
) !bool {
    if (group_ids.len <= 1 or req.aggregations_json.len == 0) return false;
    if (!canConsiderAlgebraicAggregations(req)) return false;
    const representative_group_id: ?u64 = blk: {
        for (group_ids) |group_id| {
            var route = (try table_router.resolveGroupRoute(alloc, self.catalog, self.router, group_id, routePolicyForConsistency(consistency))) orelse return false;
            defer route.deinit(alloc);
            switch (route) {
                .local => break :blk group_id,
                .remote => {},
            }
        }
        break :blk null;
    };

    const constraints = (try algebraicConstraintsForRequestAlloc(alloc, req)) orelse return false;
    defer freeAlgebraicConstraints(alloc, constraints);
    const requests = try query_api.parseAggregationRequestsJson(alloc, req.aggregations_json);
    defer query_api.freeAggregationRequests(alloc, requests);
    if (requests.len == 0) return false;

    if (representative_group_id) |group_id| {
        const first_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(first_path);
        var first_db = try openProvisionedQueryDbForTableWithRuntime(alloc, first_path, self.catalog, table_name, group_id, self.visibleRootGeneration(group_id), self.backend_runtime);
        defer first_db.close();
        if (!(try algebraicIndexFreshEnoughForRequest(alloc, req, &first_db))) return false;
        const first_entry = if (req.index_name) |index_name|
            first_db.core.index_manager.algebraicIndex(index_name) orelse return false
        else
            first_db.core.index_manager.algebraicIndex(null) orelse return false;
        if (!first_entry.index.plannerLifecycleReady()) return false;
        return try applyHostedAlgebraicDistributedAggregationsWithPlanner(
            self,
            alloc,
            group_ids,
            table_name,
            req,
            first_entry.index.name,
            constraints,
            requests,
            meta,
            consistency,
            &first_entry.index,
        );
    }

    var catalog_index = (try openCatalogAlgebraicPlannerIndex(alloc, self.catalog, table_name, req.index_name)) orelse return false;
    defer catalog_index.close();
    if (!catalog_index.plannerLifecycleReady()) return false;
    return try applyHostedAlgebraicDistributedAggregationsWithPlanner(
        self,
        alloc,
        group_ids,
        table_name,
        req,
        catalog_index.name,
        constraints,
        requests,
        meta,
        consistency,
        &catalog_index,
    );
}

fn applyHostedAlgebraicDistributedAggregationsWithPlanner(
    self: *HostedProvisionedTableReadSource,
    alloc: std.mem.Allocator,
    group_ids: []const u64,
    table_name: []const u8,
    req: db_mod.types.SearchRequest,
    planner_index_name: []const u8,
    constraints: []const db_mod.aggregations.FixedConstraint,
    requests: []const db_mod.aggregations.SearchAggregationRequest,
    meta: *query_api.QueryResponseMeta,
    consistency: raft_mod.ReadConsistency,
    planner_index: *db_mod.algebraic.index.Index,
) !bool {
    var primary_count: usize = 0;
    var pipeline_count: usize = 0;
    for (requests) |request| {
        if (db_mod.aggregations.isPipelineAggregation(request.type)) pipeline_count += 1 else primary_count += 1;
    }
    if (primary_count == 0) return false;

    var primary_results = try alloc.alloc(db_mod.aggregations.SearchAggregationResult, primary_count);
    defer if (primary_results.len > 0) alloc.free(primary_results);
    var primary_filled: usize = 0;
    errdefer {
        for (primary_results[0..primary_filled]) |*result| result.deinit(alloc);
    }
    var pipeline_requests = try alloc.alloc(db_mod.aggregations.SearchAggregationRequest, pipeline_count);
    defer if (pipeline_requests.len > 0) alloc.free(pipeline_requests);
    var pipeline_filled: usize = 0;

    for (requests) |request| {
        if (db_mod.aggregations.isPipelineAggregation(request.type)) {
            pipeline_requests[pipeline_filled] = request;
            pipeline_filled += 1;
            continue;
        }
        var request_plan = (try algebraicDistributedTensorProgramForAggregationRequestAlloc(alloc, planner_index, request, constraints, req.identity_read_generation)) orelse return false;
        defer request_plan.deinit(alloc);
        var merged = (try collectHostedAlgebraicDistributedPartials(self, alloc, group_ids, table_name, req, planner_index_name, request_plan.access_paths, request_plan.asProgram(), consistency)) orelse return false;
        defer merged.deinit(alloc);
        var result = (try algebraicAggregationFromDistributedPartialsAlloc(alloc, planner_index, request, constraints, merged)) orelse return false;
        var result_owned = true;
        errdefer if (result_owned) result.deinit(alloc);
        try db_mod.aggregations.cloneSearchAggregationResultLabelsDeep(alloc, &result);
        primary_results[primary_filled] = result;
        result_owned = false;
        primary_filled += 1;
    }

    var pipeline_results: []db_mod.aggregations.SearchAggregationResult = &.{};
    defer if (pipeline_results.len > 0) alloc.free(pipeline_results);
    var pipeline_items_owned = false;
    errdefer if (pipeline_items_owned) {
        for (pipeline_results) |*result| result.deinit(alloc);
    };
    if (pipeline_requests.len > 0) {
        pipeline_results = try db_mod.aggregations.computeRootPipelineAggregations(alloc, pipeline_requests, primary_results);
        pipeline_items_owned = true;
        for (pipeline_results) |*result| try db_mod.aggregations.cloneSearchAggregationResultLabelsDeep(alloc, result);
    }

    var results = try alloc.alloc(db_mod.aggregations.SearchAggregationResult, primary_results.len + pipeline_results.len);
    errdefer alloc.free(results);
    for (primary_results, 0..) |result, i| results[i] = result;
    primary_filled = 0;
    for (pipeline_results, 0..) |result, i| results[primary_results.len + i] = result;
    pipeline_items_owned = false;

    meta.aggregation_results = results;
    return true;
}

const RangeCardinalityPlan = struct {
    kind: db_mod.algebraic.index.CardinalityRangeKind,
    ranges: []db_mod.algebraic.index.CardinalityRangeRequest,
    children: []db_mod.algebraic.index.CardinalityChildRequest,

    fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        for (self.ranges) |range| {
            alloc.free(@constCast(range.name));
            if (range.start) |value| alloc.free(@constCast(value));
            if (range.end) |value| alloc.free(@constCast(value));
        }
        if (self.ranges.len > 0) alloc.free(self.ranges);
        if (self.children.len > 0) alloc.free(self.children);
        self.* = undefined;
    }
};

const HistogramCardinalityPlan = struct {
    kind: db_mod.algebraic.index.CardinalityHistogramKind,
    interval: f64 = 0,
    date_bucket: []const u8 = "",
    children: []db_mod.algebraic.index.CardinalityChildRequest,

    fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.date_bucket.len > 0) alloc.free(@constCast(self.date_bucket));
        if (self.children.len > 0) alloc.free(self.children);
        self.* = undefined;
    }
};

fn algebraicTermsCardinalityChildRequestsAlloc(
    alloc: std.mem.Allocator,
    request: db_mod.aggregations.SearchAggregationRequest,
) !?[]db_mod.algebraic.index.CardinalityChildRequest {
    if (!std.mem.eql(u8, request.type, "terms") or request.field.len == 0 or request.background_query != null) return null;
    var count: usize = 0;
    for (request.aggregations) |child| {
        if (db_mod.aggregations.isPipelineAggregation(child.type)) continue;
        if (!std.mem.eql(u8, child.type, "cardinality") or child.field.len == 0) return null;
        count += 1;
    }
    if (count == 0) return null;
    const children = try alloc.alloc(db_mod.algebraic.index.CardinalityChildRequest, count);
    var idx: usize = 0;
    for (request.aggregations) |child| {
        if (db_mod.aggregations.isPipelineAggregation(child.type)) continue;
        children[idx] = .{ .name = child.name, .field = child.field };
        idx += 1;
    }
    return children;
}

fn algebraicDistributedTensorProgramForAggregationRequestAlloc(
    alloc: std.mem.Allocator,
    index: *db_mod.algebraic.index.Index,
    request: db_mod.aggregations.SearchAggregationRequest,
    constraints: []const db_mod.aggregations.FixedConstraint,
    identity_read_generation: ?u64,
) !?algebraic_planner.TensorProgramQueryPlan {
    _ = identity_read_generation;

    const ir_constraints = try algebraicIrConstraintsFromFixedAlloc(alloc, constraints);
    defer if (ir_constraints.len > 0) alloc.free(ir_constraints);

    if (std.mem.eql(u8, request.type, "cardinality")) {
        return try algebraic_planner.planCardinalityPartialsTensorProgramAlloc(alloc, index, request.name, request.field, ir_constraints);
    }
    if (try algebraicTermsCardinalityChildRequestsAlloc(alloc, request)) |children| {
        defer alloc.free(children);
        return try algebraic_planner.planTermsCardinalityPartialsTensorProgramAlloc(alloc, index, request.name, request.field, children, ir_constraints);
    }
    if (try algebraicRangeCardinalityPlanAlloc(alloc, request)) |range_cardinality_value| {
        var range_cardinality = range_cardinality_value;
        defer range_cardinality.deinit(alloc);
        return try algebraic_planner.planRangeCardinalityPartialsTensorProgramAlloc(
            alloc,
            index,
            request.name,
            request.field,
            range_cardinality.kind,
            range_cardinality.ranges,
            range_cardinality.children,
            ir_constraints,
        );
    }
    if (try algebraicHistogramCardinalityPlanAlloc(alloc, request)) |histogram_cardinality_value| {
        var histogram_cardinality = histogram_cardinality_value;
        defer histogram_cardinality.deinit(alloc);
        return try algebraic_planner.planHistogramCardinalityPartialsTensorProgramAlloc(
            alloc,
            index,
            request.name,
            request.field,
            histogram_cardinality.kind,
            histogram_cardinality.interval,
            histogram_cardinality.date_bucket,
            histogram_cardinality.children,
            ir_constraints,
        );
    }
    if (try algebraicDistributedTensorProgramForRequestAlloc(alloc, index, request, constraints)) |plan| {
        return plan;
    }
    const materializations = (try algebraicDistributedMaterializationsForRequestAlloc(alloc, index, request, constraints)) orelse return null;
    defer db_mod.aggregations.freeAlgebraicDistributedMaterializations(alloc, materializations);
    return try algebraic_planner.planMaterializationPartialsTensorProgramAlloc(alloc, index, materializations);
}

fn algebraicHistogramCardinalityPlanAlloc(
    alloc: std.mem.Allocator,
    request: db_mod.aggregations.SearchAggregationRequest,
) !?HistogramCardinalityPlan {
    if (request.field.len == 0 or request.background_query != null) return null;
    const kind: db_mod.algebraic.index.CardinalityHistogramKind = if (std.mem.eql(u8, request.type, "histogram")) blk: {
        if (request.interval <= 0) return null;
        break :blk .numeric;
    } else if (std.mem.eql(u8, request.type, "date_histogram")) blk: {
        if (db_mod.aggregations.algebraicBucketName(request) == null) return null;
        break :blk .date;
    } else return null;

    var child_count: usize = 0;
    for (request.aggregations) |child| {
        if (db_mod.aggregations.isPipelineAggregation(child.type)) continue;
        if (!std.mem.eql(u8, child.type, "cardinality") or child.field.len == 0) return null;
        child_count += 1;
    }
    if (child_count == 0) return null;

    const children = try alloc.alloc(db_mod.algebraic.index.CardinalityChildRequest, child_count);
    var child_idx: usize = 0;
    for (request.aggregations) |child| {
        if (db_mod.aggregations.isPipelineAggregation(child.type)) continue;
        children[child_idx] = .{ .name = child.name, .field = child.field };
        child_idx += 1;
    }
    const date_bucket = if (kind == .date) try alloc.dupe(u8, db_mod.aggregations.algebraicBucketName(request).?) else "";
    errdefer if (date_bucket.len > 0) alloc.free(date_bucket);
    return .{
        .kind = kind,
        .interval = if (kind == .numeric) request.interval else 0,
        .date_bucket = date_bucket,
        .children = children,
    };
}

fn algebraicRangeCardinalityPlanAlloc(
    alloc: std.mem.Allocator,
    request: db_mod.aggregations.SearchAggregationRequest,
) !?RangeCardinalityPlan {
    if (request.field.len == 0 or request.background_query != null or request.distance_ranges.len > 0) return null;
    const kind: db_mod.algebraic.index.CardinalityRangeKind = if (std.mem.eql(u8, request.type, "range")) blk: {
        if (request.ranges.len == 0 or request.date_ranges.len > 0) return null;
        break :blk .numeric;
    } else if (std.mem.eql(u8, request.type, "date_range")) blk: {
        if (request.date_ranges.len == 0 or request.ranges.len > 0) return null;
        break :blk .date;
    } else return null;

    var child_count: usize = 0;
    for (request.aggregations) |child| {
        if (db_mod.aggregations.isPipelineAggregation(child.type)) continue;
        if (!std.mem.eql(u8, child.type, "cardinality") or child.field.len == 0) return null;
        child_count += 1;
    }
    if (child_count == 0) return null;

    const children = try alloc.alloc(db_mod.algebraic.index.CardinalityChildRequest, child_count);
    errdefer if (children.len > 0) alloc.free(children);
    var child_idx: usize = 0;
    for (request.aggregations) |child| {
        if (db_mod.aggregations.isPipelineAggregation(child.type)) continue;
        children[child_idx] = .{ .name = child.name, .field = child.field };
        child_idx += 1;
    }

    const range_count = if (kind == .numeric) request.ranges.len else request.date_ranges.len;
    const ranges = try alloc.alloc(db_mod.algebraic.index.CardinalityRangeRequest, range_count);
    var ranges_initialized: usize = 0;
    errdefer {
        for (ranges[0..ranges_initialized]) |range| {
            alloc.free(@constCast(range.name));
            if (range.start) |value| alloc.free(@constCast(value));
            if (range.end) |value| alloc.free(@constCast(value));
        }
        if (ranges.len > 0) alloc.free(ranges);
    }
    if (kind == .numeric) {
        for (request.ranges, 0..) |range, i| {
            const start_text = if (range.start) |value| try std.fmt.allocPrint(alloc, "{d}", .{value}) else null;
            errdefer if (start_text) |text| alloc.free(text);
            const end_text = if (range.end) |value| try std.fmt.allocPrint(alloc, "{d}", .{value}) else null;
            errdefer if (end_text) |text| alloc.free(text);
            ranges[i] = .{
                .name = try alloc.dupe(u8, range.name),
                .start = start_text,
                .end = end_text,
            };
            ranges_initialized += 1;
        }
    } else {
        for (request.date_ranges, 0..) |range, i| {
            ranges[i] = .{
                .name = try alloc.dupe(u8, range.name),
                .start = if (range.start) |value| try alloc.dupe(u8, value) else null,
                .end = if (range.end) |value| try alloc.dupe(u8, value) else null,
            };
            ranges_initialized += 1;
        }
    }
    return .{
        .kind = kind,
        .ranges = ranges,
        .children = children,
    };
}

fn algebraicDistributedMaterializationsForRequestAlloc(
    alloc: std.mem.Allocator,
    index: *db_mod.algebraic.index.Index,
    request: db_mod.aggregations.SearchAggregationRequest,
    constraints: []const db_mod.aggregations.FixedConstraint,
) !?[][]const u8 {
    if (std.mem.eql(u8, request.type, "stats")) {
        return try db_mod.aggregations.algebraicDistributedStatsMaterializationsAlloc(alloc, index, request, constraints);
    }
    if (db_mod.algebraic.algebra.Op.parse(request.type) != null) {
        return try db_mod.aggregations.algebraicDistributedMetricMaterializationsAlloc(alloc, index, request, constraints);
    }
    if (std.mem.eql(u8, request.type, "date_histogram")) {
        return try db_mod.aggregations.algebraicDistributedDateHistogramMaterializationsAlloc(alloc, index, request, constraints);
    }
    if (std.mem.eql(u8, request.type, "histogram")) {
        return try db_mod.aggregations.algebraicDistributedHistogramMaterializationsAlloc(alloc, index, request, constraints);
    }
    if (std.mem.eql(u8, request.type, "range")) {
        return try db_mod.aggregations.algebraicDistributedRangeMaterializationsAlloc(alloc, index, request, constraints);
    }
    if (std.mem.eql(u8, request.type, "date_range")) {
        return try db_mod.aggregations.algebraicDistributedDateRangeMaterializationsAlloc(alloc, index, request, constraints);
    }
    if (std.mem.eql(u8, request.type, "terms")) {
        return try db_mod.aggregations.algebraicDistributedTermsMaterializationsAlloc(alloc, index, request, constraints);
    }
    return null;
}

fn algebraicDistributedTensorProgramForRequestAlloc(
    alloc: std.mem.Allocator,
    index: *db_mod.algebraic.index.Index,
    request: db_mod.aggregations.SearchAggregationRequest,
    constraints: []const db_mod.aggregations.FixedConstraint,
) !?algebraic_planner.TensorProgramQueryPlan {
    if (request.algebraic_join) |join_ref| {
        return try algebraicDistributedJoinTensorProgramForRequestAlloc(alloc, index, request, constraints, join_ref);
    }
    if (db_mod.algebraic.algebra.Op.parse(request.type)) |op| {
        return try algebraic_planner.planMetricTensorProgramAlloc(alloc, index, .{
            .kind = .metric,
            .aggregation_name = request.name,
            .constraints = constraints,
            .metric = .{ .name = request.name, .op = op, .field = request.field },
        });
    }

    if (std.mem.eql(u8, request.type, "terms")) {
        if (request.field.len == 0 or request.background_query != null) return null;
        const bucket_field = index.fieldConfig(request.field, .group) orelse {
            const child_metrics = try algebraicDistributedChildMetricsAlloc(alloc, request.aggregations);
            defer if (child_metrics.len > 0) alloc.free(child_metrics);
            const ir_constraints = try algebraicIrConstraintsFromFixedAlloc(alloc, constraints);
            defer if (ir_constraints.len > 0) alloc.free(ir_constraints);
            if (jsonPointerField(request.field) != null and pathFactChildMetricsSupported(child_metrics)) {
                return try planPathFactTermsTensorProgramAlloc(alloc, index, request, request.field, ir_constraints, child_metrics);
            }
            return null;
        };
        if (algebraicConstraintValueForField(index, constraints, bucket_field.name) != null) return null;
        const child_metrics = try algebraicDistributedChildMetricsAlloc(alloc, request.aggregations);
        defer if (child_metrics.len > 0) alloc.free(child_metrics);
        return try algebraic_planner.planBucketQueryMultiOutputTensorProgramAlloc(alloc, index, .{
            .kind = .terms,
            .aggregation_name = request.name,
            .bucket_field = bucket_field.name,
            .constraints = constraints,
            .child_metrics = child_metrics,
        });
    }

    if (std.mem.eql(u8, request.type, "date_histogram")) {
        if (request.field.len == 0) return null;
        const time_field = index.fieldConfig(request.field, .time) orelse return null;
        const bucket_name = db_mod.aggregations.algebraicBucketName(request) orelse return null;
        const child_metrics = try algebraicDistributedChildMetricsAlloc(alloc, request.aggregations);
        defer if (child_metrics.len > 0) alloc.free(child_metrics);
        return try algebraic_planner.planBucketQueryMultiOutputTensorProgramAlloc(alloc, index, .{
            .kind = .date_histogram,
            .aggregation_name = request.name,
            .time_field = time_field.name,
            .time_bucket = bucket_name,
            .constraints = constraints,
            .child_metrics = child_metrics,
        });
    }

    if (std.mem.eql(u8, request.type, "date_range")) {
        if (request.field.len == 0 or request.date_ranges.len == 0 or request.ranges.len > 0 or request.distance_ranges.len > 0) return null;
        const child_metrics = try algebraicDistributedChildMetricsAlloc(alloc, request.aggregations);
        defer if (child_metrics.len > 0) alloc.free(child_metrics);
        const ir_constraints = try algebraicIrConstraintsFromFixedAlloc(alloc, constraints);
        defer if (ir_constraints.len > 0) alloc.free(ir_constraints);
        if (index.fieldConfig(request.field, .time)) |time_field| {
            if (!docFactChildMetricsSupported(index, child_metrics)) return null;
            return try planDocFactDateRangeTensorProgramAlloc(alloc, index, request, time_field.name, ir_constraints, child_metrics);
        }
        if (jsonPointerField(request.field) != null and pathFactChildMetricsSupported(child_metrics)) {
            return try planPathFactDateRangeTensorProgramAlloc(alloc, index, request, request.field, ir_constraints, child_metrics);
        }
        return null;
    }

    if (std.mem.eql(u8, request.type, "histogram") or std.mem.eql(u8, request.type, "range")) {
        if (request.field.len == 0) return null;
        if (std.mem.eql(u8, request.type, "histogram") and request.interval <= 0) return null;
        if (std.mem.eql(u8, request.type, "range") and (request.ranges.len == 0 or request.date_ranges.len > 0 or request.distance_ranges.len > 0)) return null;
        const bucket_field = index.fieldConfig(request.field, .group) orelse {
            const child_metrics = try algebraicDistributedChildMetricsAlloc(alloc, request.aggregations);
            defer if (child_metrics.len > 0) alloc.free(child_metrics);
            const ir_constraints = try algebraicIrConstraintsFromFixedAlloc(alloc, constraints);
            defer if (ir_constraints.len > 0) alloc.free(ir_constraints);
            if (index.fieldConfig(request.field, .measure)) |measure_field| {
                if (!docFactChildMetricsSupported(index, child_metrics)) return null;
                if (std.mem.eql(u8, request.type, "histogram")) {
                    return try planDocFactHistogramTensorProgramAlloc(alloc, index, request, measure_field.name, ir_constraints, child_metrics);
                }
                if (std.mem.eql(u8, request.type, "range")) {
                    return try planDocFactRangeTensorProgramAlloc(alloc, index, request, measure_field.name, ir_constraints, child_metrics);
                }
            }
            if (jsonPointerField(request.field) != null and pathFactChildMetricsSupported(child_metrics)) {
                if (std.mem.eql(u8, request.type, "histogram")) {
                    return try planPathFactHistogramTensorProgramAlloc(alloc, index, request, request.field, ir_constraints, child_metrics);
                }
                if (std.mem.eql(u8, request.type, "range")) {
                    return try planPathFactRangeTensorProgramAlloc(alloc, index, request, request.field, ir_constraints, child_metrics);
                }
            }
            return null;
        };
        const bucket_kind = db_mod.algebraic.value.kindFromFieldType(bucket_field.type);
        if (bucket_kind != .number and bucket_kind != .integer) return null;
        if (algebraicConstraintValueForField(index, constraints, bucket_field.name) != null) return null;
        const child_metrics = try algebraicDistributedChildMetricsAlloc(alloc, request.aggregations);
        defer if (child_metrics.len > 0) alloc.free(child_metrics);
        return try algebraic_planner.planBucketQueryMultiOutputTensorProgramAlloc(alloc, index, .{
            .kind = .terms,
            .aggregation_name = request.name,
            .bucket_field = bucket_field.name,
            .constraints = constraints,
            .child_metrics = child_metrics,
        });
    }

    return null;
}

fn algebraicDistributedJoinTensorProgramForRequestAlloc(
    alloc: std.mem.Allocator,
    index: *db_mod.algebraic.index.Index,
    request: db_mod.aggregations.SearchAggregationRequest,
    constraints: []const db_mod.aggregations.FixedConstraint,
    join_ref: algebraic_ir.JoinRef,
) !?algebraic_planner.TensorProgramQueryPlan {
    if (db_mod.algebraic.algebra.Op.parse(request.type)) |op| {
        return try algebraic_planner.planMetricTensorProgramAlloc(alloc, index, .{
            .kind = .metric,
            .aggregation_name = request.name,
            .constraints = constraints,
            .metric = .{ .name = request.name, .op = op, .field = request.field },
            .join = join_ref,
        });
    }

    if (std.mem.eql(u8, request.type, "terms")) {
        if (request.field.len == 0 or request.background_query != null) return null;
        const child_metrics = try algebraicDistributedChildMetricsAlloc(alloc, request.aggregations);
        defer if (child_metrics.len > 0) alloc.free(child_metrics);
        return try algebraic_planner.planBucketQueryMultiOutputTensorProgramAlloc(alloc, index, .{
            .kind = .terms,
            .aggregation_name = request.name,
            .bucket_field = request.field,
            .constraints = constraints,
            .child_metrics = child_metrics,
            .join = join_ref,
        });
    }

    if (std.mem.eql(u8, request.type, "date_histogram")) {
        if (request.field.len == 0) return null;
        const bucket_name = db_mod.aggregations.algebraicBucketName(request) orelse return null;
        const child_metrics = try algebraicDistributedChildMetricsAlloc(alloc, request.aggregations);
        defer if (child_metrics.len > 0) alloc.free(child_metrics);
        return try algebraic_planner.planBucketQueryMultiOutputTensorProgramAlloc(alloc, index, .{
            .kind = .date_histogram,
            .aggregation_name = request.name,
            .time_field = request.field,
            .time_bucket = bucket_name,
            .constraints = constraints,
            .child_metrics = child_metrics,
            .join = join_ref,
        });
    }

    if (std.mem.eql(u8, request.type, "histogram")) {
        if (request.field.len == 0 or request.interval <= 0) return null;
        const child_metrics = try algebraicDistributedChildMetricsAlloc(alloc, request.aggregations);
        defer if (child_metrics.len > 0) alloc.free(child_metrics);
        return try algebraic_planner.planBucketQueryMultiOutputTensorProgramAlloc(alloc, index, .{
            .kind = .histogram,
            .aggregation_name = request.name,
            .bucket_field = request.field,
            .bucket_interval = request.interval,
            .constraints = constraints,
            .child_metrics = child_metrics,
            .join = join_ref,
        });
    }

    if (std.mem.eql(u8, request.type, "range")) {
        if (request.field.len == 0 or request.ranges.len == 0 or request.date_ranges.len > 0 or request.distance_ranges.len > 0) return null;
        return try planDerivedJoinRangeTensorProgramAlloc(alloc, index, request, constraints, join_ref, .numeric);
    }

    if (std.mem.eql(u8, request.type, "date_range")) {
        if (request.field.len == 0 or request.date_ranges.len == 0 or request.ranges.len > 0 or request.distance_ranges.len > 0) return null;
        return try planDerivedJoinRangeTensorProgramAlloc(alloc, index, request, constraints, join_ref, .date);
    }

    return null;
}

fn planDerivedJoinRangeTensorProgramAlloc(
    alloc: std.mem.Allocator,
    index: *db_mod.algebraic.index.Index,
    request: db_mod.aggregations.SearchAggregationRequest,
    constraints: []const db_mod.aggregations.FixedConstraint,
    join_ref: algebraic_ir.JoinRef,
    kind: db_mod.algebraic.index.DerivedJoinRangeKind,
) !?algebraic_planner.TensorProgramQueryPlan {
    const range_field = algebraicDistributedDerivedJoinRangeField(index, request.field, kind) orelse return null;
    const child_metrics = try algebraicDistributedChildMetricsAlloc(alloc, request.aggregations);
    defer if (child_metrics.len > 0) alloc.free(child_metrics);
    if (kind == .numeric) {
        const ranges = try algebraicNumericRangeBoundsAlloc(alloc, request.ranges);
        defer freeAlgebraicOwnedRangeBounds(alloc, ranges);
        return try algebraic_planner.planDerivedJoinRangeTensorProgramAlloc(alloc, index, request.name, join_ref, range_field, ranges, child_metrics, constraints);
    }
    const ranges = try algebraicDateRangeBoundsAlloc(alloc, request.date_ranges);
    defer if (ranges.len > 0) alloc.free(ranges);
    return try algebraic_planner.planDerivedJoinRangeTensorProgramAlloc(alloc, index, request.name, join_ref, range_field, ranges, child_metrics, constraints);
}

fn algebraicDistributedDerivedJoinRangeField(
    index: *const db_mod.algebraic.index.Index,
    field_name: []const u8,
    kind: db_mod.algebraic.index.DerivedJoinRangeKind,
) ?algebraic_planner.DerivedJoinRangeField {
    var found: ?algebraic_planner.DerivedJoinRangeField = null;
    switch (kind) {
        .numeric => {
            if (index.fieldConfig(field_name, .measure)) |field| {
                const field_kind = db_mod.algebraic.value.kindFromFieldType(field.type);
                if (field_kind == .number or field_kind == .integer) found = .{ .name = field.name, .role = .measure, .kind = kind };
            }
            if (index.fieldConfig(field_name, .group)) |field| {
                const field_kind = db_mod.algebraic.value.kindFromFieldType(field.type);
                if (field_kind == .number or field_kind == .integer) {
                    if (found != null) return null;
                    found = .{ .name = field.name, .role = .group, .kind = kind };
                }
            }
        },
        .date => {
            if (index.fieldConfig(field_name, .time)) |field| {
                const field_kind = db_mod.algebraic.value.kindFromFieldType(field.type);
                if (field_kind == .datetime) found = .{ .name = field.name, .role = .time, .kind = kind };
            }
            if (index.fieldConfig(field_name, .group)) |field| {
                const field_kind = db_mod.algebraic.value.kindFromFieldType(field.type);
                if (field_kind == .datetime) {
                    if (found != null) return null;
                    found = .{ .name = field.name, .role = .group, .kind = kind };
                }
            }
        },
    }
    return found;
}

fn planPathFactTermsTensorProgramAlloc(
    alloc: std.mem.Allocator,
    index: *db_mod.algebraic.index.Index,
    request: db_mod.aggregations.SearchAggregationRequest,
    bucket_path: []const u8,
    constraints: []const algebraic_ir.Constraint,
    child_metrics: []const algebraic_ir.Metric,
) !?algebraic_planner.TensorProgramQueryPlan {
    return try algebraic_planner.planPathFactTermsTensorProgramAlloc(alloc, index, request.name, bucket_path, constraints, child_metrics);
}

fn algebraicDistributedChildMetricsAlloc(
    alloc: std.mem.Allocator,
    requests: []const db_mod.aggregations.SearchAggregationRequest,
) ![]algebraic_ir.Metric {
    var count: usize = 0;
    for (requests) |request| {
        if (db_mod.aggregations.isPipelineAggregation(request.type)) continue;
        if (std.mem.eql(u8, request.type, "stats")) {
            count += 4;
            continue;
        }
        if (db_mod.algebraic.algebra.Op.parse(request.type) == null) return error.UnsupportedQueryRequest;
        count += 1;
    }
    const out = try alloc.alloc(algebraic_ir.Metric, count);
    errdefer if (out.len > 0) alloc.free(out);
    var filled: usize = 0;
    for (requests) |request| {
        if (db_mod.aggregations.isPipelineAggregation(request.type)) continue;
        if (std.mem.eql(u8, request.type, "stats")) {
            out[filled] = .{ .name = request.name, .op = .avg, .field = request.field };
            out[filled + 1] = .{ .name = request.name, .op = .min, .field = request.field };
            out[filled + 2] = .{ .name = request.name, .op = .max, .field = request.field };
            out[filled + 3] = .{ .name = request.name, .op = .sumsquares, .field = request.field };
            filled += 4;
            continue;
        }
        const op = db_mod.algebraic.algebra.Op.parse(request.type) orelse return error.UnsupportedQueryRequest;
        out[filled] = .{ .name = request.name, .op = op, .field = request.field };
        filled += 1;
    }
    return out;
}

fn docFactChildMetricsSupported(index: *const db_mod.algebraic.index.Index, metrics: []const algebraic_ir.Metric) bool {
    for (metrics) |metric| {
        if (metric.op == .count) continue;
        if (metric.field.len == 0) return false;
        if (index.fieldConfig(metric.field, .measure) == null) return false;
    }
    return true;
}

fn pathFactChildMetricsSupported(metrics: []const algebraic_ir.Metric) bool {
    for (metrics) |metric| {
        if (metric.op == .count) continue;
        if (jsonPointerField(metric.field) == null) return false;
    }
    return true;
}

fn jsonPointerField(field: []const u8) ?[]const u8 {
    if (std.mem.startsWith(u8, field, "/")) return field;
    return null;
}

fn planDocFactHistogramTensorProgramAlloc(
    alloc: std.mem.Allocator,
    index: *db_mod.algebraic.index.Index,
    request: db_mod.aggregations.SearchAggregationRequest,
    measure_field: []const u8,
    constraints: []const algebraic_ir.Constraint,
    child_metrics: []const algebraic_ir.Metric,
) !?algebraic_planner.TensorProgramQueryPlan {
    return try algebraic_planner.planDocFactHistogramTensorProgramAlloc(alloc, index, request.name, measure_field, request.interval, constraints, child_metrics);
}

fn planDocFactRangeTensorProgramAlloc(
    alloc: std.mem.Allocator,
    index: *db_mod.algebraic.index.Index,
    request: db_mod.aggregations.SearchAggregationRequest,
    measure_field: []const u8,
    constraints: []const algebraic_ir.Constraint,
    child_metrics: []const algebraic_ir.Metric,
) !?algebraic_planner.TensorProgramQueryPlan {
    const ranges = try algebraicNumericRangeBoundsAlloc(alloc, request.ranges);
    defer freeAlgebraicOwnedRangeBounds(alloc, ranges);
    return try algebraic_planner.planDocFactRangeTensorProgramAlloc(alloc, index, request.name, measure_field, ranges, constraints, child_metrics);
}

fn planDocFactDateRangeTensorProgramAlloc(
    alloc: std.mem.Allocator,
    index: *db_mod.algebraic.index.Index,
    request: db_mod.aggregations.SearchAggregationRequest,
    time_field: []const u8,
    constraints: []const algebraic_ir.Constraint,
    child_metrics: []const algebraic_ir.Metric,
) !?algebraic_planner.TensorProgramQueryPlan {
    const ranges = try algebraicDateRangeBoundsAlloc(alloc, request.date_ranges);
    defer if (ranges.len > 0) alloc.free(ranges);
    return try algebraic_planner.planDocFactDateRangeTensorProgramAlloc(alloc, index, request.name, time_field, ranges, constraints, child_metrics);
}

fn planPathFactHistogramTensorProgramAlloc(
    alloc: std.mem.Allocator,
    index: *db_mod.algebraic.index.Index,
    request: db_mod.aggregations.SearchAggregationRequest,
    bucket_path: []const u8,
    constraints: []const algebraic_ir.Constraint,
    child_metrics: []const algebraic_ir.Metric,
) !?algebraic_planner.TensorProgramQueryPlan {
    return try algebraic_planner.planPathFactHistogramTensorProgramAlloc(alloc, index, request.name, bucket_path, request.interval, constraints, child_metrics);
}

fn planPathFactRangeTensorProgramAlloc(
    alloc: std.mem.Allocator,
    index: *db_mod.algebraic.index.Index,
    request: db_mod.aggregations.SearchAggregationRequest,
    bucket_path: []const u8,
    constraints: []const algebraic_ir.Constraint,
    child_metrics: []const algebraic_ir.Metric,
) !?algebraic_planner.TensorProgramQueryPlan {
    const ranges = try algebraicNumericRangeBoundsAlloc(alloc, request.ranges);
    defer freeAlgebraicOwnedRangeBounds(alloc, ranges);
    return try algebraic_planner.planPathFactRangeTensorProgramAlloc(alloc, index, request.name, bucket_path, ranges, constraints, child_metrics);
}

fn planPathFactDateRangeTensorProgramAlloc(
    alloc: std.mem.Allocator,
    index: *db_mod.algebraic.index.Index,
    request: db_mod.aggregations.SearchAggregationRequest,
    bucket_path: []const u8,
    constraints: []const algebraic_ir.Constraint,
    child_metrics: []const algebraic_ir.Metric,
) !?algebraic_planner.TensorProgramQueryPlan {
    const ranges = try algebraicDateRangeBoundsAlloc(alloc, request.date_ranges);
    defer if (ranges.len > 0) alloc.free(ranges);
    return try algebraic_planner.planPathFactDateRangeTensorProgramAlloc(alloc, index, request.name, bucket_path, ranges, constraints, child_metrics);
}

fn algebraicNumericRangeBoundsAlloc(
    alloc: std.mem.Allocator,
    ranges: []const db_mod.aggregations.NumericRangeRequest,
) ![]algebraic_planner.RangeBound {
    const out = try alloc.alloc(algebraic_planner.RangeBound, ranges.len);
    @memset(out, .{});
    errdefer freeAlgebraicOwnedRangeBounds(alloc, out);
    for (ranges, 0..) |range, i| {
        out[i] = .{
            .start = if (range.start) |value| try std.fmt.allocPrint(alloc, "{d}", .{value}) else null,
            .end = if (range.end) |value| try std.fmt.allocPrint(alloc, "{d}", .{value}) else null,
        };
    }
    return out;
}

fn algebraicDateRangeBoundsAlloc(
    alloc: std.mem.Allocator,
    ranges: []const db_mod.aggregations.DateRangeRequest,
) ![]algebraic_planner.RangeBound {
    const out = try alloc.alloc(algebraic_planner.RangeBound, ranges.len);
    errdefer alloc.free(out);
    for (ranges, 0..) |range, i| {
        out[i] = .{ .start = range.start, .end = range.end };
    }
    return out;
}

fn freeAlgebraicOwnedRangeBounds(alloc: std.mem.Allocator, ranges: []algebraic_planner.RangeBound) void {
    for (ranges) |range| {
        if (range.start) |value| alloc.free(@constCast(value));
        if (range.end) |value| alloc.free(@constCast(value));
    }
    if (ranges.len > 0) alloc.free(ranges);
}

fn algebraicIrConstraintsFromFixedAlloc(
    alloc: std.mem.Allocator,
    constraints: []const db_mod.aggregations.FixedConstraint,
) ![]algebraic_ir.Constraint {
    const out = try alloc.alloc(algebraic_ir.Constraint, constraints.len);
    errdefer if (out.len > 0) alloc.free(out);
    for (constraints, 0..) |constraint, i| {
        out[i] = .{ .field = constraint.field, .value = constraint.value };
    }
    return out;
}

fn algebraicConstraintValueForField(
    index: *const db_mod.algebraic.index.Index,
    constraints: []const db_mod.aggregations.FixedConstraint,
    field_name: []const u8,
) ?[]const u8 {
    _ = index;
    for (constraints) |constraint| {
        if (std.mem.eql(u8, constraint.field, field_name)) return constraint.value;
    }
    return null;
}

fn algebraicAggregationFromDistributedPartialsAlloc(
    alloc: std.mem.Allocator,
    index: *db_mod.algebraic.index.Index,
    request: db_mod.aggregations.SearchAggregationRequest,
    constraints: []const db_mod.aggregations.FixedConstraint,
    merged: db_mod.algebraic.distributed.MergeSet,
) !?db_mod.aggregations.SearchAggregationResult {
    if (std.mem.eql(u8, request.type, "stats")) {
        return try db_mod.aggregations.algebraicStatsAggregationFromDistributedPartialsAlloc(alloc, index, request, constraints, merged);
    }
    if (db_mod.algebraic.algebra.Op.parse(request.type) != null) {
        return try db_mod.aggregations.algebraicMetricAggregationFromDistributedPartialsAlloc(alloc, index, request, constraints, merged);
    }
    if (std.mem.eql(u8, request.type, "cardinality")) {
        return try db_mod.aggregations.algebraicCardinalityAggregationFromDistributedPartialsAlloc(alloc, request, merged);
    }
    if (std.mem.eql(u8, request.type, "date_histogram")) {
        return try db_mod.aggregations.algebraicDateHistogramAggregationFromDistributedPartialsAlloc(alloc, index, request, constraints, merged);
    }
    if (std.mem.eql(u8, request.type, "histogram")) {
        return try db_mod.aggregations.algebraicHistogramAggregationFromDistributedPartialsAlloc(alloc, index, request, constraints, merged);
    }
    if (std.mem.eql(u8, request.type, "range")) {
        return try db_mod.aggregations.algebraicRangeAggregationFromDistributedPartialsAlloc(alloc, index, request, constraints, merged);
    }
    if (std.mem.eql(u8, request.type, "date_range")) {
        return try db_mod.aggregations.algebraicDateRangeAggregationFromDistributedPartialsAlloc(alloc, index, request, constraints, merged);
    }
    if (std.mem.eql(u8, request.type, "terms")) {
        return try db_mod.aggregations.algebraicTermsAggregationFromDistributedPartialsAlloc(alloc, index, request, constraints, merged);
    }
    return null;
}

fn openCatalogAlgebraicPlannerIndex(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
    index_name: ?[]const u8,
) !?db_mod.algebraic.index.Index {
    const indexes_json = (try loadTableIndexesJson(alloc, catalog, table_name)) orelse return null;
    defer alloc.free(indexes_json);
    if (indexes_json.len == 0) return null;
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, indexes_json, .{}) catch return null;
    defer parsed.deinit();
    const root = switch (parsed.value) {
        .object => |object| object,
        else => return null,
    };

    if (index_name) |name| {
        const value = root.get(name) orelse return null;
        if (!catalogIndexValueIsAlgebraic(value)) return null;
        const config_json = try catalogAlgebraicConfigJsonAlloc(alloc, value);
        defer alloc.free(config_json);
        return try db_mod.algebraic.index.Index.open(alloc, name, config_json);
    }

    var it = root.iterator();
    while (it.next()) |entry| {
        if (!catalogIndexValueIsAlgebraic(entry.value_ptr.*)) continue;
        const config_json = try catalogAlgebraicConfigJsonAlloc(alloc, entry.value_ptr.*);
        defer alloc.free(config_json);
        return try db_mod.algebraic.index.Index.open(alloc, entry.key_ptr.*, config_json);
    }
    return null;
}

fn catalogIndexValueIsAlgebraic(value: std.json.Value) bool {
    if (value != .object) return false;
    const type_value = value.object.get("type") orelse return false;
    return type_value == .string and std.mem.eql(u8, type_value.string, "algebraic");
}

fn catalogAlgebraicConfigJsonAlloc(
    alloc: std.mem.Allocator,
    value: std.json.Value,
) ![]u8 {
    if (value != .object) return error.InvalidTableIndexMetadata;
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try out.append(alloc, '{');
    var first = true;
    var it = value.object.iterator();
    while (it.next()) |entry| {
        if (std.mem.eql(u8, entry.key_ptr.*, "type") or
            std.mem.eql(u8, entry.key_ptr.*, "name") or
            std.mem.eql(u8, entry.key_ptr.*, "description") or
            std.mem.eql(u8, entry.key_ptr.*, "enrichments") or
            std.mem.eql(u8, entry.key_ptr.*, "derive_from_schema"))
        {
            continue;
        }
        if (!first) try out.append(alloc, ',');
        first = false;
        try appendJsonString(alloc, &out, entry.key_ptr.*);
        try out.append(alloc, ':');
        const encoded = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(entry.value_ptr.*, .{})});
        defer alloc.free(encoded);
        try out.appendSlice(alloc, encoded);
    }
    try out.append(alloc, '}');
    return try out.toOwnedSlice(alloc);
}

fn collectHostedAlgebraicDistributedPartials(
    self: *HostedProvisionedTableReadSource,
    alloc: std.mem.Allocator,
    group_ids: []const u64,
    table_name: []const u8,
    req: db_mod.types.SearchRequest,
    selected_index_name: []const u8,
    access_paths: []const algebraic_ir.PhysicalAccessPath,
    tensor_program: algebraic_ir.TensorProgram,
    consistency: raft_mod.ReadConsistency,
) !?db_mod.algebraic.distributed.MergeSet {
    try tableReadsValidateDocIdentityReadyForMultiGroup(alloc, self.catalog, table_name, group_ids.len);
    if (searchRequestHasResolvedDocFilter(req)) return null;
    const body = try encodeAlgebraicPartialsRequestWithProgramAtGeneration(alloc, req.index_name orelse selected_index_name, req.identity_read_generation, access_paths, &.{}, tensor_program);
    defer alloc.free(body);
    var partials = std.ArrayListUnmanaged(db_mod.algebraic.distributed.Partial).empty;
    errdefer {
        for (partials.items) |partial| {
            alloc.free(@constCast(partial.canonical_axis));
            if (partial.metric.len > 0) alloc.free(@constCast(partial.metric));
            alloc.free(@constCast(partial.value));
        }
        partials.deinit(alloc);
    }

    for (group_ids) |group_id| {
        var route = (try table_router.resolveGroupRoute(alloc, self.catalog, self.router, group_id, routePolicyForConsistency(consistency))) orelse return null;
        defer route.deinit(alloc);
        const shard_partials = switch (route) {
            .local => blk: {
                const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
                defer alloc.free(path);
                var db = try openProvisionedQueryDbForTableWithRuntime(alloc, path, self.catalog, table_name, group_id, self.visibleRootGeneration(group_id), self.backend_runtime);
                defer db.close();
                if (!(try algebraicIndexFreshEnoughForRequest(alloc, req, &db))) return null;
                var parsed = try parseAlgebraicPartialsRequest(alloc, body);
                defer parsed.deinit(alloc);
                break :blk try collectAlgebraicPartialsFromDbForRequest(alloc, &db, parsed);
            },
            .remote => |remote| blk: {
                var response = (algebraicPartialsRemote(self.executor, alloc, remote.base_uri, group_id, table_name, body) catch return null) orelse return null;
                defer response.deinit(alloc);
                break :blk try parseAlgebraicPartialsResponse(alloc, response.json);
            },
        };
        defer if (shard_partials.len > 0) alloc.free(shard_partials);
        for (shard_partials) |partial| try partials.append(alloc, partial);
    }

    const partial_slice = try partials.toOwnedSlice(alloc);
    defer db_mod.algebraic.distributed.freePartials(alloc, partial_slice);
    return try db_mod.algebraic.distributed.mergePartialsAlloc(alloc, partial_slice);
}

fn collectTextStatsFromDbForRequest(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    request: ParsedTextStatsRequest,
) ![]const distributed_stats_mod.TextFieldStats {
    return switch (request) {
        .query_request => |owned_query| try db.collectSearchRequestTextStats(alloc, owned_query.req),
        .explicit_fields => |parsed| blk: {
            const generation = try db.currentIdentityReadGenerationForRequest(parsed.identity_read_generation);
            if (parsed.resolved_doc_filter) |filter| {
                if (parsed.identity_read_generation == null or generation != filter.context.identity_read_generation) return error.UnsupportedQueryRequest;
                if (!filter.context.namespace.eql(db.core.identity_namespace)) return error.DocIdentityNamespaceMismatch;
            }
            const explicit = try alloc.alloc(db_query_search.ExplicitTextStatRequest, parsed.items.len);
            defer alloc.free(explicit);
            for (parsed.items, 0..) |item, i| {
                explicit[i] = .{
                    .index_name = item.index_name,
                    .field = item.field,
                    .terms = item.terms,
                    .resolved_doc_filter = if (parsed.resolved_doc_filter) |filter| filter.resolved_doc_filter else null,
                };
            }
            break :blk try db.collectExplicitTextStats(alloc, explicit);
        },
        .background_fields => return error.InvalidQueryRequest,
    };
}

fn collectBackgroundTextStatsFromDbForRequest(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    request: ParsedTextStatsRequest,
) ![]const db_mod.aggregations.DistributedBackgroundTextStats {
    return switch (request) {
        .background_fields => |parsed| blk: {
            const generation = try db.currentIdentityReadGenerationForRequest(parsed.identity_read_generation);
            if (parsed.resolved_doc_filter) |filter| {
                if (parsed.identity_read_generation == null or generation != filter.context.identity_read_generation) return error.UnsupportedQueryRequest;
                if (!filter.context.namespace.eql(db.core.identity_namespace)) return error.DocIdentityNamespaceMismatch;
            }
            const explicit = try alloc.alloc(db_query_search.ExplicitBackgroundTextStatRequest, parsed.items.len);
            defer alloc.free(explicit);
            for (parsed.items, 0..) |item, i| {
                explicit[i] = .{
                    .aggregation_name = item.aggregation_name,
                    .index_name = item.index_name,
                    .field = item.field,
                    .terms = item.terms,
                    .background_query = item.background_query,
                    .resolved_doc_filter = if (parsed.resolved_doc_filter) |filter| filter.resolved_doc_filter else null,
                };
            }
            break :blk try db.collectExplicitBackgroundTextStats(alloc, explicit);
        },
        else => return error.InvalidQueryRequest,
    };
}

fn collectAlgebraicPartialsFromDbForRequest(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    request: ParsedAlgebraicPartialsRequest,
) ![]db_mod.algebraic.distributed.Partial {
    const generation = try db.currentIdentityReadGenerationForRequest(request.identity_read_generation);
    const entry = if (request.index_name) |index_name|
        db.core.index_manager.algebraicIndex(index_name) orelse return error.UnsupportedQueryRequest
    else
        db.core.index_manager.algebraicIndex(null) orelse return error.UnsupportedQueryRequest;
    if (entry.index.hasErrors() or !entry.index.plannerLifecycleReady()) return error.UnsupportedQueryRequest;
    if (!(try algebraicIndexFreshEnoughForName(alloc, request.index_name, db))) return error.UnsupportedQueryRequest;
    if (request.tensor_program) |*program| {
        const access_path_values = try algebraicTensorAccessPathValuesAlloc(alloc, request.tensor_access_paths);
        defer if (access_path_values.len > 0) alloc.free(access_path_values);
        var view = try program.asProgramAlloc(alloc);
        defer view.deinit(alloc);
        if (try entry.index.scanDistributedPartialsForTensorProgramAtGeneration(db.core.store, access_path_values, view.program, generation)) |partials| {
            return partials;
        }
        const exprs = try algebraicTensorProgramOutputExpressionsForIndexAlloc(alloc, &entry.index, request.tensor_access_paths, program);
        defer if (exprs.len > 0) alloc.free(exprs);
        return try entry.index.scanDistributedPartialsForExpressions(db.core.store, exprs);
    }
    try validateAlgebraicPartialsAccessPaths(alloc, request.tensor_access_paths, request.tensor_exprs);
    const exprs = try parsedAlgebraicTensorExpressionsAlloc(alloc, request.tensor_exprs);
    defer if (exprs.len > 0) alloc.free(exprs);
    return try entry.index.scanDistributedPartialsForExpressions(db.core.store, exprs);
}

fn collectBoundLocalTextStats(
    self: *BoundTableReadSource,
    alloc: std.mem.Allocator,
    table_name: []const u8,
    body: []const u8,
) !?query_api.QueryResponse {
    if (!std.mem.eql(u8, self.table_name, table_name)) return null;
    var parsed = try parseTextStatsRequest(alloc, table_name, body);
    defer parsed.deinit(alloc);
    return switch (parsed) {
        .background_fields => blk: {
            const stats = try collectBackgroundTextStatsFromDbForRequest(alloc, self.db, parsed);
            defer db_mod.aggregations.deinitDistributedBackgroundTextStats(alloc, stats);
            break :blk .{ .json = try encodeBackgroundTextStatsResponse(alloc, stats) };
        },
        else => blk: {
            const stats = try collectTextStatsFromDbForRequest(alloc, self.db, parsed);
            defer distributed_stats_mod.deinitTextFieldStats(alloc, stats);
            break :blk .{ .json = try encodeTextStatsResponse(alloc, stats) };
        },
    };
}

fn collectBoundLocalAlgebraicPartials(
    self: *BoundTableReadSource,
    alloc: std.mem.Allocator,
    table_name: []const u8,
    body: []const u8,
) !?query_api.QueryResponse {
    if (!std.mem.eql(u8, self.table_name, table_name)) return null;
    var parsed = try parseAlgebraicPartialsRequest(alloc, body);
    defer parsed.deinit(alloc);
    const partials = try collectAlgebraicPartialsFromDbForRequest(alloc, self.db, parsed);
    defer db_mod.algebraic.distributed.freePartials(alloc, partials);
    return .{ .json = try encodeAlgebraicPartialsResponse(alloc, partials) };
}

fn collectProvisionedHostedLocalTextStats(
    cache: ?*ProvisionedTableReadCache,
    replica_root_dir: []const u8,
    catalog: table_catalog.CatalogSource,
    alloc: std.mem.Allocator,
    group_id: u64,
    lsm_root_generation: u64,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
    table_name: []const u8,
    body: []const u8,
) !?query_api.QueryResponse {
    const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, replica_root_dir, group_id);
    defer alloc.free(path);
    var parsed = try parseTextStatsRequest(alloc, table_name, body);
    defer parsed.deinit(alloc);
    if (cache) |cached| {
        var db_lease = try cached.getOrOpen(path, catalog, group_id, lsm_root_generation, table_name);
        defer db_lease.release();
        const db = db_lease.db;
        return switch (parsed) {
            .background_fields => blk: {
                const stats = try collectBackgroundTextStatsFromDbForRequest(alloc, db, parsed);
                defer db_mod.aggregations.deinitDistributedBackgroundTextStats(alloc, stats);
                break :blk .{ .json = try encodeBackgroundTextStatsResponse(alloc, stats) };
            },
            else => blk: {
                const stats = try collectTextStatsFromDbForRequest(alloc, db, parsed);
                defer distributed_stats_mod.deinitTextFieldStats(alloc, stats);
                break :blk .{ .json = try encodeTextStatsResponse(alloc, stats) };
            },
        };
    } else {
        var db = try openProvisionedQueryDbForTableWithRuntime(alloc, path, catalog, table_name, group_id, lsm_root_generation, backend_runtime);
        defer db.close();
        return switch (parsed) {
            .background_fields => blk: {
                const stats = try collectBackgroundTextStatsFromDbForRequest(alloc, &db, parsed);
                defer db_mod.aggregations.deinitDistributedBackgroundTextStats(alloc, stats);
                break :blk .{ .json = try encodeBackgroundTextStatsResponse(alloc, stats) };
            },
            else => blk: {
                const stats = try collectTextStatsFromDbForRequest(alloc, &db, parsed);
                defer distributed_stats_mod.deinitTextFieldStats(alloc, stats);
                break :blk .{ .json = try encodeTextStatsResponse(alloc, stats) };
            },
        };
    }
}

fn collectProvisionedHostedLocalAlgebraicPartials(
    cache: ?*ProvisionedTableReadCache,
    replica_root_dir: []const u8,
    catalog: table_catalog.CatalogSource,
    alloc: std.mem.Allocator,
    group_id: u64,
    lsm_root_generation: u64,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
    table_name: []const u8,
    body: []const u8,
) !?query_api.QueryResponse {
    const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, replica_root_dir, group_id);
    defer alloc.free(path);
    var parsed = try parseAlgebraicPartialsRequest(alloc, body);
    defer parsed.deinit(alloc);
    if (cache) |cached| {
        var db_lease = try cached.getOrOpen(path, catalog, group_id, lsm_root_generation, table_name);
        defer db_lease.release();
        const partials = try collectAlgebraicPartialsFromDbForRequest(alloc, db_lease.db, parsed);
        defer db_mod.algebraic.distributed.freePartials(alloc, partials);
        return .{ .json = try encodeAlgebraicPartialsResponse(alloc, partials) };
    } else {
        var db = try openProvisionedQueryDbForTableWithRuntime(alloc, path, catalog, table_name, group_id, lsm_root_generation, backend_runtime);
        defer db.close();
        const partials = try collectAlgebraicPartialsFromDbForRequest(alloc, &db, parsed);
        defer db_mod.algebraic.distributed.freePartials(alloc, partials);
        return .{ .json = try encodeAlgebraicPartialsResponse(alloc, partials) };
    }
}

fn collectProvisionedSearchRequestTextStats(
    self: *ProvisionedTableReadSource,
    alloc: std.mem.Allocator,
    group_ids: []const u64,
    req: db_mod.types.SearchRequest,
    table_name: []const u8,
) ![]const distributed_stats_mod.TextFieldStats {
    if (!queryNeedsDistributedTextStats(req) or group_ids.len <= 1) return &.{};
    try tableReadsValidateDocIdentityReadyForMultiGroup(alloc, self.catalog, table_name, group_ids.len);
    const body = try encodeQueryTextStatsRequest(alloc, req);
    defer alloc.free(body);

    const plan = planFanout(.text_stats, self.io_impl, group_ids.len);
    recordFanoutPlan(.text_stats, plan);
    if (plan.parallel) {
        return try collectProvisionedSearchRequestTextStatsParallel(self, alloc, self.io_impl.?.io(), plan.width, group_ids, table_name, body);
    }
    if (plan.reason == .no_io) recordParallelFanoutFallback(.text_stats);

    const shard_stats = try alloc.alloc([]const distributed_stats_mod.TextFieldStats, group_ids.len);
    var initialized: usize = 0;
    defer {
        for (shard_stats[0..initialized]) |item| distributed_stats_mod.deinitTextFieldStats(alloc, item);
        alloc.free(shard_stats);
    }

    for (group_ids, 0..) |group_id, i| {
        var response = (try collectProvisionedHostedLocalTextStats(self.cache, self.replica_root_dir, self.catalog, alloc, group_id, self.visibleRootGeneration(group_id), self.backend_runtime, table_name, body)) orelse return error.TableNotFound;
        defer response.deinit(alloc);
        shard_stats[i] = try parseTextStatsResponse(alloc, response.json);
        initialized += 1;
    }

    return try mergeDistributedTextStats(alloc, shard_stats[0..initialized]);
}

fn collectHostedSearchRequestTextStats(
    self: *HostedProvisionedTableReadSource,
    alloc: std.mem.Allocator,
    group_ids: []const u64,
    req: db_mod.types.SearchRequest,
    table_name: []const u8,
    consistency: raft_mod.ReadConsistency,
) ![]const distributed_stats_mod.TextFieldStats {
    if (!queryNeedsDistributedTextStats(req) or group_ids.len <= 1) return &.{};
    try tableReadsValidateDocIdentityReadyForMultiGroup(alloc, self.catalog, table_name, group_ids.len);
    const body = try encodeQueryTextStatsRequest(alloc, req);
    defer alloc.free(body);

    const plan = planFanout(.text_stats, self.io_impl, group_ids.len);
    recordFanoutPlan(.text_stats, plan);
    if (plan.parallel) {
        return try collectHostedSearchRequestTextStatsParallel(self, alloc, self.io_impl.?.io(), plan.width, group_ids, table_name, body, consistency);
    }
    if (plan.reason == .no_io) recordParallelFanoutFallback(.text_stats);

    const shard_stats = try alloc.alloc([]const distributed_stats_mod.TextFieldStats, group_ids.len);
    var initialized: usize = 0;
    defer {
        for (shard_stats[0..initialized]) |item| distributed_stats_mod.deinitTextFieldStats(alloc, item);
        alloc.free(shard_stats);
    }

    for (group_ids, 0..) |group_id, i| {
        var route = (try table_router.resolveGroupRoute(alloc, self.catalog, self.router, group_id, routePolicyForConsistency(consistency))) orelse return error.TableNotFound;
        defer route.deinit(alloc);
        var response = switch (route) {
            .local => (try collectProvisionedHostedLocalTextStats(null, self.replica_root_dir, self.catalog, alloc, group_id, self.visibleRootGeneration(group_id), self.backend_runtime, table_name, body)) orelse return error.TableNotFound,
            .remote => |remote| (try textStatsRemote(self.executor, alloc, remote.base_uri, group_id, table_name, body)) orelse return error.TableNotFound,
        };
        defer response.deinit(alloc);
        shard_stats[i] = try parseTextStatsResponse(alloc, response.json);
        initialized += 1;
    }

    return try mergeDistributedTextStats(alloc, shard_stats[0..initialized]);
}

fn collectProvisionedAggregationTextStats(
    self: *ProvisionedTableReadSource,
    alloc: std.mem.Allocator,
    group_ids: []const u64,
    table_name: []const u8,
    req: db_mod.types.SearchRequest,
    hits: []const db_mod.types.SearchHit,
) ![]const distributed_stats_mod.TextFieldStats {
    if (group_ids.len <= 1 or req.aggregations_json.len == 0) return &.{};
    try tableReadsValidateDocIdentityReadyForMultiGroup(alloc, self.catalog, table_name, group_ids.len);
    const requests = try query_api.parseAggregationRequestsJson(alloc, req.aggregations_json);
    defer query_api.freeAggregationRequests(alloc, requests);
    const field_requests = try collectSignificantTermsFieldRequests(alloc, requests, hits);
    defer {
        for (field_requests) |*item| item.deinit(alloc);
        if (field_requests.len > 0) alloc.free(field_requests);
    }
    if (field_requests.len == 0) return &.{};
    const body = try encodeExplicitTextStatsRequestForSearchRequest(alloc, field_requests, req);
    defer alloc.free(body);

    const shard_stats = try alloc.alloc([]const distributed_stats_mod.TextFieldStats, group_ids.len);
    var initialized: usize = 0;
    defer {
        for (shard_stats[0..initialized]) |item| distributed_stats_mod.deinitTextFieldStats(alloc, item);
        alloc.free(shard_stats);
    }
    for (group_ids, 0..) |group_id, i| {
        var response = (try collectProvisionedHostedLocalTextStats(self.cache, self.replica_root_dir, self.catalog, alloc, group_id, self.visibleRootGeneration(group_id), self.backend_runtime, table_name, body)) orelse return error.TableNotFound;
        defer response.deinit(alloc);
        shard_stats[i] = try parseTextStatsResponse(alloc, response.json);
        initialized += 1;
    }
    return try mergeDistributedTextStats(alloc, shard_stats[0..initialized]);
}

fn collectProvisionedAggregationBackgroundTextStats(
    self: *ProvisionedTableReadSource,
    alloc: std.mem.Allocator,
    group_ids: []const u64,
    table_name: []const u8,
    req: db_mod.types.SearchRequest,
    hits: []const db_mod.types.SearchHit,
) ![]const db_mod.aggregations.DistributedBackgroundTextStats {
    if (group_ids.len <= 1 or req.aggregations_json.len == 0) return &.{};
    try tableReadsValidateDocIdentityReadyForMultiGroup(alloc, self.catalog, table_name, group_ids.len);
    const requests = try query_api.parseAggregationRequestsJson(alloc, req.aggregations_json);
    defer query_api.freeAggregationRequests(alloc, requests);
    const field_requests = try collectSignificantTermsBackgroundFieldRequests(alloc, requests, hits);
    defer {
        for (field_requests) |*item| item.deinit(alloc);
        if (field_requests.len > 0) alloc.free(field_requests);
    }
    if (field_requests.len == 0) return &.{};
    const body = try encodeBackgroundTextStatsRequestForSearchRequest(alloc, field_requests, req);
    defer alloc.free(body);

    const shard_stats = try alloc.alloc([]const db_mod.aggregations.DistributedBackgroundTextStats, group_ids.len);
    var initialized: usize = 0;
    defer {
        for (shard_stats[0..initialized]) |item| db_mod.aggregations.deinitDistributedBackgroundTextStats(alloc, item);
        alloc.free(shard_stats);
    }
    for (group_ids, 0..) |group_id, i| {
        var response = (try collectProvisionedHostedLocalTextStats(self.cache, self.replica_root_dir, self.catalog, alloc, group_id, self.visibleRootGeneration(group_id), self.backend_runtime, table_name, body)) orelse return error.TableNotFound;
        defer response.deinit(alloc);
        shard_stats[i] = try parseBackgroundTextStatsResponse(alloc, response.json);
        initialized += 1;
    }
    return try mergeDistributedBackgroundTextStats(alloc, shard_stats[0..initialized]);
}

fn collectHostedAggregationTextStats(
    self: *HostedProvisionedTableReadSource,
    alloc: std.mem.Allocator,
    group_ids: []const u64,
    table_name: []const u8,
    req: db_mod.types.SearchRequest,
    hits: []const db_mod.types.SearchHit,
    consistency: raft_mod.ReadConsistency,
) ![]const distributed_stats_mod.TextFieldStats {
    if (group_ids.len <= 1 or req.aggregations_json.len == 0) return &.{};
    try tableReadsValidateDocIdentityReadyForMultiGroup(alloc, self.catalog, table_name, group_ids.len);
    const requests = try query_api.parseAggregationRequestsJson(alloc, req.aggregations_json);
    defer query_api.freeAggregationRequests(alloc, requests);
    const field_requests = try collectSignificantTermsFieldRequests(alloc, requests, hits);
    defer {
        for (field_requests) |*item| item.deinit(alloc);
        if (field_requests.len > 0) alloc.free(field_requests);
    }
    if (field_requests.len == 0) return &.{};
    const body = try encodeExplicitTextStatsRequestForSearchRequest(alloc, field_requests, req);
    defer alloc.free(body);

    const shard_stats = try alloc.alloc([]const distributed_stats_mod.TextFieldStats, group_ids.len);
    var initialized: usize = 0;
    defer {
        for (shard_stats[0..initialized]) |item| distributed_stats_mod.deinitTextFieldStats(alloc, item);
        alloc.free(shard_stats);
    }
    for (group_ids, 0..) |group_id, i| {
        var route = (try table_router.resolveGroupRoute(alloc, self.catalog, self.router, group_id, routePolicyForConsistency(consistency))) orelse return error.TableNotFound;
        defer route.deinit(alloc);
        var response = switch (route) {
            .local => (try collectProvisionedHostedLocalTextStats(null, self.replica_root_dir, self.catalog, alloc, group_id, self.visibleRootGeneration(group_id), self.backend_runtime, table_name, body)) orelse return error.TableNotFound,
            .remote => |remote| (try textStatsRemote(self.executor, alloc, remote.base_uri, group_id, table_name, body)) orelse return error.TableNotFound,
        };
        defer response.deinit(alloc);
        shard_stats[i] = try parseTextStatsResponse(alloc, response.json);
        initialized += 1;
    }
    return try mergeDistributedTextStats(alloc, shard_stats[0..initialized]);
}

fn collectHostedAggregationBackgroundTextStats(
    self: *HostedProvisionedTableReadSource,
    alloc: std.mem.Allocator,
    group_ids: []const u64,
    table_name: []const u8,
    req: db_mod.types.SearchRequest,
    hits: []const db_mod.types.SearchHit,
    consistency: raft_mod.ReadConsistency,
) ![]const db_mod.aggregations.DistributedBackgroundTextStats {
    if (group_ids.len <= 1 or req.aggregations_json.len == 0) return &.{};
    try tableReadsValidateDocIdentityReadyForMultiGroup(alloc, self.catalog, table_name, group_ids.len);
    const requests = try query_api.parseAggregationRequestsJson(alloc, req.aggregations_json);
    defer query_api.freeAggregationRequests(alloc, requests);
    const field_requests = try collectSignificantTermsBackgroundFieldRequests(alloc, requests, hits);
    defer {
        for (field_requests) |*item| item.deinit(alloc);
        if (field_requests.len > 0) alloc.free(field_requests);
    }
    if (field_requests.len == 0) return &.{};
    const body = try encodeBackgroundTextStatsRequestForSearchRequest(alloc, field_requests, req);
    defer alloc.free(body);

    const shard_stats = try alloc.alloc([]const db_mod.aggregations.DistributedBackgroundTextStats, group_ids.len);
    var initialized: usize = 0;
    defer {
        for (shard_stats[0..initialized]) |item| db_mod.aggregations.deinitDistributedBackgroundTextStats(alloc, item);
        alloc.free(shard_stats);
    }
    for (group_ids, 0..) |group_id, i| {
        var route = (try table_router.resolveGroupRoute(alloc, self.catalog, self.router, group_id, routePolicyForConsistency(consistency))) orelse return error.TableNotFound;
        defer route.deinit(alloc);
        var response = switch (route) {
            .local => (try collectProvisionedHostedLocalTextStats(null, self.replica_root_dir, self.catalog, alloc, group_id, self.visibleRootGeneration(group_id), self.backend_runtime, table_name, body)) orelse return error.TableNotFound,
            .remote => |remote| (try textStatsRemote(self.executor, alloc, remote.base_uri, group_id, table_name, body)) orelse return error.TableNotFound,
        };
        defer response.deinit(alloc);
        shard_stats[i] = try parseBackgroundTextStatsResponse(alloc, response.json);
        initialized += 1;
    }
    return try mergeDistributedBackgroundTextStats(alloc, shard_stats[0..initialized]);
}

fn applyQueryPostProcessing(
    alloc: std.mem.Allocator,
    req: db_mod.types.SearchRequest,
    result: *db_mod.types.SearchResult,
    meta: *query_api.QueryResponseMeta,
    antfly_provider: ?managed_embedder.AntflyProvider,
    secret_store: ?*common_secrets.FileStore,
) !void {
    if (req.reranker == null or result.hits.len == 0) return;
    try applyReranker(alloc, req, result, meta, antfly_provider, secret_store);
}

fn applyReranker(
    alloc: std.mem.Allocator,
    req: db_mod.types.SearchRequest,
    result: *db_mod.types.SearchResult,
    meta: *query_api.QueryResponseMeta,
    antfly_provider: ?managed_embedder.AntflyProvider,
    secret_store: ?*common_secrets.FileStore,
) !void {
    const cfg = req.reranker orelse return;
    if (req.reranker_query_text.len == 0) return error.UnsupportedQueryRequest;

    const doc_template = if (cfg.template.len > 0)
        try alloc.dupe(u8, cfg.template)
    else
        try std.fmt.allocPrint(alloc, "{{{{{s}}}}}", .{cfg.field});
    defer alloc.free(doc_template);

    const rerank_count: usize = if (cfg.top_n) |top_n|
        @min(result.hits.len, top_n)
    else
        result.hits.len;

    const documents = try alloc.alloc([]const u8, rerank_count);
    defer alloc.free(documents);
    var initialized_docs: usize = 0;
    defer {
        for (documents[0..initialized_docs]) |document| alloc.free(document);
    }

    for (result.hits[0..rerank_count], 0..) |hit, i| {
        documents[i] = try renderRerankerDocument(alloc, doc_template, hit);
        initialized_docs += 1;
    }

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    var http = httpx.Client.initWithConfig(alloc, io_impl.io(), .{ .keep_alive = false });
    defer http.deinit();

    const rerank_start_ns = platform_time.monotonicNs();
    const scores = reranking_runtime.rerankDocumentsWithOptions(
        alloc,
        &http,
        cfg,
        .{ .antfly_provider = antfly_provider, .secret_store = secret_store },
        req.reranker_query_text,
        documents,
    ) catch |err| switch (err) {
        error.InvalidRerankerConfig, error.UnsupportedRerankerProvider => return error.InvalidQueryRequest,
        else => return err,
    };
    defer alloc.free(scores);
    if (scores.len != rerank_count) return error.InvalidRerankerResponse;

    for (result.hits[0..rerank_count], 0..) |*hit, i| {
        hit.score = scores[i];
    }
    std.sort.pdq(db_mod.types.SearchHit, result.hits[0..rerank_count], {}, struct {
        fn lessThan(_: void, a: db_mod.types.SearchHit, b: db_mod.types.SearchHit) bool {
            const a_score = a.score orelse 0;
            const b_score = b.score orelse 0;
            if (a_score != b_score) return a_score > b_score;
            return std.mem.order(u8, a.id, b.id) == .lt;
        }
    }.lessThan);

    if (cfg.top_n) |top_n| {
        try truncateSearchHits(alloc, result, @min(top_n, result.hits.len));
        result.total_hits = @min(result.total_hits, top_n);
    }

    meta.reranker = .{
        .model = cfg.model,
        .documents_reranked = @intCast(scores.len),
        .duration_ms = @intCast(@divTrunc(platform_time.monotonicNs() - rerank_start_ns, std.time.ns_per_ms)),
    };
}

fn renderRerankerDocument(
    alloc: std.mem.Allocator,
    doc_template: []const u8,
    hit: db_mod.types.SearchHit,
) ![]const u8 {
    const raw = hit.stored_data orelse return try alloc.dupe(u8, "");
    return template_mod.renderDocument(alloc, doc_template, raw) catch try alloc.dupe(u8, "");
}

fn truncateSearchHits(
    alloc: std.mem.Allocator,
    result: *db_mod.types.SearchResult,
    keep_len: usize,
) !void {
    if (keep_len >= result.hits.len) return;
    const old_hits = result.hits;
    var kept = try alloc.alloc(db_mod.types.SearchHit, keep_len);
    for (old_hits[0..keep_len], 0..) |hit, i| {
        kept[i] = hit;
    }
    for (old_hits[keep_len..]) |*hit| hit.deinit(alloc);
    alloc.free(old_hits);
    result.hits = kept;
}

fn queryRemote(
    executor: http_common.RequestExecutor,
    alloc: std.mem.Allocator,
    base_uri: []const u8,
    group_id: u64,
    table_name: []const u8,
    req: db_mod.types.SearchRequest,
) !db_mod.types.SearchResult {
    const vector_worker_body = try encodeAlgebraicVectorWorkerRequestForSearchRequestAlloc(alloc, req);
    defer if (vector_worker_body) |body| alloc.free(body);
    return try table_read_remote_wire.queryRemoteWithVectorWorkerBody(
        executor,
        alloc,
        base_uri,
        group_id,
        table_name,
        req,
        vector_worker_body,
    );
}

test "routed rows query plan executes over scanned owner rows with ctes" {
    const alloc = std.testing.allocator;

    var columns = [_]storage_schema.RelationalColumn{
        .{ .name = "id", .path = "id", .field_type = .keyword, .nullable = false },
        .{ .name = "status", .path = "status", .field_type = .keyword, .nullable = false },
        .{ .name = "amount", .path = "amount", .field_type = .numeric, .nullable = false },
    };
    const schema = storage_schema.TableSchema{
        .storage_mode = .relational,
        .relational_columns = columns[0..],
    };

    const FakeRoutedSource = struct {
        scan_calls: usize = 0,

        fn source(self: *@This()) TableReadSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .lookup = lookup,
                    .scan = scan,
                    .query = query,
                    .rows_query_plan = rowsQueryPlan,
                    .rows_aggregate_plan = rowsAggregatePlan,
                    .rows_window_plan = rowsWindowPlan,
                    .rows_join_plan = rowsJoinPlan,
                    .rows_lateral_plan = rowsLateralPlan,
                },
            };
        }

        fn lookup(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
            _: db_mod.types.LookupOptions,
            _: raft_mod.ReadConsistency,
        ) !?LookupResponse {
            return null;
        }

        fn query(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: db_mod.types.SearchRequest,
            _: raft_mod.ReadConsistency,
        ) !?query_api.QueryResponse {
            return null;
        }

        fn scan(
            ptr: *anyopaque,
            scan_alloc: std.mem.Allocator,
            table_name: []const u8,
            from_key: []const u8,
            to_key: []const u8,
            opts: db_mod.types.ScanOptions,
            _: raft_mod.ReadConsistency,
        ) !?ScanResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expect(opts.include_documents);
            try std.testing.expect(opts.include_all_fields);
            self.scan_calls += 1;
            const ndjson = if (std.mem.eql(u8, table_name, "orders"))
                if (std.mem.eql(u8, from_key, "") and std.mem.eql(u8, to_key, "n"))
                    "{\"key\":\"a\",\"id\":\"a\",\"status\":\"open\",\"amount\":1}\n{\"key\":\"b\",\"id\":\"b\",\"status\":\"closed\",\"amount\":9}\n"
                else if (std.mem.eql(u8, from_key, "n") and std.mem.eql(u8, to_key, ""))
                    "{\"key\":\"z\",\"id\":\"z\",\"status\":\"open\",\"amount\":7}\n"
                else if (std.mem.eql(u8, from_key, "") and std.mem.eql(u8, to_key, ""))
                    "{\"key\":\"a\",\"id\":\"a\",\"status\":\"open\",\"amount\":1}\n{\"key\":\"b\",\"id\":\"b\",\"status\":\"closed\",\"amount\":9}\n{\"key\":\"z\",\"id\":\"z\",\"status\":\"open\",\"amount\":7}\n"
                else
                    return error.UnexpectedRange
            else if (std.mem.eql(u8, table_name, "customers"))
                if (std.mem.eql(u8, from_key, "") and std.mem.eql(u8, to_key, ""))
                    "{\"key\":\"c1\",\"id\":\"c1\",\"status\":\"open\",\"name\":\"Ada\"}\n{\"key\":\"c2\",\"id\":\"c2\",\"status\":\"closed\",\"name\":\"Grace\"}\n"
                else
                    return error.UnexpectedRange
            else if (std.mem.eql(u8, table_name, "oversized")) {
                try std.testing.expectEqualStrings("", from_key);
                try std.testing.expectEqualStrings("", to_key);
                var out = std.ArrayListUnmanaged(u8).empty;
                errdefer out.deinit(scan_alloc);
                const line = "{\"key\":\"x\",\"id\":\"x\",\"status\":\"open\",\"amount\":1}\n";
                var index: usize = 0;
                while (index <= db_mod.types.default_relational_rows_cte_max_rows) : (index += 1) {
                    try out.appendSlice(scan_alloc, line);
                }
                return .{ .ndjson = try out.toOwnedSlice(scan_alloc) };
            } else return error.TableNotFound;
            return .{ .ndjson = try scan_alloc.dupe(u8, ndjson) };
        }

        fn rowsQueryPlan(
            ptr: *anyopaque,
            plan_alloc: std.mem.Allocator,
            table_name: []const u8,
            runtime_schema: storage_schema.TableSchema,
            plan: db_mod.types.RelationalRowsQueryPlan,
            consistency: raft_mod.ReadConsistency,
        ) !?db_mod.types.RelationalRowsQueryResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try rowsQueryPlanFromRoutedScansAlloc(plan_alloc, self.source(), table_name, runtime_schema, plan, consistency);
        }

        fn rowsAggregatePlan(
            ptr: *anyopaque,
            plan_alloc: std.mem.Allocator,
            table_name: []const u8,
            runtime_schema: storage_schema.TableSchema,
            plan: db_mod.types.RelationalRowsAggregatePlan,
            consistency: raft_mod.ReadConsistency,
        ) !?db_mod.types.RelationalRowsAggregateResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try rowsAggregatePlanFromRoutedScansAlloc(plan_alloc, self.source(), table_name, runtime_schema, plan, consistency);
        }

        fn rowsWindowPlan(
            ptr: *anyopaque,
            plan_alloc: std.mem.Allocator,
            table_name: []const u8,
            runtime_schema: storage_schema.TableSchema,
            plan: db_mod.types.RelationalRowsWindowPlan,
            consistency: raft_mod.ReadConsistency,
        ) !?db_mod.types.RelationalRowsWindowResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try rowsWindowPlanFromRoutedScansAlloc(plan_alloc, self.source(), table_name, runtime_schema, plan, consistency);
        }

        fn rowsJoinPlan(
            ptr: *anyopaque,
            plan_alloc: std.mem.Allocator,
            table_name: []const u8,
            runtime_schema: storage_schema.TableSchema,
            plan: db_mod.types.RelationalRowsJoinPlan,
            consistency: raft_mod.ReadConsistency,
        ) !?db_mod.types.RelationalRowsJoinResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try rowsJoinPlanFromRoutedScansAlloc(plan_alloc, self.source(), table_name, runtime_schema, plan, consistency);
        }

        fn rowsLateralPlan(
            ptr: *anyopaque,
            plan_alloc: std.mem.Allocator,
            table_name: []const u8,
            runtime_schema: storage_schema.TableSchema,
            plan: db_mod.types.RelationalRowsLateralPlan,
            consistency: raft_mod.ReadConsistency,
        ) !?db_mod.types.RelationalRowsJoinResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try rowsLateralPlanFromRoutedScansAlloc(plan_alloc, self.source(), table_name, runtime_schema, plan, consistency);
        }
    };

    var fake = FakeRoutedSource{};
    var source = fake.source();
    const cte_select = [_][]const u8{ "id", "amount", "status" };
    const cte_predicates = [_]storage_schema.RelationalCheck{.{
        .name = "status_open",
        .field = "status",
        .value_json = "\"open\"",
    }};
    const ctes = [_]db_mod.types.RelationalRowsCte{.{
        .name = "open_rows",
        .query = .{
            .predicates = cte_predicates[0..],
            .select = cte_select[0..],
            .select_all = false,
        },
    }};
    const ranges = [_]db_mod.types.RelationalRowsDocKeyRange{
        .{ .start = "", .end = "n" },
        .{ .start = "n", .end = "" },
    };
    const final_select = [_][]const u8{"id"};
    const order_by = [_]db_mod.types.RelationalRowsQueryOrder{.{
        .field = "amount",
        .direction = .desc,
    }};

    var result = (try source.rowsQueryPlan(alloc, "orders", schema, .{
        .ctes = ctes[0..],
        .ranges = ranges[0..],
        .query = .{
            .source_cte = "open_rows",
            .select = final_select[0..],
            .select_all = false,
            .order_by = order_by[0..],
            .limit = 1,
        },
    }, .read_index)).?;
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), fake.scan_calls);
    try std.testing.expectEqual(@as(u32, 2), result.total);
    try std.testing.expectEqual(@as(usize, 1), result.rows.len);
    try std.testing.expectEqualStrings("{\"id\":\"z\"}", result.rows[0]);

    const aggregate_group_by = [_][]const u8{"status"};
    const aggregate_specs = [_]db_mod.types.RelationalRowsAggregateSpec{
        .{ .name = "order_count", .op = .count },
        .{ .name = "amount_sum", .op = .sum, .field = "amount" },
    };
    var aggregate_result = (try source.rowsAggregatePlan(alloc, "orders", schema, .{
        .ctes = ctes[0..],
        .ranges = ranges[0..],
        .aggregate = .{
            .source = .{ .source_cte = "open_rows" },
            .group_by = aggregate_group_by[0..],
            .aggregations = aggregate_specs[0..],
        },
    }, .read_index)).?;
    defer aggregate_result.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 1), aggregate_result.total_groups);
    try std.testing.expectEqualStrings("{\"status\":\"open\",\"order_count\":2,\"amount_sum\":8}", aggregate_result.rows[0]);

    const window_specs = [_]db_mod.types.RelationalRowsWindowSpec{.{
        .output = "rn",
        .function = .row_number,
        .order_by = &.{.{ .field = "amount", .direction = .desc }},
    }};
    const window_select = [_][]const u8{ "id", "amount" };
    var window_result = (try source.rowsWindowPlan(alloc, "orders", schema, .{
        .ctes = ctes[0..],
        .ranges = ranges[0..],
        .window = .{
            .source = .{ .source_cte = "open_rows" },
            .windows = window_specs[0..],
            .select = window_select[0..],
            .select_all = false,
            .order_by = &.{.{ .field = "rn", .direction = .asc }},
        },
    }, .read_index)).?;
    defer window_result.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 2), window_result.total_rows);
    try std.testing.expectEqualStrings("{\"id\":\"z\",\"amount\":7,\"rn\":1}", window_result.rows[0]);
    try std.testing.expectEqualStrings("{\"id\":\"a\",\"amount\":1,\"rn\":2}", window_result.rows[1]);

    const join_on = [_]db_mod.types.RelationalRowsJoinOn{.{
        .left_field = "status",
        .right_field = "status",
    }};
    const join_select = [_]db_mod.types.RelationalRowsJoinProjection{
        .{ .output = "left_id", .side = .left, .field = "id" },
        .{ .output = "right_id", .side = .right, .field = "id" },
    };
    var join_result = (try source.rowsJoinPlan(alloc, "orders", schema, .{
        .ctes = ctes[0..],
        .join = .{
            .left = .{ .source_cte = "open_rows" },
            .right = .{},
            .on = join_on[0..],
            .select = join_select[0..],
            .order_by = &.{ .{ .field = "left_id", .direction = .asc }, .{ .field = "right_id", .direction = .asc } },
            .limit = 1,
        },
    }, .read_index)).?;
    defer join_result.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 4), join_result.total_rows);
    try std.testing.expectEqualStrings("{\"left_id\":\"a\",\"right_id\":\"a\"}", join_result.rows[0]);

    const lateral_correlations = [_]db_mod.types.RelationalRowsLateralCorrelation{.{
        .left_field = "status",
        .right_field = "status",
    }};
    const lateral_select = [_]db_mod.types.RelationalRowsJoinProjection{
        .{ .output = "left_id", .side = .left, .field = "id" },
        .{ .output = "latest_id", .side = .right, .field = "id" },
        .{ .output = "latest_amount", .side = .right, .field = "amount" },
    };
    var lateral_result = (try source.rowsLateralPlan(alloc, "orders", schema, .{
        .ctes = ctes[0..],
        .lateral = .{
            .left = .{ .source_cte = "open_rows", .order_by = &.{.{ .field = "id", .direction = .asc }} },
            .right = .{ .order_by = &.{.{ .field = "amount", .direction = .desc }}, .limit = 1 },
            .correlations = lateral_correlations[0..],
            .select = lateral_select[0..],
            .order_by = &.{.{ .field = "left_id", .direction = .asc }},
        },
    }, .read_index)).?;
    defer lateral_result.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 2), lateral_result.total_rows);
    try std.testing.expectEqualStrings("{\"left_id\":\"a\",\"latest_id\":\"z\",\"latest_amount\":7}", lateral_result.rows[0]);
    try std.testing.expectEqualStrings("{\"left_id\":\"z\",\"latest_id\":\"z\",\"latest_amount\":7}", lateral_result.rows[1]);

    var customer_columns = [_]storage_schema.RelationalColumn{
        .{ .name = "id", .path = "id", .field_type = .keyword, .nullable = false },
        .{ .name = "status", .path = "status", .field_type = .keyword, .nullable = false },
        .{ .name = "name", .path = "name", .field_type = .keyword, .nullable = false },
    };
    const customer_schema = storage_schema.TableSchema{
        .storage_mode = .relational,
        .relational_columns = customer_columns[0..],
    };
    const cross_join_select = [_]db_mod.types.RelationalRowsJoinProjection{
        .{ .output = "order_id", .side = .left, .field = "id" },
        .{ .output = "customer_name", .side = .right, .field = "name" },
    };
    const cross_scan_calls_before = fake.scan_calls;
    var cross_join_result = (try rowsJoinPlanFromRoutedScansWithSchemasAlloc(alloc, source, "orders", "orders", "customers", schema, schema, customer_schema, .{
        .right_table = "customers",
        .join = .{
            .left = .{},
            .right = .{},
            .on = join_on[0..],
            .select = cross_join_select[0..],
            .order_by = &.{.{ .field = "order_id", .direction = .asc }},
        },
    }, .read_index)).?;
    defer cross_join_result.deinit(alloc);
    try std.testing.expectEqual(cross_scan_calls_before + 2, fake.scan_calls);
    try std.testing.expectEqual(@as(u32, 3), cross_join_result.total_rows);
    try std.testing.expectEqualStrings("{\"order_id\":\"a\",\"customer_name\":\"Ada\"}", cross_join_result.rows[0]);
    try std.testing.expectEqualStrings("{\"order_id\":\"b\",\"customer_name\":\"Grace\"}", cross_join_result.rows[1]);
    try std.testing.expectEqualStrings("{\"order_id\":\"z\",\"customer_name\":\"Ada\"}", cross_join_result.rows[2]);

    const cross_lateral_select = [_]db_mod.types.RelationalRowsJoinProjection{
        .{ .output = "order_id", .side = .left, .field = "id" },
        .{ .output = "matched_customer", .side = .right, .field = "name" },
    };
    const cross_lateral_scan_calls_before = fake.scan_calls;
    var cross_lateral_result = (try rowsLateralPlanFromRoutedScansWithSchemasAlloc(alloc, source, "orders", "orders", "customers", schema, schema, customer_schema, .{
        .right_table = "customers",
        .lateral = .{
            .left = .{ .order_by = &.{.{ .field = "id", .direction = .asc }} },
            .right = .{ .order_by = &.{.{ .field = "name", .direction = .asc }}, .limit = 1 },
            .correlations = lateral_correlations[0..],
            .select = cross_lateral_select[0..],
            .order_by = &.{.{ .field = "order_id", .direction = .asc }},
        },
    }, .read_index)).?;
    defer cross_lateral_result.deinit(alloc);
    try std.testing.expectEqual(cross_lateral_scan_calls_before + 2, fake.scan_calls);
    try std.testing.expectEqual(@as(u32, 3), cross_lateral_result.total_rows);
    try std.testing.expectEqualStrings("{\"order_id\":\"a\",\"matched_customer\":\"Ada\"}", cross_lateral_result.rows[0]);
    try std.testing.expectEqualStrings("{\"order_id\":\"b\",\"matched_customer\":\"Grace\"}", cross_lateral_result.rows[1]);
    try std.testing.expectEqualStrings("{\"order_id\":\"z\",\"matched_customer\":\"Ada\"}", cross_lateral_result.rows[2]);

    var key_columns = [_]storage_schema.RelationalColumn{
        .{ .name = "key", .path = "key", .field_type = .keyword, .nullable = false },
    };
    const key_schema = storage_schema.TableSchema{
        .storage_mode = .relational,
        .relational_columns = key_columns[0..],
    };
    try std.testing.expectError(error.UnsupportedRowsQuery, source.rowsQueryPlan(alloc, "orders", key_schema, .{}, .read_index));
    try std.testing.expectError(error.UnsupportedRowsQuery, source.rowsQueryPlan(alloc, "oversized", schema, .{}, .read_index));
}

test "external lake rows query and aggregate plans route through lake scan hook" {
    const alloc = std.testing.allocator;

    var columns = [_]storage_schema.RelationalColumn{
        .{ .name = "id", .path = "id", .field_type = .keyword, .nullable = false },
        .{ .name = "tenant", .path = "tenant", .field_type = .keyword, .nullable = false },
        .{ .name = "amount", .path = "amount", .field_type = .numeric, .nullable = false },
        .{ .name = "attrs", .path = "attrs", .field_type = .json, .nullable = false },
        .{ .name = "tags", .path = "tags", .field_type = .array, .nullable = false },
        .{ .name = "nickname", .path = "nickname", .field_type = .keyword, .nullable = true },
    };
    const schema = storage_schema.TableSchema{
        .storage_mode = .relational,
        .relational_columns = columns[0..],
        .primary_key = .{ .columns = &.{"id"} },
        .external_base_source = .{
            .table_id = "events",
            .format = .parquet,
            .source_uri = "s3://bucket/events",
            .snapshot_mode = .{ .object_version_digest = "digest-1" },
            .schema_fingerprint = "schema-v1",
        },
    };

    const FakeLakeSource = struct {
        lake_scan_calls: usize = 0,
        lake_expression_aggregate_calls: usize = 0,
        routed_scan_calls: usize = 0,
        last_scan_limit: ?usize = null,

        fn source(self: *@This()) TableReadSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .lookup = lookup,
                    .scan = scan,
                    .query = query,
                    .rows_query_plan = rowsQueryPlan,
                    .rows_set_operation_plan = rowsSetOperationPlan,
                    .rows_aggregate_plan = rowsAggregatePlan,
                    .rows_window_plan = rowsWindowPlan,
                    .rows_join_plan = rowsJoinPlan,
                    .rows_lateral_plan = rowsLateralPlan,
                    .lake_rows_scan = lakeRowsScan,
                    .lake_rows_expression_aggregates = lakeRowsExpressionAggregates,
                },
            };
        }

        fn lookup(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
            _: db_mod.types.LookupOptions,
            _: raft_mod.ReadConsistency,
        ) !?LookupResponse {
            return null;
        }

        fn query(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: db_mod.types.SearchRequest,
            _: raft_mod.ReadConsistency,
        ) !?query_api.QueryResponse {
            return null;
        }

        fn scan(
            ptr: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
            _: []const u8,
            _: db_mod.types.ScanOptions,
            _: raft_mod.ReadConsistency,
        ) !?ScanResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.routed_scan_calls += 1;
            return error.UnexpectedRoutedScanForLakeTable;
        }

        fn rowsQueryPlan(
            ptr: *anyopaque,
            plan_alloc: std.mem.Allocator,
            table_name: []const u8,
            runtime_schema: storage_schema.TableSchema,
            plan: db_mod.types.RelationalRowsQueryPlan,
            consistency: raft_mod.ReadConsistency,
        ) !?db_mod.types.RelationalRowsQueryResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try rowsQueryPlanFromRoutedScansAlloc(plan_alloc, self.source(), table_name, runtime_schema, plan, consistency);
        }

        fn rowsSetOperationPlan(
            ptr: *anyopaque,
            plan_alloc: std.mem.Allocator,
            table_name: []const u8,
            runtime_schema: storage_schema.TableSchema,
            plan: db_mod.types.RelationalRowsSetOperationPlan,
            consistency: raft_mod.ReadConsistency,
        ) !?db_mod.types.RelationalRowsQueryResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try rowsSetOperationPlanFromRoutedScansAlloc(plan_alloc, self.source(), table_name, runtime_schema, plan, consistency);
        }

        fn rowsAggregatePlan(
            ptr: *anyopaque,
            plan_alloc: std.mem.Allocator,
            table_name: []const u8,
            runtime_schema: storage_schema.TableSchema,
            plan: db_mod.types.RelationalRowsAggregatePlan,
            consistency: raft_mod.ReadConsistency,
        ) !?db_mod.types.RelationalRowsAggregateResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try rowsAggregatePlanFromRoutedScansAlloc(plan_alloc, self.source(), table_name, runtime_schema, plan, consistency);
        }

        fn rowsWindowPlan(
            ptr: *anyopaque,
            plan_alloc: std.mem.Allocator,
            table_name: []const u8,
            runtime_schema: storage_schema.TableSchema,
            plan: db_mod.types.RelationalRowsWindowPlan,
            consistency: raft_mod.ReadConsistency,
        ) !?db_mod.types.RelationalRowsWindowResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try rowsWindowPlanFromRoutedScansAlloc(plan_alloc, self.source(), table_name, runtime_schema, plan, consistency);
        }

        fn rowsJoinPlan(
            ptr: *anyopaque,
            plan_alloc: std.mem.Allocator,
            table_name: []const u8,
            runtime_schema: storage_schema.TableSchema,
            plan: db_mod.types.RelationalRowsJoinPlan,
            consistency: raft_mod.ReadConsistency,
        ) !?db_mod.types.RelationalRowsJoinResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try rowsJoinPlanFromRoutedScansAlloc(plan_alloc, self.source(), table_name, runtime_schema, plan, consistency);
        }

        fn rowsLateralPlan(
            ptr: *anyopaque,
            plan_alloc: std.mem.Allocator,
            table_name: []const u8,
            runtime_schema: storage_schema.TableSchema,
            plan: db_mod.types.RelationalRowsLateralPlan,
            consistency: raft_mod.ReadConsistency,
        ) !?db_mod.types.RelationalRowsJoinResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try rowsLateralPlanFromRoutedScansAlloc(plan_alloc, self.source(), table_name, runtime_schema, plan, consistency);
        }

        fn lakeRowsScan(
            ptr: *anyopaque,
            scan_alloc: std.mem.Allocator,
            table_name: []const u8,
            runtime_schema: storage_schema.TableSchema,
            request: serverless_query.LakeRowsScanRequest,
            consistency: raft_mod.ReadConsistency,
        ) !?serverless_query.LakeRowsScanResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("events", table_name);
            try std.testing.expect(runtime_schema.external_base_source != null);
            try std.testing.expectEqual(raft_mod.ReadConsistency.read_index, consistency);
            if (request.predicate) |predicate| {
                if (std.mem.eql(u8, predicate.column, "tenant")) {
                    try std.testing.expectEqual(serverless_query.LakeRowsPredicateOp.eq_bytes, predicate.op);
                    try std.testing.expectEqualStrings("t2", predicate.bytes_value);
                } else if (std.mem.eql(u8, predicate.column, "amount")) {
                    try std.testing.expectEqual(serverless_query.LakeRowsPredicateOp.eq_f64, predicate.op);
                    try std.testing.expectEqual(@as(f64, 20.5), predicate.f64_value);
                } else return error.UnexpectedLakePredicate;
            }
            self.lake_scan_calls += 1;
            self.last_scan_limit = request.limit;
            return try buildRows(scan_alloc, request.projected_columns);
        }

        fn lakeRowsExpressionAggregates(
            ptr: *anyopaque,
            expression_alloc: std.mem.Allocator,
            table_name: []const u8,
            runtime_schema: storage_schema.TableSchema,
            request: serverless_query.LakeRowsExpressionAggregateRequest,
            consistency: raft_mod.ReadConsistency,
        ) !?serverless_query.LakeRowsExpressionAggregateResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("events", table_name);
            try std.testing.expect(runtime_schema.external_base_source != null);
            try std.testing.expectEqual(raft_mod.ReadConsistency.read_index, consistency);
            self.lake_expression_aggregate_calls += 1;

            const expressions = try expression_alloc.alloc(serverless_query.lake_rows.ExpressionResult, request.expressions.len);
            errdefer expression_alloc.free(expressions);
            var initialized: usize = 0;
            errdefer {
                for (expressions[0..initialized]) |*expression| expression.deinit(expression_alloc);
            }
            for (request.expressions, expressions) |spec, *out| {
                out.* = .{
                    .name = try expression_alloc.dupe(u8, spec.name),
                    .value = switch (spec.op) {
                        .count => .{ .count = 2 },
                        .sum_i64 => .{ .sum_i64 = 50 },
                        .min_i64 => .{ .min_i64 = 20 },
                        .max_i64 => .{ .max_i64 = 30 },
                        .avg_i64 => .{ .avg_i64 = .{ .sum_i64 = 50, .count = 2 } },
                    },
                };
                initialized += 1;
            }
            return .{ .expressions = expressions, .source = .rowsource_scan };
        }

        fn buildRows(
            row_alloc: std.mem.Allocator,
            projected_columns: []const []const u8,
        ) !serverless_query.LakeRowsScanResult {
            if (projected_columns.len == 0) return error.InvalidLakeRowsQuery;
            const rows = try row_alloc.alloc(serverless_query.lake_rows.ProjectedRow, 2);
            errdefer row_alloc.free(rows);
            var initialized: usize = 0;
            errdefer {
                for (rows[0..initialized]) |*row| row.deinit(row_alloc);
            }
            rows[0] = try buildRow(row_alloc, "row:1", projected_columns, 20);
            initialized += 1;
            rows[1] = try buildRow(row_alloc, "row:2", projected_columns, 30);
            initialized += 1;
            return .{ .rows = rows, .total = 2 };
        }

        fn buildRow(
            row_alloc: std.mem.Allocator,
            row_key: []const u8,
            projected_columns: []const []const u8,
            amount: i64,
        ) !serverless_query.lake_rows.ProjectedRow {
            const cells = try row_alloc.alloc(serverless_query.lake_rows.ProjectedCell, projected_columns.len);
            errdefer row_alloc.free(cells);
            var initialized: usize = 0;
            errdefer {
                for (cells[0..initialized]) |*cell| cell.deinit(row_alloc);
            }
            for (projected_columns, cells) |column, *cell| {
                cell.* = .{
                    .name = try row_alloc.dupe(u8, column),
                    .value = if (std.mem.eql(u8, column, "id"))
                        .{ .bytes = try row_alloc.dupe(u8, if (amount == 20) "a" else "b") }
                    else if (std.mem.eql(u8, column, "amount"))
                        .{ .i64 = amount }
                    else if (std.mem.eql(u8, column, "tenant"))
                        .{ .bytes = try row_alloc.dupe(u8, "t2") }
                    else if (std.mem.eql(u8, column, "attrs"))
                        .{ .json = try row_alloc.dupe(u8, if (amount == 20) "{\"tier\":\"gold\",\"flags\":{\"vip\":true}}" else "{\"tier\":\"silver\",\"flags\":{\"vip\":false}}") }
                    else if (std.mem.eql(u8, column, "tags"))
                        .{ .json = try row_alloc.dupe(u8, if (amount == 20) "[\"hot\",\"vip\"]" else "[\"cold\"]") }
                    else if (std.mem.eql(u8, column, "nickname"))
                        if (amount == 20) null else .{ .bytes = try row_alloc.dupe(u8, "nick30") }
                    else
                        return error.UnexpectedLakeProjection,
                };
                initialized += 1;
            }
            return .{
                .row_ref = .{ .relational_key = row_key },
                .cells = cells,
            };
        }
    };

    var fake = FakeLakeSource{};
    var source = fake.source();
    const select = [_][]const u8{"amount"};
    const predicates = [_]storage_schema.RelationalCheck{.{
        .name = "",
        .field = "tenant",
        .value_json = "\"t2\"",
    }};

    var query_result = (try source.rowsQueryPlan(alloc, "events", schema, .{
        .query = .{
            .select = select[0..],
            .select_all = false,
            .predicates = predicates[0..],
            .limit = 5,
        },
    }, .read_index)).?;
    defer query_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), fake.lake_scan_calls);
    try std.testing.expectEqual(@as(usize, 0), fake.routed_scan_calls);
    try std.testing.expectEqual(@as(u32, 2), query_result.total);
    try std.testing.expectEqualStrings("{\"amount\":20}", query_result.rows[0]);
    try std.testing.expectEqualStrings("{\"amount\":30}", query_result.rows[1]);

    const float_predicates = [_]storage_schema.RelationalCheck{.{
        .name = "",
        .field = "amount",
        .value_json = "20.5",
    }};
    var float_query_result = (try source.rowsQueryPlan(alloc, "events", schema, .{
        .query = .{
            .select = select[0..],
            .select_all = false,
            .predicates = float_predicates[0..],
            .limit = 5,
        },
    }, .read_index)).?;
    defer float_query_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), fake.lake_scan_calls);
    try std.testing.expectEqual(@as(usize, 0), fake.routed_scan_calls);

    const residual_predicates = [_]storage_schema.RelationalCheck{.{
        .name = "",
        .field = "amount",
        .op = .gt,
        .value_json = "20",
    }};
    var residual_query_result = (try source.rowsQueryPlan(alloc, "events", schema, .{
        .query = .{
            .select = select[0..],
            .select_all = false,
            .predicates = residual_predicates[0..],
            .limit = 5,
        },
    }, .read_index)).?;
    defer residual_query_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 3), fake.lake_scan_calls);
    try std.testing.expectEqual(@as(usize, 0), fake.routed_scan_calls);
    try std.testing.expectEqual(@as(u32, 1), residual_query_result.total);
    try std.testing.expectEqual(@as(usize, 1), residual_query_result.rows.len);
    try std.testing.expectEqualStrings("{\"amount\":30}", residual_query_result.rows[0]);

    const or_amount_ten = [_]storage_schema.RelationalCheck{.{
        .name = "",
        .field = "amount",
        .value_json = "10",
    }};
    const or_amount_thirty = [_]storage_schema.RelationalCheck{.{
        .name = "",
        .field = "amount",
        .value_json = "30",
    }};
    const not_amount_twenty = [_]storage_schema.RelationalCheck{.{
        .name = "",
        .field = "amount",
        .value_json = "20",
    }};
    const or_groups = [_]db_mod.types.RelationalRowsPredicateGroup{
        .{ .predicates = or_amount_ten[0..] },
        .{ .predicates = or_amount_thirty[0..] },
    };
    const not_groups = [_]db_mod.types.RelationalRowsPredicateGroup{.{
        .predicates = not_amount_twenty[0..],
    }};
    var group_query_result = (try source.rowsQueryPlan(alloc, "events", schema, .{
        .query = .{
            .select = select[0..],
            .select_all = false,
            .or_predicates = or_groups[0..],
            .not_predicates = not_groups[0..],
            .limit = 5,
        },
    }, .read_index)).?;
    defer group_query_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 4), fake.lake_scan_calls);
    try std.testing.expectEqual(@as(usize, 0), fake.routed_scan_calls);
    try std.testing.expectEqual(@as(u32, 1), group_query_result.total);
    try std.testing.expectEqual(@as(usize, 1), group_query_result.rows.len);
    try std.testing.expectEqualStrings("{\"amount\":30}", group_query_result.rows[0]);

    const in_predicates = [_]db_mod.types.RelationalRowsInPredicate{.{
        .field = "amount",
        .values_json = "[30]",
    }};
    var in_query_result = (try source.rowsQueryPlan(alloc, "events", schema, .{
        .query = .{
            .select = select[0..],
            .select_all = false,
            .in_predicates = in_predicates[0..],
            .limit = 5,
        },
    }, .read_index)).?;
    defer in_query_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 5), fake.lake_scan_calls);
    try std.testing.expectEqual(@as(usize, 0), fake.routed_scan_calls);
    try std.testing.expectEqual(@as(u32, 1), in_query_result.total);
    try std.testing.expectEqual(@as(usize, 1), in_query_result.rows.len);
    try std.testing.expectEqualStrings("{\"amount\":30}", in_query_result.rows[0]);

    const text_patterns = [_]db_mod.types.RelationalRowsTextPatternPredicate{.{
        .field = "tenant",
        .pattern = "T_",
        .case_insensitive = true,
    }};
    var text_pattern_query_result = (try source.rowsQueryPlan(alloc, "events", schema, .{
        .query = .{
            .select = select[0..],
            .select_all = false,
            .text_patterns = text_patterns[0..],
            .limit = 5,
        },
    }, .read_index)).?;
    defer text_pattern_query_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 6), fake.lake_scan_calls);
    try std.testing.expectEqual(@as(usize, 0), fake.routed_scan_calls);
    try std.testing.expectEqual(@as(u32, 2), text_pattern_query_result.total);
    try std.testing.expectEqual(@as(usize, 2), text_pattern_query_result.rows.len);
    try std.testing.expectEqualStrings("{\"amount\":20}", text_pattern_query_result.rows[0]);
    try std.testing.expectEqualStrings("{\"amount\":30}", text_pattern_query_result.rows[1]);

    const json_contains = [_]db_mod.types.RelationalRowsJsonContainsPredicate{.{
        .field = "attrs",
        .value_json = "{\"tier\":\"silver\"}",
    }};
    var json_contains_result = (try source.rowsQueryPlan(alloc, "events", schema, .{
        .query = .{
            .select = select[0..],
            .select_all = false,
            .json_contains = json_contains[0..],
            .limit = 5,
        },
    }, .read_index)).?;
    defer json_contains_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 7), fake.lake_scan_calls);
    try std.testing.expectEqual(@as(usize, 0), fake.routed_scan_calls);
    try std.testing.expectEqual(@as(u32, 1), json_contains_result.total);
    try std.testing.expectEqual(@as(usize, 1), json_contains_result.rows.len);
    try std.testing.expectEqualStrings("{\"amount\":30}", json_contains_result.rows[0]);

    const json_path_eq = [_]db_mod.types.RelationalRowsJsonPathEqPredicate{.{
        .field = "attrs",
        .path = "flags.vip",
        .value_json = "true",
    }};
    var json_path_eq_result = (try source.rowsQueryPlan(alloc, "events", schema, .{
        .query = .{
            .select = select[0..],
            .select_all = false,
            .json_path_eq = json_path_eq[0..],
            .limit = 5,
        },
    }, .read_index)).?;
    defer json_path_eq_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 8), fake.lake_scan_calls);
    try std.testing.expectEqual(@as(usize, 0), fake.routed_scan_calls);
    try std.testing.expectEqual(@as(u32, 1), json_path_eq_result.total);
    try std.testing.expectEqual(@as(usize, 1), json_path_eq_result.rows.len);
    try std.testing.expectEqualStrings("{\"amount\":20}", json_path_eq_result.rows[0]);

    const json_path_exists = [_]db_mod.types.RelationalRowsJsonPathExistsPredicate{.{
        .field = "attrs",
        .path = "flags.vip",
    }};
    var json_path_exists_result = (try source.rowsQueryPlan(alloc, "events", schema, .{
        .query = .{
            .select = select[0..],
            .select_all = false,
            .json_path_exists = json_path_exists[0..],
            .limit = 5,
        },
    }, .read_index)).?;
    defer json_path_exists_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 9), fake.lake_scan_calls);
    try std.testing.expectEqual(@as(usize, 0), fake.routed_scan_calls);
    try std.testing.expectEqual(@as(u32, 2), json_path_exists_result.total);
    try std.testing.expectEqual(@as(usize, 2), json_path_exists_result.rows.len);
    try std.testing.expectEqualStrings("{\"amount\":20}", json_path_exists_result.rows[0]);
    try std.testing.expectEqualStrings("{\"amount\":30}", json_path_exists_result.rows[1]);

    const json_extract = [_]db_mod.types.RelationalRowsJsonExtractProjection{
        .{ .output = "tier", .field = "attrs", .path = "tier", .as_text = true },
        .{ .output = "flags", .field = "attrs", .path = "flags" },
    };
    var json_extract_result = (try source.rowsQueryPlan(alloc, "events", schema, .{
        .query = .{
            .select_all = false,
            .json_extract = json_extract[0..],
            .limit = 5,
        },
    }, .read_index)).?;
    defer json_extract_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 10), fake.lake_scan_calls);
    try std.testing.expectEqual(@as(usize, 0), fake.routed_scan_calls);
    try std.testing.expectEqual(@as(u32, 2), json_extract_result.total);
    try std.testing.expectEqual(@as(usize, 2), json_extract_result.rows.len);
    try std.testing.expectEqualStrings("{\"tier\":\"gold\",\"flags\":{\"vip\":true}}", json_extract_result.rows[0]);
    try std.testing.expectEqualStrings("{\"tier\":\"silver\",\"flags\":{\"vip\":false}}", json_extract_result.rows[1]);

    const array_length = [_]db_mod.types.RelationalRowsArrayLengthProjection{.{
        .output = "tag_count",
        .field = "tags",
    }};
    const field_aliases = [_]db_mod.types.RelationalRowsFieldAliasProjection{.{
        .output = "tenant_alias",
        .field = "tenant",
    }};
    const coalesce = [_]db_mod.types.RelationalRowsCoalesceProjection{.{
        .output = "display_name",
        .operands = &.{
            .{ .kind = .field, .field = "nickname" },
            .{ .kind = .value, .value_json = "\"fallback\"" },
        },
    }};
    var shaped_projection_result = (try source.rowsQueryPlan(alloc, "events", schema, .{
        .query = .{
            .select_all = false,
            .array_length = array_length[0..],
            .coalesce = coalesce[0..],
            .field_aliases = field_aliases[0..],
            .limit = 5,
        },
    }, .read_index)).?;
    defer shaped_projection_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 11), fake.lake_scan_calls);
    try std.testing.expectEqual(@as(usize, 0), fake.routed_scan_calls);
    try std.testing.expectEqual(@as(u32, 2), shaped_projection_result.total);
    try std.testing.expectEqual(@as(usize, 2), shaped_projection_result.rows.len);
    try std.testing.expectEqualStrings("{\"tag_count\":2,\"display_name\":\"fallback\",\"tenant_alias\":\"t2\"}", shaped_projection_result.rows[0]);
    try std.testing.expectEqualStrings("{\"tag_count\":1,\"display_name\":\"nick30\",\"tenant_alias\":\"t2\"}", shaped_projection_result.rows[1]);

    var offset_query_result = (try source.rowsQueryPlan(alloc, "events", schema, .{
        .query = .{
            .select = select[0..],
            .select_all = false,
            .offset = 1,
            .limit = 1,
        },
    }, .read_index)).?;
    defer offset_query_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 12), fake.lake_scan_calls);
    try std.testing.expectEqual(@as(?usize, 2), fake.last_scan_limit);
    try std.testing.expectEqual(@as(usize, 0), fake.routed_scan_calls);
    try std.testing.expectEqual(@as(u32, 2), offset_query_result.total);
    try std.testing.expectEqual(@as(usize, 1), offset_query_result.rows.len);
    try std.testing.expectEqualStrings("{\"amount\":30}", offset_query_result.rows[0]);

    const doc_range_start = try relational_rows_api.physicalPrimaryKeyFromRowJsonAlloc(alloc, schema, "{\"id\":\"b\"}");
    defer alloc.free(doc_range_start);
    const doc_range_end = try relational_rows_api.physicalPrimaryKeyFromRowJsonAlloc(alloc, schema, "{\"id\":\"c\"}");
    defer alloc.free(doc_range_end);
    var doc_key_range_result = (try source.rowsQueryPlan(alloc, "events", schema, .{
        .query = .{
            .select = select[0..],
            .select_all = false,
            .doc_key_range = .{ .start = doc_range_start, .end = doc_range_end },
            .limit = 1,
        },
    }, .read_index)).?;
    defer doc_key_range_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 13), fake.lake_scan_calls);
    try std.testing.expectEqual(@as(?usize, null), fake.last_scan_limit);
    try std.testing.expectEqual(@as(usize, 0), fake.routed_scan_calls);
    try std.testing.expectEqual(@as(u32, 1), doc_key_range_result.total);
    try std.testing.expectEqual(@as(usize, 1), doc_key_range_result.rows.len);
    try std.testing.expectEqualStrings("{\"amount\":30}", doc_key_range_result.rows[0]);

    const distinct_on = [_][]const u8{"tenant"};
    var unordered_distinct_query_result = (try source.rowsQueryPlan(alloc, "events", schema, .{
        .query = .{
            .select = select[0..],
            .select_all = false,
            .distinct_on = distinct_on[0..],
            .limit = 1,
        },
    }, .read_index)).?;
    defer unordered_distinct_query_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 14), fake.lake_scan_calls);
    try std.testing.expectEqual(@as(?usize, null), fake.last_scan_limit);
    try std.testing.expectEqual(@as(usize, 0), fake.routed_scan_calls);
    try std.testing.expectEqual(@as(u32, 1), unordered_distinct_query_result.total);
    try std.testing.expectEqual(@as(usize, 1), unordered_distinct_query_result.rows.len);
    try std.testing.expectEqualStrings("{\"amount\":20}", unordered_distinct_query_result.rows[0]);

    const distinct_order_by = [_]db_mod.types.RelationalRowsQueryOrder{
        .{ .field = "tenant" },
        .{ .field = "amount", .direction = .desc },
    };
    var distinct_query_result = (try source.rowsQueryPlan(alloc, "events", schema, .{
        .query = .{
            .select = select[0..],
            .select_all = false,
            .distinct_on = distinct_on[0..],
            .order_by = distinct_order_by[0..],
            .limit = 5,
        },
    }, .read_index)).?;
    defer distinct_query_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 15), fake.lake_scan_calls);
    try std.testing.expectEqual(@as(?usize, null), fake.last_scan_limit);
    try std.testing.expectEqual(@as(usize, 0), fake.routed_scan_calls);
    try std.testing.expectEqual(@as(u32, 1), distinct_query_result.total);
    try std.testing.expectEqual(@as(usize, 1), distinct_query_result.rows.len);
    try std.testing.expectEqualStrings("{\"amount\":30}", distinct_query_result.rows[0]);

    const lower_tenant_operands = [_]db_mod.types.RelationalRowsExpression{.{
        .kind = .field,
        .field = "tenant",
    }};
    const lower_tenant_expression = db_mod.types.RelationalRowsExpression{
        .kind = .lower,
        .operands = lower_tenant_operands[0..],
    };
    const distinct_on_expressions = [_]db_mod.types.RelationalRowsExpression{lower_tenant_expression};
    const distinct_expression_order_by = [_]db_mod.types.RelationalRowsQueryOrder{
        .{ .expression = lower_tenant_expression },
        .{ .field = "amount", .direction = .desc },
    };
    var distinct_expression_query_result = (try source.rowsQueryPlan(alloc, "events", schema, .{
        .query = .{
            .select = select[0..],
            .select_all = false,
            .distinct_on_expressions = distinct_on_expressions[0..],
            .order_by = distinct_expression_order_by[0..],
            .limit = 5,
        },
    }, .read_index)).?;
    defer distinct_expression_query_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 16), fake.lake_scan_calls);
    try std.testing.expectEqual(@as(?usize, null), fake.last_scan_limit);
    try std.testing.expectEqual(@as(usize, 0), fake.routed_scan_calls);
    try std.testing.expectEqual(@as(u32, 1), distinct_expression_query_result.total);
    try std.testing.expectEqual(@as(usize, 1), distinct_expression_query_result.rows.len);
    try std.testing.expectEqualStrings("{\"amount\":30}", distinct_expression_query_result.rows[0]);

    const fast_aggregations = [_]db_mod.types.RelationalRowsAggregateSpec{
        .{ .name = "count_all", .op = .count },
        .{ .name = "sum_amount", .op = .sum, .field = "amount" },
        .{ .name = "max_amount", .op = .max, .field = "amount" },
    };
    var fast_aggregate_result = (try source.rowsAggregatePlan(alloc, "events", schema, .{
        .aggregate = .{
            .aggregations = fast_aggregations[0..],
        },
    }, .read_index)).?;
    defer fast_aggregate_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 16), fake.lake_scan_calls);
    try std.testing.expectEqual(@as(usize, 1), fake.lake_expression_aggregate_calls);
    try std.testing.expectEqual(@as(usize, 0), fake.routed_scan_calls);
    try std.testing.expectEqual(@as(u32, 1), fast_aggregate_result.total_groups);
    try std.testing.expectEqualStrings("{\"count_all\":2,\"sum_amount\":50,\"max_amount\":30}", fast_aggregate_result.rows[0]);

    const aggregations = [_]db_mod.types.RelationalRowsAggregateSpec{
        .{ .name = "count_all", .op = .count },
        .{ .name = "sum_amount", .op = .sum, .field = "amount" },
    };
    var aggregate_result = (try source.rowsAggregatePlan(alloc, "events", schema, .{
        .aggregate = .{
            .source = .{ .predicates = predicates[0..] },
            .aggregations = aggregations[0..],
        },
    }, .read_index)).?;
    defer aggregate_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 17), fake.lake_scan_calls);
    try std.testing.expectEqual(@as(usize, 1), fake.lake_expression_aggregate_calls);
    try std.testing.expectEqual(@as(usize, 0), fake.routed_scan_calls);
    try std.testing.expectEqual(@as(u32, 1), aggregate_result.total_groups);
    try std.testing.expectEqualStrings("{\"count_all\":2,\"sum_amount\":50}", aggregate_result.rows[0]);

    const set_left = db_mod.types.RelationalRowsQueryPlan{ .query = .{
        .select = select[0..],
        .select_all = false,
    } };
    const set_right = db_mod.types.RelationalRowsQueryPlan{ .query = .{
        .select = select[0..],
        .select_all = false,
        .predicates = residual_predicates[0..],
    } };
    var except_result = (try source.rowsSetOperationPlan(alloc, "events", schema, .{
        .operation = .except,
        .left = set_left,
        .right = set_right,
    }, .read_index)).?;
    defer except_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 19), fake.lake_scan_calls);
    try std.testing.expectEqual(@as(usize, 1), fake.lake_expression_aggregate_calls);
    try std.testing.expectEqual(@as(usize, 0), fake.routed_scan_calls);
    try std.testing.expectEqual(@as(u32, 1), except_result.total);
    try std.testing.expectEqual(@as(usize, 1), except_result.rows.len);
    try std.testing.expectEqualStrings("{\"amount\":20}", except_result.rows[0]);

    const set_order = [_]db_mod.types.RelationalRowsQueryOrder{.{
        .field = "amount",
        .direction = .desc,
    }};
    var union_all_result = (try source.rowsSetOperationPlan(alloc, "events", schema, .{
        .operation = .union_all,
        .left = set_left,
        .right = set_right,
        .order_by = set_order[0..],
        .limit = 2,
    }, .read_index)).?;
    defer union_all_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 21), fake.lake_scan_calls);
    try std.testing.expectEqual(@as(usize, 1), fake.lake_expression_aggregate_calls);
    try std.testing.expectEqual(@as(usize, 0), fake.routed_scan_calls);
    try std.testing.expectEqual(@as(u32, 3), union_all_result.total);
    try std.testing.expectEqual(@as(usize, 2), union_all_result.rows.len);
    try std.testing.expectEqualStrings("{\"amount\":30}", union_all_result.rows[0]);
    try std.testing.expectEqualStrings("{\"amount\":30}", union_all_result.rows[1]);

    try std.testing.expectError(error.RelationalRowsCteMaterializationRejected, source.rowsSetOperationPlan(alloc, "events", schema, .{
        .operation = .union_all,
        .left = set_left,
        .right = set_right,
        .max_rows = 2,
    }, .read_index));
    try std.testing.expectEqual(@as(usize, 23), fake.lake_scan_calls);
    try std.testing.expectEqual(@as(usize, 0), fake.routed_scan_calls);

    const window_partition = [_][]const u8{"tenant"};
    const window_order = [_]db_mod.types.RelationalRowsQueryOrder{.{
        .field = "amount",
        .direction = .desc,
    }};
    const windows = [_]db_mod.types.RelationalRowsWindowSpec{.{
        .output = "rn",
        .function = .row_number,
        .partition_by = window_partition[0..],
        .order_by = window_order[0..],
    }};
    var window_result = (try source.rowsWindowPlan(alloc, "events", schema, .{
        .window = .{
            .source = .{ .select_all = true },
            .windows = windows[0..],
            .select = select[0..],
            .order_by = window_order[0..],
        },
    }, .read_index)).?;
    defer window_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 24), fake.lake_scan_calls);
    try std.testing.expectEqual(@as(usize, 0), fake.routed_scan_calls);
    try std.testing.expectEqual(@as(u32, 2), window_result.total_rows);
    try std.testing.expectEqual(@as(usize, 2), window_result.rows.len);
    try std.testing.expectEqualStrings("{\"amount\":30,\"rn\":1}", window_result.rows[0]);
    try std.testing.expectEqualStrings("{\"amount\":20,\"rn\":2}", window_result.rows[1]);

    const left_amount_twenty = [_]storage_schema.RelationalCheck{.{
        .name = "",
        .field = "amount",
        .value_json = "20",
    }};
    const right_amount_thirty = [_]storage_schema.RelationalCheck{.{
        .name = "",
        .field = "amount",
        .value_json = "30",
    }};
    const join_on = [_]db_mod.types.RelationalRowsJoinOn{.{
        .left_field = "tenant",
        .right_field = "tenant",
    }};
    const join_select = [_]db_mod.types.RelationalRowsJoinProjection{
        .{ .output = "left_amount", .side = .left, .field = "amount" },
        .{ .output = "right_amount", .side = .right, .field = "amount" },
    };
    var join_result = (try source.rowsJoinPlan(alloc, "events", schema, .{
        .join = .{
            .left = .{ .predicates = left_amount_twenty[0..] },
            .right = .{ .predicates = right_amount_thirty[0..] },
            .on = join_on[0..],
            .select = join_select[0..],
        },
    }, .read_index)).?;
    defer join_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 25), fake.lake_scan_calls);
    try std.testing.expectEqual(@as(usize, 0), fake.routed_scan_calls);
    try std.testing.expectEqual(@as(u32, 1), join_result.total_rows);
    try std.testing.expectEqual(@as(usize, 1), join_result.rows.len);
    try std.testing.expectEqualStrings("{\"left_amount\":20,\"right_amount\":30}", join_result.rows[0]);

    const lateral_correlations = [_]db_mod.types.RelationalRowsLateralCorrelation{.{
        .left_field = "tenant",
        .right_field = "tenant",
    }};
    const lateral_select = [_]db_mod.types.RelationalRowsJoinProjection{
        .{ .output = "left_amount", .side = .left, .field = "amount" },
        .{ .output = "latest_amount", .side = .right, .field = "amount" },
    };
    var lateral_result = (try source.rowsLateralPlan(alloc, "events", schema, .{
        .lateral = .{
            .left = .{ .order_by = &.{.{ .field = "amount", .direction = .asc }} },
            .right = .{ .order_by = &.{.{ .field = "amount", .direction = .desc }}, .limit = 1 },
            .correlations = lateral_correlations[0..],
            .select = lateral_select[0..],
            .order_by = &.{.{ .field = "left_amount", .direction = .asc }},
        },
    }, .read_index)).?;
    defer lateral_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 26), fake.lake_scan_calls);
    try std.testing.expectEqual(@as(usize, 0), fake.routed_scan_calls);
    try std.testing.expectEqual(@as(u32, 2), lateral_result.total_rows);
    try std.testing.expectEqual(@as(usize, 2), lateral_result.rows.len);
    try std.testing.expectEqualStrings("{\"left_amount\":20,\"latest_amount\":30}", lateral_result.rows[0]);
    try std.testing.expectEqualStrings("{\"left_amount\":30,\"latest_amount\":30}", lateral_result.rows[1]);

    const lake_range = [_]db_mod.types.RelationalRowsDocKeyRange{.{
        .start = doc_range_start,
        .end = doc_range_end,
    }};
    var ranged_plan_result = (try source.rowsQueryPlan(alloc, "events", schema, .{
        .ranges = lake_range[0..],
        .query = .{
            .select = select[0..],
            .select_all = false,
        },
    }, .read_index)).?;
    defer ranged_plan_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 27), fake.lake_scan_calls);
    try std.testing.expectEqual(@as(usize, 0), fake.routed_scan_calls);
    try std.testing.expectEqual(@as(u32, 1), ranged_plan_result.total);
    try std.testing.expectEqual(@as(usize, 1), ranged_plan_result.rows.len);
    try std.testing.expectEqualStrings("{\"amount\":30}", ranged_plan_result.rows[0]);

    const cte_select = [_][]const u8{ "amount", "tenant" };
    const lake_ctes = [_]db_mod.types.RelationalRowsCte{.{
        .name = "high_amounts",
        .query = .{
            .predicates = residual_predicates[0..],
            .select = cte_select[0..],
            .select_all = false,
        },
    }};
    var cte_query_result = (try source.rowsQueryPlan(alloc, "events", schema, .{
        .ctes = lake_ctes[0..],
        .query = .{
            .source_cte = "high_amounts",
            .select = select[0..],
            .select_all = false,
        },
    }, .read_index)).?;
    defer cte_query_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 28), fake.lake_scan_calls);
    try std.testing.expectEqual(@as(usize, 0), fake.routed_scan_calls);
    try std.testing.expectEqual(@as(u32, 1), cte_query_result.total);
    try std.testing.expectEqual(@as(usize, 1), cte_query_result.rows.len);
    try std.testing.expectEqualStrings("{\"amount\":30}", cte_query_result.rows[0]);

    var cte_aggregate_result = (try source.rowsAggregatePlan(alloc, "events", schema, .{
        .ctes = lake_ctes[0..],
        .aggregate = .{
            .source = .{ .source_cte = "high_amounts" },
            .aggregations = aggregations[0..],
        },
    }, .read_index)).?;
    defer cte_aggregate_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 29), fake.lake_scan_calls);
    try std.testing.expectEqual(@as(usize, 1), fake.lake_expression_aggregate_calls);
    try std.testing.expectEqual(@as(usize, 0), fake.routed_scan_calls);
    try std.testing.expectEqual(@as(u32, 1), cte_aggregate_result.total_groups);
    try std.testing.expectEqualStrings("{\"count_all\":1,\"sum_amount\":30}", cte_aggregate_result.rows[0]);
}

test "lowered sql cross-table read plans execute through routed scans" {
    const alloc = std.testing.allocator;
    const orders_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"customer_id":{"type":"keyword"},"amount":{"type":"numeric"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    const customers_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"name":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;

    var parsed_orders = try schema_api.parseValidatedTableSchema(alloc, orders_schema_json);
    defer parsed_orders.deinit(alloc);
    const orders_schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed_orders);
    defer storage_schema.freeSchema(alloc, orders_schema);

    var parsed_customers = try schema_api.parseValidatedTableSchema(alloc, customers_schema_json);
    defer parsed_customers.deinit(alloc);
    const customers_schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed_customers);
    defer storage_schema.freeSchema(alloc, customers_schema);

    const FakeCatalog = struct {
        tables: [2]metadata_table_manager.TableRecord = .{
            .{ .table_id = 7, .name = "orders", .schema_json = orders_schema_json, .placement_role = "data" },
            .{ .table_id = 8, .name = "customers", .schema_json = customers_schema_json, .placement_role = "data" },
        },

        fn iface(self: *@This()) table_catalog.CatalogSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = self.tables[0..],
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const FakeRoutedSource = struct {
        scan_calls: usize = 0,

        fn source(self: *@This()) TableReadSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .lookup = lookup,
                    .scan = scan,
                    .query = query,
                    .rows_query_plan = rowsQueryPlan,
                    .rows_set_operation_plan_catalog = rowsSetOperationPlanCatalog,
                },
            };
        }

        fn lookup(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
            _: db_mod.types.LookupOptions,
            _: raft_mod.ReadConsistency,
        ) !?LookupResponse {
            return null;
        }

        fn query(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: db_mod.types.SearchRequest,
            _: raft_mod.ReadConsistency,
        ) !?query_api.QueryResponse {
            return null;
        }

        fn scan(
            ptr: *anyopaque,
            scan_alloc: std.mem.Allocator,
            table_name: []const u8,
            from_key: []const u8,
            to_key: []const u8,
            opts: db_mod.types.ScanOptions,
            _: raft_mod.ReadConsistency,
        ) !?ScanResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expect(opts.include_documents);
            try std.testing.expect(opts.include_all_fields);
            try std.testing.expectEqualStrings("", from_key);
            try std.testing.expectEqualStrings("", to_key);
            self.scan_calls += 1;
            const ndjson = if (std.mem.eql(u8, table_name, "orders"))
                "{\"key\":\"o1\",\"id\":\"o1\",\"status\":\"open\",\"customer_id\":\"c1\",\"amount\":10}\n{\"key\":\"o2\",\"id\":\"o2\",\"status\":\"closed\",\"customer_id\":\"c2\",\"amount\":5}\n"
            else if (std.mem.eql(u8, table_name, "customers"))
                "{\"key\":\"c1\",\"id\":\"c1\",\"status\":\"open\",\"name\":\"Ada\"}\n{\"key\":\"c2\",\"id\":\"c2\",\"status\":\"closed\",\"name\":\"Grace\"}\n"
            else
                return error.TableNotFound;
            return .{ .ndjson = try scan_alloc.dupe(u8, ndjson) };
        }

        fn rowsQueryPlan(
            ptr: *anyopaque,
            plan_alloc: std.mem.Allocator,
            table_name: []const u8,
            runtime_schema: storage_schema.TableSchema,
            plan: db_mod.types.RelationalRowsQueryPlan,
            consistency: raft_mod.ReadConsistency,
        ) !?db_mod.types.RelationalRowsQueryResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try rowsQueryPlanFromRoutedScansAlloc(plan_alloc, self.source(), table_name, runtime_schema, plan, consistency);
        }

        fn rowsSetOperationPlanCatalog(
            ptr: *anyopaque,
            plan_alloc: std.mem.Allocator,
            target: catalog_resources.TableTarget,
            runtime_schema: storage_schema.TableSchema,
            plan: db_mod.types.RelationalRowsSetOperationPlan,
            consistency: raft_mod.ReadConsistency,
        ) !?db_mod.types.RelationalRowsQueryResult {
            try std.testing.expectEqualStrings(catalog_resources.default_database_name, target.database_name);
            try std.testing.expectEqualStrings(catalog_resources.default_namespace_name, target.namespace_name);
            try std.testing.expectEqual(@as(?u32, db_mod.types.default_relational_rows_cte_max_rows), plan.max_rows);
            try std.testing.expectEqual(@as(?u64, db_mod.types.default_relational_rows_cte_max_bytes), plan.max_bytes);
            try std.testing.expectEqual(@as(?u64, db_mod.types.default_relational_rows_cte_spill_after_bytes), plan.spill_after_bytes);
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try rowsSetOperationPlanFromRoutedScansAlloc(plan_alloc, self.source(), target.table_name, runtime_schema, plan, consistency);
        }
    };

    var catalog = FakeCatalog{};
    var fake = FakeRoutedSource{};
    var lowered = try sql_adapter_runtime.lowerReadPlanWithCatalogAlloc(
        alloc,
        "SELECT o.id AS order_id, c.name AS customer_name FROM orders AS o LEFT JOIN customers AS c ON o.status = c.status ORDER BY order_id ASC",
        orders_schema,
        &.{},
        catalog.iface(),
    );
    defer lowered.deinit(alloc);

    var result = (try executeLoweredSqlReadPlanAlloc(
        alloc,
        fake.source(),
        catalog.iface(),
        "orders",
        orders_schema,
        lowered,
        .read_index,
    )).?;
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), fake.scan_calls);
    switch (result) {
        .join => |join_result| {
            try std.testing.expectEqual(@as(u32, 2), join_result.total_rows);
            try std.testing.expectEqual(@as(usize, 2), join_result.rows.len);
            try std.testing.expectEqualStrings("{\"order_id\":\"o1\",\"customer_name\":\"Ada\"}", join_result.rows[0]);
            try std.testing.expectEqualStrings("{\"order_id\":\"o2\",\"customer_name\":\"Grace\"}", join_result.rows[1]);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "lowered sql set operation plans preserve overlapping union all rows" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"enabled":{"type":"boolean"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;

    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer storage_schema.freeSchema(alloc, schema);

    const FakeCatalog = struct {
        fn iface(self: *@This()) table_catalog.CatalogSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const FakeSource = struct {
        scan_calls: usize = 0,

        fn source(self: *@This()) TableReadSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .lookup = lookup,
                    .scan = scan,
                    .query = query,
                    .rows_query_plan = rowsQueryPlan,
                    .rows_set_operation_plan_catalog = rowsSetOperationPlanCatalog,
                },
            };
        }

        fn lookup(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
            _: db_mod.types.LookupOptions,
            _: raft_mod.ReadConsistency,
        ) !?LookupResponse {
            return null;
        }

        fn query(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: db_mod.types.SearchRequest,
            _: raft_mod.ReadConsistency,
        ) !?query_api.QueryResponse {
            return null;
        }

        fn scan(
            ptr: *anyopaque,
            scan_alloc: std.mem.Allocator,
            table_name: []const u8,
            from_key: []const u8,
            to_key: []const u8,
            opts: db_mod.types.ScanOptions,
            _: raft_mod.ReadConsistency,
        ) !?ScanResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expect(opts.include_documents);
            try std.testing.expect(opts.include_all_fields);
            try std.testing.expectEqualStrings("usage_records", table_name);
            try std.testing.expectEqualStrings("", from_key);
            try std.testing.expectEqualStrings("", to_key);
            self.scan_calls += 1;
            const ndjson =
                "{\"key\":\"u1\",\"id\":\"u1\",\"status\":\"open\",\"enabled\":true}\n" ++
                "{\"key\":\"u2\",\"id\":\"u2\",\"status\":\"open\",\"enabled\":false}\n" ++
                "{\"key\":\"u3\",\"id\":\"u3\",\"status\":\"closed\",\"enabled\":true}\n";
            return .{ .ndjson = try scan_alloc.dupe(u8, ndjson) };
        }

        fn rowsQueryPlan(
            ptr: *anyopaque,
            plan_alloc: std.mem.Allocator,
            table_name: []const u8,
            runtime_schema: storage_schema.TableSchema,
            plan: db_mod.types.RelationalRowsQueryPlan,
            consistency: raft_mod.ReadConsistency,
        ) !?db_mod.types.RelationalRowsQueryResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try rowsQueryPlanFromRoutedScansAlloc(plan_alloc, self.source(), table_name, runtime_schema, plan, consistency);
        }

        fn rowsSetOperationPlanCatalog(
            ptr: *anyopaque,
            plan_alloc: std.mem.Allocator,
            target: catalog_resources.TableTarget,
            runtime_schema: storage_schema.TableSchema,
            plan: db_mod.types.RelationalRowsSetOperationPlan,
            consistency: raft_mod.ReadConsistency,
        ) !?db_mod.types.RelationalRowsQueryResult {
            try std.testing.expectEqualStrings(catalog_resources.default_database_name, target.database_name);
            try std.testing.expectEqualStrings(catalog_resources.default_namespace_name, target.namespace_name);
            try std.testing.expectEqual(@as(?u32, db_mod.types.default_relational_rows_cte_max_rows), plan.max_rows);
            try std.testing.expectEqual(@as(?u64, db_mod.types.default_relational_rows_cte_max_bytes), plan.max_bytes);
            try std.testing.expectEqual(@as(?u64, db_mod.types.default_relational_rows_cte_spill_after_bytes), plan.spill_after_bytes);
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try rowsSetOperationPlanFromRoutedScansAlloc(plan_alloc, self.source(), target.table_name, runtime_schema, plan, consistency);
        }
    };

    var lowered = try sql_adapter_runtime.lowerReadPlanAlloc(
        alloc,
        "SELECT id FROM usage_records WHERE status = 'open' UNION ALL SELECT id FROM usage_records WHERE enabled IS TRUE",
        schema,
        &.{},
    );
    defer lowered.deinit(alloc);
    switch (lowered) {
        .set_operation => |set_operation| {
            try std.testing.expectEqual(@as(?u32, db_mod.types.default_relational_rows_cte_max_rows), set_operation.max_rows);
            try std.testing.expectEqual(@as(?u64, db_mod.types.default_relational_rows_cte_max_bytes), set_operation.max_bytes);
            try std.testing.expectEqual(@as(?u64, db_mod.types.default_relational_rows_cte_spill_after_bytes), set_operation.spill_after_bytes);
        },
        else => return error.TestUnexpectedResult,
    }

    var catalog = FakeCatalog{};
    var fake = FakeSource{};
    var result = (try executeLoweredSqlReadPlanAlloc(
        alloc,
        fake.source(),
        catalog.iface(),
        "usage_records",
        schema,
        lowered,
        .read_index,
    )).?;
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), fake.scan_calls);
    switch (result) {
        .set_operation => |query_result| {
            try std.testing.expectEqual(@as(u32, 4), query_result.total);
            try std.testing.expectEqual(@as(usize, 4), query_result.rows.len);
            try std.testing.expectEqualStrings("{\"id\":\"u1\"}", query_result.rows[0]);
            try std.testing.expectEqualStrings("{\"id\":\"u2\"}", query_result.rows[1]);
            try std.testing.expectEqualStrings("{\"id\":\"u1\"}", query_result.rows[2]);
            try std.testing.expectEqualStrings("{\"id\":\"u3\"}", query_result.rows[3]);
        },
        else => return error.TestUnexpectedResult,
    }

    var lowered_distinct = try sql_adapter_runtime.lowerReadPlanAlloc(
        alloc,
        "SELECT id FROM usage_records WHERE status = 'open' UNION SELECT id FROM usage_records WHERE enabled IS TRUE",
        schema,
        &.{},
    );
    defer lowered_distinct.deinit(alloc);
    switch (lowered_distinct) {
        .set_operation => |set_operation| {
            try std.testing.expectEqual(@as(sql_adapter_runtime.SelectSetOperation, .union_distinct), set_operation.operation);
            try std.testing.expectEqualStrings("usage_records", set_operation.left.table_name);
            try std.testing.expectEqualStrings("usage_records", set_operation.right.table_name);
        },
        else => return error.TestUnexpectedResult,
    }

    var fake_distinct = FakeSource{};
    var distinct_result = (try executeLoweredSqlReadPlanAlloc(
        alloc,
        fake_distinct.source(),
        catalog.iface(),
        "usage_records",
        schema,
        lowered_distinct,
        .read_index,
    )).?;
    defer distinct_result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), fake_distinct.scan_calls);
    switch (distinct_result) {
        .set_operation => |query_result| {
            try std.testing.expectEqual(@as(u32, 3), query_result.total);
            try std.testing.expectEqual(@as(usize, 3), query_result.rows.len);
            try std.testing.expectEqualStrings("{\"id\":\"u1\"}", query_result.rows[0]);
            try std.testing.expectEqualStrings("{\"id\":\"u2\"}", query_result.rows[1]);
            try std.testing.expectEqualStrings("{\"id\":\"u3\"}", query_result.rows[2]);
        },
        else => return error.TestUnexpectedResult,
    }

    var lowered_intersect = try sql_adapter_runtime.lowerReadPlanAlloc(
        alloc,
        "SELECT id FROM usage_records WHERE status = 'open' INTERSECT SELECT id FROM usage_records WHERE enabled IS TRUE",
        schema,
        &.{},
    );
    defer lowered_intersect.deinit(alloc);
    switch (lowered_intersect) {
        .set_operation => |set_operation| {
            try std.testing.expectEqual(@as(sql_adapter_runtime.SelectSetOperation, .intersect), set_operation.operation);
            try std.testing.expectEqualStrings("usage_records", set_operation.left.table_name);
            try std.testing.expectEqualStrings("usage_records", set_operation.right.table_name);
        },
        else => return error.TestUnexpectedResult,
    }

    var fake_intersect = FakeSource{};
    var intersect_result = (try executeLoweredSqlReadPlanAlloc(
        alloc,
        fake_intersect.source(),
        catalog.iface(),
        "usage_records",
        schema,
        lowered_intersect,
        .read_index,
    )).?;
    defer intersect_result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), fake_intersect.scan_calls);
    switch (intersect_result) {
        .set_operation => |query_result| {
            try std.testing.expectEqual(@as(u32, 1), query_result.total);
            try std.testing.expectEqual(@as(usize, 1), query_result.rows.len);
            try std.testing.expectEqualStrings("{\"id\":\"u1\"}", query_result.rows[0]);
        },
        else => return error.TestUnexpectedResult,
    }

    var lowered_tail = try sql_adapter_runtime.lowerReadPlanAlloc(
        alloc,
        "SELECT id FROM usage_records WHERE status = 'open' UNION ALL SELECT id FROM usage_records WHERE enabled IS TRUE ORDER BY id DESC LIMIT 2 OFFSET 1",
        schema,
        &.{},
    );
    defer lowered_tail.deinit(alloc);
    switch (lowered_tail) {
        .set_operation => {},
        else => return error.TestUnexpectedResult,
    }

    var fake_tail = FakeSource{};
    var tail_result = (try executeLoweredSqlReadPlanAlloc(
        alloc,
        fake_tail.source(),
        catalog.iface(),
        "usage_records",
        schema,
        lowered_tail,
        .read_index,
    )).?;
    defer tail_result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), fake_tail.scan_calls);
    switch (tail_result) {
        .set_operation => |query_result| {
            try std.testing.expectEqual(@as(u32, 4), query_result.total);
            try std.testing.expectEqual(@as(usize, 2), query_result.rows.len);
            try std.testing.expectEqualStrings("{\"id\":\"u2\"}", query_result.rows[0]);
            try std.testing.expectEqualStrings("{\"id\":\"u1\"}", query_result.rows[1]);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "lowered sql set operation plans route cross table branches through catalog schemas" {
    const alloc = std.testing.allocator;
    const usage_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"enabled":{"type":"boolean"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    const archived_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"enabled":{"type":"boolean"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;

    var parsed_usage = try schema_api.parseValidatedTableSchema(alloc, usage_schema_json);
    defer parsed_usage.deinit(alloc);
    const usage_schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed_usage);
    defer storage_schema.freeSchema(alloc, usage_schema);

    var parsed_archived = try schema_api.parseValidatedTableSchema(alloc, archived_schema_json);
    defer parsed_archived.deinit(alloc);
    const archived_schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed_archived);
    defer storage_schema.freeSchema(alloc, archived_schema);

    const FakeCatalog = struct {
        tables: [2]metadata_table_manager.TableRecord = .{
            .{ .table_id = 31, .name = "usage_records", .schema_json = usage_schema_json, .placement_role = "data" },
            .{ .table_id = 32, .name = "archived_records", .schema_json = archived_schema_json, .placement_role = "data" },
        },

        fn iface(self: *@This()) table_catalog.CatalogSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = self.tables[0..],
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const FakeSource = struct {
        usage_scans: usize = 0,
        archived_scans: usize = 0,
        usage_catalog_queries: usize = 0,
        archived_catalog_queries: usize = 0,
        expected_database_name: []const u8 = tables_api.default_database_name,
        expected_namespace_name: []const u8 = tables_api.default_namespace_name,

        fn source(self: *@This()) TableReadSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .lookup = lookup,
                    .scan = scan,
                    .query = query,
                    .rows_query_plan = rowsQueryPlan,
                    .rows_query_plan_catalog = rowsQueryPlanCatalog,
                },
            };
        }

        fn lookup(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
            _: db_mod.types.LookupOptions,
            _: raft_mod.ReadConsistency,
        ) !?LookupResponse {
            return null;
        }

        fn query(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: db_mod.types.SearchRequest,
            _: raft_mod.ReadConsistency,
        ) !?query_api.QueryResponse {
            return null;
        }

        fn scan(
            ptr: *anyopaque,
            scan_alloc: std.mem.Allocator,
            table_name: []const u8,
            from_key: []const u8,
            to_key: []const u8,
            opts: db_mod.types.ScanOptions,
            _: raft_mod.ReadConsistency,
        ) !?ScanResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expect(opts.include_documents);
            try std.testing.expect(opts.include_all_fields);
            try std.testing.expectEqualStrings("", from_key);
            try std.testing.expectEqualStrings("", to_key);
            if (std.mem.eql(u8, table_name, "usage_records")) {
                self.usage_scans += 1;
                const ndjson =
                    "{\"key\":\"u1\",\"id\":\"u1\",\"status\":\"open\",\"enabled\":true}\n" ++
                    "{\"key\":\"u2\",\"id\":\"u2\",\"status\":\"open\",\"enabled\":false}\n" ++
                    "{\"key\":\"u3\",\"id\":\"u3\",\"status\":\"closed\",\"enabled\":true}\n";
                return .{ .ndjson = try scan_alloc.dupe(u8, ndjson) };
            }
            if (std.mem.eql(u8, table_name, "archived_records")) {
                self.archived_scans += 1;
                const ndjson =
                    "{\"key\":\"a1\",\"id\":\"a1\",\"status\":\"archived\",\"enabled\":true}\n" ++
                    "{\"key\":\"a2\",\"id\":\"a2\",\"status\":\"archived\",\"enabled\":false}\n" ++
                    "{\"key\":\"a3\",\"id\":\"a3\",\"status\":\"deleted\",\"enabled\":true}\n" ++
                    "{\"key\":\"u2\",\"id\":\"u2\",\"status\":\"deleted\",\"enabled\":false}\n";
                return .{ .ndjson = try scan_alloc.dupe(u8, ndjson) };
            }
            return error.TableNotFound;
        }

        fn rowsQueryPlan(
            _: *anyopaque,
            _: std.mem.Allocator,
            table_name: []const u8,
            _: storage_schema.TableSchema,
            _: db_mod.types.RelationalRowsQueryPlan,
            _: raft_mod.ReadConsistency,
        ) !?db_mod.types.RelationalRowsQueryResult {
            _ = table_name;
            return error.TestUnexpectedResult;
        }

        fn rowsQueryPlanCatalog(
            ptr: *anyopaque,
            plan_alloc: std.mem.Allocator,
            target: catalog_resources.TableTarget,
            runtime_schema: storage_schema.TableSchema,
            plan: db_mod.types.RelationalRowsQueryPlan,
            consistency: raft_mod.ReadConsistency,
        ) !?db_mod.types.RelationalRowsQueryResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings(self.expected_database_name, target.database_name);
            try std.testing.expectEqualStrings(self.expected_namespace_name, target.namespace_name);
            if (std.mem.eql(u8, target.table_name, "usage_records")) {
                self.usage_catalog_queries += 1;
            } else if (std.mem.eql(u8, target.table_name, "archived_records")) {
                self.archived_catalog_queries += 1;
            } else {
                return error.TableNotFound;
            }
            return try rowsQueryPlanFromRoutedScansAlloc(plan_alloc, self.source(), target.table_name, runtime_schema, plan, consistency);
        }
    };

    var catalog = FakeCatalog{};
    var lowered = try sql_adapter_runtime.lowerReadPlanWithCatalogAlloc(
        alloc,
        "SELECT id FROM usage_records WHERE status = 'open' UNION ALL SELECT id FROM archived_records WHERE enabled IS TRUE",
        usage_schema,
        &.{},
        catalog.iface(),
    );
    defer lowered.deinit(alloc);
    switch (lowered) {
        .set_operation => {},
        else => return error.TestUnexpectedResult,
    }

    var fake = FakeSource{};
    var result = (try executeLoweredSqlReadPlanAlloc(
        alloc,
        fake.source(),
        catalog.iface(),
        "usage_records",
        usage_schema,
        lowered,
        .read_index,
    )).?;
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), fake.usage_scans);
    try std.testing.expectEqual(@as(usize, 1), fake.archived_scans);
    try std.testing.expectEqual(@as(usize, 1), fake.usage_catalog_queries);
    try std.testing.expectEqual(@as(usize, 1), fake.archived_catalog_queries);
    switch (result) {
        .set_operation => |query_result| {
            try std.testing.expectEqual(@as(u32, 4), query_result.total);
            try std.testing.expectEqual(@as(usize, 4), query_result.rows.len);
            try std.testing.expectEqualStrings("{\"id\":\"u1\"}", query_result.rows[0]);
            try std.testing.expectEqualStrings("{\"id\":\"u2\"}", query_result.rows[1]);
            try std.testing.expectEqualStrings("{\"id\":\"a1\"}", query_result.rows[2]);
            try std.testing.expectEqualStrings("{\"id\":\"a3\"}", query_result.rows[3]);
        },
        else => return error.TestUnexpectedResult,
    }

    var tenant_fake = FakeSource{
        .expected_database_name = "tenant_ops",
        .expected_namespace_name = "analytics",
    };
    const tenant_session: catalog_resources.SqlCatalogSession = .{
        .current_database_name = "tenant_ops",
        .search_path = &.{ "analytics", "public" },
    };
    var tenant_result = (try executeLoweredSqlReadPlanWithSessionAlloc(
        alloc,
        tenant_fake.source(),
        catalog.iface(),
        tenant_session,
        "usage_records",
        usage_schema,
        lowered,
        .read_index,
    )).?;
    defer tenant_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), tenant_fake.usage_catalog_queries);
    try std.testing.expectEqual(@as(usize, 1), tenant_fake.archived_catalog_queries);
    switch (tenant_result) {
        .set_operation => |query_result| {
            try std.testing.expectEqual(@as(u32, 4), query_result.total);
            try std.testing.expectEqual(@as(usize, 4), query_result.rows.len);
        },
        else => return error.TestUnexpectedResult,
    }

    var lowered_except = try sql_adapter_runtime.lowerReadPlanWithCatalogAlloc(
        alloc,
        "SELECT id FROM usage_records WHERE status = 'open' EXCEPT SELECT id FROM archived_records WHERE status = 'deleted'",
        usage_schema,
        &.{},
        catalog.iface(),
    );
    defer lowered_except.deinit(alloc);
    switch (lowered_except) {
        .set_operation => |set_operation| try std.testing.expectEqualStrings("except", @tagName(set_operation.operation)),
        else => return error.TestUnexpectedResult,
    }

    var fake_except = FakeSource{};
    var except_result = (try executeLoweredSqlReadPlanAlloc(
        alloc,
        fake_except.source(),
        catalog.iface(),
        "usage_records",
        usage_schema,
        lowered_except,
        .read_index,
    )).?;
    defer except_result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), fake_except.usage_scans);
    try std.testing.expectEqual(@as(usize, 1), fake_except.archived_scans);
    try std.testing.expectEqual(@as(usize, 1), fake_except.usage_catalog_queries);
    try std.testing.expectEqual(@as(usize, 1), fake_except.archived_catalog_queries);
    switch (except_result) {
        .set_operation => |query_result| {
            try std.testing.expectEqual(@as(u32, 1), query_result.total);
            try std.testing.expectEqual(@as(usize, 1), query_result.rows.len);
            try std.testing.expectEqualStrings("{\"id\":\"u1\"}", query_result.rows[0]);
        },
        else => return error.TestUnexpectedResult,
    }

    var lowered_intersect = try sql_adapter_runtime.lowerReadPlanWithCatalogAlloc(
        alloc,
        "SELECT id FROM usage_records WHERE status = 'open' INTERSECT SELECT id FROM archived_records WHERE status = 'deleted'",
        usage_schema,
        &.{},
        catalog.iface(),
    );
    defer lowered_intersect.deinit(alloc);
    switch (lowered_intersect) {
        .set_operation => |set_operation| try std.testing.expectEqualStrings("intersect", @tagName(set_operation.operation)),
        else => return error.TestUnexpectedResult,
    }

    var fake_intersect = FakeSource{};
    var intersect_result = (try executeLoweredSqlReadPlanAlloc(
        alloc,
        fake_intersect.source(),
        catalog.iface(),
        "usage_records",
        usage_schema,
        lowered_intersect,
        .read_index,
    )).?;
    defer intersect_result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), fake_intersect.usage_scans);
    try std.testing.expectEqual(@as(usize, 1), fake_intersect.archived_scans);
    try std.testing.expectEqual(@as(usize, 1), fake_intersect.usage_catalog_queries);
    try std.testing.expectEqual(@as(usize, 1), fake_intersect.archived_catalog_queries);
    switch (intersect_result) {
        .set_operation => |query_result| {
            try std.testing.expectEqual(@as(u32, 1), query_result.total);
            try std.testing.expectEqual(@as(usize, 1), query_result.rows.len);
            try std.testing.expectEqualStrings("{\"id\":\"u2\"}", query_result.rows[0]);
        },
        else => return error.TestUnexpectedResult,
    }
}

fn parseJsonTestBody(comptime T: type, alloc: std.mem.Allocator, body: []const u8) !std.json.Parsed(T) {
    return try std.json.parseFromSlice(T, alloc, body, .{});
}

fn parseNdjsonTestRowsAlloc(comptime T: type, alloc: std.mem.Allocator, ndjson: []const u8) ![]T {
    var count: usize = 0;
    var lines = std.mem.splitScalar(u8, ndjson, '\n');
    while (lines.next()) |line| {
        if (line.len == 0) continue;
        count += 1;
    }

    const out = try alloc.alloc(T, count);
    errdefer alloc.free(out);

    var initialized: usize = 0;
    lines = std.mem.splitScalar(u8, ndjson, '\n');
    while (lines.next()) |line| {
        if (line.len == 0) continue;
        var parsed = try std.json.parseFromSlice(T, alloc, line, .{});
        defer parsed.deinit();
        out[initialized] = parsed.value;
        initialized += 1;
    }
    return out;
}

test "bound table read source uses feature db reads and returns version" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-reads";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }
    try db.batch(.{
        .writes = &.{
            .{
                .key = "doc:a",
                .value = "{\"title\":\"alpha\"}",
            },
        },
        .timestamp_ns = 1234,
    });

    var source = BoundTableReadSource.init("docs", 77, &db, raft_mod.read_gate.noopReadableLeaseRequester());
    var lookup = (try source.source().lookup(alloc, "docs", "doc:a", .{}, .read_index)).?;
    defer lookup.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 1234), lookup.version);
}

test "bound table read source executes SQL system-time as-of by commit sequence" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-system-time";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    const base_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"name":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    const versioned_schema_json =
        \\{"version":2,"storage_mode":"relational","default_type":"row","enforce_types":true,"system_versioned":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"name":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;

    try db.applyTableSchemaJson(alloc, base_schema_json, .{});
    try db.batch(.{ .writes = &.{.{ .key = "row:1", .value = "{\"id\":\"row:1\",\"name\":\"before\"}" }} });

    var versioned_parsed = try schema_api.parseValidatedTableSchema(alloc, versioned_schema_json);
    defer versioned_parsed.deinit(alloc);
    const versioned_schema = try schema_api.deriveRuntimeTableSchema(alloc, versioned_parsed);
    defer storage_schema.freeSchema(alloc, versioned_schema);
    try db.applyTableSchemaJson(alloc, versioned_schema_json, .{});
    try db.batch(.{ .writes = &.{.{ .key = "row:1", .value = "{\"id\":\"row:1\",\"name\":\"first\"}" }} });
    try db.batch(.{ .writes = &.{.{ .key = "row:1", .value = "{\"id\":\"row:1\",\"name\":\"second\"}" }} });

    const history = try db.scanSystemVersionedHistoryForDocKeyAlloc(alloc, "row:1");
    defer db_mod.docstore.DocStore.freeResults(alloc, history);
    try std.testing.expectEqual(@as(usize, 2), history.len);
    var parsed_history = try std.json.parseFromSlice(std.json.Value, alloc, history[0].value, .{});
    defer parsed_history.deinit();
    const commit_value = parsed_history.value.object.get("commit_sequence") orelse return error.TestUnexpectedResult;
    const commit_sequence: u64 = switch (commit_value) {
        .integer => |value| @intCast(value),
        .number_string => |value| try std.fmt.parseUnsigned(u64, value, 10),
        else => return error.TestUnexpectedResult,
    };

    const sql = try std.fmt.allocPrint(alloc, "SELECT id, name FROM docs FOR SYSTEM_TIME AS OF {d} WHERE id = 'row:1'", .{commit_sequence});
    defer alloc.free(sql);
    var lowered = try sql_adapter_runtime.lowerReadPlanAlloc(alloc, sql, versioned_schema, &.{});
    defer lowered.deinit(alloc);

    const FakeCatalog = struct {
        fn iface(self: *@This()) table_catalog.CatalogSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.TestUnexpectedResult;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var catalog = FakeCatalog{};
    var source = BoundTableReadSource.init("docs", 77, &db, raft_mod.read_gate.noopReadableLeaseRequester());
    var result = (try executeLoweredSqlReadPlanAlloc(
        alloc,
        source.source(),
        catalog.iface(),
        "docs",
        versioned_schema,
        lowered,
        .read_index,
    )).?;
    defer result.deinit(alloc);
    switch (result) {
        .query => |query_result| {
            try std.testing.expectEqual(@as(u32, 1), query_result.total);
            try std.testing.expectEqual(@as(usize, 1), query_result.rows.len);
            try std.testing.expect(std.mem.indexOf(u8, query_result.rows[0], "\"first\"") != null);
            try std.testing.expect(std.mem.indexOf(u8, query_result.rows[0], "\"second\"") == null);
        },
        else => return error.TestUnexpectedResult,
    }

    try std.testing.expectError(
        error.UnsupportedSqlShape,
        sql_adapter_runtime.lowerReadPlanAlloc(alloc, "SELECT id FROM docs FOR SYSTEM_TIME AS OF '2026-01-01T00:00:00Z'", versioned_schema, &.{}),
    );
}

test "lowered document sql read plans execute native lookup and bounded scan" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-document-sql-read";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer db.close();
    try db.addIndex(.{ .name = "full_text_index_v0", .kind = .full_text, .config_json = "{}" });
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"key\":\"document-key-field\",\"category\":\"release\",\"metadata\":{\"status\":\"active\",\"billing\":{\"plan\":\"pro\"}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\"}" },
            .{ .key = "doc:c", .value = "{\"title\":\"alpha\",\"key\":\"second-key\",\"category\":\"preview\",\"metadata\":{\"status\":\"active\",\"billing\":{\"plan\":\"free\"}}}" },
        },
        .sync_level = .full_index,
    });

    var parsed_schema = try schema_api.parseValidatedTableSchema(alloc,
        \\{"version":1,"default_type":"doc","document_schemas":{"doc":{"schema":{"type":"object","properties":{"title":{"type":"text"},"key":{"type":"keyword"},"metadata":{"type":"json"}},"additionalProperties":true}}}}
    );
    defer parsed_schema.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer storage_schema.freeSchema(alloc, schema);

    const FakeCatalog = struct {
        fn iface(self: *@This()) table_catalog.CatalogSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                }})[0..]),
                .ranges = &.{},
                .stores = &.{},
                .placement_intents = &.{},
                .split_transitions = &.{},
                .merge_transitions = &.{},
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var catalog = FakeCatalog{};
    var source = BoundTableReadSource.init("docs", 77, &db, raft_mod.read_gate.noopReadableLeaseRequester());

    var lookup_plan = try sql_adapter_runtime.lowerReadPlanAlloc(
        alloc,
        "SELECT _id, title, key, metadata->>'status' AS status, metadata#>>'{billing,plan}' AS plan FROM docs WHERE _id = 'doc:a'",
        schema,
        &.{},
    );
    defer lookup_plan.deinit(alloc);
    var lookup_result = (try executeLoweredSqlReadPlanAlloc(
        alloc,
        source.source(),
        catalog.iface(),
        "docs",
        schema,
        lookup_plan,
        .read_index,
    )).?;
    defer lookup_result.deinit(alloc);
    switch (lookup_result) {
        .document_query => |query| {
            try std.testing.expectEqual(@as(u32, 1), query.total);
            try std.testing.expectEqual(@as(usize, 1), query.rows.len);
            try std.testing.expectEqualStrings("{\"_id\":\"doc:a\",\"title\":\"alpha\",\"key\":\"document-key-field\",\"status\":\"active\",\"plan\":\"pro\"}", query.rows[0]);
        },
        else => return error.TestUnexpectedResult,
    }

    var doc_projection_plan = try sql_adapter_runtime.lowerReadPlanAlloc(
        alloc,
        "SELECT _id, _doc FROM docs WHERE _id = 'doc:a'",
        schema,
        &.{},
    );
    defer doc_projection_plan.deinit(alloc);
    var doc_projection_result = (try executeLoweredSqlReadPlanAlloc(
        alloc,
        source.source(),
        catalog.iface(),
        "docs",
        schema,
        doc_projection_plan,
        .read_index,
    )).?;
    defer doc_projection_result.deinit(alloc);
    switch (doc_projection_result) {
        .document_query => |query| {
            try std.testing.expectEqual(@as(u32, 1), query.total);
            try std.testing.expectEqual(@as(usize, 1), query.rows.len);
            try std.testing.expectEqualStrings("{\"_id\":\"doc:a\",\"_doc\":{\"title\":\"alpha\",\"key\":\"document-key-field\",\"category\":\"release\",\"metadata\":{\"status\":\"active\",\"billing\":{\"plan\":\"pro\"}}}}", query.rows[0]);
        },
        else => return error.TestUnexpectedResult,
    }

    var scan_plan = try sql_adapter_runtime.lowerReadPlanAlloc(
        alloc,
        "SELECT _id, title FROM docs LIMIT 2",
        schema,
        &.{},
    );
    defer scan_plan.deinit(alloc);
    var scan_result = (try executeLoweredSqlReadPlanAlloc(
        alloc,
        source.source(),
        catalog.iface(),
        "docs",
        schema,
        scan_plan,
        .read_index,
    )).?;
    defer scan_result.deinit(alloc);
    switch (scan_result) {
        .document_query => |query| {
            try std.testing.expectEqual(@as(u32, 2), query.total);
            try std.testing.expectEqual(@as(usize, 2), query.rows.len);
            try std.testing.expectEqualStrings("{\"_id\":\"doc:a\",\"title\":\"alpha\"}", query.rows[0]);
            try std.testing.expectEqualStrings("{\"_id\":\"doc:b\",\"title\":\"beta\"}", query.rows[1]);
        },
        else => return error.TestUnexpectedResult,
    }

    var star_scan_plan = try sql_adapter_runtime.lowerReadPlanAlloc(
        alloc,
        "SELECT * FROM docs LIMIT 1",
        schema,
        &.{},
    );
    defer star_scan_plan.deinit(alloc);
    var star_scan_result = (try executeLoweredSqlReadPlanAlloc(
        alloc,
        source.source(),
        catalog.iface(),
        "docs",
        schema,
        star_scan_plan,
        .read_index,
    )).?;
    defer star_scan_result.deinit(alloc);
    switch (star_scan_result) {
        .document_query => |query| {
            try std.testing.expectEqual(@as(u32, 1), query.total);
            try std.testing.expectEqual(@as(usize, 1), query.rows.len);
            try std.testing.expectEqualStrings("{\"_id\":\"doc:a\",\"_doc\":{\"title\":\"alpha\",\"key\":\"document-key-field\",\"category\":\"release\",\"metadata\":{\"status\":\"active\",\"billing\":{\"plan\":\"pro\"}}},\"title\":\"alpha\",\"key\":\"document-key-field\",\"metadata\":{\"status\":\"active\",\"billing\":{\"plan\":\"pro\"}}}", query.rows[0]);
        },
        else => return error.TestUnexpectedResult,
    }

    var virtual_schema = try sql_adapter.documentSqlSchemaForRuntimeSchemaAndIndexesJsonAlloc(
        alloc,
        schema,
        "{\"typed_paths\":{\"keyword\":[\"category\"]}}",
    );
    defer sql_adapter.deinitDocumentSqlSchema(alloc, &virtual_schema);

    var virtual_projection_sql = try sql_adapter.ParsedSql.initAlloc(
        alloc,
        "SELECT _id, category FROM docs WHERE _id = 'doc:a'",
    );
    defer virtual_projection_sql.deinit(alloc);
    var virtual_projection_document_plan = try sql_adapter.lowerDocumentReadPlanWithCapabilitiesAndVirtualSchemaParsedSqlAlloc(
        alloc,
        &virtual_projection_sql,
        schema,
        virtual_schema,
        .{},
    );
    defer virtual_projection_document_plan.deinit(alloc);
    const virtual_projection_plan = sql_adapter_runtime.LoweredReadPlan{ .document_query = virtual_projection_document_plan };
    var virtual_projection_result = (try executeLoweredSqlReadPlanAlloc(
        alloc,
        source.source(),
        catalog.iface(),
        "docs",
        schema,
        virtual_projection_plan,
        .read_index,
    )).?;
    defer virtual_projection_result.deinit(alloc);
    switch (virtual_projection_result) {
        .document_query => |query| {
            try std.testing.expectEqual(@as(u32, 1), query.total);
            try std.testing.expectEqual(@as(usize, 1), query.rows.len);
            try std.testing.expectEqualStrings("{\"_id\":\"doc:a\",\"category\":\"release\"}", query.rows[0]);
        },
        else => return error.TestUnexpectedResult,
    }

    var virtual_star_sql = try sql_adapter.ParsedSql.initAlloc(
        alloc,
        "SELECT * FROM docs WHERE _id = 'doc:a'",
    );
    defer virtual_star_sql.deinit(alloc);
    var virtual_star_document_plan = try sql_adapter.lowerDocumentReadPlanWithCapabilitiesAndVirtualSchemaParsedSqlAlloc(
        alloc,
        &virtual_star_sql,
        schema,
        virtual_schema,
        .{},
    );
    defer virtual_star_document_plan.deinit(alloc);
    const virtual_star_plan = sql_adapter_runtime.LoweredReadPlan{ .document_query = virtual_star_document_plan };
    var virtual_star_result = (try executeLoweredSqlReadPlanAlloc(
        alloc,
        source.source(),
        catalog.iface(),
        "docs",
        schema,
        virtual_star_plan,
        .read_index,
    )).?;
    defer virtual_star_result.deinit(alloc);
    switch (virtual_star_result) {
        .document_query => |query| {
            try std.testing.expectEqual(@as(u32, 1), query.total);
            try std.testing.expectEqual(@as(usize, 1), query.rows.len);
            try std.testing.expectEqualStrings("{\"_id\":\"doc:a\",\"_doc\":{\"title\":\"alpha\",\"key\":\"document-key-field\",\"category\":\"release\",\"metadata\":{\"status\":\"active\",\"billing\":{\"plan\":\"pro\"}}},\"title\":\"alpha\",\"key\":\"document-key-field\",\"metadata\":{\"status\":\"active\",\"billing\":{\"plan\":\"pro\"}},\"category\":\"release\"}", query.rows[0]);
        },
        else => return error.TestUnexpectedResult,
    }

    var ordered_scan_sql = try sql_adapter.ParsedSql.initAlloc(
        alloc,
        "SELECT _id, title FROM docs ORDER BY title DESC LIMIT 2",
    );
    defer ordered_scan_sql.deinit(alloc);
    var ordered_document_plan = try sql_adapter.lowerDocumentReadPlanWithBoundedScanPolicyParsedSqlAlloc(
        alloc,
        &ordered_scan_sql,
        schema,
        .{ .max_rows = 10 },
    );
    defer ordered_document_plan.deinit(alloc);
    const ordered_scan_plan = sql_adapter_runtime.LoweredReadPlan{ .document_query = ordered_document_plan };
    var ordered_scan_result = (try executeLoweredSqlReadPlanAlloc(
        alloc,
        source.source(),
        catalog.iface(),
        "docs",
        schema,
        ordered_scan_plan,
        .read_index,
    )).?;
    defer ordered_scan_result.deinit(alloc);
    switch (ordered_scan_result) {
        .document_query => |query| {
            try std.testing.expectEqual(@as(u32, 2), query.total);
            try std.testing.expectEqual(@as(usize, 2), query.rows.len);
            try std.testing.expectEqualStrings("{\"_id\":\"doc:b\",\"title\":\"beta\"}", query.rows[0]);
            try std.testing.expect(std.mem.eql(u8, query.rows[1], "{\"_id\":\"doc:a\",\"title\":\"alpha\"}") or
                std.mem.eql(u8, query.rows[1], "{\"_id\":\"doc:c\",\"title\":\"alpha\"}"));
        },
        else => return error.TestUnexpectedResult,
    }

    var capped_ordered_document_plan = try sql_adapter.lowerDocumentReadPlanWithBoundedScanPolicyParsedSqlAlloc(
        alloc,
        &ordered_scan_sql,
        schema,
        .{ .max_rows = 2 },
    );
    defer capped_ordered_document_plan.deinit(alloc);
    const capped_ordered_scan_plan = sql_adapter_runtime.LoweredReadPlan{ .document_query = capped_ordered_document_plan };
    try std.testing.expectError(
        error.DocumentSqlRequiresBoundedScan,
        executeLoweredSqlReadPlanAlloc(
            alloc,
            source.source(),
            catalog.iface(),
            "docs",
            schema,
            capped_ordered_scan_plan,
            .read_index,
        ),
    );

    var byte_capped_ordered_document_plan = try sql_adapter.lowerDocumentReadPlanWithBoundedScanPolicyParsedSqlAlloc(
        alloc,
        &ordered_scan_sql,
        schema,
        .{ .max_rows = 10, .max_bytes = 1 },
    );
    defer byte_capped_ordered_document_plan.deinit(alloc);
    const byte_capped_ordered_scan_plan = sql_adapter_runtime.LoweredReadPlan{ .document_query = byte_capped_ordered_document_plan };
    try std.testing.expectError(
        error.DocumentSqlRequiresBoundedScan,
        executeLoweredSqlReadPlanAlloc(
            alloc,
            source.source(),
            catalog.iface(),
            "docs",
            schema,
            byte_capped_ordered_scan_plan,
            .read_index,
        ),
    );

    var full_text_plan = try sql_adapter_runtime.lowerReadPlanAlloc(
        alloc,
        "SELECT _id, title FROM docs WHERE full_text_search('title:alpha') LIMIT 1",
        schema,
        &.{},
    );
    defer full_text_plan.deinit(alloc);
    var full_text_result = (try executeLoweredSqlReadPlanAlloc(
        alloc,
        source.source(),
        catalog.iface(),
        "docs",
        schema,
        full_text_plan,
        .read_index,
    )).?;
    defer full_text_result.deinit(alloc);
    switch (full_text_result) {
        .document_query => |query| {
            try std.testing.expectEqual(@as(u32, 1), query.total);
            try std.testing.expectEqual(@as(usize, 1), query.rows.len);
            try std.testing.expectEqualStrings("{\"_id\":\"doc:a\",\"title\":\"alpha\"}", query.rows[0]);
        },
        else => return error.TestUnexpectedResult,
    }

    var ordered_full_text_sql = try sql_adapter.ParsedSql.initAlloc(
        alloc,
        "SELECT _id, key FROM docs WHERE full_text_search('title:alpha') ORDER BY key DESC LIMIT 2",
    );
    defer ordered_full_text_sql.deinit(alloc);
    var ordered_full_text_document_plan = try sql_adapter.lowerDocumentReadPlanWithCapabilitiesParsedSqlAlloc(
        alloc,
        &ordered_full_text_sql,
        schema,
        .{
            .full_text_filters = true,
            .bounded_scan = .{ .max_rows = 10 },
        },
    );
    defer ordered_full_text_document_plan.deinit(alloc);
    const ordered_full_text_plan = sql_adapter_runtime.LoweredReadPlan{ .document_query = ordered_full_text_document_plan };
    var ordered_full_text_result = (try executeLoweredSqlReadPlanAlloc(
        alloc,
        source.source(),
        catalog.iface(),
        "docs",
        schema,
        ordered_full_text_plan,
        .read_index,
    )).?;
    defer ordered_full_text_result.deinit(alloc);
    switch (ordered_full_text_result) {
        .document_query => |query| {
            try std.testing.expectEqual(@as(u32, 2), query.total);
            try std.testing.expectEqual(@as(usize, 2), query.rows.len);
            try std.testing.expectEqualStrings("{\"_id\":\"doc:c\",\"key\":\"second-key\"}", query.rows[0]);
            try std.testing.expectEqualStrings("{\"_id\":\"doc:a\",\"key\":\"document-key-field\"}", query.rows[1]);
        },
        else => return error.TestUnexpectedResult,
    }

    var capped_ordered_full_text_document_plan = try sql_adapter.lowerDocumentReadPlanWithCapabilitiesParsedSqlAlloc(
        alloc,
        &ordered_full_text_sql,
        schema,
        .{
            .full_text_filters = true,
            .bounded_scan = .{ .max_rows = 1 },
        },
    );
    defer capped_ordered_full_text_document_plan.deinit(alloc);
    const capped_ordered_full_text_plan = sql_adapter_runtime.LoweredReadPlan{ .document_query = capped_ordered_full_text_document_plan };
    try std.testing.expectError(
        error.DocumentSqlRequiresBoundedScan,
        executeLoweredSqlReadPlanAlloc(
            alloc,
            source.source(),
            catalog.iface(),
            "docs",
            schema,
            capped_ordered_full_text_plan,
            .read_index,
        ),
    );

    var like_plan = try sql_adapter_runtime.lowerReadPlanAlloc(
        alloc,
        "SELECT _id, title FROM docs WHERE title LIKE 'alp%' LIMIT 10",
        schema,
        &.{},
    );
    defer like_plan.deinit(alloc);
    var like_result = (try executeLoweredSqlReadPlanAlloc(
        alloc,
        source.source(),
        catalog.iface(),
        "docs",
        schema,
        like_plan,
        .read_index,
    )).?;
    defer like_result.deinit(alloc);
    switch (like_result) {
        .document_query => |query| {
            try std.testing.expectEqual(@as(u32, 2), query.total);
            try std.testing.expectEqual(@as(usize, 2), query.rows.len);
            try std.testing.expectEqualStrings("{\"_id\":\"doc:a\",\"title\":\"alpha\"}", query.rows[0]);
            try std.testing.expectEqualStrings("{\"_id\":\"doc:c\",\"title\":\"alpha\"}", query.rows[1]);
        },
        else => return error.TestUnexpectedResult,
    }

    var full_text_count_plan = try sql_adapter_runtime.lowerReadPlanAlloc(
        alloc,
        "SELECT count(*) AS row_count FROM docs WHERE full_text_search('title:alpha')",
        schema,
        &.{},
    );
    defer full_text_count_plan.deinit(alloc);
    var full_text_count_result = (try executeLoweredSqlReadPlanAlloc(
        alloc,
        source.source(),
        catalog.iface(),
        "docs",
        schema,
        full_text_count_plan,
        .read_index,
    )).?;
    defer full_text_count_result.deinit(alloc);
    switch (full_text_count_result) {
        .aggregate => |aggregate| {
            try std.testing.expectEqual(@as(u32, 1), aggregate.total_groups);
            try std.testing.expectEqual(@as(usize, 1), aggregate.rows.len);
            try std.testing.expectEqualStrings("{\"row_count\":2}", aggregate.rows[0]);
        },
        else => return error.TestUnexpectedResult,
    }

    var full_text_grouped_count_plan = try sql_adapter_runtime.lowerReadPlanAlloc(
        alloc,
        "SELECT count(*) AS row_count FROM docs WHERE full_text_search('title:alpha') GROUP BY key LIMIT 10",
        schema,
        &.{},
    );
    defer full_text_grouped_count_plan.deinit(alloc);
    var full_text_grouped_count_result = (try executeLoweredSqlReadPlanAlloc(
        alloc,
        source.source(),
        catalog.iface(),
        "docs",
        schema,
        full_text_grouped_count_plan,
        .read_index,
    )).?;
    defer full_text_grouped_count_result.deinit(alloc);
    switch (full_text_grouped_count_result) {
        .aggregate => |aggregate| {
            try std.testing.expectEqual(@as(u32, 2), aggregate.total_groups);
            try std.testing.expectEqual(@as(usize, 2), aggregate.rows.len);
            try std.testing.expectEqualStrings("{\"key\":\"document-key-field\",\"row_count\":1}", aggregate.rows[0]);
            try std.testing.expectEqualStrings("{\"key\":\"second-key\",\"row_count\":1}", aggregate.rows[1]);
        },
        else => return error.TestUnexpectedResult,
    }

    var scalar_count_plan = try sql_adapter_runtime.lowerReadPlanAlloc(
        alloc,
        "SELECT count(*) AS row_count FROM docs WHERE key = 'document-key-field'",
        schema,
        &.{},
    );
    defer scalar_count_plan.deinit(alloc);
    var scalar_count_result = (try executeLoweredSqlReadPlanAlloc(
        alloc,
        source.source(),
        catalog.iface(),
        "docs",
        schema,
        scalar_count_plan,
        .read_index,
    )).?;
    defer scalar_count_result.deinit(alloc);
    switch (scalar_count_result) {
        .aggregate => |aggregate| {
            try std.testing.expectEqual(@as(u32, 1), aggregate.total_groups);
            try std.testing.expectEqual(@as(usize, 1), aggregate.rows.len);
            try std.testing.expectEqualStrings("{\"row_count\":1}", aggregate.rows[0]);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "document sql catalog read producers treat catalog misses as terminal" {
    const alloc = std.testing.allocator;
    var parsed_schema = try schema_api.parseValidatedTableSchema(alloc,
        \\{"version":1,"storage_mode":"document","default_type":"doc","document_schemas":{"doc":{"schema":{"type":"object","properties":{"title":{"type":"text"},"status":{"type":"keyword"}},"additionalProperties":true}}}}
    );
    defer parsed_schema.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer storage_schema.freeSchema(alloc, schema);

    const FakeCatalog = struct {
        fn iface(self: *@This()) table_catalog.CatalogSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 99,
                    .database_name = "tenant_ops",
                    .namespace_name = "analytics",
                    .name = "docs",
                    .placement_role = "data",
                }})[0..]),
                .ranges = &.{},
                .stores = &.{},
                .placement_intents = &.{},
                .split_transitions = &.{},
                .merge_transitions = &.{},
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const FakeSource = struct {
        lookup_catalog_calls: usize = 0,
        scan_catalog_calls: usize = 0,
        query_catalog_calls: usize = 0,
        legacy_lookup_calls: usize = 0,
        legacy_scan_calls: usize = 0,
        legacy_query_calls: usize = 0,

        fn source(self: *@This()) TableReadSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .lookup = lookup,
                    .lookup_catalog = lookupCatalog,
                    .scan = scan,
                    .scan_catalog = scanCatalog,
                    .query = query,
                    .query_catalog = queryCatalog,
                },
            };
        }

        fn expectTenantTarget(target: catalog_resources.TableTarget) !void {
            try std.testing.expectEqualStrings("tenant_ops", target.database_name);
            try std.testing.expectEqualStrings("analytics", target.namespace_name);
            try std.testing.expectEqualStrings("docs", target.table_name);
        }

        fn lookup(
            ptr: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
            _: db_mod.types.LookupOptions,
            _: raft_mod.ReadConsistency,
        ) !?LookupResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.legacy_lookup_calls += 1;
            return error.UnexpectedLegacyLookup;
        }

        fn lookupCatalog(
            ptr: *anyopaque,
            _: std.mem.Allocator,
            target: catalog_resources.TableTarget,
            key: []const u8,
            _: db_mod.types.LookupOptions,
            _: raft_mod.ReadConsistency,
        ) !?LookupResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.lookup_catalog_calls += 1;
            try expectTenantTarget(target);
            try std.testing.expectEqualStrings("doc:missing", key);
            return null;
        }

        fn scan(
            ptr: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
            _: []const u8,
            _: db_mod.types.ScanOptions,
            _: raft_mod.ReadConsistency,
        ) !?ScanResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.legacy_scan_calls += 1;
            return error.UnexpectedLegacyScan;
        }

        fn scanCatalog(
            ptr: *anyopaque,
            _: std.mem.Allocator,
            target: catalog_resources.TableTarget,
            from_key: []const u8,
            to_key: []const u8,
            opts: db_mod.types.ScanOptions,
            _: raft_mod.ReadConsistency,
        ) !?ScanResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.scan_catalog_calls += 1;
            try expectTenantTarget(target);
            try std.testing.expectEqualStrings("", from_key);
            try std.testing.expectEqualStrings("", to_key);
            try std.testing.expectEqual(@as(u32, 5), opts.limit);
            return null;
        }

        fn query(
            ptr: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: db_mod.types.SearchRequest,
            _: raft_mod.ReadConsistency,
        ) !?query_api.QueryResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.legacy_query_calls += 1;
            return error.UnexpectedLegacyQuery;
        }

        fn queryCatalog(
            ptr: *anyopaque,
            _: std.mem.Allocator,
            target: catalog_resources.TableTarget,
            req: db_mod.types.SearchRequest,
            _: raft_mod.ReadConsistency,
        ) !?query_api.QueryResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.query_catalog_calls += 1;
            try expectTenantTarget(target);
            try std.testing.expect(req.primary_text_index_name != null);
            try std.testing.expectEqualStrings("full_text_index_v0", req.primary_text_index_name.?);
            return null;
        }
    };

    const tenant_session: catalog_resources.SqlCatalogSession = .{
        .current_database_name = "tenant_ops",
        .search_path = &.{"analytics"},
    };
    var catalog = FakeCatalog{};

    var lookup_projection = try alloc.alloc(sql_adapter_runtime.DocumentProjection, 1);
    lookup_projection[0] = .{ .kind = .id, .output = try alloc.dupe(u8, "_id") };
    var lookup_ids = try alloc.alloc([]const u8, 1);
    lookup_ids[0] = try alloc.dupe(u8, "doc:missing");
    var lookup_plan = sql_adapter_runtime.LoweredReadPlan{ .document_query = .{
        .table_name = try alloc.dupe(u8, "docs"),
        .projection = lookup_projection,
        .producer = .{ .id_lookup = .{ .ids = lookup_ids } },
        .limit = 1,
    } };
    defer lookup_plan.deinit(alloc);

    var lookup_source = FakeSource{};
    var lookup_result = (try executeLoweredSqlReadPlanWithSessionAlloc(
        alloc,
        lookup_source.source(),
        catalog.iface(),
        tenant_session,
        "docs",
        schema,
        lookup_plan,
        .read_index,
    )).?;
    defer lookup_result.deinit(alloc);
    switch (lookup_result) {
        .document_query => |query_result| try std.testing.expectEqual(@as(usize, 0), query_result.rows.len),
        else => return error.TestUnexpectedResult,
    }
    try std.testing.expectEqual(@as(usize, 1), lookup_source.lookup_catalog_calls);
    try std.testing.expectEqual(@as(usize, 0), lookup_source.legacy_lookup_calls);

    var query_projection = try alloc.alloc(sql_adapter_runtime.DocumentProjection, 1);
    query_projection[0] = .{ .kind = .id, .output = try alloc.dupe(u8, "_id") };
    var query_plan = sql_adapter_runtime.LoweredReadPlan{ .document_query = .{
        .table_name = try alloc.dupe(u8, "docs"),
        .projection = query_projection,
        .producer = .{ .indexed_query = .{
            .index_name = try alloc.dupe(u8, "full_text_index_v0"),
            .full_text_query = try alloc.dupe(u8, "alpha"),
        } },
        .limit = 5,
    } };
    defer query_plan.deinit(alloc);

    var query_source = FakeSource{};
    try std.testing.expect((try executeLoweredSqlReadPlanWithSessionAlloc(
        alloc,
        query_source.source(),
        catalog.iface(),
        tenant_session,
        "docs",
        schema,
        query_plan,
        .read_index,
    )) == null);
    try std.testing.expectEqual(@as(usize, 1), query_source.query_catalog_calls);
    try std.testing.expectEqual(@as(usize, 0), query_source.legacy_query_calls);

    var scan_projection = try alloc.alloc(sql_adapter_runtime.DocumentProjection, 1);
    scan_projection[0] = .{ .kind = .id, .output = try alloc.dupe(u8, "_id") };
    var scan_plan = sql_adapter_runtime.LoweredReadPlan{ .document_query = .{
        .table_name = try alloc.dupe(u8, "docs"),
        .projection = scan_projection,
        .producer = .{ .bounded_scan = .{ .max_rows = 5 } },
        .limit = 5,
    } };
    defer scan_plan.deinit(alloc);

    var scan_source = FakeSource{};
    try std.testing.expect((try executeLoweredSqlReadPlanWithSessionAlloc(
        alloc,
        scan_source.source(),
        catalog.iface(),
        tenant_session,
        "docs",
        schema,
        scan_plan,
        .read_index,
    )) == null);
    try std.testing.expectEqual(@as(usize, 1), scan_source.scan_catalog_calls);
    try std.testing.expectEqual(@as(usize, 0), scan_source.legacy_scan_calls);
}

test "lowered document sql aggregate executes native grouped avg materialization" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-document-sql-grouped-avg";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer db.close();
    const alg_config =
        \\{
        \\  "version": 1,
        \\  "table": "docs",
        \\  "group_fields": [{"name":"status","path":"status","type":"string"}],
        \\  "measure_fields": [{"name":"amount","path":"amount","type":"number"}],
        \\  "materializations": [
        \\    {"name":"avg_by_status","op":"avg","group_by":["status"],"measure":"amount"}
        \\  ]
        \\}
    ;
    try db.addIndex(.{ .name = "amount_alg", .kind = .algebraic, .config_json = alg_config });
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"status\":\"active\",\"amount\":10}" },
            .{ .key = "doc:b", .value = "{\"status\":\"archived\",\"amount\":20}" },
            .{ .key = "doc:c", .value = "{\"status\":\"active\",\"amount\":12}" },
            .{ .key = "doc:d", .value = "{\"status\":\"archived\",\"amount\":30}" },
        },
        .sync_level = .full_index,
    });

    var parsed_schema = try schema_api.parseValidatedTableSchema(alloc,
        \\{"version":1,"storage_mode":"document","default_type":"doc","document_schemas":{"doc":{"schema":{"type":"object","properties":{"status":{"type":"keyword"},"amount":{"type":"numeric"}},"additionalProperties":true}}}}
    );
    defer parsed_schema.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer storage_schema.freeSchema(alloc, schema);

    const FakeCatalog = struct {
        fn iface(self: *@This()) table_catalog.CatalogSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .database_name = catalog_resources.default_database_name,
                    .namespace_name = catalog_resources.default_namespace_name,
                    .name = "docs",
                    .placement_role = "data",
                }})[0..]),
                .ranges = &.{},
                .stores = &.{},
                .placement_intents = &.{},
                .split_transitions = &.{},
                .merge_transitions = &.{},
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var catalog = FakeCatalog{};
    var source = BoundTableReadSource.init("docs", 77, &db, raft_mod.read_gate.noopReadableLeaseRequester());
    const indexes_json =
        \\{"amount_alg":{"type":"algebraic","materializations":[{"name":"avg_by_status","op":"avg","group_by":["status"],"measure":"amount"}]}}
    ;
    var parsed_sql = try sql_adapter.ParsedSql.initAlloc(
        alloc,
        "SELECT avg(amount) AS avg_amount FROM docs GROUP BY status LIMIT 10",
    );
    defer parsed_sql.deinit(alloc);
    var document_plan = try sql_adapter.lowerDocumentAggregatePlanWithOptionalIndexesAndCapabilitiesParsedSqlAlloc(
        alloc,
        &parsed_sql,
        schema,
        indexes_json,
        .{},
    );
    defer document_plan.deinit(alloc);
    const plan = sql_adapter_runtime.LoweredReadPlan{ .document_aggregate = document_plan };

    var result = (try executeLoweredSqlReadPlanAlloc(
        alloc,
        source.source(),
        catalog.iface(),
        "docs",
        schema,
        plan,
        .read_index,
    )).?;
    defer result.deinit(alloc);
    switch (result) {
        .aggregate => |aggregate| {
            try std.testing.expectEqual(@as(u32, 2), aggregate.total_groups);
            try std.testing.expectEqual(@as(usize, 2), aggregate.rows.len);
            try std.testing.expectEqualStrings("{\"status\":\"active\",\"avg_amount\":11}", aggregate.rows[0]);
            try std.testing.expectEqualStrings("{\"status\":\"archived\",\"avg_amount\":25}", aggregate.rows[1]);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "lowered document sql aggregate uses catalog target for non-default namespace materialization" {
    const alloc = std.testing.allocator;
    var parsed_schema = try schema_api.parseValidatedTableSchema(alloc,
        \\{"version":1,"storage_mode":"document","default_type":"doc","document_schemas":{"doc":{"schema":{"type":"object","properties":{"status":{"type":"keyword"},"amount":{"type":"numeric"}},"additionalProperties":true}}}}
    );
    defer parsed_schema.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer storage_schema.freeSchema(alloc, schema);

    const indexes_json =
        \\{"amount_alg":{"type":"algebraic","materializations":[{"name":"avg_by_status","op":"avg","group_by":["status"],"measure":"amount"}]}}
    ;
    var parsed_sql = try sql_adapter.ParsedSql.initAlloc(
        alloc,
        "SELECT avg(amount) AS avg_amount FROM docs GROUP BY status LIMIT 10",
    );
    defer parsed_sql.deinit(alloc);
    var document_plan = try sql_adapter.lowerDocumentAggregatePlanWithOptionalIndexesAndCapabilitiesParsedSqlAlloc(
        alloc,
        &parsed_sql,
        schema,
        indexes_json,
        .{},
    );
    defer document_plan.deinit(alloc);
    const plan = sql_adapter_runtime.LoweredReadPlan{ .document_aggregate = document_plan };

    const FakeCatalog = struct {
        fn iface(self: *@This()) table_catalog.CatalogSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 99,
                    .database_name = "tenant_ops",
                    .namespace_name = "analytics",
                    .name = "docs",
                    .placement_role = "data",
                }})[0..]),
                .ranges = &.{},
                .stores = &.{},
                .placement_intents = &.{},
                .split_transitions = &.{},
                .merge_transitions = &.{},
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const FakeSource = struct {
        catalog_calls: usize = 0,
        string_calls: usize = 0,

        fn source(self: *@This()) TableReadSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .lookup = lookup,
                    .scan = scan,
                    .query = query,
                    .document_algebraic_aggregate = documentAlgebraicAggregate,
                    .document_algebraic_aggregate_catalog = documentAlgebraicAggregateCatalog,
                },
            };
        }

        fn lookup(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: db_mod.types.LookupOptions, _: raft_mod.ReadConsistency) !?LookupResponse {
            return error.UnexpectedLookup;
        }

        fn scan(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: []const u8, _: db_mod.types.ScanOptions, _: raft_mod.ReadConsistency) !?ScanResponse {
            return error.UnexpectedScan;
        }

        fn query(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: db_mod.types.SearchRequest, _: raft_mod.ReadConsistency) !?query_api.QueryResponse {
            return error.UnexpectedQuery;
        }

        fn documentAlgebraicAggregate(
            ptr: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: document_sql_runtime.AlgebraicAggregateRequest,
            _: raft_mod.ReadConsistency,
        ) !?document_sql_runtime.AlgebraicAggregateResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.string_calls += 1;
            return error.UnexpectedStringAggregate;
        }

        fn documentAlgebraicAggregateCatalog(
            ptr: *anyopaque,
            aggregate_alloc: std.mem.Allocator,
            target: catalog_resources.TableTarget,
            req: document_sql_runtime.AlgebraicAggregateRequest,
            consistency: raft_mod.ReadConsistency,
        ) !?document_sql_runtime.AlgebraicAggregateResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.catalog_calls += 1;
            try std.testing.expectEqual(raft_mod.ReadConsistency.read_index, consistency);
            try std.testing.expectEqualStrings("tenant_ops", target.database_name);
            try std.testing.expectEqualStrings("analytics", target.namespace_name);
            try std.testing.expectEqualStrings("docs", target.table_name);
            try std.testing.expectEqualStrings("amount_alg", req.index_name);
            try std.testing.expectEqualStrings("avg_by_status", req.materialization_name);
            var rows = try aggregate_alloc.alloc(document_sql_runtime.AlgebraicAggregateRow, 1);
            errdefer aggregate_alloc.free(rows);
            rows[0] = .{
                .group_json = try aggregate_alloc.dupe(u8, "\"active\""),
                .value_json = try aggregate_alloc.dupe(u8, "11"),
                .raw_value = try db_mod.algebraic.algebra.encodeAvgAlloc(aggregate_alloc, .{ .sum = 11, .count = 1 }),
            };
            return .{ .rows = rows, .total_groups = 1 };
        }
    };

    const tenant_session: catalog_resources.SqlCatalogSession = .{
        .current_database_name = "tenant_ops",
        .search_path = &.{ "analytics", "public" },
    };
    var catalog = FakeCatalog{};
    var fake = FakeSource{};
    var result = (try executeLoweredSqlReadPlanWithSessionAlloc(
        alloc,
        fake.source(),
        catalog.iface(),
        tenant_session,
        "docs",
        schema,
        plan,
        .read_index,
    )).?;
    defer result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), fake.catalog_calls);
    try std.testing.expectEqual(@as(usize, 0), fake.string_calls);
    switch (result) {
        .aggregate => |aggregate| {
            try std.testing.expectEqual(@as(u32, 1), aggregate.total_groups);
            try std.testing.expectEqual(@as(usize, 1), aggregate.rows.len);
            try std.testing.expectEqualStrings("{\"status\":\"active\",\"avg_amount\":11}", aggregate.rows[0]);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "bound table read source scans keys as ndjson" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-scan";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\"}" },
        },
    });

    var source = BoundTableReadSource.init("docs", 77, &db, raft_mod.read_gate.noopReadableLeaseRequester());
    var scan = (try source.source().scan(alloc, "docs", "", "", .{
        .include_documents = true,
        .fields = &.{"title"},
        .include_all_fields = false,
    }, .read_index)).?;
    defer scan.deinit(alloc);
    const ScanRow = struct {
        key: []const u8,
        title: []const u8,
    };
    const rows = try parseNdjsonTestRowsAlloc(ScanRow, alloc, scan.ndjson);
    defer alloc.free(rows);
    try std.testing.expectEqual(@as(usize, 2), rows.len);
    try std.testing.expectEqualStrings("doc:a", rows[0].key);
    try std.testing.expectEqualStrings("alpha", rows[0].title);
}

test "bound table read source formats query responses" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-query";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }
    try db.addIndex(.{ .name = "full_text_index_v0", .kind = .full_text, .config_json = "{}" });
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"hello\"}" }},
        .sync_level = .full_index,
    });

    var source = BoundTableReadSource.init("docs", 77, &db, raft_mod.read_gate.noopReadableLeaseRequester());
    var response = (try source.source().query(alloc, "docs", .{
        .query = .{ .match = .{ .field = "body", .text = "hello" } },
        .limit = 5,
    }, .read_index)).?;
    defer response.deinit(alloc);
    var parsed = try parseJsonTestBody(metadata_openapi.QueryResponses, alloc, response.json);
    defer parsed.deinit();
    try std.testing.expectEqual(@as(usize, 1), parsed.value.responses.?.len);
    try std.testing.expectEqualStrings("doc:a", parsed.value.responses.?[0].hits.?.hits.?[0]._id);
}

test "bound table read source preflights query requests" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-preflight";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }
    try db.addIndex(.{ .name = "dv_v1", .kind = .dense_vector, .config_json = "{\"field\":\"embedding\",\"dims\":3}" });

    var source = BoundTableReadSource.init("docs", 77, &db, raft_mod.read_gate.noopReadableLeaseRequester());
    try std.testing.expectError(error.InvalidArgument, source.source().preflightQuery(alloc, "docs", .{
        .index_name = "dv_v1",
        .dense = .{ .vector = &.{ 1.0, 2.0 }, .k = 5 },
    }, .read_index, 0));

    var summary = (try source.source().preflightQuery(alloc, "docs", .{
        .index_name = "dv_v1",
        .dense = .{ .vector = &.{ 1.0, 2.0, 3.0 }, .k = 5 },
    }, .read_index, 0)).?;
    defer summary.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), summary.result_refs.len);
    try std.testing.expectEqualStrings("$embeddings_results", summary.result_refs[0]);
    try std.testing.expectEqual(@as(u32, 1), summary.shard_count);
    try std.testing.expectEqual(@as(u32, 0), summary.remote_shard_count);
    try std.testing.expectEqual(@as(u32, 1), summary.dense_query_count);
    try std.testing.expectEqual(@as(u32, 1), summary.vector_worker_candidate_count);
    try std.testing.expectEqual(@as(u32, 0), summary.vector_worker_fallback_count);
    try std.testing.expectEqual(@as(u32, 0), summary.vector_worker_filter_constraint_count);
    try std.testing.expect(!summary.vector_worker_requires_algebraic_filter_resolution);
    try std.testing.expectEqual(@as(u64, 5), summary.dense_effective_k_total);
    try std.testing.expect(summary.dense_search_width_total >= summary.dense_effective_k_total);
    try std.testing.expect(summary.dense_search_width_max >= 64);
    try std.testing.expect(summary.dense_epsilon_max >= 1.0);
}

test "bound table read source reranks hits after materialization" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-rerank";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }
    try db.addIndex(.{ .name = "full_text_index_v0", .kind = .full_text, .config_json = "{}" });
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"hello alpha\"}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"body\":\"hello beta\"}" },
        },
        .sync_level = .full_index,
    });

    var source = BoundTableReadSource.init("docs", 77, &db, raft_mod.read_gate.noopReadableLeaseRequester());
    var ts = try httpx.TestServer.start(alloc, io_impl.io(), &.{
        .{ .method = .POST, .path = "/rerank", .respond = .{
            .body = "{\"scores\":[0.1,0.9]}",
        } },
    });
    defer ts.deinit();

    const url = try std.fmt.allocPrint(alloc, "{s}", .{ts.baseUrl()});
    defer alloc.free(url);

    var response: ?query_api.QueryResponse = null;
    defer if (response) |*value| value.deinit(alloc);
    var run_err: ?anyerror = null;
    var group = std.Io.Group.init;

    const Fiber = struct {
        fn run(
            a: std.mem.Allocator,
            read_source: *BoundTableReadSource,
            out: *?query_api.QueryResponse,
            err_out: *?anyerror,
            reranker_url: []const u8,
        ) std.Io.Cancelable!void {
            out.* = read_source.source().query(a, "docs", .{
                .query = .{ .match = .{ .field = "body", .text = "hello" } },
                .limit = 10,
                .profile = true,
                .reranker = .{
                    .provider = .antfly,
                    .model = "cross-encoder/ms-marco-MiniLM-L-6-v2",
                    .field = "body",
                    .url = reranker_url,
                },
                .reranker_query_text = "hello",
            }, .read_index) catch |err| {
                err_out.* = err;
                return;
            };
        }
    };

    group.concurrent(io_impl.io(), Fiber.run, .{ alloc, &source, &response, &run_err, url }) catch return;
    try ts.handleOne();
    group.await(io_impl.io()) catch {};
    if (run_err) |err| return err;

    try std.testing.expect(response != null);
    const RerankResponse = struct {
        responses: []struct {
            hits: ?struct {
                hits: ?[]struct { _id: []const u8 } = null,
            } = null,
            profile: ?struct {
                reranker: ?struct {
                    model: []const u8,
                } = null,
            } = null,
        },
    };
    var parsed = try parseJsonTestBody(RerankResponse, alloc, response.?.json);
    defer parsed.deinit();
    const inner = parsed.value.responses[0];
    try std.testing.expectEqualStrings("doc:b", inner.hits.?.hits.?[0]._id);
    try std.testing.expectEqualStrings("doc:a", inner.hits.?.hits.?[1]._id);
    try std.testing.expectEqualStrings("cross-encoder/ms-marco-MiniLM-L-6-v2", inner.profile.?.reranker.?.model);
}

test "provisioned table read source routes lookup and scan across ranges" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-provisioned-reads";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const left_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(left_path);
    const right_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7002);
    defer alloc.free(right_path);

    var left_db = try db_mod.DB.open(alloc, left_path, .{});
    defer left_db.close();
    var right_db = try db_mod.DB.open(alloc, right_path, .{});
    defer right_db.close();

    try left_db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
    });
    try right_db.batch(.{
        .writes = &.{.{ .key = "doc:z", .value = "{\"title\":\"zeta\"}" }},
    });

    const FakeCatalog = struct {
        const statuses = [_]metadata_reconciler.MergedGroupStatus{
            .{
                .group_id = 7001,
                .doc_identity = .{
                    .namespace_table_id = 7,
                    .namespace_shard_id = 7001,
                    .namespace_range_id = 7001,
                    .next_ordinal = 4,
                    .allocated_ordinals = 3,
                    .state_rows = 3,
                    .live_ordinals = 3,
                    .complete = true,
                },
            },
            .{
                .group_id = 7002,
                .doc_identity = .{
                    .namespace_table_id = 7,
                    .namespace_shard_id = 7002,
                    .namespace_range_id = 7002,
                    .next_ordinal = 3,
                    .allocated_ordinals = 2,
                    .state_rows = 2,
                    .live_ordinals = 2,
                    .complete = true,
                },
            },
        };

        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{ .table_id = 7, .name = "docs", .placement_role = "data", .indexes_json = "{\"full_text_index_v0\":{\"type\":\"full_text\"}}" }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = "doc:m" },
                    .{ .group_id = 7002, .table_id = 7, .start_key = "doc:m", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
                .merged_group_statuses = @constCast(statuses[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = ProvisionedTableReadSource.init(path, FakeCatalog.iface(), raft_mod.read_gate.noopReadableLeaseRequester());
    var lookup = (try source.source().lookup(alloc, "docs", "doc:z", .{}, .stale)).?;
    defer lookup.deinit(alloc);
    const LookupTitle = struct { title: []const u8 };
    var parsed_lookup = try parseJsonTestBody(LookupTitle, alloc, lookup.json);
    defer parsed_lookup.deinit();
    try std.testing.expectEqualStrings("zeta", parsed_lookup.value.title);

    var scan = (try source.source().scan(alloc, "docs", "", "", .{
        .include_documents = true,
        .fields = &.{"title"},
        .include_all_fields = false,
    }, .stale)).?;
    defer scan.deinit(alloc);
    const ScanRow = struct {
        key: []const u8,
        title: []const u8,
    };
    const rows = try parseNdjsonTestRowsAlloc(ScanRow, alloc, scan.ndjson);
    defer alloc.free(rows);
    try std.testing.expectEqual(@as(usize, 2), rows.len);
    try std.testing.expectEqualStrings("alpha", rows[0].title);
    try std.testing.expectEqualStrings("zeta", rows[1].title);
}

test "provisioned standby read gate permits stale reads and routes non-stale reads to primary" {
    const alloc = std.testing.allocator;
    const root = ".zig-cache/tmp/table-reads-ha-read-gate";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), root) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), root) catch {};
    try std.Io.Dir.cwd().createDirPath(io_impl.io(), root);

    const receive_path_raw = try std.fmt.allocPrint(alloc, "{s}/received.wal", .{root});
    defer alloc.free(receive_path_raw);
    const receive_path = try alloc.dupeZ(u8, receive_path_raw);
    defer alloc.free(receive_path);
    const progress_path_raw = try std.fmt.allocPrint(alloc, "{s}/progress.wal", .{root});
    defer alloc.free(progress_path_raw);
    const progress_path = try alloc.dupeZ(u8, progress_path_raw);
    defer alloc.free(progress_path);

    var standby = try ha_standby_mod.Standby.open(alloc, receive_path.ptr, progress_path.ptr, .{
        .cluster_id = 100,
        .shard_id = 10,
        .table_id = 20,
        .timeline_id = 1,
        .epoch = 1,
    }, .{});
    defer standby.close();

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = ProvisionedTableReadSource.init("/tmp/unused-antfly-ha-read-gate", NoCatalog.iface(), raft_mod.read_gate.noopReadableLeaseRequester());
    _ = source.withHAReadGate(.{ .standby = &standby });

    try std.testing.expectError(
        error.HAReadRequiresPrimary,
        source.source().lookup(alloc, "docs", "doc:a", .{}, .read_index),
    );
    try std.testing.expectError(
        error.HAReadRequiresPrimary,
        source.source().lookup(alloc, "docs", "doc:a", .{}, .leader_lease),
    );
    try std.testing.expectError(
        error.UnexpectedCatalogCall,
        source.source().lookup(alloc, "docs", "doc:a", .{}, .stale),
    );
}

test "provisioned table read source merges query results across ranges" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-provisioned-query";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const left_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(left_path);
    const right_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7002);
    defer alloc.free(right_path);

    var left_db = try db_mod.DB.open(alloc, left_path, .{});
    defer left_db.close();
    var right_db = try db_mod.DB.open(alloc, right_path, .{});
    defer right_db.close();

    try left_db.addIndex(.{ .name = "full_text_index_v0", .kind = .full_text, .config_json = "{}" });
    try right_db.addIndex(.{ .name = "full_text_index_v0", .kind = .full_text, .config_json = "{}" });

    try left_db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"hello world\"}" }},
        .sync_level = .full_index,
    });
    try right_db.batch(.{
        .writes = &.{.{ .key = "doc:z", .value = "{\"title\":\"zeta\",\"body\":\"hello there\"}" }},
        .sync_level = .full_index,
    });

    const FakeCatalog = struct {
        const statuses = [_]metadata_reconciler.MergedGroupStatus{
            .{
                .group_id = 7001,
                .doc_identity = .{
                    .namespace_table_id = 7,
                    .namespace_shard_id = 7001,
                    .namespace_range_id = 7001,
                    .next_ordinal = 4,
                    .allocated_ordinals = 3,
                    .state_rows = 3,
                    .live_ordinals = 3,
                    .complete = true,
                },
            },
            .{
                .group_id = 7002,
                .doc_identity = .{
                    .namespace_table_id = 7,
                    .namespace_shard_id = 7002,
                    .namespace_range_id = 7002,
                    .next_ordinal = 3,
                    .allocated_ordinals = 2,
                    .state_rows = 2,
                    .live_ordinals = 2,
                    .complete = true,
                },
            },
        };

        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{ .table_id = 7, .name = "docs", .placement_role = "data", .indexes_json = "{\"full_text_index_v0\":{\"type\":\"full_text\"}}" }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = "doc:m" },
                    .{ .group_id = 7002, .table_id = 7, .start_key = "doc:m", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
                .merged_group_statuses = @constCast(statuses[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = ProvisionedTableReadSource.init(path, FakeCatalog.iface(), raft_mod.read_gate.noopReadableLeaseRequester());
    _ = source.withIo(&io_impl);
    var response = (try source.source().query(alloc, "docs", .{
        .query = .{ .match = .{ .field = "body", .text = "hello" } },
        .limit = 10,
    }, .stale)).?;
    defer response.deinit(alloc);
    var parsed = try parseJsonTestBody(metadata_openapi.QueryResponses, alloc, response.json);
    defer parsed.deinit();
    const hits = parsed.value.responses.?[0].hits.?.hits.?;
    try std.testing.expectEqualStrings("doc:a", hits[0]._id);
    try std.testing.expectEqualStrings("doc:z", hits[1]._id);
}

test "provisioned table read source serves dense queries for explicit external embeddings" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-provisioned-query-dense";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const group_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(group_path);
    var db = try db_mod.DB.open(alloc, group_path, .{});
    defer db.close();

    try db.addIndex(.{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}",
    });
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_embeddings\":{\"dense_idx\":[1,0,0]}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_embeddings\":{\"dense_idx\":[0,1,0]}}" },
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_embeddings\":{\"dense_idx\":[0.9,0.1,0]}}" },
        },
        .sync_level = .full_index,
    });

    const FakeCatalog = struct {
        const statuses = [_]metadata_reconciler.MergedGroupStatus{
            .{
                .group_id = 7001,
                .doc_identity = .{
                    .namespace_table_id = 7,
                    .namespace_shard_id = 7001,
                    .namespace_range_id = 7001,
                    .next_ordinal = 4,
                    .allocated_ordinals = 3,
                    .state_rows = 3,
                    .live_ordinals = 3,
                    .complete = true,
                },
            },
            .{
                .group_id = 7002,
                .doc_identity = .{
                    .namespace_table_id = 7,
                    .namespace_shard_id = 7002,
                    .namespace_range_id = 7002,
                    .next_ordinal = 3,
                    .allocated_ordinals = 2,
                    .state_rows = 2,
                    .live_ordinals = 2,
                    .complete = true,
                },
            },
        };

        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"dense_idx\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":3}}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
                .merged_group_statuses = @constCast(statuses[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = ProvisionedTableReadSource.init(path, FakeCatalog.iface(), raft_mod.read_gate.noopReadableLeaseRequester());
    var response = (try source.source().query(alloc, "docs", .{
        .index_name = "dense_idx",
        .query = .{ .dense_knn = .{
            .vector = &.{ 1.0, 0.0, 0.0 },
            .k = 3,
        } },
        .limit = 3,
    }, .stale)).?;
    defer response.deinit(alloc);
    var parsed = try parseJsonTestBody(metadata_openapi.QueryResponses, alloc, response.json);
    defer parsed.deinit();
    const hits = parsed.value.responses.?[0].hits.?.hits.?;
    try std.testing.expectEqualStrings("doc:a", hits[0]._id);
    try std.testing.expectEqualStrings("doc:c", hits[1]._id);
}

test "provisioned local query execution returns stamped identity request" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-provisioned-query-stamped-identity";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const group_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(group_path);
    {
        var db = try db_mod.DB.open(alloc, group_path, .{});
        defer db.close();
        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
            .sync_level = .write,
        });
    }

    const FakeCatalog = struct {
        const statuses = [_]metadata_reconciler.MergedGroupStatus{
            .{
                .group_id = 7001,
                .doc_identity = .{
                    .namespace_table_id = 7,
                    .namespace_shard_id = 7001,
                    .namespace_range_id = 7001,
                    .next_ordinal = 4,
                    .allocated_ordinals = 3,
                    .state_rows = 3,
                    .live_ordinals = 3,
                    .complete = true,
                },
            },
            .{
                .group_id = 7002,
                .doc_identity = .{
                    .namespace_table_id = 7,
                    .namespace_shard_id = 7002,
                    .namespace_range_id = 7002,
                    .next_ordinal = 3,
                    .allocated_ordinals = 2,
                    .state_rows = 2,
                    .live_ordinals = 2,
                    .complete = true,
                },
            },
        };

        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
                .merged_group_statuses = @constCast(statuses[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var execution = try queryHostedLocalDetailed(
        null,
        path,
        FakeCatalog.iface(),
        raft_mod.read_gate.noopReadableLeaseRequester(),
        alloc,
        7001,
        0,
        null,
        null,
        null,
        null,
        "docs",
        .{ .limit = 1 },
        .stale,
    );
    defer execution.result.deinit();

    try std.testing.expect(execution.request.identity_read_generation != null);
    try std.testing.expectEqual(@as(u32, 1), execution.result.total_hits);
    try std.testing.expectEqualStrings("doc:a", execution.result.hits[0].id);
}

test "provisioned table read source serves public dense query requests with read_index" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-provisioned-query-dense-public";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const group_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(group_path);
    var db = try db_mod.DB.open(alloc, group_path, .{});
    defer db.close();

    const FakeCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"dense_idx\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":3}}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    try db.addIndex(.{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}",
    });
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_embeddings\":{\"dense_idx\":[1,0,0]}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_embeddings\":{\"dense_idx\":[0,1,0]}}" },
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_embeddings\":{\"dense_idx\":[0.9,0.1,0]}}" },
        },
        .sync_level = .full_index,
    });

    var source = ProvisionedTableReadSource.init(path, FakeCatalog.iface(), raft_mod.read_gate.noopReadableLeaseRequester());

    var owned = try query_api.parseQueryRequest(alloc, null, "docs",
        \\{"embeddings":{"dense_idx":[1.0,0.0,0.0]},"indexes":["dense_idx"],"limit":3}
    );
    defer owned.deinit(alloc);

    var response = (try source.source().query(alloc, "docs", owned.req, .read_index)).?;
    defer response.deinit(alloc);
    var parsed = try parseJsonTestBody(metadata_openapi.QueryResponses, alloc, response.json);
    defer parsed.deinit();
    const hits = parsed.value.responses.?[0].hits.?.hits.?;
    try std.testing.expectEqualStrings("doc:a", hits[0]._id);
    try std.testing.expectEqualStrings("doc:c", hits[1]._id);
}

test "provisioned table read source serves profiled public dense query requests with read_index" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-provisioned-query-dense-public-profiled";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const group_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(group_path);
    var db = try db_mod.DB.open(alloc, group_path, .{});
    defer db.close();

    const FakeCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"dense_idx\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":3}}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    try db.addIndex(.{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}",
    });
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_embeddings\":{\"dense_idx\":[1,0,0]}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_embeddings\":{\"dense_idx\":[0,1,0]}}" },
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_embeddings\":{\"dense_idx\":[0.9,0.1,0]}}" },
        },
        .sync_level = .full_index,
    });

    var source = ProvisionedTableReadSource.init(path, FakeCatalog.iface(), raft_mod.read_gate.noopReadableLeaseRequester());

    var owned = try query_api.parseQueryRequest(alloc, null, "docs",
        \\{"embeddings":{"dense_idx":[1.0,0.0,0.0]},"indexes":["dense_idx"],"limit":3,"profile":true}
    );
    defer owned.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), owned.req.dense_queries.len);
    try std.testing.expectEqualStrings("dense_idx", owned.req.dense_queries[0].index_name);

    var response = (try source.source().query(alloc, "docs", owned.req, .read_index)).?;
    defer response.deinit(alloc);
    var parsed = try parseJsonTestBody(metadata_openapi.QueryResponses, alloc, response.json);
    defer parsed.deinit();
    const hits = parsed.value.responses.?[0].hits.?.hits.?;
    try std.testing.expectEqualStrings("doc:a", hits[0]._id);
    try std.testing.expectEqualStrings("doc:c", hits[1]._id);
    try std.testing.expect(parsed.value.responses.?[0].profile != null);
}

test "provisioned table read source serves public dense query requests without explicit indexes" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-provisioned-query-dense-public-implicit-index";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const group_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(group_path);
    var db = try db_mod.DB.open(alloc, group_path, .{});
    defer db.close();

    const FakeCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"dense_idx\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":3}}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    try db.addIndex(.{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}",
    });
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_embeddings\":{\"dense_idx\":[1,0,0]}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_embeddings\":{\"dense_idx\":[0,1,0]}}" },
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_embeddings\":{\"dense_idx\":[0.9,0.1,0]}}" },
        },
        .sync_level = .full_index,
    });

    var source = ProvisionedTableReadSource.init(path, FakeCatalog.iface(), raft_mod.read_gate.noopReadableLeaseRequester());

    var owned = try query_api.parseQueryRequest(alloc, null, "docs",
        \\{"embeddings":{"dense_idx":[1.0,0.0,0.0]},"limit":3}
    );
    defer owned.deinit(alloc);

    var response = (try source.source().query(alloc, "docs", owned.req, .read_index)).?;
    defer response.deinit(alloc);
    var parsed = try parseJsonTestBody(metadata_openapi.QueryResponses, alloc, response.json);
    defer parsed.deinit();
    const hits = parsed.value.responses.?[0].hits.?.hits.?;
    try std.testing.expectEqualStrings("doc:a", hits[0]._id);
    try std.testing.expectEqualStrings("doc:c", hits[1]._id);
}

test "provisioned table read source serves benchmark-shaped packed dense query with full-text present" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-provisioned-query-dense-benchmark-shaped";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const group_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(group_path);
    var db = try db_mod.DB.open(alloc, group_path, .{});
    defer db.close();

    const FakeCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json =
                    \\{"ft_v1":{"type":"full_text"},"vec":{"type":"embeddings","external":true,"dimension":3}}
                    ,
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{}",
    });
    try db.addIndex(.{
        .name = "vec",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"vec_data\",\"dims\":3,\"metric\":\"cosine\",\"external\":true}",
    });
    try db.batch(.{
        .writes = &.{
            .{ .key = "key:0", .value = "{\"id\":0,\"metadata\":0,\"vec_data\":\"0\",\"body\":\"alpha retrieval\",\"_embeddings\":{\"vec\":\"AACAPwAAAAAAAAAA\"}}" },
            .{ .key = "key:1", .value = "{\"id\":1,\"metadata\":1,\"vec_data\":\"1\",\"body\":\"beta retrieval\",\"_embeddings\":{\"vec\":\"AAAAAAAAgD8AAAAA\"}}" },
            .{ .key = "key:2", .value = "{\"id\":2,\"metadata\":2,\"vec_data\":\"2\",\"body\":\"gamma retrieval\",\"_embeddings\":{\"vec\":\"ZmZmP83MzD0AAAAA\"}}" },
        },
        .sync_level = .full_index,
    });

    var source = ProvisionedTableReadSource.init(path, FakeCatalog.iface(), raft_mod.read_gate.noopReadableLeaseRequester());

    var owned = try query_api.parseQueryRequest(alloc, null, "docs",
        \\{"embeddings":{"vec":[1.0,0.0,0.0]},"limit":3}
    );
    defer owned.deinit(alloc);

    var response = (try source.source().query(alloc, "docs", owned.req, .read_index)).?;
    defer response.deinit(alloc);
    var parsed = try parseJsonTestBody(metadata_openapi.QueryResponses, alloc, response.json);
    defer parsed.deinit();
    const hits = parsed.value.responses.?[0].hits.?.hits.?;
    try std.testing.expectEqualStrings("key:0", hits[0]._id);
    try std.testing.expectEqualStrings("key:2", hits[1]._id);
}

test "provisioned table read source preflights every local group" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-provisioned-preflight-multigroup";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const left_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(left_path);
    const right_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7002);
    defer alloc.free(right_path);

    var left_db = try db_mod.DB.open(alloc, left_path, .{});
    defer left_db.close();
    var right_db = try db_mod.DB.open(alloc, right_path, .{});
    defer right_db.close();

    try left_db.addIndex(.{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}",
    });

    const FakeCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"dense_idx\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":3}}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = "doc:m" },
                    .{ .group_id = 7002, .table_id = 7, .start_key = "doc:m", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = ProvisionedTableReadSource.init(path, FakeCatalog.iface(), raft_mod.read_gate.noopReadableLeaseRequester());
    _ = source.withIo(&io_impl);
    try std.testing.expectError(error.InvalidArgument, source.source().preflightQuery(alloc, "docs", .{
        .index_name = "dense_idx",
        .dense = .{ .vector = &.{ 1.0, 2.0, 3.0 }, .k = 5 },
    }, .read_index, 0));
    try std.testing.expectError(error.UnsupportedQueryRequest, source.source().preflightQuery(alloc, "docs", .{
        .graph_queries = &.{
            .{
                .name = "neighbors",
                .query = .{
                    .query_type = .neighbors,
                    .index_name = "graph_v1",
                    .start_nodes = .{ .result_ref = .{ .ref = "$embeddings_results", .limit = 1 } },
                    .params = .{ .edge_types = &.{}, .max_depth = 1 },
                },
            },
        },
    }, .read_index, 0));
}

test "provisioned local runtime statuses reconcile empty managed embeddings indexes" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-provisioned-runtime-status-managed";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const group_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(group_path);
    var db = try db_mod.DB.open(alloc, group_path, .{});
    defer db.close();

    const FakeCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"semantic_idx\":{\"type\":\"embeddings\",\"dimension\":3,\"external\":true}}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = ProvisionedTableReadSource.init(path, FakeCatalog.iface(), raft_mod.read_gate.noopReadableLeaseRequester());
    var cache = ProvisionedTableReadCache.init(alloc);
    defer cache.deinit();
    source.cache = &cache;
    var db_lease = try cache.getOrOpen(path, FakeCatalog.iface(), 7001, 0, "docs");
    defer db_lease.release();
    var statuses = (try source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(usize, 1), statuses.items[0].stats.indexes.len);
    try std.testing.expectEqualStrings("semantic_idx", statuses.items[0].stats.indexes[0].name);
    try std.testing.expectEqual(false, statuses.items[0].stats.indexes[0].backfill_active);
    try std.testing.expectEqual(@as(u64, 0), statuses.items[0].stats.indexes[0].doc_count);
}

test "provisioned query db installs asset producer from indexes_json and replays assets" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-provisioned-asset-enrichment";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const group_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(group_path);
    {
        var db = try db_mod.DB.open(alloc, group_path, .{});
        defer db.close();
        try db.batch(.{
            .writes = &.{.{
                .key = "doc:a",
                .value = "{\"body\":\"hello\"}",
            }},
            .sync_level = .write,
        });
    }

    var backend_runtime = try db_mod.background_runtime.BackendRuntime.init(alloc, .{});
    defer backend_runtime.deinit();

    const FakeLocal = struct {
        calls: usize = 0,

        fn embedDenseTexts(
            ptr: *anyopaque,
            a: std.mem.Allocator,
            model: []const u8,
            texts: []const []const u8,
        ) anyerror![][]f32 {
            _ = ptr;
            _ = a;
            _ = model;
            _ = texts;
            return error.TestUnexpectedResult;
        }

        fn embedSparseTexts(
            ptr: *anyopaque,
            a: std.mem.Allocator,
            model: []const u8,
            texts: []const []const u8,
        ) anyerror![]db_embedder.SparseEmbedding {
            _ = ptr;
            _ = a;
            _ = model;
            _ = texts;
            return error.TestUnexpectedResult;
        }

        fn generateText(
            ptr: *anyopaque,
            a: std.mem.Allocator,
            model: []const u8,
            roles: []const []const u8,
            contents: []const []const u8,
        ) anyerror![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            try std.testing.expectEqualStrings("local-model", model);
            try std.testing.expectEqual(@as(usize, 1), roles.len);
            try std.testing.expectEqualStrings("user", roles[0]);
            try std.testing.expectEqualStrings("hello", contents[0]);
            return try a.dupe(u8, "generator:hello");
        }
    };

    var fake = FakeLocal{};
    const local_provider = managed_embedder.AntflyProvider{
        .ptr = &fake,
        .embed_dense_texts = FakeLocal.embedDenseTexts,
        .embed_sparse_texts = FakeLocal.embedSparseTexts,
        .generate_text = FakeLocal.generateText,
    };

    const FakeCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json =
                    \\{"search_idx":{"type":"full_text","field":"body","enrichments":[{"name":"generated_title_v1","kind":"asset","field":"body","content_type":"text/plain","producer_json":"{\"type\":\"generator\",\"config\":{\"provider\":\"antfly\",\"model\":\"local-model\"}}"}]}}
                    ,
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var cache = ProvisionedTableReadCache.init(alloc);
    defer cache.deinit();
    cache.backend_runtime = &backend_runtime;
    cache.antfly_provider = local_provider;

    var db_lease = try cache.getOrOpen(path, FakeCatalog.iface(), 7001, 0, "docs");
    defer db_lease.release();

    try std.testing.expectEqual(@as(usize, 1), fake.calls);
    var lookup = (try db_lease.db.lookup(alloc, "doc:a", .{
        .fields = &.{"_artifacts"},
        .include_all_fields = false,
    })).?;
    defer lookup.deinit(alloc);
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, lookup.json, .{});
    defer parsed.deinit();
    const artifacts = parsed.value.object.get("_artifacts").?.object;
    try std.testing.expectEqualStrings("generator:hello", artifacts.get("generated_title_v1").?.object.get("value").?.string);
}

test "provisioned table read source runtime status stays cache-only without shared snapshot" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-provisioned-read-runtime-cache";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const group_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(group_path);
    var db = try db_mod.DB.open(alloc, group_path, .{});
    defer db.close();

    const WarmCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"semantic_idx\":{\"type\":\"embeddings\",\"dimension\":3,\"external\":true}}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var cache = ProvisionedTableReadCache.init(alloc);
    defer cache.deinit();
    var db_lease = try cache.getOrOpen(path, WarmCatalog.iface(), 7001, 0, "docs");
    defer db_lease.release();

    var source = ProvisionedTableReadSource.init(path, NoCatalog.iface(), raft_mod.read_gate.noopReadableLeaseRequester());
    source.cache = &cache;

    try std.testing.expect((try source.source().localRuntimeStatuses(alloc, "docs")) == null);
}

test "provisioned table read source runtime status falls back to shared snapshot cache" {
    const alloc = std.testing.allocator;

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    const items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    items[0] = .{
        .group_id = 7001,
        .stats = .{
            .doc_count = 9,
            .index_count = 1,
            .indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1),
        },
    };
    items[0].stats.indexes[0] = .{
        .name = try alloc.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 9,
    };

    const snapshots = try alloc.alloc(runtime_status.TableRuntimeSnapshot, 1);
    defer alloc.free(snapshots);
    snapshots[0] = .{
        .table_name = try alloc.dupe(u8, "docs"),
        .statuses = .{ .items = items },
    };
    snapshot_cache.replaceOwned(snapshots);

    var source = ProvisionedTableReadSource.init("/tmp/unused-antfly-runtime-snapshot", NoCatalog.iface(), raft_mod.read_gate.noopReadableLeaseRequester());
    source.runtime_status_cache = &snapshot_cache;

    var statuses = (try source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(u64, 7001), statuses.items[0].group_id);
    try std.testing.expectEqual(@as(u64, 9), statuses.items[0].stats.doc_count);
    try std.testing.expectEqualStrings("semantic_idx", statuses.items[0].stats.indexes[0].name);
}

test "provisioned table read source runtime status prefers shared snapshot cache" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-provisioned-read-runtime-prefers-snapshot";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const group_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(group_path);
    var db = try db_mod.DB.open(alloc, group_path, .{});
    defer db.close();

    const WarmCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"cached_handle_idx\":{\"type\":\"embeddings\",\"dimension\":3,\"external\":true}}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var cache = ProvisionedTableReadCache.init(alloc);
    defer cache.deinit();
    var db_lease = try cache.getOrOpen(path, WarmCatalog.iface(), 7001, 0, "docs");
    defer db_lease.release();

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();
    var indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1);
    indexes[0] = .{
        .name = try alloc.dupe(u8, "snapshot_idx"),
        .kind = .dense_vector,
        .doc_count = 42,
    };
    var status = runtime_status.LocalTableRuntimeStatus{
        .group_id = 7001,
        .stats = .{
            .doc_count = 42,
            .index_count = 1,
            .indexes = indexes,
        },
    };
    defer status.deinit(alloc);
    try snapshot_cache.upsertGroupStatus("docs", status);

    var source = ProvisionedTableReadSource.init(path, NoCatalog.iface(), raft_mod.read_gate.noopReadableLeaseRequester());
    source.cache = &cache;
    source.runtime_status_cache = &snapshot_cache;

    var statuses = (try source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(u64, 7001), statuses.items[0].group_id);
    try std.testing.expectEqual(@as(u64, 42), statuses.items[0].stats.doc_count);
    try std.testing.expectEqualStrings("snapshot_idx", statuses.items[0].stats.indexes[0].name);
}

test "provisioned table read source falls back from read_index to stale on not leader" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-provisioned-read-fallback";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const group_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(group_path);
    var db = try db_mod.DB.open(alloc, group_path, .{});
    defer db.close();

    try db.addIndex(.{ .name = "full_text_index_v0", .kind = .full_text, .config_json = "{}" });
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"hello world\"}" }},
        .sync_level = .full_index,
        .timestamp_ns = 4321,
    });

    const FakeCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{ .table_id = 7, .name = "docs", .placement_role = "data", .indexes_json = "{\"full_text_index_v0\":{\"type\":\"full_text\"}}" }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const NotLeaderOnce = struct {
        count: usize = 0,

        fn requester(self: *@This()) raft_mod.ReadableLeaseRequester {
            return .{
                .ptr = self,
                .vtable = &.{
                    .request_readable_lease = requestReadableLease,
                },
            };
        }

        fn requestReadableLease(ptr: *anyopaque, _: u64, _: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.count += 1;
            return error.NotLeader;
        }
    };

    var requester = NotLeaderOnce{};
    var source = ProvisionedTableReadSource.init(path, FakeCatalog.iface(), requester.requester());

    var lookup = (try source.source().lookup(alloc, "docs", "doc:a", .{}, .read_index)).?;
    defer lookup.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 4321), lookup.version);
    const LookupTitle = struct { title: []const u8 };
    var parsed_lookup = try parseJsonTestBody(LookupTitle, alloc, lookup.json);
    defer parsed_lookup.deinit();
    try std.testing.expectEqualStrings("alpha", parsed_lookup.value.title);

    var response = (try source.source().query(alloc, "docs", .{
        .query = .{ .match = .{ .field = "body", .text = "hello" } },
        .limit = 5,
    }, .read_index)).?;
    defer response.deinit(alloc);
    var parsed = try parseJsonTestBody(metadata_openapi.QueryResponses, alloc, response.json);
    defer parsed.deinit();
    try std.testing.expectEqualStrings("doc:a", parsed.value.responses.?[0].hits.?.hits.?[0]._id);
    try std.testing.expectEqual(@as(usize, 2), requester.count);
}

test "distributed table reads reject resolved doc filters" {
    var sentinel: u8 = 0;
    var req: db_mod.types.SearchRequest = .{
        .resolved_doc_filter = &sentinel,
    };

    try std.testing.expectError(error.UnsupportedQueryRequest, rejectCrossGroupResolvedDocFilter(req, 2));
    try rejectCrossGroupResolvedDocFilter(req, 1);
    try rejectRemoteRouteResolvedDocFilter(req, .local);
    var remote_uri_buf = [_]u8{'h'};
    try std.testing.expectError(error.UnsupportedQueryRequest, rejectRemoteRouteResolvedDocFilter(req, .{ .remote = .{ .node_id = 2, .base_uri = remote_uri_buf[0..] } }));
    req.resolved_doc_filter = null;
    try rejectCrossGroupResolvedDocFilter(req, 2);
    try rejectRemoteRouteResolvedDocFilter(req, .{ .remote = .{ .node_id = 2, .base_uri = remote_uri_buf[0..] } });
}

test "distributed table reads reject stale doc identity before multigroup fanout" {
    const alloc = std.testing.allocator;

    const FakeCatalog = struct {
        statuses: []const metadata_reconciler.MergedGroupStatus,

        fn iface(self: *@This()) table_catalog.CatalogSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = "doc:m" },
                    .{ .group_id = 7002, .table_id = 7, .start_key = "doc:m", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
                .merged_group_statuses = @constCast(self.statuses),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const healthy_statuses = [_]metadata_reconciler.MergedGroupStatus{
        .{ .group_id = 7001, .doc_identity = .{ .namespace_table_id = 7, .namespace_shard_id = 7001, .namespace_range_id = 7001, .allocated_ordinals = 1 } },
        .{ .group_id = 7002, .doc_identity = .{ .namespace_table_id = 7, .namespace_shard_id = 7002, .namespace_range_id = 7002, .allocated_ordinals = 1 } },
    };
    var healthy_catalog = FakeCatalog{ .statuses = healthy_statuses[0..] };
    try testing.validateDocIdentityReadyForMultiGroupRead(alloc, healthy_catalog.iface(), "docs", 2);
    try testing.validateDocIdentityReadyForMultiGroupRead(alloc, healthy_catalog.iface(), "docs", 1);

    var healthy_source = ProvisionedTableReadSource.init("/tmp/unused-antfly-docid-helper-guard", healthy_catalog.iface(), raft_mod.read_gate.noopReadableLeaseRequester());
    const group_ids = [_]u64{ 7001, 7002 };
    var sentinel: u8 = 0;
    const resolved_explicit_req = db_mod.types.SearchRequest{
        .aggregations_json = "{\"sig_body\":{\"type\":\"significant_terms\",\"field\":\"body\"}}",
        .resolved_doc_filter = &sentinel,
    };
    const resolved_background_req = db_mod.types.SearchRequest{
        .aggregations_json = "{\"sig_body\":{\"type\":\"significant_terms\",\"field\":\"body\",\"background_filter\":{\"match_all\":{}}}}",
        .resolved_doc_filter = &sentinel,
    };
    const stats_hits = try alloc.alloc(db_mod.types.SearchHit, 1);
    defer {
        for (stats_hits) |*hit| hit.deinit(alloc);
        alloc.free(stats_hits);
    }
    stats_hits[0] = .{
        .id = try alloc.dupe(u8, "doc:a"),
        .stored_data = try alloc.dupe(u8, "{\"body\":\"alpha beta\"}"),
    };
    try std.testing.expect((try collectProvisionedAlgebraicDistributedPartials(&healthy_source, alloc, group_ids[0..], "docs", resolved_explicit_req, "alg", &.{}, .{
        .output = .{ .input = 0 },
    })) == null);
    try std.testing.expectError(error.UnsupportedQueryRequest, collectProvisionedAggregationTextStats(&healthy_source, alloc, group_ids[0..], "docs", resolved_explicit_req, stats_hits));
    try std.testing.expectError(error.UnsupportedQueryRequest, collectProvisionedAggregationBackgroundTextStats(&healthy_source, alloc, group_ids[0..], "docs", resolved_background_req, stats_hits));

    const rebuild_required = [_]metadata_reconciler.MergedGroupStatus{
        .{ .group_id = 7001, .doc_identity = .{ .namespace_table_id = 7, .namespace_shard_id = 7001, .namespace_range_id = 7001, .allocated_ordinals = 1 } },
        .{ .group_id = 7002, .doc_identity = .{ .rebuild_required = true } },
    };
    var rebuild_catalog = FakeCatalog{ .statuses = rebuild_required[0..] };
    try std.testing.expectError(error.DocIdentityNamespaceMismatch, testing.validateDocIdentityReadyForMultiGroupRead(alloc, rebuild_catalog.iface(), "docs", 2));

    var source = ProvisionedTableReadSource.init("/tmp/unused-antfly-docid-helper-guard", rebuild_catalog.iface(), raft_mod.read_gate.noopReadableLeaseRequester());
    try std.testing.expectError(error.DocIdentityNamespaceMismatch, collectProvisionedSearchRequestTextStats(&source, alloc, group_ids[0..], .{
        .full_text = .{ .match = .{ .field = "body", .text = "hello" } },
    }, "docs"));
    try std.testing.expectError(error.DocIdentityNamespaceMismatch, collectProvisionedAggregationTextStats(&source, alloc, group_ids[0..], "docs", .{
        .aggregations_json = "unparsed because doc identity guard runs first",
    }, &.{}));
    try std.testing.expectError(error.DocIdentityNamespaceMismatch, collectProvisionedAggregationBackgroundTextStats(&source, alloc, group_ids[0..], "docs", .{
        .aggregations_json = "unparsed because doc identity guard runs first",
    }, &.{}));
    try std.testing.expectError(error.DocIdentityNamespaceMismatch, collectProvisionedAlgebraicDistributedPartials(&source, alloc, group_ids[0..], "docs", .{}, "alg", &.{}, .{
        .output = .{ .input = 0 },
    }));
}

test "simple vector shard request lowers to vector worker envelope" {
    const alloc = std.testing.allocator;
    const body = (try encodeAlgebraicVectorWorkerRequestForSearchRequestAlloc(alloc, .{
        .index_name = "dense_idx",
        .limit = 11,
        .offset = 3,
        .count_only = true,
        .profile = true,
        .include_stored = false,
        .fields = &.{ "title", "score" },
        .filter_query_json = "{\"term\":{\"path\":\"/tenant\",\"value\":\"t1\"}}",
        .exclusion_query_json = "{\"term\":{\"path\":\"/deleted\",\"value\":true}}",
        .filter_prefix = "tenant/a/",
        .filter_ids = &.{ 99, 42 },
        .exclude_ids = &.{7},
        .include_all_fields = false,
        .defer_stored_projection = true,
        .search_effort = 0.5,
        .distance_under = 0.9,
        .return_mode = .parent_with_chunks,
        .max_chunks_per_parent = 2,
        .identity_read_generation = 54321,
        .query = .{ .dense_knn = .{ .vector = &.{ 0.25, 0.5 }, .k = 7 } },
        .filter_doc_ids_positive = true,
        .filter_doc_ids = &.{ "doc:b", "doc:a" },
        .exclude_doc_ids = &.{"doc:c"},
    })).?;
    defer alloc.free(body);

    var envelope = try query_contract.parseAlgebraicVectorWorkerRequestEnvelopeAlloc(alloc, body);
    defer envelope.deinit(alloc);
    try std.testing.expectEqualStrings("dense_idx", envelope.index_name);
    try std.testing.expectEqual(algebraic_ir.PhysicalLayout.dense_vector, envelope.layout);
    try std.testing.expectEqual(@as(u32, 11), envelope.options.limit);
    try std.testing.expectEqual(@as(u32, 3), envelope.options.offset);
    try std.testing.expect(envelope.options.count_only);
    try std.testing.expect(envelope.options.profile);
    try std.testing.expect(!envelope.options.include_stored);
    try std.testing.expect(!envelope.options.include_all_fields);
    try std.testing.expect(envelope.options.defer_stored_projection);
    try std.testing.expectEqualStrings("{\"term\":{\"path\":\"/tenant\",\"value\":\"t1\"}}", envelope.options.filter_query_json);
    try std.testing.expectEqualStrings("{\"term\":{\"path\":\"/deleted\",\"value\":true}}", envelope.options.exclusion_query_json);
    try std.testing.expect(envelope.options.require_algebraic_filter_resolution);
    try std.testing.expectEqualStrings("tenant/a/", envelope.options.filter_prefix);
    try std.testing.expectEqual(@as(usize, 2), envelope.options.filter_ids.len);
    try std.testing.expectEqual(@as(u64, 99), envelope.options.filter_ids[0]);
    try std.testing.expectEqual(@as(u64, 42), envelope.options.filter_ids[1]);
    try std.testing.expectEqual(@as(usize, 1), envelope.options.exclude_ids.len);
    try std.testing.expectEqual(@as(u64, 7), envelope.options.exclude_ids[0]);
    try std.testing.expectEqual(@as(usize, 2), envelope.options.fields.len);
    try std.testing.expectEqualStrings("title", envelope.options.fields[0]);
    try std.testing.expectEqualStrings("score", envelope.options.fields[1]);
    try std.testing.expectApproxEqAbs(@as(f32, 0.5), envelope.options.search_effort.?, 0.0001);
    try std.testing.expectApproxEqAbs(@as(f32, 0.9), envelope.options.distance_under.?, 0.0001);
    try std.testing.expectEqual(db_mod.types.ReturnMode.parent_with_chunks, envelope.options.return_mode);
    try std.testing.expectEqual(@as(u32, 2), envelope.options.max_chunks_per_parent);
    try std.testing.expectEqual(@as(?u64, 54321), envelope.options.identity_read_generation);
    try std.testing.expect(envelope.native_doc_id_constraints.constraints.positive_filter);
    try std.testing.expectEqualStrings("doc:a", envelope.native_doc_id_constraints.constraints.include_doc_ids[0]);
    try std.testing.expectEqualStrings("doc:b", envelope.native_doc_id_constraints.constraints.include_doc_ids[1]);
    try std.testing.expectEqualStrings("doc:c", envelope.native_doc_id_constraints.constraints.exclude_doc_ids[0]);
    try std.testing.expect((try envelope.proveTensorProgramAlloc(alloc)).safe());

    const supported_filter = try encodeAlgebraicVectorWorkerRequestForSearchRequestAlloc(alloc, .{
        .index_name = "dense_idx",
        .limit = 7,
        .query = .{ .dense_knn = .{ .vector = &.{ 0.25, 0.5 }, .k = 7 } },
        .filter_query_json = "{\"term\":{\"path\":\"/tenant\",\"value\":\"t1\"}}",
        .exclusion_query_json = "{\"term\":{\"path\":\"/deleted\",\"value\":true}}",
    });
    try std.testing.expect(supported_filter != null);
    if (supported_filter) |body_supported| alloc.free(body_supported);

    const unsupported = try encodeAlgebraicVectorWorkerRequestForSearchRequestAlloc(alloc, .{
        .index_name = "dense_idx",
        .limit = 7,
        .query = .{ .dense_knn = .{ .vector = &.{ 0.25, 0.5 }, .k = 7 } },
        .filter_query_json = "{\"wildcard\":{\"/tenant\":\"*ice\"}}",
    });
    try std.testing.expect(unsupported == null);
}

test "vector worker preflight annotation tracks eligibility and symbolic filters" {
    const alloc = std.testing.allocator;

    var supported: db_mod.RuntimePreflightSummary = .{};
    defer supported.deinit(alloc);
    annotateVectorWorkerPreflight(alloc, &supported, .{
        .index_name = "dense_idx",
        .query = .{ .dense_knn = .{ .vector = &.{ 0.25, 0.5 }, .k = 7 } },
        .filter_query_json = "{\"term\":{\"path\":\"/tenant\",\"value\":\"t1\"}}",
        .exclusion_query_json = "{\"term\":{\"path\":\"/deleted\",\"value\":true}}",
        .filter_ids = &.{42},
        .exclude_ids = &.{7},
        .filter_doc_ids_positive = true,
        .filter_doc_ids = &.{"doc:a"},
        .exclude_doc_ids = &.{"doc:b"},
    });
    try std.testing.expectEqual(@as(u32, 1), supported.vector_worker_candidate_count);
    try std.testing.expectEqual(@as(u32, 0), supported.vector_worker_fallback_count);
    try std.testing.expectEqual(@as(u32, 6), supported.vector_worker_filter_constraint_count);
    try std.testing.expect(supported.vector_worker_requires_algebraic_filter_resolution);

    var unsupported: db_mod.RuntimePreflightSummary = .{};
    defer unsupported.deinit(alloc);
    annotateVectorWorkerPreflight(alloc, &unsupported, .{
        .index_name = "dense_idx",
        .query = .{ .dense_knn = .{ .vector = &.{ 0.25, 0.5 }, .k = 7 } },
        .filter_query_json = "{\"wildcard\":{\"/tenant\":\"*ice\"}}",
    });
    try std.testing.expectEqual(@as(u32, 0), unsupported.vector_worker_candidate_count);
    try std.testing.expectEqual(@as(u32, 1), unsupported.vector_worker_fallback_count);
    try std.testing.expectEqual(@as(u32, 1), unsupported.vector_worker_filter_constraint_count);
    try std.testing.expect(unsupported.vector_worker_requires_algebraic_filter_resolution);

    var sentinel: u8 = 0;
    var resolved_filter: db_mod.RuntimePreflightSummary = .{};
    defer resolved_filter.deinit(alloc);
    annotateVectorWorkerPreflight(alloc, &resolved_filter, .{
        .index_name = "dense_idx",
        .query = .{ .dense_knn = .{ .vector = &.{ 0.25, 0.5 }, .k = 7 } },
        .resolved_doc_filter = &sentinel,
    });
    try std.testing.expectEqual(@as(u32, 0), resolved_filter.vector_worker_candidate_count);
    try std.testing.expectEqual(@as(u32, 1), resolved_filter.vector_worker_fallback_count);
    try std.testing.expectEqual(@as(u32, 1), resolved_filter.vector_worker_filter_constraint_count);

    var non_vector: db_mod.RuntimePreflightSummary = .{};
    defer non_vector.deinit(alloc);
    annotateVectorWorkerPreflight(alloc, &non_vector, .{ .query = .{ .match_all = {} } });
    try std.testing.expectEqual(@as(u32, 0), non_vector.vector_worker_candidate_count);
    try std.testing.expectEqual(@as(u32, 0), non_vector.vector_worker_fallback_count);
}

test "remote simple vector query uses vector worker route" {
    const alloc = std.testing.allocator;

    const ExecutorState = struct {
        vector_worker_calls: usize = 0,
        query_calls: usize = 0,

        fn iface(self: *@This()) http_common.RequestExecutor {
            return .{
                .ptr = self,
                .vtable = &.{ .execute = execute },
            };
        }

        fn execute(ptr: *anyopaque, alloc_inner: std.mem.Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(http_common.Method.POST, req.method);
            if (std.mem.endsWith(u8, req.uri, "/internal/v1/groups/11/tables/docs/vector-worker")) {
                self.vector_worker_calls += 1;
                var envelope = try query_contract.parseAlgebraicVectorWorkerRequestEnvelopeAlloc(alloc_inner, req.body);
                defer envelope.deinit(alloc_inner);
                try std.testing.expectEqualStrings("dense_idx", envelope.index_name);
                try std.testing.expectEqual(@as(u32, 11), envelope.options.limit);
                try std.testing.expectEqual(@as(u32, 3), envelope.options.offset);
                try std.testing.expect(envelope.options.profile);
                try std.testing.expectEqual(@as(?u64, 77), envelope.options.identity_read_generation);
                try std.testing.expect(!envelope.options.include_all_fields);
                try std.testing.expect(envelope.options.defer_stored_projection);
                try std.testing.expectEqualStrings("{\"term\":{\"path\":\"/tenant\",\"value\":\"t1\"}}", envelope.options.filter_query_json);
                try std.testing.expectEqualStrings("{\"term\":{\"path\":\"/deleted\",\"value\":true}}", envelope.options.exclusion_query_json);
                try std.testing.expect(envelope.options.require_algebraic_filter_resolution);
                try std.testing.expectEqualStrings("tenant/a/", envelope.options.filter_prefix);
                try std.testing.expectEqual(@as(usize, 1), envelope.options.filter_ids.len);
                try std.testing.expectEqual(@as(u64, 42), envelope.options.filter_ids[0]);
                try std.testing.expectEqual(@as(usize, 1), envelope.options.exclude_ids.len);
                try std.testing.expectEqual(@as(u64, 7), envelope.options.exclude_ids[0]);
                try std.testing.expectEqual(@as(usize, 1), envelope.options.fields.len);
                try std.testing.expectEqualStrings("title", envelope.options.fields[0]);
                try std.testing.expectApproxEqAbs(@as(f32, 0.5), envelope.options.search_effort.?, 0.0001);
                try std.testing.expectApproxEqAbs(@as(f32, 0.9), envelope.options.distance_under.?, 0.0001);
                try std.testing.expectEqual(db_mod.types.ReturnMode.parent_with_chunks, envelope.options.return_mode);
                try std.testing.expectEqual(@as(u32, 2), envelope.options.max_chunks_per_parent);
                try std.testing.expect(envelope.native_doc_id_constraints.constraints.positive_filter);
            } else if (std.mem.endsWith(u8, req.uri, "/internal/v1/groups/11/tables/docs/query")) {
                self.query_calls += 1;
                try std.testing.expect(std.mem.indexOf(u8, req.body, "\"_filter_query_json\"") != null);
                try std.testing.expect(std.mem.indexOf(u8, req.body, "\"_identity_read_generation\":88") != null);
                try std.testing.expect(std.mem.indexOf(u8, req.body, "\"filter_query\"") == null);
            } else {
                return error.UnexpectedHttpRequest;
            }
            return .{
                .status = 200,
                .body = try alloc_inner.dupe(u8, "{\"responses\":[{\"hits\":{\"total\":0,\"hits\":[]},\"took\":0,\"status\":200,\"table\":\"docs\"}]}"),
            };
        }
    };

    var state = ExecutorState{};
    var vector_result = try queryRemote(state.iface(), alloc, "http://remote.test", 11, "docs", .{
        .index_name = "dense_idx",
        .limit = 11,
        .offset = 3,
        .profile = true,
        .fields = &.{"title"},
        .filter_query_json = "{\"term\":{\"path\":\"/tenant\",\"value\":\"t1\"}}",
        .exclusion_query_json = "{\"term\":{\"path\":\"/deleted\",\"value\":true}}",
        .filter_prefix = "tenant/a/",
        .filter_ids = &.{42},
        .exclude_ids = &.{7},
        .include_all_fields = false,
        .defer_stored_projection = true,
        .search_effort = 0.5,
        .distance_under = 0.9,
        .return_mode = .parent_with_chunks,
        .max_chunks_per_parent = 2,
        .identity_read_generation = 77,
        .query = .{ .dense_knn = .{ .vector = &.{ 0.25, 0.5 }, .k = 7 } },
        .filter_doc_ids_positive = true,
        .filter_doc_ids = &.{"doc:a"},
    });
    defer vector_result.deinit();
    try std.testing.expectEqual(@as(usize, 1), state.vector_worker_calls);
    try std.testing.expectEqual(@as(usize, 0), state.query_calls);
    try std.testing.expectEqual(@as(?u64, 77), vector_result.identity_read_generation);

    var fallback_result = try queryRemote(state.iface(), alloc, "http://remote.test", 11, "docs", .{
        .index_name = "dense_idx",
        .query = .{ .dense_knn = .{ .vector = &.{ 0.25, 0.5 }, .k = 7 } },
        .filter_query_json = "{\"wildcard\":{\"/tenant\":\"*ice\"}}",
        .identity_read_generation = 88,
    });
    defer fallback_result.deinit();
    try std.testing.expectEqual(@as(usize, 1), state.vector_worker_calls);
    try std.testing.expectEqual(@as(usize, 1), state.query_calls);
    try std.testing.expectEqual(@as(?u64, 88), fallback_result.identity_read_generation);
}

test "remote query rejects resolved doc filters before vector worker encoding" {
    const alloc = std.testing.allocator;
    var filter = doc_set.ResolvedDocFilter{ .include = try doc_set.fromOrdinalsAlloc(alloc, &.{ 1, 2 }) };
    defer filter.deinit(alloc);

    const ExecutorState = struct {
        calls: usize = 0,

        fn iface(self: *@This()) http_common.RequestExecutor {
            return .{
                .ptr = self,
                .vtable = &.{ .execute = execute },
            };
        }

        fn execute(ptr: *anyopaque, _: std.mem.Allocator, _: http_common.HttpRequest) !http_common.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            return error.UnexpectedHttpRequest;
        }
    };

    var state = ExecutorState{};
    try std.testing.expect((try encodeAlgebraicVectorWorkerRequestForSearchRequestAlloc(alloc, .{
        .index_name = "dense_idx",
        .query = .{ .dense_knn = .{ .vector = &.{ 0.25, 0.5 }, .k = 7 } },
        .resolved_doc_filter = &filter,
    })) == null);
    try std.testing.expectError(error.UnsupportedQueryRequest, queryRemote(state.iface(), alloc, "http://remote.test", 11, "docs", .{
        .index_name = "dense_idx",
        .query = .{ .dense_knn = .{ .vector = &.{ 0.25, 0.5 }, .k = 7 } },
        .resolved_doc_filter = &filter,
    }));
    try std.testing.expectEqual(@as(usize, 0), state.calls);
}

test "remote preflight rejects resolved doc filters before query encoding" {
    const alloc = std.testing.allocator;
    var filter = doc_set.ResolvedDocFilter{ .include = try doc_set.fromOrdinalsAlloc(alloc, &.{ 1, 2 }) };
    defer filter.deinit(alloc);

    const ExecutorState = struct {
        calls: usize = 0,

        fn iface(self: *@This()) http_common.RequestExecutor {
            return .{
                .ptr = self,
                .vtable = &.{ .execute = execute },
            };
        }

        fn execute(ptr: *anyopaque, _: std.mem.Allocator, _: http_common.HttpRequest) !http_common.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            return error.UnexpectedHttpRequest;
        }
    };

    var state = ExecutorState{};
    try std.testing.expectError(error.UnsupportedQueryRequest, preflightRemote(state.iface(), alloc, "http://remote.test", 11, "docs", .{
        .index_name = "dense_idx",
        .query = .{ .dense_knn = .{ .vector = &.{ 0.25, 0.5 }, .k = 7 } },
        .resolved_doc_filter = &filter,
    }, 0));
    try std.testing.expectEqual(@as(usize, 0), state.calls);
}

test "encode query request serializes internal resolved doc filters with wire context" {
    const alloc = std.testing.allocator;
    var filter = doc_set.ResolvedDocFilter{ .include = try doc_set.fromOrdinalsAlloc(alloc, &.{ 1, 3 }) };
    defer filter.deinit(alloc);

    const encoded = try encodeQueryRequest(alloc, .{
        .query = .{ .match_all = {} },
        .identity_read_generation = 42,
        .resolved_doc_filter = &filter,
        .resolved_doc_filter_wire_context = .{
            .namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 3 },
            .identity_read_generation = 42,
        },
    });
    defer alloc.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"_resolved_doc_filter\"") != null);

    var parsed = try query_contract.parseQueryRequest(alloc, null, "docs", encoded);
    defer parsed.deinit(alloc);
    try std.testing.expect(parsed.req.resolved_doc_filter != null);
    try std.testing.expectEqual(@as(?u64, 42), parsed.req.identity_read_generation);
    try std.testing.expect(parsed.req.resolved_doc_filter_wire_context.?.namespace.eql(.{ .table_id = 1, .shard_id = 2, .range_id = 3 }));

    const stats_body = try encodeQueryTextStatsRequest(alloc, .{
        .query = .{ .match = .{ .field = "body", .text = "hello" } },
        .identity_read_generation = 42,
        .resolved_doc_filter = &filter,
        .resolved_doc_filter_wire_context = .{
            .namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 3 },
            .identity_read_generation = 42,
        },
    });
    defer alloc.free(stats_body);
    try std.testing.expect(std.mem.indexOf(u8, stats_body, "\"_resolved_doc_filter\"") != null);

    var stats_parsed = try parseTextStatsRequest(alloc, "docs", stats_body);
    defer stats_parsed.deinit(alloc);
    const stats_query = stats_parsed.query_request.req;
    try std.testing.expect(stats_query.resolved_doc_filter != null);
    try std.testing.expectEqual(@as(?u64, 42), stats_query.identity_read_generation);
    try std.testing.expect(stats_query.resolved_doc_filter_wire_context.?.namespace.eql(.{ .table_id = 1, .shard_id = 2, .range_id = 3 }));
}

test "simple vector shard request carries serializable resolved doc filter" {
    const alloc = std.testing.allocator;
    var filter = doc_set.ResolvedDocFilter{ .include = try doc_set.fromOrdinalsAlloc(alloc, &.{ 1, 3 }) };
    defer filter.deinit(alloc);

    const body = (try encodeAlgebraicVectorWorkerRequestForSearchRequestAlloc(alloc, .{
        .index_name = "dense_idx",
        .identity_read_generation = 42,
        .query = .{ .dense_knn = .{ .vector = &.{ 0.25, 0.5 }, .k = 7 } },
        .resolved_doc_filter = &filter,
        .resolved_doc_filter_wire_context = .{
            .namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 3 },
            .identity_read_generation = 42,
        },
    })) orelse return error.TestUnexpectedResult;
    defer alloc.free(body);
    try std.testing.expect(std.mem.indexOf(u8, body, "\"_resolved_doc_filter\"") != null);

    var envelope = try query_contract.parseAlgebraicVectorWorkerRequestEnvelopeAlloc(alloc, body);
    defer envelope.deinit(alloc);
    const req = searchRequestFromVectorWorkerEnvelope(&envelope);
    try std.testing.expect(req.resolved_doc_filter != null);
    try std.testing.expectEqual(@as(?u64, 42), req.identity_read_generation);
    try std.testing.expect(req.resolved_doc_filter_wire_context.?.namespace.eql(.{ .table_id = 1, .shard_id = 2, .range_id = 3 }));
}

test "explicit text stats requests preserve identity generation" {
    const alloc = std.testing.allocator;

    var terms = [_][]const u8{"alpha"};
    const explicit_items = [_]OwnedTextStatsFieldRequest{.{
        .index_name = "text_v1",
        .field = "body",
        .terms = terms[0..],
    }};
    const explicit_body = try encodeExplicitTextStatsRequest(alloc, explicit_items[0..], 42);
    defer alloc.free(explicit_body);
    try std.testing.expect(std.mem.indexOf(u8, explicit_body, "\"_identity_read_generation\":42") != null);

    var parsed_explicit = try parseTextStatsRequest(alloc, "docs", explicit_body);
    defer parsed_explicit.deinit(alloc);
    switch (parsed_explicit) {
        .explicit_fields => |parsed| {
            try std.testing.expectEqual(@as(?u64, 42), parsed.identity_read_generation);
            try std.testing.expectEqual(@as(usize, 1), parsed.items.len);
        },
        else => return error.TestUnexpectedResult,
    }

    var bg_terms = [_][]const u8{"beta"};
    const background_items = [_]OwnedBackgroundTextStatsFieldRequest{.{
        .aggregation_name = "sig",
        .index_name = "text_v1",
        .field = "body",
        .terms = bg_terms[0..],
        .background_query = .match_all,
    }};
    const background_body = try encodeBackgroundTextStatsRequest(alloc, background_items[0..], 43);
    defer alloc.free(background_body);
    try std.testing.expect(std.mem.indexOf(u8, background_body, "\"_identity_read_generation\":43") != null);

    var parsed_background = try parseTextStatsRequest(alloc, "docs", background_body);
    defer parsed_background.deinit(alloc);
    switch (parsed_background) {
        .background_fields => |parsed| {
            try std.testing.expectEqual(@as(?u64, 43), parsed.identity_read_generation);
            try std.testing.expectEqual(@as(usize, 1), parsed.items.len);
        },
        else => return error.TestUnexpectedResult,
    }

    const envelope_only_explicit =
        \\{
        \\  "_resolved_doc_filter": {
        \\    "namespace": {"table_id": 1, "shard_id": 2, "range_id": 3},
        \\    "identity_read_generation": 44,
        \\    "include": {"kind": "ordinals", "values": [1]},
        \\    "exclude": {"kind": "none"}
        \\  },
        \\  "fields": [{"index_name":"text_v1","field":"body","terms":["alpha"]}]
        \\}
    ;
    var parsed_envelope_only_explicit = try parseTextStatsRequest(alloc, "docs", envelope_only_explicit);
    defer parsed_envelope_only_explicit.deinit(alloc);
    switch (parsed_envelope_only_explicit) {
        .explicit_fields => |parsed| {
            try std.testing.expectEqual(@as(?u64, 44), parsed.identity_read_generation);
            try std.testing.expect(parsed.resolved_doc_filter != null);
        },
        else => return error.TestUnexpectedResult,
    }

    const envelope_only_background =
        \\{
        \\  "_resolved_doc_filter": {
        \\    "namespace": {"table_id": 1, "shard_id": 2, "range_id": 3},
        \\    "identity_read_generation": 45,
        \\    "include": {"kind": "ordinals", "values": [1]},
        \\    "exclude": {"kind": "none"}
        \\  },
        \\  "background_fields": [{"aggregation_name":"sig","index_name":"text_v1","field":"body","terms":["alpha"],"background_query":{"match_all":{}}}]
        \\}
    ;
    var parsed_envelope_only_background = try parseTextStatsRequest(alloc, "docs", envelope_only_background);
    defer parsed_envelope_only_background.deinit(alloc);
    switch (parsed_envelope_only_background) {
        .background_fields => |parsed| {
            try std.testing.expectEqual(@as(?u64, 45), parsed.identity_read_generation);
            try std.testing.expect(parsed.resolved_doc_filter != null);
        },
        else => return error.TestUnexpectedResult,
    }

    const mismatched_envelope_generation =
        \\{
        \\  "_identity_read_generation": 46,
        \\  "_resolved_doc_filter": {
        \\    "namespace": {"table_id": 1, "shard_id": 2, "range_id": 3},
        \\    "identity_read_generation": 47,
        \\    "include": {"kind": "ordinals", "values": [1]},
        \\    "exclude": {"kind": "none"}
        \\  },
        \\  "fields": [{"index_name":"text_v1","field":"body","terms":["alpha"]}]
        \\}
    ;
    try std.testing.expectError(error.InvalidQueryRequest, parseTextStatsRequest(alloc, "docs", mismatched_envelope_generation));
}

test "explicit text stats requests carry resolved doc filters and apply exact projection" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-text-stats-resolved-doc-filter";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{ .start_index_workers = false });
    defer db.close();

    try db.addIndex(.{ .name = "full_text_index_v0", .kind = .full_text, .config_json = "{}" });
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"body\":\"alpha beta\"}" },
            .{ .key = "doc:b", .value = "{\"body\":\"beta gamma\"}" },
        },
        .sync_level = .full_index,
        .timestamp_ns = 1,
    });

    const generation = try db.currentIdentityReadGenerationForRequest(null);
    var filter = doc_set.ResolvedDocFilter{
        .include = try doc_set.fromOrdinalsAlloc(alloc, &.{1}),
    };
    defer filter.deinit(alloc);
    const req = db_mod.types.SearchRequest{
        .identity_read_generation = generation,
        .resolved_doc_filter = &filter,
        .resolved_doc_filter_wire_context = .{
            .namespace = db.core.identity_namespace,
            .identity_read_generation = generation,
        },
    };

    var terms = [_][]const u8{ "alpha", "beta", "gamma" };
    const explicit_items = [_]OwnedTextStatsFieldRequest{.{
        .index_name = "full_text_index_v0",
        .field = "body",
        .terms = terms[0..],
    }};
    const explicit_body = try encodeExplicitTextStatsRequestForSearchRequest(alloc, explicit_items[0..], req);
    defer alloc.free(explicit_body);
    try std.testing.expect(std.mem.indexOf(u8, explicit_body, "\"_resolved_doc_filter\"") != null);

    var parsed_explicit = try parseTextStatsRequest(alloc, "docs", explicit_body);
    defer parsed_explicit.deinit(alloc);
    const stats = try collectTextStatsFromDbForRequest(alloc, &db, parsed_explicit);
    defer distributed_stats_mod.deinitTextFieldStats(alloc, stats);
    try std.testing.expectEqual(@as(usize, 1), stats.len);
    try std.testing.expectEqual(@as(u32, 1), stats[0].global_doc_count);
    try std.testing.expectEqual(@as(u32, 1), stats[0].termDocFreq("alpha").?);
    try std.testing.expectEqual(@as(u32, 1), stats[0].termDocFreq("beta").?);
    try std.testing.expectEqual(@as(u32, 0), stats[0].termDocFreq("gamma").?);

    const background_items = [_]OwnedBackgroundTextStatsFieldRequest{.{
        .aggregation_name = "sig_body",
        .index_name = "full_text_index_v0",
        .field = "body",
        .terms = terms[0..],
        .background_query = .match_all,
    }};
    const background_body = try encodeBackgroundTextStatsRequestForSearchRequest(alloc, background_items[0..], req);
    defer alloc.free(background_body);
    try std.testing.expect(std.mem.indexOf(u8, background_body, "\"_resolved_doc_filter\"") != null);

    var parsed_background = try parseTextStatsRequest(alloc, "docs", background_body);
    defer parsed_background.deinit(alloc);
    const background_stats = try collectBackgroundTextStatsFromDbForRequest(alloc, &db, parsed_background);
    defer db_mod.aggregations.deinitDistributedBackgroundTextStats(alloc, background_stats);
    try std.testing.expectEqual(@as(usize, 1), background_stats.len);
    try std.testing.expectEqual(@as(u32, 1), background_stats[0].background_doc_count);
    const alpha_bg = for (background_stats[0].term_doc_freqs) |item| {
        if (std.mem.eql(u8, item.term, "alpha")) break item.doc_freq;
    } else return error.TestUnexpectedResult;
    const beta_bg = for (background_stats[0].term_doc_freqs) |item| {
        if (std.mem.eql(u8, item.term, "beta")) break item.doc_freq;
    } else return error.TestUnexpectedResult;
    const gamma_bg = for (background_stats[0].term_doc_freqs) |item| {
        if (std.mem.eql(u8, item.term, "gamma")) break item.doc_freq;
    } else return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(u32, 1), alpha_bg);
    try std.testing.expectEqual(@as(u32, 1), beta_bg);
    try std.testing.expectEqual(@as(u32, 0), gamma_bg);
}

test "explicit text stats requests reject stale identity generation" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-text-stats-stale-identity-generation";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{ .start_index_workers = false });
    defer db.close();

    const future_generation = db.core.nextDerivedSequence() + 1;
    const explicit_body = try std.fmt.allocPrint(alloc,
        \\{{"_identity_read_generation":{d},"fields":[{{"index_name":"text_v1","field":"body","terms":["alpha"]}}]}}
    , .{future_generation});
    defer alloc.free(explicit_body);

    var parsed_explicit = try parseTextStatsRequest(alloc, "docs", explicit_body);
    defer parsed_explicit.deinit(alloc);
    try std.testing.expectError(error.UnsupportedQueryRequest, collectTextStatsFromDbForRequest(alloc, &db, parsed_explicit));

    const background_body = try std.fmt.allocPrint(alloc,
        \\{{"_identity_read_generation":{d},"background_fields":[{{"aggregation_name":"sig","index_name":"text_v1","field":"body","terms":["alpha"],"background_query":{{"match_all":{{}}}}}}]}}
    , .{future_generation});
    defer alloc.free(background_body);

    var parsed_background = try parseTextStatsRequest(alloc, "docs", background_body);
    defer parsed_background.deinit(alloc);
    try std.testing.expectError(error.UnsupportedQueryRequest, collectBackgroundTextStatsFromDbForRequest(alloc, &db, parsed_background));
}

test "provisioned distributed aggregations collect path terms nested cardinality" {
    const alloc = std.testing.allocator;
    const path = try std.fmt.allocPrint(alloc, "/tmp/antfly-api-provisioned-algebraic-path-terms-cardinality-{d}", .{platform_time.monotonicNs()});
    defer alloc.free(path);

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const cfg =
        \\{
        \\  "version": 1,
        \\  "schema_version": 1,
        \\  "table": "docs",
        \\  "group_fields": [{"name":"product","path":"product","type":"string"}],
        \\  "materializations": []
        \\}
    ;

    const left_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(left_path);
    var left_db = try db_mod.DB.open(alloc, left_path, .{
        .start_index_workers = false,
        .identity_namespace = .{
            .table_id = 7,
            .shard_id = 7001,
            .range_id = 7001,
        },
    });
    defer left_db.close();
    try left_db.addIndex(.{ .name = "alg", .kind = .algebraic, .config_json = cfg });
    try left_db.batch(.{
        .writes = &.{
            .{ .key = "l1", .value = "{\"product\":\"pen\",\"meta\":{\"tier\":\"gold\"}}" },
            .{ .key = "l2", .value = "{\"product\":\"book\",\"meta\":{\"tier\":\"gold\"}}" },
            .{ .key = "l3", .value = "{\"product\":\"pen\",\"meta\":{\"tier\":\"silver\"}}" },
        },
        .sync_level = .full_index,
    });

    const right_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7002);
    defer alloc.free(right_path);
    var right_db = try db_mod.DB.open(alloc, right_path, .{
        .start_index_workers = false,
        .identity_namespace = .{
            .table_id = 7,
            .shard_id = 7002,
            .range_id = 7002,
        },
    });
    defer right_db.close();
    try right_db.addIndex(.{ .name = "alg", .kind = .algebraic, .config_json = cfg });
    try right_db.batch(.{
        .writes = &.{
            .{ .key = "r1", .value = "{\"product\":\"pen\",\"meta\":{\"tier\":\"silver\"}}" },
            .{ .key = "r2", .value = "{\"product\":\"notebook\",\"meta\":{\"tier\":\"silver\"}}" },
        },
        .sync_level = .full_index,
    });

    const FakeCatalog = struct {
        const statuses = [_]metadata_reconciler.MergedGroupStatus{
            .{
                .group_id = 7001,
                .doc_identity = .{
                    .namespace_table_id = 7,
                    .namespace_shard_id = 7001,
                    .namespace_range_id = 7001,
                    .next_ordinal = 4,
                    .allocated_ordinals = 3,
                    .state_rows = 3,
                    .live_ordinals = 3,
                    .complete = true,
                },
            },
            .{
                .group_id = 7002,
                .doc_identity = .{
                    .namespace_table_id = 7,
                    .namespace_shard_id = 7002,
                    .namespace_range_id = 7002,
                    .next_ordinal = 3,
                    .allocated_ordinals = 2,
                    .state_rows = 2,
                    .live_ordinals = 2,
                    .complete = true,
                },
            },
        };

        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json =
                    \\{"alg":{"version":1,"table":"docs","schema_version":1,"group_fields":[{"name":"product","path":"product","type":"string"}],"materializations":[]}}
                    ,
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = "r" },
                    .{ .group_id = 7002, .table_id = 7, .start_key = "r", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
                .merged_group_statuses = @constCast(statuses[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = ProvisionedTableReadSource.init(path, FakeCatalog.iface(), raft_mod.read_gate.noopReadableLeaseRequester());
    var group_ids = [_]u64{ 7001, 7002 };
    var meta: query_api.QueryResponseMeta = .{};
    defer meta.deinit(alloc);
    const req = db_mod.types.SearchRequest{
        .index_name = "alg",
        .aggregations_json =
        \\{"by_tier":{"type":"terms","field":"/meta/tier","sub_aggregations":{"product_cardinality":{"type":"cardinality","field":"product"},"tier_cardinality":{"type":"cardinality","field":"/meta/tier"}}}}
        ,
    };
    try std.testing.expect(try tryApplyProvisionedAlgebraicDistributedAggregations(&source, alloc, group_ids[0..], "docs", req, &meta));
    var stamped_meta: query_api.QueryResponseMeta = .{};
    defer stamped_meta.deinit(alloc);
    const current_generation = left_db.core.nextDerivedSequence();
    try std.testing.expectEqual(current_generation, right_db.core.nextDerivedSequence());
    var stamped_req = req;
    stamped_req.identity_read_generation = current_generation;
    try std.testing.expect(try tryApplyProvisionedAlgebraicDistributedAggregations(&source, alloc, group_ids[0..], "docs", stamped_req, &stamped_meta));

    const FakeRouter = struct {
        fn iface() table_router.HostedGroupRouter {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .local_node_id = localNodeId,
                    .local_status = localStatus,
                    .group_leader_node_id = groupLeaderNodeId,
                    .node_status = nodeStatus,
                    .node_base_uri = nodeBaseUri,
                },
            };
        }

        fn localNodeId(_: *anyopaque) u64 {
            return 1;
        }

        fn localStatus(_: *anyopaque, _: u64) raft_mod.HostedReplicaStatus {
            return .active;
        }

        fn groupLeaderNodeId(_: *anyopaque, _: u64) ?u64 {
            return 1;
        }

        fn nodeStatus(_: *anyopaque, _: u64, _: u64) raft_mod.HostedReplicaStatus {
            return .absent;
        }

        fn nodeBaseUri(_: *anyopaque, _: std.mem.Allocator, _: u64) !?[]u8 {
            return null;
        }
    };

    const ExecutorState = struct {
        call_count: usize = 0,

        fn iface(self: *@This()) http_common.RequestExecutor {
            return .{
                .ptr = self,
                .vtable = &.{ .execute = execute },
            };
        }

        fn execute(ptr: *anyopaque, _: std.mem.Allocator, _: http_common.HttpRequest) !http_common.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.call_count += 1;
            return error.UnexpectedHttpRequest;
        }
    };

    var executor_state = ExecutorState{};
    var hosted = HostedProvisionedTableReadSource.init(
        path,
        FakeCatalog.iface(),
        raft_mod.read_gate.noopReadableLeaseRequester(),
        FakeRouter.iface(),
        executor_state.iface(),
    );
    var hosted_meta: query_api.QueryResponseMeta = .{};
    defer hosted_meta.deinit(alloc);
    try std.testing.expect(try tryApplyHostedAlgebraicDistributedAggregations(&hosted, alloc, group_ids[0..], "docs", stamped_req, &hosted_meta, .read_index));
    try std.testing.expectEqual(@as(usize, 0), executor_state.call_count);

    try std.testing.expectEqual(@as(usize, 1), meta.aggregation_results.len);
    const aggregation = meta.aggregation_results[0];
    try std.testing.expectEqualStrings("by_tier", aggregation.name);
    try std.testing.expectEqual(@as(usize, 2), aggregation.buckets.len);
    try std.testing.expectEqualStrings("\"silver\"", aggregation.buckets[0].key_json);
    try std.testing.expectEqual(@as(i64, 3), aggregation.buckets[0].count);
    try std.testing.expectEqualStrings("{\"value\":2,\"approximate\":false}", aggregation.buckets[0].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"value\":1,\"approximate\":false}", aggregation.buckets[0].aggregations[1].value_json.?);
    try std.testing.expectEqualStrings("\"gold\"", aggregation.buckets[1].key_json);
    try std.testing.expectEqual(@as(i64, 2), aggregation.buckets[1].count);
    try std.testing.expectEqualStrings("{\"value\":2,\"approximate\":false}", aggregation.buckets[1].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"value\":1,\"approximate\":false}", aggregation.buckets[1].aggregations[1].value_json.?);
}

test "algebraic partial request fails closed when lifecycle is stale" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-algebraic-partials-stale-lifecycle";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{ .start_index_workers = false });
    defer db.close();
    try db.addIndex(.{
        .name = "alg",
        .kind = .algebraic,
        .config_json =
        \\{
        \\  "version": 1,
        \\  "schema_version": 1,
        \\  "table": "orders",
        \\  "capability_lifecycle_status": "rebuild_required",
        \\  "group_fields": [{"name":"customer","path":"customer","type":"keyword"}],
        \\  "measure_fields": [{"name":"amount","path":"amount","type":"number"}],
        \\  "materializations": [{"name":"sum_by_customer","op":"sum","group_by":["customer"],"measure":"amount"}]
        \\}
        ,
    });
    try db.batch(.{
        .writes = &.{.{ .key = "o1", .value = "{\"customer\":\"alice\",\"amount\":10}" }},
        .sync_level = .write,
    });

    const expr = algebraic_ir.TensorExpr{
        .fragment = .reduce,
        .input_dims = &.{ .doc, .scalar },
        .output_dims = &.{.bucket},
        .semantic_id = "sum_by_customer",
        .layout = .materialized_expr,
        .law_id = .sum,
    };
    var plan = (try algebraic_ir.planMaterializedExpressionAlloc(alloc, expr)).?;
    defer plan.deinit(alloc);
    const encoded = try encodeAlgebraicExpressionPartialsRequest(alloc, "alg", &.{plan.access_path}, &.{expr});
    defer alloc.free(encoded);
    var parsed = try parseAlgebraicPartialsRequest(alloc, encoded);
    defer parsed.deinit(alloc);
    try std.testing.expectError(error.UnsupportedQueryRequest, collectAlgebraicPartialsFromDbForRequest(alloc, &db, parsed));
}

test "algebraic partial request accepts current identity generation and rejects stale" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-algebraic-partials-stale-identity-generation";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{ .start_index_workers = false });
    defer db.close();
    try db.addIndex(.{
        .name = "alg",
        .kind = .algebraic,
        .config_json =
        \\{
        \\  "version": 1,
        \\  "schema_version": 1,
        \\  "table": "orders",
        \\  "group_fields": [{"name":"customer","path":"customer","type":"keyword"}],
        \\  "measure_fields": [{"name":"amount","path":"amount","type":"number"}],
        \\  "materializations": [{"name":"sum_by_customer","op":"sum","group_by":["customer"],"measure":"amount"}]
        \\}
        ,
    });
    const expr = algebraic_ir.TensorExpr{
        .fragment = .reduce,
        .input_dims = &.{ .doc, .scalar },
        .output_dims = &.{.bucket},
        .semantic_id = "sum_by_customer",
        .layout = .materialized_expr,
        .law_id = .sum,
    };
    var plan = (try algebraic_ir.planMaterializedExpressionAlloc(alloc, expr)).?;
    defer plan.deinit(alloc);
    const encoded = try encodeAlgebraicExpressionPartialsRequest(alloc, "alg", &.{plan.access_path}, &.{expr});
    defer alloc.free(encoded);

    var unstamped = try parseAlgebraicPartialsRequest(alloc, encoded);
    defer unstamped.deinit(alloc);
    const partials = try collectAlgebraicPartialsFromDbForRequest(alloc, &db, unstamped);
    defer db_mod.algebraic.distributed.freePartials(alloc, partials);

    var stamped = try parseAlgebraicPartialsRequest(alloc, encoded);
    defer stamped.deinit(alloc);
    stamped.identity_read_generation = db.core.nextDerivedSequence();
    const stamped_partials = try collectAlgebraicPartialsFromDbForRequest(alloc, &db, stamped);
    defer db_mod.algebraic.distributed.freePartials(alloc, stamped_partials);

    var stale = try parseAlgebraicPartialsRequest(alloc, encoded);
    defer stale.deinit(alloc);
    stale.identity_read_generation = db.core.nextDerivedSequence() + 1;
    try std.testing.expectError(error.UnsupportedQueryRequest, collectAlgebraicPartialsFromDbForRequest(alloc, &db, stale));
}

test "algebraic distributed planner selects derived join tensor program for metric" {
    const alloc = std.testing.allocator;

    var index = try db_mod.algebraic.index.Index.open(alloc, "alg",
        \\{"version":1,"table":"docs","schema_version":1,
        \\ "group_fields":[
        \\   {"name":"kind","path":"kind","type":"keyword"},
        \\   {"name":"customer","path":"customer","type":"keyword"},
        \\   {"name":"region","path":"region","type":"keyword"}
        \\ ],
        \\ "measure_fields":[{"name":"amount","path":"amount","type":"number"}],
        \\ "joins":[
        \\   {"name":"orders_customers","left_fields":["customer"],"right_fields":["customer"],"left_type_field":"kind","left_type_value":"order","right_type_field":"kind","right_type_value":"customer","max_fanout":8}
        \\ ],
        \\ "materializations":[]}
    );
    defer index.close();

    const request = db_mod.aggregations.SearchAggregationRequest{
        .name = "joined_amount",
        .type = "sum",
        .field = "amount",
        .algebraic_join = .{ .name = "orders_customers", .group_side = "right", .measure_side = "left" },
    };
    var program_plan = (try algebraicDistributedTensorProgramForRequestAlloc(alloc, &index, request, &.{})) orelse return error.TestUnexpectedResult;
    defer program_plan.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), program_plan.access_paths.len);
    try std.testing.expectEqual(algebraic_ir.PhysicalLayout.join_fact_rows, program_plan.access_paths[0].layout);
    try std.testing.expectEqual(@as(usize, 2), program_plan.steps.len);
    try std.testing.expectEqual(algebraic_ir.TensorFragment.join, program_plan.steps[1].expr.fragment);
    try std.testing.expectEqual(db_mod.algebraic.law.Id.sum, program_plan.steps[1].expr.law_id.?);
    try std.testing.expectEqualStrings("joined_amount", program_plan.steps[1].expr.semantic_id.?);
}

test "algebraic distributed planner selects identity-stamped derived join tensor program" {
    const alloc = std.testing.allocator;

    var index = try db_mod.algebraic.index.Index.open(alloc, "alg",
        \\{"version":1,"table":"docs","schema_version":1,
        \\ "group_fields":[
        \\   {"name":"kind","path":"kind","type":"keyword"},
        \\   {"name":"customer","path":"customer","type":"keyword"},
        \\   {"name":"region","path":"region","type":"keyword"}
        \\ ],
        \\ "measure_fields":[{"name":"amount","path":"amount","type":"number"}],
        \\ "joins":[
        \\   {"name":"orders_customers","left_fields":["customer"],"right_fields":["customer"],"left_type_field":"kind","left_type_value":"order","right_type_field":"kind","right_type_value":"customer","max_fanout":8}
        \\ ],
        \\ "materializations":[]}
    );
    defer index.close();

    const request = db_mod.aggregations.SearchAggregationRequest{
        .name = "joined_amount",
        .type = "sum",
        .field = "amount",
        .algebraic_join = .{ .name = "orders_customers", .group_side = "right", .measure_side = "left" },
    };
    var unstamped_plan = (try algebraicDistributedTensorProgramForAggregationRequestAlloc(alloc, &index, request, &.{}, null)) orelse return error.TestUnexpectedResult;
    defer unstamped_plan.deinit(alloc);
    try std.testing.expectEqual(algebraic_ir.TensorFragment.join, unstamped_plan.steps[1].expr.fragment);
    var stamped_plan = (try algebraicDistributedTensorProgramForAggregationRequestAlloc(alloc, &index, request, &.{}, 42)) orelse return error.TestUnexpectedResult;
    defer stamped_plan.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), stamped_plan.access_paths.len);
    try std.testing.expectEqual(algebraic_ir.PhysicalLayout.join_fact_rows, stamped_plan.access_paths[0].layout);
    try std.testing.expectEqual(@as(usize, 2), stamped_plan.steps.len);
    try std.testing.expectEqual(algebraic_ir.TensorFragment.join, stamped_plan.steps[1].expr.fragment);
    try std.testing.expectEqual(db_mod.algebraic.law.Id.sum, stamped_plan.steps[1].expr.law_id.?);
}

test "algebraic distributed planner selects derived join tensor program for terms child metric" {
    const alloc = std.testing.allocator;

    var index = try db_mod.algebraic.index.Index.open(alloc, "alg",
        \\{"version":1,"table":"docs","schema_version":1,
        \\ "group_fields":[
        \\   {"name":"kind","path":"kind","type":"keyword"},
        \\   {"name":"customer","path":"customer","type":"keyword"},
        \\   {"name":"region","path":"region","type":"keyword"}
        \\ ],
        \\ "measure_fields":[{"name":"amount","path":"amount","type":"number"}],
        \\ "joins":[
        \\   {"name":"orders_customers","left_fields":["customer"],"right_fields":["customer"],"left_type_field":"kind","left_type_value":"order","right_type_field":"kind","right_type_value":"customer","max_fanout":8}
        \\ ],
        \\ "materializations":[]}
    );
    defer index.close();

    const children = [_]db_mod.aggregations.SearchAggregationRequest{.{
        .name = "sum_amount",
        .type = "sum",
        .field = "amount",
    }};
    const constraints = [_]db_mod.aggregations.FixedConstraint{.{ .field = "customer", .value = "c1" }};
    const request = db_mod.aggregations.SearchAggregationRequest{
        .name = "regions",
        .type = "terms",
        .field = "region",
        .aggregations = children[0..],
        .algebraic_join = .{ .name = "orders_customers", .group_side = "right", .measure_side = "left" },
    };
    var program_plan = (try algebraicDistributedTensorProgramForRequestAlloc(alloc, &index, request, constraints[0..])) orelse return error.TestUnexpectedResult;
    defer program_plan.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), program_plan.access_paths.len);
    try std.testing.expectEqual(algebraic_ir.PhysicalLayout.join_fact_rows, program_plan.access_paths[0].layout);
    try std.testing.expectEqual(@as(usize, 3), program_plan.steps.len);
    try std.testing.expectEqual(@as(usize, 2), program_plan.outputs.len);
    try std.testing.expectEqual(algebraic_ir.TensorFragment.join, program_plan.steps[1].expr.fragment);
    try std.testing.expectEqual(db_mod.algebraic.law.Id.count, program_plan.steps[1].expr.law_id.?);
    try std.testing.expectEqualStrings("regions", program_plan.steps[1].expr.semantic_id.?);
    try std.testing.expectEqual(algebraic_ir.TensorFragment.join, program_plan.steps[2].expr.fragment);
    try std.testing.expectEqual(db_mod.algebraic.law.Id.sum, program_plan.steps[2].expr.law_id.?);
    try std.testing.expectEqualStrings("sum_amount", program_plan.steps[2].expr.semantic_id.?);
}

test "algebraic distributed planner selects derived join tensor program for histogram" {
    const alloc = std.testing.allocator;

    var index = try db_mod.algebraic.index.Index.open(alloc, "alg",
        \\{"version":1,"table":"docs","schema_version":1,
        \\ "group_fields":[
        \\   {"name":"kind","path":"kind","type":"keyword"},
        \\   {"name":"customer","path":"customer","type":"keyword"}
        \\ ],
        \\ "measure_fields":[{"name":"amount","path":"amount","type":"number"}],
        \\ "joins":[
        \\   {"name":"orders_customers","left_fields":["customer"],"right_fields":["customer"],"left_type_field":"kind","left_type_value":"order","right_type_field":"kind","right_type_value":"customer","max_fanout":8}
        \\ ],
        \\ "materializations":[]}
    );
    defer index.close();

    const request = db_mod.aggregations.SearchAggregationRequest{
        .name = "amount_histogram",
        .type = "histogram",
        .field = "amount",
        .interval = 20,
        .algebraic_join = .{ .name = "orders_customers", .group_side = "right", .measure_side = "left" },
    };
    var program_plan = (try algebraicDistributedTensorProgramForRequestAlloc(alloc, &index, request, &.{})) orelse return error.TestUnexpectedResult;
    defer program_plan.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), program_plan.access_paths.len);
    try std.testing.expectEqual(algebraic_ir.PhysicalLayout.join_fact_rows, program_plan.access_paths[0].layout);
    try std.testing.expectEqual(@as(usize, 2), program_plan.steps.len);
    try std.testing.expectEqual(algebraic_ir.TensorFragment.join, program_plan.steps[1].expr.fragment);
    try std.testing.expectEqual(db_mod.algebraic.law.Id.count, program_plan.steps[1].expr.law_id.?);
    try std.testing.expectEqualStrings("amount_histogram", program_plan.steps[1].expr.semantic_id.?);
}

test "algebraic distributed planner selects derived join tensor program for range" {
    const alloc = std.testing.allocator;

    var index = try db_mod.algebraic.index.Index.open(alloc, "alg",
        \\{"version":1,"table":"docs","schema_version":1,
        \\ "group_fields":[
        \\   {"name":"kind","path":"kind","type":"keyword"},
        \\   {"name":"customer","path":"customer","type":"keyword"}
        \\ ],
        \\ "measure_fields":[{"name":"amount","path":"amount","type":"number"}],
        \\ "joins":[
        \\   {"name":"orders_customers","left_fields":["customer"],"right_fields":["customer"],"left_type_field":"kind","left_type_value":"order","right_type_field":"kind","right_type_value":"customer","max_fanout":8}
        \\ ],
        \\ "materializations":[]}
    );
    defer index.close();

    const children = [_]db_mod.aggregations.SearchAggregationRequest{.{
        .name = "amount_sum",
        .type = "sum",
        .field = "amount",
    }};
    const request = db_mod.aggregations.SearchAggregationRequest{
        .name = "amount_ranges",
        .type = "range",
        .field = "amount",
        .ranges = &.{
            .{ .name = "low", .start = 0, .end = 20 },
            .{ .name = "high", .start = 20, .end = 40 },
        },
        .aggregations = children[0..],
        .algebraic_join = .{ .name = "orders_customers", .group_side = "right", .measure_side = "left" },
    };
    var program_plan = (try algebraicDistributedTensorProgramForRequestAlloc(alloc, &index, request, &.{})) orelse return error.TestUnexpectedResult;
    defer program_plan.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), program_plan.access_paths.len);
    try std.testing.expectEqual(algebraic_ir.PhysicalLayout.join_fact_rows, program_plan.access_paths[0].layout);
    try std.testing.expectEqual(@as(usize, 5), program_plan.steps.len);
    try std.testing.expectEqual(@as(usize, 4), program_plan.outputs.len);
    try std.testing.expectEqual(algebraic_ir.TensorFragment.join, program_plan.steps[1].expr.fragment);
    try std.testing.expectEqual(algebraic_ir.TensorFragment.join, program_plan.steps[2].expr.fragment);
    try std.testing.expectEqual(db_mod.algebraic.law.Id.count, program_plan.steps[1].expr.law_id.?);
    try std.testing.expectEqual(db_mod.algebraic.law.Id.sum, program_plan.steps[2].expr.law_id.?);

    var low_count = try db_mod.algebraic.index.decodeDerivedJoinFoldMetadataAlloc(alloc, program_plan.steps[1].expr.metadata.?);
    defer low_count.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.algebra.Op.count, low_count.request.op);
    try std.testing.expectEqual(db_mod.algebraic.index.DerivedJoinRangeKind.numeric, low_count.request.range_kind.?);
    try std.testing.expectEqual(db_mod.algebraic.fact.Role.measure, low_count.request.range_role.?);
    try std.testing.expectEqualStrings("amount", low_count.request.range_field.?);
    try std.testing.expectEqualStrings("0", low_count.request.range_start.?);
    try std.testing.expectEqualStrings("20", low_count.request.range_end.?);

    var low_sum = try db_mod.algebraic.index.decodeDerivedJoinFoldMetadataAlloc(alloc, program_plan.steps[2].expr.metadata.?);
    defer low_sum.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.algebra.Op.sum, low_sum.request.op);
    try std.testing.expectEqualStrings("amount", low_sum.request.measure.?);
}

test "algebraic distributed planner selects derived join tensor program for date range" {
    const alloc = std.testing.allocator;

    var index = try db_mod.algebraic.index.Index.open(alloc, "alg",
        \\{"version":1,"table":"docs","schema_version":1,
        \\ "group_fields":[
        \\   {"name":"kind","path":"kind","type":"keyword"},
        \\   {"name":"customer","path":"customer","type":"keyword"}
        \\ ],
        \\ "measure_fields":[{"name":"amount","path":"amount","type":"number"}],
        \\ "time_fields":[{"name":"created_at","path":"created_at","type":"datetime"}],
        \\ "joins":[
        \\   {"name":"orders_customers","left_fields":["customer"],"right_fields":["customer"],"left_type_field":"kind","left_type_value":"order","right_type_field":"kind","right_type_value":"customer","max_fanout":8}
        \\ ],
        \\ "materializations":[]}
    );
    defer index.close();

    const children = [_]db_mod.aggregations.SearchAggregationRequest{.{
        .name = "amount_sum",
        .type = "sum",
        .field = "amount",
    }};
    const request = db_mod.aggregations.SearchAggregationRequest{
        .name = "created_ranges",
        .type = "date_range",
        .field = "created_at",
        .date_ranges = &.{
            .{ .name = "may_1", .start = "2026-05-01T00:00:00Z", .end = "2026-05-02T00:00:00Z" },
            .{ .name = "may_2", .start = "2026-05-02T00:00:00Z", .end = "2026-05-03T00:00:00Z" },
        },
        .aggregations = children[0..],
        .algebraic_join = .{ .name = "orders_customers", .group_side = "right", .measure_side = "left" },
    };
    var program_plan = (try algebraicDistributedTensorProgramForRequestAlloc(alloc, &index, request, &.{})) orelse return error.TestUnexpectedResult;
    defer program_plan.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), program_plan.access_paths.len);
    try std.testing.expectEqual(algebraic_ir.PhysicalLayout.join_fact_rows, program_plan.access_paths[0].layout);
    try std.testing.expectEqual(@as(usize, 5), program_plan.steps.len);
    try std.testing.expectEqual(@as(usize, 4), program_plan.outputs.len);
    try std.testing.expectEqual(db_mod.algebraic.law.Id.count, program_plan.steps[1].expr.law_id.?);
    try std.testing.expectEqual(db_mod.algebraic.law.Id.sum, program_plan.steps[2].expr.law_id.?);

    var first_count = try db_mod.algebraic.index.decodeDerivedJoinFoldMetadataAlloc(alloc, program_plan.steps[1].expr.metadata.?);
    defer first_count.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.algebra.Op.count, first_count.request.op);
    try std.testing.expectEqual(db_mod.algebraic.index.DerivedJoinRangeKind.date, first_count.request.range_kind.?);
    try std.testing.expectEqual(db_mod.algebraic.fact.Role.time, first_count.request.range_role.?);
    try std.testing.expectEqualStrings("created_at", first_count.request.range_field.?);
    try std.testing.expectEqualStrings("2026-05-01T00:00:00Z", first_count.request.range_start.?);
    try std.testing.expectEqualStrings("2026-05-02T00:00:00Z", first_count.request.range_end.?);
}

test "algebraic distributed planner selects docfact tensor program for measure histogram" {
    const alloc = std.testing.allocator;

    var index = try db_mod.algebraic.index.Index.open(alloc, "alg",
        \\{"version":1,"table":"docs","schema_version":1,
        \\ "group_fields":[{"name":"tenant","path":"tenant","type":"keyword"}],
        \\ "measure_fields":[{"name":"amount","path":"amount","type":"number"}],
        \\ "materializations":[]}
    );
    defer index.close();

    const request = db_mod.aggregations.SearchAggregationRequest{
        .name = "amount_histogram",
        .type = "histogram",
        .field = "amount",
        .interval = 10,
    };
    const constraints = [_]db_mod.aggregations.FixedConstraint{.{ .field = "tenant", .value = "t1" }};
    var program_plan = (try algebraicDistributedTensorProgramForRequestAlloc(alloc, &index, request, constraints[0..])) orelse return error.TestUnexpectedResult;
    defer program_plan.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), program_plan.access_paths.len);
    try std.testing.expectEqual(algebraic_ir.PhysicalLayout.docfact_rows, program_plan.access_paths[0].layout);
    try std.testing.expectEqual(@as(usize, 2), program_plan.steps.len);
    try std.testing.expectEqual(algebraic_ir.TensorFragment.reduce, program_plan.steps[1].expr.fragment);
    try std.testing.expectEqual(db_mod.algebraic.law.Id.count, program_plan.steps[1].expr.law_id.?);
    try std.testing.expectEqualStrings("amount_histogram", program_plan.steps[1].expr.semantic_id.?);
    try std.testing.expect(program_plan.steps[1].expr.metadata != null);

    var fold = try db_mod.algebraic.index.decodeDocFactBucketFoldMetadataAlloc(alloc, program_plan.steps[1].expr.metadata.?);
    defer fold.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.index.DocFactBucketFoldKind.histogram, fold.request.kind);
    try std.testing.expectEqual(db_mod.algebraic.algebra.Op.count, fold.request.op);
    try std.testing.expectEqualStrings("amount", fold.request.bucket_field);
    try std.testing.expectEqual(@as(f64, 10), fold.request.histogram_interval);
    try std.testing.expectEqual(@as(usize, 1), fold.request.constraints.len);
    try std.testing.expectEqualStrings("tenant", fold.request.constraints[0].field);
    try std.testing.expectEqualStrings("t1", fold.request.constraints[0].value);
}

test "algebraic distributed planner selects docfact tensor program for measure histogram child metric" {
    const alloc = std.testing.allocator;

    var index = try db_mod.algebraic.index.Index.open(alloc, "alg",
        \\{"version":1,"table":"docs","schema_version":1,
        \\ "group_fields":[{"name":"tenant","path":"tenant","type":"keyword"}],
        \\ "measure_fields":[{"name":"amount","path":"amount","type":"number"}],
        \\ "materializations":[]}
    );
    defer index.close();

    const children = [_]db_mod.aggregations.SearchAggregationRequest{.{
        .name = "amount_sum",
        .type = "sum",
        .field = "amount",
    }};
    const request = db_mod.aggregations.SearchAggregationRequest{
        .name = "amount_histogram",
        .type = "histogram",
        .field = "amount",
        .interval = 10,
        .aggregations = children[0..],
    };
    const constraints = [_]db_mod.aggregations.FixedConstraint{.{ .field = "tenant", .value = "t1" }};
    var program_plan = (try algebraicDistributedTensorProgramForRequestAlloc(alloc, &index, request, constraints[0..])) orelse return error.TestUnexpectedResult;
    defer program_plan.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), program_plan.access_paths.len);
    try std.testing.expectEqual(algebraic_ir.PhysicalLayout.docfact_rows, program_plan.access_paths[0].layout);
    try std.testing.expectEqual(@as(usize, 3), program_plan.steps.len);
    try std.testing.expectEqual(@as(usize, 2), program_plan.outputs.len);
    try std.testing.expectEqual(db_mod.algebraic.law.Id.count, program_plan.steps[1].expr.law_id.?);
    try std.testing.expectEqual(db_mod.algebraic.law.Id.sum, program_plan.steps[2].expr.law_id.?);
    try std.testing.expectEqualStrings("amount_histogram", program_plan.steps[1].expr.semantic_id.?);
    try std.testing.expectEqualStrings("amount_sum", program_plan.steps[2].expr.semantic_id.?);

    var count_fold = try db_mod.algebraic.index.decodeDocFactBucketFoldMetadataAlloc(alloc, program_plan.steps[1].expr.metadata.?);
    defer count_fold.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.index.DocFactBucketFoldKind.histogram, count_fold.request.kind);
    try std.testing.expectEqual(db_mod.algebraic.algebra.Op.count, count_fold.request.op);
    try std.testing.expect(count_fold.request.measure == null);
    var sum_fold = try db_mod.algebraic.index.decodeDocFactBucketFoldMetadataAlloc(alloc, program_plan.steps[2].expr.metadata.?);
    defer sum_fold.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.index.DocFactBucketFoldKind.histogram, sum_fold.request.kind);
    try std.testing.expectEqual(db_mod.algebraic.algebra.Op.sum, sum_fold.request.op);
    try std.testing.expectEqualStrings("amount", sum_fold.request.measure.?);
}

test "algebraic distributed planner selects pathfact tensor program for schemaless histogram" {
    const alloc = std.testing.allocator;

    var index = try db_mod.algebraic.index.Index.open(alloc, "alg",
        \\{"version":1,"table":"docs","schema_version":1,"materializations":[]}
    );
    defer index.close();

    const children = [_]db_mod.aggregations.SearchAggregationRequest{.{
        .name = "amount_sum",
        .type = "sum",
        .field = "/amount",
    }};
    const request = db_mod.aggregations.SearchAggregationRequest{
        .name = "amount_histogram",
        .type = "histogram",
        .field = "/amount",
        .interval = 10,
        .aggregations = children[0..],
    };
    const constraints = [_]db_mod.aggregations.FixedConstraint{.{ .field = "/tenant", .value = "t1" }};
    var program_plan = (try algebraicDistributedTensorProgramForRequestAlloc(alloc, &index, request, constraints[0..])) orelse return error.TestUnexpectedResult;
    defer program_plan.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), program_plan.access_paths.len);
    try std.testing.expectEqual(algebraic_ir.PhysicalLayout.pathfact_rows, program_plan.access_paths[0].layout);
    try std.testing.expectEqual(@as(usize, 7), program_plan.steps.len);
    try std.testing.expectEqual(@as(usize, 6), program_plan.outputs.len);
    try std.testing.expectEqual(db_mod.algebraic.law.Id.count, program_plan.steps[1].expr.law_id.?);
    try std.testing.expectEqual(db_mod.algebraic.law.Id.sum, program_plan.steps[2].expr.law_id.?);
    try std.testing.expectEqual(db_mod.algebraic.law.Id.sum, program_plan.steps[3].expr.law_id.?);
    try std.testing.expectEqual(db_mod.algebraic.law.Id.count, program_plan.steps[4].expr.law_id.?);

    var count_fold = try db_mod.algebraic.index.decodePathFactBucketFoldMetadataAlloc(alloc, program_plan.steps[1].expr.metadata.?);
    defer count_fold.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.index.DocFactBucketFoldKind.histogram, count_fold.request.kind);
    try std.testing.expectEqual(db_mod.algebraic.algebra.Op.count, count_fold.request.op);
    try std.testing.expectEqualStrings("/amount", count_fold.request.bucket_path);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.number, count_fold.request.bucket_kind);
    try std.testing.expectEqual(@as(usize, 1), count_fold.request.constraints.len);
    try std.testing.expectEqualStrings("/tenant", count_fold.request.constraints[0].field);
    try std.testing.expectEqualStrings("t1", count_fold.request.constraints[0].value);
    var sum_fold = try db_mod.algebraic.index.decodePathFactBucketFoldMetadataAlloc(alloc, program_plan.steps[2].expr.metadata.?);
    defer sum_fold.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.algebra.Op.sum, sum_fold.request.op);
    try std.testing.expectEqualStrings("/amount", sum_fold.request.measure_path.?);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.number, sum_fold.request.measure_kind);
    var number_string_sum_fold = try db_mod.algebraic.index.decodePathFactBucketFoldMetadataAlloc(alloc, program_plan.steps[3].expr.metadata.?);
    defer number_string_sum_fold.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.number, number_string_sum_fold.request.bucket_kind);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.string, number_string_sum_fold.request.measure_kind);
    var string_count_fold = try db_mod.algebraic.index.decodePathFactBucketFoldMetadataAlloc(alloc, program_plan.steps[4].expr.metadata.?);
    defer string_count_fold.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.string, string_count_fold.request.bucket_kind);
    var string_sum_fold = try db_mod.algebraic.index.decodePathFactBucketFoldMetadataAlloc(alloc, program_plan.steps[6].expr.metadata.?);
    defer string_sum_fold.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.string, string_sum_fold.request.bucket_kind);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.string, string_sum_fold.request.measure_kind);
}

test "algebraic distributed planner honors pathfact numeric-string policy" {
    const alloc = std.testing.allocator;

    var index = try db_mod.algebraic.index.Index.open(alloc, "alg",
        \\{"version":1,"table":"docs","schema_version":1,"pathfact_policy":{"allow_numeric_string_coercion":false},"materializations":[]}
    );
    defer index.close();

    const children = [_]db_mod.aggregations.SearchAggregationRequest{.{
        .name = "amount_sum",
        .type = "sum",
        .field = "/amount",
    }};
    const request = db_mod.aggregations.SearchAggregationRequest{
        .name = "amount_histogram",
        .type = "histogram",
        .field = "/amount",
        .interval = 10,
        .aggregations = children[0..],
    };
    var program_plan = (try algebraicDistributedTensorProgramForRequestAlloc(alloc, &index, request, &.{})) orelse return error.TestUnexpectedResult;
    defer program_plan.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 3), program_plan.steps.len);
    try std.testing.expectEqual(@as(usize, 2), program_plan.outputs.len);

    var count_fold = try db_mod.algebraic.index.decodePathFactBucketFoldMetadataAlloc(alloc, program_plan.steps[1].expr.metadata.?);
    defer count_fold.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.index.DocFactBucketFoldKind.histogram, count_fold.request.kind);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.number, count_fold.request.bucket_kind);

    var sum_fold = try db_mod.algebraic.index.decodePathFactBucketFoldMetadataAlloc(alloc, program_plan.steps[2].expr.metadata.?);
    defer sum_fold.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.algebra.Op.sum, sum_fold.request.op);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.number, sum_fold.request.bucket_kind);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.number, sum_fold.request.measure_kind);
}

test "algebraic distributed planner selects pathfact tensor program for schemaless terms" {
    const alloc = std.testing.allocator;

    var index = try db_mod.algebraic.index.Index.open(alloc, "alg",
        \\{"version":1,"table":"docs","schema_version":1,"materializations":[]}
    );
    defer index.close();

    const request = db_mod.aggregations.SearchAggregationRequest{
        .name = "tier_terms",
        .type = "terms",
        .field = "/tier",
    };
    const constraints = [_]db_mod.aggregations.FixedConstraint{.{ .field = "/tenant", .value = "t1" }};
    var program_plan = (try algebraicDistributedTensorProgramForRequestAlloc(alloc, &index, request, constraints[0..])) orelse return error.TestUnexpectedResult;
    defer program_plan.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), program_plan.access_paths.len);
    try std.testing.expectEqual(algebraic_ir.PhysicalLayout.pathfact_rows, program_plan.access_paths[0].layout);
    try std.testing.expectEqual(@as(usize, 7), program_plan.steps.len);
    try std.testing.expectEqual(@as(usize, 6), program_plan.outputs.len);

    var string_fold = try db_mod.algebraic.index.decodePathFactBucketFoldMetadataAlloc(alloc, program_plan.steps[1].expr.metadata.?);
    defer string_fold.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.index.DocFactBucketFoldKind.terms, string_fold.request.kind);
    try std.testing.expectEqual(db_mod.algebraic.algebra.Op.count, string_fold.request.op);
    try std.testing.expectEqualStrings("/tier", string_fold.request.bucket_path);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.string, string_fold.request.bucket_kind);
    try std.testing.expectEqual(@as(usize, 1), string_fold.request.constraints.len);
    try std.testing.expectEqualStrings("/tenant", string_fold.request.constraints[0].field);
    try std.testing.expectEqualStrings("t1", string_fold.request.constraints[0].value);

    var number_fold = try db_mod.algebraic.index.decodePathFactBucketFoldMetadataAlloc(alloc, program_plan.steps[2].expr.metadata.?);
    defer number_fold.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.index.DocFactBucketFoldKind.terms, number_fold.request.kind);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.number, number_fold.request.bucket_kind);

    var bool_fold = try db_mod.algebraic.index.decodePathFactBucketFoldMetadataAlloc(alloc, program_plan.steps[3].expr.metadata.?);
    defer bool_fold.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.index.DocFactBucketFoldKind.terms, bool_fold.request.kind);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.bool, bool_fold.request.bucket_kind);

    var null_fold = try db_mod.algebraic.index.decodePathFactBucketFoldMetadataAlloc(alloc, program_plan.steps[4].expr.metadata.?);
    defer null_fold.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.index.DocFactBucketFoldKind.terms, null_fold.request.kind);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.null, null_fold.request.bucket_kind);

    var object_fold = try db_mod.algebraic.index.decodePathFactBucketFoldMetadataAlloc(alloc, program_plan.steps[5].expr.metadata.?);
    defer object_fold.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.index.DocFactBucketFoldKind.terms, object_fold.request.kind);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.object, object_fold.request.bucket_kind);

    var array_fold = try db_mod.algebraic.index.decodePathFactBucketFoldMetadataAlloc(alloc, program_plan.steps[6].expr.metadata.?);
    defer array_fold.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.index.DocFactBucketFoldKind.terms, array_fold.request.kind);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.array, array_fold.request.bucket_kind);
}

test "algebraic distributed planner carries same-path disjunction as pathfact any constraint" {
    const alloc = std.testing.allocator;

    var index = try db_mod.algebraic.index.Index.open(alloc, "alg",
        \\{"version":1,"table":"docs","schema_version":1,"materializations":[]}
    );
    defer index.close();

    const request = db_mod.aggregations.SearchAggregationRequest{
        .name = "tier_terms",
        .type = "terms",
        .field = "/tier",
    };
    const constraints = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"disjuncts\":[{\"term\":{\"/tenant\":\"t1\"}},{\"term\":{\"/tenant\":\"t2\"}}]}",
    })).?;
    defer freeAlgebraicConstraints(alloc, constraints);
    try std.testing.expectEqual(@as(usize, 1), constraints.len);

    var program_plan = (try algebraicDistributedTensorProgramForRequestAlloc(alloc, &index, request, constraints)) orelse return error.TestUnexpectedResult;
    defer program_plan.deinit(alloc);

    var string_fold = try db_mod.algebraic.index.decodePathFactBucketFoldMetadataAlloc(alloc, program_plan.steps[1].expr.metadata.?);
    defer string_fold.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), string_fold.request.constraints.len);
    try std.testing.expectEqualStrings("/tenant", string_fold.request.constraints[0].field);
    const parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, string_fold.request.constraints[0].value);
    defer {
        for (parts) |part| alloc.free(part);
        alloc.free(parts);
    }
    try std.testing.expectEqual(@as(usize, 3), parts.len);
    try std.testing.expectEqualStrings("pathfact-any:v1", parts[0]);
}

test "algebraic distributed planner selects pathfact tensor program for schemaless range" {
    const alloc = std.testing.allocator;

    var index = try db_mod.algebraic.index.Index.open(alloc, "alg",
        \\{"version":1,"table":"docs","schema_version":1,"materializations":[]}
    );
    defer index.close();

    const children = [_]db_mod.aggregations.SearchAggregationRequest{.{
        .name = "amount_sum",
        .type = "sum",
        .field = "/amount",
    }};
    const request = db_mod.aggregations.SearchAggregationRequest{
        .name = "amount_ranges",
        .type = "range",
        .field = "/amount",
        .ranges = &.{
            .{ .name = "low", .start = 0, .end = 20 },
            .{ .name = "high", .start = 20, .end = 30 },
        },
        .aggregations = children[0..],
    };
    const constraints = [_]db_mod.aggregations.FixedConstraint{.{ .field = "/tenant", .value = "t1" }};
    var program_plan = (try algebraicDistributedTensorProgramForRequestAlloc(alloc, &index, request, constraints[0..])) orelse return error.TestUnexpectedResult;
    defer program_plan.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), program_plan.access_paths.len);
    try std.testing.expectEqual(algebraic_ir.PhysicalLayout.pathfact_rows, program_plan.access_paths[0].layout);
    try std.testing.expectEqual(@as(usize, 13), program_plan.steps.len);
    try std.testing.expectEqual(@as(usize, 12), program_plan.outputs.len);

    var low_number_count = try db_mod.algebraic.index.decodePathFactBucketFoldMetadataAlloc(alloc, program_plan.steps[1].expr.metadata.?);
    defer low_number_count.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.index.DocFactBucketFoldKind.range, low_number_count.request.kind);
    try std.testing.expectEqual(db_mod.algebraic.algebra.Op.count, low_number_count.request.op);
    try std.testing.expectEqualStrings("/amount", low_number_count.request.bucket_path);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.number, low_number_count.request.bucket_kind);
    try std.testing.expectEqualStrings("0", low_number_count.request.range_start.?);
    try std.testing.expectEqualStrings("20", low_number_count.request.range_end.?);
    try std.testing.expectEqual(@as(usize, 1), low_number_count.request.constraints.len);
    try std.testing.expectEqualStrings("/tenant", low_number_count.request.constraints[0].field);
    try std.testing.expectEqualStrings("t1", low_number_count.request.constraints[0].value);

    var low_number_sum = try db_mod.algebraic.index.decodePathFactBucketFoldMetadataAlloc(alloc, program_plan.steps[2].expr.metadata.?);
    defer low_number_sum.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.algebra.Op.sum, low_number_sum.request.op);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.number, low_number_sum.request.bucket_kind);
    try std.testing.expectEqualStrings("/amount", low_number_sum.request.measure_path.?);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.number, low_number_sum.request.measure_kind);

    var low_number_string_sum = try db_mod.algebraic.index.decodePathFactBucketFoldMetadataAlloc(alloc, program_plan.steps[3].expr.metadata.?);
    defer low_number_string_sum.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.number, low_number_string_sum.request.bucket_kind);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.string, low_number_string_sum.request.measure_kind);

    var low_string_count = try db_mod.algebraic.index.decodePathFactBucketFoldMetadataAlloc(alloc, program_plan.steps[4].expr.metadata.?);
    defer low_string_count.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.string, low_string_count.request.bucket_kind);
    try std.testing.expectEqualStrings("0", low_string_count.request.range_start.?);
    try std.testing.expectEqualStrings("20", low_string_count.request.range_end.?);

    var low_string_sum = try db_mod.algebraic.index.decodePathFactBucketFoldMetadataAlloc(alloc, program_plan.steps[6].expr.metadata.?);
    defer low_string_sum.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.string, low_string_sum.request.bucket_kind);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.string, low_string_sum.request.measure_kind);

    var high_number_count = try db_mod.algebraic.index.decodePathFactBucketFoldMetadataAlloc(alloc, program_plan.steps[7].expr.metadata.?);
    defer high_number_count.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.number, high_number_count.request.bucket_kind);
    try std.testing.expectEqualStrings("20", high_number_count.request.range_start.?);
    try std.testing.expectEqualStrings("30", high_number_count.request.range_end.?);

    var high_string_sum = try db_mod.algebraic.index.decodePathFactBucketFoldMetadataAlloc(alloc, program_plan.steps[12].expr.metadata.?);
    defer high_string_sum.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.algebra.Op.sum, high_string_sum.request.op);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.string, high_string_sum.request.bucket_kind);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.string, high_string_sum.request.measure_kind);
    try std.testing.expectEqualStrings("20", high_string_sum.request.range_start.?);
    try std.testing.expectEqualStrings("30", high_string_sum.request.range_end.?);
}

test "algebraic distributed planner selects multi-output docfact tensor program for measure range" {
    const alloc = std.testing.allocator;

    var index = try db_mod.algebraic.index.Index.open(alloc, "alg",
        \\{"version":1,"table":"docs","schema_version":1,
        \\ "group_fields":[{"name":"tenant","path":"tenant","type":"keyword"}],
        \\ "measure_fields":[{"name":"amount","path":"amount","type":"number"}],
        \\ "materializations":[]}
    );
    defer index.close();

    const request = db_mod.aggregations.SearchAggregationRequest{
        .name = "amount_ranges",
        .type = "range",
        .field = "amount",
        .ranges = &.{
            .{ .name = "low", .start = 0, .end = 20 },
            .{ .name = "high", .start = 20, .end = 30 },
        },
    };
    const constraints = [_]db_mod.aggregations.FixedConstraint{.{ .field = "tenant", .value = "t1" }};
    var program_plan = (try algebraicDistributedTensorProgramForRequestAlloc(alloc, &index, request, constraints[0..])) orelse return error.TestUnexpectedResult;
    defer program_plan.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), program_plan.access_paths.len);
    try std.testing.expectEqual(algebraic_ir.PhysicalLayout.docfact_rows, program_plan.access_paths[0].layout);
    try std.testing.expectEqual(@as(usize, 3), program_plan.steps.len);
    try std.testing.expectEqual(@as(usize, 2), program_plan.outputs.len);
    try std.testing.expectEqual(algebraic_ir.TensorFragment.reduce, program_plan.steps[1].expr.fragment);
    try std.testing.expectEqual(algebraic_ir.TensorFragment.reduce, program_plan.steps[2].expr.fragment);

    var low = try db_mod.algebraic.index.decodeDocFactBucketFoldMetadataAlloc(alloc, program_plan.steps[1].expr.metadata.?);
    defer low.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.index.DocFactBucketFoldKind.range, low.request.kind);
    try std.testing.expectEqualStrings("0", low.request.range_start.?);
    try std.testing.expectEqualStrings("20", low.request.range_end.?);
    var high = try db_mod.algebraic.index.decodeDocFactBucketFoldMetadataAlloc(alloc, program_plan.steps[2].expr.metadata.?);
    defer high.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.index.DocFactBucketFoldKind.range, high.request.kind);
    try std.testing.expectEqualStrings("20", high.request.range_start.?);
    try std.testing.expectEqualStrings("30", high.request.range_end.?);
}

test "algebraic distributed planner selects multi-output docfact tensor program for date range" {
    const alloc = std.testing.allocator;

    var index = try db_mod.algebraic.index.Index.open(alloc, "alg",
        \\{"version":1,"table":"docs","schema_version":1,
        \\ "group_fields":[{"name":"tenant","path":"tenant","type":"keyword"}],
        \\ "time_fields":[{"name":"created_at","path":"created_at","type":"datetime"}],
        \\ "materializations":[]}
    );
    defer index.close();

    const request = db_mod.aggregations.SearchAggregationRequest{
        .name = "created_ranges",
        .type = "date_range",
        .field = "created_at",
        .date_ranges = &.{
            .{ .name = "first", .start = "2026-05-01T12:00:00Z", .end = "2026-05-02T00:00:00Z" },
            .{ .name = "second", .start = "2026-05-02T00:00:00Z", .end = "2026-05-03T00:00:00Z" },
        },
    };
    const constraints = [_]db_mod.aggregations.FixedConstraint{.{ .field = "tenant", .value = "t1" }};
    var program_plan = (try algebraicDistributedTensorProgramForRequestAlloc(alloc, &index, request, constraints[0..])) orelse return error.TestUnexpectedResult;
    defer program_plan.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), program_plan.access_paths.len);
    try std.testing.expectEqual(algebraic_ir.PhysicalLayout.docfact_rows, program_plan.access_paths[0].layout);
    try std.testing.expectEqual(@as(usize, 3), program_plan.steps.len);
    try std.testing.expectEqual(@as(usize, 2), program_plan.outputs.len);
    try std.testing.expectEqual(algebraic_ir.TensorFragment.reduce, program_plan.steps[1].expr.fragment);
    try std.testing.expectEqual(algebraic_ir.TensorFragment.reduce, program_plan.steps[2].expr.fragment);

    var first = try db_mod.algebraic.index.decodeDocFactBucketFoldMetadataAlloc(alloc, program_plan.steps[1].expr.metadata.?);
    defer first.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.index.DocFactBucketFoldKind.date_range, first.request.kind);
    try std.testing.expectEqualStrings("2026-05-01T12:00:00Z", first.request.range_start.?);
    try std.testing.expectEqualStrings("2026-05-02T00:00:00Z", first.request.range_end.?);
    try std.testing.expectEqual(@as(usize, 1), first.request.constraints.len);
    try std.testing.expectEqualStrings("tenant", first.request.constraints[0].field);
    try std.testing.expectEqualStrings("t1", first.request.constraints[0].value);
    var second = try db_mod.algebraic.index.decodeDocFactBucketFoldMetadataAlloc(alloc, program_plan.steps[2].expr.metadata.?);
    defer second.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.index.DocFactBucketFoldKind.date_range, second.request.kind);
    try std.testing.expectEqualStrings("2026-05-02T00:00:00Z", second.request.range_start.?);
    try std.testing.expectEqualStrings("2026-05-03T00:00:00Z", second.request.range_end.?);
}

test "algebraic distributed planner selects pathfact tensor program for schemaless date range" {
    const alloc = std.testing.allocator;

    var index = try db_mod.algebraic.index.Index.open(alloc, "alg",
        \\{"version":1,"table":"docs","schema_version":1,"materializations":[]}
    );
    defer index.close();

    const children = [_]db_mod.aggregations.SearchAggregationRequest{.{
        .name = "amount_sum",
        .type = "sum",
        .field = "/amount",
    }};
    const request = db_mod.aggregations.SearchAggregationRequest{
        .name = "created_ranges",
        .type = "date_range",
        .field = "/created_at",
        .date_ranges = &.{
            .{ .name = "first", .start = "2026-05-01T00:00:00Z", .end = "2026-05-02T00:00:00Z" },
            .{ .name = "second", .start = "2026-05-02T00:00:00Z", .end = "2026-05-03T00:00:00Z" },
        },
        .aggregations = children[0..],
    };
    const constraints = [_]db_mod.aggregations.FixedConstraint{.{ .field = "/tenant", .value = "t1" }};
    var program_plan = (try algebraicDistributedTensorProgramForRequestAlloc(alloc, &index, request, constraints[0..])) orelse return error.TestUnexpectedResult;
    defer program_plan.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), program_plan.access_paths.len);
    try std.testing.expectEqual(algebraic_ir.PhysicalLayout.pathfact_rows, program_plan.access_paths[0].layout);
    try std.testing.expectEqual(@as(usize, 7), program_plan.steps.len);
    try std.testing.expectEqual(@as(usize, 6), program_plan.outputs.len);

    var first_count = try db_mod.algebraic.index.decodePathFactBucketFoldMetadataAlloc(alloc, program_plan.steps[1].expr.metadata.?);
    defer first_count.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.index.DocFactBucketFoldKind.date_range, first_count.request.kind);
    try std.testing.expectEqual(db_mod.algebraic.algebra.Op.count, first_count.request.op);
    try std.testing.expectEqualStrings("/created_at", first_count.request.bucket_path);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.string, first_count.request.bucket_kind);
    try std.testing.expectEqualStrings("2026-05-01T00:00:00Z", first_count.request.range_start.?);
    try std.testing.expectEqualStrings("2026-05-02T00:00:00Z", first_count.request.range_end.?);
    try std.testing.expectEqual(@as(usize, 1), first_count.request.constraints.len);
    try std.testing.expectEqualStrings("/tenant", first_count.request.constraints[0].field);
    try std.testing.expectEqualStrings("t1", first_count.request.constraints[0].value);

    var first_sum = try db_mod.algebraic.index.decodePathFactBucketFoldMetadataAlloc(alloc, program_plan.steps[2].expr.metadata.?);
    defer first_sum.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.algebra.Op.sum, first_sum.request.op);
    try std.testing.expectEqualStrings("/amount", first_sum.request.measure_path.?);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.number, first_sum.request.measure_kind);

    var first_string_sum = try db_mod.algebraic.index.decodePathFactBucketFoldMetadataAlloc(alloc, program_plan.steps[3].expr.metadata.?);
    defer first_string_sum.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.algebra.Op.sum, first_string_sum.request.op);
    try std.testing.expectEqualStrings("/amount", first_string_sum.request.measure_path.?);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.string, first_string_sum.request.measure_kind);

    var second_count = try db_mod.algebraic.index.decodePathFactBucketFoldMetadataAlloc(alloc, program_plan.steps[4].expr.metadata.?);
    defer second_count.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.index.DocFactBucketFoldKind.date_range, second_count.request.kind);
    try std.testing.expectEqualStrings("2026-05-02T00:00:00Z", second_count.request.range_start.?);
    try std.testing.expectEqualStrings("2026-05-03T00:00:00Z", second_count.request.range_end.?);

    var second_string_sum = try db_mod.algebraic.index.decodePathFactBucketFoldMetadataAlloc(alloc, program_plan.steps[6].expr.metadata.?);
    defer second_string_sum.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.algebra.Op.sum, second_string_sum.request.op);
    try std.testing.expectEqualStrings("/amount", second_string_sum.request.measure_path.?);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.string, second_string_sum.request.measure_kind);
}

test "algebraic distributed planner honors pathfact datetime-string policy" {
    const alloc = std.testing.allocator;

    var index = try db_mod.algebraic.index.Index.open(alloc, "alg",
        \\{"version":1,"table":"docs","schema_version":1,"pathfact_policy":{"allow_datetime_string_coercion":false},"materializations":[]}
    );
    defer index.close();

    const request = db_mod.aggregations.SearchAggregationRequest{
        .name = "created_ranges",
        .type = "date_range",
        .field = "/created_at",
        .date_ranges = &.{
            .{ .name = "first", .start = "2026-05-01T00:00:00Z", .end = "2026-05-02T00:00:00Z" },
        },
    };
    const plan = try algebraicDistributedTensorProgramForRequestAlloc(alloc, &index, request, &.{});
    try std.testing.expect(plan == null);
}

test "hosted textStatsGroupLocal serves only the local group" {
    const test_alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-hosted-local-text-stats";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const group_path = try metadata_mod.groupDbPathFromReplicaRoot(test_alloc, path, 7);
    defer test_alloc.free(group_path);
    var db = try db_mod.DB.open(test_alloc, group_path, .{});
    defer db.close();

    try db.addIndex(.{ .name = "full_text_index_v0", .kind = .full_text, .config_json = "{}" });
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"body\":\"alpha\"}" },
            .{ .key = "doc:b", .value = "{\"body\":\"alpha beta\"}" },
            .{ .key = "doc:c", .value = "{\"body\":\"beta\"}" },
        },
        .sync_level = .full_index,
    });

    const FakeCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = @import("tables.zig").default_indexes_json,
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7, .table_id = 7, .start_key = "", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const FakeRouter = struct {
        fn iface() table_router.HostedGroupRouter {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .local_node_id = localNodeId,
                    .local_status = localStatus,
                    .group_leader_node_id = groupLeaderNodeId,
                    .node_status = nodeStatus,
                    .node_base_uri = nodeBaseUri,
                },
            };
        }

        fn localNodeId(_: *anyopaque) u64 {
            return 1;
        }

        fn localStatus(_: *anyopaque, _: u64) raft_mod.HostedReplicaStatus {
            return .active;
        }

        fn groupLeaderNodeId(_: *anyopaque, group_id: u64) ?u64 {
            return if (group_id == 7) 2 else null;
        }

        fn nodeStatus(_: *anyopaque, node_id: u64, group_id: u64) raft_mod.HostedReplicaStatus {
            if (group_id == 7 and node_id == 2) return .active;
            return .absent;
        }

        fn nodeBaseUri(_: *anyopaque, alloc: std.mem.Allocator, node_id: u64) !?[]u8 {
            if (node_id != 2) return null;
            return try alloc.dupe(u8, "http://remote.test");
        }
    };

    const ExecutorState = struct {
        call_count: usize = 0,

        fn iface(self: *@This()) http_common.RequestExecutor {
            return .{
                .ptr = self,
                .vtable = &.{
                    .execute = execute,
                },
            };
        }

        fn execute(ptr: *anyopaque, alloc: std.mem.Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.call_count += 1;
            _ = alloc;
            _ = req;
            try std.testing.expect(false);
            return error.UnexpectedHttpRequest;
        }
    };

    var executor_state = ExecutorState{};
    var hosted = HostedProvisionedTableReadSource.init(
        path,
        FakeCatalog.iface(),
        raft_mod.read_gate.noopReadableLeaseRequester(),
        FakeRouter.iface(),
        executor_state.iface(),
    );

    var response = (try hosted.source().textStatsGroupLocal(
        test_alloc,
        7,
        "docs",
        "{\"fields\":[{\"field\":\"body\",\"terms\":[\"alpha\"]}]}",
    )) orelse unreachable;
    defer response.deinit(test_alloc);

    try std.testing.expectEqual(@as(usize, 0), executor_state.call_count);
    try std.testing.expect(std.mem.indexOf(u8, response.json, "\"global_doc_count\":3") != null);
    try std.testing.expect(std.mem.indexOf(u8, response.json, "\"field\":\"body\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, response.json, "\"term\":\"alpha\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, response.json, "\"doc_freq\":2") != null);
}

test "hosted table read source preflights query locally" {
    const test_alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-hosted-local-preflight";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const group_path = try metadata_mod.groupDbPathFromReplicaRoot(test_alloc, path, 7);
    defer test_alloc.free(group_path);
    var db = try db_mod.DB.open(test_alloc, group_path, .{});
    defer db.close();
    try db.addIndex(.{ .name = "dv_v1", .kind = .dense_vector, .config_json = "{\"field\":\"embedding\",\"dims\":3}" });

    const FakeCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = @import("tables.zig").default_indexes_json,
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7, .table_id = 7, .start_key = "", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const FakeRouter = struct {
        fn iface() table_router.HostedGroupRouter {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .local_node_id = localNodeId,
                    .local_status = localStatus,
                    .group_leader_node_id = groupLeaderNodeId,
                    .node_status = nodeStatus,
                    .node_base_uri = nodeBaseUri,
                },
            };
        }

        fn localNodeId(_: *anyopaque) u64 {
            return 1;
        }

        fn localStatus(_: *anyopaque, _: u64) raft_mod.HostedReplicaStatus {
            return .active;
        }

        fn groupLeaderNodeId(_: *anyopaque, _: u64) ?u64 {
            return 1;
        }

        fn nodeStatus(_: *anyopaque, _: u64, _: u64) raft_mod.HostedReplicaStatus {
            return .absent;
        }

        fn nodeBaseUri(_: *anyopaque, _: std.mem.Allocator, _: u64) !?[]u8 {
            return null;
        }
    };

    const ExecutorState = struct {
        call_count: usize = 0,

        fn iface(self: *@This()) http_common.RequestExecutor {
            return .{
                .ptr = self,
                .vtable = &.{ .execute = execute },
            };
        }

        fn execute(ptr: *anyopaque, alloc: std.mem.Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.call_count += 1;
            _ = alloc;
            _ = req;
            return error.UnexpectedHttpRequest;
        }
    };

    var executor_state = ExecutorState{};
    var hosted = HostedProvisionedTableReadSource.init(
        path,
        FakeCatalog.iface(),
        raft_mod.read_gate.noopReadableLeaseRequester(),
        FakeRouter.iface(),
        executor_state.iface(),
    );

    try std.testing.expectError(error.InvalidArgument, hosted.source().preflightQuery(test_alloc, "docs", .{
        .index_name = "dv_v1",
        .dense = .{ .vector = &.{ 1.0, 2.0 }, .k = 5 },
    }, .read_index, 0));

    var summary = (try hosted.source().preflightQuery(test_alloc, "docs", .{
        .index_name = "dv_v1",
        .dense = .{ .vector = &.{ 1.0, 2.0, 3.0 }, .k = 5 },
    }, .read_index, 0)).?;
    defer summary.deinit(test_alloc);

    try std.testing.expectEqual(@as(usize, 0), executor_state.call_count);
    try std.testing.expectEqual(@as(usize, 1), summary.result_refs.len);
    try std.testing.expectEqualStrings("$embeddings_results", summary.result_refs[0]);
}

test "hosted table read source preflights every local group" {
    const test_alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-hosted-local-preflight-multigroup";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const left_path = try metadata_mod.groupDbPathFromReplicaRoot(test_alloc, path, 7);
    defer test_alloc.free(left_path);
    const right_path = try metadata_mod.groupDbPathFromReplicaRoot(test_alloc, path, 8);
    defer test_alloc.free(right_path);
    var left_db = try db_mod.DB.open(test_alloc, left_path, .{});
    defer left_db.close();
    var right_db = try db_mod.DB.open(test_alloc, right_path, .{});
    defer right_db.close();
    try left_db.addIndex(.{ .name = "dv_v1", .kind = .dense_vector, .config_json = "{\"field\":\"embedding\",\"dims\":3}" });

    const FakeCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = @import("tables.zig").default_indexes_json,
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7, .table_id = 7, .start_key = "", .end_key = "doc:m" },
                    .{ .group_id = 8, .table_id = 7, .start_key = "doc:m", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const FakeRouter = struct {
        fn iface() table_router.HostedGroupRouter {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .local_node_id = localNodeId,
                    .local_status = localStatus,
                    .group_leader_node_id = groupLeaderNodeId,
                    .node_status = nodeStatus,
                    .node_base_uri = nodeBaseUri,
                },
            };
        }

        fn localNodeId(_: *anyopaque) u64 {
            return 1;
        }

        fn localStatus(_: *anyopaque, _: u64) raft_mod.HostedReplicaStatus {
            return .active;
        }

        fn groupLeaderNodeId(_: *anyopaque, _: u64) ?u64 {
            return 1;
        }

        fn nodeStatus(_: *anyopaque, _: u64, _: u64) raft_mod.HostedReplicaStatus {
            return .absent;
        }

        fn nodeBaseUri(_: *anyopaque, _: std.mem.Allocator, _: u64) !?[]u8 {
            return null;
        }
    };

    const ExecutorState = struct {
        call_count: usize = 0,

        fn iface(self: *@This()) http_common.RequestExecutor {
            return .{
                .ptr = self,
                .vtable = &.{ .execute = execute },
            };
        }

        fn execute(ptr: *anyopaque, alloc: std.mem.Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.call_count += 1;
            _ = alloc;
            _ = req;
            return error.UnexpectedHttpRequest;
        }
    };

    var executor_state = ExecutorState{};
    var hosted = HostedProvisionedTableReadSource.init(
        path,
        FakeCatalog.iface(),
        raft_mod.read_gate.noopReadableLeaseRequester(),
        FakeRouter.iface(),
        executor_state.iface(),
    );
    _ = hosted.withIo(&io_impl);

    try std.testing.expectError(error.InvalidArgument, hosted.source().preflightQuery(test_alloc, "docs", .{
        .index_name = "dv_v1",
        .dense = .{ .vector = &.{ 1.0, 2.0, 3.0 }, .k = 5 },
    }, .read_index, 0));
    try std.testing.expectError(error.UnsupportedQueryRequest, hosted.source().preflightQuery(test_alloc, "docs", .{
        .graph_queries = &.{
            .{
                .name = "neighbors",
                .query = .{
                    .query_type = .neighbors,
                    .index_name = "graph_v1",
                    .start_nodes = .{ .result_ref = .{ .ref = "$embeddings_results", .limit = 1 } },
                    .params = .{ .edge_types = &.{}, .max_depth = 1 },
                },
            },
        },
    }, .read_index, 0));
    try std.testing.expectEqual(@as(usize, 0), executor_state.call_count);
}

test "hosted table read source preflights mixed local and remote groups" {
    const test_alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-hosted-preflight-mixed";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const local_path = try metadata_mod.groupDbPathFromReplicaRoot(test_alloc, path, 7);
    defer test_alloc.free(local_path);
    var local_db = try db_mod.DB.open(test_alloc, local_path, .{});
    defer local_db.close();
    try local_db.addIndex(.{ .name = "dv_v1", .kind = .dense_vector, .config_json = "{\"field\":\"embedding\",\"dims\":3}" });

    const FakeCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = @import("tables.zig").default_indexes_json,
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7, .table_id = 7, .start_key = "", .end_key = "doc:m" },
                    .{ .group_id = 8, .table_id = 7, .start_key = "doc:m", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const FakeRouter = struct {
        fn iface() table_router.HostedGroupRouter {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .local_node_id = localNodeId,
                    .local_status = localStatus,
                    .group_leader_node_id = groupLeaderNodeId,
                    .node_status = nodeStatus,
                    .node_base_uri = nodeBaseUri,
                },
            };
        }

        fn localNodeId(_: *anyopaque) u64 {
            return 1;
        }

        fn localStatus(_: *anyopaque, group_id: u64) raft_mod.HostedReplicaStatus {
            return if (group_id == 7) .active else .absent;
        }

        fn groupLeaderNodeId(_: *anyopaque, group_id: u64) ?u64 {
            return if (group_id == 7) 1 else 2;
        }

        fn nodeStatus(_: *anyopaque, node_id: u64, group_id: u64) raft_mod.HostedReplicaStatus {
            if (group_id == 8 and node_id == 2) return .active;
            return .absent;
        }

        fn nodeBaseUri(_: *anyopaque, alloc_inner: std.mem.Allocator, node_id: u64) !?[]u8 {
            if (node_id != 2) return null;
            return try alloc_inner.dupe(u8, "http://remote.test");
        }
    };

    const ExecutorState = struct {
        call_count: usize = 0,

        fn iface(self: *@This()) http_common.RequestExecutor {
            return .{
                .ptr = self,
                .vtable = &.{ .execute = execute },
            };
        }

        fn execute(ptr: *anyopaque, alloc_inner: std.mem.Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.call_count += 1;
            try std.testing.expect(std.mem.endsWith(u8, req.uri, "/internal/v1/groups/8/tables/docs/query-preflight"));
            try std.testing.expectEqual(http_common.Method.POST, req.method);
            return .{
                .status = 400,
                .body = try alloc_inner.dupe(u8, "IndexNotFound"),
            };
        }
    };

    var executor_state = ExecutorState{};
    var hosted = HostedProvisionedTableReadSource.init(
        path,
        FakeCatalog.iface(),
        raft_mod.read_gate.noopReadableLeaseRequester(),
        FakeRouter.iface(),
        executor_state.iface(),
    );
    _ = hosted.withIo(&io_impl);

    try std.testing.expectError(error.IndexNotFound, hosted.source().preflightQuery(test_alloc, "docs", .{
        .index_name = "dv_v1",
        .dense = .{ .vector = &.{ 1.0, 2.0, 3.0 }, .k = 5 },
    }, .read_index, 0));
    try std.testing.expectEqual(@as(usize, 1), executor_state.call_count);
}

test "hosted cross-range graph query expands explicit local start keys" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-hosted-cross-range-graph-explicit";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const left_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(left_path);
    const right_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7002);
    defer alloc.free(right_path);

    const graph_indexes_json =
        \\{"relations_graph":{"type":"graph","edge_types":[{"name":"mentions"}]}}
    ;

    var left_db = try db_mod.DB.open(alloc, left_path, .{});
    defer left_db.close();
    try left_db.addIndex(.{ .name = "relations_graph", .kind = .graph, .config_json = "{\"edge_types\":[{\"name\":\"mentions\"}]}" });
    try left_db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"left\"}" }},
        .sync_level = .write,
    });

    var right_db = try db_mod.DB.open(alloc, right_path, .{});
    defer right_db.close();
    try right_db.addIndex(.{ .name = "relations_graph", .kind = .graph, .config_json = "{\"edge_types\":[{\"name\":\"mentions\"}]}" });
    try right_db.batch(.{
        .writes = &.{.{ .key = "zdoc:a", .value = "{\"title\":\"right\"}" }},
        .sync_level = .write,
    });
    const graph_entry = right_db.core.graphIndex("relations_graph") orelse return error.IndexNotFound;
    try graph_entry.index.addEdge("zdoc:a", "entity:ada", "mentions", 1.0, 0, 0, "{\"target_table\":\"entities\"}");

    const FakeCatalog = struct {
        const statuses = [_]metadata_reconciler.MergedGroupStatus{
            .{
                .group_id = 7001,
                .doc_identity = .{
                    .namespace_table_id = 7,
                    .namespace_shard_id = 7001,
                    .namespace_range_id = 7001,
                    .next_ordinal = 2,
                    .allocated_ordinals = 1,
                    .state_rows = 1,
                    .live_ordinals = 1,
                    .complete = true,
                },
            },
            .{
                .group_id = 7002,
                .doc_identity = .{
                    .namespace_table_id = 7,
                    .namespace_shard_id = 7002,
                    .namespace_range_id = 7002,
                    .next_ordinal = 2,
                    .allocated_ordinals = 1,
                    .state_rows = 1,
                    .live_ordinals = 1,
                    .complete = true,
                },
            },
        };

        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = graph_indexes_json,
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .range_id = 7001, .start_key = "", .end_key = "m" },
                    .{ .group_id = 7002, .table_id = 7, .range_id = 7002, .start_key = "m", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
                .merged_group_statuses = @constCast(statuses[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const FakeRouter = struct {
        fn iface() table_router.HostedGroupRouter {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .local_node_id = localNodeId,
                    .local_status = localStatus,
                    .group_leader_node_id = groupLeaderNodeId,
                    .node_status = nodeStatus,
                    .node_base_uri = nodeBaseUri,
                },
            };
        }

        fn localNodeId(_: *anyopaque) u64 {
            return 1;
        }

        fn localStatus(_: *anyopaque, _: u64) raft_mod.HostedReplicaStatus {
            return .active;
        }

        fn groupLeaderNodeId(_: *anyopaque, _: u64) ?u64 {
            return 1;
        }

        fn nodeStatus(_: *anyopaque, _: u64, _: u64) raft_mod.HostedReplicaStatus {
            return .absent;
        }

        fn nodeBaseUri(_: *anyopaque, _: std.mem.Allocator, _: u64) !?[]u8 {
            return null;
        }
    };

    const ExecutorState = struct {
        fn iface(self: *@This()) http_common.RequestExecutor {
            return .{
                .ptr = self,
                .vtable = &.{ .execute = execute },
            };
        }

        fn execute(_: *anyopaque, _: std.mem.Allocator, _: http_common.HttpRequest) !http_common.HttpResponse {
            return error.UnexpectedHttpRequest;
        }
    };

    var executor_state = ExecutorState{};
    var hosted = HostedProvisionedTableReadSource.init(
        path,
        FakeCatalog.iface(),
        raft_mod.read_gate.noopReadableLeaseRequester(),
        FakeRouter.iface(),
        executor_state.iface(),
    );
    _ = hosted.withIo(&io_impl);

    var response = (try hosted.source().query(alloc, "docs", .{
        .query = .{ .match_all = {} },
        .limit = 10,
        .graph_queries = &.{.{
            .name = "mentions",
            .query = .{
                .query_type = .neighbors,
                .index_name = "relations_graph",
                .start_nodes = .{ .keys = &.{"zdoc:a"} },
                .params = .{ .edge_types = &.{"mentions"}, .direction = .out, .max_results = 10 },
            },
        }},
    }, .read_index)).?;
    defer response.deinit(alloc);

    try std.testing.expect(std.mem.indexOf(u8, response.json, "\"graph_results\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, response.json, "\"entity:ada\"") != null);
}

test "hosted cross-range graph metric fan-in merges compatible published shard generations" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-hosted-cross-range-graph-metric-merge";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const left_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7101);
    defer alloc.free(left_path);
    const right_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7102);
    defer alloc.free(right_path);

    const graph_indexes_json =
        \\{"graph_idx":{"type":"graph","edge_types":[{"name":"cites"}],"metrics":{"manual_degree":{"enabled":true,"kind":"degree","refresh":"manual","edge_filter":{"types":["cites"]}},"pagerank":{"enabled":true,"kind":"pagerank","refresh":"manual","max_iterations":2,"tolerance":0.000001,"edge_filter":{"types":["cites"]}},"eigenvector":{"enabled":true,"kind":"eigenvector","refresh":"manual","max_iterations":2,"tolerance":0.000001,"edge_filter":{"types":["cites"]}}}}}
    ;
    const graph_config_json =
        \\{"edge_types":[{"name":"cites"}],"metrics":{"manual_degree":{"enabled":true,"kind":"degree","refresh":"manual","edge_filter":{"types":["cites"]}},"pagerank":{"enabled":true,"kind":"pagerank","refresh":"manual","max_iterations":2,"tolerance":0.000001,"edge_filter":{"types":["cites"]}},"eigenvector":{"enabled":true,"kind":"eigenvector","refresh":"manual","max_iterations":2,"tolerance":0.000001,"edge_filter":{"types":["cites"]}}}}
    ;
    var left_db = try db_mod.DB.open(alloc, left_path, .{
        .start_index_workers = false,
        .identity_namespace = .{ .table_id = 7, .shard_id = 7101, .range_id = 7101 },
    });
    defer left_db.close();
    try left_db.addIndex(.{ .name = "graph_idx", .kind = .graph, .config_json = graph_config_json });
    try left_db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"left-a\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"left-b\"}" },
        },
        .sync_level = .write,
    });
    try left_db.runUntilIdle();
    var left_status = try left_db.refreshGraphMetric(alloc, "graph_idx", "manual_degree");
    defer left_status.deinit(alloc);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, left_status.state);

    var right_db = try db_mod.DB.open(alloc, right_path, .{
        .start_index_workers = false,
        .identity_namespace = .{ .table_id = 7, .shard_id = 7102, .range_id = 7102 },
    });
    defer right_db.close();
    try right_db.addIndex(.{ .name = "graph_idx", .kind = .graph, .config_json = graph_config_json });
    try right_db.batch(.{
        .writes = &.{
            .{ .key = "doc:n", .value = "{\"title\":\"right-n\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:o\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:o", .value = "{\"title\":\"right-o\"}" },
        },
        .sync_level = .write,
    });
    try right_db.runUntilIdle();
    var right_status = try right_db.refreshGraphMetric(alloc, "graph_idx", "manual_degree");
    defer right_status.deinit(alloc);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, right_status.state);
    try std.testing.expectEqual(left_status.published_generation, right_status.published_generation);

    const FakeCatalog = struct {
        const statuses = [_]metadata_reconciler.MergedGroupStatus{
            .{
                .group_id = 7101,
                .doc_identity = .{
                    .namespace_table_id = 7,
                    .namespace_shard_id = 7101,
                    .namespace_range_id = 7101,
                    .next_ordinal = 3,
                    .allocated_ordinals = 2,
                    .state_rows = 2,
                    .live_ordinals = 2,
                    .complete = true,
                },
            },
            .{
                .group_id = 7102,
                .doc_identity = .{
                    .namespace_table_id = 7,
                    .namespace_shard_id = 7102,
                    .namespace_range_id = 7102,
                    .next_ordinal = 3,
                    .allocated_ordinals = 2,
                    .state_rows = 2,
                    .live_ordinals = 2,
                    .complete = true,
                },
            },
        };

        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = graph_indexes_json,
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7101, .table_id = 7, .range_id = 7101, .start_key = "", .end_key = "m" },
                    .{ .group_id = 7102, .table_id = 7, .range_id = 7102, .start_key = "m", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
                .merged_group_statuses = @constCast(statuses[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const FakeRouter = struct {
        fn iface() table_router.HostedGroupRouter {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .local_node_id = localNodeId,
                    .local_status = localStatus,
                    .group_leader_node_id = groupLeaderNodeId,
                    .node_status = nodeStatus,
                    .node_base_uri = nodeBaseUri,
                },
            };
        }

        fn localNodeId(_: *anyopaque) u64 {
            return 1;
        }

        fn localStatus(_: *anyopaque, _: u64) raft_mod.HostedReplicaStatus {
            return .active;
        }

        fn groupLeaderNodeId(_: *anyopaque, _: u64) ?u64 {
            return 1;
        }

        fn nodeStatus(_: *anyopaque, _: u64, _: u64) raft_mod.HostedReplicaStatus {
            return .absent;
        }

        fn nodeBaseUri(_: *anyopaque, _: std.mem.Allocator, _: u64) !?[]u8 {
            return null;
        }
    };

    const ExecutorState = struct {
        fn iface(self: *@This()) http_common.RequestExecutor {
            return .{ .ptr = self, .vtable = &.{ .execute = execute } };
        }

        fn execute(_: *anyopaque, _: std.mem.Allocator, _: http_common.HttpRequest) !http_common.HttpResponse {
            return error.UnexpectedHttpRequest;
        }
    };

    var executor_state = ExecutorState{};
    var hosted = HostedProvisionedTableReadSource.init(
        path,
        FakeCatalog.iface(),
        raft_mod.read_gate.noopReadableLeaseRequester(),
        FakeRouter.iface(),
        executor_state.iface(),
    );
    _ = hosted.withIo(&io_impl);

    var response = (try hosted.source().query(alloc, "docs", .{
        .graph_metric_queries = &.{.{
            .name = "central",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "manual_degree",
                .top_k = 4,
                .freshness = .published,
            },
        }},
        .limit = 0,
    }, .read_index)).?;
    defer response.deinit(alloc);

    try std.testing.expect(std.mem.indexOf(u8, response.json, "\"graph_metric_results\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, response.json, "\"central\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, response.json, "\"manual_degree\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, response.json, "\"doc:b\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, response.json, "\"doc:o\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, response.json, "\"state\":\"fresh\"") != null);
}

test "hosted cross-range graph metric fan-in merges active stale shard for published" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-hosted-cross-range-graph-metric-active-stale";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const left_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7341);
    defer alloc.free(left_path);
    const right_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7342);
    defer alloc.free(right_path);

    const graph_indexes_json =
        \\{"ft_v1":{"type":"full_text","store":true},"graph_idx":{"type":"graph","edge_types":[{"name":"cites"}],"metrics":{"manual_degree":{"enabled":true,"kind":"degree","refresh":"manual","edge_filter":{"types":["cites"]}},"pagerank":{"enabled":true,"kind":"pagerank","refresh":"manual","max_iterations":2,"tolerance":0.000001,"edge_filter":{"types":["cites"]}},"eigenvector":{"enabled":true,"kind":"eigenvector","refresh":"manual","max_iterations":2,"tolerance":0.000001,"edge_filter":{"types":["cites"]}}}}}
    ;
    const graph_config_json =
        \\{"edge_types":[{"name":"cites"}],"metrics":{"manual_degree":{"enabled":true,"kind":"degree","refresh":"manual","edge_filter":{"types":["cites"]}},"pagerank":{"enabled":true,"kind":"pagerank","refresh":"manual","max_iterations":2,"tolerance":0.000001,"edge_filter":{"types":["cites"]}},"eigenvector":{"enabled":true,"kind":"eigenvector","refresh":"manual","max_iterations":2,"tolerance":0.000001,"edge_filter":{"types":["cites"]}}}}
    ;

    var left_db = try db_mod.DB.open(alloc, left_path, .{
        .start_index_workers = false,
        .identity_namespace = .{ .table_id = 7, .shard_id = 7341, .range_id = 7341 },
    });
    defer left_db.close();
    try left_db.addIndex(.{ .name = "ft_v1", .kind = .full_text, .config_json = "{\"store\":true}" });
    try left_db.addIndex(.{ .name = "graph_idx", .kind = .graph, .config_json = graph_config_json });
    try left_db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"left-a\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"left-b\"}" },
        },
        .sync_level = .full_index,
    });
    try left_db.runUntilIdle();
    var left_status = try left_db.refreshGraphMetric(alloc, "graph_idx", "manual_degree");
    defer left_status.deinit(alloc);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, left_status.state);
    var left_pagerank_status = try left_db.refreshGraphMetric(alloc, "graph_idx", "pagerank");
    defer left_pagerank_status.deinit(alloc);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, left_pagerank_status.state);
    try std.testing.expectEqual(left_status.published_generation, left_pagerank_status.published_generation);
    var left_eigenvector_status = try left_db.refreshGraphMetric(alloc, "graph_idx", "eigenvector");
    defer left_eigenvector_status.deinit(alloc);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, left_eigenvector_status.state);
    try std.testing.expectEqual(left_status.published_generation, left_eigenvector_status.published_generation);

    var right_db = try db_mod.DB.open(alloc, right_path, .{
        .start_index_workers = false,
        .identity_namespace = .{ .table_id = 7, .shard_id = 7342, .range_id = 7342 },
    });
    defer right_db.close();
    try right_db.addIndex(.{ .name = "ft_v1", .kind = .full_text, .config_json = "{\"store\":true}" });
    try right_db.addIndex(.{ .name = "graph_idx", .kind = .graph, .config_json = graph_config_json });
    try right_db.batch(.{
        .writes = &.{
            .{ .key = "doc:n", .value = "{\"title\":\"right-n\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:o\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:o", .value = "{\"title\":\"right-o\"}" },
        },
        .sync_level = .full_index,
    });
    try right_db.runUntilIdle();
    var right_status = try right_db.refreshGraphMetric(alloc, "graph_idx", "manual_degree");
    defer right_status.deinit(alloc);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, right_status.state);
    try std.testing.expectEqual(left_status.published_generation, right_status.published_generation);
    var right_pagerank_status = try right_db.refreshGraphMetric(alloc, "graph_idx", "pagerank");
    defer right_pagerank_status.deinit(alloc);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, right_pagerank_status.state);
    try std.testing.expectEqual(left_status.published_generation, right_pagerank_status.published_generation);
    var right_eigenvector_status = try right_db.refreshGraphMetric(alloc, "graph_idx", "eigenvector");
    defer right_eigenvector_status.deinit(alloc);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, right_eigenvector_status.state);
    try std.testing.expectEqual(left_status.published_generation, right_eigenvector_status.published_generation);

    try right_db.batch(.{
        .writes = &.{.{ .key = "doc:p", .value = "{\"title\":\"right-p\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:o\",\"weight\":1.0}]}}}" }},
        .sync_level = .full_index,
    });
    try right_db.runUntilIdle();
    const active_target_generation = blk: {
        const graph_entry = right_db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        const target_generation = graph_entry.index.edge_generation;
        const active_metrics = [_][]const u8{ "manual_degree", "pagerank", "eigenvector" };
        for (active_metrics) |metric_name| {
            var building = try graph_entry.index.ensureGraphMetricPlannedBuild(metric_name, target_generation);
            defer building.deinit(alloc);
            try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.building, building.state);
            try std.testing.expectEqual(target_generation, building.building_generation);

            const prepare = try graph_entry.index.runGraphMetricPlannedWorkerPageStepForMetric(metric_name, "worker-prepare");
            try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.prepare_generation, prepare.phase);
            try std.testing.expect(prepare.claimed_page);
            try std.testing.expect(prepare.completed_page);

            const advance_prepare = try graph_entry.index.runGraphMetricPlannedCoordinatorStepForMetric(metric_name);
            try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.prepare_generation, advance_prepare.phase);
            try std.testing.expect(advance_prepare.advanced_phase);

            const scan = try graph_entry.index.runGraphMetricPlannedWorkerPageStepForMetric(metric_name, "worker-scan");
            try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.scan_edges_and_out_degree, scan.phase);
            try std.testing.expect(scan.claimed_page);
            try std.testing.expect(scan.completed_page);
        }
        break :blk target_generation;
    };

    const FakeCatalog = struct {
        const statuses = [_]metadata_reconciler.MergedGroupStatus{
            .{
                .group_id = 7341,
                .doc_identity = .{
                    .namespace_table_id = 7,
                    .namespace_shard_id = 7341,
                    .namespace_range_id = 7341,
                    .next_ordinal = 3,
                    .allocated_ordinals = 2,
                    .state_rows = 2,
                    .live_ordinals = 2,
                    .complete = true,
                },
            },
            .{
                .group_id = 7342,
                .doc_identity = .{
                    .namespace_table_id = 7,
                    .namespace_shard_id = 7342,
                    .namespace_range_id = 7342,
                    .next_ordinal = 4,
                    .allocated_ordinals = 3,
                    .state_rows = 3,
                    .live_ordinals = 3,
                    .complete = true,
                },
            },
        };

        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = graph_indexes_json,
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7341, .table_id = 7, .range_id = 7341, .start_key = "", .end_key = "doc:m" },
                    .{ .group_id = 7342, .table_id = 7, .range_id = 7342, .start_key = "doc:m", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
                .merged_group_statuses = @constCast(statuses[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const FakeRouter = struct {
        fn iface() table_router.HostedGroupRouter {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .local_node_id = localNodeId,
                    .local_status = localStatus,
                    .group_leader_node_id = groupLeaderNodeId,
                    .node_status = nodeStatus,
                    .node_base_uri = nodeBaseUri,
                },
            };
        }

        fn localNodeId(_: *anyopaque) u64 {
            return 1;
        }

        fn localStatus(_: *anyopaque, _: u64) raft_mod.HostedReplicaStatus {
            return .active;
        }

        fn groupLeaderNodeId(_: *anyopaque, _: u64) ?u64 {
            return 1;
        }

        fn nodeStatus(_: *anyopaque, _: u64, _: u64) raft_mod.HostedReplicaStatus {
            return .absent;
        }

        fn nodeBaseUri(_: *anyopaque, _: std.mem.Allocator, _: u64) !?[]u8 {
            return null;
        }
    };

    const ExecutorState = struct {
        fn iface(self: *@This()) http_common.RequestExecutor {
            return .{ .ptr = self, .vtable = &.{ .execute = execute } };
        }

        fn execute(_: *anyopaque, _: std.mem.Allocator, _: http_common.HttpRequest) !http_common.HttpResponse {
            return error.UnexpectedHttpRequest;
        }
    };

    var executor_state = ExecutorState{};
    var hosted = HostedProvisionedTableReadSource.init(
        path,
        FakeCatalog.iface(),
        raft_mod.read_gate.noopReadableLeaseRequester(),
        FakeRouter.iface(),
        executor_state.iface(),
    );
    _ = hosted.withIo(&io_impl);

    const published_generation_needle = try std.fmt.allocPrint(alloc, "\"published_generation\":{d}", .{right_status.published_generation});
    defer alloc.free(published_generation_needle);
    const building_generation_needle = try std.fmt.allocPrint(alloc, "\"building_generation\":{d}", .{active_target_generation});
    defer alloc.free(building_generation_needle);
    const active_metrics = [_][]const u8{ "manual_degree", "pagerank", "eigenvector" };
    for (active_metrics) |metric_name| {
        var response = (try hosted.source().query(alloc, "docs", .{
            .graph_metric_queries = &.{.{
                .name = "central",
                .query = .{
                    .index_name = "graph_idx",
                    .metric_name = metric_name,
                    .top_k = 8,
                    .freshness = .published,
                },
            }},
            .limit = 0,
        }, .read_index)).?;
        defer response.deinit(alloc);

        try std.testing.expect(std.mem.indexOf(u8, response.json, "\"graph_metric_results\"") != null);
        try std.testing.expect(std.mem.indexOf(u8, response.json, metric_name) != null);
        try std.testing.expect(std.mem.indexOf(u8, response.json, "\"doc:b\"") != null);
        try std.testing.expect(std.mem.indexOf(u8, response.json, "\"doc:o\"") != null);
        try std.testing.expect(std.mem.indexOf(u8, response.json, "\"doc:p\"") == null);
        try std.testing.expect(std.mem.indexOf(u8, response.json, "\"state\":\"building\"") != null);
        try std.testing.expect(std.mem.indexOf(u8, response.json, published_generation_needle) != null);
        try std.testing.expect(std.mem.indexOf(u8, response.json, building_generation_needle) != null);

        var rerank_response = (try hosted.source().query(alloc, "docs", .{
            .index_name = "ft_v1",
            .full_text = .{ .match_all = {} },
            .graph_metric_rerank = .{
                .index_name = "graph_idx",
                .metric_name = metric_name,
                .freshness = .published,
                .weight = 1.0,
            },
            .limit = 5,
            .include_stored = false,
            .profile = true,
        }, .read_index)).?;
        defer rerank_response.deinit(alloc);
        try std.testing.expect(std.mem.indexOf(u8, rerank_response.json, "\"_score_details\"") != null);
        try std.testing.expect(std.mem.indexOf(u8, rerank_response.json, "\"graph_metric_rerank\"") != null);
        try std.testing.expect(std.mem.indexOf(u8, rerank_response.json, "\"doc:b\"") != null);
        try std.testing.expect(std.mem.indexOf(u8, rerank_response.json, "\"doc:o\"") != null);
        try std.testing.expect(std.mem.indexOf(u8, rerank_response.json, "\"state\":\"building\"") != null);
        try std.testing.expect(std.mem.indexOf(u8, rerank_response.json, published_generation_needle) != null);
        try std.testing.expect(std.mem.indexOf(u8, rerank_response.json, building_generation_needle) != null);

        const published_metric_reads = [_]graph_query_mod.GraphMetricRead{.{
            .name = metric_name,
            .freshness = .published,
        }};
        const published_graph_query = graph_query_mod.GraphQuery{
            .query_type = .neighbors,
            .index_name = "graph_idx",
            .start_nodes = .{ .keys = &.{ "doc:a", "doc:n" } },
            .params = .{ .edge_types = &.{"cites"}, .direction = .out, .max_results = 8 },
            .metrics = &published_metric_reads,
            .include_metric_status = true,
        };
        var traversal_response = (try hosted.source().query(alloc, "docs", .{
            .query = .{ .match_all = {} },
            .limit = 0,
            .graph_queries = &.{.{ .name = "neighbors", .query = published_graph_query }},
        }, .read_index)).?;
        defer traversal_response.deinit(alloc);

        try std.testing.expect(std.mem.indexOf(u8, traversal_response.json, "\"graph_results\"") != null);
        try std.testing.expect(std.mem.indexOf(u8, traversal_response.json, "\"doc:b\"") != null);
        try std.testing.expect(std.mem.indexOf(u8, traversal_response.json, "\"doc:o\"") != null);
        try std.testing.expect(std.mem.indexOf(u8, traversal_response.json, "\"doc:p\"") == null);
        try std.testing.expect(std.mem.indexOf(u8, traversal_response.json, "\"metric_status\"") != null);
        try std.testing.expect(std.mem.indexOf(u8, traversal_response.json, "\"state\":\"building\"") != null);
        try std.testing.expect(std.mem.indexOf(u8, traversal_response.json, published_generation_needle) != null);
        try std.testing.expect(std.mem.indexOf(u8, traversal_response.json, building_generation_needle) != null);

        const published_metric_orders = [_]graph_query_mod.GraphMetricOrder{.{
            .name = metric_name,
            .freshness = .published,
        }};
        var order_query = published_graph_query;
        order_query.order_by = &published_metric_orders;
        var order_response = (try hosted.source().query(alloc, "docs", .{
            .query = .{ .match_all = {} },
            .limit = 0,
            .graph_queries = &.{.{ .name = "ordered", .query = order_query }},
        }, .read_index)).?;
        defer order_response.deinit(alloc);
        try std.testing.expect(std.mem.indexOf(u8, order_response.json, "\"graph_results\"") != null);
        try std.testing.expect(std.mem.indexOf(u8, order_response.json, "\"state\":\"building\"") != null);
        try std.testing.expect(std.mem.indexOf(u8, order_response.json, published_generation_needle) != null);
        try std.testing.expect(std.mem.indexOf(u8, order_response.json, building_generation_needle) != null);

        const published_metric_filters = [_]graph_query_mod.GraphMetricFilter{.{
            .name = metric_name,
            .op = .gte,
            .value = 0.0,
            .freshness = .published,
        }};
        var filter_query = published_graph_query;
        filter_query.where_metric = &published_metric_filters;
        var filter_response = (try hosted.source().query(alloc, "docs", .{
            .query = .{ .match_all = {} },
            .limit = 0,
            .graph_queries = &.{.{ .name = "filtered", .query = filter_query }},
        }, .read_index)).?;
        defer filter_response.deinit(alloc);
        try std.testing.expect(std.mem.indexOf(u8, filter_response.json, "\"graph_results\"") != null);
        try std.testing.expect(std.mem.indexOf(u8, filter_response.json, "\"doc:b\"") != null);
        try std.testing.expect(std.mem.indexOf(u8, filter_response.json, "\"doc:o\"") != null);
        try std.testing.expect(std.mem.indexOf(u8, filter_response.json, "\"state\":\"building\"") != null);
        try std.testing.expect(std.mem.indexOf(u8, filter_response.json, published_generation_needle) != null);
        try std.testing.expect(std.mem.indexOf(u8, filter_response.json, building_generation_needle) != null);

        try std.testing.expectError(error.MetricStale, hosted.source().query(alloc, "docs", .{
            .graph_metric_queries = &.{.{
                .name = "central",
                .query = .{
                    .index_name = "graph_idx",
                    .metric_name = metric_name,
                    .top_k = 8,
                    .freshness = .fresh,
                },
            }},
            .limit = 0,
        }, .read_index));

        try std.testing.expectError(error.MetricStale, hosted.source().query(alloc, "docs", .{
            .index_name = "ft_v1",
            .full_text = .{ .match_all = {} },
            .graph_metric_rerank = .{
                .index_name = "graph_idx",
                .metric_name = metric_name,
                .freshness = .fresh,
                .weight = 1.0,
            },
            .limit = 5,
            .include_stored = false,
        }, .read_index));

        const fresh_metric_reads = [_]graph_query_mod.GraphMetricRead{.{
            .name = metric_name,
            .freshness = .fresh,
        }};
        var fresh_projection_query = published_graph_query;
        fresh_projection_query.metrics = &fresh_metric_reads;
        try std.testing.expectError(error.MetricStale, hosted.source().query(alloc, "docs", .{
            .query = .{ .match_all = {} },
            .limit = 0,
            .graph_queries = &.{.{ .name = "fresh_neighbors", .query = fresh_projection_query }},
        }, .read_index));

        const fresh_metric_orders = [_]graph_query_mod.GraphMetricOrder{.{
            .name = metric_name,
            .freshness = .fresh,
        }};
        var fresh_order_query = published_graph_query;
        fresh_order_query.order_by = &fresh_metric_orders;
        try std.testing.expectError(error.MetricStale, hosted.source().query(alloc, "docs", .{
            .query = .{ .match_all = {} },
            .limit = 0,
            .graph_queries = &.{.{ .name = "fresh_ordered", .query = fresh_order_query }},
        }, .read_index));

        const fresh_metric_filters = [_]graph_query_mod.GraphMetricFilter{.{
            .name = metric_name,
            .op = .gte,
            .value = 0.0,
            .freshness = .fresh,
        }};
        var fresh_filter_query = published_graph_query;
        fresh_filter_query.where_metric = &fresh_metric_filters;
        try std.testing.expectError(error.MetricStale, hosted.source().query(alloc, "docs", .{
            .query = .{ .match_all = {} },
            .limit = 0,
            .graph_queries = &.{.{ .name = "fresh_filtered", .query = fresh_filter_query }},
        }, .read_index));
    }
}

test "hosted cross-range graph metric fan-in merges nonuniform promotion shard layout" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-hosted-cross-range-graph-metric-promotion-merge";
    const shard_count = 8;
    const group_ids = [_]u64{ 7301, 7302, 7303, 7304, 7305, 7306, 7307, 7308 };
    const prefixes = [_][]const u8{ "a", "b", "c", "d", "e", "f", "g", "h" };
    const source_counts = [_]usize{ 1, 2, 3, 1, 2, 3, 1, 2 };

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const graph_indexes_json =
        \\{"graph_idx":{"type":"graph","edge_types":[{"name":"cites"}],"metrics":{"manual_degree":{"enabled":true,"kind":"degree","refresh":"manual","edge_filter":{"types":["cites"]}},"pagerank":{"enabled":true,"kind":"pagerank","refresh":"manual","max_iterations":2,"tolerance":0.000001,"edge_filter":{"types":["cites"]}},"eigenvector":{"enabled":true,"kind":"eigenvector","refresh":"manual","max_iterations":2,"tolerance":0.000001,"edge_filter":{"types":["cites"]}}}}}
    ;
    const graph_config_json =
        \\{"edge_types":[{"name":"cites"}],"metrics":{"manual_degree":{"enabled":true,"kind":"degree","refresh":"manual","edge_filter":{"types":["cites"]}},"pagerank":{"enabled":true,"kind":"pagerank","refresh":"manual","max_iterations":2,"tolerance":0.000001,"edge_filter":{"types":["cites"]}},"eigenvector":{"enabled":true,"kind":"eigenvector","refresh":"manual","max_iterations":2,"tolerance":0.000001,"edge_filter":{"types":["cites"]}}}}
    ;
    const metric_names = [_][]const u8{ "manual_degree", "pagerank", "eigenvector" };

    var db_paths: [shard_count][]u8 = undefined;
    var db_path_count: usize = 0;
    defer {
        for (db_paths[0..db_path_count]) |db_path| alloc.free(db_path);
    }
    var dbs: [shard_count]db_mod.DB = undefined;
    var db_count: usize = 0;
    defer {
        for (dbs[0..db_count]) |*db| db.close();
    }

    var published_generation: u64 = 0;
    for (group_ids, prefixes, source_counts, 0..) |group_id, prefix, source_count, shard_index| {
        db_paths[shard_index] = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, group_id);
        db_path_count += 1;
        dbs[shard_index] = try db_mod.DB.open(alloc, db_paths[shard_index], .{
            .start_index_workers = false,
            .identity_namespace = .{ .table_id = 7, .shard_id = group_id, .range_id = group_id },
        });
        db_count += 1;
        try dbs[shard_index].addIndex(.{ .name = "graph_idx", .kind = .graph, .config_json = graph_config_json });

        var writes: [4]db_mod.types.BatchWrite = undefined;
        var write_count: usize = 0;
        var owned: [8][]u8 = undefined;
        var owned_count: usize = 0;
        defer {
            for (owned[0..owned_count]) |item| alloc.free(item);
        }

        const sink_key = try std.fmt.allocPrint(alloc, "doc:{s}:target", .{prefix});
        owned[owned_count] = sink_key;
        owned_count += 1;
        const sink_value = try std.fmt.allocPrint(alloc, "{{\"title\":\"target {s}\"}}", .{prefix});
        owned[owned_count] = sink_value;
        owned_count += 1;
        writes[write_count] = .{ .key = sink_key, .value = sink_value };
        write_count += 1;

        for (0..source_count) |source_index| {
            const source_key = try std.fmt.allocPrint(alloc, "doc:{s}:source:{d}", .{ prefix, source_index });
            owned[owned_count] = source_key;
            owned_count += 1;
            const source_value = try std.fmt.allocPrint(
                alloc,
                "{{\"title\":\"source {s}-{d}\",\"_edges\":{{\"graph_idx\":{{\"cites\":[{{\"target\":\"{s}\",\"weight\":1.0}}]}}}}}}",
                .{ prefix, source_index, sink_key },
            );
            owned[owned_count] = source_value;
            owned_count += 1;
            writes[write_count] = .{ .key = source_key, .value = source_value };
            write_count += 1;
        }

        try dbs[shard_index].batch(.{
            .writes = writes[0..write_count],
            .sync_level = .write,
        });
        try dbs[shard_index].runUntilIdle();
        for (metric_names) |metric_name| {
            var status = try dbs[shard_index].refreshGraphMetric(alloc, "graph_idx", metric_name);
            defer status.deinit(alloc);
            try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, status.state);
            if (published_generation == 0) {
                published_generation = status.published_generation;
            } else {
                try std.testing.expectEqual(published_generation, status.published_generation);
            }
        }
    }

    const active_shard_indices = [_]usize{ 1, 3, 5, 7 };
    for (active_shard_indices) |shard_index| {
        const prefix = prefixes[shard_index];
        const group_id = group_ids[shard_index];

        var writes: [4]db_mod.types.BatchWrite = undefined;
        var owned: [8][]u8 = undefined;
        var owned_count: usize = 0;
        defer {
            for (owned[0..owned_count]) |item| alloc.free(item);
        }

        const active_target_key = try std.fmt.allocPrint(alloc, "doc:{s}:active-target", .{prefix});
        owned[owned_count] = active_target_key;
        owned_count += 1;
        const active_target_value = try std.fmt.allocPrint(alloc, "{{\"title\":\"active target {s}\"}}", .{prefix});
        owned[owned_count] = active_target_value;
        owned_count += 1;
        writes[0] = .{ .key = active_target_key, .value = active_target_value };

        for (0..3) |source_index| {
            const source_key = try std.fmt.allocPrint(alloc, "doc:{s}:active-source:{d}", .{ prefix, source_index });
            owned[owned_count] = source_key;
            owned_count += 1;
            const source_value = try std.fmt.allocPrint(
                alloc,
                "{{\"title\":\"active source {s}-{d}\",\"_edges\":{{\"graph_idx\":{{\"cites\":[{{\"target\":\"{s}\",\"weight\":1.0}}]}}}}}}",
                .{ prefix, source_index, active_target_key },
            );
            owned[owned_count] = source_value;
            owned_count += 1;
            writes[source_index + 1] = .{ .key = source_key, .value = source_value };
        }

        try dbs[shard_index].batch(.{
            .writes = writes[0..],
            .sync_level = .full_index,
        });
        try dbs[shard_index].runUntilIdle();

        const graph_entry = dbs[shard_index].core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        const target_generation = graph_entry.index.edge_generation;
        try std.testing.expect(target_generation > published_generation);
        for (metric_names) |metric_name| {
            var building = try graph_entry.index.ensureGraphMetricPlannedBuild(metric_name, target_generation);
            defer building.deinit(alloc);
            try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.building, building.state);
            try std.testing.expectEqual(target_generation, building.building_generation);

            const prepare = try graph_entry.index.runGraphMetricPlannedWorkerPageStepForMetric(metric_name, "worker-prepare");
            try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.prepare_generation, prepare.phase);
            try std.testing.expect(prepare.claimed_page);
            try std.testing.expect(prepare.completed_page);

            const advance_prepare = try graph_entry.index.runGraphMetricPlannedCoordinatorStepForMetric(metric_name);
            try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.prepare_generation, advance_prepare.phase);
            try std.testing.expect(advance_prepare.advanced_phase);

            const scan = try graph_entry.index.runGraphMetricPlannedWorkerPageStepForMetric(metric_name, "worker-scan");
            try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.scan_edges_and_out_degree, scan.phase);
            try std.testing.expect(scan.claimed_page);
            try std.testing.expect(scan.completed_page);
        }

        _ = group_id;
    }

    const FakeCatalog = struct {
        const statuses = [_]metadata_reconciler.MergedGroupStatus{
            .{ .group_id = 7301, .doc_identity = .{ .namespace_table_id = 7, .namespace_shard_id = 7301, .namespace_range_id = 7301, .next_ordinal = 3, .allocated_ordinals = 2, .state_rows = 2, .live_ordinals = 2, .complete = true } },
            .{ .group_id = 7302, .doc_identity = .{ .namespace_table_id = 7, .namespace_shard_id = 7302, .namespace_range_id = 7302, .next_ordinal = 8, .allocated_ordinals = 7, .state_rows = 7, .live_ordinals = 7, .complete = true } },
            .{ .group_id = 7303, .doc_identity = .{ .namespace_table_id = 7, .namespace_shard_id = 7303, .namespace_range_id = 7303, .next_ordinal = 5, .allocated_ordinals = 4, .state_rows = 4, .live_ordinals = 4, .complete = true } },
            .{ .group_id = 7304, .doc_identity = .{ .namespace_table_id = 7, .namespace_shard_id = 7304, .namespace_range_id = 7304, .next_ordinal = 7, .allocated_ordinals = 6, .state_rows = 6, .live_ordinals = 6, .complete = true } },
            .{ .group_id = 7305, .doc_identity = .{ .namespace_table_id = 7, .namespace_shard_id = 7305, .namespace_range_id = 7305, .next_ordinal = 4, .allocated_ordinals = 3, .state_rows = 3, .live_ordinals = 3, .complete = true } },
            .{ .group_id = 7306, .doc_identity = .{ .namespace_table_id = 7, .namespace_shard_id = 7306, .namespace_range_id = 7306, .next_ordinal = 9, .allocated_ordinals = 8, .state_rows = 8, .live_ordinals = 8, .complete = true } },
            .{ .group_id = 7307, .doc_identity = .{ .namespace_table_id = 7, .namespace_shard_id = 7307, .namespace_range_id = 7307, .next_ordinal = 3, .allocated_ordinals = 2, .state_rows = 2, .live_ordinals = 2, .complete = true } },
            .{ .group_id = 7308, .doc_identity = .{ .namespace_table_id = 7, .namespace_shard_id = 7308, .namespace_range_id = 7308, .next_ordinal = 8, .allocated_ordinals = 7, .state_rows = 7, .live_ordinals = 7, .complete = true } },
        };

        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = graph_indexes_json,
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7301, .table_id = 7, .range_id = 7301, .start_key = "", .end_key = "doc:b:" },
                    .{ .group_id = 7302, .table_id = 7, .range_id = 7302, .start_key = "doc:b:", .end_key = "doc:c:" },
                    .{ .group_id = 7303, .table_id = 7, .range_id = 7303, .start_key = "doc:c:", .end_key = "doc:d:" },
                    .{ .group_id = 7304, .table_id = 7, .range_id = 7304, .start_key = "doc:d:", .end_key = "doc:e:" },
                    .{ .group_id = 7305, .table_id = 7, .range_id = 7305, .start_key = "doc:e:", .end_key = "doc:f:" },
                    .{ .group_id = 7306, .table_id = 7, .range_id = 7306, .start_key = "doc:f:", .end_key = "doc:g:" },
                    .{ .group_id = 7307, .table_id = 7, .range_id = 7307, .start_key = "doc:g:", .end_key = "doc:h:" },
                    .{ .group_id = 7308, .table_id = 7, .range_id = 7308, .start_key = "doc:h:", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
                .merged_group_statuses = @constCast(statuses[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const FakeRouter = struct {
        fn iface() table_router.HostedGroupRouter {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .local_node_id = localNodeId,
                    .local_status = localStatus,
                    .group_leader_node_id = groupLeaderNodeId,
                    .node_status = nodeStatus,
                    .node_base_uri = nodeBaseUri,
                },
            };
        }

        fn localNodeId(_: *anyopaque) u64 {
            return 1;
        }

        fn localStatus(_: *anyopaque, _: u64) raft_mod.HostedReplicaStatus {
            return .active;
        }

        fn groupLeaderNodeId(_: *anyopaque, _: u64) ?u64 {
            return 1;
        }

        fn nodeStatus(_: *anyopaque, _: u64, _: u64) raft_mod.HostedReplicaStatus {
            return .absent;
        }

        fn nodeBaseUri(_: *anyopaque, _: std.mem.Allocator, _: u64) !?[]u8 {
            return null;
        }
    };

    const ExecutorState = struct {
        fn iface(self: *@This()) http_common.RequestExecutor {
            return .{ .ptr = self, .vtable = &.{ .execute = execute } };
        }

        fn execute(_: *anyopaque, _: std.mem.Allocator, _: http_common.HttpRequest) !http_common.HttpResponse {
            return error.UnexpectedHttpRequest;
        }
    };

    var executor_state = ExecutorState{};
    var hosted = HostedProvisionedTableReadSource.init(
        path,
        FakeCatalog.iface(),
        raft_mod.read_gate.noopReadableLeaseRequester(),
        FakeRouter.iface(),
        executor_state.iface(),
    );
    _ = hosted.withIo(&io_impl);

    const published_generation_needle = try std.fmt.allocPrint(alloc, "\"published_generation\":{d}", .{published_generation});
    defer alloc.free(published_generation_needle);
    for (metric_names) |metric_name| {
        var response = (try hosted.source().query(alloc, "docs", .{
            .graph_metric_queries = &.{.{
                .name = "central",
                .query = .{
                    .index_name = "graph_idx",
                    .metric_name = metric_name,
                    .top_k = 32,
                    .freshness = .published,
                },
            }},
            .limit = 0,
        }, .read_index)).?;
        defer response.deinit(alloc);

        try std.testing.expect(std.mem.indexOf(u8, response.json, "\"graph_metric_results\"") != null);
        try std.testing.expect(std.mem.indexOf(u8, response.json, metric_name) != null);
        for (prefixes) |prefix| {
            const needle = try std.fmt.allocPrint(alloc, "\"doc:{s}:target\"", .{prefix});
            defer alloc.free(needle);
            try std.testing.expect(std.mem.indexOf(u8, response.json, needle) != null);
        }
        for (active_shard_indices) |shard_index| {
            const active_needle = try std.fmt.allocPrint(alloc, "\"doc:{s}:active-target\"", .{prefixes[shard_index]});
            defer alloc.free(active_needle);
            try std.testing.expect(std.mem.indexOf(u8, response.json, active_needle) == null);
        }
        try std.testing.expect(std.mem.indexOf(u8, response.json, "\"state\":\"building\"") != null);
        try std.testing.expect(std.mem.indexOf(u8, response.json, published_generation_needle) != null);

        try std.testing.expectError(error.MetricStale, hosted.source().query(alloc, "docs", .{
            .graph_metric_queries = &.{.{
                .name = "central",
                .query = .{
                    .index_name = "graph_idx",
                    .metric_name = metric_name,
                    .top_k = 32,
                    .freshness = .fresh,
                },
            }},
            .limit = 0,
        }, .read_index));
    }
}

test "hosted cross-range graph metric fan-in merges compatible hits pair" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-hosted-cross-range-graph-metric-hits-pair";
    const shard_count = 8;
    const group_ids = [_]u64{ 7311, 7312, 7313, 7314, 7315, 7316, 7317, 7318 };
    const prefixes = [_][]const u8{ "j", "k", "l", "m", "n", "o", "p", "q" };
    const hub_counts = [_]usize{ 1, 2, 3, 2, 1, 3, 2, 1 };

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const graph_indexes_json =
        \\{"ft_v1":{"type":"full_text","store":true},"graph_idx":{"type":"graph","edge_types":[{"name":"cites"}],"metrics":{"hits_authority":{"enabled":true,"kind":"hits_authority","refresh":"manual","max_iterations":1,"tolerance":0.000001,"edge_filter":{"types":["cites"]}},"hits_hub":{"enabled":true,"kind":"hits_hub","refresh":"manual","max_iterations":1,"tolerance":0.000001,"edge_filter":{"types":["cites"]}}}}}
    ;
    const graph_config_json =
        \\{"edge_types":[{"name":"cites"}],"metrics":{"hits_authority":{"enabled":true,"kind":"hits_authority","refresh":"manual","max_iterations":1,"tolerance":0.000001,"edge_filter":{"types":["cites"]}},"hits_hub":{"enabled":true,"kind":"hits_hub","refresh":"manual","max_iterations":1,"tolerance":0.000001,"edge_filter":{"types":["cites"]}}}}
    ;

    var db_paths: [shard_count][]u8 = undefined;
    var db_path_count: usize = 0;
    defer {
        for (db_paths[0..db_path_count]) |db_path| alloc.free(db_path);
    }
    var dbs: [shard_count]db_mod.DB = undefined;
    var db_count: usize = 0;
    defer {
        for (dbs[0..db_count]) |*db| db.close();
    }

    var published_generation: u64 = 0;
    for (group_ids, prefixes, hub_counts, 0..) |group_id, prefix, hub_count, shard_index| {
        db_paths[shard_index] = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, group_id);
        db_path_count += 1;
        dbs[shard_index] = try db_mod.DB.open(alloc, db_paths[shard_index], .{
            .start_index_workers = false,
            .identity_namespace = .{ .table_id = 7, .shard_id = group_id, .range_id = group_id },
        });
        db_count += 1;
        try dbs[shard_index].addIndex(.{ .name = "ft_v1", .kind = .full_text, .config_json = "{\"store\":true}" });
        try dbs[shard_index].addIndex(.{ .name = "graph_idx", .kind = .graph, .config_json = graph_config_json });

        var writes: [4]db_mod.types.BatchWrite = undefined;
        var write_count: usize = 0;
        var owned: [8][]u8 = undefined;
        var owned_count: usize = 0;
        defer {
            for (owned[0..owned_count]) |item| alloc.free(item);
        }

        const authority_key = try std.fmt.allocPrint(alloc, "doc:{s}:authority", .{prefix});
        owned[owned_count] = authority_key;
        owned_count += 1;
        const authority_value = try std.fmt.allocPrint(alloc, "{{\"title\":\"authority {s}\"}}", .{prefix});
        owned[owned_count] = authority_value;
        owned_count += 1;
        writes[write_count] = .{ .key = authority_key, .value = authority_value };
        write_count += 1;

        for (0..hub_count) |hub_index| {
            const hub_key = try std.fmt.allocPrint(alloc, "doc:{s}:hub:{d}", .{ prefix, hub_index });
            owned[owned_count] = hub_key;
            owned_count += 1;
            const hub_value = try std.fmt.allocPrint(
                alloc,
                "{{\"title\":\"hub {s}-{d}\",\"_edges\":{{\"graph_idx\":{{\"cites\":[{{\"target\":\"{s}\",\"weight\":1.0}}]}}}}}}",
                .{ prefix, hub_index, authority_key },
            );
            owned[owned_count] = hub_value;
            owned_count += 1;
            writes[write_count] = .{ .key = hub_key, .value = hub_value };
            write_count += 1;
        }

        try dbs[shard_index].batch(.{
            .writes = writes[0..write_count],
            .sync_level = .full_index,
        });
        try dbs[shard_index].runUntilIdle();
        var authority_status = try dbs[shard_index].refreshGraphMetric(alloc, "graph_idx", "hits_authority");
        defer authority_status.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, authority_status.state);
        var hub_status = try (dbs[shard_index].core.graphIndex("graph_idx") orelse return error.IndexNotFound).index.graphMetricStatus("hits_hub");
        defer hub_status.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, hub_status.state);
        try std.testing.expectEqual(authority_status.published_generation, hub_status.published_generation);
        if (published_generation == 0) {
            published_generation = authority_status.published_generation;
        } else {
            try std.testing.expectEqual(published_generation, authority_status.published_generation);
        }
    }

    var active_target_generation: u64 = 0;
    const active_shard_indices = [_]usize{ 1, 3, 5, 7 };
    for (active_shard_indices) |shard_index| {
        const prefix = prefixes[shard_index];

        var writes: [4]db_mod.types.BatchWrite = undefined;
        var owned: [8][]u8 = undefined;
        var owned_count: usize = 0;
        defer {
            for (owned[0..owned_count]) |item| alloc.free(item);
        }

        const active_authority_key = try std.fmt.allocPrint(alloc, "doc:{s}:active-authority", .{prefix});
        owned[owned_count] = active_authority_key;
        owned_count += 1;
        const active_authority_value = try std.fmt.allocPrint(alloc, "{{\"title\":\"active authority {s}\"}}", .{prefix});
        owned[owned_count] = active_authority_value;
        owned_count += 1;
        writes[0] = .{ .key = active_authority_key, .value = active_authority_value };

        for (0..3) |hub_index| {
            const active_hub_key = try std.fmt.allocPrint(alloc, "doc:{s}:active-hub:{d}", .{ prefix, hub_index });
            owned[owned_count] = active_hub_key;
            owned_count += 1;
            const active_hub_value = try std.fmt.allocPrint(
                alloc,
                "{{\"title\":\"active hub {s}-{d}\",\"_edges\":{{\"graph_idx\":{{\"cites\":[{{\"target\":\"{s}\",\"weight\":1.0}}]}}}}}}",
                .{ prefix, hub_index, active_authority_key },
            );
            owned[owned_count] = active_hub_value;
            owned_count += 1;
            writes[hub_index + 1] = .{ .key = active_hub_key, .value = active_hub_value };
        }

        try dbs[shard_index].batch(.{
            .writes = writes[0..],
            .sync_level = .full_index,
        });
        try dbs[shard_index].runUntilIdle();

        const graph_entry = dbs[shard_index].core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        const target_generation = graph_entry.index.edge_generation;
        try std.testing.expect(target_generation > published_generation);
        if (active_target_generation == 0) {
            active_target_generation = target_generation;
        } else {
            try std.testing.expectEqual(active_target_generation, target_generation);
        }

        var building = try graph_entry.index.ensureGraphMetricPlannedBuild("hits_authority", target_generation);
        defer building.deinit(alloc);
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.building, building.state);
        try std.testing.expectEqual(target_generation, building.building_generation);

        const prepare = try graph_entry.index.runGraphMetricPlannedWorkerPageStepForMetric("hits_authority", "worker-prepare");
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.prepare_generation, prepare.phase);
        try std.testing.expect(prepare.claimed_page);
        try std.testing.expect(prepare.completed_page);

        const advance_prepare = try graph_entry.index.runGraphMetricPlannedCoordinatorStepForMetric("hits_authority");
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.prepare_generation, advance_prepare.phase);
        try std.testing.expect(advance_prepare.advanced_phase);

        const scan = try graph_entry.index.runGraphMetricPlannedWorkerPageStepForMetric("hits_authority", "worker-scan");
        try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricBuildPhase.scan_edges_and_out_degree, scan.phase);
        try std.testing.expect(scan.claimed_page);
        try std.testing.expect(scan.completed_page);
    }

    const FakeCatalog = struct {
        const statuses = [_]metadata_reconciler.MergedGroupStatus{
            .{ .group_id = 7311, .doc_identity = .{ .namespace_table_id = 7, .namespace_shard_id = 7311, .namespace_range_id = 7311, .next_ordinal = 3, .allocated_ordinals = 2, .state_rows = 2, .live_ordinals = 2, .complete = true } },
            .{ .group_id = 7312, .doc_identity = .{ .namespace_table_id = 7, .namespace_shard_id = 7312, .namespace_range_id = 7312, .next_ordinal = 8, .allocated_ordinals = 7, .state_rows = 7, .live_ordinals = 7, .complete = true } },
            .{ .group_id = 7313, .doc_identity = .{ .namespace_table_id = 7, .namespace_shard_id = 7313, .namespace_range_id = 7313, .next_ordinal = 5, .allocated_ordinals = 4, .state_rows = 4, .live_ordinals = 4, .complete = true } },
            .{ .group_id = 7314, .doc_identity = .{ .namespace_table_id = 7, .namespace_shard_id = 7314, .namespace_range_id = 7314, .next_ordinal = 8, .allocated_ordinals = 7, .state_rows = 7, .live_ordinals = 7, .complete = true } },
            .{ .group_id = 7315, .doc_identity = .{ .namespace_table_id = 7, .namespace_shard_id = 7315, .namespace_range_id = 7315, .next_ordinal = 3, .allocated_ordinals = 2, .state_rows = 2, .live_ordinals = 2, .complete = true } },
            .{ .group_id = 7316, .doc_identity = .{ .namespace_table_id = 7, .namespace_shard_id = 7316, .namespace_range_id = 7316, .next_ordinal = 9, .allocated_ordinals = 8, .state_rows = 8, .live_ordinals = 8, .complete = true } },
            .{ .group_id = 7317, .doc_identity = .{ .namespace_table_id = 7, .namespace_shard_id = 7317, .namespace_range_id = 7317, .next_ordinal = 4, .allocated_ordinals = 3, .state_rows = 3, .live_ordinals = 3, .complete = true } },
            .{ .group_id = 7318, .doc_identity = .{ .namespace_table_id = 7, .namespace_shard_id = 7318, .namespace_range_id = 7318, .next_ordinal = 7, .allocated_ordinals = 6, .state_rows = 6, .live_ordinals = 6, .complete = true } },
        };

        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = graph_indexes_json,
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7311, .table_id = 7, .range_id = 7311, .start_key = "", .end_key = "doc:k:" },
                    .{ .group_id = 7312, .table_id = 7, .range_id = 7312, .start_key = "doc:k:", .end_key = "doc:l:" },
                    .{ .group_id = 7313, .table_id = 7, .range_id = 7313, .start_key = "doc:l:", .end_key = "doc:m:" },
                    .{ .group_id = 7314, .table_id = 7, .range_id = 7314, .start_key = "doc:m:", .end_key = "doc:n:" },
                    .{ .group_id = 7315, .table_id = 7, .range_id = 7315, .start_key = "doc:n:", .end_key = "doc:o:" },
                    .{ .group_id = 7316, .table_id = 7, .range_id = 7316, .start_key = "doc:o:", .end_key = "doc:p:" },
                    .{ .group_id = 7317, .table_id = 7, .range_id = 7317, .start_key = "doc:p:", .end_key = "doc:q:" },
                    .{ .group_id = 7318, .table_id = 7, .range_id = 7318, .start_key = "doc:q:", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
                .merged_group_statuses = @constCast(statuses[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const FakeRouter = struct {
        fn iface() table_router.HostedGroupRouter {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .local_node_id = localNodeId,
                    .local_status = localStatus,
                    .group_leader_node_id = groupLeaderNodeId,
                    .node_status = nodeStatus,
                    .node_base_uri = nodeBaseUri,
                },
            };
        }

        fn localNodeId(_: *anyopaque) u64 {
            return 1;
        }

        fn localStatus(_: *anyopaque, _: u64) raft_mod.HostedReplicaStatus {
            return .active;
        }

        fn groupLeaderNodeId(_: *anyopaque, _: u64) ?u64 {
            return 1;
        }

        fn nodeStatus(_: *anyopaque, _: u64, _: u64) raft_mod.HostedReplicaStatus {
            return .absent;
        }

        fn nodeBaseUri(_: *anyopaque, _: std.mem.Allocator, _: u64) !?[]u8 {
            return null;
        }
    };

    const ExecutorState = struct {
        fn iface(self: *@This()) http_common.RequestExecutor {
            return .{ .ptr = self, .vtable = &.{ .execute = execute } };
        }

        fn execute(_: *anyopaque, _: std.mem.Allocator, _: http_common.HttpRequest) !http_common.HttpResponse {
            return error.UnexpectedHttpRequest;
        }
    };

    var executor_state = ExecutorState{};
    var hosted = HostedProvisionedTableReadSource.init(
        path,
        FakeCatalog.iface(),
        raft_mod.read_gate.noopReadableLeaseRequester(),
        FakeRouter.iface(),
        executor_state.iface(),
    );
    _ = hosted.withIo(&io_impl);

    const published_generation_needle = try std.fmt.allocPrint(alloc, "\"published_generation\":{d}", .{published_generation});
    defer alloc.free(published_generation_needle);
    const building_generation_needle = try std.fmt.allocPrint(alloc, "\"building_generation\":{d}", .{active_target_generation});
    defer alloc.free(building_generation_needle);
    var response = (try hosted.source().query(alloc, "docs", .{
        .graph_metric_queries = &.{
            .{
                .name = "authority",
                .query = .{
                    .index_name = "graph_idx",
                    .metric_name = "hits_authority",
                    .top_k = 16,
                    .freshness = .published,
                },
            },
            .{
                .name = "hub",
                .query = .{
                    .index_name = "graph_idx",
                    .metric_name = "hits_hub",
                    .top_k = 16,
                    .freshness = .published,
                },
            },
        },
        .limit = 0,
    }, .read_index)).?;
    defer response.deinit(alloc);

    try std.testing.expect(std.mem.indexOf(u8, response.json, "\"graph_metric_results\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, response.json, "\"hits_authority\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, response.json, "\"hits_hub\"") != null);
    for (prefixes) |prefix| {
        const authority_needle = try std.fmt.allocPrint(alloc, "\"doc:{s}:authority\"", .{prefix});
        defer alloc.free(authority_needle);
        try std.testing.expect(std.mem.indexOf(u8, response.json, authority_needle) != null);
        const hub_needle = try std.fmt.allocPrint(alloc, "\"doc:{s}:hub:0\"", .{prefix});
        defer alloc.free(hub_needle);
        try std.testing.expect(std.mem.indexOf(u8, response.json, hub_needle) != null);
    }
    for (active_shard_indices) |shard_index| {
        const active_authority_needle = try std.fmt.allocPrint(alloc, "\"doc:{s}:active-authority\"", .{prefixes[shard_index]});
        defer alloc.free(active_authority_needle);
        try std.testing.expect(std.mem.indexOf(u8, response.json, active_authority_needle) == null);
        const active_hub_needle = try std.fmt.allocPrint(alloc, "\"doc:{s}:active-hub:0\"", .{prefixes[shard_index]});
        defer alloc.free(active_hub_needle);
        try std.testing.expect(std.mem.indexOf(u8, response.json, active_hub_needle) == null);
    }
    try std.testing.expect(std.mem.indexOf(u8, response.json, "\"state\":\"building\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, response.json, published_generation_needle) != null);
    try std.testing.expect(std.mem.indexOf(u8, response.json, building_generation_needle) != null);

    try std.testing.expectError(error.MetricStale, hosted.source().query(alloc, "docs", .{
        .graph_metric_queries = &.{
            .{
                .name = "authority",
                .query = .{
                    .index_name = "graph_idx",
                    .metric_name = "hits_authority",
                    .top_k = 16,
                    .freshness = .fresh,
                },
            },
            .{
                .name = "hub",
                .query = .{
                    .index_name = "graph_idx",
                    .metric_name = "hits_hub",
                    .top_k = 16,
                    .freshness = .fresh,
                },
            },
        },
        .limit = 0,
    }, .read_index));

    var rerank_response = (try hosted.source().query(alloc, "docs", .{
        .index_name = "ft_v1",
        .full_text = .{ .match_all = {} },
        .graph_metric_rerank = .{
            .index_name = "graph_idx",
            .metric_name = "hits_authority",
            .freshness = .published,
            .weight = 1.0,
        },
        .limit = 32,
        .include_stored = false,
        .profile = true,
    }, .read_index)).?;
    defer rerank_response.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, rerank_response.json, "\"_score_details\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, rerank_response.json, "\"graph_metric_rerank\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, rerank_response.json, "\"hits_authority\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, rerank_response.json, "\"state\":\"building\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, rerank_response.json, published_generation_needle) != null);
    try std.testing.expect(std.mem.indexOf(u8, rerank_response.json, building_generation_needle) != null);

    try std.testing.expectError(error.MetricStale, hosted.source().query(alloc, "docs", .{
        .index_name = "ft_v1",
        .full_text = .{ .match_all = {} },
        .graph_metric_rerank = .{
            .index_name = "graph_idx",
            .metric_name = "hits_authority",
            .freshness = .fresh,
            .weight = 1.0,
        },
        .limit = 32,
        .include_stored = false,
    }, .read_index));

    const hits_metric_reads = [_]graph_query_mod.GraphMetricRead{
        .{ .name = "hits_authority", .freshness = .published },
        .{ .name = "hits_hub", .freshness = .published },
    };
    const traversal_query = graph_query_mod.GraphQuery{
        .query_type = .neighbors,
        .index_name = "graph_idx",
        .start_nodes = .{ .keys = &.{ "doc:j:hub:0", "doc:k:hub:0", "doc:l:hub:0", "doc:m:hub:0", "doc:n:hub:0", "doc:o:hub:0", "doc:p:hub:0", "doc:q:hub:0" } },
        .params = .{ .edge_types = &.{"cites"}, .direction = .out, .max_results = 16 },
        .metrics = &hits_metric_reads,
        .include_metric_status = true,
    };
    var traversal_response = (try hosted.source().query(alloc, "docs", .{
        .query = .{ .match_all = {} },
        .limit = 0,
        .graph_queries = &.{.{ .name = "hits_neighbors", .query = traversal_query }},
    }, .read_index)).?;
    defer traversal_response.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, traversal_response.json, "\"graph_results\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, traversal_response.json, "\"metric_status\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, traversal_response.json, "\"hits_authority\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, traversal_response.json, "\"hits_hub\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, traversal_response.json, "\"state\":\"building\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, traversal_response.json, published_generation_needle) != null);
    try std.testing.expect(std.mem.indexOf(u8, traversal_response.json, building_generation_needle) != null);
    for (prefixes) |prefix| {
        const authority_needle = try std.fmt.allocPrint(alloc, "\"doc:{s}:authority\"", .{prefix});
        defer alloc.free(authority_needle);
        try std.testing.expect(std.mem.indexOf(u8, traversal_response.json, authority_needle) != null);
    }
    for (active_shard_indices) |shard_index| {
        const active_authority_needle = try std.fmt.allocPrint(alloc, "\"doc:{s}:active-authority\"", .{prefixes[shard_index]});
        defer alloc.free(active_authority_needle);
        try std.testing.expect(std.mem.indexOf(u8, traversal_response.json, active_authority_needle) == null);
    }

    const hits_metric_orders = [_]graph_query_mod.GraphMetricOrder{.{
        .name = "hits_authority",
        .freshness = .published,
    }};
    var ordered_traversal_query = traversal_query;
    ordered_traversal_query.order_by = &hits_metric_orders;
    var ordered_traversal_response = (try hosted.source().query(alloc, "docs", .{
        .query = .{ .match_all = {} },
        .limit = 0,
        .graph_queries = &.{.{ .name = "ordered_hits_neighbors", .query = ordered_traversal_query }},
    }, .read_index)).?;
    defer ordered_traversal_response.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, ordered_traversal_response.json, "\"graph_results\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, ordered_traversal_response.json, "\"hits_authority\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, ordered_traversal_response.json, "\"state\":\"building\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, ordered_traversal_response.json, published_generation_needle) != null);
    try std.testing.expect(std.mem.indexOf(u8, ordered_traversal_response.json, building_generation_needle) != null);

    const hits_metric_filters = [_]graph_query_mod.GraphMetricFilter{.{
        .name = "hits_authority",
        .op = .gte,
        .value = 0.0,
        .freshness = .published,
    }};
    var filtered_traversal_query = traversal_query;
    filtered_traversal_query.where_metric = &hits_metric_filters;
    var filtered_traversal_response = (try hosted.source().query(alloc, "docs", .{
        .query = .{ .match_all = {} },
        .limit = 0,
        .graph_queries = &.{.{ .name = "filtered_hits_neighbors", .query = filtered_traversal_query }},
    }, .read_index)).?;
    defer filtered_traversal_response.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, filtered_traversal_response.json, "\"graph_results\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, filtered_traversal_response.json, "\"hits_authority\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, filtered_traversal_response.json, "\"state\":\"building\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, filtered_traversal_response.json, published_generation_needle) != null);
    try std.testing.expect(std.mem.indexOf(u8, filtered_traversal_response.json, building_generation_needle) != null);

    const fresh_hits_metric_reads = [_]graph_query_mod.GraphMetricRead{
        .{ .name = "hits_authority", .freshness = .fresh },
        .{ .name = "hits_hub", .freshness = .fresh },
    };
    var fresh_traversal_query = traversal_query;
    fresh_traversal_query.metrics = &fresh_hits_metric_reads;
    try std.testing.expectError(error.MetricStale, hosted.source().query(alloc, "docs", .{
        .query = .{ .match_all = {} },
        .limit = 0,
        .graph_queries = &.{.{ .name = "fresh_hits_neighbors", .query = fresh_traversal_query }},
    }, .read_index));

    const fresh_hits_metric_orders = [_]graph_query_mod.GraphMetricOrder{.{
        .name = "hits_authority",
        .freshness = .fresh,
    }};
    var fresh_ordered_traversal_query = traversal_query;
    fresh_ordered_traversal_query.order_by = &fresh_hits_metric_orders;
    try std.testing.expectError(error.MetricStale, hosted.source().query(alloc, "docs", .{
        .query = .{ .match_all = {} },
        .limit = 0,
        .graph_queries = &.{.{ .name = "fresh_ordered_hits_neighbors", .query = fresh_ordered_traversal_query }},
    }, .read_index));

    const fresh_hits_metric_filters = [_]graph_query_mod.GraphMetricFilter{.{
        .name = "hits_authority",
        .op = .gte,
        .value = 0.0,
        .freshness = .fresh,
    }};
    var fresh_filtered_traversal_query = traversal_query;
    fresh_filtered_traversal_query.where_metric = &fresh_hits_metric_filters;
    try std.testing.expectError(error.MetricStale, hosted.source().query(alloc, "docs", .{
        .query = .{ .match_all = {} },
        .limit = 0,
        .graph_queries = &.{.{ .name = "fresh_filtered_hits_neighbors", .query = fresh_filtered_traversal_query }},
    }, .read_index));
}

test "hosted cross-range graph metric fan-in rejects incompatible remote hits pair" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-hosted-cross-range-graph-metric-hits-pair-reject";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const graph_indexes_json =
        \\{"graph_idx":{"type":"graph","edge_types":[{"name":"cites"}],"metrics":{"hits_authority":{"enabled":true,"kind":"hits_authority","refresh":"manual","max_iterations":1,"tolerance":0.000001,"edge_filter":{"types":["cites"]}},"hits_hub":{"enabled":true,"kind":"hits_hub","refresh":"manual","max_iterations":1,"tolerance":0.000001,"edge_filter":{"types":["cites"]}}}}}
    ;

    const FakeCatalog = struct {
        const statuses = [_]metadata_reconciler.MergedGroupStatus{
            .{
                .group_id = 7321,
                .doc_identity = .{
                    .namespace_table_id = 7,
                    .namespace_shard_id = 7321,
                    .namespace_range_id = 7321,
                    .next_ordinal = 3,
                    .allocated_ordinals = 2,
                    .state_rows = 2,
                    .live_ordinals = 2,
                    .complete = true,
                },
            },
            .{
                .group_id = 7322,
                .doc_identity = .{
                    .namespace_table_id = 7,
                    .namespace_shard_id = 7322,
                    .namespace_range_id = 7322,
                    .next_ordinal = 3,
                    .allocated_ordinals = 2,
                    .state_rows = 2,
                    .live_ordinals = 2,
                    .complete = true,
                },
            },
        };

        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = graph_indexes_json,
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7321, .table_id = 7, .range_id = 7321, .start_key = "", .end_key = "doc:r:" },
                    .{ .group_id = 7322, .table_id = 7, .range_id = 7322, .start_key = "doc:r:", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
                .merged_group_statuses = @constCast(statuses[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const FakeRouter = struct {
        fn iface() table_router.HostedGroupRouter {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .local_node_id = localNodeId,
                    .local_status = localStatus,
                    .group_leader_node_id = groupLeaderNodeId,
                    .node_status = nodeStatus,
                    .node_base_uri = nodeBaseUri,
                },
            };
        }

        fn localNodeId(_: *anyopaque) u64 {
            return 1;
        }

        fn localStatus(_: *anyopaque, _: u64) raft_mod.HostedReplicaStatus {
            return .absent;
        }

        fn groupLeaderNodeId(_: *anyopaque, _: u64) ?u64 {
            return 2;
        }

        fn nodeStatus(_: *anyopaque, node_id: u64, _: u64) raft_mod.HostedReplicaStatus {
            return if (node_id == 2) .active else .absent;
        }

        fn nodeBaseUri(_: *anyopaque, alloc_inner: std.mem.Allocator, node_id: u64) !?[]u8 {
            if (node_id != 2) return null;
            return try alloc_inner.dupe(u8, "http://remote.test");
        }
    };

    const ExecutorState = struct {
        const Scenario = enum {
            generation_mismatch,
            metadata_mismatch,
            edge_filter_mismatch,
        };

        scenario: Scenario = .generation_mismatch,
        query_calls: usize = 0,

        fn iface(self: *@This()) http_common.RequestExecutor {
            return .{ .ptr = self, .vtable = &.{ .execute = execute } };
        }

        fn execute(ptr: *anyopaque, alloc_inner: std.mem.Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(http_common.Method.POST, req.method);
            self.query_calls += 1;
            if (std.mem.endsWith(u8, req.uri, "/internal/v1/groups/7321/tables/docs/query")) {
                return .{
                    .status = 200,
                    .body = try remoteHitsPairBody(alloc_inner, "q", 11, 11, 1, "cites"),
                };
            }
            if (std.mem.endsWith(u8, req.uri, "/internal/v1/groups/7322/tables/docs/query")) {
                const authority_generation: u64 = 11;
                const hub_generation: u64 = if (self.scenario == .generation_mismatch) 12 else 11;
                const metadata_version: u32 = if (self.scenario == .metadata_mismatch) 2 else 1;
                const edge_type: []const u8 = if (self.scenario == .edge_filter_mismatch) "mentions" else "cites";
                return .{
                    .status = 200,
                    .body = try remoteHitsPairBody(alloc_inner, "r", authority_generation, hub_generation, metadata_version, edge_type),
                };
            }
            return error.UnexpectedHttpRequest;
        }

        fn remoteHitsPairBody(
            alloc_inner: std.mem.Allocator,
            prefix: []const u8,
            authority_generation: u64,
            hub_generation: u64,
            metadata_version: u32,
            edge_type: []const u8,
        ) ![]u8 {
            return try std.fmt.allocPrint(
                alloc_inner,
                "{{\"responses\":[{{\"hits\":{{\"total\":0,\"hits\":[]}},\"graph_metric_results\":{{\"authority\":{{\"index_name\":\"graph_idx\",\"metric\":\"hits_authority\",\"scores\":[{{\"node\":\"doc:{s}:authority\",\"score\":1.0}}],\"status\":{{\"state\":\"fresh\",\"phase\":\"complete\",\"maintenance_paused\":false,\"build_queued\":false,\"published_generation\":{d},\"edge_generation\":{d},\"target_edge_generation\":{d},\"queued_generation\":0,\"building_generation\":0,\"metadata_version\":{d},\"edge_filter\":{{\"mode\":\"types\",\"types\":[\"{s}\"]}},\"progress\":1.0,\"converged\":true,\"iterations_completed\":1,\"delta\":0.0,\"computed_at_ms\":1780000000000}}}},\"hub\":{{\"index_name\":\"graph_idx\",\"metric\":\"hits_hub\",\"scores\":[{{\"node\":\"doc:{s}:hub\",\"score\":1.0}}],\"status\":{{\"state\":\"fresh\",\"phase\":\"complete\",\"maintenance_paused\":false,\"build_queued\":false,\"published_generation\":{d},\"edge_generation\":{d},\"target_edge_generation\":{d},\"queued_generation\":0,\"building_generation\":0,\"metadata_version\":{d},\"edge_filter\":{{\"mode\":\"types\",\"types\":[\"{s}\"]}},\"progress\":1.0,\"converged\":true,\"iterations_completed\":1,\"delta\":0.0,\"computed_at_ms\":1780000000000}}}}}},\"took\":0,\"status\":200,\"table\":\"docs\"}}]}}",
                .{
                    prefix,
                    authority_generation,
                    authority_generation,
                    authority_generation,
                    metadata_version,
                    edge_type,
                    prefix,
                    hub_generation,
                    hub_generation,
                    hub_generation,
                    metadata_version,
                    edge_type,
                },
            );
        }
    };

    var executor_state = ExecutorState{};
    var hosted = HostedProvisionedTableReadSource.init(
        path,
        FakeCatalog.iface(),
        raft_mod.read_gate.noopReadableLeaseRequester(),
        FakeRouter.iface(),
        executor_state.iface(),
    );
    _ = hosted.withIo(&io_impl);

    try std.testing.expectError(error.UnsupportedQueryRequest, hosted.source().query(alloc, "docs", .{
        .graph_metric_queries = &.{
            .{
                .name = "authority",
                .query = .{
                    .index_name = "graph_idx",
                    .metric_name = "hits_authority",
                    .top_k = 8,
                    .freshness = .published,
                },
            },
            .{
                .name = "hub",
                .query = .{
                    .index_name = "graph_idx",
                    .metric_name = "hits_hub",
                    .top_k = 8,
                    .freshness = .published,
                },
            },
        },
        .limit = 0,
    }, .read_index));
    try std.testing.expectEqual(@as(usize, 2), executor_state.query_calls);

    executor_state.scenario = .metadata_mismatch;
    executor_state.query_calls = 0;
    try std.testing.expectError(error.UnsupportedQueryRequest, hosted.source().query(alloc, "docs", .{
        .graph_metric_queries = &.{
            .{
                .name = "authority",
                .query = .{
                    .index_name = "graph_idx",
                    .metric_name = "hits_authority",
                    .top_k = 8,
                    .freshness = .published,
                },
            },
            .{
                .name = "hub",
                .query = .{
                    .index_name = "graph_idx",
                    .metric_name = "hits_hub",
                    .top_k = 8,
                    .freshness = .published,
                },
            },
        },
        .limit = 0,
    }, .read_index));
    try std.testing.expectEqual(@as(usize, 2), executor_state.query_calls);

    executor_state.scenario = .edge_filter_mismatch;
    executor_state.query_calls = 0;
    try std.testing.expectError(error.UnsupportedQueryRequest, hosted.source().query(alloc, "docs", .{
        .graph_metric_queries = &.{
            .{
                .name = "authority",
                .query = .{
                    .index_name = "graph_idx",
                    .metric_name = "hits_authority",
                    .top_k = 8,
                    .freshness = .published,
                },
            },
            .{
                .name = "hub",
                .query = .{
                    .index_name = "graph_idx",
                    .metric_name = "hits_hub",
                    .top_k = 8,
                    .freshness = .published,
                },
            },
        },
        .limit = 0,
    }, .read_index));
    try std.testing.expectEqual(@as(usize, 2), executor_state.query_calls);
}

test "hosted cross-range graph metric fan-in rejects missing remote hits status" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-hosted-cross-range-graph-metric-hits-missing-status";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const graph_indexes_json =
        \\{"graph_idx":{"type":"graph","edge_types":[{"name":"cites"}],"metrics":{"hits_authority":{"enabled":true,"kind":"hits_authority","refresh":"manual","max_iterations":1,"tolerance":0.000001,"edge_filter":{"types":["cites"]}},"hits_hub":{"enabled":true,"kind":"hits_hub","refresh":"manual","max_iterations":1,"tolerance":0.000001,"edge_filter":{"types":["cites"]}}}}}
    ;

    const FakeCatalog = struct {
        const statuses = [_]metadata_reconciler.MergedGroupStatus{
            .{
                .group_id = 7331,
                .doc_identity = .{
                    .namespace_table_id = 7,
                    .namespace_shard_id = 7331,
                    .namespace_range_id = 7331,
                    .next_ordinal = 3,
                    .allocated_ordinals = 2,
                    .state_rows = 2,
                    .live_ordinals = 2,
                    .complete = true,
                },
            },
            .{
                .group_id = 7332,
                .doc_identity = .{
                    .namespace_table_id = 7,
                    .namespace_shard_id = 7332,
                    .namespace_range_id = 7332,
                    .next_ordinal = 3,
                    .allocated_ordinals = 2,
                    .state_rows = 2,
                    .live_ordinals = 2,
                    .complete = true,
                },
            },
        };

        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = graph_indexes_json,
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7331, .table_id = 7, .range_id = 7331, .start_key = "", .end_key = "doc:t:" },
                    .{ .group_id = 7332, .table_id = 7, .range_id = 7332, .start_key = "doc:t:", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
                .merged_group_statuses = @constCast(statuses[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const FakeRouter = struct {
        fn iface() table_router.HostedGroupRouter {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .local_node_id = localNodeId,
                    .local_status = localStatus,
                    .group_leader_node_id = groupLeaderNodeId,
                    .node_status = nodeStatus,
                    .node_base_uri = nodeBaseUri,
                },
            };
        }

        fn localNodeId(_: *anyopaque) u64 {
            return 1;
        }

        fn localStatus(_: *anyopaque, _: u64) raft_mod.HostedReplicaStatus {
            return .absent;
        }

        fn groupLeaderNodeId(_: *anyopaque, _: u64) ?u64 {
            return 2;
        }

        fn nodeStatus(_: *anyopaque, node_id: u64, _: u64) raft_mod.HostedReplicaStatus {
            return if (node_id == 2) .active else .absent;
        }

        fn nodeBaseUri(_: *anyopaque, alloc_inner: std.mem.Allocator, node_id: u64) !?[]u8 {
            if (node_id != 2) return null;
            return try alloc_inner.dupe(u8, "http://remote.test");
        }
    };

    const ExecutorState = struct {
        query_calls: usize = 0,

        fn iface(self: *@This()) http_common.RequestExecutor {
            return .{ .ptr = self, .vtable = &.{ .execute = execute } };
        }

        fn execute(ptr: *anyopaque, alloc_inner: std.mem.Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(http_common.Method.POST, req.method);
            self.query_calls += 1;
            if (std.mem.endsWith(u8, req.uri, "/internal/v1/groups/7331/tables/docs/query")) {
                return .{
                    .status = 200,
                    .body = try remoteHitsPairBody(alloc_inner, "s", 21),
                };
            }
            if (std.mem.endsWith(u8, req.uri, "/internal/v1/groups/7332/tables/docs/query")) {
                return .{
                    .status = 200,
                    .body = try remoteHitsMissingHubStatusBody(alloc_inner, "t", 21),
                };
            }
            return error.UnexpectedHttpRequest;
        }

        fn remoteHitsPairBody(
            alloc_inner: std.mem.Allocator,
            prefix: []const u8,
            generation: u64,
        ) ![]u8 {
            return try std.fmt.allocPrint(
                alloc_inner,
                "{{\"responses\":[{{\"hits\":{{\"total\":0,\"hits\":[]}},\"graph_metric_results\":{{\"authority\":{{\"index_name\":\"graph_idx\",\"metric\":\"hits_authority\",\"scores\":[{{\"node\":\"doc:{s}:authority\",\"score\":1.0}}],\"status\":{{\"state\":\"fresh\",\"phase\":\"complete\",\"maintenance_paused\":false,\"build_queued\":false,\"published_generation\":{d},\"edge_generation\":{d},\"target_edge_generation\":{d},\"queued_generation\":0,\"building_generation\":0,\"progress\":1.0,\"converged\":true,\"iterations_completed\":1,\"delta\":0.0,\"computed_at_ms\":1780000000000}}}},\"hub\":{{\"index_name\":\"graph_idx\",\"metric\":\"hits_hub\",\"scores\":[{{\"node\":\"doc:{s}:hub\",\"score\":1.0}}],\"status\":{{\"state\":\"fresh\",\"phase\":\"complete\",\"maintenance_paused\":false,\"build_queued\":false,\"published_generation\":{d},\"edge_generation\":{d},\"target_edge_generation\":{d},\"queued_generation\":0,\"building_generation\":0,\"progress\":1.0,\"converged\":true,\"iterations_completed\":1,\"delta\":0.0,\"computed_at_ms\":1780000000000}}}}}},\"took\":0,\"status\":200,\"table\":\"docs\"}}]}}",
                .{ prefix, generation, generation, generation, prefix, generation, generation, generation },
            );
        }

        fn remoteHitsMissingHubStatusBody(
            alloc_inner: std.mem.Allocator,
            prefix: []const u8,
            generation: u64,
        ) ![]u8 {
            return try std.fmt.allocPrint(
                alloc_inner,
                "{{\"responses\":[{{\"hits\":{{\"total\":0,\"hits\":[]}},\"graph_metric_results\":{{\"authority\":{{\"index_name\":\"graph_idx\",\"metric\":\"hits_authority\",\"scores\":[{{\"node\":\"doc:{s}:authority\",\"score\":1.0}}],\"status\":{{\"state\":\"fresh\",\"phase\":\"complete\",\"maintenance_paused\":false,\"build_queued\":false,\"published_generation\":{d},\"edge_generation\":{d},\"target_edge_generation\":{d},\"queued_generation\":0,\"building_generation\":0,\"progress\":1.0,\"converged\":true,\"iterations_completed\":1,\"delta\":0.0,\"computed_at_ms\":1780000000000}}}},\"hub\":{{\"index_name\":\"graph_idx\",\"metric\":\"hits_hub\",\"scores\":[{{\"node\":\"doc:{s}:hub\",\"score\":1.0}}]}}}},\"took\":0,\"status\":200,\"table\":\"docs\"}}]}}",
                .{ prefix, generation, generation, generation, prefix },
            );
        }
    };

    var executor_state = ExecutorState{};
    var hosted = HostedProvisionedTableReadSource.init(
        path,
        FakeCatalog.iface(),
        raft_mod.read_gate.noopReadableLeaseRequester(),
        FakeRouter.iface(),
        executor_state.iface(),
    );
    _ = hosted.withIo(&io_impl);

    try std.testing.expectError(error.UnsupportedQueryRequest, hosted.source().query(alloc, "docs", .{
        .graph_metric_queries = &.{
            .{
                .name = "authority",
                .query = .{
                    .index_name = "graph_idx",
                    .metric_name = "hits_authority",
                    .top_k = 8,
                    .freshness = .published,
                },
            },
            .{
                .name = "hub",
                .query = .{
                    .index_name = "graph_idx",
                    .metric_name = "hits_hub",
                    .top_k = 8,
                    .freshness = .published,
                },
            },
        },
        .limit = 0,
    }, .read_index));
    try std.testing.expectEqual(@as(usize, 2), executor_state.query_calls);
}

test "hosted cross-range graph metric fan-in rejects unpublished or incompatible shard generations" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-hosted-cross-range-graph-metric-reject";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const left_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7201);
    defer alloc.free(left_path);
    const right_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7202);
    defer alloc.free(right_path);

    const graph_indexes_json =
        \\{"graph_idx":{"type":"graph","edge_types":[{"name":"cites"}],"metrics":{"manual_degree":{"enabled":true,"kind":"degree","refresh":"manual","edge_filter":{"types":["cites"]}}}}}
    ;
    const graph_config_json =
        \\{"edge_types":[{"name":"cites"}],"metrics":{"manual_degree":{"enabled":true,"kind":"degree","refresh":"manual","edge_filter":{"types":["cites"]}}}}
    ;

    var left_db = try db_mod.DB.open(alloc, left_path, .{
        .start_index_workers = false,
        .identity_namespace = .{ .table_id = 7, .shard_id = 7201, .range_id = 7201 },
    });
    defer left_db.close();
    try left_db.addIndex(.{ .name = "graph_idx", .kind = .graph, .config_json = graph_config_json });
    try left_db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"left-a\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"left-b\"}" },
        },
        .sync_level = .write,
    });
    try left_db.runUntilIdle();
    var left_status = try left_db.refreshGraphMetric(alloc, "graph_idx", "manual_degree");
    defer left_status.deinit(alloc);

    var right_db = try db_mod.DB.open(alloc, right_path, .{
        .start_index_workers = false,
        .identity_namespace = .{ .table_id = 7, .shard_id = 7202, .range_id = 7202 },
    });
    defer right_db.close();
    try right_db.addIndex(.{ .name = "graph_idx", .kind = .graph, .config_json = graph_config_json });
    try right_db.batch(.{
        .writes = &.{
            .{ .key = "doc:n", .value = "{\"title\":\"right-n\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:o\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:o", .value = "{\"title\":\"right-o\"}" },
        },
        .sync_level = .write,
    });
    try right_db.runUntilIdle();

    const FakeCatalog = struct {
        const statuses = [_]metadata_reconciler.MergedGroupStatus{
            .{
                .group_id = 7201,
                .doc_identity = .{
                    .namespace_table_id = 7,
                    .namespace_shard_id = 7201,
                    .namespace_range_id = 7201,
                    .next_ordinal = 3,
                    .allocated_ordinals = 2,
                    .state_rows = 2,
                    .live_ordinals = 2,
                    .complete = true,
                },
            },
            .{
                .group_id = 7202,
                .doc_identity = .{
                    .namespace_table_id = 7,
                    .namespace_shard_id = 7202,
                    .namespace_range_id = 7202,
                    .next_ordinal = 4,
                    .allocated_ordinals = 3,
                    .state_rows = 3,
                    .live_ordinals = 3,
                    .complete = true,
                },
            },
        };

        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = graph_indexes_json,
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7201, .table_id = 7, .range_id = 7201, .start_key = "", .end_key = "m" },
                    .{ .group_id = 7202, .table_id = 7, .range_id = 7202, .start_key = "m", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
                .merged_group_statuses = @constCast(statuses[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const FakeRouter = struct {
        fn iface() table_router.HostedGroupRouter {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .local_node_id = localNodeId,
                    .local_status = localStatus,
                    .group_leader_node_id = groupLeaderNodeId,
                    .node_status = nodeStatus,
                    .node_base_uri = nodeBaseUri,
                },
            };
        }

        fn localNodeId(_: *anyopaque) u64 {
            return 1;
        }

        fn localStatus(_: *anyopaque, _: u64) raft_mod.HostedReplicaStatus {
            return .active;
        }

        fn groupLeaderNodeId(_: *anyopaque, _: u64) ?u64 {
            return 1;
        }

        fn nodeStatus(_: *anyopaque, _: u64, _: u64) raft_mod.HostedReplicaStatus {
            return .absent;
        }

        fn nodeBaseUri(_: *anyopaque, _: std.mem.Allocator, _: u64) !?[]u8 {
            return null;
        }
    };

    const ExecutorState = struct {
        fn iface(self: *@This()) http_common.RequestExecutor {
            return .{ .ptr = self, .vtable = &.{ .execute = execute } };
        }

        fn execute(_: *anyopaque, _: std.mem.Allocator, _: http_common.HttpRequest) !http_common.HttpResponse {
            return error.UnexpectedHttpRequest;
        }
    };

    var executor_state = ExecutorState{};
    var hosted = HostedProvisionedTableReadSource.init(
        path,
        FakeCatalog.iface(),
        raft_mod.read_gate.noopReadableLeaseRequester(),
        FakeRouter.iface(),
        executor_state.iface(),
    );
    _ = hosted.withIo(&io_impl);

    const metric_req = db_mod.types.SearchRequest{
        .graph_metric_queries = &.{.{
            .name = "central",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "manual_degree",
                .top_k = 4,
                .freshness = .published,
            },
        }},
        .limit = 0,
    };

    try std.testing.expectError(error.MetricNotReady, hosted.source().query(alloc, "docs", metric_req, .read_index));

    var right_first = try right_db.refreshGraphMetric(alloc, "graph_idx", "manual_degree");
    defer right_first.deinit(alloc);
    try std.testing.expectEqual(left_status.published_generation, right_first.published_generation);
    try right_db.batch(.{
        .writes = &.{.{ .key = "doc:p", .value = "{\"title\":\"right-p\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:o\",\"weight\":1.0}]}}}" }},
        .sync_level = .write,
    });
    try right_db.runUntilIdle();
    var right_second = try right_db.rebuildGraphMetric(alloc, "graph_idx", "manual_degree");
    defer right_second.deinit(alloc);
    try std.testing.expect(right_second.published_generation > left_status.published_generation);

    try std.testing.expectError(error.UnsupportedQueryRequest, hosted.source().query(alloc, "docs", metric_req, .read_index));
}

test "provisioned lookup db opens with identity namespace" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-provisioned-lookup-identity-namespace";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const namespace: db_mod.DocIdentityNamespace = .{
        .table_id = 7,
        .shard_id = 7001,
        .range_id = 7103,
    };
    var db = try openProvisionedLookupDbForTable(alloc, path, null, backend_current_root_generation, null, null, namespace);
    defer db.close();

    try std.testing.expect(db.core.identity_namespace.eql(namespace));
}

test "provisioned warm status db opens with identity namespace" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-provisioned-warm-status-identity-namespace";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const namespace: db_mod.DocIdentityNamespace = .{
        .table_id = 7,
        .shard_id = 7001,
        .range_id = 7104,
    };
    var db = try openProvisionedWarmStatusDbForTable(alloc, path, backend_current_root_generation, null, namespace);
    defer db.close();

    try std.testing.expect(db.core.identity_namespace.eql(namespace));
}

test "provisioned direct read db opens reject stale identity namespace" {
    const alloc = std.testing.allocator;
    const lookup_path = "/tmp/antfly-api-provisioned-lookup-stale-identity-namespace";
    const status_path = "/tmp/antfly-api-provisioned-status-stale-identity-namespace";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), lookup_path) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), status_path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), lookup_path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), status_path) catch {};

    const stale_namespace: db_mod.DocIdentityNamespace = .{
        .table_id = 7,
        .shard_id = 7001,
        .range_id = 7198,
    };
    const expected_namespace: db_mod.DocIdentityNamespace = .{
        .table_id = 7,
        .shard_id = 7001,
        .range_id = 7199,
    };

    {
        var db = try db_mod.DB.open(alloc, lookup_path, .{
            .start_index_workers = false,
            .identity_namespace = stale_namespace,
        });
        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"name\":\"alpha\"}" }},
        });
        db.close();
    }
    if (openProvisionedLookupDbForTable(alloc, lookup_path, null, backend_current_root_generation, null, null, expected_namespace)) |opened| {
        var db = opened;
        db.close();
        return error.TestExpectedError;
    } else |err| try std.testing.expectEqual(error.DocIdentityNamespaceMismatch, err);

    {
        var db = try db_mod.DB.open(alloc, status_path, .{
            .start_index_workers = false,
            .identity_namespace = stale_namespace,
        });
        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"name\":\"alpha\"}" }},
        });
        db.close();
    }
    if (openProvisionedWarmStatusDbForTable(alloc, status_path, backend_current_root_generation, null, expected_namespace)) |opened| {
        var db = opened;
        db.close();
        return error.TestExpectedError;
    } else |err| try std.testing.expectEqual(error.DocIdentityNamespaceMismatch, err);
}

test "provisioned primary lookup lease fails on identity namespace mismatch" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-provisioned-primary-lookup-identity-mismatch";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer db.close();

    const CatalogState = struct {
        fn iface(self: *@This()) table_catalog.CatalogSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .description = "docs table",
                    .schema_json = "",
                    .read_schema_json = "",
                    .indexes_json = @import("tables.zig").default_indexes_json,
                    .replication_sources_json = "[]",
                    .placement_role = "data",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .range_id = 7106,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const PrimarySource = struct {
        db: *db_mod.DB,

        fn iface(self: *@This()) PrimaryLookupDbSource {
            return .{
                .ptr = self,
                .lease_group = leaseGroup,
            };
        }

        fn leaseGroup(
            ptr: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: u64,
            _: u64,
        ) !?PrimaryLookupDbLease {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return .{
                .ptr = self.db,
                .db = self.db,
                .release_fn = release,
            };
        }

        fn release(_: *anyopaque, _: std.mem.Allocator) void {}
    };

    var catalog_state = CatalogState{};
    var primary_source = PrimarySource{ .db = &db };
    try std.testing.expectError(error.DocIdentityNamespaceMismatch, lookupProvisionedLocal(
        primary_source.iface(),
        null,
        "/tmp/unused-antfly-primary-lookup-mismatch",
        catalog_state.iface(),
        raft_mod.read_gate.noopReadableLeaseRequester(),
        alloc,
        7001,
        0,
        null,
        "docs",
        "doc:a",
        .{},
        .stale,
    ));
}

test "relational unique owner lookup requires one active owner range" {
    const FakeCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "users",
                    .placement_role = "data",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
                .unique_constraint_ranges = @constCast((&[_]metadata_table_manager.UniqueConstraintRangeRecord{
                    .{
                        .table_id = 7,
                        .constraint_name = "users_email_key",
                        .start_encoded_value = "",
                        .end_encoded_value = "m",
                        .group_id = 7101,
                    },
                    .{
                        .table_id = 7,
                        .constraint_name = "users_email_key",
                        .start_encoded_value = "m",
                        .end_encoded_value = null,
                        .group_id = 7102,
                    },
                    .{
                        .table_id = 7,
                        .constraint_name = "users_phone_key",
                        .start_encoded_value = "",
                        .end_encoded_value = null,
                        .group_id = 7201,
                        .state = metadata_table_manager.unique_constraint_range_rebuilding,
                    },
                    .{
                        .table_id = 7,
                        .constraint_name = "users_handle_key",
                        .start_encoded_value = "",
                        .end_encoded_value = null,
                        .group_id = 7202,
                        .state = metadata_table_manager.unique_constraint_range_splitting,
                    },
                    .{
                        .table_id = 7,
                        .constraint_name = "users_username_key",
                        .start_encoded_value = "",
                        .end_encoded_value = null,
                        .group_id = 7203,
                        .state = metadata_table_manager.unique_constraint_range_merging,
                    },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    try std.testing.expectEqual(@as(u64, 7101), try resolveSingleUniqueOwnerGroup(std.testing.allocator, FakeCatalog.iface(), "users", "users_email_key", "a"));
    try std.testing.expectEqual(@as(u64, 7102), try resolveSingleUniqueOwnerGroup(std.testing.allocator, FakeCatalog.iface(), "users", "users_email_key", "z"));
    try std.testing.expectError(error.UniqueOwnerTopologyUnavailable, resolveSingleUniqueOwnerGroup(std.testing.allocator, FakeCatalog.iface(), "users", "users_phone_key", "p"));
    try std.testing.expectError(error.UniqueOwnerTopologyUnavailable, resolveSingleUniqueOwnerGroup(std.testing.allocator, FakeCatalog.iface(), "users", "users_handle_key", "h"));
    try std.testing.expectError(error.UniqueOwnerTopologyUnavailable, resolveSingleUniqueOwnerGroup(std.testing.allocator, FakeCatalog.iface(), "users", "users_username_key", "u"));
    try std.testing.expectError(error.UniqueOwnerTopologyUnavailable, resolveSingleUniqueOwnerGroup(std.testing.allocator, FakeCatalog.iface(), "users", "missing_key", "a"));
}

test "hosted remote temporal unique owner lookup resolves point interval" {
    const alloc = std.testing.allocator;
    const column = storage_schema.RelationalColumn{
        .name = "valid_at",
        .path = "valid_at",
        .field_type = .numeric,
    };
    const point_15 = try db_mod.relational_store.temporalPeriodBoundBytesFromJsonAlloc(alloc, "15", column);
    defer alloc.free(point_15);
    const start_12 = try db_mod.relational_store.temporalPeriodBoundBytesFromJsonAlloc(alloc, "12", column);
    defer alloc.free(start_12);
    const end_18 = try db_mod.relational_store.temporalPeriodBoundBytesFromJsonAlloc(alloc, "18", column);
    defer alloc.free(end_18);

    const ExecutorState = struct {
        point_15: []const u8,
        start_12: []const u8,
        end_18: []const u8,
        point_calls: usize = 0,
        overlap_calls: usize = 0,

        fn iface(self: *@This()) http_common.RequestExecutor {
            return .{ .ptr = self, .vtable = &.{ .execute = execute } };
        }

        fn execute(ptr: *anyopaque, alloc_inner: std.mem.Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (std.mem.endsWith(u8, req.uri, "/internal/v1/groups/7102/tables/prices/relational-temporal-unique-owner") and req.method == .POST) {
                var parsed = try std.json.parseFromSlice(struct {
                    constraint_name: []const u8,
                    encoded_value: []const u8,
                    encoded_point: []const u8,
                }, alloc_inner, req.body, .{ .allocate = .alloc_always });
                defer parsed.deinit();
                try std.testing.expectEqualStrings("prices_sku_valid_time_key", parsed.value.constraint_name);
                try std.testing.expectEqualStrings("sku:a", parsed.value.encoded_value);
                try std.testing.expectEqualStrings(self.point_15, parsed.value.encoded_point);
                self.point_calls += 1;
                return .{
                    .status = 200,
                    .content_type = try alloc_inner.dupe(u8, "application/json"),
                    .body = try std.fmt.allocPrint(
                        alloc_inner,
                        "{{\"owner_key\":{f}}}",
                        .{std.json.fmt("price:a:v2", .{})},
                    ),
                };
            }
            if (std.mem.endsWith(u8, req.uri, "/internal/v1/groups/7102/tables/prices/relational-temporal-unique-overlap-owner") and req.method == .POST) {
                var parsed = try std.json.parseFromSlice(struct {
                    constraint_name: []const u8,
                    encoded_value: []const u8,
                    encoded_start: []const u8,
                    encoded_end: []const u8,
                }, alloc_inner, req.body, .{ .allocate = .alloc_always });
                defer parsed.deinit();
                try std.testing.expectEqualStrings("prices_sku_valid_time_key", parsed.value.constraint_name);
                try std.testing.expectEqualStrings("sku:a", parsed.value.encoded_value);
                try std.testing.expectEqualStrings(self.start_12, parsed.value.encoded_start);
                try std.testing.expectEqualStrings(self.end_18, parsed.value.encoded_end);
                self.overlap_calls += 1;
                return .{
                    .status = 200,
                    .content_type = try alloc_inner.dupe(u8, "application/json"),
                    .body = try std.fmt.allocPrint(
                        alloc_inner,
                        "{{\"owner_key\":{f}}}",
                        .{std.json.fmt("price:a:v2", .{})},
                    ),
                };
            }
            return error.UnexpectedHttpRequest;
        }
    };

    var executor = ExecutorState{ .point_15 = point_15, .start_12 = start_12, .end_18 = end_18 };
    const owner = (try lookupRelationalTemporalUniqueOwnerRemote(
        executor.iface(),
        alloc,
        "http://remote.test",
        7102,
        "prices",
        "prices_sku_valid_time_key",
        "sku:a",
        point_15,
    )).?;
    defer alloc.free(owner);

    try std.testing.expectEqualStrings("price:a:v2", owner);
    try std.testing.expectEqual(@as(usize, 1), executor.point_calls);
    try std.testing.expectEqual(@as(usize, 0), executor.overlap_calls);

    const overlap_owner = (try lookupRelationalTemporalUniqueOverlapOwnerRemote(
        executor.iface(),
        alloc,
        "http://remote.test",
        7102,
        "prices",
        "prices_sku_valid_time_key",
        "sku:a",
        start_12,
        end_18,
    )).?;
    defer alloc.free(overlap_owner);

    try std.testing.expectEqualStrings("price:a:v2", overlap_owner);
    try std.testing.expectEqual(@as(usize, 1), executor.point_calls);
    try std.testing.expectEqual(@as(usize, 1), executor.overlap_calls);
}
