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
const metadata_admin = @import("../../metadata/admin.zig");
const metadata_api = @import("../../metadata/api.zig");
const metadata_mod = @import("../../metadata/mod.zig");
const metadata_reconciler = @import("../../metadata/reconciler.zig");
const common_secrets = @import("../../common/secrets.zig");
const metadata_table_manager = @import("../../metadata/table_manager.zig");
const metadata_table_provisioner = @import("../../metadata/table_provisioner.zig");
const metadata_transition_state = @import("../../metadata/transition_state.zig");
const managed_embedder = @import("../../inference/managed_embedder.zig");
const raft_mod = @import("../../raft/mod.zig");
const raft_reconciler = @import("../../raft/reconciler.zig");
const db_mod = @import("../../storage/db/mod.zig");
const storage_schema = @import("../../storage/schema.zig");
const doc_set = @import("../../storage/db/doc_set.zig");
const catalog_resources = @import("../catalog_resources.zig");
const db_embedder = @import("../../storage/db/enrichment/embedder.zig");
const asset_producer_mod = @import("../../storage/db/enrichment/asset_producer.zig");
const ha_standby_mod = @import("../../storage/ha/standby.zig");
const hbc_mod = @import("../../storage/hbc_adapter.zig");
const lsm_backend = @import("../../storage/lsm_backend/mod.zig");
const resource_manager_mod = @import("../../storage/resource_manager.zig");
const db_query_search = @import("../../storage/db/query/search_exec.zig");
const graph_mod = @import("../../graph/graph.zig");
const graph_paths = @import("../../graph/paths.zig");
const graph_query_mod = @import("../../graph/query.zig");
const reranking_runtime = @import("../../reranking/mod.zig");
const template_mod = @import("../../template.zig");
const table_catalog = @import("../../metadata/catalog/routing.zig");
const table_router = @import("../table_router.zig");
const tables_api = @import("../../metadata/catalog/table_ddl.zig");
const query_api = @import("../query.zig");
const query_contract = @import("../query_contract.zig");
const relational_rows_api = @import("../../sql/relational_rows.zig");
const row_spill = @import("../../sql/row_spill.zig");
const sql_adapter = @import("../../sql/mod.zig");
const sql_document = @import("../../sql/document.zig");
const document_sql_runtime = @import("../../sql/document_runtime.zig");
const serverless_query = @import("../../serverless/query/mod.zig");
const schema_api = @import("../../schema/mod.zig");
const distributed_graph = @import("../distributed_graph.zig");
const runtime_status = @import("../runtime_status.zig");
const http_client = @import("../http_client.zig");
const http_common = @import("../../raft/transport/http_common.zig");
const platform_time = @import("../../platform/time.zig");
const distributed_stats_mod = @import("../../search/distributed_stats.zig");
const httpx = @import("httpx");
const Io = std.Io;
const json_helpers = @import("../json_helpers.zig");
const table_read_core = @import("core.zig");
const table_read_cache = @import("cache.zig");
const table_read_document_sql = @import("document_sql.zig");
const table_read_relational_rows = @import("relational_rows.zig");
const table_read_external_lake = @import("external_lake.zig");
const table_read_remote_wire = @import("remote_wire.zig");
const table_read_fanout = @import("fanout.zig");
const table_read_graph = @import("graph.zig");

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
const rowsSourceGroupRemote = table_read_remote_wire.rowsSourceGroupRemote;
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

fn uniqueTestTmpPathAlloc(alloc: std.mem.Allocator, prefix: []const u8) ![]u8 {
    return try std.fmt.allocPrint(alloc, "/tmp/{s}-{d}", .{ prefix, platform_time.monotonicNs() });
}

const algebraicConstraintsForRequestAlloc = table_read_fanout.algebraicConstraintsForRequestAlloc;
const freeAlgebraicConstraints = table_read_fanout.freeAlgebraicConstraints;
const ParallelFanoutKind = table_read_fanout.ParallelFanoutKind;
const FanoutPlanReason = table_read_fanout.FanoutPlanReason;
const FanoutPlan = table_read_fanout.FanoutPlan;
const recordFanoutPlan = table_read_fanout.recordFanoutPlan;
const recordParallelFanout = table_read_fanout.recordParallelFanout;
const recordParallelFanoutFallback = table_read_fanout.recordParallelFanoutFallback;
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
const algebraicDistributedTensorProgramForAggregationRequestAlloc = table_read_fanout.algebraicDistributedTensorProgramForAggregationRequestAlloc;
const algebraicAggregationFromDistributedPartialsAlloc = table_read_fanout.algebraicAggregationFromDistributedPartialsAlloc;

fn executeSystemVersionedRowsPlanAsOfSequenceAlloc(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    runtime_schema: storage_schema.TableSchema,
    commit_sequence: u64,
    plan: db_mod.types.RelationalRowsQueryPlan,
) !db_mod.types.RelationalRowsQueryResult {
    if (plan.ctes.len != 0 or plan.ranges.len != 0) {
        var snapshot = try db.querySystemVersionedRelationalRowsAsOfSequence(alloc, runtime_schema, commit_sequence, .{ .select_all = true });
        defer snapshot.deinit(alloc);
        return try relational_rows_api.executeRowsQueryPlanOnJsonRowsAlloc(alloc, runtime_schema, plan, snapshot.rows);
    }
    return try db.querySystemVersionedRelationalRowsAsOfSequence(alloc, runtime_schema, commit_sequence, plan.query);
}

fn executeSystemVersionedRowsPlanAsOfTimestampNsAlloc(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    runtime_schema: storage_schema.TableSchema,
    timestamp_ns: u64,
    plan: db_mod.types.RelationalRowsQueryPlan,
) !db_mod.types.RelationalRowsQueryResult {
    const commit_sequence = try db.resolveSystemVersionedCommitSequenceAtTimestampNs(alloc, timestamp_ns);
    return try executeSystemVersionedRowsPlanAsOfSequenceAlloc(alloc, db, runtime_schema, commit_sequence, plan);
}

fn resolveTableDocKeyRangesForRowsPlanAlloc(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
    ranges: []const db_mod.types.RelationalRowsDocKeyRange,
) ![]table_catalog.TableDocKeyRangePlan {
    if (ranges.len == 0) return try table_catalog.resolveTableDocKeyRanges(alloc, catalog, table_name, "", "");

    var out = std.ArrayListUnmanaged(table_catalog.TableDocKeyRangePlan).empty;
    errdefer {
        for (out.items) |*plan| plan.deinit(alloc);
        out.deinit(alloc);
    }

    for (ranges) |range| {
        const range_plans = try table_catalog.resolveTableDocKeyRanges(alloc, catalog, table_name, range.start, range.end);
        var moved: usize = 0;
        defer {
            for (range_plans[moved..]) |*plan| plan.deinit(alloc);
            alloc.free(range_plans);
        }
        try out.ensureUnusedCapacity(alloc, range_plans.len);
        for (range_plans) |plan| {
            out.appendAssumeCapacity(plan);
            moved += 1;
        }
    }
    return try out.toOwnedSlice(alloc);
}

fn executeRelationalRowsSourceGroupLocalAlloc(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    schema_json: []const u8,
    req: db_mod.types.RelationalRowsQueryRequest,
    doc_key_range: db_mod.types.RelationalRowsDocKeyRange,
    system_time: ?table_read_core.RelationalRowsSourceGroupSystemTime,
) !db_mod.types.RelationalRowsQueryResult {
    if (req.doc_key_range != null) return error.InvalidQueryRequest;
    var parsed_schema = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer storage_schema.freeSchema(alloc, runtime_schema);

    var local_req = req;
    local_req.doc_key_range = doc_key_range;
    if (system_time) |selector| {
        if (selector.sequence != null and selector.timestamp_ns != null) return error.InvalidArgument;
        if (selector.sequence) |commit_sequence| {
            return try db.querySystemVersionedRelationalRowsAsOfSequence(alloc, runtime_schema, commit_sequence, local_req);
        }
        if (selector.timestamp_ns) |timestamp_ns| {
            return try db.querySystemVersionedRelationalRowsAsOfTimestampNs(alloc, runtime_schema, timestamp_ns, local_req);
        }
        return error.InvalidArgument;
    }
    return try db.queryRelationalRows(alloc, runtime_schema, local_req);
}

fn freeOwnedJsonRows(alloc: std.mem.Allocator, rows: []const []const u8) void {
    row_spill.freeJsonRows(alloc, rows);
}

fn benchQueryApiPhaseProfileEnabled() bool {
    return std.c.getenv("ANTFLY_BENCH_QUERY_API_PHASES\x00") != null or
        std.c.getenv("ANTFLY_BENCH_QUERY_PROFILE_EVERY\x00") != null;
}

fn nsToUsFloat(ns: u64) f64 {
    return @as(f64, @floatFromInt(ns)) / 1000.0;
}
const LookupResponse = table_read_core.LookupResponse;
const ScanResponse = table_read_core.ScanResponse;
const TextStatsResponse = table_read_core.TextStatsResponse;
const BackgroundTextStatsResponse = table_read_core.BackgroundTextStatsResponse;

const takeLoweredSqlReadRows = table_read_relational_rows.takeLoweredSqlReadRows;

const LsmStorageStats = table_read_core.LsmStorageStats;
const appendScanLine = table_read_core.appendScanLine;
const HAReadGate = table_read_core.HAReadGate;
const ReadPreparation = table_read_core.ReadPreparation;
const backend_current_root_generation = table_read_core.backend_current_root_generation;
const GroupVisibleRootGenerationSource = table_read_core.GroupVisibleRootGenerationSource;
const PrimaryLookupDbLease = table_read_core.PrimaryLookupDbLease;
const PrimaryLookupDbSource = table_read_core.PrimaryLookupDbSource;
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

const ProvisionedTableReadCache = table_read_cache.ProvisionedTableReadCache;
const openProvisionedQueryDbForTable = table_read_cache.openProvisionedQueryDbForTable;
const openProvisionedQueryDbForTableWithRuntime = table_read_cache.openProvisionedQueryDbForTableWithRuntime;
const openProvisionedQueryDbForTableWithCache = table_read_cache.openProvisionedQueryDbForTableWithCache;
const openProvisionedWarmStatusDbForTable = table_read_cache.openProvisionedWarmStatusDbForTable;
const openProvisionedLookupDbForTable = table_read_cache.openProvisionedLookupDbForTable;
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

const RelationalRowsSourceGroupRequest = table_read_core.RelationalRowsSourceGroupRequest;
const TableReadSource = table_read_core.TableReadSource;

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
    table_read_remote_wire.annotateVectorWorkerPreflight(alloc, summary, req, algebraicVectorWorkerFilterJsonSupported);
}

fn encodeAlgebraicVectorWorkerRequestForSearchRequestAlloc(
    alloc: std.mem.Allocator,
    req: db_mod.types.SearchRequest,
) !?[]u8 {
    return table_read_remote_wire.encodeAlgebraicVectorWorkerRequestForSearchRequestAlloc(alloc, req, algebraicVectorWorkerFilterJsonSupported);
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
                .rows_query_plan_system_time_as_of_timestamp_ns = rowsQueryPlanSystemTimeAsOfTimestampNs,
                .rows_query_plan_catalog_system_time_as_of_timestamp_ns = rowsQueryPlanCatalogSystemTimeAsOfTimestampNsNative,
                .rows_set_operation_plan = rowsSetOperationPlan,
                .rows_aggregate_plan = rowsAggregatePlan,
                .rows_window_plan = rowsWindowPlan,
                .rows_join_plan = rowsJoinPlan,
                .rows_lateral_plan = rowsLateralPlan,
                .relational_rows_source_group_local = relationalRowsSourceGroupLocal,
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

        var result = (self.reads.lookupWithConsistency(alloc, self.db, key, opts, consistency) catch |err| switch (err) {
            error.UnsupportedOperation => blk: {
                if (!lookupOptionsAreDefault(opts)) return err;
                try self.reads.reads.prepareLookupWithConsistency(self.reads.group_id, key, opts, consistency);
                const json = (try self.db.get(alloc, key)) orelse return null;
                break :blk db_mod.types.LookupResult{ .json = json };
            },
            else => return err,
        }) orelse return null;
        defer result.deinit(alloc);
        const version = try self.db.getTimestamp(alloc, key);

        return .{
            .json = try alloc.dupe(u8, result.json),
            .version = version,
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
            try appendScanLine(alloc, &out, entry.id, json, try self.db.getTimestamp(alloc, entry.id));
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

    fn relationalRowsSourceGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        schema_json: []const u8,
        topology_epoch: u64,
        req: db_mod.types.RelationalRowsQueryRequest,
        doc_key_range: db_mod.types.RelationalRowsDocKeyRange,
        system_time: ?table_read_core.RelationalRowsSourceGroupSystemTime,
        consistency: raft_mod.ReadConsistency,
    ) !?db_mod.types.RelationalRowsQueryResult {
        const self: *BoundTableReadSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        if (group_id != self.reads.group_id) return null;
        if (topology_epoch != 0) return error.TopologyChanged;
        try self.reads.reads.prepareScanWithConsistency(group_id, doc_key_range.start, doc_key_range.end, .{}, consistency);
        return try executeRelationalRowsSourceGroupLocalAlloc(alloc, self.db, schema_json, req, doc_key_range, system_time);
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
        try self.prepareRelationalRowsFullTableRead(consistency);
        return try executeSystemVersionedRowsPlanAsOfSequenceAlloc(alloc, self.db, runtime_schema, commit_sequence, plan);
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

    fn rowsQueryPlanSystemTimeAsOfTimestampNs(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        runtime_schema: storage_schema.TableSchema,
        timestamp_ns: u64,
        plan: db_mod.types.RelationalRowsQueryPlan,
        consistency: raft_mod.ReadConsistency,
    ) !?db_mod.types.RelationalRowsQueryResult {
        const self: *BoundTableReadSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        try self.prepareRelationalRowsFullTableRead(consistency);
        return try executeSystemVersionedRowsPlanAsOfTimestampNsAlloc(alloc, self.db, runtime_schema, timestamp_ns, plan);
    }

    fn rowsQueryPlanCatalogSystemTimeAsOfTimestampNsNative(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        target: catalog_resources.TableTarget,
        runtime_schema: storage_schema.TableSchema,
        timestamp_ns: u64,
        plan: db_mod.types.RelationalRowsQueryPlan,
        consistency: raft_mod.ReadConsistency,
    ) !?db_mod.types.RelationalRowsQueryResult {
        return try rowsQueryPlanSystemTimeAsOfTimestampNs(ptr, alloc, target.table_name, runtime_schema, timestamp_ns, plan, consistency);
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

fn lookupOptionsAreDefault(opts: db_mod.types.LookupOptions) bool {
    return opts.fields.len == 0 and opts.include_all_fields;
}

const documentAlgebraicAggregateFromDbAlloc = sql_document.aggregateFromDbAlloc;
const documentAlgebraicAggregateMergeResponsesAlloc = sql_document.mergeResponsesAlloc;
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
    inference_api_url: ?[]const u8 = null,
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

    pub fn withInferenceAPIURL(
        self: *ProvisionedTableReadSource,
        inference_api_url: ?[]const u8,
    ) *ProvisionedTableReadSource {
        self.inference_api_url = inference_api_url;
        if (self.cache) |cache| cache.inference_api_url = inference_api_url;
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
                .document_artifact_manifest = ProvisionedTableReadSource.documentArtifactManifest,
                .document_artifact_manifests = ProvisionedTableReadSource.documentArtifactManifests,
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
                .rows_query_plan_system_time_as_of_timestamp_ns = rowsQueryPlanSystemTimeAsOfTimestampNs,
                .rows_query_plan_catalog_system_time_as_of_timestamp_ns = rowsQueryPlanCatalogSystemTimeAsOfTimestampNsNative,
                .rows_set_operation_plan = rowsSetOperationPlan,
                .rows_set_operation_plan_catalog = rowsSetOperationPlanCatalogNative,
                .rows_aggregate_plan = rowsAggregatePlan,
                .rows_window_plan = rowsWindowPlan,
                .rows_join_plan = rowsJoinPlan,
                .rows_lateral_plan = rowsLateralPlan,
                .relational_rows_source_group_local = relationalRowsSourceGroupLocal,
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
        const plans = try table_catalog.resolveTableDocKeyRanges(alloc, self.catalog, table_name, from_key, to_key);
        defer {
            for (plans) |*plan| plan.deinit(alloc);
            alloc.free(plans);
        }
        if (plans.len == 0) return null;
        try tableReadsValidateDocIdentityReadyForMultiGroup(alloc, self.catalog, table_name, plans.len);
        var out = std.ArrayListUnmanaged(u8).empty;
        defer out.deinit(alloc);

        var emitted: u32 = 0;
        for (plans) |plan| {
            var group_opts = opts;
            if (opts.limit > 0) {
                if (emitted >= opts.limit) break;
                group_opts.limit = opts.limit - emitted;
            }

            var result = (try scanProvisionedHostedLocal(self.cache, self.replica_root_dir, self.catalog, self.requester, alloc, plan.group_id, self.visibleRootGeneration(plan.group_id), self.backend_runtime, table_name, plan.start_key, plan.end_key, group_opts, consistency)) orelse continue;
            defer result.deinit(alloc);
            try out.appendSlice(alloc, result.ndjson);
            emitted += @intCast(std.mem.count(u8, result.ndjson, "\n"));
        }
        try table_catalog.validateTopologyEpoch(alloc, self.catalog, table_name, plans[0].topology_epoch);
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
            const execution = try queryHostedLocalDetailed(self.cache, self.replica_root_dir, self.catalog, self.requester, alloc, group_ids[0], self.visibleRootGeneration(group_ids[0]), self.backend_runtime, self.antfly_provider, self.inference_api_url, self.secret_store, self.remote_content, table_name, req, consistency);
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

    fn rowsQueryPlanSystemTimeAsOfAlloc(
        self: *ProvisionedTableReadSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        runtime_schema: storage_schema.TableSchema,
        plan: db_mod.types.RelationalRowsQueryPlan,
        system_time: table_read_core.RelationalRowsSourceGroupSystemTime,
        consistency: raft_mod.ReadConsistency,
    ) !?db_mod.types.RelationalRowsQueryResult {
        try self.ensureHAReadAllowed(consistency);
        if (self.prepare_for_read) |prep| prep.prepareForRead(table_name, .general);
        const plans = try resolveTableDocKeyRangesForRowsPlanAlloc(alloc, self.catalog, table_name, &.{});
        defer {
            for (plans) |*range_plan| range_plan.deinit(alloc);
            alloc.free(plans);
        }
        if (plans.len == 0) return null;
        try tableReadsValidateDocIdentityReadyForMultiGroup(alloc, self.catalog, table_name, plans.len);
        const schema_json = (try table_catalog.tableSchemaJsonAlloc(alloc, self.catalog, table_name)) orelse return error.TableNotFound;
        defer alloc.free(schema_json);
        const routed_source = self.source();
        var rows = std.ArrayListUnmanaged([]const u8).empty;
        errdefer {
            for (rows.items) |row| alloc.free(@constCast(row));
            rows.deinit(alloc);
        }
        var materialization = row_spill.JsonRowsMaterializationTracker.initDefault("system-time-as-of");
        for (plans) |range_plan| {
            var snapshot = (try routed_source.relationalRowsSourceGroupLocal(
                alloc,
                range_plan.group_id,
                table_name,
                schema_json,
                range_plan.topology_epoch,
                .{ .select_all = true },
                .{ .start = range_plan.start_key, .end = range_plan.end_key },
                system_time,
                consistency,
            )) orelse return error.TopologyChanged;
            defer snapshot.deinit(alloc);
            for (snapshot.rows, 0..) |row, row_index| {
                try materialization.account(row);
                try rows.append(alloc, row);
                snapshot.rows[row_index] = "";
            }
        }
        for (plans) |range_plan| {
            try table_catalog.validateGroupTopologyEpoch(alloc, self.catalog, table_name, range_plan.group_id, range_plan.topology_epoch);
        }
        var owned_rows = try rows.toOwnedSlice(alloc);
        errdefer freeOwnedJsonRows(alloc, owned_rows);
        if (materialization.spill_required) {
            owned_rows = try row_spill.spillAndReloadOwnedJsonRowsAlloc(alloc, owned_rows, materialization.bytes, "system-time-as-of");
        }
        defer freeOwnedJsonRows(alloc, owned_rows);
        return try relational_rows_api.executeRowsQueryPlanOnJsonRowsAlloc(alloc, runtime_schema, plan, owned_rows);
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
        return try self.rowsQueryPlanSystemTimeAsOfAlloc(alloc, table_name, runtime_schema, plan, .{ .sequence = commit_sequence }, consistency);
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

    fn rowsQueryPlanSystemTimeAsOfTimestampNs(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        runtime_schema: storage_schema.TableSchema,
        timestamp_ns: u64,
        plan: db_mod.types.RelationalRowsQueryPlan,
        consistency: raft_mod.ReadConsistency,
    ) !?db_mod.types.RelationalRowsQueryResult {
        const self: *ProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        return try self.rowsQueryPlanSystemTimeAsOfAlloc(alloc, table_name, runtime_schema, plan, .{ .timestamp_ns = timestamp_ns }, consistency);
    }

    fn rowsQueryPlanCatalogSystemTimeAsOfTimestampNsNative(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        target: catalog_resources.TableTarget,
        runtime_schema: storage_schema.TableSchema,
        timestamp_ns: u64,
        plan: db_mod.types.RelationalRowsQueryPlan,
        consistency: raft_mod.ReadConsistency,
    ) !?db_mod.types.RelationalRowsQueryResult {
        const self: *ProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        const table_name = try nativeCatalogTableNameAlloc(alloc, self.catalog, target);
        defer alloc.free(table_name);
        return try rowsQueryPlanSystemTimeAsOfTimestampNs(ptr, alloc, table_name, runtime_schema, timestamp_ns, plan, consistency);
    }

    fn relationalRowsSourceGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        schema_json: []const u8,
        topology_epoch: u64,
        req: db_mod.types.RelationalRowsQueryRequest,
        doc_key_range: db_mod.types.RelationalRowsDocKeyRange,
        system_time: ?table_read_core.RelationalRowsSourceGroupSystemTime,
        consistency: raft_mod.ReadConsistency,
    ) !?db_mod.types.RelationalRowsQueryResult {
        const self: *ProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        try self.ensureHAReadAllowed(consistency);
        if (self.prepare_for_read) |prep| prep.prepareForRead(table_name, .general);
        try table_catalog.validateGroupTopologyEpoch(alloc, self.catalog, table_name, group_id, topology_epoch);
        try tableReadsValidateDocIdentityReadyForMultiGroup(alloc, self.catalog, table_name, 1);
        var reads = raft_mod.FeatureDBReads.init(group_id, self.requester);
        try reads.reads.prepareScanWithConsistency(group_id, doc_key_range.start, doc_key_range.end, .{}, consistency);
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);
        var db = try openProvisionedQueryDbForTableWithRuntime(alloc, path, self.catalog, table_name, group_id, self.visibleRootGeneration(group_id), self.backend_runtime);
        defer db.close();
        return try executeRelationalRowsSourceGroupLocalAlloc(alloc, &db, schema_json, req, doc_key_range, system_time);
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
        const execution = try queryHostedLocalDetailed(self.cache, self.replica_root_dir, self.catalog, self.requester, alloc, group_id, self.visibleRootGeneration(group_id), self.backend_runtime, self.antfly_provider, self.inference_api_url, self.secret_store, self.remote_content, table_name, req, consistency);
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
        return try queryHostedLocal(self.cache, self.replica_root_dir, self.catalog, self.requester, alloc, group_id, self.visibleRootGeneration(group_id), self.backend_runtime, self.antfly_provider, self.inference_api_url, self.secret_store, self.remote_content, table_name, req, consistency);
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
                    ctx.source.inference_api_url,
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
                .document_artifact_manifest = HostedProvisionedTableReadSource.documentArtifactManifest,
                .document_artifact_manifests = HostedProvisionedTableReadSource.documentArtifactManifests,
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
                .rows_query_plan_system_time_as_of_timestamp_ns = HostedProvisionedTableReadSource.rowsQueryPlanSystemTimeAsOfTimestampNs,
                .rows_query_plan_catalog_system_time_as_of_timestamp_ns = HostedProvisionedTableReadSource.rowsQueryPlanCatalogSystemTimeAsOfTimestampNsNative,
                .rows_set_operation_plan = rowsSetOperationPlan,
                .rows_set_operation_plan_catalog = HostedProvisionedTableReadSource.rowsSetOperationPlanCatalogNative,
                .rows_aggregate_plan = rowsAggregatePlan,
                .rows_window_plan = rowsWindowPlan,
                .rows_join_plan = rowsJoinPlan,
                .rows_lateral_plan = rowsLateralPlan,
                .relational_rows_source_group_local = HostedProvisionedTableReadSource.relationalRowsSourceGroupLocal,
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

    fn documentArtifactManifest(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        doc_key: []const u8,
        artifact_name: []const u8,
        consistency: raft_mod.ReadConsistency,
    ) !?db_mod.types.DocumentArtifactManifest {
        const self: *HostedProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        const group_id = (try table_catalog.resolveGroupForKey(alloc, self.catalog, table_name, doc_key)) orelse return null;
        var route = (try table_router.resolveGroupRoute(alloc, self.catalog, self.router, group_id, routePolicyForConsistency(consistency))) orelse return null;
        defer route.deinit(alloc);
        return switch (route) {
            .local => try documentArtifactManifestProvisionedHostedLocal(
                null,
                self.replica_root_dir,
                self.catalog,
                self.requester,
                alloc,
                group_id,
                self.visibleRootGeneration(group_id),
                self.backend_runtime,
                table_name,
                doc_key,
                artifact_name,
                consistency,
            ),
            .remote => |remote| try documentArtifactManifestRemote(self.executor, alloc, remote.base_uri, group_id, table_name, doc_key, artifact_name),
        };
    }

    fn documentArtifactManifests(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        doc_key: []const u8,
        consistency: raft_mod.ReadConsistency,
    ) !?db_mod.types.DocumentArtifactManifestList {
        const self: *HostedProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        const group_id = (try table_catalog.resolveGroupForKey(alloc, self.catalog, table_name, doc_key)) orelse return null;
        var route = (try table_router.resolveGroupRoute(alloc, self.catalog, self.router, group_id, routePolicyForConsistency(consistency))) orelse return null;
        defer route.deinit(alloc);
        return switch (route) {
            .local => try documentArtifactManifestsProvisionedHostedLocal(
                null,
                self.replica_root_dir,
                self.catalog,
                self.requester,
                alloc,
                group_id,
                self.visibleRootGeneration(group_id),
                self.backend_runtime,
                table_name,
                doc_key,
                consistency,
            ),
            .remote => |remote| try documentArtifactManifestsRemote(self.executor, alloc, remote.base_uri, group_id, table_name, doc_key),
        };
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
                error.NotFound => null,
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
                error.NotFound => continue,
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
                error.LeaderUnavailable, error.UnexpectedHttpStatus => hostedRemoteUniqueOwnerLookupError(err),
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
                error.UnexpectedHttpStatus => hostedRemoteUniqueOwnerLookupError(err),
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
                error.UnexpectedHttpStatus => hostedRemoteUniqueOwnerLookupError(err),
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

    fn relationalRowsSourceGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        schema_json: []const u8,
        topology_epoch: u64,
        req: db_mod.types.RelationalRowsQueryRequest,
        doc_key_range: db_mod.types.RelationalRowsDocKeyRange,
        system_time: ?table_read_core.RelationalRowsSourceGroupSystemTime,
        consistency: raft_mod.ReadConsistency,
    ) !?db_mod.types.RelationalRowsQueryResult {
        const self: *HostedProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        var route = (try table_router.resolveGroupRoute(alloc, self.catalog, self.router, group_id, routePolicyForConsistency(consistency))) orelse return null;
        defer route.deinit(alloc);
        return switch (route) {
            .local => blk: {
                try table_catalog.validateGroupTopologyEpoch(alloc, self.catalog, table_name, group_id, topology_epoch);
                try tableReadsValidateDocIdentityReadyForMultiGroup(alloc, self.catalog, table_name, 1);
                var reads = raft_mod.FeatureDBReads.init(group_id, self.requester);
                try reads.reads.prepareScanWithConsistency(group_id, doc_key_range.start, doc_key_range.end, .{}, consistency);
                const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
                defer alloc.free(path);
                var db = try openProvisionedQueryDbForTableWithRuntime(alloc, path, self.catalog, table_name, group_id, self.visibleRootGeneration(group_id), self.backend_runtime);
                defer db.close();
                break :blk try executeRelationalRowsSourceGroupLocalAlloc(alloc, &db, schema_json, req, doc_key_range, system_time);
            },
            .remote => |remote| try rowsSourceGroupRemote(self.executor, alloc, remote.base_uri, group_id, table_name, .{
                .schema_json = schema_json,
                .topology_epoch = topology_epoch,
                .req = req,
                .doc_key_range = doc_key_range,
                .system_time = system_time,
            }),
        };
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

    fn rowsQueryPlanSystemTimeAsOfAlloc(
        self: *HostedProvisionedTableReadSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        runtime_schema: storage_schema.TableSchema,
        plan: db_mod.types.RelationalRowsQueryPlan,
        system_time: table_read_core.RelationalRowsSourceGroupSystemTime,
        consistency: raft_mod.ReadConsistency,
    ) !?db_mod.types.RelationalRowsQueryResult {
        const plans = try resolveTableDocKeyRangesForRowsPlanAlloc(alloc, self.catalog, table_name, &.{});
        defer {
            for (plans) |*range_plan| range_plan.deinit(alloc);
            alloc.free(plans);
        }
        if (plans.len == 0) return null;
        try tableReadsValidateDocIdentityReadyForMultiGroup(alloc, self.catalog, table_name, plans.len);
        const schema_json = (try table_catalog.tableSchemaJsonAlloc(alloc, self.catalog, table_name)) orelse return error.TableNotFound;
        defer alloc.free(schema_json);
        const routed_source = self.source();
        var rows = std.ArrayListUnmanaged([]const u8).empty;
        errdefer {
            for (rows.items) |row| alloc.free(@constCast(row));
            rows.deinit(alloc);
        }
        var materialization = row_spill.JsonRowsMaterializationTracker.initDefault("system-time-as-of");
        for (plans) |range_plan| {
            var snapshot = (try routed_source.relationalRowsSourceGroupLocal(
                alloc,
                range_plan.group_id,
                table_name,
                schema_json,
                range_plan.topology_epoch,
                .{ .select_all = true },
                .{ .start = range_plan.start_key, .end = range_plan.end_key },
                system_time,
                consistency,
            )) orelse return error.TopologyChanged;
            defer snapshot.deinit(alloc);
            for (snapshot.rows, 0..) |row, row_index| {
                try materialization.account(row);
                try rows.append(alloc, row);
                snapshot.rows[row_index] = "";
            }
        }
        for (plans) |range_plan| {
            try table_catalog.validateGroupTopologyEpoch(alloc, self.catalog, table_name, range_plan.group_id, range_plan.topology_epoch);
        }
        var owned_rows = try rows.toOwnedSlice(alloc);
        errdefer freeOwnedJsonRows(alloc, owned_rows);
        if (materialization.spill_required) {
            owned_rows = try row_spill.spillAndReloadOwnedJsonRowsAlloc(alloc, owned_rows, materialization.bytes, "system-time-as-of");
        }
        defer freeOwnedJsonRows(alloc, owned_rows);
        return try relational_rows_api.executeRowsQueryPlanOnJsonRowsAlloc(alloc, runtime_schema, plan, owned_rows);
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
        return try self.rowsQueryPlanSystemTimeAsOfAlloc(alloc, table_name, runtime_schema, plan, .{ .sequence = commit_sequence }, consistency);
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

    fn rowsQueryPlanSystemTimeAsOfTimestampNs(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        runtime_schema: storage_schema.TableSchema,
        timestamp_ns: u64,
        plan: db_mod.types.RelationalRowsQueryPlan,
        consistency: raft_mod.ReadConsistency,
    ) !?db_mod.types.RelationalRowsQueryResult {
        const self: *HostedProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        return try self.rowsQueryPlanSystemTimeAsOfAlloc(alloc, table_name, runtime_schema, plan, .{ .timestamp_ns = timestamp_ns }, consistency);
    }

    fn rowsQueryPlanCatalogSystemTimeAsOfTimestampNsNative(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        target: catalog_resources.TableTarget,
        runtime_schema: storage_schema.TableSchema,
        timestamp_ns: u64,
        plan: db_mod.types.RelationalRowsQueryPlan,
        consistency: raft_mod.ReadConsistency,
    ) !?db_mod.types.RelationalRowsQueryResult {
        const self: *HostedProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
        const table_name = try nativeCatalogTableNameAlloc(alloc, self.catalog, target);
        defer alloc.free(table_name);
        return try rowsQueryPlanSystemTimeAsOfTimestampNs(ptr, alloc, table_name, runtime_schema, timestamp_ns, plan, consistency);
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
        const plans = try table_catalog.resolveTableDocKeyRanges(alloc, self.catalog, table_name, from_key, to_key);
        defer {
            for (plans) |*plan| plan.deinit(alloc);
            alloc.free(plans);
        }
        if (plans.len == 0) return null;
        try tableReadsValidateDocIdentityReadyForMultiGroup(alloc, self.catalog, table_name, plans.len);

        var out = std.ArrayListUnmanaged(u8).empty;
        defer out.deinit(alloc);
        var emitted: u32 = 0;

        for (plans) |plan| {
            var group_opts = opts;
            if (opts.limit > 0) {
                if (emitted >= opts.limit) break;
                group_opts.limit = opts.limit - emitted;
            }

            var route = (try table_router.resolveGroupRoute(alloc, self.catalog, self.router, plan.group_id, routePolicyForConsistency(consistency))) orelse return null;
            defer route.deinit(alloc);

            var result = switch (route) {
                .local => try scanProvisionedHostedLocal(null, self.replica_root_dir, self.catalog, self.requester, alloc, plan.group_id, self.visibleRootGeneration(plan.group_id), self.backend_runtime, table_name, plan.start_key, plan.end_key, group_opts, consistency),
                .remote => |remote| try scanRemote(self.executor, alloc, remote.base_uri, plan.group_id, table_name, plan.start_key, plan.end_key, group_opts),
            } orelse return null;
            defer result.deinit(alloc);

            try out.appendSlice(alloc, result.ndjson);
            emitted += @intCast(std.mem.count(u8, result.ndjson, "\n"));
        }
        try table_catalog.validateTopologyEpoch(alloc, self.catalog, table_name, plans[0].topology_epoch);
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
                const execution = try queryHostedLocalDetailed(null, self.replica_root_dir, self.catalog, self.requester, alloc, group_ids[0], self.visibleRootGeneration(group_ids[0]), self.backend_runtime, null, null, null, null, table_name, req, consistency);
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
        const execution = try queryHostedLocalDetailed(null, self.replica_root_dir, self.catalog, self.requester, alloc, group_id, self.visibleRootGeneration(group_id), self.backend_runtime, null, null, null, null, table_name, req, consistency);
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
            .local => try queryHostedLocal(null, self.replica_root_dir, self.catalog, self.requester, alloc, group_id, self.visibleRootGeneration(group_id), self.backend_runtime, null, null, null, null, table_name, req, consistency),
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
            .remote => |remote| try joinPartitionRemote(self.executor, alloc, remote.base_uri, group_id, table_name, body),
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
            .remote => |remote| try joinRowsRemote(self.executor, alloc, remote.base_uri, group_id, table_name, body),
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
            .remote => |remote| try joinUnmatchedRemote(self.executor, alloc, remote.base_uri, group_id, table_name, body),
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
            .remote => |remote| try joinFinalizeRemote(self.executor, alloc, remote.base_uri, group_id, table_name, body),
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
            .remote => |remote| try joinJobStateRemote(self.executor, alloc, remote.base_uri, group_id, table_name, body),
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

const executeLoweredSqlReadPlanAlloc = table_read_relational_rows.executeLoweredSqlReadPlanAlloc;
const executeLoweredSqlReadPlanWithSessionAlloc = table_read_relational_rows.executeLoweredSqlReadPlanWithSessionAlloc;

const catalogTargetForLoweredSqlTable = table_read_relational_rows.catalogTargetForLoweredSqlTable;
const loweredSqlSetOperationToRowsOperation = table_read_relational_rows.loweredSetOperationToRowsOperation;

const catalogRuntimeSchemaUnlessDefaultAlloc = table_read_relational_rows.catalogRuntimeSchemaUnlessDefaultAlloc;

const loweredSqlReadJoinCteTableName = table_read_relational_rows.loweredReadJoinCteTableName;

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
    return try table_read_relational_rows.rowsQueryPlanFromRoutedScansAlloc(alloc, source, table_name, runtime_schema, plan, consistency);
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
    return try table_read_relational_rows.rowsAggregatePlanFromRoutedScansAlloc(alloc, source, table_name, runtime_schema, plan, consistency);
}

fn rowsSetOperationPlanFromRoutedScansAlloc(
    alloc: std.mem.Allocator,
    source: TableReadSource,
    table_name: []const u8,
    runtime_schema: storage_schema.TableSchema,
    plan: db_mod.types.RelationalRowsSetOperationPlan,
    consistency: raft_mod.ReadConsistency,
) !?db_mod.types.RelationalRowsQueryResult {
    if (runtime_schema.external_base_source != null) {
        return try rowsSetOperationPlanFromLakeScanAlloc(alloc, source, table_name, runtime_schema, plan, consistency);
    }
    return try table_read_relational_rows.rowsSetOperationPlanFromRoutedScansAlloc(alloc, source, table_name, runtime_schema, plan, consistency);
}

const rowsQueryPlanFromLakeScanAlloc = table_read_external_lake.rowsQueryPlanFromLakeScanAlloc;
const rowsAggregatePlanFromLakeScanAlloc = table_read_external_lake.rowsAggregatePlanFromLakeScanAlloc;
const rowsSetOperationPlanFromLakeScanAlloc = table_read_external_lake.rowsSetOperationPlanFromLakeScanAlloc;
const rowsWindowPlanFromLakeScanAlloc = table_read_external_lake.rowsWindowPlanFromLakeScanAlloc;
const rowsJoinPlanFromLakeScanAlloc = table_read_external_lake.rowsJoinPlanFromLakeScanAlloc;
const rowsLateralPlanFromLakeScanAlloc = table_read_external_lake.rowsLateralPlanFromLakeScanAlloc;

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
    return try table_read_relational_rows.rowsWindowPlanFromRoutedScansAlloc(alloc, source, table_name, runtime_schema, plan, consistency);
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

const rowsJoinPlanFromRoutedScansWithSchemasAlloc = table_read_relational_rows.rowsJoinPlanFromRoutedScansWithSchemasAlloc;

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

const rowsLateralPlanFromRoutedScansWithSchemasAlloc = table_read_relational_rows.rowsLateralPlanFromRoutedScansWithSchemasAlloc;
const relationalRowsEffectiveSideTable = table_read_relational_rows.effectiveSideTable;

const RoutedRows = table_read_relational_rows.RoutedRows;
const collectMergeTargetRowsFromRoutedScansAlloc = table_read_relational_rows.collectMergeTargetRowsFromRoutedScansAlloc;
const collectMergeSourceRowsFromRoutedScansAlloc = table_read_relational_rows.collectMergeSourceRowsFromRoutedScansAlloc;
const routedRowsPlanRangesForJoinAlloc = table_read_relational_rows.routedRowsPlanRangesForJoinAlloc;
const routedRowsPlanRangesForJoinCtesAlloc = table_read_relational_rows.routedRowsPlanRangesForJoinCtesAlloc;
const resolveSingleUniqueOwnerGroup = table_read_relational_rows.resolveSingleUniqueOwnerGroup;
const lookupRelationalUniqueOwnerInDb = table_read_relational_rows.lookupRelationalUniqueOwnerInDb;
const lookupRelationalTemporalUniqueOwnerInDb = table_read_relational_rows.lookupRelationalTemporalUniqueOwnerInDb;
const lookupRelationalTemporalUniqueOverlapOwnerInDb = table_read_relational_rows.lookupRelationalTemporalUniqueOverlapOwnerInDb;

fn hostedRemoteUniqueOwnerLookupError(err: anyerror) anyerror {
    return switch (err) {
        error.LeaderUnavailable, error.UnexpectedHttpStatus => error.UniqueOwnerTopologyUnavailable,
        else => err,
    };
}

test "hosted remote unique owner status errors stay unavailable" {
    try std.testing.expectEqual(error.UniqueOwnerTopologyUnavailable, hostedRemoteUniqueOwnerLookupError(error.LeaderUnavailable));
    try std.testing.expectEqual(error.UniqueOwnerTopologyUnavailable, hostedRemoteUniqueOwnerLookupError(error.UnexpectedHttpStatus));
    try std.testing.expectEqual(error.TopologyChanged, hostedRemoteUniqueOwnerLookupError(error.TopologyChanged));
}

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
                source.inference_api_url,
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
        shard_results[i] = try queryHostedLocal(self.cache, self.replica_root_dir, self.catalog, self.requester, alloc, group_id, self.visibleRootGeneration(group_id), self.backend_runtime, self.antfly_provider, self.inference_api_url, self.secret_store, self.remote_content, table_name, fan_in_shard_req.req, consistency);
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
            .local => try queryHostedLocal(null, self.replica_root_dir, self.catalog, self.requester, alloc, group_id, self.visibleRootGeneration(group_id), self.backend_runtime, null, null, null, null, table_name, fan_in_shard_req.req, consistency),
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
                ctx.source.antfly_provider,
                ctx.source.inference_api_url,
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

    var db = try openProvisionedLookupDbForTable(
        alloc,
        path,
        null,
        lsm_root_generation,
        null,
        backend_runtime,
        try loadTableIdentityNamespaceForGroup(alloc, catalog, table_name, group_id),
    );
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

    var db = try openProvisionedLookupDbForTable(
        alloc,
        path,
        null,
        lsm_root_generation,
        null,
        backend_runtime,
        try loadTableIdentityNamespaceForGroup(alloc, catalog, table_name, group_id),
    );
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
        try appendScanLine(alloc, &out, entry.id, json, try db.getTimestamp(alloc, entry.id));
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
            try appendScanLine(alloc, &out, entry.id, json, try db.getTimestamp(alloc, entry.id));
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
            try appendScanLine(alloc, &out, entry.id, json, try db.getTimestamp(alloc, entry.id));
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
    inference_api_url: ?[]const u8,
    secret_store: ?*common_secrets.FileStore,
    remote_content: ?*const scraping.RemoteContentConfig,
    table_name: []const u8,
    req: db_mod.types.SearchRequest,
    consistency: raft_mod.ReadConsistency,
) !db_mod.types.SearchResult {
    const detailed = try queryLocalDetailed(cache, replica_root_dir, catalog, requester, alloc, group_id, lsm_root_generation, backend_runtime, antfly_provider, inference_api_url, secret_store, remote_content, table_name, req, consistency);
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
    inference_api_url: ?[]const u8,
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
        var db = try openProvisionedQueryDbForTableWithCache(alloc, path, catalog, table_name, null, null, lsm_root_generation, null, backend_runtime, antfly_provider, inference_api_url, secret_store, remote_content, identity_namespace);
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
    inference_api_url: ?[]const u8,
    secret_store: ?*common_secrets.FileStore,
    remote_content: ?*const scraping.RemoteContentConfig,
    table_name: []const u8,
    req: db_mod.types.SearchRequest,
    consistency: raft_mod.ReadConsistency,
) !db_mod.types.SearchResult {
    const detailed = try queryHostedLocalDetailed(cache, replica_root_dir, catalog, requester, alloc, group_id, lsm_root_generation, backend_runtime, antfly_provider, inference_api_url, secret_store, remote_content, table_name, req, consistency);
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
    inference_api_url: ?[]const u8,
    secret_store: ?*common_secrets.FileStore,
    remote_content: ?*const scraping.RemoteContentConfig,
    table_name: []const u8,
    req: db_mod.types.SearchRequest,
    consistency: raft_mod.ReadConsistency,
) !LocalQueryExecution {
    return queryLocalDetailed(cache, replica_root_dir, catalog, requester, alloc, group_id, lsm_root_generation, backend_runtime, antfly_provider, inference_api_url, secret_store, remote_content, table_name, req, consistency) catch |err| switch (err) {
        error.NotLeader => if (consistency == .stale) err else try queryLocalDetailed(cache, replica_root_dir, catalog, requester, alloc, group_id, lsm_root_generation, backend_runtime, antfly_provider, inference_api_url, secret_store, remote_content, table_name, req, .stale),
        else => err,
    };
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

fn documentArtifactManifestRemote(
    executor: http_common.RequestExecutor,
    alloc: std.mem.Allocator,
    base_uri: []const u8,
    group_id: u64,
    table_name: []const u8,
    doc_key: []const u8,
    artifact_name: []const u8,
) !?db_mod.types.DocumentArtifactManifest {
    var client = http_client.ApiHttpClient.init(alloc, executor);
    var response = client.fetchGroupDocumentArtifactManifest(base_uri, group_id, table_name, doc_key, artifact_name) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    defer response.deinit(alloc);
    return try std.json.parseFromSliceLeaky(db_mod.types.DocumentArtifactManifest, alloc, response.body, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = true,
    });
}

fn documentArtifactManifestsRemote(
    executor: http_common.RequestExecutor,
    alloc: std.mem.Allocator,
    base_uri: []const u8,
    group_id: u64,
    table_name: []const u8,
    doc_key: []const u8,
) !?db_mod.types.DocumentArtifactManifestList {
    var client = http_client.ApiHttpClient.init(alloc, executor);
    var response = client.fetchGroupDocumentArtifactManifests(base_uri, group_id, table_name, doc_key) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    defer response.deinit(alloc);
    return try std.json.parseFromSliceLeaky(db_mod.types.DocumentArtifactManifestList, alloc, response.body, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = true,
    });
}

fn parseJsonTestBody(comptime T: type, alloc: std.mem.Allocator, body: []const u8) !std.json.Parsed(T) {
    return try std.json.parseFromSlice(T, alloc, body, .{ .ignore_unknown_fields = true });
}

fn expectSingleNativeQueryHitId(alloc: std.mem.Allocator, body: []const u8, expected_id: []const u8) !void {
    var parsed = try parseJsonTestBody(metadata_openapi.QueryResponses, alloc, body);
    defer parsed.deinit();
    try std.testing.expectEqual(@as(usize, 1), parsed.value.responses.?.len);
    const hits = parsed.value.responses.?[0].hits.?.hits.?;
    try std.testing.expectEqual(@as(usize, 1), hits.len);
    try std.testing.expectEqualStrings(expected_id, hits[0]._id);
}

fn expectNativeQueryTotal(alloc: std.mem.Allocator, body: []const u8, expected_total: i64) !void {
    var parsed = try parseJsonTestBody(metadata_openapi.QueryResponses, alloc, body);
    defer parsed.deinit();
    try std.testing.expectEqual(@as(usize, 1), parsed.value.responses.?.len);
    try std.testing.expectEqual(expected_total, parsed.value.responses.?[0].hits.?.total.?);
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

fn openTestGroupDb(alloc: std.mem.Allocator, path: []const u8, group_id: u64) !db_mod.DB {
    return try db_mod.DB.open(alloc, path, .{
        .identity_namespace = .{
            .table_id = 7,
            .shard_id = group_id,
            .range_id = group_id,
        },
    });
}

test "bound table read source uses feature db reads and returns version" {
    const alloc = std.testing.allocator;
    const path = try uniqueTestTmpPathAlloc(alloc, "antfly-api-table-reads");
    defer alloc.free(path);

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

    var scan = (try source.source().scan(alloc, "docs", "", "", .{
        .include_documents = true,
        .fields = &.{"title"},
        .include_all_fields = false,
    }, .read_index)).?;
    defer scan.deinit(alloc);
    const ScanRow = struct {
        key: []const u8,
        version: u64,
        title: []const u8,
    };
    const rows = try parseNdjsonTestRowsAlloc(ScanRow, alloc, scan.ndjson);
    defer alloc.free(rows);
    try std.testing.expectEqual(@as(usize, 1), rows.len);
    try std.testing.expectEqualStrings("doc:a", rows[0].key);
    try std.testing.expectEqual(@as(u64, 1234), rows[0].version);
    try std.testing.expectEqualStrings("alpha", rows[0].title);
}

test "bound table read source executes SQL system-time as-of by commit sequence" {
    const alloc = std.testing.allocator;
    const path = try uniqueTestTmpPathAlloc(alloc, "antfly-api-table-system-time");
    defer alloc.free(path);

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
    try db.batch(.{
        .writes = &.{.{ .key = "row:1", .value = "{\"id\":\"row:1\",\"name\":\"first\"}" }},
        .timestamp_ns = 10_000,
    });
    try db.batch(.{
        .writes = &.{.{ .key = "row:1", .value = "{\"id\":\"row:1\",\"name\":\"second\"}" }},
        .timestamp_ns = 20_000,
    });

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
    var lowered = try sql_adapter.lower_select.lowerReadPlanAlloc(alloc, sql, versioned_schema, &.{});
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

    var lowered_timestamp = try sql_adapter.lower_select.lowerReadPlanAlloc(
        alloc,
        "SELECT id, name FROM docs FOR SYSTEM_TIME AS OF TIMESTAMP '1970-01-01T00:00:00.000010Z' WHERE id = 'row:1'",
        versioned_schema,
        &.{},
    );
    defer lowered_timestamp.deinit(alloc);
    var timestamp_result = (try executeLoweredSqlReadPlanAlloc(
        alloc,
        source.source(),
        catalog.iface(),
        "docs",
        versioned_schema,
        lowered_timestamp,
        .read_index,
    )).?;
    defer timestamp_result.deinit(alloc);
    switch (timestamp_result) {
        .query => |query_result| {
            try std.testing.expectEqual(@as(u32, 1), query_result.total);
            try std.testing.expectEqual(@as(usize, 1), query_result.rows.len);
            try std.testing.expect(std.mem.indexOf(u8, query_result.rows[0], "\"first\"") != null);
            try std.testing.expect(std.mem.indexOf(u8, query_result.rows[0], "\"second\"") == null);
        },
        else => return error.TestUnexpectedResult,
    }

    const cte_predicates = [_]storage_schema.RelationalCheck{
        .{ .name = "", .field = "id", .op = .eq, .value_json = "\"row:1\"" },
    };
    const final_predicates = [_]storage_schema.RelationalCheck{
        .{ .name = "", .field = "name", .op = .eq, .value_json = "\"first\"" },
    };
    const cte_select = [_][]const u8{ "id", "name" };
    const ctes = [_]db_mod.types.RelationalRowsCte{.{
        .name = "historical_docs",
        .query = .{
            .predicates = cte_predicates[0..],
            .select = cte_select[0..],
            .select_all = false,
        },
    }};
    var cte_result = (try source.source().rowsQueryPlanSystemTimeAsOfSequence(
        alloc,
        "docs",
        versioned_schema,
        commit_sequence,
        .{
            .ctes = ctes[0..],
            .query = .{
                .source_cte = "historical_docs",
                .predicates = final_predicates[0..],
                .select = cte_select[0..],
                .select_all = false,
            },
        },
        .read_index,
    )).?;
    defer cte_result.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 1), cte_result.total);
    try std.testing.expectEqual(@as(usize, 1), cte_result.rows.len);
    try std.testing.expect(std.mem.indexOf(u8, cte_result.rows[0], "\"first\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, cte_result.rows[0], "\"second\"") == null);

    var group_local_as_of = (try source.source().relationalRowsSourceGroupLocal(
        alloc,
        77,
        "docs",
        versioned_schema_json,
        0,
        .{
            .predicates = final_predicates[0..],
            .select = cte_select[0..],
            .select_all = false,
        },
        .{ .start = "row:0", .end = "row:2" },
        .{ .sequence = commit_sequence },
        .read_index,
    )).?;
    defer group_local_as_of.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 1), group_local_as_of.total);
    try std.testing.expectEqual(@as(usize, 1), group_local_as_of.rows.len);
    try std.testing.expect(std.mem.indexOf(u8, group_local_as_of.rows[0], "\"first\"") != null);

    var excluded_group_local_as_of = (try source.source().relationalRowsSourceGroupLocal(
        alloc,
        77,
        "docs",
        versioned_schema_json,
        0,
        .{
            .predicates = final_predicates[0..],
            .select = cte_select[0..],
            .select_all = false,
        },
        .{ .start = "row:2", .end = "" },
        .{ .sequence = commit_sequence },
        .read_index,
    )).?;
    defer excluded_group_local_as_of.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 0), excluded_group_local_as_of.total);

    const set_sql = try std.fmt.allocPrint(
        alloc,
        "SELECT id FROM docs FOR SYSTEM_TIME AS OF {d} WHERE name = 'first' UNION ALL SELECT id FROM docs FOR SYSTEM_TIME AS OF {d} WHERE name = 'first'",
        .{ commit_sequence, commit_sequence },
    );
    defer alloc.free(set_sql);
    var lowered_set = try sql_adapter.lower_select.lowerReadPlanAlloc(alloc, set_sql, versioned_schema, &.{});
    defer lowered_set.deinit(alloc);
    var set_result = (try executeLoweredSqlReadPlanAlloc(
        alloc,
        source.source(),
        catalog.iface(),
        "docs",
        versioned_schema,
        lowered_set,
        .read_index,
    )).?;
    defer set_result.deinit(alloc);
    switch (set_result) {
        .set_operation => |query_result| {
            try std.testing.expectEqual(@as(u32, 2), query_result.total);
            try std.testing.expectEqual(@as(usize, 2), query_result.rows.len);
            try std.testing.expect(std.mem.indexOf(u8, query_result.rows[0], "\"row:1\"") != null);
            try std.testing.expect(std.mem.indexOf(u8, query_result.rows[1], "\"row:1\"") != null);
            try std.testing.expect(std.mem.indexOf(u8, query_result.rows[0], "\"second\"") == null);
            try std.testing.expect(std.mem.indexOf(u8, query_result.rows[1], "\"second\"") == null);
        },
        else => return error.TestUnexpectedResult,
    }

    const timestamp_set_sql =
        "SELECT id FROM docs FOR SYSTEM_TIME AS OF '1970-01-01T00:00:00.000010Z' WHERE name = 'first' UNION ALL SELECT id FROM docs FOR SYSTEM_TIME AS OF TIMESTAMP '1970-01-01T00:00:00.000010Z' WHERE name = 'first'";
    var lowered_timestamp_set = try sql_adapter.lower_select.lowerReadPlanAlloc(alloc, timestamp_set_sql, versioned_schema, &.{});
    defer lowered_timestamp_set.deinit(alloc);
    var timestamp_set_result = (try executeLoweredSqlReadPlanAlloc(
        alloc,
        source.source(),
        catalog.iface(),
        "docs",
        versioned_schema,
        lowered_timestamp_set,
        .read_index,
    )).?;
    defer timestamp_set_result.deinit(alloc);
    switch (timestamp_set_result) {
        .set_operation => |query_result| {
            try std.testing.expectEqual(@as(u32, 2), query_result.total);
            try std.testing.expectEqual(@as(usize, 2), query_result.rows.len);
            try std.testing.expect(std.mem.indexOf(u8, query_result.rows[0], "\"row:1\"") != null);
            try std.testing.expect(std.mem.indexOf(u8, query_result.rows[1], "\"row:1\"") != null);
        },
        else => return error.TestUnexpectedResult,
    }

    const mismatched_set_sql = try std.fmt.allocPrint(
        alloc,
        "SELECT id FROM docs FOR SYSTEM_TIME AS OF {d} WHERE name = 'first' UNION ALL SELECT id FROM docs FOR SYSTEM_TIME AS OF {d} WHERE name = 'second'",
        .{ commit_sequence, commit_sequence + 1 },
    );
    defer alloc.free(mismatched_set_sql);
    if (sql_adapter.lower_select.lowerReadPlanAlloc(alloc, mismatched_set_sql, versioned_schema, &.{})) |lowered_mismatched| {
        var mismatched = lowered_mismatched;
        mismatched.deinit(alloc);
        return error.TestExpectedError;
    } else |err| switch (err) {
        error.UnsupportedSqlShape => {},
        else => return err,
    }

    if (sql_adapter.lower_select.lowerReadPlanAlloc(alloc, "SELECT id FROM docs FOR SYSTEM_TIME AS OF 'not-a-timestamp'", versioned_schema, &.{})) |lowered_invalid| {
        var invalid_lowered = lowered_invalid;
        invalid_lowered.deinit(alloc);
        return error.TestExpectedError;
    } else |err| switch (err) {
        error.UnexpectedToken, error.UnsupportedSqlShape => {},
        else => return err,
    }

    try db.batch(.{
        .writes = &.{.{ .key = "row:1", .value = "{\"id\":\"row:1\",\"name\":\"third\"}" }},
        .timestamp_ns = 30_000,
    });
    try std.testing.expectEqual(@as(usize, 1), try db.pruneSystemVersionedHistoryBeforeTimestampNs(alloc, 30_000));
    try std.testing.expectError(error.SystemVersionedHistoryPruned, source.source().rowsQueryPlanSystemTimeAsOfSequence(
        alloc,
        "docs",
        versioned_schema,
        commit_sequence,
        .{
            .query = .{
                .predicates = cte_predicates[0..],
                .select = cte_select[0..],
                .select_all = false,
            },
        },
        .read_index,
    ));
    try std.testing.expectError(error.SystemVersionedHistoryPruned, executeLoweredSqlReadPlanAlloc(
        alloc,
        source.source(),
        catalog.iface(),
        "docs",
        versioned_schema,
        lowered_timestamp,
        .read_index,
    ));

    var retained_floor_result = (try source.source().rowsQueryPlanSystemTimeAsOfTimestampNs(
        alloc,
        "docs",
        versioned_schema,
        30_000,
        .{
            .query = .{
                .predicates = cte_predicates[0..],
                .select = cte_select[0..],
                .select_all = false,
            },
        },
        .read_index,
    )).?;
    defer retained_floor_result.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 1), retained_floor_result.total);
    try std.testing.expectEqual(@as(usize, 1), retained_floor_result.rows.len);
    try std.testing.expect(std.mem.indexOf(u8, retained_floor_result.rows[0], "\"third\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, retained_floor_result.rows[0], "\"first\"") == null);
    try std.testing.expect(std.mem.indexOf(u8, retained_floor_result.rows[0], "\"second\"") == null);
}

test "provisioned table read source composes local system-time as-of with cte plans" {
    const alloc = std.testing.allocator;
    const path = try uniqueTestTmpPathAlloc(alloc, "antfly-api-provisioned-system-time-cte");
    defer alloc.free(path);

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const group_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(group_path);
    var db = try openTestGroupDb(alloc, group_path, 7001);
    defer db.close();
    const group_2_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7002);
    defer alloc.free(group_2_path);
    var db_2 = try openTestGroupDb(alloc, group_2_path, 7002);
    defer db_2.close();

    const base_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"name":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    const versioned_schema_json =
        \\{"version":2,"storage_mode":"relational","default_type":"row","enforce_types":true,"system_versioned":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"name":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;

    try db.applyTableSchemaJson(alloc, base_schema_json, .{});
    try db.batch(.{ .writes = &.{.{ .key = "row:1", .value = "{\"id\":\"row:1\",\"name\":\"before\"}" }} });
    try db_2.applyTableSchemaJson(alloc, base_schema_json, .{});
    try db_2.batch(.{ .writes = &.{.{ .key = "row:2", .value = "{\"id\":\"row:2\",\"name\":\"before\"}" }} });

    var versioned_parsed = try schema_api.parseValidatedTableSchema(alloc, versioned_schema_json);
    defer versioned_parsed.deinit(alloc);
    const versioned_schema = try schema_api.deriveRuntimeTableSchema(alloc, versioned_parsed);
    defer storage_schema.freeSchema(alloc, versioned_schema);
    const row_2_key = try relational_rows_api.physicalPrimaryKeyFromRowJsonAlloc(alloc, versioned_schema, "{\"id\":\"row:2\"}");
    defer alloc.free(row_2_key);
    try db.applyTableSchemaJson(alloc, versioned_schema_json, .{});
    try db.batch(.{
        .writes = &.{.{ .key = "row:1", .value = "{\"id\":\"row:1\",\"name\":\"first\"}" }},
        .timestamp_ns = 10_000,
    });
    try db.batch(.{
        .writes = &.{.{ .key = "row:1", .value = "{\"id\":\"row:1\",\"name\":\"second\"}" }},
        .timestamp_ns = 20_000,
    });
    try db_2.applyTableSchemaJson(alloc, versioned_schema_json, .{});
    try db_2.batch(.{
        .writes = &.{.{ .key = "row:2", .value = "{\"id\":\"row:2\",\"name\":\"group-two-first\"}" }},
        .timestamp_ns = 10_000,
    });
    try db_2.batch(.{
        .writes = &.{.{ .key = "row:2", .value = "{\"id\":\"row:2\",\"name\":\"group-two-second\"}" }},
        .timestamp_ns = 20_000,
    });

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

    const FakeCatalog = struct {
        ranges: []metadata_table_manager.RangeRecord,
        statuses: []metadata_reconciler.MergedGroupStatus,

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
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{ .table_id = 7, .name = "docs", .placement_role = "data", .schema_json = versioned_schema_json }})[0..]),
                .ranges = self.ranges,
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
                .merged_group_statuses = self.statuses,
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const cte_predicates = [_]storage_schema.RelationalCheck{
        .{ .name = "", .field = "id", .op = .eq, .value_json = "\"row:1\"" },
    };
    const final_predicates = [_]storage_schema.RelationalCheck{
        .{ .name = "", .field = "name", .op = .eq, .value_json = "\"first\"" },
    };
    const cte_select = [_][]const u8{ "id", "name" };
    const ctes = [_]db_mod.types.RelationalRowsCte{.{
        .name = "historical_docs",
        .query = .{
            .predicates = cte_predicates[0..],
            .select = cte_select[0..],
            .select_all = false,
        },
    }};
    var catalog_ranges = [_]metadata_table_manager.RangeRecord{
        .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = "row:2" },
        .{ .group_id = 7002, .table_id = 7, .start_key = "row:2", .end_key = null },
    };
    var catalog_statuses = [_]metadata_reconciler.MergedGroupStatus{
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
    var catalog = FakeCatalog{ .ranges = catalog_ranges[0..], .statuses = catalog_statuses[0..] };
    var source = ProvisionedTableReadSource.init(path, catalog.iface(), raft_mod.read_gate.noopReadableLeaseRequester());
    var cte_result = (try source.source().rowsQueryPlanSystemTimeAsOfSequence(
        alloc,
        "docs",
        versioned_schema,
        commit_sequence,
        .{
            .ctes = ctes[0..],
            .query = .{
                .source_cte = "historical_docs",
                .predicates = final_predicates[0..],
                .select = cte_select[0..],
                .select_all = false,
            },
        },
        .read_index,
    )).?;
    defer cte_result.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 1), cte_result.total);
    try std.testing.expectEqual(@as(usize, 1), cte_result.rows.len);
    try std.testing.expect(std.mem.indexOf(u8, cte_result.rows[0], "\"first\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, cte_result.rows[0], "\"second\"") == null);

    const group_two_predicates = [_]storage_schema.RelationalCheck{
        .{ .name = "", .field = "name", .op = .eq, .value_json = "\"group-two-first\"" },
    };
    var multi_range_result = (try source.source().rowsQueryPlanSystemTimeAsOfTimestampNs(
        alloc,
        "docs",
        versioned_schema,
        10_000,
        .{
            .query = .{
                .predicates = group_two_predicates[0..],
                .select = cte_select[0..],
                .select_all = false,
            },
        },
        .read_index,
    )).?;
    defer multi_range_result.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 1), multi_range_result.total);
    try std.testing.expectEqual(@as(usize, 1), multi_range_result.rows.len);
    try std.testing.expect(std.mem.indexOf(u8, multi_range_result.rows[0], "\"row:2\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, multi_range_result.rows[0], "\"group-two-first\"") != null);

    const include_range = [_]db_mod.types.RelationalRowsDocKeyRange{.{ .start = "", .end = row_2_key }};
    var ranged_result = (try source.source().rowsQueryPlanSystemTimeAsOfSequence(
        alloc,
        "docs",
        versioned_schema,
        commit_sequence,
        .{
            .ranges = include_range[0..],
            .query = .{
                .predicates = final_predicates[0..],
                .select = cte_select[0..],
                .select_all = false,
            },
        },
        .read_index,
    )).?;
    defer ranged_result.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 1), ranged_result.total);
    try std.testing.expectEqual(@as(usize, 1), ranged_result.rows.len);
    try std.testing.expect(std.mem.indexOf(u8, ranged_result.rows[0], "\"first\"") != null);

    const exclude_range = [_]db_mod.types.RelationalRowsDocKeyRange{.{ .start = row_2_key, .end = "" }};
    var excluded_result = (try source.source().rowsQueryPlanSystemTimeAsOfSequence(
        alloc,
        "docs",
        versioned_schema,
        commit_sequence,
        .{
            .ranges = exclude_range[0..],
            .query = .{
                .predicates = final_predicates[0..],
                .select = cte_select[0..],
                .select_all = false,
            },
        },
        .read_index,
    )).?;
    defer excluded_result.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 0), excluded_result.total);
    try std.testing.expectEqual(@as(usize, 0), excluded_result.rows.len);
}

test "api.table_reads.hosted remote system-time as-of routes through rows source group local" {
    const alloc = std.testing.allocator;
    const path = "/tmp/unused-antfly-hosted-remote-asof-rows-source";
    const versioned_schema_json =
        \\{"version":2,"storage_mode":"relational","default_type":"row","enforce_types":true,"system_versioned":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"name":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var versioned_parsed = try schema_api.parseValidatedTableSchema(alloc, versioned_schema_json);
    defer versioned_parsed.deinit(alloc);
    const versioned_schema = try schema_api.deriveRuntimeTableSchema(alloc, versioned_parsed);
    defer storage_schema.freeSchema(alloc, versioned_schema);

    const FakeCatalog = struct {
        topology_changed: bool = false,

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

        const changed_statuses = [_]metadata_reconciler.MergedGroupStatus{.{
            .group_id = 7003,
            .doc_identity = .{
                .namespace_table_id = 7,
                .namespace_shard_id = 7003,
                .namespace_range_id = 7003,
                .next_ordinal = 2,
                .allocated_ordinals = 1,
                .state_rows = 1,
                .live_ordinals = 1,
                .complete = true,
            },
        }};

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
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{ .table_id = 7, .name = "docs", .placement_role = "data", .schema_json = versioned_schema_json }})[0..]),
                .ranges = if (self.topology_changed)
                    @constCast((&[_]metadata_table_manager.RangeRecord{.{ .group_id = 7003, .table_id = 7, .start_key = "", .end_key = null }})[0..])
                else
                    @constCast((&[_]metadata_table_manager.RangeRecord{
                        .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = "row:2" },
                        .{ .group_id = 7002, .table_id = 7, .start_key = "row:2", .end_key = null },
                    })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
                .merged_group_statuses = if (self.topology_changed) @constCast(changed_statuses[0..]) else @constCast(statuses[0..]),
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

        fn groupLeaderNodeId(_: *anyopaque, group_id: u64) ?u64 {
            return if (group_id == 7001 or group_id == 7002) 2 else null;
        }

        fn nodeStatus(_: *anyopaque, node_id: u64, group_id: u64) raft_mod.HostedReplicaStatus {
            return if (node_id == 2 and (group_id == 7001 or group_id == 7002)) .active else .absent;
        }

        fn nodeBaseUri(_: *anyopaque, alloc_inner: std.mem.Allocator, node_id: u64) !?[]u8 {
            if (node_id != 2) return null;
            return try alloc_inner.dupe(u8, "http://remote.test");
        }
    };

    const ExecutorState = struct {
        const Mode = enum {
            sequence_cte,
            timestamp_direct,
            ranged_sequence,
            pruned,
            topology_changed,
            remote_unavailable,
        };

        calls: usize = 0,
        mode: Mode = .sequence_cte,
        catalog: *FakeCatalog,

        fn iface(self: *@This()) http_common.RequestExecutor {
            return .{ .ptr = self, .vtable = &.{ .execute = execute } };
        }

        fn execute(ptr: *anyopaque, alloc_inner: std.mem.Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            try std.testing.expectEqual(http_common.Method.POST, req.method);
            const group_id: u64 = if (std.mem.indexOf(u8, req.uri, "/groups/7001/") != null)
                7001
            else if (std.mem.indexOf(u8, req.uri, "/groups/7002/") != null)
                7002
            else
                return error.TestUnexpectedResult;
            var parsed = try std.json.parseFromSlice(table_read_core.RelationalRowsSourceGroupRequest, alloc_inner, req.body, .{
                .allocate = .alloc_always,
                .ignore_unknown_fields = true,
            });
            defer parsed.deinit();
            try std.testing.expectEqualStrings(versioned_schema_json, parsed.value.schema_json);
            try std.testing.expect(parsed.value.topology_epoch != 0);
            try std.testing.expect(parsed.value.req.select_all);
            if (group_id == 7001) {
                try std.testing.expectEqualStrings("", parsed.value.doc_key_range.start);
                try std.testing.expectEqualStrings("row:2", parsed.value.doc_key_range.end);
            } else {
                try std.testing.expectEqualStrings("row:2", parsed.value.doc_key_range.start);
                try std.testing.expectEqualStrings("", parsed.value.doc_key_range.end);
            }
            switch (self.mode) {
                .sequence_cte => {
                    try std.testing.expectEqual(@as(u64, 44), parsed.value.system_time.?.sequence.?);
                    try std.testing.expect(parsed.value.system_time.?.timestamp_ns == null);
                },
                .timestamp_direct => {
                    try std.testing.expect(parsed.value.system_time.?.sequence == null);
                    try std.testing.expectEqual(@as(u64, 55_000), parsed.value.system_time.?.timestamp_ns.?);
                },
                .ranged_sequence => {
                    try std.testing.expectEqual(@as(u64, 88), parsed.value.system_time.?.sequence.?);
                    try std.testing.expect(parsed.value.system_time.?.timestamp_ns == null);
                },
                .pruned => {
                    try std.testing.expect(parsed.value.system_time.?.sequence == null);
                    try std.testing.expectEqual(@as(u64, 13_000), parsed.value.system_time.?.timestamp_ns.?);
                    return .{
                        .status = 410,
                        .body = try alloc_inner.dupe(u8, "system-versioned history pruned"),
                    };
                },
                .topology_changed => {
                    try std.testing.expectEqual(@as(u64, 66), parsed.value.system_time.?.sequence.?);
                    try std.testing.expect(parsed.value.system_time.?.timestamp_ns == null);
                    self.catalog.topology_changed = true;
                },
                .remote_unavailable => {
                    try std.testing.expectEqual(@as(u64, 77), parsed.value.system_time.?.sequence.?);
                    try std.testing.expect(parsed.value.system_time.?.timestamp_ns == null);
                    return .{
                        .status = 503,
                        .body = try alloc_inner.dupe(u8, "leader unavailable"),
                    };
                },
            }
            const row_body = if (group_id == 7001)
                "{\"rows\":[\"{\\\"id\\\":\\\"row:1\\\",\\\"name\\\":\\\"first\\\"}\"],\"total\":1}"
            else
                "{\"rows\":[\"{\\\"id\\\":\\\"row:2\\\",\\\"name\\\":\\\"second\\\"}\"],\"total\":1}";
            return .{
                .status = 200,
                .body = try alloc_inner.dupe(u8, row_body),
            };
        }
    };

    const final_predicates = [_]storage_schema.RelationalCheck{
        .{ .name = "", .field = "name", .op = .eq, .value_json = "\"second\"" },
    };
    const cte_select = [_][]const u8{ "id", "name" };
    const ctes = [_]db_mod.types.RelationalRowsCte{.{
        .name = "historical_docs",
        .query = .{
            .select = cte_select[0..],
            .select_all = false,
        },
    }};

    var catalog = FakeCatalog{};
    var executor_state = ExecutorState{ .catalog = &catalog };
    var hosted = HostedProvisionedTableReadSource.init(
        path,
        catalog.iface(),
        raft_mod.read_gate.noopReadableLeaseRequester(),
        FakeRouter.iface(),
        executor_state.iface(),
    );
    var result = (try hosted.source().rowsQueryPlanSystemTimeAsOfSequence(
        alloc,
        "docs",
        versioned_schema,
        44,
        .{
            .ctes = ctes[0..],
            .query = .{
                .source_cte = "historical_docs",
                .predicates = final_predicates[0..],
                .select = cte_select[0..],
                .select_all = false,
            },
        },
        .read_index,
    )).?;
    defer result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), executor_state.calls);
    try std.testing.expectEqual(@as(u32, 1), result.total);
    try std.testing.expectEqual(@as(usize, 1), result.rows.len);
    try std.testing.expect(std.mem.indexOf(u8, result.rows[0], "\"second\"") != null);

    executor_state.mode = .timestamp_direct;
    var timestamp_result = (try hosted.source().rowsQueryPlanSystemTimeAsOfTimestampNs(
        alloc,
        "docs",
        versioned_schema,
        55_000,
        .{},
        .read_index,
    )).?;
    defer timestamp_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 4), executor_state.calls);
    try std.testing.expectEqual(@as(u32, 2), timestamp_result.total);
    try std.testing.expect(std.mem.indexOf(u8, timestamp_result.rows[0], "\"first\"") != null or std.mem.indexOf(u8, timestamp_result.rows[1], "\"first\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, timestamp_result.rows[0], "\"second\"") != null or std.mem.indexOf(u8, timestamp_result.rows[1], "\"second\"") != null);

    executor_state.mode = .pruned;
    try std.testing.expectError(error.SystemVersionedHistoryPruned, hosted.source().rowsQueryPlanSystemTimeAsOfTimestampNs(
        alloc,
        "docs",
        versioned_schema,
        13_000,
        .{},
        .read_index,
    ));
    try std.testing.expectEqual(@as(usize, 5), executor_state.calls);

    executor_state.mode = .topology_changed;
    try std.testing.expectError(error.TopologyChanged, hosted.source().rowsQueryPlanSystemTimeAsOfSequence(
        alloc,
        "docs",
        versioned_schema,
        66,
        .{},
        .read_index,
    ));
    try std.testing.expectEqual(@as(usize, 7), executor_state.calls);

    catalog.topology_changed = false;
    executor_state.mode = .remote_unavailable;
    try std.testing.expectError(error.LeaderUnavailable, hosted.source().rowsQueryPlanSystemTimeAsOfSequence(
        alloc,
        "docs",
        versioned_schema,
        77,
        .{},
        .read_index,
    ));
    try std.testing.expectEqual(@as(usize, 8), executor_state.calls);

    const row_2_key = try relational_rows_api.physicalPrimaryKeyFromRowJsonAlloc(alloc, versioned_schema, "{\"id\":\"row:2\"}");
    defer alloc.free(row_2_key);
    const requested_range = [_]db_mod.types.RelationalRowsDocKeyRange{.{ .start = row_2_key, .end = "" }};
    executor_state.mode = .ranged_sequence;
    var ranged_result = (try hosted.source().rowsQueryPlanSystemTimeAsOfSequence(
        alloc,
        "docs",
        versioned_schema,
        88,
        .{ .ranges = requested_range[0..] },
        .read_index,
    )).?;
    defer ranged_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 10), executor_state.calls);
    try std.testing.expectEqual(@as(u32, 1), ranged_result.total);
    try std.testing.expectEqual(@as(usize, 1), ranged_result.rows.len);
    try std.testing.expect(std.mem.indexOf(u8, ranged_result.rows[0], "\"second\"") != null);
}

test "hosted join worker remote routes preserve unavailable owners" {
    const alloc = std.testing.allocator;
    const path = "/tmp/unused-antfly-hosted-join-worker-unavailable";

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
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{ .table_id = 7, .name = "docs", .placement_role = "data" }})[0..]),
                .ranges = &.{},
                .stores = &.{},
                .placement_intents = &.{},
                .split_transitions = &.{},
                .merge_transitions = &.{},
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

        fn groupLeaderNodeId(_: *anyopaque, group_id: u64) ?u64 {
            return if (group_id == 7001) 2 else null;
        }

        fn nodeStatus(_: *anyopaque, node_id: u64, group_id: u64) raft_mod.HostedReplicaStatus {
            return if (node_id == 2 and group_id == 7001) .active else .absent;
        }

        fn nodeBaseUri(_: *anyopaque, alloc_inner: std.mem.Allocator, node_id: u64) !?[]u8 {
            if (node_id != 2) return null;
            return try alloc_inner.dupe(u8, "http://remote.test");
        }
    };

    const ExecutorState = struct {
        const Mode = enum { unavailable, internal_error };

        calls: usize = 0,
        mode: Mode = .unavailable,

        fn iface(self: *@This()) http_common.RequestExecutor {
            return .{ .ptr = self, .vtable = &.{ .execute = execute } };
        }

        fn execute(ptr: *anyopaque, alloc_inner: std.mem.Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            try std.testing.expectEqual(http_common.Method.POST, req.method);
            return .{
                .status = switch (self.mode) {
                    .unavailable => 503,
                    .internal_error => 500,
                },
                .body = try alloc_inner.dupe(u8, switch (self.mode) {
                    .unavailable => "leader unavailable",
                    .internal_error => "remote join failure",
                }),
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
    const source = hosted.source();

    try std.testing.expectError(error.LeaderUnavailable, source.joinPartitionGroupLocal(alloc, 7001, "docs", "{}"));
    try std.testing.expectError(error.LeaderUnavailable, source.joinRowsGroupLocal(alloc, 7001, "docs", "{}"));
    try std.testing.expectError(error.LeaderUnavailable, source.joinUnmatchedGroupLocal(alloc, 7001, "docs", "{}"));
    try std.testing.expectError(error.LeaderUnavailable, source.joinFinalizeGroupLocal(alloc, 7001, "docs", "{}"));
    try std.testing.expectError(error.LeaderUnavailable, source.joinJobStateGroupLocal(alloc, 7001, "docs", "{}"));
    try std.testing.expectEqual(@as(usize, 5), executor_state.calls);

    executor_state.mode = .internal_error;
    try std.testing.expectError(error.UnexpectedHttpStatus, source.joinPartitionGroupLocal(alloc, 7001, "docs", "{}"));
    try std.testing.expectError(error.UnexpectedHttpStatus, source.joinRowsGroupLocal(alloc, 7001, "docs", "{}"));
    try std.testing.expectError(error.UnexpectedHttpStatus, source.joinUnmatchedGroupLocal(alloc, 7001, "docs", "{}"));
    try std.testing.expectError(error.UnexpectedHttpStatus, source.joinFinalizeGroupLocal(alloc, 7001, "docs", "{}"));
    try std.testing.expectError(error.UnexpectedHttpStatus, source.joinJobStateGroupLocal(alloc, 7001, "docs", "{}"));
    try std.testing.expectEqual(@as(usize, 10), executor_state.calls);
}

test "hosted document artifact remote routes preserve remote failures" {
    const alloc = std.testing.allocator;
    const path = "/tmp/unused-antfly-hosted-document-artifact-failures";

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
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{ .table_id = 7, .name = "docs", .placement_role = "data" }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null }})[0..]),
                .stores = &.{},
                .placement_intents = &.{},
                .split_transitions = &.{},
                .merge_transitions = &.{},
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

        fn groupLeaderNodeId(_: *anyopaque, group_id: u64) ?u64 {
            return if (group_id == 7001) 2 else null;
        }

        fn nodeStatus(_: *anyopaque, node_id: u64, group_id: u64) raft_mod.HostedReplicaStatus {
            return if (node_id == 2 and group_id == 7001) .active else .absent;
        }

        fn nodeBaseUri(_: *anyopaque, alloc_inner: std.mem.Allocator, node_id: u64) !?[]u8 {
            if (node_id != 2) return null;
            return try alloc_inner.dupe(u8, "http://remote.test");
        }
    };

    const ExecutorState = struct {
        const Mode = enum { internal_error, not_found, unavailable };

        calls: usize = 0,
        mode: Mode = .internal_error,

        fn iface(self: *@This()) http_common.RequestExecutor {
            return .{ .ptr = self, .vtable = &.{ .execute = execute } };
        }

        fn execute(ptr: *anyopaque, alloc_inner: std.mem.Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            try std.testing.expectEqual(http_common.Method.GET, req.method);
            try std.testing.expect(std.mem.indexOf(u8, req.uri, "/internal/v1/groups/7001/tables/docs/documents/doc%3Aa/artifacts") != null);
            return .{
                .status = switch (self.mode) {
                    .internal_error => 500,
                    .not_found => 404,
                    .unavailable => 503,
                },
                .body = try alloc_inner.dupe(u8, switch (self.mode) {
                    .internal_error => "remote artifact failure",
                    .not_found => "not found",
                    .unavailable => "leader unavailable",
                }),
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
    const source = hosted.source();

    try std.testing.expectError(error.UnexpectedHttpStatus, source.documentArtifactManifest(alloc, "docs", "doc:a", "units", .read_index));
    try std.testing.expectError(error.UnexpectedHttpStatus, source.documentArtifactManifests(alloc, "docs", "doc:a", .read_index));
    try std.testing.expectEqual(@as(usize, 2), executor_state.calls);

    executor_state.mode = .not_found;
    try std.testing.expect((try source.documentArtifactManifest(alloc, "docs", "doc:a", "units", .read_index)) == null);
    try std.testing.expect((try source.documentArtifactManifests(alloc, "docs", "doc:a", .read_index)) == null);
    try std.testing.expectEqual(@as(usize, 4), executor_state.calls);

    executor_state.mode = .unavailable;
    try std.testing.expectError(error.LeaderUnavailable, source.documentArtifactManifest(alloc, "docs", "doc:a", "units", .read_index));
    try std.testing.expectError(error.LeaderUnavailable, source.documentArtifactManifests(alloc, "docs", "doc:a", .read_index));
    try std.testing.expectEqual(@as(usize, 6), executor_state.calls);
}

test "hosted lookup remote routes preserve remote failures" {
    const alloc = std.testing.allocator;
    const path = "/tmp/unused-antfly-hosted-lookup-failures";

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
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{ .table_id = 7, .name = "docs", .placement_role = "data" }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null }})[0..]),
                .stores = &.{},
                .placement_intents = &.{},
                .split_transitions = &.{},
                .merge_transitions = &.{},
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

        fn groupLeaderNodeId(_: *anyopaque, group_id: u64) ?u64 {
            return if (group_id == 7001) 2 else null;
        }

        fn nodeStatus(_: *anyopaque, node_id: u64, group_id: u64) raft_mod.HostedReplicaStatus {
            return if (node_id == 2 and group_id == 7001) .active else .absent;
        }

        fn nodeBaseUri(_: *anyopaque, alloc_inner: std.mem.Allocator, node_id: u64) !?[]u8 {
            if (node_id != 2) return null;
            return try alloc_inner.dupe(u8, "http://remote.test");
        }
    };

    const ExecutorState = struct {
        const Mode = enum { internal_error, not_found, unavailable };

        calls: usize = 0,
        mode: Mode = .internal_error,

        fn iface(self: *@This()) http_common.RequestExecutor {
            return .{ .ptr = self, .vtable = &.{ .execute = execute } };
        }

        fn execute(ptr: *anyopaque, alloc_inner: std.mem.Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            try std.testing.expectEqual(http_common.Method.GET, req.method);
            try std.testing.expect(std.mem.indexOf(u8, req.uri, "/internal/v1/groups/7001/tables/docs/lookup/doc%3Aa") != null);
            return .{
                .status = switch (self.mode) {
                    .internal_error => 500,
                    .not_found => 404,
                    .unavailable => 503,
                },
                .body = try alloc_inner.dupe(u8, switch (self.mode) {
                    .internal_error => "remote lookup failure",
                    .not_found => "not found",
                    .unavailable => "leader unavailable",
                }),
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
    const source = hosted.source();

    try std.testing.expectError(error.UnexpectedHttpStatus, source.lookup(alloc, "docs", "doc:a", .{}, .read_index));
    try std.testing.expectEqual(@as(usize, 1), executor_state.calls);

    executor_state.mode = .not_found;
    try std.testing.expect((try source.lookup(alloc, "docs", "doc:a", .{}, .read_index)) == null);
    try std.testing.expectEqual(@as(usize, 2), executor_state.calls);

    executor_state.mode = .unavailable;
    try std.testing.expectError(error.LeaderUnavailable, source.lookup(alloc, "docs", "doc:a", .{}, .read_index));
    try std.testing.expectEqual(@as(usize, 3), executor_state.calls);
}

test "api.table_reads.docid document SQL bounded scan runtime diagnostics pin row and byte caps" {
    const alloc = std.testing.allocator;

    const MockSource = struct {
        scan_payload: []const u8,
        lookup_json: []const u8 = "{\"status\":\"other\"}",

        fn source(self: *@This()) document_sql_runtime.Source {
            return .{
                .ptr = self,
                .vtable = &.{
                    .lookup = lookup,
                    .scan = scan,
                    .query = query,
                },
                .native_table_name = "docs",
                .public_table_name = "docs",
            };
        }

        fn lookup(
            ptr: *anyopaque,
            lookup_alloc: std.mem.Allocator,
            table_name: []const u8,
            key: []const u8,
            opts: document_sql_runtime.LookupOptions,
            consistency: raft_mod.ReadConsistency,
        ) !?document_sql_runtime.LookupResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = table_name;
            _ = key;
            _ = opts;
            _ = consistency;
            return .{
                .json = try lookup_alloc.dupe(u8, self.lookup_json),
                .version = 1,
            };
        }

        fn scan(
            ptr: *anyopaque,
            scan_alloc: std.mem.Allocator,
            table_name: []const u8,
            from_key: []const u8,
            to_key: []const u8,
            opts: document_sql_runtime.ScanOptions,
            consistency: raft_mod.ReadConsistency,
        ) !?document_sql_runtime.ScanResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = table_name;
            _ = from_key;
            _ = to_key;
            _ = opts;
            _ = consistency;
            return .{ .ndjson = try scan_alloc.dupe(u8, self.scan_payload) };
        }

        fn query(
            ptr: *anyopaque,
            query_alloc: std.mem.Allocator,
            table_name: []const u8,
            req: document_sql_runtime.QueryRequest,
            consistency: raft_mod.ReadConsistency,
        ) !?document_sql_runtime.QueryResponse {
            _ = ptr;
            _ = query_alloc;
            _ = table_name;
            _ = req;
            _ = consistency;
            return error.DocumentSqlIndexUnavailable;
        }
    };

    var projection = [_]sql_adapter.DocumentProjection{.{ .kind = .id, .output = "_id" }};

    const byte_capped_plan = sql_adapter.DocumentReadPlan{
        .table_name = "docs",
        .projection = projection[0..],
        .producer = .{ .bounded_scan = .{
            .max_rows = 10,
            .max_bytes = 1,
        } },
    };
    var byte_source = MockSource{ .scan_payload = "{\"key\":\"doc:a\"}\n" };
    try std.testing.expectError(
        error.DocumentSqlBoundedScanByteCapExceeded,
        document_sql_runtime.executeReadPlanAlloc(alloc, byte_source.source(), byte_capped_plan, .stale),
    );

    const row_capped_plan = sql_adapter.DocumentReadPlan{
        .table_name = "docs",
        .projection = projection[0..],
        .producer = .{ .bounded_scan = .{
            .max_rows = 1,
            .residual_filter_json = "{\"term\":{\"path\":\"/status\",\"value\":\"missing\"}}",
        } },
        .limit = 1,
    };
    var row_source = MockSource{ .scan_payload = "{\"key\":\"doc:a\"}\n" };
    try std.testing.expectError(
        error.DocumentSqlBoundedScanRowCapExceeded,
        document_sql_runtime.executeReadPlanAlloc(alloc, row_source.source(), row_capped_plan, .stale),
    );
}

test "api.table_reads.docid lowered document sql read plans execute native lookup and bounded scan" {
    const alloc = std.testing.allocator;
    const path = try uniqueTestTmpPathAlloc(alloc, "antfly-api-document-sql-read");
    defer alloc.free(path);

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
    const NativeTitleScanRow = struct {
        key: []const u8,
        version: u64,
        title: []const u8,
    };

    var lookup_plan = try sql_adapter.lower_select.lowerReadPlanAlloc(
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

    var doc_projection_plan = try sql_adapter.lower_select.lowerReadPlanAlloc(
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
    var native_lookup = (try source.source().lookup(alloc, "docs", "doc:a", .{}, .read_index)).?;
    defer native_lookup.deinit(alloc);
    const expected_lookup_parity = try std.fmt.allocPrint(alloc, "{{\"_id\":\"doc:a\",\"_doc\":{s}}}", .{native_lookup.json});
    defer alloc.free(expected_lookup_parity);
    switch (doc_projection_result) {
        .document_query => |query| try std.testing.expectEqualStrings(expected_lookup_parity, query.rows[0]),
        else => return error.TestUnexpectedResult,
    }

    var scalar_filter_plan = try sql_adapter.lower_select.lowerReadPlanAlloc(
        alloc,
        "SELECT _id FROM docs WHERE key = 'document-key-field' LIMIT 10",
        schema,
        &.{},
    );
    defer scalar_filter_plan.deinit(alloc);
    var scalar_filter_result = (try executeLoweredSqlReadPlanAlloc(
        alloc,
        source.source(),
        catalog.iface(),
        "docs",
        schema,
        scalar_filter_plan,
        .read_index,
    )).?;
    defer scalar_filter_result.deinit(alloc);
    switch (scalar_filter_result) {
        .document_query => |query| {
            try std.testing.expectEqual(@as(u32, 1), query.total);
            try std.testing.expectEqual(@as(usize, 1), query.rows.len);
            try std.testing.expectEqualStrings("{\"_id\":\"doc:a\"}", query.rows[0]);
        },
        else => return error.TestUnexpectedResult,
    }
    var native_scalar_filter = (try source.source().query(alloc, "docs", .{
        .query = .{ .match_all = {} },
        .filter_query_json = "{\"term\":{\"field\":\"key\",\"term\":\"document-key-field\"}}",
        .limit = 10,
    }, .read_index)).?;
    defer native_scalar_filter.deinit(alloc);
    try expectSingleNativeQueryHitId(alloc, native_scalar_filter.json, "doc:a");

    var json_path_filter_plan = try sql_adapter.lower_select.lowerReadPlanAlloc(
        alloc,
        "SELECT _id FROM docs WHERE metadata#>>'{billing,plan}' = 'pro' LIMIT 10",
        schema,
        &.{},
    );
    defer json_path_filter_plan.deinit(alloc);
    var json_path_filter_result = (try executeLoweredSqlReadPlanAlloc(
        alloc,
        source.source(),
        catalog.iface(),
        "docs",
        schema,
        json_path_filter_plan,
        .read_index,
    )).?;
    defer json_path_filter_result.deinit(alloc);
    switch (json_path_filter_result) {
        .document_query => |query| {
            try std.testing.expectEqual(@as(u32, 1), query.total);
            try std.testing.expectEqual(@as(usize, 1), query.rows.len);
            try std.testing.expectEqualStrings("{\"_id\":\"doc:a\"}", query.rows[0]);
        },
        else => return error.TestUnexpectedResult,
    }
    var native_json_path_filter = (try source.source().query(alloc, "docs", .{
        .query = .{ .match_all = {} },
        .filter_query_json = "{\"term\":{\"metadata.billing.plan\":\"pro\"}}",
        .limit = 10,
    }, .read_index)).?;
    defer native_json_path_filter.deinit(alloc);
    try expectSingleNativeQueryHitId(alloc, native_json_path_filter.json, "doc:a");

    var scan_plan = try sql_adapter.lower_select.lowerReadPlanAlloc(
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
    var native_scan = (try source.source().scan(alloc, "docs", "", "", .{
        .include_documents = true,
        .fields = &.{"title"},
        .include_all_fields = false,
        .limit = 2,
    }, .read_index)).?;
    defer native_scan.deinit(alloc);
    const native_scan_rows = try parseNdjsonTestRowsAlloc(NativeTitleScanRow, alloc, native_scan.ndjson);
    defer alloc.free(native_scan_rows);
    try std.testing.expectEqual(@as(usize, 2), native_scan_rows.len);
    try std.testing.expectEqualStrings("doc:a", native_scan_rows[0].key);
    try std.testing.expectEqualStrings("alpha", native_scan_rows[0].title);
    try std.testing.expectEqualStrings("doc:b", native_scan_rows[1].key);
    try std.testing.expectEqualStrings("beta", native_scan_rows[1].title);

    var star_scan_plan = try sql_adapter.lower_select.lowerReadPlanAlloc(
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
    const virtual_projection_plan = sql_adapter.LoweredReadPlan{ .document_query = virtual_projection_document_plan };
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
    const virtual_star_plan = sql_adapter.LoweredReadPlan{ .document_query = virtual_star_document_plan };
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
    const ordered_scan_plan = sql_adapter.LoweredReadPlan{ .document_query = ordered_document_plan };
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
    const capped_ordered_scan_plan = sql_adapter.LoweredReadPlan{ .document_query = capped_ordered_document_plan };
    try std.testing.expectError(
        error.DocumentSqlBoundedScanRowCapExceeded,
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
    const byte_capped_ordered_scan_plan = sql_adapter.LoweredReadPlan{ .document_query = byte_capped_ordered_document_plan };
    try std.testing.expectError(
        error.DocumentSqlBoundedScanByteCapExceeded,
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

    var full_text_plan = try sql_adapter.lower_select.lowerReadPlanAlloc(
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

    var full_text_parity_plan = try sql_adapter.lower_select.lowerReadPlanAlloc(
        alloc,
        "SELECT _id, title FROM docs WHERE full_text_search('title:beta') LIMIT 10",
        schema,
        &.{},
    );
    defer full_text_parity_plan.deinit(alloc);
    var full_text_parity_result = (try executeLoweredSqlReadPlanAlloc(
        alloc,
        source.source(),
        catalog.iface(),
        "docs",
        schema,
        full_text_parity_plan,
        .read_index,
    )).?;
    defer full_text_parity_result.deinit(alloc);
    switch (full_text_parity_result) {
        .document_query => |query| {
            try std.testing.expectEqual(@as(u32, 1), query.total);
            try std.testing.expectEqual(@as(usize, 1), query.rows.len);
            try std.testing.expectEqualStrings("{\"_id\":\"doc:b\",\"title\":\"beta\"}", query.rows[0]);
        },
        else => return error.TestUnexpectedResult,
    }
    var native_full_text = (try source.source().query(alloc, "docs", .{
        .query = .{ .match = .{ .field = "title", .text = "beta" } },
        .limit = 10,
    }, .read_index)).?;
    defer native_full_text.deinit(alloc);
    try expectSingleNativeQueryHitId(alloc, native_full_text.json, "doc:b");

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
    const ordered_full_text_plan = sql_adapter.LoweredReadPlan{ .document_query = ordered_full_text_document_plan };
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
    const capped_ordered_full_text_plan = sql_adapter.LoweredReadPlan{ .document_query = capped_ordered_full_text_document_plan };
    try std.testing.expectError(
        error.DocumentSqlBoundedScanRowCapExceeded,
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

    var like_plan = try sql_adapter.lower_select.lowerReadPlanAlloc(
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
    var native_residual_scan = (try source.source().scan(alloc, "docs", "", "", .{
        .include_documents = true,
        .fields = &.{"title"},
        .include_all_fields = false,
        .limit = 10,
    }, .read_index)).?;
    defer native_residual_scan.deinit(alloc);
    const native_residual_rows = try parseNdjsonTestRowsAlloc(NativeTitleScanRow, alloc, native_residual_scan.ndjson);
    defer alloc.free(native_residual_rows);
    var native_residual_matches: usize = 0;
    for (native_residual_rows) |row| {
        if (!std.mem.startsWith(u8, row.title, "alp")) continue;
        switch (native_residual_matches) {
            0 => {
                try std.testing.expectEqualStrings("doc:a", row.key);
                try std.testing.expectEqualStrings("alpha", row.title);
            },
            1 => {
                try std.testing.expectEqualStrings("doc:c", row.key);
                try std.testing.expectEqualStrings("alpha", row.title);
            },
            else => return error.TestUnexpectedResult,
        }
        native_residual_matches += 1;
    }
    try std.testing.expectEqual(@as(usize, 2), native_residual_matches);

    var full_text_count_plan = try sql_adapter.lower_select.lowerReadPlanAlloc(
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
    var native_full_text_alpha = (try source.source().query(alloc, "docs", .{
        .query = .{ .match = .{ .field = "title", .text = "alpha" } },
        .limit = 10,
    }, .read_index)).?;
    defer native_full_text_alpha.deinit(alloc);
    try expectNativeQueryTotal(alloc, native_full_text_alpha.json, 2);

    var full_text_grouped_count_plan = try sql_adapter.lower_select.lowerReadPlanAlloc(
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

    var full_text_grouped_having_order_plan = try sql_adapter.lower_select.lowerReadPlanAlloc(
        alloc,
        "SELECT count(*) AS row_count FROM docs WHERE full_text_search('title:alpha') GROUP BY key HAVING row_count = 1 ORDER BY key ASC LIMIT 10",
        schema,
        &.{},
    );
    defer full_text_grouped_having_order_plan.deinit(alloc);
    var full_text_grouped_having_order_result = (try executeLoweredSqlReadPlanAlloc(
        alloc,
        source.source(),
        catalog.iface(),
        "docs",
        schema,
        full_text_grouped_having_order_plan,
        .read_index,
    )).?;
    defer full_text_grouped_having_order_result.deinit(alloc);
    switch (full_text_grouped_having_order_result) {
        .aggregate => |aggregate| {
            try std.testing.expectEqual(@as(u32, 2), aggregate.total_groups);
            try std.testing.expectEqual(@as(usize, 2), aggregate.rows.len);
            try std.testing.expectEqualStrings("{\"key\":\"document-key-field\",\"row_count\":1}", aggregate.rows[0]);
            try std.testing.expectEqualStrings("{\"key\":\"second-key\",\"row_count\":1}", aggregate.rows[1]);
        },
        else => return error.TestUnexpectedResult,
    }
    {
        var parsed_native_alpha = try parseJsonTestBody(metadata_openapi.QueryResponses, alloc, native_full_text_alpha.json);
        defer parsed_native_alpha.deinit();
        const hits = parsed_native_alpha.value.responses.?[0].hits.?.hits.?;
        try std.testing.expectEqual(@as(usize, 2), hits.len);
        try std.testing.expectEqualStrings("doc:a", hits[0]._id);
        try std.testing.expectEqualStrings("doc:c", hits[1]._id);
        var native_alpha_a = (try source.source().lookup(alloc, "docs", hits[0]._id, .{}, .read_index)).?;
        defer native_alpha_a.deinit(alloc);
        var native_alpha_c = (try source.source().lookup(alloc, "docs", hits[1]._id, .{}, .read_index)).?;
        defer native_alpha_c.deinit(alloc);
        var parsed_alpha_a = try parseJsonTestBody(std.json.Value, alloc, native_alpha_a.json);
        defer parsed_alpha_a.deinit();
        var parsed_alpha_c = try parseJsonTestBody(std.json.Value, alloc, native_alpha_c.json);
        defer parsed_alpha_c.deinit();
        try std.testing.expectEqualStrings("document-key-field", parsed_alpha_a.value.object.get("key").?.string);
        try std.testing.expectEqualStrings("second-key", parsed_alpha_c.value.object.get("key").?.string);
    }

    var scalar_count_plan = try sql_adapter.lower_select.lowerReadPlanAlloc(
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
    try expectNativeQueryTotal(alloc, native_scalar_filter.json, 1);
}

test "api.table_reads.docid lowered document sql unnest read plan matches native scan expansion" {
    const alloc = std.testing.allocator;
    const path = try uniqueTestTmpPathAlloc(alloc, "antfly-api-document-sql-unnest-parity");
    defer alloc.free(path);

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer db.close();
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"tags\":[\"urgent\",\"vip\"]}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"tags\":[\"normal\"]}" },
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"tags\":[\"urgent\"]}" },
        },
        .sync_level = .full_index,
    });

    var parsed_schema = try schema_api.parseValidatedTableSchema(alloc,
        \\{"version":1,"storage_mode":"document","default_type":"doc","document_schemas":{"doc":{"schema":{"type":"object","properties":{"title":{"type":"text"},"tags":{"type":"array","items":{"type":"keyword"}}},"additionalProperties":true}}}}
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

    var parsed_sql = try sql_adapter.ParsedSql.initAlloc(
        alloc,
        "SELECT d._id, tag FROM docs AS d, UNNEST(d.tags) AS tag WHERE tag = 'urgent' LIMIT 10",
    );
    defer parsed_sql.deinit(alloc);
    var document_plan = try sql_adapter.lowerDocumentReadPlanWithBoundedScanPolicyParsedSqlAlloc(
        alloc,
        &parsed_sql,
        schema,
        .{ .max_rows = 10 },
    );
    defer document_plan.deinit(alloc);
    const plan = sql_adapter.LoweredReadPlan{ .document_query = document_plan };

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
        .document_query => |query| {
            try std.testing.expectEqual(@as(u32, 2), query.total);
            try std.testing.expectEqual(@as(usize, 2), query.rows.len);
            try std.testing.expectEqualStrings("{\"_id\":\"doc:a\",\"tag\":\"urgent\"}", query.rows[0]);
            try std.testing.expectEqualStrings("{\"_id\":\"doc:c\",\"tag\":\"urgent\"}", query.rows[1]);
        },
        else => return error.TestUnexpectedResult,
    }

    var native_scan = (try source.source().scan(alloc, "docs", "", "", .{
        .include_documents = true,
        .fields = &.{"tags"},
        .include_all_fields = false,
        .limit = 10,
    }, .read_index)).?;
    defer native_scan.deinit(alloc);

    var urgent_matches: usize = 0;
    var lines = std.mem.splitScalar(u8, native_scan.ndjson, '\n');
    while (lines.next()) |line| {
        if (line.len == 0) continue;
        var parsed = try parseJsonTestBody(std.json.Value, alloc, line);
        defer parsed.deinit();
        const key = parsed.value.object.get("key").?.string;
        const tags = parsed.value.object.get("tags").?.array.items;
        for (tags) |tag| {
            if (tag != .string or !std.mem.eql(u8, tag.string, "urgent")) continue;
            switch (urgent_matches) {
                0 => try std.testing.expectEqualStrings("doc:a", key),
                1 => try std.testing.expectEqualStrings("doc:c", key),
                else => return error.TestUnexpectedResult,
            }
            urgent_matches += 1;
        }
    }
    try std.testing.expectEqual(@as(usize, 2), urgent_matches);
}

test "api.table_reads.docid document sql view mapping runtime results match native document reads" {
    const alloc = std.testing.allocator;
    const path = try uniqueTestTmpPathAlloc(alloc, "antfly-api-document-sql-view-mapping-parity");
    defer alloc.free(path);

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer db.close();
    try db.addIndex(.{ .name = "full_text_index_v0", .kind = .full_text, .config_json = "{}" });
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"status\":\"active\",\"published\":true,\"metadata\":{\"plan\":\"pro\",\"region\":\"west\"},\"metrics\":{\"score\":9},\"tags\":[\"urgent\",\"vip\"]}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"status\":\"active\",\"published\":false,\"metadata\":{\"plan\":\"free\",\"region\":\"east\"},\"metrics\":{\"score\":2},\"tags\":[\"normal\"]}" },
            .{ .key = "doc:c", .value = "{\"title\":\"alpha\",\"status\":\"archived\",\"published\":true,\"metadata\":{\"plan\":\"pro\"},\"metrics\":{\"score\":7},\"tags\":[\"urgent\"]}" },
            .{ .key = "doc:d", .value = "{\"title\":\"alpha\",\"status\":\"active\",\"published\":true,\"metadata\":{\"plan\":\"team\",\"region\":\"west\"},\"metrics\":{\"score\":5},\"tags\":[\"vip\"]}" },
            .{ .key = "doc:e", .value = "{\"title\":\"gamma\",\"status\":\"active\",\"published\":false,\"metrics\":{\"score\":4},\"tags\":[]}" },
            .{ .key = "doc:f", .value = "{\"title\":\"delta\",\"status\":\"active\",\"published\":false,\"metadata\":{\"plan\":\"trial\",\"region\":\"west\"},\"metrics\":{\"score\":6},\"tags\":[null,\"visible\"]}" },
        },
        .sync_level = .full_index,
    });

    var parsed_schema = try schema_api.parseValidatedTableSchema(alloc,
        \\{"version":1,"storage_mode":"document","default_type":"doc","document_schemas":{"doc":{"schema":{"type":"object","additionalProperties":true}}}}
    );
    defer parsed_schema.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer storage_schema.freeSchema(alloc, schema);

    const indexes_json =
        \\{
        \\  "full_text_index_v0":{"type":"full_text","lifecycle":"ready","scalar_paths":["title","metadata.plan","metadata.region","metrics.score","published"]},
        \\  "tags_array":{"type":"array_element","path":"tags","lifecycle":"ready"},
        \\  "view_mappings":{
        \\    "support_view":{
        \\      "source_table":"docs",
        \\      "required_indexes":[
        \\        {"name":"full_text_index_v0","lifecycle":"ready"},
        \\        {"name":"tags_array","lifecycle":"ready"}
        \\      ],
        \\      "fields":[
        \\        {"name":"title_text","path":"title","type":"text"},
        \\        {"name":"status","path":"status","type":"keyword"},
        \\        {"name":"plan","path":"metadata.plan","type":"keyword","nullable":true},
        \\        {"name":"region","path":"metadata.region","type":"keyword","nullable":true},
        \\        {"name":"score","path":"metrics.score","type":"numeric","nullable":true},
        \\        {"name":"published","path":"published","type":"boolean","nullable":true},
        \\        {"name":"tag_list","path":"tags","type":"array","item_type":"keyword"}
        \\      ]
        \\    }
        \\  }
        \\}
    ;

    var virtual_schema = try sql_adapter.documentSqlSchemaForRuntimeSchemaAndIndexesJsonAlloc(alloc, schema, indexes_json);
    defer sql_adapter.deinitDocumentSqlSchema(alloc, &virtual_schema);
    const indexed_scalar_paths = [_][]const u8{ "/title", "/metadata/plan", "/metadata/region", "/metrics/score", "/published" };
    const indexed_array_paths = [_][]const u8{ "tags", "/tags" };
    const capabilities = sql_adapter.DocumentSqlCapabilities{
        .full_text_filters = true,
        .indexed_scalar_filter_paths = indexed_scalar_paths[0..],
        .indexed_array_element_paths = indexed_array_paths[0..],
        .bounded_scan = .{ .max_rows = 100 },
    };

    var source = BoundTableReadSource.init("docs", 77, &db, raft_mod.read_gate.noopReadableLeaseRequester());
    var adapter = table_read_document_sql.RuntimeSourceAdapter{
        .source = source.source(),
        .target = .{
            .database_name = catalog_resources.default_database_name,
            .namespace_name = catalog_resources.default_namespace_name,
            .table_name = "docs",
        },
        .native_table_name = "docs",
        .public_table_name = "support_view",
    };

    const Harness = struct {
        fn expectRows(
            allocator: std.mem.Allocator,
            runtime_source: document_sql_runtime.Source,
            table_schema: storage_schema.TableSchema,
            document_sql_schema: sql_adapter.DocumentSqlSchema,
            document_capabilities: sql_adapter.DocumentSqlCapabilities,
            sql: []const u8,
            expected_total: u32,
            expected_rows: []const []const u8,
        ) !sql_adapter.DocumentReadPlan {
            var parsed_sql = try sql_adapter.ParsedSql.initAlloc(allocator, sql);
            defer parsed_sql.deinit(allocator);
            var document_plan = try sql_adapter.lowerDocumentReadPlanWithCapabilitiesAndVirtualSchemaParsedSqlAlloc(
                allocator,
                &parsed_sql,
                table_schema,
                document_sql_schema,
                document_capabilities,
            );
            errdefer document_plan.deinit(allocator);
            try std.testing.expect(document_plan.view_mapping != null);
            try std.testing.expectEqualStrings("support_view", document_plan.view_mapping.?.name);
            try std.testing.expectEqualStrings("docs", document_plan.view_mapping.?.source_table);

            var result = (try document_sql_runtime.executeReadPlanAlloc(
                allocator,
                runtime_source,
                document_plan,
                .read_index,
            )).?;
            defer result.deinit(allocator);
            try std.testing.expectEqual(expected_total, result.total);
            try std.testing.expectEqual(expected_rows.len, result.rows.len);
            for (expected_rows, 0..) |expected, i| {
                try std.testing.expectEqualStrings(expected, result.rows[i]);
            }
            return document_plan;
        }

        fn expectIndexedFilter(plan: sql_adapter.DocumentReadPlan) !void {
            switch (plan.producer) {
                .indexed_query => |query| if (query.filter_query_json == null) return error.TestUnexpectedResult,
                else => return error.TestUnexpectedResult,
            }
        }
    };

    var equality = try Harness.expectRows(
        alloc,
        adapter.runtimeSource(),
        schema,
        virtual_schema,
        capabilities,
        "SELECT _id, plan FROM support_view WHERE plan = 'pro' LIMIT 10",
        2,
        &.{
            "{\"_id\":\"doc:a\",\"plan\":\"pro\"}",
            "{\"_id\":\"doc:c\",\"plan\":\"pro\"}",
        },
    );
    defer equality.deinit(alloc);
    try Harness.expectIndexedFilter(equality);

    var in_list = try Harness.expectRows(
        alloc,
        adapter.runtimeSource(),
        schema,
        virtual_schema,
        capabilities,
        "SELECT _id, plan FROM support_view WHERE plan IN ('team', 'free') LIMIT 10",
        2,
        &.{
            "{\"_id\":\"doc:b\",\"plan\":\"free\"}",
            "{\"_id\":\"doc:d\",\"plan\":\"team\"}",
        },
    );
    defer in_list.deinit(alloc);
    try Harness.expectIndexedFilter(in_list);

    var is_null = try Harness.expectRows(
        alloc,
        adapter.runtimeSource(),
        schema,
        virtual_schema,
        capabilities,
        "SELECT _id, plan FROM support_view WHERE plan IS NULL LIMIT 10",
        1,
        &.{
            "{\"_id\":\"doc:e\",\"plan\":null}",
        },
    );
    defer is_null.deinit(alloc);
    try Harness.expectIndexedFilter(is_null);

    var is_not_null = try Harness.expectRows(
        alloc,
        adapter.runtimeSource(),
        schema,
        virtual_schema,
        capabilities,
        "SELECT _id, plan FROM support_view WHERE plan IS NOT NULL LIMIT 10",
        4,
        &.{
            "{\"_id\":\"doc:a\",\"plan\":\"pro\"}",
            "{\"_id\":\"doc:b\",\"plan\":\"free\"}",
            "{\"_id\":\"doc:c\",\"plan\":\"pro\"}",
            "{\"_id\":\"doc:d\",\"plan\":\"team\"}",
        },
    );
    defer is_not_null.deinit(alloc);
    try Harness.expectIndexedFilter(is_not_null);

    var range = try Harness.expectRows(
        alloc,
        adapter.runtimeSource(),
        schema,
        virtual_schema,
        capabilities,
        "SELECT _id, score FROM support_view WHERE score >= 5 LIMIT 10",
        3,
        &.{
            "{\"_id\":\"doc:a\",\"score\":9}",
            "{\"_id\":\"doc:c\",\"score\":7}",
            "{\"_id\":\"doc:d\",\"score\":5}",
        },
    );
    defer range.deinit(alloc);
    try Harness.expectIndexedFilter(range);

    var between = try Harness.expectRows(
        alloc,
        adapter.runtimeSource(),
        schema,
        virtual_schema,
        capabilities,
        "SELECT _id, score FROM support_view WHERE score BETWEEN 4 AND 7 LIMIT 10",
        3,
        &.{
            "{\"_id\":\"doc:c\",\"score\":7}",
            "{\"_id\":\"doc:d\",\"score\":5}",
            "{\"_id\":\"doc:e\",\"score\":4}",
        },
    );
    defer between.deinit(alloc);
    try Harness.expectIndexedFilter(between);

    var text_field = try Harness.expectRows(
        alloc,
        adapter.runtimeSource(),
        schema,
        virtual_schema,
        capabilities,
        "SELECT _id, title_text FROM support_view WHERE title_text = 'alpha' LIMIT 10",
        3,
        &.{
            "{\"_id\":\"doc:a\",\"title_text\":\"alpha\"}",
            "{\"_id\":\"doc:c\",\"title_text\":\"alpha\"}",
            "{\"_id\":\"doc:d\",\"title_text\":\"alpha\"}",
        },
    );
    defer text_field.deinit(alloc);
    try Harness.expectIndexedFilter(text_field);

    var boolean_field = try Harness.expectRows(
        alloc,
        adapter.runtimeSource(),
        schema,
        virtual_schema,
        capabilities,
        "SELECT _id, published FROM support_view WHERE published IS TRUE LIMIT 10",
        3,
        &.{
            "{\"_id\":\"doc:a\",\"published\":true}",
            "{\"_id\":\"doc:c\",\"published\":true}",
            "{\"_id\":\"doc:d\",\"published\":true}",
        },
    );
    defer boolean_field.deinit(alloc);
    try Harness.expectIndexedFilter(boolean_field);

    var missing_nested = try Harness.expectRows(
        alloc,
        adapter.runtimeSource(),
        schema,
        virtual_schema,
        capabilities,
        "SELECT _id, region FROM support_view WHERE region IS NULL LIMIT 10",
        2,
        &.{
            "{\"_id\":\"doc:c\",\"region\":null}",
            "{\"_id\":\"doc:e\",\"region\":null}",
        },
    );
    defer missing_nested.deinit(alloc);
    try Harness.expectIndexedFilter(missing_nested);

    var multi_field = try Harness.expectRows(
        alloc,
        adapter.runtimeSource(),
        schema,
        virtual_schema,
        capabilities,
        "SELECT _id, title_text, plan, score, published, region FROM support_view WHERE region = 'west' LIMIT 10",
        3,
        &.{
            "{\"_id\":\"doc:a\",\"title_text\":\"alpha\",\"plan\":\"pro\",\"score\":9,\"published\":true,\"region\":\"west\"}",
            "{\"_id\":\"doc:d\",\"title_text\":\"alpha\",\"plan\":\"team\",\"score\":5,\"published\":true,\"region\":\"west\"}",
            "{\"_id\":\"doc:f\",\"title_text\":\"delta\",\"plan\":\"trial\",\"score\":6,\"published\":false,\"region\":\"west\"}",
        },
    );
    defer multi_field.deinit(alloc);
    try Harness.expectIndexedFilter(multi_field);

    var mapped_scalar_scan = (try source.source().scan(alloc, "docs", "", "", .{
        .include_documents = true,
        .fields = &.{ "title", "metadata", "published" },
        .include_all_fields = false,
        .limit = 100,
    }, .read_index)).?;
    defer mapped_scalar_scan.deinit(alloc);
    var native_title_alpha: usize = 0;
    var native_published_true: usize = 0;
    var native_region_null: usize = 0;
    var native_region_west: usize = 0;
    var mapped_scalar_lines = std.mem.splitScalar(u8, mapped_scalar_scan.ndjson, '\n');
    while (mapped_scalar_lines.next()) |line| {
        if (line.len == 0) continue;
        var parsed = try parseJsonTestBody(std.json.Value, alloc, line);
        defer parsed.deinit();
        if (parsed.value.object.get("title")) |title| {
            if (title == .string and std.mem.eql(u8, title.string, "alpha")) native_title_alpha += 1;
        }
        if (parsed.value.object.get("published")) |published| {
            if (published == .bool and published.bool) native_published_true += 1;
        }
        const region = if (parsed.value.object.get("metadata")) |metadata|
            if (metadata == .object) metadata.object.get("region") else null
        else
            null;
        if (region == null or region.? == .null) {
            native_region_null += 1;
        } else if (region.? == .string and std.mem.eql(u8, region.?.string, "west")) {
            native_region_west += 1;
        }
    }
    try std.testing.expectEqual(@as(usize, 3), native_title_alpha);
    try std.testing.expectEqual(@as(usize, 3), native_published_true);
    try std.testing.expectEqual(@as(usize, 2), native_region_null);
    try std.testing.expectEqual(@as(usize, 3), native_region_west);

    var ordered = try Harness.expectRows(
        alloc,
        adapter.runtimeSource(),
        schema,
        virtual_schema,
        capabilities,
        "SELECT _id, plan, score FROM support_view WHERE plan = 'pro' ORDER BY score DESC LIMIT 1",
        1,
        &.{
            "{\"_id\":\"doc:a\",\"plan\":\"pro\",\"score\":9}",
        },
    );
    defer ordered.deinit(alloc);
    try std.testing.expect(ordered.order_by != null);
    try Harness.expectIndexedFilter(ordered);

    var residual = try Harness.expectRows(
        alloc,
        adapter.runtimeSource(),
        schema,
        virtual_schema,
        capabilities,
        "SELECT _id, title_text, status FROM support_view WHERE full_text_search('title:alpha') AND status = 'active' LIMIT 10",
        2,
        &.{
            "{\"_id\":\"doc:a\",\"title_text\":\"alpha\",\"status\":\"active\"}",
            "{\"_id\":\"doc:d\",\"title_text\":\"alpha\",\"status\":\"active\"}",
        },
    );
    defer residual.deinit(alloc);
    try std.testing.expect(residual.producer.indexed_query.residual_filter_json != null);
    var native_full_text = (try source.source().query(alloc, "docs", .{
        .query = .{ .match = .{ .field = "title", .text = "alpha" } },
        .index_name = residual.producer.indexed_query.index_name,
        .limit = 100,
    }, .read_index)).?;
    defer native_full_text.deinit(alloc);
    try expectNativeQueryTotal(alloc, native_full_text.json, 3);

    var unnest = try Harness.expectRows(
        alloc,
        adapter.runtimeSource(),
        schema,
        virtual_schema,
        capabilities,
        "SELECT d._id, tag FROM support_view AS d, UNNEST(d.tag_list) AS tag WHERE tag = 'urgent' LIMIT 10",
        2,
        &.{
            "{\"_id\":\"doc:a\",\"tag\":\"urgent\"}",
            "{\"_id\":\"doc:c\",\"tag\":\"urgent\"}",
        },
    );
    defer unnest.deinit(alloc);
    try std.testing.expect(unnest.unnest != null);
    try Harness.expectIndexedFilter(unnest);

    var lateral_unnest = try Harness.expectRows(
        alloc,
        adapter.runtimeSource(),
        schema,
        virtual_schema,
        capabilities,
        "SELECT d._id, tag FROM support_view AS d JOIN LATERAL UNNEST(d.tag_list) AS tag ON true WHERE tag = 'urgent' LIMIT 10",
        2,
        &.{
            "{\"_id\":\"doc:a\",\"tag\":\"urgent\"}",
            "{\"_id\":\"doc:c\",\"tag\":\"urgent\"}",
        },
    );
    defer lateral_unnest.deinit(alloc);
    try std.testing.expect(lateral_unnest.unnest != null);
    try Harness.expectIndexedFilter(lateral_unnest);

    var cross_lateral_unnest = try Harness.expectRows(
        alloc,
        adapter.runtimeSource(),
        schema,
        virtual_schema,
        capabilities,
        "SELECT d._id, tag FROM support_view AS d CROSS JOIN LATERAL UNNEST(d.tag_list) AS tag WHERE tag = 'urgent' LIMIT 10",
        2,
        &.{
            "{\"_id\":\"doc:a\",\"tag\":\"urgent\"}",
            "{\"_id\":\"doc:c\",\"tag\":\"urgent\"}",
        },
    );
    defer cross_lateral_unnest.deinit(alloc);
    try std.testing.expect(cross_lateral_unnest.unnest != null);
    try Harness.expectIndexedFilter(cross_lateral_unnest);

    var lateral_unnest_residual = try Harness.expectRows(
        alloc,
        adapter.runtimeSource(),
        schema,
        virtual_schema,
        capabilities,
        "SELECT d._id, tag FROM support_view AS d JOIN LATERAL UNNEST(d.tag_list) AS tag ON true WHERE full_text_search('title:alpha') AND tag = 'urgent' AND status = 'active' LIMIT 10",
        1,
        &.{
            "{\"_id\":\"doc:a\",\"tag\":\"urgent\"}",
        },
    );
    defer lateral_unnest_residual.deinit(alloc);
    try std.testing.expect(lateral_unnest_residual.unnest != null);
    try std.testing.expect(lateral_unnest_residual.producer.indexed_query.residual_filter_json != null);

    var native_scan = (try source.source().scan(alloc, "docs", "", "", .{
        .include_documents = true,
        .fields = &.{"tags"},
        .include_all_fields = false,
        .limit = 100,
    }, .read_index)).?;
    defer native_scan.deinit(alloc);
    var urgent_matches: usize = 0;
    var lines = std.mem.splitScalar(u8, native_scan.ndjson, '\n');
    while (lines.next()) |line| {
        if (line.len == 0) continue;
        var parsed = try parseJsonTestBody(std.json.Value, alloc, line);
        defer parsed.deinit();
        const key = parsed.value.object.get("key").?.string;
        const tags = parsed.value.object.get("tags").?.array.items;
        for (tags) |tag| {
            if (tag != .string or !std.mem.eql(u8, tag.string, "urgent")) continue;
            switch (urgent_matches) {
                0 => try std.testing.expectEqualStrings("doc:a", key),
                1 => try std.testing.expectEqualStrings("doc:c", key),
                else => return error.TestUnexpectedResult,
            }
            urgent_matches += 1;
        }
    }
    try std.testing.expectEqual(@as(usize, 2), urgent_matches);

    var unnest_range = try Harness.expectRows(
        alloc,
        adapter.runtimeSource(),
        schema,
        virtual_schema,
        capabilities,
        "SELECT d._id, tag FROM support_view AS d, UNNEST(d.tag_list) AS tag WHERE tag > 'u' LIMIT 10",
        3,
        &.{
            "{\"_id\":\"doc:a\",\"tag\":\"urgent\"}",
            "{\"_id\":\"doc:a\",\"tag\":\"vip\"}",
            "{\"_id\":\"doc:f\",\"tag\":\"visible\"}",
        },
    );
    defer unnest_range.deinit(alloc);
    try std.testing.expect(unnest_range.unnest != null);
    try Harness.expectIndexedFilter(unnest_range);

    var unnest_pattern = try Harness.expectRows(
        alloc,
        adapter.runtimeSource(),
        schema,
        virtual_schema,
        capabilities,
        "SELECT d._id, tag FROM support_view AS d, UNNEST(d.tag_list) AS tag WHERE tag LIKE 'v%' LIMIT 10",
        2,
        &.{
            "{\"_id\":\"doc:a\",\"tag\":\"vip\"}",
            "{\"_id\":\"doc:f\",\"tag\":\"visible\"}",
        },
    );
    defer unnest_pattern.deinit(alloc);
    try std.testing.expect(unnest_pattern.unnest != null);
    try Harness.expectIndexedFilter(unnest_pattern);

    var unnest_ilike = try Harness.expectRows(
        alloc,
        adapter.runtimeSource(),
        schema,
        virtual_schema,
        capabilities,
        "SELECT d._id, tag FROM support_view AS d, UNNEST(d.tag_list) AS tag WHERE tag ILIKE 'V%' LIMIT 10",
        2,
        &.{
            "{\"_id\":\"doc:a\",\"tag\":\"vip\"}",
            "{\"_id\":\"doc:f\",\"tag\":\"visible\"}",
        },
    );
    defer unnest_ilike.deinit(alloc);
    try std.testing.expect(unnest_ilike.unnest != null);
    try Harness.expectIndexedFilter(unnest_ilike);

    var unnest_null = try Harness.expectRows(
        alloc,
        adapter.runtimeSource(),
        schema,
        virtual_schema,
        capabilities,
        "SELECT d._id, tag FROM support_view AS d, UNNEST(d.tag_list) AS tag WHERE tag IS NULL LIMIT 10",
        1,
        &.{
            "{\"_id\":\"doc:f\",\"tag\":null}",
        },
    );
    defer unnest_null.deinit(alloc);
    try std.testing.expect(unnest_null.unnest != null);
    try Harness.expectIndexedFilter(unnest_null);

    var unnest_not_null = try Harness.expectRows(
        alloc,
        adapter.runtimeSource(),
        schema,
        virtual_schema,
        capabilities,
        "SELECT d._id, tag FROM support_view AS d, UNNEST(d.tag_list) AS tag WHERE tag IS NOT NULL LIMIT 10",
        6,
        &.{
            "{\"_id\":\"doc:a\",\"tag\":\"urgent\"}",
            "{\"_id\":\"doc:a\",\"tag\":\"vip\"}",
            "{\"_id\":\"doc:b\",\"tag\":\"stale\"}",
            "{\"_id\":\"doc:c\",\"tag\":\"urgent\"}",
            "{\"_id\":\"doc:c\",\"tag\":\"review\"}",
            "{\"_id\":\"doc:f\",\"tag\":\"visible\"}",
        },
    );
    defer unnest_not_null.deinit(alloc);
    try std.testing.expect(unnest_not_null.unnest != null);
    try Harness.expectIndexedFilter(unnest_not_null);

    var unnest_not_equal = try Harness.expectRows(
        alloc,
        adapter.runtimeSource(),
        schema,
        virtual_schema,
        capabilities,
        "SELECT d._id, tag FROM support_view AS d, UNNEST(d.tag_list) AS tag WHERE tag <> 'urgent' LIMIT 10",
        4,
        &.{
            "{\"_id\":\"doc:a\",\"tag\":\"vip\"}",
            "{\"_id\":\"doc:b\",\"tag\":\"stale\"}",
            "{\"_id\":\"doc:c\",\"tag\":\"review\"}",
            "{\"_id\":\"doc:f\",\"tag\":\"visible\"}",
        },
    );
    defer unnest_not_equal.deinit(alloc);
    try std.testing.expect(unnest_not_equal.unnest != null);
    try Harness.expectIndexedFilter(unnest_not_equal);

    var unnest_compound = try Harness.expectRows(
        alloc,
        adapter.runtimeSource(),
        schema,
        virtual_schema,
        capabilities,
        "SELECT d._id, tag FROM support_view AS d, UNNEST(d.tag_list) AS tag WHERE tag = 'urgent' AND tag <> 'stale' LIMIT 10",
        2,
        &.{
            "{\"_id\":\"doc:a\",\"tag\":\"urgent\"}",
            "{\"_id\":\"doc:c\",\"tag\":\"urgent\"}",
        },
    );
    defer unnest_compound.deinit(alloc);
    try std.testing.expect(unnest_compound.unnest != null);
    try Harness.expectIndexedFilter(unnest_compound);

    var mapped_range_matches: usize = 0;
    var mapped_pattern_matches: usize = 0;
    var mapped_ilike_matches: usize = 0;
    var mapped_null_matches: usize = 0;
    var mapped_not_null_matches: usize = 0;
    var mapped_not_equal_matches: usize = 0;
    var mapped_compound_matches: usize = 0;
    var mapped_lines = std.mem.splitScalar(u8, native_scan.ndjson, '\n');
    while (mapped_lines.next()) |line| {
        if (line.len == 0) continue;
        var parsed = try parseJsonTestBody(std.json.Value, alloc, line);
        defer parsed.deinit();
        const tags = parsed.value.object.get("tags").?.array.items;
        for (tags) |tag| {
            if (tag == .null) {
                mapped_null_matches += 1;
                continue;
            }
            if (tag != .string) continue;
            mapped_not_null_matches += 1;
            if (std.mem.order(u8, tag.string, "u") == .gt) mapped_range_matches += 1;
            if (std.mem.startsWith(u8, tag.string, "v")) mapped_pattern_matches += 1;
            if (tag.string.len > 0 and (tag.string[0] == 'v' or tag.string[0] == 'V')) mapped_ilike_matches += 1;
            if (!std.mem.eql(u8, tag.string, "urgent")) mapped_not_equal_matches += 1;
            if (std.mem.eql(u8, tag.string, "urgent") and !std.mem.eql(u8, tag.string, "stale")) mapped_compound_matches += 1;
        }
    }
    try std.testing.expectEqual(@as(usize, 3), mapped_range_matches);
    try std.testing.expectEqual(@as(usize, 2), mapped_pattern_matches);
    try std.testing.expectEqual(@as(usize, 2), mapped_ilike_matches);
    try std.testing.expectEqual(@as(usize, 1), mapped_null_matches);
    try std.testing.expectEqual(@as(usize, 6), mapped_not_null_matches);
    try std.testing.expectEqual(@as(usize, 4), mapped_not_equal_matches);
    try std.testing.expectEqual(@as(usize, 2), mapped_compound_matches);
}

test "api.table_reads.docid document sql catalog read producers treat catalog misses as terminal" {
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

    var lookup_projection = try alloc.alloc(sql_adapter.DocumentProjection, 1);
    lookup_projection[0] = .{ .kind = .id, .output = try alloc.dupe(u8, "_id") };
    var lookup_ids = try alloc.alloc([]const u8, 1);
    lookup_ids[0] = try alloc.dupe(u8, "doc:missing");
    var lookup_plan = sql_adapter.LoweredReadPlan{ .document_query = .{
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

    var query_projection = try alloc.alloc(sql_adapter.DocumentProjection, 1);
    query_projection[0] = .{ .kind = .id, .output = try alloc.dupe(u8, "_id") };
    var query_plan = sql_adapter.LoweredReadPlan{ .document_query = .{
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

    var scan_projection = try alloc.alloc(sql_adapter.DocumentProjection, 1);
    scan_projection[0] = .{ .kind = .id, .output = try alloc.dupe(u8, "_id") };
    var scan_plan = sql_adapter.LoweredReadPlan{ .document_query = .{
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

test "api.table_reads.docid lowered document sql aggregate executes native grouped avg materialization" {
    const alloc = std.testing.allocator;
    const path = try uniqueTestTmpPathAlloc(alloc, "antfly-api-document-sql-grouped-avg");
    defer alloc.free(path);

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
    const plan = sql_adapter.LoweredReadPlan{ .document_aggregate = document_plan };

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

test "api.table_reads.docid lowered document sql aggregate uses catalog target for non-default namespace materialization" {
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
    const plan = sql_adapter.LoweredReadPlan{ .document_aggregate = document_plan };

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
    const path = try uniqueTestTmpPathAlloc(alloc, "antfly-api-table-scan");
    defer alloc.free(path);

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
        version: u64,
        title: []const u8,
    };
    const rows = try parseNdjsonTestRowsAlloc(ScanRow, alloc, scan.ndjson);
    defer alloc.free(rows);
    try std.testing.expectEqual(@as(usize, 2), rows.len);
    try std.testing.expectEqualStrings("doc:a", rows[0].key);
    try std.testing.expect(rows[0].version > 0);
    try std.testing.expectEqualStrings("alpha", rows[0].title);
}

test "bound table read source formats query responses" {
    const alloc = std.testing.allocator;
    const path = try uniqueTestTmpPathAlloc(alloc, "antfly-api-table-query");
    defer alloc.free(path);

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
    const path = try uniqueTestTmpPathAlloc(alloc, "antfly-api-table-preflight");
    defer alloc.free(path);

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
    const path = try uniqueTestTmpPathAlloc(alloc, "antfly-api-table-rerank");
    defer alloc.free(path);

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
    const path = try uniqueTestTmpPathAlloc(alloc, "antfly-api-provisioned-reads");
    defer alloc.free(path);

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const left_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(left_path);
    const right_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7002);
    defer alloc.free(right_path);

    var left_db = try openTestGroupDb(alloc, left_path, 7001);
    defer left_db.close();
    var right_db = try openTestGroupDb(alloc, right_path, 7002);
    defer right_db.close();

    try left_db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" },
            .{ .key = "doc:z", .value = "{\"title\":\"left-stray\"}" },
        },
    });
    try right_db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"right-stray\"}" },
            .{ .key = "doc:z", .value = "{\"title\":\"zeta\"}" },
        },
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
        version: u64,
        title: []const u8,
    };
    const rows = try parseNdjsonTestRowsAlloc(ScanRow, alloc, scan.ndjson);
    defer alloc.free(rows);
    try std.testing.expectEqual(@as(usize, 2), rows.len);
    try std.testing.expect(rows[0].version > 0);
    try std.testing.expect(rows[1].version > 0);
    try std.testing.expectEqualStrings("alpha", rows[0].title);
    try std.testing.expectEqualStrings("zeta", rows[1].title);
}

test "api.table_reads.docid provisioned standby read gate permits stale reads and routes non-stale reads to primary" {
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
    const path = try uniqueTestTmpPathAlloc(alloc, "antfly-api-provisioned-query");
    defer alloc.free(path);

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const left_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(left_path);
    const right_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7002);
    defer alloc.free(right_path);

    var left_db = try openTestGroupDb(alloc, left_path, 7001);
    defer left_db.close();
    var right_db = try openTestGroupDb(alloc, right_path, 7002);
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
    try std.testing.expectEqual(@as(usize, 2), hits.len);
    try std.testing.expect(
        (std.mem.eql(u8, hits[0]._id, "doc:a") and std.mem.eql(u8, hits[1]._id, "doc:z")) or
            (std.mem.eql(u8, hits[0]._id, "doc:z") and std.mem.eql(u8, hits[1]._id, "doc:a")),
    );
}

test "provisioned table read source serves dense queries for explicit external embeddings" {
    const alloc = std.testing.allocator;
    const path = try uniqueTestTmpPathAlloc(alloc, "antfly-api-provisioned-query-dense");
    defer alloc.free(path);

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const group_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(group_path);
    var db = try openTestGroupDb(alloc, group_path, 7001);
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
    const path = try uniqueTestTmpPathAlloc(alloc, "antfly-api-provisioned-query-stamped-identity");
    defer alloc.free(path);

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const group_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(group_path);
    {
        var db = try openTestGroupDb(alloc, group_path, 7001);
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
    const path = try uniqueTestTmpPathAlloc(alloc, "antfly-api-provisioned-query-dense-public");
    defer alloc.free(path);

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const group_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(group_path);
    var db = try openTestGroupDb(alloc, group_path, 7001);
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
    const path = try uniqueTestTmpPathAlloc(alloc, "antfly-api-provisioned-query-dense-public-profiled");
    defer alloc.free(path);

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const group_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(group_path);
    var db = try openTestGroupDb(alloc, group_path, 7001);
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
    const path = try uniqueTestTmpPathAlloc(alloc, "antfly-api-provisioned-query-dense-public-implicit-index");
    defer alloc.free(path);

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const group_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(group_path);
    var db = try openTestGroupDb(alloc, group_path, 7001);
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
    const path = try uniqueTestTmpPathAlloc(alloc, "antfly-api-provisioned-query-dense-benchmark-shaped");
    defer alloc.free(path);

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const group_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(group_path);
    var db = try openTestGroupDb(alloc, group_path, 7001);
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
    const path = try uniqueTestTmpPathAlloc(alloc, "antfly-api-provisioned-preflight-multigroup");
    defer alloc.free(path);

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const left_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(left_path);
    const right_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7002);
    defer alloc.free(right_path);

    var left_db = try openTestGroupDb(alloc, left_path, 7001);
    defer left_db.close();
    var right_db = try openTestGroupDb(alloc, right_path, 7002);
    defer right_db.close();

    try left_db.addIndex(.{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}",
    });
    try right_db.addIndex(.{
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
    var dense_summary = (try source.source().preflightQuery(alloc, "docs", .{
        .index_name = "dense_idx",
        .dense = .{ .vector = &.{ 1.0, 2.0, 3.0 }, .k = 5 },
    }, .read_index, 0)).?;
    defer dense_summary.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 2), dense_summary.shard_count);
    try std.testing.expectEqual(@as(u32, 0), dense_summary.remote_shard_count);
    try std.testing.expectEqual(@as(u32, 2), dense_summary.dense_query_count);
    try std.testing.expectEqual(@as(u32, 2), dense_summary.vector_worker_candidate_count);
    try std.testing.expectEqual(@as(u64, 10), dense_summary.dense_effective_k_total);
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
    const path = try uniqueTestTmpPathAlloc(alloc, "antfly-api-provisioned-runtime-status-managed");
    defer alloc.free(path);

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const group_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(group_path);
    var db = try openTestGroupDb(alloc, group_path, 7001);
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
    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();
    var indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1);
    indexes[0] = .{
        .name = try alloc.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 0,
    };
    var status = runtime_status.LocalTableRuntimeStatus{
        .group_id = 7001,
        .metadata = .{ .source = .synthetic_config, .freshness = .fresh },
        .stats = .{
            .index_count = 1,
            .indexes = indexes,
        },
    };
    defer status.deinit(alloc);
    try snapshot_cache.upsertGroupStatus("docs", status);
    source.runtime_status_cache = &snapshot_cache;
    var statuses = (try source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(usize, 1), statuses.items[0].stats.indexes.len);
    try std.testing.expectEqualStrings("semantic_idx", statuses.items[0].stats.indexes[0].name);
    try std.testing.expectEqual(false, statuses.items[0].stats.indexes[0].backfill_active);
    try std.testing.expectEqual(@as(u64, 0), statuses.items[0].stats.indexes[0].doc_count);
}

test "provisioned query db installs asset producer from indexes_json without read-side replay" {
    const alloc = std.testing.allocator;
    const path = try uniqueTestTmpPathAlloc(alloc, "antfly-api-provisioned-asset-enrichment");
    defer alloc.free(path);
    const indexes_json =
        \\{"search_idx":{"type":"full_text","field":"body","enrichments":[{"name":"generated_title_v1","kind":"asset","field":"body","content_type":"text/plain","producer_json":"{\"type\":\"generator\",\"config\":{\"provider\":\"antfly\",\"model\":\"local-model\"}}"}]}}
    ;

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const group_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(group_path);
    {
        var db = try openTestGroupDb(alloc, group_path, 7001);
        defer db.close();
        _ = try metadata_table_provisioner.reconcileDbIndexesWithOptions(alloc, &db, indexes_json, .{
            .drain_resolver_backfill = false,
        });
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
                    .indexes_json = indexes_json,
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

    try std.testing.expectEqual(@as(usize, 0), fake.calls);
}

test "provisioned table read source runtime status stays cache-only without shared snapshot" {
    const alloc = std.testing.allocator;
    const path = try uniqueTestTmpPathAlloc(alloc, "antfly-api-provisioned-read-runtime-cache");
    defer alloc.free(path);

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const group_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(group_path);
    var db = try openTestGroupDb(alloc, group_path, 7001);
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
    const path = try uniqueTestTmpPathAlloc(alloc, "antfly-api-provisioned-read-runtime-prefers-snapshot");
    defer alloc.free(path);

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const group_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(group_path);
    var db = try openTestGroupDb(alloc, group_path, 7001);
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
    const path = try uniqueTestTmpPathAlloc(alloc, "antfly-api-provisioned-read-fallback");
    defer alloc.free(path);

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const group_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(group_path);
    var db = try openTestGroupDb(alloc, group_path, 7001);
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

test "api.table_reads.docid explicit text stats requests preserve identity generation" {
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

test "api.table_reads.docid explicit text stats requests carry resolved doc filters and apply exact projection" {
    const alloc = std.testing.allocator;
    const path = try uniqueTestTmpPathAlloc(alloc, "antfly-api-text-stats-resolved-doc-filter");
    defer alloc.free(path);

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

test "api.table_reads.docid explicit text stats requests reject stale identity generation" {
    const alloc = std.testing.allocator;
    const path = try uniqueTestTmpPathAlloc(alloc, "antfly-api-text-stats-stale-identity-generation");
    defer alloc.free(path);

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

test "api.table_reads.docid algebraic partial request fails closed when lifecycle is stale" {
    const alloc = std.testing.allocator;
    const path = try uniqueTestTmpPathAlloc(alloc, "antfly-api-algebraic-partials-stale-lifecycle");
    defer alloc.free(path);

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

test "api.table_reads.docid algebraic partial request accepts current identity generation and rejects stale" {
    const alloc = std.testing.allocator;
    const path = try uniqueTestTmpPathAlloc(alloc, "antfly-api-algebraic-partials-stale-identity-generation");
    defer alloc.free(path);

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

test "hosted textStatsGroupLocal serves only the local group" {
    const test_alloc = std.testing.allocator;
    const path = try uniqueTestTmpPathAlloc(test_alloc, "antfly-api-hosted-local-text-stats");
    defer test_alloc.free(path);

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const group_path = try metadata_mod.groupDbPathFromReplicaRoot(test_alloc, path, 7);
    defer test_alloc.free(group_path);
    var db = try openTestGroupDb(test_alloc, group_path, 7);
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
                    .indexes_json = tables_api.default_indexes_json,
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
    const path = try uniqueTestTmpPathAlloc(test_alloc, "antfly-api-hosted-local-preflight");
    defer test_alloc.free(path);

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const group_path = try metadata_mod.groupDbPathFromReplicaRoot(test_alloc, path, 7);
    defer test_alloc.free(group_path);
    var db = try openTestGroupDb(test_alloc, group_path, 7);
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
                    .indexes_json = "{\"dv_v1\":{\"type\":\"embeddings\",\"dimension\":3,\"external\":true}}",
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
    const path = try uniqueTestTmpPathAlloc(test_alloc, "antfly-api-hosted-local-preflight-multigroup");
    defer test_alloc.free(path);

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const left_path = try metadata_mod.groupDbPathFromReplicaRoot(test_alloc, path, 7);
    defer test_alloc.free(left_path);
    const right_path = try metadata_mod.groupDbPathFromReplicaRoot(test_alloc, path, 8);
    defer test_alloc.free(right_path);
    var left_db = try openTestGroupDb(test_alloc, left_path, 7);
    defer left_db.close();
    var right_db = try openTestGroupDb(test_alloc, right_path, 8);
    defer right_db.close();
    try left_db.addIndex(.{ .name = "dv_v1", .kind = .dense_vector, .config_json = "{\"field\":\"embedding\",\"dims\":3}" });
    try right_db.addIndex(.{ .name = "dv_v1", .kind = .dense_vector, .config_json = "{\"field\":\"embedding\",\"dims\":3}" });

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
                    .indexes_json = "{\"dv_v1\":{\"type\":\"embeddings\",\"dimension\":3,\"external\":true}}",
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

    var dense_summary = (try hosted.source().preflightQuery(test_alloc, "docs", .{
        .index_name = "dv_v1",
        .dense = .{ .vector = &.{ 1.0, 2.0, 3.0 }, .k = 5 },
    }, .read_index, 0)).?;
    defer dense_summary.deinit(test_alloc);
    try std.testing.expectEqual(@as(u32, 2), dense_summary.shard_count);
    try std.testing.expectEqual(@as(u32, 0), dense_summary.remote_shard_count);
    try std.testing.expectEqual(@as(u32, 2), dense_summary.dense_query_count);
    try std.testing.expectEqual(@as(u32, 2), dense_summary.vector_worker_candidate_count);
    try std.testing.expectEqual(@as(u64, 10), dense_summary.dense_effective_k_total);
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
    const path = try uniqueTestTmpPathAlloc(test_alloc, "antfly-api-hosted-preflight-mixed");
    defer test_alloc.free(path);

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
                    .indexes_json = tables_api.default_indexes_json,
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
    const path = try uniqueTestTmpPathAlloc(alloc, "antfly-api-hosted-cross-range-graph-explicit");
    defer alloc.free(path);

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

    var left_db = try openTestGroupDb(alloc, left_path, 7001);
    defer left_db.close();
    try left_db.addIndex(.{ .name = "relations_graph", .kind = .graph, .config_json = "{\"edge_types\":[{\"name\":\"mentions\"}]}" });
    try left_db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"left\"}" }},
        .sync_level = .write,
    });

    var right_db = try openTestGroupDb(alloc, right_path, 7002);
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
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/hosted-cross-range-graph-metric-merge", .{tmp.sub_path});
    defer alloc.free(path);

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
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/hosted-cross-range-graph-metric-active-stale", .{tmp.sub_path});
    defer alloc.free(path);

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
    var left_db_open = true;
    defer if (left_db_open) left_db.close();
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
    var right_db_open = true;
    defer if (right_db_open) right_db.close();
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

    left_db.close();
    left_db_open = false;
    right_db.close();
    right_db_open = false;

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
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/hosted-cross-range-graph-metric-promotion-merge", .{tmp.sub_path});
    defer alloc.free(path);
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
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/hosted-cross-range-graph-metric-hits-pair", .{tmp.sub_path});
    defer alloc.free(path);
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

    for (dbs[0..db_count]) |*db| db.close();
    db_count = 0;

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
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/hosted-cross-range-graph-metric-hits-pair-reject", .{tmp.sub_path});
    defer alloc.free(path);

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
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/hosted-cross-range-graph-metric-hits-missing-status", .{tmp.sub_path});
    defer alloc.free(path);

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
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/hosted-cross-range-graph-metric-reject", .{tmp.sub_path});
    defer alloc.free(path);

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

test "provisioned primary lookup lease fails on identity namespace mismatch" {
    const alloc = std.testing.allocator;
    const path = try uniqueTestTmpPathAlloc(alloc, "antfly-api-provisioned-primary-lookup-identity-mismatch");
    defer alloc.free(path);

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
                    .indexes_json = tables_api.default_indexes_json,
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
