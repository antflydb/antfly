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

pub const runtime = @import("runtime.zig");
pub const materializer = @import("materializer.zig");
pub const request = @import("request.zig");
pub const plan = @import("plan.zig");
pub const cache = @import("cache.zig");
pub const indexed_reader = @import("indexed_reader.zig");
pub const graph_reader = @import("graph_reader.zig");
pub const lake_rows = @import("lake_rows.zig");
pub const lake_explain = @import("lake_explain.zig");
pub const lake_cache = @import("lake_cache.zig");
pub const lake_range_io = @import("lake_range_io.zig");

pub const QueryRuntime = runtime.QueryRuntime;
pub const QuerySession = runtime.QuerySession;
pub const QueryRequest = request.QueryRequest;
pub const SearchPlan = plan.SearchPlan;
pub const QueryMode = request.QueryMode;
pub const GraphQueryDirection = request.GraphQueryDirection;
pub const GraphTraverseRequest = request.GraphTraverseRequest;
pub const GraphShortestPathRequest = request.GraphShortestPathRequest;
pub const QueryOperator = request.QueryOperator;
pub const QueryFusionStrategy = request.QueryFusionStrategy;
pub const SparseTermWeight = request.SparseTermWeight;
pub const GraphNeighborsRequest = request.GraphNeighborsRequest;
pub const QuerySearchHit = request.SearchHit;
pub const freeSearchHits = request.freeHits;
pub const GraphNeighbor = graph_reader.Neighbor;
pub const GraphTraversalNode = graph_reader.TraversalNode;
pub const GraphPathHop = graph_reader.PathHop;
pub const GraphShortestPath = graph_reader.ShortestPath;
pub const freeGraphNeighbors = graph_reader.freeNeighbors;
pub const freeGraphTraversalNodes = graph_reader.freeTraversalNodes;
pub const freeGraphPathHops = graph_reader.freePathHops;
pub const freeGraphShortestPath = graph_reader.freeShortestPath;
pub const QuerySearchExecutionStats = indexed_reader.SearchExecutionStats;
pub const parseSearchPlanAlloc = plan.parseSearchPlanAlloc;
pub const parseGraphNeighborsPlanAlloc = plan.parseGraphNeighborsPlanAlloc;
pub const parseGraphTraversePlanAlloc = plan.parseGraphTraversePlanAlloc;
pub const parseGraphShortestPathPlanAlloc = plan.parseGraphShortestPathPlanAlloc;
pub const QueryCache = cache.QueryCache;
pub const QueryCacheConfig = cache.QueryCacheConfig;
pub const QueryCacheStats = cache.QueryCacheStats;
pub const QueryExecutionMetrics = runtime.QueryExecutionMetrics;
pub const NamespaceQueryExecutionMetrics = runtime.NamespaceQueryExecutionMetrics;
pub const LakeRowsGroupByRequest = lake_rows.GroupByRequest;
pub const LakeRowsGroupByResult = lake_rows.GroupByResult;
pub const LakeRowsHydrateResult = lake_rows.HydrateResult;
pub const LakeRowsPredicate = lake_rows.Predicate;
pub const LakeRowsPredicateOp = lake_rows.PredicateOp;
pub const LakeRowsScanRequest = lake_rows.ScanRequest;
pub const LakeRowsScanResult = lake_rows.ScanResult;
pub const LakeExplainOperation = lake_explain.Operation;
pub const LakeExplainRequest = lake_explain.Request;
pub const LakeExplainPlan = lake_explain.Plan;
pub const explainLakeQuery = lake_explain.explain;
pub const LakeCacheClass = lake_cache.CacheClass;
pub const LakeCacheBudget = lake_cache.Budget;
pub const LakeCacheAccounting = lake_cache.Accounting;
pub const LakeRangeCacheLane = lake_range_io.CacheLane;
pub const LakeRangePurpose = lake_range_io.RangePurpose;
pub const LakeRangeObjectVersion = lake_range_io.ObjectVersion;
pub const LakeRangeObjectRef = lake_range_io.ObjectRef;
pub const LakeByteRange = lake_range_io.ByteRange;
pub const LakeRangeRead = lake_range_io.RangeRead;
pub const LakeRangeCoalesceOptions = lake_range_io.CoalesceOptions;
pub const accountLakeCacheArtifacts = lake_cache.accountArtifacts;
pub const classifyLakeCacheArtifact = lake_cache.classifyArtifact;
pub const planLakeParquetFooterRead = lake_range_io.planParquetFooterRead;
pub const coalesceLakePhysicalReadsAlloc = lake_range_io.coalescePhysicalReadsAlloc;
pub const executeLakeRowsGroupByAlloc = lake_rows.executeGroupByAlloc;
pub const hydrateLakeRowsAlloc = lake_rows.hydrateRowsAlloc;
pub const scanLakeRowsAlloc = lake_rows.scanRowsAlloc;
pub const searchIndexedPlanAlloc = indexed_reader.searchPlanAlloc;
pub const searchIndexedPlanWithStatsAlloc = indexed_reader.searchPlanAllocWithStats;
pub const searchIndexedAlloc = indexed_reader.searchAlloc;
pub const searchIndexedWithStatsAlloc = indexed_reader.searchAllocWithStats;
pub const warmIndexedSearchPlanPath = indexed_reader.warmSearchPlanPath;
pub const warmIndexedSearchPath = indexed_reader.warmSearchPath;
pub const graphNeighborsAlloc = graph_reader.neighborsAlloc;
pub const graphTraverseAlloc = graph_reader.traverseAlloc;
pub const graphShortestPathAlloc = graph_reader.shortestPathAlloc;
pub const QueryMaterializerMutation = materializer.Mutation;
pub const QueryMaterializedDocument = materializer.Document;
pub const materializeDocumentsAlloc = materializer.materializeAlloc;
pub const materializeDocumentsOverBaseAlloc = materializer.materializeOverBaseAlloc;
pub const freeMaterializedDocuments = materializer.freeDocuments;

test "serverless query module compiles" {
    _ = runtime;
    _ = materializer;
    _ = request;
    _ = plan;
    _ = cache;
    _ = indexed_reader;
    _ = graph_reader;
    _ = lake_rows;
    _ = lake_explain;
    _ = lake_cache;
    _ = lake_range_io;
    _ = QueryRuntime;
    _ = QuerySession;
    _ = QueryRequest;
    _ = SearchPlan;
    _ = QueryMode;
    _ = GraphQueryDirection;
    _ = GraphTraverseRequest;
    _ = GraphShortestPathRequest;
    _ = QueryOperator;
    _ = QueryFusionStrategy;
    _ = SparseTermWeight;
    _ = GraphNeighborsRequest;
    _ = QuerySearchHit;
    _ = freeSearchHits;
    _ = GraphNeighbor;
    _ = GraphTraversalNode;
    _ = GraphPathHop;
    _ = GraphShortestPath;
    _ = freeGraphNeighbors;
    _ = freeGraphTraversalNodes;
    _ = freeGraphPathHops;
    _ = freeGraphShortestPath;
    _ = QuerySearchExecutionStats;
    _ = parseSearchPlanAlloc;
    _ = parseGraphNeighborsPlanAlloc;
    _ = parseGraphTraversePlanAlloc;
    _ = parseGraphShortestPathPlanAlloc;
    _ = QueryCache;
    _ = QueryCacheConfig;
    _ = QueryCacheStats;
    _ = QueryExecutionMetrics;
    _ = NamespaceQueryExecutionMetrics;
    _ = LakeRowsGroupByRequest;
    _ = LakeRowsGroupByResult;
    _ = LakeRowsHydrateResult;
    _ = LakeRowsPredicate;
    _ = LakeRowsPredicateOp;
    _ = LakeRowsScanRequest;
    _ = LakeRowsScanResult;
    _ = LakeExplainOperation;
    _ = LakeExplainRequest;
    _ = LakeExplainPlan;
    _ = explainLakeQuery;
    _ = LakeCacheClass;
    _ = LakeCacheBudget;
    _ = LakeCacheAccounting;
    _ = LakeRangeCacheLane;
    _ = LakeRangePurpose;
    _ = LakeRangeObjectVersion;
    _ = LakeRangeObjectRef;
    _ = LakeByteRange;
    _ = LakeRangeRead;
    _ = LakeRangeCoalesceOptions;
    _ = accountLakeCacheArtifacts;
    _ = classifyLakeCacheArtifact;
    _ = planLakeParquetFooterRead;
    _ = coalesceLakePhysicalReadsAlloc;
    _ = executeLakeRowsGroupByAlloc;
    _ = hydrateLakeRowsAlloc;
    _ = scanLakeRowsAlloc;
    _ = searchIndexedPlanAlloc;
    _ = searchIndexedPlanWithStatsAlloc;
    _ = searchIndexedAlloc;
    _ = searchIndexedWithStatsAlloc;
    _ = warmIndexedSearchPlanPath;
    _ = warmIndexedSearchPath;
    _ = graphNeighborsAlloc;
    _ = graphTraverseAlloc;
    _ = graphShortestPathAlloc;
    _ = QueryMaterializerMutation;
    _ = QueryMaterializedDocument;
    _ = materializeDocumentsAlloc;
    _ = materializeDocumentsOverBaseAlloc;
    _ = freeMaterializedDocuments;

    const alloc = std.testing.allocator;
    const sources = @import("../search_sources.zig").defaultPublishedSearchSources();

    try std.testing.expectError(error.InvalidQueryRequest, parseSearchPlanAlloc(
        alloc,
        "{\"embeddings\":{\"serverless_chunk\":[1.0,0.0,0.0]},\"identity_read_generation\":7}",
        sources,
    ));
    try std.testing.expectError(error.InvalidQueryRequest, parseSearchPlanAlloc(
        alloc,
        "{\"embeddings\":{\"serverless_chunk\":[1.0,0.0,0.0]},\"allow_doc_identity_reassignment\":true}",
        sources,
    ));
    try std.testing.expectError(error.InvalidQueryRequest, parseSearchPlanAlloc(
        alloc,
        "{\"text\":\"alpha\",\"native_doc_id_constraints\":{\"include_doc_ids\":[\"doc:a\"]}}",
        sources,
    ));
    try std.testing.expectError(error.InvalidQueryRequest, parseGraphNeighborsPlanAlloc(
        alloc,
        "{\"doc_id\":\"doc:a\",\"identity_read_generation\":7}",
    ));
    try std.testing.expectError(error.InvalidQueryRequest, parseGraphTraversePlanAlloc(
        alloc,
        "{\"start_doc_id\":\"doc:a\",\"allow_doc_identity_reassignment\":true}",
    ));
    try std.testing.expectError(error.InvalidQueryRequest, parseGraphShortestPathPlanAlloc(
        alloc,
        "{\"start_doc_id\":\"doc:a\",\"end_doc_id\":\"doc:b\",\"native_doc_id_constraints\":{\"include_doc_ids\":[\"doc:a\"]}}",
    ));
}
