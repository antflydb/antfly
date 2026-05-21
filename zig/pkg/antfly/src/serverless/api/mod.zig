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

pub const types = @import("types.zig");
pub const query_types = @import("query_types.zig");
pub const codec = @import("codec.zig");
pub const service = @import("service.zig");
pub const http_routes = @import("http_routes.zig");
pub const http_types = @import("http_types.zig");
pub const http_handler = @import("http_handler.zig");
pub const catalog = @import("../catalog/mod.zig");

pub const DocumentMutation = types.DocumentMutation;
pub const MutationKind = types.MutationKind;
pub const EnsureNamespaceRequest = types.EnsureNamespaceRequest;
pub const EnsureNamespaceResult = types.EnsureNamespaceResult;
pub const EnsureTableRequest = types.EnsureTableRequest;
pub const EnsureTableResult = types.EnsureTableResult;
pub const TableRecord = types.TableRecord;
pub const IngestBatchRequest = types.IngestBatchRequest;
pub const IngestBatchResult = types.IngestBatchResult;
pub const TableIngestBatchRequest = types.TableIngestBatchRequest;
pub const TableIngestBatchResult = types.TableIngestBatchResult;
pub const NamespacePolicyRequest = types.NamespacePolicyRequest;
pub const NamespacePolicyResult = types.NamespacePolicyResult;
pub const TablePolicyResult = types.TablePolicyResult;
pub const HeadPublishRequest = types.HeadPublishRequest;
pub const HeadPublishResult = types.HeadPublishResult;
pub const TableBuildResult = types.TableBuildResult;
pub const TableBuildStatus = types.TableBuildStatus;
pub const RuntimeRole = types.RuntimeRole;
pub const RuntimeStorageBackend = types.RuntimeStorageBackend;
pub const RuntimeStorageTarget = types.RuntimeStorageTarget;
pub const RuntimeStatusResult = types.RuntimeStatusResult;
pub const HealthResult = types.HealthResult;
pub const MetricsNamespace = types.MetricsNamespace;
pub const MetricsResult = types.MetricsResult;
pub const QueryView = query_types.QueryView;
pub const QueryArtifactSummary = query_types.QueryArtifactSummary;
pub const QueryMutation = query_types.QueryMutation;
pub const QueryDocument = query_types.QueryDocument;
pub const QueryArtifactContents = query_types.QueryArtifactContents;
pub const QueryTailMutation = query_types.QueryTailMutation;
pub const QueryResult = query_types.QueryResult;
pub const TableQueryResult = query_types.TableQueryResult;
pub const QueryArtifactResult = query_types.QueryArtifactResult;
pub const QueryRequest = query_types.QueryRequest;
pub const QueryMode = query_types.QueryMode;
pub const GraphQueryDirection = query_types.GraphQueryDirection;
pub const QueryOperator = query_types.QueryOperator;
pub const QueryFusionStrategy = query_types.QueryFusionStrategy;
pub const SparseTermWeight = query_types.SparseTermWeight;
pub const GraphNeighborsRequest = query_types.GraphNeighborsRequest;
pub const GraphTraverseRequest = query_types.GraphTraverseRequest;
pub const GraphShortestPathRequest = query_types.GraphShortestPathRequest;
pub const QueryHit = query_types.QueryHit;
pub const QuerySearchResult = query_types.QuerySearchResult;
pub const TableQuerySearchResult = query_types.TableQuerySearchResult;
pub const GraphNeighbor = query_types.GraphNeighbor;
pub const GraphPathHop = query_types.GraphPathHop;
pub const GraphNeighborsResult = query_types.GraphNeighborsResult;
pub const TableGraphNeighborsResult = query_types.TableGraphNeighborsResult;
pub const GraphTraversalNode = query_types.GraphTraversalNode;
pub const GraphTraverseResult = query_types.GraphTraverseResult;
pub const TableGraphTraverseResult = query_types.TableGraphTraverseResult;
pub const GraphShortestPathResult = query_types.GraphShortestPathResult;
pub const TableGraphShortestPathResult = query_types.TableGraphShortestPathResult;
pub const encodeMutationAlloc = codec.encodeMutationAlloc;
pub const decodeMutationAlloc = codec.decodeMutationAlloc;
pub const Service = service.Service;
pub const HttpMethod = http_routes.HttpMethod;
pub const Route = http_routes.Route;
pub const matchRoute = http_routes.match;
pub const HttpRequest = http_types.HttpRequest;
pub const HttpResponse = http_types.HttpResponse;
pub const HttpHandler = http_handler.HttpHandler;
pub const CatalogService = catalog.CatalogService;

test "serverless api module compiles" {
    _ = types;
    _ = query_types;
    _ = codec;
    _ = service;
    _ = http_routes;
    _ = http_types;
    _ = http_handler;
    _ = catalog;
    _ = DocumentMutation;
    _ = MutationKind;
    _ = EnsureNamespaceRequest;
    _ = EnsureNamespaceResult;
    _ = EnsureTableRequest;
    _ = EnsureTableResult;
    _ = TableRecord;
    _ = IngestBatchRequest;
    _ = IngestBatchResult;
    _ = TableIngestBatchRequest;
    _ = TableIngestBatchResult;
    _ = NamespacePolicyRequest;
    _ = NamespacePolicyResult;
    _ = TablePolicyResult;
    _ = HeadPublishRequest;
    _ = HeadPublishResult;
    _ = TableBuildResult;
    _ = TableBuildStatus;
    _ = RuntimeRole;
    _ = RuntimeStorageBackend;
    _ = RuntimeStorageTarget;
    _ = RuntimeStatusResult;
    _ = HealthResult;
    _ = MetricsNamespace;
    _ = MetricsResult;
    _ = QueryView;
    _ = QueryArtifactSummary;
    _ = QueryMutation;
    _ = QueryDocument;
    _ = QueryArtifactContents;
    _ = QueryTailMutation;
    _ = QueryResult;
    _ = TableQueryResult;
    _ = QueryArtifactResult;
    _ = QueryRequest;
    _ = QueryMode;
    _ = GraphQueryDirection;
    _ = QueryOperator;
    _ = QueryFusionStrategy;
    _ = SparseTermWeight;
    _ = GraphNeighborsRequest;
    _ = GraphTraverseRequest;
    _ = GraphShortestPathRequest;
    _ = QueryHit;
    _ = QuerySearchResult;
    _ = TableQuerySearchResult;
    _ = GraphNeighbor;
    _ = GraphPathHop;
    _ = GraphNeighborsResult;
    _ = TableGraphNeighborsResult;
    _ = GraphTraversalNode;
    _ = GraphTraverseResult;
    _ = TableGraphTraverseResult;
    _ = GraphShortestPathResult;
    _ = TableGraphShortestPathResult;
    _ = encodeMutationAlloc;
    _ = decodeMutationAlloc;
    _ = Service;
    _ = HttpMethod;
    _ = Route;
    _ = matchRoute;
    _ = HttpRequest;
    _ = HttpResponse;
    _ = HttpHandler;
    _ = CatalogService;
}

test "serverless public graph seed totals mark saturated pages incomplete" {
    try http_handler.testPublicGraphSeedTotalHits();
}
