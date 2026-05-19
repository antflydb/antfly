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
const transport_routes = @import("raft/transport/routes.zig");
const http_common = @import("raft/transport/http_common.zig");
const public_test_helpers = @import("public_test_helpers.zig");
const serverless = @import("serverless/mod.zig");
const managed_embedder = @import("inference/managed_embedder.zig");

pub const EnsureNamespaceResult = serverless.EnsureNamespaceResult;
pub const EnsureTableResult = serverless.EnsureTableResult;
pub const TableRecord = serverless.TableRecord;
pub const TableIndexListResponse = std.json.Value;
pub const TableIndexResponse = std.json.Value;
pub const TableIngestBatchResult = serverless.TableIngestBatchResult;
pub const TablePolicyResult = serverless.TablePolicyResult;
pub const TableBuildResult = serverless.TableBuildResult;
pub const TableBuildStatusResponse = serverless.TableBuildStatus;
pub const QueryHeadArtifact = serverless.QueryArtifactSummary;
pub const QueryHeadResponse = serverless.QueryResult;
pub const TableQueryResponse = serverless.TableQueryResult;
pub const QuerySearchResponse = serverless.QuerySearchResult;
pub const TableQuerySearchResponse = serverless.TableQuerySearchResult;
pub const GraphNeighborsResponse = serverless.GraphNeighborsResult;
pub const GraphTraverseResponse = serverless.GraphTraverseResult;
pub const GraphShortestPathResponse = serverless.GraphShortestPathResult;
pub const TableGraphNeighborsResponse = serverless.TableGraphNeighborsResult;
pub const TableGraphTraverseResponse = serverless.TableGraphTraverseResult;
pub const TableGraphShortestPathResponse = serverless.TableGraphShortestPathResult;
pub const SingleArtifactResponse = serverless.QueryArtifactResult;
pub const QueryArtifactWithContents = serverless.QueryArtifactContents;
pub const HealthResponse = serverless.HealthResult;
pub const MetricsResponse = serverless.MetricsResult;
pub const RuntimeStatusResponse = serverless.RuntimeStatusResult;
// Product-facing callers should use the table view.
pub const ServerlessTableHttpClient = ServerlessHttpClient.TableApi;
// Ops/debug callers that need serving internals should use the internal view.
pub const ServerlessInternalHttpClient = ServerlessHttpClient.InternalApi;

pub const ServerlessHttpClient = struct {
    alloc: std.mem.Allocator,
    executor: http_common.RequestExecutor,

    // Canonical product surface for cheap hosted Antfly.
    pub const TableApi = struct {
        client: *ServerlessHttpClient,

        pub fn ensure(self: TableApi, base_uri: []const u8, table_name: []const u8, created_at_ns: u64) !EnsureTableResult {
            return try self.client.ensureTable(base_uri, table_name, created_at_ns);
        }

        pub fn ensureWithPolicy(
            self: TableApi,
            base_uri: []const u8,
            table_name: []const u8,
            req: serverless.EnsureTableRequest,
        ) !EnsureTableResult {
            return try self.client.ensureTableWithPolicy(base_uri, table_name, req);
        }

        pub fn list(self: TableApi, base_uri: []const u8) !std.json.Parsed([]TableRecord) {
            return try self.client.listTables(base_uri);
        }

        pub fn ingest(
            self: TableApi,
            base_uri: []const u8,
            req: serverless.TableIngestBatchRequest,
        ) !TableIngestBatchResult {
            return try self.client.ingestTableBatch(base_uri, req);
        }

        pub fn listIndexes(self: TableApi, base_uri: []const u8, table_name: []const u8) !std.json.Parsed(TableIndexListResponse) {
            return try self.client.listTableIndexes(base_uri, table_name);
        }

        pub fn getIndex(self: TableApi, base_uri: []const u8, table_name: []const u8, index_name: []const u8) !std.json.Parsed(TableIndexResponse) {
            return try self.client.getTableIndex(base_uri, table_name, index_name);
        }

        pub fn createIndex(self: TableApi, base_uri: []const u8, table_name: []const u8, index_name: []const u8, payload: std.json.Value) !std.json.Parsed(std.json.Value) {
            return try self.client.createTableIndex(base_uri, table_name, index_name, payload);
        }

        pub fn deleteIndex(self: TableApi, base_uri: []const u8, table_name: []const u8, index_name: []const u8) !std.json.Parsed(std.json.Value) {
            return try self.client.deleteTableIndex(base_uri, table_name, index_name);
        }

        pub fn query(self: TableApi, base_uri: []const u8, table_name: []const u8) !std.json.Parsed(TableQueryResponse) {
            return try self.client.queryTable(base_uri, table_name);
        }

        pub fn queryPublished(self: TableApi, base_uri: []const u8, table_name: []const u8) !std.json.Parsed(TableQueryResponse) {
            return try self.client.queryTablePublished(base_uri, table_name);
        }

        pub fn queryLatest(self: TableApi, base_uri: []const u8, table_name: []const u8) !std.json.Parsed(TableQueryResponse) {
            return try self.client.queryTableLatest(base_uri, table_name);
        }

        pub fn search(
            self: TableApi,
            base_uri: []const u8,
            table_name: []const u8,
            req: serverless.QueryRequest,
        ) !std.json.Parsed(TableQuerySearchResponse) {
            return try self.client.searchTable(base_uri, table_name, req);
        }

        pub fn graphNeighbors(
            self: TableApi,
            base_uri: []const u8,
            table_name: []const u8,
            req: serverless.GraphNeighborsRequest,
        ) !std.json.Parsed(TableGraphNeighborsResponse) {
            return try self.client.graphNeighborsTable(base_uri, table_name, req);
        }

        pub fn graphTraverse(
            self: TableApi,
            base_uri: []const u8,
            table_name: []const u8,
            req: serverless.GraphTraverseRequest,
        ) !std.json.Parsed(TableGraphTraverseResponse) {
            return try self.client.graphTraverseTable(base_uri, table_name, req);
        }

        pub fn graphShortestPath(
            self: TableApi,
            base_uri: []const u8,
            table_name: []const u8,
            req: serverless.GraphShortestPathRequest,
        ) !std.json.Parsed(TableGraphShortestPathResponse) {
            return try self.client.graphShortestPathTable(base_uri, table_name, req);
        }
    };

    // Explicit serving/debug surface for internal namespace-level operations.
    pub const InternalApi = struct {
        client: *ServerlessHttpClient,

        pub fn ensureNamespace(
            self: InternalApi,
            base_uri: []const u8,
            namespace: []const u8,
            created_at_ns: u64,
        ) !EnsureNamespaceResult {
            return try self.client.ensureNamespace(base_uri, namespace, created_at_ns);
        }

        pub fn ensureNamespaceWithPolicy(
            self: InternalApi,
            base_uri: []const u8,
            namespace: []const u8,
            req: serverless.EnsureNamespaceRequest,
        ) !EnsureNamespaceResult {
            return try self.client.ensureNamespaceWithPolicy(base_uri, namespace, req);
        }

        pub fn listNamespaces(self: InternalApi, base_uri: []const u8) !std.json.Parsed([]serverless.NamespaceRecord) {
            return try self.client.listNamespaces(base_uri);
        }

        pub fn fetchPolicy(self: InternalApi, base_uri: []const u8, namespace: []const u8) !serverless.NamespacePolicyResult {
            return try self.client.fetchPolicy(base_uri, namespace);
        }

        pub fn fetchTablePolicy(self: InternalApi, base_uri: []const u8, table_name: []const u8) !TablePolicyResult {
            return try self.client.fetchTablePolicy(base_uri, table_name);
        }

        pub fn updatePolicy(
            self: InternalApi,
            base_uri: []const u8,
            namespace: []const u8,
            req: serverless.NamespacePolicyRequest,
        ) !serverless.NamespacePolicyResult {
            return try self.client.updatePolicy(base_uri, namespace, req);
        }

        pub fn updateTablePolicy(
            self: InternalApi,
            base_uri: []const u8,
            table_name: []const u8,
            req: serverless.NamespacePolicyRequest,
        ) !TablePolicyResult {
            return try self.client.updateTablePolicy(base_uri, table_name, req);
        }

        pub fn publishHead(
            self: InternalApi,
            base_uri: []const u8,
            namespace: []const u8,
            req: serverless.HeadPublishRequest,
        ) !serverless.HeadPublishResult {
            return try self.client.publishHead(base_uri, namespace, req);
        }

        pub fn ingest(
            self: InternalApi,
            base_uri: []const u8,
            req: serverless.IngestBatchRequest,
        ) !serverless.IngestBatchResult {
            return try self.client.ingestBatch(base_uri, req);
        }

        pub fn build(self: InternalApi, base_uri: []const u8, namespace: []const u8) !serverless.BuildResult {
            return try self.client.buildNamespace(base_uri, namespace);
        }

        pub fn buildTable(self: InternalApi, base_uri: []const u8, table_name: []const u8) !TableBuildResult {
            return try self.client.buildTable(base_uri, table_name);
        }

        pub fn buildStatus(self: InternalApi, base_uri: []const u8, namespace: []const u8) !serverless.BuildStatus {
            return try self.client.buildStatus(base_uri, namespace);
        }

        pub fn buildTableStatus(self: InternalApi, base_uri: []const u8, table_name: []const u8) !TableBuildStatusResponse {
            return try self.client.tableBuildStatus(base_uri, table_name);
        }

        pub fn fetchHead(self: InternalApi, base_uri: []const u8, namespace: []const u8) !std.json.Parsed(serverless.Manifest) {
            return try self.client.fetchHead(base_uri, namespace);
        }

        pub fn queryHead(self: InternalApi, base_uri: []const u8, namespace: []const u8) !std.json.Parsed(QueryHeadResponse) {
            return try self.client.queryHead(base_uri, namespace);
        }

        pub fn query(self: InternalApi, base_uri: []const u8, namespace: []const u8) !std.json.Parsed(QueryHeadResponse) {
            return try self.client.query(base_uri, namespace);
        }

        pub fn queryLatest(self: InternalApi, base_uri: []const u8, namespace: []const u8) !std.json.Parsed(QueryHeadResponse) {
            return try self.client.queryLatest(base_uri, namespace);
        }

        pub fn queryVersion(
            self: InternalApi,
            base_uri: []const u8,
            namespace: []const u8,
            version: u64,
        ) !std.json.Parsed(QueryHeadResponse) {
            return try self.client.queryVersion(base_uri, namespace, version);
        }

        pub fn search(
            self: InternalApi,
            base_uri: []const u8,
            namespace: []const u8,
            req: serverless.QueryRequest,
        ) !std.json.Parsed(QuerySearchResponse) {
            return try self.client.search(base_uri, namespace, req);
        }

        pub fn graphNeighbors(
            self: InternalApi,
            base_uri: []const u8,
            namespace: []const u8,
            req: serverless.GraphNeighborsRequest,
        ) !std.json.Parsed(GraphNeighborsResponse) {
            return try self.client.graphNeighbors(base_uri, namespace, req);
        }

        pub fn graphNeighborsVersion(
            self: InternalApi,
            base_uri: []const u8,
            namespace: []const u8,
            version: u64,
            req: serverless.GraphNeighborsRequest,
        ) !std.json.Parsed(GraphNeighborsResponse) {
            return try self.client.graphNeighborsVersion(base_uri, namespace, version, req);
        }

        pub fn graphTraverse(
            self: InternalApi,
            base_uri: []const u8,
            namespace: []const u8,
            req: serverless.GraphTraverseRequest,
        ) !std.json.Parsed(GraphTraverseResponse) {
            return try self.client.graphTraverse(base_uri, namespace, req);
        }

        pub fn graphTraverseVersion(
            self: InternalApi,
            base_uri: []const u8,
            namespace: []const u8,
            version: u64,
            req: serverless.GraphTraverseRequest,
        ) !std.json.Parsed(GraphTraverseResponse) {
            return try self.client.graphTraverseVersion(base_uri, namespace, version, req);
        }

        pub fn graphShortestPath(
            self: InternalApi,
            base_uri: []const u8,
            namespace: []const u8,
            req: serverless.GraphShortestPathRequest,
        ) !std.json.Parsed(GraphShortestPathResponse) {
            return try self.client.graphShortestPath(base_uri, namespace, req);
        }

        pub fn graphShortestPathVersion(
            self: InternalApi,
            base_uri: []const u8,
            namespace: []const u8,
            version: u64,
            req: serverless.GraphShortestPathRequest,
        ) !std.json.Parsed(GraphShortestPathResponse) {
            return try self.client.graphShortestPathVersion(base_uri, namespace, version, req);
        }

        pub fn queryHeadArtifact(
            self: InternalApi,
            base_uri: []const u8,
            namespace: []const u8,
            artifact_index: usize,
        ) !std.json.Parsed(SingleArtifactResponse) {
            return try self.client.queryHeadArtifact(base_uri, namespace, artifact_index);
        }

        pub fn queryVersionArtifact(
            self: InternalApi,
            base_uri: []const u8,
            namespace: []const u8,
            version: u64,
            artifact_index: usize,
        ) !std.json.Parsed(SingleArtifactResponse) {
            return try self.client.queryVersionArtifact(base_uri, namespace, version, artifact_index);
        }
    };

    pub fn init(alloc: std.mem.Allocator, executor: http_common.RequestExecutor) ServerlessHttpClient {
        return .{
            .alloc = alloc,
            .executor = executor,
        };
    }

    // Product code should reach table routes through this view.
    pub fn tables(self: *ServerlessHttpClient) TableApi {
        return .{ .client = self };
    }

    // Internal tooling can opt into namespace/artifact/debug routes here.
    pub fn internal(self: *ServerlessHttpClient) InternalApi {
        return .{ .client = self };
    }

    fn ensureNamespace(
        self: *ServerlessHttpClient,
        base_uri: []const u8,
        namespace: []const u8,
        created_at_ns: u64,
    ) !EnsureNamespaceResult {
        return try self.ensureNamespaceWithPolicy(base_uri, namespace, .{
            .created_at_ns = created_at_ns,
        });
    }

    fn ensureNamespaceWithPolicy(
        self: *ServerlessHttpClient,
        base_uri: []const u8,
        namespace: []const u8,
        req: serverless.EnsureNamespaceRequest,
    ) !EnsureNamespaceResult {
        const path = try namespacePath(self.alloc, namespace);
        defer self.alloc.free(path);

        const body = try std.json.Stringify.valueAlloc(self.alloc, req, .{});
        defer self.alloc.free(body);

        return try self.requestJsonValue(EnsureNamespaceResult, .PUT, base_uri, path, body);
    }

    fn ensureTable(
        self: *ServerlessHttpClient,
        base_uri: []const u8,
        table_name: []const u8,
        created_at_ns: u64,
    ) !EnsureTableResult {
        return try self.ensureTableWithPolicy(base_uri, table_name, .{
            .created_at_ns = created_at_ns,
        });
    }

    fn ensureTableWithPolicy(
        self: *ServerlessHttpClient,
        base_uri: []const u8,
        table_name: []const u8,
        req: serverless.EnsureTableRequest,
    ) !EnsureTableResult {
        const path = try tablePath(self.alloc, table_name);
        defer self.alloc.free(path);

        const body = try std.json.Stringify.valueAlloc(self.alloc, req, .{});
        defer self.alloc.free(body);

        return try self.requestJsonValue(EnsureTableResult, .PUT, base_uri, path, body);
    }

    fn fetchPolicy(self: *ServerlessHttpClient, base_uri: []const u8, namespace: []const u8) !serverless.NamespacePolicyResult {
        const path = try std.fmt.allocPrint(self.alloc, "/internal/v1/namespaces/{s}/policy", .{namespace});
        defer self.alloc.free(path);
        return try self.requestJsonValue(serverless.NamespacePolicyResult, .GET, base_uri, path, null);
    }

    fn fetchTablePolicy(self: *ServerlessHttpClient, base_uri: []const u8, table_name: []const u8) !TablePolicyResult {
        const path = try std.fmt.allocPrint(self.alloc, "/internal/v1/tables/{s}/policy", .{table_name});
        defer self.alloc.free(path);
        return try self.requestJsonValue(TablePolicyResult, .GET, base_uri, path, null);
    }

    fn updatePolicy(
        self: *ServerlessHttpClient,
        base_uri: []const u8,
        namespace: []const u8,
        req: serverless.NamespacePolicyRequest,
    ) !serverless.NamespacePolicyResult {
        const path = try std.fmt.allocPrint(self.alloc, "/internal/v1/namespaces/{s}/policy", .{namespace});
        defer self.alloc.free(path);
        const body = try std.json.Stringify.valueAlloc(self.alloc, req, .{});
        defer self.alloc.free(body);
        return try self.requestJsonValue(serverless.NamespacePolicyResult, .PUT, base_uri, path, body);
    }

    fn updateTablePolicy(
        self: *ServerlessHttpClient,
        base_uri: []const u8,
        table_name: []const u8,
        req: serverless.NamespacePolicyRequest,
    ) !TablePolicyResult {
        const path = try std.fmt.allocPrint(self.alloc, "/internal/v1/tables/{s}/policy", .{table_name});
        defer self.alloc.free(path);
        const body = try std.json.Stringify.valueAlloc(self.alloc, req, .{});
        defer self.alloc.free(body);
        return try self.requestJsonValue(TablePolicyResult, .PUT, base_uri, path, body);
    }

    fn publishHead(
        self: *ServerlessHttpClient,
        base_uri: []const u8,
        namespace: []const u8,
        req: serverless.HeadPublishRequest,
    ) !serverless.HeadPublishResult {
        const path = try std.fmt.allocPrint(self.alloc, "/internal/v1/namespaces/{s}/head", .{namespace});
        defer self.alloc.free(path);

        const body = try std.json.Stringify.valueAlloc(self.alloc, req, .{});
        defer self.alloc.free(body);
        return try self.requestJsonValueAllowConflict(serverless.HeadPublishResult, .PUT, base_uri, path, body);
    }

    pub fn status(self: *ServerlessHttpClient, base_uri: []const u8) !std.json.Parsed(RuntimeStatusResponse) {
        return try self.requestJson(RuntimeStatusResponse, .GET, base_uri, "/status", null);
    }

    pub fn health(self: *ServerlessHttpClient, base_uri: []const u8) !std.json.Parsed(HealthResponse) {
        return try self.requestJson(HealthResponse, .GET, base_uri, "/health", null);
    }

    pub fn metrics(self: *ServerlessHttpClient, base_uri: []const u8) !std.json.Parsed(MetricsResponse) {
        return try self.requestJson(MetricsResponse, .GET, base_uri, "/metrics", null);
    }

    fn listNamespaces(self: *ServerlessHttpClient, base_uri: []const u8) !std.json.Parsed([]serverless.NamespaceRecord) {
        return try self.requestJson([]serverless.NamespaceRecord, .GET, base_uri, "/internal/v1/namespaces", null);
    }

    fn listTables(self: *ServerlessHttpClient, base_uri: []const u8) !std.json.Parsed([]TableRecord) {
        return try self.requestJson([]TableRecord, .GET, base_uri, "/tables", null);
    }

    fn listTableIndexes(self: *ServerlessHttpClient, base_uri: []const u8, table_name: []const u8) !std.json.Parsed(TableIndexListResponse) {
        const path = try std.fmt.allocPrint(self.alloc, "/tables/{s}/indexes", .{table_name});
        defer self.alloc.free(path);
        return try self.requestJson(TableIndexListResponse, .GET, base_uri, path, null);
    }

    fn getTableIndex(self: *ServerlessHttpClient, base_uri: []const u8, table_name: []const u8, index_name: []const u8) !std.json.Parsed(TableIndexResponse) {
        const path = try std.fmt.allocPrint(self.alloc, "/tables/{s}/indexes/{s}", .{ table_name, index_name });
        defer self.alloc.free(path);
        return try self.requestJson(TableIndexResponse, .GET, base_uri, path, null);
    }

    fn createTableIndex(
        self: *ServerlessHttpClient,
        base_uri: []const u8,
        table_name: []const u8,
        index_name: []const u8,
        payload: std.json.Value,
    ) !std.json.Parsed(std.json.Value) {
        const path = try std.fmt.allocPrint(self.alloc, "/tables/{s}/indexes/{s}", .{ table_name, index_name });
        defer self.alloc.free(path);
        const body = try std.fmt.allocPrint(self.alloc, "{f}", .{std.json.fmt(payload, .{})});
        defer self.alloc.free(body);
        return try self.requestJson(std.json.Value, .POST, base_uri, path, body);
    }

    fn deleteTableIndex(
        self: *ServerlessHttpClient,
        base_uri: []const u8,
        table_name: []const u8,
        index_name: []const u8,
    ) !std.json.Parsed(std.json.Value) {
        const path = try std.fmt.allocPrint(self.alloc, "/tables/{s}/indexes/{s}", .{ table_name, index_name });
        defer self.alloc.free(path);
        return try self.requestJson(std.json.Value, .DELETE, base_uri, path, null);
    }

    fn ingestBatch(
        self: *ServerlessHttpClient,
        base_uri: []const u8,
        req: serverless.IngestBatchRequest,
    ) !serverless.IngestBatchResult {
        const path = try std.fmt.allocPrint(self.alloc, "/internal/v1/namespaces/{s}/ingest-batch", .{req.namespace});
        defer self.alloc.free(path);

        const JsonMutation = struct {
            kind: []const u8,
            doc_id: []const u8,
            body: ?[]const u8 = null,
        };
        const json_mutations = try self.alloc.alloc(JsonMutation, req.mutations.len);
        defer self.alloc.free(json_mutations);

        for (req.mutations, 0..) |mutation, idx| {
            json_mutations[idx] = .{
                .kind = mutationKindString(mutation.kind),
                .doc_id = mutation.doc_id,
                .body = mutation.body,
            };
        }

        const body = try std.json.Stringify.valueAlloc(self.alloc, .{
            .timestamp_ns = req.timestamp_ns,
            .mutations = json_mutations,
        }, .{});
        defer self.alloc.free(body);

        return try self.requestJsonValue(serverless.IngestBatchResult, .PUT, base_uri, path, body);
    }

    fn ingestTableBatch(
        self: *ServerlessHttpClient,
        base_uri: []const u8,
        req: serverless.TableIngestBatchRequest,
    ) !TableIngestBatchResult {
        const path = try std.fmt.allocPrint(self.alloc, "/tables/{s}/ingest-batch", .{req.table_name});
        defer self.alloc.free(path);

        const JsonMutation = struct {
            kind: []const u8,
            doc_id: []const u8,
            body: ?[]const u8 = null,
        };
        const json_mutations = try self.alloc.alloc(JsonMutation, req.mutations.len);
        defer self.alloc.free(json_mutations);

        for (req.mutations, 0..) |mutation, idx| {
            json_mutations[idx] = .{
                .kind = mutationKindString(mutation.kind),
                .doc_id = mutation.doc_id,
                .body = mutation.body,
            };
        }

        const body = try std.json.Stringify.valueAlloc(self.alloc, .{
            .timestamp_ns = req.timestamp_ns,
            .mutations = json_mutations,
        }, .{});
        defer self.alloc.free(body);

        return try self.requestJsonValue(TableIngestBatchResult, .PUT, base_uri, path, body);
    }

    fn buildNamespace(self: *ServerlessHttpClient, base_uri: []const u8, namespace: []const u8) !serverless.BuildResult {
        const path = try std.fmt.allocPrint(self.alloc, "/internal/v1/namespaces/{s}/build", .{namespace});
        defer self.alloc.free(path);
        return try self.requestJsonValue(serverless.BuildResult, .POST, base_uri, path, "");
    }

    fn buildTable(self: *ServerlessHttpClient, base_uri: []const u8, table_name: []const u8) !TableBuildResult {
        const path = try std.fmt.allocPrint(self.alloc, "/internal/v1/tables/{s}/build", .{table_name});
        defer self.alloc.free(path);
        return try self.requestJsonValue(TableBuildResult, .POST, base_uri, path, "");
    }

    fn buildStatus(self: *ServerlessHttpClient, base_uri: []const u8, namespace: []const u8) !serverless.BuildStatus {
        const path = try std.fmt.allocPrint(self.alloc, "/internal/v1/namespaces/{s}/build-status", .{namespace});
        defer self.alloc.free(path);
        return try self.requestJsonValue(serverless.BuildStatus, .GET, base_uri, path, null);
    }

    fn tableBuildStatus(self: *ServerlessHttpClient, base_uri: []const u8, table_name: []const u8) !TableBuildStatusResponse {
        const path = try std.fmt.allocPrint(self.alloc, "/internal/v1/tables/{s}/build-status", .{table_name});
        defer self.alloc.free(path);
        return try self.requestJsonValue(TableBuildStatusResponse, .GET, base_uri, path, null);
    }

    fn fetchHead(self: *ServerlessHttpClient, base_uri: []const u8, namespace: []const u8) !std.json.Parsed(serverless.Manifest) {
        const path = try std.fmt.allocPrint(self.alloc, "/internal/v1/namespaces/{s}/head", .{namespace});
        defer self.alloc.free(path);
        return try self.requestJson(serverless.Manifest, .GET, base_uri, path, null);
    }

    fn queryHead(self: *ServerlessHttpClient, base_uri: []const u8, namespace: []const u8) !std.json.Parsed(QueryHeadResponse) {
        const path = try std.fmt.allocPrint(self.alloc, "/internal/v1/namespaces/{s}/query/head", .{namespace});
        defer self.alloc.free(path);
        return try self.requestJson(QueryHeadResponse, .GET, base_uri, path, null);
    }

    fn query(self: *ServerlessHttpClient, base_uri: []const u8, namespace: []const u8) !std.json.Parsed(QueryHeadResponse) {
        const path = try std.fmt.allocPrint(self.alloc, "/internal/v1/namespaces/{s}/query", .{namespace});
        defer self.alloc.free(path);
        return try self.requestJson(QueryHeadResponse, .GET, base_uri, path, null);
    }

    fn queryTable(self: *ServerlessHttpClient, base_uri: []const u8, table_name: []const u8) !std.json.Parsed(TableQueryResponse) {
        const path = try std.fmt.allocPrint(self.alloc, "/tables/{s}/query", .{table_name});
        defer self.alloc.free(path);
        return try self.requestJson(TableQueryResponse, .GET, base_uri, path, null);
    }

    fn queryTablePublished(self: *ServerlessHttpClient, base_uri: []const u8, table_name: []const u8) !std.json.Parsed(TableQueryResponse) {
        const path = try std.fmt.allocPrint(self.alloc, "/tables/{s}/query/published", .{table_name});
        defer self.alloc.free(path);
        return try self.requestJson(TableQueryResponse, .GET, base_uri, path, null);
    }

    fn queryTableLatest(self: *ServerlessHttpClient, base_uri: []const u8, table_name: []const u8) !std.json.Parsed(TableQueryResponse) {
        const path = try std.fmt.allocPrint(self.alloc, "/tables/{s}/query/latest", .{table_name});
        defer self.alloc.free(path);
        return try self.requestJson(TableQueryResponse, .GET, base_uri, path, null);
    }

    fn search(
        self: *ServerlessHttpClient,
        base_uri: []const u8,
        namespace: []const u8,
        req: serverless.QueryRequest,
    ) !std.json.Parsed(QuerySearchResponse) {
        const path = try std.fmt.allocPrint(self.alloc, "/internal/v1/namespaces/{s}/query/search", .{namespace});
        defer self.alloc.free(path);
        const body = try std.json.Stringify.valueAlloc(self.alloc, req, .{});
        defer self.alloc.free(body);
        return try self.requestJson(QuerySearchResponse, .POST, base_uri, path, body);
    }

    fn searchTable(
        self: *ServerlessHttpClient,
        base_uri: []const u8,
        table_name: []const u8,
        req: serverless.QueryRequest,
    ) !std.json.Parsed(TableQuerySearchResponse) {
        const path = try std.fmt.allocPrint(self.alloc, "/tables/{s}/query/search", .{table_name});
        defer self.alloc.free(path);
        const body = try std.json.Stringify.valueAlloc(self.alloc, req, .{});
        defer self.alloc.free(body);
        return try self.requestJson(TableQuerySearchResponse, .POST, base_uri, path, body);
    }

    fn graphNeighbors(
        self: *ServerlessHttpClient,
        base_uri: []const u8,
        namespace: []const u8,
        req: serverless.GraphNeighborsRequest,
    ) !std.json.Parsed(GraphNeighborsResponse) {
        const path = try std.fmt.allocPrint(self.alloc, "/internal/v1/namespaces/{s}/query/graph/neighbors", .{namespace});
        defer self.alloc.free(path);
        const body = try std.json.Stringify.valueAlloc(self.alloc, req, .{});
        defer self.alloc.free(body);
        return try self.requestJson(GraphNeighborsResponse, .POST, base_uri, path, body);
    }

    fn graphNeighborsTable(
        self: *ServerlessHttpClient,
        base_uri: []const u8,
        table_name: []const u8,
        req: serverless.GraphNeighborsRequest,
    ) !std.json.Parsed(TableGraphNeighborsResponse) {
        const path = try std.fmt.allocPrint(self.alloc, "/tables/{s}/query/graph/neighbors", .{table_name});
        defer self.alloc.free(path);
        const body = try std.json.Stringify.valueAlloc(self.alloc, req, .{});
        defer self.alloc.free(body);
        return try self.requestJson(TableGraphNeighborsResponse, .POST, base_uri, path, body);
    }

    fn graphNeighborsVersion(
        self: *ServerlessHttpClient,
        base_uri: []const u8,
        namespace: []const u8,
        version: u64,
        req: serverless.GraphNeighborsRequest,
    ) !std.json.Parsed(GraphNeighborsResponse) {
        const path = try std.fmt.allocPrint(self.alloc, "/internal/v1/namespaces/{s}/query/versions/{d}/graph/neighbors", .{ namespace, version });
        defer self.alloc.free(path);
        const body = try std.json.Stringify.valueAlloc(self.alloc, req, .{});
        defer self.alloc.free(body);
        return try self.requestJson(GraphNeighborsResponse, .POST, base_uri, path, body);
    }

    fn graphTraverse(
        self: *ServerlessHttpClient,
        base_uri: []const u8,
        namespace: []const u8,
        req: serverless.GraphTraverseRequest,
    ) !std.json.Parsed(GraphTraverseResponse) {
        const path = try std.fmt.allocPrint(self.alloc, "/internal/v1/namespaces/{s}/query/graph/traverse", .{namespace});
        defer self.alloc.free(path);
        const body = try std.json.Stringify.valueAlloc(self.alloc, req, .{});
        defer self.alloc.free(body);
        return try self.requestJson(GraphTraverseResponse, .POST, base_uri, path, body);
    }

    fn graphTraverseTable(
        self: *ServerlessHttpClient,
        base_uri: []const u8,
        table_name: []const u8,
        req: serverless.GraphTraverseRequest,
    ) !std.json.Parsed(TableGraphTraverseResponse) {
        const path = try std.fmt.allocPrint(self.alloc, "/tables/{s}/query/graph/traverse", .{table_name});
        defer self.alloc.free(path);
        const body = try std.json.Stringify.valueAlloc(self.alloc, req, .{});
        defer self.alloc.free(body);
        return try self.requestJson(TableGraphTraverseResponse, .POST, base_uri, path, body);
    }

    fn graphTraverseVersion(
        self: *ServerlessHttpClient,
        base_uri: []const u8,
        namespace: []const u8,
        version: u64,
        req: serverless.GraphTraverseRequest,
    ) !std.json.Parsed(GraphTraverseResponse) {
        const path = try std.fmt.allocPrint(self.alloc, "/internal/v1/namespaces/{s}/query/versions/{d}/graph/traverse", .{ namespace, version });
        defer self.alloc.free(path);
        const body = try std.json.Stringify.valueAlloc(self.alloc, req, .{});
        defer self.alloc.free(body);
        return try self.requestJson(GraphTraverseResponse, .POST, base_uri, path, body);
    }

    fn graphShortestPath(
        self: *ServerlessHttpClient,
        base_uri: []const u8,
        namespace: []const u8,
        req: serverless.GraphShortestPathRequest,
    ) !std.json.Parsed(GraphShortestPathResponse) {
        const path = try std.fmt.allocPrint(self.alloc, "/internal/v1/namespaces/{s}/query/graph/shortest-path", .{namespace});
        defer self.alloc.free(path);
        const body = try std.json.Stringify.valueAlloc(self.alloc, req, .{});
        defer self.alloc.free(body);
        return try self.requestJson(GraphShortestPathResponse, .POST, base_uri, path, body);
    }

    fn graphShortestPathTable(
        self: *ServerlessHttpClient,
        base_uri: []const u8,
        table_name: []const u8,
        req: serverless.GraphShortestPathRequest,
    ) !std.json.Parsed(TableGraphShortestPathResponse) {
        const path = try std.fmt.allocPrint(self.alloc, "/tables/{s}/query/graph/shortest-path", .{table_name});
        defer self.alloc.free(path);
        const body = try std.json.Stringify.valueAlloc(self.alloc, req, .{});
        defer self.alloc.free(body);
        return try self.requestJson(TableGraphShortestPathResponse, .POST, base_uri, path, body);
    }

    fn graphShortestPathVersion(
        self: *ServerlessHttpClient,
        base_uri: []const u8,
        namespace: []const u8,
        version: u64,
        req: serverless.GraphShortestPathRequest,
    ) !std.json.Parsed(GraphShortestPathResponse) {
        const path = try std.fmt.allocPrint(self.alloc, "/internal/v1/namespaces/{s}/query/versions/{d}/graph/shortest-path", .{ namespace, version });
        defer self.alloc.free(path);
        const body = try std.json.Stringify.valueAlloc(self.alloc, req, .{});
        defer self.alloc.free(body);
        return try self.requestJson(GraphShortestPathResponse, .POST, base_uri, path, body);
    }

    fn queryLatest(self: *ServerlessHttpClient, base_uri: []const u8, namespace: []const u8) !std.json.Parsed(QueryHeadResponse) {
        const path = try std.fmt.allocPrint(self.alloc, "/internal/v1/namespaces/{s}/query/latest", .{namespace});
        defer self.alloc.free(path);
        return try self.requestJson(QueryHeadResponse, .GET, base_uri, path, null);
    }

    fn queryVersion(
        self: *ServerlessHttpClient,
        base_uri: []const u8,
        namespace: []const u8,
        version: u64,
    ) !std.json.Parsed(QueryHeadResponse) {
        const path = try std.fmt.allocPrint(self.alloc, "/internal/v1/namespaces/{s}/query/versions/{d}", .{ namespace, version });
        defer self.alloc.free(path);
        return try self.requestJson(QueryHeadResponse, .GET, base_uri, path, null);
    }

    fn queryHeadArtifact(
        self: *ServerlessHttpClient,
        base_uri: []const u8,
        namespace: []const u8,
        artifact_index: usize,
    ) !std.json.Parsed(SingleArtifactResponse) {
        const path = try std.fmt.allocPrint(self.alloc, "/internal/v1/namespaces/{s}/query/head/artifacts/{d}", .{ namespace, artifact_index });
        defer self.alloc.free(path);
        return try self.requestJson(SingleArtifactResponse, .GET, base_uri, path, null);
    }

    fn queryVersionArtifact(
        self: *ServerlessHttpClient,
        base_uri: []const u8,
        namespace: []const u8,
        version: u64,
        artifact_index: usize,
    ) !std.json.Parsed(SingleArtifactResponse) {
        const path = try std.fmt.allocPrint(
            self.alloc,
            "/internal/v1/namespaces/{s}/query/versions/{d}/artifacts/{d}",
            .{ namespace, version, artifact_index },
        );
        defer self.alloc.free(path);
        return try self.requestJson(SingleArtifactResponse, .GET, base_uri, path, null);
    }

    fn requestJson(
        self: *ServerlessHttpClient,
        comptime T: type,
        method: http_common.Method,
        base_uri: []const u8,
        path: []const u8,
        body: ?[]const u8,
    ) !std.json.Parsed(T) {
        const uri = try join(self.alloc, base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executor.execute(self.alloc, .{
            .method = method,
            .uri = uri,
            .content_type = if (body != null) "application/json" else null,
            .body = body orelse "",
        });
        defer resp.deinit(self.alloc);
        if (resp.status < 200 or resp.status >= 300) return error.UnexpectedHttpStatus;
        return try std.json.parseFromSlice(T, self.alloc, resp.body, .{ .allocate = .alloc_always });
    }

    fn requestJsonValue(
        self: *ServerlessHttpClient,
        comptime T: type,
        method: http_common.Method,
        base_uri: []const u8,
        path: []const u8,
        body: ?[]const u8,
    ) !T {
        const uri = try join(self.alloc, base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executor.execute(self.alloc, .{
            .method = method,
            .uri = uri,
            .content_type = if (body != null) "application/json" else null,
            .body = body orelse "",
        });
        defer resp.deinit(self.alloc);
        if (resp.status < 200 or resp.status >= 300) return error.UnexpectedHttpStatus;
        return try std.json.parseFromSliceLeaky(T, self.alloc, resp.body, .{ .allocate = .alloc_always });
    }

    fn requestJsonValueAllowConflict(
        self: *ServerlessHttpClient,
        comptime T: type,
        method: http_common.Method,
        base_uri: []const u8,
        path: []const u8,
        body: ?[]const u8,
    ) !T {
        const uri = try join(self.alloc, base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executor.execute(self.alloc, .{
            .method = method,
            .uri = uri,
            .content_type = if (body != null) "application/json" else null,
            .body = body orelse "",
        });
        defer resp.deinit(self.alloc);
        if (resp.status != 200 and resp.status != 409) return error.UnexpectedHttpStatus;
        return try std.json.parseFromSliceLeaky(T, self.alloc, resp.body, .{ .allocate = .alloc_always });
    }
};

fn join(alloc: std.mem.Allocator, base_uri: []const u8, path: []const u8) ![]u8 {
    return try transport_routes.Routes.join(alloc, base_uri, path);
}

fn namespacePath(alloc: std.mem.Allocator, namespace: []const u8) ![]u8 {
    return try std.fmt.allocPrint(alloc, "/internal/v1/namespaces/{s}", .{namespace});
}

fn tablePath(alloc: std.mem.Allocator, table_name: []const u8) ![]u8 {
    return try std.fmt.allocPrint(alloc, "/tables/{s}", .{table_name});
}

fn mutationKindString(kind: serverless.MutationKind) []const u8 {
    return switch (kind) {
        .upsert => "upsert",
        .delete => "delete",
    };
}

test "serverless http client round-trips serverless http server" {
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

    var fs_artifacts = try @import("serverless/artifacts/mod.zig").FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try @import("serverless/manifest/mod.zig").FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try @import("serverless/catalog/fs_progress_store.zig").FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try @import("serverless/wal/mod.zig").FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_catalog = try @import("serverless/catalog/fs_store.zig").FsStore.init(alloc, std.mem.span(catalog_root));
    var catalog_store = fs_catalog.catalogStore();
    defer catalog_store.deinit();

    var builder = @import("serverless/build/mod.zig").Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var api = @import("serverless/api/service.zig").Service.init(alloc, &wal_store, &builder);
    var catalog = @import("serverless/catalog/mod.zig").CatalogService.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store, &builder, &catalog_store);
    defer catalog.deinit();
    var query = @import("serverless/query/mod.zig").QueryRuntime.init(alloc, &artifact_store, &manifest_store, &progress_store);
    defer query.deinit();
    var runtime_status = serverless.RuntimeStatusResult{
        .role = .combined,
        .tick_interval_ms = 1,
        .validated = true,
        .published_search_sources = try serverless.search_sources.clonePublishedSearchSourcesAlloc(
            alloc,
            serverless.search_sources.defaultPublishedSearchSources(),
        ),
        .targets = try alloc.alloc(serverless.RuntimeStorageTarget, 0),
    };
    defer runtime_status.deinit(alloc);
    var handler = @import("serverless/api/mod.zig").HttpHandler.init(alloc, &api, &catalog, &manifest_store, &progress_store, &query, &runtime_status);
    var server = @import("serverless_http_server.zig").ServerlessHttpServer.init(alloc, .{}, &handler);
    var client = ServerlessHttpClient.init(alloc, server.executor());
    const internal = client.internal();

    var status = try client.status("");
    defer status.deinit();
    try std.testing.expect(status.value.validated);
    try std.testing.expectEqual(@as(u64, 1), status.value.tick_interval_ms);

    var health = try client.health("");
    defer health.deinit();
    try std.testing.expect(health.value.live);
    try std.testing.expect(health.value.ready);
    try std.testing.expectEqual(@as(usize, 0), health.value.namespace_count);

    var metrics = try client.metrics("");
    defer metrics.deinit();
    try std.testing.expect(metrics.value.live);
    try std.testing.expectEqual(@as(usize, 0), metrics.value.namespace_count);
    try std.testing.expectEqual(@as(u64, 0), metrics.value.total_pending_records);

    var ensured = try internal.ensureNamespaceWithPolicy("", "docs", .{
        .created_at_ns = 100,
        .policy = .{
            .default_query_view = .published,
            .keep_latest_versions = 4,
        },
    });
    defer ensured.deinit(alloc);
    try std.testing.expect(ensured.created);
    try std.testing.expectEqual(@as(usize, 4), ensured.policy.keep_latest_versions);

    var namespaces = try internal.listNamespaces("");
    defer namespaces.deinit();
    try std.testing.expectEqual(@as(usize, 1), namespaces.value.len);
    try std.testing.expectEqualStrings("docs", namespaces.value[0].name);
    try std.testing.expectEqual(@as(usize, 4), namespaces.value[0].policy.keep_latest_versions);

    var policy = try internal.fetchPolicy("", "docs");
    defer policy.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 4), policy.policy.keep_latest_versions);

    const mutations = [_]serverless.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "alpha" },
        .{ .kind = .delete, .doc_id = "doc-b", .body = null },
    };
    var ingest = try internal.ingest("", .{
        .namespace = "docs",
        .timestamp_ns = 200,
        .mutations = &mutations,
    });
    defer ingest.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), ingest.mutation_count);

    var before = try internal.buildStatus("", "docs");
    defer before.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 2), before.pending_records);
    try std.testing.expectEqualStrings(serverless.search_sources.default_chunk_embedding_index_name, before.published_search_sources.findVector().?.index_name);
    try std.testing.expectEqual(serverless.search_sources.VectorDocumentSource.chunk_embeddings_or_top_level, before.published_search_sources.findVector().?.document_source);
    try std.testing.expectEqualStrings(serverless.search_sources.default_sparse_embedding_index_name, before.published_search_sources.findSparse().?.index_name);
    try std.testing.expectEqual(serverless.search_sources.SparseDocumentSource.sparse_embedding, before.published_search_sources.findSparse().?.document_source);
    try std.testing.expect(before.materialized_search_sources.findVector() == null);
    try std.testing.expect(before.materialized_search_sources.findSparse() == null);
    try std.testing.expect(!before.materialized_derived_outputs.containsKind(.chunk_preview));
    try std.testing.expect(!before.materialized_derived_outputs.containsKind(.rerank_terms));

    var build = try internal.build("", "docs");
    defer build.deinit(alloc);
    try std.testing.expect(build.published);

    var materialized_query_head = try internal.queryHead("", "docs");
    defer materialized_query_head.deinit();
    try std.testing.expect(materialized_query_head.value.materialized_search_sources.findVector() == null);
    try std.testing.expect(materialized_query_head.value.materialized_search_sources.findSparse() == null);
    try std.testing.expect(!materialized_query_head.value.materialized_derived_outputs.containsKind(.chunk_preview));
    try std.testing.expect(!materialized_query_head.value.materialized_derived_outputs.containsKind(.rerank_terms));

    var head = try internal.fetchHead("", "docs");
    defer head.deinit();
    try std.testing.expectEqual(@as(u64, 1), head.value.version);
    try std.testing.expectEqual(@as(u64, 2), head.value.wal_end_lsn);

    var query_head = try internal.queryHead("", "docs");
    defer query_head.deinit();
    try std.testing.expectEqual(@as(usize, 3), query_head.value.artifacts.len);
    try std.testing.expectEqual(serverless.QueryView.published, query_head.value.view);
    try std.testing.expectEqual(@as(usize, 1), query_head.value.documents.len);
    try std.testing.expectEqualStrings("doc-a", query_head.value.documents[0].doc_id);
    try std.testing.expect(query_head.value.artifacts[0].byte_len > 0);
    try std.testing.expect(std.mem.startsWith(u8, query_head.value.artifacts[0].artifact_id, "sha256:"));
    try std.testing.expectEqual(@as(usize, 64), query_head.value.artifacts[0].checksum.len);
    try std.testing.expectEqual(serverless.ArtifactKind.document_segment, query_head.value.artifacts[1].kind);
    try std.testing.expect(!query_head.value.artifacts[1].materialized_derived_outputs.containsKind(.chunk_preview));
    try std.testing.expectEqual(serverless.ArtifactKind.text_segment, query_head.value.artifacts[2].kind);
    try std.testing.expect(!query_head.value.artifacts[2].materialized_derived_outputs.containsKind(.chunk_preview));

    var search = try internal.search("", "docs", .{
        .text = @constCast("alpha"),
        .limit = 5,
    });
    defer search.deinit();
    try std.testing.expectEqual(@as(u64, 1), search.value.version);
    try std.testing.expectEqual(@as(usize, 1), search.value.hits.len);
    try std.testing.expectEqualStrings("doc-a", search.value.hits[0].doc_id);
    try std.testing.expectEqualStrings("alpha", search.value.hits[0].body);

    var query_version = try internal.queryVersion("", "docs", 1);
    defer query_version.deinit();
    try std.testing.expectEqual(@as(u64, 1), query_version.value.version);

    var query_artifact = try internal.queryHeadArtifact("", "docs", 0);
    defer query_artifact.deinit();
    try std.testing.expectEqual(@as(usize, 0), query_artifact.value.artifact.index);
    try std.testing.expectEqual(@as(usize, 2), query_artifact.value.artifact.mutations.len);
    try std.testing.expectEqualStrings("doc-a", query_artifact.value.artifact.mutations[0].doc_id);
    try std.testing.expectEqualStrings("alpha", query_artifact.value.artifact.mutations[0].body.?);
    try std.testing.expect(std.mem.startsWith(u8, query_artifact.value.artifact.artifact_id, "sha256:"));
    try std.testing.expectEqual(@as(usize, 64), query_artifact.value.artifact.checksum.len);
    try std.testing.expect(!query_artifact.value.artifact.materialized_derived_outputs.containsKind(.chunk_preview));

    var document_artifact = try internal.queryHeadArtifact("", "docs", 1);
    defer document_artifact.deinit();
    try std.testing.expectEqual(serverless.ArtifactKind.document_segment, document_artifact.value.artifact.kind);
    try std.testing.expectEqual(@as(usize, 0), document_artifact.value.artifact.mutations.len);
    try std.testing.expectEqual(@as(usize, 1), document_artifact.value.artifact.documents.len);
    try std.testing.expectEqualStrings("doc-a", document_artifact.value.artifact.documents[0].doc_id);
    try std.testing.expectEqualStrings("alpha", document_artifact.value.artifact.documents[0].body);

    var version_artifact = try internal.queryVersionArtifact("", "docs", 1, 0);
    defer version_artifact.deinit();
    try std.testing.expectEqual(@as(u64, 1), version_artifact.value.version);

    const next_mutations = [_]serverless.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-c", .body = "gamma" },
    };
    var next_ingest = try internal.ingest("", .{
        .namespace = "docs",
        .timestamp_ns = 250,
        .mutations = &next_mutations,
    });
    defer next_ingest.deinit(alloc);

    var latest = try internal.queryLatest("", "docs");
    defer latest.deinit();
    try std.testing.expectEqual(serverless.QueryView.latest, latest.value.view);
    try std.testing.expectEqual(@as(usize, 2), latest.value.documents.len);
    try std.testing.expectEqual(@as(usize, 1), latest.value.overlay_mutation_count);

    var updated_policy = try internal.updatePolicy("", "docs", .{
        .default_query_view = .latest,
        .keep_latest_versions = 2,
    });
    defer updated_policy.deinit(alloc);
    try std.testing.expectEqual(serverless.DefaultQueryView.latest, updated_policy.policy.default_query_view);

    var default_query = try internal.query("", "docs");
    defer default_query.deinit();
    try std.testing.expectEqual(serverless.QueryView.latest, default_query.value.view);
    try std.testing.expectEqual(@as(usize, 2), default_query.value.documents.len);

    var publish_head = try internal.publishHead("", "docs", .{
        .version = 1,
        .expected_head = null,
    });
    defer publish_head.deinit(alloc);
    try std.testing.expect(!publish_head.published);
    try std.testing.expectEqual(@as(?u64, 1), publish_head.current_head);
}

test "serverless http client round-trips the table public API routes" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    var catalog_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-table");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-table");
    const wal_root = tmpPath(&wal_root_buf, "wal-table");
    const catalog_root = tmpPath(&catalog_root_buf, "catalog-table");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);
    defer cleanupTmp(catalog_root);

    var fs_artifacts = try @import("serverless/artifacts/mod.zig").FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try @import("serverless/manifest/mod.zig").FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try @import("serverless/catalog/fs_progress_store.zig").FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try @import("serverless/wal/mod.zig").FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_catalog = try @import("serverless/catalog/fs_store.zig").FsStore.init(alloc, std.mem.span(catalog_root));
    var catalog_store = fs_catalog.catalogStore();
    defer catalog_store.deinit();

    var builder = @import("serverless/build/mod.zig").Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var api = @import("serverless/api/service.zig").Service.init(alloc, &wal_store, &builder);
    var catalog = @import("serverless/catalog/mod.zig").CatalogService.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store, &builder, &catalog_store);
    defer catalog.deinit();
    var query = @import("serverless/query/mod.zig").QueryRuntime.init(alloc, &artifact_store, &manifest_store, &progress_store);
    defer query.deinit();
    var runtime_status = serverless.RuntimeStatusResult{
        .role = .combined,
        .tick_interval_ms = 1,
        .validated = true,
        .published_search_sources = try serverless.search_sources.clonePublishedSearchSourcesAlloc(
            alloc,
            serverless.search_sources.defaultPublishedSearchSources(),
        ),
        .targets = try alloc.alloc(serverless.RuntimeStorageTarget, 0),
    };
    defer runtime_status.deinit(alloc);
    var handler = @import("serverless/api/mod.zig").HttpHandler.init(alloc, &api, &catalog, &manifest_store, &progress_store, &query, &runtime_status);
    var server = @import("serverless_http_server.zig").ServerlessHttpServer.init(alloc, .{}, &handler);
    var client = ServerlessHttpClient.init(alloc, server.executor());
    const tables_api = client.tables();
    const internal = client.internal();

    var ensured = try tables_api.ensureWithPolicy("", "docs", .{
        .created_at_ns = 100,
        .policy = .{
            .default_query_view = .latest,
            .keep_latest_versions = 3,
        },
    });
    defer ensured.deinit(alloc);
    try std.testing.expect(ensured.created);
    try std.testing.expectEqualStrings("docs", ensured.table_name);

    var tables = try tables_api.list("");
    defer tables.deinit();
    try std.testing.expectEqual(@as(usize, 1), tables.value.len);
    try std.testing.expectEqualStrings("docs", tables.value[0].table_name);

    var table_policy = try internal.fetchTablePolicy("", "docs");
    defer table_policy.deinit(alloc);
    try std.testing.expectEqualStrings("docs", table_policy.table_name);
    try std.testing.expectEqual(serverless.DefaultQueryView.latest, table_policy.policy.default_query_view);

    const mutations = [_]serverless.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "alpha" },
    };
    var ingest = try tables_api.ingest("", .{
        .table_name = "docs",
        .timestamp_ns = 200,
        .mutations = &mutations,
    });
    defer ingest.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), ingest.mutation_count);

    var build_status = try internal.buildTableStatus("", "docs");
    defer build_status.deinit(alloc);
    try std.testing.expectEqualStrings("docs", build_status.table_name);
    try std.testing.expectEqual(@as(u64, 1), build_status.pending_records);

    var build = try internal.buildTable("", "docs");
    defer build.deinit(alloc);
    try std.testing.expectEqualStrings("docs", build.table_name);
    try std.testing.expect(build.published);

    var query_published = try tables_api.queryPublished("", "docs");
    defer query_published.deinit();
    try std.testing.expectEqualStrings("docs", query_published.value.table_name);
    try std.testing.expectEqual(serverless.QueryView.published, query_published.value.view);
    try std.testing.expectEqual(@as(usize, 1), query_published.value.documents.len);

    var search = try tables_api.search("", "docs", .{
        .text = @constCast("alpha"),
        .limit = 5,
    });
    defer search.deinit();
    try std.testing.expectEqualStrings("docs", search.value.table_name);
    try std.testing.expectEqual(@as(usize, 1), search.value.hits.len);

    const next_mutations = [_]serverless.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-b", .body = "beta" },
    };
    var next_ingest = try tables_api.ingest("", .{
        .table_name = "docs",
        .timestamp_ns = 250,
        .mutations = &next_mutations,
    });
    defer next_ingest.deinit(alloc);

    var query_default = try tables_api.query("", "docs");
    defer query_default.deinit();
    try std.testing.expectEqualStrings("docs", query_default.value.table_name);
    try std.testing.expectEqual(serverless.QueryView.latest, query_default.value.view);
    try std.testing.expectEqual(@as(usize, 2), query_default.value.documents.len);

    var latest = try tables_api.queryLatest("", "docs");
    defer latest.deinit();
    try std.testing.expectEqualStrings("docs", latest.value.table_name);
    try std.testing.expectEqual(serverless.QueryView.latest, latest.value.view);
    try std.testing.expectEqual(@as(usize, 1), latest.value.overlay_mutation_count);
}

test "serverless http client round-trips graph query endpoints" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    var catalog_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-graph");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-graph");
    const wal_root = tmpPath(&wal_root_buf, "wal-graph");
    const catalog_root = tmpPath(&catalog_root_buf, "catalog-graph");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);
    defer cleanupTmp(catalog_root);

    var fs_artifacts = try @import("serverless/artifacts/mod.zig").FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try @import("serverless/manifest/mod.zig").FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try @import("serverless/catalog/fs_progress_store.zig").FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try @import("serverless/wal/mod.zig").FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_catalog = try @import("serverless/catalog/fs_store.zig").FsStore.init(alloc, std.mem.span(catalog_root));
    var catalog_store = fs_catalog.catalogStore();
    defer catalog_store.deinit();

    var builder = @import("serverless/build/mod.zig").Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var api = @import("serverless/api/service.zig").Service.init(alloc, &wal_store, &builder);
    var catalog = @import("serverless/catalog/mod.zig").CatalogService.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store, &builder, &catalog_store);
    defer catalog.deinit();
    var query = @import("serverless/query/mod.zig").QueryRuntime.init(alloc, &artifact_store, &manifest_store, &progress_store);
    defer query.deinit();
    var runtime_status = serverless.RuntimeStatusResult{
        .role = .combined,
        .tick_interval_ms = 1,
        .validated = true,
        .published_search_sources = try serverless.search_sources.clonePublishedSearchSourcesAlloc(
            alloc,
            serverless.search_sources.defaultPublishedSearchSources(),
        ),
        .targets = try alloc.alloc(serverless.RuntimeStorageTarget, 0),
    };
    defer runtime_status.deinit(alloc);
    var handler = @import("serverless/api/mod.zig").HttpHandler.init(alloc, &api, &catalog, &manifest_store, &progress_store, &query, &runtime_status);
    var server = @import("serverless_http_server.zig").ServerlessHttpServer.init(alloc, .{}, &handler);
    var client = ServerlessHttpClient.init(alloc, server.executor());
    const internal = client.internal();
    const tables_api = client.tables();

    var ensured = try internal.ensureNamespace("", "docs", 100);
    defer ensured.deinit(alloc);
    try std.testing.expect(ensured.created);
    var ensured_table = try tables_api.ensure("", "docs", 100);
    defer ensured_table.deinit(alloc);
    try std.testing.expect(ensured_table.created);

    const mutations = [_]serverless.DocumentMutation{
        .{
            .kind = .upsert,
            .doc_id = "doc-a",
            .body =
            \\{"text":"alpha","graph_edges":[{"target":"doc-b","edge_type":"cites","weight":1.5},{"target":"doc-c","edge_type":"related","weight":0.5}]}
            ,
        },
        .{
            .kind = .upsert,
            .doc_id = "doc-b",
            .body =
            \\{"text":"beta","graph_edges":[{"target":"doc-c","edge_type":"cites","weight":2.0}]}
            ,
        },
        .{
            .kind = .upsert,
            .doc_id = "doc-c",
            .body =
            \\{"text":"gamma"}
            ,
        },
    };
    var ingest = try internal.ingest("", .{
        .namespace = "docs",
        .timestamp_ns = 200,
        .mutations = &mutations,
    });
    defer ingest.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 3), ingest.mutation_count);

    var build = try internal.build("", "docs");
    defer build.deinit(alloc);
    try std.testing.expect(build.published);

    var neighbors = try internal.graphNeighbors("", "docs", .{
        .doc_id = @constCast("doc-a"),
        .direction = .out,
        .limit = 10,
    });
    defer neighbors.deinit();
    try std.testing.expectEqual(@as(u64, 1), neighbors.value.version);
    try std.testing.expectEqual(@as(usize, 2), neighbors.value.neighbors.len);
    try std.testing.expectEqualStrings("doc-a", neighbors.value.node_id);
    try std.testing.expectEqualStrings("doc-b", neighbors.value.neighbors[0].doc_id);
    try std.testing.expectEqualStrings("cites", neighbors.value.neighbors[0].edge_type);
    try std.testing.expectEqualStrings("doc-c", neighbors.value.neighbors[1].doc_id);
    try std.testing.expectEqualStrings("related", neighbors.value.neighbors[1].edge_type);

    var version_neighbors = try internal.graphNeighborsVersion("", "docs", 1, .{
        .doc_id = @constCast("doc-a"),
        .direction = .out,
        .edge_types = &.{@constCast("cites")},
        .limit = 10,
    });
    defer version_neighbors.deinit();
    try std.testing.expectEqual(@as(u64, 1), version_neighbors.value.version);
    try std.testing.expectEqual(@as(usize, 1), version_neighbors.value.neighbors.len);
    try std.testing.expectEqualStrings("doc-b", version_neighbors.value.neighbors[0].doc_id);

    var table_neighbors = try tables_api.graphNeighbors("", "docs", .{
        .doc_id = @constCast("doc-a"),
        .direction = .out,
        .limit = 10,
    });
    defer table_neighbors.deinit();
    try std.testing.expectEqual(@as(u64, 1), table_neighbors.value.version);
    try std.testing.expectEqualStrings("docs", table_neighbors.value.table_name);
    try std.testing.expectEqual(@as(usize, 2), table_neighbors.value.neighbors.len);

    var traverse = try internal.graphTraverse("", "docs", .{
        .start_doc_id = @constCast("doc-a"),
        .direction = .out,
        .edge_types = &.{@constCast("cites")},
        .max_depth = 2,
        .limit = 10,
        .include_start = true,
    });
    defer traverse.deinit();
    try std.testing.expectEqual(@as(usize, 3), traverse.value.nodes.len);
    try std.testing.expectEqualStrings("doc-a", traverse.value.nodes[0].doc_id);
    try std.testing.expectEqualStrings("doc-b", traverse.value.nodes[1].doc_id);
    try std.testing.expectEqualStrings("doc-a", traverse.value.nodes[1].parent_doc_id.?);
    try std.testing.expectEqualStrings("doc-c", traverse.value.nodes[2].doc_id);
    try std.testing.expectEqualStrings("doc-b", traverse.value.nodes[2].parent_doc_id.?);
    try std.testing.expectEqual(@as(usize, 3), traverse.value.nodes[2].path.?.len);
    try std.testing.expectEqualStrings("doc-a", traverse.value.nodes[2].path.?[0]);
    try std.testing.expectEqualStrings("doc-b", traverse.value.nodes[2].path.?[1]);
    try std.testing.expectEqualStrings("doc-c", traverse.value.nodes[2].path.?[2]);

    var version_traverse = try internal.graphTraverseVersion("", "docs", 1, .{
        .start_doc_id = @constCast("doc-a"),
        .direction = .out,
        .edge_types = &.{@constCast("cites")},
        .max_depth = 2,
        .limit = 10,
        .include_start = true,
    });
    defer version_traverse.deinit();
    try std.testing.expectEqual(@as(u64, 1), version_traverse.value.version);
    try std.testing.expectEqual(@as(usize, 3), version_traverse.value.nodes.len);
    try std.testing.expectEqualStrings("doc-c", version_traverse.value.nodes[2].doc_id);
    try std.testing.expectEqualStrings("doc-b", version_traverse.value.nodes[2].parent_doc_id.?);

    var table_traverse = try tables_api.graphTraverse("", "docs", .{
        .start_doc_id = @constCast("doc-a"),
        .direction = .out,
        .edge_types = &.{@constCast("cites")},
        .max_depth = 2,
        .limit = 10,
        .include_start = true,
    });
    defer table_traverse.deinit();
    try std.testing.expectEqualStrings("docs", table_traverse.value.table_name);
    try std.testing.expectEqual(@as(usize, 3), table_traverse.value.nodes.len);

    var shortest = try internal.graphShortestPath("", "docs", .{
        .start_doc_id = @constCast("doc-a"),
        .end_doc_id = @constCast("doc-c"),
        .direction = .out,
        .edge_types = &.{@constCast("cites")},
        .max_depth = 4,
    });
    defer shortest.deinit();
    try std.testing.expect(shortest.value.found);
    try std.testing.expectEqual(@as(?u32, 2), shortest.value.depth);
    try std.testing.expectEqual(@as(usize, 3), shortest.value.node_path.?.len);
    try std.testing.expectEqualStrings("doc-a", shortest.value.node_path.?[0]);
    try std.testing.expectEqualStrings("doc-b", shortest.value.node_path.?[1]);
    try std.testing.expectEqualStrings("doc-c", shortest.value.node_path.?[2]);
    try std.testing.expectEqual(@as(usize, 2), shortest.value.edge_path.?.len);
    try std.testing.expectEqualStrings("doc-a", shortest.value.edge_path.?[0].from_doc_id);
    try std.testing.expectEqualStrings("doc-c", shortest.value.edge_path.?[1].to_doc_id);

    var version_shortest = try internal.graphShortestPathVersion("", "docs", 1, .{
        .start_doc_id = @constCast("doc-a"),
        .end_doc_id = @constCast("doc-c"),
        .direction = .out,
        .edge_types = &.{@constCast("cites")},
        .max_depth = 4,
    });
    defer version_shortest.deinit();
    try std.testing.expectEqual(@as(u64, 1), version_shortest.value.version);
    try std.testing.expect(version_shortest.value.found);
    try std.testing.expectEqual(@as(?u32, 2), version_shortest.value.depth);
    try std.testing.expectEqual(@as(usize, 3), version_shortest.value.node_path.?.len);
    try std.testing.expectEqualStrings("doc-b", version_shortest.value.node_path.?[1]);

    var table_shortest = try tables_api.graphShortestPath("", "docs", .{
        .start_doc_id = @constCast("doc-a"),
        .end_doc_id = @constCast("doc-c"),
        .direction = .out,
        .edge_types = &.{@constCast("cites")},
        .max_depth = 4,
    });
    defer table_shortest.deinit();
    try std.testing.expectEqualStrings("docs", table_shortest.value.table_name);
    try std.testing.expect(table_shortest.value.found);
    try std.testing.expectEqual(@as(?u32, 2), table_shortest.value.depth);

    try std.testing.expectError(error.UnexpectedHttpStatus, internal.graphNeighbors("", "docs", .{
        .doc_id = @constCast("   "),
    }));
    try std.testing.expectError(error.UnexpectedHttpStatus, internal.graphNeighborsVersion("", "docs", 1, .{
        .doc_id = @constCast("   "),
    }));
    try std.testing.expectError(error.UnexpectedHttpStatus, tables_api.graphNeighbors("", "docs", .{
        .doc_id = @constCast("   "),
    }));
    try std.testing.expectError(error.UnexpectedHttpStatus, internal.graphTraverse("", "docs", .{
        .start_doc_id = @constCast(""),
    }));
    try std.testing.expectError(error.UnexpectedHttpStatus, internal.graphTraverseVersion("", "docs", 1, .{
        .start_doc_id = @constCast(""),
    }));
    try std.testing.expectError(error.UnexpectedHttpStatus, tables_api.graphTraverse("", "docs", .{
        .start_doc_id = @constCast(""),
    }));
    try std.testing.expectError(error.UnexpectedHttpStatus, internal.graphShortestPath("", "docs", .{
        .start_doc_id = @constCast("doc-a"),
        .end_doc_id = @constCast(""),
    }));
    try std.testing.expectError(error.UnexpectedHttpStatus, internal.graphShortestPathVersion("", "docs", 1, .{
        .start_doc_id = @constCast("doc-a"),
        .end_doc_id = @constCast(""),
    }));
    try std.testing.expectError(error.UnexpectedHttpStatus, tables_api.graphShortestPath("", "docs", .{
        .start_doc_id = @constCast("doc-a"),
        .end_doc_id = @constCast(""),
    }));
}

test "serverless http client round-trips over std http listener" {
    const alloc = std.testing.allocator;
    const std_http_listener = @import("raft/transport/std_http_listener.zig");
    const std_http_executor = @import("raft/transport/std_http_executor.zig");

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    var catalog_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-listener");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-listener");
    const wal_root = tmpPath(&wal_root_buf, "wal-listener");
    const catalog_root = tmpPath(&catalog_root_buf, "catalog-listener");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);
    defer cleanupTmp(catalog_root);

    var fs_artifacts = try @import("serverless/artifacts/mod.zig").FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try @import("serverless/manifest/mod.zig").FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try @import("serverless/catalog/fs_progress_store.zig").FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try @import("serverless/wal/mod.zig").FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_catalog = try @import("serverless/catalog/fs_store.zig").FsStore.init(alloc, std.mem.span(catalog_root));
    var catalog_store = fs_catalog.catalogStore();
    defer catalog_store.deinit();

    var builder = @import("serverless/build/mod.zig").Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var api = @import("serverless/api/service.zig").Service.init(alloc, &wal_store, &builder);
    var catalog = @import("serverless/catalog/mod.zig").CatalogService.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store, &builder, &catalog_store);
    defer catalog.deinit();
    var query = @import("serverless/query/mod.zig").QueryRuntime.init(alloc, &artifact_store, &manifest_store, &progress_store);
    defer query.deinit();
    var runtime_status = serverless.RuntimeStatusResult{
        .role = .combined,
        .tick_interval_ms = 1,
        .validated = true,
        .published_search_sources = try serverless.search_sources.clonePublishedSearchSourcesAlloc(
            alloc,
            serverless.search_sources.defaultPublishedSearchSources(),
        ),
        .targets = try alloc.alloc(serverless.RuntimeStorageTarget, 0),
    };
    defer runtime_status.deinit(alloc);
    var handler = @import("serverless/api/mod.zig").HttpHandler.init(alloc, &api, &catalog, &manifest_store, &progress_store, &query, &runtime_status);
    var server = @import("serverless_http_server.zig").ServerlessHttpServer.init(alloc, .{}, &handler);
    var listener = std_http_listener.StdHttpListener.init(alloc, .{}, server.executor());
    defer listener.deinit();
    try listener.start();

    const base_uri = try listener.baseUri(alloc);
    defer alloc.free(base_uri);

    var executor = std_http_executor.StdHttpExecutor.init(alloc, .{});
    defer executor.deinit();
    var client = ServerlessHttpClient.init(alloc, executor.executor());
    const internal = client.internal();

    var status = try client.status(base_uri);
    defer status.deinit();
    try std.testing.expect(status.value.validated);

    var health = try client.health(base_uri);
    defer health.deinit();
    try std.testing.expect(health.value.live);
    try std.testing.expect(health.value.ready);

    var metrics = try client.metrics(base_uri);
    defer metrics.deinit();
    try std.testing.expect(metrics.value.live);
    try std.testing.expectEqual(@as(usize, 0), metrics.value.namespace_count);

    var ensured = try internal.ensureNamespace(base_uri, "docs", 300);
    defer ensured.deinit(alloc);
    try std.testing.expect(ensured.created);

    const mutations = [_]serverless.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "alpha" },
    };
    var ingest = try internal.ingest(base_uri, .{
        .namespace = "docs",
        .timestamp_ns = 400,
        .mutations = &mutations,
    });
    defer ingest.deinit(alloc);

    var build = try internal.build(base_uri, "docs");
    defer build.deinit(alloc);
    try std.testing.expect(build.published);

    var query_head = try internal.queryHead(base_uri, "docs");
    defer query_head.deinit();
    try std.testing.expectEqual(@as(usize, 3), query_head.value.artifacts.len);
    try std.testing.expectEqual(serverless.QueryView.published, query_head.value.view);
    try std.testing.expectEqual(@as(usize, 1), query_head.value.documents.len);
    try std.testing.expect(query_head.value.artifacts[0].byte_len > 0);

    var search = try internal.search(base_uri, "docs", .{
        .text = @constCast("alpha"),
        .limit = 5,
    });
    defer search.deinit();
    try std.testing.expectEqual(@as(usize, 1), search.value.hits.len);
    try std.testing.expectEqualStrings("doc-a", search.value.hits[0].doc_id);
}

test "serverless http client round-trips semantic search with embedding_template" {
    const alloc = std.testing.allocator;

    const FakeEmbeddingProvider = struct {
        fn executor() http_common.RequestExecutor {
            return .{ .ptr = undefined, .vtable = &.{ .execute = execute } };
        }

        fn execute(_: *anyopaque, req_alloc: std.mem.Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
            try std.testing.expectEqual(http_common.Method.POST, req.method);
            const alpha = std.mem.indexOf(u8, req.body, "alpha") != null;
            const beta = std.mem.indexOf(u8, req.body, "beta") != null;
            const vector = if (alpha and !beta)
                "[1,0,0]"
            else if (beta and !alpha)
                "[0,1,0]"
            else
                "[0.5,0.5,0]";
            const body = try std.fmt.allocPrint(
                req_alloc,
                "{{\"object\":\"list\",\"data\":[{{\"object\":\"embedding\",\"index\":0,\"embedding\":{s}}}],\"model\":\"text-embedding-3-small\",\"usage\":{{\"prompt_tokens\":1,\"total_tokens\":1}}}}",
                .{vector},
            );
            return .{
                .status = 200,
                .content_type = try req_alloc.dupe(u8, "application/json"),
                .body = body,
            };
        }
    };

    const FakeRemoteAssets = struct {
        fn executor() http_common.RequestExecutor {
            return .{ .ptr = undefined, .vtable = &.{ .execute = execute } };
        }

        fn execute(_: *anyopaque, req_alloc: std.mem.Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
            try std.testing.expectEqual(http_common.Method.GET, req.method);
            if (std.mem.endsWith(u8, req.uri, "/alpha.txt")) {
                return .{
                    .status = 200,
                    .content_type = try req_alloc.dupe(u8, "text/plain"),
                    .body = try req_alloc.dupe(u8, "alpha transcript"),
                };
            }
            if (std.mem.endsWith(u8, req.uri, "/kitten.png")) {
                return .{
                    .status = 200,
                    .content_type = try req_alloc.dupe(u8, "image/png"),
                    .body = try req_alloc.dupe(u8, "png"),
                };
            }
            return .{
                .status = 404,
                .content_type = try req_alloc.dupe(u8, "text/plain"),
                .body = try req_alloc.dupe(u8, "missing"),
            };
        }
    };

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    var catalog_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-semantic");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-semantic");
    const wal_root = tmpPath(&wal_root_buf, "wal-semantic");
    const catalog_root = tmpPath(&catalog_root_buf, "catalog-semantic");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);
    defer cleanupTmp(catalog_root);

    var embed_listener = @import("raft/transport/std_http_listener.zig").StdHttpListener.init(alloc, .{}, FakeEmbeddingProvider.executor());
    defer embed_listener.deinit();
    try embed_listener.start();
    const embed_base_uri = try embed_listener.baseUri(alloc);
    defer alloc.free(embed_base_uri);

    var remote_listener = @import("raft/transport/std_http_listener.zig").StdHttpListener.init(alloc, .{}, FakeRemoteAssets.executor());
    defer remote_listener.deinit();
    try remote_listener.start();
    const remote_base_uri = try remote_listener.baseUri(alloc);
    defer alloc.free(remote_base_uri);

    const indexes_json = try std.fmt.allocPrint(alloc,
        \\{{"serverless_chunk":{{"type":"embeddings","field":"text","dimension":3,"embedder":{{"provider":"openai","model":"text-embedding-3-small","url":"{s}"}}}}}}
    , .{embed_base_uri});
    defer alloc.free(indexes_json);
    var query_embedder = try managed_embedder.ManagedEmbedder.initFromIndexesJson(alloc, indexes_json);
    defer query_embedder.deinit();

    var fs_artifacts = try @import("serverless/artifacts/mod.zig").FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try @import("serverless/manifest/mod.zig").FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try @import("serverless/catalog/fs_progress_store.zig").FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try @import("serverless/wal/mod.zig").FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_catalog = try @import("serverless/catalog/fs_store.zig").FsStore.init(alloc, std.mem.span(catalog_root));
    var catalog_store = fs_catalog.catalogStore();
    defer catalog_store.deinit();

    var builder = @import("serverless/build/mod.zig").Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var api = @import("serverless/api/service.zig").Service.init(alloc, &wal_store, &builder);
    var catalog = @import("serverless/catalog/mod.zig").CatalogService.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store, &builder, &catalog_store);
    defer catalog.deinit();
    var query = @import("serverless/query/mod.zig").QueryRuntime.init(alloc, &artifact_store, &manifest_store, &progress_store);
    defer query.deinit();
    var runtime_status = serverless.RuntimeStatusResult{
        .role = .combined,
        .tick_interval_ms = 1,
        .validated = true,
        .published_search_sources = try serverless.search_sources.clonePublishedSearchSourcesAlloc(
            alloc,
            serverless.search_sources.defaultPublishedSearchSources(),
        ),
        .targets = try alloc.alloc(serverless.RuntimeStorageTarget, 0),
    };
    defer runtime_status.deinit(alloc);
    var handler = @import("serverless/api/mod.zig").HttpHandler.init(alloc, &api, &catalog, &manifest_store, &progress_store, &query, &runtime_status);
    handler.setPublishedSearchSources(serverless.search_sources.defaultPublishedSearchSources());
    handler.setManagedDenseQueryEmbedder(&query_embedder, "serverless_chunk");
    var server = @import("serverless_http_server.zig").ServerlessHttpServer.init(alloc, .{}, &handler);
    var client = ServerlessHttpClient.init(alloc, server.executor());
    const internal = client.internal();
    const tables_api = client.tables();

    var status = try client.status("");
    defer status.deinit();
    try std.testing.expectEqualStrings(serverless.search_sources.default_chunk_embedding_index_name, status.value.published_search_sources.findVector().?.index_name);
    try std.testing.expectEqualStrings(serverless.search_sources.default_sparse_embedding_index_name, status.value.published_search_sources.findSparse().?.index_name);

    var ensured = try internal.ensureNamespace("", "docs", 100);
    defer ensured.deinit(alloc);

    var ensured_table = try tables_api.ensure("", "docs", 100);
    defer ensured_table.deinit(alloc);
    try std.testing.expect(ensured_table.created);

    var dense_index_obj = std.json.ObjectMap.empty;
    defer dense_index_obj.deinit(alloc);
    try dense_index_obj.put(alloc, "name", .{ .string = serverless.search_sources.default_chunk_embedding_index_name });
    try dense_index_obj.put(alloc, "type", .{ .string = "embeddings" });
    try dense_index_obj.put(alloc, "external", .{ .bool = true });
    try dense_index_obj.put(alloc, "dimension", .{ .integer = 3 });
    var created_dense_index = try tables_api.createIndex(
        "",
        "docs",
        serverless.search_sources.default_chunk_embedding_index_name,
        .{ .object = dense_index_obj },
    );
    defer created_dense_index.deinit();

    var sparse_index_obj = std.json.ObjectMap.empty;
    defer sparse_index_obj.deinit(alloc);
    try sparse_index_obj.put(alloc, "name", .{ .string = serverless.search_sources.default_sparse_embedding_index_name });
    try sparse_index_obj.put(alloc, "type", .{ .string = "embeddings" });
    try sparse_index_obj.put(alloc, "external", .{ .bool = true });
    try sparse_index_obj.put(alloc, "sparse", .{ .bool = true });
    var created_sparse_index = try tables_api.createIndex(
        "",
        "docs",
        serverless.search_sources.default_sparse_embedding_index_name,
        .{ .object = sparse_index_obj },
    );
    defer created_sparse_index.deinit();

    const mutations = [_]serverless.DocumentMutation{
        .{
            .kind = .upsert,
            .doc_id = "doc-a",
            .body = "{\"text\":\"alpha bravo\",\"_embeddings\":{\"serverless_chunk\":[1,0,0],\"serverless_sparse\":{\"alpha\":1.0,\"bravo\":0.25}},\"chunk_preview\":[\"alpha bravo\"],\"rerank_terms\":[\"alpha\",\"bravo\"],\"_enrichment\":{\"chunk_preview\":true,\"chunk_preview_version\":1,\"rerank_terms\":true,\"rerank_terms_version\":1}}",
        },
        .{
            .kind = .upsert,
            .doc_id = "doc-b",
            .body = "{\"text\":\"beta charlie\",\"_embeddings\":{\"serverless_chunk\":[0,1,0],\"serverless_sparse\":{\"beta\":1.0,\"charlie\":0.25}}}",
        },
    };
    var ingest = try internal.ingest("", .{
        .namespace = "docs",
        .timestamp_ns = 200,
        .mutations = &mutations,
    });
    defer ingest.deinit(alloc);

    var build = try internal.build("", "docs");
    defer build.deinit(alloc);
    try std.testing.expect(build.published);

    var query_head = try internal.queryHead("", "docs");
    defer query_head.deinit();
    try std.testing.expectEqualStrings(serverless.search_sources.default_chunk_embedding_index_name, query_head.value.materialized_search_sources.findVector().?.index_name);
    try std.testing.expectEqualStrings(serverless.search_sources.default_sparse_embedding_index_name, query_head.value.materialized_search_sources.findSparse().?.index_name);
    try std.testing.expectEqualStrings(serverless.search_sources.default_chunk_preview_output_name, query_head.value.materialized_derived_outputs.findByKind(.chunk_preview).?.name);
    try std.testing.expectEqualStrings(serverless.search_sources.default_rerank_terms_output_name, query_head.value.materialized_derived_outputs.findByKind(.rerank_terms).?.name);
    try std.testing.expectEqual(@as(usize, 5), query_head.value.artifacts.len);
    try std.testing.expectEqual(serverless.ArtifactKind.sparse_segment, query_head.value.artifacts[3].kind);
    try std.testing.expectEqualStrings(serverless.search_sources.default_sparse_embedding_index_name, query_head.value.artifacts[3].search_sources.findSparse().?.index_name);
    try std.testing.expect(query_head.value.artifacts[3].search_sources.findVector() == null);
    try std.testing.expect(!query_head.value.artifacts[3].materialized_derived_outputs.containsKind(.chunk_preview));
    try std.testing.expectEqual(serverless.ArtifactKind.vector_segment, query_head.value.artifacts[4].kind);
    try std.testing.expectEqualStrings(serverless.search_sources.default_chunk_embedding_index_name, query_head.value.artifacts[4].search_sources.findVector().?.index_name);
    try std.testing.expect(query_head.value.artifacts[4].search_sources.findSparse() == null);
    try std.testing.expect(!query_head.value.artifacts[4].materialized_derived_outputs.containsKind(.chunk_preview));

    try std.testing.expectEqual(serverless.ArtifactKind.document_segment, query_head.value.artifacts[1].kind);
    try std.testing.expectEqualStrings(serverless.search_sources.default_chunk_preview_output_name, query_head.value.artifacts[1].materialized_derived_outputs.findByKind(.chunk_preview).?.name);
    try std.testing.expectEqualStrings(serverless.search_sources.default_rerank_terms_output_name, query_head.value.artifacts[1].materialized_derived_outputs.findByKind(.rerank_terms).?.name);

    var enriched_query_artifact = try internal.queryHeadArtifact("", "docs", 1);
    defer enriched_query_artifact.deinit();
    try std.testing.expectEqual(serverless.ArtifactKind.document_segment, enriched_query_artifact.value.artifact.kind);
    try std.testing.expectEqualStrings(serverless.search_sources.default_chunk_preview_output_name, enriched_query_artifact.value.artifact.materialized_derived_outputs.findByKind(.chunk_preview).?.name);
    try std.testing.expectEqualStrings(serverless.search_sources.default_rerank_terms_output_name, enriched_query_artifact.value.artifact.materialized_derived_outputs.findByKind(.rerank_terms).?.name);

    const query_url = try std.fmt.allocPrint(alloc, "{s}/alpha.txt", .{remote_base_uri});
    defer alloc.free(query_url);
    var search_indexes = [_][]u8{@constCast("serverless_chunk")};
    const raw_search_body = try std.json.Stringify.valueAlloc(alloc, serverless.QueryRequest{
        .text = @constCast(""),
        .semantic_search = @constCast(query_url),
        .embedding_template = @constCast("{{remoteText url=this}}"),
        .indexes = search_indexes[0..],
        .limit = 5,
    }, .{});
    defer alloc.free(raw_search_body);
    var raw_search = try server.executor().execute(alloc, .{
        .method = .POST,
        .uri = "/tables/docs/query/search",
        .content_type = "application/json",
        .body = raw_search_body,
    });
    defer raw_search.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), raw_search.status);

    var search = try tables_api.search("", "docs", .{
        .text = @constCast(""),
        .semantic_search = @constCast(query_url),
        .embedding_template = @constCast("{{remoteText url=this}}"),
        .indexes = search_indexes[0..],
        .limit = 5,
    });
    defer search.deinit();
    try std.testing.expectEqual(@as(usize, 1), search.value.hits.len);
    try std.testing.expectEqualStrings("doc-a", search.value.hits[0].doc_id);

    var sparse_search_indexes = [_][]u8{@constCast("serverless_sparse")};
    var sparse_features = [_]serverless.SparseTermWeight{
        .{ .term = @constCast("alpha"), .weight = 1.0 },
    };
    var sparse_search = try tables_api.search("", "docs", .{
        .text = @constCast(""),
        .sparse = sparse_features[0..],
        .indexes = sparse_search_indexes[0..],
        .mode = .sparse,
        .limit = 5,
    });
    defer sparse_search.deinit();
    try std.testing.expectEqual(@as(usize, 1), sparse_search.value.hits.len);
    try std.testing.expectEqualStrings("doc-a", sparse_search.value.hits[0].doc_id);

    var hybrid_indexes = [_][]u8{ @constCast("serverless_chunk"), @constCast("serverless_sparse") };
    var hybrid_features = [_]serverless.SparseTermWeight{
        .{ .term = @constCast("alpha"), .weight = 1.0 },
    };
    var hybrid_search = try tables_api.search("", "docs", .{
        .text = @constCast("alpha"),
        .semantic_search = @constCast(query_url),
        .embedding_template = @constCast("{{remoteText url=this}}"),
        .sparse = hybrid_features[0..],
        .indexes = hybrid_indexes[0..],
        .mode = .hybrid,
        .limit = 5,
    });
    defer hybrid_search.deinit();
    try std.testing.expectEqual(@as(usize, 1), hybrid_search.value.hits.len);
    try std.testing.expectEqualStrings("doc-a", hybrid_search.value.hits[0].doc_id);

    const bad_query_url = try std.fmt.allocPrint(alloc, "{s}/kitten.png", .{remote_base_uri});
    defer alloc.free(bad_query_url);
    try std.testing.expectError(error.UnexpectedHttpStatus, tables_api.search("", "docs", .{
        .text = @constCast(""),
        .semantic_search = @constCast(bad_query_url),
        .embedding_template = @constCast("{{remoteText url=this}}"),
        .indexes = search_indexes[0..],
        .limit = 5,
    }));

    try std.testing.expectError(error.UnexpectedHttpStatus, tables_api.search("", "docs", .{
        .text = @constCast(""),
        .embedding_template = @constCast("{{remoteText url=this}}"),
        .indexes = search_indexes[0..],
        .limit = 5,
    }));

    var bad_dense_indexes = [_][]u8{@constCast("unknown_dense")};
    try std.testing.expectError(error.UnexpectedHttpStatus, tables_api.search("", "docs", .{
        .text = @constCast(""),
        .semantic_search = @constCast(query_url),
        .embedding_template = @constCast("{{remoteText url=this}}"),
        .indexes = bad_dense_indexes[0..],
        .limit = 5,
    }));

    var bad_indexes = [_][]u8{@constCast("unknown_sparse")};
    try std.testing.expectError(error.UnexpectedHttpStatus, tables_api.search("", "docs", .{
        .text = @constCast(""),
        .sparse = sparse_features[0..],
        .indexes = bad_indexes[0..],
        .mode = .sparse,
        .limit = 5,
    }));

    var bad_hybrid_dense_indexes = [_][]u8{ @constCast("unknown_dense"), @constCast("serverless_sparse") };
    try std.testing.expectError(error.UnexpectedHttpStatus, tables_api.search("", "docs", .{
        .text = @constCast("alpha"),
        .semantic_search = @constCast(query_url),
        .embedding_template = @constCast("{{remoteText url=this}}"),
        .sparse = hybrid_features[0..],
        .indexes = bad_hybrid_dense_indexes[0..],
        .mode = .hybrid,
        .limit = 5,
    }));

    var bad_hybrid_sparse_indexes = [_][]u8{ @constCast("serverless_chunk"), @constCast("unknown_sparse") };
    try std.testing.expectError(error.UnexpectedHttpStatus, tables_api.search("", "docs", .{
        .text = @constCast("alpha"),
        .semantic_search = @constCast(query_url),
        .embedding_template = @constCast("{{remoteText url=this}}"),
        .sparse = hybrid_features[0..],
        .indexes = bad_hybrid_sparse_indexes[0..],
        .mode = .hybrid,
        .limit = 5,
    }));
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
    const slice = std.fmt.bufPrint(buf, "/tmp/antfly-serverless-http-client-{s}-{d}-{d}\x00", .{
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
