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

/// AntflyApiHandler implements the generated ServerRouter handler interfaces
/// (metadata_openapi and usermgr_openapi) by calling business logic directly
/// on ApiHttpServer and its underlying services.
///
/// Each handler method extracts wire parameters from the httpx Context, calls
/// a typed operation, and adapts its owned result to an httpx.Response.
const std = @import("std");
const httpx = @import("httpx");
const runtime_http_bridge = @import("../runtime_http_bridge.zig");
const inference_connection_abi = @import("../inference_connection_abi.zig");
const http_common = @import("../raft/transport/http_common.zig");
const http_route_helpers = @import("http_route_helpers.zig");
const http_server_mod = @import("http_server.zig");
const operation_contract = @import("operation.zig");
const probe_operations = @import("probe_operations.zig");
const storage_maintenance_operations = @import("storage_maintenance_operations.zig");
const internal_group_operations = @import("internal_group_operations.zig");
const internal_query_operations = @import("internal_query_operations.zig");
const contextual_operations = @import("contextual_operations.zig");
const protocol_adapters = @import("protocol_adapters.zig");
const mcp = @import("antfly_mcp");
const internal_join_operations = @import("internal_join_operations.zig");
const internal_repair_operations = @import("internal_repair_operations.zig");
const internal_batch_forwarding = @import("internal_batch_forwarding.zig");
const algebraic_partials_wire = @import("algebraic_partials_wire.zig");
const http_client = @import("http_client.zig");
const repair_jobs = @import("repair_jobs.zig");
const ApiHttpServer = http_server_mod.ApiHttpServer;
const AuthenticatedIdentity = http_server_mod.AuthenticatedIdentity;

const common_secrets = @import("../common/secrets.zig");
const common_config = @import("../common/config.zig");
const ha_mutation_inventory = @import("../storage/ha/mutation_inventory.zig");
const cluster = @import("cluster.zig");
const cluster_api_http = @import("cluster_api_http.zig");
const connections_api = @import("connections.zig");
const backups_api = @import("backups.zig");
const batch_api = @import("batch.zig");
const restore_jobs = @import("restore_jobs.zig");
const public_table_http = @import("public_table_http.zig");
const tables_api = @import("tables.zig");
const table_contract = @import("table_contract.zig");
const table_reads = if (builtin.is_test) @import("table_reads.zig") else @import("table_read_source.zig");
const table_writes = @import("table_writes.zig");
const linear_merge_api = @import("linear_merge.zig");
const transactions_api = @import("transactions.zig");
const distributed_txn = @import("distributed_txn.zig");
const distributed_graph = @import("distributed_graph.zig");
const retrieval_agent = @import("retrieval_agent.zig");
const generating_runtime = @import("../generating/mod.zig");
const query_api = @import("query.zig");
const query_contract = @import("query_contract.zig");
const query_builder_agent = @import("query_builder_agent.zig");
const managed_embedder = @import("../inference/managed_embedder.zig");
const metadata_table_manager = @import("../metadata/table_manager.zig");
const metadata_api = @import("../metadata/api.zig");
const metadata_authority = @import("../metadata/authority.zig");
const metadata_transition_state = @import("../metadata/transition_state.zig");
const test_contract_helpers = @import("test_contract_helpers.zig");
const platform_time = @import("antfly_platform").time;
const usermgr = @import("../usermgr/mod.zig");
const raft_mod = @import("../raft/mod.zig");
const raft_reconciler = @import("../raft/reconciler.zig");
const casbin = @import("antfly_casbin");
const builtin = @import("builtin");

const db_mod = @import("../storage/db/mod.zig");
const metadata_openapi = @import("antfly_metadata_openapi");
const usermgr_openapi = @import("antfly_usermgr_openapi");
const routes = @import("http_routes.zig").Routes;
const request_admission_policy = @import("request_admission_policy.zig");
const admin_routes = @import("../admin/routes.zig");
const internal_routes = @import("../internal/routes.zig");

const ParsedGlobalQueryTable = struct {
    parsed: std.json.Parsed(metadata_openapi.QueryRequest),
    table_name: []const u8,

    fn deinit(self: *@This()) void {
        self.parsed.deinit();
    }
};

fn parseGlobalQueryTable(alloc: std.mem.Allocator, body: []const u8) !ParsedGlobalQueryTable {
    try query_contract.validatePublicQuerySortTupleContract(alloc, body);
    var parsed = metadata_openapi.server.parseGlobalQueryBody(alloc, body) catch return error.InvalidQueryRequest;
    errdefer parsed.deinit();
    return .{
        .parsed = parsed,
        .table_name = parsed.value.table orelse "",
    };
}

fn isNdjsonContentType(content_type: ?[]const u8) bool {
    const value = content_type orelse return false;
    const media_type = std.mem.trim(u8, if (std.mem.indexOfScalar(u8, value, ';')) |idx| value[0..idx] else value, " \t");
    return std.ascii.eqlIgnoreCase(media_type, "application/x-ndjson");
}

pub const RequestAdmission = http_server_mod.RequestAdmission;
const default_query_body_admission_capacity: usize = 32;

pub const AntflyApiHandler = struct {
    api_server: *ApiHttpServer,
    /// Separate phase admission for H2 query bodies. Slow uploads must not
    /// consume execution permits, but still need a global waiter bound.
    query_body_admission: RequestAdmission = RequestAdmission.init(default_query_body_admission_capacity),

    /// Cancellation observation is owned by httpx. Retain this lifecycle hook
    /// while direct and linked handler construction share one interface.
    pub fn initRuntime(_: *AntflyApiHandler, _: std.mem.Allocator) !void {}

    /// Register the complete generated public surface for one handler
    /// instance. Runtime roles call this common registrar instead of carrying
    /// their own duplicate route arrays or active-handler globals.
    pub fn registerRoutes(self: *AntflyApiHandler, server: *httpx.Server) !void {
        try self.installMiddleware(server);
        return self.registerRoutesWithOptions(server, true, true);
    }

    /// Standalone owns a stronger readiness contract than the shared API
    /// kernel (listener publication, maintenance admission, and initialized
    /// API state), so it supplies the two root probe handlers itself.
    pub fn registerRoutesWithoutProbes(self: *AntflyApiHandler, server: *httpx.Server) !void {
        try self.installMiddleware(server);
        return self.registerRoutesWithOptions(server, true, false);
    }

    /// Metadata owns a distinct administration surface and must not expose
    /// data-node protocol or internal-group routes on its admin listener.
    pub fn registerGeneratedRoutesWithProbes(self: *AntflyApiHandler, server: *httpx.Server) !void {
        try self.installMiddleware(server);
        return self.registerRoutesWithOptions(server, false, true);
    }

    fn installMiddleware(self: *AntflyApiHandler, server: *httpx.Server) !void {
        try server.use(httpx.Middleware.bind("antfly-request-stats", self, recordRequest));
        try server.use(httpx.Middleware.bind("antfly-ha-mutation-policy", self, enforceHaMutationPolicy));
    }

    fn recordRequest(self: *AntflyApiHandler, ctx: *httpx.Context, next: *httpx.Next) !httpx.Response {
        self.api_server.recordHandledRequest();
        return next.call(ctx) catch |err| mapIngressError(ctx, err);
    }

    /// Continuous-HA mutation safety belongs to ingress policy, not to a
    /// compatibility dispatcher or individual business handlers. The
    /// classifier consumes canonical application paths, so generated
    /// `/db/v1` routes and root contextual routes share one inventory.
    fn enforceHaMutationPolicy(self: *AntflyApiHandler, ctx: *httpx.Context, next: *httpx.Next) !httpx.Response {
        if (try self.haMutationRejection(ctx)) |response| return response;
        return next.call(ctx);
    }

    /// Dispatch one route through the application-owned ingress policy.
    ///
    /// Direct registrations install the same policy as `httpx` middleware.
    /// Independently code-generated runtimes cannot carry that middleware
    /// through the route-manifest ABI, so their dispatch export must enter here
    /// instead of invoking the manifest's raw route handler. Keeping the
    /// policy in the API kernel also prevents host runtimes from duplicating
    /// application configuration or error classification.
    pub fn dispatchLinkedRoute(self: *AntflyApiHandler, ctx: *httpx.Context, route_handler: httpx.Handler) !httpx.Response {
        self.api_server.recordHandledRequest();
        if (try self.haMutationRejection(ctx)) |response| return response;
        return route_handler.invoke(ctx) catch |err| mapIngressError(ctx, err);
    }

    fn haMutationRejection(self: *AntflyApiHandler, ctx: *httpx.Context) !?httpx.Response {
        const policy = self.api_server.haMutationPolicy();
        if (!policy.failover_safe_mutations_only) return null;
        const path = http_server_mod.stripApiPrefix(ctx.request.uri.path);
        const mutation = classifyHaMutation(ctx.request.method, path) orelse return null;
        if (mutation.disposition != .reject and
            (mutation.disposition != .remote_apply or policy.remote_apply_mutations_enabled))
        {
            return null;
        }

        _ = ctx.status(503);
        return try ctx.json(.{
            .@"error" = "mutation is not continuously replicated while HA is active",
            .code = "ha_mutation_not_replicated",
            .surface = @tagName(mutation.surface),
        });
    }

    fn classifyHaMutation(method: httpx.Method, path: []const u8) ?ha_mutation_inventory.Classification {
        const inventory_method: http_common.Method = switch (method) {
            .GET, .HEAD, .OPTIONS => return null,
            .POST => .POST,
            .PUT => .PUT,
            .DELETE => .DELETE,
            // The inventory's deliberately small method type makes every new
            // mutation verb opt in. Do not let a future PATCH or custom route
            // bypass continuous-HA durability policy merely because the
            // application-method adapter has not learned that verb yet.
            else => return .{
                .surface = .unclassified_non_get,
                .disposition = .reject,
            },
        };
        return ha_mutation_inventory.classify(inventory_method, path);
    }

    fn metadataNotLeaderResponse(ctx: *httpx.Context) !httpx.Response {
        _ = ctx.status(503);
        try ctx.setHeader("content-type", "application/json");
        try ctx.setHeader("Retry-After", "1");
        try ctx.setHeader(http_common.metadata_not_leader_header, http_common.metadata_not_leader_value);
        _ = ctx.response.body("{\"error\":\"metadata leader unavailable\"}");
        return ctx.response.build();
    }

    fn mapIngressError(ctx: *httpx.Context, err: anyerror) !httpx.Response {
        if (metadata_authority.isRetryableError(err)) return metadataNotLeaderResponse(ctx);
        return err;
    }

    fn registerRoutesWithOptions(
        self: *AntflyApiHandler,
        server: *httpx.Server,
        include_contextual: bool,
        include_probes: bool,
    ) !void {
        var prefixed = PrefixedServer("/db/v1", httpx.Server){ .inner = server };
        try self.registerRouteSets(&prefixed, server, include_contextual, include_probes);
    }

    /// One generated route manifest shared by native servers and the linked
    /// kernel registrar boundary. `public_server` already represents the
    /// `/db/v1` namespace; `root_server` owns root-level APIs such as auth.
    pub fn registerRouteSets(
        self: *AntflyApiHandler,
        public_server: anytype,
        root_server: anytype,
        include_contextual: bool,
        include_probes: bool,
    ) !void {
        const metadata_router = metadata_openapi.server.ServerRouter(AntflyApiHandler).init(self);
        try metadata_router.register(public_server);
        const usermgr_router = usermgr_openapi.server.ServerRouter(AntflyApiHandler).init(self);
        try usermgr_router.register(root_server);
        if (include_contextual) {
            try self.registerContextualRoutes(root_server, include_probes);
        } else if (include_probes) {
            try self.registerProbes(root_server);
        }
    }

    /// Hand-written protocol, operational, administrative, and internal
    /// routes share this registrar in every runtime role. The generated data
    /// API remains available only below `/db/v1`; historical root aliases are
    /// intentionally not registered.
    fn registerContextualRoutes(self: *AntflyApiHandler, server: anytype, include_probes: bool) !void {
        if (include_probes) {
            try self.registerProbes(server);
        }

        const mcp_paths = [_][]const u8{ routes.mcp_v1, routes.mcp_v1_prefix ++ "*" };
        const mcp_handler = httpx.Handler.bind(self, mcpRoute);
        inline for (mcp_paths) |path| {
            try server.get(path, mcp_handler);
            try server.post(path, mcp_handler);
            try server.delete(path, mcp_handler);
        }
        if (self.api_server.cfg.experimental) {
            try server.post(routes.a2a, httpx.Handler.bind(self, a2aRoute));
            const a2a_card_handler = httpx.Handler.bind(self, a2aCardRoute);
            try server.get(routes.agent_card, a2a_card_handler);
        }

        const ard_handler = httpx.Handler.bind(self, ardRoute);
        try server.get(routes.ai_catalog, ard_handler);
        try server.get(routes.ard_v1, ard_handler);
        try server.get(routes.ard_v1 ++ "/*", ard_handler);
        try server.post(routes.ard_v1_search, ard_handler);
        try server.post(routes.ard_v1_explore, ard_handler);

        const extension_paths = [_][]const u8{
            routes.extensions_v1,
            routes.extensions_v1_packages,
            routes.extensions_v1_packages_prefix ++ "*",
            routes.extensions_v1_installed,
            routes.extensions_v1_installed_prefix ++ "*",
        };
        const extension_handler = httpx.Handler.bind(self, extensionRoute);
        inline for (extension_paths) |path| {
            try server.get(path, extension_handler);
            try server.post(path, extension_handler);
            try server.put(path, extension_handler);
        }
        const extension_agent_handler = httpx.Handler.bind(self, extensionAgentRoute);
        try server.get(routes.agents_v1_extensions_prefix ++ "*", extension_agent_handler);
        try server.post(routes.agents_v1_extensions_prefix ++ "*", extension_agent_handler);
        try server.put(routes.agents_v1_extensions_prefix ++ "*", extension_agent_handler);

        const ha_admin_paths = [_][]const u8{ admin_routes.ha, admin_routes.ha ++ "/*" };
        const ha_handler = httpx.Handler.bind(self, haRoute);
        inline for (ha_admin_paths) |path| {
            try server.get(path, ha_handler);
            try server.post(path, ha_handler);
            try server.put(path, ha_handler);
            try server.delete(path, ha_handler);
        }
        try server.post(admin_routes.maintenance_check, httpx.Handler.bind(self, checkStorage));
        try server.post(admin_routes.maintenance_compact, httpx.Handler.bind(self, compactStorage));
        try server.post(admin_routes.maintenance_vacuum, httpx.Handler.bind(self, vacuumStorage));
        try server.get(admin_routes.maintenance_jobs_prefix ++ "*", httpx.Handler.bind(self, getStorageMaintenanceJob));
        try server.delete(admin_routes.maintenance_jobs_prefix ++ "*", httpx.Handler.bind(self, cancelStorageMaintenanceJob));

        const ha_internal_paths = [_][]const u8{ internal_routes.ha, internal_routes.ha ++ "/*" };
        inline for (ha_internal_paths) |path| {
            try server.get(path, ha_handler);
            try server.post(path, ha_handler);
            try server.put(path, ha_handler);
            try server.delete(path, ha_handler);
        }

        const group_prefix = routes.internal_groups_prefix ++ ":group_id";
        const table_prefix = group_prefix ++ "/tables/:table_name";
        const internal_table_prefix = routes.internal_tables_prefix ++ ":table_name";
        try server.get(routes.internal_capabilities, httpx.Handler.bind(self, internalCapabilities));
        try server.get(group_prefix ++ routes.group_db_median_key_suffix, httpx.Handler.bind(self, internalGroupMedianKey));
        try server.get(table_prefix ++ routes.documents_marker ++ ":key", httpx.Handler.bind(self, internalGroupLookup));
        try server.get(table_prefix ++ routes.documents_marker ++ ":key" ++ routes.artifacts_suffix, httpx.Handler.bind(self, internalDocumentArtifactManifests));
        try server.get(table_prefix ++ routes.documents_marker ++ ":key" ++ routes.artifacts_marker ++ ":artifact_name", httpx.Handler.bind(self, internalDocumentArtifactManifest));
        try server.get(internal_table_prefix ++ routes.repair_jobs_marker ++ ":job_id" ++ routes.repair_attempts_marker ++ ":attempt_id" ++ routes.repair_cancel_state_suffix, httpx.Handler.bind(self, internalRepairCancelState));
        try server.post(table_prefix ++ routes.join_job_state_suffix, httpx.Handler.bind(self, internalJoinJobState));
        try server.post(table_prefix ++ routes.join_finalize_suffix, httpx.Handler.bind(self, internalJoinFinalize));
        try server.post(table_prefix ++ routes.join_rows_suffix, httpx.Handler.bind(self, internalJoinRows));
        try server.post(table_prefix ++ routes.join_unmatched_suffix, httpx.Handler.bind(self, internalJoinUnmatched));
        try server.post(table_prefix ++ routes.join_partition_suffix, httpx.Handler.bind(self, internalJoinPartition));
        try server.post(internal_table_prefix ++ routes.corrupt_embedding_artifact_suffix, httpx.Handler.bind(self, internalCorruptEmbeddingArtifact));
        try server.post(group_prefix ++ routes.shard_ops_observe_split_suffix, httpx.Handler.bind(self, internalObserveSplit));
        try server.post(group_prefix ++ routes.shard_ops_observe_merge_suffix, httpx.Handler.bind(self, internalObserveMerge));
        try server.post(group_prefix ++ routes.shard_ops_execute_suffix, httpx.Handler.bind(self, internalExecuteTransition));
        try server.post(table_prefix ++ routes.batch_suffix, httpx.Handler.bind(self, internalGroupBatch));
        try server.post(table_prefix ++ routes.documents_suffix, httpx.Handler.bind(self, internalGroupScan));
        try server.post(table_prefix ++ routes.query_suffix, httpx.Handler.bind(self, internalGroupQuery));
        try server.post(table_prefix ++ routes.query_preflight_suffix, httpx.Handler.bind(self, internalGroupQueryPreflight));
        try server.post(table_prefix ++ routes.vector_worker_suffix, httpx.Handler.bind(self, internalGroupVectorWorker));
        try server.post(table_prefix ++ routes.graph_expand_suffix, httpx.Handler.bind(self, internalGraphExpand));
        try server.post(table_prefix ++ routes.graph_hydrate_suffix, httpx.Handler.bind(self, internalGraphHydrate));
        try server.post(table_prefix ++ routes.graph_edges_suffix, httpx.Handler.bind(self, internalGraphEdges));
        try server.post(table_prefix ++ routes.text_stats_suffix, httpx.Handler.bind(self, internalTextStats));
        try server.post(table_prefix ++ routes.algebraic_partials_suffix, httpx.Handler.bind(self, internalAlgebraicPartials));
        try server.post(table_prefix ++ routes.routed_batch_suffix, httpx.Handler.bind(self, internalGroupRoutedBatch));
        try server.post(table_prefix ++ routes.txn_begin_suffix, httpx.Handler.bind(self, internalTxnBegin));
        try server.post(table_prefix ++ routes.txn_prepare_suffix, httpx.Handler.bind(self, internalTxnPrepare));
        try server.post(table_prefix ++ routes.txn_resolve_suffix, httpx.Handler.bind(self, internalTxnResolve));
        try server.post(table_prefix ++ routes.txn_status_suffix, httpx.Handler.bind(self, internalTxnStatus));
        try server.post(table_prefix ++ routes.txn_acknowledge_suffix, httpx.Handler.bind(self, internalTxnAcknowledge));
        try server.post(table_prefix ++ routes.artifact_repair_suffix, httpx.Handler.bind(self, internalArtifactRepairList));
        try server.post(table_prefix ++ routes.artifact_repair_run_suffix, httpx.Handler.bind(self, internalArtifactRepairRun));
        const document_artifact_prefix = table_prefix ++ routes.documents_marker ++ ":key" ++ routes.artifacts_marker ++ ":artifact_name";
        try server.post(document_artifact_prefix ++ routes.placement_update_suffix, httpx.Handler.bind(self, internalDocumentArtifactPlacementUpdate));
        try server.post(document_artifact_prefix ++ routes.child_range_batch_suffix, httpx.Handler.bind(self, internalDocumentArtifactChildRangeBatch));
        try server.post(document_artifact_prefix ++ routes.reprocess_suffix, httpx.Handler.bind(self, internalDocumentArtifactReprocess));
        try server.post(table_prefix ++ routes.artifacts_marker ++ ":artifact_name" ++ routes.reprocess_suffix, httpx.Handler.bind(self, internalTableArtifactReprocess));
        try server.post(routes.agents_retrieval, httpx.Handler.bind(self, internalRetrieval));
    }

    fn registerProbes(self: *AntflyApiHandler, server: anytype) !void {
        try server.get(routes.healthz, httpx.Handler.bind(self, healthz));
        try server.get(routes.readyz, httpx.Handler.bind(self, readyz));
    }

    pub fn deinitRuntime(self: *AntflyApiHandler) void {
        _ = self;
    }

    fn respondOwnedApiResponse(ctx: *httpx.Context, resp: anytype) !httpx.Response {
        return respondOwnedApiResponseWithAllocator(ctx, resp, ctx.allocator);
    }

    fn respondOwnedApiResponseWithAllocator(ctx: *httpx.Context, resp: anytype, alloc: std.mem.Allocator) !httpx.Response {
        defer resp.deinit(alloc);
        const Response = @TypeOf(resp.*);
        _ = ctx.status(resp.status);
        const is_json = if (@hasField(Response, "json")) resp.json else resp.status >= 200 and resp.status < 300;
        if (is_json) {
            try ctx.setHeader("content-type", "application/json");
        } else {
            try ctx.setHeader("content-type", "text/plain; charset=utf-8");
        }
        if (@hasField(Response, "retry_after_seconds")) {
            if (resp.retry_after_seconds) |seconds| {
                var retry_after_buf: [10]u8 = undefined;
                const retry_after = try std.fmt.bufPrint(&retry_after_buf, "{d}", .{seconds});
                try ctx.setHeader("Retry-After", retry_after);
            }
        }
        _ = ctx.response.body(resp.body);
        return ctx.response.build();
    }

    fn respondOwnedContextualResponse(
        ctx: *httpx.Context,
        resp: *contextual_operations.OwnedResponse,
        alloc: std.mem.Allocator,
    ) !httpx.Response {
        defer resp.deinit(alloc);
        _ = ctx.status(resp.status);
        try ctx.setHeader("content-type", resp.content_type);
        for (resp.headers) |header| try ctx.setHeader(header.name, header.value);
        if (resp.public_cors) try ctx.setHeader("Access-Control-Allow-Origin", "*");
        _ = ctx.response.body(resp.body);
        return ctx.response.build();
    }

    fn respondApiResponseBody(ctx: *httpx.Context, status: u16, body: []const u8) !httpx.Response {
        _ = ctx.status(status);
        if (status >= 200 and status < 300) {
            try ctx.setHeader("content-type", "application/json");
        } else {
            try ctx.setHeader("content-type", "text/plain; charset=utf-8");
        }
        _ = ctx.response.body(body);
        return ctx.response.build();
    }

    fn respondJsonErrorBody(ctx: *httpx.Context, status: u16, body: []const u8) !httpx.Response {
        _ = ctx.status(status);
        try ctx.setHeader("content-type", "application/json");
        _ = ctx.response.body(body);
        return ctx.response.build();
    }

    fn respondQueryEmbeddingOperationalError(ctx: *httpx.Context, err: anyerror) !?httpx.Response {
        const normalized = http_server_mod.normalizeQueryEmbeddingOperationalError(err) orelse return null;
        const response = switch (normalized) {
            error.QueryEmbeddingInputTooLarge => .{ @as(u16, 413), "query embedding input too large", false },
            error.QueryEmbeddingOverloaded => .{ @as(u16, 429), "query embedding overloaded", true },
            error.EmbedRateLimited => .{ @as(u16, 429), "query embedding rate limited", true },
            error.EmbedTransientFailure => .{ @as(u16, 503), "query embedding temporarily unavailable", true },
            error.EmbedUpstreamFailure => .{ @as(u16, 502), "query embedding provider failed", false },
            error.Timeout => .{ @as(u16, 504), "query embedding timed out", false },
            else => return null,
        };
        if (response[2]) try ctx.setHeader("Retry-After", "1");
        _ = ctx.status(response[0]);
        return try ctx.text(response[1]);
    }

    const OffloadedTableBatch = struct {
        alloc: std.mem.Allocator,
        table_name: []const u8,
        body_data: []const u8,
        api: public_table_http.TableApi,
        done: std.atomic.Value(bool) = .init(false),
        result: ?public_table_http.OwnedResponse = null,
        err: ?anyerror = null,

        fn run(self: *@This()) void {
            defer self.done.store(true, .release);
            self.result = public_table_http.handleTableBatch(
                self.alloc,
                self.table_name,
                self.body_data,
                self.api,
            ) catch |err| {
                self.err = err;
                return;
            };
        }
    };

    fn handleTableBatchInline(
        ctx: *httpx.Context,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        body_data: []const u8,
        api: public_table_http.TableApi,
    ) !httpx.Response {
        var resp = try public_table_http.handleTableBatch(alloc, table_name, body_data, api);
        defer resp.deinit(alloc);
        return respondApiResponseBody(ctx, resp.status, resp.body);
    }

    fn handleTableBatchOffEventLoop(
        ctx: *httpx.Context,
        backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
        table_name: []const u8,
        body_data: []const u8,
        api: public_table_http.TableApi,
    ) !httpx.Response {
        const runtime = backend_runtime orelse return handleTableBatchInline(ctx, ctx.allocator, table_name, body_data, api);
        var runtime_io = runtime.io() orelse return handleTableBatchInline(ctx, ctx.allocator, table_name, body_data, api);
        const job_alloc = std.heap.page_allocator;
        const owned_table_name = job_alloc.dupe(u8, table_name) catch |err| {
            std.log.warn("batch offload table-name allocation failed; executing inline err={s}", .{@errorName(err)});
            return handleTableBatchInline(ctx, ctx.allocator, table_name, body_data, api);
        };
        defer job_alloc.free(owned_table_name);
        const owned_body_data = job_alloc.dupe(u8, body_data) catch |err| {
            std.log.warn("batch offload body allocation failed; executing inline err={s}", .{@errorName(err)});
            return handleTableBatchInline(ctx, ctx.allocator, table_name, body_data, api);
        };
        defer job_alloc.free(owned_body_data);
        var job = OffloadedTableBatch{
            .alloc = job_alloc,
            .table_name = owned_table_name,
            .body_data = owned_body_data,
            .api = api,
        };
        var future = runtime_io.concurrent(OffloadedTableBatch.run, .{&job}) catch |err| {
            // Saturating the backend executor must not turn a valid write into
            // an empty HTTP disconnect. The request still owns its buffers, so
            // executing synchronously is a safe bounded degradation path.
            std.log.warn("batch offload scheduling failed; executing inline err={s}", .{@errorName(err)});
            return handleTableBatchInline(ctx, ctx.allocator, table_name, body_data, api);
        };
        while (!job.done.load(.acquire)) {
            // The borrowed request token is consumed only by post-commit
            // visibility waits. Never cancel the whole future here: that
            // could interrupt proposal before its durability is known.
            if (ctx.isCancellationRequested()) {
                break;
            }
            ctx.io.sleep(std.Io.Duration.fromMilliseconds(1), .awake) catch {};
        }
        _ = future.await(runtime_io);
        if (job.err) |err| return err;
        var resp = job.result.?;
        defer resp.deinit(job_alloc);
        return respondApiResponseBody(ctx, resp.status, resp.body);
    }

    fn forwardTransactionSession(
        self: *AntflyApiHandler,
        ctx: *httpx.Context,
        txn_id: db_mod.types.TxnId,
        body: []const u8,
    ) !?httpx.Response {
        const method: contextual_operations.Method = switch (ctx.request.method) {
            .GET => .get,
            .POST => .post,
            .PUT => .put,
            .DELETE => .delete,
            else => return error.UnsupportedMethod,
        };
        var response = (try self.api_server.forwardSessionOperation(txn_id, .{
            .method = method,
            .target = ctx.request.uri.raw,
            .authorization = ctx.header("authorization"),
            .content_type = ctx.header("content-type"),
            .body = body,
        })) orelse return null;
        return try respondOwnedContextualResponse(ctx, &response, self.api_server.alloc);
    }

    fn haRoute(self: *AntflyApiHandler, ctx: *httpx.Context) !httpx.Response {
        const method: contextual_operations.Method = switch (ctx.request.method) {
            .GET => .get,
            .POST => .post,
            .PUT => .put,
            .DELETE => .delete,
            else => return textResponse(ctx, 405, "method not allowed"),
        };
        const body = (try ctx.body()) orelse "";
        var response = (try self.api_server.executeHaRoute(.{
            .method = method,
            .target = ctx.request.uri.raw,
            .authorization = ctx.header("authorization"),
            .content_type = ctx.header("content-type"),
            .body = body,
        })) orelse return textResponse(ctx, 404, "not found");
        defer response.deinit();
        _ = ctx.status(response.status);
        try ctx.setHeader("content-type", response.content_type);
        _ = ctx.response.body(response.body);
        return ctx.response.build();
    }

    fn extensionRoute(self: *AntflyApiHandler, ctx: *httpx.Context) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |response| return response;

        const method: contextual_operations.Method = switch (ctx.request.method) {
            .GET => .get,
            .POST => .post,
            .PUT => .put,
            else => return jsonResponse(ctx, 405, "{\"error\":\"method not allowed\"}"),
        };
        const path = ctx.request.uri.path;
        const body = (try ctx.body()) orelse "";
        var response = self.api_server.executeExtensionRoute(method, path, body) catch |err| {
            if (metadata_authority.isRetryableError(err) and
                ApiHttpServer.extensionRouteMutatesMetadata(method, path))
            {
                _ = ctx.status(503);
                try ctx.setHeader("content-type", "application/json");
                try ctx.setHeader("Retry-After", "1");
                try ctx.setHeader(http_common.metadata_not_leader_header, http_common.metadata_not_leader_value);
                _ = ctx.response.body("{\"error\":\"metadata leader unavailable\"}");
                return ctx.response.build();
            }
            return err;
        } orelse return jsonResponse(ctx, 404, "{\"error\":\"not found\"}");
        defer response.deinit(self.api_server.alloc);
        _ = ctx.status(response.status);
        try ctx.setHeader("content-type", response.content_type);
        _ = ctx.response.body(response.body);
        return ctx.response.build();
    }

    fn ardRoute(self: *AntflyApiHandler, ctx: *httpx.Context) !httpx.Response {
        const path = ctx.request.uri.path;
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        const public_catalog = std.mem.eql(u8, path, routes.ai_catalog) and self.api_server.cfg.ard_public_catalog_enabled;
        const carries_authentication = ctx.header("authorization") != null or
            ctx.header(http_server_mod.trusted_principal_header) != null;
        if (!public_catalog or carries_authentication) {
            if (try self.authorizeRequest(ctx, &authenticated_identity)) |response| return response;
        }

        const body = (try ctx.body()) orelse "";
        var response = self.api_server.executeArd(
            path,
            ctx.request.uri.query orelse "",
            body,
            authenticated_identity,
        ) catch |err| return switch (err) {
            error.Unauthorized => unauthorizedResponse(ctx),
            error.Forbidden => textResponse(ctx, 403, "forbidden"),
            error.NotFound => jsonResponse(ctx, 404, "{\"error\":\"not found\"}"),
            error.InvalidArdSearchRequest => if (std.mem.eql(u8, path, routes.ard_v1_explore))
                jsonResponse(ctx, 400, "{\"error\":\"invalid ARD explore request\"}")
            else
                jsonResponse(ctx, 400, "{\"error\":\"invalid ARD search request\"}"),
            error.InvalidArdAgentsRequest => jsonResponse(ctx, 400, "{\"error\":\"invalid ARD agents request\"}"),
            else => return err,
        };
        defer response.deinit(self.api_server.alloc);
        _ = ctx.status(response.status);
        try ctx.setHeader("content-type", response.content_type);
        if (response.public_cors) try ctx.setHeader("Access-Control-Allow-Origin", "*");
        _ = ctx.response.body(response.body);
        return ctx.response.build();
    }

    fn extensionAgentRoute(self: *AntflyApiHandler, ctx: *httpx.Context) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |response| return response;
        const method: contextual_operations.Method = switch (ctx.request.method) {
            .GET => .get,
            .POST => .post,
            .PUT => .put,
            .DELETE => .delete,
            else => return jsonResponse(ctx, 405, "{\"error\":\"method not allowed\"}"),
        };
        var response = self.api_server.executeExtensionAgent(
            method,
            ctx.request.uri.path,
            ctx.request.uri.query orelse "",
            authenticated_identity,
        ) catch |err| return switch (err) {
            error.NotFound => jsonResponse(ctx, 404, "{\"error\":\"not found\"}"),
            error.MethodNotAllowed => jsonResponse(ctx, 405, "{\"error\":\"method not allowed\"}"),
            else => return err,
        };
        defer response.deinit(self.api_server.alloc);
        _ = ctx.status(response.status);
        try ctx.setHeader("content-type", response.content_type);
        _ = ctx.response.body(response.body);
        return ctx.response.build();
    }

    fn a2aCardRoute(self: *AntflyApiHandler, ctx: *httpx.Context) !httpx.Response {
        const body = try protocol_adapters.a2aCardJsonAlloc(
            self.api_server,
            ApiHttpServer.queryEmbeddingSecurityScope(null),
            @as(?AuthenticatedIdentity, null),
        );
        defer self.api_server.alloc.free(body);
        return jsonResponse(ctx, 200, body);
    }

    fn a2aRoute(self: *AntflyApiHandler, ctx: *httpx.Context) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |response| return response;
        const body = (try ctx.body()) orelse "";
        var response = try protocol_adapters.executeA2aRequest(
            self.api_server,
            ctx.header("authorization"),
            body,
            ApiHttpServer.queryEmbeddingSecurityScope(authenticated_identity),
            authenticated_identity,
        );
        defer response.deinit(self.api_server.alloc);
        _ = ctx.status(response.status);
        try ctx.setHeader("content-type", response.content_type);
        _ = ctx.response.body(response.body);
        return ctx.response.build();
    }

    fn mcpRoute(self: *AntflyApiHandler, ctx: *httpx.Context) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |response| return response;
        const method: contextual_operations.Method = switch (ctx.request.method) {
            .GET => .get,
            .POST => .post,
            .DELETE => .delete,
            else => .put,
        };
        const path = ctx.request.uri.path;
        var extension_name: ?[]const u8 = null;
        var endpoint_path: []const u8 = routes.mcp_v1;
        if (std.mem.startsWith(u8, path, routes.mcp_v1_extension_profiles_prefix)) {
            const profile = path[routes.mcp_v1_extension_profiles_prefix.len..];
            if (!std.mem.eql(u8, profile, "copilot"))
                return jsonResponse(ctx, 404, "{\"error\":\"not found\"}");
        } else if (routes.matchMcpExtension(path)) |extension| {
            extension_name = extension.name;
            endpoint_path = ctx.request.uri.raw;
        }
        const body = (try ctx.body()) orelse "";
        const request = protocol_adapters.McpRequest{
            .method = method,
            .endpoint_path = endpoint_path,
            .authorization = ctx.header("authorization"),
            .trusted_principal = ctx.header(http_server_mod.trusted_principal_header),
            .session_id = ctx.header(mcp.session_id_header),
            .last_event_id = ctx.header(mcp.last_event_id_header),
            .body = body,
        };
        var response = if (extension_name) |name|
            try protocol_adapters.executeExtensionMcpRequest(self.api_server, request, authenticated_identity, name)
        else
            try protocol_adapters.executeMcpRequest(self.api_server, request, authenticated_identity);
        defer response.deinit(self.api_server.alloc);
        _ = ctx.status(response.status);
        try ctx.setHeader("content-type", response.content_type);
        for (response.headers) |header| try ctx.setHeader(header.name, header.value);
        _ = ctx.response.body(response.body);
        return ctx.response.build();
    }

    fn authorizeStorageMaintenance(
        self: *AntflyApiHandler,
        ctx: *httpx.Context,
        identity: *?AuthenticatedIdentity,
    ) !?httpx.Response {
        const native_auth_available =
            (self.api_server.cfg.auth_enabled or self.api_server.cfg.trusted_principal_secret != null) and
            (self.api_server.cfg.user_manager != null or self.api_server.cfg.trusted_principal_secret != null);
        if (native_auth_available) return try self.authorizeRequest(ctx, identity);

        const expected = self.api_server.cfg.admin_bearer_token orelse
            return try textResponse(ctx, 403, "admin API disabled without authentication");
        if (expected.len == 0)
            return try textResponse(ctx, 403, "admin API disabled without authentication");
        const authorization = ctx.header("authorization") orelse
            return try unauthorizedResponse(ctx);
        if (!std.mem.startsWith(u8, authorization, "Bearer ") or
            !http_server_mod.constantTimeEql(expected, authorization["Bearer ".len..]))
        {
            return try unauthorizedResponse(ctx);
        }
        return null;
    }

    fn operationContext(ctx: *httpx.Context, identity: ?AuthenticatedIdentity) operation_contract.RequestContext {
        return .{
            .cancellation = if (ctx.cancellation != null or ctx.cancellation_probe != null) .{
                .ptr = ctx,
                .is_cancelled_fn = struct {
                    fn call(raw: *const anyopaque) bool {
                        const context: *const httpx.Context = @ptrCast(@alignCast(raw));
                        return context.isCancellationRequested();
                    }
                }.call,
            } else .none,
            .request_id = ctx.header("x-request-id") orelse "",
            .principal = if (identity) |authenticated| .{
                .kind = .user,
                .subject = authenticated.username,
            } else null,
        };
    }

    fn requestCancellation(ctx: *const httpx.Context) http_common.RequestCancellation {
        return .{
            .borrowed = ctx.cancellation,
            .borrowed_context = if (ctx.cancellation_probe != null) ctx else null,
            .borrowed_is_cancelled = if (ctx.cancellation_probe != null) struct {
                fn call(raw: *const anyopaque) bool {
                    const context: *const httpx.Context = @ptrCast(@alignCast(raw));
                    return context.isCancellationRequested();
                }
            }.call else null,
        };
    }

    fn probeOperations(self: *AntflyApiHandler) probe_operations.Operations {
        return .{ .readiness = .{
            .ptr = self.api_server,
            .check_fn = struct {
                fn call(ptr: *anyopaque) !void {
                    const server: *ApiHttpServer = @ptrCast(@alignCast(ptr));
                    try server.checkReady();
                }
            }.call,
        } };
    }

    fn healthz(self: *AntflyApiHandler, ctx: *httpx.Context) !httpx.Response {
        const status = self.probeOperations().health(operationContext(ctx, null)) catch |err| switch (err) {
            error.Canceled => return textResponse(ctx, 408, "request canceled"),
            error.DeadlineExceeded => return textResponse(ctx, 504, "request deadline exceeded"),
            else => return err,
        };
        return ctx.json(.{ .status = @tagName(status) });
    }

    fn readyz(self: *AntflyApiHandler, ctx: *httpx.Context) !httpx.Response {
        const status = self.probeOperations().ready(operationContext(ctx, null)) catch |err| switch (err) {
            error.Canceled => return textResponse(ctx, 408, "request canceled"),
            error.DeadlineExceeded => return textResponse(ctx, 504, "request deadline exceeded"),
            else => return err,
        };
        if (status == .not_ready) _ = ctx.status(503);
        return ctx.json(.{ .status = @tagName(status) });
    }

    fn storageMaintenanceErrorResponse(ctx: *httpx.Context, err: storage_maintenance_operations.Error) !httpx.Response {
        return switch (err) {
            error.Unsupported => textResponse(ctx, 422, "storage maintenance unsupported"),
            error.OperationUnsupported => textResponse(ctx, 422, "storage maintenance operation unsupported"),
            error.MaintenanceBusy => textResponse(ctx, 409, "storage maintenance already running"),
            error.IdempotencyConflict => textResponse(ctx, 409, "idempotency key reused for another operation"),
            error.InvalidIdempotencyKey, error.InvalidArgument => textResponse(ctx, 400, "invalid idempotency key"),
            error.MaintenanceHistoryFull, error.CapacityExhausted => blk: {
                try ctx.setHeader("Retry-After", "1");
                break :blk textResponse(ctx, 429, "maintenance job history is full; retry after retained job records expire");
            },
            error.MaintenanceJobIdExhausted => textResponse(ctx, 503, "maintenance job namespace exhausted; restart the server before submitting more maintenance work"),
            error.NotFound => textResponse(ctx, 404, "not found"),
            error.Canceled => textResponse(ctx, 408, "request canceled"),
            error.DeadlineExceeded => textResponse(ctx, 504, "request deadline exceeded"),
            error.Unavailable => textResponse(ctx, 503, "storage maintenance unavailable"),
            error.Unauthorized => unauthorizedResponse(ctx),
            error.Forbidden => textResponse(ctx, 403, "forbidden"),
            error.Conflict => textResponse(ctx, 409, "conflict"),
            error.Internal => textResponse(ctx, 500, "internal server error"),
        };
    }

    fn storageMaintenanceJobResponse(
        ctx: *httpx.Context,
        status: u16,
        snapshot: @import("../storage/maintenance.zig").Coordinator.Snapshot,
    ) !httpx.Response {
        _ = ctx.status(status);
        return ctx.json(.{
            .job_id = snapshot.job_id,
            .operation = snapshot.operation,
            .state = snapshot.state,
            .created_at_ms = snapshot.created_at_ms,
            .started_at_ms = snapshot.started_at_ms,
            .completed_at_ms = snapshot.completed_at_ms,
            .result = snapshot.result,
            .@"error" = snapshot.error_name,
        });
    }

    fn startStorageMaintenance(
        self: *AntflyApiHandler,
        ctx: *httpx.Context,
        maintenance_operation: @import("../storage/maintenance.zig").Operation,
    ) !httpx.Response {
        var identity: ?AuthenticatedIdentity = null;
        defer if (identity) |*authenticated| authenticated.deinit(self.api_server.alloc);
        if (try self.authorizeStorageMaintenance(ctx, &identity)) |response| return response;

        const operations = storage_maintenance_operations.Operations.init(self.api_server.cfg.storage_maintenance);
        const snapshot = operations.start(operationContext(ctx, identity), .{
            .operation = maintenance_operation,
            .idempotency_key = ctx.header("Idempotency-Key"),
        }) catch |err| return storageMaintenanceErrorResponse(ctx, err);
        return storageMaintenanceJobResponse(ctx, 202, snapshot);
    }

    fn checkStorage(self: *AntflyApiHandler, ctx: *httpx.Context) !httpx.Response {
        return self.startStorageMaintenance(ctx, .check);
    }

    fn compactStorage(self: *AntflyApiHandler, ctx: *httpx.Context) !httpx.Response {
        return self.startStorageMaintenance(ctx, .compact);
    }

    fn vacuumStorage(self: *AntflyApiHandler, ctx: *httpx.Context) !httpx.Response {
        return self.startStorageMaintenance(ctx, .vacuum);
    }

    fn storageMaintenanceJobId(ctx: *httpx.Context) !?u64 {
        const path = ctx.request.uri.path;
        if (!std.mem.startsWith(u8, path, admin_routes.maintenance_jobs_prefix)) return null;
        const raw_id = path[admin_routes.maintenance_jobs_prefix.len..];
        if (raw_id.len == 0 or std.mem.indexOfScalar(u8, raw_id, '/') != null) return null;
        return std.fmt.parseUnsigned(u64, raw_id, 10) catch null;
    }

    fn readStorageMaintenanceJob(
        self: *AntflyApiHandler,
        ctx: *httpx.Context,
        cancel: bool,
    ) !httpx.Response {
        var identity: ?AuthenticatedIdentity = null;
        defer if (identity) |*authenticated| authenticated.deinit(self.api_server.alloc);
        if (try self.authorizeStorageMaintenance(ctx, &identity)) |response| return response;
        const job_id = (try storageMaintenanceJobId(ctx)) orelse return textResponse(ctx, 404, "not found");

        const operations = storage_maintenance_operations.Operations.init(self.api_server.cfg.storage_maintenance);
        const request = operationContext(ctx, identity);
        const snapshot = if (cancel)
            operations.cancel(request, .{ .job_id = job_id }) catch |err| return storageMaintenanceErrorResponse(ctx, err)
        else
            operations.get(request, .{ .job_id = job_id }) catch |err| return storageMaintenanceErrorResponse(ctx, err);
        return storageMaintenanceJobResponse(ctx, if (cancel) 202 else 200, snapshot);
    }

    fn getStorageMaintenanceJob(self: *AntflyApiHandler, ctx: *httpx.Context) !httpx.Response {
        return self.readStorageMaintenanceJob(ctx, false);
    }

    fn cancelStorageMaintenanceJob(self: *AntflyApiHandler, ctx: *httpx.Context) !httpx.Response {
        return self.readStorageMaintenanceJob(ctx, true);
    }

    fn internalRepairCancelState(self: *AntflyApiHandler, ctx: *httpx.Context) !httpx.Response {
        const encoded_table_name = ctx.param("table_name") orelse return textResponse(ctx, 400, "invalid path parameter");
        const table_name = (try decodePathParamOrBadRequest(ctx, encoded_table_name)) orelse
            return textResponse(ctx, 400, "invalid path parameter");
        defer ctx.allocator.free(table_name);
        const job_id_raw = ctx.param("job_id") orelse return textResponse(ctx, 400, "invalid repair job id");
        const job_id = std.fmt.parseUnsigned(u64, job_id_raw, 10) catch
            return textResponse(ctx, 400, "invalid repair job id");
        const attempt_id_raw = ctx.param("attempt_id") orelse return textResponse(ctx, 400, "invalid repair attempt id");
        const attempt_id = std.fmt.parseUnsigned(u64, attempt_id_raw, 10) catch
            return textResponse(ctx, 400, "invalid repair attempt id");
        const result = (internal_repair_operations.Operations{ .store = &self.api_server.repair_job_store }).cancelState(
            ctx.allocator,
            operationContext(ctx, null),
            .{ .table_name = table_name, .job_id = job_id, .attempt_id = attempt_id },
        ) catch |err| return switch (err) {
            error.NotFound => textResponse(ctx, 404, "not found"),
            error.Canceled => textResponse(ctx, 408, "request canceled"),
            error.DeadlineExceeded => textResponse(ctx, 504, "request deadline exceeded"),
            error.Internal => textResponse(ctx, 500, "invalid repair job state"),
            else => textResponse(ctx, 500, "internal server error"),
        };
        return ctx.json(result);
    }

    fn internalGroupOperations(self: *AntflyApiHandler) internal_group_operations.Operations {
        return .{
            .reads = self.api_server.table_reads,
            .shard_db_adapter = self.api_server.cfg.shard_db_adapter,
            .writes = self.api_server.table_writes,
            .shard_ops = self.api_server.cfg.shard_ops,
            .batch_validator = .{
                .ptr = self.api_server,
                .validate_fn = struct {
                    fn call(ptr: *anyopaque, table_name: []const u8, writes: []const db_mod.types.BatchWrite) !void {
                        const server: *ApiHttpServer = @ptrCast(@alignCast(ptr));
                        return server.validateTableWritesAgainstSchema(table_name, writes);
                    }
                }.call,
            },
            .reject_unrouted_batch = self.api_server.cfg.routed_raft_batch_writer != null,
            .routed_raft_batch_writer = self.api_server.cfg.routed_raft_batch_writer,
            .txn_validator = .{
                .ptr = self.api_server,
                .validate_fn = struct {
                    fn call(ptr: *anyopaque, table_name: []const u8, writes: []const db_mod.types.TransactionWrite) !void {
                        const server: *ApiHttpServer = @ptrCast(@alignCast(ptr));
                        return server.validateTableWritesAgainstSchema(table_name, writes);
                    }
                }.call,
            },
            .repair_cancellation_lookup = .{
                .ptr = self.api_server,
                .is_requested_fn = struct {
                    fn call(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, job_id: u64, attempt_id: u64, base_uri: ?[]const u8) !bool {
                        const server: *ApiHttpServer = @ptrCast(@alignCast(ptr));
                        if (base_uri) |uri| {
                            const executor = server.cfg.session_executor orelse return error.Unavailable;
                            var client = http_client.ApiHttpClient.init(alloc, executor);
                            return client.fetchTableRepairCancelRequested(uri, table_name, job_id, attempt_id);
                        }
                        const encoded = try server.repair_job_store.loadJobAlloc(alloc, job_id);
                        defer if (encoded) |bytes| alloc.free(bytes);
                        const body = encoded orelse return true;
                        var parsed = try std.json.parseFromSlice(repair_jobs.JobState, alloc, body, .{ .ignore_unknown_fields = true });
                        defer parsed.deinit();
                        return parsed.value.cancel_requested or
                            repair_jobs.isTerminalPhase(parsed.value.phase) or
                            parsed.value.attempt_id != attempt_id;
                    }
                }.call,
            },
        };
    }

    const InternalHttpErrorSpec = struct {
        status: u16,
        message: []const u8,
    };

    /// Classify cross-operation transport semantics once, independently of an
    /// HTTP context. Typed internal operations retain their own error sets;
    /// this is only the shared wire projection at the `httpx` boundary.
    fn sharedInternalHttpErrorSpec(err: anyerror) ?InternalHttpErrorSpec {
        return switch (err) {
            error.DocIdentityNamespaceMismatch => .{
                .status = 409,
                .message = "doc identity namespace mismatch",
            },
            error.HierarchyCursorStale => .{
                .status = 409,
                .message = "HierarchyCursorStale",
            },
            else => null,
        };
    }

    fn internalGroupErrorResponse(ctx: *httpx.Context, err: internal_group_operations.Error) !httpx.Response {
        if (sharedInternalHttpErrorSpec(err)) |spec|
            return textResponse(ctx, spec.status, spec.message);
        return switch (err) {
            error.NotFound => textResponse(ctx, 404, "not found"),
            error.Unsupported => textResponse(ctx, 405, "method not allowed"),
            error.TopologyChanged => textResponse(ctx, 409, "topology changed"),
            error.IdentityReadGenerationChanged => textResponse(ctx, 409, "identity read generation changed"),
            error.StorageReadTemporarilyUnavailable => textResponse(ctx, 503, "storage read temporarily unavailable"),
            error.Canceled => textResponse(ctx, 408, "request canceled"),
            error.DeadlineExceeded => textResponse(ctx, 504, "request deadline exceeded"),
            else => textResponse(ctx, 500, "internal server error"),
        };
    }

    fn internalGroupMedianKey(self: *AntflyApiHandler, ctx: *httpx.Context) !httpx.Response {
        const group_id_raw = ctx.param("group_id") orelse return textResponse(ctx, 400, "invalid group id");
        const group_id = std.fmt.parseUnsigned(u64, group_id_raw, 10) catch
            return textResponse(ctx, 400, "invalid group id");
        const median_key = self.internalGroupOperations().medianKey(
            ctx.allocator,
            operationContext(ctx, null),
            group_id,
        ) catch |err| return internalGroupErrorResponse(ctx, err);
        defer if (median_key) |value| ctx.allocator.free(value);
        return ctx.json(.{ .median_key = median_key });
    }

    fn internalGroupLookup(self: *AntflyApiHandler, ctx: *httpx.Context) !httpx.Response {
        const group_id_raw = ctx.param("group_id") orelse return textResponse(ctx, 400, "invalid group id");
        const group_id = std.fmt.parseUnsigned(u64, group_id_raw, 10) catch
            return textResponse(ctx, 400, "invalid group id");
        const table_name = ctx.param("table_name") orelse return textResponse(ctx, 400, "invalid table name");
        const encoded_key = ctx.param("key") orelse return textResponse(ctx, 400, "invalid path parameter");
        const key = (try decodePathParamOrBadRequest(ctx, encoded_key)) orelse
            return textResponse(ctx, 400, "invalid path parameter");
        defer ctx.allocator.free(key);
        var lookup_options = http_route_helpers.parseLookupOptions(
            ctx.allocator,
            ctx.request.uri.query orelse "",
        ) catch return textResponse(ctx, 400, "invalid lookup options");
        defer lookup_options.deinit(ctx.allocator);
        const consistency = http_server_mod.parseLookupReadConsistency(ctx.request.uri.query orelse "") catch
            return textResponse(ctx, 400, "invalid read consistency");
        var result = self.internalGroupOperations().lookup(
            ctx.allocator,
            operationContext(ctx, null),
            .{
                .group_id = group_id,
                .table_name = table_name,
                .key = key,
                .options = lookup_options.opts,
                .consistency = consistency,
            },
        ) catch |err| return internalGroupErrorResponse(ctx, err);
        defer result.deinit(ctx.allocator);
        var version_buf: [20]u8 = undefined;
        const version = try std.fmt.bufPrint(&version_buf, "{d}", .{result.version});
        try ctx.setHeader("X-Antfly-Version", version);
        return jsonResponse(ctx, 200, result.json);
    }

    fn internalJoinJobState(self: *AntflyApiHandler, ctx: *httpx.Context) !httpx.Response {
        const body = (try ctx.body()) orelse return textResponse(ctx, 400, "invalid join job state request");
        var input = std.json.parseFromSlice(
            @import("distributed_join.zig").EncodedJoinJobStateRequest,
            ctx.allocator,
            body,
            .{ .ignore_unknown_fields = true },
        ) catch return textResponse(ctx, 400, "invalid join job state request");
        defer input.deinit();
        var result = (internal_join_operations.Operations{ .job_store = &self.api_server.join_job_store }).jobState(
            ctx.allocator,
            operationContext(ctx, null),
            input.value.job_id,
        ) catch |err| return switch (err) {
            error.NotFound => textResponse(ctx, 404, "not found"),
            error.Canceled => textResponse(ctx, 408, "request canceled"),
            error.DeadlineExceeded => textResponse(ctx, 504, "request deadline exceeded"),
            else => textResponse(ctx, 500, "internal server error"),
        };
        defer result.deinit();
        return ctx.json(result.parsed.value);
    }

    fn internalJoinOperations(self: *AntflyApiHandler) internal_join_operations.Operations {
        return .{
            .job_store = &self.api_server.join_job_store,
            .join_context = self.api_server.joinContext(),
            .reads = self.api_server.table_reads,
        };
    }

    const InternalGroupTableParams = struct {
        group_id: u64,
        table_name: []u8,

        fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
            alloc.free(self.table_name);
        }
    };

    fn internalGroupTableParams(ctx: *httpx.Context) !?InternalGroupTableParams {
        const group_id_raw = ctx.param("group_id") orelse return null;
        const group_id = std.fmt.parseUnsigned(u64, group_id_raw, 10) catch return null;
        const encoded_table_name = ctx.param("table_name") orelse return null;
        const table_name = (try decodePathParamOrBadRequest(ctx, encoded_table_name)) orelse return null;
        return .{ .group_id = group_id, .table_name = table_name };
    }

    fn internalJoinErrorResponse(ctx: *httpx.Context, err: internal_join_operations.Error, invalid_message: []const u8) !httpx.Response {
        if (sharedInternalHttpErrorSpec(err)) |spec|
            return textResponse(ctx, spec.status, spec.message);
        return switch (err) {
            error.InvalidQueryRequest, error.UnsupportedQueryRequest => textResponse(ctx, 400, invalid_message),
            error.NotFound => textResponse(ctx, 404, "not found"),
            error.Timeout, error.Canceled => textResponse(ctx, 408, "query timeout"),
            error.TopologyChanged => textResponse(ctx, 409, "topology changed"),
            error.DeadlineExceeded => textResponse(ctx, 504, "request deadline exceeded"),
            error.Unavailable => textResponse(ctx, 503, "join unavailable"),
            else => textResponse(ctx, 500, "internal server error"),
        };
    }

    fn internalJoinFinalize(self: *AntflyApiHandler, ctx: *httpx.Context) !httpx.Response {
        var params = (try internalGroupTableParams(ctx)) orelse return textResponse(ctx, 400, "invalid path parameter");
        defer params.deinit(ctx.allocator);
        const body = (try ctx.body()) orelse return textResponse(ctx, 400, "invalid join finalize request");
        var input = @import("distributed_join.zig").parseJoinFinalizeRequest(ctx.allocator, body) catch
            return textResponse(ctx, 400, "invalid join finalize request");
        defer input.deinit(ctx.allocator);
        var result = self.internalJoinOperations().finalize(
            ctx.allocator,
            operationContext(ctx, null),
            params.group_id,
            params.table_name,
            input,
        ) catch |err| return internalJoinErrorResponse(ctx, err, "invalid join finalize request");
        defer result.deinit(ctx.allocator);
        const encoded = try self.api_server.join_job_store.encodeJoinPartitionResponse(ctx.allocator, result);
        defer ctx.allocator.free(encoded);
        return jsonResponse(ctx, 200, encoded);
    }

    fn internalJoinRows(self: *AntflyApiHandler, ctx: *httpx.Context) !httpx.Response {
        var params = (try internalGroupTableParams(ctx)) orelse return textResponse(ctx, 400, "invalid path parameter");
        defer params.deinit(ctx.allocator);
        const body = (try ctx.body()) orelse return textResponse(ctx, 400, "invalid join rows request");
        var input = @import("distributed_join.zig").parseJoinRowsRequest(ctx.allocator, body) catch
            return textResponse(ctx, 400, "invalid join rows request");
        defer input.deinit(ctx.allocator);
        const hits = self.internalJoinOperations().rows(
            ctx.allocator,
            operationContext(ctx, null),
            params.group_id,
            params.table_name,
            input,
        ) catch |err| return internalJoinErrorResponse(ctx, err, "invalid join rows request");
        defer {
            for (hits) |*hit| @import("distributed_join.zig").deinitJsonValue(ctx.allocator, hit);
            if (hits.len > 0) ctx.allocator.free(hits);
        }
        const encoded = try @import("distributed_join.zig").encodeJoinRowsResponse(ctx.allocator, hits);
        defer ctx.allocator.free(encoded);
        return jsonResponse(ctx, 200, encoded);
    }

    fn internalJoinUnmatched(self: *AntflyApiHandler, ctx: *httpx.Context) !httpx.Response {
        var params = (try internalGroupTableParams(ctx)) orelse return textResponse(ctx, 400, "invalid path parameter");
        defer params.deinit(ctx.allocator);
        const body = (try ctx.body()) orelse return textResponse(ctx, 400, "invalid join unmatched request");
        var input = @import("distributed_join.zig").parseJoinUnmatchedRequest(ctx.allocator, body) catch
            return textResponse(ctx, 400, "invalid join unmatched request");
        defer input.deinit(ctx.allocator);
        const result = self.internalJoinOperations().unmatched(
            ctx.allocator,
            operationContext(ctx, null),
            params.group_id,
            params.table_name,
            input,
        ) catch |err| return internalJoinErrorResponse(ctx, err, "invalid join unmatched request");
        defer {
            for (result.hits) |*hit| @import("distributed_join.zig").deinitJsonValue(ctx.allocator, @constCast(hit));
            if (result.hits.len > 0) ctx.allocator.free(@constCast(result.hits));
        }
        const encoded = try @import("distributed_join.zig").encodeJoinUnmatchedResponse(ctx.allocator, result.hits, result.right_rows_scanned);
        defer ctx.allocator.free(encoded);
        return jsonResponse(ctx, 200, encoded);
    }

    fn internalJoinPartition(self: *AntflyApiHandler, ctx: *httpx.Context) !httpx.Response {
        var params = (try internalGroupTableParams(ctx)) orelse return textResponse(ctx, 400, "invalid path parameter");
        defer params.deinit(ctx.allocator);
        const body = (try ctx.body()) orelse return textResponse(ctx, 400, "invalid join partition request");
        var input = @import("distributed_join.zig").parseJoinPartitionRequest(ctx.allocator, body) catch
            return textResponse(ctx, 400, "invalid join partition request");
        defer input.deinit(ctx.allocator);
        var result = self.internalJoinOperations().partition(
            ctx.allocator,
            operationContext(ctx, null),
            params.group_id,
            params.table_name,
            input,
        ) catch |err| return internalJoinErrorResponse(ctx, err, "invalid join partition request");
        defer result.deinit(ctx.allocator);
        const encoded = try self.api_server.join_job_store.encodeJoinPartitionResponse(ctx.allocator, result);
        defer ctx.allocator.free(encoded);
        return jsonResponse(ctx, 200, encoded);
    }

    fn internalCorruptEmbeddingArtifact(self: *AntflyApiHandler, ctx: *httpx.Context) !httpx.Response {
        const encoded_table_name = ctx.param("table_name") orelse return textResponse(ctx, 400, "invalid path parameter");
        const table_name = (try decodePathParamOrBadRequest(ctx, encoded_table_name)) orelse
            return textResponse(ctx, 400, "invalid path parameter");
        defer ctx.allocator.free(table_name);
        const body = (try ctx.body()) orelse return textResponse(ctx, 400, "invalid corrupt embedding artifact request");
        const WireRequest = struct { doc_key: []const u8, index_name: []const u8 };
        var input = std.json.parseFromSlice(WireRequest, ctx.allocator, body, .{ .allocate = .alloc_always }) catch
            return textResponse(ctx, 400, "invalid corrupt embedding artifact request");
        defer input.deinit();
        self.internalGroupOperations().corruptEmbeddingArtifact(
            ctx.allocator,
            operationContext(ctx, null),
            table_name,
            input.value.doc_key,
            input.value.index_name,
        ) catch |err| return internalGroupErrorResponse(ctx, err);
        return ctx.json(struct {}{});
    }

    const InternalDocumentArtifactParams = struct {
        group_id: u64,
        table_name: []u8,
        key: []u8,
        artifact_name: []u8,

        fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
            alloc.free(self.table_name);
            alloc.free(self.key);
            alloc.free(self.artifact_name);
            self.* = undefined;
        }
    };

    fn internalDocumentArtifactParams(ctx: *httpx.Context) !?InternalDocumentArtifactParams {
        const group_id = (try internalGroupId(ctx)) orelse return null;
        const table_name = (try decodePathParamOrBadRequest(ctx, ctx.param("table_name") orelse return null)) orelse return null;
        errdefer ctx.allocator.free(table_name);
        const key = (try decodePathParamOrBadRequest(ctx, ctx.param("key") orelse return null)) orelse return null;
        errdefer ctx.allocator.free(key);
        const artifact_name = (try decodePathParamOrBadRequest(ctx, ctx.param("artifact_name") orelse return null)) orelse return null;
        return .{ .group_id = group_id, .table_name = table_name, .key = key, .artifact_name = artifact_name };
    }

    fn internalArtifactWriteErrorResponse(ctx: *httpx.Context, err: internal_group_operations.Error, invalid_message: []const u8) !httpx.Response {
        return switch (err) {
            error.InvalidArgument => textResponse(ctx, 400, invalid_message),
            error.DocIdentityNamespaceMismatch => textResponse(ctx, 409, "doc identity namespace mismatch"),
            error.Unsupported => textResponse(ctx, 405, "method not allowed"),
            error.NotFound => textResponse(ctx, 404, "not found"),
            error.Canceled => textResponse(ctx, 408, "request canceled"),
            error.DeadlineExceeded => textResponse(ctx, 504, "request deadline exceeded"),
            else => textResponse(ctx, 500, "internal server error"),
        };
    }

    fn internalDocumentArtifactPlacementUpdate(self: *AntflyApiHandler, ctx: *httpx.Context) !httpx.Response {
        var params = (try internalDocumentArtifactParams(ctx)) orelse return textResponse(ctx, 400, "invalid path parameter");
        defer params.deinit(ctx.allocator);
        const body = (try ctx.body()) orelse return textResponse(ctx, 400, "invalid document artifact placement request");
        var input = std.json.parseFromSlice(db_mod.types.DocumentArtifactChildRangePlacementUpdate, ctx.allocator, body, .{ .allocate = .alloc_always }) catch
            return textResponse(ctx, 400, "invalid document artifact placement request");
        defer input.deinit();
        self.internalGroupOperations().updateDocumentArtifactChildRangePlacement(
            ctx.allocator,
            operationContext(ctx, null),
            params.group_id,
            params.table_name,
            params.key,
            params.artifact_name,
            input.value,
        ) catch |err| return internalArtifactWriteErrorResponse(ctx, err, "invalid document artifact placement request");
        return ctx.json(.{ .placement = "updated" });
    }

    fn internalDocumentArtifactManifests(self: *AntflyApiHandler, ctx: *httpx.Context) !httpx.Response {
        const group_id = (try internalGroupId(ctx)) orelse return textResponse(ctx, 400, "invalid path parameter");
        const table_name = (try decodePathParamOrBadRequest(ctx, ctx.param("table_name") orelse return textResponse(ctx, 400, "invalid path parameter"))) orelse
            return textResponse(ctx, 400, "invalid path parameter");
        defer ctx.allocator.free(table_name);
        const key = (try decodePathParamOrBadRequest(ctx, ctx.param("key") orelse return textResponse(ctx, 400, "invalid path parameter"))) orelse
            return textResponse(ctx, 400, "invalid path parameter");
        defer ctx.allocator.free(key);
        var result = self.internalGroupOperations().documentArtifactManifests(
            ctx.allocator,
            operationContext(ctx, null),
            group_id,
            table_name,
            key,
        ) catch |err| return internalGroupErrorResponse(ctx, err);
        defer result.deinit(ctx.allocator);
        return ctx.json(result);
    }

    fn internalDocumentArtifactManifest(self: *AntflyApiHandler, ctx: *httpx.Context) !httpx.Response {
        var params = (try internalDocumentArtifactParams(ctx)) orelse return textResponse(ctx, 400, "invalid path parameter");
        defer params.deinit(ctx.allocator);
        var result = self.internalGroupOperations().documentArtifactManifest(
            ctx.allocator,
            operationContext(ctx, null),
            params.group_id,
            params.table_name,
            params.key,
            params.artifact_name,
        ) catch |err| return internalGroupErrorResponse(ctx, err);
        defer result.deinit(ctx.allocator);
        return ctx.json(result);
    }

    fn internalDocumentArtifactChildRangeBatch(self: *AntflyApiHandler, ctx: *httpx.Context) !httpx.Response {
        var params = (try internalDocumentArtifactParams(ctx)) orelse return textResponse(ctx, 400, "invalid path parameter");
        defer params.deinit(ctx.allocator);
        const body = (try ctx.body()) orelse return textResponse(ctx, 400, "invalid document artifact child range batch request");
        var input = std.json.parseFromSlice(db_mod.DocumentArtifactChildRangeApplyBatch, ctx.allocator, body, .{ .allocate = .alloc_always }) catch
            return textResponse(ctx, 400, "invalid document artifact child range batch request");
        defer input.deinit();
        const sequence = self.internalGroupOperations().applyDocumentArtifactChildRangeBatch(
            ctx.allocator,
            operationContext(ctx, null),
            params.group_id,
            params.table_name,
            params.key,
            params.artifact_name,
            input.value,
        ) catch |err| return internalArtifactWriteErrorResponse(ctx, err, "invalid document artifact child range batch request");
        return ctx.json(.{ .sequence = sequence });
    }

    fn internalDocumentArtifactReprocess(self: *AntflyApiHandler, ctx: *httpx.Context) !httpx.Response {
        var params = (try internalDocumentArtifactParams(ctx)) orelse return textResponse(ctx, 400, "invalid path parameter");
        defer params.deinit(ctx.allocator);
        self.internalGroupOperations().reprocessDocumentArtifact(
            ctx.allocator,
            operationContext(ctx, null),
            params.group_id,
            params.table_name,
            params.key,
            params.artifact_name,
        ) catch |err| return internalArtifactWriteErrorResponse(ctx, err, "invalid document artifact reprocess request");
        return ctx.json(.{ .reprocess = "triggered" });
    }

    fn internalTableArtifactReprocess(self: *AntflyApiHandler, ctx: *httpx.Context) !httpx.Response {
        var params = (try internalGroupTableParams(ctx)) orelse return textResponse(ctx, 400, "invalid path parameter");
        defer params.deinit(ctx.allocator);
        const artifact_name = (try decodePathParamOrBadRequest(ctx, ctx.param("artifact_name") orelse return textResponse(ctx, 400, "invalid path parameter"))) orelse
            return textResponse(ctx, 400, "invalid path parameter");
        defer ctx.allocator.free(artifact_name);
        const body = (try ctx.body()) orelse "{}";
        var input = std.json.parseFromSlice(db_mod.types.DocumentArtifactTableReprocessRequest, ctx.allocator, if (body.len > 0) body else "{}", .{ .allocate = .alloc_always }) catch
            return textResponse(ctx, 400, "invalid document artifact reprocess request");
        defer input.deinit();
        var result = self.internalGroupOperations().reprocessDocumentArtifactRange(
            ctx.allocator,
            operationContext(ctx, null),
            params.group_id,
            params.table_name,
            artifact_name,
            input.value,
        ) catch |err| return internalArtifactWriteErrorResponse(ctx, err, "invalid document artifact reprocess request");
        defer result.deinit(ctx.allocator);

        const FailureResponse = struct { key: []const u8, error_code: []const u8 };
        const ShardCursorResponse = struct {
            group_id: ?u64,
            next_key: []const u8,
            scanned: usize,
            reprocessed: usize,
            skipped: usize,
            failed: usize,
            limit: u32,
        };
        const failures = try ctx.allocator.alloc(FailureResponse, result.failures.len);
        defer ctx.allocator.free(failures);
        for (result.failures, failures) |failure, *out| out.* = .{ .key = failure.key, .error_code = failure.error_code };
        const shard_cursors = try ctx.allocator.alloc(ShardCursorResponse, result.shard_cursors.len);
        defer ctx.allocator.free(shard_cursors);
        for (result.shard_cursors, shard_cursors) |cursor, *out| out.* = .{
            .group_id = cursor.group_id,
            .next_key = cursor.next_key,
            .scanned = cursor.scanned,
            .reprocessed = cursor.reprocessed,
            .skipped = cursor.skipped,
            .failed = cursor.failed,
            .limit = cursor.limit,
        };
        const pending_shards = if (result.shard_cursors.len > 0) result.shard_cursors.len else if (result.next_key != null) @as(usize, 1) else 0;
        _ = ctx.status(202);
        return ctx.json(.{
            .reprocess = "triggered",
            .reprocess_status = if (pending_shards == 0) "complete" else "in_progress",
            .artifact_name = artifact_name,
            .scanned = result.scanned,
            .reprocessed = result.reprocessed,
            .skipped = result.skipped,
            .failed = result.failed,
            .limit = result.limit,
            .next_key = result.next_key,
            .pending_shards = pending_shards,
            .failures = failures,
            .shard_cursors = shard_cursors,
        });
    }

    fn internalArtifactRepairList(self: *AntflyApiHandler, ctx: *httpx.Context) !httpx.Response {
        var params = (try internalGroupTableParams(ctx)) orelse return textResponse(ctx, 400, "invalid path parameter");
        defer params.deinit(ctx.allocator);
        const body = (try ctx.body()) orelse "{}";
        var input = std.json.parseFromSlice(db_mod.types.ArtifactRepairListRequest, ctx.allocator, if (body.len > 0) body else "{}", .{ .allocate = .alloc_always }) catch
            return textResponse(ctx, 400, "invalid artifact repair list request");
        defer input.deinit();
        var result = self.internalGroupOperations().listArtifactRepairIssues(
            ctx.allocator,
            operationContext(ctx, null),
            params.group_id,
            params.table_name,
            input.value,
        ) catch |err| return internalArtifactWriteErrorResponse(ctx, err, "invalid artifact repair list request");
        defer result.deinit(ctx.allocator);
        return ctx.json(result);
    }

    fn internalArtifactRepairRun(self: *AntflyApiHandler, ctx: *httpx.Context) !httpx.Response {
        var params = (try internalGroupTableParams(ctx)) orelse return textResponse(ctx, 400, "invalid path parameter");
        defer params.deinit(ctx.allocator);
        const body = (try ctx.body()) orelse "{}";
        var input = std.json.parseFromSlice(db_mod.types.ArtifactRepairRunRequest, ctx.allocator, if (body.len > 0) body else "{}", .{ .allocate = .alloc_always }) catch
            return textResponse(ctx, 400, "invalid artifact repair request");
        defer input.deinit();
        var result = self.internalGroupOperations().repairArtifactIssues(
            ctx.allocator,
            operationContext(ctx, null),
            params.group_id,
            params.table_name,
            input.value,
        ) catch |err| return switch (err) {
            error.InvalidRepairCancelToken => textResponse(ctx, 400, "invalid repair cancel token"),
            error.RepairCanceled => textResponse(ctx, 409, "repair cancelled"),
            error.Unavailable => textResponse(ctx, 503, "repair cancel unavailable"),
            else => internalArtifactWriteErrorResponse(ctx, err, "invalid artifact repair request"),
        };
        defer result.deinit(ctx.allocator);
        _ = ctx.status(202);
        return ctx.json(result);
    }

    fn internalGroupId(ctx: *httpx.Context) !?u64 {
        const raw = ctx.param("group_id") orelse return null;
        return std.fmt.parseUnsigned(u64, raw, 10) catch null;
    }

    fn internalTransitionErrorResponse(
        ctx: *httpx.Context,
        err: internal_group_operations.Error,
        mismatch_message: []const u8,
    ) !httpx.Response {
        return switch (err) {
            error.InvalidArgument => textResponse(ctx, 400, mismatch_message),
            error.NotFound => textResponse(ctx, 404, "not found"),
            error.TopologyChanged => textResponse(ctx, 409, "topology changed"),
            error.DocIdentityNamespaceMismatch => textResponse(ctx, 409, "doc identity namespace mismatch"),
            error.GroupLeaderUnavailable => textResponse(ctx, 503, "group leader unavailable"),
            error.Unsupported => textResponse(ctx, 405, "method not allowed"),
            error.Canceled => textResponse(ctx, 408, "request canceled"),
            error.DeadlineExceeded => textResponse(ctx, 504, "request deadline exceeded"),
            else => textResponse(ctx, 500, "internal server error"),
        };
    }

    fn internalObserveSplit(self: *AntflyApiHandler, ctx: *httpx.Context) !httpx.Response {
        const group_id = (try internalGroupId(ctx)) orelse return textResponse(ctx, 400, "invalid group id");
        const body = (try ctx.body()) orelse return textResponse(ctx, 400, "invalid split transition request");
        var record = @import("internal_transition_wire.zig").parseSplitTransitionRecord(ctx.allocator, body) catch
            return textResponse(ctx, 400, "invalid split transition request");
        defer @import("internal_transition_wire.zig").freeSplitTransitionRecordOwned(ctx.allocator, &record);
        const observation = self.internalGroupOperations().observeSplit(
            operationContext(ctx, null),
            group_id,
            record,
        ) catch |err| return internalTransitionErrorResponse(ctx, err, "group does not match transition");
        return ctx.json(observation);
    }

    fn internalObserveMerge(self: *AntflyApiHandler, ctx: *httpx.Context) !httpx.Response {
        const group_id = (try internalGroupId(ctx)) orelse return textResponse(ctx, 400, "invalid group id");
        const body = (try ctx.body()) orelse return textResponse(ctx, 400, "invalid merge transition request");
        var record = @import("internal_transition_wire.zig").parseMergeTransitionRecord(ctx.allocator, body) catch
            return textResponse(ctx, 400, "invalid merge transition request");
        defer @import("internal_transition_wire.zig").freeMergeTransitionRecordOwned(ctx.allocator, &record);
        const observation = self.internalGroupOperations().observeMerge(
            operationContext(ctx, null),
            group_id,
            record,
        ) catch |err| return internalTransitionErrorResponse(ctx, err, "group does not match transition");
        return ctx.json(observation);
    }

    fn internalExecuteTransition(self: *AntflyApiHandler, ctx: *httpx.Context) !httpx.Response {
        const group_id = (try internalGroupId(ctx)) orelse return textResponse(ctx, 400, "invalid group id");
        const body = (try ctx.body()) orelse return textResponse(ctx, 400, "invalid transition action request");
        var action = @import("internal_transition_wire.zig").parseTransitionAction(ctx.allocator, body) catch
            return textResponse(ctx, 400, "invalid transition action request");
        defer @import("internal_transition_wire.zig").freeTransitionActionOwned(ctx.allocator, &action);
        self.internalGroupOperations().executeTransition(
            operationContext(ctx, null),
            group_id,
            action,
        ) catch |err| return internalTransitionErrorResponse(ctx, err, "group does not match transition action");
        return ctx.json(struct {}{});
    }

    fn internalGroupBatch(self: *AntflyApiHandler, ctx: *httpx.Context) !httpx.Response {
        var params = (try internalGroupTableParams(ctx)) orelse return textResponse(ctx, 400, "invalid path parameter");
        defer params.deinit(ctx.allocator);
        if (self.api_server.cfg.routed_raft_batch_writer != null)
            return textResponse(ctx, 404, "legacy raft batch forwarding unsupported");
        const forwarding = internal_batch_forwarding.parseValues(
            ctx.header(internal_batch_forwarding.remaining_ms_header),
            ctx.header(internal_batch_forwarding.forwards_remaining_header),
            ctx.header(internal_batch_forwarding.campaign_allowed_header),
        ) catch return textResponse(ctx, 400, "invalid raft batch forwarding headers");
        if (forwarding != null)
            return textResponse(ctx, 400, "raft batch forwarding headers require routed endpoint");
        const body = (try ctx.body()) orelse "";
        var input = batch_api.parseInternalBatchRequest(ctx.allocator, body) catch |err| return switch (err) {
            error.InvalidBatchRequest => textResponse(ctx, 400, "invalid batch request"),
            error.ValueTooLong => textResponse(ctx, 413, "value too large"),
            else => textResponse(ctx, 500, "internal server error"),
        };
        defer input.deinit(ctx.allocator);
        const result = self.internalGroupOperations().batch(
            ctx.allocator,
            operationContext(ctx, null),
            params.group_id,
            params.table_name,
            input.req,
        ) catch |err| return switch (err) {
            error.InvalidArgument => textResponse(ctx, 400, "invalid batch request"),
            error.DocIdentityNamespaceMismatch => textResponse(ctx, 409, "doc identity namespace mismatch"),
            error.EnrichmentWaitCanceled,
            error.EnrichmentWaitTimeout,
            error.EnrichmentRetryInProgress,
            => blk: {
                try ctx.setHeader(internal_batch_forwarding.outcome_header, internal_batch_forwarding.outcome_committed_visibility_pending_v1);
                break :blk textResponse(ctx, 202, internal_batch_forwarding.committed_visibility_pending_body);
            },
            error.EnrichmentWorkerFailed => blk: {
                try ctx.setHeader(internal_batch_forwarding.outcome_header, internal_batch_forwarding.outcome_committed_repair_required_v1);
                break :blk textResponse(ctx, 202, internal_batch_forwarding.committed_repair_required_body);
            },
            error.RaftBatchWriteOutcomeUnknown => blk: {
                try ctx.setHeader(internal_batch_forwarding.outcome_header, internal_batch_forwarding.outcome_unknown_v1);
                break :blk textResponse(ctx, 409, "write outcome unknown");
            },
            error.GroupLeaderUnavailable => blk: {
                try ctx.setHeader(internal_batch_forwarding.outcome_header, internal_batch_forwarding.outcome_not_proposed_v1);
                break :blk textResponse(ctx, 503, "group leader unavailable");
            },
            error.Unsupported => textResponse(ctx, 404, "legacy raft batch forwarding unsupported"),
            error.NotFound => textResponse(ctx, 404, "not found"),
            error.Canceled => textResponse(ctx, 408, "request canceled"),
            error.DeadlineExceeded => textResponse(ctx, 504, "request deadline exceeded"),
            else => textResponse(ctx, 500, "internal server error"),
        };
        _ = ctx.status(201);
        return ctx.json(.{
            .inserted = result.inserted,
            .deleted = result.deleted,
            .transformed = result.transformed,
        });
    }

    fn internalCapabilities(_: *AntflyApiHandler, ctx: *httpx.Context) !httpx.Response {
        return ctx.json(.{
            .data_raft_batch_protocol_version = internal_batch_forwarding.raft_batch_protocol_version,
        });
    }

    fn internalQueryContext(self: *AntflyApiHandler, ctx: *httpx.Context) internal_query_operations.Context {
        var result = self.api_server.internalQueryContext();
        if (result.query_planning) |*planning| {
            planning.query_cancellation = operationContext(ctx, null).cancellation;
        }
        return result;
    }

    fn internalQueryPlanningErrorResponse(ctx: *httpx.Context, err: anyerror) !httpx.Response {
        if (err == error.InvalidQueryRequest or err == error.UnsupportedQueryRequest)
            return textResponse(ctx, 400, @errorName(err));
        const normalized = internal_query_operations.normalizeQueryEmbeddingOperationalError(err) orelse return err;
        return switch (normalized) {
            error.QueryEmbeddingInputTooLarge => textResponse(ctx, 413, "query embedding input too large"),
            error.QueryEmbeddingOverloaded => textResponse(ctx, 429, "query embedding overloaded"),
            error.EmbedRateLimited => textResponse(ctx, 429, "query embedding rate limited"),
            error.EmbedTransientFailure => textResponse(ctx, 503, "query embedding temporarily unavailable"),
            error.EmbedUpstreamFailure => textResponse(ctx, 502, "query embedding provider failed"),
            error.Timeout => textResponse(ctx, 504, "query embedding timed out"),
            else => unreachable,
        };
    }

    fn routeInternalQuery(
        query_context: internal_query_operations.Context,
        ctx: *httpx.Context,
        table_name: []const u8,
        request: *db_mod.types.SearchRequest,
    ) !?httpx.Response {
        query_context.routeQuery(table_name, request) catch |err| {
            const response = switch (err) {
                error.TableNotFound => try textResponse(ctx, 404, @errorName(err)),
                error.InvalidSchemaUpdateRequest, error.InvalidTableIndexMetadata => try textResponse(ctx, 500, "invalid table metadata"),
                else => return err,
            };
            return @as(?httpx.Response, response);
        };
        return null;
    }

    fn internalQueryResponse(ctx: *httpx.Context, result: *const query_api.QueryResponse) !httpx.Response {
        var arena_impl = std.heap.ArenaAllocator.init(ctx.allocator);
        defer arena_impl.deinit();
        const response = try std.json.parseFromSliceLeaky(metadata_openapi.QueryResponses, arena_impl.allocator(), result.json, .{
            .allocate = .alloc_always,
        });
        if (result.identity_read_generation) |generation| {
            var buf: [32]u8 = undefined;
            const value = try std.fmt.bufPrint(&buf, "{d}", .{generation});
            try ctx.setHeader(query_api.QueryResponse.identity_read_generation_header, value);
        }
        return ctx.json(response);
    }

    fn internalGroupQuery(self: *AntflyApiHandler, ctx: *httpx.Context) !httpx.Response {
        var params = (try internalGroupTableParams(ctx)) orelse return textResponse(ctx, 400, "invalid path parameter");
        defer params.deinit(ctx.allocator);
        const body = (try ctx.body()) orelse "";
        const query_context = self.internalQueryContext(ctx);
        var query_request = query_context.planQuery(ctx.allocator, params.table_name, body) catch |err|
            return internalQueryPlanningErrorResponse(ctx, err);
        defer query_request.deinit(ctx.allocator);
        query_request.req.cancellation = operationContext(ctx, null).cancellation;
        if (try routeInternalQuery(query_context, ctx, params.table_name, &query_request.req)) |response| return response;
        var result = self.internalGroupOperations().query(
            ctx.allocator,
            operationContext(ctx, null),
            params.group_id,
            params.table_name,
            query_request.req,
        ) catch |err| return if (err == error.InvalidArgument)
            textResponse(ctx, 400, @errorName(err))
        else
            internalGroupErrorResponse(ctx, err);
        defer result.deinit(ctx.allocator);
        return internalQueryResponse(ctx, &result);
    }

    const InternalQueryPreflightWire = struct {
        query_request: std.json.Value,
        max_work: u32 = 0,
    };

    fn parseInternalQueryPreflight(
        alloc: std.mem.Allocator,
        body: []const u8,
    ) !struct { query_request_body: []u8, max_work: u32 } {
        var parsed = std.json.parseFromSlice(InternalQueryPreflightWire, alloc, body, .{ .allocate = .alloc_always }) catch {
            return .{
                .query_request_body = try alloc.dupe(u8, body),
                .max_work = 0,
            };
        };
        defer parsed.deinit();
        return .{
            .query_request_body = try std.json.Stringify.valueAlloc(alloc, parsed.value.query_request, .{}),
            .max_work = parsed.value.max_work,
        };
    }

    fn internalGroupQueryPreflight(self: *AntflyApiHandler, ctx: *httpx.Context) !httpx.Response {
        var params = (try internalGroupTableParams(ctx)) orelse return textResponse(ctx, 400, "invalid path parameter");
        defer params.deinit(ctx.allocator);
        const body = (try ctx.body()) orelse "";
        const preflight = try parseInternalQueryPreflight(ctx.allocator, body);
        defer ctx.allocator.free(preflight.query_request_body);
        const query_context = self.internalQueryContext(ctx);
        var query_request = query_context.planQuery(ctx.allocator, params.table_name, preflight.query_request_body) catch |err|
            return internalQueryPlanningErrorResponse(ctx, err);
        defer query_request.deinit(ctx.allocator);
        query_request.req.cancellation = operationContext(ctx, null).cancellation;
        if (try routeInternalQuery(query_context, ctx, params.table_name, &query_request.req)) |response| return response;
        var summary = self.internalGroupOperations().queryPreflight(
            ctx.allocator,
            operationContext(ctx, null),
            params.group_id,
            params.table_name,
            query_request.req,
            preflight.max_work,
        ) catch |err| return if (err == error.InvalidArgument)
            textResponse(ctx, 400, @errorName(err))
        else
            internalGroupErrorResponse(ctx, err);
        defer summary.deinit(ctx.allocator);
        return ctx.json(summary);
    }

    fn internalGroupVectorWorker(self: *AntflyApiHandler, ctx: *httpx.Context) !httpx.Response {
        var params = (try internalGroupTableParams(ctx)) orelse return textResponse(ctx, 400, "invalid path parameter");
        defer params.deinit(ctx.allocator);
        const body = (try ctx.body()) orelse "";
        var envelope = query_contract.parseAlgebraicVectorWorkerRequestEnvelopeAlloc(ctx.allocator, body) catch |err| switch (err) {
            error.OutOfMemory => return err,
            else => return textResponse(ctx, 400, "invalid vector worker request"),
        };
        defer envelope.deinit(ctx.allocator);
        var query_request = query_contract.searchRequestFromVectorWorkerEnvelope(&envelope);
        defer if (query_request.primary_text_index_name) |index_name| ctx.allocator.free(index_name);
        query_request.cancellation = operationContext(ctx, null).cancellation;
        const query_context = self.internalQueryContext(ctx);
        if (try routeInternalQuery(query_context, ctx, params.table_name, &query_request)) |response| return response;
        var result = self.internalGroupOperations().query(
            ctx.allocator,
            operationContext(ctx, null),
            params.group_id,
            params.table_name,
            query_request,
        ) catch |err| return if (err == error.InvalidArgument)
            textResponse(ctx, 400, @errorName(err))
        else
            internalGroupErrorResponse(ctx, err);
        defer result.deinit(ctx.allocator);
        return internalQueryResponse(ctx, &result);
    }

    fn internalRetrieval(self: *AntflyApiHandler, ctx: *httpx.Context) !httpx.Response {
        const body = (try ctx.body()) orelse "";
        const response = self.api_server.executeInternalRetrieval(body) catch |err| switch (err) {
            error.TreeRootSetTooLarge => return textResponse(ctx, 422, "tree root set exceeds the bounded retrieval limit"),
            error.InvalidRetrievalAgentRequest, error.UnsupportedRetrievalAgentRequest => return textResponse(ctx, 400, "invalid retrieval agent request"),
            error.TableNotFound => return textResponse(ctx, 404, "not found"),
            error.DocIdentityNamespaceMismatch => return textResponse(ctx, 503, "doc identity unavailable"),
            else => {
                if (try respondQueryEmbeddingOperationalError(ctx, err)) |operational_response| return operational_response;
                std.log.err("internal retrieval failed err={}", .{err});
                return err;
            },
        };
        defer self.api_server.alloc.free(response.body);
        if (std.mem.eql(u8, response.content_type, "text/event-stream")) {
            _ = ctx.status(200);
            try ctx.setHeader("content-type", "text/event-stream");
            _ = ctx.response.body(response.body);
            return ctx.response.build();
        }
        var arena_impl = std.heap.ArenaAllocator.init(ctx.allocator);
        defer arena_impl.deinit();
        const value = try std.json.parseFromSliceLeaky(metadata_openapi.RetrievalAgentResult, arena_impl.allocator(), response.body, .{
            .allocate = .alloc_always,
        });
        return ctx.json(value);
    }

    fn internalGroupScan(self: *AntflyApiHandler, ctx: *httpx.Context) !httpx.Response {
        var params = (try internalGroupTableParams(ctx)) orelse return textResponse(ctx, 400, "invalid path parameter");
        defer params.deinit(ctx.allocator);
        const body = (try ctx.body()) orelse "";
        var input = http_route_helpers.parseScanKeysRequest(ctx.allocator, body) catch |err| return switch (err) {
            error.InvalidQueryRequest, error.UnsupportedQueryRequest => textResponse(ctx, 400, "invalid scan request"),
            else => err,
        };
        defer input.deinit(ctx.allocator);
        var result = self.internalGroupOperations().scan(
            ctx.allocator,
            operationContext(ctx, null),
            params.group_id,
            params.table_name,
            input.from,
            input.to,
            input.opts,
        ) catch |err| return internalGroupErrorResponse(ctx, err);
        defer result.deinit(ctx.allocator);
        _ = ctx.status(200);
        try ctx.setHeader("content-type", "application/x-ndjson");
        _ = ctx.response.body(result.ndjson);
        return ctx.response.build();
    }

    fn internalGraphExpand(self: *AntflyApiHandler, ctx: *httpx.Context) !httpx.Response {
        var params = (try internalGroupTableParams(ctx)) orelse return textResponse(ctx, 400, "invalid path parameter");
        defer params.deinit(ctx.allocator);
        const body = (try ctx.body()) orelse return textResponse(ctx, 400, "invalid graph expand request");
        var input = distributed_graph.parseGraphExpandRequest(ctx.allocator, body) catch return textResponse(ctx, 400, "invalid graph expand request");
        defer input.deinit(ctx.allocator);
        var result = self.internalGroupOperations().graphExpand(ctx.allocator, operationContext(ctx, null), params.group_id, params.table_name, input) catch |err|
            return if (err == error.InvalidArgument) textResponse(ctx, 400, @errorName(err)) else internalGroupErrorResponse(ctx, err);
        defer result.deinit(ctx.allocator);
        return ctx.json(result);
    }

    fn internalGraphHydrate(self: *AntflyApiHandler, ctx: *httpx.Context) !httpx.Response {
        var params = (try internalGroupTableParams(ctx)) orelse return textResponse(ctx, 400, "invalid path parameter");
        defer params.deinit(ctx.allocator);
        const body = (try ctx.body()) orelse return textResponse(ctx, 400, "invalid graph hydrate request");
        var input = distributed_graph.parseGraphHydrateRequest(ctx.allocator, body) catch return textResponse(ctx, 400, "invalid graph hydrate request");
        defer input.deinit(ctx.allocator);
        var result = self.internalGroupOperations().graphHydrate(ctx.allocator, operationContext(ctx, null), params.group_id, params.table_name, input) catch |err|
            return internalGroupErrorResponse(ctx, err);
        defer result.deinit(ctx.allocator);
        return ctx.json(result);
    }

    fn internalGraphEdges(self: *AntflyApiHandler, ctx: *httpx.Context) !httpx.Response {
        var params = (try internalGroupTableParams(ctx)) orelse return textResponse(ctx, 400, "invalid path parameter");
        defer params.deinit(ctx.allocator);
        const body = (try ctx.body()) orelse return textResponse(ctx, 400, "invalid graph edges request");
        var input = distributed_graph.parseGraphEdgesRequest(ctx.allocator, body) catch return textResponse(ctx, 400, "invalid graph edges request");
        defer input.deinit(ctx.allocator);
        var result = self.internalGroupOperations().graphEdges(ctx.allocator, operationContext(ctx, null), params.group_id, params.table_name, input) catch |err|
            return if (err == error.InvalidArgument) textResponse(ctx, 400, "invalid graph edges request") else internalGroupErrorResponse(ctx, err);
        defer result.deinit(ctx.allocator);
        return ctx.json(result);
    }

    fn internalTextStats(self: *AntflyApiHandler, ctx: *httpx.Context) !httpx.Response {
        var params = (try internalGroupTableParams(ctx)) orelse return textResponse(ctx, 400, "invalid path parameter");
        defer params.deinit(ctx.allocator);
        const body = (try ctx.body()) orelse "";
        var result = self.internalGroupOperations().textStats(ctx.allocator, operationContext(ctx, null), params.group_id, params.table_name, body) catch |err|
            return if (err == error.InvalidArgument) textResponse(ctx, 400, "invalid text stats request") else internalGroupErrorResponse(ctx, err);
        defer result.deinit(ctx.allocator);
        return jsonResponse(ctx, 200, result.json);
    }

    fn internalAlgebraicPartials(self: *AntflyApiHandler, ctx: *httpx.Context) !httpx.Response {
        var params = (try internalGroupTableParams(ctx)) orelse return textResponse(ctx, 400, "invalid path parameter");
        defer params.deinit(ctx.allocator);
        const body = (try ctx.body()) orelse "";
        var result = self.internalGroupOperations().algebraicPartials(ctx.allocator, operationContext(ctx, null), params.group_id, params.table_name, body) catch |err|
            return if (err == error.InvalidArgument) textResponse(ctx, 400, "invalid algebraic partials request") else internalGroupErrorResponse(ctx, err);
        defer result.deinit(ctx.allocator);
        if (algebraic_partials_wire.acceptsBase64V1(ctx.header(algebraic_partials_wire.response_encoding_header))) {
            return jsonResponse(ctx, 200, result.json);
        }
        const legacy = algebraic_partials_wire.base64V1ToLegacyAlloc(ctx.allocator, result.json) catch
            return textResponse(ctx, 500, "internal server error");
        defer ctx.allocator.free(legacy);
        return jsonResponse(ctx, 200, legacy);
    }

    fn internalGroupRoutedBatch(self: *AntflyApiHandler, ctx: *httpx.Context) !httpx.Response {
        var params = (try internalGroupTableParams(ctx)) orelse return textResponse(ctx, 400, "invalid path parameter");
        defer params.deinit(ctx.allocator);
        const forwarding = internal_batch_forwarding.parseValues(
            ctx.header(internal_batch_forwarding.remaining_ms_header),
            ctx.header(internal_batch_forwarding.forwards_remaining_header),
            ctx.header(internal_batch_forwarding.campaign_allowed_header),
        ) catch return textResponse(ctx, 400, "invalid raft batch forwarding headers");
        const forwarding_context = forwarding orelse return textResponse(ctx, 400, "missing raft batch forwarding headers");
        const body = (try ctx.body()) orelse "";
        var input = batch_api.parseInternalBatchRequest(ctx.allocator, body) catch |err| return switch (err) {
            error.InvalidBatchRequest => textResponse(ctx, 400, "invalid batch request"),
            error.ValueTooLong => textResponse(ctx, 413, "value too large"),
            else => textResponse(ctx, 500, "internal server error"),
        };
        defer input.deinit(ctx.allocator);
        const result = self.internalGroupOperations().routedBatch(
            ctx.allocator,
            operationContext(ctx, null),
            params.group_id,
            params.table_name,
            input.req,
            forwarding_context,
        ) catch |err| return switch (err) {
            error.InvalidArgument => textResponse(ctx, 400, "invalid batch request"),
            error.DocIdentityNamespaceMismatch => textResponse(ctx, 409, "doc identity namespace mismatch"),
            error.EnrichmentWaitCanceled,
            error.EnrichmentWaitTimeout,
            error.EnrichmentRetryInProgress,
            => blk: {
                try ctx.setHeader(internal_batch_forwarding.outcome_header, internal_batch_forwarding.outcome_committed_visibility_pending_v1);
                break :blk textResponse(ctx, 202, internal_batch_forwarding.committed_visibility_pending_body);
            },
            error.EnrichmentWorkerFailed => blk: {
                try ctx.setHeader(internal_batch_forwarding.outcome_header, internal_batch_forwarding.outcome_committed_repair_required_v1);
                break :blk textResponse(ctx, 202, internal_batch_forwarding.committed_repair_required_body);
            },
            error.RaftBatchWriteOutcomeUnknown => blk: {
                try ctx.setHeader(internal_batch_forwarding.outcome_header, internal_batch_forwarding.outcome_unknown_v1);
                break :blk textResponse(ctx, 409, "write outcome unknown");
            },
            error.GroupLeaderUnavailable => blk: {
                try ctx.setHeader(internal_batch_forwarding.outcome_header, internal_batch_forwarding.outcome_not_proposed_v1);
                break :blk textResponse(ctx, 503, "group leader unavailable");
            },
            error.Unavailable => blk: {
                try ctx.setHeader(internal_batch_forwarding.outcome_header, internal_batch_forwarding.outcome_not_proposed_v1);
                break :blk textResponse(ctx, 503, "routed raft batch unavailable");
            },
            error.NotFound => textResponse(ctx, 404, "not found"),
            error.Canceled => textResponse(ctx, 408, "request canceled"),
            error.DeadlineExceeded => textResponse(ctx, 504, "request deadline exceeded"),
            else => textResponse(ctx, 500, "internal server error"),
        };
        _ = ctx.status(201);
        return ctx.json(.{ .inserted = result.inserted, .deleted = result.deleted, .transformed = result.transformed });
    }

    fn internalTxnErrorResponse(ctx: *httpx.Context, err: internal_group_operations.Error) !httpx.Response {
        return switch (err) {
            error.InvalidArgument => textResponse(ctx, 400, "invalid transaction request"),
            error.DecisionConflict => textResponse(ctx, 409, "decision conflict"),
            error.TransactionConflict => textResponse(ctx, 409, "transaction conflict"),
            error.TopologyChanged => textResponse(ctx, 409, "topology changed"),
            error.DocIdentityNamespaceMismatch => textResponse(ctx, 409, "doc identity namespace mismatch"),
            error.Unsupported => textResponse(ctx, 405, "method not allowed"),
            error.NotFound => textResponse(ctx, 404, "not found"),
            error.Canceled => textResponse(ctx, 408, "request canceled"),
            error.DeadlineExceeded => textResponse(ctx, 504, "request deadline exceeded"),
            error.EnrichmentWaitCanceled,
            error.EnrichmentWaitTimeout,
            error.EnrichmentRetryInProgress,
            => textResponse(ctx, 202, "committed_visibility_pending"),
            error.EnrichmentWorkerFailed => textResponse(ctx, 202, "committed_repair_required"),
            error.Unavailable => textResponse(ctx, 503, "transaction unavailable"),
            else => textResponse(ctx, 500, "internal server error"),
        };
    }

    fn internalTxnBegin(self: *AntflyApiHandler, ctx: *httpx.Context) !httpx.Response {
        var params = (try internalGroupTableParams(ctx)) orelse return textResponse(ctx, 400, "invalid path parameter");
        defer params.deinit(ctx.allocator);
        const body = (try ctx.body()) orelse return textResponse(ctx, 400, "invalid transaction request");
        var input = distributed_txn.parseTxnBeginRequest(ctx.allocator, body) catch return textResponse(ctx, 400, "invalid transaction request");
        defer distributed_txn.freeTxnBeginRequest(ctx.allocator, &input);
        self.internalGroupOperations().txnBegin(ctx.allocator, operationContext(ctx, null), params.group_id, params.table_name, input) catch |err|
            return internalTxnErrorResponse(ctx, err);
        return ctx.json(struct {}{});
    }

    fn internalTxnPrepare(self: *AntflyApiHandler, ctx: *httpx.Context) !httpx.Response {
        var params = (try internalGroupTableParams(ctx)) orelse return textResponse(ctx, 400, "invalid path parameter");
        defer params.deinit(ctx.allocator);
        const body = (try ctx.body()) orelse return textResponse(ctx, 400, "invalid transaction request");
        var input = distributed_txn.parseTxnPrepareRequest(ctx.allocator, body) catch return textResponse(ctx, 400, "invalid transaction request");
        defer distributed_txn.freeTxnPrepareRequest(ctx.allocator, &input);
        self.internalGroupOperations().txnPrepare(ctx.allocator, operationContext(ctx, null), params.group_id, params.table_name, input) catch |err|
            return internalTxnErrorResponse(ctx, err);
        return ctx.json(struct {}{});
    }

    fn internalTxnResolve(self: *AntflyApiHandler, ctx: *httpx.Context) !httpx.Response {
        var params = (try internalGroupTableParams(ctx)) orelse return textResponse(ctx, 400, "invalid path parameter");
        defer params.deinit(ctx.allocator);
        const body = (try ctx.body()) orelse return textResponse(ctx, 400, "invalid transaction request");
        const input = distributed_txn.parseTxnResolveRequest(ctx.allocator, body) catch return textResponse(ctx, 400, "invalid transaction request");
        self.internalGroupOperations().txnResolve(ctx.allocator, operationContext(ctx, null), params.group_id, params.table_name, input) catch |err|
            return internalTxnErrorResponse(ctx, err);
        return ctx.json(struct {}{});
    }

    fn internalTxnStatus(self: *AntflyApiHandler, ctx: *httpx.Context) !httpx.Response {
        var params = (try internalGroupTableParams(ctx)) orelse return textResponse(ctx, 400, "invalid path parameter");
        defer params.deinit(ctx.allocator);
        const body = (try ctx.body()) orelse return textResponse(ctx, 400, "invalid transaction request");
        const txn_id = distributed_txn.parseTxnStatusRequest(ctx.allocator, body) catch return textResponse(ctx, 400, "invalid transaction request");
        const status = self.internalGroupOperations().txnStatus(ctx.allocator, operationContext(ctx, null), params.group_id, params.table_name, txn_id) catch |err|
            return internalTxnErrorResponse(ctx, err);
        return ctx.json(distributed_txn.TxnStatusResponse{ .status = status });
    }

    fn internalTxnAcknowledge(self: *AntflyApiHandler, ctx: *httpx.Context) !httpx.Response {
        var params = (try internalGroupTableParams(ctx)) orelse return textResponse(ctx, 400, "invalid path parameter");
        defer params.deinit(ctx.allocator);
        const body = (try ctx.body()) orelse return textResponse(ctx, 400, "invalid transaction request");
        var input = distributed_txn.parseTxnAcknowledgeRequest(ctx.allocator, body) catch return textResponse(ctx, 400, "invalid transaction request");
        defer distributed_txn.freeTxnAcknowledgeRequest(ctx.allocator, &input);
        self.internalGroupOperations().txnAcknowledge(ctx.allocator, operationContext(ctx, null), params.group_id, params.table_name, input) catch |err|
            return internalTxnErrorResponse(ctx, err);
        return ctx.json(struct {}{});
    }

    // ---------------------------------------------------------------
    // Authentication helper
    // ---------------------------------------------------------------

    fn authenticate(self: *AntflyApiHandler, ctx: *httpx.Context) !?AuthenticatedIdentity {
        if (!self.api_server.cfg.auth_enabled and self.api_server.cfg.trusted_principal_secret == null) return null;
        return self.api_server.authenticateRequest(.{
            .authorization = ctx.header("authorization"),
            .trusted_principal = ctx.header(http_server_mod.trusted_principal_header),
        }) catch |err| switch (err) {
            error.Unauthorized, error.InvalidPassword, error.UserNotFound, error.ApiKeyInvalid, error.ApiKeyNotFound, error.ApiKeyExpired => {
                return null;
            },
            else => return err,
        };
    }

    fn requireAuth(self: *AntflyApiHandler, ctx: *httpx.Context) !?AuthenticatedIdentity {
        if (!self.api_server.cfg.auth_enabled and self.api_server.cfg.trusted_principal_secret == null) return null;
        const identity = (try self.authenticate(ctx)) orelse {
            return error.Unauthorized;
        };
        return identity;
    }

    fn requestMethod(ctx: *httpx.Context) ?http_common.Method {
        return switch (ctx.request.method) {
            .GET => .GET,
            .POST => .POST,
            .PUT => .PUT,
            .DELETE => .DELETE,
            else => null,
        };
    }

    fn jsonResponse(ctx: *httpx.Context, status: u16, body: []const u8) !httpx.Response {
        _ = ctx.status(status);
        try ctx.setHeader("content-type", "application/json");
        _ = ctx.response.body(body);
        return ctx.response.build();
    }

    fn jsonErrorResponse(ctx: *httpx.Context, status: u16, message: []const u8) !httpx.Response {
        _ = ctx.status(status);
        return ctx.json(.{ .@"error" = message });
    }

    fn textResponse(ctx: *httpx.Context, status: u16, body: []const u8) !httpx.Response {
        _ = ctx.status(status);
        try ctx.setHeader("content-type", "text/plain");
        _ = ctx.response.body(body);
        return ctx.response.build();
    }

    fn queryOverloadedResponse(ctx: *httpx.Context) !httpx.Response {
        try ctx.setHeader("Retry-After", "1");
        // HTTP/2 has stream-local overload and forbids connection-specific
        // headers. HTTP/1 must close rejected keep-alive sockets promptly so
        // they do not retain connection-admission permits.
        if (ctx.h1_sock != null) try ctx.setHeader("Connection", "close");
        return textResponse(ctx, 429, "query capacity exhausted");
    }

    fn writeOverloadedResponse(ctx: *httpx.Context) !httpx.Response {
        try ctx.setHeader("Retry-After", "1");
        if (ctx.h1_sock != null) try ctx.setHeader("Connection", "close");
        return textResponse(ctx, 429, "write capacity exhausted");
    }

    fn inferenceOverloadedResponse(ctx: *httpx.Context) !httpx.Response {
        try ctx.setHeader("Retry-After", connections_api.inference_retry_after_seconds);
        if (ctx.h1_sock != null) try ctx.setHeader("Connection", "close");
        return ctx.status(503).json(connections_api.inferenceAdmissionFailure());
    }

    fn inferenceInvokeResponse(ctx: *httpx.Context, result: *const connections_api.InvokeResult) !httpx.Response {
        if (result.retry_after) |value| try ctx.setHeader("Retry-After", value);
        _ = ctx.status(result.status);
        try ctx.setHeader("content-type", result.content_type orelse "application/json");
        _ = ctx.response.body(result.body);
        return ctx.response.build();
    }

    fn acquirePublicOperation(
        self: *AntflyApiHandler,
        ctx: *httpx.Context,
        comptime operation_id: []const u8,
    ) !?httpx.Response {
        const class = comptime request_admission_policy.publicOperationClass(operation_id) orelse
            @compileError("public operation is missing an admission policy: " ++ operation_id);
        return switch (class) {
            .none => @compileError("operation does not use foreground admission: " ++ operation_id),
            .query => if (self.api_server.tryAcquireQuery()) null else try queryOverloadedResponse(ctx),
            .write => if (self.api_server.tryAcquireWrite()) null else try writeOverloadedResponse(ctx),
            .inference => if (self.api_server.tryAcquireInference()) null else try inferenceOverloadedResponse(ctx),
        };
    }

    fn releasePublicOperation(self: *AntflyApiHandler, comptime operation_id: []const u8) void {
        const class = comptime request_admission_policy.publicOperationClass(operation_id) orelse
            @compileError("public operation is missing an admission policy: " ++ operation_id);
        switch (class) {
            .none => @compileError("operation does not use foreground admission: " ++ operation_id),
            .query => self.api_server.releaseQuery(),
            .write => self.api_server.releaseWrite(),
            .inference => self.api_server.releaseInference(),
        }
    }

    fn queryBodyOverloadedResponse(ctx: *httpx.Context) !httpx.Response {
        try ctx.setHeader("Retry-After", "1");
        return textResponse(ctx, 429, "query body capacity exhausted");
    }

    fn unauthorizedResponse(ctx: *httpx.Context) !httpx.Response {
        try ctx.setHeader("WWW-Authenticate", "Basic realm=\"antfly\"");
        return jsonResponse(ctx, 401, "{\"error\":\"unauthorized\"}");
    }

    fn decodePathParamOrBadRequest(ctx: *httpx.Context, encoded: []const u8) !?[]u8 {
        return http_route_helpers.decodePercentEncodedPathComponentAlloc(ctx.allocator, encoded) catch |err| switch (err) {
            error.InvalidArgument => {
                _ = ctx.status(400);
                return null;
            },
            else => return err,
        };
    }

    fn authorizeRequest(self: *AntflyApiHandler, ctx: *httpx.Context, identity: *?AuthenticatedIdentity) !?httpx.Response {
        identity.* = null;
        if (!self.api_server.cfg.auth_enabled and self.api_server.cfg.trusted_principal_secret == null) return null;
        if (self.api_server.cfg.user_manager == null and self.api_server.cfg.trusted_principal_secret == null) return null;

        const path = http_server_mod.stripApiPrefix(ctx.request.uri.path);
        identity.* = self.api_server.authenticateRequest(.{
            .authorization = ctx.header("authorization"),
            .trusted_principal = ctx.header(http_server_mod.trusted_principal_header),
        }) catch |err| switch (err) {
            error.Unauthorized, error.InvalidPassword, error.UserNotFound, error.ApiKeyInvalid, error.ApiKeyNotFound, error.ApiKeyExpired => {
                return try unauthorizedResponse(ctx);
            },
            else => return err,
        };
        const authenticated = identity.*.?;

        if (http_server_mod.requiresAdminPermission(path) and !http_server_mod.permissionsAllow(authenticated.permissions, .@"*", "*", .admin)) {
            return try jsonErrorResponse(ctx, 403, "forbidden");
        }
        const method = requestMethod(ctx) orelse return null;
        const required_permission = http_server_mod.requiredPermissionForRequest(ctx.allocator, method, path) catch |err| switch (err) {
            error.InvalidArgument => return try textResponse(ctx, 400, "invalid path parameter"),
            else => return err,
        };
        if (required_permission) |required| {
            defer required.deinit(ctx.allocator);
            if (!http_server_mod.permissionsAllow(authenticated.permissions, required.resource_type, required.resource, required.permission_type)) {
                return try jsonErrorResponse(ctx, 403, "forbidden");
            }
        }
        return null;
    }

    // ---------------------------------------------------------------
    // metadata_openapi handler interface (29 methods)
    // ---------------------------------------------------------------

    pub fn getStatus(self: *AntflyApiHandler, ctx: *httpx.Context) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const alloc = ctx.allocator;
        var public_status = try self.api_server.loadClusterStatus(alloc);
        defer public_status.deinit(alloc);
        return ctx.json(public_status);
    }

    pub fn getCluster(self: *AntflyApiHandler, ctx: *httpx.Context) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const alloc = ctx.allocator;
        var topology = try self.api_server.loadClusterTopology(alloc);
        defer topology.deinit(alloc);
        return ctx.json(topology);
    }

    pub fn listConnections(self: *AntflyApiHandler, ctx: *httpx.Context, params: metadata_openapi.server.ListConnectionsParams) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const alloc = ctx.allocator;
        const body = try self.api_server.listConnectionsJsonAlloc(alloc, params.types, params.include, params.refresh);
        defer alloc.free(body);
        try ctx.setHeader("content-type", "application/json");
        _ = ctx.response.body(body);
        return ctx.response.build();
    }

    pub fn invokeInferenceConnection(self: *AntflyApiHandler, ctx: *httpx.Context, connection_id: []const u8, operation: []const u8) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        comptime std.debug.assert(request_admission_policy.publicOperationClass("invokeInferenceConnection").? == .inference);
        const admission_owner = self.api_server.inferenceConnectionAdmissionOwner(connection_id, operation) catch |err| switch (err) {
            error.ConnectionCapabilityMissing => return textResponse(ctx, 403, @errorName(err)),
            error.ConnectionNotFound, error.ConnectionNotInference, error.InvalidConfig, error.ConnectionURLMissing, error.InvalidConnectionURL, error.ProviderNotAntflyCompatible, error.UnsupportedInferenceOperation => return textResponse(ctx, 400, @errorName(err)),
        };
        const caller_owns_admission = admission_owner == .caller;
        if (caller_owns_admission and !self.api_server.tryAcquireInference()) return inferenceOverloadedResponse(ctx);
        defer if (caller_owns_admission) self.api_server.releaseInference();
        const body = (try ctx.body()) orelse return textResponse(ctx, 400, "request body required");
        const cancellation = httpx.CancellationToken.fromCallback(ctx, struct {
            fn isCancelled(raw: *const anyopaque) bool {
                const request_context: *const httpx.Context = @ptrCast(@alignCast(raw));
                return request_context.isCancellationRequested();
            }
        }.isCancelled);
        const deadline_ns = connections_api.inferenceInvocationDeadlineNs();
        var transport = runtime_http_bridge.Outbound{ .context = ctx };
        var result = self.api_server.invokeInferenceConnection(
            ctx.allocator,
            connection_id,
            operation,
            body,
            cancellation,
            deadline_ns,
            transport.stream(),
        ) catch |err| {
            // Once a stream has committed its headers, the listener must close
            // that stream on failure; attempting to synthesize a second HTTP
            // response would corrupt the transport.
            if (transport.started) return err;
            return switch (err) {
                error.ConnectionCapabilityMissing => textResponse(ctx, 403, @errorName(err)),
                error.ConnectionNotFound, error.ConnectionNotInference, error.InvalidConfig, error.ConnectionURLMissing, error.InvalidConnectionURL, error.ProviderNotAntflyCompatible, error.UnsupportedInferenceOperation => textResponse(ctx, 400, @errorName(err)),
                error.Canceled, error.Cancelled => error.Canceled,
                error.Timeout => textResponse(ctx, 504, "inference invocation deadline exceeded"),
                else => textResponse(ctx, 502, @errorName(err)),
            };
        };
        defer result.deinit(ctx.allocator);
        if (transport.started) return ctx.response.build();
        return inferenceInvokeResponse(ctx, &result);
    }

    pub fn listSecrets(self: *AntflyApiHandler, ctx: *httpx.Context) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const alloc = ctx.allocator;
        const listed = if (self.api_server.cfg.secret_store) |secret_store|
            try secret_store.list(alloc)
        else
            try common_secrets.listEnvironmentSecrets(alloc);
        defer common_secrets.freeListedSecrets(alloc, listed);
        const secret_list = try http_server_mod.makeSecretList(alloc, listed);
        defer alloc.free(secret_list.secrets);
        return ctx.json(secret_list);
    }

    pub fn putSecret(self: *AntflyApiHandler, ctx: *httpx.Context, key: []const u8) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const alloc = ctx.allocator;
        const secret_store = self.api_server.cfg.secret_store orelse {
            _ = ctx.status(503);
            return ctx.text("secret management not available in multi-node mode");
        };
        const body_data = (try ctx.body()) orelse {
            _ = ctx.status(400);
            return ctx.text("invalid secret request");
        };
        var parsed = metadata_openapi.server.parsePutSecretBody(alloc, body_data) catch {
            _ = ctx.status(400);
            return ctx.text("invalid secret request");
        };
        defer parsed.deinit();
        var listed = secret_store.put(alloc, key, parsed.value.value) catch |err| switch (err) {
            error.InvalidSecretKey => {
                _ = ctx.status(400);
                return ctx.text("invalid secret key");
            },
            else => return err,
        };
        defer listed.deinit(alloc);
        return ctx.json(http_server_mod.makeSecretEntry(listed));
    }

    pub fn deleteSecret(self: *AntflyApiHandler, ctx: *httpx.Context, key: []const u8) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const secret_store = self.api_server.cfg.secret_store orelse {
            _ = ctx.status(503);
            return ctx.text("secret management not available in multi-node mode");
        };
        if (!(try secret_store.delete(key))) {
            _ = ctx.status(404);
            return ctx.text("not found");
        }
        _ = ctx.status(204);
        return ctx.text("");
    }

    const CommitResponseMode = enum {
        transaction,
        multi_batch,
    };

    pub fn multiBatchWrite(self: *AntflyApiHandler, ctx: *httpx.Context) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const alloc = ctx.allocator;
        const body_data = (try ctx.body()) orelse {
            _ = ctx.status(400);
            return ctx.text("missing body");
        };
        var commit_req = transactions_api.parseMultiBatchRequest(alloc, body_data) catch |err| switch (err) {
            error.OutOfMemory => return err,
            else => {
                _ = ctx.status(400);
                return ctx.text("invalid multi-batch request");
            },
        };
        defer commit_req.deinit(alloc);
        if (try self.acquirePublicOperation(ctx, "multiBatchWrite")) |response| return response;
        defer self.releasePublicOperation("multiBatchWrite");
        return try self.executeCommitRequest(ctx, authenticated_identity, &commit_req, .multi_batch);
    }

    pub fn commitTransaction(self: *AntflyApiHandler, ctx: *httpx.Context) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const alloc = ctx.allocator;
        const body_data = (try ctx.body()) orelse {
            _ = ctx.status(400);
            return ctx.text("invalid transaction commit request");
        };
        var commit_req = transactions_api.parseCommitRequest(alloc, body_data) catch |err| switch (err) {
            error.InvalidTransactionCommitRequest => {
                _ = ctx.status(400);
                return ctx.text("invalid transaction commit request");
            },
            else => return err,
        };
        defer commit_req.deinit(alloc);
        if (try self.acquirePublicOperation(ctx, "commitTransaction")) |response| return response;
        defer self.releasePublicOperation("commitTransaction");
        return try self.executeCommitRequest(ctx, authenticated_identity, &commit_req, .transaction);
    }

    fn executeCommitRequest(
        self: *AntflyApiHandler,
        ctx: *httpx.Context,
        authenticated_identity: ?AuthenticatedIdentity,
        commit_req: *transactions_api.OwnedTransactionCommitRequest,
        response_mode: CommitResponseMode,
    ) !httpx.Response {
        const alloc = ctx.allocator;
        const source = self.api_server.table_writes orelse {
            _ = ctx.status(404);
            return ctx.text("not found");
        };
        if (!(try self.api_server.transactionRequestAuthorized(authenticated_identity, commit_req.*))) {
            _ = ctx.status(403);
            return ctx.text("forbidden");
        }

        const distributed_tables = try commit_req.distributedTables(alloc);
        defer if (distributed_tables.len > 0) alloc.free(distributed_tables);
        self.api_server.validateCommitTablesAgainstSchema(distributed_tables) catch |err| switch (err) {
            error.InvalidBatchRequest,
            error.InvalidArgument,
            error.InvalidGraphEdges,
            error.UnsupportedTransformOperation,
            => {
                _ = ctx.status(400);
                return ctx.text("invalid transaction commit request");
            },
            else => return err,
        };
        if (try self.api_server.validateCommitReadSet(commit_req.*)) |conflict| {
            var arena_impl = std.heap.ArenaAllocator.init(ctx.allocator);
            defer arena_impl.deinit();
            const response = try transactions_api.buildCommitResponse(
                arena_impl.allocator(),
                "aborted",
                conflict,
                null,
            );
            _ = ctx.status(409);
            return ctx.json(response);
        }

        const commit_request = operationContext(ctx, authenticated_identity);
        const outcome = ((switch (response_mode) {
            .transaction => source.commitTransactionWithCancellation(alloc, distributed_tables, commit_req.sync_level, commit_request.cancellation),
            .multi_batch => source.commitBatchWithCancellation(alloc, distributed_tables, commit_req.sync_level, commit_request.cancellation),
        }) catch |err| switch (err) {
            error.InvalidBatchRequest,
            error.InvalidArgument,
            error.InvalidGraphEdges,
            error.UnsupportedTransformOperation,
            => {
                _ = ctx.status(400);
                return ctx.text("invalid transaction commit request");
            },
            error.TopologyChanged => {
                var arena_impl = std.heap.ArenaAllocator.init(alloc);
                defer arena_impl.deinit();
                const response = try transactions_api.buildCommitResponse(
                    arena_impl.allocator(),
                    "aborted",
                    transactions_api.topologyChangedConflict(if (commit_req.tables.len > 0) commit_req.tables[0].table_name else ""),
                    null,
                );
                _ = ctx.status(409);
                return ctx.json(response);
            },
            error.DecisionConflict => {
                var arena_impl = std.heap.ArenaAllocator.init(alloc);
                defer arena_impl.deinit();
                const response = try transactions_api.buildCommitResponse(
                    arena_impl.allocator(),
                    "aborted",
                    transactions_api.decisionConflict(if (commit_req.tables.len > 0) commit_req.tables[0].table_name else ""),
                    null,
                );
                _ = ctx.status(409);
                return ctx.json(response);
            },
            error.DocIdentityNamespaceMismatch => {
                var arena_impl = std.heap.ArenaAllocator.init(alloc);
                defer arena_impl.deinit();
                const response = try transactions_api.buildCommitResponse(
                    arena_impl.allocator(),
                    "aborted",
                    transactions_api.docIdentityUnavailableConflict(if (commit_req.tables.len > 0) commit_req.tables[0].table_name else ""),
                    null,
                );
                _ = ctx.status(409);
                return ctx.json(response);
            },
            error.TxnNotFound, error.InvalidTxnRecord => {
                var arena_impl = std.heap.ArenaAllocator.init(alloc);
                defer arena_impl.deinit();
                const response = try transactions_api.buildCommitResponse(
                    arena_impl.allocator(),
                    "aborted",
                    transactions_api.tornStateConflict(if (commit_req.tables.len > 0) commit_req.tables[0].table_name else ""),
                    null,
                );
                _ = ctx.status(409);
                return ctx.json(response);
            },
            error.CommitVisibilityNotSatisfied,
            error.EnrichmentWaitCanceled,
            error.EnrichmentWaitTimeout,
            error.EnrichmentRetryInProgress,
            => {
                var arena_impl = std.heap.ArenaAllocator.init(ctx.allocator);
                defer arena_impl.deinit();
                _ = ctx.status(202);
                return switch (response_mode) {
                    .transaction => ctx.json(try transactions_api.buildCommitResponse(
                        arena_impl.allocator(),
                        "committed_visibility_pending",
                        null,
                        commit_req.tables,
                    )),
                    .multi_batch => ctx.json(try transactions_api.buildMultiBatchResponse(
                        arena_impl.allocator(),
                        "committed_visibility_pending",
                        commit_req.tables,
                    )),
                };
            },
            error.EnrichmentWorkerFailed => {
                var arena_impl = std.heap.ArenaAllocator.init(ctx.allocator);
                defer arena_impl.deinit();
                _ = ctx.status(202);
                return switch (response_mode) {
                    .transaction => ctx.json(try transactions_api.buildCommitResponse(
                        arena_impl.allocator(),
                        "committed_repair_required",
                        null,
                        commit_req.tables,
                    )),
                    .multi_batch => ctx.json(try transactions_api.buildMultiBatchResponse(
                        arena_impl.allocator(),
                        "committed_repair_required",
                        commit_req.tables,
                    )),
                };
            },
            error.CommitPropagationIncomplete => {
                var arena_impl = std.heap.ArenaAllocator.init(ctx.allocator);
                defer arena_impl.deinit();
                _ = ctx.status(202);
                return switch (response_mode) {
                    .transaction => ctx.json(try transactions_api.buildCommitResponse(
                        arena_impl.allocator(),
                        "committed_recovery_pending",
                        null,
                        commit_req.tables,
                    )),
                    .multi_batch => ctx.json(try transactions_api.buildMultiBatchResponse(
                        arena_impl.allocator(),
                        "committed_recovery_pending",
                        commit_req.tables,
                    )),
                };
            },
            error.CommitDecisionUnknown => {
                _ = ctx.status(500);
                return ctx.text("transaction outcome is unknown; do not retry this stateless request because it may already have committed; use a transaction session for retryable commits");
            },
            error.AbortDecisionNotDurable, error.TransactionBeginFailed => {
                _ = ctx.status(503);
                return ctx.text("transaction coordinator is temporarily unavailable");
            },
            error.UnsupportedOperation => {
                _ = ctx.status(405);
                return ctx.text("method not allowed");
            },
            error.TableNotFound, error.UnknownGroup => {
                _ = ctx.status(404);
                return ctx.text("not found");
            },
            else => return err,
        }) orelse {
            _ = ctx.status(404);
            return ctx.text("not found");
        };

        switch (outcome) {
            .committed => |committed| {
                var arena_impl = std.heap.ArenaAllocator.init(alloc);
                defer arena_impl.deinit();
                switch (response_mode) {
                    .transaction => {
                        const terminal_status = transactions_api.terminalCommitStatusForOutcome(
                            committed.propagation_pending,
                            committed.visibility_pending,
                            committed.visibility_retry_pending,
                            committed.visibility_repair_required,
                        );
                        const status = transactions_api.terminalCommitResponseStatus(
                            terminal_status,
                            committed.visibility_repair_required,
                        );
                        const response = try transactions_api.buildCommitResponse(arena_impl.allocator(), status, null, commit_req.tables);
                        if (terminal_status != .committed or committed.visibility_repair_required)
                            _ = ctx.status(202);
                        return ctx.json(response);
                    },
                    .multi_batch => {
                        const terminal_status = transactions_api.terminalCommitStatusForOutcome(
                            committed.propagation_pending,
                            committed.visibility_pending,
                            committed.visibility_retry_pending,
                            committed.visibility_repair_required,
                        );
                        const status = transactions_api.terminalCommitResponseStatus(
                            terminal_status,
                            committed.visibility_repair_required,
                        );
                        const response = try transactions_api.buildMultiBatchResponse(arena_impl.allocator(), status, commit_req.tables);
                        _ = ctx.status(if (terminal_status == .committed and !committed.visibility_repair_required) 201 else 202);
                        return ctx.json(response);
                    },
                }
            },
            .conflict => |conflict| {
                const enriched_conflict = try self.api_server.enrichCommitConflict(commit_req.*, conflict);
                var arena_impl = std.heap.ArenaAllocator.init(alloc);
                defer arena_impl.deinit();
                const response = try transactions_api.buildCommitResponse(
                    arena_impl.allocator(),
                    "aborted",
                    enriched_conflict,
                    null,
                );
                _ = ctx.status(409);
                return ctx.json(response);
            },
        }
    }

    pub fn listTransactionSessions(self: *AntflyApiHandler, ctx: *httpx.Context) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const alloc = self.api_server.alloc;
        const sessions = try self.api_server.txn_sessions.listStatusesForPrincipal(
            alloc,
            if (authenticated_identity) |identity| identity.username else null,
        );
        defer alloc.free(sessions);
        var arena_impl = std.heap.ArenaAllocator.init(ctx.allocator);
        defer arena_impl.deinit();
        const response = try transactions_api.buildSessionListResponse(arena_impl.allocator(), sessions);
        return ctx.json(response);
    }

    pub fn cleanupTransactionSessions(self: *AntflyApiHandler, ctx: *httpx.Context, params: metadata_openapi.server.CleanupTransactionSessionsParams) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const now_ns = platform_time.realtimeNs();
        const cutoff_ns = if (params.cutoff_ns) |value|
            std.fmt.parseUnsigned(u64, value, 10) catch {
                _ = ctx.status(400);
                return ctx.text("invalid cutoff");
            }
        else if (self.api_server.cfg.session_ttl_ns) |ttl_ns|
            now_ns -| ttl_ns
        else {
            _ = ctx.status(400);
            return ctx.text("missing cutoff");
        };
        const removed = try self.api_server.cleanupExpiredSessions(cutoff_ns);
        return ctx.json(transactions_api.buildSessionCleanupResponse(removed, cutoff_ns));
    }

    pub fn beginTransaction(self: *AntflyApiHandler, ctx: *httpx.Context) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const body_data = (try ctx.body()) orelse "";
        const alloc = self.api_server.alloc;
        const begin_req = transactions_api.parseBeginRequest(alloc, body_data) catch {
            _ = ctx.status(400);
            return ctx.text("invalid transaction begin request");
        };
        const session = try self.api_server.txn_sessions.beginForPrincipal(
            alloc,
            begin_req,
            self.api_server.localSessionNodeId(),
            if (authenticated_identity) |identity| identity.username else null,
        );
        var arena_impl = std.heap.ArenaAllocator.init(ctx.allocator);
        defer arena_impl.deinit();
        const response = try transactions_api.buildBeginResponse(arena_impl.allocator(), session);
        _ = ctx.status(201);
        return ctx.json(response);
    }

    pub fn getTransactionSession(self: *AntflyApiHandler, ctx: *httpx.Context, transaction_id: []const u8) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const txn_id = distributed_txn.parseTxnIdHex(transaction_id) catch {
            _ = ctx.status(400);
            return ctx.text("invalid transaction id");
        };
        if (try self.forwardTransactionSession(ctx, txn_id, "")) |response| return response;
        if (!(try self.api_server.transactionSessionAccessible(txn_id, authenticated_identity))) {
            _ = ctx.status(404);
            return ctx.text("not found");
        }
        const alloc = self.api_server.alloc;
        var details = (self.api_server.txn_sessions.getDetails(alloc, txn_id) catch |err| switch (err) {
            error.SessionLeaseLost => {
                _ = ctx.status(409);
                return ctx.text("session lease lost");
            },
            else => return err,
        }) orelse {
            _ = ctx.status(404);
            return ctx.text("not found");
        };
        defer details.deinit(alloc);
        var arena_impl = std.heap.ArenaAllocator.init(ctx.allocator);
        defer arena_impl.deinit();
        const response = try transactions_api.buildSessionDetailsResponse(arena_impl.allocator(), details);
        return ctx.json(response);
    }

    pub fn stageTransactionSession(self: *AntflyApiHandler, ctx: *httpx.Context, transaction_id: []const u8) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const body_data = (try ctx.body()) orelse "";
        const txn_id = distributed_txn.parseTxnIdHex(transaction_id) catch {
            _ = ctx.status(400);
            return ctx.text("invalid transaction id");
        };
        if (try self.forwardTransactionSession(ctx, txn_id, body_data)) |response| return response;
        if (!(try self.api_server.transactionSessionAccessible(txn_id, authenticated_identity))) {
            _ = ctx.status(404);
            return ctx.text("not found");
        }
        const alloc = self.api_server.alloc;
        var stage_req = transactions_api.parseCommitRequest(alloc, body_data) catch |err| switch (err) {
            error.InvalidTransactionCommitRequest => {
                _ = ctx.status(400);
                return ctx.text("invalid transaction stage request");
            },
            else => return err,
        };
        defer stage_req.deinit(alloc);
        if (!(try self.api_server.transactionRequestAuthorized(authenticated_identity, stage_req))) {
            _ = ctx.status(403);
            return ctx.text("forbidden");
        }
        const session = (self.api_server.txn_sessions.stage(alloc, txn_id, &stage_req) catch |err| switch (err) {
            error.SessionLeaseLost => {
                _ = ctx.status(409);
                return ctx.text("session lease lost");
            },
            error.TransactionCommitSealed => {
                _ = ctx.status(409);
                return ctx.text("transaction commit is sealed");
            },
            else => return err,
        }) orelse {
            _ = ctx.status(404);
            return ctx.text("not found");
        };
        var arena_impl = std.heap.ArenaAllocator.init(ctx.allocator);
        defer arena_impl.deinit();
        const response = try transactions_api.buildStageResponse(arena_impl.allocator(), session.txn_id);
        return ctx.json(response);
    }

    pub fn stageTransactionRead(self: *AntflyApiHandler, ctx: *httpx.Context, transaction_id: []const u8) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const body_data = (try ctx.body()) orelse "";
        const txn_id = distributed_txn.parseTxnIdHex(transaction_id) catch {
            _ = ctx.status(400);
            return ctx.text("invalid transaction id");
        };
        if (try self.forwardTransactionSession(ctx, txn_id, body_data)) |response| return response;
        if (!(try self.api_server.transactionSessionAccessible(txn_id, authenticated_identity))) {
            _ = ctx.status(404);
            return ctx.text("not found");
        }
        const alloc = self.api_server.alloc;
        var read_req = transactions_api.parseStageReadPayload(alloc, body_data) catch {
            _ = ctx.status(400);
            return ctx.text("invalid transaction read request");
        };
        defer read_req.deinit(alloc);
        if (authenticated_identity) |identity| {
            if (!http_server_mod.permissionsAllow(identity.permissions, .table, read_req.table_name, .read)) {
                _ = ctx.status(403);
                return ctx.text("forbidden");
            }
        }

        var owned_snapshot = (self.api_server.txn_sessions.getReadSnapshot(alloc, txn_id, read_req.table_name, read_req.key) catch |err| switch (err) {
            error.SessionLeaseLost => {
                _ = ctx.status(409);
                return ctx.text("session lease lost");
            },
            else => return err,
        }) orelse transactions_api.SessionReadSnapshot{
            .table_name = try alloc.dupe(u8, read_req.table_name),
            .key = try alloc.dupe(u8, read_req.key),
            .version = 0,
        };
        defer owned_snapshot.deinit(alloc);

        if (owned_snapshot.version == 0 and self.api_server.table_reads != null) {
            const fetched = try self.api_server.lookupStageReadSnapshot(read_req.table_name, read_req.key);
            if (owned_snapshot.document_json) |document_json| alloc.free(document_json);
            alloc.free(owned_snapshot.table_name);
            alloc.free(owned_snapshot.key);
            owned_snapshot = .{
                .table_name = try alloc.dupe(u8, fetched.table_name),
                .key = try alloc.dupe(u8, fetched.key),
                .version = fetched.version,
                .document_json = if (fetched.document_json) |document_json| try alloc.dupe(u8, document_json) else null,
            };
            if (fetched.document_json) |document_json| alloc.free(document_json);
        } else if (owned_snapshot.version == 0) {
            owned_snapshot.version = read_req.version;
        }
        if (!(try self.api_server.transactionReadSnapshotVisible(
            authenticated_identity,
            owned_snapshot.table_name,
            owned_snapshot.key,
            owned_snapshot.document_json,
        ))) {
            if (owned_snapshot.document_json) |document_json| {
                alloc.free(document_json);
                owned_snapshot.document_json = null;
            }
            owned_snapshot.version = 0;
        }
        if (owned_snapshot.version != read_req.version) {
            var arena_impl = std.heap.ArenaAllocator.init(ctx.allocator);
            defer arena_impl.deinit();
            const response = try transactions_api.buildSessionCommitResponse(
                arena_impl.allocator(),
                txn_id,
                "conflict",
                transactions_api.versionConflict(read_req.table_name, read_req.key, read_req.version, owned_snapshot.version),
                null,
            );
            _ = ctx.status(409);
            return ctx.json(response);
        }

        var stage_req = try transactions_api.ownedRequestFromStageReadRequest(alloc, read_req);
        defer stage_req.deinit(alloc);
        const session = (self.api_server.txn_sessions.stageRead(alloc, txn_id, &stage_req, owned_snapshot.stage()) catch |err| switch (err) {
            error.SessionLeaseLost => {
                _ = ctx.status(409);
                return ctx.text("session lease lost");
            },
            error.TransactionCommitSealed => {
                _ = ctx.status(409);
                return ctx.text("transaction commit is sealed");
            },
            else => return err,
        }) orelse {
            _ = ctx.status(404);
            return ctx.text("not found");
        };
        _ = session;
        var arena_impl = std.heap.ArenaAllocator.init(ctx.allocator);
        defer arena_impl.deinit();
        const response = try transactions_api.buildStageReadResponse(arena_impl.allocator(), txn_id, owned_snapshot.stage());
        return ctx.json(response);
    }

    pub fn stageTransactionWrite(self: *AntflyApiHandler, ctx: *httpx.Context, transaction_id: []const u8) !httpx.Response {
        return self.stageSessionMutation(ctx, transaction_id, .write);
    }

    pub fn stageTransactionDelete(self: *AntflyApiHandler, ctx: *httpx.Context, transaction_id: []const u8) !httpx.Response {
        return self.stageSessionMutation(ctx, transaction_id, .delete);
    }

    const SessionMutationKind = enum { write, delete };

    fn stageSessionMutation(self: *AntflyApiHandler, ctx: *httpx.Context, transaction_id: []const u8, kind: SessionMutationKind) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const body_data = (try ctx.body()) orelse "";
        const txn_id = distributed_txn.parseTxnIdHex(transaction_id) catch {
            _ = ctx.status(400);
            return ctx.text("invalid transaction id");
        };
        if (try self.forwardTransactionSession(ctx, txn_id, body_data)) |response| return response;
        if (!(try self.api_server.transactionSessionAccessible(txn_id, authenticated_identity))) {
            _ = ctx.status(404);
            return ctx.text("not found");
        }
        const alloc = self.api_server.alloc;
        var stage_req = switch (kind) {
            .write => transactions_api.parseStageWriteRequest(alloc, body_data) catch {
                _ = ctx.status(400);
                return ctx.text("invalid transaction write request");
            },
            .delete => transactions_api.parseStageDeleteRequest(alloc, body_data) catch {
                _ = ctx.status(400);
                return ctx.text("invalid transaction delete request");
            },
        };
        defer stage_req.deinit(alloc);
        if (!(try self.api_server.transactionRequestAuthorized(authenticated_identity, stage_req))) {
            _ = ctx.status(403);
            return ctx.text("forbidden");
        }
        const session = (self.api_server.txn_sessions.stage(alloc, txn_id, &stage_req) catch |err| switch (err) {
            error.SessionLeaseLost => {
                _ = ctx.status(409);
                return ctx.text("session lease lost");
            },
            error.TransactionCommitSealed => {
                _ = ctx.status(409);
                return ctx.text("transaction commit is sealed");
            },
            else => return err,
        }) orelse {
            _ = ctx.status(404);
            return ctx.text("not found");
        };
        var arena_impl = std.heap.ArenaAllocator.init(ctx.allocator);
        defer arena_impl.deinit();
        const response = try transactions_api.buildStageResponse(arena_impl.allocator(), session.txn_id);
        return ctx.json(response);
    }

    pub fn createTransactionSavepoint(self: *AntflyApiHandler, ctx: *httpx.Context, transaction_id: []const u8) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const body_data = (try ctx.body()) orelse "";
        const txn_id = distributed_txn.parseTxnIdHex(transaction_id) catch {
            _ = ctx.status(400);
            return ctx.text("invalid transaction id");
        };
        if (try self.forwardTransactionSession(ctx, txn_id, body_data)) |response| return response;
        if (!(try self.api_server.transactionSessionAccessible(txn_id, authenticated_identity))) {
            _ = ctx.status(404);
            return ctx.text("not found");
        }
        const info = (self.api_server.txn_sessions.createSavepoint(self.api_server.alloc, txn_id) catch |err| switch (err) {
            error.SessionLeaseLost => {
                _ = ctx.status(409);
                return ctx.text("session lease lost");
            },
            error.TransactionCommitSealed => {
                _ = ctx.status(409);
                return ctx.text("transaction commit is sealed");
            },
            error.SavepointLimitExceeded => {
                _ = ctx.status(409);
                return ctx.text("savepoint limit exceeded");
            },
            else => return err,
        }) orelse {
            _ = ctx.status(404);
            return ctx.text("not found");
        };
        var arena_impl = std.heap.ArenaAllocator.init(ctx.allocator);
        defer arena_impl.deinit();
        const response = try transactions_api.buildSavepointResponse(arena_impl.allocator(), info);
        return ctx.json(response);
    }

    pub fn rollbackTransactionSavepoint(self: *AntflyApiHandler, ctx: *httpx.Context, transaction_id: []const u8, savepoint_id: []const u8) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const body_data = (try ctx.body()) orelse "";
        const txn_id = distributed_txn.parseTxnIdHex(transaction_id) catch {
            _ = ctx.status(400);
            return ctx.text("invalid transaction id");
        };
        const parsed_savepoint_id = std.fmt.parseUnsigned(u64, savepoint_id, 10) catch {
            _ = ctx.status(400);
            return ctx.text("invalid savepoint id");
        };
        if (try self.forwardTransactionSession(ctx, txn_id, body_data)) |response| return response;
        if (!(try self.api_server.transactionSessionAccessible(txn_id, authenticated_identity))) {
            _ = ctx.status(404);
            return ctx.text("not found");
        }
        const info = (self.api_server.txn_sessions.rollbackToSavepoint(self.api_server.alloc, txn_id, parsed_savepoint_id) catch |err| switch (err) {
            error.SessionLeaseLost => {
                _ = ctx.status(409);
                return ctx.text("session lease lost");
            },
            error.TransactionCommitSealed => {
                _ = ctx.status(409);
                return ctx.text("transaction commit is sealed");
            },
            else => return err,
        }) orelse {
            _ = ctx.status(404);
            return ctx.text("not found");
        };
        var arena_impl = std.heap.ArenaAllocator.init(ctx.allocator);
        defer arena_impl.deinit();
        const response = try transactions_api.buildRollbackResponse(arena_impl.allocator(), info);
        return ctx.json(response);
    }

    pub fn commitTransactionSession(self: *AntflyApiHandler, ctx: *httpx.Context, transaction_id: []const u8) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const body_data = (try ctx.body()) orelse "";
        const source = self.api_server.table_writes orelse {
            _ = ctx.status(404);
            return ctx.text("not found");
        };
        const txn_id = distributed_txn.parseTxnIdHex(transaction_id) catch {
            _ = ctx.status(400);
            return ctx.text("invalid transaction id");
        };
        if (try self.forwardTransactionSession(ctx, txn_id, body_data)) |response| return response;
        if (!(try self.api_server.transactionSessionAccessible(txn_id, authenticated_identity))) {
            _ = ctx.status(404);
            return ctx.text("not found");
        }
        const alloc = self.api_server.alloc;
        const session = self.api_server.txn_sessions.getInfo(txn_id) orelse {
            _ = ctx.status(404);
            return ctx.text("not found");
        };
        var parsed_req: ?transactions_api.OwnedTransactionCommitRequest = null;
        defer if (parsed_req) |*commit_req| commit_req.deinit(alloc);
        if (!transactions_api.isEmptySessionCommitBody(body_data)) {
            parsed_req = transactions_api.parseCommitRequest(alloc, body_data) catch |err| switch (err) {
                error.InvalidTransactionCommitRequest => {
                    _ = ctx.status(400);
                    return ctx.text("invalid transaction commit request");
                },
                else => return err,
            };
        }
        var commit_req = (self.api_server.txn_sessions.cloneCommitRequest(alloc, txn_id, if (parsed_req) |*value| value else null) catch |err| switch (err) {
            error.TransactionCommitRequestMismatch => {
                _ = ctx.status(409);
                return ctx.text("transaction commit retry body does not match the sealed request");
            },
            error.SessionLeaseLost => {
                var arena_impl = std.heap.ArenaAllocator.init(ctx.allocator);
                defer arena_impl.deinit();
                const response = try transactions_api.buildSessionCommitResponse(
                    arena_impl.allocator(),
                    txn_id,
                    "aborted",
                    transactions_api.sessionLeaseLostConflict(if (parsed_req) |value| if (value.tables.len > 0) value.tables[0].table_name else "" else ""),
                    null,
                );
                _ = ctx.status(409);
                return ctx.json(response);
            },
            else => return err,
        }) orelse {
            _ = ctx.status(400);
            return ctx.text("transaction has no staged writes");
        };
        defer commit_req.deinit(alloc);
        if (!(try self.api_server.transactionRequestAuthorized(authenticated_identity, commit_req))) {
            _ = ctx.status(403);
            return ctx.text("forbidden");
        }

        if (try self.api_server.txn_sessions.getTerminalCommit(alloc, txn_id)) |terminal_value| {
            var terminal = terminal_value;
            defer terminal.deinit(alloc);
            var status = terminal.status;
            // A complete terminal response is durable before the coordinator
            // self-participant is released. Retrying that final ACK is safe;
            // pending terminal states must instead replay phase two under the
            // same transaction ID through session maintenance.
            if (status == .committed and !terminal.coordinator_acknowledged) {
                if (terminal.coordinator_group_id) |coordinator_group_id| {
                    if (try self.acquirePublicOperation(ctx, "commitTransactionSession")) |response| return response;
                    defer self.releasePublicOperation("commitTransactionSession");
                    const coordinator_table_name = terminal.coordinator_table_name orelse return error.InvalidTransactionSessionRecord;
                    const acknowledged = source.acknowledgeTransactionCommit(
                        alloc,
                        txn_id,
                        coordinator_group_id,
                        coordinator_table_name,
                    ) catch |err| blk: {
                        std.log.warn("stable transaction coordinator acknowledgement retry deferred txn_id={x} err={s}", .{ txn_id, @errorName(err) });
                        break :blk null;
                    };
                    if (acknowledged == null) {
                        status = .committed_recovery_pending;
                    } else if ((self.api_server.txn_sessions.markTerminalCoordinatorAcknowledged(alloc, txn_id) catch |err| blk: {
                        std.log.warn("failed to persist stable transaction coordinator acknowledgement txn_id={x} err={s}", .{ txn_id, @errorName(err) });
                        break :blk null;
                    }) == null) {
                        status = .committed_recovery_pending;
                    }
                }
            }
            var arena_impl = std.heap.ArenaAllocator.init(ctx.allocator);
            defer arena_impl.deinit();
            const response = try transactions_api.buildSessionCommitResponse(
                arena_impl.allocator(),
                txn_id,
                transactions_api.terminalCommitResponseStatus(status, terminal.repair_required),
                null,
                commit_req.tables,
            );
            _ = ctx.status(if (status == .committed and !terminal.repair_required) 200 else 202);
            return ctx.json(response);
        }

        const distributed_tables = try commit_req.distributedTables(alloc);
        defer if (distributed_tables.len > 0) alloc.free(distributed_tables);
        self.api_server.validateCommitTablesAgainstSchema(distributed_tables) catch |err| switch (err) {
            error.InvalidBatchRequest,
            error.InvalidArgument,
            error.InvalidGraphEdges,
            error.UnsupportedTransformOperation,
            => {
                _ = ctx.status(400);
                return ctx.text("invalid transaction commit request");
            },
            else => return err,
        };
        if (try self.api_server.validateCommitReadSet(commit_req)) |conflict| {
            _ = self.api_server.txn_sessions.remove(alloc, txn_id);
            var arena_impl = std.heap.ArenaAllocator.init(ctx.allocator);
            defer arena_impl.deinit();
            const response = try transactions_api.buildSessionCommitResponse(
                arena_impl.allocator(),
                txn_id,
                "aborted",
                conflict,
                null,
            );
            _ = ctx.status(409);
            return ctx.json(response);
        }

        if (try self.acquirePublicOperation(ctx, "commitTransactionSession")) |response| return response;
        defer self.releasePublicOperation("commitTransactionSession");

        // Persist the exact sealed request as recoverable work before 2PC can
        // choose a durable decision. This closes the response/crash window:
        // maintenance can replay the same transaction ID without duplicating
        // non-idempotent transforms.
        _ = (self.api_server.txn_sessions.markCommitExecutionStarted(alloc, txn_id) catch |err| switch (err) {
            error.SessionLeaseLost => {
                _ = ctx.status(409);
                return ctx.text("session lease lost");
            },
            else => return err,
        }) orelse {
            _ = ctx.status(404);
            return ctx.text("not found");
        };

        const commit_request = operationContext(ctx, null);
        const outcome = (source.commitTransactionWithIdAndCancellation(
            alloc,
            txn_id,
            session.begin_timestamp,
            distributed_tables,
            session.sync_level,
            commit_request.cancellation,
        ) catch |err| switch (err) {
            error.InvalidBatchRequest,
            error.InvalidArgument,
            error.InvalidGraphEdges,
            error.UnsupportedTransformOperation,
            => {
                // Participant validation terminally aborts this transaction
                // ID, so retaining the session would only produce conflicts.
                _ = self.api_server.txn_sessions.remove(alloc, txn_id);
                _ = ctx.status(400);
                return ctx.text("invalid transaction commit request");
            },
            error.TopologyChanged => {
                _ = self.api_server.txn_sessions.remove(alloc, txn_id);
                var arena_impl = std.heap.ArenaAllocator.init(ctx.allocator);
                defer arena_impl.deinit();
                const response = try transactions_api.buildSessionCommitResponse(
                    arena_impl.allocator(),
                    txn_id,
                    "aborted",
                    transactions_api.topologyChangedConflict(if (commit_req.tables.len > 0) commit_req.tables[0].table_name else ""),
                    null,
                );
                _ = ctx.status(409);
                return ctx.json(response);
            },
            error.DecisionConflict => {
                _ = self.api_server.txn_sessions.remove(alloc, txn_id);
                var arena_impl = std.heap.ArenaAllocator.init(ctx.allocator);
                defer arena_impl.deinit();
                const response = try transactions_api.buildSessionCommitResponse(
                    arena_impl.allocator(),
                    txn_id,
                    "aborted",
                    transactions_api.decisionConflict(if (commit_req.tables.len > 0) commit_req.tables[0].table_name else ""),
                    null,
                );
                _ = ctx.status(409);
                return ctx.json(response);
            },
            error.DocIdentityNamespaceMismatch => {
                _ = self.api_server.txn_sessions.remove(alloc, txn_id);
                var arena_impl = std.heap.ArenaAllocator.init(ctx.allocator);
                defer arena_impl.deinit();
                const response = try transactions_api.buildSessionCommitResponse(
                    arena_impl.allocator(),
                    txn_id,
                    "aborted",
                    transactions_api.docIdentityUnavailableConflict(if (commit_req.tables.len > 0) commit_req.tables[0].table_name else ""),
                    null,
                );
                _ = ctx.status(409);
                return ctx.json(response);
            },
            error.UnsupportedOperation => {
                _ = self.api_server.txn_sessions.remove(alloc, txn_id);
                _ = ctx.status(405);
                return ctx.text("method not allowed");
            },
            error.TableNotFound => {
                _ = self.api_server.txn_sessions.remove(alloc, txn_id);
                _ = ctx.status(404);
                return ctx.text("not found");
            },
            error.UnknownGroup => {
                _ = self.api_server.txn_sessions.remove(alloc, txn_id);
                var arena_impl = std.heap.ArenaAllocator.init(ctx.allocator);
                defer arena_impl.deinit();
                const response = try transactions_api.buildSessionCommitResponse(
                    arena_impl.allocator(),
                    txn_id,
                    "aborted",
                    transactions_api.participantUnavailableConflict(if (commit_req.tables.len > 0) commit_req.tables[0].table_name else ""),
                    null,
                );
                _ = ctx.status(409);
                return ctx.json(response);
            },
            error.CommitVisibilityNotSatisfied,
            error.EnrichmentWaitCanceled,
            error.EnrichmentWaitTimeout,
            error.EnrichmentRetryInProgress,
            => {
                // markCommitExecutionStarted durably retained the sealed request
                // before entering 2PC. These errors are post-commit visibility
                // outcomes, so leave that recovery handoff live (it will replay
                // and recover coordinator metadata) while returning an
                // unambiguous committed response to the caller now.
                var arena_impl = std.heap.ArenaAllocator.init(ctx.allocator);
                defer arena_impl.deinit();
                const response = try transactions_api.buildSessionCommitResponse(
                    arena_impl.allocator(),
                    txn_id,
                    "committed_visibility_pending",
                    null,
                    commit_req.tables,
                );
                _ = ctx.status(202);
                return ctx.json(response);
            },
            error.EnrichmentWorkerFailed => {
                // Terminal repair debt is also post-commit. Recovery still owns
                // the retained coordinator acknowledgement; the public result
                // must distinguish operator repair from a live retry.
                // Persist the repair result before returning it. A source that
                // throws this legacy outcome cannot return coordinator metadata
                // in the same call, so maintenance replays the transaction at
                // write durability solely to recover and release that handoff.
                _ = (self.api_server.txn_sessions.recordTerminalCommitWithRepair(
                    alloc,
                    txn_id,
                    .committed,
                    true,
                    null,
                    null,
                ) catch |persist_err| {
                    std.log.err("failed to persist stable transaction repair handoff txn_id={x} err={s}", .{ txn_id, @errorName(persist_err) });
                    _ = ctx.status(503);
                    return ctx.text("transaction committed; durable repair handoff is pending");
                }) orelse {
                    _ = ctx.status(503);
                    return ctx.text("transaction committed; durable repair handoff is pending");
                };
                var arena_impl = std.heap.ArenaAllocator.init(ctx.allocator);
                defer arena_impl.deinit();
                const response = try transactions_api.buildSessionCommitResponse(
                    arena_impl.allocator(),
                    txn_id,
                    "committed_repair_required",
                    null,
                    commit_req.tables,
                );
                _ = ctx.status(202);
                return ctx.json(response);
            },
            error.CommitPropagationIncomplete => {
                _ = ctx.status(503);
                return ctx.text("transaction committed; participant recovery is pending");
            },
            error.CommitDecisionUnknown => {
                _ = ctx.status(503);
                return ctx.text("transaction outcome is unknown; retry this transaction id");
            },
            error.AbortDecisionNotDurable, error.TransactionBeginFailed => {
                _ = ctx.status(503);
                return ctx.text("transaction coordinator is temporarily unavailable");
            },
            else => return err,
        }) orelse {
            _ = ctx.status(404);
            return ctx.text("not found");
        };

        switch (outcome) {
            .committed => |committed| {
                var status = transactions_api.terminalCommitStatusForOutcome(
                    committed.propagation_pending,
                    committed.visibility_pending,
                    committed.visibility_retry_pending,
                    committed.visibility_repair_required,
                );
                _ = (self.api_server.txn_sessions.recordTerminalCommitWithRepair(
                    alloc,
                    txn_id,
                    status,
                    committed.visibility_repair_required,
                    committed.coordinator_group_id,
                    committed.coordinator_table_name,
                ) catch |err| {
                    std.log.err("failed to persist stable transaction terminal result txn_id={x} err={s}", .{ txn_id, @errorName(err) });
                    _ = ctx.status(503);
                    return ctx.text("transaction committed; durable response handoff is pending");
                }) orelse {
                    _ = ctx.status(503);
                    return ctx.text("transaction committed; durable response handoff is pending");
                };

                if (status == .committed) {
                    if (committed.coordinator_group_id) |coordinator_group_id| {
                        const coordinator_table_name = committed.coordinator_table_name orelse return error.InvalidTransactionSessionRecord;
                        const acknowledged = source.acknowledgeTransactionCommit(
                            alloc,
                            txn_id,
                            coordinator_group_id,
                            coordinator_table_name,
                        ) catch |err| blk: {
                            std.log.warn("stable transaction coordinator acknowledgement deferred txn_id={x} err={s}", .{ txn_id, @errorName(err) });
                            break :blk null;
                        };
                        if (acknowledged == null) {
                            status = .committed_recovery_pending;
                        } else if ((self.api_server.txn_sessions.markTerminalCoordinatorAcknowledged(alloc, txn_id) catch |err| blk: {
                            std.log.warn("failed to persist stable transaction acknowledgement txn_id={x} err={s}", .{ txn_id, @errorName(err) });
                            break :blk null;
                        }) == null) {
                            status = .committed_recovery_pending;
                        }
                    }
                }
                var arena_impl = std.heap.ArenaAllocator.init(ctx.allocator);
                defer arena_impl.deinit();
                const response = try transactions_api.buildSessionCommitResponse(
                    arena_impl.allocator(),
                    txn_id,
                    transactions_api.terminalCommitResponseStatus(status, committed.visibility_repair_required),
                    null,
                    commit_req.tables,
                );
                _ = ctx.status(if (status == .committed and !committed.visibility_repair_required) 200 else 202);
                return ctx.json(response);
            },
            .conflict => |conflict| {
                _ = self.api_server.txn_sessions.remove(alloc, txn_id);
                const enriched_conflict = try self.api_server.enrichCommitConflict(commit_req, conflict);
                var arena_impl = std.heap.ArenaAllocator.init(ctx.allocator);
                defer arena_impl.deinit();
                const response = try transactions_api.buildSessionCommitResponse(
                    arena_impl.allocator(),
                    txn_id,
                    "aborted",
                    enriched_conflict,
                    null,
                );
                _ = ctx.status(409);
                return ctx.json(response);
            },
        }
    }

    pub fn abortTransactionSession(self: *AntflyApiHandler, ctx: *httpx.Context, transaction_id: []const u8) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const body_data = (try ctx.body()) orelse "";
        const txn_id = distributed_txn.parseTxnIdHex(transaction_id) catch {
            _ = ctx.status(400);
            return ctx.text("invalid transaction id");
        };
        if (try self.forwardTransactionSession(ctx, txn_id, body_data)) |response| return response;
        if (!(try self.api_server.transactionSessionAccessible(txn_id, authenticated_identity))) {
            _ = ctx.status(404);
            return ctx.text("not found");
        }
        if (try self.api_server.txn_sessions.getTerminalCommit(self.api_server.alloc, txn_id)) |terminal_value| {
            var terminal = terminal_value;
            defer terminal.deinit(self.api_server.alloc);
            _ = ctx.status(409);
            return ctx.text("transaction is already committed");
        }
        if (!self.api_server.txn_sessions.remove(self.api_server.alloc, txn_id)) {
            _ = ctx.status(404);
            return ctx.text("not found");
        }
        var arena_impl = std.heap.ArenaAllocator.init(ctx.allocator);
        defer arena_impl.deinit();
        const response = try transactions_api.buildAbortResponse(arena_impl.allocator(), txn_id);
        return ctx.json(response);
    }

    pub fn backup(self: *AntflyApiHandler, ctx: *httpx.Context) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const body_data = (try ctx.body()) orelse "";
        var resp = try cluster_api_http.handleClusterBackup(ctx.allocator, body_data, self.api_server.clusterApi(), self.api_server.cfg.secret_store, self.api_server.cfg.node_config, self.api_server.sharedApiIo());
        return respondOwnedApiResponse(ctx, &resp);
    }

    pub fn restore(self: *AntflyApiHandler, ctx: *httpx.Context) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const body_data = (try ctx.body()) orelse "";
        var resp = try self.api_server.handlePublicClusterRestore(
            body_data,
            ctx.header("idempotency-key"),
            if (authenticated_identity) |identity| identity.username else null,
        );
        return respondOwnedContextualResponse(ctx, &resp, self.api_server.alloc);
    }

    pub fn getRestoreJob(self: *AntflyApiHandler, ctx: *httpx.Context, job_id_raw: []const u8) !httpx.Response {
        return try self.restoreJob(ctx, job_id_raw, false);
    }

    pub fn cancelRestoreJob(self: *AntflyApiHandler, ctx: *httpx.Context, job_id_raw: []const u8) !httpx.Response {
        return try self.restoreJob(ctx, job_id_raw, true);
    }

    pub fn listRestoreJobs(self: *AntflyApiHandler, ctx: *httpx.Context, params: metadata_openapi.server.ListRestoreJobsParams) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const limit = if (params.limit) |value|
            std.fmt.parseInt(usize, value, 10) catch return try textResponse(ctx, 400, "invalid restore job list limit")
        else
            50;
        if (limit == 0 or limit > 100) return try textResponse(ctx, 400, "invalid restore job list limit");
        const cursor = if (params.cursor) |value| blk: {
            const parsed = std.fmt.parseUnsigned(u64, value, 10) catch return try textResponse(ctx, 400, "invalid restore job list cursor");
            if (parsed == 0) return try textResponse(ctx, 400, "invalid restore job list cursor");
            break :blk parsed;
        } else null;
        const phase = if (params.phase) |value|
            std.meta.stringToEnum(restore_jobs.Phase, value) orelse return try textResponse(ctx, 400, "invalid restore job list phase")
        else
            null;
        const scope = if (params.scope) |value|
            std.meta.stringToEnum(restore_jobs.Scope, value) orelse return try textResponse(ctx, 400, "invalid restore job list scope")
        else
            null;
        var resp = try self.api_server.handlePublicListRestoreJobs(authenticated_identity, .{
            .limit = limit,
            .cursor = cursor,
            .phase = phase,
            .scope = scope,
        });
        return respondOwnedContextualResponse(ctx, &resp, self.api_server.alloc);
    }

    fn restoreJob(self: *AntflyApiHandler, ctx: *httpx.Context, job_id_raw: []const u8, cancel: bool) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const job_id = std.fmt.parseUnsigned(u64, job_id_raw, 10) catch return try textResponse(ctx, 400, "invalid job id");
        if (authenticated_identity) |identity| {
            if (!(try self.api_server.restoreJobAllowed(ctx.allocator, identity, job_id))) return try textResponse(ctx, 404, "not found");
        }
        var resp = try self.api_server.handlePublicRestoreJob(job_id, cancel);
        return respondOwnedContextualResponse(ctx, &resp, self.api_server.alloc);
    }

    pub fn listBackups(self: *AntflyApiHandler, ctx: *httpx.Context, params: metadata_openapi.server.ListBackupsParams) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const limit = if (params.limit) |value|
            std.fmt.parseInt(usize, value, 10) catch return try textResponse(ctx, 400, "invalid backup list limit")
        else
            backups_api.default_backup_list_limit;
        if (limit == 0 or limit > backups_api.max_backup_list_limit) return try textResponse(ctx, 400, "invalid backup list limit");
        if (params.cursor) |cursor| backups_api.validateBackupId(cursor) catch return try textResponse(ctx, 400, "invalid backup list cursor");
        var resp = try cluster_api_http.handleClusterBackupList(ctx.allocator, params.location, params.connection, self.api_server.clusterApi(), self.api_server.cfg.secret_store, self.api_server.cfg.node_config, self.api_server.sharedApiIo(), .{
            .limit = limit,
            .cursor = params.cursor,
        });
        return respondOwnedApiResponse(ctx, &resp);
    }

    pub fn globalQuery(self: *AntflyApiHandler, ctx: *httpx.Context) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const body_data = body: {
            const needs_h2_body_slot = ctx.hasStreamingRequestBody();
            if (needs_h2_body_slot and !self.query_body_admission.tryAcquire())
                return queryBodyOverloadedResponse(ctx);
            defer if (needs_h2_body_slot) self.query_body_admission.release();
            break :body (try ctx.body()) orelse {
                _ = ctx.status(400);
                return ctx.text("missing body");
            };
        };
        // Body admission is released before expensive execution starts so
        // slow ingress and query compute cannot starve one another.
        if (try self.acquirePublicOperation(ctx, "globalQuery")) |response| return response;
        defer self.releasePublicOperation("globalQuery");
        var cancellation = requestCancellation(ctx);
        if (isNdjsonContentType(ctx.header("content-type"))) {
            var resp = try self.api_server.handleAdmittedPublicGlobalMultiQueryWithCancellation(
                body_data,
                authenticated_identity,
                &cancellation,
            );
            return respondOwnedContextualResponse(ctx, &resp, self.api_server.alloc);
        }
        var parsed_table = parseGlobalQueryTable(ctx.allocator, body_data) catch {
            _ = ctx.status(400);
            return ctx.text("invalid query request");
        };
        defer parsed_table.deinit();
        var resp = try self.api_server.handleAdmittedPublicTableQueryWithContentTypeCancellation(
            parsed_table.table_name,
            body_data,
            ctx.header("content-type"),
            authenticated_identity,
            &cancellation,
        );
        return respondOwnedContextualResponse(ctx, &resp, self.api_server.alloc);
    }

    pub fn evaluate(self: *AntflyApiHandler, ctx: *httpx.Context) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const alloc = ctx.allocator;
        const body_data = (try ctx.body()) orelse {
            return jsonErrorResponse(ctx, 400, "invalid eval request");
        };
        var parsed = metadata_openapi.server.parseEvaluateBody(alloc, body_data) catch {
            return jsonErrorResponse(ctx, 400, "invalid eval request");
        };
        defer parsed.deinit();
        var arena_impl = std.heap.ArenaAllocator.init(alloc);
        defer arena_impl.deinit();
        const response = retrieval_agent.buildEvalResponse(arena_impl.allocator(), parsed.value) catch |err| switch (err) {
            error.InvalidEvalRequest => {
                return jsonErrorResponse(ctx, 400, "invalid eval request");
            },
            else => return err,
        };
        return ctx.json(response);
    }

    pub fn queryBuilderAgent(self: *AntflyApiHandler, ctx: *httpx.Context) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const body_data = (try ctx.body()) orelse {
            return jsonErrorResponse(ctx, 400, "invalid query builder request");
        };
        var response = try self.api_server.executeQueryBuilderAgent(body_data, authenticated_identity);
        defer response.deinit(self.api_server.alloc);
        _ = ctx.status(response.status);
        try ctx.setHeader("content-type", response.content_type);
        for (response.headers) |header| try ctx.setHeader(header.name, header.value);
        _ = ctx.response.body(response.body);
        return ctx.response.build();
    }

    pub fn retrievalAgent(self: *AntflyApiHandler, ctx: *httpx.Context) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const alloc = ctx.allocator;
        const source = self.api_server.table_reads orelse {
            _ = ctx.status(404);
            return ctx.text("not found");
        };
        const body_data = (try ctx.body()) orelse {
            _ = ctx.status(400);
            return ctx.text("invalid retrieval agent request");
        };
        if (try self.acquirePublicOperation(ctx, "retrievalAgent")) |response| return response;
        defer self.releasePublicOperation("retrievalAgent");

        const RetrievalQueryRunner = struct {
            server: *ApiHttpServer,
            source: table_reads.TableReadSource,
            query_embedding_security_scope: ApiHttpServer.QueryEmbeddingSecurityScope,
            authenticated_identity: ?AuthenticatedIdentity,

            fn iface(runner: *@This()) retrieval_agent.QueryRunner {
                return .{
                    .ptr = runner,
                    .vtable = &.{
                        .run_query = runQuery,
                        .scan_key_page = runScanKeyPage,
                        .probe_incoming_edges = probeIncomingEdges,
                    },
                };
            }

            fn runQuery(
                ptr: *anyopaque,
                a: std.mem.Allocator,
                table_name: []const u8,
                query_json: []const u8,
            ) !query_api.QueryResponse {
                const runner: *@This() = @ptrCast(@alignCast(ptr));
                var semantic_resolver = runner.server.semanticStatusResolver(runner.query_embedding_security_scope.domain, runner.query_embedding_security_scope.value);
                var query_req = query_api.parsePublicQueryRequest(a, semantic_resolver.iface(), table_name, query_json) catch |err| {
                    if (query_api.isPublicQueryValidationError(err)) {
                        return error.InvalidRetrievalAgentRequest;
                    }
                    return err;
                };
                defer query_req.deinit(a);
                runner.server.maybeRouteQueryToReadSchema(table_name, &query_req.req) catch |err| switch (err) {
                    error.TableNotFound => return err,
                    error.InvalidSchemaUpdateRequest, error.InvalidTableIndexMetadata => return error.InvalidRetrievalAgentRequest,
                    else => return err,
                };
                const row_filter_json = try http_server_mod.resolveEffectiveRowFilterJson(
                    a,
                    runner.authenticated_identity,
                    table_name,
                );
                defer if (row_filter_json) |value| a.free(value);
                if (row_filter_json) |value| {
                    http_server_mod.injectRowFilterIntoSearchRequest(a, &query_req.req, value) catch
                        return error.InvalidRetrievalAgentRequest;
                }
                if (runner.authenticated_identity) |*identity| {
                    ApiHttpServer.attachGraphTableReadAuthorizer(&query_req.req, identity);
                }
                return (runner.source.query(
                    a,
                    table_name,
                    query_req.req,
                    .read_index,
                ) catch |err| {
                    if (err == error.DocIdentityNamespaceMismatch) return err;
                    std.log.err("retrieval query failed table={s} query={s} err={}", .{ table_name, query_json, err });
                    return err;
                }) orelse error.TableNotFound;
            }

            fn runScanKeyPage(
                ptr: *anyopaque,
                a: std.mem.Allocator,
                table_name: []const u8,
                after_key: []const u8,
                limit: u32,
                filter_query_json: ?[]const u8,
                exclusion_query_json: ?[]const u8,
            ) !retrieval_agent.QueryRunner.KeyPage {
                const runner: *@This() = @ptrCast(@alignCast(ptr));
                return try runner.server.scanRetrievalKeyPage(
                    a,
                    runner.source,
                    table_name,
                    after_key,
                    limit,
                    filter_query_json,
                    exclusion_query_json,
                    runner.authenticated_identity,
                );
            }

            fn probeIncomingEdges(
                ptr: *anyopaque,
                a: std.mem.Allocator,
                table_name: []const u8,
                index_name: []const u8,
                keys: []const []const u8,
            ) ![]bool {
                const runner: *@This() = @ptrCast(@alignCast(ptr));
                return try runner.server.probeRetrievalIncomingEdges(
                    a,
                    runner.source,
                    table_name,
                    index_name,
                    keys,
                );
            }
        };

        const RetrievalGenerationRunner = struct {
            antfly_provider: ?managed_embedder.AntflyProvider,
            secret_store: ?*common_secrets.FileStore,
            io: std.Io,

            fn iface(runner: *@This()) retrieval_agent.GenerationRunner {
                return .{
                    .ptr = runner,
                    .vtable = &.{ .execute_chain = executeChain },
                };
            }

            fn executeChain(
                ptr: *anyopaque,
                a: std.mem.Allocator,
                chain: []const generating_runtime.ChainLink,
                messages: []const generating_runtime.ChatMessage,
            ) !generating_runtime.GenerateResult {
                const runner: *@This() = @ptrCast(@alignCast(ptr));
                var client = httpx.Client.initWithConfig(a, runner.io, .{ .keep_alive = false });
                defer client.deinit();
                return try generating_runtime.executeChainWithOptions(a, &client, chain, .{ .antfly_provider = runner.antfly_provider, .secret_store = runner.secret_store }, messages);
            }
        };
        var generation_runner = RetrievalGenerationRunner{
            .antfly_provider = self.api_server.antfly_provider,
            .secret_store = self.api_server.cfg.secret_store,
            .io = self.api_server.inferenceIo(),
        };

        var query_runner = RetrievalQueryRunner{
            .server = self.api_server,
            .source = source,
            .query_embedding_security_scope = ApiHttpServer.queryEmbeddingSecurityScope(authenticated_identity),
            .authenticated_identity = authenticated_identity,
        };
        const retrieval_resp = retrieval_agent.execute(alloc, query_runner.iface(), generation_runner.iface(), body_data) catch |err| switch (err) {
            error.TreeRootSetTooLarge => {
                _ = ctx.status(422);
                return ctx.text("tree root set exceeds the bounded retrieval limit");
            },
            error.InvalidRetrievalAgentRequest, error.UnsupportedRetrievalAgentRequest => {
                _ = ctx.status(400);
                return ctx.text("invalid retrieval agent request");
            },
            error.TableNotFound => {
                _ = ctx.status(404);
                return ctx.text("not found");
            },
            error.DocIdentityNamespaceMismatch => {
                _ = ctx.status(503);
                return ctx.text("doc identity unavailable");
            },
            else => {
                if (try respondQueryEmbeddingOperationalError(ctx, err)) |response| return response;
                std.log.err("public retrieval failed err={}", .{err});
                return err;
            },
        };
        defer alloc.free(retrieval_resp.body);
        if (std.mem.eql(u8, retrieval_resp.content_type, "text/event-stream")) {
            try ctx.setHeader("content-type", "text/event-stream");
            _ = ctx.response.body(retrieval_resp.body);
            return ctx.response.build();
        }
        var arena_impl = std.heap.ArenaAllocator.init(alloc);
        defer arena_impl.deinit();
        const response = try std.json.parseFromSliceLeaky(metadata_openapi.RetrievalAgentResult, arena_impl.allocator(), retrieval_resp.body, .{
            .allocate = .alloc_always,
        });
        return ctx.json(response);
    }

    pub fn listTables(self: *AntflyApiHandler, ctx: *httpx.Context, params: metadata_openapi.server.ListTablesParams) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const alloc = ctx.allocator;
        var snapshot = (try self.api_server.source.adminSnapshot()) orelse {
            _ = ctx.status(404);
            return ctx.text("not found");
        };
        defer self.api_server.source.freeAdminSnapshot(&snapshot);
        if (params.pattern != null) {
            _ = ctx.status(400);
            return ctx.text("unsupported table pattern");
        }
        const storage_statuses = try self.api_server.collectTableStorageStatuses(alloc, &snapshot, params.prefix);
        defer if (storage_statuses) |items| alloc.free(items);
        var arena_impl = std.heap.ArenaAllocator.init(alloc);
        defer arena_impl.deinit();
        const response = try tables_api.buildTableListWithStorageStatuses(arena_impl.allocator(), &snapshot, params.prefix, storage_statuses);
        return ctx.json(response);
    }

    pub fn getTable(self: *AntflyApiHandler, ctx: *httpx.Context, table_name: []const u8) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const alloc = ctx.allocator;
        const decoded_table_name = (try decodePathParamOrBadRequest(ctx, table_name)) orelse return ctx.text("invalid path parameter");
        defer alloc.free(decoded_table_name);
        if (http_server_mod.runtimeSchemaDebugRequested(ctx.request.uri.query orelse "")) {
            if (!self.api_server.runtimeSchemaDebugAllowed(authenticated_identity)) return jsonErrorResponse(ctx, 403, "forbidden");
            const debug_body = (try self.api_server.encodeTableRuntimeSchemaDebugAlloc(alloc, decoded_table_name)) orelse
                return jsonErrorResponse(ctx, 404, "not found");
            defer alloc.free(debug_body);
            return jsonResponse(ctx, 200, debug_body);
        }
        var snapshot = (try self.api_server.source.adminSnapshot()) orelse {
            _ = ctx.status(404);
            return ctx.text("not found");
        };
        defer self.api_server.source.freeAdminSnapshot(&snapshot);
        var storage_status_buf: [1]tables_api.TableStorageStatus = undefined;
        const storage_statuses = try self.api_server.bestEffortSingleTableStorageStatuses(decoded_table_name, &snapshot, &storage_status_buf);
        const body = (try tables_api.encodeSingleTableStatusWithStorageStatuses(alloc, &snapshot, decoded_table_name, storage_statuses)) orelse {
            _ = ctx.status(404);
            return ctx.text("not found");
        };
        defer alloc.free(body);
        return respondApiResponseBody(ctx, 200, body);
    }

    pub fn createTable(self: *AntflyApiHandler, ctx: *httpx.Context, table_name: []const u8) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const alloc = ctx.allocator;
        const decoded_table_name = (try decodePathParamOrBadRequest(ctx, table_name)) orelse return ctx.text("invalid path parameter");
        defer alloc.free(decoded_table_name);
        const body_data = (try ctx.body()) orelse {
            _ = ctx.status(400);
            return ctx.text("invalid create table request");
        };
        var create_req = table_contract.parseCreateTableRequest(alloc, body_data) catch |err| {
            if (table_contract.classifyCreateTableRequestError(err) == .internal_failure) {
                std.log.err("create table request parsing failed: {} body_len={d}", .{ err, body_data.len });
                return err;
            }
            std.log.debug("create table request rejected: {} body_len={d}", .{ err, body_data.len });
            _ = ctx.status(400);
            return ctx.text(table_contract.createTableRequestErrorMessage(err, body_data));
        };
        defer create_req.deinit(alloc);
        const normalized_indexes_json = table_writes.normalizeManagedEmbeddingIndexDimensionsJsonWithOptions(
            alloc,
            create_req.indexes_json orelse tables_api.default_indexes_json,
            .{
                .antfly_provider = self.api_server.antfly_provider,
                .io = self.api_server.inferenceIo(),
                .secret_store = self.api_server.cfg.secret_store,
                .remote_content = self.api_server.cfg.remote_content,
                .inference_api_url = self.api_server.configuredInferenceAPIURL(),
                .inference_api_key = self.api_server.cfg.inference_api_key,
            },
        ) catch |err| switch (err) {
            error.InvalidCreateTableRequest, error.UnsupportedCreateTableRequest => {
                _ = ctx.status(400);
                return ctx.text("unsupported table index configuration");
            },
            error.ModelNotFound => return respondJsonErrorBody(ctx, 404, "{\"error\":\"MODEL_NOT_FOUND\",\"message\":\"model not found\"}"),
            error.EmbeddingProbeUnavailable => {
                _ = ctx.status(503);
                return ctx.text("table index validation probe unavailable");
            },
            else => return err,
        };
        if (create_req.indexes_json) |old_indexes_json| alloc.free(old_indexes_json);
        create_req.indexes_json = normalized_indexes_json;
        tables_api.validatePublicAlgebraicIndexesJson(alloc, create_req.indexes_json orelse tables_api.default_indexes_json) catch {
            _ = ctx.status(400);
            return ctx.text("unsupported table index configuration");
        };
        std.log.info("public create table begin table={s}", .{decoded_table_name});
        const metadata_create_start_ns = platform_time.monotonicNs();
        var metadata_create_attempts: usize = 0;
        while (true) {
            metadata_create_attempts += 1;
            self.api_server.source.createTable(alloc, decoded_table_name, create_req) catch |err| switch (err) {
                error.TableAlreadyExists => {
                    _ = ctx.status(409);
                    return ctx.text("table already exists");
                },
                error.InvalidCreateTableRequest => {
                    _ = ctx.status(400);
                    return ctx.text("invalid table configuration");
                },
                error.UnsupportedOperation => {
                    _ = ctx.status(405);
                    return ctx.text("method not allowed");
                },
                error.UnexpectedHttpStatus => {
                    const elapsed_ns = platform_time.monotonicNs() -| metadata_create_start_ns;
                    if (!self.api_server.shouldRetryConfiguredMetadataMutation(err, elapsed_ns, metadata_create_attempts)) {
                        std.log.err("public create table metadata create failed table={s} err={}", .{ decoded_table_name, err });
                        return err;
                    }
                    sleepNs(self.api_server.metadataMutationRetryPollNs());
                    continue;
                },
                else => {
                    if (metadata_authority.isRetryableError(err)) {
                        const elapsed_ns = platform_time.monotonicNs() -| metadata_create_start_ns;
                        if (self.api_server.shouldRetryConfiguredMetadataMutation(err, elapsed_ns, metadata_create_attempts)) {
                            sleepNs(self.api_server.metadataMutationRetryPollNs());
                            continue;
                        }
                        return error.NotLeader;
                    }
                    std.log.err("public create table metadata create failed table={s} err={}", .{ decoded_table_name, err });
                    return err;
                },
            };
            break;
        }
        std.log.info("public create table metadata done table={s}", .{decoded_table_name});
        const local_create_handled = if (self.api_server.table_writes) |table_writes_source| blk: {
            break :blk (table_writes_source.createTable(alloc, decoded_table_name, create_req) catch |err| switch (err) {
                error.InvalidCreateTableRequest, error.UnsupportedCreateTableRequest => {
                    _ = ctx.status(400);
                    return ctx.text("unsupported table index configuration");
                },
                error.EmbeddingProbeUnavailable => {
                    _ = ctx.status(503);
                    return ctx.text("table index validation probe unavailable");
                },
                else => {
                    std.log.err("public create table local create failed table={s} err={}", .{ decoded_table_name, err });
                    return err;
                },
            }) != null;
        } else false;
        if (local_create_handled) {
            std.log.info("public create table wait projected presence table={s}", .{decoded_table_name});
            self.api_server.waitForProjectedTablePresence(decoded_table_name) catch |err| switch (err) {
                error.TableVisibilityTimeout => {
                    std.log.err("public create table metadata visibility timed out table={s}", .{decoded_table_name});
                    _ = ctx.status(500);
                    return ctx.text("table create did not converge");
                },
                else => return err,
            };
        } else {
            const metadata_wait_handled = self.api_server.source.waitTableLifecycle(decoded_table_name, .present) catch |err| switch (err) {
                error.TableVisibilityTimeout => {
                    std.log.err("public create table metadata lifecycle timed out table={s}", .{decoded_table_name});
                    _ = ctx.status(500);
                    return ctx.text("table create did not converge");
                },
                else => {
                    std.log.err("public create table metadata lifecycle failed table={s} err={}", .{ decoded_table_name, err });
                    return err;
                },
            };
            if (!metadata_wait_handled) {
                std.log.info("public create table wait metadata visibility table={s}", .{decoded_table_name});
                self.api_server.waitForTableVisibility(decoded_table_name, .present) catch |err| switch (err) {
                    error.TableVisibilityTimeout => {
                        std.log.err("public create table metadata visibility timed out table={s}", .{decoded_table_name});
                        _ = ctx.status(500);
                        return ctx.text("table create did not converge");
                    },
                    else => return err,
                };
            }
        }
        std.log.info("public create table visible table={s}", .{decoded_table_name});

        var snapshot = (try self.api_server.source.adminSnapshot()) orelse {
            _ = ctx.status(404);
            return ctx.text("not found");
        };
        defer self.api_server.source.freeAdminSnapshot(&snapshot);
        var arena_impl = std.heap.ArenaAllocator.init(alloc);
        defer arena_impl.deinit();
        const response = (try tables_api.buildSingleTableStatusWithStorageStatuses(arena_impl.allocator(), &snapshot, decoded_table_name, null)) orelse {
            _ = ctx.status(404);
            return ctx.text("not found");
        };
        return ctx.json(response);
    }

    pub fn dropTable(self: *AntflyApiHandler, ctx: *httpx.Context, table_name: []const u8) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const alloc = ctx.allocator;
        const decoded_table_name = (try decodePathParamOrBadRequest(ctx, table_name)) orelse return ctx.text("invalid path parameter");
        defer alloc.free(decoded_table_name);
        var local_drop_group_ids: ?[]u64 = null;
        defer if (local_drop_group_ids) |group_ids| alloc.free(group_ids);
        if (self.api_server.table_writes != null) {
            if (try self.api_server.source.adminSnapshot()) |snapshot_value| {
                var snapshot = snapshot_value;
                defer self.api_server.source.freeAdminSnapshot(&snapshot);
                local_drop_group_ids = try ApiHttpServer.tableGroupIdsFromSnapshot(alloc, &snapshot, decoded_table_name);
            }
        }
        self.api_server.source.dropTable(alloc, decoded_table_name) catch |err| switch (err) {
            error.TableNotFound => {
                _ = ctx.status(404);
                return ctx.text("not found");
            },
            error.UnsupportedOperation => {
                _ = ctx.status(405);
                return ctx.text("method not allowed");
            },
            else => {
                std.log.err("public drop table metadata remove failed table={s} err={s}", .{ decoded_table_name, @errorName(err) });
                return err;
            },
        };
        if (self.api_server.table_writes) |write_source| {
            const group_ids = local_drop_group_ids orelse &.{};
            _ = write_source.dropTable(alloc, decoded_table_name, group_ids) catch |err| switch (err) {
                error.TableNotFound => null,
                else => {
                    std.log.err("public drop table local cleanup failed table={s} err={s}", .{ decoded_table_name, @errorName(err) });
                    return err;
                },
            };
        }
        self.api_server.waitForTableVisibility(decoded_table_name, .absent) catch |err| switch (err) {
            error.TableVisibilityTimeout => {
                std.log.err("public drop table metadata visibility timed out table={s}", .{decoded_table_name});
                _ = ctx.status(500);
                return ctx.text("table delete did not converge");
            },
            else => return err,
        };
        _ = ctx.status(204);
        return ctx.text("");
    }

    pub fn queryTable(self: *AntflyApiHandler, ctx: *httpx.Context, table_name: []const u8) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const decoded_table_name = (try decodePathParamOrBadRequest(ctx, table_name)) orelse return ctx.text("invalid path parameter");
        defer ctx.allocator.free(decoded_table_name);
        const body_data = body: {
            const needs_h2_body_slot = ctx.hasStreamingRequestBody();
            if (needs_h2_body_slot and !self.query_body_admission.tryAcquire())
                return queryBodyOverloadedResponse(ctx);
            defer if (needs_h2_body_slot) self.query_body_admission.release();
            break :body (try ctx.body()) orelse {
                _ = ctx.status(400);
                return ctx.text("missing body");
            };
        };
        if (try self.acquirePublicOperation(ctx, "queryTable")) |response| return response;
        defer self.releasePublicOperation("queryTable");
        var cancellation = requestCancellation(ctx);
        var resp = try self.api_server.handleAdmittedPublicTableQueryWithContentTypeCancellation(
            decoded_table_name,
            body_data,
            ctx.header("content-type"),
            authenticated_identity,
            &cancellation,
        );
        return respondOwnedContextualResponse(ctx, &resp, self.api_server.alloc);
    }

    pub fn batchWrite(self: *AntflyApiHandler, ctx: *httpx.Context, table_name: []const u8) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const decoded_table_name = (try decodePathParamOrBadRequest(ctx, table_name)) orelse return ctx.text("invalid path parameter");
        defer ctx.allocator.free(decoded_table_name);
        const body_data = (try ctx.body()) orelse {
            _ = ctx.status(400);
            return ctx.text("missing body");
        };
        if (try self.acquirePublicOperation(ctx, "batchWrite")) |response| return response;
        defer self.releasePublicOperation("batchWrite");
        return try handleTableBatchOffEventLoop(
            ctx,
            self.api_server.cfg.backend_runtime,
            decoded_table_name,
            body_data,
            self.api_server.tableApi(operationContext(ctx, authenticated_identity)),
        );
    }

    pub fn linearMerge(self: *AntflyApiHandler, ctx: *httpx.Context, table_name: []const u8) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const alloc = ctx.allocator;
        const decoded_table_name = (try decodePathParamOrBadRequest(ctx, table_name)) orelse return ctx.text("invalid path parameter");
        defer alloc.free(decoded_table_name);
        const reads = self.api_server.table_reads orelse {
            _ = ctx.status(404);
            return ctx.text("not found");
        };
        const writes = self.api_server.table_writes orelse {
            _ = ctx.status(404);
            return ctx.text("not found");
        };
        if (!(try self.api_server.tableExists(decoded_table_name))) {
            _ = ctx.status(404);
            return ctx.text("not found");
        }
        const body_data = (try ctx.body()) orelse {
            _ = ctx.status(400);
            return ctx.text("invalid linear merge request");
        };
        if (try self.acquirePublicOperation(ctx, "linearMerge")) |response| return response;
        defer self.releasePublicOperation("linearMerge");
        var merge_req = linear_merge_api.parseRequest(alloc, body_data) catch |err| switch (err) {
            error.InvalidLinearMergeRequest => {
                _ = ctx.status(400);
                return ctx.text("invalid linear merge request");
            },
            else => return err,
        };
        defer merge_req.deinit(alloc);

        self.api_server.validateTableWritesAgainstSchema(decoded_table_name, merge_req.writes) catch |err| switch (err) {
            error.InvalidBatchRequest => {
                _ = ctx.status(400);
                return ctx.text("invalid linear merge request");
            },
            else => return err,
        };

        var arena_impl = std.heap.ArenaAllocator.init(alloc);
        defer arena_impl.deinit();
        const response = linear_merge_api.executeResponse(
            arena_impl.allocator(),
            reads,
            writes,
            decoded_table_name,
            merge_req,
            operationContext(ctx, authenticated_identity),
        ) catch |err| switch (err) {
            error.Canceled, error.DeadlineExceeded => return err,
            error.InvalidLinearMergeRequest, error.InvalidBatchRequest => {
                _ = ctx.status(400);
                return ctx.text("invalid linear merge request");
            },
            error.TableNotFound => {
                _ = ctx.status(404);
                return ctx.text("not found");
            },
            else => return err,
        };
        return ctx.json(response);
    }

    pub fn backupTable(self: *AntflyApiHandler, ctx: *httpx.Context, table_name: []const u8) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const decoded_table_name = (try decodePathParamOrBadRequest(ctx, table_name)) orelse return ctx.text("invalid path parameter");
        defer ctx.allocator.free(decoded_table_name);
        const body_data = (try ctx.body()) orelse "";
        var resp = try public_table_http.handleTableBackup(ctx.allocator, decoded_table_name, body_data, self.api_server.tableApi(operationContext(ctx, authenticated_identity)), self.api_server.cfg.secret_store, self.api_server.cfg.node_config, self.api_server.sharedApiIo());
        return respondOwnedApiResponse(ctx, &resp);
    }

    pub fn restoreTable(self: *AntflyApiHandler, ctx: *httpx.Context, table_name: []const u8) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const decoded_table_name = (try decodePathParamOrBadRequest(ctx, table_name)) orelse return ctx.text("invalid path parameter");
        defer ctx.allocator.free(decoded_table_name);
        const body_data = (try ctx.body()) orelse "";
        var resp = try self.api_server.handlePublicTableRestore(
            decoded_table_name,
            body_data,
            ctx.header("idempotency-key"),
            if (authenticated_identity) |identity| identity.username else null,
        );
        return respondOwnedContextualResponse(ctx, &resp, self.api_server.alloc);
    }

    pub fn updateSchema(self: *AntflyApiHandler, ctx: *httpx.Context, table_name: []const u8) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const alloc = ctx.allocator;
        const decoded_table_name = (try decodePathParamOrBadRequest(ctx, table_name)) orelse return ctx.text("invalid path parameter");
        defer alloc.free(decoded_table_name);
        const body_data = (try ctx.body()) orelse {
            _ = ctx.status(400);
            return ctx.text("invalid schema update request");
        };
        var invalid_schema_message = table_contract.schemaUpdateRequestErrorMessage(error.InvalidSchemaUpdateRequest, body_data);
        const schema_json = table_contract.parseSchemaUpdateRequest(alloc, body_data) catch |err| {
            invalid_schema_message = table_contract.schemaUpdateRequestErrorMessage(err, body_data);
            _ = ctx.status(400);
            return ctx.text(invalid_schema_message);
        };
        defer alloc.free(schema_json);

        const table_before = try self.api_server.loadOwnedTableRecord(alloc, decoded_table_name);
        if (table_before == null) {
            _ = self.api_server.source.updateSchema(alloc, decoded_table_name, schema_json) catch |err| switch (err) {
                error.InvalidSchemaUpdateRequest => {
                    _ = ctx.status(400);
                    return ctx.text(invalid_schema_message);
                },
                error.TableNotFound => {
                    _ = ctx.status(404);
                    return ctx.text("not found");
                },
                error.UnsupportedOperation => blk: {
                    const table_writes_source = self.api_server.table_writes orelse {
                        _ = ctx.status(404);
                        return ctx.text("not found");
                    };
                    _ = table_writes_source.updateSchema(alloc, decoded_table_name, schema_json) catch |write_err| switch (write_err) {
                        error.InvalidSchemaUpdateRequest, error.InvalidCreateTableRequest => {
                            _ = ctx.status(400);
                            return ctx.text(invalid_schema_message);
                        },
                        else => return write_err,
                    } orelse {
                        _ = ctx.status(404);
                        return ctx.text("not found");
                    };
                    break :blk null;
                },
                else => return err,
            };
            var arena_impl = std.heap.ArenaAllocator.init(alloc);
            defer arena_impl.deinit();
            const value = try http_server_mod.buildLocalSchemaUpdateStatus(arena_impl.allocator(), decoded_table_name, schema_json);
            return ctx.json(value);
        }
        defer metadata_table_manager.freeTable(alloc, table_before.?);

        var local_schema_applied = false;
        const committed_version = self.api_server.source.updateSchema(alloc, decoded_table_name, schema_json) catch |err| switch (err) {
            error.InvalidSchemaUpdateRequest => {
                _ = ctx.status(400);
                return ctx.text(invalid_schema_message);
            },
            error.TableNotFound => {
                _ = ctx.status(404);
                return ctx.text("not found");
            },
            error.UnsupportedOperation => blk: {
                const table_writes_source = self.api_server.table_writes orelse {
                    _ = ctx.status(405);
                    return ctx.text("method not allowed");
                };
                _ = table_writes_source.updateSchema(alloc, decoded_table_name, schema_json) catch |write_err| switch (write_err) {
                    error.InvalidSchemaUpdateRequest, error.InvalidCreateTableRequest => {
                        _ = ctx.status(400);
                        return ctx.text(invalid_schema_message);
                    },
                    else => return write_err,
                };
                local_schema_applied = true;
                break :blk null;
            },
            else => return err,
        };
        var expectation = try self.api_server.schemaProjectionExpectationAlloc(alloc, &table_before.?, schema_json, committed_version);
        defer expectation.deinit(alloc);
        self.api_server.waitForSchemaUpdateProjection(decoded_table_name, expectation, committed_version) catch |err| switch (err) {
            error.TableVisibilityTimeout => {
                _ = ctx.status(500);
                return ctx.text("schema update did not converge");
            },
            error.TableGenerationChanged => {
                _ = ctx.status(409);
                return ctx.text("schema update was superseded; retry request");
            },
            else => return err,
        };
        self.api_server.reconcileProjectedSchemaUpdate(alloc, decoded_table_name, schema_json, local_schema_applied) catch |write_err| switch (write_err) {
            error.InvalidSchemaUpdateRequest, error.InvalidCreateTableRequest => {
                _ = ctx.status(400);
                return ctx.text(invalid_schema_message);
            },
            else => return write_err,
        };

        const body = try self.api_server.encodeSchemaUpdateResponse(decoded_table_name, schema_json);
        defer self.api_server.alloc.free(body);
        return jsonResponse(ctx, 200, body);
    }

    pub fn scanKeys(self: *AntflyApiHandler, ctx: *httpx.Context, table_name: []const u8) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const alloc = ctx.allocator;
        const decoded_table_name = (try decodePathParamOrBadRequest(ctx, table_name)) orelse return ctx.text("invalid path parameter");
        defer alloc.free(decoded_table_name);
        // The OpenAPI request body is optional; an absent body is the default
        // unbounded-range scan, just like an explicitly empty legacy request.
        const body_data = (try ctx.body()) orelse "";
        var scan_req = http_route_helpers.parseScanKeysRequest(alloc, body_data) catch |err| {
            if (http_route_helpers.scanRequestError(err)) |response| {
                _ = ctx.status(response.status);
                return ctx.text(response.message);
            }
            return err;
        };
        defer scan_req.deinit(alloc);

        if (try self.acquirePublicOperation(ctx, "scanKeys")) |response| return response;
        defer self.releasePublicOperation("scanKeys");
        const source = self.api_server.table_reads orelse {
            _ = ctx.status(404);
            return ctx.text("not found");
        };

        var result = (source.scan(
            alloc,
            decoded_table_name,
            scan_req.from,
            scan_req.to,
            scan_req.opts,
            .read_index,
        ) catch |err| switch (err) {
            error.TableNotFound => {
                _ = ctx.status(404);
                return ctx.text("not found");
            },
            error.HAReadRequiresPrimary, error.ReadRequiresPrimary => {
                _ = ctx.status(503);
                return ctx.text("read requires primary");
            },
            error.HAReadWaitForApply, error.HAReadWaitForMetadata, error.ReadUnavailable => {
                _ = ctx.status(503);
                return ctx.text("standby read unavailable");
            },
            error.PersistentDescriptorAdmissionExhausted,
            error.StorageReadTemporarilyUnavailable,
            => {
                var response = try public_table_http.storageReadTemporarilyUnavailableOwnedResponse(alloc);
                return respondOwnedApiResponse(ctx, &response);
            },
            else => return err,
        }) orelse {
            _ = ctx.status(404);
            return ctx.text("not found");
        };
        defer result.deinit(alloc);

        const row_filter_json = try http_server_mod.resolveEffectiveRowFilterJson(alloc, authenticated_identity, decoded_table_name);
        defer if (row_filter_json) |value| alloc.free(value);
        if (row_filter_json) |value| {
            const filtered = try self.api_server.filterScanResultByRowFilter(alloc, source, decoded_table_name, result.ndjson, value);
            defer alloc.free(filtered);
            try ctx.setHeader("content-type", "application/x-ndjson");
            _ = ctx.response.body(filtered);
            return ctx.response.build();
        }
        try ctx.setHeader("content-type", "application/x-ndjson");
        _ = ctx.response.body(result.ndjson);
        return ctx.response.build();
    }

    pub fn lookupKey(self: *AntflyApiHandler, ctx: *httpx.Context, table_name: []const u8, key: []const u8, params: metadata_openapi.server.LookupKeyParams) !httpx.Response {
        _ = params;
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const alloc = ctx.allocator;
        const decoded_table_name = (try decodePathParamOrBadRequest(ctx, table_name)) orelse return ctx.text("invalid path parameter");
        defer alloc.free(decoded_table_name);
        const decoded_key = (try decodePathParamOrBadRequest(ctx, key)) orelse return ctx.text("invalid path parameter");
        defer alloc.free(decoded_key);
        const source = self.api_server.table_reads orelse {
            _ = ctx.status(404);
            return ctx.text("not found");
        };

        var lookup_opts = try http_route_helpers.parseLookupOptions(alloc, ctx.request.uri.query orelse "");
        defer lookup_opts.deinit(alloc);
        const consistency = http_server_mod.parseLookupReadConsistency(ctx.request.uri.query orelse "") catch {
            _ = ctx.status(400);
            return ctx.text("invalid read consistency");
        };

        var result = (source.lookup(alloc, decoded_table_name, decoded_key, lookup_opts.opts, consistency) catch |err| switch (err) {
            error.TableNotFound => {
                _ = ctx.status(404);
                return ctx.text("not found");
            },
            error.HAReadRequiresPrimary, error.ReadRequiresPrimary => {
                _ = ctx.status(503);
                return ctx.text("read requires primary");
            },
            error.HAReadWaitForApply, error.HAReadWaitForMetadata, error.ReadUnavailable => {
                _ = ctx.status(503);
                return ctx.text("standby read unavailable");
            },
            error.PersistentDescriptorAdmissionExhausted,
            error.StorageReadTemporarilyUnavailable,
            => {
                var response = try public_table_http.storageReadTemporarilyUnavailableOwnedResponse(alloc);
                return respondOwnedApiResponse(ctx, &response);
            },
            else => return err,
        }) orelse {
            _ = ctx.status(404);
            return ctx.text("not found");
        };
        defer result.deinit(alloc);

        const row_filter_json = try http_server_mod.resolveEffectiveRowFilterJson(alloc, authenticated_identity, decoded_table_name);
        defer if (row_filter_json) |value| alloc.free(value);
        if (row_filter_json) |value| {
            if (!(try self.api_server.docJsonMatchesRowFilter(decoded_key, result.json, value))) {
                _ = ctx.status(404);
                return ctx.text("not found");
            }
        }
        try ctx.setHeader("content-type", "application/json");
        const version = try std.fmt.allocPrint(alloc, "{d}", .{result.version});
        defer alloc.free(version);
        try ctx.setHeader("X-Antfly-Version", version);
        _ = ctx.response.body(result.json);
        return ctx.response.build();
    }

    pub fn getDocumentArtifactManifest(self: *AntflyApiHandler, ctx: *httpx.Context, table_name: []const u8, key: []const u8, artifact_name: []const u8, params: metadata_openapi.server.GetDocumentArtifactManifestParams) !httpx.Response {
        _ = params;
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const alloc = ctx.allocator;
        const decoded_table_name = (try decodePathParamOrBadRequest(ctx, table_name)) orelse return ctx.text("invalid path parameter");
        defer alloc.free(decoded_table_name);
        const decoded_key = (try decodePathParamOrBadRequest(ctx, key)) orelse return ctx.text("invalid path parameter");
        defer alloc.free(decoded_key);
        const decoded_artifact_name = (try decodePathParamOrBadRequest(ctx, artifact_name)) orelse return ctx.text("invalid path parameter");
        defer alloc.free(decoded_artifact_name);
        const opts = self.api_server.documentArtifactManifestOptionsForRequest(decoded_table_name, ctx.request.uri.query orelse "", authenticated_identity) catch |err| switch (err) {
            error.InvalidDetail => {
                _ = ctx.status(400);
                return ctx.text("invalid artifact detail");
            },
            error.Forbidden => {
                _ = ctx.status(403);
                return ctx.text("forbidden");
            },
        };
        if (!(try self.api_server.sourceDocumentVisibleToIdentity(decoded_table_name, decoded_key, authenticated_identity))) {
            _ = ctx.status(404);
            return ctx.text("not found");
        }
        var resp = try public_table_http.handleDocumentArtifactManifest(alloc, decoded_table_name, decoded_key, decoded_artifact_name, opts, self.api_server.tableApi(operationContext(ctx, authenticated_identity)));
        return respondOwnedApiResponse(ctx, &resp);
    }

    pub fn listDocumentArtifactManifests(self: *AntflyApiHandler, ctx: *httpx.Context, table_name: []const u8, key: []const u8, params: metadata_openapi.server.ListDocumentArtifactManifestsParams) !httpx.Response {
        _ = params;
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const alloc = ctx.allocator;
        const decoded_table_name = (try decodePathParamOrBadRequest(ctx, table_name)) orelse return ctx.text("invalid path parameter");
        defer alloc.free(decoded_table_name);
        const decoded_key = (try decodePathParamOrBadRequest(ctx, key)) orelse return ctx.text("invalid path parameter");
        defer alloc.free(decoded_key);
        const opts = self.api_server.documentArtifactManifestOptionsForRequest(decoded_table_name, ctx.request.uri.query orelse "", authenticated_identity) catch |err| switch (err) {
            error.InvalidDetail => {
                _ = ctx.status(400);
                return ctx.text("invalid artifact detail");
            },
            error.Forbidden => {
                _ = ctx.status(403);
                return ctx.text("forbidden");
            },
        };
        if (!(try self.api_server.sourceDocumentVisibleToIdentity(decoded_table_name, decoded_key, authenticated_identity))) {
            _ = ctx.status(404);
            return ctx.text("not found");
        }
        var resp = try public_table_http.handleDocumentArtifactManifests(alloc, decoded_table_name, decoded_key, opts, self.api_server.tableApi(operationContext(ctx, authenticated_identity)));
        return respondOwnedApiResponse(ctx, &resp);
    }

    pub fn listArtifactEnrichments(self: *AntflyApiHandler, ctx: *httpx.Context, table_name: []const u8) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const decoded_table_name = (try decodePathParamOrBadRequest(ctx, table_name)) orelse return ctx.text("invalid path parameter");
        defer ctx.allocator.free(decoded_table_name);
        var resp = try public_table_http.handleListArtifactEnrichments(ctx.allocator, decoded_table_name, self.api_server.tableApi(operationContext(ctx, authenticated_identity)));
        return respondOwnedApiResponse(ctx, &resp);
    }

    pub fn listTableRepairIssues(self: *AntflyApiHandler, ctx: *httpx.Context, table_name: []const u8) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        if (ctx.request.uri.query) |query| {
            if (query.len != 0) return textResponse(ctx, 400, "repair requests use json body");
        }
        const decoded_table_name = (try decodePathParamOrBadRequest(ctx, table_name)) orelse return ctx.text("invalid path parameter");
        defer ctx.allocator.free(decoded_table_name);
        const body_data = (try ctx.body()) orelse "";
        var response = try self.api_server.handlePublicListArtifactRepairIssues(decoded_table_name, body_data);
        return respondOwnedApiResponseWithAllocator(ctx, &response, self.api_server.alloc);
    }

    pub fn listArtifactRepairIssues(self: *AntflyApiHandler, ctx: *httpx.Context, table_name: []const u8) !httpx.Response {
        return try self.listTableRepairIssues(ctx, table_name);
    }

    pub fn runTableRepair(self: *AntflyApiHandler, ctx: *httpx.Context, table_name: []const u8) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        if (ctx.request.uri.query) |query| {
            if (query.len != 0) return textResponse(ctx, 400, "repair requests use json body");
        }
        const decoded_table_name = (try decodePathParamOrBadRequest(ctx, table_name)) orelse return ctx.text("invalid path parameter");
        defer ctx.allocator.free(decoded_table_name);
        const body_data = (try ctx.body()) orelse "";
        var response = try self.api_server.handlePublicRunTableRepair(decoded_table_name, body_data);
        return respondOwnedApiResponseWithAllocator(ctx, &response, self.api_server.alloc);
    }

    pub fn startTableRepairJob(self: *AntflyApiHandler, ctx: *httpx.Context, table_name: []const u8) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        if (ctx.request.uri.query) |query| {
            if (query.len != 0) return textResponse(ctx, 400, "repair job requests use json body");
        }
        const decoded_table_name = (try decodePathParamOrBadRequest(ctx, table_name)) orelse return ctx.text("invalid path parameter");
        defer ctx.allocator.free(decoded_table_name);
        const body_data = (try ctx.body()) orelse "";
        var response = try self.api_server.handlePublicStartTableRepairJob(decoded_table_name, body_data);
        return respondOwnedApiResponseWithAllocator(ctx, &response, self.api_server.alloc);
    }

    pub fn getTableRepairJob(self: *AntflyApiHandler, ctx: *httpx.Context, table_name: []const u8, job_id: []const u8) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const decoded_table_name = (try decodePathParamOrBadRequest(ctx, table_name)) orelse return ctx.text("invalid path parameter");
        defer ctx.allocator.free(decoded_table_name);
        var response = try self.api_server.handlePublicTableRepairJob(decoded_table_name, job_id);
        return respondOwnedApiResponseWithAllocator(ctx, &response, self.api_server.alloc);
    }

    pub fn advanceTableRepairJob(self: *AntflyApiHandler, ctx: *httpx.Context, table_name: []const u8, job_id: []const u8) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const decoded_table_name = (try decodePathParamOrBadRequest(ctx, table_name)) orelse return ctx.text("invalid path parameter");
        defer ctx.allocator.free(decoded_table_name);
        var response = try self.api_server.handlePublicAdvanceTableRepairJob(decoded_table_name, job_id);
        return respondOwnedApiResponseWithAllocator(ctx, &response, self.api_server.alloc);
    }

    pub fn cancelTableRepairJob(self: *AntflyApiHandler, ctx: *httpx.Context, table_name: []const u8, job_id: []const u8) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const decoded_table_name = (try decodePathParamOrBadRequest(ctx, table_name)) orelse return ctx.text("invalid path parameter");
        defer ctx.allocator.free(decoded_table_name);
        var response = try self.api_server.handlePublicCancelTableRepairJob(decoded_table_name, job_id);
        return respondOwnedApiResponseWithAllocator(ctx, &response, self.api_server.alloc);
    }

    pub fn reprocessDocumentArtifact(self: *AntflyApiHandler, ctx: *httpx.Context, table_name: []const u8, key: []const u8, artifact_name: []const u8) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const alloc = ctx.allocator;
        const decoded_table_name = (try decodePathParamOrBadRequest(ctx, table_name)) orelse return ctx.text("invalid path parameter");
        defer alloc.free(decoded_table_name);
        const decoded_key = (try decodePathParamOrBadRequest(ctx, key)) orelse return ctx.text("invalid path parameter");
        defer alloc.free(decoded_key);
        const decoded_artifact_name = (try decodePathParamOrBadRequest(ctx, artifact_name)) orelse return ctx.text("invalid path parameter");
        defer alloc.free(decoded_artifact_name);
        if (!(try self.api_server.sourceDocumentVisibleToIdentity(decoded_table_name, decoded_key, authenticated_identity))) {
            _ = ctx.status(404);
            return ctx.text("not found");
        }
        var resp = try public_table_http.handleReprocessDocumentArtifact(alloc, decoded_table_name, decoded_key, decoded_artifact_name, self.api_server.tableApi(operationContext(ctx, authenticated_identity)));
        return respondOwnedApiResponse(ctx, &resp);
    }

    pub fn reprocessDocumentArtifactRange(self: *AntflyApiHandler, ctx: *httpx.Context, table_name: []const u8, artifact_name: []const u8) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const alloc = ctx.allocator;
        const decoded_table_name = (try decodePathParamOrBadRequest(ctx, table_name)) orelse return ctx.text("invalid path parameter");
        defer alloc.free(decoded_table_name);
        const decoded_artifact_name = (try decodePathParamOrBadRequest(ctx, artifact_name)) orelse return ctx.text("invalid path parameter");
        defer alloc.free(decoded_artifact_name);
        const body_data = (try ctx.body()) orelse "";
        var resp = try public_table_http.handleReprocessDocumentArtifactRange(alloc, decoded_table_name, decoded_artifact_name, body_data, self.api_server.tableApi(operationContext(ctx, authenticated_identity)));
        return respondOwnedApiResponse(ctx, &resp);
    }

    pub fn startDocumentArtifactReprocessJob(self: *AntflyApiHandler, ctx: *httpx.Context, table_name: []const u8, artifact_name: []const u8) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const decoded_table_name = (try decodePathParamOrBadRequest(ctx, table_name)) orelse return ctx.text("invalid path parameter");
        defer ctx.allocator.free(decoded_table_name);
        const body_data = (try ctx.body()) orelse "";
        var response = try self.api_server.handlePublicStartDocumentArtifactReprocessJob(decoded_table_name, artifact_name, body_data);
        return respondOwnedApiResponseWithAllocator(ctx, &response, self.api_server.alloc);
    }

    pub fn getDocumentArtifactReprocessJob(self: *AntflyApiHandler, ctx: *httpx.Context, table_name: []const u8, artifact_name: []const u8, job_id: []const u8) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const decoded_table_name = (try decodePathParamOrBadRequest(ctx, table_name)) orelse return ctx.text("invalid path parameter");
        defer ctx.allocator.free(decoded_table_name);
        var response = try self.api_server.handlePublicDocumentArtifactReprocessJob(decoded_table_name, artifact_name, job_id);
        return respondOwnedApiResponseWithAllocator(ctx, &response, self.api_server.alloc);
    }

    pub fn advanceDocumentArtifactReprocessJob(self: *AntflyApiHandler, ctx: *httpx.Context, table_name: []const u8, artifact_name: []const u8, job_id: []const u8) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const decoded_table_name = (try decodePathParamOrBadRequest(ctx, table_name)) orelse return ctx.text("invalid path parameter");
        defer ctx.allocator.free(decoded_table_name);
        var response = try self.api_server.handlePublicAdvanceDocumentArtifactReprocessJob(decoded_table_name, artifact_name, job_id);
        return respondOwnedApiResponseWithAllocator(ctx, &response, self.api_server.alloc);
    }

    pub fn cancelDocumentArtifactReprocessJob(self: *AntflyApiHandler, ctx: *httpx.Context, table_name: []const u8, artifact_name: []const u8, job_id: []const u8) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const decoded_table_name = (try decodePathParamOrBadRequest(ctx, table_name)) orelse return ctx.text("invalid path parameter");
        defer ctx.allocator.free(decoded_table_name);
        var response = try self.api_server.handlePublicCancelDocumentArtifactReprocessJob(decoded_table_name, artifact_name, job_id);
        return respondOwnedApiResponseWithAllocator(ctx, &response, self.api_server.alloc);
    }

    pub fn listIndexes(self: *AntflyApiHandler, ctx: *httpx.Context, table_name: []const u8) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const decoded_table_name = (try decodePathParamOrBadRequest(ctx, table_name)) orelse return ctx.text("invalid path parameter");
        defer ctx.allocator.free(decoded_table_name);
        var resp = try public_table_http.handleTableListIndexes(ctx.allocator, decoded_table_name, self.api_server.tableApi(operationContext(ctx, authenticated_identity)));
        return respondOwnedApiResponse(ctx, &resp);
    }

    pub fn getIndex(self: *AntflyApiHandler, ctx: *httpx.Context, table_name: []const u8, index_name: []const u8) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const decoded_table_name = (try decodePathParamOrBadRequest(ctx, table_name)) orelse return ctx.text("invalid path parameter");
        defer ctx.allocator.free(decoded_table_name);
        const decoded_index_name = (try decodePathParamOrBadRequest(ctx, index_name)) orelse return ctx.text("invalid path parameter");
        defer ctx.allocator.free(decoded_index_name);
        if (http_server_mod.runtimeSchemaDebugRequested(ctx.request.uri.query orelse "")) {
            if (!self.api_server.runtimeSchemaDebugAllowed(authenticated_identity)) return jsonErrorResponse(ctx, 403, "forbidden");
            const debug_body = (try self.api_server.encodeIndexRuntimeSchemaDebugAlloc(ctx.allocator, decoded_table_name, decoded_index_name)) orelse
                return jsonErrorResponse(ctx, 404, "not found");
            defer ctx.allocator.free(debug_body);
            return jsonResponse(ctx, 200, debug_body);
        }
        var resp = try public_table_http.handleTableGetIndex(ctx.allocator, decoded_table_name, decoded_index_name, self.api_server.tableApi(operationContext(ctx, authenticated_identity)));
        return respondOwnedApiResponse(ctx, &resp);
    }

    pub fn createIndex(self: *AntflyApiHandler, ctx: *httpx.Context, table_name: []const u8, index_name: []const u8) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const decoded_table_name = (try decodePathParamOrBadRequest(ctx, table_name)) orelse return ctx.text("invalid path parameter");
        defer ctx.allocator.free(decoded_table_name);
        const decoded_index_name = (try decodePathParamOrBadRequest(ctx, index_name)) orelse return ctx.text("invalid path parameter");
        defer ctx.allocator.free(decoded_index_name);
        const body_data = (try ctx.body()) orelse "";
        var resp = try public_table_http.handleTableCreateIndex(ctx.allocator, decoded_table_name, decoded_index_name, body_data, self.api_server.tableApi(operationContext(ctx, authenticated_identity)));
        return respondOwnedApiResponse(ctx, &resp);
    }

    pub fn dropIndex(self: *AntflyApiHandler, ctx: *httpx.Context, table_name: []const u8, index_name: []const u8) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const decoded_table_name = (try decodePathParamOrBadRequest(ctx, table_name)) orelse return ctx.text("invalid path parameter");
        defer ctx.allocator.free(decoded_table_name);
        const decoded_index_name = (try decodePathParamOrBadRequest(ctx, index_name)) orelse return ctx.text("invalid path parameter");
        defer ctx.allocator.free(decoded_index_name);
        var resp = try public_table_http.handleTableDeleteIndex(ctx.allocator, decoded_table_name, decoded_index_name, self.api_server.tableApi(operationContext(ctx, authenticated_identity)));
        return respondOwnedApiResponse(ctx, &resp);
    }

    pub fn putArtifactEnrichment(self: *AntflyApiHandler, ctx: *httpx.Context, table_name: []const u8, artifact_name: []const u8) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const alloc = ctx.allocator;
        const decoded_table_name = (try decodePathParamOrBadRequest(ctx, table_name)) orelse return ctx.text("invalid path parameter");
        defer alloc.free(decoded_table_name);
        const decoded_artifact_name = (try decodePathParamOrBadRequest(ctx, artifact_name)) orelse return ctx.text("invalid path parameter");
        defer alloc.free(decoded_artifact_name);
        const body_data = (try ctx.body()) orelse "";
        var resp = try public_table_http.handlePutArtifactEnrichment(alloc, decoded_table_name, decoded_artifact_name, body_data, self.api_server.tableApi(operationContext(ctx, authenticated_identity)));
        return respondOwnedApiResponse(ctx, &resp);
    }

    pub fn deleteArtifactEnrichment(self: *AntflyApiHandler, ctx: *httpx.Context, table_name: []const u8, artifact_name: []const u8) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const alloc = ctx.allocator;
        const decoded_table_name = (try decodePathParamOrBadRequest(ctx, table_name)) orelse return ctx.text("invalid path parameter");
        defer alloc.free(decoded_table_name);
        const decoded_artifact_name = (try decodePathParamOrBadRequest(ctx, artifact_name)) orelse return ctx.text("invalid path parameter");
        defer alloc.free(decoded_artifact_name);
        var resp = try public_table_http.handleDeleteArtifactEnrichment(alloc, decoded_table_name, decoded_artifact_name, self.api_server.tableApi(operationContext(ctx, authenticated_identity)));
        return respondOwnedApiResponse(ctx, &resp);
    }

    // ---------------------------------------------------------------
    // usermgr_openapi handler interface (16 methods)
    // ---------------------------------------------------------------

    pub fn getCurrentUser(self: *AntflyApiHandler, ctx: *httpx.Context) !httpx.Response {
        const alloc = ctx.allocator;
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const identity = authenticated_identity orelse return try unauthorizedResponse(ctx);
        var arena_impl = std.heap.ArenaAllocator.init(alloc);
        defer arena_impl.deinit();
        const current_user = try http_server_mod.makeCurrentUserResponse(arena_impl.allocator(), identity.username, identity.permissions, identity.metadata_json);
        return ctx.json(current_user);
    }

    pub fn listUsers(self: *AntflyApiHandler, ctx: *httpx.Context) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const alloc = ctx.allocator;
        const manager = self.api_server.cfg.user_manager orelse {
            _ = ctx.status(503);
            return ctx.text("user management not configured");
        };
        const users = try manager.listUsers();
        defer http_server_mod.freeOwnedStrings(alloc, users);
        const listed_users = try http_server_mod.makeListedUsers(alloc, users);
        defer alloc.free(listed_users);
        return ctx.json(listed_users);
    }

    pub fn getUserByName(self: *AntflyApiHandler, ctx: *httpx.Context, user_name: []const u8) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const alloc = ctx.allocator;
        const manager = self.api_server.cfg.user_manager orelse {
            _ = ctx.status(503);
            return ctx.text("user management not configured");
        };
        var user = manager.getUser(user_name) catch |err| switch (err) {
            error.UserNotFound => {
                _ = ctx.status(404);
                return ctx.text("not found");
            },
            else => return err,
        };
        defer user.deinit(alloc);
        var arena_impl = std.heap.ArenaAllocator.init(alloc);
        defer arena_impl.deinit();
        const generated = try http_server_mod.userToOpenApi(arena_impl.allocator(), user);
        return ctx.json(generated);
    }

    pub fn createUser(self: *AntflyApiHandler, ctx: *httpx.Context, user_name: []const u8) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const alloc = ctx.allocator;
        const manager = self.api_server.cfg.user_manager orelse {
            _ = ctx.status(503);
            return ctx.text("user management not configured");
        };
        const body_data = (try ctx.body()) orelse {
            _ = ctx.status(400);
            return ctx.text("invalid create user request");
        };
        var create_req = http_server_mod.parseCreateUserRequest(alloc, body_data, user_name) catch {
            _ = ctx.status(400);
            return ctx.text("invalid create user request");
        };
        defer create_req.deinit(alloc);
        var created = manager.createUserWithMetadata(create_req.username, create_req.password, create_req.initial_policies, create_req.metadata_json) catch |err| switch (err) {
            error.UserExists => {
                _ = ctx.status(409);
                return ctx.text("user already exists");
            },
            error.InvalidMetadata => {
                _ = ctx.status(400);
                return ctx.text("invalid create user request");
            },
            else => return err,
        };
        defer created.deinit(alloc);
        _ = ctx.status(201);
        var arena_impl = std.heap.ArenaAllocator.init(alloc);
        defer arena_impl.deinit();
        const generated = try http_server_mod.userToOpenApi(arena_impl.allocator(), created);
        return ctx.json(generated);
    }

    pub fn deleteUser(self: *AntflyApiHandler, ctx: *httpx.Context, user_name: []const u8) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const manager = self.api_server.cfg.user_manager orelse {
            _ = ctx.status(503);
            return ctx.text("user management not configured");
        };
        manager.deleteUser(user_name) catch |err| switch (err) {
            error.UserNotFound => {
                _ = ctx.status(404);
                return ctx.text("not found");
            },
            else => return err,
        };
        _ = ctx.status(204);
        return ctx.text("");
    }

    pub fn updateUserPassword(self: *AntflyApiHandler, ctx: *httpx.Context, user_name: []const u8) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const alloc = ctx.allocator;
        const manager = self.api_server.cfg.user_manager orelse {
            _ = ctx.status(503);
            return ctx.text("user management not configured");
        };
        const body_data = (try ctx.body()) orelse {
            _ = ctx.status(400);
            return ctx.text("invalid password update request");
        };
        const new_password = http_server_mod.parsePasswordUpdateRequest(alloc, body_data) catch {
            _ = ctx.status(400);
            return ctx.text("invalid password update request");
        };
        defer alloc.free(new_password);
        manager.updatePassword(user_name, new_password) catch |err| switch (err) {
            error.UserNotFound => {
                _ = ctx.status(404);
                return ctx.text("not found");
            },
            else => return err,
        };
        return ctx.json(.{ .message = "Password updated successfully" });
    }

    pub fn getUserPermissions(self: *AntflyApiHandler, ctx: *httpx.Context, user_name: []const u8) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const alloc = ctx.allocator;
        const manager = self.api_server.cfg.user_manager orelse {
            _ = ctx.status(503);
            return ctx.text("user management not configured");
        };
        const permissions = manager.getPermissionsForUser(user_name) catch |err| switch (err) {
            error.UserNotFound => {
                _ = ctx.status(404);
                return ctx.text("not found");
            },
            else => return err,
        };
        defer http_server_mod.freePermissions(alloc, permissions);
        const generated_permissions = try http_server_mod.clonePermissionsToOpenApi(alloc, permissions);
        defer alloc.free(generated_permissions);
        return ctx.json(generated_permissions);
    }

    pub fn addPermissionToUser(self: *AntflyApiHandler, ctx: *httpx.Context, user_name: []const u8) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const alloc = ctx.allocator;
        const manager = self.api_server.cfg.user_manager orelse {
            _ = ctx.status(503);
            return ctx.text("user management not configured");
        };
        const body_data = (try ctx.body()) orelse {
            _ = ctx.status(400);
            return ctx.text("invalid permission request");
        };
        var permission = http_server_mod.parsePermissionBody(alloc, body_data) catch {
            _ = ctx.status(400);
            return ctx.text("invalid permission request");
        };
        defer permission.deinit(alloc);
        manager.addPermissionToUser(user_name, permission) catch |err| switch (err) {
            error.UserNotFound => {
                _ = ctx.status(404);
                return ctx.text("not found");
            },
            error.InvalidPermissionType, error.InvalidResourceType => {
                _ = ctx.status(400);
                return ctx.text("invalid permission request");
            },
            else => return err,
        };
        _ = ctx.status(201);
        return ctx.json(.{ .message = "Permission added successfully" });
    }

    pub fn removePermissionFromUser(self: *AntflyApiHandler, ctx: *httpx.Context, user_name: []const u8, params: usermgr_openapi.server.RemovePermissionFromUserParams) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const manager = self.api_server.cfg.user_manager orelse {
            _ = ctx.status(503);
            return ctx.text("user management not configured");
        };
        manager.removePermissionFromUser(
            user_name,
            params.resource,
            usermgr.ResourceType.fromSlice(params.resource_type) catch {
                _ = ctx.status(400);
                return ctx.text("invalid resourceType");
            },
        ) catch |err| switch (err) {
            error.UserNotFound, error.RoleNotFound => {
                _ = ctx.status(404);
                return ctx.text("not found");
            },
            else => return err,
        };
        _ = ctx.status(204);
        return ctx.text("");
    }

    pub fn listUserRoles(self: *AntflyApiHandler, ctx: *httpx.Context, user_name: []const u8) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const alloc = ctx.allocator;
        const manager = self.api_server.cfg.user_manager orelse {
            _ = ctx.status(503);
            return ctx.text("user management not configured");
        };
        const roles = manager.getRolesForUser(user_name) catch |err| switch (err) {
            error.UserNotFound => {
                _ = ctx.status(404);
                return ctx.text("not found");
            },
            else => return err,
        };
        defer http_server_mod.freeOwnedStrings(alloc, roles);
        return ctx.json(roles);
    }

    pub fn addRoleToUser(self: *AntflyApiHandler, ctx: *httpx.Context, user_name: []const u8) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const alloc = ctx.allocator;
        const manager = self.api_server.cfg.user_manager orelse {
            _ = ctx.status(503);
            return ctx.text("user management not configured");
        };
        const body_data = (try ctx.body()) orelse {
            _ = ctx.status(400);
            return ctx.text("invalid role request");
        };
        const role = http_server_mod.parseRoleAssignmentBody(alloc, body_data) catch {
            _ = ctx.status(400);
            return ctx.text("invalid role request");
        };
        defer alloc.free(role);
        manager.addRoleToUser(user_name, role) catch |err| switch (err) {
            error.UserNotFound => {
                _ = ctx.status(404);
                return ctx.text("not found");
            },
            error.InvalidRole => {
                _ = ctx.status(400);
                return ctx.text("invalid role request");
            },
            else => return err,
        };
        _ = ctx.status(201);
        return ctx.json(.{ .message = "Role added successfully" });
    }

    pub fn removeRoleFromUser(self: *AntflyApiHandler, ctx: *httpx.Context, user_name: []const u8, params: usermgr_openapi.server.RemoveRoleFromUserParams) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const manager = self.api_server.cfg.user_manager orelse {
            _ = ctx.status(503);
            return ctx.text("user management not configured");
        };
        manager.removeRoleFromUser(user_name, params.role) catch |err| switch (err) {
            error.UserNotFound, error.RoleNotFound => {
                _ = ctx.status(404);
                return ctx.text("not found");
            },
            else => return err,
        };
        _ = ctx.status(204);
        return ctx.text("");
    }

    pub fn listAuthSubjects(self: *AntflyApiHandler, ctx: *httpx.Context) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const alloc = ctx.allocator;
        const manager = self.api_server.cfg.user_manager orelse {
            _ = ctx.status(503);
            return ctx.text("user management not configured");
        };
        const subjects = try manager.listAuthSubjects();
        defer http_server_mod.freeAuthSubjects(alloc, subjects);
        var arena_impl = std.heap.ArenaAllocator.init(alloc);
        defer arena_impl.deinit();
        return ctx.json(try http_server_mod.authSubjectsToResponse(arena_impl.allocator(), subjects));
    }

    pub fn listRowFilters(self: *AntflyApiHandler, ctx: *httpx.Context, user_name: []const u8) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const alloc = ctx.allocator;
        const manager = self.api_server.cfg.user_manager orelse {
            _ = ctx.status(503);
            return ctx.text("user management not configured");
        };
        const row_filters = manager.listRowFilters(user_name) catch |err| switch (err) {
            error.UserNotFound => {
                _ = ctx.status(404);
                return ctx.text("not found");
            },
            else => return err,
        };
        defer http_server_mod.freeRowFilters(alloc, row_filters);
        var arena_impl = std.heap.ArenaAllocator.init(alloc);
        defer arena_impl.deinit();
        const arena = arena_impl.allocator();
        const generated = try arena.alloc(usermgr_openapi.RowFilterEntry, row_filters.len);
        for (row_filters, 0..) |entry, i| {
            generated[i] = try http_server_mod.rowFilterEntryToOpenApi(arena, entry);
        }
        return ctx.json(generated);
    }

    pub fn getRowFilter(self: *AntflyApiHandler, ctx: *httpx.Context, user_name: []const u8, table: []const u8) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const alloc = ctx.allocator;
        const manager = self.api_server.cfg.user_manager orelse {
            _ = ctx.status(503);
            return ctx.text("user management not configured");
        };
        const filter_json = manager.getRowFilter(user_name, table) catch |err| switch (err) {
            error.UserNotFound, error.RowFilterNotFound => {
                _ = ctx.status(404);
                return ctx.text("not found");
            },
            else => return err,
        };
        defer alloc.free(filter_json);
        var arena_impl = std.heap.ArenaAllocator.init(alloc);
        defer arena_impl.deinit();
        const generated = try http_server_mod.rowFilterEntryToOpenApi(arena_impl.allocator(), .{
            .table = @constCast(table),
            .filter = @constCast(filter_json),
        });
        return ctx.json(generated);
    }

    pub fn setRowFilter(self: *AntflyApiHandler, ctx: *httpx.Context, user_name: []const u8, table: []const u8) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const alloc = ctx.allocator;
        const manager = self.api_server.cfg.user_manager orelse {
            _ = ctx.status(503);
            return ctx.text("user management not configured");
        };
        const body_data = (try ctx.body()) orelse {
            return jsonErrorResponse(ctx, 400, "invalid row filter");
        };
        var parsed_filter = usermgr_openapi.server.parseSetRowFilterBody(alloc, body_data) catch {
            return jsonErrorResponse(ctx, 400, "invalid row filter");
        };
        defer parsed_filter.deinit();
        const normalized_filter = try std.json.Stringify.valueAlloc(alloc, parsed_filter.value, .{});
        defer alloc.free(normalized_filter);
        http_server_mod.validateAuthRowFilterJson(alloc, normalized_filter) catch {
            return jsonErrorResponse(ctx, 400, "invalid row filter");
        };
        manager.setRowFilter(user_name, table, normalized_filter) catch |err| switch (err) {
            error.UserNotFound => {
                _ = ctx.status(404);
                return ctx.text("not found");
            },
            else => {
                return jsonErrorResponse(ctx, 400, "invalid row filter");
            },
        };
        var arena_impl = std.heap.ArenaAllocator.init(alloc);
        defer arena_impl.deinit();
        const generated = try http_server_mod.rowFilterEntryToOpenApi(arena_impl.allocator(), .{
            .table = @constCast(table),
            .filter = @constCast(normalized_filter),
        });
        return ctx.json(generated);
    }

    pub fn removeRowFilter(self: *AntflyApiHandler, ctx: *httpx.Context, user_name: []const u8, table: []const u8) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const manager = self.api_server.cfg.user_manager orelse {
            _ = ctx.status(503);
            return ctx.text("user management not configured");
        };
        manager.removeRowFilter(user_name, table) catch |err| switch (err) {
            error.UserNotFound, error.RowFilterNotFound => {
                _ = ctx.status(404);
                return ctx.text("not found");
            },
            else => return err,
        };
        _ = ctx.status(204);
        return ctx.text("");
    }

    pub fn listSubjectRowFilters(self: *AntflyApiHandler, ctx: *httpx.Context, subject: []const u8) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const alloc = ctx.allocator;
        const manager = self.api_server.cfg.user_manager orelse {
            _ = ctx.status(503);
            return ctx.text("user management not configured");
        };
        const row_filters = try manager.listSubjectRowFilters(subject);
        defer http_server_mod.freeRowFilters(alloc, row_filters);
        var arena_impl = std.heap.ArenaAllocator.init(alloc);
        defer arena_impl.deinit();
        const arena = arena_impl.allocator();
        const generated = try arena.alloc(usermgr_openapi.RowFilterEntry, row_filters.len);
        for (row_filters, 0..) |entry, i| {
            generated[i] = try http_server_mod.rowFilterEntryToOpenApi(arena, entry);
        }
        return ctx.json(generated);
    }

    pub fn getSubjectRowFilter(self: *AntflyApiHandler, ctx: *httpx.Context, subject: []const u8, table: []const u8) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const alloc = ctx.allocator;
        const manager = self.api_server.cfg.user_manager orelse {
            _ = ctx.status(503);
            return ctx.text("user management not configured");
        };
        const filter_json = manager.getSubjectRowFilter(subject, table) catch |err| switch (err) {
            error.RowFilterNotFound => {
                _ = ctx.status(404);
                return ctx.text("not found");
            },
            else => return err,
        };
        defer alloc.free(filter_json);
        var arena_impl = std.heap.ArenaAllocator.init(alloc);
        defer arena_impl.deinit();
        const generated = try http_server_mod.rowFilterEntryToOpenApi(arena_impl.allocator(), .{
            .table = @constCast(table),
            .filter = @constCast(filter_json),
        });
        return ctx.json(generated);
    }

    pub fn setSubjectRowFilter(self: *AntflyApiHandler, ctx: *httpx.Context, subject: []const u8, table: []const u8) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const alloc = ctx.allocator;
        const manager = self.api_server.cfg.user_manager orelse {
            _ = ctx.status(503);
            return ctx.text("user management not configured");
        };
        const body_data = (try ctx.body()) orelse {
            return jsonErrorResponse(ctx, 400, "invalid row filter");
        };
        var parsed_filter = usermgr_openapi.server.parseSetSubjectRowFilterBody(alloc, body_data) catch {
            return jsonErrorResponse(ctx, 400, "invalid row filter");
        };
        defer parsed_filter.deinit();
        const normalized_filter = try std.json.Stringify.valueAlloc(alloc, parsed_filter.value, .{});
        defer alloc.free(normalized_filter);
        http_server_mod.validateAuthRowFilterJson(alloc, normalized_filter) catch {
            return jsonErrorResponse(ctx, 400, "invalid row filter");
        };
        manager.setSubjectRowFilter(subject, table, normalized_filter) catch {
            return jsonErrorResponse(ctx, 400, "invalid row filter");
        };
        var arena_impl = std.heap.ArenaAllocator.init(alloc);
        defer arena_impl.deinit();
        const generated = try http_server_mod.rowFilterEntryToOpenApi(arena_impl.allocator(), .{
            .table = @constCast(table),
            .filter = @constCast(normalized_filter),
        });
        return ctx.json(generated);
    }

    pub fn removeSubjectRowFilter(self: *AntflyApiHandler, ctx: *httpx.Context, subject: []const u8, table: []const u8) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const manager = self.api_server.cfg.user_manager orelse {
            _ = ctx.status(503);
            return ctx.text("user management not configured");
        };
        manager.removeSubjectRowFilter(subject, table) catch |err| switch (err) {
            error.RowFilterNotFound => {
                _ = ctx.status(404);
                return ctx.text("not found");
            },
            else => return err,
        };
        _ = ctx.status(204);
        return ctx.text("");
    }

    pub fn listApiKeys(self: *AntflyApiHandler, ctx: *httpx.Context, user_name: []const u8) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const alloc = ctx.allocator;
        const manager = self.api_server.cfg.user_manager orelse {
            _ = ctx.status(503);
            return ctx.text("user management not configured");
        };
        const keys = manager.listApiKeys(user_name) catch |err| switch (err) {
            error.UserNotFound => {
                _ = ctx.status(404);
                return ctx.text("not found");
            },
            else => return err,
        };
        defer http_server_mod.freeApiKeys(alloc, keys);
        var arena_impl = std.heap.ArenaAllocator.init(alloc);
        defer arena_impl.deinit();
        const arena = arena_impl.allocator();
        const generated = try arena.alloc(usermgr_openapi.ApiKey, keys.len);
        for (keys, 0..) |api_key, i| {
            generated[i] = try http_server_mod.apiKeyToOpenApi(arena, api_key);
        }
        return ctx.json(generated);
    }

    pub fn createApiKey(self: *AntflyApiHandler, ctx: *httpx.Context, user_name: []const u8) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const alloc = ctx.allocator;
        const manager = self.api_server.cfg.user_manager orelse {
            _ = ctx.status(503);
            return ctx.text("user management not configured");
        };
        const body_data = (try ctx.body()) orelse {
            _ = ctx.status(400);
            return ctx.text("invalid api key request");
        };
        var create_req = http_server_mod.parseCreateApiKeyRequest(alloc, body_data) catch {
            _ = ctx.status(400);
            return ctx.text("invalid api key request");
        };
        defer create_req.deinit(alloc);
        var created = manager.createApiKey(
            user_name,
            create_req.name,
            create_req.permissions,
            create_req.row_filter,
            create_req.expires_at_ns,
        ) catch |err| switch (err) {
            error.UserNotFound => {
                _ = ctx.status(404);
                return ctx.text("not found");
            },
            error.PrivilegeEscalation => {
                _ = ctx.status(403);
                return ctx.text("privilege escalation");
            },
            else => return err,
        };
        defer created.deinit(alloc);
        var arena_impl = std.heap.ArenaAllocator.init(alloc);
        defer arena_impl.deinit();
        const generated = try http_server_mod.createdApiKeyToOpenApi(arena_impl.allocator(), created);
        _ = ctx.status(201);
        return ctx.json(generated);
    }

    pub fn deleteApiKey(self: *AntflyApiHandler, ctx: *httpx.Context, user_name: []const u8, key_id: []const u8) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const manager = self.api_server.cfg.user_manager orelse {
            _ = ctx.status(503);
            return ctx.text("user management not configured");
        };
        manager.deleteApiKey(user_name, key_id) catch |err| switch (err) {
            error.ApiKeyNotFound => {
                _ = ctx.status(404);
                return ctx.text("not found");
            },
            else => return err,
        };
        _ = ctx.status(204);
        return ctx.text("");
    }
};

fn sleepNs(duration_ns: u64) void {
    var req = std.posix.timespec{
        .sec = @intCast(duration_ns / std.time.ns_per_s),
        .nsec = @intCast(duration_ns % std.time.ns_per_s),
    };
    while (true) switch (std.posix.errno(std.posix.system.nanosleep(&req, &req))) {
        .SUCCESS => return,
        .INTR => continue,
        else => return,
    };
}

fn PrefixedServer(comptime prefix: []const u8, comptime Inner: type) type {
    return struct {
        inner: *Inner,

        pub fn post(self: *const @This(), comptime path: []const u8, handler_fn: httpx.Handler) !void {
            try self.inner.post(prefix ++ path, handler_fn);
        }

        pub fn get(self: *const @This(), comptime path: []const u8, handler_fn: httpx.Handler) !void {
            try self.inner.get(prefix ++ path, handler_fn);
        }

        pub fn put(self: *const @This(), comptime path: []const u8, handler_fn: httpx.Handler) !void {
            try self.inner.put(prefix ++ path, handler_fn);
        }

        pub fn delete(self: *const @This(), comptime path: []const u8, handler_fn: httpx.Handler) !void {
            try self.inner.delete(prefix ++ path, handler_fn);
        }
    };
}

const TestAuthManager = struct {
    store: usermgr.MemoryStore,
    policy_store: casbin.MemoryAdapter,
    manager: usermgr.UserManager,
};

fn initTestAuthManager(alloc: std.mem.Allocator) !TestAuthManager {
    return .{
        .store = usermgr.MemoryStore.init(alloc),
        .policy_store = casbin.MemoryAdapter.init(alloc),
        .manager = undefined,
    };
}

fn bindTestAuthManager(alloc: std.mem.Allocator, auth: *TestAuthManager) !void {
    auth.manager = try usermgr.UserManager.init(
        alloc,
        auth.store.iface(),
        try usermgr.initDefaultEnforcer(alloc, auth.policy_store.iface()),
    );
}

fn encodeBasicAuthorization(alloc: std.mem.Allocator, username: []const u8, password: []const u8) ![]u8 {
    const raw = try std.fmt.allocPrint(alloc, "{s}:{s}", .{ username, password });
    defer alloc.free(raw);
    const size = std.base64.standard.Encoder.calcSize(raw.len);
    const encoded = try alloc.alloc(u8, size);
    defer alloc.free(encoded);
    _ = std.base64.standard.Encoder.encode(encoded, raw);
    return try std.fmt.allocPrint(alloc, "Basic {s}", .{encoded});
}

const HttpxE2eServer = struct {
    allocator: std.mem.Allocator,
    io_impl: std.Io.Threaded,
    server: httpx.Server,
    handler: AntflyApiHandler,
    thread: ?std.Thread = null,

    fn init(self: *HttpxE2eServer, allocator: std.mem.Allocator, api_server: *ApiHttpServer) !void {
        return self.initWithLimits(allocator, api_server, 32, 1_000);
    }

    fn initWithLimits(
        self: *HttpxE2eServer,
        allocator: std.mem.Allocator,
        api_server: *ApiHttpServer,
        query_capacity: usize,
        max_connections: u32,
    ) !void {
        api_server.query_admission = RequestAdmission.init(query_capacity);
        self.* = .{
            .allocator = allocator,
            .io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{}),
            .server = undefined,
            .handler = .{
                .api_server = api_server,
                .query_body_admission = RequestAdmission.init(query_capacity),
            },
            .thread = null,
        };
        errdefer self.io_impl.deinit();

        try self.handler.initRuntime(allocator);
        errdefer self.handler.deinitRuntime();

        self.server = httpx.Server.initWithConfig(allocator, self.io_impl.io(), .{
            .host = "127.0.0.1",
            .port = 0,
            .header_read_timeout_ms = 30_000,
            .body_read_timeout_ms = 30_000,
            .response_write_timeout_ms = 30_000,
            .max_connections = max_connections,
        });
        errdefer self.server.deinit();

        try self.handler.registerRoutes(&self.server);

        try self.server.bind();
        self.thread = try std.Thread.spawn(.{}, listenHttpxE2eServer, .{&self.server});
    }

    fn deinit(self: *HttpxE2eServer) void {
        if (self.thread) |thread| {
            self.server.stop();
            thread.join();
        }
        self.handler.deinitRuntime();
        self.server.deinit();
        self.io_impl.deinit();
        self.* = undefined;
    }

    fn baseUrl(self: *HttpxE2eServer, alloc: std.mem.Allocator) ![]u8 {
        const addr = self.server.boundAddress() orelse return error.AddressNotAvailable;
        return std.fmt.allocPrint(alloc, "http://127.0.0.1:{d}", .{addr.ip4.port});
    }
};

fn listenHttpxE2eServer(server: *httpx.Server) void {
    server.listen() catch |err| switch (err) {
        else => std.debug.panic("httpx e2e listener failed: {}", .{err}),
    };
}

fn getWithRetry(
    client: *httpx.Client,
    io: std.Io,
    url: []const u8,
    headers: ?[]const [2][]const u8,
    max_attempts: usize,
) !httpx.Response {
    var attempts: usize = 0;
    while (attempts < max_attempts) : (attempts += 1) {
        return client.get(url, .{ .headers = headers }) catch |err| {
            if (err != error.ConnectionRefused or attempts + 1 >= max_attempts) return err;
            io.sleep(std.Io.Duration.fromMilliseconds(50), .awake) catch {};
            continue;
        };
    }
    unreachable;
}

fn requestWithRetry(
    client: *httpx.Client,
    io: std.Io,
    method: httpx.Method,
    url: []const u8,
    body: ?[]const u8,
    headers: ?[]const [2][]const u8,
    max_attempts: usize,
) !httpx.Response {
    var attempts: usize = 0;
    while (attempts < max_attempts) : (attempts += 1) {
        return client.request(method, url, .{
            .body = body,
            .headers = headers,
        }) catch |err| {
            if (err != error.ConnectionRefused or attempts + 1 >= max_attempts) return err;
            io.sleep(std.Io.Duration.fromMilliseconds(50), .awake) catch {};
            continue;
        };
    }
    unreachable;
}

const AuthStatusSource = struct {
    fn iface(_: *@This()) http_server_mod.StatusSource {
        return .{
            .ptr = undefined,
            .vtable = &.{ .status = status },
        };
    }

    fn status(_: *anyopaque) !metadata_api.MetadataStatus {
        return .{
            .metadata_group_id = 77,
            .metrics = .{},
            .projected_stores = 1,
        };
    }
};

const LookupStatusSource = struct {
    fn iface(_: *@This()) http_server_mod.StatusSource {
        return .{
            .ptr = undefined,
            .vtable = &.{
                .status = status,
                .admin_snapshot = adminSnapshot,
                .free_admin_snapshot = freeAdminSnapshot,
            },
        };
    }

    fn status(_: *anyopaque) !metadata_api.MetadataStatus {
        return .{
            .metadata_group_id = 1,
            .metrics = .{},
            .projected_stores = 1,
        };
    }

    fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
        return .{
            .status = .{ .metadata_group_id = 1, .metrics = .{} },
            .tables = @constCast((&[_]metadata_table_manager.TableRecord{
                .{ .table_id = 1, .name = "docs", .placement_role = "data" },
            })[0..]),
            .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                .{ .group_id = 10, .table_id = 1, .start_key = "doc:a", .end_key = "doc:z" },
            })[0..]),
            .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
            .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
            .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
            .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
        };
    }

    fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
};

const SchemaUpdateStatusSource = struct {
    projection_wait_calls: std.atomic.Value(u32) = .init(0),
    schema_json: ?[]const u8 = null,
    owns_schema_json: bool = false,
    table_buf: [1]metadata_table_manager.TableRecord = undefined,
    range_buf: [1]metadata_table_manager.RangeRecord = undefined,

    fn iface(self: *@This()) http_server_mod.StatusSource {
        return .{
            .ptr = self,
            .vtable = &.{
                .status = status,
                .admin_snapshot = adminSnapshot,
                .free_admin_snapshot = freeAdminSnapshot,
                .update_schema = updateSchema,
                .wait_table_projection = waitTableProjection,
            },
        };
    }

    fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.owns_schema_json) alloc.free(self.schema_json.?);
    }

    fn replaceSchemaJson(self: *@This(), alloc: std.mem.Allocator, next: []const u8) !void {
        if (self.owns_schema_json) alloc.free(self.schema_json.?);
        self.schema_json = try alloc.dupe(u8, next);
        self.owns_schema_json = true;
    }

    fn status(_: *anyopaque) !metadata_api.MetadataStatus {
        return .{
            .metadata_group_id = 1,
            .metrics = .{},
            .projected_stores = 1,
        };
    }

    fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        self.table_buf[0] = .{
            .table_id = 7,
            .name = "docs",
            .schema_json = tables_api.effectiveSchemaJson(self.schema_json),
            .indexes_json = tables_api.default_indexes_json,
            .placement_role = "data",
        };
        self.range_buf[0] = .{
            .group_id = 7001,
            .table_id = 7,
            .start_key = "",
            .end_key = null,
        };
        return .{
            .status = .{ .metadata_group_id = 1, .metrics = .{} },
            .tables = &self.table_buf,
            .ranges = &self.range_buf,
            .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
            .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
            .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
            .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
        };
    }

    fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}

    fn updateSchema(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, schema_json: []const u8) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        try std.testing.expectEqualStrings("docs", table_name);
        try self.replaceSchemaJson(alloc, schema_json);
    }

    fn waitTableProjection(ptr: *anyopaque, table_name: []const u8, schema_json: ?[]const u8, indexes_json: ?[]const u8) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        try std.testing.expectEqualStrings("docs", table_name);
        try std.testing.expect(indexes_json != null);
        try std.testing.expect(schema_json != null);
        try std.testing.expect(self.schema_json != null);
        _ = self.projection_wait_calls.fetchAdd(1, .monotonic);
    }
};

const SchemaReconcileWriteSource = struct {
    reconcile_calls: std.atomic.Value(u32) = .init(0),
    synchronous_update_calls: std.atomic.Value(u32) = .init(0),

    fn iface(self: *@This()) table_writes.TableWriteSource {
        return .{
            .ptr = self,
            .vtable = &.{
                .batch = batch,
                .update_schema = updateSchema,
                .request_table_structural_reconcile = requestStructuralReconcile,
            },
        };
    }

    fn batch(
        _: *anyopaque,
        _: std.mem.Allocator,
        _: []const u8,
        _: db_mod.types.BatchRequest,
    ) !?void {
        return {};
    }

    fn updateSchema(
        ptr: *anyopaque,
        _: std.mem.Allocator,
        _: []const u8,
        _: []const u8,
    ) !?void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        _ = self.synchronous_update_calls.fetchAdd(1, .monotonic);
        return {};
    }

    fn requestStructuralReconcile(
        ptr: *anyopaque,
        _: std.mem.Allocator,
        table_name: []const u8,
    ) !?void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        try std.testing.expectEqualStrings("docs", table_name);
        _ = self.reconcile_calls.fetchAdd(1, .monotonic);
        return {};
    }
};

test "typed internal HTTP errors preserve conflict semantics" {
    const spec = AntflyApiHandler.sharedInternalHttpErrorSpec(error.DocIdentityNamespaceMismatch).?;
    try std.testing.expectEqual(@as(u16, 409), spec.status);
    try std.testing.expectEqualStrings("doc identity namespace mismatch", spec.message);
    const stale_cursor = AntflyApiHandler.sharedInternalHttpErrorSpec(error.HierarchyCursorStale).?;
    try std.testing.expectEqual(@as(u16, 409), stale_cursor.status);
    try std.testing.expectEqualStrings("HierarchyCursorStale", stale_cursor.message);
    try std.testing.expect(AntflyApiHandler.sharedInternalHttpErrorSpec(error.NotFound) == null);
}

test "HA mutation middleware fails closed for unregistered HTTP methods" {
    try std.testing.expect(AntflyApiHandler.classifyHaMutation(.GET, "/tables/docs") == null);
    try std.testing.expect(AntflyApiHandler.classifyHaMutation(.HEAD, "/tables/docs") == null);
    try std.testing.expect(AntflyApiHandler.classifyHaMutation(.OPTIONS, "/tables/docs") == null);

    const patch = AntflyApiHandler.classifyHaMutation(.PATCH, "/tables/docs").?;
    try std.testing.expectEqual(ha_mutation_inventory.Surface.unclassified_non_get, patch.surface);
    try std.testing.expectEqual(ha_mutation_inventory.Disposition.reject, patch.disposition);
}

test "httpx multi batch route uses the batch commit hook and public response contract" {
    if (comptime builtin.os.tag == .windows or builtin.os.tag == .freestanding) return;

    const FakeWrites = struct {
        batch_calls: usize = 0,
        transaction_calls: usize = 0,
        batch_commit_calls: usize = 0,
        cancellation_transaction_calls: usize = 0,
        cancellation_batch_calls: usize = 0,
        fail_batch_commit: bool = false,
        unknown_batch_commit: bool = false,
        defer_batch_commit: bool = false,
        defer_transaction_commit: bool = false,
        cancel_batch_visibility_wait: bool = false,
        cancel_transaction_visibility_wait: bool = false,

        fn source(self: *@This()) table_writes.TableWriteSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .batch = batch,
                    .commit_transaction = commitTransaction,
                    .commit_transaction_with_cancellation = commitTransactionWithCancellation,
                    .commit_batch = commitBatch,
                    .commit_batch_with_cancellation = commitBatchWithCancellation,
                },
            };
        }

        fn batch(
            ptr: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: db_mod.types.BatchRequest,
        ) anyerror!?void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.batch_calls += 1;
            return error.TestUnexpectedResult;
        }

        fn commitTransaction(
            ptr: *anyopaque,
            _: std.mem.Allocator,
            _: []const distributed_txn.TableCommitRequest,
            _: db_mod.types.SyncLevel,
        ) anyerror!?distributed_txn.CommitOutcome {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.transaction_calls += 1;
            if (self.defer_transaction_commit) return .{ .committed = .{
                .participant_count = 2,
                .visibility_pending = true,
                .visibility_retry_pending = true,
            } };
            return error.TestUnexpectedResult;
        }

        fn commitTransactionWithCancellation(
            ptr: *anyopaque,
            alloc_: std.mem.Allocator,
            tables: []const distributed_txn.TableCommitRequest,
            sync_level: db_mod.types.SyncLevel,
            cancellation: db_mod.types.CancellationToken,
        ) anyerror!?distributed_txn.CommitOutcome {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.cancellation_transaction_calls += 1;
            try std.testing.expect(cancellation.ptr != null);
            if (self.cancel_transaction_visibility_wait) return error.EnrichmentWaitCanceled;
            return commitTransaction(ptr, alloc_, tables, sync_level);
        }

        fn commitBatch(
            ptr: *anyopaque,
            _: std.mem.Allocator,
            tables: []const distributed_txn.TableCommitRequest,
            sync_level: db_mod.types.SyncLevel,
        ) anyerror!?distributed_txn.CommitOutcome {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.batch_commit_calls += 1;
            if (self.fail_batch_commit) return error.CommitPropagationIncomplete;
            if (self.unknown_batch_commit) return error.CommitDecisionUnknown;
            if (self.defer_batch_commit) return .{ .committed = .{
                .participant_count = 2,
                .propagation_pending = true,
            } };
            try std.testing.expectEqual(@as(usize, 2), tables.len);
            try std.testing.expectEqualStrings("users", tables[0].table_name);
            try std.testing.expectEqualStrings("orders", tables[1].table_name);
            try std.testing.expectEqual(@as(usize, 1), tables[0].writes.len);
            try std.testing.expectEqual(@as(usize, 1), tables[1].deletes.len);
            try std.testing.expectEqual(db_mod.types.SyncLevel.write, sync_level);
            return .{ .committed = .{ .participant_count = 2 } };
        }

        fn commitBatchWithCancellation(
            ptr: *anyopaque,
            alloc_: std.mem.Allocator,
            tables: []const distributed_txn.TableCommitRequest,
            sync_level: db_mod.types.SyncLevel,
            cancellation: db_mod.types.CancellationToken,
        ) anyerror!?distributed_txn.CommitOutcome {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.cancellation_batch_calls += 1;
            try std.testing.expect(cancellation.ptr != null);
            if (self.cancel_batch_visibility_wait) return error.EnrichmentWaitCanceled;
            return commitBatch(ptr, alloc_, tables, sync_level);
        }
    };

    const alloc = std.testing.allocator;
    var status = AuthStatusSource{};
    var writes = FakeWrites{};
    var api_server = ApiHttpServer.init(alloc, .{}, status.iface(), null, writes.source());
    var e2e_server: HttpxE2eServer = undefined;
    e2e_server.init(alloc, &api_server) catch |err| switch (err) {
        // Restricted test environments may forbid even loopback listeners.
        // The same test runs normally in CI and release validation.
        error.Unexpected => return error.SkipZigTest,
        else => return err,
    };
    defer e2e_server.deinit();

    var client_io = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer client_io.deinit();
    var client = httpx.Client.initWithConfig(alloc, client_io.io(), .{ .keep_alive = false });
    defer client.deinit();
    const base_url = try e2e_server.baseUrl(alloc);
    defer alloc.free(base_url);
    const batch_url = try std.fmt.allocPrint(alloc, "{s}/db/v1/batch", .{base_url});
    defer alloc.free(batch_url);
    const transaction_url = try std.fmt.allocPrint(alloc, "{s}/db/v1/transactions/commit", .{base_url});
    defer alloc.free(transaction_url);
    const headers = [_][2][]const u8{.{ "content-type", "application/json" }};

    var response = try requestWithRetry(
        &client,
        client_io.io(),
        .POST,
        batch_url,
        "{\"tables\":{\"users\":{\"inserts\":{\"user:1\":{\"name\":\"Alice\"}}},\"orders\":{\"deletes\":[\"order:old\"]}},\"sync_level\":\"write\"}",
        &headers,
        20,
    );
    defer response.deinit();
    try std.testing.expectEqual(@as(u16, 201), response.status.code);
    try std.testing.expectEqualStrings("application/json", response.contentType().?);
    var parsed = try std.json.parseFromSlice(transactions_api.MultiBatchResponse, alloc, response.body.?, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(u32, 1), parsed.value.tables.map.get("users").?.inserted);
    try std.testing.expectEqual(@as(u32, 1), parsed.value.tables.map.get("orders").?.deleted);
    try std.testing.expectEqual(@as(usize, 1), writes.batch_commit_calls);
    try std.testing.expectEqual(@as(usize, 1), writes.cancellation_batch_calls);
    try std.testing.expectEqual(@as(usize, 0), writes.transaction_calls);
    try std.testing.expectEqual(@as(usize, 0), writes.batch_calls);

    writes.defer_batch_commit = true;
    var pending = try requestWithRetry(
        &client,
        client_io.io(),
        .POST,
        batch_url,
        "{\"tables\":{\"users\":{\"inserts\":{\"user:2\":{\"name\":\"Bob\"}}},\"orders\":{\"deletes\":[\"order:old\"]}},\"sync_level\":\"write\"}",
        &headers,
        20,
    );
    defer pending.deinit();
    try std.testing.expectEqual(@as(u16, 202), pending.status.code);
    var pending_parsed = try std.json.parseFromSlice(transactions_api.MultiBatchResponse, alloc, pending.body.?, .{});
    defer pending_parsed.deinit();
    try std.testing.expectEqualStrings("committed_recovery_pending", pending_parsed.value.status);
    writes.defer_batch_commit = false;

    writes.defer_transaction_commit = true;
    var transaction_pending = try requestWithRetry(
        &client,
        client_io.io(),
        .POST,
        transaction_url,
        "{\"read_set\":[],\"tables\":{\"users\":{\"inserts\":{\"user:3\":{\"name\":\"Carol\"}}}}}",
        &headers,
        20,
    );
    defer transaction_pending.deinit();
    try std.testing.expectEqual(@as(u16, 202), transaction_pending.status.code);
    var transaction_pending_parsed = try std.json.parseFromSlice(transactions_api.CommitResponse, alloc, transaction_pending.body.?, .{});
    defer transaction_pending_parsed.deinit();
    try std.testing.expectEqualStrings("committed_visibility_pending", transaction_pending_parsed.value.status);
    try std.testing.expectEqual(@as(usize, 1), writes.transaction_calls);
    try std.testing.expectEqual(@as(usize, 1), writes.cancellation_transaction_calls);
    writes.defer_transaction_commit = false;

    var rejected = try requestWithRetry(
        &client,
        client_io.io(),
        .POST,
        batch_url,
        "{\"read_set\":[],\"tables\":{\"users\":{\"deletes\":[\"user:1\"]}}}",
        &headers,
        20,
    );
    defer rejected.deinit();
    try std.testing.expectEqual(@as(u16, 400), rejected.status.code);
    try std.testing.expectEqual(@as(usize, 2), writes.batch_commit_calls);

    writes.fail_batch_commit = true;
    var committed_pending = try requestWithRetry(
        &client,
        client_io.io(),
        .POST,
        batch_url,
        "{\"tables\":{\"users\":{\"inserts\":{\"user:2\":{\"name\":\"Bob\"}}},\"orders\":{\"deletes\":[\"order:old\"]}},\"sync_level\":\"write\"}",
        &headers,
        20,
    );
    defer committed_pending.deinit();
    try std.testing.expectEqual(@as(u16, 202), committed_pending.status.code);
    var committed_pending_parsed = try std.json.parseFromSlice(transactions_api.MultiBatchResponse, alloc, committed_pending.body.?, .{});
    defer committed_pending_parsed.deinit();
    try std.testing.expectEqualStrings("committed_recovery_pending", committed_pending_parsed.value.status);
    try std.testing.expectEqual(@as(usize, 3), writes.batch_commit_calls);

    writes.fail_batch_commit = false;
    writes.unknown_batch_commit = true;
    var unknown = try requestWithRetry(
        &client,
        client_io.io(),
        .POST,
        batch_url,
        "{\"tables\":{\"users\":{\"inserts\":{\"user:2\":{\"name\":\"Bob\"}}},\"orders\":{\"deletes\":[\"order:old\"]}},\"sync_level\":\"write\"}",
        &headers,
        20,
    );
    defer unknown.deinit();
    try std.testing.expectEqual(@as(u16, 500), unknown.status.code);
    try std.testing.expectEqualStrings(
        "transaction outcome is unknown; do not retry this stateless request because it may already have committed; use a transaction session for retryable commits",
        unknown.body.?,
    );
    try std.testing.expectEqual(@as(usize, 4), writes.batch_commit_calls);

    writes.unknown_batch_commit = false;
    writes.cancel_batch_visibility_wait = true;
    var canceled_batch_wait = try requestWithRetry(
        &client,
        client_io.io(),
        .POST,
        batch_url,
        "{\"tables\":{\"users\":{\"inserts\":{\"user:4\":{\"name\":\"Dana\"}}},\"orders\":{\"deletes\":[\"order:old\"]}},\"sync_level\":\"enrichments\"}",
        &headers,
        20,
    );
    defer canceled_batch_wait.deinit();
    try std.testing.expectEqual(@as(u16, 202), canceled_batch_wait.status.code);
    var canceled_batch_parsed = try std.json.parseFromSlice(transactions_api.MultiBatchResponse, alloc, canceled_batch_wait.body.?, .{});
    defer canceled_batch_parsed.deinit();
    try std.testing.expectEqualStrings("committed_visibility_pending", canceled_batch_parsed.value.status);
    try std.testing.expectEqual(@as(usize, 5), writes.cancellation_batch_calls);

    writes.cancel_transaction_visibility_wait = true;
    var canceled_transaction_wait = try requestWithRetry(
        &client,
        client_io.io(),
        .POST,
        transaction_url,
        "{\"read_set\":[],\"tables\":{\"users\":{\"inserts\":{\"user:5\":{\"name\":\"Eve\"}}}},\"sync_level\":\"enrichments\"}",
        &headers,
        20,
    );
    defer canceled_transaction_wait.deinit();
    try std.testing.expectEqual(@as(u16, 202), canceled_transaction_wait.status.code);
    var canceled_transaction_parsed = try std.json.parseFromSlice(transactions_api.CommitResponse, alloc, canceled_transaction_wait.body.?, .{});
    defer canceled_transaction_parsed.deinit();
    try std.testing.expectEqualStrings("committed_visibility_pending", canceled_transaction_parsed.value.status);
    try std.testing.expectEqual(@as(usize, 2), writes.cancellation_transaction_calls);
}

test "httpx stable transaction commit durably hands off recovery before acknowledgement" {
    if (comptime builtin.os.tag == .windows or builtin.os.tag == .freestanding) return;

    const FakeWrites = struct {
        commit_calls: usize = 0,
        cancellation_commit_calls: usize = 0,
        acknowledge_calls: usize = 0,
        next_commit_error: ?anyerror = error.EnrichmentWaitCanceled,

        fn source(self: *@This()) table_writes.TableWriteSource {
            return .{ .ptr = self, .vtable = &.{
                .batch = batch,
                .commit_transaction_with_id = commitTransactionWithId,
                .commit_transaction_with_id_with_cancellation = commitTransactionWithIdAndCancellation,
                .acknowledge_transaction_commit = acknowledgeTransactionCommit,
            } };
        }

        fn batch(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: db_mod.types.BatchRequest) anyerror!?void {
            return error.TestUnexpectedResult;
        }

        fn commitTransactionWithId(
            ptr: *anyopaque,
            _: std.mem.Allocator,
            _: db_mod.types.TxnId,
            _: u64,
            tables: []const distributed_txn.TableCommitRequest,
            sync_level: db_mod.types.SyncLevel,
        ) anyerror!?distributed_txn.CommitOutcome {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.commit_calls += 1;
            try std.testing.expectEqual(@as(usize, 1), tables.len);
            try std.testing.expectEqualStrings("docs", tables[0].table_name);
            if (self.next_commit_error) |err| {
                try std.testing.expectEqual(db_mod.types.SyncLevel.propose, sync_level);
                self.next_commit_error = null;
                return err;
            }
            try std.testing.expectEqual(db_mod.types.SyncLevel.write, sync_level);
            return .{ .committed = .{
                .participant_count = 1,
                .coordinator_group_id = 7001,
                .coordinator_table_name = "docs",
                .propagation_pending = self.commit_calls == 1,
            } };
        }

        fn commitTransactionWithIdAndCancellation(
            ptr: *anyopaque,
            alloc_: std.mem.Allocator,
            txn_id: db_mod.types.TxnId,
            begin_timestamp: u64,
            tables: []const distributed_txn.TableCommitRequest,
            sync_level: db_mod.types.SyncLevel,
            cancellation: db_mod.types.CancellationToken,
        ) anyerror!?distributed_txn.CommitOutcome {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.cancellation_commit_calls += 1;
            try std.testing.expect(cancellation.ptr != null);
            return commitTransactionWithId(ptr, alloc_, txn_id, begin_timestamp, tables, sync_level);
        }

        fn acknowledgeTransactionCommit(
            ptr: *anyopaque,
            _: std.mem.Allocator,
            _: db_mod.types.TxnId,
            coordinator_group_id: u64,
            coordinator_table_name: []const u8,
        ) anyerror!?void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.acknowledge_calls += 1;
            try std.testing.expectEqual(@as(u64, 7001), coordinator_group_id);
            try std.testing.expectEqualStrings("docs", coordinator_table_name);
            if (self.acknowledge_calls == 1) return error.InjectedAcknowledgementFailure;
            return {};
        }
    };

    const alloc = std.testing.allocator;
    var status = AuthStatusSource{};
    var writes = FakeWrites{};
    var api_server = ApiHttpServer.init(alloc, .{}, status.iface(), null, writes.source());
    defer api_server.deinit();
    var e2e_server: HttpxE2eServer = undefined;
    e2e_server.init(alloc, &api_server) catch |err| switch (err) {
        error.Unexpected => return error.SkipZigTest,
        else => return err,
    };
    defer e2e_server.deinit();

    var client_io = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer client_io.deinit();
    var client = httpx.Client.initWithConfig(alloc, client_io.io(), .{ .keep_alive = false });
    defer client.deinit();
    const base_url = try e2e_server.baseUrl(alloc);
    defer alloc.free(base_url);
    const begin_url = try std.fmt.allocPrint(alloc, "{s}/db/v1/transactions/begin", .{base_url});
    defer alloc.free(begin_url);
    const headers = [_][2][]const u8{.{ "content-type", "application/json" }};

    var begin = try requestWithRetry(
        &client,
        client_io.io(),
        .POST,
        begin_url,
        "{\"sync_level\":\"propose\"}",
        &headers,
        20,
    );
    defer begin.deinit();
    try std.testing.expectEqual(@as(u16, 201), begin.status.code);
    var parsed_begin = try std.json.parseFromSlice(transactions_api.BeginResponse, alloc, begin.body.?, .{});
    defer parsed_begin.deinit();
    const commit_url = try std.fmt.allocPrint(
        alloc,
        "{s}/db/v1/transactions/{s}/commit",
        .{ base_url, parsed_begin.value.transaction_id },
    );
    defer alloc.free(commit_url);
    const commit_body = "{\"read_set\":[],\"tables\":{\"docs\":{\"inserts\":{\"counter\":{\"value\":1}}}}}";

    var first = try requestWithRetry(&client, client_io.io(), .POST, commit_url, commit_body, &headers, 20);
    defer first.deinit();
    try std.testing.expectEqual(@as(u16, 202), first.status.code);
    var parsed_first = try std.json.parseFromSlice(transactions_api.SessionCommitResponse, alloc, first.body.?, .{});
    defer parsed_first.deinit();
    try std.testing.expectEqualStrings("committed_visibility_pending", parsed_first.value.status);
    try std.testing.expectEqual(@as(usize, 1), writes.commit_calls);
    try std.testing.expectEqual(@as(usize, 1), writes.cancellation_commit_calls);
    try std.testing.expectEqual(@as(usize, 0), writes.acknowledge_calls);

    // The post-commit wait error leaves the exact sealed request in durable
    // recovery. Recovery obtains coordinator metadata under the same transaction
    // ID; the first final ACK fails, and the next pass retries only that
    // idempotent handoff.
    try api_server.runSessionMaintenanceOnce();
    try std.testing.expectEqual(@as(usize, 2), writes.commit_calls);
    try std.testing.expectEqual(@as(usize, 1), writes.cancellation_commit_calls);
    try std.testing.expectEqual(@as(usize, 1), writes.acknowledge_calls);
    try api_server.runSessionMaintenanceOnce();
    try std.testing.expectEqual(@as(usize, 2), writes.commit_calls);
    try std.testing.expectEqual(@as(usize, 2), writes.acknowledge_calls);

    var retry = try requestWithRetry(&client, client_io.io(), .POST, commit_url, commit_body, &headers, 20);
    defer retry.deinit();
    try std.testing.expectEqual(@as(u16, 200), retry.status.code);
    var parsed_retry = try std.json.parseFromSlice(transactions_api.SessionCommitResponse, alloc, retry.body.?, .{});
    defer parsed_retry.deinit();
    try std.testing.expectEqualStrings("committed", parsed_retry.value.status);
    try std.testing.expectEqual(@as(usize, 2), writes.commit_calls);
    try std.testing.expectEqual(@as(usize, 2), writes.acknowledge_calls);

    // Terminal enrichment debt uses the same durable replay handoff but must
    // never be presented as a live retry to the caller.
    var repair_begin = try requestWithRetry(
        &client,
        client_io.io(),
        .POST,
        begin_url,
        "{\"sync_level\":\"propose\"}",
        &headers,
        20,
    );
    defer repair_begin.deinit();
    try std.testing.expectEqual(@as(u16, 201), repair_begin.status.code);
    var parsed_repair_begin = try std.json.parseFromSlice(transactions_api.BeginResponse, alloc, repair_begin.body.?, .{});
    defer parsed_repair_begin.deinit();
    const repair_commit_url = try std.fmt.allocPrint(
        alloc,
        "{s}/db/v1/transactions/{s}/commit",
        .{ base_url, parsed_repair_begin.value.transaction_id },
    );
    defer alloc.free(repair_commit_url);
    writes.next_commit_error = error.EnrichmentWorkerFailed;

    var repair = try requestWithRetry(&client, client_io.io(), .POST, repair_commit_url, commit_body, &headers, 20);
    defer repair.deinit();
    try std.testing.expectEqual(@as(u16, 202), repair.status.code);
    var parsed_repair = try std.json.parseFromSlice(transactions_api.SessionCommitResponse, alloc, repair.body.?, .{});
    defer parsed_repair.deinit();
    try std.testing.expectEqualStrings("committed_repair_required", parsed_repair.value.status);
    try std.testing.expectEqual(@as(usize, 3), writes.commit_calls);

    try api_server.runSessionMaintenanceOnce();
    try std.testing.expectEqual(@as(usize, 4), writes.commit_calls);
    try std.testing.expectEqual(@as(usize, 3), writes.acknowledge_calls);
}

test "httpx MCP route preserves protocol session headers" {
    const alloc = std.testing.allocator;
    var source = AuthStatusSource{};
    var api_server = ApiHttpServer.init(alloc, .{}, source.iface(), null, null);
    defer api_server.deinit();
    var e2e_server: HttpxE2eServer = undefined;
    try e2e_server.init(alloc, &api_server);
    defer e2e_server.deinit();
    var client_io = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer client_io.deinit();
    var client = httpx.Client.initWithConfig(alloc, client_io.io(), .{ .keep_alive = false });
    defer client.deinit();
    const base_url = try e2e_server.baseUrl(alloc);
    defer alloc.free(base_url);
    const url = try std.fmt.allocPrint(alloc, "{s}/mcp/v1", .{base_url});
    defer alloc.free(url);
    const headers = [_][2][]const u8{.{ "content-type", "application/json" }};
    var response = try requestWithRetry(
        &client,
        client_io.io(),
        .POST,
        url,
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\",\"params\":{}}",
        &headers,
        20,
    );
    defer response.deinit();
    try std.testing.expectEqual(@as(u16, 200), response.status.code);
    try std.testing.expect(response.header(mcp.session_id_header) != null);
    try std.testing.expectEqualStrings("2025-06-18", response.header("Mcp-Protocol-Version").?);
    try std.testing.expect(std.mem.indexOf(u8, response.body.?, "\"protocolVersion\":\"2025-06-18\"") != null);
}

test "httpx shared registrar keeps root probes and rejects removed data aliases" {
    const alloc = std.testing.allocator;
    var source = AuthStatusSource{};
    var api_server = ApiHttpServer.init(alloc, .{}, source.iface(), null, null);
    defer api_server.deinit();

    var e2e_server: HttpxE2eServer = undefined;
    try e2e_server.init(alloc, &api_server);
    defer e2e_server.deinit();

    var client_io = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer client_io.deinit();
    var client = httpx.Client.initWithConfig(alloc, client_io.io(), .{ .keep_alive = false });
    defer client.deinit();
    const base_url = try e2e_server.baseUrl(alloc);
    defer alloc.free(base_url);

    inline for (.{ "/healthz", "/readyz" }) |path| {
        const url = try std.fmt.allocPrint(alloc, "{s}{s}", .{ base_url, path });
        defer alloc.free(url);
        var response = try getWithRetry(&client, client_io.io(), url, null, 20);
        defer response.deinit();
        try std.testing.expectEqual(@as(u16, 200), response.status.code);
    }

    const extensions_url = try std.fmt.allocPrint(alloc, "{s}/extensions/v1", .{base_url});
    defer alloc.free(extensions_url);
    var extensions = try getWithRetry(&client, client_io.io(), extensions_url, null, 20);
    defer extensions.deinit();
    try std.testing.expectEqual(@as(u16, 200), extensions.status.code);
    try std.testing.expectEqualStrings("application/json", extensions.header("content-type").?);
    try std.testing.expect(std.mem.indexOf(u8, extensions.body.?, "\"packages\":\"/extensions/v1/packages\"") != null);

    const encoded_job = try api_server.repair_job_store.startJob(alloc, "documents", .{});
    defer alloc.free(encoded_job);
    var parsed_job = try std.json.parseFromSlice(@import("repair_jobs.zig").JobState, alloc, encoded_job, .{});
    defer parsed_job.deinit();
    const cancel_state_url = try std.fmt.allocPrint(
        alloc,
        "{s}/internal/v1/tables/documents/repair/jobs/{d}/attempts/{d}/cancel-state",
        .{ base_url, parsed_job.value.job_id, parsed_job.value.attempt_id },
    );
    defer alloc.free(cancel_state_url);
    var cancel_state_response = try getWithRetry(&client, client_io.io(), cancel_state_url, null, 20);
    defer cancel_state_response.deinit();
    try std.testing.expectEqual(@as(u16, 200), cancel_state_response.status.code);
    try std.testing.expectEqualStrings("{\"cancel_requested\":false}", cancel_state_response.body.?);

    inline for (.{ "/status", "/tables", "/secrets", "/transactions", "/backup", "/restore" }) |path| {
        const url = try std.fmt.allocPrint(alloc, "{s}{s}", .{ base_url, path });
        defer alloc.free(url);
        var response = try getWithRetry(&client, client_io.io(), url, null, 20);
        defer response.deinit();
        try std.testing.expectEqual(@as(u16, 404), response.status.code);
    }
}

test "httpx internal control routes call typed operations directly" {
    const alloc = std.testing.allocator;
    const Fake = struct {
        fn reads() table_reads.TableReadSource {
            return .{ .ptr = undefined, .vtable = &.{
                .lookup = publicLookup,
                .scan = scan,
                .query = query,
                .lookup_group_local = groupLookup,
            } };
        }

        fn shardDb() @import("../metadata/domain.zig").ShardDbAdapter {
            return .{ .ptr = undefined, .vtable = &.{
                .fetch_median_key = medianKey,
                .schema_index_ready = schemaIndexReady,
            } };
        }

        fn publicLookup(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: db_mod.types.LookupOptions, _: raft_mod.ReadConsistency) !?table_reads.LookupResponse {
            return null;
        }

        fn scan(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: []const u8, _: db_mod.types.ScanOptions, _: raft_mod.ReadConsistency) !?table_reads.ScanResponse {
            return null;
        }

        fn query(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: db_mod.types.SearchRequest, _: raft_mod.ReadConsistency) !?query_api.QueryResponse {
            return null;
        }

        fn groupLookup(_: *anyopaque, inner_alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, key: []const u8, opts: db_mod.types.LookupOptions, consistency: raft_mod.ReadConsistency) !?table_reads.LookupResponse {
            try std.testing.expectEqual(@as(u64, 7), group_id);
            try std.testing.expectEqualStrings("docs", table_name);
            try std.testing.expectEqualStrings("doc:a", key);
            try std.testing.expectEqual(@as(usize, 1), opts.fields.len);
            try std.testing.expectEqualStrings("title", opts.fields[0]);
            try std.testing.expectEqual(raft_mod.ReadConsistency.stale, consistency);
            return .{ .json = try inner_alloc.dupe(u8, "{\"title\":\"alpha\"}"), .version = 42 };
        }

        fn medianKey(_: *anyopaque, inner_alloc: std.mem.Allocator, group_id: u64) !?[]u8 {
            try std.testing.expectEqual(@as(u64, 7), group_id);
            return try inner_alloc.dupe(u8, "doc:m");
        }

        fn schemaIndexReady(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: u64, _: u32, _: u32) !bool {
            return true;
        }
    };

    var source = AuthStatusSource{};
    var api_server = ApiHttpServer.init(alloc, .{ .shard_db_adapter = Fake.shardDb() }, source.iface(), Fake.reads(), null);
    defer api_server.deinit();
    var e2e_server: HttpxE2eServer = undefined;
    try e2e_server.init(alloc, &api_server);
    defer e2e_server.deinit();
    var client_io = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer client_io.deinit();
    var client = httpx.Client.initWithConfig(alloc, client_io.io(), .{ .keep_alive = false });
    defer client.deinit();
    const base_url = try e2e_server.baseUrl(alloc);
    defer alloc.free(base_url);

    const lookup_url = try std.fmt.allocPrint(alloc, "{s}/internal/v1/groups/7/tables/docs/documents/doc:a?fields=title&read_consistency=stale", .{base_url});
    defer alloc.free(lookup_url);
    var lookup = try getWithRetry(&client, client_io.io(), lookup_url, null, 20);
    defer lookup.deinit();
    try std.testing.expectEqual(@as(u16, 200), lookup.status.code);
    try std.testing.expectEqualStrings("{\"title\":\"alpha\"}", lookup.body.?);
    try std.testing.expectEqualStrings("42", lookup.header("X-Antfly-Version").?);

    const median_url = try std.fmt.allocPrint(alloc, "{s}/internal/v1/groups/7/db/median-key", .{base_url});
    defer alloc.free(median_url);
    var median = try getWithRetry(&client, client_io.io(), median_url, null, 20);
    defer median.deinit();
    try std.testing.expectEqual(@as(u16, 200), median.status.code);
    try std.testing.expectEqualStrings("{\"median_key\":\"doc:m\"}", median.body.?);

    const join_state_url = try std.fmt.allocPrint(alloc, "{s}/internal/v1/groups/7/tables/docs/join-job-state", .{base_url});
    defer alloc.free(join_state_url);
    const headers = [_][2][]const u8{.{ "content-type", "application/json" }};
    var invalid_join_state = try requestWithRetry(&client, client_io.io(), .POST, join_state_url, "{}", &headers, 20);
    defer invalid_join_state.deinit();
    try std.testing.expectEqual(@as(u16, 400), invalid_join_state.status.code);
    try std.testing.expectEqualStrings("invalid join job state request", invalid_join_state.body.?);

    inline for (.{
        .{ "join-finalize", "invalid join finalize request" },
        .{ "join-rows", "invalid join rows request" },
        .{ "join-unmatched", "invalid join unmatched request" },
        .{ "join-partition", "invalid join partition request" },
    }) |case| {
        const url = try std.fmt.allocPrint(alloc, "{s}/internal/v1/groups/7/tables/docs/{s}", .{ base_url, case[0] });
        defer alloc.free(url);
        var response = try requestWithRetry(&client, client_io.io(), .POST, url, "{}", &headers, 20);
        defer response.deinit();
        try std.testing.expectEqual(@as(u16, 400), response.status.code);
        try std.testing.expectEqualStrings(case[1], response.body.?);
    }

    const corrupt_url = try std.fmt.allocPrint(alloc, "{s}/internal/v1/tables/docs/corrupt-embedding-artifact", .{base_url});
    defer alloc.free(corrupt_url);
    var invalid_corrupt = try requestWithRetry(&client, client_io.io(), .POST, corrupt_url, "{}", &headers, 20);
    defer invalid_corrupt.deinit();
    try std.testing.expectEqual(@as(u16, 400), invalid_corrupt.status.code);
    try std.testing.expectEqualStrings("invalid corrupt embedding artifact request", invalid_corrupt.body.?);

    inline for (.{
        .{ "shard-ops/observe-split", "invalid split transition request" },
        .{ "shard-ops/observe-merge", "invalid merge transition request" },
        .{ "shard-ops/execute", "invalid transition action request" },
    }) |case| {
        const url = try std.fmt.allocPrint(alloc, "{s}/internal/v1/groups/7/{s}", .{ base_url, case[0] });
        defer alloc.free(url);
        var response = try requestWithRetry(&client, client_io.io(), .POST, url, "{}", &headers, 20);
        defer response.deinit();
        try std.testing.expectEqual(@as(u16, 400), response.status.code);
        try std.testing.expectEqualStrings(case[1], response.body.?);
    }

    const batch_url = try std.fmt.allocPrint(alloc, "{s}/internal/v1/groups/7/tables/docs/batch", .{base_url});
    defer alloc.free(batch_url);
    var invalid_batch = try requestWithRetry(&client, client_io.io(), .POST, batch_url, "[]", &headers, 20);
    defer invalid_batch.deinit();
    try std.testing.expectEqual(@as(u16, 400), invalid_batch.status.code);
    try std.testing.expectEqualStrings("invalid batch request", invalid_batch.body.?);

    const routed_batch_url = try std.fmt.allocPrint(alloc, "{s}/internal/v1/groups/7/tables/docs/batch-routed-v1", .{base_url});
    defer alloc.free(routed_batch_url);
    var missing_forwarding = try requestWithRetry(&client, client_io.io(), .POST, routed_batch_url, "{}", &headers, 20);
    defer missing_forwarding.deinit();
    try std.testing.expectEqual(@as(u16, 400), missing_forwarding.status.code);
    try std.testing.expectEqualStrings("missing raft batch forwarding headers", missing_forwarding.body.?);

    const scan_url = try std.fmt.allocPrint(alloc, "{s}/internal/v1/groups/7/tables/docs/documents", .{base_url});
    defer alloc.free(scan_url);
    var invalid_scan = try requestWithRetry(&client, client_io.io(), .POST, scan_url, "[]", &headers, 20);
    defer invalid_scan.deinit();
    try std.testing.expectEqual(@as(u16, 400), invalid_scan.status.code);
    try std.testing.expectEqualStrings("invalid scan request", invalid_scan.body.?);

    inline for (.{
        .{ "query", "InvalidQueryRequest" },
        .{ "query-preflight", "InvalidQueryRequest" },
        .{ "vector-worker", "invalid vector worker request" },
    }) |case| {
        const url = try std.fmt.allocPrint(alloc, "{s}/internal/v1/groups/7/tables/docs/{s}", .{ base_url, case[0] });
        defer alloc.free(url);
        var response = try requestWithRetry(&client, client_io.io(), .POST, url, "[]", &headers, 20);
        defer response.deinit();
        try std.testing.expectEqual(@as(u16, 400), response.status.code);
        try std.testing.expectEqualStrings(case[1], response.body.?);
    }

    const retrieval_url = try std.fmt.allocPrint(alloc, "{s}/agents/retrieval", .{base_url});
    defer alloc.free(retrieval_url);
    var invalid_retrieval = try requestWithRetry(&client, client_io.io(), .POST, retrieval_url, "[]", &headers, 20);
    defer invalid_retrieval.deinit();
    try std.testing.expectEqual(@as(u16, 400), invalid_retrieval.status.code);
    try std.testing.expectEqualStrings("invalid retrieval agent request", invalid_retrieval.body.?);

    inline for (.{
        .{ "graph-expand", "invalid graph expand request" },
        .{ "graph-hydrate", "invalid graph hydrate request" },
        .{ "graph-edges", "invalid graph edges request" },
    }) |case| {
        const url = try std.fmt.allocPrint(alloc, "{s}/internal/v1/groups/7/tables/docs/{s}", .{ base_url, case[0] });
        defer alloc.free(url);
        var response = try requestWithRetry(&client, client_io.io(), .POST, url, "{}", &headers, 20);
        defer response.deinit();
        try std.testing.expectEqual(@as(u16, 400), response.status.code);
        try std.testing.expectEqualStrings(case[1], response.body.?);
    }

    inline for (.{
        .{ "document_units_v1:placement", "invalid document artifact placement request" },
        .{ "document_units_v1:child-range-batch", "invalid document artifact child range batch request" },
    }) |case| {
        const url = try std.fmt.allocPrint(alloc, "{s}/internal/v1/groups/7/tables/docs/documents/doc-a/artifacts/{s}", .{ base_url, case[0] });
        defer alloc.free(url);
        var response = try requestWithRetry(&client, client_io.io(), .POST, url, "[]", &headers, 20);
        defer response.deinit();
        try std.testing.expectEqual(@as(u16, 400), response.status.code);
        try std.testing.expectEqualStrings(case[1], response.body.?);
    }

    const table_reprocess_url = try std.fmt.allocPrint(alloc, "{s}/internal/v1/groups/7/tables/docs/artifacts/document_units_v1/reprocess", .{base_url});
    defer alloc.free(table_reprocess_url);
    var invalid_table_reprocess = try requestWithRetry(&client, client_io.io(), .POST, table_reprocess_url, "[]", &headers, 20);
    defer invalid_table_reprocess.deinit();
    try std.testing.expectEqual(@as(u16, 400), invalid_table_reprocess.status.code);
    try std.testing.expectEqualStrings("invalid document artifact reprocess request", invalid_table_reprocess.body.?);

    const repair_list_url = try std.fmt.allocPrint(alloc, "{s}/internal/v1/groups/7/tables/docs/repair/issues", .{base_url});
    defer alloc.free(repair_list_url);
    var invalid_repair_list = try requestWithRetry(&client, client_io.io(), .POST, repair_list_url, "[]", &headers, 20);
    defer invalid_repair_list.deinit();
    try std.testing.expectEqual(@as(u16, 400), invalid_repair_list.status.code);
    try std.testing.expectEqualStrings("invalid artifact repair list request", invalid_repair_list.body.?);

    const repair_run_url = try std.fmt.allocPrint(alloc, "{s}/internal/v1/groups/7/tables/docs/repair/run", .{base_url});
    defer alloc.free(repair_run_url);
    var invalid_repair_run = try requestWithRetry(&client, client_io.io(), .POST, repair_run_url, "[]", &headers, 20);
    defer invalid_repair_run.deinit();
    try std.testing.expectEqual(@as(u16, 400), invalid_repair_run.status.code);
    try std.testing.expectEqualStrings("invalid artifact repair request", invalid_repair_run.body.?);

    inline for (.{ "txn-begin", "txn-prepare", "txn-resolve", "txn-status", "txn-acknowledge" }) |suffix| {
        const url = try std.fmt.allocPrint(alloc, "{s}/internal/v1/groups/7/tables/docs/{s}", .{ base_url, suffix });
        defer alloc.free(url);
        var response = try requestWithRetry(&client, client_io.io(), .POST, url, "{}", &headers, 20);
        defer response.deinit();
        try std.testing.expectEqual(@as(u16, 400), response.status.code);
        try std.testing.expectEqualStrings("invalid transaction request", response.body.?);
    }
}

test "httpx ARD routes call typed contextual operations directly" {
    const alloc = std.testing.allocator;
    var source = AuthStatusSource{};
    var api_server = ApiHttpServer.init(alloc, .{
        .experimental = true,
        .ard_public_catalog_enabled = true,
        .ard_publisher_domain = "tenant.example.com",
        .ard_display_name = "Tenant Antfly",
    }, source.iface(), null, null);
    defer api_server.deinit();
    var e2e_server: HttpxE2eServer = undefined;
    try e2e_server.init(alloc, &api_server);
    defer e2e_server.deinit();
    var client_io = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer client_io.deinit();
    var client = httpx.Client.initWithConfig(alloc, client_io.io(), .{ .keep_alive = false });
    defer client.deinit();
    const base_url = try e2e_server.baseUrl(alloc);
    defer alloc.free(base_url);

    const registry_url = try std.fmt.allocPrint(alloc, "{s}/ard/v1", .{base_url});
    defer alloc.free(registry_url);
    var registry = try getWithRetry(&client, client_io.io(), registry_url, null, 20);
    defer registry.deinit();
    try std.testing.expectEqual(@as(u16, 200), registry.status.code);
    try std.testing.expectEqualStrings("application/json", registry.header("content-type").?);
    try std.testing.expect(std.mem.indexOf(u8, registry.body.?, "\"catalog\"") != null);

    const public_url = try std.fmt.allocPrint(alloc, "{s}/.well-known/ai-catalog.json", .{base_url});
    defer alloc.free(public_url);
    var public_catalog = try getWithRetry(&client, client_io.io(), public_url, null, 20);
    defer public_catalog.deinit();
    try std.testing.expectEqual(@as(u16, 200), public_catalog.status.code);
    try std.testing.expectEqualStrings("*", public_catalog.header("Access-Control-Allow-Origin").?);
    try std.testing.expect(std.mem.indexOf(u8, public_catalog.body.?, "application/ai-registry+json") != null);

    const card_url = try std.fmt.allocPrint(alloc, "{s}/.well-known/agent-card.json", .{base_url});
    defer alloc.free(card_url);
    var card = try getWithRetry(&client, client_io.io(), card_url, null, 20);
    defer card.deinit();
    try std.testing.expectEqual(@as(u16, 200), card.status.code);
    try std.testing.expect(std.mem.indexOf(u8, card.body.?, "\"skills\"") != null);

    const a2a_url = try std.fmt.allocPrint(alloc, "{s}/a2a", .{base_url});
    defer alloc.free(a2a_url);
    const json_headers = [_][2][]const u8{.{ "content-type", "application/json" }};
    var a2a_response = try requestWithRetry(&client, client_io.io(), .POST, a2a_url, "{}", &json_headers, 20);
    defer a2a_response.deinit();
    try std.testing.expectEqual(@as(u16, 200), a2a_response.status.code);
    try std.testing.expectEqualStrings("application/json", a2a_response.header("content-type").?);
}

test "httpx storage maintenance routes call typed operations directly" {
    const alloc = std.testing.allocator;
    const maintenance_mod = @import("../storage/maintenance.zig");
    const FakeMaintenance = struct {
        fn source(self: *@This()) maintenance_mod.Source {
            return .{ .ptr = self, .vtable = &.{ .status = status, .run = run } };
        }

        fn status(_: *anyopaque) maintenance_mod.Status {
            return .{
                .engine = "lite",
                .format = "aflite",
                .fsync = true,
                .maintenance = .{
                    .check = true,
                    .compact = true,
                    .vacuum = true,
                    .online = true,
                },
            };
        }

        fn run(
            _: *anyopaque,
            maintenance_operation: maintenance_mod.Operation,
            cancel: *const maintenance_mod.CancelToken,
        ) anyerror!maintenance_mod.Result {
            try cancel.check();
            return switch (maintenance_operation) {
                .check => .{ .valid = true, .file_size = 4096 },
                .compact, .vacuum => .{
                    .before_size = 8192,
                    .after_size = 4096,
                    .reclaimed_bytes = 4096,
                },
            };
        }
    };

    var status_source = AuthStatusSource{};
    var maintenance_source = FakeMaintenance{};
    var backend_runtime = try db_mod.background_runtime.BackendRuntimeHandle.init(alloc, .{ .backend = .io_threaded });
    defer backend_runtime.deinit();
    var coordinator = try maintenance_mod.Coordinator.init(alloc, maintenance_source.source(), backend_runtime.ptr());
    defer coordinator.deinit();
    var api_server = ApiHttpServer.init(alloc, .{
        .storage_maintenance = &coordinator,
        .admin_bearer_token = "maintenance-secret",
    }, status_source.iface(), null, null);
    defer api_server.deinit();

    var e2e_server: HttpxE2eServer = undefined;
    try e2e_server.init(alloc, &api_server);
    defer e2e_server.deinit();

    var client_io = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer client_io.deinit();
    var client = httpx.Client.initWithConfig(alloc, client_io.io(), .{ .keep_alive = false });
    defer client.deinit();
    const base_url = try e2e_server.baseUrl(alloc);
    defer alloc.free(base_url);

    inline for (.{ "/healthz", "/readyz" }) |probe_path| {
        const probe_url = try std.fmt.allocPrint(alloc, "{s}{s}", .{ base_url, probe_path });
        defer alloc.free(probe_url);
        var probe = try getWithRetry(&client, client_io.io(), probe_url, null, 20);
        defer probe.deinit();
        try std.testing.expectEqual(@as(u16, 200), probe.status.code);
    }

    const status_url = try std.fmt.allocPrint(alloc, "{s}/db/v1/status", .{base_url});
    defer alloc.free(status_url);
    var status_response = try getWithRetry(&client, client_io.io(), status_url, null, 20);
    defer status_response.deinit();
    try std.testing.expectEqual(@as(u16, 200), status_response.status.code);
    try std.testing.expect(std.mem.indexOf(u8, status_response.body.?, "\"engine\":\"lite\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, status_response.body.?, "\"vacuum\":true") != null);

    const check_url = try std.fmt.allocPrint(alloc, "{s}{s}", .{ base_url, admin_routes.maintenance_check });
    defer alloc.free(check_url);
    var unauthorized = try requestWithRetry(&client, client_io.io(), .POST, check_url, null, null, 20);
    defer unauthorized.deinit();
    try std.testing.expectEqual(@as(u16, 401), unauthorized.status.code);

    const start_headers = [_][2][]const u8{
        .{ "authorization", "Bearer maintenance-secret" },
        .{ "Idempotency-Key", "check-1" },
    };
    var started = try requestWithRetry(&client, client_io.io(), .POST, check_url, null, &start_headers, 20);
    defer started.deinit();
    try std.testing.expectEqual(@as(u16, 202), started.status.code);
    try std.testing.expect(std.mem.indexOf(u8, started.body.?, "\"operation\":\"check\"") != null);
    const MaintenanceStart = struct { job_id: u64 };
    var parsed_started = try std.json.parseFromSlice(MaintenanceStart, alloc, started.body.?, .{ .ignore_unknown_fields = true });
    defer parsed_started.deinit();
    const job_id = parsed_started.value.job_id;

    var replay = try requestWithRetry(&client, client_io.io(), .POST, check_url, null, &start_headers, 20);
    defer replay.deinit();
    try std.testing.expectEqual(@as(u16, 202), replay.status.code);
    var parsed_replay = try std.json.parseFromSlice(MaintenanceStart, alloc, replay.body.?, .{ .ignore_unknown_fields = true });
    defer parsed_replay.deinit();
    try std.testing.expectEqual(job_id, parsed_replay.value.job_id);

    var completed = false;
    for (0..1_000) |_| {
        const snapshot = coordinator.get(job_id) orelse return error.MissingMaintenanceJob;
        if (snapshot.state == .succeeded) {
            completed = true;
            break;
        }
        try client_io.io().sleep(std.Io.Duration.fromMilliseconds(1), .awake);
    }
    try std.testing.expect(completed);

    const job_url = try std.fmt.allocPrint(alloc, "{s}{s}{d}", .{ base_url, admin_routes.maintenance_jobs_prefix, job_id });
    defer alloc.free(job_url);
    const auth_headers = [_][2][]const u8{.{ "authorization", "Bearer maintenance-secret" }};
    var fetched = try getWithRetry(&client, client_io.io(), job_url, &auth_headers, 20);
    defer fetched.deinit();
    try std.testing.expectEqual(@as(u16, 200), fetched.status.code);
    try std.testing.expect(std.mem.indexOf(u8, fetched.body.?, "\"valid\":true") != null);

    var canceled = try requestWithRetry(&client, client_io.io(), .DELETE, job_url, null, &auth_headers, 20);
    defer canceled.deinit();
    try std.testing.expectEqual(@as(u16, 202), canceled.status.code);
    try std.testing.expect(std.mem.indexOf(u8, canceled.body.?, "\"state\":\"succeeded\"") != null);
}

test "httpx owned response preserves retryable JSON metadata" {
    const alloc = std.testing.allocator;
    var request = try httpx.Request.init(alloc, .GET, "http://127.0.0.1/db/v1/tables/docs/doc:a");
    defer request.deinit();
    var ctx = httpx.Context.init(alloc, undefined, &request);
    defer ctx.deinit();

    var owned = try public_table_http.storageReadTemporarilyUnavailableOwnedResponse(alloc);
    var response = try AntflyApiHandler.respondOwnedApiResponse(&ctx, &owned);
    defer response.deinit();

    try std.testing.expectEqual(@as(u16, 503), response.status.code);
    try std.testing.expectEqualStrings("application/json", response.headers.get("content-type").?);
    try std.testing.expectEqualStrings("1", response.headers.get("Retry-After").?);
    try std.testing.expectEqualStrings(public_table_http.storage_read_temporarily_unavailable_body, response.body.?);
}

test "httpx query admission rejects saturated queries without blocking control routes" {
    const alloc = std.testing.allocator;
    var source = AuthStatusSource{};
    var api_server = ApiHttpServer.init(alloc, .{ .query_max_concurrent_requests = 1 }, source.iface(), null, null);
    var handler = AntflyApiHandler{
        .api_server = &api_server,
        .query_body_admission = RequestAdmission.init(1),
    };

    try std.testing.expect(api_server.tryAcquireQuery());
    defer api_server.releaseQuery();

    var query_request = try httpx.Request.init(alloc, .POST, "http://127.0.0.1/db/v1/tables/docs/query");
    defer query_request.deinit();
    query_request.body = "{}";
    var query_ctx = httpx.Context.init(alloc, undefined, &query_request);
    defer query_ctx.deinit();
    var rejected = try handler.queryTable(&query_ctx, "docs");
    defer rejected.deinit();
    try std.testing.expectEqual(@as(u16, 429), rejected.status.code);
    try std.testing.expectEqualStrings("1", rejected.headers.get("Retry-After").?);
    try std.testing.expect(rejected.headers.get("Connection") == null);
    const admission_stats = api_server.queryAdmissionStats();
    try std.testing.expectEqual(@as(usize, 1), admission_stats.in_flight);
    try std.testing.expectEqual(@as(usize, 1), admission_stats.peak_in_flight);
    try std.testing.expectEqual(@as(u64, 1), admission_stats.rejected_total);

    var scan_request = try httpx.Request.init(alloc, .POST, "http://127.0.0.1/db/v1/tables/docs/documents");
    defer scan_request.deinit();
    scan_request.body = "{}";
    var scan_ctx = httpx.Context.init(alloc, undefined, &scan_request);
    defer scan_ctx.deinit();
    var scan_rejected = try handler.scanKeys(&scan_ctx, "docs");
    defer scan_rejected.deinit();
    try std.testing.expectEqual(@as(u16, 429), scan_rejected.status.code);
    try std.testing.expectEqualStrings("query capacity exhausted", scan_rejected.body orelse "");
    try std.testing.expectEqual(@as(u64, 2), api_server.queryAdmissionStats().rejected_total);

    var h1_query_request = try httpx.Request.init(alloc, .POST, "http://127.0.0.1/db/v1/tables/docs/query");
    defer h1_query_request.deinit();
    h1_query_request.body = "{}";
    var h1_query_ctx = httpx.Context.init(alloc, std.testing.io, &h1_query_request);
    defer h1_query_ctx.deinit();
    var h1_socket = httpx.Socket{ .handle = 0, .io = std.testing.io };
    h1_query_ctx.h1_sock = &h1_socket;
    var h1_rejected = try handler.queryTable(&h1_query_ctx, "docs");
    defer h1_rejected.deinit();
    try std.testing.expectEqual(@as(u16, 429), h1_rejected.status.code);
    try std.testing.expectEqualStrings("close", h1_rejected.headers.get("Connection").?);

    // Slow H2 upload admission is independent from query execution. Reject
    // before reading the streaming body and leave the execution count intact.
    try std.testing.expect(handler.query_body_admission.tryAcquire());
    defer handler.query_body_admission.release();
    var h2_query_request = try httpx.Request.init(alloc, .POST, "http://127.0.0.1/db/v1/tables/docs/query");
    defer h2_query_request.deinit();
    var h2_query_ctx = httpx.Context.init(alloc, std.testing.io, &h2_query_request);
    defer h2_query_ctx.deinit();
    const UnusedStreamingBody = struct {
        fn readAll(_: ?*anyopaque) !?[]const u8 {
            return error.TestUnexpectedResult;
        }
    };
    h2_query_ctx.body_delegate = .{
        .ptr = null,
        .read_all = UnusedStreamingBody.readAll,
        .streaming = true,
    };
    var h2_rejected = try handler.queryTable(&h2_query_ctx, "docs");
    defer h2_rejected.deinit();
    try std.testing.expectEqual(@as(u16, 429), h2_rejected.status.code);
    try std.testing.expectEqualStrings("query body capacity exhausted", h2_rejected.body orelse "");
    try std.testing.expectEqual(@as(usize, 1), api_server.queryAdmissionStats().in_flight);
    try std.testing.expectEqual(@as(usize, 1), handler.query_body_admission.stats().in_flight);
    try std.testing.expectEqual(@as(u64, 1), handler.query_body_admission.stats().rejected_total);

    var control_request = try httpx.Request.init(alloc, .GET, "http://127.0.0.1/db/v1/status");
    defer control_request.deinit();
    var control_ctx = httpx.Context.init(alloc, undefined, &control_request);
    defer control_ctx.deinit();
    var control = try handler.getStatus(&control_ctx);
    defer control.deinit();
    try std.testing.expectEqual(@as(u16, 200), control.status.code);
}

test "httpx inference connection uses the configured shared admission owner" {
    const SharedAdmission = struct {
        gate: RequestAdmission = RequestAdmission.init(1),

        fn tryAcquire(ptr: *anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.gate.tryAcquire();
        }

        fn release(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.gate.release();
        }

        fn stats(ptr: *anyopaque) RequestAdmission.Stats {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.gate.stats();
        }
    };

    const alloc = std.testing.allocator;
    var source = AuthStatusSource{};
    var shared: SharedAdmission = .{};
    var node_config = try common_config.Config.parseFromSlice(alloc,
        \\{
        \\  "connections": {
        \\    "remote": {
        \\      "kind": "inference",
        \\      "capabilities": ["models.embed"],
        \\      "inference": { "provider": "antfly", "url": "https://inference.example.com" }
        \\    }
        \\  }
        \\}
    );
    defer node_config.deinit();
    var api_server = ApiHttpServer.init(alloc, .{
        .inference_max_concurrent_requests = 99,
        .inference_request_admission_source = .{
            .ptr = &shared,
            .try_acquire_fn = SharedAdmission.tryAcquire,
            .release_fn = SharedAdmission.release,
            .stats_fn = SharedAdmission.stats,
        },
        .node_config = &node_config,
    }, source.iface(), null, null);
    defer api_server.deinit();
    var handler = AntflyApiHandler{ .api_server = &api_server };

    try std.testing.expect(shared.gate.tryAcquire());
    defer shared.gate.release();

    var request = try httpx.Request.init(alloc, .POST, "http://127.0.0.1/connections/remote/inference/embed");
    defer request.deinit();
    request.body = "{}";
    var ctx = httpx.Context.init(alloc, std.testing.io, &request);
    defer ctx.deinit();

    var rejected = try handler.invokeInferenceConnection(&ctx, "remote", "embed");
    defer rejected.deinit();
    try std.testing.expectEqual(@as(u16, 503), rejected.status.code);
    try std.testing.expectEqualStrings("1", rejected.headers.get("Retry-After").?);
    var payload = try std.json.parseFromSlice(
        struct { reason: []const u8, retryable: bool, retry_after_ms: u32 },
        alloc,
        rejected.body.?,
        .{ .ignore_unknown_fields = true },
    );
    defer payload.deinit();
    try std.testing.expectEqualStrings("inference_admission", payload.value.reason);
    try std.testing.expect(payload.value.retryable);
    try std.testing.expectEqual(@as(u32, 1000), payload.value.retry_after_ms);
    try std.testing.expectEqual(@as(u64, 1), shared.gate.stats().rejected_total);
}

test "local inference connection admission is owned exactly once by its target" {
    const SharedTarget = struct {
        gate: RequestAdmission = RequestAdmission.init(1),

        fn tryAcquire(ptr: *anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.gate.tryAcquire();
        }

        fn release(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.gate.release();
        }

        fn stats(ptr: *anyopaque) RequestAdmission.Stats {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.gate.stats();
        }

        fn invoke(context: *const inference_connection_abi.InvokeContext) callconv(.c) inference_connection_abi.Status {
            const self: *@This() = @ptrCast(@alignCast(context.target_context));
            const alloc = context.allocator.asStd();
            if (context.cancellation.requested() or
                !std.mem.eql(u8, context.operation.slice(), "embed") or
                !std.mem.eql(u8, context.body.slice(), "{}"))
            {
                return inference_connection_abi.statusFromError(error.InvalidArgument);
            }
            if (!self.gate.tryAcquire()) {
                const body = alloc.dupe(u8, "capacity exhausted") catch
                    return inference_connection_abi.statusFromError(error.OutOfMemory);
                context.out_response.* = .{
                    .status = 503,
                    .body = .{ .ptr = body.ptr, .len = body.len },
                };
                return .ok;
            }
            defer self.gate.release();
            const body = alloc.dupe(u8, "{\"ok\":true}") catch
                return inference_connection_abi.statusFromError(error.OutOfMemory);
            context.out_response.* = .{
                .status = 200,
                .body = .{ .ptr = body.ptr, .len = body.len },
            };
            return .ok;
        }
    };

    const alloc = std.testing.allocator;
    var source = AuthStatusSource{};
    var shared: SharedTarget = .{};
    var api_server = ApiHttpServer.init(alloc, .{
        .inference_request_admission_source = .{
            .ptr = &shared,
            .try_acquire_fn = SharedTarget.tryAcquire,
            .release_fn = SharedTarget.release,
            .stats_fn = SharedTarget.stats,
        },
        .local_inference_connection_target = .{
            .capabilities = inference_connection_abi.Capability.streaming_response,
            .context = &shared,
            .invoke = SharedTarget.invoke,
        },
    }, source.iface(), null, null);
    defer api_server.deinit();
    var handler = AntflyApiHandler{ .api_server = &api_server };

    var request = try httpx.Request.init(alloc, .POST, "http://127.0.0.1/connections/local-inference/inference/embed");
    defer request.deinit();
    request.body = "{}";
    var ctx = httpx.Context.init(alloc, std.testing.io, &request);
    defer ctx.deinit();

    var response = try handler.invokeInferenceConnection(&ctx, common_config.local_inference_connection_id, "embed");
    defer response.deinit();
    try std.testing.expectEqual(@as(u16, 200), response.status.code);
    try std.testing.expectEqualStrings("{\"ok\":true}", response.body.?);
    const stats = shared.gate.stats();
    try std.testing.expectEqual(@as(usize, 0), stats.in_flight);
    try std.testing.expectEqual(@as(usize, 1), stats.peak_in_flight);
    try std.testing.expectEqual(@as(u64, 0), stats.rejected_total);
}

test "httpx inference connection requires inference write permission" {
    const Target = struct {
        invoked: bool = false,

        fn invoke(context: *const inference_connection_abi.InvokeContext) callconv(.c) inference_connection_abi.Status {
            const self: *@This() = @ptrCast(@alignCast(context.target_context));
            self.invoked = true;
            const body = context.allocator.asStd().dupe(u8, "{\"ok\":true}") catch
                return inference_connection_abi.statusFromError(error.OutOfMemory);
            context.out_response.* = .{
                .status = 200,
                .body = .{ .ptr = body.ptr, .len = body.len },
            };
            return .ok;
        }
    };

    const alloc = std.testing.allocator;
    var auth = try initTestAuthManager(alloc);
    try bindTestAuthManager(alloc, &auth);
    defer auth.manager.deinit();
    defer auth.policy_store.deinit();
    defer auth.store.deinit();

    var table_read = try usermgr.Permission.initOwned(alloc, .table, "*", .read);
    defer table_read.deinit(alloc);
    var reader = try auth.manager.createUser("reader", "reader", &.{table_read});
    defer reader.deinit(alloc);
    const authorization = try encodeBasicAuthorization(alloc, "reader", "reader");
    defer alloc.free(authorization);

    var source = AuthStatusSource{};
    var target = Target{};
    var api_server = ApiHttpServer.init(alloc, .{
        .auth_enabled = true,
        .user_manager = &auth.manager,
        .local_inference_connection_target = .{
            .capabilities = inference_connection_abi.Capability.streaming_response,
            .context = &target,
            .invoke = Target.invoke,
        },
    }, source.iface(), null, null);
    defer api_server.deinit();
    var handler = AntflyApiHandler{ .api_server = &api_server };

    var denied_request = try httpx.Request.init(alloc, .POST, "http://127.0.0.1/connections/local-inference/inference/embed");
    defer denied_request.deinit();
    try denied_request.headers.append("authorization", authorization);
    denied_request.body = "{}";
    var denied_ctx = httpx.Context.init(alloc, std.testing.io, &denied_request);
    defer denied_ctx.deinit();
    var denied = try handler.invokeInferenceConnection(&denied_ctx, common_config.local_inference_connection_id, "embed");
    defer denied.deinit();
    try std.testing.expectEqual(@as(u16, 403), denied.status.code);
    try std.testing.expect(!target.invoked);

    var inference_write = try usermgr.Permission.initOwned(alloc, .inference, "*", .write);
    defer inference_write.deinit(alloc);
    try auth.manager.addPermissionToUser("reader", inference_write);

    var allowed_request = try httpx.Request.init(alloc, .POST, "http://127.0.0.1/connections/local-inference/inference/embed");
    defer allowed_request.deinit();
    try allowed_request.headers.append("authorization", authorization);
    allowed_request.body = "{}";
    var allowed_ctx = httpx.Context.init(alloc, std.testing.io, &allowed_request);
    defer allowed_ctx.deinit();
    var allowed = try handler.invokeInferenceConnection(&allowed_ctx, common_config.local_inference_connection_id, "embed");
    defer allowed.deinit();
    try std.testing.expectEqual(@as(u16, 200), allowed.status.code);
    try std.testing.expect(target.invoked);
}

test "httpx inference connection propagates failures after stream commit" {
    const Target = struct {
        fn invoke(context: *const inference_connection_abi.InvokeContext) callconv(.c) inference_connection_abi.Status {
            const stream = context.stream;
            if (stream.start.?(stream.context, 200) != .ok or
                stream.write.?(stream.context, inference_connection_abi.Bytes.init("data: partial\n\n")) != .ok)
            {
                return inference_connection_abi.statusFromError(error.Unavailable);
            }
            return inference_connection_abi.statusFromError(error.Timeout);
        }
    };
    const Stream = struct {
        started: bool = false,
        bytes: [32]u8 = undefined,
        len: usize = 0,

        fn start(raw: ?*anyopaque, status: u16) !void {
            const self: *@This() = @ptrCast(@alignCast(raw orelse return error.InvalidArgument));
            self.started = status == 200;
        }

        fn write(raw: ?*anyopaque, bytes: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(raw orelse return error.InvalidArgument));
            if (bytes.len > self.bytes.len - self.len) return error.NoSpaceLeft;
            @memcpy(self.bytes[self.len..][0..bytes.len], bytes);
            self.len += bytes.len;
        }

        fn close(_: ?*anyopaque) !void {}
    };

    const alloc = std.testing.allocator;
    var source = AuthStatusSource{};
    var target_context: u8 = 0;
    var api_server = ApiHttpServer.init(alloc, .{
        .local_inference_connection_target = .{
            .capabilities = inference_connection_abi.Capability.streaming_response,
            .context = &target_context,
            .invoke = Target.invoke,
        },
    }, source.iface(), null, null);
    defer api_server.deinit();
    var handler = AntflyApiHandler{ .api_server = &api_server };

    var request = try httpx.Request.init(alloc, .POST, "http://127.0.0.1/connections/local-inference/inference/generate");
    defer request.deinit();
    request.body = "{\"stream\":true}";
    var ctx = httpx.Context.init(alloc, std.testing.io, &request);
    defer ctx.deinit();
    var stream = Stream{};
    ctx.stream_delegate = .{
        .ptr = &stream,
        .start = Stream.start,
        .write = Stream.write,
        .close = Stream.close,
    };

    try std.testing.expectError(
        error.Timeout,
        handler.invokeInferenceConnection(&ctx, common_config.local_inference_connection_id, "generate"),
    );
    try std.testing.expect(stream.started);
    try std.testing.expectEqualStrings("data: partial\n\n", stream.bytes[0..stream.len]);
}

test "httpx inference connection preserves upstream retry guidance" {
    const alloc = std.testing.allocator;
    var request = try httpx.Request.init(alloc, .POST, "http://127.0.0.1/connections/remote/inference/embed");
    defer request.deinit();
    var ctx = httpx.Context.init(alloc, std.testing.io, &request);
    defer ctx.deinit();
    var result = connections_api.InvokeResult{
        .status = 503,
        .body = try alloc.dupe(u8, "{\"error\":\"busy\"}"),
        .retry_after = try alloc.dupe(u8, "4"),
        .content_type = try alloc.dupe(u8, "application/problem+json"),
    };
    defer result.deinit(alloc);

    var response = try AntflyApiHandler.inferenceInvokeResponse(&ctx, &result);
    defer response.deinit();
    try std.testing.expectEqual(@as(u16, 503), response.status.code);
    try std.testing.expectEqualStrings("4", response.headers.get("Retry-After").?);
    try std.testing.expectEqualStrings("application/problem+json", response.headers.get("Content-Type").?);
    try std.testing.expectEqualStrings("{\"error\":\"busy\"}", response.body.?);
}

test "httpx query admission releases a cancelled query slot" {
    var admission = RequestAdmission.init(1);
    try std.testing.expect(admission.tryAcquire());
    var cancellation = http_common.RequestCancellation{};
    cancellation.cancel();
    try std.testing.expect(cancellation.isCancelled());
    admission.release();
    try std.testing.expect(admission.tryAcquire());
    admission.release();
}

test "httpx write admission rejects saturated table mutations" {
    const alloc = std.testing.allocator;
    var source = AuthStatusSource{};
    var api_server = ApiHttpServer.init(alloc, .{ .write_max_concurrent_requests = 1 }, source.iface(), null, null);
    defer api_server.deinit();
    var handler = AntflyApiHandler{ .api_server = &api_server };

    try std.testing.expect(api_server.tryAcquireWrite());
    defer api_server.releaseWrite();

    var request = try httpx.Request.init(alloc, .POST, "http://127.0.0.1/db/v1/tables/docs/batch");
    defer request.deinit();
    request.body = "{\"inserts\":{\"doc:a\":{\"title\":\"alpha\"}}}";
    var ctx = httpx.Context.init(alloc, std.testing.io, &request);
    defer ctx.deinit();

    var rejected = try handler.batchWrite(&ctx, "docs");
    defer rejected.deinit();
    try std.testing.expectEqual(@as(u16, 429), rejected.status.code);
    try std.testing.expectEqualStrings("1", rejected.headers.get("Retry-After").?);
    try std.testing.expectEqualStrings("write capacity exhausted", rejected.body orelse "");
    try std.testing.expectEqual(@as(u64, 1), api_server.writeAdmissionStats().rejected_total);
}

test "httpx query admission treats zero capacity as unlimited" {
    var admission = RequestAdmission.init(0);
    for (0..64) |_| try std.testing.expect(admission.tryAcquire());
    try std.testing.expectEqual(@as(usize, 64), admission.stats().in_flight);
    for (0..64) |_| admission.release();
    try std.testing.expectEqual(@as(usize, 0), admission.stats().in_flight);
    try std.testing.expectEqual(@as(u64, 0), admission.stats().rejected_total);
}

test "httpx production path sheds 128 abandoned queries and preserves control recovery" {
    if (comptime builtin.os.tag == .windows or builtin.os.tag == .freestanding) return;
    // This test deliberately pushes four waves through a 32-connection
    // listener while other Zig test binaries may be saturating the CI host.
    // Keep the 1 ms poll interval responsive locally, but allow enough wall
    // time for every accepted socket to reach admission under bounded runner
    // scheduling before asserting exact request accounting.
    const convergence_poll_attempts = 30_000;

    const BlockingReads = struct {
        started: std.atomic.Value(u32) = .init(0),
        cancelled: std.atomic.Value(u32) = .init(0),
        release: std.atomic.Value(bool) = .init(false),

        fn source(self: *@This()) table_reads.TableReadSource {
            return .{ .ptr = self, .vtable = &vtable };
        }

        const vtable = table_reads.TableReadSource.VTable{
            .lookup = lookup,
            .scan = scan,
            .query = query,
        };

        fn lookup(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
            _: db_mod.types.LookupOptions,
            _: raft_mod.ReadConsistency,
        ) anyerror!?table_reads.LookupResponse {
            return error.UnexpectedTestCall;
        }

        fn scan(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
            _: []const u8,
            _: db_mod.types.ScanOptions,
            _: raft_mod.ReadConsistency,
        ) anyerror!?table_reads.ScanResponse {
            return error.UnexpectedTestCall;
        }

        fn query(
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            req: db_mod.types.SearchRequest,
            _: raft_mod.ReadConsistency,
        ) anyerror!?query_api.QueryResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (!std.mem.eql(u8, table_name, "docs")) return null;
            _ = self.started.fetchAdd(1, .monotonic);
            while (!self.release.load(.acquire)) {
                if (req.cancellation) |signal| {
                    if (signal.isCancelled()) {
                        _ = self.cancelled.fetchAdd(1, .monotonic);
                        return error.Cancelled;
                    }
                }
                var delay = std.posix.timespec{ .sec = 0, .nsec = std.time.ns_per_ms };
                _ = std.posix.system.nanosleep(&delay, &delay);
            }
            return .{ .json = try alloc.dupe(u8, "{\"responses\":[]}") };
        }
    };

    const alloc = std.testing.allocator;
    var reads = BlockingReads{};
    defer reads.release.store(true, .release);
    var status_source = AuthStatusSource{};
    var api_server = ApiHttpServer.init(alloc, .{}, status_source.iface(), reads.source(), null);

    var e2e_server: HttpxE2eServer = undefined;
    e2e_server.initWithLimits(alloc, &api_server, 8, 32) catch |err| switch (err) {
        error.Unexpected => return error.SkipZigTest,
        else => return err,
    };
    defer e2e_server.deinit();

    const address = e2e_server.server.boundAddress() orelse return error.AddressNotAvailable;
    const client_io = std.Io.Threaded.global_single_threaded.io();
    var clients = [_]?httpx.Socket{null} ** 128;
    defer for (&clients) |*slot| {
        if (slot.*) |*client| client.close();
        slot.* = null;
    };

    const request =
        "POST /db/v1/tables/docs/query HTTP/1.1\r\n" ++
        "Host: localhost\r\n" ++
        "Content-Type: application/json\r\n" ++
        "Content-Length: 11\r\n\r\n" ++
        "{\"limit\":1}";
    for (&clients) |*slot| {
        var client = try httpx.Socket.connect(address, client_io);
        errdefer client.close();
        try client.sendAll(request);
        slot.* = client;
    }

    for (0..convergence_poll_attempts) |_| {
        const admission = api_server.queryAdmissionStats();
        if (admission.in_flight == 8 and admission.rejected_total == 120) break;
        var delay = std.posix.timespec{ .sec = 0, .nsec = std.time.ns_per_ms };
        _ = std.posix.system.nanosleep(&delay, &delay);
    }
    const saturated = api_server.queryAdmissionStats();
    try std.testing.expectEqual(@as(usize, 8), saturated.in_flight);
    try std.testing.expectEqual(@as(u64, 120), saturated.rejected_total);
    try std.testing.expectEqual(@as(u32, 8), reads.started.load(.acquire));
    try std.testing.expect(e2e_server.server.runtimeStats().active_connections <= 32);
    // Cancellation observation is universal, so rejected requests may still
    // be unwinding briefly after admission reaches its steady state. Wait
    // until only the eight deliberately blocked requests remain.
    for (0..convergence_poll_attempts) |_| {
        if (e2e_server.server.httpRuntimeStats().active_h1_cancellation_observers == 8) break;
        var delay = std.posix.timespec{ .sec = 0, .nsec = std.time.ns_per_ms };
        _ = std.posix.system.nanosleep(&delay, &delay);
    }
    try std.testing.expectEqual(@as(usize, 8), e2e_server.server.httpRuntimeStats().active_h1_cancellation_observers);

    // Rejected keep-alive clients must not retain all connection permits. The
    // real status route remains reachable while every expensive slot is held.
    var control_io = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer control_io.deinit();
    var control_client = httpx.Client.initWithConfig(alloc, control_io.io(), .{ .keep_alive = false });
    defer control_client.deinit();
    const base_url = try e2e_server.baseUrl(alloc);
    defer alloc.free(base_url);
    const status_url = try std.fmt.allocPrint(alloc, "{s}/db/v1/status", .{base_url});
    defer alloc.free(status_url);
    var status = try getWithRetry(&control_client, control_io.io(), status_url, null, 20);
    defer status.deinit();
    try std.testing.expectEqual(@as(u16, 200), status.status.code);

    // Simulate abortive client disconnects. An orderly FIN is not an HTTP/1
    // cancellation signal, so use SO_LINGER(0) to produce a hard reset that
    // the transport observer may safely propagate.
    for (&clients) |*slot| {
        if (slot.*) |*client| {
            var linger = std.posix.linger{ .onoff = 1, .linger = 0 };
            std.posix.setsockopt(
                client.handle,
                std.posix.SOL.SOCKET,
                std.posix.SO.LINGER,
                std.mem.asBytes(&linger),
            ) catch {};
            client.close();
        }
        slot.* = null;
    }
    for (0..convergence_poll_attempts) |_| {
        const admission = api_server.queryAdmissionStats();
        const runtime = e2e_server.server.httpRuntimeStats();
        if (admission.in_flight == 0 and runtime.active_h1_cancellation_observers == 0 and reads.cancelled.load(.acquire) == 8) break;
        var delay = std.posix.timespec{ .sec = 0, .nsec = std.time.ns_per_ms };
        _ = std.posix.system.nanosleep(&delay, &delay);
    }
    try std.testing.expectEqual(@as(usize, 0), api_server.queryAdmissionStats().in_flight);
    try std.testing.expectEqual(@as(usize, 0), e2e_server.server.httpRuntimeStats().active_h1_cancellation_observers);
    try std.testing.expectEqual(@as(u32, 8), reads.cancelled.load(.acquire));

    reads.release.store(true, .release);
    const query_url = try std.fmt.allocPrint(alloc, "{s}/db/v1/tables/docs/query", .{base_url});
    defer alloc.free(query_url);
    var recovered = try requestWithRetry(&control_client, control_io.io(), .POST, query_url, "{\"limit\":1}", null, 20);
    defer recovered.deinit();
    try std.testing.expectEqual(@as(u16, 200), recovered.status.code);
}

test "httpx antfly routes require auth and enforce admin middleware" {
    const alloc = std.testing.allocator;

    var auth = try initTestAuthManager(alloc);
    try bindTestAuthManager(alloc, &auth);
    defer auth.manager.deinit();
    defer auth.policy_store.deinit();
    defer auth.store.deinit();

    var admin_permission = [_]usermgr.Permission{
        try usermgr.Permission.initOwned(alloc, .@"*", "*", .admin),
    };
    defer admin_permission[0].deinit(alloc);
    var admin = try auth.manager.createUser("admin", "admin", &admin_permission);
    defer admin.deinit(alloc);

    var read_permission = [_]usermgr.Permission{
        try usermgr.Permission.initOwned(alloc, .table, "*", .read),
    };
    defer read_permission[0].deinit(alloc);
    var reader = try auth.manager.createUser("reader", "reader", &read_permission);
    defer reader.deinit(alloc);

    const store_path = try std.fmt.allocPrint(alloc, ".zig-cache/test-httpx-handler-secrets-{d}.json", .{platform_time.monotonicNs()});
    defer alloc.free(store_path);
    var cleanup_io = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer cleanup_io.deinit();
    defer std.Io.Dir.cwd().deleteFile(cleanup_io.io(), store_path) catch {};

    var secret_store = try common_secrets.FileStore.init(alloc, store_path);
    defer secret_store.deinit();

    var source = AuthStatusSource{};
    var api_server = ApiHttpServer.init(alloc, .{
        .auth_enabled = true,
        .deployment_mode = .standalone,
        .secret_store = &secret_store,
        .user_manager = &auth.manager,
    }, source.iface(), null, null);
    defer api_server.deinit();

    var e2e_server: HttpxE2eServer = undefined;
    try e2e_server.init(alloc, &api_server);
    defer e2e_server.deinit();

    var client_io = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer client_io.deinit();
    var client = httpx.Client.initWithConfig(alloc, client_io.io(), .{ .keep_alive = false });
    defer client.deinit();

    const base_url = try e2e_server.baseUrl(alloc);
    defer alloc.free(base_url);

    inline for (.{ "/healthz", "/readyz" }) |probe_path| {
        const probe_url = try std.fmt.allocPrint(alloc, "{s}{s}", .{ base_url, probe_path });
        defer alloc.free(probe_url);
        var probe = try getWithRetry(&client, client_io.io(), probe_url, null, 20);
        defer probe.deinit();
        try std.testing.expectEqual(@as(u16, 200), probe.status.code);
    }

    const status_url = try std.fmt.allocPrint(alloc, "{s}/db/v1/status", .{base_url});
    defer alloc.free(status_url);
    var unauthorized = try getWithRetry(&client, client_io.io(), status_url, null, 20);
    defer unauthorized.deinit();
    try std.testing.expectEqual(@as(u16, 401), unauthorized.status.code);
    try std.testing.expectEqualStrings("application/json", unauthorized.contentType().?);
    try std.testing.expectEqualStrings("Basic realm=\"antfly\"", unauthorized.header("WWW-Authenticate").?);
    var unauthorized_body = try std.json.parseFromSlice(struct { @"error": []const u8 }, alloc, unauthorized.body.?, .{});
    defer unauthorized_body.deinit();
    try std.testing.expectEqualStrings("unauthorized", unauthorized_body.value.@"error");

    const reader_auth = try encodeBasicAuthorization(alloc, "reader", "reader");
    defer alloc.free(reader_auth);
    const secrets_url = try std.fmt.allocPrint(alloc, "{s}/db/v1/secrets", .{base_url});
    defer alloc.free(secrets_url);
    const reader_headers = [_][2][]const u8{.{ "authorization", reader_auth }};
    var forbidden = try getWithRetry(&client, client_io.io(), secrets_url, &reader_headers, 20);
    defer forbidden.deinit();
    try std.testing.expectEqual(@as(u16, 403), forbidden.status.code);
    try std.testing.expectEqualStrings("application/json", forbidden.contentType().?);
    try std.testing.expectEqualStrings("{\"error\":\"forbidden\"}", forbidden.body.?);

    const batch_url = try std.fmt.allocPrint(alloc, "{s}/db/v1/tables/docs/batch", .{base_url});
    defer alloc.free(batch_url);
    const batch_headers = [_][2][]const u8{
        .{ "authorization", reader_auth },
        .{ "content-type", "application/json" },
    };
    for (0..100) |_| {
        var denied_batch = try requestWithRetry(
            &client,
            client_io.io(),
            .POST,
            batch_url,
            "{\"inserts\":{\"doc-2\":{\"title\":\"blocked\"}}}",
            &batch_headers,
            20,
        );
        defer denied_batch.deinit();
        try std.testing.expectEqual(@as(u16, 403), denied_batch.status.code);
        try std.testing.expectEqualStrings("application/json", denied_batch.contentType().?);
        try std.testing.expectEqualStrings("{\"error\":\"forbidden\"}", denied_batch.body.?);
    }

    const admin_auth = try encodeBasicAuthorization(alloc, "admin", "admin");
    defer alloc.free(admin_auth);
    const me_url = try std.fmt.allocPrint(alloc, "{s}/auth/v1/me", .{base_url});
    defer alloc.free(me_url);
    const admin_headers = [_][2][]const u8{.{ "authorization", admin_auth }};
    var me_resp = try getWithRetry(&client, client_io.io(), me_url, &admin_headers, 20);
    defer me_resp.deinit();
    try std.testing.expectEqual(@as(u16, 200), me_resp.status.code);
    try std.testing.expectEqualStrings("application/json", me_resp.contentType().?);
    var me_body = try std.json.parseFromSlice(struct { username: []const u8 }, alloc, me_resp.body.?, .{ .ignore_unknown_fields = true });
    defer me_body.deinit();
    try std.testing.expectEqualStrings("admin", me_body.value.username);
}

test "httpx antfly lookup route preserves projection and headers" {
    const LookupResponse = struct {
        title: []const u8,
    };
    const alloc = std.testing.allocator;
    const db_path = try std.fmt.allocPrint(alloc, "/tmp/antfly-httpx-handler-lookup-{d}", .{platform_time.monotonicNs()});
    defer alloc.free(db_path);

    var fs_io = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer fs_io.deinit();
    std.Io.Dir.cwd().deleteTree(fs_io.io(), db_path) catch {};

    var db = try db_mod.DB.open(alloc, db_path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(fs_io.io(), db_path) catch {};
    }
    try db.batch(.{
        .writes = &.{
            .{
                .key = "doc:a",
                .value = "{\"title\":\"alpha\",\"body\":\"hello\"}",
            },
        },
        .timestamp_ns = 4321,
    });

    var table_source = table_reads.BoundTableReadSource.init("docs", 77, &db, raft_mod.read_gate.noopReadableLeaseRequester());
    var source = LookupStatusSource{};
    var api_server = ApiHttpServer.init(alloc, .{}, source.iface(), table_source.source(), null);

    var e2e_server: HttpxE2eServer = undefined;
    try e2e_server.init(alloc, &api_server);
    defer e2e_server.deinit();

    var client_io = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer client_io.deinit();
    var client = httpx.Client.initWithConfig(alloc, client_io.io(), .{ .keep_alive = false });
    defer client.deinit();

    const base_url = try e2e_server.baseUrl(alloc);
    defer alloc.free(base_url);
    const lookup_url = try std.fmt.allocPrint(alloc, "{s}/db/v1/tables/docs/documents/doc:a?fields=title", .{base_url});
    defer alloc.free(lookup_url);

    var resp = try getWithRetry(&client, client_io.io(), lookup_url, null, 20);
    defer resp.deinit();
    try std.testing.expectEqual(@as(u16, 200), resp.status.code);
    try std.testing.expectEqualStrings("application/json", resp.contentType().?);
    try std.testing.expectEqualStrings("4321", resp.header("X-Antfly-Version").?);

    var parsed = try std.json.parseFromSlice(LookupResponse, alloc, resp.body.?, .{});
    defer parsed.deinit();
    try std.testing.expectEqualStrings("alpha", parsed.value.title);
}

test "httpx antfly reads map missing table errors to not found" {
    const MissingTableReads = struct {
        fn source() table_reads.TableReadSource {
            return .{ .ptr = undefined, .vtable = &vtable };
        }

        const vtable = table_reads.TableReadSource.VTable{
            .lookup = lookup,
            .scan = scan,
            .query = query,
        };

        fn lookup(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
            _: db_mod.types.LookupOptions,
            _: raft_mod.ReadConsistency,
        ) anyerror!?table_reads.LookupResponse {
            return error.TableNotFound;
        }

        fn scan(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
            _: []const u8,
            _: db_mod.types.ScanOptions,
            _: raft_mod.ReadConsistency,
        ) anyerror!?table_reads.ScanResponse {
            return error.TableNotFound;
        }

        fn query(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: db_mod.types.SearchRequest,
            _: raft_mod.ReadConsistency,
        ) anyerror!?query_api.QueryResponse {
            return error.UnexpectedTestCall;
        }
    };

    const alloc = std.testing.allocator;
    var status_source = LookupStatusSource{};
    var api_server = ApiHttpServer.init(alloc, .{}, status_source.iface(), MissingTableReads.source(), null);
    var handler = AntflyApiHandler{ .api_server = &api_server };

    var request = try httpx.Request.init(alloc, .GET, "http://127.0.0.1/db/v1/tables/docs/documents/doc:a");
    defer request.deinit();
    var ctx = httpx.Context.init(alloc, undefined, &request);
    defer ctx.deinit();

    var response = try handler.lookupKey(&ctx, "docs", "doc:a", .{});
    defer response.deinit();
    try std.testing.expectEqual(@as(u16, 404), response.status.code);
    try std.testing.expectEqualStrings("text/plain; charset=utf-8", response.contentType().?);
    try std.testing.expectEqualStrings("not found", response.body.?);

    var scan_request = try httpx.Request.init(alloc, .POST, "http://127.0.0.1/db/v1/tables/docs/documents");
    defer scan_request.deinit();
    var scan_ctx = httpx.Context.init(alloc, undefined, &scan_request);
    defer scan_ctx.deinit();

    var scan_response = try handler.scanKeys(&scan_ctx, "docs");
    defer scan_response.deinit();
    try std.testing.expectEqual(@as(u16, 404), scan_response.status.code);
    try std.testing.expectEqualStrings("text/plain; charset=utf-8", scan_response.contentType().?);
    try std.testing.expectEqualStrings("not found", scan_response.body.?);
}

test "httpx antfly scan honors optional body and documented bad requests" {
    const alloc = std.testing.allocator;
    const db_path = try std.fmt.allocPrint(alloc, "/tmp/antfly-httpx-handler-scan-{d}", .{platform_time.monotonicNs()});
    defer alloc.free(db_path);

    var fs_io = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer fs_io.deinit();
    std.Io.Dir.cwd().deleteTree(fs_io.io(), db_path) catch {};

    var db = try db_mod.DB.open(alloc, db_path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(fs_io.io(), db_path) catch {};
    }
    try db.batch(.{
        .writes = &.{
            .{
                .key = "doc:a",
                .value = "{\"title\":\"alpha\",\"body\":\"hello\"}",
            },
        },
        .timestamp_ns = 4321,
    });

    var table_source = table_reads.BoundTableReadSource.init("docs", 77, &db, raft_mod.read_gate.noopReadableLeaseRequester());
    var source = LookupStatusSource{};
    var api_server = ApiHttpServer.init(alloc, .{}, source.iface(), table_source.source(), null);

    var e2e_server: HttpxE2eServer = undefined;
    e2e_server.init(alloc, &api_server) catch |err| switch (err) {
        // Restricted test environments may forbid even loopback listeners.
        // The same test runs normally in CI and release validation.
        error.Unexpected => return error.SkipZigTest,
        else => return err,
    };
    defer e2e_server.deinit();

    var client_io = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer client_io.deinit();
    var client = httpx.Client.initWithConfig(alloc, client_io.io(), .{ .keep_alive = false });
    defer client.deinit();

    const base_url = try e2e_server.baseUrl(alloc);
    defer alloc.free(base_url);
    const scan_url = try std.fmt.allocPrint(alloc, "{s}/db/v1/tables/docs/documents", .{base_url});
    defer alloc.free(scan_url);

    var bodyless = try requestWithRetry(&client, client_io.io(), .POST, scan_url, null, null, 20);
    defer bodyless.deinit();
    try std.testing.expectEqual(@as(u16, 200), bodyless.status.code);
    try std.testing.expectEqualStrings("application/x-ndjson", bodyless.contentType().?);
    try std.testing.expect(std.mem.indexOf(u8, bodyless.body.?, "\"_id\":\"doc:a\"") != null);

    const headers = [_][2][]const u8{.{ "content-type", "application/json" }};
    var unsupported = try requestWithRetry(
        &client,
        client_io.io(),
        .POST,
        scan_url,
        "{\"filter_query\":{\"match_phrase\":\"paid receipt\",\"field\":\"body\"}}",
        &headers,
        20,
    );
    defer unsupported.deinit();
    try std.testing.expectEqual(@as(u16, 400), unsupported.status.code);
    try std.testing.expectEqualStrings("unsupported scan filter query", unsupported.body.?);
}

test "httpx antfly lookup decodes percent-encoded path keys" {
    const LookupResponse = struct {
        title: []const u8,
    };
    const alloc = std.testing.allocator;
    const db_path = try std.fmt.allocPrint(alloc, "/tmp/antfly-httpx-handler-lookup-encoded-{d}", .{platform_time.monotonicNs()});
    defer alloc.free(db_path);

    var fs_io = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer fs_io.deinit();
    std.Io.Dir.cwd().deleteTree(fs_io.io(), db_path) catch {};

    var db = try db_mod.DB.open(alloc, db_path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(fs_io.io(), db_path) catch {};
    }
    try db.batch(.{
        .writes = &.{
            .{
                .key = "docs/getting-started.md",
                .value = "{\"title\":\"alpha\",\"body\":\"hello\"}",
            },
        },
        .timestamp_ns = 4321,
    });

    var table_source = table_reads.BoundTableReadSource.init("docs", 77, &db, raft_mod.read_gate.noopReadableLeaseRequester());
    var source = LookupStatusSource{};
    var api_server = ApiHttpServer.init(alloc, .{}, source.iface(), table_source.source(), null);

    var e2e_server: HttpxE2eServer = undefined;
    try e2e_server.init(alloc, &api_server);
    defer e2e_server.deinit();

    var client_io = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer client_io.deinit();
    var client = httpx.Client.initWithConfig(alloc, client_io.io(), .{ .keep_alive = false });
    defer client.deinit();

    const base_url = try e2e_server.baseUrl(alloc);
    defer alloc.free(base_url);
    const lookup_url = try std.fmt.allocPrint(alloc, "{s}/db/v1/tables/docs/documents/docs%2Fgetting-started.md?fields=title", .{base_url});
    defer alloc.free(lookup_url);

    var resp = try getWithRetry(&client, client_io.io(), lookup_url, null, 20);
    defer resp.deinit();
    try std.testing.expectEqual(@as(u16, 200), resp.status.code);
    try std.testing.expectEqualStrings("application/json", resp.contentType().?);

    var parsed = try std.json.parseFromSlice(LookupResponse, alloc, resp.body.?, .{});
    defer parsed.deinit();
    try std.testing.expectEqualStrings("alpha", parsed.value.title);
}

test "httpx antfly schema update returns full table status after projection" {
    const alloc = std.testing.allocator;

    var source = SchemaUpdateStatusSource{};
    defer source.deinit(alloc);
    var writes = SchemaReconcileWriteSource{};
    var api_server = ApiHttpServer.init(alloc, .{}, source.iface(), null, writes.iface());

    var e2e_server: HttpxE2eServer = undefined;
    try e2e_server.init(alloc, &api_server);
    defer e2e_server.deinit();

    var client_io = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer client_io.deinit();
    var client = httpx.Client.initWithConfig(alloc, client_io.io(), .{ .keep_alive = false });
    defer client.deinit();

    const base_url = try e2e_server.baseUrl(alloc);
    defer alloc.free(base_url);
    const schema_url = try std.fmt.allocPrint(alloc, "{s}/db/v1/tables/docs/schema", .{base_url});
    defer alloc.free(schema_url);
    const schema_body = try test_contract_helpers.encodeSchemaUpdateRequest(alloc);
    defer alloc.free(schema_body);
    const headers = [_][2][]const u8{.{ "content-type", "application/json" }};

    var resp = try requestWithRetry(&client, client_io.io(), .PUT, schema_url, schema_body, &headers, 20);
    defer resp.deinit();
    try std.testing.expectEqual(@as(u16, 200), resp.status.code);
    try std.testing.expectEqualStrings("application/json", resp.contentType().?);

    var parsed = try std.json.parseFromSlice(metadata_openapi.TableStatus, alloc, resp.body.?, .{});
    defer parsed.deinit();
    try std.testing.expectEqualStrings("docs", parsed.value.name);
    try std.testing.expect(parsed.value.schema != null);
    try std.testing.expectEqual(@as(u32, 1), source.projection_wait_calls.load(.monotonic));
    try std.testing.expectEqual(@as(u32, 1), writes.reconcile_calls.load(.monotonic));
    try std.testing.expectEqual(@as(u32, 0), writes.synchronous_update_calls.load(.monotonic));
}

test "httpx global query table name comes from request body" {
    var parsed_table = try parseGlobalQueryTable(std.testing.allocator,
        \\{"table":"files","limit":5}
    );
    defer parsed_table.deinit();

    try std.testing.expectEqualStrings("files", parsed_table.table_name);
}

test "httpx query endpoints accept ndjson multiquery bodies" {
    const alloc = std.testing.allocator;
    const db_path = try std.fmt.allocPrint(alloc, "/tmp/antfly-httpx-handler-ndjson-query-{d}", .{platform_time.monotonicNs()});
    defer alloc.free(db_path);

    var fs_io = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer fs_io.deinit();
    std.Io.Dir.cwd().deleteTree(fs_io.io(), db_path) catch {};

    var db = try db_mod.DB.open(alloc, db_path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(fs_io.io(), db_path) catch {};
    }
    try db.batch(.{
        .writes = &.{
            .{
                .key = "doc:a",
                .value = "{\"title\":\"alpha\",\"body\":\"hello\"}",
            },
        },
        .timestamp_ns = 4321,
    });

    var table_source = table_reads.BoundTableReadSource.init("docs", 77, &db, raft_mod.read_gate.noopReadableLeaseRequester());
    var source = LookupStatusSource{};
    var api_server = ApiHttpServer.init(alloc, .{}, source.iface(), table_source.source(), null);

    var e2e_server: HttpxE2eServer = undefined;
    try e2e_server.init(alloc, &api_server);
    defer e2e_server.deinit();

    var client_io = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer client_io.deinit();
    var client = httpx.Client.initWithConfig(alloc, client_io.io(), .{ .keep_alive = false });
    defer client.deinit();

    const base_url = try e2e_server.baseUrl(alloc);
    defer alloc.free(base_url);
    const table_url = try std.fmt.allocPrint(alloc, "{s}/db/v1/tables/docs/query", .{base_url});
    defer alloc.free(table_url);
    const global_url = try std.fmt.allocPrint(alloc, "{s}/db/v1/query", .{base_url});
    defer alloc.free(global_url);
    const headers = [_][2][]const u8{.{ "content-type", "application/x-ndjson" }};

    const table_body =
        \\{"fields":["title"],"limit":1}
        \\{"fields":["title"],"limit":1}
    ;
    var table_resp = try requestWithRetry(&client, client_io.io(), .POST, table_url, table_body, &headers, 20);
    defer table_resp.deinit();
    try std.testing.expectEqual(@as(u16, 200), table_resp.status.code);
    var table_parsed = try std.json.parseFromSlice(std.json.Value, alloc, table_resp.body.?, .{ .ignore_unknown_fields = true });
    defer table_parsed.deinit();
    try std.testing.expectEqual(@as(usize, 2), table_parsed.value.object.get("responses").?.array.items.len);

    const global_body =
        \\{"table":"docs","fields":["title"],"limit":1}
        \\{"table":"docs","fields":["title"],"limit":1}
    ;
    var global_resp = try requestWithRetry(&client, client_io.io(), .POST, global_url, global_body, &headers, 20);
    defer global_resp.deinit();
    try std.testing.expectEqual(@as(u16, 200), global_resp.status.code);
    var global_parsed = try std.json.parseFromSlice(std.json.Value, alloc, global_resp.body.?, .{ .ignore_unknown_fields = true });
    defer global_parsed.deinit();
    try std.testing.expectEqual(@as(usize, 2), global_parsed.value.object.get("responses").?.array.items.len);
}

test "httpx antfly cluster restore preserves backup location validation" {
    const alloc = std.testing.allocator;

    var source = AuthStatusSource{};
    var api_server = ApiHttpServer.init(alloc, .{}, source.iface(), null, null);

    var e2e_server: HttpxE2eServer = undefined;
    try e2e_server.init(alloc, &api_server);
    defer e2e_server.deinit();

    var client_io = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer client_io.deinit();
    var client = httpx.Client.initWithConfig(alloc, client_io.io(), .{ .keep_alive = false });
    defer client.deinit();

    const base_url = try e2e_server.baseUrl(alloc);
    defer alloc.free(base_url);
    const restore_url = try std.fmt.allocPrint(alloc, "{s}/db/v1/restore", .{base_url});
    defer alloc.free(restore_url);
    const restore_body = "{\"backup_id\":\"snap1\",\"location\":\"ftp://bad\"}";
    const headers = [_][2][]const u8{.{ "content-type", "application/json" }};

    var resp = try requestWithRetry(&client, client_io.io(), .POST, restore_url, restore_body, &headers, 20);
    defer resp.deinit();
    try std.testing.expectEqual(@as(u16, 400), resp.status.code);
    try std.testing.expectEqualStrings("text/plain; charset=utf-8", resp.contentType().?);
    try std.testing.expectEqualStrings("unsupported backup location", resp.body.?);
}
