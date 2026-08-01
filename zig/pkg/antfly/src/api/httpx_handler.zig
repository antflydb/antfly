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
/// Each handler method extracts parameters from the httpx Context, calls the
/// appropriate business logic, and returns an httpx.Response natively or via
/// the respond() helper for methods that return http_common.HttpResponse.
const std = @import("std");
const httpx = @import("httpx");
const http_common = @import("../raft/transport/http_common.zig");
const http_route_helpers = @import("http_route_helpers.zig");
const http_server_mod = @import("http_server.zig");
const ApiHttpServer = http_server_mod.ApiHttpServer;
const AuthenticatedIdentity = http_server_mod.AuthenticatedIdentity;

const common_secrets = @import("../common/secrets.zig");
const cluster = @import("cluster.zig");
const cluster_api_http = @import("cluster_api_http.zig");
const backups_api = @import("backups.zig");
const restore_jobs = @import("restore_jobs.zig");
const public_table_http = @import("public_table_http.zig");
const tables_api = @import("tables.zig");
const table_contract = @import("table_contract.zig");
const table_reads = @import("table_reads.zig");
const table_writes = @import("table_writes.zig");
const linear_merge_api = @import("linear_merge.zig");
const transactions_api = @import("transactions.zig");
const distributed_txn = @import("distributed_txn.zig");
const retrieval_agent = @import("retrieval_agent.zig");
const generating_runtime = @import("../generating/mod.zig");
const query_api = @import("query.zig");
const query_contract = @import("query_contract.zig");
const query_builder_agent = @import("query_builder_agent.zig");
const managed_embedder = @import("../inference/managed_embedder.zig");
const metadata_table_manager = @import("../metadata/table_manager.zig");
const metadata_api = @import("../metadata/api.zig");
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

/// Bounded admission for expensive public query execution.  This lives at the
/// httpx adapter boundary so status, auth, and other control routes retain a
/// path even when query workers are saturated.
pub const QueryAdmission = struct {
    capacity: usize = 64,
    in_flight: std.atomic.Value(usize) = .init(0),

    pub fn init(capacity: usize) QueryAdmission {
        return .{ .capacity = capacity };
    }

    pub fn tryAcquire(self: *QueryAdmission) bool {
        var observed = self.in_flight.load(.acquire);
        while (observed < self.capacity) {
            if (self.in_flight.cmpxchgWeak(observed, observed + 1, .acq_rel, .acquire) == null) return true;
            observed = self.in_flight.load(.acquire);
        }
        return false;
    }

    pub fn release(self: *QueryAdmission) void {
        _ = self.in_flight.fetchSub(1, .acq_rel);
    }
};

/// Watches only for a terminal peer socket event while a query is executing.
/// It deliberately does not read the socket. This endpoint does not support
/// HTTP/1 keep-alive or pipelining, so a readable peer socket is terminal
/// (including Darwin's normal-FIN readiness) rather than request bytes owned
/// by httpx's connection loop. HTTP/2 stream reset support remains in httpx's
/// stream layer.
const PeerCancellationWatcher = struct {
    socket: *httpx.Socket,
    cancellation: *http_common.RequestCancellation,
    done: std.atomic.Value(bool) = .init(false),
    thread: ?std.Thread = null,

    fn start(self: *@This()) !void {
        if (comptime builtin.os.tag == .windows) return;
        self.thread = try std.Thread.spawn(.{}, run, .{self});
    }

    fn deinit(self: *@This()) void {
        self.done.store(true, .release);
        if (self.thread) |thread| thread.join();
    }

    fn run(self: *@This()) void {
        if (comptime builtin.os.tag == .windows) return;
        while (!self.done.load(.acquire)) {
            var fds = [_]std.posix.pollfd{.{
                .fd = self.socket.handle,
                .events = std.posix.POLL.IN | std.posix.POLL.ERR,
                .revents = 0,
            }};
            const ready = std.posix.poll(&fds, 25) catch return;
            if (ready == 0) continue;
            if (fds[0].revents & (std.posix.POLL.IN | std.posix.POLL.ERR | std.posix.POLL.HUP | std.posix.POLL.NVAL) != 0) {
                self.cancellation.cancel();
                return;
            }
        }
    }
};

pub const AntflyApiHandler = struct {
    api_server: *ApiHttpServer,
    query_admission: QueryAdmission = .{},

    // ---------------------------------------------------------------
    // Response conversion: http_common.HttpResponse -> httpx.Response
    // ---------------------------------------------------------------

    pub fn respond(ctx: *httpx.Context, resp: *http_common.HttpResponse) !httpx.Response {
        defer resp.deinit(ctx.allocator);
        _ = ctx.status(resp.status);
        if (resp.content_type) |ct| {
            try ctx.setHeader("content-type", ct);
        } else if (resp.status >= 200 and resp.status < 300) {
            // Legacy API responses without an explicit type are JSON.  Keep
            // this compatibility default at the shared adapter so every
            // successful route presents the same wire contract.
            try ctx.setHeader("content-type", "application/json");
        }
        for (resp.headers) |hdr| {
            try ctx.setHeader(hdr.name, hdr.value);
        }
        _ = ctx.response.body(resp.body);
        return ctx.response.build();
    }

    pub fn respondWithAllocator(ctx: *httpx.Context, resp: *http_common.HttpResponse, alloc: std.mem.Allocator) !httpx.Response {
        defer resp.deinit(alloc);
        _ = ctx.status(resp.status);
        if (resp.content_type) |ct| {
            try ctx.setHeader("content-type", ct);
        } else if (resp.status >= 200 and resp.status < 300) {
            try ctx.setHeader("content-type", "application/json");
        }
        for (resp.headers) |hdr| {
            try ctx.setHeader(hdr.name, hdr.value);
        }
        _ = ctx.response.body(resp.body);
        return ctx.response.build();
    }

    fn respondOwnedApiResponse(ctx: *httpx.Context, resp: anytype) !httpx.Response {
        defer resp.deinit(ctx.allocator);
        return respondApiResponseBody(ctx, resp.status, resp.body);
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
            self.result = public_table_http.handleTableBatch(
                self.alloc,
                self.table_name,
                self.body_data,
                self.api,
            ) catch |err| {
                self.err = err;
                self.done.store(true, .release);
                return;
            };
            self.done.store(true, .release);
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
            ctx.io.sleep(std.Io.Duration.fromMilliseconds(1), .awake) catch {};
        }
        _ = future.await(runtime_io);
        if (job.err) |err| return err;
        var resp = job.result.?;
        defer resp.deinit(job_alloc);
        return respondApiResponseBody(ctx, resp.status, resp.body);
    }

    pub const ConvertedHttpRequest = struct {
        value: http_common.HttpRequest,
        alloc: std.mem.Allocator,

        pub fn deinit(self: *@This()) void {
            self.alloc.free(self.value.headers);
            self.* = undefined;
        }
    };

    pub fn httpRequestFromContext(ctx: *httpx.Context, body_data_opt: ?[]const u8) !ConvertedHttpRequest {
        const method: http_common.Method = switch (ctx.request.method) {
            .GET => .GET,
            .POST => .POST,
            .PUT => .PUT,
            .DELETE => .DELETE,
            else => {
                return error.UnsupportedMethod;
            },
        };
        const headers = try ctx.allocator.alloc(http_common.RequestHeader, ctx.request.headers.entries.items.len);
        errdefer ctx.allocator.free(headers);
        for (ctx.request.headers.entries.items, 0..) |entry, i| {
            headers[i] = .{ .name = entry.name, .value = entry.value };
        }
        const body_data = body_data_opt orelse (try ctx.body()) orelse "";
        return .{
            .value = .{
                .method = method,
                .uri = ctx.request.uri.raw,
                .headers = headers,
                .authorization = ctx.header("authorization"),
                .content_type = ctx.header("content-type"),
                .body = body_data,
            },
            .alloc = ctx.allocator,
        };
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

    fn textResponse(ctx: *httpx.Context, status: u16, body: []const u8) !httpx.Response {
        _ = ctx.status(status);
        try ctx.setHeader("content-type", "text/plain");
        _ = ctx.response.body(body);
        return ctx.response.build();
    }

    fn queryOverloadedResponse(ctx: *httpx.Context) !httpx.Response {
        try ctx.setHeader("Retry-After", "1");
        return textResponse(ctx, 429, "query capacity exhausted");
    }

    fn startPeerCancellationWatcher(ctx: *httpx.Context, cancellation: *http_common.RequestCancellation, watcher: *?PeerCancellationWatcher) void {
        const socket = ctx.h1_sock orelse return;
        watcher.* = .{ .socket = socket, .cancellation = cancellation };
        if (watcher.*) |*value| value.start() catch {
            // Failure to start observation must not fail a valid query. The
            // listener still owns normal request lifetime and response I/O.
            watcher.* = null;
        };
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
            return try textResponse(ctx, 403, "forbidden");
        }
        const method = requestMethod(ctx) orelse return null;
        const required_permission = http_server_mod.requiredPermissionForRequest(ctx.allocator, method, path) catch |err| switch (err) {
            error.InvalidArgument => return try textResponse(ctx, 400, "invalid path parameter"),
            else => return err,
        };
        if (required_permission) |required| {
            defer required.deinit(ctx.allocator);
            if (!http_server_mod.permissionsAllow(authenticated.permissions, required.resource_type, required.resource, required.permission_type)) {
                return try textResponse(ctx, 403, "forbidden");
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
        const metadata_status = try self.api_server.source.status();
        var public_status = try cluster.fromMetadataStatus(alloc, metadata_status);
        defer public_status.deinit(alloc);
        public_status.auth_enabled = self.api_server.cfg.auth_enabled;
        public_status.deployment_mode = self.api_server.cfg.deployment_mode;
        public_status.storage = self.api_server.currentStorageRuntimeStatus();
        if (self.api_server.cfg.secret_store) |secret_store| {
            _ = secret_store.refreshIfChanged() catch |err| {
                std.log.warn("secret store status refresh skipped err={}", .{err});
            };
            try cluster.applySecretStoreHealth(alloc, &public_status, secret_store.healthSnapshot());
        }
        if (self.api_server.cfg.remote_content) |remote_content| {
            if (remote_content.runtimeHealth()) |health| try cluster.applyRuntimeConfigHealth(alloc, &public_status, health);
        }
        return ctx.json(public_status);
    }

    pub fn getCluster(self: *AntflyApiHandler, ctx: *httpx.Context) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const alloc = ctx.allocator;
        const metadata_status = try self.api_server.source.status();
        var public_status = try cluster.fromMetadataStatus(alloc, metadata_status);
        defer public_status.deinit(alloc);
        public_status.auth_enabled = self.api_server.cfg.auth_enabled;
        public_status.deployment_mode = self.api_server.cfg.deployment_mode;
        public_status.storage = self.api_server.currentStorageRuntimeStatus();
        if (self.api_server.cfg.secret_store) |secret_store| {
            _ = secret_store.refreshIfChanged() catch |err| {
                std.log.warn("secret store status refresh skipped err={}", .{err});
            };
            try cluster.applySecretStoreHealth(alloc, &public_status, secret_store.healthSnapshot());
        }
        if (self.api_server.cfg.remote_content) |remote_content| {
            if (remote_content.runtimeHealth()) |health| try cluster.applyRuntimeConfigHealth(alloc, &public_status, health);
        }
        var snapshot_opt = try self.api_server.source.cachedAdminSnapshot();
        if (snapshot_opt == null) {
            snapshot_opt = try self.api_server.source.adminSnapshot();
        }
        if (snapshot_opt) |*snapshot| {
            defer self.api_server.source.freeAdminSnapshot(snapshot);
            var topology = try cluster.topologyFromStatusAndSnapshot(alloc, public_status, snapshot);
            defer topology.deinit(alloc);
            return ctx.json(topology);
        }
        var topology = try cluster.topologyFromStatus(alloc, public_status);
        defer topology.deinit(alloc);
        return ctx.json(topology);
    }

    pub fn listConnections(self: *AntflyApiHandler, ctx: *httpx.Context, params: metadata_openapi.server.ListConnectionsParams) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const alloc = ctx.allocator;
        const body = try self.api_server.listConnectionsJsonAlloc(alloc, params.types, params.include, params.refresh);
        try ctx.setHeader("content-type", "application/json");
        _ = ctx.response.body(body);
        return ctx.response.build();
    }

    pub fn invokeInferenceConnection(self: *AntflyApiHandler, ctx: *httpx.Context, connection_id: []const u8, operation: []const u8) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const body = (try ctx.body()) orelse return textResponse(ctx, 400, "request body required");
        const result = self.api_server.invokeInferenceConnection(ctx.allocator, connection_id, operation, body) catch |err| switch (err) {
            error.ConnectionCapabilityMissing => return textResponse(ctx, 403, @errorName(err)),
            error.ConnectionNotFound, error.ConnectionNotInference, error.InvalidConfig, error.ConnectionURLMissing, error.InvalidConnectionURL, error.ProviderNotAntflyCompatible, error.UnsupportedInferenceOperation => return textResponse(ctx, 400, @errorName(err)),
            else => return textResponse(ctx, 502, @errorName(err)),
        };
        return jsonResponse(ctx, result.status, result.body);
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

    pub fn multiBatchWrite(self: *AntflyApiHandler, ctx: *httpx.Context) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const body_data = (try ctx.body()) orelse {
            _ = ctx.status(400);
            return ctx.text("missing body");
        };
        return try handleTableBatchOffEventLoop(ctx, self.api_server.cfg.backend_runtime, "", body_data, self.api_server.tableApi());
    }

    pub fn commitTransaction(self: *AntflyApiHandler, ctx: *httpx.Context) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const alloc = ctx.allocator;
        const source = self.api_server.table_writes orelse {
            _ = ctx.status(404);
            return ctx.text("not found");
        };
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
        if (!(try self.api_server.transactionRequestAuthorized(authenticated_identity, commit_req))) {
            _ = ctx.status(403);
            return ctx.text("forbidden");
        }

        const distributed_tables = try commit_req.distributedTables(alloc);
        defer if (distributed_tables.len > 0) alloc.free(distributed_tables);
        self.api_server.validateCommitTablesAgainstSchema(distributed_tables) catch |err| switch (err) {
            error.InvalidBatchRequest => {
                _ = ctx.status(400);
                return ctx.text("invalid transaction commit request");
            },
            else => return err,
        };
        if (try self.api_server.validateCommitReadSet(commit_req)) |conflict| {
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

        const outcome = (source.commitTransaction(alloc, distributed_tables, commit_req.sync_level) catch |err| switch (err) {
            error.InvalidBatchRequest => {
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
            .committed => {
                var arena_impl = std.heap.ArenaAllocator.init(alloc);
                defer arena_impl.deinit();
                const response = try transactions_api.buildCommitResponse(arena_impl.allocator(), "committed", null, commit_req.tables);
                return ctx.json(response);
            },
            .conflict => |conflict| {
                const enriched_conflict = try self.api_server.enrichCommitConflict(commit_req, conflict);
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
        var converted_req = httpRequestFromContext(ctx, "") catch {
            _ = ctx.status(405);
            return ctx.text("method not allowed");
        };
        defer converted_req.deinit();
        if (try self.api_server.forwardSessionRequest(txn_id, converted_req.value)) |forwarded| {
            var resp = forwarded;
            return respond(ctx, &resp);
        }
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
        var converted_req = httpRequestFromContext(ctx, body_data) catch {
            _ = ctx.status(405);
            return ctx.text("method not allowed");
        };
        defer converted_req.deinit();
        if (try self.api_server.forwardSessionRequest(txn_id, converted_req.value)) |forwarded| {
            var resp = forwarded;
            return respond(ctx, &resp);
        }
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
        var converted_req = httpRequestFromContext(ctx, body_data) catch {
            _ = ctx.status(405);
            return ctx.text("method not allowed");
        };
        defer converted_req.deinit();
        if (try self.api_server.forwardSessionRequest(txn_id, converted_req.value)) |forwarded| {
            var resp = forwarded;
            return respond(ctx, &resp);
        }
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
        var converted_req = httpRequestFromContext(ctx, body_data) catch {
            _ = ctx.status(405);
            return ctx.text("method not allowed");
        };
        defer converted_req.deinit();
        if (try self.api_server.forwardSessionRequest(txn_id, converted_req.value)) |forwarded| {
            var resp = forwarded;
            return respond(ctx, &resp);
        }
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
        var converted_req = httpRequestFromContext(ctx, body_data) catch {
            _ = ctx.status(405);
            return ctx.text("method not allowed");
        };
        defer converted_req.deinit();
        if (try self.api_server.forwardSessionRequest(txn_id, converted_req.value)) |forwarded| {
            var resp = forwarded;
            return respond(ctx, &resp);
        }
        if (!(try self.api_server.transactionSessionAccessible(txn_id, authenticated_identity))) {
            _ = ctx.status(404);
            return ctx.text("not found");
        }
        const info = (self.api_server.txn_sessions.createSavepoint(self.api_server.alloc, txn_id) catch |err| switch (err) {
            error.SessionLeaseLost => {
                _ = ctx.status(409);
                return ctx.text("session lease lost");
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
        var converted_req = httpRequestFromContext(ctx, body_data) catch {
            _ = ctx.status(405);
            return ctx.text("method not allowed");
        };
        defer converted_req.deinit();
        if (try self.api_server.forwardSessionRequest(txn_id, converted_req.value)) |forwarded| {
            var resp = forwarded;
            return respond(ctx, &resp);
        }
        if (!(try self.api_server.transactionSessionAccessible(txn_id, authenticated_identity))) {
            _ = ctx.status(404);
            return ctx.text("not found");
        }
        const info = (self.api_server.txn_sessions.rollbackToSavepoint(self.api_server.alloc, txn_id, parsed_savepoint_id) catch |err| switch (err) {
            error.SessionLeaseLost => {
                _ = ctx.status(409);
                return ctx.text("session lease lost");
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
        var converted_req = httpRequestFromContext(ctx, body_data) catch {
            _ = ctx.status(405);
            return ctx.text("method not allowed");
        };
        defer converted_req.deinit();
        if (try self.api_server.forwardSessionRequest(txn_id, converted_req.value)) |forwarded| {
            var resp = forwarded;
            return respond(ctx, &resp);
        }
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

        const distributed_tables = try commit_req.distributedTables(alloc);
        defer if (distributed_tables.len > 0) alloc.free(distributed_tables);
        self.api_server.validateCommitTablesAgainstSchema(distributed_tables) catch |err| switch (err) {
            error.InvalidBatchRequest => {
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

        const outcome = (source.commitTransactionWithId(alloc, txn_id, session.begin_timestamp, distributed_tables, session.sync_level) catch |err| switch (err) {
            error.InvalidBatchRequest => {
                _ = ctx.status(400);
                return ctx.text("invalid transaction commit request");
            },
            error.TopologyChanged => {
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
                _ = ctx.status(405);
                return ctx.text("method not allowed");
            },
            error.TableNotFound => {
                _ = ctx.status(404);
                return ctx.text("not found");
            },
            error.UnknownGroup => {
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
            else => return err,
        }) orelse {
            _ = ctx.status(404);
            return ctx.text("not found");
        };

        switch (outcome) {
            .committed => {
                _ = self.api_server.txn_sessions.remove(alloc, txn_id);
                var arena_impl = std.heap.ArenaAllocator.init(ctx.allocator);
                defer arena_impl.deinit();
                const response = try transactions_api.buildSessionCommitResponse(arena_impl.allocator(), txn_id, "committed", null, commit_req.tables);
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
        var converted_req = httpRequestFromContext(ctx, body_data) catch {
            _ = ctx.status(405);
            return ctx.text("method not allowed");
        };
        defer converted_req.deinit();
        if (try self.api_server.forwardSessionRequest(txn_id, converted_req.value)) |forwarded| {
            var resp = forwarded;
            return respond(ctx, &resp);
        }
        if (!(try self.api_server.transactionSessionAccessible(txn_id, authenticated_identity))) {
            _ = ctx.status(404);
            return ctx.text("not found");
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
        return respondWithAllocator(ctx, &resp, self.api_server.alloc);
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
        return respondWithAllocator(ctx, &resp, self.api_server.alloc);
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
        return respondWithAllocator(ctx, &resp, self.api_server.alloc);
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
        if (!self.query_admission.tryAcquire()) return queryOverloadedResponse(ctx);
        defer self.query_admission.release();
        var cancellation = http_common.RequestCancellation{ .borrowed = ctx.cancellation };
        var watcher: ?PeerCancellationWatcher = null;
        startPeerCancellationWatcher(ctx, &cancellation, &watcher);
        defer if (watcher) |*value| value.deinit();
        const body_data = (try ctx.body()) orelse {
            _ = ctx.status(400);
            return ctx.text("missing body");
        };
        if (isNdjsonContentType(ctx.header("content-type"))) {
            var resp = try self.api_server.handlePublicGlobalMultiQueryWithCancellation(
                body_data,
                authenticated_identity,
                &cancellation,
            );
            return respondWithAllocator(ctx, &resp, self.api_server.alloc);
        }
        var parsed_table = parseGlobalQueryTable(ctx.allocator, body_data) catch {
            _ = ctx.status(400);
            return ctx.text("invalid query request");
        };
        defer parsed_table.deinit();
        var resp = try self.api_server.handlePublicTableQueryWithContentTypeCancellation(
            parsed_table.table_name,
            body_data,
            ctx.header("content-type"),
            authenticated_identity,
            &cancellation,
        );
        return respondWithAllocator(ctx, &resp, self.api_server.alloc);
    }

    pub fn evaluate(self: *AntflyApiHandler, ctx: *httpx.Context) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const alloc = ctx.allocator;
        const body_data = (try ctx.body()) orelse {
            _ = ctx.status(400);
            return ctx.text("invalid eval request");
        };
        var parsed = metadata_openapi.server.parseEvaluateBody(alloc, body_data) catch {
            _ = ctx.status(400);
            return ctx.text("invalid eval request");
        };
        defer parsed.deinit();
        var arena_impl = std.heap.ArenaAllocator.init(alloc);
        defer arena_impl.deinit();
        const response = retrieval_agent.buildEvalResponse(arena_impl.allocator(), parsed.value) catch |err| switch (err) {
            error.InvalidEvalRequest => {
                _ = ctx.status(400);
                return ctx.text("invalid eval request");
            },
            else => return err,
        };
        return ctx.json(response);
    }

    pub fn queryBuilderAgent(self: *AntflyApiHandler, ctx: *httpx.Context) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const alloc = ctx.allocator;
        const body_data = (try ctx.body()) orelse {
            _ = ctx.status(400);
            return ctx.text("invalid query builder request");
        };
        var parsed = metadata_openapi.server.parseQueryBuilderAgentBody(alloc, body_data) catch {
            _ = ctx.status(400);
            return ctx.text("invalid query builder request");
        };
        defer parsed.deinit();
        if (parsed.value.intent.len == 0) {
            _ = ctx.status(400);
            return ctx.text("invalid query builder request");
        }

        var table_context: ?query_builder_agent.QueryBuilderTableContext = null;
        defer if (table_context) |context| http_server_mod.freeQueryBuilderTableContext(alloc, context);
        var runtime_validator_context: ?http_server_mod.QueryBuilderRuntimeQueryRequestValidatorContext = null;
        if (parsed.value.table) |table_name| {
            if (authenticated_identity) |identity| {
                if (!http_server_mod.permissionsAllow(identity.permissions, .table, table_name, .read)) {
                    _ = ctx.status(403);
                    return ctx.text("forbidden");
                }
            }
            table_context = self.api_server.loadQueryBuilderTableContext(table_name) catch |err| switch (err) {
                error.TableNotFound => {
                    _ = ctx.status(404);
                    return ctx.text("not found");
                },
                else => return err,
            };
            if (self.api_server.table_reads) |reads| {
                runtime_validator_context = .{
                    .server = self.api_server,
                    .source = reads,
                    .table_name = table_name,
                    .query_embedding_security_scope = ApiHttpServer.queryEmbeddingSecurityScope(authenticated_identity),
                };
                table_context.?.runtime_query_request_validator = runtime_validator_context.?.iface();
            }
        }

        var arena_impl = std.heap.ArenaAllocator.init(alloc);
        defer arena_impl.deinit();
        const QueryBuilderGenerationRunner = struct {
            antfly_provider: ?managed_embedder.AntflyProvider,
            secret_store: ?*common_secrets.FileStore,
            io: std.Io,

            fn iface(runner: *@This()) query_builder_agent.GenerationRunner {
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
        var generation_runner = QueryBuilderGenerationRunner{
            .antfly_provider = self.api_server.antfly_provider,
            .secret_store = self.api_server.cfg.secret_store,
            .io = self.api_server.inferenceIo(),
        };
        var collected_context = query_builder_agent.collectQueryBuilderContext(table_context);
        const response = query_builder_agent.buildQueryBuilderResponseWithCollectedContext(arena_impl.allocator(), parsed.value, &collected_context, generation_runner.iface()) catch |err| switch (err) {
            error.InvalidQueryBuilderRequest => {
                _ = ctx.status(400);
                return ctx.text("invalid query builder request");
            },
            error.DocIdentityNamespaceMismatch => {
                _ = ctx.status(503);
                return ctx.text("doc identity unavailable");
            },
            else => {
                if (try respondQueryEmbeddingOperationalError(ctx, err)) |operational_response| return operational_response;
                return err;
            },
        };
        return ctx.json(response);
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
            if (err == error.InvalidCreateTableSchemaRequest) {
                return ctx.text(table_contract.createTableRequestErrorMessage(body_data));
            }
            return ctx.text("invalid create table request");
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
        const metadata_create_timeout_ns = 5 * std.time.ns_per_s;
        const metadata_create_poll_ns = 50 * std.time.ns_per_ms;
        const metadata_create_start_ns = platform_time.monotonicNs();
        while (true) {
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
                    if (platform_time.monotonicNs() -| metadata_create_start_ns >= metadata_create_timeout_ns) {
                        std.log.err("public create table metadata create failed table={s} err={}", .{ decoded_table_name, err });
                        return err;
                    }
                    sleepNs(metadata_create_poll_ns);
                    continue;
                },
                else => {
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
        if (!self.query_admission.tryAcquire()) return queryOverloadedResponse(ctx);
        defer self.query_admission.release();
        var cancellation = http_common.RequestCancellation{ .borrowed = ctx.cancellation };
        var watcher: ?PeerCancellationWatcher = null;
        startPeerCancellationWatcher(ctx, &cancellation, &watcher);
        defer if (watcher) |*value| value.deinit();
        const decoded_table_name = (try decodePathParamOrBadRequest(ctx, table_name)) orelse return ctx.text("invalid path parameter");
        defer ctx.allocator.free(decoded_table_name);
        const body_data = (try ctx.body()) orelse {
            _ = ctx.status(400);
            return ctx.text("missing body");
        };
        var resp = try self.api_server.handlePublicTableQueryWithContentTypeCancellation(
            decoded_table_name,
            body_data,
            ctx.header("content-type"),
            authenticated_identity,
            &cancellation,
        );
        return respondWithAllocator(ctx, &resp, self.api_server.alloc);
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
        return try handleTableBatchOffEventLoop(ctx, self.api_server.cfg.backend_runtime, decoded_table_name, body_data, self.api_server.tableApi());
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
        ) catch |err| switch (err) {
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
        var resp = try public_table_http.handleTableBackup(ctx.allocator, decoded_table_name, body_data, self.api_server.tableApi(), self.api_server.cfg.secret_store, self.api_server.cfg.node_config, self.api_server.sharedApiIo());
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
        return respondWithAllocator(ctx, &resp, self.api_server.alloc);
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
        const invalid_schema_message = table_contract.schemaUpdateRequestErrorMessage(body_data);
        const schema_json = table_contract.parseSchemaUpdateRequest(alloc, body_data) catch {
            _ = ctx.status(400);
            return ctx.text(invalid_schema_message);
        };
        defer alloc.free(schema_json);

        const table_before = try self.api_server.loadOwnedTableRecord(alloc, decoded_table_name);
        if (table_before == null) {
            self.api_server.source.updateSchema(alloc, decoded_table_name, schema_json) catch |err| switch (err) {
                error.InvalidSchemaUpdateRequest => {
                    _ = ctx.status(400);
                    return ctx.text(invalid_schema_message);
                },
                error.TableNotFound => {
                    _ = ctx.status(404);
                    return ctx.text("not found");
                },
                error.UnsupportedOperation => {
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
        self.api_server.source.updateSchema(alloc, decoded_table_name, schema_json) catch |err| switch (err) {
            error.InvalidSchemaUpdateRequest => {
                _ = ctx.status(400);
                return ctx.text(invalid_schema_message);
            },
            error.TableNotFound => {
                _ = ctx.status(404);
                return ctx.text("not found");
            },
            error.UnsupportedOperation => {
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
            },
            else => return err,
        };
        const expected_table = try tables_api.applySchemaUpdateRecord(alloc, &table_before.?, schema_json);
        defer metadata_table_manager.freeTable(alloc, expected_table);
        self.api_server.waitForMetadataProjection(decoded_table_name, expected_table.schema_json, expected_table.indexes_json) catch |err| switch (err) {
            error.TableVisibilityTimeout => {
                _ = ctx.status(500);
                return ctx.text("schema update did not converge");
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
        const source = self.api_server.table_reads orelse {
            _ = ctx.status(404);
            return ctx.text("not found");
        };
        // The OpenAPI request body is optional; an absent body is the default
        // unbounded-range scan, just like an explicitly empty legacy request.
        const body_data = (try ctx.body()) orelse "";
        var scan_req = http_route_helpers.parseScanKeysRequest(alloc, body_data) catch |err| {
            if (try http_route_helpers.scanRequestErrorResponse(alloc, err)) |response| {
                var owned_response = response;
                return respond(ctx, &owned_response);
            }
            return err;
        };
        defer scan_req.deinit(alloc);

        var result = (try source.scan(
            alloc,
            decoded_table_name,
            scan_req.from,
            scan_req.to,
            scan_req.opts,
            .read_index,
        )) orelse {
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
            error.HAReadRequiresPrimary, error.ReadRequiresPrimary => {
                _ = ctx.status(503);
                return ctx.text("read requires primary");
            },
            error.HAReadWaitForApply, error.HAReadWaitForMetadata, error.ReadUnavailable => {
                _ = ctx.status(503);
                return ctx.text("standby read unavailable");
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
        var resp = try public_table_http.handleDocumentArtifactManifest(alloc, decoded_table_name, decoded_key, decoded_artifact_name, opts, self.api_server.tableApi());
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
        var resp = try public_table_http.handleDocumentArtifactManifests(alloc, decoded_table_name, decoded_key, opts, self.api_server.tableApi());
        return respondOwnedApiResponse(ctx, &resp);
    }

    pub fn listArtifactEnrichments(self: *AntflyApiHandler, ctx: *httpx.Context, table_name: []const u8) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const decoded_table_name = (try decodePathParamOrBadRequest(ctx, table_name)) orelse return ctx.text("invalid path parameter");
        defer ctx.allocator.free(decoded_table_name);
        var resp = try public_table_http.handleListArtifactEnrichments(ctx.allocator, decoded_table_name, self.api_server.tableApi());
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
        return respondWithAllocator(ctx, &response, self.api_server.alloc);
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
        return respondWithAllocator(ctx, &response, self.api_server.alloc);
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
        return respondWithAllocator(ctx, &response, self.api_server.alloc);
    }

    pub fn getTableRepairJob(self: *AntflyApiHandler, ctx: *httpx.Context, table_name: []const u8, job_id: []const u8) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const decoded_table_name = (try decodePathParamOrBadRequest(ctx, table_name)) orelse return ctx.text("invalid path parameter");
        defer ctx.allocator.free(decoded_table_name);
        var response = try self.api_server.handlePublicTableRepairJob(decoded_table_name, job_id);
        return respondWithAllocator(ctx, &response, self.api_server.alloc);
    }

    pub fn advanceTableRepairJob(self: *AntflyApiHandler, ctx: *httpx.Context, table_name: []const u8, job_id: []const u8) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const decoded_table_name = (try decodePathParamOrBadRequest(ctx, table_name)) orelse return ctx.text("invalid path parameter");
        defer ctx.allocator.free(decoded_table_name);
        var response = try self.api_server.handlePublicAdvanceTableRepairJob(decoded_table_name, job_id);
        return respondWithAllocator(ctx, &response, self.api_server.alloc);
    }

    pub fn cancelTableRepairJob(self: *AntflyApiHandler, ctx: *httpx.Context, table_name: []const u8, job_id: []const u8) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const decoded_table_name = (try decodePathParamOrBadRequest(ctx, table_name)) orelse return ctx.text("invalid path parameter");
        defer ctx.allocator.free(decoded_table_name);
        var response = try self.api_server.handlePublicCancelTableRepairJob(decoded_table_name, job_id);
        return respondWithAllocator(ctx, &response, self.api_server.alloc);
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
        var resp = try public_table_http.handleReprocessDocumentArtifact(alloc, decoded_table_name, decoded_key, decoded_artifact_name, self.api_server.tableApi());
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
        var resp = try public_table_http.handleReprocessDocumentArtifactRange(alloc, decoded_table_name, decoded_artifact_name, body_data, self.api_server.tableApi());
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
        return respondWithAllocator(ctx, &response, self.api_server.alloc);
    }

    pub fn getDocumentArtifactReprocessJob(self: *AntflyApiHandler, ctx: *httpx.Context, table_name: []const u8, artifact_name: []const u8, job_id: []const u8) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const decoded_table_name = (try decodePathParamOrBadRequest(ctx, table_name)) orelse return ctx.text("invalid path parameter");
        defer ctx.allocator.free(decoded_table_name);
        var response = try self.api_server.handlePublicDocumentArtifactReprocessJob(decoded_table_name, artifact_name, job_id);
        return respondWithAllocator(ctx, &response, self.api_server.alloc);
    }

    pub fn advanceDocumentArtifactReprocessJob(self: *AntflyApiHandler, ctx: *httpx.Context, table_name: []const u8, artifact_name: []const u8, job_id: []const u8) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const decoded_table_name = (try decodePathParamOrBadRequest(ctx, table_name)) orelse return ctx.text("invalid path parameter");
        defer ctx.allocator.free(decoded_table_name);
        var response = try self.api_server.handlePublicAdvanceDocumentArtifactReprocessJob(decoded_table_name, artifact_name, job_id);
        return respondWithAllocator(ctx, &response, self.api_server.alloc);
    }

    pub fn cancelDocumentArtifactReprocessJob(self: *AntflyApiHandler, ctx: *httpx.Context, table_name: []const u8, artifact_name: []const u8, job_id: []const u8) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const decoded_table_name = (try decodePathParamOrBadRequest(ctx, table_name)) orelse return ctx.text("invalid path parameter");
        defer ctx.allocator.free(decoded_table_name);
        var response = try self.api_server.handlePublicCancelDocumentArtifactReprocessJob(decoded_table_name, artifact_name, job_id);
        return respondWithAllocator(ctx, &response, self.api_server.alloc);
    }

    pub fn listIndexes(self: *AntflyApiHandler, ctx: *httpx.Context, table_name: []const u8) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.api_server.alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const decoded_table_name = (try decodePathParamOrBadRequest(ctx, table_name)) orelse return ctx.text("invalid path parameter");
        defer ctx.allocator.free(decoded_table_name);
        var resp = try public_table_http.handleTableListIndexes(ctx.allocator, decoded_table_name, self.api_server.tableApi());
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
        var resp = try public_table_http.handleTableGetIndex(ctx.allocator, decoded_table_name, decoded_index_name, self.api_server.tableApi());
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
        var resp = try public_table_http.handleTableCreateIndex(ctx.allocator, decoded_table_name, decoded_index_name, body_data, self.api_server.tableApi());
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
        var resp = try public_table_http.handleTableDeleteIndex(ctx.allocator, decoded_table_name, decoded_index_name, self.api_server.tableApi());
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
        var resp = try public_table_http.handlePutArtifactEnrichment(alloc, decoded_table_name, decoded_artifact_name, body_data, self.api_server.tableApi());
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
        var resp = try public_table_http.handleDeleteArtifactEnrichment(alloc, decoded_table_name, decoded_artifact_name, self.api_server.tableApi());
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
            _ = ctx.status(400);
            return ctx.text("invalid row filter");
        };
        var parsed_filter = usermgr_openapi.server.parseSetRowFilterBody(alloc, body_data) catch {
            _ = ctx.status(400);
            return ctx.text("invalid row filter");
        };
        defer parsed_filter.deinit();
        const normalized_filter = try std.json.Stringify.valueAlloc(alloc, parsed_filter.value, .{});
        defer alloc.free(normalized_filter);
        http_server_mod.validateAuthRowFilterJson(alloc, normalized_filter) catch {
            _ = ctx.status(400);
            return ctx.text("invalid row filter");
        };
        manager.setRowFilter(user_name, table, normalized_filter) catch |err| switch (err) {
            error.UserNotFound => {
                _ = ctx.status(404);
                return ctx.text("not found");
            },
            else => {
                _ = ctx.status(400);
                return ctx.text("invalid row filter");
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
            _ = ctx.status(400);
            return ctx.text("invalid row filter");
        };
        var parsed_filter = usermgr_openapi.server.parseSetSubjectRowFilterBody(alloc, body_data) catch {
            _ = ctx.status(400);
            return ctx.text("invalid row filter");
        };
        defer parsed_filter.deinit();
        const normalized_filter = try std.json.Stringify.valueAlloc(alloc, parsed_filter.value, .{});
        defer alloc.free(normalized_filter);
        http_server_mod.validateAuthRowFilterJson(alloc, normalized_filter) catch {
            _ = ctx.status(400);
            return ctx.text("invalid row filter");
        };
        manager.setSubjectRowFilter(subject, table, normalized_filter) catch {
            _ = ctx.status(400);
            return ctx.text("invalid row filter");
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
        self.* = .{
            .allocator = allocator,
            .io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{}),
            .server = undefined,
            .handler = .{ .api_server = api_server },
            .thread = null,
        };
        errdefer self.io_impl.deinit();

        self.server = httpx.Server.initWithConfig(allocator, self.io_impl.io(), .{
            .host = "127.0.0.1",
            .port = 0,
            .request_timeout_ms = 30_000,
        });
        errdefer self.server.deinit();

        const metadata_router = metadata_openapi.server.ServerRouter(AntflyApiHandler).init(&self.handler);
        var prefixed = PrefixedServer("/db/v1", httpx.Server){ .inner = &self.server };
        try metadata_router.register(&prefixed);

        const usermgr_router = usermgr_openapi.server.ServerRouter(AntflyApiHandler).init(&self.handler);
        try usermgr_router.register(&self.server);

        try self.server.bind();
        self.thread = try std.Thread.spawn(.{}, listenHttpxE2eServer, .{&self.server});
    }

    fn deinit(self: *HttpxE2eServer) void {
        if (self.thread) |thread| {
            self.server.stop();
            thread.join();
        }
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

test "httpx internal request conversion preserves protocol headers" {
    const alloc = std.testing.allocator;

    var request = try httpx.Request.init(alloc, .POST, "http://127.0.0.1/mcp/v1/extensions/memoryaf");
    defer request.deinit();
    try request.setHeader("Content-Type", "application/json");
    try request.setHeader("Mcp-Session-Id", "session-123");

    var ctx = httpx.Context.init(alloc, undefined, &request);
    defer ctx.deinit();

    var converted = try AntflyApiHandler.httpRequestFromContext(&ctx, "{}");
    defer converted.deinit();

    try std.testing.expectEqualStrings("session-123", converted.value.header("mcp-session-id") orelse return error.MissingHeader);
    try std.testing.expectEqualStrings("application/json", converted.value.content_type orelse return error.MissingContentType);
    try std.testing.expectEqualStrings("{}", converted.value.body);
}

test "httpx query admission rejects saturated queries without blocking control routes" {
    const alloc = std.testing.allocator;
    var source = AuthStatusSource{};
    var api_server = ApiHttpServer.init(alloc, .{}, source.iface(), null, null);
    var handler = AntflyApiHandler{
        .api_server = &api_server,
        .query_admission = QueryAdmission.init(1),
    };

    try std.testing.expect(handler.query_admission.tryAcquire());
    defer handler.query_admission.release();

    var query_request = try httpx.Request.init(alloc, .POST, "http://127.0.0.1/db/v1/tables/docs/query");
    defer query_request.deinit();
    query_request.body = "{}";
    var query_ctx = httpx.Context.init(alloc, undefined, &query_request);
    defer query_ctx.deinit();
    var rejected = try handler.queryTable(&query_ctx, "docs");
    defer rejected.deinit();
    try std.testing.expectEqual(@as(u16, 429), rejected.status.code);
    try std.testing.expectEqualStrings("1", rejected.headers.get("Retry-After").?);

    var control_request = try httpx.Request.init(alloc, .GET, "http://127.0.0.1/db/v1/status");
    defer control_request.deinit();
    var control_ctx = httpx.Context.init(alloc, undefined, &control_request);
    defer control_ctx.deinit();
    var control = try handler.getStatus(&control_ctx);
    defer control.deinit();
    try std.testing.expectEqual(@as(u16, 200), control.status.code);
}

test "httpx query admission releases a cancelled query slot" {
    var admission = QueryAdmission.init(1);
    try std.testing.expect(admission.tryAcquire());
    var cancellation = http_common.RequestCancellation{};
    cancellation.cancel();
    try std.testing.expect(cancellation.isCancelled());
    admission.release();
    try std.testing.expect(admission.tryAcquire());
    admission.release();
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

    var e2e_server: HttpxE2eServer = undefined;
    try e2e_server.init(alloc, &api_server);
    defer e2e_server.deinit();

    var client_io = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer client_io.deinit();
    var client = httpx.Client.initWithConfig(alloc, client_io.io(), .{ .keep_alive = false });
    defer client.deinit();

    const base_url = try e2e_server.baseUrl(alloc);
    defer alloc.free(base_url);

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
    try std.testing.expectEqualStrings("text/plain", forbidden.contentType().?);
    try std.testing.expectEqualStrings("forbidden", forbidden.body.?);

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
        try std.testing.expectEqualStrings("text/plain", denied_batch.contentType().?);
        try std.testing.expectEqualStrings("forbidden", denied_batch.body.?);
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
    var me_body = try std.json.parseFromSlice(struct { username: []const u8 }, alloc, me_resp.body.?, .{});
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
