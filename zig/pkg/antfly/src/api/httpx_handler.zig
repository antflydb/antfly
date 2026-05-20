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
const public_table_http = @import("public_table_http.zig");
const tables_api = @import("tables.zig");
const table_contract = @import("table_contract.zig");
const table_reads = @import("table_reads.zig");
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
const platform_time = @import("../platform/time.zig");
const usermgr = @import("../usermgr/mod.zig");
const raft_mod = @import("../raft/mod.zig");
const raft_reconciler = @import("../raft/reconciler.zig");
const casbin = @import("antfly_casbin");

const db_mod = @import("../storage/db/mod.zig");
const metadata_openapi = @import("antfly_metadata_openapi");
const usermgr_openapi = @import("antfly_usermgr_openapi");

pub const AntflyApiHandler = struct {
    api_server: *ApiHttpServer,

    // ---------------------------------------------------------------
    // Response conversion: http_common.HttpResponse -> httpx.Response
    // ---------------------------------------------------------------

    pub fn respond(ctx: *httpx.Context, resp: *http_common.HttpResponse) !httpx.Response {
        defer resp.deinit(ctx.allocator);
        _ = ctx.status(resp.status);
        if (resp.content_type) |ct| {
            try ctx.setHeader("content-type", ct);
        }
        for (resp.headers) |hdr| {
            try ctx.setHeader(hdr.name, hdr.value);
        }
        _ = ctx.response.body(resp.body);
        return ctx.response.build();
    }

    fn respondWithAllocator(ctx: *httpx.Context, resp: *http_common.HttpResponse, alloc: std.mem.Allocator) !httpx.Response {
        defer resp.deinit(alloc);
        _ = ctx.status(resp.status);
        if (resp.content_type) |ct| {
            try ctx.setHeader("content-type", ct);
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
        var job = OffloadedTableBatch{
            .alloc = std.heap.page_allocator,
            .table_name = table_name,
            .body_data = body_data,
            .api = api,
        };
        var future = try runtime_io.concurrent(OffloadedTableBatch.run, .{&job});
        while (!job.done.load(.acquire)) {
            ctx.io.sleep(std.Io.Duration.fromMilliseconds(1), .awake) catch {};
        }
        _ = future.await(runtime_io);
        if (job.err) |err| return err;
        var resp = job.result.?;
        defer resp.deinit(std.heap.page_allocator);
        return respondApiResponseBody(ctx, resp.status, resp.body);
    }

    fn httpRequestFromContext(ctx: *httpx.Context, body_data: []const u8) !http_common.HttpRequest {
        const method: http_common.Method = switch (ctx.request.method) {
            .GET => .GET,
            .POST => .POST,
            .PUT => .PUT,
            .DELETE => .DELETE,
            else => {
                return error.UnsupportedMethod;
            },
        };
        return .{
            .method = method,
            .uri = ctx.request.uri.raw,
            .authorization = ctx.header("authorization"),
            .content_type = ctx.header("content-type"),
            .body = body_data,
        };
    }

    // ---------------------------------------------------------------
    // Authentication helper
    // ---------------------------------------------------------------

    fn authenticate(self: *AntflyApiHandler, ctx: *httpx.Context) !?AuthenticatedIdentity {
        if (!self.api_server.cfg.auth_enabled) return null;
        const auth_header = ctx.header("authorization");
        return self.api_server.authenticateRequest(auth_header) catch |err| switch (err) {
            error.Unauthorized, error.InvalidPassword, error.UserNotFound, error.ApiKeyInvalid, error.ApiKeyNotFound, error.ApiKeyExpired => {
                return null;
            },
            else => return err,
        };
    }

    fn requireAuth(self: *AntflyApiHandler, ctx: *httpx.Context) !?AuthenticatedIdentity {
        if (!self.api_server.cfg.auth_enabled) return null;
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

    fn unauthorizedResponse(ctx: *httpx.Context) !httpx.Response {
        try ctx.setHeader("WWW-Authenticate", "Basic realm=\"antfly\"");
        return jsonResponse(ctx, 401, "{\"error\":\"unauthorized\"}");
    }

    fn authorizeRequest(self: *AntflyApiHandler, ctx: *httpx.Context, identity: *?AuthenticatedIdentity) !?httpx.Response {
        identity.* = null;
        if (!self.api_server.cfg.auth_enabled) return null;
        if (self.api_server.cfg.user_manager == null) return null;

        const path = http_server_mod.stripApiPrefix(ctx.request.uri.path);
        identity.* = self.api_server.authenticateRequest(ctx.header("authorization")) catch |err| switch (err) {
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
        if (http_server_mod.requiredPermissionForRequest(method, path)) |required| {
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
        defer if (authenticated_identity) |*identity| identity.deinit(ctx.allocator);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const alloc = ctx.allocator;
        const metadata_status = try self.api_server.source.status();
        var public_status = try cluster.fromMetadataStatus(alloc, metadata_status);
        defer public_status.deinit(alloc);
        public_status.auth_enabled = self.api_server.cfg.auth_enabled;
        public_status.swarm_mode = self.api_server.cfg.swarm_mode;
        if (self.api_server.cfg.secret_store) |secret_store| {
            _ = secret_store.refreshIfChanged() catch |err| {
                std.log.warn("secret store status refresh skipped err={}", .{err});
            };
            cluster.applySecretStoreHealth(&public_status, secret_store.healthSnapshot());
        }
        return ctx.json(public_status);
    }

    pub fn listSecrets(self: *AntflyApiHandler, ctx: *httpx.Context) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(ctx.allocator);
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
        defer if (authenticated_identity) |*identity| identity.deinit(ctx.allocator);
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
        defer if (authenticated_identity) |*identity| identity.deinit(ctx.allocator);
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
        defer if (authenticated_identity) |*identity| identity.deinit(ctx.allocator);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const body_data = (try ctx.body()) orelse {
            _ = ctx.status(400);
            return ctx.text("missing body");
        };
        return try handleTableBatchOffEventLoop(ctx, self.api_server.cfg.backend_runtime, "", body_data, self.api_server.tableApi());
    }

    pub fn commitTransaction(self: *AntflyApiHandler, ctx: *httpx.Context) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(ctx.allocator);
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
        defer if (authenticated_identity) |*identity| identity.deinit(ctx.allocator);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const alloc = self.api_server.alloc;
        const sessions = try self.api_server.txn_sessions.listStatuses(alloc);
        defer alloc.free(sessions);
        var arena_impl = std.heap.ArenaAllocator.init(ctx.allocator);
        defer arena_impl.deinit();
        const response = try transactions_api.buildSessionListResponse(arena_impl.allocator(), sessions);
        return ctx.json(response);
    }

    pub fn cleanupTransactionSessions(self: *AntflyApiHandler, ctx: *httpx.Context, params: metadata_openapi.server.CleanupTransactionSessionsParams) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(ctx.allocator);
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
        defer if (authenticated_identity) |*identity| identity.deinit(ctx.allocator);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const body_data = (try ctx.body()) orelse "";
        const alloc = self.api_server.alloc;
        const begin_req = transactions_api.parseBeginRequest(alloc, body_data) catch {
            _ = ctx.status(400);
            return ctx.text("invalid transaction begin request");
        };
        const session = try self.api_server.txn_sessions.begin(alloc, begin_req, self.api_server.localSessionNodeId());
        var arena_impl = std.heap.ArenaAllocator.init(ctx.allocator);
        defer arena_impl.deinit();
        const response = try transactions_api.buildBeginResponse(arena_impl.allocator(), session);
        _ = ctx.status(201);
        return ctx.json(response);
    }

    pub fn getTransactionSession(self: *AntflyApiHandler, ctx: *httpx.Context, transaction_id: []const u8) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(ctx.allocator);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const txn_id = distributed_txn.parseTxnIdHex(transaction_id) catch {
            _ = ctx.status(400);
            return ctx.text("invalid transaction id");
        };
        const forward_req = httpRequestFromContext(ctx, "") catch {
            _ = ctx.status(405);
            return ctx.text("method not allowed");
        };
        if (try self.api_server.forwardSessionRequest(txn_id, forward_req)) |forwarded| {
            var resp = forwarded;
            return respond(ctx, &resp);
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
        defer if (authenticated_identity) |*identity| identity.deinit(ctx.allocator);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const body_data = (try ctx.body()) orelse "";
        const txn_id = distributed_txn.parseTxnIdHex(transaction_id) catch {
            _ = ctx.status(400);
            return ctx.text("invalid transaction id");
        };
        const forward_req = httpRequestFromContext(ctx, body_data) catch {
            _ = ctx.status(405);
            return ctx.text("method not allowed");
        };
        if (try self.api_server.forwardSessionRequest(txn_id, forward_req)) |forwarded| {
            var resp = forwarded;
            return respond(ctx, &resp);
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
        defer if (authenticated_identity) |*identity| identity.deinit(ctx.allocator);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const body_data = (try ctx.body()) orelse "";
        const txn_id = distributed_txn.parseTxnIdHex(transaction_id) catch {
            _ = ctx.status(400);
            return ctx.text("invalid transaction id");
        };
        const forward_req = httpRequestFromContext(ctx, body_data) catch {
            _ = ctx.status(405);
            return ctx.text("method not allowed");
        };
        if (try self.api_server.forwardSessionRequest(txn_id, forward_req)) |forwarded| {
            var resp = forwarded;
            return respond(ctx, &resp);
        }
        const alloc = self.api_server.alloc;
        var read_req = transactions_api.parseStageReadPayload(alloc, body_data) catch {
            _ = ctx.status(400);
            return ctx.text("invalid transaction read request");
        };
        defer read_req.deinit(alloc);

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
        defer if (authenticated_identity) |*identity| identity.deinit(ctx.allocator);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const body_data = (try ctx.body()) orelse "";
        const txn_id = distributed_txn.parseTxnIdHex(transaction_id) catch {
            _ = ctx.status(400);
            return ctx.text("invalid transaction id");
        };
        const forward_req = httpRequestFromContext(ctx, body_data) catch {
            _ = ctx.status(405);
            return ctx.text("method not allowed");
        };
        if (try self.api_server.forwardSessionRequest(txn_id, forward_req)) |forwarded| {
            var resp = forwarded;
            return respond(ctx, &resp);
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
        defer if (authenticated_identity) |*identity| identity.deinit(ctx.allocator);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const body_data = (try ctx.body()) orelse "";
        const txn_id = distributed_txn.parseTxnIdHex(transaction_id) catch {
            _ = ctx.status(400);
            return ctx.text("invalid transaction id");
        };
        const forward_req = httpRequestFromContext(ctx, body_data) catch {
            _ = ctx.status(405);
            return ctx.text("method not allowed");
        };
        if (try self.api_server.forwardSessionRequest(txn_id, forward_req)) |forwarded| {
            var resp = forwarded;
            return respond(ctx, &resp);
        }
        const alloc = self.api_server.alloc;
        const info = (self.api_server.txn_sessions.createSavepoint(alloc, txn_id) catch |err| switch (err) {
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
        defer if (authenticated_identity) |*identity| identity.deinit(ctx.allocator);
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
        const forward_req = httpRequestFromContext(ctx, body_data) catch {
            _ = ctx.status(405);
            return ctx.text("method not allowed");
        };
        if (try self.api_server.forwardSessionRequest(txn_id, forward_req)) |forwarded| {
            var resp = forwarded;
            return respond(ctx, &resp);
        }
        const alloc = self.api_server.alloc;
        const info = (self.api_server.txn_sessions.rollbackToSavepoint(alloc, txn_id, parsed_savepoint_id) catch |err| switch (err) {
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
        defer if (authenticated_identity) |*identity| identity.deinit(ctx.allocator);
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
        const forward_req = httpRequestFromContext(ctx, body_data) catch {
            _ = ctx.status(405);
            return ctx.text("method not allowed");
        };
        if (try self.api_server.forwardSessionRequest(txn_id, forward_req)) |forwarded| {
            var resp = forwarded;
            return respond(ctx, &resp);
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
        defer if (authenticated_identity) |*identity| identity.deinit(ctx.allocator);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const body_data = (try ctx.body()) orelse "";
        const txn_id = distributed_txn.parseTxnIdHex(transaction_id) catch {
            _ = ctx.status(400);
            return ctx.text("invalid transaction id");
        };
        const forward_req = httpRequestFromContext(ctx, body_data) catch {
            _ = ctx.status(405);
            return ctx.text("method not allowed");
        };
        if (try self.api_server.forwardSessionRequest(txn_id, forward_req)) |forwarded| {
            var resp = forwarded;
            return respond(ctx, &resp);
        }
        const alloc = self.api_server.alloc;
        if (!self.api_server.txn_sessions.remove(alloc, txn_id)) {
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
        defer if (authenticated_identity) |*identity| identity.deinit(ctx.allocator);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const body_data = (try ctx.body()) orelse "";
        var resp = try cluster_api_http.handleClusterBackup(ctx.allocator, body_data, self.api_server.clusterApi(), self.api_server.cfg.secret_store);
        return respondOwnedApiResponse(ctx, &resp);
    }

    pub fn restore(self: *AntflyApiHandler, ctx: *httpx.Context) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(ctx.allocator);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const body_data = (try ctx.body()) orelse "";
        var resp = try cluster_api_http.handleClusterRestore(ctx.allocator, body_data, self.api_server.clusterApi(), self.api_server.cfg.secret_store);
        return respondOwnedApiResponse(ctx, &resp);
    }

    pub fn listBackups(self: *AntflyApiHandler, ctx: *httpx.Context, params: metadata_openapi.server.ListBackupsParams) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(ctx.allocator);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        var resp = try cluster_api_http.handleClusterBackupList(ctx.allocator, params.location, self.api_server.clusterApi());
        return respondOwnedApiResponse(ctx, &resp);
    }

    pub fn globalQuery(self: *AntflyApiHandler, ctx: *httpx.Context) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(ctx.allocator);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const body_data = (try ctx.body()) orelse {
            _ = ctx.status(400);
            return ctx.text("missing body");
        };
        var resp = try self.api_server.handlePublicTableQuery(
            "",
            body_data,
            authenticated_identity,
        );
        return respondWithAllocator(ctx, &resp, self.api_server.alloc);
    }

    pub fn evaluate(self: *AntflyApiHandler, ctx: *httpx.Context) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(ctx.allocator);
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
        defer if (authenticated_identity) |*identity| identity.deinit(ctx.allocator);
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
                };
                table_context.?.runtime_query_request_validator = runtime_validator_context.?.iface();
            }
        }

        var arena_impl = std.heap.ArenaAllocator.init(alloc);
        defer arena_impl.deinit();
        const QueryBuilderGenerationRunner = struct {
            local_termite_provider: ?managed_embedder.LocalTermiteProvider,
            secret_store: ?*common_secrets.FileStore,

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
                var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
                defer io_impl.deinit();
                var client = httpx.Client.initWithConfig(a, io_impl.io(), .{ .keep_alive = false });
                defer client.deinit();
                return try generating_runtime.executeChainWithOptions(a, &client, chain, .{ .local_termite_provider = runner.local_termite_provider, .secret_store = runner.secret_store }, messages);
            }
        };
        var generation_runner = QueryBuilderGenerationRunner{ .local_termite_provider = self.api_server.local_termite_provider, .secret_store = self.api_server.cfg.secret_store };
        var collected_context = query_builder_agent.collectQueryBuilderContext(table_context);
        const response = query_builder_agent.buildQueryBuilderResponseWithCollectedContext(arena_impl.allocator(), parsed.value, &collected_context, generation_runner.iface()) catch |err| switch (err) {
            error.InvalidQueryBuilderRequest => {
                _ = ctx.status(400);
                return ctx.text("invalid query builder request");
            },
            else => return err,
        };
        return ctx.json(response);
    }

    pub fn retrievalAgent(self: *AntflyApiHandler, ctx: *httpx.Context) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(ctx.allocator);
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

            fn iface(runner: *@This()) retrieval_agent.QueryRunner {
                return .{
                    .ptr = runner,
                    .vtable = &.{
                        .run_query = runQuery,
                        .scan_keys = runScanKeys,
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
                var semantic_resolver = http_server_mod.SemanticStatusResolver{ .source = runner.server.source, .local_termite_provider = runner.server.local_termite_provider };
                var query_req = query_api.parseQueryRequest(a, semantic_resolver.iface(), table_name, query_json) catch |err| switch (err) {
                    error.InvalidQueryRequest, error.UnsupportedQueryRequest => return error.InvalidRetrievalAgentRequest,
                    else => return err,
                };
                defer query_req.deinit(a);
                runner.server.maybeRouteQueryToReadSchema(table_name, &query_req.req) catch |err| switch (err) {
                    error.TableNotFound => return err,
                    error.InvalidSchemaUpdateRequest, error.InvalidTableIndexMetadata => return error.InvalidRetrievalAgentRequest,
                    else => return err,
                };
                return (runner.source.query(
                    a,
                    table_name,
                    query_req.req,
                    .read_index,
                ) catch |err| {
                    std.log.err("retrieval query failed table={s} query={s} err={}", .{ table_name, query_json, err });
                    return err;
                }) orelse error.TableNotFound;
            }

            fn runScanKeys(
                ptr: *anyopaque,
                a: std.mem.Allocator,
                table_name: []const u8,
            ) ![]const []const u8 {
                const runner: *@This() = @ptrCast(@alignCast(ptr));
                var scan = (try runner.source.scan(
                    a,
                    table_name,
                    "",
                    "",
                    .{ .limit = 0 },
                    .read_index,
                )) orelse return error.TableNotFound;
                defer scan.deinit(a);

                var keys = std.ArrayListUnmanaged([]const u8).empty;
                errdefer {
                    for (keys.items) |key| a.free(key);
                    keys.deinit(a);
                }

                var lines = std.mem.splitScalar(u8, scan.ndjson, '\n');
                while (lines.next()) |line| {
                    if (line.len == 0) continue;
                    const key = http_server_mod.scanLineKey(a, line) catch return error.InvalidRetrievalAgentRequest;
                    try keys.append(a, key);
                }
                return try keys.toOwnedSlice(a);
            }
        };

        const RetrievalGenerationRunner = struct {
            local_termite_provider: ?managed_embedder.LocalTermiteProvider,
            secret_store: ?*common_secrets.FileStore,

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
                var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
                defer io_impl.deinit();
                var client = httpx.Client.initWithConfig(a, io_impl.io(), .{ .keep_alive = false });
                defer client.deinit();
                return try generating_runtime.executeChainWithOptions(a, &client, chain, .{ .local_termite_provider = runner.local_termite_provider, .secret_store = runner.secret_store }, messages);
            }
        };
        var generation_runner = RetrievalGenerationRunner{ .local_termite_provider = self.api_server.local_termite_provider, .secret_store = self.api_server.cfg.secret_store };

        var query_runner = RetrievalQueryRunner{
            .server = self.api_server,
            .source = source,
        };
        const retrieval_resp = retrieval_agent.execute(alloc, query_runner.iface(), generation_runner.iface(), body_data) catch |err| switch (err) {
            error.InvalidRetrievalAgentRequest, error.UnsupportedRetrievalAgentRequest => {
                _ = ctx.status(400);
                return ctx.text("invalid retrieval agent request");
            },
            error.TableNotFound => {
                _ = ctx.status(404);
                return ctx.text("not found");
            },
            else => {
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
        defer if (authenticated_identity) |*identity| identity.deinit(ctx.allocator);
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
        const storage_statuses = try self.api_server.collectTableStorageStatuses(&snapshot, params.prefix);
        defer if (storage_statuses) |items| alloc.free(items);
        var arena_impl = std.heap.ArenaAllocator.init(alloc);
        defer arena_impl.deinit();
        const response = try tables_api.buildTableListWithStorageStatuses(arena_impl.allocator(), &snapshot, params.prefix, storage_statuses);
        return ctx.json(response);
    }

    pub fn getTable(self: *AntflyApiHandler, ctx: *httpx.Context, table_name: []const u8) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(ctx.allocator);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const alloc = ctx.allocator;
        var snapshot = (try self.api_server.source.adminSnapshot()) orelse {
            _ = ctx.status(404);
            return ctx.text("not found");
        };
        defer self.api_server.source.freeAdminSnapshot(&snapshot);
        var storage_status_buf: [1]tables_api.TableStorageStatus = undefined;
        const storage_statuses = try self.api_server.bestEffortSingleTableStorageStatuses(table_name, &storage_status_buf);
        var arena_impl = std.heap.ArenaAllocator.init(alloc);
        defer arena_impl.deinit();
        const response = (try tables_api.buildSingleTableStatusWithStorageStatuses(arena_impl.allocator(), &snapshot, table_name, storage_statuses)) orelse {
            _ = ctx.status(404);
            return ctx.text("not found");
        };
        return ctx.json(response);
    }

    pub fn createTable(self: *AntflyApiHandler, ctx: *httpx.Context, table_name: []const u8) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(ctx.allocator);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const alloc = ctx.allocator;
        const body_data = (try ctx.body()) orelse {
            _ = ctx.status(400);
            return ctx.text("invalid create table request");
        };
        var create_req = table_contract.parseCreateTableRequest(alloc, body_data) catch {
            _ = ctx.status(400);
            return ctx.text("invalid create table request");
        };
        defer create_req.deinit(alloc);
        tables_api.validatePublicAlgebraicIndexesJson(alloc, create_req.indexes_json orelse tables_api.default_indexes_json) catch {
            _ = ctx.status(400);
            return ctx.text("unsupported table index configuration");
        };
        std.log.info("public create table begin table={s}", .{table_name});
        const metadata_create_timeout_ns = 5 * std.time.ns_per_s;
        const metadata_create_poll_ns = 50 * std.time.ns_per_ms;
        const metadata_create_start_ns = platform_time.monotonicNs();
        while (true) {
            self.api_server.source.createTable(alloc, table_name, create_req) catch |err| switch (err) {
                error.UnsupportedOperation => {
                    _ = ctx.status(405);
                    return ctx.text("method not allowed");
                },
                error.UnexpectedHttpStatus => {
                    if (platform_time.monotonicNs() -| metadata_create_start_ns >= metadata_create_timeout_ns) {
                        std.log.err("public create table metadata create failed table={s} err={}", .{ table_name, err });
                        return err;
                    }
                    sleepNs(metadata_create_poll_ns);
                    continue;
                },
                else => {
                    std.log.err("public create table metadata create failed table={s} err={}", .{ table_name, err });
                    return err;
                },
            };
            break;
        }
        std.log.info("public create table metadata done table={s}", .{table_name});
        const local_create_handled = if (self.api_server.table_writes) |table_writes_source| blk: {
            break :blk (table_writes_source.createTable(alloc, table_name, create_req) catch |err| switch (err) {
                error.InvalidCreateTableRequest, error.UnsupportedCreateTableRequest => {
                    _ = ctx.status(400);
                    return ctx.text("unsupported table index configuration");
                },
                else => {
                    std.log.err("public create table local create failed table={s} err={}", .{ table_name, err });
                    return err;
                },
            }) != null;
        } else false;
        if (local_create_handled) {
            std.log.info("public create table wait projected presence table={s}", .{table_name});
            self.api_server.waitForProjectedTablePresence(table_name) catch |err| switch (err) {
                error.TableVisibilityTimeout => {
                    std.log.err("public create table metadata visibility timed out table={s}", .{table_name});
                    _ = ctx.status(500);
                    return ctx.text("table create did not converge");
                },
                else => return err,
            };
        } else {
            const metadata_wait_handled = self.api_server.source.waitTableLifecycle(table_name, .present) catch |err| switch (err) {
                error.TableVisibilityTimeout => {
                    std.log.err("public create table metadata lifecycle timed out table={s}", .{table_name});
                    _ = ctx.status(500);
                    return ctx.text("table create did not converge");
                },
                else => {
                    std.log.err("public create table metadata lifecycle failed table={s} err={}", .{ table_name, err });
                    return err;
                },
            };
            if (!metadata_wait_handled) {
                std.log.info("public create table wait metadata visibility table={s}", .{table_name});
                self.api_server.waitForTableVisibility(table_name, .present) catch |err| switch (err) {
                    error.TableVisibilityTimeout => {
                        std.log.err("public create table metadata visibility timed out table={s}", .{table_name});
                        _ = ctx.status(500);
                        return ctx.text("table create did not converge");
                    },
                    else => return err,
                };
            }
        }
        std.log.info("public create table visible table={s}", .{table_name});

        var snapshot = (try self.api_server.source.adminSnapshot()) orelse {
            _ = ctx.status(404);
            return ctx.text("not found");
        };
        defer self.api_server.source.freeAdminSnapshot(&snapshot);
        var arena_impl = std.heap.ArenaAllocator.init(alloc);
        defer arena_impl.deinit();
        const response = (try tables_api.buildSingleTableStatusWithStorageStatuses(arena_impl.allocator(), &snapshot, table_name, null)) orelse {
            _ = ctx.status(404);
            return ctx.text("not found");
        };
        return ctx.json(response);
    }

    pub fn dropTable(self: *AntflyApiHandler, ctx: *httpx.Context, table_name: []const u8) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(ctx.allocator);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const alloc = ctx.allocator;
        var local_drop_group_ids: ?[]u64 = null;
        defer if (local_drop_group_ids) |group_ids| alloc.free(group_ids);
        if (self.api_server.table_writes != null) {
            if (try self.api_server.source.adminSnapshot()) |snapshot_value| {
                var snapshot = snapshot_value;
                defer self.api_server.source.freeAdminSnapshot(&snapshot);
                local_drop_group_ids = try ApiHttpServer.tableGroupIdsFromSnapshot(alloc, &snapshot, table_name);
            }
        }
        self.api_server.source.dropTable(alloc, table_name) catch |err| switch (err) {
            error.TableNotFound => {
                _ = ctx.status(404);
                return ctx.text("not found");
            },
            error.UnsupportedOperation => {
                _ = ctx.status(405);
                return ctx.text("method not allowed");
            },
            else => {
                std.log.err("public drop table metadata remove failed table={s} err={s}", .{ table_name, @errorName(err) });
                return err;
            },
        };
        if (self.api_server.table_writes) |write_source| {
            const group_ids = local_drop_group_ids orelse &.{};
            _ = write_source.dropTable(alloc, table_name, group_ids) catch |err| switch (err) {
                error.TableNotFound => null,
                else => {
                    std.log.err("public drop table local cleanup failed table={s} err={s}", .{ table_name, @errorName(err) });
                    return err;
                },
            };
        }
        self.api_server.waitForTableVisibility(table_name, .absent) catch |err| switch (err) {
            error.TableVisibilityTimeout => {
                std.log.err("public drop table metadata visibility timed out table={s}", .{table_name});
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
        defer if (authenticated_identity) |*identity| identity.deinit(ctx.allocator);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const body_data = (try ctx.body()) orelse {
            _ = ctx.status(400);
            return ctx.text("missing body");
        };
        var resp = try self.api_server.handlePublicTableQuery(
            table_name,
            body_data,
            authenticated_identity,
        );
        return respondWithAllocator(ctx, &resp, self.api_server.alloc);
    }

    pub fn batchWrite(self: *AntflyApiHandler, ctx: *httpx.Context, table_name: []const u8) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(ctx.allocator);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const body_data = (try ctx.body()) orelse {
            _ = ctx.status(400);
            return ctx.text("missing body");
        };
        return try handleTableBatchOffEventLoop(ctx, self.api_server.cfg.backend_runtime, table_name, body_data, self.api_server.tableApi());
    }

    pub fn linearMerge(self: *AntflyApiHandler, ctx: *httpx.Context, table_name: []const u8) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(ctx.allocator);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const alloc = ctx.allocator;
        const reads = self.api_server.table_reads orelse {
            _ = ctx.status(404);
            return ctx.text("not found");
        };
        const writes = self.api_server.table_writes orelse {
            _ = ctx.status(404);
            return ctx.text("not found");
        };
        if (!(try self.api_server.tableExists(table_name))) {
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

        self.api_server.validateTableWritesAgainstSchema(table_name, merge_req.writes) catch |err| switch (err) {
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
            table_name,
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
        defer if (authenticated_identity) |*identity| identity.deinit(ctx.allocator);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const body_data = (try ctx.body()) orelse "";
        var resp = try public_table_http.handleTableBackup(ctx.allocator, table_name, body_data, self.api_server.tableApi(), self.api_server.cfg.secret_store);
        return respondOwnedApiResponse(ctx, &resp);
    }

    pub fn restoreTable(self: *AntflyApiHandler, ctx: *httpx.Context, table_name: []const u8) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(ctx.allocator);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const body_data = (try ctx.body()) orelse "";
        var resp = try public_table_http.handleTableRestore(ctx.allocator, table_name, body_data, self.api_server.tableApi(), self.api_server.cfg.secret_store);
        return respondOwnedApiResponse(ctx, &resp);
    }

    pub fn updateSchema(self: *AntflyApiHandler, ctx: *httpx.Context, table_name: []const u8) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(ctx.allocator);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const alloc = ctx.allocator;
        const body_data = (try ctx.body()) orelse {
            _ = ctx.status(400);
            return ctx.text("invalid schema update request");
        };
        const schema_json = table_contract.parseSchemaUpdateRequest(alloc, body_data) catch {
            _ = ctx.status(400);
            return ctx.text("invalid schema update request");
        };
        defer alloc.free(schema_json);

        const table_before = try self.api_server.loadOwnedTableRecord(table_name);
        if (table_before == null) {
            self.api_server.source.updateSchema(alloc, table_name, schema_json) catch |err| switch (err) {
                error.InvalidSchemaUpdateRequest => {
                    _ = ctx.status(400);
                    return ctx.text("invalid schema update request");
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
                    _ = table_writes_source.updateSchema(alloc, table_name, schema_json) catch |write_err| switch (write_err) {
                        error.InvalidSchemaUpdateRequest, error.InvalidCreateTableRequest => {
                            _ = ctx.status(400);
                            return ctx.text("invalid schema update request");
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
            const value = try http_server_mod.buildLocalSchemaUpdateStatus(arena_impl.allocator(), table_name, schema_json);
            return ctx.json(value);
        }
        defer metadata_table_manager.freeTable(alloc, table_before.?);

        var local_schema_applied = false;
        self.api_server.source.updateSchema(alloc, table_name, schema_json) catch |err| switch (err) {
            error.InvalidSchemaUpdateRequest => {
                _ = ctx.status(400);
                return ctx.text("invalid schema update request");
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
                _ = table_writes_source.updateSchema(alloc, table_name, schema_json) catch |write_err| switch (write_err) {
                    error.InvalidSchemaUpdateRequest, error.InvalidCreateTableRequest => {
                        _ = ctx.status(400);
                        return ctx.text("invalid schema update request");
                    },
                    else => return write_err,
                };
                local_schema_applied = true;
            },
            else => return err,
        };
        const expected_table = try tables_api.applySchemaUpdateRecord(alloc, &table_before.?, schema_json);
        defer metadata_table_manager.freeTable(alloc, expected_table);
        self.api_server.waitForMetadataProjection(table_name, expected_table.schema_json, expected_table.indexes_json) catch |err| switch (err) {
            error.TableVisibilityTimeout => {
                _ = ctx.status(500);
                return ctx.text("schema update did not converge");
            },
            else => return err,
        };
        if (self.api_server.table_writes) |table_writes_source| {
            if (!local_schema_applied) {
                _ = table_writes_source.updateSchema(alloc, table_name, schema_json) catch |write_err| switch (write_err) {
                    error.InvalidSchemaUpdateRequest, error.InvalidCreateTableRequest => {
                        _ = ctx.status(400);
                        return ctx.text("invalid schema update request");
                    },
                    else => return write_err,
                };
            }
            if (try self.api_server.source.runRound()) {
                _ = try self.api_server.source.runRound();
                _ = try self.api_server.source.runRound();
            }
        }

        const body = try self.api_server.encodeSchemaUpdateResponse(table_name, schema_json);
        defer self.api_server.alloc.free(body);
        return jsonResponse(ctx, 200, body);
    }

    pub fn scanKeys(self: *AntflyApiHandler, ctx: *httpx.Context, table_name: []const u8) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(ctx.allocator);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const alloc = ctx.allocator;
        const source = self.api_server.table_reads orelse {
            _ = ctx.status(404);
            return ctx.text("not found");
        };
        const body_data = (try ctx.body()) orelse {
            _ = ctx.status(400);
            return ctx.text("missing body");
        };
        var scan_req = try http_route_helpers.parseScanKeysRequest(alloc, body_data);
        defer scan_req.deinit(alloc);

        var result = (try source.scan(
            alloc,
            table_name,
            scan_req.from,
            scan_req.to,
            scan_req.opts,
            .read_index,
        )) orelse {
            _ = ctx.status(404);
            return ctx.text("not found");
        };
        defer result.deinit(alloc);

        const row_filter_json = try http_server_mod.resolveEffectiveRowFilterJson(alloc, authenticated_identity, table_name);
        defer if (row_filter_json) |value| alloc.free(value);
        if (row_filter_json) |value| {
            const filtered = try self.api_server.filterScanResultByRowFilter(source, table_name, result.ndjson, value);
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
        defer if (authenticated_identity) |*identity| identity.deinit(ctx.allocator);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const alloc = ctx.allocator;
        const decoded_key = try http_route_helpers.decodePercentEncodedPathComponentAlloc(alloc, key);
        defer alloc.free(decoded_key);
        const source = self.api_server.table_reads orelse {
            _ = ctx.status(404);
            return ctx.text("not found");
        };

        var lookup_opts = try http_route_helpers.parseLookupOptions(alloc, ctx.request.uri.query orelse "");
        defer lookup_opts.deinit(alloc);

        var result = (try source.lookup(alloc, table_name, decoded_key, lookup_opts.opts, .read_index)) orelse {
            _ = ctx.status(404);
            return ctx.text("not found");
        };
        defer result.deinit(alloc);

        const row_filter_json = try http_server_mod.resolveEffectiveRowFilterJson(alloc, authenticated_identity, table_name);
        defer if (row_filter_json) |value| alloc.free(value);
        if (row_filter_json) |value| {
            if (!(try self.api_server.docMatchesRowFilter(source, table_name, decoded_key, value))) {
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

    pub fn listIndexes(self: *AntflyApiHandler, ctx: *httpx.Context, table_name: []const u8) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(ctx.allocator);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        var resp = try public_table_http.handleTableListIndexes(ctx.allocator, table_name, self.api_server.tableApi());
        return respondOwnedApiResponse(ctx, &resp);
    }

    pub fn getIndex(self: *AntflyApiHandler, ctx: *httpx.Context, table_name: []const u8, index_name: []const u8) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(ctx.allocator);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        var resp = try public_table_http.handleTableGetIndex(ctx.allocator, table_name, index_name, self.api_server.tableApi());
        return respondOwnedApiResponse(ctx, &resp);
    }

    pub fn createIndex(self: *AntflyApiHandler, ctx: *httpx.Context, table_name: []const u8, index_name: []const u8) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(ctx.allocator);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const body_data = (try ctx.body()) orelse "";
        var resp = try public_table_http.handleTableCreateIndex(ctx.allocator, table_name, index_name, body_data, self.api_server.tableApi());
        return respondOwnedApiResponse(ctx, &resp);
    }

    pub fn dropIndex(self: *AntflyApiHandler, ctx: *httpx.Context, table_name: []const u8, index_name: []const u8) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(ctx.allocator);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        var resp = try public_table_http.handleTableDeleteIndex(ctx.allocator, table_name, index_name, self.api_server.tableApi());
        return respondOwnedApiResponse(ctx, &resp);
    }

    // ---------------------------------------------------------------
    // usermgr_openapi handler interface (16 methods)
    // ---------------------------------------------------------------

    pub fn getCurrentUser(self: *AntflyApiHandler, ctx: *httpx.Context) !httpx.Response {
        const alloc = ctx.allocator;
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(alloc);
        if (try self.authorizeRequest(ctx, &authenticated_identity)) |resp| return resp;
        const identity = authenticated_identity orelse return try unauthorizedResponse(ctx);
        var arena_impl = std.heap.ArenaAllocator.init(alloc);
        defer arena_impl.deinit();
        const current_user = try http_server_mod.makeCurrentUserResponse(arena_impl.allocator(), identity.username, identity.permissions, identity.metadata_json);
        return ctx.json(current_user);
    }

    pub fn listUsers(self: *AntflyApiHandler, ctx: *httpx.Context) !httpx.Response {
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(ctx.allocator);
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
        defer if (authenticated_identity) |*identity| identity.deinit(ctx.allocator);
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
        defer if (authenticated_identity) |*identity| identity.deinit(ctx.allocator);
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
        defer if (authenticated_identity) |*identity| identity.deinit(ctx.allocator);
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
        defer if (authenticated_identity) |*identity| identity.deinit(ctx.allocator);
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
        defer if (authenticated_identity) |*identity| identity.deinit(ctx.allocator);
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
        defer if (authenticated_identity) |*identity| identity.deinit(ctx.allocator);
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
        defer if (authenticated_identity) |*identity| identity.deinit(ctx.allocator);
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
        defer if (authenticated_identity) |*identity| identity.deinit(ctx.allocator);
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
        defer if (authenticated_identity) |*identity| identity.deinit(ctx.allocator);
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
        defer if (authenticated_identity) |*identity| identity.deinit(ctx.allocator);
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
        defer if (authenticated_identity) |*identity| identity.deinit(ctx.allocator);
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
        defer if (authenticated_identity) |*identity| identity.deinit(ctx.allocator);
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
        defer if (authenticated_identity) |*identity| identity.deinit(ctx.allocator);
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
        defer if (authenticated_identity) |*identity| identity.deinit(ctx.allocator);
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
        defer if (authenticated_identity) |*identity| identity.deinit(ctx.allocator);
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
        defer if (authenticated_identity) |*identity| identity.deinit(ctx.allocator);
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
        defer if (authenticated_identity) |*identity| identity.deinit(ctx.allocator);
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
        defer if (authenticated_identity) |*identity| identity.deinit(ctx.allocator);
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
        defer if (authenticated_identity) |*identity| identity.deinit(ctx.allocator);
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
        defer if (authenticated_identity) |*identity| identity.deinit(ctx.allocator);
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
        defer if (authenticated_identity) |*identity| identity.deinit(ctx.allocator);
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
        defer if (authenticated_identity) |*identity| identity.deinit(ctx.allocator);
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
        var prefixed = PrefixedServer("/api/v1", httpx.Server){ .inner = &self.server };
        try metadata_router.register(&prefixed);

        const usermgr_router = usermgr_openapi.server.ServerRouter(AntflyApiHandler).init(&self.handler);
        try usermgr_router.register(&prefixed);

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
        return .{
            .status = .{ .metadata_group_id = 1, .metrics = .{} },
            .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                .table_id = 7,
                .name = "docs",
                .schema_json = tables_api.effectiveSchemaJson(self.schema_json),
                .indexes_json = tables_api.default_indexes_json,
                .placement_role = "data",
            }})[0..]),
            .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                .group_id = 7001,
                .table_id = 7,
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
        try std.testing.expectEqualStrings(self.schema_json.?, schema_json.?);
        _ = self.projection_wait_calls.fetchAdd(1, .monotonic);
    }
};

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
        .swarm_mode = true,
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

    const status_url = try std.fmt.allocPrint(alloc, "{s}/api/v1/status", .{base_url});
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
    const secrets_url = try std.fmt.allocPrint(alloc, "{s}/api/v1/secrets", .{base_url});
    defer alloc.free(secrets_url);
    const reader_headers = [_][2][]const u8{.{ "authorization", reader_auth }};
    var forbidden = try getWithRetry(&client, client_io.io(), secrets_url, &reader_headers, 20);
    defer forbidden.deinit();
    try std.testing.expectEqual(@as(u16, 403), forbidden.status.code);
    try std.testing.expectEqualStrings("text/plain", forbidden.contentType().?);
    try std.testing.expectEqualStrings("forbidden", forbidden.body.?);

    const admin_auth = try encodeBasicAuthorization(alloc, "admin", "admin");
    defer alloc.free(admin_auth);
    const me_url = try std.fmt.allocPrint(alloc, "{s}/api/v1/auth/v1/me", .{base_url});
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
    const lookup_url = try std.fmt.allocPrint(alloc, "{s}/api/v1/tables/docs/lookup/doc:a?fields=title", .{base_url});
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
    const lookup_url = try std.fmt.allocPrint(alloc, "{s}/api/v1/tables/docs/lookup/docs%2Fgetting-started.md?fields=title", .{base_url});
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
    const schema_url = try std.fmt.allocPrint(alloc, "{s}/api/v1/tables/docs/schema", .{base_url});
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
    const restore_url = try std.fmt.allocPrint(alloc, "{s}/api/v1/restore", .{base_url});
    defer alloc.free(restore_url);
    const restore_body = "{\"backup_id\":\"snap1\",\"location\":\"ftp://bad\"}";
    const headers = [_][2][]const u8{.{ "content-type", "application/json" }};

    var resp = try requestWithRetry(&client, client_io.io(), .POST, restore_url, restore_body, &headers, 20);
    defer resp.deinit();
    try std.testing.expectEqual(@as(u16, 400), resp.status.code);
    try std.testing.expectEqualStrings("text/plain; charset=utf-8", resp.contentType().?);
    try std.testing.expectEqualStrings("unsupported backup location", resp.body.?);
}
