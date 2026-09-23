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
const httpx = @import("httpx");
const http_common = @import("raft/transport/http_common.zig");
const serverless_http_routes = @import("serverless/api/http_routes.zig");
const serverless_http_types = @import("serverless/api/http_types.zig");
const IngressScope = @import("serverless/api/ingress.zig").Scope;

const secrets = @import("common/secrets.zig");
pub const ServerlessHttpServerConfig = struct {
    secret_store: ?*secrets.FileStore = null,
    // Explicit credential required even on otherwise unauthenticated serverless
    // deployments. TLS terminates at the deployment's trusted ingress.
    secret_admin_token: ?[]const u8 = null,
};

test "workload admission serverless adapters retain body ownership through output drain" {
    const Owner = @import("common/workload_allocator.zig").Owner;
    const Controller = @import("common/workload_admission.zig").Controller;
    const Producer = struct {
        gate: Controller = Controller.initConfigured(1, .{ .max_retained_bytes = 4096 }),

        pub fn handle(self: *@This(), _: serverless_http_types.HttpRequest) !serverless_http_types.HttpResponse {
            const owner = try Owner.create(std.testing.allocator, &self.gate);
            errdefer owner.release();
            const alloc = owner.allocator();
            const content_type = try alloc.dupe(u8, "application/json");
            errdefer alloc.free(content_type);
            return .{
                .status = 200,
                .content_type = content_type,
                .body = try alloc.dupe(u8, "{\"ok\":true}"),
                .memory_owner = owner,
            };
        }
    };
    var producer: Producer = .{};
    var producer_live = true;
    defer if (producer_live) producer.gate.deinitMemory();
    var server = ServerlessHttpServer.init(std.testing.allocator, .{}, &producer);
    var buffered = try server.handle(.{ .method = .POST, .uri = "/db/v1/tables/docs/query", .body = "{}" });
    defer buffered.deinit(std.testing.allocator);
    var request = try httpx.Request.init(std.testing.allocator, .POST, "/db/v1/tables/docs/query");
    defer request.deinit();
    request.body = "{}";
    var ctx = httpx.Context.init(std.testing.allocator, std.testing.io, &request);
    var ctx_live = true;
    defer if (ctx_live) ctx.deinit();
    var native = try server.handleHttpx(&ctx);
    defer native.deinit();
    ctx.deinit();
    ctx_live = false;
    try std.testing.expect(producer.gate.stats().retained_bytes >= buffered.body.len + native.body.?.len);
    producer.gate.deinitMemory();
    producer_live = false;
    producer = undefined;
    try std.testing.expectEqualStrings("{\"ok\":true}", buffered.body);
    try std.testing.expectEqualStrings("{\"ok\":true}", native.body.?);
}

test "workload admission serverless ingress protects probes before body read and retains composite output" {
    const Owner = @import("common/workload_allocator.zig").Owner;
    const Controller = @import("common/workload_admission.zig").Controller;
    const Producer = struct {
        ingress: @import("common/workload_ingress.zig").Runtime = .init(.{ .max_requests = 2, .max_retained_bytes = 16384, .control_requests = 1, .control_retained_bytes = 4096 }),
        gate: Controller = .initConfigured(1, .{ .max_retained_bytes = 8192 }),
        calls: usize = 0,
        shrink_after_write: bool = false,

        pub fn beginIngress(self: *@This(), req: serverless_http_types.HttpRequest) !?*IngressScope {
            return try IngressScope.create(std.testing.allocator, if (std.mem.eql(u8, req.path, "/healthz")) &self.ingress.control else &self.ingress.general);
        }
        pub fn handle(self: *@This(), req: serverless_http_types.HttpRequest) !serverless_http_types.HttpResponse {
            const acquired = if (req.ingress == null) try self.beginIngress(req) else null;
            defer if (acquired) |scope| scope.release();
            const scope = req.ingress orelse acquired.?;
            const owner = try Owner.createChild(scope.owner, &self.gate);
            errdefer owner.release();
            const alloc = owner.allocator();
            const content_type = try alloc.dupe(u8, "application/json");
            errdefer alloc.free(content_type);
            const body = try alloc.dupe(u8, "{\"ok\":true}");
            errdefer alloc.free(body);
            self.calls += 1;
            if (self.shrink_after_write) {
                scope.write_started = true;
                scope.write_completed = true;
                try self.ingress.general.reconfigure(1, .{ .max_retained_bytes = 1 });
            }
            scope.retain();
            return .{ .status = 200, .content_type = content_type, .body = body, .memory_owner = owner, .ingress = scope };
        }
    };
    const alloc = std.testing.allocator;
    var producer: Producer = .{};
    var producer_live = true;
    defer if (producer_live) {
        producer.gate.deinitMemory();
        producer.ingress.deinitMemory();
    };
    var server = ServerlessHttpServer.init(alloc, .{}, &producer);
    var buffered = try server.handle(.{ .method = .GET, .uri = "/tables" });
    var buffered_live = true;
    defer if (buffered_live) buffered.deinit(alloc);
    const Delegate = struct {
        reads: usize = 0,
        fn read(raw: ?*anyopaque) !?[]const u8 {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.reads += 1;
            return "{}";
        }
    };
    var delegate: Delegate = .{};
    var denied_request = try httpx.Request.init(alloc, .POST, "/tables/docs/batch");
    defer denied_request.deinit();
    var denied_ctx = httpx.Context.init(alloc, std.testing.io, &denied_request);
    defer denied_ctx.deinit();
    denied_ctx.body_delegate = .{ .ptr = &delegate, .read_all = Delegate.read, .streaming = true };
    var denied = try server.handleHttpx(&denied_ctx);
    defer denied.deinit();
    try std.testing.expectEqual(@as(u16, 429), denied.status.code);
    try std.testing.expectEqual(@as(usize, 0), delegate.reads);
    try std.testing.expectEqual(@as(usize, 1), producer.calls);

    var probe_request = try httpx.Request.init(alloc, .GET, "/healthz");
    defer probe_request.deinit();
    var probe_ctx = httpx.Context.init(alloc, std.testing.io, &probe_request);
    var probe_ctx_live = true;
    defer if (probe_ctx_live) probe_ctx.deinit();
    var probe = try server.handleHttpx(&probe_ctx);
    var probe_live = true;
    defer if (probe_live) probe.deinit();
    try std.testing.expectEqual(@as(u16, 200), probe.status.code);
    probe_ctx.deinit();
    probe_ctx_live = false;
    try std.testing.expectEqual(@as(usize, 1), producer.ingress.general.stats().in_flight);
    try std.testing.expectEqual(@as(usize, 1), producer.ingress.control.stats().in_flight);
    buffered.deinit(alloc);
    buffered_live = false;
    try std.testing.expectEqual(@as(usize, 0), producer.ingress.general.stats().in_flight);

    // A header/output allocation denied after durable work cannot become an
    // execution_started=false retry signal at the transport adapter.
    producer.shrink_after_write = true;
    var write_request = try httpx.Request.init(alloc, .POST, "/tables/docs/batch");
    defer write_request.deinit();
    write_request.body = "{}";
    var write_ctx = httpx.Context.init(alloc, std.testing.io, &write_request);
    var write_ctx_live = true;
    defer if (write_ctx_live) write_ctx.deinit();
    var write_response = try server.handleHttpx(&write_ctx);
    defer write_response.deinit();
    try std.testing.expectEqual(@as(u16, 503), write_response.status.code);
    try std.testing.expect(std.mem.indexOf(u8, write_response.body.?, "committed_pending") != null);
    try std.testing.expect(std.mem.indexOf(u8, write_response.body.?, "\"retryable\":false") != null);
    write_ctx.deinit();
    write_ctx_live = false;
    try std.testing.expectEqual(@as(usize, 1), producer.ingress.general.stats().in_flight);
    producer.gate.deinitMemory();
    producer.ingress.deinitMemory();
    producer_live = false;
    producer = undefined;
    try std.testing.expectEqualStrings("{\"ok\":true}", probe.body.?);
    probe.deinit();
    probe_live = false;
}

pub const Handler = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    const VTable = struct {
        handle: *const fn (*anyopaque, serverless_http_types.HttpRequest) anyerror!serverless_http_types.HttpResponse,
        begin_ingress: *const fn (*anyopaque, serverless_http_types.HttpRequest) anyerror!?*IngressScope,
    };

    pub fn handle(self: Handler, req: serverless_http_types.HttpRequest) !serverless_http_types.HttpResponse {
        return self.vtable.handle(self.ptr, req);
    }

    pub fn beginIngress(self: Handler, req: serverless_http_types.HttpRequest) !?*IngressScope {
        return self.vtable.begin_ingress(self.ptr, req);
    }
};

pub const ServerlessHttpServer = struct {
    alloc: std.mem.Allocator,
    cfg: ServerlessHttpServerConfig,
    handler: Handler,

    pub fn init(
        alloc: std.mem.Allocator,
        cfg: ServerlessHttpServerConfig,
        handler: anytype,
    ) ServerlessHttpServer {
        return .{
            .alloc = alloc,
            .cfg = cfg,
            .handler = handlerIface(handler),
        };
    }

    pub fn executor(self: *ServerlessHttpServer) http_common.RequestExecutor {
        return .{
            .ptr = self,
            .vtable = &.{
                .execute = execute,
                .execute_stream = executeStream,
            },
        };
    }

    /// Preserve the one-request snapshot contract for in-process routed scans.
    /// The serverless handler currently owns a bounded response buffer, but the
    /// executor still exposes it through the streaming ABI so coordinators do
    /// not split one logical scan into independently-versioned page requests.
    fn executeStream(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        req: http_common.HttpRequest,
        writer: http_common.StreamWriter,
    ) !bool {
        const self: *ServerlessHttpServer = @ptrCast(@alignCast(ptr));
        var response = try self.handle(req);
        defer response.deinit(self.alloc);
        const header_alloc = response.owner_allocator orelse alloc;
        const headers = try header_alloc.alloc(http_common.RequestHeader, response.headers.len);
        defer header_alloc.free(headers);
        for (response.headers, headers) |source, *destination| {
            destination.* = .{ .name = source.name, .value = source.value };
        }
        try writer.start(alloc, .{
            .status = response.status,
            .content_type = response.content_type,
            .headers = headers,
        });
        try writer.writeAll(response.body);
        try writer.flush();
        return true;
    }

    pub fn handle(self: *ServerlessHttpServer, req: http_common.HttpRequest) !http_common.HttpResponse {
        if (isSecretPath(req.uri)) return self.handleSecrets(req);
        const method: serverless_http_routes.HttpMethod = switch (req.method) {
            .GET => .get,
            .POST => .post,
            .PUT => .put,
            .DELETE => .delete,
        };

        const path = if (std.mem.indexOfScalar(u8, req.uri, '?')) |query_index| req.uri[0..query_index] else req.uri;
        var resp = try self.handler.handle(.{
            .method = method,
            .path = path,
            .body = req.body,
            .cancellation = if (req.cancellation) |value| value.token() else .none,
            .deadline_ns = if (req.timeout_ms) |ms| @import("antfly_platform").time.monotonicNs() +| @as(u64, ms) * std.time.ns_per_ms else null,
        });
        defer resp.deinit(self.alloc);

        const alloc = if (resp.memory_owner) |owner| owner.allocator() else self.alloc;
        var response: http_common.HttpResponse = .{
            .status = resp.status,
            .owner_allocator = alloc,
            .content_type = resp.content_type,
            .body = resp.body,
        };
        if (resp.ingress) |scope| {
            std.debug.assert(scope.previous_output == null);
            if (resp.memory_owner) |owner| scope.previous_output = .{ .ptr = owner, .release = releaseResponseMemory };
            response.allocation_owner = .{ .ptr = scope, .release = IngressScope.releaseOutput };
        } else if (resp.memory_owner) |owner|
            response.allocation_owner = .{ .ptr = owner, .release = releaseResponseMemory };
        // Move the allocation owner and buffers together across the adapter.
        resp.content_type = &.{};
        resp.body = &.{};
        resp.memory_owner = null;
        resp.ingress = null;
        errdefer response.deinit(alloc);
        if (resp.retry_after_seconds) |seconds| {
            const value = try std.fmt.allocPrint(alloc, "{d}", .{seconds});
            defer alloc.free(value);
            const name_owned = try alloc.dupe(u8, "Retry-After");
            errdefer alloc.free(name_owned);
            const value_owned = try alloc.dupe(u8, value);
            errdefer alloc.free(value_owned);
            const headers = try alloc.alloc(http_common.Header, 1);
            headers[0] = .{
                .name = name_owned,
                .value = value_owned,
            };
            response.headers = headers;
        }
        return response;
    }

    fn isSecretPath(uri: []const u8) bool {
        const path = if (std.mem.indexOfScalar(u8, uri, '?')) |i| uri[0..i] else uri;
        return std.mem.eql(u8, path, "/secrets") or std.mem.startsWith(u8, path, "/secrets/") or std.mem.eql(u8, path, "/db/v1/secrets") or std.mem.startsWith(u8, path, "/db/v1/secrets/");
    }
    fn secretResponse(self: *ServerlessHttpServer, status: u16, body: []const u8) !http_common.HttpResponse {
        const out = try self.alloc.dupe(u8, body);
        errdefer self.alloc.free(out);
        return .{ .status = status, .owner_allocator = self.alloc, .body = out, .content_type = try self.alloc.dupe(u8, "application/json") };
    }
    fn handleSecrets(self: *ServerlessHttpServer, req: http_common.HttpRequest) !http_common.HttpResponse {
        const token = self.cfg.secret_admin_token orelse return self.secretResponse(503, "{\"error\":\"secret administration disabled\"}");
        if (token.len < 32) return self.secretResponse(503, "{\"error\":\"secret administration disabled\"}");
        const header = req.authorization orelse req.header("Authorization") orelse return self.secretResponse(401, "{\"error\":\"unauthorized\"}");
        if (!std.mem.startsWith(u8, header, "Bearer ")) return self.secretResponse(401, "{\"error\":\"unauthorized\"}");
        var expected: [32]u8 = undefined;
        var supplied: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(token, &expected, .{});
        std.crypto.hash.sha2.Sha256.hash(header[7..], &supplied, .{});
        if (!std.crypto.timing_safe.eql([32]u8, expected, supplied)) return self.secretResponse(401, "{\"error\":\"unauthorized\"}");
        return self.secretOperation(req) catch |err| switch (err) {
            error.InvalidSecretKey, error.InvalidArgument, error.InvalidRequest => self.secretResponse(400, "{\"error\":\"invalid secret request\"}"),
            error.Conflict => self.secretResponse(409, "{\"error\":\"secret revision conflict\"}"),
            error.ResourceRequestTooLarge => self.secretResponse(413, "{\"error\":\"native secret collection limit exceeded\"}"),
            else => self.secretResponse(503, "{\"error\":\"secret source unavailable\"}"),
        };
    }
    fn secretOperation(self: *ServerlessHttpServer, req: http_common.HttpRequest) !http_common.HttpResponse {
        const store = self.cfg.secret_store orelse return error.Unavailable;
        var path = if (std.mem.indexOfScalar(u8, req.uri, '?')) |i| req.uri[0..i] else req.uri;
        if (std.mem.startsWith(u8, path, "/db/v1")) path = path[6..];
        if (std.mem.eql(u8, path, "/secrets")) {
            if (req.method != .GET) return self.secretResponse(405, "{}");
            const listed = try store.list(self.alloc);
            defer secrets.freeListedSecrets(self.alloc, listed);
            const body = try std.json.Stringify.valueAlloc(self.alloc, .{ .secrets = listed, .writable = store.writable }, .{});
            defer self.alloc.free(body);
            return self.secretResponse(200, body);
        }
        const key = path["/secrets/".len..];
        if (req.method == .PUT) {
            if (req.body.len > 6 * @import("common/secret_contract.zig").max_value_bytes + 1024) return error.InvalidRequest;
            var parsed = std.json.parseFromSlice(struct { value: []const u8 }, self.alloc, req.body, .{}) catch return error.InvalidRequest;
            defer parsed.deinit();
            var result = try store.put(self.alloc, key, parsed.value.value);
            defer result.deinit(self.alloc);
            const body = try std.json.Stringify.valueAlloc(self.alloc, result, .{});
            defer self.alloc.free(body);
            return self.secretResponse(200, body);
        }
        if (req.method == .DELETE) return self.secretResponse(if (try store.delete(key)) 204 else 404, "");
        return self.secretResponse(405, "{}");
    }

    fn releaseResponseMemory(raw: *anyopaque) void {
        const owner: *@import("common/workload_allocator.zig").Owner = @ptrCast(@alignCast(raw));
        owner.release();
    }

    fn retainResponseMemory(raw: *anyopaque) void {
        const owner: *@import("common/workload_allocator.zig").Owner = @ptrCast(@alignCast(raw));
        owner.retain();
    }

    /// Native httpx adapter. Request decoding and response encoding remain at
    /// the transport edge; the serverless handler receives its canonical
    /// transport-neutral request exactly once.
    pub fn handleHttpx(self: *ServerlessHttpServer, ctx: *httpx.Context) !httpx.Response {
        if (isSecretPath(ctx.request.uri.path)) {
            const method: http_common.Method = switch (ctx.request.method) {
                .GET => .GET,
                .PUT => .PUT,
                .DELETE => .DELETE,
                else => return ctx.status(405).text("method not allowed"),
            };
            var response = try self.handleSecrets(.{ .method = method, .uri = ctx.request.uri.path, .authorization = ctx.header("Authorization"), .body = (try ctx.body()) orelse "" });
            defer response.deinit(self.alloc);
            _ = ctx.status(response.status);
            try ctx.setHeader("Cache-Control", "no-store");
            try ctx.setHeader("Content-Type", response.content_type orelse "application/json");
            _ = ctx.response.body(response.body);
            return ctx.response.build();
        }
        const scope = self.handler.beginIngress(.{
            .method = switch (ctx.request.method) {
                .GET => .get,
                .PUT => .put,
                .DELETE => .delete,
                else => .post,
            },
            .path = ctx.request.uri.path,
            .deadline_ns = ctx.application_deadline_ns,
            .deadline_io = ctx.application_deadline_io,
            .cancellation = .{ .ptr = ctx, .is_cancelled_fn = contextCancelled },
        }) catch |err| return self.ingressFailure(err, null);
        defer if (scope) |value| value.release();
        if (scope) |value| {
            // This adapter is the first application callback. Transport-owned
            // Request bytes keep their separate framing/body capacity owner.
            if (ctx.request_memory != null or ctx.data != null or ctx.decoded_query_values.capacity != 0 or
                ctx.response.body_memory != null or ctx.response.body_owned or ctx.response.headers.entries.capacity != 0)
                return error.IngressMustPrecedePlanning;
            const alloc = value.owner.allocator();
            value.retain();
            ctx.request_memory = .{ .allocator = alloc, .ptr = value, .retain = IngressScope.retainOpaque, .release = IngressScope.releaseOpaque };
            ctx.allocator = alloc;
            ctx.response.allocator = alloc;
            ctx.response.headers.allocator = alloc;
            value.owner.retain();
            ctx.response.body_memory = .{ .allocator = alloc, .ptr = value.owner, .retain = retainResponseMemory, .release = releaseResponseMemory };
        }
        var result = self.handleHttpxAdmitted(ctx, scope) catch |err| blk: {
            if (scope) |value| if (value.owner.budget_exhausted.load(.acquire))
                break :blk try self.ingressFailure(error.AdmissionBytesExhausted, value);
            return err;
        };
        if (scope) |value| {
            std.debug.assert(value.previous_output == null);
            if (result.retirement) |retirement| value.previous_output = .{ .ptr = retirement.ptr, .release = retirement.release };
            value.retain();
            result.retirement = .{ .ptr = value, .release = IngressScope.releaseOutput };
        }
        return result;
    }

    fn contextCancelled(raw: *const anyopaque) bool {
        const ctx: *const httpx.Context = @ptrCast(@alignCast(raw));
        return ctx.isCancellationRequested();
    }

    fn ingressFailure(self: *ServerlessHttpServer, err: anyerror, scope: ?*IngressScope) !httpx.Response {
        if (scope) |value| if (value.write_started) {
            var response = httpx.Response.init(self.alloc, 503);
            errdefer response.deinit();
            try response.headers.append("Content-Type", "application/json");
            response.body = if (value.write_completed)
                "{\"reason\":\"committed_pending\",\"stage\":\"execution\",\"execution_started\":true,\"retryable\":false,\"write_outcome\":\"committed\"}"
            else
                "{\"reason\":\"write_outcome_unknown\",\"stage\":\"execution\",\"execution_started\":true,\"retryable\":false,\"write_outcome\":\"unknown\"}";
            return response;
        };
        const status: u16 = switch (err) {
            error.AdmissionFull, error.AdmissionBytesExhausted, error.AdmissionRequestTooLarge => 429,
            error.AdmissionClosed => 503,
            error.DeadlineExceeded => 504,
            else => return err,
        };
        var response = httpx.Response.init(self.alloc, status);
        errdefer response.deinit();
        try response.headers.append("Content-Type", "application/json");
        response.body = switch (err) {
            error.AdmissionFull => "{\"reason\":\"instance_busy\",\"stage\":\"admission\",\"execution_started\":false}",
            error.AdmissionClosed => "{\"reason\":\"draining\",\"stage\":\"admission\",\"execution_started\":false}",
            error.DeadlineExceeded => "{\"reason\":\"deadline_exceeded\",\"stage\":\"admission\",\"execution_started\":false}",
            else => if (scope != null and scope.?.execution_started)
                "{\"reason\":\"resource_exhausted\",\"stage\":\"execution\",\"execution_started\":true}"
            else
                "{\"reason\":\"resource_exhausted\",\"stage\":\"admission\",\"execution_started\":false}",
        };
        return response;
    }

    fn handleHttpxAdmitted(self: *ServerlessHttpServer, ctx: *httpx.Context, scope: ?*IngressScope) !httpx.Response {
        _ = self.cfg;
        const method: serverless_http_routes.HttpMethod = switch (ctx.request.method) {
            .GET => .get,
            .POST => .post,
            .PUT => .put,
            .DELETE => .delete,
            else => return try ctx.status(405).text("method not allowed"),
        };
        const body = (try ctx.body()) orelse "";
        var response = try self.handler.handle(.{
            .method = method,
            .path = ctx.request.uri.path,
            .body = body,
            .deadline_ns = ctx.application_deadline_ns,
            .deadline_io = ctx.application_deadline_io,
            .ingress = scope,
            .cancellation = .{
                .ptr = ctx,
                .is_cancelled_fn = struct {
                    fn call(raw: *const anyopaque) bool {
                        const request_context: *const httpx.Context = @ptrCast(@alignCast(raw));
                        return request_context.isCancellationRequested();
                    }
                }.call,
            },
        });
        defer response.deinit(self.alloc);

        if (response.memory_owner) |owner| {
            std.debug.assert(!ctx.response.body_owned);
            if (ctx.response.body_memory) |previous| previous.release(previous.ptr);
            owner.retain();
            ctx.response.body_memory = .{
                .allocator = owner.allocator(),
                .ptr = owner,
                .retain = retainResponseMemory,
                .release = releaseResponseMemory,
            };
        }
        _ = ctx.status(response.status);
        try ctx.setHeader("Content-Type", response.content_type);
        if (response.retry_after_seconds) |seconds| {
            var retry_after_buf: [10]u8 = undefined;
            const value = try std.fmt.bufPrint(&retry_after_buf, "{d}", .{seconds});
            try ctx.setHeader("Retry-After", value);
        }
        _ = ctx.response.body(response.body);
        if (response.memory_owner != null) {
            // The installed body allocator owns this exact allocation.
            ctx.response.body_owned = true;
            response.body = &.{};
        }
        return try ctx.response.build();
    }

    fn execute(ptr: *anyopaque, _: std.mem.Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
        const self: *ServerlessHttpServer = @ptrCast(@alignCast(ptr));
        return try self.handle(req);
    }
};

/// Caller-owned-I/O publication of an existing serverless protocol stack.
/// This is useful both for embedded deployments and deterministic VOPR worlds:
/// the handler and catalog remain owned by their production stack while every
/// listener, connection, request task, timeout, and shutdown wake borrows the
/// supplied `std.Io`.
pub const HttpxRuntime = struct {
    alloc: std.mem.Allocator,
    server: *httpx.Server,
    listener_task: *httpx.ListenerTask,
    base_uri: []u8,

    pub fn start(alloc: std.mem.Allocator, io: std.Io, target: *ServerlessHttpServer) !HttpxRuntime {
        const server = try alloc.create(httpx.Server);
        errdefer alloc.destroy(server);
        server.* = httpx.Server.initWithConfig(alloc, io, .{
            .host = "127.0.0.1",
            .port = 0,
            .header_read_timeout_ms = 0,
            .body_read_timeout_ms = 0,
            .response_write_timeout_ms = 0,
            .max_connections = 16,
            .max_request_tasks = 16,
            .borrow_http_runtime_io = true,
            .h1_disconnect_cancellation = .disabled,
        });
        errdefer server.deinit();
        server.global(httpx.Handler.bind(target, ServerlessHttpServer.handleHttpx));

        const listener_task = try alloc.create(httpx.ListenerTask);
        errdefer alloc.destroy(listener_task);
        listener_task.* = httpx.ListenerTask.init(server);
        try listener_task.start();
        errdefer {
            listener_task.requestStop();
            listener_task.join() catch {};
        }

        const address = server.boundAddress() orelse return error.ListenerNotStarted;
        const base_uri = try std.fmt.allocPrint(alloc, "http://{f}", .{address});
        return .{
            .alloc = alloc,
            .server = server,
            .listener_task = listener_task,
            .base_uri = base_uri,
        };
    }

    pub fn deinit(self: *HttpxRuntime) void {
        self.listener_task.requestStop();
        self.listener_task.join() catch |err|
            std.debug.panic("serverless HTTP listener failed: {s}", .{@errorName(err)});
        self.alloc.destroy(self.listener_task);
        self.server.deinit();
        self.alloc.destroy(self.server);
        self.alloc.free(self.base_uri);
        self.* = undefined;
    }
};

fn handlerIface(handler: anytype) Handler {
    const HandlerType = @TypeOf(handler);
    const Child = switch (@typeInfo(HandlerType)) {
        .pointer => |pointer| pointer.child,
        else => @compileError("ServerlessHttpServer.init expects a handler pointer"),
    };
    const Adapter = struct {
        fn handle(ptr: *anyopaque, req: serverless_http_types.HttpRequest) !serverless_http_types.HttpResponse {
            const typed: *Child = @ptrCast(@alignCast(ptr));
            return typed.handle(req);
        }

        fn beginIngress(ptr: *anyopaque, req: serverless_http_types.HttpRequest) !?*IngressScope {
            if (comptime @hasDecl(Child, "beginIngress")) {
                const typed: *Child = @ptrCast(@alignCast(ptr));
                return typed.beginIngress(req);
            }
            return null;
        }
    };
    return .{
        .ptr = handler,
        .vtable = &.{
            .handle = Adapter.handle,
            .begin_ingress = Adapter.beginIngress,
        },
    };
}

test "serverless http server adapts handler to common executor" {
    const alloc = std.testing.allocator;
    const FakeHandler = struct {
        alloc: std.mem.Allocator,
        last_method: ?serverless_http_routes.HttpMethod = null,
        last_path: ?[]const u8 = null,
        last_body: ?[]const u8 = null,

        fn handle(self: *@This(), req: serverless_http_types.HttpRequest) !serverless_http_types.HttpResponse {
            self.last_method = req.method;
            self.last_path = req.path;
            self.last_body = req.body;
            if (std.mem.eql(u8, req.path, "/status")) {
                return .{
                    .status = 200,
                    .content_type = try self.alloc.dupe(u8, "application/json"),
                    .body = try self.alloc.dupe(u8, "{\"validated\":true}"),
                };
            }
            if (std.mem.eql(u8, req.path, "/internal/v1/tables/docs/build")) {
                return .{
                    .status = 202,
                    .content_type = try self.alloc.dupe(u8, "application/json"),
                    .body = try self.alloc.dupe(u8, "{\"accepted\":true}"),
                };
            }
            return .{
                .status = 404,
                .content_type = try self.alloc.dupe(u8, "text/plain"),
                .body = try self.alloc.dupe(u8, "not found"),
            };
        }
    };

    var handler = FakeHandler{ .alloc = alloc };
    var server = ServerlessHttpServer.init(alloc, .{}, &handler);

    var status = try server.executor().execute(alloc, .{
        .method = .GET,
        .uri = "/status?probe=1",
    });
    defer status.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), status.status);
    try std.testing.expect(std.mem.indexOf(u8, status.body, "\"validated\":true") != null);
    try std.testing.expectEqual(serverless_http_routes.HttpMethod.get, handler.last_method.?);
    try std.testing.expectEqualStrings("/status", handler.last_path.?);

    var build = try server.executor().execute(alloc, .{
        .method = .POST,
        .uri = "/internal/v1/tables/docs/build",
    });
    defer build.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 202), build.status);
    try std.testing.expectEqual(serverless_http_routes.HttpMethod.post, handler.last_method.?);
    try std.testing.expectEqualStrings("/internal/v1/tables/docs/build", handler.last_path.?);
}

test "serverless http server passes through handler responses" {
    const alloc = std.testing.allocator;
    const FakeHandler = struct {
        alloc: std.mem.Allocator,
        last_method: ?serverless_http_routes.HttpMethod = null,
        last_path: ?[]const u8 = null,

        fn handle(self: *@This(), req: serverless_http_types.HttpRequest) !serverless_http_types.HttpResponse {
            self.last_method = req.method;
            self.last_path = req.path;
            return .{
                .status = 405,
                .content_type = try self.alloc.dupe(u8, "text/plain"),
                .body = try self.alloc.dupe(u8, "method not allowed"),
            };
        }
    };

    var handler = FakeHandler{ .alloc = alloc };
    var server = ServerlessHttpServer.init(alloc, .{}, &handler);

    var resp = try server.executor().execute(alloc, .{
        .method = .DELETE,
        .uri = "/tables/docs",
    });
    defer resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 405), resp.status);
    try std.testing.expectEqual(serverless_http_routes.HttpMethod.delete, handler.last_method.?);
    try std.testing.expectEqualStrings("/tables/docs", handler.last_path.?);
}

test "serverless http executor exposes one-request streaming" {
    const alloc = std.testing.allocator;
    const FakeHandler = struct {
        alloc: std.mem.Allocator,

        fn handle(self: *@This(), _: serverless_http_types.HttpRequest) !serverless_http_types.HttpResponse {
            return .{
                .status = 200,
                .content_type = try self.alloc.dupe(u8, "application/x-ndjson"),
                .body = try self.alloc.dupe(u8, "{\"_id\":\"a\"}\n"),
            };
        }
    };
    const Capture = struct {
        alloc: std.mem.Allocator,
        status: u16 = 0,
        body: std.ArrayListUnmanaged(u8) = .empty,

        fn start(raw: *anyopaque, _: std.mem.Allocator, response: http_common.StreamingResponse) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.status = response.status;
        }
        fn writeAll(raw: *anyopaque, bytes: []const u8) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.body.appendSlice(self.alloc, bytes);
        }
        fn flush(_: *anyopaque) anyerror!void {}
        fn writer(self: *@This()) http_common.StreamWriter {
            return .{ .ptr = self, .vtable = &.{
                .start = start,
                .write_all = writeAll,
                .flush = flush,
            } };
        }
    };

    var handler = FakeHandler{ .alloc = alloc };
    var server = ServerlessHttpServer.init(alloc, .{}, &handler);
    var capture = Capture{ .alloc = alloc };
    defer capture.body.deinit(alloc);
    try std.testing.expect((try server.executor().executeStream(
        alloc,
        .{ .method = .GET, .uri = "/internal/v1/groups/1/tables/docs/documents" },
        capture.writer(),
    )).?);
    try std.testing.expectEqual(@as(u16, 200), capture.status);
    try std.testing.expectEqualStrings("{\"_id\":\"a\"}\n", capture.body.items);
}

test "native serverless adapter preserves route path and retry metadata" {
    const alloc = std.testing.allocator;
    const FakeHandler = struct {
        alloc: std.mem.Allocator,
        observed_path: ?[]const u8 = null,

        fn handle(self: *@This(), req: serverless_http_types.HttpRequest) !serverless_http_types.HttpResponse {
            self.observed_path = req.path;
            return .{
                .status = 429,
                .content_type = try self.alloc.dupe(u8, "text/plain"),
                .body = try self.alloc.dupe(u8, "capacity exhausted"),
                .retry_after_seconds = 3,
            };
        }
    };

    var handler = FakeHandler{ .alloc = alloc };
    var server = ServerlessHttpServer.init(alloc, .{}, &handler);
    var request = try httpx.Request.init(alloc, .GET, "/query/search?profile=true");
    defer request.deinit();
    var ctx = httpx.Context.init(alloc, std.testing.io, &request);
    defer ctx.deinit();
    var response = try server.handleHttpx(&ctx);
    defer response.deinit();

    try std.testing.expectEqualStrings("/query/search", handler.observed_path.?);
    try std.testing.expectEqual(@as(u16, 429), response.status.code);
    try std.testing.expectEqualStrings("3", response.header("Retry-After").?);
}

test "native serverless adapter lends request cancellation to the handler" {
    const alloc = std.testing.allocator;
    const FakeHandler = struct {
        alloc: std.mem.Allocator,

        fn handle(self: *@This(), req: serverless_http_types.HttpRequest) !serverless_http_types.HttpResponse {
            try req.ensureActive();
            return .{
                .status = 200,
                .content_type = try self.alloc.dupe(u8, "text/plain"),
                .body = try self.alloc.dupe(u8, "ok"),
            };
        }
    };

    var cancelled = std.atomic.Value(bool).init(true);
    var handler = FakeHandler{ .alloc = alloc };
    var server = ServerlessHttpServer.init(alloc, .{}, &handler);
    var request = try httpx.Request.init(alloc, .GET, "/status");
    defer request.deinit();
    var ctx = httpx.Context.init(alloc, std.testing.io, &request);
    defer ctx.deinit();
    ctx.cancellation = &cancelled;

    try std.testing.expectError(error.Canceled, server.handleHttpx(&ctx));
}
