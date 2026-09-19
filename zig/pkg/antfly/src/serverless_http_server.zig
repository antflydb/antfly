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

const secrets = @import("common/secrets.zig");
pub const ServerlessHttpServerConfig = struct {
    secret_store: ?*secrets.FileStore = null,
    // Explicit credential required even on otherwise unauthenticated serverless
    // deployments. TLS terminates at the deployment's trusted ingress.
    secret_admin_token: ?[]const u8 = null,
};

pub const Handler = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    const VTable = struct {
        handle: *const fn (*anyopaque, serverless_http_types.HttpRequest) anyerror!serverless_http_types.HttpResponse,
    };

    pub fn handle(self: Handler, req: serverless_http_types.HttpRequest) !serverless_http_types.HttpResponse {
        return self.vtable.handle(self.ptr, req);
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
        const headers = try alloc.alloc(http_common.RequestHeader, response.headers.len);
        defer alloc.free(headers);
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
        });
        defer resp.deinit(self.alloc);

        var response = http_common.HttpResponse{
            .status = resp.status,
            .owner_allocator = self.alloc,
            .content_type = try self.alloc.dupe(u8, resp.content_type),
            .body = try self.alloc.dupe(u8, resp.body),
        };
        errdefer response.deinit(self.alloc);
        if (resp.retry_after_seconds) |seconds| {
            const value = try std.fmt.allocPrint(self.alloc, "{d}", .{seconds});
            defer self.alloc.free(value);
            const name_owned = try self.alloc.dupe(u8, "Retry-After");
            errdefer self.alloc.free(name_owned);
            const value_owned = try self.alloc.dupe(u8, value);
            errdefer self.alloc.free(value_owned);
            const headers = try self.alloc.alloc(http_common.Header, 1);
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
            return ctx.text(response.body);
        }
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

        _ = ctx.status(response.status);
        try ctx.setHeader("Content-Type", response.content_type);
        if (response.retry_after_seconds) |seconds| {
            var retry_after_buf: [10]u8 = undefined;
            const value = try std.fmt.bufPrint(&retry_after_buf, "{d}", .{seconds});
            try ctx.setHeader("Retry-After", value);
        }
        _ = ctx.response.body(response.body);
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
    };
    return .{
        .ptr = handler,
        .vtable = &.{
            .handle = Adapter.handle,
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
