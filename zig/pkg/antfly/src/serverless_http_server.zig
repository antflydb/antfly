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

pub const ServerlessHttpServerConfig = struct {};

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
            },
        };
    }

    pub fn handle(self: *ServerlessHttpServer, req: http_common.HttpRequest) !http_common.HttpResponse {
        _ = self.cfg;
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

    /// Native httpx adapter. Request decoding and response encoding remain at
    /// the transport edge; the serverless handler receives its canonical
    /// transport-neutral request exactly once.
    pub fn handleHttpx(self: *ServerlessHttpServer, ctx: *httpx.Context) !httpx.Response {
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
