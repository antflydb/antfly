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

        var resp = try self.handler.handle(.{
            .method = method,
            .path = req.uri,
            .body = req.body,
        });
        defer resp.deinit(self.alloc);

        return .{
            .status = resp.status,
            .content_type = try self.alloc.dupe(u8, resp.content_type),
            .body = try self.alloc.dupe(u8, resp.body),
        };
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
        .uri = "/status",
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
