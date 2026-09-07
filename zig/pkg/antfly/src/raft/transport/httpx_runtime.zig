// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Caller-owned `std.Io` publication for the production Raft HTTP handler.
//! This is used by deterministic cluster compositions and is deliberately
//! independent of VOPR: Threaded and other `std.Io` implementations can use
//! the same adapter.

const std = @import("std");
const httpx = @import("httpx");
const common = @import("http_common.zig");
const routes = @import("routes.zig");

pub const Runtime = struct {
    alloc: std.mem.Allocator,
    handler: *Handler,
    server: *httpx.Server,
    listener: *httpx.ListenerTask,
    base_uri: []u8,

    pub fn start(
        alloc: std.mem.Allocator,
        io: std.Io,
        target: common.RequestExecutor,
    ) !Runtime {
        return startAt(alloc, io, target, "127.0.0.1", 0);
    }

    /// Publish the production Raft handler at a caller-owned stable address.
    /// Process restart compositions use this to rebind the advertised service
    /// endpoint instead of inventing a metadata topology change.
    pub fn startAt(
        alloc: std.mem.Allocator,
        io: std.Io,
        target: common.RequestExecutor,
        host: []const u8,
        port: u16,
    ) !Runtime {
        const server = try alloc.create(httpx.Server);
        errdefer alloc.destroy(server);
        server.* = httpx.Server.initWithConfig(alloc, io, .{
            .host = host,
            .port = port,
            .header_read_timeout_ms = 0,
            .body_read_timeout_ms = 0,
            .response_write_timeout_ms = 0,
            .max_connections = 32,
            .borrow_http_runtime_io = true,
            .h1_disconnect_cancellation = .disabled,
        });
        errdefer server.deinit();

        const handler = try alloc.create(Handler);
        errdefer alloc.destroy(handler);
        handler.* = .{ .target = target };
        var result = Runtime{
            .alloc = alloc,
            .handler = handler,
            .server = server,
            .listener = undefined,
            .base_uri = undefined,
        };
        try server.get(routes.Routes.health, httpx.Handler.bind(handler, Handler.proxy));
        try server.get(routes.Routes.capabilities, httpx.Handler.bind(handler, Handler.proxy));
        try server.post(routes.Routes.raft_batch, httpx.Handler.bind(handler, Handler.proxy));
        try server.post(routes.Routes.snapshot_upload ++ "/:snapshot_id", httpx.Handler.bind(handler, Handler.proxy));
        try server.get(routes.Routes.snapshot_fetch ++ "/:snapshot_id", httpx.Handler.bind(handler, Handler.proxy));

        const listener = try alloc.create(httpx.ListenerTask);
        errdefer alloc.destroy(listener);
        listener.* = httpx.ListenerTask.init(server);
        try listener.start();
        errdefer {
            listener.requestStop();
            listener.join() catch {};
        }
        result.listener = listener;
        const address = server.boundAddress() orelse return error.AddressNotAvailable;
        result.base_uri = try std.fmt.allocPrint(alloc, "http://{f}", .{address});
        return result;
    }

    pub fn deinit(self: *Runtime) void {
        self.listener.requestStop();
        self.listener.join() catch |err|
            std.debug.panic("Raft HTTP listener failed: {s}", .{@errorName(err)});
        self.alloc.destroy(self.listener);
        self.server.deinit();
        self.alloc.destroy(self.server);
        self.alloc.destroy(self.handler);
        self.alloc.free(self.base_uri);
        self.* = undefined;
    }

    /// Publish listener shutdown without joining or destroying its owner.
    /// Deployment teardown uses this before driving a shared deterministic
    /// scheduler to quiescence.
    pub fn requestStop(self: *Runtime) void {
        self.listener.requestStop();
    }

    pub fn setTarget(self: *Runtime, target: common.RequestExecutor) void {
        self.handler.target = target;
    }

    pub fn requestCount(self: *const Runtime) u64 {
        return self.handler.request_count.load(.acquire);
    }
};

const Handler = struct {
    target: common.RequestExecutor,
    request_count: std.atomic.Value(u64) = .init(0),

    fn proxy(self: *Handler, ctx: *httpx.Context) !httpx.Response {
        _ = self.request_count.fetchAdd(1, .monotonic);
        const body = (try ctx.body()) orelse "";
        var request_headers: [4]common.RequestHeader = undefined;
        var header_count: usize = 0;
        inline for ([_][]const u8{
            "x-antfly-raft-group-id",
            "x-antfly-raft-from-node-id",
            "x-antfly-raft-to-node-id",
            "x-antfly-raft-term",
        }) |name| if (ctx.header(name)) |value| {
            request_headers[header_count] = .{ .name = name, .value = value };
            header_count += 1;
        };
        var response = try self.target.execute(ctx.allocator, .{
            .method = switch (ctx.request.method) {
                .GET => .GET,
                .POST => .POST,
                .PUT => .PUT,
                .DELETE => .DELETE,
                else => return ctx.status(405).text("method not allowed"),
            },
            .uri = ctx.request.uri.path,
            .headers = request_headers[0..header_count],
            .authorization = ctx.header("authorization"),
            .content_type = ctx.header("content-type"),
            .body = body,
        });
        defer response.deinit(ctx.allocator);
        _ = ctx.status(response.status);
        if (response.content_type) |content_type| try ctx.setHeader("content-type", content_type);
        for (response.headers) |header| try ctx.setHeader(header.name, header.value);
        _ = ctx.response.body(response.body);
        return ctx.response.build();
    }
};

/// Converts the relative URI supplied by the fault-policy executor into a
/// request to one concrete wire listener.
pub const AbsoluteExecutor = struct {
    alloc: std.mem.Allocator,
    base_uri: []const u8,
    inner: common.RequestExecutor,

    pub fn executor(self: *AbsoluteExecutor) common.RequestExecutor {
        return .{ .ptr = self, .vtable = &.{ .execute = execute } };
    }

    fn execute(
        ptr: *anyopaque,
        response_alloc: std.mem.Allocator,
        req: common.HttpRequest,
    ) !common.HttpResponse {
        const self: *AbsoluteExecutor = @ptrCast(@alignCast(ptr));
        const uri = try routes.Routes.join(self.alloc, self.base_uri, req.uri);
        defer self.alloc.free(uri);
        var wire_request = req;
        wire_request.uri = uri;
        return try self.inner.execute(response_alloc, wire_request);
    }
};
