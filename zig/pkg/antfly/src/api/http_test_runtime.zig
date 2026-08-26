// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0.

//! Real public `httpx` transport ownership for API integration tests.
//!
//! Stateful fixtures use the same generated/contextual registrar and owned
//! listener-task lifecycle as production. They must not revive
//! `ApiHttpServer.executor()` merely to obtain an ephemeral test port.

const std = @import("std");
const httpx = @import("httpx");
const http_server = @import("http_server.zig");
const httpx_handler = @import("httpx_handler.zig");
const http_common = @import("../raft/transport/http_common.zig");
const std_http_executor = @import("../raft/transport/std_http_executor.zig");

/// Exercise one compact request through the production `httpx` registrar and
/// listener. Generated data paths are canonicalized below `/db/v1`; root
/// probes and contextual protocols retain their Kubernetes/protocol paths.
pub fn executeOnce(
    alloc: std.mem.Allocator,
    api: *http_server.ApiHttpServer,
    req: http_common.HttpRequest,
) !http_common.HttpResponse {
    var runtime = try Runtime.startOwned(alloc, api);
    defer runtime.deinit();

    const base_uri = try runtime.baseUri(alloc);
    defer alloc.free(base_uri);
    const canonical_path = try canonicalRequestPath(alloc, req.uri);
    defer alloc.free(canonical_path);
    const absolute_uri = try std.fmt.allocPrint(alloc, "{s}{s}", .{ base_uri, canonical_path });
    defer alloc.free(absolute_uri);

    var executor = std_http_executor.StdHttpExecutor.init(alloc, .{});
    defer executor.deinit();
    return executor.executor().execute(alloc, .{
        .method = req.method,
        .uri = absolute_uri,
        .headers = req.headers,
        .source_node_id = req.source_node_id,
        .authorization = req.authorization,
        .content_type = req.content_type,
        .timeout_ms = req.timeout_ms,
        .body = req.body,
        .cancellation = req.cancellation,
        .delivery_tracker = req.delivery_tracker,
    });
}

fn canonicalRequestPath(alloc: std.mem.Allocator, uri: []const u8) ![]u8 {
    if (std.mem.startsWith(u8, uri, "/db/v1") or isRootRoute(uri)) return alloc.dupe(u8, uri);
    return std.fmt.allocPrint(alloc, "/db/v1{s}", .{uri});
}

fn isRootRoute(uri: []const u8) bool {
    const root_prefixes = [_][]const u8{
        "/healthz",
        "/readyz",
        "/auth/v1",
        "/internal/v1",
        "/agents/retrieval",
        "/mcp/v1",
        "/a2a",
        "/.well-known/",
        "/ard/v1",
        "/extensions/v1",
        "/agents/v1/extensions/",
        "/admin/",
        "/ha/",
    };
    for (root_prefixes) |prefix| {
        if (std.mem.startsWith(u8, uri, prefix)) return true;
    }
    return false;
}

pub const Runtime = struct {
    alloc: std.mem.Allocator,
    owned_io: ?*std.Io.Threaded,
    server: *httpx.Server,
    handler: *httpx_handler.AntflyApiHandler,
    listener_task: *httpx.ListenerTask,
    active: bool = true,

    pub fn initStopped(alloc: std.mem.Allocator) Runtime {
        return .{
            .alloc = alloc,
            .owned_io = null,
            .server = undefined,
            .handler = undefined,
            .listener_task = undefined,
            .active = false,
        };
    }

    pub fn startOwned(alloc: std.mem.Allocator, api: *http_server.ApiHttpServer) !Runtime {
        const io_impl = try alloc.create(std.Io.Threaded);
        errdefer alloc.destroy(io_impl);
        io_impl.* = std.Io.Threaded.init(alloc, .{});
        errdefer io_impl.deinit();
        var runtime = try startShared(alloc, io_impl.io(), api);
        runtime.owned_io = io_impl;
        return runtime;
    }

    /// Starts the production public HTTP listener on caller-owned I/O. This
    /// keeps integration fixtures usable with Threaded while allowing VOPR to
    /// place every listener, client, timeout, and shutdown task under one
    /// deterministic scheduler.
    pub fn startShared(
        alloc: std.mem.Allocator,
        io: std.Io,
        api: *http_server.ApiHttpServer,
    ) !Runtime {
        const handler = try alloc.create(httpx_handler.AntflyApiHandler);
        errdefer alloc.destroy(handler);
        handler.* = .{ .api_server = api };
        try handler.initRuntime(alloc);
        errdefer handler.deinitRuntime();

        const server = try alloc.create(httpx.Server);
        errdefer alloc.destroy(server);
        server.* = httpx.Server.initWithConfig(alloc, io, .{
            .host = "127.0.0.1",
            .port = 0,
            // Test owners drive failure timing explicitly. Keeping transport
            // deadlines disabled prevents a deterministic scheduler from
            // treating an arbitrary jump to a production wall-clock timeout
            // as part of an otherwise clean history.
            .header_read_timeout_ms = 0,
            .body_read_timeout_ms = 0,
            .response_write_timeout_ms = 0,
            .max_connections = 64,
            .borrow_http_runtime_io = true,
            .h1_disconnect_cancellation = .disabled,
        });
        errdefer server.deinit();
        try handler.registerRoutes(server);

        const listener_task = try alloc.create(httpx.ListenerTask);
        errdefer alloc.destroy(listener_task);
        listener_task.* = httpx.ListenerTask.init(server);
        try listener_task.start();
        errdefer {
            listener_task.requestStop();
            listener_task.join() catch {};
        }

        return .{
            .alloc = alloc,
            .owned_io = null,
            .server = server,
            .handler = handler,
            .listener_task = listener_task,
        };
    }

    pub fn baseUri(self: *const Runtime, alloc: std.mem.Allocator) ![]u8 {
        const address = self.server.boundAddress() orelse return error.AddressNotAvailable;
        return try std.fmt.allocPrint(alloc, "http://{f}", .{address});
    }

    pub fn deinit(self: *Runtime) void {
        if (!self.active) {
            self.* = undefined;
            return;
        }
        self.listener_task.requestStop();
        self.listener_task.join() catch |err| std.debug.panic("public API test listener failed: {s}", .{@errorName(err)});
        self.alloc.destroy(self.listener_task);
        self.server.deinit();
        self.alloc.destroy(self.server);
        self.handler.deinitRuntime();
        self.alloc.destroy(self.handler);
        if (self.owned_io) |io_impl| {
            io_impl.deinit();
            self.alloc.destroy(io_impl);
        }
        self.* = undefined;
    }

    pub fn requestStop(self: *Runtime) void {
        if (self.active) self.listener_task.requestStop();
    }
};
