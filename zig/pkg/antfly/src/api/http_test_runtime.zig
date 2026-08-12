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

pub const Runtime = struct {
    alloc: std.mem.Allocator,
    io_impl: *std.Io.Threaded,
    server: *httpx.Server,
    handler: *httpx_handler.AntflyApiHandler,
    listener_task: *httpx.ListenerTask,
    active: bool = true,

    pub fn initStopped(alloc: std.mem.Allocator) Runtime {
        return .{
            .alloc = alloc,
            .io_impl = undefined,
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

        const handler = try alloc.create(httpx_handler.AntflyApiHandler);
        errdefer alloc.destroy(handler);
        handler.* = .{ .api_server = api };
        try handler.initRuntime(alloc);
        errdefer handler.deinitRuntime();

        const server = try alloc.create(httpx.Server);
        errdefer alloc.destroy(server);
        server.* = httpx.Server.initWithConfig(alloc, io_impl.io(), .{
            .host = "127.0.0.1",
            .port = 0,
            .request_timeout_ms = 30_000,
            .max_connections = 64,
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
            .io_impl = io_impl,
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
        self.io_impl.deinit();
        self.alloc.destroy(self.io_impl);
        self.* = undefined;
    }
};
