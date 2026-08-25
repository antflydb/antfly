// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0.

//! Real `httpx` transport ownership for integration and simulation tests.

const std = @import("std");
const httpx = @import("httpx");
const metadata_http_server = @import("http_server.zig");

pub const Runtime = struct {
    alloc: std.mem.Allocator,
    owned_io: ?*std.Io.Threaded,
    server: *httpx.Server,
    listener_task: *httpx.ListenerTask,

    pub fn startOwned(alloc: std.mem.Allocator, admin: *metadata_http_server.MetadataHttpServer) !Runtime {
        const io_impl = try alloc.create(std.Io.Threaded);
        errdefer alloc.destroy(io_impl);
        io_impl.* = std.Io.Threaded.init(alloc, .{});
        errdefer io_impl.deinit();
        var runtime = try startShared(alloc, io_impl.io(), admin);
        runtime.owned_io = io_impl;
        return runtime;
    }

    pub fn startShared(alloc: std.mem.Allocator, io: std.Io, admin: *metadata_http_server.MetadataHttpServer) !Runtime {
        const server = try alloc.create(httpx.Server);
        errdefer alloc.destroy(server);
        server.* = httpx.Server.initWithConfig(alloc, io, .{
            .host = "127.0.0.1",
            .port = 0,
            // The caller owns the complete transport runtime. In particular,
            // VOPR must not let metadata listeners silently allocate a
            // Threaded accept/connection pool outside the selected schedule.
            .header_read_timeout_ms = 0,
            .body_read_timeout_ms = 0,
            .response_write_timeout_ms = 0,
            .max_connections = 64,
            .borrow_http_runtime_io = true,
            .h1_disconnect_cancellation = .disabled,
        });
        errdefer server.deinit();
        try admin.registerRoutes(server);

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
            .listener_task = listener_task,
        };
    }

    pub fn baseUri(self: *const Runtime, alloc: std.mem.Allocator) ![]u8 {
        const address = self.server.boundAddress() orelse return error.AddressNotAvailable;
        return try std.fmt.allocPrint(alloc, "http://{f}", .{address});
    }

    pub fn deinit(self: *Runtime) void {
        self.listener_task.requestStop();
        self.listener_task.join() catch |err| std.debug.panic("metadata test listener failed: {s}", .{@errorName(err)});
        self.alloc.destroy(self.listener_task);
        self.server.deinit();
        self.alloc.destroy(self.server);
        if (self.owned_io) |io_impl| {
            io_impl.deinit();
            self.alloc.destroy(io_impl);
        }
        self.* = undefined;
    }
};
