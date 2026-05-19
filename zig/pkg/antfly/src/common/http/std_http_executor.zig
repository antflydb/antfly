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
const common = @import("http_common.zig");

pub const StdHttpExecutorConfig = struct {
    read_buffer_size: usize = 8 * 1024,
    write_buffer_size: usize = 1024,
    max_response_bytes: usize = 4 << 20,
    thread_stack_size: usize = 1 * 1024 * 1024,
    keep_alive: bool = false,
    /// Proactively retire pooled HTTP/1.1 connections before a server-side
    /// keep-alive cap closes them. 0 means unlimited client-side reuse.
    max_requests_per_connection: u32 = 32,
};

pub const StdHttpExecutor = struct {
    const IoOwner = enum {
        owned,
        shared,
    };

    alloc: std.mem.Allocator,
    cfg: StdHttpExecutorConfig,
    io_impl: *std.Io.Threaded,
    io_owner: IoOwner,
    client: std.http.Client,
    lifecycle_mutex: std.Io.Mutex,
    idle_cond: std.Io.Condition,
    closing: bool,
    in_flight: usize,
    reuse_mutex: std.Io.Mutex,
    requests_on_current_connection: u32,

    pub fn initInPlace(self: *StdHttpExecutor, alloc: std.mem.Allocator, cfg: StdHttpExecutorConfig) void {
        const io_impl = alloc.create(std.Io.Threaded) catch @panic("OOM");
        io_impl.* = std.Io.Threaded.init(alloc, .{ .stack_size = cfg.thread_stack_size });
        self.* = .{
            .alloc = alloc,
            .cfg = cfg,
            .io_impl = io_impl,
            .io_owner = .owned,
            .client = undefined,
            .lifecycle_mutex = .init,
            .idle_cond = .init,
            .closing = false,
            .in_flight = 0,
            .reuse_mutex = .init,
            .requests_on_current_connection = 0,
        };
        self.client = .{
            .allocator = alloc,
            .io = io_impl.io(),
            .read_buffer_size = cfg.read_buffer_size,
            .write_buffer_size = cfg.write_buffer_size,
        };
    }

    pub fn initSharedInPlace(self: *StdHttpExecutor, alloc: std.mem.Allocator, cfg: StdHttpExecutorConfig, io_impl: *std.Io.Threaded) void {
        self.* = .{
            .alloc = alloc,
            .cfg = cfg,
            .io_impl = io_impl,
            .io_owner = .shared,
            .client = undefined,
            .lifecycle_mutex = .init,
            .idle_cond = .init,
            .closing = false,
            .in_flight = 0,
            .reuse_mutex = .init,
            .requests_on_current_connection = 0,
        };
        self.client = .{
            .allocator = alloc,
            .io = io_impl.io(),
            .read_buffer_size = cfg.read_buffer_size,
            .write_buffer_size = cfg.write_buffer_size,
        };
    }

    pub fn init(alloc: std.mem.Allocator, cfg: StdHttpExecutorConfig) StdHttpExecutor {
        var self: StdHttpExecutor = undefined;
        self.initInPlace(alloc, cfg);
        return self;
    }

    pub fn deinit(self: *StdHttpExecutor) void {
        const io = self.io_impl.io();
        self.lifecycle_mutex.lockUncancelable(io);
        self.closing = true;
        while (self.in_flight != 0) {
            self.idle_cond.waitUncancelable(io, &self.lifecycle_mutex);
        }
        self.lifecycle_mutex.unlock(io);

        self.client.deinit();
        if (self.io_owner == .owned) {
            self.io_impl.deinit();
            self.alloc.destroy(self.io_impl);
        }
        self.* = undefined;
    }

    pub fn executor(self: *StdHttpExecutor) common.RequestExecutor {
        return .{
            .ptr = self,
            .vtable = &.{
                .execute = execute,
            },
        };
    }

    fn execute(ptr: *anyopaque, alloc: std.mem.Allocator, req: common.HttpRequest) !common.HttpResponse {
        const self: *StdHttpExecutor = @ptrCast(@alignCast(ptr));
        try self.beginRequest();
        defer self.endRequest();

        const uri = try std.Uri.parse(req.uri);
        const method = switch (req.method) {
            .GET => std.http.Method.GET,
            .POST => std.http.Method.POST,
            .PUT => std.http.Method.PUT,
            .DELETE => std.http.Method.DELETE,
        };

        var extra_headers_buf: [1]std.http.Header = undefined;
        const extra_headers: []const std.http.Header = if (req.content_type != null)
            extra_headers_buf[0..1]
        else
            &.{};
        if (req.content_type) |content_type| {
            extra_headers_buf[0] = .{
                .name = "content-type",
                .value = content_type,
            };
        }

        const request_keep_alive = self.reserveRequestKeepAlive();
        var request = try std.http.Client.request(&self.client, method, uri, .{
            .extra_headers = extra_headers,
            .keep_alive = request_keep_alive,
        });
        defer request.deinit();

        if (req.body.len > 0 or method.requestHasBody()) {
            request.transfer_encoding = .{ .content_length = req.body.len };
            var body_buffer: [16 * 1024]u8 = undefined;
            var body_writer = try request.sendBodyUnflushed(&body_buffer);
            if (req.body.len > 0) {
                try body_writer.writer.writeAll(req.body);
            }
            try body_writer.end();
            try request.connection.?.flush();
        } else {
            try request.sendBodiless();
        }

        var response = try request.receiveHead(&.{});
        const content_type = if (response.head.content_type) |value|
            try alloc.dupe(u8, value)
        else
            null;
        errdefer if (content_type) |value| alloc.free(value);

        var header_count: usize = 0;
        var header_it = response.head.iterateHeaders();
        while (header_it.next()) |header| {
            if (std.ascii.eqlIgnoreCase(header.name, "content-type")) continue;
            header_count += 1;
        }
        var headers: []common.Header = if (header_count > 0)
            try alloc.alloc(common.Header, header_count)
        else
            @constCast((&[_]common.Header{})[0..]);
        var header_index: usize = 0;
        errdefer {
            for (headers[0..header_index]) |*header| header.deinit(alloc);
            if (header_count > 0) alloc.free(headers);
        }

        header_it = response.head.iterateHeaders();
        while (header_it.next()) |header| {
            if (std.ascii.eqlIgnoreCase(header.name, "content-type")) continue;
            headers[header_index] = .{
                .name = try alloc.dupe(u8, header.name),
                .value = try alloc.dupe(u8, header.value),
            };
            header_index += 1;
        }

        var transfer_buffer: [512]u8 = undefined;
        const body = try response.reader(&transfer_buffer).allocRemaining(alloc, .limited(self.cfg.max_response_bytes));

        const connection_closing = if (request.connection) |connection| connection.closing else true;
        self.recordCompletedRequest(request_keep_alive, connection_closing);
        return .{
            .status = @intFromEnum(response.head.status),
            .content_type = content_type,
            .headers = headers,
            .body = body,
        };
    }

    fn beginRequest(self: *StdHttpExecutor) !void {
        const io = self.io_impl.io();
        self.lifecycle_mutex.lockUncancelable(io);
        defer self.lifecycle_mutex.unlock(io);

        if (self.closing) return error.ExecutorShuttingDown;
        self.in_flight += 1;
    }

    fn endRequest(self: *StdHttpExecutor) void {
        const io = self.io_impl.io();
        self.lifecycle_mutex.lockUncancelable(io);
        defer self.lifecycle_mutex.unlock(io);

        self.in_flight -= 1;
        if (self.in_flight == 0) self.idle_cond.broadcast(io);
    }

    fn reserveRequestKeepAlive(self: *StdHttpExecutor) bool {
        const io = self.io_impl.io();
        self.reuse_mutex.lockUncancelable(io);
        defer self.reuse_mutex.unlock(io);

        if (!self.cfg.keep_alive) return false;
        const max_requests = self.cfg.max_requests_per_connection;
        if (max_requests == 0) return true;
        if (self.requests_on_current_connection + 1 >= max_requests) {
            self.requests_on_current_connection = 0;
            return false;
        }
        self.requests_on_current_connection += 1;
        return true;
    }

    fn recordCompletedRequest(self: *StdHttpExecutor, request_keep_alive: bool, connection_closing: bool) void {
        const io = self.io_impl.io();
        self.reuse_mutex.lockUncancelable(io);
        defer self.reuse_mutex.unlock(io);

        if (!request_keep_alive) {
            self.requests_on_current_connection = 0;
            return;
        }
        if (connection_closing) {
            self.requests_on_current_connection = 0;
            return;
        }
    }
};

test "std http executor module compiles" {
    _ = StdHttpExecutorConfig;
    _ = StdHttpExecutor;
}

test "std http executor retires pooled connection before configured cap" {
    var executor = StdHttpExecutor.init(std.testing.allocator, .{
        .keep_alive = true,
        .max_requests_per_connection = 3,
    });
    defer executor.deinit();

    try std.testing.expect(executor.reserveRequestKeepAlive());
    executor.recordCompletedRequest(true, false);
    try std.testing.expectEqual(@as(u32, 1), executor.requests_on_current_connection);

    try std.testing.expect(executor.reserveRequestKeepAlive());
    executor.recordCompletedRequest(true, false);
    try std.testing.expectEqual(@as(u32, 2), executor.requests_on_current_connection);

    try std.testing.expect(!executor.reserveRequestKeepAlive());
    executor.recordCompletedRequest(false, true);
    try std.testing.expectEqual(@as(u32, 0), executor.requests_on_current_connection);
}

test "std http executor resets reuse count when server closes connection" {
    var executor = StdHttpExecutor.init(std.testing.allocator, .{ .max_requests_per_connection = 32 });
    defer executor.deinit();

    executor.requests_on_current_connection = 7;

    executor.recordCompletedRequest(true, true);
    try std.testing.expectEqual(@as(u32, 0), executor.requests_on_current_connection);
}
