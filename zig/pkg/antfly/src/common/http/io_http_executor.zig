// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0.

//! HTTP request execution over a caller-owned `std.Io`.
//!
//! Unlike `StdHttpExecutor`, this owner does not create or depend on
//! `std.Io.Threaded`. Production, integration, and VOPR callers can therefore
//! put clients and listeners under the same scheduler and clock.

const std = @import("std");
const httpx = @import("httpx");
const common = @import("http_common.zig");

pub const IoHttpExecutorConfig = struct {
    max_response_bytes: usize = 4 << 20,
    connect_timeout_ms: u64 = 30_000,
    read_timeout_ms: u64 = 30_000,
    write_timeout_ms: u64 = 30_000,
    keep_alive: bool = false,
    pool_max_connections: u32 = 20,
    pool_max_per_host: u32 = 5,
};

pub const IoHttpExecutor = struct {
    alloc: std.mem.Allocator,
    io: std.Io,
    client: httpx.Client,

    pub fn init(
        alloc: std.mem.Allocator,
        io: std.Io,
        cfg: IoHttpExecutorConfig,
    ) IoHttpExecutor {
        const client_config: httpx.ClientConfig = .{
            .timeouts = .{
                .connect_ms = cfg.connect_timeout_ms,
                .read_ms = cfg.read_timeout_ms,
                .write_ms = cfg.write_timeout_ms,
            },
            .retry_policy = .{ .max_retries = 0 },
            .redirect_policy = .{ .follow_redirects = false },
            .max_response_size = cfg.max_response_bytes,
            .keep_alive = cfg.keep_alive,
            .pool_max_connections = cfg.pool_max_connections,
            .pool_max_per_host = cfg.pool_max_per_host,
            .cache_resolved_addresses = true,
            .cancel_in_flight_on_shutdown = true,
        };
        return .{
            .alloc = alloc,
            .io = io,
            .client = httpx.Client.initWithConfig(alloc, io, client_config),
        };
    }

    pub fn deinit(self: *IoHttpExecutor) void {
        self.client.deinit();
        self.* = undefined;
    }

    pub fn activeRequestCount(self: *const IoHttpExecutor) usize {
        return self.client.activeRequestCount();
    }

    pub fn beginShutdown(self: *IoHttpExecutor) void {
        self.client.beginShutdown();
    }

    pub fn drainShutdown(self: *IoHttpExecutor) void {
        self.client.drainShutdown();
    }

    pub fn executor(self: *IoHttpExecutor) common.RequestExecutor {
        return .{
            .ptr = self,
            .vtable = &.{ .execute = execute },
        };
    }

    fn execute(
        ptr: *anyopaque,
        response_alloc: std.mem.Allocator,
        req: common.HttpRequest,
    ) !common.HttpResponse {
        const self: *IoHttpExecutor = @ptrCast(@alignCast(ptr));
        if (req.delivery_tracker) |tracker| tracker.markNotSent();
        if (req.timeout_ms != null and req.timeout_ms.? == 0) return error.Timeout;
        if (req.cancellation) |cancellation| {
            if (cancellation.isCancelled()) return error.Cancelled;
        }

        const extra_count = @as(usize, @intFromBool(req.content_type != null)) +
            @as(usize, @intFromBool(req.authorization != null));
        const header_pairs = try self.alloc.alloc([2][]const u8, req.headers.len + extra_count);
        defer self.alloc.free(header_pairs);
        var header_index: usize = 0;
        if (req.content_type) |content_type| {
            header_pairs[header_index] = .{ "content-type", content_type };
            header_index += 1;
        }
        if (req.authorization) |authorization| {
            header_pairs[header_index] = .{ "authorization", authorization };
            header_index += 1;
        }
        for (req.headers) |header| {
            header_pairs[header_index] = .{ header.name, header.value };
            header_index += 1;
        }

        if (req.delivery_tracker) |tracker| tracker.markMayHaveBeenSent();
        var response = try self.client.request(switch (req.method) {
            .GET => .GET,
            .POST => .POST,
            .PUT => .PUT,
            .DELETE => .DELETE,
        }, req.uri, .{
            .headers = header_pairs,
            .body = if (req.body.len == 0) null else req.body,
            .timeout_ms = if (req.timeout_ms) |timeout_ms| timeout_ms else null,
            .cancellation = if (req.cancellation) |cancellation| blk: {
                const token = cancellation.token();
                break :blk httpx.CancellationToken.fromCallback(token.ptr, token.is_cancelled_fn);
            } else null,
            .follow_redirects = false,
        });
        defer response.deinit();

        const content_type = if (response.contentType()) |value|
            try response_alloc.dupe(u8, value)
        else
            null;
        errdefer if (content_type) |value| response_alloc.free(value);

        var header_count: usize = 0;
        for (response.headers.iterator()) |header| {
            if (!std.ascii.eqlIgnoreCase(header.name, "content-type")) header_count += 1;
        }
        const headers: []common.Header = if (header_count == 0)
            @constCast((&[_]common.Header{})[0..])
        else
            try response_alloc.alloc(common.Header, header_count);
        var copied_headers: usize = 0;
        errdefer {
            for (headers[0..copied_headers]) |*header| header.deinit(response_alloc);
            if (header_count > 0) response_alloc.free(headers);
        }
        for (response.headers.iterator()) |header| {
            if (std.ascii.eqlIgnoreCase(header.name, "content-type")) continue;
            const owned_name = try response_alloc.dupe(u8, header.name);
            errdefer response_alloc.free(owned_name);
            headers[copied_headers] = .{
                .name = owned_name,
                .value = try response_alloc.dupe(u8, header.value),
            };
            copied_headers += 1;
        }

        const body = if (response.body) |value|
            if (value.len == 0)
                @constCast((&[_]u8{})[0..])
            else
                try response_alloc.dupe(u8, value)
        else
            @constCast((&[_]u8{})[0..]);
        return .{
            .status = response.status.code,
            .content_type = content_type,
            .headers = headers,
            .body = body,
        };
    }
};

test "I/O HTTP executor rejects an already-cancelled request without transport work" {
    var cancelled = std.atomic.Value(bool).init(true);
    const cancellation = common.RequestCancellation{ .borrowed = &cancelled };
    var tracker: common.RequestDeliveryTracker = .{};
    var executor = IoHttpExecutor.init(std.testing.allocator, std.testing.io, .{});
    defer executor.deinit();

    try std.testing.expectError(error.Cancelled, executor.executor().execute(std.testing.allocator, .{
        .method = .GET,
        .uri = "http://127.0.0.1:1/never-sent",
        .cancellation = &cancellation,
        .delivery_tracker = &tracker,
    }));
    try std.testing.expectEqual(.not_sent, tracker.load());
}
