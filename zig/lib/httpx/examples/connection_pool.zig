//! Connection Pool Example
//!
//! Demonstrates HTTP connection pooling for reusing connections.

const std = @import("std");
const httpx = @import("httpx");
const common = httpx.common;

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;

    std.debug.print("=== Connection Pool Example ===\n\n", .{});

    var pool = httpx.pool.ConnectionPool.initWithConfig(allocator, init.io, .{
        .max_connections = 20,
        .max_per_host = 5,
        .idle_timeout_ms = 60_000,
        .max_requests_per_connection = 1000,
    }, {});
    defer pool.deinit();

    std.debug.print("Pool Configuration:\n", .{});
    std.debug.print("  Max connections: {d}\n", .{pool.config.max_connections});
    std.debug.print("  Max per host: {d}\n", .{pool.config.max_per_host});
    std.debug.print("  Idle timeout: {d}ms\n", .{pool.config.idle_timeout_ms});
    std.debug.print("  Max requests/connection: {d}\n", .{pool.config.max_requests_per_connection});

    std.debug.print("\nPool Statistics:\n", .{});
    std.debug.print("  Total connections: {d}\n", .{pool.totalCount()});
    std.debug.print("  Active connections: {d}\n", .{pool.activeCount()});
    std.debug.print("  Idle connections: {d}\n", .{pool.idleCount()});

    std.debug.print("\nConnection health checking:\n", .{});
    const now = common.milliTimestamp(init.io);
    const conn = httpx.pool.Connection{
        .socket = undefined,
        .host = "api.example.com",
        .port = 443,
        .created_at = now,
        .last_used = now,
    };
    std.debug.print("  Connection to {s}:{d}\n", .{ conn.host, conn.port });
    std.debug.print("  Is healthy (60s timeout): {}\n", .{conn.isHealthy(60_000, now)});

    std.debug.print("\nDemo complete!\n", .{});
}
