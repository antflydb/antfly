//! Middleware Example
//!
//! Demonstrates using middleware for cross-cutting concerns.

const std = @import("std");
const httpx = @import("httpx");

fn apiHandler(_: *httpx.Context) anyerror!httpx.Response {
    unreachable;
}

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;

    std.debug.print("=== Middleware Example ===\n\n", .{});

    var server = httpx.Server.init(allocator, init.io);
    defer server.deinit();

    try server.use(httpx.logger());
    try server.use(httpx.cors(.{
        .allowed_origins = &.{ "https://example.com", "https://app.example.com" },
        .allowed_methods = &.{ .GET, .POST, .PUT, .DELETE },
        .allow_credentials = true,
    }));
    try server.use(httpx.helmet());

    try server.get("/api/data", apiHandler);

    std.debug.print("Middleware Stack:\n", .{});
    std.debug.print("-----------------\n", .{});
    for (server.middleware.items, 0..) |mw, i| {
        std.debug.print("  {d}. {s}\n", .{ i + 1, mw.name });
    }

    std.debug.print("\nAvailable Middleware:\n", .{});
    std.debug.print("  - logger(): Request/response logging\n", .{});
    std.debug.print("  - cors(): Cross-Origin Resource Sharing\n", .{});
    std.debug.print("  - helmet(): Security headers\n", .{});
    std.debug.print("  - requestId(): Unique request ID\n", .{});

    std.debug.print("\nCORS Configuration:\n", .{});
    const cors_config = httpx.middleware.CorsConfig{
        .allowed_origins = &.{"*"},
        .allow_credentials = false,
        .max_age = 86400,
    };
    std.debug.print("  Max age: {d}s\n", .{cors_config.max_age});
    std.debug.print("  Allow credentials: {}\n", .{cors_config.allow_credentials});

}
