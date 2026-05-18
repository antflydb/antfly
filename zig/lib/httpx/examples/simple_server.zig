//! Simple HTTP Server Example
//!
//! Demonstrates creating a basic HTTP server with routing.

const std = @import("std");
const httpx = @import("httpx");

fn helloHandler(ctx: *httpx.Context) anyerror!httpx.Response {
    return ctx.text("Hello, World!");
}

fn jsonHandler(ctx: *httpx.Context) anyerror!httpx.Response {
    return ctx.json(.{
        .message = "Hello from httpx.zig!",
        .version = "1.0.0",
    });
}

fn userHandler(ctx: *httpx.Context) anyerror!httpx.Response {
    const user_id = ctx.param("id") orelse "unknown";
    var buf: [256]u8 = undefined;
    const msg = std.fmt.bufPrint(&buf, "User ID: {s}", .{user_id}) catch "error";
    return ctx.text(msg);
}

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;

    std.debug.print("=== Simple HTTP Server Example ===\n\n", .{});

    var server = httpx.Server.initWithConfig(allocator, init.io, .{
        .host = "127.0.0.1",
        .port = 8080,
        .max_connections = 1000,
        .keep_alive = true,
    });
    defer server.deinit();

    try server.get("/", helloHandler);
    try server.get("/api/status", jsonHandler);
    try server.get("/users/:id", userHandler);
    try server.post("/users", helloHandler);

    std.debug.print("Server Configuration:\n", .{});
    std.debug.print("  Host: {s}\n", .{server.config.host});
    std.debug.print("  Port: {d}\n", .{server.config.port});
    std.debug.print("  Max connections: {d}\n", .{server.config.max_connections});
    std.debug.print("  Keep-alive: {}\n", .{server.config.keep_alive});

    std.debug.print("\nRegistered routes:\n", .{});
    std.debug.print("  GET  /             -> helloHandler\n", .{});
    std.debug.print("  GET  /api/status   -> jsonHandler\n", .{});
    std.debug.print("  GET  /users/:id    -> userHandler\n", .{});
    std.debug.print("  POST /users        -> helloHandler\n", .{});

    std.debug.print("\nServer is running at http://127.0.0.1:8080\n", .{});
    std.debug.print("Try: /, /api/status, /users/123\n", .{});

    try server.listen();
}
