//! Router Example
//!
//! Demonstrates pattern-based routing with path parameters.

const std = @import("std");
const httpx = @import("httpx");

fn indexHandler(_: *httpx.Context) anyerror!httpx.Response {
    unreachable;
}

pub fn main() !void {
    const allocator = std.heap.smp_allocator;

    std.debug.print("=== Router Example ===\n\n", .{});

    var router = httpx.Router.init(allocator);
    defer router.deinit();

    try router.add(.GET, "/", indexHandler);
    try router.add(.GET, "/users", indexHandler);
    try router.add(.GET, "/users/:id", indexHandler);
    try router.add(.GET, "/users/:userId/posts/:postId", indexHandler);
    try router.add(.POST, "/users", indexHandler);
    try router.add(.PUT, "/users/:id", indexHandler);
    try router.add(.DELETE, "/users/:id", indexHandler);
    try router.add(.GET, "/static/*", indexHandler);

    std.debug.print("Registered Routes:\n", .{});
    std.debug.print("  GET    /\n", .{});
    std.debug.print("  GET    /users\n", .{});
    std.debug.print("  GET    /users/:id\n", .{});
    std.debug.print("  GET    /users/:userId/posts/:postId\n", .{});
    std.debug.print("  POST   /users\n", .{});
    std.debug.print("  PUT    /users/:id\n", .{});
    std.debug.print("  DELETE /users/:id\n", .{});
    std.debug.print("  GET    /static/*\n", .{});

    std.debug.print("\nRoute Matching Tests:\n", .{});

    var params_buf: [16]httpx.router.RouteParam = undefined;

    if (router.find(.GET, "/users/42", &params_buf)) |result| {
        std.debug.print("  GET /users/42 -> matched!\n", .{});
        std.debug.print("    Parameters: {d}\n", .{result.params.len});
        for (result.params) |p| {
            std.debug.print("      {s} = {s}\n", .{ p.name, p.value });
        }
    }

    if (router.find(.GET, "/users/123/posts/456", &params_buf)) |result| {
        std.debug.print("  GET /users/123/posts/456 -> matched!\n", .{});
        std.debug.print("    Parameters: {d}\n", .{result.params.len});
        for (result.params) |p| {
            std.debug.print("      {s} = {s}\n", .{ p.name, p.value });
        }
    }

    if (router.find(.DELETE, "/users/99", &params_buf)) |_| {
        std.debug.print("  DELETE /users/99 -> matched!\n", .{});
    }

    if (router.find(.PATCH, "/users/1", &params_buf)) |_| {
        std.debug.print("  PATCH /users/1 -> matched!\n", .{});
    } else {
        std.debug.print("  PATCH /users/1 -> not found (expected)\n", .{});
    }

    std.debug.print("\nRoute groups for API versioning:\n", .{});
    var api_v1 = router.group("/api/v1");
    _ = &api_v1;
    std.debug.print("  /api/v1/users\n", .{});
    std.debug.print("  /api/v1/posts\n", .{});
}
