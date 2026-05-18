//! Request/Response Interceptors Example
//!
//! Demonstrates using interceptors to modify requests and responses.

const std = @import("std");
const httpx = @import("httpx");

fn logRequest(request: *httpx.Request, context: ?*anyopaque) anyerror!void {
    _ = context;
    std.debug.print("[Interceptor] Request: {s} {s}\n", .{
        request.method.toString(),
        request.uri.path,
    });
}

fn logResponse(response: *httpx.Response, context: ?*anyopaque) anyerror!void {
    _ = context;
    std.debug.print("[Interceptor] Response: {d} {s}\n", .{
        response.status.code,
        response.status.phrase,
    });
}

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;

    std.debug.print("=== Request/Response Interceptors Example ===\n\n", .{});

    var client = httpx.Client.initWithConfig(allocator, init.io, .{
        .user_agent = "httpx.zig-interceptor-demo/1.0",
    });
    defer client.deinit();

    try client.addInterceptor(.{
        .request_fn = logRequest,
        .response_fn = logResponse,
        .context = null,
    });

    std.debug.print("Interceptors registered: {d}\n", .{client.interceptors.items.len});

    std.debug.print("\nInterceptor use cases:\n", .{});
    std.debug.print("  - Add authentication headers\n", .{});
    std.debug.print("  - Log all requests/responses\n", .{});
    std.debug.print("  - Modify request body\n", .{});
    std.debug.print("  - Handle rate limiting\n", .{});
    std.debug.print("  - Add timing metrics\n", .{});
    std.debug.print("  - Cache responses\n", .{});

    std.debug.print("\nExample: Adding auth header in interceptor:\n", .{});
    std.debug.print("  fn addAuth(req: *Request, ctx: ?*anyopaque) !void {{\n", .{});
    std.debug.print("      const token = @as(*[]const u8, @ptrCast(ctx.?)).*;\n", .{});
    std.debug.print("      try req.headers.set(\"Authorization\", token);\n", .{});
    std.debug.print("  }}\n", .{});

    std.debug.print("\nDemo complete!\n", .{});
}
