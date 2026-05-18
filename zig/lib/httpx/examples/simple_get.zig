//! Simple GET Request Example
//!
//! Demonstrates making a basic HTTP GET request using httpx.zig.

const std = @import("std");
const httpx = @import("httpx");

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;

    std.debug.print("=== Simple GET Request Example ===\n\n", .{});

    var client = httpx.Client.init(allocator, init.io);
    defer client.deinit();

    std.debug.print("Creating GET request to httpbin.org...\n", .{});

    var request = try httpx.Request.init(allocator, .GET, "https://httpbin.org/get");
    defer request.deinit();

    try request.headers.set("Accept", "application/json");
    try request.headers.set("User-Agent", "httpx.zig/1.0");

    const serialized = try request.toSlice(allocator);
    defer allocator.free(serialized);

    std.debug.print("\nRequest:\n", .{});
    std.debug.print("--------\n", .{});
    std.debug.print("{s}\n", .{serialized});

    std.debug.print("\nDemo complete! (Network request skipped for offline demo)\n", .{});
    std.debug.print("\nHTTP Method Properties:\n", .{});
    std.debug.print("  GET is idempotent: {}\n", .{httpx.Method.GET.isIdempotent()});
    std.debug.print("  GET is safe: {}\n", .{httpx.Method.GET.isSafe()});
    std.debug.print("  GET has request body: {}\n", .{httpx.Method.GET.hasRequestBody()});
}
