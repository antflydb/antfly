//! Concurrent Requests Example
//!
//! Demonstrates executing multiple HTTP requests in parallel.

const std = @import("std");
const httpx = @import("httpx");

pub fn main() !void {
    const allocator = std.heap.smp_allocator;

    std.debug.print("=== Concurrent Requests Example ===\n\n", .{});

    var builder = httpx.concurrency.BatchBuilder.init(allocator);
    defer builder.deinit();

    _ = try builder.get("https://httpbin.org/get");
    _ = try builder.get("https://httpbin.org/delay/1");
    _ = try builder.get("https://httpbin.org/headers");
    _ = try builder.post("https://httpbin.org/post", "{\"event\":\"page_view\"}");

    std.debug.print("Batch contains {d} requests:\n", .{builder.count()});
    for (builder.requests.items, 0..) |req, i| {
        std.debug.print("  {d}. {s} {s}\n", .{ i + 1, req.method.toString(), req.url });
    }

    std.debug.print("\nRequest specifications:\n", .{});
    const specs = [_]httpx.concurrency.RequestSpec{
        .{ .method = .GET, .url = "https://api1.example.com/data" },
        .{ .method = .GET, .url = "https://api2.example.com/data" },
        .{ .method = .POST, .url = "https://api3.example.com/data", .body = "{}" },
    };

    for (specs, 0..) |spec, i| {
        std.debug.print("  Spec {d}: {s} {s}", .{ i + 1, spec.method.toString(), spec.url });
        if (spec.body) |body| {
            std.debug.print(" (body: {d} bytes)", .{body.len});
        }
        std.debug.print("\n", .{});
    }

    std.debug.print("\nDemo complete! (Network requests skipped for offline demo)\n", .{});
}
