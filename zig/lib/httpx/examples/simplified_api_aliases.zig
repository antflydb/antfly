//! Simplified API Aliases Demo
//!
//! Demonstrates top-level and client-level alias helpers for concise client code.

const std = @import("std");
const httpx = @import("httpx");

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    std.debug.print("=== Simplified API Demo ===\n\n", .{});

    var client = httpx.Client.init(allocator, init.io);
    defer client.deinit();

    // Demonstrate various HTTP methods
    var resp_get = try client.request(.GET, "https://httpbin.org/get", .{});
    defer resp_get.deinit();
    std.debug.print("GET status: {d}\n", .{resp_get.status.code});

    var resp_post = try client.request(.POST, "https://httpbin.org/post", .{});
    defer resp_post.deinit();
    std.debug.print("POST status: {d}\n", .{resp_post.status.code});

    var resp_options = try client.request(.OPTIONS, "https://httpbin.org/get", .{});
    defer resp_options.deinit();
    std.debug.print("OPTIONS status: {d}\n", .{resp_options.status.code});
}
