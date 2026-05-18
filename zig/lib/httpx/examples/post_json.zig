//! POST JSON Request Example
//!
//! Demonstrates sending JSON data in a POST request.

const std = @import("std");
const httpx = @import("httpx");

pub fn main() !void {
    const allocator = std.heap.smp_allocator;

    std.debug.print("=== POST JSON Request Example ===\n\n", .{});

    var request = try httpx.Request.init(allocator, .POST, "https://httpbin.org/post");
    defer request.deinit();

    const json_body =
        \\{
        \\  "name": "John Doe",
        \\  "email": "john@example.com",
        \\  "age": 30
        \\}
    ;

    try request.setJson(json_body);
    try request.headers.set("Accept", "application/json");

    const serialized = try request.toSlice(allocator);
    defer allocator.free(serialized);

    std.debug.print("Request:\n", .{});
    std.debug.print("--------\n", .{});
    std.debug.print("{s}\n", .{serialized});

    std.debug.print("\nUsing JsonBuilder:\n", .{});
    std.debug.print("------------------\n", .{});

    var builder = httpx.json.JsonBuilder.init(allocator);
    defer builder.deinit();

    try builder.beginObject();
    try builder.key("name");
    try builder.string("Jane Doe");
    try builder.key("email");
    try builder.string("jane@example.com");
    try builder.key("active");
    try builder.boolean(true);
    try builder.key("roles");
    try builder.beginArray();
    try builder.string("admin");
    try builder.string("user");
    try builder.endArray();
    try builder.endObject();

    std.debug.print("Built JSON: {s}\n", .{builder.toSlice()});

    std.debug.print("\nPOST method properties:\n", .{});
    std.debug.print("  Has request body: {}\n", .{httpx.Method.POST.hasRequestBody()});
    std.debug.print("  Is idempotent: {}\n", .{httpx.Method.POST.isIdempotent()});
}
