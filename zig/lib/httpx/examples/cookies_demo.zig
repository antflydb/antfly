//! Cookie Jar Demo
//!
//! Demonstrates storing, reading, and attaching cookies with the httpx client.

const std = @import("std");
const httpx = @import("httpx");

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;

    std.debug.print("=== Cookie Jar Demo ===\n\n", .{});

    var client = httpx.Client.init(allocator, init.io);
    defer client.deinit();

    try client.setCookie("session", "abc123");
    try client.setCookie("theme", "dark");

    std.debug.print("Cookie session: {s}\n", .{client.getCookie("session") orelse "<missing>"});
    std.debug.print("Cookie theme: {s}\n", .{client.getCookie("theme") orelse "<missing>"});

    const removed = client.removeCookie("theme");
    std.debug.print("Removed theme cookie: {}\n", .{removed});

    // Build and serialize a request to demonstrate attached cookies.
    var req = try httpx.Request.init(allocator, .GET, "https://example.com/account");
    defer req.deinit();

    if (client.getCookie("session")) |session| {
        var cookie_buf: [256]u8 = undefined;
        const cookie_val = std.fmt.bufPrint(&cookie_buf, "session={s}", .{session}) catch "session=?";
        try req.headers.set(httpx.HeaderName.COOKIE, cookie_val);
    }

    const wire = try req.toSlice(allocator);
    defer allocator.free(wire);

    std.debug.print("\nSerialized request with Cookie header:\n{s}\n", .{wire});

    client.clearCookies();
    std.debug.print(
        "Cookies cleared (session present? {}):\n",
        .{client.getCookie("session") != null},
    );
}
