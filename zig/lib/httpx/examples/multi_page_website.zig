//! Multi-page Website Server Example
//!
//! Runs an httpx.zig server that serves a small multi-page site from
//! `examples/multi_page_site/site`.

const std = @import("std");
const httpx = @import("httpx");

const site_root = "examples/multi_page_site/site";

fn demoPort(environ_map: *std.process.Environ.Map) u16 {
    if (environ_map.get("HTTPX_DEMO_PORT")) |value| {
        return std.fmt.parseInt(u16, value, 10) catch 8090;
    }
    return 8090;
}

fn contentTypeForPath(path: []const u8) []const u8 {
    if (std.mem.endsWith(u8, path, ".html")) return "text/html; charset=utf-8";
    if (std.mem.endsWith(u8, path, ".css")) return "text/css; charset=utf-8";
    if (std.mem.endsWith(u8, path, ".js")) return "application/javascript; charset=utf-8";
    if (std.mem.endsWith(u8, path, ".png")) return "image/png";
    if (std.mem.endsWith(u8, path, ".jpg") or std.mem.endsWith(u8, path, ".jpeg")) return "image/jpeg";
    if (std.mem.endsWith(u8, path, ".svg")) return "image/svg+xml";
    return "application/octet-stream";
}

fn serveRelativePath(ctx: *httpx.Context, rel_path: []const u8) anyerror!httpx.Response {
    if (std.mem.indexOf(u8, rel_path, "..") != null or std.mem.indexOfScalar(u8, rel_path, '\\') != null) {
        return ctx.status(400).text("Invalid path");
    }

    var full_path_buf: [1024]u8 = undefined;
    const full_path = std.fmt.bufPrint(&full_path_buf, "{s}/{s}", .{ site_root, rel_path }) catch {
        return ctx.status(414).text("Path too long");
    };

    const body = std.Io.Dir.cwd().readFileAlloc(ctx.io, full_path, ctx.allocator, std.Io.Limit.limited(1024 * 1024)) catch {
        return ctx.status(404).text("Not Found");
    };

    var resp = httpx.Response.init(ctx.allocator, 200);
    try resp.headers.set("Content-Type", contentTypeForPath(rel_path));
    try resp.headers.set("Cache-Control", "no-cache");
    resp.body = body;
    resp.body_owned = true;
    return resp;
}

fn homeHandler(ctx: *httpx.Context) anyerror!httpx.Response {
    return serveRelativePath(ctx, "index.html");
}

fn aboutHandler(ctx: *httpx.Context) anyerror!httpx.Response {
    return serveRelativePath(ctx, "about.html");
}

fn contactHandler(ctx: *httpx.Context) anyerror!httpx.Response {
    return serveRelativePath(ctx, "contact.html");
}

fn logoHandler(ctx: *httpx.Context) anyerror!httpx.Response {
    return serveRelativePath(ctx, "assets/images/httpx.zig-transparent.png");
}

fn staticHandler(ctx: *httpx.Context) anyerror!httpx.Response {
    const prefix = "/static/";
    if (!std.mem.startsWith(u8, ctx.request.uri.path, prefix)) {
        return ctx.status(400).text("Invalid static route");
    }

    const suffix = ctx.request.uri.path[prefix.len..];
    if (suffix.len == 0) {
        return ctx.status(400).text("Missing static path");
    }

    var rel_buf: [1024]u8 = undefined;
    const rel_path = std.fmt.bufPrint(&rel_buf, "assets/{s}", .{suffix}) catch {
        return ctx.status(414).text("Path too long");
    };

    return serveRelativePath(ctx, rel_path);
}

fn redirectHomeHandler(ctx: *httpx.Context) anyerror!httpx.Response {
    return ctx.redirect("/", 302);
}

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    const port = demoPort(init.environ_map);

    var server = httpx.Server.initWithConfig(allocator, init.io, .{
        .host = "127.0.0.1",
        .port = port,
        .keep_alive = false,
        .request_timeout_ms = 10_000,
    });
    defer server.deinit();

    try server.get("/", homeHandler);
    try server.get("/about", aboutHandler);
    try server.get("/contact", contactHandler);
    try server.get("/logo", logoHandler);
    try server.get("/go-home", redirectHomeHandler);
    try server.get("/static/*", staticHandler);

    std.debug.print("=== Multi-page Website Example ===\n", .{});
    std.debug.print("Serving site from: {s}\n", .{site_root});
    std.debug.print("Routes: page routes + /logo (file route) + /static/* (directory route)\n", .{});
    std.debug.print("Open: http://127.0.0.1:{d}/\n", .{port});
    std.debug.print("Set HTTPX_DEMO_PORT to override the default port.\n", .{});
    std.debug.print("Keep-Alive: disabled for smoother demo asset loading\n", .{});
    std.debug.print("Press Ctrl+C to stop.\n", .{});

    try server.listen();
}
