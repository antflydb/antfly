//! Static File Server Example
//!
//! Demonstrates:
//! - serving explicit single-file static routes
//! - serving directory-based wildcard static routes
//! - redirects using httpx.zig response helpers

const std = @import("std");
const httpx = @import("httpx");

const site_root = "examples/multi_page_site/site";
const assets_root = "examples/multi_page_site/site/assets";

fn demoPort(environ_map: *std.process.Environ.Map) u16 {
    if (environ_map.get("HTTPX_DEMO_PORT")) |value| {
        return std.fmt.parseInt(u16, value, 10) catch 8080;
    }
    return 8080;
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

fn serveFileWithType(ctx: *httpx.Context, path: []const u8) anyerror!httpx.Response {
    const body = std.Io.Dir.cwd().readFileAlloc(ctx.io, path, ctx.allocator, std.Io.Limit.limited(1024 * 1024)) catch {
        return ctx.status(404).text("Not Found");
    };

    var resp = httpx.Response.init(ctx.allocator, 200);
    try resp.headers.set("Content-Type", contentTypeForPath(path));
    resp.body = body;
    resp.body_owned = true;
    return resp;
}

fn serveSitePath(ctx: *httpx.Context, rel_path: []const u8) anyerror!httpx.Response {
    if (std.mem.indexOf(u8, rel_path, "..") != null or std.mem.indexOfScalar(u8, rel_path, '\\') != null) {
        return ctx.status(400).text("Invalid path");
    }

    var path_buf: [1024]u8 = undefined;
    const full_path = std.fmt.bufPrint(&path_buf, "{s}/{s}", .{ site_root, rel_path }) catch {
        return ctx.status(414).text("Path too long");
    };

    return serveFileWithType(ctx, full_path);
}

fn serveAssetsPath(ctx: *httpx.Context, rel_path: []const u8) anyerror!httpx.Response {
    if (std.mem.indexOf(u8, rel_path, "..") != null or std.mem.indexOfScalar(u8, rel_path, '\\') != null) {
        return ctx.status(400).text("Invalid path");
    }

    var path_buf: [1024]u8 = undefined;
    const full_path = std.fmt.bufPrint(&path_buf, "{s}/{s}", .{ assets_root, rel_path }) catch {
        return ctx.status(414).text("Path too long");
    };

    return serveFileWithType(ctx, full_path);
}

fn homeHandler(ctx: *httpx.Context) anyerror!httpx.Response {
    return ctx.html(
        \\<h1>httpx.zig static files demo</h1>
        \\<p>This demo focuses on static file serving patterns.</p>
        \\<ul>
        \\  <li><a href="/logo">File route: /logo</a></li>
        \\  <li><a href="/styles.css">File route: /styles.css</a></li>
        \\  <li><a href="/assets/styles.css">Directory route: /assets/*</a></li>
        \\  <li><a href="/images/httpx.zig-transparent.png">Directory route: /images/*</a></li>
        \\  <li><a href="/site-home">Full page from site folder</a></li>
        \\  <li><a href="/go-home">Redirect route</a></li>
        \\</ul>
    );
}

fn siteHomeHandler(ctx: *httpx.Context) anyerror!httpx.Response {
    return serveSitePath(ctx, "index.html");
}

fn logoHandler(ctx: *httpx.Context) anyerror!httpx.Response {
    return serveAssetsPath(ctx, "images/httpx.zig-transparent.png");
}

fn cssFileHandler(ctx: *httpx.Context) anyerror!httpx.Response {
    return serveAssetsPath(ctx, "styles.css");
}

fn jsFileHandler(ctx: *httpx.Context) anyerror!httpx.Response {
    return serveAssetsPath(ctx, "app.js");
}

fn assetsByPrefixHandler(ctx: *httpx.Context, prefix: []const u8) anyerror!httpx.Response {
    if (!std.mem.startsWith(u8, ctx.request.uri.path, prefix)) {
        return ctx.status(400).text("Invalid asset route");
    }
    const wildcard = ctx.request.uri.path[prefix.len..];
    if (wildcard.len == 0) {
        return ctx.status(400).text("Missing asset path");
    }

    // Keep this example simple and safe: avoid path traversal.
    if (std.mem.indexOf(u8, wildcard, "..") != null or
        std.mem.indexOfScalar(u8, wildcard, '\\') != null)
    {
        return ctx.status(400).text("Invalid asset path");
    }

    var rel_buf: [1024]u8 = undefined;
    const rel_path = std.fmt.bufPrint(&rel_buf, "assets/{s}", .{wildcard}) catch {
        return ctx.status(414).text("Path too long");
    };

    return serveSitePath(ctx, rel_path);
}

fn assetsHandler(ctx: *httpx.Context) anyerror!httpx.Response {
    return assetsByPrefixHandler(ctx, "/assets/");
}

fn imagesHandler(ctx: *httpx.Context) anyerror!httpx.Response {
    const prefix = "/images/";
    if (!std.mem.startsWith(u8, ctx.request.uri.path, prefix)) {
        return ctx.status(400).text("Invalid image route");
    }
    const wildcard = ctx.request.uri.path[prefix.len..];
    if (wildcard.len == 0) {
        return ctx.status(400).text("Missing image path");
    }

    var rel_buf: [1024]u8 = undefined;
    const rel_path = std.fmt.bufPrint(&rel_buf, "images/{s}", .{wildcard}) catch {
        return ctx.status(414).text("Path too long");
    };

    return serveAssetsPath(ctx, rel_path);
}

fn redirectHandler(ctx: *httpx.Context) anyerror!httpx.Response {
    return ctx.redirect("/", 302);
}

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;

    std.debug.print("=== Static File Server Example ===\n\n", .{});

    const port = demoPort(init.environ_map);

    var server = httpx.Server.initWithConfig(allocator, init.io, .{
        .host = "127.0.0.1",
        .port = port,
        .keep_alive = false,
    });
    defer server.deinit();

    try server.get("/", homeHandler);
    try server.get("/site-home", siteHomeHandler);
    try server.get("/logo", logoHandler);
    try server.get("/styles.css", cssFileHandler);
    try server.get("/app.js", jsFileHandler);
    try server.get("/assets/*", assetsHandler);
    try server.get("/images/*", imagesHandler);
    try server.get("/go-home", redirectHandler);

    std.debug.print("Registered routes:\n", .{});
    std.debug.print("  GET /               -> demo index (HTML response)\n", .{});
    std.debug.print("  GET /site-home      -> explicit file route to site index.html\n", .{});
    std.debug.print("  GET /logo           -> explicit file route to site image\n", .{});
    std.debug.print("  GET /styles.css     -> explicit file route to site CSS\n", .{});
    std.debug.print("  GET /app.js         -> explicit file route to site JS\n", .{});
    std.debug.print("  GET /assets/*       -> directory wildcard route to assets root\n", .{});
    std.debug.print("  GET /images/*       -> directory wildcard route to image directory\n", .{});
    std.debug.print("  GET /go-home        -> redirect('/')\n", .{});

    std.debug.print("\nTry:\n", .{});
    std.debug.print("  http://127.0.0.1:{d}/\n", .{port});
    std.debug.print("  http://127.0.0.1:{d}/site-home\n", .{port});
    std.debug.print("  http://127.0.0.1:{d}/logo\n", .{port});
    std.debug.print("  http://127.0.0.1:{d}/styles.css\n", .{port});
    std.debug.print("  http://127.0.0.1:{d}/app.js\n", .{port});
    std.debug.print("  http://127.0.0.1:{d}/assets/styles.css\n", .{port});
    std.debug.print("  http://127.0.0.1:{d}/assets/app.js\n", .{port});
    std.debug.print("  http://127.0.0.1:{d}/images/httpx.zig-transparent.png\n", .{port});
    std.debug.print("  http://127.0.0.1:{d}/assets/images/httpx.zig-transparent.png\n", .{port});

    std.debug.print("\nStarting server at http://127.0.0.1:{d}/\n", .{port});
    std.debug.print("Set HTTPX_DEMO_PORT to override the default port.\n", .{});
    std.debug.print("Press Ctrl+C to stop.\n", .{});

    try server.listen();
}
