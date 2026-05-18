//! Shared utility helpers used across client/server/core modules.

const std = @import("std");
const mem = std.mem;
const arrayListWriter = @import("array_list_writer.zig").arrayListWriter;
const containsCrLf = @import("../core/headers.zig").containsCrLf;
const Io = std.Io;

/// Monotonic millisecond timestamp for connection health and deadline tracking.
/// Uses std.Io.Clock.awake — the fiber-cooperative monotonic clock.
pub fn milliTimestamp(io: Io) i64 {
    const now = Io.Clock.awake.now(io);
    return @intCast(@divFloor(now.nanoseconds, std.time.ns_per_ms));
}

/// Parsed cookie name/value pair from a Set-Cookie header value.
pub const CookiePair = struct {
    name: []const u8,
    value: []const u8,
};

pub const SameSite = enum {
    lax,
    strict,
    none,

    pub fn toHeaderValue(self: @This()) []const u8 {
        return switch (self) {
            .lax => "Lax",
            .strict => "Strict",
            .none => "None",
        };
    }
};

pub const CookieOptions = struct {
    path: ?[]const u8 = "/",
    domain: ?[]const u8 = null,
    max_age: ?i64 = null,
    secure: bool = false,
    http_only: bool = true,
    same_site: ?SameSite = .lax,
};

/// Returns a query parameter value from a raw query string.
///
/// For key-only query entries (e.g. `?debug`), returns an empty slice.
pub fn queryValue(query: []const u8, key: []const u8) ?[]const u8 {
    var it = mem.splitScalar(u8, query, '&');
    while (it.next()) |part| {
        const eq_idx = mem.indexOfScalar(u8, part, '=') orelse {
            if (mem.eql(u8, part, key)) return "";
            continue;
        };

        const k = part[0..eq_idx];
        if (!mem.eql(u8, k, key)) continue;
        return part[eq_idx + 1 ..];
    }

    return null;
}

/// Parses the `name=value` segment from a Set-Cookie header value.
///
/// Attributes after `;` are ignored.
pub fn parseSetCookiePair(set_cookie: []const u8) ?CookiePair {
    const semicolon = mem.indexOfScalar(u8, set_cookie, ';') orelse set_cookie.len;
    const pair = set_cookie[0..semicolon];
    const eq = mem.indexOfScalar(u8, pair, '=') orelse return null;

    const name = mem.trim(u8, pair[0..eq], " \t");
    const value = mem.trim(u8, pair[eq + 1 ..], " \t");
    if (name.len == 0) return null;

    return .{ .name = name, .value = value };
}

/// Returns a cookie value from a Cookie header string.
///
/// Example header: `session=abc123; theme=dark`
pub fn cookieValue(cookie_header: []const u8, name: []const u8) ?[]const u8 {
    var it = mem.splitScalar(u8, cookie_header, ';');
    while (it.next()) |segment| {
        const part = mem.trim(u8, segment, " \t");
        const eq = mem.indexOfScalar(u8, part, '=') orelse continue;
        const k = mem.trim(u8, part[0..eq], " \t");
        if (!mem.eql(u8, k, name)) continue;
        return mem.trim(u8, part[eq + 1 ..], " \t");
    }
    return null;
}

/// Builds a Set-Cookie header value with common RFC 6265 attributes.
/// Returns error.HeaderContainsCrLf if any option value contains CR or LF.
pub fn buildSetCookieHeader(allocator: std.mem.Allocator, name: []const u8, value: []const u8, options: CookieOptions) ![]u8 {
    // Validate all string inputs against CRLF injection.
    if (containsCrLf(name) or containsCrLf(value)) return error.HeaderContainsCrLf;
    if (options.path) |p| if (containsCrLf(p)) return error.HeaderContainsCrLf;
    if (options.domain) |d| if (containsCrLf(d)) return error.HeaderContainsCrLf;

    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(allocator);
    const writer = arrayListWriter(&out, allocator);

    try writer.print("{s}={s}", .{ name, value });

    if (options.path) |path| {
        try writer.print("; Path={s}", .{path});
    }
    if (options.domain) |domain| {
        try writer.print("; Domain={s}", .{domain});
    }
    if (options.max_age) |max_age| {
        try writer.print("; Max-Age={d}", .{max_age});
    }
    if (options.same_site) |same_site| {
        try writer.print("; SameSite={s}", .{same_site.toHeaderValue()});
    }
    if (options.secure) {
        try writer.writeAll("; Secure");
    }
    if (options.http_only) {
        try writer.writeAll("; HttpOnly");
    }

    return out.toOwnedSlice(allocator);
}

/// Returns a best-effort MIME type for a file path extension.
///
/// For types that also exist in `ContentType`, this map uses the same MIME
/// strings. Additional format-specific entries (fonts, video, etc.) that
/// don't have a `ContentType` variant live only here.
pub fn mimeTypeFromPath(path: []const u8) []const u8 {
    const CT = @import("../core/types.zig").ContentType;
    const mime_map = std.StaticStringMap([]const u8).initComptime(.{
        .{ ".html", CT.text_html.toString() ++ "; charset=utf-8" },
        .{ ".htm", CT.text_html.toString() ++ "; charset=utf-8" },
        .{ ".css", CT.text_css.toString() ++ "; charset=utf-8" },
        .{ ".js", CT.text_javascript.toString() ++ "; charset=utf-8" },
        .{ ".json", CT.application_json.toString() },
        .{ ".txt", CT.text_plain.toString() ++ "; charset=utf-8" },
        .{ ".svg", CT.image_svg.toString() },
        .{ ".png", CT.image_png.toString() },
        .{ ".jpg", CT.image_jpeg.toString() },
        .{ ".jpeg", CT.image_jpeg.toString() },
        .{ ".gif", CT.image_gif.toString() },
        .{ ".webp", CT.image_webp.toString() },
        .{ ".ico", "image/x-icon" },
        .{ ".xml", CT.application_xml.toString() },
        .{ ".pdf", "application/pdf" },
        .{ ".woff", "font/woff" },
        .{ ".woff2", "font/woff2" },
        .{ ".mp4", "video/mp4" },
        .{ ".webm", "video/webm" },
    });
    const ext = std.fs.path.extension(path);
    return mime_map.get(ext) orelse CT.application_octet_stream.toString();
}

test "queryValue parses normal and key-only params" {
    const q = "q=zig&lang=en&debug";
    try std.testing.expectEqualStrings("zig", queryValue(q, "q").?);
    try std.testing.expectEqualStrings("en", queryValue(q, "lang").?);
    try std.testing.expectEqualStrings("", queryValue(q, "debug").?);
    try std.testing.expect(queryValue(q, "missing") == null);
}

test "parseSetCookiePair extracts first cookie segment" {
    const p = parseSetCookiePair("session=abc123; Path=/; HttpOnly").?;
    try std.testing.expectEqualStrings("session", p.name);
    try std.testing.expectEqualStrings("abc123", p.value);

    try std.testing.expect(parseSetCookiePair("; Path=/") == null);
}

test "cookieValue parses Cookie header" {
    const header = "session=abc123; theme=dark; csrftoken=xyz";
    try std.testing.expectEqualStrings("abc123", cookieValue(header, "session").?);
    try std.testing.expectEqualStrings("dark", cookieValue(header, "theme").?);
    try std.testing.expect(cookieValue(header, "missing") == null);
}

test "buildSetCookieHeader includes options" {
    const allocator = std.testing.allocator;
    const set_cookie = try buildSetCookieHeader(allocator, "session", "abc123", .{
        .path = "/",
        .max_age = 3600,
        .secure = true,
        .http_only = true,
        .same_site = .strict,
    });
    defer allocator.free(set_cookie);

    try std.testing.expect(mem.indexOf(u8, set_cookie, "session=abc123") != null);
    try std.testing.expect(mem.indexOf(u8, set_cookie, "Path=/") != null);
    try std.testing.expect(mem.indexOf(u8, set_cookie, "Max-Age=3600") != null);
    try std.testing.expect(mem.indexOf(u8, set_cookie, "SameSite=Strict") != null);
    try std.testing.expect(mem.indexOf(u8, set_cookie, "Secure") != null);
    try std.testing.expect(mem.indexOf(u8, set_cookie, "HttpOnly") != null);
}

test "mimeTypeFromPath maps known extensions" {
    try std.testing.expectEqualStrings("text/html; charset=utf-8", mimeTypeFromPath("index.html"));
    try std.testing.expectEqualStrings("application/json", mimeTypeFromPath("api.json"));
    try std.testing.expectEqualStrings("image/png", mimeTypeFromPath("logo.png"));
    try std.testing.expectEqualStrings("application/octet-stream", mimeTypeFromPath("archive.bin"));
}
