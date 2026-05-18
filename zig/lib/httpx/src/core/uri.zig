//! URI Parsing and Manipulation for httpx.zig
//!
//! Implements URI parsing according to RFC 3986 with support for:
//!
//! - Full URI parsing (scheme, userinfo, host, port, path, query, fragment)
//! - Percent-encoding and decoding
//! - Path normalization
//! - Query string building
//! - Automatic port detection for common schemes

const std = @import("std");
const arrayListWriter = @import("../util/array_list_writer.zig").arrayListWriter;
const encoding = @import("../util/encoding.zig");
const mem = std.mem;
const Allocator = mem.Allocator;

/// Parsed URI structure per RFC 3986.
pub const Uri = struct {
    scheme: ?[]const u8 = null,
    userinfo: ?[]const u8 = null,
    host: ?[]const u8 = null,
    port: ?u16 = null,
    path: []const u8 = "/",
    query: ?[]const u8 = null,
    fragment: ?[]const u8 = null,
    raw: []const u8,

    const Self = @This();

    /// Parses a URI string into its components.
    pub fn parse(uri_string: []const u8) !Self {
        var uri = Self{ .raw = uri_string };
        var remaining = uri_string;

        // Search for "://" but verify the candidate scheme contains no "/" or "?"
        // to avoid false matches in path-only URLs like "/redirect?url=https://x".
        if (mem.indexOf(u8, remaining, "://")) |scheme_end| {
            const candidate = remaining[0..scheme_end];
            if (mem.indexOfScalar(u8, candidate, '/') == null and
                mem.indexOfScalar(u8, candidate, '?') == null)
            {
                uri.scheme = candidate;
                remaining = remaining[scheme_end + 3 ..];
            }
        }

        if (mem.indexOf(u8, remaining, "#")) |frag_start| {
            uri.fragment = remaining[frag_start + 1 ..];
            remaining = remaining[0..frag_start];
        }

        if (mem.indexOf(u8, remaining, "?")) |query_start| {
            uri.query = remaining[query_start + 1 ..];
            remaining = remaining[0..query_start];
        }

        if (mem.indexOf(u8, remaining, "/")) |path_start| {
            uri.path = remaining[path_start..];
            remaining = remaining[0..path_start];
        } else {
            uri.path = "/";
        }

        if (mem.indexOf(u8, remaining, "@")) |auth_end| {
            uri.userinfo = remaining[0..auth_end];
            remaining = remaining[auth_end + 1 ..];
        }

        if (remaining.len > 0 and remaining[0] == '[') {
            if (mem.indexOf(u8, remaining, "]")) |bracket_end| {
                uri.host = remaining[1..bracket_end];
                remaining = remaining[bracket_end + 1 ..];
            }
        }

        if (mem.lastIndexOf(u8, remaining, ":")) |port_sep| {
            if (std.fmt.parseInt(u16, remaining[port_sep + 1 ..], 10)) |port| {
                uri.port = port;
                remaining = remaining[0..port_sep];
            } else |_| {}
        }

        if (remaining.len > 0 and uri.host == null) {
            uri.host = remaining;
        }

        return uri;
    }

    /// Returns the effective port, using scheme defaults if not specified.
    pub fn effectivePort(self: Self) u16 {
        if (self.port) |p| return p;
        if (self.scheme) |s| {
            if (mem.eql(u8, s, "https")) return 443;
            if (mem.eql(u8, s, "http")) return 80;
            if (mem.eql(u8, s, "ws")) return 80;
            if (mem.eql(u8, s, "wss")) return 443;
            if (mem.eql(u8, s, "ftp")) return 21;
        }
        return 80;
    }

    /// Returns true if the scheme requires TLS.
    pub fn isTls(self: Self) bool {
        if (self.scheme) |s| {
            return mem.eql(u8, s, "https") or mem.eql(u8, s, "wss");
        }
        return false;
    }

    /// Returns true if this is a WebSocket URI.
    pub fn isWebSocket(self: Self) bool {
        if (self.scheme) |s| {
            return mem.eql(u8, s, "ws") or mem.eql(u8, s, "wss");
        }
        return false;
    }

    /// Builds the request path including query string.
    pub fn requestPath(self: Self, allocator: Allocator) ![]u8 {
        if (self.query) |q| {
            return std.fmt.allocPrint(allocator, "{s}?{s}", .{ self.path, q });
        }
        return allocator.dupe(u8, self.path);
    }

    /// Reconstructs the full URI string.
    pub fn format(self: Self, allocator: Allocator) ![]u8 {
        var buffer = std.ArrayListUnmanaged(u8).empty;
        const writer = arrayListWriter(&buffer, allocator);

        if (self.scheme) |s| try writer.print("{s}://", .{s});
        if (self.userinfo) |u| try writer.print("{s}@", .{u});
        if (self.host) |h| try writer.print("{s}", .{h});
        if (self.port) |p| try writer.print(":{d}", .{p});
        try writer.print("{s}", .{self.path});
        if (self.query) |q| try writer.print("?{s}", .{q});
        if (self.fragment) |f| try writer.print("#{s}", .{f});

        return buffer.toOwnedSlice(allocator);
    }

    /// Returns the authority component (userinfo@host:port).
    pub fn authority(self: Self, allocator: Allocator) ![]u8 {
        var buffer = std.ArrayListUnmanaged(u8).empty;
        const writer = arrayListWriter(&buffer, allocator);

        if (self.userinfo) |u| try writer.print("{s}@", .{u});
        if (self.host) |h| try writer.print("{s}", .{h});
        if (self.port) |p| try writer.print(":{d}", .{p});

        return buffer.toOwnedSlice(allocator);
    }
};

/// Percent-encodes a string for URI inclusion. Delegates to encoding.zig.
pub const encode = encoding.PercentEncoding.encode;

/// Decodes a percent-encoded string. Delegates to encoding.zig.
pub const decode = encoding.PercentEncoding.decode;

/// Encodes query parameters as a query string. Delegates to encoding.zig.
pub const encodeQueryParams = encoding.encodeFormData;

test "URI parsing basic" {
    const uri = try Uri.parse("https://example.com/path");
    try std.testing.expectEqualStrings("https", uri.scheme.?);
    try std.testing.expectEqualStrings("example.com", uri.host.?);
    try std.testing.expectEqualStrings("/path", uri.path);
}

test "URI parsing with port" {
    const uri = try Uri.parse("http://localhost:8080/api");
    try std.testing.expectEqualStrings("localhost", uri.host.?);
    try std.testing.expectEqual(@as(u16, 8080), uri.port.?);
}

test "URI parsing with query and fragment" {
    const uri = try Uri.parse("https://example.com/search?q=test#results");
    try std.testing.expectEqualStrings("q=test", uri.query.?);
    try std.testing.expectEqualStrings("results", uri.fragment.?);
}

test "URI effective port" {
    const https = try Uri.parse("https://example.com/");
    try std.testing.expectEqual(@as(u16, 443), https.effectivePort());

    const http = try Uri.parse("http://example.com/");
    try std.testing.expectEqual(@as(u16, 80), http.effectivePort());
}

test "URI TLS detection" {
    const https = try Uri.parse("https://example.com/");
    try std.testing.expect(https.isTls());

    const http = try Uri.parse("http://example.com/");
    try std.testing.expect(!http.isTls());
}

test "URI parsing path-only with scheme-like query" {
    // Path-only URLs must not match "://" inside the path or query.
    const uri = try Uri.parse("/redirect?url=https://other.com");
    try std.testing.expect(uri.scheme == null);
    try std.testing.expectEqualStrings("/redirect", uri.path);
    try std.testing.expectEqualStrings("url=https://other.com", uri.query.?);
}

test "URI format roundtrip" {
    const allocator = std.testing.allocator;
    const uri = try Uri.parse("https://user:pass@example.com:8443/path?q=1#frag");
    const formatted = try uri.format(allocator);
    defer allocator.free(formatted);
    try std.testing.expectEqualStrings("https://user:pass@example.com:8443/path?q=1#frag", formatted);
}

test "URI authority" {
    const allocator = std.testing.allocator;
    const uri = try Uri.parse("https://user@example.com:443/path");
    const auth = try uri.authority(allocator);
    defer allocator.free(auth);
    try std.testing.expectEqualStrings("user@example.com:443", auth);
}

test "URI requestPath with query" {
    const allocator = std.testing.allocator;
    const uri = try Uri.parse("https://example.com/api?key=val");
    const rp = try uri.requestPath(allocator);
    defer allocator.free(rp);
    try std.testing.expectEqualStrings("/api?key=val", rp);
}

test "URI requestPath without query" {
    const allocator = std.testing.allocator;
    const uri = try Uri.parse("https://example.com/api");
    const rp = try uri.requestPath(allocator);
    defer allocator.free(rp);
    try std.testing.expectEqualStrings("/api", rp);
}

test "URI IPv6 parsing" {
    const uri = try Uri.parse("https://[::1]:8080/path");
    try std.testing.expectEqualStrings("::1", uri.host.?);
    try std.testing.expectEqual(@as(u16, 8080), uri.port.?);
    try std.testing.expectEqualStrings("/path", uri.path);
}

test "URI isWebSocket" {
    const ws = try Uri.parse("ws://example.com/ws");
    try std.testing.expect(ws.isWebSocket());
    try std.testing.expect(!ws.isTls());

    const wss = try Uri.parse("wss://example.com/ws");
    try std.testing.expect(wss.isWebSocket());
    try std.testing.expect(wss.isTls());

    const http = try Uri.parse("https://example.com/");
    try std.testing.expect(!http.isWebSocket());
}

test "URI userinfo parsing" {
    const uri = try Uri.parse("https://admin:secret@host.com/");
    try std.testing.expectEqualStrings("admin:secret", uri.userinfo.?);
    try std.testing.expectEqualStrings("host.com", uri.host.?);
}

test "URI effectivePort defaults" {
    const http = try Uri.parse("http://example.com/");
    try std.testing.expectEqual(@as(u16, 80), http.effectivePort());

    const https = try Uri.parse("https://example.com/");
    try std.testing.expectEqual(@as(u16, 443), https.effectivePort());

    const ws = try Uri.parse("ws://example.com/");
    try std.testing.expectEqual(@as(u16, 80), ws.effectivePort());

    const wss = try Uri.parse("wss://example.com/");
    try std.testing.expectEqual(@as(u16, 443), wss.effectivePort());

    const ftp = try Uri.parse("ftp://example.com/");
    try std.testing.expectEqual(@as(u16, 21), ftp.effectivePort());
}

test "Percent encoding" {
    const allocator = std.testing.allocator;

    const encoded = try encode(allocator, "hello world");
    defer allocator.free(encoded);
    try std.testing.expectEqualStrings("hello%20world", encoded);
}

test "Percent decoding" {
    const allocator = std.testing.allocator;

    const decoded = try decode(allocator, "hello%20world");
    defer allocator.free(decoded);
    try std.testing.expectEqualStrings("hello world", decoded);
}
