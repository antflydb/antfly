//! HTTP Request Representation for httpx.zig
//!
//! Provides the Request structure and RequestBuilder for constructing
//! HTTP requests with a fluent API. Features include:
//!
//! - Support for all HTTP methods and versions
//! - Header management with automatic Content-Length
//! - Body handling for JSON, form data, and binary
//! - Request serialization for wire format

const std = @import("std");
const serializeToSlice = @import("../util/array_list_writer.zig").serializeToSlice;
const mem = std.mem;
const Allocator = mem.Allocator;

const types = @import("types.zig");
const Headers = @import("headers.zig").Headers;
const HeaderName = @import("headers.zig").HeaderName;
const Uri = @import("uri.zig").Uri;
const PercentEncoding = @import("../util/encoding.zig").PercentEncoding;
const SharedBodyBudget = @import("../protocol/body_budget.zig").SharedBodyBudget;

/// HTTP request representation.
pub const Request = struct {
    allocator: Allocator,
    method: types.Method,
    uri: Uri,
    version: types.Version = .HTTP_1_1,
    headers: Headers,
    body: ?[]const u8 = null,
    body_owned: bool = false,
    /// Full allocation backing an owned body when its capacity is larger than
    /// the visible body length. This lets body-transform middleware retain a
    /// geometrically-grown buffer without an exact-size copy.
    body_allocation: ?[]u8 = null,
    /// Server-side application allocations that retain a transformed request
    /// body (for example, a decompressed payload) share the same process-wide
    /// budget as transport-owned request bytes. Client requests leave this
    /// unset.
    body_budget: ?*SharedBodyBudget = null,
    body_budget_reserved: usize = 0,
    custom_method: ?[]const u8 = null,
    query_owned: bool = false,
    query_builder: ?std.ArrayListUnmanaged(u8) = null,
    context: ?*anyopaque = null,
    /// Optional caller-specific response ceiling. Client execution clamps
    /// this to the configured client-wide maximum.
    max_response_size: ?usize = null,

    const Self = @This();

    /// Creates a new request with the given method and URL.
    pub fn init(allocator: Allocator, method: types.Method, url: []const u8) !Self {
        const uri = try Uri.parse(url);
        var headers = Headers.init(allocator);

        if (uri.host) |host| {
            const port = uri.effectivePort();
            const is_default_port = (uri.isTls() and port == 443) or (!uri.isTls() and port == 80);
            if (is_default_port) {
                try headers.set(HeaderName.HOST, host);
            } else {
                var host_buf: [264]u8 = undefined;
                const host_with_port = std.fmt.bufPrint(&host_buf, "{s}:{d}", .{ host, port }) catch
                    return error.HostNameTooLong;
                try headers.set(HeaderName.HOST, host_with_port);
            }
        }

        return .{
            .allocator = allocator,
            .method = method,
            .uri = uri,
            .headers = headers,
        };
    }

    /// Releases all allocated memory.
    pub fn deinit(self: *Self) void {
        self.headers.deinit();
        self.freeOwnedBody();
        if (self.body_budget) |budget| budget.release(self.body_budget_reserved);
        self.body_budget_reserved = 0;
        if (self.query_builder) |*builder| {
            builder.deinit(self.allocator);
        } else if (self.query_owned) {
            if (self.uri.query) |q| {
                self.allocator.free(q);
            }
        }
    }

    /// Reserves retained application-side body capacity before allocating it.
    /// Keeping the reservation on Request makes teardown ordering exact: the
    /// owned buffer is freed before its capacity is returned to the budget.
    pub fn tryReserveBodyBuffer(self: *Self, amount: usize) bool {
        const budget = self.body_budget orelse return true;
        if (!budget.tryReserve(amount)) return false;
        self.body_budget_reserved += amount;
        return true;
    }

    pub fn releaseBodyBuffer(self: *Self, amount: usize) void {
        if (amount == 0) return;
        std.debug.assert(self.body_budget_reserved >= amount);
        if (self.body_budget) |budget| budget.release(amount);
        self.body_budget_reserved -= amount;
    }

    fn freeOwnedBody(self: *Self) void {
        if (!self.body_owned) return;
        if (self.body_allocation) |allocation| {
            self.allocator.free(allocation);
        } else if (self.body) |body| {
            self.allocator.free(body);
        }
        self.body = null;
        self.body_owned = false;
        self.body_allocation = null;
    }

    /// Replaces the body with an already-owned allocation. `body` may expose
    /// only the initialized prefix of `allocation`; both slices must start at
    /// the same address. Ownership transfers even if updating Content-Length
    /// fails, so normal request teardown remains sufficient on every path.
    pub fn replaceOwnedBodyAllocation(self: *Self, body: []u8, allocation: []u8) !void {
        std.debug.assert(body.len <= allocation.len);
        std.debug.assert(body.ptr == allocation.ptr);
        self.freeOwnedBody();
        self.body = body;
        self.body_owned = true;
        self.body_allocation = allocation;
        try self.headers.setContentLength(body.len);
    }

    /// Sets the request body with ownership.
    pub fn setBody(self: *Self, body: []const u8) !void {
        self.freeOwnedBody();
        self.body = try self.allocator.dupe(u8, body);
        self.body_owned = true;
        self.body_allocation = null;
        try self.headers.setContentLength(body.len);
    }

    /// Borrows a request body for the duration of synchronous client
    /// execution. Callers must keep `body` alive until the client request
    /// method returns, including redirects and retries. This is intentionally
    /// separate from `setBody`: the latter preserves its ownership/copying
    /// contract for builders and asynchronously retained requests.
    pub fn setBorrowedBody(self: *Self, body: []const u8) !void {
        self.freeOwnedBody();
        self.body = body;
        self.body_owned = false;
        self.body_allocation = null;
        try self.headers.setContentLength(body.len);
    }

    /// Sets the request body as JSON with appropriate headers.
    pub fn setJson(self: *Self, body: []const u8) !void {
        try self.headers.set(HeaderName.CONTENT_TYPE, "application/json");
        try self.setBody(body);
    }

    /// Sets a request header.
    pub fn setHeader(self: *Self, name: []const u8, value: []const u8) !void {
        try self.headers.set(name, value);
    }

    /// Appends a URL query parameter to the request URI.
    ///
    /// The key and value are percent-encoded before being added.
    /// Uses an append-based approach to avoid O(N²) copying when adding
    /// multiple parameters sequentially.
    pub fn addQueryParam(self: *Self, key: []const u8, value: []const u8) !void {
        const enc_key = try PercentEncoding.encode(self.allocator, key);
        defer self.allocator.free(enc_key);
        const enc_value = try PercentEncoding.encode(self.allocator, value);
        defer self.allocator.free(enc_value);

        if (self.query_builder == null) {
            self.query_builder = .empty;
            // Seed the builder with any pre-existing query string.
            if (self.uri.query) |q| {
                try self.query_builder.?.appendSlice(self.allocator, q);
            }
        }

        var builder = &self.query_builder.?;
        if (builder.items.len > 0) {
            try builder.append(self.allocator, '&');
        }
        try builder.appendSlice(self.allocator, enc_key);
        try builder.append(self.allocator, '=');
        try builder.appendSlice(self.allocator, enc_value);

        // Free the previous owned query string if any.
        if (self.query_owned) {
            if (self.uri.query) |q| {
                self.allocator.free(q);
            }
        }
        // Point the URI query at the builder's live buffer.
        self.uri.query = builder.items;
        self.query_owned = false; // Builder owns the memory now.
    }

    /// Returns the host from the URI.
    pub fn getHost(self: *const Self) ?[]const u8 {
        return self.uri.host;
    }

    /// Returns the effective port.
    pub fn getPort(self: *const Self) u16 {
        return self.uri.effectivePort();
    }

    /// Returns true if the request uses TLS.
    pub fn isTls(self: *const Self) bool {
        return self.uri.isTls();
    }

    /// Serializes the request to HTTP/1.1 wire format.
    pub fn serialize(self: *const Self, writer: anytype) !void {
        const method_str = if (self.method == .CUSTOM)
            self.custom_method orelse "CUSTOM"
        else
            self.method.toString();

        const path = self.uri.path;
        const version_str = self.version.toString();

        try writer.writeAll(method_str);
        try writer.writeAll(" ");
        try writer.writeAll(path);
        if (self.uri.query) |q| {
            try writer.writeAll("?");
            try writer.writeAll(q);
        }
        try writer.writeAll(" ");
        try writer.writeAll(version_str);
        try writer.writeAll("\r\n");

        try self.headers.serialize(writer);
        try writer.writeAll("\r\n");

        if (self.body) |body| {
            try writer.writeAll(body);
        }
    }

    /// Serializes to an allocated buffer.
    pub fn toSlice(self: *const Self, allocator: Allocator) ![]u8 {
        return serializeToSlice(allocator, self);
    }
};

/// Fluent builder for constructing requests.
pub const RequestBuilder = struct {
    allocator: Allocator,
    method: types.Method = .GET,
    url: ?[]const u8 = null,
    version: types.Version = .HTTP_1_1,
    headers: Headers,
    body: ?[]const u8 = null,

    const Self = @This();

    /// Creates a new request builder.
    pub fn init(allocator: Allocator) Self {
        return .{
            .allocator = allocator,
            .headers = Headers.init(allocator),
        };
    }

    /// Releases builder resources.
    pub fn deinit(self: *Self) void {
        self.headers.deinit();
    }

    /// Sets the HTTP method.
    pub fn setMethod(self: *Self, method: types.Method) *Self {
        self.method = method;
        return self;
    }

    /// Sets the request URL.
    pub fn setUrl(self: *Self, url: []const u8) *Self {
        self.url = url;
        return self;
    }

    /// Sets the HTTP version.
    pub fn setVersion(self: *Self, version: types.Version) *Self {
        self.version = version;
        return self;
    }

    /// Adds a header.
    pub fn addHeader(self: *Self, name: []const u8, value: []const u8) !*Self {
        try self.headers.append(name, value);
        return self;
    }

    /// Sets the request body.
    pub fn setBody(self: *Self, body: []const u8) *Self {
        self.body = body;
        return self;
    }

    /// Sets a JSON body with appropriate Content-Type.
    pub fn setJsonBody(self: *Self, body: []const u8) !*Self {
        _ = try self.addHeader(HeaderName.CONTENT_TYPE, "application/json");
        self.body = body;
        return self;
    }

    /// Builds the final request.
    pub fn build(self: *Self) !Request {
        const url = self.url orelse return error.MissingUrl;
        var request = try Request.init(self.allocator, self.method, url);
        request.version = self.version;

        for (self.headers.iterator()) |h| {
            try request.headers.append(h.name, h.value);
        }

        if (self.body) |b| {
            try request.setBody(b);
        }

        return request;
    }
};

test "Request initialization" {
    const allocator = std.testing.allocator;
    var request = try Request.init(allocator, .GET, "https://example.com/api");
    defer request.deinit();

    try std.testing.expectEqual(types.Method.GET, request.method);
    try std.testing.expectEqualStrings("example.com", request.uri.host.?);
}

test "Request with body" {
    const allocator = std.testing.allocator;
    var request = try Request.init(allocator, .POST, "https://example.com/api");
    defer request.deinit();

    try request.setJson("{\"key\":\"value\"}");
    try std.testing.expect(request.body != null);
    try std.testing.expectEqualStrings("application/json", request.headers.get(HeaderName.CONTENT_TYPE).?);
}

test "Request can borrow an already-owned body without copying it" {
    const allocator = std.testing.allocator;
    var request = try Request.init(allocator, .POST, "https://example.com/api");
    defer request.deinit();

    var body = [_]u8{ 1, 2, 3, 4 };
    try request.setBorrowedBody(&body);
    try std.testing.expect(!request.body_owned);
    try std.testing.expectEqual(@intFromPtr(body[0..].ptr), @intFromPtr(request.body.?.ptr));
    try std.testing.expectEqualStrings("4", request.headers.get(HeaderName.CONTENT_LENGTH).?);
}

test "Request builder" {
    const allocator = std.testing.allocator;
    var builder = RequestBuilder.init(allocator);
    defer builder.deinit();

    _ = builder.setMethod(.POST).setUrl("https://example.com/api");
    _ = try builder.addHeader("X-Custom", "value");
    _ = builder.setBody("test body");

    var request = try builder.build();
    defer request.deinit();

    try std.testing.expectEqual(types.Method.POST, request.method);
}

test "Request serialization" {
    const allocator = std.testing.allocator;
    var request = try Request.init(allocator, .GET, "https://example.com/api");
    defer request.deinit();

    const serialized = try request.toSlice(allocator);
    defer allocator.free(serialized);

    try std.testing.expect(mem.startsWith(u8, serialized, "GET /api HTTP/1.1\r\n"));
}

test "Request serialization includes headers and body" {
    const allocator = std.testing.allocator;
    var request = try Request.init(allocator, .POST, "/api/data");
    defer request.deinit();

    try request.headers.set("Content-Type", "application/json");
    request.body = "{\"key\":\"value\"}";

    const serialized = try request.toSlice(allocator);
    defer allocator.free(serialized);

    // Request line.
    try std.testing.expect(mem.startsWith(u8, serialized, "POST /api/data HTTP/1.1\r\n"));
    // Header present.
    try std.testing.expect(mem.indexOf(u8, serialized, "Content-Type: application/json\r\n") != null);
    // Body after blank line.
    try std.testing.expect(mem.endsWith(u8, serialized, "\r\n\r\n{\"key\":\"value\"}"));
}

test "Request addQueryParam" {
    const allocator = std.testing.allocator;
    var request = try Request.init(allocator, .GET, "https://example.com/search");
    defer request.deinit();

    try request.addQueryParam("q", "zig lang");
    try request.addQueryParam("page", "1");

    try std.testing.expectEqualStrings("q=zig%20lang&page=1", request.uri.query.?);

    const serialized = try request.toSlice(allocator);
    defer allocator.free(serialized);
    try std.testing.expect(mem.indexOf(u8, serialized, "GET /search?q=zig%20lang&page=1 HTTP/1.1") != null);
}
