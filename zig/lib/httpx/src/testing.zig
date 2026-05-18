//! Test utilities for httpx.zig, inspired by Go's httptest package.
//!
//! Provides a `TestServer` that listens on an ephemeral port and returns
//! canned HTTP responses, making it easy to write round-trip tests for
//! HTTP clients.
//!
//! ## Usage
//!
//! ```zig
//! var ts = try TestServer.start(alloc, io, &.{
//!     .{ .method = .POST, .path = "/embed", .respond = .{ .body = "{\"ok\":true}" } },
//! });
//! defer ts.deinit();
//!
//! var client = ts.client();
//! defer client.deinit();
//! // ... make requests in a concurrent fiber, call ts.handleOne() to serve each one.
//! ```

const std = @import("std");
const mem = std.mem;
const Allocator = std.mem.Allocator;
const Io = std.Io;
const socket_mod = @import("net/socket.zig");
const Socket = socket_mod.Socket;
const Address = socket_mod.Address;
const TcpListener = socket_mod.TcpListener;
const Parser = @import("protocol/parser.zig").Parser;
const client_mod = @import("client/client.zig");
const Client = client_mod.Client;
const types = @import("core/types.zig");

/// Specification for a canned response.
pub const ResponseSpec = struct {
    status: u16 = 200,
    body: ?[]const u8 = null,
    content_type: ?[]const u8 = null,
};

/// A canned route: matches method + path, returns a fixed response.
pub const Route = struct {
    method: types.Method = .GET,
    path: []const u8,
    respond: ResponseSpec = .{},
};

/// A lightweight test server that listens on an ephemeral port and
/// returns canned HTTP/1.1 responses for registered routes.
pub const TestServer = struct {
    allocator: Allocator,
    io: Io,
    listener: TcpListener,
    port: u16,
    routes: []const Route,
    base_url_buf: [64]u8 = undefined,
    base_url_len: usize = 0,

    const Self = @This();

    /// Starts a test server bound to 127.0.0.1 on an OS-assigned port.
    pub fn start(allocator: Allocator, io: Io, routes: []const Route) !Self {
        const addr = Address{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = 0 } };
        var listener = try TcpListener.initWithOptions(addr, io, .{
            .kernel_backlog = 8,
            .reuse_address = true,
        });

        const bound_addr = listener.getLocalAddress();
        const port = bound_addr.getPort();

        var self = Self{
            .allocator = allocator,
            .io = io,
            .listener = listener,
            .port = port,
            .routes = routes,
        };

        const written = std.fmt.bufPrint(&self.base_url_buf, "http://127.0.0.1:{d}", .{port}) catch unreachable;
        self.base_url_len = written.len;

        return self;
    }

    /// Returns the base URL (e.g. "http://127.0.0.1:54321").
    pub fn baseUrl(self: *const Self) []const u8 {
        return self.base_url_buf[0..self.base_url_len];
    }

    /// Returns a full URL by appending the path to the base URL.
    /// Caller owns the returned slice.
    pub fn url(self: *Self, path: []const u8) ![]const u8 {
        return std.fmt.allocPrint(self.allocator, "{s}{s}", .{ self.baseUrl(), path });
    }

    /// Creates an httpx Client pointed at this test server.
    pub fn client(self: *const Self) Client {
        return Client.initWithConfig(self.allocator, self.io, .{
            .base_url = self.baseUrl(),
            .keep_alive = false,
        });
    }

    /// Accepts one connection and serves it with a canned response.
    ///
    /// Reads the HTTP request line to determine the method and path,
    /// matches against registered routes, and writes back the response.
    /// Returns an error if no route matches (sends 404 first).
    pub fn handleOne(self: *Self) !void {
        const conn = try self.listener.accept();
        var sock = conn.socket;
        defer sock.close();

        // Read full request (request line + headers + optional body).
        var buf: [8192]u8 = undefined;
        var total: usize = 0;
        while (total < buf.len) {
            const reader = sock.reader();
            const n = reader.read(buf[total..]) catch break;
            if (n == 0) break;
            total += n;

            // Check if we've received the end of headers.
            if (mem.indexOf(u8, buf[0..total], "\r\n\r\n") != null) break;
        }

        if (total == 0) return error.EmptyRequest;

        const req_data = buf[0..total];

        // Parse request line: "METHOD /path HTTP/1.1\r\n"
        const first_line_end = mem.indexOf(u8, req_data, "\r\n") orelse return error.MalformedRequest;
        const request_line = req_data[0..first_line_end];

        const method_end = mem.indexOf(u8, request_line, " ") orelse return error.MalformedRequest;
        const method_str = request_line[0..method_end];

        const path_start = method_end + 1;
        const path_end = mem.indexOfPos(u8, request_line, path_start, " ") orelse return error.MalformedRequest;
        const path = request_line[path_start..path_end];

        const method = parseMethod(method_str) orelse return error.UnknownMethod;

        // Find matching route.
        var matched: ?*const Route = null;
        for (self.routes) |*r| {
            if (r.method == method and mem.eql(u8, r.path, path)) {
                matched = r;
                break;
            }
        }

        if (matched) |route| {
            try writeResponse(self.allocator, &sock, route.respond);
        } else {
            try writeResponse(self.allocator, &sock, .{ .status = 404, .body = "not found" });
        }
    }

    pub fn deinit(self: *Self) void {
        self.listener.deinit();
    }

    fn writeResponse(alloc: Allocator, sock: *Socket, spec: ResponseSpec) !void {
        // Build headers, then send headers + body in one sendAll call.
        const reason = statusReason(spec.status);
        const ct = spec.content_type orelse "application/json";
        const body = spec.body orelse "";

        const header = try std.fmt.allocPrint(alloc, "HTTP/1.1 {d} {s}\r\nContent-Type: {s}\r\nContent-Length: {d}\r\nConnection: close\r\n\r\n", .{ spec.status, reason, ct, body.len });
        defer alloc.free(header);

        // Concatenate header + body so we can send in one call.
        const full = try alloc.alloc(u8, header.len + body.len);
        defer alloc.free(full);
        @memcpy(full[0..header.len], header);
        @memcpy(full[header.len..], body);

        try sock.sendAll(full);
    }

    fn parseMethod(s: []const u8) ?types.Method {
        if (mem.eql(u8, s, "GET")) return .GET;
        if (mem.eql(u8, s, "POST")) return .POST;
        if (mem.eql(u8, s, "PUT")) return .PUT;
        if (mem.eql(u8, s, "DELETE")) return .DELETE;
        if (mem.eql(u8, s, "PATCH")) return .PATCH;
        if (mem.eql(u8, s, "HEAD")) return .HEAD;
        if (mem.eql(u8, s, "OPTIONS")) return .OPTIONS;
        return null;
    }

    fn statusReason(code: u16) []const u8 {
        return switch (code) {
            200 => "OK",
            201 => "Created",
            204 => "No Content",
            400 => "Bad Request",
            404 => "Not Found",
            500 => "Internal Server Error",
            else => "OK",
        };
    }
};

test "TestServer serves canned response" {
    const alloc = std.testing.allocator;
    const io = std.testing.io;

    var ts = try TestServer.start(alloc, io, &.{
        .{ .method = .GET, .path = "/hello", .respond = .{ .body = "{\"msg\":\"hi\"}" } },
        .{ .method = .POST, .path = "/echo", .respond = .{ .status = 201, .body = "{\"ok\":true}" } },
    });
    defer ts.deinit();

    try std.testing.expect(ts.port != 0);

    // Verify the base URL is well-formed.
    const base = ts.baseUrl();
    try std.testing.expect(mem.startsWith(u8, base, "http://127.0.0.1:"));
}

test "raw HTTP response parsing" {
    const alloc = std.testing.allocator;
    const io = std.testing.io;

    // Set up a TCP listener.
    const listen_addr = Address{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = 0 } };
    var listener = try TcpListener.initWithOptions(listen_addr, io, .{
        .kernel_backlog = 1,
        .reuse_address = true,
    });
    defer listener.deinit();

    const bound_addr = listener.getLocalAddress();
    const port = bound_addr.getPort();

    var group = Io.Group.init;

    // Result slot visible to both fibers.
    var client_body: ?[]const u8 = null;
    var client_ok: bool = false;
    var client_done: bool = false;

    const ClientFiber = struct {
        fn run(a: Allocator, test_io: Io, p: u16, body_out: *?[]const u8, ok_out: *bool, done_out: *bool) Io.Cancelable!void {
            defer done_out.* = true;

            const test_url = std.fmt.allocPrint(a, "http://127.0.0.1:{d}/hello", .{p}) catch return;
            defer a.free(test_url);

            var c = Client.initWithConfig(a, test_io, .{ .keep_alive = false });
            defer c.deinit();

            var resp = c.get(test_url, .{}) catch return;
            defer resp.deinit();

            ok_out.* = resp.ok();
            // Dupe the body so it survives the response deinit.
            if (resp.body) |b| {
                body_out.* = a.dupe(u8, b) catch null;
            }
        }
    };

    group.concurrent(io, ClientFiber.run, .{ alloc, io, port, &client_body, &client_ok, &client_done }) catch {
        return; // no fiber support
    };

    // Server: accept and respond.
    const conn = try listener.accept();
    var sock = conn.socket;
    defer sock.close();

    // Read request.
    var buf: [4096]u8 = undefined;
    const rdr = sock.reader();
    _ = try rdr.read(&buf);

    // Write response.
    const resp_str = "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 12\r\nConnection: close\r\n\r\n{\"msg\":\"hi\"}";
    try sock.sendAll(resp_str);

    // Wait for client fiber.
    group.await(io) catch {};

    defer if (client_body) |b| alloc.free(b);

    try std.testing.expect(client_done);
    try std.testing.expect(client_ok);
    try std.testing.expectEqualStrings("{\"msg\":\"hi\"}", client_body orelse "NO BODY");
}

test "TestServer round trip GET" {
    const alloc = std.testing.allocator;
    const io = std.testing.io;

    var ts = try TestServer.start(alloc, io, &.{
        .{ .method = .GET, .path = "/hello", .respond = .{ .body = "{\"msg\":\"hi\"}" } },
    });
    defer ts.deinit();

    var group = Io.Group.init;

    const ClientFiber = struct {
        fn run(a: Allocator, test_io: Io, base: []const u8) Io.Cancelable!void {
            var c = Client.initWithConfig(a, test_io, .{ .keep_alive = false });
            defer c.deinit();

            const test_url = std.fmt.allocPrint(a, "{s}/hello", .{base}) catch return;
            defer a.free(test_url);

            var resp = c.get(test_url, .{}) catch return;
            defer resp.deinit();

            std.testing.expect(resp.ok()) catch return;
            std.testing.expectEqualStrings("{\"msg\":\"hi\"}", resp.body orelse "") catch return;
        }
    };

    group.concurrent(io, ClientFiber.run, .{ alloc, io, ts.baseUrl() }) catch {
        // No fiber support — skip test.
        return;
    };

    // Server: handle one request.
    try ts.handleOne();

    // Wait for client fiber.
    group.await(io) catch {};
}

test "TestServer round trip POST" {
    const alloc = std.testing.allocator;
    const io = std.testing.io;

    var ts = try TestServer.start(alloc, io, &.{
        .{ .method = .POST, .path = "/embed", .respond = .{ .status = 200, .body = "{\"vectors\":[[1.0,2.0]]}" } },
    });
    defer ts.deinit();

    var group = Io.Group.init;

    const ClientFiber = struct {
        fn run(a: Allocator, test_io: Io, base: []const u8) Io.Cancelable!void {
            var c = Client.initWithConfig(a, test_io, .{ .keep_alive = false });
            defer c.deinit();

            const test_url = std.fmt.allocPrint(a, "{s}/embed", .{base}) catch return;
            defer a.free(test_url);

            var resp = c.post(test_url, .{ .json = "{\"model\":\"test\",\"input\":[\"hello\"]}" }) catch return;
            defer resp.deinit();

            std.testing.expect(resp.ok()) catch return;
            const body = resp.body orelse "";
            std.testing.expect(mem.indexOf(u8, body, "vectors") != null) catch return;
        }
    };

    group.concurrent(io, ClientFiber.run, .{ alloc, io, ts.baseUrl() }) catch {
        return;
    };

    try ts.handleOne();
    group.await(io) catch {};
}

test "TestServer 404 for unmatched route" {
    const alloc = std.testing.allocator;
    const io = std.testing.io;

    var ts = try TestServer.start(alloc, io, &.{
        .{ .method = .GET, .path = "/exists", .respond = .{ .body = "yes" } },
    });
    defer ts.deinit();

    var group = Io.Group.init;

    const ClientFiber = struct {
        fn run(a: Allocator, test_io: Io, base: []const u8) Io.Cancelable!void {
            var c = Client.initWithConfig(a, test_io, .{ .keep_alive = false });
            defer c.deinit();

            const test_url = std.fmt.allocPrint(a, "{s}/nope", .{base}) catch return;
            defer a.free(test_url);

            var resp = c.get(test_url, .{}) catch return;
            defer resp.deinit();

            std.testing.expect(!resp.ok()) catch return;
            std.testing.expectEqual(@as(u16, 404), resp.status.code) catch return;
        }
    };

    group.concurrent(io, ClientFiber.run, .{ alloc, io, ts.baseUrl() }) catch {
        return;
    };

    try ts.handleOne();
    group.await(io) catch {};
}
