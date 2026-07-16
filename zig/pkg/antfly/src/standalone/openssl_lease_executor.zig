// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0.

//! Narrow HTTPS executor for the in-cluster Kubernetes Lease watchdog.
//!
//! Zig 0.16's TLS client rejects a server-side optional CertificateRequest.
//! Kubernetes API servers commonly send that request because they also support
//! client-certificate authentication. This executor keeps the existing
//! RequestExecutor boundary while using OpenSSL only for the watchdog's
//! authenticated, verified, bounded GET.

const std = @import("std");
const common = @import("../common/http/http_common.zig");

const TransportResult = enum(c_int) {
    ok = 0,
    invalid_request = 1,
    resolve_failed = 2,
    connect_failed = 3,
    timeout = 4,
    tls_init_failed = 5,
    tls_verify_failed = 6,
    write_failed = 7,
    read_failed = 8,
    response_too_large = 9,
    invalid_response = 10,
    out_of_memory = 11,
    _,
};

extern fn antfly_openssl_lease_get(
    host: [*:0]const u8,
    port: u16,
    ca_path: [*:0]const u8,
    authorization: [*:0]const u8,
    path: [*:0]const u8,
    timeout_ms: u32,
    max_body: usize,
    out_status: *u16,
    out_body: *?[*]u8,
    out_body_len: *usize,
) callconv(.c) c_int;

extern fn antfly_openssl_lease_free(ptr: ?*anyopaque) callconv(.c) void;

pub const OpenSslLeaseExecutor = struct {
    alloc: std.mem.Allocator,
    io: std.Io,
    ca_path: [:0]u8,
    max_response_bytes: usize,

    pub fn init(
        alloc: std.mem.Allocator,
        io: std.Io,
        ca_path: []const u8,
        max_response_bytes: usize,
    ) !OpenSslLeaseExecutor {
        if (ca_path.len == 0 or max_response_bytes == 0 or max_response_bytes > 16 * 1024 * 1024) {
            return error.InvalidLeaseOpenSslConfig;
        }
        return .{
            .alloc = alloc,
            .io = io,
            .ca_path = try alloc.dupeZ(u8, ca_path),
            .max_response_bytes = max_response_bytes,
        };
    }

    pub fn deinit(self: *OpenSslLeaseExecutor) void {
        self.alloc.free(self.ca_path);
        self.* = undefined;
    }

    pub fn executor(self: *OpenSslLeaseExecutor) common.RequestExecutor {
        return .{ .ptr = self, .vtable = &.{ .execute = execute } };
    }

    fn execute(ptr: *anyopaque, alloc: std.mem.Allocator, req: common.HttpRequest) !common.HttpResponse {
        const self: *OpenSslLeaseExecutor = @ptrCast(@alignCast(ptr));
        if (req.method != .GET or req.body.len != 0 or req.headers.len != 0 or req.content_type != null) {
            return error.UnsupportedLeaseOpenSslRequest;
        }
        const authorization = req.authorization orelse return error.KubernetesServiceAccountTokenMissing;
        const timeout_ms = req.timeout_ms orelse return error.LeaseOpenSslTimeoutMissing;
        if (timeout_ms == 0) return error.Timeout;

        const uri = try std.Uri.parse(req.uri);
        if (!std.mem.eql(u8, uri.scheme, "https") or uri.user != null or uri.password != null or
            uri.query != null or uri.fragment != null)
        {
            return error.InvalidLeaseOpenSslURI;
        }
        const host_name = try uri.getHostAlloc(alloc);
        const host = try alloc.dupeZ(u8, host_name.bytes);
        defer alloc.free(host);
        const raw_path = try uri.path.toRawMaybeAlloc(alloc);
        if (raw_path.len == 0 or raw_path[0] != '/') return error.InvalidLeaseOpenSslURI;
        const path = try alloc.dupeZ(u8, raw_path);
        defer alloc.free(path);
        const authorization_z = try alloc.dupeZ(u8, authorization);
        defer {
            std.crypto.secureZero(u8, authorization_z);
            alloc.free(authorization_z);
        }

        var status: u16 = 0;
        var c_body: ?[*]u8 = null;
        var body_len: usize = 0;
        const result: TransportResult = @enumFromInt(antfly_openssl_lease_get(
            host,
            uri.port orelse 443,
            self.ca_path,
            authorization_z,
            path,
            timeout_ms,
            self.max_response_bytes,
            &status,
            &c_body,
            &body_len,
        ));
        if (result != .ok) return transportError(result);
        const source = c_body orelse return error.InvalidLeaseOpenSslResponse;
        defer antfly_openssl_lease_free(source);
        const body = if (body_len == 0)
            @constCast((&[_]u8{})[0..])
        else
            try alloc.dupe(u8, source[0..body_len]);
        return .{ .status = status, .body = body };
    }

    fn transportError(result: TransportResult) anyerror {
        return switch (result) {
            .invalid_request => error.InvalidLeaseOpenSslRequest,
            .resolve_failed => error.LeaseOpenSslResolveFailed,
            .connect_failed => error.LeaseOpenSslConnectFailed,
            .timeout => error.Timeout,
            .tls_init_failed => error.LeaseOpenSslTlsInitializationFailed,
            .tls_verify_failed => error.LeaseOpenSslTlsVerificationFailed,
            .write_failed => error.LeaseOpenSslWriteFailed,
            .read_failed => error.LeaseOpenSslReadFailed,
            .response_too_large => error.ResponseTooLarge,
            .invalid_response => error.InvalidLeaseOpenSslResponse,
            .out_of_memory => error.OutOfMemory,
            .ok => unreachable,
            _ => error.LeaseOpenSslUnknownFailure,
        };
    }
};

test "OpenSSL Lease executor rejects unscoped request shapes" {
    var executor = try OpenSslLeaseExecutor.init(std.testing.allocator, std.testing.io, "/tmp/ca.crt", 4096);
    defer executor.deinit();
    try std.testing.expectError(error.UnsupportedLeaseOpenSslRequest, executor.executor().execute(std.testing.allocator, .{
        .method = .POST,
        .uri = "https://localhost/lease",
        .authorization = "Bearer hidden",
        .timeout_ms = 1000,
    }));
    try std.testing.expectError(error.InvalidLeaseOpenSslURI, executor.executor().execute(std.testing.allocator, .{
        .method = .GET,
        .uri = "http://localhost/lease",
        .authorization = "Bearer hidden",
        .timeout_ms = 1000,
    }));
}

const optional_client_auth_server =
    \\import pathlib
    \\import socket
    \\import ssl
    \\import sys
    \\import time
    \\cert, key, port_file = sys.argv[1:4]
    \\listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    \\listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    \\listener.bind(('127.0.0.1', 0))
    \\listener.listen(1)
    \\pathlib.Path(port_file).write_text(str(listener.getsockname()[1]))
    \\ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    \\ctx.load_cert_chain(certfile=cert, keyfile=key)
    \\ctx.load_verify_locations(cafile=cert)
    \\ctx.verify_mode = ssl.CERT_OPTIONAL
    \\conn, _ = listener.accept()
    \\with conn:
    \\    with ctx.wrap_socket(conn, server_side=True) as tls_conn:
    \\        request = b''
    \\        while b'\r\n\r\n' not in request:
    \\            chunk = tls_conn.recv(4096)
    \\            if not chunk:
    \\                break
    \\            request += chunk
    \\        authorized = b'Authorization: Bearer test-token\r\n' in request
    \\        body = b'{"ok":true}' if authorized else b'{"ok":false}'
    \\        status = b'200 OK' if authorized else b'401 Unauthorized'
    \\        tls_conn.sendall(b'HTTP/1.1 ' + status + b'\r\nContent-Type: application/json\r\nContent-Length: ' + str(len(body)).encode() + b'\r\nConnection: close\r\n\r\n' + body)
    \\        time.sleep(5)
    \\listener.close()
;

test "OpenSSL Lease executor accepts optional CertificateRequest with verified hostname" {
    const alloc = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(io, .{ .sub_path = "cert.pem", .data = @embedFile("testdata/optional-client-auth.crt") });
    try tmp.dir.writeFile(io, .{ .sub_path = "key.pem", .data = @embedFile("testdata/optional-client-auth.key") });
    try tmp.dir.writeFile(io, .{ .sub_path = "server.py", .data = optional_client_auth_server });
    const root = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(root);
    const cert_path = try std.fs.path.join(alloc, &.{ root, "cert.pem" });
    defer alloc.free(cert_path);

    var child = std.process.spawn(io, .{
        .argv = &.{ "python3", "server.py", "cert.pem", "key.pem", "port.txt" },
        .cwd = .{ .dir = tmp.dir },
        .stdin = .ignore,
        .stdout = .ignore,
        .stderr = .inherit,
    }) catch |err| switch (err) {
        error.FileNotFound => return,
        else => return err,
    };
    defer child.kill(io);

    var port: ?u16 = null;
    var attempt: usize = 0;
    while (attempt < 40 and port == null) : (attempt += 1) {
        const raw = tmp.dir.readFileAlloc(io, "port.txt", alloc, .limited(32)) catch |err| switch (err) {
            error.FileNotFound => {
                io.sleep(std.Io.Duration.fromMilliseconds(50), .awake) catch {};
                continue;
            },
            else => return err,
        };
        defer alloc.free(raw);
        port = try std.fmt.parseInt(u16, std.mem.trim(u8, raw, " \t\r\n"), 10);
    }
    const resolved_port = port orelse return error.TestExpectedEqual;
    const uri = try std.fmt.allocPrint(alloc, "https://localhost:{d}/lease", .{resolved_port});
    defer alloc.free(uri);
    var executor = try OpenSslLeaseExecutor.init(alloc, io, cert_path, 4096);
    defer executor.deinit();
    var response = try executor.executor().execute(alloc, .{
        .method = .GET,
        .uri = uri,
        .authorization = "Bearer test-token",
        .timeout_ms = 3000,
    });
    defer response.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), response.status);
    try std.testing.expectEqualStrings("{\"ok\":true}", response.body);
}
