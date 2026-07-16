// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0.

//! Narrow dependency-free HTTPS executor for the Kubernetes Lease watchdog.
//!
//! The checked-in httpx TLS client carries Antfly's Zig 0.16 compatibility
//! patch for an optional server CertificateRequest. This executor preserves the
//! projected Kubernetes CA, DNS hostname verification, service-account bearer
//! authentication, one monotonic request deadline, and a bounded response.

const std = @import("std");
const httpx = @import("httpx");
const common = @import("../common/http/http_common.zig");

pub const ZigLeaseExecutor = struct {
    alloc: std.mem.Allocator,
    io: std.Io,
    ca_path: []u8,
    client: httpx.Client,
    max_response_bytes: usize,

    pub fn init(
        alloc: std.mem.Allocator,
        io: std.Io,
        ca_path: []const u8,
        max_response_bytes: usize,
    ) !ZigLeaseExecutor {
        if (!std.fs.path.isAbsolute(ca_path) or max_response_bytes == 0 or max_response_bytes > 16 * 1024 * 1024) {
            return error.InvalidLeaseTlsConfig;
        }
        const owned_ca_path = try alloc.dupe(u8, ca_path);
        errdefer alloc.free(owned_ca_path);
        return .{
            .alloc = alloc,
            .io = io,
            .ca_path = owned_ca_path,
            .client = httpx.Client.initWithConfig(alloc, io, .{
                .timeouts = .{
                    .connect_ms = 30_000,
                    .read_ms = 30_000,
                    .write_ms = 30_000,
                },
                .retry_policy = .{ .max_retries = 0 },
                .redirect_policy = .{ .follow_redirects = false },
                .max_response_size = max_response_bytes,
                .verify_ssl = true,
                .tls_ca_file = owned_ca_path,
                .keep_alive = false,
                .cache_resolved_addresses = true,
            }),
            .max_response_bytes = max_response_bytes,
        };
    }

    pub fn deinit(self: *ZigLeaseExecutor) void {
        self.client.deinit();
        self.alloc.free(self.ca_path);
        self.* = undefined;
    }

    pub fn executor(self: *ZigLeaseExecutor) common.RequestExecutor {
        return .{ .ptr = self, .vtable = &.{ .execute = execute } };
    }

    fn execute(ptr: *anyopaque, alloc: std.mem.Allocator, req: common.HttpRequest) !common.HttpResponse {
        const self: *ZigLeaseExecutor = @ptrCast(@alignCast(ptr));
        if (req.method != .GET or req.body.len != 0 or req.headers.len != 0 or req.content_type != null) {
            return error.UnsupportedLeaseTlsRequest;
        }
        const authorization = req.authorization orelse return error.KubernetesServiceAccountTokenMissing;
        const timeout_ms = req.timeout_ms orelse return error.LeaseTlsTimeoutMissing;
        if (timeout_ms == 0) return error.Timeout;

        const uri = try std.Uri.parse(req.uri);
        if (!std.mem.eql(u8, uri.scheme, "https") or uri.user != null or uri.password != null or
            uri.query != null or uri.fragment != null)
        {
            return error.InvalidLeaseTlsURI;
        }

        var response = try self.client.request(.GET, req.uri, .{
            .headers = &.{.{ "authorization", authorization }},
            .timeout_ms = timeout_ms,
            .follow_redirects = false,
        });
        defer response.deinit();
        const source = response.body orelse "";
        const body = if (source.len == 0)
            @constCast((&[_]u8{})[0..])
        else
            try alloc.dupe(u8, source);
        return .{
            .status = response.status.code,
            .body = body,
        };
    }
};

test "Zig Lease executor rejects unscoped request shapes" {
    var executor = try ZigLeaseExecutor.init(std.testing.allocator, std.testing.io, "/tmp/ca.crt", 4096);
    defer executor.deinit();
    try std.testing.expectError(error.UnsupportedLeaseTlsRequest, executor.executor().execute(std.testing.allocator, .{
        .method = .POST,
        .uri = "https://localhost/lease",
        .authorization = "Bearer hidden",
        .timeout_ms = 1000,
    }));
    try std.testing.expectError(error.InvalidLeaseTlsURI, executor.executor().execute(std.testing.allocator, .{
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
    \\    try:
    \\        with ctx.wrap_socket(conn, server_side=True) as tls_conn:
    \\            request = b''
    \\            while b'\r\n\r\n' not in request:
    \\                chunk = tls_conn.recv(4096)
    \\                if not chunk:
    \\                    break
    \\                request += chunk
    \\            authorized = b'authorization: bearer test-token\r\n' in request.lower()
    \\            body = b'{"ok":true}' if authorized else b'{"ok":false}'
    \\            status = b'200 OK' if authorized else b'401 Unauthorized'
    \\            tls_conn.sendall(b'HTTP/1.1 ' + status + b'\r\nContent-Type: application/json\r\nContent-Length: ' + str(len(body)).encode() + b'\r\nConnection: close\r\n\r\n' + body)
    \\    except ssl.SSLError:
    \\        pass
    \\listener.close()
;

fn spawnOptionalClientAuthServer(
    alloc: std.mem.Allocator,
    io: std.Io,
    tmp: *std.testing.TmpDir,
) !struct { child: std.process.Child, cert_path: []u8, port: u16 } {
    try tmp.dir.writeFile(io, .{ .sub_path = "cert.pem", .data = @embedFile("testdata/optional-client-auth.crt") });
    try tmp.dir.writeFile(io, .{ .sub_path = "key.pem", .data = @embedFile("testdata/optional-client-auth.key") });
    try tmp.dir.writeFile(io, .{ .sub_path = "server.py", .data = optional_client_auth_server });
    const root = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(root);
    const cert_path = try std.fs.path.join(alloc, &.{ root, "cert.pem" });
    errdefer alloc.free(cert_path);

    var child = std.process.spawn(io, .{
        .argv = &.{ "python3", "server.py", "cert.pem", "key.pem", "port.txt" },
        .cwd = .{ .dir = tmp.dir },
        .stdin = .ignore,
        .stdout = .ignore,
        .stderr = .inherit,
    }) catch |err| switch (err) {
        error.FileNotFound => return error.SkipZigTest,
        else => return err,
    };
    errdefer child.kill(io);

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
    return .{
        .child = child,
        .cert_path = cert_path,
        .port = port orelse return error.TestExpectedEqual,
    };
}

test "Zig Lease executor accepts optional CertificateRequest with projected CA and verified hostname" {
    const alloc = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var server = try spawnOptionalClientAuthServer(alloc, io, &tmp);
    defer {
        server.child.kill(io);
        alloc.free(server.cert_path);
    }
    const uri = try std.fmt.allocPrint(alloc, "https://localhost:{d}/lease", .{server.port});
    defer alloc.free(uri);
    var executor = try ZigLeaseExecutor.init(alloc, io, server.cert_path, 4096);
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

test "Zig Lease executor rejects optional CertificateRequest hostname mismatch" {
    const alloc = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var server = try spawnOptionalClientAuthServer(alloc, io, &tmp);
    defer {
        server.child.kill(io);
        alloc.free(server.cert_path);
    }
    const uri = try std.fmt.allocPrint(alloc, "https://127.0.0.1:{d}/lease", .{server.port});
    defer alloc.free(uri);
    var executor = try ZigLeaseExecutor.init(alloc, io, server.cert_path, 4096);
    defer executor.deinit();
    try std.testing.expectError(error.CertificateHostMismatch, executor.executor().execute(alloc, .{
        .method = .GET,
        .uri = uri,
        .authorization = "Bearer test-token",
        .timeout_ms = 3000,
    }));
}
