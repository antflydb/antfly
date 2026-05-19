//! TLS/SSL Support for httpx.zig
//!
//! Provides TLS configuration and a session wrapper for HTTPS connections.
//! This module uses Zig's standard library TLS client (`std.crypto.tls.Client`).
//!
//! ## What Works Today
//!
//! - **Client TLS 1.2/1.3**: Full handshake, certificate verification, SNI.
//! - **HTTP/2 over TLS**: Set `http2_enabled` or `force_http2` in `ClientConfig`.
//!   The client sends the h2 connection preface directly ("prior knowledge" mode,
//!   RFC 7540 §3.4) and multiplexes streams on a single connection.
//!
//! ## Stdlib Limitations (Zig 0.16)
//!
//! - **No server TLS**: `std.crypto.tls` provides `Client` but no `Server`.
//!   Deploy the httpx server behind a TLS-terminating reverse proxy (nginx,
//!   Caddy, etc.) that forwards plaintext h2c or HTTP/1.1 to the server.
//! - **No ALPN read-back**: `std.crypto.tls.Client` does not expose the
//!   negotiated ALPN protocol. The `alpn_protocols` field and
//!   `negotiated_protocol` session field are scaffolded for forward
//!   compatibility — when the stdlib adds ALPN support, wire it into
//!   `TlsSession.handshake()` at the marked scaffold site.

const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;
const builtin = @import("builtin");
const Socket = @import("../net/socket.zig").Socket;
const SocketIoReader = @import("../net/socket.zig").SocketIoReader;
const SocketIoWriter = @import("../net/socket.zig").SocketIoWriter;

/// Minimum TLS version configuration.
pub const TlsVersion = enum {
    tls_1_0,
    tls_1_1,
    tls_1_2,
    tls_1_3,

    pub fn toString(self: TlsVersion) []const u8 {
        return switch (self) {
            .tls_1_0 => "TLSv1.0",
            .tls_1_1 => "TLSv1.1",
            .tls_1_2 => "TLSv1.2",
            .tls_1_3 => "TLSv1.3",
        };
    }
};

/// TLS verification mode.
pub const VerifyMode = enum {
    none,
    peer,
    fail_if_no_peer_cert,
    client_once,
};

/// TLS configuration for clients and servers.
///
/// ## Implemented Fields
///
/// - `verify_mode`: `.none` disables certificate verification; all other
///   modes enable it (the stdlib does not distinguish sub-modes).
/// - `verify_hostname`: controls SNI hostname verification.
/// - `server_name`: explicit SNI override.
///
/// ## Unimplemented Fields (Zig 0.16 stdlib limitation)
///
/// The following fields are accepted but **not wired into the handshake**.
/// Setting them has no effect today. They are retained for forward
/// compatibility — when the stdlib gains support, they will be plumbed
/// into `TlsSession.handshake()`.
///
/// - `min_version` / `max_version`: stdlib negotiates TLS 1.2/1.3 automatically.
/// - `ca_file` / `ca_path`: system CA bundle is always used; custom CAs are ignored.
/// - `cert_file` / `key_file`: mutual TLS (client certificates) is not supported.
/// - `alpn_protocols`: ALPN is not exposed by `std.crypto.tls.Client`.
/// - `cipher_suites`: cipher selection is not configurable.
pub const TlsConfig = struct {
    allocator: Allocator,
    /// Unimplemented: stdlib negotiates version automatically.
    min_version: TlsVersion = .tls_1_2,
    /// Unimplemented: stdlib negotiates version automatically.
    max_version: TlsVersion = .tls_1_3,
    verify_mode: VerifyMode = .peer,
    verify_hostname: bool = true,
    /// Unimplemented: system CA bundle is always used.
    ca_file: ?[]const u8 = null,
    /// Unimplemented: system CA bundle is always used.
    ca_path: ?[]const u8 = null,
    /// Unimplemented: mutual TLS is not supported.
    cert_file: ?[]const u8 = null,
    /// Unimplemented: mutual TLS is not supported.
    key_file: ?[]const u8 = null,
    /// Unimplemented: ALPN is not exposed by `std.crypto.tls.Client`.
    alpn_protocols: []const []const u8 = &.{"http/1.1"},
    /// Unimplemented: cipher selection is not configurable.
    cipher_suites: ?[]const u8 = null,
    server_name: ?[]const u8 = null,

    const Self = @This();

    /// Creates a default TLS configuration.
    pub fn init(allocator: Allocator) Self {
        return .{ .allocator = allocator };
    }

    /// Creates a configuration that skips certificate verification.
    pub fn insecure(allocator: Allocator) Self {
        var config = init(allocator);
        config.verify_mode = .none;
        config.verify_hostname = false;
        return config;
    }

    /// Sets the CA certificate file.
    pub fn setCaFile(self: *Self, path: []const u8) void {
        self.ca_file = path;
    }

    /// Sets the client certificate and key files.
    pub fn setClientCert(self: *Self, cert_file: []const u8, key_file: []const u8) void {
        self.cert_file = cert_file;
        self.key_file = key_file;
    }

    /// Sets the server name for SNI.
    pub fn setServerName(self: *Self, name: []const u8) void {
        self.server_name = name;
    }

    /// Creates a copy of the configuration.
    pub fn clone(self: *const Self) Self {
        return self.*;
    }
};

/// TLS session state.
pub const TlsSession = struct {
    allocator: Allocator,
    io: Io,
    config: TlsConfig,
    connected: bool = false,
    socket: ?*Socket = null,

    net_read_buf: ?[]u8 = null,
    net_write_buf: ?[]u8 = null,
    tls_read_buf: ?[]u8 = null,
    tls_write_buf: ?[]u8 = null,
    net_in: ?SocketIoReader = null,
    net_out: ?SocketIoWriter = null,

    ca_bundle: ?std.crypto.Certificate.Bundle = null,
    ca_bundle_lock: std.Io.RwLock = .init,
    client: ?std.crypto.tls.Client = null,

    /// Protocol negotiated via ALPN (e.g. "h2", "http/1.1").
    /// Currently always null because `std.crypto.tls.Client` does not expose
    /// the negotiated protocol. Set manually when using "prior knowledge" mode.
    negotiated_protocol: ?[]const u8 = null,

    const Self = @This();

    /// Creates a new TLS session with the given configuration.
    pub fn init(config: TlsConfig, io: Io) Self {
        return .{
            .allocator = config.allocator,
            .io = io,
            .config = config,
        };
    }

    /// Releases session resources.
    /// Sends a TLS close_notify alert before tearing down, so the peer can
    /// distinguish a clean close from a truncation attack.
    pub fn deinit(self: *Self) void {
        if (self.client) |*c| {
            c.end() catch {};
            self.client = null;
        }

        if (self.ca_bundle) |*bundle| {
            bundle.deinit(self.allocator);
            self.ca_bundle = null;
        }

        if (self.net_read_buf) |buf| self.allocator.free(buf);
        if (self.net_write_buf) |buf| self.allocator.free(buf);
        if (self.tls_read_buf) |buf| self.allocator.free(buf);
        if (self.tls_write_buf) |buf| self.allocator.free(buf);

        self.net_read_buf = null;
        self.net_write_buf = null;
        self.tls_read_buf = null;
        self.tls_write_buf = null;
        self.net_in = null;
        self.net_out = null;
        self.connected = false;
    }

    /// Attaches a connected socket that will carry the TLS session.
    pub fn attachSocket(self: *Self, socket: *Socket) void {
        self.socket = socket;
    }

    /// Performs the TLS handshake. Must be called exactly once per session.
    /// Returns error.AlreadyConnected if called again after a successful handshake.
    pub fn handshake(self: *Self, hostname: []const u8) !void {
        if (self.connected) return error.AlreadyConnected;
        const tls = std.crypto.tls;
        const sock = self.socket orelse return error.MissingTransport;
        const min_tls_buf = tls.Client.min_buffer_len;
        const net_buf_len: usize = @max(16 * 1024, min_tls_buf);

        // Allocate buffers once per session.
        if (self.net_read_buf == null) self.net_read_buf = try self.allocator.alloc(u8, net_buf_len);
        if (self.net_write_buf == null) self.net_write_buf = try self.allocator.alloc(u8, net_buf_len);

        if (self.tls_read_buf == null) self.tls_read_buf = try self.allocator.alloc(u8, min_tls_buf);
        if (self.tls_write_buf == null) self.tls_write_buf = try self.allocator.alloc(u8, min_tls_buf);

        const net_in = SocketIoReader.init(sock, self.net_read_buf.?);
        const net_out = SocketIoWriter.init(sock, self.net_write_buf.?);
        self.net_in = net_in;
        self.net_out = net_out;

        const verify = self.config.verify_mode != .none;
        const verify_host = verify and self.config.verify_hostname;

        // System CA bundle (cross-platform); optional if verification is disabled.
        if (verify) {
            var bundle: std.crypto.Certificate.Bundle = .{
                .map = .{},
                .bytes = .empty,
            };
            try bundle.rescan(self.allocator, self.io, Io.Timestamp.now(self.io, .real));
            // Transfer ownership to self immediately so deinit() handles cleanup
            // if a later fallible operation (e.g. tls.Client.init) fails. An errdefer
            // on the local `bundle` would cause a double-free because `bundle` is
            // captured by value while `self.ca_bundle` holds the same allocation.
            self.ca_bundle = bundle;
        }

        const sni_host = self.config.server_name orelse hostname;

        var entropy: [tls.Client.Options.entropy_len]u8 = undefined;
        self.io.randomSecure(&entropy) catch return error.EntropyUnavailable;

        const now = Io.Timestamp.now(self.io, .real);

        const client = if (@hasField(tls.Client.Options, "realtime_now"))
            try tls.Client.init(&self.net_in.?.reader_iface, &self.net_out.?.writer_iface, .{
                .host = if (verify_host) .{ .explicit = sni_host } else .{ .no_verification = {} },
                .ca = if (verify) self.caBundleOption() else .{ .no_verification = {} },
                .ssl_key_log = null,
                // Many HTTP servers close TLS transports without sending close_notify.
                // Higher HTTP framing still validates completeness for chunked/content-length bodies.
                .allow_truncation_attacks = true,
                .write_buffer = self.tls_write_buf.?,
                .read_buffer = self.tls_read_buf.?,
                .entropy = &entropy,
                .realtime_now = now,
            })
        else
            try tls.Client.init(&self.net_in.?.reader_iface, &self.net_out.?.writer_iface, .{
                .host = if (verify_host) .{ .explicit = sni_host } else .{ .no_verification = {} },
                .ca = if (verify) self.caBundleOption() else .{ .no_verification = {} },
                .ssl_key_log = null,
                // Many HTTP servers close TLS transports without sending close_notify.
                // Higher HTTP framing still validates completeness for chunked/content-length bodies.
                .allow_truncation_attacks = true,
                .write_buffer = self.tls_write_buf.?,
                .read_buffer = self.tls_read_buf.?,
                .entropy = &entropy,
                .realtime_now_seconds = now.toSeconds(),
            });

        self.client = client;
        self.connected = true;

        // Scaffold: read negotiated ALPN protocol from the TLS client.
        // Zig 0.16 `std.crypto.tls.Client` does not expose this field.
        // When the stdlib adds ALPN support, wire it in here:
        //   self.negotiated_protocol = client.alpn_protocol;
        // Until then, callers that need h2 use "prior knowledge" mode.
    }

    fn caBundleOption(self: *Self) @FieldType(std.crypto.tls.Client.Options, "ca") {
        const CaOption = @FieldType(std.crypto.tls.Client.Options, "ca");
        const BundleOption = @FieldType(CaOption, "bundle");
        if (BundleOption == std.crypto.Certificate.Bundle) {
            return .{ .bundle = self.ca_bundle.? };
        }
        return .{ .bundle = .{
            .gpa = self.allocator,
            .io = self.io,
            .lock = &self.ca_bundle_lock,
            .bundle = if (self.ca_bundle) |*bundle| bundle else unreachable,
        } };
    }

    /// Reads decrypted data from the session.
    pub fn read(self: *Self, buffer: []u8) !usize {
        const c = if (self.client) |*c| c else return error.NotConnected;
        var iov = [_][]u8{buffer};
        return c.reader.readVec(&iov) catch |err| switch (err) {
            error.EndOfStream => 0,
            else => error.ReadFailed,
        };
    }

    /// Writes data to be encrypted and sent.
    pub fn write(self: *Self, data: []const u8) !usize {
        const c = if (self.client) |*c| c else return error.NotConnected;
        try c.writer.writeAll(data);
        try c.writer.flush();
        const net_out = if (self.net_out) |*w| w else return error.NotConnected;
        try net_out.writer_iface.flush();
        return data.len;
    }

    pub fn flush(self: *Self) !void {
        const c = if (self.client) |*c| c else return error.NotConnected;
        try c.writer.flush();
        const net_out = if (self.net_out) |*w| w else return error.NotConnected;
        try net_out.writer_iface.flush();
    }

    /// Returns an I/O reader for decrypted TLS payload.
    pub fn getReader(self: *Self) !*std.Io.Reader {
        const c = if (self.client) |*c| c else return error.NotConnected;
        return &c.reader;
    }

    /// Returns an I/O writer for TLS-encrypted payload.
    pub fn getWriter(self: *Self) !*std.Io.Writer {
        const c = if (self.client) |*c| c else return error.NotConnected;
        return &c.writer;
    }

    /// Closes the TLS session and releases all resources.
    /// This is equivalent to deinit() — after close(), the session cannot be reused.
    /// Closes the TLS session and releases all resources.
    /// Equivalent to deinit() — after close(), the session cannot be reused.
    pub fn close(self: *Self) void {
        self.deinit();
    }
};

/// Parses a PEM-encoded certificate.
pub fn parsePemCertificate(allocator: Allocator, pem_data: []const u8) ![]const u8 {
    const begin_marker = "-----BEGIN CERTIFICATE-----";
    const end_marker = "-----END CERTIFICATE-----";

    const start = std.mem.indexOf(u8, pem_data, begin_marker) orelse return error.InvalidPem;
    const end = std.mem.indexOf(u8, pem_data, end_marker) orelse return error.InvalidPem;

    if (end <= start + begin_marker.len) return error.InvalidPem;

    var base64_block = pem_data[start + begin_marker.len .. end];
    base64_block = std.mem.trim(u8, base64_block, " \t\r\n");

    // Remove all whitespace/newlines from the base64 body.
    var compact = std.ArrayListUnmanaged(u8).empty;
    defer compact.deinit(allocator);
    for (base64_block) |ch| {
        if (ch == '\r' or ch == '\n' or ch == '\t' or ch == ' ') continue;
        try compact.append(allocator, ch);
    }

    const decoder = std.base64.standard.Decoder;
    const out_len = try decoder.calcSizeForSlice(compact.items);
    const out = try allocator.alloc(u8, out_len);
    errdefer allocator.free(out);
    _ = decoder.decode(out, compact.items) catch return error.InvalidPem;
    return out;
}

/// Returns the system's default CA certificate path.
pub fn getSystemCaPath() ?[]const u8 {
    return switch (builtin.os.tag) {
        .linux => "/etc/ssl/certs/ca-certificates.crt",
        .macos => "/etc/ssl/cert.pem",
        .windows => null,
        .freebsd, .netbsd, .openbsd => "/etc/ssl/cert.pem",
        else => null,
    };
}

test "TlsConfig initialization" {
    const allocator = std.testing.allocator;
    const config = TlsConfig.init(allocator);

    try std.testing.expectEqual(TlsVersion.tls_1_2, config.min_version);
    try std.testing.expectEqual(TlsVersion.tls_1_3, config.max_version);
    try std.testing.expect(config.verify_hostname);
}

test "TlsConfig insecure" {
    const allocator = std.testing.allocator;
    const config = TlsConfig.insecure(allocator);

    try std.testing.expectEqual(VerifyMode.none, config.verify_mode);
    try std.testing.expect(!config.verify_hostname);
}

test "TlsSession initialization" {
    const allocator = std.testing.allocator;
    const config = TlsConfig.init(allocator);
    var session = TlsSession.init(config, std.testing.io);
    defer session.deinit();

    try std.testing.expect(!session.connected);
}

test "System CA path" {
    const path = getSystemCaPath();
    if (builtin.os.tag == .linux or builtin.os.tag == .macos) {
        try std.testing.expect(path != null);
    }
}

test "TLS version strings" {
    try std.testing.expectEqualStrings("TLSv1.2", TlsVersion.tls_1_2.toString());
    try std.testing.expectEqualStrings("TLSv1.3", TlsVersion.tls_1_3.toString());
}

test "parsePemCertificate decodes base64 payload" {
    const allocator = std.testing.allocator;
    const pem =
        "-----BEGIN CERTIFICATE-----\n" ++
        "AQID\n" ++
        "-----END CERTIFICATE-----\n";

    const der = try parsePemCertificate(allocator, pem);
    defer allocator.free(der);

    try std.testing.expectEqual(@as(usize, 3), der.len);
    try std.testing.expectEqual(@as(u8, 0x01), der[0]);
    try std.testing.expectEqual(@as(u8, 0x02), der[1]);
    try std.testing.expectEqual(@as(u8, 0x03), der[2]);
}
