// Copyright 2026 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

const std = @import("std");
const builtin = @import("builtin");
const objectstore = @import("objectstore");

const Allocator = std.mem.Allocator;

pub const DownloadedContent = struct {
    content_type: []u8,
    data: []u8,

    pub fn deinit(self: *DownloadedContent, alloc: Allocator) void {
        alloc.free(self.content_type);
        alloc.free(self.data);
        self.* = undefined;
    }
};

pub const HttpError = struct {
    status: u16,
    message: []const u8,
};

pub const DownloadOutcome = union(enum) {
    ok: DownloadedContent,
    http_error: HttpError,
};

pub const HTTPHeader = struct {
    name: []const u8,
    value: []const u8,
};

pub const ContentSecurityConfig = struct {
    allowed_hosts: ?[]const []u8 = null,
    block_private_ips: ?bool = null,
    max_download_size_bytes: ?u64 = null,
    download_timeout_seconds: ?u32 = null,
    max_image_dimension: ?u32 = null,
    allowed_paths: ?[]const []u8 = null,
    user_agent: ?[]u8 = null,

    pub fn deinit(self: *ContentSecurityConfig, alloc: std.mem.Allocator) void {
        if (self.allowed_hosts) |values| freeOwnedStringSlice(alloc, values);
        if (self.allowed_paths) |values| freeOwnedStringSlice(alloc, values);
        if (self.user_agent) |value| alloc.free(value);
        self.* = undefined;
    }
};

pub const S3CredentialsConfig = struct {
    endpoint: ?[]u8 = null,
    use_ssl: ?bool = null,
    access_key_id: ?[]u8 = null,
    secret_access_key: ?[]u8 = null,
    session_token: ?[]u8 = null,

    pub fn deinit(self: *S3CredentialsConfig, alloc: std.mem.Allocator) void {
        if (self.endpoint) |value| alloc.free(value);
        if (self.access_key_id) |value| alloc.free(value);
        if (self.secret_access_key) |value| alloc.free(value);
        if (self.session_token) |value| alloc.free(value);
        self.* = undefined;
    }
};

pub const S3CredentialConfig = struct {
    endpoint: ?[]u8 = null,
    use_ssl: ?bool = null,
    access_key_id: ?[]u8 = null,
    secret_access_key: ?[]u8 = null,
    session_token: ?[]u8 = null,
    buckets: ?[]const []u8 = null,
    security: ?ContentSecurityConfig = null,

    pub fn deinit(self: *S3CredentialConfig, alloc: std.mem.Allocator) void {
        if (self.endpoint) |value| alloc.free(value);
        if (self.access_key_id) |value| alloc.free(value);
        if (self.secret_access_key) |value| alloc.free(value);
        if (self.session_token) |value| alloc.free(value);
        if (self.buckets) |values| freeOwnedStringSlice(alloc, values);
        if (self.security) |*security| security.deinit(alloc);
        self.* = undefined;
    }
};

pub const HTTPCredentialConfig = struct {
    base_url: ?[]u8 = null,
    headers: std.StringArrayHashMapUnmanaged([]u8) = .{},
    security: ?ContentSecurityConfig = null,

    pub fn deinit(self: *HTTPCredentialConfig, alloc: std.mem.Allocator) void {
        if (self.base_url) |value| alloc.free(value);
        var it = self.headers.iterator();
        while (it.next()) |entry| {
            alloc.free(entry.key_ptr.*);
            alloc.free(entry.value_ptr.*);
        }
        self.headers.deinit(alloc);
        if (self.security) |*security| security.deinit(alloc);
        self.* = undefined;
    }
};

pub const RemoteContentConfig = struct {
    security: ?ContentSecurityConfig = null,
    default_s3: ?[]u8 = null,
    s3: std.StringArrayHashMapUnmanaged(S3CredentialConfig) = .{},
    http: std.StringArrayHashMapUnmanaged(HTTPCredentialConfig) = .{},
    /// Optional process-owned publisher for hot-reloadable configuration. The
    /// publisher is borrowed and must outlive every use of this facade.
    runtime: ?RemoteContentRuntime = null,

    pub const Snapshot = struct {
        config: *const RemoteContentConfig,
        context: ?*anyopaque = null,
        release_fn: ?*const fn (*anyopaque) void = null,

        pub fn deinit(self: *Snapshot) void {
            if (self.context) |context| self.release_fn.?(context);
            self.* = undefined;
        }
    };

    pub const RuntimeHealth = struct {
        generation: u64,
        hash: [32]u8,
        last_reload_failed: bool,
        stale_snapshot: bool,
        reload_successes: u64,
        reload_failures: u64,
    };

    pub const RemoteContentRuntime = struct {
        context: *anyopaque,
        acquire_fn: *const fn (*anyopaque) ?Snapshot,
        health_fn: *const fn (*anyopaque) RuntimeHealth,

        pub fn acquire(self: RemoteContentRuntime) ?Snapshot {
            return self.acquire_fn(self.context);
        }

        pub fn health(self: RemoteContentRuntime) RuntimeHealth {
            return self.health_fn(self.context);
        }
    };

    pub fn acquire(self: *const RemoteContentConfig) Snapshot {
        if (self.runtime) |runtime| return runtime.acquire() orelse .{ .config = self };
        return .{ .config = self };
    }

    pub fn runtimeHealth(self: *const RemoteContentConfig) ?RuntimeHealth {
        return if (self.runtime) |runtime| runtime.health() else null;
    }

    pub fn getS3(self: *const RemoteContentConfig, name: []const u8) ?*const S3CredentialConfig {
        return self.s3.getPtr(name);
    }

    pub fn getHttp(self: *const RemoteContentConfig, name: []const u8) ?*const HTTPCredentialConfig {
        return self.http.getPtr(name);
    }

    pub fn deinit(self: *RemoteContentConfig, alloc: std.mem.Allocator) void {
        if (self.security) |*security| security.deinit(alloc);
        if (self.default_s3) |value| alloc.free(value);

        var s3_it = self.s3.iterator();
        while (s3_it.next()) |entry| {
            alloc.free(entry.key_ptr.*);
            entry.value_ptr.deinit(alloc);
        }
        self.s3.deinit(alloc);

        var http_it = self.http.iterator();
        while (http_it.next()) |entry| {
            alloc.free(entry.key_ptr.*);
            entry.value_ptr.deinit(alloc);
        }
        self.http.deinit(alloc);
        self.* = undefined;
    }
};

pub fn downloadContentAlloc(
    alloc: Allocator,
    uri: []const u8,
    security: ?*const ContentSecurityConfig,
    s3_credentials: ?*const S3CredentialsConfig,
) !DownloadedContent {
    const outcome = try downloadContentOutcomeAlloc(alloc, uri, security, s3_credentials);
    return switch (outcome) {
        .ok => |downloaded| downloaded,
        .http_error => error.HttpFetchFailed,
    };
}

pub fn downloadContentOutcomeAlloc(
    alloc: Allocator,
    uri: []const u8,
    security: ?*const ContentSecurityConfig,
    s3_credentials: ?*const S3CredentialsConfig,
) !DownloadOutcome {
    return try downloadContentOutcomeAllocWithHeaders(alloc, uri, security, s3_credentials, null);
}

pub fn downloadContentOutcomeAllocWithHeaders(
    alloc: Allocator,
    uri: []const u8,
    security: ?*const ContentSecurityConfig,
    s3_credentials: ?*const S3CredentialsConfig,
    http_headers: ?[]const HTTPHeader,
) !DownloadOutcome {
    if (std.mem.startsWith(u8, uri, "data:")) {
        return .{ .ok = try parseDataUriAlloc(alloc, uri, security) };
    }

    const parsed = try std.Uri.parse(uri);
    if (std.mem.eql(u8, parsed.scheme, "http") or std.mem.eql(u8, parsed.scheme, "https")) {
        return try downloadHttpOutcomeAlloc(alloc, parsed, security, http_headers);
    }
    if (std.mem.eql(u8, parsed.scheme, "file")) {
        if (parsed.host) |host| {
            if (host.percent_encoded.len > 0 and !std.ascii.eqlIgnoreCase(host.percent_encoded, "localhost"))
                return error.InvalidHost;
        }
        const path_buf = try alloc.dupe(u8, parsed.path.percent_encoded);
        defer alloc.free(path_buf);
        const path = std.Uri.percentDecodeInPlace(path_buf);
        return .{ .ok = try downloadFileAlloc(alloc, path, security) };
    }
    if (std.mem.eql(u8, parsed.scheme, "s3")) {
        return .{ .ok = try downloadS3Alloc(alloc, parsed, uri, security, s3_credentials) };
    }
    return error.UnsupportedUrlScheme;
}

pub fn isEmptyContentSecurity(value: ContentSecurityConfig) bool {
    return value.allowed_hosts == null and
        value.block_private_ips == null and
        value.max_download_size_bytes == null and
        value.download_timeout_seconds == null and
        value.max_image_dimension == null and
        value.allowed_paths == null and
        value.user_agent == null;
}

pub fn effectiveContentSecurity(
    primary: ?*const ContentSecurityConfig,
    fallback: ?*const ContentSecurityConfig,
) ?*const ContentSecurityConfig {
    if (primary) |security| {
        if (!isEmptyContentSecurity(security.*)) return security;
    }
    if (fallback) |security| {
        if (!isEmptyContentSecurity(security.*)) return security;
    }
    return null;
}

fn freeOwnedStringSlice(alloc: std.mem.Allocator, values: []const []u8) void {
    for (values) |value| alloc.free(value);
    alloc.free(values);
}

pub fn dataUriDecodedSize(uri: []const u8) !usize {
    const prefix = "data:";
    if (!std.mem.startsWith(u8, uri, prefix)) return error.InvalidDataUri;

    const payload = uri[prefix.len..];
    const comma = std.mem.indexOfScalar(u8, payload, ',') orelse return error.InvalidDataUri;
    const meta = payload[0..comma];
    const body = payload[comma + 1 ..];

    if (std.mem.endsWith(u8, meta, ";base64")) {
        return std.base64.standard.Decoder.calcSizeForSlice(body) catch return error.InvalidBase64;
    }

    return try percentDecodedLen(body);
}

fn percentDecodedLen(value: []const u8) !usize {
    var len: usize = 0;
    var i: usize = 0;
    while (i < value.len) {
        if (value[i] == '%') {
            if (i + 2 >= value.len) return error.InvalidDataUri;
            _ = std.fmt.charToDigit(value[i + 1], 16) catch return error.InvalidDataUri;
            _ = std.fmt.charToDigit(value[i + 2], 16) catch return error.InvalidDataUri;
            i += 3;
        } else {
            i += 1;
        }
        len += 1;
    }
    return len;
}

fn validateDownloadSize(decoded_len: usize, security: ?*const ContentSecurityConfig) !void {
    const max_size = if (security) |cfg| cfg.max_download_size_bytes else null;
    if (max_size) |max| {
        if (@as(u64, @intCast(decoded_len)) > max) return error.StreamTooLong;
    }
}

fn parseDataUriAlloc(alloc: Allocator, uri: []const u8, security: ?*const ContentSecurityConfig) !DownloadedContent {
    const prefix = "data:";
    if (!std.mem.startsWith(u8, uri, prefix)) return error.InvalidDataUri;

    const payload = uri[prefix.len..];
    const comma = std.mem.indexOfScalar(u8, payload, ',') orelse return error.InvalidDataUri;
    const meta = payload[0..comma];
    const body = payload[comma + 1 ..];

    if (std.mem.endsWith(u8, meta, ";base64")) {
        const mime = meta[0 .. meta.len - ";base64".len];
        const decoded_len = std.base64.standard.Decoder.calcSizeForSlice(body) catch return error.InvalidBase64;
        try validateDownloadSize(decoded_len, security);
        const decoded = try alloc.alloc(u8, decoded_len);
        errdefer alloc.free(decoded);
        std.base64.standard.Decoder.decode(decoded, body) catch return error.InvalidBase64;
        return .{
            .content_type = try alloc.dupe(u8, if (mime.len > 0) mime else "application/octet-stream"),
            .data = decoded,
        };
    }

    const decoded_len = try percentDecodedLen(body);
    try validateDownloadSize(decoded_len, security);
    const decoded_body_buf = try alloc.dupe(u8, body);
    var data = decoded_body_buf;
    errdefer alloc.free(data);
    const decoded_body = std.Uri.percentDecodeInPlace(decoded_body_buf);
    if (decoded_body.len != decoded_body_buf.len) {
        const exact = try alloc.dupe(u8, decoded_body);
        alloc.free(decoded_body_buf);
        data = exact;
    }

    return .{
        .content_type = try alloc.dupe(u8, if (meta.len > 0) meta else "text/plain"),
        .data = data,
    };
}

fn downloadHttpOutcomeAlloc(
    alloc: Allocator,
    uri: std.Uri,
    security: ?*const ContentSecurityConfig,
    http_headers: ?[]const HTTPHeader,
) !DownloadOutcome {
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    try validateUrlSecurity(uri, security, io_impl.io());

    var client = std.http.Client{
        .allocator = alloc,
        .io = io_impl.io(),
    };
    defer client.deinit();

    var headers = std.ArrayListUnmanaged(std.http.Header).empty;
    defer headers.deinit(alloc);
    try headers.append(alloc, .{
        .name = "User-Agent",
        .value = if (security) |cfg| cfg.user_agent orelse "AntflyDB/1.0" else "AntflyDB/1.0",
    });
    if (http_headers) |extra_headers| {
        for (extra_headers) |header| {
            if (header.name.len == 0) continue;
            try headers.append(alloc, .{ .name = header.name, .value = header.value });
        }
    }

    var request = try std.http.Client.request(&client, .GET, uri, .{
        .keep_alive = false,
        // Redirects must be revalidated before connecting. The standard
        // client's automatic redirect loop does not expose that boundary, so
        // treat redirects as ordinary non-success responses here.
        .redirect_behavior = .unhandled,
        .extra_headers = headers.items,
    });
    defer request.deinit();

    if (security) |cfg| {
        if (cfg.block_private_ips orelse false) {
            try validateConnectedPeer(request.connection.?);
        }
    }

    try request.sendBodiless();
    var response = try request.receiveHead(&.{});
    if (response.head.status.class() != .success) {
        return .{
            .http_error = .{
                .status = @intFromEnum(response.head.status),
                .message = "remote fetch failed",
            },
        };
    }

    const mime = if (response.head.content_type) |value|
        trimMimeParameters(value)
    else
        "application/octet-stream";
    const owned_mime = try alloc.dupe(u8, mime);
    errdefer alloc.free(owned_mime);

    const max_size: usize = if (security) |cfg|
        @intCast(cfg.max_download_size_bytes orelse (100 * 1024 * 1024))
    else
        100 * 1024 * 1024;

    var transfer_buffer: [512]u8 = undefined;
    const body = try response.reader(&transfer_buffer).allocRemaining(alloc, .limited(max_size));
    errdefer alloc.free(body);

    return .{ .ok = .{
        .content_type = owned_mime,
        .data = body,
    } };
}

fn downloadFileAlloc(
    alloc: Allocator,
    path: []const u8,
    security: ?*const ContentSecurityConfig,
) !DownloadedContent {
    const file_security = security orelse return error.PathNotAllowed;
    const allowed_paths = file_security.allowed_paths orelse return error.PathNotAllowed;
    if (allowed_paths.len == 0) return error.PathNotAllowed;

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();

    const canonical_path = std.Io.Dir.cwd().realPathFileAlloc(io_impl.io(), path, alloc) catch return error.PathNotAllowed;
    defer alloc.free(canonical_path);
    try validateCanonicalFilePathSecurity(alloc, io_impl.io(), canonical_path, security);

    const limit: usize = @intCast(file_security.max_download_size_bytes orelse (100 * 1024 * 1024));
    const preflight_stat = try std.Io.Dir.cwd().statFile(io_impl.io(), canonical_path, .{});
    if (preflight_stat.kind != .file) return error.PathNotAllowed;
    var file = try std.Io.Dir.openFileAbsolute(io_impl.io(), canonical_path, .{ .allow_directory = false });
    defer file.close(io_impl.io());
    try validateOpenedFileSecurity(alloc, io_impl.io(), file, security);
    var reader = file.reader(io_impl.io(), &.{});
    const data = try reader.interface.allocRemaining(alloc, .limited(limit));
    errdefer alloc.free(data);
    return .{
        .content_type = try alloc.dupe(u8, guessMimeType(canonical_path)),
        .data = data,
    };
}

fn downloadS3Alloc(
    alloc: Allocator,
    parsed: std.Uri,
    original_uri: []const u8,
    security: ?*const ContentSecurityConfig,
    s3_credentials: ?*const S3CredentialsConfig,
) !DownloadedContent {
    const creds_cfg = s3_credentials orelse return error.MissingS3Credentials;

    const bucket, const key, const endpoint = try parseS3LocationAlloc(alloc, parsed, creds_cfg);
    defer alloc.free(bucket);
    defer alloc.free(key);
    defer alloc.free(endpoint);

    const joined_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ bucket, key });
    defer alloc.free(joined_path);
    try validatePathSecurity(joined_path, security);

    const creds = objectstore.S3Credentials{
        .endpoint = try alloc.dupe(u8, endpoint),
        .use_ssl = creds_cfg.use_ssl orelse true,
        .access_key_id = try alloc.dupe(u8, creds_cfg.access_key_id orelse return error.MissingAccessKeyId),
        .secret_access_key = try alloc.dupe(u8, creds_cfg.secret_access_key orelse return error.MissingSecretAccessKey),
        .session_token = if (creds_cfg.session_token) |value| try alloc.dupe(u8, value) else null,
        .region = try alloc.dupe(u8, "us-east-1"),
    };

    var client = try objectstore.S3.Client.init(alloc, .{
        .credentials = creds,
        .addressing_style = .path,
    });
    defer client.deinit();

    var store_client = client.client();
    var result = try store_client.getObject(bucket, key, .{});
    defer result.deinit(alloc);

    if (security) |cfg| {
        if (cfg.max_download_size_bytes) |max_size| {
            if (result.body.len > max_size) return error.StreamTooLong;
        }
    }

    _ = original_uri;
    return .{
        .content_type = try alloc.dupe(u8, result.metadata.content_type orelse guessMimeType(key)),
        .data = try alloc.dupe(u8, result.body),
    };
}

fn parseS3LocationAlloc(
    alloc: Allocator,
    parsed: std.Uri,
    creds_cfg: *const S3CredentialsConfig,
) !struct { []u8, []u8, []u8 } {
    const host = parsed.host orelse return error.InvalidS3Url;
    const host_text = host.percent_encoded;
    const path = trimLeftSlash(parsed.path.percent_encoded);
    if (path.len == 0) return error.InvalidS3Url;

    const host_is_endpoint = std.mem.indexOfScalar(u8, host_text, '.') != null or std.mem.indexOfScalar(u8, host_text, ':') != null;
    if (host_is_endpoint) {
        const slash = std.mem.indexOfScalar(u8, path, '/') orelse return error.InvalidS3Url;
        return .{
            try alloc.dupe(u8, path[0..slash]),
            try alloc.dupe(u8, path[slash + 1 ..]),
            try alloc.dupe(u8, host_text),
        };
    }

    const configured_endpoint = creds_cfg.endpoint orelse return error.MissingEndpoint;
    return .{
        try alloc.dupe(u8, host_text),
        try alloc.dupe(u8, path),
        try alloc.dupe(u8, configured_endpoint),
    };
}

fn validateUrlSecurity(parsed: std.Uri, security: ?*const ContentSecurityConfig, io: std.Io) !void {
    const cfg = security orelse return;
    const host = (parsed.host orelse return error.InvalidHost).percent_encoded;
    if (host.len == 0) return error.InvalidHost;

    if (cfg.allowed_hosts) |allowed_hosts| {
        var allowed = false;
        for (allowed_hosts) |entry| {
            if (std.ascii.eqlIgnoreCase(entry, host)) {
                allowed = true;
                break;
            }
        }
        if (!allowed) return error.HostNotAllowed;
    }

    if (cfg.block_private_ips orelse false) {
        try validateResolvedHost(host, parsed.port orelse if (std.mem.eql(u8, parsed.scheme, "https")) 443 else 80, io);
    }
}

/// S3 allowlists are logical bucket/key prefixes rather than filesystem
/// roots. Keep their existing semantics separate from local-file admission.
fn validatePathSecurity(path: []const u8, security: ?*const ContentSecurityConfig) !void {
    const cfg = security orelse return;
    const allowed_paths = cfg.allowed_paths orelse return;
    for (allowed_paths) |allowed| {
        if (std.mem.startsWith(u8, path, allowed)) return;
    }
    return error.PathNotAllowed;
}

fn validateOpenedFileSecurity(
    alloc: Allocator,
    io: std.Io,
    file: std.Io.File,
    security: ?*const ContentSecurityConfig,
) !void {
    if ((try file.stat(io)).kind != .file) return error.PathNotAllowed;

    var opened_path_buffer: [std.fs.max_path_bytes]u8 = undefined;
    const opened_path_len = file.realPath(io, &opened_path_buffer) catch return error.PathNotAllowed;
    try validateCanonicalFilePathSecurity(alloc, io, opened_path_buffer[0..opened_path_len], security);
}

fn validateCanonicalFilePathSecurity(
    alloc: Allocator,
    io: std.Io,
    canonical_path: []const u8,
    security: ?*const ContentSecurityConfig,
) !void {
    const cfg = security orelse return error.PathNotAllowed;
    const allowed_paths = cfg.allowed_paths orelse return error.PathNotAllowed;
    if (allowed_paths.len == 0) return error.PathNotAllowed;
    for (allowed_paths) |allowed| {
        if (allowed.len == 0) continue;
        const canonical_root = std.Io.Dir.cwd().realPathFileAlloc(io, allowed, alloc) catch continue;
        const contained = pathIsWithinRoot(canonical_root, canonical_path);
        alloc.free(canonical_root);
        if (contained) return;
    }
    return error.PathNotAllowed;
}

fn pathIsWithinRoot(root: []const u8, path: []const u8) bool {
    if (std.mem.eql(u8, root, path)) return true;
    if (root.len == 1 and root[0] == std.fs.path.sep)
        return path.len > 0 and path[0] == std.fs.path.sep;
    return path.len > root.len and
        std.mem.startsWith(u8, path, root) and
        path[root.len] == std.fs.path.sep;
}

fn validateResolvedHost(host: []const u8, port: u16, io: std.Io) !void {
    if (std.ascii.eqlIgnoreCase(host, "localhost") or std.ascii.endsWithIgnoreCase(host, ".localhost") or
        std.ascii.endsWithIgnoreCase(host, ".local")) return error.PrivateIpBlocked;

    if (std.Io.net.IpAddress.parse(host, port)) |address| {
        if (!isPublicAddress(address)) return error.PrivateIpBlocked;
        return;
    } else |_| {}

    const host_name = std.Io.net.HostName.init(host) catch return error.InvalidHost;
    var lookup_buffer: [32]std.Io.net.HostName.LookupResult = undefined;
    var lookup_queue: std.Io.Queue(std.Io.net.HostName.LookupResult) = .init(&lookup_buffer);
    try std.Io.net.HostName.lookup(host_name, io, &lookup_queue, .{ .port = port });
    var found_address = false;
    while (true) {
        const result = lookup_queue.getOne(io) catch |err| switch (err) {
            error.Closed => break,
            else => return err,
        };
        switch (result) {
            .address => |address| {
                found_address = true;
                // Mixed public/private DNS answers are rejected. Otherwise an
                // attacker can steer the client's address selection.
                if (!isPublicAddress(address)) return error.PrivateIpBlocked;
            },
            .canonical_name => {},
        }
    }
    if (!found_address) return error.InvalidHost;
}

fn validateConnectedPeer(connection: *std.http.Client.Connection) !void {
    // Ip literals have no DNS race, but use the same peer check on supported
    // server platforms to protect against resolver or transport surprises.
    if (builtin.os.tag == .windows) {
        // std.Io does not currently expose a portable peer-address query on
        // Windows. Fail closed rather than leaving DNS rebinding exploitable.
        return error.PeerAddressVerificationUnavailable;
    } else {
        var storage: std.Io.Threaded.PosixAddress = undefined;
        var storage_len: std.posix.socklen_t = @sizeOf(std.Io.Threaded.PosixAddress);
        try std.posix.getpeername(connection.stream_reader.stream.socket.handle, &storage.any, &storage_len);
        if (!isPublicAddress(std.Io.Threaded.addressFromPosix(&storage))) {
            connection.closing = true;
            return error.PrivateIpBlocked;
        }
    }
}

fn isPublicAddress(address: std.Io.net.IpAddress) bool {
    return switch (address) {
        .ip4 => |ip4| {
            const b = ip4.bytes;
            return !(b[0] == 0 or
                b[0] == 10 or
                (b[0] == 100 and (b[1] & 0xc0) == 0x40) or
                b[0] == 127 or
                (b[0] == 169 and b[1] == 254) or
                (b[0] == 172 and b[1] >= 16 and b[1] <= 31) or
                (b[0] == 192 and b[1] == 0 and (b[2] == 0 or b[2] == 2)) or
                (b[0] == 192 and b[1] == 88 and b[2] == 99) or
                (b[0] == 192 and b[1] == 168) or
                (b[0] == 198 and (b[1] == 18 or b[1] == 19)) or
                (b[0] == 198 and b[1] == 51 and b[2] == 100) or
                (b[0] == 203 and b[1] == 0 and b[2] == 113) or
                b[0] >= 224);
        },
        .ip6 => |ip6| {
            const b = ip6.bytes;
            if (std.Io.net.Ip4Address.fromIp6(ip6)) |ip4| return isPublicAddress(.{ .ip4 = ip4 });
            const zero_96_prefix = std.mem.eql(u8, b[0..12], &([_]u8{0} ** 12));
            const is_nat64 = std.mem.eql(u8, b[0..12], &.{ 0, 0x64, 0xff, 0x9b, 0, 0, 0, 0, 0, 0, 0, 0 });
            if (is_nat64) {
                return isPublicAddress(.{ .ip4 = .{ .bytes = b[12..16].*, .port = ip6.port } });
            }
            return ip6.interface.isNone() and
                !zero_96_prefix and
                (b[0] & 0xfe) != 0xfc and
                !(b[0] == 0xfe and (b[1] & 0xc0) >= 0x80) and
                b[0] != 0xff and
                !std.mem.eql(u8, b[0..4], &.{ 0x20, 0x01, 0x0d, 0xb8 });
        },
    };
}

fn guessMimeType(path: []const u8) []const u8 {
    const ext = std.fs.path.extension(path);
    if (std.ascii.eqlIgnoreCase(ext, ".html") or std.ascii.eqlIgnoreCase(ext, ".htm")) return "text/html";
    if (std.ascii.eqlIgnoreCase(ext, ".pdf")) return "application/pdf";
    if (std.ascii.eqlIgnoreCase(ext, ".png")) return "image/png";
    if (std.ascii.eqlIgnoreCase(ext, ".jpg") or std.ascii.eqlIgnoreCase(ext, ".jpeg")) return "image/jpeg";
    if (std.ascii.eqlIgnoreCase(ext, ".gif")) return "image/gif";
    if (std.ascii.eqlIgnoreCase(ext, ".webp")) return "image/webp";
    if (std.ascii.eqlIgnoreCase(ext, ".svg")) return "image/svg+xml";
    if (std.ascii.eqlIgnoreCase(ext, ".txt")) return "text/plain";
    if (std.ascii.eqlIgnoreCase(ext, ".md") or std.ascii.eqlIgnoreCase(ext, ".markdown")) return "text/markdown";
    return "application/octet-stream";
}

fn trimLeftSlash(path: []const u8) []const u8 {
    var idx: usize = 0;
    while (idx < path.len and path[idx] == '/') : (idx += 1) {}
    return path[idx..];
}

fn trimMimeParameters(value: []const u8) []const u8 {
    const semi = std.mem.indexOfScalar(u8, value, ';') orelse return value;
    return std.mem.trim(u8, value[0..semi], &std.ascii.whitespace);
}

test "effective content security prefers primary when non-empty" {
    var primary = ContentSecurityConfig{
        .block_private_ips = false,
    };
    var fallback = ContentSecurityConfig{
        .block_private_ips = true,
    };
    const effective = effectiveContentSecurity(&primary, &fallback).?;
    try std.testing.expectEqual(@as(?bool, false), effective.block_private_ips);
}

test "effective content security falls back when primary is empty" {
    var primary = ContentSecurityConfig{};
    var fallback = ContentSecurityConfig{
        .block_private_ips = true,
    };
    const effective = effectiveContentSecurity(&primary, &fallback).?;
    try std.testing.expectEqual(@as(?bool, true), effective.block_private_ips);
}

test "download content parses data uri" {
    const alloc = std.testing.allocator;
    var downloaded = try downloadContentAlloc(alloc, "data:text/plain;base64,aGVsbG8=", null, null);
    defer downloaded.deinit(alloc);
    try std.testing.expectEqualStrings("text/plain", downloaded.content_type);
    try std.testing.expectEqualStrings("hello", downloaded.data);
}

test "download content percent decodes non-base64 data uri" {
    const alloc = std.testing.allocator;
    var downloaded = try downloadContentAlloc(alloc, "data:text/plain,alpha%20beta%2Bgamma", null, null);
    defer downloaded.deinit(alloc);
    try std.testing.expectEqualStrings("text/plain", downloaded.content_type);
    try std.testing.expectEqualStrings("alpha beta+gamma", downloaded.data);
}

test "download content enforces data uri decoded byte limit" {
    const alloc = std.testing.allocator;
    var security = ContentSecurityConfig{ .max_download_size_bytes = 4 };
    try std.testing.expectError(error.StreamTooLong, downloadContentAlloc(alloc, "data:text/plain;base64,aGVsbG8=", &security, null));
    try std.testing.expectError(error.StreamTooLong, downloadContentAlloc(alloc, "data:text/plain,alpha%20beta", &security, null));
}

test "download content reads percent encoded file uri" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "image file.png",
        .data = "png-bytes",
    });

    const rel_path = try std.fs.path.join(alloc, &.{ ".zig-cache", "tmp", tmp.sub_path[0..], "image file.png" });
    defer alloc.free(rel_path);
    const abs_path = try std.Io.Dir.cwd().realPathFileAlloc(std.testing.io, rel_path, alloc);
    defer alloc.free(abs_path);

    const raw_uri = try std.fmt.allocPrint(alloc, "file://{s}", .{abs_path});
    defer alloc.free(raw_uri);
    const uri = try std.mem.replaceOwned(u8, alloc, raw_uri, " ", "%20");
    defer alloc.free(uri);

    try std.testing.expectError(error.PathNotAllowed, downloadContentAlloc(alloc, uri, null, null));

    const allowed_root = std.fs.path.dirname(abs_path).?;
    const allowed_paths = [_][]u8{@constCast(allowed_root)};
    var downloaded = try downloadContentAlloc(alloc, uri, &.{ .allowed_paths = &allowed_paths }, null);
    defer downloaded.deinit(alloc);
    try std.testing.expectEqualStrings("image/png", downloaded.content_type);
    try std.testing.expectEqualStrings("png-bytes", downloaded.data);
}

test "download content file allowlist uses canonical path boundaries" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.createDirPath(std.testing.io, "allowed");
    try tmp.dir.createDirPath(std.testing.io, "allowed-escape");
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "allowed/safe.txt", .data = "safe" });
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "allowed-escape/secret.txt", .data = "secret" });

    const tmp_rel = try std.fs.path.join(alloc, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer alloc.free(tmp_rel);
    const tmp_abs = try std.Io.Dir.cwd().realPathFileAlloc(std.testing.io, tmp_rel, alloc);
    defer alloc.free(tmp_abs);
    const allowed_root = try std.fs.path.join(alloc, &.{ tmp_abs, "allowed" });
    defer alloc.free(allowed_root);
    const escaped_path = try std.fs.path.join(alloc, &.{ tmp_abs, "allowed-escape", "secret.txt" });
    defer alloc.free(escaped_path);
    const escaped_uri = try std.fmt.allocPrint(alloc, "file://{s}", .{escaped_path});
    defer alloc.free(escaped_uri);
    const allowed_paths = [_][]u8{allowed_root};
    try std.testing.expectError(error.PathNotAllowed, downloadContentAlloc(alloc, escaped_uri, &.{ .allowed_paths = &allowed_paths }, null));
    const directory_uri = try std.fmt.allocPrint(alloc, "file://{s}", .{allowed_root});
    defer alloc.free(directory_uri);
    try std.testing.expectError(error.PathNotAllowed, downloadContentAlloc(alloc, directory_uri, &.{ .allowed_paths = &allowed_paths }, null));

    if (builtin.os.tag != .windows) {
        try tmp.dir.symLink(std.testing.io, "../allowed-escape/secret.txt", "allowed/link.txt", .{});
        const link_path = try std.fs.path.join(alloc, &.{ tmp_abs, "allowed", "link.txt" });
        defer alloc.free(link_path);
        const link_uri = try std.fmt.allocPrint(alloc, "file://{s}", .{link_path});
        defer alloc.free(link_uri);
        try std.testing.expectError(error.PathNotAllowed, downloadContentAlloc(alloc, link_uri, &.{ .allowed_paths = &allowed_paths }, null));
    }
}

test "download content blocks disallowed hosts" {
    const alloc = std.testing.allocator;
    const allowed_hosts = [_][]u8{@constCast("cdn.example.com")};
    try std.testing.expectError(error.HostNotAllowed, downloadContentAlloc(alloc, "https://example.com/a.png", &.{
        .allowed_hosts = &allowed_hosts,
    }, null));
}

test "download content blocks private ip literals" {
    const alloc = std.testing.allocator;
    try std.testing.expectError(error.PrivateIpBlocked, downloadContentAlloc(alloc, "http://127.0.0.1/test.png", &.{
        .block_private_ips = true,
    }, null));
}

test "remote address policy rejects non-public ranges" {
    const parse = std.Io.net.IpAddress.parse;
    try std.testing.expect(!isPublicAddress(try parse("0.0.0.0", 80)));
    try std.testing.expect(!isPublicAddress(try parse("100.64.0.1", 80)));
    try std.testing.expect(!isPublicAddress(try parse("169.254.169.254", 80)));
    try std.testing.expect(!isPublicAddress(try parse("192.168.1.1", 80)));
    try std.testing.expect(!isPublicAddress(try parse("::1", 80)));
    try std.testing.expect(!isPublicAddress(try parse("fc00::1", 80)));
    try std.testing.expect(!isPublicAddress(try parse("fe80::1", 80)));
    try std.testing.expect(!isPublicAddress(try parse("::ffff:127.0.0.1", 80)));
    try std.testing.expect(isPublicAddress(try parse("8.8.8.8", 80)));
    try std.testing.expect(isPublicAddress(try parse("2606:4700:4700::1111", 80)));
}
