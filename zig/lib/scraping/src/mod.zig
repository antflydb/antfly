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
    if (std.mem.startsWith(u8, uri, "data:")) {
        return .{ .ok = try parseDataUriAlloc(alloc, uri) };
    }

    const parsed = try std.Uri.parse(uri);
    if (std.mem.eql(u8, parsed.scheme, "http") or std.mem.eql(u8, parsed.scheme, "https")) {
        try validateUrlSecurity(parsed, security);
        return try downloadHttpOutcomeAlloc(alloc, parsed, security);
    }
    if (std.mem.eql(u8, parsed.scheme, "file")) {
        const path_buf = try alloc.dupe(u8, parsed.path.percent_encoded);
        defer alloc.free(path_buf);
        const path = std.Uri.percentDecodeInPlace(path_buf);
        try validatePathSecurity(path, security);
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

fn parseDataUriAlloc(alloc: Allocator, uri: []const u8) !DownloadedContent {
    const prefix = "data:";
    if (!std.mem.startsWith(u8, uri, prefix)) return error.InvalidDataUri;

    const payload = uri[prefix.len..];
    const comma = std.mem.indexOfScalar(u8, payload, ',') orelse return error.InvalidDataUri;
    const meta = payload[0..comma];
    const body = payload[comma + 1 ..];

    if (std.mem.endsWith(u8, meta, ";base64")) {
        const mime = meta[0 .. meta.len - ";base64".len];
        const decoded_len = std.base64.standard.Decoder.calcSizeForSlice(body) catch return error.InvalidBase64;
        const decoded = try alloc.alloc(u8, decoded_len);
        errdefer alloc.free(decoded);
        std.base64.standard.Decoder.decode(decoded, body) catch return error.InvalidBase64;
        return .{
            .content_type = try alloc.dupe(u8, if (mime.len > 0) mime else "application/octet-stream"),
            .data = decoded,
        };
    }

    return .{
        .content_type = try alloc.dupe(u8, if (meta.len > 0) meta else "text/plain"),
        .data = try alloc.dupe(u8, body),
    };
}

fn downloadHttpOutcomeAlloc(
    alloc: Allocator,
    uri: std.Uri,
    security: ?*const ContentSecurityConfig,
) !DownloadOutcome {
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();

    var client = std.http.Client{
        .allocator = alloc,
        .io = io_impl.io(),
    };
    defer client.deinit();

    var headers: [1]std.http.Header = .{.{
        .name = "User-Agent",
        .value = if (security) |cfg| cfg.user_agent orelse "AntflyDB/1.0" else "AntflyDB/1.0",
    }};

    var request = try std.http.Client.request(&client, .GET, uri, .{
        .keep_alive = false,
        .extra_headers = &headers,
    });
    defer request.deinit();

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

    const max_size: usize = if (security) |cfg|
        @intCast(cfg.max_download_size_bytes orelse (100 * 1024 * 1024))
    else
        100 * 1024 * 1024;

    var transfer_buffer: [512]u8 = undefined;
    const body = try response.reader(&transfer_buffer).allocRemaining(alloc, .limited(max_size));
    errdefer alloc.free(body);

    return .{ .ok = .{
        .content_type = try alloc.dupe(u8, mime),
        .data = body,
    } };
}

fn downloadFileAlloc(
    alloc: Allocator,
    path: []const u8,
    security: ?*const ContentSecurityConfig,
) !DownloadedContent {
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();

    const limit: usize = if (security) |cfg|
        @intCast(cfg.max_download_size_bytes orelse (100 * 1024 * 1024))
    else
        100 * 1024 * 1024;
    var file = try std.Io.Dir.openFileAbsolute(io_impl.io(), path, .{});
    defer file.close(io_impl.io());
    var reader = file.reader(io_impl.io(), &.{});
    const data = try reader.interface.allocRemaining(alloc, .limited(limit));
    errdefer alloc.free(data);
    return .{
        .content_type = try alloc.dupe(u8, guessMimeType(path)),
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

fn validateUrlSecurity(parsed: std.Uri, security: ?*const ContentSecurityConfig) !void {
    const cfg = security orelse return;
    const host = (parsed.host orelse return error.InvalidHost).percent_encoded;

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
        if (isPrivateHost(host)) return error.PrivateIpBlocked;
    }
}

fn validatePathSecurity(path: []const u8, security: ?*const ContentSecurityConfig) !void {
    const cfg = security orelse return;
    const allowed_paths = cfg.allowed_paths orelse return;
    for (allowed_paths) |allowed| {
        if (std.mem.startsWith(u8, path, allowed)) return;
    }
    return error.PathNotAllowed;
}

fn isPrivateHost(host: []const u8) bool {
    if (std.ascii.eqlIgnoreCase(host, "localhost")) return true;
    if (std.mem.endsWith(u8, host, ".local")) return true;

    const address = std.Io.net.IpAddress.parse(host, 0) catch return false;
    return switch (address) {
        .ip4 => |ip4| {
            const b = ip4.bytes;
            return b[0] == 10 or
                (b[0] == 172 and b[1] >= 16 and b[1] <= 31) or
                (b[0] == 192 and b[1] == 168) or
                (b[0] == 169 and b[1] == 254) or
                b[0] == 127;
        },
        .ip6 => |ip6| {
            const b = ip6.bytes;
            return !ip6.interface.isNone() or
                b[0] == 0xfe and (b[1] & 0xc0) == 0x80 or
                (b[0] & 0xfe) == 0xfc or
                (b[0] == 0 and b[1] == 0 and b[2] == 0 and b[3] == 0 and b[4] == 0 and b[5] == 0 and b[6] == 0 and b[7] == 0 and b[8] == 0 and b[9] == 0 and b[10] == 0 and b[11] == 0 and b[12] == 0 and b[13] == 0 and b[14] == 0 and b[15] == 1);
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

    var downloaded = try downloadContentAlloc(alloc, uri, null, null);
    defer downloaded.deinit(alloc);
    try std.testing.expectEqualStrings("image/png", downloaded.content_type);
    try std.testing.expectEqualStrings("png-bytes", downloaded.data);
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
