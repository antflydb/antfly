// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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
const httpx = @import("httpx");
const objectstore = @import("objectstore");

pub const data_uri = @import("antfly_data_uri");

// Public connection-input contracts. Configuration consumers already depend on
// scraping; they do not need a direct dependency on the concrete object store.
pub const GcsCredentialSource = objectstore.Gcs.CredentialSource;
pub const GcsCredentialConfig = objectstore.Gcs.Credentials;

const Allocator = std.mem.Allocator;
const default_max_download_size_bytes: u64 = 100 * 1024 * 1024;
const default_download_timeout_ms: u64 = 30_000;

/// Borrowed, transport-neutral cancellation for one remote-content request.
/// Reuse objectstore's callback contract so HTTP, S3, and file sources share
/// one operation context without coupling this library to an application.
pub const CancellationToken = objectstore.CancellationToken;

/// Supported media transports. Shared with offline dependency inspection so
/// destination evidence cannot enable a protocol this runtime cannot execute.
pub const ContentScheme = enum { http, https, file, s3, gs, data };

pub fn contentScheme(scheme: []const u8) error{UnsupportedUrlScheme}!ContentScheme {
    inline for (@typeInfo(ContentScheme).@"enum".field_names) |name| {
        if (std.ascii.eqlIgnoreCase(scheme, name)) return @field(ContentScheme, name);
    }
    return error.UnsupportedUrlScheme;
}

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
    /// Bytes buffered before the non-success status was classified. Callers
    /// with aggregate budgets must charge these just like successful bodies.
    downloaded_bytes: u64 = 0,
    /// Retry-After delta or HTTP-date, parsed while response headers are alive.
    retry_after_ms: ?u64 = null,
};

pub const DownloadOutcome = union(enum) {
    ok: DownloadedContent,
    http_error: HttpError,
};

pub const HTTPHeader = struct {
    name: []const u8,
    value: []const u8,
    /// Capability boundary for credential-bearing headers. Redirects retain
    /// credentials only while every hop stays under this canonical URL prefix.
    credential_base_url: ?[]const u8 = null,
};

/// Execution context for one remote-content operation. For HTTP/S3,
/// `timeout_ms` is an additional ceiling and the configured timeout (or 30s
/// default) still applies. File I/O uses caller cancellation when this is null;
/// a finite file deadline requires a caller-supplied blocking-I/O boundary.
/// Zero disables that individual ceiling rather than the configured one.
/// Hard quota for connection state, TLS buffers, trust roots and HTTP metadata.
/// Bodies have a separate allocation domain and never consume this quota.
pub const default_download_transport_memory_bytes: usize = 8 << 20;

pub fn downloadMemoryPeakBytes(max_body_bytes: usize, transport_memory_limit_bytes: usize) error{Overflow}!usize {
    return std.math.add(usize, transport_memory_limit_bytes, try httpx.Client.bufferedResponseBodyPeakBytes(max_body_bytes));
}

pub const DownloadContext = struct {
    io: std.Io,
    transport_memory_limit_bytes: usize = default_download_transport_memory_bytes,
    timeout_ms: ?u64 = null,
    cancellation: ?CancellationToken = null,
    /// A real request-owned interruption boundary supplied by the runtime.
    /// Filesystem syscalls cannot themselves promise a hard timeout.
    blocking_io: ?BlockingIo = null,

    pub const BlockingIo = struct {
        context: *const anyopaque,
        enter: *const fn (*const anyopaque) anyerror!u64,
        leave: *const fn (*const anyopaque, u64) void,
        check: ?*const fn (*const anyopaque) anyerror!void = null,
    };
    pub const BlockingLease = struct {
        boundary: ?BlockingIo = null,
        token: u64 = 0,
        pub fn deinit(self: BlockingLease) void {
            if (self.boundary) |b| b.leave(b.context, self.token);
        }
    };
    fn enterFileIo(self: @This()) !BlockingLease {
        try self.check();
        if (self.blocking_io) |b| return .{ .boundary = b, .token = try b.enter(b.context) };
        if ((self.timeout_ms orelse 0) != 0) return error.FileTimeoutUnsupported;
        return .{};
    }

    pub fn check(self: @This()) !void {
        try self.io.checkCancel();
        if (self.blocking_io) |b| if (b.check) |check_control| try check_control(b.context);
        if (self.cancellation) |token| try token.check();
    }
};

pub const ContentSecurityConfig = struct {
    allowed_hosts: ?[]const []u8 = null,
    block_private_ips: ?bool = null,
    /// RFC 6052 network-specific prefixes used by the deployment's NAT64
    /// translators. Embedded IPv4 destinations are classified before connect.
    nat64_prefixes: ?[]const []u8 = null,
    max_download_size_bytes: ?u64 = null,
    download_timeout_seconds: ?u32 = null,
    max_image_dimension: ?u32 = null,
    /// Local file downloads require an explicit, nonempty path allowlist.
    allowed_paths: ?[]const []u8 = null,
    /// Exact source companion base URLs a chat `source` part may fetch, including
    /// private addresses. Every other `source` URL keeps private-IP blocking.
    source_media_base_urls: ?[]const []u8 = null,
    user_agent: ?[]u8 = null,

    pub fn deinit(self: *ContentSecurityConfig, alloc: std.mem.Allocator) void {
        if (self.allowed_hosts) |values| freeOwnedStringSlice(alloc, values);
        if (self.nat64_prefixes) |values| freeOwnedStringSlice(alloc, values);
        if (self.allowed_paths) |values| freeOwnedStringSlice(alloc, values);
        if (self.source_media_base_urls) |values| freeOwnedStringSlice(alloc, values);
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

/// Borrowed view of an explicitly configured external-I/O connection. The
/// owning configuration snapshot outlives every fetch; no credentials are
/// inferred from a gs:// URI or from an unscoped process identity.
pub const GcsContentConnection = struct {
    buckets: []const []u8 = &.{},
    prefix: ?[]u8 = null,
    project_id: ?[]u8 = null,
    endpoint: ?[]u8 = null,
    credentials: objectstore.Gcs.Credentials = .{},

    pub fn allows(self: *const GcsContentConnection, location: *const GcsLocation) bool {
        var allowed = false;
        for (self.buckets) |bucket| if (std.mem.eql(u8, bucket, location.bucket)) {
            allowed = true;
            break;
        };
        if (!allowed) return false;
        const prefix = self.prefix orelse return true;
        return prefix.len == 0 or hasComponentPrefix(location.key, std.mem.trimEnd(u8, prefix, "/"), '/');
    }
};

pub const GcsLocation = struct {
    bucket: []u8,
    key: []u8,
    generation: ?[]u8,

    pub fn deinit(self: *GcsLocation, alloc: Allocator) void {
        alloc.free(self.bucket);
        alloc.free(self.key);
        if (self.generation) |value| alloc.free(value);
        self.* = undefined;
    }
};

pub fn parseGcsLocationAlloc(alloc: Allocator, uri: []const u8) !GcsLocation {
    const parsed = std.Uri.parse(uri) catch return error.InvalidGcsUrl;
    if (!std.ascii.eqlIgnoreCase(parsed.scheme, "gs") or parsed.user != null or
        parsed.password != null or parsed.port != null or parsed.fragment != null) return error.InvalidGcsUrl;
    const bucket = (parsed.host orelse return error.InvalidGcsUrl).percent_encoded;
    if (bucket.len < 3 or bucket.len > 222) return error.InvalidGcsUrl;
    for (bucket) |c| if (!std.ascii.isLower(c) and !std.ascii.isDigit(c) and c != '-' and c != '_' and c != '.') return error.InvalidGcsUrl;
    if (!std.ascii.isAlphanumeric(bucket[0]) or !std.ascii.isAlphanumeric(bucket[bucket.len - 1])) return error.InvalidGcsUrl;
    if (!std.mem.startsWith(u8, parsed.path.percent_encoded, "/")) return error.InvalidGcsUrl;
    const encoded_key = parsed.path.percent_encoded[1..];
    if (encoded_key.len == 0) return error.InvalidGcsUrl;
    var encoded_index: usize = 0;
    while (encoded_index < encoded_key.len) : (encoded_index += 1) {
        const c = encoded_key[encoded_index];
        if (c <= 0x20 or c == 0x7f) return error.InvalidGcsUrl;
        if (c == '%') {
            if (encoded_index + 2 >= encoded_key.len or !std.ascii.isHex(encoded_key[encoded_index + 1]) or !std.ascii.isHex(encoded_key[encoded_index + 2])) return error.InvalidGcsUrl;
            encoded_index += 2;
        }
    }
    const key = try alloc.dupe(u8, encoded_key);
    defer alloc.free(key);
    const decoded = std.Uri.percentDecodeInPlace(key);
    for (decoded) |c| if (c < 0x20 or c == 0x7f) return error.InvalidGcsUrl;
    const owned_key = try alloc.dupe(u8, decoded);
    errdefer alloc.free(owned_key);
    var generation: ?[]u8 = null;
    if (parsed.query) |query| {
        const value = query.percent_encoded;
        if (!std.mem.startsWith(u8, value, "generation=")) return error.InvalidGcsUrl;
        const digits = value["generation=".len..];
        if (digits.len == 0 or digits.len > 20 or digits[0] == '0') return error.InvalidGcsUrl;
        for (digits) |c| if (!std.ascii.isDigit(c)) return error.InvalidGcsUrl;
        _ = std.fmt.parseUnsigned(u64, digits, 10) catch return error.InvalidGcsUrl;
        generation = try alloc.dupe(u8, digits);
    }
    errdefer if (generation) |value| alloc.free(value);
    return .{ .bucket = try alloc.dupe(u8, bucket), .key = owned_key, .generation = generation };
}

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
    /// Derived only from canonical connections with content.fetch capability.
    gcs_connections: std.StringArrayHashMapUnmanaged(GcsContentConnection) = .{},
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
        // These callbacks cross server/kernel archives with independent Zig
        // error tables. Return a stable code and write the snapshot on success.
        pub const AcquireStatus = enum(u8) { ok, out_of_memory, timeout, canceled, unavailable };
        context: *anyopaque,
        acquire_fn: *const fn (*anyopaque, *Snapshot) callconv(.c) AcquireStatus,
        health_fn: ?*const fn (*anyopaque) RuntimeHealth = null,

        pub fn acquire(self: RemoteContentRuntime) !Snapshot {
            var snapshot: Snapshot = undefined;
            switch (self.acquire_fn(self.context, &snapshot)) {
                .ok => return snapshot,
                .out_of_memory => return error.OutOfMemory,
                .timeout => return error.Timeout,
                .canceled => return error.Canceled,
                .unavailable => return error.ResourceTemporarilyUnavailable,
            }
        }

        pub fn health(self: RemoteContentRuntime) ?RuntimeHealth {
            return if (self.health_fn) |health_fn| health_fn(self.context) else null;
        }
    };

    /// Acquisition failures propagate without fetching with a fallback policy.
    pub fn acquire(self: *const RemoteContentConfig) !Snapshot {
        if (self.runtime) |runtime| return runtime.acquire();
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
        // Keys and connection values are borrowed from the owning Config.
        self.gcs_connections.deinit(alloc);
        self.* = undefined;
    }
};

/// JSON payload for the compiled storage owner. It carries destination policy
/// and every scoped credential (S3, HTTP and GCS), with secret references left
/// unresolved. The derived facade borrows values from this payload; only its
/// maps are owned. Release them with `deinitConfig` before destroying the
/// parsed payload.
pub const RemoteContentSnapshotData = struct {
    security: ContentSecurityConfig = .{},
    default_s3: ?[]u8 = null,
    s3: []const NamedS3Credential = &.{},
    http: []const NamedHttpCredential = &.{},
    gcs_connections: []const NamedGcsConnection = &.{},

    pub const NamedS3Credential = struct {
        name: []const u8,
        credential: S3CredentialConfig,
    };

    /// JSON cannot carry `HTTPCredentialConfig.headers` as a map, so headers
    /// cross the boundary as a list.
    pub const NamedHttpCredential = struct {
        name: []const u8,
        base_url: ?[]u8 = null,
        headers: []const Header = &.{},
        security: ?ContentSecurityConfig = null,

        pub const Header = struct {
            name: []const u8,
            value: []u8,
        };
    };

    pub const NamedGcsConnection = struct {
        name: []const u8,
        connection: GcsContentConnection,
    };

    pub fn configAlloc(self: *const @This(), alloc: Allocator) !RemoteContentConfig {
        var config = RemoteContentConfig{ .security = self.security, .default_s3 = self.default_s3 };
        errdefer deinitConfig(&config, alloc);
        for (self.s3) |entry| {
            const result = try config.s3.getOrPut(alloc, entry.name);
            if (result.found_existing) return error.InvalidRemoteContentConfig;
            result.value_ptr.* = entry.credential;
        }
        for (self.http) |entry| {
            const result = try config.http.getOrPut(alloc, entry.name);
            if (result.found_existing) return error.InvalidRemoteContentConfig;
            result.value_ptr.* = .{ .base_url = entry.base_url, .security = entry.security };
            for (entry.headers) |header| {
                const slot = try result.value_ptr.headers.getOrPut(alloc, header.name);
                if (slot.found_existing) return error.InvalidRemoteContentConfig;
                slot.value_ptr.* = header.value;
            }
        }
        for (self.gcs_connections) |entry| {
            const result = try config.gcs_connections.getOrPut(alloc, entry.name);
            if (result.found_existing) return error.InvalidRemoteContentConfig;
            result.value_ptr.* = entry.connection;
        }
        return config;
    }

    /// Releases the maps built by `configAlloc`. Keys and values stay borrowed
    /// from the parsed payload and are not freed here.
    pub fn deinitConfig(config: *RemoteContentConfig, alloc: Allocator) void {
        config.s3.deinit(alloc);
        for (config.http.values()) |*credential| credential.headers.deinit(alloc);
        config.http.deinit(alloc);
        config.gcs_connections.deinit(alloc);
    }
};

pub fn downloadContentAlloc(
    alloc: Allocator,
    uri: []const u8,
    security: ?*const ContentSecurityConfig,
    s3_credentials: ?*const S3CredentialsConfig,
) !DownloadedContent {
    const outcome = try downloadContentOutcomeAllocImpl(alloc, null, uri, security, s3_credentials, null);
    return downloadOutcomeContent(outcome);
}

pub fn downloadContentAllocWithContext(
    alloc: Allocator,
    context: DownloadContext,
    uri: []const u8,
    security: ?*const ContentSecurityConfig,
    s3_credentials: ?*const S3CredentialsConfig,
) !DownloadedContent {
    const outcome = try downloadContentOutcomeAllocImpl(alloc, context, uri, security, s3_credentials, null);
    return downloadOutcomeContent(outcome);
}

pub fn downloadContentOutcomeAlloc(
    alloc: Allocator,
    uri: []const u8,
    security: ?*const ContentSecurityConfig,
    s3_credentials: ?*const S3CredentialsConfig,
) !DownloadOutcome {
    return downloadContentOutcomeAllocImpl(alloc, null, uri, security, s3_credentials, null);
}

pub fn downloadContentOutcomeAllocWithContext(
    alloc: Allocator,
    context: DownloadContext,
    uri: []const u8,
    security: ?*const ContentSecurityConfig,
    s3_credentials: ?*const S3CredentialsConfig,
) !DownloadOutcome {
    return downloadContentOutcomeAllocImpl(alloc, context, uri, security, s3_credentials, null);
}

pub fn downloadContentOutcomeAllocWithHeaders(
    alloc: Allocator,
    uri: []const u8,
    security: ?*const ContentSecurityConfig,
    s3_credentials: ?*const S3CredentialsConfig,
    http_headers: ?[]const HTTPHeader,
) !DownloadOutcome {
    return downloadContentOutcomeAllocImpl(alloc, null, uri, security, s3_credentials, http_headers);
}

pub fn downloadContentOutcomeAllocWithHeadersAndContext(
    alloc: Allocator,
    context: DownloadContext,
    uri: []const u8,
    security: ?*const ContentSecurityConfig,
    s3_credentials: ?*const S3CredentialsConfig,
    http_headers: ?[]const HTTPHeader,
) !DownloadOutcome {
    return downloadContentOutcomeAllocImpl(alloc, context, uri, security, s3_credentials, http_headers);
}

fn downloadOutcomeContent(outcome: DownloadOutcome) !DownloadedContent {
    return switch (outcome) {
        .ok => |downloaded| downloaded,
        .http_error => error.HttpFetchFailed,
    };
}

fn downloadContentOutcomeAllocImpl(
    alloc: Allocator,
    maybe_context: ?DownloadContext,
    uri: []const u8,
    security: ?*const ContentSecurityConfig,
    s3_credentials: ?*const S3CredentialsConfig,
    http_headers: ?[]const HTTPHeader,
) !DownloadOutcome {
    if (maybe_context) |context| try context.check();
    if (data_uri.hasScheme(uri)) {
        const maybe_ceiling: ?DownloadCeiling = if (maybe_context) |context|
            try DownloadCeiling.init(context, security)
        else
            null;
        var downloaded = try parseDataUriAlloc(alloc, uri, security);
        errdefer downloaded.deinit(alloc);
        if (maybe_ceiling) |ceiling| try ceiling.check();
        return .{ .ok = downloaded };
    }

    const parsed = try std.Uri.parse(uri);
    switch (try contentScheme(parsed.scheme)) {
        .http, .https => {
            try validateUrlSecurity(parsed, security);
            if (maybe_context) |context| return downloadHttpOutcomeAlloc(alloc, context, uri, security, http_headers);
            var io_impl = std.Io.Threaded.init(alloc, .{});
            defer io_impl.deinit();
            return downloadHttpOutcomeAlloc(alloc, .{ .io = io_impl.io() }, uri, security, http_headers);
        },
        .file => {
            if (parsed.host) |host| {
                if (host.percent_encoded.len > 0 and !std.ascii.eqlIgnoreCase(host.percent_encoded, "localhost"))
                    return error.InvalidHost;
            }
            const path_buf = try alloc.dupe(u8, parsed.path.percent_encoded);
            defer alloc.free(path_buf);
            const path = std.Uri.percentDecodeInPlace(path_buf);
            if (maybe_context) |context| return .{ .ok = try downloadFileAllocWithContext(alloc, context, path, security) };
            return .{ .ok = try downloadFileAlloc(alloc, path, security) };
        },
        .s3 => {
            return .{ .ok = try downloadS3Alloc(alloc, maybe_context, parsed, security, s3_credentials) };
        },
        .gs => return error.GcsConnectionNotFoundOrOutOfScope,
        .data => unreachable, // Handled before URI parsing.
    }
}

pub fn isEmptyContentSecurity(value: ContentSecurityConfig) bool {
    return value.allowed_hosts == null and
        value.block_private_ips == null and
        value.nat64_prefixes == null and
        value.max_download_size_bytes == null and
        value.download_timeout_seconds == null and
        value.max_image_dimension == null and
        value.allowed_paths == null and
        value.source_media_base_urls == null and
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
    return (try data_uri.parseRequired(uri)).decodedSize();
}

fn validateDownloadSize(decoded_len: usize, security: ?*const ContentSecurityConfig) !void {
    if (@as(u64, @intCast(decoded_len)) > maxDownloadSizeBytes(security)) return error.StreamTooLong;
}

fn maxDownloadSizeBytes(security: ?*const ContentSecurityConfig) u64 {
    return if (security) |cfg|
        cfg.max_download_size_bytes orelse default_max_download_size_bytes
    else
        default_max_download_size_bytes;
}

fn maxDownloadSize(security: ?*const ContentSecurityConfig) usize {
    return std.math.cast(usize, maxDownloadSizeBytes(security)) orelse std.math.maxInt(usize);
}

fn parseDataUriAlloc(alloc: Allocator, uri: []const u8, security: ?*const ContentSecurityConfig) !DownloadedContent {
    const parsed = try data_uri.parseRequired(uri);
    try validateDownloadSize(try parsed.decodedSize(), security);
    var decoded = try data_uri.decodeAlloc(alloc, uri);
    errdefer decoded.deinit(alloc);
    const result = DownloadedContent{ .content_type = decoded.media_type, .data = decoded.data };
    decoded = undefined;
    return result;
}

fn downloadHttpOutcomeAlloc(
    alloc: Allocator,
    context: DownloadContext,
    uri: []const u8,
    security: ?*const ContentSecurityConfig,
    http_headers: ?[]const HTTPHeader,
) !DownloadOutcome {
    try context.check();
    const ceiling = try DownloadCeiling.init(context, security);
    var transport = httpx.QuotaAllocator{ .backing = alloc, .limit = context.transport_memory_limit_bytes };
    var client_config = httpClientConfig(security);
    client_config.http1_response_body_allocator = alloc;
    var client = httpx.Client.initWithConfig(transport.allocator(), context.io, client_config);
    defer client.deinit();

    var headers = std.ArrayListUnmanaged([2][]const u8).empty;
    defer headers.deinit(alloc);
    var credential_base_url: ?[]const u8 = null;
    if (http_headers) |extra_headers| {
        for (extra_headers) |header| {
            if (header.name.len == 0) continue;
            try headers.append(alloc, .{ header.name, header.value });
            if (header.credential_base_url) |base_url| {
                if (credential_base_url) |existing| {
                    if (!std.mem.eql(u8, existing, base_url)) return error.InvalidCredentialScope;
                } else {
                    credential_base_url = base_url;
                }
            }
        }
    }

    var current_url = try alloc.dupe(u8, uri);
    defer alloc.free(current_url);
    var current_uri = try std.Uri.parse(current_url);
    try validateHttpUri(current_uri);
    try validateUrlSecurity(current_uri, security);
    var send_credentials = credential_base_url == null or httpUrlMatchesBase(current_uri, credential_base_url.?);
    const max_redirects: usize = 5;
    var redirects: usize = 0;

    while (true) {
        var response = client.request(.GET, current_url, .{
            .headers = if (send_credentials and headers.items.len > 0) headers.items else null,
            .follow_redirects = false,
            .timeout_ms = try ceiling.remainingTimeoutMs(),
            .max_response_size = maxDownloadSize(security),
            .cancellation = if (context.cancellation) |token|
                httpx.CancellationToken.fromCallback(token.ptr, token.is_cancelled_fn)
            else
                null,
        }) catch |err| switch (err) {
            error.AddressRejected => return error.PrivateIpBlocked,
            error.ResponseTooLarge => return error.StreamTooLong,
            error.OutOfMemory => return if (transport.exceeded) error.DownloadTransportMemoryExceeded else err,
            else => return err,
        };
        defer response.deinit();

        if (response.ok()) {
            const mime = trimMimeParameters(response.contentType() orelse "application/octet-stream");
            const owned_mime = try alloc.dupe(u8, mime);
            errdefer alloc.free(owned_mime);
            const body = try takeHttpResponseBodyAlloc(alloc, &response);
            errdefer alloc.free(body);
            try ceiling.check();
            return .{ .ok = .{
                .content_type = owned_mime,
                .data = body,
            } };
        }

        if (isSupportedRedirect(response.status.code)) {
            if (redirects >= max_redirects) return error.TooManyRedirects;
            const location = response.location() orelse return error.InvalidRedirect;
            const next_url = try resolveRedirectUrlAlloc(alloc, current_uri, location);
            errdefer alloc.free(next_url);
            const next_uri = try std.Uri.parse(next_url);
            try validateHttpUri(next_uri);
            try validateUrlSecurity(next_uri, security);
            send_credentials = send_credentials and
                sameOrigin(current_uri, next_uri) and
                (credential_base_url == null or httpUrlMatchesBase(next_uri, credential_base_url.?));
            alloc.free(current_url);
            current_url = next_url;
            current_uri = next_uri;
            redirects += 1;
            continue;
        }

        return .{ .http_error = .{
            .status = response.status.code,
            .retry_after_ms = retryAfterMs(response.header("Retry-After"), @intCast(@max(@divFloor(std.Io.Clock.real.now(context.io).nanoseconds, std.time.ns_per_ms), 0))),
            .message = "remote fetch failed",
            .downloaded_bytes = if (response.body) |body| @intCast(body.len) else 0,
        } };
    }
}

/// Transfers an httpx-owned response body when possible. The downloader and
/// client share an allocator, so a successful response does not briefly use
/// twice the configured response limit.
fn takeHttpResponseBodyAlloc(alloc: Allocator, response: *httpx.Response) ![]u8 {
    return response.takeBodyAlloc(alloc);
}

fn validateHttpUri(uri: std.Uri) !void {
    if (!std.ascii.eqlIgnoreCase(uri.scheme, "http") and !std.ascii.eqlIgnoreCase(uri.scheme, "https"))
        return error.UnsupportedUrlScheme;
    if (uri.host == null) return error.InvalidHost;
    if (uri.user != null or uri.password != null) return error.InvalidHost;
}

fn isSupportedRedirect(status: u16) bool {
    return switch (status) {
        301, 302, 303, 307, 308 => true,
        else => false,
    };
}

fn effectiveHttpPort(uri: std.Uri) u16 {
    return uri.port orelse if (std.ascii.eqlIgnoreCase(uri.scheme, "https")) 443 else 80;
}

fn sameOrigin(lhs: std.Uri, rhs: std.Uri) bool {
    const lhs_host = lhs.host orelse return false;
    const rhs_host = rhs.host orelse return false;
    return std.ascii.eqlIgnoreCase(lhs.scheme, rhs.scheme) and
        std.ascii.eqlIgnoreCase(lhs_host.percent_encoded, rhs_host.percent_encoded) and
        effectiveHttpPort(lhs) == effectiveHttpPort(rhs);
}

/// Matches a destination against a credential capability using canonical
/// scheme, authority and path-segment boundaries.
pub fn httpUrlMatchesBase(target: std.Uri, base_url: []const u8) bool {
    const base = std.Uri.parse(base_url) catch return false;
    const target_host = target.host orelse return false;
    const base_host = base.host orelse return false;
    if (base.user != null or base.password != null or base.query != null or base.fragment != null) return false;
    if (target.user != null or target.password != null) return false;
    if (!std.ascii.eqlIgnoreCase(target.scheme, base.scheme) or
        !std.ascii.eqlIgnoreCase(target_host.percent_encoded, base_host.percent_encoded) or
        effectiveHttpPort(target) != effectiveHttpPort(base)) return false;

    const base_path = base.path.percent_encoded;
    const target_path = target.path.percent_encoded;
    if (!credentialPathIsCanonical(base_path) or !credentialPathIsCanonical(target_path)) return false;
    if (base_path.len == 0 or std.mem.eql(u8, base_path, "/")) return true;
    if (std.mem.eql(u8, target_path, base_path)) return true;
    if (!std.mem.startsWith(u8, target_path, base_path)) return false;
    return std.mem.endsWith(u8, base_path, "/") or
        (target_path.len > base_path.len and target_path[base_path.len] == '/');
}

fn credentialPathIsCanonical(path: []const u8) bool {
    var segment_start: usize = 0;
    var i: usize = 0;
    while (i <= path.len) : (i += 1) {
        if (i != path.len and path[i] != '/') continue;
        const segment = path[segment_start..i];
        var decoded_len: usize = 0;
        var only_dots = segment.len > 0;
        var j: usize = 0;
        while (j < segment.len) {
            const byte = if (segment[j] == '%') blk: {
                if (j + 2 >= segment.len) return false;
                const high = std.fmt.charToDigit(segment[j + 1], 16) catch return false;
                const low = std.fmt.charToDigit(segment[j + 2], 16) catch return false;
                j += 3;
                break :blk @as(u8, @intCast(high * 16 + low));
            } else blk: {
                const value = segment[j];
                j += 1;
                break :blk value;
            };
            if (byte == '/' or byte == '\\') return false;
            decoded_len += 1;
            only_dots = only_dots and byte == '.';
        }
        if (only_dots and (decoded_len == 1 or decoded_len == 2)) return false;
        segment_start = i + 1;
    }
    return true;
}

fn formatUriAlloc(alloc: Allocator, uri: std.Uri) ![]u8 {
    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    try uri.format(&out.writer);
    return try out.toOwnedSlice();
}

fn resolveRedirectUrlAlloc(alloc: Allocator, base: std.Uri, location: []const u8) ![]u8 {
    const merged_path_capacity = std.math.add(usize, base.path.percent_encoded.len, location.len + 1) catch
        return error.OutOfMemory;
    const scratch_len = std.math.add(usize, location.len, merged_path_capacity) catch
        return error.OutOfMemory;
    const storage = try alloc.alloc(u8, scratch_len);
    defer alloc.free(storage);
    @memcpy(storage[0..location.len], location);
    var remaining = storage;
    var resolved = try std.Uri.resolveInPlace(base, location.len, &remaining);
    resolved.fragment = null;
    return try formatUriAlloc(alloc, resolved);
}

fn httpClientConfig(security: ?*const ContentSecurityConfig) httpx.ClientConfig {
    const block_private = if (security) |cfg| cfg.block_private_ips orelse true else false;
    return .{
        .user_agent = if (security) |cfg| cfg.user_agent orelse "AntflyDB/1.0" else "AntflyDB/1.0",
        .max_response_size = maxDownloadSize(security),
        .keep_alive = false,
        .cookies_enabled = false,
        .retry_policy = .noRetry(),
        .redirect_policy = .noFollow(),
        .address_filter = if (block_private) .{
            .context = if (security) |cfg| @ptrCast(cfg) else null,
            .acceptsFn = allowGlobalAddress,
        } else null,
    };
}

fn effectiveDownloadTimeoutMs(context: DownloadContext, security: ?*const ContentSecurityConfig) u64 {
    const configured_ms = if (security) |cfg|
        if (cfg.download_timeout_seconds) |seconds| @as(u64, seconds) * 1000 else default_download_timeout_ms
    else
        default_download_timeout_ms;
    const context_ms = context.timeout_ms orelse return configured_ms;
    if (configured_ms == 0) return context_ms;
    if (context_ms == 0) return configured_ms;
    return @min(configured_ms, context_ms);
}

const DownloadCeiling = struct {
    io: std.Io,
    cancellation: ?CancellationToken,
    timeout_ms: u64,
    started_at: std.Io.Timestamp,

    fn init(context: DownloadContext, security: ?*const ContentSecurityConfig) !DownloadCeiling {
        try context.check();
        return .{
            .io = context.io,
            .cancellation = context.cancellation,
            .timeout_ms = effectiveDownloadTimeoutMs(context, security),
            .started_at = std.Io.Timestamp.now(context.io, .awake),
        };
    }

    fn remainingTimeoutMs(self: *const DownloadCeiling) !u64 {
        try self.io.checkCancel();
        if (self.cancellation) |token| try token.check();
        if (self.timeout_ms == 0) return 0;
        const elapsed_ns = std.Io.Timestamp.durationTo(
            self.started_at,
            std.Io.Timestamp.now(self.io, .awake),
        ).toNanoseconds();
        return remainingDownloadTimeoutMs(self.timeout_ms, elapsed_ns);
    }

    fn check(self: *const DownloadCeiling) !void {
        _ = try self.remainingTimeoutMs();
    }
};

fn remainingDownloadTimeoutMs(timeout_ms: u64, elapsed_ns: i96) !u64 {
    if (timeout_ms == 0) return 0;
    const timeout_ns = @as(i96, timeout_ms) * std.time.ns_per_ms;
    const bounded_elapsed_ns = @max(@as(i96, 0), elapsed_ns);
    if (bounded_elapsed_ns >= timeout_ns) return error.Timeout;
    const remaining_ns = timeout_ns - bounded_elapsed_ns;
    return @intCast(@max(
        @as(i96, 1),
        @divTrunc(remaining_ns + std.time.ns_per_ms - 1, std.time.ns_per_ms),
    ));
}

fn downloadFileAlloc(
    alloc: Allocator,
    path: []const u8,
    security: ?*const ContentSecurityConfig,
) !DownloadedContent {
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    return downloadFileAllocWithIo(alloc, io_impl.io(), path, security);
}

fn downloadFileAllocWithContext(
    alloc: Allocator,
    context: DownloadContext,
    path: []const u8,
    security: ?*const ContentSecurityConfig,
) !DownloadedContent {
    // Authorization precedes timeout handling so forbidden file requests are
    // permanent denials even when the caller supplies a request deadline.
    try requireFileAccess(security);
    // std.Io file operations are cancelable but do not accept a deadline.
    // A caller-supplied execution boundary supervises potentially blocking
    // filesystems; without one a finite deadline fails closed.
    const lease = try context.enterFileIo();
    defer lease.deinit();
    try context.check();
    var downloaded = try downloadFileAllocWithIo(alloc, context.io, path, security);
    errdefer downloaded.deinit(alloc);
    try context.check();
    return downloaded;
}

fn downloadFileAllocWithIo(
    alloc: Allocator,
    io: std.Io,
    path: []const u8,
    security: ?*const ContentSecurityConfig,
) !DownloadedContent {
    try requireFileAccess(security);

    const limit = maxDownloadSize(security);
    try validateFilePathSecurityBeforeOpen(alloc, io, path, security);
    var file = try openRegularFile(io, path);
    defer file.close(io);
    try validateOpenFilePathSecurity(alloc, io, file, security);
    var reader = file.reader(io, &.{});
    const data = try allocRemainingBounded(&reader.interface, alloc, limit);
    errdefer alloc.free(data);
    return .{
        .content_type = try alloc.dupe(u8, guessMimeType(path)),
        .data = data,
    };
}

/// Storage-side authorization uses a fresh remote-content snapshot for every
/// source extraction, including continuation and repair. The inference worker
/// independently applies its own local filesystem policy before reading.
pub fn authorizeLocalSource(alloc: Allocator, io: std.Io, uri: []const u8, remote: ?*const RemoteContentConfig) !void {
    const config = remote orelse return error.PathNotAllowed;
    var snapshot = try config.acquire();
    defer snapshot.deinit();
    const security = if (snapshot.config.security) |*value| value else null;
    try requireFileAccess(security);
    const parsed = try std.Uri.parse(uri);
    const path_buf = try alloc.dupe(u8, parsed.path.percent_encoded);
    defer alloc.free(path_buf);
    const path = std.Uri.percentDecodeInPlace(path_buf);
    if (std.mem.indexOfScalar(u8, path, 0) != null) return error.InvalidSourceReference;
    try validateFilePathSecurityBeforeOpen(alloc, io, path, security);
}

fn openRegularFile(io: std.Io, path: []const u8) !std.Io.File {
    return @import("antfly_platform").filesystem.openRegularFile(io, path) catch |err| return switch (err) {
        error.NotRegularFile => error.InvalidSourceReference,
        else => err,
    };
}

fn requireFileAccess(security: ?*const ContentSecurityConfig) !void {
    const cfg = security orelse return error.PathNotAllowed;
    const allowed_paths = cfg.allowed_paths orelse return error.PathNotAllowed;
    if (allowed_paths.len == 0) return error.PathNotAllowed;
}

fn allocRemainingBounded(reader: *std.Io.Reader, alloc: Allocator, max_size: usize) ![]u8 {
    // Io's limited reader needs one byte of probe room to distinguish exact
    // fit from overflow. The returned payload never exceeds max_size.
    const probe_limit: ?usize = std.math.add(usize, max_size, 1) catch null;
    const data = if (probe_limit) |limit|
        try reader.allocRemaining(alloc, .limited(limit))
    else
        try reader.allocRemaining(alloc, .unlimited);
    if (data.len > max_size) {
        alloc.free(data);
        return error.StreamTooLong;
    }
    return data;
}

fn transportError(quota: *const httpx.QuotaAllocator, err: anyerror) anyerror {
    return if (err == error.OutOfMemory and quota.exceeded)
        error.DownloadTransportMemoryExceeded
    else
        normalizeS3DownloadError(err);
}

fn downloadHttpConfig(security: ?*const ContentSecurityConfig, body_allocator: Allocator) httpx.ClientConfig {
    var config = httpClientConfig(security);
    config.http1_response_body_allocator = body_allocator;
    return config;
}

pub const LocalVerificationCache = @import("local_verification.zig").Cache;
const sameFileVersion = @import("local_verification.zig").sameFileVersion;

/// Version-pinned range transport under the same URI, DNS and credential
/// boundaries as ordinary downloads. No redirects or mutable-object fallback.
pub const PinnedRange = struct {
    pub const VersionKind = @import("antfly_source_reference").VersionKind;
    allocator: Allocator,
    context: DownloadContext,
    transport: httpx.QuotaAllocator,
    security: ?*const ContentSecurityConfig,
    uri: []u8 = &.{},
    version: []u8 = &.{},
    identity: []u8 = &.{},
    kind: VersionKind,
    length: u64,
    bucket: []u8 = &.{},
    key: []u8 = &.{},
    backend: ?union(enum) { http: httpx.Client, s3: objectstore.S3.Client, file: LocalFile } = null,
    headers: []const HTTPHeader = &.{},
    file_io_lease: ?DownloadContext.BlockingLease = null,

    const LocalFile = struct {
        file: std.Io.File,
        stat: std.Io.File.Stat,
        fn check(self: *const LocalFile, io: std.Io) !void {
            const current = try self.file.stat(io);
            if (!sameFileVersion(current, self.stat)) return error.SourceVersionChanged;
        }
    };

    /// Validate the immutable source contract without allocation, credential
    /// resolution, transport construction or I/O. Destination policy and
    /// credentials are checked separately when creating the transport.
    pub fn validateReference(uri: []const u8, version: []const u8, kind: VersionKind, length: u64) !std.Uri {
        return @import("antfly_source_reference").validateReference(uri, version, kind, length);
    }

    pub fn create(alloc: Allocator, context: DownloadContext, uri: []const u8, version: []const u8, kind: VersionKind, length: u64, security: ?*const ContentSecurityConfig, credentials: ?*const S3CredentialsConfig, headers: []const HTTPHeader) !*PinnedRange {
        return createWithLocalCache(alloc, context, uri, version, kind, length, security, credentials, headers, null);
    }

    pub fn createWithLocalCache(alloc: Allocator, context: DownloadContext, uri: []const u8, version: []const u8, kind: VersionKind, length: u64, security: ?*const ContentSecurityConfig, credentials: ?*const S3CredentialsConfig, headers: []const HTTPHeader, cache: ?*LocalVerificationCache) !*PinnedRange {
        try context.check();
        if (headers.len > 128) return error.InvalidSourceReference;
        const parsed = try validateReference(uri, version, kind, length);
        const self = try alloc.create(PinnedRange);
        self.* = .{ .allocator = alloc, .context = context, .transport = .{ .backing = alloc, .limit = context.transport_memory_limit_bytes }, .security = security, .kind = kind, .length = length, .headers = headers };
        errdefer self.destroy();
        self.initBackend(uri, version, parsed, credentials, cache) catch |err| return transportError(&self.transport, err);
        return self;
    }

    fn initBackend(self: *PinnedRange, uri: []const u8, version: []const u8, parsed: std.Uri, credentials: ?*const S3CredentialsConfig, cache: ?*LocalVerificationCache) !void {
        const alloc = self.allocator;
        const transport_alloc = self.transport.allocator();
        const context = self.context;
        const security = self.security;
        const headers = self.headers;
        self.uri = try alloc.dupe(u8, uri);
        self.version = try alloc.dupe(u8, version);
        if (std.ascii.eqlIgnoreCase(parsed.scheme, "http") or std.ascii.eqlIgnoreCase(parsed.scheme, "https")) {
            try validateUrlSecurity(parsed, security);
            self.identity = try alloc.dupe(u8, uri);
            for (headers) |header| {
                if (std.ascii.eqlIgnoreCase(header.name, "Range") or std.ascii.eqlIgnoreCase(header.name, "If-Match") or std.ascii.eqlIgnoreCase(header.name, "Accept-Encoding")) return error.InvalidSourceReference;
                if (header.credential_base_url) |base| if (!httpUrlMatchesBase(parsed, base)) return error.CredentialUrlMismatch;
            }
            self.backend = .{ .http = httpx.Client.initWithConfig(transport_alloc, context.io, downloadHttpConfig(security, alloc)) };
        } else if (std.ascii.eqlIgnoreCase(parsed.scheme, "s3")) {
            if (headers.len != 0) return error.InvalidSourceReference;
            const cfg = credentials orelse return error.MissingS3Credentials;
            var endpoint = try objectstore.resolveS3EndpointAlloc(alloc, cfg.endpoint orelse return error.MissingEndpoint, cfg.use_ssl orelse true);
            defer endpoint.deinit(alloc);
            const bucket, const key = try parseS3LocationAlloc(alloc, parsed, endpoint.endpoint, endpoint.use_ssl);
            self.bucket = bucket;
            self.key = key;
            self.identity = try std.fmt.allocPrint(alloc, "s3:{s}:{s}:{s}/{s}", .{ if (endpoint.use_ssl) "https" else "http", endpoint.endpoint, bucket, key });
            const joined = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ bucket, key });
            defer alloc.free(joined);
            try validateS3PathSecurity(joined, security);
            var owned = try allocS3Credentials(transport_alloc, endpoint.endpoint, endpoint.use_ssl, cfg);
            const client = objectstore.S3.Client.initWithHttpContext(transport_alloc, .{ .credentials = owned, .addressing_style = .path }, .{ .io = context.io, .timeout_ms = context.timeout_ms, .client_config = downloadHttpConfig(security, alloc) }) catch |err| {
                owned.deinit(transport_alloc);
                return err;
            };
            self.backend = .{ .s3 = client };
        } else if (std.ascii.eqlIgnoreCase(parsed.scheme, "file")) {
            if (headers.len != 0) return error.InvalidSourceReference;
            try requireFileAccess(security);
            self.file_io_lease = try context.enterFileIo();
            const path_buf = try alloc.dupe(u8, parsed.path.percent_encoded);
            defer alloc.free(path_buf);
            const path = std.Uri.percentDecodeInPlace(path_buf);
            if (std.mem.indexOfScalar(u8, path, 0) != null) return error.InvalidSourceReference;
            try validateFilePathSecurityBeforeOpen(alloc, context.io, path, security);
            const file = try openRegularFile(context.io, path);
            errdefer file.close(context.io);
            try validateOpenFilePathSecurity(alloc, context.io, file, security);
            const stat = try file.stat(context.io);
            if (stat.kind != .file) return error.InvalidSourceReference;
            if (stat.size != self.length) return error.SourceVersionChanged;
            const local = LocalFile{ .file = file, .stat = stat };
            var canonical_buf: [std.Io.Dir.max_path_bytes]u8 = undefined;
            self.identity = try std.fmt.allocPrint(alloc, "file:{s}", .{canonical_buf[0..try file.realPath(context.io, &canonical_buf)]});
            var key_hash = std.crypto.hash.sha2.Sha256.init(.{});
            key_hash.update(self.identity);
            const key = key_hash.finalResult();
            const cached_digest = if (cache) |receipts| receipts.lookup(key, stat) else null;
            const digest = cached_digest orelse computed: {
                const verification = if (cache) |receipts| try receipts.acquire(context, key) else null;
                defer if (verification) |lease| lease.deinit();
                // A preceding verifier may have populated the receipt while
                // this request waited. Warm hits never occupy flight slots.
                try local.check(context.io);
                if (cache) |receipts| if (receipts.lookup(key, stat)) |digest| break :computed digest;
                if (cache) |receipts| _ = receipts.hash_count.fetchAdd(1, .monotonic);
                // Hash outside the cache lock. Warm continuation requests use
                // verified metadata without rescanning a multi-GB recording.
                var hash = std.crypto.hash.sha2.Sha256.init(.{});
                const buffer = try alloc.alloc(u8, 256 * 1024);
                defer alloc.free(buffer);
                var offset: u64 = 0;
                while (offset < self.length) {
                    try context.check();
                    const n = try file.readPositional(context.io, &.{buffer[0..@intCast(@min(buffer.len, self.length - offset))]}, offset);
                    if (n == 0) return error.SourceVersionChanged;
                    hash.update(buffer[0..n]);
                    if (cache) |receipts| _ = receipts.hashed_bytes.fetchAdd(n, .monotonic);
                    offset += n;
                }
                try context.check();
                try local.check(context.io);
                const actual = std.fmt.bytesToHex(hash.finalResult(), .lower);
                // Receipts bind actual bytes to a descriptor stamp, not an
                // untrusted caller version. Even simultaneous wrong claims
                // share this one scan and cannot evict it with random digests.
                if (cache) |receipts| receipts.remember(key, stat, actual);
                break :computed actual;
            };
            if (!std.mem.eql(u8, &digest, version)) return error.SourceVersionChanged;
            try local.check(context.io);
            self.backend = .{ .file = local };
        } else return error.UnsupportedUrlScheme;
    }
    pub fn destroy(self: *PinnedRange) void {
        if (self.backend) |*backend| switch (backend.*) {
            .http => |*client| client.deinit(),
            .s3 => |*client| client.deinit(),
            .file => |*local| local.file.close(self.context.io),
        };
        if (self.file_io_lease) |lease| lease.deinit();
        const a = self.allocator;
        a.free(self.uri);
        a.free(self.version);
        a.free(self.identity);
        a.free(self.bucket);
        a.free(self.key);
        a.destroy(self);
    }
    /// Also guard completion when the media read-ahead cache serves every read.
    pub fn validate(self: *PinnedRange) !void {
        try self.context.check();
        if (self.backend) |*backend| switch (backend.*) {
            .file => |*local| {
                try requireFileAccess(self.security);
                try validateOpenFilePathSecurity(self.allocator, self.context.io, local.file, self.security);
                try local.check(self.context.io);
            },
            else => {},
        };
    }
    pub fn readAt(self: *PinnedRange, offset: u64, out: []u8) !usize {
        return self.readAtInternal(offset, out) catch |err| transportError(&self.transport, err);
    }
    fn readAtInternal(self: *PinnedRange, offset: u64, out: []u8) !usize {
        try self.context.check();
        if (offset > self.length or out.len == 0 or out.len > self.length - offset) return error.InvalidRemoteRange;
        if (out.len > maxDownloadSize(self.security)) return error.StreamTooLong;
        const ceiling = try DownloadCeiling.init(self.context, self.security);
        switch (self.backend.?) {
            .file => |*local| {
                try requireFileAccess(self.security);
                try validateOpenFilePathSecurity(self.allocator, self.context.io, local.file, self.security);
                try local.check(self.context.io);
                var done: usize = 0;
                while (done < out.len) {
                    try self.context.check();
                    const n = try local.file.readPositional(self.context.io, &.{out[done..]}, offset + done);
                    if (n == 0) return error.SourceVersionChanged;
                    done += n;
                }
                try local.check(self.context.io);
                try self.context.check();
            },
            .http => |*client| {
                const headers = try self.allocator.alloc([2][]const u8, self.headers.len + 3);
                defer self.allocator.free(headers);
                var range: [96]u8 = undefined;
                headers[0] = .{ "Range", try std.fmt.bufPrint(&range, "bytes={d}-{d}", .{ offset, offset + out.len - 1 }) };
                headers[1] = .{ "If-Match", self.version };
                headers[2] = .{ "Accept-Encoding", "identity" };
                for (self.headers, headers[3..]) |header, *dest| dest.* = .{ header.name, header.value };
                var response = try client.get(self.uri, .{ .headers = headers, .max_response_size = out.len, .follow_redirects = false, .max_retries = 0, .cookies_enabled = false, .timeout_ms = try ceiling.remainingTimeoutMs(), .cancellation = if (self.context.cancellation) |token| httpx.CancellationToken.fromCallback(token.ptr, token.is_cancelled_fn) else null });
                defer response.deinit();
                try self.context.check();
                if (response.status.code == 412) return error.SourceVersionChanged;
                if (response.status.code != 206) return error.InvalidRemoteRange;
                if (!std.mem.eql(u8, response.header("ETag") orelse return error.SourceVersionChanged, self.version)) return error.SourceVersionChanged;
                if (response.header("Content-Encoding")) |encoding| if (!std.ascii.eqlIgnoreCase(encoding, "identity")) return error.InvalidRemoteRange;
                const body = response.body orelse return error.InvalidRemoteRange;
                if (body.len != out.len) return error.InvalidRemoteRange;
                try validatePinnedContentRange(response.header("Content-Range") orelse return error.InvalidRemoteRange, offset, out.len, self.length);
                @memcpy(out, body);
            },
            .s3 => |*client| {
                var store = client.client();
                store.allocator = self.allocator;
                var result = try store.getObject(self.bucket, self.key, .{ .version_id = if (self.kind == .s3_version_id) self.version else null, .if_match_etag = if (self.kind == .strong_etag) self.version else null, .range = .{ .offset = offset, .length = out.len }, .skip_metadata_probe = true, .max_response_bytes = out.len, .cancellation = self.context.cancellation });
                defer result.deinit(self.allocator);
                try self.context.check();
                const range = result.response_range orelse return error.InvalidRemoteRange;
                if (result.response_status != 206 or range.offset != offset or range.total != self.length or range.length != out.len or result.body.len != out.len) return error.InvalidRemoteRange;
                if (self.kind == .strong_etag) {
                    if (!result.response_etag_strong) return error.SourceVersionChanged;
                    const etag = result.metadata.etag orelse return error.SourceVersionChanged;
                    const normalized = if (etag.len >= 2 and etag[0] == '"' and etag[etag.len - 1] == '"') etag[1 .. etag.len - 1] else etag;
                    if (!std.mem.eql(u8, normalized, self.version[1 .. self.version.len - 1])) return error.SourceVersionChanged;
                } else if (!std.mem.eql(u8, result.metadata.version_id orelse return error.SourceVersionChanged, self.version)) return error.SourceVersionChanged;
                @memcpy(out, result.body);
            },
        }
        return out.len;
    }
};
fn validatePinnedContentRange(value: []const u8, offset: u64, length: usize, total: u64) !void {
    if (!std.mem.startsWith(u8, value, "bytes ")) return error.InvalidRemoteRange;
    var parts = std.mem.splitScalar(u8, value[6..], '/');
    const endpoints = parts.next() orelse return error.InvalidRemoteRange;
    const wire_total = std.fmt.parseInt(u64, parts.next() orelse return error.InvalidRemoteRange, 10) catch return error.InvalidRemoteRange;
    if (parts.next() != null or wire_total != total) return error.InvalidRemoteRange;
    var range = std.mem.splitScalar(u8, endpoints, '-');
    const first = std.fmt.parseInt(u64, range.next().?, 10) catch return error.InvalidRemoteRange;
    const last = std.fmt.parseInt(u64, range.next() orelse return error.InvalidRemoteRange, 10) catch return error.InvalidRemoteRange;
    if (range.next() != null or first != offset or last < first or last >= total or last - first + 1 != length) return error.InvalidRemoteRange;
}

/// Structural S3 validation without credentials, allocation or transport I/O.
/// A template prefix may have a known authority while its key is still open.
pub fn validateS3Uri(uri: std.Uri, options: struct { complete: bool = true }) !void {
    if (!std.ascii.eqlIgnoreCase(uri.scheme, "s3")) return error.UnsupportedUrlScheme;
    const host = uri.host orelse return error.InvalidS3Url;
    if (host.percent_encoded.len == 0) return error.InvalidS3Url;
    if (options.complete and trimLeftSlash(uri.path.percent_encoded).len == 0) return error.InvalidS3Url;
}

/// Fetch through an explicitly authorized GCS connection. A bare gs:// URL
/// never opts into ambient credentials. Generation-qualified reads use the
/// existing JSON API client's immutable-version validation.
pub fn downloadGcsContentAlloc(
    alloc: Allocator,
    maybe_context: ?DownloadContext,
    uri: []const u8,
    security: ?*const ContentSecurityConfig,
    connection: *const GcsContentConnection,
) !DownloadedContent {
    var transport: httpx.QuotaAllocator = .{ .backing = alloc, .limit = if (maybe_context) |ctx| ctx.transport_memory_limit_bytes else default_download_transport_memory_bytes };
    return downloadGcsContentAllocInternal(alloc, maybe_context, uri, security, connection, &transport) catch |err| transportError(&transport, err);
}

fn downloadGcsContentAllocInternal(
    alloc: Allocator,
    maybe_context: ?DownloadContext,
    uri: []const u8,
    security: ?*const ContentSecurityConfig,
    connection: *const GcsContentConnection,
    transport: *httpx.QuotaAllocator,
) !DownloadedContent {
    const transport_alloc = transport.allocator();
    var location = try parseGcsLocationAlloc(alloc, uri);
    defer location.deinit(alloc);
    if (!connection.allows(&location)) return error.GcsConnectionNotFoundOrOutOfScope;
    const joined = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ location.bucket, location.key });
    defer alloc.free(joined);
    try validateS3PathSecurity(joined, security);
    var local_io: ?std.Io.Threaded = if (maybe_context == null) std.Io.Threaded.init(alloc, .{}) else null;
    defer if (local_io) |*owned| owned.deinit();
    const context = maybe_context orelse DownloadContext{ .io = local_io.?.io() };
    const ceiling = try DownloadCeiling.init(context, security);
    try ceiling.check();
    var cfg = try objectstore.Gcs.clientConfigForCredentialsAlloc(
        transport_alloc,
        connection.credentials,
        connection.project_id,
        connection.endpoint,
        null,
        context.io,
        context.io,
    );
    var config_owned = true;
    errdefer if (config_owned) cfg.deinit(transport_alloc);
    try validateUrlSecurity(try std.Uri.parse(cfg.endpoint), security);
    cfg.http_client_config = downloadHttpConfig(security, alloc);
    const remaining = try ceiling.remainingTimeoutMs();
    cfg.request_timeout_ms = remaining;
    cfg.request_deadline_ns = if (remaining == 0) null else @import("antfly_platform").time.monotonicNs() +| (remaining *| std.time.ns_per_ms);
    var client = try objectstore.Gcs.JsonApiClient.init(transport_alloc, cfg);
    config_owned = false;
    defer client.deinit();
    var store = client.client();
    store.allocator = alloc;
    const max_bytes = maxDownloadSizeBytes(security);
    const response_ceiling = std.math.add(u64, max_bytes, 1) catch max_bytes;
    var result = store.getObject(location.bucket, location.key, .{
        .version_id = location.generation,
        .skip_metadata_probe = location.generation != null,
        .max_response_bytes = std.math.cast(usize, response_ceiling) orelse std.math.maxInt(usize),
        .cancellation = context.cancellation,
    }) catch |err| return normalizeS3DownloadError(err);
    defer result.deinit(alloc);
    if (result.body.len > max_bytes) return error.StreamTooLong;
    const content_type = try alloc.dupe(u8, result.metadata.content_type orelse guessMimeType(location.key));
    errdefer alloc.free(content_type);
    try ceiling.check();
    const data = result.body;
    result.body = &.{};
    return .{ .content_type = content_type, .data = data };
}

fn downloadS3Alloc(
    alloc: Allocator,
    maybe_context: ?DownloadContext,
    parsed: std.Uri,
    security: ?*const ContentSecurityConfig,
    s3_credentials: ?*const S3CredentialsConfig,
) !DownloadedContent {
    var transport: httpx.QuotaAllocator = .{ .backing = alloc, .limit = if (maybe_context) |ctx| ctx.transport_memory_limit_bytes else default_download_transport_memory_bytes };
    return downloadS3AllocInternal(alloc, maybe_context, parsed, security, s3_credentials, &transport) catch |err| transportError(&transport, err);
}

fn downloadS3AllocInternal(
    alloc: Allocator,
    maybe_context: ?DownloadContext,
    parsed: std.Uri,
    security: ?*const ContentSecurityConfig,
    s3_credentials: ?*const S3CredentialsConfig,
    transport: *httpx.QuotaAllocator,
) !DownloadedContent {
    const transport_alloc = transport.allocator();
    try validateS3Uri(parsed, .{});
    var local_io_impl: ?std.Io.Threaded = if (maybe_context == null)
        std.Io.Threaded.init(alloc, .{})
    else
        null;
    defer if (local_io_impl) |*io_impl| io_impl.deinit();
    const context = maybe_context orelse DownloadContext{ .io = local_io_impl.?.io() };
    const ceiling = try DownloadCeiling.init(context, security);
    const creds_cfg = s3_credentials orelse return error.MissingS3Credentials;
    const configured_endpoint = creds_cfg.endpoint orelse return error.MissingEndpoint;
    var resolved_endpoint = try objectstore.resolveS3EndpointAlloc(
        alloc,
        configured_endpoint,
        creds_cfg.use_ssl orelse true,
    );
    defer resolved_endpoint.deinit(alloc);

    const bucket, const key = try parseS3LocationAlloc(
        alloc,
        parsed,
        resolved_endpoint.endpoint,
        resolved_endpoint.use_ssl,
    );
    defer alloc.free(bucket);
    defer alloc.free(key);

    const joined_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ bucket, key });
    defer alloc.free(joined_path);
    try validateS3PathSecurity(joined_path, security);

    var creds = try allocS3Credentials(transport_alloc, resolved_endpoint.endpoint, resolved_endpoint.use_ssl, creds_cfg);
    var client = objectstore.S3.Client.initWithHttpContext(transport_alloc, .{
        .credentials = creds,
        .addressing_style = .path,
    }, .{
        .io = ceiling.io,
        .timeout_ms = ceiling.remainingTimeoutMs() catch |err| {
            creds.deinit(transport_alloc);
            return err;
        },
        .client_config = downloadHttpConfig(security, alloc),
    }) catch |err| {
        creds.deinit(transport_alloc);
        return err;
    };
    defer client.deinit();

    var store_client = client.client();
    store_client.allocator = alloc;
    const max_size = maxDownloadSizeBytes(security);
    // getObject performs the metadata HEAD needed to populate its result.
    // A second preflight stat would turn every download into HEAD+HEAD+GET.
    const range_length = std.math.add(u64, max_size, 1) catch null;
    var result = store_client.getObject(bucket, key, .{
        .range = if (range_length) |length| .{ .offset = 0, .length = length } else null,
        .cancellation = context.cancellation,
    }) catch |err| return normalizeS3DownloadError(err);
    defer result.deinit(alloc);
    if (result.body.len > max_size) return error.StreamTooLong;

    const content_type = try alloc.dupe(u8, result.metadata.content_type orelse guessMimeType(key));
    errdefer alloc.free(content_type);
    try ceiling.check();
    const data = result.body;
    result.body = &.{};
    const downloaded = DownloadedContent{
        .content_type = content_type,
        .data = data,
    };
    return downloaded;
}

fn normalizeS3DownloadError(err: anyerror) anyerror {
    return switch (err) {
        error.ResponseTooLarge => error.StreamTooLong,
        error.AddressRejected => error.PrivateIpBlocked,
        else => err,
    };
}

fn allocS3Credentials(
    alloc: Allocator,
    endpoint: []const u8,
    use_ssl: bool,
    cfg: *const S3CredentialsConfig,
) !objectstore.S3Credentials {
    const access_key_id = cfg.access_key_id orelse return error.MissingAccessKeyId;
    const secret_access_key = cfg.secret_access_key orelse return error.MissingSecretAccessKey;
    const owned_endpoint = try alloc.dupe(u8, endpoint);
    errdefer alloc.free(owned_endpoint);
    const owned_access_key_id = try alloc.dupe(u8, access_key_id);
    errdefer alloc.free(owned_access_key_id);
    const owned_secret_access_key = try alloc.dupe(u8, secret_access_key);
    errdefer alloc.free(owned_secret_access_key);
    const owned_session_token = if (cfg.session_token) |value| try alloc.dupe(u8, value) else null;
    errdefer if (owned_session_token) |value| alloc.free(value);
    const region = try alloc.dupe(u8, "us-east-1");
    errdefer alloc.free(region);
    return .{
        .endpoint = owned_endpoint,
        .use_ssl = use_ssl,
        .access_key_id = owned_access_key_id,
        .secret_access_key = owned_secret_access_key,
        .session_token = owned_session_token,
        .region = region,
    };
}

fn parseS3LocationAlloc(
    alloc: Allocator,
    parsed: std.Uri,
    configured_endpoint: []const u8,
    use_ssl: bool,
) !struct { []u8, []u8 } {
    try validateS3Uri(parsed, .{});
    const host_text = parsed.host.?.percent_encoded;
    const path = trimLeftSlash(parsed.path.percent_encoded);

    if (try s3AuthorityMatchesEndpoint(alloc, host_text, parsed.port, configured_endpoint, use_ssl)) {
        const slash = std.mem.indexOfScalar(u8, path, '/') orelse return error.InvalidS3Url;
        if (slash == 0 or slash + 1 == path.len) return error.InvalidS3Url;
        const bucket = try alloc.dupe(u8, path[0..slash]);
        errdefer alloc.free(bucket);
        return .{
            bucket,
            try alloc.dupe(u8, path[slash + 1 ..]),
        };
    }

    const bucket = try alloc.dupe(u8, host_text);
    errdefer alloc.free(bucket);
    return .{
        bucket,
        try alloc.dupe(u8, path),
    };
}

/// Returns the canonical bucket used by the S3 downloader for both
/// `s3://bucket/key` and endpoint-style `s3://endpoint/bucket/key` URLs.
pub fn s3BucketAlloc(
    alloc: Allocator,
    parsed: std.Uri,
    configured_endpoint: []const u8,
    use_ssl: ?bool,
) ![]u8 {
    var resolved_endpoint = try objectstore.resolveS3EndpointAlloc(
        alloc,
        configured_endpoint,
        use_ssl orelse true,
    );
    defer resolved_endpoint.deinit(alloc);
    const bucket, const key = try parseS3LocationAlloc(
        alloc,
        parsed,
        resolved_endpoint.endpoint,
        resolved_endpoint.use_ssl,
    );
    defer alloc.free(key);
    return bucket;
}

fn s3AuthorityMatchesEndpoint(
    alloc: Allocator,
    host: []const u8,
    port: ?u16,
    endpoint: []const u8,
    use_ssl: bool,
) !bool {
    const authority = if (port) |value|
        try std.fmt.allocPrint(alloc, "{s}:{d}", .{ host, value })
    else
        try alloc.dupe(u8, host);
    defer alloc.free(authority);
    if (std.ascii.eqlIgnoreCase(authority, endpoint)) return true;

    const default_port: u16 = if (use_ssl) 443 else 80;
    if (port == default_port and std.ascii.eqlIgnoreCase(host, endpoint)) return true;
    if (port == null) {
        const endpoint_with_default_port = try std.fmt.allocPrint(alloc, "{s}:{d}", .{ host, default_port });
        defer alloc.free(endpoint_with_default_port);
        if (std.ascii.eqlIgnoreCase(endpoint_with_default_port, endpoint)) return true;
    }
    return false;
}

fn validateUrlSecurity(parsed: std.Uri, security: ?*const ContentSecurityConfig) !void {
    const cfg = security orelse return;
    var host_buffer: [std.Io.net.HostName.max_len]u8 = undefined;
    const host = ((parsed.host orelse return error.InvalidHost).toRaw(&host_buffer) catch return error.InvalidHost);

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

    // Reject dangerous literal spellings before network I/O. DNS names are
    // vetted and pinned by httpx's connection-time address filter.
    if (cfg.block_private_ips orelse true and isNonGlobalHostWithSecurity(host, cfg)) return error.PrivateIpBlocked;
}

fn validateOpenFilePathSecurity(
    alloc: Allocator,
    io: std.Io,
    file: std.Io.File,
    security: ?*const ContentSecurityConfig,
) !void {
    const cfg = security orelse return;
    const allowed_paths = cfg.allowed_paths orelse return;
    var canonical_path_buf: [std.Io.Dir.max_path_bytes]u8 = undefined;
    const canonical_path = canonical_path_buf[0..try file.realPath(io, &canonical_path_buf)];
    try validateCanonicalFilePathSecurity(alloc, io, canonical_path, allowed_paths);
}

fn validateFilePathSecurityBeforeOpen(
    alloc: Allocator,
    io: std.Io,
    path: []const u8,
    security: ?*const ContentSecurityConfig,
) !void {
    const cfg = security orelse return;
    const allowed_paths = cfg.allowed_paths orelse return;
    const resolved_path = try lexicalAbsolutePathAlloc(alloc, io, path);
    defer alloc.free(resolved_path);
    for (allowed_paths) |allowed| {
        const resolved_allowed = try lexicalAbsolutePathAlloc(alloc, io, allowed);
        defer alloc.free(resolved_allowed);
        if (hasComponentPrefix(resolved_path, resolved_allowed, std.fs.path.sep)) return;
    }
    return error.PathNotAllowed;
}

fn lexicalAbsolutePathAlloc(alloc: Allocator, io: std.Io, path: []const u8) ![]u8 {
    if (std.fs.path.isAbsolute(path)) return std.fs.path.resolve(alloc, &.{path});
    const cwd = try std.Io.Dir.cwd().realPathFileAlloc(io, ".", alloc);
    defer alloc.free(cwd);
    return std.fs.path.resolve(alloc, &.{ cwd, path });
}

fn validateCanonicalFilePathSecurity(
    alloc: Allocator,
    io: std.Io,
    canonical_path: []const u8,
    allowed_paths: []const []u8,
) !void {
    for (allowed_paths) |allowed| {
        const canonical_allowed = realPathExistingAlloc(alloc, io, allowed) catch |err| switch (err) {
            error.FileNotFound, error.NotDir => continue,
            else => return err,
        };
        defer alloc.free(canonical_allowed);
        if (hasComponentPrefix(canonical_path, canonical_allowed, std.fs.path.sep)) return;
    }
    return error.PathNotAllowed;
}

fn validateS3PathSecurity(path: []const u8, security: ?*const ContentSecurityConfig) !void {
    const cfg = security orelse return;
    const allowed_paths = cfg.allowed_paths orelse return;
    for (allowed_paths) |allowed| {
        if (hasComponentPrefix(path, allowed, '/')) return;
    }
    return error.PathNotAllowed;
}

fn realPathExistingAlloc(alloc: Allocator, io: std.Io, path: []const u8) ![:0]u8 {
    if (std.fs.path.isAbsolute(path)) {
        return try std.Io.Dir.realPathFileAbsoluteAlloc(io, path, alloc);
    }
    return try std.Io.Dir.cwd().realPathFileAlloc(io, path, alloc);
}

fn hasComponentPrefix(path: []const u8, prefix: []const u8, separator: u8) bool {
    if (prefix.len == 0 or !std.mem.startsWith(u8, path, prefix)) return false;
    return path.len == prefix.len or prefix[prefix.len - 1] == separator or path[prefix.len] == separator;
}

fn allowGlobalAddress(context: ?*const anyopaque, address: std.Io.net.IpAddress) bool {
    const security: ?*const ContentSecurityConfig = if (context) |ptr| @ptrCast(@alignCast(ptr)) else null;
    return switch (address) {
        .ip4 => |ip4| !isNonGlobalIpv4(ip4.bytes),
        .ip6 => |ip6| ip6.interface.isNone() and !isNonGlobalIpv6WithSecurity(ip6, security),
    };
}

/// `block_private_ips` rejects literal addresses that are not globally
/// reachable, not only RFC 1918 addresses. Hostnames are classified after DNS
/// resolution by `allowGlobalAddress` at the connection boundary.
fn isNonGlobalHost(host: []const u8) bool {
    return isNonGlobalHostWithSecurity(host, null);
}

fn isNonGlobalHostWithSecurity(host: []const u8, security: ?*const ContentSecurityConfig) bool {
    var normalized = std.mem.trimEnd(u8, host, ".");
    if (normalized.len >= 2 and normalized[0] == '[' and normalized[normalized.len - 1] == ']') {
        normalized = normalized[1 .. normalized.len - 1];
    }
    // Zone identifiers are only meaningful for scoped destinations. Zig's
    // literal parser intentionally leaves them to the resolver, so reject
    // them here rather than accidentally treating a scoped address as public.
    if (std.mem.indexOfScalar(u8, normalized, '%') != null) return true;
    if (std.ascii.eqlIgnoreCase(normalized, "localhost")) return true;
    if (std.ascii.endsWithIgnoreCase(normalized, ".localhost")) return true;
    if (std.ascii.endsWithIgnoreCase(normalized, ".local")) return true;

    const address = std.Io.net.IpAddress.parse(normalized, 0) catch
        return looksLikeNoncanonicalIpLiteral(normalized);
    return switch (address) {
        .ip4 => |ip4| {
            return isNonGlobalIpv4(ip4.bytes);
        },
        .ip6 => |ip6| {
            return !ip6.interface.isNone() or isNonGlobalIpv6WithSecurity(ip6, security);
        },
    };
}

fn looksLikeNoncanonicalIpLiteral(host: []const u8) bool {
    if (host.len == 0) return false;

    var decimal_only = true;
    for (host) |char| {
        if (!std.ascii.isDigit(char) and char != '.') {
            decimal_only = false;
            break;
        }
    }
    if (decimal_only) return true;

    var saw_hex_component = false;
    var components = std.mem.splitScalar(u8, host, '.');
    while (components.next()) |component| {
        if (component.len > 2 and component[0] == '0' and (component[1] == 'x' or component[1] == 'X')) {
            for (component[2..]) |char| if (!std.ascii.isHex(char)) return false;
            saw_hex_component = true;
            continue;
        }
        if (component.len == 0) return false;
        for (component) |char| if (!std.ascii.isDigit(char)) return false;
    }
    return saw_hex_component;
}

fn isNonGlobalIpv4(b: [4]u8) bool {
    return b[0] == 0 or
        b[0] == 10 or
        (b[0] == 100 and b[1] >= 64 and b[1] <= 127) or
        b[0] == 127 or
        (b[0] == 169 and b[1] == 254) or
        (b[0] == 172 and b[1] >= 16 and b[1] <= 31) or
        (b[0] == 192 and b[1] == 0 and (b[2] == 0 or b[2] == 2)) or
        (b[0] == 192 and b[1] == 168) or
        (b[0] == 192 and b[1] == 88 and b[2] == 99) or
        (b[0] == 198 and (b[1] == 18 or b[1] == 19)) or
        (b[0] == 198 and b[1] == 51 and b[2] == 100) or
        (b[0] == 203 and b[1] == 0 and b[2] == 113) or
        b[0] >= 224;
}

const Nat64Prefix = struct {
    bytes: [16]u8,
    length: u8,
};

const Nat64Decode = union(enum) {
    no_match,
    malformed,
    ipv4: [4]u8,
};

const well_known_nat64 = Nat64Prefix{
    .bytes = .{ 0, 0x64, 0xff, 0x9b, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 },
    .length = 96,
};

const local_use_nat64 = Nat64Prefix{
    .bytes = .{ 0, 0x64, 0xff, 0x9b, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 },
    .length = 48,
};

fn validNat64PrefixLength(length: u8) bool {
    return switch (length) {
        32, 40, 48, 56, 64, 96 => true,
        else => false,
    };
}

fn parseNat64Prefix(text: []const u8) !Nat64Prefix {
    const slash = std.mem.lastIndexOfScalar(u8, text, '/') orelse return error.InvalidNat64Prefix;
    if (slash == 0 or slash + 1 == text.len) return error.InvalidNat64Prefix;
    const length = std.fmt.parseUnsigned(u8, text[slash + 1 ..], 10) catch return error.InvalidNat64Prefix;
    if (!validNat64PrefixLength(length)) return error.InvalidNat64Prefix;
    const address = std.Io.net.IpAddress.parse(text[0..slash], 0) catch return error.InvalidNat64Prefix;
    const bytes = switch (address) {
        .ip6 => |ip6| ip6.bytes,
        .ip4 => return error.InvalidNat64Prefix,
    };
    const prefix_bytes = @as(usize, length) / 8;
    if (!std.mem.allEqual(u8, bytes[prefix_bytes..], 0)) return error.InvalidNat64Prefix;
    return .{ .bytes = bytes, .length = length };
}

pub fn validateNat64Prefixes(security: ContentSecurityConfig) !void {
    if (security.nat64_prefixes) |prefixes| {
        for (prefixes) |prefix| _ = try parseNat64Prefix(prefix);
    }
}

fn decodeNat64(ip6: std.Io.net.Ip6Address, prefix: Nat64Prefix) Nat64Decode {
    const b = ip6.bytes;
    const prefix_bytes = @as(usize, prefix.length) / 8;
    if (!std.mem.eql(u8, b[0..prefix_bytes], prefix.bytes[0..prefix_bytes])) return .no_match;
    if (prefix.length == 96) return .{ .ipv4 = b[12..16].* };
    if (b[8] != 0) return .malformed;
    const decoded, const suffix_start = switch (prefix.length) {
        32 => .{ b[4..8].*, @as(usize, 9) },
        40 => .{ .{ b[5], b[6], b[7], b[9] }, @as(usize, 10) },
        48 => .{ .{ b[6], b[7], b[9], b[10] }, @as(usize, 11) },
        56 => .{ .{ b[7], b[9], b[10], b[11] }, @as(usize, 12) },
        64 => .{ b[9..13].*, @as(usize, 13) },
        else => return .malformed,
    };
    if (!std.mem.allEqual(u8, b[suffix_start..], 0)) return .malformed;
    return .{ .ipv4 = decoded };
}

fn nat64NonGlobal(ip6: std.Io.net.Ip6Address, prefix: Nat64Prefix) ?bool {
    return switch (decodeNat64(ip6, prefix)) {
        .no_match => null,
        .malformed => true,
        .ipv4 => |bytes| isNonGlobalIpv4(bytes),
    };
}

fn isNonGlobalIpv6WithSecurity(ip6: std.Io.net.Ip6Address, security: ?*const ContentSecurityConfig) bool {
    if (nat64NonGlobal(ip6, well_known_nat64)) |non_global| return non_global;
    if (nat64NonGlobal(ip6, local_use_nat64)) |non_global| return non_global;
    if (security) |cfg| if (cfg.nat64_prefixes) |prefixes| {
        for (prefixes) |text| {
            const prefix = parseNat64Prefix(text) catch return true;
            if (nat64NonGlobal(ip6, prefix)) |non_global| return non_global;
        }
    };
    return isNonGlobalIpv6(ip6.bytes);
}

fn isNonGlobalIpv6(b: [16]u8) bool {
    // Preserve the IPv4 classification for IPv4-mapped literals.
    if (std.mem.allEqual(u8, b[0..10], 0) and b[10] == 0xff and b[11] == 0xff) {
        return isNonGlobalIpv4(b[12..16].*);
    }

    // Transition addresses carry an IPv4 destination and must inherit its
    // classification instead of treating the outer IPv6 prefix as public.
    if (b[0] == 0 and b[1] == 0x64 and b[2] == 0xff and b[3] == 0x9b and
        std.mem.allEqual(u8, b[4..12], 0))
    {
        return isNonGlobalIpv4(b[12..16].*);
    }
    if (b[0] == 0x20 and b[1] == 0x02) return isNonGlobalIpv4(b[2..6].*);

    // Conservatively reject space outside global unicast (2000::/3). This
    // includes the RFC 8215 local-use NAT64 prefix 64:ff9b:1::/48. Then reject
    // special-use sub-ranges that sit inside global unicast, including Teredo:
    // its outer address is global-looking but embeds an IPv4 destination.
    if ((b[0] & 0xe0) != 0x20) return true;
    return (b[0] == 0x20 and b[1] == 0x01 and b[2] == 0 and b[3] == 0) or // Teredo
        (b[0] == 0x20 and b[1] == 0x01 and b[2] == 0 and b[3] == 0x02 and b[4] == 0 and b[5] == 0) or // benchmarking
        (b[0] == 0x20 and b[1] == 0x01 and b[2] == 0 and (b[3] & 0xf0) == 0x10) or // ORCHID
        (b[0] == 0x20 and b[1] == 0x01 and b[2] == 0 and (b[3] & 0xf0) == 0x20) or // ORCHIDv2
        (b[0] == 0x20 and b[1] == 0x01 and b[2] == 0x0d and b[3] == 0xb8) or // documentation
        (b[0] == 0x3f and b[1] == 0xfe) or // deprecated 6bone
        (b[0] == 0x3f and b[1] == 0xff and (b[2] & 0xf0) == 0); // documentation
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

test "HTTP client applies address response and redirect boundaries" {
    const security = ContentSecurityConfig{
        .block_private_ips = true,
        .max_download_size_bytes = 4,
    };
    const config = httpClientConfig(&security);
    try std.testing.expect(config.address_filter != null);
    try std.testing.expect(!config.keep_alive);
    try std.testing.expect(!config.cookies_enabled);
    try std.testing.expect(!config.redirect_policy.follow_redirects);
    try std.testing.expectEqual(@as(usize, 4), config.max_response_size);

    const global = try std.Io.net.IpAddress.parse("8.8.8.8", 443);
    const private = try std.Io.net.IpAddress.parse("127.0.0.1", 80);
    try std.testing.expect(config.address_filter.?.accepts(global));
    try std.testing.expect(!config.address_filter.?.accepts(private));
}

test "download context timeout cannot lengthen configured deadline" {
    const security = ContentSecurityConfig{ .download_timeout_seconds = 7 };
    try std.testing.expectEqual(
        @as(u64, 1234),
        effectiveDownloadTimeoutMs(.{ .io = std.testing.io, .timeout_ms = 1234 }, &security),
    );
    try std.testing.expectEqual(
        @as(u64, 7000),
        effectiveDownloadTimeoutMs(.{ .io = std.testing.io, .timeout_ms = 30_000 }, &security),
    );
    try std.testing.expectEqual(
        @as(u64, 7000),
        effectiveDownloadTimeoutMs(.{ .io = std.testing.io, .timeout_ms = 0 }, &security),
    );
    try std.testing.expectEqual(
        @as(u64, 7000),
        effectiveDownloadTimeoutMs(.{ .io = std.testing.io }, &security),
    );
    try std.testing.expectEqual(
        default_download_timeout_ms,
        effectiveDownloadTimeoutMs(.{ .io = std.testing.io }, null),
    );
    try std.testing.expectEqual(
        @as(u64, 1234),
        effectiveDownloadTimeoutMs(
            .{ .io = std.testing.io, .timeout_ms = 1234 },
            &.{ .download_timeout_seconds = 0 },
        ),
    );
}

test "download ceiling reports the remaining total operation timeout" {
    try std.testing.expectEqual(@as(u64, 0), try remainingDownloadTimeoutMs(0, std.time.ns_per_s));
    try std.testing.expectEqual(@as(u64, 1), try remainingDownloadTimeoutMs(100, 99 * std.time.ns_per_ms + 1));
    try std.testing.expectError(error.Timeout, remainingDownloadTimeoutMs(100, 100 * std.time.ns_per_ms));
}

test "download content parses data uri" {
    const alloc = std.testing.allocator;
    var downloaded = try downloadContentAlloc(alloc, "data:text/plain;base64,aGVsbG8=", null, null);
    defer downloaded.deinit(alloc);
    try std.testing.expectEqualStrings("text/plain", downloaded.content_type);
    try std.testing.expectEqualStrings("hello", downloaded.data);
}

test "contextual data URI and URI scheme dispatch are case insensitive" {
    const alloc = std.testing.allocator;
    var downloaded = try downloadContentAllocWithContext(
        alloc,
        .{ .io = std.testing.io, .timeout_ms = 1000 },
        "DATA:text/plain;base64,aGVsbG8=",
        null,
        null,
    );
    defer downloaded.deinit(alloc);
    try std.testing.expectEqualStrings("hello", downloaded.data);

    const allowed_hosts = [_][]u8{@constCast("allowed.example")};
    try std.testing.expectError(
        error.HostNotAllowed,
        downloadContentAlloc(
            alloc,
            "HTTPS://blocked.example/content",
            &.{ .allowed_hosts = &allowed_hosts },
            null,
        ),
    );
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

test "download size defaults apply when content security omits the field" {
    const security = ContentSecurityConfig{};
    try std.testing.expectEqual(default_max_download_size_bytes, maxDownloadSizeBytes(null));
    try std.testing.expectEqual(default_max_download_size_bytes, maxDownloadSizeBytes(&security));
    try std.testing.expectError(error.StreamTooLong, validateDownloadSize(default_max_download_size_bytes + 1, &security));
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

    const paths = [_][]u8{abs_path};
    const security = ContentSecurityConfig{ .allowed_paths = &paths };
    var downloaded = try downloadContentAlloc(alloc, uri, &security, null);
    defer downloaded.deinit(alloc);
    try std.testing.expectEqualStrings("image/png", downloaded.content_type);
    try std.testing.expectEqualStrings("png-bytes", downloaded.data);
}

test "file downloads require explicit paths before opening a target" {
    const alloc = std.testing.allocator;
    const uri = "file:///path/that/does/not/exist";
    const omitted = ContentSecurityConfig{ .block_private_ips = true };
    const empty = ContentSecurityConfig{ .allowed_paths = &.{} };
    try std.testing.expectError(error.PathNotAllowed, downloadContentAlloc(alloc, uri, null, null));
    try std.testing.expectError(error.PathNotAllowed, downloadContentAlloc(alloc, uri, &omitted, null));
    try std.testing.expectError(error.PathNotAllowed, downloadContentAlloc(alloc, uri, &empty, null));
    try std.testing.expectError(error.PathNotAllowed, downloadContentOutcomeAllocWithContext(alloc, .{ .io = std.testing.io }, uri, &omitted, null));
    const deadline_context = DownloadContext{ .io = std.testing.io, .timeout_ms = 1 };
    try std.testing.expectError(error.PathNotAllowed, downloadContentOutcomeAllocWithContext(alloc, deadline_context, uri, null, null));
    try std.testing.expectError(error.PathNotAllowed, downloadContentOutcomeAllocWithContext(alloc, deadline_context, uri, &omitted, null));
    try std.testing.expectError(error.PathNotAllowed, downloadContentOutcomeAllocWithContext(alloc, deadline_context, uri, &empty, null));
}

test "deadline-bound file downloads fail closed when file IO cannot enforce timeouts" {
    const context = DownloadContext{ .io = std.testing.io, .timeout_ms = 1 };
    const paths = [_][]u8{@constCast("/not-opened-because-timeout-is-unsupported.txt")};
    const security = ContentSecurityConfig{ .allowed_paths = &paths };
    try std.testing.expectError(
        error.FileTimeoutUnsupported,
        downloadContentOutcomeAllocWithContext(
            std.testing.allocator,
            context,
            "file:///not-opened-because-timeout-is-unsupported.txt",
            &security,
            null,
        ),
    );
}

test "caller-owned IO preserves file downloads without a request deadline" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "context.txt", .data = "bounded by size" });
    const path = try tmp.dir.realPathFileAlloc(std.testing.io, "context.txt", alloc);
    defer alloc.free(path);

    const paths = [_][]u8{path};
    const security = ContentSecurityConfig{ .allowed_paths = &paths };
    var downloaded = try downloadFileAllocWithContext(
        alloc,
        .{ .io = std.testing.io },
        path,
        &security,
    );
    defer downloaded.deinit(alloc);
    try std.testing.expectEqualStrings("bounded by size", downloaded.data);
}

test "file download accepts an exact cap and rejects cap plus one" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "exact.txt", .data = "abcd" });
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "over.txt", .data = "abcde" });

    const exact = try tmp.dir.realPathFileAlloc(std.testing.io, "exact.txt", alloc);
    defer alloc.free(exact);
    const over = try tmp.dir.realPathFileAlloc(std.testing.io, "over.txt", alloc);
    defer alloc.free(over);
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", alloc);
    defer alloc.free(root);
    const paths = [_][]u8{root};
    const security = ContentSecurityConfig{ .max_download_size_bytes = 4, .allowed_paths = &paths };

    var downloaded = try downloadFileAlloc(alloc, exact, &security);
    defer downloaded.deinit(alloc);
    try std.testing.expectEqualStrings("abcd", downloaded.data);
    try std.testing.expectError(error.StreamTooLong, downloadFileAlloc(alloc, over, &security));
}

test "file path security requires canonical component containment" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.createDirPath(std.testing.io, "allowed");
    try tmp.dir.createDirPath(std.testing.io, "allowed-sibling");
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "allowed/child.txt", .data = "allowed" });
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "allowed-sibling/child.txt", .data = "blocked" });

    const allowed_root = try tmp.dir.realPathFileAlloc(std.testing.io, "allowed", alloc);
    defer alloc.free(allowed_root);
    const allowed_child = try tmp.dir.realPathFileAlloc(std.testing.io, "allowed/child.txt", alloc);
    defer alloc.free(allowed_child);
    const sibling_child = try tmp.dir.realPathFileAlloc(std.testing.io, "allowed-sibling/child.txt", alloc);
    defer alloc.free(sibling_child);
    const allowed_paths = [_][]u8{allowed_root};
    const security = ContentSecurityConfig{ .allowed_paths = &allowed_paths };

    var downloaded = try downloadFileAlloc(alloc, allowed_child, &security);
    defer downloaded.deinit(alloc);
    try std.testing.expectEqualStrings("allowed", downloaded.data);
    try std.testing.expectError(error.PathNotAllowed, downloadFileAlloc(alloc, sibling_child, &security));
}

test "file path security accepts relative roots and rejects outside paths before lookup" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.createDirPath(std.testing.io, "allowed");
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "allowed/child.txt", .data = "allowed" });

    const relative_root = try std.fs.path.join(alloc, &.{ ".zig-cache", "tmp", tmp.sub_path[0..], "allowed" });
    defer alloc.free(relative_root);
    const absolute_child = try tmp.dir.realPathFileAlloc(std.testing.io, "allowed/child.txt", alloc);
    defer alloc.free(absolute_child);
    const allowed_paths = [_][]u8{relative_root};
    const security = ContentSecurityConfig{ .allowed_paths = &allowed_paths };

    var downloaded = try downloadFileAlloc(alloc, absolute_child, &security);
    defer downloaded.deinit(alloc);
    try std.testing.expectEqualStrings("allowed", downloaded.data);

    const nonexistent_allowed = [_][]u8{@constCast("/definitely/not/an/antfly/allowed/root")};
    try std.testing.expectError(error.PathNotAllowed, validateFilePathSecurityBeforeOpen(
        alloc,
        std.testing.io,
        "/definitely/not/an/antfly/outside/file",
        &.{ .allowed_paths = &nonexistent_allowed },
    ));
}

test "file path security rejects symlink escape" {
    const builtin = @import("builtin");
    if (builtin.os.tag == .windows or builtin.os.tag == .wasi or builtin.os.tag == .freestanding) return error.SkipZigTest;

    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.createDirPath(std.testing.io, "allowed");
    try tmp.dir.createDirPath(std.testing.io, "outside");
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "outside/secret.txt", .data = "secret" });
    try tmp.dir.symLink(std.testing.io, "../outside/secret.txt", "allowed/link.txt", .{});

    const allowed_root = try tmp.dir.realPathFileAlloc(std.testing.io, "allowed", alloc);
    defer alloc.free(allowed_root);
    const link_path = try std.fs.path.join(alloc, &.{ allowed_root, "link.txt" });
    defer alloc.free(link_path);
    const allowed_paths = [_][]u8{allowed_root};
    const security = ContentSecurityConfig{ .allowed_paths = &allowed_paths };

    try std.testing.expectError(error.PathNotAllowed, downloadFileAlloc(alloc, link_path, &security));
}

test "explicit empty allowlists deny remote content without probing targets" {
    const allowed_hosts = [_][]u8{};
    const allowed_paths = [_][]u8{};
    const security = ContentSecurityConfig{
        .allowed_hosts = &allowed_hosts,
        .allowed_paths = &allowed_paths,
    };

    try std.testing.expectError(error.HostNotAllowed, downloadContentAlloc(std.testing.allocator, "https://does-not-exist.invalid/image.png", &security, null));
    try std.testing.expectError(error.PathNotAllowed, downloadFileAlloc(std.testing.allocator, "/path/that/does/not/exist", &security));
    try std.testing.expectError(error.PathNotAllowed, validateS3PathSecurity("bucket/object.png", &security));
}

test "s3 path security requires component containment" {
    const allowed_paths = [_][]u8{@constCast("bucket/prefix")};
    const security = ContentSecurityConfig{ .allowed_paths = &allowed_paths };

    try validateS3PathSecurity("bucket/prefix/object.png", &security);
    try std.testing.expectError(error.PathNotAllowed, validateS3PathSecurity("bucket/prefix-sibling/object.png", &security));
}

test "S3 transport boundary errors keep scraper semantics" {
    try std.testing.expectEqual(error.StreamTooLong, normalizeS3DownloadError(error.ResponseTooLarge));
    try std.testing.expectEqual(error.PrivateIpBlocked, normalizeS3DownloadError(error.AddressRejected));
    try std.testing.expectEqual(error.Timeout, normalizeS3DownloadError(error.Timeout));
}

test "s3 locations always use the configured endpoint" {
    const alloc = std.testing.allocator;

    const bucket_uri = try std.Uri.parse("s3://attacker.example/bucket/object.png");
    const bucket, const key = try parseS3LocationAlloc(alloc, bucket_uri, "s3.internal.example", true);
    defer alloc.free(bucket);
    defer alloc.free(key);
    try std.testing.expectEqualStrings("attacker.example", bucket);
    try std.testing.expectEqualStrings("bucket/object.png", key);

    const endpoint_uri = try std.Uri.parse("s3://s3.internal.example/bucket/object.png");
    const endpoint_bucket, const endpoint_key = try parseS3LocationAlloc(alloc, endpoint_uri, "S3.INTERNAL.EXAMPLE", true);
    defer alloc.free(endpoint_bucket);
    defer alloc.free(endpoint_key);
    try std.testing.expectEqualStrings("bucket", endpoint_bucket);
    try std.testing.expectEqualStrings("object.png", endpoint_key);

    var resolved = try objectstore.resolveS3EndpointAlloc(alloc, "http://LOCALHOST:9000", true);
    defer resolved.deinit(alloc);
    const port_uri = try std.Uri.parse("s3://localhost:9000/bucket/object.png");
    const port_bucket, const port_key = try parseS3LocationAlloc(alloc, port_uri, resolved.endpoint, resolved.use_ssl);
    defer alloc.free(port_bucket);
    defer alloc.free(port_key);
    try std.testing.expectEqualStrings("LOCALHOST:9000", resolved.endpoint);
    try std.testing.expect(!resolved.use_ssl);
    try std.testing.expectEqualStrings("bucket", port_bucket);
    try std.testing.expectEqualStrings("object.png", port_key);

    const canonical_endpoint_bucket = try s3BucketAlloc(
        alloc,
        endpoint_uri,
        "https://s3.internal.example",
        null,
    );
    defer alloc.free(canonical_endpoint_bucket);
    try std.testing.expectEqualStrings("bucket", canonical_endpoint_bucket);

    const canonical_bucket_style = try s3BucketAlloc(
        alloc,
        bucket_uri,
        "https://s3.internal.example",
        null,
    );
    defer alloc.free(canonical_bucket_style);
    try std.testing.expectEqualStrings("attacker.example", canonical_bucket_style);
}

test "s3 credential construction validates and owns every field" {
    const alloc = std.testing.allocator;
    try std.testing.expectError(error.MissingAccessKeyId, allocS3Credentials(alloc, "s3.example.com", true, &.{}));
    try std.testing.expectError(error.MissingSecretAccessKey, allocS3Credentials(alloc, "s3.example.com", true, &.{
        .access_key_id = @constCast("key"),
    }));

    var creds = try allocS3Credentials(alloc, "s3.example.com", false, &.{
        .access_key_id = @constCast("key"),
        .secret_access_key = @constCast("secret"),
        .session_token = @constCast("token"),
    });
    defer creds.deinit(alloc);
    try std.testing.expectEqualStrings("s3.example.com", creds.endpoint);
    try std.testing.expect(!creds.use_ssl);
    try std.testing.expectEqualStrings("key", creds.access_key_id);
    try std.testing.expectEqualStrings("secret", creds.secret_access_key);
    try std.testing.expectEqualStrings("token", creds.session_token.?);
}

test "download content blocks disallowed hosts" {
    const alloc = std.testing.allocator;
    const allowed_hosts = [_][]u8{@constCast("cdn.example.com")};
    try std.testing.expectError(error.HostNotAllowed, downloadContentAlloc(alloc, "https://example.com/a.png", &.{
        .allowed_hosts = &allowed_hosts,
    }, null));
}

test "safe HTTP policy accepts allowlisted DNS for connection-time vetting" {
    const allowed_hosts = [_][]u8{@constCast("cdn.example.com")};
    const uri = try std.Uri.parse("https://cdn.example.com/image.png");
    const security = ContentSecurityConfig{
        .allowed_hosts = &allowed_hosts,
        .block_private_ips = true,
    };
    try validateUrlSecurity(uri, &security);
    try std.testing.expect(httpClientConfig(&security).address_filter != null);
}

test "safe HTTP policy rejects private DNS at the connection boundary" {
    const allowed_hosts = [_][]u8{@constCast("localhost")};
    try std.testing.expectError(
        error.PrivateIpBlocked,
        downloadContentAllocWithContext(
            std.testing.allocator,
            .{ .io = std.testing.io, .timeout_ms = 1000 },
            "http://localhost/image.png",
            &.{ .allowed_hosts = &allowed_hosts, .block_private_ips = true },
            null,
        ),
    );
}

test "explicit private-IP opt out permits an allowlisted hostname" {
    const allowed_hosts = [_][]u8{@constCast("cdn.example.com")};
    const uri = try std.Uri.parse("https://cdn.example.com/image.png");
    try validateUrlSecurity(uri, &.{
        .allowed_hosts = &allowed_hosts,
        .block_private_ips = false,
    });
}

test "safe HTTP policy permits an allowlisted global IP literal" {
    const allowed_hosts = [_][]u8{@constCast("8.8.8.8")};
    const uri = try std.Uri.parse("https://8.8.8.8/image.png");
    try validateUrlSecurity(uri, &.{
        .allowed_hosts = &allowed_hosts,
        .block_private_ips = true,
    });
}

test "safe HTTP policy rejects an allowlisted private IP literal" {
    const allowed_hosts = [_][]u8{@constCast("127.0.0.1")};
    const uri = try std.Uri.parse("http://127.0.0.1/image.png");
    try std.testing.expectError(error.PrivateIpBlocked, validateUrlSecurity(uri, &.{
        .allowed_hosts = &allowed_hosts,
        .block_private_ips = true,
    }));
}

test "download content blocks private ip literals" {
    const alloc = std.testing.allocator;
    try std.testing.expectError(error.PrivateIpBlocked, downloadContentAlloc(alloc, "http://127.0.0.1/test.png", &.{
        .block_private_ips = true,
    }, null));
}

test "content security defaults to blocking non-global literal hosts" {
    const security = ContentSecurityConfig{};
    const loopback = try std.Uri.parse("http://127.0.0.1/test.png");
    try std.testing.expectError(error.PrivateIpBlocked, validateUrlSecurity(loopback, &security));
    const ipv6_loopback = try std.Uri.parse("http://[::1]/test.png");
    try std.testing.expectError(error.PrivateIpBlocked, validateUrlSecurity(ipv6_loopback, &security));
    const scoped_link_local = try std.Uri.parse("http://[fe80::1%25en0]/test.png");
    try std.testing.expectError(error.PrivateIpBlocked, validateUrlSecurity(scoped_link_local, &security));

    const blocked = [_][]const u8{
        "localhost",
        "api.localhost.",
        "name.LOCAL",
        "2130706433",
        "127.1",
        "0177.0.0.1",
        "0x7f000001",
        "0x7f.0.0.1",
        "0.1.2.3",
        "10.1.2.3",
        "100.64.0.1",
        "100.127.255.254",
        "127.0.0.1",
        "169.254.1.2",
        "172.31.255.254",
        "192.0.0.1",
        "192.0.2.1",
        "192.168.1.2",
        "192.88.99.1",
        "198.18.0.1",
        "198.19.255.254",
        "198.51.100.1",
        "203.0.113.1",
        "224.0.0.1",
        "239.255.255.250",
        "240.0.0.1",
        "255.255.255.255",
        "::",
        "::1",
        "::ffff:127.0.0.1",
        "::ffff:100.64.0.1",
        "64:ff9b::7f00:1",
        "64:ff9b:1::1",
        "100::1",
        "2001:0:4136:e378:8000:63bf:3fff:fdd2",
        "2001:2::1",
        "2001:10::1",
        "2001:20::1",
        "2001:db8::1",
        "2002:7f00:1::",
        "3ffe::1",
        "3fff::1",
        "5f00::1",
        "fc00::1",
        "fe80::1",
        "fe80::1%en0",
        "fec0::1",
        "ff02::1",
    };
    for (blocked) |host| {
        try std.testing.expect(isNonGlobalHost(host));
    }

    const global = [_][]const u8{
        "8.8.8.8",
        "93.184.216.34",
        "100.128.0.1",
        "2001:4860:4860::8888",
        "2002:0808:0808::1",
        "64:ff9b::808:808",
        "::ffff:8.8.8.8",
        "123.example.com",
    };
    for (global) |host| {
        try std.testing.expect(!isNonGlobalHost(host));
    }
}

test "configured NAT64 prefixes classify the embedded IPv4 destination" {
    const prefixes = [_][]u8{@constCast("2001:470:64::/96")};
    const security = ContentSecurityConfig{ .nat64_prefixes = &prefixes };
    try validateNat64Prefixes(security);

    const private = try std.Io.net.IpAddress.parse("2001:470:64::a9fe:a9fe", 80);
    const public = try std.Io.net.IpAddress.parse("2001:470:64::808:808", 80);
    try std.testing.expect(!allowGlobalAddress(@ptrCast(&security), private));
    try std.testing.expect(allowGlobalAddress(@ptrCast(&security), public));
}

test "NAT64 prefix validation rejects unsupported and non-canonical prefixes" {
    const invalid_length = [_][]u8{@constCast("2001:470::/72")};
    try std.testing.expectError(error.InvalidNat64Prefix, validateNat64Prefixes(.{ .nat64_prefixes = &invalid_length }));
    const non_canonical = [_][]u8{@constCast("2001:470::1/96")};
    try std.testing.expectError(error.InvalidNat64Prefix, validateNat64Prefixes(.{ .nat64_prefixes = &non_canonical }));
}

test "redirect resolution and credential capabilities are origin and path aware" {
    const alloc = std.testing.allocator;
    const base = try std.Uri.parse("https://cdn.example.com/a/b/document.pdf?old=1");
    const relative = try resolveRedirectUrlAlloc(alloc, base, "../images/page.png?size=2#ignored");
    defer alloc.free(relative);
    try std.testing.expectEqualStrings("https://cdn.example.com/a/images/page.png?size=2", relative);

    try std.testing.expect(httpUrlMatchesBase(try std.Uri.parse(relative), "https://cdn.example.com/a"));
    try std.testing.expect(!httpUrlMatchesBase(try std.Uri.parse("https://cdn.example.com/admin"), "https://cdn.example.com/a"));
    try std.testing.expect(!httpUrlMatchesBase(try std.Uri.parse("https://cdn.example.com/a/%2e%2e/admin"), "https://cdn.example.com/a"));
    try std.testing.expect(!sameOrigin(base, try std.Uri.parse("https://assets.example.net/page.png")));
}

test "HTTP response body ownership transfers without a copy" {
    const alloc = std.testing.allocator;
    var response = httpx.Response.init(alloc, 200);
    response.body = try alloc.dupe(u8, "owned-body");
    response.body_owned = true;
    defer response.deinit();

    const original = response.body.?.ptr;
    const taken = try takeHttpResponseBodyAlloc(alloc, &response);
    defer alloc.free(taken);
    try std.testing.expectEqual(original, taken.ptr);
    try std.testing.expect(response.body == null);
}

const TestHttpResponseServer = struct {
    io: std.Io,
    server: *std.Io.net.Server,
    body: []const u8,
    content_encoding: ?[]const u8,
    status_code: u16 = 200,
    extra_headers: []const u8 = "",
    expected_request_line: ?[]const u8 = null,
    expected_authorization: ?[]const u8 = null,
    failure: ?anyerror = null,

    fn serve(self: *@This()) void {
        var stream = self.server.accept(self.io) catch |err| {
            self.failure = err;
            return;
        };
        defer stream.close(self.io);

        // Drain the GET headers before closing the connection; unread request
        // bytes can otherwise turn this response into a TCP reset.
        var read_buffer: [4096]u8 = undefined;
        var reader = stream.reader(self.io, &read_buffer);
        var first = true;
        var authorized = self.expected_authorization == null;
        while (true) {
            const line = reader.interface.takeDelimiterInclusive('\n') catch |err| {
                self.failure = err;
                return;
            };
            if (first) {
                if (self.expected_request_line) |expected| if (!std.mem.eql(u8, line, expected)) {
                    self.failure = error.UnexpectedTestRequest;
                    return;
                };
                first = false;
            }
            if (line.len >= 14 and std.ascii.eqlIgnoreCase(line[0..14], "Authorization:")) {
                if (self.expected_authorization) |expected| authorized = std.mem.eql(u8, std.mem.trim(u8, line[14..], " \r\n"), expected);
            }
            if (std.mem.eql(u8, line, "\r\n")) break;
        }
        if (!authorized) {
            self.failure = error.MissingTestAuthorization;
            return;
        }

        var write_buffer: [1024]u8 = undefined;
        var writer = stream.writer(self.io, &write_buffer);
        writer.interface.print(
            "HTTP/1.1 {d} OK\r\nContent-Type: text/plain; charset=utf-8\r\nConnection: close\r\n{s}",
            .{ self.status_code, self.extra_headers },
        ) catch |err| {
            self.failure = err;
            return;
        };
        if (self.content_encoding) |encoding| {
            writer.interface.print("Content-Encoding: {s}\r\n", .{encoding}) catch |err| {
                self.failure = err;
                return;
            };
        }
        writer.interface.print("Content-Length: {d}\r\n\r\n", .{self.body.len}) catch |err| {
            self.failure = err;
            return;
        };
        writer.interface.writeAll(self.body) catch |err| {
            self.failure = err;
            return;
        };
        writer.interface.flush() catch |err| {
            self.failure = err;
        };
    }
};

fn downloadTestHttpResponseAlloc(
    alloc: Allocator,
    body: []const u8,
    content_encoding: ?[]const u8,
    max_download_size_bytes: u64,
) !DownloadedContent {
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    const listen_addr = try std.Io.net.IpAddress.parse("127.0.0.1", 0);
    var server = try listen_addr.listen(io, .{});
    var server_open = true;
    defer if (server_open) server.deinit(io);

    var fixture = TestHttpResponseServer{
        .io = io,
        .server = &server,
        .body = body,
        .content_encoding = content_encoding,
    };
    const uri = try std.fmt.allocPrint(alloc, "http://{f}/blob", .{server.socket.address});
    defer alloc.free(uri);
    var thread = try std.testing.io.concurrent(TestHttpResponseServer.serve, .{&fixture});
    var security = ContentSecurityConfig{
        .block_private_ips = false,
        .max_download_size_bytes = max_download_size_bytes,
    };
    var downloaded = downloadContentAlloc(alloc, uri, &security, null) catch |err| {
        server.deinit(io);
        server_open = false;
        thread.await(std.testing.io);
        return err;
    };
    thread.await(std.testing.io);
    if (fixture.failure) |server_err| {
        downloaded.deinit(alloc);
        return server_err;
    }
    return downloaded;
}

test "HTTP download returns identical decoded bytes for supported content encodings" {
    const alloc = std.testing.allocator;
    const expected = "GitHub compressed content stays readable.\n";
    const gzip = [_]u8{ 31, 139, 8, 0, 0, 0, 0, 0, 2, 255, 115, 207, 44, 241, 40, 77, 82, 72, 206, 207, 45, 40, 74, 45, 46, 78, 77, 1, 50, 243, 74, 82, 243, 74, 20, 138, 75, 18, 43, 139, 21, 138, 82, 19, 83, 18, 147, 114, 82, 245, 184, 0, 187, 214, 11, 13, 42, 0, 0, 0 };
    const deflate = [_]u8{ 120, 156, 115, 207, 44, 241, 40, 77, 82, 72, 206, 207, 45, 40, 74, 45, 46, 78, 77, 1, 50, 243, 74, 82, 243, 74, 20, 138, 75, 18, 43, 139, 21, 138, 82, 19, 83, 18, 147, 114, 82, 245, 184, 0, 87, 78, 15, 144 };
    const deflate_raw = deflate[2 .. deflate.len - 4];
    const cases = [_]struct {
        encoding: ?[]const u8,
        body: []const u8,
    }{
        .{ .encoding = null, .body = expected },
        .{ .encoding = "gzip", .body = &gzip },
        .{ .encoding = "deflate", .body = &deflate },
        .{ .encoding = "deflate", .body = deflate_raw },
    };

    for (cases) |case| {
        var downloaded = try downloadTestHttpResponseAlloc(alloc, case.body, case.encoding, 1024);
        defer downloaded.deinit(alloc);
        try std.testing.expectEqualStrings("text/plain", downloaded.content_type);
        try std.testing.expectEqualStrings(expected, downloaded.data);
    }
}

test "HTTP download enforces its size limit on decoded bytes" {
    const alloc = std.testing.allocator;
    const gzip = [_]u8{ 31, 139, 8, 0, 0, 0, 0, 0, 2, 255, 115, 207, 44, 241, 40, 77, 82, 72, 206, 207, 45, 40, 74, 45, 46, 78, 77, 1, 50, 243, 74, 82, 243, 74, 20, 138, 75, 18, 43, 139, 21, 138, 82, 19, 83, 18, 147, 114, 82, 245, 184, 0, 187, 214, 11, 13, 42, 0, 0, 0 };
    try std.testing.expectError(
        error.StreamTooLong,
        downloadTestHttpResponseAlloc(alloc, &gzip, "gzip", 41),
    );
}

test "pinned remote ranges enforce immutable length and version without whole object fallback" {
    const a = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(a, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    const cases = [_]struct { status: u16, headers: []const u8, expected: ?anyerror }{
        .{ .status = 206, .headers = "ETag: \"v1\"\r\nContent-Range: bytes 2-5/10\r\n", .expected = null },
        .{ .status = 206, .headers = "ETag: \"v2\"\r\nContent-Range: bytes 2-5/10\r\n", .expected = error.SourceVersionChanged },
        .{ .status = 206, .headers = "ETag: \"v1\"\r\nContent-Range: bytes 2-5/11\r\n", .expected = error.InvalidRemoteRange },
        .{ .status = 200, .headers = "ETag: \"v1\"\r\n", .expected = error.InvalidRemoteRange },
        .{ .status = 412, .headers = "", .expected = error.SourceVersionChanged },
    };
    for (cases) |case| {
        var server = try (try std.Io.net.IpAddress.parse("127.0.0.1", 0)).listen(io, .{});
        var open = true;
        defer if (open) server.deinit(io);
        var fixture = TestHttpResponseServer{ .io = io, .server = &server, .body = "2345", .content_encoding = null, .status_code = case.status, .extra_headers = case.headers };
        const uri = try std.fmt.allocPrint(a, "http://{f}/blob", .{server.socket.address});
        defer a.free(uri);
        var security = ContentSecurityConfig{ .block_private_ips = false };
        const range = try PinnedRange.create(a, .{ .io = io }, uri, "\"v1\"", .strong_etag, 10, &security, null, &.{});
        defer range.destroy();
        var thread = try std.testing.io.concurrent(TestHttpResponseServer.serve, .{&fixture});
        var out: [4]u8 = undefined;
        const result = range.readAt(2, &out);
        server.deinit(io);
        open = false;
        thread.await(std.testing.io);
        if (fixture.failure) |err| return err;
        if (case.expected) |err| try std.testing.expectError(err, result) else {
            try std.testing.expectEqual(@as(usize, 4), try result);
            try std.testing.expectEqualStrings("2345", &out);
        }
    }
}

test "pinned source reference validation is shared and requires no allocator or transport" {
    const Case = struct {
        uri: []const u8 = "https://example.org/clip.mp4",
        version: []const u8 = "\"v1\"",
        kind: PinnedRange.VersionKind = .strong_etag,
        length: u64 = 1024,
        expected: anyerror,
    };
    const long_version: [4097]u8 = @splat('x');
    const long_uri: [16 * 1024 + 1]u8 = @splat('x');
    const cases = [_]Case{
        .{ .version = "", .expected = error.InvalidSourceReference },
        .{ .version = "v1", .expected = error.ImmutableVersionRequired },
        .{ .version = "W/\"v1\"", .expected = error.ImmutableVersionRequired },
        .{ .version = "\"bad\"etag\"", .expected = error.ImmutableVersionRequired },
        .{ .version = "\"bad\netag\"", .expected = error.ImmutableVersionRequired },
        .{ .version = &long_version, .expected = error.InvalidSourceReference },
        .{ .uri = &long_uri, .expected = error.InvalidSourceReference },
        .{ .length = 0, .expected = error.InvalidSourceReference },
        .{ .kind = .s3_version_id, .expected = error.InvalidSourceReference },
        .{ .uri = "https://user:password@example.org/clip.mp4", .expected = error.InvalidSourceReference },
        .{ .uri = "https://example.org/clip.mp4#page=2", .expected = error.InvalidSourceReference },
        .{ .uri = "data:video/mp4;base64,AQID", .expected = error.UnsupportedUrlScheme },
        .{ .uri = "gs://bucket/clip.mp4", .expected = error.UnsupportedUrlScheme },
        .{ .uri = "s3://bucket/clip.mp4", .version = "null", .kind = .s3_version_id, .expected = error.ImmutableVersionRequired },
        .{ .uri = "s3://bucket", .version = "v1", .kind = .s3_version_id, .expected = error.InvalidS3Url },
    };
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    for (cases) |case| {
        try std.testing.expectError(case.expected, PinnedRange.validateReference(case.uri, case.version, case.kind, case.length));
        // Runtime rejects the same contract before allocating or constructing
        // an HTTP/S3 client; no destination credentials are required here.
        try std.testing.expectError(case.expected, PinnedRange.create(failing.allocator(), .{ .io = std.testing.io }, case.uri, case.version, case.kind, case.length, null, null, &.{}));
    }
    _ = try PinnedRange.validateReference("https://example.org/clip.mp4", "\"v1\"", .strong_etag, 1024);
    _ = try PinnedRange.validateReference("http://example.org/clip.mp4", "\"\"", .strong_etag, 1024);
    _ = try PinnedRange.validateReference("s3://bucket/clip.mp4", "v1", .s3_version_id, 1024);
    _ = try PinnedRange.validateReference("s3://endpoint/bucket/clip.mp4", "\"etag\"", .strong_etag, 1024);
}

test "S3 URI structure is checked before allocation credentials or transport" {
    const a = std.testing.allocator;
    var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    for ([_][]const u8{ "s3://bucket", "s3:relative", "s3:///key", "s3://bucket/", "s3://bucket///" }) |text| {
        const uri = try std.Uri.parse(text);
        try std.testing.expectError(error.InvalidS3Url, validateS3Uri(uri, .{}));
        try std.testing.expectError(error.InvalidS3Url, downloadContentAlloc(failing.allocator(), text, null, null));
        try std.testing.expectError(error.InvalidS3Url, PinnedRange.validateReference(text, "v1", .s3_version_id, 1024));
    }
    try validateS3Uri(try std.Uri.parse("s3://bucket/"), .{ .complete = false });
    try std.testing.expectError(error.InvalidS3Url, validateS3Uri(try std.Uri.parse("s3:///key"), .{ .complete = false }));
    try validateS3Uri(try std.Uri.parse("s3://bucket/key"), .{});
    try validateS3Uri(try std.Uri.parse("S3://bucket/key"), .{});

    for ([_][]const u8{ "s3://objects.example.org/bucket", "s3://objects.example.org/bucket/" }) |text| {
        try std.testing.expectError(error.InvalidS3Url, s3BucketAlloc(a, try std.Uri.parse(text), "https://objects.example.org", true));
    }
    // A nonempty key ending in '/' is valid in either addressing style.
    for ([_][]const u8{ "s3://bucket/folder/", "s3://objects.example.org/bucket/folder/" }) |text| {
        const bucket = try s3BucketAlloc(a, try std.Uri.parse(text), "https://objects.example.org", true);
        defer a.free(bucket);
        try std.testing.expectEqualStrings("bucket", bucket);
    }
}

test "GCS locators retain encoded names and immutable generation selectors" {
    const a = std.testing.allocator;
    var location = try parseGcsLocationAlloc(a, "gs://bucket/objects/a%20b%2Fc.pdf?generation=1793305553268935");
    defer location.deinit(a);
    try std.testing.expectEqualStrings("bucket", location.bucket);
    try std.testing.expectEqualStrings("objects/a b/c.pdf", location.key);
    try std.testing.expectEqualStrings("1793305553268935", location.generation.?);
    for ([_][]const u8{
        "gs://bucket/objects/doc?generation=0",              "gs://bucket/objects/doc?generation=01",
        "gs://bucket/objects/doc?generation=1&generation=2", "gs://bucket/objects/doc?token=secret",
        "gs://user@bucket/objects/doc",                      "gs://bucket:443/objects/doc",
        "gs://bucket/objects/doc#fragment",                  "gs://bucket/objects/doc?generation=18446744073709551616",
        "gs://bucket/",                                      "gs://bucket/objects/%ZZ",
        "gs://bucket/objects/%0",                            "gs://bucket/objects/%00",
        "gs://bucket/objects/raw space",
    }) |uri| try std.testing.expectError(error.InvalidGcsUrl, parseGcsLocationAlloc(a, uri));
}

test "GCS scopes reject requests before identity or credentials are loaded" {
    const a = std.testing.allocator;
    const buckets = [_][]u8{@constCast("bucket")};
    const connection = GcsContentConnection{ .buckets = &buckets, .prefix = @constCast("objects/approved"), .credentials = .{ .source = .service_account, .credentials_path = @constCast("__missing_google_credentials__.json") } };
    for ([_][]const u8{ "gs://other-bucket/objects/approved/doc", "gs://bucket/objects/approved-elsewhere/doc", "gs://bucket/objects/unapproved/doc" }) |uri|
        try std.testing.expectError(error.GcsConnectionNotFoundOrOutOfScope, downloadGcsContentAlloc(a, null, uri, null, &connection));
    try std.testing.expectError(error.GcsConnectionNotFoundOrOutOfScope, downloadContentAlloc(a, "gs://bucket/objects/approved/doc", null, null));
}

test "GCS transport authenticates and verifies generation and byte bounds" {
    const a = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(a, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    const cases = [_]struct { max_bytes: u64, generation: []const u8, expected: ?anyerror }{
        .{ .max_bytes = 4, .generation = "123", .expected = null },
        .{ .max_bytes = 3, .generation = "123", .expected = error.StreamTooLong },
        .{ .max_bytes = 2, .generation = "123", .expected = error.StreamTooLong },
        .{ .max_bytes = 4, .generation = "124", .expected = error.PreconditionFailed },
    };
    for (cases) |case| {
        var server = try (try std.Io.net.IpAddress.parse("127.0.0.1", 0)).listen(io, .{});
        var server_open = true;
        defer if (server_open) server.deinit(io);
        const headers = try std.fmt.allocPrint(a, "x-goog-generation: {s}\r\n", .{case.generation});
        defer a.free(headers);
        var fixture = TestHttpResponseServer{ .io = io, .server = &server, .body = "PDF!", .content_encoding = null, .extra_headers = headers, .expected_request_line = "GET /storage/v1/b/bucket/o/objects%2Fdoc.pdf?generation=123&alt=media HTTP/1.1\r\n", .expected_authorization = "Bearer scoped-fixture-token" };
        const endpoint = try std.fmt.allocPrint(a, "http://{f}/storage/v1", .{server.socket.address});
        defer a.free(endpoint);
        const buckets = [_][]u8{@constCast("bucket")};
        const connection = GcsContentConnection{ .buckets = &buckets, .prefix = @constCast("objects"), .endpoint = endpoint, .credentials = .{ .source = .bearer_token, .bearer_token = @constCast("scoped-fixture-token") } };
        const security = ContentSecurityConfig{ .block_private_ips = false, .max_download_size_bytes = case.max_bytes };
        var thread = try std.testing.io.concurrent(TestHttpResponseServer.serve, .{&fixture});
        const result = downloadGcsContentAlloc(a, .{ .io = io }, "gs://bucket/objects/doc.pdf?generation=123", &security, &connection);
        server.deinit(io);
        server_open = false;
        thread.await(std.testing.io);
        if (fixture.failure) |err| return err;
        if (case.expected) |err| try std.testing.expectError(err, result) else {
            var downloaded = try result;
            defer downloaded.deinit(a);
            try std.testing.expectEqualStrings("PDF!", downloaded.data);
        }
    }
}

test "GCS canceled requests fail before credential discovery" {
    const a = std.testing.allocator;
    var canceled = std.atomic.Value(bool).init(true);
    const buckets = [_][]u8{@constCast("bucket")};
    const connection = GcsContentConnection{ .buckets = &buckets, .credentials = .{ .source = .service_account, .credentials_path = @constCast("__missing_google_credentials__.json") } };
    try std.testing.expectError(error.Canceled, downloadGcsContentAlloc(a, .{ .io = std.testing.io, .cancellation = CancellationToken.fromAtomic(&canceled) }, "gs://bucket/doc.pdf", null, &connection));
}

test "remote content snapshot owns borrowed GCS views and rolls back allocation failures" {
    const Harness = struct {
        fn run(alloc: Allocator) !void {
            const raw =
                \\{"security":{"max_download_size_bytes":1024},"gcs_connections":[{"name":"archive","connection":{"buckets":["originals"],"prefix":"objects/sha256","credentials":{"source":"bearer_token","bearer_token":"${secret:reader}"}}}]}
            ;
            var parsed = try std.json.parseFromSlice(RemoteContentSnapshotData, alloc, raw, .{ .allocate = .alloc_always });
            defer parsed.deinit();
            var config = try parsed.value.configAlloc(alloc);
            defer RemoteContentSnapshotData.deinitConfig(&config, alloc);
            try std.testing.expectEqual(@as(?u64, 1024), config.security.?.max_download_size_bytes);
            const connection = config.gcs_connections.get("archive").?;
            try std.testing.expectEqualStrings("${secret:reader}", connection.credentials.bearer_token.?);
            var location = try parseGcsLocationAlloc(alloc, "gs://originals/objects/sha256/doc.pdf?generation=123");
            defer location.deinit(alloc);
            try std.testing.expect(connection.allows(&location));
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{});
}

test "remote content snapshot carries S3 and HTTP credentials and rolls back allocation failures" {
    const Harness = struct {
        fn run(alloc: Allocator) !void {
            const raw =
                \\{"default_s3":"r2","s3":[{"name":"r2","credential":{"endpoint":"account.r2.cloudflarestorage.com","use_ssl":true,"buckets":["media"],"access_key_id":"${secret:r2-access}","secret_access_key":"${secret:r2-secret}"}}],"http":[{"name":"github","base_url":"https://api.github.com/repos/acme/","headers":[{"name":"Authorization","value":"${secret:github}"},{"name":"Accept","value":"application/vnd.github.raw+json"}],"security":{"allowed_hosts":["api.github.com"]}}]}
            ;
            var parsed = try std.json.parseFromSlice(RemoteContentSnapshotData, alloc, raw, .{ .allocate = .alloc_always });
            defer parsed.deinit();
            var config = try parsed.value.configAlloc(alloc);
            defer RemoteContentSnapshotData.deinitConfig(&config, alloc);
            try std.testing.expectEqualStrings("r2", config.default_s3.?);
            const s3 = config.getS3("r2").?;
            try std.testing.expectEqualStrings("account.r2.cloudflarestorage.com", s3.endpoint.?);
            try std.testing.expectEqualStrings("media", s3.buckets.?[0]);
            try std.testing.expectEqualStrings("${secret:r2-secret}", s3.secret_access_key.?);
            const http = config.getHttp("github").?;
            try std.testing.expectEqual(@as(usize, 2), http.headers.count());
            try std.testing.expectEqualStrings("${secret:github}", http.headers.get("Authorization").?);
            try std.testing.expectEqualStrings("api.github.com", http.security.?.allowed_hosts.?[0]);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{});
}

test "remote content snapshot rejects duplicate credential names" {
    const raw =
        \\{"s3":[{"name":"r2","credential":{}},{"name":"r2","credential":{}}]}
    ;
    var parsed = try std.json.parseFromSlice(RemoteContentSnapshotData, std.testing.allocator, raw, .{ .allocate = .alloc_always });
    defer parsed.deinit();
    try std.testing.expectError(error.InvalidRemoteContentConfig, parsed.value.configAlloc(std.testing.allocator));
}

/// Keep Retry-After as a duration; supervisors persist their own retry clock.
pub fn retryAfterMs(raw: ?[]const u8, now_ms: u64) ?u64 {
    const value = std.mem.trim(u8, raw orelse return null, " \t");
    if (value.len == 0) return null;
    if (std.fmt.parseUnsigned(u64, value, 10)) |seconds|
        return std.math.mul(u64, seconds, 1000) catch std.math.maxInt(u64)
    else |_| {}
    const timestamp = objectstore.retention_time.httpDate(value) orelse return null;
    return timestamp -| now_ms;
}

test "remote fetch retry hints retain delta seconds and validated HTTP dates" {
    try std.testing.expectEqual(@as(?u64, 3000), retryAfterMs("3", 0));
    try std.testing.expectEqual(@as(?u64, 1000), retryAfterMs("Thu, 01 Jan 1970 00:00:02 GMT", 1000));
    try std.testing.expectEqual(@as(?u64, 0), retryAfterMs("Thu, 01 Jan 1970 00:00:02 GMT", 3000));
    try std.testing.expectEqual(@as(?u64, null), retryAfterMs("garbage", 0));
    try std.testing.expectEqual(@as(?u64, null), retryAfterMs(null, 0));
}

test "HTTP downloader exposes transport quota exhaustion without retaining allocations" {
    const a = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    try std.testing.expectError(error.DownloadTransportMemoryExceeded, downloadContentAllocWithContext(a, .{ .io = io_impl.io(), .transport_memory_limit_bytes = 1 }, "https://example.org/recording.wav", &.{ .max_download_size_bytes = 4096 }, null));
}

test "object store and pinned range transports enforce the same quota" {
    const a = std.testing.allocator;
    const context = DownloadContext{ .io = std.testing.io, .transport_memory_limit_bytes = 1 };
    const security = ContentSecurityConfig{ .block_private_ips = false, .max_download_size_bytes = 4 };
    const credentials = S3CredentialsConfig{ .endpoint = @constCast("127.0.0.1:1"), .use_ssl = false, .access_key_id = @constCast("fixture"), .secret_access_key = @constCast("fixture") };
    try std.testing.expectError(error.DownloadTransportMemoryExceeded, downloadContentAllocWithContext(a, context, "s3://bucket/clip.wav", &security, &credentials));
    try std.testing.expectError(error.DownloadTransportMemoryExceeded, PinnedRange.create(a, context, "s3://bucket/clip.wav", "v1", .s3_version_id, 4, &security, &credentials, &.{}));
    const buckets = [_][]u8{@constCast("bucket")};
    const connection = GcsContentConnection{ .buckets = &buckets, .endpoint = @constCast("http://127.0.0.1:1/storage/v1"), .credentials = .{ .source = .bearer_token, .bearer_token = @constCast("fixture") } };
    try std.testing.expectError(error.DownloadTransportMemoryExceeded, downloadGcsContentAlloc(a, context, "gs://bucket/clip.wav", &security, &connection));
    const range = try PinnedRange.create(a, context, "http://127.0.0.1:1/clip.wav", "\"v1\"", .strong_etag, 4, &security, null, &.{});
    defer range.destroy();
    var bytes: [4]u8 = undefined;
    try std.testing.expectError(error.DownloadTransportMemoryExceeded, range.readAt(0, &bytes));
}

test "S3 downloads and pinned ranges detach results from scoped transport allocators" {
    const a = std.testing.allocator;
    var threaded = std.Io.Threaded.init(a, .{});
    defer threaded.deinit();
    const io = threaded.io();
    var upstream = try httpx.TestServer.start(a, io, &.{
        .{ .method = .HEAD, .path = "/bucket/clip.wav", .respond = .{ .body = "okay", .headers = &.{.{ .name = "ETag", .value = "\"v1\"" }} } },
        .{ .method = .GET, .path = "/bucket/clip.wav", .respond = .{ .status = 206, .body = "okay", .content_type = "audio/wav", .headers = &.{ .{ .name = "ETag", .value = "\"v1\"" }, .{ .name = "Content-Range", .value = "bytes 0-3/4" } } } },
    });
    defer upstream.deinit();
    const Serve = struct {
        fn run(server: *httpx.TestServer, failure: *?anyerror) std.Io.Cancelable!void {
            // HEAD+GET for the full read, one GET for the pinned range.
            for (0..3) |_| server.handleOne() catch |err| {
                failure.* = err;
                return;
            };
        }
    };
    var serving: std.Io.Group = .init;
    defer serving.cancel(io);
    var failure: ?anyerror = null;
    try serving.concurrent(io, Serve.run, .{ &upstream, &failure });
    const credentials = S3CredentialsConfig{ .endpoint = @constCast(upstream.baseUrl()), .use_ssl = false, .access_key_id = @constCast("fixture"), .secret_access_key = @constCast("fixture") };
    const security = ContentSecurityConfig{ .block_private_ips = false, .max_download_size_bytes = 4 };
    var downloaded = try downloadContentAllocWithContext(a, .{ .io = io }, "s3://bucket/clip.wav", &security, &credentials);
    defer downloaded.deinit(a);
    try std.testing.expectEqualStrings("okay", downloaded.data);
    try std.testing.expectEqualStrings("audio/wav", downloaded.content_type);
    const range = try PinnedRange.create(a, .{ .io = io }, "s3://bucket/clip.wav", "\"v1\"", .strong_etag, 4, &security, &credentials, &.{});
    defer range.destroy();
    var bytes: [4]u8 = undefined;
    try std.testing.expectEqual(@as(usize, 4), try range.readAt(0, &bytes));
    try std.testing.expectEqualStrings("okay", &bytes);
    try serving.await(io);
    if (failure) |err| return err;
    try std.testing.expectEqual(@as(usize, 1), upstream.routeHitCount(0));
    try std.testing.expectEqual(@as(usize, 2), upstream.routeHitCount(1));
}

test "pinned local ranges verify digest, bound reads, detect edits and revoke access" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const data = "0123456789";
    try tmp.dir.writeFile(io, .{ .sub_path = "video file.mp4", .data = data });
    const path = try tmp.dir.realPathFileAlloc(io, "video file.mp4", a);
    defer a.free(path);
    const raw = try std.fmt.allocPrint(a, "file://{s}", .{path});
    defer a.free(raw);
    const uri = try std.mem.replaceOwned(u8, a, raw, " ", "%20");
    defer a.free(uri);
    const paths = [_][]u8{path};
    // The object exceeds the download cap; only individual ranges are capped.
    var security = ContentSecurityConfig{ .allowed_paths = &paths, .max_download_size_bytes = 4 };
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(data, &digest, .{});
    const version = std.fmt.bytesToHex(digest, .lower);
    const range = try PinnedRange.create(a, .{ .io = io }, uri, &version, .file_sha256, data.len, &security, null, &.{});
    defer range.destroy();
    var out: [4]u8 = undefined;
    try std.testing.expectEqual(@as(usize, 4), try range.readAt(3, &out));
    try std.testing.expectEqualStrings("3456", &out);
    try std.testing.expectError(error.InvalidRemoteRange, range.readAt(8, &out));
    var too_big: [5]u8 = undefined;
    try std.testing.expectError(error.StreamTooLong, range.readAt(0, &too_big));
    security.allowed_paths = &.{};
    try std.testing.expectError(error.PathNotAllowed, range.readAt(0, &out));
    security.allowed_paths = &paths;
    try std.testing.expectEqual(@as(usize, 4), try range.readAt(0, &out));
    // Same length changes are detected, even without a length mismatch.
    try tmp.dir.writeFile(io, .{ .sub_path = "video file.mp4", .data = "abcdefghij" });
    try std.testing.expectError(error.SourceVersionChanged, range.readAt(0, &out));
    try std.testing.expectError(error.SourceVersionChanged, PinnedRange.create(a, .{ .io = io }, uri, &version, .file_sha256, data.len, &security, null, &.{}));
    try std.testing.expectError(error.SourceVersionChanged, PinnedRange.create(a, .{ .io = io }, uri, &version, .file_sha256, data.len + 1, &security, null, &.{}));
    try std.testing.expectError(error.PathNotAllowed, PinnedRange.create(a, .{ .io = io }, uri, &version, .file_sha256, data.len, null, null, &.{}));
    try std.testing.expectError(error.FileTimeoutUnsupported, PinnedRange.create(a, .{ .io = io, .timeout_ms = 1 }, uri, &version, .file_sha256, data.len, &security, null, &.{}));
}

test "pinned local ranges reject canonical symlink escape" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.createDirPath(io, "allowed");
    try tmp.dir.writeFile(io, .{ .sub_path = "secret", .data = "secret" });
    try tmp.dir.symLink(io, "../secret", "allowed/link", .{});
    const root = try tmp.dir.realPathFileAlloc(io, "allowed", a);
    defer a.free(root);
    const uri = try std.fmt.allocPrint(a, "file://{s}/link", .{root});
    defer a.free(uri);
    const paths = [_][]u8{root};
    const security = ContentSecurityConfig{ .allowed_paths = &paths };
    const version = "0000000000000000000000000000000000000000000000000000000000000000";
    try std.testing.expectError(error.PathNotAllowed, PinnedRange.create(a, .{ .io = io }, uri, version, .file_sha256, 6, &security, null, &.{}));
}

test "local verification receipts avoid repeated full-file reads and invalidate on change" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(io, .{ .sub_path = "media", .data = "0123456789" });
    const path = try tmp.dir.realPathFileAlloc(io, "media", a);
    defer a.free(path);
    const uri = try std.fmt.allocPrint(a, "file://{s}", .{path});
    defer a.free(uri);
    const paths = [_][]u8{path};
    var security = ContentSecurityConfig{ .allowed_paths = &paths };
    var hash: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash("0123456789", &hash, .{});
    const version = std.fmt.bytesToHex(hash, .lower);
    var cache: LocalVerificationCache = .{};
    const cold = try PinnedRange.createWithLocalCache(a, .{ .io = io }, uri, &version, .file_sha256, 10, &security, null, &.{}, &cache);
    cold.destroy();
    // A warm continuation cannot allocate the 256 KiB streaming hash buffer.
    var bytes: [32 * 1024]u8 = undefined;
    var bounded = std.heap.FixedBufferAllocator.init(&bytes);
    const warm = try PinnedRange.createWithLocalCache(bounded.allocator(), .{ .io = io }, uri, &version, .file_sha256, 10, &security, null, &.{}, &cache);
    defer warm.destroy();
    var out: [4]u8 = undefined;
    _ = try warm.readAt(6, &out);
    try std.testing.expectEqualStrings("6789", &out);
    security.allowed_paths = &.{};
    try std.testing.expectError(error.PathNotAllowed, PinnedRange.createWithLocalCache(a, .{ .io = io }, uri, &version, .file_sha256, 10, &security, null, &.{}, &cache));
    security.allowed_paths = &paths;
    try tmp.dir.writeFile(io, .{ .sub_path = "media", .data = "abcdefghij" });
    try std.testing.expectError(error.SourceVersionChanged, warm.validate());
    try std.testing.expectError(error.SourceVersionChanged, PinnedRange.createWithLocalCache(a, .{ .io = io }, uri, &version, .file_sha256, 10, &security, null, &.{}, &cache));
}

test "local source authorization acquires hot-reloaded policy for every extraction" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(io, .{ .sub_path = "media", .data = "media" });
    const path = try tmp.dir.realPathFileAlloc(io, "media", a);
    defer a.free(path);
    const uri = try std.fmt.allocPrint(a, "file://{s}", .{path});
    defer a.free(uri);
    const paths = [_][]u8{path};
    const Publisher = struct {
        config: RemoteContentConfig = .{ .security = .{ .allowed_paths = &.{} } },
        acquires: usize = 0,
        releases: usize = 0,
        fn acquire(raw: *anyopaque, out: *RemoteContentConfig.Snapshot) callconv(.c) RemoteContentConfig.RemoteContentRuntime.AcquireStatus {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.acquires += 1;
            out.* = .{ .config = &self.config, .context = self, .release_fn = release };
            return .ok;
        }
        fn release(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.releases += 1;
        }
    };
    var publisher: Publisher = .{};
    const facade = RemoteContentConfig{ .runtime = .{ .context = &publisher, .acquire_fn = Publisher.acquire } };
    try std.testing.expectError(error.PathNotAllowed, authorizeLocalSource(a, io, uri, &facade));
    publisher.config.security.?.allowed_paths = &paths;
    try authorizeLocalSource(a, io, uri, &facade);
    publisher.config.security.?.allowed_paths = &.{};
    try std.testing.expectError(error.PathNotAllowed, authorizeLocalSource(a, io, uri, &facade));
    try std.testing.expectEqual(@as(usize, 3), publisher.acquires);
    try std.testing.expectEqual(publisher.acquires, publisher.releases);
}

const LocalSourceFixture = struct {
    allocator: Allocator,
    tmp: std.testing.TmpDir,
    path: [:0]u8,
    uri: []u8,
    version: [64]u8,
    length: u64,
    fn init(a: Allocator, data: []const u8) !LocalSourceFixture {
        var tmp = std.testing.tmpDir(.{});
        errdefer tmp.cleanup();
        try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "media", .data = data });
        const path = try tmp.dir.realPathFileAlloc(std.testing.io, "media", a);
        errdefer a.free(path);
        const uri = try std.fmt.allocPrint(a, "file://{s}", .{path});
        var hash: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(data, &hash, .{});
        return .{ .allocator = a, .tmp = tmp, .path = path, .uri = uri, .version = std.fmt.bytesToHex(hash, .lower), .length = data.len };
    }
    fn deinit(self: *LocalSourceFixture) void {
        self.allocator.free(self.path);
        self.allocator.free(self.uri);
        self.tmp.cleanup();
    }
};

test "local ranges retain the runtime deadline boundary until descriptor cleanup" {
    const a = std.testing.allocator;
    var fixture = try LocalSourceFixture.init(a, "0123456789");
    defer fixture.deinit();
    const paths = [_][]u8{fixture.path};
    const security = ContentSecurityConfig{ .allowed_paths = &paths };
    const Boundary = struct {
        enters: usize = 0,
        leaves: usize = 0,
        expired: bool = false,
        fn enter(raw: *const anyopaque) !u64 {
            const self: *@This() = @ptrCast(@alignCast(@constCast(raw)));
            self.enters += 1;
            return self.enters;
        }
        fn leave(raw: *const anyopaque, _: u64) void {
            const self: *@This() = @ptrCast(@alignCast(@constCast(raw)));
            self.leaves += 1;
        }
        fn check(raw: *const anyopaque) !void {
            const self: *const @This() = @ptrCast(@alignCast(raw));
            if (self.expired) return error.Timeout;
        }
    };
    var boundary: Boundary = .{};
    const context = DownloadContext{ .io = std.testing.io, .timeout_ms = 1000, .blocking_io = .{ .context = &boundary, .enter = Boundary.enter, .leave = Boundary.leave, .check = Boundary.check } };
    const range = try PinnedRange.create(a, context, fixture.uri, &fixture.version, .file_sha256, fixture.length, &security, null, &.{});
    var destroyed = false;
    defer if (!destroyed) range.destroy();
    try std.testing.expectEqual(@as(usize, 1), boundary.enters);
    try std.testing.expectEqual(@as(usize, 0), boundary.leaves);
    var bytes: [4]u8 = undefined;
    _ = try range.readAt(0, &bytes);
    try std.testing.expectEqualStrings("0123", &bytes);
    boundary.expired = true;
    try std.testing.expectError(error.Timeout, range.readAt(0, &bytes));
    range.destroy();
    destroyed = true;
    try std.testing.expectEqual(boundary.enters, boundary.leaves);
    boundary.expired = false;
    var downloaded = try downloadContentOutcomeAllocWithContext(a, context, fixture.uri, &security, null);
    defer downloaded.ok.deinit(a);
    try std.testing.expectEqualStrings("0123456789", downloaded.ok.data);
    try std.testing.expectEqual(boundary.enters, boundary.leaves);
}

test "local verification coalesces cold opens and releases cancelled waiters" {
    const a = std.testing.allocator;
    const base = std.testing.io;
    var fixture = try LocalSourceFixture.init(a, "0123456789");
    defer fixture.deinit();
    const paths = [_][]u8{fixture.path};
    const security = ContentSecurityConfig{ .allowed_paths = &paths };
    var cache: LocalVerificationCache = .{};
    const Meter = struct {
        var entered: std.atomic.Value(bool) = .init(false);
        var proceed: std.atomic.Value(bool) = .init(false);
        fn read(raw: ?*anyopaque, file: std.Io.File, data: []const []u8, offset: u64) std.Io.File.ReadPositionalError!usize {
            entered.store(true, .release);
            while (!proceed.load(.acquire)) std.atomic.spinLoopHint();
            return std.testing.io.vtable.fileReadPositional(raw, file, data, offset);
        }
    };
    var table = base.vtable.*;
    table.fileReadPositional = Meter.read;
    const io = std.Io{ .userdata = base.userdata, .vtable = &table };
    const Work = struct {
        context: DownloadContext,
        fixture: *const LocalSourceFixture,
        security: *const ContentSecurityConfig,
        cache: *LocalVerificationCache,
        failure: ?anyerror = null,
        fn run(self: *@This()) void {
            const range = PinnedRange.createWithLocalCache(std.testing.allocator, self.context, self.fixture.uri, &self.fixture.version, .file_sha256, self.fixture.length, self.security, null, &.{}, self.cache) catch |err| {
                self.failure = err;
                return;
            };
            range.destroy();
        }
    };
    var first = Work{ .context = .{ .io = io }, .fixture = &fixture, .security = &security, .cache = &cache };
    const t1 = try std.Thread.spawn(.{}, Work.run, .{&first});
    defer {
        Meter.proceed.store(true, .release);
        t1.join();
    }
    while (!Meter.entered.load(.acquire)) try base.sleep(.fromMilliseconds(1), .awake);
    const Cancel = struct {
        flag: std.atomic.Value(bool) = .init(false),
        fn cancelled(raw: *const anyopaque) bool {
            const self: *const @This() = @ptrCast(@alignCast(raw));
            return self.flag.load(.acquire);
        }
    };
    var cancel: Cancel = .{};
    var second = first;
    second.context.cancellation = .{ .ptr = &cancel, .is_cancelled_fn = Cancel.cancelled };
    const t2 = try std.Thread.spawn(.{}, Work.run, .{&second});
    // Wait until the second caller has actually joined the same verification.
    var attempts: usize = 0;
    while (true) : (attempts += 1) {
        cache.lock();
        var references: usize = 0;
        for (&cache.flights) |*flight| references += flight.references;
        cache.mutex.unlock();
        if (references == 2 or attempts == 1000) break;
        try base.sleep(.fromMilliseconds(1), .awake);
    }
    cancel.flag.store(true, .release);
    t2.join();
    try std.testing.expect(second.failure != null);
    try std.testing.expect(second.failure.? == @as(anyerror, error.Cancelled) or second.failure.? == @as(anyerror, error.Canceled));
    var third = first;
    const t3 = try std.Thread.spawn(.{}, Work.run, .{&third});
    Meter.proceed.store(true, .release);
    t3.join();
    try std.testing.expect(third.failure == null);
    try std.testing.expectEqual(@as(u64, 1), cache.hash_count.load(.monotonic));
    try std.testing.expectEqual(fixture.length, cache.hashed_bytes.load(.monotonic));
}

test "local verification bounds flights, evicts receipts, and balances failed allocation cleanup" {
    const a = std.testing.allocator;
    var fixture = try LocalSourceFixture.init(a, "0123456789");
    defer fixture.deinit();
    const paths = [_][]u8{fixture.path};
    const security = ContentSecurityConfig{ .allowed_paths = &paths };
    var cache: LocalVerificationCache = .{};
    var leases: [64]LocalVerificationCache.VerificationLease = undefined;
    for (&leases, 0..) |*lease, i| {
        var key: [32]u8 = @splat(0);
        std.mem.writeInt(u64, key[0..8], i, .little);
        lease.* = try cache.acquire(DownloadContext{ .io = std.testing.io }, key);
    }
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, cache.acquire(DownloadContext{ .io = std.testing.io }, @splat(255)));
    for (leases) |lease| lease.deinit();
    const range = try PinnedRange.createWithLocalCache(a, .{ .io = std.testing.io }, fixture.uri, &fixture.version, .file_sha256, fixture.length, &security, null, &.{}, &cache);
    const stat = range.backend.?.file.stat;
    range.destroy();
    for (0..65) |i| {
        var key: [32]u8 = @splat(255);
        std.mem.writeInt(u64, key[0..8], i, .little);
        cache.remember(key, stat, fixture.version);
    }
    const again = try PinnedRange.createWithLocalCache(a, .{ .io = std.testing.io }, fixture.uri, &fixture.version, .file_sha256, fixture.length, &security, null, &.{}, &cache);
    again.destroy();
    try std.testing.expectEqual(@as(u64, 2), cache.hash_count.load(.monotonic));
    const Allocations = struct {
        fn check(alloc: Allocator, f: *const LocalSourceFixture, policy: *const ContentSecurityConfig) !void {
            var local: LocalVerificationCache = .{};
            defer for (local.flights) |flight| std.debug.assert(flight.references == 0);
            const opened = try PinnedRange.createWithLocalCache(alloc, .{ .io = std.testing.io }, f.uri, &f.version, .file_sha256, f.length, policy, null, &.{}, &local);
            defer opened.destroy();
            var bytes: [4]u8 = undefined;
            _ = try opened.readAt(0, &bytes);
            try opened.validate();
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Allocations.check, .{ &fixture, &security });
}

test "local source benchmark verifies 128 MiB once with bounded warm range reads" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const length = 128 * 1024 * 1024;
    var block: [64 * 1024]u8 = undefined;
    for (&block, 0..) |*byte, i| byte.* = @intCast(i % 251);
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    {
        const file = try tmp.dir.createFile(io, "large-media", .{});
        defer file.close(io);
        for (0..length / block.len) |_| {
            try file.writeStreamingAll(io, &block);
            hash.update(&block);
        }
    }
    const path = try tmp.dir.realPathFileAlloc(io, "large-media", a);
    defer a.free(path);
    const uri = try std.fmt.allocPrint(a, "file://{s}", .{path});
    defer a.free(uri);
    const paths = [_][]u8{path};
    const security = ContentSecurityConfig{ .allowed_paths = &paths, .max_download_size_bytes = 4096 };
    const version = std.fmt.bytesToHex(hash.finalResult(), .lower);
    var cache: LocalVerificationCache = .{};
    const started = std.Io.Timestamp.now(io, .awake);
    const cold = try PinnedRange.createWithLocalCache(a, .{ .io = io }, uri, &version, .file_sha256, length, &security, null, &.{}, &cache);
    cold.destroy();
    const cold_ms = started.durationTo(.now(io, .awake)).toMilliseconds();
    // The warm reader has too little memory to allocate a hash buffer or copy
    // the input. Head/middle/tail requests qualify positional reads as well.
    var memory: [32 * 1024]u8 = undefined;
    var bounded = std.heap.FixedBufferAllocator.init(&memory);
    const warm_start = std.Io.Timestamp.now(io, .awake);
    const warm = try PinnedRange.createWithLocalCache(bounded.allocator(), .{ .io = io }, uri, &version, .file_sha256, length, &security, null, &.{}, &cache);
    defer warm.destroy();
    var bytes: [4096]u8 = undefined;
    for ([_]u64{ 0, length / 2, length - bytes.len }) |offset| {
        _ = try warm.readAt(offset, &bytes);
        for (bytes, 0..) |byte, i| try std.testing.expectEqual(block[@intCast((offset + i) % block.len)], byte);
    }
    try warm.validate();
    try std.testing.expectEqual(@as(u64, 1), cache.hash_count.load(.monotonic));
    try std.testing.expectEqual(@as(u64, length), cache.hashed_bytes.load(.monotonic));
    // Wrong claims reuse the computed digest instead of repeating a full scan.
    try std.testing.expectError(error.SourceVersionChanged, PinnedRange.createWithLocalCache(a, .{ .io = io }, uri, "0000000000000000000000000000000000000000000000000000000000000000", .file_sha256, length, &security, null, &.{}, &cache));
    try std.testing.expectEqual(@as(u64, 1), cache.hash_count.load(.monotonic));
    std.debug.print("local source 128 MiB: cold verification {d}ms; warm open plus three 4 KiB ranges {d}ms; warm allocator {d} bytes; hash scans {d}\n", .{ cold_ms, warm_start.durationTo(.now(io, .awake)).toMilliseconds(), bounded.end_index, cache.hash_count.load(.monotonic) });
}

test "local sources reject edits during hashing and positional reads without publishing receipts" {
    const a = std.testing.allocator;
    const base = std.testing.io;
    var fixture = try LocalSourceFixture.init(a, "0123456789");
    defer fixture.deinit();
    const paths = [_][]u8{fixture.path};
    const security = ContentSecurityConfig{ .allowed_paths = &paths };
    const Mutation = struct {
        var dir: std.Io.Dir = undefined;
        var active: bool = false;
        fn read(raw: ?*anyopaque, file: std.Io.File, data: []const []u8, offset: u64) std.Io.File.ReadPositionalError!usize {
            const n = try std.testing.io.vtable.fileReadPositional(raw, file, data, offset);
            if (active) {
                active = false;
                dir.writeFile(std.testing.io, .{ .sub_path = "media", .data = "changed" }) catch return error.InputOutput;
            }
            return n;
        }
    };
    Mutation.dir = fixture.tmp.dir;
    var table = base.vtable.*;
    table.fileReadPositional = Mutation.read;
    const observed = std.Io{ .userdata = base.userdata, .vtable = &table };
    var cache: LocalVerificationCache = .{};
    Mutation.active = true;
    try std.testing.expectError(error.SourceVersionChanged, PinnedRange.createWithLocalCache(a, .{ .io = observed }, fixture.uri, &fixture.version, .file_sha256, fixture.length, &security, null, &.{}, &cache));
    for (cache.entries) |entry| try std.testing.expect(entry == null);
    for (cache.flights) |flight| try std.testing.expectEqual(@as(usize, 0), flight.references);
    try fixture.tmp.dir.writeFile(base, .{ .sub_path = "media", .data = "0123456789" });
    const range = try PinnedRange.createWithLocalCache(a, .{ .io = observed }, fixture.uri, &fixture.version, .file_sha256, fixture.length, &security, null, &.{}, &cache);
    defer range.destroy();
    Mutation.active = true;
    var bytes: [4]u8 = undefined;
    try std.testing.expectError(error.SourceVersionChanged, range.readAt(0, &bytes));
}

test "local source cancellation during hashing releases verification and publishes no receipt" {
    const a = std.testing.allocator;
    const base = std.testing.io;
    var fixture = try LocalSourceFixture.init(a, "0123456789");
    defer fixture.deinit();
    const paths = [_][]u8{fixture.path};
    const security = ContentSecurityConfig{ .allowed_paths = &paths };
    const Cancel = struct {
        var cancelled: std.atomic.Value(bool) = .init(false);
        fn read(raw: ?*anyopaque, file: std.Io.File, data: []const []u8, offset: u64) std.Io.File.ReadPositionalError!usize {
            const n = try std.testing.io.vtable.fileReadPositional(raw, file, data, offset);
            cancelled.store(true, .release);
            return n;
        }
    };
    Cancel.cancelled.store(false, .release);
    var table = base.vtable.*;
    table.fileReadPositional = Cancel.read;
    const observed = std.Io{ .userdata = base.userdata, .vtable = &table };
    var cache: LocalVerificationCache = .{};
    try std.testing.expectError(error.Canceled, PinnedRange.createWithLocalCache(a, .{ .io = observed, .cancellation = CancellationToken.fromAtomic(&Cancel.cancelled) }, fixture.uri, &fixture.version, .file_sha256, fixture.length, &security, null, &.{}, &cache));
    for (cache.entries) |entry| try std.testing.expect(entry == null);
    for (cache.flights) |flight| try std.testing.expectEqual(@as(usize, 0), flight.references);
    const range = try PinnedRange.createWithLocalCache(a, .{ .io = base }, fixture.uri, &fixture.version, .file_sha256, fixture.length, &security, null, &.{}, &cache);
    defer range.destroy();
}

test "warm local receipts bypass busy and saturated verification flights" {
    const a = std.testing.allocator;
    var fixture = try LocalSourceFixture.init(a, "0123456789");
    defer fixture.deinit();
    const paths = [_][]u8{fixture.path};
    const security = ContentSecurityConfig{ .allowed_paths = &paths };
    var cache: LocalVerificationCache = .{};
    const cold = try PinnedRange.createWithLocalCache(a, .{ .io = std.testing.io }, fixture.uri, &fixture.version, .file_sha256, fixture.length, &security, null, &.{}, &cache);
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update(cold.identity);
    const actual_key = hash.finalResult();
    cold.destroy();
    var leases: [64]LocalVerificationCache.VerificationLease = undefined;
    var held: usize = 0;
    defer for (leases[0..held]) |lease| lease.deinit();
    for (&leases, 0..) |*lease, i| {
        var key: [32]u8 = @splat(0);
        std.mem.writeInt(u64, key[0..8], i, .little);
        if (i == 0) key = actual_key;
        lease.* = try cache.acquire(DownloadContext{ .io = std.testing.io }, key);
        held += 1;
    }
    // Also hold this file's own flight: a warm request must not wait for it.
    // A regression must fail promptly instead of hanging this test forever.
    const Deadline = struct {
        expires: u64,
        fn cancelled(raw: *const anyopaque) bool {
            const self: *const @This() = @ptrCast(@alignCast(raw));
            return @import("antfly_platform").time.monotonicNs() >= self.expires;
        }
    };
    const deadline = Deadline{ .expires = @import("antfly_platform").time.monotonicNs() + std.time.ns_per_s };
    const warm = try PinnedRange.createWithLocalCache(a, .{ .io = std.testing.io, .cancellation = CancellationToken.fromCallback(&deadline, Deadline.cancelled) }, fixture.uri, &fixture.version, .file_sha256, fixture.length, &security, null, &.{}, &cache);
    defer warm.destroy();
    var bytes: [4]u8 = undefined;
    _ = try warm.readAt(6, &bytes);
    try std.testing.expectEqualStrings("6789", &bytes);
    try std.testing.expectEqual(@as(u64, 1), cache.hash_count.load(.monotonic));
    try std.testing.expectError(error.SourceVersionChanged, PinnedRange.createWithLocalCache(a, .{ .io = std.testing.io }, fixture.uri, "0000000000000000000000000000000000000000000000000000000000000000", .file_sha256, fixture.length, &security, null, &.{}, &cache));
}
