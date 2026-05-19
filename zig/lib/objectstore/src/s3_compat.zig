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
const platform = @import("antfly_platform");
const httpx = @import("httpx");
const types = @import("types.zig");

pub const Scheme = enum {
    http,
    https,
};

pub const AddressingStyle = enum {
    virtual_hosted,
    path,
};

pub const Credentials = struct {
    endpoint: []u8,
    use_ssl: bool = true,
    access_key_id: []u8,
    secret_access_key: []u8,
    session_token: ?[]u8 = null,
    region: []u8,

    pub fn deinit(self: *Credentials, alloc: std.mem.Allocator) void {
        alloc.free(self.endpoint);
        alloc.free(self.access_key_id);
        alloc.free(self.secret_access_key);
        if (self.session_token) |value| alloc.free(value);
        alloc.free(self.region);
        self.* = undefined;
    }
};

pub const Config = struct {
    endpoint: []const u8,
    region: []const u8,
    access_key_id: []const u8,
    secret_access_key: []const u8,
    session_token: ?[]const u8 = null,
    scheme: Scheme = .https,
    addressing_style: AddressingStyle = .virtual_hosted,
};

pub const EndpointResolution = struct {
    endpoint: []u8,
    use_ssl: bool,

    pub fn deinit(self: *EndpointResolution, alloc: std.mem.Allocator) void {
        alloc.free(self.endpoint);
        self.* = undefined;
    }
};

pub const RequestShape = struct {
    method: []const u8,
    uri: []u8,
    host: []u8,
    content_type: ?[]u8 = null,

    pub fn deinit(self: *RequestShape, alloc: std.mem.Allocator) void {
        alloc.free(self.uri);
        alloc.free(self.host);
        if (self.content_type) |value| alloc.free(value);
        self.* = undefined;
    }
};

pub const S3Path = struct {
    bucket: []u8,
    key: []u8,

    pub fn deinit(self: *S3Path, alloc: std.mem.Allocator) void {
        alloc.free(self.bucket);
        alloc.free(self.key);
        self.* = undefined;
    }
};

pub fn resolveEndpointAlloc(alloc: std.mem.Allocator, endpoint: []const u8, use_ssl: bool) !EndpointResolution {
    if (std.mem.startsWith(u8, endpoint, "http://") or std.mem.startsWith(u8, endpoint, "https://")) {
        const parsed = try std.Uri.parse(endpoint);
        if (parsed.host == null) return error.InvalidEndpoint;
        const host = parsed.host.?.percent_encoded;
        const resolved = if (parsed.port) |port|
            try std.fmt.allocPrint(alloc, "{s}:{d}", .{ host, port })
        else
            try alloc.dupe(u8, host);
        return .{
            .endpoint = resolved,
            .use_ssl = std.mem.eql(u8, parsed.scheme, "https"),
        };
    }

    return .{
        .endpoint = try alloc.dupe(u8, endpoint),
        .use_ssl = use_ssl,
    };
}

pub fn credentialsFromEnvAlloc(
    alloc: std.mem.Allocator,
    endpoint_override: ?[]const u8,
    use_ssl: bool,
    access_key_id_override: ?[]const u8,
    secret_access_key_override: ?[]const u8,
    session_token_override: ?[]const u8,
    region_override: ?[]const u8,
) !Credentials {
    const endpoint = try firstValueOwned(alloc, endpoint_override, "AWS_ENDPOINT_URL", null);
    errdefer if (endpoint.len > 0) alloc.free(endpoint);
    const access_key_id = try firstValueOwned(alloc, access_key_id_override, "AWS_ACCESS_KEY_ID", null);
    errdefer alloc.free(access_key_id);
    const secret_access_key = try firstValueOwned(alloc, secret_access_key_override, "AWS_SECRET_ACCESS_KEY", null);
    errdefer alloc.free(secret_access_key);
    const session_token = try optionalValueOwned(alloc, session_token_override, "AWS_SESSION_TOKEN");
    errdefer if (session_token) |value| alloc.free(value);
    const region = try firstValueOwned(alloc, region_override, "AWS_REGION", "us-east-1");
    errdefer alloc.free(region);

    if (endpoint.len == 0) return error.MissingEndpoint;
    if (access_key_id.len == 0) return error.MissingAccessKeyId;
    if (secret_access_key.len == 0) return error.MissingSecretAccessKey;

    var resolved = try resolveEndpointAlloc(alloc, endpoint, use_ssl);
    defer resolved.deinit(alloc);
    alloc.free(endpoint);

    return .{
        .endpoint = try alloc.dupe(u8, resolved.endpoint),
        .use_ssl = resolved.use_ssl,
        .access_key_id = access_key_id,
        .secret_access_key = secret_access_key,
        .session_token = session_token,
        .region = region,
    };
}

pub fn parseCanonicalS3UrlAlloc(alloc: std.mem.Allocator, location: []const u8) !S3Path {
    const parsed = try std.Uri.parse(location);
    if (!std.mem.eql(u8, parsed.scheme, "s3")) return error.InvalidScheme;
    const host = parsed.host orelse return error.MissingBucket;
    const bucket = host.percent_encoded;
    if (bucket.len == 0) return error.MissingBucket;
    return .{
        .bucket = try alloc.dupe(u8, bucket),
        .key = try alloc.dupe(u8, trimLeftSlash(parsed.path.percent_encoded)),
    };
}

pub fn extractBucketFromUrlAlloc(alloc: std.mem.Allocator, location: []const u8) ![]u8 {
    const parsed = try std.Uri.parse(location);
    if (!std.mem.eql(u8, parsed.scheme, "s3")) return error.InvalidScheme;
    if (parsed.host) |host| {
        const bucket = host.percent_encoded;
        if (bucket.len > 0 and std.mem.indexOfScalar(u8, bucket, '.') == null and std.mem.indexOfScalar(u8, bucket, ':') == null) {
            return try alloc.dupe(u8, bucket);
        }
    }

    const path = trimLeftSlash(parsed.path.percent_encoded);
    if (path.len == 0) return error.MissingBucket;
    const slash = std.mem.indexOfScalar(u8, path, '/') orelse path.len;
    return try alloc.dupe(u8, path[0..slash]);
}

pub fn objectUriAlloc(alloc: std.mem.Allocator, cfg: Config, bucket: []const u8, key: []const u8) !RequestShape {
    const scheme = switch (cfg.scheme) {
        .http => "http",
        .https => "https",
    };
    const escaped_key = try escapeKeyAlloc(alloc, key);
    defer alloc.free(escaped_key);

    return switch (cfg.addressing_style) {
        .virtual_hosted => .{
            .method = "GET",
            .uri = try std.fmt.allocPrint(alloc, "{s}://{s}.{s}/{s}", .{ scheme, bucket, cfg.endpoint, escaped_key }),
            .host = try std.fmt.allocPrint(alloc, "{s}.{s}", .{ bucket, cfg.endpoint }),
        },
        .path => .{
            .method = "GET",
            .uri = try std.fmt.allocPrint(alloc, "{s}://{s}/{s}/{s}", .{ scheme, cfg.endpoint, bucket, escaped_key }),
            .host = try alloc.dupe(u8, cfg.endpoint),
        },
    };
}

pub fn putObjectShapeAlloc(alloc: std.mem.Allocator, cfg: Config, bucket: []const u8, key: []const u8, opts: types.PutOptions) !RequestShape {
    var shape = try objectUriAlloc(alloc, cfg, bucket, key);
    shape.method = "PUT";
    if (opts.content_type) |value| shape.content_type = try alloc.dupe(u8, value);
    return shape;
}

fn firstValueOwned(alloc: std.mem.Allocator, override: ?[]const u8, env_name: []const u8, fallback: ?[]const u8) ![]u8 {
    if (override) |value| return try alloc.dupe(u8, value);
    if (try optionalEnvOwned(alloc, env_name)) |value| return value;
    if (fallback) |value| return try alloc.dupe(u8, value);
    return try alloc.alloc(u8, 0);
}

fn optionalValueOwned(alloc: std.mem.Allocator, override: ?[]const u8, env_name: []const u8) !?[]u8 {
    if (override) |value| return try alloc.dupe(u8, value);
    return try optionalEnvOwned(alloc, env_name);
}

fn optionalEnvOwned(alloc: std.mem.Allocator, env_name: []const u8) !?[]u8 {
    const env_name_z = try alloc.dupeZ(u8, env_name);
    defer alloc.free(env_name_z);
    const value = platform.env.getenvSlice(env_name_z) orelse return null;
    return try alloc.dupe(u8, value);
}

fn escapeKeyAlloc(alloc: std.mem.Allocator, key: []const u8) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    for (key) |byte| {
        if (byte == '/') {
            try out.append(alloc, '/');
            continue;
        }
        const encoded = try httpx.uri.encode(alloc, &.{byte});
        defer alloc.free(encoded);
        try out.appendSlice(alloc, encoded);
    }
    return try out.toOwnedSlice(alloc);
}

fn trimLeftSlash(path: []const u8) []const u8 {
    if (path.len == 0) return path;
    var idx: usize = 0;
    while (idx < path.len and path[idx] == '/') : (idx += 1) {}
    return path[idx..];
}

test "resolve endpoint strips scheme and infers ssl" {
    const alloc = std.testing.allocator;
    var resolved = try resolveEndpointAlloc(alloc, "https://storage.googleapis.com", false);
    defer resolved.deinit(alloc);
    try std.testing.expectEqualStrings("storage.googleapis.com", resolved.endpoint);
    try std.testing.expect(resolved.use_ssl);
}

test "canonical s3 url returns bucket and prefix" {
    const alloc = std.testing.allocator;
    var parsed = try parseCanonicalS3UrlAlloc(alloc, "s3://my-bucket/path/to/backups/");
    defer parsed.deinit(alloc);
    try std.testing.expectEqualStrings("my-bucket", parsed.bucket);
    try std.testing.expectEqualStrings("path/to/backups/", parsed.key);
}

test "extract bucket handles endpoint style s3 url" {
    const alloc = std.testing.allocator;
    const bucket = try extractBucketFromUrlAlloc(alloc, "s3://storage.googleapis.com/my-bucket/path/object.txt");
    defer alloc.free(bucket);
    try std.testing.expectEqualStrings("my-bucket", bucket);
}

test "s3 compat builds path-style object uri" {
    const alloc = std.testing.allocator;
    var shape = try objectUriAlloc(alloc, .{
        .endpoint = "127.0.0.1:9000",
        .region = "us-east-1",
        .access_key_id = "a",
        .secret_access_key = "b",
        .scheme = .http,
        .addressing_style = .path,
    }, "bucket", "a/b.txt");
    defer shape.deinit(alloc);
    try std.testing.expectEqualStrings("http://127.0.0.1:9000/bucket/a/b.txt", shape.uri);
}

test "credentials from env rejects missing endpoint" {
    const alloc = std.testing.allocator;
    try std.testing.expectError(error.MissingEndpoint, credentialsFromEnvAlloc(
        alloc,
        "",
        false,
        "access",
        "secret",
        null,
        "us-east-1",
    ));
}

test "credentials from env rejects missing access key id" {
    const alloc = std.testing.allocator;
    try std.testing.expectError(error.MissingAccessKeyId, credentialsFromEnvAlloc(
        alloc,
        "127.0.0.1:9000",
        false,
        "",
        "secret",
        null,
        "us-east-1",
    ));
}

test "credentials from env rejects missing secret access key" {
    const alloc = std.testing.allocator;
    try std.testing.expectError(error.MissingSecretAccessKey, credentialsFromEnvAlloc(
        alloc,
        "127.0.0.1:9000",
        false,
        "access",
        "",
        null,
        "us-east-1",
    ));
}
