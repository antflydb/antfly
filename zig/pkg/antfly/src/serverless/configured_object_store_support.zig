// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Config-aware object-store opening for external lake table bindings.
//!
//! This module deliberately sits outside low-level object_store_support.zig so
//! scanner and scaffold tests do not inherit the full node-config dependency
//! graph. API query routing and serverless publication both use this policy.

const std = @import("std");
const common_config = @import("../common/config.zig");
const common_secrets = @import("../common/secrets.zig");
const catalog_binding = @import("external_source/catalog_binding.zig");
const object_store_support = @import("object_store_support.zig");
const remote_uri = @import("remote_uri.zig");

const Allocator = std.mem.Allocator;

pub const BindingObjectStoreOpenOptions = struct {
    file_bucket: []const u8 = "antfly",
    node_config: ?*const common_config.Config = null,
    secret_store: ?*common_secrets.FileStore = null,
    read_only: bool = true,
};

pub fn openBindingObjectStoreAlloc(
    alloc: Allocator,
    binding: catalog_binding.Binding,
    options: BindingObjectStoreOpenOptions,
) !object_store_support.OpenedObjectStore {
    if (binding.format != .parquet and binding.format != .iceberg) return error.UnsupportedRowsQuery;
    if (binding.credential_ref == null) return try object_store_support.OpenedObjectStore.initRemoteUriWithOptions(
        alloc,
        binding.source_uri,
        options.file_bucket,
        .{ .ensure_bucket = !options.read_only },
    );
    return try openCredentialedBindingObjectStoreAlloc(alloc, binding, options);
}

fn openCredentialedBindingObjectStoreAlloc(
    alloc: Allocator,
    binding: catalog_binding.Binding,
    options: BindingObjectStoreOpenOptions,
) !object_store_support.OpenedObjectStore {
    const credential = binding.credential_ref orelse return error.ExternalLakeCredentialRefRequired;
    const node_config = options.node_config orelse return error.ExternalLakeCredentialRefNotFound;
    const connection = node_config.connections.get(credential.ref_id) orelse return error.ExternalLakeCredentialRefNotFound;
    if (connection.kind != .external_io) return error.UnsupportedExternalLakeCredentialRef;
    if (!hasConnectionCapability(connection, "lake_read")) return error.UnsupportedExternalLakeCredentialRef;
    const external_io = connection.external_io orelse return error.UnsupportedExternalLakeCredentialRef;

    var parsed = try remote_uri.parseAlloc(alloc, binding.source_uri);
    defer switch (parsed) {
        .file => |value| alloc.free(value),
        .gcs => |*value| value.deinit(alloc),
        .s3 => |*value| value.deinit(alloc),
    };

    return switch (parsed) {
        .s3 => |value| try openCredentialedS3PrefixAlloc(
            alloc,
            options.secret_store,
            external_io,
            value.bucket,
            value.prefix,
            options.read_only,
        ),
        .gcs => |value| try openCredentialedGcsPrefixAlloc(
            alloc,
            options.secret_store,
            external_io,
            value.bucket,
            value.prefix,
            options.read_only,
        ),
        .file => |path| blk: {
            if (external_io.protocol != .filesystem) return error.UnsupportedExternalLakeCredentialRef;
            const root = external_io.root orelse return error.UnsupportedExternalLakeCredentialRef;
            const resolved_path = try resolveFilesystemSourcePathAlloc(alloc, root, path);
            defer alloc.free(resolved_path);
            const file_uri = try std.fmt.allocPrint(alloc, "file://{s}", .{resolved_path});
            defer alloc.free(file_uri);
            break :blk try object_store_support.OpenedObjectStore.initFileUriWithOptions(
                alloc,
                file_uri,
                options.file_bucket,
                .{ .ensure_bucket = !options.read_only },
            );
        },
    };
}

fn openCredentialedS3PrefixAlloc(
    alloc: Allocator,
    secret_store: ?*common_secrets.FileStore,
    external_io: common_config.Config.ExternalIoConnectionConfig,
    bucket: []const u8,
    prefix: []const u8,
    read_only: bool,
) !object_store_support.OpenedObjectStore {
    if (external_io.protocol != .s3) return error.UnsupportedExternalLakeCredentialRef;
    try ensureBucketAllowed(bucket, external_io.buckets);
    if (external_io.prefix) |allowed| try ensurePrefixAllowed(prefix, allowed);

    const endpoint = if (external_io.endpoint) |value| try common_secrets.resolveReferenceOwned(alloc, secret_store, value) else null;
    defer if (endpoint) |value| alloc.free(value);
    var resolved_credentials = try common_config.Config.resolveExternalIoCredentials(alloc, external_io, secret_store);
    defer resolved_credentials.deinit(alloc);
    const resolved = resolved_credentials.apply(external_io);
    switch (resolved.credentials.source) {
        .default, .static => {},
        .profile, .web_identity => return error.UnsupportedExternalLakeCredentialRef,
    }

    return try object_store_support.OpenedObjectStore.initS3UriWithOverridesAndOptions(
        alloc,
        bucket,
        prefix,
        .{
            .endpoint = endpoint,
            .region = resolved.region,
            .use_ssl = external_io.use_ssl orelse true,
            .access_key_id = resolved.credentials.access_key_id,
            .secret_access_key = resolved.credentials.secret_access_key,
            .session_token = resolved.credentials.session_token,
            .addressing_style = switch (resolved.addressing_style) {
                .path => .path,
                .virtual_hosted => .virtual_hosted,
            },
        },
        .{ .ensure_bucket = !read_only and resolved.bucket_provisioning == .create_if_missing },
    );
}

fn openCredentialedGcsPrefixAlloc(
    alloc: Allocator,
    secret_store: ?*common_secrets.FileStore,
    external_io: common_config.Config.ExternalIoConnectionConfig,
    bucket: []const u8,
    prefix: []const u8,
    read_only: bool,
) !object_store_support.OpenedObjectStore {
    if (external_io.protocol != .gcs) return error.UnsupportedExternalLakeCredentialRef;
    try ensureBucketAllowed(bucket, external_io.buckets);
    if (external_io.prefix) |allowed| try ensurePrefixAllowed(prefix, allowed);
    var resolved_credentials = try common_config.Config.resolveExternalIoCredentials(alloc, external_io, secret_store);
    defer resolved_credentials.deinit(alloc);
    const resolved = resolved_credentials.apply(external_io);
    const bearer_token = switch (resolved.gcs_credentials.source) {
        .default => try gcsBearerTokenFromHeadersAlloc(alloc, secret_store, external_io.headers),
        .bearer_token => if (resolved.gcs_credentials.bearer_token) |token| try alloc.dupe(u8, token) else return error.UnsupportedExternalLakeCredentialRef,
        .service_account => return error.UnsupportedExternalLakeCredentialRef,
    };
    defer if (bearer_token) |value| alloc.free(value);
    return try object_store_support.OpenedObjectStore.initGcsUriWithBearerTokenAndOptions(
        alloc,
        bucket,
        prefix,
        bearer_token,
        .{ .ensure_bucket = !read_only and resolved.bucket_provisioning == .create_if_missing },
    );
}

fn gcsBearerTokenFromHeadersAlloc(
    alloc: Allocator,
    secret_store: ?*common_secrets.FileStore,
    headers: std.StringArrayHashMapUnmanaged([]u8),
) !?[]u8 {
    const raw_authorization = headerValueIgnoreCase(headers, "Authorization") orelse return null;
    const authorization = try common_secrets.resolveReferenceOwned(alloc, secret_store, raw_authorization);
    errdefer alloc.free(authorization);
    const trimmed = std.mem.trim(u8, authorization, " \t\r\n");
    if (!std.mem.startsWith(u8, trimmed, "Bearer ")) return error.UnsupportedExternalLakeCredentialRef;
    const token = std.mem.trim(u8, trimmed["Bearer ".len..], " \t\r\n");
    if (token.len == 0) return error.UnsupportedExternalLakeCredentialRef;
    if (token.ptr == authorization.ptr and token.len == authorization.len) return authorization;
    const owned = try alloc.dupe(u8, token);
    alloc.free(authorization);
    return owned;
}

fn headerValueIgnoreCase(headers: std.StringArrayHashMapUnmanaged([]u8), name: []const u8) ?[]const u8 {
    var it = headers.iterator();
    while (it.next()) |entry| {
        if (std.ascii.eqlIgnoreCase(entry.key_ptr.*, name)) return entry.value_ptr.*;
    }
    return null;
}

fn ensureBucketAllowed(bucket: []const u8, allowed_buckets: []const []const u8) !void {
    if (allowed_buckets.len == 0) return;
    for (allowed_buckets) |allowed| {
        if (std.mem.eql(u8, allowed, "*") or std.mem.eql(u8, allowed, bucket)) return;
    }
    return error.ExternalLakeCredentialScopeMismatch;
}

fn ensurePrefixAllowed(prefix: []const u8, allowed_prefix: []const u8) !void {
    const allowed = std.mem.trimEnd(u8, allowed_prefix, "/");
    const requested = std.mem.trimEnd(u8, prefix, "/");
    if (allowed.len == 0) return;
    if (std.mem.eql(u8, requested, allowed)) return;
    if (requested.len > allowed.len and
        std.mem.startsWith(u8, requested, allowed) and
        requested[allowed.len] == '/') return;
    return error.ExternalLakeCredentialScopeMismatch;
}

fn resolveFilesystemSourcePathAlloc(
    alloc: Allocator,
    configured_root: []const u8,
    uri_path: []const u8,
) ![]u8 {
    if (!std.fs.path.isAbsolute(configured_root)) return error.UnsupportedExternalLakeCredentialRef;
    const relative = std.mem.trimStart(u8, uri_path, "/");
    if (relative.len > 0) try validateFilesystemSourceRelativePath(relative);

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    const canonical_root = std.Io.Dir.realPathFileAbsoluteAlloc(io, configured_root, alloc) catch |err| switch (err) {
        error.FileNotFound, error.NotDir => return error.UnsupportedExternalLakeCredentialRef,
        else => return err,
    };
    defer alloc.free(canonical_root);

    const candidate = if (relative.len == 0)
        try alloc.dupe(u8, canonical_root)
    else
        try std.fs.path.join(alloc, &.{ canonical_root, relative });
    errdefer alloc.free(candidate);

    // Resolve the nearest existing ancestor so an existing symlink cannot
    // redirect a root-relative source outside the administrator-owned root.
    var ancestor: []const u8 = candidate;
    while (true) {
        const canonical = std.Io.Dir.realPathFileAbsoluteAlloc(io, ancestor, alloc) catch |err| switch (err) {
            error.FileNotFound, error.NotDir => {
                ancestor = std.fs.path.dirname(ancestor) orelse return error.ExternalLakeCredentialScopeMismatch;
                continue;
            },
            else => return err,
        };
        defer alloc.free(canonical);
        if (!filesystemPathIsWithin(canonical_root, canonical)) return error.ExternalLakeCredentialScopeMismatch;
        break;
    }
    return candidate;
}

fn validateFilesystemSourceRelativePath(path: []const u8) !void {
    if (path.len > 4096 or std.fs.path.isAbsolute(path) or
        std.mem.indexOfAny(u8, path, "\\\x00") != null)
    {
        return error.ExternalLakeCredentialScopeMismatch;
    }
    var components = std.mem.splitScalar(u8, path, '/');
    while (components.next()) |component| {
        if (component.len == 0 or std.mem.eql(u8, component, ".") or std.mem.eql(u8, component, "..")) {
            return error.ExternalLakeCredentialScopeMismatch;
        }
    }
}

fn filesystemPathIsWithin(root: []const u8, candidate: []const u8) bool {
    if (std.mem.eql(u8, root, candidate)) return true;
    if (candidate.len <= root.len or !std.mem.startsWith(u8, candidate, root)) return false;
    return root[root.len - 1] == std.fs.path.sep or candidate[root.len] == std.fs.path.sep;
}

fn hasConnectionCapability(connection: common_config.Config.ConnectionConfig, capability: []const u8) bool {
    for (connection.capabilities) |value| {
        if (std.mem.eql(u8, value, capability)) return true;
    }
    return false;
}

test "credentialed binding object store opens scoped filesystem source" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const cwd = try std.Io.Dir.cwd().realPathFileAlloc(std.testing.io, ".", alloc);
    defer alloc.free(cwd);
    const allowed_root = try std.fs.path.resolve(alloc, &.{ cwd, ".zig-cache", "tmp", tmp.sub_path[0..], "allowed" });
    defer alloc.free(allowed_root);
    try std.Io.Dir.createDirAbsolute(std.testing.io, allowed_root, .default_dir);
    const allowed_root_json = try std.json.Stringify.valueAlloc(alloc, allowed_root, .{});
    defer alloc.free(allowed_root_json);
    const allowed_path = try std.fs.path.resolve(alloc, &.{ allowed_root, "events" });
    defer alloc.free(allowed_path);

    const cfg_json = try std.fmt.allocPrint(alloc,
        \\{{
        \\  "connections": {{
        \\    "prod-lake-read": {{
        \\      "kind": "external_io",
        \\      "capabilities": ["lake_read"],
        \\      "external_io": {{
        \\        "protocol": "filesystem",
        \\        "root": {s}
        \\      }}
        \\    }}
        \\  }}
        \\}}
    , .{allowed_root_json});
    defer alloc.free(cfg_json);
    var cfg = try common_config.Config.parseFromSlice(alloc, cfg_json);
    defer cfg.deinit();

    var opened = try openBindingObjectStoreAlloc(alloc, .{
        .table_id = "events",
        .format = .parquet,
        .source_uri = "file:///events",
        .credential_ref = .{ .ref_id = "prod-lake-read", .scope = "events" },
        .schema_fingerprint = "schema-v1",
    }, .{
        .file_bucket = "external-lake",
        .node_config = &cfg,
    });
    defer opened.deinit();
    try std.testing.expectEqualStrings("external-lake", opened.bucket);
    try std.testing.expectEqualStrings(allowed_path, opened.fs_client.?.root_dir);
    try std.testing.expect(!(try opened.client.bucketExists("external-lake")));

    try std.testing.expectError(error.ExternalLakeCredentialScopeMismatch, openBindingObjectStoreAlloc(alloc, .{
        .table_id = "events",
        .format = .parquet,
        .source_uri = "file:///../denied/events",
        .credential_ref = .{ .ref_id = "prod-lake-read", .scope = "events" },
        .schema_fingerprint = "schema-v1",
    }, .{
        .file_bucket = "external-lake",
        .node_config = &cfg,
    }));
}

test "serverless external source credentialed binding opens scoped gcs source with bearer token" {
    const alloc = std.testing.allocator;
    const cfg_json =
        \\{
        \\  "connections": {
        \\    "prod-gcs-lake-read": {
        \\      "kind": "external_io",
        \\      "capabilities": ["lake_read"],
        \\      "external_io": {
        \\        "protocol": "gcs",
        \\        "buckets": ["lake-bucket"],
        \\        "prefix": "events",
        \\        "credentials": {
        \\          "source": "bearer_token",
        \\          "bearer_token": "test-token"
        \\        }
        \\      }
        \\    }
        \\  }
        \\}
    ;
    var cfg = try common_config.Config.parseFromSlice(alloc, cfg_json);
    defer cfg.deinit();

    var opened = try openBindingObjectStoreAlloc(alloc, .{
        .table_id = "events",
        .format = .parquet,
        .source_uri = "gs://lake-bucket/events/2026",
        .credential_ref = .{ .ref_id = "prod-gcs-lake-read", .scope = "events" },
        .schema_fingerprint = "schema-v1",
    }, .{
        .file_bucket = "external-lake",
        .node_config = &cfg,
    });
    defer opened.deinit();

    try std.testing.expectEqualStrings("lake-bucket", opened.bucket);
    try std.testing.expectEqualStrings("events/2026", opened.prefix);
    const gcs_client = opened.gcs_client orelse return error.TestExpectedEqual;
    switch (gcs_client.cfg.auth) {
        .bearer_token => |token| try std.testing.expectEqualStrings("test-token", token),
        else => return error.TestExpectedEqual,
    }

    try std.testing.expectError(error.ExternalLakeCredentialScopeMismatch, openBindingObjectStoreAlloc(alloc, .{
        .table_id = "events",
        .format = .parquet,
        .source_uri = "gs://lake-bucket/other",
        .credential_ref = .{ .ref_id = "prod-gcs-lake-read", .scope = "events" },
        .schema_fingerprint = "schema-v1",
    }, .{
        .file_bucket = "external-lake",
        .node_config = &cfg,
    }));
    try std.testing.expectError(error.ExternalLakeCredentialScopeMismatch, openBindingObjectStoreAlloc(alloc, .{
        .table_id = "events-private",
        .format = .parquet,
        .source_uri = "gs://lake-bucket/events-private/2026",
        .credential_ref = .{ .ref_id = "prod-gcs-lake-read", .scope = "events" },
        .schema_fingerprint = "schema-v1",
    }, .{
        .file_bucket = "external-lake",
        .node_config = &cfg,
    }));
}

test "serverless external source credentialed binding opens scoped s3 source with configured credentials" {
    const alloc = std.testing.allocator;
    const cfg_json =
        \\{
        \\  "connections": {
        \\    "prod-s3-lake-read": {
        \\      "kind": "external_io",
        \\      "capabilities": ["lake_read"],
        \\      "external_io": {
        \\        "protocol": "s3",
        \\        "endpoint": "http://127.0.0.1:9000",
        \\        "use_ssl": true,
        \\        "buckets": ["lake-bucket"],
        \\        "prefix": "events",
        \\        "credentials": {
        \\          "source": "static",
        \\          "access_key_id": "test-key",
        \\          "secret_access_key": "test-secret",
        \\          "session_token": "test-session"
        \\        }
        \\      }
        \\    }
        \\  }
        \\}
    ;
    var cfg = try common_config.Config.parseFromSlice(alloc, cfg_json);
    defer cfg.deinit();

    var opened = try openBindingObjectStoreAlloc(alloc, .{
        .table_id = "events",
        .format = .parquet,
        .source_uri = "s3://lake-bucket/events/2026",
        .credential_ref = .{ .ref_id = "prod-s3-lake-read", .scope = "events" },
        .schema_fingerprint = "schema-v1",
    }, .{
        .file_bucket = "external-lake",
        .node_config = &cfg,
    });
    defer opened.deinit();

    try std.testing.expectEqualStrings("lake-bucket", opened.bucket);
    try std.testing.expectEqualStrings("events/2026", opened.prefix);
    const s3_client = opened.s3_client orelse return error.TestExpectedEqual;
    try std.testing.expectEqualStrings("127.0.0.1:9000", s3_client.cfg.credentials.endpoint);
    try std.testing.expect(!s3_client.cfg.credentials.use_ssl);
    try std.testing.expectEqualStrings("test-key", s3_client.cfg.credentials.access_key_id);
    try std.testing.expectEqualStrings("test-secret", s3_client.cfg.credentials.secret_access_key);
    try std.testing.expectEqualStrings("test-session", s3_client.cfg.credentials.session_token.?);

    try std.testing.expectError(error.ExternalLakeCredentialScopeMismatch, openBindingObjectStoreAlloc(alloc, .{
        .table_id = "events",
        .format = .parquet,
        .source_uri = "s3://lake-bucket/other",
        .credential_ref = .{ .ref_id = "prod-s3-lake-read", .scope = "events" },
        .schema_fingerprint = "schema-v1",
    }, .{
        .file_bucket = "external-lake",
        .node_config = &cfg,
    }));
    try std.testing.expectError(error.ExternalLakeCredentialScopeMismatch, openBindingObjectStoreAlloc(alloc, .{
        .table_id = "events-private",
        .format = .parquet,
        .source_uri = "s3://lake-bucket/events-private/2026",
        .credential_ref = .{ .ref_id = "prod-s3-lake-read", .scope = "events" },
        .schema_fingerprint = "schema-v1",
    }, .{
        .file_bucket = "external-lake",
        .node_config = &cfg,
    }));
}

test "serverless external source resolves s3 credential secrets" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const secret_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/s3-secrets.json", .{tmp.sub_path});
    defer alloc.free(secret_path);
    var secret_store = try common_secrets.FileStore.init(alloc, secret_path);
    defer secret_store.deinit();
    var stored_endpoint = try secret_store.put(alloc, "s3.lake.endpoint", "http://127.0.0.1:9000");
    defer stored_endpoint.deinit(alloc);
    var stored_key = try secret_store.put(alloc, "s3.lake.access_key_id", "secret-key");
    defer stored_key.deinit(alloc);
    var stored_secret = try secret_store.put(alloc, "s3.lake.secret_access_key", "secret-secret");
    defer stored_secret.deinit(alloc);
    var stored_session = try secret_store.put(alloc, "s3.lake.session_token", "secret-session");
    defer stored_session.deinit(alloc);

    const cfg_json =
        \\{
        \\  "connections": {
        \\    "prod-s3-lake-read": {
        \\      "kind": "external_io",
        \\      "capabilities": ["lake_read"],
        \\      "external_io": {
        \\        "protocol": "s3",
        \\        "endpoint": "${secret:s3.lake.endpoint}",
        \\        "buckets": ["lake-bucket"],
        \\        "credentials": {
        \\          "source": "static",
        \\          "access_key_id": "${secret:s3.lake.access_key_id}",
        \\          "secret_access_key": "${secret:s3.lake.secret_access_key}",
        \\          "session_token": "${secret:s3.lake.session_token}"
        \\        }
        \\      }
        \\    }
        \\  }
        \\}
    ;
    var cfg = try common_config.Config.parseFromSliceWithSecrets(alloc, cfg_json, &secret_store);
    defer cfg.deinit();

    var opened = try openBindingObjectStoreAlloc(alloc, .{
        .table_id = "events",
        .format = .parquet,
        .source_uri = "s3://lake-bucket/events",
        .credential_ref = .{ .ref_id = "prod-s3-lake-read", .scope = "events" },
        .schema_fingerprint = "schema-v1",
    }, .{
        .file_bucket = "external-lake",
        .node_config = &cfg,
        .secret_store = &secret_store,
    });
    defer opened.deinit();

    const s3_client = opened.s3_client orelse return error.TestExpectedEqual;
    try std.testing.expectEqualStrings("127.0.0.1:9000", s3_client.cfg.credentials.endpoint);
    try std.testing.expect(!s3_client.cfg.credentials.use_ssl);
    try std.testing.expectEqualStrings("secret-key", s3_client.cfg.credentials.access_key_id);
    try std.testing.expectEqualStrings("secret-secret", s3_client.cfg.credentials.secret_access_key);
    try std.testing.expectEqualStrings("secret-session", s3_client.cfg.credentials.session_token.?);
}

test "serverless external source resolves gcs bearer token secret" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const secret_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/gcs-secrets.json", .{tmp.sub_path});
    defer alloc.free(secret_path);
    var secret_store = try common_secrets.FileStore.init(alloc, secret_path);
    defer secret_store.deinit();
    var stored = try secret_store.put(alloc, "gcs.lake.token", "secret-token");
    defer stored.deinit(alloc);

    const cfg_json =
        \\{
        \\  "connections": {
        \\    "prod-gcs-lake-read": {
        \\      "kind": "external_io",
        \\      "capabilities": ["lake_read"],
        \\      "external_io": {
        \\        "protocol": "gcs",
        \\        "buckets": ["lake-bucket"],
        \\        "credentials": {
        \\          "source": "bearer_token",
        \\          "bearer_token": "${secret:gcs.lake.token}"
        \\        }
        \\      }
        \\    }
        \\  }
        \\}
    ;
    var cfg = try common_config.Config.parseFromSliceWithSecrets(alloc, cfg_json, &secret_store);
    defer cfg.deinit();

    var opened = try openBindingObjectStoreAlloc(alloc, .{
        .table_id = "events",
        .format = .parquet,
        .source_uri = "gs://lake-bucket/events",
        .credential_ref = .{ .ref_id = "prod-gcs-lake-read", .scope = "events" },
        .schema_fingerprint = "schema-v1",
    }, .{
        .file_bucket = "external-lake",
        .node_config = &cfg,
        .secret_store = &secret_store,
    });
    defer opened.deinit();

    const gcs_client = opened.gcs_client orelse return error.TestExpectedEqual;
    switch (gcs_client.cfg.auth) {
        .bearer_token => |token| try std.testing.expectEqualStrings("secret-token", token),
        else => return error.TestExpectedEqual,
    }
}

test "credential-free binding object store opens read-only without creating file bucket" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const lake_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/credential-free", .{tmp.sub_path});
    defer alloc.free(lake_path);
    const source_uri = try std.fmt.allocPrint(alloc, "file://{s}", .{lake_path});
    defer alloc.free(source_uri);

    var opened = try openBindingObjectStoreAlloc(alloc, .{
        .table_id = "events",
        .format = .parquet,
        .source_uri = source_uri,
        .schema_fingerprint = "schema-v1",
    }, .{ .file_bucket = "external-lake" });
    defer opened.deinit();

    try std.testing.expectEqualStrings("external-lake", opened.bucket);
    try std.testing.expect(!(try opened.client.bucketExists("external-lake")));
}
