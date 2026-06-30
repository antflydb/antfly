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
            if (external_io.prefix) |allowed| try ensurePrefixAllowed(path, allowed);
            const file_uri = try std.fmt.allocPrint(alloc, "file://{s}", .{path});
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
    const access_key_id = if (external_io.access_key_id) |value| try common_secrets.resolveReferenceOwned(alloc, secret_store, value) else null;
    defer if (access_key_id) |value| alloc.free(value);
    const secret_access_key = if (external_io.secret_access_key) |value| try common_secrets.resolveReferenceOwned(alloc, secret_store, value) else null;
    defer if (secret_access_key) |value| alloc.free(value);
    const session_token = if (external_io.session_token) |value| try common_secrets.resolveReferenceOwned(alloc, secret_store, value) else null;
    defer if (session_token) |value| alloc.free(value);

    return try object_store_support.OpenedObjectStore.initS3UriWithOverridesAndOptions(
        alloc,
        bucket,
        prefix,
        .{
            .endpoint = endpoint,
            .use_ssl = external_io.use_ssl orelse true,
            .access_key_id = access_key_id,
            .secret_access_key = secret_access_key,
            .session_token = session_token,
        },
        .{ .ensure_bucket = !read_only },
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
    const bearer_token = try gcsBearerTokenFromHeadersAlloc(alloc, secret_store, external_io.headers);
    defer if (bearer_token) |value| alloc.free(value);
    return try object_store_support.OpenedObjectStore.initGcsUriWithBearerTokenAndOptions(
        alloc,
        bucket,
        prefix,
        bearer_token,
        .{ .ensure_bucket = !read_only },
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
    if (allowed_prefix.len == 0) return;
    if (std.mem.startsWith(u8, prefix, allowed_prefix)) return;
    return error.ExternalLakeCredentialScopeMismatch;
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
    const allowed_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/allowed/events", .{tmp.sub_path});
    defer alloc.free(allowed_path);
    const denied_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/denied/events", .{tmp.sub_path});
    defer alloc.free(denied_path);

    const cfg_json = try std.fmt.allocPrint(alloc,
        \\{{
        \\  "connections": {{
        \\    "prod-lake-read": {{
        \\      "kind": "external_io",
        \\      "capabilities": ["lake_read"],
        \\      "external_io": {{
        \\        "protocol": "filesystem",
        \\        "prefix": ".zig-cache/tmp/{s}/allowed"
        \\      }}
        \\    }}
        \\  }}
        \\}}
    , .{tmp.sub_path});
    defer alloc.free(cfg_json);
    var cfg = try common_config.Config.parseFromSlice(alloc, cfg_json);
    defer cfg.deinit();

    const allowed_uri = try std.fmt.allocPrint(alloc, "file://{s}", .{allowed_path});
    defer alloc.free(allowed_uri);
    var opened = try openBindingObjectStoreAlloc(alloc, .{
        .table_id = "events",
        .format = .parquet,
        .source_uri = allowed_uri,
        .credential_ref = .{ .ref_id = "prod-lake-read", .scope = "events" },
        .schema_fingerprint = "schema-v1",
    }, .{
        .file_bucket = "external-lake",
        .node_config = &cfg,
    });
    defer opened.deinit();
    try std.testing.expectEqualStrings("external-lake", opened.bucket);
    try std.testing.expect(!(try opened.client.bucketExists("external-lake")));

    const denied_uri = try std.fmt.allocPrint(alloc, "file://{s}", .{denied_path});
    defer alloc.free(denied_uri);
    try std.testing.expectError(error.ExternalLakeCredentialScopeMismatch, openBindingObjectStoreAlloc(alloc, .{
        .table_id = "events",
        .format = .parquet,
        .source_uri = denied_uri,
        .credential_ref = .{ .ref_id = "prod-lake-read", .scope = "events" },
        .schema_fingerprint = "schema-v1",
    }, .{
        .file_bucket = "external-lake",
        .node_config = &cfg,
    }));
}

test "credentialed binding object store opens scoped gcs source with bearer token" {
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
        \\        "headers": {
        \\          "Authorization": "Bearer test-token"
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
}

test "credentialed binding object store opens scoped s3 source with configured credentials" {
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
        \\        "access_key_id": "test-key",
        \\        "secret_access_key": "test-secret",
        \\        "session_token": "test-session"
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
}

test "credentialed binding object store resolves s3 credential secrets" {
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
        \\        "access_key_id": "${secret:s3.lake.access_key_id}",
        \\        "secret_access_key": "${secret:s3.lake.secret_access_key}",
        \\        "session_token": "${secret:s3.lake.session_token}"
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

test "credentialed binding object store resolves gcs authorization secret" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const secret_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/gcs-secrets.json", .{tmp.sub_path});
    defer alloc.free(secret_path);
    var secret_store = try common_secrets.FileStore.init(alloc, secret_path);
    defer secret_store.deinit();
    var stored = try secret_store.put(alloc, "gcs.lake.authorization", "Bearer secret-token");
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
        \\        "headers": {
        \\          "authorization": "${secret:gcs.lake.authorization}"
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
