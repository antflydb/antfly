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
};

pub fn openBindingObjectStoreAlloc(
    alloc: Allocator,
    binding: catalog_binding.Binding,
    options: BindingObjectStoreOpenOptions,
) !object_store_support.OpenedObjectStore {
    if (binding.format != .parquet and binding.format != .iceberg) return error.UnsupportedRowsQuery;
    if (binding.credential_ref == null) return try object_store_support.OpenedObjectStore.initRemoteUri(
        alloc,
        binding.source_uri,
        options.file_bucket,
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
        ),
        .gcs => |value| try openCredentialedGcsPrefixAlloc(alloc, external_io, value.bucket, value.prefix),
        .file => |path| blk: {
            if (external_io.protocol != .filesystem) return error.UnsupportedExternalLakeCredentialRef;
            if (external_io.prefix) |allowed| try ensurePrefixAllowed(path, allowed);
            const file_uri = try std.fmt.allocPrint(alloc, "file://{s}", .{path});
            defer alloc.free(file_uri);
            break :blk try object_store_support.OpenedObjectStore.initFileUri(alloc, file_uri, options.file_bucket);
        },
    };
}

fn openCredentialedS3PrefixAlloc(
    alloc: Allocator,
    secret_store: ?*common_secrets.FileStore,
    external_io: common_config.Config.ExternalIoConnectionConfig,
    bucket: []const u8,
    prefix: []const u8,
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

    return try object_store_support.OpenedObjectStore.initS3UriWithOverrides(alloc, bucket, prefix, .{
        .endpoint = endpoint,
        .use_ssl = external_io.use_ssl orelse true,
        .access_key_id = access_key_id,
        .secret_access_key = secret_access_key,
        .session_token = session_token,
    });
}

fn openCredentialedGcsPrefixAlloc(
    alloc: Allocator,
    external_io: common_config.Config.ExternalIoConnectionConfig,
    bucket: []const u8,
    prefix: []const u8,
) !object_store_support.OpenedObjectStore {
    if (external_io.protocol != .gcs) return error.UnsupportedExternalLakeCredentialRef;
    try ensureBucketAllowed(bucket, external_io.buckets);
    if (external_io.prefix) |allowed| try ensurePrefixAllowed(prefix, allowed);
    return try object_store_support.OpenedObjectStore.initGcsUri(alloc, bucket, prefix);
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
