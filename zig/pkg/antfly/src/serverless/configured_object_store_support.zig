// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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
const common_config = @import("antfly_local_sources").common_config;
const common_secrets = @import("antfly_local_sources").common_secrets;
const catalog_binding = @import("antfly_local_sources").serverless_external_source_catalog_binding;
const object_store_support = @import("antfly_local_sources").serverless_object_store_support;
const remote_uri = @import("antfly_local_sources").serverless_remote_uri;
const lake_catalog = @import("antfly_local_sources").serverless_external_source_mod.lake_catalog;

const Allocator = std.mem.Allocator;

/// Opens Antfly-owned publication storage. Source lake credentials never grant
/// artifact write authority; this requires an independently configured lane.
pub fn openNativeArtifactObjectStoreAlloc(
    alloc: Allocator,
    node_config: *const common_config.Config,
    secret_store: ?*common_secrets.FileStore,
    read_only: bool,
) !object_store_support.OpenedObjectStore {
    const location = node_config.storage.artifacts;
    const connection_id = location.connection orelse return error.NativeArtifactStorageRequired;
    const bucket = location.bucket orelse return error.NativeArtifactStorageRequired;
    const prefix = location.prefix orelse "native-lake-indexes";
    const connection = node_config.connections.get(connection_id) orelse return error.NativeArtifactStorageRequired;
    if (connection.kind != .external_io or !hasConnectionCapability(connection, "storage.primary")) return error.NativeArtifactStorageUnauthorized;
    const external_io = connection.external_io orelse return error.NativeArtifactStorageUnauthorized;
    return switch (external_io.protocol) {
        .s3 => try openCredentialedS3PrefixAlloc(alloc, secret_store, external_io, bucket, prefix, read_only),
        .gcs => try openCredentialedGcsPrefixAlloc(alloc, secret_store, external_io, bucket, prefix, read_only),
        .filesystem => blk: {
            const root = external_io.root orelse return error.NativeArtifactStorageUnauthorized;
            try validateFilesystemSourceRelativePath(prefix);
            const path = try resolveFilesystemSourcePathAlloc(alloc, root, prefix);
            defer alloc.free(path);
            const uri = try std.fmt.allocPrint(alloc, "file://{s}", .{path});
            defer alloc.free(uri);
            break :blk try object_store_support.OpenedObjectStore.initFileUriWithOptions(alloc, uri, bucket, .{ .ensure_bucket = !read_only });
        },
        else => error.NativeArtifactStorageUnauthorized,
    };
}

pub const BindingObjectStoreOpenOptions = struct {
    /// Borrowed options must outlive synchronous lake source opening.
    pub fn lakeOptions(self: *const @This()) @import("antfly_local_sources").serverless_lake_host.OpenOptions {
        return .{ .file_bucket = self.file_bucket, .resolver = .{ .ptr = self, .open_fn = openLake }, .catalog_resolver = .{ .ptr = self, .load_fn = loadLakeCatalog }, .snapshot_pin_resolver = .{ .ptr = self, .acquire = acquireSnapshotPin } };
    }
    fn acquireSnapshotPin(raw: *const anyopaque, a: Allocator, source: catalog_binding.Binding, snapshot: []const u8, uuid: []const u8, context: lake_catalog.types.Context) anyerror!?@import("antfly_local_sources").serverless_lake_host.SnapshotPin {
        _ = a;
        const pin_alloc = @import("antfly_platform").allocator.processAllocator(std.heap.smp_allocator);
        const self: *const @This() = @ptrCast(@alignCast(raw));
        if (source.catalog == null or self.node_config == null or self.node_config.?.storage.artifacts.connection == null or snapshot.len == 0) return null;
        const pins = @import("lake_snapshot_pins.zig");
        var opened = try openNativeArtifactObjectStoreAlloc(pin_alloc, self.node_config.?, self.secret_store, false);
        errdefer opened.deinit();
        const prefix = try pins.namespace(pin_alloc, opened.prefix, source, uuid);
        errdefer pin_alloc.free(prefix);
        const id = try pin_alloc.dupe(u8, snapshot);
        errdefer pin_alloc.free(id);
        const owner = try pin_alloc.create(pins.Owner);
        errdefer pin_alloc.destroy(owner);
        owner.* = .{ .a = pin_alloc, .opened = opened, .prefix = prefix, .snapshot = id, .parent = context, .io = context.io orelse return error.LakeSnapshotReadLeaseExpired };
        return try owner.start();
    }
    fn openLake(raw: *const anyopaque, alloc: Allocator, source: catalog_binding.Binding) anyerror!object_store_support.OpenedObjectStore {
        const self: *const @This() = @ptrCast(@alignCast(raw));
        return openBindingObjectStoreAlloc(alloc, source, self.*);
    }
    fn loadLakeCatalog(raw: *const anyopaque, alloc: Allocator, source: catalog_binding.Binding, context: lake_catalog.types.Context) anyerror!lake_catalog.types.Table {
        const self: *const @This() = @ptrCast(@alignCast(raw));
        var result = try executeLakeCatalogAlloc(alloc, source, self.*, context, .load);
        if (self.retained_catalog_metadata) |retained| {
            defer result.deinit(alloc);
            var arena = std.heap.ArenaAllocator.init(alloc);
            defer arena.deinit();
            const scratch = arena.allocator();
            const current = try lake_catalog.metadata.parse(scratch, result.table.metadata_json);
            const pinned = try lake_catalog.metadata.parse(scratch, retained.metadata_json);
            const current_uuid = try lake_catalog.metadata.str(try lake_catalog.metadata.get(current, "table-uuid"));
            const pinned_uuid = try lake_catalog.metadata.str(try lake_catalog.metadata.get(pinned, "table-uuid"));
            if (!std.mem.eql(u8, current_uuid, pinned_uuid)) return error.ExternalLakeSnapshotMismatch;
            const location = try alloc.dupe(u8, retained.metadata_location);
            errdefer alloc.free(location);
            return .{ .metadata_location = location, .metadata_json = try alloc.dupe(u8, retained.metadata_json) };
        }
        return result.table;
    }
    /// Server-authenticated metadata; load still verifies external incarnation.
    retained_catalog_metadata: ?lake_catalog.types.Table = null,
    file_bucket: []const u8 = "antfly",
    node_config: ?*const common_config.Config = null,
    secret_store: ?*common_secrets.FileStore = null,
    read_only: bool = true,
    catalog_table_id: u64 = 0,
    catalog_generation: u64 = 0,
};

pub const CatalogOperation = union(enum) {
    load,
    create: lake_catalog.types.Commit,
    commit: lake_catalog.types.Commit,
    resolve: struct { id: []const u8, hash: []const u8 },
};
pub const CatalogResult = union(enum) {
    table: lake_catalog.types.Table,
    outcome: lake_catalog.types.Outcome,
    pub fn deinit(self: *CatalogResult, a: Allocator) void {
        if (self.* == .table) self.table.deinit(a);
    }
};

pub fn executeLakeCatalogAlloc(a: Allocator, binding: catalog_binding.Binding, options: BindingObjectStoreOpenOptions, context: lake_catalog.types.Context, operation: CatalogOperation) !CatalogResult {
    try context.ensureActive();
    try binding.validateSupported();
    const config = binding.catalog orelse return error.InvalidLakeCatalog;
    const mutation = operation == .create or operation == .commit;
    if (mutation and binding.write_policy != .iceberg_writer) return error.ExternalLakeReadOnly;
    var source_options = options;
    source_options.read_only = !mutation or config.type == .rest;
    var opened = try openBindingObjectStoreAlloc(a, binding, source_options);
    defer opened.deinit();
    var catalog: lake_catalog.Catalog = undefined;
    if (config.type == .managed) {
        catalog = .{ .managed = .{ .client = opened.client, .bucket = opened.bucket, .prefix = opened.prefix, .source_uri = binding.source_uri, .context = context } };
        return executeCatalog(a, &catalog, operation);
    }
    const node = options.node_config orelse return error.LakeCatalogConnectionRequired;
    const connection = node.connections.get(config.connection.?) orelse return error.LakeCatalogConnectionRequired;
    if (connection.kind != .external_io or !hasConnectionCapability(connection, "lake_catalog_read") or (mutation and !hasConnectionCapability(connection, "lake_catalog_write"))) return error.LakeCatalogForbidden;
    const external = connection.external_io orelse return error.LakeCatalogForbidden;
    if (external.protocol != .http) return error.LakeCatalogForbidden;
    try ensureCatalogOriginAllowed(config.uri.?, external.hosts);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var headers: std.ArrayList([2][]const u8) = .empty;
    var header_it = external.headers.iterator();
    while (header_it.next()) |entry| {
        // Connection headers are node policy; references are resolved per call
        // so rotated credentials never become table metadata or cache identity.
        const value = try common_secrets.resolveReferenceOwned(scratch, options.secret_store, entry.value_ptr.*);
        if (std.mem.indexOfAny(u8, value, "\r\n\x00") != null or std.mem.indexOfAny(u8, entry.key_ptr.*, "\r\n\x00") != null) return error.LakeCatalogForbidden;
        if (std.ascii.eqlIgnoreCase(entry.key_ptr.*, "Idempotency-Key") or std.ascii.eqlIgnoreCase(entry.key_ptr.*, "Host") or std.ascii.eqlIgnoreCase(entry.key_ptr.*, "Content-Length")) return error.LakeCatalogForbidden;
        try headers.append(scratch, .{ entry.key_ptr.*, value });
    }
    var io_impl: ?std.Io.Threaded = if (context.io == null) std.Io.Threaded.init(a, .{}) else null;
    defer if (io_impl) |*io| io.deinit();
    var http = @import("httpx").Client.initWithConfig(a, context.io orelse io_impl.?.io(), .{ .keep_alive = false, .cookies_enabled = false, .max_response_size = lake_catalog.types.max_metadata_bytes, .timeouts = .{ .connect_ms = 10_000, .read_ms = 30_000, .write_ms = 30_000 } });
    defer http.deinit();
    var transport: lake_catalog.rest.HttpTransport = .{ .client = &http, .headers = headers.items };
    var journal_store: ?object_store_support.OpenedObjectStore = null;
    defer if (journal_store) |*store| store.deinit();
    var journal: ?lake_catalog.rest.Journal = null;
    if (mutation or operation == .resolve) {
        journal_store = try openNativeArtifactObjectStoreAlloc(a, node, options.secret_store, !mutation);
        const authority = try std.json.Stringify.valueAlloc(scratch, .{ .source = binding.source_uri, .catalog = config }, .{});
        const prefix = try std.fmt.allocPrint(scratch, "{s}/lake-catalog/{d}/{d}/{s}", .{ journal_store.?.prefix, options.catalog_table_id, options.catalog_generation, lake_catalog.types.digestHex(authority) });
        journal = .{ .client = journal_store.?.client, .bucket = journal_store.?.bucket, .prefix = prefix };
    }
    catalog = .{ .rest = .{ .config = config, .transport = transport.transport(), .context = context, .journal = journal, .now_ms = @intCast(@import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms) } };
    return executeCatalog(a, &catalog, operation);
}
fn executeCatalog(a: Allocator, catalog: *lake_catalog.Catalog, operation: CatalogOperation) !CatalogResult {
    return switch (operation) {
        .load => .{ .table = try catalog.load(a) },
        .create => |c| .{ .table = try catalog.create(a, c.id, c.body, c.timestamp_ms) },
        .commit => |c| .{ .table = try catalog.commit(a, c) },
        .resolve => |c| .{ .outcome = try catalog.resolve(a, c.id, c.hash) },
    };
}
fn uriHost(component: std.Uri.Component) []const u8 {
    return switch (component) {
        .raw => |v| v,
        .percent_encoded => |v| v,
    };
}
fn ensureCatalogOriginAllowed(uri: []const u8, allowed: []const []u8) !void {
    const requested = try std.Uri.parse(uri);
    for (allowed) |origin| {
        const candidate = std.Uri.parse(origin) catch continue;
        if (candidate.host == null or requested.host == null or candidate.user != null or candidate.password != null) continue;
        if (!std.ascii.eqlIgnoreCase(candidate.scheme, requested.scheme) or !std.ascii.eqlIgnoreCase(uriHost(candidate.host.?), uriHost(requested.host.?))) continue;
        const default_port: u16 = if (std.ascii.eqlIgnoreCase(requested.scheme, "https")) 443 else 80;
        if ((candidate.port orelse default_port) == (requested.port orelse default_port)) return;
    }
    return error.LakeCatalogForbidden;
}

pub fn openBindingObjectStoreAlloc(
    alloc: Allocator,
    binding: catalog_binding.Binding,
    options: BindingObjectStoreOpenOptions,
) !object_store_support.OpenedObjectStore {
    try binding.validate();
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
    if (!hasConnectionCapability(connection, "lake_read") or (!options.read_only and !hasConnectionCapability(connection, "lake_write"))) return error.UnsupportedExternalLakeCredentialRef;
    const external_io = connection.external_io orelse return error.UnsupportedExternalLakeCredentialRef;

    var parsed = try remote_uri.parseAlloc(alloc, binding.source_uri);
    defer switch (parsed) {
        .file => |value| alloc.free(value),
        .gcs => |*value| value.deinit(alloc),
        .s3 => |*value| value.deinit(alloc),
    };

    return switch (parsed) {
        .s3 => |value| blk: {
            try ensurePrefixAllowed(value.prefix, credential.scope);
            break :blk try openCredentialedS3PrefixAlloc(
                alloc,
                options.secret_store,
                external_io,
                value.bucket,
                value.prefix,
                options.read_only,
            );
        },
        .gcs => |value| blk: {
            try ensurePrefixAllowed(value.prefix, credential.scope);
            break :blk try openCredentialedGcsPrefixAlloc(
                alloc,
                options.secret_store,
                external_io,
                value.bucket,
                value.prefix,
                options.read_only,
            );
        },
        .file => |path| blk: {
            if (external_io.protocol != .filesystem) return error.UnsupportedExternalLakeCredentialRef;
            const root = external_io.root orelse return error.UnsupportedExternalLakeCredentialRef;
            try ensurePrefixAllowed(std.mem.trimStart(u8, path, "/"), credential.scope);
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
    if (!catalog_binding.isPrefixWithinCredentialScope(prefix, allowed_prefix)) {
        return error.ExternalLakeCredentialScopeMismatch;
    }
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

    // The binding scope is a second, catalog-owned restriction and must not be
    // silently widened by a broader matching connection prefix.
    try std.testing.expectError(error.ExternalLakeCredentialScopeMismatch, openBindingObjectStoreAlloc(alloc, .{
        .table_id = "events",
        .format = .parquet,
        .source_uri = "gs://lake-bucket/events/2026",
        .credential_ref = .{ .ref_id = "prod-gcs-lake-read", .scope = "events/private" },
        .schema_fingerprint = "schema-v1",
    }, .{
        .file_bucket = "external-lake",
        .node_config = &cfg,
    }));

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
