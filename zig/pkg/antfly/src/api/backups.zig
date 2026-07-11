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

const std = @import("std");
const metadata_openapi = @import("antfly_metadata_openapi");
const fs_paths = @import("../common/fs_paths.zig");
const group_ids = @import("../common/group_ids.zig");
const metadata_table_manager = @import("../metadata/table_manager.zig");
const object_storage = @import("../storage/object_storage.zig");
const remote_uri = @import("../serverless/remote_uri.zig");
const tables_api = @import("tables.zig");
const common_secrets = @import("../common/secrets.zig");
const extension_domain = @import("../extensions/mod.zig");

pub const BackupRequest = metadata_openapi.BackupRequest;
pub const RestoreRequest = metadata_openapi.RestoreRequest;
pub const ClusterBackupRequest = struct {
    backup_id: []const u8,
    location: []const u8,
    table_names: ?[]const []const u8 = null,
};
pub const ClusterRestoreRequest = struct {
    backup_id: []const u8,
    location: []const u8,
    table_names: ?[]const []const u8 = null,
    restore_mode: ?[]const u8 = null,
};

pub const format_version: u32 = 1;
pub const cluster_format_version: u32 = 1;
pub const table_backup_id = "table";
pub const antfly_version = "zig-dev";
pub const max_portable_backup_file_bytes: usize = 1024 * 1024 * 1024;

pub const BackupFormat = enum {
    native,
    portable,
};

pub const TableBackupManifest = struct {
    format_version: u32 = format_version,
    backup_id: []const u8,
    table_name: []const u8,
    description: []const u8,
    schema_json: []const u8,
    read_schema_json: []const u8,
    indexes_json: []const u8,
    replication_sources_json: []const u8,
    shards: []const ShardSnapshot,

    pub fn deinit(self: *TableBackupManifest, alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.backup_id));
        alloc.free(@constCast(self.table_name));
        alloc.free(@constCast(self.description));
        alloc.free(@constCast(self.schema_json));
        alloc.free(@constCast(self.read_schema_json));
        alloc.free(@constCast(self.indexes_json));
        alloc.free(@constCast(self.replication_sources_json));
        for (self.shards) |shard| shard.deinit(alloc);
        alloc.free(@constCast(self.shards));
        self.* = undefined;
    }
};

pub const ShardSnapshot = struct {
    group_id: u64,
    start_key: []const u8,
    end_key: ?[]const u8 = null,
    snapshot_path: []const u8,

    pub fn deinit(self: ShardSnapshot, alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.start_key));
        if (self.end_key) |value| alloc.free(@constCast(value));
        alloc.free(@constCast(self.snapshot_path));
    }
};

pub const TableBackupPlan = struct {
    backup_root: []const u8,
    backup_id: []const u8,
    format: BackupFormat = .native,
};

pub const TableRestorePlan = struct {
    backup_root: []const u8,
    manifest: *const TableBackupManifest,
    source_location: ?[]const u8 = null,
    reconcile_only: bool = false,
};

pub fn validateRestorableManifestLayout(manifest: *const TableBackupManifest) !void {
    if (manifest.shards.len == 0) return error.UnsupportedBackupFormat;
    if (manifest.shards.len != 1) return error.UnsupportedMultiRangeTable;
}

pub const BackupLocation = union(enum) {
    file: []u8,
    remote: RemoteBackupStore,

    pub fn deinit(self: *BackupLocation, alloc: std.mem.Allocator) void {
        switch (self.*) {
            .file => |value| alloc.free(value),
            .remote => |*store| store.deinit(),
        }
        self.* = undefined;
    }
};

const RemoteBackupStore = struct {
    alloc: std.mem.Allocator,
    client: object_storage.ObjectStorage,
    gcs_client: ?*object_storage.Gcs.JsonApiClient = null,
    s3_client: ?*object_storage.S3.Client = null,
    owns_client: bool = true,
    bucket: []u8,
    prefix: []u8,

    fn initRemoteUri(alloc: std.mem.Allocator, location: []const u8, secret_store: ?*common_secrets.FileStore) !RemoteBackupStore {
        const normalized = try normalizeRemoteLocationAlloc(alloc, location);
        defer alloc.free(normalized);

        var parsed = try remote_uri.parseAlloc(alloc, normalized);
        defer switch (parsed) {
            .file => |value| alloc.free(value),
            .gcs => |*value| value.deinit(alloc),
            .s3 => |*value| value.deinit(alloc),
        };

        return switch (parsed) {
            .file => error.UnsupportedBackupLocation,
            .gcs => |value| try initGcsUri(alloc, value.bucket, value.prefix),
            .s3 => |value| try initS3Uri(alloc, value.bucket, value.prefix, secret_store),
        };
    }

    fn initGcsUri(alloc: std.mem.Allocator, bucket: []const u8, prefix: []const u8) !RemoteBackupStore {
        const gcs = try alloc.create(object_storage.Gcs.JsonApiClient);
        errdefer alloc.destroy(gcs);
        const cfg = try object_storage.Gcs.jsonApiClientConfigFromEnvAlloc(alloc);
        gcs.* = try object_storage.Gcs.JsonApiClient.init(alloc, cfg);

        return .{
            .alloc = alloc,
            .client = gcs.client(),
            .gcs_client = gcs,
            .bucket = try alloc.dupe(u8, bucket),
            .prefix = try alloc.dupe(u8, prefix),
        };
    }

    fn initS3Uri(
        alloc: std.mem.Allocator,
        bucket: []const u8,
        prefix: []const u8,
        secret_store: ?*common_secrets.FileStore,
    ) !RemoteBackupStore {
        const s3 = try alloc.create(object_storage.S3.Client);
        errdefer alloc.destroy(s3);
        var overrides = try loadS3SecretOverrides(alloc, secret_store);
        defer overrides.deinit(alloc);
        const cfg = try object_storage.S3.fromEnvAlloc(
            alloc,
            overrides.endpoint,
            true,
            overrides.access_key_id,
            overrides.secret_access_key,
            overrides.session_token,
            overrides.region,
            .path,
        );
        s3.* = try object_storage.S3.Client.init(alloc, cfg);

        return .{
            .alloc = alloc,
            .client = s3.client(),
            .s3_client = s3,
            .bucket = try alloc.dupe(u8, bucket),
            .prefix = try alloc.dupe(u8, prefix),
        };
    }

    fn initWithClient(
        alloc: std.mem.Allocator,
        client: object_storage.ObjectStorage,
        bucket: []const u8,
        prefix: []const u8,
    ) !RemoteBackupStore {
        return .{
            .alloc = alloc,
            .client = client,
            .owns_client = false,
            .bucket = try alloc.dupe(u8, bucket),
            .prefix = try alloc.dupe(u8, prefix),
        };
    }

    fn deinit(self: *RemoteBackupStore) void {
        if (self.owns_client) self.client.deinit();
        if (self.gcs_client) |gcs| self.alloc.destroy(gcs);
        if (self.s3_client) |s3| self.alloc.destroy(s3);
        self.alloc.free(self.bucket);
        self.alloc.free(self.prefix);
        self.* = undefined;
    }

    fn ensureBucket(self: *RemoteBackupStore) !void {
        if (!(try self.client.bucketExists(self.bucket))) try self.client.makeBucket(self.bucket);
    }

    fn keyAlloc(self: *const RemoteBackupStore, alloc: std.mem.Allocator, suffix: []const u8) ![]u8 {
        const trimmed_suffix = trimLeftSlash(suffix);
        if (self.prefix.len == 0) return try alloc.dupe(u8, trimmed_suffix);
        if (trimmed_suffix.len == 0) return try alloc.dupe(u8, self.prefix);
        return try std.fmt.allocPrint(alloc, "{s}/{s}", .{ self.prefix, trimmed_suffix });
    }

    fn writeBytes(self: *RemoteBackupStore, alloc: std.mem.Allocator, suffix: []const u8, body: []const u8, content_type: []const u8) !void {
        try self.ensureBucket();
        const key = try self.keyAlloc(alloc, suffix);
        defer alloc.free(key);
        var result = try self.client.putObject(self.bucket, key, body, .{ .content_type = content_type });
        defer result.deinit(alloc);
    }

    fn readBytesAlloc(self: *RemoteBackupStore, alloc: std.mem.Allocator, suffix: []const u8) ![]u8 {
        const key = try self.keyAlloc(alloc, suffix);
        defer alloc.free(key);
        var result = try self.client.getObject(self.bucket, key, .{});
        defer result.deinit(alloc);
        return try alloc.dupe(u8, result.body);
    }

    fn listObjectsPage(
        self: *RemoteBackupStore,
        alloc: std.mem.Allocator,
        suffix: []const u8,
        recursive: bool,
        max_keys: u32,
        continuation_token: ?[]const u8,
    ) !object_storage.ListResult {
        if (!(try self.client.bucketExists(self.bucket))) {
            return .{
                .entries = try alloc.alloc(object_storage.ListEntry, 0),
                .common_prefixes = try alloc.alloc([]u8, 0),
            };
        }
        var key_prefix = try self.keyAlloc(alloc, suffix);
        defer alloc.free(key_prefix);
        if (!recursive and key_prefix.len > 0 and !std.mem.endsWith(u8, key_prefix, "/")) {
            const with_slash = try std.fmt.allocPrint(alloc, "{s}/", .{key_prefix});
            alloc.free(key_prefix);
            key_prefix = with_slash;
        }
        return try self.client.listObjects(self.bucket, .{
            .prefix = key_prefix,
            .recursive = recursive,
            .max_keys = max_keys,
            .continuation_token = continuation_token,
        });
    }

    fn listObjects(self: *RemoteBackupStore, alloc: std.mem.Allocator, suffix: []const u8) !object_storage.ListResult {
        return try self.listObjectsPage(alloc, suffix, true, 10_000, null);
    }

    fn listTopLevelObjectsPage(
        self: *RemoteBackupStore,
        alloc: std.mem.Allocator,
        continuation_token: ?[]const u8,
    ) !object_storage.ListResult {
        return try self.listObjectsPage(alloc, "", false, 1000, continuation_token);
    }

    fn uploadDirectoryRecursive(self: *RemoteBackupStore, alloc: std.mem.Allocator, src_path: []const u8, dest_suffix: []const u8) !void {
        try self.ensureBucket();

        var io_impl = std.Io.Threaded.init(alloc, .{});
        defer io_impl.deinit();
        const io = io_impl.io();

        var src_dir = try std.Io.Dir.cwd().openDir(io, src_path, .{ .iterate = true });
        defer src_dir.close(io);

        var walker = try src_dir.walk(alloc);
        defer walker.deinit();

        while (try walker.next(io)) |entry| {
            if (entry.kind != .file) {
                if (entry.kind == .directory) continue;
                return error.UnsupportedBackupArtifact;
            }

            const local_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ src_path, entry.path });
            defer alloc.free(local_path);
            const key_suffix = try joinPathAlloc(alloc, dest_suffix, entry.path);
            defer alloc.free(key_suffix);
            const key = try self.keyAlloc(alloc, key_suffix);
            defer alloc.free(key);
            var result = try self.client.putFile(self.bucket, key, local_path, .{
                .content_type = "application/octet-stream",
            });
            defer result.deinit(alloc);
        }
    }

    fn downloadDirectoryRecursive(self: *RemoteBackupStore, alloc: std.mem.Allocator, src_suffix: []const u8, dest_path: []const u8) !void {
        const key_prefix = try self.keyAlloc(alloc, src_suffix);
        defer alloc.free(key_prefix);

        var listed = try self.listObjects(alloc, src_suffix);
        defer listed.deinit(alloc);
        if (listed.entries.len == 0) return error.FileNotFound;

        for (listed.entries) |entry| {
            const rel = trimLeftSlash(entry.key[key_prefix.len..]);
            if (rel.len == 0) continue;
            const dest_file = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ dest_path, rel });
            defer alloc.free(dest_file);
            try self.client.getFile(self.bucket, entry.key, dest_file, .{});
        }
    }
};

const S3SecretOverrides = struct {
    endpoint: ?[]u8 = null,
    access_key_id: ?[]u8 = null,
    secret_access_key: ?[]u8 = null,
    session_token: ?[]u8 = null,
    region: ?[]u8 = null,

    fn deinit(self: *S3SecretOverrides, alloc: std.mem.Allocator) void {
        if (self.endpoint) |value| alloc.free(value);
        if (self.access_key_id) |value| alloc.free(value);
        if (self.secret_access_key) |value| alloc.free(value);
        if (self.session_token) |value| alloc.free(value);
        if (self.region) |value| alloc.free(value);
        self.* = undefined;
    }
};

fn loadS3SecretOverrides(alloc: std.mem.Allocator, secret_store: ?*common_secrets.FileStore) !S3SecretOverrides {
    const store = secret_store orelse return .{};
    return .{
        .endpoint = try firstStoredSecretOwned(alloc, store, &.{ "aws.endpoint_url", "AWS_ENDPOINT_URL" }),
        .access_key_id = try firstStoredSecretOwned(alloc, store, &.{ "aws.access_key_id", "AWS_ACCESS_KEY_ID" }),
        .secret_access_key = try firstStoredSecretOwned(alloc, store, &.{ "aws.secret_access_key", "AWS_SECRET_ACCESS_KEY" }),
        .session_token = try firstStoredSecretOwned(alloc, store, &.{ "aws.session_token", "AWS_SESSION_TOKEN" }),
        .region = try firstStoredSecretOwned(alloc, store, &.{ "aws.region", "AWS_REGION" }),
    };
}

fn firstStoredSecretOwned(
    alloc: std.mem.Allocator,
    store: *common_secrets.FileStore,
    keys: []const []const u8,
) !?[]u8 {
    for (keys) |key| {
        if (try store.getOwned(alloc, key)) |value| return value;
    }
    return null;
}

pub const ClusterTableBackupEntry = struct {
    name: []const u8,
    table_backup_id: []const u8,

    pub fn deinit(self: *ClusterTableBackupEntry, alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.name));
        alloc.free(@constCast(self.table_backup_id));
        self.* = undefined;
    }
};

pub const ClusterBackupManifest = struct {
    format_version: u32 = cluster_format_version,
    backup_id: []const u8,
    timestamp: []const u8,
    location: []const u8,
    antfly_version: []const u8,
    tables: []const ClusterTableBackupEntry,
    installed_extensions: []extension_domain.InstalledExtension = &.{},
    extension_members: []extension_domain.ExtensionMember = &.{},
    extension_dependencies: []extension_domain.ExtensionDependency = &.{},

    pub fn deinit(self: *ClusterBackupManifest, alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.backup_id));
        alloc.free(@constCast(self.timestamp));
        alloc.free(@constCast(self.location));
        alloc.free(@constCast(self.antfly_version));
        for (self.tables) |table| {
            var owned = table;
            owned.deinit(alloc);
        }
        alloc.free(@constCast(self.tables));
        for (self.installed_extensions) |extension| {
            var owned = extension;
            owned.deinitOwned(alloc);
        }
        if (self.installed_extensions.len > 0) alloc.free(@constCast(self.installed_extensions));
        for (self.extension_members) |member| {
            var owned = member;
            owned.deinitOwned(alloc);
        }
        if (self.extension_members.len > 0) alloc.free(@constCast(self.extension_members));
        for (self.extension_dependencies) |dependency| {
            var owned = dependency;
            owned.deinitOwned(alloc);
        }
        if (self.extension_dependencies.len > 0) alloc.free(@constCast(self.extension_dependencies));
        self.* = undefined;
    }
};

pub const ClusterTableBackupStatus = struct {
    name: []const u8,
    status: []const u8,
    @"error": ?[]const u8 = null,
};

pub const ClusterTableRestoreStatus = struct {
    name: []const u8,
    status: []const u8,
    @"error": ?[]const u8 = null,
};

pub const BackupInfo = struct {
    backup_id: []const u8,
    timestamp: []const u8,
    tables: []const []const u8,
    location: []const u8,
    antfly_version: []const u8,
};

pub fn openBackupLocation(alloc: std.mem.Allocator, location: []const u8) !BackupLocation {
    return try openBackupLocationWithSecrets(alloc, location, null);
}

pub fn openBackupLocationWithSecrets(
    alloc: std.mem.Allocator,
    location: []const u8,
    secret_store: ?*common_secrets.FileStore,
) !BackupLocation {
    if (std.mem.startsWith(u8, location, "file://")) {
        return .{ .file = try alloc.dupe(u8, try parseFileLocation(location)) };
    }
    if (std.mem.startsWith(u8, location, "s3://") or std.mem.startsWith(u8, location, "gs://") or std.mem.startsWith(u8, location, "gcs://")) {
        return .{ .remote = try RemoteBackupStore.initRemoteUri(alloc, location, secret_store) };
    }
    return error.UnsupportedBackupLocation;
}

pub fn backupLocationErrorMessage(err: anyerror) ?[]const u8 {
    return switch (err) {
        error.UnsupportedBackupLocation, error.UnsupportedRemoteUri => "unsupported backup location",
        error.InvalidBackupLocation, error.InvalidRemoteUri => "invalid backup location",
        error.MissingEndpoint => "missing S3-compatible endpoint; set AWS_ENDPOINT_URL for s3:// backups",
        error.MissingAccessKeyId => "missing S3-compatible access key; set AWS_ACCESS_KEY_ID for s3:// backups",
        error.MissingSecretAccessKey => "missing S3-compatible secret; set AWS_SECRET_ACCESS_KEY for s3:// backups",
        error.MissingServiceAccount => "missing GCS auth; set GCS_BEARER_TOKEN, GOOGLE_OAUTH_ACCESS_TOKEN, GOOGLE_SERVICE_ACCOUNT_JSON, or GOOGLE_APPLICATION_CREDENTIALS for gs:// backups",
        error.MissingProjectId => "missing GCS project id; set GOOGLE_CLOUD_PROJECT or GCLOUD_PROJECT for gs:// backups",
        else => null,
    };
}

pub fn parseBackupRequest(alloc: std.mem.Allocator, body: []const u8) !std.json.Parsed(BackupRequest) {
    return metadata_openapi.server.parseBackupTableBody(alloc, body);
}

pub fn parseRestoreRequest(alloc: std.mem.Allocator, body: []const u8) !std.json.Parsed(RestoreRequest) {
    return metadata_openapi.server.parseRestoreTableBody(alloc, body);
}

pub fn parseClusterBackupRequest(alloc: std.mem.Allocator, body: []const u8) !ClusterBackupRequest {
    var parsed = try metadata_openapi.server.parseBackupBody(alloc, body);
    defer parsed.deinit();
    return .{
        .backup_id = try alloc.dupe(u8, parsed.value.backup_id),
        .location = try alloc.dupe(u8, parsed.value.location),
        .table_names = try cloneOptionalStringSlice(alloc, parsed.value.table_names),
    };
}

pub fn parseClusterRestoreRequest(alloc: std.mem.Allocator, body: []const u8) !ClusterRestoreRequest {
    var parsed = try metadata_openapi.server.parseRestoreBody(alloc, body);
    defer parsed.deinit();
    return .{
        .backup_id = try alloc.dupe(u8, parsed.value.backup_id),
        .location = try alloc.dupe(u8, parsed.value.location),
        .table_names = try cloneOptionalStringSlice(alloc, parsed.value.table_names),
        .restore_mode = if (parsed.value.restore_mode) |value| try alloc.dupe(u8, value) else null,
    };
}

pub fn parseFileLocation(location: []const u8) ![]const u8 {
    if (!std.mem.startsWith(u8, location, "file://")) return error.UnsupportedBackupLocation;
    const path = location["file://".len..];
    if (path.len == 0 or path[0] != '/') return error.InvalidBackupLocation;
    return path;
}

pub fn createManifest(
    alloc: std.mem.Allocator,
    backup_id: []const u8,
    table: *const metadata_table_manager.TableRecord,
    shards: []const ShardSnapshot,
) !TableBackupManifest {
    const owned_shards = try alloc.alloc(ShardSnapshot, shards.len);
    var initialized: usize = 0;
    errdefer {
        for (owned_shards[0..initialized]) |shard| shard.deinit(alloc);
        alloc.free(owned_shards);
    }

    for (shards, 0..) |shard, i| {
        owned_shards[i] = .{
            .group_id = shard.group_id,
            .start_key = try alloc.dupe(u8, shard.start_key),
            .end_key = if (shard.end_key) |value| try alloc.dupe(u8, value) else null,
            .snapshot_path = try alloc.dupe(u8, shard.snapshot_path),
        };
        initialized += 1;
    }

    return .{
        .backup_id = try alloc.dupe(u8, backup_id),
        .table_name = try alloc.dupe(u8, table.name),
        .description = try alloc.dupe(u8, table.description),
        .schema_json = try alloc.dupe(u8, table.schema_json),
        .read_schema_json = try alloc.dupe(u8, table.read_schema_json),
        .indexes_json = try alloc.dupe(u8, table.indexes_json),
        .replication_sources_json = try alloc.dupe(u8, table.replication_sources_json),
        .shards = owned_shards,
    };
}

pub fn writeManifest(
    alloc: std.mem.Allocator,
    backup_root: []const u8,
    manifest: *const TableBackupManifest,
) !void {
    const path = try metadataPath(alloc, backup_root, manifest.backup_id);
    defer alloc.free(path);
    try ensureDirPath(backup_root);

    const encoded = try stringifyJsonAlloc(alloc, manifest.*);
    defer alloc.free(encoded);
    try writeFileAbsolute(path, encoded);
}

pub fn readManifest(
    alloc: std.mem.Allocator,
    backup_root: []const u8,
    backup_id: []const u8,
) !TableBackupManifest {
    const path = try metadataPath(alloc, backup_root, backup_id);
    defer alloc.free(path);
    const body = try readFileAbsoluteAlloc(alloc, path, 16 * 1024 * 1024);
    defer alloc.free(body);

    return parseTableBackupManifestOrPortable(alloc, body, backup_id);
}

pub fn writeManifestToLocation(
    alloc: std.mem.Allocator,
    location: *BackupLocation,
    manifest: *const TableBackupManifest,
) !void {
    switch (location.*) {
        .file => |backup_root| try writeManifest(alloc, backup_root, manifest),
        .remote => |*store| {
            const encoded = try stringifyJsonAlloc(alloc, manifest.*);
            defer alloc.free(encoded);
            const suffix = try metadataPath(alloc, "", manifest.backup_id);
            defer alloc.free(suffix);
            try store.writeBytes(alloc, trimLeftSlash(suffix), encoded, "application/json");
        },
    }
}

pub fn readManifestFromLocation(
    alloc: std.mem.Allocator,
    location: *BackupLocation,
    backup_id: []const u8,
) !TableBackupManifest {
    switch (location.*) {
        .file => |backup_root| return try readManifest(alloc, backup_root, backup_id),
        .remote => |*store| {
            const suffix = try metadataPath(alloc, "", backup_id);
            defer alloc.free(suffix);
            const body = try store.readBytesAlloc(alloc, trimLeftSlash(suffix));
            defer alloc.free(body);
            return parseTableBackupManifestOrPortable(alloc, body, backup_id);
        },
    }
}

fn parseTableBackupManifestOrPortable(
    alloc: std.mem.Allocator,
    body: []const u8,
    backup_id: []const u8,
) !TableBackupManifest {
    if (std.json.parseFromSlice(TableBackupManifest, alloc, body, .{ .allocate = .alloc_always })) |parsed| {
        defer parsed.deinit();
        if (parsed.value.format_version != format_version) return error.UnsupportedBackupFormat;
        return try cloneTableBackupManifest(alloc, parsed.value);
    } else |_| {
        return try parseGoPortableTableManifest(alloc, body, backup_id);
    }
}

const PortableShard = struct {
    group_id: u64,
    shard_id: []u8,
    start_key: []u8,
    end_key: ?[]u8,
    snapshot_path: []u8,

    fn deinit(self: PortableShard, alloc: std.mem.Allocator) void {
        alloc.free(self.shard_id);
        alloc.free(self.start_key);
        if (self.end_key) |end| alloc.free(end);
        alloc.free(self.snapshot_path);
    }
};

fn parseGoPortableTableManifest(
    alloc: std.mem.Allocator,
    body: []const u8,
    backup_id: []const u8,
) !TableBackupManifest {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, body, .{});
    defer parsed.deinit();
    const root = switch (parsed.value) {
        .object => |object| object,
        else => return error.InvalidBackupRequest,
    };
    const table_name = switch (root.get("name") orelse return error.InvalidBackupRequest) {
        .string => |value| value,
        else => return error.InvalidBackupRequest,
    };
    const schema_value = root.get("schema") orelse return error.InvalidBackupRequest;
    const indexes_json = try normalizeGoPortableIndexesJson(alloc, root.get("indexes"));
    errdefer alloc.free(indexes_json);
    const shards_value = switch (root.get("shards") orelse return error.InvalidBackupRequest) {
        .object => |object| object,
        else => return error.InvalidBackupRequest,
    };

    var shards_list = std.ArrayListUnmanaged(PortableShard).empty;
    defer {
        for (shards_list.items) |shard| shard.deinit(alloc);
        shards_list.deinit(alloc);
    }

    var it = shards_value.iterator();
    while (it.next()) |entry| {
        const shard_object = switch (entry.value_ptr.*) {
            .object => |object| object,
            else => return error.InvalidBackupRequest,
        };
        const raw_group_id = try std.fmt.parseInt(u64, entry.key_ptr.*, 16);
        const byte_range = switch (shard_object.get("byte_range") orelse return error.InvalidBackupRequest) {
            .array => |array| array,
            else => return error.InvalidBackupRequest,
        };
        if (byte_range.items.len != 2) return error.InvalidBackupRequest;
        const start_encoded = switch (byte_range.items[0]) {
            .string => |value| value,
            else => return error.InvalidBackupRequest,
        };
        const end_encoded = switch (byte_range.items[1]) {
            .string => |value| value,
            else => return error.InvalidBackupRequest,
        };
        const start_key = try decodePortableByteRangeBoundary(alloc, start_encoded);
        errdefer alloc.free(start_key);
        const end_key = if (end_encoded.len > 0) try decodePortableByteRangeBoundary(alloc, end_encoded) else null;
        errdefer if (end_key) |value| alloc.free(value);
        const snapshot_path = try std.fmt.allocPrint(alloc, "{s}-{s}.afb", .{ backup_id, entry.key_ptr.* });
        errdefer alloc.free(snapshot_path);
        try shards_list.append(alloc, .{
            .group_id = group_ids.dataGroupIdFromHash(raw_group_id),
            .shard_id = try alloc.dupe(u8, entry.key_ptr.*),
            .start_key = start_key,
            .end_key = end_key,
            .snapshot_path = snapshot_path,
        });
    }
    std.mem.sort(PortableShard, shards_list.items, {}, portableShardLessThan);

    const shards = try alloc.alloc(ShardSnapshot, shards_list.items.len);
    var initialized: usize = 0;
    errdefer {
        for (shards[0..initialized]) |shard| shard.deinit(alloc);
        alloc.free(shards);
    }
    for (shards_list.items, 0..) |portable_shard, i| {
        shards[i] = .{
            .group_id = portable_shard.group_id,
            .start_key = try alloc.dupe(u8, portable_shard.start_key),
            .end_key = if (portable_shard.end_key) |value| try alloc.dupe(u8, value) else null,
            .snapshot_path = try alloc.dupe(u8, portable_shard.snapshot_path),
        };
        initialized += 1;
    }

    return .{
        .backup_id = try alloc.dupe(u8, backup_id),
        .table_name = try alloc.dupe(u8, table_name),
        .description = try alloc.dupe(u8, ""),
        .schema_json = try stringifyJsonAlloc(alloc, schema_value),
        .read_schema_json = try alloc.dupe(u8, ""),
        .indexes_json = indexes_json,
        .replication_sources_json = try alloc.dupe(u8, "[]"),
        .shards = shards,
    };
}

fn normalizeGoPortableIndexesJson(alloc: std.mem.Allocator, maybe_indexes: ?std.json.Value) ![]u8 {
    const indexes = maybe_indexes orelse return try alloc.dupe(u8, "{}");
    const object = switch (indexes) {
        .object => |object| object,
        else => return error.InvalidBackupRequest,
    };

    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try out.append(alloc, '{');
    var first = true;
    var it = object.iterator();
    while (it.next()) |entry| {
        if (entry.value_ptr.* != .object) return error.InvalidBackupRequest;
        if (!first) try out.append(alloc, ',');
        first = false;
        try appendJsonString(alloc, &out, entry.key_ptr.*);
        try out.append(alloc, ':');
        try appendGoPortableIndexConfigJson(alloc, &out, entry.key_ptr.*, entry.value_ptr.*);
    }
    try out.append(alloc, '}');
    return try out.toOwnedSlice(alloc);
}

fn appendGoPortableIndexConfigJson(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    index_name: []const u8,
    value: std.json.Value,
) !void {
    const object = switch (value) {
        .object => |object| object,
        else => return error.InvalidBackupRequest,
    };

    var has_non_empty_name = false;
    if (object.get("name")) |name_value| {
        has_non_empty_name = switch (name_value) {
            .string => |name| name.len > 0,
            else => false,
        };
    }

    try out.append(alloc, '{');
    var first = true;
    var it = object.iterator();
    while (it.next()) |entry| {
        if (std.mem.eql(u8, entry.key_ptr.*, "name") and !has_non_empty_name) continue;
        if (!first) try out.append(alloc, ',');
        first = false;
        try appendJsonString(alloc, out, entry.key_ptr.*);
        try out.append(alloc, ':');
        try appendGoPortableJsonValue(alloc, out, entry.key_ptr.*, entry.value_ptr.*);
    }
    if (!has_non_empty_name) {
        if (!first) try out.append(alloc, ',');
        try appendJsonString(alloc, out, "name");
        try out.append(alloc, ':');
        try appendJsonString(alloc, out, index_name);
    }
    try out.append(alloc, '}');
}

fn appendGoPortableJsonValue(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    field_name: []const u8,
    value: std.json.Value,
) !void {
    switch (value) {
        .object => |object| {
            try out.append(alloc, '{');
            var first = true;
            var it = object.iterator();
            while (it.next()) |entry| {
                if (!first) try out.append(alloc, ',');
                first = false;
                try appendJsonString(alloc, out, entry.key_ptr.*);
                try out.append(alloc, ':');
                try appendGoPortableJsonValue(alloc, out, entry.key_ptr.*, entry.value_ptr.*);
            }
            try out.append(alloc, '}');
        },
        .array => |array| {
            try out.append(alloc, '[');
            for (array.items, 0..) |item, i| {
                if (i > 0) try out.append(alloc, ',');
                try appendGoPortableJsonValue(alloc, out, field_name, item);
            }
            try out.append(alloc, ']');
        },
        .string => |text| {
            // Antfly Go 0.1.x portable metadata used the old local inference
            // provider name "termite"; Zig's public index API calls the same
            // local inference provider "antfly".
            if (std.mem.eql(u8, field_name, "provider") and std.mem.eql(u8, text, "termite")) {
                try appendJsonString(alloc, out, "antfly");
            } else {
                try appendJsonString(alloc, out, text);
            }
        },
        else => {
            const encoded = try stringifyJsonAlloc(alloc, value);
            defer alloc.free(encoded);
            try out.appendSlice(alloc, encoded);
        },
    }
}

fn appendJsonString(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), value: []const u8) !void {
    const encoded = try stringifyJsonAlloc(alloc, value);
    defer alloc.free(encoded);
    try out.appendSlice(alloc, encoded);
}

fn portableShardLessThan(_: void, a: PortableShard, b: PortableShard) bool {
    const start_order = std.mem.order(u8, a.start_key, b.start_key);
    if (start_order != .eq) return start_order == .lt;
    return a.group_id < b.group_id;
}

fn decodePortableByteRangeBoundary(alloc: std.mem.Allocator, encoded: []const u8) ![]u8 {
    if (encoded.len == 0) return try alloc.dupe(u8, "");
    const size = try std.base64.standard.Decoder.calcSizeForSlice(encoded);
    const out = try alloc.alloc(u8, size);
    errdefer alloc.free(out);
    try std.base64.standard.Decoder.decode(out, encoded);
    return out;
}

pub fn metadataPath(alloc: std.mem.Allocator, backup_root: []const u8, backup_id: []const u8) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{s}/{s}-metadata.json", .{ backup_root, backup_id });
}

pub fn clusterMetadataPath(alloc: std.mem.Allocator, backup_root: []const u8, backup_id: []const u8) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{s}/{s}-cluster-metadata.json", .{ backup_root, backup_id });
}

pub fn shardSnapshotPath(alloc: std.mem.Allocator, backup_root: []const u8, backup_id: []const u8, group_id: u64) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{s}/{s}/groups/{d}", .{ backup_root, backup_id, group_id });
}

pub fn shardSnapshotRelPath(alloc: std.mem.Allocator, backup_id: []const u8, group_id: u64) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{s}/groups/{d}", .{ backup_id, group_id });
}

pub fn encodeBackupSuccess(alloc: std.mem.Allocator) ![]u8 {
    return try alloc.dupe(u8, "{\"backup\":\"successful\"}");
}

pub fn encodeRestoreTriggered(alloc: std.mem.Allocator) ![]u8 {
    return try alloc.dupe(u8, "{\"restore\":\"triggered\"}");
}

pub fn encodeRestoreDurabilityPending(alloc: std.mem.Allocator) ![]u8 {
    return try alloc.dupe(u8, "{\"restore\":\"committed\",\"durability\":\"pending\"}");
}

pub fn encodeRestoreDurabilityConfirmed(alloc: std.mem.Allocator) ![]u8 {
    return try alloc.dupe(u8, "{\"restore\":\"committed\",\"durability\":\"durable\"}");
}

pub fn clusterTableBackupId(alloc: std.mem.Allocator, cluster_backup_id: []const u8, table_name: []const u8) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{s}-{s}", .{ table_name, cluster_backup_id });
}

pub fn createClusterManifest(
    alloc: std.mem.Allocator,
    backup_id: []const u8,
    location: []const u8,
    table_entries: []const ClusterTableBackupEntry,
) !ClusterBackupManifest {
    return try createClusterManifestWithExtensions(alloc, backup_id, location, table_entries, &.{}, &.{}, &.{});
}

pub fn createClusterManifestWithExtensions(
    alloc: std.mem.Allocator,
    backup_id: []const u8,
    location: []const u8,
    table_entries: []const ClusterTableBackupEntry,
    installed_extensions: []const extension_domain.InstalledExtension,
    extension_members: []const extension_domain.ExtensionMember,
    extension_dependencies: []const extension_domain.ExtensionDependency,
) !ClusterBackupManifest {
    const owned_entries = try alloc.alloc(ClusterTableBackupEntry, table_entries.len);
    var initialized: usize = 0;
    errdefer {
        for (owned_entries[0..initialized]) |*entry| entry.deinit(alloc);
        alloc.free(owned_entries);
    }
    for (table_entries, 0..) |entry, i| {
        owned_entries[i] = .{
            .name = try alloc.dupe(u8, entry.name),
            .table_backup_id = try alloc.dupe(u8, entry.table_backup_id),
        };
        initialized += 1;
    }
    const owned_installed = try cloneInstalledExtensions(alloc, installed_extensions);
    errdefer freeInstalledExtensions(alloc, owned_installed);
    const owned_members = try cloneExtensionMembers(alloc, extension_members);
    errdefer freeExtensionMembers(alloc, owned_members);
    const owned_dependencies = try cloneExtensionDependencies(alloc, extension_dependencies);
    errdefer freeExtensionDependencies(alloc, owned_dependencies);

    return .{
        .backup_id = try alloc.dupe(u8, backup_id),
        .timestamp = try currentTimestampRfc3339(alloc),
        .location = try alloc.dupe(u8, location),
        .antfly_version = try alloc.dupe(u8, antfly_version),
        .tables = owned_entries,
        .installed_extensions = owned_installed,
        .extension_members = owned_members,
        .extension_dependencies = owned_dependencies,
    };
}

pub fn writeClusterManifest(
    alloc: std.mem.Allocator,
    backup_root: []const u8,
    manifest: *const ClusterBackupManifest,
) !void {
    const path = try clusterMetadataPath(alloc, backup_root, manifest.backup_id);
    defer alloc.free(path);
    try ensureDirPath(backup_root);

    const encoded = try stringifyJsonAlloc(alloc, manifest.*);
    defer alloc.free(encoded);
    try writeFileAbsolute(path, encoded);
}

pub fn readClusterManifest(
    alloc: std.mem.Allocator,
    backup_root: []const u8,
    backup_id: []const u8,
) !ClusterBackupManifest {
    const path = try clusterMetadataPath(alloc, backup_root, backup_id);
    defer alloc.free(path);
    const body = try readFileAbsoluteAlloc(alloc, path, 16 * 1024 * 1024);
    defer alloc.free(body);

    var parsed = try std.json.parseFromSlice(ClusterBackupManifest, alloc, body, .{ .allocate = .alloc_always });
    defer parsed.deinit();
    if (parsed.value.format_version != cluster_format_version) return error.UnsupportedBackupFormat;
    return try cloneClusterBackupManifest(alloc, parsed.value);
}

pub fn writeClusterManifestToLocation(
    alloc: std.mem.Allocator,
    location: *BackupLocation,
    manifest: *const ClusterBackupManifest,
) !void {
    switch (location.*) {
        .file => |backup_root| try writeClusterManifest(alloc, backup_root, manifest),
        .remote => |*store| {
            const encoded = try stringifyJsonAlloc(alloc, manifest.*);
            defer alloc.free(encoded);
            const suffix = try clusterMetadataPath(alloc, "", manifest.backup_id);
            defer alloc.free(suffix);
            try store.writeBytes(alloc, trimLeftSlash(suffix), encoded, "application/json");
        },
    }
}

pub fn readClusterManifestFromLocation(
    alloc: std.mem.Allocator,
    location: *BackupLocation,
    backup_id: []const u8,
) !ClusterBackupManifest {
    switch (location.*) {
        .file => |backup_root| return try readClusterManifest(alloc, backup_root, backup_id),
        .remote => |*store| {
            const suffix = try clusterMetadataPath(alloc, "", backup_id);
            defer alloc.free(suffix);
            const body = try store.readBytesAlloc(alloc, trimLeftSlash(suffix));
            defer alloc.free(body);
            var parsed = try std.json.parseFromSlice(ClusterBackupManifest, alloc, body, .{ .allocate = .alloc_always });
            defer parsed.deinit();
            if (parsed.value.format_version != cluster_format_version) return error.UnsupportedBackupFormat;
            return try cloneClusterBackupManifest(alloc, parsed.value);
        },
    }
}

pub fn encodeClusterBackupResponse(
    alloc: std.mem.Allocator,
    backup_id: []const u8,
    statuses: []const ClusterTableBackupStatus,
) ![]u8 {
    return try stringifyJsonAlloc(alloc, .{
        .backup_id = backup_id,
        .tables = statuses,
        .status = clusterBackupOverallStatus(statuses),
    });
}

pub fn encodeClusterRestoreResponse(
    alloc: std.mem.Allocator,
    statuses: []const ClusterTableRestoreStatus,
) ![]u8 {
    return try stringifyJsonAlloc(alloc, .{
        .tables = statuses,
        .status = clusterRestoreOverallStatus(statuses),
    });
}

pub fn encodeBackupListResponse(alloc: std.mem.Allocator, infos: []const BackupInfo) ![]u8 {
    return try stringifyJsonAlloc(alloc, .{ .backups = infos });
}

pub fn listClusterBackups(alloc: std.mem.Allocator, backup_root: []const u8, location: []const u8) ![]BackupInfo {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    var dir = std.Io.Dir.cwd().openDir(io, backup_root, .{ .iterate = true }) catch |err| switch (err) {
        error.FileNotFound => return try alloc.alloc(BackupInfo, 0),
        else => return err,
    };
    defer dir.close(io);

    var it = dir.iterate();
    var infos = std.ArrayListUnmanaged(BackupInfo).empty;
    errdefer {
        for (infos.items) |info| freeBackupInfo(alloc, info);
        infos.deinit(alloc);
    }

    while (try it.next(io)) |entry| {
        if (entry.kind != .file) continue;
        if (!std.mem.endsWith(u8, entry.name, "-cluster-metadata.json")) continue;
        const backup_id = entry.name[0 .. entry.name.len - "-cluster-metadata.json".len];
        var manifest = try readClusterManifest(alloc, backup_root, backup_id);
        defer manifest.deinit(alloc);

        const tables = try alloc.alloc([]const u8, manifest.tables.len);
        var initialized_tables: usize = 0;
        errdefer {
            for (tables[0..initialized_tables]) |value| alloc.free(@constCast(value));
            alloc.free(tables);
        }
        for (manifest.tables, 0..) |table, i| {
            tables[i] = try alloc.dupe(u8, table.name);
            initialized_tables += 1;
        }

        try infos.append(alloc, .{
            .backup_id = try alloc.dupe(u8, manifest.backup_id),
            .timestamp = try alloc.dupe(u8, manifest.timestamp),
            .tables = tables,
            .location = try alloc.dupe(u8, location),
            .antfly_version = try alloc.dupe(u8, manifest.antfly_version),
        });
    }

    return try infos.toOwnedSlice(alloc);
}

pub fn listClusterBackupsFromLocation(
    alloc: std.mem.Allocator,
    location_uri: []const u8,
) ![]BackupInfo {
    var location = try openBackupLocation(alloc, location_uri);
    defer location.deinit(alloc);

    if (location == .file) {
        return try listClusterBackups(alloc, location.file, location_uri);
    }

    return try listClusterBackupsFromOpenedLocation(alloc, &location, location_uri);
}

fn listClusterBackupsFromOpenedLocation(
    alloc: std.mem.Allocator,
    location: *BackupLocation,
    location_uri: []const u8,
) ![]BackupInfo {
    switch (location.*) {
        .remote => {},
        .file => unreachable,
    }

    var infos = std.ArrayListUnmanaged(BackupInfo).empty;
    errdefer {
        for (infos.items) |info| freeBackupInfo(alloc, info);
        infos.deinit(alloc);
    }

    var continuation_token: ?[]u8 = null;
    defer if (continuation_token) |token| alloc.free(token);
    while (true) {
        var listed = try location.remote.listTopLevelObjectsPage(alloc, continuation_token);
        defer listed.deinit(alloc);
        var next_token: ?[]u8 = null;
        defer if (next_token) |token| alloc.free(token);

        for (listed.entries) |entry| {
            if (!std.mem.endsWith(u8, entry.key, "-cluster-metadata.json")) continue;
            var manifest = try readClusterManifestFromLocation(alloc, location, backupIdFromClusterMetadataKey(entry.key));
            defer manifest.deinit(alloc);

            const tables = try alloc.alloc([]const u8, manifest.tables.len);
            var initialized_tables: usize = 0;
            errdefer {
                for (tables[0..initialized_tables]) |value| alloc.free(@constCast(value));
                alloc.free(tables);
            }
            for (manifest.tables, 0..) |table, i| {
                tables[i] = try alloc.dupe(u8, table.name);
                initialized_tables += 1;
            }

            try infos.append(alloc, .{
                .backup_id = try alloc.dupe(u8, manifest.backup_id),
                .timestamp = try alloc.dupe(u8, manifest.timestamp),
                .tables = tables,
                .location = try alloc.dupe(u8, location_uri),
                .antfly_version = try alloc.dupe(u8, manifest.antfly_version),
            });
        }

        if (listed.next_continuation_token) |token| {
            next_token = try alloc.dupe(u8, token);
        }

        if (continuation_token) |token| alloc.free(token);
        continuation_token = next_token;
        next_token = null;
        if (continuation_token == null) break;
    }

    return try infos.toOwnedSlice(alloc);
}

pub fn findClusterTable(
    manifest: *const ClusterBackupManifest,
    table_name: []const u8,
) ?*const ClusterTableBackupEntry {
    for (manifest.tables) |*table| {
        if (std.mem.eql(u8, table.name, table_name)) return table;
    }
    return null;
}

pub fn createTableRequestFromManifest(alloc: std.mem.Allocator, manifest: *const TableBackupManifest) !tables_api.CreateTableRequest {
    if (manifest.read_schema_json.len > 0) return error.UnsupportedBackupMigrationState;
    try tables_api.validateIndexesJson(alloc, manifest.indexes_json);
    return .{
        .description = if (manifest.description.len > 0) try alloc.dupe(u8, manifest.description) else null,
        .indexes_json = try alloc.dupe(u8, manifest.indexes_json),
        .schema_json = if (manifest.schema_json.len > 0) try alloc.dupe(u8, manifest.schema_json) else null,
        .replication_sources_json = if (manifest.replication_sources_json.len > 0) try alloc.dupe(u8, manifest.replication_sources_json) else null,
    };
}

pub fn deriveRestoreTableRecord(
    alloc: std.mem.Allocator,
    table_name: []const u8,
    location_uri: []const u8,
    manifest: *const TableBackupManifest,
) !metadata_table_manager.TableRecord {
    _ = location_uri;
    var req = try createTableRequestFromManifest(alloc, manifest);
    defer req.deinit(alloc);
    var table = try metadata_table_manager.cloneTable(alloc, tables_api.deriveTableRecord(table_name, req));
    table.min_ranges = @intCast(@max(manifest.shards.len, 1));
    return table;
}

pub fn deriveRestoreRanges(
    alloc: std.mem.Allocator,
    table_id: u64,
    location_uri: []const u8,
    manifest: *const TableBackupManifest,
) ![]metadata_table_manager.RangeRecord {
    if (manifest.shards.len == 0) return error.UnsupportedBackupFormat;
    const ranges = try alloc.alloc(metadata_table_manager.RangeRecord, manifest.shards.len);
    var initialized: usize = 0;
    errdefer {
        for (ranges[0..initialized]) |record| metadata_table_manager.freeRange(alloc, record);
        alloc.free(ranges);
    }
    for (manifest.shards, 0..) |shard, i| {
        if (!group_ids.isDataGroupId(shard.group_id)) return error.UnsupportedBackupFormat;
        ranges[i] = .{
            .group_id = shard.group_id,
            .table_id = table_id,
            .start_key = try alloc.dupe(u8, shard.start_key),
            .end_key = if (shard.end_key) |end| try alloc.dupe(u8, end) else null,
            .restore_backup_id = try alloc.dupe(u8, manifest.backup_id),
            .restore_location = try alloc.dupe(u8, location_uri),
            .restore_snapshot_path = try alloc.dupe(u8, shard.snapshot_path),
        };
        initialized += 1;
    }
    return ranges;
}

pub fn findShardSnapshot(manifest: *const TableBackupManifest, group_id: u64) ?*const ShardSnapshot {
    for (manifest.shards) |*shard| {
        if (shard.group_id == group_id) return shard;
    }
    return null;
}

pub fn copyDirectoryToLocation(
    alloc: std.mem.Allocator,
    location: *BackupLocation,
    backup_id: []const u8,
    group_id: u64,
    src_path: []const u8,
) !void {
    switch (location.*) {
        .file => |backup_root| {
            const dest_root = try shardSnapshotPath(alloc, backup_root, backup_id, group_id);
            defer alloc.free(dest_root);
            try copyDirectoryRecursive(alloc, src_path, dest_root);
        },
        .remote => |*store| {
            const dest_suffix = try shardSnapshotRelPath(alloc, backup_id, group_id);
            defer alloc.free(dest_suffix);
            try store.uploadDirectoryRecursive(alloc, src_path, dest_suffix);
        },
    }
}

pub fn copyDirectoryFromLocation(
    alloc: std.mem.Allocator,
    location: *BackupLocation,
    snapshot_path: []const u8,
    dest_path: []const u8,
) !void {
    switch (location.*) {
        .file => |backup_root| {
            const src_root = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ backup_root, snapshot_path });
            defer alloc.free(src_root);
            try copyDirectoryRecursive(alloc, src_root, dest_path);
        },
        .remote => |*store| try store.downloadDirectoryRecursive(alloc, snapshot_path, dest_path),
    }
}

pub fn copyFileFromLocation(
    alloc: std.mem.Allocator,
    location: *BackupLocation,
    snapshot_path: []const u8,
    dest_path: []const u8,
) !void {
    switch (location.*) {
        .file => |backup_root| {
            const src_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ backup_root, snapshot_path });
            defer alloc.free(src_path);
            try copyFileAbsolute(src_path, dest_path);
        },
        .remote => |*store| {
            const body = try store.readBytesAlloc(alloc, trimLeftSlash(snapshot_path));
            defer alloc.free(body);
            try writeFileAbsolute(dest_path, body);
        },
    }
}

pub fn copyFileToLocation(
    alloc: std.mem.Allocator,
    location: *BackupLocation,
    snapshot_path: []const u8,
    src_path: []const u8,
    content_type: []const u8,
) !void {
    switch (location.*) {
        .file => |backup_root| {
            const dest_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ backup_root, snapshot_path });
            defer alloc.free(dest_path);
            try copyFileAbsolute(src_path, dest_path);
        },
        .remote => |*store| {
            const body = try readFileAbsoluteAlloc(alloc, src_path, max_portable_backup_file_bytes);
            defer alloc.free(body);
            try store.writeBytes(alloc, trimLeftSlash(snapshot_path), body, content_type);
        },
    }
}

pub fn writeFileToLocation(
    alloc: std.mem.Allocator,
    location: *BackupLocation,
    snapshot_path: []const u8,
    body: []const u8,
    content_type: []const u8,
) !void {
    switch (location.*) {
        .file => |backup_root| {
            const dest_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ backup_root, snapshot_path });
            defer alloc.free(dest_path);
            try writeFileAbsolute(dest_path, body);
        },
        .remote => |*store| try store.writeBytes(alloc, trimLeftSlash(snapshot_path), body, content_type),
    }
}

fn cloneTableBackupManifest(alloc: std.mem.Allocator, manifest: TableBackupManifest) !TableBackupManifest {
    const shards = try alloc.alloc(ShardSnapshot, manifest.shards.len);
    var initialized_shards: usize = 0;
    errdefer {
        for (shards[0..initialized_shards]) |shard| shard.deinit(alloc);
        alloc.free(shards);
    }
    for (manifest.shards, 0..) |shard, i| {
        shards[i] = .{
            .group_id = shard.group_id,
            .start_key = try alloc.dupe(u8, shard.start_key),
            .end_key = if (shard.end_key) |value| try alloc.dupe(u8, value) else null,
            .snapshot_path = try alloc.dupe(u8, shard.snapshot_path),
        };
        initialized_shards += 1;
    }

    return .{
        .format_version = manifest.format_version,
        .backup_id = try alloc.dupe(u8, manifest.backup_id),
        .table_name = try alloc.dupe(u8, manifest.table_name),
        .description = try alloc.dupe(u8, manifest.description),
        .schema_json = try alloc.dupe(u8, manifest.schema_json),
        .read_schema_json = try alloc.dupe(u8, manifest.read_schema_json),
        .indexes_json = try alloc.dupe(u8, manifest.indexes_json),
        .replication_sources_json = try alloc.dupe(u8, manifest.replication_sources_json),
        .shards = shards,
    };
}

fn cloneClusterBackupManifest(alloc: std.mem.Allocator, manifest: ClusterBackupManifest) !ClusterBackupManifest {
    const tables = try alloc.alloc(ClusterTableBackupEntry, manifest.tables.len);
    var initialized_tables: usize = 0;
    errdefer {
        for (tables[0..initialized_tables]) |*table| table.deinit(alloc);
        alloc.free(tables);
    }
    for (manifest.tables, 0..) |table, i| {
        tables[i] = .{
            .name = try alloc.dupe(u8, table.name),
            .table_backup_id = try alloc.dupe(u8, table.table_backup_id),
        };
        initialized_tables += 1;
    }
    const installed_extensions = try cloneInstalledExtensions(alloc, manifest.installed_extensions);
    errdefer freeInstalledExtensions(alloc, installed_extensions);
    const extension_members = try cloneExtensionMembers(alloc, manifest.extension_members);
    errdefer freeExtensionMembers(alloc, extension_members);
    const extension_dependencies = try cloneExtensionDependencies(alloc, manifest.extension_dependencies);
    errdefer freeExtensionDependencies(alloc, extension_dependencies);

    return .{
        .format_version = manifest.format_version,
        .backup_id = try alloc.dupe(u8, manifest.backup_id),
        .timestamp = try alloc.dupe(u8, manifest.timestamp),
        .location = try alloc.dupe(u8, manifest.location),
        .antfly_version = try alloc.dupe(u8, manifest.antfly_version),
        .tables = tables,
        .installed_extensions = installed_extensions,
        .extension_members = extension_members,
        .extension_dependencies = extension_dependencies,
    };
}

fn cloneInstalledExtensions(alloc: std.mem.Allocator, extensions: []const extension_domain.InstalledExtension) ![]extension_domain.InstalledExtension {
    const out = try alloc.alloc(extension_domain.InstalledExtension, extensions.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*extension| extension.deinitOwned(alloc);
        alloc.free(out);
    }
    for (extensions, 0..) |extension, i| {
        out[i] = .{
            .name = try alloc.dupe(u8, extension.name),
            .package_name = try alloc.dupe(u8, extension.package_name),
            .package_version = try alloc.dupe(u8, extension.package_version),
            .package_digest = try alloc.dupe(u8, extension.package_digest),
            .scope = .{
                .kind = extension.scope.kind,
                .table_name = if (extension.scope.table_name.len > 0) try alloc.dupe(u8, extension.scope.table_name) else "",
            },
            .config_json = try alloc.dupe(u8, extension.config_json),
            .granted_capabilities = try cloneExtensionCapabilities(alloc, extension.granted_capabilities),
            .installed_at_epoch_ms = extension.installed_at_epoch_ms,
            .status = extension.status,
        };
        initialized += 1;
    }
    return out;
}

fn freeInstalledExtensions(alloc: std.mem.Allocator, extensions: []extension_domain.InstalledExtension) void {
    for (extensions) |*extension| extension.deinitOwned(alloc);
    if (extensions.len > 0) alloc.free(extensions);
}

fn cloneExtensionMembers(alloc: std.mem.Allocator, members: []const extension_domain.ExtensionMember) ![]extension_domain.ExtensionMember {
    const out = try alloc.alloc(extension_domain.ExtensionMember, members.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*member| member.deinitOwned(alloc);
        alloc.free(out);
    }
    for (members, 0..) |member, i| {
        out[i] = .{
            .extension_name = try alloc.dupe(u8, member.extension_name),
            .scope = .{
                .kind = member.scope.kind,
                .table_name = if (member.scope.table_name.len > 0) try alloc.dupe(u8, member.scope.table_name) else "",
            },
            .object_kind = member.object_kind,
            .object_name = try alloc.dupe(u8, member.object_name),
            .table_name = if (member.table_name.len > 0) try alloc.dupe(u8, member.table_name) else "",
            .shape_kind = member.shape_kind,
            .shape_name = if (member.shape_name.len > 0) try alloc.dupe(u8, member.shape_name) else "",
            .shape_version = if (member.shape_version.len > 0) try alloc.dupe(u8, member.shape_version) else "",
            .owner_metadata_json = try alloc.dupe(u8, member.owner_metadata_json),
        };
        initialized += 1;
    }
    return out;
}

fn freeExtensionMembers(alloc: std.mem.Allocator, members: []extension_domain.ExtensionMember) void {
    for (members) |*member| member.deinitOwned(alloc);
    if (members.len > 0) alloc.free(members);
}

fn cloneExtensionDependencies(alloc: std.mem.Allocator, dependencies: []const extension_domain.ExtensionDependency) ![]extension_domain.ExtensionDependency {
    const out = try alloc.alloc(extension_domain.ExtensionDependency, dependencies.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*dependency| dependency.deinitOwned(alloc);
        alloc.free(out);
    }
    for (dependencies, 0..) |dependency, i| {
        out[i] = try extension_domain.cloneExtensionDependencyAlloc(alloc, dependency);
        initialized += 1;
    }
    return out;
}

fn freeExtensionDependencies(alloc: std.mem.Allocator, dependencies: []extension_domain.ExtensionDependency) void {
    for (dependencies) |*dependency| dependency.deinitOwned(alloc);
    if (dependencies.len > 0) alloc.free(dependencies);
}

fn cloneExtensionCapabilities(alloc: std.mem.Allocator, capabilities: []const extension_domain.Capability) ![]extension_domain.Capability {
    const out = try alloc.alloc(extension_domain.Capability, capabilities.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |capability| {
            alloc.free(capability.name);
            if (capability.scope.len > 0) alloc.free(capability.scope);
        }
        alloc.free(out);
    }
    for (capabilities, 0..) |capability, i| {
        out[i] = .{
            .name = try alloc.dupe(u8, capability.name),
            .scope = if (capability.scope.len > 0) try alloc.dupe(u8, capability.scope) else "",
        };
        initialized += 1;
    }
    return out;
}

fn backupIdFromClusterMetadataKey(key: []const u8) []const u8 {
    const base = std.fs.path.basename(key);
    return base[0 .. base.len - "-cluster-metadata.json".len];
}

fn joinPathAlloc(alloc: std.mem.Allocator, left: []const u8, right: []const u8) ![]u8 {
    if (left.len == 0) return try alloc.dupe(u8, trimLeftSlash(right));
    if (right.len == 0) return try alloc.dupe(u8, left);
    return try std.fmt.allocPrint(alloc, "{s}/{s}", .{ trimRightSlash(left), trimLeftSlash(right) });
}

fn normalizeRemoteLocationAlloc(alloc: std.mem.Allocator, location: []const u8) ![]u8 {
    if (std.mem.startsWith(u8, location, "gcs://")) {
        return try std.fmt.allocPrint(alloc, "gs://{s}", .{location["gcs://".len..]});
    }
    return try alloc.dupe(u8, location);
}

fn trimLeftSlash(value: []const u8) []const u8 {
    var idx: usize = 0;
    while (idx < value.len and value[idx] == '/') : (idx += 1) {}
    return value[idx..];
}

fn trimRightSlash(value: []const u8) []const u8 {
    var end = value.len;
    while (end > 0 and value[end - 1] == '/') : (end -= 1) {}
    return value[0..end];
}

pub fn copyDirectoryRecursive(alloc: std.mem.Allocator, src_path: []const u8, dest_path: []const u8) !void {
    try ensureDirPath(dest_path);

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    var src_dir = try std.Io.Dir.cwd().openDir(io, src_path, .{ .iterate = true });
    defer src_dir.close(io);

    var walker = try src_dir.walk(alloc);
    defer walker.deinit();

    while (try walker.next(io)) |entry| {
        const src_entry_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ src_path, entry.path });
        defer alloc.free(src_entry_path);
        const dest_entry_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ dest_path, entry.path });
        defer alloc.free(dest_entry_path);

        switch (entry.kind) {
            .directory => try ensureDirPath(dest_entry_path),
            .file => try copyFileAbsolute(src_entry_path, dest_entry_path),
            else => return error.UnsupportedBackupArtifact,
        }
    }
}

fn writeFileAbsolute(path: []const u8, data: []const u8) !void {
    if (std.fs.path.dirname(path)) |dir_name| try ensureDirPath(dir_name);
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    var file = try fs_paths.createFilePortable(io, path, .{ .truncate = true });
    defer file.close(io);

    var buf: [1024]u8 = undefined;
    var writer = file.writer(io, &buf);
    try writer.interface.writeAll(data);
    try writer.end();
}

fn readFileAbsoluteAlloc(alloc: std.mem.Allocator, path: []const u8, max_bytes: usize) ![]u8 {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    var file = if (std.fs.path.isAbsolute(path))
        try std.Io.Dir.openFileAbsolute(io, path, .{})
    else
        try std.Io.Dir.cwd().openFile(io, path, .{});
    defer file.close(io);
    const stat = try file.stat(io);
    var reader: std.Io.File.Reader = .initSize(file, io, &.{}, stat.size);
    return try reader.interface.allocRemaining(alloc, .limited(max_bytes));
}

fn copyFileAbsolute(src_path: []const u8, dest_path: []const u8) !void {
    if (std.fs.path.dirname(dest_path)) |dir_name| try ensureDirPath(dir_name);

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    var src = if (std.fs.path.isAbsolute(src_path))
        try std.Io.Dir.openFileAbsolute(io, src_path, .{})
    else
        try std.Io.Dir.cwd().openFile(io, src_path, .{});
    defer src.close(io);
    const src_stat = try src.stat(io);

    var dest = try fs_paths.createFilePortable(io, dest_path, .{ .truncate = true });
    defer dest.close(io);

    var writer_buf: [1024]u8 = undefined;
    var writer = dest.writer(io, &writer_buf);
    var src_reader: std.Io.File.Reader = .initSize(src, io, &.{}, src_stat.size);
    _ = writer.interface.sendFileAll(&src_reader, .unlimited) catch |err| switch (err) {
        error.ReadFailed => return src_reader.err.?,
        error.WriteFailed => return writer.err.?,
    };
    try writer.flush();
}

fn ensureDirPath(path: []const u8) !void {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    try fs_paths.createDirPathPortable(io_impl.io(), path);
}

fn stringifyJsonAlloc(alloc: std.mem.Allocator, value: anytype) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(value, .{})});
}

fn cloneOptionalStringSlice(alloc: std.mem.Allocator, values: ?[]const []const u8) !?[]const []const u8 {
    const source = values orelse return null;
    const result = try alloc.alloc([]const u8, source.len);
    var initialized: usize = 0;
    errdefer {
        for (result[0..initialized]) |item| alloc.free(@constCast(item));
        alloc.free(result);
    }
    for (source, 0..) |item, i| {
        result[i] = try alloc.dupe(u8, item);
        initialized += 1;
    }
    return result;
}

pub fn freeClusterBackupRequest(alloc: std.mem.Allocator, req: *ClusterBackupRequest) void {
    alloc.free(req.backup_id);
    alloc.free(req.location);
    if (req.table_names) |values| freeStringSlice(alloc, values);
    req.* = undefined;
}

pub fn freeClusterRestoreRequest(alloc: std.mem.Allocator, req: *ClusterRestoreRequest) void {
    alloc.free(req.backup_id);
    alloc.free(req.location);
    if (req.table_names) |values| freeStringSlice(alloc, values);
    if (req.restore_mode) |value| alloc.free(value);
    req.* = undefined;
}

pub fn freeBackupInfo(alloc: std.mem.Allocator, info: BackupInfo) void {
    alloc.free(@constCast(info.backup_id));
    alloc.free(@constCast(info.timestamp));
    for (info.tables) |table| alloc.free(@constCast(table));
    alloc.free(info.tables);
    alloc.free(@constCast(info.location));
    alloc.free(@constCast(info.antfly_version));
}

pub fn freeBackupInfos(alloc: std.mem.Allocator, infos: []const BackupInfo) void {
    for (infos) |info| freeBackupInfo(alloc, info);
    alloc.free(@constCast(infos));
}

fn freeStringSlice(alloc: std.mem.Allocator, values: []const []const u8) void {
    for (values) |value| alloc.free(@constCast(value));
    alloc.free(@constCast(values));
}

pub fn validateClusterRestoreMode(mode: ?[]const u8) ![]const u8 {
    const selected = mode orelse "fail_if_exists";
    if (!std.mem.eql(u8, selected, "fail_if_exists") and
        !std.mem.eql(u8, selected, "skip_if_exists") and
        !std.mem.eql(u8, selected, "overwrite")) return error.InvalidBackupRequest;
    return selected;
}

fn clusterBackupOverallStatus(statuses: []const ClusterTableBackupStatus) []const u8 {
    var completed: usize = 0;
    var failed: usize = 0;
    for (statuses) |status| {
        if (std.mem.eql(u8, status.status, "completed")) completed += 1 else failed += 1;
    }
    if (completed == 0) return "failed";
    if (failed > 0) return "partial";
    return "completed";
}

fn clusterRestoreOverallStatus(statuses: []const ClusterTableRestoreStatus) []const u8 {
    var triggered: usize = 0;
    var failed: usize = 0;
    for (statuses) |status| {
        if (std.mem.eql(u8, status.status, "triggered")) {
            triggered += 1;
        } else if (std.mem.eql(u8, status.status, "failed")) {
            failed += 1;
        }
    }
    if (triggered == 0 and failed > 0) return "failed";
    if (failed > 0) return "partial";
    return "triggered";
}

fn currentTimestampRfc3339(alloc: std.mem.Allocator) ![]u8 {
    return try alloc.dupe(u8, "1970-01-01T00:00:00Z");
}

test "backup manifest round trips through metadata path" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/backup-manifest", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    const shards = [_]ShardSnapshot{
        .{
            .group_id = 7,
            .start_key = "",
            .end_key = null,
            .snapshot_path = "snap/groups/7",
        },
    };
    var manifest = try createManifest(
        std.testing.allocator,
        "snap",
        &.{
            .table_id = 1,
            .name = "docs",
            .description = "docs table",
            .schema_json = "{\"default_type\":\"doc\"}",
            .read_schema_json = "",
            .indexes_json = "{\"full_text_index_v0\":{\"type\":\"full_text\"}}",
            .replication_sources_json = "[]",
        },
        &shards,
    );
    defer manifest.deinit(std.testing.allocator);

    try writeManifest(std.testing.allocator, root, &manifest);

    var loaded = try readManifest(std.testing.allocator, root, "snap");
    defer loaded.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("snap", loaded.backup_id);
    try std.testing.expectEqualStrings("docs", loaded.table_name);
    try std.testing.expectEqual(@as(usize, 1), loaded.shards.len);
    try std.testing.expectEqual(@as(u64, 7), loaded.shards[0].group_id);
}

test "cluster backup manifest round trips extension metadata" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/cluster-backup-manifest", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    const tables = [_]ClusterTableBackupEntry{.{
        .name = "memories",
        .table_backup_id = "memories-snap",
    }};
    const installed = [_]extension_domain.InstalledExtension{.{
        .name = "memoryaf",
        .package_name = "memoryaf",
        .package_version = "1.0.0",
        .package_digest = "sha256:abc",
        .scope = .{ .kind = .table, .table_name = "memories" },
        .granted_capabilities = &.{.{ .name = "read:table", .scope = "memories" }},
        .status = .ready,
    }};
    const members = [_]extension_domain.ExtensionMember{
        .{
            .extension_name = "memoryaf",
            .scope = .{ .kind = .table, .table_name = "memories" },
            .object_kind = .data_shape,
            .object_name = "memory_record",
            .shape_kind = .document,
            .shape_version = "1",
            .owner_metadata_json = "{\"type\":\"object\"}",
        },
        .{
            .extension_name = "memoryaf",
            .scope = .{ .kind = .table, .table_name = "memories" },
            .object_kind = .generated_artifact,
            .object_name = "memory_embedding",
            .shape_name = "memory_embedding_shape",
            .shape_version = "2",
            .owner_metadata_json = "{\"kind\":\"embedding\"}",
        },
    };

    var manifest = try createClusterManifestWithExtensions(
        std.testing.allocator,
        "snap",
        "file:///tmp/backups",
        &tables,
        &installed,
        &members,
        &.{.{
            .extension_name = "memoryaf",
            .required_extension_name = "antfly_core",
            .package_name = "antfly_core",
            .version_requirement = ">=1.0.0",
        }},
    );
    defer manifest.deinit(std.testing.allocator);

    try writeClusterManifest(std.testing.allocator, root, &manifest);

    var loaded = try readClusterManifest(std.testing.allocator, root, "snap");
    defer loaded.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), loaded.installed_extensions.len);
    try std.testing.expectEqualStrings("memoryaf", loaded.installed_extensions[0].name);
    try std.testing.expectEqualStrings("sha256:abc", loaded.installed_extensions[0].package_digest);
    try std.testing.expectEqualStrings("memories", loaded.installed_extensions[0].scope.table_name);
    try std.testing.expectEqual(@as(usize, 1), loaded.installed_extensions[0].granted_capabilities.len);
    try std.testing.expectEqualStrings("read:table", loaded.installed_extensions[0].granted_capabilities[0].name);
    try std.testing.expectEqual(@as(usize, 2), loaded.extension_members.len);
    try std.testing.expectEqual(.data_shape, loaded.extension_members[0].object_kind);
    try std.testing.expectEqual(extension_domain.DataShapeKind.document, loaded.extension_members[0].shape_kind.?);
    try std.testing.expectEqualStrings("{\"type\":\"object\"}", loaded.extension_members[0].owner_metadata_json);
    try std.testing.expectEqual(.generated_artifact, loaded.extension_members[1].object_kind);
    try std.testing.expectEqualStrings("memory_embedding_shape", loaded.extension_members[1].shape_name);
    try std.testing.expectEqualStrings("2", loaded.extension_members[1].shape_version);
    try std.testing.expectEqual(@as(usize, 1), loaded.extension_dependencies.len);
    try std.testing.expectEqualStrings("antfly_core", loaded.extension_dependencies[0].package_name);
}

test "backup location parsing requires absolute file uri" {
    try std.testing.expectEqualStrings("/tmp/antfly-backup", try parseFileLocation("file:///tmp/antfly-backup"));
    try std.testing.expectError(error.UnsupportedBackupLocation, parseFileLocation("s3://bucket/path"));
    try std.testing.expectError(error.InvalidBackupLocation, parseFileLocation("file://relative"));
}

test "backup manifest round trips through remote objectstore location" {
    var memory = object_storage.MemoryObjectStorage.init(std.testing.allocator);
    defer memory.deinit();
    const client = memory.client();
    var location: BackupLocation = .{
        .remote = try RemoteBackupStore.initWithClient(std.testing.allocator, client, "bucket", "backups/prod"),
    };
    defer location.deinit(std.testing.allocator);

    const shards = [_]ShardSnapshot{
        .{
            .group_id = 7,
            .start_key = "",
            .end_key = null,
            .snapshot_path = "snap/groups/7",
        },
    };
    var manifest = try createManifest(
        std.testing.allocator,
        "snap",
        &.{
            .table_id = 1,
            .name = "docs",
            .description = "docs table",
            .schema_json = "{\"default_type\":\"doc\"}",
            .read_schema_json = "",
            .indexes_json = "{\"full_text_index_v0\":{\"type\":\"full_text\"}}",
            .replication_sources_json = "[]",
        },
        &shards,
    );
    defer manifest.deinit(std.testing.allocator);

    try writeManifestToLocation(std.testing.allocator, &location, &manifest);

    var loaded = try readManifestFromLocation(std.testing.allocator, &location, "snap");
    defer loaded.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("snap", loaded.backup_id);
    try std.testing.expectEqualStrings("docs", loaded.table_name);
    try std.testing.expectEqual(@as(usize, 1), loaded.shards.len);
    try std.testing.expectEqual(@as(u64, 7), loaded.shards[0].group_id);
}

test "cluster backup list uses top-level remote manifests without recursing into payloads" {
    var memory = object_storage.MemoryObjectStorage.init(std.testing.allocator);
    defer memory.deinit();
    const client = memory.client();
    var location: BackupLocation = .{
        .remote = try RemoteBackupStore.initWithClient(std.testing.allocator, client, "bucket", "backups/prod"),
    };
    defer location.deinit(std.testing.allocator);

    const entries = [_]ClusterTableBackupEntry{
        .{ .name = "docs", .table_backup_id = "docs-prod-snap" },
    };
    var manifest = try createClusterManifest(std.testing.allocator, "prod-snap", "s3://bucket/backups/prod", &entries);
    defer manifest.deinit(std.testing.allocator);
    try writeClusterManifestToLocation(std.testing.allocator, &location, &manifest);

    var raw_client = memory.client();
    var nested = try raw_client.putObject(
        "bucket",
        "backups/prod/prod-snap/groups/7/table-file.tbl",
        "payload",
        .{ .content_type = "application/octet-stream" },
    );
    defer nested.deinit(std.testing.allocator);

    const listed = try listClusterBackupsFromOpenedLocation(std.testing.allocator, &location, "s3://bucket/backups/prod");
    defer {
        for (listed) |info| freeBackupInfo(std.testing.allocator, info);
        std.testing.allocator.free(listed);
    }

    try std.testing.expectEqual(@as(usize, 1), listed.len);
    try std.testing.expectEqualStrings("prod-snap", listed[0].backup_id);
    try std.testing.expectEqual(@as(usize, 1), listed[0].tables.len);
    try std.testing.expectEqualStrings("docs", listed[0].tables[0]);
}

test "go portable metadata parses as table backup manifest" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/go-portable-manifest", .{tmp.sub_path});
    defer std.testing.allocator.free(root);
    try ensureDirPath(root);

    const backup_id = "portable-snap";
    const path = try metadataPath(std.testing.allocator, root, backup_id);
    defer std.testing.allocator.free(path);
    try writeFileAbsolute(path,
        \\{
        \\  "name": "docs",
        \\  "schema": {"default_type":"doc"},
        \\  "indexes": {"legacy_vec":{"type":"embeddings","embedder":{"provider":"termite"}}},
        \\  "shards": {
        \\    "0000000000000001": {"byte_range":["","Qw=="]}
        \\  }
        \\}
    );

    var manifest = try readManifest(std.testing.allocator, root, backup_id);
    defer manifest.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings(backup_id, manifest.backup_id);
    try std.testing.expectEqualStrings("docs", manifest.table_name);
    try std.testing.expectEqualStrings("{\"default_type\":\"doc\"}", manifest.schema_json);
    try std.testing.expectEqualStrings("{\"legacy_vec\":{\"type\":\"embeddings\",\"embedder\":{\"provider\":\"antfly\"},\"name\":\"legacy_vec\"}}", manifest.indexes_json);
    try std.testing.expectEqual(@as(usize, 1), manifest.shards.len);
    try std.testing.expectEqual(group_ids.dataGroupIdFromHash(1), manifest.shards[0].group_id);
    try std.testing.expectEqualStrings("", manifest.shards[0].start_key);
    try std.testing.expectEqualStrings("C", manifest.shards[0].end_key.?);
    try std.testing.expectEqualStrings("portable-snap-0000000000000001.afb", manifest.shards[0].snapshot_path);
}

test "backup remote location normalizes gcs alias" {
    const normalized = try normalizeRemoteLocationAlloc(std.testing.allocator, "gcs://bucket/path");
    defer std.testing.allocator.free(normalized);
    try std.testing.expectEqualStrings("gs://bucket/path", normalized);
}

test "derive restore table record returns owned table metadata" {
    const manifest = TableBackupManifest{
        .backup_id = "snap",
        .table_name = "docs",
        .description = "docs table",
        .schema_json = "{\"default_type\":\"doc\"}",
        .read_schema_json = "",
        .indexes_json = "{\"full_text_index_v0\":{\"type\":\"full_text\"}}",
        .replication_sources_json = "[]",
        .shards = &.{
            .{
                .group_id = 7,
                .start_key = "",
                .end_key = null,
                .snapshot_path = "snap/groups/7",
            },
        },
    };

    const table = try deriveRestoreTableRecord(std.testing.allocator, "docs_restored", "file:///tmp/out", &manifest);
    defer metadata_table_manager.freeTable(std.testing.allocator, table);

    try std.testing.expectEqualStrings("docs_restored", table.name);
    try std.testing.expectEqualStrings("{\"default_type\":\"doc\"}", table.schema_json);
    try std.testing.expectEqualStrings("{\"full_text_index_v0\":{\"type\":\"full_text\"}}", table.indexes_json);
    try std.testing.expectEqual(@as(u32, 1), table.min_ranges);
}
