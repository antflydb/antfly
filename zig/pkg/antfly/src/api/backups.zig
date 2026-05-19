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
};

pub const TableRestorePlan = struct {
    backup_root: []const u8,
    manifest: *const TableBackupManifest,
};

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

    fn listObjects(self: *RemoteBackupStore, alloc: std.mem.Allocator, suffix: []const u8) !object_storage.ListResult {
        if (!(try self.client.bucketExists(self.bucket))) {
            return .{
                .entries = try alloc.alloc(object_storage.ListEntry, 0),
                .common_prefixes = try alloc.alloc([]u8, 0),
            };
        }
        const key_prefix = try self.keyAlloc(alloc, suffix);
        defer alloc.free(key_prefix);
        return try self.client.listObjects(self.bucket, .{
            .prefix = key_prefix,
            .recursive = true,
            .max_keys = 10_000,
        });
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

    var parsed = try std.json.parseFromSlice(TableBackupManifest, alloc, body, .{ .allocate = .alloc_always });
    defer parsed.deinit();
    if (parsed.value.format_version != format_version) return error.UnsupportedBackupFormat;
    return try cloneTableBackupManifest(alloc, parsed.value);
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
            var parsed = try std.json.parseFromSlice(TableBackupManifest, alloc, body, .{ .allocate = .alloc_always });
            defer parsed.deinit();
            if (parsed.value.format_version != format_version) return error.UnsupportedBackupFormat;
            return try cloneTableBackupManifest(alloc, parsed.value);
        },
    }
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

pub fn clusterTableBackupId(alloc: std.mem.Allocator, cluster_backup_id: []const u8, table_name: []const u8) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{s}-{s}", .{ table_name, cluster_backup_id });
}

pub fn createClusterManifest(
    alloc: std.mem.Allocator,
    backup_id: []const u8,
    location: []const u8,
    table_entries: []const ClusterTableBackupEntry,
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

    return .{
        .backup_id = try alloc.dupe(u8, backup_id),
        .timestamp = try currentTimestampRfc3339(alloc),
        .location = try alloc.dupe(u8, location),
        .antfly_version = try alloc.dupe(u8, antfly_version),
        .tables = owned_entries,
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

    var listed = try location.remote.listObjects(alloc, "");
    defer listed.deinit(alloc);

    var infos = std.ArrayListUnmanaged(BackupInfo).empty;
    errdefer {
        for (infos.items) |info| freeBackupInfo(alloc, info);
        infos.deinit(alloc);
    }

    for (listed.entries) |entry| {
        if (!std.mem.endsWith(u8, entry.key, "-cluster-metadata.json")) continue;
        var manifest = try readClusterManifestFromLocation(alloc, &location, backupIdFromClusterMetadataKey(entry.key));
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

    return .{
        .format_version = manifest.format_version,
        .backup_id = try alloc.dupe(u8, manifest.backup_id),
        .timestamp = try alloc.dupe(u8, manifest.timestamp),
        .location = try alloc.dupe(u8, manifest.location),
        .antfly_version = try alloc.dupe(u8, manifest.antfly_version),
        .tables = tables,
    };
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
