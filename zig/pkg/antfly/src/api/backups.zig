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
const platform_time = @import("antfly_platform").time;
const metadata_openapi = @import("antfly_metadata_openapi");
const fs_paths = @import("../common/fs_paths.zig");
const group_ids = @import("../common/group_ids.zig");
const metadata_table_manager = @import("../metadata/table_manager.zig");
const object_storage = @import("../storage/object_storage.zig");
const remote_uri = @import("../serverless/remote_uri.zig");
const tables_api = @import("tables.zig");
const common_secrets = @import("../common/secrets.zig");
const common_config = @import("../common/config.zig");
const bedrock = @import("../inference/bedrock.zig");
const httpx = @import("httpx");
const extension_domain = @import("../extensions/mod.zig");
const google_auth = @import("antfly_google").auth;

pub const BackupRequest = metadata_openapi.BackupRequest;
pub const RestoreRequest = metadata_openapi.RestoreRequest;
pub const ClusterBackupRequest = struct {
    backup_id: []const u8,
    location: []const u8,
    connection: ?[]const u8 = null,
    format: BackupFormat = .portable,
    table_names: ?[]const []const u8 = null,
};
pub const ClusterRestoreRequest = struct {
    backup_id: []const u8,
    location: []const u8,
    connection: ?[]const u8 = null,
    table_names: ?[]const []const u8 = null,
    restore_mode: ?[]const u8 = null,
};

pub const format_version: u32 = 2;
pub const cluster_format_version: u32 = 2;
pub const table_backup_id = "table";
pub const antfly_version = "zig-dev";
pub const max_portable_backup_file_bytes: usize = 1024 * 1024 * 1024;
pub const max_backup_manifest_bytes: usize = 16 * 1024 * 1024;
pub const max_backup_attempt_marker_bytes: usize = 2 * 1024 * 1024;
pub const max_cluster_backup_attempt_tables: usize = 4096;
pub const backup_attempt_marker_version: u32 = 1;
pub const backup_attempt_head_version: u32 = 1;
pub const backup_attempt_reclaim_age_ns: u64 = 24 * std.time.ns_per_hour;
pub const backup_attempt_reclaim_batch_size: usize = 2;
pub const backup_attempt_reclaim_scan_budget: usize = 64;
pub const backup_attempt_health_scan_budget: usize = 10_000;
pub const backup_attempt_reclaim_object_budget: usize = 10_000;
pub const backup_attempt_cleanup_object_budget: usize = 1_000_000;
pub const backup_integrity_read_chunk_bytes: u64 = 8 * 1024 * 1024;
pub const backup_integrity_max_native_files: usize = 1_000_000;
const incomplete_backup_prefix = ".antfly-incomplete";
const backup_attempt_head_name = ".antfly-backup-attempt-head.json";
pub const manifest_too_large_message = "backup manifest exceeds 16 MiB limit";
pub const integrity_failure_message = "backup artifact failed integrity verification";

pub fn isArtifactIntegrityError(err: anyerror) bool {
    return switch (err) {
        error.BackupIntegrityMissing,
        error.BackupArtifactMissing,
        error.BackupArtifactFormatMismatch,
        error.BackupArtifactIntegrityMismatch,
        error.RestoreArtifactIdentityMissing,
        error.RestoreArtifactIdentityMismatch,
        error.SourceFileChanged,
        => true,
        else => false,
    };
}
pub const default_backup_list_limit: usize = 100;
pub const max_backup_list_limit: usize = 1000;

pub const BackupListOptions = struct {
    limit: usize = default_backup_list_limit,
    cursor: ?[]const u8 = null,
};

pub const BackupListPage = struct {
    backups: []BackupInfo,
    next_cursor: ?[]u8 = null,

    pub fn deinit(self: *BackupListPage, alloc: std.mem.Allocator) void {
        freeBackupInfos(alloc, self.backups);
        if (self.next_cursor) |cursor| alloc.free(cursor);
        self.* = undefined;
    }
};

pub const BackupFormat = enum {
    native,
    portable,
};

pub const ArtifactIntegrityMode = enum {
    declared,
    derive_after_materialization,
};

pub const TableBackupManifest = struct {
    format_version: u32 = format_version,
    format: BackupFormat,
    artifact_integrity_mode: ArtifactIntegrityMode = .declared,
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
    artifact_size_bytes: u64 = 0,
    artifact_sha256: []const u8 = "",

    pub fn deinit(self: ShardSnapshot, alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.start_key));
        if (self.end_key) |value| alloc.free(@constCast(value));
        alloc.free(@constCast(self.snapshot_path));
        if (self.artifact_sha256.len > 0) alloc.free(@constCast(self.artifact_sha256));
    }
};

pub const ArtifactIntegrity = struct {
    size_bytes: u64,
    sha256: []u8,

    pub fn deinit(self: *ArtifactIntegrity, alloc: std.mem.Allocator) void {
        alloc.free(self.sha256);
        self.* = undefined;
    }
};

pub const TableBackupPlan = struct {
    backup_root: []const u8,
    backup_id: []const u8,
    format: BackupFormat = .native,
    io: ?std.Io = null,
};

pub const TableRestorePlan = struct {
    backup_root: []const u8,
    manifest: *const TableBackupManifest,
    /// Immutable storage identity of this table artifact. This can differ
    /// from the logical backup ID for a table inside a cluster backup.
    artifact_backup_id: []const u8,
    /// Stable location of the admitted backup source. The storage boundary
    /// canonicalizes this into the durable restore idempotency identity after
    /// a remote artifact is copied into a private local staging root.
    source_location: []const u8,
    reconcile_only: bool = false,
    replace_existing: bool = false,
    publication_hook: ?RestorePublicationHook = null,
    /// Borrowed server/backend runtime for positional archive I/O. Embedded
    /// callers may omit this and use the bounded threaded fallback.
    io: ?std.Io = null,
};

pub const max_restore_source_identity_bytes: usize = 4096;

/// Produces the bounded, canonical identity persisted with a restored
/// generation. Canonicalization makes equivalent accepted spellings (such as
/// gcs:// and gs://, redundant file path components, or trailing object-store
/// separators) share one idempotency key.
pub fn canonicalRestoreSourceIdentityAlloc(alloc: std.mem.Allocator, raw: []const u8) ![]u8 {
    if (raw.len == 0 or raw.len > max_restore_source_identity_bytes)
        return error.InvalidBackupRequest;
    for (raw) |byte| {
        if (byte < 0x20 or byte == 0x7f) return error.InvalidBackupRequest;
    }
    if (std.mem.indexOfAny(u8, raw, "?#") != null) return error.InvalidBackupRequest;

    const normalized = normalizeRemoteLocationAlloc(alloc, raw) catch |err| switch (err) {
        error.OutOfMemory => return err,
    };
    defer alloc.free(normalized);
    _ = std.Uri.parse(normalized) catch return error.InvalidBackupRequest;

    var parsed = remote_uri.parseAlloc(alloc, normalized) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.InvalidBackupRequest,
    };
    defer switch (parsed) {
        .file => |path| alloc.free(path),
        .gcs => |*value| value.deinit(alloc),
        .s3 => |*value| value.deinit(alloc),
    };
    return switch (parsed) {
        .file => |path| blk: {
            if (!std.fs.path.isAbsolute(path)) return error.InvalidBackupRequest;
            const canonical_path = std.fs.path.resolve(alloc, &.{path}) catch |err| switch (err) {
                error.OutOfMemory => return err,
            };
            defer alloc.free(canonical_path);
            break :blk std.fmt.allocPrint(alloc, "file://{s}", .{canonical_path});
        },
        .gcs => |value| canonicalObjectStoreLocationAlloc(alloc, "gs", value),
        .s3 => |value| canonicalObjectStoreLocationAlloc(alloc, "s3", value),
    };
}

fn canonicalObjectStoreLocationAlloc(
    alloc: std.mem.Allocator,
    scheme: []const u8,
    location: remote_uri.BucketPath,
) ![]u8 {
    const prefix = trimRightSlash(location.prefix);
    if (prefix.len == 0) return try std.fmt.allocPrint(alloc, "{s}://{s}", .{ scheme, location.bucket });
    return try std.fmt.allocPrint(alloc, "{s}://{s}/{s}", .{ scheme, location.bucket, prefix });
}

pub fn validateCanonicalRestoreSourceIdentity(
    alloc: std.mem.Allocator,
    identity: []const u8,
) !void {
    const canonical = try canonicalRestoreSourceIdentityAlloc(alloc, identity);
    defer alloc.free(canonical);
    if (!std.mem.eql(u8, identity, canonical)) return error.InvalidBackupRequest;
}

pub const RestorePublicationHook = struct {
    ptr: *anyopaque,
    publish_definition: *const fn (ptr: *anyopaque) anyerror!void,
    rollback_definition: *const fn (ptr: *anyopaque) anyerror!void,

    pub fn publish(self: @This()) !void {
        try self.publish_definition(self.ptr);
    }

    pub fn rollback(self: @This()) !void {
        try self.rollback_definition(self.ptr);
    }
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

pub const OpenOptions = struct {
    secret_store: ?*common_secrets.FileStore = null,
    node_config: ?*const common_config.Config = null,
    connection: ?[]const u8 = null,
    required_capability: []const u8 = "",
    /// Borrow the server's shared backend runtime for dynamic credential HTTP
    /// refresh. CLI and embedded callers may omit it and receive an owned
    /// threaded fallback.
    io: ?std.Io = null,
};

const AwsCredentialContext = struct {
    alloc: std.mem.Allocator,
    io_impl: ?*std.Io.Threaded,
    http: httpx.Client,
    cache: bedrock.CredentialCache = .{},
    region: []u8,
    source: bedrock.CredentialSource,

    fn init(alloc: std.mem.Allocator, region: []const u8, source: bedrock.CredentialSource, shared_io: ?std.Io) !AwsCredentialContext {
        const owned_region = try alloc.dupe(u8, region);
        errdefer alloc.free(owned_region);
        const io_impl: ?*std.Io.Threaded = if (shared_io == null) blk: {
            const owned = try alloc.create(std.Io.Threaded);
            owned.* = std.Io.Threaded.init(alloc, .{});
            break :blk owned;
        } else null;
        errdefer if (io_impl) |owned| {
            owned.deinit();
            alloc.destroy(owned);
        };
        return .{
            .alloc = alloc,
            .io_impl = io_impl,
            .http = httpx.Client.init(alloc, shared_io orelse io_impl.?.io()),
            .region = owned_region,
            .source = source,
        };
    }

    fn deinit(self: *AwsCredentialContext) void {
        self.cache.deinit(self.alloc);
        self.http.deinit();
        if (self.io_impl) |io_impl| {
            io_impl.deinit();
            self.alloc.destroy(io_impl);
        }
        self.alloc.free(self.region);
        self.* = undefined;
    }

    fn provider(self: *AwsCredentialContext) object_storage.S3.CredentialProvider {
        return .{ .ptr = self, .get_fn = get };
    }

    fn get(ptr: *anyopaque, alloc: std.mem.Allocator) anyerror!object_storage.S3.DynamicCredentials {
        const self: *AwsCredentialContext = @ptrCast(@alignCast(ptr));
        _ = alloc;
        const lease = try self.cache.getLeaseForSource(self.alloc, &self.http, self.region, self.source);
        const credentials = lease.credentials();
        return .{
            .access_key_id = @constCast(credentials.access_key_id),
            .secret_access_key = @constCast(credentials.secret_access_key),
            .session_token = if (credentials.session_token) |value| @constCast(value) else null,
            .ownership = .{ .borrowed = .{
                .ctx = lease.releaseContext(),
                .release = bedrock.CredentialCache.Lease.releaseOpaque,
            } },
        };
    }
};

fn authorizedObjectConnection(
    config: *const common_config.Config,
    connection_id: []const u8,
    protocol: common_config.Config.ExternalIoProtocol,
    bucket: []const u8,
    raw_prefix: []const u8,
    required_capability: []const u8,
) !common_config.Config.ExternalIoConnectionConfig {
    const connection = config.connections.get(connection_id) orelse return error.ConnectionNotFound;
    if (connection.kind != .external_io) return error.ConnectionKindMismatch;
    const external = connection.external_io orelse return error.ConnectionKindMismatch;
    if (external.protocol != protocol) return error.ConnectionProtocolMismatch;
    var capability_allowed = false;
    for (connection.capabilities) |capability| {
        if (std.mem.eql(u8, capability, required_capability)) {
            capability_allowed = true;
            break;
        }
    }
    if (!capability_allowed) return error.ConnectionCapabilityDenied;
    if (external.buckets.len == 0) return error.ConnectionBucketDenied;
    var bucket_allowed = false;
    for (external.buckets) |allowed| {
        if (std.mem.eql(u8, allowed, bucket)) {
            bucket_allowed = true;
            break;
        }
    }
    if (!bucket_allowed) return error.ConnectionBucketDenied;
    if (external.prefix) |scope_raw| {
        const scope = std.mem.trim(u8, scope_raw, "/");
        const prefix = std.mem.trim(u8, raw_prefix, "/");
        if (scope.len > 0 and !(std.mem.eql(u8, scope, prefix) or (prefix.len > scope.len and std.mem.startsWith(u8, prefix, scope) and prefix[scope.len] == '/'))) {
            return error.ConnectionPrefixDenied;
        }
    }
    return external;
}

fn authorizedFilesystemConnection(
    config: *const common_config.Config,
    connection_id: []const u8,
    required_capability: []const u8,
) !common_config.Config.ExternalIoConnectionConfig {
    const connection = config.connections.get(connection_id) orelse return error.ConnectionNotFound;
    if (connection.kind != .external_io) return error.ConnectionKindMismatch;
    const external = connection.external_io orelse return error.ConnectionKindMismatch;
    if (external.protocol != .filesystem or external.root == null) return error.ConnectionProtocolMismatch;
    for (connection.capabilities) |capability| {
        if (std.mem.eql(u8, capability, required_capability)) return external;
    }
    return error.ConnectionCapabilityDenied;
}

fn s3ConfigForConnection(
    alloc: std.mem.Allocator,
    external: common_config.Config.ExternalIoConnectionConfig,
    credential_context: *?*AwsCredentialContext,
    io: ?std.Io,
) !object_storage.S3.Config {
    const static = external.credentials.source == .static;
    var cfg = try object_storage.S3.fromEnvAlloc(
        alloc,
        external.endpoint,
        external.use_ssl orelse true,
        if (static) external.credentials.access_key_id else "dynamic-provider",
        if (static) external.credentials.secret_access_key else "dynamic-provider",
        if (static) external.credentials.session_token else null,
        external.region,
        switch (external.addressing_style) {
            .path => .path,
            .virtual_hosted => .virtual_hosted,
        },
    );
    errdefer cfg.deinit(alloc);
    if (static) return cfg;

    const source: bedrock.CredentialSource = switch (external.credentials.source) {
        .default => .default,
        .static => unreachable,
        .profile => .{ .profile = .{
            .name = external.credentials.profile orelse return error.InvalidConnectionCredentials,
            .shared_credentials_file = external.credentials.shared_credentials_file,
        } },
        .web_identity => .{ .web_identity = .{
            .role_arn = external.credentials.role_arn orelse return error.InvalidConnectionCredentials,
            .token_file = external.credentials.token_file orelse return error.InvalidConnectionCredentials,
            .session_name = external.credentials.session_name orelse "antfly-backup",
            .sts_endpoint = external.credentials.sts_endpoint,
        } },
    };
    const context = try alloc.create(AwsCredentialContext);
    errdefer alloc.destroy(context);
    context.* = try AwsCredentialContext.init(alloc, cfg.credentials.region, source, io);
    credential_context.* = context;
    cfg.credential_provider = context.provider();
    return cfg;
}

const RemoteBackupStore = struct {
    const BoundedReadOptions = struct {
        known_size: ?u64 = null,
        skip_metadata_probe: bool = false,
    };

    alloc: std.mem.Allocator,
    io_impl: ?*std.Io.Threaded = null,
    io: std.Io,
    client: object_storage.ObjectStorage,
    gcs_client: ?*object_storage.Gcs.JsonApiClient = null,
    s3_client: ?*object_storage.S3.Client = null,
    credential_context: ?*AwsCredentialContext = null,
    resolved_credentials: ?common_config.Config.ResolvedExternalIoCredentials = null,
    owns_client: bool = true,
    create_bucket_if_missing: bool = false,
    bucket_ready: std.atomic.Value(bool) = .init(false),
    bucket: []u8,
    prefix: []u8,

    fn initRemoteUri(alloc: std.mem.Allocator, location: []const u8, options: OpenOptions) !RemoteBackupStore {
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
            .gcs => |value| try initGcsUri(alloc, value.bucket, value.prefix, options),
            .s3 => |value| try initS3Uri(alloc, value.bucket, value.prefix, options),
        };
    }

    fn initGcsUri(alloc: std.mem.Allocator, bucket: []const u8, prefix: []const u8, options: OpenOptions) !RemoteBackupStore {
        const io_impl: ?*std.Io.Threaded = if (options.io == null) blk: {
            const owned = try alloc.create(std.Io.Threaded);
            owned.* = std.Io.Threaded.init(alloc, .{});
            break :blk owned;
        } else null;
        errdefer if (io_impl) |owned| {
            owned.deinit();
            alloc.destroy(owned);
        };
        const io = options.io orelse io_impl.?.io();
        const gcs = try alloc.create(object_storage.Gcs.JsonApiClient);
        errdefer alloc.destroy(gcs);
        var create_bucket_if_missing = false;
        var resolved_credentials: ?common_config.Config.ResolvedExternalIoCredentials = null;
        errdefer if (resolved_credentials) |*credentials| credentials.deinit(alloc);
        const cfg = if (options.connection) |connection_id| blk: {
            const external = try authorizedObjectConnection(
                options.node_config orelse return error.ConnectionConfigUnavailable,
                connection_id,
                .gcs,
                bucket,
                prefix,
                options.required_capability,
            );
            create_bucket_if_missing = external.bucket_provisioning == .create_if_missing;
            resolved_credentials = try common_config.Config.resolveExternalIoCredentials(alloc, external, options.secret_store);
            break :blk try gcsConfigForConnection(alloc, resolved_credentials.?.apply(external), io);
        } else blk: {
            var env_cfg = try object_storage.Gcs.jsonApiClientConfigFromEnvAlloc(alloc);
            env_cfg.io = io;
            break :blk env_cfg;
        };
        gcs.* = try object_storage.Gcs.JsonApiClient.init(alloc, cfg);
        errdefer {
            var client = gcs.client();
            client.deinit();
        }
        const owned_bucket = try alloc.dupe(u8, bucket);
        errdefer alloc.free(owned_bucket);
        const owned_prefix = try alloc.dupe(u8, prefix);

        return .{
            .alloc = alloc,
            .io_impl = io_impl,
            .io = io,
            .client = gcs.client(),
            .gcs_client = gcs,
            .resolved_credentials = resolved_credentials,
            .create_bucket_if_missing = create_bucket_if_missing,
            .bucket = owned_bucket,
            .prefix = owned_prefix,
        };
    }

    fn initS3Uri(
        alloc: std.mem.Allocator,
        bucket: []const u8,
        prefix: []const u8,
        options: OpenOptions,
    ) !RemoteBackupStore {
        const io_impl: ?*std.Io.Threaded = if (options.io == null) blk: {
            const owned = try alloc.create(std.Io.Threaded);
            owned.* = std.Io.Threaded.init(alloc, .{});
            break :blk owned;
        } else null;
        errdefer if (io_impl) |owned| {
            owned.deinit();
            alloc.destroy(owned);
        };
        const io = options.io orelse io_impl.?.io();
        const s3 = try alloc.create(object_storage.S3.Client);
        errdefer alloc.destroy(s3);
        var credential_context: ?*AwsCredentialContext = null;
        errdefer if (credential_context) |context| {
            context.deinit();
            alloc.destroy(context);
        };
        var create_bucket_if_missing = false;
        var resolved_credentials: ?common_config.Config.ResolvedExternalIoCredentials = null;
        errdefer if (resolved_credentials) |*credentials| credentials.deinit(alloc);
        var cfg = if (options.connection) |connection_id| blk: {
            const connection = try authorizedObjectConnection(options.node_config orelse return error.ConnectionConfigUnavailable, connection_id, .s3, bucket, prefix, options.required_capability);
            create_bucket_if_missing = connection.bucket_provisioning == .create_if_missing;
            resolved_credentials = try common_config.Config.resolveExternalIoCredentials(alloc, connection, options.secret_store);
            break :blk try s3ConfigForConnection(alloc, resolved_credentials.?.apply(connection), &credential_context, io);
        } else blk: {
            var overrides = try loadS3SecretOverrides(alloc, options.secret_store);
            defer overrides.deinit(alloc);
            break :blk try object_storage.S3.fromEnvAlloc(
                alloc,
                overrides.endpoint,
                true,
                overrides.access_key_id,
                overrides.secret_access_key,
                overrides.session_token,
                overrides.region,
                .path,
            );
        };
        cfg.io = io;
        s3.* = try object_storage.S3.Client.init(alloc, cfg);
        errdefer {
            var client = s3.client();
            client.deinit();
        }
        const owned_bucket = try alloc.dupe(u8, bucket);
        errdefer alloc.free(owned_bucket);
        const owned_prefix = try alloc.dupe(u8, prefix);

        return .{
            .alloc = alloc,
            .io_impl = io_impl,
            .io = io,
            .client = s3.client(),
            .s3_client = s3,
            .credential_context = credential_context,
            .resolved_credentials = resolved_credentials,
            .create_bucket_if_missing = create_bucket_if_missing,
            .bucket = owned_bucket,
            .prefix = owned_prefix,
        };
    }

    fn initWithClient(
        alloc: std.mem.Allocator,
        client: object_storage.ObjectStorage,
        bucket: []const u8,
        prefix: []const u8,
    ) !RemoteBackupStore {
        const io_impl = try alloc.create(std.Io.Threaded);
        errdefer alloc.destroy(io_impl);
        io_impl.* = std.Io.Threaded.init(alloc, .{});
        errdefer io_impl.deinit();
        const owned_bucket = try alloc.dupe(u8, bucket);
        errdefer alloc.free(owned_bucket);
        const owned_prefix = try alloc.dupe(u8, prefix);
        return .{
            .alloc = alloc,
            .io_impl = io_impl,
            .io = io_impl.io(),
            .client = client,
            .owns_client = false,
            .create_bucket_if_missing = true,
            .bucket = owned_bucket,
            .prefix = owned_prefix,
        };
    }

    fn deinit(self: *RemoteBackupStore) void {
        if (self.owns_client) self.client.deinit();
        if (self.gcs_client) |gcs| self.alloc.destroy(gcs);
        if (self.s3_client) |s3| self.alloc.destroy(s3);
        if (self.credential_context) |context| {
            context.deinit();
            self.alloc.destroy(context);
        }
        if (self.resolved_credentials) |*credentials| credentials.deinit(self.alloc);
        if (self.io_impl) |io_impl| {
            io_impl.deinit();
            self.alloc.destroy(io_impl);
        }
        self.alloc.free(self.bucket);
        self.alloc.free(self.prefix);
        self.* = undefined;
    }

    fn ensureBucket(self: *RemoteBackupStore) !void {
        // Normal backup writers may intentionally have PutObject without
        // HeadBucket/ListBucket. Only provisioning connections need to probe
        // and create buckets; ordinary writes let the object operation report
        // a missing or unauthorized bucket directly.
        if (!self.create_bucket_if_missing) return;
        if (self.bucket_ready.load(.acquire)) return;
        if (try self.client.bucketExists(self.bucket)) {
            self.bucket_ready.store(true, .release);
            return;
        }
        self.client.makeBucket(self.bucket) catch |err| {
            // Another request may have created the bucket after our probe.
            // Recheck rather than surfacing a harmless provider conflict.
            if (self.client.bucketExists(self.bucket) catch false) {
                self.bucket_ready.store(true, .release);
                return;
            }
            return err;
        };
        self.bucket_ready.store(true, .release);
    }

    fn keyAlloc(self: *const RemoteBackupStore, alloc: std.mem.Allocator, suffix: []const u8) ![]u8 {
        const canonical_prefix = trimRightSlash(self.prefix);
        const trimmed_suffix = trimLeftSlash(suffix);
        if (trimmed_suffix.len > 0) try validateArtifactRelativePath(trimmed_suffix);
        if (canonical_prefix.len == 0) return try alloc.dupe(u8, trimmed_suffix);
        if (trimmed_suffix.len == 0) return try alloc.dupe(u8, canonical_prefix);
        return try std.fmt.allocPrint(alloc, "{s}/{s}", .{ canonical_prefix, trimmed_suffix });
    }

    fn keyPrefixAlloc(self: *const RemoteBackupStore, alloc: std.mem.Allocator, suffix: []const u8) ![]u8 {
        const trimmed = std.mem.trim(u8, suffix, "/");
        if (trimmed.len == 0) return error.InvalidBackupArtifactPath;
        const base = try self.keyAlloc(alloc, trimmed);
        defer alloc.free(base);
        return try std.fmt.allocPrint(alloc, "{s}/", .{base});
    }

    fn writeBytes(self: *RemoteBackupStore, alloc: std.mem.Allocator, suffix: []const u8, body: []const u8, content_type: []const u8) !void {
        try self.ensureBucket();
        const key = try self.keyAlloc(alloc, suffix);
        defer alloc.free(key);
        var result = try self.client.putObject(self.bucket, key, body, .{ .content_type = content_type });
        defer result.deinit(alloc);
    }

    fn writeBytesIfAbsent(self: *RemoteBackupStore, alloc: std.mem.Allocator, suffix: []const u8, body: []const u8, content_type: []const u8) !void {
        try self.ensureBucket();
        const key = try self.keyAlloc(alloc, suffix);
        defer alloc.free(key);
        var result = self.client.putObject(self.bucket, key, body, .{
            .content_type = content_type,
            .if_none_match = true,
        }) catch |err| switch (err) {
            error.PreconditionFailed => return error.BackupAlreadyExists,
            else => return err,
        };
        defer result.deinit(alloc);
    }

    fn deleteSuffix(self: *RemoteBackupStore, alloc: std.mem.Allocator, suffix: []const u8) !void {
        const key = try self.keyAlloc(alloc, suffix);
        defer alloc.free(key);
        self.client.deleteObject(self.bucket, key, .{}) catch |err| switch (err) {
            error.FileNotFound => {},
            else => return err,
        };
    }

    /// Delete a small mutable object only when its current contents still name
    /// the expected owner. The ETag condition closes the GET/delete race.
    fn deleteSuffixIfOwned(
        self: *RemoteBackupStore,
        alloc: std.mem.Allocator,
        suffix: []const u8,
        expected_owner: []const u8,
    ) !bool {
        const key = try self.keyAlloc(alloc, suffix);
        defer alloc.free(key);
        var result = self.client.getObject(self.bucket, key, .{
            .range = .{ .offset = 0, .length = @intCast(expected_owner.len + 3) },
            .skip_metadata_probe = true,
        }) catch |err| switch (err) {
            error.FileNotFound => return false,
            else => return err,
        };
        defer result.deinit(alloc);
        const actual_owner = std.mem.trimEnd(u8, result.body, "\r\n");
        if (!std.mem.eql(u8, actual_owner, expected_owner)) return false;
        const etag = result.metadata.etag orelse
            return error.BackupReservationIdentityUnavailable;
        self.client.deleteObject(self.bucket, key, .{
            .if_match_etag = etag,
        }) catch |err| switch (err) {
            error.FileNotFound, error.PreconditionFailed => return false,
            else => return err,
        };
        return true;
    }

    fn suffixOwnerMatches(
        self: *RemoteBackupStore,
        alloc: std.mem.Allocator,
        suffix: []const u8,
        expected_owner: []const u8,
    ) !?bool {
        const key = try self.keyAlloc(alloc, suffix);
        defer alloc.free(key);
        var result = self.client.getObject(self.bucket, key, .{
            .range = .{ .offset = 0, .length = @intCast(expected_owner.len + 3) },
            .skip_metadata_probe = true,
        }) catch |err| switch (err) {
            error.FileNotFound => return null,
            else => return err,
        };
        defer result.deinit(alloc);
        return std.mem.eql(
            u8,
            std.mem.trimEnd(u8, result.body, "\r\n"),
            expected_owner,
        );
    }

    fn deletePrefix(
        self: *RemoteBackupStore,
        alloc: std.mem.Allocator,
        suffix: []const u8,
        object_budget: *usize,
    ) !void {
        const key_prefix = try self.keyPrefixAlloc(alloc, suffix);
        defer alloc.free(key_prefix);
        // Delete in bounded pages. Restarting at the prefix after each batch
        // avoids continuation-token invalidation while the keyset is changing.
        while (true) {
            if (object_budget.* == 0) return error.BackupCleanupBudgetExceeded;
            const page_size: u32 = @intCast(@min(object_budget.*, 1000));
            var listed = try self.client.listObjects(self.bucket, .{
                .prefix = key_prefix,
                .recursive = true,
                .max_keys = page_size,
            });
            defer listed.deinit(alloc);
            if (listed.entries.len == 0) return;
            for (listed.entries) |entry| {
                if (!std.mem.startsWith(u8, entry.key, key_prefix))
                    return error.InvalidBackupArtifactPath;
                self.client.deleteObject(self.bucket, entry.key, .{}) catch |err| switch (err) {
                    error.FileNotFound => {},
                    else => return err,
                };
                object_budget.* -= 1;
            }
        }
    }

    fn writeFile(self: *RemoteBackupStore, alloc: std.mem.Allocator, suffix: []const u8, src_path: []const u8, content_type: []const u8) !void {
        try self.ensureBucket();
        const key = try self.keyAlloc(alloc, suffix);
        defer alloc.free(key);
        var result = try self.client.putFileWithIo(self.io, self.bucket, key, src_path, .{ .content_type = content_type });
        defer result.deinit(alloc);
    }

    fn readBytesAllocLimited(self: *RemoteBackupStore, alloc: std.mem.Allocator, suffix: []const u8, max_bytes: usize) ![]u8 {
        const key = try self.keyAlloc(alloc, suffix);
        defer alloc.free(key);
        return try self.readKeyBytesAllocLimited(alloc, key, max_bytes, .{});
    }

    fn readKeyBytesAllocLimited(
        self: *RemoteBackupStore,
        alloc: std.mem.Allocator,
        key: []const u8,
        max_bytes: usize,
        options: BoundedReadOptions,
    ) ![]u8 {
        if (max_bytes == std.math.maxInt(usize)) return error.InvalidBackupManifestLimit;
        if (options.known_size) |size| {
            if (size > @as(u64, @intCast(max_bytes))) return error.BackupManifestTooLarge;
        }
        var result = self.client.getObject(self.bucket, key, .{
            // Fetch one sentinel byte beyond the accepted limit. The range is
            // authoritative for buffering even if provider metadata is stale.
            .range = .{ .offset = 0, .length = @intCast(max_bytes + 1) },
            .skip_metadata_probe = options.skip_metadata_probe,
            .max_response_bytes = max_bytes + 1,
        }) catch |err| switch (err) {
            error.ResponseTooLarge => return error.BackupManifestTooLarge,
            else => return err,
        };
        defer result.deinit(alloc);
        if (result.body.len > max_bytes) return error.BackupManifestTooLarge;
        return try alloc.dupe(u8, result.body);
    }

    fn validateArtifactAvailable(
        self: *RemoteBackupStore,
        alloc: std.mem.Allocator,
        format: BackupFormat,
        integrity_mode: ArtifactIntegrityMode,
        shard: *const ShardSnapshot,
    ) !void {
        switch (format) {
            .portable => {
                const key = try self.keyAlloc(alloc, shard.snapshot_path);
                defer alloc.free(key);
                var metadata = try self.client.statObject(self.bucket, key);
                defer metadata.deinit(alloc);
                if (integrity_mode == .declared and
                    metadata.content_length != shard.artifact_size_bytes)
                {
                    return error.BackupArtifactIntegrityMismatch;
                }
            },
            .native => {
                const key_prefix = try self.keyPrefixAlloc(alloc, shard.snapshot_path);
                defer alloc.free(key_prefix);
                var listed = try self.client.listObjects(self.bucket, .{
                    .prefix = key_prefix,
                    .recursive = true,
                    .max_keys = 1,
                });
                defer listed.deinit(alloc);
                if (listed.entries.len == 0 or
                    !std.mem.startsWith(u8, listed.entries[0].key, key_prefix))
                {
                    return error.BackupArtifactMissing;
                }
            },
        }
    }

    fn hashObjectVersion(
        self: *RemoteBackupStore,
        alloc: std.mem.Allocator,
        key: []const u8,
        size: u64,
        etag: []const u8,
        hasher: *std.crypto.hash.sha2.Sha256,
    ) !void {
        var offset: u64 = 0;
        while (offset < size) {
            const wanted = @min(size - offset, backup_integrity_read_chunk_bytes);
            var result = self.client.getObject(self.bucket, key, .{
                .range = .{ .offset = offset, .length = wanted },
                .if_match_etag = etag,
                .skip_metadata_probe = true,
            }) catch |err| switch (err) {
                error.PreconditionFailed => return error.SourceFileChanged,
                else => return err,
            };
            defer result.deinit(alloc);
            if (result.body.len != wanted) return error.SourceFileChanged;
            hasher.update(result.body);
            offset += wanted;
        }
    }

    fn verifyPortableArtifactIntegrity(
        self: *RemoteBackupStore,
        alloc: std.mem.Allocator,
        shard: *const ShardSnapshot,
    ) !void {
        const key = try self.keyAlloc(alloc, shard.snapshot_path);
        defer alloc.free(key);
        var metadata = try self.client.statObject(self.bucket, key);
        defer metadata.deinit(alloc);
        if (metadata.content_length != shard.artifact_size_bytes)
            return error.BackupArtifactIntegrityMismatch;
        const etag = metadata.etag orelse return error.RestoreArtifactIdentityMissing;
        var hasher = std.crypto.hash.sha2.Sha256.init(.{});
        try self.hashObjectVersion(
            alloc,
            key,
            metadata.content_length,
            etag,
            &hasher,
        );
        var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
        hasher.final(&digest);
        const hex = std.fmt.bytesToHex(digest, .lower);
        if (!std.mem.eql(u8, &hex, shard.artifact_sha256))
            return error.BackupArtifactIntegrityMismatch;
    }

    const RemoteNativeArtifactFile = struct {
        key: []u8,
        path: []u8,
        etag: []u8,
        size: u64,

        fn deinit(self: RemoteNativeArtifactFile, alloc: std.mem.Allocator) void {
            alloc.free(self.key);
            alloc.free(self.path);
            alloc.free(self.etag);
        }
    };

    fn remoteNativeArtifactFileLessThan(
        _: void,
        lhs: RemoteNativeArtifactFile,
        rhs: RemoteNativeArtifactFile,
    ) bool {
        return std.mem.order(u8, lhs.path, rhs.path) == .lt;
    }

    fn verifyNativeArtifactIntegrity(
        self: *RemoteBackupStore,
        alloc: std.mem.Allocator,
        shard: *const ShardSnapshot,
    ) !void {
        const key_prefix = try self.keyPrefixAlloc(alloc, shard.snapshot_path);
        defer alloc.free(key_prefix);
        var files = std.ArrayListUnmanaged(RemoteNativeArtifactFile).empty;
        defer {
            for (files.items) |entry| entry.deinit(alloc);
            files.deinit(alloc);
        }
        var continuation_token: ?[]u8 = null;
        defer if (continuation_token) |value| alloc.free(value);
        while (true) {
            var listed = try self.client.listObjects(self.bucket, .{
                .prefix = key_prefix,
                .recursive = true,
                .max_keys = 1000,
                .continuation_token = continuation_token,
            });
            defer listed.deinit(alloc);
            for (listed.entries) |entry| {
                if (!std.mem.startsWith(u8, entry.key, key_prefix))
                    return error.InvalidBackupArtifactPath;
                const relative_path = entry.key[key_prefix.len..];
                if (relative_path.len == 0) continue;
                try validateArtifactRelativePath(relative_path);
                if (files.items.len == backup_integrity_max_native_files)
                    return error.BackupArtifactTooLarge;
                const etag = if (entry.etag) |value|
                    try alloc.dupe(u8, value)
                else blk: {
                    var metadata = try self.client.statObject(self.bucket, entry.key);
                    defer metadata.deinit(alloc);
                    if (metadata.content_length != entry.size)
                        return error.SourceFileChanged;
                    break :blk try alloc.dupe(
                        u8,
                        metadata.etag orelse return error.RestoreArtifactIdentityMissing,
                    );
                };
                errdefer alloc.free(etag);
                const owned_key = try alloc.dupe(u8, entry.key);
                errdefer alloc.free(owned_key);
                const owned_path = try alloc.dupe(u8, relative_path);
                errdefer alloc.free(owned_path);
                try files.append(alloc, .{
                    .key = owned_key,
                    .path = owned_path,
                    .etag = etag,
                    .size = entry.size,
                });
            }
            const next = if (listed.next_continuation_token) |value|
                try alloc.dupe(u8, value)
            else
                null;
            if (continuation_token) |value| alloc.free(value);
            continuation_token = next;
            if (continuation_token == null) break;
        }
        if (files.items.len == 0) return error.BackupArtifactMissing;
        std.mem.sort(
            RemoteNativeArtifactFile,
            files.items,
            {},
            remoteNativeArtifactFileLessThan,
        );

        var total_size: u64 = 0;
        var hasher = std.crypto.hash.sha2.Sha256.init(.{});
        hasher.update("antfly-native-backup-tree-v1");
        hashArtifactU64(&hasher, @intCast(files.items.len));
        for (files.items) |entry| {
            total_size = std.math.add(u64, total_size, entry.size) catch
                return error.BackupArtifactTooLarge;
            hashArtifactBytes(&hasher, entry.path);
            hashArtifactU64(&hasher, entry.size);
            try self.hashObjectVersion(
                alloc,
                entry.key,
                entry.size,
                entry.etag,
                &hasher,
            );
        }
        hashArtifactU64(&hasher, total_size);
        var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
        hasher.final(&digest);
        const hex = std.fmt.bytesToHex(digest, .lower);
        if (total_size != shard.artifact_size_bytes or
            !std.mem.eql(u8, &hex, shard.artifact_sha256))
        {
            return error.BackupArtifactIntegrityMismatch;
        }
    }

    fn verifyArtifactIntegrity(
        self: *RemoteBackupStore,
        alloc: std.mem.Allocator,
        format: BackupFormat,
        shard: *const ShardSnapshot,
    ) !void {
        if (!isLowerSha256Hex(shard.artifact_sha256))
            return error.BackupIntegrityMissing;
        return switch (format) {
            .portable => self.verifyPortableArtifactIntegrity(alloc, shard),
            .native => self.verifyNativeArtifactIntegrity(alloc, shard),
        };
    }

    fn readFile(self: *RemoteBackupStore, alloc: std.mem.Allocator, suffix: []const u8, dest_path: []const u8) !void {
        const key = try self.keyAlloc(alloc, suffix);
        defer alloc.free(key);
        try self.client.getFileWithIo(self.io, self.bucket, key, dest_path, .{});
    }

    fn listObjectsPage(
        self: *RemoteBackupStore,
        alloc: std.mem.Allocator,
        suffix: []const u8,
        recursive: bool,
        max_keys: u32,
        start_after: ?[]const u8,
        continuation_token: ?[]const u8,
    ) !object_storage.ListResult {
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
            .start_after = start_after,
            .continuation_token = continuation_token,
        });
    }

    fn listTopLevelObjectsPage(
        self: *RemoteBackupStore,
        alloc: std.mem.Allocator,
        start_after: ?[]const u8,
        continuation_token: ?[]const u8,
    ) !object_storage.ListResult {
        return try self.listObjectsPage(alloc, "", false, 1000, start_after, continuation_token);
    }

    fn uploadDirectoryRecursive(self: *RemoteBackupStore, alloc: std.mem.Allocator, src_path: []const u8, dest_suffix: []const u8) !void {
        try self.ensureBucket();

        const io = self.io;

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
            var result = try self.client.putFileWithIo(io, self.bucket, key, local_path, .{
                .content_type = "application/octet-stream",
            });
            defer result.deinit(alloc);
        }
    }

    fn downloadDirectoryRecursive(self: *RemoteBackupStore, alloc: std.mem.Allocator, src_suffix: []const u8, dest_path: []const u8) !void {
        return try self.downloadDirectoryRecursiveWithPageSize(alloc, src_suffix, dest_path, 1000);
    }

    fn downloadDirectoryRecursiveWithPageSize(self: *RemoteBackupStore, alloc: std.mem.Allocator, src_suffix: []const u8, dest_path: []const u8, page_size: u32) !void {
        if (page_size == 0) return error.InvalidPageSize;
        const base_key = try self.keyAlloc(alloc, src_suffix);
        defer alloc.free(base_key);
        const key_prefix = if (base_key.len == 0)
            try alloc.alloc(u8, 0)
        else
            try std.fmt.allocPrint(alloc, "{s}/", .{base_key});
        defer alloc.free(key_prefix);

        if (try self.client.getPrefixWithIo(self.io, self.bucket, key_prefix, dest_path)) |downloaded| {
            if (downloaded == 0) return error.FileNotFound;
            return;
        }

        var found = false;
        var continuation_token: ?[]u8 = null;
        defer if (continuation_token) |token| alloc.free(token);
        while (true) {
            var listed = try self.client.listObjects(self.bucket, .{
                .prefix = key_prefix,
                .recursive = true,
                .max_keys = page_size,
                .continuation_token = continuation_token,
            });
            defer listed.deinit(alloc);
            var next_token = if (listed.next_continuation_token) |token| try alloc.dupe(u8, token) else null;
            errdefer if (next_token) |token| alloc.free(token);

            for (listed.entries) |entry| {
                if (!std.mem.startsWith(u8, entry.key, key_prefix)) return error.InvalidBackupArtifactPath;
                const rel = entry.key[key_prefix.len..];
                if (rel.len == 0) continue;
                try validateArtifactRelativePath(rel);
                const dest_file = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ dest_path, rel });
                defer alloc.free(dest_file);
                try self.client.getFileWithIo(self.io, self.bucket, entry.key, dest_file, .{});
                found = true;
            }

            if (continuation_token != null and next_token != null and std.mem.eql(u8, continuation_token.?, next_token.?)) {
                return error.InvalidContinuationToken;
            }
            if (continuation_token) |token| alloc.free(token);
            continuation_token = next_token;
            next_token = null;
            if (continuation_token == null) break;
        }
        if (!found) return error.FileNotFound;
    }
};

pub fn gcsConfigForConnection(
    alloc: std.mem.Allocator,
    external: common_config.Config.ExternalIoConnectionConfig,
    io: ?std.Io,
) !object_storage.Gcs.JsonApiConfig {
    var cfg = switch (external.gcs_credentials.source) {
        .default => try object_storage.Gcs.jsonApiClientConfigFromEnvAlloc(alloc),
        .bearer_token => try object_storage.Gcs.jsonApiClientConfigWithBearerTokenAlloc(
            alloc,
            external.gcs_credentials.bearer_token orelse return error.InvalidConnectionCredentials,
            external.project_id,
        ),
        .service_account => blk: {
            var account = if (external.gcs_credentials.service_account_json) |raw|
                try google_auth.parseServiceAccountJsonAlloc(alloc, raw)
            else
                try google_auth.serviceAccountFromFileAllocWithIo(alloc, external.gcs_credentials.credentials_path orelse return error.InvalidConnectionCredentials, io);
            var account_owned = true;
            errdefer if (account_owned) account.deinit(alloc);
            const account_project_id = if (account.project_id) |value| try alloc.dupe(u8, value) else null;
            defer if (account_project_id) |value| alloc.free(value);
            var auth_cfg = try google_auth.configFromServiceAccountAlloc(
                alloc,
                account,
                external.gcs_credentials.scope orelse google_auth.default_scope,
            );
            account_owned = false;
            errdefer auth_cfg.deinit(alloc);
            const source = try alloc.create(google_auth.CachedTokenSource);
            errdefer alloc.destroy(source);
            source.* = try google_auth.CachedTokenSource.initWithIo(alloc, auth_cfg, io);
            var value = try object_storage.Gcs.jsonApiClientConfigAlloc(alloc);
            value.auth = .{ .google_token_source = source };
            if (external.project_id orelse account_project_id) |project_id| value.project_id = try alloc.dupe(u8, project_id);
            break :blk value;
        },
    };
    errdefer cfg.deinit(alloc);
    cfg.io = io;
    if (external.endpoint) |endpoint| {
        alloc.free(cfg.endpoint);
        cfg.endpoint = try alloc.dupe(u8, endpoint);
    }
    if (external.upload_endpoint) |endpoint| {
        alloc.free(cfg.upload_endpoint);
        cfg.upload_endpoint = try alloc.dupe(u8, endpoint);
    }
    if (external.project_id) |project_id| {
        if (cfg.project_id) |previous| alloc.free(previous);
        cfg.project_id = try alloc.dupe(u8, project_id);
    }
    return cfg;
}

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
    /// Current Go cluster backups publish table metadata under a derived ID
    /// while naming portable shard artifacts with the cluster backup ID.
    /// Native Zig manifests leave this null because their table manifest
    /// already declares every artifact path.
    artifact_backup_id: ?[]const u8 = null,

    pub fn deinit(self: *ClusterTableBackupEntry, alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.name));
        alloc.free(@constCast(self.table_backup_id));
        if (self.artifact_backup_id) |value| alloc.free(@constCast(value));
        self.* = undefined;
    }
};

pub const ClusterBackupAttemptTable = struct {
    name: []const u8,
    table_backup_id: []const u8,
    artifact_backup_id: []const u8,
};

pub const ClusterBackupAttemptMarker = struct {
    format_version: u32 = backup_attempt_marker_version,
    attempt_id: []const u8,
    cluster_backup_id: []const u8,
    created_at_unix_ns: u64,
    format: BackupFormat,
    tables: []const ClusterBackupAttemptTable,
};

pub const ClusterBackupAttemptHead = struct {
    format_version: u32 = backup_attempt_head_version,
    attempt_id: []const u8,
};

pub const ClusterBackupManifest = struct {
    format_version: u32 = cluster_format_version,
    state: enum { complete } = .complete,
    backup_id: []const u8,
    timestamp: []const u8,
    location: []const u8,
    antfly_version: []const u8,
    expected_table_count: usize = 0,
    completed_table_count: usize = 0,
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
    return try openBackupLocationWithOptions(alloc, location, .{ .secret_store = secret_store });
}

pub fn openBackupLocationWithOptions(
    alloc: std.mem.Allocator,
    location: []const u8,
    options: OpenOptions,
) !BackupLocation {
    if (std.mem.startsWith(u8, location, "file://")) {
        if (options.connection) |connection_id| {
            const external = try authorizedFilesystemConnection(
                options.node_config orelse return error.ConnectionConfigUnavailable,
                connection_id,
                options.required_capability,
            );
            return .{ .file = try resolveFilesystemLocationAlloc(alloc, external.root.?, location, options.io) };
        }
        return .{ .file = try alloc.dupe(u8, try parseFileLocation(location)) };
    }
    if (std.mem.startsWith(u8, location, "s3://") or std.mem.startsWith(u8, location, "gs://") or std.mem.startsWith(u8, location, "gcs://")) {
        return .{ .remote = try RemoteBackupStore.initRemoteUri(alloc, location, options) };
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
        error.ConnectionConfigUnavailable => "named backup connection is unavailable on this server",
        error.ConnectionNotFound => "backup connection was not found",
        error.ConnectionKindMismatch, error.ConnectionProtocolMismatch => "backup connection protocol does not match the location",
        error.ConnectionCapabilityDenied => "backup connection does not grant the required capability",
        error.ConnectionBucketDenied => "backup location bucket is outside the connection allowlist",
        error.ConnectionPrefixDenied => "backup location prefix is outside the connection scope",
        error.InvalidConnectionCredentials => "backup connection credential configuration is invalid",
        error.SecretNotFound => "a secret referenced by the backup connection was not found",
        error.BucketNotFound => "backup bucket does not exist and the connection does not allow provisioning",
        error.InvalidBackupRangeTopology => "backup shard ranges are not contiguous",
        else => null,
    };
}

pub fn parseBackupRequest(alloc: std.mem.Allocator, body: []const u8) !std.json.Parsed(BackupRequest) {
    return std.json.parseFromSlice(BackupRequest, alloc, body, .{});
}

pub fn parseRestoreRequest(alloc: std.mem.Allocator, body: []const u8) !std.json.Parsed(RestoreRequest) {
    return std.json.parseFromSlice(RestoreRequest, alloc, body, .{});
}

test "backup API requests reject unknown operational fields" {
    var backup = try parseBackupRequest(std.testing.allocator,
        \\{"backup_id":"daily","location":"s3://archive/daily","connection":"archive-writer","format":"native"}
    );
    backup.deinit();
    try std.testing.expectError(error.UnknownField, parseBackupRequest(std.testing.allocator,
        \\{"backup_id":"daily","location":"s3://archive/daily","connection":"archive-writer","formt":"native"}
    ));

    var parsed = try parseRestoreRequest(std.testing.allocator,
        \\{"backup_id":"daily","location":"s3://archive/daily","connection":"archive-reader"}
    );
    parsed.deinit();
    try std.testing.expectError(error.UnknownField, parseRestoreRequest(std.testing.allocator,
        \\{"backup_id":"daily","location":"s3://archive/daily","connection":"archive-reader","format":"portable"}
    ));
    try std.testing.expectError(error.UnknownField, parseRestoreRequest(std.testing.allocator,
        \\{"backup_id":"daily","location":"s3://archive/daily","connection":"archive-reader","conection":"typo"}
    ));
    try std.testing.expectError(error.UnknownField, parseClusterBackupRequest(std.testing.allocator,
        \\{"backup_id":"daily","location":"s3://archive/daily","connection":"archive-writer","formt":"native"}
    ));
    try std.testing.expectError(error.UnknownField, parseClusterRestoreRequest(std.testing.allocator,
        \\{"backup_id":"daily","location":"s3://archive/daily","connection":"archive-reader","format":"portable"}
    ));
}

pub fn parseClusterBackupRequest(alloc: std.mem.Allocator, body: []const u8) !ClusterBackupRequest {
    var parsed = try std.json.parseFromSlice(metadata_openapi.ClusterBackupRequest, alloc, body, .{});
    defer parsed.deinit();
    try validateBackupId(parsed.value.backup_id);
    const format = try parseBackupFormat(parsed.value.format);
    const backup_id = try alloc.dupe(u8, parsed.value.backup_id);
    errdefer alloc.free(backup_id);
    const location = try alloc.dupe(u8, parsed.value.location);
    errdefer alloc.free(location);
    const connection = try alloc.dupe(u8, parsed.value.connection);
    errdefer alloc.free(connection);
    try validateSelectedTableNames(parsed.value.table_names);
    const table_names = try cloneOptionalStringSlice(alloc, parsed.value.table_names);
    errdefer if (table_names) |values| freeStringSlice(alloc, values);
    return .{
        .backup_id = backup_id,
        .location = location,
        .connection = connection,
        .format = format,
        .table_names = table_names,
    };
}

pub fn parseBackupFormat(value: ?[]const u8) !BackupFormat {
    const format = value orelse return .portable;
    return std.meta.stringToEnum(BackupFormat, format) orelse error.UnsupportedBackupFormat;
}

test "cluster backup format defaults portable and preserves explicit native" {
    const alloc = std.testing.allocator;
    var default_req = try parseClusterBackupRequest(alloc,
        \\{"backup_id":"daily","location":"s3://archive/backups","connection":"archive-writer"}
    );
    defer freeClusterBackupRequest(alloc, &default_req);
    try std.testing.expectEqual(BackupFormat.portable, default_req.format);

    var native_req = try parseClusterBackupRequest(alloc,
        \\{"backup_id":"daily-native","location":"s3://archive/backups","connection":"archive-writer","format":"native"}
    );
    defer freeClusterBackupRequest(alloc, &native_req);
    try std.testing.expectEqual(BackupFormat.native, native_req.format);
}

test "cluster backup and restore reject duplicate table selectors" {
    const alloc = std.testing.allocator;
    try std.testing.expectError(error.DuplicateBackupTableName, parseClusterBackupRequest(alloc,
        \\{"backup_id":"daily","location":"s3://archive/backups","connection":"archive-writer","table_names":["docs","docs"]}
    ));
    try std.testing.expectError(error.DuplicateBackupTableName, parseClusterRestoreRequest(alloc,
        \\{"backup_id":"daily","location":"s3://archive/backups","connection":"archive-reader","table_names":["docs","docs"]}
    ));
}

pub fn parseClusterRestoreRequest(alloc: std.mem.Allocator, body: []const u8) !ClusterRestoreRequest {
    var parsed = try std.json.parseFromSlice(metadata_openapi.ClusterRestoreRequest, alloc, body, .{});
    defer parsed.deinit();
    try validateBackupId(parsed.value.backup_id);
    const backup_id = try alloc.dupe(u8, parsed.value.backup_id);
    errdefer alloc.free(backup_id);
    const location = try alloc.dupe(u8, parsed.value.location);
    errdefer alloc.free(location);
    const connection = try alloc.dupe(u8, parsed.value.connection);
    errdefer alloc.free(connection);
    try validateSelectedTableNames(parsed.value.table_names);
    const table_names = try cloneOptionalStringSlice(alloc, parsed.value.table_names);
    errdefer if (table_names) |values| freeStringSlice(alloc, values);
    const restore_mode = if (parsed.value.restore_mode) |value| try alloc.dupe(u8, value) else null;
    errdefer if (restore_mode) |value| alloc.free(value);
    return .{
        .backup_id = backup_id,
        .location = location,
        .connection = connection,
        .table_names = table_names,
        .restore_mode = restore_mode,
    };
}

pub fn parseFileLocation(location: []const u8) ![]const u8 {
    if (!std.mem.startsWith(u8, location, "file://")) return error.UnsupportedBackupLocation;
    const path = location["file://".len..];
    if (path.len == 0 or path[0] != '/') return error.InvalidBackupLocation;
    return path;
}

pub fn validateBackupId(value: []const u8) !void {
    if (value.len == 0 or value.len > 128) return error.InvalidBackupId;
    for (value) |c| {
        if (!std.ascii.isAlphanumeric(c) and c != '-' and c != '_' and c != '.') return error.InvalidBackupId;
    }
    if (std.mem.eql(u8, value, ".") or std.mem.eql(u8, value, "..")) return error.InvalidBackupId;
}

fn ensureManifestSize(encoded: []const u8, max_bytes: usize) !void {
    if (encoded.len > max_bytes) return error.BackupManifestTooLarge;
}

pub fn validateArtifactRelativePath(path: []const u8) !void {
    if (path.len == 0 or path.len > 4096 or std.fs.path.isAbsolute(path) or std.mem.indexOfScalar(u8, path, '\\') != null or std.mem.indexOfScalar(u8, path, 0) != null) {
        return error.InvalidBackupArtifactPath;
    }
    var components = std.mem.splitScalar(u8, path, '/');
    while (components.next()) |component| {
        if (component.len == 0 or std.mem.eql(u8, component, ".") or std.mem.eql(u8, component, "..")) return error.InvalidBackupArtifactPath;
    }
}

fn resolveFilesystemLocationAlloc(alloc: std.mem.Allocator, configured_root: []const u8, location: []const u8, shared_io: ?std.Io) ![]u8 {
    const uri_path = try parseFileLocation(location);
    const relative = std.mem.trimStart(u8, uri_path, "/");
    if (relative.len > 0) try validateArtifactRelativePath(relative);

    var io_impl: ?std.Io.Threaded = if (shared_io == null) std.Io.Threaded.init(alloc, .{}) else null;
    defer if (io_impl) |*owned| owned.deinit();
    const io = shared_io orelse io_impl.?.io();
    const canonical_root = try std.Io.Dir.realPathFileAbsoluteAlloc(io, configured_root, alloc);
    defer alloc.free(canonical_root);
    const candidate = if (relative.len == 0)
        try alloc.dupe(u8, canonical_root)
    else
        try std.fs.path.join(alloc, &.{ canonical_root, relative });
    errdefer alloc.free(candidate);

    // Resolve the nearest existing ancestor. This rejects pre-existing symlinks
    // that leave the configured root while still allowing backup directories to
    // be created below it.
    var ancestor: []const u8 = candidate;
    while (true) {
        const canonical = std.Io.Dir.realPathFileAbsoluteAlloc(io, ancestor, alloc) catch |err| switch (err) {
            error.FileNotFound, error.NotDir => {
                ancestor = std.fs.path.dirname(ancestor) orelse return error.InvalidBackupLocation;
                continue;
            },
            else => return err,
        };
        defer alloc.free(canonical);
        if (!pathIsWithin(canonical_root, canonical)) return error.ConnectionPrefixDenied;
        break;
    }
    return candidate;
}

fn pathIsWithin(root: []const u8, candidate: []const u8) bool {
    if (std.mem.eql(u8, root, candidate)) return true;
    if (candidate.len <= root.len or !std.mem.startsWith(u8, candidate, root)) return false;

    // Canonical filesystem roots already end in the platform separator (for
    // example `/`). Requiring another separator after that root incorrectly
    // rejects every descendant of a root-scoped administrative connection.
    return root[root.len - 1] == std.fs.path.sep or candidate[root.len] == std.fs.path.sep;
}

test "restore filesystem scope containment handles filesystem roots and component boundaries" {
    try std.testing.expect(pathIsWithin("/", "/private/tmp/backup"));
    try std.testing.expect(pathIsWithin("/var/backups", "/var/backups/tenant-a"));
    try std.testing.expect(!pathIsWithin("/var/backups", "/var/backups-evil"));
}

pub fn createManifest(
    alloc: std.mem.Allocator,
    backup_id: []const u8,
    format: BackupFormat,
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
            .artifact_size_bytes = shard.artifact_size_bytes,
            .artifact_sha256 = if (shard.artifact_sha256.len > 0)
                try alloc.dupe(u8, shard.artifact_sha256)
            else
                "",
        };
        initialized += 1;
    }

    return .{
        .format = format,
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
    try validatePublishedTableManifest(alloc, manifest, manifest.backup_id);
    const path = try metadataPath(alloc, backup_root, manifest.backup_id);
    defer alloc.free(path);
    try ensureDirPath(backup_root);

    const encoded = try stringifyJsonAlloc(alloc, manifest.*);
    defer alloc.free(encoded);
    try ensureManifestSize(encoded, max_backup_manifest_bytes);
    try writeFileAbsoluteIfAbsent(alloc, path, encoded);
}

pub fn readManifest(
    alloc: std.mem.Allocator,
    backup_root: []const u8,
    backup_id: []const u8,
) !TableBackupManifest {
    const path = try metadataPath(alloc, backup_root, backup_id);
    defer alloc.free(path);
    const body = try readFileAbsoluteAlloc(alloc, path, max_backup_manifest_bytes);
    defer alloc.free(body);

    return parseTableBackupManifest(alloc, body, backup_id);
}

pub fn writeManifestToLocation(
    alloc: std.mem.Allocator,
    location: *BackupLocation,
    manifest: *const TableBackupManifest,
) !void {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    return writeManifestToLocationWithIo(alloc, io_impl.io(), location, manifest);
}

pub fn writeManifestToLocationWithIo(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    manifest: *const TableBackupManifest,
) !void {
    switch (location.*) {
        .file => |backup_root| {
            try validatePublishedTableManifest(alloc, manifest, manifest.backup_id);
            const path = try metadataPath(alloc, backup_root, manifest.backup_id);
            defer alloc.free(path);
            const encoded = try stringifyJsonAlloc(alloc, manifest.*);
            defer alloc.free(encoded);
            try ensureManifestSize(encoded, max_backup_manifest_bytes);
            try writeFileAbsoluteIfAbsentWithIo(alloc, io, path, encoded);
        },
        .remote => |*store| {
            try validatePublishedTableManifest(alloc, manifest, manifest.backup_id);
            const encoded = try stringifyJsonAlloc(alloc, manifest.*);
            defer alloc.free(encoded);
            try ensureManifestSize(encoded, max_backup_manifest_bytes);
            const suffix = try metadataPath(alloc, "", manifest.backup_id);
            defer alloc.free(suffix);
            try store.writeBytesIfAbsent(alloc, trimLeftSlash(suffix), encoded, "application/json");
        },
    }
}

pub fn readManifestFromLocation(
    alloc: std.mem.Allocator,
    location: *BackupLocation,
    backup_id: []const u8,
) !TableBackupManifest {
    return try readManifestFromLocationWithArtifactBackupId(
        alloc,
        location,
        backup_id,
        backup_id,
    );
}

pub fn readManifestFromLocationWithArtifactBackupId(
    alloc: std.mem.Allocator,
    location: *BackupLocation,
    backup_id: []const u8,
    artifact_backup_id: []const u8,
) !TableBackupManifest {
    try validateBackupId(artifact_backup_id);
    switch (location.*) {
        .file => |backup_root| {
            const path = try metadataPath(alloc, backup_root, backup_id);
            defer alloc.free(path);
            const body = try readFileAbsoluteAlloc(alloc, path, max_backup_manifest_bytes);
            defer alloc.free(body);
            return try parseTableBackupManifestWithArtifactBackupId(
                alloc,
                body,
                backup_id,
                artifact_backup_id,
            );
        },
        .remote => |*store| {
            const suffix = try metadataPath(alloc, "", backup_id);
            defer alloc.free(suffix);
            const body = try store.readBytesAllocLimited(alloc, trimLeftSlash(suffix), max_backup_manifest_bytes);
            defer alloc.free(body);
            return parseTableBackupManifestWithArtifactBackupId(
                alloc,
                body,
                backup_id,
                artifact_backup_id,
            );
        },
    }
}

pub fn manifestExistsAtLocation(alloc: std.mem.Allocator, location: *BackupLocation, backup_id: []const u8) !bool {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    return manifestExistsAtLocationWithIo(alloc, io_impl.io(), location, backup_id);
}

pub fn manifestExistsAtLocationWithIo(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    backup_id: []const u8,
) !bool {
    return switch (location.*) {
        .file => |backup_root| blk: {
            const path = try metadataPath(alloc, backup_root, backup_id);
            defer alloc.free(path);
            break :blk try pathExistsWithIo(io, path);
        },
        // Do not require restore.read/HeadObject authority from a write-only
        // backup connection. The conditional manifest put is the authoritative
        // conflict check for object storage.
        .remote => false,
    };
}

fn parseTableBackupManifest(
    alloc: std.mem.Allocator,
    body: []const u8,
    backup_id: []const u8,
) !TableBackupManifest {
    return try parseTableBackupManifestWithArtifactBackupId(
        alloc,
        body,
        backup_id,
        backup_id,
    );
}

fn parseTableBackupManifestWithArtifactBackupId(
    alloc: std.mem.Allocator,
    body: []const u8,
    backup_id: []const u8,
    artifact_backup_id: []const u8,
) !TableBackupManifest {
    var value = try std.json.parseFromSlice(std.json.Value, alloc, body, .{});
    defer value.deinit();
    const root = switch (value.value) {
        .object => |object| object,
        else => return error.InvalidBackupRequest,
    };
    if (root.get("format_version") != null) {
        var parsed = try std.json.parseFromSlice(TableBackupManifest, alloc, body, .{ .allocate = .alloc_always });
        defer parsed.deinit();
        try validatePublishedTableManifest(alloc, &parsed.value, backup_id);
        return try cloneTableBackupManifest(alloc, parsed.value);
    }
    return try parseGoPortableTableManifest(alloc, root, backup_id, artifact_backup_id);
}

pub fn validateTableManifest(
    alloc: std.mem.Allocator,
    manifest: *const TableBackupManifest,
    requested_backup_id: []const u8,
) !void {
    if (manifest.format_version != format_version) return error.UnsupportedBackupFormat;
    if (!std.mem.eql(u8, manifest.backup_id, requested_backup_id)) return error.InvalidBackupRequest;
    try validateManifestShards(alloc, manifest);
}

fn validatePublishedTableManifest(
    alloc: std.mem.Allocator,
    manifest: *const TableBackupManifest,
    requested_backup_id: []const u8,
) !void {
    try validateTableManifest(alloc, manifest, requested_backup_id);
    // Derivation is an in-memory bridge for the exact Go envelope, whose
    // format cannot carry integrity metadata. Never accept it as authority in
    // the current Zig manifest or a writer could publish a checksum-free
    // backup that merely hashes whatever bytes happen to be downloaded.
    if (manifest.artifact_integrity_mode != .declared)
        return error.BackupIntegrityMissing;
}

fn validateManifestShards(
    alloc: std.mem.Allocator,
    manifest: *const TableBackupManifest,
) !void {
    if (manifest.shards.len == 0) return error.UnsupportedBackupFormat;
    var group_ids_seen = std.AutoHashMapUnmanaged(u64, void).empty;
    defer group_ids_seen.deinit(alloc);
    var paths_seen = std.StringHashMapUnmanaged(void).empty;
    defer paths_seen.deinit(alloc);

    for (manifest.shards) |shard| {
        try validateArtifactRelativePath(shard.snapshot_path);
        if (shard.end_key) |end_key| {
            if (end_key.len == 0 or std.mem.order(u8, shard.start_key, end_key) != .lt)
                return error.InvalidBackupRequest;
        }
        const group_entry = try group_ids_seen.getOrPut(alloc, shard.group_id);
        if (group_entry.found_existing) return error.InvalidBackupRequest;
        const path_entry = try paths_seen.getOrPut(alloc, shard.snapshot_path);
        if (path_entry.found_existing) return error.InvalidBackupRequest;

        const is_portable_path = std.mem.endsWith(u8, shard.snapshot_path, ".afb");
        if ((manifest.format == .portable) != is_portable_path)
            return error.BackupArtifactFormatMismatch;
        if (manifest.artifact_integrity_mode == .declared and
            !isLowerSha256Hex(shard.artifact_sha256))
        {
            return error.BackupIntegrityMissing;
        }
        if (manifest.artifact_integrity_mode == .derive_after_materialization and
            (manifest.format != .portable or
                shard.artifact_size_bytes != 0 or
                shard.artifact_sha256.len != 0))
        {
            return error.InvalidBackupRequest;
        }
    }

    const ordered = try alloc.dupe(ShardSnapshot, manifest.shards);
    defer alloc.free(ordered);
    std.mem.sort(ShardSnapshot, ordered, {}, struct {
        fn lessThan(_: void, lhs: ShardSnapshot, rhs: ShardSnapshot) bool {
            const start_order = std.mem.order(u8, lhs.start_key, rhs.start_key);
            if (start_order != .eq) return start_order == .lt;
            return lhs.group_id < rhs.group_id;
        }
    }.lessThan);
    for (ordered[0 .. ordered.len - 1], ordered[1..]) |left, right| {
        const left_end = left.end_key orelse return error.InvalidBackupRangeTopology;
        if (!std.mem.eql(u8, left_end, right.start_key))
            return error.InvalidBackupRangeTopology;
    }
}

pub fn validateRestoreManifest(
    alloc: std.mem.Allocator,
    manifest: *const TableBackupManifest,
    requested_backup_id: []const u8,
) !void {
    return try validateTableManifest(alloc, manifest, requested_backup_id);
}

fn isLowerSha256Hex(value: []const u8) bool {
    if (value.len != std.crypto.hash.sha2.Sha256.digest_length * 2) return false;
    for (value) |c| {
        if (!std.ascii.isDigit(c) and !(c >= 'a' and c <= 'f')) return false;
    }
    return true;
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
    root: std.json.ObjectMap,
    backup_id: []const u8,
    artifact_backup_id: []const u8,
) !TableBackupManifest {
    if (root.count() != 3) return error.InvalidBackupRequest;
    const version = switch (root.get("version") orelse return error.InvalidBackupRequest) {
        .integer => |value| value,
        else => return error.InvalidBackupRequest,
    };
    if (version != 1) return error.UnsupportedBackupFormat;
    const format = switch (root.get("format") orelse return error.InvalidBackupRequest) {
        .string => |value| value,
        else => return error.InvalidBackupRequest,
    };
    if (!std.mem.eql(u8, format, "portable")) return error.UnsupportedBackupFormat;
    const table = switch (root.get("table") orelse return error.InvalidBackupRequest) {
        .object => |object| object,
        else => return error.InvalidBackupRequest,
    };
    const table_name = switch (table.get("name") orelse return error.InvalidBackupRequest) {
        .string => |value| value,
        else => return error.InvalidBackupRequest,
    };
    if (table_name.len == 0 or table_name.len > 4096) return error.InvalidBackupRequest;

    const schema_json = try stringifyOptionalGoTableField(alloc, table.get("schema"), "{}");
    errdefer alloc.free(schema_json);
    const read_schema_json = try stringifyOptionalGoTableField(alloc, table.get("read_schema"), "");
    errdefer alloc.free(read_schema_json);
    const indexes_json = try normalizeGoPortableIndexesJson(alloc, table.get("indexes"));
    errdefer alloc.free(indexes_json);
    const replication_sources_json = try stringifyOptionalGoTableField(
        alloc,
        table.get("replication_sources"),
        "[]",
    );
    errdefer alloc.free(replication_sources_json);
    const description = if (table.get("description")) |value|
        switch (value) {
            .null => "",
            .string => |text| text,
            else => return error.InvalidBackupRequest,
        }
    else
        "";
    const shards_value = switch (table.get("shards") orelse return error.InvalidBackupRequest) {
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
        const end_key = if (end_encoded.len > 0)
            try decodePortableByteRangeBoundary(alloc, end_encoded)
        else
            null;
        errdefer if (end_key) |value| alloc.free(value);
        const snapshot_path = try std.fmt.allocPrint(alloc, "{s}-{s}.afb", .{
            artifact_backup_id,
            entry.key_ptr.*,
        });
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

    var manifest: TableBackupManifest = .{
        .format = .portable,
        .artifact_integrity_mode = .derive_after_materialization,
        .backup_id = try alloc.dupe(u8, backup_id),
        .table_name = try alloc.dupe(u8, table_name),
        .description = try alloc.dupe(u8, description),
        .schema_json = schema_json,
        .read_schema_json = read_schema_json,
        .indexes_json = indexes_json,
        .replication_sources_json = replication_sources_json,
        .shards = shards,
    };
    errdefer manifest.deinit(alloc);
    try validateTableManifest(alloc, &manifest, backup_id);
    return manifest;
}

fn stringifyOptionalGoTableField(
    alloc: std.mem.Allocator,
    maybe_value: ?std.json.Value,
    absent: []const u8,
) ![]u8 {
    const value = maybe_value orelse return try alloc.dupe(u8, absent);
    if (value == .null) return try alloc.dupe(u8, absent);
    return try stringifyJsonAlloc(alloc, value);
}

fn normalizeGoPortableIndexesJson(alloc: std.mem.Allocator, maybe_indexes: ?std.json.Value) ![]u8 {
    const indexes = maybe_indexes orelse return try alloc.dupe(u8, "{}");
    if (indexes == .null) return try alloc.dupe(u8, "{}");
    const object = switch (indexes) {
        .object => |value| value,
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
        .object => |item| item,
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
    try validateBackupId(backup_id);
    return try std.fmt.allocPrint(alloc, "{s}/{s}-metadata.json", .{ backup_root, backup_id });
}

pub fn clusterMetadataPath(alloc: std.mem.Allocator, backup_root: []const u8, backup_id: []const u8) ![]u8 {
    try validateBackupId(backup_id);
    return try std.fmt.allocPrint(alloc, "{s}/{s}-cluster-metadata.json", .{ backup_root, backup_id });
}

fn reservationPath(
    alloc: std.mem.Allocator,
    backup_root: []const u8,
    backup_id: []const u8,
    cluster: bool,
) ![]u8 {
    try validateBackupId(backup_id);
    return if (cluster)
        try std.fmt.allocPrint(alloc, "{s}/{s}-cluster-reservation", .{ backup_root, backup_id })
    else
        try std.fmt.allocPrint(alloc, "{s}/{s}-reservation", .{ backup_root, backup_id });
}

pub fn reserveBackupAtLocation(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    backup_id: []const u8,
    cluster: bool,
) !void {
    const suffix = try reservationPath(alloc, "", backup_id, cluster);
    defer alloc.free(suffix);
    switch (location.*) {
        .file => |backup_root| {
            const path = try reservationPath(alloc, backup_root, backup_id, cluster);
            defer alloc.free(path);
            try writeFileAbsoluteIfAbsentWithIo(alloc, io, path, "reserved\n");
        },
        .remote => |*store| try store.writeBytesIfAbsent(
            alloc,
            trimLeftSlash(suffix),
            "reserved\n",
            "text/plain",
        ),
    }
}

/// Cluster reservations are leases owned by an immutable attempt ID. Cleanup
/// must present the same owner, so a stale process cannot release a retry's
/// live admission fence.
pub fn reserveClusterBackupAttemptAtLocation(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    backup_id: []const u8,
    attempt_id: []const u8,
) !void {
    try validateBackupId(attempt_id);
    const owner = try std.fmt.allocPrint(alloc, "{s}\n", .{attempt_id});
    defer alloc.free(owner);
    const suffix = try reservationPath(alloc, "", backup_id, true);
    defer alloc.free(suffix);
    switch (location.*) {
        .file => |backup_root| {
            const path = try reservationPath(alloc, backup_root, backup_id, true);
            defer alloc.free(path);
            try writeFileAbsoluteIfAbsentWithIo(alloc, io, path, owner);
        },
        .remote => |*store| try store.writeBytesIfAbsent(
            alloc,
            trimLeftSlash(suffix),
            owner,
            "text/plain",
        ),
    }
}

fn incompleteBackupMarkerPath(
    alloc: std.mem.Allocator,
    backup_root: []const u8,
    attempt_id: []const u8,
) ![]u8 {
    try validateBackupId(attempt_id);
    return try std.fmt.allocPrint(alloc, "{s}/{s}/{s}.json", .{
        backup_root,
        incomplete_backup_prefix,
        attempt_id,
    });
}

fn backupAttemptHeadPath(alloc: std.mem.Allocator, backup_root: []const u8) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{s}/{s}", .{ backup_root, backup_attempt_head_name });
}

fn validateClusterBackupAttemptHead(head: *const ClusterBackupAttemptHead) !void {
    if (head.format_version != backup_attempt_head_version)
        return error.UnsupportedBackupFormat;
    try validateBackupId(head.attempt_id);
}

pub fn writeClusterBackupAttemptHead(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    attempt_id: []const u8,
) !void {
    const head: ClusterBackupAttemptHead = .{ .attempt_id = attempt_id };
    try validateClusterBackupAttemptHead(&head);
    const encoded = try stringifyJsonAlloc(alloc, head);
    defer alloc.free(encoded);
    try ensureManifestSize(encoded, max_backup_attempt_marker_bytes);
    switch (location.*) {
        .file => |backup_root| {
            const path = try backupAttemptHeadPath(alloc, backup_root);
            defer alloc.free(path);
            try writeFileAbsoluteAtomicallyWithIo(alloc, io, path, encoded);
        },
        .remote => |*store| try store.writeBytes(
            alloc,
            backup_attempt_head_name,
            encoded,
            "application/json",
        ),
    }
}

fn readClusterBackupAttemptHead(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
) !?std.json.Parsed(ClusterBackupAttemptHead) {
    const body = switch (location.*) {
        .file => |backup_root| blk: {
            const path = try backupAttemptHeadPath(alloc, backup_root);
            defer alloc.free(path);
            break :blk readFileAbsoluteAllocWithIo(
                alloc,
                io,
                path,
                max_backup_attempt_marker_bytes,
            ) catch |err| switch (err) {
                error.FileNotFound => return null,
                else => return err,
            };
        },
        .remote => |*store| store.readBytesAllocLimited(
            alloc,
            backup_attempt_head_name,
            max_backup_attempt_marker_bytes,
        ) catch |err| switch (err) {
            error.FileNotFound => return null,
            else => return err,
        },
    };
    defer alloc.free(body);
    var parsed = try std.json.parseFromSlice(
        ClusterBackupAttemptHead,
        alloc,
        body,
        .{ .allocate = .alloc_always },
    );
    errdefer parsed.deinit();
    try validateClusterBackupAttemptHead(&parsed.value);
    return parsed;
}

fn validateClusterBackupAttemptMarker(
    alloc: std.mem.Allocator,
    marker: *const ClusterBackupAttemptMarker,
    expected_attempt_id: []const u8,
) !void {
    if (marker.format_version != backup_attempt_marker_version)
        return error.UnsupportedBackupFormat;
    try validateBackupId(marker.attempt_id);
    try validateBackupId(marker.cluster_backup_id);
    if (!std.mem.eql(u8, marker.attempt_id, expected_attempt_id))
        return error.InvalidBackupRequest;
    if (std.mem.eql(u8, marker.attempt_id, marker.cluster_backup_id) or
        marker.created_at_unix_ns == 0)
    {
        return error.InvalidBackupRequest;
    }
    if (marker.tables.len == 0 or marker.tables.len > max_cluster_backup_attempt_tables)
        return error.InvalidBackupRequest;

    var table_names = std.StringHashMapUnmanaged(void).empty;
    defer table_names.deinit(alloc);
    var ids = std.StringHashMapUnmanaged(void).empty;
    defer ids.deinit(alloc);
    try table_names.ensureTotalCapacity(alloc, @intCast(marker.tables.len));
    try ids.ensureTotalCapacity(alloc, @intCast(marker.tables.len * 2 + 2));
    ids.putAssumeCapacity(marker.attempt_id, {});
    ids.putAssumeCapacity(marker.cluster_backup_id, {});
    for (marker.tables) |table| {
        if (table.name.len == 0 or table.name.len > 4096) return error.InvalidBackupRequest;
        try validateBackupId(table.table_backup_id);
        try validateBackupId(table.artifact_backup_id);
        if (std.mem.eql(u8, table.table_backup_id, table.artifact_backup_id) or
            table_names.contains(table.name) or
            ids.contains(table.table_backup_id) or
            ids.contains(table.artifact_backup_id))
        {
            return error.InvalidBackupRequest;
        }
        table_names.putAssumeCapacity(table.name, {});
        ids.putAssumeCapacity(table.table_backup_id, {});
        ids.putAssumeCapacity(table.artifact_backup_id, {});
    }
}

pub fn writeClusterBackupAttemptMarker(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    marker: *const ClusterBackupAttemptMarker,
) !void {
    try validateClusterBackupAttemptMarker(alloc, marker, marker.attempt_id);
    const encoded = try stringifyJsonAlloc(alloc, marker.*);
    defer alloc.free(encoded);
    try ensureManifestSize(encoded, max_backup_attempt_marker_bytes);
    const suffix = try incompleteBackupMarkerPath(alloc, "", marker.attempt_id);
    defer alloc.free(suffix);
    switch (location.*) {
        .file => |backup_root| {
            const path = try incompleteBackupMarkerPath(alloc, backup_root, marker.attempt_id);
            defer alloc.free(path);
            try writeFileAbsoluteIfAbsentWithIo(alloc, io, path, encoded);
        },
        .remote => |*store| try store.writeBytesIfAbsent(
            alloc,
            trimLeftSlash(suffix),
            encoded,
            "application/json",
        ),
    }
}

fn readClusterBackupAttemptMarker(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    attempt_id: []const u8,
) !std.json.Parsed(ClusterBackupAttemptMarker) {
    const suffix = try incompleteBackupMarkerPath(alloc, "", attempt_id);
    defer alloc.free(suffix);
    const body = switch (location.*) {
        .file => |backup_root| blk: {
            const path = try incompleteBackupMarkerPath(alloc, backup_root, attempt_id);
            defer alloc.free(path);
            break :blk try readFileAbsoluteAllocWithIo(alloc, io, path, max_backup_attempt_marker_bytes);
        },
        .remote => |*store| try store.readBytesAllocLimited(
            alloc,
            trimLeftSlash(suffix),
            max_backup_attempt_marker_bytes,
        ),
    };
    defer alloc.free(body);
    var parsed = try std.json.parseFromSlice(
        ClusterBackupAttemptMarker,
        alloc,
        body,
        .{ .allocate = .alloc_always },
    );
    errdefer parsed.deinit();
    try validateClusterBackupAttemptMarker(alloc, &parsed.value, attempt_id);
    return parsed;
}

const LatestClusterBackupAttempt = struct {
    attempt_id: []u8,
    created_at_unix_ns: u64,

    fn deinit(self: *LatestClusterBackupAttempt, alloc: std.mem.Allocator) void {
        alloc.free(self.attempt_id);
        self.* = undefined;
    }
};

fn considerLatestClusterBackupAttempt(
    alloc: std.mem.Allocator,
    latest: *?LatestClusterBackupAttempt,
    marker: *const ClusterBackupAttemptMarker,
) !void {
    if (latest.*) |current| {
        if (marker.created_at_unix_ns < current.created_at_unix_ns or
            (marker.created_at_unix_ns == current.created_at_unix_ns and
                std.mem.order(u8, marker.attempt_id, current.attempt_id) != .gt)) return;
        alloc.free(current.attempt_id);
    }
    latest.* = .{
        .attempt_id = try alloc.dupe(u8, marker.attempt_id),
        .created_at_unix_ns = marker.created_at_unix_ns,
    };
}

fn latestClusterBackupAttemptForHealth(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
) !?LatestClusterBackupAttempt {
    if (try readClusterBackupAttemptHead(alloc, io, location)) |parsed_value| {
        var parsed = parsed_value;
        defer parsed.deinit();
        return .{
            .attempt_id = try alloc.dupe(u8, parsed.value.attempt_id),
            // Ordering is established by atomic publication of the head, not
            // by a process-local wall clock.
            .created_at_unix_ns = 0,
        };
    }

    // Upgrade fallback for repositories created before the authoritative head
    // existed. New writes always publish the head, making the steady-state
    // health lookup O(1).
    var latest: ?LatestClusterBackupAttempt = null;
    errdefer if (latest) |*value| value.deinit(alloc);
    var scanned: usize = 0;
    switch (location.*) {
        .file => |backup_root| {
            const marker_root = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ backup_root, incomplete_backup_prefix });
            defer alloc.free(marker_root);
            var dir = std.Io.Dir.cwd().openDir(io, marker_root, .{ .iterate = true }) catch |err| switch (err) {
                error.FileNotFound => return null,
                else => return err,
            };
            defer dir.close(io);
            var iterator = dir.iterate();
            while (try iterator.next(io)) |entry| {
                if (entry.kind != .file or !std.mem.endsWith(u8, entry.name, ".json")) continue;
                if (scanned == backup_attempt_health_scan_budget)
                    return error.BackupAttemptHealthScanLimitExceeded;
                scanned += 1;
                const attempt_id = entry.name[0 .. entry.name.len - ".json".len];
                try validateBackupId(attempt_id);
                var parsed = try readClusterBackupAttemptMarker(alloc, io, location, attempt_id);
                defer parsed.deinit();
                try considerLatestClusterBackupAttempt(alloc, &latest, &parsed.value);
            }
        },
        .remote => |*store| {
            const key_prefix = try store.keyPrefixAlloc(alloc, incomplete_backup_prefix);
            defer alloc.free(key_prefix);
            var listed = try store.client.listObjects(store.bucket, .{
                .prefix = key_prefix,
                .recursive = false,
                .max_keys = @intCast(backup_attempt_health_scan_budget),
            });
            defer listed.deinit(alloc);
            if (listed.next_continuation_token != null)
                return error.BackupAttemptHealthScanLimitExceeded;
            for (listed.entries) |entry| {
                if (!std.mem.startsWith(u8, entry.key, key_prefix))
                    return error.InvalidBackupRequest;
                const name = entry.key[key_prefix.len..];
                if (std.mem.indexOfScalar(u8, name, '/') != null or
                    !std.mem.endsWith(u8, name, ".json"))
                    return error.InvalidBackupRequest;
                if (scanned == backup_attempt_health_scan_budget)
                    return error.BackupAttemptHealthScanLimitExceeded;
                scanned += 1;
                const attempt_id = name[0 .. name.len - ".json".len];
                try validateBackupId(attempt_id);
                var parsed = try readClusterBackupAttemptMarker(alloc, io, location, attempt_id);
                defer parsed.deinit();
                try considerLatestClusterBackupAttempt(alloc, &latest, &parsed.value);
            }
        },
    }
    return latest;
}

fn deleteFileOrTreeAbsolute(io: std.Io, path: []const u8) !bool {
    std.Io.Dir.deleteFileAbsolute(io, path) catch |err| switch (err) {
        error.FileNotFound => return false,
        error.IsDir => {
            try std.Io.Dir.cwd().deleteTree(io, path);
        },
        else => return err,
    };
    return true;
}

fn deletePathDurably(io: std.Io, path: []const u8) !void {
    _ = try deleteFileOrTreeAbsolute(io, path);
    // A retry can observe an already-absent path after a previous unlink
    // succeeded but its directory sync failed. Re-sync the parent in both
    // cases so successful cleanup always establishes a durable absence.
    try fs_paths.syncDirPortable(io, std.fs.path.dirname(path) orelse "/");
}

pub fn cleanupTableBackupAttemptAtLocation(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    backup_id: []const u8,
    artifact_backup_id: []const u8,
    format: BackupFormat,
) !void {
    var object_budget: usize = backup_attempt_cleanup_object_budget;
    return cleanupTableBackupAttemptAtLocationWithBudget(
        alloc,
        io,
        location,
        backup_id,
        artifact_backup_id,
        format,
        &object_budget,
        true,
    );
}

pub fn cleanupUnpublishedTableBackupAttemptAtLocation(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    backup_id: []const u8,
    artifact_backup_id: []const u8,
    format: BackupFormat,
) !void {
    var object_budget: usize = backup_attempt_cleanup_object_budget;
    return cleanupTableBackupAttemptAtLocationWithBudget(
        alloc,
        io,
        location,
        backup_id,
        artifact_backup_id,
        format,
        &object_budget,
        false,
    );
}

fn cleanupTableBackupAttemptAtLocationWithBudget(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    backup_id: []const u8,
    artifact_backup_id: []const u8,
    format: BackupFormat,
    object_budget: *usize,
    manifest_owned: bool,
) !void {
    try validateBackupId(backup_id);
    try validateBackupId(artifact_backup_id);
    switch (location.*) {
        .file => |backup_root| {
            if (manifest_owned) {
                const manifest_path = try metadataPath(alloc, backup_root, backup_id);
                defer alloc.free(manifest_path);
                // Remove and durably fence the table commit record before its
                // payload. A crash must never resurrect a manifest whose
                // artifact cleanup had already started.
                try deletePathDurably(io, manifest_path);
            }
            const artifact_path = switch (format) {
                .native => try std.fmt.allocPrint(alloc, "{s}/{s}", .{ backup_root, artifact_backup_id }),
                .portable => try std.fmt.allocPrint(alloc, "{s}/{s}.afb", .{ backup_root, artifact_backup_id }),
            };
            defer alloc.free(artifact_path);
            try deletePathDurably(io, artifact_path);
            const reservation_path = try reservationPath(alloc, backup_root, backup_id, false);
            defer alloc.free(reservation_path);
            try deletePathDurably(io, reservation_path);
        },
        .remote => |*store| {
            if (manifest_owned) {
                const manifest_suffix = try metadataPath(alloc, "", backup_id);
                defer alloc.free(manifest_suffix);
                try store.deleteSuffix(alloc, trimLeftSlash(manifest_suffix));
            }
            switch (format) {
                .native => try store.deletePrefix(alloc, artifact_backup_id, object_budget),
                .portable => {
                    const artifact_suffix = try std.fmt.allocPrint(alloc, "{s}.afb", .{artifact_backup_id});
                    defer alloc.free(artifact_suffix);
                    try store.deleteSuffix(alloc, artifact_suffix);
                },
            }
            const reservation_suffix = try reservationPath(alloc, "", backup_id, false);
            defer alloc.free(reservation_suffix);
            try store.deleteSuffix(alloc, trimLeftSlash(reservation_suffix));
        },
    }
}

pub fn deleteClusterBackupAttemptMarker(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    attempt_id: []const u8,
) !void {
    const suffix = try incompleteBackupMarkerPath(alloc, "", attempt_id);
    defer alloc.free(suffix);
    switch (location.*) {
        .file => |backup_root| {
            const path = try incompleteBackupMarkerPath(alloc, backup_root, attempt_id);
            defer alloc.free(path);
            try deletePathDurably(io, path);
        },
        .remote => |*store| try store.deleteSuffix(alloc, trimLeftSlash(suffix)),
    }
}

pub fn cleanupClusterReservationAtLocation(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    backup_id: []const u8,
) !void {
    const reservation_suffix = try reservationPath(alloc, "", backup_id, true);
    defer alloc.free(reservation_suffix);
    switch (location.*) {
        .file => |backup_root| {
            const path = try reservationPath(alloc, backup_root, backup_id, true);
            defer alloc.free(path);
            try deletePathDurably(io, path);
        },
        .remote => |*store| try store.deleteSuffix(alloc, trimLeftSlash(reservation_suffix)),
    }
}

pub fn cleanupClusterReservationIfOwnedAtLocation(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    backup_id: []const u8,
    attempt_id: []const u8,
) !bool {
    try validateBackupId(attempt_id);
    const reservation_suffix = try reservationPath(alloc, "", backup_id, true);
    defer alloc.free(reservation_suffix);
    return switch (location.*) {
        .file => |backup_root| blk: {
            const path = try reservationPath(alloc, backup_root, backup_id, true);
            defer alloc.free(path);
            const lock_path = try std.fmt.allocPrint(alloc, "{s}.publish.lock", .{path});
            defer alloc.free(lock_path);
            var lock_file = if (std.fs.path.isAbsolute(lock_path))
                try std.Io.Dir.createFileAbsolute(io, lock_path, .{ .truncate = false })
            else
                try std.Io.Dir.cwd().createFile(io, lock_path, .{ .truncate = false });
            defer lock_file.close(io);
            try lock_file.lock(io, .exclusive);
            defer lock_file.unlock(io);

            const body = readFileAbsoluteAllocWithIo(alloc, io, path, 131) catch |err| switch (err) {
                error.FileNotFound => break :blk false,
                else => return err,
            };
            defer alloc.free(body);
            if (!std.mem.eql(u8, std.mem.trimEnd(u8, body, "\r\n"), attempt_id))
                break :blk false;
            try deletePathDurably(io, path);
            break :blk true;
        },
        .remote => |*store| try store.deleteSuffixIfOwned(
            alloc,
            trimLeftSlash(reservation_suffix),
            attempt_id,
        ),
    };
}

fn clusterReservationOwnerMatchesAtLocation(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    backup_id: []const u8,
    attempt_id: []const u8,
) !?bool {
    try validateBackupId(attempt_id);
    const reservation_suffix = try reservationPath(alloc, "", backup_id, true);
    defer alloc.free(reservation_suffix);
    return switch (location.*) {
        .file => |backup_root| blk: {
            const path = try reservationPath(alloc, backup_root, backup_id, true);
            defer alloc.free(path);
            const lock_path = try std.fmt.allocPrint(alloc, "{s}.publish.lock", .{path});
            defer alloc.free(lock_path);
            var lock_file = if (std.fs.path.isAbsolute(lock_path))
                try std.Io.Dir.createFileAbsolute(io, lock_path, .{ .truncate = false })
            else
                try std.Io.Dir.cwd().createFile(io, lock_path, .{ .truncate = false });
            defer lock_file.close(io);
            try lock_file.lock(io, .exclusive);
            defer lock_file.unlock(io);
            const body = readFileAbsoluteAllocWithIo(alloc, io, path, 131) catch |err| switch (err) {
                error.FileNotFound => break :blk null,
                else => return err,
            };
            defer alloc.free(body);
            break :blk std.mem.eql(
                u8,
                std.mem.trimEnd(u8, body, "\r\n"),
                attempt_id,
            );
        },
        .remote => |*store| try store.suffixOwnerMatches(
            alloc,
            trimLeftSlash(reservation_suffix),
            attempt_id,
        ),
    };
}

pub fn cleanupClusterBackupAttemptAtLocation(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    marker: *const ClusterBackupAttemptMarker,
) !void {
    try validateClusterBackupAttemptMarker(alloc, marker, marker.attempt_id);
    var object_budget: usize = backup_attempt_cleanup_object_budget;
    var cleanup_error: ?anyerror = null;
    for (marker.tables) |table| {
        cleanupTableBackupAttemptAtLocationWithBudget(
            alloc,
            io,
            location,
            table.table_backup_id,
            table.artifact_backup_id,
            marker.format,
            &object_budget,
            true,
        ) catch |err| {
            if (cleanup_error == null) cleanup_error = err;
        };
    }
    if (cleanup_error) |err| return err;
    // Release the reservation for retry, but retain the lightweight marker as
    // bounded health history.
    _ = try cleanupClusterReservationIfOwnedAtLocation(
        alloc,
        io,
        location,
        marker.cluster_backup_id,
        marker.attempt_id,
    );
}

fn reclaimClusterBackupAttemptMarker(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    marker: *const ClusterBackupAttemptMarker,
    authoritative_attempt_id: ?[]const u8,
    object_budget: *usize,
) !bool {
    var committed = readClusterManifestFromLocation(
        alloc,
        location,
        marker.cluster_backup_id,
    ) catch |err| switch (err) {
        // Age alone is not proof of abandonment. The immutable reservation
        // owner is the authority: retain live work, and reclaim only after the
        // owner has released the fence or a retry has replaced it.
        error.FileNotFound => {
            if ((try clusterReservationOwnerMatchesAtLocation(
                alloc,
                io,
                location,
                marker.cluster_backup_id,
                marker.attempt_id,
            )) == true) return false;
            var cleanup_error: ?anyerror = null;
            for (marker.tables) |table| {
                cleanupTableBackupAttemptAtLocationWithBudget(
                    alloc,
                    io,
                    location,
                    table.table_backup_id,
                    table.artifact_backup_id,
                    marker.format,
                    object_budget,
                    true,
                ) catch |cleanup_err| {
                    if (cleanup_error == null) cleanup_error = cleanup_err;
                };
            }
            if (cleanup_error) |cleanup_err| return cleanup_err;
            if (authoritative_attempt_id) |attempt_id| {
                if (!std.mem.eql(u8, marker.attempt_id, attempt_id))
                    try deleteClusterBackupAttemptMarker(alloc, io, location, marker.attempt_id);
            }
            return true;
        },
        else => return err,
    };
    defer committed.deinit(alloc);

    const attempt_id = authoritative_attempt_id orelse return false;
    if (std.mem.eql(u8, marker.attempt_id, attempt_id)) return false;
    // The head, not every historical journal, is ordering authority.
    try deleteClusterBackupAttemptMarker(alloc, io, location, marker.attempt_id);
    return true;
}

fn markerIsStale(marker: *const ClusterBackupAttemptMarker, now_unix_ns: u64) bool {
    if (marker.created_at_unix_ns > now_unix_ns) return false;
    return now_unix_ns - marker.created_at_unix_ns >= backup_attempt_reclaim_age_ns;
}

fn reclaimClusterBackupAttemptById(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    attempt_id: []const u8,
    now_unix_ns: u64,
    authoritative_attempt_id: ?[]const u8,
    object_budget: *usize,
) !bool {
    var parsed = try readClusterBackupAttemptMarker(alloc, io, location, attempt_id);
    defer parsed.deinit();
    if (!markerIsStale(&parsed.value, now_unix_ns)) return false;
    return try reclaimClusterBackupAttemptMarker(
        alloc,
        io,
        location,
        &parsed.value,
        authoritative_attempt_id,
        object_budget,
    );
}

pub fn reclaimStaleClusterBackupAttempts(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    now_unix_ns: u64,
) !usize {
    var reclaimed: usize = 0;
    var object_budget: usize = backup_attempt_reclaim_object_budget;
    var parsed_head = try readClusterBackupAttemptHead(alloc, io, location);
    defer if (parsed_head) |*head| head.deinit();
    const authoritative_attempt_id: ?[]const u8 = if (parsed_head) |*head|
        head.value.attempt_id
    else
        null;
    switch (location.*) {
        .file => |backup_root| {
            const marker_root = try std.fmt.allocPrint(alloc, "{s}/{s}", .{
                backup_root,
                incomplete_backup_prefix,
            });
            defer alloc.free(marker_root);
            var dir = std.Io.Dir.cwd().openDir(io, marker_root, .{ .iterate = true }) catch |err| switch (err) {
                error.FileNotFound => return 0,
                else => return err,
            };
            defer dir.close(io);
            var iterator = dir.iterate();
            var scanned: usize = 0;
            while (scanned < backup_attempt_reclaim_scan_budget and
                reclaimed < backup_attempt_reclaim_batch_size)
            {
                const entry = try iterator.next(io) orelse break;
                scanned += 1;
                if (entry.kind != .file or !std.mem.endsWith(u8, entry.name, ".json")) continue;
                const attempt_id = entry.name[0 .. entry.name.len - ".json".len];
                validateBackupId(attempt_id) catch continue;
                if (reclaimClusterBackupAttemptById(
                    alloc,
                    io,
                    location,
                    attempt_id,
                    now_unix_ns,
                    authoritative_attempt_id,
                    &object_budget,
                ) catch |err| {
                    std.log.warn("stale backup attempt reclamation deferred phase=local class={s}", .{@errorName(err)});
                    continue;
                }) {
                    reclaimed += 1;
                }
            }
        },
        .remote => |*store| {
            const key_prefix = try store.keyPrefixAlloc(alloc, incomplete_backup_prefix);
            defer alloc.free(key_prefix);
            var listed = try store.client.listObjects(store.bucket, .{
                .prefix = key_prefix,
                .recursive = true,
                .max_keys = @intCast(backup_attempt_reclaim_scan_budget),
            });
            defer listed.deinit(alloc);
            for (listed.entries) |entry| {
                if (reclaimed >= backup_attempt_reclaim_batch_size) break;
                if (!std.mem.startsWith(u8, entry.key, key_prefix)) continue;
                const name = entry.key[key_prefix.len..];
                if (std.mem.indexOfScalar(u8, name, '/') != null or
                    !std.mem.endsWith(u8, name, ".json"))
                {
                    continue;
                }
                const attempt_id = name[0 .. name.len - ".json".len];
                validateBackupId(attempt_id) catch continue;
                if (reclaimClusterBackupAttemptById(
                    alloc,
                    io,
                    location,
                    attempt_id,
                    now_unix_ns,
                    authoritative_attempt_id,
                    &object_budget,
                ) catch |err| {
                    std.log.warn("stale backup attempt reclamation deferred phase=remote class={s}", .{@errorName(err)});
                    continue;
                }) {
                    reclaimed += 1;
                }
            }
        },
    }
    return reclaimed;
}

pub fn shardSnapshotPath(alloc: std.mem.Allocator, backup_root: []const u8, backup_id: []const u8, group_id: u64) ![]u8 {
    try validateBackupId(backup_id);
    return try std.fmt.allocPrint(alloc, "{s}/{s}/groups/{d}", .{ backup_root, backup_id, group_id });
}

pub fn shardSnapshotRelPath(alloc: std.mem.Allocator, backup_id: []const u8, group_id: u64) ![]u8 {
    try validateBackupId(backup_id);
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
        const name = try alloc.dupe(u8, entry.name);
        errdefer alloc.free(name);
        const owned_table_backup_id = try alloc.dupe(u8, entry.table_backup_id);
        errdefer alloc.free(owned_table_backup_id);
        const owned_artifact_backup_id = if (entry.artifact_backup_id) |value|
            try alloc.dupe(u8, value)
        else
            null;
        owned_entries[i] = .{
            .name = name,
            .table_backup_id = owned_table_backup_id,
            .artifact_backup_id = owned_artifact_backup_id,
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
        .expected_table_count = owned_entries.len,
        .completed_table_count = owned_entries.len,
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
    if (manifest.format_version != cluster_format_version)
        return error.UnsupportedBackupFormat;
    try validateClusterManifest(alloc, manifest, manifest.backup_id);
    const path = try clusterMetadataPath(alloc, backup_root, manifest.backup_id);
    defer alloc.free(path);
    try ensureDirPath(backup_root);

    const encoded = try stringifyJsonAlloc(alloc, manifest.*);
    defer alloc.free(encoded);
    try ensureManifestSize(encoded, max_backup_manifest_bytes);
    try writeFileAbsoluteIfAbsent(alloc, path, encoded);
}

pub fn readClusterManifest(
    alloc: std.mem.Allocator,
    backup_root: []const u8,
    backup_id: []const u8,
) !ClusterBackupManifest {
    const path = try clusterMetadataPath(alloc, backup_root, backup_id);
    defer alloc.free(path);
    const body = try readFileAbsoluteAlloc(alloc, path, max_backup_manifest_bytes);
    defer alloc.free(body);
    return try parseClusterManifestBytes(alloc, body, backup_id);
}

pub fn writeClusterManifestToLocation(
    alloc: std.mem.Allocator,
    location: *BackupLocation,
    manifest: *const ClusterBackupManifest,
) !void {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    return writeClusterManifestToLocationWithIo(alloc, io_impl.io(), location, manifest);
}

pub fn writeClusterManifestToLocationWithIo(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    manifest: *const ClusterBackupManifest,
) !void {
    if (manifest.format_version != cluster_format_version)
        return error.UnsupportedBackupFormat;
    try validateClusterManifest(alloc, manifest, manifest.backup_id);
    switch (location.*) {
        .file => |backup_root| {
            const path = try clusterMetadataPath(alloc, backup_root, manifest.backup_id);
            defer alloc.free(path);
            const encoded = try stringifyJsonAlloc(alloc, manifest.*);
            defer alloc.free(encoded);
            try ensureManifestSize(encoded, max_backup_manifest_bytes);
            try writeFileAbsoluteIfAbsentWithIo(alloc, io, path, encoded);
        },
        .remote => |*store| {
            const encoded = try stringifyJsonAlloc(alloc, manifest.*);
            defer alloc.free(encoded);
            try ensureManifestSize(encoded, max_backup_manifest_bytes);
            const suffix = try clusterMetadataPath(alloc, "", manifest.backup_id);
            defer alloc.free(suffix);
            try store.writeBytesIfAbsent(alloc, trimLeftSlash(suffix), encoded, "application/json");
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
            const body = try store.readBytesAllocLimited(alloc, trimLeftSlash(suffix), max_backup_manifest_bytes);
            defer alloc.free(body);
            return try parseClusterManifestBytes(alloc, body, backup_id);
        },
    }
}

fn readClusterManifestFromRemoteKey(
    alloc: std.mem.Allocator,
    store: *RemoteBackupStore,
    key: []const u8,
    listed_size: u64,
    backup_id: []const u8,
) !ClusterBackupManifest {
    const body = try store.readKeyBytesAllocLimited(
        alloc,
        key,
        max_backup_manifest_bytes,
        .{
            .known_size = listed_size,
            .skip_metadata_probe = true,
        },
    );
    defer alloc.free(body);
    return try parseClusterManifestBytes(alloc, body, backup_id);
}

fn parseClusterManifestBytes(
    alloc: std.mem.Allocator,
    body: []const u8,
    backup_id: []const u8,
) !ClusterBackupManifest {
    var value = try std.json.parseFromSlice(std.json.Value, alloc, body, .{});
    defer value.deinit();
    const root = switch (value.value) {
        .object => |object| object,
        else => return error.InvalidBackupRequest,
    };
    if (root.get("format_version") != null) {
        var parsed = try std.json.parseFromSlice(ClusterBackupManifest, alloc, body, .{ .allocate = .alloc_always });
        defer parsed.deinit();
        try validateClusterManifest(alloc, &parsed.value, backup_id);
        return try cloneClusterBackupManifest(alloc, parsed.value);
    }
    return try parseGoPortableClusterManifest(alloc, root, backup_id);
}

fn parseGoPortableClusterManifest(
    alloc: std.mem.Allocator,
    root: std.json.ObjectMap,
    backup_id: []const u8,
) !ClusterBackupManifest {
    // This is the exact current Go aggregate envelope, not a permissive
    // compatibility path. Keeping the root shape closed prevents an unrelated
    // or future version from being silently interpreted as restorable.
    if (root.count() != 9) return error.InvalidBackupRequest;
    const version = switch (root.get("version") orelse return error.InvalidBackupRequest) {
        .integer => |value| value,
        else => return error.InvalidBackupRequest,
    };
    if (version != 2) return error.UnsupportedBackupFormat;
    const state = switch (root.get("state") orelse return error.InvalidBackupRequest) {
        .string => |value| value,
        else => return error.InvalidBackupRequest,
    };
    if (!std.mem.eql(u8, state, "complete")) return error.IncompleteClusterBackup;
    const encoded_backup_id = switch (root.get("backup_id") orelse return error.InvalidBackupRequest) {
        .string => |value| value,
        else => return error.InvalidBackupRequest,
    };
    if (!std.mem.eql(u8, encoded_backup_id, backup_id)) return error.InvalidBackupRequest;
    const format = switch (root.get("format") orelse return error.InvalidBackupRequest) {
        .string => |value| value,
        else => return error.InvalidBackupRequest,
    };
    if (!std.mem.eql(u8, format, "portable")) return error.UnsupportedBackupFormat;
    const timestamp = switch (root.get("timestamp") orelse return error.InvalidBackupRequest) {
        .string => |value| value,
        else => return error.InvalidBackupRequest,
    };
    const source_version = switch (root.get("antfly_version") orelse return error.InvalidBackupRequest) {
        .string => |value| value,
        else => return error.InvalidBackupRequest,
    };
    const expected = try nonNegativeJsonIntegerAsUsize(
        root.get("expected_table_count") orelse return error.InvalidBackupRequest,
    );
    const completed = try nonNegativeJsonIntegerAsUsize(
        root.get("completed_table_count") orelse return error.InvalidBackupRequest,
    );
    const table_values = switch (root.get("tables") orelse return error.InvalidBackupRequest) {
        .array => |value| value,
        else => return error.InvalidBackupRequest,
    };
    if (expected == 0 or expected > max_cluster_backup_attempt_tables or
        completed != expected or table_values.items.len != expected)
    {
        return error.IncompleteClusterBackup;
    }

    const tables = try alloc.alloc(ClusterTableBackupEntry, table_values.items.len);
    var initialized: usize = 0;
    var common_location: ?[]u8 = null;
    errdefer {
        for (tables[0..initialized]) |*table| table.deinit(alloc);
        alloc.free(tables);
        if (common_location) |location| alloc.free(location);
    }
    for (table_values.items, 0..) |table_value, i| {
        const table = switch (table_value) {
            .object => |value| value,
            else => return error.InvalidBackupRequest,
        };
        if (table.count() != 4) return error.InvalidBackupRequest;
        const name = switch (table.get("name") orelse return error.InvalidBackupRequest) {
            .string => |value| value,
            else => return error.InvalidBackupRequest,
        };
        const status = switch (table.get("status") orelse return error.InvalidBackupRequest) {
            .string => |value| value,
            else => return error.InvalidBackupRequest,
        };
        if (!std.mem.eql(u8, status, "completed")) return error.IncompleteClusterBackup;
        _ = try nonNegativeJsonIntegerAsUsize(
            table.get("shard_count") orelse return error.InvalidBackupRequest,
        );
        const backup_location = switch (table.get("backup_location") orelse return error.InvalidBackupRequest) {
            .string => |value| value,
            else => return error.InvalidBackupRequest,
        };
        const split = try goTableMetadataLocationParts(backup_location);
        if (common_location) |location| {
            if (!std.mem.eql(u8, location, split.root)) return error.InvalidBackupRequest;
        } else {
            common_location = try alloc.dupe(u8, split.root);
        }
        const owned_name = try alloc.dupe(u8, name);
        errdefer alloc.free(owned_name);
        const owned_table_backup_id = try alloc.dupe(u8, split.backup_id);
        errdefer alloc.free(owned_table_backup_id);
        const owned_artifact_backup_id = try alloc.dupe(u8, backup_id);
        tables[i] = .{
            .name = owned_name,
            .table_backup_id = owned_table_backup_id,
            .artifact_backup_id = owned_artifact_backup_id,
        };
        initialized += 1;
    }

    const owned_backup_id = try alloc.dupe(u8, backup_id);
    errdefer alloc.free(owned_backup_id);
    const owned_timestamp = try alloc.dupe(u8, timestamp);
    errdefer alloc.free(owned_timestamp);
    const owned_source_version = try alloc.dupe(u8, source_version);
    errdefer alloc.free(owned_source_version);
    var manifest: ClusterBackupManifest = .{
        .backup_id = owned_backup_id,
        .timestamp = owned_timestamp,
        .location = common_location.?,
        .antfly_version = owned_source_version,
        .expected_table_count = expected,
        .completed_table_count = completed,
        .tables = tables,
    };
    try validateClusterManifest(alloc, &manifest, backup_id);
    common_location = null;
    return manifest;
}

fn nonNegativeJsonIntegerAsUsize(value: std.json.Value) !usize {
    const integer = switch (value) {
        .integer => |item| item,
        else => return error.InvalidBackupRequest,
    };
    if (integer < 0) return error.InvalidBackupRequest;
    return std.math.cast(usize, integer) orelse error.InvalidBackupRequest;
}

const GoTableMetadataLocationParts = struct {
    root: []const u8,
    backup_id: []const u8,
};

fn goTableMetadataLocationParts(location: []const u8) !GoTableMetadataLocationParts {
    const slash = std.mem.lastIndexOfScalar(u8, location, '/') orelse
        return error.InvalidBackupRequest;
    const leaf = location[slash + 1 ..];
    const suffix = "-metadata.json";
    if (leaf.len <= suffix.len or !std.mem.endsWith(u8, leaf, suffix))
        return error.InvalidBackupRequest;
    const id = leaf[0 .. leaf.len - suffix.len];
    try validateBackupId(id);
    return .{ .root = location[0..slash], .backup_id = id };
}

pub fn clusterManifestExistsAtLocation(alloc: std.mem.Allocator, location: *BackupLocation, backup_id: []const u8) !bool {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    return clusterManifestExistsAtLocationWithIo(alloc, io_impl.io(), location, backup_id);
}

pub fn clusterManifestExistsAtLocationWithIo(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    backup_id: []const u8,
) !bool {
    return switch (location.*) {
        .file => |backup_root| blk: {
            const path = try clusterMetadataPath(alloc, backup_root, backup_id);
            defer alloc.free(path);
            break :blk try pathExistsWithIo(io, path);
        },
        .remote => false,
    };
}

fn validateClusterManifest(
    alloc: std.mem.Allocator,
    manifest: *const ClusterBackupManifest,
    requested_backup_id: []const u8,
) !void {
    if (manifest.format_version != cluster_format_version)
        return error.UnsupportedBackupFormat;
    try validateBackupId(manifest.backup_id);
    if (!std.mem.eql(u8, manifest.backup_id, requested_backup_id)) return error.InvalidBackupRequest;
    if (manifest.state != .complete or
        manifest.expected_table_count == 0 or
        manifest.expected_table_count > max_cluster_backup_attempt_tables or
        manifest.expected_table_count != manifest.tables.len or
        manifest.completed_table_count != manifest.expected_table_count)
    {
        return error.IncompleteClusterBackup;
    }
    var table_names = std.StringHashMapUnmanaged(void).empty;
    defer table_names.deinit(alloc);
    var table_backup_ids = std.StringHashMapUnmanaged(void).empty;
    defer table_backup_ids.deinit(alloc);
    try table_names.ensureTotalCapacity(alloc, @intCast(manifest.tables.len));
    try table_backup_ids.ensureTotalCapacity(alloc, @intCast(manifest.tables.len));

    for (manifest.tables) |table| {
        if (table.name.len == 0 or table.name.len > 4096) return error.InvalidBackupRequest;
        try validateBackupId(table.table_backup_id);
        if (table.artifact_backup_id) |artifact_backup_id|
            try validateBackupId(artifact_backup_id);
        if (std.mem.eql(u8, table.table_backup_id, manifest.backup_id) or
            table_names.contains(table.name) or
            table_backup_ids.contains(table.table_backup_id))
        {
            return error.InvalidBackupRequest;
        }
        table_names.putAssumeCapacity(table.name, {});
        table_backup_ids.putAssumeCapacity(table.table_backup_id, {});
    }
}

fn validateLocalArtifactAvailable(
    alloc: std.mem.Allocator,
    io: std.Io,
    backup_root: []const u8,
    format: BackupFormat,
    integrity_mode: ArtifactIntegrityMode,
    shard: *const ShardSnapshot,
) !void {
    const artifact_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{
        backup_root,
        shard.snapshot_path,
    });
    defer alloc.free(artifact_path);
    switch (format) {
        .portable => {
            const stat = try std.Io.Dir.cwd().statFile(io, artifact_path, .{});
            if (integrity_mode == .declared and stat.size != shard.artifact_size_bytes)
                return error.BackupArtifactIntegrityMismatch;
        },
        .native => {
            var dir = try std.Io.Dir.cwd().openDir(io, artifact_path, .{ .iterate = true });
            defer dir.close(io);
            var walker = try dir.walk(alloc);
            defer walker.deinit();
            while (try walker.next(io)) |entry| {
                if (entry.kind == .file) return;
            }
            return error.BackupArtifactMissing;
        },
    }
}

/// Performs bounded metadata-only admission for a restorable cluster backup.
/// Table manifests are fully parsed and every referenced artifact is checked
/// for presence; portable artifacts additionally bind the advertised byte
/// length. Full SHA-256 verification remains part of restore/materialization so
/// listing never downloads multi-gigabyte backup payloads.
pub fn validateClusterBackupArtifactsAtLocation(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    manifest: *const ClusterBackupManifest,
) !void {
    try validateClusterManifest(alloc, manifest, manifest.backup_id);
    for (manifest.tables) |table| {
        var table_manifest = try readManifestFromLocationWithArtifactBackupId(
            alloc,
            location,
            table.table_backup_id,
            table.artifact_backup_id orelse table.table_backup_id,
        );
        defer table_manifest.deinit(alloc);
        if (!std.mem.eql(u8, table_manifest.table_name, table.name))
            return error.InvalidBackupRequest;
        for (table_manifest.shards) |*shard| switch (location.*) {
            .file => |backup_root| try validateLocalArtifactAvailable(
                alloc,
                io,
                backup_root,
                table_manifest.format,
                table_manifest.artifact_integrity_mode,
                shard,
            ),
            .remote => |*store| try store.validateArtifactAvailable(
                alloc,
                table_manifest.format,
                table_manifest.artifact_integrity_mode,
                shard,
            ),
        };
    }
}

/// Cryptographically verifies a complete cluster backup with bounded memory.
/// This is reserved for health/restore admission; ordinary pagination keeps
/// using metadata-only checks to avoid repeatedly downloading payloads.
pub fn verifyClusterBackupArtifactsIntegrityAtLocation(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    manifest: *const ClusterBackupManifest,
) !void {
    try validateClusterManifest(alloc, manifest, manifest.backup_id);
    for (manifest.tables) |table| {
        var table_manifest = try readManifestFromLocationWithArtifactBackupId(
            alloc,
            location,
            table.table_backup_id,
            table.artifact_backup_id orelse table.table_backup_id,
        );
        defer table_manifest.deinit(alloc);
        if (!std.mem.eql(u8, table_manifest.table_name, table.name))
            return error.InvalidBackupRequest;
        if (table_manifest.artifact_integrity_mode != .declared)
            return error.BackupIntegrityMissing;
        for (table_manifest.shards) |*shard| switch (location.*) {
            .file => |backup_root| {
                const artifact_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{
                    backup_root,
                    shard.snapshot_path,
                });
                defer alloc.free(artifact_path);
                verifyShardArtifactIntegrity(
                    alloc,
                    io,
                    table_manifest.format,
                    artifact_path,
                    shard,
                ) catch |err| switch (err) {
                    error.FileNotFound => return error.BackupArtifactMissing,
                    else => return err,
                };
            },
            .remote => |*store| store.verifyArtifactIntegrity(
                alloc,
                table_manifest.format,
                shard,
            ) catch |err| switch (err) {
                error.FileNotFound => return error.BackupArtifactMissing,
                else => return err,
            },
        };
    }
}

pub fn ensureNewestClusterBackupAttemptRestorable(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
) !void {
    var latest = (try latestClusterBackupAttemptForHealth(alloc, io, location)) orelse return;
    defer latest.deinit(alloc);
    var marker = try readClusterBackupAttemptMarker(alloc, io, location, latest.attempt_id);
    defer marker.deinit();
    var manifest = readClusterManifestFromLocation(
        alloc,
        location,
        marker.value.cluster_backup_id,
    ) catch |err| switch (err) {
        error.FileNotFound => return error.IncompleteClusterBackup,
        else => return err,
    };
    defer manifest.deinit(alloc);
    if (manifest.format_version != cluster_format_version or
        manifest.expected_table_count != marker.value.tables.len or
        manifest.completed_table_count != marker.value.tables.len)
        return error.IncompleteClusterBackup;
    for (marker.value.tables) |attempt_table| {
        const committed_table = findClusterTable(&manifest, attempt_table.name) orelse
            return error.IncompleteClusterBackup;
        if (!std.mem.eql(u8, committed_table.table_backup_id, attempt_table.table_backup_id))
            return error.IncompleteClusterBackup;
        if (committed_table.artifact_backup_id) |artifact_backup_id| {
            if (!std.mem.eql(u8, artifact_backup_id, attempt_table.artifact_backup_id))
                return error.IncompleteClusterBackup;
        }
    }
    try verifyClusterBackupArtifactsIntegrityAtLocation(alloc, io, location, &manifest);
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

pub fn encodeBackupListResponse(alloc: std.mem.Allocator, page: *const BackupListPage) ![]u8 {
    if (page.next_cursor) |cursor| return try stringifyJsonAlloc(alloc, .{
        .backups = page.backups,
        .next_cursor = cursor,
    });
    return try stringifyJsonAlloc(alloc, .{ .backups = page.backups });
}

fn validateBackupListOptions(options: BackupListOptions) !void {
    if (options.limit == 0 or options.limit > max_backup_list_limit) return error.InvalidBackupListLimit;
    if (options.cursor) |cursor| try validateBackupId(cursor);
}

fn maxHeapSiftUp(ids: [][]u8, start_index: usize) void {
    var index = start_index;
    while (index > 0) {
        const parent = (index - 1) / 2;
        if (std.mem.order(u8, ids[parent], ids[index]) != .lt) break;
        std.mem.swap([]u8, &ids[parent], &ids[index]);
        index = parent;
    }
}

fn maxHeapSiftDown(ids: [][]u8, start_index: usize) void {
    var index = start_index;
    while (true) {
        const left = index * 2 + 1;
        if (left >= ids.len) return;
        const right = left + 1;
        const greatest = if (right < ids.len and std.mem.order(u8, ids[left], ids[right]) == .lt) right else left;
        if (std.mem.order(u8, ids[index], ids[greatest]) != .lt) return;
        std.mem.swap([]u8, &ids[index], &ids[greatest]);
        index = greatest;
    }
}

fn retainSmallestBackupId(alloc: std.mem.Allocator, ids: *std.ArrayListUnmanaged([]u8), capacity: usize, backup_id: []const u8) !void {
    if (ids.items.len == capacity and std.mem.order(u8, backup_id, ids.items[0]) != .lt) return;
    const owned = try alloc.dupe(u8, backup_id);
    errdefer alloc.free(owned);
    if (ids.items.len < capacity) {
        try ids.append(alloc, owned);
        maxHeapSiftUp(ids.items, ids.items.len - 1);
        return;
    }
    alloc.free(ids.items[0]);
    ids.items[0] = owned;
    maxHeapSiftDown(ids.items, 0);
}

fn couldRetainSmallestBackupId(ids: *const std.ArrayListUnmanaged([]u8), capacity: usize, backup_id: []const u8) bool {
    return ids.items.len < capacity or std.mem.order(u8, backup_id, ids.items[0]) == .lt;
}

fn backupIdLessThan(_: void, lhs: []u8, rhs: []u8) bool {
    return std.mem.order(u8, lhs, rhs) == .lt;
}

fn backupInfoFromManifest(alloc: std.mem.Allocator, manifest: *const ClusterBackupManifest, location: []const u8) !BackupInfo {
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
    const backup_id = try alloc.dupe(u8, manifest.backup_id);
    errdefer alloc.free(backup_id);
    const timestamp = try alloc.dupe(u8, manifest.timestamp);
    errdefer alloc.free(timestamp);
    const owned_location = try alloc.dupe(u8, location);
    errdefer alloc.free(owned_location);
    const version = try alloc.dupe(u8, manifest.antfly_version);
    return .{
        .backup_id = backup_id,
        .timestamp = timestamp,
        .tables = tables,
        .location = owned_location,
        .antfly_version = version,
    };
}

pub fn listClusterBackups(alloc: std.mem.Allocator, backup_root: []const u8, location: []const u8, options: BackupListOptions) !BackupListPage {
    try validateBackupListOptions(options);
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    var dir = std.Io.Dir.cwd().openDir(io, backup_root, .{ .iterate = true }) catch |err| switch (err) {
        error.FileNotFound => return .{ .backups = try alloc.alloc(BackupInfo, 0) },
        else => return err,
    };
    defer dir.close(io);

    var it = dir.iterate();
    var backup_ids = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (backup_ids.items) |backup_id| alloc.free(backup_id);
        backup_ids.deinit(alloc);
    }
    const cursor_key = if (options.cursor) |cursor| try clusterMetadataPath(alloc, "", cursor) else null;
    defer if (cursor_key) |value| alloc.free(value);
    const cursor_name = if (cursor_key) |value| std.fs.path.basename(value) else null;
    while (try it.next(io)) |entry| {
        if (entry.kind != .file) continue;
        if (!std.mem.endsWith(u8, entry.name, "-cluster-metadata.json")) continue;
        const backup_id = entry.name[0 .. entry.name.len - "-cluster-metadata.json".len];
        validateBackupId(backup_id) catch {
            logBackupListSkipped(.key_validation, "invalid_manifest");
            continue;
        };
        if (cursor_name) |cursor| if (std.mem.order(u8, entry.name, cursor) != .gt) continue;
        if (!couldRetainSmallestBackupId(&backup_ids, options.limit + 1, entry.name)) continue;
        // Validate before admitting the key into the bounded selection heap.
        // Otherwise corrupt manifests can consume page capacity and make valid
        // backups beyond them unreachable. Noncompetitive keys do not incur a
        // manifest read.
        var validated = readClusterManifest(alloc, backup_root, backup_id) catch |err| {
            if (!isSkippableBackupListManifestError(err)) {
                logBackupListFailure(.manifest_read, backupListManifestErrorClass(err));
                return err;
            }
            logBackupListSkipped(.manifest_read, backupListManifestErrorClass(err));
            continue;
        };
        if (validated.format_version != cluster_format_version) {
            validated.deinit(alloc);
            continue;
        }
        validated.deinit(alloc);
        try retainSmallestBackupId(alloc, &backup_ids, options.limit + 1, entry.name);
    }
    std.mem.sort([]u8, backup_ids.items, {}, backupIdLessThan);
    const has_more = backup_ids.items.len > options.limit;
    if (has_more) alloc.free(backup_ids.pop().?);

    var infos = std.ArrayListUnmanaged(BackupInfo).empty;
    errdefer {
        for (infos.items) |info| freeBackupInfo(alloc, info);
        infos.deinit(alloc);
    }
    try infos.ensureTotalCapacity(alloc, backup_ids.items.len);
    for (backup_ids.items) |manifest_name| {
        const backup_id = backupIdFromClusterMetadataKey(manifest_name);
        var manifest = readClusterManifest(alloc, backup_root, backup_id) catch |err| {
            if (!isSkippableBackupListManifestError(err)) {
                logBackupListFailure(.manifest_read, backupListManifestErrorClass(err));
                return err;
            }
            logBackupListSkipped(.manifest_read, backupListManifestErrorClass(err));
            continue;
        };
        defer manifest.deinit(alloc);
        if (manifest.format_version != cluster_format_version) continue;
        infos.appendAssumeCapacity(try backupInfoFromManifest(alloc, &manifest, location));
    }
    const next_cursor = if (has_more and infos.items.len > 0) try alloc.dupe(u8, infos.items[infos.items.len - 1].backup_id) else null;
    errdefer if (next_cursor) |cursor| alloc.free(cursor);
    return .{ .backups = try infos.toOwnedSlice(alloc), .next_cursor = next_cursor };
}

pub fn listClusterBackupsFromLocation(
    alloc: std.mem.Allocator,
    location_uri: []const u8,
    options: BackupListOptions,
) !BackupListPage {
    var location = try openBackupLocation(alloc, location_uri);
    defer location.deinit(alloc);

    if (location == .file) {
        return try listClusterBackups(alloc, location.file, location_uri, options);
    }

    return try listClusterBackupsFromOpenedLocation(alloc, &location, location_uri, options);
}

pub fn backupListErrorClass(err: anyerror) []const u8 {
    return switch (err) {
        error.FileNotFound => "not_found",
        error.AccessDenied, error.Unauthorized => "access_denied",
        error.Timeout,
        error.ConnectionTimeout,
        error.ConnectionTimedOut,
        => "timeout",
        error.RemoteUnavailable,
        error.RateLimited,
        error.UnexpectedHttpStatus,
        error.ConnectionFailed,
        error.ConnectionReset,
        error.ConnectionRefused,
        error.ConnectionResetByPeer,
        error.ConnectionClosed,
        error.NetworkUnreachable,
        error.NetworkDown,
        error.HostUnreachable,
        error.DnsResolutionFailed,
        error.TlsHandshakeFailed,
        error.TlsCertificateError,
        error.TlsError,
        error.RecvFailed,
        error.SendFailed,
        error.ProtocolError,
        error.StreamError,
        error.FlowControlError,
        error.FrameError,
        error.CompressionError,
        error.Http2Error,
        error.Http3Error,
        error.QuicError,
        => "transport",
        else => "internal",
    };
}

pub const BackupListDiagnosticPhase = enum {
    enumerate,
    key_validation,
    manifest_read,
    request,
};

const BackupListDiagnosticOutcome = enum {
    failed,
    skipped,
};

fn formatBackupListDiagnostic(
    buffer: []u8,
    outcome: BackupListDiagnosticOutcome,
    phase: BackupListDiagnosticPhase,
    class: []const u8,
) ![]const u8 {
    return try std.fmt.bufPrint(
        buffer,
        "cluster backup list {s} phase={s} class={s}",
        .{ @tagName(outcome), @tagName(phase), class },
    );
}

fn logBackupListFailure(phase: BackupListDiagnosticPhase, class: []const u8) void {
    var buffer: [160]u8 = undefined;
    const diagnostic = formatBackupListDiagnostic(&buffer, .failed, phase, class) catch
        "cluster backup list failed phase=request class=internal";
    std.log.err("{s}", .{diagnostic});
}

fn logBackupListSkipped(phase: BackupListDiagnosticPhase, class: []const u8) void {
    var buffer: [160]u8 = undefined;
    const diagnostic = formatBackupListDiagnostic(&buffer, .skipped, phase, class) catch
        "cluster backup list skipped phase=manifest_read class=internal";
    std.log.warn("{s}", .{diagnostic});
}

pub fn logBackupListRequestFailure(err: anyerror) void {
    logBackupListFailure(.request, backupListErrorClass(err));
}

fn backupListManifestErrorClass(err: anyerror) []const u8 {
    if (isInvalidBackupListManifestError(err)) return "invalid_manifest";
    const storage_class = backupListErrorClass(err);
    return storage_class;
}

fn isSkippableBackupListManifestError(err: anyerror) bool {
    return err == error.FileNotFound or isInvalidBackupListManifestError(err);
}

fn isInvalidBackupListManifestError(err: anyerror) bool {
    return switch (err) {
        error.SyntaxError,
        error.UnexpectedEndOfInput,
        error.UnexpectedToken,
        error.InvalidNumber,
        error.InvalidCharacter,
        error.Overflow,
        error.InvalidEnumTag,
        error.DuplicateField,
        error.UnknownField,
        error.MissingField,
        error.LengthMismatch,
        error.ValueTooLong,
        error.BufferUnderrun,
        error.UnsupportedBackupFormat,
        error.InvalidBackupId,
        error.InvalidBackupRequest,
        error.IncompleteClusterBackup,
        error.BackupManifestTooLarge,
        => true,
        else => false,
    };
}

fn backupIdFromListedRemoteClusterMetadataKey(
    store: *const RemoteBackupStore,
    key: []const u8,
) ?[]const u8 {
    const canonical_prefix = trimRightSlash(store.prefix);
    const relative = if (canonical_prefix.len == 0)
        key
    else blk: {
        if (!std.mem.startsWith(u8, key, canonical_prefix) or
            key.len <= canonical_prefix.len or
            key[canonical_prefix.len] != '/')
        {
            return null;
        }
        break :blk key[canonical_prefix.len + 1 ..];
    };
    const suffix = "-cluster-metadata.json";
    if (relative.len <= suffix.len or
        std.mem.indexOfScalar(u8, relative, '/') != null or
        !std.mem.endsWith(u8, relative, suffix))
    {
        return null;
    }
    return relative[0 .. relative.len - suffix.len];
}

pub fn listClusterBackupsFromOpenedLocation(
    alloc: std.mem.Allocator,
    location: *BackupLocation,
    location_uri: []const u8,
    options: BackupListOptions,
) !BackupListPage {
    try validateBackupListOptions(options);
    switch (location.*) {
        .file => |path| return try listClusterBackups(alloc, path, location_uri, options),
        .remote => {},
    }
    var infos = std.ArrayListUnmanaged(BackupInfo).empty;
    errdefer {
        for (infos.items) |info| freeBackupInfo(alloc, info);
        infos.deinit(alloc);
    }
    try infos.ensureTotalCapacity(alloc, options.limit);

    var continuation_token: ?[]u8 = null;
    defer if (continuation_token) |token| alloc.free(token);
    const start_after = if (options.cursor) |cursor| blk: {
        const suffix = try clusterMetadataPath(alloc, "", cursor);
        defer alloc.free(suffix);
        break :blk try location.remote.keyAlloc(alloc, trimLeftSlash(suffix));
    } else null;
    defer if (start_after) |value| alloc.free(value);
    var has_more = false;
    while (true) {
        var listed = location.remote.listTopLevelObjectsPage(
            alloc,
            if (continuation_token == null) start_after else null,
            continuation_token,
        ) catch |err| {
            logBackupListFailure(.enumerate, backupListErrorClass(err));
            return err;
        };
        defer listed.deinit(alloc);
        var next_token: ?[]u8 = null;
        defer if (next_token) |token| alloc.free(token);

        for (listed.entries) |entry| {
            if (!std.mem.endsWith(u8, entry.key, "-cluster-metadata.json")) continue;
            const backup_id = backupIdFromListedRemoteClusterMetadataKey(&location.remote, entry.key) orelse {
                logBackupListSkipped(.key_validation, "invalid_manifest");
                continue;
            };
            validateBackupId(backup_id) catch {
                logBackupListSkipped(.key_validation, "invalid_manifest");
                continue;
            };
            var manifest = readClusterManifestFromRemoteKey(
                alloc,
                &location.remote,
                entry.key,
                entry.size,
                backup_id,
            ) catch |err| {
                if (!isSkippableBackupListManifestError(err)) {
                    logBackupListFailure(.manifest_read, backupListManifestErrorClass(err));
                    return err;
                }
                logBackupListSkipped(.manifest_read, backupListManifestErrorClass(err));
                continue;
            };
            defer manifest.deinit(alloc);
            if (manifest.format_version != cluster_format_version) continue;
            if (infos.items.len == options.limit) {
                has_more = true;
                break;
            }
            infos.appendAssumeCapacity(try backupInfoFromManifest(alloc, &manifest, location_uri));
        }

        if (has_more) break;

        if (listed.next_continuation_token) |token| {
            next_token = try alloc.dupe(u8, token);
        }

        if (continuation_token) |token| alloc.free(token);
        continuation_token = next_token;
        next_token = null;
        if (continuation_token == null) break;
    }
    const next_cursor = if (has_more and infos.items.len > 0) try alloc.dupe(u8, infos.items[infos.items.len - 1].backup_id) else null;
    errdefer if (next_cursor) |cursor| alloc.free(cursor);
    return .{ .backups = try infos.toOwnedSlice(alloc), .next_cursor = next_cursor };
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
    try tables_api.validateStoredIndexesJson(alloc, manifest.indexes_json);
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
    connection: []const u8,
    artifact_backup_id: []const u8,
    manifest: *const TableBackupManifest,
) ![]metadata_table_manager.RangeRecord {
    if (manifest.shards.len == 0) return error.UnsupportedBackupFormat;
    if (connection.len == 0 or connection.len > 256) return error.InvalidBackupRequest;
    try validateBackupId(artifact_backup_id);
    const ranges = try alloc.alloc(metadata_table_manager.RangeRecord, manifest.shards.len);
    var initialized: usize = 0;
    errdefer {
        for (ranges[0..initialized]) |record| metadata_table_manager.freeRange(alloc, record);
        alloc.free(ranges);
    }
    for (manifest.shards, 0..) |shard, i| {
        if (!group_ids.isDataGroupId(shard.group_id)) return error.UnsupportedBackupFormat;
        ranges[i] = try deriveRestoreRange(
            alloc,
            table_id,
            location_uri,
            connection,
            manifest.backup_id,
            artifact_backup_id,
            shard,
        );
        initialized += 1;
    }
    return ranges;
}

fn deriveRestoreRange(
    alloc: std.mem.Allocator,
    table_id: u64,
    location_uri: []const u8,
    connection: []const u8,
    backup_id: []const u8,
    artifact_backup_id: []const u8,
    shard: ShardSnapshot,
) !metadata_table_manager.RangeRecord {
    const start_key = try alloc.dupe(u8, shard.start_key);
    errdefer alloc.free(start_key);
    const end_key = if (shard.end_key) |end| try alloc.dupe(u8, end) else null;
    errdefer if (end_key) |end| alloc.free(end);
    const owned_backup_id = try alloc.dupe(u8, backup_id);
    errdefer alloc.free(owned_backup_id);
    const owned_artifact_backup_id = try alloc.dupe(u8, artifact_backup_id);
    errdefer alloc.free(owned_artifact_backup_id);
    const restore_location = try alloc.dupe(u8, location_uri);
    errdefer alloc.free(restore_location);
    const restore_snapshot_path = try alloc.dupe(u8, shard.snapshot_path);
    errdefer alloc.free(restore_snapshot_path);
    const restore_connection = try alloc.dupe(u8, connection);
    errdefer alloc.free(restore_connection);
    const restore_artifact_sha256 = try alloc.dupe(u8, shard.artifact_sha256);
    errdefer alloc.free(restore_artifact_sha256);
    return .{
        .group_id = shard.group_id,
        .table_id = table_id,
        .start_key = start_key,
        .end_key = end_key,
        .restore_backup_id = owned_backup_id,
        .restore_artifact_backup_id = owned_artifact_backup_id,
        .restore_location = restore_location,
        .restore_snapshot_path = restore_snapshot_path,
        .restore_connection = restore_connection,
        .restore_artifact_size_bytes = shard.artifact_size_bytes,
        .restore_artifact_sha256 = restore_artifact_sha256,
    };
}

pub fn findShardSnapshot(manifest: *const TableBackupManifest, group_id: u64) ?*const ShardSnapshot {
    for (manifest.shards) |*shard| {
        if (shard.group_id == group_id) return shard;
    }
    return null;
}

pub fn findShardSnapshotByPath(
    manifest: *const TableBackupManifest,
    snapshot_path: []const u8,
) ?*const ShardSnapshot {
    for (manifest.shards) |*shard| {
        if (std.mem.eql(u8, shard.snapshot_path, snapshot_path)) return shard;
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
    return try copyDirectoryFromLocationUsingIo(alloc, null, location, snapshot_path, dest_path);
}

pub fn copyDirectoryFromLocationUsingIo(
    alloc: std.mem.Allocator,
    shared_io: ?std.Io,
    location: *BackupLocation,
    snapshot_path: []const u8,
    dest_path: []const u8,
) !void {
    try validateArtifactRelativePath(snapshot_path);
    switch (location.*) {
        .file => |backup_root| {
            const src_root = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ backup_root, snapshot_path });
            defer alloc.free(src_root);
            if (shared_io) |io|
                try copyDirectoryRecursiveWithIo(alloc, io, src_root, dest_path, .transient)
            else {
                var io_impl = std.Io.Threaded.init(alloc, .{});
                defer io_impl.deinit();
                try copyDirectoryRecursiveWithIo(alloc, io_impl.io(), src_root, dest_path, .transient);
            }
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
    return try copyFileFromLocationUsingIo(alloc, null, location, snapshot_path, dest_path);
}

pub fn copyFileFromLocationUsingIo(
    alloc: std.mem.Allocator,
    shared_io: ?std.Io,
    location: *BackupLocation,
    snapshot_path: []const u8,
    dest_path: []const u8,
) !void {
    try validateArtifactRelativePath(snapshot_path);
    switch (location.*) {
        .file => |backup_root| {
            const src_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ backup_root, snapshot_path });
            defer alloc.free(src_path);
            if (shared_io) |io|
                try copyFileAbsoluteWithIoOptions(io, src_path, dest_path, .transient)
            else
                try copyFileAbsoluteWithDurability(src_path, dest_path, .transient);
        },
        .remote => |*store| {
            try store.readFile(alloc, trimLeftSlash(snapshot_path), dest_path);
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
    try validateArtifactRelativePath(snapshot_path);
    switch (location.*) {
        .file => |backup_root| {
            const dest_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ backup_root, snapshot_path });
            defer alloc.free(dest_path);
            try copyFileAbsolute(src_path, dest_path);
        },
        .remote => |*store| {
            try store.writeFile(alloc, trimLeftSlash(snapshot_path), src_path, content_type);
        },
    }
}

pub fn populateShardArtifactIntegrity(
    alloc: std.mem.Allocator,
    shared_io: ?std.Io,
    format: BackupFormat,
    artifact_path: []const u8,
    shard: *ShardSnapshot,
) !void {
    var integrity = try artifactIntegrityAlloc(alloc, shared_io, format, artifact_path);
    if (shard.artifact_sha256.len > 0) alloc.free(@constCast(shard.artifact_sha256));
    shard.artifact_size_bytes = integrity.size_bytes;
    shard.artifact_sha256 = integrity.sha256;
    integrity = undefined;
}

pub fn deriveManifestArtifactIntegrity(
    alloc: std.mem.Allocator,
    shared_io: ?std.Io,
    backup_root: []const u8,
    manifest: *TableBackupManifest,
) !void {
    if (manifest.artifact_integrity_mode == .declared) return;
    if (manifest.format != .portable) return error.UnsupportedBackupFormat;
    for (@constCast(manifest.shards)) |*shard| {
        const artifact_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{
            backup_root,
            shard.snapshot_path,
        });
        defer alloc.free(artifact_path);
        try populateShardArtifactIntegrity(alloc, shared_io, .portable, artifact_path, shard);
    }
    manifest.artifact_integrity_mode = .declared;
    try validateTableManifest(alloc, manifest, manifest.backup_id);
}

pub fn verifyShardArtifactIntegrity(
    alloc: std.mem.Allocator,
    shared_io: ?std.Io,
    format: BackupFormat,
    artifact_path: []const u8,
    shard: *const ShardSnapshot,
) !void {
    if (!isLowerSha256Hex(shard.artifact_sha256)) return error.BackupIntegrityMissing;
    var actual = try artifactIntegrityAlloc(alloc, shared_io, format, artifact_path);
    defer actual.deinit(alloc);
    if (actual.size_bytes != shard.artifact_size_bytes or
        !std.mem.eql(u8, actual.sha256, shard.artifact_sha256))
    {
        return error.BackupArtifactIntegrityMismatch;
    }
}

pub fn artifactIntegrityAlloc(
    alloc: std.mem.Allocator,
    shared_io: ?std.Io,
    format: BackupFormat,
    artifact_path: []const u8,
) !ArtifactIntegrity {
    if (shared_io) |io| return try artifactIntegrityAllocWithIo(alloc, io, format, artifact_path);
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    return try artifactIntegrityAllocWithIo(alloc, io_impl.io(), format, artifact_path);
}

pub fn portableBytesIntegrityAlloc(alloc: std.mem.Allocator, bytes: []const u8) !ArtifactIntegrity {
    var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
    const hex = std.fmt.bytesToHex(digest, .lower);
    return .{
        .size_bytes = @intCast(bytes.len),
        .sha256 = try alloc.dupe(u8, &hex),
    };
}

fn artifactIntegrityAllocWithIo(
    alloc: std.mem.Allocator,
    io: std.Io,
    format: BackupFormat,
    artifact_path: []const u8,
) !ArtifactIntegrity {
    return switch (format) {
        .portable => try fileArtifactIntegrityAlloc(alloc, io, artifact_path),
        .native => try directoryArtifactIntegrityAlloc(alloc, io, artifact_path),
    };
}

fn fileArtifactIntegrityAlloc(
    alloc: std.mem.Allocator,
    io: std.Io,
    path: []const u8,
) !ArtifactIntegrity {
    var file = if (std.fs.path.isAbsolute(path))
        try std.Io.Dir.openFileAbsolute(io, path, .{})
    else
        try std.Io.Dir.cwd().openFile(io, path, .{});
    defer file.close(io);
    const initial_stat = try file.stat(io);

    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    try hashFileContents(io, file, initial_stat, &hasher);
    var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    hasher.final(&digest);
    const hex = std.fmt.bytesToHex(digest, .lower);
    return .{
        .size_bytes = initial_stat.size,
        .sha256 = try alloc.dupe(u8, &hex),
    };
}

const NativeArtifactFile = struct {
    path: []u8,
    size: u64,

    fn deinit(self: NativeArtifactFile, alloc: std.mem.Allocator) void {
        alloc.free(self.path);
    }
};

fn directoryArtifactIntegrityAlloc(
    alloc: std.mem.Allocator,
    io: std.Io,
    path: []const u8,
) !ArtifactIntegrity {
    var dir = if (std.fs.path.isAbsolute(path))
        try std.Io.Dir.openDirAbsolute(io, path, .{ .iterate = true })
    else
        try std.Io.Dir.cwd().openDir(io, path, .{ .iterate = true });
    defer dir.close(io);

    var files = std.ArrayListUnmanaged(NativeArtifactFile).empty;
    defer {
        for (files.items) |entry| entry.deinit(alloc);
        files.deinit(alloc);
    }
    var walker = try dir.walk(alloc);
    defer walker.deinit();
    while (try walker.next(io)) |entry| {
        switch (entry.kind) {
            .directory => {},
            .file => {
                const stat = try dir.statFile(io, entry.path, .{});
                const normalized = try alloc.dupe(u8, entry.path);
                errdefer alloc.free(normalized);
                if (std.fs.path.sep != '/') {
                    for (normalized) |*c| if (c.* == std.fs.path.sep) {
                        c.* = '/';
                    };
                }
                try files.append(alloc, .{ .path = normalized, .size = stat.size });
            },
            else => return error.UnsupportedBackupArtifact,
        }
    }
    std.mem.sort(NativeArtifactFile, files.items, {}, nativeArtifactFileLessThan);

    var total_size: u64 = 0;
    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    hasher.update("antfly-native-backup-tree-v1");
    hashArtifactU64(&hasher, @intCast(files.items.len));
    for (files.items) |entry| {
        total_size = std.math.add(u64, total_size, entry.size) catch return error.BackupArtifactTooLarge;
        hashArtifactBytes(&hasher, entry.path);
        hashArtifactU64(&hasher, entry.size);

        var file = try dir.openFile(io, entry.path, .{});
        defer file.close(io);
        const initial_stat = try file.stat(io);
        if (initial_stat.size != entry.size) return error.SourceFileChanged;
        try hashFileContents(io, file, initial_stat, &hasher);
    }
    hashArtifactU64(&hasher, total_size);

    var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    hasher.final(&digest);
    const hex = std.fmt.bytesToHex(digest, .lower);
    return .{
        .size_bytes = total_size,
        .sha256 = try alloc.dupe(u8, &hex),
    };
}

fn nativeArtifactFileLessThan(_: void, lhs: NativeArtifactFile, rhs: NativeArtifactFile) bool {
    return std.mem.order(u8, lhs.path, rhs.path) == .lt;
}

fn hashFileContents(
    io: std.Io,
    file: std.Io.File,
    initial_stat: std.Io.File.Stat,
    hasher: *std.crypto.hash.sha2.Sha256,
) !void {
    var buf: [256 * 1024]u8 = undefined;
    var offset: u64 = 0;
    while (offset < initial_stat.size) {
        const wanted: usize = @intCast(@min(initial_stat.size - offset, buf.len));
        const n = try file.readPositionalAll(io, buf[0..wanted], offset);
        if (n != wanted) return error.SourceFileChanged;
        hasher.update(buf[0..n]);
        offset += n;
    }
    var extra: [1]u8 = undefined;
    if (try file.readPositionalAll(io, &extra, offset) != 0) return error.SourceFileChanged;
    const final_stat = try file.stat(io);
    if (final_stat.size != initial_stat.size or !std.meta.eql(final_stat.mtime, initial_stat.mtime))
        return error.SourceFileChanged;
}

fn hashArtifactU64(hasher: *std.crypto.hash.sha2.Sha256, value: u64) void {
    var encoded: [@sizeOf(u64)]u8 = undefined;
    std.mem.writeInt(u64, &encoded, value, .little);
    hasher.update(&encoded);
}

fn hashArtifactBytes(hasher: *std.crypto.hash.sha2.Sha256, value: []const u8) void {
    hashArtifactU64(hasher, @intCast(value.len));
    hasher.update(value);
}

pub fn writeFileToLocation(
    alloc: std.mem.Allocator,
    location: *BackupLocation,
    snapshot_path: []const u8,
    body: []const u8,
    content_type: []const u8,
) !void {
    try validateArtifactRelativePath(snapshot_path);
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
            .artifact_size_bytes = shard.artifact_size_bytes,
            .artifact_sha256 = if (shard.artifact_sha256.len > 0)
                try alloc.dupe(u8, shard.artifact_sha256)
            else
                "",
        };
        initialized_shards += 1;
    }

    return .{
        .format_version = manifest.format_version,
        .format = manifest.format,
        .artifact_integrity_mode = manifest.artifact_integrity_mode,
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
        const name = try alloc.dupe(u8, table.name);
        errdefer alloc.free(name);
        const owned_table_backup_id = try alloc.dupe(u8, table.table_backup_id);
        errdefer alloc.free(owned_table_backup_id);
        const artifact_backup_id = if (table.artifact_backup_id) |value|
            try alloc.dupe(u8, value)
        else
            null;
        tables[i] = .{
            .name = name,
            .table_backup_id = owned_table_backup_id,
            .artifact_backup_id = artifact_backup_id,
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
        .state = manifest.state,
        .backup_id = try alloc.dupe(u8, manifest.backup_id),
        .timestamp = try alloc.dupe(u8, manifest.timestamp),
        .location = try alloc.dupe(u8, manifest.location),
        .antfly_version = try alloc.dupe(u8, manifest.antfly_version),
        .expected_table_count = manifest.expected_table_count,
        .completed_table_count = manifest.completed_table_count,
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
    return try copyDirectoryRecursiveUsingIo(alloc, null, src_path, dest_path);
}

pub fn copyDirectoryRecursiveUsingIo(
    alloc: std.mem.Allocator,
    shared_io: ?std.Io,
    src_path: []const u8,
    dest_path: []const u8,
) !void {
    if (shared_io) |io| return try copyDirectoryRecursiveWithIo(alloc, io, src_path, dest_path, .durable);
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    return copyDirectoryRecursiveWithIo(alloc, io_impl.io(), src_path, dest_path, .durable);
}

const CopyDurability = enum {
    transient,
    durable,
};

fn copyDirectoryRecursiveWithIo(
    alloc: std.mem.Allocator,
    io: std.Io,
    src_path: []const u8,
    dest_path: []const u8,
    durability: CopyDurability,
) !void {
    try ensureDirPathWithIo(io, dest_path);

    var durable_dirs = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (durable_dirs.items) |path| alloc.free(path);
        durable_dirs.deinit(alloc);
    }
    if (durability == .durable) {
        const owned_dest_path = try alloc.dupe(u8, dest_path);
        durable_dirs.append(alloc, owned_dest_path) catch |err| {
            alloc.free(owned_dest_path);
            return err;
        };
    }

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
            .directory => {
                try ensureDirPathWithIo(io, dest_entry_path);
                if (durability == .durable) {
                    const owned_dir_path = try alloc.dupe(u8, dest_entry_path);
                    durable_dirs.append(alloc, owned_dir_path) catch |err| {
                        alloc.free(owned_dir_path);
                        return err;
                    };
                }
            },
            .file => try copyFileAbsoluteWithIoOptions(io, src_entry_path, dest_entry_path, durability),
            else => return error.UnsupportedBackupArtifact,
        }
    }

    if (durability == .transient) return;
    std.mem.sort([]u8, durable_dirs.items, {}, struct {
        fn lessThan(_: void, lhs: []u8, rhs: []u8) bool {
            if (lhs.len != rhs.len) return lhs.len > rhs.len;
            return std.mem.order(u8, lhs, rhs) == .gt;
        }
    }.lessThan);
    for (durable_dirs.items) |dir_path| try fs_paths.syncDirPortable(io, dir_path);
    try syncPathAncestorsWithIo(io, std.fs.path.dirname(dest_path) orelse ".");
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
    try file.sync(io);
    try syncPathAncestorsWithIo(io, std.fs.path.dirname(path) orelse ".");
}

fn writeFileAbsoluteIfAbsent(alloc: std.mem.Allocator, path: []const u8, data: []const u8) !void {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    return writeFileAbsoluteIfAbsentWithIo(alloc, io_impl.io(), path, data);
}

fn writeFileAbsoluteIfAbsentWithIo(
    alloc: std.mem.Allocator,
    io: std.Io,
    path: []const u8,
    data: []const u8,
) !void {
    if (std.fs.path.dirname(path)) |dir_name| try ensureDirPathWithIo(io, dir_name);

    const lock_path = try std.fmt.allocPrint(alloc, "{s}.publish.lock", .{path});
    defer alloc.free(lock_path);
    var lock_file = if (std.fs.path.isAbsolute(lock_path))
        try std.Io.Dir.createFileAbsolute(io, lock_path, .{ .truncate = false })
    else
        try std.Io.Dir.cwd().createFile(io, lock_path, .{ .truncate = false });
    defer lock_file.close(io);
    // Manifest publication is the backup commit point. Locking support is
    // required so two Antfly processes sharing a filesystem cannot both pass
    // the existence check and overwrite one another.
    try lock_file.lock(io, .exclusive);
    defer lock_file.unlock(io);
    const exists = blk: {
        _ = std.Io.Dir.cwd().statFile(io, path, .{}) catch |err| switch (err) {
            error.FileNotFound => break :blk false,
            else => return err,
        };
        break :blk true;
    };
    if (exists) return error.BackupAlreadyExists;

    var entropy: [8]u8 = undefined;
    try io.randomSecure(&entropy);
    const nonce = std.fmt.bytesToHex(entropy, .lower);
    const tmp_path = try std.fmt.allocPrint(alloc, "{s}.tmp-{s}", .{ path, &nonce });
    defer alloc.free(tmp_path);
    errdefer if (std.fs.path.isAbsolute(tmp_path))
        std.Io.Dir.deleteFileAbsolute(io, tmp_path) catch {}
    else
        std.Io.Dir.cwd().deleteFile(io, tmp_path) catch {};

    var file = if (std.fs.path.isAbsolute(tmp_path))
        try std.Io.Dir.createFileAbsolute(io, tmp_path, .{ .truncate = true })
    else
        try std.Io.Dir.cwd().createFile(io, tmp_path, .{ .truncate = true });
    var file_open = true;
    defer if (file_open) file.close(io);
    var buf: [4096]u8 = undefined;
    var writer = file.writer(io, &buf);
    try writer.interface.writeAll(data);
    try writer.end();
    try file.sync(io);
    file.close(io);
    file_open = false;
    if (std.fs.path.isAbsolute(path))
        try std.Io.Dir.renameAbsolute(tmp_path, path, io)
    else
        try std.Io.Dir.rename(std.Io.Dir.cwd(), tmp_path, std.Io.Dir.cwd(), path, io);
    try syncPathAncestorsWithIo(io, std.fs.path.dirname(path) orelse ".");
}

/// Replace a small mutable control record with an fsync + atomic rename. The
/// sibling lock serializes writers that share a filesystem.
fn writeFileAbsoluteAtomicallyWithIo(
    alloc: std.mem.Allocator,
    io: std.Io,
    path: []const u8,
    data: []const u8,
) !void {
    if (std.fs.path.dirname(path)) |dir_name| try ensureDirPathWithIo(io, dir_name);
    const lock_path = try std.fmt.allocPrint(alloc, "{s}.publish.lock", .{path});
    defer alloc.free(lock_path);
    var lock_file = if (std.fs.path.isAbsolute(lock_path))
        try std.Io.Dir.createFileAbsolute(io, lock_path, .{ .truncate = false })
    else
        try std.Io.Dir.cwd().createFile(io, lock_path, .{ .truncate = false });
    defer lock_file.close(io);
    try lock_file.lock(io, .exclusive);
    defer lock_file.unlock(io);

    var entropy: [8]u8 = undefined;
    try io.randomSecure(&entropy);
    const nonce = std.fmt.bytesToHex(entropy, .lower);
    const tmp_path = try std.fmt.allocPrint(alloc, "{s}.tmp-{s}", .{ path, &nonce });
    defer alloc.free(tmp_path);
    errdefer if (std.fs.path.isAbsolute(tmp_path))
        std.Io.Dir.deleteFileAbsolute(io, tmp_path) catch {}
    else
        std.Io.Dir.cwd().deleteFile(io, tmp_path) catch {};

    var file = if (std.fs.path.isAbsolute(tmp_path))
        try std.Io.Dir.createFileAbsolute(io, tmp_path, .{ .truncate = true })
    else
        try std.Io.Dir.cwd().createFile(io, tmp_path, .{ .truncate = true });
    var file_open = true;
    defer if (file_open) file.close(io);
    var buf: [4096]u8 = undefined;
    var writer = file.writer(io, &buf);
    try writer.interface.writeAll(data);
    try writer.end();
    try file.sync(io);
    file.close(io);
    file_open = false;
    if (std.fs.path.isAbsolute(path))
        try std.Io.Dir.renameAbsolute(tmp_path, path, io)
    else
        try std.Io.Dir.rename(std.Io.Dir.cwd(), tmp_path, std.Io.Dir.cwd(), path, io);
    try syncPathAncestorsWithIo(io, std.fs.path.dirname(path) orelse ".");
}

fn readFileAbsoluteAlloc(alloc: std.mem.Allocator, path: []const u8, max_bytes: usize) ![]u8 {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    return readFileAbsoluteAllocWithIo(alloc, io_impl.io(), path, max_bytes);
}

fn readFileAbsoluteAllocWithIo(
    alloc: std.mem.Allocator,
    io: std.Io,
    path: []const u8,
    max_bytes: usize,
) ![]u8 {
    var file = if (std.fs.path.isAbsolute(path))
        try std.Io.Dir.openFileAbsolute(io, path, .{})
    else
        try std.Io.Dir.cwd().openFile(io, path, .{});
    defer file.close(io);
    const stat = try file.stat(io);
    var reader: std.Io.File.Reader = .initSize(file, io, &.{}, stat.size);
    return try reader.interface.allocRemaining(alloc, .limited(max_bytes));
}

fn pathExists(path: []const u8) !bool {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    return pathExistsWithIo(io_impl.io(), path);
}

fn pathExistsWithIo(io: std.Io, path: []const u8) !bool {
    _ = std.Io.Dir.cwd().statFile(io, path, .{}) catch |err| switch (err) {
        error.FileNotFound => return false,
        else => return err,
    };
    return true;
}

fn copyFileAbsolute(src_path: []const u8, dest_path: []const u8) !void {
    return try copyFileAbsoluteWithDurability(src_path, dest_path, .durable);
}

fn copyFileAbsoluteWithDurability(
    src_path: []const u8,
    dest_path: []const u8,
    durability: CopyDurability,
) !void {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    try copyFileAbsoluteWithIoOptions(io, src_path, dest_path, durability);
    if (durability == .durable)
        try syncPathAncestorsWithIo(io, std.fs.path.dirname(dest_path) orelse ".");
}

fn copyFileAbsoluteWithIoOptions(
    io: std.Io,
    src_path: []const u8,
    dest_path: []const u8,
    durability: CopyDurability,
) !void {
    if (std.fs.path.dirname(dest_path)) |dir_name| try ensureDirPathWithIo(io, dir_name);

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
    try writer.end();
    const final_src_stat = try src.stat(io);
    if (final_src_stat.size != src_stat.size or !std.meta.eql(final_src_stat.mtime, src_stat.mtime))
        return error.SourceFileChanged;
    if (durability == .durable) try dest.sync(io);
}

fn ensureDirPath(path: []const u8) !void {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    return ensureDirPathWithIo(io_impl.io(), path);
}

fn ensureDirPathWithIo(io: std.Io, path: []const u8) !void {
    try fs_paths.createDirPathPortable(io, path);
}

fn syncPathAncestorsWithIo(io: std.Io, start_path: []const u8) !void {
    var current = start_path;
    while (true) {
        try fs_paths.syncDirPortable(io, if (current.len == 0) "." else current);
        const parent = std.fs.path.dirname(current) orelse break;
        if (std.mem.eql(u8, parent, current)) break;
        current = parent;
    }
}

fn stringifyJsonAlloc(alloc: std.mem.Allocator, value: anytype) ![]u8 {
    return try std.json.Stringify.valueAlloc(alloc, value, .{ .emit_null_optional_fields = false });
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

fn validateSelectedTableNames(values: ?[]const []const u8) !void {
    const names = values orelse return;
    if (names.len > 256) return error.TooManyBackupTables;
    for (names, 0..) |name, i| {
        if (name.len == 0 or name.len > 4096) return error.InvalidBackupTableName;
        for (names[0..i]) |previous| {
            if (std.mem.eql(u8, previous, name)) return error.DuplicateBackupTableName;
        }
    }
}

pub fn freeClusterBackupRequest(alloc: std.mem.Allocator, req: *ClusterBackupRequest) void {
    alloc.free(req.backup_id);
    alloc.free(req.location);
    if (req.connection) |value| alloc.free(value);
    if (req.table_names) |values| freeStringSlice(alloc, values);
    req.* = undefined;
}

pub fn freeClusterRestoreRequest(alloc: std.mem.Allocator, req: *ClusterRestoreRequest) void {
    alloc.free(req.backup_id);
    alloc.free(req.location);
    if (req.connection) |value| alloc.free(value);
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
    var committed: usize = 0;
    var durability_pending: usize = 0;
    var skipped: usize = 0;
    var failed: usize = 0;
    for (statuses) |status| {
        if (std.mem.eql(u8, status.status, "triggered")) {
            triggered += 1;
        } else if (std.mem.eql(u8, status.status, "committed")) {
            committed += 1;
        } else if (std.mem.eql(u8, status.status, "durability_pending")) {
            durability_pending += 1;
        } else if (std.mem.eql(u8, status.status, "skipped")) {
            skipped += 1;
        } else if (std.mem.eql(u8, status.status, "failed")) {
            failed += 1;
        }
    }
    const successful = triggered + committed + durability_pending + skipped;
    if (successful == 0 and failed > 0) return "failed";
    if (failed > 0) return "partial";
    if (durability_pending > 0) return "durability_pending";
    if (triggered > 0) return "triggered";
    return "completed";
}

test "cluster restore overall status distinguishes accepted committed and partial outcomes" {
    try std.testing.expectEqualStrings("completed", clusterRestoreOverallStatus(&.{
        .{ .name = "a", .status = "committed" },
        .{ .name = "b", .status = "skipped" },
    }));
    try std.testing.expectEqualStrings("triggered", clusterRestoreOverallStatus(&.{
        .{ .name = "a", .status = "committed" },
        .{ .name = "b", .status = "triggered" },
    }));
    try std.testing.expectEqualStrings("durability_pending", clusterRestoreOverallStatus(&.{
        .{ .name = "a", .status = "committed" },
        .{ .name = "b", .status = "durability_pending" },
    }));
    try std.testing.expectEqualStrings("partial", clusterRestoreOverallStatus(&.{
        .{ .name = "a", .status = "committed" },
        .{ .name = "b", .status = "failed" },
    }));
    try std.testing.expectEqualStrings("failed", clusterRestoreOverallStatus(&.{
        .{ .name = "a", .status = "failed" },
    }));
}

fn currentTimestampRfc3339(alloc: std.mem.Allocator) ![]u8 {
    const epoch_seconds = std.time.epoch.EpochSeconds{
        .secs = @divFloor(platform_time.realtimeNs(), std.time.ns_per_s),
    };
    const year_day = epoch_seconds.getEpochDay().calculateYearDay();
    const month_day = year_day.calculateMonthDay();
    const day_seconds = epoch_seconds.getDaySeconds();
    return try std.fmt.allocPrint(alloc, "{d:0>4}-{d:0>2}-{d:0>2}T{d:0>2}:{d:0>2}:{d:0>2}Z", .{
        year_day.year,
        month_day.month.numeric(),
        month_day.day_index + 1,
        day_seconds.getHoursIntoDay(),
        day_seconds.getMinutesIntoHour(),
        day_seconds.getSecondsIntoMinute(),
    });
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
            .artifact_size_bytes = 0,
            .artifact_sha256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        },
    };
    var manifest = try createManifest(
        std.testing.allocator,
        "snap",
        .native,
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
    try std.testing.expectError(
        error.BackupAlreadyExists,
        writeManifest(std.testing.allocator, root, &manifest),
    );

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
    try std.testing.expectEqual(.complete, loaded.state);
    try std.testing.expectEqual(@as(usize, 1), loaded.expected_table_count);
    try std.testing.expectEqual(@as(usize, 1), loaded.completed_table_count);
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

test "cluster backup manifest rejects incomplete coverage" {
    const tables = [_]ClusterTableBackupEntry{.{
        .name = "docs",
        .table_backup_id = "docs-snap",
    }};
    var manifest = try createClusterManifest(
        std.testing.allocator,
        "snap",
        "file:///tmp/backups",
        &tables,
    );
    defer manifest.deinit(std.testing.allocator);

    try validateClusterManifest(std.testing.allocator, &manifest, "snap");

    manifest.completed_table_count = 0;
    try std.testing.expectError(
        error.IncompleteClusterBackup,
        validateClusterManifest(std.testing.allocator, &manifest, "snap"),
    );
    manifest.completed_table_count = 1;
    manifest.expected_table_count = 2;
    try std.testing.expectError(
        error.IncompleteClusterBackup,
        validateClusterManifest(std.testing.allocator, &manifest, "snap"),
    );
    manifest.expected_table_count = 1;
    manifest.format_version = 1;
    manifest.expected_table_count = 0;
    manifest.completed_table_count = 0;
    try std.testing.expectError(
        error.UnsupportedBackupFormat,
        validateClusterManifest(std.testing.allocator, &manifest, "snap"),
    );

    var empty = try createClusterManifest(
        std.testing.allocator,
        "empty",
        "file:///tmp/backups",
        &.{},
    );
    defer empty.deinit(std.testing.allocator);
    try std.testing.expectError(
        error.IncompleteClusterBackup,
        validateClusterManifest(std.testing.allocator, &empty, "empty"),
    );
}

test "backup location parsing requires absolute file uri" {
    try std.testing.expectEqualStrings("/tmp/antfly-backup", try parseFileLocation("file:///tmp/antfly-backup"));
    try std.testing.expectError(error.UnsupportedBackupLocation, parseFileLocation("s3://bucket/path"));
    try std.testing.expectError(error.InvalidBackupLocation, parseFileLocation("file://relative"));
}

test "restore source identities are bounded and canonical" {
    const alloc = std.testing.allocator;

    const canonical_file = try canonicalRestoreSourceIdentityAlloc(
        alloc,
        "file:///tmp/antfly/../backups",
    );
    defer alloc.free(canonical_file);
    try std.testing.expectEqualStrings("file:///tmp/backups", canonical_file);

    const canonical_gcs = try canonicalRestoreSourceIdentityAlloc(
        alloc,
        "gcs://archive/backups/daily/",
    );
    defer alloc.free(canonical_gcs);
    try std.testing.expectEqualStrings("gs://archive/backups/daily", canonical_gcs);

    try validateCanonicalRestoreSourceIdentity(alloc, "s3://archive/backups");
    try std.testing.expectError(
        error.InvalidBackupRequest,
        validateCanonicalRestoreSourceIdentity(alloc, "s3://archive/backups/"),
    );
    try std.testing.expectError(
        error.InvalidBackupRequest,
        canonicalRestoreSourceIdentityAlloc(alloc, ""),
    );
    try std.testing.expectError(
        error.InvalidBackupRequest,
        canonicalRestoreSourceIdentityAlloc(alloc, "file://relative"),
    );
    try std.testing.expectError(
        error.InvalidBackupRequest,
        canonicalRestoreSourceIdentityAlloc(alloc, "s3://archive/bad\x00path"),
    );
    try std.testing.expectError(
        error.InvalidBackupRequest,
        canonicalRestoreSourceIdentityAlloc(alloc, "s3://archive/backups?token=secret"),
    );
    try std.testing.expectError(
        error.InvalidBackupRequest,
        canonicalRestoreSourceIdentityAlloc(alloc, "file:///tmp/backups#fragment"),
    );

    const oversized = try alloc.alloc(u8, max_restore_source_identity_bytes + 1);
    defer alloc.free(oversized);
    @memset(oversized, 'a');
    try std.testing.expectError(
        error.InvalidBackupRequest,
        canonicalRestoreSourceIdentityAlloc(alloc, oversized),
    );

    var failing = std.testing.FailingAllocator.init(alloc, .{ .fail_index = 0 });
    try std.testing.expectError(
        error.OutOfMemory,
        canonicalRestoreSourceIdentityAlloc(failing.allocator(), "s3://archive/backups"),
    );
}

test "backup manifest round trips through remote objectstore location" {
    var memory = object_storage.MemoryObjectStorage.init(std.testing.allocator);
    defer memory.deinit();
    const client = memory.client();
    var location: BackupLocation = .{
        .remote = try RemoteBackupStore.initWithClient(std.testing.allocator, client, "bucket", "backups/prod"),
    };
    defer location.deinit(std.testing.allocator);
    // A backup.write connection does not imply HeadBucket/ListBucket. Verify
    // publication goes directly through conditional PutObject when bucket
    // provisioning is disabled.
    location.remote.create_bucket_if_missing = false;

    const shards = [_]ShardSnapshot{
        .{
            .group_id = 7,
            .start_key = "",
            .end_key = null,
            .snapshot_path = "snap/groups/7",
            .artifact_size_bytes = 0,
            .artifact_sha256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        },
    };
    var manifest = try createManifest(
        std.testing.allocator,
        "snap",
        .native,
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
    try std.testing.expectError(
        error.BackupAlreadyExists,
        writeManifestToLocation(std.testing.allocator, &location, &manifest),
    );

    var loaded = try readManifestFromLocation(std.testing.allocator, &location, "snap");
    defer loaded.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("snap", loaded.backup_id);
    try std.testing.expectEqualStrings("docs", loaded.table_name);
    try std.testing.expectEqual(@as(usize, 1), loaded.shards.len);
    try std.testing.expectEqual(@as(u64, 7), loaded.shards[0].group_id);
}

test "current Go portable metadata envelope materializes into a verified Zig manifest" {
    const alloc = std.testing.allocator;
    const body =
        \\{"version":1,"format":"portable","table":{"name":"docs","description":"documents","schema":{"version":0},"indexes":{"embedding":{"provider":"termite"}},"shards":{"1":{"byte_range":["",""]}}}}
    ;
    var manifest = try parseTableBackupManifest(alloc, body, "go-snap");
    defer manifest.deinit(alloc);
    try std.testing.expectEqual(ArtifactIntegrityMode.derive_after_materialization, manifest.artifact_integrity_mode);
    try std.testing.expectEqual(BackupFormat.portable, manifest.format);
    try std.testing.expectEqualStrings("docs", manifest.table_name);
    try std.testing.expectEqualStrings("go-snap-1.afb", manifest.shards[0].snapshot_path);
    try std.testing.expect(std.mem.indexOf(u8, manifest.indexes_json, "\"provider\":\"antfly\"") != null);
    try std.testing.expectError(
        error.BackupIntegrityMissing,
        validatePublishedTableManifest(alloc, &manifest, manifest.backup_id),
    );
    const unverified_current = try stringifyJsonAlloc(alloc, manifest);
    defer alloc.free(unverified_current);
    try std.testing.expectError(
        error.BackupIntegrityMissing,
        parseTableBackupManifest(alloc, unverified_current, manifest.backup_id),
    );

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer alloc.free(root);
    const artifact_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{
        root,
        manifest.shards[0].snapshot_path,
    });
    defer alloc.free(artifact_path);
    try writeFileAbsolute(artifact_path, "portable-afb");
    try deriveManifestArtifactIntegrity(alloc, null, root, &manifest);
    try std.testing.expectEqual(ArtifactIntegrityMode.declared, manifest.artifact_integrity_mode);
    try std.testing.expectEqual(@as(u64, "portable-afb".len), manifest.shards[0].artifact_size_bytes);
    try verifyShardArtifactIntegrity(alloc, null, .portable, artifact_path, &manifest.shards[0]);

    try std.testing.expectError(
        error.UnsupportedBackupFormat,
        parseTableBackupManifest(
            alloc,
            "{\"version\":2,\"format\":\"portable\",\"table\":{}}",
            "go-snap",
        ),
    );
}

test "current Go portable cluster envelope resolves table metadata ids" {
    const alloc = std.testing.allocator;
    const body =
        \\{"version":2,"state":"complete","backup_id":"go-cluster","timestamp":"2026-07-25T12:00:00Z","antfly_version":"go-current","format":"portable","expected_table_count":2,"completed_table_count":2,"tables":[{"name":"docs","backup_location":"s3://archive/prod/table-a-metadata.json","shard_count":1,"status":"completed"},{"name":"events","backup_location":"s3://archive/prod/table-b-metadata.json","shard_count":2,"status":"completed"}]}
    ;
    var manifest = try parseClusterManifestBytes(alloc, body, "go-cluster");
    defer manifest.deinit(alloc);
    try std.testing.expectEqual(cluster_format_version, manifest.format_version);
    try std.testing.expectEqualStrings("s3://archive/prod", manifest.location);
    try std.testing.expectEqual(@as(usize, 2), manifest.tables.len);
    try std.testing.expectEqualStrings("docs", manifest.tables[0].name);
    try std.testing.expectEqualStrings("table-a", manifest.tables[0].table_backup_id);
    try std.testing.expectEqualStrings("go-cluster", manifest.tables[0].artifact_backup_id.?);
    try std.testing.expectEqualStrings("table-b", manifest.tables[1].table_backup_id);

    var table_manifest = try parseTableBackupManifestWithArtifactBackupId(
        alloc,
        "{\"version\":1,\"format\":\"portable\",\"table\":{\"name\":\"docs\",\"shards\":{\"1\":{\"byte_range\":[\"\",\"\"]}}}}",
        manifest.tables[0].table_backup_id,
        manifest.tables[0].artifact_backup_id.?,
    );
    defer table_manifest.deinit(alloc);
    try std.testing.expectEqualStrings("table-a", table_manifest.backup_id);
    try std.testing.expectEqualStrings("go-cluster-1.afb", table_manifest.shards[0].snapshot_path);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer alloc.free(root);
    const artifact_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{
        root,
        table_manifest.shards[0].snapshot_path,
    });
    defer alloc.free(artifact_path);
    try writeFileAbsolute(artifact_path, "go-portable-artifact");
    try deriveManifestArtifactIntegrity(alloc, null, root, &table_manifest);

    const table = try deriveRestoreTableRecord(alloc, "docs", "file:///backup", &table_manifest);
    defer metadata_table_manager.freeTable(alloc, table);
    const ranges = try deriveRestoreRanges(
        alloc,
        table.table_id,
        "file:///backup",
        "production-backups",
        manifest.tables[0].artifact_backup_id.?,
        &table_manifest,
    );
    defer {
        for (ranges) |record| metadata_table_manager.freeRange(alloc, record);
        alloc.free(ranges);
    }
    try std.testing.expectEqualStrings("table-a", ranges[0].restore_backup_id);
    try std.testing.expectEqualStrings("go-cluster", ranges[0].restore_artifact_backup_id);
    try std.testing.expectEqual(@as(u64, "go-portable-artifact".len), ranges[0].restore_artifact_size_bytes);
    try std.testing.expectEqual(@as(usize, 64), ranges[0].restore_artifact_sha256.len);

    try std.testing.expectError(
        error.UnsupportedBackupFormat,
        parseClusterManifestBytes(
            alloc,
            "{\"version\":1,\"state\":\"complete\",\"backup_id\":\"go-cluster\",\"timestamp\":\"2026-07-25T12:00:00Z\",\"antfly_version\":\"go-old\",\"format\":\"portable\",\"expected_table_count\":1,\"completed_table_count\":1,\"tables\":[{\"name\":\"docs\",\"backup_location\":\"s3://archive/prod/table-a-metadata.json\",\"shard_count\":1,\"status\":\"completed\"}]}",
            "go-cluster",
        ),
    );
}

test "remote backup metadata reads are size bounded" {
    const alloc = std.testing.allocator;
    try ensureManifestSize("0123456789abcdef", 16);
    try std.testing.expectError(error.BackupManifestTooLarge, ensureManifestSize("0123456789abcdefX", 16));

    var memory = object_storage.MemoryObjectStorage.init(alloc);
    defer memory.deinit();
    var client = memory.client();
    var put = try client.putObject("bucket", "backups/manifest.json", "0123456789abcdefX", .{});
    put.deinit(alloc);

    var store = try RemoteBackupStore.initWithClient(alloc, memory.client(), "bucket", "backups");
    defer store.deinit();
    try std.testing.expectError(
        error.BackupManifestTooLarge,
        store.readBytesAllocLimited(alloc, "manifest.json", 16),
    );

    var replacement = try client.putObject("bucket", "backups/manifest.json", "0123456789abcdef", .{});
    replacement.deinit(alloc);
    const body = try store.readBytesAllocLimited(alloc, "manifest.json", 16);
    defer alloc.free(body);
    try std.testing.expectEqualStrings("0123456789abcdef", body);
}

test "remote backup key joins canonicalize only the prefix boundary" {
    const alloc = std.testing.allocator;
    var memory = object_storage.MemoryObjectStorage.init(alloc);
    defer memory.deinit();

    inline for (&.{
        .{ "backups/prod", "snap-cluster-metadata.json", "backups/prod/snap-cluster-metadata.json" },
        .{ "backups/prod/", "/snap-cluster-metadata.json", "backups/prod/snap-cluster-metadata.json" },
        .{ "/", "/snap-cluster-metadata.json", "snap-cluster-metadata.json" },
        .{ "backups//prod/", "snap-cluster-metadata.json", "backups//prod/snap-cluster-metadata.json" },
    }) |case| {
        var store = try RemoteBackupStore.initWithClient(alloc, memory.client(), "bucket", case[0]);
        defer store.deinit();
        const key = try store.keyAlloc(alloc, case[1]);
        defer alloc.free(key);
        try std.testing.expectEqualStrings(case[2], key);
    }

    var trailing = try RemoteBackupStore.initWithClient(alloc, memory.client(), "bucket", "backups/prod/");
    defer trailing.deinit();
    try std.testing.expectEqualStrings(
        "snap",
        backupIdFromListedRemoteClusterMetadataKey(
            &trailing,
            "backups/prod/snap-cluster-metadata.json",
        ).?,
    );
    try std.testing.expectEqual(
        @as(?[]const u8, null),
        backupIdFromListedRemoteClusterMetadataKey(
            &trailing,
            "backups/prod/snap/groups/7/nested-cluster-metadata.json",
        ),
    );

    try std.testing.expectEqualStrings("not_found", backupListErrorClass(error.FileNotFound));
    try std.testing.expectEqualStrings("access_denied", backupListErrorClass(error.AccessDenied));
    try std.testing.expectEqualStrings("timeout", backupListErrorClass(error.Timeout));
    try std.testing.expectEqualStrings("timeout", backupListErrorClass(error.ConnectionTimeout));
    try std.testing.expectEqualStrings("transport", backupListErrorClass(error.RemoteUnavailable));
    try std.testing.expectEqualStrings("transport", backupListErrorClass(error.ConnectionFailed));
    try std.testing.expectEqualStrings("transport", backupListErrorClass(error.ConnectionReset));
    try std.testing.expectEqualStrings("transport", backupListErrorClass(error.DnsResolutionFailed));
    try std.testing.expectEqualStrings("transport", backupListErrorClass(error.TlsHandshakeFailed));
    try std.testing.expectEqualStrings("transport", backupListErrorClass(error.RecvFailed));
    try std.testing.expectEqualStrings("invalid_manifest", backupListManifestErrorClass(error.SyntaxError));
    try std.testing.expect(isSkippableBackupListManifestError(error.SyntaxError));
    try std.testing.expect(isSkippableBackupListManifestError(error.FileNotFound));
    try std.testing.expect(!isSkippableBackupListManifestError(error.OutOfMemory));
    try std.testing.expect(!isSkippableBackupListManifestError(error.AccessDenied));
    try std.testing.expect(!isSkippableBackupListManifestError(error.InputOutput));
    try std.testing.expectEqualStrings("internal", backupListManifestErrorClass(error.OutOfMemory));
    try std.testing.expectEqualStrings("internal", backupListManifestErrorClass(error.InputOutput));

    var diagnostic_buffer: [160]u8 = undefined;
    const diagnostic = try formatBackupListDiagnostic(
        &diagnostic_buffer,
        .failed,
        .manifest_read,
        backupListManifestErrorClass(error.AccessDenied),
    );
    try std.testing.expectEqualStrings(
        "cluster backup list failed phase=manifest_read class=access_denied",
        diagnostic,
    );
    inline for (&.{
        "s3://customer-bucket/private-prefix",
        "AWS4-HMAC-SHA256 Credential=secret",
        "Authorization: Bearer secret",
        "customer-backup-id",
    }) |sensitive| {
        try std.testing.expect(std.mem.indexOf(u8, diagnostic, sensitive) == null);
    }
}

test "remote portable file transfer uses objectstore file paths" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/object-store", .{tmp.sub_path});
    defer alloc.free(root);
    const source_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/source.afb", .{tmp.sub_path});
    defer alloc.free(source_path);
    const restored_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/restored.afb", .{tmp.sub_path});
    defer alloc.free(restored_path);
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    std.Io.Dir.cwd().deleteTree(io, root) catch {};
    defer std.Io.Dir.cwd().deleteTree(io, root) catch {};
    std.Io.Dir.cwd().deleteFile(io, source_path) catch {};
    defer std.Io.Dir.cwd().deleteFile(io, source_path) catch {};
    std.Io.Dir.cwd().deleteFile(io, restored_path) catch {};
    defer std.Io.Dir.cwd().deleteFile(io, restored_path) catch {};

    var source = try std.Io.Dir.cwd().createFile(io, source_path, .{ .truncate = true });
    try source.writePositionalAll(io, "portable-file-body", 0);
    try source.sync(io);
    source.close(io);

    var filesystem = try object_storage.FilesystemObjectStorage.initWithIo(alloc, root, io);
    defer filesystem.deinit();
    var location: BackupLocation = .{
        .remote = try RemoteBackupStore.initWithClient(alloc, filesystem.client(), "bucket", "backups/prod"),
    };
    defer location.deinit(alloc);

    try copyFileToLocation(alloc, &location, "snap/data.afb", source_path, "application/vnd.antfly.backup");
    try copyFileFromLocation(alloc, &location, "snap/data.afb", restored_path);
    const restored = try std.Io.Dir.cwd().readFileAlloc(io, restored_path, alloc, .limited(1024));
    defer alloc.free(restored);
    try std.testing.expectEqualStrings("portable-file-body", restored);
}

test "remote backup directory download paginates and enforces segment prefix" {
    const alloc = std.testing.allocator;
    const dest_root = ".zig-cache/test-paginated-backup-download";
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    std.Io.Dir.cwd().deleteTree(io, dest_root) catch {};
    defer std.Io.Dir.cwd().deleteTree(io, dest_root) catch {};

    var memory = object_storage.MemoryObjectStorage.init(alloc);
    defer memory.deinit();
    var raw_client = memory.client();
    inline for (&.{
        .{ "backups/prod/snap/a/one", "one" },
        .{ "backups/prod/snap/a/nested/two", "two" },
        .{ "backups/prod/snap/a/three", "three" },
        .{ "backups/prod/snap/ab/evil", "evil" },
    }) |entry| {
        var put = try raw_client.putObject("bucket", entry[0], entry[1], .{});
        put.deinit(alloc);
    }

    var store = try RemoteBackupStore.initWithClient(alloc, memory.client(), "bucket", "backups/prod");
    defer store.deinit();
    try store.downloadDirectoryRecursiveWithPageSize(alloc, "snap/a", dest_root, 2);

    inline for (&.{
        .{ "one", "one" },
        .{ "nested/two", "two" },
        .{ "three", "three" },
    }) |expected| {
        const path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ dest_root, expected[0] });
        defer alloc.free(path);
        const body = try std.Io.Dir.cwd().readFileAlloc(io, path, alloc, .limited(16));
        defer alloc.free(body);
        try std.testing.expectEqualStrings(expected[1], body);
    }
    const escaped_path = try std.fmt.allocPrint(alloc, "{s}/b/evil", .{dest_root});
    defer alloc.free(escaped_path);
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().readFileAlloc(io, escaped_path, alloc, .limited(16)));
}

fn writePortableListValidationFixture(
    alloc: std.mem.Allocator,
    location: *BackupLocation,
    fixture_backup_id: []const u8,
    table_name: []const u8,
) !void {
    const artifact_path = try std.fmt.allocPrint(alloc, "{s}.afb", .{fixture_backup_id});
    defer alloc.free(artifact_path);
    const shards = [_]ShardSnapshot{.{
        .group_id = 1,
        .start_key = "",
        .snapshot_path = artifact_path,
    }};
    const table: metadata_table_manager.TableRecord = .{
        .table_id = 1,
        .name = table_name,
        .schema_json = "{}",
    };
    var manifest = try createManifest(alloc, fixture_backup_id, .portable, &table, &shards);
    defer manifest.deinit(alloc);
    const payload = "payload";
    var integrity = try portableBytesIntegrityAlloc(alloc, payload);
    const mutable_shard = &@constCast(manifest.shards)[0];
    mutable_shard.artifact_size_bytes = integrity.size_bytes;
    mutable_shard.artifact_sha256 = integrity.sha256;
    integrity = undefined;
    switch (location.*) {
        .file => |backup_root| {
            const absolute_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{
                backup_root,
                artifact_path,
            });
            defer alloc.free(absolute_path);
            try writeFileAbsolute(absolute_path, payload);
        },
        .remote => |*store| try store.writeBytes(
            alloc,
            artifact_path,
            payload,
            "application/vnd.antfly.backup",
        ),
    }
    try writeManifestToLocation(alloc, location, &manifest);
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
    try writePortableListValidationFixture(
        std.testing.allocator,
        &location,
        entries[0].table_backup_id,
        entries[0].name,
    );
    var manifest = try createClusterManifest(std.testing.allocator, "prod-snap", "s3://bucket/backups/prod", &entries);
    defer manifest.deinit(std.testing.allocator);
    try writeClusterManifestToLocation(std.testing.allocator, &location, &manifest);
    try std.testing.expectError(
        error.BackupAlreadyExists,
        writeClusterManifestToLocation(std.testing.allocator, &location, &manifest),
    );
    inline for (&.{ "prod-snap-2", "prod-snap-3" }) |backup_id| {
        var extra = try createClusterManifest(std.testing.allocator, backup_id, "s3://bucket/backups/prod", &entries);
        defer extra.deinit(std.testing.allocator);
        try writeClusterManifestToLocation(std.testing.allocator, &location, &extra);
    }

    var raw_client = memory.client();
    var nested = try raw_client.putObject(
        "bucket",
        "backups/prod/prod-snap/groups/7/table-file.tbl",
        "payload",
        .{ .content_type = "application/octet-stream" },
    );
    defer nested.deinit(std.testing.allocator);
    var corrupt = try raw_client.putObject(
        "bucket",
        "backups/prod/prod-snap-z-cluster-metadata.json",
        "{not-json",
        .{ .content_type = "application/json" },
    );
    defer corrupt.deinit(std.testing.allocator);

    var first = try listClusterBackupsFromOpenedLocation(std.testing.allocator, &location, "s3://bucket/backups/prod", .{ .limit = 2 });
    defer first.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 2), first.backups.len);
    try std.testing.expectEqualStrings("prod-snap-2", first.backups[0].backup_id);
    try std.testing.expectEqualStrings("prod-snap-3", first.backups[1].backup_id);
    try std.testing.expectEqual(@as(usize, 1), first.backups[0].tables.len);
    try std.testing.expectEqualStrings("docs", first.backups[0].tables[0]);
    try std.testing.expectEqualStrings("prod-snap-3", first.next_cursor.?);
    const first_json = try encodeBackupListResponse(std.testing.allocator, &first);
    defer std.testing.allocator.free(first_json);
    try std.testing.expect(std.mem.indexOf(u8, first_json, "\"next_cursor\":\"prod-snap-3\"") != null);

    var second = try listClusterBackupsFromOpenedLocation(std.testing.allocator, &location, "s3://bucket/backups/prod", .{
        .limit = 1,
        .cursor = first.next_cursor,
    });
    defer second.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), second.backups.len);
    try std.testing.expectEqualStrings("prod-snap", second.backups[0].backup_id);
    try std.testing.expectEqual(@as(?[]u8, null), second.next_cursor);
    const second_json = try encodeBackupListResponse(std.testing.allocator, &second);
    defer std.testing.allocator.free(second_json);
    try std.testing.expect(std.mem.indexOf(u8, second_json, "next_cursor") == null);
}

test "incomplete cluster backup attempts do not hide committed backups" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    var memory = object_storage.MemoryObjectStorage.init(alloc);
    defer memory.deinit();
    var location: BackupLocation = .{
        .remote = try RemoteBackupStore.initWithClient(alloc, memory.client(), "bucket", "backups"),
    };
    defer location.deinit(alloc);

    try writePortableListValidationFixture(alloc, &location, "old-table", "docs");
    const old_entries = [_]ClusterTableBackupEntry{.{
        .name = "docs",
        .table_backup_id = "old-table",
    }};
    var old_manifest = try createClusterManifest(
        alloc,
        "old-cluster",
        "s3://bucket/backups",
        &old_entries,
    );
    defer old_manifest.deinit(alloc);
    try writeClusterManifestToLocation(alloc, &location, &old_manifest);

    const attempt_tables = [_]ClusterBackupAttemptTable{.{
        .name = "docs",
        .table_backup_id = "new-table",
        .artifact_backup_id = "new-artifact",
    }};
    const now_ns: u64 = @intCast(std.Io.Timestamp.now(io, .real).toNanoseconds());
    const marker: ClusterBackupAttemptMarker = .{
        .attempt_id = "new-attempt",
        .cluster_backup_id = "new-cluster",
        .created_at_unix_ns = now_ns,
        .format = .portable,
        .tables = &attempt_tables,
    };
    try writeClusterBackupAttemptMarker(alloc, io, &location, &marker);

    var before_corrupt = try listClusterBackupsFromOpenedLocation(
        alloc,
        &location,
        "s3://bucket/backups",
        .{ .limit = 10 },
    );
    defer before_corrupt.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), before_corrupt.backups.len);
    try std.testing.expectEqualStrings("old-cluster", before_corrupt.backups[0].backup_id);
    try location.remote.writeBytes(
        alloc,
        "new-cluster-cluster-metadata.json",
        "{not-json",
        "application/json",
    );
    var after_corrupt = try listClusterBackupsFromOpenedLocation(
        alloc,
        &location,
        "s3://bucket/backups",
        .{ .limit = 10 },
    );
    defer after_corrupt.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), after_corrupt.backups.len);
    try std.testing.expectEqualStrings("old-cluster", after_corrupt.backups[0].backup_id);
}

test "cluster backup list defers artifact validation to restore admission" {
    const alloc = std.testing.allocator;
    var memory = object_storage.MemoryObjectStorage.init(alloc);
    defer memory.deinit();
    var location: BackupLocation = .{
        .remote = try RemoteBackupStore.initWithClient(alloc, memory.client(), "bucket", "backups"),
    };
    defer location.deinit(alloc);

    try writePortableListValidationFixture(alloc, &location, "missing-table", "docs");
    try location.remote.deleteSuffix(alloc, "missing-table.afb");
    const entries = [_]ClusterTableBackupEntry{.{
        .name = "docs",
        .table_backup_id = "missing-table",
    }};
    var manifest = try createClusterManifest(
        alloc,
        "missing-cluster",
        "s3://bucket/backups",
        &entries,
    );
    defer manifest.deinit(alloc);
    try writeClusterManifestToLocation(alloc, &location, &manifest);

    var listed = try listClusterBackupsFromOpenedLocation(
        alloc,
        &location,
        "s3://bucket/backups",
        .{ .limit = 10 },
    );
    defer listed.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), listed.backups.len);
    try std.testing.expectEqualStrings("missing-cluster", listed.backups[0].backup_id);
    try std.testing.expectError(
        error.FileNotFound,
        validateClusterBackupArtifactsAtLocation(
            alloc,
            location.remote.io,
            &location,
            &manifest,
        ),
    );
}

test "cluster backup list canonicalizes trailing prefix through s3 protocol" {
    const alloc = std.testing.allocator;
    const backup_ids = [_][]const u8{ "snap-a", "snap-b", "snap-c" };
    const entries = [_]ClusterTableBackupEntry{
        .{ .name = "docs", .table_backup_id = "docs-snap" },
    };
    var encoded_manifests: [backup_ids.len][]u8 = undefined;
    var encoded_count: usize = 0;
    defer for (encoded_manifests[0..encoded_count]) |encoded| alloc.free(encoded);
    for (backup_ids, 0..) |backup_id, index| {
        var manifest = try createClusterManifest(
            alloc,
            backup_id,
            "s3://bucket/backups/prod",
            &entries,
        );
        defer manifest.deinit(alloc);
        encoded_manifests[index] = try stringifyJsonAlloc(alloc, manifest);
        encoded_count += 1;
    }

    const FakeS3 = struct {
        const table_manifest =
            \\{"format_version":2,"format":"portable","artifact_integrity_mode":"declared","backup_id":"docs-snap","table_name":"docs","description":"","schema_json":"{}","read_schema_json":"","indexes_json":"{}","replication_sources_json":"[]","shards":[{"group_id":1,"start_key":"","end_key":null,"snapshot_path":"docs-snap.afb","artifact_size_bytes":1,"artifact_sha256":"0000000000000000000000000000000000000000000000000000000000000000"}]}
        ;
        manifests: *const [backup_ids.len][]u8,
        list_requests: usize = 0,
        manifest_head_requests: usize = 0,
        manifest_get_requests: usize = 0,

        fn manifestForKey(self: *@This(), key: []const u8) ?[]const u8 {
            inline for (backup_ids, 0..) |backup_id, index| {
                var expected_buf: [128]u8 = undefined;
                const expected = std.fmt.bufPrint(
                    &expected_buf,
                    "backups/prod/{s}-cluster-metadata.json",
                    .{backup_id},
                ) catch unreachable;
                if (std.mem.eql(u8, key, expected)) return self.manifests[index];
            }
            return null;
        }

        fn request(
            ctx: ?*anyopaque,
            request_alloc: std.mem.Allocator,
            method: object_storage.S3.HttpMethod,
            url: []const u8,
            headers: []const object_storage.S3.HeaderPair,
            _: ?[]const u8,
            _: ?[]const u8,
            max_response_size: ?usize,
        ) !object_storage.S3.TransportResponse {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            const parsed = try std.Uri.parse(url);
            const encoded_path = parsed.path.percent_encoded;
            const path_buf = try request_alloc.dupe(u8, encoded_path);
            defer request_alloc.free(path_buf);
            const path = std.Uri.percentDecodeInPlace(path_buf);

            if (method == .GET and parsed.query != null) {
                try std.testing.expectEqualStrings("/bucket", path);
                const encoded_query = parsed.query.?.percent_encoded;
                const query_buf = try request_alloc.dupe(u8, encoded_query);
                defer request_alloc.free(query_buf);
                const query = std.Uri.percentDecodeInPlace(query_buf);
                try std.testing.expect(std.mem.indexOf(u8, query, "delimiter=/") != null);
                try std.testing.expect(std.mem.indexOf(u8, query, "prefix=backups/prod/") != null);
                try std.testing.expect(std.mem.indexOf(u8, query, "backups/prod//") == null);
                self.list_requests += 1;

                const page = if (std.mem.indexOf(u8, query, ".antfly-incomplete/") != null)
                    "<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>"
                else if (std.mem.indexOf(u8, query, "continuation-token=page-2") != null or
                    std.mem.indexOf(u8, query, "start-after=backups/prod/snap-b-cluster-metadata.json") != null)
                    "<ListBucketResult><Contents><Key>backups/prod/snap-c-cluster-metadata.json</Key><ETag>\"c\"</ETag><Size>1</Size></Contents><IsTruncated>false</IsTruncated></ListBucketResult>"
                else
                    "<ListBucketResult><Contents><Key>backups/prod/snap-a-cluster-metadata.json</Key><ETag>\"a\"</ETag><Size>1</Size></Contents><Contents><Key>backups/prod/snap-b-cluster-metadata.json</Key><ETag>\"b\"</ETag><Size>1</Size></Contents><CommonPrefixes><Prefix>backups/prod/snap-a/</Prefix></CommonPrefixes><IsTruncated>true</IsTruncated><NextContinuationToken>page-2</NextContinuationToken></ListBucketResult>";
                return .{
                    .status = 200,
                    .body = try request_alloc.dupe(u8, page),
                };
            }

            const object_path_prefix = "/bucket/";
            try std.testing.expect(std.mem.startsWith(u8, path, object_path_prefix));
            const key = path[object_path_prefix.len..];
            try std.testing.expect(std.mem.indexOf(u8, key, "backups/prod//") == null);
            if (std.mem.eql(u8, key, "backups/prod/.antfly-backup-attempt-head.json")) {
                return .{
                    .status = 404,
                    .body = try request_alloc.alloc(u8, 0),
                };
            }
            const manifest_body = self.manifestForKey(key) orelse if (std.mem.eql(
                u8,
                key,
                "backups/prod/docs-snap-metadata.json",
            ))
                table_manifest
            else if (std.mem.eql(u8, key, "backups/prod/docs-snap.afb"))
                "x"
            else
                return error.UnexpectedS3ObjectKey;
            return switch (method) {
                .HEAD => blk: {
                    self.manifest_head_requests += 1;
                    break :blk .{
                        .status = 200,
                        .body = try request_alloc.alloc(u8, 0),
                        .etag = try request_alloc.dupe(u8, "\"manifest\""),
                        .content_length = @intCast(manifest_body.len),
                        .content_type = try request_alloc.dupe(u8, "application/json"),
                    };
                },
                .GET => blk: {
                    try std.testing.expectEqual(
                        @as(?usize, max_backup_manifest_bytes + 1),
                        max_response_size,
                    );
                    var has_bounded_range = false;
                    for (headers) |header| {
                        if (std.ascii.eqlIgnoreCase(header[0], "Range")) {
                            var expected_buf: [64]u8 = undefined;
                            const expected = try std.fmt.bufPrint(
                                &expected_buf,
                                "bytes=0-{d}",
                                .{max_backup_manifest_bytes},
                            );
                            try std.testing.expectEqualStrings(expected, header[1]);
                            has_bounded_range = true;
                        }
                    }
                    try std.testing.expect(has_bounded_range);
                    self.manifest_get_requests += 1;
                    break :blk .{
                        .status = 206,
                        .body = try request_alloc.dupe(u8, manifest_body),
                        .etag = try request_alloc.dupe(u8, "\"manifest\""),
                        .content_type = try request_alloc.dupe(u8, "application/json"),
                    };
                },
                else => error.UnexpectedS3Method,
            };
        }
    };

    var fake = FakeS3{ .manifests = &encoded_manifests };
    const s3_config = object_storage.S3.Config{
        .credentials = .{
            .endpoint = try alloc.dupe(u8, "s3.test.invalid"),
            .use_ssl = false,
            .access_key_id = try alloc.dupe(u8, "test-key"),
            .secret_access_key = try alloc.dupe(u8, "test-secret"),
            .region = try alloc.dupe(u8, "us-east-1"),
        },
        .addressing_style = .path,
    };
    var s3_impl = object_storage.S3.Client.initWithRequestFn(
        alloc,
        s3_config,
        &fake,
        FakeS3.request,
    );
    var s3_client = s3_impl.client();
    defer s3_client.deinit();

    var canonical_uri = try remote_uri.bucketPathFromS3UriAlloc(alloc, "s3://bucket/backups/prod");
    defer canonical_uri.deinit(alloc);
    var trailing_uri = try remote_uri.bucketPathFromS3UriAlloc(alloc, "s3://bucket/backups/prod/");
    defer trailing_uri.deinit(alloc);
    try std.testing.expectEqualStrings(canonical_uri.prefix, trailing_uri.prefix);

    var canonical: BackupLocation = .{
        .remote = try RemoteBackupStore.initWithClient(
            alloc,
            s3_impl.client(),
            canonical_uri.bucket,
            canonical_uri.prefix,
        ),
    };
    defer canonical.deinit(alloc);
    var trailing: BackupLocation = .{
        .remote = try RemoteBackupStore.initWithClient(
            alloc,
            s3_impl.client(),
            trailing_uri.bucket,
            trailing_uri.prefix,
        ),
    };
    defer trailing.deinit(alloc);

    var canonical_first = try listClusterBackupsFromOpenedLocation(
        alloc,
        &canonical,
        "s3://bucket/backups/prod",
        .{ .limit = 2 },
    );
    defer canonical_first.deinit(alloc);
    var trailing_first = try listClusterBackupsFromOpenedLocation(
        alloc,
        &trailing,
        "s3://bucket/backups/prod/",
        .{ .limit = 2 },
    );
    defer trailing_first.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), canonical_first.backups.len);
    try std.testing.expectEqual(canonical_first.backups.len, trailing_first.backups.len);
    for (canonical_first.backups, trailing_first.backups) |canonical_info, trailing_info| {
        try std.testing.expectEqualStrings(canonical_info.backup_id, trailing_info.backup_id);
    }
    try std.testing.expectEqualStrings(canonical_first.next_cursor.?, trailing_first.next_cursor.?);

    var canonical_second = try listClusterBackupsFromOpenedLocation(
        alloc,
        &canonical,
        "s3://bucket/backups/prod",
        .{ .limit = 2, .cursor = canonical_first.next_cursor },
    );
    defer canonical_second.deinit(alloc);
    var trailing_second = try listClusterBackupsFromOpenedLocation(
        alloc,
        &trailing,
        "s3://bucket/backups/prod/",
        .{ .limit = 2, .cursor = trailing_first.next_cursor },
    );
    defer trailing_second.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), canonical_second.backups.len);
    try std.testing.expectEqualStrings(
        canonical_second.backups[0].backup_id,
        trailing_second.backups[0].backup_id,
    );
    try std.testing.expectEqual(@as(?[]u8, null), canonical_second.next_cursor);
    try std.testing.expectEqual(@as(?[]u8, null), trailing_second.next_cursor);
    // Discovery is one paginated top-level LIST traversal per request. It
    // never scans attempt journals or probes table artifacts.
    try std.testing.expectEqual(@as(usize, 6), fake.list_requests);
    // Aggregate reads reuse LIST sizes and go straight to bounded GETs.
    try std.testing.expectEqual(@as(usize, 0), fake.manifest_head_requests);
    try std.testing.expectEqual(@as(usize, 8), fake.manifest_get_requests);
}

test "cluster backup list canonicalizes trailing remote prefix slash" {
    const alloc = std.testing.allocator;
    var memory = object_storage.MemoryObjectStorage.init(alloc);
    defer memory.deinit();
    var canonical: BackupLocation = .{
        .remote = try RemoteBackupStore.initWithClient(alloc, memory.client(), "bucket", "backups/prod"),
    };
    defer canonical.deinit(alloc);
    var trailing: BackupLocation = .{
        .remote = try RemoteBackupStore.initWithClient(alloc, memory.client(), "bucket", "backups/prod/"),
    };
    defer trailing.deinit(alloc);

    const entries = [_]ClusterTableBackupEntry{
        .{ .name = "docs", .table_backup_id = "docs-snap" },
    };
    try writePortableListValidationFixture(
        alloc,
        &canonical,
        entries[0].table_backup_id,
        entries[0].name,
    );
    inline for (&.{ "snap-a", "snap-b", "snap-c" }, 0..) |backup_id, index| {
        var manifest = try createClusterManifest(
            alloc,
            backup_id,
            if (index % 2 == 0) "s3://bucket/backups/prod" else "s3://bucket/backups/prod/",
            &entries,
        );
        defer manifest.deinit(alloc);
        const writer = if (index % 2 == 0) &canonical else &trailing;
        try writeClusterManifestToLocation(alloc, writer, &manifest);
    }

    var canonical_restore = try readClusterManifestFromLocation(alloc, &canonical, "snap-a");
    defer canonical_restore.deinit(alloc);
    var trailing_restore = try readClusterManifestFromLocation(alloc, &trailing, "snap-a");
    defer trailing_restore.deinit(alloc);
    try std.testing.expectEqualStrings(canonical_restore.backup_id, trailing_restore.backup_id);
    try verifyClusterBackupArtifactsIntegrityAtLocation(
        alloc,
        canonical.remote.io,
        &canonical,
        &canonical_restore,
    );
    try verifyClusterBackupArtifactsIntegrityAtLocation(
        alloc,
        trailing.remote.io,
        &trailing,
        &trailing_restore,
    );

    var raw_client = memory.client();
    var listed = try raw_client.listObjects("bucket", .{
        .prefix = "backups/prod/",
        .recursive = true,
    });
    defer listed.deinit(alloc);
    // Three aggregate manifests plus the table manifest and portable payload
    // used by list-time restorable-artifact admission.
    try std.testing.expectEqual(@as(usize, 5), listed.entries.len);
    for (listed.entries) |entry| {
        try std.testing.expect(std.mem.indexOf(u8, entry.key, "backups/prod//") == null);
    }

    var canonical_first = try listClusterBackupsFromOpenedLocation(
        alloc,
        &canonical,
        "s3://bucket/backups/prod",
        .{ .limit = 2 },
    );
    defer canonical_first.deinit(alloc);
    var trailing_first = try listClusterBackupsFromOpenedLocation(
        alloc,
        &trailing,
        "s3://bucket/backups/prod/",
        .{ .limit = 2 },
    );
    defer trailing_first.deinit(alloc);
    try std.testing.expectEqual(canonical_first.backups.len, trailing_first.backups.len);
    for (canonical_first.backups, trailing_first.backups) |canonical_info, trailing_info| {
        try std.testing.expectEqualStrings(canonical_info.backup_id, trailing_info.backup_id);
    }
    try std.testing.expectEqualStrings(canonical_first.next_cursor.?, trailing_first.next_cursor.?);

    var canonical_second = try listClusterBackupsFromOpenedLocation(
        alloc,
        &canonical,
        "s3://bucket/backups/prod",
        .{ .limit = 2, .cursor = canonical_first.next_cursor },
    );
    defer canonical_second.deinit(alloc);
    var trailing_second = try listClusterBackupsFromOpenedLocation(
        alloc,
        &trailing,
        "s3://bucket/backups/prod/",
        .{ .limit = 2, .cursor = trailing_first.next_cursor },
    );
    defer trailing_second.deinit(alloc);
    try std.testing.expectEqual(canonical_second.backups.len, trailing_second.backups.len);
    for (canonical_second.backups, trailing_second.backups) |canonical_info, trailing_info| {
        try std.testing.expectEqualStrings(canonical_info.backup_id, trailing_info.backup_id);
    }
    try std.testing.expectEqual(@as(?[]u8, null), canonical_second.next_cursor);
    try std.testing.expectEqual(@as(?[]u8, null), trailing_second.next_cursor);
}

test "cluster backup list canonicalizes trailing prefix through s3 protocol against env-configured s3 endpoint" {
    const Integration = struct {
        fn enabled() bool {
            const value_z = std.c.getenv("OBJECTSTORE_S3_INTEGRATION") orelse return false;
            const value = std.mem.span(value_z);
            return value.len > 0 and
                !std.mem.eql(u8, value, "0") and
                !std.mem.eql(u8, value, "false");
        }

        fn requiredOwned(alloc: std.mem.Allocator, name: [:0]const u8) ![]u8 {
            const value_z = std.c.getenv(name) orelse return error.MissingIntegrationEnvironment;
            return try alloc.dupe(u8, std.mem.span(value_z));
        }

        fn nonce() u64 {
            var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
            defer io_impl.deinit();
            return @intCast(std.Io.Timestamp.now(io_impl.io(), .awake).toNanoseconds());
        }
    };

    if (!Integration.enabled()) return error.SkipZigTest;
    const alloc = std.testing.allocator;
    const bucket = try Integration.requiredOwned(alloc, "OBJECTSTORE_S3_TEST_BUCKET");
    defer alloc.free(bucket);

    const cfg = try object_storage.S3.fromEnvAlloc(
        alloc,
        null,
        true,
        null,
        null,
        null,
        null,
        .path,
    );
    var s3_impl = try object_storage.S3.Client.init(alloc, cfg);
    var owning_client = s3_impl.client();
    defer owning_client.deinit();
    if (!(try owning_client.bucketExists(bucket))) try owning_client.makeBucket(bucket);

    const prefix = try std.fmt.allocPrint(
        alloc,
        "antfly-backup-list-integration/{d}",
        .{Integration.nonce()},
    );
    defer alloc.free(prefix);
    const trailing_prefix = try std.fmt.allocPrint(alloc, "{s}/", .{prefix});
    defer alloc.free(trailing_prefix);
    const canonical_uri = try std.fmt.allocPrint(alloc, "s3://{s}/{s}", .{ bucket, prefix });
    defer alloc.free(canonical_uri);
    const trailing_uri = try std.fmt.allocPrint(alloc, "{s}/", .{canonical_uri});
    defer alloc.free(trailing_uri);

    const backup_ids = [_][]const u8{ "snap-a", "snap-b", "snap-c" };
    const cleanup_suffixes = [_][]const u8{
        "snap-a-cluster-metadata.json",
        "snap-b-cluster-metadata.json",
        "snap-c-cluster-metadata.json",
        "docs-snap-metadata.json",
        "docs-snap.afb",
    };
    defer {
        for (cleanup_suffixes) |suffix| {
            const key = std.fmt.allocPrint(alloc, "{s}/{s}", .{ prefix, suffix }) catch continue;
            defer alloc.free(key);
            owning_client.deleteObject(bucket, key, .{}) catch {};
        }
    }

    var canonical: BackupLocation = .{
        .remote = try RemoteBackupStore.initWithClient(
            alloc,
            s3_impl.client(),
            bucket,
            prefix,
        ),
    };
    defer canonical.deinit(alloc);
    var trailing: BackupLocation = .{
        .remote = try RemoteBackupStore.initWithClient(
            alloc,
            s3_impl.client(),
            bucket,
            trailing_prefix,
        ),
    };
    defer trailing.deinit(alloc);

    const entries = [_]ClusterTableBackupEntry{
        .{ .name = "docs", .table_backup_id = "docs-snap" },
    };
    try writePortableListValidationFixture(
        alloc,
        &canonical,
        entries[0].table_backup_id,
        entries[0].name,
    );
    inline for (backup_ids, 0..) |backup_id, index| {
        var manifest = try createClusterManifest(
            alloc,
            backup_id,
            canonical_uri,
            &entries,
        );
        defer manifest.deinit(alloc);
        const writer = if (index % 2 == 0) &canonical else &trailing;
        try writeClusterManifestToLocation(alloc, writer, &manifest);
    }

    var canonical_restore = try readClusterManifestFromLocation(alloc, &canonical, backup_ids[0]);
    defer canonical_restore.deinit(alloc);
    var trailing_restore = try readClusterManifestFromLocation(alloc, &trailing, backup_ids[0]);
    defer trailing_restore.deinit(alloc);
    try std.testing.expectEqualStrings(canonical_restore.backup_id, trailing_restore.backup_id);
    try verifyClusterBackupArtifactsIntegrityAtLocation(
        alloc,
        canonical.remote.io,
        &canonical,
        &canonical_restore,
    );
    try verifyClusterBackupArtifactsIntegrityAtLocation(
        alloc,
        trailing.remote.io,
        &trailing,
        &trailing_restore,
    );

    var canonical_first = try listClusterBackupsFromOpenedLocation(
        alloc,
        &canonical,
        canonical_uri,
        .{ .limit = 2 },
    );
    defer canonical_first.deinit(alloc);
    var trailing_first = try listClusterBackupsFromOpenedLocation(
        alloc,
        &trailing,
        trailing_uri,
        .{ .limit = 2 },
    );
    defer trailing_first.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), canonical_first.backups.len);
    try std.testing.expectEqual(canonical_first.backups.len, trailing_first.backups.len);
    for (canonical_first.backups, trailing_first.backups) |canonical_info, trailing_info| {
        try std.testing.expectEqualStrings(canonical_info.backup_id, trailing_info.backup_id);
    }
    try std.testing.expectEqualStrings(canonical_first.next_cursor.?, trailing_first.next_cursor.?);

    var canonical_second = try listClusterBackupsFromOpenedLocation(
        alloc,
        &canonical,
        canonical_uri,
        .{ .limit = 2, .cursor = canonical_first.next_cursor },
    );
    defer canonical_second.deinit(alloc);
    var trailing_second = try listClusterBackupsFromOpenedLocation(
        alloc,
        &trailing,
        trailing_uri,
        .{ .limit = 2, .cursor = trailing_first.next_cursor },
    );
    defer trailing_second.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), canonical_second.backups.len);
    try std.testing.expectEqualStrings(
        canonical_second.backups[0].backup_id,
        trailing_second.backups[0].backup_id,
    );
    try std.testing.expectEqual(@as(?[]u8, null), canonical_second.next_cursor);
    try std.testing.expectEqual(@as(?[]u8, null), trailing_second.next_cursor);
}

test "remote backup reservations fence duplicate execution and can be released after cleanup" {
    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    var memory = object_storage.MemoryObjectStorage.init(std.testing.allocator);
    defer memory.deinit();
    var location: BackupLocation = .{
        .remote = try RemoteBackupStore.initWithClient(std.testing.allocator, memory.client(), "bucket", "backups"),
    };
    defer location.deinit(std.testing.allocator);

    try reserveBackupAtLocation(std.testing.allocator, io, &location, "cluster-snap", true);
    try std.testing.expectError(
        error.BackupAlreadyExists,
        reserveBackupAtLocation(std.testing.allocator, io, &location, "cluster-snap", true),
    );
    try cleanupClusterReservationAtLocation(std.testing.allocator, io, &location, "cluster-snap");
    try reserveBackupAtLocation(std.testing.allocator, io, &location, "cluster-snap", true);
}

test "cluster reservation cleanup cannot release another attempt owner" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    var memory = object_storage.MemoryObjectStorage.init(alloc);
    defer memory.deinit();
    var location: BackupLocation = .{
        .remote = try RemoteBackupStore.initWithClient(alloc, memory.client(), "bucket", "backups"),
    };
    defer location.deinit(alloc);

    try reserveClusterBackupAttemptAtLocation(
        alloc,
        io,
        &location,
        "cluster-snap",
        "attempt-old",
    );
    try std.testing.expect(!try cleanupClusterReservationIfOwnedAtLocation(
        alloc,
        io,
        &location,
        "cluster-snap",
        "attempt-new",
    ));
    try std.testing.expectError(
        error.BackupAlreadyExists,
        reserveClusterBackupAttemptAtLocation(
            alloc,
            io,
            &location,
            "cluster-snap",
            "attempt-new",
        ),
    );
    try std.testing.expect(try cleanupClusterReservationIfOwnedAtLocation(
        alloc,
        io,
        &location,
        "cluster-snap",
        "attempt-old",
    ));
    try reserveClusterBackupAttemptAtLocation(
        alloc,
        io,
        &location,
        "cluster-snap",
        "attempt-new",
    );
}

test "attempt head ordering ignores producer wall clocks and journal scans" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    var memory = object_storage.MemoryObjectStorage.init(alloc);
    defer memory.deinit();
    var location: BackupLocation = .{
        .remote = try RemoteBackupStore.initWithClient(alloc, memory.client(), "bucket", "backups"),
    };
    defer location.deinit(alloc);

    try writePortableListValidationFixture(alloc, &location, "old-table", "docs");
    const committed_tables = [_]ClusterTableBackupEntry{.{
        .name = "docs",
        .table_backup_id = "old-table",
    }};
    var committed = try createClusterManifest(
        alloc,
        "old-cluster",
        "s3://bucket/backups",
        &committed_tables,
    );
    defer committed.deinit(alloc);
    try writeClusterManifestToLocationWithIo(alloc, io, &location, &committed);

    const old_attempt_tables = [_]ClusterBackupAttemptTable{.{
        .name = "docs",
        .table_backup_id = "old-table",
        .artifact_backup_id = "old-artifact",
    }};
    const old_marker: ClusterBackupAttemptMarker = .{
        .attempt_id = "attempt-old",
        .cluster_backup_id = "old-cluster",
        .created_at_unix_ns = std.math.maxInt(u64),
        .format = .portable,
        .tables = &old_attempt_tables,
    };
    try writeClusterBackupAttemptMarker(alloc, io, &location, &old_marker);

    const new_attempt_tables = [_]ClusterBackupAttemptTable{.{
        .name = "docs",
        .table_backup_id = "new-table",
        .artifact_backup_id = "new-artifact",
    }};
    const new_marker: ClusterBackupAttemptMarker = .{
        .attempt_id = "attempt-new",
        .cluster_backup_id = "new-cluster",
        .created_at_unix_ns = 1,
        .format = .portable,
        .tables = &new_attempt_tables,
    };
    try writeClusterBackupAttemptMarker(alloc, io, &location, &new_marker);
    try location.remote.writeBytes(
        alloc,
        ".antfly-incomplete/poison.json",
        "{not-json",
        "application/json",
    );
    try writeClusterBackupAttemptHead(alloc, io, &location, new_marker.attempt_id);

    try std.testing.expectError(
        error.IncompleteClusterBackup,
        ensureNewestClusterBackupAttemptRestorable(alloc, io, &location),
    );
}

test "newest attempt health rejects same-size artifact corruption" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/integrity", .{tmp.sub_path});
    defer alloc.free(root);
    var location: BackupLocation = .{ .file = root };

    try writePortableListValidationFixture(alloc, &location, "table-snap", "docs");
    const committed_tables = [_]ClusterTableBackupEntry{.{
        .name = "docs",
        .table_backup_id = "table-snap",
    }};
    var committed = try createClusterManifest(
        alloc,
        "cluster-snap",
        "file:///backup",
        &committed_tables,
    );
    defer committed.deinit(alloc);
    try writeClusterManifestToLocationWithIo(alloc, io, &location, &committed);
    const attempt_tables = [_]ClusterBackupAttemptTable{.{
        .name = "docs",
        .table_backup_id = "table-snap",
        .artifact_backup_id = "artifact-snap",
    }};
    const marker: ClusterBackupAttemptMarker = .{
        .attempt_id = "attempt-snap",
        .cluster_backup_id = "cluster-snap",
        .created_at_unix_ns = 1,
        .format = .portable,
        .tables = &attempt_tables,
    };
    try writeClusterBackupAttemptMarker(alloc, io, &location, &marker);
    try writeClusterBackupAttemptHead(alloc, io, &location, marker.attempt_id);
    const artifact_path = try std.fmt.allocPrint(alloc, "{s}/table-snap.afb", .{root});
    defer alloc.free(artifact_path);
    try writeFileAbsolute(artifact_path, "PAYLOAD");

    try std.testing.expectError(
        error.BackupArtifactIntegrityMismatch,
        ensureNewestClusterBackupAttemptRestorable(alloc, io, &location),
    );

    var memory = object_storage.MemoryObjectStorage.init(alloc);
    defer memory.deinit();
    var remote: BackupLocation = .{
        .remote = try RemoteBackupStore.initWithClient(alloc, memory.client(), "bucket", "backups"),
    };
    defer remote.deinit(alloc);
    try writePortableListValidationFixture(alloc, &remote, "remote-table", "docs");
    const remote_tables = [_]ClusterTableBackupEntry{.{
        .name = "docs",
        .table_backup_id = "remote-table",
    }};
    var remote_manifest = try createClusterManifest(
        alloc,
        "remote-cluster",
        "s3://bucket/backups",
        &remote_tables,
    );
    defer remote_manifest.deinit(alloc);
    try verifyClusterBackupArtifactsIntegrityAtLocation(
        alloc,
        io,
        &remote,
        &remote_manifest,
    );
    try remote.remote.writeBytes(
        alloc,
        "remote-table.afb",
        "PAYLOAD",
        "application/vnd.antfly.backup",
    );
    try std.testing.expectError(
        error.BackupArtifactIntegrityMismatch,
        verifyClusterBackupArtifactsIntegrityAtLocation(
            alloc,
            io,
            &remote,
            &remote_manifest,
        ),
    );
}

test "unpublished remote cleanup preserves a conflicting manifest" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    var memory = object_storage.MemoryObjectStorage.init(alloc);
    defer memory.deinit();
    var location: BackupLocation = .{
        .remote = try RemoteBackupStore.initWithClient(alloc, memory.client(), "bucket", "backups"),
    };
    defer location.deinit(alloc);

    try location.remote.writeBytes(alloc, "shared-metadata.json", "existing-commit", "application/json");
    try reserveBackupAtLocation(alloc, io, &location, "shared", false);
    try location.remote.writeBytes(
        alloc,
        "attempt-artifact.afb",
        "new-attempt",
        "application/vnd.antfly.backup",
    );
    try cleanupUnpublishedTableBackupAttemptAtLocation(
        alloc,
        io,
        &location,
        "shared",
        "attempt-artifact",
        .portable,
    );

    const committed = try location.remote.readBytesAllocLimited(alloc, "shared-metadata.json", 64);
    defer alloc.free(committed);
    try std.testing.expectEqualStrings("existing-commit", committed);
    try std.testing.expectError(
        error.FileNotFound,
        location.remote.readBytesAllocLimited(alloc, "attempt-artifact.afb", 64),
    );
    try reserveBackupAtLocation(alloc, io, &location, "shared", false);
}

test "cluster backup attempt markers reject overlapping cleanup identities" {
    const tables = [_]ClusterBackupAttemptTable{.{
        .name = "docs",
        .table_backup_id = "shared-id",
        .artifact_backup_id = "shared-id",
    }};
    var marker: ClusterBackupAttemptMarker = .{
        .attempt_id = "attempt-snap",
        .cluster_backup_id = "cluster-snap",
        .created_at_unix_ns = 1,
        .format = .portable,
        .tables = &tables,
    };
    try std.testing.expectError(
        error.InvalidBackupRequest,
        validateClusterBackupAttemptMarker(std.testing.allocator, &marker, marker.attempt_id),
    );

    const valid_tables = [_]ClusterBackupAttemptTable{.{
        .name = "docs",
        .table_backup_id = "table-snap",
        .artifact_backup_id = "artifact-snap",
    }};
    marker.tables = &valid_tables;
    marker.created_at_unix_ns = 0;
    try std.testing.expectError(
        error.InvalidBackupRequest,
        validateClusterBackupAttemptMarker(std.testing.allocator, &marker, marker.attempt_id),
    );
}

test "stale uncommitted cluster backup attempt is retained without lease authority" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    var memory = object_storage.MemoryObjectStorage.init(alloc);
    defer memory.deinit();
    var location: BackupLocation = .{
        .remote = try RemoteBackupStore.initWithClient(alloc, memory.client(), "bucket", "backups"),
    };
    defer location.deinit(alloc);

    try reserveClusterBackupAttemptAtLocation(
        alloc,
        io,
        &location,
        "cluster-snap",
        "attempt-snap",
    );
    try reserveBackupAtLocation(alloc, io, &location, "table-snap", false);
    try location.remote.writeBytes(
        alloc,
        "artifact-snap.afb",
        "payload",
        "application/vnd.antfly.backup",
    );
    const tables = [_]ClusterBackupAttemptTable{.{
        .name = "docs",
        .table_backup_id = "table-snap",
        .artifact_backup_id = "artifact-snap",
    }};
    const marker: ClusterBackupAttemptMarker = .{
        .attempt_id = "attempt-snap",
        .cluster_backup_id = "cluster-snap",
        .created_at_unix_ns = 1,
        .format = .portable,
        .tables = &tables,
    };
    try writeClusterBackupAttemptMarker(alloc, io, &location, &marker);

    try std.testing.expectEqual(
        @as(usize, 0),
        try reclaimStaleClusterBackupAttempts(
            alloc,
            io,
            &location,
            backup_attempt_reclaim_age_ns + 1,
        ),
    );
    const retained_artifact = try location.remote.readBytesAllocLimited(alloc, "artifact-snap.afb", 16);
    defer alloc.free(retained_artifact);
    try std.testing.expectEqualStrings("payload", retained_artifact);
    var retained_marker = try readClusterBackupAttemptMarker(alloc, io, &location, "attempt-snap");
    defer retained_marker.deinit();
    try std.testing.expectError(
        error.BackupAlreadyExists,
        reserveBackupAtLocation(alloc, io, &location, "cluster-snap", true),
    );
    try std.testing.expectError(
        error.BackupAlreadyExists,
        reserveBackupAtLocation(alloc, io, &location, "table-snap", false),
    );

    try std.testing.expect(try cleanupClusterReservationIfOwnedAtLocation(
        alloc,
        io,
        &location,
        "cluster-snap",
        "attempt-snap",
    ));
    try reserveClusterBackupAttemptAtLocation(
        alloc,
        io,
        &location,
        "cluster-snap",
        "attempt-retry",
    );
    try std.testing.expectEqual(
        @as(usize, 1),
        try reclaimStaleClusterBackupAttempts(
            alloc,
            io,
            &location,
            backup_attempt_reclaim_age_ns + 1,
        ),
    );
    try std.testing.expectError(
        error.FileNotFound,
        location.remote.readBytesAllocLimited(alloc, "artifact-snap.afb", 16),
    );
    try std.testing.expectError(
        error.BackupAlreadyExists,
        reserveClusterBackupAttemptAtLocation(
            alloc,
            io,
            &location,
            "cluster-snap",
            "attempt-third",
        ),
    );
}

test "stale cluster backup attempt preserves aggregate referenced artifacts" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    var memory = object_storage.MemoryObjectStorage.init(alloc);
    defer memory.deinit();
    var location: BackupLocation = .{
        .remote = try RemoteBackupStore.initWithClient(alloc, memory.client(), "bucket", "backups"),
    };
    defer location.deinit(alloc);

    const tables = [_]ClusterBackupAttemptTable{
        .{ .name = "kept", .table_backup_id = "kept-table", .artifact_backup_id = "kept-artifact" },
        .{ .name = "loser", .table_backup_id = "loser-table", .artifact_backup_id = "loser-artifact" },
    };
    const marker: ClusterBackupAttemptMarker = .{
        .attempt_id = "attempt-two",
        .cluster_backup_id = "cluster-two",
        .created_at_unix_ns = 1,
        .format = .native,
        .tables = &tables,
    };
    try reserveBackupAtLocation(alloc, io, &location, marker.cluster_backup_id, true);
    try writeClusterBackupAttemptMarker(alloc, io, &location, &marker);
    for (tables) |table| {
        try reserveBackupAtLocation(alloc, io, &location, table.table_backup_id, false);
        const manifest_suffix = try metadataPath(alloc, "", table.table_backup_id);
        defer alloc.free(manifest_suffix);
        try location.remote.writeBytes(alloc, trimLeftSlash(manifest_suffix), "table", "application/json");
        const artifact_suffix = try std.fmt.allocPrint(alloc, "{s}/groups/1/store", .{table.artifact_backup_id});
        defer alloc.free(artifact_suffix);
        try location.remote.writeBytes(alloc, artifact_suffix, table.name, "application/octet-stream");
    }
    const committed_tables = [_]ClusterTableBackupEntry{.{
        .name = tables[0].name,
        .table_backup_id = tables[0].table_backup_id,
    }};
    var committed = try createClusterManifest(
        alloc,
        marker.cluster_backup_id,
        "s3://bucket/backups",
        &committed_tables,
    );
    defer committed.deinit(alloc);
    try writeClusterManifestToLocationWithIo(alloc, io, &location, &committed);

    try std.testing.expectEqual(
        @as(usize, 0),
        try reclaimStaleClusterBackupAttempts(
            alloc,
            io,
            &location,
            marker.created_at_unix_ns + backup_attempt_reclaim_age_ns,
        ),
    );
    const kept_suffix = try std.fmt.allocPrint(alloc, "{s}/groups/1/store", .{tables[0].artifact_backup_id});
    defer alloc.free(kept_suffix);
    const kept = try location.remote.readBytesAllocLimited(alloc, kept_suffix, 64);
    defer alloc.free(kept);
    try std.testing.expectEqualStrings(tables[0].name, kept);
    const loser_suffix = try std.fmt.allocPrint(alloc, "{s}/groups/1/store", .{tables[1].artifact_backup_id});
    defer alloc.free(loser_suffix);
    const loser = try location.remote.readBytesAllocLimited(alloc, loser_suffix, 64);
    defer alloc.free(loser);
    try std.testing.expectEqualStrings(tables[1].name, loser);
    var retained = try readClusterBackupAttemptMarker(alloc, io, &location, marker.attempt_id);
    defer retained.deinit();
    try std.testing.expectError(
        error.BackupAlreadyExists,
        reserveBackupAtLocation(alloc, io, &location, marker.cluster_backup_id, true),
    );
}

test "filesystem backup listing is bounded and cursor stable" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/backup-list", .{tmp.sub_path});
    defer alloc.free(root);
    const entries = [_]ClusterTableBackupEntry{.{ .name = "docs", .table_backup_id = "docs-snapshot" }};
    var location: BackupLocation = .{ .file = root };
    try writePortableListValidationFixture(
        alloc,
        &location,
        entries[0].table_backup_id,
        entries[0].name,
    );
    inline for (&.{ "prod-snap", "prod-snap-3", "prod-snap-2" }) |backup_id| {
        var manifest = try createClusterManifest(alloc, backup_id, "file:///backups", &entries);
        defer manifest.deinit(alloc);
        try writeClusterManifest(alloc, root, &manifest);
    }
    const corrupt_path = try clusterMetadataPath(alloc, root, "prod-snap-1");
    defer alloc.free(corrupt_path);
    try writeFileAbsolute(corrupt_path, "{not-json");

    var first = try listClusterBackups(alloc, root, "file:///backups", .{ .limit = 2 });
    defer first.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), first.backups.len);
    try std.testing.expectEqualStrings("prod-snap-2", first.backups[0].backup_id);
    try std.testing.expectEqualStrings("prod-snap-3", first.backups[1].backup_id);
    try std.testing.expectEqualStrings("prod-snap-3", first.next_cursor.?);

    var second = try listClusterBackups(alloc, root, "file:///backups", .{
        .limit = 2,
        .cursor = first.next_cursor,
    });
    defer second.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), second.backups.len);
    try std.testing.expectEqualStrings("prod-snap", second.backups[0].backup_id);
    try std.testing.expectEqual(@as(?[]u8, null), second.next_cursor);
}

test "native backup directory copy preserves nested files" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const src = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/copy-src", .{tmp.sub_path});
    defer alloc.free(src);
    const dst = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/copy-dst", .{tmp.sub_path});
    defer alloc.free(dst);
    const top = try std.fmt.allocPrint(alloc, "{s}/top.sst", .{src});
    defer alloc.free(top);
    const nested = try std.fmt.allocPrint(alloc, "{s}/nested/data.sst", .{src});
    defer alloc.free(nested);
    try writeFileAbsolute(top, "top");
    try writeFileAbsolute(nested, "nested");

    try copyDirectoryRecursive(alloc, src, dst);

    const copied_top_path = try std.fmt.allocPrint(alloc, "{s}/top.sst", .{dst});
    defer alloc.free(copied_top_path);
    const copied_nested_path = try std.fmt.allocPrint(alloc, "{s}/nested/data.sst", .{dst});
    defer alloc.free(copied_nested_path);
    const copied_top = try readFileAbsoluteAlloc(alloc, copied_top_path, 16);
    defer alloc.free(copied_top);
    const copied_nested = try readFileAbsoluteAlloc(alloc, copied_nested_path, 16);
    defer alloc.free(copied_nested);
    try std.testing.expectEqualStrings("top", copied_top);
    try std.testing.expectEqualStrings("nested", copied_nested);

    var source_integrity = try artifactIntegrityAlloc(alloc, null, .native, src);
    defer source_integrity.deinit(alloc);
    var copied_integrity = try artifactIntegrityAlloc(alloc, null, .native, dst);
    defer copied_integrity.deinit(alloc);
    try std.testing.expectEqual(source_integrity.size_bytes, copied_integrity.size_bytes);
    try std.testing.expectEqualStrings(source_integrity.sha256, copied_integrity.sha256);

    const expected = ShardSnapshot{
        .group_id = 7,
        .start_key = "",
        .snapshot_path = "snap/groups/7",
        .artifact_size_bytes = copied_integrity.size_bytes,
        .artifact_sha256 = copied_integrity.sha256,
    };
    try writeFileAbsolute(copied_nested_path, "corrupt");
    try std.testing.expectError(
        error.BackupArtifactIntegrityMismatch,
        verifyShardArtifactIntegrity(alloc, null, .native, dst, &expected),
    );
}

test "backup manifest validation rejects ambiguous or unbound artifacts" {
    const valid_hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    const duplicate_groups = [_]ShardSnapshot{
        .{
            .group_id = 7,
            .start_key = "",
            .end_key = "m",
            .snapshot_path = "generation/groups/7",
            .artifact_sha256 = valid_hash,
        },
        .{
            .group_id = 7,
            .start_key = "m",
            .snapshot_path = "generation/groups/8",
            .artifact_sha256 = valid_hash,
        },
    };
    var manifest = TableBackupManifest{
        .format = .native,
        .backup_id = "snap",
        .table_name = "docs",
        .description = "",
        .schema_json = "{}",
        .read_schema_json = "",
        .indexes_json = "{}",
        .replication_sources_json = "[]",
        .shards = &duplicate_groups,
    };
    try std.testing.expectError(
        error.InvalidBackupRequest,
        validateTableManifest(std.testing.allocator, &manifest, "snap"),
    );

    const duplicate_paths = [_]ShardSnapshot{
        .{
            .group_id = 7,
            .start_key = "",
            .end_key = "m",
            .snapshot_path = "generation/groups/shared",
            .artifact_sha256 = valid_hash,
        },
        .{
            .group_id = 8,
            .start_key = "m",
            .snapshot_path = "generation/groups/shared",
            .artifact_sha256 = valid_hash,
        },
    };
    manifest.shards = &duplicate_paths;
    try std.testing.expectError(
        error.InvalidBackupRequest,
        validateTableManifest(std.testing.allocator, &manifest, "snap"),
    );

    const gapped_ranges = [_]ShardSnapshot{
        .{
            .group_id = 7,
            .start_key = "",
            .end_key = "m",
            .snapshot_path = "generation/groups/7",
            .artifact_sha256 = valid_hash,
        },
        .{
            .group_id = 8,
            .start_key = "n",
            .snapshot_path = "generation/groups/8",
            .artifact_sha256 = valid_hash,
        },
    };
    manifest.shards = &gapped_ranges;
    try std.testing.expectError(
        error.InvalidBackupRangeTopology,
        validateTableManifest(std.testing.allocator, &manifest, "snap"),
    );

    const overlapping_ranges = [_]ShardSnapshot{
        .{
            .group_id = 7,
            .start_key = "",
            .end_key = "n",
            .snapshot_path = "generation/groups/7",
            .artifact_sha256 = valid_hash,
        },
        .{
            .group_id = 8,
            .start_key = "m",
            .snapshot_path = "generation/groups/8",
            .artifact_sha256 = valid_hash,
        },
    };
    manifest.shards = &overlapping_ranges;
    try std.testing.expectError(
        error.InvalidBackupRangeTopology,
        validateTableManifest(std.testing.allocator, &manifest, "snap"),
    );

    const missing_integrity = [_]ShardSnapshot{.{
        .group_id = 7,
        .start_key = "",
        .snapshot_path = "generation/groups/7",
    }};
    manifest.shards = &missing_integrity;
    try std.testing.expectError(
        error.BackupIntegrityMissing,
        validateTableManifest(std.testing.allocator, &manifest, "snap"),
    );

    const mismatched_format = [_]ShardSnapshot{.{
        .group_id = 7,
        .start_key = "",
        .snapshot_path = "generation.afb",
        .artifact_sha256 = valid_hash,
    }};
    manifest.shards = &mismatched_format;
    try std.testing.expectError(
        error.BackupArtifactFormatMismatch,
        validateTableManifest(std.testing.allocator, &manifest, "snap"),
    );

    const unsupported_version = [_]ShardSnapshot{.{
        .group_id = 7,
        .start_key = "",
        .snapshot_path = "generation.afb",
        .artifact_sha256 = valid_hash,
    }};
    manifest.format_version = 0;
    manifest.format = .portable;
    manifest.shards = &unsupported_version;
    try std.testing.expectError(
        error.UnsupportedBackupFormat,
        validateRestoreManifest(std.testing.allocator, &manifest, "snap"),
    );
}

test "portable backup integrity rejects changed staged bytes" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/portable.afb", .{tmp.sub_path});
    defer alloc.free(path);
    try writeFileAbsolute(path, "portable-backup");

    var integrity = try artifactIntegrityAlloc(alloc, null, .portable, path);
    defer integrity.deinit(alloc);
    const shard = ShardSnapshot{
        .group_id = 7,
        .start_key = "",
        .snapshot_path = "generation.afb",
        .artifact_size_bytes = integrity.size_bytes,
        .artifact_sha256 = integrity.sha256,
    };
    try verifyShardArtifactIntegrity(alloc, null, .portable, path, &shard);

    try writeFileAbsolute(path, "changed-backup");
    try std.testing.expectError(
        error.BackupArtifactIntegrityMismatch,
        verifyShardArtifactIntegrity(alloc, null, .portable, path, &shard),
    );
}

test "backup remote location normalizes gcs alias" {
    const normalized = try normalizeRemoteLocationAlloc(std.testing.allocator, "gcs://bucket/path");
    defer std.testing.allocator.free(normalized);
    try std.testing.expectEqualStrings("gs://bucket/path", normalized);
}

test "backup connections enforce capability bucket and segment bounded prefix" {
    const json =
        \\{
        \\  "connections": {
        \\    "archive": {
        \\      "kind": "external_io",
        \\      "capabilities": ["backup.write", "restore.read"],
        \\      "external_io": {
        \\        "protocol": "s3",
        \\        "buckets": ["prod-archive"],
        \\        "prefix": "tenant-a/backups"
        \\      }
        \\    }
        \\  }
        \\}
    ;
    var config = try common_config.Config.parseFromSlice(std.testing.allocator, json);
    defer config.deinit();

    _ = try authorizedObjectConnection(&config, "archive", .s3, "prod-archive", "tenant-a/backups/daily", "backup.write");
    _ = try authorizedObjectConnection(&config, "archive", .s3, "prod-archive", "tenant-a/backups/daily", "restore.read");
    try std.testing.expectError(error.ConnectionCapabilityDenied, authorizedObjectConnection(&config, "archive", .s3, "prod-archive", "tenant-a/backups/daily", "objects.delete"));
    try std.testing.expectError(error.ConnectionBucketDenied, authorizedObjectConnection(&config, "archive", .s3, "other", "tenant-a/backups/daily", "backup.write"));
    try std.testing.expectError(error.ConnectionPrefixDenied, authorizedObjectConnection(&config, "archive", .s3, "prod-archive", "tenant-a/backups-evil", "backup.write"));
}

test "backup identifiers and artifact paths reject traversal" {
    try validateBackupId("daily-2026.07.11");
    try std.testing.expectError(error.InvalidBackupId, validateBackupId("../escape"));
    try std.testing.expectError(error.InvalidBackupId, validateBackupId("nested/name"));
    try validateArtifactRelativePath("daily/groups/7/data.bin");
    try std.testing.expectError(error.InvalidBackupArtifactPath, validateArtifactRelativePath("daily/../secrets"));
    try std.testing.expectError(error.InvalidBackupArtifactPath, validateArtifactRelativePath("/etc/passwd"));
}

test "derive restore table record returns owned table metadata" {
    const manifest = TableBackupManifest{
        .format = .native,
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

test "restore manifest preserves trusted coverage incarnation metadata" {
    const manifest = TableBackupManifest{
        .format = .native,
        .backup_id = "snap",
        .table_name = "docs",
        .description = "",
        .schema_json = "{}",
        .read_schema_json = "",
        .indexes_json = "{\"semantic\":{\"type\":\"embeddings\",\"dimension\":3,\"_coverage_incarnation\":42}}",
        .replication_sources_json = "[]",
        .shards = &.{},
    };

    var request = try createTableRequestFromManifest(std.testing.allocator, &manifest);
    defer request.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings(manifest.indexes_json, request.indexes_json.?);

    var invalid = manifest;
    invalid.indexes_json = "{\"full_text\":{\"type\":\"full_text\",\"_coverage_incarnation\":42}}";
    try std.testing.expectError(
        error.InvalidCreateTableRequest,
        createTableRequestFromManifest(std.testing.allocator, &invalid),
    );
}
