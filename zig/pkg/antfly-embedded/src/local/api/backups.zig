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

pub const std = @import("std");
pub const fs_paths = @import("antfly_runtime_fs").fs_paths;
pub const group_ids = @import("../common/group_ids.zig");
pub const threaded_io_limits = @import("antfly_runtime_fs").threaded_io_limits;
pub const metadata_table_manager = @import("../metadata/catalog.zig");
pub const object_storage = @import("../storage/object_storage.zig");
pub const remote_uri = @import("../serverless/remote_uri.zig");
pub const tables_api = @import("tables.zig");
pub const common_secrets = @import("../common/secrets.zig");
pub const common_config = @import("../common/config.zig");
pub const bedrock = @import("antfly_inference_bedrock");
pub const httpx = @import("httpx");
pub const google_auth = @import("antfly_google").auth;
pub const backup_contract = @import("backup_contract.zig");
pub const CancellationToken = @import("antfly_cancellation").CancellationToken;

pub fn objectCancellationToken(cancellation: CancellationToken) ?object_storage.CancellationToken {
    return object_storage.CancellationToken.fromCallback(
        cancellation.ptr,
        cancellation.is_cancelled_fn,
    );
}

pub const format_version = backup_contract.format_version;
pub const max_backup_manifest_bytes: usize = 16 * 1024 * 1024;
pub const max_backup_attempt_lease_bytes: usize = 256;
// `:` is intentionally outside validateBackupId's public alphabet. Cleanup
// owners therefore cannot be confused with a caller-supplied attempt ID, while
// the suffix can still carry the full 128-byte public identity losslessly.
pub const backup_cleanup_lease_owner_prefix = "antfly-cleanup:";
pub const backup_attempt_lease_renew_interval_ns: u64 = std.time.ns_per_min;
/// Current storage owners renew this lease for as long as artifact production
/// remains live. The initial 24-hour term preserves the pre-lease grace window
/// when forwarding to a rolling-upgrade peer that does not yet renew it.
pub const backup_attempt_lease_clock_skew_allowance_ns: u64 =
    2 * backup_attempt_lease_renew_interval_ns;
pub const backup_integrity_read_chunk_bytes: u64 = 8 * 1024 * 1024;
pub const backup_integrity_max_native_files: usize = 1_000_000;
pub const backup_integrity_max_native_list_pages: usize =
    (backup_integrity_max_native_files + 999) / 1000 + 1;
pub const BackupFormat = backup_contract.BackupFormat;
pub const ArtifactIntegrityMode = backup_contract.ArtifactIntegrityMode;
pub const TableBackupManifest = backup_contract.TableBackupManifest;
pub const ShardSnapshot = backup_contract.ShardSnapshot;

pub const ArtifactIntegrity = struct {
    size_bytes: u64,
    sha256: []u8,

    pub fn deinit(self: *ArtifactIntegrity, alloc: std.mem.Allocator) void {
        alloc.free(self.sha256);
        self.* = undefined;
    }
};

pub const TableBackupPlan = backup_contract.TableBackupPlan;
pub const TableRestorePlan = backup_contract.TableRestorePlan;
pub const BackupOperationControl = backup_contract.BackupOperationControl;
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

/// Production adapter from the configured backup location to the canonical
/// refs/manifests/blobs repository. The adapter borrows `location`; callers
/// must keep both values alive for the complete repository operation.
pub const OpenOptions = struct {
    secret_store: ?*common_secrets.FileStore = null,
    node_config: ?*const common_config.Config = null,
    connection: ?[]const u8 = null,
    required_capability: []const u8 = "",
    /// Network-only authority retained by remote object clients and dynamic
    /// credential refresh. Callers may omit it and receive an owned threaded
    /// fallback.
    network_io: ?std.Io = null,
    /// Filesystem-only authority used while resolving local repository paths
    /// and credential files. It is never retained as the remote transport.
    filesystem_io: ?std.Io = null,
};

pub fn createOwnedThreadedIo(alloc: std.mem.Allocator) !*std.Io.Threaded {
    const owned = try alloc.create(std.Io.Threaded);
    // CLI and embedded backup stores can live for the whole operation and
    // issue concurrent HTTP work. Keep the fallback finite when a server
    // runtime was not supplied.
    owned.* = threaded_io_limits.initService(alloc);
    return owned;
}

pub const AwsCredentialContext = struct {
    alloc: std.mem.Allocator,
    io_impl: ?*std.Io.Threaded,
    http: httpx.Client,
    cache: bedrock.CredentialCache = .{},
    region: []u8,
    source: bedrock.CredentialSource,
    filesystem_io: ?std.Io,

    pub fn init(
        alloc: std.mem.Allocator,
        region: []const u8,
        source: bedrock.CredentialSource,
        network_io: ?std.Io,
        filesystem_io: ?std.Io,
    ) !AwsCredentialContext {
        const owned_region = try alloc.dupe(u8, region);
        errdefer alloc.free(owned_region);
        const io_impl: ?*std.Io.Threaded = if (network_io == null) blk: {
            break :blk try createOwnedThreadedIo(alloc);
        } else null;
        errdefer if (io_impl) |owned| {
            owned.deinit();
            alloc.destroy(owned);
        };
        return .{
            .alloc = alloc,
            .io_impl = io_impl,
            .http = httpx.Client.init(alloc, network_io orelse io_impl.?.io()),
            .region = owned_region,
            .source = source,
            .filesystem_io = filesystem_io,
        };
    }

    pub fn deinit(self: *AwsCredentialContext) void {
        self.cache.deinit(self.alloc);
        self.http.deinit();
        if (self.io_impl) |io_impl| {
            io_impl.deinit();
            self.alloc.destroy(io_impl);
        }
        self.alloc.free(self.region);
        self.* = undefined;
    }

    pub fn provider(self: *AwsCredentialContext) object_storage.S3.CredentialProvider {
        return .{ .ptr = self, .get_fn = get };
    }

    pub fn get(ptr: *anyopaque, alloc: std.mem.Allocator) anyerror!object_storage.S3.DynamicCredentials {
        const self: *AwsCredentialContext = @ptrCast(@alignCast(ptr));
        _ = alloc;
        const lease = try self.cache.getLeaseForSourceWithIo(
            self.alloc,
            &self.http,
            self.filesystem_io,
            self.region,
            self.source,
        );
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

pub fn authorizedObjectConnection(
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

pub fn authorizedFilesystemConnection(
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

pub fn s3ConfigForConnection(
    alloc: std.mem.Allocator,
    external: common_config.Config.ExternalIoConnectionConfig,
    credential_context: *?*AwsCredentialContext,
    network_io: ?std.Io,
    filesystem_io: ?std.Io,
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
    context.* = try AwsCredentialContext.init(
        alloc,
        cfg.credentials.region,
        source,
        network_io,
        filesystem_io,
    );
    credential_context.* = context;
    cfg.credential_provider = context.provider();
    return cfg;
}

pub const BackupLeaseFenceClaim = enum { active, claimed };

pub const RemoteBackupStore = struct {
    const BoundedReadOptions = struct {
        known_size: ?u64 = null,
        skip_metadata_probe: bool = false,
        if_match_etag: ?[]const u8 = null,
        cancellation: CancellationToken = .none,
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

    pub fn initRemoteUri(alloc: std.mem.Allocator, location: []const u8, options: OpenOptions) !RemoteBackupStore {
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

    pub fn initGcsUri(alloc: std.mem.Allocator, bucket: []const u8, prefix: []const u8, options: OpenOptions) !RemoteBackupStore {
        const io_impl: ?*std.Io.Threaded = if (options.network_io == null) blk: {
            break :blk try createOwnedThreadedIo(alloc);
        } else null;
        errdefer if (io_impl) |owned| {
            owned.deinit();
            alloc.destroy(owned);
        };
        const network_io = options.network_io orelse io_impl.?.io();
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
            break :blk try gcsConfigForConnection(
                alloc,
                resolved_credentials.?.apply(external),
                network_io,
                options.filesystem_io,
            );
        } else try object_storage.Gcs.jsonApiClientConfigFromEnvWithAuthoritiesAlloc(
            alloc,
            null,
            network_io,
            options.filesystem_io,
        );
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
            .io = network_io,
            .client = gcs.client(),
            .gcs_client = gcs,
            .resolved_credentials = resolved_credentials,
            .create_bucket_if_missing = create_bucket_if_missing,
            .bucket = owned_bucket,
            .prefix = owned_prefix,
        };
    }

    pub fn initS3Uri(
        alloc: std.mem.Allocator,
        bucket: []const u8,
        prefix: []const u8,
        options: OpenOptions,
    ) !RemoteBackupStore {
        const io_impl: ?*std.Io.Threaded = if (options.network_io == null) blk: {
            break :blk try createOwnedThreadedIo(alloc);
        } else null;
        errdefer if (io_impl) |owned| {
            owned.deinit();
            alloc.destroy(owned);
        };
        const network_io = options.network_io orelse io_impl.?.io();
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
            break :blk try s3ConfigForConnection(
                alloc,
                resolved_credentials.?.apply(connection),
                &credential_context,
                network_io,
                options.filesystem_io,
            );
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
        cfg.io = network_io;
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
            .io = network_io,
            .client = s3.client(),
            .s3_client = s3,
            .credential_context = credential_context,
            .resolved_credentials = resolved_credentials,
            .create_bucket_if_missing = create_bucket_if_missing,
            .bucket = owned_bucket,
            .prefix = owned_prefix,
        };
    }

    pub fn initWithClient(
        alloc: std.mem.Allocator,
        client: object_storage.ObjectStorage,
        bucket: []const u8,
        prefix: []const u8,
    ) !RemoteBackupStore {
        const io_impl = try createOwnedThreadedIo(alloc);
        errdefer alloc.destroy(io_impl);
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

    pub fn deinit(self: *RemoteBackupStore) void {
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

    pub fn ensureBucket(self: *RemoteBackupStore) !void {
        return self.ensureBucketWithCancellation(.none);
    }

    pub fn ensureBucketWithCancellation(
        self: *RemoteBackupStore,
        cancellation: CancellationToken,
    ) !void {
        // Normal backup writers may intentionally have PutObject without
        // HeadBucket/ListBucket. Only provisioning connections need to probe
        // and create buckets; ordinary writes let the object operation report
        // a missing or unauthorized bucket directly.
        if (!self.create_bucket_if_missing) return;
        if (self.bucket_ready.load(.acquire)) return;
        try cancellation.check();
        const options: object_storage.BucketOptions = .{
            .cancellation = objectCancellationToken(cancellation),
        };
        if (try self.client.bucketExistsWithOptions(self.bucket, options)) {
            self.bucket_ready.store(true, .release);
            return;
        }
        self.client.makeBucketWithOptions(self.bucket, options) catch |err| {
            // Another request may have created the bucket after our probe.
            // Recheck rather than surfacing a harmless provider conflict.
            try cancellation.check();
            if (self.client.bucketExistsWithOptions(self.bucket, options) catch false) {
                self.bucket_ready.store(true, .release);
                return;
            }
            try cancellation.check();
            return err;
        };
        try cancellation.check();
        self.bucket_ready.store(true, .release);
    }

    pub fn keyAlloc(self: *const RemoteBackupStore, alloc: std.mem.Allocator, suffix: []const u8) ![]u8 {
        const canonical_prefix = trimRightSlash(self.prefix);
        const trimmed_suffix = trimLeftSlash(suffix);
        if (trimmed_suffix.len > 0) try validateArtifactRelativePath(trimmed_suffix);
        if (canonical_prefix.len == 0) return try alloc.dupe(u8, trimmed_suffix);
        if (trimmed_suffix.len == 0) return try alloc.dupe(u8, canonical_prefix);
        return try std.fmt.allocPrint(alloc, "{s}/{s}", .{ canonical_prefix, trimmed_suffix });
    }

    pub fn keyPrefixAlloc(self: *const RemoteBackupStore, alloc: std.mem.Allocator, suffix: []const u8) ![]u8 {
        const trimmed = std.mem.trim(u8, suffix, "/");
        if (trimmed.len == 0) return error.InvalidBackupArtifactPath;
        const base = try self.keyAlloc(alloc, trimmed);
        defer alloc.free(base);
        return try std.fmt.allocPrint(alloc, "{s}/", .{base});
    }

    pub fn writeBytes(self: *RemoteBackupStore, alloc: std.mem.Allocator, suffix: []const u8, body: []const u8, content_type: []const u8) !void {
        return self.writeBytesWithCancellation(alloc, suffix, body, content_type, .none);
    }

    pub fn writeBytesWithCancellation(
        self: *RemoteBackupStore,
        alloc: std.mem.Allocator,
        suffix: []const u8,
        body: []const u8,
        content_type: []const u8,
        cancellation: CancellationToken,
    ) !void {
        try cancellation.check();
        try self.ensureBucketWithCancellation(cancellation);
        const key = try self.keyAlloc(alloc, suffix);
        defer alloc.free(key);
        var result = try self.client.putObject(self.bucket, key, body, .{
            .content_type = content_type,
            .cancellation = objectCancellationToken(cancellation),
        });
        defer result.deinit(alloc);
    }

    pub fn writeBytesIfAbsent(self: *RemoteBackupStore, alloc: std.mem.Allocator, suffix: []const u8, body: []const u8, content_type: []const u8) !void {
        return self.writeBytesIfAbsentWithCancellation(alloc, suffix, body, content_type, .none);
    }

    pub fn writeBytesIfAbsentWithCancellation(
        self: *RemoteBackupStore,
        alloc: std.mem.Allocator,
        suffix: []const u8,
        body: []const u8,
        content_type: []const u8,
        cancellation: CancellationToken,
    ) !void {
        try cancellation.check();
        try self.ensureBucketWithCancellation(cancellation);
        const key = try self.keyAlloc(alloc, suffix);
        defer alloc.free(key);
        var result = self.client.putObject(self.bucket, key, body, .{
            .content_type = content_type,
            .if_none_match = true,
            .cancellation = objectCancellationToken(cancellation),
        }) catch |err| switch (err) {
            error.PreconditionFailed => return error.BackupAlreadyExists,
            else => return err,
        };
        defer result.deinit(alloc);
    }

    pub fn replaceBytesIfOwned(
        self: *RemoteBackupStore,
        alloc: std.mem.Allocator,
        suffix: []const u8,
        expected_owner: []const u8,
        body: []const u8,
        content_type: []const u8,
    ) !bool {
        return self.replaceBytesIfOwnedWithCancellation(
            alloc,
            suffix,
            expected_owner,
            body,
            content_type,
            .none,
        );
    }

    pub fn replaceBytesIfOwnedWithCancellation(
        self: *RemoteBackupStore,
        alloc: std.mem.Allocator,
        suffix: []const u8,
        expected_owner: []const u8,
        body: []const u8,
        content_type: []const u8,
        cancellation: CancellationToken,
    ) !bool {
        try cancellation.check();
        const key = try self.keyAlloc(alloc, suffix);
        defer alloc.free(key);
        var current = self.client.getObject(self.bucket, key, .{
            .range = .{ .offset = 0, .length = max_backup_attempt_lease_bytes },
            .skip_metadata_probe = true,
            .max_response_bytes = max_backup_attempt_lease_bytes,
            .cancellation = objectCancellationToken(cancellation),
        }) catch |err| switch (err) {
            error.FileNotFound => return false,
            else => return err,
        };
        defer current.deinit(alloc);
        if (!std.mem.eql(u8, reservationOwner(current.body), expected_owner))
            return false;
        const etag = current.metadata.etag orelse
            return error.BackupReservationIdentityUnavailable;
        var result = self.client.putObject(self.bucket, key, body, .{
            .content_type = content_type,
            .if_match_etag = etag,
            .cancellation = objectCancellationToken(cancellation),
        }) catch |err| switch (err) {
            error.FileNotFound, error.PreconditionFailed => return false,
            else => return err,
        };
        defer result.deinit(alloc);
        return true;
    }

    pub fn deleteSuffix(self: *RemoteBackupStore, alloc: std.mem.Allocator, suffix: []const u8) !void {
        return self.deleteSuffixWithCancellation(alloc, suffix, .none);
    }

    pub fn deleteSuffixWithCancellation(
        self: *RemoteBackupStore,
        alloc: std.mem.Allocator,
        suffix: []const u8,
        cancellation: CancellationToken,
    ) !void {
        try cancellation.check();
        const key = try self.keyAlloc(alloc, suffix);
        defer alloc.free(key);
        self.client.deleteObject(self.bucket, key, .{
            .cancellation = objectCancellationToken(cancellation),
        }) catch |err| switch (err) {
            error.FileNotFound => {},
            else => return err,
        };
    }

    pub fn deleteSuffixBudgeted(
        self: *RemoteBackupStore,
        alloc: std.mem.Allocator,
        suffix: []const u8,
        operation_budget: *usize,
    ) !void {
        return self.deleteSuffixBudgetedWithCancellation(
            alloc,
            suffix,
            operation_budget,
            .none,
        );
    }

    pub fn deleteSuffixBudgetedWithCancellation(
        self: *RemoteBackupStore,
        alloc: std.mem.Allocator,
        suffix: []const u8,
        operation_budget: *usize,
        cancellation: CancellationToken,
    ) !void {
        try cancellation.check();
        if (operation_budget.* == 0) return error.BackupCleanupBudgetExceeded;
        operation_budget.* -= 1;
        try self.deleteSuffixWithCancellation(alloc, suffix, cancellation);
    }

    /// Delete a small mutable object only when its current contents still name
    /// the expected owner. The ETag condition closes the GET/delete race.
    pub fn deleteSuffixIfOwned(
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
            .max_response_bytes = expected_owner.len + 3,
        }) catch |err| switch (err) {
            error.FileNotFound => return false,
            else => return err,
        };
        defer result.deinit(alloc);
        const actual_owner = reservationOwner(result.body);
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

    pub fn suffixOwnerMatches(
        self: *RemoteBackupStore,
        alloc: std.mem.Allocator,
        suffix: []const u8,
        expected_owner: []const u8,
    ) !?bool {
        return self.suffixOwnerMatchesWithCancellation(
            alloc,
            suffix,
            expected_owner,
            .none,
        );
    }

    pub fn suffixOwnerMatchesWithCancellation(
        self: *RemoteBackupStore,
        alloc: std.mem.Allocator,
        suffix: []const u8,
        expected_owner: []const u8,
        cancellation: CancellationToken,
    ) !?bool {
        try cancellation.check();
        const key = try self.keyAlloc(alloc, suffix);
        defer alloc.free(key);
        var result = self.client.getObject(self.bucket, key, .{
            .range = .{ .offset = 0, .length = max_backup_attempt_lease_bytes },
            .skip_metadata_probe = true,
            .max_response_bytes = max_backup_attempt_lease_bytes,
            .cancellation = objectCancellationToken(cancellation),
        }) catch |err| switch (err) {
            error.FileNotFound => return null,
            else => return err,
        };
        defer result.deinit(alloc);
        return std.mem.eql(u8, reservationOwner(result.body), expected_owner);
    }

    /// Atomically remove an expired lease and return its owner. The ETag
    /// protects a concurrent heartbeat or replacement from stale cleanup.
    pub fn takeExpiredReservation(
        self: *RemoteBackupStore,
        alloc: std.mem.Allocator,
        suffix: []const u8,
        now_unix_ns: u64,
        expected_owner: ?[]const u8,
    ) !?[]u8 {
        const key = try self.keyAlloc(alloc, suffix);
        defer alloc.free(key);
        var current = self.client.getObject(self.bucket, key, .{
            .range = .{ .offset = 0, .length = max_backup_attempt_lease_bytes },
            .skip_metadata_probe = true,
            .max_response_bytes = max_backup_attempt_lease_bytes,
        }) catch |err| switch (err) {
            error.FileNotFound => return null,
            else => return err,
        };
        defer current.deinit(alloc);
        const lease = parseClusterBackupReservationLease(current.body) catch
            return null;
        if (expected_owner) |owner| {
            if (!std.mem.eql(u8, lease.attempt_id, owner)) return null;
        }
        if (!clusterBackupLeaseReclaimable(
            lease.expires_at_unix_ns,
            now_unix_ns,
        ))
            return null;
        const etag = current.metadata.etag orelse
            return error.BackupReservationIdentityUnavailable;
        const owned_attempt_id = try alloc.dupe(u8, lease.attempt_id);
        errdefer alloc.free(owned_attempt_id);
        self.client.deleteObject(self.bucket, key, .{
            .if_match_etag = etag,
        }) catch |err| switch (err) {
            error.FileNotFound, error.PreconditionFailed => {
                alloc.free(owned_attempt_id);
                return null;
            },
            else => return err,
        };
        return owned_attempt_id;
    }

    /// Atomically replaces an expired lease, or fills an absent lease slot,
    /// with a durable cleanup owner. Unlike deletion, the replacement keeps
    /// delayed writers fenced until exact cleanup has completed.
    pub fn claimExpiredLeaseWithFence(
        self: *RemoteBackupStore,
        alloc: std.mem.Allocator,
        suffix: []const u8,
        expected_owner: []const u8,
        cleanup_owner: []const u8,
        legacy_cleanup_owner: ?[]const u8,
        now_unix_ns: u64,
        replacement: []const u8,
    ) !BackupLeaseFenceClaim {
        return self.claimExpiredLeaseWithFenceAndCancellation(
            alloc,
            suffix,
            expected_owner,
            cleanup_owner,
            legacy_cleanup_owner,
            now_unix_ns,
            replacement,
            .none,
        );
    }

    pub fn claimExpiredLeaseWithFenceAndCancellation(
        self: *RemoteBackupStore,
        alloc: std.mem.Allocator,
        suffix: []const u8,
        expected_owner: []const u8,
        cleanup_owner: []const u8,
        legacy_cleanup_owner: ?[]const u8,
        now_unix_ns: u64,
        replacement: []const u8,
        cancellation: CancellationToken,
    ) !BackupLeaseFenceClaim {
        try cancellation.check();
        const key = try self.keyAlloc(alloc, suffix);
        defer alloc.free(key);
        var current = self.client.getObject(self.bucket, key, .{
            .range = .{ .offset = 0, .length = max_backup_attempt_lease_bytes },
            .skip_metadata_probe = true,
            .max_response_bytes = max_backup_attempt_lease_bytes,
            .cancellation = objectCancellationToken(cancellation),
        }) catch |err| switch (err) {
            error.FileNotFound => {
                var created = self.client.putObject(self.bucket, key, replacement, .{
                    .content_type = "text/plain",
                    .if_none_match = true,
                    .cancellation = objectCancellationToken(cancellation),
                }) catch |put_err| switch (put_err) {
                    error.PreconditionFailed => return .active,
                    else => return put_err,
                };
                created.deinit(alloc);
                return .claimed;
            },
            else => return err,
        };
        defer current.deinit(alloc);
        const lease = parseClusterBackupReservationLease(current.body) catch return .active;
        if (std.mem.eql(u8, lease.attempt_id, cleanup_owner)) return .claimed;
        if (legacy_cleanup_owner) |legacy_owner| {
            if (std.mem.eql(u8, lease.attempt_id, legacy_owner)) {
                const etag = current.metadata.etag orelse
                    return error.BackupReservationIdentityUnavailable;
                var migrated = self.client.putObject(self.bucket, key, replacement, .{
                    .content_type = "text/plain",
                    .if_match_etag = etag,
                    .cancellation = objectCancellationToken(cancellation),
                }) catch |err| switch (err) {
                    error.FileNotFound, error.PreconditionFailed => return .active,
                    else => return err,
                };
                migrated.deinit(alloc);
                return .claimed;
            }
        }
        if (!std.mem.eql(u8, lease.attempt_id, expected_owner) or
            !clusterBackupLeaseReclaimable(lease.expires_at_unix_ns, now_unix_ns))
            return .active;
        const etag = current.metadata.etag orelse
            return error.BackupReservationIdentityUnavailable;
        var replaced = self.client.putObject(self.bucket, key, replacement, .{
            .content_type = "text/plain",
            .if_match_etag = etag,
            .cancellation = objectCancellationToken(cancellation),
        }) catch |err| switch (err) {
            error.FileNotFound, error.PreconditionFailed => return .active,
            else => return err,
        };
        replaced.deinit(alloc);
        return .claimed;
    }

    pub fn deletePrefix(
        self: *RemoteBackupStore,
        alloc: std.mem.Allocator,
        suffix: []const u8,
        operation_budget: *usize,
    ) !void {
        return self.deletePrefixWithCancellation(
            alloc,
            suffix,
            operation_budget,
            .none,
        );
    }

    pub fn deletePrefixWithCancellation(
        self: *RemoteBackupStore,
        alloc: std.mem.Allocator,
        suffix: []const u8,
        operation_budget: *usize,
        cancellation: CancellationToken,
    ) !void {
        try cancellation.check();
        const key_prefix = try self.keyPrefixAlloc(alloc, suffix);
        defer alloc.free(key_prefix);
        // Delete in bounded pages. Restarting at the prefix after each batch
        // avoids continuation-token invalidation while the keyset is changing.
        while (true) {
            try cancellation.check();
            // Reserve one operation for the listing request and at least one
            // for forward progress deleting an object returned by that list.
            if (operation_budget.* < 2) return error.BackupCleanupBudgetExceeded;
            const page_size: u32 = @intCast(@min(operation_budget.* - 1, 1000));
            operation_budget.* -= 1;
            var listed = try self.client.listObjects(self.bucket, .{
                .prefix = key_prefix,
                .recursive = true,
                .max_keys = page_size,
                .cancellation = objectCancellationToken(cancellation),
            });
            defer listed.deinit(alloc);
            if (listed.entries.len == 0) return;
            for (listed.entries) |entry| {
                try cancellation.check();
                if (!std.mem.startsWith(u8, entry.key, key_prefix))
                    return error.InvalidBackupArtifactPath;
                if (operation_budget.* == 0) return error.BackupCleanupBudgetExceeded;
                operation_budget.* -= 1;
                self.client.deleteObject(self.bucket, entry.key, .{
                    .cancellation = objectCancellationToken(cancellation),
                }) catch |err| switch (err) {
                    error.FileNotFound => {},
                    else => return err,
                };
            }
        }
    }

    pub fn writeFile(
        self: *RemoteBackupStore,
        alloc: std.mem.Allocator,
        source_io: std.Io,
        suffix: []const u8,
        src_path: []const u8,
        content_type: []const u8,
        cancellation: CancellationToken,
    ) !void {
        try cancellation.check();
        try self.ensureBucketWithCancellation(cancellation);
        const key = try self.keyAlloc(alloc, suffix);
        defer alloc.free(key);
        var result = try self.client.putFileWithIo(source_io, self.bucket, key, src_path, .{
            .content_type = content_type,
            .cancellation = objectCancellationToken(cancellation),
        });
        defer result.deinit(alloc);
    }

    pub fn readBytesAllocLimited(self: *RemoteBackupStore, alloc: std.mem.Allocator, suffix: []const u8, max_bytes: usize) ![]u8 {
        const key = try self.keyAlloc(alloc, suffix);
        defer alloc.free(key);
        return try self.readKeyBytesAllocLimited(alloc, key, max_bytes, .{});
    }

    pub fn readBytesAllocLimitedWithCancellation(
        self: *RemoteBackupStore,
        alloc: std.mem.Allocator,
        suffix: []const u8,
        max_bytes: usize,
        cancellation: CancellationToken,
    ) ![]u8 {
        const key = try self.keyAlloc(alloc, suffix);
        defer alloc.free(key);
        return try self.readKeyBytesAllocLimited(alloc, key, max_bytes, .{
            .cancellation = cancellation,
        });
    }

    pub fn readKeyBytesAllocLimited(
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
            .if_match_etag = options.if_match_etag,
            .max_response_bytes = max_bytes + 1,
            .cancellation = objectCancellationToken(options.cancellation),
        }) catch |err| switch (err) {
            error.ResponseTooLarge => return error.BackupManifestTooLarge,
            error.PreconditionFailed => return error.SourceFileChanged,
            else => return err,
        };
        defer result.deinit(alloc);
        if (result.body.len > max_bytes) return error.BackupManifestTooLarge;
        return try alloc.dupe(u8, result.body);
    }

    pub fn validateArtifactAvailable(
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
                if (metadata.content_length == 0)
                    return error.BackupArtifactMissing;
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

    pub fn hashObjectVersion(
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
                // Some object-storage gateways ignore Range. Keep every
                // integrity-verification request bounded independently of
                // provider behavior so a large artifact cannot be buffered.
                .max_response_bytes = @intCast(wanted),
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

    pub fn verifyPortableArtifactIntegrity(
        self: *RemoteBackupStore,
        alloc: std.mem.Allocator,
        shard: *const ShardSnapshot,
    ) !void {
        return self.verifyPortableArtifactIntegrityWithIdentity(
            alloc,
            shard,
            null,
        );
    }

    pub fn verifyPortableArtifactIntegrityWithIdentity(
        self: *RemoteBackupStore,
        alloc: std.mem.Allocator,
        shard: *const ShardSnapshot,
        identity_hasher: ?*std.crypto.hash.sha2.Sha256,
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
        if (identity_hasher) |identity| {
            hashArtifactBytes(identity, key);
            hashArtifactU64(identity, metadata.content_length);
            hashArtifactBytes(identity, etag);
        }
    }

    const RemoteNativeArtifactScan = struct {
        file_count: u64,
        total_size: u64,
    };

    /// Scan a native artifact in provider key order without retaining its
    /// object set. The first pass obtains the file count required by the
    /// stable v1 tree-hash envelope; the second pass streams payload bytes.
    /// Strict key progress rejects unordered/repeated pages, while the page
    /// ceiling also bounds pathological empty-page token chains.
    pub fn scanNativeArtifact(
        self: *RemoteBackupStore,
        alloc: std.mem.Allocator,
        key_prefix: []const u8,
        hasher: ?*std.crypto.hash.sha2.Sha256,
        identity_hasher: ?*std.crypto.hash.sha2.Sha256,
    ) !RemoteNativeArtifactScan {
        var file_count: u64 = 0;
        var total_size: u64 = 0;
        var continuation_token: ?[]u8 = null;
        defer if (continuation_token) |value| alloc.free(value);
        var previous_page_last_key: ?[]u8 = null;
        defer if (previous_page_last_key) |value| alloc.free(value);
        var page_count: usize = 0;

        while (true) {
            if (page_count == backup_integrity_max_native_list_pages)
                return error.BackupArtifactTooLarge;
            page_count += 1;

            var listed = try self.client.listObjects(self.bucket, .{
                .prefix = key_prefix,
                .recursive = true,
                .max_keys = 1000,
                .continuation_token = continuation_token,
            });
            defer listed.deinit(alloc);

            var previous_key: ?[]const u8 = previous_page_last_key;
            var page_last_key: ?[]const u8 = null;
            for (listed.entries) |entry| {
                if (!std.mem.startsWith(u8, entry.key, key_prefix))
                    return error.InvalidBackupArtifactPath;
                if (previous_key) |value| {
                    if (std.mem.order(u8, value, entry.key) != .lt)
                        return error.InvalidContinuationToken;
                }
                previous_key = entry.key;
                page_last_key = entry.key;

                const relative_path = entry.key[key_prefix.len..];
                if (relative_path.len == 0) continue;
                try validateArtifactRelativePath(relative_path);
                if (file_count == backup_integrity_max_native_files)
                    return error.BackupArtifactTooLarge;
                file_count += 1;
                total_size = std.math.add(u64, total_size, entry.size) catch
                    return error.BackupArtifactTooLarge;

                if (hasher != null or identity_hasher != null) {
                    var owned_etag: ?[]u8 = null;
                    defer if (owned_etag) |value| alloc.free(value);
                    const etag = if (entry.etag) |value|
                        value
                    else blk: {
                        var metadata = try self.client.statObject(self.bucket, entry.key);
                        defer metadata.deinit(alloc);
                        if (metadata.content_length != entry.size)
                            return error.SourceFileChanged;
                        owned_etag = try alloc.dupe(
                            u8,
                            metadata.etag orelse
                                return error.RestoreArtifactIdentityMissing,
                        );
                        break :blk owned_etag.?;
                    };
                    if (identity_hasher) |identity| {
                        hashArtifactBytes(identity, relative_path);
                        hashArtifactU64(identity, entry.size);
                        hashArtifactBytes(identity, etag);
                    }
                    if (hasher) |stream| {
                        hashArtifactBytes(stream, relative_path);
                        hashArtifactU64(stream, entry.size);
                        try self.hashObjectVersion(
                            alloc,
                            entry.key,
                            entry.size,
                            etag,
                            stream,
                        );
                    }
                }
            }

            if (page_last_key) |value| {
                const owned = try alloc.dupe(u8, value);
                if (previous_page_last_key) |previous| alloc.free(previous);
                previous_page_last_key = owned;
            }

            const next = if (listed.next_continuation_token) |value|
                try alloc.dupe(u8, value)
            else
                null;
            errdefer if (next) |value| alloc.free(value);
            if (continuation_token != null and next != null and
                std.mem.eql(u8, continuation_token.?, next.?))
            {
                return error.InvalidContinuationToken;
            }
            if (continuation_token) |value| alloc.free(value);
            continuation_token = next;
            if (continuation_token == null) break;
        }
        return .{ .file_count = file_count, .total_size = total_size };
    }

    pub fn verifyNativeArtifactIntegrity(
        self: *RemoteBackupStore,
        alloc: std.mem.Allocator,
        shard: *const ShardSnapshot,
    ) !void {
        return self.verifyNativeArtifactIntegrityWithIdentity(
            alloc,
            shard,
            null,
        );
    }

    pub fn verifyNativeArtifactIntegrityWithIdentity(
        self: *RemoteBackupStore,
        alloc: std.mem.Allocator,
        shard: *const ShardSnapshot,
        identity_hasher: ?*std.crypto.hash.sha2.Sha256,
    ) !void {
        const key_prefix = try self.keyPrefixAlloc(alloc, shard.snapshot_path);
        defer alloc.free(key_prefix);

        const counted = try self.scanNativeArtifact(alloc, key_prefix, null, null);
        if (counted.file_count == 0) return error.BackupArtifactMissing;
        var hasher = std.crypto.hash.sha2.Sha256.init(.{});
        hasher.update("antfly-native-backup-tree-v1");
        hashArtifactU64(&hasher, counted.file_count);
        const streamed = try self.scanNativeArtifact(
            alloc,
            key_prefix,
            &hasher,
            identity_hasher,
        );
        if (streamed.file_count != counted.file_count)
            return error.SourceFileChanged;
        if (identity_hasher) |identity|
            hashArtifactU64(identity, streamed.file_count);
        hashArtifactU64(&hasher, streamed.total_size);
        var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
        hasher.final(&digest);
        const hex = std.fmt.bytesToHex(digest, .lower);
        if (streamed.total_size != shard.artifact_size_bytes or
            !std.mem.eql(u8, &hex, shard.artifact_sha256))
        {
            return error.BackupArtifactIntegrityMismatch;
        }
    }

    pub fn verifyArtifactIntegrity(
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

    pub fn readFile(self: *RemoteBackupStore, alloc: std.mem.Allocator, destination_io: std.Io, suffix: []const u8, dest_path: []const u8) !void {
        const key = try self.keyAlloc(alloc, suffix);
        defer alloc.free(key);
        try self.client.getFileWithIo(destination_io, self.bucket, key, dest_path, .{});
    }

    pub fn copyFileVerified(
        self: *RemoteBackupStore,
        alloc: std.mem.Allocator,
        destination_io: std.Io,
        suffix: []const u8,
        destination_path: []const u8,
        expected_size: u64,
        expected_sha256: []const u8,
        cancellation: CancellationToken,
    ) !void {
        const key = try self.keyAlloc(alloc, suffix);
        defer alloc.free(key);
        var metadata = try self.client.statObject(self.bucket, key);
        defer metadata.deinit(alloc);
        if (metadata.content_length != expected_size)
            return error.BackupArtifactIntegrityMismatch;
        const etag = metadata.etag orelse return error.RestoreArtifactIdentityMissing;
        if (std.fs.path.dirname(destination_path)) |parent|
            try ensureDirPathWithIo(destination_io, parent);
        errdefer std.Io.Dir.cwd().deleteFile(destination_io, destination_path) catch {};
        var destination = try fs_paths.createFilePortable(destination_io, destination_path, .{ .truncate = true });
        defer destination.close(destination_io);

        const object_cancellation = object_storage.CancellationToken.fromCallback(
            cancellation.ptr,
            cancellation.is_cancelled_fn,
        );
        var hasher = std.crypto.hash.sha2.Sha256.init(.{});
        var offset: u64 = 0;
        while (offset < expected_size) {
            try cancellation.check();
            const wanted = @min(expected_size - offset, backup_integrity_read_chunk_bytes);
            var result = self.client.getObject(self.bucket, key, .{
                .range = .{ .offset = offset, .length = wanted },
                .if_match_etag = etag,
                .skip_metadata_probe = true,
                .max_response_bytes = @intCast(wanted),
                .cancellation = object_cancellation,
            }) catch |err| switch (err) {
                error.PreconditionFailed => return error.SourceFileChanged,
                else => return err,
            };
            defer result.deinit(alloc);
            if (result.body.len != wanted) return error.SourceFileChanged;
            hasher.update(result.body);
            try destination.writePositionalAll(destination_io, result.body, offset);
            offset += wanted;
        }
        var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
        hasher.final(&digest);
        const actual = std.fmt.bytesToHex(digest, .lower);
        if (!std.mem.eql(u8, &actual, expected_sha256))
            return error.BackupArtifactIntegrityMismatch;
        try destination.sync(destination_io);
    }

    pub fn listObjectsPage(
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

    pub fn listTopLevelObjectsPage(
        self: *RemoteBackupStore,
        alloc: std.mem.Allocator,
        start_after: ?[]const u8,
        continuation_token: ?[]const u8,
    ) !object_storage.ListResult {
        return try self.listObjectsPage(alloc, "", false, 1000, start_after, continuation_token);
    }

    pub fn uploadDirectoryRecursive(
        self: *RemoteBackupStore,
        alloc: std.mem.Allocator,
        source_io: std.Io,
        src_path: []const u8,
        dest_suffix: []const u8,
        cancellation: CancellationToken,
    ) !void {
        try cancellation.check();
        try self.ensureBucketWithCancellation(cancellation);

        const io = source_io;

        var src_dir = try std.Io.Dir.cwd().openDir(io, src_path, .{ .iterate = true });
        defer src_dir.close(io);

        var walker = try src_dir.walk(alloc);
        defer walker.deinit();

        while (try walker.next(io)) |entry| {
            try cancellation.check();
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
                .cancellation = objectCancellationToken(cancellation),
            });
            defer result.deinit(alloc);
        }
        try cancellation.check();
    }

    pub fn downloadDirectoryRecursive(self: *RemoteBackupStore, alloc: std.mem.Allocator, src_suffix: []const u8, dest_path: []const u8) !void {
        var io_impl = std.Io.Threaded.init(alloc, .{});
        defer io_impl.deinit();
        return try self.downloadDirectoryRecursiveWithPageSizeAndCancellation(alloc, io_impl.io(), src_suffix, dest_path, 1000, .none);
    }

    pub fn downloadDirectoryRecursiveWithCancellation(
        self: *RemoteBackupStore,
        alloc: std.mem.Allocator,
        destination_io: std.Io,
        src_suffix: []const u8,
        dest_path: []const u8,
        cancellation: CancellationToken,
    ) !void {
        return try self.downloadDirectoryRecursiveWithPageSizeAndCancellation(
            alloc,
            destination_io,
            src_suffix,
            dest_path,
            1000,
            cancellation,
        );
    }

    pub fn downloadDirectoryRecursiveWithPageSize(self: *RemoteBackupStore, alloc: std.mem.Allocator, src_suffix: []const u8, dest_path: []const u8, page_size: u32) !void {
        var io_impl = std.Io.Threaded.init(alloc, .{});
        defer io_impl.deinit();
        return try self.downloadDirectoryRecursiveWithPageSizeAndCancellation(alloc, io_impl.io(), src_suffix, dest_path, page_size, .none);
    }

    pub fn downloadDirectoryRecursiveWithPageSizeAndCancellation(
        self: *RemoteBackupStore,
        alloc: std.mem.Allocator,
        destination_io: std.Io,
        src_suffix: []const u8,
        dest_path: []const u8,
        page_size: u32,
        cancellation: CancellationToken,
    ) !void {
        if (page_size == 0) return error.InvalidPageSize;
        try cancellation.check();
        const object_cancellation = object_storage.CancellationToken.fromCallback(
            cancellation.ptr,
            cancellation.is_cancelled_fn,
        );
        const base_key = try self.keyAlloc(alloc, src_suffix);
        defer alloc.free(base_key);
        const key_prefix = if (base_key.len == 0)
            try alloc.alloc(u8, 0)
        else
            try std.fmt.allocPrint(alloc, "{s}/", .{base_key});
        defer alloc.free(key_prefix);

        // Native prefix transfers do not yet expose semantic cancellation.
        // Preserve that fast path for uncancellable callers; restore jobs use
        // per-object transfers so active provider requests can be interrupted.
        if (object_cancellation == null) {
            if (try self.client.getPrefixWithIo(destination_io, self.bucket, key_prefix, dest_path)) |downloaded| {
                if (downloaded == 0) return error.FileNotFound;
                return;
            }
        }

        var found = false;
        var continuation_token: ?[]u8 = null;
        defer if (continuation_token) |token| alloc.free(token);
        while (true) {
            try cancellation.check();
            var listed = try self.client.listObjects(self.bucket, .{
                .prefix = key_prefix,
                .recursive = true,
                .max_keys = page_size,
                .continuation_token = continuation_token,
                .cancellation = object_cancellation,
            });
            defer listed.deinit(alloc);
            try cancellation.check();
            var next_token = if (listed.next_continuation_token) |token| try alloc.dupe(u8, token) else null;
            errdefer if (next_token) |token| alloc.free(token);

            for (listed.entries) |entry| {
                try cancellation.check();
                if (!std.mem.startsWith(u8, entry.key, key_prefix)) return error.InvalidBackupArtifactPath;
                const rel = entry.key[key_prefix.len..];
                if (rel.len == 0) continue;
                try validateArtifactRelativePath(rel);
                const dest_file = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ dest_path, rel });
                defer alloc.free(dest_file);
                try self.client.getFileWithIo(destination_io, self.bucket, entry.key, dest_file, .{
                    .cancellation = object_cancellation,
                });
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
    network_io: ?std.Io,
    filesystem_io: ?std.Io,
) !object_storage.Gcs.JsonApiConfig {
    var cfg = switch (external.gcs_credentials.source) {
        .default => try object_storage.Gcs.jsonApiClientConfigFromEnvWithAuthoritiesAlloc(
            alloc,
            external.gcs_credentials.scope,
            network_io,
            filesystem_io,
        ),
        .bearer_token => try object_storage.Gcs.jsonApiClientConfigWithBearerTokenAlloc(
            alloc,
            external.gcs_credentials.bearer_token orelse return error.InvalidConnectionCredentials,
            external.project_id,
        ),
        .service_account => blk: {
            var account = if (external.gcs_credentials.service_account_json) |raw|
                try google_auth.parseServiceAccountJsonAlloc(alloc, raw)
            else
                try google_auth.serviceAccountFromFileAllocWithIo(alloc, external.gcs_credentials.credentials_path orelse return error.InvalidConnectionCredentials, filesystem_io);
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
            source.* = try google_auth.CachedTokenSource.initWithAuthorities(
                alloc,
                auth_cfg,
                network_io,
                filesystem_io,
            );
            var value = try object_storage.Gcs.jsonApiClientConfigAlloc(alloc);
            value.auth = .{ .google_token_source = source };
            if (external.project_id orelse account_project_id) |project_id| value.project_id = try alloc.dupe(u8, project_id);
            break :blk value;
        },
    };
    errdefer cfg.deinit(alloc);
    cfg.io = network_io;
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

pub const S3SecretOverrides = struct {
    endpoint: ?[]u8 = null,
    access_key_id: ?[]u8 = null,
    secret_access_key: ?[]u8 = null,
    session_token: ?[]u8 = null,
    region: ?[]u8 = null,

    pub fn deinit(self: *S3SecretOverrides, alloc: std.mem.Allocator) void {
        if (self.endpoint) |value| alloc.free(value);
        if (self.access_key_id) |value| alloc.free(value);
        if (self.secret_access_key) |value| alloc.free(value);
        if (self.session_token) |value| alloc.free(value);
        if (self.region) |value| alloc.free(value);
        self.* = undefined;
    }
};

pub fn loadS3SecretOverrides(alloc: std.mem.Allocator, secret_store: ?*common_secrets.FileStore) !S3SecretOverrides {
    const store = secret_store orelse return .{};
    return .{
        .endpoint = try firstStoredSecretOwned(alloc, store, &.{ "aws.endpoint_url", "AWS_ENDPOINT_URL" }),
        .access_key_id = try firstStoredSecretOwned(alloc, store, &.{ "aws.access_key_id", "AWS_ACCESS_KEY_ID" }),
        .secret_access_key = try firstStoredSecretOwned(alloc, store, &.{ "aws.secret_access_key", "AWS_SECRET_ACCESS_KEY" }),
        .session_token = try firstStoredSecretOwned(alloc, store, &.{ "aws.session_token", "AWS_SESSION_TOKEN" }),
        .region = try firstStoredSecretOwned(alloc, store, &.{ "aws.region", "AWS_REGION" }),
    };
}

pub fn firstStoredSecretOwned(
    alloc: std.mem.Allocator,
    store: *common_secrets.FileStore,
    keys: []const []const u8,
) !?[]u8 {
    for (keys) |key| {
        if (try store.getOwned(alloc, key)) |value| return value;
    }
    return null;
}

pub const ClusterBackupReservationLease = struct {
    attempt_id: []const u8,
    expires_at_unix_ns: u64,
};

pub fn validateBackupLeaseOwner(owner: []const u8) !void {
    if (std.mem.startsWith(u8, owner, backup_cleanup_lease_owner_prefix)) {
        return validateBackupId(owner[backup_cleanup_lease_owner_prefix.len..]);
    }
    // Legacy cleanup owners used the public ID alphabet, so they are validated
    // as ordinary IDs. Special-casing that prefix would reject otherwise-valid
    // caller IDs such as the prefix itself and recreate the namespace bug this
    // compatibility path is intended to remove.
    return validateBackupId(owner);
}

pub fn reservationOwner(body: []const u8) []const u8 {
    const end = std.mem.indexOfScalar(u8, body, '\n') orelse body.len;
    return std.mem.trimEnd(u8, body[0..end], "\r");
}

pub fn parseClusterBackupReservationLease(body: []const u8) !ClusterBackupReservationLease {
    const newline = std.mem.indexOfScalar(u8, body, '\n') orelse
        return error.InvalidBackupRequest;
    const attempt_id = std.mem.trimEnd(u8, body[0..newline], "\r");
    try validateBackupLeaseOwner(attempt_id);
    const expiration_text = std.mem.trim(
        u8,
        body[newline + 1 ..],
        " \t\r\n",
    );
    if (expiration_text.len == 0) return error.InvalidBackupRequest;
    const expires_at_unix_ns = try std.fmt.parseInt(u64, expiration_text, 10);
    if (expires_at_unix_ns == 0) return error.InvalidBackupRequest;
    return .{
        .attempt_id = attempt_id,
        .expires_at_unix_ns = expires_at_unix_ns,
    };
}

pub fn clusterBackupLeaseReclaimable(
    expires_at_unix_ns: u64,
    now_unix_ns: u64,
) bool {
    const reclaim_after_unix_ns =
        expires_at_unix_ns +| backup_attempt_lease_clock_skew_allowance_ns;
    return now_unix_ns >= reclaim_after_unix_ns;
}

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
            return .{ .file = try resolveFilesystemLocationAlloc(alloc, external.root.?, location, options.filesystem_io) };
        }
        return .{ .file = try alloc.dupe(u8, try parseFileLocation(location)) };
    }
    if (std.mem.startsWith(u8, location, "s3://") or std.mem.startsWith(u8, location, "gs://") or std.mem.startsWith(u8, location, "gcs://")) {
        return .{ .remote = try RemoteBackupStore.initRemoteUri(alloc, location, options) };
    }
    return error.UnsupportedBackupLocation;
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

pub fn ensureManifestSize(encoded: []const u8, max_bytes: usize) !void {
    if (encoded.len > max_bytes) return error.BackupManifestTooLarge;
}

pub const validateArtifactRelativePath = @import("backup_contract.zig").validateArtifactRelativePath;

pub fn resolveFilesystemLocationAlloc(alloc: std.mem.Allocator, configured_root: []const u8, location: []const u8, shared_io: ?std.Io) ![]u8 {
    const uri_path = try parseFileLocation(location);
    const relative = std.mem.trimStart(u8, uri_path, "/");
    if (relative.len > 0) try validateArtifactRelativePath(relative);

    var io_impl: ?std.Io.Threaded = if (shared_io == null)
        threaded_io_limits.initService(alloc)
    else
        null;
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
    // Preserve the sentinel in realPathFileAbsoluteAlloc's allocation shape;
    // erasing it to []u8 would free one byte less than was allocated.
    var canonical_ancestor: ?[:0]u8 = null;
    defer if (canonical_ancestor) |value| alloc.free(value);
    while (true) {
        const canonical = std.Io.Dir.realPathFileAbsoluteAlloc(io, ancestor, alloc) catch |err| switch (err) {
            error.FileNotFound, error.NotDir => {
                ancestor = std.fs.path.dirname(ancestor) orelse return error.InvalidBackupLocation;
                continue;
            },
            else => return err,
        };
        if (!pathIsWithin(canonical_root, canonical)) {
            alloc.free(canonical);
            return error.ConnectionPrefixDenied;
        }
        canonical_ancestor = canonical;
        break;
    }

    // Authorization and later no-follow traversal must address the same
    // filesystem identity. `realPath` above may have accepted an in-scope
    // alias such as macOS `/var` -> `/private/var`; returning the lexical
    // candidate would then make the secure component walker reject that alias
    // as `NotDir`. Rebase only the not-yet-existing suffix onto the proven
    // canonical ancestor. Every subsequently created component is still
    // opened with no-follow semantics, so an attacker cannot redirect it.
    const suffix_start = if (ancestor.len == candidate.len)
        candidate.len
    else if (ancestor.len == 1 and ancestor[0] == std.fs.path.sep)
        1
    else
        ancestor.len + 1;
    const suffix = candidate[suffix_start..];
    const resolved = if (suffix.len == 0)
        try alloc.dupe(u8, canonical_ancestor.?)
    else
        try std.fs.path.join(alloc, &.{ canonical_ancestor.?, suffix });
    alloc.free(candidate);
    return resolved;
}

pub fn pathIsWithin(root: []const u8, candidate: []const u8) bool {
    if (std.mem.eql(u8, root, candidate)) return true;
    if (candidate.len <= root.len or !std.mem.startsWith(u8, candidate, root)) return false;

    // Canonical filesystem roots already end in the platform separator (for
    // example `/`). Requiring another separator after that root incorrectly
    // rejects every descendant of a root-scoped administrative connection.
    return root[root.len - 1] == std.fs.path.sep or candidate[root.len] == std.fs.path.sep;
}

pub fn createManifest(
    alloc: std.mem.Allocator,
    backup_id: []const u8,
    format: BackupFormat,
    table: *const metadata_table_manager.TableRecord,
    shards: []const ShardSnapshot,
) !TableBackupManifest {
    var proof_count: usize = 0;
    for (shards) |shard| {
        if (shard.accepted_generation_summary_digest) |digest| {
            if (format != .portable or
                !std.mem.eql(u8, &digest, &try @import("../storage/portable_backup.zig").sourceGenerationAdmissionSummaryDigest(shardAdmissionNamespace(table.table_id, shard), shard.accepted_generation_summary)))
                return error.BackupIntegrityFailure;
            proof_count += 1;
        } else if (shard.accepted_generation_summary.len != 0) return error.BackupIntegrityFailure;
    }
    if (proof_count != 0 and proof_count != shards.len) return error.BackupIntegrityFailure;
    const owned_shards = try alloc.alloc(ShardSnapshot, shards.len);
    var initialized: usize = 0;
    errdefer {
        for (owned_shards[0..initialized]) |shard| shard.deinit(alloc);
        alloc.free(owned_shards);
    }

    for (shards, 0..) |shard, i| {
        owned_shards[i] = .{
            .group_id = shard.group_id,
            .range_id = shard.range_id,
            .doc_identity_shard_id = shard.doc_identity_shard_id,
            .doc_identity_range_id = shard.doc_identity_range_id,
            .split_attempt_epoch = shard.split_attempt_epoch,
            .start_key = try alloc.dupe(u8, shard.start_key),
            .end_key = if (shard.end_key) |value| try alloc.dupe(u8, value) else null,
            .snapshot_path = try alloc.dupe(u8, shard.snapshot_path),
            .artifact_size_bytes = shard.artifact_size_bytes,
            .artifact_sha256 = if (shard.artifact_sha256.len > 0)
                try alloc.dupe(u8, shard.artifact_sha256)
            else
                "",
            .native_manifest_size_bytes = shard.native_manifest_size_bytes,
            .native_manifest_sha256 = if (shard.native_manifest_sha256.len > 0)
                try alloc.dupe(u8, shard.native_manifest_sha256)
            else
                "",
            .accepted_generation_summary_digest = shard.accepted_generation_summary_digest,
            .accepted_generation_summary = try cloneAcceptedGenerationSummary(alloc, shard.accepted_generation_summary),
        };
        initialized += 1;
    }

    const owned_backup_id = try alloc.dupe(u8, backup_id);
    errdefer alloc.free(owned_backup_id);
    const owned_table_name = try alloc.dupe(u8, table.name);
    errdefer alloc.free(owned_table_name);
    const owned_description = try alloc.dupe(u8, table.description);
    errdefer alloc.free(owned_description);
    const owned_schema_json = try alloc.dupe(u8, table.schema_json);
    errdefer alloc.free(owned_schema_json);
    const owned_read_schema_json = try alloc.dupe(u8, table.read_schema_json);
    errdefer alloc.free(owned_read_schema_json);
    const owned_indexes_json = try alloc.dupe(u8, table.indexes_json);
    errdefer alloc.free(owned_indexes_json);
    const owned_replication_sources_json = try alloc.dupe(u8, table.replication_sources_json);
    errdefer alloc.free(owned_replication_sources_json);

    const manifest: TableBackupManifest = .{
        .format = format,
        .backup_id = owned_backup_id,
        .table_name = owned_table_name,
        .table_id = table.table_id,
        .description = owned_description,
        .schema_json = owned_schema_json,
        .read_schema_json = owned_read_schema_json,
        .indexes_json = owned_indexes_json,
        .replication_sources_json = owned_replication_sources_json,
        .shards = owned_shards,
    };
    // Constructors produce only complete, publishable table generations.
    // Shard-artifact helpers may operate on partial sets while work is in
    // progress, but a manifest cannot escape until its ranges cover the whole
    // keyspace and every artifact has a declared identity.
    try validatePublishedTableManifest(alloc, &manifest, backup_id);
    return manifest;
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
    return writeManifestToLocationWithIoAndCancellation(
        alloc,
        io,
        location,
        manifest,
        .none,
    );
}

pub fn writeManifestToLocationWithIoAndCancellation(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    manifest: *const TableBackupManifest,
    cancellation: CancellationToken,
) !void {
    try cancellation.check();
    switch (location.*) {
        .file => |backup_root| {
            try validatePublishedTableManifest(alloc, manifest, manifest.backup_id);
            const path = try metadataPath(alloc, backup_root, manifest.backup_id);
            defer alloc.free(path);
            const encoded = try stringifyJsonAlloc(alloc, manifest.*);
            defer alloc.free(encoded);
            try ensureManifestSize(encoded, max_backup_manifest_bytes);
            try writeFileAbsoluteIfAbsentWithIoAndCancellation(
                alloc,
                io,
                path,
                encoded,
                cancellation,
            );
        },
        .remote => |*store| {
            try validatePublishedTableManifest(alloc, manifest, manifest.backup_id);
            const encoded = try stringifyJsonAlloc(alloc, manifest.*);
            defer alloc.free(encoded);
            try ensureManifestSize(encoded, max_backup_manifest_bytes);
            const suffix = try metadataPath(alloc, "", manifest.backup_id);
            defer alloc.free(suffix);
            try store.writeBytesIfAbsentWithCancellation(
                alloc,
                trimLeftSlash(suffix),
                encoded,
                "application/json",
                cancellation,
            );
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

pub fn parseTableBackupManifest(
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

pub fn parseTableBackupManifestWithArtifactBackupId(
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
    var proof_count: usize = 0;
    for (manifest.shards) |shard| {
        if (shard.accepted_generation_summary_digest) |digest| {
            if (manifest.format != .portable or manifest.table_id == 0 or
                !std.mem.eql(u8, &digest, &try @import("../storage/portable_backup.zig").sourceGenerationAdmissionSummaryDigest(shardAdmissionNamespace(manifest.table_id, shard), shard.accepted_generation_summary)))
                return error.BackupIntegrityFailure;
            proof_count += 1;
        } else if (shard.accepted_generation_summary.len != 0) return error.BackupIntegrityFailure;
    }
    if (proof_count != 0 and proof_count != manifest.shards.len) return error.BackupIntegrityFailure;
    try validateManifestShards(alloc, manifest);
}

pub fn validatePublishedTableManifest(
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

pub fn validateManifestShards(
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
        const has_native_manifest_identity = shard.native_manifest_size_bytes != 0 or
            shard.native_manifest_sha256.len != 0;
        if (has_native_manifest_identity and
            (manifest.format != .native or shard.native_manifest_size_bytes == 0 or
                !isLowerSha256Hex(shard.native_manifest_sha256)))
        {
            return error.InvalidBackupRequest;
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
    metadata_table_manager.sortKeyspaceRanges(ShardSnapshot, ordered);
    metadata_table_manager.validateCompleteKeyspaceRanges(ordered) catch
        return error.InvalidBackupRangeTopology;
}

pub fn isLowerSha256Hex(value: []const u8) bool {
    if (value.len != std.crypto.hash.sha2.Sha256.digest_length * 2) return false;
    for (value) |c| {
        if (!std.ascii.isDigit(c) and !(c >= 'a' and c <= 'f')) return false;
    }
    return true;
}

pub const PortableShard = struct {
    group_id: u64,
    shard_id: []u8,
    start_key: []u8,
    end_key: ?[]u8,
    snapshot_path: []u8,
    artifact_size_bytes: u64,
    artifact_sha256: []u8,

    pub fn deinit(self: PortableShard, alloc: std.mem.Allocator) void {
        alloc.free(self.shard_id);
        alloc.free(self.start_key);
        if (self.end_key) |end| alloc.free(end);
        alloc.free(self.snapshot_path);
        alloc.free(self.artifact_sha256);
    }
};

pub const GoPortableArtifactIntegrity = struct {
    size_bytes: u64,
    sha256: []const u8,
};

pub fn parseGoPortableTableManifest(
    alloc: std.mem.Allocator,
    root: std.json.ObjectMap,
    backup_id: []const u8,
    artifact_backup_id: []const u8,
) !TableBackupManifest {
    const version = switch (root.get("version") orelse return error.InvalidBackupRequest) {
        .integer => |value| value,
        else => return error.InvalidBackupRequest,
    };
    if (version != 2) return error.UnsupportedBackupFormat;
    if (root.count() != 4) return error.InvalidBackupRequest;
    const format = switch (root.get("format") orelse return error.InvalidBackupRequest) {
        .string => |value| value,
        else => return error.InvalidBackupRequest,
    };
    if (!std.mem.eql(u8, format, "portable")) return error.UnsupportedBackupFormat;
    const artifact_values = switch (root.get("artifacts") orelse
        return error.InvalidBackupRequest) {
        .array => |value| value,
        else => return error.InvalidBackupRequest,
    };
    var artifacts = std.StringHashMapUnmanaged(GoPortableArtifactIntegrity).empty;
    defer artifacts.deinit(alloc);
    try artifacts.ensureTotalCapacity(alloc, @intCast(artifact_values.items.len));
    for (artifact_values.items) |artifact_value| {
        const artifact = switch (artifact_value) {
            .object => |value| value,
            else => return error.InvalidBackupRequest,
        };
        if (artifact.count() != 3) return error.InvalidBackupRequest;
        const name = switch (artifact.get("name") orelse return error.InvalidBackupRequest) {
            .string => |value| value,
            else => return error.InvalidBackupRequest,
        };
        try validateArtifactRelativePath(name);
        if (std.mem.indexOfAny(u8, name, "/\\") != null)
            return error.InvalidBackupRequest;
        const size_integer = switch (artifact.get("size_bytes") orelse
            return error.InvalidBackupRequest) {
            .integer => |value| value,
            else => return error.InvalidBackupRequest,
        };
        if (size_integer <= 0) return error.InvalidBackupRequest;
        const size_bytes = std.math.cast(u64, size_integer) orelse
            return error.InvalidBackupRequest;
        const sha256 = switch (artifact.get("sha256") orelse
            return error.InvalidBackupRequest) {
            .string => |value| value,
            else => return error.InvalidBackupRequest,
        };
        if (!isLowerSha256Hex(sha256)) return error.BackupIntegrityMissing;
        const entry = try artifacts.getOrPut(alloc, name);
        if (entry.found_existing) return error.InvalidBackupRequest;
        entry.value_ptr.* = .{
            .size_bytes = size_bytes,
            .sha256 = sha256,
        };
    }
    const table = switch (root.get("table") orelse return error.InvalidBackupRequest) {
        .object => |object| object,
        else => return error.InvalidBackupRequest,
    };
    const table_name = switch (table.get("name") orelse return error.InvalidBackupRequest) {
        .string => |value| value,
        else => return error.InvalidBackupRequest,
    };
    if (table_name.len == 0 or table_name.len > 4096) return error.InvalidBackupRequest;

    var manifest_owns_backing = false;
    const schema_json = try stringifyOptionalGoTableField(alloc, table.get("schema"), "{}");
    errdefer if (!manifest_owns_backing) alloc.free(schema_json);
    const read_schema_json = try stringifyOptionalGoTableField(alloc, table.get("read_schema"), "");
    errdefer if (!manifest_owns_backing) alloc.free(read_schema_json);
    const indexes_json = try normalizeGoPortableIndexesJson(alloc, table.get("indexes"));
    errdefer if (!manifest_owns_backing) alloc.free(indexes_json);
    const replication_sources_json = try stringifyOptionalGoTableField(
        alloc,
        table.get("replication_sources"),
        "[]",
    );
    errdefer if (!manifest_owns_backing) alloc.free(replication_sources_json);
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
        const artifact = artifacts.get(snapshot_path) orelse
            return error.BackupIntegrityMissing;
        _ = artifacts.remove(snapshot_path);
        const artifact_sha256 = try alloc.dupe(u8, artifact.sha256);
        errdefer alloc.free(artifact_sha256);
        const shard_id = try alloc.dupe(u8, entry.key_ptr.*);
        errdefer alloc.free(shard_id);
        try shards_list.append(alloc, .{
            .group_id = group_ids.dataGroupIdFromHash(raw_group_id),
            .shard_id = shard_id,
            .start_key = start_key,
            .end_key = end_key,
            .snapshot_path = snapshot_path,
            .artifact_size_bytes = artifact.size_bytes,
            .artifact_sha256 = artifact_sha256,
        });
    }
    if (artifacts.count() != 0) return error.InvalidBackupRequest;
    std.mem.sort(PortableShard, shards_list.items, {}, portableShardLessThan);

    const shards = try alloc.alloc(ShardSnapshot, shards_list.items.len);
    var initialized: usize = 0;
    errdefer {
        if (!manifest_owns_backing) {
            for (shards[0..initialized]) |shard| shard.deinit(alloc);
            alloc.free(shards);
        }
    }
    for (shards_list.items, 0..) |portable_shard, i| {
        const start_key = try alloc.dupe(u8, portable_shard.start_key);
        errdefer alloc.free(start_key);
        const end_key = if (portable_shard.end_key) |value|
            try alloc.dupe(u8, value)
        else
            null;
        errdefer if (end_key) |value| alloc.free(value);
        const snapshot_path = try alloc.dupe(u8, portable_shard.snapshot_path);
        errdefer alloc.free(snapshot_path);
        const artifact_sha256 = try alloc.dupe(u8, portable_shard.artifact_sha256);
        errdefer alloc.free(artifact_sha256);
        shards[i] = .{
            .group_id = portable_shard.group_id,
            .start_key = start_key,
            .end_key = end_key,
            .snapshot_path = snapshot_path,
            .artifact_size_bytes = portable_shard.artifact_size_bytes,
            .artifact_sha256 = artifact_sha256,
        };
        initialized += 1;
    }

    const identity = identity: {
        const owned_backup_id = try alloc.dupe(u8, backup_id);
        errdefer alloc.free(owned_backup_id);
        const owned_table_name = try alloc.dupe(u8, table_name);
        errdefer alloc.free(owned_table_name);
        const owned_description = try alloc.dupe(u8, description);
        errdefer alloc.free(owned_description);
        break :identity .{
            .backup_id = owned_backup_id,
            .table_name = owned_table_name,
            .description = owned_description,
        };
    };
    var manifest: TableBackupManifest = .{
        .format = .portable,
        .artifact_integrity_mode = .declared,
        .backup_id = identity.backup_id,
        .table_name = identity.table_name,
        .description = identity.description,
        .schema_json = schema_json,
        .read_schema_json = read_schema_json,
        .indexes_json = indexes_json,
        .replication_sources_json = replication_sources_json,
        .shards = shards,
    };
    manifest_owns_backing = true;
    errdefer manifest.deinit(alloc);
    try validateTableManifest(alloc, &manifest, backup_id);
    return manifest;
}

pub fn stringifyOptionalGoTableField(
    alloc: std.mem.Allocator,
    maybe_value: ?std.json.Value,
    absent: []const u8,
) ![]u8 {
    const value = maybe_value orelse return try alloc.dupe(u8, absent);
    if (value == .null) return try alloc.dupe(u8, absent);
    return try stringifyJsonAlloc(alloc, value);
}

pub fn normalizeGoPortableIndexesJson(alloc: std.mem.Allocator, maybe_indexes: ?std.json.Value) ![]u8 {
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

pub fn appendGoPortableIndexConfigJson(
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

pub fn appendGoPortableJsonValue(
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

pub fn appendJsonString(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), value: []const u8) !void {
    const encoded = try stringifyJsonAlloc(alloc, value);
    defer alloc.free(encoded);
    try out.appendSlice(alloc, encoded);
}

pub fn portableShardLessThan(_: void, a: PortableShard, b: PortableShard) bool {
    const start_order = std.mem.order(u8, a.start_key, b.start_key);
    if (start_order != .eq) return start_order == .lt;
    return a.group_id < b.group_id;
}

pub fn decodePortableByteRangeBoundary(alloc: std.mem.Allocator, encoded: []const u8) ![]u8 {
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

pub fn lockFileExclusiveWithCancellation(
    io: std.Io,
    file: std.Io.File,
    cancellation: CancellationToken,
) !void {
    if (cancellation.ptr == null or cancellation.is_cancelled_fn == null)
        return file.lock(io, .exclusive);
    var backoff_ms: i64 = 1;
    while (true) {
        try cancellation.check();
        if (try file.tryLock(io, .exclusive)) {
            cancellation.check() catch |err| {
                file.unlock(io);
                return err;
            };
            return;
        }
        try io.sleep(.fromMilliseconds(backoff_ms), .awake);
        backoff_ms = @min(backoff_ms * 2, 10);
    }
}

pub fn shardSnapshotPath(alloc: std.mem.Allocator, backup_root: []const u8, backup_id: []const u8, group_id: u64) ![]u8 {
    try validateBackupId(backup_id);
    return try std.fmt.allocPrint(alloc, "{s}/{s}/groups/{d}", .{ backup_root, backup_id, group_id });
}

pub fn shardSnapshotRelPath(alloc: std.mem.Allocator, backup_id: []const u8, group_id: u64) ![]u8 {
    try validateBackupId(backup_id);
    return try std.fmt.allocPrint(alloc, "{s}/groups/{d}", .{ backup_id, group_id });
}

pub fn hashArtifactI128(hasher: *std.crypto.hash.sha2.Sha256, value: i128) void {
    var encoded: [@sizeOf(i128)]u8 = undefined;
    std.mem.writeInt(i128, &encoded, value, .little);
    hasher.update(&encoded);
}

pub fn hashLocalArtifactStat(
    hasher: *std.crypto.hash.sha2.Sha256,
    stat: std.Io.File.Stat,
) void {
    hashArtifactU64(hasher, @intCast(stat.inode));
    hashArtifactU64(hasher, stat.size);
    hashArtifactI128(hasher, stat.mtime.toNanoseconds());
    hashArtifactI128(hasher, stat.ctime.toNanoseconds());
}

pub fn localArtifactStatsEqual(
    lhs: std.Io.File.Stat,
    rhs: std.Io.File.Stat,
) bool {
    return lhs.inode == rhs.inode and
        lhs.size == rhs.size and
        std.meta.eql(lhs.mtime, rhs.mtime) and
        std.meta.eql(lhs.ctime, rhs.ctime);
}

pub fn createTableRequestFromManifest(alloc: std.mem.Allocator, manifest: *const TableBackupManifest) !tables_api.CreateTableRequest {
    if (manifest.read_schema_json.len > 0) return error.UnsupportedBackupMigrationState;
    try tables_api.validateStoredIndexesJson(alloc, manifest.indexes_json);
    return .{
        // Supported backup manifests contain primary-owned artifacts. Restore
        // must preserve that authority instead of applying fresh-table policy.
        .storage = .{ .dense_embeddings = .primary_lsm },
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

pub fn copyDirectoryToLocation(
    alloc: std.mem.Allocator,
    location: *BackupLocation,
    backup_id: []const u8,
    group_id: u64,
    src_path: []const u8,
) !void {
    return copyDirectoryToLocationWithCancellation(
        alloc,
        location,
        backup_id,
        group_id,
        src_path,
        .none,
    );
}

pub fn copyDirectoryToLocationWithCancellation(
    alloc: std.mem.Allocator,
    location: *BackupLocation,
    backup_id: []const u8,
    group_id: u64,
    src_path: []const u8,
    cancellation: CancellationToken,
) !void {
    return copyDirectoryToLocationUsingIoWithCancellation(
        alloc,
        null,
        location,
        backup_id,
        group_id,
        src_path,
        cancellation,
    );
}

pub fn copyDirectoryToLocationUsingIoWithCancellation(
    alloc: std.mem.Allocator,
    filesystem_io: ?std.Io,
    location: *BackupLocation,
    backup_id: []const u8,
    group_id: u64,
    src_path: []const u8,
    cancellation: CancellationToken,
) !void {
    // `filesystem_io` owns only the local source tree. Remote object clients
    // retain their configured transport I/O for repository requests.
    try cancellation.check();
    var io_impl: ?std.Io.Threaded = if (filesystem_io == null) std.Io.Threaded.init(alloc, .{}) else null;
    defer if (io_impl) |*owned| owned.deinit();
    const source_io = filesystem_io orelse io_impl.?.io();
    switch (location.*) {
        .file => |backup_root| {
            const dest_root = try shardSnapshotPath(alloc, backup_root, backup_id, group_id);
            defer alloc.free(dest_root);
            try copyDirectoryRecursiveWithIo(
                alloc,
                source_io,
                src_path,
                dest_root,
                .transient,
                cancellation,
            );
        },
        .remote => |*store| {
            const dest_suffix = try shardSnapshotRelPath(alloc, backup_id, group_id);
            defer alloc.free(dest_suffix);
            try store.uploadDirectoryRecursive(alloc, source_io, src_path, dest_suffix, cancellation);
        },
    }
}

pub fn copyDirectoryFromLocationUsingIoWithCancellation(
    alloc: std.mem.Allocator,
    filesystem_io: ?std.Io,
    location: *BackupLocation,
    snapshot_path: []const u8,
    dest_path: []const u8,
    cancellation: CancellationToken,
) !void {
    // `filesystem_io` owns only the local destination tree. Remote object
    // clients retain their configured transport I/O for repository requests.
    try cancellation.check();
    try validateArtifactRelativePath(snapshot_path);
    var io_impl: ?std.Io.Threaded = if (filesystem_io == null) std.Io.Threaded.init(alloc, .{}) else null;
    defer if (io_impl) |*owned| owned.deinit();
    const destination_io = filesystem_io orelse io_impl.?.io();
    switch (location.*) {
        .file => |backup_root| {
            const src_root = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ backup_root, snapshot_path });
            defer alloc.free(src_root);
            try copyDirectoryRecursiveWithIo(alloc, destination_io, src_root, dest_path, .transient, cancellation);
        },
        .remote => |*store| try store.downloadDirectoryRecursiveWithCancellation(
            alloc,
            destination_io,
            snapshot_path,
            dest_path,
            cancellation,
        ),
    }
}

pub fn copyFileFromLocationVerifiedUsingIo(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    relative_path: []const u8,
    destination_path: []const u8,
    expected_size: u64,
    expected_sha256: []const u8,
    cancellation: CancellationToken,
) !void {
    try cancellation.check();
    try validateArtifactRelativePath(relative_path);
    if (!isLowerSha256Hex(expected_sha256)) return error.BackupIntegrityMissing;
    switch (location.*) {
        .file => |backup_root| {
            const source_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ backup_root, relative_path });
            defer alloc.free(source_path);
            var source = if (std.fs.path.isAbsolute(source_path))
                try std.Io.Dir.openFileAbsolute(io, source_path, .{})
            else
                try std.Io.Dir.cwd().openFile(io, source_path, .{});
            const source_stat = source.stat(io) catch |err| {
                source.close(io);
                return err;
            };
            source.close(io);
            if (source_stat.size != expected_size)
                return error.BackupArtifactIntegrityMismatch;
            errdefer std.Io.Dir.cwd().deleteFile(io, destination_path) catch {};
            var hasher = std.crypto.hash.sha2.Sha256.init(.{});
            try copyFileAndHashCancellable(
                io,
                source_path,
                destination_path,
                source_stat,
                &hasher,
                cancellation,
            );
            var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
            hasher.final(&digest);
            const actual = std.fmt.bytesToHex(digest, .lower);
            if (!std.mem.eql(u8, &actual, expected_sha256))
                return error.BackupArtifactIntegrityMismatch;
        },
        .remote => |*store| try store.copyFileVerified(
            alloc,
            io,
            trimLeftSlash(relative_path),
            destination_path,
            expected_size,
            expected_sha256,
            cancellation,
        ),
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

pub fn verifyShardArtifactIntegrityWithCancellation(
    alloc: std.mem.Allocator,
    shared_io: ?std.Io,
    format: BackupFormat,
    artifact_path: []const u8,
    shard: *const ShardSnapshot,
    cancellation: CancellationToken,
) !void {
    if (!isLowerSha256Hex(shard.artifact_sha256)) return error.BackupIntegrityMissing;
    var actual = try artifactIntegrityAllocWithCancellation(
        alloc,
        shared_io,
        format,
        artifact_path,
        cancellation,
    );
    defer actual.deinit(alloc);
    if (actual.size_bytes != shard.artifact_size_bytes or
        !std.mem.eql(u8, actual.sha256, shard.artifact_sha256))
    {
        return error.BackupArtifactIntegrityMismatch;
    }
}

/// Restore-only verification for native generations. A whole-tree mismatch
/// may represent a missing/corrupt generated projection. It is safe to defer
/// that classification to the native validator only when the separately
/// authenticated per-file generation manifest is still exact.
pub fn nativeGenerationManifestIntegrityAllocWithCancellation(
    alloc: std.mem.Allocator,
    shared_io: ?std.Io,
    artifact_path: []const u8,
    cancellation: CancellationToken,
) !ArtifactIntegrity {
    const manifest_path = try std.fmt.allocPrint(alloc, "{s}/native-generation.json", .{artifact_path});
    defer alloc.free(manifest_path);
    try cancellation.check();
    if (shared_io) |io|
        return try fileArtifactIntegrityAllocCancellable(alloc, io, manifest_path, cancellation);
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    return try fileArtifactIntegrityAllocCancellable(alloc, io_impl.io(), manifest_path, cancellation);
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

pub fn artifactIntegrityAllocWithCancellation(
    alloc: std.mem.Allocator,
    shared_io: ?std.Io,
    format: BackupFormat,
    artifact_path: []const u8,
    cancellation: CancellationToken,
) !ArtifactIntegrity {
    try cancellation.check();
    if (shared_io) |io|
        return try artifactIntegrityAllocCancellableWithIo(alloc, io, format, artifact_path, cancellation);
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    return try artifactIntegrityAllocCancellableWithIo(alloc, io_impl.io(), format, artifact_path, cancellation);
}

pub fn artifactIntegrityAllocCancellableWithIo(
    alloc: std.mem.Allocator,
    io: std.Io,
    format: BackupFormat,
    artifact_path: []const u8,
    cancellation: CancellationToken,
) !ArtifactIntegrity {
    return switch (format) {
        .portable => try fileArtifactIntegrityAllocCancellable(alloc, io, artifact_path, cancellation),
        .native => try directoryArtifactIntegrityAllocCancellable(alloc, io, artifact_path, cancellation),
    };
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

pub fn artifactIntegrityAllocWithIo(
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

pub fn fileArtifactIntegrityAlloc(
    alloc: std.mem.Allocator,
    io: std.Io,
    path: []const u8,
) !ArtifactIntegrity {
    return fileArtifactIntegrityAllocWithIdentity(alloc, io, path, null);
}

pub fn fileArtifactIntegrityAllocWithIdentity(
    alloc: std.mem.Allocator,
    io: std.Io,
    path: []const u8,
    identity_hasher: ?*std.crypto.hash.sha2.Sha256,
) !ArtifactIntegrity {
    var file = if (std.fs.path.isAbsolute(path))
        try std.Io.Dir.openFileAbsolute(io, path, .{})
    else
        try std.Io.Dir.cwd().openFile(io, path, .{});
    defer file.close(io);
    const initial_stat = try file.stat(io);

    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    try hashFileContents(io, file, initial_stat, &hasher);
    if (identity_hasher) |identity|
        hashLocalArtifactStat(identity, initial_stat);
    var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    hasher.final(&digest);
    const hex = std.fmt.bytesToHex(digest, .lower);
    return .{
        .size_bytes = initial_stat.size,
        .sha256 = try alloc.dupe(u8, &hex),
    };
}

pub fn fileArtifactIntegrityAllocCancellable(
    alloc: std.mem.Allocator,
    io: std.Io,
    path: []const u8,
    cancellation: CancellationToken,
) !ArtifactIntegrity {
    try cancellation.check();
    var file = if (std.fs.path.isAbsolute(path))
        try std.Io.Dir.openFileAbsolute(io, path, .{})
    else
        try std.Io.Dir.cwd().openFile(io, path, .{});
    defer file.close(io);
    const initial_stat = try file.stat(io);
    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    try hashFileContentsCancellable(io, file, initial_stat, &hasher, cancellation);
    var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    hasher.final(&digest);
    const hex = std.fmt.bytesToHex(digest, .lower);
    return .{
        .size_bytes = initial_stat.size,
        .sha256 = try alloc.dupe(u8, &hex),
    };
}

pub const NativeArtifactFile = struct {
    path: []u8,
    stat: std.Io.File.Stat,

    pub fn deinit(self: NativeArtifactFile, alloc: std.mem.Allocator) void {
        alloc.free(self.path);
    }
};

pub fn directoryArtifactIntegrityAlloc(
    alloc: std.mem.Allocator,
    io: std.Io,
    path: []const u8,
) !ArtifactIntegrity {
    return directoryArtifactIntegrityAllocWithIdentity(
        alloc,
        io,
        path,
        null,
    );
}

pub fn directoryArtifactIntegrityAllocWithIdentity(
    alloc: std.mem.Allocator,
    io: std.Io,
    path: []const u8,
    identity_hasher: ?*std.crypto.hash.sha2.Sha256,
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
                if (files.items.len == backup_integrity_max_native_files)
                    return error.BackupArtifactTooLarge;
                const stat = try dir.statFile(io, entry.path, .{});
                const normalized = try alloc.dupe(u8, entry.path);
                errdefer alloc.free(normalized);
                if (std.fs.path.sep != '/') {
                    for (normalized) |*c| if (c.* == std.fs.path.sep) {
                        c.* = '/';
                    };
                }
                try files.append(alloc, .{ .path = normalized, .stat = stat });
            },
            else => return error.UnsupportedBackupArtifact,
        }
    }
    if (files.items.len == 0) return error.BackupArtifactMissing;
    std.mem.sort(NativeArtifactFile, files.items, {}, nativeArtifactFileLessThan);

    var total_size: u64 = 0;
    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    hasher.update("antfly-native-backup-tree-v1");
    hashArtifactU64(&hasher, @intCast(files.items.len));
    if (identity_hasher) |identity|
        hashArtifactU64(identity, @intCast(files.items.len));
    for (files.items) |entry| {
        total_size = std.math.add(u64, total_size, entry.stat.size) catch
            return error.BackupArtifactTooLarge;
        hashArtifactBytes(&hasher, entry.path);
        hashArtifactU64(&hasher, entry.stat.size);
        if (identity_hasher) |identity| {
            hashArtifactBytes(identity, entry.path);
            hashLocalArtifactStat(identity, entry.stat);
        }

        var file = try dir.openFile(io, entry.path, .{});
        defer file.close(io);
        const initial_stat = try file.stat(io);
        if (!localArtifactStatsEqual(initial_stat, entry.stat))
            return error.SourceFileChanged;
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

pub fn directoryArtifactIntegrityAllocCancellable(
    alloc: std.mem.Allocator,
    io: std.Io,
    path: []const u8,
    cancellation: CancellationToken,
) !ArtifactIntegrity {
    try cancellation.check();
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
        try cancellation.check();
        switch (entry.kind) {
            .directory => {},
            .file => {
                if (files.items.len == backup_integrity_max_native_files)
                    return error.BackupArtifactTooLarge;
                const stat = try dir.statFile(io, entry.path, .{});
                const normalized = try alloc.dupe(u8, entry.path);
                errdefer alloc.free(normalized);
                if (std.fs.path.sep != '/') {
                    for (normalized) |*c| {
                        if (c.* == std.fs.path.sep) c.* = '/';
                    }
                }
                try files.append(alloc, .{ .path = normalized, .stat = stat });
            },
            else => return error.UnsupportedBackupArtifact,
        }
    }
    if (files.items.len == 0) return error.BackupArtifactMissing;
    std.mem.sort(NativeArtifactFile, files.items, {}, nativeArtifactFileLessThan);

    var total_size: u64 = 0;
    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    hasher.update("antfly-native-backup-tree-v1");
    hashArtifactU64(&hasher, @intCast(files.items.len));
    for (files.items) |entry| {
        try cancellation.check();
        total_size = std.math.add(u64, total_size, entry.stat.size) catch
            return error.BackupArtifactTooLarge;
        hashArtifactBytes(&hasher, entry.path);
        hashArtifactU64(&hasher, entry.stat.size);
        var file = try dir.openFile(io, entry.path, .{});
        defer file.close(io);
        const initial_stat = try file.stat(io);
        if (!localArtifactStatsEqual(initial_stat, entry.stat))
            return error.SourceFileChanged;
        try hashFileContentsCancellable(io, file, initial_stat, &hasher, cancellation);
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

pub fn nativeArtifactFileLessThan(_: void, lhs: NativeArtifactFile, rhs: NativeArtifactFile) bool {
    return std.mem.order(u8, lhs.path, rhs.path) == .lt;
}

pub fn hashFileContents(
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
    if (!localArtifactStatsEqual(final_stat, initial_stat))
        return error.SourceFileChanged;
}

pub fn hashFileContentsCancellable(
    io: std.Io,
    file: std.Io.File,
    initial_stat: std.Io.File.Stat,
    hasher: *std.crypto.hash.sha2.Sha256,
    cancellation: CancellationToken,
) !void {
    var buf: [256 * 1024]u8 = undefined;
    var offset: u64 = 0;
    while (offset < initial_stat.size) {
        try cancellation.check();
        const wanted: usize = @intCast(@min(initial_stat.size - offset, buf.len));
        const n = try file.readPositionalAll(io, buf[0..wanted], offset);
        if (n != wanted) return error.SourceFileChanged;
        hasher.update(buf[0..n]);
        offset += n;
    }
    try cancellation.check();
    var extra: [1]u8 = undefined;
    if (try file.readPositionalAll(io, &extra, offset) != 0) return error.SourceFileChanged;
    const final_stat = try file.stat(io);
    if (!localArtifactStatsEqual(final_stat, initial_stat))
        return error.SourceFileChanged;
}

pub fn hashArtifactU64(hasher: *std.crypto.hash.sha2.Sha256, value: u64) void {
    var encoded: [@sizeOf(u64)]u8 = undefined;
    std.mem.writeInt(u64, &encoded, value, .little);
    hasher.update(&encoded);
}

pub fn hashArtifactBytes(hasher: *std.crypto.hash.sha2.Sha256, value: []const u8) void {
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

pub fn cloneTableBackupManifest(alloc: std.mem.Allocator, manifest: TableBackupManifest) !TableBackupManifest {
    const shards = try alloc.alloc(ShardSnapshot, manifest.shards.len);
    var initialized_shards: usize = 0;
    errdefer {
        for (shards[0..initialized_shards]) |shard| shard.deinit(alloc);
        alloc.free(shards);
    }
    for (manifest.shards, 0..) |shard, i| {
        shards[i] = .{
            .group_id = shard.group_id,
            .range_id = shard.range_id,
            .doc_identity_shard_id = shard.doc_identity_shard_id,
            .doc_identity_range_id = shard.doc_identity_range_id,
            .split_attempt_epoch = shard.split_attempt_epoch,
            .start_key = try alloc.dupe(u8, shard.start_key),
            .end_key = if (shard.end_key) |value| try alloc.dupe(u8, value) else null,
            .snapshot_path = try alloc.dupe(u8, shard.snapshot_path),
            .artifact_size_bytes = shard.artifact_size_bytes,
            .artifact_sha256 = if (shard.artifact_sha256.len > 0)
                try alloc.dupe(u8, shard.artifact_sha256)
            else
                "",
            .native_manifest_size_bytes = shard.native_manifest_size_bytes,
            .native_manifest_sha256 = if (shard.native_manifest_sha256.len > 0)
                try alloc.dupe(u8, shard.native_manifest_sha256)
            else
                "",
            .accepted_generation_summary_digest = shard.accepted_generation_summary_digest,
            .accepted_generation_summary = try cloneAcceptedGenerationSummary(alloc, shard.accepted_generation_summary),
        };
        initialized_shards += 1;
    }

    return .{
        .format_version = manifest.format_version,
        .format = manifest.format,
        .artifact_integrity_mode = manifest.artifact_integrity_mode,
        .backup_id = try alloc.dupe(u8, manifest.backup_id),
        .table_name = try alloc.dupe(u8, manifest.table_name),
        .table_id = manifest.table_id,
        .description = try alloc.dupe(u8, manifest.description),
        .schema_json = try alloc.dupe(u8, manifest.schema_json),
        .read_schema_json = try alloc.dupe(u8, manifest.read_schema_json),
        .indexes_json = try alloc.dupe(u8, manifest.indexes_json),
        .replication_sources_json = try alloc.dupe(u8, manifest.replication_sources_json),
        .shards = shards,
    };
}

/// Derive the mutable restore envelope for a destination table without
/// changing the immutable source manifest that authenticated the backup.
/// Native bundle extraction uses this after verifying the sealed AFB2 source;
/// the resulting table manifest is the target-scoped intent consumed by the
/// ordinary restore API.
pub fn deriveRestoreManifestForTargetTable(
    alloc: std.mem.Allocator,
    source: TableBackupManifest,
    target_table_name: []const u8,
) !TableBackupManifest {
    if (target_table_name.len == 0) return error.InvalidBackupRequest;
    var target = try cloneTableBackupManifest(alloc, source);
    errdefer target.deinit(alloc);
    const owned_target_name = try alloc.dupe(u8, target_table_name);
    alloc.free(@constCast(target.table_name));
    target.table_name = owned_target_name;
    return target;
}

pub fn joinPathAlloc(alloc: std.mem.Allocator, left: []const u8, right: []const u8) ![]u8 {
    if (left.len == 0) return try alloc.dupe(u8, trimLeftSlash(right));
    if (right.len == 0) return try alloc.dupe(u8, left);
    return try std.fmt.allocPrint(alloc, "{s}/{s}", .{ trimRightSlash(left), trimLeftSlash(right) });
}

pub fn normalizeRemoteLocationAlloc(alloc: std.mem.Allocator, location: []const u8) ![]u8 {
    if (std.mem.startsWith(u8, location, "gcs://")) {
        return try std.fmt.allocPrint(alloc, "gs://{s}", .{location["gcs://".len..]});
    }
    return try alloc.dupe(u8, location);
}

pub fn trimLeftSlash(value: []const u8) []const u8 {
    var idx: usize = 0;
    while (idx < value.len and value[idx] == '/') : (idx += 1) {}
    return value[idx..];
}

pub fn trimRightSlash(value: []const u8) []const u8 {
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
    return copyDirectoryRecursiveUsingIoWithCancellation(alloc, shared_io, src_path, dest_path, .none);
}

pub fn copyDirectoryRecursiveUsingIoWithCancellation(
    alloc: std.mem.Allocator,
    shared_io: ?std.Io,
    src_path: []const u8,
    dest_path: []const u8,
    cancellation: CancellationToken,
) !void {
    try cancellation.check();
    if (shared_io) |io| return try copyDirectoryRecursiveWithIo(alloc, io, src_path, dest_path, .durable, cancellation);
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    return copyDirectoryRecursiveWithIo(alloc, io_impl.io(), src_path, dest_path, .durable, cancellation);
}

/// Copies a native artifact and computes the canonical tree digest from the
/// exact bytes written. This avoids a second corpus-sized read after local
/// materialization and binds the advertised integrity to the destination.
pub fn copyNativeDirectoryWithIntegrityUsingIo(
    alloc: std.mem.Allocator,
    io: std.Io,
    src_path: []const u8,
    dest_path: []const u8,
    cancellation: CancellationToken,
) !ArtifactIntegrity {
    try cancellation.check();
    try ensureDirPathWithIo(io, dest_path);
    var src_dir = if (std.fs.path.isAbsolute(src_path))
        try std.Io.Dir.openDirAbsolute(io, src_path, .{ .iterate = true })
    else
        try std.Io.Dir.cwd().openDir(io, src_path, .{ .iterate = true });
    defer src_dir.close(io);

    var files = std.ArrayListUnmanaged(NativeArtifactFile).empty;
    defer {
        for (files.items) |entry| entry.deinit(alloc);
        files.deinit(alloc);
    }
    var durable_dirs = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (durable_dirs.items) |path| alloc.free(path);
        durable_dirs.deinit(alloc);
    }
    try durable_dirs.append(alloc, try alloc.dupe(u8, dest_path));

    var walker = try src_dir.walk(alloc);
    defer walker.deinit();
    while (try walker.next(io)) |entry| {
        try cancellation.check();
        switch (entry.kind) {
            .directory => {
                const destination = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ dest_path, entry.path });
                errdefer alloc.free(destination);
                try ensureDirPathWithIo(io, destination);
                try durable_dirs.append(alloc, destination);
            },
            .file => {
                if (files.items.len == backup_integrity_max_native_files)
                    return error.BackupArtifactTooLarge;
                const normalized = try alloc.dupe(u8, entry.path);
                errdefer alloc.free(normalized);
                if (std.fs.path.sep != '/') {
                    for (normalized) |*c| {
                        if (c.* == std.fs.path.sep) c.* = '/';
                    }
                }
                try files.append(alloc, .{
                    .path = normalized,
                    .stat = try src_dir.statFile(io, entry.path, .{}),
                });
            },
            else => return error.UnsupportedBackupArtifact,
        }
    }
    if (files.items.len == 0) return error.BackupArtifactMissing;
    std.mem.sort(NativeArtifactFile, files.items, {}, nativeArtifactFileLessThan);

    var total_size: u64 = 0;
    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    hasher.update("antfly-native-backup-tree-v1");
    hashArtifactU64(&hasher, @intCast(files.items.len));
    for (files.items) |entry| {
        try cancellation.check();
        total_size = std.math.add(u64, total_size, entry.stat.size) catch
            return error.BackupArtifactTooLarge;
        hashArtifactBytes(&hasher, entry.path);
        hashArtifactU64(&hasher, entry.stat.size);
        const source = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ src_path, entry.path });
        defer alloc.free(source);
        const destination = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ dest_path, entry.path });
        defer alloc.free(destination);
        try copyFileAndHashCancellable(io, source, destination, entry.stat, &hasher, cancellation);
    }
    hashArtifactU64(&hasher, total_size);

    std.mem.sort([]u8, durable_dirs.items, {}, struct {
        pub fn lessThan(_: void, lhs: []u8, rhs: []u8) bool {
            if (lhs.len != rhs.len) return lhs.len > rhs.len;
            return std.mem.order(u8, lhs, rhs) == .gt;
        }
    }.lessThan);
    for (durable_dirs.items) |directory| {
        try cancellation.check();
        try fs_paths.syncDirPortable(io, directory);
    }
    try syncPathAncestorsWithIo(io, std.fs.path.dirname(dest_path) orelse ".");
    var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    hasher.final(&digest);
    const hex = std.fmt.bytesToHex(digest, .lower);
    return .{
        .size_bytes = total_size,
        .sha256 = try alloc.dupe(u8, &hex),
    };
}

pub fn copyFileAndHashCancellable(
    io: std.Io,
    source_path: []const u8,
    destination_path: []const u8,
    initial_stat: std.Io.File.Stat,
    hasher: *std.crypto.hash.sha2.Sha256,
    cancellation: CancellationToken,
) !void {
    if (std.fs.path.dirname(destination_path)) |parent| try ensureDirPathWithIo(io, parent);
    var source = if (std.fs.path.isAbsolute(source_path))
        try std.Io.Dir.openFileAbsolute(io, source_path, .{})
    else
        try std.Io.Dir.cwd().openFile(io, source_path, .{});
    defer source.close(io);
    if (!localArtifactStatsEqual(try source.stat(io), initial_stat)) return error.SourceFileChanged;
    var destination = try fs_paths.createFilePortable(io, destination_path, .{ .truncate = true });
    defer destination.close(io);
    var buffer: [256 * 1024]u8 = undefined;
    var offset: u64 = 0;
    while (offset < initial_stat.size) {
        try cancellation.check();
        const len: usize = @intCast(@min(initial_stat.size - offset, buffer.len));
        if (try source.readPositionalAll(io, buffer[0..len], offset) != len)
            return error.SourceFileChanged;
        hasher.update(buffer[0..len]);
        try destination.writePositionalAll(io, buffer[0..len], offset);
        offset += len;
    }
    if (!localArtifactStatsEqual(try source.stat(io), initial_stat)) return error.SourceFileChanged;
    try destination.sync(io);
}

pub const CopyDurability = enum {
    transient,
    durable,
};

pub fn copyDirectoryRecursiveWithIo(
    alloc: std.mem.Allocator,
    io: std.Io,
    src_path: []const u8,
    dest_path: []const u8,
    durability: CopyDurability,
    cancellation: CancellationToken,
) !void {
    try cancellation.check();
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
        try cancellation.check();
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
            .file => try copyFileAbsoluteWithIoOptionsCancellable(io, src_entry_path, dest_entry_path, durability, cancellation),
            else => return error.UnsupportedBackupArtifact,
        }
    }

    if (durability == .transient) return;
    std.mem.sort([]u8, durable_dirs.items, {}, struct {
        pub fn lessThan(_: void, lhs: []u8, rhs: []u8) bool {
            if (lhs.len != rhs.len) return lhs.len > rhs.len;
            return std.mem.order(u8, lhs, rhs) == .gt;
        }
    }.lessThan);
    for (durable_dirs.items) |dir_path| {
        try cancellation.check();
        try fs_paths.syncDirPortable(io, dir_path);
    }
    try syncPathAncestorsWithIo(io, std.fs.path.dirname(dest_path) orelse ".");
}

pub fn writeFileAbsolute(path: []const u8, data: []const u8) !void {
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

pub fn writeFileAbsoluteIfAbsent(alloc: std.mem.Allocator, path: []const u8, data: []const u8) !void {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    return writeFileAbsoluteIfAbsentWithIo(alloc, io_impl.io(), path, data);
}

pub fn writeFileAbsoluteIfAbsentWithIo(
    alloc: std.mem.Allocator,
    io: std.Io,
    path: []const u8,
    data: []const u8,
) !void {
    return writeFileAbsoluteIfAbsentWithIoAndCancellation(alloc, io, path, data, .none);
}

pub fn writeFileAbsoluteIfAbsentWithIoAndCancellation(
    alloc: std.mem.Allocator,
    io: std.Io,
    path: []const u8,
    data: []const u8,
    cancellation: CancellationToken,
) !void {
    try cancellation.check();
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
    try lockFileExclusiveWithCancellation(io, lock_file, cancellation);
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
    const cancellation_chunk_bytes = 1024 * 1024;
    var offset: usize = 0;
    while (offset < data.len) {
        try cancellation.check();
        const chunk_len = @min(cancellation_chunk_bytes, data.len - offset);
        try writer.interface.writeAll(data[offset..][0..chunk_len]);
        offset += chunk_len;
    }
    try writer.end();
    try cancellation.check();
    try file.sync(io);
    file.close(io);
    file_open = false;
    try cancellation.check();
    if (std.fs.path.isAbsolute(path))
        try std.Io.Dir.renameAbsolute(tmp_path, path, io)
    else
        try std.Io.Dir.rename(std.Io.Dir.cwd(), tmp_path, std.Io.Dir.cwd(), path, io);
    try syncPathAncestorsWithIo(io, std.fs.path.dirname(path) orelse ".");
}

pub fn readFileAbsoluteAlloc(alloc: std.mem.Allocator, path: []const u8, max_bytes: usize) ![]u8 {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    return readFileAbsoluteAllocWithIo(alloc, io_impl.io(), path, max_bytes);
}

pub fn readFileAbsoluteAllocWithIo(
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

pub fn copyFileAbsoluteWithIoOptions(
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

pub fn copyFileAbsoluteWithIoOptionsCancellable(
    io: std.Io,
    src_path: []const u8,
    dest_path: []const u8,
    durability: CopyDurability,
    cancellation: CancellationToken,
) !void {
    try cancellation.check();
    if (cancellation.ptr == null or cancellation.is_cancelled_fn == null)
        return copyFileAbsoluteWithIoOptions(io, src_path, dest_path, durability);
    if (std.fs.path.dirname(dest_path)) |dir_name| try ensureDirPathWithIo(io, dir_name);

    var src = if (std.fs.path.isAbsolute(src_path))
        try std.Io.Dir.openFileAbsolute(io, src_path, .{})
    else
        try std.Io.Dir.cwd().openFile(io, src_path, .{});
    defer src.close(io);
    const src_stat = try src.stat(io);

    var dest = try fs_paths.createFilePortable(io, dest_path, .{ .truncate = true });
    defer dest.close(io);

    var buffer: [256 * 1024]u8 = undefined;
    var offset: u64 = 0;
    while (offset < src_stat.size) {
        try cancellation.check();
        const wanted: usize = @intCast(@min(src_stat.size - offset, buffer.len));
        const read = try src.readPositionalAll(io, buffer[0..wanted], offset);
        if (read != wanted) return error.SourceFileChanged;
        try dest.writePositionalAll(io, buffer[0..read], offset);
        offset += read;
    }
    const final_src_stat = try src.stat(io);
    if (final_src_stat.size != src_stat.size or !std.meta.eql(final_src_stat.mtime, src_stat.mtime))
        return error.SourceFileChanged;
    if (durability == .durable) try dest.sync(io);
}

pub fn ensureDirPath(path: []const u8) !void {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    return ensureDirPathWithIo(io_impl.io(), path);
}

pub fn ensureDirPathWithIo(io: std.Io, path: []const u8) !void {
    try fs_paths.createDirPathPortable(io, path);
}

pub fn syncPathAncestorsWithIo(io: std.Io, start_path: []const u8) !void {
    var current = start_path;
    while (true) {
        try fs_paths.syncDirPortable(io, if (current.len == 0) "." else current);
        const parent = std.fs.path.dirname(current) orelse break;
        if (std.mem.eql(u8, parent, current)) break;
        current = parent;
    }
}

pub fn stringifyJsonAlloc(alloc: std.mem.Allocator, value: anytype) ![]u8 {
    return try std.json.Stringify.valueAlloc(alloc, value, .{ .emit_null_optional_fields = false });
}

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

pub fn canonicalObjectStoreLocationAlloc(
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

pub fn validateRestoreManifest(
    alloc: std.mem.Allocator,
    manifest: *const TableBackupManifest,
    requested_backup_id: []const u8,
) !void {
    return try validateTableManifest(alloc, manifest, requested_backup_id);
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

pub fn copyFileFromLocationUsingIo(
    alloc: std.mem.Allocator,
    filesystem_io: ?std.Io,
    location: *BackupLocation,
    snapshot_path: []const u8,
    dest_path: []const u8,
) !void {
    // The explicit I/O authority is for the local destination, not transport.
    try validateArtifactRelativePath(snapshot_path);
    var io_impl: ?std.Io.Threaded = if (filesystem_io == null) std.Io.Threaded.init(alloc, .{}) else null;
    defer if (io_impl) |*owned| owned.deinit();
    const destination_io = filesystem_io orelse io_impl.?.io();
    switch (location.*) {
        .file => |backup_root| {
            const src_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ backup_root, snapshot_path });
            defer alloc.free(src_path);
            try copyFileAbsoluteWithIoOptions(destination_io, src_path, dest_path, .transient);
        },
        .remote => |*store| {
            try store.readFile(alloc, destination_io, trimLeftSlash(snapshot_path), dest_path);
        },
    }
}

/// Reads one authenticated control file without enumerating or buffering the
/// artifact generation that contains it. Restore uses this for the native
/// generation manifest before admitting any corpus-sized bytes.
pub fn readFileFromLocationUsingIoLimited(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    relative_path: []const u8,
    max_bytes: usize,
) ![]u8 {
    try validateArtifactRelativePath(relative_path);
    return switch (location.*) {
        .file => |backup_root| blk: {
            const source_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ backup_root, relative_path });
            defer alloc.free(source_path);
            if (max_bytes == std.math.maxInt(usize)) return error.InvalidBackupManifestLimit;
            var source = if (std.fs.path.isAbsolute(source_path))
                try std.Io.Dir.openFileAbsolute(io, source_path, .{})
            else
                try std.Io.Dir.cwd().openFile(io, source_path, .{});
            defer source.close(io);
            const stat = try source.stat(io);
            if (stat.size > max_bytes) return error.BackupManifestTooLarge;
            break :blk try readFileAbsoluteAllocWithIo(alloc, io, source_path, max_bytes + 1);
        },
        .remote => |*store| try store.readBytesAllocLimited(
            alloc,
            trimLeftSlash(relative_path),
            max_bytes,
        ),
    };
}

/// Copies exactly one manifest-declared artifact directly into an unpublished
/// generation. Size and digest are checked against the bytes written, and a
/// remote object's immutable identity is pinned across bounded range reads.
/// A bounded transfer primitive shared by restartable materializers. The
/// caller owns the returned bytes and pins the final file digest from the
/// authenticated manifest; provider ETags additionally fence each range read.
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

pub fn verifyRestorableShardArtifactIntegrityWithCancellation(
    alloc: std.mem.Allocator,
    shared_io: ?std.Io,
    format: BackupFormat,
    artifact_path: []const u8,
    shard: *const ShardSnapshot,
    cancellation: CancellationToken,
) !void {
    verifyShardArtifactIntegrityWithCancellation(
        alloc,
        shared_io,
        format,
        artifact_path,
        shard,
        cancellation,
    ) catch |err| switch (err) {
        error.BackupArtifactIntegrityMismatch => {
            if (format != .native or shard.native_manifest_size_bytes == 0 or
                !isLowerSha256Hex(shard.native_manifest_sha256))
            {
                return err;
            }
            var manifest_integrity = try nativeGenerationManifestIntegrityAllocWithCancellation(
                alloc,
                shared_io,
                artifact_path,
                cancellation,
            );
            defer manifest_integrity.deinit(alloc);
            if (manifest_integrity.size_bytes != shard.native_manifest_size_bytes or
                !std.mem.eql(u8, manifest_integrity.sha256, shard.native_manifest_sha256))
            {
                return error.BackupArtifactIntegrityMismatch;
            }
            std.log.warn("native backup tree integrity differs; authenticated generation manifest will classify projection repair", .{});
        },
        else => return err,
    };
}

pub fn validateRestorableManifestLayout(manifest: *const TableBackupManifest) !void {
    if (manifest.shards.len == 0) return error.UnsupportedBackupFormat;
}

pub fn validateSingleRangeRestoreManifestLayout(manifest: *const TableBackupManifest) !void {
    try validateRestorableManifestLayout(manifest);
    if (manifest.shards.len != 1) return error.UnsupportedMultiRangeTable;
}

pub fn cloneAcceptedGenerationSummary(alloc: std.mem.Allocator, entries: []const TableBackupManifest.AcceptedGeneration) ![]const TableBackupManifest.AcceptedGeneration {
    if (entries.len == 0) return &.{};
    const owned = try alloc.alloc(TableBackupManifest.AcceptedGeneration, entries.len);
    var initialized: usize = 0;
    errdefer {
        for (owned[0..initialized]) |entry| {
            alloc.free(entry.child_table_name);
            alloc.free(entry.constraint_name);
        }
        alloc.free(owned);
    }
    for (entries, owned) |entry, *target| {
        const child_name = try alloc.dupe(u8, entry.child_table_name);
        errdefer alloc.free(child_name);
        target.* = .{
            .child_table_id = entry.child_table_id,
            .child_table_name = child_name,
            .constraint_name = try alloc.dupe(u8, entry.constraint_name),
            .active_generation = entry.active_generation,
            .source_scope_digest = entry.source_scope_digest,
        };
        initialized += 1;
    }
    return owned;
}

pub fn shardAdmissionNamespace(table_id: u64, shard: ShardSnapshot) @import("../storage/db/doc_identity_namespace.zig").Namespace {
    return .{
        .table_id = table_id,
        .shard_id = if (shard.doc_identity_shard_id != 0) shard.doc_identity_shard_id else shard.group_id,
        .range_id = if (shard.doc_identity_range_id != 0) shard.doc_identity_range_id else if (shard.range_id != 0) shard.range_id else shard.group_id,
    };
}
