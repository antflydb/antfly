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
const ant_json = @import("antfly-json");
const platform_time = @import("antfly_platform").time;
const metadata_openapi = @import("antfly_metadata_openapi");
const fs_paths = @import("../common/fs_paths.zig");
const group_ids = @import("../common/group_ids.zig");
const threaded_io_limits = @import("../common/threaded_io_limits.zig");
const metadata_api = @import("../metadata/api.zig");
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
const backup_contract = @import("backup_contract.zig");
const backup_repository = @import("../storage/backup_repository.zig");
const CancellationToken = @import("../common/cancellation.zig").CancellationToken;

fn objectCancellationToken(cancellation: CancellationToken) ?object_storage.CancellationToken {
    return object_storage.CancellationToken.fromCallback(
        cancellation.ptr,
        cancellation.is_cancelled_fn,
    );
}

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

pub const format_version = backup_contract.format_version;
pub const cluster_format_version: u32 = 2;
pub const table_backup_id = "table";
pub const antfly_version = "zig-dev";
pub const max_portable_backup_file_bytes: usize = 1024 * 1024 * 1024;
pub const max_backup_manifest_bytes: usize = 16 * 1024 * 1024;
pub const max_backup_attempt_marker_bytes: usize = 2 * 1024 * 1024;
pub const max_backup_attempt_cursor_bytes: usize = 8 * 1024;
const max_backup_attempt_quarantine_bytes: usize = max_backup_attempt_marker_bytes + 2048;
const max_backup_attempt_lease_bytes: usize = 256;
// `:` is intentionally outside validateBackupId's public alphabet. Cleanup
// owners therefore cannot be confused with a caller-supplied attempt ID, while
// the suffix can still carry the full 128-byte public identity losslessly.
const backup_cleanup_lease_owner_prefix = "antfly-cleanup:";
const legacy_backup_cleanup_lease_owner_prefix = "antfly-cleanup-";
const backup_attempt_quarantine_prefix = ".antfly-quarantine";
const cluster_backup_attempt_quarantine_suffix = ".quarantine";
const cluster_backup_attempt_head_transition_operation_cost: usize = 2;
const cluster_backup_attempt_quarantine_operation_cost: usize = 4;
pub const max_cluster_backup_attempt_tables: usize = 4096;
pub const backup_attempt_marker_version: u32 = 1;
pub const backup_attempt_head_version: u32 = 3;
pub const backup_attempt_reclaim_age_ns: u64 = 24 * std.time.ns_per_hour;
pub const backup_attempt_reclaim_batch_size: usize = 2;
pub const backup_attempt_reclaim_scan_budget: usize = 64;
const backup_attempt_reclaim_shard_count: u8 = 64;
const backup_attempt_reclaim_claim_timeout_ns: u64 = std.time.ns_per_hour;
const max_backup_attempt_reclaim_ticket_bytes: usize = 64;
const backup_attempt_staging_orphan_age_ns: u64 =
    backup_attempt_reclaim_claim_timeout_ns +
    backup_attempt_lease_clock_skew_allowance_ns;
const backup_attempt_staging_scan_budget: usize = 16;
/// Upgrade old quarantine evidence incrementally. Each record may be slightly
/// larger than 2 MiB, so keeping this batch small bounds both peak memory and
/// object-store work on the shared maintenance lane.
const backup_attempt_legacy_quarantine_migration_batch_size: usize = 2;
/// Maximum remote storage operations a single opportunistic maintenance job
/// may spend on attempt fencing and artifact deletion. Keeping this quantum
/// small prevents cleanup from monopolizing the shared background I/O lane; an
/// incomplete marker and monotonic cursor remain durable for the next pass.
pub const backup_attempt_reclaim_object_budget: usize = 256;
/// A foreground conflict may advance stale-attempt cleanup, but it must not
/// turn one API request into an unbounded object-store or filesystem sweep.
pub const backup_attempt_request_reclaim_object_budget: usize = 32;
/// Bounds read/open work that cannot make durable progress until a leaf is
/// reached. Antfly-produced trees are shallow; exceeding this limit indicates
/// a corrupt or adversarial repository and fails closed without recursion.
const backup_cleanup_local_traversal_budget: usize = 512;
pub const backup_attempt_cleanup_object_budget: usize = 1_000_000;
pub const backup_attempt_lease_duration_ns: u64 = 5 * std.time.ns_per_min;
pub const backup_attempt_lease_renew_interval_ns: u64 = std.time.ns_per_min;
/// Current storage owners renew this lease for as long as artifact production
/// remains live. The initial 24-hour term preserves the pre-lease grace window
/// when forwarding to a rolling-upgrade peer that does not yet renew it.
pub const table_backup_writer_lease_duration_ns: u64 = backup_attempt_reclaim_age_ns;
/// Lease timestamps are produced by the backup coordinator but may be examined
/// by a different node. Reclamation therefore waits beyond the advertised
/// expiry by this bounded skew envelope. Deployments must keep host clocks
/// within this two-renewal-period tolerance.
pub const backup_attempt_lease_clock_skew_allowance_ns: u64 =
    2 * backup_attempt_lease_renew_interval_ns;
pub const backup_integrity_read_chunk_bytes: u64 = 8 * 1024 * 1024;
pub const backup_integrity_max_native_files: usize = 1_000_000;
pub const backup_integrity_max_native_list_pages: usize =
    (backup_integrity_max_native_files + 999) / 1000 + 1;
const artifact_verification_cache_max_entries: usize = 65_536;
const incomplete_backup_prefix = ".antfly-incomplete";
const backup_attempt_head_name = ".antfly-backup-attempt-head.json";
const current_go_backup_attempt_head_name = ".antfly-go-backup-attempt-head.json";
const backup_attempt_reclaim_cursor_name = ".antfly-backup-reclaim-cursor";
const cluster_writer_lease_scan_cursor_suffix = ".writer-lease-scan";
const max_cluster_writer_lease_scan_cursor_bytes: usize = 512;
const backup_attempt_reclaim_index_name = ".antfly-backup-reclaim-index-v1";
const table_backup_writer_lease_suffix = "-writer-lease";
const current_go_backup_attempt_version: u32 = 1;
const current_go_backup_attempt_head_version: u32 = 1;
const current_go_backup_attempt_head_max_bytes: usize = 8 * 1024;
const current_go_backup_attempt_max_artifacts: usize = 1_000_000;
const backup_list_max_pages: usize = 10_000;
const table_backup_attempt_reservation_version: u32 = 1;
const max_table_backup_attempt_reservation_bytes: usize = 4 * 1024;
pub const manifest_too_large_message = "backup manifest exceeds 16 MiB limit";
pub const metadata_capability_unavailable_body =
    "{\"code\":\"metadata_capability_unavailable\",\"error\":\"metadata capability unavailable\",\"message\":\"backup requires metadata capability linearizable_snapshot; upgrade metadata nodes before retrying\",\"required_capability\":\"linearizable_snapshot\",\"retryable\":true,\"retry_after_ms\":5000}";
pub const metadata_capability_retry_after_seconds: u32 = 5;
pub const integrity_failure_message = "backup artifact failed integrity verification";
pub const backup_already_exists_body =
    "{\"code\":\"backup_already_exists\",\"error\":\"backup id already exists\",\"message\":\"backup id already exists\",\"retryable\":false}";
pub const catalog_changed_body =
    "{\"code\":\"table_catalog_changed\",\"error\":\"table changed during backup admission\",\"message\":\"table changed during backup admission\",\"retryable\":false}";
pub fn encodeBackupOutcomeAmbiguousBody(
    alloc: std.mem.Allocator,
    backup_id: []const u8,
    artifact_backup_id: ?[]const u8,
) ![]u8 {
    return stringifyJsonAlloc(alloc, .{
        .code = "backup_outcome_ambiguous",
        .@"error" = "backup outcome is ambiguous; inspect the backup id before retrying",
        .message = "backup outcome is ambiguous; inspect the backup id and artifact id before retrying",
        .retryable = false,
        .backup_id = backup_id,
        .artifact_backup_id = artifact_backup_id,
    });
}

/// Request-scoped receipts for artifacts already verified byte-for-byte.
///
/// Entries bind the declared manifest identity to a freshly probed storage
/// identity. A cache hit therefore avoids a second payload stream while still
/// detecting replacement or mutation between repository-health admission and
/// materialization. The hard ceiling prevents an adversarial manifest from
/// turning one restore request into unbounded resident state.
pub const ArtifactVerificationCache = struct {
    entries: std.AutoHashMapUnmanaged(
        [std.crypto.hash.sha2.Sha256.digest_length]u8,
        [std.crypto.hash.sha2.Sha256.digest_length]u8,
    ) = .empty,

    pub fn deinit(self: *ArtifactVerificationCache, alloc: std.mem.Allocator) void {
        self.entries.deinit(alloc);
        self.* = .{};
    }

    fn receipt(
        self: *const ArtifactVerificationCache,
        key: [std.crypto.hash.sha2.Sha256.digest_length]u8,
    ) ?[std.crypto.hash.sha2.Sha256.digest_length]u8 {
        return self.entries.get(key);
    }

    fn record(
        self: *ArtifactVerificationCache,
        alloc: std.mem.Allocator,
        key: [std.crypto.hash.sha2.Sha256.digest_length]u8,
        identity: [std.crypto.hash.sha2.Sha256.digest_length]u8,
    ) !void {
        if (!self.entries.contains(key) and
            self.entries.count() >= artifact_verification_cache_max_entries)
        {
            return;
        }
        try self.entries.put(alloc, key, identity);
    }
};

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
pub const TableBackupFence = backup_contract.TableBackupFence;
pub const BackupOperationControl = backup_contract.BackupOperationControl;
pub const TableRestorePlan = backup_contract.TableRestorePlan;

pub const backup_fence_metadata_group_id_header = backup_contract.backup_fence_metadata_group_id_header;
pub const backup_fence_metadata_incarnation_header = backup_contract.backup_fence_metadata_incarnation_header;
pub const backup_fence_table_id_header = backup_contract.backup_fence_table_id_header;
pub const backup_fence_definition_header = backup_contract.backup_fence_definition_header;
pub const backup_fence_topology_count_header = backup_contract.backup_fence_topology_count_header;
pub const backup_fence_topology_header = backup_contract.backup_fence_topology_header;
pub const backup_writer_not_after_header = backup_contract.backup_writer_not_after_header;
pub const backup_remaining_ms_header = backup_contract.backup_remaining_ms_header;
pub const max_backup_server_budget_ms = backup_contract.max_backup_server_budget_ms;
pub const backup_outcome_header = backup_contract.backup_outcome_header;
pub const backup_outcome_stopped_v1 = backup_contract.backup_outcome_stopped_v1;
pub const catalog_changed_message = backup_contract.catalog_changed_message;
pub const backup_outcome_ambiguous_message = backup_contract.backup_outcome_ambiguous_message;

pub fn parseTableBackupFenceHeaderValues(
    metadata_group_id_value: ?[]const u8,
    metadata_incarnation_value: ?[]const u8,
    table_id_value: ?[]const u8,
    definition_value: ?[]const u8,
    topology_count_value: ?[]const u8,
    topology_value: ?[]const u8,
) !?TableBackupFence {
    return parseTableBackupFenceHeaderValuesWithDeadline(
        metadata_group_id_value,
        metadata_incarnation_value,
        table_id_value,
        definition_value,
        topology_count_value,
        topology_value,
        null,
    );
}

pub fn parseTableBackupFenceHeaderValuesWithDeadline(
    metadata_group_id_value: ?[]const u8,
    metadata_incarnation_value: ?[]const u8,
    table_id_value: ?[]const u8,
    definition_value: ?[]const u8,
    topology_count_value: ?[]const u8,
    topology_value: ?[]const u8,
    writer_not_after_value: ?[]const u8,
) !?TableBackupFence {
    var metadata_present_count: u8 = 0;
    if (metadata_group_id_value != null) metadata_present_count += 1;
    if (metadata_incarnation_value != null) metadata_present_count += 1;
    var table_present_count: u8 = 0;
    if (table_id_value != null) table_present_count += 1;
    if (definition_value != null) table_present_count += 1;
    if (topology_count_value != null) table_present_count += 1;
    if (topology_value != null) table_present_count += 1;
    if (metadata_present_count == 0 and table_present_count == 0) {
        if (writer_not_after_value != null) return error.InvalidBackupFence;
        return null;
    }
    // Four-field fences were emitted by the immediately preceding rolling
    // version. Accept them without metadata identity so old coordinators can
    // still reach upgraded owners; every newly emitted fence uses all six.
    if ((metadata_present_count != 0 and metadata_present_count != 2) or table_present_count != 4)
        return error.InvalidBackupFence;
    if (writer_not_after_value != null and metadata_present_count != 2)
        return error.InvalidBackupFence;

    if (definition_value.?.len != std.crypto.hash.sha2.Sha256.digest_length * 2 or
        topology_value.?.len != std.crypto.hash.sha2.Sha256.digest_length * 2)
        return error.InvalidBackupFence;
    if (metadata_incarnation_value) |incarnation| {
        if (incarnation.len != 32) return error.InvalidBackupFence;
        for (incarnation) |char| {
            if (!std.ascii.isDigit(char) and !(char >= 'a' and char <= 'f')) return error.InvalidBackupFence;
        }
    }
    var definition_digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    var topology_digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    _ = std.fmt.hexToBytes(&definition_digest, definition_value.?) catch return error.InvalidBackupFence;
    _ = std.fmt.hexToBytes(&topology_digest, topology_value.?) catch return error.InvalidBackupFence;
    const fence: TableBackupFence = .{
        .metadata_group_id = if (metadata_group_id_value) |value|
            std.fmt.parseInt(u64, value, 10) catch return error.InvalidBackupFence
        else
            0,
        .metadata_incarnation = if (metadata_incarnation_value) |value| value[0..32].* else [_]u8{0} ** 32,
        .table_id = std.fmt.parseInt(u64, table_id_value.?, 10) catch return error.InvalidBackupFence,
        .definition_digest = definition_digest,
        .topology_range_count = std.fmt.parseInt(u64, topology_count_value.?, 10) catch return error.InvalidBackupFence,
        .topology_digest = topology_digest,
        .writer_not_after_unix_ns = if (writer_not_after_value) |value| blk: {
            const parsed = std.fmt.parseInt(u64, value, 10) catch return error.InvalidBackupFence;
            if (parsed == 0) return error.InvalidBackupFence;
            break :blk parsed;
        } else null,
    };
    if (writer_not_after_value != null and !fence.hasMetadataIdentity())
        return error.InvalidBackupFence;
    return fence;
}

test "backup fence headers require a complete canonical fence" {
    const digest = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
    const incarnation = "0123456789abcdef0123456789abcdef";
    const parsed = (try parseTableBackupFenceHeaderValues("3", incarnation, "7", digest, "1", digest)).?;
    try std.testing.expectEqual(@as(u64, 3), parsed.metadata_group_id);
    try std.testing.expectEqualStrings(incarnation, &parsed.metadata_incarnation);
    try std.testing.expectEqual(@as(u64, 7), parsed.table_id);
    try std.testing.expectEqual(@as(u64, 1), parsed.topology_range_count);
    const legacy = (try parseTableBackupFenceHeaderValues(null, null, "7", digest, "1", digest)).?;
    try std.testing.expect(!legacy.hasMetadataIdentity());
    try std.testing.expect(legacy.matches(parsed));
    try std.testing.expect(!parsed.matches(legacy));
    try std.testing.expectError(error.InvalidBackupFence, parseTableBackupFenceHeaderValues("3", incarnation, "7", null, "1", digest));
    try std.testing.expectError(error.InvalidBackupFence, parseTableBackupFenceHeaderValues("3", null, "7", digest, "1", digest));
    try std.testing.expectError(error.InvalidBackupFence, parseTableBackupFenceHeaderValues("3", incarnation, "7", "00", "1", digest));
    try std.testing.expectError(error.InvalidBackupFence, parseTableBackupFenceHeaderValues("3", "0123456789ABCDEF0123456789ABCDEF", "7", digest, "1", digest));
    const deadline_fence = (try parseTableBackupFenceHeaderValuesWithDeadline(
        "3",
        incarnation,
        "7",
        digest,
        "1",
        digest,
        "123",
    )).?;
    try std.testing.expectEqual(@as(?u64, 123), deadline_fence.writer_not_after_unix_ns);
    try std.testing.expectError(
        error.InvalidBackupFence,
        parseTableBackupFenceHeaderValuesWithDeadline(null, null, null, null, null, null, "123"),
    );
    try std.testing.expectError(
        error.InvalidBackupFence,
        parseTableBackupFenceHeaderValuesWithDeadline(null, null, "7", digest, "1", digest, "123"),
    );
    try std.testing.expectError(
        error.InvalidBackupFence,
        parseTableBackupFenceHeaderValuesWithDeadline("0", incarnation, "7", digest, "1", digest, "123"),
    );
}

pub fn tableBackupFence(
    snapshot: *const metadata_api.AdminSnapshot,
    table: *const metadata_table_manager.TableRecord,
) TableBackupFence {
    const topology = metadata_api.catalogTableTopology(table.table_id, snapshot.ranges);
    return tableBackupFenceWithTopology(snapshot, table, topology);
}

pub fn tableBackupFenceWithTopology(
    snapshot: *const metadata_api.AdminSnapshot,
    table: *const metadata_table_manager.TableRecord,
    topology: metadata_api.CatalogTableTopology,
) TableBackupFence {
    return .{
        .metadata_group_id = snapshot.status.metadata_group_id,
        .metadata_incarnation = snapshot.status.metadata_incarnation orelse [_]u8{0} ** 32,
        .table_id = table.table_id,
        .definition_digest = backup_contract.tableDefinitionDigest(
            table.table_id,
            table.name,
            table.description,
            table.schema_json,
            table.read_schema_json,
            table.indexes_json,
            table.replication_sources_json,
        ),
        .topology_range_count = topology.range_count,
        .topology_digest = topology.digest,
    };
}

pub fn tableBackupFenceMatches(
    expected: TableBackupFence,
    snapshot: *const metadata_api.AdminSnapshot,
    table: *const metadata_table_manager.TableRecord,
) bool {
    return expected.matches(tableBackupFence(snapshot, table));
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

pub const RestorePublicationHook = backup_contract.RestorePublicationHook;

pub fn validateRestorableManifestLayout(manifest: *const TableBackupManifest) !void {
    if (manifest.shards.len == 0) return error.UnsupportedBackupFormat;
}

pub fn validateSingleRangeRestoreManifestLayout(manifest: *const TableBackupManifest) !void {
    try validateRestorableManifestLayout(manifest);
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

/// Production adapter from the configured backup location to the canonical
/// refs/manifests/blobs repository. The adapter borrows `location`; callers
/// must keep both values alive for the complete repository operation.
const repository_epoch_path = "control/repository.epoch";
const repository_coordinator_path = "control/repository.coordinator";
const repository_epoch_max_bytes: usize = 32;
const repository_coordinator_duration_ns: u64 = 5 * std.time.ns_per_min;

fn parseRepositoryEpoch(encoded: []const u8) !u64 {
    const trimmed = std.mem.trim(u8, encoded, " \t\r\n");
    if (trimmed.len == 0) return error.InvalidBackupRepositoryEpoch;
    return std.fmt.parseUnsigned(u64, trimmed, 10) catch
        return error.InvalidBackupRepositoryEpoch;
}

fn encodeRepositoryEpoch(buf: *[32]u8, epoch: u64) []const u8 {
    return std.fmt.bufPrint(buf, "{d}\n", .{epoch}) catch unreachable;
}

const RemoteRepositoryCoordinator = struct {
    alloc: std.mem.Allocator,
    store: *RemoteBackupStore,
    owner: [32]u8,
    etag: []u8,
    released: bool = false,

    fn deinit(self: *@This()) void {
        self.alloc.free(self.etag);
        self.* = undefined;
    }

    /// Refreshing with an ETag CAS is the fencing check. An owner paused past
    /// expiry cannot resume a control-plane mutation after a successor has
    /// replaced the coordinator object.
    fn refresh(self: *@This()) !void {
        const now_unix_ns: u64 = @intCast(platform_time.realtimeNs());
        const encoded = try encodeClusterBackupReservationLease(
            self.alloc,
            &self.owner,
            now_unix_ns +| repository_coordinator_duration_ns,
        );
        defer self.alloc.free(encoded);
        const key = try self.store.keyAlloc(self.alloc, repository_coordinator_path);
        defer self.alloc.free(key);
        var result = self.store.client.putObject(self.store.bucket, key, encoded, .{
            .content_type = "text/plain",
            .if_match_etag = self.etag,
        }) catch |err| switch (err) {
            error.FileNotFound, error.PreconditionFailed => return error.BackupPublicationFenced,
            else => return err,
        };
        defer result.deinit(self.alloc);
        const next_etag = result.etag orelse return error.BackupReservationIdentityUnavailable;
        const owned = try self.alloc.dupe(u8, next_etag);
        self.alloc.free(self.etag);
        self.etag = owned;
    }

    fn release(self: *@This()) !void {
        if (self.released) return;
        if (!try self.store.deleteSuffixIfOwned(
            self.alloc,
            repository_coordinator_path,
            &self.owner,
        )) return error.BackupPublicationFenced;
        self.released = true;
    }
};

fn acquireRemoteRepositoryCoordinator(
    alloc: std.mem.Allocator,
    store: *RemoteBackupStore,
) !RemoteRepositoryCoordinator {
    try store.ensureBucket();
    var entropy: [16]u8 = undefined;
    try store.io.randomSecure(&entropy);
    const owner = std.fmt.bytesToHex(entropy, .lower);
    const now_unix_ns: u64 = @intCast(platform_time.realtimeNs());
    const encoded = try encodeClusterBackupReservationLease(
        alloc,
        &owner,
        now_unix_ns +| repository_coordinator_duration_ns,
    );
    defer alloc.free(encoded);
    const key = try store.keyAlloc(alloc, repository_coordinator_path);
    defer alloc.free(key);
    var result = store.client.putObject(store.bucket, key, encoded, .{
        .content_type = "text/plain",
        .if_none_match = true,
    }) catch |err| switch (err) {
        error.PreconditionFailed => blk: {
            var current = try store.client.getObject(store.bucket, key, .{
                .range = .{ .offset = 0, .length = max_backup_attempt_lease_bytes },
                .skip_metadata_probe = true,
                .max_response_bytes = max_backup_attempt_lease_bytes,
            });
            defer current.deinit(alloc);
            const lease = parseClusterBackupReservationLease(current.body) catch
                return error.BackupRepositoryBusy;
            if (!clusterBackupLeaseReclaimable(lease.expires_at_unix_ns, now_unix_ns))
                return error.BackupRepositoryBusy;
            const etag = current.metadata.etag orelse
                return error.BackupReservationIdentityUnavailable;
            break :blk store.client.putObject(store.bucket, key, encoded, .{
                .content_type = "text/plain",
                .if_match_etag = etag,
            }) catch |replace_err| switch (replace_err) {
                error.FileNotFound, error.PreconditionFailed => return error.BackupRepositoryBusy,
                else => return replace_err,
            };
        },
        else => return err,
    };
    defer result.deinit(alloc);
    const etag = try alloc.dupe(
        u8,
        result.etag orelse return error.BackupReservationIdentityUnavailable,
    );
    return .{ .alloc = alloc, .store = store, .owner = owner, .etag = etag };
}

pub const RepositoryLocationBackend = struct {
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,

    pub fn backend(self: *@This()) backup_repository.Backend {
        return .{
            .context = self,
            .garbage_collector = switch (self.location.*) {
                .file => .{ .delete_unreachable = deleteIfOlderThan },
                .remote => null,
            },
            .put_immutable = putImmutable,
            .read_alloc = readAlloc,
            .contains = contains,
            .read_stable_epoch = readStableRepositoryEpoch,
            .activate_lease = activateRepositoryLease,
            .renew_lease = renewRepositoryLease,
            .finalize_publication = finalizeRepositoryPublication,
            .release_lease = releaseRepositoryLease,
            .put_blob_from_file = putBlobFromFile,
            .materialize_blob_to_file = materializeBlobToFile,
        };
    }

    fn fromContext(context: *anyopaque) *@This() {
        return @ptrCast(@alignCast(context));
    }

    fn putImmutable(context: *anyopaque, path: []const u8, bytes: []const u8) !void {
        const self = fromContext(context);
        try validateArtifactRelativePath(path);
        switch (self.location.*) {
            .file => |backup_root| {
                var root = try openOrCreateBackupRootNoFollow(self.io, backup_root);
                defer root.close(self.io);
                try ensureBackupRelativeParentNoFollow(self.io, root, path);
                if (try writeFileToBackupRootIfAbsentLocked(self.alloc, self.io, root, path, bytes)) return;
                const existing = try readFileFromBackupRootAlloc(
                    self.alloc,
                    self.io,
                    root,
                    path,
                    bytes.len +| 1,
                );
                defer self.alloc.free(existing);
                if (!std.mem.eql(u8, existing, bytes)) return error.BackupPublicationConflict;
            },
            .remote => |*store| {
                store.writeBytesIfAbsent(self.alloc, path, bytes, "application/json") catch |err| switch (err) {
                    error.BackupAlreadyExists => {
                        const existing = try store.readBytesAllocLimited(self.alloc, path, bytes.len +| 1);
                        defer self.alloc.free(existing);
                        if (!std.mem.eql(u8, existing, bytes)) return error.BackupPublicationConflict;
                    },
                    else => return err,
                };
            },
        }
    }

    fn readAlloc(context: *anyopaque, alloc: std.mem.Allocator, path: []const u8, limit: usize) ![]u8 {
        const self = fromContext(context);
        return readFileFromLocationUsingIoLimited(alloc, self.io, self.location, path, limit);
    }

    fn contains(context: *anyopaque, path: []const u8) !bool {
        const self = fromContext(context);
        try validateArtifactRelativePath(path);
        return switch (self.location.*) {
            .file => |backup_root| blk: {
                var root = openBackupRootNoFollow(self.io, backup_root) catch |err| switch (err) {
                    error.FileNotFound => break :blk false,
                    else => return err,
                };
                defer root.close(self.io);
                break :blk try fileExistsFromBackupRoot(self.io, root, path);
            },
            .remote => |*store| blk: {
                const key = try store.keyAlloc(self.alloc, path);
                defer self.alloc.free(key);
                var metadata = store.client.statObject(store.bucket, key) catch |err| switch (err) {
                    error.FileNotFound => break :blk false,
                    else => return err,
                };
                metadata.deinit(self.alloc);
                break :blk true;
            },
        };
    }

    fn publishRef(
        context: *anyopaque,
        path: []const u8,
        encoded: []const u8,
        expected_manifest_sha256: ?[]const u8,
        expected_generation: ?u64,
    ) !void {
        const self = fromContext(context);
        try validateArtifactRelativePath(path);
        var next = try backup_repository.parseRefCanonical(self.alloc, encoded);
        defer next.deinit();
        switch (self.location.*) {
            .file => |backup_root| {
                var root = try openOrCreateBackupRootNoFollow(self.io, backup_root);
                defer root.close(self.io);
                const lock_path = try std.fmt.allocPrint(self.alloc, "{s}.cas.lock", .{path});
                defer self.alloc.free(lock_path);
                try ensureBackupRelativeParentNoFollow(self.io, root, lock_path);
                var lock = try openOrCreateBackupLockFile(self.io, root, lock_path);
                defer lock.close(self.io);
                try lock.lock(self.io, .exclusive);
                defer lock.unlock(self.io);
                const current_encoded = readFileFromBackupRootAlloc(
                    self.alloc,
                    self.io,
                    root,
                    path,
                    64 * 1024,
                ) catch |err| switch (err) {
                    error.FileNotFound => null,
                    else => return err,
                };
                defer if (current_encoded) |value| self.alloc.free(value);
                // Exact replay is success after an ambiguous client result.
                if (current_encoded) |value| {
                    if (std.mem.eql(u8, value, encoded)) return;
                }
                var current: ?backup_repository.ParsedRef = if (current_encoded) |value|
                    try backup_repository.parseRefCanonical(self.alloc, value)
                else
                    null;
                defer if (current) |*value| value.deinit();
                try backup_repository.validateRefPublication(.{
                    .next = next.value,
                    .expected_manifest_sha256 = expected_manifest_sha256,
                    .expected_generation = expected_generation,
                }, if (current) |value| value.value else null);
                try ensureBackupRelativeParentNoFollow(self.io, root, path);
                try replaceFileInBackupRootUnderHeldLock(self.alloc, self.io, root, path, encoded);
            },
            .remote => |*store| {
                try store.ensureBucket();
                const key = try store.keyAlloc(self.alloc, path);
                defer self.alloc.free(key);
                var current_result = store.client.getObject(store.bucket, key, .{
                    .range = .{ .offset = 0, .length = 64 * 1024 + 1 },
                    .max_response_bytes = 64 * 1024 + 1,
                }) catch |err| switch (err) {
                    error.FileNotFound => null,
                    else => return err,
                };
                defer if (current_result) |*value| value.deinit(self.alloc);
                if (current_result) |*value| {
                    if (std.mem.eql(u8, value.body, encoded)) return;
                }
                var current: ?backup_repository.ParsedRef = if (current_result) |*value|
                    try backup_repository.parseRefCanonical(self.alloc, value.body)
                else
                    null;
                defer if (current) |*value| value.deinit();
                try backup_repository.validateRefPublication(.{
                    .next = next.value,
                    .expected_manifest_sha256 = expected_manifest_sha256,
                    .expected_generation = expected_generation,
                }, if (current) |value| value.value else null);
                var result = store.client.putObject(store.bucket, key, encoded, .{
                    .content_type = "application/json",
                    .if_none_match = current_result == null,
                    .if_match_etag = if (current_result) |value|
                        value.metadata.etag orelse return error.BackupReservationIdentityUnavailable
                    else
                        null,
                }) catch |err| switch (err) {
                    error.FileNotFound, error.PreconditionFailed => return error.BackupRefConflict,
                    else => return err,
                };
                result.deinit(self.alloc);
            },
        }
    }

    fn localRefMatchesEncoded(
        self: *@This(),
        root: std.Io.Dir,
        path: []const u8,
        encoded: []const u8,
    ) !bool {
        const current = readFileFromBackupRootAlloc(
            self.alloc,
            self.io,
            root,
            path,
            64 * 1024,
        ) catch |err| switch (err) {
            error.FileNotFound => return false,
            else => return err,
        };
        defer self.alloc.free(current);
        return std.mem.eql(u8, current, encoded);
    }

    fn remoteRefMatchesEncoded(
        self: *@This(),
        store: *RemoteBackupStore,
        path: []const u8,
        encoded: []const u8,
    ) !bool {
        const current = store.readBytesAllocLimited(self.alloc, path, 64 * 1024) catch |err| switch (err) {
            error.FileNotFound => return false,
            else => return err,
        };
        defer self.alloc.free(current);
        return std.mem.eql(u8, current, encoded);
    }

    fn readStableRepositoryEpoch(context: *anyopaque) !u64 {
        const self = fromContext(context);
        return switch (self.location.*) {
            .file => |backup_root| blk: {
                var root = try openOrCreateBackupRootNoFollow(self.io, backup_root);
                defer root.close(self.io);
                try ensureBackupRelativeParentNoFollow(self.io, root, repository_coordinator_path);
                var lock = try openOrCreateBackupLockFile(self.io, root, repository_coordinator_path);
                defer lock.close(self.io);
                try lock.lock(self.io, .exclusive);
                defer lock.unlock(self.io);
                break :blk try self.stabilizeLocalRepositoryEpochUnderLock(root);
            },
            .remote => |*store| blk: {
                var coordinator = try acquireRemoteRepositoryCoordinator(self.alloc, store);
                defer coordinator.deinit();
                defer coordinator.release() catch {};
                break :blk try self.stabilizeRemoteRepositoryEpochUnderLock(store, &coordinator);
            },
        };
    }

    fn nextRepositoryEpoch(epoch: u64) !u64 {
        if (epoch == std.math.maxInt(u64)) return error.BackupRepositoryEpochExhausted;
        return epoch + 1;
    }

    fn writeLocalRepositoryEpochUnderLock(
        self: *@This(),
        root: std.Io.Dir,
        epoch: u64,
    ) !void {
        var encoded: [32]u8 = undefined;
        try replaceFileInBackupRootUnderHeldLock(
            self.alloc,
            self.io,
            root,
            repository_epoch_path,
            encodeRepositoryEpoch(&encoded, epoch),
        );
    }

    fn writeRemoteRepositoryEpochUnderLock(
        self: *@This(),
        store: *RemoteBackupStore,
        coordinator: *RemoteRepositoryCoordinator,
        epoch: u64,
    ) !void {
        try coordinator.refresh();
        var encoded: [32]u8 = undefined;
        const key = try store.keyAlloc(self.alloc, repository_epoch_path);
        defer self.alloc.free(key);
        var current = store.client.getObject(store.bucket, key, .{
            .range = .{ .offset = 0, .length = repository_epoch_max_bytes },
            .skip_metadata_probe = true,
            .max_response_bytes = repository_epoch_max_bytes,
        }) catch |err| switch (err) {
            error.FileNotFound => null,
            else => return err,
        };
        defer if (current) |*value| value.deinit(self.alloc);
        if (current) |*value| {
            const observed = try parseRepositoryEpoch(value.body);
            if (epoch != observed +| 1) return error.BackupRepositoryEpochChanged;
        } else if (epoch != 1) return error.BackupRepositoryEpochChanged;
        var result = store.client.putObject(store.bucket, key, encodeRepositoryEpoch(&encoded, epoch), .{
            .content_type = "text/plain",
            .if_none_match = current == null,
            .if_match_etag = if (current) |value|
                value.metadata.etag orelse return error.BackupReservationIdentityUnavailable
            else
                null,
        }) catch |err| switch (err) {
            error.FileNotFound, error.PreconditionFailed => return error.BackupRepositoryEpochChanged,
            else => return err,
        };
        result.deinit(self.alloc);
    }

    fn readLocalRepositoryEpochAssumeRoot(self: *@This(), root: std.Io.Dir) !u64 {
        const encoded = readFileFromBackupRootAlloc(
            self.alloc,
            self.io,
            root,
            repository_epoch_path,
            repository_epoch_max_bytes,
        ) catch |err| switch (err) {
            error.FileNotFound => return 0,
            else => return err,
        };
        defer self.alloc.free(encoded);
        return try parseRepositoryEpoch(encoded);
    }

    fn readRemoteRepositoryEpochAssumeStore(
        self: *@This(),
        store: *RemoteBackupStore,
    ) !u64 {
        const encoded = store.readBytesAllocLimited(
            self.alloc,
            repository_epoch_path,
            repository_epoch_max_bytes,
        ) catch |err| switch (err) {
            error.FileNotFound => return 0,
            else => return err,
        };
        defer self.alloc.free(encoded);
        return try parseRepositoryEpoch(encoded);
    }

    fn stabilizeLocalRepositoryEpochUnderLock(
        self: *@This(),
        root: std.Io.Dir,
    ) !u64 {
        const observed = try readLocalRepositoryEpochAssumeRoot(self, root);
        if (observed % 2 == 0) return observed;
        const stable = try nextRepositoryEpoch(observed);
        try self.writeLocalRepositoryEpochUnderLock(root, stable);
        return stable;
    }

    fn beginLocalRepositoryRootMutationUnderLock(
        self: *@This(),
        root: std.Io.Dir,
    ) !u64 {
        const stable = try self.stabilizeLocalRepositoryEpochUnderLock(root);
        const in_progress = try nextRepositoryEpoch(stable);
        const committed = try nextRepositoryEpoch(in_progress);
        try self.writeLocalRepositoryEpochUnderLock(root, in_progress);
        return committed;
    }

    fn stabilizeRemoteRepositoryEpochUnderLock(
        self: *@This(),
        store: *RemoteBackupStore,
        coordinator: *RemoteRepositoryCoordinator,
    ) !u64 {
        try coordinator.refresh();
        const observed = try readRemoteRepositoryEpochAssumeStore(self, store);
        if (observed % 2 == 0) return observed;
        const stable = try nextRepositoryEpoch(observed);
        try self.writeRemoteRepositoryEpochUnderLock(store, coordinator, stable);
        return stable;
    }

    fn beginRemoteRepositoryRootMutationUnderLock(
        self: *@This(),
        store: *RemoteBackupStore,
        coordinator: *RemoteRepositoryCoordinator,
    ) !u64 {
        const stable = try self.stabilizeRemoteRepositoryEpochUnderLock(store, coordinator);
        const in_progress = try nextRepositoryEpoch(stable);
        const committed = try nextRepositoryEpoch(in_progress);
        try self.writeRemoteRepositoryEpochUnderLock(store, coordinator, in_progress);
        return committed;
    }

    fn deleteRemoteRepositoryLease(
        self: *@This(),
        store: *RemoteBackupStore,
        lease_path: []const u8,
        fencing_token: u64,
    ) !void {
        const key = try store.keyAlloc(self.alloc, lease_path);
        defer self.alloc.free(key);
        var current = try store.client.getObject(store.bucket, key, .{
            .range = .{ .offset = 0, .length = 64 * 1024 },
            .skip_metadata_probe = true,
            .max_response_bytes = 64 * 1024,
        });
        defer current.deinit(self.alloc);
        var lease = try backup_repository.parseLeaseCanonical(self.alloc, current.body, 0);
        defer lease.deinit();
        if (lease.value.fencing_token != fencing_token) return error.BackupPublicationFenced;
        const etag = current.metadata.etag orelse return error.BackupReservationIdentityUnavailable;
        store.client.deleteObject(store.bucket, key, .{ .if_match_etag = etag }) catch |err| switch (err) {
            error.FileNotFound, error.PreconditionFailed => return error.BackupPublicationFenced,
            else => return err,
        };
    }

    fn activateRepositoryLease(
        context: *anyopaque,
        path: []const u8,
        encoded: []const u8,
        fencing_token: u64,
    ) !u64 {
        const self = fromContext(context);
        try validateArtifactRelativePath(path);
        return switch (self.location.*) {
            .file => |backup_root| blk: {
                var root = try openOrCreateBackupRootNoFollow(self.io, backup_root);
                defer root.close(self.io);
                try ensureBackupRelativeParentNoFollow(self.io, root, repository_coordinator_path);
                var lock = try openOrCreateBackupLockFile(self.io, root, repository_coordinator_path);
                defer lock.close(self.io);
                try lock.lock(self.io, .exclusive);
                defer lock.unlock(self.io);
                const existing = readFileFromBackupRootAlloc(
                    self.alloc,
                    self.io,
                    root,
                    path,
                    64 * 1024,
                ) catch |err| switch (err) {
                    error.FileNotFound => null,
                    else => return err,
                };
                defer if (existing) |value| self.alloc.free(value);
                if (existing) |value| {
                    var parsed = try backup_repository.parseLeaseCanonical(self.alloc, value, 0);
                    defer parsed.deinit();
                    if (parsed.value.fencing_token != fencing_token or
                        !std.mem.eql(u8, value, encoded)) return error.BackupRepositoryBusy;
                    break :blk try self.stabilizeLocalRepositoryEpochUnderLock(root);
                } else {
                    const stable = try self.beginLocalRepositoryRootMutationUnderLock(root);
                    try ensureBackupRelativeParentNoFollow(self.io, root, path);
                    try replaceFileInBackupRootUnderHeldLock(self.alloc, self.io, root, path, encoded);
                    try self.writeLocalRepositoryEpochUnderLock(root, stable);
                    break :blk stable;
                }
            },
            .remote => |*store| blk: {
                var coordinator = try acquireRemoteRepositoryCoordinator(self.alloc, store);
                defer coordinator.deinit();
                defer coordinator.release() catch {};
                try coordinator.refresh();
                const existing = store.readBytesAllocLimited(self.alloc, path, 64 * 1024) catch |err| switch (err) {
                    error.FileNotFound => null,
                    else => return err,
                };
                defer if (existing) |value| self.alloc.free(value);
                if (existing) |value| {
                    var parsed = try backup_repository.parseLeaseCanonical(self.alloc, value, 0);
                    defer parsed.deinit();
                    if (parsed.value.fencing_token != fencing_token or
                        !std.mem.eql(u8, value, encoded)) return error.BackupRepositoryBusy;
                    break :blk try self.stabilizeRemoteRepositoryEpochUnderLock(store, &coordinator);
                } else {
                    const stable = try self.beginRemoteRepositoryRootMutationUnderLock(store, &coordinator);
                    try coordinator.refresh();
                    try store.writeBytesIfAbsent(self.alloc, path, encoded, "application/json");
                    try self.writeRemoteRepositoryEpochUnderLock(store, &coordinator, stable);
                    break :blk stable;
                }
            },
        };
    }

    fn finalizeRepositoryPublication(
        context: *anyopaque,
        lease_path: []const u8,
        fencing_token: u64,
        manifest_sha256: []const u8,
        now_unix_ns: i128,
        ref_path: []const u8,
        encoded_ref: []const u8,
        expected_manifest_sha256: ?[]const u8,
        expected_generation: ?u64,
    ) !u64 {
        const self = fromContext(context);
        var next_ref = try backup_repository.parseRefCanonical(self.alloc, encoded_ref);
        defer next_ref.deinit();
        if (!std.mem.eql(u8, next_ref.value.manifest_sha256, manifest_sha256))
            return error.BackupPublicationFenced;
        return switch (self.location.*) {
            .file => |backup_root| blk: {
                var root = try openOrCreateBackupRootNoFollow(self.io, backup_root);
                defer root.close(self.io);
                try ensureBackupRelativeParentNoFollow(self.io, root, repository_coordinator_path);
                var lock = try openOrCreateBackupLockFile(self.io, root, repository_coordinator_path);
                defer lock.close(self.io);
                try lock.lock(self.io, .exclusive);
                defer lock.unlock(self.io);
                if (try self.localRefMatchesEncoded(root, ref_path, encoded_ref))
                    break :blk try self.stabilizeLocalRepositoryEpochUnderLock(root);
                const lease_bytes = readFileFromBackupRootAlloc(
                    self.alloc,
                    self.io,
                    root,
                    lease_path,
                    64 * 1024,
                ) catch |err| switch (err) {
                    error.FileNotFound => {
                        if (!try self.localRefMatchesEncoded(root, ref_path, encoded_ref))
                            return error.BackupPublicationFenced;
                        break :blk try self.stabilizeLocalRepositoryEpochUnderLock(root);
                    },
                    else => return err,
                };
                defer self.alloc.free(lease_bytes);
                var lease = try backup_repository.parseLeaseCanonical(self.alloc, lease_bytes, now_unix_ns);
                defer lease.deinit();
                if (lease.value.fencing_token != fencing_token or
                    !std.mem.eql(u8, lease.value.manifest_sha256, manifest_sha256))
                    return error.BackupPublicationFenced;
                const stable = try self.beginLocalRepositoryRootMutationUnderLock(root);
                try publishRef(context, ref_path, encoded_ref, expected_manifest_sha256, expected_generation);
                // The ref is the commit point. Failed cleanup only retains
                // data until lease expiry and cannot make commit ambiguous.
                deleteFileDurablyFromBackupRoot(self.io, root, lease_path) catch {};
                try self.writeLocalRepositoryEpochUnderLock(root, stable);
                break :blk stable;
            },
            .remote => |*store| blk: {
                var coordinator = try acquireRemoteRepositoryCoordinator(self.alloc, store);
                defer coordinator.deinit();
                defer coordinator.release() catch {};
                try coordinator.refresh();
                if (try self.remoteRefMatchesEncoded(store, ref_path, encoded_ref))
                    break :blk try self.stabilizeRemoteRepositoryEpochUnderLock(store, &coordinator);
                const lease_bytes = store.readBytesAllocLimited(self.alloc, lease_path, 64 * 1024) catch |err| switch (err) {
                    error.FileNotFound => {
                        if (!try self.remoteRefMatchesEncoded(store, ref_path, encoded_ref))
                            return error.BackupPublicationFenced;
                        break :blk try self.stabilizeRemoteRepositoryEpochUnderLock(store, &coordinator);
                    },
                    else => return err,
                };
                defer self.alloc.free(lease_bytes);
                var lease = try backup_repository.parseLeaseCanonical(self.alloc, lease_bytes, now_unix_ns);
                defer lease.deinit();
                if (lease.value.fencing_token != fencing_token or
                    !std.mem.eql(u8, lease.value.manifest_sha256, manifest_sha256))
                    return error.BackupPublicationFenced;
                const stable = try self.beginRemoteRepositoryRootMutationUnderLock(store, &coordinator);
                try coordinator.refresh();
                try publishRef(context, ref_path, encoded_ref, expected_manifest_sha256, expected_generation);
                deleteRemoteRepositoryLease(self, store, lease_path, fencing_token) catch {};
                try self.writeRemoteRepositoryEpochUnderLock(store, &coordinator, stable);
                break :blk stable;
            },
        };
    }

    fn renewRepositoryLease(
        context: *anyopaque,
        lease_path: []const u8,
        fencing_token: u64,
        manifest_sha256: []const u8,
        now_unix_ns: i128,
        expires_at_unix_ns: i128,
    ) !u64 {
        const self = fromContext(context);
        if (expires_at_unix_ns <= now_unix_ns) return error.InvalidBackupLease;
        return switch (self.location.*) {
            .file => |backup_root| blk: {
                var root = try openOrCreateBackupRootNoFollow(self.io, backup_root);
                defer root.close(self.io);
                try ensureBackupRelativeParentNoFollow(self.io, root, repository_coordinator_path);
                var lock = try openOrCreateBackupLockFile(self.io, root, repository_coordinator_path);
                defer lock.close(self.io);
                try lock.lock(self.io, .exclusive);
                defer lock.unlock(self.io);
                const encoded = try readFileFromBackupRootAlloc(self.alloc, self.io, root, lease_path, 64 * 1024);
                defer self.alloc.free(encoded);
                var parsed = try backup_repository.parseLeaseCanonical(self.alloc, encoded, now_unix_ns);
                defer parsed.deinit();
                if (parsed.value.fencing_token != fencing_token or
                    !std.mem.eql(u8, parsed.value.manifest_sha256, manifest_sha256))
                    return error.BackupPublicationFenced;
                if (expires_at_unix_ns == parsed.value.expires_at_unix_ns)
                    break :blk try self.stabilizeLocalRepositoryEpochUnderLock(root);
                if (expires_at_unix_ns < parsed.value.expires_at_unix_ns)
                    return error.InvalidBackupLease;
                const renewed: backup_repository.Lease = .{
                    .lease_id = parsed.value.lease_id,
                    .manifest_sha256 = parsed.value.manifest_sha256,
                    .base_manifest_sha256 = parsed.value.base_manifest_sha256,
                    .fencing_token = fencing_token,
                    .expires_at_unix_ns = expires_at_unix_ns,
                };
                const renewed_encoded = try backup_repository.encodeLeaseCanonicalAlloc(self.alloc, renewed, now_unix_ns);
                defer self.alloc.free(renewed_encoded);
                const stable = try self.beginLocalRepositoryRootMutationUnderLock(root);
                try replaceFileInBackupRootUnderHeldLock(self.alloc, self.io, root, lease_path, renewed_encoded);
                try self.writeLocalRepositoryEpochUnderLock(root, stable);
                break :blk stable;
            },
            .remote => |*store| blk: {
                var coordinator = try acquireRemoteRepositoryCoordinator(self.alloc, store);
                defer coordinator.deinit();
                defer coordinator.release() catch {};
                try coordinator.refresh();
                const key = try store.keyAlloc(self.alloc, lease_path);
                defer self.alloc.free(key);
                var current = try store.client.getObject(store.bucket, key, .{
                    .range = .{ .offset = 0, .length = 64 * 1024 },
                    .skip_metadata_probe = true,
                    .max_response_bytes = 64 * 1024,
                });
                defer current.deinit(self.alloc);
                var parsed = try backup_repository.parseLeaseCanonical(self.alloc, current.body, now_unix_ns);
                defer parsed.deinit();
                if (parsed.value.fencing_token != fencing_token or
                    !std.mem.eql(u8, parsed.value.manifest_sha256, manifest_sha256))
                    return error.BackupPublicationFenced;
                if (expires_at_unix_ns == parsed.value.expires_at_unix_ns)
                    break :blk try self.stabilizeRemoteRepositoryEpochUnderLock(store, &coordinator);
                if (expires_at_unix_ns < parsed.value.expires_at_unix_ns)
                    return error.InvalidBackupLease;
                const renewed: backup_repository.Lease = .{
                    .lease_id = parsed.value.lease_id,
                    .manifest_sha256 = parsed.value.manifest_sha256,
                    .base_manifest_sha256 = parsed.value.base_manifest_sha256,
                    .fencing_token = fencing_token,
                    .expires_at_unix_ns = expires_at_unix_ns,
                };
                const renewed_encoded = try backup_repository.encodeLeaseCanonicalAlloc(self.alloc, renewed, now_unix_ns);
                defer self.alloc.free(renewed_encoded);
                const etag = current.metadata.etag orelse return error.BackupReservationIdentityUnavailable;
                const stable = try self.beginRemoteRepositoryRootMutationUnderLock(store, &coordinator);
                try coordinator.refresh();
                var result = store.client.putObject(store.bucket, key, renewed_encoded, .{
                    .content_type = "application/json",
                    .if_match_etag = etag,
                }) catch |err| switch (err) {
                    error.FileNotFound, error.PreconditionFailed => return error.BackupPublicationFenced,
                    else => return err,
                };
                result.deinit(self.alloc);
                try self.writeRemoteRepositoryEpochUnderLock(store, &coordinator, stable);
                break :blk stable;
            },
        };
    }

    fn releaseRepositoryLease(
        context: *anyopaque,
        lease_path: []const u8,
        fencing_token: u64,
    ) !u64 {
        const self = fromContext(context);
        return switch (self.location.*) {
            .file => |backup_root| blk: {
                var root = try openOrCreateBackupRootNoFollow(self.io, backup_root);
                defer root.close(self.io);
                try ensureBackupRelativeParentNoFollow(self.io, root, repository_coordinator_path);
                var lock = try openOrCreateBackupLockFile(self.io, root, repository_coordinator_path);
                defer lock.close(self.io);
                try lock.lock(self.io, .exclusive);
                defer lock.unlock(self.io);
                const lease_bytes = readFileFromBackupRootAlloc(
                    self.alloc,
                    self.io,
                    root,
                    lease_path,
                    64 * 1024,
                ) catch |err| switch (err) {
                    error.FileNotFound => break :blk try self.stabilizeLocalRepositoryEpochUnderLock(root),
                    else => return err,
                };
                defer self.alloc.free(lease_bytes);
                var lease = try backup_repository.parseLeaseCanonical(self.alloc, lease_bytes, 0);
                defer lease.deinit();
                if (lease.value.fencing_token != fencing_token) return error.BackupPublicationFenced;
                const stable = try self.beginLocalRepositoryRootMutationUnderLock(root);
                try deleteFileDurablyFromBackupRoot(self.io, root, lease_path);
                try self.writeLocalRepositoryEpochUnderLock(root, stable);
                break :blk stable;
            },
            .remote => |*store| blk: {
                var coordinator = try acquireRemoteRepositoryCoordinator(self.alloc, store);
                defer coordinator.deinit();
                defer coordinator.release() catch {};
                try coordinator.refresh();
                const stable = try self.beginRemoteRepositoryRootMutationUnderLock(store, &coordinator);
                deleteRemoteRepositoryLease(self, store, lease_path, fencing_token) catch |err| switch (err) {
                    error.FileNotFound => {},
                    else => return err,
                };
                try self.writeRemoteRepositoryEpochUnderLock(store, &coordinator, stable);
                break :blk stable;
            },
        };
    }

    fn deleteIfOlderThan(
        context: *anyopaque,
        path: []const u8,
        cutoff_unix_ns: i128,
        expected_epoch: u64,
    ) !bool {
        const self = fromContext(context);
        try validateArtifactRelativePath(path);
        return switch (self.location.*) {
            .file => |backup_root| blk: {
                var root = openBackupRootNoFollow(self.io, backup_root) catch |err| switch (err) {
                    error.FileNotFound => break :blk false,
                    else => return err,
                };
                defer root.close(self.io);
                try ensureBackupRelativeParentNoFollow(self.io, root, repository_coordinator_path);
                var coordinator = try openOrCreateBackupLockFile(self.io, root, repository_coordinator_path);
                defer coordinator.close(self.io);
                try coordinator.lock(self.io, .exclusive);
                defer coordinator.unlock(self.io);
                if (expected_epoch % 2 != 0 or
                    try readLocalRepositoryEpochAssumeRoot(self, root) != expected_epoch)
                    return error.BackupRepositoryEpochChanged;
                const lock_path = try std.fmt.allocPrint(self.alloc, "{s}.publish.lock", .{path});
                defer self.alloc.free(lock_path);
                try ensureBackupRelativeParentNoFollow(self.io, root, lock_path);
                var lock = try openOrCreateBackupLockFile(self.io, root, lock_path);
                defer lock.close(self.io);
                try lock.lock(self.io, .exclusive);
                defer lock.unlock(self.io);
                var parent = openBackupRelativeParentNoFollow(self.io, root, path) catch |err| switch (err) {
                    error.FileNotFound => break :blk false,
                    else => return err,
                };
                defer parent.deinit(self.io);
                const stat = parent.dir.statFile(self.io, parent.basename, .{}) catch |err| switch (err) {
                    error.FileNotFound => break :blk false,
                    else => return err,
                };
                if (stat.mtime.toNanoseconds() >= cutoff_unix_ns) break :blk false;
                try deleteFileDurablyFromBackupRoot(self.io, root, path);
                break :blk true;
            },
            .remote => blk: {
                // A renewable coordinator plus ETag cannot fence a process
                // paused after its last refresh. Content-addressed bytes can
                // also be republished with the same ETag. Remote repositories
                // therefore remain append-only until a transactional catalog
                // can tombstone and delete an exact storage generation.
                break :blk error.BackupRepositoryGcUnsupported;
            },
        };
    }

    fn putBlobFromFile(
        context: *anyopaque,
        io: std.Io,
        path: []const u8,
        source_path: []const u8,
        logical_size_bytes: u64,
        sha256: []const u8,
        publication_fence: u64,
    ) !backup_repository.VerifiedBlobReceipt {
        const self = fromContext(context);
        try validateArtifactRelativePath(path);
        if (publication_fence == 0) return error.InvalidBackupLease;
        var source = if (std.fs.path.isAbsolute(source_path))
            try std.Io.Dir.openFileAbsolute(io, source_path, .{})
        else
            try std.Io.Dir.cwd().openFile(io, source_path, .{});
        defer source.close(io);
        const initial_source_stat = try source.stat(io);
        try validateFileIntegrity(self.alloc, io, source_path, logical_size_bytes, sha256);
        if (!localArtifactStatsEqual(initial_source_stat, try source.stat(io)))
            return error.SourceFileChanged;
        var storage_identity: u64 = 0;
        switch (self.location.*) {
            .file => |backup_root| {
                var root = try openOrCreateBackupRootNoFollow(io, backup_root);
                defer root.close(io);
                try ensureBackupRelativeParentNoFollow(io, root, path);
                const created = try copyFileToBackupRootIfAbsentLocked(
                    self.alloc,
                    io,
                    root,
                    path,
                    source_path,
                );
                _ = created;
                // Verify both newly copied and pre-existing objects. The
                // source pathname can be replaced after the pre-hash, and a
                // content-addressed key must never be accepted on source-stat
                // evidence alone.
                try validateBackupRootFileIntegrity(
                    io,
                    root,
                    path,
                    logical_size_bytes,
                    sha256,
                );
                var parent = try openBackupRelativeParentNoFollow(io, root, path);
                defer parent.deinit(io);
                const stored_stat = try parent.dir.statFile(io, parent.basename, .{});
                storage_identity = localArtifactStatIdentity(stored_stat);
            },
            .remote => |*store| {
                try store.ensureBucket();
                const key = try store.keyAlloc(self.alloc, path);
                defer self.alloc.free(key);
                var maybe_result: ?object_storage.PutResult = store.client.putFileWithIo(io, store.bucket, key, source_path, .{
                    .content_type = "application/octet-stream",
                    .if_none_match = true,
                }) catch |err| switch (err) {
                    error.PreconditionFailed => blk: {
                        storage_identity = try verifyRemoteBlob(
                            self.alloc,
                            store,
                            key,
                            logical_size_bytes,
                            sha256,
                            null,
                        );
                        break :blk null;
                    },
                    else => return err,
                };
                if (maybe_result) |*result| {
                    defer result.deinit(self.alloc);
                    const uploaded_etag = try self.alloc.dupe(
                        u8,
                        result.etag orelse return error.BackupReservationIdentityUnavailable,
                    );
                    defer self.alloc.free(uploaded_etag);
                    storage_identity = verifyRemoteBlob(
                        self.alloc,
                        store,
                        key,
                        logical_size_bytes,
                        sha256,
                        uploaded_etag,
                    ) catch |verify_err| {
                        store.client.deleteObject(store.bucket, key, .{
                            .if_match_etag = uploaded_etag,
                        }) catch |delete_err| switch (delete_err) {
                            error.FileNotFound, error.PreconditionFailed => {},
                            else => return delete_err,
                        };
                        return verify_err;
                    };
                }
            },
        }
        if (!localArtifactStatsEqual(initial_source_stat, try source.stat(io)))
            return error.SourceFileChanged;
        var current_source = if (std.fs.path.isAbsolute(source_path))
            try std.Io.Dir.openFileAbsolute(io, source_path, .{})
        else
            try std.Io.Dir.cwd().openFile(io, source_path, .{});
        defer current_source.close(io);
        if (!localArtifactStatsEqual(initial_source_stat, try current_source.stat(io)))
            return error.SourceFileChanged;
        return .{
            .content_sha256 = sha256,
            .storage_sha256 = sha256,
            .logical_size_bytes = logical_size_bytes,
            .publication_fence = publication_fence,
            .storage_identity = storage_identity,
        };
    }

    fn materializeBlobToFile(
        context: *anyopaque,
        io: std.Io,
        path: []const u8,
        destination_path: []const u8,
        logical_size_bytes: u64,
        sha256: []const u8,
    ) !void {
        const self = fromContext(context);
        try copyFileFromLocationVerifiedUsingIo(
            self.alloc,
            io,
            self.location,
            path,
            destination_path,
            logical_size_bytes,
            sha256,
            .none,
        );
    }
};

test "canonical repository file adapter streams blobs and compare-and-swaps refs" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/repository", .{tmp.sub_path});
    defer alloc.free(root);
    const source_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/source.bin", .{tmp.sub_path});
    defer alloc.free(source_path);
    const restored_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/restored.bin", .{tmp.sub_path});
    defer alloc.free(restored_path);
    try writeFileAbsolute(source_path, "shared immutable bytes");

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    var location: BackupLocation = .{ .file = try alloc.dupe(u8, root) };
    defer location.deinit(alloc);
    var adapter: RepositoryLocationBackend = .{
        .alloc = alloc,
        .io = io_impl.io(),
        .location = &location,
    };
    const backend = adapter.backend();

    try backend.put_immutable(backend.context, "manifests/control", "one");
    try backend.put_immutable(backend.context, "manifests/control", "one");
    try std.testing.expectError(
        error.BackupPublicationConflict,
        backend.put_immutable(backend.context, "manifests/control", "two"),
    );

    var integrity = try portableBytesIntegrityAlloc(alloc, "shared immutable bytes");
    defer integrity.deinit(alloc);
    const blob_path = try backup_repository.blobPathAlloc(alloc, integrity.sha256);
    defer alloc.free(blob_path);
    const receipt = try backend.put_blob_from_file(
        backend.context,
        io_impl.io(),
        blob_path,
        source_path,
        integrity.size_bytes,
        integrity.sha256,
        1,
    );
    try std.testing.expectEqual(@as(u64, 1), receipt.publication_fence);
    try std.testing.expect(receipt.storage_identity != 0);
    try std.testing.expect(try backend.contains(backend.context, blob_path));
    try backend.materialize_blob_to_file(
        backend.context,
        io_impl.io(),
        blob_path,
        restored_path,
        integrity.size_bytes,
        integrity.sha256,
    );
    var restored = try artifactIntegrityAlloc(alloc, io_impl.io(), .portable, restored_path);
    defer restored.deinit(alloc);
    try std.testing.expectEqual(integrity.size_bytes, restored.size_bytes);
    try std.testing.expectEqualStrings(integrity.sha256, restored.sha256);

    const manifest_a = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    const manifest_b = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789";
    const ref_path = try backup_repository.refPathAlloc(alloc, "daily");
    defer alloc.free(ref_path);
    const first: backup_repository.Ref = .{
        .backup_id = "daily",
        .manifest_sha256 = manifest_a,
        .generation = 1,
        .updated_at_unix_ns = 1,
    };
    const first_encoded = try backup_repository.encodeRefCanonicalAlloc(alloc, first);
    defer alloc.free(first_encoded);
    try RepositoryLocationBackend.publishRef(&adapter, ref_path, first_encoded, null, null);
    try RepositoryLocationBackend.publishRef(&adapter, ref_path, first_encoded, null, null);
    const second: backup_repository.Ref = .{
        .backup_id = "daily",
        .manifest_sha256 = manifest_b,
        .generation = 2,
        .updated_at_unix_ns = 2,
    };
    const second_encoded = try backup_repository.encodeRefCanonicalAlloc(alloc, second);
    defer alloc.free(second_encoded);
    try RepositoryLocationBackend.publishRef(&adapter, ref_path, second_encoded, manifest_a, 1);
    const resolved = try backend.read_alloc(backend.context, alloc, ref_path, 64 * 1024);
    defer alloc.free(resolved);
    var parsed = try backup_repository.parseRefCanonical(alloc, resolved);
    defer parsed.deinit();
    try std.testing.expectEqual(@as(u64, 2), parsed.value.generation);
    try std.testing.expectEqualStrings(manifest_b, parsed.value.manifest_sha256);

    const source_root = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer alloc.free(source_root);
    const manifest: backup_repository.Manifest = .{
        .backup_id = "session",
        .table_id = 7,
        .table_name = "docs",
        .catalog_sha256 = integrity.sha256,
        .representation = .native,
        .created_at_unix_ns = 1,
        .shards = &.{.{
            .group_id = 9,
            .start_key_base64 = "",
            .object_paths = &.{"source.bin"},
            .capture_revision = 1,
            .checkpoint_revision = 1,
        }},
        .objects = &.{.{
            .logical_path = "source.bin",
            .role = "catalog",
            .content_sha256 = integrity.sha256,
            .logical_size_bytes = integrity.size_bytes,
        }},
        .blobs = &.{.{
            .content_sha256 = integrity.sha256,
            .storage_sha256 = integrity.sha256,
            .representative_object_path = "source.bin",
            .logical_size_bytes = integrity.size_bytes,
            .stored_size_bytes = integrity.size_bytes,
        }},
    };
    const plan: backup_repository.SnapshotPlan = .{ .manifest = manifest };
    var session = try backup_repository.beginPublication(
        alloc,
        backend,
        plan,
        "session-publication",
        41,
        1,
        100,
    );
    defer session.deinit(alloc);
    const receipts = try backup_repository.uploadSnapshotBlobsFromDirectory(
        alloc,
        io_impl.io(),
        backend,
        plan,
        session,
        source_root,
    );
    defer alloc.free(receipts);
    const committed_epoch = try backup_repository.finishPublication(alloc, backend, plan, session, receipts, .{ .next = .{
        .backup_id = "session",
        .manifest_sha256 = session.manifest_sha256,
        .generation = 1,
        .updated_at_unix_ns = 2,
    } }, 2);
    try std.testing.expect(committed_epoch % 2 == 0);
    // An ambiguous client result is exactly replayable even though successful
    // finalization already consumed the publication lease.
    try std.testing.expectEqual(committed_epoch, try backup_repository.finishPublication(
        alloc,
        backend,
        plan,
        session,
        receipts,
        .{ .next = .{
            .backup_id = "session",
            .manifest_sha256 = session.manifest_sha256,
            .generation = 1,
            .updated_at_unix_ns = 2,
        } },
        101,
    ));
    var snapshot = try backup_repository.resolveSnapshot(alloc, backend, "session");
    defer snapshot.deinit();
    try std.testing.expectEqualStrings(session.manifest_sha256, snapshot.ref.value.manifest_sha256);
}

test "remote repository coordinator fences a resumed stale owner" {
    const alloc = std.testing.allocator;
    var memory = object_storage.MemoryObjectStorage.init(alloc);
    defer memory.deinit();
    var location: BackupLocation = .{
        .remote = try RemoteBackupStore.initWithClient(alloc, memory.client(), "bucket", "backups"),
    };
    defer location.deinit(alloc);
    const store = &location.remote;

    var stale = try acquireRemoteRepositoryCoordinator(alloc, store);
    defer stale.deinit();
    var adapter: RepositoryLocationBackend = .{
        .alloc = alloc,
        .io = store.io,
        .location = &location,
    };
    const repository_backend = adapter.backend();
    try std.testing.expect(repository_backend.garbage_collector == null);
    var empty_reachability: backup_repository.Reachability = .{ .repository_epoch = 0 };
    defer empty_reachability.deinit(alloc);
    try std.testing.expectError(
        error.BackupRepositoryGcUnsupported,
        backup_repository.sweepUnreachable(
            alloc,
            repository_backend,
            &empty_reachability,
            &.{.{
                .kind = .blob,
                .sha256 = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
            }},
            0,
        ),
    );
    try adapter.writeRemoteRepositoryEpochUnderLock(store, &stale, 1);
    try std.testing.expectError(
        error.BackupRepositoryEpochChanged,
        adapter.writeRemoteRepositoryEpochUnderLock(store, &stale, 1),
    );
    const successor_owner = "successor-owner";
    const encoded = try encodeClusterBackupReservationLease(
        alloc,
        successor_owner,
        @as(u64, @intCast(platform_time.realtimeNs())) +| repository_coordinator_duration_ns,
    );
    defer alloc.free(encoded);
    const key = try store.keyAlloc(alloc, repository_coordinator_path);
    defer alloc.free(key);
    var replacement = try store.client.putObject(store.bucket, key, encoded, .{
        .content_type = "text/plain",
        .if_match_etag = stale.etag,
    });
    replacement.deinit(alloc);

    try std.testing.expectError(error.BackupPublicationFenced, stale.refresh());
    try std.testing.expectError(error.BackupPublicationFenced, stale.release());
}

test "repository remote verification accepts only full-object SHA-256 proofs" {
    const expected = "0000000000000000000000000000000000000000000000000000000000000000";
    var metadata: object_storage.ObjectMetadata = .{
        .bucket = @constCast("bucket"),
        .key = @constCast("key"),
        .checksum = .{
            .algorithm = .sha256_hex,
            .value = @constCast(expected),
            .checksum_type = .full_object,
        },
        .content_length = 0,
    };
    try std.testing.expect(remoteFullSha256ProofMatches(metadata, expected).?);
    metadata.checksum.?.value = @constCast("1000000000000000000000000000000000000000000000000000000000000000");
    try std.testing.expect(!remoteFullSha256ProofMatches(metadata, expected).?);
    metadata.checksum.?.checksum_type = .composite;
    try std.testing.expect(remoteFullSha256ProofMatches(metadata, expected) == null);

    const digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = @splat(0);
    var encoded: [std.base64.standard.Encoder.calcSize(digest.len)]u8 = undefined;
    const base64_digest = std.base64.standard.Encoder.encode(&encoded, &digest);
    metadata.checksum = .{
        .algorithm = .sha256_base64,
        .value = @constCast(base64_digest),
        .checksum_type = .full_object,
    };
    try std.testing.expect(remoteFullSha256ProofMatches(metadata, expected).?);
    metadata.checksum_scope = .response_body;
    try std.testing.expect(remoteFullSha256ProofMatches(metadata, expected) == null);
}

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

fn createOwnedThreadedIo(alloc: std.mem.Allocator) !*std.Io.Threaded {
    const owned = try alloc.create(std.Io.Threaded);
    // CLI and embedded backup stores can live for the whole operation and
    // issue concurrent HTTP work. Keep the fallback finite when a server
    // runtime was not supplied.
    owned.* = threaded_io_limits.initService(alloc);
    return owned;
}

test "owned backup runtime has a finite worker ceiling" {
    const owned = try createOwnedThreadedIo(std.testing.allocator);
    defer std.testing.allocator.destroy(owned);
    defer owned.deinit();

    try std.testing.expectEqual(
        std.Io.Limit.limited(threaded_io_limits.service),
        owned.concurrent_limit,
    );
}

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
            break :blk try createOwnedThreadedIo(alloc);
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

const BackupLeaseFenceClaim = enum { active, claimed };

const RemoteBackupStore = struct {
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
            break :blk try createOwnedThreadedIo(alloc);
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
            break :blk try createOwnedThreadedIo(alloc);
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
        return self.ensureBucketWithCancellation(.none);
    }

    fn ensureBucketWithCancellation(
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
        return self.writeBytesWithCancellation(alloc, suffix, body, content_type, .none);
    }

    fn writeBytesWithCancellation(
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

    fn writeBytesIfAbsent(self: *RemoteBackupStore, alloc: std.mem.Allocator, suffix: []const u8, body: []const u8, content_type: []const u8) !void {
        return self.writeBytesIfAbsentWithCancellation(alloc, suffix, body, content_type, .none);
    }

    fn writeBytesIfAbsentWithCancellation(
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

    fn replaceBytesIfOwned(
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

    fn replaceBytesIfOwnedWithCancellation(
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

    fn deleteSuffix(self: *RemoteBackupStore, alloc: std.mem.Allocator, suffix: []const u8) !void {
        return self.deleteSuffixWithCancellation(alloc, suffix, .none);
    }

    fn deleteSuffixWithCancellation(
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

    fn deleteSuffixBudgeted(
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

    fn deleteSuffixBudgetedWithCancellation(
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

    fn suffixOwnerMatches(
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

    fn suffixOwnerMatchesWithCancellation(
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
    fn takeExpiredReservation(
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
    fn claimExpiredLeaseWithFence(
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

    fn claimExpiredLeaseWithFenceAndCancellation(
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

    fn deletePrefix(
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

    fn deletePrefixWithCancellation(
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

    fn writeFile(
        self: *RemoteBackupStore,
        alloc: std.mem.Allocator,
        suffix: []const u8,
        src_path: []const u8,
        content_type: []const u8,
        cancellation: CancellationToken,
    ) !void {
        try cancellation.check();
        try self.ensureBucketWithCancellation(cancellation);
        const key = try self.keyAlloc(alloc, suffix);
        defer alloc.free(key);
        var result = try self.client.putFileWithIo(self.io, self.bucket, key, src_path, .{
            .content_type = content_type,
            .cancellation = objectCancellationToken(cancellation),
        });
        defer result.deinit(alloc);
    }

    fn readBytesAllocLimited(self: *RemoteBackupStore, alloc: std.mem.Allocator, suffix: []const u8, max_bytes: usize) ![]u8 {
        const key = try self.keyAlloc(alloc, suffix);
        defer alloc.free(key);
        return try self.readKeyBytesAllocLimited(alloc, key, max_bytes, .{});
    }

    fn readBytesAllocLimitedWithCancellation(
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

    fn verifyPortableArtifactIntegrity(
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

    fn verifyPortableArtifactIntegrityWithIdentity(
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
    fn scanNativeArtifact(
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

    fn verifyNativeArtifactIntegrity(
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

    fn verifyNativeArtifactIntegrityWithIdentity(
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

    fn copyFileVerified(
        self: *RemoteBackupStore,
        alloc: std.mem.Allocator,
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
            try ensureDirPathWithIo(self.io, parent);
        errdefer std.Io.Dir.cwd().deleteFile(self.io, destination_path) catch {};
        var destination = try fs_paths.createFilePortable(self.io, destination_path, .{ .truncate = true });
        defer destination.close(self.io);

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
            try destination.writePositionalAll(self.io, result.body, offset);
            offset += wanted;
        }
        var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
        hasher.final(&digest);
        const actual = std.fmt.bytesToHex(digest, .lower);
        if (!std.mem.eql(u8, &actual, expected_sha256))
            return error.BackupArtifactIntegrityMismatch;
        try destination.sync(self.io);
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

    fn uploadDirectoryRecursive(
        self: *RemoteBackupStore,
        alloc: std.mem.Allocator,
        src_path: []const u8,
        dest_suffix: []const u8,
        cancellation: CancellationToken,
    ) !void {
        try cancellation.check();
        try self.ensureBucketWithCancellation(cancellation);

        const io = self.io;

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

    fn downloadDirectoryRecursive(self: *RemoteBackupStore, alloc: std.mem.Allocator, src_suffix: []const u8, dest_path: []const u8) !void {
        return try self.downloadDirectoryRecursiveWithPageSizeAndCancellation(alloc, src_suffix, dest_path, 1000, .none);
    }

    fn downloadDirectoryRecursiveWithCancellation(
        self: *RemoteBackupStore,
        alloc: std.mem.Allocator,
        src_suffix: []const u8,
        dest_path: []const u8,
        cancellation: CancellationToken,
    ) !void {
        return try self.downloadDirectoryRecursiveWithPageSizeAndCancellation(
            alloc,
            src_suffix,
            dest_path,
            1000,
            cancellation,
        );
    }

    fn downloadDirectoryRecursiveWithPageSize(self: *RemoteBackupStore, alloc: std.mem.Allocator, src_suffix: []const u8, dest_path: []const u8, page_size: u32) !void {
        return try self.downloadDirectoryRecursiveWithPageSizeAndCancellation(alloc, src_suffix, dest_path, page_size, .none);
    }

    fn downloadDirectoryRecursiveWithPageSizeAndCancellation(
        self: *RemoteBackupStore,
        alloc: std.mem.Allocator,
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
            if (try self.client.getPrefixWithIo(self.io, self.bucket, key_prefix, dest_path)) |downloaded| {
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
                try self.client.getFileWithIo(self.io, self.bucket, entry.key, dest_file, .{
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
    /// True only when every writer participating in this attempt honors the
    /// durable per-table lease fence used by stale-attempt reclamation.
    writer_lease_fencing: bool = false,
    /// True only when forwarded writers also reject first lease adoption after
    /// the per-table reservation's persisted not-after deadline. This lets
    /// cleanup retire per-generation tombstones without weakening delayed-write
    /// safety or imposing an attempt-wide duration limit.
    writer_delivery_deadline_fencing: bool = false,
    tables: []const ClusterBackupAttemptTable,
};

pub const ClusterBackupAttemptState = enum {
    active,
    committed,
    failed,
};

const ClusterBackupAttemptQuarantineReason = enum {
    manifest_generation_mismatch,
};

const ClusterBackupAttemptQuarantine = struct {
    format_version: u32 = 1,
    attempt_id: []const u8,
    cluster_backup_id: []const u8,
    reason: ClusterBackupAttemptQuarantineReason,
    quarantined_at_unix_ns: u64,
    marker_sha256: []const u8,
    marker: ClusterBackupAttemptMarker,
};

pub const ClusterBackupAttemptHead = struct {
    format_version: u32 = backup_attempt_head_version,
    attempt_id: []const u8,
    state: ClusterBackupAttemptState = .active,
    /// Monotonic repository mutation sequence. Restore admission compares this
    /// before and after validation so an active attempt cannot appear and retire
    /// inside the admission window without being observed.
    generation: u64,
};

const ClusterBackupReservationLease = struct {
    attempt_id: []const u8,
    expires_at_unix_ns: u64,
};

fn backupCleanupLeaseOwner(identity: []const u8, buf: *[backup_cleanup_lease_owner_prefix.len + 64]u8) []const u8 {
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(identity, &digest, .{});
    const encoded = std.fmt.bytesToHex(digest, .lower);
    @memcpy(buf[0..backup_cleanup_lease_owner_prefix.len], backup_cleanup_lease_owner_prefix);
    @memcpy(buf[backup_cleanup_lease_owner_prefix.len..], &encoded);
    return buf;
}

fn legacyBackupCleanupLeaseOwner(
    identity: []const u8,
    buf: *[legacy_backup_cleanup_lease_owner_prefix.len + 64]u8,
) []const u8 {
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(identity, &digest, .{});
    const encoded = std.fmt.bytesToHex(digest, .lower);
    @memcpy(buf[0..legacy_backup_cleanup_lease_owner_prefix.len], legacy_backup_cleanup_lease_owner_prefix);
    @memcpy(buf[legacy_backup_cleanup_lease_owner_prefix.len..], &encoded);
    return buf;
}

fn clusterCleanupLeaseOwner(
    attempt_id: []const u8,
    buf: *[backup_cleanup_lease_owner_prefix.len + 128]u8,
) ![]const u8 {
    try validateBackupId(attempt_id);
    @memcpy(buf[0..backup_cleanup_lease_owner_prefix.len], backup_cleanup_lease_owner_prefix);
    @memcpy(buf[backup_cleanup_lease_owner_prefix.len .. backup_cleanup_lease_owner_prefix.len + attempt_id.len], attempt_id);
    return buf[0 .. backup_cleanup_lease_owner_prefix.len + attempt_id.len];
}

fn legacyClusterCleanupLeaseOwner(
    attempt_id: []const u8,
    buf: *[legacy_backup_cleanup_lease_owner_prefix.len + 128]u8,
) ![]const u8 {
    try validateBackupId(attempt_id);
    @memcpy(buf[0..legacy_backup_cleanup_lease_owner_prefix.len], legacy_backup_cleanup_lease_owner_prefix);
    @memcpy(buf[legacy_backup_cleanup_lease_owner_prefix.len .. legacy_backup_cleanup_lease_owner_prefix.len + attempt_id.len], attempt_id);
    return buf[0 .. legacy_backup_cleanup_lease_owner_prefix.len + attempt_id.len];
}

fn validateBackupLeaseOwner(owner: []const u8) !void {
    if (std.mem.startsWith(u8, owner, backup_cleanup_lease_owner_prefix)) {
        return validateBackupId(owner[backup_cleanup_lease_owner_prefix.len..]);
    }
    // Legacy cleanup owners used the public ID alphabet, so they are validated
    // as ordinary IDs. Special-casing that prefix would reject otherwise-valid
    // caller IDs such as the prefix itself and recreate the namespace bug this
    // compatibility path is intended to remove.
    return validateBackupId(owner);
}

fn reservationOwner(body: []const u8) []const u8 {
    const end = std.mem.indexOfScalar(u8, body, '\n') orelse body.len;
    return std.mem.trimEnd(u8, body[0..end], "\r");
}

fn parseClusterBackupReservationLease(body: []const u8) !ClusterBackupReservationLease {
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

fn clusterBackupLeaseReclaimable(
    expires_at_unix_ns: u64,
    now_unix_ns: u64,
) bool {
    const reclaim_after_unix_ns =
        expires_at_unix_ns +| backup_attempt_lease_clock_skew_allowance_ns;
    return now_unix_ns >= reclaim_after_unix_ns;
}

fn encodeClusterBackupReservationLease(
    alloc: std.mem.Allocator,
    attempt_id: []const u8,
    expires_at_unix_ns: u64,
) ![]u8 {
    try validateBackupLeaseOwner(attempt_id);
    if (expires_at_unix_ns == 0) return error.InvalidBackupRequest;
    return try std.fmt.allocPrint(alloc, "{s}\n{d}\n", .{
        attempt_id,
        expires_at_unix_ns,
    });
}

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
    code: ?[]const u8 = null,
    retryable: ?bool = null,
    backup_id: ?[]const u8 = null,
    artifact_backup_id: ?[]const u8 = null,
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

test "filesystem backup location returns the canonical authorized identity" {
    if (std.fs.path.sep != '/') return error.SkipZigTest;
    const alloc = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(root);
    const physical = try std.fs.path.join(alloc, &.{ root, "physical" });
    defer alloc.free(physical);
    try fs_paths.createDirPathPortable(io, physical);
    const alias = try std.fs.path.join(alloc, &.{ root, "alias" });
    defer alloc.free(alias);
    try std.Io.Dir.symLinkAbsolute(io, physical, alias, .{ .is_directory = true });

    // A connection-scoped file URI is rooted at configured_root; its URI path
    // is intentionally relative to that capability root.
    const resolved = try resolveFilesystemLocationAlloc(alloc, root, "file:///alias/new/backup", io);
    defer alloc.free(resolved);
    const expected = try std.fs.path.join(alloc, &.{ physical, "new", "backup" });
    defer alloc.free(expected);
    try std.testing.expectEqualStrings(expected, resolved);

    var opened = try openOrCreateBackupRootNoFollow(io, resolved);
    opened.close(io);
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
    return manifestExistsAtLocationWithIoAndCancellation(
        alloc,
        io,
        location,
        backup_id,
        .none,
    );
}

pub fn manifestExistsAtLocationWithIoAndCancellation(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    backup_id: []const u8,
    cancellation: CancellationToken,
) !bool {
    try cancellation.check();
    return switch (location.*) {
        .file => |backup_root| blk: {
            const path = try metadataPath(alloc, backup_root, backup_id);
            defer alloc.free(path);
            const exists = try pathExistsWithIo(io, path);
            try cancellation.check();
            break :blk exists;
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
    artifact_size_bytes: u64,
    artifact_sha256: []u8,

    fn deinit(self: PortableShard, alloc: std.mem.Allocator) void {
        alloc.free(self.shard_id);
        alloc.free(self.start_key);
        if (self.end_key) |end| alloc.free(end);
        alloc.free(self.snapshot_path);
        alloc.free(self.artifact_sha256);
    }
};

const GoPortableArtifactIntegrity = struct {
    size_bytes: u64,
    sha256: []const u8,
};

fn parseGoPortableTableManifest(
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
        // Cluster backup attempts mutate repository-global health state. A
        // single location-wide lease serializes those mutations even when
        // callers use distinct logical backup IDs.
        try std.fmt.allocPrint(alloc, "{s}/.antfly-cluster-reservation", .{backup_root})
    else
        try std.fmt.allocPrint(alloc, "{s}/{s}-reservation", .{ backup_root, backup_id });
}

fn tableBackupWriterLeasePath(
    alloc: std.mem.Allocator,
    backup_root: []const u8,
    artifact_backup_id: []const u8,
) ![]u8 {
    try validateBackupId(artifact_backup_id);
    return try std.fmt.allocPrint(alloc, "{s}/{s}{s}", .{
        backup_root,
        artifact_backup_id,
        table_backup_writer_lease_suffix,
    });
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
            var backup_dir = try openOrCreateBackupRootNoFollow(io, backup_root);
            defer backup_dir.close(io);
            if (!try writeFileToBackupRootIfAbsentLocked(
                alloc,
                io,
                backup_dir,
                trimLeftSlash(suffix),
                "reserved\n",
            )) return error.BackupAlreadyExists;
        },
        .remote => |*store| try store.writeBytesIfAbsent(
            alloc,
            trimLeftSlash(suffix),
            "reserved\n",
            "text/plain",
        ),
    }
}

/// Durably binds a logical table backup ID to the opaque artifact generation
/// before any payload can be written. Ambiguous outcomes retain this record so
/// operators and automated reconciliation can identify the exact artifacts;
/// the old `reserved\n` body remains accepted as an existence-only legacy
/// reservation.
pub fn reserveTableBackupAttemptAtLocation(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    backup_id: []const u8,
    artifact_backup_id: []const u8,
    format: BackupFormat,
    fence: TableBackupFence,
) !void {
    return reserveTableBackupAttemptAtLocationWithCancellation(
        alloc,
        io,
        location,
        backup_id,
        artifact_backup_id,
        format,
        fence,
        .none,
    );
}

pub fn reserveTableBackupAttemptAtLocationWithCancellation(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    backup_id: []const u8,
    artifact_backup_id: []const u8,
    format: BackupFormat,
    fence: TableBackupFence,
    cancellation: CancellationToken,
) !void {
    try cancellation.check();
    try validateBackupId(backup_id);
    try validateBackupId(artifact_backup_id);
    const definition_digest = std.fmt.bytesToHex(fence.definition_digest, .lower);
    const topology_digest = std.fmt.bytesToHex(fence.topology_digest, .lower);
    const encoded = try stringifyJsonAlloc(alloc, .{
        .format_version = table_backup_attempt_reservation_version,
        .created_at_unix_ns = @as(u64, @intCast(std.Io.Timestamp.now(io, .real).toNanoseconds())),
        .backup_id = backup_id,
        .artifact_backup_id = artifact_backup_id,
        .format = format,
        .metadata_group_id = fence.metadata_group_id,
        .metadata_incarnation = fence.metadata_incarnation[0..],
        .table_id = fence.table_id,
        .definition_sha256 = definition_digest[0..],
        .topology_range_count = fence.topology_range_count,
        .topology_sha256 = topology_digest[0..],
        .writer_not_after_unix_ns = fence.writer_not_after_unix_ns,
    });
    defer alloc.free(encoded);
    if (encoded.len > max_table_backup_attempt_reservation_bytes)
        return error.BackupManifestTooLarge;

    switch (location.*) {
        .file => |backup_root| {
            const suffix = try reservationPath(alloc, "", backup_id, false);
            defer alloc.free(suffix);
            var backup_dir = try openOrCreateBackupRootNoFollow(io, backup_root);
            defer backup_dir.close(io);
            if (!try writeFileToBackupRootIfAbsentLockedWithCancellation(
                alloc,
                io,
                backup_dir,
                trimLeftSlash(suffix),
                encoded,
                cancellation,
            )) return error.BackupAlreadyExists;
        },
        .remote => |*store| {
            const suffix = try reservationPath(alloc, "", backup_id, false);
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

const TableBackupAttemptReservation = struct {
    format_version: u32,
    created_at_unix_ns: ?u64 = null,
    backup_id: []const u8,
    artifact_backup_id: []const u8,
    format: BackupFormat,
    writer_not_after_unix_ns: ?u64 = null,
};

fn readTableBackupAttemptReservation(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    backup_id: []const u8,
) !?std.json.Parsed(TableBackupAttemptReservation) {
    return readTableBackupAttemptReservationWithCancellation(
        alloc,
        io,
        location,
        backup_id,
        .none,
    );
}

fn readTableBackupAttemptReservationWithCancellation(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    backup_id: []const u8,
    cancellation: CancellationToken,
) !?std.json.Parsed(TableBackupAttemptReservation) {
    try cancellation.check();
    try validateBackupId(backup_id);
    const body = switch (location.*) {
        .file => |backup_root| blk: {
            const suffix = try reservationPath(alloc, "", backup_id, false);
            defer alloc.free(suffix);
            var backup_dir = openBackupRootNoFollow(io, backup_root) catch |err| switch (err) {
                error.FileNotFound => return null,
                else => return err,
            };
            defer backup_dir.close(io);
            break :blk readFileFromBackupRootAlloc(
                alloc,
                io,
                backup_dir,
                trimLeftSlash(suffix),
                max_table_backup_attempt_reservation_bytes,
            ) catch |err| switch (err) {
                error.FileNotFound => return null,
                else => return err,
            };
        },
        .remote => |*store| blk: {
            const suffix = try reservationPath(alloc, "", backup_id, false);
            defer alloc.free(suffix);
            break :blk store.readBytesAllocLimitedWithCancellation(
                alloc,
                trimLeftSlash(suffix),
                max_table_backup_attempt_reservation_bytes,
                cancellation,
            ) catch |err| switch (err) {
                error.FileNotFound => return null,
                else => return err,
            };
        },
    };
    try cancellation.check();
    defer alloc.free(body);
    if (std.mem.eql(u8, body, "reserved\n")) return null;
    var parsed = try std.json.parseFromSlice(
        TableBackupAttemptReservation,
        alloc,
        body,
        .{ .ignore_unknown_fields = true, .allocate = .alloc_always },
    );
    errdefer parsed.deinit();
    if (parsed.value.format_version != table_backup_attempt_reservation_version or
        !std.mem.eql(u8, parsed.value.backup_id, backup_id))
        return error.InvalidBackupRequest;
    if (parsed.value.created_at_unix_ns) |created_at| if (created_at == 0)
        return error.InvalidBackupRequest;
    if (parsed.value.writer_not_after_unix_ns) |deadline| if (deadline == 0)
        return error.InvalidBackupRequest;
    try validateBackupId(parsed.value.artifact_backup_id);
    return parsed;
}

pub fn tableBackupAttemptArtifactIdAlloc(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    backup_id: []const u8,
) !?[]u8 {
    return tableBackupAttemptArtifactIdAllocWithCancellation(
        alloc,
        io,
        location,
        backup_id,
        .none,
    );
}

pub fn tableBackupAttemptArtifactIdAllocWithCancellation(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    backup_id: []const u8,
    cancellation: CancellationToken,
) !?[]u8 {
    var parsed = (try readTableBackupAttemptReservationWithCancellation(
        alloc,
        io,
        location,
        backup_id,
        cancellation,
    )) orelse return null;
    defer parsed.deinit();
    return try alloc.dupe(u8, parsed.value.artifact_backup_id);
}

pub fn tableBackupAttemptMatchesAtLocation(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    backup_id: []const u8,
    artifact_backup_id: []const u8,
) !bool {
    return tableBackupAttemptMatchesAtLocationWithCancellation(
        alloc,
        io,
        location,
        backup_id,
        artifact_backup_id,
        .none,
    );
}

pub fn tableBackupAttemptMatchesAtLocationWithCancellation(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    backup_id: []const u8,
    artifact_backup_id: []const u8,
    cancellation: CancellationToken,
) !bool {
    var parsed = (try readTableBackupAttemptReservationWithCancellation(
        alloc,
        io,
        location,
        backup_id,
        cancellation,
    )) orelse return false;
    defer parsed.deinit();
    return std.mem.eql(u8, parsed.value.artifact_backup_id, artifact_backup_id);
}

fn deleteTableBackupReservationIfArtifactOwnedAtLocation(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    backup_id: []const u8,
    artifact_backup_id: []const u8,
) !bool {
    return deleteTableBackupReservationIfArtifactOwnedAtLocationWithCancellation(
        alloc,
        io,
        location,
        backup_id,
        artifact_backup_id,
        .none,
    );
}

fn deleteTableBackupReservationIfArtifactOwnedAtLocationWithCancellation(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    backup_id: []const u8,
    artifact_backup_id: []const u8,
    cancellation: CancellationToken,
) !bool {
    try cancellation.check();
    try validateBackupId(backup_id);
    try validateBackupId(artifact_backup_id);
    const suffix = try reservationPath(alloc, "", backup_id, false);
    defer alloc.free(suffix);
    return switch (location.*) {
        .file => |backup_root| blk: {
            var backup_dir = openBackupRootNoFollow(io, backup_root) catch |err| switch (err) {
                error.FileNotFound => break :blk false,
                else => return err,
            };
            defer backup_dir.close(io);
            const relative_path = trimLeftSlash(suffix);
            const lock_path = try std.fmt.allocPrint(alloc, "{s}.publish.lock", .{relative_path});
            defer alloc.free(lock_path);
            var lock_file = try openOrCreateBackupLockFile(io, backup_dir, lock_path);
            defer lock_file.close(io);
            try lockFileExclusiveWithCancellation(io, lock_file, cancellation);
            defer lock_file.unlock(io);
            const body = readFileFromBackupRootAlloc(
                alloc,
                io,
                backup_dir,
                relative_path,
                max_table_backup_attempt_reservation_bytes,
            ) catch |err| switch (err) {
                error.FileNotFound => break :blk false,
                else => return err,
            };
            defer alloc.free(body);
            if (std.mem.eql(u8, body, "reserved\n")) break :blk false;
            var parsed = try std.json.parseFromSlice(
                TableBackupAttemptReservation,
                alloc,
                body,
                .{ .ignore_unknown_fields = true, .allocate = .alloc_always },
            );
            defer parsed.deinit();
            if (!std.mem.eql(u8, parsed.value.backup_id, backup_id) or
                !std.mem.eql(u8, parsed.value.artifact_backup_id, artifact_backup_id))
                break :blk false;
            try cancellation.check();
            try deleteFileDurablyFromBackupRoot(io, backup_dir, relative_path);
            break :blk true;
        },
        .remote => |*store| blk: {
            const key = try store.keyAlloc(alloc, trimLeftSlash(suffix));
            defer alloc.free(key);
            var current = store.client.getObject(store.bucket, key, .{
                .range = .{ .offset = 0, .length = max_table_backup_attempt_reservation_bytes },
                .skip_metadata_probe = true,
                .max_response_bytes = max_table_backup_attempt_reservation_bytes,
                .cancellation = objectCancellationToken(cancellation),
            }) catch |err| switch (err) {
                error.FileNotFound => break :blk false,
                else => return err,
            };
            defer current.deinit(alloc);
            var parsed = try std.json.parseFromSlice(
                TableBackupAttemptReservation,
                alloc,
                current.body,
                .{ .ignore_unknown_fields = true, .allocate = .alloc_always },
            );
            defer parsed.deinit();
            if (!std.mem.eql(u8, parsed.value.backup_id, backup_id) or
                !std.mem.eql(u8, parsed.value.artifact_backup_id, artifact_backup_id))
                break :blk false;
            const etag = current.metadata.etag orelse
                return error.BackupReservationIdentityUnavailable;
            store.client.deleteObject(store.bucket, key, .{
                .if_match_etag = etag,
                .cancellation = objectCancellationToken(cancellation),
            }) catch |err| switch (err) {
                error.FileNotFound, error.PreconditionFailed => break :blk false,
                else => return err,
            };
            break :blk true;
        },
    };
}

/// Creates the storage-writer liveness fence before a request can be delivered
/// to another node. The artifact generation is globally unique to the attempt,
/// so it is also the immutable lease owner.
pub fn reserveTableBackupWriterLeaseAtLocation(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    artifact_backup_id: []const u8,
    expires_at_unix_ns: u64,
) !void {
    return reserveTableBackupWriterLeaseAtLocationWithCancellation(
        alloc,
        io,
        location,
        artifact_backup_id,
        expires_at_unix_ns,
        .none,
    );
}

pub fn reserveTableBackupWriterLeaseAtLocationWithCancellation(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    artifact_backup_id: []const u8,
    expires_at_unix_ns: u64,
    cancellation: CancellationToken,
) !void {
    try cancellation.check();
    const lease = try encodeClusterBackupReservationLease(
        alloc,
        artifact_backup_id,
        expires_at_unix_ns,
    );
    defer alloc.free(lease);
    const suffix = try tableBackupWriterLeasePath(alloc, "", artifact_backup_id);
    defer alloc.free(suffix);
    switch (location.*) {
        .file => |backup_root| {
            var backup_dir = try openOrCreateBackupRootNoFollow(io, backup_root);
            defer backup_dir.close(io);
            try ensureBackupRelativeParentNoFollow(
                io,
                backup_dir,
                trimLeftSlash(suffix),
            );
            if (!try writeFileToBackupRootIfAbsentLockedWithCancellation(
                alloc,
                io,
                backup_dir,
                trimLeftSlash(suffix),
                lease,
                cancellation,
            )) return error.BackupAlreadyExists;
        },
        .remote => |*store| try store.writeBytesIfAbsentWithCancellation(
            alloc,
            trimLeftSlash(suffix),
            lease,
            "text/plain",
            cancellation,
        ),
    }
}

/// Renews an existing writer fence. Forwarded storage owners use this to adopt
/// the coordinator-created lease; they never recreate a lease that cleanup has
/// already claimed.
pub fn renewTableBackupWriterLeaseAtLocation(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    artifact_backup_id: []const u8,
    expires_at_unix_ns: u64,
) !bool {
    return renewTableBackupWriterLeaseAtLocationWithCancellation(
        alloc,
        io,
        location,
        artifact_backup_id,
        expires_at_unix_ns,
        .none,
    );
}

pub fn renewTableBackupWriterLeaseAtLocationWithCancellation(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    artifact_backup_id: []const u8,
    expires_at_unix_ns: u64,
    cancellation: CancellationToken,
) !bool {
    try cancellation.check();
    const lease = try encodeClusterBackupReservationLease(
        alloc,
        artifact_backup_id,
        expires_at_unix_ns,
    );
    defer alloc.free(lease);
    const suffix = try tableBackupWriterLeasePath(alloc, "", artifact_backup_id);
    defer alloc.free(suffix);
    return switch (location.*) {
        .file => |backup_root| blk: {
            var backup_dir = try openBackupRootNoFollow(io, backup_root);
            defer backup_dir.close(io);
            const relative_path = trimLeftSlash(suffix);
            const lock_path = try std.fmt.allocPrint(alloc, "{s}.publish.lock", .{relative_path});
            defer alloc.free(lock_path);
            var lock_file = try openOrCreateBackupLockFile(io, backup_dir, lock_path);
            defer lock_file.close(io);
            try lockFileExclusiveWithCancellation(io, lock_file, cancellation);
            defer lock_file.unlock(io);
            const body = readFileFromBackupRootAlloc(
                alloc,
                io,
                backup_dir,
                relative_path,
                max_backup_attempt_lease_bytes,
            ) catch |err| switch (err) {
                error.FileNotFound => break :blk false,
                else => return err,
            };
            defer alloc.free(body);
            if (!std.mem.eql(u8, reservationOwner(body), artifact_backup_id))
                break :blk false;
            try replaceFileInBackupRootUnderHeldLockWithHookAndCancellation(
                alloc,
                io,
                backup_dir,
                relative_path,
                lease,
                null,
                cancellation,
            );
            break :blk true;
        },
        .remote => |*store| blk: {
            if (try store.replaceBytesIfOwnedWithCancellation(
                alloc,
                trimLeftSlash(suffix),
                artifact_backup_id,
                lease,
                "text/plain",
                cancellation,
            )) break :blk true;
            // The forwarding coordinator and storage owner deliberately share
            // this attempt identity. A same-owner CAS winner is a successful
            // renewal, not fencing; only absence or a different owner loses it.
            break :blk (try store.suffixOwnerMatchesWithCancellation(
                alloc,
                trimLeftSlash(suffix),
                artifact_backup_id,
                cancellation,
            )) == true;
        },
    };
}

pub fn releaseTableBackupWriterLeaseAtLocation(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    artifact_backup_id: []const u8,
) !bool {
    return releaseTableBackupWriterLeaseAtLocationWithCancellation(
        alloc,
        io,
        location,
        artifact_backup_id,
        .none,
    );
}

pub fn releaseTableBackupWriterLeaseAtLocationWithCancellation(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    artifact_backup_id: []const u8,
    cancellation: CancellationToken,
) !bool {
    return (try retireTableBackupWriterLeaseAtLocationWithCancellation(
        alloc,
        io,
        location,
        artifact_backup_id,
        cancellation,
    )) == .retired;
}

/// The four terminal observations of a conditional writer-lease retirement.
/// Callers that guard deletion authority must distinguish an already-absent
/// lease from a lease that exists under another owner or changed after it was
/// read; collapsing all four states to `false` can discard the only durable
/// retry address while a writer is still live.
pub const TableBackupWriterLeaseRetirement = enum {
    absent,
    retired,
    different_owner,
    raced,
};

pub fn retireTableBackupWriterLeaseAtLocationWithCancellation(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    artifact_backup_id: []const u8,
    cancellation: CancellationToken,
) !TableBackupWriterLeaseRetirement {
    return retireTableBackupLeaseIfOwnedByAnyAtLocationWithRoot(
        alloc,
        io,
        location,
        artifact_backup_id,
        &.{artifact_backup_id},
        null,
        cancellation,
    );
}

fn releaseTableBackupLeaseIfOwnedAtLocationWithCancellation(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    artifact_backup_id: []const u8,
    expected_owner: []const u8,
    cancellation: CancellationToken,
) !bool {
    return releaseTableBackupLeaseIfOwnedByAnyAtLocationWithRoot(
        alloc,
        io,
        location,
        artifact_backup_id,
        &.{expected_owner},
        null,
        cancellation,
    );
}

fn releaseTableBackupLeaseIfOwnedAtLocation(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    artifact_backup_id: []const u8,
    expected_owner: []const u8,
) !bool {
    return releaseTableBackupLeaseIfOwnedAtLocationWithCancellation(
        alloc,
        io,
        location,
        artifact_backup_id,
        expected_owner,
        .none,
    );
}

fn tableBackupLeaseOwnerAllowed(body: []const u8, expected_owners: []const []const u8) bool {
    const actual_owner = reservationOwner(body);
    for (expected_owners) |expected_owner| {
        if (std.mem.eql(u8, actual_owner, expected_owner)) return true;
    }
    return false;
}

/// Reads the single mutable lease object once and conditionally deletes any of
/// the terminal owner spellings. The owners are mutually exclusive, so probing
/// them with separate GETs adds latency without improving fencing.
fn releaseTableBackupLeaseIfOwnedByAnyAtLocation(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    artifact_backup_id: []const u8,
    expected_owners: []const []const u8,
) !bool {
    return releaseTableBackupLeaseIfOwnedByAnyAtLocationWithRoot(
        alloc,
        io,
        location,
        artifact_backup_id,
        expected_owners,
        null,
        .none,
    );
}

fn releaseTableBackupLeaseIfOwnedByAnyAtLocationWithRoot(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    artifact_backup_id: []const u8,
    expected_owners: []const []const u8,
    pinned_root: ?std.Io.Dir,
    cancellation: CancellationToken,
) !bool {
    return (try retireTableBackupLeaseIfOwnedByAnyAtLocationWithRoot(
        alloc,
        io,
        location,
        artifact_backup_id,
        expected_owners,
        pinned_root,
        cancellation,
    )) == .retired;
}

fn retireTableBackupLeaseIfOwnedByAnyAtLocationWithRoot(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    artifact_backup_id: []const u8,
    expected_owners: []const []const u8,
    pinned_root: ?std.Io.Dir,
    cancellation: CancellationToken,
) !TableBackupWriterLeaseRetirement {
    try cancellation.check();
    if (expected_owners.len == 0) return .different_owner;
    const suffix = try tableBackupWriterLeasePath(alloc, "", artifact_backup_id);
    defer alloc.free(suffix);
    return switch (location.*) {
        .file => |backup_root| blk: {
            var root = try accessBackupRoot(io, backup_root, pinned_root, false);
            defer root.deinit(io);
            const backup_dir = root.dir;
            const relative_path = trimLeftSlash(suffix);
            const lock_path = try std.fmt.allocPrint(alloc, "{s}.publish.lock", .{relative_path});
            defer alloc.free(lock_path);
            var lock_file = try openOrCreateBackupLockFile(io, backup_dir, lock_path);
            defer lock_file.close(io);
            try lockFileExclusiveWithCancellation(io, lock_file, cancellation);
            defer lock_file.unlock(io);
            const body = readFileFromBackupRootAlloc(
                alloc,
                io,
                backup_dir,
                relative_path,
                max_backup_attempt_lease_bytes,
            ) catch |err| switch (err) {
                error.FileNotFound => break :blk .absent,
                else => return err,
            };
            defer alloc.free(body);
            if (!tableBackupLeaseOwnerAllowed(body, expected_owners))
                break :blk .different_owner;
            try cancellation.check();
            try deleteFileDurablyFromBackupRoot(io, backup_dir, relative_path);
            break :blk .retired;
        },
        .remote => |*store| blk: {
            const key = try store.keyAlloc(alloc, trimLeftSlash(suffix));
            defer alloc.free(key);
            var current = store.client.getObject(store.bucket, key, .{
                .range = .{ .offset = 0, .length = max_backup_attempt_lease_bytes },
                .skip_metadata_probe = true,
                .max_response_bytes = max_backup_attempt_lease_bytes,
                .cancellation = objectCancellationToken(cancellation),
            }) catch |err| switch (err) {
                error.FileNotFound => break :blk .absent,
                else => return err,
            };
            defer current.deinit(alloc);
            if (!tableBackupLeaseOwnerAllowed(current.body, expected_owners))
                break :blk .different_owner;
            const etag = current.metadata.etag orelse
                return error.BackupReservationIdentityUnavailable;
            store.client.deleteObject(store.bucket, key, .{
                .if_match_etag = etag,
                .cancellation = objectCancellationToken(cancellation),
            }) catch |err| switch (err) {
                error.FileNotFound => break :blk .absent,
                error.PreconditionFailed => break :blk .raced,
                else => return err,
            };
            break :blk .retired;
        },
    };
}

fn releaseTableBackupCleanupFenceWithBudget(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    artifact_backup_id: []const u8,
    operation_budget: *usize,
) !void {
    return releaseTableBackupCleanupFenceWithRootBudgetAndCancellation(
        alloc,
        io,
        location,
        artifact_backup_id,
        operation_budget,
        null,
        .none,
    );
}

fn releaseTableBackupCleanupFenceWithRootBudget(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    artifact_backup_id: []const u8,
    operation_budget: *usize,
    pinned_root: ?std.Io.Dir,
) !void {
    return releaseTableBackupCleanupFenceWithRootBudgetAndCancellation(
        alloc,
        io,
        location,
        artifact_backup_id,
        operation_budget,
        pinned_root,
        .none,
    );
}

fn releaseTableBackupCleanupFenceWithRootBudgetAndCancellation(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    artifact_backup_id: []const u8,
    operation_budget: *usize,
    pinned_root: ?std.Io.Dir,
    cancellation: CancellationToken,
) !void {
    try cancellation.check();
    const cost: usize = switch (location.*) {
        .file => 1,
        // Both cleanup-owner spellings share one mutable object, so one read
        // and at most one conditional delete covers rolling upgrades.
        .remote => 2,
    };
    if (operation_budget.* < cost) return error.BackupCleanupBudgetExceeded;
    operation_budget.* -= cost;
    var cleanup_owner_buf: [backup_cleanup_lease_owner_prefix.len + 64]u8 = undefined;
    const cleanup_owner = backupCleanupLeaseOwner(artifact_backup_id, &cleanup_owner_buf);
    var legacy_cleanup_owner_buf: [legacy_backup_cleanup_lease_owner_prefix.len + 64]u8 = undefined;
    const legacy_cleanup_owner = legacyBackupCleanupLeaseOwner(
        artifact_backup_id,
        &legacy_cleanup_owner_buf,
    );
    _ = try releaseTableBackupLeaseIfOwnedByAnyAtLocationWithRoot(
        alloc,
        io,
        location,
        artifact_backup_id,
        &.{ cleanup_owner, legacy_cleanup_owner },
        pinned_root,
        cancellation,
    );
}

fn releaseTableBackupWriterStateWithBudget(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    artifact_backup_id: []const u8,
    operation_budget: *usize,
) !void {
    return releaseTableBackupWriterStateWithRootBudget(
        alloc,
        io,
        location,
        artifact_backup_id,
        operation_budget,
        null,
    );
}

fn releaseTableBackupWriterStateWithRootBudget(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    artifact_backup_id: []const u8,
    operation_budget: *usize,
    pinned_root: ?std.Io.Dir,
) !void {
    const cost: usize = switch (location.*) {
        // All owner spellings share one object: one read and at most one
        // conditional delete retires the complete committed state.
        .remote => 2,
        .file => 1,
    };
    if (operation_budget.* < cost)
        return error.BackupCleanupBudgetExceeded;
    operation_budget.* -= cost;
    var cleanup_owner_buf: [backup_cleanup_lease_owner_prefix.len + 64]u8 = undefined;
    const cleanup_owner = backupCleanupLeaseOwner(artifact_backup_id, &cleanup_owner_buf);
    var legacy_cleanup_owner_buf: [legacy_backup_cleanup_lease_owner_prefix.len + 64]u8 = undefined;
    const legacy_cleanup_owner = legacyBackupCleanupLeaseOwner(
        artifact_backup_id,
        &legacy_cleanup_owner_buf,
    );
    _ = try releaseTableBackupLeaseIfOwnedByAnyAtLocationWithRoot(
        alloc,
        io,
        location,
        artifact_backup_id,
        &.{ artifact_backup_id, cleanup_owner, legacy_cleanup_owner },
        pinned_root,
        .none,
    );
}

/// Retires every owner spelling for an artifact generation after its canonical
/// manifest is durable. This treats an absent writer owner and a cleanup
/// tombstone installed by a publication race as equally successful terminal
/// states, while surfacing storage failures for durable retry.
pub fn releaseCommittedTableBackupWriterStateAtLocation(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    artifact_backup_id: []const u8,
) !void {
    var operation_budget: usize = 2;
    try releaseTableBackupWriterStateWithBudget(
        alloc,
        io,
        location,
        artifact_backup_id,
        &operation_budget,
    );
}

/// Retries sidecar retirement after the canonical table manifest is already
/// durable. Logical reservations use an opaque, distinct artifact generation;
/// a same-ID reservation is a forwarded envelope whose coordinator may still
/// need the shared writer lease to publish its canonical manifest.
pub fn reconcileCommittedTableBackupWriterStateAtLocation(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    backup_id: []const u8,
) !bool {
    if (!try tableManifestRecordExistsForReconciliation(alloc, io, location, backup_id))
        return false;
    var parsed = (try readTableBackupAttemptReservation(
        alloc,
        io,
        location,
        backup_id,
    )) orelse return false;
    defer parsed.deinit();
    if (std.mem.eql(u8, backup_id, parsed.value.artifact_backup_id))
        return false;
    try releaseCommittedTableBackupWriterStateAtLocation(
        alloc,
        io,
        location,
        parsed.value.artifact_backup_id,
    );
    return true;
}

/// Returns true only after proving that no live storage writer owns this
/// artifact. An expired or absent lease is atomically replaced by a cleanup
/// tombstone so a delayed forwarded request cannot recreate or renew it after
/// cleanup begins.
fn claimExpiredTableBackupWriterLeaseAtLocation(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    artifact_backup_id: []const u8,
    now_unix_ns: u64,
) !BackupLeaseFenceClaim {
    return claimExpiredTableBackupWriterLeaseAtLocationWithRootAndCancellation(
        alloc,
        io,
        location,
        artifact_backup_id,
        now_unix_ns,
        null,
        .none,
    );
}

fn claimExpiredTableBackupWriterLeaseAtLocationWithRoot(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    artifact_backup_id: []const u8,
    now_unix_ns: u64,
    pinned_root: ?std.Io.Dir,
) !BackupLeaseFenceClaim {
    return claimExpiredTableBackupWriterLeaseAtLocationWithRootAndCancellation(
        alloc,
        io,
        location,
        artifact_backup_id,
        now_unix_ns,
        pinned_root,
        .none,
    );
}

fn claimExpiredTableBackupWriterLeaseAtLocationWithRootAndCancellation(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    artifact_backup_id: []const u8,
    now_unix_ns: u64,
    pinned_root: ?std.Io.Dir,
    cancellation: CancellationToken,
) !BackupLeaseFenceClaim {
    try cancellation.check();
    var cleanup_owner_buf: [backup_cleanup_lease_owner_prefix.len + 64]u8 = undefined;
    const cleanup_owner = backupCleanupLeaseOwner(artifact_backup_id, &cleanup_owner_buf);
    var legacy_cleanup_owner_buf: [legacy_backup_cleanup_lease_owner_prefix.len + 64]u8 = undefined;
    const legacy_cleanup_owner = legacyBackupCleanupLeaseOwner(artifact_backup_id, &legacy_cleanup_owner_buf);
    const cleanup_lease = try encodeClusterBackupReservationLease(
        alloc,
        cleanup_owner,
        std.math.maxInt(u64),
    );
    defer alloc.free(cleanup_lease);
    const suffix = try tableBackupWriterLeasePath(alloc, "", artifact_backup_id);
    defer alloc.free(suffix);
    return switch (location.*) {
        .file => |backup_root| blk: {
            var root = try accessBackupRoot(io, backup_root, pinned_root, true);
            defer root.deinit(io);
            const backup_dir = root.dir;
            const relative_path = trimLeftSlash(suffix);
            const lock_path = try std.fmt.allocPrint(alloc, "{s}.publish.lock", .{relative_path});
            defer alloc.free(lock_path);
            var lock_file = try openOrCreateBackupLockFile(io, backup_dir, lock_path);
            defer lock_file.close(io);
            try lockFileExclusiveWithCancellation(io, lock_file, cancellation);
            defer lock_file.unlock(io);
            const body = readFileFromBackupRootAlloc(
                alloc,
                io,
                backup_dir,
                relative_path,
                max_backup_attempt_lease_bytes,
            ) catch |err| switch (err) {
                error.FileNotFound => {
                    try cancellation.check();
                    try replaceFileInBackupRootUnderHeldLock(
                        alloc,
                        io,
                        backup_dir,
                        relative_path,
                        cleanup_lease,
                    );
                    break :blk .claimed;
                },
                else => return err,
            };
            defer alloc.free(body);
            const lease = parseClusterBackupReservationLease(body) catch break :blk .active;
            if (std.mem.eql(u8, lease.attempt_id, cleanup_owner)) break :blk .claimed;
            if (std.mem.eql(u8, lease.attempt_id, legacy_cleanup_owner)) {
                try cancellation.check();
                try replaceFileInBackupRootUnderHeldLock(alloc, io, backup_dir, relative_path, cleanup_lease);
                break :blk .claimed;
            }
            if (!std.mem.eql(u8, lease.attempt_id, artifact_backup_id) or
                !clusterBackupLeaseReclaimable(lease.expires_at_unix_ns, now_unix_ns))
                break :blk .active;
            try cancellation.check();
            try replaceFileInBackupRootUnderHeldLock(alloc, io, backup_dir, relative_path, cleanup_lease);
            break :blk .claimed;
        },
        .remote => |*store| try store.claimExpiredLeaseWithFenceAndCancellation(
            alloc,
            trimLeftSlash(suffix),
            artifact_backup_id,
            cleanup_owner,
            legacy_cleanup_owner,
            now_unix_ns,
            cleanup_lease,
            cancellation,
        ),
    };
}

pub const TableBackupAttemptReclaimResult = enum { active, committed, reclaimed };

/// Immutable identity and timing fields from one reservation read. Callers use
/// this same generation for the response receipt, admission decision, and
/// reclamation attempt so a concurrently replaced reservation cannot splice
/// together observations from two backup attempts.
pub const TableBackupAttemptInspection = struct {
    artifact_backup_id: []u8,
    format: BackupFormat,
    created_at_unix_ns: ?u64,
    writer_not_after_unix_ns: ?u64,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.artifact_backup_id);
        self.* = undefined;
    }

    pub fn reclaimNotBeforeUnixNs(self: *const @This()) ?u64 {
        const created_at = self.created_at_unix_ns orelse return null;
        return created_at +| backup_attempt_reclaim_age_ns +|
            backup_attempt_lease_clock_skew_allowance_ns;
    }
};

pub const TableBackupAttemptReclaimProgress = enum {
    active,
    committed,
    reclaimed,
    incomplete,
    replaced,
};

/// Reads and owns one exact reservation generation. The read participates in
/// the caller's complete operation quantum instead of hiding unbounded control
/// I/O outside the cleanup budget.
pub fn inspectTableBackupAttemptAtLocationWithBudgetAndCancellation(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    backup_id: []const u8,
    operation_budget: *usize,
    cancellation: CancellationToken,
) !?TableBackupAttemptInspection {
    try cancellation.check();
    try consumeBackupCleanupOperation(operation_budget);
    var parsed = (try readTableBackupAttemptReservationWithCancellation(
        alloc,
        io,
        location,
        backup_id,
        cancellation,
    )) orelse return null;
    defer parsed.deinit();
    return .{
        .artifact_backup_id = try alloc.dupe(u8, parsed.value.artifact_backup_id),
        .format = parsed.value.format,
        .created_at_unix_ns = parsed.value.created_at_unix_ns,
        .writer_not_after_unix_ns = parsed.value.writer_not_after_unix_ns,
    };
}

fn tableManifestRecordExistsForReconciliation(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    backup_id: []const u8,
) !bool {
    return tableManifestRecordExistsForReconciliationWithCancellation(
        alloc,
        io,
        location,
        backup_id,
        .none,
    );
}

fn tableManifestRecordExistsForReconciliationWithCancellation(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    backup_id: []const u8,
    cancellation: CancellationToken,
) !bool {
    try cancellation.check();
    return switch (location.*) {
        .file => |backup_root| blk: {
            const path = try metadataPath(alloc, backup_root, backup_id);
            defer alloc.free(path);
            const exists = try pathExistsWithIo(io, path);
            try cancellation.check();
            break :blk exists;
        },
        .remote => |*store| blk: {
            const suffix = try metadataPath(alloc, "", backup_id);
            defer alloc.free(suffix);
            const body = store.readBytesAllocLimitedWithCancellation(
                alloc,
                trimLeftSlash(suffix),
                1,
                cancellation,
            ) catch |err| switch (err) {
                error.FileNotFound => break :blk false,
                // An oversized or corrupt commit record is still a commit
                // record. Cleanup must fail closed and preserve its payload.
                error.BackupManifestTooLarge, error.StreamTooLong => break :blk true,
                else => return err,
            };
            alloc.free(body);
            break :blk true;
        },
    };
}

/// Checks the canonical commit point while charging the same bounded quantum
/// as reservation inspection and cleanup. Collision handling uses this after
/// reading an exact generation so a durable winner is reported as committed
/// instead of as an ambiguous in-flight attempt.
pub fn tableBackupAttemptCommittedAtLocationWithBudgetAndCancellation(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    backup_id: []const u8,
    operation_budget: *usize,
    cancellation: CancellationToken,
) !bool {
    try cancellation.check();
    try consumeBackupCleanupOperation(operation_budget);
    return try tableManifestRecordExistsForReconciliationWithCancellation(
        alloc,
        io,
        location,
        backup_id,
        cancellation,
    );
}

fn reserveReclaimOperations(
    location: *const BackupLocation,
    operation_budget: *usize,
) !void {
    const cost: usize = switch (location.*) {
        .file, .remote => 2,
    };
    if (operation_budget.* < cost) return error.BackupCleanupBudgetExceeded;
    operation_budget.* -= cost;
}

/// Advances exactly the inspected reservation generation. Budget exhaustion is
/// normal resumable progress; replacement is also explicit so a foreground
/// caller can reread the new generation before reporting an artifact identity.
pub fn advanceExpiredTableBackupAttemptAtLocationWithCancellation(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    backup_id: []const u8,
    inspection: *const TableBackupAttemptInspection,
    now_unix_ns: u64,
    operation_budget: *usize,
    cancellation: CancellationToken,
) !TableBackupAttemptReclaimProgress {
    try cancellation.check();
    const eligible_at = inspection.reclaimNotBeforeUnixNs() orelse return .active;
    if (now_unix_ns < eligible_at) return .active;

    reserveReclaimOperations(location, operation_budget) catch return .incomplete;
    const lease_claim = try claimExpiredTableBackupWriterLeaseAtLocationWithRootAndCancellation(
        alloc,
        io,
        location,
        inspection.artifact_backup_id,
        now_unix_ns,
        null,
        cancellation,
    );
    if (lease_claim == .active) return .active;

    // Revalidate the logical generation after fencing its artifact. Another
    // reconciler may have completed A and admitted B while this worker waited.
    consumeBackupCleanupOperation(operation_budget) catch return .incomplete;
    if (!try tableBackupAttemptMatchesAtLocationWithCancellation(
        alloc,
        io,
        location,
        backup_id,
        inspection.artifact_backup_id,
        cancellation,
    )) {
        // The generation is no longer the logical owner. Current writers also
        // carry a delivery deadline, so once that deadline has passed the
        // cleanup tombstone installed above has no remaining safety role. Do
        // not orphan one lease object per replacement race. Legacy attempts
        // have no such proof and deliberately retain their permanent fence.
        if (inspection.writer_not_after_unix_ns) |writer_not_after_unix_ns| {
            if (now_unix_ns >= writer_not_after_unix_ns) {
                releaseTableBackupCleanupFenceWithRootBudgetAndCancellation(
                    alloc,
                    io,
                    location,
                    inspection.artifact_backup_id,
                    operation_budget,
                    null,
                    cancellation,
                ) catch |err| switch (err) {
                    error.BackupCleanupBudgetExceeded => return .incomplete,
                    else => return err,
                };
            }
        }
        return .replaced;
    }

    try consumeBackupCleanupOperation(operation_budget);
    if (try tableManifestRecordExistsForReconciliationWithCancellation(
        alloc,
        io,
        location,
        backup_id,
        cancellation,
    )) {
        releaseTableBackupCleanupFenceWithRootBudgetAndCancellation(
            alloc,
            io,
            location,
            inspection.artifact_backup_id,
            operation_budget,
            null,
            cancellation,
        ) catch |err| switch (err) {
            error.BackupCleanupBudgetExceeded => return .incomplete,
            else => return err,
        };
        return .committed;
    }

    cleanupTableBackupAttemptAtLocationWithBudgetAndCancellation(
        alloc,
        io,
        location,
        backup_id,
        inspection.artifact_backup_id,
        inspection.format,
        operation_budget,
        false,
        false,
        cancellation,
    ) catch |err| switch (err) {
        error.BackupCleanupBudgetExceeded => return .incomplete,
        else => return err,
    };
    if (inspection.writer_not_after_unix_ns) |writer_not_after_unix_ns| {
        if (now_unix_ns >= writer_not_after_unix_ns) {
            releaseTableBackupCleanupFenceWithRootBudgetAndCancellation(
                alloc,
                io,
                location,
                inspection.artifact_backup_id,
                operation_budget,
                null,
                cancellation,
            ) catch |err| switch (err) {
                error.BackupCleanupBudgetExceeded => return .incomplete,
                else => return err,
            };
        }
    }
    const reservation_cost: usize = switch (location.*) {
        .file => 1,
        .remote => 2,
    };
    if (operation_budget.* < reservation_cost) return .incomplete;
    operation_budget.* -= reservation_cost;
    const deleted = try deleteTableBackupReservationIfArtifactOwnedAtLocationWithCancellation(
        alloc,
        io,
        location,
        backup_id,
        inspection.artifact_backup_id,
        cancellation,
    );
    return if (deleted) .reclaimed else .replaced;
}

/// Reconciles one stale standalone attempt without racing a delayed current
/// writer. Current reservations carry a timestamp while retaining format
/// version 1 so older nodes can still read them during a rolling upgrade. Every
/// current writer verifies that the reservation still owns its artifact after
/// acquiring the lease. A legacy attempt with no timestamp and no lease remains
/// conservatively retained because an older writer may still be live.
pub fn reclaimExpiredTableBackupAttemptAtLocation(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    backup_id: []const u8,
    now_unix_ns: u64,
) !TableBackupAttemptReclaimResult {
    var object_budget: usize = backup_attempt_request_reclaim_object_budget;
    var inspection = (try inspectTableBackupAttemptAtLocationWithBudgetAndCancellation(
        alloc,
        io,
        location,
        backup_id,
        &object_budget,
        .none,
    )) orelse return .active;
    defer inspection.deinit(alloc);
    const progress = try advanceExpiredTableBackupAttemptAtLocationWithCancellation(
        alloc,
        io,
        location,
        backup_id,
        &inspection,
        now_unix_ns,
        &object_budget,
        .none,
    );
    return switch (progress) {
        .active => .active,
        .committed => .committed,
        .reclaimed, .replaced => .reclaimed,
        .incomplete => error.BackupCleanupBudgetExceeded,
    };
}

const ClusterAttemptCleanupPhase = enum {
    writer_leases,
    artifacts,
    committed_writer_state,
};

const ClusterWriterLeaseScanCursor = struct {
    format_version: u32 = 1,
    attempt_id: []const u8,
    phase: ClusterAttemptCleanupPhase = .writer_leases,
    next_table_index: usize,
};

const ClusterAttemptCleanupProgress = struct {
    phase: ClusterAttemptCleanupPhase = .writer_leases,
    next_table_index: usize = 0,
};

fn cleanupProgressLessThan(a: ClusterAttemptCleanupProgress, b: ClusterAttemptCleanupProgress) bool {
    const a_phase = @intFromEnum(a.phase);
    const b_phase = @intFromEnum(b.phase);
    return a_phase < b_phase or (a_phase == b_phase and a.next_table_index < b.next_table_index);
}

fn parseClusterAttemptCleanupProgress(
    alloc: std.mem.Allocator,
    body: []const u8,
    marker: *const ClusterBackupAttemptMarker,
) ClusterAttemptCleanupProgress {
    var parsed = std.json.parseFromSlice(
        ClusterWriterLeaseScanCursor,
        alloc,
        body,
        .{ .allocate = .alloc_always },
    ) catch return .{};
    defer parsed.deinit();
    if (parsed.value.format_version != 1 or
        !std.mem.eql(u8, parsed.value.attempt_id, marker.attempt_id) or
        parsed.value.next_table_index > marker.tables.len)
        return .{};
    return .{
        .phase = parsed.value.phase,
        .next_table_index = parsed.value.next_table_index,
    };
}

fn clusterWriterLeaseScanCursorPath(
    alloc: std.mem.Allocator,
    backup_root: []const u8,
    attempt_id: []const u8,
) ![]u8 {
    try validateBackupId(attempt_id);
    return try std.fmt.allocPrint(alloc, "{s}/{s}/{s}{s}", .{
        backup_root,
        incomplete_backup_prefix,
        attempt_id,
        cluster_writer_lease_scan_cursor_suffix,
    });
}

fn clusterBackupAttemptQuarantinePath(
    alloc: std.mem.Allocator,
    backup_root: []const u8,
    attempt_id: []const u8,
) ![]u8 {
    try validateBackupId(attempt_id);
    return try std.fmt.allocPrint(alloc, "{s}/{s}/{s}{s}", .{
        backup_root,
        backup_attempt_quarantine_prefix,
        attempt_id,
        cluster_backup_attempt_quarantine_suffix,
    });
}

fn legacyClusterBackupAttemptQuarantinePath(
    alloc: std.mem.Allocator,
    backup_root: []const u8,
    attempt_id: []const u8,
) ![]u8 {
    try validateBackupId(attempt_id);
    return try std.fmt.allocPrint(alloc, "{s}/{s}/{s}{s}", .{
        backup_root,
        incomplete_backup_prefix,
        attempt_id,
        cluster_backup_attempt_quarantine_suffix,
    });
}

fn consumeBackupCleanupOperation(operation_budget: *usize) !void {
    if (operation_budget.* == 0) return error.BackupCleanupBudgetExceeded;
    operation_budget.* -= 1;
}

fn readClusterWriterLeaseScanCursorWithBudget(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    marker: *const ClusterBackupAttemptMarker,
    remote_operation_budget: ?*usize,
) !ClusterAttemptCleanupProgress {
    return readClusterWriterLeaseScanCursorWithRootBudget(
        alloc,
        io,
        location,
        marker,
        remote_operation_budget,
        null,
    );
}

fn readClusterWriterLeaseScanCursorWithRootBudget(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    marker: *const ClusterBackupAttemptMarker,
    remote_operation_budget: ?*usize,
    pinned_root: ?std.Io.Dir,
) !ClusterAttemptCleanupProgress {
    const suffix = try clusterWriterLeaseScanCursorPath(alloc, "", marker.attempt_id);
    defer alloc.free(suffix);
    const body = switch (location.*) {
        .file => |backup_root| blk: {
            var root = accessBackupRoot(io, backup_root, pinned_root, false) catch |err| switch (err) {
                error.FileNotFound => return .{},
                else => return err,
            };
            defer root.deinit(io);
            break :blk readFileFromBackupRootAlloc(
                alloc,
                io,
                root.dir,
                trimLeftSlash(suffix),
                max_cluster_writer_lease_scan_cursor_bytes,
            ) catch |err| switch (err) {
                error.FileNotFound => return .{},
                error.StreamTooLong => return .{},
                else => return err,
            };
        },
        .remote => |*store| blk: {
            if (remote_operation_budget) |budget|
                try consumeBackupCleanupOperation(budget);
            break :blk store.readBytesAllocLimited(
                alloc,
                trimLeftSlash(suffix),
                max_cluster_writer_lease_scan_cursor_bytes,
            ) catch |err| switch (err) {
                error.FileNotFound => return .{},
                error.BackupManifestTooLarge, error.StreamTooLong => return .{},
                else => return err,
            };
        },
    };
    defer alloc.free(body);
    return parseClusterAttemptCleanupProgress(alloc, body, marker);
}

fn readClusterWriterLeaseScanCursor(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    marker: *const ClusterBackupAttemptMarker,
) !ClusterAttemptCleanupProgress {
    return readClusterWriterLeaseScanCursorWithBudget(
        alloc,
        io,
        location,
        marker,
        null,
    );
}

fn writeClusterWriterLeaseScanCursorMonotonicWithBudget(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    marker: *const ClusterBackupAttemptMarker,
    progress: ClusterAttemptCleanupProgress,
    remote_operation_budget: ?*usize,
) !void {
    return writeClusterWriterLeaseScanCursorMonotonicWithRootBudget(
        alloc,
        io,
        location,
        marker,
        progress,
        remote_operation_budget,
        null,
    );
}

fn writeClusterWriterLeaseScanCursorMonotonicWithRootBudget(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    marker: *const ClusterBackupAttemptMarker,
    progress: ClusterAttemptCleanupProgress,
    remote_operation_budget: ?*usize,
    pinned_root: ?std.Io.Dir,
) !void {
    const encoded = try stringifyJsonAlloc(alloc, ClusterWriterLeaseScanCursor{
        .attempt_id = marker.attempt_id,
        .phase = progress.phase,
        .next_table_index = progress.next_table_index,
    });
    defer alloc.free(encoded);
    if (encoded.len > max_cluster_writer_lease_scan_cursor_bytes)
        return error.BackupManifestTooLarge;
    const suffix = try clusterWriterLeaseScanCursorPath(alloc, "", marker.attempt_id);
    defer alloc.free(suffix);
    switch (location.*) {
        .file => |backup_root| {
            var root = try accessBackupRoot(io, backup_root, pinned_root, true);
            defer root.deinit(io);
            const backup_dir = root.dir;
            const relative_path = trimLeftSlash(suffix);
            try ensureBackupRelativeParentNoFollow(io, backup_dir, relative_path);
            const lock_path = try std.fmt.allocPrint(alloc, "{s}.publish.lock", .{relative_path});
            defer alloc.free(lock_path);
            var lock_file = try openOrCreateBackupLockFile(io, backup_dir, lock_path);
            defer lock_file.close(io);
            try lock_file.lock(io, .exclusive);
            defer lock_file.unlock(io);
            const current_body = readFileFromBackupRootAlloc(
                alloc,
                io,
                backup_dir,
                relative_path,
                max_cluster_writer_lease_scan_cursor_bytes,
            ) catch |err| switch (err) {
                error.FileNotFound, error.StreamTooLong => null,
                else => return err,
            };
            defer if (current_body) |body| alloc.free(body);
            if (current_body) |body| {
                const current = parseClusterAttemptCleanupProgress(alloc, body, marker);
                if (!cleanupProgressLessThan(current, progress)) return;
            }
            try replaceFileInBackupRootUnderHeldLock(
                alloc,
                io,
                backup_dir,
                relative_path,
                encoded,
            );
        },
        .remote => |*store| {
            const key = try store.keyAlloc(alloc, trimLeftSlash(suffix));
            defer alloc.free(key);
            var attempts: usize = 0;
            while (attempts < 8) : (attempts += 1) {
                if (remote_operation_budget) |budget|
                    try consumeBackupCleanupOperation(budget);
                var current = store.client.getObject(store.bucket, key, .{
                    .range = .{ .offset = 0, .length = max_cluster_writer_lease_scan_cursor_bytes },
                    .skip_metadata_probe = true,
                    .max_response_bytes = max_cluster_writer_lease_scan_cursor_bytes,
                }) catch |err| switch (err) {
                    error.FileNotFound => {
                        if (remote_operation_budget) |budget|
                            try consumeBackupCleanupOperation(budget);
                        var created = store.client.putObject(store.bucket, key, encoded, .{
                            .content_type = "application/json",
                            .if_none_match = true,
                        }) catch |put_err| switch (put_err) {
                            error.PreconditionFailed => continue,
                            else => return put_err,
                        };
                        created.deinit(alloc);
                        return;
                    },
                    else => return err,
                };
                defer current.deinit(alloc);
                const current_progress = parseClusterAttemptCleanupProgress(alloc, current.body, marker);
                if (!cleanupProgressLessThan(current_progress, progress)) return;
                const etag = current.metadata.etag orelse
                    return error.BackupReservationIdentityUnavailable;
                if (remote_operation_budget) |budget|
                    try consumeBackupCleanupOperation(budget);
                var replaced = store.client.putObject(store.bucket, key, encoded, .{
                    .content_type = "application/json",
                    .if_match_etag = etag,
                }) catch |put_err| switch (put_err) {
                    error.FileNotFound, error.PreconditionFailed => continue,
                    else => return put_err,
                };
                replaced.deinit(alloc);
                return;
            }
            return error.BackupRepositoryBusy;
        },
    }
}

fn writeClusterWriterLeaseScanCursorMonotonic(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    marker: *const ClusterBackupAttemptMarker,
    progress: ClusterAttemptCleanupProgress,
) !void {
    return writeClusterWriterLeaseScanCursorMonotonicWithBudget(
        alloc,
        io,
        location,
        marker,
        progress,
        null,
    );
}

fn deleteClusterWriterLeaseScanCursor(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    attempt_id: []const u8,
) !void {
    return deleteClusterWriterLeaseScanCursorWithRoot(
        alloc,
        io,
        location,
        attempt_id,
        null,
    );
}

fn deleteClusterWriterLeaseScanCursorWithRoot(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    attempt_id: []const u8,
    pinned_root: ?std.Io.Dir,
) !void {
    const suffix = try clusterWriterLeaseScanCursorPath(alloc, "", attempt_id);
    defer alloc.free(suffix);
    switch (location.*) {
        .file => |backup_root| {
            var root = try accessBackupRoot(io, backup_root, pinned_root, false);
            defer root.deinit(io);
            try deleteFileDurablyFromBackupRoot(
                io,
                root.dir,
                trimLeftSlash(suffix),
            );
        },
        .remote => |*store| try store.deleteSuffix(alloc, trimLeftSlash(suffix)),
    }
}

fn claimExpiredClusterTableWriterLeases(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    marker: *const ClusterBackupAttemptMarker,
    now_unix_ns: u64,
    object_budget: *usize,
) !bool {
    return claimExpiredClusterTableWriterLeasesWithRoot(
        alloc,
        io,
        location,
        marker,
        now_unix_ns,
        object_budget,
        null,
    );
}

fn claimExpiredClusterTableWriterLeasesWithRoot(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    marker: *const ClusterBackupAttemptMarker,
    now_unix_ns: u64,
    object_budget: *usize,
    pinned_root: ?std.Io.Dir,
) !bool {
    if (!marker.writer_lease_fencing or object_budget.* == 0) return false;
    const progress = try readClusterWriterLeaseScanCursorWithRootBudget(
        alloc,
        io,
        location,
        marker,
        null,
        pinned_root,
    );
    // A committed reconciliation cursor is only reachable if an operator
    // removed the canonical manifest after publication. Reset the advisory
    // cursor and restart fencing before any artifact cleanup; never reuse its
    // table index as payload-cleanup progress.
    if (progress.phase == .committed_writer_state) {
        try deleteClusterWriterLeaseScanCursorWithRoot(
            alloc,
            io,
            location,
            marker.attempt_id,
            pinned_root,
        );
        return false;
    }
    if (progress.phase == .artifacts) return true;
    var index = progress.next_table_index;
    const claim_cost: usize = switch (location.*) {
        // The worst case is one conditional read plus one create/replace. Use
        // that cost even when an existing tombstone only needs the read so the
        // maintenance quantum remains a hard upper bound.
        .remote => 2,
        .file => 1,
    };
    while (index < marker.tables.len and object_budget.* >= claim_cost) : (index += 1) {
        object_budget.* -= claim_cost;
        const table = marker.tables[index];
        if ((try claimExpiredTableBackupWriterLeaseAtLocationWithRoot(
            alloc,
            io,
            location,
            table.artifact_backup_id,
            now_unix_ns,
            pinned_root,
        )) == .active) {
            try writeClusterWriterLeaseScanCursorMonotonicWithRootBudget(alloc, io, location, marker, .{
                .phase = .writer_leases,
                .next_table_index = index,
            }, null, pinned_root);
            return false;
        }
    }
    if (index == marker.tables.len) {
        try writeClusterWriterLeaseScanCursorMonotonicWithRootBudget(alloc, io, location, marker, .{
            .phase = .artifacts,
            .next_table_index = 0,
        }, null, pinned_root);
        return true;
    }
    try writeClusterWriterLeaseScanCursorMonotonicWithRootBudget(alloc, io, location, marker, .{
        .phase = .writer_leases,
        .next_table_index = index,
    }, null, pinned_root);
    return false;
}

/// A committed aggregate proves every table writer finished before
/// publication. Retire any writer or cleanup owner left behind by a transient
/// conditional-delete failure before discarding the marker that makes those
/// exact artifact generations discoverable.
fn releaseCommittedClusterTableWriterStates(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    marker: *const ClusterBackupAttemptMarker,
    operation_budget: *usize,
) !bool {
    return releaseCommittedClusterTableWriterStatesWithRoot(
        alloc,
        io,
        location,
        marker,
        operation_budget,
        null,
    );
}

fn releaseCommittedClusterTableWriterStatesWithRoot(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    marker: *const ClusterBackupAttemptMarker,
    operation_budget: *usize,
    pinned_root: ?std.Io.Dir,
) !bool {
    if (!marker.writer_lease_fencing) return true;
    const remote = switch (location.*) {
        .remote => true,
        .file => false,
    };
    const progress = readClusterWriterLeaseScanCursorWithRootBudget(
        alloc,
        io,
        location,
        marker,
        if (remote) operation_budget else null,
        pinned_root,
    ) catch |err| switch (err) {
        error.BackupCleanupBudgetExceeded => return false,
        else => return err,
    };
    // The artifacts phase can contain cleanup tombstones installed by stale
    // reconciliation immediately before publication became visible. A
    // distinct terminal phase prevents mistaking those tombstones for already
    // retired committed state.
    var index: usize = if (progress.phase == .committed_writer_state)
        progress.next_table_index
    else
        0;
    if (progress.phase == .committed_writer_state and index == marker.tables.len)
        return true;
    const release_cost: usize = switch (location.*) {
        .remote => 2,
        .file => 1,
    };
    // A normal remote cursor CAS is one GET plus one conditional PUT. Leave
    // that capacity untouched while retiring tables so durable progress never
    // depends on exceeding the maintenance quantum.
    const cursor_write_reserve: usize = if (remote) 2 else 0;
    while (index < marker.tables.len and
        operation_budget.* >= release_cost + cursor_write_reserve) : (index += 1)
    {
        try releaseTableBackupWriterStateWithRootBudget(
            alloc,
            io,
            location,
            marker.tables[index].artifact_backup_id,
            operation_budget,
            pinned_root,
        );
    }
    if (index == marker.tables.len) {
        writeClusterWriterLeaseScanCursorMonotonicWithRootBudget(
            alloc,
            io,
            location,
            marker,
            .{
                .phase = .committed_writer_state,
                .next_table_index = index,
            },
            if (remote) operation_budget else null,
            pinned_root,
        ) catch |err| switch (err) {
            error.BackupCleanupBudgetExceeded => return false,
            else => return err,
        };
        return true;
    }
    if (index == 0 and operation_budget.* < cursor_write_reserve) return false;
    writeClusterWriterLeaseScanCursorMonotonicWithRootBudget(
        alloc,
        io,
        location,
        marker,
        .{
            .phase = .committed_writer_state,
            .next_table_index = index,
        },
        if (remote) operation_budget else null,
        pinned_root,
    ) catch |err| switch (err) {
        error.BackupCleanupBudgetExceeded => return false,
        else => return err,
    };
    return false;
}

/// Cluster reservations are leases owned by an immutable attempt ID. Cleanup
/// must present the same owner, so a stale process cannot release a retry's
/// live admission fence.
pub fn reserveClusterBackupAttemptLeaseAtLocation(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    backup_id: []const u8,
    attempt_id: []const u8,
    expires_at_unix_ns: u64,
) !void {
    return reserveClusterBackupAttemptLeaseAtLocationWithCancellation(
        alloc,
        io,
        location,
        backup_id,
        attempt_id,
        expires_at_unix_ns,
        .none,
    );
}

pub fn reserveClusterBackupAttemptLeaseAtLocationWithCancellation(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    backup_id: []const u8,
    attempt_id: []const u8,
    expires_at_unix_ns: u64,
    cancellation: CancellationToken,
) !void {
    try cancellation.check();
    const lease = try encodeClusterBackupReservationLease(
        alloc,
        attempt_id,
        expires_at_unix_ns,
    );
    defer alloc.free(lease);
    const suffix = try reservationPath(alloc, "", backup_id, true);
    defer alloc.free(suffix);
    switch (location.*) {
        .file => |backup_root| {
            var backup_dir = try openOrCreateBackupRootNoFollow(io, backup_root);
            defer backup_dir.close(io);
            try cancellation.check();
            if (!try writeFileToBackupRootIfAbsentLockedWithCancellation(
                alloc,
                io,
                backup_dir,
                trimLeftSlash(suffix),
                lease,
                cancellation,
            )) return error.BackupAlreadyExists;
        },
        .remote => |*store| try store.writeBytesIfAbsentWithCancellation(
            alloc,
            trimLeftSlash(suffix),
            lease,
            "text/plain",
            cancellation,
        ),
    }
}

fn claimExpiredClusterReservationForCleanup(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    marker: *const ClusterBackupAttemptMarker,
    now_unix_ns: u64,
) !bool {
    return claimExpiredClusterReservationForCleanupWithRoot(
        alloc,
        io,
        location,
        marker,
        now_unix_ns,
        null,
    );
}

fn claimExpiredClusterReservationForCleanupWithRoot(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    marker: *const ClusterBackupAttemptMarker,
    now_unix_ns: u64,
    pinned_root: ?std.Io.Dir,
) !bool {
    if (!marker.writer_lease_fencing) return false;
    var cleanup_owner_buf: [backup_cleanup_lease_owner_prefix.len + 128]u8 = undefined;
    const cleanup_owner = clusterCleanupLeaseOwner(marker.attempt_id, &cleanup_owner_buf) catch
        return false;
    var legacy_cleanup_owner_buf: [legacy_backup_cleanup_lease_owner_prefix.len + 128]u8 = undefined;
    const legacy_cleanup_owner = legacyClusterCleanupLeaseOwner(
        marker.attempt_id,
        &legacy_cleanup_owner_buf,
    ) catch return false;
    const cleanup_lease = try encodeClusterBackupReservationLease(
        alloc,
        cleanup_owner,
        std.math.maxInt(u64),
    );
    defer alloc.free(cleanup_lease);
    const suffix = try reservationPath(alloc, "", marker.cluster_backup_id, true);
    defer alloc.free(suffix);
    return switch (location.*) {
        .file => |backup_root| blk: {
            var root = try accessBackupRoot(io, backup_root, pinned_root, true);
            defer root.deinit(io);
            const backup_dir = root.dir;
            const relative_path = trimLeftSlash(suffix);
            const lock_path = try std.fmt.allocPrint(alloc, "{s}.publish.lock", .{relative_path});
            defer alloc.free(lock_path);
            var lock_file = try openOrCreateBackupLockFile(io, backup_dir, lock_path);
            defer lock_file.close(io);
            try lock_file.lock(io, .exclusive);
            defer lock_file.unlock(io);
            const body = readFileFromBackupRootAlloc(
                alloc,
                io,
                backup_dir,
                relative_path,
                max_backup_attempt_lease_bytes,
            ) catch |err| switch (err) {
                error.FileNotFound => {
                    try replaceFileInBackupRootUnderHeldLock(
                        alloc,
                        io,
                        backup_dir,
                        relative_path,
                        cleanup_lease,
                    );
                    break :blk true;
                },
                else => return err,
            };
            defer alloc.free(body);
            const lease = parseClusterBackupReservationLease(body) catch break :blk false;
            if (std.mem.eql(u8, lease.attempt_id, cleanup_owner)) break :blk true;
            if (std.mem.eql(u8, lease.attempt_id, legacy_cleanup_owner)) {
                try replaceFileInBackupRootUnderHeldLock(alloc, io, backup_dir, relative_path, cleanup_lease);
                break :blk true;
            }
            if (!std.mem.eql(u8, lease.attempt_id, marker.attempt_id) or
                !clusterBackupLeaseReclaimable(lease.expires_at_unix_ns, now_unix_ns))
                break :blk false;
            try replaceFileInBackupRootUnderHeldLock(alloc, io, backup_dir, relative_path, cleanup_lease);
            break :blk true;
        },
        .remote => |*store| (try store.claimExpiredLeaseWithFence(
            alloc,
            trimLeftSlash(suffix),
            marker.attempt_id,
            cleanup_owner,
            legacy_cleanup_owner,
            now_unix_ns,
            cleanup_lease,
        )) == .claimed,
    };
}

/// Renew only the reservation version still owned by this attempt. A false
/// result means the caller has been fenced and must not publish a manifest.
pub fn renewClusterBackupAttemptLeaseAtLocation(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    backup_id: []const u8,
    attempt_id: []const u8,
    expires_at_unix_ns: u64,
) !bool {
    return renewClusterBackupAttemptLeaseAtLocationWithCancellation(
        alloc,
        io,
        location,
        backup_id,
        attempt_id,
        expires_at_unix_ns,
        .none,
    );
}

pub fn renewClusterBackupAttemptLeaseAtLocationWithCancellation(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    backup_id: []const u8,
    attempt_id: []const u8,
    expires_at_unix_ns: u64,
    cancellation: CancellationToken,
) !bool {
    try cancellation.check();
    const lease = try encodeClusterBackupReservationLease(
        alloc,
        attempt_id,
        expires_at_unix_ns,
    );
    defer alloc.free(lease);
    const suffix = try reservationPath(alloc, "", backup_id, true);
    defer alloc.free(suffix);
    return switch (location.*) {
        .file => |backup_root| blk: {
            var backup_dir = openBackupRootNoFollow(io, backup_root) catch |err| switch (err) {
                error.FileNotFound => break :blk false,
                else => return err,
            };
            defer backup_dir.close(io);
            const relative_path = trimLeftSlash(suffix);
            const lock_path = try std.fmt.allocPrint(alloc, "{s}.publish.lock", .{relative_path});
            defer alloc.free(lock_path);
            var lock_file = try openOrCreateBackupLockFile(io, backup_dir, lock_path);
            defer lock_file.close(io);
            try lockFileExclusiveWithCancellation(io, lock_file, cancellation);
            defer lock_file.unlock(io);
            const body = readFileFromBackupRootAlloc(
                alloc,
                io,
                backup_dir,
                relative_path,
                max_backup_attempt_lease_bytes,
            ) catch |err| switch (err) {
                error.FileNotFound => break :blk false,
                else => return err,
            };
            defer alloc.free(body);
            if (!std.mem.eql(u8, reservationOwner(body), attempt_id))
                break :blk false;
            try replaceFileInBackupRootUnderHeldLockWithHookAndCancellation(
                alloc,
                io,
                backup_dir,
                relative_path,
                lease,
                null,
                cancellation,
            );
            break :blk true;
        },
        .remote => |*store| try store.replaceBytesIfOwnedWithCancellation(
            alloc,
            trimLeftSlash(suffix),
            attempt_id,
            lease,
            "text/plain",
            cancellation,
        ),
    };
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

fn currentGoBackupAttemptHeadPath(
    alloc: std.mem.Allocator,
    backup_root: []const u8,
) ![]u8 {
    return try std.fmt.allocPrint(
        alloc,
        "{s}/{s}",
        .{ backup_root, current_go_backup_attempt_head_name },
    );
}

fn backupAttemptReclaimCursorPath(alloc: std.mem.Allocator, backup_root: []const u8) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{s}/{s}", .{
        backup_root,
        backup_attempt_reclaim_cursor_name,
    });
}

const LocalBackupAttemptReclaimTicketKind = enum {
    queued,
    claimed,
};

const BackupStagingPublicationTestHook = struct {
    io: std.Io,
    staged: std.atomic.Value(bool) = .init(false),
    progress: std.Io.Event = .unset,
    release: std.Io.Event = .unset,
    force_modify_timestamp_ns: ?i96 = null,
    failure: ?anyerror = null,

    fn pauseAfterSyncInDir(
        self: *@This(),
        dir: std.Io.Dir,
        path: []const u8,
    ) void {
        if (self.force_modify_timestamp_ns) |timestamp_ns| {
            const timestamp = std.Io.Timestamp.fromNanoseconds(timestamp_ns);
            dir.setTimestamps(self.io, path, .{
                .modify_timestamp = .{ .new = timestamp },
            }) catch |err| {
                self.failure = err;
            };
        }
        self.staged.store(true, .release);
        self.progress.set(self.io);
        self.release.waitUncancelable(self.io);
    }
};

const BackupMaintenanceTestHook = struct {
    stale_staging_candidate: std.atomic.Value(bool) = .init(false),
    progress: std.Io.Event = .unset,
    pause_after_claims: bool = false,
    claims_ready: std.Io.Event = .unset,
    claims_release: std.Io.Event = .unset,
};

fn localBackupAttemptReclaimShard(eligible_at_unix_ns: u64) u8 {
    const bucket = @divTrunc(eligible_at_unix_ns, std.time.ns_per_hour);
    return @intCast(bucket % backup_attempt_reclaim_shard_count);
}

fn localBackupAttemptReclaimTicketPath(
    alloc: std.mem.Allocator,
    backup_root: []const u8,
    kind: LocalBackupAttemptReclaimTicketKind,
    shard: u8,
    attempt_id: []const u8,
) ![]u8 {
    try validateBackupId(attempt_id);
    if (shard >= backup_attempt_reclaim_shard_count)
        return error.InvalidBackupRequest;
    return try std.fmt.allocPrint(alloc, "{s}/{s}/{s}/{d:0>2}/{s}", .{
        backup_root,
        backup_attempt_reclaim_index_name,
        @tagName(kind),
        shard,
        attempt_id,
    });
}

fn localBackupAttemptReclaimShardPath(
    alloc: std.mem.Allocator,
    backup_root: []const u8,
    kind: LocalBackupAttemptReclaimTicketKind,
    shard: u8,
) ![]u8 {
    if (shard >= backup_attempt_reclaim_shard_count)
        return error.InvalidBackupRequest;
    return try std.fmt.allocPrint(alloc, "{s}/{s}/{s}/{d:0>2}", .{
        backup_root,
        backup_attempt_reclaim_index_name,
        @tagName(kind),
        shard,
    });
}

fn localBackupAttemptReclaimStagingShardPath(
    alloc: std.mem.Allocator,
    backup_root: []const u8,
    kind: LocalBackupAttemptReclaimTicketKind,
    shard: u8,
) ![]u8 {
    if (shard >= backup_attempt_reclaim_shard_count)
        return error.InvalidBackupRequest;
    return try std.fmt.allocPrint(alloc, "{s}/{s}/.staging/{s}/{d:0>2}", .{
        backup_root,
        backup_attempt_reclaim_index_name,
        @tagName(kind),
        shard,
    });
}

fn localBackupAttemptPublicationLockPath(
    alloc: std.mem.Allocator,
    backup_root: []const u8,
    attempt_id: []const u8,
) ![]u8 {
    try validateBackupId(attempt_id);
    return try std.fmt.allocPrint(alloc, "{s}/{s}/publication-locks/{s}", .{
        backup_root,
        backup_attempt_reclaim_index_name,
        attempt_id,
    });
}

fn localBackupAttemptReclaimCursor(body: ?[]const u8) u8 {
    const value = body orelse return 0;
    const trimmed = std.mem.trim(u8, value, " \r\n\t");
    const shard = std.fmt.parseInt(u8, trimmed, 10) catch return 0;
    return if (shard < backup_attempt_reclaim_shard_count) shard else 0;
}

fn localBackupAttemptReclaimTicketTimestamp(
    alloc: std.mem.Allocator,
    io: std.Io,
    backup_root: std.Io.Dir,
    relative_path: []const u8,
) !?u64 {
    const body = readFileFromBackupRootAlloc(
        alloc,
        io,
        backup_root,
        relative_path,
        max_backup_attempt_reclaim_ticket_bytes,
    ) catch |err| switch (err) {
        error.FileNotFound => return null,
        error.StreamTooLong => return null,
        else => return err,
    };
    defer alloc.free(body);
    const trimmed = std.mem.trim(u8, body, " \r\n\t");
    return std.fmt.parseInt(u64, trimmed, 10) catch null;
}

fn replaceLocalBackupAttemptReclaimTicketTimestamp(
    alloc: std.mem.Allocator,
    io: std.Io,
    backup_root: std.Io.Dir,
    relative_path: []const u8,
    timestamp_unix_ns: u64,
) !void {
    return replaceLocalBackupAttemptReclaimTicketTimestampWithHook(
        alloc,
        io,
        backup_root,
        relative_path,
        timestamp_unix_ns,
        null,
    );
}

fn replaceLocalBackupAttemptReclaimTicketTimestampWithHook(
    alloc: std.mem.Allocator,
    io: std.Io,
    backup_root: std.Io.Dir,
    relative_path: []const u8,
    timestamp_unix_ns: u64,
    test_hook: ?*BackupStagingPublicationTestHook,
) !void {
    try validateArtifactRelativePath(relative_path);
    var body_buf: [32]u8 = undefined;
    const body = try std.fmt.bufPrint(&body_buf, "{d}\n", .{timestamp_unix_ns});
    const shard_dir = std.fs.path.dirname(relative_path) orelse
        return error.InvalidBackupRequest;
    const state_dir = std.fs.path.dirname(shard_dir) orelse
        return error.InvalidBackupRequest;
    const reclaim_root = std.fs.path.dirname(state_dir) orelse
        return error.InvalidBackupRequest;
    if (!std.mem.eql(
        u8,
        std.fs.path.basename(reclaim_root),
        backup_attempt_reclaim_index_name,
    )) return error.InvalidBackupRequest;
    const kind = std.meta.stringToEnum(
        LocalBackupAttemptReclaimTicketKind,
        std.fs.path.basename(state_dir),
    ) orelse return error.InvalidBackupRequest;
    const shard = std.fmt.parseInt(
        u8,
        std.fs.path.basename(shard_dir),
        10,
    ) catch return error.InvalidBackupRequest;
    const staging_dir = try localBackupAttemptReclaimStagingShardPath(
        alloc,
        "",
        kind,
        shard,
    );
    defer alloc.free(staging_dir);
    try replaceFileFromBackupStagingDirUnderHeldLockWithHook(
        alloc,
        io,
        backup_root,
        relative_path,
        trimLeftSlash(staging_dir),
        body,
        test_hook,
    );
}

fn renameLocalBackupAttemptReclaimTicket(
    io: std.Io,
    backup_root: std.Io.Dir,
    source_relative_path: []const u8,
    destination_relative_path: []const u8,
) !void {
    try ensureBackupRelativeParentNoFollow(io, backup_root, destination_relative_path);
    var source = try openBackupRelativeParentNoFollow(io, backup_root, source_relative_path);
    defer source.deinit(io);
    var destination = try openBackupRelativeParentNoFollow(
        io,
        backup_root,
        destination_relative_path,
    );
    defer destination.deinit(io);
    try std.Io.Dir.rename(
        source.dir,
        source.basename,
        destination.dir,
        destination.basename,
        io,
    );
    try fs_paths.syncDirectoryHandlePortable(io, source.dir);
    try fs_paths.syncDirectoryHandlePortable(io, destination.dir);
}

fn localBackupAttemptReclaimClaimExpired(
    claimed_at_unix_ns: ?u64,
    now_unix_ns: u64,
) bool {
    const claimed_at = claimed_at_unix_ns orelse return true;
    // A future timestamp is evidence that this observer's clock is behind,
    // not that the claim is abandoned. Apply the same cross-node skew envelope
    // as reservation leases before permitting a second worker to take over.
    if (claimed_at > now_unix_ns) return false;
    const safe_timeout_ns =
        backup_attempt_reclaim_claim_timeout_ns +|
        backup_attempt_lease_clock_skew_allowance_ns;
    return now_unix_ns - claimed_at >= safe_timeout_ns;
}

fn validateClusterBackupAttemptHead(head: *const ClusterBackupAttemptHead) !void {
    if (head.format_version != backup_attempt_head_version)
        return error.UnsupportedBackupFormat;
    try validateBackupId(head.attempt_id);
    if (head.generation == 0) return error.InvalidBackupRequest;
}

fn clusterBackupAttemptHeadsEqual(
    a: ?*const ClusterBackupAttemptHead,
    b: ?*const ClusterBackupAttemptHead,
) bool {
    if (a == null or b == null) return a == null and b == null;
    return a.?.generation == b.?.generation and
        a.?.state == b.?.state and
        std.mem.eql(u8, a.?.attempt_id, b.?.attempt_id);
}

fn nextClusterBackupAttemptHeadGeneration(current: u64) !u64 {
    if (current == std.math.maxInt(u64))
        return error.BackupPublicationConflict;
    return current + 1;
}

pub fn writeClusterBackupAttemptHead(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    attempt_id: []const u8,
) !void {
    return writeClusterBackupAttemptHeadWithCancellation(
        alloc,
        io,
        location,
        attempt_id,
        .none,
    );
}

pub fn writeClusterBackupAttemptHeadWithCancellation(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    attempt_id: []const u8,
    cancellation: CancellationToken,
) !void {
    try cancellation.check();
    try validateBackupId(attempt_id);
    switch (location.*) {
        .file => |backup_root| {
            var backup_dir = try openOrCreateBackupRootNoFollow(io, backup_root);
            defer backup_dir.close(io);
            const path = try backupAttemptHeadPath(alloc, "");
            defer alloc.free(path);
            const relative_path = trimLeftSlash(path);
            const lock_path = try std.fmt.allocPrint(alloc, "{s}.publish.lock", .{relative_path});
            defer alloc.free(lock_path);
            var lock_file = try openOrCreateBackupLockFile(io, backup_dir, lock_path);
            defer lock_file.close(io);
            try lockFileExclusiveWithCancellation(io, lock_file, cancellation);
            defer lock_file.unlock(io);
            try cancellation.check();

            const previous = readFileFromBackupRootAlloc(
                alloc,
                io,
                backup_dir,
                relative_path,
                max_backup_attempt_marker_bytes,
            ) catch |err| switch (err) {
                error.FileNotFound => null,
                else => return err,
            };
            defer if (previous) |body| alloc.free(body);
            var previous_parsed: ?std.json.Parsed(ClusterBackupAttemptHead) = if (previous) |body|
                try std.json.parseFromSlice(
                    ClusterBackupAttemptHead,
                    alloc,
                    body,
                    .{ .allocate = .alloc_always },
                )
            else
                null;
            defer if (previous_parsed) |*parsed| parsed.deinit();
            if (previous_parsed) |*parsed| try validateClusterBackupAttemptHead(&parsed.value);
            const generation = if (previous_parsed) |parsed|
                try nextClusterBackupAttemptHeadGeneration(parsed.value.generation)
            else
                1;
            const head: ClusterBackupAttemptHead = .{
                .attempt_id = attempt_id,
                .generation = generation,
            };
            const encoded = try stringifyJsonAlloc(alloc, head);
            defer alloc.free(encoded);
            try ensureManifestSize(encoded, max_backup_attempt_marker_bytes);
            try cancellation.check();
            try replaceFileInBackupRootUnderHeldLockWithHookAndCancellation(
                alloc,
                io,
                backup_dir,
                relative_path,
                encoded,
                null,
                cancellation,
            );
        },
        .remote => |*store| {
            const key = try store.keyAlloc(alloc, backup_attempt_head_name);
            defer alloc.free(key);
            var retry_count: usize = 0;
            while (retry_count < 16) : (retry_count += 1) {
                try cancellation.check();
                var current = store.client.getObject(store.bucket, key, .{
                    .range = .{
                        .offset = 0,
                        .length = max_backup_attempt_marker_bytes + 1,
                    },
                    .skip_metadata_probe = true,
                    .max_response_bytes = max_backup_attempt_marker_bytes + 1,
                    .cancellation = objectCancellationToken(cancellation),
                }) catch |err| switch (err) {
                    error.FileNotFound => null,
                    else => return err,
                };
                defer if (current) |*value| value.deinit(alloc);
                var current_parsed: ?std.json.Parsed(ClusterBackupAttemptHead) =
                    if (current) |value| blk: {
                        if (value.body.len > max_backup_attempt_marker_bytes)
                            return error.BackupManifestTooLarge;
                        break :blk try std.json.parseFromSlice(
                            ClusterBackupAttemptHead,
                            alloc,
                            value.body,
                            .{ .allocate = .alloc_always },
                        );
                    } else null;
                defer if (current_parsed) |*parsed| parsed.deinit();
                if (current_parsed) |*parsed|
                    try validateClusterBackupAttemptHead(&parsed.value);
                const generation = if (current_parsed) |parsed|
                    try nextClusterBackupAttemptHeadGeneration(parsed.value.generation)
                else
                    1;
                const head: ClusterBackupAttemptHead = .{
                    .attempt_id = attempt_id,
                    .generation = generation,
                };
                const encoded = try stringifyJsonAlloc(alloc, head);
                defer alloc.free(encoded);
                try ensureManifestSize(encoded, max_backup_attempt_marker_bytes);
                var published = store.client.putObject(store.bucket, key, encoded, .{
                    .content_type = "application/json",
                    .if_none_match = current == null,
                    .if_match_etag = if (current) |value|
                        value.metadata.etag orelse
                            return error.BackupReservationIdentityUnavailable
                    else
                        null,
                    .cancellation = objectCancellationToken(cancellation),
                }) catch |err| switch (err) {
                    error.FileNotFound, error.PreconditionFailed => continue,
                    else => return err,
                };
                published.deinit(alloc);
                return;
            }
            return error.BackupPublicationConflict;
        },
    }
}

fn readClusterBackupAttemptHead(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
) !?std.json.Parsed(ClusterBackupAttemptHead) {
    return readClusterBackupAttemptHeadWithRoot(alloc, io, location, null);
}

fn readClusterBackupAttemptHeadWithRoot(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    pinned_root: ?std.Io.Dir,
) !?std.json.Parsed(ClusterBackupAttemptHead) {
    const body = switch (location.*) {
        .file => |backup_root| blk: {
            const path = try backupAttemptHeadPath(alloc, "");
            defer alloc.free(path);
            var root = accessBackupRoot(io, backup_root, pinned_root, false) catch |err| switch (err) {
                error.FileNotFound => return null,
                else => return err,
            };
            defer root.deinit(io);
            break :blk readFileFromBackupRootAlloc(
                alloc,
                io,
                root.dir,
                trimLeftSlash(path),
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

/// Conditionally transition the authoritative attempt head. Normal lifecycle
/// transitions only move an active head to a terminal state. Exact mismatch
/// quarantine is the sole caller permitted to fail a committed head.
fn transitionClusterBackupAttemptHeadIfOwned(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    attempt_id: []const u8,
    target_state: ClusterBackupAttemptState,
    allow_committed_failure: bool,
) !bool {
    return transitionClusterBackupAttemptHeadIfOwnedWithRoot(
        alloc,
        io,
        location,
        attempt_id,
        target_state,
        allow_committed_failure,
        null,
    );
}

fn transitionClusterBackupAttemptHeadIfOwnedWithRoot(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    attempt_id: []const u8,
    target_state: ClusterBackupAttemptState,
    allow_committed_failure: bool,
    pinned_root: ?std.Io.Dir,
) !bool {
    try validateBackupId(attempt_id);
    std.debug.assert(target_state != .active);
    return switch (location.*) {
        .file => |backup_root| blk: {
            var root = try accessBackupRoot(io, backup_root, pinned_root, false);
            defer root.deinit(io);
            const backup_dir = root.dir;
            const path = try backupAttemptHeadPath(alloc, "");
            defer alloc.free(path);
            const relative_path = trimLeftSlash(path);
            const lock_path = try std.fmt.allocPrint(alloc, "{s}.publish.lock", .{relative_path});
            defer alloc.free(lock_path);
            var lock_file = try openOrCreateBackupLockFile(io, backup_dir, lock_path);
            defer lock_file.close(io);
            try lock_file.lock(io, .exclusive);
            defer lock_file.unlock(io);

            const body = readFileFromBackupRootAlloc(
                alloc,
                io,
                backup_dir,
                relative_path,
                max_backup_attempt_marker_bytes,
            ) catch |err| switch (err) {
                error.FileNotFound => break :blk false,
                else => return err,
            };
            defer alloc.free(body);
            var parsed = try std.json.parseFromSlice(
                ClusterBackupAttemptHead,
                alloc,
                body,
                .{ .allocate = .alloc_always },
            );
            defer parsed.deinit();
            try validateClusterBackupAttemptHead(&parsed.value);
            if (!std.mem.eql(u8, parsed.value.attempt_id, attempt_id))
                break :blk false;
            if (parsed.value.state == target_state) break :blk true;
            if (parsed.value.state != .active and
                !(allow_committed_failure and
                    parsed.value.state == .committed and
                    target_state == .failed))
                break :blk false;
            const updated: ClusterBackupAttemptHead = .{
                .attempt_id = attempt_id,
                .state = target_state,
                .generation = try nextClusterBackupAttemptHeadGeneration(parsed.value.generation),
            };
            const encoded = try stringifyJsonAlloc(alloc, updated);
            defer alloc.free(encoded);
            try ensureManifestSize(encoded, max_backup_attempt_marker_bytes);
            try replaceFileInBackupRootUnderHeldLock(
                alloc,
                io,
                backup_dir,
                relative_path,
                encoded,
            );
            break :blk true;
        },
        .remote => |*store| blk: {
            const key = try store.keyAlloc(alloc, backup_attempt_head_name);
            defer alloc.free(key);
            var result = store.client.getObject(store.bucket, key, .{
                .range = .{
                    .offset = 0,
                    .length = max_backup_attempt_marker_bytes + 1,
                },
                .skip_metadata_probe = true,
                .max_response_bytes = max_backup_attempt_marker_bytes + 1,
            }) catch |err| switch (err) {
                error.FileNotFound => break :blk false,
                else => return err,
            };
            defer result.deinit(alloc);
            if (result.body.len > max_backup_attempt_marker_bytes)
                return error.BackupManifestTooLarge;
            var parsed = try std.json.parseFromSlice(
                ClusterBackupAttemptHead,
                alloc,
                result.body,
                .{ .allocate = .alloc_always },
            );
            defer parsed.deinit();
            try validateClusterBackupAttemptHead(&parsed.value);
            if (!std.mem.eql(u8, parsed.value.attempt_id, attempt_id))
                break :blk false;
            if (parsed.value.state == target_state) break :blk true;
            if (parsed.value.state != .active and
                !(allow_committed_failure and
                    parsed.value.state == .committed and
                    target_state == .failed))
                break :blk false;
            const updated: ClusterBackupAttemptHead = .{
                .attempt_id = attempt_id,
                .state = target_state,
                .generation = try nextClusterBackupAttemptHeadGeneration(parsed.value.generation),
            };
            const encoded = try stringifyJsonAlloc(alloc, updated);
            defer alloc.free(encoded);
            try ensureManifestSize(encoded, max_backup_attempt_marker_bytes);
            const etag = result.metadata.etag orelse
                return error.BackupReservationIdentityUnavailable;
            var replaced = store.client.putObject(store.bucket, key, encoded, .{
                .content_type = "application/json",
                .if_match_etag = etag,
            }) catch |err| switch (err) {
                error.FileNotFound, error.PreconditionFailed => break :blk false,
                else => return err,
            };
            defer replaced.deinit(alloc);
            break :blk true;
        },
    };
}

/// Preserve failed-attempt evidence after exact artifact cleanup. Restore
/// health remains failed until a later attempt publishes a new authoritative
/// head and complete aggregate manifest.
pub fn retireClusterBackupAttemptHeadIfOwned(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    attempt_id: []const u8,
) !bool {
    return transitionClusterBackupAttemptHeadIfOwned(
        alloc,
        io,
        location,
        attempt_id,
        .failed,
        false,
    );
}

fn retireClusterBackupAttemptHeadIfOwnedWithRoot(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    attempt_id: []const u8,
    pinned_root: ?std.Io.Dir,
) !bool {
    return transitionClusterBackupAttemptHeadIfOwnedWithRoot(
        alloc,
        io,
        location,
        attempt_id,
        .failed,
        false,
        pinned_root,
    );
}

fn quarantineClusterBackupAttemptHeadIfOwned(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    attempt_id: []const u8,
) !bool {
    return transitionClusterBackupAttemptHeadIfOwned(
        alloc,
        io,
        location,
        attempt_id,
        .failed,
        true,
    );
}

fn quarantineClusterBackupAttemptHeadIfOwnedWithRoot(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    attempt_id: []const u8,
    pinned_root: ?std.Io.Dir,
) !bool {
    return transitionClusterBackupAttemptHeadIfOwnedWithRoot(
        alloc,
        io,
        location,
        attempt_id,
        .failed,
        true,
        pinned_root,
    );
}

/// Record normal completion after the immutable aggregate manifest has become
/// durable. The aggregate remains the commit point; a lost terminal-state
/// update is safe because active admission still validates that exact commit.
pub fn commitClusterBackupAttemptHeadIfOwned(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    attempt_id: []const u8,
) !bool {
    return transitionClusterBackupAttemptHeadIfOwned(
        alloc,
        io,
        location,
        attempt_id,
        .committed,
        false,
    );
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
    if (marker.writer_delivery_deadline_fencing) {
        if (!marker.writer_lease_fencing) return error.InvalidBackupRequest;
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
    return writeClusterBackupAttemptMarkerWithHook(
        alloc,
        io,
        location,
        marker,
        null,
    );
}

pub fn writeClusterBackupAttemptMarkerWithCancellation(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    marker: *const ClusterBackupAttemptMarker,
    cancellation: CancellationToken,
) !void {
    return writeClusterBackupAttemptMarkerWithHookAndCancellation(
        alloc,
        io,
        location,
        marker,
        null,
        cancellation,
    );
}

fn writeClusterBackupAttemptMarkerWithHook(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    marker: *const ClusterBackupAttemptMarker,
    test_hook: ?*BackupStagingPublicationTestHook,
) !void {
    return writeClusterBackupAttemptMarkerWithHookAndCancellation(
        alloc,
        io,
        location,
        marker,
        test_hook,
        .none,
    );
}

fn writeClusterBackupAttemptMarkerWithHookAndCancellation(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    marker: *const ClusterBackupAttemptMarker,
    test_hook: ?*BackupStagingPublicationTestHook,
    cancellation: CancellationToken,
) !void {
    try cancellation.check();
    try validateClusterBackupAttemptMarker(alloc, marker, marker.attempt_id);
    const encoded = try stringifyJsonAlloc(alloc, marker.*);
    defer alloc.free(encoded);
    try ensureManifestSize(encoded, max_backup_attempt_marker_bytes);
    const suffix = try incompleteBackupMarkerPath(alloc, "", marker.attempt_id);
    defer alloc.free(suffix);
    switch (location.*) {
        .file => |backup_root| {
            var backup_dir = try openOrCreateBackupRootNoFollow(io, backup_root);
            defer backup_dir.close(io);
            const relative_path = trimLeftSlash(suffix);
            const publication_lock_path = try localBackupAttemptPublicationLockPath(
                alloc,
                "",
                marker.attempt_id,
            );
            defer alloc.free(publication_lock_path);
            const relative_publication_lock_path = trimLeftSlash(publication_lock_path);
            const eligible_at_unix_ns =
                marker.created_at_unix_ns +| backup_attempt_reclaim_age_ns;
            const shard = localBackupAttemptReclaimShard(eligible_at_unix_ns);
            const ticket_path = try localBackupAttemptReclaimTicketPath(
                alloc,
                "",
                .queued,
                shard,
                marker.attempt_id,
            );
            defer alloc.free(ticket_path);
            const relative_ticket_path = trimLeftSlash(ticket_path);
            const staging_dir = try localBackupAttemptReclaimStagingShardPath(
                alloc,
                "",
                .queued,
                shard,
            );
            defer alloc.free(staging_dir);
            const relative_staging_dir = trimLeftSlash(staging_dir);

            // Establish every directory needed by the two-record publication
            // before exposing the per-attempt lock. This keeps first-use
            // repository initialization out of the critical section and
            // prevents asynchronous maintenance from observing a partially
            // prepared control layout.
            try ensureBackupRelativeParentNoFollow(io, backup_dir, relative_publication_lock_path);
            try ensureBackupRelativeParentNoFollow(io, backup_dir, relative_ticket_path);
            try ensureBackupRelativeParentNoFollow(io, backup_dir, relative_path);
            try ensureBackupRelativeDirectoryNoFollow(io, backup_dir, relative_staging_dir);
            var publication_lock = try openOrCreateBackupLockFile(
                io,
                backup_dir,
                relative_publication_lock_path,
            );
            defer publication_lock.close(io);
            try lockFileExclusiveWithCancellation(io, publication_lock, cancellation);
            defer publication_lock.unlock(io);
            try cancellation.check();

            // Publish the durable reclaim ticket before the marker. A crash
            // may leave an orphan ticket, which bounded maintenance removes.
            // The per-attempt lock closes that publication race without making
            // request admission wait behind unrelated directory enumeration.
            if (!(try fileExistsFromBackupRoot(io, backup_dir, relative_ticket_path))) {
                var body_buf: [32]u8 = undefined;
                const ticket_body = try std.fmt.bufPrint(
                    &body_buf,
                    "{d}\n",
                    .{eligible_at_unix_ns},
                );
                try replaceFileFromBackupStagingDirUnderHeldLockWithHook(
                    alloc,
                    io,
                    backup_dir,
                    relative_ticket_path,
                    relative_staging_dir,
                    ticket_body,
                    test_hook,
                );
            }
            try cancellation.check();
            if (!try writeFileToBackupRootIfAbsentWithCancellation(
                io,
                backup_dir,
                relative_path,
                encoded,
                cancellation,
            )) return error.BackupAlreadyExists;
        },
        .remote => |*store| try store.writeBytesIfAbsentWithCancellation(
            alloc,
            trimLeftSlash(suffix),
            encoded,
            "application/json",
            cancellation,
        ),
    }
}

fn readClusterBackupAttemptMarker(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    attempt_id: []const u8,
) !std.json.Parsed(ClusterBackupAttemptMarker) {
    return readClusterBackupAttemptMarkerWithRoot(
        alloc,
        io,
        location,
        attempt_id,
        null,
    );
}

fn readClusterBackupAttemptMarkerWithRoot(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    attempt_id: []const u8,
    pinned_root: ?std.Io.Dir,
) !std.json.Parsed(ClusterBackupAttemptMarker) {
    const suffix = try incompleteBackupMarkerPath(alloc, "", attempt_id);
    defer alloc.free(suffix);
    const body = switch (location.*) {
        .file => |backup_root| blk: {
            var root = try accessBackupRoot(io, backup_root, pinned_root, false);
            defer root.deinit(io);
            break :blk try readFileFromBackupRootAlloc(
                alloc,
                io,
                root.dir,
                trimLeftSlash(suffix),
                max_backup_attempt_marker_bytes,
            );
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

const CurrentGoClusterBackupAttemptMarker = struct {
    version: u32,
    attempt_id: []const u8,
    backup_id: []const u8,
    created_at: []const u8,
    format: BackupFormat,
    expected_table_count: usize,
    table_names: []const []const u8,
    metadata_ids: []const []const u8,
    artifact_names: []const []const u8,
};

const CurrentGoClusterBackupAttemptHead = struct {
    version: u32,
    generation: u64,
    attempt_id: []const u8,
    backup_id: []const u8,
    state: ClusterBackupAttemptState,
    marker_sha256: []const u8,
};

fn validateCurrentGoClusterBackupAttemptHead(
    head: *const CurrentGoClusterBackupAttemptHead,
) !void {
    if (head.version != current_go_backup_attempt_head_version)
        return error.UnsupportedBackupFormat;
    try validateBackupId(head.attempt_id);
    try validateBackupId(head.backup_id);
    if (head.generation == 0)
        return error.InvalidBackupRequest;
    if (!isLowerSha256Hex(head.marker_sha256))
        return error.InvalidBackupRequest;
}

fn currentGoClusterBackupAttemptHeadsEqual(
    lhs: *const CurrentGoClusterBackupAttemptHead,
    rhs: *const CurrentGoClusterBackupAttemptHead,
) bool {
    return lhs.version == rhs.version and
        lhs.generation == rhs.generation and
        std.mem.eql(u8, lhs.attempt_id, rhs.attempt_id) and
        std.mem.eql(u8, lhs.backup_id, rhs.backup_id) and
        lhs.state == rhs.state and
        std.mem.eql(u8, lhs.marker_sha256, rhs.marker_sha256);
}

fn readCurrentGoClusterBackupAttemptHead(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
) !?std.json.Parsed(CurrentGoClusterBackupAttemptHead) {
    const body = switch (location.*) {
        .file => |backup_root| blk: {
            const path = try currentGoBackupAttemptHeadPath(alloc, backup_root);
            defer alloc.free(path);
            break :blk readFileAbsoluteAllocWithIo(
                alloc,
                io,
                path,
                current_go_backup_attempt_head_max_bytes,
            ) catch |err| switch (err) {
                error.FileNotFound => return null,
                else => return err,
            };
        },
        .remote => |*store| store.readBytesAllocLimited(
            alloc,
            current_go_backup_attempt_head_name,
            current_go_backup_attempt_head_max_bytes,
        ) catch |err| switch (err) {
            error.FileNotFound => return null,
            else => return err,
        },
    };
    defer alloc.free(body);
    var parsed = try std.json.parseFromSlice(
        CurrentGoClusterBackupAttemptHead,
        alloc,
        body,
        .{
            .allocate = .alloc_always,
            .duplicate_field_behavior = .@"error",
            .ignore_unknown_fields = false,
        },
    );
    errdefer parsed.deinit();
    try validateCurrentGoClusterBackupAttemptHead(&parsed.value);
    return parsed;
}

const CurrentGoAttemptTimestamp = struct {
    year: u16,
    month: u8,
    day: u8,
    hour: u8,
    minute: u8,
    second: u8,
    nanosecond: u32,
};

const ParsedCurrentGoClusterBackupAttempt = struct {
    parsed: std.json.Parsed(CurrentGoClusterBackupAttemptMarker),
    created_at: CurrentGoAttemptTimestamp,

    fn deinit(self: *@This()) void {
        self.parsed.deinit();
        self.* = undefined;
    }
};

fn parseFixedWidthDecimal(comptime T: type, text: []const u8) !T {
    if (text.len == 0) return error.InvalidBackupRequest;
    for (text) |byte| if (byte < '0' or byte > '9')
        return error.InvalidBackupRequest;
    return std.fmt.parseInt(T, text, 10) catch error.InvalidBackupRequest;
}

fn isLeapYear(year: u16) bool {
    return year % 4 == 0 and (year % 100 != 0 or year % 400 == 0);
}

fn daysInMonth(year: u16, month: u8) u8 {
    return switch (month) {
        1, 3, 5, 7, 8, 10, 12 => 31,
        4, 6, 9, 11 => 30,
        2 => if (isLeapYear(year)) 29 else 28,
        else => 0,
    };
}

/// Go's time.Time JSON encoder emits UTC RFC3339Nano for the current backup
/// producer. Requiring that exact canonical zone keeps ordering deterministic
/// without accepting historical timestamp dialects.
fn parseCurrentGoAttemptTimestamp(text: []const u8) !CurrentGoAttemptTimestamp {
    if (text.len < 20 or text.len > 30 or
        text[4] != '-' or text[7] != '-' or text[10] != 'T' or
        text[13] != ':' or text[16] != ':' or text[text.len - 1] != 'Z')
    {
        return error.InvalidBackupRequest;
    }
    const year = try parseFixedWidthDecimal(u16, text[0..4]);
    const month = try parseFixedWidthDecimal(u8, text[5..7]);
    const day = try parseFixedWidthDecimal(u8, text[8..10]);
    const hour = try parseFixedWidthDecimal(u8, text[11..13]);
    const minute = try parseFixedWidthDecimal(u8, text[14..16]);
    const second = try parseFixedWidthDecimal(u8, text[17..19]);
    if (year == 0 or day == 0 or day > daysInMonth(year, month) or
        hour > 23 or minute > 59 or second > 59)
    {
        return error.InvalidBackupRequest;
    }

    var nanosecond: u32 = 0;
    if (text.len > 20) {
        if (text[19] != '.') return error.InvalidBackupRequest;
        const fractional = text[20 .. text.len - 1];
        if (fractional.len == 0 or fractional.len > 9)
            return error.InvalidBackupRequest;
        nanosecond = try parseFixedWidthDecimal(u32, fractional);
        var padding = 9 - fractional.len;
        while (padding > 0) : (padding -= 1) nanosecond *= 10;
    } else if (text[19] != 'Z') {
        return error.InvalidBackupRequest;
    }
    const timestamp: CurrentGoAttemptTimestamp = .{
        .year = year,
        .month = month,
        .day = day,
        .hour = hour,
        .minute = minute,
        .second = second,
        .nanosecond = nanosecond,
    };
    // Go rejects time.Time{} attempt timestamps before publication.
    if (timestamp.year == 1 and timestamp.month == 1 and timestamp.day == 1 and
        timestamp.hour == 0 and timestamp.minute == 0 and timestamp.second == 0 and
        timestamp.nanosecond == 0)
    {
        return error.InvalidBackupRequest;
    }
    return timestamp;
}

fn currentGoAttemptTimestampOrder(
    lhs: CurrentGoAttemptTimestamp,
    rhs: CurrentGoAttemptTimestamp,
) std.math.Order {
    inline for (std.meta.fields(CurrentGoAttemptTimestamp)) |field| {
        const order = std.math.order(@field(lhs, field.name), @field(rhs, field.name));
        if (order != .eq) return order;
    }
    return .eq;
}

fn isGoUnicodeWhitespace(codepoint: u21) bool {
    return switch (codepoint) {
        0x0009...0x000d,
        0x0020,
        0x0085,
        0x00a0,
        0x1680,
        0x2000...0x200a,
        0x2028,
        0x2029,
        0x202f,
        0x205f,
        0x3000,
        => true,
        else => false,
    };
}

/// Mirrors Go's strings.TrimSpace(name) != "" admission check without
/// allocating a normalized copy.
fn currentGoTableNameHasContent(name: []const u8) bool {
    var iterator = (std.unicode.Utf8View.init(name) catch return false).iterator();
    while (iterator.nextCodepoint()) |codepoint| {
        if (!isGoUnicodeWhitespace(codepoint)) return true;
    }
    return false;
}

fn validateCurrentGoBackupId(value: []const u8) !void {
    try validateBackupId(value);
    if (std.mem.indexOf(u8, value, "..") != null)
        return error.InvalidBackupId;
}

fn currentGoTableBackupMetadataIdMatches(
    table_name: []const u8,
    backup_id: []const u8,
    metadata_id: []const u8,
) bool {
    const prefix = "table-";
    if (metadata_id.len != prefix.len + std.crypto.hash.sha2.Sha256.digest_length * 2 or
        !std.mem.startsWith(u8, metadata_id, prefix))
    {
        return false;
    }
    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    hasher.update(table_name);
    hasher.update(&[_]u8{0});
    hasher.update(backup_id);
    var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    hasher.final(&digest);
    const hex = std.fmt.bytesToHex(digest, .lower);
    return std.mem.eql(u8, metadata_id[prefix.len..], &hex);
}

fn validateCurrentGoClusterBackupAttempt(
    alloc: std.mem.Allocator,
    marker: *const CurrentGoClusterBackupAttemptMarker,
    expected_attempt_id: []const u8,
) !CurrentGoAttemptTimestamp {
    if (marker.version != current_go_backup_attempt_version or
        !std.mem.eql(u8, marker.attempt_id, expected_attempt_id))
    {
        return error.InvalidBackupRequest;
    }
    try validateCurrentGoBackupId(marker.attempt_id);
    try validateCurrentGoBackupId(marker.backup_id);
    if (std.mem.eql(u8, marker.attempt_id, marker.backup_id) or
        marker.expected_table_count == 0 or
        marker.expected_table_count > max_cluster_backup_attempt_tables or
        marker.table_names.len != marker.expected_table_count or
        marker.metadata_ids.len != marker.expected_table_count or
        marker.artifact_names.len > current_go_backup_attempt_max_artifacts)
    {
        return error.InvalidBackupRequest;
    }

    var table_names = std.StringHashMapUnmanaged(void).empty;
    defer table_names.deinit(alloc);
    try table_names.ensureTotalCapacity(alloc, @intCast(marker.table_names.len));
    for (marker.table_names) |name| {
        if (name.len > 4096 or !currentGoTableNameHasContent(name))
            return error.InvalidBackupRequest;
        const entry = try table_names.getOrPut(alloc, name);
        if (entry.found_existing) return error.InvalidBackupRequest;
    }

    const identity_count = std.math.add(
        usize,
        2 + marker.metadata_ids.len,
        marker.artifact_names.len,
    ) catch return error.InvalidBackupRequest;
    var identities = std.StringHashMapUnmanaged(void).empty;
    defer identities.deinit(alloc);
    try identities.ensureTotalCapacity(alloc, @intCast(identity_count));
    identities.putAssumeCapacity(marker.attempt_id, {});
    identities.putAssumeCapacity(marker.backup_id, {});
    for (marker.metadata_ids, 0..) |metadata_id, i| {
        try validateCurrentGoBackupId(metadata_id);
        if (!currentGoTableBackupMetadataIdMatches(
            marker.table_names[i],
            marker.backup_id,
            metadata_id,
        ))
            return error.InvalidBackupRequest;
        const entry = try identities.getOrPut(alloc, metadata_id);
        if (entry.found_existing) return error.InvalidBackupRequest;
    }
    for (marker.artifact_names) |artifact_name| {
        if (artifact_name.len == 0 or artifact_name.len > 4096 or
            std.mem.indexOfAny(u8, artifact_name, "/\\") != null)
        {
            return error.InvalidBackupRequest;
        }
        try validateArtifactRelativePath(artifact_name);
        const entry = try identities.getOrPut(alloc, artifact_name);
        if (entry.found_existing) return error.InvalidBackupRequest;
    }
    return parseCurrentGoAttemptTimestamp(marker.created_at);
}

fn parseCurrentGoClusterBackupAttempt(
    alloc: std.mem.Allocator,
    body: []const u8,
    expected_attempt_id: []const u8,
) !ParsedCurrentGoClusterBackupAttempt {
    var parsed = try std.json.parseFromSlice(
        CurrentGoClusterBackupAttemptMarker,
        alloc,
        body,
        .{
            .allocate = .alloc_always,
            .duplicate_field_behavior = .@"error",
            .ignore_unknown_fields = false,
        },
    );
    errdefer parsed.deinit();
    return .{
        .created_at = try validateCurrentGoClusterBackupAttempt(
            alloc,
            &parsed.value,
            expected_attempt_id,
        ),
        .parsed = parsed,
    };
}

fn readCurrentGoStableMarkerFile(
    alloc: std.mem.Allocator,
    io: std.Io,
    path: []const u8,
) ![]u8 {
    var file = if (std.fs.path.isAbsolute(path))
        try std.Io.Dir.openFileAbsolute(io, path, .{})
    else
        try std.Io.Dir.cwd().openFile(io, path, .{});
    defer file.close(io);
    const initial = try file.stat(io);
    if (initial.kind != .file) return error.InvalidBackupRequest;
    if (initial.size > max_backup_manifest_bytes)
        return error.BackupManifestTooLarge;
    var reader: std.Io.File.Reader = .initSize(file, io, &.{}, initial.size);
    const body = try reader.interface.allocRemaining(
        alloc,
        .limited(max_backup_manifest_bytes),
    );
    errdefer alloc.free(body);
    const final = try file.stat(io);
    if (initial.inode != final.inode or
        initial.size != final.size or
        !std.meta.eql(initial.mtime, final.mtime) or
        !std.meta.eql(initial.ctime, final.ctime))
    {
        return error.SourceFileChanged;
    }
    return body;
}

fn readCurrentGoAttemptMarkerForHead(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    head: *const CurrentGoClusterBackupAttemptHead,
) !ParsedCurrentGoClusterBackupAttempt {
    const suffix = try std.fmt.allocPrint(
        alloc,
        "{s}/{s}.json",
        .{ incomplete_backup_prefix, head.attempt_id },
    );
    defer alloc.free(suffix);
    const body = switch (location.*) {
        .file => |backup_root| blk: {
            const path = try incompleteBackupMarkerPath(
                alloc,
                backup_root,
                head.attempt_id,
            );
            defer alloc.free(path);
            break :blk try readCurrentGoStableMarkerFile(
                alloc,
                io,
                path,
            );
        },
        .remote => |*store| try store.readBytesAllocLimited(
            alloc,
            suffix,
            max_backup_manifest_bytes,
        ),
    };
    defer alloc.free(body);
    var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(body, &digest, .{});
    const hex = std.fmt.bytesToHex(digest, .lower);
    if (!std.mem.eql(u8, &hex, head.marker_sha256))
        return error.BackupArtifactIntegrityMismatch;
    var parsed = try parseCurrentGoClusterBackupAttempt(
        alloc,
        body,
        head.attempt_id,
    );
    errdefer parsed.deinit();
    if (!std.mem.eql(u8, parsed.parsed.value.backup_id, head.backup_id))
        return error.InvalidBackupRequest;
    return parsed;
}

fn deleteFileOrTreeWithIo(io: std.Io, path: []const u8) !bool {
    const result = if (std.fs.path.isAbsolute(path))
        std.Io.Dir.deleteFileAbsolute(io, path)
    else
        std.Io.Dir.cwd().deleteFile(io, path);
    result catch |err| switch (err) {
        error.FileNotFound => return false,
        error.IsDir => {
            try std.Io.Dir.cwd().deleteTree(io, path);
        },
        else => return err,
    };
    return true;
}

fn deletePathDurably(io: std.Io, path: []const u8) !void {
    _ = try deleteFileOrTreeWithIo(io, path);
    // A retry can observe an already-absent path after a previous unlink
    // succeeded but its directory sync failed. Re-sync the parent in both
    // cases so successful cleanup always establishes a durable absence.
    try fs_paths.syncDirPortable(
        io,
        std.fs.path.dirname(path) orelse if (std.fs.path.isAbsolute(path)) "/" else ".",
    );
}

const LocalCleanupFrame = struct {
    dir: std.Io.Dir,
    iterator: std.Io.Dir.Iterator,
    parent: std.Io.Dir,
    name: []u8,
};

fn deleteLocalCleanupLeaf(
    io: std.Io,
    dir: std.Io.Dir,
    sub_path: []const u8,
    operation_budget: *usize,
    cancellation: CancellationToken,
) !bool {
    try cancellation.check();
    if (operation_budget.* == 0) return false;
    operation_budget.* -= 1;
    dir.deleteFile(io, sub_path) catch |err| switch (err) {
        error.FileNotFound => {},
        // The entry changed from a leaf to a directory. Restart from the root
        // on the next bounded pass and reopen it without following symlinks.
        error.IsDir => return false,
        else => return err,
    };
    return true;
}

/// Deletes at most `operation_budget` filesystem entries with a separate hard
/// bound on traversal work. The explicit stack avoids attacker-controlled call
/// depth, and every directory open refuses symlink traversal so neither a
/// static link nor a concurrent directory-to-link replacement can escape the
/// backup root.
fn deleteFileOrTreeFromDirWithBudget(
    alloc: std.mem.Allocator,
    io: std.Io,
    dir: std.Io.Dir,
    sub_path: []const u8,
    operation_budget: *usize,
    cancellation: CancellationToken,
) !bool {
    try cancellation.check();
    if (operation_budget.* == 0) return false;
    var traversal_budget: usize = backup_cleanup_local_traversal_budget;
    traversal_budget -= 1;
    const root = dir.openDir(io, sub_path, .{
        .iterate = true,
        .follow_symlinks = false,
    }) catch |err| switch (err) {
        error.FileNotFound => return true,
        error.NotDir, error.SymLinkLoop => return deleteLocalCleanupLeaf(
            io,
            dir,
            sub_path,
            operation_budget,
            cancellation,
        ),
        else => return err,
    };
    var frames = std.ArrayListUnmanaged(LocalCleanupFrame).empty;
    defer {
        for (frames.items) |*frame| {
            frame.dir.close(io);
            alloc.free(frame.name);
        }
        frames.deinit(alloc);
    }
    const root_name = alloc.dupe(u8, sub_path) catch |err| {
        root.close(io);
        return err;
    };
    frames.append(alloc, .{
        .dir = root,
        .iterator = root.iterateAssumeFirstIteration(),
        .parent = dir,
        .name = root_name,
    }) catch |err| {
        alloc.free(root_name);
        root.close(io);
        return err;
    };

    while (frames.items.len > 0) {
        try cancellation.check();
        if (operation_budget.* == 0) return false;
        if (traversal_budget == 0)
            return error.BackupCleanupTraversalLimitExceeded;
        traversal_budget -= 1;
        const top = &frames.items[frames.items.len - 1];
        const entry = try top.iterator.next(io);
        if (entry) |child_entry| {
            if (child_entry.kind != .directory) {
                if (!try deleteLocalCleanupLeaf(
                    io,
                    top.dir,
                    child_entry.name,
                    operation_budget,
                    cancellation,
                )) return false;
                continue;
            }
            if (traversal_budget == 0)
                return error.BackupCleanupTraversalLimitExceeded;
            traversal_budget -= 1;
            const child = top.dir.openDir(io, child_entry.name, .{
                .iterate = true,
                .follow_symlinks = false,
            }) catch |err| switch (err) {
                error.FileNotFound => continue,
                error.NotDir, error.SymLinkLoop => {
                    if (!try deleteLocalCleanupLeaf(
                        io,
                        top.dir,
                        child_entry.name,
                        operation_budget,
                        cancellation,
                    )) return false;
                    continue;
                },
                else => return err,
            };
            const child_name = alloc.dupe(u8, child_entry.name) catch |err| {
                child.close(io);
                return err;
            };
            const parent = top.dir;
            frames.append(alloc, .{
                .dir = child,
                .iterator = child.iterateAssumeFirstIteration(),
                .parent = parent,
                .name = child_name,
            }) catch |err| {
                alloc.free(child_name);
                child.close(io);
                return err;
            };
            continue;
        }

        if (operation_budget.* == 0) return false;
        var finished = frames.pop().?;
        finished.dir.close(io);
        operation_budget.* -= 1;
        finished.parent.deleteDir(io, finished.name) catch |err| switch (err) {
            error.FileNotFound => {},
            error.DirNotEmpty => {
                alloc.free(finished.name);
                return false;
            },
            // A concurrent replacement is now a leaf. Never reopen it through
            // the stale directory type observed by the iterator.
            error.NotDir => {
                const deleted = deleteLocalCleanupLeaf(
                    io,
                    finished.parent,
                    finished.name,
                    operation_budget,
                    cancellation,
                ) catch |delete_err| {
                    alloc.free(finished.name);
                    return delete_err;
                };
                if (!deleted) {
                    alloc.free(finished.name);
                    return false;
                }
            },
            else => {
                alloc.free(finished.name);
                return err;
            },
        };
        alloc.free(finished.name);
    }
    return true;
}

/// Opens every component of a repository root relative to the previously
/// opened directory and refuses symlinks at every boundary. Opening an
/// absolute path in one syscall only applies `O_NOFOLLOW` to its final
/// component, which still permits an ancestor-swap escape.
fn openBackupRootNoFollowMode(
    io: std.Io,
    backup_root: []const u8,
    create_missing: bool,
) !std.Io.Dir {
    if (backup_root.len == 0) return error.InvalidBackupLocation;
    var components = std.fs.path.componentIterator(backup_root);
    var current: std.Io.Dir = if (components.root()) |root|
        try std.Io.Dir.openDirAbsolute(io, root, .{ .follow_symlinks = false })
    else
        std.Io.Dir.cwd();
    var current_owned = components.root() != null;
    var component_budget: usize = backup_cleanup_local_traversal_budget;
    errdefer if (current_owned) current.close(io);

    while (components.next()) |component| {
        if (std.mem.eql(u8, component.name, ".")) continue;
        if (std.mem.eql(u8, component.name, ".."))
            return error.InvalidBackupLocation;
        if (component_budget == 0)
            return error.BackupCleanupTraversalLimitExceeded;
        component_budget -= 1;
        const child = current.openDir(io, component.name, .{
            .follow_symlinks = false,
        }) catch |err| switch (err) {
            error.FileNotFound => if (create_missing) blk: {
                current.createDir(io, component.name, .default_dir) catch |create_err| switch (create_err) {
                    error.PathAlreadyExists => {},
                    else => return create_err,
                };
                try fs_paths.syncDirectoryHandlePortable(io, current);
                break :blk try current.openDir(io, component.name, .{
                    .follow_symlinks = false,
                });
            } else return err,
            else => return err,
        };
        if (current_owned) current.close(io);
        current = child;
        current_owned = true;
    }
    if (!current_owned)
        return try std.Io.Dir.cwd().openDir(io, ".", .{ .follow_symlinks = false });
    return current;
}

fn openBackupRootNoFollow(io: std.Io, backup_root: []const u8) !std.Io.Dir {
    return openBackupRootNoFollowMode(io, backup_root, false);
}

fn openOrCreateBackupRootNoFollow(io: std.Io, backup_root: []const u8) !std.Io.Dir {
    return openBackupRootNoFollowMode(io, backup_root, true);
}

const BackupRootAccess = struct {
    dir: std.Io.Dir,
    owned: bool,

    fn deinit(self: *@This(), io: std.Io) void {
        if (self.owned) self.dir.close(io);
        self.* = undefined;
    }
};

/// Borrows the repository descriptor held by a larger transaction when one is
/// available. This prevents a path rename/replacement from splitting a local
/// maintenance transaction across two repositories.
fn accessBackupRoot(
    io: std.Io,
    backup_root: []const u8,
    pinned_root: ?std.Io.Dir,
    create_missing: bool,
) !BackupRootAccess {
    if (pinned_root) |dir| return .{ .dir = dir, .owned = false };
    return .{
        .dir = if (create_missing)
            try openOrCreateBackupRootNoFollow(io, backup_root)
        else
            try openBackupRootNoFollow(io, backup_root),
        .owned = true,
    };
}

const BackupRelativeParent = struct {
    dir: std.Io.Dir,
    owned: bool,
    basename: []const u8,

    fn deinit(self: *@This(), io: std.Io) void {
        if (self.owned) self.dir.close(io);
        self.* = undefined;
    }
};

/// Resolves every parent component beneath an already-held repository root.
/// Callers then operate on the final basename so no nested symlink can redirect
/// a control-record read, lock creation, or deletion outside the repository.
fn openBackupRelativeParentNoFollow(
    io: std.Io,
    backup_root: std.Io.Dir,
    relative_path: []const u8,
) !BackupRelativeParent {
    try validateArtifactRelativePath(relative_path);
    var current = backup_root;
    var current_owned = false;
    errdefer if (current_owned) current.close(io);
    var component_budget: usize = backup_cleanup_local_traversal_budget;
    if (std.fs.path.dirname(relative_path)) |parent_path| {
        var components = std.mem.splitScalar(u8, parent_path, '/');
        while (components.next()) |component| {
            if (component_budget == 0)
                return error.BackupCleanupTraversalLimitExceeded;
            component_budget -= 1;
            const child = try current.openDir(io, component, .{
                .follow_symlinks = false,
            });
            if (current_owned) current.close(io);
            current = child;
            current_owned = true;
        }
    }
    return .{
        .dir = current,
        .owned = current_owned,
        .basename = std.fs.path.basename(relative_path),
    };
}

fn openBackupRelativeDirNoFollow(
    io: std.Io,
    backup_root: std.Io.Dir,
    relative_path: []const u8,
    iterate: bool,
) !std.Io.Dir {
    var parent = try openBackupRelativeParentNoFollow(io, backup_root, relative_path);
    defer parent.deinit(io);
    return try parent.dir.openDir(io, parent.basename, .{
        .iterate = iterate,
        .follow_symlinks = false,
    });
}

fn ensureBackupRelativeDirectoryNoFollow(
    io: std.Io,
    backup_root: std.Io.Dir,
    relative_path: []const u8,
) !void {
    try validateArtifactRelativePath(relative_path);
    var current = backup_root;
    var current_owned = false;
    defer if (current_owned) current.close(io);
    var component_budget: usize = backup_cleanup_local_traversal_budget;
    var components = std.mem.splitScalar(u8, relative_path, '/');
    while (components.next()) |component| {
        if (component_budget == 0)
            return error.BackupCleanupTraversalLimitExceeded;
        component_budget -= 1;
        const child = current.openDir(io, component, .{
            .follow_symlinks = false,
        }) catch |err| switch (err) {
            error.FileNotFound => blk: {
                current.createDir(io, component, .default_dir) catch |create_err| switch (create_err) {
                    error.PathAlreadyExists => {},
                    else => return create_err,
                };
                try fs_paths.syncDirectoryHandlePortable(io, current);
                break :blk try current.openDir(io, component, .{
                    .follow_symlinks = false,
                });
            },
            else => return err,
        };
        if (current_owned) current.close(io);
        current = child;
        current_owned = true;
    }
}

fn ensureBackupRelativeParentNoFollow(
    io: std.Io,
    backup_root: std.Io.Dir,
    relative_path: []const u8,
) !void {
    try validateArtifactRelativePath(relative_path);
    if (std.fs.path.dirname(relative_path)) |parent_path|
        try ensureBackupRelativeDirectoryNoFollow(io, backup_root, parent_path);
}

fn readFileFromBackupRootAlloc(
    alloc: std.mem.Allocator,
    io: std.Io,
    backup_root: std.Io.Dir,
    relative_path: []const u8,
    max_bytes: usize,
) ![]u8 {
    var parent = try openBackupRelativeParentNoFollow(io, backup_root, relative_path);
    defer parent.deinit(io);
    var file = try parent.dir.openFile(io, parent.basename, .{
        .allow_directory = false,
        .follow_symlinks = false,
        .resolve_beneath = true,
    });
    defer file.close(io);
    const stat = try file.stat(io);
    var reader: std.Io.File.Reader = .initSize(file, io, &.{}, stat.size);
    return try reader.interface.allocRemaining(alloc, .limited(max_bytes));
}

fn fileExistsFromBackupRoot(
    io: std.Io,
    backup_root: std.Io.Dir,
    relative_path: []const u8,
) !bool {
    var parent = openBackupRelativeParentNoFollow(
        io,
        backup_root,
        relative_path,
    ) catch |err| switch (err) {
        error.FileNotFound => return false,
        else => return err,
    };
    defer parent.deinit(io);
    var file = parent.dir.openFile(io, parent.basename, .{
        .allow_directory = false,
        .follow_symlinks = false,
        .resolve_beneath = true,
    }) catch |err| switch (err) {
        error.FileNotFound => return false,
        else => return err,
    };
    file.close(io);
    return true;
}

fn openOrCreateBackupLockFile(
    io: std.Io,
    backup_root: std.Io.Dir,
    relative_path: []const u8,
) !std.Io.File {
    var parent = try openBackupRelativeParentNoFollow(io, backup_root, relative_path);
    defer parent.deinit(io);
    var attempts: usize = 0;
    while (attempts < 8) : (attempts += 1) {
        return parent.dir.openFile(io, parent.basename, .{
            .mode = .read_write,
            .allow_directory = false,
            .follow_symlinks = false,
            .resolve_beneath = true,
        }) catch |err| switch (err) {
            error.FileNotFound => parent.dir.createFile(io, parent.basename, .{
                .read = true,
                .truncate = false,
                .exclusive = true,
                .resolve_beneath = true,
            }) catch |create_err| switch (create_err) {
                error.PathAlreadyExists => continue,
                else => return create_err,
            },
            else => return err,
        };
    }
    return error.BackupRepositoryBusy;
}

/// Acquires a cross-process publication lock without hiding an unbounded wait
/// inside a request deadline. Maintenance callers retain the kernel's
/// efficient blocking lock; cancellable request paths poll with a short capped
/// backoff so cancellation latency stays below one scheduler quantum without
/// busy-spinning under contention.
fn lockFileExclusiveWithCancellation(
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

fn deleteFileDurablyFromBackupRoot(
    io: std.Io,
    backup_root: std.Io.Dir,
    relative_path: []const u8,
) !void {
    var parent = openBackupRelativeParentNoFollow(
        io,
        backup_root,
        relative_path,
    ) catch |err| switch (err) {
        error.FileNotFound => return,
        else => return err,
    };
    defer parent.deinit(io);
    parent.dir.deleteFile(io, parent.basename) catch |err| switch (err) {
        error.FileNotFound => {},
        else => return err,
    };
    try fs_paths.syncDirectoryHandlePortable(io, parent.dir);
}

fn deleteFileDurablyFromDir(
    io: std.Io,
    dir: std.Io.Dir,
    basename: []const u8,
) !void {
    // Directory iteration already gives us a native basename. Do not apply
    // the portable artifact-path grammar here: on POSIX, '\\' is an ordinary
    // filename byte, and a malformed reclaim ticket must remain removable
    // instead of permanently poisoning its shard.
    if (basename.len == 0 or
        std.mem.indexOfScalar(u8, basename, 0) != null or
        std.mem.eql(u8, basename, ".") or
        std.mem.eql(u8, basename, ".."))
        return error.InvalidBackupArtifactPath;
    if (!std.mem.eql(u8, basename, std.fs.path.basename(basename)))
        return error.InvalidBackupArtifactPath;
    dir.deleteFile(io, basename) catch |err| switch (err) {
        error.FileNotFound => {},
        else => return err,
    };
    try fs_paths.syncDirectoryHandlePortable(io, dir);
}

fn writeFileToBackupRootIfAbsent(
    io: std.Io,
    backup_root: std.Io.Dir,
    relative_path: []const u8,
    data: []const u8,
) !bool {
    return writeFileToBackupRootIfAbsentWithCancellation(
        io,
        backup_root,
        relative_path,
        data,
        .none,
    );
}

fn writeFileToBackupRootIfAbsentWithCancellation(
    io: std.Io,
    backup_root: std.Io.Dir,
    relative_path: []const u8,
    data: []const u8,
    cancellation: CancellationToken,
) !bool {
    try cancellation.check();
    var parent = try openBackupRelativeParentNoFollow(io, backup_root, relative_path);
    defer parent.deinit(io);
    // Stage the complete bytes on the same filesystem and materialize them
    // with a no-replace link. A crash can leave at most an unnamed/private
    // staging inode, never a truncated visible lease or quarantine record.
    var atomic_file = try parent.dir.createFileAtomic(io, parent.basename, .{
        .make_path = false,
        .replace = false,
    });
    defer atomic_file.deinit(io);
    var buffer: [4096]u8 = undefined;
    var writer = atomic_file.file.writer(io, &buffer);
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
    try atomic_file.file.sync(io);
    try cancellation.check();
    atomic_file.link(io) catch |err| switch (err) {
        error.PathAlreadyExists => return false,
        else => return err,
    };
    try fs_paths.syncDirectoryHandlePortable(io, parent.dir);
    return true;
}

fn writeFileToBackupRootIfAbsentLocked(
    alloc: std.mem.Allocator,
    io: std.Io,
    backup_root: std.Io.Dir,
    relative_path: []const u8,
    data: []const u8,
) !bool {
    return writeFileToBackupRootIfAbsentLockedWithCancellation(
        alloc,
        io,
        backup_root,
        relative_path,
        data,
        .none,
    );
}

fn writeFileToBackupRootIfAbsentLockedWithCancellation(
    alloc: std.mem.Allocator,
    io: std.Io,
    backup_root: std.Io.Dir,
    relative_path: []const u8,
    data: []const u8,
    cancellation: CancellationToken,
) !bool {
    try cancellation.check();
    const lock_path = try std.fmt.allocPrint(alloc, "{s}.publish.lock", .{relative_path});
    defer alloc.free(lock_path);
    try ensureBackupRelativeParentNoFollow(io, backup_root, lock_path);
    var lock_file = try openOrCreateBackupLockFile(io, backup_root, lock_path);
    defer lock_file.close(io);
    try lockFileExclusiveWithCancellation(io, lock_file, cancellation);
    defer lock_file.unlock(io);
    return writeFileToBackupRootIfAbsentWithCancellation(
        io,
        backup_root,
        relative_path,
        data,
        cancellation,
    );
}

fn copyFileToBackupRootIfAbsentLocked(
    alloc: std.mem.Allocator,
    io: std.Io,
    backup_root: std.Io.Dir,
    relative_path: []const u8,
    source_path: []const u8,
) !bool {
    const lock_path = try std.fmt.allocPrint(alloc, "{s}.publish.lock", .{relative_path});
    defer alloc.free(lock_path);
    try ensureBackupRelativeParentNoFollow(io, backup_root, lock_path);
    var lock_file = try openOrCreateBackupLockFile(io, backup_root, lock_path);
    defer lock_file.close(io);
    try lock_file.lock(io, .exclusive);
    defer lock_file.unlock(io);
    if (try fileExistsFromBackupRoot(io, backup_root, relative_path)) return false;

    var parent = try openBackupRelativeParentNoFollow(io, backup_root, relative_path);
    defer parent.deinit(io);
    var source = if (std.fs.path.isAbsolute(source_path))
        try std.Io.Dir.openFileAbsolute(io, source_path, .{})
    else
        try std.Io.Dir.cwd().openFile(io, source_path, .{});
    defer source.close(io);
    const source_stat = try source.stat(io);
    var atomic_file = try parent.dir.createFileAtomic(io, parent.basename, .{
        .make_path = false,
        .replace = false,
    });
    defer atomic_file.deinit(io);
    var writer_buffer: [64 * 1024]u8 = undefined;
    var writer = atomic_file.file.writer(io, &writer_buffer);
    var source_reader: std.Io.File.Reader = .initSize(source, io, &.{}, source_stat.size);
    _ = writer.interface.sendFileAll(&source_reader, .unlimited) catch |err| switch (err) {
        error.ReadFailed => return source_reader.err.?,
        error.WriteFailed => return writer.err.?,
    };
    try writer.end();
    const final_source_stat = try source.stat(io);
    if (final_source_stat.size != source_stat.size or
        !std.meta.eql(final_source_stat.mtime, source_stat.mtime))
        return error.SourceFileChanged;
    try atomic_file.file.sync(io);
    atomic_file.link(io) catch |err| switch (err) {
        error.PathAlreadyExists => return false,
        else => return err,
    };
    try fs_paths.syncDirectoryHandlePortable(io, parent.dir);
    return true;
}

fn validateFileIntegrity(
    alloc: std.mem.Allocator,
    io: std.Io,
    path: []const u8,
    expected_size: u64,
    expected_sha256: []const u8,
) !void {
    if (!isLowerSha256Hex(expected_sha256)) return error.BackupIntegrityMissing;
    var integrity = try artifactIntegrityAlloc(alloc, io, .portable, path);
    defer integrity.deinit(alloc);
    if (integrity.size_bytes != expected_size or
        !std.mem.eql(u8, integrity.sha256, expected_sha256))
        return error.BackupArtifactIntegrityMismatch;
}

fn validateBackupRootFileIntegrity(
    io: std.Io,
    backup_root: std.Io.Dir,
    relative_path: []const u8,
    expected_size: u64,
    expected_sha256: []const u8,
) !void {
    if (!isLowerSha256Hex(expected_sha256)) return error.BackupIntegrityMissing;
    var parent = try openBackupRelativeParentNoFollow(io, backup_root, relative_path);
    defer parent.deinit(io);
    var file = try parent.dir.openFile(io, parent.basename, .{
        .allow_directory = false,
        .follow_symlinks = false,
        .resolve_beneath = true,
    });
    defer file.close(io);
    const stat = try file.stat(io);
    if (stat.size != expected_size) return error.BackupArtifactIntegrityMismatch;
    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    try hashFileContents(io, file, stat, &hasher);
    var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    hasher.final(&digest);
    const actual = std.fmt.bytesToHex(digest, .lower);
    if (!std.mem.eql(u8, &actual, expected_sha256))
        return error.BackupArtifactIntegrityMismatch;
}

fn verifyRemoteBlob(
    alloc: std.mem.Allocator,
    store: *RemoteBackupStore,
    key: []const u8,
    expected_size: u64,
    expected_sha256: []const u8,
    expected_etag: ?[]const u8,
) !u64 {
    if (!isLowerSha256Hex(expected_sha256)) return error.BackupIntegrityMissing;
    var metadata = try store.client.statObject(store.bucket, key);
    defer metadata.deinit(alloc);
    if (metadata.content_length != expected_size)
        return error.BackupArtifactIntegrityMismatch;
    const etag = metadata.etag orelse return error.RestoreArtifactIdentityMissing;
    if (expected_etag) |identity| {
        if (!std.mem.eql(u8, identity, etag)) return error.SourceFileChanged;
    }
    if (remoteFullSha256ProofMatches(metadata, expected_sha256)) |matches| {
        if (!matches) return error.BackupArtifactIntegrityMismatch;
    } else {
        // Providers without a full-object SHA-256 still receive the same
        // guarantee by hashing one GET pinned to the exact post-upload ETag.
        var hasher = std.crypto.hash.sha2.Sha256.init(.{});
        try store.hashObjectVersion(alloc, key, expected_size, etag, &hasher);
        var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
        hasher.final(&digest);
        const actual = std.fmt.bytesToHex(digest, .lower);
        if (!std.mem.eql(u8, &actual, expected_sha256))
            return error.BackupArtifactIntegrityMismatch;
    }
    var identity = std.hash.Wyhash.hash(expected_size, etag);
    if (identity == 0) identity = 1;
    return identity;
}

/// Returns null when provider metadata cannot prove a full-object SHA-256.
/// A supported but mismatched checksum is a conclusive integrity failure.
fn remoteFullSha256ProofMatches(
    metadata: object_storage.ObjectMetadata,
    expected_sha256: []const u8,
) ?bool {
    if (metadata.checksum_scope != .object) return null;
    const checksum = metadata.checksum orelse return null;
    if (checksum.checksum_type != .full_object) return null;
    return switch (checksum.algorithm) {
        .sha256_hex => std.ascii.eqlIgnoreCase(checksum.value, expected_sha256),
        .sha256_base64 => blk: {
            var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
            _ = std.fmt.hexToBytes(&digest, expected_sha256) catch break :blk false;
            var encoded: [std.base64.standard.Encoder.calcSize(digest.len)]u8 = undefined;
            const value = std.base64.standard.Encoder.encode(&encoded, &digest);
            break :blk std.mem.eql(u8, checksum.value, value);
        },
        else => null,
    };
}

fn replaceFileInBackupRootUnderHeldLock(
    alloc: std.mem.Allocator,
    io: std.Io,
    backup_root: std.Io.Dir,
    relative_path: []const u8,
    data: []const u8,
) !void {
    return replaceFileInBackupRootUnderHeldLockWithHookAndCancellation(
        alloc,
        io,
        backup_root,
        relative_path,
        data,
        null,
        .none,
    );
}

fn replaceFileInBackupRootUnderHeldLockWithHook(
    alloc: std.mem.Allocator,
    io: std.Io,
    backup_root: std.Io.Dir,
    relative_path: []const u8,
    data: []const u8,
    test_hook: ?*BackupStagingPublicationTestHook,
) !void {
    return replaceFileInBackupRootUnderHeldLockWithHookAndCancellation(
        alloc,
        io,
        backup_root,
        relative_path,
        data,
        test_hook,
        .none,
    );
}

fn replaceFileInBackupRootUnderHeldLockWithHookAndCancellation(
    alloc: std.mem.Allocator,
    io: std.Io,
    backup_root: std.Io.Dir,
    relative_path: []const u8,
    data: []const u8,
    test_hook: ?*BackupStagingPublicationTestHook,
    cancellation: CancellationToken,
) !void {
    try cancellation.check();
    var parent = try openBackupRelativeParentNoFollow(io, backup_root, relative_path);
    defer parent.deinit(io);
    var entropy: [8]u8 = undefined;
    try io.randomSecure(&entropy);
    const nonce = std.fmt.bytesToHex(entropy, .lower);
    const tmp_name = try std.fmt.allocPrint(alloc, "{s}.tmp-{s}", .{ parent.basename, &nonce });
    defer alloc.free(tmp_name);
    var file = try parent.dir.createFile(io, tmp_name, .{
        .truncate = true,
        .exclusive = true,
        .resolve_beneath = true,
    });
    var file_open = true;
    errdefer {
        if (file_open) file.close(io);
        parent.dir.deleteFile(io, tmp_name) catch {};
    }
    defer if (file_open) file.close(io);
    var buffer: [4096]u8 = undefined;
    var writer = file.writer(io, &buffer);
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
    if (test_hook) |hook| hook.pauseAfterSyncInDir(parent.dir, tmp_name);
    try cancellation.check();
    file.close(io);
    file_open = false;
    try std.Io.Dir.rename(parent.dir, tmp_name, parent.dir, parent.basename, io);
    try fs_paths.syncDirectoryHandlePortable(io, parent.dir);
}

fn replaceFileFromBackupStagingDirUnderHeldLockWithHook(
    alloc: std.mem.Allocator,
    io: std.Io,
    backup_root: std.Io.Dir,
    relative_path: []const u8,
    relative_staging_dir: []const u8,
    data: []const u8,
    test_hook: ?*BackupStagingPublicationTestHook,
) !void {
    try ensureBackupRelativeParentNoFollow(io, backup_root, relative_path);
    try ensureBackupRelativeDirectoryNoFollow(io, backup_root, relative_staging_dir);
    var destination = try openBackupRelativeParentNoFollow(io, backup_root, relative_path);
    defer destination.deinit(io);
    const tmp_path = try std.fmt.allocPrint(alloc, "{s}/{s}.replace.tmp", .{
        relative_staging_dir,
        destination.basename,
    });
    defer alloc.free(tmp_path);
    var staging = try openBackupRelativeParentNoFollow(io, backup_root, tmp_path);
    defer staging.deinit(io);
    staging.dir.deleteFile(io, staging.basename) catch |err| switch (err) {
        error.FileNotFound => {},
        else => return err,
    };
    var file = try staging.dir.createFile(io, staging.basename, .{
        .truncate = false,
        .exclusive = true,
        .resolve_beneath = true,
    });
    var file_open = true;
    errdefer {
        if (file_open) file.close(io);
        staging.dir.deleteFile(io, staging.basename) catch {};
    }
    defer if (file_open) file.close(io);
    var buffer: [4096]u8 = undefined;
    var writer = file.writer(io, &buffer);
    try writer.interface.writeAll(data);
    try writer.end();
    try file.sync(io);
    file.close(io);
    file_open = false;
    if (test_hook) |hook| hook.pauseAfterSyncInDir(staging.dir, staging.basename);
    try std.Io.Dir.rename(
        staging.dir,
        staging.basename,
        destination.dir,
        destination.basename,
        io,
    );
    try fs_paths.syncDirectoryHandlePortable(io, staging.dir);
    try fs_paths.syncDirectoryHandlePortable(io, destination.dir);
}

fn deletePathDurablyFromBackupRootWithBudgetAndCancellation(
    alloc: std.mem.Allocator,
    io: std.Io,
    backup_root: std.Io.Dir,
    relative_path: []const u8,
    operation_budget: *usize,
    cancellation: CancellationToken,
) !void {
    try cancellation.check();
    try validateArtifactRelativePath(relative_path);
    if (!try deleteFileOrTreeFromDirWithBudget(
        alloc,
        io,
        backup_root,
        relative_path,
        operation_budget,
        cancellation,
    )) return error.BackupCleanupBudgetExceeded;
    try cancellation.check();
    try fs_paths.syncDirectoryHandlePortable(io, backup_root);
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
        true,
    );
}

/// Rolls back an unpublished table generation without losing its durable
/// retry address. Payloads are removed first, the writer lease is retired
/// second, and the logical reservation is deleted last. Any intermediate
/// failure therefore leaves the exact backup/artifact binding available for
/// an idempotent retry instead of stranding an undiscoverable lease.
pub const TableBackupWriterStateCleanup = enum {
    /// The lease belongs to a forwarding coordinator and must outlive this
    /// storage-owner rollback.
    preserve,
    /// This executor created, or may have created, the lease. Conditionally
    /// retire it when the durable owner still matches this artifact.
    retire_if_owned,
};

pub fn cleanupUnpublishedTableBackupAttemptAndWriterStateAtLocation(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    backup_id: []const u8,
    artifact_backup_id: []const u8,
    format: BackupFormat,
    writer_state_cleanup: TableBackupWriterStateCleanup,
) !void {
    return cleanupUnpublishedTableBackupAttemptAndWriterStateAtLocationWithCancellation(
        alloc,
        io,
        location,
        backup_id,
        artifact_backup_id,
        format,
        writer_state_cleanup,
        .none,
    );
}

pub fn cleanupUnpublishedTableBackupAttemptAndWriterStateAtLocationWithCancellation(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    backup_id: []const u8,
    artifact_backup_id: []const u8,
    format: BackupFormat,
    writer_state_cleanup: TableBackupWriterStateCleanup,
    cancellation: CancellationToken,
) !void {
    var object_budget: usize = backup_attempt_cleanup_object_budget;
    const progress = try advanceUnpublishedTableBackupAttemptCleanupAtLocationWithHook(
        alloc,
        io,
        location,
        backup_id,
        artifact_backup_id,
        format,
        writer_state_cleanup,
        &object_budget,
        cancellation,
        null,
    );
    if (progress == .incomplete) return error.BackupCleanupBudgetExceeded;
}

pub const TableBackupAttemptCleanupProgress = enum {
    complete,
    incomplete,
};

/// Advances rollback by at most `object_budget` storage operations. Payloads
/// are removed before writer state, and reservations remain durable until
/// lease retirement has an unambiguous terminal outcome. Repeating this call
/// is idempotent and is the common primitive for request and maintenance work.
pub fn advanceUnpublishedTableBackupAttemptCleanupAtLocationWithCancellation(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    backup_id: []const u8,
    artifact_backup_id: []const u8,
    format: BackupFormat,
    writer_state_cleanup: TableBackupWriterStateCleanup,
    object_budget: *usize,
    cancellation: CancellationToken,
) !TableBackupAttemptCleanupProgress {
    return advanceUnpublishedTableBackupAttemptCleanupAtLocationWithHook(
        alloc,
        io,
        location,
        backup_id,
        artifact_backup_id,
        format,
        writer_state_cleanup,
        object_budget,
        cancellation,
        null,
    );
}

const TableBackupCleanupBeforeWriterStateHook = *const fn () anyerror!void;

fn advanceUnpublishedTableBackupAttemptCleanupAtLocationWithHook(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    backup_id: []const u8,
    artifact_backup_id: []const u8,
    format: BackupFormat,
    writer_state_cleanup: TableBackupWriterStateCleanup,
    object_budget: *usize,
    cancellation: CancellationToken,
    before_writer_state: ?TableBackupCleanupBeforeWriterStateHook,
) !TableBackupAttemptCleanupProgress {
    cleanupTableBackupAttemptAtLocationWithBudgetAndCancellation(
        alloc,
        io,
        location,
        backup_id,
        artifact_backup_id,
        format,
        object_budget,
        false,
        false,
        cancellation,
    ) catch |err| switch (err) {
        error.BackupCleanupBudgetExceeded => return .incomplete,
        else => return err,
    };
    if (before_writer_state) |hook| try hook();
    try cancellation.check();
    if (writer_state_cleanup == .retire_if_owned) {
        const retirement_cost: usize = switch (location.*) {
            .file => 1,
            .remote => 2,
        };
        if (object_budget.* < retirement_cost) return .incomplete;
        object_budget.* -= retirement_cost;
        const retirement = try retireTableBackupWriterLeaseAtLocationWithCancellation(
            alloc,
            io,
            location,
            artifact_backup_id,
            cancellation,
        );
        switch (retirement) {
            .absent, .retired => {},
            .different_owner, .raced => return error.BackupWriterLeaseRetirementDeferred,
        }
    }
    const reservation_cost: usize = switch (location.*) {
        .file => 1,
        .remote => 2,
    };
    if (object_budget.* < reservation_cost) return .incomplete;
    object_budget.* -= reservation_cost;
    _ = try deleteTableBackupReservationIfArtifactOwnedAtLocationWithCancellation(
        alloc,
        io,
        location,
        backup_id,
        artifact_backup_id,
        cancellation,
    );
    if (!std.mem.eql(u8, backup_id, artifact_backup_id)) {
        if (object_budget.* < reservation_cost) return .incomplete;
        object_budget.* -= reservation_cost;
        _ = try deleteTableBackupReservationIfArtifactOwnedAtLocationWithCancellation(
            alloc,
            io,
            location,
            artifact_backup_id,
            artifact_backup_id,
            cancellation,
        );
    }
    return .complete;
}

/// Removes the temporary manifest and reservation published by a forwarded
/// storage-owner hop while preserving its payload for the coordinator's
/// canonical manifest. This is best-effort after canonical publication, but
/// forms part of confirmed rollback before publication.
pub fn retireForwardedTableBackupEnvelopeAtLocation(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    backup_id: []const u8,
) !void {
    try validateBackupId(backup_id);
    switch (location.*) {
        .file => |backup_root| {
            var backup_dir = try openBackupRootNoFollow(io, backup_root);
            defer backup_dir.close(io);
            const manifest_path = try metadataPath(alloc, "", backup_id);
            defer alloc.free(manifest_path);
            try deleteFileDurablyFromBackupRoot(
                io,
                backup_dir,
                trimLeftSlash(manifest_path),
            );
            const reservation_path = try reservationPath(alloc, "", backup_id, false);
            defer alloc.free(reservation_path);
            try deleteFileDurablyFromBackupRoot(
                io,
                backup_dir,
                trimLeftSlash(reservation_path),
            );
        },
        .remote => |*store| {
            const manifest_suffix = try metadataPath(alloc, "", backup_id);
            defer alloc.free(manifest_suffix);
            try store.deleteSuffix(alloc, trimLeftSlash(manifest_suffix));
            const reservation_suffix = try reservationPath(alloc, "", backup_id, false);
            defer alloc.free(reservation_suffix);
            try store.deleteSuffix(alloc, trimLeftSlash(reservation_suffix));
        },
    }
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
    delete_logical_reservation: bool,
) !void {
    return cleanupTableBackupAttemptAtLocationWithBudgetAndCancellation(
        alloc,
        io,
        location,
        backup_id,
        artifact_backup_id,
        format,
        object_budget,
        manifest_owned,
        delete_logical_reservation,
        .none,
    );
}

fn cleanupTableBackupAttemptAtLocationWithBudgetAndCancellation(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    backup_id: []const u8,
    artifact_backup_id: []const u8,
    format: BackupFormat,
    object_budget: *usize,
    manifest_owned: bool,
    delete_logical_reservation: bool,
    cancellation: CancellationToken,
) !void {
    return cleanupTableBackupAttemptAtLocationWithRootBudget(
        alloc,
        io,
        location,
        backup_id,
        artifact_backup_id,
        format,
        object_budget,
        manifest_owned,
        delete_logical_reservation,
        null,
        cancellation,
    );
}

fn cleanupTableBackupAttemptAtLocationWithRootBudget(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    backup_id: []const u8,
    artifact_backup_id: []const u8,
    format: BackupFormat,
    object_budget: *usize,
    manifest_owned: bool,
    delete_logical_reservation: bool,
    pinned_root: ?std.Io.Dir,
    cancellation: CancellationToken,
) !void {
    try cancellation.check();
    try validateBackupId(backup_id);
    try validateBackupId(artifact_backup_id);
    switch (location.*) {
        .file => |backup_root| {
            var root = try accessBackupRoot(io, backup_root, pinned_root, false);
            defer root.deinit(io);
            const backup_dir = root.dir;
            if (manifest_owned) {
                try cancellation.check();
                const manifest_path = try metadataPath(alloc, "", backup_id);
                defer alloc.free(manifest_path);
                // Remove and durably fence the table commit record before its
                // payload. A crash must never resurrect a manifest whose
                // artifact cleanup had already started.
                try deletePathDurablyFromBackupRootWithBudgetAndCancellation(
                    alloc,
                    io,
                    backup_dir,
                    trimLeftSlash(manifest_path),
                    object_budget,
                    cancellation,
                );
            }
            if (!std.mem.eql(u8, backup_id, artifact_backup_id)) {
                try cancellation.check();
                const forwarded_manifest_path = try metadataPath(alloc, "", artifact_backup_id);
                defer alloc.free(forwarded_manifest_path);
                // A successful storage-owner hop publishes this temporary
                // envelope before the coordinator installs the logical
                // manifest. Remove it before its payload during rollback or
                // stale-attempt reconciliation.
                try deletePathDurablyFromBackupRootWithBudgetAndCancellation(
                    alloc,
                    io,
                    backup_dir,
                    trimLeftSlash(forwarded_manifest_path),
                    object_budget,
                    cancellation,
                );
            }
            switch (format) {
                .native => {
                    try cancellation.check();
                    try deletePathDurablyFromBackupRootWithBudgetAndCancellation(
                        alloc,
                        io,
                        backup_dir,
                        artifact_backup_id,
                        object_budget,
                        cancellation,
                    );
                },
                .portable => {
                    // Legacy/standalone portable backups use one sibling file;
                    // provisioned multi-range backups use a directory of
                    // per-group files. Reclaim both shapes idempotently.
                    const legacy_artifact_path = try std.fmt.allocPrint(alloc, "{s}.afb", .{artifact_backup_id});
                    defer alloc.free(legacy_artifact_path);
                    try cancellation.check();
                    try deletePathDurablyFromBackupRootWithBudgetAndCancellation(
                        alloc,
                        io,
                        backup_dir,
                        legacy_artifact_path,
                        object_budget,
                        cancellation,
                    );
                    try cancellation.check();
                    try deletePathDurablyFromBackupRootWithBudgetAndCancellation(
                        alloc,
                        io,
                        backup_dir,
                        artifact_backup_id,
                        object_budget,
                        cancellation,
                    );
                },
            }
            if (delete_logical_reservation) {
                try cancellation.check();
                const reservation_path = try reservationPath(alloc, "", backup_id, false);
                defer alloc.free(reservation_path);
                try deletePathDurablyFromBackupRootWithBudgetAndCancellation(
                    alloc,
                    io,
                    backup_dir,
                    trimLeftSlash(reservation_path),
                    object_budget,
                    cancellation,
                );
            }
            if (!std.mem.eql(u8, backup_id, artifact_backup_id)) {
                try cancellation.check();
                const forwarded_reservation_path = try reservationPath(alloc, "", artifact_backup_id, false);
                defer alloc.free(forwarded_reservation_path);
                try deletePathDurablyFromBackupRootWithBudgetAndCancellation(
                    alloc,
                    io,
                    backup_dir,
                    trimLeftSlash(forwarded_reservation_path),
                    object_budget,
                    cancellation,
                );
            }
        },
        .remote => |*store| {
            if (manifest_owned) {
                const manifest_suffix = try metadataPath(alloc, "", backup_id);
                defer alloc.free(manifest_suffix);
                try store.deleteSuffixBudgetedWithCancellation(alloc, trimLeftSlash(manifest_suffix), object_budget, cancellation);
            }
            if (!std.mem.eql(u8, backup_id, artifact_backup_id)) {
                const forwarded_manifest_suffix = try metadataPath(alloc, "", artifact_backup_id);
                defer alloc.free(forwarded_manifest_suffix);
                try store.deleteSuffixBudgetedWithCancellation(alloc, trimLeftSlash(forwarded_manifest_suffix), object_budget, cancellation);
            }
            switch (format) {
                .native => try store.deletePrefixWithCancellation(alloc, artifact_backup_id, object_budget, cancellation),
                .portable => {
                    const artifact_suffix = try std.fmt.allocPrint(alloc, "{s}.afb", .{artifact_backup_id});
                    defer alloc.free(artifact_suffix);
                    try store.deleteSuffixBudgetedWithCancellation(alloc, artifact_suffix, object_budget, cancellation);
                    try store.deletePrefixWithCancellation(alloc, artifact_backup_id, object_budget, cancellation);
                },
            }
            if (delete_logical_reservation) {
                const reservation_suffix = try reservationPath(alloc, "", backup_id, false);
                defer alloc.free(reservation_suffix);
                try store.deleteSuffixBudgetedWithCancellation(alloc, trimLeftSlash(reservation_suffix), object_budget, cancellation);
            }
            if (!std.mem.eql(u8, backup_id, artifact_backup_id)) {
                const forwarded_reservation_suffix = try reservationPath(alloc, "", artifact_backup_id, false);
                defer alloc.free(forwarded_reservation_suffix);
                try store.deleteSuffixBudgetedWithCancellation(alloc, trimLeftSlash(forwarded_reservation_suffix), object_budget, cancellation);
            }
        },
    }
}

fn deleteClusterBackupAttemptMarkerFromBackupRoot(
    alloc: std.mem.Allocator,
    io: std.Io,
    backup_dir: std.Io.Dir,
    marker: *const ClusterBackupAttemptMarker,
) !void {
    const cursor_path = try clusterWriterLeaseScanCursorPath(alloc, "", marker.attempt_id);
    defer alloc.free(cursor_path);
    try deleteFileDurablyFromBackupRoot(io, backup_dir, trimLeftSlash(cursor_path));
    const publication_lock_path = try localBackupAttemptPublicationLockPath(
        alloc,
        "",
        marker.attempt_id,
    );
    defer alloc.free(publication_lock_path);
    var publication_lock = try openOrCreateBackupLockFile(
        io,
        backup_dir,
        trimLeftSlash(publication_lock_path),
    );
    defer publication_lock.close(io);
    try publication_lock.lock(io, .exclusive);
    defer publication_lock.unlock(io);

    const path = try incompleteBackupMarkerPath(alloc, "", marker.attempt_id);
    defer alloc.free(path);
    try deleteFileDurablyFromBackupRoot(io, backup_dir, trimLeftSlash(path));
    // Successful attempts normally complete before maintenance moves their
    // future-dated ticket. If maintenance already claimed or moved it, the
    // marker-first ordering lets the selector drop the orphan on its next pass.
    const ticket_path = try localBackupAttemptReclaimTicketPath(
        alloc,
        "",
        .queued,
        localBackupAttemptReclaimShard(
            marker.created_at_unix_ns +| backup_attempt_reclaim_age_ns,
        ),
        marker.attempt_id,
    );
    defer alloc.free(ticket_path);
    try deleteFileDurablyFromBackupRoot(
        io,
        backup_dir,
        trimLeftSlash(ticket_path),
    );
}

pub fn deleteClusterBackupAttemptMarker(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    marker: *const ClusterBackupAttemptMarker,
) !void {
    return deleteClusterBackupAttemptMarkerWithRoot(
        alloc,
        io,
        location,
        marker,
        null,
    );
}

fn deleteClusterBackupAttemptMarkerWithRoot(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    marker: *const ClusterBackupAttemptMarker,
    pinned_root: ?std.Io.Dir,
) !void {
    try validateClusterBackupAttemptMarker(alloc, marker, marker.attempt_id);
    // Progress is advisory but must not outlive its attempt marker. Removing
    // it first is crash-safe: a retry merely restarts an idempotent bounded
    // lease scan from table zero.
    switch (location.*) {
        .file => |backup_root| {
            var root = try accessBackupRoot(io, backup_root, pinned_root, false);
            defer root.deinit(io);
            try deleteClusterBackupAttemptMarkerFromBackupRoot(
                alloc,
                io,
                root.dir,
                marker,
            );
        },
        .remote => |*store| {
            try deleteClusterWriterLeaseScanCursor(alloc, io, location, marker.attempt_id);
            const suffix = try incompleteBackupMarkerPath(alloc, "", marker.attempt_id);
            defer alloc.free(suffix);
            try store.deleteSuffix(alloc, trimLeftSlash(suffix));
        },
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
            var backup_dir = openBackupRootNoFollow(io, backup_root) catch |err| switch (err) {
                error.FileNotFound => return,
                else => return err,
            };
            defer backup_dir.close(io);
            const relative_path = trimLeftSlash(reservation_suffix);
            const lock_path = try std.fmt.allocPrint(alloc, "{s}.publish.lock", .{relative_path});
            defer alloc.free(lock_path);
            var lock_file = try openOrCreateBackupLockFile(io, backup_dir, lock_path);
            defer lock_file.close(io);
            try lock_file.lock(io, .exclusive);
            defer lock_file.unlock(io);
            try deleteFileDurablyFromBackupRoot(
                io,
                backup_dir,
                relative_path,
            );
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
    return cleanupClusterReservationIfOwnedAtLocationWithRoot(
        alloc,
        io,
        location,
        backup_id,
        attempt_id,
        null,
    );
}

fn cleanupClusterReservationIfOwnedAtLocationWithRoot(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    backup_id: []const u8,
    attempt_id: []const u8,
    pinned_root: ?std.Io.Dir,
) !bool {
    try validateBackupLeaseOwner(attempt_id);
    const reservation_suffix = try reservationPath(alloc, "", backup_id, true);
    defer alloc.free(reservation_suffix);
    return switch (location.*) {
        .file => |backup_root| blk: {
            var root = accessBackupRoot(io, backup_root, pinned_root, false) catch |err| switch (err) {
                error.FileNotFound => break :blk false,
                else => return err,
            };
            defer root.deinit(io);
            const backup_dir = root.dir;
            const relative_path = trimLeftSlash(reservation_suffix);
            const lock_path = try std.fmt.allocPrint(alloc, "{s}.publish.lock", .{relative_path});
            defer alloc.free(lock_path);
            var lock_file = try openOrCreateBackupLockFile(io, backup_dir, lock_path);
            defer lock_file.close(io);
            try lock_file.lock(io, .exclusive);
            defer lock_file.unlock(io);

            const body = readFileFromBackupRootAlloc(
                alloc,
                io,
                backup_dir,
                relative_path,
                max_backup_attempt_lease_bytes,
            ) catch |err| switch (err) {
                error.FileNotFound => break :blk false,
                else => return err,
            };
            defer alloc.free(body);
            if (!std.mem.eql(u8, reservationOwner(body), attempt_id))
                break :blk false;
            try deleteFileDurablyFromBackupRoot(io, backup_dir, relative_path);
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
    return clusterReservationOwnerMatchesAtLocationWithRoot(
        alloc,
        io,
        location,
        backup_id,
        attempt_id,
        null,
    );
}

fn clusterReservationOwnerMatchesAtLocationWithRoot(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    backup_id: []const u8,
    attempt_id: []const u8,
    pinned_root: ?std.Io.Dir,
) !?bool {
    try validateBackupLeaseOwner(attempt_id);
    const reservation_suffix = try reservationPath(alloc, "", backup_id, true);
    defer alloc.free(reservation_suffix);
    return switch (location.*) {
        .file => |backup_root| blk: {
            var root = accessBackupRoot(io, backup_root, pinned_root, false) catch |err| switch (err) {
                error.FileNotFound => break :blk null,
                else => return err,
            };
            defer root.deinit(io);
            const backup_dir = root.dir;
            const relative_path = trimLeftSlash(reservation_suffix);
            const lock_path = try std.fmt.allocPrint(alloc, "{s}.publish.lock", .{relative_path});
            defer alloc.free(lock_path);
            var lock_file = try openOrCreateBackupLockFile(io, backup_dir, lock_path);
            defer lock_file.close(io);
            try lock_file.lock(io, .exclusive);
            defer lock_file.unlock(io);
            const body = readFileFromBackupRootAlloc(
                alloc,
                io,
                backup_dir,
                relative_path,
                max_backup_attempt_lease_bytes,
            ) catch |err| switch (err) {
                error.FileNotFound => break :blk null,
                else => return err,
            };
            defer alloc.free(body);
            break :blk std.mem.eql(u8, reservationOwner(body), attempt_id);
        },
        .remote => |*store| try store.suffixOwnerMatches(
            alloc,
            trimLeftSlash(reservation_suffix),
            attempt_id,
        ),
    };
}

fn clusterReservationOwnerMatchesAttemptAtLocation(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    backup_id: []const u8,
    attempt_id: []const u8,
) !bool {
    return clusterReservationOwnerMatchesAttemptAtLocationWithRoot(
        alloc,
        io,
        location,
        backup_id,
        attempt_id,
        null,
    );
}

fn clusterReservationOwnerMatchesAttemptAtLocationWithRoot(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    backup_id: []const u8,
    attempt_id: []const u8,
    pinned_root: ?std.Io.Dir,
) !bool {
    if ((try clusterReservationOwnerMatchesAtLocationWithRoot(
        alloc,
        io,
        location,
        backup_id,
        attempt_id,
        pinned_root,
    )) == true) return true;
    var cleanup_owner_buf: [backup_cleanup_lease_owner_prefix.len + 128]u8 = undefined;
    const cleanup_owner = try clusterCleanupLeaseOwner(attempt_id, &cleanup_owner_buf);
    if ((try clusterReservationOwnerMatchesAtLocationWithRoot(
        alloc,
        io,
        location,
        backup_id,
        cleanup_owner,
        pinned_root,
    )) == true) return true;
    var legacy_cleanup_owner_buf: [legacy_backup_cleanup_lease_owner_prefix.len + 128]u8 = undefined;
    const legacy_cleanup_owner = try legacyClusterCleanupLeaseOwner(
        attempt_id,
        &legacy_cleanup_owner_buf,
    );
    return (try clusterReservationOwnerMatchesAtLocationWithRoot(
        alloc,
        io,
        location,
        backup_id,
        legacy_cleanup_owner,
        pinned_root,
    )) == true;
}

fn clusterReservationAttemptIdAtLocation(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    backup_id: []const u8,
) !?[]u8 {
    return clusterReservationAttemptIdAtLocationWithRoot(
        alloc,
        io,
        location,
        backup_id,
        null,
    );
}

fn clusterReservationAttemptIdAtLocationWithRoot(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    backup_id: []const u8,
    pinned_root: ?std.Io.Dir,
) !?[]u8 {
    const reservation_suffix = try reservationPath(alloc, "", backup_id, true);
    defer alloc.free(reservation_suffix);
    const body = switch (location.*) {
        .file => |backup_root| blk: {
            var root = accessBackupRoot(io, backup_root, pinned_root, false) catch |err| switch (err) {
                error.FileNotFound => return null,
                else => return err,
            };
            defer root.deinit(io);
            break :blk readFileFromBackupRootAlloc(
                alloc,
                io,
                root.dir,
                trimLeftSlash(reservation_suffix),
                max_backup_attempt_lease_bytes,
            ) catch |err| switch (err) {
                error.FileNotFound => return null,
                else => return err,
            };
        },
        .remote => |*store| store.readBytesAllocLimited(
            alloc,
            trimLeftSlash(reservation_suffix),
            max_backup_attempt_lease_bytes,
        ) catch |err| switch (err) {
            error.FileNotFound => return null,
            else => return err,
        },
    };
    defer alloc.free(body);
    const lease = try parseClusterBackupReservationLease(body);
    const attempt_id = if (std.mem.startsWith(u8, lease.attempt_id, backup_cleanup_lease_owner_prefix))
        lease.attempt_id[backup_cleanup_lease_owner_prefix.len..]
    else if (std.mem.startsWith(u8, lease.attempt_id, legacy_backup_cleanup_lease_owner_prefix)) blk: {
        // The legacy cleanup namespace overlapped the public backup-ID
        // alphabet. Prefer an exact journal when one exists so a legitimate
        // `antfly-cleanup-*` attempt is never mistaken for a tombstone. When
        // it does not, preserve rolling-upgrade recovery by resolving the old
        // cleanup owner to its suffix.
        var exact_marker = readClusterBackupAttemptMarkerWithRoot(
            alloc,
            io,
            location,
            lease.attempt_id,
            pinned_root,
        ) catch |err| switch (err) {
            error.FileNotFound => break :blk lease.attempt_id[legacy_backup_cleanup_lease_owner_prefix.len..],
            else => return err,
        };
        exact_marker.deinit();
        break :blk lease.attempt_id;
    } else lease.attempt_id;
    try validateBackupId(attempt_id);
    return try alloc.dupe(u8, attempt_id);
}

/// Atomically claim an expired lease for cleanup. Returns the old owner only
/// when the exact lease version observed as expired was removed.
fn takeExpiredClusterReservationAtLocation(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    backup_id: []const u8,
    now_unix_ns: u64,
    expected_owner: ?[]const u8,
) !?[]u8 {
    const reservation_suffix = try reservationPath(alloc, "", backup_id, true);
    defer alloc.free(reservation_suffix);
    return switch (location.*) {
        .file => |backup_root| blk: {
            var backup_dir = openBackupRootNoFollow(io, backup_root) catch |err| switch (err) {
                error.FileNotFound => break :blk null,
                else => return err,
            };
            defer backup_dir.close(io);
            const relative_path = trimLeftSlash(reservation_suffix);
            const lock_path = try std.fmt.allocPrint(alloc, "{s}.publish.lock", .{relative_path});
            defer alloc.free(lock_path);
            var lock_file = try openOrCreateBackupLockFile(io, backup_dir, lock_path);
            defer lock_file.close(io);
            try lock_file.lock(io, .exclusive);
            defer lock_file.unlock(io);
            const body = readFileFromBackupRootAlloc(
                alloc,
                io,
                backup_dir,
                relative_path,
                max_backup_attempt_lease_bytes,
            ) catch |err| switch (err) {
                error.FileNotFound => break :blk null,
                else => return err,
            };
            defer alloc.free(body);
            const lease = parseClusterBackupReservationLease(body) catch
                break :blk null;
            if (expected_owner) |owner| {
                if (!std.mem.eql(u8, lease.attempt_id, owner)) break :blk null;
            }
            if (!clusterBackupLeaseReclaimable(
                lease.expires_at_unix_ns,
                now_unix_ns,
            )) {
                break :blk null;
            }
            const owned_attempt_id = try alloc.dupe(u8, lease.attempt_id);
            errdefer alloc.free(owned_attempt_id);
            try deleteFileDurablyFromBackupRoot(io, backup_dir, relative_path);
            break :blk owned_attempt_id;
        },
        .remote => |*store| try store.takeExpiredReservation(
            alloc,
            trimLeftSlash(reservation_suffix),
            now_unix_ns,
            expected_owner,
        ),
    };
}

/// Reclaim the repository-global crashed attempt without a bucket-wide scan.
/// `backup_id` is the requesting backup's validated identity; ownership and
/// cleanup scope always come from the lease's immutable attempt marker.
pub fn reclaimExpiredClusterBackupAttemptAtLocation(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    backup_id: []const u8,
    now_unix_ns: u64,
) !bool {
    try validateBackupId(backup_id);
    const observed_attempt_id =
        (try clusterReservationAttemptIdAtLocation(
            alloc,
            io,
            location,
            backup_id,
        )) orelse return false;
    defer alloc.free(observed_attempt_id);
    var parsed = readClusterBackupAttemptMarker(
        alloc,
        io,
        location,
        observed_attempt_id,
    ) catch |err| switch (err) {
        error.FileNotFound => {
            var requested_commit = readClusterManifestFromLocation(
                alloc,
                location,
                backup_id,
            ) catch |manifest_err| switch (manifest_err) {
                error.FileNotFound => null,
                error.BackupManifestTooLarge, error.StreamTooLong => return false,
                else => return manifest_err,
            };
            if (requested_commit) |*manifest| manifest.deinit(alloc);
            const taken = try takeExpiredClusterReservationAtLocation(
                alloc,
                io,
                location,
                backup_id,
                now_unix_ns,
                observed_attempt_id,
            );
            if (taken) |attempt_id| {
                alloc.free(attempt_id);
                return true;
            }
            return false;
        },
        else => return err,
    };
    defer parsed.deinit();

    var committed = readClusterManifestFromLocation(
        alloc,
        location,
        parsed.value.cluster_backup_id,
    ) catch |err| switch (err) {
        error.FileNotFound => null,
        // A present but unreadable commit record is never proof that an
        // attempt remained unpublished. Preserve the lease, journal, and
        // artifacts for explicit operator repair; consuming the lease here
        // would admit a new repository-wide mutation whose cleanup scope
        // cannot be proven disjoint.
        error.BackupManifestTooLarge, error.StreamTooLong => return false,
        else => return err,
    };
    if (committed) |*manifest| {
        manifest.deinit(alloc);
        _ = try cleanupClusterReservationIfOwnedAtLocation(
            alloc,
            io,
            location,
            backup_id,
            observed_attempt_id,
        );
        var cleanup_owner_buf: [backup_cleanup_lease_owner_prefix.len + 128]u8 = undefined;
        const cleanup_owner = try clusterCleanupLeaseOwner(observed_attempt_id, &cleanup_owner_buf);
        _ = try cleanupClusterReservationIfOwnedAtLocation(
            alloc,
            io,
            location,
            backup_id,
            cleanup_owner,
        );
        var legacy_cleanup_owner_buf: [legacy_backup_cleanup_lease_owner_prefix.len + 128]u8 = undefined;
        const legacy_cleanup_owner = try legacyClusterCleanupLeaseOwner(
            observed_attempt_id,
            &legacy_cleanup_owner_buf,
        );
        _ = try cleanupClusterReservationIfOwnedAtLocation(
            alloc,
            io,
            location,
            backup_id,
            legacy_cleanup_owner,
        );
        if (clusterReservationOwnerMatchesAttemptAtLocation(
            alloc,
            io,
            location,
            backup_id,
            observed_attempt_id,
        ) catch return false) return false;
        const remaining_owner = clusterReservationAttemptIdAtLocation(
            alloc,
            io,
            location,
            backup_id,
        ) catch return false;
        defer if (remaining_owner) |owner| alloc.free(owner);
        // The aggregate manifest is the commit point. Its stale journal and
        // any crash-left reservation no longer protect publication and must
        // not remain discoverable forever.
        try deleteClusterBackupAttemptMarker(alloc, io, location, &parsed.value);
        return true;
    }
    if (!try claimExpiredClusterReservationForCleanup(
        alloc,
        io,
        location,
        &parsed.value,
        now_unix_ns,
    )) return false;
    var writer_lease_budget: usize = backup_attempt_reclaim_scan_budget;
    if (!try claimExpiredClusterTableWriterLeases(
        alloc,
        io,
        location,
        &parsed.value,
        now_unix_ns,
        &writer_lease_budget,
    )) return false;
    // Keep request-path recovery latency bounded. If the final lease probe
    // consumed this invocation's entire quantum, the durable cursor lets the
    // next retry proceed without rescanning those tables.
    if (writer_lease_budget == 0) return false;
    return try cleanupClusterBackupAttemptIncrementally(
        alloc,
        io,
        location,
        &parsed.value,
        &writer_lease_budget,
    );
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
            true,
        ) catch |err| {
            if (cleanup_error == null) cleanup_error = err;
        };
    }
    if (cleanup_error) |err| return err;
    // Once exact artifacts are gone, release the mutation lease and preserve a
    // failed admission tombstone. The conditional head transition cannot erase
    // a retry that has already become authoritative.
    const reservation_released = try cleanupClusterReservationIfOwnedAtLocation(
        alloc,
        io,
        location,
        marker.cluster_backup_id,
        marker.attempt_id,
    );
    if (!reservation_released and
        (try clusterReservationOwnerMatchesAtLocation(
            alloc,
            io,
            location,
            marker.cluster_backup_id,
            marker.attempt_id,
        )) == true)
    {
        // A concurrent lease renewal won the conditional delete. Keep the
        // journal discoverable so a later bounded reclaimer can finish safely.
        return error.BackupAttemptLeaseStillOwned;
    }
    _ = try retireClusterBackupAttemptHeadIfOwned(
        alloc,
        io,
        location,
        marker.attempt_id,
    );
    try deleteClusterBackupAttemptMarker(alloc, io, location, marker);
}

/// Exact maintenance entry point for request-independent cleanup. The durable
/// marker is the source of truth, so background work owns only the attempt ID
/// and never borrows request-lifetime table slices.
pub fn cleanupClusterBackupAttemptByIdAtLocation(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    attempt_id: []const u8,
) !void {
    var marker = try readClusterBackupAttemptMarker(
        alloc,
        io,
        location,
        attempt_id,
    );
    defer marker.deinit();
    return cleanupClusterBackupAttemptAtLocation(
        alloc,
        io,
        location,
        &marker.value,
    );
}

fn cleanupClusterBackupAttemptIncrementally(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    marker: *const ClusterBackupAttemptMarker,
    operation_budget: *usize,
) !bool {
    return cleanupClusterBackupAttemptIncrementallyWithRoot(
        alloc,
        io,
        location,
        marker,
        operation_budget,
        null,
    );
}

fn cleanupClusterBackupAttemptIncrementallyWithRoot(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    marker: *const ClusterBackupAttemptMarker,
    operation_budget: *usize,
    pinned_root: ?std.Io.Dir,
) !bool {
    const remote = switch (location.*) {
        .remote => true,
        .file => false,
    };
    const progress = try readClusterWriterLeaseScanCursorWithRootBudget(
        alloc,
        io,
        location,
        marker,
        null,
        pinned_root,
    );
    if (progress.phase != .artifacts) return false;
    var index = progress.next_table_index;
    while (index < marker.tables.len and operation_budget.* > 0) {
        const table = marker.tables[index];
        cleanupTableBackupAttemptAtLocationWithRootBudget(
            alloc,
            io,
            location,
            table.table_backup_id,
            table.artifact_backup_id,
            marker.format,
            operation_budget,
            true,
            true,
            pinned_root,
            .none,
        ) catch |err| switch (err) {
            error.BackupCleanupBudgetExceeded => {
                try writeClusterWriterLeaseScanCursorMonotonicWithRootBudget(alloc, io, location, marker, .{
                    .phase = .artifacts,
                    .next_table_index = index,
                }, null, pinned_root);
                return false;
            },
            else => return err,
        };
        if (marker.writer_delivery_deadline_fencing) {
            releaseTableBackupCleanupFenceWithRootBudget(
                alloc,
                io,
                location,
                table.artifact_backup_id,
                operation_budget,
                pinned_root,
            ) catch |err| switch (err) {
                error.BackupCleanupBudgetExceeded => {
                    try writeClusterWriterLeaseScanCursorMonotonicWithRootBudget(alloc, io, location, marker, .{
                        .phase = .artifacts,
                        .next_table_index = index,
                    }, null, pinned_root);
                    return false;
                },
                else => return err,
            };
        }
        index += 1;
    }

    try writeClusterWriterLeaseScanCursorMonotonicWithRootBudget(alloc, io, location, marker, .{
        .phase = .artifacts,
        .next_table_index = index,
    }, null, pinned_root);
    if (index != marker.tables.len) return false;

    // Final control-record retirement is a small constant amount of work. Do
    // not begin it at the tail of an exhausted remote cleanup quantum.
    if (remote and operation_budget.* < 10) return false;
    if (remote) operation_budget.* -= 10;
    var cluster_cleanup_owner_buf: [backup_cleanup_lease_owner_prefix.len + 128]u8 = undefined;
    const cluster_cleanup_owner = try clusterCleanupLeaseOwner(marker.attempt_id, &cluster_cleanup_owner_buf);
    var reservation_released = try cleanupClusterReservationIfOwnedAtLocationWithRoot(
        alloc,
        io,
        location,
        marker.cluster_backup_id,
        cluster_cleanup_owner,
        pinned_root,
    );
    var legacy_cluster_cleanup_owner_buf: [legacy_backup_cleanup_lease_owner_prefix.len + 128]u8 = undefined;
    const legacy_cluster_cleanup_owner = try legacyClusterCleanupLeaseOwner(
        marker.attempt_id,
        &legacy_cluster_cleanup_owner_buf,
    );
    if (!reservation_released) {
        reservation_released = try cleanupClusterReservationIfOwnedAtLocationWithRoot(
            alloc,
            io,
            location,
            marker.cluster_backup_id,
            legacy_cluster_cleanup_owner,
            pinned_root,
        );
    }
    if (!reservation_released and
        ((try clusterReservationOwnerMatchesAtLocationWithRoot(
            alloc,
            io,
            location,
            marker.cluster_backup_id,
            cluster_cleanup_owner,
            pinned_root,
        )) == true or
            (try clusterReservationOwnerMatchesAtLocationWithRoot(
                alloc,
                io,
                location,
                marker.cluster_backup_id,
                legacy_cluster_cleanup_owner,
                pinned_root,
            )) == true))
        return error.BackupAttemptLeaseStillOwned;
    _ = try retireClusterBackupAttemptHeadIfOwnedWithRoot(
        alloc,
        io,
        location,
        marker.attempt_id,
        pinned_root,
    );
    try deleteClusterBackupAttemptMarkerWithRoot(
        alloc,
        io,
        location,
        marker,
        pinned_root,
    );
    return true;
}

fn clusterBackupAttemptQuarantineMatches(
    alloc: std.mem.Allocator,
    body: []const u8,
    marker: *const ClusterBackupAttemptMarker,
    expected_marker_sha256: []const u8,
) bool {
    var parsed = std.json.parseFromSlice(
        ClusterBackupAttemptQuarantine,
        alloc,
        body,
        .{ .allocate = .alloc_always },
    ) catch return false;
    defer parsed.deinit();
    if (parsed.value.format_version != 1 or
        parsed.value.reason != .manifest_generation_mismatch or
        parsed.value.quarantined_at_unix_ns == 0 or
        !std.mem.eql(u8, parsed.value.attempt_id, marker.attempt_id) or
        !std.mem.eql(u8, parsed.value.cluster_backup_id, marker.cluster_backup_id) or
        !std.mem.eql(u8, parsed.value.marker_sha256, expected_marker_sha256))
        return false;
    validateClusterBackupAttemptMarker(
        alloc,
        &parsed.value.marker,
        marker.attempt_id,
    ) catch return false;
    const parsed_marker = stringifyJsonAlloc(alloc, parsed.value.marker) catch return false;
    defer alloc.free(parsed_marker);
    var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(parsed_marker, &digest, .{});
    const actual_marker_sha256 = std.fmt.bytesToHex(digest, .lower);
    return std.mem.eql(u8, &actual_marker_sha256, expected_marker_sha256);
}

/// Moves a generation-mismatched marker out of the active `.json` scan
/// namespace only after a durable evidence record exists. Payloads,
/// reservations, and writer leases remain untouched for operator recovery.
fn quarantineClusterBackupAttemptMarker(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    marker: *const ClusterBackupAttemptMarker,
    now_unix_ns: u64,
    object_budget: *usize,
) !void {
    return quarantineClusterBackupAttemptMarkerWithRoot(
        alloc,
        io,
        location,
        marker,
        now_unix_ns,
        object_budget,
        null,
    );
}

fn quarantineClusterBackupAttemptMarkerWithRoot(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    marker: *const ClusterBackupAttemptMarker,
    now_unix_ns: u64,
    object_budget: *usize,
    pinned_root: ?std.Io.Dir,
) !void {
    if (object_budget.* < cluster_backup_attempt_quarantine_operation_cost)
        return error.BackupCleanupBudgetExceeded;
    object_budget.* -= cluster_backup_attempt_quarantine_operation_cost;

    const marker_json = try stringifyJsonAlloc(alloc, marker.*);
    defer alloc.free(marker_json);
    var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(marker_json, &digest, .{});
    const marker_sha256 = std.fmt.bytesToHex(digest, .lower);
    const encoded = try stringifyJsonAlloc(alloc, ClusterBackupAttemptQuarantine{
        .attempt_id = marker.attempt_id,
        .cluster_backup_id = marker.cluster_backup_id,
        .reason = .manifest_generation_mismatch,
        .quarantined_at_unix_ns = now_unix_ns,
        .marker_sha256 = marker_sha256[0..],
        .marker = marker.*,
    });
    defer alloc.free(encoded);
    if (encoded.len > max_backup_attempt_quarantine_bytes)
        return error.BackupManifestTooLarge;
    const suffix = try clusterBackupAttemptQuarantinePath(alloc, "", marker.attempt_id);
    defer alloc.free(suffix);
    switch (location.*) {
        .file => |backup_root| {
            var root = try accessBackupRoot(io, backup_root, pinned_root, false);
            defer root.deinit(io);
            const backup_dir = root.dir;
            try ensureBackupRelativeParentNoFollow(
                io,
                backup_dir,
                trimLeftSlash(suffix),
            );
            if (!try writeFileToBackupRootIfAbsent(
                io,
                backup_dir,
                trimLeftSlash(suffix),
                encoded,
            )) {
                const existing = try readFileFromBackupRootAlloc(
                    alloc,
                    io,
                    backup_dir,
                    trimLeftSlash(suffix),
                    max_backup_attempt_quarantine_bytes,
                );
                defer alloc.free(existing);
                if (!clusterBackupAttemptQuarantineMatches(
                    alloc,
                    existing,
                    marker,
                    marker_sha256[0..],
                )) return error.BackupPublicationConflict;
            }
            try deleteClusterBackupAttemptMarkerFromBackupRoot(
                alloc,
                io,
                backup_dir,
                marker,
            );
        },
        .remote => |*store| {
            const key = try store.keyAlloc(alloc, trimLeftSlash(suffix));
            defer alloc.free(key);
            var quarantine_exists = false;
            if (store.client.putObject(store.bucket, key, encoded, .{
                .content_type = "application/json",
                .if_none_match = true,
            })) |result_value| {
                var result = result_value;
                result.deinit(alloc);
            } else |err| switch (err) {
                error.PreconditionFailed => quarantine_exists = true,
                else => return err,
            }
            if (quarantine_exists) {
                const existing = try store.readBytesAllocLimited(
                    alloc,
                    trimLeftSlash(suffix),
                    max_backup_attempt_quarantine_bytes,
                );
                defer alloc.free(existing);
                if (!clusterBackupAttemptQuarantineMatches(
                    alloc,
                    existing,
                    marker,
                    marker_sha256[0..],
                )) return error.BackupPublicationConflict;
            }
            try deleteClusterBackupAttemptMarkerWithRoot(
                alloc,
                io,
                location,
                marker,
                pinned_root,
            );
        },
    }
    std.log.warn(
        "cluster backup attempt quarantined reason=manifest_generation_mismatch attempt={s} backup={s}",
        .{ marker.attempt_id, marker.cluster_backup_id },
    );
}

fn reclaimClusterBackupAttemptMarker(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    marker: *const ClusterBackupAttemptMarker,
    now_unix_ns: u64,
    authoritative_attempt_id: ?[]const u8,
    object_budget: *usize,
) !bool {
    return reclaimClusterBackupAttemptMarkerWithRoot(
        alloc,
        io,
        location,
        marker,
        now_unix_ns,
        authoritative_attempt_id,
        object_budget,
        null,
    );
}

fn reclaimClusterBackupAttemptMarkerWithRoot(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    marker: *const ClusterBackupAttemptMarker,
    now_unix_ns: u64,
    authoritative_attempt_id: ?[]const u8,
    object_budget: *usize,
    pinned_root: ?std.Io.Dir,
) !bool {
    var committed = readClusterManifestFromLocationWithRoot(
        alloc,
        io,
        location,
        marker.cluster_backup_id,
        pinned_root,
    ) catch |err| switch (err) {
        // Marker age only makes an attempt eligible for examination. The
        // renewable reservation is the authority: atomically take it only
        // after expiry, or retain the marker while this owner is still live.
        error.FileNotFound => {
            if (!try claimExpiredClusterReservationForCleanupWithRoot(
                alloc,
                io,
                location,
                marker,
                now_unix_ns,
                pinned_root,
            )) return false;
            if (!try claimExpiredClusterTableWriterLeasesWithRoot(
                alloc,
                io,
                location,
                marker,
                now_unix_ns,
                object_budget,
                pinned_root,
            )) return false;
            // Finishing the lease scan at the budget boundary is progress, not
            // permission to begin an unbudgeted payload cleanup. The durable
            // cursor makes the next pass start directly at cleanup.
            if (object_budget.* == 0) return false;
            return try cleanupClusterBackupAttemptIncrementallyWithRoot(
                alloc,
                io,
                location,
                marker,
                object_budget,
                pinned_root,
            );
        },
        else => return err,
    };
    defer committed.deinit(alloc);

    if (!clusterManifestMatchesAttemptMarker(&committed, marker)) {
        // Any aggregate is a fail-closed payload retention boundary, but only
        // an exact aggregate proves this marker's writers finished. First
        // fence an authoritative head, then move the marker out of the active
        // scan namespace behind a durable evidence record. Writer leases and
        // payloads remain untouched.
        const quarantine_sequence_cost =
            cluster_backup_attempt_head_transition_operation_cost +
            cluster_backup_attempt_quarantine_operation_cost;
        if (object_budget.* < quarantine_sequence_cost) return false;
        object_budget.* -= cluster_backup_attempt_head_transition_operation_cost;
        _ = try quarantineClusterBackupAttemptHeadIfOwnedWithRoot(
            alloc,
            io,
            location,
            marker.attempt_id,
            pinned_root,
        );
        quarantineClusterBackupAttemptMarkerWithRoot(
            alloc,
            io,
            location,
            marker,
            now_unix_ns,
            object_budget,
            pinned_root,
        ) catch |err| switch (err) {
            error.BackupCleanupBudgetExceeded => return false,
            else => return err,
        };
        return true;
    }
    _ = authoritative_attempt_id;
    if (!try releaseCommittedClusterTableWriterStatesWithRoot(
        alloc,
        io,
        location,
        marker,
        object_budget,
        pinned_root,
    )) return false;
    const remote = switch (location.*) {
        .remote => true,
        .file => false,
    };
    // Reservation-owner variants plus cursor/marker retirement are
    // a fixed amount of work, but still participate in the remote I/O budget.
    if (remote and object_budget.* < 10) return false;
    if (remote) object_budget.* -= 10;
    _ = try cleanupClusterReservationIfOwnedAtLocationWithRoot(
        alloc,
        io,
        location,
        marker.cluster_backup_id,
        marker.attempt_id,
        pinned_root,
    );
    var cleanup_owner_buf: [backup_cleanup_lease_owner_prefix.len + 128]u8 = undefined;
    const cleanup_owner = try clusterCleanupLeaseOwner(marker.attempt_id, &cleanup_owner_buf);
    _ = try cleanupClusterReservationIfOwnedAtLocationWithRoot(
        alloc,
        io,
        location,
        marker.cluster_backup_id,
        cleanup_owner,
        pinned_root,
    );
    var legacy_cleanup_owner_buf: [legacy_backup_cleanup_lease_owner_prefix.len + 128]u8 = undefined;
    const legacy_cleanup_owner = try legacyClusterCleanupLeaseOwner(
        marker.attempt_id,
        &legacy_cleanup_owner_buf,
    );
    _ = try cleanupClusterReservationIfOwnedAtLocationWithRoot(
        alloc,
        io,
        location,
        marker.cluster_backup_id,
        legacy_cleanup_owner,
        pinned_root,
    );
    if (clusterReservationOwnerMatchesAttemptAtLocationWithRoot(
        alloc,
        io,
        location,
        marker.cluster_backup_id,
        marker.attempt_id,
        pinned_root,
    ) catch return false) return false;
    const remaining_owner = clusterReservationAttemptIdAtLocationWithRoot(
        alloc,
        io,
        location,
        marker.cluster_backup_id,
        pinned_root,
    ) catch return false;
    defer if (remaining_owner) |owner| alloc.free(owner);
    // A durable aggregate makes this journal non-authoritative even when the
    // head still names the same attempt (for example, after a crash between
    // commit and marker retirement).
    try deleteClusterBackupAttemptMarkerWithRoot(
        alloc,
        io,
        location,
        marker,
        pinned_root,
    );
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
    return reclaimClusterBackupAttemptByIdWithRoot(
        alloc,
        io,
        location,
        attempt_id,
        now_unix_ns,
        authoritative_attempt_id,
        object_budget,
        null,
    );
}

fn reclaimClusterBackupAttemptByIdWithRoot(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    attempt_id: []const u8,
    now_unix_ns: u64,
    authoritative_attempt_id: ?[]const u8,
    object_budget: *usize,
    pinned_root: ?std.Io.Dir,
) !bool {
    var parsed = try readClusterBackupAttemptMarkerWithRoot(
        alloc,
        io,
        location,
        attempt_id,
        pinned_root,
    );
    defer parsed.deinit();
    if (!markerIsStale(&parsed.value, now_unix_ns)) return false;
    return try reclaimClusterBackupAttemptMarkerWithRoot(
        alloc,
        io,
        location,
        &parsed.value,
        now_unix_ns,
        authoritative_attempt_id,
        object_budget,
        pinned_root,
    );
}

const LocalBackupAttemptReclaimDisposition = enum {
    requeue,
    drop,
};

const LocalBackupAttemptReclaimClaim = struct {
    attempt_id: []u8,
    shard: u8,
    disposition: LocalBackupAttemptReclaimDisposition = .requeue,
};

fn openLocalBackupAttemptReclaimDir(
    io: std.Io,
    backup_root: std.Io.Dir,
    relative_path: []const u8,
) !std.Io.Dir {
    return openBackupRelativeDirNoFollow(io, backup_root, relative_path, true);
}

fn localBackupAttemptStagingFileStale(
    mtime: std.Io.Timestamp,
    now_unix_ns: u64,
) bool {
    const mtime_ns = mtime.toNanoseconds();
    if (mtime_ns < 0) return true;
    const modified_at = std.math.cast(u64, mtime_ns) orelse return false;
    if (modified_at > now_unix_ns) return false;
    return now_unix_ns - modified_at >= backup_attempt_staging_orphan_age_ns;
}

fn scavengeLocalBackupAttemptStagingShard(
    alloc: std.mem.Allocator,
    io: std.Io,
    backup_root: std.Io.Dir,
    kind: LocalBackupAttemptReclaimTicketKind,
    shard: u8,
    now_unix_ns: u64,
    remaining_budget: *usize,
    test_hook: ?*BackupMaintenanceTestHook,
) !usize {
    if (remaining_budget.* == 0) return 0;
    const staging_root = try localBackupAttemptReclaimStagingShardPath(
        alloc,
        "",
        kind,
        shard,
    );
    defer alloc.free(staging_root);
    const relative_staging_root = trimLeftSlash(staging_root);
    var dir = openLocalBackupAttemptReclaimDir(
        io,
        backup_root,
        relative_staging_root,
    ) catch |err| switch (err) {
        error.FileNotFound => return 0,
        else => return err,
    };
    defer dir.close(io);

    const suffix = ".replace.tmp";
    var iterator = dir.iterate();
    var examined: usize = 0;
    while (remaining_budget.* > 0) {
        const entry = try iterator.next(io) orelse break;
        remaining_budget.* -= 1;
        examined += 1;
        if (entry.kind != .file or !std.mem.endsWith(u8, entry.name, suffix))
            continue;
        const attempt_id = entry.name[0 .. entry.name.len - suffix.len];
        validateBackupId(attempt_id) catch continue;
        const stat = dir.statFile(io, entry.name, .{ .follow_symlinks = false }) catch |err| switch (err) {
            error.FileNotFound => continue,
            else => return err,
        };
        if (stat.kind != .file) continue;
        if (!localBackupAttemptStagingFileStale(stat.mtime, now_unix_ns))
            continue;

        const publication_lock_path = try localBackupAttemptPublicationLockPath(
            alloc,
            "",
            attempt_id,
        );
        defer alloc.free(publication_lock_path);
        const relative_publication_lock_path = trimLeftSlash(publication_lock_path);
        try ensureBackupRelativeParentNoFollow(
            io,
            backup_root,
            relative_publication_lock_path,
        );
        var publication_lock = try openOrCreateBackupLockFile(
            io,
            backup_root,
            relative_publication_lock_path,
        );
        defer publication_lock.close(io);
        if (test_hook) |hook| {
            hook.stale_staging_candidate.store(true, .release);
            hook.progress.set(io);
        }
        try publication_lock.lock(io, .exclusive);
        defer publication_lock.unlock(io);

        const current_stat = dir.statFile(
            io,
            entry.name,
            .{ .follow_symlinks = false },
        ) catch |err| switch (err) {
            error.FileNotFound => continue,
            else => return err,
        };
        if (current_stat.kind != .file) continue;
        if (!localBackupAttemptStagingFileStale(
            current_stat.mtime,
            now_unix_ns,
        )) continue;
        try deleteFileDurablyFromDir(io, dir, entry.name);
    }
    return examined;
}

fn recoverExpiredLocalBackupAttemptReclaimClaims(
    alloc: std.mem.Allocator,
    io: std.Io,
    backup_root: std.Io.Dir,
    shard: u8,
    now_unix_ns: u64,
) !void {
    const claim_root = try localBackupAttemptReclaimShardPath(
        alloc,
        "",
        .claimed,
        shard,
    );
    defer alloc.free(claim_root);
    var dir = openLocalBackupAttemptReclaimDir(
        io,
        backup_root,
        trimLeftSlash(claim_root),
    ) catch |err| switch (err) {
        error.FileNotFound => return,
        else => return err,
    };
    defer dir.close(io);
    var iterator = dir.iterate();
    var examined: usize = 0;
    while (examined < backup_attempt_reclaim_scan_budget) {
        const entry = try iterator.next(io) orelse break;
        examined += 1;
        if (entry.kind != .file) continue;
        validateBackupId(entry.name) catch {
            try deleteFileDurablyFromDir(io, dir, entry.name);
            continue;
        };
        const claim_path = try localBackupAttemptReclaimTicketPath(
            alloc,
            "",
            .claimed,
            shard,
            entry.name,
        );
        defer alloc.free(claim_path);
        const claimed_at = try localBackupAttemptReclaimTicketTimestamp(
            alloc,
            io,
            backup_root,
            trimLeftSlash(claim_path),
        );
        if (!localBackupAttemptReclaimClaimExpired(claimed_at, now_unix_ns))
            continue;

        // The claim timeout already fenced the abandoned worker. Requeue in
        // the same bucket as immediately eligible so this pass can process it
        // without another full cursor rotation.
        const retry_at_unix_ns = now_unix_ns;
        const destination_shard = shard;
        const queue_path = try localBackupAttemptReclaimTicketPath(
            alloc,
            "",
            .queued,
            destination_shard,
            entry.name,
        );
        defer alloc.free(queue_path);
        try replaceLocalBackupAttemptReclaimTicketTimestamp(
            alloc,
            io,
            backup_root,
            trimLeftSlash(claim_path),
            retry_at_unix_ns,
        );
        renameLocalBackupAttemptReclaimTicket(
            io,
            backup_root,
            trimLeftSlash(claim_path),
            trimLeftSlash(queue_path),
        ) catch |err| switch (err) {
            error.FileNotFound => {},
            else => return err,
        };
    }
}

fn claimLocalBackupAttemptsForReclamation(
    alloc: std.mem.Allocator,
    io: std.Io,
    backup_root: std.Io.Dir,
    now_unix_ns: u64,
    test_hook: ?*BackupMaintenanceTestHook,
) !std.ArrayListUnmanaged(LocalBackupAttemptReclaimClaim) {
    var claims = std.ArrayListUnmanaged(LocalBackupAttemptReclaimClaim).empty;
    errdefer {
        for (claims.items) |claim| alloc.free(claim.attempt_id);
        claims.deinit(alloc);
    }

    const cursor_path = try backupAttemptReclaimCursorPath(alloc, "");
    defer alloc.free(cursor_path);
    const relative_cursor_path = trimLeftSlash(cursor_path);
    const cursor_body = readFileFromBackupRootAlloc(
        alloc,
        io,
        backup_root,
        relative_cursor_path,
        max_backup_attempt_cursor_bytes,
    ) catch |err| switch (err) {
        error.FileNotFound => null,
        // A corrupt legacy cursor is advisory state. Resetting to shard zero
        // preserves bounded progress without allowing an oversized control
        // file to disable maintenance permanently.
        error.StreamTooLong => null,
        else => return err,
    };
    defer if (cursor_body) |value| alloc.free(value);
    const start_shard = localBackupAttemptReclaimCursor(cursor_body);
    var staging_budget = backup_attempt_staging_scan_budget;

    var offset: u8 = 0;
    while (offset < backup_attempt_reclaim_shard_count) : (offset += 1) {
        const shard = (start_shard +% offset) % backup_attempt_reclaim_shard_count;
        var staging_examined = try scavengeLocalBackupAttemptStagingShard(
            alloc,
            io,
            backup_root,
            .queued,
            shard,
            now_unix_ns,
            &staging_budget,
            test_hook,
        );
        staging_examined += try scavengeLocalBackupAttemptStagingShard(
            alloc,
            io,
            backup_root,
            .claimed,
            shard,
            now_unix_ns,
            &staging_budget,
            test_hook,
        );
        try recoverExpiredLocalBackupAttemptReclaimClaims(
            alloc,
            io,
            backup_root,
            shard,
            now_unix_ns,
        );

        const queue_root = try localBackupAttemptReclaimShardPath(
            alloc,
            "",
            .queued,
            shard,
        );
        defer alloc.free(queue_root);
        var dir = openLocalBackupAttemptReclaimDir(
            io,
            backup_root,
            trimLeftSlash(queue_root),
        ) catch |err| switch (err) {
            error.FileNotFound => {
                if (staging_examined > 0) {
                    const next_shard =
                        (shard + 1) % backup_attempt_reclaim_shard_count;
                    var cursor_buf: [8]u8 = undefined;
                    const cursor = try std.fmt.bufPrint(
                        &cursor_buf,
                        "{d}\n",
                        .{next_shard},
                    );
                    try replaceFileInBackupRootUnderHeldLock(
                        alloc,
                        io,
                        backup_root,
                        relative_cursor_path,
                        cursor,
                    );
                    return claims;
                }
                continue;
            },
            else => return err,
        };
        defer dir.close(io);

        var iterator = dir.iterate();
        var examined: usize = 0;
        while (examined < backup_attempt_reclaim_scan_budget) {
            const entry = try iterator.next(io) orelse break;
            examined += 1;
            if (entry.kind != .file) continue;
            validateBackupId(entry.name) catch {
                try deleteFileDurablyFromDir(io, dir, entry.name);
                continue;
            };
            const queue_path = try localBackupAttemptReclaimTicketPath(
                alloc,
                "",
                .queued,
                shard,
                entry.name,
            );
            defer alloc.free(queue_path);
            const publication_lock_path = try localBackupAttemptPublicationLockPath(
                alloc,
                "",
                entry.name,
            );
            defer alloc.free(publication_lock_path);
            const relative_publication_lock_path = trimLeftSlash(publication_lock_path);
            try ensureBackupRelativeParentNoFollow(
                io,
                backup_root,
                relative_publication_lock_path,
            );
            var publication_lock = try openOrCreateBackupLockFile(
                io,
                backup_root,
                relative_publication_lock_path,
            );
            defer publication_lock.close(io);
            try publication_lock.lock(io, .exclusive);
            defer publication_lock.unlock(io);
            const not_before = try localBackupAttemptReclaimTicketTimestamp(
                alloc,
                io,
                backup_root,
                trimLeftSlash(queue_path),
            );
            if (not_before) |eligible_at| {
                if (eligible_at > now_unix_ns) {
                    // Marker deletion is the authoritative completion signal.
                    // Drop orphan tickets before considering fairness rotation;
                    // otherwise every successful backup would generate durable
                    // cross-directory renames until its original 24-hour
                    // eligibility timestamp.
                    const marker_path = try incompleteBackupMarkerPath(
                        alloc,
                        "",
                        entry.name,
                    );
                    defer alloc.free(marker_path);
                    if (!(try fileExistsFromBackupRoot(
                        io,
                        backup_root,
                        trimLeftSlash(marker_path),
                    ))) {
                        try deleteFileDurablyFromDir(io, dir, entry.name);
                        continue;
                    }
                    continue;
                }
            }
            if (claims.items.len == backup_attempt_reclaim_batch_size)
                break;

            try replaceLocalBackupAttemptReclaimTicketTimestamp(
                alloc,
                io,
                backup_root,
                trimLeftSlash(queue_path),
                now_unix_ns,
            );
            const claim_path = try localBackupAttemptReclaimTicketPath(
                alloc,
                "",
                .claimed,
                shard,
                entry.name,
            );
            defer alloc.free(claim_path);
            renameLocalBackupAttemptReclaimTicket(
                io,
                backup_root,
                trimLeftSlash(queue_path),
                trimLeftSlash(claim_path),
            ) catch |err| switch (err) {
                error.FileNotFound => continue,
                else => return err,
            };
            try claims.append(alloc, .{
                .attempt_id = try alloc.dupe(u8, entry.name),
                .shard = shard,
            });
        }

        if (examined > 0 or staging_examined > 0) {
            const next_shard = (shard + 1) % backup_attempt_reclaim_shard_count;
            var cursor_buf: [8]u8 = undefined;
            const cursor = try std.fmt.bufPrint(&cursor_buf, "{d}\n", .{next_shard});
            try replaceFileInBackupRootUnderHeldLock(
                alloc,
                io,
                backup_root,
                relative_cursor_path,
                cursor,
            );
            return claims;
        }
    }

    try deleteFileDurablyFromBackupRoot(io, backup_root, relative_cursor_path);
    return claims;
}

fn finalizeLocalBackupAttemptReclaimClaims(
    alloc: std.mem.Allocator,
    io: std.Io,
    backup_root: std.Io.Dir,
    now_unix_ns: u64,
    claims: []const LocalBackupAttemptReclaimClaim,
) !void {
    for (claims) |claim| {
        const claim_path = try localBackupAttemptReclaimTicketPath(
            alloc,
            "",
            .claimed,
            claim.shard,
            claim.attempt_id,
        );
        defer alloc.free(claim_path);
        switch (claim.disposition) {
            .drop => deleteFileDurablyFromBackupRoot(
                io,
                backup_root,
                trimLeftSlash(claim_path),
            ) catch |err| switch (err) {
                error.FileNotFound => {},
                else => return err,
            },
            .requeue => {
                const retry_at_unix_ns =
                    now_unix_ns +| backup_attempt_lease_renew_interval_ns;
                const destination_shard =
                    localBackupAttemptReclaimShard(retry_at_unix_ns);
                const queue_path = try localBackupAttemptReclaimTicketPath(
                    alloc,
                    "",
                    .queued,
                    destination_shard,
                    claim.attempt_id,
                );
                defer alloc.free(queue_path);
                try replaceLocalBackupAttemptReclaimTicketTimestamp(
                    alloc,
                    io,
                    backup_root,
                    trimLeftSlash(claim_path),
                    retry_at_unix_ns,
                );
                renameLocalBackupAttemptReclaimTicket(
                    io,
                    backup_root,
                    trimLeftSlash(claim_path),
                    trimLeftSlash(queue_path),
                ) catch |err| switch (err) {
                    error.FileNotFound => {},
                    else => return err,
                };
            },
        }
    }
}

/// Preserve quarantine evidence written by older nodes, then retire its old
/// active-scan key with an identity fence. The copy is byte-for-byte so future
/// schema fields remain available to operators during rolling upgrades.
fn migrateLegacyRemoteBackupAttemptQuarantine(
    alloc: std.mem.Allocator,
    store: *RemoteBackupStore,
    attempt_id: []const u8,
    operation_budget: *usize,
) !bool {
    const legacy_suffix = try legacyClusterBackupAttemptQuarantinePath(
        alloc,
        "",
        attempt_id,
    );
    defer alloc.free(legacy_suffix);
    const legacy_key = try store.keyAlloc(alloc, trimLeftSlash(legacy_suffix));
    defer alloc.free(legacy_key);
    try consumeBackupCleanupOperation(operation_budget);
    var legacy = store.client.getObject(store.bucket, legacy_key, .{
        .range = .{
            .offset = 0,
            .length = max_backup_attempt_quarantine_bytes + 1,
        },
        .skip_metadata_probe = true,
        .max_response_bytes = max_backup_attempt_quarantine_bytes + 1,
    }) catch |err| switch (err) {
        error.FileNotFound => return false,
        else => return err,
    };
    defer legacy.deinit(alloc);
    if (legacy.body.len > max_backup_attempt_quarantine_bytes)
        return error.BackupManifestTooLarge;
    const legacy_etag = legacy.metadata.etag orelse
        return error.BackupReservationIdentityUnavailable;

    const destination_suffix = try clusterBackupAttemptQuarantinePath(
        alloc,
        "",
        attempt_id,
    );
    defer alloc.free(destination_suffix);
    const destination_key = try store.keyAlloc(
        alloc,
        trimLeftSlash(destination_suffix),
    );
    defer alloc.free(destination_key);
    try consumeBackupCleanupOperation(operation_budget);
    if (store.client.putObject(store.bucket, destination_key, legacy.body, .{
        .content_type = "application/json",
        .if_none_match = true,
    })) |result_value| {
        var result = result_value;
        result.deinit(alloc);
    } else |err| switch (err) {
        error.PreconditionFailed => {
            try consumeBackupCleanupOperation(operation_budget);
            var existing = try store.client.getObject(store.bucket, destination_key, .{
                .range = .{
                    .offset = 0,
                    .length = max_backup_attempt_quarantine_bytes + 1,
                },
                .skip_metadata_probe = true,
                .max_response_bytes = max_backup_attempt_quarantine_bytes + 1,
            });
            defer existing.deinit(alloc);
            if (existing.body.len > max_backup_attempt_quarantine_bytes)
                return error.BackupManifestTooLarge;
            if (!std.mem.eql(u8, existing.body, legacy.body) and
                !backupAttemptQuarantineBodiesEquivalent(
                    alloc,
                    legacy.body,
                    existing.body,
                    attempt_id,
                ))
                return error.BackupPublicationConflict;
        },
        else => return err,
    }

    try consumeBackupCleanupOperation(operation_budget);
    store.client.deleteObject(store.bucket, legacy_key, .{
        .if_match_etag = legacy_etag,
    }) catch |err| switch (err) {
        error.FileNotFound => return true,
        error.PreconditionFailed => return false,
        else => return err,
    };
    return true;
}

fn backupAttemptQuarantineBodiesEquivalent(
    alloc: std.mem.Allocator,
    legacy_body: []const u8,
    destination_body: []const u8,
    expected_attempt_id: []const u8,
) bool {
    var parsed = std.json.parseFromSlice(
        ClusterBackupAttemptQuarantine,
        alloc,
        legacy_body,
        .{ .allocate = .alloc_always },
    ) catch return false;
    defer parsed.deinit();
    if (!std.mem.eql(u8, parsed.value.attempt_id, expected_attempt_id))
        return false;
    // The legacy record is deletion authority. Validate its self-contained
    // evidence before accepting semantic equivalence with the destination;
    // otherwise a corrupt body can borrow a valid destination digest and be
    // conditionally deleted even though its embedded marker was altered.
    if (!clusterBackupAttemptQuarantineMatches(
        alloc,
        legacy_body,
        &parsed.value.marker,
        parsed.value.marker_sha256,
    )) return false;
    return clusterBackupAttemptQuarantineMatches(
        alloc,
        destination_body,
        &parsed.value.marker,
        parsed.value.marker_sha256,
    );
}

pub fn reclaimStaleClusterBackupAttempts(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    now_unix_ns: u64,
) !usize {
    return reclaimStaleClusterBackupAttemptsWithHook(
        alloc,
        io,
        location,
        now_unix_ns,
        null,
    );
}

fn reclaimStaleClusterBackupAttemptsWithHook(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    now_unix_ns: u64,
    test_hook: ?*BackupMaintenanceTestHook,
) !usize {
    var reclaimed: usize = 0;
    var object_budget: usize = backup_attempt_reclaim_object_budget;
    switch (location.*) {
        .file => |backup_root| {
            var backup_dir = openBackupRootNoFollow(io, backup_root) catch |err| switch (err) {
                error.FileNotFound => return 0,
                else => return err,
            };
            defer backup_dir.close(io);
            var parsed_head = try readClusterBackupAttemptHeadWithRoot(
                alloc,
                io,
                location,
                backup_dir,
            );
            defer if (parsed_head) |*head| head.deinit();
            const authoritative_attempt_id: ?[]const u8 = if (parsed_head) |*head|
                head.value.attempt_id
            else
                null;
            const cursor_path = try backupAttemptReclaimCursorPath(alloc, "");
            defer alloc.free(cursor_path);
            const relative_cursor_path = trimLeftSlash(cursor_path);
            const cursor_lock_path = try std.fmt.allocPrint(
                alloc,
                "{s}.publish.lock",
                .{relative_cursor_path},
            );
            defer alloc.free(cursor_lock_path);
            var cursor_lock = try openOrCreateBackupLockFile(
                io,
                backup_dir,
                cursor_lock_path,
            );
            defer cursor_lock.close(io);
            try cursor_lock.lock(io, .exclusive);
            var claims = claimLocalBackupAttemptsForReclamation(
                alloc,
                io,
                backup_dir,
                now_unix_ns,
                test_hook,
            ) catch |err| {
                cursor_lock.unlock(io);
                return err;
            };
            cursor_lock.unlock(io);
            if (test_hook) |hook| {
                if (hook.pause_after_claims) {
                    hook.claims_ready.set(io);
                    hook.claims_release.waitUncancelable(io);
                }
            }
            defer {
                for (claims.items) |claim| alloc.free(claim.attempt_id);
                claims.deinit(alloc);
            }

            for (claims.items) |*claim| {
                if (reclaimed >= backup_attempt_reclaim_batch_size) continue;
                if (reclaimClusterBackupAttemptByIdWithRoot(
                    alloc,
                    io,
                    location,
                    claim.attempt_id,
                    now_unix_ns,
                    authoritative_attempt_id,
                    &object_budget,
                    backup_dir,
                ) catch |err| {
                    if (err == error.FileNotFound) {
                        claim.disposition = .drop;
                        continue;
                    }
                    std.log.warn("stale backup attempt reclamation deferred phase=local class={s}", .{@errorName(err)});
                    continue;
                }) {
                    claim.disposition = .drop;
                    reclaimed += 1;
                }
            }

            try cursor_lock.lock(io, .exclusive);
            finalizeLocalBackupAttemptReclaimClaims(
                alloc,
                io,
                backup_dir,
                now_unix_ns,
                claims.items,
            ) catch |err| {
                cursor_lock.unlock(io);
                return err;
            };
            cursor_lock.unlock(io);
        },
        .remote => |*store| {
            var parsed_head = try readClusterBackupAttemptHead(alloc, io, location);
            defer if (parsed_head) |*head| head.deinit();
            const authoritative_attempt_id: ?[]const u8 = if (parsed_head) |*head|
                head.value.attempt_id
            else
                null;
            const key_prefix = try store.keyPrefixAlloc(alloc, incomplete_backup_prefix);
            defer alloc.free(key_prefix);
            const cursor_body = store.readBytesAllocLimited(
                alloc,
                backup_attempt_reclaim_cursor_name,
                max_backup_attempt_cursor_bytes,
            ) catch |err| switch (err) {
                error.FileNotFound => null,
                else => return err,
            };
            defer if (cursor_body) |value| alloc.free(value);
            const start_after = if (cursor_body) |value| blk: {
                const trimmed = std.mem.trim(u8, value, "\r\n");
                if (!std.mem.startsWith(u8, trimmed, key_prefix) or
                    trimmed.len <= key_prefix.len)
                {
                    break :blk null;
                }
                break :blk trimmed;
            } else null;
            var listed = try store.client.listObjects(store.bucket, .{
                .prefix = key_prefix,
                .recursive = true,
                .start_after = start_after,
                .max_keys = @intCast(backup_attempt_reclaim_scan_budget),
            });
            defer listed.deinit(alloc);
            var legacy_quarantines_examined: usize = 0;
            for (listed.entries) |entry| {
                if (reclaimed >= backup_attempt_reclaim_batch_size) break;
                if (!std.mem.startsWith(u8, entry.key, key_prefix)) continue;
                const name = entry.key[key_prefix.len..];
                if (std.mem.indexOfScalar(u8, name, '/') != null) continue;
                if (std.mem.endsWith(u8, name, cluster_backup_attempt_quarantine_suffix)) {
                    if (legacy_quarantines_examined <
                        backup_attempt_legacy_quarantine_migration_batch_size)
                    {
                        legacy_quarantines_examined += 1;
                        const attempt_id = name[0 .. name.len - cluster_backup_attempt_quarantine_suffix.len];
                        validateBackupId(attempt_id) catch continue;
                        _ = migrateLegacyRemoteBackupAttemptQuarantine(
                            alloc,
                            store,
                            attempt_id,
                            &object_budget,
                        ) catch |err| {
                            std.log.warn(
                                "legacy backup quarantine migration deferred class={s}",
                                .{@errorName(err)},
                            );
                        };
                    }
                    continue;
                }
                if (!std.mem.endsWith(u8, name, ".json")) {
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

            // Persist an opaque lexicographic scan position so live, corrupt,
            // or legacy markers at the front of the prefix cannot starve
            // later abandoned attempts. At end-of-prefix, remove the cursor;
            // the next bounded invocation wraps to the beginning.
            if (listed.next_continuation_token != null and listed.entries.len > 0) {
                const last_key = listed.entries[listed.entries.len - 1].key;
                try store.writeBytes(
                    alloc,
                    backup_attempt_reclaim_cursor_name,
                    last_key,
                    "text/plain",
                );
            } else {
                try store.deleteSuffix(alloc, backup_attempt_reclaim_cursor_name);
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

pub fn encodeErrorBody(alloc: std.mem.Allocator, message: []const u8) ![]u8 {
    return stringifyJsonAlloc(alloc, .{ .@"error" = message });
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
    return writeClusterManifestToLocationWithIoAndCancellation(
        alloc,
        io,
        location,
        manifest,
        .none,
    );
}

pub fn writeClusterManifestToLocationWithIoAndCancellation(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    manifest: *const ClusterBackupManifest,
    cancellation: CancellationToken,
) !void {
    try cancellation.check();
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
            try writeFileAbsoluteIfAbsentWithIoAndCancellation(
                alloc,
                io,
                path,
                encoded,
                cancellation,
            );
        },
        .remote => |*store| {
            const encoded = try stringifyJsonAlloc(alloc, manifest.*);
            defer alloc.free(encoded);
            try ensureManifestSize(encoded, max_backup_manifest_bytes);
            const suffix = try clusterMetadataPath(alloc, "", manifest.backup_id);
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

fn readClusterManifestFromLocationWithRoot(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    backup_id: []const u8,
    pinned_root: ?std.Io.Dir,
) !ClusterBackupManifest {
    return switch (location.*) {
        .file => |backup_root| blk: {
            var root = try accessBackupRoot(io, backup_root, pinned_root, false);
            defer root.deinit(io);
            const suffix = try clusterMetadataPath(alloc, "", backup_id);
            defer alloc.free(suffix);
            const body = try readFileFromBackupRootAlloc(
                alloc,
                io,
                root.dir,
                trimLeftSlash(suffix),
                max_backup_manifest_bytes,
            );
            defer alloc.free(body);
            break :blk try parseClusterManifestBytes(alloc, body, backup_id);
        },
        .remote => try readClusterManifestFromLocation(alloc, location, backup_id),
    };
}

const ClusterManifestEncoding = enum {
    zig_v2,
    go_portable_v2,
};

const DecodedClusterManifest = struct {
    manifest: ClusterBackupManifest,
    encoding: ClusterManifestEncoding,
};

fn readClusterManifestFromLocationDecoded(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    backup_id: []const u8,
) !DecodedClusterManifest {
    const body = switch (location.*) {
        .file => |backup_root| blk: {
            const path = try clusterMetadataPath(alloc, backup_root, backup_id);
            defer alloc.free(path);
            break :blk try readFileAbsoluteAllocWithIo(
                alloc,
                io,
                path,
                max_backup_manifest_bytes,
            );
        },
        .remote => |*store| blk: {
            const suffix = try clusterMetadataPath(alloc, "", backup_id);
            defer alloc.free(suffix);
            break :blk try store.readBytesAllocLimited(
                alloc,
                trimLeftSlash(suffix),
                max_backup_manifest_bytes,
            );
        },
    };
    defer alloc.free(body);
    return try parseClusterManifestBytesDecoded(alloc, body, backup_id);
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
    const decoded = try parseClusterManifestBytesDecoded(alloc, body, backup_id);
    return decoded.manifest;
}

fn parseClusterManifestBytesDecoded(
    alloc: std.mem.Allocator,
    body: []const u8,
    backup_id: []const u8,
) !DecodedClusterManifest {
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
        return .{
            .manifest = try cloneClusterBackupManifest(alloc, parsed.value),
            .encoding = .zig_v2,
        };
    }
    return .{
        .manifest = try parseGoPortableClusterManifest(alloc, root, backup_id),
        .encoding = .go_portable_v2,
    };
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
            if (stat.kind != .file or stat.size == 0)
                return error.BackupArtifactMissing;
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
/// for a non-empty regular payload; portable artifacts additionally bind the
/// advertised byte length. Full SHA-256 verification remains part of
/// restore/materialization so listing never downloads multi-gigabyte payloads.
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

fn hashArtifactI128(hasher: *std.crypto.hash.sha2.Sha256, value: i128) void {
    var encoded: [@sizeOf(i128)]u8 = undefined;
    std.mem.writeInt(i128, &encoded, value, .little);
    hasher.update(&encoded);
}

fn hashLocalArtifactStat(
    hasher: *std.crypto.hash.sha2.Sha256,
    stat: std.Io.File.Stat,
) void {
    hashArtifactU64(hasher, @intCast(stat.inode));
    hashArtifactU64(hasher, stat.size);
    hashArtifactI128(hasher, stat.mtime.toNanoseconds());
    hashArtifactI128(hasher, stat.ctime.toNanoseconds());
}

fn localArtifactStatsEqual(
    lhs: std.Io.File.Stat,
    rhs: std.Io.File.Stat,
) bool {
    return lhs.inode == rhs.inode and
        lhs.size == rhs.size and
        std.meta.eql(lhs.mtime, rhs.mtime) and
        std.meta.eql(lhs.ctime, rhs.ctime);
}

fn localArtifactStatIdentity(stat: std.Io.File.Stat) u64 {
    var hasher = std.hash.Wyhash.init(@intCast(stat.inode));
    hasher.update(std.mem.asBytes(&stat.size));
    const mtime = stat.mtime.toNanoseconds();
    const ctime = stat.ctime.toNanoseconds();
    hasher.update(std.mem.asBytes(&mtime));
    hasher.update(std.mem.asBytes(&ctime));
    const identity = hasher.final();
    return if (identity == 0) 1 else identity;
}

const LocalNativeIdentityFile = struct {
    path: []u8,
    stat: std.Io.File.Stat,

    fn deinit(self: LocalNativeIdentityFile, alloc: std.mem.Allocator) void {
        alloc.free(self.path);
    }
};

fn localNativeIdentityFileLessThan(
    _: void,
    lhs: LocalNativeIdentityFile,
    rhs: LocalNativeIdentityFile,
) bool {
    return std.mem.order(u8, lhs.path, rhs.path) == .lt;
}

fn hashLocalNativeArtifactIdentity(
    alloc: std.mem.Allocator,
    io: std.Io,
    path: []const u8,
    hasher: *std.crypto.hash.sha2.Sha256,
) !void {
    var dir = if (std.fs.path.isAbsolute(path))
        try std.Io.Dir.openDirAbsolute(io, path, .{ .iterate = true })
    else
        try std.Io.Dir.cwd().openDir(io, path, .{ .iterate = true });
    defer dir.close(io);

    var files = std.ArrayListUnmanaged(LocalNativeIdentityFile).empty;
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
                const normalized = try alloc.dupe(u8, entry.path);
                errdefer alloc.free(normalized);
                if (std.fs.path.sep != '/') {
                    for (normalized) |*c| if (c.* == std.fs.path.sep) {
                        c.* = '/';
                    };
                }
                try files.append(alloc, .{
                    .path = normalized,
                    .stat = try dir.statFile(io, entry.path, .{}),
                });
            },
            else => return error.UnsupportedBackupArtifact,
        }
    }
    if (files.items.len == 0) return error.BackupArtifactMissing;
    std.mem.sort(
        LocalNativeIdentityFile,
        files.items,
        {},
        localNativeIdentityFileLessThan,
    );
    hashArtifactU64(hasher, @intCast(files.items.len));
    for (files.items) |entry| {
        hashArtifactBytes(hasher, entry.path);
        hashLocalArtifactStat(hasher, entry.stat);
    }
}

fn hashRemoteNativeArtifactIdentity(
    alloc: std.mem.Allocator,
    store: *RemoteBackupStore,
    shard: *const ShardSnapshot,
    hasher: *std.crypto.hash.sha2.Sha256,
) !void {
    const key_prefix = try store.keyPrefixAlloc(alloc, shard.snapshot_path);
    defer alloc.free(key_prefix);
    var continuation_token: ?[]u8 = null;
    defer if (continuation_token) |value| alloc.free(value);
    var previous_key: ?[]u8 = null;
    defer if (previous_key) |value| alloc.free(value);
    var file_count: usize = 0;
    var page_count: usize = 0;

    while (true) {
        if (page_count == backup_integrity_max_native_list_pages)
            return error.BackupArtifactTooLarge;
        page_count += 1;
        var listed = try store.client.listObjects(store.bucket, .{
            .prefix = key_prefix,
            .recursive = true,
            .max_keys = 1000,
            .continuation_token = continuation_token,
        });
        defer listed.deinit(alloc);

        for (listed.entries) |entry| {
            if (!std.mem.startsWith(u8, entry.key, key_prefix))
                return error.InvalidBackupArtifactPath;
            if (previous_key) |value| {
                if (std.mem.order(u8, value, entry.key) != .lt)
                    return error.InvalidContinuationToken;
            }
            const owned_key = try alloc.dupe(u8, entry.key);
            if (previous_key) |value| alloc.free(value);
            previous_key = owned_key;

            const relative_path = entry.key[key_prefix.len..];
            if (relative_path.len == 0) continue;
            try validateArtifactRelativePath(relative_path);
            if (file_count == backup_integrity_max_native_files)
                return error.BackupArtifactTooLarge;
            file_count += 1;
            var owned_etag: ?[]u8 = null;
            defer if (owned_etag) |value| alloc.free(value);
            const etag = if (entry.etag) |value|
                value
            else blk: {
                var metadata = try store.client.statObject(store.bucket, entry.key);
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
            hashArtifactBytes(hasher, relative_path);
            hashArtifactU64(hasher, entry.size);
            hashArtifactBytes(hasher, etag);
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
    if (file_count == 0) return error.BackupArtifactMissing;
    hashArtifactU64(hasher, @intCast(file_count));
}

fn artifactVerificationCacheKeyHasher(
    location: *BackupLocation,
    format: BackupFormat,
    shard: *const ShardSnapshot,
) std.crypto.hash.sha2.Sha256 {
    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    hasher.update("antfly-artifact-verification-receipt-v1");
    hasher.update(&.{@intFromEnum(format)});
    hashArtifactBytes(&hasher, shard.snapshot_path);
    hashArtifactU64(&hasher, shard.artifact_size_bytes);
    hashArtifactBytes(&hasher, shard.artifact_sha256);
    hashArtifactU64(&hasher, shard.native_manifest_size_bytes);
    hashArtifactBytes(&hasher, shard.native_manifest_sha256);

    switch (location.*) {
        .file => |backup_root| {
            hasher.update("file");
            hashArtifactBytes(&hasher, backup_root);
        },
        .remote => |*store| {
            hasher.update("remote");
            hashArtifactBytes(&hasher, store.bucket);
            hashArtifactBytes(&hasher, store.prefix);
        },
    }
    return hasher;
}

fn artifactVerificationReceiptLookupKey(
    location: *BackupLocation,
    format: BackupFormat,
    shard: *const ShardSnapshot,
) [std.crypto.hash.sha2.Sha256.digest_length]u8 {
    var hasher = artifactVerificationCacheKeyHasher(location, format, shard);
    var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    hasher.final(&digest);
    return digest;
}

fn artifactVerificationCacheKey(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    format: BackupFormat,
    shard: *const ShardSnapshot,
) ![std.crypto.hash.sha2.Sha256.digest_length]u8 {
    var hasher = artifactVerificationCacheKeyHasher(location, format, shard);
    switch (location.*) {
        .file => |backup_root| {
            const artifact_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{
                backup_root,
                shard.snapshot_path,
            });
            defer alloc.free(artifact_path);
            switch (format) {
                .portable => {
                    var file = if (std.fs.path.isAbsolute(artifact_path))
                        try std.Io.Dir.openFileAbsolute(io, artifact_path, .{})
                    else
                        try std.Io.Dir.cwd().openFile(io, artifact_path, .{});
                    defer file.close(io);
                    const stat = try file.stat(io);
                    if (stat.kind != .file) return error.BackupArtifactMissing;
                    hashLocalArtifactStat(&hasher, stat);
                },
                .native => try hashLocalNativeArtifactIdentity(
                    alloc,
                    io,
                    artifact_path,
                    &hasher,
                ),
            }
        },
        .remote => |*store| switch (format) {
            .portable => {
                const key = try store.keyAlloc(alloc, shard.snapshot_path);
                defer alloc.free(key);
                var metadata = try store.client.statObject(store.bucket, key);
                defer metadata.deinit(alloc);
                const etag = metadata.etag orelse
                    return error.RestoreArtifactIdentityMissing;
                hashArtifactBytes(&hasher, key);
                hashArtifactU64(&hasher, metadata.content_length);
                hashArtifactBytes(&hasher, etag);
            },
            .native => try hashRemoteNativeArtifactIdentity(
                alloc,
                store,
                shard,
                &hasher,
            ),
        },
    }
    var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    hasher.final(&digest);
    return digest;
}

fn recordArtifactVerificationReceiptAfterFence(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    format: BackupFormat,
    shard: *const ShardSnapshot,
    cache: *ArtifactVerificationCache,
    receipt_key: [std.crypto.hash.sha2.Sha256.digest_length]u8,
    exact_identity: [std.crypto.hash.sha2.Sha256.digest_length]u8,
) !void {
    // The exact pass binds every payload byte, while this metadata-only pass
    // fences directory membership and object identity changes that overlap
    // verification. Cache misses therefore require no second payload read.
    const final_identity = artifactVerificationCacheKey(
        alloc,
        io,
        location,
        format,
        shard,
    ) catch |err| switch (err) {
        error.FileNotFound => return error.BackupArtifactMissing,
        else => return err,
    };
    if (!std.mem.eql(u8, &exact_identity, &final_identity))
        return error.SourceFileChanged;
    try cache.record(alloc, receipt_key, final_identity);
}

/// Cryptographically verifies one table backup with bounded memory. Restore
/// admission uses this immediately before publication so the exact requested
/// artifact, rather than an unrelated newer backup, is bound to the restore.
pub fn verifyTableBackupArtifactsIntegrityAtLocation(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    table_manifest: *const TableBackupManifest,
) !void {
    return verifyTableBackupArtifactsIntegrityAtLocationWithCache(
        alloc,
        io,
        location,
        table_manifest,
        null,
    );
}

pub fn verifyTableBackupArtifactsIntegrityAtLocationWithCache(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    table_manifest: *const TableBackupManifest,
    cache: ?*ArtifactVerificationCache,
) !void {
    if (table_manifest.artifact_integrity_mode != .declared)
        return error.BackupIntegrityMissing;
    for (table_manifest.shards) |*shard| {
        const receipt_key = if (cache != null)
            artifactVerificationReceiptLookupKey(
                location,
                table_manifest.format,
                shard,
            )
        else
            null;
        if (receipt_key) |key| {
            if (cache.?.receipt(key)) |verified_identity| {
                const current_identity = artifactVerificationCacheKey(
                    alloc,
                    io,
                    location,
                    table_manifest.format,
                    shard,
                ) catch |err| switch (err) {
                    error.FileNotFound => return error.BackupArtifactMissing,
                    else => return err,
                };
                if (std.mem.eql(u8, &verified_identity, &current_identity))
                    continue;
            }
        }

        var exact_identity_hasher = artifactVerificationCacheKeyHasher(
            location,
            table_manifest.format,
            shard,
        );
        const identity_hasher = if (cache != null)
            &exact_identity_hasher
        else
            null;
        switch (location.*) {
            .file => |backup_root| {
                const artifact_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{
                    backup_root,
                    shard.snapshot_path,
                });
                defer alloc.free(artifact_path);
                var actual = switch (table_manifest.format) {
                    .portable => fileArtifactIntegrityAllocWithIdentity(
                        alloc,
                        io,
                        artifact_path,
                        identity_hasher,
                    ),
                    .native => directoryArtifactIntegrityAllocWithIdentity(
                        alloc,
                        io,
                        artifact_path,
                        identity_hasher,
                    ),
                } catch |err| switch (err) {
                    error.FileNotFound => return error.BackupArtifactMissing,
                    else => return err,
                };
                defer actual.deinit(alloc);
                if (actual.size_bytes != shard.artifact_size_bytes or
                    !std.mem.eql(u8, actual.sha256, shard.artifact_sha256))
                {
                    return error.BackupArtifactIntegrityMismatch;
                }
            },
            .remote => |*store| switch (table_manifest.format) {
                .portable => store.verifyPortableArtifactIntegrityWithIdentity(
                    alloc,
                    shard,
                    identity_hasher,
                ),
                .native => store.verifyNativeArtifactIntegrityWithIdentity(
                    alloc,
                    shard,
                    identity_hasher,
                ),
            } catch |err| switch (err) {
                error.FileNotFound => return error.BackupArtifactMissing,
                else => return err,
            },
        }

        if (receipt_key) |key| {
            var exact_identity: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
            exact_identity_hasher.final(&exact_identity);
            try recordArtifactVerificationReceiptAfterFence(
                alloc,
                io,
                location,
                table_manifest.format,
                shard,
                cache.?,
                key,
                exact_identity,
            );
        }
    }
}

/// Cryptographically verifies a complete cluster backup with bounded memory.
/// Callers use this for both the authoritative newest-attempt health gate and
/// the exact backup being materialized. Reads are streamed in bounded chunks;
/// repository integrity therefore remains fail-closed without buffering large
/// artifacts in memory.
pub fn verifyClusterBackupArtifactsIntegrityAtLocation(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    manifest: *const ClusterBackupManifest,
) !void {
    return verifyClusterBackupArtifactsIntegrityAtLocationWithCache(
        alloc,
        io,
        location,
        manifest,
        null,
    );
}

pub fn verifyClusterBackupArtifactsIntegrityAtLocationWithCache(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    manifest: *const ClusterBackupManifest,
    cache: ?*ArtifactVerificationCache,
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
        try verifyTableBackupArtifactsIntegrityAtLocationWithCache(
            alloc,
            io,
            location,
            &table_manifest,
            cache,
        );
    }
}

fn ensureClusterBackupAttemptHeadRestorable(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    head: *const ClusterBackupAttemptHead,
    cache: ?*ArtifactVerificationCache,
) !void {
    try validateClusterBackupAttemptHead(head);
    if (head.state == .failed) return error.IncompleteClusterBackup;
    var marker = try readClusterBackupAttemptMarker(alloc, io, location, head.attempt_id);
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
    if (!clusterManifestMatchesAttemptMarker(&manifest, &marker.value))
        return error.IncompleteClusterBackup;
    // Repository health is fail-closed: a historical restore must not mask
    // same-size corruption in the authoritative newest attempt. Receipts
    // eliminate duplicate payload reads when that attempt is also selected.
    try verifyClusterBackupArtifactsIntegrityAtLocationWithCache(
        alloc,
        io,
        location,
        &manifest,
        cache,
    );
}

fn clusterManifestMatchesAttemptMarker(
    manifest: *const ClusterBackupManifest,
    marker: *const ClusterBackupAttemptMarker,
) bool {
    if (manifest.format_version != cluster_format_version or
        manifest.expected_table_count != marker.tables.len or
        manifest.completed_table_count != marker.tables.len)
        return false;
    for (marker.tables) |attempt_table| {
        const committed_table = findClusterTable(manifest, attempt_table.name) orelse
            return false;
        if (!std.mem.eql(u8, committed_table.table_backup_id, attempt_table.table_backup_id))
            return false;
        if (committed_table.artifact_backup_id) |artifact_backup_id| {
            if (!std.mem.eql(u8, artifact_backup_id, attempt_table.artifact_backup_id))
                return false;
        }
    }
    return true;
}

pub fn ensureNewestClusterBackupAttemptRestorable(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
) !void {
    var head = (try readClusterBackupAttemptHead(alloc, io, location)) orelse return;
    defer head.deinit();
    try ensureClusterBackupAttemptHeadRestorable(
        alloc,
        io,
        location,
        &head.value,
        null,
    );
}

fn validateNewestCurrentGoBackupAttempt(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    marker: *const CurrentGoClusterBackupAttemptMarker,
    cache: ?*ArtifactVerificationCache,
) !ClusterBackupManifest {
    if (marker.format != .portable) return error.UnsupportedBackupFormat;

    var decoded = readClusterManifestFromLocationDecoded(
        alloc,
        io,
        location,
        marker.backup_id,
    ) catch |err| switch (err) {
        error.FileNotFound => return error.IncompleteClusterBackup,
        else => return err,
    };
    errdefer decoded.manifest.deinit(alloc);
    if (decoded.encoding != .go_portable_v2 or
        decoded.manifest.expected_table_count != marker.expected_table_count or
        decoded.manifest.completed_table_count != marker.expected_table_count)
    {
        return error.IncompleteClusterBackup;
    }

    var metadata_by_table = std.StringHashMapUnmanaged([]const u8).empty;
    defer metadata_by_table.deinit(alloc);
    try metadata_by_table.ensureTotalCapacity(
        alloc,
        @intCast(marker.table_names.len),
    );
    for (marker.table_names, marker.metadata_ids) |table_name, metadata_id|
        metadata_by_table.putAssumeCapacity(table_name, metadata_id);

    var expected_artifacts = std.StringHashMapUnmanaged(void).empty;
    defer expected_artifacts.deinit(alloc);
    try expected_artifacts.ensureTotalCapacity(
        alloc,
        @intCast(marker.artifact_names.len),
    );
    for (marker.artifact_names) |artifact_name|
        expected_artifacts.putAssumeCapacity(artifact_name, {});

    for (decoded.manifest.tables) |table| {
        const metadata_id = metadata_by_table.get(table.name) orelse
            return error.IncompleteClusterBackup;
        if (!std.mem.eql(u8, metadata_id, table.table_backup_id))
            return error.IncompleteClusterBackup;
        _ = metadata_by_table.remove(table.name);

        var table_manifest = readManifestFromLocationWithArtifactBackupId(
            alloc,
            location,
            table.table_backup_id,
            marker.backup_id,
        ) catch |err| switch (err) {
            error.FileNotFound => return error.IncompleteClusterBackup,
            else => return err,
        };
        defer table_manifest.deinit(alloc);
        if (table_manifest.format != .portable or
            table_manifest.artifact_integrity_mode != .declared or
            !std.mem.eql(u8, table_manifest.table_name, table.name))
        {
            return error.IncompleteClusterBackup;
        }
        for (table_manifest.shards) |*shard| {
            if (!expected_artifacts.remove(shard.snapshot_path))
                return error.IncompleteClusterBackup;
        }
        try verifyTableBackupArtifactsIntegrityAtLocationWithCache(
            alloc,
            io,
            location,
            &table_manifest,
            cache,
        );
    }
    if (metadata_by_table.count() != 0 or expected_artifacts.count() != 0)
        return error.IncompleteClusterBackup;
    return decoded.manifest;
}

fn readCurrentGoManifestForRestoreAdmission(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    backup_id: []const u8,
    cache: ?*ArtifactVerificationCache,
) !ClusterBackupManifest {
    var before = (try readCurrentGoClusterBackupAttemptHead(
        alloc,
        io,
        location,
    )) orelse return error.IncompleteClusterBackup;
    defer before.deinit();
    if (before.value.state == .failed)
        return error.IncompleteClusterBackup;
    var newest_parsed = readCurrentGoAttemptMarkerForHead(
        alloc,
        io,
        location,
        &before.value,
    ) catch |err| switch (err) {
        error.FileNotFound => return error.IncompleteClusterBackup,
        else => return err,
    };
    defer newest_parsed.deinit();
    const newest = &newest_parsed.parsed.value;

    // A Zig producer may have started while the migration head was being
    // validated. Never mix both repository protocols in one admission.
    var intermediate_head = try readClusterBackupAttemptHead(alloc, io, location);
    defer if (intermediate_head) |*head| head.deinit();
    if (intermediate_head != null) return error.BackupRepositoryBusy;

    var newest_manifest = try validateNewestCurrentGoBackupAttempt(
        alloc,
        io,
        location,
        newest,
        cache,
    );
    var decoded: DecodedClusterManifest = if (std.mem.eql(
        u8,
        backup_id,
        newest.backup_id,
    ))
        .{ .manifest = newest_manifest, .encoding = .go_portable_v2 }
    else blk: {
        newest_manifest.deinit(alloc);
        break :blk try readClusterManifestFromLocationDecoded(
            alloc,
            io,
            location,
            backup_id,
        );
    };
    errdefer decoded.manifest.deinit(alloc);
    if (decoded.encoding != .go_portable_v2)
        return error.IncompleteClusterBackup;

    var after = (try readCurrentGoClusterBackupAttemptHead(
        alloc,
        io,
        location,
    )) orelse return error.BackupRepositoryBusy;
    defer after.deinit();
    var final_head = try readClusterBackupAttemptHead(alloc, io, location);
    defer if (final_head) |*head| head.deinit();
    if (final_head != null or
        !currentGoClusterBackupAttemptHeadsEqual(&before.value, &after.value))
    {
        return error.BackupRepositoryBusy;
    }
    return decoded.manifest;
}

/// Restore admission is intentionally repository-scoped: the newest published
/// attempt must be complete and intact before any requested historical
/// manifest may be selected. Keeping both operations behind one API prevents a
/// caller from accidentally bypassing the newest-attempt health invariant.
pub fn readClusterManifestForRestoreAdmission(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    backup_id: []const u8,
) !ClusterBackupManifest {
    return readClusterManifestForRestoreAdmissionWithCache(
        alloc,
        io,
        location,
        backup_id,
        null,
    );
}

pub fn readClusterManifestForRestoreAdmissionWithCache(
    alloc: std.mem.Allocator,
    io: std.Io,
    location: *BackupLocation,
    backup_id: []const u8,
    cache: ?*ArtifactVerificationCache,
) !ClusterBackupManifest {
    // Zig repositories use their monotonic head. Current Go repositories use
    // the compact digest-pinned head. Headless journals are intentionally not
    // accepted by this breaking release.
    var attempt: usize = 0;
    while (attempt < 4) : (attempt += 1) {
        const before = try readClusterBackupAttemptHead(alloc, io, location);
        if (before == null) {
            return readCurrentGoManifestForRestoreAdmission(
                alloc,
                io,
                location,
                backup_id,
                cache,
            ) catch |err| switch (err) {
                error.BackupRepositoryBusy => continue,
                else => return err,
            };
        }
        var stable_before = before.?;
        defer stable_before.deinit();
        try ensureClusterBackupAttemptHeadRestorable(
            alloc,
            io,
            location,
            &stable_before.value,
            cache,
        );

        var manifest = try readClusterManifestFromLocation(alloc, location, backup_id);
        errdefer manifest.deinit(alloc);
        var after = (try readClusterBackupAttemptHead(alloc, io, location)) orelse {
            manifest.deinit(alloc);
            continue;
        };
        defer after.deinit();
        if (clusterBackupAttemptHeadsEqual(
            &stable_before.value,
            &after.value,
        )) {
            return manifest;
        }
        manifest.deinit(alloc);
    }
    return error.BackupRepositoryBusy;
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

fn validateBackupListContinuationProgress(
    current: ?[]const u8,
    next: ?[]const u8,
) !void {
    if (current != null and next != null and
        std.mem.eql(u8, current.?, next.?))
    {
        return error.InvalidContinuationToken;
    }
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
    if (isInvalidBackupManifestError(err)) return "invalid_manifest";
    const storage_class = backupListErrorClass(err);
    return storage_class;
}

fn isSkippableBackupListManifestError(err: anyerror) bool {
    return err == error.FileNotFound or isInvalidBackupManifestError(err);
}

/// Errors that prove the bytes read from a backup repository are not a valid
/// Antfly manifest. Storage, allocation, and transport failures are
/// deliberately excluded so restore handlers do not misreport operational
/// outages as permanent client errors.
pub fn isInvalidBackupManifestError(err: anyerror) bool {
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
    var page_count: usize = 0;
    var previous_key: ?[]u8 = null;
    defer if (previous_key) |key| alloc.free(key);
    while (true) {
        if (page_count == backup_list_max_pages)
            return error.BackupRepositoryHealthScanLimitExceeded;
        page_count += 1;
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
            if (previous_key) |key| {
                if (std.mem.order(u8, key, entry.key) != .lt)
                    return error.InvalidContinuationToken;
            } else if (start_after) |key| {
                if (std.mem.order(u8, key, entry.key) != .lt)
                    return error.InvalidContinuationToken;
            }
            if (previous_key) |key| alloc.free(key);
            previous_key = try alloc.dupe(u8, entry.key);

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
        try validateBackupListContinuationProgress(
            continuation_token,
            next_token,
        );

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
    const ordered_shards = try alloc.dupe(ShardSnapshot, manifest.shards);
    defer alloc.free(ordered_shards);
    metadata_table_manager.sortKeyspaceRanges(ShardSnapshot, ordered_shards);
    metadata_table_manager.validateCompleteKeyspaceRanges(ordered_shards) catch
        return error.InvalidBackupRangeTopology;

    const ranges = try alloc.alloc(metadata_table_manager.RangeRecord, ordered_shards.len);
    var initialized: usize = 0;
    errdefer {
        for (ranges[0..initialized]) |record| metadata_table_manager.freeRange(alloc, record);
        alloc.free(ranges);
    }
    for (ordered_shards, 0..) |shard, i| {
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
    const restore_native_manifest_sha256 = try alloc.dupe(u8, shard.native_manifest_sha256);
    errdefer alloc.free(restore_native_manifest_sha256);
    return .{
        .group_id = shard.group_id,
        .range_id = if (shard.range_id == 0) shard.group_id else shard.range_id,
        .table_id = table_id,
        .doc_identity_shard_id = shard.doc_identity_shard_id,
        .doc_identity_range_id = shard.doc_identity_range_id,
        .split_attempt_epoch = shard.split_attempt_epoch,
        .start_key = start_key,
        .end_key = end_key,
        .restore_backup_id = owned_backup_id,
        .restore_artifact_backup_id = owned_artifact_backup_id,
        .restore_location = restore_location,
        .restore_snapshot_path = restore_snapshot_path,
        .restore_connection = restore_connection,
        .restore_artifact_size_bytes = shard.artifact_size_bytes,
        .restore_artifact_sha256 = restore_artifact_sha256,
        .restore_native_manifest_size_bytes = shard.native_manifest_size_bytes,
        .restore_native_manifest_sha256 = restore_native_manifest_sha256,
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
    try cancellation.check();
    switch (location.*) {
        .file => |backup_root| {
            const dest_root = try shardSnapshotPath(alloc, backup_root, backup_id, group_id);
            defer alloc.free(dest_root);
            var io_impl = std.Io.Threaded.init(alloc, .{});
            defer io_impl.deinit();
            try copyDirectoryRecursiveWithIo(
                alloc,
                io_impl.io(),
                src_path,
                dest_root,
                .transient,
                cancellation,
            );
        },
        .remote => |*store| {
            const dest_suffix = try shardSnapshotRelPath(alloc, backup_id, group_id);
            defer alloc.free(dest_suffix);
            try store.uploadDirectoryRecursive(alloc, src_path, dest_suffix, cancellation);
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
    return try copyDirectoryFromLocationUsingIoWithCancellation(
        alloc,
        shared_io,
        location,
        snapshot_path,
        dest_path,
        .none,
    );
}

pub fn copyDirectoryFromLocationUsingIoWithCancellation(
    alloc: std.mem.Allocator,
    shared_io: ?std.Io,
    location: *BackupLocation,
    snapshot_path: []const u8,
    dest_path: []const u8,
    cancellation: CancellationToken,
) !void {
    try cancellation.check();
    try validateArtifactRelativePath(snapshot_path);
    switch (location.*) {
        .file => |backup_root| {
            const src_root = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ backup_root, snapshot_path });
            defer alloc.free(src_root);
            if (shared_io) |io|
                try copyDirectoryRecursiveWithIo(alloc, io, src_root, dest_path, .transient, cancellation)
            else {
                var io_impl = std.Io.Threaded.init(alloc, .{});
                defer io_impl.deinit();
                try copyDirectoryRecursiveWithIo(alloc, io_impl.io(), src_root, dest_path, .transient, cancellation);
            }
        },
        .remote => |*store| try store.downloadDirectoryRecursiveWithCancellation(
            alloc,
            snapshot_path,
            dest_path,
            cancellation,
        ),
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
            trimLeftSlash(relative_path),
            destination_path,
            expected_size,
            expected_sha256,
            cancellation,
        ),
    }
}

pub fn copyFileToLocation(
    alloc: std.mem.Allocator,
    location: *BackupLocation,
    snapshot_path: []const u8,
    src_path: []const u8,
    content_type: []const u8,
) !void {
    return copyFileToLocationWithCancellation(
        alloc,
        location,
        snapshot_path,
        src_path,
        content_type,
        .none,
    );
}

pub fn copyFileToLocationWithCancellation(
    alloc: std.mem.Allocator,
    location: *BackupLocation,
    snapshot_path: []const u8,
    src_path: []const u8,
    content_type: []const u8,
    cancellation: CancellationToken,
) !void {
    try cancellation.check();
    try validateArtifactRelativePath(snapshot_path);
    switch (location.*) {
        .file => |backup_root| {
            const dest_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ backup_root, snapshot_path });
            defer alloc.free(dest_path);
            var io_impl = std.Io.Threaded.init(alloc, .{});
            defer io_impl.deinit();
            try copyFileAbsoluteWithIoOptionsCancellable(
                io_impl.io(),
                src_path,
                dest_path,
                .durable,
                cancellation,
            );
            try syncPathAncestorsWithIo(io_impl.io(), std.fs.path.dirname(dest_path) orelse ".");
        },
        .remote => |*store| {
            try store.writeFile(alloc, trimLeftSlash(snapshot_path), src_path, content_type, cancellation);
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

pub fn populateShardArtifactIntegrityWithCancellation(
    alloc: std.mem.Allocator,
    shared_io: ?std.Io,
    format: BackupFormat,
    artifact_path: []const u8,
    shard: *ShardSnapshot,
    cancellation: CancellationToken,
) !void {
    var integrity = try artifactIntegrityAllocWithCancellation(
        alloc,
        shared_io,
        format,
        artifact_path,
        cancellation,
    );
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

fn artifactIntegrityAllocCancellableWithIo(
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
    return fileArtifactIntegrityAllocWithIdentity(alloc, io, path, null);
}

fn fileArtifactIntegrityAllocWithIdentity(
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

fn fileArtifactIntegrityAllocCancellable(
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

const NativeArtifactFile = struct {
    path: []u8,
    stat: std.Io.File.Stat,

    fn deinit(self: NativeArtifactFile, alloc: std.mem.Allocator) void {
        alloc.free(self.path);
    }
};

fn directoryArtifactIntegrityAlloc(
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

fn directoryArtifactIntegrityAllocWithIdentity(
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

fn directoryArtifactIntegrityAllocCancellable(
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
    if (!localArtifactStatsEqual(final_stat, initial_stat))
        return error.SourceFileChanged;
}

fn hashFileContentsCancellable(
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
        fn lessThan(_: void, lhs: []u8, rhs: []u8) bool {
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

fn copyFileAndHashCancellable(
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
        fn lessThan(_: void, lhs: []u8, rhs: []u8) bool {
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
    return writeFileAbsoluteIfAbsentWithIoAndCancellation(alloc, io, path, data, .none);
}

fn writeFileAbsoluteIfAbsentWithIoAndCancellation(
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

fn replaceFileAbsoluteUnderHeldLock(
    alloc: std.mem.Allocator,
    io: std.Io,
    path: []const u8,
    data: []const u8,
) !void {
    if (std.fs.path.dirname(path)) |dir_name| try ensureDirPathWithIo(io, dir_name);
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
    try replaceFileAbsoluteUnderHeldLock(alloc, io, path, data);
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

fn copyFileAbsoluteWithIoOptionsCancellable(
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

test "native artifact copy observes cancellation between io chunks" {
    const alloc = std.testing.allocator;
    var source_tmp = std.testing.tmpDir(.{});
    defer source_tmp.cleanup();
    var destination_tmp = std.testing.tmpDir(.{});
    defer destination_tmp.cleanup();
    const source = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}", .{source_tmp.sub_path});
    defer alloc.free(source);
    const destination = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/copy", .{destination_tmp.sub_path});
    defer alloc.free(destination);
    const source_file = try std.fmt.allocPrint(alloc, "{s}/large.bin", .{source});
    defer alloc.free(source_file);
    var file = try fs_paths.createFilePortable(std.testing.io, source_file, .{ .truncate = true });
    defer file.close(std.testing.io);
    var writer_buffer: [4096]u8 = undefined;
    var writer = file.writer(std.testing.io, &writer_buffer);
    const zeros = [_]u8{0} ** (256 * 1024);
    for (0..4) |_| try writer.interface.writeAll(&zeros);
    try writer.end();

    const CancelAfter = struct {
        calls: std.atomic.Value(u32) = .init(0),

        fn isCancelled(ptr: *const anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(@constCast(ptr)));
            return self.calls.fetchAdd(1, .acq_rel) >= 4;
        }
    };
    var state = CancelAfter{};
    try std.testing.expectError(
        error.Canceled,
        copyNativeDirectoryWithIntegrityUsingIo(
            alloc,
            std.testing.io,
            source,
            destination,
            .{ .ptr = &state, .is_cancelled_fn = CancelAfter.isCancelled },
        ),
    );
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
    var ambiguous: usize = 0;
    for (statuses) |status| {
        if (std.mem.eql(u8, status.status, "completed")) {
            completed += 1;
        } else if (std.mem.eql(u8, status.status, "ambiguous")) {
            ambiguous += 1;
        } else {
            failed += 1;
        }
    }
    if (ambiguous > 0) return "ambiguous";
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

test "backup manifest cancellation prevents late publication and permits a later clean attempt" {
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

    var cancelled = std.atomic.Value(bool).init(true);
    try std.testing.expectError(
        error.Canceled,
        writeManifestToLocationWithIoAndCancellation(
            std.testing.allocator,
            location.remote.io,
            &location,
            &manifest,
            CancellationToken.fromAtomic(&cancelled),
        ),
    );
    try std.testing.expectError(
        error.FileNotFound,
        readManifestFromLocation(std.testing.allocator, &location, "snap"),
    );
    cancelled.store(false, .release);
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
        \\{"version":2,"format":"portable","artifacts":[{"name":"go-snap-1.afb","size_bytes":12,"sha256":"ff45daa2dbf814d8ad252fae57bc62d1980fe8e3f2cff145af1072208db937cf"}],"table":{"name":"docs","description":"documents","schema":{"version":0},"indexes":{"embedding":{"provider":"termite"}},"shards":{"1":{"byte_range":["",""]}}}}
    ;
    var manifest = try parseTableBackupManifest(alloc, body, "go-snap");
    defer manifest.deinit(alloc);
    try std.testing.expectEqual(ArtifactIntegrityMode.declared, manifest.artifact_integrity_mode);
    try std.testing.expectEqual(BackupFormat.portable, manifest.format);
    try std.testing.expectEqualStrings("docs", manifest.table_name);
    try std.testing.expectEqualStrings("go-snap-1.afb", manifest.shards[0].snapshot_path);
    try std.testing.expect(std.mem.indexOf(u8, manifest.indexes_json, "\"provider\":\"antfly\"") != null);
    try validatePublishedTableManifest(alloc, &manifest, manifest.backup_id);
    const current = try stringifyJsonAlloc(alloc, manifest);
    defer alloc.free(current);
    var reparsed = try parseTableBackupManifest(alloc, current, manifest.backup_id);
    reparsed.deinit(alloc);

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
    try std.testing.expectEqual(@as(u64, "portable-afb".len), manifest.shards[0].artifact_size_bytes);
    try verifyShardArtifactIntegrity(alloc, null, .portable, artifact_path, &manifest.shards[0]);

    try std.testing.expectError(
        error.UnsupportedBackupFormat,
        parseTableBackupManifest(
            alloc,
            "{\"version\":1,\"format\":\"portable\",\"table\":{}}",
            "go-snap",
        ),
    );
}

test "current Go portable metadata parsing is allocation failure safe" {
    const Runner = struct {
        fn run(alloc: std.mem.Allocator) !void {
            const body =
                \\{"version":2,"format":"portable","artifacts":[{"name":"go-snap-1.afb","size_bytes":12,"sha256":"ff45daa2dbf814d8ad252fae57bc62d1980fe8e3f2cff145af1072208db937cf"}],"table":{"name":"docs","description":"documents","schema":{"version":0},"indexes":{"embedding":{"provider":"termite"}},"shards":{"1":{"byte_range":["",""]}}}}
            ;
            var manifest = try parseTableBackupManifest(alloc, body, "go-snap");
            defer manifest.deinit(alloc);
            try std.testing.expectEqualStrings("docs", manifest.table_name);
        }
    };
    try std.testing.checkAllAllocationFailures(
        std.testing.allocator,
        Runner.run,
        .{},
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
        "{\"version\":2,\"format\":\"portable\",\"artifacts\":[{\"name\":\"go-cluster-1.afb\",\"size_bytes\":20,\"sha256\":\"54e3c2a20e9aebe140e2f41e79fc798f0ccfae5641fd30da978b594321b9c559\"}],\"table\":{\"name\":\"docs\",\"shards\":{\"1\":{\"byte_range\":[\"\",\"\"]}}}}",
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
    try verifyShardArtifactIntegrity(
        alloc,
        null,
        .portable,
        artifact_path,
        &table_manifest.shards[0],
    );

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
    try validateBackupListContinuationProgress(null, "page-1");
    try validateBackupListContinuationProgress("page-1", "page-2");
    try std.testing.expectError(
        error.InvalidContinuationToken,
        validateBackupListContinuationProgress("page-1", "page-1"),
    );

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
            _: ?object_storage.CancellationToken,
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

test "cluster repository reservation serializes distinct backup ids and owners" {
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

    try reserveClusterBackupAttemptLeaseAtLocation(
        alloc,
        io,
        &location,
        "cluster-snap",
        "attempt-old",
        std.math.maxInt(u64),
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
        reserveClusterBackupAttemptLeaseAtLocation(
            alloc,
            io,
            &location,
            "different-cluster",
            "attempt-new",
            std.math.maxInt(u64),
        ),
    );
    try std.testing.expect(try cleanupClusterReservationIfOwnedAtLocation(
        alloc,
        io,
        &location,
        "cluster-snap",
        "attempt-old",
    ));
    try reserveClusterBackupAttemptLeaseAtLocation(
        alloc,
        io,
        &location,
        "cluster-snap",
        "attempt-new",
        std.math.maxInt(u64),
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
        readClusterManifestForRestoreAdmission(
            alloc,
            io,
            &location,
            committed.backup_id,
        ),
    );
}

test "attempt head generation detects publication and retirement ABA" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    var memory = object_storage.MemoryObjectStorage.init(alloc);
    defer memory.deinit();
    var location: BackupLocation = .{
        .remote = try RemoteBackupStore.initWithClient(
            alloc,
            memory.client(),
            "bucket",
            "backups",
        ),
    };
    defer location.deinit(alloc);

    try writeClusterBackupAttemptHead(alloc, io, &location, "attempt-a");
    var first = (try readClusterBackupAttemptHead(alloc, io, &location)) orelse
        return error.TestUnexpectedResult;
    defer first.deinit();
    try std.testing.expectEqual(@as(u64, 1), first.value.generation);

    try writeClusterBackupAttemptHead(alloc, io, &location, "attempt-b");
    var second = (try readClusterBackupAttemptHead(alloc, io, &location)) orelse
        return error.TestUnexpectedResult;
    defer second.deinit();
    try std.testing.expectEqual(@as(u64, 2), second.value.generation);
    try std.testing.expect(!clusterBackupAttemptHeadsEqual(&first.value, &second.value));

    try std.testing.expect(try retireClusterBackupAttemptHeadIfOwned(
        alloc,
        io,
        &location,
        "attempt-b",
    ));
    var retired = (try readClusterBackupAttemptHead(alloc, io, &location)) orelse
        return error.TestUnexpectedResult;
    defer retired.deinit();
    try std.testing.expectEqual(@as(u64, 3), retired.value.generation);
    try std.testing.expectEqual(ClusterBackupAttemptState.failed, retired.value.state);
    try std.testing.expect(!clusterBackupAttemptHeadsEqual(&second.value, &retired.value));

    try writeClusterBackupAttemptHead(alloc, io, &location, "attempt-c");
    try std.testing.expect(try commitClusterBackupAttemptHeadIfOwned(
        alloc,
        io,
        &location,
        "attempt-c",
    ));
    try std.testing.expect(!try retireClusterBackupAttemptHeadIfOwned(
        alloc,
        io,
        &location,
        "attempt-c",
    ));
    var committed = (try readClusterBackupAttemptHead(alloc, io, &location)) orelse
        return error.TestUnexpectedResult;
    defer committed.deinit();
    try std.testing.expectEqual(@as(u64, 5), committed.value.generation);
    try std.testing.expectEqual(ClusterBackupAttemptState.committed, committed.value.state);
}

test "newest attempt exact verification detects corruption and receipts revalidate identity" {
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
        ensureNewestClusterBackupAttemptRestorable(
            alloc,
            io,
            &location,
        ),
    );
    try std.testing.expectError(
        error.BackupArtifactIntegrityMismatch,
        verifyClusterBackupArtifactsIntegrityAtLocation(
            alloc,
            io,
            &location,
            &committed,
        ),
    );

    try writeFileAbsolute(artifact_path, "payload");
    var verification_cache: ArtifactVerificationCache = .{};
    defer verification_cache.deinit(alloc);
    var admitted = try readClusterManifestForRestoreAdmissionWithCache(
        alloc,
        io,
        &location,
        "cluster-snap",
        &verification_cache,
    );
    defer admitted.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), verification_cache.entries.count());
    try verifyClusterBackupArtifactsIntegrityAtLocationWithCache(
        alloc,
        io,
        &location,
        &admitted,
        &verification_cache,
    );
    try std.testing.expectEqual(@as(usize, 1), verification_cache.entries.count());

    // A same-size replacement must miss the receipt because its immutable
    // filesystem identity changed, then fail exact verification.
    try writeFileAbsolute(artifact_path, "PAYLOAD");
    try std.testing.expectError(
        error.BackupArtifactIntegrityMismatch,
        verifyClusterBackupArtifactsIntegrityAtLocationWithCache(
            alloc,
            io,
            &location,
            &admitted,
            &verification_cache,
        ),
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
    var remote_cache: ArtifactVerificationCache = .{};
    defer remote_cache.deinit(alloc);
    try verifyClusterBackupArtifactsIntegrityAtLocationWithCache(
        alloc,
        io,
        &remote,
        &remote_manifest,
        &remote_cache,
    );
    try std.testing.expectEqual(@as(usize, 1), remote_cache.entries.count());
    try verifyClusterBackupArtifactsIntegrityAtLocationWithCache(
        alloc,
        io,
        &remote,
        &remote_manifest,
        &remote_cache,
    );
    try remote.remote.writeBytes(
        alloc,
        "remote-table.afb",
        "PAYLOAD",
        "application/vnd.antfly.backup",
    );
    try std.testing.expectError(
        error.BackupArtifactIntegrityMismatch,
        verifyClusterBackupArtifactsIntegrityAtLocationWithCache(
            alloc,
            io,
            &remote,
            &remote_manifest,
            &remote_cache,
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

test "forwarded backup envelope retirement preserves canonical payload" {
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

    try reserveBackupAtLocation(alloc, io, &location, "forwarded", false);
    try location.remote.writeBytes(alloc, "forwarded-metadata.json", "temporary-envelope", "application/json");
    try location.remote.writeBytes(alloc, "forwarded.afb", "canonical-payload", "application/vnd.antfly.backup");
    try retireForwardedTableBackupEnvelopeAtLocation(alloc, io, &location, "forwarded");

    try std.testing.expectError(error.FileNotFound, location.remote.readBytesAllocLimited(alloc, "forwarded-metadata.json", 64));
    const payload = try location.remote.readBytesAllocLimited(alloc, "forwarded.afb", 64);
    defer alloc.free(payload);
    try std.testing.expectEqualStrings("canonical-payload", payload);
    try reserveBackupAtLocation(alloc, io, &location, "forwarded", false);
}

test "table backup reservation durably binds logical and artifact ids" {
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

    try reserveTableBackupAttemptAtLocation(
        alloc,
        io,
        &location,
        "logical",
        "artifact-generation",
        .portable,
        .{
            .metadata_group_id = 3,
            .metadata_incarnation = "0123456789abcdef0123456789abcdef".*,
            .table_id = 7,
            .definition_digest = [_]u8{0x11} ** 32,
            .topology_range_count = 1,
            .topology_digest = [_]u8{0x22} ** 32,
        },
    );
    var reservation = (try readTableBackupAttemptReservation(alloc, io, &location, "logical")).?;
    defer reservation.deinit();
    try std.testing.expectEqual(table_backup_attempt_reservation_version, reservation.value.format_version);
    try std.testing.expect(reservation.value.created_at_unix_ns != null);
    try std.testing.expectEqual(@as(?u64, null), reservation.value.writer_not_after_unix_ns);
    const artifact_id = (try tableBackupAttemptArtifactIdAlloc(alloc, io, &location, "logical")).?;
    defer alloc.free(artifact_id);
    try std.testing.expectEqualStrings("artifact-generation", artifact_id);
    try std.testing.expectError(
        error.BackupAlreadyExists,
        reserveTableBackupAttemptAtLocation(
            alloc,
            io,
            &location,
            "logical",
            "other-generation",
            .portable,
            .{
                .metadata_group_id = 3,
                .metadata_incarnation = "0123456789abcdef0123456789abcdef".*,
                .table_id = 7,
                .definition_digest = [_]u8{0x11} ** 32,
                .topology_range_count = 1,
                .topology_digest = [_]u8{0x22} ** 32,
            },
        ),
    );

    // Cleanup must bind its delete to the artifact generation it inspected.
    // A retry that acquires the logical name after the old reservation is
    // removed cannot be erased by the old cleanup worker.
    try std.testing.expect(try deleteTableBackupReservationIfArtifactOwnedAtLocation(
        alloc,
        io,
        &location,
        "logical",
        "artifact-generation",
    ));
    try reserveTableBackupAttemptAtLocation(
        alloc,
        io,
        &location,
        "logical",
        "retry-generation",
        .portable,
        .{
            .metadata_group_id = 3,
            .metadata_incarnation = "0123456789abcdef0123456789abcdef".*,
            .table_id = 7,
            .definition_digest = [_]u8{0x11} ** 32,
            .topology_range_count = 1,
            .topology_digest = [_]u8{0x22} ** 32,
        },
    );
    try std.testing.expect(!try deleteTableBackupReservationIfArtifactOwnedAtLocation(
        alloc,
        io,
        &location,
        "logical",
        "artifact-generation",
    ));
    try std.testing.expect(try tableBackupAttemptMatchesAtLocation(
        alloc,
        io,
        &location,
        "logical",
        "retry-generation",
    ));

    const response_body = try encodeBackupOutcomeAmbiguousBody(alloc, "logical", artifact_id);
    defer alloc.free(response_body);
    try ant_json.testing.expectEqualJsonText(
        alloc,
        "{\"code\":\"backup_outcome_ambiguous\",\"error\":\"backup outcome is ambiguous; inspect the backup id before retrying\",\"message\":\"backup outcome is ambiguous; inspect the backup id and artifact id before retrying\",\"retryable\":false,\"backup_id\":\"logical\",\"artifact_backup_id\":\"artifact-generation\"}",
        response_body,
    );
}

test "stale table reclaim reports a concurrently replaced generation" {
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
    const fence: TableBackupFence = .{
        .metadata_group_id = 3,
        .metadata_incarnation = "0123456789abcdef0123456789abcdef".*,
        .table_id = 7,
        .definition_digest = [_]u8{0x11} ** 32,
        .topology_range_count = 1,
        .topology_digest = [_]u8{0x22} ** 32,
        .writer_not_after_unix_ns = 2,
    };
    try reserveTableBackupAttemptAtLocation(alloc, io, &location, "logical", "artifact-a", .portable, fence);
    var budget: usize = backup_attempt_reclaim_object_budget;
    var inspection = (try inspectTableBackupAttemptAtLocationWithBudgetAndCancellation(
        alloc,
        io,
        &location,
        "logical",
        &budget,
        .none,
    )).?;
    defer inspection.deinit(alloc);

    try std.testing.expect(try deleteTableBackupReservationIfArtifactOwnedAtLocation(
        alloc,
        io,
        &location,
        "logical",
        "artifact-a",
    ));
    try reserveTableBackupAttemptAtLocation(alloc, io, &location, "logical", "artifact-b", .portable, fence);

    try std.testing.expectEqual(
        TableBackupAttemptReclaimProgress.replaced,
        try advanceExpiredTableBackupAttemptAtLocationWithCancellation(
            alloc,
            io,
            &location,
            "logical",
            &inspection,
            std.math.maxInt(u64),
            &budget,
            .none,
        ),
    );
    try std.testing.expect(try tableBackupAttemptMatchesAtLocation(
        alloc,
        io,
        &location,
        "logical",
        "artifact-b",
    ));
    const replaced_lease_suffix = try tableBackupWriterLeasePath(alloc, "", "artifact-a");
    defer alloc.free(replaced_lease_suffix);
    try std.testing.expectError(
        error.FileNotFound,
        location.remote.readBytesAllocLimited(
            alloc,
            trimLeftSlash(replaced_lease_suffix),
            max_backup_attempt_lease_bytes,
        ),
    );
}

test "stale table reclaim honors cancellation before storage mutation" {
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
    const fence: TableBackupFence = .{
        .metadata_group_id = 3,
        .metadata_incarnation = "0123456789abcdef0123456789abcdef".*,
        .table_id = 7,
        .definition_digest = [_]u8{0x11} ** 32,
        .topology_range_count = 1,
        .topology_digest = [_]u8{0x22} ** 32,
    };
    try reserveTableBackupAttemptAtLocation(alloc, io, &location, "logical", "artifact", .portable, fence);
    var budget: usize = backup_attempt_reclaim_object_budget;
    var inspection = (try inspectTableBackupAttemptAtLocationWithBudgetAndCancellation(
        alloc,
        io,
        &location,
        "logical",
        &budget,
        .none,
    )).?;
    defer inspection.deinit(alloc);
    var cancelled = std.atomic.Value(bool).init(true);
    try std.testing.expectError(
        error.Canceled,
        advanceExpiredTableBackupAttemptAtLocationWithCancellation(
            alloc,
            io,
            &location,
            "logical",
            &inspection,
            std.math.maxInt(u64),
            &budget,
            CancellationToken.fromAtomic(&cancelled),
        ),
    );
    try std.testing.expect(try tableBackupAttemptMatchesAtLocation(
        alloc,
        io,
        &location,
        "logical",
        "artifact",
    ));
}

test "table backup collision commit check is bounded and exact" {
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

    try location.remote.writeBytes(
        alloc,
        "logical-metadata.json",
        "{}",
        "application/json",
    );
    var budget: usize = 1;
    try std.testing.expect(try tableBackupAttemptCommittedAtLocationWithBudgetAndCancellation(
        alloc,
        io,
        &location,
        "logical",
        &budget,
        .none,
    ));
    try std.testing.expectEqual(@as(usize, 0), budget);
    try std.testing.expectError(
        error.BackupCleanupBudgetExceeded,
        tableBackupAttemptCommittedAtLocationWithBudgetAndCancellation(
            alloc,
            io,
            &location,
            "logical",
            &budget,
            .none,
        ),
    );
}

test "table backup writer lease fences cleanup until the storage owner expires" {
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

    try reserveTableBackupWriterLeaseAtLocation(alloc, io, &location, "artifact", 100);
    try std.testing.expectEqual(BackupLeaseFenceClaim.active, try claimExpiredTableBackupWriterLeaseAtLocation(
        alloc,
        io,
        &location,
        "artifact",
        100 + backup_attempt_lease_clock_skew_allowance_ns - 1,
    ));
    try std.testing.expect(try renewTableBackupWriterLeaseAtLocation(
        alloc,
        io,
        &location,
        "artifact",
        200,
    ));
    try std.testing.expectEqual(BackupLeaseFenceClaim.claimed, try claimExpiredTableBackupWriterLeaseAtLocation(
        alloc,
        io,
        &location,
        "artifact",
        200 + backup_attempt_lease_clock_skew_allowance_ns,
    ));
    try std.testing.expect(!try renewTableBackupWriterLeaseAtLocation(
        alloc,
        io,
        &location,
        "artifact",
        300,
    ));
    try std.testing.expectError(
        error.BackupAlreadyExists,
        reserveTableBackupWriterLeaseAtLocation(alloc, io, &location, "artifact", 300),
    );

    // Missing is not equivalent to safe: a forwarded request may simply be
    // delayed before its first lease write. Claiming the empty slot keeps that
    // writer fenced throughout cleanup.
    try std.testing.expectEqual(BackupLeaseFenceClaim.claimed, try claimExpiredTableBackupWriterLeaseAtLocation(
        alloc,
        io,
        &location,
        "delayed-artifact",
        1,
    ));
    try std.testing.expectError(
        error.BackupAlreadyExists,
        reserveTableBackupWriterLeaseAtLocation(alloc, io, &location, "delayed-artifact", 300),
    );
}

test "standalone table backup stale reclamation fences delayed writers" {
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
    const fence: TableBackupFence = .{
        .metadata_group_id = 3,
        .metadata_incarnation = "0123456789abcdef0123456789abcdef".*,
        .table_id = 7,
        .definition_digest = [_]u8{0x11} ** 32,
        .topology_range_count = 1,
        .topology_digest = [_]u8{0x22} ** 32,
    };
    try reserveTableBackupAttemptAtLocation(alloc, io, &location, "logical", "artifact", .portable, fence);
    try reserveTableBackupWriterLeaseAtLocation(alloc, io, &location, "artifact", 1);
    try location.remote.writeBytes(alloc, "artifact.afb", "partial", "application/vnd.antfly.backup");

    try std.testing.expectEqual(
        TableBackupAttemptReclaimResult.reclaimed,
        try reclaimExpiredTableBackupAttemptAtLocation(
            alloc,
            io,
            &location,
            "logical",
            std.math.maxInt(u64),
        ),
    );
    try std.testing.expect((try tableBackupAttemptArtifactIdAlloc(alloc, io, &location, "logical")) == null);
    try std.testing.expect(!try renewTableBackupWriterLeaseAtLocation(alloc, io, &location, "artifact", std.math.maxInt(u64)));
    try std.testing.expectError(
        error.BackupAlreadyExists,
        reserveTableBackupWriterLeaseAtLocation(alloc, io, &location, "artifact", std.math.maxInt(u64)),
    );
    try std.testing.expect(!try tableBackupAttemptMatchesAtLocation(alloc, io, &location, "logical", "artifact"));
    try std.testing.expectError(error.FileNotFound, location.remote.readBytesAllocLimited(alloc, "artifact.afb", 64));
}

test "deadline-fenced stale table cleanup retires its generation tombstone" {
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
    const fence: TableBackupFence = .{
        .metadata_group_id = 3,
        .metadata_incarnation = "0123456789abcdef0123456789abcdef".*,
        .table_id = 7,
        .definition_digest = [_]u8{0x11} ** 32,
        .topology_range_count = 1,
        .topology_digest = [_]u8{0x22} ** 32,
        .writer_not_after_unix_ns = 2,
    };
    try reserveTableBackupAttemptAtLocation(alloc, io, &location, "logical", "artifact", .portable, fence);
    try reserveTableBackupWriterLeaseAtLocation(alloc, io, &location, "artifact", 2);
    try location.remote.writeBytes(alloc, "artifact.afb", "partial", "application/vnd.antfly.backup");

    try std.testing.expectEqual(
        TableBackupAttemptReclaimResult.reclaimed,
        try reclaimExpiredTableBackupAttemptAtLocation(
            alloc,
            io,
            &location,
            "logical",
            std.math.maxInt(u64),
        ),
    );
    // The protocol deadline, rather than a permanent per-generation object,
    // now fences late delivery.
    const lease_suffix = try tableBackupWriterLeasePath(alloc, "", "artifact");
    defer alloc.free(lease_suffix);
    try std.testing.expectError(
        error.FileNotFound,
        location.remote.readBytesAllocLimited(alloc, trimLeftSlash(lease_suffix), max_backup_attempt_lease_bytes),
    );
}

test "unpublished table cleanup retains its retry address until writer state retires" {
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
    const fence: TableBackupFence = .{
        .metadata_group_id = 3,
        .metadata_incarnation = "0123456789abcdef0123456789abcdef".*,
        .table_id = 7,
        .definition_digest = [_]u8{0x11} ** 32,
        .topology_range_count = 1,
        .topology_digest = [_]u8{0x22} ** 32,
    };
    try reserveTableBackupAttemptAtLocation(alloc, io, &location, "logical", "artifact", .portable, fence);
    try reserveTableBackupWriterLeaseAtLocation(alloc, io, &location, "artifact", std.math.maxInt(u64));
    try location.remote.writeBytes(alloc, "artifact.afb", "partial", "application/vnd.antfly.backup");

    const InjectedFailure = struct {
        fn fail() !void {
            return error.InjectedWriterStateFailure;
        }
    };
    var cleanup_budget: usize = backup_attempt_cleanup_object_budget;
    try std.testing.expectError(
        error.InjectedWriterStateFailure,
        advanceUnpublishedTableBackupAttemptCleanupAtLocationWithHook(
            alloc,
            io,
            &location,
            "logical",
            "artifact",
            .portable,
            .retire_if_owned,
            &cleanup_budget,
            .none,
            InjectedFailure.fail,
        ),
    );

    // Payload deletion may finish first, but the durable reservation remains
    // until the writer lease is known to be gone, making the cleanup retryable.
    try std.testing.expect(try tableBackupAttemptMatchesAtLocation(alloc, io, &location, "logical", "artifact"));
    try std.testing.expectError(error.FileNotFound, location.remote.readBytesAllocLimited(alloc, "artifact.afb", 64));
    const lease_suffix = try tableBackupWriterLeasePath(alloc, "", "artifact");
    defer alloc.free(lease_suffix);
    const retained_lease = try location.remote.readBytesAllocLimited(
        alloc,
        trimLeftSlash(lease_suffix),
        max_backup_attempt_lease_bytes,
    );
    alloc.free(retained_lease);

    try cleanupUnpublishedTableBackupAttemptAndWriterStateAtLocation(
        alloc,
        io,
        &location,
        "logical",
        "artifact",
        .portable,
        .retire_if_owned,
    );
    try std.testing.expect(!try tableBackupAttemptMatchesAtLocation(alloc, io, &location, "logical", "artifact"));
    try std.testing.expectError(
        error.FileNotFound,
        location.remote.readBytesAllocLimited(alloc, trimLeftSlash(lease_suffix), max_backup_attempt_lease_bytes),
    );
}

test "unpublished table cleanup preserves its reservation on writer owner mismatch" {
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
    const fence: TableBackupFence = .{
        .metadata_group_id = 3,
        .metadata_incarnation = "0123456789abcdef0123456789abcdef".*,
        .table_id = 7,
        .definition_digest = [_]u8{0x11} ** 32,
        .topology_range_count = 1,
        .topology_digest = [_]u8{0x22} ** 32,
    };
    try reserveTableBackupAttemptAtLocation(alloc, io, &location, "logical", "artifact", .portable, fence);
    try reserveTableBackupWriterLeaseAtLocation(alloc, io, &location, "artifact", std.math.maxInt(u64));
    const lease_suffix = try tableBackupWriterLeasePath(alloc, "", "artifact");
    defer alloc.free(lease_suffix);
    try location.remote.writeBytes(
        alloc,
        trimLeftSlash(lease_suffix),
        "replacement-owner\n123\n",
        "text/plain",
    );

    try std.testing.expectEqual(
        TableBackupWriterLeaseRetirement.different_owner,
        try retireTableBackupWriterLeaseAtLocationWithCancellation(
            alloc,
            io,
            &location,
            "artifact",
            .none,
        ),
    );
    try std.testing.expectError(
        error.BackupWriterLeaseRetirementDeferred,
        cleanupUnpublishedTableBackupAttemptAndWriterStateAtLocation(
            alloc,
            io,
            &location,
            "logical",
            "artifact",
            .portable,
            .retire_if_owned,
        ),
    );
    try std.testing.expect(try tableBackupAttemptMatchesAtLocation(
        alloc,
        io,
        &location,
        "logical",
        "artifact",
    ));
}

test "unpublished table cleanup exposes bounded resumable progress" {
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
    const fence: TableBackupFence = .{
        .metadata_group_id = 3,
        .metadata_incarnation = "0123456789abcdef0123456789abcdef".*,
        .table_id = 7,
        .definition_digest = [_]u8{0x11} ** 32,
        .topology_range_count = 1,
        .topology_digest = [_]u8{0x22} ** 32,
    };
    try reserveTableBackupAttemptAtLocation(alloc, io, &location, "logical", "artifact", .native, fence);
    try reserveTableBackupWriterLeaseAtLocation(alloc, io, &location, "artifact", std.math.maxInt(u64));
    for (0..64) |i| {
        const suffix = try std.fmt.allocPrint(alloc, "artifact/groups/1/file-{d}", .{i});
        defer alloc.free(suffix);
        try location.remote.writeBytes(alloc, suffix, "partial", "application/octet-stream");
    }

    var request_budget: usize = backup_attempt_request_reclaim_object_budget;
    try std.testing.expectEqual(
        TableBackupAttemptCleanupProgress.incomplete,
        try advanceUnpublishedTableBackupAttemptCleanupAtLocationWithCancellation(
            alloc,
            io,
            &location,
            "logical",
            "artifact",
            .native,
            .retire_if_owned,
            &request_budget,
            .none,
        ),
    );
    try std.testing.expectEqual(@as(usize, 0), request_budget);
    try std.testing.expect(try tableBackupAttemptMatchesAtLocation(
        alloc,
        io,
        &location,
        "logical",
        "artifact",
    ));

    var passes: usize = 0;
    while (passes < 8) : (passes += 1) {
        var maintenance_budget: usize = backup_attempt_reclaim_object_budget;
        const progress = try advanceUnpublishedTableBackupAttemptCleanupAtLocationWithCancellation(
            alloc,
            io,
            &location,
            "logical",
            "artifact",
            .native,
            .retire_if_owned,
            &maintenance_budget,
            .none,
        );
        if (progress == .complete) break;
    }
    try std.testing.expect(passes < 8);
    try std.testing.expect(!try tableBackupAttemptMatchesAtLocation(
        alloc,
        io,
        &location,
        "logical",
        "artifact",
    ));
}

test "standalone stale reclaim bounds foreground native artifact deletion" {
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
    const fence: TableBackupFence = .{
        .metadata_group_id = 3,
        .metadata_incarnation = "0123456789abcdef0123456789abcdef".*,
        .table_id = 7,
        .definition_digest = [_]u8{0x11} ** 32,
        .topology_range_count = 1,
        .topology_digest = [_]u8{0x22} ** 32,
        .writer_not_after_unix_ns = 2,
    };
    try reserveTableBackupAttemptAtLocation(alloc, io, &location, "logical", "artifact", .native, fence);
    try reserveTableBackupWriterLeaseAtLocation(alloc, io, &location, "artifact", 2);
    for (0..64) |i| {
        const suffix = try std.fmt.allocPrint(alloc, "artifact/groups/1/file-{d}", .{i});
        defer alloc.free(suffix);
        try location.remote.writeBytes(alloc, suffix, "partial", "application/octet-stream");
    }

    try std.testing.expectError(
        error.BackupCleanupBudgetExceeded,
        reclaimExpiredTableBackupAttemptAtLocation(
            alloc,
            io,
            &location,
            "logical",
            std.math.maxInt(u64),
        ),
    );
    // The foreground quantum made progress but left both its durable cursor
    // (the reservation) and later payload objects for a subsequent retry.
    const retained_artifact = (try tableBackupAttemptArtifactIdAlloc(alloc, io, &location, "logical")).?;
    defer alloc.free(retained_artifact);
    const tail = try location.remote.readBytesAllocLimited(
        alloc,
        "artifact/groups/1/file-63",
        16,
    );
    alloc.free(tail);

    var passes: usize = 0;
    while (passes < 8) : (passes += 1) {
        const result = reclaimExpiredTableBackupAttemptAtLocation(
            alloc,
            io,
            &location,
            "logical",
            std.math.maxInt(u64),
        ) catch |err| switch (err) {
            error.BackupCleanupBudgetExceeded => continue,
            else => return err,
        };
        try std.testing.expectEqual(TableBackupAttemptReclaimResult.reclaimed, result);
        break;
    }
    try std.testing.expect(passes < 8);
}

test "standalone table backup stale reclamation preserves committed manifests and legacy missing leases" {
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
    const fence: TableBackupFence = .{
        .metadata_group_id = 3,
        .metadata_incarnation = "0123456789abcdef0123456789abcdef".*,
        .table_id = 7,
        .definition_digest = [_]u8{0x11} ** 32,
        .topology_range_count = 1,
        .topology_digest = [_]u8{0x22} ** 32,
    };
    try reserveTableBackupAttemptAtLocation(alloc, io, &location, "committed", "committed-artifact", .portable, fence);
    try reserveTableBackupWriterLeaseAtLocation(alloc, io, &location, "committed-artifact", 1);
    try location.remote.writeBytes(alloc, "committed-metadata.json", "{}", "application/json");
    try std.testing.expectEqual(
        TableBackupAttemptReclaimResult.committed,
        try reclaimExpiredTableBackupAttemptAtLocation(alloc, io, &location, "committed", std.math.maxInt(u64)),
    );
    const committed_artifact = (try tableBackupAttemptArtifactIdAlloc(alloc, io, &location, "committed")).?;
    defer alloc.free(committed_artifact);
    try std.testing.expectEqualStrings("committed-artifact", committed_artifact);

    try location.remote.writeBytes(
        alloc,
        "legacy-reservation",
        "{\"format_version\":1,\"backup_id\":\"legacy\",\"artifact_backup_id\":\"legacy-artifact\",\"format\":\"portable\"}",
        "application/json",
    );
    try std.testing.expectEqual(
        TableBackupAttemptReclaimResult.active,
        try reclaimExpiredTableBackupAttemptAtLocation(alloc, io, &location, "legacy", std.math.maxInt(u64)),
    );
}

test "committed table reconciliation retires writer state but preserves forwarded envelopes" {
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
    const fence: TableBackupFence = .{
        .metadata_group_id = 3,
        .metadata_incarnation = "0123456789abcdef0123456789abcdef".*,
        .table_id = 7,
        .definition_digest = [_]u8{0x11} ** 32,
        .topology_range_count = 1,
        .topology_digest = [_]u8{0x22} ** 32,
        .writer_not_after_unix_ns = 2,
    };
    try reserveTableBackupAttemptAtLocation(alloc, io, &location, "logical", "artifact", .portable, fence);
    try reserveTableBackupWriterLeaseAtLocation(alloc, io, &location, "artifact", std.math.maxInt(u64));
    try location.remote.writeBytes(alloc, "logical-metadata.json", "{}", "application/json");
    try std.testing.expect(try reconcileCommittedTableBackupWriterStateAtLocation(
        alloc,
        io,
        &location,
        "logical",
    ));
    const logical_lease = try tableBackupWriterLeasePath(alloc, "", "artifact");
    defer alloc.free(logical_lease);
    try std.testing.expectError(
        error.FileNotFound,
        location.remote.readBytesAllocLimited(
            alloc,
            trimLeftSlash(logical_lease),
            max_backup_attempt_lease_bytes,
        ),
    );

    try reserveTableBackupAttemptAtLocation(alloc, io, &location, "forwarded", "forwarded", .portable, fence);
    try reserveTableBackupWriterLeaseAtLocation(alloc, io, &location, "forwarded", std.math.maxInt(u64));
    try location.remote.writeBytes(alloc, "forwarded-metadata.json", "{}", "application/json");
    try std.testing.expect(!try reconcileCommittedTableBackupWriterStateAtLocation(
        alloc,
        io,
        &location,
        "forwarded",
    ));
    const forwarded_lease = try tableBackupWriterLeasePath(alloc, "", "forwarded");
    defer alloc.free(forwarded_lease);
    const retained = try location.remote.readBytesAllocLimited(
        alloc,
        trimLeftSlash(forwarded_lease),
        max_backup_attempt_lease_bytes,
    );
    alloc.free(retained);
}

test "committed cluster writer reconciliation charges actual remote operations" {
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
    const tables = [_]ClusterBackupAttemptTable{.{
        .name = "docs",
        .table_backup_id = "logical",
        .artifact_backup_id = "artifact",
    }};
    const marker: ClusterBackupAttemptMarker = .{
        .attempt_id = "budgeted-commit",
        .cluster_backup_id = "budgeted-cluster",
        .created_at_unix_ns = 1,
        .format = .portable,
        .writer_lease_fencing = true,
        .tables = &tables,
    };
    try reserveTableBackupWriterLeaseAtLocation(
        alloc,
        io,
        &location,
        tables[0].artifact_backup_id,
        std.math.maxInt(u64),
    );
    memory.resetOperationCount();
    var operation_budget: usize = 5;
    try std.testing.expect(try releaseCommittedClusterTableWriterStates(
        alloc,
        io,
        &location,
        &marker,
        &operation_budget,
    ));
    // Cursor GET, lease GET+DELETE, and cursor GET+PUT are the exact five
    // backend operations charged above.
    try std.testing.expectEqual(@as(u64, 5), memory.operationCount());
    try std.testing.expectEqual(@as(usize, 0), operation_budget);

    memory.resetOperationCount();
    var completed_budget: usize = 11;
    try std.testing.expect(try releaseCommittedClusterTableWriterStates(
        alloc,
        io,
        &location,
        &marker,
        &completed_budget,
    ));
    try std.testing.expectEqual(@as(u64, 1), memory.operationCount());
    try std.testing.expectEqual(@as(usize, 10), completed_budget);
}

test "cluster writer lease reclamation persists bounded scan progress" {
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
        .{ .name = "a", .table_backup_id = "table-a", .artifact_backup_id = "artifact-a" },
        .{ .name = "b", .table_backup_id = "table-b", .artifact_backup_id = "artifact-b" },
        .{ .name = "c", .table_backup_id = "table-c", .artifact_backup_id = "artifact-c" },
    };
    const marker: ClusterBackupAttemptMarker = .{
        .attempt_id = "attempt",
        .cluster_backup_id = "cluster",
        .created_at_unix_ns = 1,
        .format = .portable,
        .writer_lease_fencing = true,
        .tables = &tables,
    };
    for (tables) |table|
        try reserveTableBackupWriterLeaseAtLocation(alloc, io, &location, table.artifact_backup_id, 1);

    var first_budget: usize = 2;
    try std.testing.expect(!try claimExpiredClusterTableWriterLeases(
        alloc,
        io,
        &location,
        &marker,
        std.math.maxInt(u64),
        &first_budget,
    ));
    try std.testing.expectEqual(@as(usize, 1), (try readClusterWriterLeaseScanCursor(alloc, io, &location, &marker)).next_table_index);
    var second_budget: usize = 2;
    try std.testing.expect(!try claimExpiredClusterTableWriterLeases(
        alloc,
        io,
        &location,
        &marker,
        std.math.maxInt(u64),
        &second_budget,
    ));
    try std.testing.expectEqual(@as(usize, 2), (try readClusterWriterLeaseScanCursor(alloc, io, &location, &marker)).next_table_index);
    try writeClusterWriterLeaseScanCursorMonotonic(alloc, io, &location, &marker, .{
        .phase = .writer_leases,
        .next_table_index = 1,
    });
    try std.testing.expectEqual(@as(usize, 2), (try readClusterWriterLeaseScanCursor(alloc, io, &location, &marker)).next_table_index);
    var final_budget: usize = 2;
    try std.testing.expect(try claimExpiredClusterTableWriterLeases(
        alloc,
        io,
        &location,
        &marker,
        std.math.maxInt(u64),
        &final_budget,
    ));
    const completed_progress = try readClusterWriterLeaseScanCursor(alloc, io, &location, &marker);
    try std.testing.expectEqual(ClusterAttemptCleanupPhase.artifacts, completed_progress.phase);
    try std.testing.expectEqual(@as(usize, 0), completed_progress.next_table_index);
    try writeClusterWriterLeaseScanCursorMonotonic(alloc, io, &location, &marker, .{
        .phase = .writer_leases,
        .next_table_index = tables.len,
    });
    try std.testing.expectEqual(
        ClusterAttemptCleanupPhase.artifacts,
        (try readClusterWriterLeaseScanCursor(alloc, io, &location, &marker)).phase,
    );

    const cursor_suffix = try clusterWriterLeaseScanCursorPath(alloc, "", marker.attempt_id);
    defer alloc.free(cursor_suffix);
    try location.remote.writeBytes(alloc, trimLeftSlash(cursor_suffix), "not-json", "application/json");
    const reset_progress = try readClusterWriterLeaseScanCursor(alloc, io, &location, &marker);
    try std.testing.expectEqual(ClusterAttemptCleanupProgress{}, reset_progress);
}

test "legacy cluster attempts without writer fencing are retained" {
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
    const tables = [_]ClusterBackupAttemptTable{.{
        .name = "docs",
        .table_backup_id = "legacy-table",
        .artifact_backup_id = "legacy-artifact",
    }};
    const marker: ClusterBackupAttemptMarker = .{
        .attempt_id = "legacy-attempt",
        .cluster_backup_id = "legacy-cluster",
        .created_at_unix_ns = 1,
        .format = .portable,
        .tables = &tables,
    };
    var budget: usize = 8;
    try std.testing.expect(!try claimExpiredClusterTableWriterLeases(
        alloc,
        io,
        &location,
        &marker,
        std.math.maxInt(u64),
        &budget,
    ));
    try reserveTableBackupWriterLeaseAtLocation(
        alloc,
        io,
        &location,
        tables[0].artifact_backup_id,
        std.math.maxInt(u64),
    );
}

test "remote cluster artifact cleanup advances within a strict operation budget" {
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
        .{ .name = "a", .table_backup_id = "table-a", .artifact_backup_id = "artifact-a" },
        .{ .name = "b", .table_backup_id = "table-b", .artifact_backup_id = "artifact-b" },
        .{ .name = "c", .table_backup_id = "table-c", .artifact_backup_id = "artifact-c" },
    };
    const marker: ClusterBackupAttemptMarker = .{
        .attempt_id = "budget-attempt",
        .cluster_backup_id = "budget-cluster",
        .created_at_unix_ns = 1,
        .format = .portable,
        .writer_lease_fencing = true,
        .tables = &tables,
    };
    const fence: TableBackupFence = .{
        .metadata_group_id = 3,
        .metadata_incarnation = "0123456789abcdef0123456789abcdef".*,
        .table_id = 7,
        .definition_digest = [_]u8{0x11} ** 32,
        .topology_range_count = 1,
        .topology_digest = [_]u8{0x22} ** 32,
    };
    try writeClusterBackupAttemptMarker(alloc, io, &location, &marker);
    try reserveClusterBackupAttemptLeaseAtLocation(
        alloc,
        io,
        &location,
        marker.cluster_backup_id,
        marker.attempt_id,
        1,
    );
    try std.testing.expect(try claimExpiredClusterReservationForCleanup(
        alloc,
        io,
        &location,
        &marker,
        std.math.maxInt(u64),
    ));
    for (tables) |table| {
        try reserveTableBackupAttemptAtLocation(
            alloc,
            io,
            &location,
            table.table_backup_id,
            table.artifact_backup_id,
            .portable,
            fence,
        );
        const artifact_suffix = try std.fmt.allocPrint(alloc, "{s}.afb", .{table.artifact_backup_id});
        defer alloc.free(artifact_suffix);
        try location.remote.writeBytes(
            alloc,
            artifact_suffix,
            table.name,
            "application/vnd.antfly.backup",
        );
    }
    var fence_budget: usize = tables.len * 2;
    try std.testing.expect(try claimExpiredClusterTableWriterLeases(
        alloc,
        io,
        &location,
        &marker,
        std.math.maxInt(u64),
        &fence_budget,
    ));
    try std.testing.expectError(
        error.BackupAlreadyExists,
        reserveTableBackupWriterLeaseAtLocation(alloc, io, &location, tables[0].artifact_backup_id, 100),
    );

    for (1..tables.len + 1) |expected_index| {
        // Portable cleanup uses five exact object operations. Every quantum
        // completes exactly one table, and its immutable-generation tombstone
        // remains durable after the cursor advances.
        var operation_budget: usize = 5;
        try std.testing.expect(!try cleanupClusterBackupAttemptIncrementally(
            alloc,
            io,
            &location,
            &marker,
            &operation_budget,
        ));
        const progress = try readClusterWriterLeaseScanCursor(alloc, io, &location, &marker);
        try std.testing.expectEqual(ClusterAttemptCleanupPhase.artifacts, progress.phase);
        try std.testing.expectEqual(expected_index, progress.next_table_index);
        try std.testing.expectError(
            error.BackupAlreadyExists,
            reserveTableBackupWriterLeaseAtLocation(
                alloc,
                io,
                &location,
                tables[expected_index - 1].artifact_backup_id,
                100,
            ),
        );
    }
    var final_budget: usize = 10;
    try std.testing.expect(try cleanupClusterBackupAttemptIncrementally(
        alloc,
        io,
        &location,
        &marker,
        &final_budget,
    ));
    try std.testing.expectError(
        error.FileNotFound,
        readClusterBackupAttemptMarker(alloc, io, &location, marker.attempt_id),
    );
}

test "table backup cleanup removes the forwarded artifact envelope before payload" {
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
    const fence: TableBackupFence = .{
        .metadata_group_id = 3,
        .metadata_incarnation = "0123456789abcdef0123456789abcdef".*,
        .table_id = 7,
        .definition_digest = [_]u8{0x11} ** 32,
        .topology_range_count = 1,
        .topology_digest = [_]u8{0x22} ** 32,
    };
    try reserveTableBackupAttemptAtLocation(alloc, io, &location, "logical", "artifact", .portable, fence);
    try reserveTableBackupAttemptAtLocation(alloc, io, &location, "artifact", "artifact", .portable, fence);
    try location.remote.writeBytes(alloc, "artifact-metadata.json", "{}", "application/json");
    try location.remote.writeBytes(alloc, "artifact.afb", "payload", "application/vnd.antfly.backup");

    try cleanupTableBackupAttemptAtLocation(
        alloc,
        io,
        &location,
        "logical",
        "artifact",
        .portable,
    );

    try std.testing.expect((try tableBackupAttemptArtifactIdAlloc(alloc, io, &location, "logical")) == null);
    try std.testing.expect((try tableBackupAttemptArtifactIdAlloc(alloc, io, &location, "artifact")) == null);
    try std.testing.expectError(error.FileNotFound, location.remote.readBytesAllocLimited(alloc, "artifact-metadata.json", 64));
    try std.testing.expectError(error.FileNotFound, location.remote.readBytesAllocLimited(alloc, "artifact.afb", 64));
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

test "stale owned cluster backup attempt retains generation fences and retires authoritative head" {
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

    try reserveClusterBackupAttemptLeaseAtLocation(
        alloc,
        io,
        &location,
        "cluster-snap",
        "attempt-snap",
        2,
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
        .writer_lease_fencing = true,
        .tables = &tables,
    };
    try writeClusterBackupAttemptMarker(alloc, io, &location, &marker);
    try writeClusterBackupAttemptHead(alloc, io, &location, marker.attempt_id);

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
        error.FileNotFound,
        readClusterBackupAttemptMarker(alloc, io, &location, "attempt-snap"),
    );
    var failed_head = (try readClusterBackupAttemptHead(alloc, io, &location)) orelse
        return error.TestUnexpectedResult;
    defer failed_head.deinit();
    try std.testing.expectEqual(ClusterBackupAttemptState.failed, failed_head.value.state);
    try std.testing.expectEqualStrings(marker.attempt_id, failed_head.value.attempt_id);
    try std.testing.expectError(
        error.IncompleteClusterBackup,
        ensureNewestClusterBackupAttemptRestorable(alloc, io, &location),
    );
    try reserveClusterBackupAttemptLeaseAtLocation(
        alloc,
        io,
        &location,
        "cluster-snap",
        "attempt-retry",
        std.math.maxInt(u64),
    );
    try reserveBackupAtLocation(alloc, io, &location, "table-snap", false);
    try std.testing.expectError(
        error.BackupAlreadyExists,
        reserveTableBackupWriterLeaseAtLocation(
            alloc,
            io,
            &location,
            "artifact-snap",
            std.math.maxInt(u64),
        ),
    );
}

test "deadline-fenced cluster cleanup retires table writer tombstones" {
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
    const now_unix_ns = backup_attempt_reclaim_age_ns + 1;
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
        .writer_lease_fencing = true,
        .writer_delivery_deadline_fencing = true,
        .tables = &tables,
    };
    try writeClusterBackupAttemptMarker(alloc, io, &location, &marker);
    try writeClusterBackupAttemptHead(alloc, io, &location, marker.attempt_id);
    try reserveClusterBackupAttemptLeaseAtLocation(
        alloc,
        io,
        &location,
        marker.cluster_backup_id,
        marker.attempt_id,
        1,
    );
    try reserveBackupAtLocation(alloc, io, &location, tables[0].table_backup_id, false);
    try reserveTableBackupWriterLeaseAtLocation(alloc, io, &location, tables[0].artifact_backup_id, 1);
    try location.remote.writeBytes(
        alloc,
        "artifact-snap.afb",
        "partial",
        "application/vnd.antfly.backup",
    );

    try std.testing.expectEqual(
        @as(usize, 1),
        try reclaimStaleClusterBackupAttempts(alloc, io, &location, now_unix_ns),
    );
    const lease_suffix = try tableBackupWriterLeasePath(alloc, "", tables[0].artifact_backup_id);
    defer alloc.free(lease_suffix);
    try std.testing.expectError(
        error.FileNotFound,
        location.remote.readBytesAllocLimited(
            alloc,
            trimLeftSlash(lease_suffix),
            max_backup_attempt_lease_bytes,
        ),
    );
}

test "expired recovery preserves an oversized remote commit record" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    var memory = object_storage.MemoryObjectStorage.init(alloc);
    defer memory.deinit();
    var location: BackupLocation = .{
        .remote = try RemoteBackupStore.initWithClient(
            alloc,
            memory.client(),
            "bucket",
            "backups",
        ),
    };
    defer location.deinit(alloc);

    try reserveClusterBackupAttemptLeaseAtLocation(
        alloc,
        io,
        &location,
        "cluster-snap",
        "attempt-snap",
        1,
    );
    const manifest_suffix = try clusterMetadataPath(alloc, "", "cluster-snap");
    defer alloc.free(manifest_suffix);
    const oversized = try alloc.alloc(u8, max_backup_manifest_bytes + 1);
    defer alloc.free(oversized);
    @memset(oversized, 'x');
    try location.remote.writeBytes(
        alloc,
        trimLeftSlash(manifest_suffix),
        oversized,
        "application/json",
    );

    try std.testing.expect(!try reclaimExpiredClusterBackupAttemptAtLocation(
        alloc,
        io,
        &location,
        "cluster-snap",
        std.math.maxInt(u64),
    ));
    try std.testing.expectEqual(
        @as(?bool, true),
        try clusterReservationOwnerMatchesAtLocation(
            alloc,
            io,
            &location,
            "cluster-snap",
            "attempt-snap",
        ),
    );
}

test "cluster backup reservation heartbeat fences premature and stale recovery" {
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
        .writer_lease_fencing = true,
        .tables = &tables,
    };
    try writeClusterBackupAttemptMarker(alloc, io, &location, &marker);
    try reserveClusterBackupAttemptLeaseAtLocation(
        alloc,
        io,
        &location,
        marker.cluster_backup_id,
        marker.attempt_id,
        100,
    );

    try std.testing.expect(!try reclaimExpiredClusterBackupAttemptAtLocation(
        alloc,
        io,
        &location,
        marker.cluster_backup_id,
        99,
    ));
    try std.testing.expect(try renewClusterBackupAttemptLeaseAtLocation(
        alloc,
        io,
        &location,
        marker.cluster_backup_id,
        marker.attempt_id,
        200,
    ));
    try std.testing.expect(!try reclaimExpiredClusterBackupAttemptAtLocation(
        alloc,
        io,
        &location,
        marker.cluster_backup_id,
        150,
    ));
    try std.testing.expect(!try reclaimExpiredClusterBackupAttemptAtLocation(
        alloc,
        io,
        &location,
        marker.cluster_backup_id,
        200,
    ));
    try std.testing.expect(!try reclaimExpiredClusterBackupAttemptAtLocation(
        alloc,
        io,
        &location,
        marker.cluster_backup_id,
        200 + backup_attempt_lease_clock_skew_allowance_ns - 1,
    ));
    try std.testing.expect(try reclaimExpiredClusterBackupAttemptAtLocation(
        alloc,
        io,
        &location,
        marker.cluster_backup_id,
        200 + backup_attempt_lease_clock_skew_allowance_ns,
    ));
    try std.testing.expect(!(try renewClusterBackupAttemptLeaseAtLocation(
        alloc,
        io,
        &location,
        marker.cluster_backup_id,
        marker.attempt_id,
        300,
    )));
    try std.testing.expectError(
        error.FileNotFound,
        readClusterBackupAttemptMarker(alloc, io, &location, marker.attempt_id),
    );
}

test "filesystem reservation publication shares the cleanup claim lock" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(
        alloc,
        ".zig-cache/tmp/{s}/reservation-publication-lock",
        .{tmp.sub_path},
    );
    defer alloc.free(root);
    var location: BackupLocation = .{ .file = root };
    var backup_dir = try openOrCreateBackupRootNoFollow(io, root);
    defer backup_dir.close(io);
    const reservation_path = try reservationPath(alloc, "", "cluster-snap", true);
    defer alloc.free(reservation_path);
    const relative_path = trimLeftSlash(reservation_path);
    const lock_path = try std.fmt.allocPrint(alloc, "{s}.publish.lock", .{relative_path});
    defer alloc.free(lock_path);
    var lock_file = try openOrCreateBackupLockFile(io, backup_dir, lock_path);
    defer lock_file.close(io);
    try lock_file.lock(io, .exclusive);
    var lock_held = true;
    defer if (lock_held) lock_file.unlock(io);

    const Race = struct {
        io: std.Io,
        location: *BackupLocation,
        started: std.Io.Event = .unset,
        done: std.Io.Event = .unset,
        failure: ?anyerror = null,

        fn publish(self: *@This()) void {
            self.started.set(self.io);
            reserveClusterBackupAttemptLeaseAtLocation(
                std.heap.page_allocator,
                self.io,
                self.location,
                "cluster-snap",
                "attempt-snap",
                std.math.maxInt(u64),
            ) catch |err| {
                self.failure = err;
            };
            self.done.set(self.io);
        }
    };
    var race: Race = .{ .io = io, .location = &location };
    var publisher = try io.concurrent(Race.publish, .{&race});
    var publisher_pending = true;
    defer if (publisher_pending) {
        if (lock_held) {
            lock_file.unlock(io);
            lock_held = false;
        }
        _ = publisher.await(io);
    };
    race.started.waitUncancelable(io);
    var publication_blocked = false;
    race.done.waitTimeout(io, .{ .duration = .{
        .raw = std.Io.Duration.fromMilliseconds(10),
        .clock = .awake,
    } }) catch |err| switch (err) {
        error.Timeout => publication_blocked = true,
        error.Canceled => return err,
    };
    try std.testing.expect(publication_blocked);
    lock_file.unlock(io);
    lock_held = false;
    race.done.waitUncancelable(io);
    _ = publisher.await(io);
    publisher_pending = false;
    if (race.failure) |err| return err;
    try std.testing.expect((try clusterReservationOwnerMatchesAtLocation(
        alloc,
        io,
        &location,
        "cluster-snap",
        "attempt-snap",
    )) == true);
}

test "filesystem publication lock wait observes cancellation promptly" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(
        alloc,
        ".zig-cache/tmp/{s}/cancellable-publication-lock",
        .{tmp.sub_path},
    );
    defer alloc.free(root);
    var backup_dir = try openOrCreateBackupRootNoFollow(io, root);
    defer backup_dir.close(io);
    const relative_path = "control/reservation.json";
    const lock_path = try std.fmt.allocPrint(alloc, "{s}.publish.lock", .{relative_path});
    defer alloc.free(lock_path);
    try ensureBackupRelativeParentNoFollow(io, backup_dir, lock_path);
    var held_lock = try openOrCreateBackupLockFile(io, backup_dir, lock_path);
    defer held_lock.close(io);
    try held_lock.lock(io, .exclusive);
    defer held_lock.unlock(io);

    const State = struct {
        checks: usize = 0,

        fn isCancelled(raw: *const anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(@constCast(raw)));
            self.checks += 1;
            return self.checks >= 4;
        }
    };
    var state = State{};
    const started_at = std.Io.Timestamp.now(io, .awake);
    try std.testing.expectError(
        error.Canceled,
        writeFileToBackupRootIfAbsentLockedWithCancellation(
            alloc,
            io,
            backup_dir,
            relative_path,
            "reserved\n",
            .{ .ptr = &state, .is_cancelled_fn = State.isCancelled },
        ),
    );
    const elapsed_ns = std.Io.Timestamp.durationTo(
        started_at,
        std.Io.Timestamp.now(io, .awake),
    ).toNanoseconds();
    try std.testing.expect(elapsed_ns < 200 * std.time.ns_per_ms);
    try std.testing.expect(!try fileExistsFromBackupRoot(io, backup_dir, relative_path));
}

test "backup root publication cancellation leaves no visible control record" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(
        alloc,
        ".zig-cache/tmp/{s}/canceled-control-publication",
        .{tmp.sub_path},
    );
    defer alloc.free(root);
    var backup_dir = try openOrCreateBackupRootNoFollow(io, root);
    defer backup_dir.close(io);
    const relative_path = "control/attempt.json";
    try ensureBackupRelativeParentNoFollow(io, backup_dir, relative_path);

    const State = struct {
        checks: usize = 0,

        fn isCancelled(raw: *const anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(@constCast(raw)));
            self.checks += 1;
            return self.checks >= 3;
        }
    };
    var state = State{};
    try std.testing.expectError(
        error.Canceled,
        writeFileToBackupRootIfAbsentWithCancellation(
            io,
            backup_dir,
            relative_path,
            "control record",
            .{ .ptr = &state, .is_cancelled_fn = State.isCancelled },
        ),
    );
    try std.testing.expect(state.checks >= 3);
    try std.testing.expect(!try fileExistsFromBackupRoot(io, backup_dir, relative_path));
}

test "filesystem cluster backup lease supports the maximum owner identity" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(
        alloc,
        ".zig-cache/tmp/{s}/lease-bound",
        .{tmp.sub_path},
    );
    defer alloc.free(root);
    var location: BackupLocation = .{ .file = root };
    const attempt_id = [_]u8{'a'} ** 128;

    try reserveClusterBackupAttemptLeaseAtLocation(
        alloc,
        io,
        &location,
        "cluster-snap",
        &attempt_id,
        std.math.maxInt(u64),
    );
    try std.testing.expect(try renewClusterBackupAttemptLeaseAtLocation(
        alloc,
        io,
        &location,
        "cluster-snap",
        &attempt_id,
        std.math.maxInt(u64) - 1,
    ));
    try std.testing.expect(try cleanupClusterReservationIfOwnedAtLocation(
        alloc,
        io,
        &location,
        "cluster-snap",
        &attempt_id,
    ));
}

test "cluster cleanup lease supports the maximum public attempt identity" {
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
    const attempt_id = [_]u8{'a'} ** 128;
    const tables = [_]ClusterBackupAttemptTable{.{
        .name = "docs",
        .table_backup_id = "table-snap",
        .artifact_backup_id = "artifact-snap",
    }};
    const marker: ClusterBackupAttemptMarker = .{
        .attempt_id = &attempt_id,
        .cluster_backup_id = "cluster-snap",
        .created_at_unix_ns = 1,
        .format = .portable,
        .writer_lease_fencing = true,
        .tables = &tables,
    };
    try reserveClusterBackupAttemptLeaseAtLocation(
        alloc,
        io,
        &location,
        marker.cluster_backup_id,
        marker.attempt_id,
        1,
    );
    try std.testing.expect(try claimExpiredClusterReservationForCleanup(
        alloc,
        io,
        &location,
        &marker,
        std.math.maxInt(u64),
    ));
    const observed = (try clusterReservationAttemptIdAtLocation(
        alloc,
        io,
        &location,
        marker.cluster_backup_id,
    )).?;
    defer alloc.free(observed);
    try std.testing.expectEqualStrings(marker.attempt_id, observed);
}

test "legacy cleanup prefix remains a valid public attempt identity" {
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
    const tables = [_]ClusterBackupAttemptTable{.{
        .name = "docs",
        .table_backup_id = "table-snap",
        .artifact_backup_id = "artifact-snap",
    }};
    const marker: ClusterBackupAttemptMarker = .{
        .attempt_id = legacy_backup_cleanup_lease_owner_prefix,
        .cluster_backup_id = "cluster-snap",
        .created_at_unix_ns = 1,
        .format = .portable,
        .writer_lease_fencing = true,
        .tables = &tables,
    };
    try writeClusterBackupAttemptMarker(alloc, io, &location, &marker);
    try reserveClusterBackupAttemptLeaseAtLocation(
        alloc,
        io,
        &location,
        marker.cluster_backup_id,
        marker.attempt_id,
        std.math.maxInt(u64),
    );
    const observed = (try clusterReservationAttemptIdAtLocation(
        alloc,
        io,
        &location,
        marker.cluster_backup_id,
    )).?;
    defer alloc.free(observed);
    try std.testing.expectEqualStrings(marker.attempt_id, observed);
}

test "filesystem artifact cleanup stops at its operation budget and resumes" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/bounded-cleanup", .{tmp.sub_path});
    defer alloc.free(root);
    const nested_root = try std.fmt.allocPrint(alloc, "{s}/artifact/groups/1", .{root});
    defer alloc.free(nested_root);
    try ensureDirPathWithIo(io, nested_root);
    for (0..5) |i| {
        const path = try std.fmt.allocPrint(alloc, "{s}/file-{d}", .{ nested_root, i });
        defer alloc.free(path);
        try writeFileAbsolute(path, "payload");
    }
    const absolute_root = try std.Io.Dir.cwd().realPathFileAlloc(io, root, alloc);
    defer alloc.free(absolute_root);
    var location: BackupLocation = .{ .file = absolute_root };
    var passes: usize = 0;
    while (passes < 8) : (passes += 1) {
        var budget: usize = 2;
        cleanupTableBackupAttemptAtLocationWithBudget(
            alloc,
            io,
            &location,
            "logical",
            "artifact",
            .native,
            &budget,
            false,
            false,
        ) catch |err| switch (err) {
            error.BackupCleanupBudgetExceeded => continue,
            else => return err,
        };
        break;
    }
    try std.testing.expect(passes > 0);
    try std.testing.expect(passes < 8);
    const artifact_path = try std.fmt.allocPrint(alloc, "{s}/artifact", .{absolute_root});
    defer alloc.free(artifact_path);
    try std.testing.expect(!try pathExistsWithIo(io, artifact_path));
}

test "filesystem artifact cleanup unlinks symlinks without traversing their targets" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/symlink-cleanup", .{tmp.sub_path});
    defer alloc.free(root);
    const outside = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/outside", .{tmp.sub_path});
    defer alloc.free(outside);
    try ensureDirPathWithIo(io, root);
    try ensureDirPathWithIo(io, outside);
    const sentinel = try std.fmt.allocPrint(alloc, "{s}/sentinel", .{outside});
    defer alloc.free(sentinel);
    try writeFileAbsolute(sentinel, "preserve");
    const artifact_path = try std.fmt.allocPrint(alloc, "{s}/artifact", .{root});
    defer alloc.free(artifact_path);
    try std.Io.Dir.cwd().symLink(io, "../outside", artifact_path, .{ .is_directory = true });

    var location: BackupLocation = .{ .file = root };
    var budget: usize = 8;
    try cleanupTableBackupAttemptAtLocationWithBudget(
        alloc,
        io,
        &location,
        "logical",
        "artifact",
        .native,
        &budget,
        false,
        false,
    );
    try std.testing.expect(!try pathExistsWithIo(io, artifact_path));
    const preserved = try readFileAbsoluteAllocWithIo(alloc, io, sentinel, 16);
    defer alloc.free(preserved);
    try std.testing.expectEqualStrings("preserve", preserved);
}

test "filesystem artifact cleanup refuses a symlinked repository ancestor" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const parent = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/ancestor-cleanup", .{tmp.sub_path});
    defer alloc.free(parent);
    const outside = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/outside-root", .{tmp.sub_path});
    defer alloc.free(outside);
    try ensureDirPathWithIo(io, parent);
    const actual_root = try std.fmt.allocPrint(alloc, "{s}/repository", .{outside});
    defer alloc.free(actual_root);
    const outside_artifact = try std.fmt.allocPrint(alloc, "{s}/artifact", .{actual_root});
    defer alloc.free(outside_artifact);
    try ensureDirPathWithIo(io, outside_artifact);
    const sentinel = try std.fmt.allocPrint(alloc, "{s}/sentinel", .{outside_artifact});
    defer alloc.free(sentinel);
    try writeFileAbsolute(sentinel, "preserve");
    const redirected_ancestor = try std.fmt.allocPrint(alloc, "{s}/redirect", .{parent});
    defer alloc.free(redirected_ancestor);
    try std.Io.Dir.cwd().symLink(io, "../outside-root", redirected_ancestor, .{ .is_directory = true });
    const configured_root = try std.fmt.allocPrint(alloc, "{s}/redirect/repository", .{parent});
    defer alloc.free(configured_root);

    var location: BackupLocation = .{ .file = configured_root };
    var budget: usize = 8;
    var rejected = false;
    cleanupTableBackupAttemptAtLocationWithBudget(
        alloc,
        io,
        &location,
        "logical",
        "artifact",
        .native,
        &budget,
        false,
        false,
    ) catch |err| switch (err) {
        error.SymLinkLoop, error.NotDir => rejected = true,
        else => return err,
    };
    try std.testing.expect(rejected);
    const preserved = try readFileAbsoluteAllocWithIo(alloc, io, sentinel, 16);
    defer alloc.free(preserved);
    try std.testing.expectEqualStrings("preserve", preserved);
}

test "filesystem control cleanup refuses a symlinked repository ancestor" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const parent = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/control-ancestor", .{tmp.sub_path});
    defer alloc.free(parent);
    const outside = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/control-outside", .{tmp.sub_path});
    defer alloc.free(outside);
    const actual_root = try std.fmt.allocPrint(alloc, "{s}/repository", .{outside});
    defer alloc.free(actual_root);
    try ensureDirPathWithIo(io, actual_root);
    try ensureDirPathWithIo(io, parent);
    const redirected_ancestor = try std.fmt.allocPrint(alloc, "{s}/redirect", .{parent});
    defer alloc.free(redirected_ancestor);
    try std.Io.Dir.cwd().symLink(io, "../control-outside", redirected_ancestor, .{ .is_directory = true });
    const configured_root = try std.fmt.allocPrint(alloc, "{s}/redirect/repository", .{parent});
    defer alloc.free(configured_root);
    var actual_location: BackupLocation = .{ .file = actual_root };
    var redirected_location: BackupLocation = .{ .file = configured_root };

    var cluster_reserve_rejected = false;
    reserveClusterBackupAttemptLeaseAtLocation(
        alloc,
        io,
        &redirected_location,
        "redirected-cluster",
        "redirected-attempt",
        std.math.maxInt(u64),
    ) catch |err| switch (err) {
        error.SymLinkLoop, error.NotDir => cluster_reserve_rejected = true,
        else => return err,
    };
    try std.testing.expect(cluster_reserve_rejected);
    try reserveClusterBackupAttemptLeaseAtLocation(
        alloc,
        io,
        &actual_location,
        "cluster-lease",
        "cluster-attempt",
        std.math.maxInt(u64),
    );
    var cluster_release_rejected = false;
    _ = cleanupClusterReservationIfOwnedAtLocation(
        alloc,
        io,
        &redirected_location,
        "cluster-lease",
        "cluster-attempt",
    ) catch |err| switch (err) {
        error.SymLinkLoop, error.NotDir => cluster_release_rejected = true,
        else => return err,
    };
    try std.testing.expect(cluster_release_rejected);
    try std.testing.expect((try clusterReservationOwnerMatchesAtLocation(
        alloc,
        io,
        &actual_location,
        "cluster-lease",
        "cluster-attempt",
    )) == true);
    var cluster_delete_rejected = false;
    cleanupClusterReservationAtLocation(
        alloc,
        io,
        &redirected_location,
        "cluster-lease",
    ) catch |err| switch (err) {
        error.SymLinkLoop, error.NotDir => cluster_delete_rejected = true,
        else => return err,
    };
    try std.testing.expect(cluster_delete_rejected);
    try std.testing.expect((try clusterReservationOwnerMatchesAtLocation(
        alloc,
        io,
        &actual_location,
        "cluster-lease",
        "cluster-attempt",
    )) == true);

    var head_write_rejected = false;
    writeClusterBackupAttemptHead(
        alloc,
        io,
        &redirected_location,
        "redirected-head",
    ) catch |err| switch (err) {
        error.SymLinkLoop, error.NotDir => head_write_rejected = true,
        else => return err,
    };
    try std.testing.expect(head_write_rejected);
    try std.testing.expect((try readClusterBackupAttemptHead(alloc, io, &actual_location)) == null);

    var reserve_rejected = false;
    reserveTableBackupWriterLeaseAtLocation(
        alloc,
        io,
        &redirected_location,
        "redirected-generation",
        std.math.maxInt(u64),
    ) catch |err| switch (err) {
        error.SymLinkLoop, error.NotDir => reserve_rejected = true,
        else => return err,
    };
    try std.testing.expect(reserve_rejected);
    const redirected_lease_path = try tableBackupWriterLeasePath(
        alloc,
        actual_root,
        "redirected-generation",
    );
    defer alloc.free(redirected_lease_path);
    try std.testing.expect(!try pathExistsWithIo(io, redirected_lease_path));

    try reserveTableBackupWriterLeaseAtLocation(
        alloc,
        io,
        &actual_location,
        "lease-generation",
        std.math.maxInt(u64),
    );
    var release_rejected = false;
    _ = releaseTableBackupWriterLeaseAtLocation(
        alloc,
        io,
        &redirected_location,
        "lease-generation",
    ) catch |err| switch (err) {
        error.SymLinkLoop, error.NotDir => release_rejected = true,
        else => return err,
    };
    try std.testing.expect(release_rejected);
    const lease_path = try tableBackupWriterLeasePath(alloc, actual_root, "lease-generation");
    defer alloc.free(lease_path);
    try std.testing.expect(try pathExistsWithIo(io, lease_path));

    const envelope_manifest = try metadataPath(alloc, actual_root, "forwarded");
    defer alloc.free(envelope_manifest);
    const envelope_reservation = try reservationPath(alloc, actual_root, "forwarded", false);
    defer alloc.free(envelope_reservation);
    try writeFileAbsolute(envelope_manifest, "manifest");
    try writeFileAbsolute(envelope_reservation, "reservation");
    var envelope_rejected = false;
    retireForwardedTableBackupEnvelopeAtLocation(
        alloc,
        io,
        &redirected_location,
        "forwarded",
    ) catch |err| switch (err) {
        error.SymLinkLoop, error.NotDir => envelope_rejected = true,
        else => return err,
    };
    try std.testing.expect(envelope_rejected);
    try std.testing.expect(try pathExistsWithIo(io, envelope_manifest));
    try std.testing.expect(try pathExistsWithIo(io, envelope_reservation));

    const tables = [_]ClusterBackupAttemptTable{.{
        .name = "docs",
        .table_backup_id = "marker-table",
        .artifact_backup_id = "marker-artifact",
    }};
    const marker: ClusterBackupAttemptMarker = .{
        .attempt_id = "marker-attempt",
        .cluster_backup_id = "marker-cluster",
        .created_at_unix_ns = 1,
        .format = .portable,
        .writer_lease_fencing = true,
        .tables = &tables,
    };
    var marker_write_rejected = false;
    writeClusterBackupAttemptMarker(
        alloc,
        io,
        &redirected_location,
        &marker,
    ) catch |err| switch (err) {
        error.SymLinkLoop, error.NotDir => marker_write_rejected = true,
        else => return err,
    };
    try std.testing.expect(marker_write_rejected);
    const marker_path = try incompleteBackupMarkerPath(alloc, actual_root, marker.attempt_id);
    defer alloc.free(marker_path);
    try std.testing.expect(!try pathExistsWithIo(io, marker_path));

    try writeClusterBackupAttemptMarker(alloc, io, &actual_location, &marker);
    const quarantine_outside = try std.fmt.allocPrint(
        alloc,
        "{s}/quarantine-outside",
        .{outside},
    );
    defer alloc.free(quarantine_outside);
    try ensureDirPathWithIo(io, quarantine_outside);
    const quarantine_link = try std.fmt.allocPrint(
        alloc,
        "{s}/{s}",
        .{ actual_root, backup_attempt_quarantine_prefix },
    );
    defer alloc.free(quarantine_link);
    try std.Io.Dir.cwd().symLink(
        io,
        "../quarantine-outside",
        quarantine_link,
        .{ .is_directory = true },
    );
    var quarantine_budget = cluster_backup_attempt_quarantine_operation_cost;
    var quarantine_rejected = false;
    quarantineClusterBackupAttemptMarker(
        alloc,
        io,
        &actual_location,
        &marker,
        2,
        &quarantine_budget,
    ) catch |err| switch (err) {
        error.SymLinkLoop, error.NotDir => quarantine_rejected = true,
        else => return err,
    };
    try std.testing.expect(quarantine_rejected);
    try std.testing.expect(try pathExistsWithIo(io, marker_path));

    var marker_rejected = false;
    deleteClusterBackupAttemptMarker(
        alloc,
        io,
        &redirected_location,
        &marker,
    ) catch |err| switch (err) {
        error.SymLinkLoop, error.NotDir => marker_rejected = true,
        else => return err,
    };
    try std.testing.expect(marker_rejected);
    try std.testing.expect(try pathExistsWithIo(io, marker_path));
}

test "filesystem attempt maintenance refuses a symlinked reclaim shard" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(
        alloc,
        ".zig-cache/tmp/{s}/maintenance-root",
        .{tmp.sub_path},
    );
    defer alloc.free(root);
    const outside = try std.fmt.allocPrint(
        alloc,
        ".zig-cache/tmp/{s}/maintenance-outside",
        .{tmp.sub_path},
    );
    defer alloc.free(outside);
    const index_root = try std.fmt.allocPrint(
        alloc,
        "{s}/{s}",
        .{ root, backup_attempt_reclaim_index_name },
    );
    defer alloc.free(index_root);
    for ([_][]const u8{
        ".staging/queued/00",
        ".staging/claimed/00",
        "claimed/00",
        "queued",
        "publication-locks",
    }) |suffix| {
        const path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ index_root, suffix });
        defer alloc.free(path);
        try ensureDirPathWithIo(io, path);
    }
    try ensureDirPathWithIo(io, outside);
    const victim = try std.fmt.allocPrint(alloc, "{s}/victim!", .{outside});
    defer alloc.free(victim);
    try writeFileAbsolute(victim, "keep");
    const queue_shard = try std.fmt.allocPrint(alloc, "{s}/queued/00", .{index_root});
    defer alloc.free(queue_shard);
    try std.Io.Dir.cwd().symLink(
        io,
        "../../../maintenance-outside",
        queue_shard,
        .{ .is_directory = true },
    );

    var location: BackupLocation = .{ .file = root };
    var rejected = false;
    _ = reclaimStaleClusterBackupAttempts(alloc, io, &location, 1) catch |err| switch (err) {
        error.SymLinkLoop, error.NotDir => rejected = true,
        else => return err,
    };
    try std.testing.expect(rejected);
    const preserved = try readFileAbsoluteAllocWithIo(alloc, io, victim, 16);
    defer alloc.free(preserved);
    try std.testing.expectEqualStrings("keep", preserved);
}

test "filesystem reclaim removes invalid POSIX tickets containing backslashes" {
    if (std.fs.path.sep != '/') return error.SkipZigTest;
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(
        alloc,
        ".zig-cache/tmp/{s}/backslash-ticket",
        .{tmp.sub_path},
    );
    defer alloc.free(root);
    try ensureDirPathWithIo(io, root);
    var backup_dir = try openBackupRootNoFollow(io, root);
    defer backup_dir.close(io);
    const shard_path = try localBackupAttemptReclaimShardPath(
        alloc,
        "",
        .claimed,
        0,
    );
    defer alloc.free(shard_path);
    try ensureBackupRelativeDirectoryNoFollow(
        io,
        backup_dir,
        trimLeftSlash(shard_path),
    );
    var shard_dir = try openBackupRelativeDirNoFollow(
        io,
        backup_dir,
        trimLeftSlash(shard_path),
        false,
    );
    defer shard_dir.close(io);
    var ticket = try shard_dir.createFile(io, "bad\\ticket", .{});
    ticket.close(io);
    try fs_paths.syncDirectoryHandlePortable(io, shard_dir);

    try recoverExpiredLocalBackupAttemptReclaimClaims(
        alloc,
        io,
        backup_dir,
        0,
        std.math.maxInt(u64),
    );
    try std.testing.expectError(
        error.FileNotFound,
        shard_dir.statFile(io, "bad\\ticket", .{ .follow_symlinks = false }),
    );
}

test "local reclaim stays bound to its opened repository after a root swap" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(
        alloc,
        ".zig-cache/tmp/{s}/reclaim-root",
        .{tmp.sub_path},
    );
    defer alloc.free(root);
    const moved_root = try std.fmt.allocPrint(
        alloc,
        ".zig-cache/tmp/{s}/reclaim-root-opened",
        .{tmp.sub_path},
    );
    defer alloc.free(moved_root);
    var location: BackupLocation = .{ .file = root };
    const tables = [_]ClusterBackupAttemptTable{.{
        .name = "docs",
        .table_backup_id = "root-swap-table",
        .artifact_backup_id = "root-swap-artifact",
    }};
    const marker: ClusterBackupAttemptMarker = .{
        .attempt_id = "root-swap-attempt",
        .cluster_backup_id = "root-swap-cluster",
        .created_at_unix_ns = 1,
        .format = .portable,
        .writer_lease_fencing = true,
        .tables = &tables,
    };

    try writeClusterBackupAttemptHead(alloc, io, &location, marker.attempt_id);
    try writeClusterBackupAttemptMarker(alloc, io, &location, &marker);
    try reserveClusterBackupAttemptLeaseAtLocation(
        alloc,
        io,
        &location,
        marker.cluster_backup_id,
        marker.attempt_id,
        1,
    );
    try reserveTableBackupWriterLeaseAtLocation(
        alloc,
        io,
        &location,
        tables[0].artifact_backup_id,
        1,
    );
    const original_artifact = try std.fmt.allocPrint(
        alloc,
        "{s}/{s}.afb",
        .{ root, tables[0].artifact_backup_id },
    );
    defer alloc.free(original_artifact);
    try writeFileAbsolute(original_artifact, "opened repository payload");

    const Maintenance = struct {
        io: std.Io,
        location: *BackupLocation,
        hook: *BackupMaintenanceTestHook,
        reclaimed: usize = 0,
        failure: ?anyerror = null,

        fn run(self: *@This()) void {
            self.reclaimed = reclaimStaleClusterBackupAttemptsWithHook(
                std.heap.page_allocator,
                self.io,
                self.location,
                backup_attempt_reclaim_age_ns + 2,
                self.hook,
            ) catch |err| {
                self.failure = err;
                return;
            };
        }
    };
    var hook: BackupMaintenanceTestHook = .{ .pause_after_claims = true };
    var maintenance: Maintenance = .{
        .io = io,
        .location = &location,
        .hook = &hook,
    };
    var task = try io.concurrent(Maintenance.run, .{&maintenance});
    var task_pending = true;
    defer if (task_pending) {
        hook.claims_release.set(io);
        _ = task.await(io);
    };
    hook.claims_ready.waitUncancelable(io);

    try std.Io.Dir.rename(
        std.Io.Dir.cwd(),
        root,
        std.Io.Dir.cwd(),
        moved_root,
        io,
    );
    try writeClusterBackupAttemptHead(alloc, io, &location, marker.attempt_id);
    try writeClusterBackupAttemptMarker(alloc, io, &location, &marker);
    try reserveClusterBackupAttemptLeaseAtLocation(
        alloc,
        io,
        &location,
        marker.cluster_backup_id,
        marker.attempt_id,
        std.math.maxInt(u64),
    );
    try reserveTableBackupWriterLeaseAtLocation(
        alloc,
        io,
        &location,
        tables[0].artifact_backup_id,
        std.math.maxInt(u64),
    );
    const replacement_artifact = try std.fmt.allocPrint(
        alloc,
        "{s}/{s}.afb",
        .{ root, tables[0].artifact_backup_id },
    );
    defer alloc.free(replacement_artifact);
    try writeFileAbsolute(replacement_artifact, "replacement repository payload");

    hook.claims_release.set(io);
    _ = task.await(io);
    task_pending = false;
    if (maintenance.failure) |err| return err;
    try std.testing.expectEqual(@as(usize, 1), maintenance.reclaimed);

    const opened_marker = try incompleteBackupMarkerPath(
        alloc,
        moved_root,
        marker.attempt_id,
    );
    defer alloc.free(opened_marker);
    const replacement_marker = try incompleteBackupMarkerPath(
        alloc,
        root,
        marker.attempt_id,
    );
    defer alloc.free(replacement_marker);
    const moved_artifact = try std.fmt.allocPrint(
        alloc,
        "{s}/{s}.afb",
        .{ moved_root, tables[0].artifact_backup_id },
    );
    defer alloc.free(moved_artifact);
    try std.testing.expect(!(try pathExistsWithIo(io, opened_marker)));
    try std.testing.expect(!(try pathExistsWithIo(io, moved_artifact)));
    try std.testing.expect(try pathExistsWithIo(io, replacement_marker));
    const replacement_payload = try readFileAbsoluteAllocWithIo(
        alloc,
        io,
        replacement_artifact,
        64,
    );
    defer alloc.free(replacement_payload);
    try std.testing.expectEqualStrings(
        "replacement repository payload",
        replacement_payload,
    );
}

test "filesystem artifact cleanup rejects adversarial depth within a fixed traversal bound" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/deep-cleanup", .{tmp.sub_path});
    defer alloc.free(root);
    var deep_path = std.ArrayListUnmanaged(u8).empty;
    defer deep_path.deinit(alloc);
    try deep_path.appendSlice(alloc, root);
    try deep_path.appendSlice(alloc, "/artifact");
    for (0..300) |_| try deep_path.appendSlice(alloc, "/d");
    try ensureDirPathWithIo(io, deep_path.items);

    var location: BackupLocation = .{ .file = root };
    var budget: usize = backup_attempt_request_reclaim_object_budget;
    try std.testing.expectError(
        error.BackupCleanupTraversalLimitExceeded,
        cleanupTableBackupAttemptAtLocationWithBudget(
            alloc,
            io,
            &location,
            "logical",
            "artifact",
            .native,
            &budget,
            false,
            false,
        ),
    );
}

test "stale committed marker retires when the authoritative head is unchanged" {
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
        .writer_lease_fencing = true,
        .tables = &attempt_tables,
    };
    try writeClusterBackupAttemptMarker(alloc, io, &location, &marker);
    try writeClusterBackupAttemptHead(alloc, io, &location, marker.attempt_id);
    try std.testing.expect(try commitClusterBackupAttemptHeadIfOwned(alloc, io, &location, marker.attempt_id));
    try reserveClusterBackupAttemptLeaseAtLocation(
        alloc,
        io,
        &location,
        marker.cluster_backup_id,
        marker.attempt_id,
        1,
    );
    // Simulate stale reclamation claiming the writer just before publication
    // became visible. The artifacts cursor must not make committed-marker
    // reconciliation skip the retained cleanup tombstone.
    try reserveTableBackupWriterLeaseAtLocation(
        alloc,
        io,
        &location,
        attempt_tables[0].artifact_backup_id,
        1,
    );
    try std.testing.expectEqual(
        BackupLeaseFenceClaim.claimed,
        try claimExpiredTableBackupWriterLeaseAtLocation(
            alloc,
            io,
            &location,
            attempt_tables[0].artifact_backup_id,
            std.math.maxInt(u64),
        ),
    );
    try writeClusterWriterLeaseScanCursorMonotonic(alloc, io, &location, &marker, .{
        .phase = .artifacts,
        .next_table_index = 0,
    });
    const committed_tables = [_]ClusterTableBackupEntry{.{
        .name = "docs",
        .table_backup_id = "table-snap",
        .artifact_backup_id = "artifact-snap",
    }};
    var manifest = try createClusterManifest(
        alloc,
        marker.cluster_backup_id,
        "s3://bucket/backups",
        &committed_tables,
    );
    defer manifest.deinit(alloc);
    try writeClusterManifestToLocationWithIo(alloc, io, &location, &manifest);

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
        readClusterBackupAttemptMarker(alloc, io, &location, marker.attempt_id),
    );
    try std.testing.expect((try clusterReservationAttemptIdAtLocation(
        alloc,
        io,
        &location,
        marker.cluster_backup_id,
    )) == null);
    const writer_lease = try tableBackupWriterLeasePath(
        alloc,
        "",
        attempt_tables[0].artifact_backup_id,
    );
    defer alloc.free(writer_lease);
    try std.testing.expectError(
        error.FileNotFound,
        location.remote.readBytesAllocLimited(
            alloc,
            trimLeftSlash(writer_lease),
            max_backup_attempt_lease_bytes,
        ),
    );
}

test "committed cluster reconciliation quarantines a different marker generation" {
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
    const stale_tables = [_]ClusterBackupAttemptTable{.{
        .name = "docs",
        .table_backup_id = "stale-table",
        .artifact_backup_id = "stale-artifact",
    }};
    const stale_marker: ClusterBackupAttemptMarker = .{
        .attempt_id = "stale-generation",
        .cluster_backup_id = "reused-cluster-id",
        .created_at_unix_ns = 1,
        .format = .portable,
        .writer_lease_fencing = true,
        .tables = &stale_tables,
    };
    try writeClusterBackupAttemptMarker(alloc, io, &location, &stale_marker);
    try writeClusterBackupAttemptHead(alloc, io, &location, stale_marker.attempt_id);
    try std.testing.expect(try commitClusterBackupAttemptHeadIfOwned(
        alloc,
        io,
        &location,
        stale_marker.attempt_id,
    ));
    try reserveTableBackupWriterLeaseAtLocation(
        alloc,
        io,
        &location,
        stale_tables[0].artifact_backup_id,
        std.math.maxInt(u64),
    );
    const committed_tables = [_]ClusterTableBackupEntry{.{
        .name = "docs",
        .table_backup_id = "new-table",
        .artifact_backup_id = "new-artifact",
    }};
    var manifest = try createClusterManifest(
        alloc,
        stale_marker.cluster_backup_id,
        "s3://bucket/backups",
        &committed_tables,
    );
    defer manifest.deinit(alloc);
    try writeClusterManifestToLocationWithIo(alloc, io, &location, &manifest);

    const quarantine_path = try clusterBackupAttemptQuarantinePath(
        alloc,
        "",
        stale_marker.attempt_id,
    );
    defer alloc.free(quarantine_path);
    var undersized_budget =
        cluster_backup_attempt_head_transition_operation_cost +
        cluster_backup_attempt_quarantine_operation_cost - 1;
    try std.testing.expect(!try reclaimClusterBackupAttemptMarker(
        alloc,
        io,
        &location,
        &stale_marker,
        backup_attempt_reclaim_age_ns + 1,
        stale_marker.attempt_id,
        &undersized_budget,
    ));
    try std.testing.expectEqual(
        cluster_backup_attempt_head_transition_operation_cost +
            cluster_backup_attempt_quarantine_operation_cost - 1,
        undersized_budget,
    );
    var retained_marker = try readClusterBackupAttemptMarker(
        alloc,
        io,
        &location,
        stale_marker.attempt_id,
    );
    retained_marker.deinit();
    var retained_head = (try readClusterBackupAttemptHead(alloc, io, &location)).?;
    try std.testing.expectEqual(ClusterBackupAttemptState.committed, retained_head.value.state);
    retained_head.deinit();
    try std.testing.expectError(
        error.FileNotFound,
        location.remote.readBytesAllocLimited(
            alloc,
            trimLeftSlash(quarantine_path),
            max_backup_attempt_quarantine_bytes,
        ),
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
        readClusterBackupAttemptMarker(alloc, io, &location, stale_marker.attempt_id),
    );
    var failed_head = (try readClusterBackupAttemptHead(alloc, io, &location)).?;
    defer failed_head.deinit();
    try std.testing.expectEqual(ClusterBackupAttemptState.failed, failed_head.value.state);
    const quarantine = try location.remote.readBytesAllocLimited(
        alloc,
        trimLeftSlash(quarantine_path),
        max_backup_attempt_quarantine_bytes,
    );
    defer alloc.free(quarantine);
    const marker_json = try stringifyJsonAlloc(alloc, stale_marker);
    defer alloc.free(marker_json);
    var marker_digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(marker_json, &marker_digest, .{});
    const marker_sha256 = std.fmt.bytesToHex(marker_digest, .lower);
    try std.testing.expect(clusterBackupAttemptQuarantineMatches(
        alloc,
        quarantine,
        &stale_marker,
        marker_sha256[0..],
    ));
    // The quarantine suffix is outside the active `.json` namespace, so the
    // next maintenance pass does not reconsider the mismatch.
    try std.testing.expectEqual(
        @as(usize, 0),
        try reclaimStaleClusterBackupAttempts(
            alloc,
            io,
            &location,
            backup_attempt_reclaim_age_ns + 2,
        ),
    );
    const stale_lease = try tableBackupWriterLeasePath(
        alloc,
        "",
        stale_tables[0].artifact_backup_id,
    );
    defer alloc.free(stale_lease);
    const retained_lease = try location.remote.readBytesAllocLimited(
        alloc,
        trimLeftSlash(stale_lease),
        max_backup_attempt_lease_bytes,
    );
    alloc.free(retained_lease);
}

test "filesystem stale attempt reclamation index prevents directory-order starvation" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(
        alloc,
        ".zig-cache/tmp/{s}/reclaim-cursor",
        .{tmp.sub_path},
    );
    defer alloc.free(root);
    var location: BackupLocation = .{ .file = root };

    const now_unix_ns = backup_attempt_reclaim_age_ns + 1;
    for (0..backup_attempt_reclaim_scan_budget) |i| {
        const attempt_id = try std.fmt.allocPrint(alloc, "a-{d:0>3}", .{i});
        defer alloc.free(attempt_id);
        const cluster_id = try std.fmt.allocPrint(alloc, "cluster-{d:0>3}", .{i});
        defer alloc.free(cluster_id);
        const table_id = try std.fmt.allocPrint(alloc, "table-{d:0>3}", .{i});
        defer alloc.free(table_id);
        const artifact_id = try std.fmt.allocPrint(alloc, "artifact-{d:0>3}", .{i});
        defer alloc.free(artifact_id);
        const tables = [_]ClusterBackupAttemptTable{.{
            .name = "docs",
            .table_backup_id = table_id,
            .artifact_backup_id = artifact_id,
        }};
        const marker: ClusterBackupAttemptMarker = .{
            .attempt_id = attempt_id,
            .cluster_backup_id = cluster_id,
            .created_at_unix_ns = now_unix_ns,
            .format = .portable,
            .tables = &tables,
        };
        try writeClusterBackupAttemptMarker(alloc, io, &location, &marker);
    }
    const stale_tables = [_]ClusterBackupAttemptTable{.{
        .name = "docs",
        .table_backup_id = "z-stale-table",
        .artifact_backup_id = "z-stale-artifact",
    }};
    const stale_marker: ClusterBackupAttemptMarker = .{
        .attempt_id = "z-stale",
        .cluster_backup_id = "z-stale-cluster",
        .created_at_unix_ns = 1,
        .format = .portable,
        .writer_lease_fencing = true,
        .tables = &stale_tables,
    };
    try writeClusterBackupAttemptMarker(alloc, io, &location, &stale_marker);

    var reclaimed: usize = 0;
    for (0..backup_attempt_reclaim_shard_count) |_| {
        reclaimed += try reclaimStaleClusterBackupAttempts(
            alloc,
            io,
            &location,
            now_unix_ns,
        );
        if (reclaimed > 0) break;
    }
    try std.testing.expectEqual(@as(usize, 1), reclaimed);
    try std.testing.expectError(
        error.FileNotFound,
        readClusterBackupAttemptMarker(alloc, io, &location, stale_marker.attempt_id),
    );
}

test "filesystem completed attempt tickets are deleted instead of durably rotated" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(
        alloc,
        ".zig-cache/tmp/{s}/reclaim-completed",
        .{tmp.sub_path},
    );
    defer alloc.free(root);
    var location: BackupLocation = .{ .file = root };
    const tables = [_]ClusterBackupAttemptTable{.{
        .name = "docs",
        .table_backup_id = "completed-table",
        .artifact_backup_id = "completed-artifact",
    }};

    const direct_marker: ClusterBackupAttemptMarker = .{
        .attempt_id = "completed-direct",
        .cluster_backup_id = "completed-cluster-direct",
        .created_at_unix_ns = 1,
        .format = .portable,
        .tables = &tables,
    };
    try writeClusterBackupAttemptMarker(alloc, io, &location, &direct_marker);
    const direct_ticket = try localBackupAttemptReclaimTicketPath(
        alloc,
        root,
        .queued,
        localBackupAttemptReclaimShard(
            direct_marker.created_at_unix_ns +| backup_attempt_reclaim_age_ns,
        ),
        direct_marker.attempt_id,
    );
    defer alloc.free(direct_ticket);
    try std.testing.expect(try pathExistsWithIo(io, direct_ticket));
    try deleteClusterBackupAttemptMarker(
        alloc,
        io,
        &location,
        &direct_marker,
    );
    try std.testing.expect(!(try pathExistsWithIo(io, direct_ticket)));

    // Simulate maintenance racing a longer-running backup and moving its
    // future ticket before completion. Marker deletion may miss that moved
    // ticket, but the next selector pass must delete it in place rather than
    // rotate it again.
    const raced_marker: ClusterBackupAttemptMarker = .{
        .attempt_id = "completed-after-rotation",
        .cluster_backup_id = "completed-cluster-after-rotation",
        .created_at_unix_ns = 2,
        .format = .portable,
        .tables = &tables,
    };
    try writeClusterBackupAttemptMarker(alloc, io, &location, &raced_marker);
    const source_shard = localBackupAttemptReclaimShard(
        raced_marker.created_at_unix_ns +| backup_attempt_reclaim_age_ns,
    );
    const destination_shard =
        (source_shard + 1) % backup_attempt_reclaim_shard_count;
    const source_ticket = try localBackupAttemptReclaimTicketPath(
        alloc,
        root,
        .queued,
        source_shard,
        raced_marker.attempt_id,
    );
    defer alloc.free(source_ticket);
    const moved_ticket = try localBackupAttemptReclaimTicketPath(
        alloc,
        root,
        .queued,
        destination_shard,
        raced_marker.attempt_id,
    );
    defer alloc.free(moved_ticket);
    var maintenance_dir = try openBackupRootNoFollow(io, root);
    defer maintenance_dir.close(io);
    try renameLocalBackupAttemptReclaimTicket(
        io,
        maintenance_dir,
        trimLeftSlash(source_ticket[root.len..]),
        trimLeftSlash(moved_ticket[root.len..]),
    );
    try deleteClusterBackupAttemptMarker(
        alloc,
        io,
        &location,
        &raced_marker,
    );
    try std.testing.expectEqual(
        @as(usize, 0),
        try reclaimStaleClusterBackupAttempts(alloc, io, &location, 2),
    );
    try std.testing.expect(!(try pathExistsWithIo(io, moved_ticket)));
    const next_ticket = try localBackupAttemptReclaimTicketPath(
        alloc,
        root,
        .queued,
        (destination_shard + 1) % backup_attempt_reclaim_shard_count,
        raced_marker.attempt_id,
    );
    defer alloc.free(next_ticket);
    try std.testing.expect(!(try pathExistsWithIo(io, next_ticket)));
}

test "filesystem attempt publication tolerates concurrent bounded maintenance" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(
        alloc,
        ".zig-cache/tmp/{s}/publication-maintenance-race",
        .{tmp.sub_path},
    );
    defer alloc.free(root);
    try ensureDirPathWithIo(io, root);

    const tables = [_]ClusterBackupAttemptTable{.{
        .name = "docs",
        .table_backup_id = "race-table",
        .artifact_backup_id = "race-artifact",
    }};

    const Race = struct {
        io: std.Io,
        location: *BackupLocation,
        marker: *const ClusterBackupAttemptMarker,
        publication_hook: *BackupStagingPublicationTestHook,
        maintenance_hook: *BackupMaintenanceTestHook,
        publish_err: ?anyerror = null,
        maintenance_err: ?anyerror = null,

        fn publish(self: *@This()) void {
            defer self.publication_hook.progress.set(self.io);
            writeClusterBackupAttemptMarkerWithHook(
                std.heap.page_allocator,
                self.io,
                self.location,
                self.marker,
                self.publication_hook,
            ) catch |err| {
                self.publish_err = err;
            };
        }

        fn maintain(self: *@This()) void {
            defer self.maintenance_hook.progress.set(self.io);
            _ = reclaimStaleClusterBackupAttemptsWithHook(
                std.heap.page_allocator,
                self.io,
                self.location,
                backup_attempt_staging_orphan_age_ns + 1,
                self.maintenance_hook,
            ) catch |err| {
                self.maintenance_err = err;
                return;
            };
        }
    };

    // Pause exactly after the hidden ticket is durable and before rename. Its
    // forced old mtime makes maintenance take the deletion path; the existing
    // per-attempt lock must fence deletion until publication completes.
    for (0..4) |iteration| {
        const iteration_root = try std.fmt.allocPrint(
            alloc,
            "{s}/iteration-{d}",
            .{ root, iteration },
        );
        defer alloc.free(iteration_root);
        try ensureDirPathWithIo(io, iteration_root);
        var location: BackupLocation = .{ .file = iteration_root };
        const attempt_id = try std.fmt.allocPrint(alloc, "race-attempt-{d}", .{iteration});
        defer alloc.free(attempt_id);
        const cluster_backup_id = try std.fmt.allocPrint(alloc, "race-cluster-{d}", .{iteration});
        defer alloc.free(cluster_backup_id);
        const marker: ClusterBackupAttemptMarker = .{
            .attempt_id = attempt_id,
            .cluster_backup_id = cluster_backup_id,
            .created_at_unix_ns = 1,
            .format = .portable,
            .tables = &tables,
        };

        var publication_hook: BackupStagingPublicationTestHook = .{
            .io = io,
            .force_modify_timestamp_ns = 0,
        };
        var maintenance_hook: BackupMaintenanceTestHook = .{};
        var race: Race = .{
            .io = io,
            .location = &location,
            .marker = &marker,
            .publication_hook = &publication_hook,
            .maintenance_hook = &maintenance_hook,
        };
        var publisher = try io.concurrent(Race.publish, .{&race});
        var publisher_pending = true;
        defer if (publisher_pending) {
            publication_hook.release.set(io);
            _ = publisher.await(io);
        };
        publication_hook.progress.waitUncancelable(io);
        if (!publication_hook.staged.load(.acquire)) {
            _ = publisher.await(io);
            publisher_pending = false;
            if (race.publish_err) |err| return err;
            return error.TestUnexpectedResult;
        }
        var maintenance = try io.concurrent(Race.maintain, .{&race});
        var maintenance_pending = true;
        defer {
            if (maintenance_pending) _ = maintenance.await(io);
        }
        maintenance_hook.progress.waitUncancelable(io);
        const maintenance_observed_staging =
            maintenance_hook.stale_staging_candidate.load(.acquire);
        publication_hook.release.set(io);
        _ = publisher.await(io);
        publisher_pending = false;
        _ = maintenance.await(io);
        maintenance_pending = false;

        if (publication_hook.failure) |err| return err;
        if (race.publish_err) |err| return err;
        if (race.maintenance_err) |err| return err;
        try std.testing.expect(maintenance_observed_staging);
        var parsed = try readClusterBackupAttemptMarker(
            alloc,
            io,
            &location,
            marker.attempt_id,
        );
        defer parsed.deinit();
        try std.testing.expectEqualStrings(
            marker.cluster_backup_id,
            parsed.value.cluster_backup_id,
        );
    }
}

test "filesystem attempt maintenance removes only stale staged tickets" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(
        alloc,
        ".zig-cache/tmp/{s}/staging-scavenge",
        .{tmp.sub_path},
    );
    defer alloc.free(root);
    var location: BackupLocation = .{ .file = root };
    const staging_root = try localBackupAttemptReclaimStagingShardPath(
        alloc,
        root,
        .queued,
        0,
    );
    defer alloc.free(staging_root);
    try ensureDirPathWithIo(io, staging_root);

    const stale_path = try std.fmt.allocPrint(
        alloc,
        "{s}/stale-attempt.replace.tmp",
        .{staging_root},
    );
    defer alloc.free(stale_path);
    const recent_path = try std.fmt.allocPrint(
        alloc,
        "{s}/recent-attempt.replace.tmp",
        .{staging_root},
    );
    defer alloc.free(recent_path);
    for ([_][]const u8{ stale_path, recent_path }) |path| {
        var file = try std.Io.Dir.cwd().createFile(io, path, .{
            .truncate = true,
        });
        file.close(io);
    }
    const now_unix_ns = backup_attempt_staging_orphan_age_ns + 100;
    try std.Io.Dir.cwd().setTimestamps(io, stale_path, .{
        .modify_timestamp = .{
            .new = std.Io.Timestamp.fromNanoseconds(0),
        },
    });
    try std.Io.Dir.cwd().setTimestamps(io, recent_path, .{
        .modify_timestamp = .{
            .new = std.Io.Timestamp.fromNanoseconds(
                @intCast(now_unix_ns - 1),
            ),
        },
    });

    try std.testing.expectEqual(
        @as(usize, 0),
        try reclaimStaleClusterBackupAttempts(
            alloc,
            io,
            &location,
            now_unix_ns,
        ),
    );
    try std.testing.expect(!(try pathExistsWithIo(io, stale_path)));
    try std.testing.expect(try pathExistsWithIo(io, recent_path));
}

test "filesystem stale attempt reclamation recovers an abandoned claim" {
    const alloc = std.testing.allocator;
    try std.testing.expect(!localBackupAttemptReclaimClaimExpired(100, 99));
    try std.testing.expect(!localBackupAttemptReclaimClaimExpired(
        1,
        1 + backup_attempt_reclaim_claim_timeout_ns,
    ));
    try std.testing.expect(localBackupAttemptReclaimClaimExpired(
        1,
        1 + backup_attempt_reclaim_claim_timeout_ns +
            backup_attempt_lease_clock_skew_allowance_ns,
    ));
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(
        alloc,
        ".zig-cache/tmp/{s}/reclaim-claim",
        .{tmp.sub_path},
    );
    defer alloc.free(root);
    var location: BackupLocation = .{ .file = root };

    const tables = [_]ClusterBackupAttemptTable{.{
        .name = "docs",
        .table_backup_id = "claim-table",
        .artifact_backup_id = "claim-artifact",
    }};
    const marker: ClusterBackupAttemptMarker = .{
        .attempt_id = "abandoned-claim",
        .cluster_backup_id = "claim-cluster",
        .created_at_unix_ns = 1,
        .format = .portable,
        .writer_lease_fencing = true,
        .tables = &tables,
    };
    try writeClusterBackupAttemptMarker(alloc, io, &location, &marker);

    const shard = localBackupAttemptReclaimShard(
        marker.created_at_unix_ns +| backup_attempt_reclaim_age_ns,
    );
    const queue_path = try localBackupAttemptReclaimTicketPath(
        alloc,
        root,
        .queued,
        shard,
        marker.attempt_id,
    );
    defer alloc.free(queue_path);
    const claim_path = try localBackupAttemptReclaimTicketPath(
        alloc,
        root,
        .claimed,
        shard,
        marker.attempt_id,
    );
    defer alloc.free(claim_path);
    var maintenance_dir = try openBackupRootNoFollow(io, root);
    defer maintenance_dir.close(io);
    try replaceLocalBackupAttemptReclaimTicketTimestamp(
        alloc,
        io,
        maintenance_dir,
        trimLeftSlash(queue_path[root.len..]),
        1,
    );
    try renameLocalBackupAttemptReclaimTicket(
        io,
        maintenance_dir,
        trimLeftSlash(queue_path[root.len..]),
        trimLeftSlash(claim_path[root.len..]),
    );

    const now_unix_ns =
        backup_attempt_reclaim_age_ns +
        backup_attempt_reclaim_claim_timeout_ns + 2;
    var reclaimed: usize = 0;
    for (0..2) |_| {
        reclaimed += try reclaimStaleClusterBackupAttempts(
            alloc,
            io,
            &location,
            now_unix_ns,
        );
        if (reclaimed > 0) break;
    }
    try std.testing.expectEqual(@as(usize, 1), reclaimed);
    try std.testing.expectError(
        error.FileNotFound,
        readClusterBackupAttemptMarker(alloc, io, &location, marker.attempt_id),
    );
}

test "remote stale attempt reclamation cursor prevents prefix starvation" {
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

    const now_unix_ns = backup_attempt_reclaim_age_ns + 1;
    // Durable mismatch evidence has its own prefix and therefore cannot
    // consume the bounded `.antfly-incomplete` list page used for active
    // reconciliation, even after many quarantines accumulate.
    for (0..backup_attempt_reclaim_scan_budget) |i| {
        const attempt_id = try std.fmt.allocPrint(alloc, "q-{d:0>3}", .{i});
        defer alloc.free(attempt_id);
        const quarantine_path = try clusterBackupAttemptQuarantinePath(
            alloc,
            "",
            attempt_id,
        );
        defer alloc.free(quarantine_path);
        try location.remote.writeBytes(
            alloc,
            trimLeftSlash(quarantine_path),
            "{}",
            "application/json",
        );
    }
    // A rolling upgrade may encounter durable evidence written under the old
    // active-marker prefix. It must make bounded migration progress without
    // discarding the evidence or repeatedly taxing every cursor rotation.
    for (0..backup_attempt_reclaim_scan_budget) |i| {
        const attempt_id = try std.fmt.allocPrint(alloc, "0legacy-{d:0>3}", .{i});
        defer alloc.free(attempt_id);
        const quarantine_path = try legacyClusterBackupAttemptQuarantinePath(
            alloc,
            "",
            attempt_id,
        );
        defer alloc.free(quarantine_path);
        try location.remote.writeBytes(
            alloc,
            trimLeftSlash(quarantine_path),
            "legacy-quarantine",
            "application/json",
        );
    }
    for (0..backup_attempt_reclaim_scan_budget) |i| {
        const attempt_id = try std.fmt.allocPrint(alloc, "a-{d:0>3}", .{i});
        defer alloc.free(attempt_id);
        const cluster_id = try std.fmt.allocPrint(alloc, "cluster-{d:0>3}", .{i});
        defer alloc.free(cluster_id);
        const table_id = try std.fmt.allocPrint(alloc, "table-{d:0>3}", .{i});
        defer alloc.free(table_id);
        const artifact_id = try std.fmt.allocPrint(alloc, "artifact-{d:0>3}", .{i});
        defer alloc.free(artifact_id);
        const tables = [_]ClusterBackupAttemptTable{.{
            .name = "docs",
            .table_backup_id = table_id,
            .artifact_backup_id = artifact_id,
        }};
        const marker: ClusterBackupAttemptMarker = .{
            .attempt_id = attempt_id,
            .cluster_backup_id = cluster_id,
            .created_at_unix_ns = now_unix_ns,
            .format = .portable,
            .tables = &tables,
        };
        try writeClusterBackupAttemptMarker(alloc, io, &location, &marker);
    }
    const stale_tables = [_]ClusterBackupAttemptTable{.{
        .name = "docs",
        .table_backup_id = "z-stale-table",
        .artifact_backup_id = "z-stale-artifact",
    }};
    const stale_marker: ClusterBackupAttemptMarker = .{
        .attempt_id = "z-stale",
        .cluster_backup_id = "z-stale-cluster",
        .created_at_unix_ns = 1,
        .format = .portable,
        .writer_lease_fencing = true,
        .tables = &stale_tables,
    };
    try writeClusterBackupAttemptMarker(alloc, io, &location, &stale_marker);

    try std.testing.expectEqual(
        @as(usize, 0),
        try reclaimStaleClusterBackupAttempts(alloc, io, &location, now_unix_ns),
    );
    for (0..backup_attempt_legacy_quarantine_migration_batch_size) |i| {
        const attempt_id = try std.fmt.allocPrint(alloc, "0legacy-{d:0>3}", .{i});
        defer alloc.free(attempt_id);
        const legacy_path = try legacyClusterBackupAttemptQuarantinePath(
            alloc,
            "",
            attempt_id,
        );
        defer alloc.free(legacy_path);
        try std.testing.expectError(
            error.FileNotFound,
            location.remote.readBytesAllocLimited(
                alloc,
                trimLeftSlash(legacy_path),
                max_backup_attempt_quarantine_bytes,
            ),
        );
        const migrated_path = try clusterBackupAttemptQuarantinePath(
            alloc,
            "",
            attempt_id,
        );
        defer alloc.free(migrated_path);
        const migrated = try location.remote.readBytesAllocLimited(
            alloc,
            trimLeftSlash(migrated_path),
            max_backup_attempt_quarantine_bytes,
        );
        defer alloc.free(migrated);
        try std.testing.expectEqualStrings("legacy-quarantine", migrated);
    }
    const pending_attempt_id = "0legacy-002";
    const pending_legacy_path = try legacyClusterBackupAttemptQuarantinePath(
        alloc,
        "",
        pending_attempt_id,
    );
    defer alloc.free(pending_legacy_path);
    const pending_legacy = try location.remote.readBytesAllocLimited(
        alloc,
        trimLeftSlash(pending_legacy_path),
        max_backup_attempt_quarantine_bytes,
    );
    defer alloc.free(pending_legacy);
    try std.testing.expectEqualStrings("legacy-quarantine", pending_legacy);
    try std.testing.expectEqual(
        @as(usize, 0),
        try reclaimStaleClusterBackupAttempts(alloc, io, &location, now_unix_ns),
    );
    try std.testing.expectEqual(
        @as(usize, 1),
        try reclaimStaleClusterBackupAttempts(alloc, io, &location, now_unix_ns),
    );
    try std.testing.expectError(
        error.FileNotFound,
        readClusterBackupAttemptMarker(alloc, io, &location, stale_marker.attempt_id),
    );
}

test "legacy quarantine equivalence rejects a borrowed marker digest" {
    const alloc = std.testing.allocator;
    const legacy_tables = [_]ClusterBackupAttemptTable{.{
        .name = "docs",
        .table_backup_id = "legacy-table",
        .artifact_backup_id = "legacy-artifact",
    }};
    const destination_tables = [_]ClusterBackupAttemptTable{.{
        .name = "docs",
        .table_backup_id = "destination-table",
        .artifact_backup_id = "destination-artifact",
    }};
    const legacy_marker: ClusterBackupAttemptMarker = .{
        .attempt_id = "quarantine-attempt",
        .cluster_backup_id = "quarantine-cluster",
        .created_at_unix_ns = 1,
        .format = .portable,
        .tables = &legacy_tables,
    };
    const destination_marker: ClusterBackupAttemptMarker = .{
        .attempt_id = legacy_marker.attempt_id,
        .cluster_backup_id = legacy_marker.cluster_backup_id,
        .created_at_unix_ns = 1,
        .format = .portable,
        .tables = &destination_tables,
    };
    const destination_marker_json = try stringifyJsonAlloc(alloc, destination_marker);
    defer alloc.free(destination_marker_json);
    var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(destination_marker_json, &digest, .{});
    const destination_digest = std.fmt.bytesToHex(digest, .lower);
    const legacy_body = try stringifyJsonAlloc(alloc, ClusterBackupAttemptQuarantine{
        .attempt_id = legacy_marker.attempt_id,
        .cluster_backup_id = legacy_marker.cluster_backup_id,
        .reason = .manifest_generation_mismatch,
        .quarantined_at_unix_ns = 1,
        .marker_sha256 = destination_digest[0..],
        .marker = legacy_marker,
    });
    defer alloc.free(legacy_body);
    const destination_body = try stringifyJsonAlloc(alloc, ClusterBackupAttemptQuarantine{
        .attempt_id = destination_marker.attempt_id,
        .cluster_backup_id = destination_marker.cluster_backup_id,
        .reason = .manifest_generation_mismatch,
        .quarantined_at_unix_ns = 2,
        .marker_sha256 = destination_digest[0..],
        .marker = destination_marker,
    });
    defer alloc.free(destination_body);

    const expected_destination = try std.fmt.allocPrint(
        alloc,
        "{{\"format_version\":1,\"attempt_id\":\"quarantine-attempt\",\"cluster_backup_id\":\"quarantine-cluster\",\"reason\":\"manifest_generation_mismatch\",\"quarantined_at_unix_ns\":2,\"marker_sha256\":\"{s}\",\"marker\":{{\"format_version\":1,\"attempt_id\":\"quarantine-attempt\",\"cluster_backup_id\":\"quarantine-cluster\",\"created_at_unix_ns\":1,\"format\":\"portable\",\"writer_lease_fencing\":false,\"writer_delivery_deadline_fencing\":false,\"tables\":[{{\"name\":\"docs\",\"table_backup_id\":\"destination-table\",\"artifact_backup_id\":\"destination-artifact\"}}]}}}}",
        .{destination_digest},
    );
    defer alloc.free(expected_destination);
    // Use lib/json's semantic fixture helper so key order is irrelevant while
    // the complete durable evidence shape remains covered.
    try ant_json.testing.expectEqualJsonText(
        alloc,
        expected_destination,
        destination_body,
    );
    try std.testing.expect(!backupAttemptQuarantineBodiesEquivalent(
        alloc,
        legacy_body,
        destination_body,
        legacy_marker.attempt_id,
    ));

    var memory = object_storage.MemoryObjectStorage.init(alloc);
    defer memory.deinit();
    var location: BackupLocation = .{
        .remote = try RemoteBackupStore.initWithClient(
            alloc,
            memory.client(),
            "bucket",
            "backups",
        ),
    };
    defer location.deinit(alloc);
    const legacy_path = try legacyClusterBackupAttemptQuarantinePath(
        alloc,
        "",
        legacy_marker.attempt_id,
    );
    defer alloc.free(legacy_path);
    const destination_path = try clusterBackupAttemptQuarantinePath(
        alloc,
        "",
        legacy_marker.attempt_id,
    );
    defer alloc.free(destination_path);
    try location.remote.writeBytes(
        alloc,
        trimLeftSlash(legacy_path),
        legacy_body,
        "application/json",
    );
    try location.remote.writeBytes(
        alloc,
        trimLeftSlash(destination_path),
        destination_body,
        "application/json",
    );
    var operation_budget: usize = 8;
    try std.testing.expectError(
        error.BackupPublicationConflict,
        migrateLegacyRemoteBackupAttemptQuarantine(
            alloc,
            &location.remote,
            legacy_marker.attempt_id,
            &operation_budget,
        ),
    );
    const preserved_legacy = try location.remote.readBytesAllocLimited(
        alloc,
        trimLeftSlash(legacy_path),
        max_backup_attempt_quarantine_bytes,
    );
    defer alloc.free(preserved_legacy);
    try ant_json.testing.expectEqualJsonText(
        alloc,
        legacy_body,
        preserved_legacy,
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
        .writer_lease_fencing = true,
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
        @as(usize, 1),
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
    try std.testing.expectError(
        error.FileNotFound,
        readClusterBackupAttemptMarker(alloc, io, &location, marker.attempt_id),
    );
    const quarantine_suffix = try clusterBackupAttemptQuarantinePath(
        alloc,
        "",
        marker.attempt_id,
    );
    defer alloc.free(quarantine_suffix);
    const quarantine = try location.remote.readBytesAllocLimited(
        alloc,
        trimLeftSlash(quarantine_suffix),
        max_backup_attempt_quarantine_bytes,
    );
    alloc.free(quarantine);
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
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer alloc.free(root);
    const src = try std.fmt.allocPrint(alloc, "{s}/copy-src", .{root});
    defer alloc.free(src);
    const dst = try std.fmt.allocPrint(alloc, "{s}/copy-dst", .{root});
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

    var local_location: BackupLocation = .{ .file = root };
    var local_expected = expected;
    local_expected.snapshot_path = "copy-dst";
    var local_exact_hasher = artifactVerificationCacheKeyHasher(
        &local_location,
        .native,
        &local_expected,
    );
    var local_exact = try directoryArtifactIntegrityAllocWithIdentity(
        alloc,
        io,
        dst,
        &local_exact_hasher,
    );
    defer local_exact.deinit(alloc);
    try std.testing.expectEqual(local_expected.artifact_size_bytes, local_exact.size_bytes);
    try std.testing.expectEqualStrings(local_expected.artifact_sha256, local_exact.sha256);
    var local_exact_receipt: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    local_exact_hasher.final(&local_exact_receipt);
    const local_probe_receipt = try artifactVerificationCacheKey(
        alloc,
        io,
        &local_location,
        .native,
        &local_expected,
    );
    try std.testing.expectEqualSlices(u8, &local_exact_receipt, &local_probe_receipt);

    const bounded_top = try readFileFromLocationUsingIoLimited(
        alloc,
        io,
        &local_location,
        "copy-src/top.sst",
        3,
    );
    defer alloc.free(bounded_top);
    try std.testing.expectEqualStrings("top", bounded_top);
    var top_digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash("top", &top_digest, .{});
    const top_sha256 = std.fmt.bytesToHex(top_digest, .lower);
    const verified_top_path = try std.fmt.allocPrint(alloc, "{s}/verified/top.sst", .{root});
    defer alloc.free(verified_top_path);
    try copyFileFromLocationVerifiedUsingIo(
        alloc,
        io,
        &local_location,
        "copy-src/top.sst",
        verified_top_path,
        3,
        &top_sha256,
        .none,
    );
    const verified_top = try readFileAbsoluteAllocWithIo(alloc, io, verified_top_path, 4);
    defer alloc.free(verified_top);
    try std.testing.expectEqualStrings("top", verified_top);

    var memory = object_storage.MemoryObjectStorage.init(alloc);
    defer memory.deinit();
    var remote_location: BackupLocation = .{
        .remote = try RemoteBackupStore.initWithClient(
            alloc,
            memory.client(),
            "bucket",
            "backups",
        ),
    };
    defer remote_location.deinit(alloc);
    try remote_location.remote.uploadDirectoryRecursive(alloc, src, expected.snapshot_path, .none);
    const remote_verified_top_path = try std.fmt.allocPrint(alloc, "{s}/verified/remote-top.sst", .{root});
    defer alloc.free(remote_verified_top_path);
    try copyFileFromLocationVerifiedUsingIo(
        alloc,
        io,
        &remote_location,
        "snap/groups/7/top.sst",
        remote_verified_top_path,
        3,
        &top_sha256,
        .none,
    );
    const remote_verified_top = try readFileAbsoluteAllocWithIo(alloc, io, remote_verified_top_path, 4);
    defer alloc.free(remote_verified_top);
    try std.testing.expectEqualStrings("top", remote_verified_top);
    var remote_exact_hasher = artifactVerificationCacheKeyHasher(
        &remote_location,
        .native,
        &expected,
    );
    try remote_location.remote.verifyNativeArtifactIntegrityWithIdentity(
        alloc,
        &expected,
        &remote_exact_hasher,
    );
    var remote_exact_receipt: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    remote_exact_hasher.final(&remote_exact_receipt);
    const remote_probe_receipt = try artifactVerificationCacheKey(
        alloc,
        io,
        &remote_location,
        .native,
        &expected,
    );
    try std.testing.expectEqualSlices(u8, &remote_exact_receipt, &remote_probe_receipt);

    try writeFileAbsolute(copied_nested_path, "corrupt");
    try std.testing.expectError(
        error.BackupArtifactIntegrityMismatch,
        verifyShardArtifactIntegrity(alloc, null, .native, dst, &expected),
    );
}

test "native artifact verification consistently rejects empty directories" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const empty = try std.fmt.allocPrint(
        alloc,
        ".zig-cache/tmp/{s}/empty-native",
        .{tmp.sub_path},
    );
    defer alloc.free(empty);
    try ensureDirPathWithIo(io, empty);

    try std.testing.expectError(
        error.BackupArtifactMissing,
        directoryArtifactIntegrityAllocWithIdentity(alloc, io, empty, null),
    );
    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    try std.testing.expectError(
        error.BackupArtifactMissing,
        hashLocalNativeArtifactIdentity(alloc, io, empty, &hasher),
    );
}

test "native verification receipt rejects membership changes after exact pass" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer alloc.free(root);
    const artifact_root = try std.fmt.allocPrint(alloc, "{s}/native", .{root});
    defer alloc.free(artifact_root);
    const initial_path = try std.fmt.allocPrint(alloc, "{s}/initial.sst", .{artifact_root});
    defer alloc.free(initial_path);
    try writeFileAbsolute(initial_path, "initial");

    var integrity = try directoryArtifactIntegrityAlloc(alloc, io, artifact_root);
    defer integrity.deinit(alloc);
    const shard: ShardSnapshot = .{
        .group_id = 7,
        .start_key = "",
        .snapshot_path = "native",
        .artifact_size_bytes = integrity.size_bytes,
        .artifact_sha256 = integrity.sha256,
    };
    var location: BackupLocation = .{ .file = root };
    const receipt_key = artifactVerificationReceiptLookupKey(
        &location,
        .native,
        &shard,
    );
    const exact_identity = try artifactVerificationCacheKey(
        alloc,
        io,
        &location,
        .native,
        &shard,
    );

    const added_path = try std.fmt.allocPrint(alloc, "{s}/added.sst", .{artifact_root});
    defer alloc.free(added_path);
    try writeFileAbsolute(added_path, "added");
    var cache: ArtifactVerificationCache = .{};
    defer cache.deinit(alloc);
    try std.testing.expectError(
        error.SourceFileChanged,
        recordArtifactVerificationReceiptAfterFence(
            alloc,
            io,
            &location,
            .native,
            &shard,
            &cache,
            receipt_key,
            exact_identity,
        ),
    );
    try std.testing.expectEqual(@as(usize, 0), cache.entries.count());
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

    const missing_leading_range = [_]ShardSnapshot{.{
        .group_id = 7,
        .start_key = "m",
        .snapshot_path = "generation/groups/7",
        .artifact_sha256 = valid_hash,
    }};
    manifest.shards = &missing_leading_range;
    try std.testing.expectError(
        error.InvalidBackupRangeTopology,
        validateTableManifest(std.testing.allocator, &manifest, "snap"),
    );

    const missing_trailing_range = [_]ShardSnapshot{.{
        .group_id = 7,
        .start_key = "",
        .end_key = "m",
        .snapshot_path = "generation/groups/7",
        .artifact_sha256 = valid_hash,
    }};
    manifest.shards = &missing_trailing_range;
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

test "restore admission validates the stable newest current Go attempt" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    var memory = object_storage.MemoryObjectStorage.init(alloc);
    defer memory.deinit();
    var location: BackupLocation = .{
        .remote = try RemoteBackupStore.initWithClient(
            alloc,
            memory.client(),
            "bucket",
            "backups",
        ),
    };
    defer location.deinit(alloc);

    const go_backup_id = "go-cluster";
    const go_manifest_path = try clusterMetadataPath(alloc, "", go_backup_id);
    defer alloc.free(go_manifest_path);
    try location.remote.writeBytes(
        alloc,
        go_manifest_path,
        "{\"version\":2,\"state\":\"complete\",\"backup_id\":\"go-cluster\",\"timestamp\":\"2026-07-25T12:00:00Z\",\"antfly_version\":\"go-current\",\"format\":\"portable\",\"expected_table_count\":1,\"completed_table_count\":1,\"tables\":[{\"name\":\"docs\",\"backup_location\":\"s3://archive/prod/table-77cfb73404d45d27f72ecbfb232c3fbaf6efbb64592b5ae78fca3e5c544fd3d4-metadata.json\",\"shard_count\":1,\"status\":\"completed\"}]}",
        "application/json",
    );
    try std.testing.expectError(
        error.IncompleteClusterBackup,
        readClusterManifestForRestoreAdmission(
            alloc,
            io,
            &location,
            go_backup_id,
        ),
    );

    try location.remote.writeBytes(
        alloc,
        "table-77cfb73404d45d27f72ecbfb232c3fbaf6efbb64592b5ae78fca3e5c544fd3d4-metadata.json",
        "{\"version\":2,\"format\":\"portable\",\"artifacts\":[{\"name\":\"go-cluster-1.afb\",\"size_bytes\":17,\"sha256\":\"2042f5c3b5166c9f5cca6eb5c16a9d84c0df1dc673088ebe971e5f20e0e326a6\"}],\"table\":{\"name\":\"docs\",\"shards\":{\"1\":{\"byte_range\":[\"\",\"\"]}}}}",
        "application/json",
    );
    try location.remote.writeBytes(
        alloc,
        "go-cluster-1.afb",
        "portable-artifact",
        "application/octet-stream",
    );
    const go_attempt_body =
        "{\"version\":1,\"attempt_id\":\"afba-go\",\"backup_id\":\"go-cluster\",\"created_at\":\"2026-07-25T12:00:00.25Z\",\"format\":\"portable\",\"expected_table_count\":1,\"table_names\":[\"docs\"],\"metadata_ids\":[\"table-77cfb73404d45d27f72ecbfb232c3fbaf6efbb64592b5ae78fca3e5c544fd3d4\"],\"artifact_names\":[\"go-cluster-1.afb\"]}";
    try location.remote.writeBytes(
        alloc,
        ".antfly-incomplete/afba-go.json",
        go_attempt_body,
        "application/json",
    );
    var go_attempt_digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(go_attempt_body, &go_attempt_digest, .{});
    const go_attempt_hex = std.fmt.bytesToHex(go_attempt_digest, .lower);
    const go_head = try std.fmt.allocPrint(
        alloc,
        "{{\"version\":1,\"generation\":1,\"attempt_id\":\"afba-go\",\"backup_id\":\"go-cluster\",\"state\":\"committed\",\"marker_sha256\":\"{s}\"}}",
        .{&go_attempt_hex},
    );
    defer alloc.free(go_head);
    try location.remote.writeBytes(
        alloc,
        current_go_backup_attempt_head_name,
        go_head,
        "application/json",
    );

    var admitted = try readClusterManifestForRestoreAdmission(
        alloc,
        io,
        &location,
        go_backup_id,
    );
    defer admitted.deinit(alloc);
    try std.testing.expectEqualStrings(go_backup_id, admitted.backup_id);
    try std.testing.expectEqualStrings(
        "table-77cfb73404d45d27f72ecbfb232c3fbaf6efbb64592b5ae78fca3e5c544fd3d4",
        admitted.tables[0].table_backup_id,
    );

    // Headless journals are deliberately unsupported in the breaking release.
    try location.remote.deleteSuffix(
        alloc,
        current_go_backup_attempt_head_name,
    );
    try std.testing.expectError(
        error.IncompleteClusterBackup,
        readClusterManifestForRestoreAdmission(
            alloc,
            io,
            &location,
            go_backup_id,
        ),
    );
    try location.remote.writeBytes(
        alloc,
        current_go_backup_attempt_head_name,
        go_head,
        "application/json",
    );

    try location.remote.writeBytes(
        alloc,
        "go-cluster-1.afb",
        "PORTABLE-ARTIFACT",
        "application/octet-stream",
    );
    try std.testing.expectError(
        error.BackupArtifactIntegrityMismatch,
        readClusterManifestForRestoreAdmission(
            alloc,
            io,
            &location,
            go_backup_id,
        ),
    );
    try location.remote.writeBytes(
        alloc,
        "go-cluster-1.afb",
        "portable-artifact",
        "application/octet-stream",
    );

    const zig_backup_id = "zig-cluster";
    const zig_tables = [_]ClusterTableBackupEntry{.{
        .name = "docs",
        .table_backup_id = "zig-table",
    }};
    var zig_manifest = try createClusterManifest(
        alloc,
        zig_backup_id,
        "s3://bucket/backups",
        &zig_tables,
    );
    defer zig_manifest.deinit(alloc);
    try writeClusterManifestToLocationWithIo(alloc, io, &location, &zig_manifest);
    try std.testing.expectError(
        error.IncompleteClusterBackup,
        readClusterManifestForRestoreAdmission(
            alloc,
            io,
            &location,
            zig_backup_id,
        ),
    );

    const newer_attempt_body =
        "{\"version\":1,\"attempt_id\":\"afba-newer\",\"backup_id\":\"newer-cluster\",\"created_at\":\"2026-07-25T12:00:00.3Z\",\"format\":\"portable\",\"expected_table_count\":1,\"table_names\":[\"events\"],\"metadata_ids\":[\"table-b2aa693bbed89e7783245e7a6b468282bff526817c1285fd6bb3b8707b3187b7\"],\"artifact_names\":[\"newer-cluster-1.afb\"]}";
    try location.remote.writeBytes(
        alloc,
        ".antfly-incomplete/afba-newer.json",
        newer_attempt_body,
        "application/json",
    );
    var newer_attempt_digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(newer_attempt_body, &newer_attempt_digest, .{});
    const newer_attempt_hex = std.fmt.bytesToHex(newer_attempt_digest, .lower);
    const newer_head = try std.fmt.allocPrint(
        alloc,
        "{{\"version\":1,\"generation\":2,\"attempt_id\":\"afba-newer\",\"backup_id\":\"newer-cluster\",\"state\":\"active\",\"marker_sha256\":\"{s}\"}}",
        .{&newer_attempt_hex},
    );
    defer alloc.free(newer_head);
    try location.remote.writeBytes(
        alloc,
        current_go_backup_attempt_head_name,
        newer_head,
        "application/json",
    );
    try std.testing.expectError(
        error.IncompleteClusterBackup,
        readClusterManifestForRestoreAdmission(
            alloc,
            io,
            &location,
            go_backup_id,
        ),
    );
}

test "current Go attempt timestamps preserve RFC3339Nano ordering" {
    const whole_second = try parseCurrentGoAttemptTimestamp(
        "2026-07-25T12:00:00Z",
    );
    const fractional = try parseCurrentGoAttemptTimestamp(
        "2026-07-25T12:00:00.9Z",
    );
    const next_second = try parseCurrentGoAttemptTimestamp(
        "2026-07-25T12:00:01.000000001Z",
    );
    try std.testing.expectEqual(
        std.math.Order.lt,
        currentGoAttemptTimestampOrder(whole_second, fractional),
    );
    try std.testing.expectEqual(
        std.math.Order.lt,
        currentGoAttemptTimestampOrder(fractional, next_second),
    );
    try std.testing.expectError(
        error.InvalidBackupRequest,
        parseCurrentGoAttemptTimestamp("2025-02-29T12:00:00Z"),
    );
    try std.testing.expectError(
        error.InvalidBackupRequest,
        parseCurrentGoAttemptTimestamp("0001-01-01T00:00:00Z"),
    );
    _ = try parseCurrentGoAttemptTimestamp("2024-02-29T12:00:00Z");
}

test "current Go table metadata identity matches the producer derivation" {
    try std.testing.expect(currentGoTableBackupMetadataIdMatches(
        "docs",
        "go-cluster",
        "table-77cfb73404d45d27f72ecbfb232c3fbaf6efbb64592b5ae78fca3e5c544fd3d4",
    ));
    try std.testing.expect(!currentGoTableBackupMetadataIdMatches(
        "docs",
        "go-cluster",
        "table-a",
    ));
    try std.testing.expectError(
        error.InvalidBackupId,
        validateCurrentGoBackupId("go..cluster"),
    );
}

test "current Go attempt parser is strict in one pass" {
    const alloc = std.testing.allocator;
    const valid =
        "{\"version\":1,\"attempt_id\":\"afba-go\",\"backup_id\":\"go-cluster\",\"created_at\":\"2026-07-25T12:00:00Z\",\"format\":\"portable\",\"expected_table_count\":1,\"table_names\":[\"docs\"],\"metadata_ids\":[\"table-77cfb73404d45d27f72ecbfb232c3fbaf6efbb64592b5ae78fca3e5c544fd3d4\"],\"artifact_names\":[\"go-cluster-1.afb\"]}";
    var parsed = try parseCurrentGoClusterBackupAttempt(alloc, valid, "afba-go");
    parsed.deinit();
    try std.testing.expectError(
        error.UnknownField,
        parseCurrentGoClusterBackupAttempt(
            alloc,
            "{\"version\":1,\"attempt_id\":\"afba-go\",\"backup_id\":\"go-cluster\",\"created_at\":\"2026-07-25T12:00:00Z\",\"format\":\"portable\",\"expected_table_count\":1,\"table_names\":[\"docs\"],\"metadata_ids\":[\"table-77cfb73404d45d27f72ecbfb232c3fbaf6efbb64592b5ae78fca3e5c544fd3d4\"],\"artifact_names\":[\"go-cluster-1.afb\"],\"future\":true}",
            "afba-go",
        ),
    );
    try std.testing.expectError(
        error.DuplicateField,
        parseCurrentGoClusterBackupAttempt(
            alloc,
            "{\"version\":1,\"version\":1,\"attempt_id\":\"afba-go\",\"backup_id\":\"go-cluster\",\"created_at\":\"2026-07-25T12:00:00Z\",\"format\":\"portable\",\"expected_table_count\":1,\"table_names\":[\"docs\"],\"metadata_ids\":[\"table-77cfb73404d45d27f72ecbfb232c3fbaf6efbb64592b5ae78fca3e5c544fd3d4\"],\"artifact_names\":[\"go-cluster-1.afb\"]}",
            "afba-go",
        ),
    );
    try std.testing.expectError(
        error.InvalidBackupRequest,
        parseCurrentGoClusterBackupAttempt(
            alloc,
            "{\"version\":1,\"attempt_id\":\"afba-go\",\"backup_id\":\"go-cluster\",\"created_at\":\"2026-07-25T12:00:00Z\",\"format\":\"portable\",\"expected_table_count\":1,\"table_names\":[\"docs\"],\"metadata_ids\":[\"table-a\"],\"artifact_names\":[\"go-cluster-1.afb\"]}",
            "afba-go",
        ),
    );
}

test "current Go migration head is strict" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    var memory = object_storage.MemoryObjectStorage.init(alloc);
    defer memory.deinit();
    var location: BackupLocation = .{
        .remote = try RemoteBackupStore.initWithClient(
            alloc,
            memory.client(),
            "bucket",
            "backups",
        ),
    };
    defer location.deinit(alloc);

    try location.remote.writeBytes(
        alloc,
        current_go_backup_attempt_head_name,
        "{\"version\":1,\"generation\":1,\"attempt_id\":\"afba-go\",\"backup_id\":\"go-cluster\",\"state\":\"active\",\"marker_sha256\":\"0000000000000000000000000000000000000000000000000000000000000000\",\"future\":true}",
        "application/json",
    );
    try std.testing.expectError(
        error.UnknownField,
        readCurrentGoClusterBackupAttemptHead(
            alloc,
            io_impl.io(),
            &location,
        ),
    );
}

test "current Go migration head selects only the digest-pinned marker" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    var memory = object_storage.MemoryObjectStorage.init(alloc);
    defer memory.deinit();
    var location: BackupLocation = .{
        .remote = try RemoteBackupStore.initWithClient(
            alloc,
            memory.client(),
            "bucket",
            "backups",
        ),
    };
    defer location.deinit(alloc);

    const marker_body =
        "{\"version\":1,\"attempt_id\":\"afba-go\",\"backup_id\":\"go-cluster\",\"created_at\":\"2026-07-25T12:00:00Z\",\"format\":\"portable\",\"expected_table_count\":1,\"table_names\":[\"docs\"],\"metadata_ids\":[\"table-77cfb73404d45d27f72ecbfb232c3fbaf6efbb64592b5ae78fca3e5c544fd3d4\"],\"artifact_names\":[\"go-cluster-1.afb\"]}";
    try location.remote.writeBytes(
        alloc,
        ".antfly-incomplete/afba-go.json",
        marker_body,
        "application/json",
    );
    // An unrelated malformed journal entry must not add admission I/O or
    // influence the authoritative migration decision.
    try location.remote.writeBytes(
        alloc,
        ".antfly-incomplete/afba-unreferenced.json",
        "{not-json",
        "application/json",
    );
    var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(marker_body, &digest, .{});
    const hex = std.fmt.bytesToHex(digest, .lower);
    const head: CurrentGoClusterBackupAttemptHead = .{
        .version = current_go_backup_attempt_head_version,
        .generation = 1,
        .attempt_id = "afba-go",
        .backup_id = "go-cluster",
        .state = .committed,
        .marker_sha256 = &hex,
    };
    var parsed = try readCurrentGoAttemptMarkerForHead(
        alloc,
        io_impl.io(),
        &location,
        &head,
    );
    defer parsed.deinit();
    try std.testing.expectEqualStrings(
        "go-cluster",
        parsed.parsed.value.backup_id,
    );
}

test "current Go migration head rejects marker digest mismatch" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    var memory = object_storage.MemoryObjectStorage.init(alloc);
    defer memory.deinit();
    var location: BackupLocation = .{
        .remote = try RemoteBackupStore.initWithClient(
            alloc,
            memory.client(),
            "bucket",
            "backups",
        ),
    };
    defer location.deinit(alloc);

    try location.remote.writeBytes(
        alloc,
        ".antfly-incomplete/afba-go.json",
        "{}",
        "application/json",
    );
    const head: CurrentGoClusterBackupAttemptHead = .{
        .version = current_go_backup_attempt_head_version,
        .generation = 1,
        .attempt_id = "afba-go",
        .backup_id = "go-cluster",
        .state = .committed,
        .marker_sha256 = "0000000000000000000000000000000000000000000000000000000000000000",
    };
    try std.testing.expectError(
        error.BackupArtifactIntegrityMismatch,
        readCurrentGoAttemptMarkerForHead(
            alloc,
            io_impl.io(),
            &location,
            &head,
        ),
    );
}

test "portable artifact availability rejects empty and non-regular local payloads" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const backup_root = try std.fmt.allocPrint(
        alloc,
        ".zig-cache/tmp/{s}/artifact-availability",
        .{tmp.sub_path},
    );
    defer alloc.free(backup_root);

    const empty_path = try std.fmt.allocPrint(
        alloc,
        "{s}/empty.afb",
        .{backup_root},
    );
    defer alloc.free(empty_path);
    try writeFileAbsolute(empty_path, "");
    var shard: ShardSnapshot = .{
        .group_id = 1,
        .start_key = "",
        .snapshot_path = "empty.afb",
    };
    try std.testing.expectError(
        error.BackupArtifactMissing,
        validateLocalArtifactAvailable(
            alloc,
            io,
            backup_root,
            .portable,
            .derive_after_materialization,
            &shard,
        ),
    );

    const directory_path = try std.fmt.allocPrint(
        alloc,
        "{s}/directory.afb",
        .{backup_root},
    );
    defer alloc.free(directory_path);
    try ensureDirPathWithIo(io, directory_path);
    shard.snapshot_path = "directory.afb";
    try std.testing.expectError(
        error.BackupArtifactMissing,
        validateLocalArtifactAvailable(
            alloc,
            io,
            backup_root,
            .portable,
            .derive_after_materialization,
            &shard,
        ),
    );

    try writeFileAbsolute(empty_path, "portable-artifact");
    shard.snapshot_path = "empty.afb";
    try validateLocalArtifactAvailable(
        alloc,
        io,
        backup_root,
        .portable,
        .derive_after_materialization,
        &shard,
    );
}

test "current Go attempt validation matches Unicode table-name trimming" {
    const alloc = std.testing.allocator;
    const marker =
        "{\"version\":1,\"attempt_id\":\"afba-space\",\"backup_id\":\"go-space\",\"created_at\":\"2026-07-25T12:00:00Z\",\"format\":\"portable\",\"expected_table_count\":1,\"table_names\":[\"\\u3000\"],\"metadata_ids\":[\"table-a\"],\"artifact_names\":[]}";
    try std.testing.expectError(
        error.InvalidBackupRequest,
        parseCurrentGoClusterBackupAttempt(alloc, marker, "afba-space"),
    );
}
