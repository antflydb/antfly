// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at https://www.antfly.io/licensing/ELv2-license.

//! Data-only backup/restore contract shared across compiled runtime units.
//! Remote-store implementations and backup algorithms stay in backups.zig.

const std = @import("std");
const CancellationToken = @import("../common/cancellation.zig").CancellationToken;

pub const format_version: u32 = 2;
pub const backup_fence_metadata_group_id_header = "X-Antfly-Backup-Metadata-Group-Id";
pub const backup_fence_metadata_incarnation_header = "X-Antfly-Backup-Metadata-Incarnation";
pub const backup_fence_table_id_header = "X-Antfly-Backup-Table-Id";
pub const backup_fence_definition_header = "X-Antfly-Backup-Definition-SHA256";
pub const backup_fence_topology_count_header = "X-Antfly-Backup-Topology-Count";
pub const backup_fence_topology_header = "X-Antfly-Backup-Topology-SHA256";
pub const backup_writer_not_after_header = "X-Antfly-Backup-Writer-Not-After-Unix-Ns";
pub const catalog_changed_message = "table changed during backup admission";
pub const backup_outcome_ambiguous_message = "backup outcome is ambiguous; inspect the backup id before retrying";

pub const BackupFormat = enum {
    native,
    portable,
};

pub const ArtifactIntegrityMode = enum {
    declared,
    derive_after_materialization,
};

/// Compact, allocation-free catalog identity carried with backup execution.
/// Storage owners must validate it after acquiring their structural-operation
/// guard so a drop/recreate or schema/topology change cannot pair artifacts
/// from one table incarnation with metadata from another.
pub const TableBackupFence = struct {
    metadata_group_id: u64,
    /// Canonical 128-bit metadata-cluster identity encoded as lowercase hex.
    /// All zeroes represent a legacy snapshot that did not expose incarnation.
    metadata_incarnation: [32]u8,
    table_id: u64,
    definition_digest: [std.crypto.hash.sha2.Sha256.digest_length]u8,
    topology_range_count: u64,
    topology_digest: [std.crypto.hash.sha2.Sha256.digest_length]u8,
    /// Current coordinators bound how long a forwarded request may wait before
    /// its storage owner adopts the pre-created writer lease. Null identifies
    /// a legacy delivery that requires a permanent cleanup tombstone.
    writer_not_after_unix_ns: ?u64 = null,

    pub fn hasMetadataIdentity(self: @This()) bool {
        return self.metadata_group_id != 0 and
            !std.mem.allEqual(u8, &self.metadata_incarnation, 0);
    }

    /// Checks an observed fence against this expected fence. Legacy expected
    /// fences intentionally omit metadata identity for one rolling-upgrade
    /// window. A current expected fence never accepts an observation that
    /// cannot prove the same metadata-cluster incarnation.
    pub fn matches(expected: @This(), observed: @This()) bool {
        const metadata_identity_matches = !expected.hasMetadataIdentity() or
            (observed.hasMetadataIdentity() and
                expected.metadata_group_id == observed.metadata_group_id and
                std.crypto.timing_safe.eql(@TypeOf(expected.metadata_incarnation), expected.metadata_incarnation, observed.metadata_incarnation));
        return metadata_identity_matches and expected.table_id == observed.table_id and
            expected.topology_range_count == observed.topology_range_count and
            std.crypto.timing_safe.eql(@TypeOf(expected.definition_digest), expected.definition_digest, observed.definition_digest) and
            std.crypto.timing_safe.eql(@TypeOf(expected.topology_digest), expected.topology_digest, observed.topology_digest);
    }
};

fn hashDefinitionField(hasher: *std.crypto.hash.sha2.Sha256, value: []const u8) void {
    var encoded_len: [8]u8 = undefined;
    std.mem.writeInt(u64, &encoded_len, @intCast(value.len), .big);
    hasher.update(&encoded_len);
    hasher.update(value);
}

pub fn tableDefinitionDigest(
    table_id: u64,
    name: []const u8,
    description: []const u8,
    schema_json: []const u8,
    read_schema_json: []const u8,
    indexes_json: []const u8,
    replication_sources_json: []const u8,
) [std.crypto.hash.sha2.Sha256.digest_length]u8 {
    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    var encoded_table_id: [8]u8 = undefined;
    std.mem.writeInt(u64, &encoded_table_id, table_id, .big);
    hasher.update(&encoded_table_id);
    hashDefinitionField(&hasher, name);
    hashDefinitionField(&hasher, description);
    hashDefinitionField(&hasher, schema_json);
    hashDefinitionField(&hasher, read_schema_json);
    hashDefinitionField(&hasher, indexes_json);
    hashDefinitionField(&hasher, replication_sources_json);
    return hasher.finalResult();
}

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

pub const TableBackupPlan = struct {
    backup_root: []const u8,
    backup_id: []const u8,
    format: BackupFormat = .native,
    io: ?std.Io = null,
    fence: ?TableBackupFence = null,
    /// Borrowed cooperative cancellation for capture, hashing, and local
    /// materialization. Durable publication still reports ambiguity according
    /// to the backup protocol once its commit point has been crossed.
    cancellation: CancellationToken = .none,
};

pub const TableRestorePlan = struct {
    backup_root: []const u8,
    manifest: *const TableBackupManifest,
    artifact_backup_id: []const u8,
    source_location: []const u8,
    reconcile_only: bool = false,
    replace_existing: bool = false,
    publication_hook: ?RestorePublicationHook = null,
    io: ?std.Io = null,
    /// Cooperative cancellation for long-running staged repair. The token's
    /// semantic callback is safe across compiled runtime boundaries and is
    /// borrowed only for the synchronous restore callback.
    cancellation: CancellationToken = .none,
};
