// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the License 2.0 at https://www.antfly.io/licensing/ELv2-license.

//! Runtime-independent topology record wire types shared by metadata and
//! portable storage artifacts. Keep this module below both layers so decoding
//! a seed never imports the metadata control loop into storage-only binaries.

const std = @import("std");

pub const TableRecord = struct {
    table_id: u64,
    name: []const u8,
    description: []const u8 = "",
    schema_json: []const u8 = "",
    read_schema_json: []const u8 = "",
    indexes_json: []const u8 = "{}",
    replication_sources_json: []const u8 = "[]",
    placement_role: []const u8 = "data",
    restore_backup_id: []const u8 = "",
    restore_location: []const u8 = "",
    desired_replica_count: u16 = 3,
    min_ranges: u32 = 1,

    pub fn migrationState(self: *const TableRecord) TableMigrationState {
        return .{
            .schema_json = self.schema_json,
            .read_schema_json = self.read_schema_json,
        };
    }

    pub fn indexCatalog(self: *const TableRecord) TableIndexCatalog {
        return .{ .indexes_json = self.indexes_json };
    }
};

pub const TableMigrationState = struct {
    schema_json: []const u8,
    read_schema_json: []const u8,

    pub fn migrating(self: TableMigrationState) bool {
        return self.read_schema_json.len > 0;
    }
};

pub const TableIndexCatalog = struct {
    indexes_json: []const u8,
};

pub const RangeRecord = struct {
    group_id: u64,
    range_id: u64 = 0,
    table_id: u64,
    start_key: []const u8,
    end_key: ?[]const u8 = null,
    doc_identity_shard_id: u64 = 0,
    doc_identity_range_id: u64 = 0,
    /// Monotonic source-local split attempt allocator. Durable metadata
    /// advances this only in the CAS command that admits the corresponding
    /// transition, so an epoch cannot be consumed without recovery state.
    split_attempt_epoch: u64 = 0,
    restore_backup_id: []const u8 = "",
    restore_artifact_backup_id: []const u8 = "",
    restore_location: []const u8 = "",
    restore_snapshot_path: []const u8 = "",
    /// Cluster-local authority used to resolve `restore_location`. This is an
    /// identifier only; credentials remain in each node's secret/config store.
    restore_connection: []const u8 = "",
    /// Content identity captured from the immutable backup manifest at
    /// admission. New restores require a SHA-256 binding.
    restore_artifact_size_bytes: u64 = 0,
    restore_artifact_sha256: []const u8 = "",
    /// Separately pinned identity for the native generation inventory. A
    /// distributed restore may classify projection-local damage only when this
    /// exact manifest remains authenticated across admission and execution.
    restore_native_manifest_size_bytes: u64 = 0,
    restore_native_manifest_sha256: []const u8 = "",
    /// Durable, bounded idempotency provenance for the most recently
    /// completed restore. Active replica progress can be garbage-collected
    /// without making an exact job retry ambiguous.
    completed_restore_fingerprint: RestoreCompletionFingerprint =
        empty_restore_completion_fingerprint,
};

pub const RestoreCompletionFingerprint =
    [std.crypto.hash.sha2.Sha256.digest_length]u8;
pub const empty_restore_completion_fingerprint: RestoreCompletionFingerprint =
    @as([std.crypto.hash.sha2.Sha256.digest_length]u8, @splat(0));
