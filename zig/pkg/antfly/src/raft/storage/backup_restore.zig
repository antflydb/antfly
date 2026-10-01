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

//! Server replica-catalog admission around local backup materialization.
const engine = @import("../../storage/backup_restore.zig");
pub const PreparedRestore = engine.PreparedRestore;
pub const RestoreAuthority = engine.RestoreAuthority;
pub const RestoreOptions = engine.RestoreOptions;
pub const RestoreSource = engine.RestoreSource;
pub const applyRestoreSnapshotToPath = engine.applyRestoreSnapshotToPath;
pub const applyRestoreSnapshotToPathWithExclusiveTransition = engine.applyRestoreSnapshotToPathWithExclusiveTransition;
pub const applyRestoreSnapshotToPathWithOptions = engine.applyRestoreSnapshotToPathWithOptions;
pub const applyRestoreSnapshotToReplicaRoot = engine.applyRestoreSnapshotToReplicaRoot;
pub const cleanupSnapshotsForPublishedRestore = engine.cleanupSnapshotsForPublishedRestore;
pub const groupDbPathFromReplicaRoot = engine.groupDbPathFromReplicaRoot;
pub const prepareRestoreSnapshotToPathWithExclusiveTransition = engine.prepareRestoreSnapshotToPathWithExclusiveTransition;
pub const prepareRestoreSnapshotToPathWithPreparation = engine.prepareRestoreSnapshotToPathWithPreparation;
pub const publishPreparedRestore = engine.publishPreparedRestore;
pub const reconcileCommittedRestoreWithExclusiveTransition = engine.reconcileCommittedRestoreWithExclusiveTransition;
pub const reconcileCommittedRestoreWithExclusiveTransitionWithIo = engine.reconcileCommittedRestoreWithExclusiveTransitionWithIo;
pub const validateCommittedRestoreIdentity = engine.validateCommittedRestoreIdentity;
pub const validateCommittedRestoreIdentityWithIo = engine.validateCommittedRestoreIdentityWithIo;
pub const validateImportedRestoreIdentity = engine.validateImportedRestoreIdentity;
pub const validateImportedRestoreIdentityWithIo = engine.validateImportedRestoreIdentityWithIo;
const backups_api = @import("../../api/local_backups.zig");
const db_mod = @import("../../storage/db/selected_root.zig").db;
pub const publishedRestoreAlreadyApplied = engine.publishedRestoreAlreadyApplied;
const std = @import("std");
const writeFile = engine.test_support.writeFile;

pub fn applyBackupRestoreFromRecord(
    alloc: std.mem.Allocator,
    replica_root_dir: []const u8,
    group_id: u64,
    restore: @import("catalog.zig").BackupRestoreBootstrapRecord,
) !void {
    return try applyBackupRestoreFromRecordWithOptions(alloc, replica_root_dir, group_id, restore, .{});
}

pub fn applyBackupRestoreFromRecordWithOptions(
    alloc: std.mem.Allocator,
    replica_root_dir: []const u8,
    group_id: u64,
    restore: @import("catalog.zig").BackupRestoreBootstrapRecord,
    open_options: backups_api.OpenOptions,
) !void {
    try restore.validate();
    const path = try groupDbPathFromReplicaRoot(alloc, replica_root_dir, group_id);
    defer alloc.free(path);
    const source: RestoreSource = .{
        .backup_id = restore.backup_id,
        .artifact_backup_id = restore.artifact_backup_id,
        .location = restore.location,
        .snapshot_path = restore.snapshot_path,
        .authority = .{ .external = restore.connection },
        .expected_artifact_size_bytes = restore.artifact_size_bytes,
        .expected_artifact_sha256 = restore.artifact_sha256,
        .expected_native_manifest_size_bytes = restore.native_manifest_size_bytes,
        .expected_native_manifest_sha256 = restore.native_manifest_sha256,
        .open_options = open_options,
    };
    if (try publishedRestoreAlreadyApplied(alloc, path, group_id, source)) return;
    try applyRestoreSnapshotToPathWithOptions(alloc, path, group_id, source, .{
        .expected_table_name = if (restore.destination_table_name.len > 0) restore.destination_table_name else null,
        .expected_identity_namespace = if (restore.destination_table_id != 0) .{
            .table_id = restore.destination_table_id,
            .shard_id = restore.destination_shard_id,
            .range_id = restore.destination_range_id,
        } else null,
    });
}

test "backup restore bootstrap adopts an exact imported generation while repair holds a reader" {
    const alloc = std.testing.allocator;
    const group_id: u64 = 1701;
    const artifact_sha256 = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/restore-bootstrap-idempotence", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try groupDbPathFromReplicaRoot(alloc, replica_root_dir, group_id);
    defer alloc.free(path);
    const marker_path = try std.fmt.allocPrint(alloc, "{s}/.restore-state", .{path});
    defer alloc.free(marker_path);
    try writeFile(marker_path,
        \\{"format_version":1,"backup_id":"backup-1701","location":"s3://backup/antfly","artifact_sha256":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","snapshot_path":"backup-1701/groups/1701.afb","group_id":1701,"phase":"rebuild_graph","primary_restored":true,"runtime_repair_complete":false,"last_error":""}
    );

    var resident_read = (try db_mod.generation_lifecycle.acquirePublishedGenerationRead(alloc, path)) orelse
        return error.TestUnexpectedResult;
    defer resident_read.deinit();

    const exact: @import("catalog.zig").BackupRestoreBootstrapRecord = .{
        .backup_id = "backup-1701",
        .artifact_backup_id = "artifact-1701",
        .location = "s3://backup/antfly",
        .snapshot_path = "backup-1701/groups/1701.afb",
        .connection = "backup-store",
        .artifact_size_bytes = 1,
        .artifact_sha256 = artifact_sha256,
    };
    try applyBackupRestoreFromRecord(alloc, replica_root_dir, group_id, exact);

    var aliased_source = exact;
    aliased_source.artifact_backup_id = "artifact-1701-copy";
    aliased_source.connection = "rotated-backup-store";
    try applyBackupRestoreFromRecord(alloc, replica_root_dir, group_id, aliased_source);

    var different = exact;
    different.backup_id = "backup-1701-different";
    try std.testing.expectError(
        error.GenerationTransitionActive,
        applyBackupRestoreFromRecord(alloc, replica_root_dir, group_id, different),
    );
}
