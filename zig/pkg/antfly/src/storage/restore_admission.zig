// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! A restore intent authorizes opening only its exact imported generation.
//! Keep this read lease until DB.open has acquired its own generation lease.
const std = @import("std");
const lifecycle = @import("db/generation_lifecycle.zig");
const backup_restore = @import("../raft/storage/backup_restore.zig");
const Identity = @import("restore_identity.zig").Identity;

pub fn acquire(
    alloc: std.mem.Allocator,
    io: std.Io,
    path: []const u8,
    group_id: u64,
    identity: Identity,
) !lifecycle.ReadLease {
    var lease = (lifecycle.acquirePublishedGenerationReadWithIo(alloc, path, io) catch |err| switch (err) {
        error.GenerationTransitionActive => return error.StorageReadTemporarilyUnavailable,
        else => return err,
    }) orelse return error.StorageReadTemporarilyUnavailable;
    errdefer lease.deinit();
    backup_restore.validateImportedRestoreIdentityWithIo(alloc, io, path, group_id, .{
        .backup_id = identity.backup_id,
        .artifact_backup_id = identity.backup_id,
        .location = identity.location,
        .snapshot_path = identity.snapshot_path,
        .authority = .staged_local,
        .expected_artifact_size_bytes = 0,
        .expected_artifact_sha256 = identity.artifact_sha256,
        .expected_native_manifest_size_bytes = identity.native_manifest_size_bytes,
        .expected_native_manifest_sha256 = identity.native_manifest_sha256,
    }) catch |err| switch (err) {
        error.RestoreIdentityMismatch => return error.StorageReadTemporarilyUnavailable,
        else => return err,
    };
    return lease;
}
