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
const fs_paths = @import("../../common/fs_paths.zig");
const threaded_io_limits = @import("../../common/threaded_io_limits.zig");
const backups_api = @import("../../api/backups.zig");
const db_mod = @import("../../storage/db/mod.zig");
const doc_identity = @import("../../storage/db/doc_identity.zig");
const portable_backup = @import("../../storage/portable_backup.zig");

pub const RestoreAuthority = union(enum) {
    /// A private artifact already admitted and staged by Antfly.
    staged_local,
    /// An external source resolved through this cluster-local connection ID.
    external: []const u8,
};

pub const RestoreSource = struct {
    backup_id: []const u8,
    artifact_backup_id: []const u8,
    location: []const u8,
    identity_location: ?[]const u8 = null,
    snapshot_path: []const u8,
    authority: RestoreAuthority,
    expected_artifact_size_bytes: u64,
    expected_artifact_sha256: []const u8,
    manifest: ?*const backups_api.TableBackupManifest = null,
    io: ?std.Io = null,
    open_options: backups_api.OpenOptions = .{},
};

const RestoreIoScope = struct {
    alloc: std.mem.Allocator,
    io_value: std.Io,
    owned: ?*std.Io.Threaded = null,

    fn init(alloc: std.mem.Allocator, restore: RestoreSource) !RestoreIoScope {
        if (restore.open_options.io orelse restore.io) |shared_io| {
            return .{ .alloc = alloc, .io_value = shared_io };
        }
        const owned = try alloc.create(std.Io.Threaded);
        errdefer alloc.destroy(owned);
        owned.* = threaded_io_limits.initService(alloc);
        return .{ .alloc = alloc, .io_value = owned.io(), .owned = owned };
    }

    fn deinit(self: *RestoreIoScope) void {
        if (self.owned) |owned| {
            owned.deinit();
            self.alloc.destroy(owned);
        }
        self.* = undefined;
    }

    fn io(self: *const RestoreIoScope) std.Io {
        return self.io_value;
    }
};

fn restoreIdentityLocation(restore: RestoreSource) []const u8 {
    return restore.identity_location orelse restore.location;
}

fn openRestoreLocation(
    alloc: std.mem.Allocator,
    restore: RestoreSource,
    io: std.Io,
) !backups_api.BackupLocation {
    var options = restore.open_options;
    options.connection = switch (restore.authority) {
        .external => |connection| blk: {
            if (connection.len == 0) return error.RestoreConnectionMissing;
            break :blk connection;
        },
        .staged_local => blk: {
            if (restore.identity_location == null or
                !std.mem.startsWith(u8, restore.location, "file://"))
            {
                return error.InvalidStagedRestoreSource;
            }
            backups_api.validateCanonicalRestoreSourceIdentity(
                alloc,
                restore.identity_location.?,
            ) catch |err| switch (err) {
                error.OutOfMemory => return err,
                else => return error.InvalidStagedRestoreSource,
            };
            break :blk null;
        },
    };
    options.required_capability = "restore.read";
    options.io = io;
    return try backups_api.openBackupLocationWithOptions(alloc, restore.location, options);
}

fn validateExpectedArtifactBinding(
    restore: RestoreSource,
    shard: *const backups_api.ShardSnapshot,
) !void {
    if (restore.expected_artifact_sha256.len != std.crypto.hash.sha2.Sha256.digest_length * 2)
        return error.RestoreArtifactIdentityMissing;
    if (restore.expected_artifact_size_bytes != shard.artifact_size_bytes or
        !std.mem.eql(u8, restore.expected_artifact_sha256, shard.artifact_sha256))
    {
        return error.RestoreArtifactIdentityMismatch;
    }
}

pub const RestoreOptions = struct {
    expected_table_name: ?[]const u8 = null,
    expected_identity_namespace: ?doc_identity.Namespace = null,
};

pub fn groupDbPathFromReplicaRoot(alloc: std.mem.Allocator, replica_root_dir: []const u8, group_id: u64) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{s}/group-{d}/table-db", .{ replica_root_dir, group_id });
}

pub fn applyRestoreSnapshotToReplicaRoot(
    alloc: std.mem.Allocator,
    replica_root_dir: []const u8,
    group_id: u64,
    restore: RestoreSource,
    expected_table_name: ?[]const u8,
) !void {
    const path = try groupDbPathFromReplicaRoot(alloc, replica_root_dir, group_id);
    defer alloc.free(path);
    try applyRestoreSnapshotToPath(alloc, path, group_id, restore, expected_table_name);
}

pub fn applyRestoreSnapshotToPath(
    alloc: std.mem.Allocator,
    path: []const u8,
    group_id: u64,
    restore: RestoreSource,
    expected_table_name: ?[]const u8,
) !void {
    try applyRestoreSnapshotToPathWithOptions(alloc, path, group_id, restore, .{
        .expected_table_name = expected_table_name,
    });
}

pub fn applyRestoreSnapshotToPathWithOptions(
    alloc: std.mem.Allocator,
    path: []const u8,
    group_id: u64,
    restore: RestoreSource,
    options: RestoreOptions,
) !void {
    var io_scope = try RestoreIoScope.init(alloc, restore);
    defer io_scope.deinit();
    var restore_with_io = restore;
    restore_with_io.io = io_scope.io();
    var transition = try db_mod.generation_lifecycle.beginProcessExclusiveWithIo(path, io_scope.io());
    defer transition.deinit();
    try applyRestoreSnapshotToPathWithExclusiveTransition(&transition, alloc, path, group_id, restore_with_io, options);
}

pub fn applyRestoreSnapshotToPathWithExclusiveTransition(
    transition: *db_mod.generation_lifecycle.ExclusiveTransition,
    alloc: std.mem.Allocator,
    path: []const u8,
    group_id: u64,
    restore: RestoreSource,
    options: RestoreOptions,
) !void {
    var prepared = (try prepareRestoreSnapshotToPathWithExclusiveTransition(
        transition,
        alloc,
        path,
        group_id,
        restore,
        options,
    )) orelse return;
    defer prepared.deinit();
    const outcome = try publishPreparedRestore(alloc, path, &prepared);
    if (outcome == .durability_uncertain) return error.GenerationDurabilityUncertain;
}

/// Builds and validates a replacement generation without mutating the live
/// root. The caller must hold `transition` until the returned generation is
/// either published or destroyed.
pub fn prepareRestoreSnapshotToPathWithExclusiveTransition(
    transition: *db_mod.generation_lifecycle.ExclusiveTransition,
    alloc: std.mem.Allocator,
    path: []const u8,
    group_id: u64,
    restore: RestoreSource,
    options: RestoreOptions,
) !?db_mod.generation_lifecycle.StagedGeneration {
    try transition.validate(path);
    try transition.reconcilePublished();
    return try prepareRestoreSnapshotIfNeeded(transition, alloc, path, group_id, restore, options);
}

/// Prepares a sibling generation while the current generation remains
/// readable. The caller must drain serving leases and promote `preparation`
/// before publishing the returned generation.
pub fn prepareRestoreSnapshotToPathWithPreparation(
    preparation: *db_mod.generation_lifecycle.PreparationTransition,
    alloc: std.mem.Allocator,
    path: []const u8,
    group_id: u64,
    restore: RestoreSource,
    options: RestoreOptions,
) !?db_mod.generation_lifecycle.StagedGeneration {
    return try prepareRestoreSnapshotIfNeeded(preparation, alloc, path, group_id, restore, options);
}

pub fn publishPreparedRestore(
    alloc: std.mem.Allocator,
    path: []const u8,
    prepared: *db_mod.generation_lifecycle.StagedGeneration,
) !db_mod.generation_lifecycle.PublicationOutcome {
    try prepared.validateLivePath(path);
    const outcome = try prepared.publish();
    cleanupSnapshotsForPublishedRestore(alloc, prepared.io, path);
    return outcome;
}

pub fn reconcileCommittedRestoreWithExclusiveTransition(
    transition: *db_mod.generation_lifecycle.ExclusiveTransition,
    alloc: std.mem.Allocator,
    path: []const u8,
    group_id: u64,
    restore: RestoreSource,
) !void {
    var io_scope = try RestoreIoScope.init(alloc, restore);
    defer io_scope.deinit();
    try reconcileCommittedRestoreWithExclusiveTransitionWithIo(
        transition,
        alloc,
        io_scope.io(),
        path,
        group_id,
        restore,
    );
}

pub fn reconcileCommittedRestoreWithExclusiveTransitionWithIo(
    transition: *db_mod.generation_lifecycle.ExclusiveTransition,
    alloc: std.mem.Allocator,
    io: std.Io,
    path: []const u8,
    group_id: u64,
    restore: RestoreSource,
) !void {
    try transition.validate(path);
    try transition.reconcilePublished();
    try validateCommittedRestoreIdentityWithIo(alloc, io, path, group_id, restore);
}

pub fn validateCommittedRestoreIdentity(
    alloc: std.mem.Allocator,
    path: []const u8,
    group_id: u64,
    restore: RestoreSource,
) !void {
    var io_scope = try RestoreIoScope.init(alloc, restore);
    defer io_scope.deinit();
    try validateCommittedRestoreIdentityWithIo(alloc, io_scope.io(), path, group_id, restore);
}

pub fn validateCommittedRestoreIdentityWithIo(
    alloc: std.mem.Allocator,
    io: std.Io,
    path: []const u8,
    group_id: u64,
    restore: RestoreSource,
) !void {
    var state = (try db_mod.DB.readRestoreStateForPathWithIo(alloc, io, path)) orelse return error.RestoreIdentityMismatch;
    defer state.deinit(alloc);
    if (!state.primary_restored or
        !state.runtime_repair_complete or
        state.group_id != group_id or
        !std.mem.eql(u8, state.backup_id, restore.backup_id) or
        !std.mem.eql(u8, state.location, restoreIdentityLocation(restore)) or
        !std.mem.eql(u8, state.artifact_sha256, restore.expected_artifact_sha256) or
        !std.mem.eql(u8, state.snapshot_path, restore.snapshot_path))
    {
        return error.RestoreIdentityMismatch;
    }
}

fn publishedRestoreAlreadyApplied(
    alloc: std.mem.Allocator,
    path: []const u8,
    group_id: u64,
    restore: RestoreSource,
) !bool {
    var generation_read = (try db_mod.generation_lifecycle.acquirePublishedGenerationRead(alloc, path)) orelse
        return false;
    defer generation_read.deinit();
    validateCommittedRestoreIdentity(alloc, path, group_id, restore) catch |err| switch (err) {
        error.RestoreIdentityMismatch => return false,
        else => return err,
    };
    return true;
}

pub fn validateImportedRestoreIdentity(
    alloc: std.mem.Allocator,
    path: []const u8,
    group_id: u64,
    restore: RestoreSource,
) !void {
    var io_scope = try RestoreIoScope.init(alloc, restore);
    defer io_scope.deinit();
    try validateImportedRestoreIdentityWithIo(alloc, io_scope.io(), path, group_id, restore);
}

pub fn validateImportedRestoreIdentityWithIo(
    alloc: std.mem.Allocator,
    io: std.Io,
    path: []const u8,
    group_id: u64,
    restore: RestoreSource,
) !void {
    var state = (try db_mod.DB.readRestoreStateForPathWithIo(alloc, io, path)) orelse return error.RestoreIdentityMismatch;
    defer state.deinit(alloc);
    if (!state.primary_restored or
        state.group_id != group_id or
        !std.mem.eql(u8, state.backup_id, restore.backup_id) or
        !std.mem.eql(u8, state.location, restoreIdentityLocation(restore)) or
        !std.mem.eql(u8, state.artifact_sha256, restore.expected_artifact_sha256) or
        !std.mem.eql(u8, state.snapshot_path, restore.snapshot_path))
    {
        return error.RestoreIdentityMismatch;
    }
}

pub fn applyBackupRestoreFromRecord(
    alloc: std.mem.Allocator,
    replica_root_dir: []const u8,
    group_id: u64,
    restore: @import("../catalog.zig").BackupRestoreBootstrapRecord,
) !void {
    return try applyBackupRestoreFromRecordWithOptions(alloc, replica_root_dir, group_id, restore, .{});
}

pub fn applyBackupRestoreFromRecordWithOptions(
    alloc: std.mem.Allocator,
    replica_root_dir: []const u8,
    group_id: u64,
    restore: @import("../catalog.zig").BackupRestoreBootstrapRecord,
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
        .open_options = open_options,
    };
    if (try publishedRestoreAlreadyApplied(alloc, path, group_id, source)) return;
    try applyRestoreSnapshotToPathWithOptions(alloc, path, group_id, source, .{});
}

fn prepareRestoreSnapshotIfNeeded(
    transition: anytype,
    alloc: std.mem.Allocator,
    path: []const u8,
    group_id: u64,
    restore: RestoreSource,
    options: RestoreOptions,
) !?db_mod.generation_lifecycle.StagedGeneration {
    var io_scope = try RestoreIoScope.init(alloc, restore);
    defer io_scope.deinit();
    const io = io_scope.io();

    if (try db_mod.DB.readRestoreStateForPathWithIo(alloc, io, path)) |state_value| {
        var state = state_value;
        defer state.deinit(alloc);
        if (state.primary_restored and
            std.mem.eql(u8, state.backup_id, restore.backup_id) and
            std.mem.eql(u8, state.location, restoreIdentityLocation(restore)) and
            std.mem.eql(u8, state.artifact_sha256, restore.expected_artifact_sha256) and
            std.mem.eql(u8, state.snapshot_path, restore.snapshot_path) and
            state.group_id == group_id)
        {
            // The state is persisted inside the staged generation before that
            // generation is sealed and atomically published. Its content hash
            // is therefore the idempotence proof; rescanning the external
            // artifact would be weaker, O(artifact size), and racy.
            return null;
        }
    }

    return try prepareRestoreSnapshot(transition, alloc, io, path, group_id, restore, options);
}

fn prepareRestoreSnapshot(
    transition: anytype,
    alloc: std.mem.Allocator,
    io: std.Io,
    path: []const u8,
    group_id: u64,
    restore: RestoreSource,
    options: RestoreOptions,
) !db_mod.generation_lifecycle.StagedGeneration {
    var location = try openRestoreLocation(alloc, restore, io);
    defer location.deinit(alloc);
    var owned_manifest: ?backups_api.TableBackupManifest = null;
    defer if (owned_manifest) |*manifest| manifest.deinit(alloc);
    const manifest = restore.manifest orelse blk: {
        owned_manifest = try backups_api.readManifestFromLocationWithArtifactBackupId(
            alloc,
            &location,
            restore.backup_id,
            restore.artifact_backup_id,
        );
        break :blk &owned_manifest.?;
    };
    try backups_api.validateRestoreManifest(alloc, manifest, restore.backup_id);
    if (options.expected_table_name) |table_name| {
        if (!std.mem.eql(u8, manifest.table_name, table_name)) {
            std.log.err("restore manifest validation failed phase=table_identity class=mismatch", .{});
            return error.InvalidBackupRequest;
        }
    }
    if (manifest.read_schema_json.len > 0) return error.UnsupportedBackupMigrationState;
    const shard = resolveRestoreShard(manifest, group_id, restore.snapshot_path) orelse
        return error.InvalidBackupRequest;
    try validateExpectedArtifactBinding(restore, shard);
    const snapshot_path = shard.snapshot_path;

    var staged_generation = try transition.beginStaging();
    errdefer staged_generation.deinit();
    const staged_path = staged_generation.path();

    switch (manifest.format) {
        .portable => {
            try applyPortableRestore(
                &staged_generation,
                alloc,
                staged_path,
                group_id,
                restore,
                &location,
                io,
                shard,
                manifest,
                options,
            );
            return staged_generation;
        },
        .native => {},
    }

    const snapshot_root = try stageRestoreSnapshot(alloc, io, path, &location, snapshot_path);
    defer {
        destroyPathIfExistsWithIo(io, snapshot_root);
        alloc.free(snapshot_root);
    }
    try backups_api.verifyShardArtifactIntegrity(
        alloc,
        io,
        .native,
        snapshot_root,
        shard,
    );

    std.log.info("native restore staged generation phase=materialization", .{});
    try db_mod.DB.restoreSnapshotToDeferredRuntimeRepairWithIo(&staged_generation, alloc, io, snapshot_root, staged_path, .{
        .identity_namespace = options.expected_identity_namespace,
    }, .{
        .backup_id = restore.backup_id,
        .location = restoreIdentityLocation(restore),
        .artifact_sha256 = shard.artifact_sha256,
        .snapshot_path = snapshot_path,
        .group_id = group_id,
    });
    std.log.info("native restore staged generation phase=prepared", .{});
    return staged_generation;
}

fn applyPortableRestore(
    staged_generation: *const db_mod.generation_lifecycle.StagedGeneration,
    alloc: std.mem.Allocator,
    path: []const u8,
    group_id: u64,
    restore: RestoreSource,
    location: *backups_api.BackupLocation,
    io: std.Io,
    shard: *const backups_api.ShardSnapshot,
    manifest: *const backups_api.TableBackupManifest,
    options: RestoreOptions,
) !void {
    const embedding_source_fields = try portableEmbeddingSourceFieldsFromIndexesJson(alloc, manifest.indexes_json);
    defer freePortableEmbeddingSourceFields(alloc, embedding_source_fields);
    const afb_path = try stageRestoreFile(alloc, io, path, location, shard.snapshot_path);
    defer {
        if (std.fs.path.dirname(afb_path)) |staging_dir| destroyPathIfExistsWithIo(io, staging_dir);
        alloc.free(afb_path);
    }
    try backups_api.verifyShardArtifactIntegrity(alloc, io, .portable, afb_path, shard);

    var db = try db_mod.DB.open(alloc, path, .{
        .identity_namespace = options.expected_identity_namespace,
        .start_index_workers = false,
        .staged_generation = staged_generation,
    });
    var db_closed = false;
    defer if (!db_closed) db.close();
    var afb_file = if (std.fs.path.isAbsolute(afb_path))
        try std.Io.Dir.openFileAbsolute(io, afb_path, .{})
    else
        try std.Io.Dir.cwd().openFile(io, afb_path, .{});
    defer afb_file.close(io);
    const afb_stat = try afb_file.stat(io);
    try portable_backup.importPortableFileWithOptions(alloc, db.core.store, io, afb_file, afb_stat.size, .{
        .identity_namespace = options.expected_identity_namespace,
        .prefer_existing_identity_namespace = true,
        .import_derived_indexes = true,
        .embedding_source_fields = embedding_source_fields,
    });
    // Go portable AFBs may contain portable logical artifacts (for example
    // embedding batches) as well as old on-disk index directories. Keep the
    // logical artifacts in the DocStore, but drop runtime index directories so
    // configured indexes are rebuilt by Zig.
    const indexes_path = try std.fmt.allocPrint(alloc, "{s}/indexes", .{path});
    defer alloc.free(indexes_path);
    db.close();
    db_closed = true;
    destroyPathIfExists(indexes_path);
    try db_mod.DB.markRestorePrimaryRestoredForPathWithArtifactWithIo(
        alloc,
        io,
        path,
        restore.backup_id,
        restoreIdentityLocation(restore),
        shard.artifact_sha256,
        shard.snapshot_path,
        group_id,
    );
}

fn resolveRestoreShard(
    manifest: *const backups_api.TableBackupManifest,
    group_id: u64,
    requested_snapshot_path: []const u8,
) ?*const backups_api.ShardSnapshot {
    if (requested_snapshot_path.len > 0)
        return backups_api.findShardSnapshotByPath(manifest, requested_snapshot_path);
    return backups_api.findShardSnapshot(manifest, group_id);
}

fn portableEmbeddingSourceFieldsFromIndexesJson(
    alloc: std.mem.Allocator,
    indexes_json: []const u8,
) ![]portable_backup.ImportOptions.EmbeddingSourceField {
    if (indexes_json.len == 0) return &.{};
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, indexes_json, .{});
    defer parsed.deinit();
    const object = switch (parsed.value) {
        .object => |object| object,
        else => return &.{},
    };

    var fields = std.ArrayListUnmanaged(portable_backup.ImportOptions.EmbeddingSourceField).empty;
    errdefer {
        for (fields.items) |field| {
            alloc.free(field.index_name);
            alloc.free(field.field_name);
        }
        fields.deinit(alloc);
    }
    var it = object.iterator();
    while (it.next()) |entry| {
        const cfg = switch (entry.value_ptr.*) {
            .object => |cfg| cfg,
            else => continue,
        };
        const type_value = cfg.get("type") orelse continue;
        if (type_value != .string) continue;
        if (!std.mem.eql(u8, type_value.string, "embeddings") and
            !std.mem.eql(u8, type_value.string, "dense_vector") and
            !std.mem.eql(u8, type_value.string, "sparse_vector")) continue;
        const field_value = cfg.get("field") orelse continue;
        if (field_value != .string or field_value.string.len == 0) continue;
        try fields.append(alloc, .{
            .index_name = try alloc.dupe(u8, entry.key_ptr.*),
            .field_name = try alloc.dupe(u8, field_value.string),
        });
    }
    return try fields.toOwnedSlice(alloc);
}

fn freePortableEmbeddingSourceFields(
    alloc: std.mem.Allocator,
    fields: []const portable_backup.ImportOptions.EmbeddingSourceField,
) void {
    for (fields) |field| {
        alloc.free(field.index_name);
        alloc.free(field.field_name);
    }
    if (fields.len > 0) alloc.free(fields);
}

fn stageRestoreSnapshot(
    alloc: std.mem.Allocator,
    io: std.Io,
    path: []const u8,
    location: *backups_api.BackupLocation,
    snapshot_path: []const u8,
) ![]u8 {
    const staging_root = try std.fmt.allocPrint(alloc, "{s}.restore-source", .{path});
    errdefer alloc.free(staging_root);
    destroyPathIfExistsWithIo(io, staging_root);
    errdefer destroyPathIfExistsWithIo(io, staging_root);
    try backups_api.copyDirectoryFromLocationUsingIo(alloc, io, location, snapshot_path, staging_root);
    return staging_root;
}

fn stageRestoreFile(
    alloc: std.mem.Allocator,
    io: std.Io,
    path: []const u8,
    location: *backups_api.BackupLocation,
    snapshot_path: []const u8,
) ![]u8 {
    const staging_path = try std.fmt.allocPrint(alloc, "{s}.restore-source/{s}", .{ path, std.fs.path.basename(snapshot_path) });
    errdefer alloc.free(staging_path);
    const staging_dir = std.fs.path.dirname(staging_path) orelse return error.InvalidBackupRequest;
    destroyPathIfExistsWithIo(io, staging_dir);
    errdefer destroyPathIfExistsWithIo(io, staging_dir);
    try fs_paths.createDirPathPortable(io, staging_dir);
    try backups_api.copyFileFromLocationUsingIo(alloc, io, location, snapshot_path, staging_path);
    return staging_path;
}

fn cleanupSnapshotsForPublishedRestore(alloc: std.mem.Allocator, io: std.Io, path: []const u8) void {
    const snapshot_dir = std.fmt.allocPrint(alloc, "{s}.snapshots", .{path}) catch return;
    defer alloc.free(snapshot_dir);
    destroyPathIfExistsWithIo(io, snapshot_dir);
}

fn ensureDirPath(path: []const u8) !void {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    try fs_paths.createDirPathPortable(io_impl.io(), path);
}

fn destroyPathIfExists(path: []const u8) void {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
}

fn destroyPathIfExistsWithIo(io: std.Io, path: []const u8) void {
    std.Io.Dir.cwd().deleteTree(io, path) catch {};
}

fn writeFile(path: []const u8, data: []const u8) !void {
    if (std.fs.path.dirname(path)) |dir| try ensureDirPath(dir);
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    try std.Io.Dir.cwd().writeFile(io_impl.io(), .{
        .sub_path = path,
        .data = data,
    });
}

fn readFileAlloc(alloc: std.mem.Allocator, path: []const u8, max_bytes: usize) ![]u8 {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    return try std.Io.Dir.cwd().readFileAlloc(io_impl.io(), path, alloc, .limited(max_bytes));
}

fn reconcileDbIndexes(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    indexes_json: []const u8,
) !usize {
    const removed = try removeMissingIndexes(alloc, db, indexes_json);
    const added = try ensureIndexes(alloc, db, indexes_json);
    if (added > 0 or removed > 0) {
        try db.core.index_manager.syncAll(false);
    }
    return added + removed;
}

fn removeMissingIndexes(alloc: std.mem.Allocator, db: *db_mod.DB, indexes_json: []const u8) !usize {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, indexes_json, .{});
    defer parsed.deinit();
    const object = switch (parsed.value) {
        .object => |object| object,
        else => return error.InvalidTableIndexMetadata,
    };

    const current = try db.listIndexes(alloc);
    defer db_mod.types.freeIndexConfigs(alloc, current);

    var removed: usize = 0;
    for (current) |cfg| {
        if (object.contains(cfg.name)) continue;
        if (try db.deleteIndex(cfg.name)) removed += 1;
    }
    return removed;
}

fn ensureIndexes(alloc: std.mem.Allocator, db: *db_mod.DB, indexes_json: []const u8) !usize {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, indexes_json, .{});
    defer parsed.deinit();
    const object = switch (parsed.value) {
        .object => |object| object,
        else => return error.InvalidTableIndexMetadata,
    };

    var added: usize = 0;
    var it = object.iterator();
    while (it.next()) |entry| {
        const kind = try parseIndexKind(entry.value_ptr.*);
        switch (kind) {
            .full_text => {
                if (db.core.index_manager.textIndex(entry.key_ptr.*) != null) continue;
            },
            .dense_vector => {
                if (db.core.index_manager.denseIndex(entry.key_ptr.*) != null) continue;
            },
            .sparse_vector => {
                if (db.core.index_manager.sparseIndex(entry.key_ptr.*) != null) continue;
            },
            .graph => {
                if (db.core.index_manager.graphIndex(entry.key_ptr.*) != null) continue;
            },
            .algebraic => {
                if (db.core.index_manager.algebraicIndex(entry.key_ptr.*) != null) continue;
            },
        }

        const config_json = extractIndexConfigJson(alloc, entry.key_ptr.*, entry.value_ptr.*) catch |err| {
            std.log.warn("restore skipped index config index={s} err={}", .{ entry.key_ptr.*, err });
            continue;
        };
        defer alloc.free(config_json);
        db.addIndex(.{
            .name = entry.key_ptr.*,
            .kind = kind,
            .config_json = config_json,
        }) catch |err| {
            _ = db.deleteIndex(entry.key_ptr.*) catch false;
            std.log.warn("restore skipped index create index={s} err={}", .{ entry.key_ptr.*, err });
            continue;
        };
        added += 1;
    }
    return added;
}

fn parseIndexKind(value: std.json.Value) !db_mod.types.IndexKind {
    if (value != .object) return .full_text;
    const type_value = value.object.get("type") orelse return .full_text;
    if (type_value != .string) return error.InvalidCreateTableRequest;
    if (std.mem.eql(u8, type_value.string, "full_text")) return .full_text;
    if (std.mem.eql(u8, type_value.string, "graph")) return .graph;
    if (std.mem.eql(u8, type_value.string, "algebraic")) return .algebraic;
    if (std.mem.eql(u8, type_value.string, "embeddings")) {
        const sparse = if (value.object.get("sparse")) |sparse_value| switch (sparse_value) {
            .bool => sparse_value.bool,
            else => return error.InvalidCreateTableRequest,
        } else false;
        return if (sparse) .sparse_vector else .dense_vector;
    }
    return error.UnsupportedCreateTableRequest;
}

fn extractIndexConfigJson(alloc: std.mem.Allocator, index_name: []const u8, value: std.json.Value) ![]u8 {
    const managed_embedder = @import("../../inference/managed_embedder.zig");
    if (value != .object) return try alloc.dupe(u8, "{}");
    switch (try parseIndexKind(value)) {
        .dense_vector, .sparse_vector => return try managed_embedder.translateEmbeddingsIndexConfigJson(alloc, index_name, value),
        else => {},
    }

    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try out.append(alloc, '{');
    var first = true;
    var it = value.object.iterator();
    while (it.next()) |entry| {
        if (std.mem.eql(u8, entry.key_ptr.*, "type") or
            std.mem.eql(u8, entry.key_ptr.*, "name") or
            std.mem.eql(u8, entry.key_ptr.*, "description") or
            std.mem.eql(u8, entry.key_ptr.*, "version") or
            std.mem.eql(u8, entry.key_ptr.*, "enrichments"))
        {
            continue;
        }
        if (!first) try out.append(alloc, ',');
        first = false;
        try appendJsonString(alloc, &out, entry.key_ptr.*);
        try out.append(alloc, ':');
        const encoded = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(entry.value_ptr.*, .{})});
        defer alloc.free(encoded);
        try out.appendSlice(alloc, encoded);
    }
    try out.append(alloc, '}');
    return try out.toOwnedSlice(alloc);
}

fn appendJsonString(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), value: []const u8) !void {
    const escaped = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(value, .{})});
    defer alloc.free(escaped);
    try out.appendSlice(alloc, escaped);
}

test "backup restore bootstrap deduplicates exact content across source aliases while a reader is resident" {
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
        \\{"format_version":1,"backup_id":"backup-1701","location":"s3://backup/antfly","artifact_sha256":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","snapshot_path":"backup-1701/groups/1701.afb","group_id":1701,"phase":"complete","primary_restored":true,"runtime_repair_complete":true,"last_error":""}
    );

    var resident_read = (try db_mod.generation_lifecycle.acquirePublishedGenerationRead(alloc, path)) orelse
        return error.TestUnexpectedResult;
    defer resident_read.deinit();

    const exact: @import("../catalog.zig").BackupRestoreBootstrapRecord = .{
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
