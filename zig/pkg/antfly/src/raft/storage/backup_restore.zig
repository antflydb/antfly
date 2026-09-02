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
const CancellationToken = @import("../../common/cancellation.zig").CancellationToken;

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
    expected_native_manifest_size_bytes: u64 = 0,
    expected_native_manifest_sha256: []const u8 = "",
    manifest: ?*const backups_api.TableBackupManifest = null,
    /// Server restore admission, generation publication, and cleanup must use
    /// the same bounded runtime that owns the database backend.
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime = null,
    io: ?std.Io = null,
    cancellation: CancellationToken = .none,
    open_options: backups_api.OpenOptions = .{},
};

const RestoreIoScope = struct {
    alloc: std.mem.Allocator,
    io_value: std.Io,
    owned: ?*std.Io.Threaded = null,

    fn init(alloc: std.mem.Allocator, restore: RestoreSource) !RestoreIoScope {
        if (restore.backend_runtime) |runtime| {
            if (runtime.filesystemIo()) |runtime_io|
                return .{ .alloc = alloc, .io_value = runtime_io };
        }
        if (restore.open_options.io orelse restore.io) |shared_io| {
            return .{ .alloc = alloc, .io_value = shared_io };
        }
        if (restore.backend_runtime != null) return error.BackendRuntimeIoUnavailable;
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
    if (restore.expected_native_manifest_size_bytes != shard.native_manifest_size_bytes or
        !std.mem.eql(u8, restore.expected_native_manifest_sha256, shard.native_manifest_sha256))
    {
        return error.RestoreArtifactIdentityMismatch;
    }
}

pub const RestoreOptions = struct {
    expected_table_name: ?[]const u8 = null,
    expected_identity_namespace: ?doc_identity.Namespace = null,
};

/// One unpublished restore generation and the immutable backend decision used
/// to materialize it. Native restores must carry the decision through every
/// candidate open; reconstructing OpenOptions later can invoke a path-sensitive
/// configurator against the sibling staging path or select a different storage
/// topology after the artifact has already passed admission.
pub const PreparedRestore = struct {
    _generation: db_mod.generation_lifecycle.StagedGeneration,
    _native_open_plan: ?db_mod.NativeRestoreOpenPlan = null,

    pub fn deinit(self: *@This()) void {
        self._generation.deinit();
    }

    pub fn path(self: *const @This()) []const u8 {
        return self._generation.path();
    }

    pub fn stagedGeneration(self: *const @This()) *const db_mod.generation_lifecycle.StagedGeneration {
        return &self._generation;
    }

    pub fn nativeOpenPlan(self: *const @This()) ?*const db_mod.NativeRestoreOpenPlan {
        if (self._native_open_plan) |*plan| return plan;
        return null;
    }

    pub fn seal(self: *@This()) !void {
        try self._generation.seal();
    }
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
    // Provisioning and Raft admission may discover the same restore intent on
    // adjacent control turns. Check the published generation under a read
    // lease before requesting an exclusive transition: once the exact import
    // identity is durable, the managed repair owner is allowed to keep that
    // generation open while it advances restartable runtime-repair phases.
    // Requiring repair completion here would make the idempotent caller fight
    // that owner forever with GenerationTransitionActive.
    if (try publishedRestoreAlreadyImported(alloc, path, group_id, restore_with_io)) return;
    var transition = try db_mod.generation_lifecycle.beginProcessExclusiveWithRuntimeAndIo(
        path,
        restore.backend_runtime,
        io_scope.io(),
    );
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
    try repairPreparedRestoreUntilComplete(alloc, &prepared, restore, options);
    const outcome = try publishPreparedRestore(alloc, path, &prepared);
    if (outcome == .durability_uncertain) return error.GenerationDurabilityUncertain;
}

fn repairPreparedRestoreUntilComplete(
    alloc: std.mem.Allocator,
    prepared: *const PreparedRestore,
    restore: RestoreSource,
    options: RestoreOptions,
) !void {
    var io_scope = try RestoreIoScope.init(alloc, restore);
    defer io_scope.deinit();
    const io = io_scope.io();
    var state = (try db_mod.DB.readRestoreStateForPathWithIo(alloc, io, prepared.path())) orelse return;
    defer state.deinit(alloc);
    const open_options = try preparedRestoreOpenOptionsForRepair(prepared, restore, options);
    var restored = try db_mod.DB.open(alloc, prepared.path(), open_options);
    defer restored.close();
    // Backup artifacts contain the source replica's applied-entry fence. A
    // restored generation is admitted into a distinct Raft history even when
    // metadata intentionally preserves the logical group id, so publishing
    // that fence would make the first destination entry look conflicting (or
    // already applied). Clear it on the isolated candidate before either
    // portable deferred repair or native inline repair can publish the root.
    try restored.clearRaftAppliedEntry();
    // Legacy/portable repair may require table-managed provider wiring and is
    // completed by the provisioning restore job. Native selective repair is
    // self-contained and must finish before this lower-level bootstrap can
    // publish the candidate.
    if (!std.mem.eql(u8, state.phase, "repair_indexes")) return;

    var repair_cancellation = db_mod.types.RepairCancellation{ .token = restore.cancellation };
    while (try restored.restoreRuntimeRepairNeeded()) {
        try restore.cancellation.check();
        _ = restored.repairRestoreRuntimeStateStepIfNeededWithIoAndRepairOptions(
            alloc,
            io,
            .{ .cancel_check = repair_cancellation.check() },
        ) catch |err| switch (err) {
            error.RestoreRuntimeRepairIncomplete,
            error.RestoreDenseArtifactRebuildIncomplete,
            error.RestoreDenseConfigProofIncomplete,
            error.RestoreDenseCounterProofIncomplete,
            error.RestoreDenseIndexProofIncomplete,
            error.RestoreDenseCoverageProofIncomplete,
            error.RestoreDenseCheckpointIncomplete,
            error.RestoreIndexAvailabilityIncomplete,
            => {
                io.sleep(std.Io.Duration.fromMilliseconds(100), .awake) catch |sleep_err| switch (sleep_err) {
                    error.Canceled => std.Io.recancel(io),
                };
                continue;
            },
            else => return err,
        };
    }
    try restored.syncIndexes(true);
}

fn preparedRestoreOpenOptionsForRepair(
    prepared: *const PreparedRestore,
    restore: RestoreSource,
    options: RestoreOptions,
) !db_mod.OpenOptions {
    const native_open_plan = prepared.nativeOpenPlan();
    var open_options = if (native_open_plan) |plan|
        try plan.optionsForStagedGeneration(prepared.stagedGeneration())
    else
        db_mod.OpenOptions{ .backend_runtime = restore.backend_runtime };
    // These are repair execution policies, not backend topology. Apply them
    // after native admission while retaining the plan's storage capabilities.
    open_options.open_mode = .writer_no_replay;
    open_options.identity_namespace = options.expected_identity_namespace;
    open_options.prefer_existing_identity_namespace = true;
    open_options.staged_generation = prepared.stagedGeneration();
    open_options.start_index_workers = false;
    open_options.start_optional_runtimes = false;
    open_options.start_optional_runtime_workers = false;
    open_options.ha_write_gate = null;
    open_options.ha_async_effect_mirror = null;
    open_options.ha_async_batch_mirror = null;
    open_options.ha_async_metadata_mirror = null;
    open_options.ttl_cleanup = .{ .enabled = false };
    open_options.transaction_recovery = .{ .enabled = false };
    open_options.text_merge = .{ .enabled = false };
    return open_options;
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
) !?PreparedRestore {
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
) !?PreparedRestore {
    return try prepareRestoreSnapshotIfNeeded(preparation, alloc, path, group_id, restore, options);
}

pub fn publishPreparedRestore(
    alloc: std.mem.Allocator,
    path: []const u8,
    prepared: *PreparedRestore,
) !db_mod.generation_lifecycle.PublicationOutcome {
    try prepared._generation.validateLivePath(path);
    const outcome = try prepared._generation.publish();
    cleanupSnapshotsForPublishedRestore(alloc, prepared._generation.io orelse std.Options.debug_io, path);
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
        state.native_manifest_size_bytes != restore.expected_native_manifest_size_bytes or
        !std.mem.eql(u8, state.native_manifest_sha256, restore.expected_native_manifest_sha256) or
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

fn publishedRestoreAlreadyImported(
    alloc: std.mem.Allocator,
    path: []const u8,
    group_id: u64,
    restore: RestoreSource,
) !bool {
    var generation_read = (try db_mod.generation_lifecycle.acquirePublishedGenerationRead(alloc, path)) orelse
        return false;
    defer generation_read.deinit();
    validateImportedRestoreIdentity(alloc, path, group_id, restore) catch |err| switch (err) {
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
        state.native_manifest_size_bytes != restore.expected_native_manifest_size_bytes or
        !std.mem.eql(u8, state.native_manifest_sha256, restore.expected_native_manifest_sha256) or
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
        .expected_native_manifest_size_bytes = restore.native_manifest_size_bytes,
        .expected_native_manifest_sha256 = restore.native_manifest_sha256,
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
) !?PreparedRestore {
    try restore.cancellation.check();
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
            state.native_manifest_size_bytes == restore.expected_native_manifest_size_bytes and
            std.mem.eql(u8, state.native_manifest_sha256, restore.expected_native_manifest_sha256) and
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
) !PreparedRestore {
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

    var prepared = PreparedRestore{ ._generation = try transition.beginStaging() };
    errdefer prepared.deinit();
    const staged_generation = prepared.stagedGeneration();
    const staged_path = prepared.path();

    switch (manifest.format) {
        .portable => {
            try applyPortableRestore(
                staged_generation,
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
            return prepared;
        },
        .native => {},
    }

    if (shard.native_manifest_size_bytes != 0) {
        prepared._native_open_plan = try applyManifestNativeRestore(
            staged_generation,
            alloc,
            io,
            group_id,
            restore,
            &location,
            shard,
            options,
        );
        std.log.info("native restore staged generation phase=prepared", .{});
        return prepared;
    }

    // v0.2.0 native snapshots predate the generation manifest. Preserve their
    // released compatibility path, including whole-tree integrity validation;
    // unreleased intermediate manifest schemas are never inferred here.
    const legacy_restore_plan = try db_mod.DB.resolveNativeRestoreOpenPlan(
        path,
        .{
            .identity_namespace = options.expected_identity_namespace,
            .backend_runtime = restore.backend_runtime,
        },
    );
    if (legacy_restore_plan.physicalRootMode() != .filesystem_managed)
        return error.NativeBackupStorageBackendUnsupported;
    const legacy_restore_open_options = try legacy_restore_plan.optionsForStagedGeneration(staged_generation);
    const snapshot_root = try stageRestoreSnapshot(alloc, io, path, &location, snapshot_path, restore.cancellation);
    defer {
        destroyPathIfExistsWithIo(io, snapshot_root);
        alloc.free(snapshot_root);
    }
    try backups_api.verifyRestorableShardArtifactIntegrityWithCancellation(
        alloc,
        io,
        .native,
        snapshot_root,
        shard,
        restore.cancellation,
    );

    std.log.info("native restore staged generation phase=materialization", .{});
    try db_mod.DB.restoreSnapshotToDeferredRuntimeRepairWithIoAndCancellation(staged_generation, alloc, io, snapshot_root, staged_path, legacy_restore_open_options, .{
        .backup_id = restore.backup_id,
        .location = restoreIdentityLocation(restore),
        .artifact_sha256 = shard.artifact_sha256,
        .native_manifest_size_bytes = shard.native_manifest_size_bytes,
        .native_manifest_sha256 = shard.native_manifest_sha256,
        .snapshot_path = snapshot_path,
        .group_id = group_id,
    }, restore.cancellation);
    std.log.info("native restore staged generation phase=prepared", .{});
    prepared._native_open_plan = legacy_restore_plan;
    return prepared;
}

fn applyManifestNativeRestore(
    staged_generation: *const db_mod.generation_lifecycle.StagedGeneration,
    alloc: std.mem.Allocator,
    io: std.Io,
    group_id: u64,
    restore: RestoreSource,
    location: *backups_api.BackupLocation,
    shard: *const backups_api.ShardSnapshot,
    options: RestoreOptions,
) !db_mod.NativeRestoreOpenPlan {
    if (shard.native_manifest_size_bytes > db_mod.native_backup.max_manifest_bytes)
        return error.InvalidNativeBackupManifest;
    const manifest_source_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{
        shard.snapshot_path,
        db_mod.native_backup.manifest_file_name,
    });
    defer alloc.free(manifest_source_path);
    const raw = try backups_api.readFileFromLocationUsingIoLimited(
        alloc,
        io,
        location,
        manifest_source_path,
        db_mod.native_backup.max_manifest_bytes,
    );
    defer alloc.free(raw);
    if (raw.len != shard.native_manifest_size_bytes or
        !sha256Matches(raw, shard.native_manifest_sha256))
    {
        return error.BackupArtifactIntegrityMismatch;
    }

    var generation = try db_mod.native_backup.parseManifestBytes(alloc, raw);
    defer generation.deinit();
    if (try db_mod.native_backup.declaredGenerationBytes(
        generation.value(),
        @intCast(raw.len),
    ) != shard.artifact_size_bytes) {
        return error.BackupArtifactIntegrityMismatch;
    }

    const staged_path = staged_generation.path();
    const restore_plan = try db_mod.DB.resolveNativeRestoreOpenPlan(
        staged_generation.livePath(),
        .{
            .identity_namespace = options.expected_identity_namespace,
            .backend_runtime = restore.backend_runtime,
        },
    );
    // Directory exchange cannot publish a composed external namespace. Reject
    // immediately after the authenticated bounded manifest read, before any
    // primary or projection corpus transfer. Portable restore remains the
    // source-portable fallback for these backends.
    if (restore_plan.physicalRootMode() != .filesystem_managed)
        return error.NativeBackupStorageBackendUnsupported;
    const restore_open_options = try restore_plan.optionsForStagedGeneration(staged_generation);
    const logical_primary_root = try std.fmt.allocPrint(alloc, "{s}/.native-primary-import", .{staged_path});
    defer alloc.free(logical_primary_root);
    defer destroyPathIfExistsWithIo(io, logical_primary_root);
    const physical_primary = std.mem.eql(
        u8,
        generation.value().primary.artifact_format,
        "antfly-lsm-checkpoint",
    );

    // Primary and small durable metadata come first. They let the restored
    // catalog classify config/incarnation/checkpoint compatibility before any
    // projection corpus bytes are requested from remote storage.
    for (generation.value().artifacts) |artifact| {
        if (artifact.role == .projection) continue;
        try copyNativeManifestArtifact(
            alloc,
            io,
            restore,
            location,
            shard.snapshot_path,
            staged_path,
            logical_primary_root,
            physical_primary,
            artifact,
        );
    }

    std.log.info("native restore staged generation phase=primary_validation", .{});
    try db_mod.DB.restoreMaterializedNativePrimaryAndPlanProjectionsWithIoAndCancellation(
        staged_generation,
        alloc,
        io,
        if (physical_primary) staged_path else logical_primary_root,
        staged_path,
        restore_open_options,
        .{
            .backup_id = restore.backup_id,
            .location = restoreIdentityLocation(restore),
            .artifact_sha256 = shard.artifact_sha256,
            .native_manifest_size_bytes = shard.native_manifest_size_bytes,
            .native_manifest_sha256 = shard.native_manifest_sha256,
            .snapshot_path = shard.snapshot_path,
            .group_id = group_id,
        },
        &generation,
        restore.cancellation,
    );

    // Transfer only artifact groups which survived static and restored-catalog
    // planning. A single missing/corrupt member invalidates its projection and
    // causes every later member of that group to be skipped.
    for (generation.value().artifacts) |artifact| {
        if (artifact.role != .projection or generation.projectionInvalid(artifact.projection_name)) continue;
        copyNativeManifestArtifact(
            alloc,
            io,
            restore,
            location,
            shard.snapshot_path,
            staged_path,
            logical_primary_root,
            physical_primary,
            artifact,
        ) catch |err| switch (err) {
            error.NativeBackupArtifactMissing => {
                try generation.invalidateProjection(artifact.projection_name, .missing_artifact);
            },
            error.NativeBackupArtifactIntegrityMismatch => {
                try generation.invalidateProjection(artifact.projection_name, .integrity_mismatch);
            },
            else => return err,
        };
    }
    try generation.discardInvalidProjectionArtifacts(io, staged_path);

    std.log.info("native restore staged generation phase=physical_validation", .{});
    try db_mod.DB.finishMaterializedNativeGenerationWithIo(
        staged_generation,
        alloc,
        io,
        staged_path,
        restore_open_options,
        &generation,
        true,
    );
    return restore_plan;
}

fn copyNativeManifestArtifact(
    alloc: std.mem.Allocator,
    io: std.Io,
    restore: RestoreSource,
    location: *backups_api.BackupLocation,
    snapshot_path: []const u8,
    staged_path: []const u8,
    logical_primary_root: []const u8,
    physical_primary: bool,
    artifact: db_mod.native_backup.Artifact,
) !void {
    try restore.cancellation.check();
    const source_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ snapshot_path, artifact.path });
    defer alloc.free(source_path);
    const materialized_path = if (artifact.role == .primary and physical_primary)
        artifact.path["primary-lsm/".len..]
    else
        artifact.path;
    const destination_root = if (artifact.role == .primary and !physical_primary)
        logical_primary_root
    else
        staged_path;
    const destination_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ destination_root, materialized_path });
    defer alloc.free(destination_path);
    backups_api.copyFileFromLocationVerifiedUsingIo(
        alloc,
        io,
        location,
        source_path,
        destination_path,
        artifact.size_bytes,
        artifact.sha256,
        restore.cancellation,
    ) catch |err| switch (err) {
        error.FileNotFound => return error.NativeBackupArtifactMissing,
        error.BackupArtifactIntegrityMismatch => return error.NativeBackupArtifactIntegrityMismatch,
        else => return err,
    };
}

fn sha256Matches(bytes: []const u8, expected: []const u8) bool {
    if (expected.len != std.crypto.hash.sha2.Sha256.digest_length * 2) return false;
    var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
    const actual = std.fmt.bytesToHex(digest, .lower);
    return std.mem.eql(u8, &actual, expected);
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
        .backend_runtime = restore.backend_runtime,
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
    try db_mod.DB.markRestorePrimaryRestoredForPathWithIdentityWithIo(alloc, io, path, .{
        .backup_id = restore.backup_id,
        .location = restoreIdentityLocation(restore),
        .artifact_sha256 = shard.artifact_sha256,
        .native_manifest_size_bytes = shard.native_manifest_size_bytes,
        .native_manifest_sha256 = shard.native_manifest_sha256,
        .snapshot_path = shard.snapshot_path,
        .group_id = group_id,
    });
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
    cancellation: CancellationToken,
) ![]u8 {
    const staging_root = try std.fmt.allocPrint(alloc, "{s}.restore-source", .{path});
    errdefer alloc.free(staging_root);
    destroyPathIfExistsWithIo(io, staging_root);
    errdefer destroyPathIfExistsWithIo(io, staging_root);
    try backups_api.copyDirectoryFromLocationUsingIoWithCancellation(
        alloc,
        io,
        location,
        snapshot_path,
        staging_root,
        cancellation,
    );
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

test "restore binding pins the authenticated native generation manifest" {
    const artifact_sha256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    const native_sha256 = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    const shard = backups_api.ShardSnapshot{
        .group_id = 7,
        .start_key = "",
        .snapshot_path = "backup/groups/7",
        .artifact_size_bytes = 4096,
        .artifact_sha256 = artifact_sha256,
        .native_manifest_size_bytes = 512,
        .native_manifest_sha256 = native_sha256,
    };
    const exact = RestoreSource{
        .backup_id = "backup",
        .artifact_backup_id = "backup-artifacts",
        .location = "file:///tmp/backup",
        .snapshot_path = shard.snapshot_path,
        .authority = .staged_local,
        .expected_artifact_size_bytes = shard.artifact_size_bytes,
        .expected_artifact_sha256 = artifact_sha256,
        .expected_native_manifest_size_bytes = shard.native_manifest_size_bytes,
        .expected_native_manifest_sha256 = native_sha256,
    };
    try validateExpectedArtifactBinding(exact, &shard);

    var changed_inner_manifest = exact;
    changed_inner_manifest.expected_native_manifest_sha256 = artifact_sha256;
    try std.testing.expectError(
        error.RestoreArtifactIdentityMismatch,
        validateExpectedArtifactBinding(changed_inner_manifest, &shard),
    );
}

test "prepared native restore repair reuses target backend admission" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/prepared-native-restore-plan", .{tmp.sub_path});
    defer alloc.free(path);
    defer destroyPathIfExists(path);

    const ConfiguratorContext = struct {
        expected_path: []const u8,
        calls: usize = 0,

        fn configure(ptr: *anyopaque, configured_path: []const u8, opaque_options: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const opts: *db_mod.OpenOptions = @ptrCast(@alignCast(opaque_options));
            if (!std.mem.eql(u8, configured_path, self.expected_path)) return error.UnexpectedConfiguredPath;
            self.calls += 1;
            opts.start_index_workers = false;
            opts.start_optional_runtimes = false;
            opts.ttl_cleanup = .{ .enabled = false };
        }
    };
    var context = ConfiguratorContext{ .expected_path = path };
    var runtime = try db_mod.background_runtime.BackendRuntimeHandle.init(alloc, .{ .backend = .io_threaded });
    defer runtime.deinit();
    runtime.ptr().db_open_configurator = .{
        .ptr = &context,
        .configure_fn = ConfiguratorContext.configure,
    };
    const native_plan = try db_mod.DB.resolveNativeRestoreOpenPlan(path, .{ .backend_runtime = runtime.ptr() });
    try std.testing.expectEqual(@as(usize, 1), context.calls);

    var transition = try db_mod.generation_lifecycle.beginProcessExclusiveWithRuntime(path, runtime.ptr());
    defer transition.deinit();
    var prepared = PreparedRestore{
        ._generation = try transition.beginStaging(),
        ._native_open_plan = native_plan,
    };
    defer prepared.deinit();
    const restore = RestoreSource{
        .backup_id = "backup",
        .artifact_backup_id = "artifact",
        .location = "file:///unused",
        .snapshot_path = "unused",
        .authority = .staged_local,
        .expected_artifact_size_bytes = 0,
        .expected_artifact_sha256 = "",
        .backend_runtime = runtime.ptr(),
    };
    const repair_options = try preparedRestoreOpenOptionsForRepair(&prepared, restore, .{});
    try std.testing.expectEqual(prepared.stagedGeneration(), repair_options.staged_generation.?);
    var db = try db_mod.DB.open(alloc, prepared.path(), repair_options);
    defer db.close();
    try std.testing.expectEqual(@as(usize, 1), context.calls);
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
