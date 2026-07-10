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
const backups_api = @import("../../api/backups.zig");
const db_mod = @import("../../storage/db/mod.zig");
const doc_identity = @import("../../storage/db/doc_identity.zig");
const lsm_table_file = @import("../../storage/lsm/table_file.zig");
const portable_backup = @import("../../storage/portable_backup.zig");

pub const RestoreSource = struct {
    backup_id: []const u8,
    location: []const u8,
    snapshot_path: []const u8,
};

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
    var transition = try db_mod.generation_lifecycle.beginProcessExclusive(path);
    defer transition.deinit();
    try applyRestoreSnapshotToPathWithExclusiveTransition(&transition, alloc, path, group_id, restore, options);
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

pub fn publishPreparedRestore(
    alloc: std.mem.Allocator,
    path: []const u8,
    prepared: *db_mod.generation_lifecycle.StagedGeneration,
) !db_mod.generation_lifecycle.PublicationOutcome {
    try prepared.validateLivePath(path);
    const outcome = try prepared.publish();
    cleanupSnapshotsForPublishedRestore(alloc, path);
    return outcome;
}

pub fn restoreSnapshotMatchesPath(
    alloc: std.mem.Allocator,
    path: []const u8,
    restore: RestoreSource,
    options: RestoreOptions,
) !bool {
    const snapshot_doc_count = try restoreSnapshotDocCount(alloc, restore);

    var db = db_mod.DB.open(alloc, path, .{
        .identity_namespace = options.expected_identity_namespace,
        .open_mode = .query_readonly,
        .start_index_workers = false,
        .start_optional_runtimes = false,
    }) catch |err| switch (err) {
        error.FileNotFound, error.IdentityNamespaceMismatch => return false,
        else => return err,
    };
    defer db.close();

    const stats = try db.stats(alloc);
    defer db_mod.types.freeDBStats(alloc, stats);
    return stats.doc_count == snapshot_doc_count;
}

pub fn applyBackupRestoreFromRecord(
    alloc: std.mem.Allocator,
    replica_root_dir: []const u8,
    group_id: u64,
    restore: @import("../catalog.zig").BackupRestoreBootstrapRecord,
) !void {
    try applyRestoreSnapshotToReplicaRoot(alloc, replica_root_dir, group_id, .{
        .backup_id = restore.backup_id,
        .location = restore.location,
        .snapshot_path = restore.snapshot_path,
    }, null);
}

fn prepareRestoreSnapshotIfNeeded(
    transition: *db_mod.generation_lifecycle.ExclusiveTransition,
    alloc: std.mem.Allocator,
    path: []const u8,
    group_id: u64,
    restore: RestoreSource,
    options: RestoreOptions,
) !?db_mod.generation_lifecycle.StagedGeneration {
    if (try db_mod.DB.readRestoreStateForPath(alloc, path)) |state_value| {
        var state = state_value;
        defer state.deinit(alloc);
        if (state.primary_restored and
            std.mem.eql(u8, state.backup_id, restore.backup_id) and
            std.mem.eql(u8, state.location, restore.location) and
            std.mem.eql(u8, state.snapshot_path, restore.snapshot_path) and
            state.group_id == group_id)
        {
            if (!state.runtime_repair_complete or
                try restoreSnapshotMatchesPath(alloc, path, restore, options))
            {
                return null;
            }
        }
    }

    return try prepareRestoreSnapshot(transition, alloc, path, group_id, restore, options);
}

fn prepareRestoreSnapshot(
    transition: *db_mod.generation_lifecycle.ExclusiveTransition,
    alloc: std.mem.Allocator,
    path: []const u8,
    group_id: u64,
    restore: RestoreSource,
    options: RestoreOptions,
) !db_mod.generation_lifecycle.StagedGeneration {
    var location = try backups_api.openBackupLocation(alloc, restore.location);
    defer location.deinit(alloc);
    var manifest = try backups_api.readManifestFromLocation(alloc, &location, restore.backup_id);
    defer manifest.deinit(alloc);
    if (options.expected_table_name) |table_name| {
        if (!std.mem.eql(u8, manifest.table_name, table_name)) {
            std.log.err("restore manifest table mismatch expected={s} actual={s} backup_id={s} snapshot_path={s}", .{
                table_name,
                manifest.table_name,
                restore.backup_id,
                restore.snapshot_path,
            });
            return error.InvalidBackupRequest;
        }
    }
    if (manifest.read_schema_json.len > 0) return error.UnsupportedBackupMigrationState;
    const snapshot_path = if (restore.snapshot_path.len > 0)
        restore.snapshot_path
    else blk: {
        const shard = backups_api.findShardSnapshot(&manifest, group_id) orelse return error.UnsupportedBackupFormat;
        break :blk shard.snapshot_path;
    };

    var staged_generation = try transition.beginStaging();
    errdefer staged_generation.deinit();
    const staged_path = staged_generation.path();

    if (std.mem.endsWith(u8, snapshot_path, ".afb")) {
        try applyPortableRestore(alloc, staged_path, group_id, restore, snapshot_path, &manifest, options);
        return staged_generation;
    }

    const snapshot_root = try stageRestoreSnapshot(alloc, path, &location, snapshot_path);
    defer switch (location) {
        .file => alloc.free(snapshot_root),
        .remote => {
            destroyPathIfExists(snapshot_root);
            alloc.free(snapshot_root);
        },
    };

    std.log.info("native restore build staged generation live_path={s} staged_path={s} snapshot_root={s}", .{ path, staged_path, snapshot_root });
    try db_mod.DB.restoreSnapshotToDeferredRuntimeRepair(&staged_generation, alloc, snapshot_root, staged_path, .{
        .identity_namespace = options.expected_identity_namespace,
    }, .{
        .backup_id = restore.backup_id,
        .location = restore.location,
        .snapshot_path = snapshot_path,
        .group_id = group_id,
    });
    std.log.info("native restore generation prepared live_path={s} staged_path={s} snapshot_root={s}", .{ path, staged_path, snapshot_root });
    return staged_generation;
}

fn restoreSnapshotDocCount(alloc: std.mem.Allocator, restore: RestoreSource) !u64 {
    var location = try backups_api.openBackupLocation(alloc, restore.location);
    defer location.deinit(alloc);

    if (std.mem.endsWith(u8, restore.snapshot_path, ".afb")) {
        const afb_path = try stageRestoreFile(alloc, "", &location, restore.snapshot_path);
        defer switch (location) {
            .file => alloc.free(afb_path),
            .remote => {
                if (std.fs.path.dirname(afb_path)) |staging_dir| destroyPathIfExists(staging_dir);
                alloc.free(afb_path);
            },
        };
        return try portableSnapshotDocCount(alloc, afb_path);
    }

    const snapshot_root = try stageRestoreSnapshot(alloc, "", &location, restore.snapshot_path);
    defer switch (location) {
        .file => alloc.free(snapshot_root),
        .remote => {
            destroyPathIfExists(snapshot_root);
            alloc.free(snapshot_root);
        },
    };

    const snapshot_path = try std.fmt.allocPrint(alloc, "{s}/store.bin", .{snapshot_root});
    defer alloc.free(snapshot_path);

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const raw = try std.Io.Dir.cwd().readFileAlloc(io_impl.io(), snapshot_path, alloc, .limited(256 * 1024 * 1024));
    defer alloc.free(raw);

    var decoded = try lsm_table_file.decodeAlloc(alloc, raw);
    defer decoded.deinit(alloc);
    return @intCast(decoded.entries.len);
}

fn applyPortableRestore(
    alloc: std.mem.Allocator,
    path: []const u8,
    group_id: u64,
    restore: RestoreSource,
    snapshot_path: []const u8,
    manifest: *const backups_api.TableBackupManifest,
    options: RestoreOptions,
) !void {
    const embedding_source_fields = try portableEmbeddingSourceFieldsFromIndexesJson(alloc, manifest.indexes_json);
    defer freePortableEmbeddingSourceFields(alloc, embedding_source_fields);
    var location = try backups_api.openBackupLocation(alloc, restore.location);
    defer location.deinit(alloc);
    const afb_path = try stageRestoreFile(alloc, path, &location, snapshot_path);
    defer switch (location) {
        .file => alloc.free(afb_path),
        .remote => {
            destroyPathIfExists(afb_path);
            alloc.free(afb_path);
        },
    };

    const afb_data = try readFileAlloc(alloc, afb_path, 16 * 1024 * 1024 * 1024);
    defer alloc.free(afb_data);

    var db = try db_mod.DB.open(alloc, path, .{
        .identity_namespace = options.expected_identity_namespace,
        .start_index_workers = false,
    });
    var db_closed = false;
    defer if (!db_closed) db.close();
    try portable_backup.importPortableWithOptions(alloc, db.core.store, afb_data, .{
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
    try db_mod.DB.markRestorePrimaryRestoredForPath(alloc, path, restore.backup_id, restore.location, snapshot_path, group_id);
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

fn portableSnapshotDocCount(alloc: std.mem.Allocator, afb_path: []const u8) !u64 {
    const afb_data = try readFileAlloc(alloc, afb_path, 16 * 1024 * 1024 * 1024);
    defer alloc.free(afb_data);
    var reader = @import("../../storage/backup_codec.zig").SliceReader.init(afb_data);
    _ = try reader.readHeader();
    var count: u64 = 0;
    while (reader.pos < reader.data.len) {
        const block = try reader.readBlock(alloc);
        defer alloc.free(block.payload);
        if (block.block_type == .document_batch) {
            const entries = try @import("../../storage/backup_codec.zig").decodeDocumentBatch(alloc, block.payload);
            defer {
                for (entries) |entry| {
                    alloc.free(entry.key);
                    alloc.free(entry.value);
                }
                alloc.free(entries);
            }
            count += @intCast(entries.len);
        }
    }
    return count;
}

fn stageRestoreSnapshot(
    alloc: std.mem.Allocator,
    path: []const u8,
    location: *backups_api.BackupLocation,
    snapshot_path: []const u8,
) ![]u8 {
    return switch (location.*) {
        .file => |backup_root| try std.fmt.allocPrint(alloc, "{s}/{s}", .{ backup_root, snapshot_path }),
        .remote => blk: {
            const staging_root = try std.fmt.allocPrint(alloc, "{s}.restore-staging", .{path});
            errdefer alloc.free(staging_root);
            destroyPathIfExists(staging_root);
            try backups_api.copyDirectoryFromLocation(alloc, location, snapshot_path, staging_root);
            break :blk staging_root;
        },
    };
}

fn stageRestoreFile(
    alloc: std.mem.Allocator,
    path: []const u8,
    location: *backups_api.BackupLocation,
    snapshot_path: []const u8,
) ![]u8 {
    return switch (location.*) {
        .file => |backup_root| try std.fmt.allocPrint(alloc, "{s}/{s}", .{ backup_root, snapshot_path }),
        .remote => blk: {
            const staging_path = try std.fmt.allocPrint(alloc, "{s}.restore-staging/{s}", .{ path, std.fs.path.basename(snapshot_path) });
            errdefer alloc.free(staging_path);
            const staging_dir = std.fs.path.dirname(staging_path) orelse return error.InvalidBackupRequest;
            destroyPathIfExists(staging_dir);
            try ensureDirPath(staging_dir);
            try backups_api.copyFileFromLocation(alloc, location, snapshot_path, staging_path);
            break :blk staging_path;
        },
    };
}

fn cleanupSnapshotsForPublishedRestore(alloc: std.mem.Allocator, path: []const u8) void {
    const snapshot_dir = std.fmt.allocPrint(alloc, "{s}.snapshots", .{path}) catch return;
    defer alloc.free(snapshot_dir);
    destroyPathIfExists(snapshot_dir);
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
