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

//! Local portable generation construction and atomic publication. Remote
//! backup locations and server promotion manifests stay in restore_staging.
const std = @import("std");
const builtin = @import("builtin");
const platform_time = @import("antfly_platform").time;

const Allocator = std.mem.Allocator;
pub const connection = @import("connection.zig");
pub const db_mod = @import("antfly_source_root").antfly_sources.physical_db;
pub const fs_paths = @import("antfly_runtime_fs").fs_paths;
pub const LiteDb = connection.Connection;
var portable_generation_nonce: u64 = 0;
pub const portable_generation_lease_magic = "antfly-lite-portable-generation-v1\n";
var test_fail_published_file_directory_sync: std.atomic.Value(bool) = .init(false);

const PreparedGenerationAdoption = struct {
    live: *@import("backend.zig").Handle,
    prepared: *@import("backend.zig").Handle,

    fn adopt(context: PreparedGenerationAdoption) !db_mod.DB.PortableGenerationPublication {
        return switch (try context.live.replaceWithPreparedGeneration(context.prepared)) {
            .complete => .complete,
            .durability_unknown => .durability_unknown,
        };
    }
};

/// Confirms that a previously renamed Lite file is crash-durable. A failure
/// occurs after the rename commit point and must therefore be translated by
/// callers to `DurabilityOutcomeUnknown`, never to a normal retryable error.
pub fn confirmPublishedFileDurability(io: std.Io, path: []const u8) !void {
    if (builtin.is_test and test_fail_published_file_directory_sync.swap(false, .acq_rel)) {
        return error.InjectedPublishedFileDirectorySyncFailure;
    }
    try fs_paths.syncDirPortable(io, std.fs.path.dirname(path) orelse ".");
}

pub fn failNextPublishedFileDirectorySyncForTest() void {
    std.debug.assert(builtin.is_test);
    test_fail_published_file_directory_sync.store(true, .release);
}

pub fn isImportTargetEmpty(allocator: Allocator, db: *db_mod.DB) !bool {
    return try db.isPortableImportTargetEmpty(allocator);
}

pub fn finalizeRestoredLiteDb(allocator: Allocator, db: *db_mod.DB) !void {
    var profile = RestoreWorkProfile.init();
    _ = try db.rebuildDenseIndexesForTargetCoverage(allocator);
    profile.mark(db, "rebuildDenseIndexesForTargetCoverage");
    _ = try db.rebuildSparseIndexesForTargetCoverage(allocator);
    profile.mark(db, "rebuildSparseIndexesForTargetCoverage");
    try db.rebuildGraphIndexesForTargetCoverage(allocator);
    profile.mark(db, "rebuildGraphIndexesForTargetCoverage");
    _ = try db.replayGeneratedEnrichmentsFromStoredDocs(allocator);
    profile.mark(db, "replayGeneratedEnrichmentsFromStoredDocs");
    try db.runUntilIdle();
    profile.mark(db, "runUntilIdle");
    try db.sync(true);
    profile.mark(db, "sync");
    try db.syncIndexes(true);
    profile.mark(db, "syncIndexes");
}

/// Populates a disposable Lite generation. All archive writes are bounded by
/// portable block size; callers must delete the file if this returns an error.
pub fn populateUnpublishedLiteDb(allocator: Allocator, db: *db_mod.DB, backup: []const u8) !void {
    var profile = RestoreWorkProfile.init();
    try db.importPortableIntoUnpublishedEmpty(allocator, backup, connection.embeddedRootIdentity());
    profile.mark(db, "import");
    try finalizeRestoredLiteDb(allocator, db);
}

/// File-backed population path for bounded-memory CLI restores.
pub fn populateUnpublishedLiteDbFromPortableFile(
    allocator: Allocator,
    db: *db_mod.DB,
    io: std.Io,
    file: std.Io.File,
    file_size: u64,
) !void {
    try db.importPortableFileIntoUnpublishedEmpty(
        allocator,
        io,
        file,
        file_size,
        connection.embeddedRootIdentity(),
    );
    try finalizeRestoredLiteDb(allocator, db);
}

/// Builds and finalizes a complete sibling generation, then atomically adopts
/// it into an already-open, strictly pristine Lite DB. All expensive/failure-
/// prone index and enrichment construction precedes the publication boundary.
pub fn importPortableIntoLiteDb(
    allocator: Allocator,
    db: *db_mod.DB,
    lite_backend: *@import("backend.zig").Handle,
    backup: []const u8,
) !void {
    if (!(try db.isPortableImportTargetEmpty(allocator))) return error.LiteImportTargetNotEmpty;
    if (lite_backend.engine != .native_single_file) return error.UnsupportedOperation;

    const target_path = lite_backend.native_docstore.?.file.path;
    const nonce = @atomicRmw(u64, &portable_generation_nonce, .Add, 1, .monotonic);
    const tmp_path = try std.fmt.allocPrint(
        allocator,
        "{s}.portable-restore-{d}-{x}.aflite",
        .{ target_path, platform_time.monotonicNs(), nonce },
    );
    defer allocator.free(tmp_path);

    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    scavengePortableRestoreGenerations(allocator, io, target_path) catch |err| {
        std.log.warn("Lite portable restore scavenging failed target={s} class={s}", .{ target_path, @errorName(err) });
    };
    deleteFileIfExists(io, tmp_path) catch {};
    const tmp_lock_path = try std.fmt.allocPrint(allocator, "{s}.lock", .{tmp_path});
    defer allocator.free(tmp_lock_path);
    defer deleteFileIfExists(io, tmp_lock_path) catch {};
    errdefer deleteFileIfExists(io, tmp_path) catch {};

    var workspace = try lite_backend.native_docstore.?.reserveGenerationWorkspace();
    defer {
        // Close the prepared owner (later defers) and remove failed staging
        // bytes before making their reservation available to live appends.
        deleteFileIfExists(io, tmp_path) catch {};
        workspace.deinit();
    }
    var prepared = try LiteDb.createWithOptions(allocator, tmp_path, true, .{
        .reclamation = workspace.options,
        .fsync = !lite_backend.native_docstore.?.file.no_sync,
        .writer_lock_marker = portable_generation_lease_magic,
    });
    var prepared_live = true;
    errdefer if (prepared_live) prepared.close();
    try populateUnpublishedLiteDb(allocator, &prepared.db, backup);
    var prepared_runtime = try db.preparePortableRuntimeMetadata(
        prepared.db.core.store,
        connection.embeddedRootIdentity(),
    );
    defer prepared_runtime.deinit();

    // Quiesce the prepared runtime before moving its native file descriptor.
    // Finalization above already synced both primary and derived state.
    prepared.db.close();
    var prepared_backend = prepared.backend;
    prepared_live = false;
    prepared = undefined;
    defer prepared_backend.deinit();

    try db.adoptPreparedPortableGenerationIfEmpty(
        allocator,
        &prepared_runtime,
        PreparedGenerationAdoption{
            .live = lite_backend,
            .prepared = &prepared_backend,
        },
        PreparedGenerationAdoption.adopt,
    );
}

pub fn deleteFileIfExists(io: std.Io, path: []const u8) !void {
    if (std.fs.path.isAbsolute(path)) {
        std.Io.Dir.deleteFileAbsolute(io, path) catch |err| switch (err) {
            error.FileNotFound => {},
            else => return err,
        };
    } else {
        std.Io.Dir.cwd().deleteFile(io, path) catch |err| switch (err) {
            error.FileNotFound => {},
            else => return err,
        };
    }
}

pub fn scavengePortableRestoreGenerations(
    allocator: Allocator,
    io: std.Io,
    target_path: []const u8,
) !void {
    const parent = std.fs.path.dirname(target_path) orelse ".";
    const target_name = std.fs.path.basename(target_path);
    const prefix = try std.fmt.allocPrint(allocator, "{s}.portable-restore-", .{target_name});
    defer allocator.free(prefix);

    var dir = try std.Io.Dir.cwd().openDir(io, parent, .{ .iterate = true });
    defer dir.close(io);
    var iterator = dir.iterateAssumeFirstIteration();
    while (try iterator.next(io)) |entry| {
        // Scan leases rather than data files. The writer records the lease
        // marker before creating the native generation, so this also reaps a
        // process that died in that otherwise easy-to-miss interval.
        if (entry.kind != .file or
            !std.mem.startsWith(u8, entry.name, prefix) or
            !std.mem.endsWith(u8, entry.name, ".aflite.lock")) continue;

        const lease_path = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ parent, entry.name });
        defer allocator.free(lease_path);
        const candidate_name = entry.name[0 .. entry.name.len - ".lock".len];
        const candidate = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ parent, candidate_name });
        defer allocator.free(candidate);
        var lease = std.Io.Dir.cwd().openFile(io, lease_path, .{
            .mode = .read_write,
            .lock = .exclusive,
            .lock_nonblocking = true,
        }) catch |err| switch (err) {
            error.AccessDenied, error.FileBusy, error.WouldBlock, error.FileLocksUnsupported, error.FileNotFound => continue,
            else => return err,
        };
        var lease_open = true;
        defer if (lease_open) lease.close(io);

        const stat = lease.stat(io) catch continue;
        if (stat.size != portable_generation_lease_magic.len) continue;
        var magic: [portable_generation_lease_magic.len]u8 = undefined;
        _ = lease.readPositionalAll(io, &magic, 0) catch continue;
        if (!std.mem.eql(u8, &magic, portable_generation_lease_magic)) continue;

        try deleteFileIfExists(io, candidate);
        lease.close(io);
        lease_open = false;
        try deleteFileIfExists(io, lease_path);
        std.log.info("removed abandoned Lite portable generation path={s}", .{candidate});
    }
}

pub const RestoreWorkProfile = struct {
    started: u64,
    pub fn init() RestoreWorkProfile {
        return .{ .started = if (builtin.is_test and @import("antfly_platform").env.getenvBool("ANTFLY_TEST_WORK_PROFILE")) platform_time.monotonicNs() else 0 };
    }
    pub fn markTime(self: *RestoreWorkProfile, label: []const u8) void {
        if (self.started == 0) return;
        const now = platform_time.monotonicNs();
        std.debug.print("\nWORK restore {s} ns={d}\n", .{ label, now - self.started });
        self.started = now;
    }

    pub fn mark(self: *RestoreWorkProfile, db: *db_mod.DB, label: []const u8) void {
        if (self.started == 0) return;
        const now = platform_time.monotonicNs();
        const stats = db.core.index_manager.snapshotLsmWriteStats();
        std.debug.print("\nWORK restore {s} ns={d} manifest_writes={d} manifest_bytes={d} wal_resets={d}\n", .{ label, now - self.started, stats.manifest_writes, stats.manifest_bytes, stats.wal_resets });
        self.started = now;
    }
};
