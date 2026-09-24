// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! No-debt pool maintenance. A fixed pair of obsolete generations bounds
//! reader-delayed reclamation; the manifest carries every remaining path so
//! process death never loses deletion authority. This is not a Raft callback.
const std = @import("std");
const repository = @import("repository.zig");
const manifest_set = @import("manifest_set.zig");
const manifest = @import("../lsm/manifest.zig");
const storage_io = @import("storage_io.zig");
const domains = @import("completion_allocator.zig");
const stream = @import("completion_maintenance.zig");
const runtime = @import("runtime.zig");
const wal = @import("wal.zig");
const completion = @import("completion_runtime.zig");
const Allocator = std.mem.Allocator;
pub const Fault = enum { before_publish, after_publish, after_reset };
pub var test_fail_at: ?Fault = null;
fn fault(point: Fault) !void {
    if (@import("builtin").is_test) if (test_fail_at == point) return error.RecoveryRequired;
}

pub const Retired = struct {
    paths: [256]?[]u8 = @splat(null),
    count: usize = 0,

    pub fn deinit(self: *Retired, alloc: Allocator) void {
        for (self.paths[0..self.count]) |path| if (path) |p| alloc.free(p);
        self.* = .{};
    }
    fn add(self: *Retired, alloc: Allocator, path: []const u8) !void {
        for (self.paths[0..self.count]) |old| if (old) |p| if (std.mem.eql(u8, p, path)) return;
        if (self.count == self.paths.len) return error.CompletionPlanCapacityExceeded;
        self.paths[self.count] = try alloc.dupe(u8, path);
        self.count += 1;
    }
    fn compact(self: *Retired) void {
        var count: usize = 0;
        for (self.paths[0..self.count]) |path| if (path) |p| {
            self.paths[count] = p;
            count += 1;
        };
        @memset(self.paths[count..], null);
        self.count = count;
    }
};

fn addSpec(specs: []storage_io.NativeCompletionIo.FileSpec, count: *usize, path: []const u8, bytes: usize, append: bool, delete: bool) !void {
    for (specs[0..count.*]) |*spec| if (std.mem.eql(u8, spec.path, path)) {
        spec.max_bytes = @max(spec.max_bytes, bytes);
        spec.allow_append = spec.allow_append or append;
        spec.allow_delete = spec.allow_delete or delete;
        return;
    };
    if (count.* == specs.len) return error.CompletionFileCapacityExceeded;
    specs[count.*] = .{ .path = path, .max_bytes = bytes, .allow_append = append, .allow_delete = delete };
    count.* += 1;
}

pub fn run(comptime Backend: type, pool: anytype, backend: *Backend) !void {
    const Context = struct {
        pool: @TypeOf(pool),
        backend: *Backend,
        fn execute(context: @This(), scratch: Allocator) !void {
            return runWithWorkspace(Backend, context.pool, context.backend, scratch);
        }
    };
    return pool.compiler.withCompletion(void, Context{ .pool = pool, .backend = backend }, Context.execute);
}

fn runWithWorkspace(comptime Backend: type, pool: anytype, backend: *Backend, scratch: Allocator) !void {
    if (pool.failed or backend.manifest_recovery_required or !pool.restored) return error.RecoveryRequired;
    if (pool.maintenance_active or pool.startup_reconciliation_pending or backend.hasDurableCompletions() or backend.activeImmutableMemtableCount() != 0)
        return error.CompletionReservationBusy;
    for (pool.cells[0..pool.cell_count]) |cell| if (cell.phase == .accepted or cell.phase == .prepared) return error.CompletionReservationBusy;
    pool.ready = false;
    pool.capacity_certified = false;
    // Rebinding removes old planned paths. A failed attempt must never let
    // qualifyFresh reopen admission against that obsolete path plan.
    pool.maintenance_pending = true;
    pool.maintenance_active = true;
    defer pool.maintenance_active = false;
    backend.retainReaderKind(.other);
    defer backend.releaseReaderKind(.other);
    const control = pool.control.allocator();
    // Replay/old readers may retain nodes in pool.scratch. The compiler domain
    // is exclusively borrowed and empty at entry, so those nodes cannot steal
    // the maintenance working frontier or fragment its starting span.
    const root = backend.root_dir.?;
    // Paths are an explicitly bounded part of the retained control domain.
    if (root.len > 512) return error.UnsupportedCompletionProfile;
    if (backend.runs.count() > 68) return error.CompletionPlanCapacityExceeded;
    if (!pool.legacy_obsolete_imported) {
        if (backend.obsolete_paths.count() > pool.retired[0].paths.len) return error.CompletionPlanCapacityExceeded;
        var imported: Retired = .{};
        errdefer imported.deinit(control);
        var cursor = backend.obsolete_paths.iterator();
        while (cursor.next()) |item| try imported.add(control, item.path);
        pool.retired[0] = imported;
        pool.legacy_obsolete_imported = true;
    }
    // The old allowlist already includes previous generations after a cycle;
    // startup imports need rebinding before reclamation can access them.
    var specs: [storage_io.NativeCompletionIo.max_prepared_files]storage_io.NativeCompletionIo.FileSpec = undefined;
    var spec_count: usize = 0;
    for (pool.io.files) |file| try addSpec(&specs, &spec_count, file.final, file.max_bytes, file.allow_append, file.allow_delete);
    for (pool.retired) |generation| for (generation.paths[0..generation.count]) |path| if (path) |p|
        try addSpec(&specs, &spec_count, p, repository.maxRunFileReadBytes(), false, true);
    try pool.io.replacePreparedFiles(control, specs[0..spec_count]);
    for (&pool.retired) |*generation| {
        for (generation.paths[0..generation.count]) |*path| if (path.*) |p| {
            if (backend.obsoletePathPinnedByOpenVersion(p)) continue;
            pool.io.storage().deleteFileAbsolute(p) catch |err| if (err != error.FileNotFound) return err;
            try pool.io.storage().syncParentAbsolute(p);
            control.free(p);
            path.* = null;
        };
        generation.compact();
    }
    const retired_index = for (pool.retired, 0..) |generation, i| {
        if (generation.count == 0) break i;
    } else return error.CompletionReservationBusy;
    var retiring: Retired = .{};
    defer retiring.deinit(control);
    var input_paths: [68][]const u8 = undefined;
    for (0..backend.runs.count()) |i| {
        const path = backend.runs.at(i).path orelse return error.UnsupportedCompletionProfile;
        input_paths[i] = path;
        try retiring.add(control, path);
    }
    if (backend.manifest_journal.descriptor) |descriptor| {
        const checkpoint = try manifest_set.pathAlloc(scratch, root, descriptor.checkpoint, .checkpoint);
        defer scratch.free(checkpoint);
        try retiring.add(control, checkpoint);
    }
    for (backend.manifest_journal.segments[0..backend.manifest_journal.segment_count]) |id| {
        inline for (.{ manifest_set.Kind.journal, manifest_set.Kind.next }) |kind| {
            const path = try manifest_set.pathAlloc(scratch, root, id, kind);
            defer scratch.free(path);
            try retiring.add(control, path);
        }
    }
    const output_base = backend.next_run_id;
    const manifest_id = try std.math.add(u64, output_base, 64);
    const cohort_base = try std.math.add(u64, manifest_id, 1);
    const next_id = try std.math.add(u64, cohort_base, completion.max_slots);
    // Reserve names monotonically even when construction fails before publish.
    backend.next_run_id = next_id;
    var output_paths: [64][]u8 = undefined;
    var output_count: usize = 0;
    defer for (output_paths[0..output_count]) |path| control.free(path);
    for (&output_paths, 0..) |*path, i| {
        path.* = try repository.runPath(control, root, output_base + i);
        output_count += 1;
    }
    var next_paths: [completion.max_slots]?[]const u8 = @splat(null);
    var next_pins: [completion.max_slots]?Backend.CompletionRunPathPin = @splat(null);
    defer {
        for (&next_pins) |*pin| if (pin.*) |*held| held.release();
        for (next_paths) |path| if (path) |p| control.free(p);
    }
    for (0..completion.max_slots) |i| {
        next_paths[i] = try repository.runPath(control, root, cohort_base + i);
        next_pins[i] = try Backend.pinCompletionRunPath(control, next_paths[i].?);
    }
    const checkpoint_path = try manifest_set.pathAlloc(control, root, manifest_id, .checkpoint);
    defer control.free(checkpoint_path);
    var next_journal: ?[]u8 = try manifest_set.pathAlloc(control, root, manifest_id, .journal);
    defer if (next_journal) |path| control.free(path);
    const pointer_path = try std.fs.path.join(control, &.{ root, "manifest.bin" });
    defer control.free(pointer_path);
    // Rebuild, rather than append to, the allowlist: obsolete unused planned
    // paths must not consume one additional slot on every successful cycle.
    spec_count = 0;
    for (pool.guard_paths) |path| try addSpec(&specs, &spec_count, path, completion.limits.max_encoded_bytes + completion.guard.header_bytes, false, true);
    for (pool.accepted_paths) |path| try addSpec(&specs, &spec_count, path, @import("completion_pool.zig").max_accepted_bytes, false, true);
    for (pool.control_accepted_paths) |path| try addSpec(&specs, &spec_count, path, @import("completion_control_accepted.zig").max_bytes, false, true);
    var wal_paths: [3][]u8 = undefined;
    var wal_count: usize = 0;
    defer for (wal_paths[0..wal_count]) |path| control.free(path);
    for ([_][]const u8{ "wal.log", "wal/replay.index", "wal/replay.segments" }, 0..) |name, i| {
        wal_paths[i] = try std.fs.path.join(control, &.{ root, name });
        wal_count += 1;
        try addSpec(&specs, &spec_count, wal_paths[i], completion.limits.wal_bytes, false, i == 0);
    }
    for (input_paths[0..backend.runs.count()]) |path| try addSpec(&specs, &spec_count, path, repository.maxRunFileReadBytes(), false, true);
    for (pool.retired) |generation| for (generation.paths[0..generation.count]) |path| if (path) |p| try addSpec(&specs, &spec_count, p, repository.maxRunFileReadBytes(), false, true);
    for (retiring.paths[0..retiring.count]) |path| try addSpec(&specs, &spec_count, path.?, repository.maxRunFileReadBytes(), false, true);
    for (output_paths) |path| try addSpec(&specs, &spec_count, path, repository.maxRunFileReadBytes(), false, true);
    for (next_paths) |path| try addSpec(&specs, &spec_count, path.?, completion.limits.flush_bytes, false, true);
    try addSpec(&specs, &spec_count, checkpoint_path, repository.maxManifestReadBytes(), false, true);
    try addSpec(&specs, &spec_count, next_journal.?, repository.maxManifestReadBytes(), true, true);
    try addSpec(&specs, &spec_count, pointer_path, repository.maxManifestReadBytes(), false, false);
    try pool.io.replacePreparedFiles(control, specs[0..spec_count]);
    var current = try backend.mutable.snapshot(scratch);
    defer current.deinit(scratch);
    // Input identities and mutable are stable behind the maintenance fence.
    // Reader/owner lifetime stays pinned while the expensive scan runs unlocked.
    runtime.unlockBackend(Backend, backend, true);
    const result = stream.buildCertified(scratch, pool.io.storage(), root, input_paths[0..backend.runs.count()], &current, output_base, .{
        .max_inputs = pool.config.shape.max_runs,
        .max_outputs = 64,
        .max_metadata_bytes = pool.config.shape.max_metadata_bytes,
        .max_output_metadata_bytes = pool.config.shape.max_metadata_bytes,
        .max_block_bytes = pool.config.shape.max_block_bytes,
        .max_record_bytes = pool.config.shape.max_record_bytes,
    });
    _ = runtime.lockBackend(Backend, backend);
    var manifest_attempted = false;
    defer if (!manifest_attempted) {
        for (output_paths) |path| pool.io.storage().deleteFileAbsolute(path) catch {};
        pool.io.storage().deleteFileAbsolute(checkpoint_path) catch {};
        if (next_journal) |path| pool.io.storage().deleteFileAbsolute(path) catch {};
    };
    var built = try result;
    defer {
        for (built.items) |*run_item| run_item.deinit(scratch);
        built.deinit(scratch);
    }
    if (backend.closing.load(.acquire)) return error.LsmBackendClosed;
    try pool.checkMaintenanceMetadata(built.items);
    // A completed cohort relinquishes every unused reservation. Published
    // children retain their exact old generation; empty-domain selection does
    // not mistake fragmented reader-held bytes for the next contiguous span.
    for (pool.cells[0..pool.cell_count]) |*cell| if (cell.publication) |reservation| {
        reservation.finish();
        cell.publication = null;
    };
    var new_reservations = try pool.generations.reserveCells();
    defer new_reservations.deinit();
    const pub_alloc = (try pool.generations.emptyMetadata()).allocator();
    // A full replacement must not retain the prior tree's Account header:
    // that allocator-owned header alone would pin an old generation forever.
    var candidate: @TypeOf(backend.runs) = .{ .destroy_run = backend.runs.destroy_run };
    defer candidate.deinit(backend.allocator);
    const Directory = @import("run_directory.zig").Directory;
    var directory: ?*Directory = try Directory.create(pub_alloc);
    defer if (directory) |held| held.destroy(backend.allocator);
    const View = struct {
        allocator: Allocator,
        options: @TypeOf(backend.options),
        pub fn retainRunSnapshotRef(_: *@This(), entry: *repository.Run) !void {
            try Backend.retainRunSnapshotRef(undefined, entry);
        }
        pub fn releaseDirectoryRunSnapshotRef(entry: *repository.Run) void {
            Backend.releaseDirectoryRunSnapshotRef(entry);
        }
    };
    var view = View{ .allocator = pub_alloc, .options = backend.options };
    var output_pins: [64]?Backend.CompletionRunPathPin = @splat(null);
    defer for (&output_pins) |*pin| if (pin.*) |*held| held.release();
    for (built.items, 0..) |item, i| {
        output_pins[i] = try Backend.pinCompletionRunPath(control, item.path.?);
        var copy = try repository.cloneRunCompactionSnapshot(pub_alloc, item);
        copy.metadata_allocator = pub_alloc;
        candidate.append(pub_alloc, copy) catch |err| {
            copy.deinit(backend.allocator);
            return err;
        };
        try directory.?.put(&view, candidate.find(&copy).?.*);
    }
    var durable: ?*Directory = try directory.?.fork(pub_alloc);
    defer if (durable) |held| held.destroy(backend.allocator);
    const Source = struct {
        cursor: Directory.Cursor,
        root: []const u8,
        generations: [3]*const Retired,
        generation: usize = 0,
        index: usize = 0,
        pub fn runCount(self: *@This()) usize {
            return self.cursor.directory.count();
        }
        pub fn obsoleteCount(self: *@This()) usize {
            var count: usize = 0;
            for (self.generations) |g| count += g.count;
            return count;
        }
        pub fn nextRun(self: *@This()) ?manifest.RunMeta {
            var meta = repository.runMeta((self.cursor.next() orelse return null).run.*);
            meta.path = repository.manifestRelativePath(self.root, meta.path);
            return meta;
        }
        pub fn nextObsolete(self: *@This()) ?manifest.ObsoletePathMeta {
            while (self.generation < self.generations.len) {
                const g = self.generations[self.generation];
                if (self.index < g.count) {
                    const path = g.paths[self.index].?;
                    self.index += 1;
                    return .{ .path = repository.manifestRelativePath(self.root, path), .delete_after_ns = 0 };
                }
                self.generation += 1;
                self.index = 0;
            }
            return null;
        }
    };
    var source = Source{ .cursor = directory.?.readCursor(), .root = root, .generations = .{ &pool.retired[0], &pool.retired[1], &retiring } };
    const sequence = try std.math.add(u64, backend.manifest_journal.sequence.?, 1);
    const next_sequence = try std.math.add(u64, sequence, 1);
    const checkpoint_bytes = try manifest_set.writeCheckpoint(scratch, pool.io.storage(), root, manifest_id, sequence, next_id, &source, repository.maxManifestReadBytes());
    try manifest_set.createSegment(scratch, pool.io.storage(), root, manifest_id, next_sequence);
    try fault(.before_publish);
    const descriptor: manifest_set.Descriptor = .{ .checkpoint = manifest_id, .first_segment = manifest_id, .sequence = sequence };
    var wal_lock = try backend.acquireWalOperationLock(.exclusive);
    defer wal_lock.release();
    manifest_attempted = true;
    errdefer backend.fenceFailedBulkWal();
    try manifest_set.publish(scratch, pool.io.storage(), root, descriptor);
    try fault(.after_publish);
    backend.invalidateMutableReadSnapshot();
    backend.invalidateReadVersion();
    std.mem.swap(@TypeOf(backend.runs), &backend.runs, &candidate);
    if (backend.run_directory) |old| old.destroy(backend.allocator);
    backend.run_directory = directory;
    directory = null;
    backend.run_directory_dirty = false;
    if (backend.manifest_directory) |old| old.destroy(backend.allocator);
    backend.manifest_directory = durable;
    durable = null;
    backend.mutable.deinit(backend.allocator);
    backend.mutable = .{};
    backend.mutable_wal_range = .{};
    backend.obsolete_paths.deinit(backend.allocator);
    backend.manifest_journal.deinit(backend.allocator);
    backend.manifest_journal = .{ .descriptor = descriptor, .active_segment = manifest_id, .sequence = sequence, .checkpoint_sequence = sequence, .segment_count = 1, .next_run_id = next_id, .bytes = checkpoint_bytes + manifest_set.header_len };
    backend.manifest_journal.segments[0] = manifest_id;
    backend.manifest_dirty = false;
    backend.manifest_unpublished_wire_bytes = 0;
    backend.manifest_pending_mutation_bytes = 0;
    pool.retired[retired_index] = retiring;
    retiring = .{};
    backend.clearPublishedWalLogicalDebtLocked();
    backend.syncTrackedInMemoryStateUsageCurrentLocked();
    try wal.protectedReset(pool.io.storage(), scratch, root);
    try fault(.after_reset);
    backend.wal_retention.primary = .{ .oldest_retained_segment = 1, .current_segment = 1 };
    backend.wal_retention.replay = .{ .current_segment = 1 };
    backend.wal_retention.primary_ns = backend.writeStatsNowNs();
    backend.wal_retention.replay_ns = backend.writeStatsNowNs();
    try @TypeOf(pool.*).restoreWalCredits(pool, backend);
    pool.wal_pin.manager.observeUsage(.lsm_wal_retention, &backend.tracked_wal_retention_bytes, 0);
    for (&pool.run_path_pins) |*pin| if (pin.*) |*held| held.release();
    for (pool.run_paths) |path| if (path) |p| control.free(p);
    pool.run_paths = next_paths;
    next_paths = @splat(null);
    pool.run_path_pins = next_pins;
    next_pins = @splat(null);
    control.free(pool.journal_path);
    pool.journal_path = next_journal.?;
    next_journal = null;
    pool.cohort = .{ .base_run_id = cohort_base, .initial_runs = @intCast(backend.runs.count()), .cohort_id = pool.config.identity.incarnation };
    for (pool.cells[0..pool.cell_count], 0..) |*cell, i| {
        if (new_reservations.items[i]) |reservation| {
            cell.publication = reservation;
            new_reservations.items[i] = null;
        }
        cell.phase = .free;
        cell.baseline = .{};
        cell.term = 0;
        cell.index = 0;
        cell.publication_notified = false;
        cell.restored_prepared = false;
        cell.resolution = null;
    }
    pool.maintenance_pending = false;
    // Qualification is a separate explicit step; no physical progress receipt
    // or successful checkpoint alone can manufacture admission readiness.
}
