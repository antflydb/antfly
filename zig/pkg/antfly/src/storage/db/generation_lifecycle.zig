// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2

const std = @import("std");
const builtin = @import("builtin");
const platform = @import("antfly_platform");
const fs_paths = @import("../../common/fs_paths.zig");
const background_runtime = @import("../background_runtime.zig");

const Allocator = std.mem.Allocator;
const publication_marker_name = ".antfly-generation-publication-v2";
const publication_marker_tmp_name = ".antfly-generation-publication-v2.tmp";
const max_publication_marker_bytes = 4096;
const publication_lock_suffix = ".antfly-generation.lock";
const preparation_lock_suffix = ".antfly-generation-prepare.lock";
const retired_cleanup_lock_name = ".antfly-generation-cleanup.lock";
const cleanup_intent_dir_suffix = ".antfly-generation-cleanup-v1";
const cleanup_intent_file_suffix = ".json";
const cleanup_intent_tmp_suffix = ".tmp";
const max_cleanup_intent_bytes = 1024;
const max_reconciled_paths = 8192;

const PublicationPhase = enum {
    prepared,
    committed,
};

const PublicationMarker = struct {
    version: u8 = 2,
    phase: PublicationPhase,
    retained_name: []const u8,
    had_live_generation: bool,
};

const OwnedPublicationMarker = struct {
    phase: PublicationPhase,
    retained_name: []u8,
    had_live_generation: bool,

    fn deinit(self: *@This(), alloc: Allocator) void {
        alloc.free(self.retained_name);
        self.* = undefined;
    }
};

/// Cleanup is a separate durable transaction from namespace publication.
/// Every retired generation owns one immutable, sibling-scoped intent so an
/// older worker can acknowledge only its own deletion and can never erase the
/// crash-recovery marker of a newer publication.
const CleanupIntent = struct {
    version: u8 = 1,
    retained_name: []const u8,
};

fn publicationLockPathAlloc(alloc: Allocator, canonical_path: []const u8) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{s}{s}", .{ canonical_path, publication_lock_suffix });
}

fn preparationLockPathAlloc(alloc: Allocator, canonical_path: []const u8) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{s}{s}", .{ canonical_path, preparation_lock_suffix });
}

fn openPathLockWithIo(io: std.Io, lock_path: []const u8, lock: std.Io.File.Lock) !std.Io.File {
    if (std.fs.path.dirname(lock_path)) |parent| try fs_paths.createDirPathPortable(io, parent);
    return std.Io.Dir.cwd().createFile(io, lock_path, .{
        .read = true,
        .truncate = false,
        .lock = lock,
        .lock_nonblocking = true,
    }) catch |err| switch (err) {
        error.WouldBlock => error.GenerationTransitionActive,
        error.FileLocksUnsupported => error.GenerationFileLocksUnsupported,
        else => err,
    };
}

fn openPublicationLockWithIo(io: std.Io, alloc: Allocator, canonical_path: []const u8, lock: std.Io.File.Lock) !std.Io.File {
    const lock_path = try publicationLockPathAlloc(alloc, canonical_path);
    defer alloc.free(lock_path);
    return try openPathLockWithIo(io, lock_path, lock);
}

fn openPublicationLock(alloc: Allocator, canonical_path: []const u8, lock: std.Io.File.Lock) !std.Io.File {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    return try openPublicationLockWithIo(io_impl.io(), alloc, canonical_path, lock);
}

fn openPreparationLockWithIo(io: std.Io, alloc: Allocator, canonical_path: []const u8) !std.Io.File {
    const lock_path = try preparationLockPathAlloc(alloc, canonical_path);
    defer alloc.free(lock_path);
    return try openPathLockWithIo(io, lock_path, .exclusive);
}

fn closePublicationLockWithIo(io: std.Io, file: std.Io.File) void {
    file.close(io);
}

fn closePublicationLock(file: std.Io.File) void {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    file.close(io_impl.io());
}

const LeaseKind = enum { preparation, exclusive, reconciliation, read };

fn realPathAlloc(alloc: Allocator, io: std.Io, path: []const u8) ![:0]u8 {
    if (std.fs.path.isAbsolute(path)) return try std.Io.Dir.realPathFileAbsoluteAlloc(io, path, alloc);
    return try std.Io.Dir.cwd().realPathFileAlloc(io, path, alloc);
}

fn canonicalPathAllocWithIo(alloc: Allocator, io: std.Io, path: []const u8) ![]u8 {
    const absolute = if (std.fs.path.isAbsolute(path))
        try std.fs.path.resolve(alloc, &.{path})
    else blk: {
        const cwd = try realPathAlloc(alloc, io, ".");
        defer alloc.free(cwd);
        break :blk try std.fs.path.resolve(alloc, &.{ cwd, path });
    };
    errdefer alloc.free(absolute);

    // Resolve the deepest existing ancestor, then append the still-missing
    // suffix. This keeps a path's identity stable before and after creation
    // (notably /tmp -> /private/tmp on macOS).
    var probe: []const u8 = absolute;
    while (true) {
        const canonical: ?[:0]u8 = realPathAlloc(alloc, io, probe) catch |err| switch (err) {
            error.OutOfMemory => return err,
            else => null,
        };
        if (canonical) |existing| {
            defer alloc.free(existing);
            const suffix = std.mem.trimStart(u8, absolute[probe.len..], &.{std.fs.path.sep});
            if (suffix.len == 0) {
                alloc.free(absolute);
                return try alloc.dupe(u8, existing);
            }
            const result = try std.fs.path.join(alloc, &.{ existing, suffix });
            alloc.free(absolute);
            return result;
        }
        const parent = std.fs.path.dirname(probe) orelse break;
        if (std.mem.eql(u8, parent, probe)) break;
        probe = parent;
    }
    return absolute;
}

fn canonicalPathAlloc(alloc: Allocator, path: []const u8) ![]u8 {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    return try canonicalPathAllocWithIo(alloc, io_impl.io(), path);
}

pub const PublicationOutcome = enum {
    durable,
    durability_uncertain,
};

const CleanupScheduler = struct {
    alloc: Allocator,
    io: std.Io,
    lane: background_runtime.DurableJobLane,
    owner_id: u64,

    fn fromRuntime(runtime: ?*background_runtime.BackendRuntime) ?CleanupScheduler {
        const active = runtime orelse return null;
        // The manual runtime executes jobs inline. Preserve cleanup as durable
        // reconciliation debt instead of extending publication downtime.
        if (active.threaded_jobs == null) return null;
        return .{
            .alloc = active.alloc,
            .io = active.io() orelse return null,
            .lane = active.durable_jobs,
            .owner_id = active.retired_generation_cleanup_owner_id,
        };
    }
};

var test_fail_post_publish_sync = false;
var test_fail_reconciliation_sync = false;
var test_disable_atomic_exchange = false;
var test_block_retired_cleanup = std.atomic.Value(bool).init(false);
var test_retired_cleanup_started = std.atomic.Value(bool).init(false);
var test_retired_cleanup_failures_remaining = std.atomic.Value(usize).init(0);

pub fn failNextPublishedParentSyncForTest() void {
    std.debug.assert(builtin.is_test);
    test_fail_post_publish_sync = true;
}

pub fn failNextReconciliationSyncForTest() void {
    std.debug.assert(builtin.is_test);
    test_fail_reconciliation_sync = true;
}

const ActiveTransition = struct {
    path: []u8,
    path_key: []u8,
    id: u64,
    kind: LeaseKind,
};

const PathState = struct {
    readers: usize = 0,
    preparation: bool = false,
    reconciliation: bool = false,
    exclusive: bool = false,

    fn isEmpty(self: PathState) bool {
        return self.readers == 0 and !self.preparation and !self.reconciliation and !self.exclusive;
    }
};

const Manager = struct {
    allocator: Allocator,
    mutex: std.atomic.Mutex = .unlocked,
    next_id: u64 = 1,
    active: std.AutoHashMapUnmanaged(u64, ActiveTransition) = .empty,
    path_states: std.StringHashMapUnmanaged(PathState) = .empty,
    reconciled_paths: std.StringHashMapUnmanaged(void) = .empty,

    pub fn init(allocator: Allocator) Manager {
        return .{ .allocator = allocator };
    }

    fn getOrPutPathStateLocked(self: *Manager, path_key: []const u8) !*PathState {
        const entry = try self.path_states.getOrPut(self.allocator, path_key);
        if (!entry.found_existing) {
            errdefer _ = self.path_states.remove(path_key);
            entry.key_ptr.* = try self.allocator.dupe(u8, path_key);
            entry.value_ptr.* = .{};
        }
        return entry.value_ptr;
    }

    fn removeEmptyPathStateLocked(self: *Manager, path_key: []const u8) void {
        const state = self.path_states.get(path_key) orelse return;
        if (!state.isEmpty()) return;
        const removed = self.path_states.fetchRemove(path_key) orelse unreachable;
        self.allocator.free(removed.key);
    }

    pub fn beginExclusive(self: *Manager, path: []const u8) !ExclusiveTransition {
        var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
        defer io_impl.deinit();
        var transition = try self.beginExclusiveWithIo(path, io_impl.io());
        transition.io = null;
        return transition;
    }

    fn beginExclusiveWithIo(self: *Manager, path: []const u8, io: std.Io) !ExclusiveTransition {
        const path_key = try canonicalPathAllocWithIo(self.allocator, io, path);
        errdefer self.allocator.free(path_key);
        const owned_path = try self.allocator.dupe(u8, path);
        errdefer self.allocator.free(owned_path);

        // Filesystem lock acquisition can block or fail. Keep it outside the
        // manager mutex so rollback never has to re-enter the mutex through an
        // ExclusiveTransition destructor.
        const preparation_lock = try openPreparationLockWithIo(io, self.allocator, path_key);
        errdefer closePublicationLockWithIo(io, preparation_lock);
        const publication_lock = try openPublicationLockWithIo(io, self.allocator, path_key, .exclusive);
        errdefer closePublicationLockWithIo(io, publication_lock);

        platform.sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();

        if (self.path_states.get(path_key)) |state| {
            if (!state.isEmpty()) return error.GenerationTransitionActive;
        }
        self.removeReconciledPathLocked(path_key);

        const id = self.next_id;
        self.next_id +%= 1;
        if (self.next_id == 0) self.next_id = 1;
        const state = try self.getOrPutPathStateLocked(path_key);
        state.exclusive = true;
        errdefer {
            state.exclusive = false;
            self.removeEmptyPathStateLocked(path_key);
        }
        try self.active.putNoClobber(self.allocator, id, .{ .path = owned_path, .path_key = path_key, .id = id, .kind = .exclusive });
        const transition = ExclusiveTransition{
            .manager = self,
            .alloc = self.allocator,
            .path = owned_path,
            .id = id,
            .publication_lock = publication_lock,
            .preparation_lock = preparation_lock,
            .io = io,
        };
        return transition;
    }

    pub fn beginPreparation(self: *Manager, path: []const u8, cleanup_scheduler: ?CleanupScheduler) !PreparationTransition {
        var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
        defer io_impl.deinit();
        var transition = try self.beginPreparationWithIo(path, cleanup_scheduler, io_impl.io());
        transition.io = null;
        return transition;
    }

    fn beginPreparationWithIo(
        self: *Manager,
        path: []const u8,
        cleanup_scheduler: ?CleanupScheduler,
        io: std.Io,
    ) !PreparationTransition {
        const path_key = try canonicalPathAllocWithIo(self.allocator, io, path);
        errdefer self.allocator.free(path_key);
        const owned_path = try self.allocator.dupe(u8, path);
        errdefer self.allocator.free(owned_path);

        const preparation_lock = try openPreparationLockWithIo(io, self.allocator, path_key);
        errdefer closePublicationLockWithIo(io, preparation_lock);
        const publication_lock = try openPublicationLockWithIo(io, self.allocator, path_key, .shared);
        errdefer closePublicationLockWithIo(io, publication_lock);

        platform.sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        if (self.path_states.get(path_key)) |state| {
            if (state.preparation or state.exclusive or state.reconciliation) return error.GenerationTransitionActive;
        }

        const id = self.next_id;
        self.next_id +%= 1;
        if (self.next_id == 0) self.next_id = 1;
        const state = try self.getOrPutPathStateLocked(path_key);
        state.preparation = true;
        errdefer {
            state.preparation = false;
            self.removeEmptyPathStateLocked(path_key);
        }
        try self.active.putNoClobber(self.allocator, id, .{ .path = owned_path, .path_key = path_key, .id = id, .kind = .preparation });
        return .{
            .manager = self,
            .alloc = self.allocator,
            .path = owned_path,
            .id = id,
            .cleanup_scheduler = cleanup_scheduler,
            .publication_lock = publication_lock,
            .preparation_lock = preparation_lock,
            .io = io,
        };
    }

    fn beginReconciliation(self: *Manager, path: []const u8) !?ReconciliationLease {
        var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
        defer io_impl.deinit();
        return try self.beginReconciliationWithIo(path, io_impl.io());
    }

    fn beginReconciliationWithIo(self: *Manager, path: []const u8, io: std.Io) !?ReconciliationLease {
        const path_key = try canonicalPathAllocWithIo(self.allocator, io, path);
        errdefer self.allocator.free(path_key);
        const owned_path = try self.allocator.dupe(u8, path);
        errdefer self.allocator.free(owned_path);

        platform.sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        if (self.reconciled_paths.contains(path_key)) {
            self.allocator.free(owned_path);
            self.allocator.free(path_key);
            return null;
        }
        if (self.path_states.get(path_key)) |state| {
            if (state.readers != 0 and !state.exclusive and !state.reconciliation) {
                self.allocator.free(owned_path);
                self.allocator.free(path_key);
                return null;
            }
            if (!state.isEmpty()) return error.GenerationTransitionActive;
        }

        const id = self.next_id;
        self.next_id +%= 1;
        if (self.next_id == 0) self.next_id = 1;
        const state = try self.getOrPutPathStateLocked(path_key);
        state.reconciliation = true;
        errdefer {
            state.reconciliation = false;
            self.removeEmptyPathStateLocked(path_key);
        }
        try self.active.putNoClobber(self.allocator, id, .{ .path = owned_path, .path_key = path_key, .id = id, .kind = .reconciliation });
        return .{ .manager = self, .path = owned_path, .path_key = path_key, .id = id };
    }

    fn beginRead(self: *Manager, path: []const u8) !ReadLease {
        const path_key = try canonicalPathAlloc(self.allocator, path);
        errdefer self.allocator.free(path_key);
        const owned_path = try self.allocator.dupe(u8, path);
        errdefer self.allocator.free(owned_path);

        platform.sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        if (self.path_states.get(path_key)) |state| {
            if (state.exclusive or state.reconciliation) return error.GenerationTransitionActive;
        }
        const id = self.next_id;
        self.next_id +%= 1;
        if (self.next_id == 0) self.next_id = 1;
        const state = try self.getOrPutPathStateLocked(path_key);
        state.readers += 1;
        errdefer {
            state.readers -= 1;
            self.removeEmptyPathStateLocked(path_key);
        }
        try self.active.putNoClobber(self.allocator, id, .{ .path = owned_path, .path_key = path_key, .id = id, .kind = .read });
        return .{ .manager = self, .path = owned_path, .path_key = path_key, .id = id };
    }

    /// Commits a cached read only after its caller holds the shared
    /// publication lock. Returning null means the reconciliation evidence
    /// disappeared while that lock was being acquired, so the caller must
    /// release it and retry through reconciliation.
    fn beginReadIfReconciled(self: *Manager, path: []const u8) !?ReadLease {
        const path_key = try canonicalPathAlloc(self.allocator, path);
        defer self.allocator.free(path_key);
        return try self.beginReadIfReconciledCanonical(path, path_key);
    }

    fn beginReadIfReconciledCanonical(self: *Manager, path: []const u8, path_key: []const u8) !?ReadLease {
        const active_path_key = try self.allocator.dupe(u8, path_key);
        errdefer self.allocator.free(active_path_key);
        const owned_path = try self.allocator.dupe(u8, path);
        errdefer self.allocator.free(owned_path);

        platform.sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        const existing_state = self.path_states.get(path_key);
        if (existing_state) |state| {
            if (state.exclusive or state.reconciliation) return error.GenerationTransitionActive;
        }
        const protected_by_local_reader = if (existing_state) |state| state.readers != 0 else false;
        if (!protected_by_local_reader and !self.reconciled_paths.contains(path_key)) {
            self.allocator.free(owned_path);
            self.allocator.free(active_path_key);
            return null;
        }

        const id = self.next_id;
        self.next_id +%= 1;
        if (self.next_id == 0) self.next_id = 1;
        const state = try self.getOrPutPathStateLocked(path_key);
        state.readers += 1;
        errdefer {
            state.readers -= 1;
            self.removeEmptyPathStateLocked(path_key);
        }
        try self.active.putNoClobber(self.allocator, id, .{ .path = owned_path, .path_key = active_path_key, .id = id, .kind = .read });
        return .{ .manager = self, .path = owned_path, .path_key = active_path_key, .id = id };
    }

    fn hasReaders(self: *Manager, path: []const u8) !bool {
        const path_key = try canonicalPathAlloc(self.allocator, path);
        defer self.allocator.free(path_key);
        platform.sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        const state = self.path_states.get(path_key) orelse return false;
        return state.readers != 0 and !state.exclusive and !state.reconciliation;
    }

    fn finishExclusive(self: *Manager, path: []const u8, id: u64) void {
        platform.sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();

        const removed = self.active.fetchRemove(id) orelse unreachable;
        std.debug.assert(removed.value.kind == .exclusive and std.mem.eql(u8, removed.value.path, path));
        const state = self.path_states.getPtr(removed.value.path_key) orelse unreachable;
        std.debug.assert(state.exclusive);
        state.exclusive = false;
        self.removeEmptyPathStateLocked(removed.value.path_key);
        self.allocator.free(removed.value.path);
        self.allocator.free(removed.value.path_key);
    }

    fn validateExclusive(self: *Manager, id: u64, path: []const u8) !void {
        platform.sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        const entry = self.active.get(id) orelse return error.InvalidGenerationTransition;
        if (entry.kind == .exclusive and std.mem.eql(u8, entry.path, path)) return;
        return error.InvalidGenerationTransition;
    }

    fn validateStaging(self: *Manager, id: u64, path: []const u8) !void {
        platform.sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        const entry = self.active.get(id) orelse return error.InvalidGenerationTransition;
        if ((entry.kind == .preparation or entry.kind == .exclusive) and std.mem.eql(u8, entry.path, path)) return;
        return error.InvalidGenerationTransition;
    }

    fn finishPreparation(self: *Manager, path: []const u8, id: u64) void {
        platform.sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        const removed = self.active.fetchRemove(id) orelse unreachable;
        std.debug.assert(removed.value.kind == .preparation and std.mem.eql(u8, removed.value.path, path));
        const state = self.path_states.getPtr(removed.value.path_key) orelse unreachable;
        std.debug.assert(state.preparation);
        state.preparation = false;
        self.removeEmptyPathStateLocked(removed.value.path_key);
        self.allocator.free(removed.value.path);
        self.allocator.free(removed.value.path_key);
    }

    fn promotePreparation(self: *Manager, path: []const u8, id: u64) !void {
        platform.sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        const active = self.active.getPtr(id) orelse return error.InvalidGenerationTransition;
        if (active.kind != .preparation or !std.mem.eql(u8, active.path, path)) return error.InvalidGenerationTransition;
        const state = self.path_states.getPtr(active.path_key) orelse unreachable;
        if (state.readers != 0 or state.exclusive or state.reconciliation or !state.preparation) return error.GenerationTransitionActive;
        state.preparation = false;
        state.exclusive = true;
        active.kind = .exclusive;
        self.removeReconciledPathLocked(active.path_key);
    }

    fn removeReconciledPathLocked(self: *Manager, path: []const u8) void {
        if (self.reconciled_paths.fetchRemove(path)) |removed| self.allocator.free(removed.key);
    }

    fn finishReconciliation(self: *Manager, path: []const u8, id: u64, mark_reconciled: bool) void {
        platform.sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        const removed = self.active.fetchRemove(id) orelse unreachable;
        std.debug.assert(removed.value.kind == .reconciliation and std.mem.eql(u8, removed.value.path, path));
        const state = self.path_states.getPtr(removed.value.path_key) orelse unreachable;
        std.debug.assert(state.reconciliation);
        state.reconciliation = false;
        self.removeEmptyPathStateLocked(removed.value.path_key);
        self.allocator.free(removed.value.path);
        if (mark_reconciled) {
            const entry = self.reconciled_paths.getOrPut(self.allocator, removed.value.path_key) catch |err| {
                std.log.warn("generation reconciliation cache insert failed path={s} err={s}", .{ removed.value.path_key, @errorName(err) });
                self.allocator.free(removed.value.path_key);
                return;
            };
            if (entry.found_existing) {
                self.allocator.free(removed.value.path_key);
            }
            if (self.reconciled_paths.count() > max_reconciled_paths) {
                var iterator = self.reconciled_paths.keyIterator();
                const victim = iterator.next() orelse return;
                self.removeReconciledPathLocked(victim.*);
            }
        } else {
            self.allocator.free(removed.value.path_key);
        }
    }

    fn promoteReconciliationToRead(self: *Manager, path: []const u8, id: u64, mark_reconciled: bool) !void {
        platform.sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        const active = self.active.getPtr(id) orelse return error.InvalidGenerationTransition;
        if (active.kind == .reconciliation and std.mem.eql(u8, active.path, path)) {
            if (mark_reconciled and !self.reconciled_paths.contains(active.path_key)) {
                const cache_key = try self.allocator.dupe(u8, active.path_key);
                errdefer self.allocator.free(cache_key);
                try self.reconciled_paths.put(self.allocator, cache_key, {});
                if (self.reconciled_paths.count() > max_reconciled_paths) {
                    var iterator = self.reconciled_paths.keyIterator();
                    const victim = iterator.next() orelse unreachable;
                    self.removeReconciledPathLocked(victim.*);
                }
            }
            const state = self.path_states.getPtr(active.path_key) orelse unreachable;
            std.debug.assert(state.reconciliation and state.readers == 0);
            state.reconciliation = false;
            state.readers = 1;
            active.kind = .read;
            return;
        }
        return error.InvalidGenerationTransition;
    }

    fn finishRead(self: *Manager, path: []const u8, id: u64) void {
        platform.sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        const removed = self.active.fetchRemove(id) orelse unreachable;
        std.debug.assert(removed.value.kind == .read and std.mem.eql(u8, removed.value.path, path));
        const state = self.path_states.getPtr(removed.value.path_key) orelse unreachable;
        std.debug.assert(state.readers > 0);
        state.readers -= 1;
        // A shared publication lock makes this cache valid only while at least
        // one local reader remains. Once the final reader drains, another
        // process may publish before the next open.
        if (state.readers == 0) self.removeReconciledPathLocked(removed.value.path_key);
        self.removeEmptyPathStateLocked(removed.value.path_key);
        self.allocator.free(removed.value.path);
        self.allocator.free(removed.value.path_key);
    }

    pub fn deinit(self: *Manager) void {
        platform.sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        std.debug.assert(self.active.count() == 0);
        self.active.deinit(self.allocator);
        self.active = .empty;
        std.debug.assert(self.path_states.count() == 0);
        self.path_states.deinit(self.allocator);
        self.path_states = .empty;
        var reconciled_it = self.reconciled_paths.keyIterator();
        while (reconciled_it.next()) |path| self.allocator.free(path.*);
        self.reconciled_paths.deinit(self.allocator);
        self.reconciled_paths = .empty;
    }
};

const ReconciliationLease = struct {
    manager: *Manager,
    path: []const u8,
    path_key: []const u8,
    id: u64,
    active: bool = true,

    fn complete(self: *@This()) void {
        if (!self.active) return;
        self.manager.finishReconciliation(self.path, self.id, true);
        self.active = false;
    }

    fn promoteToRead(self: *@This(), publication_lock: std.Io.File, io: ?std.Io, mark_reconciled: bool) !ReadLease {
        if (!self.active) return error.InvalidGenerationTransition;
        try self.manager.promoteReconciliationToRead(self.path, self.id, mark_reconciled);
        self.active = false;
        return .{
            .manager = self.manager,
            .path = self.path,
            .path_key = self.path_key,
            .id = self.id,
            .publication_lock = publication_lock,
            .io = io,
        };
    }

    fn deinit(self: *@This()) void {
        if (!self.active) return;
        self.manager.finishReconciliation(self.path, self.id, false);
        self.active = false;
    }
};

pub const ReadLease = struct {
    manager: *Manager,
    path: []const u8,
    path_key: []const u8,
    id: u64,
    publication_lock: ?std.Io.File = null,
    io: ?std.Io = null,
    active: bool = true,

    pub fn deinit(self: *@This()) void {
        if (!self.active) return;
        const publication_lock = self.publication_lock;
        self.publication_lock = null;
        // Retire local reader/cache state before releasing the interprocess
        // lock so no local opener can trust stale reconciliation state across
        // an external publication window.
        self.manager.finishRead(self.path, self.id);
        if (publication_lock) |file| {
            if (self.io) |io| closePublicationLockWithIo(io, file) else closePublicationLock(file);
        }
        self.active = false;
    }
};

pub const PreparationTransition = struct {
    manager: *Manager,
    alloc: Allocator,
    path: []const u8,
    id: u64,
    cleanup_scheduler: ?CleanupScheduler = null,
    io: ?std.Io = null,
    publication_lock: ?std.Io.File,
    preparation_lock: ?std.Io.File,
    active: bool = true,

    pub fn beginStaging(self: *PreparationTransition) !StagedGeneration {
        if (!self.active) return error.InvalidGenerationTransition;
        try self.manager.validateStaging(self.id, self.path);
        return try beginStagingGeneration(self.alloc, self.manager, self.path, self.id, self.cleanup_scheduler, self.io, false);
    }

    pub fn promote(self: *PreparationTransition) !ExclusiveTransition {
        if (!self.active) return error.InvalidGenerationTransition;
        var fallback_io_impl: std.Io.Threaded = undefined;
        var fallback_io_owned = false;
        defer if (fallback_io_owned) fallback_io_impl.deinit();
        const io = self.io orelse blk: {
            fallback_io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
            fallback_io_owned = true;
            break :blk fallback_io_impl.io();
        };
        const path_key = try canonicalPathAllocWithIo(self.alloc, io, self.path);
        defer self.alloc.free(path_key);
        if (self.publication_lock) |file| closePublicationLockWithIo(io, file);
        self.publication_lock = null;
        const publication_lock = openPublicationLockWithIo(io, self.alloc, path_key, .exclusive) catch |promotion_err| {
            self.publication_lock = openPublicationLockWithIo(io, self.alloc, path_key, .shared) catch |reacquire_err| {
                std.log.err("generation preparation lock recovery failed phase=promote promotion_class={s} reacquire_class={s}", .{
                    @errorName(promotion_err),
                    @errorName(reacquire_err),
                });
                return reacquire_err;
            };
            return promotion_err;
        };
        self.manager.promotePreparation(self.path, self.id) catch |promotion_err| {
            closePublicationLockWithIo(io, publication_lock);
            self.publication_lock = openPublicationLockWithIo(io, self.alloc, path_key, .shared) catch |reacquire_err| {
                std.log.err("generation preparation lock recovery failed phase=state_promotion promotion_class={s} reacquire_class={s}", .{
                    @errorName(promotion_err),
                    @errorName(reacquire_err),
                });
                return reacquire_err;
            };
            return promotion_err;
        };
        self.active = false;
        const preparation_lock = self.preparation_lock.?;
        self.preparation_lock = null;
        return .{
            .manager = self.manager,
            .alloc = self.alloc,
            .path = self.path,
            .id = self.id,
            .publication_lock = publication_lock,
            .preparation_lock = preparation_lock,
            .cleanup_scheduler = self.cleanup_scheduler,
            .io = self.io,
        };
    }

    pub fn deinit(self: *PreparationTransition) void {
        if (!self.active) return;
        if (self.publication_lock) |file| {
            if (self.io) |io| closePublicationLockWithIo(io, file) else closePublicationLock(file);
        }
        self.publication_lock = null;
        if (self.preparation_lock) |file| {
            if (self.io) |io| closePublicationLockWithIo(io, file) else closePublicationLock(file);
        }
        self.preparation_lock = null;
        self.manager.finishPreparation(self.path, self.id);
        self.active = false;
    }
};

pub const ExclusiveTransition = struct {
    manager: *Manager,
    alloc: Allocator,
    path: []const u8,
    id: u64,
    publication_lock: ?std.Io.File = null,
    preparation_lock: ?std.Io.File = null,
    cleanup_scheduler: ?CleanupScheduler = null,
    io: ?std.Io = null,
    active: bool = true,

    pub fn validate(self: *const ExclusiveTransition, path: []const u8) !void {
        if (!self.active or !std.mem.eql(u8, self.path, path)) return error.InvalidGenerationTransition;
        try self.manager.validateExclusive(self.id, path);
    }

    pub fn deinit(self: *ExclusiveTransition) void {
        if (!self.active) return;
        if (self.publication_lock) |file| {
            if (self.io) |io| closePublicationLockWithIo(io, file) else closePublicationLock(file);
        }
        self.publication_lock = null;
        if (self.preparation_lock) |file| {
            if (self.io) |io| closePublicationLockWithIo(io, file) else closePublicationLock(file);
        }
        self.preparation_lock = null;
        self.manager.finishExclusive(self.path, self.id);
        self.active = false;
    }

    pub fn reconcilePublished(self: *ExclusiveTransition) !void {
        try self.validate(self.path);
        if (self.io) |io| {
            _ = try reconcilePublishedGenerationExclusive(self.alloc, io, self.path, self.cleanup_scheduler);
            return;
        }
        var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
        defer io_impl.deinit();
        _ = try reconcilePublishedGenerationExclusive(self.alloc, io_impl.io(), self.path, self.cleanup_scheduler);
    }

    pub fn beginStaging(self: *ExclusiveTransition) !StagedGeneration {
        try self.validate(self.path);
        return try beginStagingGeneration(self.alloc, self.manager, self.path, self.id, self.cleanup_scheduler, self.io, true);
    }
};

fn beginStagingGeneration(
    alloc: Allocator,
    manager: *Manager,
    path: []const u8,
    transition_id: u64,
    cleanup_scheduler: ?CleanupScheduler,
    io_override: ?std.Io,
    reconcile: bool,
) !StagedGeneration {
    const live_path = try alloc.dupe(u8, path);
    errdefer alloc.free(live_path);
    const live_path_z = try alloc.dupeZ(u8, path);
    errdefer alloc.free(live_path_z);
    // The basename is also the durable publication/cleanup identity. Mix wall
    // and monotonic time so an intent surviving a process or host restart does
    // not alias a new staging generation after local counters reset.
    const nonce = platform.time.realtimeNs() ^ std.math.rotl(u64, platform.time.monotonicNs(), 23);
    const staging_path = try std.fmt.allocPrint(alloc, "{s}.restore-stage-{x}-{x}", .{ path, transition_id, nonce });
    errdefer alloc.free(staging_path);
    const staging_path_z = try alloc.dupeZ(u8, staging_path);
    errdefer alloc.free(staging_path_z);

    if (io_override) |io| {
        return try beginStagingGenerationWithIo(
            alloc,
            manager,
            live_path,
            live_path_z,
            staging_path,
            staging_path_z,
            transition_id,
            cleanup_scheduler,
            io,
            reconcile,
        );
    }
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    var staged = try beginStagingGenerationWithIo(
        alloc,
        manager,
        live_path,
        live_path_z,
        staging_path,
        staging_path_z,
        transition_id,
        cleanup_scheduler,
        io_impl.io(),
        reconcile,
    );
    // The compatibility I/O exists only for this operation. Do not retain its
    // capability after the local runtime is deinitialized.
    staged.io = null;
    return staged;
}

fn beginStagingGenerationWithIo(
    alloc: Allocator,
    manager: *Manager,
    live_path: []u8,
    live_path_z: [:0]u8,
    staging_path: []u8,
    staging_path_z: [:0]u8,
    transition_id: u64,
    cleanup_scheduler: ?CleanupScheduler,
    io: std.Io,
    reconcile: bool,
) !StagedGeneration {
    if (reconcile) _ = try reconcilePublishedGenerationExclusive(alloc, io, live_path, cleanup_scheduler);
    if (pathExists(io, staging_path)) return error.GenerationStagingCollision;
    try fs_paths.createDirPathPortable(io, staging_path);
    errdefer std.Io.Dir.cwd().deleteTree(io, staging_path) catch {};
    return .{
        .alloc = alloc,
        .manager = manager,
        .transition_id = transition_id,
        .live_path = live_path,
        .live_path_z = live_path_z,
        .staging_path = staging_path,
        .staging_path_z = staging_path_z,
        .cleanup_scheduler = cleanup_scheduler,
        .io = io,
    };
}

pub const StagedGeneration = struct {
    alloc: Allocator,
    manager: *Manager,
    transition_id: u64,
    live_path: []u8,
    live_path_z: [:0]u8,
    staging_path: []u8,
    staging_path_z: [:0]u8,
    published: bool = false,
    publication_committed: bool = false,
    had_live_generation: bool = false,
    publication_outcome: ?PublicationOutcome = null,
    sealed: bool = false,
    preserve_retired: bool = false,
    cleanup_scheduler: ?CleanupScheduler = null,
    /// Runtime-owned I/O carried by runtime-backed transitions. Legacy direct
    /// callers leave this null and retain the historical local-I/O fallback.
    io: ?std.Io = null,
    closed: bool = false,

    pub fn path(self: *const StagedGeneration) []const u8 {
        return self.staging_path;
    }

    pub fn livePath(self: *const StagedGeneration) []const u8 {
        return self.live_path;
    }

    pub fn validatePath(self: *const StagedGeneration, path_value: []const u8) !void {
        if (self.closed or !std.mem.eql(u8, self.staging_path, path_value)) return error.InvalidGenerationTransition;
        try self.manager.validateStaging(self.transition_id, self.live_path);
    }

    pub fn validateLivePath(self: *const StagedGeneration, path_value: []const u8) !void {
        if (self.closed or !std.mem.eql(u8, self.live_path, path_value)) return error.InvalidGenerationTransition;
        try self.manager.validateStaging(self.transition_id, self.live_path);
    }

    /// Makes every staged file durable before serving admission is quiesced.
    /// Further writes to the candidate after sealing violate the transition
    /// contract and require another seal before publication.
    pub fn seal(self: *StagedGeneration) !void {
        if (self.io) |io| return try self.sealWithIo(io);
        var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
        defer io_impl.deinit();
        return try self.sealWithIo(io_impl.io());
    }

    fn sealWithIo(self: *StagedGeneration, io: std.Io) !void {
        if (self.closed or self.published) return error.InvalidGenerationTransition;
        try self.manager.validateStaging(self.transition_id, self.live_path);
        if (!pathExists(io, self.staging_path)) return error.GenerationStagingMissing;
        try syncTreePortable(self.alloc, io, self.staging_path);
        const parent = std.fs.path.dirname(self.staging_path) orelse if (std.fs.path.isAbsolute(self.staging_path)) "/" else ".";
        try fs_paths.syncDirPortable(io, parent);
        self.sealed = true;
    }

    /// Atomically installs the candidate while retaining the prior namespace
    /// for an explicit catalog validation. The exclusive transition keeps the
    /// candidate unservable until commitPublication() advances caller-owned
    /// admission. Every successful call must be followed by commitPublication
    /// or rollbackPublication.
    pub fn publishPrepared(self: *StagedGeneration) !PublicationOutcome {
        if (self.io) |io| return try self.publishPreparedWithIo(io);
        var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
        defer io_impl.deinit();
        return try self.publishPreparedWithIo(io_impl.io());
    }

    fn publishPreparedWithIo(self: *StagedGeneration, io: std.Io) !PublicationOutcome {
        if (self.closed or self.published) return error.InvalidGenerationTransition;
        try self.manager.validateExclusive(self.transition_id, self.live_path);
        if (!pathExists(io, self.staging_path)) return error.GenerationStagingMissing;
        const parent = std.fs.path.dirname(self.live_path) orelse if (std.fs.path.isAbsolute(self.live_path)) "/" else ".";
        const had_live_generation = pathExists(io, self.live_path);
        try writePublicationMarker(self.alloc, io, self.staging_path, .{
            .phase = .prepared,
            .retained_name = std.fs.path.basename(self.staging_path),
            .had_live_generation = had_live_generation,
        });
        if (!self.sealed) try self.sealWithIo(io);
        try fs_paths.syncDirPortable(io, parent);

        if (had_live_generation) {
            if (!exchangeDirectoriesAtomicSentinel(self.live_path_z, self.staging_path_z)) {
                return error.AtomicGenerationExchangeUnavailable;
            }
            // The namespace mutation has completed. Record that before any operation
            // that can fail so deinit never mistakes the retired root for staging.
            self.published = true;
            self.preserve_retired = true;
            const outcome = syncPublishedParent(io, parent);
            self.had_live_generation = true;
            self.publication_outcome = outcome;
            return outcome;
        }

        try std.Io.Dir.rename(std.Io.Dir.cwd(), self.staging_path, std.Io.Dir.cwd(), self.live_path, io);

        self.published = true;
        const outcome = syncPublishedParent(io, parent);
        self.publication_outcome = outcome;
        return outcome;
    }

    /// Commits a prepared namespace exchange and retires the previous root.
    /// Asynchronous cleanup scheduling failures remain reconciliation debt.
    pub fn commitPublication(self: *StagedGeneration) !void {
        if (self.io) |io| return try self.commitPublicationWithIo(io);
        var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
        defer io_impl.deinit();
        return try self.commitPublicationWithIo(io_impl.io());
    }

    fn commitPublicationWithIo(self: *StagedGeneration, io: std.Io) !void {
        if (self.closed or !self.published or self.publication_committed) return error.InvalidGenerationTransition;
        try self.manager.validateExclusive(self.transition_id, self.live_path);
        const parent = std.fs.path.dirname(self.live_path) orelse if (std.fs.path.isAbsolute(self.live_path)) "/" else ".";
        try writePublicationMarker(self.alloc, io, self.live_path, .{
            .phase = .committed,
            .retained_name = std.fs.path.basename(self.staging_path),
            .had_live_generation = self.had_live_generation,
        });
        if (self.publication_outcome.? == .durable) {
            if (self.had_live_generation) {
                // Persist cleanup debt before the mutable recovery marker can
                // be removed. The intent is scoped to this retained generation
                // and survives queue loss, worker failure, and later publishes.
                try writeCleanupIntent(
                    self.alloc,
                    io,
                    self.live_path,
                    std.fs.path.basename(self.staging_path),
                );
                if (self.cleanup_scheduler != null) {
                    scheduleRetiredGenerationCleanupAfterPublication(
                        self.cleanup_scheduler,
                        self.staging_path,
                        parent,
                        self.live_path,
                    ) catch |err| {
                        // The immutable cleanup intent remains durable. A
                        // later read admission can rediscover and retire the
                        // old generation without touching publication state.
                        std.log.warn("retired generation cleanup scheduling deferred path={s} err={s}", .{ self.staging_path, @errorName(err) });
                    };
                } else {
                    deleteRetiredGenerationPaths(
                        self.alloc,
                        io,
                        &.{self.staging_path},
                        parent,
                    ) catch |err| {
                        // Keep both the immutable cleanup intent and generated
                        // stage name as reconciliation debt if synchronous
                        // standalone cleanup cannot complete.
                        std.log.warn("retired generation cleanup deferred path={s} err={s}", .{ self.staging_path, @errorName(err) });
                        self.publication_committed = true;
                        return;
                    };
                    acknowledgeCleanupIntent(
                        self.alloc,
                        io,
                        self.live_path,
                        std.fs.path.basename(self.staging_path),
                    ) catch |err| {
                        std.log.warn("retired generation cleanup intent acknowledgement deferred path={s} err={s}", .{ self.staging_path, @errorName(err) });
                    };
                }
                _ = clearPublicationMarker(self.alloc, io, self.live_path);
            } else {
                _ = clearPublicationMarker(self.alloc, io, self.live_path);
            }
        } else if (self.had_live_generation) {
            std.log.warn("retaining previous generation after uncertain publication path={s}", .{self.staging_path});
        }
        self.publication_committed = true;
    }

    /// Restores the prior namespace after a post-exchange validation failure.
    /// The exclusive generation transition prevents either namespace from
    /// being admitted while this exchange is in flight.
    pub fn rollbackPublication(self: *StagedGeneration) !void {
        if (self.io) |io| return try self.rollbackPublicationWithIo(io);
        var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
        defer io_impl.deinit();
        return try self.rollbackPublicationWithIo(io_impl.io());
    }

    fn rollbackPublicationWithIo(self: *StagedGeneration, io: std.Io) !void {
        if (self.closed or !self.published or self.publication_committed) return error.InvalidGenerationTransition;
        try self.manager.validateExclusive(self.transition_id, self.live_path);
        const parent = std.fs.path.dirname(self.live_path) orelse if (std.fs.path.isAbsolute(self.live_path)) "/" else ".";
        if (self.had_live_generation) {
            if (!exchangeDirectoriesAtomicSentinel(self.live_path_z, self.staging_path_z)) {
                return error.AtomicGenerationExchangeUnavailable;
            }
        } else {
            try std.Io.Dir.rename(std.Io.Dir.cwd(), self.live_path, std.Io.Dir.cwd(), self.staging_path, io);
        }
        self.published = false;
        self.preserve_retired = false;
        self.had_live_generation = false;
        self.publication_outcome = null;
        if (syncPublishedParent(io, parent) == .durability_uncertain) return error.GenerationRollbackDurabilityUncertain;
    }

    /// Convenience API for callers whose publication has no external
    /// validation step.
    pub fn publish(self: *StagedGeneration) !PublicationOutcome {
        const outcome = try self.publishPrepared();
        try self.commitPublication();
        return outcome;
    }

    pub fn deinit(self: *StagedGeneration) void {
        if (self.closed) return;
        if (self.io) |io| return self.deinitWithIo(io);
        var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
        defer io_impl.deinit();
        self.deinitWithIo(io_impl.io());
    }

    fn deinitWithIo(self: *StagedGeneration, io: std.Io) void {
        if (self.published and !self.publication_committed) {
            self.rollbackPublicationWithIo(io) catch |err| {
                std.log.err("prepared generation rollback failed path={s} err={s}", .{ self.live_path, @errorName(err) });
                self.preserve_retired = true;
            };
        }
        if (self.published) {
            if (!self.preserve_retired) std.Io.Dir.cwd().deleteTree(io, self.staging_path) catch {};
        } else {
            std.Io.Dir.cwd().deleteTree(io, self.staging_path) catch {};
        }
        self.alloc.free(self.staging_path);
        self.alloc.free(self.staging_path_z);
        self.alloc.free(self.live_path);
        self.alloc.free(self.live_path_z);
        self.closed = true;
    }
};

fn pathExists(io: std.Io, path: []const u8) bool {
    _ = std.Io.Dir.cwd().statFile(io, path, .{}) catch return false;
    return true;
}

fn publicationMarkerPathAlloc(alloc: Allocator, root: []const u8) ![]u8 {
    return try std.fs.path.join(alloc, &.{ root, publication_marker_name });
}

fn publicationMarkerTmpPathAlloc(alloc: Allocator, root: []const u8) ![]u8 {
    return try std.fs.path.join(alloc, &.{ root, publication_marker_tmp_name });
}

fn writePublicationMarker(alloc: Allocator, io: std.Io, root: []const u8, marker: PublicationMarker) !void {
    const encoded = try std.json.Stringify.valueAlloc(alloc, marker, .{});
    defer alloc.free(encoded);
    if (encoded.len > max_publication_marker_bytes) return error.InvalidGenerationPublicationMarker;
    const marker_path = try publicationMarkerPathAlloc(alloc, root);
    defer alloc.free(marker_path);
    const tmp_path = try publicationMarkerTmpPathAlloc(alloc, root);
    defer alloc.free(tmp_path);
    errdefer std.Io.Dir.cwd().deleteFile(io, tmp_path) catch {};
    {
        var file = try fs_paths.createFilePortable(io, tmp_path, .{ .truncate = true });
        defer file.close(io);
        var writer_buffer: [1024]u8 = undefined;
        var writer = file.writer(io, &writer_buffer);
        try writer.interface.writeAll(encoded);
        try writer.end();
        try file.sync(io);
    }
    try std.Io.Dir.rename(std.Io.Dir.cwd(), tmp_path, std.Io.Dir.cwd(), marker_path, io);
    try fs_paths.syncDirPortable(io, root);
}

fn readPublicationMarker(alloc: Allocator, io: std.Io, root: []const u8) !?OwnedPublicationMarker {
    const marker_path = try publicationMarkerPathAlloc(alloc, root);
    defer alloc.free(marker_path);
    const encoded = std.Io.Dir.cwd().readFileAlloc(io, marker_path, alloc, .limited(max_publication_marker_bytes)) catch |err| switch (err) {
        // NotDir: the live root is a file (e.g. an Antfly Lite *.aflite
        // database), so a marker can never exist beneath it.
        error.FileNotFound, error.NotDir => return null,
        else => return err,
    };
    defer alloc.free(encoded);
    return try parsePublicationMarker(alloc, encoded);
}

fn parsePublicationMarker(alloc: Allocator, encoded: []const u8) !OwnedPublicationMarker {
    var parsed = std.json.parseFromSlice(PublicationMarker, alloc, encoded, .{ .allocate = .alloc_always }) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        else => return error.InvalidGenerationPublicationMarker,
    };
    defer parsed.deinit();
    if (parsed.value.version != 2 or parsed.value.retained_name.len == 0 or
        std.mem.indexOfAny(u8, parsed.value.retained_name, "/\\") != null)
    {
        return error.InvalidGenerationPublicationMarker;
    }
    return .{
        .phase = parsed.value.phase,
        .retained_name = try alloc.dupe(u8, parsed.value.retained_name),
        .had_live_generation = parsed.value.had_live_generation,
    };
}

test "generation publication marker parsing preserves allocator exhaustion" {
    const encoded =
        \\{"version":2,"phase":"prepared","retained_name":"table.retained","had_live_generation":true}
    ;
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    try std.testing.expectError(error.OutOfMemory, parsePublicationMarker(failing.allocator(), encoded));
    try std.testing.expectError(error.InvalidGenerationPublicationMarker, parsePublicationMarker(std.testing.allocator, "{"));
}

fn clearPublicationMarker(alloc: Allocator, io: std.Io, root: []const u8) bool {
    const marker_path = publicationMarkerPathAlloc(alloc, root) catch return false;
    defer alloc.free(marker_path);
    std.Io.Dir.cwd().deleteFile(io, marker_path) catch |err| switch (err) {
        error.FileNotFound, error.NotDir => return true,
        else => {
            std.log.warn("generation publication marker cleanup deferred path={s} err={s}", .{ marker_path, @errorName(err) });
            return false;
        },
    };
    fs_paths.syncDirPortable(io, root) catch |err| {
        std.log.warn("generation publication marker sync deferred path={s} err={s}", .{ root, @errorName(err) });
        return false;
    };
    return true;
}

fn cleanupIntentDirAlloc(alloc: Allocator, live_path: []const u8) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{s}{s}", .{ live_path, cleanup_intent_dir_suffix });
}

fn cleanupIntentPathAlloc(
    alloc: Allocator,
    live_path: []const u8,
    retained_name: []const u8,
) ![]u8 {
    const validated = try retainedGenerationPathAlloc(alloc, live_path, retained_name);
    alloc.free(validated);
    const intent_dir = try cleanupIntentDirAlloc(alloc, live_path);
    defer alloc.free(intent_dir);
    return try std.fmt.allocPrint(alloc, "{s}/{s}{s}", .{
        intent_dir,
        retained_name,
        cleanup_intent_file_suffix,
    });
}

fn writeCleanupIntent(
    alloc: Allocator,
    io: std.Io,
    live_path: []const u8,
    retained_name: []const u8,
) !void {
    const validated = try retainedGenerationPathAlloc(alloc, live_path, retained_name);
    defer alloc.free(validated);
    const intent_dir = try cleanupIntentDirAlloc(alloc, live_path);
    defer alloc.free(intent_dir);
    try fs_paths.createDirPathPortable(io, intent_dir);
    const intent_path = try cleanupIntentPathAlloc(alloc, live_path, retained_name);
    defer alloc.free(intent_path);
    const tmp_path = try std.fmt.allocPrint(alloc, "{s}{s}", .{ intent_path, cleanup_intent_tmp_suffix });
    defer alloc.free(tmp_path);
    errdefer std.Io.Dir.cwd().deleteFile(io, tmp_path) catch {};
    const encoded = try std.json.Stringify.valueAlloc(alloc, CleanupIntent{
        .retained_name = retained_name,
    }, .{});
    defer alloc.free(encoded);
    if (encoded.len > max_cleanup_intent_bytes) return error.InvalidGenerationCleanupIntent;
    {
        var file = try fs_paths.createFilePortable(io, tmp_path, .{ .truncate = true });
        defer file.close(io);
        var writer_buffer: [1024]u8 = undefined;
        var writer = file.writer(io, &writer_buffer);
        try writer.interface.writeAll(encoded);
        try writer.end();
        try file.sync(io);
    }
    try std.Io.Dir.rename(std.Io.Dir.cwd(), tmp_path, std.Io.Dir.cwd(), intent_path, io);
    try fs_paths.syncDirPortable(io, intent_dir);
}

fn parseCleanupIntentRetainedName(
    alloc: Allocator,
    encoded: []const u8,
) ![]u8 {
    var parsed = std.json.parseFromSlice(CleanupIntent, alloc, encoded, .{
        .allocate = .alloc_always,
    }) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        else => return error.InvalidGenerationCleanupIntent,
    };
    defer parsed.deinit();
    if (parsed.value.version != 1 or parsed.value.retained_name.len == 0 or
        std.mem.indexOfAny(u8, parsed.value.retained_name, "/\\") != null)
    {
        return error.InvalidGenerationCleanupIntent;
    }
    return try alloc.dupe(u8, parsed.value.retained_name);
}

fn acknowledgeCleanupIntent(
    alloc: Allocator,
    io: std.Io,
    live_path: []const u8,
    retained_name: []const u8,
) !void {
    const intent_path = try cleanupIntentPathAlloc(alloc, live_path, retained_name);
    defer alloc.free(intent_path);
    std.Io.Dir.cwd().deleteFile(io, intent_path) catch |err| switch (err) {
        error.FileNotFound, error.NotDir => return,
        else => return err,
    };
    const intent_dir = try cleanupIntentDirAlloc(alloc, live_path);
    defer alloc.free(intent_dir);
    try fs_paths.syncDirPortable(io, intent_dir);
}

const RetiredGenerationCleanupBatch = struct {
    alloc: Allocator,
    io: std.Io,
    scheduler: CleanupScheduler,
    paths: [][]u8,
    parent: []u8,
    live_path: []u8,
    retry_round: u8,

    fn run(ptr: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (builtin.is_test and test_block_retired_cleanup.load(.acquire)) {
            test_retired_cleanup_started.store(true, .release);
            while (test_block_retired_cleanup.load(.acquire)) platform.time.yieldBriefly();
        }
        var retry_round = self.retry_round;
        while (true) {
            const max_attempts = 3;
            var attempt: usize = 0;
            while (true) : (attempt += 1) {
                const result = cleanup: {
                    if (consumeTestRetiredCleanupFailure()) break :cleanup error.TestRetiredCleanupFailure;
                    deleteRetiredGenerationPaths(self.alloc, self.io, self.paths, self.parent) catch |err| break :cleanup err;
                    for (self.paths) |path| {
                        acknowledgeCleanupIntent(
                            self.alloc,
                            self.io,
                            self.live_path,
                            std.fs.path.basename(path),
                        ) catch |err| break :cleanup err;
                    }
                    return;
                };
                if (attempt + 1 != max_attempts) {
                    const delay_ms: i64 = if (attempt == 0) 10 else 100;
                    self.io.sleep(std.Io.Duration.fromMilliseconds(delay_ms), .awake) catch return result;
                    continue;
                }

                if (!self.scheduler.lane.isAccepting()) return;
                const retry_delay_ms = retiredCleanupRetryDelayMs(retry_round, self.live_path);
                std.log.warn("retired generation cleanup remains pending; retrying path={s} delay_ms={} err={s}", .{
                    self.live_path,
                    retry_delay_ms,
                    @errorName(result),
                });
                if (!try waitForRetiredCleanupRetry(self, retry_delay_ms)) return;
                const next_round = if (retry_round == std.math.maxInt(u8)) retry_round else retry_round + 1;
                scheduleRetiredGenerationCleanupBatchAtRound(
                    self.scheduler,
                    self.paths,
                    self.parent,
                    self.live_path,
                    next_round,
                ) catch |err| switch (err) {
                    error.BackendRuntimeShuttingDown,
                    error.BackgroundOwnerClosing,
                    error.BackgroundOwnerClosed,
                    => return,
                    else => {
                        // Keep the current payload alive when allocating or
                        // admitting its successor fails. The durable intent is
                        // never left without an in-process retry while the
                        // runtime can still make progress.
                        retry_round = next_round;
                        break;
                    },
                };
                return;
            }
        }
    }

    fn deinit(ptr: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const alloc = self.alloc;
        for (self.paths) |path| alloc.free(path);
        alloc.free(self.paths);
        alloc.free(self.parent);
        alloc.free(self.live_path);
        alloc.destroy(self);
    }
};

fn consumeTestRetiredCleanupFailure() bool {
    if (!builtin.is_test) return false;
    var remaining = test_retired_cleanup_failures_remaining.load(.acquire);
    while (remaining != 0) {
        if (test_retired_cleanup_failures_remaining.cmpxchgWeak(
            remaining,
            remaining - 1,
            .acq_rel,
            .acquire,
        )) |observed| {
            remaining = observed;
            continue;
        }
        return true;
    }
    return false;
}

fn retiredCleanupRetryDelayMs(retry_round: u8, live_path: []const u8) i64 {
    if (builtin.is_test) return 1;
    const shift: u6 = @intCast(@min(retry_round, 5));
    const exponential_ms = @as(u64, 1_000) << shift;
    const base_ms = @min(exponential_ms, 30_000);
    const jitter_window = @divFloor(base_ms, 4) + 1;
    const jitter_ms = std.hash.Wyhash.hash(retry_round, live_path) % jitter_window;
    return @intCast(base_ms + jitter_ms);
}

fn waitForRetiredCleanupRetry(self: *RetiredGenerationCleanupBatch, delay_ms: i64) !bool {
    var remaining_ms = delay_ms;
    while (remaining_ms > 0) {
        if (!self.scheduler.lane.isAccepting()) return false;
        const slice_ms = @min(remaining_ms, 100);
        try self.io.sleep(std.Io.Duration.fromMilliseconds(slice_ms), .awake);
        remaining_ms -= slice_ms;
    }
    return self.scheduler.lane.isAccepting();
}

fn deleteRetiredGenerationPaths(alloc: Allocator, io: std.Io, paths: []const []const u8, parent: []const u8) !void {
    const lock_path = try std.fs.path.join(alloc, &.{ parent, retired_cleanup_lock_name });
    defer alloc.free(lock_path);
    const cleanup_lock = std.Io.Dir.cwd().createFile(io, lock_path, .{
        .read = true,
        .truncate = false,
        .lock = .exclusive,
        .lock_nonblocking = false,
    }) catch |err| switch (err) {
        // The parent was concurrently retired, so every target below it is
        // already unreachable and cleanup is complete.
        error.FileNotFound, error.NotDir => return,
        else => return err,
    };
    defer cleanup_lock.close(io);

    var first_error: ?anyerror = null;
    for (paths) |path| {
        if (!pathExists(io, path)) continue;
        std.Io.Dir.cwd().deleteTree(io, path) catch |err| switch (err) {
            error.NotDir => {},
            else => if (first_error == null) {
                first_error = err;
            },
        };
    }
    fs_paths.syncDirPortable(io, parent) catch |err| switch (err) {
        error.FileNotFound, error.NotDir => return,
        else => return err,
    };
    if (first_error) |err| return err;
}

fn scheduleRetiredGenerationCleanupAfterPublication(
    scheduler: ?CleanupScheduler,
    path: []const u8,
    parent: []const u8,
    live_path: []const u8,
) !void {
    return try scheduleRetiredGenerationCleanupBatch(scheduler, &.{path}, parent, live_path);
}

fn scheduleRetiredGenerationCleanupBatch(
    scheduler: ?CleanupScheduler,
    paths: []const []const u8,
    parent: []const u8,
    live_path: []const u8,
) !void {
    const active = scheduler orelse return;
    return try scheduleRetiredGenerationCleanupBatchAtRound(active, paths, parent, live_path, 0);
}

fn scheduleRetiredGenerationCleanupBatchAtRound(
    active: CleanupScheduler,
    paths: []const []const u8,
    parent: []const u8,
    live_path: []const u8,
    retry_round: u8,
) !void {
    if (paths.len == 0) return;
    const work = try active.alloc.create(RetiredGenerationCleanupBatch);
    errdefer active.alloc.destroy(work);
    const owned_paths = try active.alloc.alloc([]u8, paths.len);
    var initialized: usize = 0;
    errdefer {
        for (owned_paths[0..initialized]) |path| active.alloc.free(path);
        active.alloc.free(owned_paths);
    }
    for (paths, 0..) |path, i| {
        owned_paths[i] = try active.alloc.dupe(u8, path);
        initialized += 1;
    }
    const owned_parent = try active.alloc.dupe(u8, parent);
    errdefer active.alloc.free(owned_parent);
    const owned_live_path = try active.alloc.dupe(u8, live_path);
    errdefer active.alloc.free(owned_live_path);
    work.* = .{
        .alloc = active.alloc,
        .io = active.io,
        .scheduler = active,
        .paths = owned_paths,
        .parent = owned_parent,
        .live_path = owned_live_path,
        .retry_round = retry_round,
    };
    try active.lane.submit(.{
        .owner_id = active.owner_id,
        .class = .cleanup,
        .ptr = work,
        .run = RetiredGenerationCleanupBatch.run,
        .deinit = RetiredGenerationCleanupBatch.deinit,
    });
}

fn isGeneratedStageName(name: []const u8, prefix: []const u8) bool {
    if (!std.mem.startsWith(u8, name, prefix)) return false;
    const suffix = name[prefix.len..];
    const separator = std.mem.indexOfScalar(u8, suffix, '-') orelse return false;
    if (separator == 0 or separator + 1 == suffix.len) return false;
    if (std.mem.indexOfScalarPos(u8, suffix, separator + 1, '-') != null) return false;
    for (suffix[0..separator]) |byte| if (!std.ascii.isHex(byte)) return false;
    for (suffix[separator + 1 ..]) |byte| if (!std.ascii.isHex(byte)) return false;
    return true;
}

fn retainedGenerationPathAlloc(
    alloc: Allocator,
    live_path: []const u8,
    retained_name: []const u8,
) ![]u8 {
    const parent = std.fs.path.dirname(live_path) orelse if (std.fs.path.isAbsolute(live_path)) "/" else ".";
    const live_name = std.fs.path.basename(live_path);
    const stage_prefix = try std.fmt.allocPrint(alloc, "{s}.restore-stage-", .{live_name});
    defer alloc.free(stage_prefix);
    if (!isGeneratedStageName(retained_name, stage_prefix)) return error.InvalidGenerationPublicationMarker;
    return try std.fs.path.join(alloc, &.{ parent, retained_name });
}

fn rollbackPreparedPublishedGeneration(
    alloc: Allocator,
    io: std.Io,
    live_path: []const u8,
    marker: OwnedPublicationMarker,
) !void {
    const retained_path = try retainedGenerationPathAlloc(alloc, live_path, marker.retained_name);
    defer alloc.free(retained_path);
    const parent = std.fs.path.dirname(live_path) orelse if (std.fs.path.isAbsolute(live_path)) "/" else ".";

    if (marker.had_live_generation) {
        if (!pathExists(io, retained_path)) return error.GenerationRollbackRootMissing;
        var retained_marker = try readPublicationMarker(alloc, io, retained_path);
        defer if (retained_marker) |*value| value.deinit(alloc);
        if (retained_marker != null) return error.InvalidGenerationRollbackRoot;
        const live_path_z = try alloc.dupeZ(u8, live_path);
        defer alloc.free(live_path_z);
        const retained_path_z = try alloc.dupeZ(u8, retained_path);
        defer alloc.free(retained_path_z);
        if (!exchangeDirectoriesAtomicSentinel(live_path_z, retained_path_z)) {
            return error.AtomicGenerationExchangeUnavailable;
        }
    } else {
        if (pathExists(io, retained_path)) return error.GenerationStagingCollision;
        try std.Io.Dir.rename(std.Io.Dir.cwd(), live_path, std.Io.Dir.cwd(), retained_path, io);
    }
    if (syncPublishedParent(io, parent) == .durability_uncertain) {
        return error.GenerationRollbackDurabilityUncertain;
    }
}

fn cleanupStagedGenerations(
    alloc: Allocator,
    io: std.Io,
    live_path: []const u8,
    scheduler: ?CleanupScheduler,
    deferred_cleanup: *std.ArrayListUnmanaged([]u8),
) !void {
    const parent = std.fs.path.dirname(live_path) orelse if (std.fs.path.isAbsolute(live_path)) "/" else ".";
    const live_name = std.fs.path.basename(live_path);
    const stage_prefix = try std.fmt.allocPrint(alloc, "{s}.restore-stage-", .{live_name});
    defer alloc.free(stage_prefix);

    var dir = (if (std.fs.path.isAbsolute(parent))
        std.Io.Dir.openDirAbsolute(io, parent, .{ .iterate = true })
    else
        std.Io.Dir.cwd().openDir(io, parent, .{ .iterate = true })) catch |err| switch (err) {
        error.FileNotFound, error.NotDir => return,
        else => return err,
    };
    defer dir.close(io);

    var iterator = dir.iterate();
    while (try iterator.next(io)) |entry| {
        if (entry.kind != .directory or !isGeneratedStageName(entry.name, stage_prefix)) continue;
        const stale_path = try std.fs.path.join(alloc, &.{ parent, entry.name });
        defer alloc.free(stale_path);
        var marker = readPublicationMarker(alloc, io, stale_path) catch |err| {
            std.log.warn("stale generation marker validation deferred path={s} err={s}", .{ stale_path, @errorName(err) });
            continue;
        };
        defer if (marker) |*value| value.deinit(alloc);
        // Converting every discoverable staging directory to an immutable
        // intent makes the filesystem the cleanup queue. Scheduling can now
        // fail without losing the path on the next reconciliation or restart.
        try writeCleanupIntent(alloc, io, live_path, entry.name);
    }

    const intent_dir_path = try cleanupIntentDirAlloc(alloc, live_path);
    defer alloc.free(intent_dir_path);
    var intent_dir = (if (std.fs.path.isAbsolute(intent_dir_path))
        std.Io.Dir.openDirAbsolute(io, intent_dir_path, .{ .iterate = true })
    else
        std.Io.Dir.cwd().openDir(io, intent_dir_path, .{ .iterate = true })) catch |err| switch (err) {
        error.FileNotFound, error.NotDir => return,
        else => return err,
    };
    defer intent_dir.close(io);

    var cleanup_paths = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (cleanup_paths.items) |path| alloc.free(path);
        cleanup_paths.deinit(alloc);
    }
    var intent_iterator = intent_dir.iterate();
    while (try intent_iterator.next(io)) |entry| {
        if (entry.kind != .file or !std.mem.endsWith(u8, entry.name, cleanup_intent_file_suffix)) continue;
        if (cleanup_paths.items.len == max_reconciled_paths) return error.TooManyGenerationCleanupIntents;
        const intent_path = try std.fs.path.join(alloc, &.{ intent_dir_path, entry.name });
        defer alloc.free(intent_path);
        const encoded = try std.Io.Dir.cwd().readFileAlloc(
            io,
            intent_path,
            alloc,
            .limited(max_cleanup_intent_bytes),
        );
        defer alloc.free(encoded);
        const retained_name = try parseCleanupIntentRetainedName(alloc, encoded);
        defer alloc.free(retained_name);
        const expected_name = try std.fmt.allocPrint(alloc, "{s}{s}", .{
            retained_name,
            cleanup_intent_file_suffix,
        });
        defer alloc.free(expected_name);
        if (!std.mem.eql(u8, expected_name, entry.name)) return error.InvalidGenerationCleanupIntent;
        const cleanup_path = try retainedGenerationPathAlloc(alloc, live_path, retained_name);
        errdefer alloc.free(cleanup_path);
        try cleanup_paths.append(alloc, cleanup_path);
    }

    if (cleanup_paths.items.len == 0) return;
    if (scheduler != null) {
        std.log.info("scheduling durable generation cleanup path={s} count={}", .{ live_path, cleanup_paths.items.len });
        try scheduleRetiredGenerationCleanupBatch(scheduler, cleanup_paths.items, parent, live_path);
    } else {
        try deferred_cleanup.ensureUnusedCapacity(alloc, cleanup_paths.items.len);
        for (cleanup_paths.items) |path| deferred_cleanup.appendAssumeCapacity(path);
        cleanup_paths.clearRetainingCapacity();
    }
}

fn reconcilePublishedGeneration(
    alloc: Allocator,
    io: std.Io,
    live_path: []const u8,
    scheduler: ?CleanupScheduler,
    deferred_cleanup: *std.ArrayListUnmanaged([]u8),
) !bool {
    var marker = try readPublicationMarker(alloc, io, live_path);
    defer if (marker) |*value| value.deinit(alloc);
    if (marker) |value| {
        const parent = std.fs.path.dirname(live_path) orelse if (std.fs.path.isAbsolute(live_path)) "/" else ".";
        switch (value.phase) {
            .prepared => try rollbackPreparedPublishedGeneration(alloc, io, live_path, value),
            .committed => {
                const retained_path = try retainedGenerationPathAlloc(alloc, live_path, value.retained_name);
                defer alloc.free(retained_path);
                if (builtin.is_test and test_fail_reconciliation_sync) {
                    test_fail_reconciliation_sync = false;
                    return error.GenerationDurabilityUncertain;
                }
                fs_paths.syncDirPortable(io, parent) catch return error.GenerationDurabilityUncertain;
                if (pathExists(io, retained_path)) {
                    try writeCleanupIntent(alloc, io, live_path, value.retained_name);
                }
            },
        }
    }

    cleanupStagedGenerations(alloc, io, live_path, scheduler, deferred_cleanup) catch |err| {
        std.log.warn("stale generation reconciliation deferred path={s} err={s}", .{ live_path, @errorName(err) });
        return false;
    };
    return clearPublicationMarker(alloc, io, live_path);
}

fn reconcilePublishedGenerationExclusive(
    alloc: Allocator,
    io: std.Io,
    live_path: []const u8,
    scheduler: ?CleanupScheduler,
) !bool {
    var deferred_cleanup = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (deferred_cleanup.items) |stale_path| alloc.free(stale_path);
        deferred_cleanup.deinit(alloc);
    }
    const reconciled = try reconcilePublishedGeneration(alloc, io, live_path, scheduler, &deferred_cleanup);
    if (deferred_cleanup.items.len > 0) {
        const parent = std.fs.path.dirname(live_path) orelse if (std.fs.path.isAbsolute(live_path)) "/" else ".";
        try deleteRetiredGenerationPaths(alloc, io, deferred_cleanup.items, parent);
        for (deferred_cleanup.items) |stale_path| {
            try acknowledgeCleanupIntent(alloc, io, live_path, std.fs.path.basename(stale_path));
        }
    }
    return reconciled;
}

pub fn acquirePublishedGenerationRead(alloc: Allocator, path: []const u8) !?ReadLease {
    return try acquirePublishedGenerationReadWithRuntime(alloc, path, null);
}

pub fn acquirePublishedGenerationReadWithRuntime(alloc: Allocator, path: []const u8, runtime: ?*background_runtime.BackendRuntime) !?ReadLease {
    var fallback_io_impl: std.Io.Threaded = undefined;
    var fallback_io_owned = false;
    defer if (fallback_io_owned) fallback_io_impl.deinit();
    const io = if (runtime) |active|
        active.filesystemIo() orelse return error.BackendRuntimeIoUnavailable
    else blk: {
        fallback_io_impl = std.Io.Threaded.init(alloc, .{});
        fallback_io_owned = true;
        break :blk fallback_io_impl.io();
    };
    const retained_io: ?std.Io = if (runtime != null) io else null;
    var reconciliation = reconcile: while (true) {
        if (try process_manager.beginReconciliationWithIo(path, io)) |value| break :reconcile value;

        const path_key = try canonicalPathAllocWithIo(process_manager_allocator, io, path);
        defer process_manager_allocator.free(path_key);
        const publication_lock = try openPublicationLockWithIo(io, process_manager_allocator, path_key, .shared);
        var lock_owned = true;
        errdefer if (lock_owned) closePublicationLockWithIo(io, publication_lock);
        if (try process_manager.beginReadIfReconciledCanonical(path, path_key)) |read_value| {
            var read_lease = read_value;
            read_lease.publication_lock = publication_lock;
            read_lease.io = retained_io;
            lock_owned = false;
            return read_lease;
        }
        closePublicationLockWithIo(io, publication_lock);
        lock_owned = false;
    };
    defer reconciliation.deinit();
    var publication_lock = try openPublicationLockWithIo(io, process_manager_allocator, reconciliation.path_key, .exclusive);
    errdefer closePublicationLockWithIo(io, publication_lock);
    var deferred_cleanup = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (deferred_cleanup.items) |stale_path| alloc.free(stale_path);
        deferred_cleanup.deinit(alloc);
    }
    const cleanup_scheduler = CleanupScheduler.fromRuntime(runtime);
    const reconciled = try reconcilePublishedGeneration(alloc, io, path, cleanup_scheduler, &deferred_cleanup);
    try publication_lock.downgradeLock(io);
    var read_lease = try reconciliation.promoteToRead(publication_lock, retained_io, reconciled);
    errdefer read_lease.deinit();
    if (deferred_cleanup.items.len > 0) {
        const parent = std.fs.path.dirname(path) orelse if (std.fs.path.isAbsolute(path)) "/" else ".";
        deleteRetiredGenerationPaths(alloc, io, deferred_cleanup.items, parent) catch |err| {
            std.log.warn("stale generation cleanup remains retryable after read admission path={s} count={} err={s}", .{ path, deferred_cleanup.items.len, @errorName(err) });
            return read_lease;
        };
        for (deferred_cleanup.items) |stale_path| {
            acknowledgeCleanupIntent(alloc, io, path, std.fs.path.basename(stale_path)) catch |err| {
                std.log.warn("stale generation cleanup acknowledgement remains retryable path={s} err={s}", .{ stale_path, @errorName(err) });
            };
        }
    }
    return read_lease;
}

fn syncTreePortable(alloc: Allocator, io: std.Io, root: []const u8) !void {
    if (builtin.os.tag == .windows or builtin.os.tag == .wasi or builtin.os.tag == .freestanding) return;

    var dir = if (std.fs.path.isAbsolute(root))
        try std.Io.Dir.openDirAbsolute(io, root, .{ .iterate = true })
    else
        try std.Io.Dir.cwd().openDir(io, root, .{ .iterate = true });
    defer dir.close(io);

    var iterator = dir.iterate();
    while (try iterator.next(io)) |entry| {
        const child = try std.fs.path.join(alloc, &.{ root, entry.name });
        defer alloc.free(child);
        switch (entry.kind) {
            .directory => try syncTreePortable(alloc, io, child),
            .file => {
                const file = if (std.fs.path.isAbsolute(child))
                    try std.Io.Dir.openFileAbsolute(io, child, .{})
                else
                    try std.Io.Dir.cwd().openFile(io, child, .{});
                defer file.close(io);
                try file.sync(io);
            },
            else => {},
        }
    }
    try fs_paths.syncDirPortable(io, root);
}

fn syncPublishedParent(io: std.Io, parent: []const u8) PublicationOutcome {
    if (builtin.is_test and test_fail_post_publish_sync) {
        test_fail_post_publish_sync = false;
        return .durability_uncertain;
    }
    fs_paths.syncDirPortable(io, parent) catch |err| {
        std.log.err("published generation parent sync failed path={s} err={s}", .{ parent, @errorName(err) });
        return .durability_uncertain;
    };
    return .durable;
}

fn exchangeDirectoriesAtomic(alloc: Allocator, left: []const u8, right: []const u8) !bool {
    const left_z = try alloc.dupeZ(u8, left);
    defer alloc.free(left_z);
    const right_z = try alloc.dupeZ(u8, right);
    defer alloc.free(right_z);
    return exchangeDirectoriesAtomicSentinel(left_z, right_z);
}

fn exchangeDirectoriesAtomicSentinel(left_z: [:0]const u8, right_z: [:0]const u8) bool {
    if (builtin.is_test and test_disable_atomic_exchange) return false;
    if (comptime builtin.os.tag == .macos and builtin.link_libc) {
        const Darwin = struct {
            extern "c" fn renameatx_np(
                old_dir_fd: c_int,
                old_path: [*:0]const u8,
                new_dir_fd: c_int,
                new_path: [*:0]const u8,
                flags: c_uint,
            ) c_int;
        };
        const rename_swap: c_uint = 0x0000_0002;
        return Darwin.renameatx_np(std.posix.AT.FDCWD, left_z.ptr, std.posix.AT.FDCWD, right_z.ptr, rename_swap) == 0;
    }
    if (comptime builtin.os.tag == .linux) {
        const linux = std.os.linux;
        return linux.errno(linux.renameat2(
            linux.AT.FDCWD,
            left_z.ptr,
            linux.AT.FDCWD,
            right_z.ptr,
            .{ .EXCHANGE = true },
        )) == .SUCCESS;
    }
    return false;
}

const process_manager_allocator = if (builtin.link_libc) std.heap.c_allocator else std.heap.page_allocator;
var process_manager: Manager = Manager.init(process_manager_allocator);

pub fn beginProcessExclusive(path: []const u8) !ExclusiveTransition {
    return try process_manager.beginExclusive(path);
}

pub fn beginProcessExclusiveWithRuntime(path: []const u8, runtime: ?*background_runtime.BackendRuntime) !ExclusiveTransition {
    return try beginProcessExclusiveWithRuntimeAndIo(path, runtime, null);
}

pub fn beginProcessExclusiveWithRuntimeAndIo(
    path: []const u8,
    runtime: ?*background_runtime.BackendRuntime,
    io_override: ?std.Io,
) !ExclusiveTransition {
    const io = if (runtime) |active| active.filesystemIo() orelse io_override else io_override;
    if (io == null) {
        if (runtime != null) return error.BackendRuntimeIoUnavailable;
        return try process_manager.beginExclusive(path);
    }
    var transition = try process_manager.beginExclusiveWithIo(path, io.?);
    transition.cleanup_scheduler = CleanupScheduler.fromRuntime(runtime);
    return transition;
}

pub fn beginProcessPreparationWithRuntime(path: []const u8, runtime: ?*background_runtime.BackendRuntime) !PreparationTransition {
    return try beginProcessPreparationWithRuntimeAndIo(path, runtime, null);
}

pub fn beginProcessPreparationWithRuntimeAndIo(
    path: []const u8,
    runtime: ?*background_runtime.BackendRuntime,
    io_override: ?std.Io,
) !PreparationTransition {
    const io = if (runtime) |active| active.filesystemIo() orelse io_override else io_override;
    if (io == null) {
        if (runtime != null) return error.BackendRuntimeIoUnavailable;
        return try process_manager.beginPreparation(path, null);
    }
    return try process_manager.beginPreparationWithIo(path, CleanupScheduler.fromRuntime(runtime), io.?);
}

pub fn hasPublishedGenerationRead(path: []const u8) !bool {
    return try process_manager.hasReaders(path);
}

test "generation lifecycle serializes the same root and validates capability target" {
    const alloc = std.testing.allocator;
    var manager = Manager.init(alloc);
    defer manager.deinit();

    var first = try manager.beginExclusive("/tmp/table-a");
    defer first.deinit();
    try first.validate("/tmp/table-a");
    try std.testing.expectError(error.InvalidGenerationTransition, first.validate("/tmp/table-b"));
    try std.testing.expectError(error.GenerationTransitionActive, manager.beginExclusive("/tmp/table-a"));
    try std.testing.expectError(error.GenerationTransitionActive, manager.beginExclusive("/tmp/../tmp/table-a"));

    var other = try manager.beginExclusive("/tmp/table-b");
    other.deinit();
}

test "generation preparation overlaps reads but promotion requires reader drain" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/prepared", .{tmp.sub_path});
    defer alloc.free(path);

    var manager = Manager.init(alloc);
    defer manager.deinit();
    var preparation = try manager.beginPreparation(path, null);
    defer preparation.deinit();
    var read = try manager.beginRead(path);
    try std.testing.expectError(error.GenerationTransitionActive, preparation.promote());
    read.deinit();

    var exclusive = try preparation.promote();
    defer exclusive.deinit();
    try exclusive.validate(path);
    try std.testing.expectError(error.GenerationTransitionActive, manager.beginRead(path));
}

test "generation preparation excludes cross-manager publishers" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/prepared-cross-manager", .{tmp.sub_path});
    defer alloc.free(path);

    var first = Manager.init(alloc);
    defer first.deinit();
    var second = Manager.init(alloc);
    defer second.deinit();
    var preparation = try first.beginPreparation(path, null);
    defer preparation.deinit();
    try std.testing.expectError(error.GenerationTransitionActive, second.beginExclusive(path));
}

test "generation lifecycle returns cross-manager filesystem lock contention" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/cross-manager", .{tmp.sub_path});
    defer alloc.free(path);
    var first_manager = Manager.init(alloc);
    defer first_manager.deinit();
    var second_manager = Manager.init(alloc);
    defer second_manager.deinit();

    var first = try first_manager.beginExclusive(path);
    defer first.deinit();
    try std.testing.expectError(error.GenerationTransitionActive, second_manager.beginExclusive(path));
}

test "generation lifecycle retains shared readers until the final owner closes" {
    const alloc = std.testing.allocator;
    var manager = Manager.init(alloc);
    defer manager.deinit();

    var first = try manager.beginRead("/tmp/table-readers");
    var second = try manager.beginRead("/tmp/../tmp/table-readers");
    try std.testing.expectEqual(@as(usize, 1), manager.path_states.count());
    const state = manager.path_states.get(first.path_key) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(usize, 2), state.readers);
    try std.testing.expectError(error.GenerationTransitionActive, manager.beginExclusive("/tmp/table-readers"));

    first.deinit();
    try std.testing.expectError(error.GenerationTransitionActive, manager.beginExclusive("/tmp/table-readers"));
    second.deinit();
    try std.testing.expectEqual(@as(usize, 0), manager.path_states.count());

    var transition = try manager.beginExclusive("/tmp/table-readers");
    transition.deinit();
}

test "serving reconciliation serializes with exclusive generation transition" {
    const path = "/tmp/antfly-generation-reconciliation-serialization";
    var reconciliation = (try process_manager.beginReconciliation(path)) orelse return error.TestUnexpectedResult;
    defer reconciliation.deinit();

    try std.testing.expectError(error.GenerationTransitionActive, beginProcessExclusive(path));
    reconciliation.complete();

    var transition = try beginProcessExclusive(path);
    transition.deinit();
}

test "reconciliation cache canonicalizes aliases for transition invalidation" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var manager = Manager.init(alloc);
    defer manager.deinit();

    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/live", .{tmp.sub_path});
    defer alloc.free(path);
    try fs_paths.createDirPathPortable(std.testing.io, path);
    const path_alias = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/./{s}/live", .{tmp.sub_path});
    defer alloc.free(path_alias);
    const canonical_path = try canonicalPathAlloc(alloc, path);
    defer alloc.free(canonical_path);
    var reconciliation = (try manager.beginReconciliation(path_alias)) orelse return error.TestUnexpectedResult;
    reconciliation.complete();

    try std.testing.expect(manager.reconciled_paths.contains(canonical_path));
    try std.testing.expect(!manager.reconciled_paths.contains(path_alias));

    var transition = try manager.beginExclusive(path);
    defer transition.deinit();
    try std.testing.expect(!manager.reconciled_paths.contains(canonical_path));
}

test "reconciliation cache expires when the final reader drains" {
    const alloc = std.testing.allocator;
    var manager = Manager.init(alloc);
    defer manager.deinit();

    const path = "/tmp/antfly-generation-reconciliation-reader-cache";
    var reconciliation = (try manager.beginReconciliation(path)) orelse return error.TestUnexpectedResult;
    try manager.promoteReconciliationToRead(path, reconciliation.id, true);
    reconciliation.active = false;
    try std.testing.expect(manager.reconciled_paths.contains(reconciliation.path_key));

    manager.finishRead(path, reconciliation.id);
    try std.testing.expect(!manager.reconciled_paths.contains(path));
    try std.testing.expect((try manager.beginReadIfReconciled(path)) == null);
}

test "cached read admission revalidates after filesystem lock acquisition" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/cached-read-revalidation", .{tmp.sub_path});
    defer alloc.free(path);
    try fs_paths.createDirPathPortable(std.testing.io, path);

    var manager = Manager.init(alloc);
    defer manager.deinit();
    var reconciliation = (try manager.beginReconciliation(path)) orelse return error.TestUnexpectedResult;
    try manager.promoteReconciliationToRead(path, reconciliation.id, true);
    reconciliation.active = false;

    // This is the optimistic decision made before the next reader acquires its
    // filesystem lock. The final old reader then drains during that interval.
    try std.testing.expect((try manager.beginReconciliation(path)) == null);
    manager.finishRead(path, reconciliation.id);

    const path_key = try canonicalPathAlloc(alloc, path);
    defer alloc.free(path_key);
    const publication_lock = try openPublicationLock(alloc, path_key, .shared);
    defer closePublicationLock(publication_lock);
    try std.testing.expect((try manager.beginReadIfReconciledCanonical(path, path_key)) == null);
}

test "process cached read admission retains shared publication locks" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/process-cached-read", .{tmp.sub_path});
    defer alloc.free(path);
    try fs_paths.createDirPathPortable(std.testing.io, path);

    var first = (try acquirePublishedGenerationRead(alloc, path)) orelse return error.TestUnexpectedResult;
    var first_active = true;
    defer if (first_active) first.deinit();
    var second = (try acquirePublishedGenerationRead(alloc, path)) orelse return error.TestUnexpectedResult;
    var second_active = true;
    defer if (second_active) second.deinit();

    try std.testing.expectError(error.GenerationTransitionActive, beginProcessExclusive(path));
    first.deinit();
    first_active = false;
    try std.testing.expectError(error.GenerationTransitionActive, beginProcessExclusive(path));
    second.deinit();
    second_active = false;

    var transition = try beginProcessExclusive(path);
    transition.deinit();
}

test "staged generation cannot publish after its exclusive capability is released" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const live_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/released", .{tmp.sub_path});
    defer alloc.free(live_path);
    var manager = Manager.init(alloc);
    defer manager.deinit();
    var transition = try manager.beginExclusive(live_path);
    var staged = try transition.beginStaging();
    defer staged.deinit();
    try std.testing.expect(staged.io == null);

    transition.deinit();
    try std.testing.expectError(error.InvalidGenerationTransition, staged.validatePath(staged.path()));
    try std.testing.expectError(error.InvalidGenerationTransition, staged.validateLivePath(live_path));
    try std.testing.expectError(error.InvalidGenerationTransition, staged.publish());
}

test "staged generation preserves live root until publication" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const live_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/live", .{tmp.sub_path});
    defer alloc.free(live_path);
    const live_value_path = try std.fmt.allocPrint(alloc, "{s}/value", .{live_path});
    defer alloc.free(live_value_path);

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    try fs_paths.createDirPathPortable(io, live_path);
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = live_value_path, .data = "old" });

    var manager = Manager.init(alloc);
    defer manager.deinit();
    var transition = try manager.beginExclusive(live_path);
    defer transition.deinit();
    var staged = try transition.beginStaging();
    defer staged.deinit();

    const staged_value_path = try std.fmt.allocPrint(alloc, "{s}/value", .{staged.path()});
    defer alloc.free(staged_value_path);
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = staged_value_path, .data = "new" });

    const before = try std.Io.Dir.cwd().readFileAlloc(io, live_value_path, alloc, .limited(16));
    defer alloc.free(before);
    try std.testing.expectEqualStrings("old", before);

    try std.testing.expectEqual(PublicationOutcome.durable, try staged.publish());
    const after = try std.Io.Dir.cwd().readFileAlloc(io, live_value_path, alloc, .limited(16));
    defer alloc.free(after);
    try std.testing.expectEqualStrings("new", after);
}

test "staged generation seals while readers remain admitted before atomic publication" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const live_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/prepared-live", .{tmp.sub_path});
    defer alloc.free(live_path);
    const live_value_path = try std.fmt.allocPrint(alloc, "{s}/value", .{live_path});
    defer alloc.free(live_value_path);

    try fs_paths.createDirPathPortable(std.testing.io, live_path);
    try std.Io.Dir.cwd().writeFile(std.testing.io, .{ .sub_path = live_value_path, .data = "old" });

    var manager = Manager.init(alloc);
    defer manager.deinit();
    var preparation = try manager.beginPreparation(live_path, null);
    defer preparation.deinit();
    var reader = try manager.beginRead(live_path);
    var reader_active = true;
    defer if (reader_active) reader.deinit();
    var staged = try preparation.beginStaging();
    defer staged.deinit();

    const staged_value_path = try std.fmt.allocPrint(alloc, "{s}/value", .{staged.path()});
    defer alloc.free(staged_value_path);
    try std.Io.Dir.cwd().writeFile(std.testing.io, .{ .sub_path = staged_value_path, .data = "new" });

    try staged.seal();
    try std.testing.expect(staged.sealed);
    try std.testing.expectError(error.GenerationTransitionActive, preparation.promote());
    const before = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, live_value_path, alloc, .limited(16));
    defer alloc.free(before);
    try std.testing.expectEqualStrings("old", before);

    reader.deinit();
    reader_active = false;
    var transition = try preparation.promote();
    defer transition.deinit();
    try std.testing.expectEqual(PublicationOutcome.durable, try staged.publish());
    const after = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, live_value_path, alloc, .limited(16));
    defer alloc.free(after);
    try std.testing.expectEqualStrings("new", after);
}

test "prepared generation publication rolls back before serving admission" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    const alloc = failing.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const live_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/rollback-live", .{tmp.sub_path});
    defer alloc.free(live_path);
    const live_value_path = try std.fmt.allocPrint(alloc, "{s}/value", .{live_path});
    defer alloc.free(live_value_path);
    try fs_paths.createDirPathPortable(std.testing.io, live_path);
    try std.Io.Dir.cwd().writeFile(std.testing.io, .{ .sub_path = live_value_path, .data = "old" });

    var transition = try beginProcessExclusive(live_path);
    defer transition.deinit();
    var staged = try transition.beginStaging();
    defer staged.deinit();
    const staged_value_path = try std.fmt.allocPrint(alloc, "{s}/value", .{staged.path()});
    defer alloc.free(staged_value_path);
    try std.Io.Dir.cwd().writeFile(std.testing.io, .{ .sub_path = staged_value_path, .data = "candidate" });

    try std.testing.expectEqual(PublicationOutcome.durable, try staged.publishPrepared());
    const candidate = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, live_value_path, alloc, .limited(16));
    defer alloc.free(candidate);
    try std.testing.expectEqualStrings("candidate", candidate);

    failing.fail_index = failing.alloc_index;
    failing.resize_fail_index = failing.resize_index;
    try staged.rollbackPublication();
    failing.fail_index = std.math.maxInt(usize);
    failing.resize_fail_index = std.math.maxInt(usize);
    const restored = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, live_value_path, alloc, .limited(16));
    defer alloc.free(restored);
    try std.testing.expectEqualStrings("old", restored);
}

test "prepared generation reconciliation rolls back an exchanged candidate" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const live_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/prepared-crash-live", .{tmp.sub_path});
    defer alloc.free(live_path);
    const live_value_path = try std.fmt.allocPrint(alloc, "{s}/value", .{live_path});
    defer alloc.free(live_value_path);
    try fs_paths.createDirPathPortable(std.testing.io, live_path);
    try std.Io.Dir.cwd().writeFile(std.testing.io, .{ .sub_path = live_value_path, .data = "previous" });

    var manager = Manager.init(alloc);
    defer manager.deinit();
    var transition = try manager.beginExclusive(live_path);
    defer transition.deinit();
    var staged = try transition.beginStaging();
    defer staged.deinit();
    const staged_value_path = try std.fmt.allocPrint(alloc, "{s}/value", .{staged.path()});
    defer alloc.free(staged_value_path);
    try std.Io.Dir.cwd().writeFile(std.testing.io, .{ .sub_path = staged_value_path, .data = "candidate" });

    try std.testing.expectEqual(PublicationOutcome.durable, try staged.publishPrepared());
    try std.testing.expect(try reconcilePublishedGenerationExclusive(alloc, std.testing.io, live_path, null));
    staged.published = false;
    staged.preserve_retired = false;
    staged.had_live_generation = false;
    staged.publication_outcome = null;

    const restored = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, live_value_path, alloc, .limited(32));
    defer alloc.free(restored);
    try std.testing.expectEqualStrings("previous", restored);
    try std.testing.expect(!pathExists(std.testing.io, staged.path()));
}

test "prepared first generation reconciliation removes an unvalidated candidate" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const live_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/prepared-first-live", .{tmp.sub_path});
    defer alloc.free(live_path);

    var manager = Manager.init(alloc);
    defer manager.deinit();
    var transition = try manager.beginExclusive(live_path);
    defer transition.deinit();
    var staged = try transition.beginStaging();
    defer staged.deinit();
    const staged_value_path = try std.fmt.allocPrint(alloc, "{s}/value", .{staged.path()});
    defer alloc.free(staged_value_path);
    try std.Io.Dir.cwd().writeFile(std.testing.io, .{ .sub_path = staged_value_path, .data = "candidate" });

    try std.testing.expectEqual(PublicationOutcome.durable, try staged.publishPrepared());
    try std.testing.expect(try reconcilePublishedGenerationExclusive(alloc, std.testing.io, live_path, null));
    staged.published = false;
    staged.preserve_retired = false;
    staged.publication_outcome = null;

    try std.testing.expect(!pathExists(std.testing.io, live_path));
    try std.testing.expect(!pathExists(std.testing.io, staged.path()));
}

test "committed generation reconciliation preserves the validated candidate" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const live_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/committed-crash-live", .{tmp.sub_path});
    defer alloc.free(live_path);
    const live_value_path = try std.fmt.allocPrint(alloc, "{s}/value", .{live_path});
    defer alloc.free(live_value_path);
    try fs_paths.createDirPathPortable(std.testing.io, live_path);
    try std.Io.Dir.cwd().writeFile(std.testing.io, .{ .sub_path = live_value_path, .data = "previous" });

    var manager = Manager.init(alloc);
    defer manager.deinit();
    var transition = try manager.beginExclusive(live_path);
    defer transition.deinit();
    var staged = try transition.beginStaging();
    defer staged.deinit();
    const staged_value_path = try std.fmt.allocPrint(alloc, "{s}/value", .{staged.path()});
    defer alloc.free(staged_value_path);
    try std.Io.Dir.cwd().writeFile(std.testing.io, .{ .sub_path = staged_value_path, .data = "candidate" });

    try std.testing.expectEqual(PublicationOutcome.durable, try staged.publishPrepared());
    try writePublicationMarker(alloc, std.testing.io, live_path, .{
        .phase = .committed,
        .retained_name = std.fs.path.basename(staged.path()),
        .had_live_generation = true,
    });
    staged.publication_committed = true;
    try std.testing.expect(try reconcilePublishedGenerationExclusive(alloc, std.testing.io, live_path, null));

    const published = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, live_value_path, alloc, .limited(32));
    defer alloc.free(published);
    try std.testing.expectEqualStrings("candidate", published);
    try std.testing.expect(!pathExists(std.testing.io, staged.path()));
}

test "published generation reports post-commit sync failure without returning an error" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const live_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/uncertain", .{tmp.sub_path});
    defer alloc.free(live_path);
    try fs_paths.createDirPathPortable(std.testing.io, live_path);
    const old_value_path = try std.fmt.allocPrint(alloc, "{s}/value", .{live_path});
    defer alloc.free(old_value_path);
    try std.Io.Dir.cwd().writeFile(std.testing.io, .{ .sub_path = old_value_path, .data = "previous" });

    var manager = Manager.init(alloc);
    defer manager.deinit();
    var transition = try manager.beginExclusive(live_path);
    defer transition.deinit();
    var staged = try transition.beginStaging();
    defer staged.deinit();

    const value_path = try std.fmt.allocPrint(alloc, "{s}/value", .{staged.path()});
    defer alloc.free(value_path);
    try std.Io.Dir.cwd().writeFile(std.testing.io, .{ .sub_path = value_path, .data = "committed" });

    test_fail_post_publish_sync = true;
    defer test_fail_post_publish_sync = false;
    try std.testing.expectEqual(PublicationOutcome.durability_uncertain, try staged.publish());

    const live_value_path = try std.fmt.allocPrint(alloc, "{s}/value", .{live_path});
    defer alloc.free(live_value_path);
    const value = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, live_value_path, alloc, .limited(32));
    defer alloc.free(value);
    try std.testing.expectEqualStrings("committed", value);

    const retired_value = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, value_path, alloc, .limited(32));
    defer alloc.free(retired_value);
    try std.testing.expectEqualStrings("previous", retired_value);

    transition.deinit();
    var cleanup_runtime = try background_runtime.BackendRuntimeHandle.init(alloc, .{ .backend = .io_threaded });
    var cleanup_runtime_active = true;
    defer if (cleanup_runtime_active) cleanup_runtime.deinit();
    var read_lease = (try acquirePublishedGenerationReadWithRuntime(alloc, live_path, cleanup_runtime.ptr())) orelse return error.TestUnexpectedResult;
    try std.testing.expectError(error.GenerationTransitionActive, beginProcessExclusive(live_path));
    const marker_path = try publicationMarkerPathAlloc(alloc, live_path);
    defer alloc.free(marker_path);
    try std.testing.expect(!pathExists(std.testing.io, marker_path));
    read_lease.deinit();
    cleanup_runtime.deinit();
    cleanup_runtime_active = false;
    try std.testing.expect(!pathExists(std.testing.io, value_path));

    var next_transition = try beginProcessExclusive(live_path);
    next_transition.deinit();
}

test "manual generation runtime uses an explicit filesystem io authority" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const live_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/manual-runtime-io", .{tmp.sub_path});
    defer alloc.free(live_path);

    var runtime = try background_runtime.BackendRuntimeHandle.init(alloc, .{ .backend = .manual });
    defer runtime.deinit();
    try std.testing.expectError(
        error.BackendRuntimeIoUnavailable,
        beginProcessExclusiveWithRuntime(live_path, runtime.ptr()),
    );
    var transition = try beginProcessExclusiveWithRuntimeAndIo(live_path, runtime.ptr(), std.testing.io);
    defer transition.deinit();
    try std.testing.expect(transition.io != null);
}

test "durable publication retires the previous generation through the cleanup runtime" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const live_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/async-retire", .{tmp.sub_path});
    defer alloc.free(live_path);
    try fs_paths.createDirPathPortable(std.testing.io, live_path);
    const live_value_path = try std.fmt.allocPrint(alloc, "{s}/value", .{live_path});
    defer alloc.free(live_value_path);
    try std.Io.Dir.cwd().writeFile(std.testing.io, .{ .sub_path = live_value_path, .data = "previous" });

    var runtime = try background_runtime.BackendRuntimeHandle.init(alloc, .{ .backend = .io_threaded });
    defer runtime.deinit();
    var transition = try beginProcessExclusiveWithRuntime(live_path, runtime.ptr());
    defer transition.deinit();
    try std.testing.expect(transition.io != null);
    var staged = try transition.beginStaging();
    defer staged.deinit();
    try std.testing.expect(staged.io != null);
    const staged_value_path = try std.fmt.allocPrint(alloc, "{s}/value", .{staged.path()});
    defer alloc.free(staged_value_path);
    try std.Io.Dir.cwd().writeFile(std.testing.io, .{ .sub_path = staged_value_path, .data = "current" });

    test_retired_cleanup_started.store(false, .release);
    test_block_retired_cleanup.store(true, .release);
    defer test_block_retired_cleanup.store(false, .release);
    try std.testing.expectEqual(PublicationOutcome.durable, try staged.publish());

    var attempts: usize = 0;
    while (!test_retired_cleanup_started.load(.acquire) and attempts < 10_000) : (attempts += 1) platform.time.yieldBriefly();
    try std.testing.expect(test_retired_cleanup_started.load(.acquire));
    try std.testing.expect(pathExists(std.testing.io, staged.path()));
    const marker_path = try publicationMarkerPathAlloc(alloc, live_path);
    defer alloc.free(marker_path);
    const intent_path = try cleanupIntentPathAlloc(alloc, live_path, std.fs.path.basename(staged.path()));
    defer alloc.free(intent_path);
    try std.testing.expect(!pathExists(std.testing.io, marker_path));
    try std.testing.expect(pathExists(std.testing.io, intent_path));
    const current = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, live_value_path, alloc, .limited(16));
    defer alloc.free(current);
    try std.testing.expectEqualStrings("current", current);

    test_block_retired_cleanup.store(false, .release);
    const scheduler = staged.cleanup_scheduler orelse return error.TestUnexpectedResult;
    scheduler.lane.drainOwner(scheduler.owner_id);
    try std.testing.expect(!pathExists(std.testing.io, staged.path()));
    try std.testing.expect(!pathExists(std.testing.io, marker_path));
    try std.testing.expect(!pathExists(std.testing.io, intent_path));
}

test "durable cleanup debt is rescheduled until the retired generation is reclaimed" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const live_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/retry-retire", .{tmp.sub_path});
    defer alloc.free(live_path);
    try fs_paths.createDirPathPortable(std.testing.io, live_path);
    const live_value_path = try std.fmt.allocPrint(alloc, "{s}/value", .{live_path});
    defer alloc.free(live_value_path);
    try std.Io.Dir.cwd().writeFile(std.testing.io, .{ .sub_path = live_value_path, .data = "previous" });

    var runtime = try background_runtime.BackendRuntimeHandle.init(alloc, .{ .backend = .io_threaded });
    defer runtime.deinit();
    var transition = try beginProcessExclusiveWithRuntime(live_path, runtime.ptr());
    defer transition.deinit();
    var staged = try transition.beginStaging();
    defer staged.deinit();
    const staged_value_path = try std.fmt.allocPrint(alloc, "{s}/value", .{staged.path()});
    defer alloc.free(staged_value_path);
    try std.Io.Dir.cwd().writeFile(std.testing.io, .{ .sub_path = staged_value_path, .data = "current" });

    test_retired_cleanup_failures_remaining.store(3, .release);
    defer test_retired_cleanup_failures_remaining.store(0, .release);
    try std.testing.expectEqual(PublicationOutcome.durable, try staged.publish());
    const retired_path = try alloc.dupe(u8, staged.path());
    defer alloc.free(retired_path);
    const intent_path = try cleanupIntentPathAlloc(alloc, live_path, std.fs.path.basename(retired_path));
    defer alloc.free(intent_path);

    const scheduler = staged.cleanup_scheduler orelse return error.TestUnexpectedResult;
    scheduler.lane.drainOwner(scheduler.owner_id);
    try std.testing.expectEqual(@as(usize, 0), test_retired_cleanup_failures_remaining.load(.acquire));
    try std.testing.expect(!pathExists(std.testing.io, retired_path));
    try std.testing.expect(!pathExists(std.testing.io, intent_path));
    const current = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, live_value_path, alloc, .limited(16));
    defer alloc.free(current);
    try std.testing.expectEqualStrings("current", current);
}

test "older cleanup cannot acknowledge a newer prepared publication" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const live_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/cleanup-publication-race", .{tmp.sub_path});
    defer alloc.free(live_path);
    try fs_paths.createDirPathPortable(std.testing.io, live_path);
    const live_value_path = try std.fmt.allocPrint(alloc, "{s}/value", .{live_path});
    defer alloc.free(live_value_path);
    try std.Io.Dir.cwd().writeFile(std.testing.io, .{ .sub_path = live_value_path, .data = "zero" });

    var runtime = try background_runtime.BackendRuntimeHandle.init(alloc, .{ .backend = .io_threaded });
    defer runtime.deinit();
    test_retired_cleanup_started.store(false, .release);
    test_block_retired_cleanup.store(true, .release);
    defer test_block_retired_cleanup.store(false, .release);

    var first_transition = try beginProcessExclusiveWithRuntime(live_path, runtime.ptr());
    var first = try first_transition.beginStaging();
    defer first.deinit();
    const first_value_path = try std.fmt.allocPrint(alloc, "{s}/value", .{first.path()});
    defer alloc.free(first_value_path);
    try std.Io.Dir.cwd().writeFile(std.testing.io, .{ .sub_path = first_value_path, .data = "one" });
    try std.testing.expectEqual(PublicationOutcome.durable, try first.publish());
    var attempts: usize = 0;
    while (!test_retired_cleanup_started.load(.acquire) and attempts < 10_000) : (attempts += 1) platform.time.yieldBriefly();
    try std.testing.expect(test_retired_cleanup_started.load(.acquire));
    first_transition.deinit();

    var second_transition = try beginProcessExclusiveWithRuntime(live_path, runtime.ptr());
    defer second_transition.deinit();
    var second = try second_transition.beginStaging();
    defer second.deinit();
    const second_value_path = try std.fmt.allocPrint(alloc, "{s}/value", .{second.path()});
    defer alloc.free(second_value_path);
    try std.Io.Dir.cwd().writeFile(std.testing.io, .{ .sub_path = second_value_path, .data = "two" });
    try std.testing.expectEqual(PublicationOutcome.durable, try second.publishPrepared());

    test_block_retired_cleanup.store(false, .release);
    const scheduler = first.cleanup_scheduler orelse return error.TestUnexpectedResult;
    scheduler.lane.drainOwner(scheduler.owner_id);

    var marker = (try readPublicationMarker(alloc, std.testing.io, live_path)) orelse return error.TestUnexpectedResult;
    defer marker.deinit(alloc);
    try std.testing.expectEqual(PublicationPhase.prepared, marker.phase);
    const published = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, live_value_path, alloc, .limited(16));
    defer alloc.free(published);
    try std.testing.expectEqualStrings("two", published);

    try second.rollbackPublication();
}

test "cleanup submission failure preserves a durable retry intent" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const live_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/submission-debt", .{tmp.sub_path});
    defer alloc.free(live_path);
    try fs_paths.createDirPathPortable(std.testing.io, live_path);
    const stale_path = try std.fmt.allocPrint(alloc, "{s}.restore-stage-a-b", .{live_path});
    defer alloc.free(stale_path);
    try fs_paths.createDirPathPortable(std.testing.io, stale_path);

    const RejectingLane = struct {
        fn submit(_: *anyopaque, _: background_runtime.Job) !void {
            return error.ResourceBudgetExceeded;
        }
        fn drain(_: *anyopaque, _: u64) void {}
        fn close(_: *anyopaque, _: u64) void {}
        fn poll(_: *anyopaque, _: usize) !usize {
            return 0;
        }
        const vtable = background_runtime.DurableJobLane.VTable{
            .submit = submit,
            .drain_owner = drain,
            .close_owner = close,
            .poll = poll,
        };
    };
    var lane_state: u8 = 0;
    const scheduler = CleanupScheduler{
        .alloc = alloc,
        .io = std.testing.io,
        .lane = .{ .ptr = &lane_state, .vtable = &RejectingLane.vtable },
        .owner_id = 1,
    };
    var deferred = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (deferred.items) |path| alloc.free(path);
        deferred.deinit(alloc);
    }
    try std.testing.expect(!try reconcilePublishedGeneration(alloc, std.testing.io, live_path, scheduler, &deferred));
    const intent_path = try cleanupIntentPathAlloc(alloc, live_path, std.fs.path.basename(stale_path));
    defer alloc.free(intent_path);
    try std.testing.expect(pathExists(std.testing.io, stale_path));
    try std.testing.expect(pathExists(std.testing.io, intent_path));

    try std.testing.expect(try reconcilePublishedGenerationExclusive(alloc, std.testing.io, live_path, null));
    try std.testing.expect(!pathExists(std.testing.io, stale_path));
    try std.testing.expect(!pathExists(std.testing.io, intent_path));
}

test "generation publication reclaims retired roots synchronously without a cleanup runtime" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const live_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/sync-retire", .{tmp.sub_path});
    defer alloc.free(live_path);
    try fs_paths.createDirPathPortable(std.testing.io, live_path);
    const old_value_path = try std.fmt.allocPrint(alloc, "{s}/value", .{live_path});
    defer alloc.free(old_value_path);
    try std.Io.Dir.cwd().writeFile(std.testing.io, .{ .sub_path = old_value_path, .data = "previous" });

    var transition = try beginProcessExclusive(live_path);
    var staged = try transition.beginStaging();
    defer staged.deinit();
    const new_value_path = try std.fmt.allocPrint(alloc, "{s}/value", .{staged.path()});
    defer alloc.free(new_value_path);
    try std.Io.Dir.cwd().writeFile(std.testing.io, .{ .sub_path = new_value_path, .data = "current" });
    try std.testing.expectEqual(PublicationOutcome.durable, try staged.publish());
    try std.testing.expect(!pathExists(std.testing.io, staged.path()));
    transition.deinit();

    var lease = (try acquirePublishedGenerationRead(alloc, live_path)) orelse return error.TestUnexpectedResult;
    lease.deinit();
    try std.testing.expect(!pathExists(std.testing.io, staged.path()));
}

test "retired generation cleanup is idempotent across concurrent workers" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const parent = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer alloc.free(parent);
    const retired = try std.fmt.allocPrint(alloc, "{s}/table.restore-stage-a-b", .{parent});
    defer alloc.free(retired);
    try fs_paths.createDirPathPortable(std.testing.io, retired);
    const value_path = try std.fs.path.join(alloc, &.{ retired, "value" });
    defer alloc.free(value_path);
    try std.Io.Dir.cwd().writeFile(std.testing.io, .{ .sub_path = value_path, .data = "retired" });

    const Worker = struct {
        path: []const u8,
        parent: []const u8,
        err: ?anyerror = null,

        fn run(self: *@This()) void {
            var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
            defer io_impl.deinit();
            deleteRetiredGenerationPaths(std.heap.page_allocator, io_impl.io(), &.{self.path}, self.parent) catch |err| {
                self.err = err;
            };
        }
    };
    var first = Worker{ .path = retired, .parent = parent };
    var second = Worker{ .path = retired, .parent = parent };
    const first_thread = try std.Thread.spawn(.{}, Worker.run, .{&first});
    const second_thread = try std.Thread.spawn(.{}, Worker.run, .{&second});
    first_thread.join();
    second_thread.join();

    try std.testing.expect(first.err == null);
    try std.testing.expect(second.err == null);
    try std.testing.expect(!pathExists(std.testing.io, retired));
}

test "atomic exchange failure leaves live and staged generations unchanged" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const live_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/exchange-unavailable", .{tmp.sub_path});
    defer alloc.free(live_path);
    try fs_paths.createDirPathPortable(std.testing.io, live_path);
    const live_value_path = try std.fmt.allocPrint(alloc, "{s}/value", .{live_path});
    defer alloc.free(live_value_path);
    try std.Io.Dir.cwd().writeFile(std.testing.io, .{ .sub_path = live_value_path, .data = "live" });

    var manager = Manager.init(alloc);
    defer manager.deinit();
    var transition = try manager.beginExclusive(live_path);
    defer transition.deinit();
    var staged = try transition.beginStaging();
    defer staged.deinit();
    const staged_value_path = try std.fmt.allocPrint(alloc, "{s}/value", .{staged.path()});
    defer alloc.free(staged_value_path);
    try std.Io.Dir.cwd().writeFile(std.testing.io, .{ .sub_path = staged_value_path, .data = "staged" });

    test_disable_atomic_exchange = true;
    defer test_disable_atomic_exchange = false;
    try std.testing.expectError(error.AtomicGenerationExchangeUnavailable, staged.publish());

    const live_value = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, live_value_path, alloc, .limited(16));
    defer alloc.free(live_value);
    try std.testing.expectEqualStrings("live", live_value);
    const staged_value = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, staged_value_path, alloc, .limited(16));
    defer alloc.free(staged_value);
    try std.testing.expectEqualStrings("staged", staged_value);
}
