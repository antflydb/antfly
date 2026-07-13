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
const publication_marker_name = ".antfly-generation-publication-v1";
const publication_marker_data = "antfly_generation_publication_v1\n";
const publication_lock_suffix = ".antfly-generation.lock";
const preparation_lock_suffix = ".antfly-generation-prepare.lock";
const max_reconciled_paths = 8192;

fn publicationLockPathAlloc(alloc: Allocator, canonical_path: []const u8) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{s}{s}", .{ canonical_path, publication_lock_suffix });
}

fn preparationLockPathAlloc(alloc: Allocator, canonical_path: []const u8) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{s}{s}", .{ canonical_path, preparation_lock_suffix });
}

fn openPathLock(lock_path: []const u8, lock: std.Io.File.Lock) !std.Io.File {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    if (std.fs.path.dirname(lock_path)) |parent| try fs_paths.createDirPathPortable(io_impl.io(), parent);
    return std.Io.Dir.cwd().createFile(io_impl.io(), lock_path, .{
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

fn openPublicationLock(alloc: Allocator, canonical_path: []const u8, lock: std.Io.File.Lock) !std.Io.File {
    const lock_path = try publicationLockPathAlloc(alloc, canonical_path);
    defer alloc.free(lock_path);
    return try openPathLock(lock_path, lock);
}

fn openPreparationLock(alloc: Allocator, canonical_path: []const u8) !std.Io.File {
    const lock_path = try preparationLockPathAlloc(alloc, canonical_path);
    defer alloc.free(lock_path);
    return try openPathLock(lock_path, .exclusive);
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

fn canonicalPathAlloc(alloc: Allocator, path: []const u8) ![]u8 {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
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

pub const PublicationOutcome = enum {
    durable,
    durability_uncertain,
};

const CleanupScheduler = struct {
    alloc: Allocator,
    lane: background_runtime.DurableJobLane,
    owner_id: u64,

    fn fromRuntime(runtime: ?*background_runtime.BackendRuntime) ?CleanupScheduler {
        const active = runtime orelse return null;
        // The manual runtime executes jobs inline. Preserve cleanup as durable
        // reconciliation debt instead of extending publication downtime.
        if (active.threaded_jobs == null) return null;
        return .{
            .alloc = active.alloc,
            .lane = active.durable_jobs,
            .owner_id = active.allocOwnerId(),
        };
    }
};

var test_fail_post_publish_sync = false;
var test_fail_reconciliation_sync = false;
var test_disable_atomic_exchange = false;
var test_block_retired_cleanup = std.atomic.Value(bool).init(false);
var test_retired_cleanup_started = std.atomic.Value(bool).init(false);

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
        const path_key = try canonicalPathAlloc(self.allocator, path);
        errdefer self.allocator.free(path_key);
        const owned_path = try self.allocator.dupe(u8, path);
        errdefer self.allocator.free(owned_path);

        // Filesystem lock acquisition can block or fail. Keep it outside the
        // manager mutex so rollback never has to re-enter the mutex through an
        // ExclusiveTransition destructor.
        const preparation_lock = try openPreparationLock(self.allocator, path_key);
        errdefer closePublicationLock(preparation_lock);
        const publication_lock = try openPublicationLock(self.allocator, path_key, .exclusive);
        errdefer closePublicationLock(publication_lock);

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
        };
        return transition;
    }

    pub fn beginPreparation(self: *Manager, path: []const u8, cleanup_scheduler: ?CleanupScheduler) !PreparationTransition {
        const path_key = try canonicalPathAlloc(self.allocator, path);
        errdefer self.allocator.free(path_key);
        const owned_path = try self.allocator.dupe(u8, path);
        errdefer self.allocator.free(owned_path);

        const preparation_lock = try openPreparationLock(self.allocator, path_key);
        errdefer closePublicationLock(preparation_lock);
        const publication_lock = try openPublicationLock(self.allocator, path_key, .shared);
        errdefer closePublicationLock(publication_lock);

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
        };
    }

    fn beginReconciliation(self: *Manager, path: []const u8) !?ReconciliationLease {
        const path_key = try canonicalPathAlloc(self.allocator, path);
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

    fn promoteToRead(self: *@This(), publication_lock: std.Io.File, mark_reconciled: bool) !ReadLease {
        if (!self.active) return error.InvalidGenerationTransition;
        try self.manager.promoteReconciliationToRead(self.path, self.id, mark_reconciled);
        self.active = false;
        return .{
            .manager = self.manager,
            .path = self.path,
            .path_key = self.path_key,
            .id = self.id,
            .publication_lock = publication_lock,
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
    active: bool = true,

    pub fn deinit(self: *@This()) void {
        if (!self.active) return;
        const publication_lock = self.publication_lock;
        self.publication_lock = null;
        // Retire local reader/cache state before releasing the interprocess
        // lock so no local opener can trust stale reconciliation state across
        // an external publication window.
        self.manager.finishRead(self.path, self.id);
        if (publication_lock) |file| closePublicationLock(file);
        self.active = false;
    }
};

pub const PreparationTransition = struct {
    manager: *Manager,
    alloc: Allocator,
    path: []const u8,
    id: u64,
    cleanup_scheduler: ?CleanupScheduler = null,
    publication_lock: ?std.Io.File,
    preparation_lock: ?std.Io.File,
    active: bool = true,

    pub fn beginStaging(self: *PreparationTransition) !StagedGeneration {
        if (!self.active) return error.InvalidGenerationTransition;
        try self.manager.validateStaging(self.id, self.path);
        return try beginStagingGeneration(self.alloc, self.manager, self.path, self.id, self.cleanup_scheduler, false);
    }

    pub fn promote(self: *PreparationTransition) !ExclusiveTransition {
        if (!self.active) return error.InvalidGenerationTransition;
        const path_key = try canonicalPathAlloc(self.alloc, self.path);
        defer self.alloc.free(path_key);
        if (self.publication_lock) |file| closePublicationLock(file);
        self.publication_lock = null;
        const publication_lock = openPublicationLock(self.alloc, path_key, .exclusive) catch |promotion_err| {
            self.publication_lock = openPublicationLock(self.alloc, path_key, .shared) catch |reacquire_err| {
                std.log.err("generation preparation failed to restore publication read lock path={s} promotion_err={s} reacquire_err={s}", .{
                    self.path,
                    @errorName(promotion_err),
                    @errorName(reacquire_err),
                });
                return reacquire_err;
            };
            return promotion_err;
        };
        self.manager.promotePreparation(self.path, self.id) catch |promotion_err| {
            closePublicationLock(publication_lock);
            self.publication_lock = openPublicationLock(self.alloc, path_key, .shared) catch |reacquire_err| {
                std.log.err("generation preparation failed to restore publication read lock path={s} promotion_err={s} reacquire_err={s}", .{
                    self.path,
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
        };
    }

    pub fn deinit(self: *PreparationTransition) void {
        if (!self.active) return;
        if (self.publication_lock) |file| closePublicationLock(file);
        self.publication_lock = null;
        if (self.preparation_lock) |file| closePublicationLock(file);
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
    active: bool = true,

    pub fn validate(self: *const ExclusiveTransition, path: []const u8) !void {
        if (!self.active or !std.mem.eql(u8, self.path, path)) return error.InvalidGenerationTransition;
    }

    pub fn deinit(self: *ExclusiveTransition) void {
        if (!self.active) return;
        if (self.publication_lock) |file| closePublicationLock(file);
        self.publication_lock = null;
        if (self.preparation_lock) |file| closePublicationLock(file);
        self.preparation_lock = null;
        self.manager.finishExclusive(self.path, self.id);
        self.active = false;
    }

    pub fn reconcilePublished(self: *ExclusiveTransition) !void {
        try self.validate(self.path);
        var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
        defer io_impl.deinit();
        _ = try reconcilePublishedGenerationExclusive(self.alloc, io_impl.io(), self.path, self.cleanup_scheduler);
    }

    pub fn beginStaging(self: *ExclusiveTransition) !StagedGeneration {
        try self.validate(self.path);
        return try beginStagingGeneration(self.alloc, self.manager, self.path, self.id, self.cleanup_scheduler, true);
    }
};

fn beginStagingGeneration(
    alloc: Allocator,
    manager: *Manager,
    path: []const u8,
    transition_id: u64,
    cleanup_scheduler: ?CleanupScheduler,
    reconcile: bool,
) !StagedGeneration {
    const live_path = try alloc.dupe(u8, path);
    errdefer alloc.free(live_path);
    const nonce = platform.time.monotonicNs();
    const staging_path = try std.fmt.allocPrint(alloc, "{s}.restore-stage-{x}-{x}", .{ path, transition_id, nonce });
    errdefer alloc.free(staging_path);

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    if (reconcile) _ = try reconcilePublishedGenerationExclusive(alloc, io, path, cleanup_scheduler);
    if (pathExists(io, staging_path)) return error.GenerationStagingCollision;
    try fs_paths.createDirPathPortable(io, staging_path);
    errdefer std.Io.Dir.cwd().deleteTree(io, staging_path) catch {};
    try writePublicationMarker(alloc, io, staging_path);
    return .{
        .alloc = alloc,
        .manager = manager,
        .transition_id = transition_id,
        .live_path = live_path,
        .staging_path = staging_path,
        .cleanup_scheduler = cleanup_scheduler,
    };
}

pub const StagedGeneration = struct {
    alloc: Allocator,
    manager: *Manager,
    transition_id: u64,
    live_path: []u8,
    staging_path: []u8,
    published: bool = false,
    sealed: bool = false,
    preserve_retired: bool = false,
    cleanup_scheduler: ?CleanupScheduler = null,
    closed: bool = false,

    pub fn path(self: *const StagedGeneration) []const u8 {
        return self.staging_path;
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
        if (self.closed or self.published) return error.InvalidGenerationTransition;
        try self.manager.validateStaging(self.transition_id, self.live_path);
        var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
        defer io_impl.deinit();
        const io = io_impl.io();
        if (!pathExists(io, self.staging_path)) return error.GenerationStagingMissing;
        try syncTreePortable(self.alloc, io, self.staging_path);
        const parent = std.fs.path.dirname(self.staging_path) orelse if (std.fs.path.isAbsolute(self.staging_path)) "/" else ".";
        try fs_paths.syncDirPortable(io, parent);
        self.sealed = true;
    }

    /// Errors are returned only before the live namespace changes. Once the
    /// generation is visible, durability failures are represented in the
    /// outcome so callers must finish their committed transition.
    pub fn publish(self: *StagedGeneration) !PublicationOutcome {
        if (self.closed or self.published) return error.InvalidGenerationTransition;
        try self.manager.validateExclusive(self.transition_id, self.live_path);

        if (!self.sealed) try self.seal();

        var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
        defer io_impl.deinit();
        const io = io_impl.io();
        if (!pathExists(io, self.staging_path)) return error.GenerationStagingMissing;
        const parent = std.fs.path.dirname(self.live_path) orelse if (std.fs.path.isAbsolute(self.live_path)) "/" else ".";
        try fs_paths.syncDirPortable(io, parent);

        const had_live_generation = pathExists(io, self.live_path);
        if (had_live_generation) {
            if (!try exchangeDirectoriesAtomic(self.alloc, self.live_path, self.staging_path)) {
                return error.AtomicGenerationExchangeUnavailable;
            }
            // The namespace mutation has completed. Record that before any operation
            // that can fail so deinit never mistakes the retired root for staging.
            self.published = true;
            self.preserve_retired = true;
            const outcome = syncPublishedParent(io, parent);
            if (outcome == .durable) {
                _ = clearPublicationMarker(self.alloc, io, self.live_path);
                scheduleRetiredGenerationCleanup(self.cleanup_scheduler, self.staging_path, parent) catch |err| {
                    std.log.warn("retired generation cleanup scheduling deferred path={s} err={s}", .{ self.staging_path, @errorName(err) });
                };
            } else {
                // The exchange is visible but not known durable. Keep the old root
                // available for operator recovery instead of destroying both sides
                // of the last durable namespace state.
                self.preserve_retired = true;
                std.log.err("retaining previous generation after uncertain publication path={s}", .{self.staging_path});
            }
            return outcome;
        }

        try std.Io.Dir.rename(std.Io.Dir.cwd(), self.staging_path, std.Io.Dir.cwd(), self.live_path, io);

        self.published = true;
        const outcome = syncPublishedParent(io, parent);
        if (outcome == .durable) _ = clearPublicationMarker(self.alloc, io, self.live_path);
        return outcome;
    }

    pub fn deinit(self: *StagedGeneration) void {
        if (self.closed) return;
        var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
        defer io_impl.deinit();
        if (self.published) {
            if (!self.preserve_retired) std.Io.Dir.cwd().deleteTree(io_impl.io(), self.staging_path) catch {};
        } else {
            std.Io.Dir.cwd().deleteTree(io_impl.io(), self.staging_path) catch {};
        }
        self.alloc.free(self.staging_path);
        self.alloc.free(self.live_path);
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

fn writePublicationMarker(alloc: Allocator, io: std.Io, root: []const u8) !void {
    const marker_path = try publicationMarkerPathAlloc(alloc, root);
    defer alloc.free(marker_path);
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = marker_path, .data = publication_marker_data });
}

fn hasPublicationMarker(alloc: Allocator, io: std.Io, root: []const u8) !bool {
    const marker_path = try publicationMarkerPathAlloc(alloc, root);
    defer alloc.free(marker_path);
    const marker = std.Io.Dir.cwd().readFileAlloc(io, marker_path, alloc, .limited(publication_marker_data.len + 1)) catch |err| switch (err) {
        // NotDir: the live root is a file (e.g. an Antfly Lite *.aflite
        // database), so a marker can never exist beneath it.
        error.FileNotFound, error.NotDir => return false,
        else => return err,
    };
    defer alloc.free(marker);
    if (!std.mem.eql(u8, marker, publication_marker_data)) return error.InvalidGenerationPublicationMarker;
    return true;
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

const RetiredGenerationCleanupBatch = struct {
    alloc: Allocator,
    paths: [][]u8,
    parent: []u8,

    fn run(ptr: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (builtin.is_test and test_block_retired_cleanup.load(.acquire)) {
            test_retired_cleanup_started.store(true, .release);
            while (test_block_retired_cleanup.load(.acquire)) platform.time.yieldBriefly();
        }
        var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
        defer io_impl.deinit();
        const io = io_impl.io();
        try deleteRetiredGenerationPaths(io, self.paths, self.parent);
    }

    fn deinit(ptr: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const alloc = self.alloc;
        for (self.paths) |path| alloc.free(path);
        alloc.free(self.paths);
        alloc.free(self.parent);
        alloc.destroy(self);
    }
};

fn deleteRetiredGenerationPaths(io: std.Io, paths: []const []const u8, parent: []const u8) !void {
    var first_error: ?anyerror = null;
    for (paths) |path| {
        std.Io.Dir.cwd().deleteTree(io, path) catch |err| switch (err) {
            error.NotDir => {},
            else => if (first_error == null) {
                first_error = err;
            },
        };
    }
    try fs_paths.syncDirPortable(io, parent);
    if (first_error) |err| return err;
}

fn scheduleRetiredGenerationCleanup(scheduler: ?CleanupScheduler, path: []const u8, parent: []const u8) !void {
    return try scheduleRetiredGenerationCleanupBatch(scheduler, &.{path}, parent);
}

fn scheduleRetiredGenerationCleanupBatch(scheduler: ?CleanupScheduler, paths: []const []const u8, parent: []const u8) !void {
    const active = scheduler orelse return;
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
    work.* = .{
        .alloc = active.alloc,
        .paths = owned_paths,
        .parent = try active.alloc.dupe(u8, parent),
    };
    errdefer active.alloc.free(work.parent);
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

    var stale_paths = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (stale_paths.items) |path| alloc.free(path);
        stale_paths.deinit(alloc);
    }

    var iterator = dir.iterate();
    while (try iterator.next(io)) |entry| {
        if (entry.kind != .directory or !isGeneratedStageName(entry.name, stage_prefix)) continue;
        const stale_path = try std.fs.path.join(alloc, &.{ parent, entry.name });
        var stale_path_owned = true;
        defer if (stale_path_owned) alloc.free(stale_path);
        const has_marker = hasPublicationMarker(alloc, io, stale_path) catch |err| {
            std.log.warn("stale generation marker validation deferred path={s} err={s}", .{ stale_path, @errorName(err) });
            continue;
        };
        if (scheduler != null) {
            std.log.info("scheduling stale generation cleanup path={s} marker={}", .{ stale_path, has_marker });
        } else {
            std.log.info("deferring stale generation cleanup until read admission path={s} marker={}", .{ stale_path, has_marker });
        }
        try stale_paths.append(alloc, stale_path);
        stale_path_owned = false;
    }
    if (scheduler != null) {
        scheduleRetiredGenerationCleanupBatch(scheduler, stale_paths.items, parent) catch |err| {
            std.log.warn("stale generation cleanup batch scheduling deferred path={s} count={} err={s}", .{ live_path, stale_paths.items.len, @errorName(err) });
        };
    } else {
        try deferred_cleanup.ensureUnusedCapacity(alloc, stale_paths.items.len);
        for (stale_paths.items) |path| deferred_cleanup.appendAssumeCapacity(path);
        stale_paths.clearRetainingCapacity();
    }
}

fn reconcilePublishedGeneration(
    alloc: Allocator,
    io: std.Io,
    live_path: []const u8,
    scheduler: ?CleanupScheduler,
    deferred_cleanup: *std.ArrayListUnmanaged([]u8),
) !bool {
    if (try hasPublicationMarker(alloc, io, live_path)) {
        const parent = std.fs.path.dirname(live_path) orelse if (std.fs.path.isAbsolute(live_path)) "/" else ".";
        if (builtin.is_test and test_fail_reconciliation_sync) {
            test_fail_reconciliation_sync = false;
            return error.GenerationDurabilityUncertain;
        }
        fs_paths.syncDirPortable(io, parent) catch return error.GenerationDurabilityUncertain;
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
        try deleteRetiredGenerationPaths(io, deferred_cleanup.items, parent);
    }
    return reconciled;
}

pub fn acquirePublishedGenerationRead(alloc: Allocator, path: []const u8) !?ReadLease {
    return try acquirePublishedGenerationReadWithRuntime(alloc, path, null);
}

pub fn acquirePublishedGenerationReadWithRuntime(alloc: Allocator, path: []const u8, runtime: ?*background_runtime.BackendRuntime) !?ReadLease {
    var reconciliation = reconcile: while (true) {
        if (try process_manager.beginReconciliation(path)) |value| break :reconcile value;

        const path_key = try canonicalPathAlloc(process_manager_allocator, path);
        defer process_manager_allocator.free(path_key);
        const publication_lock = try openPublicationLock(process_manager_allocator, path_key, .shared);
        var lock_owned = true;
        errdefer if (lock_owned) closePublicationLock(publication_lock);
        if (try process_manager.beginReadIfReconciledCanonical(path, path_key)) |read_value| {
            var read_lease = read_value;
            read_lease.publication_lock = publication_lock;
            lock_owned = false;
            return read_lease;
        }
        closePublicationLock(publication_lock);
        lock_owned = false;
    };
    defer reconciliation.deinit();
    var publication_lock = try openPublicationLock(process_manager_allocator, reconciliation.path_key, .exclusive);
    errdefer closePublicationLock(publication_lock);
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    var deferred_cleanup = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (deferred_cleanup.items) |stale_path| alloc.free(stale_path);
        deferred_cleanup.deinit(alloc);
    }
    const cleanup_scheduler = CleanupScheduler.fromRuntime(runtime);
    const reconciled = try reconcilePublishedGeneration(alloc, io_impl.io(), path, cleanup_scheduler, &deferred_cleanup);
    try publication_lock.downgradeLock(io_impl.io());
    var read_lease = try reconciliation.promoteToRead(publication_lock, reconciled);
    errdefer read_lease.deinit();
    if (deferred_cleanup.items.len > 0) {
        const parent = std.fs.path.dirname(path) orelse if (std.fs.path.isAbsolute(path)) "/" else ".";
        deleteRetiredGenerationPaths(io_impl.io(), deferred_cleanup.items, parent) catch |err| {
            std.log.warn("stale generation cleanup remains retryable after read admission path={s} count={} err={s}", .{ path, deferred_cleanup.items.len, @errorName(err) });
        };
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
        const left_z = try alloc.dupeZ(u8, left);
        defer alloc.free(left_z);
        const right_z = try alloc.dupeZ(u8, right);
        defer alloc.free(right_z);
        const rename_swap: c_uint = 0x0000_0002;
        return Darwin.renameatx_np(std.posix.AT.FDCWD, left_z.ptr, std.posix.AT.FDCWD, right_z.ptr, rename_swap) == 0;
    }
    if (comptime builtin.os.tag == .linux) {
        const linux = std.os.linux;
        const left_z = try alloc.dupeZ(u8, left);
        defer alloc.free(left_z);
        const right_z = try alloc.dupeZ(u8, right);
        defer alloc.free(right_z);
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
    var transition = try process_manager.beginExclusive(path);
    transition.cleanup_scheduler = CleanupScheduler.fromRuntime(runtime);
    return transition;
}

pub fn beginProcessPreparationWithRuntime(path: []const u8, runtime: ?*background_runtime.BackendRuntime) !PreparationTransition {
    return try process_manager.beginPreparation(path, CleanupScheduler.fromRuntime(runtime));
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
    var staged = try transition.beginStaging();
    defer staged.deinit();
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
    const current = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, live_value_path, alloc, .limited(16));
    defer alloc.free(current);
    try std.testing.expectEqualStrings("current", current);

    test_block_retired_cleanup.store(false, .release);
    const scheduler = staged.cleanup_scheduler orelse return error.TestUnexpectedResult;
    scheduler.lane.drainOwner(scheduler.owner_id);
    try std.testing.expect(!pathExists(std.testing.io, staged.path()));
}

test "generation reconciliation reclaims stale roots after read admission without a cleanup runtime" {
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
    try std.testing.expect(pathExists(std.testing.io, staged.path()));
    transition.deinit();

    var lease = (try acquirePublishedGenerationRead(alloc, live_path)) orelse return error.TestUnexpectedResult;
    lease.deinit();
    try std.testing.expect(!pathExists(std.testing.io, staged.path()));
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
