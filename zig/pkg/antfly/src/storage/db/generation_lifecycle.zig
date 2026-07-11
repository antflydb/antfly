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

const Allocator = std.mem.Allocator;
const publication_marker_name = ".antfly-generation-publication-v1";
const publication_marker_data = "antfly_generation_publication_v1\n";
const publication_lock_suffix = ".antfly-generation.lock";
const max_reconciled_paths = 8192;

fn publicationLockPathAlloc(alloc: Allocator, canonical_path: []const u8) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{s}{s}", .{ canonical_path, publication_lock_suffix });
}

fn openPublicationLock(alloc: Allocator, canonical_path: []const u8, lock: std.Io.File.Lock) !std.Io.File {
    const lock_path = try publicationLockPathAlloc(alloc, canonical_path);
    defer alloc.free(lock_path);
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

fn closePublicationLock(file: std.Io.File) void {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    file.close(io_impl.io());
}

const LeaseKind = enum { exclusive, reconciliation, read };

fn realPathAlloc(alloc: Allocator, io: std.Io, path: []const u8) ![:0]u8 {
    if (std.fs.path.isAbsolute(path)) return try std.Io.Dir.realPathFileAbsoluteAlloc(io, path, alloc);
    return try std.Io.Dir.cwd().realPathFileAlloc(io, path, alloc);
}

fn canonicalPathAlloc(alloc: Allocator, path: []const u8) ![]u8 {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    const existing: ?[:0]u8 = realPathAlloc(alloc, io, path) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => null,
    };
    if (existing) |canonical| {
        defer alloc.free(canonical);
        return try alloc.dupe(u8, canonical);
    }

    const parent = std.fs.path.dirname(path) orelse ".";
    const canonical_parent: ?[:0]u8 = realPathAlloc(alloc, io, parent) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => null,
    };
    if (canonical_parent) |canonical| {
        defer alloc.free(canonical);
        return try std.fs.path.join(alloc, &.{ canonical, std.fs.path.basename(path) });
    }

    if (std.fs.path.isAbsolute(path)) return try std.fs.path.resolve(alloc, &.{path});
    const cwd: ?[:0]u8 = realPathAlloc(alloc, io, ".") catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => null,
    };
    if (cwd) |canonical| {
        defer alloc.free(canonical);
        return try std.fs.path.resolve(alloc, &.{ canonical, path });
    }
    return try std.fs.path.resolve(alloc, &.{path});
}

pub const PublicationOutcome = enum {
    durable,
    durability_uncertain,
};

var test_fail_post_publish_sync = false;
var test_fail_reconciliation_sync = false;
var test_disable_atomic_exchange = false;

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
    reconciliation: bool = false,
    exclusive: bool = false,

    fn isEmpty(self: PathState) bool {
        return self.readers == 0 and !self.reconciliation and !self.exclusive;
    }
};

pub const Manager = struct {
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
        };
        return transition;
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

    fn validateExclusive(self: *Manager, id: u64) !void {
        platform.sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        const entry = self.active.get(id) orelse return error.InvalidGenerationTransition;
        if (entry.kind == .exclusive) return;
        return error.InvalidGenerationTransition;
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
        if (self.publication_lock) |file| closePublicationLock(file);
        self.publication_lock = null;
        self.manager.finishRead(self.path, self.id);
        self.active = false;
    }
};

pub const ExclusiveTransition = struct {
    manager: *Manager,
    alloc: Allocator,
    path: []const u8,
    id: u64,
    publication_lock: ?std.Io.File = null,
    active: bool = true,

    pub fn validate(self: *const ExclusiveTransition, path: []const u8) !void {
        if (!self.active or !std.mem.eql(u8, self.path, path)) return error.InvalidGenerationTransition;
    }

    pub fn deinit(self: *ExclusiveTransition) void {
        if (!self.active) return;
        if (self.publication_lock) |file| closePublicationLock(file);
        self.publication_lock = null;
        self.manager.finishExclusive(self.path, self.id);
        self.active = false;
    }

    pub fn reconcilePublished(self: *ExclusiveTransition) !void {
        try self.validate(self.path);
        var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
        defer io_impl.deinit();
        _ = try reconcilePublishedGeneration(self.alloc, io_impl.io(), self.path);
    }

    pub fn beginStaging(self: *ExclusiveTransition) !StagedGeneration {
        try self.validate(self.path);
        const live_path = try self.alloc.dupe(u8, self.path);
        errdefer self.alloc.free(live_path);
        const nonce = platform.time.monotonicNs();
        const staging_path = try std.fmt.allocPrint(self.alloc, "{s}.restore-stage-{x}-{x}", .{ self.path, self.id, nonce });
        errdefer self.alloc.free(staging_path);

        var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
        defer io_impl.deinit();
        const io = io_impl.io();
        _ = try reconcilePublishedGeneration(self.alloc, io, self.path);
        if (pathExists(io, staging_path)) return error.GenerationStagingCollision;
        try fs_paths.createDirPathPortable(io, staging_path);
        errdefer std.Io.Dir.cwd().deleteTree(io, staging_path) catch {};
        try writePublicationMarker(self.alloc, io, staging_path);
        return .{
            .alloc = self.alloc,
            .manager = self.manager,
            .transition_id = self.id,
            .live_path = live_path,
            .staging_path = staging_path,
        };
    }
};

pub const StagedGeneration = struct {
    alloc: Allocator,
    manager: *Manager,
    transition_id: u64,
    live_path: []u8,
    staging_path: []u8,
    published: bool = false,
    preserve_retired: bool = false,
    closed: bool = false,

    pub fn path(self: *const StagedGeneration) []const u8 {
        return self.staging_path;
    }

    pub fn validatePath(self: *const StagedGeneration, path_value: []const u8) !void {
        if (self.closed or !std.mem.eql(u8, self.staging_path, path_value)) return error.InvalidGenerationTransition;
    }

    pub fn validateLivePath(self: *const StagedGeneration, path_value: []const u8) !void {
        if (self.closed or !std.mem.eql(u8, self.live_path, path_value)) return error.InvalidGenerationTransition;
    }

    /// Errors are returned only before the live namespace changes. Once the
    /// generation is visible, durability failures are represented in the
    /// outcome so callers must finish their committed transition.
    pub fn publish(self: *StagedGeneration) !PublicationOutcome {
        if (self.closed or self.published) return error.InvalidGenerationTransition;
        try self.manager.validateExclusive(self.transition_id);

        var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
        defer io_impl.deinit();
        const io = io_impl.io();
        if (!pathExists(io, self.staging_path)) return error.GenerationStagingMissing;
        try syncTreePortable(self.alloc, io, self.staging_path);
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
            const outcome = syncPublishedParent(io, parent);
            if (outcome == .durable) {
                const retired_removed = removeTreeIfExists(io, self.staging_path) catch |err| blk: {
                    std.log.warn("retired exchanged generation cleanup deferred path={s} err={s}", .{ self.staging_path, @errorName(err) });
                    break :blk false;
                };
                if (retired_removed) {
                    fs_paths.syncDirPortable(io, parent) catch |err| {
                        std.log.warn("retired exchanged generation parent sync deferred path={s} err={s}", .{ parent, @errorName(err) });
                        return outcome;
                    };
                    _ = clearPublicationMarker(self.alloc, io, self.live_path);
                }
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

fn removeTreeIfExists(io: std.Io, path: []const u8) !bool {
    if (!pathExists(io, path)) return false;
    try std.Io.Dir.cwd().deleteTree(io, path);
    return true;
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

fn cleanupStagedGenerations(alloc: Allocator, io: std.Io, live_path: []const u8) !bool {
    const parent = std.fs.path.dirname(live_path) orelse if (std.fs.path.isAbsolute(live_path)) "/" else ".";
    const live_name = std.fs.path.basename(live_path);
    const stage_prefix = try std.fmt.allocPrint(alloc, "{s}.restore-stage-", .{live_name});
    defer alloc.free(stage_prefix);

    var dir = (if (std.fs.path.isAbsolute(parent))
        std.Io.Dir.openDirAbsolute(io, parent, .{ .iterate = true })
    else
        std.Io.Dir.cwd().openDir(io, parent, .{ .iterate = true })) catch |err| switch (err) {
        error.FileNotFound, error.NotDir => return false,
        else => return err,
    };
    defer dir.close(io);

    var removed = false;
    var iterator = dir.iterate();
    while (try iterator.next(io)) |entry| {
        if (entry.kind != .directory or !isGeneratedStageName(entry.name, stage_prefix)) continue;
        const stale_path = try std.fs.path.join(alloc, &.{ parent, entry.name });
        defer alloc.free(stale_path);
        const has_marker = hasPublicationMarker(alloc, io, stale_path) catch |err| {
            std.log.warn("stale generation marker validation deferred path={s} err={s}", .{ stale_path, @errorName(err) });
            continue;
        };
        if (!has_marker) std.log.info("reclaiming markerless retired generation path={s}", .{stale_path});
        std.Io.Dir.cwd().deleteTree(io, stale_path) catch |err| {
            std.log.warn("stale generation cleanup deferred path={s} err={s}", .{ stale_path, @errorName(err) });
            continue;
        };
        removed = true;
    }
    return removed;
}

fn reconcilePublishedGeneration(alloc: Allocator, io: std.Io, live_path: []const u8) !bool {
    if (try hasPublicationMarker(alloc, io, live_path)) {
        const parent = std.fs.path.dirname(live_path) orelse if (std.fs.path.isAbsolute(live_path)) "/" else ".";
        if (builtin.is_test and test_fail_reconciliation_sync) {
            test_fail_reconciliation_sync = false;
            return error.GenerationDurabilityUncertain;
        }
        fs_paths.syncDirPortable(io, parent) catch return error.GenerationDurabilityUncertain;
    }

    const removed = cleanupStagedGenerations(alloc, io, live_path) catch |err| {
        std.log.warn("stale generation reconciliation deferred path={s} err={s}", .{ live_path, @errorName(err) });
        return false;
    };
    if (removed) {
        const parent = std.fs.path.dirname(live_path) orelse if (std.fs.path.isAbsolute(live_path)) "/" else ".";
        fs_paths.syncDirPortable(io, parent) catch |err| {
            std.log.warn("stale generation parent sync deferred path={s} err={s}", .{ parent, @errorName(err) });
            return false;
        };
    }
    return clearPublicationMarker(alloc, io, live_path);
}

pub fn acquirePublishedGenerationRead(alloc: Allocator, path: []const u8) !?ReadLease {
    if (std.mem.indexOf(u8, std.fs.path.basename(path), ".restore-stage-") != null) return null;
    var reconciliation = (try process_manager.beginReconciliation(path)) orelse {
        var read_lease = try process_manager.beginRead(path);
        errdefer read_lease.deinit();
        read_lease.publication_lock = try openPublicationLock(process_manager_allocator, read_lease.path_key, .shared);
        return read_lease;
    };
    defer reconciliation.deinit();
    var publication_lock = try openPublicationLock(process_manager_allocator, reconciliation.path_key, .exclusive);
    errdefer closePublicationLock(publication_lock);
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const reconciled = try reconcilePublishedGeneration(alloc, io_impl.io(), path);
    try publication_lock.downgradeLock(io_impl.io());
    return try reconciliation.promoteToRead(publication_lock, reconciled);
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
    var read_lease = (try acquirePublishedGenerationRead(alloc, live_path)) orelse return error.TestUnexpectedResult;
    try std.testing.expectError(error.GenerationTransitionActive, beginProcessExclusive(live_path));
    try std.testing.expect(!pathExists(std.testing.io, value_path));
    const marker_path = try publicationMarkerPathAlloc(alloc, live_path);
    defer alloc.free(marker_path);
    try std.testing.expect(!pathExists(std.testing.io, marker_path));
    read_lease.deinit();

    var next_transition = try beginProcessExclusive(live_path);
    next_transition.deinit();
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
