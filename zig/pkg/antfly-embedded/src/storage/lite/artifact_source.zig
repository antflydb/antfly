// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Owned native artifact ranges. A catalog reference leases the immutable value
//! root, rather than an entire checkpoint. A locked, empty marker distinguishes
//! live leases from crash leftovers without changing the .aflite wire format.
//! Provider/runtime ownership is independent of the document Store lifetime.
const std = @import("std");
const native = @import("native.zig");
const docs = @import("docstore.zig");
const Source = @import("../../segment_source.zig").Source;
const limits = @import("antfly_runtime_fs").threaded_io_limits;
const Allocator = std.mem.Allocator;

const key_prefix = native.NativeFile.artifact_lease_key_prefix;
const marker_prefix = ".aflite-lease-";

pub const Registry = struct {
    allocator: Allocator,
    io_impl: std.Io.Threaded,
    path: []u8,
    store: ?*docs.Store,
    publication_guard: ?std.Io.File = null,
    mutex: std.Io.Mutex = .init,
    callbacks_done: std.Io.Event = .is_set,
    callbacks: usize = 0,
    references: std.atomic.Value(usize) = .init(1),
    first: ?*State = null,
    retired_bytes: std.atomic.Value(u64) = .init(0),
    owned_resources: ?struct { manager: *@import("../resource_manager.zig").ResourceManager, allocator: Allocator } = null,

    pub fn create(allocator: Allocator, store: *docs.Store) !*Registry {
        const self = try allocator.create(Registry);
        errdefer allocator.destroy(self);
        const path = try allocator.dupe(u8, store.file.path);
        errdefer allocator.free(path);
        self.* = .{ .allocator = allocator, .io_impl = limits.initService(allocator), .path = path, .store = store };
        errdefer self.io_impl.deinit();
        if (!store.read_only) self.publication_guard = try openPublicationGuard(allocator, self.io(), path);
        return self;
    }

    /// Transfer the backend's resource owner before dropping the Store's
    /// registry reference. Pinned artifact readers and their cache callbacks
    /// can outlive backend close, so accounting must outlive those readers too.
    pub fn adoptOwnedResources(self: *Registry, manager: *@import("../resource_manager.zig").ResourceManager, allocator: Allocator) void {
        std.debug.assert(self.owned_resources == null);
        self.owned_resources = .{ .manager = manager, .allocator = allocator };
    }

    fn io(self: *Registry) std.Io {
        return self.io_impl.io();
    }

    fn release(self: *Registry) void {
        if (self.references.fetchSub(1, .acq_rel) != 1) return;
        std.debug.assert(self.first == null);
        std.debug.assert(self.callbacks == 0);
        std.debug.assert(self.retired_bytes.load(.acquire) == 0);
        if (self.publication_guard) |guard| guard.close(self.io());
        self.io_impl.deinit();
        if (self.owned_resources) |owner| {
            owner.manager.deinit(owner.allocator);
            owner.allocator.destroy(owner.manager);
        }
        self.allocator.free(self.path);
        self.allocator.destroy(self);
    }

    // Store close detaches before acquiring any Store lock. A callback uses
    // the captured pointer only while counted, and never holds this mutex
    // while entering the Store's writer queue (or waiting for publication).
    pub fn detach(self: *Registry) void {
        const runtime = self.io();
        self.mutex.lockUncancelable(runtime);
        self.store = null;
        const waiting = self.callbacks != 0;
        self.mutex.unlock(runtime);
        if (waiting) self.callbacks_done.waitUncancelable(runtime);
    }

    pub fn releaseOwner(self: *Registry) void {
        self.release();
    }

    fn beginCallback(self: *Registry) ?*docs.Store {
        const runtime = self.io();
        self.mutex.lockUncancelable(runtime);
        defer self.mutex.unlock(runtime);
        const owner = self.store orelse return null;
        if (self.callbacks == 0) self.callbacks_done.reset();
        self.callbacks += 1;
        return owner;
    }

    fn endCallback(self: *Registry) void {
        const runtime = self.io();
        self.mutex.lockUncancelable(runtime);
        defer self.mutex.unlock(runtime);
        self.callbacks -= 1;
        if (self.callbacks == 0) self.callbacks_done.set(runtime);
    }

    pub fn testPageReads(self: *Registry) u64 {
        std.debug.assert(@import("builtin").is_test);
        const runtime = self.io();
        self.mutex.lockUncancelable(runtime);
        defer self.mutex.unlock(runtime);
        var total: u64 = 0;
        var it = self.first;
        while (it) |state| : (it = state.next) {
            @import("antfly_platform").sync.lockYielding(&state.lifetime_mutex);
            if (!state.resources_closed.load(.monotonic)) total += state.reader.test_page_reads.load(.monotonic);
            state.lifetime_mutex.unlock();
        }
        return total;
    }

    pub fn hasGeneration(self: *Registry, generation: u64) bool {
        const runtime = self.io();
        self.mutex.lockUncancelable(runtime);
        defer self.mutex.unlock(runtime);
        var it = self.first;
        while (it) |state| : (it = state.next) {
            if (state.generation == generation and !state.resources_closed.load(.acquire)) return true;
        }
        return false;
    }

    /// Drop cache-only descriptors before rewrite admission. The caller holds
    /// the owner publication mutex; final adoption additionally fences opens
    /// with generation_lock. Existing uses win the lifetime-lock race and
    /// remain pinned; future acquisitions reject the closed source.
    pub fn expireIdle(self: *Registry, generation: u64) void {
        const runtime = self.io();
        self.mutex.lockUncancelable(runtime);
        defer self.mutex.unlock(runtime);
        var it = self.first;
        while (it) |state| : (it = state.next) {
            if (state.generation != generation) continue;
            @import("antfly_platform").sync.lockYielding(&state.lifetime_mutex);
            if (state.idle_expiry and state.active_uses == 0) state.closeResources();
            state.lifetime_mutex.unlock();
        }
    }

    pub fn retire(self: *Registry, generation: u64, bytes: u64, charged_by_reader: bool) void {
        const runtime = self.io();
        self.mutex.lockUncancelable(runtime);
        defer self.mutex.unlock(runtime);
        var it = self.first;
        var claimant: ?*State = null;
        while (it) |state| : (it = state.next) {
            if (state.generation != generation) continue;
            state.retired_size = bytes;
            state.retire();
            if (!state.resources_closed.load(.acquire)) claimant = state;
        }
        if (!charged_by_reader) if (claimant) |state| {
            state.retired_claim = bytes;
            _ = self.retired_bytes.fetchAdd(bytes, .acq_rel);
        };
    }

    pub fn releaseReaderFence(self: *Registry, generation: u64) void {
        const runtime = self.io();
        self.mutex.lockUncancelable(runtime);
        defer self.mutex.unlock(runtime);
        var it = self.first;
        while (it) |state| : (it = state.next) {
            if (state.generation != generation or state.retired_size == 0 or state.resources_closed.load(.acquire)) continue;
            state.retired_claim = state.retired_size;
            _ = self.retired_bytes.fetchAdd(state.retired_claim, .acq_rel);
            return;
        }
    }

    fn releaseClosedClaim(self: *Registry, state: *State) void {
        const runtime = self.io();
        self.mutex.lockUncancelable(runtime);
        defer self.mutex.unlock(runtime);
        if (state.retired_claim == 0) return;
        var it = self.first;
        while (it) |other| : (it = other.next) {
            if (other == state or other.generation != state.generation or other.resources_closed.load(.acquire)) continue;
            other.retired_claim = state.retired_claim;
            state.retired_claim = 0;
            return;
        }
        _ = self.retired_bytes.fetchSub(state.retired_claim, .acq_rel);
        state.retired_claim = 0;
    }

    fn createMarker(self: *Registry, path: []const u8) !std.Io.File {
        const runtime = self.io();
        // File locks alone do not serialize callers sharing one descriptor.
        // This mutex covers only publication, never the Store writer queue.
        self.mutex.lockUncancelable(runtime);
        defer self.mutex.unlock(runtime);
        const guard = self.publication_guard orelse return error.ReadOnly;
        try guard.lock(runtime, .exclusive);
        defer guard.unlock(runtime);
        return std.Io.Dir.cwd().createFile(runtime, path, .{ .read = true, .exclusive = true, .lock = .exclusive, .lock_nonblocking = true, .permissions = .fromMode(0o600) });
    }

    pub fn open(self: *Registry, key: []const u8) !Source {
        const owner = self.beginCallback() orelse return error.ReadOnly;
        defer self.endCallback();
        // Capturing a root and opening its inode must straddle no adoption.
        const owner_io = owner.file.runtime();
        owner.generation_lock.lockSharedUncancelable(owner_io);
        defer owner.generation_lock.unlockShared(owner_io);
        const runtime = self.io();
        const a = self.allocator;
        const state = try a.create(State);
        errdefer a.destroy(state);
        var random: [16]u8 = undefined;
        try runtime.randomSecure(&random);
        const identity = try std.fmt.allocPrint(a, "{x}", .{random});
        defer a.free(identity);
        const lease_key = try std.mem.concat(a, u8, &.{ key_prefix, identity });
        errdefer a.free(lease_key);
        const marker_name = try std.mem.concat(a, u8, &.{ marker_prefix, identity });
        defer a.free(marker_name);
        const marker_path = try std.fs.path.join(a, &.{ std.fs.path.dirname(self.path) orelse ".", marker_name });
        errdefer a.free(marker_path);
        const marker: ?std.Io.File = if (owner.read_only) null else try self.createMarker(marker_path);
        errdefer if (marker) |file| {
            file.close(runtime);
            std.Io.Dir.cwd().deleteFile(runtime, marker_path) catch {};
        };
        // Writable roots are protected by the catalog reference. Read-only
        // sources use the established external-reader lock, since they cannot
        // publish a lease and external writers must not reuse their pages.
        var reader = try native.NativeFile.openWithIo(a, runtime, self.path, .{ .read_only = true, .internal_reader = !owner.read_only, .no_sync = true });
        errdefer reader.close();
        reader.page_cache_enabled.store(false, .monotonic);
        var capture = Capture{ .allocator = a, .key = key, .lease_key = lease_key };
        errdefer capture.value.deinit(a);
        if (owner.read_only) {
            capture.checkpoint = reader.activeCheckpoint();
            capture.value = try reader.openIndexValue(a, key, capture.checkpoint);
        } else try owner.submitMutation(&capture, Capture.apply);
        state.* = .{
            .registry = self,
            .reader = reader,
            .value = capture.value,
            .checkpoint = capture.checkpoint,
            .generation = owner.assessment_generation,
            .lease_key = lease_key,
            .marker_path = marker_path,
            .marker = marker,
        };
        state.cache = try @import("../../segment_source.zig").ConcurrentBlockCache.init(a, .{ .ranges = .{ .ptr = state, .length = state.value.length, .read_into = State.readUncached, .checksum = State.checksum, .visit_range = State.visit, .close = State.closeUncached, .resource_manager = owner.resource_manager } }, 160 * 1024);
        self.mutex.lockUncancelable(runtime);
        state.next = self.first;
        self.first = state;
        _ = self.references.fetchAdd(1, .monotonic);
        self.mutex.unlock(runtime);
        return .{ .ranges = .{ .ptr = state, .length = state.value.length, .read_into = State.read, .checksum = State.checksum, .read_authenticated = State.authenticate, .close = State.close, .retained_bytes = State.retainedBytes, .resource_manager = owner.resource_manager, .acquire_use = State.acquireUse, .release_use = State.releaseUse, .enable_idle_expiry = State.enableIdleExpiry } };
    }
};

const Capture = struct {
    allocator: Allocator,
    key: []const u8,
    lease_key: []const u8,
    checkpoint: native.CheckpointSlot = .{},
    value: native.NativeFile.IndexValue = .{ .root = 0, .length = 0 },

    fn apply(ptr: *anyopaque, file: *native.NativeFile) !void {
        const self: *Capture = @ptrCast(@alignCast(ptr));
        self.checkpoint = try file.materializeTransactionCheckpoint();
        self.value = try file.openIndexValue(self.allocator, self.key, self.checkpoint);
        errdefer {
            self.value.deinit(self.allocator);
            self.value = .{ .root = 0, .length = 0 };
        }
        try file.putCatalogBatch(&.{.{ .key = self.lease_key, .value = self.value.inline_bytes, .external_value_root_page = self.value.root, .external_value_len = self.value.length }});
    }
};

const Remove = struct {
    key: []const u8,
    fn apply(ptr: *anyopaque, file: *native.NativeFile) !void {
        const self: *Remove = @ptrCast(@alignCast(ptr));
        try file.putCatalogBatch(&.{.{ .key = self.key, .is_delete = true }});
    }
};

const State = struct {
    registry: *Registry,
    reader: native.NativeFile,
    value: native.NativeFile.IndexValue,
    checkpoint: native.CheckpointSlot,
    generation: u64,
    lease_key: []u8,
    marker_path: []u8,
    marker: ?std.Io.File,
    next: ?*State = null,
    retired_size: u64 = 0,
    retired_claim: u64 = 0,
    cache: ?@import("../../segment_source.zig").ConcurrentBlockCache = null,
    lifetime_mutex: std.atomic.Mutex = .unlocked,
    active_uses: usize = 0,
    idle_expiry: bool = false,
    retired: bool = false,
    resources_closed: std.atomic.Value(bool) = .init(false),

    fn closeResources(self: *State) void {
        if (self.resources_closed.load(.monotonic)) return;
        self.cache.?.deinit();
        self.cache = null;
        self.reader.close();
        if (self.marker) |marker| {
            marker.close(self.registry.io());
            self.marker = null;
            std.Io.Dir.cwd().deleteFile(self.registry.io(), self.marker_path) catch {};
        }
        self.resources_closed.store(true, .release);
    }

    // Registry -> lifetime is the only nested lock order. Release closes
    // resources first, then updates registry accounting after unlocking.
    fn retire(self: *State) void {
        @import("antfly_platform").sync.lockYielding(&self.lifetime_mutex);
        defer self.lifetime_mutex.unlock();
        self.retired = true;
        if (self.idle_expiry and self.active_uses == 0) self.closeResources();
    }

    fn acquireUse(ptr: *anyopaque) bool {
        const self: *State = @ptrCast(@alignCast(ptr));
        @import("antfly_platform").sync.lockYielding(&self.lifetime_mutex);
        defer self.lifetime_mutex.unlock();
        if (self.resources_closed.load(.monotonic)) return false;
        self.active_uses += 1;
        return true;
    }

    fn releaseUse(ptr: *anyopaque) void {
        const self: *State = @ptrCast(@alignCast(ptr));
        @import("antfly_platform").sync.lockYielding(&self.lifetime_mutex);
        std.debug.assert(self.active_uses > 0);
        self.active_uses -= 1;
        const expire = self.active_uses == 0 and self.idle_expiry and self.retired;
        if (expire) self.closeResources();
        self.lifetime_mutex.unlock();
        if (expire) self.registry.releaseClosedClaim(self);
    }

    fn enableIdleExpiry(ptr: *anyopaque) void {
        const self: *State = @ptrCast(@alignCast(ptr));
        @import("antfly_platform").sync.lockYielding(&self.lifetime_mutex);
        self.idle_expiry = true;
        const expire = self.active_uses == 0 and self.retired;
        if (expire) self.closeResources();
        self.lifetime_mutex.unlock();
        if (expire) self.registry.releaseClosedClaim(self);
    }

    fn read(ptr: *anyopaque, offset: u64, out: []u8) !void {
        const self: *State = @ptrCast(@alignCast(ptr));
        try self.cache.?.readInto(offset, out);
    }

    fn readUncached(ptr: *anyopaque, offset: u64, out: []u8) !void {
        const self: *State = @ptrCast(@alignCast(ptr));
        try self.reader.readIndexValueInto(self.value, offset, out, self.checkpoint);
    }

    fn retainedBytes(ptr: *anyopaque) usize {
        const self: *State = @ptrCast(@alignCast(ptr));
        @import("antfly_platform").sync.lockYielding(&self.lifetime_mutex);
        defer self.lifetime_mutex.unlock();
        return if (self.cache) |*cache| cache.retainedBytes() else 0;
    }

    fn visit(ptr: *anyopaque, offset: u64, length: u64, context: *anyopaque, visitor: *const fn (*anyopaque, u64, []const u8) anyerror!void) !void {
        const self: *State = @ptrCast(@alignCast(ptr));
        return self.reader.visitIndexValue(self.value, offset, length, self.checkpoint, context, visitor);
    }

    fn authenticate(ptr: *anyopaque, offset: u64, length: u64, within: usize, out: []u8, expected: ?u32) !void {
        const self: *State = @ptrCast(@alignCast(ptr));
        return self.cache.?.readAuthenticated(offset, length, within, out, expected);
    }

    fn checksum(ptr: *anyopaque, offset: u64, length: u64) !u32 {
        const self: *State = @ptrCast(@alignCast(ptr));
        return self.reader.checksumIndexValue(self.value, offset, length, self.checkpoint);
    }

    fn closeUncached(_: *anyopaque) void {}

    fn close(ptr: *anyopaque) void {
        const self: *State = @ptrCast(@alignCast(ptr));
        const registry = self.registry;
        const runtime = registry.io();
        @import("antfly_platform").sync.lockYielding(&self.lifetime_mutex);
        const remove_alias = !self.resources_closed.load(.monotonic) and self.marker != null;
        self.lifetime_mutex.unlock();
        // Snapshot under the lifetime lock, but never enter the publication
        // lane while holding it (publication may retire this same state).
        if (remove_alias) if (registry.beginCallback()) |owner| {
            defer registry.endCallback();
            var remove = Remove{ .key = self.lease_key };
            // On failure the marker will disappear and bounded orphan
            // service can retry. A failed release never frees live pages.
            owner.submitMutation(&remove, Remove.apply) catch {};
        };
        @import("antfly_platform").sync.lockYielding(&self.lifetime_mutex);
        self.closeResources();
        self.lifetime_mutex.unlock();
        registry.mutex.lockUncancelable(runtime);
        var link = &registry.first;
        while (link.* != self) link = &link.*.?.next;
        link.* = self.next;
        if (self.retired_claim != 0) {
            var it = registry.first;
            var transferred = false;
            while (it) |other| : (it = other.next) {
                if (other.generation != self.generation or other.resources_closed.load(.acquire)) continue;
                other.retired_claim = self.retired_claim;
                transferred = true;
                break;
            }
            if (!transferred) _ = registry.retired_bytes.fetchSub(self.retired_claim, .acq_rel);
        }
        registry.mutex.unlock(runtime);
        self.value.deinit(registry.allocator);
        registry.allocator.free(self.lease_key);
        registry.allocator.free(self.marker_path);
        registry.allocator.destroy(self);
        registry.release();
    }
};

/// Call under the owner publication mutex or before exposing a newly opened
/// owner. Locked markers represent live sources, including sources held by a
/// previous Store in this process. Missing or unlocked markers are orphans.
/// One call examines at most 64 aliases. The continuation prevents permanent
/// live leases at the start of the index from starving later crash leftovers.
pub const SweepCursor = struct {
    after: [key_prefix.len + 32]u8 = undefined,
    valid: bool = false,
};

pub fn cleanupOrphans(file: *native.NativeFile, sweep: *SweepCursor) !usize {
    const a = file.allocator;
    const runtime = file.runtime();
    const checkpoint = try file.materializeTransactionCheckpoint();
    var cursor = try file.metadataCatalogCursor(checkpoint, key_prefix);
    defer cursor.deinit();
    if (sweep.valid) cursor.start_after = &sweep.after;
    var keys: [64][]u8 = undefined;
    var count: usize = 0;
    defer for (keys[0..count]) |key| a.free(key);
    var examined: usize = 0;
    while (examined < keys.len) : (examined += 1) {
        const record = (try cursor.next()) orelse {
            sweep.valid = false;
            break;
        };
        var retain = false;
        defer if (!retain) a.free(record.key);
        const id = record.key[key_prefix.len..];
        if (id.len != 32) return error.InvalidArtifactLease;
        for (id) |byte| if (!std.ascii.isHex(byte)) return error.InvalidArtifactLease;
        @memcpy(&sweep.after, record.key);
        sweep.valid = true;
        const name = try std.mem.concat(a, u8, &.{ marker_prefix, id });
        defer a.free(name);
        const path = try std.fs.path.join(a, &.{ std.fs.path.dirname(file.path) orelse ".", name });
        defer a.free(path);
        const marker = std.Io.Dir.cwd().openFile(runtime, path, .{ .mode = .read_write, .lock = .exclusive, .lock_nonblocking = true }) catch |err| switch (err) {
            error.WouldBlock => continue,
            error.FileNotFound => null,
            else => return err,
        };
        if (marker) |owned| owned.close(runtime);
        keys[count] = record.key;
        retain = true;
        count += 1;
        std.Io.Dir.cwd().deleteFile(runtime, path) catch {};
    }
    if (count == 0) return 0;
    // Cursor navigation is finished before publication/reclamation can run.
    var mutations: [64]native.CatalogMutation = undefined;
    for (keys[0..count], 0..) |key, i| mutations[i] = .{ .key = key, .is_delete = true };
    try file.putCatalogBatch(mutations[0..count]);
    return count;
}

/// Vacuum intentionally drops inode-local aliases. Reap their crash-leftover
/// markers too, even when no catalog alias remains. Keep a directory cursor so
/// each maintenance cycle examines at most 64 entries without restarting at
/// permanent live files. Locks arbitrate with sources from any process/owner.
pub const MarkerSweep = struct {
    iterator: ?std.Io.Dir.Iterator = null,
    publication_guard: ?std.Io.File = null,

    fn resetIterator(self: *MarkerSweep, runtime: std.Io) void {
        if (self.iterator) |it| it.reader.dir.close(runtime);
        self.iterator = null;
    }
    pub fn deinit(self: *MarkerSweep, runtime: std.Io) void {
        self.resetIterator(runtime);
        if (self.publication_guard) |guard| guard.close(runtime);
        self.publication_guard = null;
    }

    pub fn run(self: *MarkerSweep, file: *native.NativeFile) !void {
        const runtime = file.runtime();
        if (self.publication_guard == null) self.publication_guard = try openPublicationGuard(file.allocator, runtime, file.path);
        const guard = self.publication_guard.?;
        // A new marker is visible before createFile has acquired its lock.
        // Coordinate that window with every sweeper in the directory, even
        // when another Store or process owns the marker's artifact root.
        try guard.lock(runtime, .exclusive);
        defer guard.unlock(runtime);
        if (self.iterator == null) {
            const dir = try std.Io.Dir.cwd().openDir(runtime, std.fs.path.dirname(file.path) orelse ".", .{ .iterate = true });
            self.iterator = dir.iterate();
        }
        errdefer self.resetIterator(runtime);
        const it = &self.iterator.?;
        for (0..64) |_| {
            const entry = (try it.next(runtime)) orelse {
                self.resetIterator(runtime);
                return;
            };
            if (entry.kind != .file or !std.mem.startsWith(u8, entry.name, marker_prefix)) continue;
            const id = entry.name[marker_prefix.len..];
            if (id.len != 32) continue;
            var valid = true;
            for (id) |byte| if (!std.ascii.isHex(byte)) {
                valid = false;
                break;
            };
            if (!valid) continue;
            const marker = it.reader.dir.openFile(runtime, entry.name, .{ .mode = .read_write, .lock = .exclusive, .lock_nonblocking = true }) catch |err| switch (err) {
                error.WouldBlock, error.FileNotFound => continue,
                else => return err,
            };
            defer marker.close(runtime);
            try it.reader.dir.deleteFile(runtime, entry.name);
        }
    }
};

// This persistent inode is deliberately outside the UUID marker namespace.
// Never unlink it: replacing its inode could split cross-process coordination.
fn openPublicationGuard(allocator: Allocator, io: std.Io, root: []const u8) !std.Io.File {
    const path = try std.fs.path.join(allocator, &.{ std.fs.path.dirname(root) orelse ".", ".aflite-lease-publication.lock" });
    defer allocator.free(path);
    return std.Io.Dir.cwd().createFile(io, path, .{ .read = true, .truncate = false, .permissions = .fromMode(0o600) });
}

test "lite persistent mapped lease publication excludes cross-root marker sweeping" {
    if (@import("builtin").os.tag != .linux and @import("builtin").os.tag != .macos) return error.SkipZigTest;
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/writer.aflite", .{tmp.sub_path});
    defer a.free(root);
    const other = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/other.aflite", .{tmp.sub_path});
    defer a.free(other);
    var file = try native.NativeFile.createWithIo(a, io, other, .{ .no_sync = true });
    defer file.close();
    const guard = try openPublicationGuard(a, io, root);
    defer guard.close(io);
    try guard.lock(io, .exclusive);
    var held = true;
    defer if (held) guard.unlock(io);
    const probe = try openPublicationGuard(a, io, other);
    defer probe.close(io);
    try std.testing.expect(!try probe.tryLock(io, .exclusive));
    const marker_path = try std.fs.path.join(a, &.{ std.fs.path.dirname(root).?, ".aflite-lease-0123456789abcdef0123456789abcdef" });
    defer a.free(marker_path);
    // Hold the exact create-to-lock window open deliberately. A different
    // root's sweeper must not reap this unaliased, not-yet-locked marker.
    const marker = try std.Io.Dir.cwd().createFile(io, marker_path, .{ .read = true, .exclusive = true });
    var marker_open = true;
    defer if (marker_open) marker.close(io);
    const Worker = struct {
        file: *native.NativeFile,
        started: std.Io.Event = .unset,
        done: std.Io.Event = .unset,
        result: anyerror!void = {},
        fn run(self: *@This()) void {
            var sweep: MarkerSweep = .{};
            defer sweep.deinit(self.file.runtime());
            self.started.set(self.file.runtime());
            self.result = sweep.run(self.file);
            self.done.set(self.file.runtime());
        }
    };
    var worker = Worker{ .file = &file };
    const thread = try std.Thread.spawn(.{}, Worker.run, .{&worker});
    var joined = false;
    defer if (!joined) {
        if (held) {
            guard.unlock(io);
            held = false;
        }
        thread.join();
    };
    worker.started.waitUncancelable(io);
    try io.sleep(.fromMilliseconds(10), .awake);
    try std.testing.expect(!worker.done.isSet());
    try marker.lock(io, .exclusive);
    guard.unlock(io);
    held = false;
    thread.join();
    joined = true;
    try worker.result;
    try std.Io.Dir.cwd().access(io, marker_path, .{});
    // A released/crashed lease remains reclaimable; the coordination inode
    // is persistent and cannot itself be mistaken for a UUID marker.
    marker.close(io);
    marker_open = false;
    var sweep: MarkerSweep = .{};
    defer sweep.deinit(io);
    try sweep.run(&file);
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().access(io, marker_path, .{}));
    try std.testing.expect(try probe.tryLock(io, .exclusive));
    probe.unlock(io);
}
