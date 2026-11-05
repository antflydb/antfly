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
const DecodedCache = @import("../../segment_source.zig").ImmutableDecodeCache;
const limits = @import("antfly_runtime_fs").threaded_io_limits;
const Allocator = std.mem.Allocator;

const key_prefix = native.NativeFile.artifact_lease_key_prefix;
const marker_prefix = ".aflite-lease-";
const source_cache_bytes = 160 * 1024;
const decoded_cache_bytes = 1024 * 1024;

const ArtifactIdentity = struct {
    path: []const u8,
    key: []const u8,
    inode: u64,
    root: u64,
    length: u64,
    inline_bytes: []const u8,
    pub fn hash(_: @This(), key: ArtifactIdentity) u64 {
        var h = std.hash.Wyhash.init(0);
        h.update(key.path);
        h.update(key.key);
        h.update(std.mem.asBytes(&key.inode));
        h.update(std.mem.asBytes(&key.root));
        h.update(std.mem.asBytes(&key.length));
        h.update(key.inline_bytes);
        return h.final();
    }
    pub fn eql(_: @This(), a: ArtifactIdentity, b: ArtifactIdentity) bool {
        return a.inode == b.inode and a.root == b.root and a.length == b.length and std.mem.eql(u8, a.path, b.path) and std.mem.eql(u8, a.key, b.key) and std.mem.eql(u8, a.inline_bytes, b.inline_bytes);
    }
};

// Weak entries: only open Sources own references. An existing durable alias
// keeps its immutable value alive, making root identity safe against page ABA.
// Never share inode-fenced readers: that would extend a coarse reuse fence.
var source_pool_mutex: std.atomic.Mutex = .unlocked;
var source_pool: std.HashMapUnmanaged(ArtifactIdentity, *State, ArtifactIdentity, 80) = .empty;
var source_pool_removals: usize = 0;

fn removePooled(identity: ArtifactIdentity) void {
    if (!source_pool.removeContext(identity, identity)) return;
    source_pool_removals += 1;
    // WAL/value versions churn while unchanged segments remain open. Keep
    // absent lookups independent of the historical number of closed sources.
    if (source_pool_removals >= @max(@as(usize, 64), source_pool.capacity() / 16)) {
        source_pool.rehash(identity);
        source_pool_removals = 0;
    }
}

fn hasPooledSources() bool {
    @import("antfly_platform").sync.lockYielding(&source_pool_mutex);
    defer source_pool_mutex.unlock();
    return source_pool.count() != 0;
}

fn retainPooled(identity: ArtifactIdentity) ?*State {
    @import("antfly_platform").sync.lockYielding(&source_pool_mutex);
    defer source_pool_mutex.unlock();
    const state = source_pool.getContext(identity, identity) orelse return null;
    @import("antfly_platform").sync.lockYielding(&state.lifetime_mutex);
    defer state.lifetime_mutex.unlock();
    if (state.resources_closed.load(.monotonic)) return null;
    state.source_references += 1;
    state.shared = true;
    return state;
}

/// A file owner may retain immutable artifact Sources across runtime teardown.
/// Entries hold neither a DB nor a primary snapshot. Exact pooled identities
/// remain protected by their durable artifact leases for the retained lifetime.
/// All methods are serialized by the file owner's admission gate.
pub const WorkingSet = struct {
    const max_entries = 128;
    const max_bytes = 128 * 1024 * 1024;
    pub const max_range_bytes = 128 * 1024 * 1024;
    const lifetime_ns = 5 * std.time.ns_per_s;
    entries: [max_entries]?*State = @splat(null),
    count: usize = 0,
    inode: ?u64 = null,
    bytes: u64 = 0,
    range_bytes: u64 = 0,
    expires_ns: u64 = 0,

    pub fn clear(self: *WorkingSet) void {
        // Release outside both pool and registry mutexes: final release may
        // retire its marker and resource-accounting owner.
        for (self.entries[0..self.count]) |entry| {
            const state = entry.?;
            State.releaseUse(state);
            State.close(state);
        }
        self.* = .{};
    }

    pub fn observe(self: *WorkingSet, inode: u64, now_ns: u64) void {
        if ((self.inode != null and self.inode.? != inode) or now_ns >= self.expires_ns)
            self.clear();
    }

    pub fn capture(self: *WorkingSet, registry: *Registry, inode: u64, generation_bytes: u64, now_ns: u64) void {
        self.observe(inode, now_ns);
        if (self.count == max_entries) return;
        // Source references are protected by the pool mutex. Registry owns
        // list membership; each state's lifetime mutex protects active uses.
        @import("antfly_platform").sync.lockYielding(&source_pool_mutex);
        defer source_pool_mutex.unlock();
        const io = registry.io();
        registry.mutex.lockUncancelable(io);
        defer registry.mutex.unlock(io);
        var next = registry.first;
        var visited: usize = 0;
        while (next) |state| {
            next = state.next;
            visited += 1;
            if (visited > max_entries or self.count == max_entries) break;
            self.captureState(state, inode, generation_bytes, now_ns);
        }
        // A pooled hit can belong to a detached prior registry. Remember only
        // bounded copied identities here, never raw State pointers or strong
        // Registry references. Resolve them while holding the pool mutex.
        for (registry.borrowed[0..registry.borrowed_count]) |identity| {
            if (self.count == max_entries) break;
            if (source_pool.getContext(identity, identity)) |state|
                self.captureState(state, inode, generation_bytes, now_ns);
        }
    }

    // Caller holds pool and registry mutexes, in that order.
    fn captureState(self: *WorkingSet, state: *State, inode: u64, generation_bytes: u64, now_ns: u64) void {
        const identity = state.pool_identity orelse return;
        if (identity.inode != inode) return;
        var duplicate = false;
        for (self.entries[0..self.count]) |entry| if (entry.? == state) {
            duplicate = true;
            break;
        };
        if (duplicate) return;
        @import("antfly_platform").sync.lockYielding(&state.lifetime_mutex);
        defer state.lifetime_mutex.unlock();
        if (state.marker == null or state.resources_closed.load(.monotonic)) return;
        const overhead = state.source().ranges.retention_overhead_bytes;
        // File ranges and resident memory have independent bounds.
        // The overhead reserves both source and decoded cache capacity.
        const charge = overhead;
        if (charge > max_bytes -| self.bytes or state.value.length > @min(generation_bytes, max_range_bytes) -| self.range_bytes) return;
        state.source_references += 1;
        state.active_uses += 1;
        self.entries[self.count] = state;
        self.count += 1;
        self.bytes += charge;
        self.range_bytes += state.value.length;
        self.inode = inode;
        // Existing entries do not extend their deadline on every poll.
        if (self.expires_ns == 0) self.expires_ns = now_ns +| lifetime_ns;
    }
};

pub const Registry = struct {
    allocator: Allocator,
    io_impl: std.Io.Threaded,
    path: [:0]u8,
    marker_namespace_safe: bool = false,
    store: ?*docs.Store,
    publication_guard: ?std.Io.File = null,
    mutex: std.Io.Mutex = .init,
    callbacks_done: std.Io.Event = .is_set,
    callbacks: usize = 0,
    references: std.atomic.Value(usize) = .init(1),
    first: ?*State = null,
    borrowed: [128]ArtifactIdentity = undefined,
    borrowed_count: usize = 0,
    borrowed_next: usize = 0,
    retired_bytes: std.atomic.Value(u64) = .init(0),
    coordinator: ?*native.ReaderCoordinator = null,
    owned_resources: ?struct { manager: *@import("../resource_manager.zig").ResourceManager, allocator: Allocator } = null,

    pub fn create(allocator: Allocator, store: *docs.Store) !*Registry {
        const self = try allocator.create(Registry);
        errdefer allocator.destroy(self);
        // Canonicalize symlinks before choosing a marker directory. Hard-link
        // aliases remain distinct and must prove the persisted namespace below.
        const path = try std.Io.Dir.cwd().realPathFileAlloc(store.file.runtime(), store.file.path, allocator);
        errdefer allocator.free(path);
        const marker_namespace_safe = !store.read_only and try @import("reader_epochs.zig").namespaceMatches(allocator, store.file.runtime(), path, store.file.header.reader_epoch_namespace);
        self.* = .{ .allocator = allocator, .io_impl = limits.initService(allocator), .path = path, .store = store, .marker_namespace_safe = marker_namespace_safe };
        errdefer self.io_impl.deinit();
        self.coordinator = store.file.reader_coordinator;
        if (self.coordinator) |coordinator| coordinator.retain();
        errdefer if (self.coordinator) |coordinator| coordinator.release();
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
        if (self.coordinator) |coordinator| coordinator.release();
        for (self.borrowed[0..self.borrowed_count]) |identity| {
            self.allocator.free(identity.key);
            self.allocator.free(identity.inline_bytes);
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
        // Cached descriptors may outlive their DB wrapper. Once publication
        // callbacks have drained, retire their weak resources immediately.
        // Explicit readers and active uses retain their independent lifetime.
        self.mutex.lockUncancelable(runtime);
        defer self.mutex.unlock(runtime);
        var it = self.first;
        while (it) |state| : (it = state.next) state.retire();
        it = self.first;
        while (it) |state| : (it = state.next) {
            if (state.resources_closed.load(.acquire)) self.releaseClosedClaimAssumeLocked(state);
        }
    }

    pub fn releaseOwner(self: *Registry) void {
        self.release();
    }

    /// Immutable decoder metadata can outlive its Store even when its range
    /// Source was pooled from a different registry. Keep this Store's resource
    /// accounting owner alive until that metadata is released. Backend close
    /// transfers the owned manager here before dropping its owner reference.
    pub fn retainOwner(self: *Registry) void {
        _ = self.references.fetchAdd(1, .monotonic);
    }

    pub fn beginCallback(self: *Registry) ?*docs.Store {
        const runtime = self.io();
        self.mutex.lockUncancelable(runtime);
        defer self.mutex.unlock(runtime);
        const owner = self.store orelse return null;
        if (self.callbacks == 0) self.callbacks_done.reset();
        self.callbacks += 1;
        return owner;
    }

    pub fn endCallback(self: *Registry) void {
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
            if (!state.shared and state.idle_expiry and state.active_uses == 0) state.closeResources();
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
        self.releaseClosedClaimAssumeLocked(state);
    }

    fn releaseClosedClaimAssumeLocked(self: *Registry, state: *State) void {
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

    pub fn beginConstruction(self: *Registry) !Construction {
        const owner = self.beginCallback() orelse return error.ReadOnly;
        defer self.endCallback();
        if (owner.read_only) return error.ReadOnly;
        if (!self.marker_namespace_safe) return error.UnsafeNativeReaderAlias;
        const runtime = self.io();
        const a = self.allocator;
        owner.generation_lock.lockSharedUncancelable(owner.file.runtime());
        defer owner.generation_lock.unlockShared(owner.file.runtime());
        const inode = (try owner.file.file.stat(owner.file.runtime())).inode;
        var random: [16]u8 = undefined;
        try runtime.randomSecure(&random);
        const id = std.fmt.bytesToHex(random, .lower);
        const key = try std.mem.concat(a, u8, &.{ key_prefix, &id });
        errdefer a.free(key);
        const marker_name = try std.mem.concat(a, u8, &.{ marker_prefix, &id });
        defer a.free(marker_name);
        const path = try std.fs.path.join(a, &.{ std.fs.path.dirname(self.path) orelse ".", marker_name });
        errdefer a.free(path);
        const marker = try self.createMarker(path);
        errdefer {
            marker.close(runtime);
            std.Io.Dir.cwd().deleteFile(runtime, path) catch {};
        }
        const ticket = if (self.coordinator) |coordinator| try coordinator.reserveArtifactRelease(inode, key) else null;
        _ = self.references.fetchAdd(1, .monotonic);
        return .{ .registry = self, .key = key, .marker_path = path, .marker = marker, .inode = inode, .release_ticket = ticket };
    }

    fn findPooled(self: *Registry, owner: *docs.Store, key: []const u8) !?*State {
        if (!self.marker_namespace_safe or !owner.artifactPoolLookupAllowed()) return null;
        // The pin lasts only for lookup. A hit already owns a durable value
        // lease. Do not recursively acquire generation_lock around beginRead.
        var pin = try owner.beginRead();
        defer pin.abort();
        const file = try pin.readFile();
        var value = try file.openIndexValue(self.allocator, key, pin.checkpoint);
        defer value.deinit(self.allocator);
        const identity: ArtifactIdentity = .{ .path = self.path, .key = key, .inode = (try file.file.stat(file.runtime())).inode, .root = value.root, .length = value.length, .inline_bytes = value.inline_bytes };
        const state = retainPooled(identity) orelse return null;
        // The returned source reference keeps the pooled identity alive while
        // this optional cache bookkeeping runs, without holding the pool lock.
        self.rememberBorrowed(identity) catch {};
        return state;
    }

    fn rememberBorrowed(self: *Registry, identity: ArtifactIdentity) !void {
        const runtime = self.io();
        self.mutex.lockUncancelable(runtime);
        defer self.mutex.unlock(runtime);
        for (self.borrowed[0..self.borrowed_count]) |known|
            if (identity.eql(known, identity)) return;
        const key = try self.allocator.dupe(u8, identity.key);
        errdefer self.allocator.free(key);
        const inline_bytes = try self.allocator.dupe(u8, identity.inline_bytes);
        const slot = if (self.borrowed_count < self.borrowed.len) self.borrowed_count else self.borrowed_next;
        if (slot < self.borrowed_count) {
            self.allocator.free(self.borrowed[slot].key);
            self.allocator.free(self.borrowed[slot].inline_bytes);
        } else self.borrowed_count += 1;
        self.borrowed[slot] = identity;
        self.borrowed[slot].path = self.path;
        self.borrowed[slot].key = key;
        self.borrowed[slot].inline_bytes = inline_bytes;
        self.borrowed_next = (slot + 1) % self.borrowed.len;
    }

    pub fn open(self: *Registry, key: []const u8) !Source {
        const owner = self.beginCallback() orelse return error.ReadOnly;
        defer self.endCallback();
        // Capturing a root and opening its inode must straddle no adoption.
        const owner_io = owner.file.runtime();
        const runtime = self.io();
        const a = self.allocator;
        if (hasPooledSources()) {
            // An optional lookup must not refuse a source which the normal
            // publication path can open (including newly staged artifacts).
            if (self.findPooled(owner, key) catch null) |existing| {
                owner.recordArtifactSourceOpen(true);
                return existing.source();
            }
        }
        owner.generation_lock.lockSharedUncancelable(owner_io);
        defer owner.generation_lock.unlockShared(owner_io);
        const state = try a.create(State);
        errdefer a.destroy(state);
        const artifact_key = try a.dupe(u8, key);
        errdefer a.free(artifact_key);
        var random: [16]u8 = undefined;
        try runtime.randomSecure(&random);
        const lease_identity = try std.fmt.allocPrint(a, "{x}", .{random});
        defer a.free(lease_identity);
        const lease_key = try std.mem.concat(a, u8, &.{ key_prefix, lease_identity });
        errdefer a.free(lease_key);
        const marker_name = try std.mem.concat(a, u8, &.{ marker_prefix, lease_identity });
        defer a.free(marker_name);
        const marker_path = try std.fs.path.join(a, &.{ std.fs.path.dirname(self.path) orelse ".", marker_name });
        errdefer a.free(marker_path);
        const reader_fenced = owner.read_only or owner.artifact_reader_fence or !self.marker_namespace_safe;
        const marker: ?std.Io.File = if (reader_fenced) null else try self.createMarker(marker_path);
        errdefer if (marker) |file| {
            file.close(runtime);
            std.Io.Dir.cwd().deleteFile(runtime, marker_path) catch {};
        };
        // Writable roots are protected by the catalog reference. Read-only
        // sources use the established external-reader lock, since they cannot
        // publish a lease and external writers must not reuse their pages.
        var reader = try native.NativeFile.openWithIo(a, runtime, self.path, .{ .read_only = true, .internal_reader = !reader_fenced, .reader_coordinator = owner.file.reader_coordinator, .no_sync = true });
        errdefer reader.close();
        if (!reader_fenced) reader.reader_coordinator = null;
        reader.page_cache_enabled.store(false, .monotonic);
        var capture = Capture{ .allocator = a, .key = key, .lease_key = lease_key };
        errdefer capture.value.deinit(a);
        if (reader_fenced) {
            capture.checkpoint = reader.activeCheckpoint();
            capture.value = try reader.openIndexValue(a, key, capture.checkpoint);
        } else try owner.submitArtifactLeaseMutation(&capture, Capture.apply);
        const captured_inode = (try reader.file.stat(reader.runtime())).inode;
        const release_ticket = if (!reader_fenced and self.coordinator != null)
            try self.coordinator.?.reserveArtifactRelease(captured_inode, lease_key)
        else
            null;
        errdefer if (release_ticket) |ticket| self.coordinator.?.destroyArtifactReleases(ticket);
        state.* = .{
            .registry = self,
            .reader = reader,
            .value = capture.value,
            .checkpoint = capture.checkpoint,
            .generation = owner.assessment_generation,
            .lease_key = lease_key,
            .release_ticket = release_ticket,
            .marker_path = marker_path,
            .marker = marker,
            .resource_manager = owner.resource_manager,
            .artifact_key = artifact_key,
        };
        try initializeSourceCache(self, state, captured_inode);
        return self.registerSource(state, owner, captured_inode, false);
    }

    // Once the source is fully prepared, registration cannot fail. A sealed
    // construction can transfer its existing registry reference and lease.
    fn registerSource(self: *Registry, state: *State, owner: *docs.Store, captured_inode: u64, transfer_reference: bool) Source {
        const runtime = self.io();
        self.mutex.lockUncancelable(runtime);
        state.next = self.first;
        self.first = state;
        if (!transfer_reference) _ = self.references.fetchAdd(1, .monotonic);
        self.mutex.unlock(runtime);
        if (state.marker != null and owner.artifactPoolLookupAllowed()) {
            const identity: ArtifactIdentity = .{ .path = self.path, .key = state.artifact_key, .inode = captured_inode, .root = state.value.root, .length = state.value.length, .inline_bytes = state.value.inline_bytes };
            @import("antfly_platform").sync.lockYielding(&source_pool_mutex);
            // A closed old source can remain referenced by a weak LSM cache.
            // Replace its map key too, before its final close frees the bytes.
            removePooled(identity);
            source_pool.putContext(std.heap.page_allocator, identity, state, identity) catch {
                // Pooling is optional; the newly opened lease remains valid.
                source_pool_mutex.unlock();
                owner.recordArtifactSourceOpen(false);
                return state.source();
            };
            state.pool_identity = identity;
            source_pool_mutex.unlock();
        }
        owner.recordArtifactSourceOpen(false);
        return state.source();
    }
};

/// A bounded unpublished native value. Its locked marker, release hint, and
/// registry reference outlive Store refresh. Appends use the current admitted
/// Store and are idempotent after an ambiguous successful publication.
pub const Construction = struct {
    registry: ?*Registry,
    key: []u8,
    marker_path: []u8,
    marker: std.Io.File,
    inode: u64,
    release_ticket: ?*native.ReaderCoordinator.ArtifactRelease,
    length: usize = 0,
    sealed: bool = false,

    pub fn append(self: *Construction, owner: *docs.Store, bytes: []const u8) !void {
        if (self.registry == null or self.sealed) return error.InvalidArtifactConstructionState;
        const Request = struct {
            construction: *Construction,
            bytes: []const u8,
            fn apply(raw: *anyopaque, file: *native.NativeFile) !void {
                const request: *@This() = @ptrCast(@alignCast(raw));
                const building = request.construction;
                if ((try file.file.stat(file.runtime())).inode != building.inode) return error.StaleArtifactConstruction;
                try file.appendArtifactConstruction(building.key, building.length, request.bytes);
            }
        };
        var request: Request = .{ .construction = self, .bytes = bytes };
        try owner.submitArtifactLeaseMutation(&request, Request.apply);
        self.length += bytes.len;
    }

    pub fn seal(self: *Construction, expected_length: usize) !void {
        if (self.registry == null or expected_length == 0 or self.length != expected_length) return error.InvalidArtifactConstructionState;
        self.sealed = true;
    }

    pub fn link(self: *Construction, owner: *docs.Store, path: []const u8) !void {
        if (self.registry == null or !self.sealed) return error.InvalidArtifactConstructionState;
        const Request = struct {
            construction: *const Construction,
            path: []const u8,
            fn apply(raw: *anyopaque, file: *native.NativeFile) !void {
                const request: *@This() = @ptrCast(@alignCast(raw));
                const building = request.construction;
                if ((try file.file.stat(file.runtime())).inode != building.inode) return error.StaleArtifactConstruction;
                try file.linkArtifactConstruction(building.key, request.path, building.length);
            }
        };
        var request: Request = .{ .construction = self, .path = path };
        try owner.submitIndexMutation(&request, Request.apply, true);
    }

    /// Prepare the immutable reader before linking, then transfer this
    /// construction's durable value lease into that reader. A second lease
    /// publication would protect the very same root and require another commit.
    /// No fallible operation follows link success, so cleanup never removes a
    /// path whose creation failed or whose publication outcome is uncertain.
    pub fn linkSource(self: *Construction, owner: *docs.Store, path: []const u8) !Source {
        const registry = self.registry orelse return error.InvalidArtifactConstructionState;
        if (!self.sealed) return error.InvalidArtifactConstructionState;
        const captured_owner = registry.beginCallback() orelse return error.EnrichmentWaitCanceled;
        defer registry.endCallback();
        if (captured_owner != owner) return error.StaleArtifactConstruction;
        const runtime = registry.io();
        owner.generation_lock.lockSharedUncancelable(owner.file.runtime());
        defer owner.generation_lock.unlockShared(owner.file.runtime());
        const a = registry.allocator;
        const state = try a.create(State);
        errdefer a.destroy(state);
        const artifact_key = try a.dupe(u8, path);
        errdefer a.free(artifact_key);
        var reader = try native.NativeFile.openWithIo(a, runtime, registry.path, .{ .read_only = true, .internal_reader = true, .reader_coordinator = owner.file.reader_coordinator, .no_sync = true });
        errdefer reader.close();
        reader.reader_coordinator = null;
        reader.page_cache_enabled.store(false, .monotonic);
        const captured_inode = (try reader.file.stat(reader.runtime())).inode;
        if (captured_inode != self.inode) return error.StaleArtifactConstruction;
        const checkpoint = reader.activeCheckpoint();
        var value = try reader.openArtifactLeaseValue(a, self.key, checkpoint);
        errdefer value.deinit(a);
        if (value.length != self.length) return error.StaleArtifactConstruction;
        state.* = .{
            .registry = registry,
            .reader = reader,
            .value = value,
            .checkpoint = checkpoint,
            .generation = owner.assessment_generation,
            .lease_key = self.key,
            .release_ticket = self.release_ticket,
            .marker_path = self.marker_path,
            .marker = self.marker,
            .resource_manager = owner.resource_manager,
            .artifact_key = artifact_key,
        };
        try initializeSourceCache(registry, state, captured_inode);
        errdefer state.cache.?.deinit();
        try self.link(owner, path);
        self.registry = null;
        self.release_ticket = null;
        return registry.registerSource(state, owner, captured_inode, true);
    }

    pub fn deinit(self: *Construction) void {
        const registry = self.registry orelse return;
        self.registry = null;
        self.marker.close(registry.io());
        std.Io.Dir.cwd().deleteFile(registry.io(), self.marker_path) catch {};
        if (self.release_ticket) |ticket| {
            registry.coordinator.?.enqueueArtifactReleases(ticket);
            self.release_ticket = null;
        }
        registry.allocator.free(self.key);
        registry.allocator.free(self.marker_path);
        registry.release();
    }
};

test "lite artifact working set recaptures pooled sources through a new registry" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/recapture.aflite", .{tmp.sub_path});
    defer a.free(path);
    var first = try docs.Store.createWithOptions(a, path, .{ .io = std.testing.io, .no_sync = true });
    first.maintenance_start_suppressed = true;
    var first_live = true;
    defer if (first_live) first.close();
    try first.file.putIndexCatalogRecord("/index/kept", "immutable payload");
    var original = try first.openArtifactSource("/index/kept");
    defer original.close();
    const inode = (try first.file.file.stat(first.file.runtime())).inode;
    var working: WorkingSet = .{};
    defer working.clear();
    working.capture(first.artifact_registry.?, inode, 1024 * 1024, 1);
    try std.testing.expectEqual(@as(usize, 1), working.count);
    first.close();
    first_live = false;
    var second = try docs.Store.openWithOptions(a, path, .{ .io = std.testing.io, .no_sync = true });
    second.maintenance_start_suppressed = true;
    defer second.close();
    var pooled = try second.openArtifactSource("/index/kept");
    defer pooled.close();
    try std.testing.expect(pooled.acquireUse());
    defer pooled.releaseUse();
    try std.testing.expectEqual(@as(usize, 1), second.artifact_registry.?.borrowed_count);
    try std.testing.expect(second.artifact_registry.?.first == null);
    const expiry = working.expires_ns;
    working.capture(second.artifact_registry.?, inode, 1024 * 1024, expiry);
    try std.testing.expectEqual(@as(usize, 1), working.count);
    try std.testing.expect(working.expires_ns > expiry);
    var bytes: [17]u8 = undefined;
    try pooled.readInto(0, &bytes);
    try std.testing.expectEqualStrings("immutable payload", &bytes);
    working.observe(inode + 1, expiry + 1);
    try std.testing.expectEqual(@as(usize, 0), working.count);
}

test "lite artifact construction transfers its lease without another publication" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/construction-transfer.aflite", .{tmp.sub_path});
    defer a.free(path);
    var store = try docs.Store.createWithOptions(a, path, .{ .io = std.testing.io, .no_sync = true });
    store.maintenance_start_suppressed = true;
    var store_live = true;
    defer if (store_live) store.close();
    var construction = try store.beginArtifactConstruction();
    defer construction.deinit();
    const lease_key = try a.dupe(u8, construction.key);
    defer a.free(lease_key);
    const payload: [65536]u8 = @splat('p');
    try construction.append(&store, &payload);
    try construction.seal(payload.len);
    try store.file.putIndexCatalogRecord("/taken", "foreign");
    const before = store.file.activeCheckpoint();
    try std.testing.expectError(error.PathAlreadyExists, construction.linkSource(&store, "/taken"));
    try std.testing.expect(construction.registry != null);
    try std.testing.expectEqual(before.commit_sequence, store.file.activeCheckpoint().commit_sequence);
    store.file.test_page_write_fail_after = 0;
    const failed = construction.linkSource(&store, "/activated");
    store.file.test_page_write_fail_after = null;
    try std.testing.expectError(error.TestPageWriteFailure, failed);
    try std.testing.expect(construction.registry != null);
    var source = try construction.linkSource(&store, "/activated");
    var source_live = true;
    defer if (source_live) source.close();
    try std.testing.expect(construction.registry == null);
    try std.testing.expectEqual(before.commit_sequence + 1, store.file.activeCheckpoint().commit_sequence);
    try std.testing.expectEqualSlices(u8, &before.logical.metadata, &store.file.activeCheckpoint().logical.metadata);
    // The prepared wrapper can close immediately: the source now owns its
    // existing marker, value lease, release ticket and registry reference.
    construction.deinit();
    store.close();
    store_live = false;
    store = try docs.Store.openWithOptions(a, path, .{ .io = std.testing.io, .no_sync = true });
    store.maintenance_start_suppressed = true;
    store_live = true;
    var sweep: SweepCursor = .{};
    try std.testing.expectEqual(@as(usize, 0), try cleanupOrphans(&store.file, &sweep));
    try store.file.deleteIndexCatalogRecord("/activated");
    _ = try store.file.reclaimPages(4096);
    var got: [65536]u8 = undefined;
    try source.readInto(0, &got);
    try std.testing.expectEqualSlices(u8, &payload, &got);
    const foreign = (try store.file.getIndexCatalogRecordAlloc(a, "/taken")).?;
    defer a.free(foreign);
    try std.testing.expectEqualStrings("foreign", foreign);
    source.close();
    source_live = false;
    sweep = .{};
    _ = try cleanupOrphans(&store.file, &sweep);
    try std.testing.expect((try store.file.getCatalogRecordAlloc(a, lease_key)) == null);
    try std.testing.expect((try store.file.check()).valid);
}

test "lite artifact construction survives Store refresh and releases orphan ownership after close" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/construction.aflite", .{tmp.sub_path});
    defer a.free(path);
    var store = try docs.Store.createWithOptions(a, path, .{ .io = std.testing.io, .no_sync = true });
    // This fixture services orphan records explicitly through the NativeFile.
    store.maintenance_start_suppressed = true;
    var store_live = true;
    defer if (store_live) store.close();
    var construction = try store.beginArtifactConstruction();
    defer construction.deinit();
    const lease_key = try a.dupe(u8, construction.key);
    defer a.free(lease_key);
    const first: [131072]u8 = @splat('a');
    const second: [65536]u8 = @splat('b');
    try construction.append(&store, &first);
    store.close();
    store_live = false;
    store = try docs.Store.openWithOptions(a, path, .{ .io = std.testing.io, .no_sync = true });
    store.maintenance_start_suppressed = true;
    store_live = true;
    var sweep: SweepCursor = .{};
    try std.testing.expectEqual(@as(usize, 0), try cleanupOrphans(&store.file, &sweep));
    try construction.append(&store, &second);
    try construction.seal(first.len + second.len);
    try std.testing.expectError(error.InvalidArtifactConstructionState, construction.append(&store, "late"));
    try construction.link(&store, "/index/activated");
    var source = try store.openArtifactSource("/index/activated");
    var source_live = true;
    defer if (source_live) source.close();
    store.close();
    store_live = false;
    // Closing construction after its original Store is gone only closes its
    // marker and queues a preallocated hint; it must never call that Store.
    construction.deinit();
    store = try docs.Store.openWithOptions(a, path, .{ .io = std.testing.io, .no_sync = true });
    store.maintenance_start_suppressed = true;
    store_live = true;
    sweep = .{};
    _ = try cleanupOrphans(&store.file, &sweep);
    try std.testing.expect((try store.file.getCatalogRecordAlloc(a, lease_key)) == null);
    try store.file.deleteIndexCatalogRecord("/index/activated");
    var got: [8]u8 = undefined;
    try source.readInto(first.len - 4, &got);
    try std.testing.expectEqualStrings("aaaabbbb", &got);
    try std.testing.expect((try store.file.check()).valid);
    source.close();
    source_live = false;
    sweep = .{};
    _ = try cleanupOrphans(&store.file, &sweep);
    try std.testing.expect((try store.file.check()).valid);
}

test "lite artifact construction rejects publication on a replacement inode" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/construction-replace.aflite", .{tmp.sub_path});
    defer a.free(path);
    var store = try docs.Store.createWithOptions(a, path, .{ .io = std.testing.io, .no_sync = true });
    store.maintenance_start_suppressed = true;
    defer store.close();
    var construction = try store.beginArtifactConstruction();
    defer construction.deinit();
    try construction.append(&store, "staged");
    _ = try store.vacuum();
    try std.testing.expectError(error.StaleArtifactConstruction, construction.append(&store, "refused"));
    try construction.seal("staged".len);
    try std.testing.expectError(error.StaleArtifactConstruction, construction.link(&store, "/index/never"));
    try std.testing.expect((try store.file.getIndexCatalogRecordAlloc(a, "/index/never")) == null);
    try std.testing.expect((try store.file.check()).valid);
}

fn initializeSourceCache(registry: *Registry, state: *State, captured_inode: u64) !void {
    var identity_digest = std.crypto.hash.sha2.Sha256.init(.{});
    identity_digest.update("antfly native immutable artifact v1");
    identity_digest.update(std.mem.asBytes(&captured_inode));
    identity_digest.update(std.mem.asBytes(&state.value.root));
    identity_digest.update(std.mem.asBytes(&state.value.length));
    identity_digest.update(state.value.inline_bytes);
    state.immutable_identity = identity_digest.finalResult();
    state.cache = try @import("../../segment_source.zig").ConcurrentBlockCache.init(registry.allocator, .{ .ranges = .{ .ptr = state, .length = state.value.length, .read_into = State.readUncached, .checksum = State.checksum, .visit_range = State.visitUncached, .close = State.closeUncached, .resource_manager = state.resource_manager } }, source_cache_bytes);
}

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

const State = struct {
    immutable_identity: [32]u8 = undefined,
    registry: *Registry,
    artifact_key: []u8,
    reader: native.NativeFile,
    value: native.NativeFile.IndexValue,
    checkpoint: native.CheckpointSlot,
    generation: u64,
    lease_key: []u8,
    release_ticket: ?*native.ReaderCoordinator.ArtifactRelease = null,
    marker_path: []u8,
    marker: ?std.Io.File,
    next: ?*State = null,
    retired_size: u64 = 0,
    retired_claim: u64 = 0,
    cache: ?@import("../../segment_source.zig").ConcurrentBlockCache = null,
    decoded_cache: ?*DecodedCache = null,
    lifetime_mutex: std.atomic.Mutex = .unlocked,
    active_uses: usize = 0,
    decoded_users: usize = 0, // lifetime_mutex; distinct from general source uses
    idle_expiry: bool = false,
    retired: bool = false,
    resources_closed: std.atomic.Value(bool) = .init(false),
    source_references: usize = 1, // source_pool_mutex
    shared: bool = false, // lifetime_mutex
    pool_identity: ?ArtifactIdentity = null,
    resource_manager: ?*@import("../resource_manager.zig").ResourceManager = null,

    fn source(self: *State) Source {
        return .{ .ranges = .{ .ptr = self, .length = self.value.length, .read_into = read, .visit_range = visit, .checksum = checksum, .read_authenticated = authenticate, .close = close, .retain_owned = retainOwned, .retained_bytes = retainedBytes, .retention_overhead_bytes = 2 * source_cache_bytes + decoded_cache_bytes + DecodedCache.metadata_overhead + 64 * 1024 + @sizeOf(State) + self.value.inline_bytes.len + self.artifact_key.len + self.lease_key.len + self.marker_path.len, .retention_artifact_only = self.marker != null, .resource_manager = self.resource_manager, .acquire_use = acquireUse, .release_use = releaseUse, .enable_idle_expiry = enableIdleExpiry, .immutable_identity = self.immutable_identity, .acquire_decoded = acquireDecoded, .lookup_decoded = lookupDecoded } };
    }

    fn retainOwned(ptr: *anyopaque) !Source {
        const self: *State = @ptrCast(@alignCast(ptr));
        @import("antfly_platform").sync.lockYielding(&source_pool_mutex);
        defer source_pool_mutex.unlock();
        @import("antfly_platform").sync.lockYielding(&self.lifetime_mutex);
        defer self.lifetime_mutex.unlock();
        if (self.resources_closed.load(.monotonic)) return error.EnrichmentWaitCanceled;
        self.source_references += 1;
        self.shared = true;
        return self.source();
    }

    fn closeResources(self: *State) void {
        if (self.resources_closed.load(.monotonic)) return;
        if (self.decoded_cache) |cache| cache.deinit();
        self.decoded_cache = null;
        self.cache.?.deinit();
        self.cache = null;
        self.reader.close();
        if (self.marker) |marker| {
            marker.close(self.registry.io());
            self.marker = null;
            std.Io.Dir.cwd().deleteFile(self.registry.io(), self.marker_path) catch {};
        }
        if (self.release_ticket) |ticket| {
            self.release_ticket = null;
            self.registry.coordinator.?.enqueueArtifactReleases(ticket);
        }
        self.resources_closed.store(true, .release);
    }

    // Registry -> lifetime is the only nested lock order. Release closes
    // resources first, then updates registry accounting after unlocking.
    fn retire(self: *State) void {
        @import("antfly_platform").sync.lockYielding(&self.lifetime_mutex);
        defer self.lifetime_mutex.unlock();
        self.retired = true;
        if (!self.shared and self.idle_expiry and self.active_uses == 0) self.closeResources();
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
        const expire = !self.shared and self.active_uses == 0 and self.idle_expiry and self.retired;
        if (expire) self.closeResources();
        self.lifetime_mutex.unlock();
        if (expire) self.registry.releaseClosedClaim(self);
    }

    fn enableIdleExpiry(ptr: *anyopaque) void {
        const self: *State = @ptrCast(@alignCast(ptr));
        @import("antfly_platform").sync.lockYielding(&self.lifetime_mutex);
        self.idle_expiry = true;
        const expire = !self.shared and self.active_uses == 0 and self.retired;
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
        return (if (self.cache) |*cache| cache.retainedBytes() else 0) +| (if (self.decoded_cache) |cache| cache.retainedBytes() else 0);
    }

    fn pinDecoded(self: *State) bool {
        @import("antfly_platform").sync.lockYielding(&source_pool_mutex);
        defer source_pool_mutex.unlock();
        @import("antfly_platform").sync.lockYielding(&self.lifetime_mutex);
        defer self.lifetime_mutex.unlock();
        if (self.resources_closed.load(.monotonic)) return false;
        self.active_uses += 1;
        self.decoded_users += 1;
        // A decoded lease may outlive its caller's Source. Its use prevents
        // idle expiry, and its reference keeps this provider/accounting owner.
        self.source_references += 1;
        return true;
    }

    fn releaseDecodedPin(self: *State) void {
        @import("antfly_platform").sync.lockYielding(&self.lifetime_mutex);
        std.debug.assert(self.decoded_users != 0);
        self.decoded_users -= 1;
        self.lifetime_mutex.unlock();
        releaseUse(self);
        close(self);
    }

    fn discardDeclinedDecode(self: *State) void {
        @import("antfly_platform").sync.lockYielding(&self.lifetime_mutex);
        defer self.lifetime_mutex.unlock();
        // This caller still owns its pin. All other decoders and returned
        // leases count separately, even when a handoff also uses this source.
        // Give the fallback its original host headroom, including metadata.
        if (self.decoded_users != 1) return;
        if (self.decoded_cache) |cache| cache.deinit();
        self.decoded_cache = null;
    }

    fn releaseDecoded(raw: *anyopaque, item: *anyopaque) void {
        const self: *State = @ptrCast(@alignCast(raw));
        (DecodedCache.Lease{ .cache = self.decoded_cache.?, .entry = @ptrCast(@alignCast(item)) }).release();
        self.releaseDecodedPin();
    }

    fn lookupDecoded(raw: *anyopaque, key: @import("../../segment_source.zig").DecodeKey) !?@import("../../segment_source.zig").Decoded {
        const self: *State = @ptrCast(@alignCast(raw));
        if (!self.pinDecoded()) return null;
        var transferred = false;
        defer if (!transferred) self.releaseDecodedPin();
        @import("antfly_platform").sync.lockYielding(&self.lifetime_mutex);
        const cache = self.decoded_cache;
        self.lifetime_mutex.unlock();
        const lease = (cache orelse return null).lookup(key) orelse return null;
        transferred = true;
        return .{ .value = lease.entry.value.?, .owner = self, .item = lease.entry, .release_fn = releaseDecoded, .retained_bytes = lease.entry.arena.queryCapacity() };
    }

    fn acquireDecoded(raw: *anyopaque, request: @import("../../segment_source.zig").DecodeRequest) !?@import("../../segment_source.zig").Decoded {
        const self: *State = @ptrCast(@alignCast(raw));
        if (!self.pinDecoded()) return null;
        var transferred = false;
        defer if (!transferred) self.releaseDecodedPin();
        @import("antfly_platform").sync.lockYielding(&self.lifetime_mutex);
        if (self.decoded_cache == null) self.decoded_cache = DecodedCache.create(self.registry.allocator, self.resource_manager, decoded_cache_bytes) catch null;
        const cache = self.decoded_cache;
        self.lifetime_mutex.unlock();
        const lease = (try (cache orelse return null).acquire(request)) orelse {
            self.discardDeclinedDecode();
            return null;
        };
        transferred = true;
        return .{ .value = lease.entry.value.?, .owner = self, .item = lease.entry, .release_fn = releaseDecoded, .retained_bytes = lease.entry.arena.queryCapacity() };
    }

    fn visit(ptr: *anyopaque, offset: u64, length: u64, visitor_port: @import("../../segment_source.zig").SourceVisitor) !void {
        var receiving_visitor = visitor_port;
        const context: *anyopaque = &receiving_visitor;
        const visitor = @import("../../segment_source.zig").SourceVisitor.consume;
        const self: *State = @ptrCast(@alignCast(ptr));
        return self.cache.?.visitRange(offset, length, context, visitor);
    }

    fn visitUncached(ptr: *anyopaque, offset: u64, length: u64, visitor_port: @import("../../segment_source.zig").SourceVisitor) !void {
        var receiving_visitor = visitor_port;
        const context: *anyopaque = &receiving_visitor;
        const visitor = @import("../../segment_source.zig").SourceVisitor.consume;
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
        @import("antfly_platform").sync.lockYielding(&source_pool_mutex);
        self.source_references -= 1;
        if (self.source_references != 0) {
            source_pool_mutex.unlock();
            return;
        }
        if (self.pool_identity) |identity| {
            if (source_pool.getContext(identity, identity) == self) removePooled(identity);
        }
        if (source_pool.count() == 0) {
            source_pool.deinit(std.heap.page_allocator);
            source_pool = .empty;
            source_pool_removals = 0;
        }
        source_pool_mutex.unlock();
        const registry = self.registry;
        const runtime = registry.io();
        @import("antfly_platform").sync.lockYielding(&self.lifetime_mutex);
        self.closeResources();
        self.lifetime_mutex.unlock();
        // Closing a runtime can already hold its owner's admission gate.
        // Never acquire a writer/proof lane here: dropping the locked marker
        // makes the alias eligible for the existing bounded orphan collector.
        if (registry.beginCallback()) |owner| {
            defer registry.endCallback();
            owner.maintenance_wake.set(owner.file.runtime());
        }
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
        registry.allocator.free(self.artifact_key);
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
    if (!try @import("reader_epochs.zig").namespaceMatches(a, runtime, file.path, file.header.reader_epoch_namespace)) return 0;
    const canonical = try std.Io.Dir.cwd().realPathFileAlloc(runtime, file.path, a);
    defer a.free(canonical);
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
        const path = try std.fs.path.join(a, &.{ std.fs.path.dirname(canonical) orelse ".", name });
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
    const owns_transaction = file.transaction_header == null;
    if (owns_transaction) try file.beginArtifactLeaseTransaction();
    errdefer if (owns_transaction) file.abortTransaction();
    try file.putCatalogBatch(mutations[0..count]);
    if (owns_transaction) try file.commitTransaction();
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
            const canonical = try std.Io.Dir.cwd().realPathFileAlloc(runtime, file.path, file.allocator);
            defer file.allocator.free(canonical);
            const dir = try std.Io.Dir.cwd().openDir(runtime, std.fs.path.dirname(canonical) orelse ".", .{ .iterate = true });
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
    const canonical = std.Io.Dir.cwd().realPathFileAlloc(io, root, allocator) catch |err| switch (err) {
        error.FileNotFound => missing: {
            const parent = try std.Io.Dir.cwd().realPathFileAlloc(io, std.fs.path.dirname(root) orelse ".", allocator);
            defer allocator.free(parent);
            break :missing try std.fs.path.joinZ(allocator, &.{ parent, std.fs.path.basename(root) });
        },
        else => return err,
    };
    defer allocator.free(canonical);
    const path = try std.fs.path.join(allocator, &.{ std.fs.path.dirname(canonical) orelse ".", ".aflite-lease-publication.lock" });
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

test "lite artifact path aliases retain snapshot protection across orphan cleanup" {
    if (@import("builtin").os.tag != .linux and @import("builtin").os.tag != .macos) return error.SkipZigTest;
    const a = std.testing.allocator;
    const io = std.testing.io;
    for ([_]bool{ false, true }) |symlink_alias| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/original.aflite", .{tmp.sub_path});
        defer a.free(path);
        const other = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/alias", .{tmp.sub_path});
        defer a.free(other);
        try std.Io.Dir.cwd().createDirPath(io, other);
        const alias = try std.fs.path.join(a, &.{ other, "reader.aflite" });
        defer a.free(alias);
        var writer = try docs.Store.createWithOptions(a, path, .{ .io = io, .no_sync = true });
        var writer_live = true;
        defer if (writer_live) writer.close();
        writer.maintenance_start_suppressed = true;
        const bytes: [8192]u8 = @splat('h');
        try writer.file.putIndexCatalogRecord("/index/kept", &bytes);
        if (symlink_alias) {
            const target = try std.Io.Dir.cwd().realPathFileAlloc(io, path, a);
            defer a.free(target);
            try std.Io.Dir.cwd().symLink(io, target, alias, .{});
        } else try std.Io.Dir.cwd().hardLink(path, std.Io.Dir.cwd(), alias, io, .{});
        writer.close();
        writer_live = false;
        var alias_store = try docs.Store.openWithOptions(a, alias, .{ .io = io, .no_sync = true });
        var alias_live = true;
        defer if (alias_live) alias_store.close();
        alias_store.maintenance_start_suppressed = true;
        var source = try alias_store.openArtifactSource("/index/kept");
        defer source.close();
        var adapter = @import("index_storage.zig").Store.init(a, &alias_store);
        if (try adapter.storage().acquireImmutableStager(a)) |value| {
            var stager = value;
            defer stager.deinit();
            try std.testing.expect(symlink_alias);
        } else try std.testing.expect(!symlink_alias);
        if (!symlink_alias) try std.testing.expectError(error.UnsafeNativeReaderAlias, alias_store.beginArtifactConstruction());
        alias_store.close();
        alias_live = false;
        writer = try docs.Store.openWithOptions(a, path, .{ .io = io, .no_sync = true });
        writer_live = true;
        writer.maintenance_start_suppressed = true;
        var sweep: SweepCursor = .{};
        // A locked live source through another path must never look orphaned.
        try std.testing.expectEqual(@as(usize, 0), try cleanupOrphans(&writer.file, &sweep));
        try writer.file.deleteIndexCatalogRecord("/index/kept");
        for (0..8) |i| {
            var key: [32]u8 = undefined;
            try writer.file.putIndexCatalogRecord(try std.fmt.bufPrint(&key, "/other/{d}", .{i}), &bytes);
        }
        var actual: [64]u8 = undefined;
        try source.readInto(4096, &actual);
        try std.testing.expectEqualSlices(u8, bytes[4096..][0..actual.len], &actual);
        try std.testing.expect((try writer.file.check()).valid);
    }
}
