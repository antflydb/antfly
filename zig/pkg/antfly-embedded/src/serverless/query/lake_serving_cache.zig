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

//! Server-owned version-keyed range cache. The mutex protects memory only;
//! provider I/O runs outside it. Bytes never live in a request allocator.
const std = @import("std");
const parquet = @import("lake_parquet_rowgroup.zig");
const ranges = @import("lake_range_io.zig");
const Context = @import("lake_read_context.zig").Context;
const ObjectReader = @import("lake_object_reader.zig").ObjectStorageRangeReader;
const Allocator = std.mem.Allocator;
pub const Cache = struct {
    alloc: Allocator,
    decoded: @import("lake_decoded_cache.zig").Cache,
    /// Manifest decoding has separate admission from snapshot construction.
    manifests: @import("lake_decoded_cache.zig").Cache,
    inventory_chunks: @import("lake_decoded_cache.zig").Cache,
    delete_files: @import("lake_decoded_cache.zig").Cache,
    inventory_catalogs: @import("lake_inventory_catalog.zig").Cache,
    /// In-flight physical reads share owned results even when residency is denied.
    physical: @import("lake_decoded_cache.zig").Cache,
    mutex: std.atomic.Mutex = .unlocked,
    entries: std.StringHashMapUnmanaged(*Entry) = .empty,
    flights: std.StringHashMapUnmanaged(*Flight) = .empty,
    mappings: std.StringHashMapUnmanaged(*Mapping) = .empty,
    mapped_bytes: usize = 0,
    max_mapped_bytes: usize = 8 * 1024 * 1024,
    max_mappings: usize = 128,
    max_bytes: usize = 64 * 1024 * 1024,
    max_entries: usize = 4096,
    stats: Stats = .{},
    tick: u64 = 0,
    persistent: ?parquet.PersistentObjectRangeCache = null,
    persistent_mutex: std.Io.Mutex = .init,
    persistent_retry_after_ns: i96 = 0,
    persistent_ready: std.atomic.Value(bool) = .init(false),
    persistent_startup: std.Io.Group = .init,
    persistent_startup_io: ?std.Io = null,

    const Mapping = struct {
        cache: *Cache,
        key: []u8,
        value: parquet.PersistentObjectRangeCache.MappedEntry,
        refs: usize = 1,
        touched: u64,
        fn release(self: *Mapping) void {
            while (!self.cache.mutex.tryLock()) std.atomic.spinLoopHint();
            defer self.cache.mutex.unlock();
            std.debug.assert(self.refs != 0);
            self.refs -= 1;
        }
        fn destroy(self: *Mapping) void {
            std.debug.assert(self.refs == 0);
            self.value.deinit();
            self.cache.alloc.free(self.key);
            self.cache.alloc.destroy(self);
        }
    };

    /// Publish one disk owner. The server owns the worker and drains it
    /// after cursors are quiescent; a request never owns cache I/O state.
    pub fn ensurePersistent(self: *Cache, io: std.Io, root: []const u8, policy: parquet.PersistentObjectRangeCachePolicy, resources: parquet.PersistentObjectRangeCacheResources) !void {
        return self.ensurePersistentAt(io, root, policy, resources, std.Io.Clock.now(.awake, io).nanoseconds);
    }

    // Retry optional startup without blocking concurrent RAM/source reads.
    fn ensurePersistentAt(self: *Cache, io: std.Io, root: []const u8, policy: parquet.PersistentObjectRangeCachePolicy, resources: parquet.PersistentObjectRangeCacheResources, now: i96) !void {
        try policy.validate();
        if (root.len == 0) return error.InvalidPersistentObjectRangeCachePolicy;
        if (self.persistent_ready.load(.acquire)) return;
        if (!self.persistent_mutex.tryLock()) return;
        defer self.persistent_mutex.unlock(io);
        if (self.persistent_ready.load(.acquire) or now < self.persistent_retry_after_ns) return;
        try self.initializePersistent(io, root, policy, resources, now);
    }

    /// Schedule inventory restoration once, without borrowing request memory or
    /// waiting for filesystem work. Until publication, reads use RAM/source.
    /// The caller's Io and resource manager must outlive this cache.
    pub fn startPersistent(self: *Cache, io: std.Io, root: []const u8, policy: parquet.PersistentObjectRangeCachePolicy, resources: parquet.PersistentObjectRangeCacheResources) !void {
        try policy.validate();
        if (root.len == 0) return error.InvalidPersistentObjectRangeCachePolicy;
        if (self.persistentReady() or !self.persistent_mutex.tryLock()) return;
        var handed_off = false;
        defer if (!handed_off) self.persistent_mutex.unlock(io);
        const now = std.Io.Clock.now(.awake, io).nanoseconds;
        if (self.persistentReady() or now < self.persistent_retry_after_ns) return;
        const owned = self.alloc.dupe(u8, root) catch |err| {
            self.recordDiskUnavailable(err);
            self.persistent_retry_after_ns = now +| 30 * std.time.ns_per_s;
            return;
        };
        defer if (!handed_off) self.alloc.free(owned);
        self.persistent_startup_io = io;
        self.persistent_startup.concurrent(io, restorePersistent, .{ self, io, owned, policy, resources }) catch |err| {
            self.recordDiskUnavailable(err);
            self.persistent_retry_after_ns = now +| 30 * std.time.ns_per_s;
            return;
        };
        handed_off = true;
    }

    fn restorePersistent(self: *Cache, io: std.Io, root: []u8, policy: parquet.PersistentObjectRangeCachePolicy, resources: parquet.PersistentObjectRangeCacheResources) std.Io.Cancelable!void {
        defer self.persistent_mutex.unlock(io);
        defer self.alloc.free(root);
        try self.initializePersistent(io, root, policy, resources, std.Io.Clock.now(.awake, io).nanoseconds);
    }

    /// Requires the startup mutex, which remains held by the background owner
    /// until publication or failure. No cache-memory lock covers inventory I/O.
    fn initializePersistent(self: *Cache, io: std.Io, root: []const u8, policy: parquet.PersistentObjectRangeCachePolicy, resources: parquet.PersistentObjectRangeCacheResources, now: i96) std.Io.Cancelable!void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        self.stats.disk_init_attempts +|= 1;
        self.mutex.unlock();
        var coordinated = resources;
        coordinated.reclaim_idle = .{ .ptr = self, .reclaim_one = reclaimIdleMapping };
        const disk = parquet.PersistentObjectRangeCache.initWithPolicyAndResources(io, root, policy, coordinated) catch |err| {
            if (err == error.Canceled) return error.Canceled;
            // Local cache availability is never source/readiness authority.
            // Retry transient ownership, worker, or filesystem failures without
            // repeating startup inventory I/O or warnings for every request.
            while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
            self.stats.disk_unavailable = @errorName(err);
            self.stats.disk_init_failures +|= 1;
            self.mutex.unlock();
            std.log.scoped(.lake_cache).warn("persistent cache unavailable; serving through RAM/source: {s}", .{@errorName(err)});
            self.persistent_retry_after_ns = now +| 30 * std.time.ns_per_s;
            return;
        };
        // Publish only after the complete disk owner is initialized.
        self.persistent = disk;
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        self.stats.disk_unavailable = null;
        self.mutex.unlock();
        self.persistent_ready.store(true, .release);
    }

    /// The owner is immutable after publication, until all server readers
    /// quiesce. Startup callers can skip path allocation and startup locking
    /// once ready; a failed initialization must still enter bounded recovery.
    pub fn persistentReady(self: *const Cache) bool {
        return self.persistent_ready.load(.acquire);
    }

    fn persistentCache(self: *Cache) ?*parquet.PersistentObjectRangeCache {
        if (!self.persistent_ready.load(.acquire)) return null;
        return &self.persistent.?;
    }

    pub fn recordDiskUnavailable(self: *Cache, err: anyerror) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        self.stats.disk_unavailable = @errorName(err);
    }

    pub fn persistentStats(self: *Cache) ?parquet.PersistentObjectRangeCacheStats {
        // Status must not wait for startup inventory I/O. Once published, the
        // disk owner is immutable until server readers/status calls quiesce.
        const disk = self.persistentCache() orelse return null;
        return disk.statsSnapshot();
    }

    const Flight = struct { key: []u8, event: std.Io.Event = .unset, refs: usize = 1 };
    const Claim = struct { flight: *Flight, leader: bool };
    fn begin(self: *Cache, key: []const u8) !?Claim {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        if (self.flights.get(key)) |flight| {
            flight.refs += 1;
            return .{ .flight = flight, .leader = false };
        }
        if (self.flights.count() >= 512) return null;
        const flight = try self.alloc.create(Flight);
        errdefer self.alloc.destroy(flight);
        const owned = try self.alloc.dupe(u8, key);
        errdefer self.alloc.free(owned);
        flight.* = .{ .key = owned };
        try self.flights.put(self.alloc, owned, flight);
        return .{ .flight = flight, .leader = true };
    }
    fn releaseFlight(self: *Cache, flight: *Flight) void {
        flight.refs -= 1;
        if (flight.refs == 0) {
            self.alloc.free(flight.key);
            self.alloc.destroy(flight);
        }
    }
    fn finish(self: *Cache, claim: Claim, io: std.Io) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        if (claim.leader) {
            _ = self.flights.remove(claim.flight.key);
            claim.flight.event.set(io);
        }
        self.releaseFlight(claim.flight);
    }
    /// Alias entries pin one immutable provider response. Charge the full
    /// envelope to each alias conservatively: a partially admitted envelope
    /// must never retain unaccounted bytes after its siblings are evicted.
    const Envelope = struct {
        bytes: []u8,
        refs: std.atomic.Value(usize) = .init(1),
        fn release(self: *Envelope) void {
            if (self.refs.fetchSub(1, .acq_rel) == 1) {
                std.heap.page_allocator.free(self.bytes);
                std.heap.page_allocator.destroy(self);
            }
        }
    };
    const Entry = struct {
        cache: *Cache,
        bytes: []u8,
        touched: u64,
        protected: bool = false,
        refs: usize = 0,
        envelope: ?*Envelope = null,
        fn charge(self: *const Entry) usize {
            return if (self.envelope) |owner| owner.bytes.len else self.bytes.len;
        }
        fn destroy(self: *Entry) void {
            if (self.envelope) |owner| owner.release() else self.cache.alloc.free(self.bytes);
            self.cache.alloc.destroy(self);
        }
    };
    pub const Stats = struct {
        hits: u64 = 0,
        misses: u64 = 0,
        stored_bytes: usize = 0,
        protected_stored_bytes: usize = 0,
        evictions: u64 = 0,
        disk_unavailable: ?[]const u8 = null,
        disk_init_attempts: u64 = 0,
        disk_init_failures: u64 = 0,
        disk_hits: u64 = 0,
        mapping_hits: u64 = 0,
        disk_bytes: u64 = 0,
        provider_reads: u64 = 0,
        provider_bytes: u64 = 0,
        completed_queries: u64 = 0,
        query_total_ns: u64 = 0,
        query_publication_ns: u64 = 0,
        query_search_ns: u64 = 0,
        /// Nested in search/delivery: includes residual and final hydration.
        query_hydration_ns: u64 = 0,
        query_delivery_ns: u64 = 0,
    };
    pub const QueryPhases = struct {
        total_ns: u64,
        publication_ns: u64,
        search_ns: u64,
        hydration_ns: u64,
        delivery_ns: u64,
    };
    /// Per-request durations, not global cache deltas (other queries can run
    /// concurrently). Status exports these counters without per-query logs.
    pub fn recordQuery(self: *Cache, phases: QueryPhases) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        self.stats.completed_queries +|= 1;
        self.stats.query_total_ns +|= phases.total_ns;
        self.stats.query_publication_ns +|= phases.publication_ns;
        self.stats.query_search_ns +|= phases.search_ns;
        self.stats.query_hydration_ns +|= phases.hydration_ns;
        self.stats.query_delivery_ns +|= phases.delivery_ns;
    }
    fn recordRead(self: *Cache, disk: bool, bytes: usize) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        if (disk) {
            self.stats.disk_hits +|= 1;
            self.stats.disk_bytes +|= bytes;
        } else {
            self.stats.provider_reads +|= 1;
            self.stats.provider_bytes +|= bytes;
        }
    }
    pub fn recordPhysicalRead(self: *Cache, bytes: usize) void {
        self.recordRead(false, bytes);
    }
    pub fn init(alloc: Allocator) Cache {
        return initWithMemoryLimit(alloc, 64 * 1024 * 1024);
    }
    pub fn initWithMemoryLimit(alloc: Allocator, maximum: usize) Cache {
        return .{ .alloc = alloc, .decoded = .{ .a = alloc }, .manifests = .{ .a = alloc }, .inventory_chunks = .{ .a = alloc }, .delete_files = .{ .a = alloc }, .inventory_catalogs = .{ .budget = .{ .backing = alloc, .limit = 128 * 1024 * 1024 } }, .physical = .{ .a = alloc, .max_bytes = 0, .max_entries = 0 }, .max_bytes = maximum };
    }
    pub fn deinit(self: *Cache) void {
        // Request/status callers are quiescent. Cancel and join restoration
        // before destroying its cache, callback owner or filesystem Io.
        if (self.persistent_startup_io) |io| self.persistent_startup.cancel(io);
        // Readers are quiescent. Join accepted writes before destroying the
        // mapping table borrowed by the worker's pressure callback.
        if (self.persistent) |*disk| disk.flush();
        var mappings = self.mappings.valueIterator();
        while (mappings.next()) |entry| entry.*.destroy();
        self.mappings.deinit(self.alloc);
        if (self.persistent) |*disk| disk.deinit();
        self.decoded.deinit();
        self.delete_files.deinit();
        self.inventory_catalogs.deinit();
        self.inventory_chunks.deinit();
        self.manifests.deinit();
        self.physical.deinit();
        std.debug.assert(self.flights.count() == 0);
        self.flights.deinit(self.alloc);
        var iter = self.entries.iterator();
        while (iter.next()) |entry| {
            self.alloc.free(entry.key_ptr.*);
            std.debug.assert(entry.value_ptr.*.refs == 0);
            entry.value_ptr.*.destroy();
        }
        self.entries.deinit(self.alloc);
        self.* = undefined;
    }
    pub fn snapshot(self: *Cache) Stats {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        return self.stats;
    }

    pub const ImmutableLoader = struct {
        ptr: *anyopaque,
        load: *const fn (*anyopaque, Allocator) anyerror![]u8,
        /// Coalesced loaders may fill several cache units with one provider
        /// request. Report physical traffic for this load, not logical fills.
        provider_read: ?*const fn (*anyopaque) ProviderRead = null,
    };
    pub const ProviderRead = struct { requests: u64, bytes: u64 };

    /// The caller proves current authorization and coverage before this call.
    /// Credential/store scope and authenticated identity partition both tiers;
    /// cached bytes never provide source authority. Admission remains optional.
    pub fn readImmutableAlloc(self: *Cache, a: Allocator, scope: [32]u8, identity: []const u8, length: usize, digest: [32]u8, context: Context, loader: ImmutableLoader) ![]u8 {
        try context.ensureActive();
        const key = try immutableKey(a, scope, identity, length, digest);
        defer a.free(key);
        return self.readImmutableKeyAlloc(a, key, length, digest, context, loader);
    }
    fn immutableKey(a: Allocator, scope: [32]u8, identity: []const u8, length: usize, digest: [32]u8) ![]u8 {
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("native-lake-immutable-cache-v1");
        hash.update(&scope);
        var encoded: [8]u8 = undefined;
        std.mem.writeInt(u64, &encoded, identity.len, .little);
        hash.update(&encoded);
        hash.update(identity);
        std.mem.writeInt(u64, &encoded, length, .little);
        hash.update(&encoded);
        hash.update(&digest);
        return std.fmt.allocPrint(a, "{s}:purpose=sidecar_payload", .{std.fmt.bytesToHex(hash.finalResult(), .lower)});
    }
    pub const ImmutableLease = union(enum) {
        mapping: *Mapping,
        shared: ranges.RangeLease,
        heap: struct { alloc: Allocator, bytes: []u8 },
        mapped: parquet.PersistentObjectRangeCache.MappedEntry,
        pub fn bytes(self: ImmutableLease) []const u8 {
            return switch (self) {
                .mapping => |value| value.value.bytes,
                .shared => |value| value.bytes,
                .heap => |value| value.bytes,
                .mapped => |value| value.bytes,
            };
        }
        pub fn deinit(self: *ImmutableLease) void {
            switch (self.*) {
                .mapping => |value| value.release(),
                .shared => |value| value.release(),
                .heap => |value| value.alloc.free(value.bytes),
                .mapped => |*value| value.deinit(),
            }
            self.* = undefined;
        }
    };
    /// Prefer a verified mapping for native immutable snapshots. Cold misses
    /// share existing singleflight/provider verification and asynchronously
    /// populate disk; callers can retain that bounded heap payload meanwhile.
    pub fn readImmutableLease(self: *Cache, a: Allocator, scope: [32]u8, identity: []const u8, length: usize, digest: [32]u8, context: Context, loader: ImmutableLoader) !ImmutableLease {
        try context.ensureActive();
        const key = try immutableKey(a, scope, identity, length, digest);
        defer a.free(key);
        if (self.persistentCache()) |disk| if (try disk.readMapped(a, key, length, digest, context)) |mapped| {
            self.recordRead(true, mapped.bytes.len);
            return .{ .mapped = mapped };
        };
        return .{ .heap = .{ .alloc = a, .bytes = try self.readImmutableKeyAlloc(a, key, length, digest, context, loader) } };
    }
    /// Small immutable ranges prefer a pinned RAM entry. No payload allocation
    /// or copy occurs on a hit; pinned entries remain charged and unevictable.
    /// Large contiguous segment callers retain readImmutableLease's disk-first
    /// policy so they can discard clean mapped pages under memory pressure.
    /// Lookup only: never fetch the provider. A persisted broad pack can serve
    /// sparse reads after restart through one verified, bounded disk mapping.
    pub fn pinImmutableBlock(self: *Cache, a: Allocator, scope: [32]u8, identity: []const u8, length: usize, digest: [32]u8, context: Context) !?ImmutableLease {
        try context.ensureActive();
        const key = try immutableKey(a, scope, identity, length, digest);
        defer a.free(key);
        if (self.pin(key)) |pinned| {
            errdefer pinned.release();
            try context.ensureActive();
            return .{ .shared = pinned };
        }
        if (self.pinMapping(key)) |mapping| {
            errdefer mapping.release();
            try context.ensureActive();
            return .{ .mapping = mapping };
        }
        if (self.persistentCache()) |disk| if (try disk.readMapped(a, key, length, digest, context)) |mapped| {
            self.recordRead(true, mapped.bytes.len);
            return self.admitMapping(key, mapped);
        };
        return null;
    }

    pub fn readImmutableBlockLease(self: *Cache, a: Allocator, scope: [32]u8, identity: []const u8, length: usize, digest: [32]u8, context: Context, loader: ImmutableLoader) !ImmutableLease {
        try context.ensureActive();
        const key = try immutableKey(a, scope, identity, length, digest);
        defer a.free(key);
        if (self.pin(key)) |pinned| {
            errdefer pinned.release();
            try context.ensureActive();
            return .{ .shared = pinned };
        }
        if (self.pinMapping(key)) |mapping| {
            errdefer mapping.release();
            try context.ensureActive();
            return .{ .mapping = mapping };
        }
        var lease = try self.readImmutableLease(a, scope, identity, length, digest, context, loader);
        errdefer lease.deinit();
        if (lease == .heap) if (self.pin(key)) |pinned| {
            errdefer pinned.release();
            try context.ensureActive();
            lease.deinit();
            return .{ .shared = pinned };
        };
        if (lease == .mapped) return self.admitMapping(key, lease.mapped);
        return lease;
    }
    /// Authentication proof for immutable bytes. Producers authenticate once,
    /// then pair the proof with an owner that keeps that exact slice alive.
    pub const VerifiedBytes = struct {
        bytes: []const u8,
        digest: [32]u8,
        pub fn authenticate(bytes: []const u8, digest: [32]u8) !@This() {
            var actual: [32]u8 = undefined;
            std.crypto.hash.sha2.Sha256.hash(bytes, &actual, .{});
            if (!std.mem.eql(u8, &actual, &digest)) return error.ArtifactIntegrityMismatch;
            return .{ .bytes = bytes, .digest = digest };
        }
    };
    pub const VerifiedLease = struct { value: VerifiedBytes, owner: ranges.RangeLease };
    /// Look up authenticated unit residency without starting a unit flight.
    /// Pack loaders coordinate misses with their own shared physical flight.
    pub fn lookupImmutableBlockLease(self: *Cache, a: Allocator, scope: [32]u8, identity: []const u8, length: usize, digest: [32]u8, context: Context) !?ImmutableLease {
        try context.ensureActive();
        const key = try immutableKey(a, scope, identity, length, digest);
        defer a.free(key);
        if (self.pin(key)) |value| {
            errdefer value.release();
            try context.ensureActive();
            return .{ .shared = value };
        }
        if (self.pinMapping(key)) |value| {
            errdefer value.release();
            try context.ensureActive();
            return .{ .mapping = value };
        }
        if (self.persistentCache()) |disk| if (try disk.readMapped(a, key, length, digest, context)) |value| {
            self.recordRead(true, length);
            var lease = self.admitMapping(key, value);
            errdefer lease.deinit();
            try context.ensureActive();
            return lease;
        };
        return null;
    }
    /// Consume a verified slice lease. Admission copies a unit once into its
    /// independently evictable RAM entry; denied admission returns the original
    /// shared physical slice, without an intermediate allocation or rehash.
    pub fn admitVerifiedBlock(self: *Cache, a: Allocator, scope: [32]u8, identity: []const u8, length: usize, digest: [32]u8, context: Context, verified: VerifiedLease) !ImmutableLease {
        var retained = true;
        defer if (retained) verified.owner.release();
        try context.ensureActive();
        if (verified.value.bytes.ptr != verified.owner.bytes.ptr or verified.value.bytes.len != verified.owner.bytes.len or verified.value.bytes.len != length or !std.mem.eql(u8, &verified.value.digest, &digest)) return error.ArtifactIntegrityMismatch;
        const key = try immutableKey(a, scope, identity, length, digest);
        defer a.free(key);
        if (self.persistentCache()) |disk| _ = disk.enqueueWrite(key, verified.value.bytes);
        self.store(key, verified.value.bytes) catch {};
        if (self.pin(key)) |value| {
            errdefer value.release();
            try context.ensureActive();
            return .{ .shared = value };
        }
        try context.ensureActive();
        retained = false;
        return .{ .shared = verified.owner };
    }
    /// Probe verified residency without joining unit flights or issuing provider I/O.
    /// Physical read planners must never wait on a unit flight while owning a
    /// physical flight: another unit leader may already be waiting on them.
    pub fn probeImmutableBlock(self: *Cache, a: Allocator, scope: [32]u8, identity: []const u8, length: usize, digest: [32]u8, context: Context) !bool {
        try context.ensureActive();
        const key = try immutableKey(a, scope, identity, length, digest);
        defer a.free(key);
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        const resident = self.entries.contains(key) or self.mappings.contains(key);
        self.mutex.unlock();
        if (resident) return true;
        if (self.persistentCache()) |disk| if (try disk.readMapped(a, key, length, digest, context)) |value| {
            var lease = self.admitMapping(key, value);
            lease.deinit();
            return true;
        };
        return false;
    }
    fn pinMapping(self: *Cache, key: []const u8) ?*Mapping {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        const mapping = self.mappings.get(key) orelse return null;
        mapping.refs += 1;
        self.tick +|= 1;
        mapping.touched = self.tick;
        self.stats.mapping_hits +|= 1;
        return mapping;
    }
    fn reclaimIdleMapping(raw: *anyopaque) bool {
        const self: *Cache = @ptrCast(@alignCast(raw));
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        var victim: ?*Mapping = null;
        var entries = self.mappings.valueIterator();
        while (entries.next()) |entry| {
            if (entry.*.refs != 0) continue;
            if (victim == null or entry.*.touched < victim.?.touched) victim = entry.*;
        }
        const retired = victim orelse {
            self.mutex.unlock();
            return false;
        };
        _ = self.mappings.remove(retired.key);
        self.mapped_bytes -= retired.value.mapping.len;
        self.mutex.unlock();
        // munmap and the disk owner's lease release may block under paging.
        // Detach first so unrelated readers can use the cache meanwhile.
        retired.destroy();
        return true;
    }
    // Only verified immutable cache inodes enter this owner. Replacement is
    // atomic; pins keep the original mapping and disk eviction lease alive.
    // Large contiguous artifacts keep their separate disk-first lifetime.
    fn admitMapping(self: *Cache, key: []const u8, value: parquet.PersistentObjectRangeCache.MappedEntry) ImmutableLease {
        var reclaimed: usize = 0;
        while (true) {
            while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
            var locked = true;
            defer if (locked) self.mutex.unlock();
            if (self.mappings.get(key)) |existing| {
                existing.refs += 1;
                self.mutex.unlock();
                locked = false;
                var redundant = value;
                redundant.deinit();
                return .{ .mapping = existing };
            }
            if (value.mapping.len > self.max_mapped_bytes or self.max_mappings == 0) return .{ .mapped = value };
            if (value.mapping.len > self.max_mapped_bytes -| self.mapped_bytes or self.mappings.count() >= self.max_mappings) {
                // Admission is optional. Bound retries when other readers keep
                // consuming capacity while cleanup runs outside the lock.
                if (reclaimed >= self.max_mappings) return .{ .mapped = value };
                var victim: ?*Mapping = null;
                var entries = self.mappings.valueIterator();
                while (entries.next()) |entry| {
                    if (entry.*.refs != 0) continue;
                    if (victim == null or entry.*.touched < victim.?.touched) victim = entry.*;
                }
                const retired = victim orelse return .{ .mapped = value };
                _ = self.mappings.remove(retired.key);
                self.mapped_bytes -= retired.value.mapping.len;
                self.mutex.unlock();
                locked = false;
                retired.destroy();
                reclaimed += 1;
                // Another reader may have populated this key or used the freed
                // capacity. Recheck identity and both limits after cleanup.
                continue;
            }
            const mapping = self.alloc.create(Mapping) catch return .{ .mapped = value };
            const owned = self.alloc.dupe(u8, key) catch {
                self.alloc.destroy(mapping);
                return .{ .mapped = value };
            };
            self.tick +|= 1;
            mapping.* = .{ .cache = self, .key = owned, .value = value, .touched = self.tick };
            self.mappings.put(self.alloc, owned, mapping) catch {
                self.alloc.free(owned);
                self.alloc.destroy(mapping);
                return .{ .mapped = value };
            };
            self.mapped_bytes += value.mapping.len;
            return .{ .mapping = mapping };
        }
    }

    fn readImmutableKeyAlloc(self: *Cache, a: Allocator, key: []const u8, length: usize, digest: [32]u8, context: Context, loader: ImmutableLoader) ![]u8 {
        if (try self.lookup(a, key)) |bytes| {
            errdefer a.free(bytes);
            try context.ensureActive();
            return bytes;
        }
        var claim: ?Claim = null;
        if (context.io) |io| {
            while (true) {
                claim = self.begin(key) catch null;
                if (claim == null or claim.?.leader) break;
                const waiting = claim.?;
                claim = null;
                {
                    defer self.finish(waiting, io);
                    while (!waiting.flight.event.isSet()) {
                        try context.ensureActive();
                        waiting.flight.event.waitTimeout(io, .{ .duration = .{ .clock = .awake, .raw = .fromMilliseconds(10) } }) catch |err| switch (err) {
                            error.Timeout => continue,
                            else => return err,
                        };
                    }
                }
                try context.ensureActive();
                if (try self.lookup(a, key)) |bytes| return bytes;
            }
        }
        defer if (claim) |active| self.finish(active, context.io.?);
        if (claim != null) if (try self.lookup(a, key)) |bytes| return bytes;
        const disk_bytes = if (self.persistentCache()) |disk| try disk.readAlloc(a, key, length) else null;
        if (disk_bytes) |bytes| {
            var actual: [32]u8 = undefined;
            std.crypto.hash.sha2.Sha256.hash(bytes, &actual, .{});
            if (bytes.len == length and std.mem.eql(u8, &actual, &digest)) {
                errdefer a.free(bytes);
                try context.ensureActive();
                self.recordRead(true, bytes.len);
                self.store(key, bytes) catch {};
                return bytes;
            }
            // An evictable cache entry is not authoritative. Retry the verified
            // immutable provider rather than failing from local disk damage.
            a.free(bytes);
        }
        const bytes = try loader.load(loader.ptr, a);
        errdefer a.free(bytes);
        try context.ensureActive();
        var actual: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(bytes, &actual, .{});
        if (bytes.len != length or !std.mem.eql(u8, &actual, &digest)) return error.ArtifactIntegrityMismatch;
        const traffic: ProviderRead = if (loader.provider_read) |read| read(loader.ptr) else .{ .requests = 1, .bytes = bytes.len };
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        self.stats.provider_reads +|= traffic.requests;
        self.stats.provider_bytes +|= traffic.bytes;
        self.mutex.unlock();
        if (self.persistentCache()) |disk| _ = disk.enqueueWrite(key, bytes);
        self.store(key, bytes) catch {};
        return bytes;
    }
    fn pin(self: *Cache, key: []const u8) ?ranges.RangeLease {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        if (self.entries.get(key)) |entry| {
            self.tick +%= 1;
            entry.touched = self.tick;
            entry.refs += 1;
            self.stats.hits += 1;
            return .{ .bytes = entry.bytes, .owner = .{ .shared = .{ .ptr = entry, .release_fn = releaseRange } } };
        }
        self.stats.misses += 1;
        return null;
    }
    fn releaseRange(raw: *anyopaque) void {
        const entry: *Entry = @ptrCast(@alignCast(raw));
        while (!entry.cache.mutex.tryLock()) std.atomic.spinLoopHint();
        defer entry.cache.mutex.unlock();
        std.debug.assert(entry.refs != 0);
        entry.refs -= 1;
    }
    fn lookup(self: *Cache, alloc: Allocator, key: []const u8) !?[]u8 {
        const lease = self.pin(key) orelse return null;
        defer lease.release();
        return try alloc.dupe(u8, lease.bytes);
    }
    fn store(self: *Cache, key: []const u8, bytes: []const u8) !void {
        return self.storeOwned(key, bytes, null);
    }
    fn storeOwned(self: *Cache, key: []const u8, bytes: []const u8, envelope: ?*Envelope) !void {
        const charge = if (envelope) |owner| owner.bytes.len else bytes.len;
        if (charge > self.max_bytes or self.max_entries == 0) return;
        const owned_key = try self.alloc.dupe(u8, key);
        defer self.alloc.free(owned_key);
        const owned_bytes = if (envelope != null) @constCast(bytes) else try self.alloc.dupe(u8, bytes);
        var admitted = false;
        defer if (!admitted and envelope == null) self.alloc.free(owned_bytes);
        const item = try self.alloc.create(Entry);
        defer if (!admitted) self.alloc.destroy(item);
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        // Concurrent misses may fetch the same immutable object range.
        if (self.entries.contains(key)) return;
        const lane = parquet.cacheLaneFromObjectRangeCacheKey(key);
        const protected = lane == .metadata or lane == .serving_sidecar;
        while (self.entries.count() != 0 and (self.stats.stored_bytes > self.max_bytes - charge or self.entries.count() >= self.max_entries)) {
            var oldest: ?[]const u8 = null;
            var touched: u64 = std.math.maxInt(u64);
            for (0..2) |pass| {
                var iter = self.entries.iterator();
                while (iter.next()) |entry| {
                    const candidate = entry.value_ptr.*;
                    if (candidate.refs != 0) continue;
                    if (pass == 0 and candidate.protected) continue;
                    if (candidate.protected and !protected and self.stats.protected_stored_bytes <= self.max_bytes / 4) continue;
                    if (oldest == null or candidate.touched < touched) {
                        oldest = entry.key_ptr.*;
                        touched = candidate.touched;
                    }
                }
                if (oldest != null) break;
            }
            const removed = self.entries.fetchRemove(oldest orelse return).?;
            self.stats.stored_bytes -= removed.value.charge();
            if (removed.value.protected) self.stats.protected_stored_bytes -= removed.value.charge();
            self.stats.evictions += 1;
            self.alloc.free(removed.key);
            removed.value.destroy();
        }
        const map_key = try self.alloc.dupe(u8, owned_key);
        errdefer self.alloc.free(map_key);
        self.tick +%= 1;
        item.* = .{ .cache = self, .bytes = owned_bytes, .touched = self.tick, .protected = protected, .envelope = envelope };
        try self.entries.put(self.alloc, map_key, item);
        if (envelope) |owner| _ = owner.refs.fetchAdd(1, .monotonic);
        admitted = true;
        self.stats.stored_bytes += charge;
        if (protected) self.stats.protected_stored_bytes += charge;
    }
};
pub const Reader = struct {
    const Scheduling = @import("../../sql/parallel_scheduler.zig");
    const Prefetch = struct {
        task: Scheduling.Task(anyerror!void),
        bytes: usize,
        key: [32]u8,
        work: *Warm,
    };
    const Warm = struct {
        reader: *Reader,
        read: ranges.RangeRead,
        arena: std.heap.ArenaAllocator,
        cancelled: std.atomic.Value(bool) = .init(false),
        page_end: ?u64 = null,
        page_limit: usize = 0,
        members: []ranges.RangeRead = &.{},
        claims: [8]?Cache.Claim = @splat(null),
        fn releaseClaims(self: *Warm) void {
            for (&self.claims) |*claim| if (claim.*) |held| {
                self.reader.cache.finish(held, self.reader.context.io.?);
                claim.* = null;
            };
        }
        fn destroy(self: *Warm) void {
            self.releaseClaims();
            self.arena.deinit();
            std.heap.page_allocator.destroy(self);
        }
        fn canceled(raw: *const anyopaque) bool {
            const self: *const Warm = @ptrCast(@alignCast(raw));
            if (self.cancelled.load(.acquire)) return true;
            self.reader.context.ensureActive() catch return true;
            return false;
        }
    };
    cache: *Cache,
    base: ObjectReader,
    scope: [32]u8,
    context: Context,
    scheduler: *Scheduling.Scheduler = Scheduling.global(),
    pending: [8]?Prefetch = @splat(null),
    prefetch_bytes: usize = 0,
    /// Rolling lookahead, at most eight ranges and 32 MiB per reader. Shared
    /// I/O admission further limits in-flight bytes under memory pressure.
    /// Workers use independent page allocations, never a SQL arena/quota.
    pub fn prefetch(self: *Reader, reads: []const ranges.RangeRead) !void {
        if (self.context.io == null or self.cache.max_bytes == 0) return;
        // Only explicitly planned cold ranges enter an envelope. Zero gap
        // prevents a pruned page or unprojected column from being overread.
        var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
        defer arena.deinit();
        const a = arena.allocator();
        var cold: [8]ranges.RangeRead = undefined;
        var count: usize = 0;
        for (reads) |read| {
            if (count == cold.len) break;
            const key = try self.rangeKey(a, read);
            if (self.cache.pin(key)) |lease| {
                lease.release();
                continue;
            }
            cold[count] = read;
            count += 1;
        }
        const envelopes = try ranges.coalescePhysicalReadsAlloc(a, cold[0..count], .{ .max_range_bytes = 2 * 1024 * 1024 });
        for (envelopes) |envelope| {
            var members: [8]ranges.RangeRead = undefined;
            var length: usize = 0;
            for (cold[0..count]) |read| if (ranges.sameObjectVersion(read.object, envelope.object) and read.purpose == envelope.purpose and std.mem.eql(u8, read.compression_codec, envelope.compression_codec) and read.range.offset >= envelope.range.offset and read.range.end() <= envelope.range.end()) {
                members[length] = read;
                length += 1;
            };
            // Header/footer discovery retains its original interpretation.
            const shared = length > 1 and envelope.purpose == .parquet_column_chunk;
            try self.prefetchImpl(&.{envelope}, null, 0, if (shared) members[0..length] else &.{});
        }
    }
    fn rangeKey(self: *Reader, a: Allocator, read: ranges.RangeRead) ![]u8 {
        const key = try read.cacheKeyAlloc(a);
        defer a.free(key);
        return std.fmt.allocPrint(a, "{s}:{s}", .{ std.fmt.bytesToHex(self.scope, .lower), key });
    }
    fn ownRead(a: Allocator, read: ranges.RangeRead) !ranges.RangeRead {
        var owned = read;
        owned.object.bucket = try a.dupe(u8, read.object.bucket);
        owned.object.key = try a.dupe(u8, read.object.key);
        owned.object.version.etag = try a.dupe(u8, read.object.version.etag);
        owned.object.version.version_id = try a.dupe(u8, read.object.version.version_id);
        owned.compression_codec = try a.dupe(u8, read.compression_codec);
        owned.decoded_column_id = try a.dupe(u8, read.decoded_column_id);
        return owned;
    }
    /// Header discovery belongs to I/O workers. Reserve the bounded possible
    /// payload before discovering its size, rather than blocking a scan pull.
    pub fn prefetchPage(self: *Reader, read: ranges.RangeRead, chunk_end: u64, limit: usize) !void {
        if (chunk_end <= read.range.offset or limit < read.range.len) return;
        return self.prefetchImpl(&.{read}, chunk_end, @intCast(@min(chunk_end - read.range.offset, @min(limit, 2 * 1024 * 1024))), &.{});
    }
    fn prefetchImpl(self: *Reader, reads: []const ranges.RangeRead, page_end: ?u64, page_limit: usize, members: []const ranges.RangeRead) !void {
        self.reapPrefetch();
        const io = self.context.io orelse return;
        try self.context.ensureActive();
        for (reads) |read| {
            const slot = for (self.pending, 0..) |pending, i| {
                if (pending == null) break i;
            } else break;
            const reserved = @max(read.range.len, page_limit);
            if (reserved > 32 * 1024 * 1024 -| self.prefetch_bytes) continue;
            const key = try self.objectKey(std.heap.page_allocator, read, "prefetch-v1");
            const duplicate = for (self.pending) |pending| {
                if (pending) |active| if (std.mem.eql(u8, &key, &active.key)) break true;
            } else false;
            if (duplicate) continue;
            const work = try std.heap.page_allocator.create(Warm);
            work.* = .{ .reader = self, .read = read, .arena = .init(std.heap.page_allocator) };
            work.page_end = page_end;
            work.page_limit = page_limit;
            var discarded = false;
            errdefer if (!discarded) work.destroy();
            const a = work.arena.allocator();
            work.read = try ownRead(a, read);
            work.members = try a.alloc(ranges.RangeRead, members.len);
            for (members, work.members, 0..) |member, *owned, i| {
                owned.* = try ownRead(a, member);
                const member_key = try self.rangeKey(a, member);
                if (try self.cache.begin(member_key)) |claim| {
                    if (claim.leader) work.claims[i] = claim else self.cache.finish(claim, io);
                }
            }
            const task = self.scheduler.submitIoTransient(io, @intCast(reserved *| 2), warm, .{work}) orelse {
                work.destroy();
                discarded = true;
                // Shared admission may fit individual pages even when their
                // envelope is too large. Preserve the useful smaller jobs.
                if (members.len != 0) try self.prefetchImpl(members, null, 0, &.{});
                continue;
            };
            self.pending[slot] = .{ .task = task, .bytes = @intCast(reserved), .key = key, .work = work };
            self.prefetch_bytes += @intCast(reserved);
        }
    }
    /// Completed workers have already released execution admission. Joining
    /// them here only reclaims descriptors; unrelated slow reads never stall
    /// a scan pull or the submission of another batch.
    pub fn reapPrefetch(self: *Reader) void {
        const io = self.context.io orelse return;
        for (&self.pending) |*pending| if (pending.*) |*active| {
            if (!active.task.isComplete()) continue;
            active.task.await(io) catch {};
            self.prefetch_bytes -= active.bytes;
            active.work.destroy();
            pending.* = null;
        };
    }
    fn warm(work: *Warm) anyerror!void {
        const self = work.reader;
        const read = work.read;
        defer work.releaseClaims();
        var worker: Reader = .{ .cache = self.cache, .base = self.base, .scope = self.scope, .context = self.context };
        const token: @import("../../storage/object_storage.zig").CancellationToken = .{ .ptr = work, .is_cancelled_fn = Warm.canceled };
        worker.context.cancellation = token;
        worker.base.cancellation = token;
        if (work.members.len != 0) {
            const owner = try std.heap.page_allocator.create(Cache.Envelope);
            const bytes = worker.base.parquetReader().readPlannedAlloc(std.heap.page_allocator, read) catch |err| {
                std.heap.page_allocator.destroy(owner);
                return err;
            };
            owner.* = .{ .bytes = bytes };
            defer owner.release();
            try worker.context.ensureActive();
            self.cache.recordRead(false, bytes.len);
            for (work.members) |member| {
                const key = try worker.rangeKey(work.arena.allocator(), member);
                const begin: usize = @intCast(member.range.offset - read.range.offset);
                const slice = bytes[begin..][0..@intCast(member.range.len)];
                if (self.cache.persistentCache()) |disk| _ = disk.enqueueWrite(key, slice);
                self.cache.storeOwned(key, slice, owner) catch {};
            }
            return;
        }
        if (work.page_end) |end| {
            var probe = read;
            const page = @import("lake_parquet_page.zig");
            while (true) {
                const lease = try readLease(&worker, std.heap.page_allocator, probe);
                defer lease.release();
                const parsed = page.parsePageHeader(lease.bytes) catch |err| {
                    const next = @min(end - probe.range.offset, @min(probe.range.len * 2, @min(work.page_limit, 64 * 1024)));
                    if (next <= probe.range.len) return err;
                    probe.range.len = next;
                    continue;
                };
                try parsed.header.validateResourceLimits();
                const length = std.math.add(usize, parsed.header_len, parsed.header.compressed_page_size) catch return error.ParquetPageTooLarge;
                if (length > end - probe.range.offset or length > work.page_limit) return;
                if (length <= lease.bytes.len) return;
                probe.range.len = length;
                const payload = try readLease(&worker, std.heap.page_allocator, probe);
                payload.release();
                return;
            }
        }
        if (read.purpose == .parquet_footer) {
            const lease = try worker.footerRead(read);
            defer lease.release();
        } else {
            const bytes = try readLease(&worker, std.heap.page_allocator, read);
            defer bytes.release();
        }
    }
    /// Prefetch failures are speculative. Required reads preserve their own
    /// errors; close cancels/joins before footer metadata or clients are freed.
    pub fn drain(self: *Reader, cancel: bool) void {
        self.drainExceptFooter(cancel, false);
    }
    /// Footer jobs own their immutable range descriptors and belong to the
    /// scan, so file teardown can preserve them without borrowing file metadata.
    pub fn clearFile(self: *Reader) void {
        self.drainExceptFooter(true, true);
    }
    fn drainExceptFooter(self: *Reader, cancel: bool, preserve_footer: bool) void {
        const io = self.context.io orelse return;
        for (&self.pending) |*future| if (future.*) |*active| {
            if (preserve_footer and active.work.read.purpose == .parquet_footer) continue;
            if (cancel) {
                active.work.cancelled.store(true, .release);
                active.task.cancel(io) catch {};
            } else {
                active.task.await(io) catch {};
            }
            self.prefetch_bytes -= active.bytes;
            active.work.destroy();
            future.* = null;
        };
    }
    pub fn reader(self: *Reader) parquet.ObjectRangeReader {
        return .{ .ctx = self, .read_range_alloc = readRange, .read_planned_range_alloc = readPlanned, .read_planned_range_lease = readLease };
    }
    pub fn objectKey(self: *Reader, a: Allocator, read: ranges.RangeRead, interpretation: []const u8) ![32]u8 {
        const key = try read.cacheKeyAlloc(a);
        defer a.free(key);
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update(&self.scope);
        hash.update(key);
        hash.update(interpretation);
        return hash.finalResult();
    }
    pub fn footer(self: *Reader, file: @import("../external_source/types.zig").FileEntry) !@import("lake_decoded_cache.zig").Lease {
        try self.context.ensureActive();
        const read = try ranges.planParquetFooterRead(try ranges.objectRefForExternalFileUri(file), 64 * 1024);
        return self.footerRead(read);
    }
    fn footerRead(self: *Reader, read: ranges.RangeRead) !@import("lake_decoded_cache.zig").Lease {
        try self.context.ensureActive();
        const key = try self.objectKey(std.heap.page_allocator, read, "parsed-footer-v1");
        var loader = struct {
            reader: *Reader,
            read: ranges.RangeRead,
            fn load(raw: *anyopaque, item: *@import("lake_decoded_cache.zig").Item) !void {
                const self_loader: *@This() = @ptrCast(@alignCast(raw));
                const decoded = try self_loader.reader.decodeFooter(item.arena.allocator(), self_loader.read);
                item.payload = .{ .footer = decoded };
            }
        }{ .reader = self, .read = read };
        return self.cache.decoded.acquire(key, 32 * 1024 * 1024, self.context, .{ .ptr = &loader, .load = @TypeOf(loader).load });
    }
    fn decodeFooter(self: *Reader, a: Allocator, read: ranges.RangeRead) !@import("lake_parquet_metadata.zig").ParsedFooter {
        const tail_lease = try self.reader().readPlannedLease(a, read);
        defer tail_lease.release();
        const tail = tail_lease.bytes;
        const footer_api = @import("lake_parquet_footer.zig");
        const preflight = try footer_api.parseFooterPreflight(read.object.byte_len, read.range.offset, tail);
        if (preflight.metadataSlice(tail)) |bytes|
            return @import("lake_parquet_metadata.zig").parseFooterMetadataAlloc(a, bytes, read.object.byte_len);
        const bytes_lease = try self.reader().readPlannedLease(a, try footer_api.planFooterMetadataRead(read.object, read.range.offset, tail));
        defer bytes_lease.release();
        return @import("lake_parquet_metadata.zig").parseFooterMetadataAlloc(a, bytes_lease.bytes, read.object.byte_len);
    }
    fn readRange(raw: *anyopaque, alloc: Allocator, bucket: []const u8, key: []const u8, offset: u64, len: usize) ![]u8 {
        const self: *Reader = @ptrCast(@alignCast(raw));
        try self.context.ensureActive();
        return self.base.parquetReader().readAlloc(alloc, bucket, key, offset, len);
    }
    fn readPlanned(raw: *anyopaque, alloc: Allocator, read: ranges.RangeRead) ![]u8 {
        const lease = try readLease(raw, alloc, read);
        switch (lease.owner) {
            .allocation => return @constCast(lease.bytes),
            .shared => {
                defer lease.release();
                return try alloc.dupe(u8, lease.bytes);
            },
        }
    }
    fn readLease(raw: *anyopaque, alloc: Allocator, read: ranges.RangeRead) !ranges.RangeLease {
        const self: *Reader = @ptrCast(@alignCast(raw));
        try self.context.ensureActive();
        try read.validate();
        // Planned reads require immutable version evidence before cache lookup.
        const range_key = try read.cacheKeyAlloc(alloc);
        defer alloc.free(range_key);
        const key = try std.fmt.allocPrint(alloc, "{s}:{s}", .{ std.fmt.bytesToHex(self.scope, .lower), range_key });
        defer alloc.free(key);
        if (self.cache.pin(key)) |bytes| {
            errdefer bytes.release();
            try self.context.ensureActive();
            return bytes;
        }
        var claim: ?Cache.Claim = null;
        if (self.context.io) |io| {
            while (true) {
                claim = self.cache.begin(key) catch null;
                if (claim == null or claim.?.leader) break;
                const waiting = claim.?;
                claim = null;
                {
                    defer self.cache.finish(waiting, io);
                    while (!waiting.flight.event.isSet()) {
                        try self.context.ensureActive();
                        waiting.flight.event.waitTimeout(io, .{ .duration = .{ .clock = .awake, .raw = .fromMilliseconds(10) } }) catch |err| switch (err) {
                            error.Timeout => continue,
                            else => return err,
                        };
                    }
                }
                try self.context.ensureActive();
                if (self.cache.pin(key)) |bytes| return bytes;
                // A failed/canceled speculative leader does not poison other
                // requests. Retry under this reader's own cancellation token.
            }
        }
        defer if (claim) |active| self.cache.finish(active, self.context.io.?);
        // Close the lookup/claim race without issuing a duplicate read.
        if (claim != null) if (self.cache.pin(key)) |bytes| return bytes;
        // The credential-scoped, versioned key is identical in both tiers.
        // Disk corruption/missing entries are misses inside the persistent
        // cache; an allocation failure still belongs to this request.
        const disk_bytes = if (self.cache.persistentCache()) |disk| try disk.readAlloc(alloc, key, read.range.len) else null;
        const bytes = disk_bytes orelse try self.base.parquetReader().readPlannedAlloc(alloc, read);
        self.cache.recordRead(disk_bytes != null, bytes.len);
        errdefer alloc.free(bytes);
        try self.context.ensureActive();
        // Cache admission is optional and never turns a successful read into
        // an allocation failure in a long-lived shared owner.
        if (disk_bytes == null) if (self.cache.persistentCache()) |disk| {
            _ = disk.enqueueWrite(key, bytes);
        };
        self.cache.store(key, bytes) catch {};
        if (self.cache.pin(key)) |lease| {
            alloc.free(bytes);
            return lease;
        }
        return .{ .bytes = bytes, .owner = .{ .allocation = alloc } };
    }
};
test "external lake shared cache bounds memory and segregates versions and credential scopes" {
    const alloc = std.testing.allocator;
    var cache = Cache.init(alloc);
    defer cache.deinit();
    cache.max_bytes = 6;
    try cache.store("scope-a:v1", "abc");
    const first = (try cache.lookup(alloc, "scope-a:v1")).?;
    defer alloc.free(first);
    try std.testing.expectEqualStrings("abc", first);
    try std.testing.expect((try cache.lookup(alloc, "scope-b:v1")) == null);
    try std.testing.expect((try cache.lookup(alloc, "scope-a:v2")) == null);
    try cache.store("scope-a:v2", "def");
    try cache.store("scope-b:v1", "ghi");
    const stats = cache.snapshot();
    try std.testing.expectEqual(@as(usize, 6), stats.stored_bytes);
    try std.testing.expectEqual(@as(u64, 1), stats.evictions);
}

test "external lake prefetch rolls across blocked batches warms versions and joins on close" {
    const storage = @import("../../storage/object_storage.zig");
    const a = std.testing.allocator;
    var memory = storage.MemoryObjectStorage.init(a);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("bucket");
    var put = try client.putObject("bucket", "data", "abcdefghijklmnopqrstuvwxyz012345", .{});
    defer put.deinit(a);
    const Slow = struct {
        base: storage.ObjectStorage,
        gate: std.atomic.Value(bool) = .init(false),
        canceled: std.atomic.Value(usize) = .init(0),
        entered: std.atomic.Value(usize) = .init(0),
        vtable: storage.ObjectStorage.VTable,
        fn get(raw: *anyopaque, alloc: Allocator, bucket: []const u8, key: []const u8, options: storage.GetOptions) !storage.GetResult {
            const self: *@This() = @ptrCast(@alignCast(raw));
            _ = self.entered.fetchAdd(1, .acq_rel);
            // Deliberately poll outside the worker's I/O cancellation system:
            // a provider may own a separate runtime. The composed token must
            // stop it when LIMIT closes the cursor without canceling request.
            while (!self.gate.load(.acquire)) {
                if (options.cancellation) |token| token.check() catch |err| {
                    _ = self.canceled.fetchAdd(1, .acq_rel);
                    return err;
                };
                std.atomic.spinLoopHint();
            }
            var base = self.base;
            base.allocator = alloc;
            return base.getObject(bucket, key, options);
        }
    };
    var slow: Slow = .{ .base = client, .vtable = client.vtable.* };
    slow.vtable.get_object = Slow.get;
    var cache = Cache.init(a);
    defer cache.deinit();
    var scheduler: Reader.Scheduling.Scheduler = .{ .max_io_workers = 8, .max_bytes = 128 };
    var reader: Reader = .{ .cache = &cache, .base = ObjectReader.init(.{ .allocator = a, .ptr = &slow, .vtable = &slow.vtable }), .scope = @splat(0), .context = .{ .io = std.testing.io }, .scheduler = &scheduler };
    defer reader.drain(true);
    const object: ranges.ObjectRef = .{ .bucket = "bucket", .key = "data", .byte_len = 32, .version = .{ .etag = put.etag.? } };
    var reads: [4]ranges.RangeRead = undefined;
    for (&reads, 0..) |*read, i| read.* = .{ .object = object, .range = .{ .offset = i * 4, .len = 4 }, .purpose = .parquet_column_chunk };
    try reader.prefetch(&reads);
    defer slow.gate.store(true, .release);
    for (0..200) |_| {
        if (slow.entered.load(.acquire) >= 1) break;
        try std.testing.io.sleep(.fromMilliseconds(10), .awake);
    }
    try std.testing.expect(slow.entered.load(.acquire) >= 1);
    var next_reads = reads;
    for (&next_reads) |*read| read.range.offset += 16;
    const Rolling = struct {
        fn run(target: *Reader, next: []const ranges.RangeRead, finished: *std.atomic.Value(bool)) !void {
            try target.prefetch(next);
            finished.store(true, .release);
        }
    };
    var finished: std.atomic.Value(bool) = .init(false);
    var rolling = try std.testing.io.concurrent(Rolling.run, .{ &reader, &next_reads, &finished });
    var rolling_joined = false;
    defer if (!rolling_joined) {
        slow.gate.store(true, .release);
        _ = rolling.cancel(std.testing.io) catch {};
    };
    for (0..200) |_| {
        if (finished.load(.acquire)) break;
        try std.testing.io.sleep(.fromMilliseconds(10), .awake);
    }
    const submitted_while_blocked = finished.load(.acquire);
    // Open the provider before joining even on regression, so a drain-before-
    // submit implementation fails the assertion instead of wedging the suite.
    slow.gate.store(true, .release);
    const rolling_result = rolling.await(std.testing.io);
    rolling_joined = true;
    try rolling_result;
    try std.testing.expect(submitted_while_blocked);
    try std.testing.expectEqual(@as(usize, 32), reader.prefetch_bytes);
    reader.drain(false);
    try std.testing.expectEqual(@as(usize, 2), slow.entered.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), scheduler.bytes);
    const bytes = try reader.reader().readPlannedAlloc(a, reads[2]);
    defer a.free(bytes);
    try std.testing.expectEqualStrings("ijkl", bytes);
    try std.testing.expectEqual(@as(usize, 2), slow.entered.load(.acquire));
    try std.testing.expect(cache.snapshot().hits > 0);
    // Concurrent misses for an identical immutable range share one provider
    // request. Each waiter remains cancelable under its own read context.
    reader.scope[0] = 2;
    slow.gate.store(false, .release);
    const identical_before = slow.entered.load(.acquire);
    try reader.prefetch(&.{ reads[0], reads[0], reads[0], reads[0] });
    for (0..200) |_| {
        if (slow.entered.load(.acquire) > identical_before) break;
        try std.testing.io.sleep(.fromMilliseconds(10), .awake);
    }
    try std.testing.expectEqual(identical_before + 1, slow.entered.load(.acquire));
    slow.gate.store(true, .release);
    reader.drain(false);
    try std.testing.expectEqual(identical_before + 1, slow.entered.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), cache.flights.count());
    // A fresh scope misses the prior cache. Cancellation drains blocked jobs
    // before their provider/metadata owners go out of scope.
    slow.gate.store(false, .release);
    reader.scope[0] = 1;
    const entered_before = slow.entered.load(.acquire);
    // Shared memory pressure shrinks the rolling window. A large first range
    // must not prevent a later small useful range from being admitted.
    scheduler.max_bytes = 24;
    var large = reads[0];
    large.range.len = 16;
    try reader.prefetch(&.{ large, reads[0], reads[1], reads[2], reads[3] });
    try std.testing.expectEqual(@as(usize, 4), reader.prefetch_bytes);
    for (0..200) |_| {
        if (slow.entered.load(.acquire) > entered_before) break;
        try std.testing.io.sleep(.fromMilliseconds(10), .awake);
    }
    reader.drain(true);
    try std.testing.expect(slow.canceled.load(.acquire) > 0);
    for (reader.pending) |future| try std.testing.expect(future == null);
    try std.testing.expectEqual(@as(usize, 0), scheduler.io_workers);
    try std.testing.expectEqual(@as(usize, 0), scheduler.io_bytes);
    // A next-file footer owns its descriptors and remains scan-owned while
    // current-file page jobs are canceled. Final close still joins the footer.
    scheduler.max_bytes = 128;
    reader.scope[0] = 3;
    slow.gate.store(false, .release);
    const footer_before = slow.entered.load(.acquire);
    var footer = reads[0];
    footer.purpose = .parquet_footer;
    try reader.prefetch(&.{ footer, reads[1] });
    for (0..200) |_| {
        if (slow.entered.load(.acquire) >= footer_before + 2) break;
        try std.testing.io.sleep(.fromMilliseconds(10), .awake);
    }
    try std.testing.expectEqual(footer_before + 2, slow.entered.load(.acquire));
    const canceled_before = slow.canceled.load(.acquire);
    reader.clearFile();
    try std.testing.expectEqual(canceled_before + 1, slow.canceled.load(.acquire));
    try std.testing.expectEqual(@as(usize, 4), reader.prefetch_bytes);
    var retained: usize = 0;
    for (reader.pending) |future| if (future) |active| {
        retained += 1;
        try std.testing.expectEqual(@as(@TypeOf(footer.purpose), .parquet_footer), active.work.read.purpose);
        try std.testing.expect(active.work.read.object.key.ptr != footer.object.key.ptr);
        try std.testing.expect(!active.work.cancelled.load(.acquire));
    };
    try std.testing.expectEqual(@as(usize, 1), retained);
    reader.drain(true);
    try std.testing.expectEqual(canceled_before + 2, slow.canceled.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), reader.prefetch_bytes);
    try std.testing.expectEqual(@as(usize, 0), scheduler.bytes);
    // A required next-file open joins the preserved parsed-footer flight;
    // it does not issue another provider request after file teardown.
    const parquet_bytes = try @import("lake_parquet_rowgroup.zig").buildTestPlainI64ParquetObjectAlloc(a, &.{.{ .column_id = "n", .values = &.{ 1, 2 } }});
    defer a.free(parquet_bytes);
    var next_put = try client.putObject("bucket", "next.parquet", parquet_bytes, .{});
    defer next_put.deinit(a);
    const next_file: @import("../external_source/types.zig").FileEntry = .{ .file_id = @constCast("next"), .object_uri = @constCast("s3://bucket/next.parquet"), .etag = next_put.etag.?, .byte_len = parquet_bytes.len, .row_count = 2, .row_groups = &.{} };
    const next_footer = try ranges.planParquetFooterRead(try ranges.objectRefForExternalFileUri(next_file), 64 * 1024);
    scheduler.max_bytes = 1024 * 1024;
    reader.scope[0] = 4;
    const opened_before = slow.entered.load(.acquire);
    try reader.prefetch(&.{next_footer});
    for (0..200) |_| {
        if (slow.entered.load(.acquire) > opened_before) break;
        try std.testing.io.sleep(.fromMilliseconds(10), .awake);
    }
    try std.testing.expectEqual(opened_before + 1, slow.entered.load(.acquire));
    reader.clearFile();
    slow.gate.store(true, .release);
    const parsed = try reader.footer(next_file);
    parsed.release();
    reader.drain(false);
    try std.testing.expectEqual(opened_before + 1, slow.entered.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), scheduler.bytes);
}

test "external lake immutable artifact cache survives restart without provider reads" {
    const a = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(a, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/immutable-cache", .{tmp.sub_path});
    defer a.free(root);
    var source = struct {
        calls: usize = 0,
        fail: bool = false,
        fn load(raw: *anyopaque, alloc: Allocator) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            if (self.fail) return error.ProviderUnavailable;
            return alloc.dupe(u8, "authenticated");
        }
    }{};
    const loader: Cache.ImmutableLoader = .{ .ptr = &source, .load = @TypeOf(source).load };
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash("authenticated", &digest, .{});
    {
        var cache = Cache.initWithMemoryLimit(a, 0);
        defer cache.deinit();
        try cache.ensurePersistent(io, root, .{}, .{});
        const bytes = try cache.readImmutableAlloc(a, @splat(1), "root", 13, digest, .{ .io = io }, loader);
        defer a.free(bytes);
        try std.testing.expectEqualStrings("authenticated", bytes);
    }
    source.fail = true;
    {
        var cache = Cache.initWithMemoryLimit(a, 0);
        defer cache.deinit();
        try cache.ensurePersistent(io, root, .{}, .{});
        const bytes = try cache.readImmutableAlloc(a, @splat(1), "root", 13, digest, .{ .io = io }, loader);
        defer a.free(bytes);
        try std.testing.expectEqualStrings("authenticated", bytes);
        try std.testing.expectEqual(@as(usize, 1), source.calls);
        try std.testing.expectEqual(@as(u64, 1), cache.snapshot().disk_hits);
        try std.testing.expectError(error.ProviderUnavailable, cache.readImmutableAlloc(a, @splat(2), "root", 13, digest, .{ .io = io }, loader));
    }
}

test "external lake immutable cache authenticates payloads and separates credential scopes" {
    const a = std.testing.allocator;
    var cache = Cache.initWithMemoryLimit(a, 32);
    defer cache.deinit();
    var source = struct {
        calls: usize = 0,
        bytes: []const u8 = "immutable",
        fn load(raw: *anyopaque, alloc: Allocator) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            return alloc.dupe(u8, self.bytes);
        }
    }{};
    const loader: Cache.ImmutableLoader = .{ .ptr = &source, .load = @TypeOf(source).load };
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(source.bytes, &digest, .{});
    for (0..2) |_| {
        const bytes = try cache.readImmutableAlloc(a, @splat(1), "artifact", 9, digest, .{ .io = std.testing.io }, loader);
        defer a.free(bytes);
        try std.testing.expectEqualStrings("immutable", bytes);
    }
    try std.testing.expectEqual(@as(usize, 1), source.calls);
    const isolated = try cache.readImmutableAlloc(a, @splat(2), "artifact", 9, digest, .{}, loader);
    defer a.free(isolated);
    try std.testing.expectEqual(@as(usize, 2), source.calls);
    source.bytes = "corrupted";
    try std.testing.expectError(error.ArtifactIntegrityMismatch, cache.readImmutableAlloc(a, @splat(3), "artifact", 9, digest, .{}, loader));
    source.bytes = "immutable";
    const retry = try cache.readImmutableAlloc(a, @splat(3), "artifact", 9, digest, .{}, loader);
    defer a.free(retry);
    try std.testing.expectEqual(@as(usize, 4), source.calls);
    try std.testing.expect(cache.snapshot().stored_bytes <= 32);
    const canceled = @import("../../storage/object_storage.zig").CancellationToken{ .ptr = &source, .is_cancelled_fn = struct {
        fn check(_: *const anyopaque) bool {
            return true;
        }
    }.check };
    try std.testing.expectError(error.Canceled, cache.readImmutableAlloc(a, @splat(1), "artifact", 9, digest, .{ .cancellation = canceled }, loader));
    try std.testing.expectEqual(@as(usize, 4), source.calls);
}

test "external lake immutable native mappings survive eviction and reject cache damage" {
    if (comptime @import("builtin").os.tag == .freestanding or @import("builtin").os.tag == .wasi or @import("builtin").os.tag == .windows) return error.SkipZigTest;
    const a = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(a, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/native-mappings", .{tmp.sub_path});
    defer a.free(path);
    var cache = Cache.init(a);
    defer cache.deinit();
    try cache.ensurePersistent(io, path, .{ .max_entries = 1, .max_total_bytes = 1024, .protected_bytes = 0 }, .{});
    const Provider = struct {
        calls: usize = 0,
        fn load(raw: *anyopaque, alloc: Allocator) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            return alloc.dupe(u8, "native segment");
        }
    };
    var provider: Provider = .{};
    const loader: Cache.ImmutableLoader = .{ .ptr = &provider, .load = Provider.load };
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash("native segment", &digest, .{});
    var cold = try cache.readImmutableLease(a, @splat(1), "segment", 14, digest, .{ .io = io }, loader);
    cold.deinit();
    cache.persistent.?.flush();
    {
        var mapped = try cache.readImmutableLease(a, @splat(1), "segment", 14, digest, .{ .io = io }, loader);
        defer mapped.deinit();
        try std.testing.expect(mapped == .mapped);
        try std.testing.expectEqualStrings("native segment", mapped.bytes());
        try std.testing.expectEqual(@as(usize, 1), provider.calls);
        _ = cache.persistent.?.enqueueWrite("eviction pressure", "another payload");
        cache.persistent.?.flush();
        var again = try cache.readImmutableLease(a, @splat(1), "segment", 14, digest, .{ .io = io }, loader);
        defer again.deinit();
        try std.testing.expect(again == .mapped);
        try std.testing.expectEqualStrings("native segment", again.bytes());
    }
    const key = try Cache.immutableKey(a, @splat(1), "segment", 14, digest);
    defer a.free(key);
    var key_digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(key, &key_digest, .{});
    const damaged_path = try std.fs.path.join(a, &.{ path, &std.fmt.bytesToHex(key_digest, .lower) });
    defer a.free(damaged_path);
    const damaged = try std.Io.Dir.cwd().createFile(io, damaged_path, .{});
    defer damaged.close(io);
    try damaged.writePositionalAll(io, "damaged cache", 0);
    var repaired = try cache.readImmutableLease(a, @splat(1), "segment", 14, digest, .{ .io = io }, loader);
    defer repaired.deinit();
    try std.testing.expectEqualStrings("native segment", repaired.bytes());
    try std.testing.expect(cache.persistentStats().?.corrupt_entries_removed != 0);
    // A canceled publication owner cannot obtain a new mapping from warm disk.
    try std.testing.expectError(error.DeadlineExceeded, cache.readImmutableLease(a, @splat(1), "segment", 14, digest, .{ .deadline_ns = 0 }, loader));
}

test "external lake range leases pin cache bytes across bounded eviction" {
    const a = std.testing.allocator;
    var cache = Cache.init(a);
    defer cache.deinit();
    cache.max_bytes = 6;
    try cache.store("first", "abc");
    const lease = cache.pin("first").?;
    try cache.store("second", "def");
    try cache.store("third", "ghi");
    try std.testing.expectEqualStrings("abc", lease.bytes);
    const again = cache.pin("first").?;
    try std.testing.expectEqual(lease.bytes.ptr, again.bytes.ptr);
    again.release();
    lease.release();
    try cache.store("fourth", "jkl");
    try std.testing.expect(cache.snapshot().stored_bytes <= cache.max_bytes);
}

fn rangeAdmissionAllocationScenario(a: Allocator) !void {
    var cache = Cache.init(a);
    defer cache.deinit();
    try cache.store("versioned-range", "immutable payload");
    const lease = cache.pin("versioned-range").?;
    defer lease.release();
    try std.testing.expectEqualStrings("immutable payload", lease.bytes);
}
test "external lake range admission unwinds every allocation failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, rangeAdmissionAllocationScenario, .{});
}

test "external lake serving persistent tier survives restart and isolates credentials and versions" {
    const storage = @import("../../storage/object_storage.zig");
    const a = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(a, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/serving-cache", .{tmp.sub_path});
    defer a.free(root);
    var memory = storage.MemoryObjectStorage.init(a);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("bucket");
    var put = try client.putObject("bucket", "data", "abcdefgh", .{});
    defer put.deinit(a);
    const Provider = struct {
        base: storage.ObjectStorage,
        calls: usize = 0,
        fail: bool = false,
        vtable: storage.ObjectStorage.VTable,
        fn get(raw: *anyopaque, alloc: Allocator, bucket: []const u8, key: []const u8, options: storage.GetOptions) !storage.GetResult {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            if (self.fail) return error.ProviderUnavailable;
            var base = self.base;
            base.allocator = alloc;
            return base.getObject(bucket, key, options);
        }
    };
    var provider: Provider = .{ .base = client, .vtable = client.vtable.* };
    provider.vtable.get_object = Provider.get;
    const base = ObjectReader.init(.{ .allocator = a, .ptr = &provider, .vtable = &provider.vtable });
    const read: ranges.RangeRead = .{ .object = .{ .bucket = "bucket", .key = "data", .byte_len = 8, .version = .{ .etag = put.etag.? } }, .range = .{ .offset = 2, .len = 4 }, .purpose = .parquet_column_chunk };
    {
        var cache = Cache.init(a);
        defer cache.deinit();
        try cache.ensurePersistent(io, root, .{}, .{});
        var reader: Reader = .{ .cache = &cache, .base = base, .scope = @splat(1), .context = .{ .io = io } };
        const bytes = try reader.reader().readPlannedAlloc(a, read);
        defer a.free(bytes);
        try std.testing.expectEqualStrings("cdef", bytes);
        try std.testing.expectEqual(@as(usize, 1), provider.calls);
        // deinit drains the accepted write before destroying the worker.
    }
    provider.fail = true;
    {
        var cache = Cache.init(a);
        defer cache.deinit();
        try cache.ensurePersistent(io, root, .{}, .{});
        // Exercise disk leases even when RAM admission is disabled.
        cache.max_bytes = 0;
        var reader: Reader = .{ .cache = &cache, .base = base, .scope = @splat(1), .context = .{ .io = io } };
        const bytes = try reader.reader().readPlannedLease(a, read);
        defer bytes.release();
        try std.testing.expectEqualStrings("cdef", bytes.bytes);
        try std.testing.expectEqual(@as(usize, 1), provider.calls);
        try std.testing.expectEqual(@as(usize, 1), cache.persistentStats().?.read_hits);
        reader.scope = @splat(2);
        try std.testing.expectError(error.ProviderUnavailable, reader.reader().readPlannedAlloc(a, read));
        reader.scope = @splat(1);
        var changed = read;
        changed.object.version.etag = "changed-version";
        try std.testing.expectError(error.ProviderUnavailable, reader.reader().readPlannedAlloc(a, changed));
        changed.object.version = .{};
        try std.testing.expectError(error.InvalidLakeRangeRead, reader.reader().readPlannedAlloc(a, changed));
        try std.testing.expectEqual(@as(usize, 3), provider.calls);
    }
}

test "external lake serving RAM protects metadata under broad scan pressure" {
    const a = std.testing.allocator;
    var cache = Cache.init(a);
    defer cache.deinit();
    cache.max_bytes = 12;
    try cache.store("lake-range:v2:purpose=parquet_footer:identity=footer", "abc");
    try cache.store("scan-1", "def");
    try cache.store("scan-2", "ghi");
    try cache.store("scan-3", "jkl");
    try cache.store("scan-4", "mno");
    const footer = cache.pin("lake-range:v2:purpose=parquet_footer:identity=footer").?;
    defer footer.release();
    try std.testing.expectEqualStrings("abc", footer.bytes);
    try std.testing.expectEqual(@as(usize, 3), cache.snapshot().protected_stored_bytes);
    try std.testing.expectEqual(@as(usize, 12), cache.snapshot().stored_bytes);
}

test "external lake disk cache initialization failure preserves source reads and retries after cooldown" {
    const a = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(a, .{});
    defer io_impl.deinit();
    const Clock = struct {
        var awake_ns: i96 = 0;
        fn now(_: ?*anyopaque, clock: std.Io.Clock) std.Io.Timestamp {
            if (clock == .awake) return .{ .nanoseconds = awake_ns };
            return std.testing.io.vtable.now(std.testing.io.userdata, clock);
        }
    };
    Clock.awake_ns = 0;
    var vtable = io_impl.io().vtable.*;
    vtable.now = Clock.now;
    const io: std.Io = .{ .userdata = io_impl.io().userdata, .vtable = &vtable };
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(io, .{ .sub_path = "not-a-directory", .data = "file" });
    const root = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/not-a-directory/cache", .{tmp.sub_path});
    defer a.free(root);
    var cache = Cache.init(a);
    defer cache.deinit();
    {
        try std.testing.expect(cache.persistent_mutex.tryLock());
        defer cache.persistent_mutex.unlock(io);
        try cache.ensurePersistent(io, root, .{}, .{});
        try std.testing.expectEqual(@as(u64, 0), cache.snapshot().disk_init_attempts);
        try std.testing.expect(cache.persistentStats() == null);
    }
    try cache.ensurePersistent(io, root, .{}, .{});
    try std.testing.expect(cache.persistent == null);
    try std.testing.expect(cache.snapshot().disk_unavailable != null);
    var memory = @import("../../storage/object_storage.zig").MemoryObjectStorage.init(a);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("bucket");
    var put = try client.putObject("bucket", "data", "data", .{});
    defer put.deinit(a);
    var reader: Reader = .{ .cache = &cache, .base = ObjectReader.init(client), .scope = @splat(0), .context = .{ .io = io } };
    const read: ranges.RangeRead = .{ .object = .{ .bucket = "bucket", .key = "data", .byte_len = 4, .version = .{ .etag = put.etag.? } }, .range = .{ .offset = 0, .len = 4 }, .purpose = .parquet_column_chunk };
    const lease = try reader.reader().readPlannedLease(a, read);
    defer lease.release();
    try std.testing.expectEqualStrings("data", lease.bytes);
    try std.testing.expectEqual(@as(u64, 1), cache.snapshot().provider_reads);
    try std.testing.expectEqual(@as(u64, 1), cache.snapshot().disk_init_attempts);
    try std.testing.expectEqual(@as(u64, 1), cache.snapshot().disk_init_failures);
    // A repaired directory must not trigger repeated inventory scans inside
    // the cooldown. Advance the injected Io clock instead of sleeping.
    try tmp.dir.deleteFile(io, "not-a-directory");
    try cache.ensurePersistent(io, root, .{}, .{});
    try std.testing.expect(cache.persistent == null);
    Clock.awake_ns = 30 * std.time.ns_per_s;
    try cache.ensurePersistent(io, root, .{}, .{});
    try std.testing.expect(cache.persistent != null);
    try std.testing.expect(cache.snapshot().disk_unavailable == null);
    try std.testing.expectEqual(@as(u64, 2), cache.snapshot().disk_init_attempts);
    try cache.ensurePersistent(io, root, .{}, .{});
    try std.testing.expectEqual(@as(u64, 2), cache.snapshot().disk_init_attempts);
    try std.testing.expectEqual(.enqueued, cache.persistent.?.enqueueWrite("recovered", "data"));
    cache.persistent.?.flush();
    try std.testing.expectEqual(@as(usize, 1), cache.persistentStats().?.writes_completed);
}

const InventoryGate = struct {
    var base: std.Io = undefined;
    var entered: std.Io.Event = .unset;
    var release: std.Io.Event = .unset;
    fn stat(raw: ?*anyopaque, dir: std.Io.Dir, path: []const u8, options: std.Io.Dir.StatFileOptions) std.Io.Dir.StatFileError!std.Io.File.Stat {
        entered.set(base);
        try release.wait(base);
        return base.vtable.dirStatFile(raw, dir, path, options);
    }
    fn read(raw: ?*anyopaque, file: std.Io.File, data: []const []u8, offset: u64) std.Io.File.ReadPositionalError!usize {
        entered.set(base);
        try release.wait(base);
        return base.vtable.fileReadPositional(raw, file, data, offset);
    }
};

test "external lake background inventory does not block reads and retains restart hits" {
    if (comptime @import("builtin").os.tag == .freestanding or @import("builtin").os.tag == .wasi or @import("builtin").os.tag == .windows) return error.SkipZigTest;
    const a = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(a, .{ .async_limit = .nothing, .concurrent_limit = .limited(2) });
    defer io_impl.deinit();
    const base = io_impl.io();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/background", .{tmp.sub_path});
    defer a.free(root);
    const policy: parquet.PersistentObjectRangeCachePolicy = .{ .max_entries = 1, .max_total_bytes = 1024 };
    // Use the actual payload digest in the authenticated key and read below.
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash("data", &digest, .{});
    const authenticated_key = try Cache.immutableKey(a, @splat(1), "segment", 4, digest);
    defer a.free(authenticated_key);
    {
        var disk = try parquet.PersistentObjectRangeCache.initWithPolicy(base, root, policy);
        defer disk.deinit();
        try std.testing.expectEqual(.enqueued, disk.enqueueWrite(authenticated_key, "data"));
        disk.flush();
    }
    InventoryGate.base = base;
    InventoryGate.entered = .unset;
    InventoryGate.release = .unset;
    var vtable = base.vtable.*;
    vtable.dirStatFile = InventoryGate.stat;
    const io: std.Io = .{ .userdata = base.userdata, .vtable = &vtable };
    var cache = Cache.initWithMemoryLimit(a, 0);
    defer cache.deinit();
    defer InventoryGate.release.set(base);
    const borrowed = try a.dupe(u8, root);
    try cache.startPersistent(io, borrowed, policy, .{});
    a.free(borrowed);
    try InventoryGate.entered.wait(base);
    try std.testing.expect(!cache.persistentReady());
    try std.testing.expect(cache.persistentStats() == null);
    // A second request neither waits on inventory nor starts a second owner.
    try cache.startPersistent(io, root, policy, .{});
    try std.testing.expectEqual(@as(u64, 1), cache.snapshot().disk_init_attempts);
    const Provider = struct {
        calls: usize = 0,
        fn load(raw: *anyopaque, alloc: Allocator) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            return alloc.dupe(u8, "data");
        }
    };
    var provider: Provider = .{};
    const loader: Cache.ImmutableLoader = .{ .ptr = &provider, .load = Provider.load };
    var first = try cache.readImmutableLease(a, @splat(1), "segment", 4, digest, .{ .io = base }, loader);
    first.deinit();
    try std.testing.expectEqual(@as(usize, 1), provider.calls);
    InventoryGate.release.set(base);
    try cache.persistent_startup.await(io);
    try std.testing.expect(cache.persistentReady());
    var restarted = try cache.readImmutableLease(a, @splat(1), "segment", 4, digest, .{ .io = base }, loader);
    defer restarted.deinit();
    try std.testing.expectEqualStrings("data", restarted.bytes());
    try std.testing.expectEqual(@as(usize, 1), provider.calls);
    try std.testing.expectEqual(@as(usize, 1), cache.persistentStats().?.entries);
    try std.testing.expect(cache.persistentStats().?.stored_bytes <= policy.max_total_bytes);
}

test "external lake shutdown cancels and joins blocked inventory restoration" {
    const a = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(a, .{ .async_limit = .nothing, .concurrent_limit = .limited(2) });
    defer io_impl.deinit();
    const base = io_impl.io();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/cancel", .{tmp.sub_path});
    defer a.free(root);
    {
        var disk = try parquet.PersistentObjectRangeCache.init(base, root);
        defer disk.deinit();
        try std.testing.expectEqual(.enqueued, disk.enqueueWrite("key", "data"));
        disk.flush();
    }
    for ([_]bool{ false, true }) |header_read| {
        InventoryGate.base = base;
        InventoryGate.entered = .unset;
        InventoryGate.release = .unset;
        var vtable = base.vtable.*;
        if (header_read) vtable.fileReadPositional = InventoryGate.read else vtable.dirStatFile = InventoryGate.stat;
        const io: std.Io = .{ .userdata = base.userdata, .vtable = &vtable };
        var cache = Cache.init(a);
        var closed = false;
        defer if (!closed) cache.deinit();
        try cache.startPersistent(io, root, .{}, .{});
        try InventoryGate.entered.wait(base);
        if (header_read) {
            cache.persistent_startup.cancel(io);
            // Provenance recovery must propagate cancellation, never publish a
            // partially restored cache after treating Canceled as an unprotected key.
            try std.testing.expect(!cache.persistentReady());
        }
        cache.deinit();
        closed = true;
        // The canceled owner must release OWNER.lock and all inventoried memory.
        var disk = try parquet.PersistentObjectRangeCache.init(base, root);
        defer disk.deinit();
        try std.testing.expectEqual(@as(usize, 1), disk.statsSnapshot().entries);
    }
}

test "external lake query phase counters retain nested hydration and saturate" {
    var cache = Cache.init(std.testing.allocator);
    defer cache.deinit();
    const phases: Cache.QueryPhases = .{ .total_ns = 100, .publication_ns = 20, .search_ns = 50, .hydration_ns = 30, .delivery_ns = 30 };
    cache.recordQuery(phases);
    cache.recordQuery(phases);
    const stats = cache.snapshot();
    try std.testing.expectEqual(@as(u64, 2), stats.completed_queries);
    try std.testing.expectEqual(@as(u64, 200), stats.query_total_ns);
    try std.testing.expectEqual(@as(u64, 40), stats.query_publication_ns);
    try std.testing.expectEqual(@as(u64, 100), stats.query_search_ns);
    try std.testing.expectEqual(@as(u64, 60), stats.query_hydration_ns);
    try std.testing.expectEqual(@as(u64, 60), stats.query_delivery_ns);
    cache.stats.query_total_ns = std.math.maxInt(u64);
    cache.recordQuery(phases);
    try std.testing.expectEqual(std.math.maxInt(u64), cache.snapshot().query_total_ns);
}

test "external lake disk cache recovers from ownership contention with bounded retries" {
    const a = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(a, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/recovered-cache", .{tmp.sub_path});
    defer a.free(root);
    const start = 100 * std.time.ns_per_s;
    const retry = start + 30 * std.time.ns_per_s;
    var cache = Cache.initWithMemoryLimit(a, 0);
    defer cache.deinit();
    try std.testing.expect(!cache.persistentReady());
    {
        var owner = try parquet.PersistentObjectRangeCache.init(io, root);
        defer owner.deinit();
        try cache.ensurePersistentAt(io, root, .{}, .{}, start);
        try std.testing.expect(cache.persistentCache() == null);
        try std.testing.expectEqualStrings("WouldBlock", cache.snapshot().disk_unavailable.?);
        try std.testing.expect(!cache.persistentReady());
        try std.testing.expectEqual(@as(u64, 1), cache.snapshot().disk_init_failures);
        // A busy serving process must not repeat inventory/lock attempts.
        for (0..100) |_| try cache.ensurePersistentAt(io, root, .{}, .{}, retry - 1);
        try std.testing.expectEqual(@as(u64, 1), cache.snapshot().disk_init_attempts);
    }
    try cache.ensurePersistentAt(io, root, .{}, .{}, retry);
    try std.testing.expect(cache.persistentReady());
    try std.testing.expect(cache.persistentStats() != null);
    try std.testing.expect(cache.snapshot().disk_unavailable == null);
    try std.testing.expectEqual(@as(u64, 2), cache.snapshot().disk_init_attempts);
    try cache.ensurePersistentAt(io, root, .{}, .{}, retry + 60 * std.time.ns_per_s);
    try std.testing.expectEqual(@as(u64, 2), cache.snapshot().disk_init_attempts);
    // The recovered tier accepts and serves verified bytes with RAM disabled.
    const disk = cache.persistentCache().?;
    try std.testing.expectEqual(parquet.PersistentObjectRangeCacheEnqueueResult.enqueued, disk.enqueueWrite("recovered", "bytes"));
    disk.flush();
    const bytes = (try disk.readAlloc(a, "recovered", 5)).?;
    defer a.free(bytes);
    try std.testing.expectEqualStrings("bytes", bytes);
    try std.testing.expectEqual(@as(usize, 1), disk.statsSnapshot().writes_completed);
}

test "external lake immutable block leases borrow RAM and pin bytes through eviction pressure" {
    const a = std.testing.allocator;
    var cache = Cache.init(a);
    defer cache.deinit();
    cache.max_entries = 1;
    const Provider = struct {
        calls: usize = 0,
        fn load(raw: *anyopaque, alloc: Allocator) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            return alloc.dupe(u8, "immutable block");
        }
    };
    var provider: Provider = .{};
    const loader: Cache.ImmutableLoader = .{ .ptr = &provider, .load = Provider.load };
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash("immutable block", &digest, .{});
    var first = try cache.readImmutableBlockLease(a, @splat(1), "block", 15, digest, .{}, loader);
    defer first.deinit();
    var second = try cache.readImmutableBlockLease(a, @splat(1), "block", 15, digest, .{}, loader);
    defer second.deinit();
    try std.testing.expect(first == .shared and second == .shared);
    try std.testing.expect(first.bytes().ptr == second.bytes().ptr);
    try cache.store("eviction pressure", "other bytes");
    try std.testing.expectEqualStrings("immutable block", first.bytes());
    try std.testing.expectEqual(@as(usize, 1), provider.calls);
    try std.testing.expectError(error.DeadlineExceeded, cache.readImmutableBlockLease(a, @splat(1), "block", 15, digest, .{ .deadline_ns = 0 }, loader));
}

test "external lake bounded verified mappings reuse owners and pin through pressure" {
    if (comptime @import("builtin").os.tag == .freestanding or @import("builtin").os.tag == .wasi or @import("builtin").os.tag == .windows) return error.SkipZigTest;
    const a = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(a, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/block-mappings", .{tmp.sub_path});
    defer a.free(path);
    var cache = Cache.initWithMemoryLimit(a, 0);
    defer cache.deinit();
    cache.max_mappings = 1;
    try cache.ensurePersistent(io, path, .{}, .{});
    const Provider = struct {
        calls: usize = 0,
        fn load(raw: *anyopaque, alloc: Allocator) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            return alloc.dupe(u8, "verified block");
        }
    };
    var provider: Provider = .{};
    const loader: Cache.ImmutableLoader = .{ .ptr = &provider, .load = Provider.load };
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash("verified block", &digest, .{});
    var cold = try cache.readImmutableBlockLease(a, @splat(1), "block", 14, digest, .{ .io = io }, loader);
    cold.deinit();
    cache.persistent.?.flush();
    var first = try cache.readImmutableBlockLease(a, @splat(1), "block", 14, digest, .{ .io = io }, loader);
    defer first.deinit();
    var second = try cache.readImmutableBlockLease(a, @splat(1), "block", 14, digest, .{ .io = io }, loader);
    defer second.deinit();
    try std.testing.expect(first == .mapping and second == .mapping);
    try std.testing.expect(first.bytes().ptr == second.bytes().ptr);
    try std.testing.expectEqual(@as(u64, 1), cache.snapshot().mapping_hits);
    try std.testing.expectEqual(@as(usize, 1), cache.persistentStats().?.read_hits);
    try std.testing.expectError(error.DeadlineExceeded, cache.readImmutableBlockLease(a, @splat(1), "block", 14, digest, .{ .io = io, .deadline_ns = 0 }, loader));
    var other_cold = try cache.readImmutableBlockLease(a, @splat(2), "block", 14, digest, .{ .io = io }, loader);
    other_cold.deinit();
    cache.persistent.?.flush();
    var pressure = try cache.readImmutableBlockLease(a, @splat(2), "block", 14, digest, .{ .io = io }, loader);
    defer pressure.deinit();
    try std.testing.expect(pressure == .mapped);
    try std.testing.expectEqualStrings("verified block", first.bytes());
    try std.testing.expect(cache.mapped_bytes <= cache.max_mapped_bytes);
    try std.testing.expectEqual(@as(usize, 1), cache.mappings.count());
    try std.testing.expectEqual(@as(usize, 2), provider.calls);
}

test "external lake disk pressure reclaims idle mappings while preserving active readers" {
    if (comptime @import("builtin").os.tag == .freestanding or @import("builtin").os.tag == .wasi or @import("builtin").os.tag == .windows) return error.SkipZigTest;
    const a = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(a, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/mapping-pressure", .{tmp.sub_path});
    defer a.free(path);
    var cache = Cache.initWithMemoryLimit(a, 0);
    defer cache.deinit();
    try cache.ensurePersistent(io, path, .{ .max_entries = 2 }, .{});
    const Provider = struct {
        fn load(_: *anyopaque, alloc: Allocator) ![]u8 {
            return alloc.dupe(u8, "verified block");
        }
    };
    var dummy: u8 = 0;
    const loader: Cache.ImmutableLoader = .{ .ptr = &dummy, .load = Provider.load };
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash("verified block", &digest, .{});
    for ([_][]const u8{ "active", "idle" }) |key| {
        var cold = try cache.readImmutableBlockLease(a, @splat(1), key, 14, digest, .{ .io = io }, loader);
        cold.deinit();
    }
    cache.persistent.?.flush();
    var active = try cache.readImmutableBlockLease(a, @splat(1), "active", 14, digest, .{ .io = io }, loader);
    defer active.deinit();
    {
        var idle = try cache.readImmutableBlockLease(a, @splat(1), "idle", 14, digest, .{ .io = io }, loader);
        defer idle.deinit();
        try std.testing.expect(active == .mapping and idle == .mapping);
        try std.testing.expect(!Cache.reclaimIdleMapping(&cache));
    }
    // Both entries remain disk-pinned, but only one has a live query reader.
    // Disk capacity is much smaller than the mapping cache's independent limit.
    var incoming = try cache.readImmutableBlockLease(a, @splat(1), "incoming", 14, digest, .{ .io = io }, loader);
    incoming.deinit();
    cache.persistent.?.flush();
    try std.testing.expectEqual(@as(usize, 1), cache.mappings.count());
    try std.testing.expectEqual(@as(usize, 1), cache.persistentStats().?.evicted_entries);
    var admitted = try cache.readImmutableBlockLease(a, @splat(1), "incoming", 14, digest, .{ .io = io }, loader);
    defer admitted.deinit();
    try std.testing.expect(admitted == .mapping);
    try std.testing.expectEqualStrings("verified block", active.bytes());
}

test "external lake verified slice admission retains denied ownership and copies resident units once" {
    const a = std.testing.allocator;
    var cache = Cache.init(a);
    defer cache.deinit();
    const Owner = struct {
        released: usize = 0,
        fn release(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.released += 1;
        }
    };
    var owner: Owner = .{};
    const bytes = "verified unit";
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
    const verified: Cache.VerifiedLease = .{ .value = try Cache.VerifiedBytes.authenticate(bytes, digest), .owner = .{ .bytes = bytes, .owner = .{ .shared = .{ .ptr = &owner, .release_fn = Owner.release } } } };
    cache.max_entries = 0;
    var denied = try cache.admitVerifiedBlock(a, @splat(1), "unit", bytes.len, digest, .{}, verified);
    try std.testing.expect(denied.bytes().ptr == bytes.ptr);
    try std.testing.expectEqual(@as(usize, 0), owner.released);
    denied.deinit();
    try std.testing.expectEqual(@as(usize, 1), owner.released);
    cache.max_entries = 4096;
    var admitted = try cache.admitVerifiedBlock(a, @splat(1), "unit", bytes.len, digest, .{}, verified);
    defer admitted.deinit();
    try std.testing.expect(admitted.bytes().ptr != bytes.ptr);
    try std.testing.expectEqualStrings(bytes, admitted.bytes());
    try std.testing.expectEqual(@as(usize, 2), owner.released);
    try std.testing.expectError(error.ArtifactIntegrityMismatch, cache.admitVerifiedBlock(a, @splat(2), "unit", bytes.len + 1, digest, .{}, verified));
    try std.testing.expectEqual(@as(usize, 3), owner.released);
    try std.testing.expectError(error.ArtifactIntegrityMismatch, Cache.VerifiedBytes.authenticate("corrupt", digest));
}

test "external lake prefetch coalesces adjacent pages with shared bounded lease slices" {
    const storage = @import("../../storage/object_storage.zig");
    const a = std.testing.allocator;
    var memory = storage.MemoryObjectStorage.init(a);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("bucket");
    var put = try client.putObject("bucket", "pages", "abcdefghijklmnop", .{});
    defer put.deinit(a);
    const Provider = struct {
        base: storage.ObjectStorage,
        calls: std.atomic.Value(usize) = .init(0),
        vtable: storage.ObjectStorage.VTable,
        fn get(raw: *anyopaque, alloc: Allocator, bucket: []const u8, key: []const u8, options: storage.GetOptions) !storage.GetResult {
            const self: *@This() = @ptrCast(@alignCast(raw));
            _ = self.calls.fetchAdd(1, .monotonic);
            var base = self.base;
            base.allocator = alloc;
            return base.getObject(bucket, key, options);
        }
    };
    var provider: Provider = .{ .base = client, .vtable = client.vtable.* };
    provider.vtable.get_object = Provider.get;
    var cache = Cache.initWithMemoryLimit(a, 64);
    defer cache.deinit();
    var scheduler: Reader.Scheduling.Scheduler = .{ .max_io_workers = 8, .max_bytes = 128 };
    var reader: Reader = .{ .cache = &cache, .base = ObjectReader.init(.{ .allocator = a, .ptr = &provider, .vtable = &provider.vtable }), .scope = @splat(0), .context = .{ .io = std.testing.io }, .scheduler = &scheduler };
    defer reader.drain(true);
    const object: ranges.ObjectRef = .{ .bucket = "bucket", .key = "pages", .byte_len = 16, .version = .{ .etag = put.etag.? } };
    const reads = [_]ranges.RangeRead{
        .{ .object = object, .range = .{ .offset = 0, .len = 4 }, .purpose = .parquet_column_chunk },
        .{ .object = object, .range = .{ .offset = 4, .len = 4 }, .purpose = .parquet_column_chunk },
        // The gap is never fetched speculatively.
        .{ .object = object, .range = .{ .offset = 12, .len = 4 }, .purpose = .parquet_column_chunk },
    };
    try reader.prefetch(&reads);
    // Required reads wait on member flights, then borrow their exact slices.
    const first = try Reader.readLease(&reader, a, reads[0]);
    defer first.release();
    const second = try Reader.readLease(&reader, a, reads[1]);
    defer second.release();
    reader.drain(false);
    try std.testing.expectEqual(@as(usize, 2), provider.calls.load(.acquire));
    try std.testing.expectEqualStrings("abcd", first.bytes);
    try std.testing.expectEqualStrings("efgh", second.bytes);
    try std.testing.expectEqual(@intFromPtr(first.bytes.ptr) + 4, @intFromPtr(second.bytes.ptr));
    try std.testing.expect(cache.snapshot().stored_bytes <= cache.max_bytes);
    // A hot envelope is not fetched again, and pinned slices survive pressure.
    try reader.prefetch(&reads);
    reader.drain(false);
    cache.max_bytes = 16;
    try cache.store("pressure", "0123456789abcdef");
    try std.testing.expectEqualStrings("efgh", second.bytes);
    try std.testing.expectEqual(@as(usize, 2), provider.calls.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), cache.flights.count());
    try std.testing.expectEqual(@as(usize, 0), scheduler.bytes);
}
