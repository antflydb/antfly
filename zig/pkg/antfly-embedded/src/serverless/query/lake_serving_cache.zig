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
    persistent_retry_after_ns: u64 = 0,
    persistent_ready: std.atomic.Value(bool) = .init(false),

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

    /// Configure once before reads. The server owns the worker and drains it
    /// after cursors are quiescent; a request never owns cache I/O state.
    pub fn ensurePersistent(self: *Cache, io: std.Io, root: []const u8, policy: parquet.PersistentObjectRangeCachePolicy, resources: parquet.PersistentObjectRangeCacheResources) !void {
        return self.ensurePersistentAt(io, root, policy, resources, @import("antfly_platform").time.monotonicNs());
    }

    // Keep recovery opportunistic and bounded: no timer/worker while disabled,
    // no directory inventory on every query, and no request sleeps on backoff.
    fn ensurePersistentAt(self: *Cache, io: std.Io, root: []const u8, policy: parquet.PersistentObjectRangeCachePolicy, resources: parquet.PersistentObjectRangeCacheResources, now: u64) !void {
        if (self.persistent_ready.load(.acquire)) return;
        try self.persistent_mutex.lock(io);
        defer self.persistent_mutex.unlock(io);
        try policy.validate();
        if (root.len == 0) return error.InvalidPersistentObjectRangeCachePolicy;
        if (self.persistent_ready.load(.acquire) or now < self.persistent_retry_after_ns) return;
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        self.stats.disk_init_attempts +|= 1;
        self.mutex.unlock();
        var coordinated = resources;
        coordinated.reclaim_idle = .{ .ptr = self, .reclaim_one = reclaimIdleMapping };
        const disk = parquet.PersistentObjectRangeCache.initWithPolicyAndResources(io, root, policy, coordinated) catch |err| {
            if (err == error.Canceled) return err;
            // Local cache availability is never source/readiness authority.
            // Retry after a minute so transient ownership/resource failures do
            // not silently disable restart reuse for this process's lifetime.
            self.recordDiskUnavailable(err);
            self.persistent_retry_after_ns = now +| 60 * std.time.ns_per_s;
            while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
            self.stats.disk_init_failures +|= 1;
            self.mutex.unlock();
            return;
        };
        // Publish exactly once. Readers must acquire ready before touching the
        // optional owner, including when they began serving during a failure.
        self.persistent = disk;
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        self.stats.disk_unavailable = null;
        self.mutex.unlock();
        self.persistent_ready.store(true, .release);
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
    const Entry = struct { cache: *Cache, bytes: []u8, touched: u64, protected: bool = false, refs: usize = 0 };
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
    };
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
    pub fn init(alloc: Allocator) Cache {
        return initWithMemoryLimit(alloc, 64 * 1024 * 1024);
    }
    pub fn initWithMemoryLimit(alloc: Allocator, maximum: usize) Cache {
        return .{ .alloc = alloc, .decoded = .{ .a = alloc }, .max_bytes = maximum };
    }
    pub fn deinit(self: *Cache) void {
        // Readers are quiescent. Join accepted writes before destroying the
        // mapping table borrowed by the worker's pressure callback.
        if (self.persistent) |*disk| disk.flush();
        var mappings = self.mappings.valueIterator();
        while (mappings.next()) |entry| entry.*.destroy();
        self.mappings.deinit(self.alloc);
        if (self.persistent) |*disk| disk.deinit();
        self.decoded.deinit();
        std.debug.assert(self.flights.count() == 0);
        self.flights.deinit(self.alloc);
        var iter = self.entries.iterator();
        while (iter.next()) |entry| {
            self.alloc.free(entry.key_ptr.*);
            std.debug.assert(entry.value_ptr.*.refs == 0);
            self.alloc.free(entry.value_ptr.*.bytes);
            self.alloc.destroy(entry.value_ptr.*);
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
    };

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
        defer self.mutex.unlock();
        var victim: ?*Mapping = null;
        var entries = self.mappings.valueIterator();
        while (entries.next()) |entry| {
            if (entry.*.refs != 0) continue;
            if (victim == null or entry.*.touched < victim.?.touched) victim = entry.*;
        }
        const retired = victim orelse return false;
        _ = self.mappings.remove(retired.key);
        self.mapped_bytes -= retired.value.mapping.len;
        retired.destroy();
        return true;
    }
    // Only verified immutable cache inodes enter this owner. Replacement is
    // atomic; pins keep the original mapping and disk eviction lease alive.
    // Large contiguous artifacts keep their separate disk-first lifetime.
    fn admitMapping(self: *Cache, key: []const u8, value: parquet.PersistentObjectRangeCache.MappedEntry) ImmutableLease {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        if (self.mappings.get(key)) |existing| {
            existing.refs += 1;
            var redundant = value;
            redundant.deinit();
            return .{ .mapping = existing };
        }
        if (value.mapping.len > self.max_mapped_bytes or self.max_mappings == 0) return .{ .mapped = value };
        while (value.mapping.len > self.max_mapped_bytes -| self.mapped_bytes or self.mappings.count() >= self.max_mappings) {
            var victim: ?*Mapping = null;
            var entries = self.mappings.valueIterator();
            while (entries.next()) |entry| {
                if (entry.*.refs != 0) continue;
                if (victim == null or entry.*.touched < victim.?.touched) victim = entry.*;
            }
            const retired = victim orelse return .{ .mapped = value };
            _ = self.mappings.remove(retired.key);
            self.mapped_bytes -= retired.value.mapping.len;
            retired.destroy();
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
        self.recordRead(false, bytes.len);
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
        if (bytes.len > self.max_bytes or self.max_entries == 0) return;
        const owned_key = try self.alloc.dupe(u8, key);
        defer self.alloc.free(owned_key);
        const owned_bytes = try self.alloc.dupe(u8, bytes);
        var admitted = false;
        defer if (!admitted) self.alloc.free(owned_bytes);
        const item = try self.alloc.create(Entry);
        defer if (!admitted) self.alloc.destroy(item);
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        // Concurrent misses may fetch the same immutable object range.
        if (self.entries.contains(key)) return;
        const lane = parquet.cacheLaneFromObjectRangeCacheKey(key);
        const protected = lane == .metadata or lane == .serving_sidecar;
        while (self.entries.count() != 0 and (self.stats.stored_bytes > self.max_bytes - bytes.len or self.entries.count() >= self.max_entries)) {
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
            self.stats.stored_bytes -= removed.value.bytes.len;
            if (removed.value.protected) self.stats.protected_stored_bytes -= removed.value.bytes.len;
            self.stats.evictions += 1;
            self.alloc.free(removed.key);
            self.alloc.free(removed.value.bytes);
            self.alloc.destroy(removed.value);
        }
        const map_key = try self.alloc.dupe(u8, owned_key);
        errdefer self.alloc.free(map_key);
        self.tick +%= 1;
        item.* = .{ .cache = self, .bytes = owned_bytes, .touched = self.tick, .protected = protected };
        try self.entries.put(self.alloc, map_key, item);
        admitted = true;
        self.stats.stored_bytes += bytes.len;
        if (protected) self.stats.protected_stored_bytes += bytes.len;
    }
};
pub const Reader = struct {
    cache: *Cache,
    base: ObjectReader,
    scope: [32]u8,
    context: Context,
    pending: [4]?@import("../../sql/parallel_scheduler.zig").Task(anyerror!void) = @splat(null),
    prefetch_bytes: usize = 0,
    prefetch_cancelled: std.atomic.Value(bool) = .init(false),
    /// One lookahead batch, four concurrent ranges, at most 32 MiB in flight.
    /// Workers use independent page allocations, never a SQL arena/quota.
    pub fn prefetch(self: *Reader, reads: []const ranges.RangeRead) !void {
        self.drain(false);
        const io = self.context.io orelse return;
        try self.context.ensureActive();
        self.prefetch_cancelled.store(false, .release);
        self.prefetch_bytes = 0;
        for (reads[0..@min(reads.len, self.pending.len)], 0..) |read, i| {
            if (read.range.len > 32 * 1024 * 1024 -| self.prefetch_bytes) break;
            self.prefetch_bytes += @intCast(read.range.len);
            self.pending[i] = @import("../../sql/parallel_scheduler.zig").global().submit(io, @intCast(read.range.len *| 2), warm, .{ self, read }) orelse break;
        }
    }
    fn warm(self: *Reader, read: ranges.RangeRead) anyerror!void {
        var worker: Reader = .{ .cache = self.cache, .base = self.base, .scope = self.scope, .context = self.context };
        const token: @import("../../storage/object_storage.zig").CancellationToken = .{ .ptr = self, .is_cancelled_fn = prefetchCanceled };
        worker.context.cancellation = token;
        worker.base.cancellation = token;
        if (read.purpose == .parquet_footer) {
            const lease = try worker.footerRead(read);
            defer lease.release();
        } else {
            const bytes = try readLease(&worker, std.heap.page_allocator, read);
            defer bytes.release();
        }
    }
    fn prefetchCanceled(raw: *const anyopaque) bool {
        const self: *const Reader = @ptrCast(@alignCast(raw));
        if (self.prefetch_cancelled.load(.acquire)) return true;
        self.context.ensureActive() catch return true;
        return false;
    }
    /// Prefetch failures are speculative. Required reads preserve their own
    /// errors; close cancels/joins before footer metadata or clients are freed.
    pub fn drain(self: *Reader, cancel: bool) void {
        if (cancel) self.prefetch_cancelled.store(true, .release);
        const io = self.context.io orelse return;
        for (&self.pending) |*future| if (future.*) |*active| {
            if (cancel) {
                active.cancel(io) catch {};
            } else {
                active.await(io) catch {};
            }
            future.* = null;
        };
        self.prefetch_bytes = 0;
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
                item.payload = .{ .footer = try self_loader.reader.decodeFooter(item.arena.allocator(), self_loader.read) };
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

test "external lake prefetch overlaps bounded ranges warms versions and joins on close" {
    const storage = @import("../../storage/object_storage.zig");
    const a = std.testing.allocator;
    var memory = storage.MemoryObjectStorage.init(a);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("bucket");
    var put = try client.putObject("bucket", "data", "abcdefghijklmnop", .{});
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
    var reader: Reader = .{ .cache = &cache, .base = ObjectReader.init(.{ .allocator = a, .ptr = &slow, .vtable = &slow.vtable }), .scope = @splat(0), .context = .{ .io = std.testing.io } };
    defer reader.drain(true);
    const object: ranges.ObjectRef = .{ .bucket = "bucket", .key = "data", .byte_len = 16, .version = .{ .etag = put.etag.? } };
    var reads: [4]ranges.RangeRead = undefined;
    for (&reads, 0..) |*read, i| read.* = .{ .object = object, .range = .{ .offset = i * 4, .len = 4 }, .purpose = .parquet_column_chunk };
    try reader.prefetch(&reads);
    defer slow.gate.store(true, .release);
    for (0..200) |_| {
        if (slow.entered.load(.acquire) >= 2) break;
        try std.testing.io.sleep(.fromMilliseconds(10), .awake);
    }
    try std.testing.expect(slow.entered.load(.acquire) >= 2);
    slow.gate.store(true, .release);
    reader.drain(false);
    try std.testing.expectEqual(@as(usize, 4), slow.entered.load(.acquire));
    const bytes = try reader.reader().readPlannedAlloc(a, reads[2]);
    defer a.free(bytes);
    try std.testing.expectEqualStrings("ijkl", bytes);
    try std.testing.expectEqual(@as(usize, 4), slow.entered.load(.acquire));
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
    try reader.prefetch(&reads);
    for (0..200) |_| {
        if (slow.entered.load(.acquire) > entered_before) break;
        try std.testing.io.sleep(.fromMilliseconds(10), .awake);
    }
    reader.drain(true);
    try std.testing.expect(slow.canceled.load(.acquire) > 0);
    for (reader.pending) |future| try std.testing.expect(future == null);
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

test "external lake disk cache initialization failure preserves source reads" {
    const a = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(a, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(io, .{ .sub_path = "not-a-directory", .data = "file" });
    const root = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/not-a-directory/cache", .{tmp.sub_path});
    defer a.free(root);
    var cache = Cache.init(a);
    defer cache.deinit();
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
    const retry = start + 60 * std.time.ns_per_s;
    var cache = Cache.initWithMemoryLimit(a, 0);
    defer cache.deinit();
    {
        var owner = try parquet.PersistentObjectRangeCache.init(io, root);
        defer owner.deinit();
        try cache.ensurePersistentAt(io, root, .{}, .{}, start);
        try std.testing.expect(cache.persistentCache() == null);
        try std.testing.expectEqualStrings("WouldBlock", cache.snapshot().disk_unavailable.?);
        try std.testing.expectEqual(@as(u64, 1), cache.snapshot().disk_init_failures);
        // A busy serving process must not repeat inventory/lock attempts.
        for (0..100) |_| try cache.ensurePersistentAt(io, root, .{}, .{}, retry - 1);
        try std.testing.expectEqual(@as(u64, 1), cache.snapshot().disk_init_attempts);
    }
    try cache.ensurePersistentAt(io, root, .{}, .{}, retry);
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
