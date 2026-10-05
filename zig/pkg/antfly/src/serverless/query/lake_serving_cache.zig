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
    max_bytes: usize = 64 * 1024 * 1024,
    max_entries: usize = 4096,
    stats: Stats = .{},
    tick: u64 = 0,
    persistent: ?parquet.PersistentObjectRangeCache = null,
    persistent_mutex: std.Io.Mutex = .init,
    persistent_attempted: bool = false,
    persistent_ready: std.atomic.Value(bool) = .init(false),

    /// Configure once before reads. The server owns the worker and drains it
    /// after cursors are quiescent; a request never owns cache I/O state.
    pub fn ensurePersistent(self: *Cache, io: std.Io, root: []const u8, policy: parquet.PersistentObjectRangeCachePolicy, resources: parquet.PersistentObjectRangeCacheResources) !void {
        try self.persistent_mutex.lock(io);
        defer self.persistent_mutex.unlock(io);
        try policy.validate();
        if (root.len == 0) return error.InvalidPersistentObjectRangeCachePolicy;
        if (self.persistent_attempted) return;
        self.persistent = parquet.PersistentObjectRangeCache.initWithPolicyAndResources(io, root, policy, resources) catch |err| {
            if (err == error.Canceled) return err;
            // Local cache availability is never source/readiness authority.
            // Report the reason once and continue serving through RAM/source.
            while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
            self.stats.disk_unavailable = @errorName(err);
            self.mutex.unlock();
            self.persistent_attempted = true;
            return;
        };
        self.persistent_attempted = true;
        self.persistent_ready.store(true, .release);
    }

    pub fn persistentStats(self: *Cache) ?parquet.PersistentObjectRangeCacheStats {
        // Status must not wait for startup inventory I/O. Once published, the
        // disk owner is immutable until server readers/status calls quiesce.
        if (!self.persistent_ready.load(.acquire)) return null;
        return self.persistent.?.statsSnapshot();
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
        disk_hits: u64 = 0,
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
        if (self.cache.decoded.lookup(key)) |lease| return lease;
        const lease = try self.cache.decoded.create(32 * 1024 * 1024);
        errdefer lease.release();
        const a = lease.item.arena.allocator();
        const tail_lease = try self.reader().readPlannedLease(a, read);
        defer tail_lease.release();
        const tail = tail_lease.bytes;
        const footer_api = @import("lake_parquet_footer.zig");
        const preflight = try footer_api.parseFooterPreflight(read.object.byte_len, read.range.offset, tail);
        if (preflight.metadataSlice(tail)) |bytes| {
            lease.item.payload = .{ .footer = try @import("lake_parquet_metadata.zig").parseFooterMetadataAlloc(a, bytes, read.object.byte_len) };
        } else {
            const bytes_lease = try self.reader().readPlannedLease(a, try footer_api.planFooterMetadataRead(read.object, read.range.offset, tail));
            defer bytes_lease.release();
            const bytes = bytes_lease.bytes;
            lease.item.payload = .{ .footer = try @import("lake_parquet_metadata.zig").parseFooterMetadataAlloc(a, bytes, read.object.byte_len) };
        }
        try self.context.ensureActive();
        self.cache.decoded.publish(key, lease);
        return lease;
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
        const disk_bytes = if (self.cache.persistent) |*disk| try disk.readAlloc(alloc, key, read.range.len) else null;
        const bytes = disk_bytes orelse try self.base.parquetReader().readPlannedAlloc(alloc, read);
        self.cache.recordRead(disk_bytes != null, bytes.len);
        errdefer alloc.free(bytes);
        try self.context.ensureActive();
        // Cache admission is optional and never turns a successful read into
        // an allocation failure in a long-lived shared owner.
        if (disk_bytes == null) if (self.cache.persistent) |*disk| {
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
