// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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

//! Schema-independent immutable plans only: authorization, catalog resolution,
//! parameter values and bound plans MUST remain request-local. The allocator
//! must support concurrent calls; the owner and allocator outlive all leases.
//! Shutdown drains acquisitions/leases before deinit. All mutex operations use
//! std.Io; callers must use compatible Io executors for this shared mutex.
const std = @import("std");
const compiler = @import("antfly_local_sources").sql_compiler;
const Budget = @import("antfly_local_sources").sql_memory_budget;
const Digest = [32]u8;

pub const Config = struct {
    max_entries: usize = 256,
    max_bytes: usize = 32 << 20,
    max_compile_bytes: usize = 4 << 20,
    max_concurrent_compiles: usize = 4,
    compiler_limits: compiler.Limits = .{},
};
pub const Control = struct {
    work: compiler.WorkControl,
    wait: ?*const fn (*anyopaque, std.Io) error{SqlPreparationInterrupted}!void = null,
};
pub const Key = struct {
    statement: []const u8,
    principal: []const u8 = "",
    database: []const u8 = "",
    namespace: []const u8 = "",
};
pub const Stats = struct {
    hits: u64 = 0,
    misses: u64 = 0,
    evictions: u64 = 0,
    race_hits: u64 = 0,
    busy: u64 = 0,
    entries: usize = 0,
    compiling: usize = 0,
    leases: usize = 0,
    /// Includes fixed hash buckets, live entries, and full in-flight reservations.
    bytes: usize = 0,
};
const Entry = struct {
    digest: Digest,
    budget: Budget,
    compiled: compiler.Compiled = undefined,
    refs: usize = 0,
    /// Returned loans remain represented in refs until the cache reconciles
    /// them. The cache owns this metadata and the complete compiled region.
    returned: std.atomic.Value(usize) = .init(0),
    bucket_next: ?*Entry = null,
    prev: ?*Entry = null,
    next: ?*Entry = null,
    tracked_prev: ?*Entry = null,
    tracked_next: ?*Entry = null,
};
pub const Lease = struct {
    cache: *Cache,
    entry: *Entry,

    pub fn compiled(self: *const Lease) *const compiler.Compiled {
        return &self.entry.compiled;
    }
    /// Move-only lease; release exactly once, even on cancellation/error.
    pub fn release(self: *Lease, io: std.Io) void {
        const cache = self.cache;
        const entry = self.entry;
        if (cache.mutex.tryLock()) {
            cache.reconcileEntry(entry);
            std.debug.assert(entry.refs != 0);
            entry.refs -= 1;
            cache.counters.leases -= 1;
            _ = cache.live_leases.fetchSub(1, .release);
            if (entry.refs == 0) cache.appendIdle(entry);
            self.* = undefined;
            cache.mutex.unlock(io);
            return;
        }
        // The original loan pins entry until this final atomic publication.
        // No entry access follows it: reconciliation can make it evictable.
        // Shutdown joins every acquisition/release before deinitializing cache.
        _ = cache.live_leases.fetchSub(1, .release);
        _ = entry.returned.fetchAdd(1, .release);
        self.* = undefined;
    }
};

pub const Cache = struct {
    allocator: std.mem.Allocator,
    /// Immutable for the initialized cache's lifetime.
    config: Config,
    mutex: std.Io.Mutex = .init,
    buckets: []?*Entry = &.{},
    oldest: ?*Entry = null,
    newest: ?*Entry = null,
    counters: Stats = .{},
    live_leases: std.atomic.Value(usize) = .init(0),
    tracked_head: ?*Entry = null,
    retirement_cursor: ?*Entry = null,
    returned_compiles: std.atomic.Value(usize) = .init(0),
    retired_entries: std.atomic.Value(usize) = .init(0),
    retired_bytes: std.atomic.Value(usize) = .init(0),
    initialized: bool = false,

    /// Lazy allocation permits embedding in an infallible server constructor.
    /// Keep the Cache address stable after its first acquisition.
    pub fn init(allocator: std.mem.Allocator, config: Config) Cache {
        return .{ .allocator = allocator, .config = config };
    }

    pub fn stats(self: *Cache, io: std.Io) Stats {
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        self.reconcileRefunds();
        var result = self.counters;
        result.leases = self.live_leases.load(.acquire);
        return result;
    }

    pub fn deinit(self: *Cache, io: std.Io) void {
        if (!self.initialized) {
            self.* = undefined;
            return;
        }
        self.mutex.lockUncancelable(io);
        self.reconcileRefunds();
        _ = self.reconcileReturns(self.config.max_entries);
        std.debug.assert(self.counters.leases == 0 and self.counters.compiling == 0);
        while (self.oldest) |entry| {
            self.removeIdle(entry);
            self.removeTracked(entry);
            self.destroy(entry);
        }
        self.allocator.free(self.buckets);
        self.mutex.unlock(io);
        self.* = undefined;
    }

    pub fn acquire(self: *Cache, io: std.Io, key: Key, diagnostic: *compiler.Diagnostic) !Lease {
        return self.acquireControlled(io, key, diagnostic, null);
    }

    pub fn acquireControlled(self: *Cache, io: std.Io, key: Key, diagnostic: *compiler.Diagnostic, control: ?Control) !Lease {
        diagnostic.* = .{};
        try io.checkCancel();
        // Reject before hashing arbitrary request bytes or touching cache state.
        if (key.statement.len > self.config.compiler_limits.max_bytes) {
            diagnostic.message = "SQL statement byte budget exceeded";
            return error.SqlLimitExceeded;
        }
        if (key.principal.len > 4096 or key.database.len > 4096 or key.namespace.len > 4096)
            return error.SqlLimitExceeded;
        const work = if (control) |value| value.work else null;
        const digest = try hashControlled(key, work);
        try self.lockForPreparation(io, control);
        var locked = true;
        defer if (locked) self.mutex.unlock(io);
        self.reconcileRefunds();
        try self.ensureBuckets();
        const reservation = self.config.max_compile_bytes + @sizeOf(Entry);
        var reconciled = self.reconcileReturns(64);
        while (true) {
            if (self.find(digest)) |entry| {
                self.counters.hits +%= 1;
                return self.lease(entry);
            }
            if (self.counters.compiling >= self.config.max_concurrent_compiles) return self.busy();
            if (self.counters.entries + self.counters.compiling < self.config.max_entries and
                reservation <= self.config.max_bytes - self.counters.bytes) break;
            if (self.oldest == null and reconciled < self.counters.entries) {
                // No cursor into cache storage escapes the mutex. This cursor
                // belongs to the cache and removal repairs it under the lock.
                self.mutex.unlock(io);
                locked = false;
                if (work) |value| try value.checkpoint(value.ptr);
                try self.lockForPreparation(io, control);
                locked = true;
                self.reconcileRefunds();
                const scanned = self.reconcileReturns(64);
                if (scanned == 0) return self.busy();
                reconciled += scanned;
                continue;
            }
            const victim = self.oldest orelse return self.busy();
            self.removeIdle(victim);
            self.removeBucket(victim);
            self.removeTracked(victim);
            const reclaimed = @sizeOf(Entry) + victim.budget.live;
            // Keep bytes/entry charged while freeing outside the mutex. Another
            // miss cannot reserve memory that has not actually been released.
            self.mutex.unlock(io);
            locked = false;
            self.destroy(victim);
            // Publish refunds only after all arena and entry buffers are free.
            // Counts can lag bytes briefly; both describe cache-owned cleanup
            // and are reconciled without retaining this request's authority.
            _ = self.retired_bytes.fetchAdd(reclaimed, .release);
            _ = self.retired_entries.fetchAdd(1, .release);
            if (work) |value| try value.checkpoint(value.ptr);
            try self.lockForPreparation(io, control);
            locked = true;
            self.reconcileRefunds();
        }
        self.counters.misses +%= 1;
        self.counters.compiling += 1;
        self.counters.bytes += reservation;
        self.mutex.unlock(io);
        locked = false;
        // This publication is mandatory even after cancellation. Its fixed
        // refund remains cache-owned; it never waits for the publication mutex.
        var reserved = true;
        defer if (reserved) {
            _ = self.returned_compiles.fetchAdd(1, .release);
        };
        const entry = try self.allocator.create(Entry);
        errdefer self.allocator.destroy(entry);
        entry.* = .{ .digest = digest, .budget = .{ .backing = self.allocator, .limit = self.config.max_compile_bytes } };
        entry.compiled = compiler.compileDiagnosticControlled(entry.budget.allocator(), key.statement, self.config.compiler_limits, diagnostic, work) catch |err| {
            if (err == error.OutOfMemory and entry.budget.exhausted) {
                diagnostic.message = "SQL compilation memory budget exceeded";
                return error.SqlLimitExceeded;
            }
            return err;
        };
        errdefer entry.compiled.deinit();
        try io.checkCancel();
        if (work) |value| try value.checkpoint(value.ptr);
        try self.lockForPreparation(io, control);
        locked = true;
        self.reconcileRefunds();
        if (self.find(digest)) |winner| {
            const result = self.lease(winner);
            self.counters.race_hits +%= 1;
            // Free losing compiled data without holding the publication mutex.
            self.mutex.unlock(io);
            locked = false;
            self.destroy(entry);
            return result;
        }
        self.counters.compiling -= 1;
        self.counters.entries += 1;
        self.counters.bytes -= reservation - (@sizeOf(Entry) + entry.budget.live);
        reserved = false;
        const bucket_index = self.bucket(digest);
        entry.bucket_next = self.buckets[bucket_index];
        self.buckets[bucket_index] = entry;
        // New entries are not on the idle list until their first release.
        entry.refs = 1;
        self.counters.leases += 1;
        _ = self.live_leases.fetchAdd(1, .release);
        self.track(entry);
        return .{ .cache = self, .entry = entry };
    }

    fn lockForPreparation(self: *Cache, io: std.Io, control: ?Control) !void {
        if (control) |value| {
            if (value.wait) |wait| {
                while (true) {
                    try value.work.checkpoint(value.work.ptr);
                    if (self.mutex.tryLock()) return;
                    try wait(value.work.ptr, io);
                }
            }
        }
        try self.mutex.lock(io);
    }

    fn ensureBuckets(self: *Cache) !void {
        if (self.buckets.len != 0) return;
        if (self.config.max_entries == 0 or self.config.max_entries > 65536 or
            self.config.max_compile_bytes == 0 or self.config.max_concurrent_compiles == 0)
            return error.InvalidSqlPlanCacheConfig;
        const count = std.math.ceilPowerOfTwoAssert(usize, self.config.max_entries * 2);
        const metadata = count * @sizeOf(?*Entry);
        if (metadata >= self.config.max_bytes or self.config.max_compile_bytes > self.config.max_bytes - metadata or
            @sizeOf(Entry) > self.config.max_bytes - metadata - self.config.max_compile_bytes)
            return error.InvalidSqlPlanCacheConfig;
        self.buckets = try self.allocator.alloc(?*Entry, count);
        @memset(self.buckets, null);
        self.counters.bytes = metadata;
        self.initialized = true;
    }
    fn busy(self: *Cache) error{SqlPlanCacheBusy} {
        self.counters.busy +%= 1;
        return error.SqlPlanCacheBusy;
    }
    fn bucket(self: *Cache, digest: Digest) usize {
        return @as(usize, @truncate(std.mem.readInt(u64, digest[0..8], .little))) & (self.buckets.len - 1);
    }
    fn find(self: *Cache, digest: Digest) ?*Entry {
        var cursor = self.buckets[self.bucket(digest)];
        while (cursor) |entry| : (cursor = entry.bucket_next) {
            if (std.mem.eql(u8, &entry.digest, &digest)) return entry;
        }
        return null;
    }
    fn removeBucket(self: *Cache, entry: *Entry) void {
        var cursor = &self.buckets[self.bucket(entry.digest)];
        while (cursor.*.? != entry) cursor = &cursor.*.?.bucket_next;
        cursor.* = entry.bucket_next;
    }
    fn lease(self: *Cache, entry: *Entry) Lease {
        self.reconcileEntry(entry);
        if (entry.refs == 0) self.removeIdle(entry);
        entry.refs += 1;
        self.counters.leases += 1;
        _ = self.live_leases.fetchAdd(1, .release);
        return .{ .cache = self, .entry = entry };
    }
    fn reconcileEntry(self: *Cache, entry: *Entry) void {
        const returned = entry.returned.swap(0, .acquire);
        if (returned == 0) return;
        std.debug.assert(returned <= entry.refs);
        entry.refs -= returned;
        self.counters.leases -= returned;
        if (entry.refs == 0) self.appendIdle(entry);
    }
    fn reconcileRefunds(self: *Cache) void {
        const compiles = self.returned_compiles.swap(0, .acquire);
        const entries = self.retired_entries.swap(0, .acquire);
        const bytes = self.retired_bytes.swap(0, .acquire);
        std.debug.assert(compiles <= self.counters.compiling and entries <= self.counters.entries);
        self.counters.compiling -= compiles;
        self.counters.entries -= entries;
        self.counters.bytes -= compiles * (self.config.max_compile_bytes + @sizeOf(Entry)) + bytes;
        self.counters.evictions +%= entries;
    }
    /// Fixed work under the cache mutex. Published entries and this cursor are
    /// cache-owned; physically removed entries repair the cursor before free.
    fn reconcileReturns(self: *Cache, limit: usize) usize {
        var scanned: usize = 0;
        const bound = @min(limit, self.counters.entries);
        while (scanned < bound) : (scanned += 1) {
            const entry = self.retirement_cursor orelse self.tracked_head orelse break;
            self.retirement_cursor = entry.tracked_next orelse self.tracked_head;
            self.reconcileEntry(entry);
        }
        return scanned;
    }
    fn track(self: *Cache, entry: *Entry) void {
        entry.tracked_next = self.tracked_head;
        if (self.tracked_head) |head| head.tracked_prev = entry;
        self.tracked_head = entry;
    }
    fn removeTracked(self: *Cache, entry: *Entry) void {
        if (self.retirement_cursor == entry) self.retirement_cursor = entry.tracked_next;
        if (entry.tracked_prev) |previous| previous.tracked_next = entry.tracked_next else self.tracked_head = entry.tracked_next;
        if (entry.tracked_next) |next| next.tracked_prev = entry.tracked_prev;
        if (self.retirement_cursor == null) self.retirement_cursor = self.tracked_head;
        entry.tracked_prev = null;
        entry.tracked_next = null;
    }
    fn appendIdle(self: *Cache, entry: *Entry) void {
        entry.prev = self.newest;
        entry.next = null;
        if (self.newest) |previous| previous.next = entry else self.oldest = entry;
        self.newest = entry;
    }
    fn removeIdle(self: *Cache, entry: *Entry) void {
        if (entry.prev) |previous| previous.next = entry.next else self.oldest = entry.next;
        if (entry.next) |next| next.prev = entry.prev else self.newest = entry.prev;
        entry.prev = null;
        entry.next = null;
    }
    fn destroy(self: *Cache, entry: *Entry) void {
        entry.compiled.deinit();
        std.debug.assert(entry.budget.live == 0);
        self.allocator.destroy(entry);
    }
};

fn hash(key: Key) Digest {
    return hashControlled(key, null) catch unreachable;
}

fn hashControlled(key: Key, control: ?compiler.WorkControl) error{SqlPreparationInterrupted}!Digest {
    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    for ([_][]const u8{ "antfly.sql.compiled-plan.v1", key.principal, key.database, key.namespace, key.statement }) |field| {
        var length: [8]u8 = undefined;
        std.mem.writeInt(u64, &length, field.len, .little);
        hasher.update(&length);
        var offset: usize = 0;
        while (offset < field.len) {
            if (control) |work| try work.checkpoint(work.ptr);
            const end = offset + @min(@as(usize, 4096), field.len - offset);
            hasher.update(field[offset..end]);
            offset = end;
        }
    }
    return hasher.finalResult();
}

test "SQL plan cache leases survive churn, scope partitions and idle LRU is bounded" {
    const io = std.testing.io;
    var cache = Cache.init(std.testing.allocator, .{ .max_entries = 2 });
    defer cache.deinit(io);
    var diagnostic: compiler.Diagnostic = .{};
    const key: Key = .{ .statement = "SELECT _id FROM t WHERE _id=$1", .principal = "alice" };
    var pinned = try cache.acquire(io, key, &diagnostic);
    const identity = pinned.compiled();
    var same = try cache.acquire(io, key, &diagnostic);
    try std.testing.expect(identity == same.compiled());
    same.release(io);
    var other = try cache.acquire(io, .{ .statement = key.statement, .principal = "bob" }, &diagnostic);
    try std.testing.expect(other.compiled() != identity);
    try std.testing.expectError(error.SqlPlanCacheBusy, cache.acquire(io, .{ .statement = "SELECT * FROM u" }, &diagnostic));
    other.release(io);
    var replacement = try cache.acquire(io, .{ .statement = "SELECT * FROM u" }, &diagnostic);
    replacement.release(io);
    try std.testing.expectEqual(@as(u32, 1), pinned.compiled().parameter_count);
    pinned.release(io);
    const stats = cache.stats(io);
    try std.testing.expectEqual(@as(usize, 2), stats.entries);
    try std.testing.expectEqual(@as(u64, 1), stats.evictions);
    try std.testing.expect(stats.bytes <= cache.config.max_bytes);
    try std.testing.expectEqual(@as(usize, 0), stats.leases);
}

test "SQL plan cache final loan return under contended mutex preserves physical cache charge" {
    const io = std.testing.io;
    var backing: Budget = .{ .backing = std.testing.allocator, .limit = 32 << 20 };
    var cache = Cache.init(backing.allocator(), .{ .max_entries = 1 });
    defer cache.deinit(io);
    var diagnostic: compiler.Diagnostic = .{};
    var loan = try cache.acquire(io, .{ .statement = "SELECT 'cache-owned value'" }, &diagnostic);
    const entry = loan.entry;
    const charged = cache.stats(io).bytes;
    {
        cache.mutex.lockUncancelable(io);
        defer cache.mutex.unlock(io);
        loan.release(io); // Must return on this same thread while the lock is held.
        try std.testing.expectEqual(@as(usize, 1), entry.refs);
        try std.testing.expectEqual(@as(usize, 1), entry.returned.load(.acquire));
        try std.testing.expectEqual(@as(usize, 0), cache.live_leases.load(.acquire));
        try std.testing.expectEqual(charged, cache.counters.bytes);
        try std.testing.expectEqual(charged, backing.live);
    }
    var replacement = try cache.acquire(io, .{ .statement = "SELECT 'replacement'" }, &diagnostic);
    replacement.release(io);
    try std.testing.expectEqual(@as(u64, 1), cache.stats(io).evictions);
    try std.testing.expectEqual(@as(usize, 0), cache.stats(io).leases);
    try std.testing.expectEqual(backing.live, cache.stats(io).bytes);
}

test "SQL plan cache reconciles contended loan returns without retiring a live borrower" {
    const io = std.testing.io;
    var cache = Cache.init(std.testing.allocator, .{ .max_entries = 1 });
    defer cache.deinit(io);
    var diagnostic: compiler.Diagnostic = .{};
    const key: Key = .{ .statement = "SELECT 17" };
    var first = try cache.acquire(io, key, &diagnostic);
    var retained = try cache.acquire(io, key, &diagnostic);
    const program = retained.compiled();
    cache.mutex.lockUncancelable(io);
    first.release(io);
    cache.mutex.unlock(io);
    var third = try cache.acquire(io, key, &diagnostic);
    try std.testing.expect(third.compiled() == program);
    try std.testing.expectEqual(@as(usize, 2), cache.stats(io).leases);
    try std.testing.expectError(error.SqlPlanCacheBusy, cache.acquire(io, .{ .statement = "SELECT 18" }, &diagnostic));
    cache.mutex.lockUncancelable(io);
    third.release(io);
    retained.release(io);
    cache.mutex.unlock(io);
    try std.testing.expectEqual(@as(usize, 0), cache.stats(io).leases);
    // deinit owns and reconciles both pending returns before physical free.
}

test "SQL plan cache bounded retirement batches find a returned loan beyond the first quantum" {
    const io = std.testing.io;
    var cache = Cache.init(std.testing.allocator, .{ .max_entries = 130 });
    defer cache.deinit(io);
    var diagnostic: compiler.Diagnostic = .{};
    var loans: [130]Lease = undefined;
    for (&loans, 0..) |*loan, index| {
        var buffer: [32]u8 = undefined;
        const statement = try std.fmt.bufPrint(&buffer, "SELECT {d}", .{index});
        loan.* = try cache.acquire(io, .{ .statement = statement }, &diagnostic);
    }
    const Probe = struct {
        cache: *Cache,
        unlocked_checks: usize = 0,
        fn checkpoint(raw: *anyopaque) error{SqlPreparationInterrupted}!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (!self.cache.mutex.tryLock()) return error.SqlPreparationInterrupted;
            self.unlocked_checks += 1;
            self.cache.mutex.unlock(std.testing.io);
        }
    };
    // Position a cold miss before 129 pinned entries. Its only evictable entry
    // is past two quanta, so the miss must checkpoint without holding the lock.
    cache.mutex.lockUncancelable(io);
    cache.retirement_cursor = cache.tracked_head;
    loans[0].release(io);
    cache.mutex.unlock(io);
    var probe: Probe = .{ .cache = &cache };
    var replacement = try cache.acquireControlled(io, .{ .statement = "SELECT 999" }, &diagnostic, .{ .work = .{ .ptr = &probe, .checkpoint = Probe.checkpoint } });
    replacement.release(io);
    for (loans[1..]) |*loan| loan.release(io);
    try std.testing.expect(probe.unlocked_checks >= 3);
    try std.testing.expectEqual(@as(u64, 1), cache.stats(io).evictions);
    try std.testing.expectEqual(@as(usize, 0), cache.stats(io).leases);
}

test "SQL plan cache concurrent contended returns preserve every loan until owner reconciliation" {
    const io = std.testing.io;
    var cache = Cache.init(std.testing.allocator, .{ .max_entries = 1 });
    defer cache.deinit(io);
    var diagnostic: compiler.Diagnostic = .{};
    const key: Key = .{ .statement = "SELECT 'immutable shared plan'" };
    var loans: [128]Lease = undefined;
    for (&loans) |*loan| loan.* = try cache.acquire(io, key, &diagnostic);
    const entry = loans[0].entry;
    const program = loans[0].compiled();
    const Worker = struct {
        fn run(batch: []Lease) void {
            for (batch) |*loan| loan.release(std.testing.io);
        }
    };
    {
        var workers: [8]std.Io.Future(void) = undefined;
        var launched: usize = 0;
        var joined = false;
        cache.mutex.lockUncancelable(io);
        defer cache.mutex.unlock(io);
        defer if (!joined) {
            for (workers[0..launched]) |*worker| worker.await(io);
            for (loans[launched * 16 ..]) |*loan| loan.release(io);
        };
        for (&workers, 0..) |*worker, index| {
            worker.* = try io.concurrent(Worker.run, .{loans[index * 16 ..][0..16]});
            launched += 1;
        }
        for (&workers) |*worker| worker.await(io);
        joined = true;
        try std.testing.expectEqual(@as(usize, 128), entry.refs);
        try std.testing.expectEqual(@as(usize, 128), entry.returned.load(.acquire));
        try std.testing.expectEqual(@as(usize, 0), cache.live_leases.load(.acquire));
    }
    var reused = try cache.acquire(io, key, &diagnostic);
    try std.testing.expect(reused.compiled() == program);
    try std.testing.expectEqual(@as(usize, 1), entry.refs);
    try std.testing.expectEqual(@as(usize, 0), entry.returned.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), cache.stats(io).leases);
    reused.release(io);
}

test "SQL plan cache interrupted compilation returns reservation under held publication mutex" {
    const Probe = struct {
        cache: *Cache,
        checks: usize = 0,
        fail_at: ?usize = null,
        held: bool = false,
        fn checkpoint(raw: *anyopaque) error{SqlPreparationInterrupted}!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
            if (self.fail_at == self.checks) {
                self.cache.mutex.lockUncancelable(std.testing.io);
                self.held = true;
                return error.SqlPreparationInterrupted;
            }
        }
        fn control(self: *@This()) Control {
            return .{ .work = .{ .ptr = self, .checkpoint = checkpoint } };
        }
    };
    const io = std.testing.io;
    const key: Key = .{ .statement = "SELECT 'owned semantic region', ARRAY[1,2,3]" };
    var diagnostic: compiler.Diagnostic = .{};
    var baseline = Cache.init(std.testing.allocator, .{});
    defer baseline.deinit(io);
    var successful: Probe = .{ .cache = &baseline };
    var loan = try baseline.acquireControlled(io, key, &diagnostic, successful.control());
    loan.release(io);
    try std.testing.expect(successful.checks > 1);
    var backing: Budget = .{ .backing = std.testing.allocator, .limit = 32 << 20 };
    var interrupted = Cache.init(backing.allocator(), .{});
    defer interrupted.deinit(io);
    var fault: Probe = .{ .cache = &interrupted, .fail_at = successful.checks };
    defer if (fault.held) interrupted.mutex.unlock(io);
    try std.testing.expectError(error.SqlPreparationInterrupted, interrupted.acquireControlled(io, key, &diagnostic, fault.control()));
    try std.testing.expect(fault.held);
    const metadata = interrupted.buckets.len * @sizeOf(?*Entry);
    const reservation = interrupted.config.max_compile_bytes + @sizeOf(Entry);
    try std.testing.expectEqual(@as(usize, 1), interrupted.counters.compiling);
    try std.testing.expectEqual(@as(usize, 1), interrupted.returned_compiles.load(.acquire));
    try std.testing.expectEqual(metadata + reservation, interrupted.counters.bytes);
    // All semantic/entry buffers are physically gone before the atomic refund.
    try std.testing.expectEqual(metadata, backing.live);
    interrupted.mutex.unlock(io);
    fault.held = false;
    const stats = interrupted.stats(io);
    try std.testing.expectEqual(@as(usize, 0), stats.compiling);
    try std.testing.expectEqual(metadata, stats.bytes);
}

test "SQL plan cache interrupted eviction keeps physical refund owned without mutex reacquisition" {
    const Probe = struct {
        cache: *Cache,
        held: bool = false,
        fn checkpoint(raw: *anyopaque) error{SqlPreparationInterrupted}!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.cache.retired_entries.load(.acquire) != 0) {
                self.cache.mutex.lockUncancelable(std.testing.io);
                self.held = true;
                return error.SqlPreparationInterrupted;
            }
        }
    };
    const io = std.testing.io;
    var backing: Budget = .{ .backing = std.testing.allocator, .limit = 32 << 20 };
    var cache = Cache.init(backing.allocator(), .{ .max_entries = 1 });
    defer cache.deinit(io);
    var diagnostic: compiler.Diagnostic = .{};
    var loan = try cache.acquire(io, .{ .statement = "SELECT 'evicted physical region'" }, &diagnostic);
    loan.release(io);
    const original_charge = cache.stats(io).bytes;
    var probe: Probe = .{ .cache = &cache };
    defer if (probe.held) cache.mutex.unlock(io);
    try std.testing.expectError(error.SqlPreparationInterrupted, cache.acquireControlled(io, .{ .statement = "SELECT 'never compiled'" }, &diagnostic, .{ .work = .{ .ptr = &probe, .checkpoint = Probe.checkpoint } }));
    try std.testing.expect(probe.held);
    const metadata = cache.buckets.len * @sizeOf(?*Entry);
    try std.testing.expectEqual(metadata, backing.live);
    try std.testing.expectEqual(original_charge, cache.counters.bytes);
    try std.testing.expectEqual(original_charge - metadata, cache.retired_bytes.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), cache.retired_entries.load(.acquire));
    cache.mutex.unlock(io);
    probe.held = false;
    const stats = cache.stats(io);
    try std.testing.expectEqual(@as(usize, 0), stats.entries);
    try std.testing.expectEqual(@as(usize, 0), stats.compiling);
    try std.testing.expectEqual(@as(u64, 1), stats.evictions);
    try std.testing.expectEqual(metadata, stats.bytes);
}

test "SQL plan cache failures preserve diagnostics and reclaim all admission" {
    const io = std.testing.io;
    var cache = Cache.init(std.testing.allocator, .{ .max_compile_bytes = 128 });
    defer cache.deinit(io);
    var diagnostic: compiler.Diagnostic = .{};
    try std.testing.expectError(error.SqlLimitExceeded, cache.acquire(io, .{ .statement = "SELECT * FROM t" }, &diagnostic));
    try std.testing.expectEqualStrings("SQL compilation memory budget exceeded", diagnostic.message);
    try std.testing.expectEqual(@as(usize, 0), cache.stats(io).compiling);
    var normal = Cache.init(std.testing.allocator, .{});
    defer normal.deinit(io);
    try std.testing.expectError(error.UnsupportedSqlShape, normal.acquire(io, .{ .statement = "SELECT * FROM t FOR UPDATE" }, &diagnostic));
    try std.testing.expect(diagnostic.start > 0);
    try std.testing.expectEqual(@as(usize, 0), normal.stats(io).entries);
    try std.testing.expectEqual(@as(usize, 0), normal.stats(io).compiling);
}

test "SQL plan cache bounds actual live allocation through byte-driven eviction" {
    // Compiler arena growth varies with optimization and Zig's layouts. Measure
    // one representative plan, then constrain the cache to fewer plans than its
    // entry limit rather than assuming that compilation fits in 4 KiB.
    var compile_budget: Budget = .{ .backing = std.testing.allocator, .limit = 1024 * 1024 };
    var probe = try compiler.compile(compile_budget.allocator(), "SELECT _id,name,age FROM t WHERE age>29", .{});
    const retained_bytes = compile_budget.live;
    // Leave room for arena growth when its backing allocator is another
    // admission budget rather than the testing allocator used by this probe.
    const compile_bytes = 2 * std.math.ceilPowerOfTwoAssert(usize, compile_budget.peak);
    probe.deinit();
    try std.testing.expectEqual(@as(usize, 0), compile_budget.live);
    const cache_bytes = compile_bytes + @sizeOf(Entry) + 2 * (retained_bytes + @sizeOf(Entry)) + 16 * @sizeOf(?*Entry);
    var backing: Budget = .{ .backing = std.testing.allocator, .limit = cache_bytes };
    var cache = Cache.init(backing.allocator(), .{ .max_entries = 8, .max_bytes = cache_bytes, .max_compile_bytes = compile_bytes });
    defer cache.deinit(std.testing.io);
    var diagnostic: compiler.Diagnostic = .{};
    for (0..30) |i| {
        var buffer: [128]u8 = undefined;
        const statement = try std.fmt.bufPrint(&buffer, "SELECT _id,name,age FROM t WHERE age>{d}", .{i});
        var lease = try cache.acquire(std.testing.io, .{ .statement = statement }, &diagnostic);
        lease.release(std.testing.io);
        try std.testing.expectEqual(backing.live, cache.stats(std.testing.io).bytes);
    }
    try std.testing.expect(cache.stats(std.testing.io).evictions > 0);
    try std.testing.expect(cache.stats(std.testing.io).entries < cache.config.max_entries);
    try std.testing.expect(backing.peak <= cache.config.max_bytes);
    try std.testing.expect(!backing.exhausted);
}

test "SQL plan cache releases partial allocations on failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, struct {
        fn check(alloc: std.mem.Allocator) !void {
            var cache = Cache.init(alloc, .{});
            defer cache.deinit(std.testing.io);
            var diagnostic: compiler.Diagnostic = .{};
            var lease = try cache.acquire(std.testing.io, .{ .statement = "SELECT name FROM users WHERE age > $1" }, &diagnostic);
            lease.release(std.testing.io);
        }
    }.check, .{});
}

test "SQL plan cache cancellation before publication releases the compiled plan" {
    const Cancellation = struct {
        var checks: usize = 0;
        fn check(_: ?*anyopaque) std.Io.Cancelable!void {
            checks += 1;
            if (checks == 2) return error.Canceled;
        }
    };
    Cancellation.checks = 0;
    var vtable = std.testing.io.vtable.*;
    vtable.checkCancel = Cancellation.check;
    var io = std.testing.io;
    io.vtable = &vtable;
    var cache = Cache.init(std.testing.allocator, .{});
    defer cache.deinit(io);
    var diagnostic: compiler.Diagnostic = .{};
    try std.testing.expectError(error.Canceled, cache.acquire(io, .{ .statement = "SELECT name FROM t" }, &diagnostic));
    const observed = cache.stats(io);
    try std.testing.expectEqual(@as(usize, 0), observed.compiling);
    try std.testing.expectEqual(@as(usize, 0), observed.entries);
    try std.testing.expectEqual(cache.buckets.len * @sizeOf(?*Entry), observed.bytes);
}

test "SQL plan cache racing misses publish one owned plan and return both reservations" {
    const Gate = struct {
        arrivals: std.atomic.Value(u32) = .init(0),
        ready: std.Io.Event = .unset,
        fn alloc(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (len == @sizeOf(Entry)) {
                if (self.arrivals.fetchAdd(1, .acq_rel) == 0) self.ready.waitUncancelable(std.testing.io) else self.ready.set(std.testing.io);
            }
            return std.testing.allocator.rawAlloc(len, alignment, ra);
        }
        fn free(_: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ra: usize) void {
            std.testing.allocator.rawFree(bytes, alignment, ra);
        }
        fn allocator(self: *@This()) std.mem.Allocator {
            return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = std.mem.Allocator.noResize, .remap = std.mem.Allocator.noRemap, .free = free } };
        }
    };
    const Worker = struct {
        cache: *Cache,
        address: ?*const compiler.Compiled = null,
        failure: ?anyerror = null,
        fn run(self: *@This()) void {
            var diagnostic: compiler.Diagnostic = .{};
            var acquired = self.cache.acquire(std.testing.io, .{ .statement = "SELECT name FROM t WHERE age=$1" }, &diagnostic) catch |err| {
                self.failure = err;
                return;
            };
            self.address = acquired.compiled();
            acquired.release(std.testing.io);
        }
    };
    var gate: Gate = .{};
    var cache = Cache.init(gate.allocator(), .{});
    defer cache.deinit(std.testing.io);
    var first: Worker = .{ .cache = &cache };
    var second: Worker = .{ .cache = &cache };
    var first_task = try std.testing.io.concurrent(Worker.run, .{&first});
    errdefer {
        gate.ready.set(std.testing.io);
        first_task.await(std.testing.io);
    }
    var second_task = try std.testing.io.concurrent(Worker.run, .{&second});
    first_task.await(std.testing.io);
    second_task.await(std.testing.io);
    try std.testing.expect(first.failure == null and second.failure == null);
    try std.testing.expect(first.address != null and first.address == second.address);
    const observed = cache.stats(std.testing.io);
    try std.testing.expectEqual(@as(u64, 1), observed.race_hits);
    try std.testing.expectEqual(@as(usize, 1), observed.entries);
    try std.testing.expectEqual(@as(usize, 0), observed.compiling);
    try std.testing.expectEqual(@as(usize, 0), observed.leases);
    try std.testing.expect(observed.bytes < cache.config.max_compile_bytes);
}

test "SQL plan cache late preparation interruption retires semantic memory and reservations before publication" {
    const RetirementProbe = struct {
        backing: std.mem.Allocator,
        owner: *Cache = undefined,
        armed: bool = false,
        frees: usize = 0,
        freed_bytes: usize = 0,
        early_refund: bool = false,
        held_mutex: bool = false,
        fn allocator(self: *@This()) std.mem.Allocator {
            return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
        }
        fn alloc(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return self.backing.rawAlloc(len, alignment, ra);
        }
        fn resize(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) bool {
            return false;
        }
        fn remap(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) ?[*]u8 {
            return null;
        }
        fn observe(self: *@This()) void {
            if (!self.owner.mutex.tryLock()) {
                self.held_mutex = true;
                return;
            }
            defer self.owner.mutex.unlock(std.testing.io);
            const reservation = self.owner.config.max_compile_bytes + @sizeOf(Entry);
            const metadata = self.owner.buckets.len * @sizeOf(?*Entry);
            if (self.owner.counters.compiling != 1 or self.owner.counters.bytes != metadata + reservation)
                self.early_refund = true;
        }
        fn free(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ra: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.armed) self.observe();
            self.backing.rawFree(bytes, alignment, ra);
            if (self.armed) {
                // The reservation must still be present after the backing
                // allocator has physically finished freeing this buffer.
                self.observe();
                self.frees += 1;
                self.freed_bytes += bytes.len;
            }
        }
    };
    const Probe = struct {
        owner: *Cache,
        checks: usize = 0,
        fail_at: ?usize = null,
        held_mutex: bool = false,
        retirement: ?*RetirementProbe = null,
        fn checkpoint(raw: *anyopaque) error{SqlPreparationInterrupted}!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
            if (!self.owner.mutex.tryLock()) {
                self.held_mutex = true;
                return error.SqlPreparationInterrupted;
            }
            self.owner.mutex.unlock(std.testing.io);
            if (self.fail_at == self.checks) {
                if (self.retirement) |retirement| retirement.armed = true;
                return error.SqlPreparationInterrupted;
            }
        }
        fn control(self: *@This()) Control {
            return .{ .work = .{ .ptr = self, .checkpoint = checkpoint } };
        }
    };
    const io = std.testing.io;
    const key: Key = .{ .statement = "SELECT 'retained semantic value'", .principal = "alice" };
    var diagnostic: compiler.Diagnostic = .{};
    var last_checkpoint: usize = 0;
    var no_resize = std.testing.FailingAllocator.init(std.testing.allocator, .{ .resize_fail_index = 0 });
    {
        var owner = Cache.init(no_resize.allocator(), .{});
        defer owner.deinit(io);
        var probe: Probe = .{ .owner = &owner };
        var lease = try owner.acquireControlled(io, key, &diagnostic, probe.control());
        defer lease.release(io);
        try std.testing.expect(!probe.held_mutex);
        last_checkpoint = probe.checks;
        try std.testing.expect(last_checkpoint > 1);
    }
    // The final checkpoint is before publication, after successful semantic
    // compilation. Interrupting here must reclaim the program and reservation.
    {
        var retirement: RetirementProbe = .{ .backing = no_resize.allocator() };
        var owner = Cache.init(retirement.allocator(), .{});
        retirement.owner = &owner;
        defer owner.deinit(io);
        var probe: Probe = .{ .owner = &owner, .fail_at = last_checkpoint, .retirement = &retirement };
        try std.testing.expectError(error.SqlPreparationInterrupted, owner.acquireControlled(io, key, &diagnostic, probe.control()));
        retirement.armed = false;
        try std.testing.expect(!probe.held_mutex);
        try std.testing.expect(!retirement.held_mutex);
        try std.testing.expect(!retirement.early_refund);
        try std.testing.expect(retirement.frees >= 2);
        try std.testing.expect(retirement.freed_bytes > @sizeOf(Entry));
        const stats = owner.stats(io);
        try std.testing.expectEqual(@as(usize, 0), stats.compiling);
        try std.testing.expectEqual(@as(usize, 0), stats.entries);
        try std.testing.expectEqual(@as(usize, 0), stats.leases);
        try std.testing.expectEqual(owner.buckets.len * @sizeOf(?*Entry), stats.bytes);
        // No interrupted program was cached; a subsequent acquisition compiles.
        var retry = try owner.acquire(io, key, &diagnostic);
        defer retry.release(io);
        try std.testing.expectEqual(@as(u64, 0), owner.stats(io).hits);
    }
}

test "SQL plan cache controlled incremental hashing preserves scoped digest identity" {
    const Probe = struct {
        checks: usize = 0,
        fn checkpoint(raw: *anyopaque) error{SqlPreparationInterrupted}!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
        }
    };
    const statement: [16385]u8 = @splat('x');
    const key: Key = .{ .statement = &statement, .principal = "alice", .database = "db", .namespace = "public" };
    var expected = std.crypto.hash.sha2.Sha256.init(.{});
    for ([_][]const u8{ "antfly.sql.compiled-plan.v1", key.principal, key.database, key.namespace, key.statement }) |field| {
        var length: [8]u8 = undefined;
        std.mem.writeInt(u64, &length, field.len, .little);
        expected.update(&length);
        expected.update(field);
    }
    var probe: Probe = .{};
    const actual = try hashControlled(key, .{ .ptr = &probe, .checkpoint = Probe.checkpoint });
    try std.testing.expectEqualSlices(u8, &expected.finalResult(), &actual);
    try std.testing.expect(probe.checks > 5);
}
