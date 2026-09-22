// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).
//! Schema-independent immutable plans only: authorization, catalog resolution,
//! parameter values and bound plans MUST remain request-local. The allocator
//! must support concurrent calls; the owner and allocator outlive all leases.
//! Shutdown drains acquisitions/leases before deinit. All mutex operations use
//! std.Io; callers must use compatible Io executors for this shared mutex.
const std = @import("std");
const compiler = @import("compiler.zig");
const Budget = @import("memory_budget.zig");
const Digest = [32]u8;

pub const Config = struct {
    max_entries: usize = 256,
    max_bytes: usize = 32 << 20,
    max_compile_bytes: usize = 4 << 20,
    max_concurrent_compiles: usize = 4,
    compiler_limits: compiler.Limits = .{},
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
    bucket_next: ?*Entry = null,
    prev: ?*Entry = null,
    next: ?*Entry = null,
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
        cache.mutex.lockUncancelable(io);
        defer cache.mutex.unlock(io);
        std.debug.assert(self.entry.refs != 0);
        self.entry.refs -= 1;
        cache.counters.leases -= 1;
        if (self.entry.refs == 0) cache.appendIdle(self.entry);
        self.* = undefined;
    }
};

pub const Cache = struct {
    allocator: std.mem.Allocator,
    config: Config,
    mutex: std.Io.Mutex = .init,
    buckets: []?*Entry = &.{},
    oldest: ?*Entry = null,
    newest: ?*Entry = null,
    counters: Stats = .{},
    initialized: bool = false,

    /// Lazy allocation permits embedding in an infallible server constructor.
    /// Keep the Cache address stable after its first acquisition.
    pub fn init(allocator: std.mem.Allocator, config: Config) Cache {
        return .{ .allocator = allocator, .config = config };
    }

    pub fn stats(self: *Cache, io: std.Io) Stats {
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        return self.counters;
    }

    pub fn deinit(self: *Cache, io: std.Io) void {
        if (!self.initialized) {
            self.* = undefined;
            return;
        }
        self.mutex.lockUncancelable(io);
        std.debug.assert(self.counters.leases == 0 and self.counters.compiling == 0);
        while (self.oldest) |entry| {
            self.removeIdle(entry);
            self.destroy(entry);
        }
        self.allocator.free(self.buckets);
        self.mutex.unlock(io);
        self.* = undefined;
    }

    pub fn acquire(self: *Cache, io: std.Io, key: Key, diagnostic: *compiler.Diagnostic) !Lease {
        diagnostic.* = .{};
        try io.checkCancel();
        // Reject before hashing arbitrary request bytes or touching cache state.
        if (key.statement.len > self.config.compiler_limits.max_bytes) {
            diagnostic.message = "SQL statement byte budget exceeded";
            return error.SqlLimitExceeded;
        }
        if (key.principal.len > 4096 or key.database.len > 4096 or key.namespace.len > 4096)
            return error.SqlLimitExceeded;
        const digest = hash(key);
        try self.mutex.lock(io);
        var locked = true;
        defer if (locked) self.mutex.unlock(io);
        try self.ensureBuckets();
        const reservation = self.config.max_compile_bytes + @sizeOf(Entry);
        while (true) {
            if (self.find(digest)) |entry| {
                self.counters.hits +%= 1;
                return self.lease(entry);
            }
            if (self.counters.compiling >= self.config.max_concurrent_compiles) return self.busy();
            if (self.counters.entries + self.counters.compiling < self.config.max_entries and
                reservation <= self.config.max_bytes - self.counters.bytes) break;
            const victim = self.oldest orelse return self.busy();
            self.removeIdle(victim);
            self.removeBucket(victim);
            const reclaimed = @sizeOf(Entry) + victim.budget.live;
            // Keep bytes/entry charged while freeing outside the mutex. Another
            // miss cannot reserve memory that has not actually been released.
            self.mutex.unlock(io);
            self.destroy(victim);
            self.mutex.lockUncancelable(io);
            self.counters.entries -= 1;
            self.counters.bytes -= reclaimed;
            self.counters.evictions +%= 1;
        }
        self.counters.misses +%= 1;
        self.counters.compiling += 1;
        self.counters.bytes += reservation;
        self.mutex.unlock(io);
        locked = false;
        // Cleanup is uncancelable: every admission reservation is reclaimed.
        var reserved = true;
        defer if (reserved) {
            self.mutex.lockUncancelable(io);
            self.counters.compiling -= 1;
            self.counters.bytes -= reservation;
            self.mutex.unlock(io);
        };
        const entry = try self.allocator.create(Entry);
        errdefer self.allocator.destroy(entry);
        entry.* = .{ .digest = digest, .budget = .{ .backing = self.allocator, .limit = self.config.max_compile_bytes } };
        entry.compiled = compiler.compileDiagnostic(entry.budget.allocator(), key.statement, self.config.compiler_limits, diagnostic) catch |err| {
            if (err == error.OutOfMemory and entry.budget.exhausted) {
                diagnostic.message = "SQL compilation memory budget exceeded";
                return error.SqlLimitExceeded;
            }
            return err;
        };
        errdefer entry.compiled.deinit();
        try io.checkCancel();
        try self.mutex.lock(io);
        locked = true;
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
        return .{ .cache = self, .entry = entry };
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
        if (entry.refs == 0) self.removeIdle(entry);
        entry.refs += 1;
        self.counters.leases += 1;
        return .{ .cache = self, .entry = entry };
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
    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    for ([_][]const u8{ "antfly.sql.compiled-plan.v1", key.principal, key.database, key.namespace, key.statement }) |field| {
        var length: [8]u8 = undefined;
        std.mem.writeInt(u64, &length, field.len, .little);
        hasher.update(&length);
        hasher.update(field);
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
    var backing: Budget = .{ .backing = std.testing.allocator, .limit = 8192 };
    var cache = Cache.init(backing.allocator(), .{ .max_entries = 8, .max_bytes = 8192, .max_compile_bytes = 4096 });
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
