//! Disposable bindings from existing, index-scoped ANN member identities to
//! rows in one retained source generation. No payloads or borrowed pointers.
//! Replacement generations start empty; old leases keep their own bindings.
const std = @import("std");
const resources = @import("resource_manager.zig");
const Allocator = std.mem.Allocator;

pub const Row = struct {
    reader: u32,
    row: u32,
    source_sequence: u64,
    revision: u64,
};

pub const Cache = struct {
    const Entry = struct {
        incarnation: u64 = 0,
        member: u64 = 0,
        row: Row = undefined,
    };
    const ways = 4;
    const stripe_count = 256;
    const Stripe = struct {
        mutex: std.atomic.Mutex = .unlocked,
        entries: ?[]Entry = null,
        reservation: ?resources.Reservation = null,
        hand: usize = 0,
    };
    alloc: Allocator,
    count: usize,
    manager: ?*resources.ResourceManager,
    reclaimer: ?u64 = null,
    resident_bytes: std.atomic.Value(u64) = .init(0),
    stripes: [stripe_count]Stripe = @splat(.{}),

    pub fn create(alloc: Allocator, count: usize, manager: ?*resources.ResourceManager) !*Cache {
        if (count < stripe_count * ways or count > 1048576 or !std.math.isPowerOfTwo(count)) return error.InvalidArgument;
        const self = try alloc.create(Cache);
        self.* = .{ .alloc = alloc, .count = count, .manager = manager };
        errdefer alloc.destroy(self);
        if (manager) |m| self.reclaimer = try m.registerReclaimer(.dense_source_payload_state, self, reclaim);
        return self;
    }

    pub fn deinit(self: *Cache) void {
        if (self.reclaimer) |id| self.manager.?.unregisterReclaimer(id);
        _ = reclaim(self, std.math.maxInt(u64));
        self.alloc.destroy(self);
    }

    pub fn reclaim(ptr: *anyopaque, target: u64) u64 {
        const self: *Cache = @ptrCast(@alignCast(ptr));
        var released: u64 = 0;
        for (&self.stripes) |*stripe| {
            if (released >= target) break;
            if (!stripe.mutex.tryLock()) continue;
            if (stripe.entries) |entries| {
                released += entries.len * @sizeOf(Entry);
                _ = self.resident_bytes.fetchSub(entries.len * @sizeOf(Entry), .monotonic);
                self.alloc.free(entries);
                stripe.entries = null;
                if (stripe.reservation) |*reservation| reservation.release();
                stripe.reservation = null;
            }
            stripe.mutex.unlock();
        }
        return released;
    }

    fn bucket(self: *Cache, incarnation: u64, member: u64) usize {
        const identity = [2]u64{ incarnation, member };
        return @intCast(std.hash.Wyhash.hash(0, std.mem.asBytes(&identity)) & (self.count / ways - 1));
    }

    pub fn get(self: *Cache, incarnation: u64, member: u64) ?Row {
        if (incarnation == 0) return null;
        const b = self.bucket(incarnation, member);
        const stripe = &self.stripes[b % stripe_count];
        // Contention and memory eviction are ordinary misses, never waits in
        // the query path. Copy the small row while protected; no view escapes.
        if (!stripe.mutex.tryLock()) return null;
        defer stripe.mutex.unlock();
        const entries = stripe.entries orelse return null;
        for (entries[b / stripe_count * ways ..][0..ways]) |entry| {
            if (entry.incarnation == incarnation and entry.member == member) return entry.row;
        }
        return null;
    }

    pub fn put(self: *Cache, incarnation: u64, member: u64, row: Row) void {
        if (incarnation == 0) return;
        const b = self.bucket(incarnation, member);
        const stripe = &self.stripes[b % stripe_count];
        if (!stripe.mutex.tryLock()) return;
        defer stripe.mutex.unlock();
        if (stripe.entries == null) {
            const count = self.count / stripe_count;
            // Immediate admission cannot invoke reclaimers while this stripe
            // is locked. Failure merely leaves the authoritative lookup active.
            var reservation: ?resources.Reservation = if (self.manager) |m|
                m.reserveImmediate(.dense_source_payload_state, count * @sizeOf(Entry)) catch return
            else
                null;
            const entries = self.alloc.alloc(Entry, count) catch {
                if (reservation) |*r| r.release();
                return;
            };
            @memset(entries, .{});
            stripe.entries = entries;
            stripe.reservation = reservation;
            _ = self.resident_bytes.fetchAdd(entries.len * @sizeOf(Entry), .monotonic);
        }
        const entries = stripe.entries.?[b / stripe_count * ways ..][0..ways];
        var destination: ?*Entry = null;
        for (entries) |*entry| {
            if (entry.incarnation == incarnation and entry.member == member) {
                destination = entry;
                break;
            }
            if (entry.incarnation == 0) destination = entry;
        }
        const entry = destination orelse &entries[stripe.hand % ways];
        stripe.hand +%= 1;
        entry.* = .{ .incarnation = incarnation, .member = member, .row = row };
    }
};

test "vector block member bindings isolate indexes, generations, eviction and contention" {
    const alloc = std.testing.allocator;
    const old = try Cache.create(alloc, 1024, null);
    defer old.deinit();
    const next = try Cache.create(alloc, 1024, null);
    defer next.deinit();
    const first: Row = .{ .reader = 1, .row = 9, .source_sequence = 4, .revision = 7 };
    const second: Row = .{ .reader = 2, .row = 3, .source_sequence = 5, .revision = 8 };
    old.put(1, 42, first);
    old.put(2, 42, second);
    next.put(1, 42, second);
    try std.testing.expectEqualDeep(first, old.get(1, 42).?);
    try std.testing.expectEqualDeep(second, old.get(2, 42).?);
    try std.testing.expectEqualDeep(second, next.get(1, 42).?);
    try std.testing.expect(next.get(2, 42) == null);
    const stripe = &old.stripes[old.bucket(1, 42) % Cache.stripe_count];
    try std.testing.expect(stripe.mutex.tryLock());
    try std.testing.expect(old.get(1, 42) == null);
    old.put(1, 42, second);
    stripe.mutex.unlock();
    try std.testing.expectEqualDeep(first, old.get(1, 42).?);
    try std.testing.expect(Cache.reclaim(old, std.math.maxInt(u64)) > 0);
    try std.testing.expect(old.get(1, 42) == null);
    try std.testing.expectEqualDeep(second, next.get(1, 42).?);
    for (0..10000) |id| old.put(1, id, first);
    for (0..10000) |id| if (old.get(1, id)) |row| try std.testing.expectEqualDeep(first, row);
}

test "vector block member binding residency is admitted and reclaimable" {
    const alloc = std.testing.allocator;
    var manager = resources.ResourceManager.init(.{});
    defer manager.deinit(alloc);
    const cache = try Cache.create(alloc, 1024, &manager);
    defer cache.deinit();
    const before = manager.snapshot().memory.used_bytes;
    cache.put(1, 42, .{ .reader = 0, .row = 3, .source_sequence = 9, .revision = 2 });
    try std.testing.expect(cache.get(1, 42) != null);
    const resident = manager.snapshot().memory.used_bytes - before;
    try std.testing.expectEqual(@as(u64, 4 * @sizeOf(Cache.Entry)), resident);
    try std.testing.expectEqual(resident, cache.resident_bytes.load(.monotonic));
    try std.testing.expectEqual(resident, Cache.reclaim(cache, std.math.maxInt(u64)));
    try std.testing.expectEqual(@as(u64, 0), cache.resident_bytes.load(.monotonic));
    try std.testing.expectEqual(before, manager.snapshot().memory.used_bytes);
    try std.testing.expect(cache.get(1, 42) == null);
}
