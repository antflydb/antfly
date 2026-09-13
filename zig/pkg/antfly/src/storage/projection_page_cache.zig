//! Node-wide optional clean-page cache. Keys identify an immutable open file,
//! not a path or generation number that another index might reuse. Readers
//! copy under a short stripe lock; no I/O or resource admission holds that lock.
const std = @import("std");
const resources = @import("resource_manager.zig");
pub const page_size = 16 * 1024;
const slot_count = 2048; // 32 MiB maximum; pages are admitted lazily on reuse.

pub const Cache = struct {
    const Entry = struct {
        mutex: std.atomic.Mutex = .unlocked,
        file: u64 = 0,
        offset: usize = 0,
        probation_file: u64 = 0,
        probation_offset: usize = 0,
        bytes: ?[]u8 = null,
        valid: usize = 0,
        reservation: ?resources.Reservation = null,
        borrowers: usize = 0,
    };
    manager: *resources.ResourceManager,
    metadata: resources.Reservation,
    reclaimer: u64 = 0,
    entries: [slot_count]Entry = @splat(.{}),
    hits: std.atomic.Value(u64) = .init(0),
    misses: std.atomic.Value(u64) = .init(0),
    resident: std.atomic.Value(u64) = .init(0),
    reclaimed: std.atomic.Value(u64) = .init(0),

    pub fn create(manager: *resources.ResourceManager) !*Cache {
        var reservation = try manager.reserveImmediate(.hbc_node_metadata_cache, @sizeOf(Cache));
        errdefer reservation.release();
        const self = try std.heap.page_allocator.create(Cache);
        errdefer std.heap.page_allocator.destroy(self);
        self.* = .{ .manager = manager, .metadata = reservation };
        self.reclaimer = try manager.registerReclaimer(.hbc_node_metadata_cache, self, reclaim);
        return self;
    }

    fn entry(self: *Cache, file: u64, offset: usize) *Entry {
        var key: [16]u8 = undefined;
        std.mem.writeInt(u64, key[0..8], file, .little);
        std.mem.writeInt(u64, key[8..16], @intCast(offset), .little);
        return &self.entries[std.hash.Wyhash.hash(0, &key) & (slot_count - 1)];
    }

    pub const Lookup = enum { hit, miss, admit };
    pub const Lease = struct {
        slot: *Entry,
        bytes: []const u8,

        pub fn deinit(self: *Lease) void {
            while (!self.slot.mutex.tryLock()) std.atomic.spinLoopHint();
            std.debug.assert(self.slot.borrowers != 0);
            self.slot.borrowers -= 1;
            self.slot.mutex.unlock();
            self.* = undefined;
        }
    };

    /// The caller retains the immutable file generation and this lease until
    /// its last use of the bytes. Pinned slots cannot be overwritten/reclaimed.
    pub fn borrow(self: *Cache, file: u64, offset: usize, within: usize, len: usize) ?Lease {
        const slot = self.entry(file, offset);
        if (!slot.mutex.tryLock()) return null;
        defer slot.mutex.unlock();
        const bytes = slot.bytes orelse return null;
        if (slot.file != file or slot.offset != offset or within > slot.valid or len > slot.valid - within) return null;
        slot.borrowers += 1;
        _ = self.hits.fetchAdd(1, .monotonic);
        return .{ .slot = slot, .bytes = bytes[within..][0..len] };
    }
    pub fn copy(self: *Cache, file: u64, offset: usize, within: usize, out: []u8) Lookup {
        const slot = self.entry(file, offset);
        if (!slot.mutex.tryLock()) return .miss;
        defer slot.mutex.unlock();
        if (slot.bytes) |bytes| {
            if (slot.file == file and slot.offset == offset and within <= slot.valid and out.len <= slot.valid - within) {
                @memcpy(out, bytes[within..][0..out.len]);
                _ = self.hits.fetchAdd(1, .monotonic);
                return .hit;
            }
        }
        _ = self.misses.fetchAdd(1, .monotonic);
        const repeated = slot.probation_file == file and slot.probation_offset == offset;
        slot.probation_file = file;
        slot.probation_offset = offset;
        return if (repeated) .admit else .miss;
    }

    pub fn put(self: *Cache, file: u64, offset: usize, bytes: []const u8) void {
        std.debug.assert(bytes.len <= page_size);
        const slot = self.entry(file, offset);
        if (!slot.mutex.tryLock()) return;
        if (slot.borrowers != 0) {
            slot.mutex.unlock();
            return;
        }
        if (slot.bytes) |destination| {
            @memcpy(destination[0..bytes.len], bytes);
            slot.file = file;
            slot.offset = offset;
            slot.valid = bytes.len;
            slot.mutex.unlock();
            return;
        }
        slot.mutex.unlock();
        // Best-effort, non-reclaiming admission outside the cache lock. A miss
        // must always remain serviceable when the host memory budget is full.
        var reservation = self.manager.reserveImmediate(.hbc_node_metadata_cache, page_size) catch return;
        const destination = std.heap.page_allocator.alloc(u8, page_size) catch {
            reservation.release();
            return;
        };
        if (!slot.mutex.tryLock()) {
            std.heap.page_allocator.free(destination);
            reservation.release();
            return;
        }
        defer slot.mutex.unlock();
        if (slot.bytes != null) {
            std.heap.page_allocator.free(destination);
            reservation.release();
            return;
        }
        @memcpy(destination[0..bytes.len], bytes);
        slot.bytes = destination;
        slot.reservation = reservation;
        slot.file = file;
        slot.offset = offset;
        slot.valid = bytes.len;
        _ = self.resident.fetchAdd(page_size, .monotonic);
    }

    pub fn reclaim(ptr: *anyopaque, target: u64) u64 {
        const self: *Cache = @ptrCast(@alignCast(ptr));
        var freed: u64 = 0;
        for (&self.entries) |*slot| {
            if (freed >= target) break;
            if (!slot.mutex.tryLock()) continue;
            if (slot.borrowers != 0) {
                slot.mutex.unlock();
                continue;
            }
            const bytes = slot.bytes;
            var reservation = slot.reservation;
            slot.bytes = null;
            slot.reservation = null;
            slot.probation_file = 0;
            slot.mutex.unlock();
            if (bytes) |owned| {
                std.heap.page_allocator.free(owned);
                reservation.?.release();
                freed += page_size;
                _ = self.resident.fetchSub(page_size, .monotonic);
            }
        }
        _ = self.reclaimed.fetchAdd(freed, .monotonic);
        return freed;
    }

    pub fn deinit(self: *Cache) void {
        self.manager.unregisterReclaimer(self.reclaimer);
        _ = reclaim(self, std.math.maxInt(u64));
        std.debug.assert(self.resident.load(.monotonic) == 0);
        self.metadata.release();
        std.heap.page_allocator.destroy(self);
    }
};

test "projection page cache identity, probation, and pressure reclamation" {
    var manager = resources.ResourceManager.init(.{});
    defer manager.deinit(std.testing.allocator);
    const cache = try Cache.create(&manager);
    defer cache.deinit();
    var out: [3]u8 = undefined;
    try std.testing.expectEqual(.miss, cache.copy(1, 0, 0, &out));
    try std.testing.expectEqual(.admit, cache.copy(1, 0, 0, &out));
    cache.put(1, 0, "abcdef");
    try std.testing.expectEqual(.hit, cache.copy(1, 0, 2, &out));
    try std.testing.expectEqualStrings("cde", &out);
    try std.testing.expect(cache.copy(2, 0, 2, &out) != .hit);
    try std.testing.expect(cache.copy(1, 0, 5, &out) != .hit);
    try std.testing.expectEqual(@as(u64, page_size), Cache.reclaim(cache, 1));
    try std.testing.expectEqual(@as(u64, 0), cache.resident.load(.monotonic));
    try std.testing.expectEqual(.miss, cache.copy(1, 0, 0, &out));
}

test "projection page leases prevent overwrite and reclamation until the last reader" {
    var manager = resources.ResourceManager.init(.{});
    defer manager.deinit(std.testing.allocator);
    const cache = try Cache.create(&manager);
    defer cache.deinit();
    cache.put(1, 0, "abcdef");
    var first = cache.borrow(1, 0, 1, 3).?;
    var second = cache.borrow(1, 0, 0, 6).?;
    try std.testing.expect(cache.borrow(2, 0, 0, 3) == null);
    try std.testing.expect(cache.borrow(1, 0, 5, 3) == null);
    cache.put(1, 0, "changed");
    try std.testing.expectEqualStrings("bcd", first.bytes);
    try std.testing.expectEqual(@as(u64, 0), Cache.reclaim(cache, page_size));
    first.deinit();
    try std.testing.expectEqual(@as(u64, 0), Cache.reclaim(cache, page_size));
    try std.testing.expectEqualStrings("abcdef", second.bytes);
    second.deinit();
    try std.testing.expectEqual(@as(u64, page_size), Cache.reclaim(cache, page_size));
    try std.testing.expect(cache.borrow(1, 0, 0, 3) == null);
}
