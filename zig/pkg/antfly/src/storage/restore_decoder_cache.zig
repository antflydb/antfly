// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: LicenseRef-Elastic-2.0

//! One published, read-only source decoder per resident restore owner. A hit
//! keeps the generation read lease pinned; a miss or terminal transition closes
//! it before the source generation can be replaced or retired. The restore
//! cursor is durable in the target, never in this disposable cache.
const std = @import("std");
const builtin = @import("builtin");
const DB = @import("db/db.zig").DB;
const Namespace = @import("db/doc_identity_namespace.zig").Namespace;
const Runtime = @import("background_runtime.zig").BackendRuntime;
const Scheduler = @import("../common/maintenance_scheduler.zig").Scheduler;
const ResourceManager = @import("resource_manager.zig").ResourceManager;

const idle_ms: u64 = 30_000;
// Bound timer/reclaimer registrations as well as open decoder handles: idle
// eviction closes the read lease but retains its registration slot until the
// restore owner terminates. Overflow uses a one-shot decoder.
pub const max_resident_decoders: u32 = 16;
var resident_decoders: std.atomic.Value(u32) = .init(0);

pub fn residentDecoderCount() u32 {
    return resident_decoders.load(.acquire);
}

fn reserveSlot() bool {
    var observed = resident_decoders.load(.acquire);
    while (observed < max_resident_decoders) {
        if (resident_decoders.cmpxchgWeak(observed, observed + 1, .acq_rel, .acquire)) |new_observed| {
            observed = new_observed;
        } else return true;
    }
    return false;
}

pub const Key = struct {
    scope: [32]u8,
    artifact: [32]u8,
    descriptor: [32]u8,
    namespace: Namespace,
    path: []const u8,

    fn eql(self: Key, other: Key) bool {
        return std.mem.eql(u8, &self.scope, &other.scope) and
            std.mem.eql(u8, &self.artifact, &other.artifact) and
            std.mem.eql(u8, &self.descriptor, &other.descriptor) and
            self.namespace.eql(other.namespace) and
            std.mem.eql(u8, self.path, other.path);
    }
};

pub const Cache = struct {
    mutex: std.Io.Mutex = .init,
    drained: std.Io.Condition = .init,
    active_pages: u32 = 0,
    key: ?Key = null,
    decoder: ?*DB = null,
    alloc: ?std.mem.Allocator = null,
    io: ?std.Io = null,
    runtime: ?*Runtime = null,
    idle_handle: ?Scheduler.Handle = null,
    manager: ?*ResourceManager = null,
    reclaimer: u64 = 0,
    slot_owned: bool = false,
    retiring: bool = false,
    last_used_ms: u64 = 0,

    /// Caller holds mutex. A pinned read lease makes the verified publication
    /// marker immutable until clearLocked closes the decoder.
    pub fn getLocked(self: *Cache, key: Key) ?*DB {
        if (self.retiring) return null;
        if (self.key) |existing| if (existing.eql(key)) return self.decoder;
        return null;
    }

    pub fn clearLocked(self: *Cache) void {
        while (self.active_pages != 0) self.drained.waitUncancelable(self.io.?, &self.mutex);
        if (self.decoder) |decoder| {
            decoder.close();
            self.alloc.?.destroy(decoder);
        }
        if (self.key) |key| self.alloc.?.free(key.path);
        self.decoder = null;
        self.key = null;
        self.alloc = null;
        self.last_used_ms = 0;
        if (self.slot_owned and self.idle_handle == null and self.reclaimer == 0) {
            _ = resident_decoders.fetchSub(1, .acq_rel);
            self.slot_owned = false;
        }
    }

    /// Caller holds mutex and has already verified the published marker.
    /// Ownership of decoder transfers only after the path allocation succeeds.
    pub fn installLocked(self: *Cache, alloc: std.mem.Allocator, io: std.Io, runtime: *Runtime, manager: ?*ResourceManager, key: Key, decoder: *DB, stable_address: bool) !bool {
        if (self.retiring) return false;
        if (!stable_address and !builtin.is_test) return false;
        // A stable owner needs both independent retirement paths before it
        // reserves a process-wide slot. Without a memory manager it must use
        // the caller's one-shot decoder, not park an idle timer forever.
        if (stable_address and manager == null) return false;
        const new_slot = !self.slot_owned;
        if (new_slot and !reserveSlot()) return false;
        var slot_transferred = !new_slot;
        defer if (new_slot and !slot_transferred) {
            _ = resident_decoders.fetchSub(1, .acq_rel);
        };
        self.io = io;
        self.runtime = runtime;
        // A stable production owner may retain a generation read lease only
        // when a timer can retire it without another restore request. If the
        // scheduler is unavailable, the caller uses this decoder for one page
        // and closes it; cache failure must not fail the restore itself.
        if (stable_address and self.idle_handle == null) {
            const scheduler = runtime.maintenanceScheduler() catch return false;
            self.idle_handle = scheduler.register(self, idleStep) catch return false;
            self.slot_owned = true;
            slot_transferred = true;
        }
        if (stable_address and self.reclaimer == 0) {
            // A transient registration failure keeps the already bounded
            // timer/slot for a later retry. Joining its callback while the
            // owner mutex is held would deadlock; retire() joins it safely.
            const bound = manager.?;
            self.reclaimer = bound.registerReclaimer(.relational_preparation_working_set, self, reclaim) catch return false;
            self.manager = bound;
        }
        const path = try alloc.dupe(u8, key.path);
        std.debug.assert(self.decoder == null and self.key == null);
        self.key = .{ .scope = key.scope, .artifact = key.artifact, .descriptor = key.descriptor, .namespace = key.namespace, .path = path };
        self.decoder = decoder;
        self.alloc = alloc;
        self.runtime = runtime;
        self.slot_owned = true;
        slot_transferred = true;
        self.touchLocked(io);
        // Registrations retain this address. Stack-opened test/embedded DBs
        // still use the cache but evict lazily or on close, never via a timer.
        runtime.wakeMaintenance(self);
        return true;
    }

    pub fn touchLocked(self: *Cache, io: std.Io) void {
        self.last_used_ms = nowMs(io);
        if (self.runtime) |runtime| runtime.wakeMaintenance(self);
    }

    /// Prepared pages may borrow source-owned bytes through the Raft
    /// proposal. Keep the generation read lease alive without holding mutex.
    pub fn pinLocked(self: *Cache) void {
        std.debug.assert(self.decoder != null);
        self.active_pages += 1;
    }

    pub fn unpin(self: *Cache, io: std.Io) void {
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        std.debug.assert(self.active_pages != 0);
        self.active_pages -= 1;
        if (self.active_pages == 0) self.drained.broadcast(io);
    }

    pub fn expireLocked(self: *Cache, io: std.Io) void {
        if (self.decoder != null and nowMs(io) -| self.last_used_ms >= idle_ms) self.clearLocked();
    }

    pub fn retire(self: *Cache, io: std.Io) void {
        self.mutex.lockUncancelable(io);
        while (self.retiring) self.drained.waitUncancelable(io, &self.mutex);
        self.retiring = true;
        self.clearLocked();
        var idle_handle = self.idle_handle;
        self.idle_handle = null;
        const manager = self.manager;
        const reclaimer = self.reclaimer;
        self.manager = null;
        self.reclaimer = 0;
        self.mutex.unlock(io);
        // Join callbacks without the owner mutex: either may be waiting to
        // inspect an idle decoder. New installs fall back to one-shot while
        // this terminal transition is in flight.
        if (idle_handle) |*handle| handle.cancel(io);
        if (manager) |value| value.unregisterReclaimer(reclaimer);
        self.mutex.lockUncancelable(io);
        if (self.slot_owned) {
            _ = resident_decoders.fetchSub(1, .acq_rel);
            self.slot_owned = false;
        }
        self.retiring = false;
        self.drained.broadcast(io);
        self.mutex.unlock(io);
    }

    pub fn deinit(self: *Cache, io: std.Io) void {
        self.retire(io);
    }

    fn reclaim(raw: *anyopaque, _: u64) u64 {
        const self: *Cache = @ptrCast(@alignCast(raw));
        // Allocation can request reclamation while this owner prepares a
        // page. Never re-enter its mutex or evict a decoder in active use.
        if (!self.mutex.tryLock()) return 0;
        defer self.mutex.unlock(self.io.?);
        if (self.decoder == null or self.active_pages != 0) return 0;
        self.clearLocked();
        return @sizeOf(DB);
    }

    pub fn reclaimForTest(self: *Cache) u64 {
        if (!builtin.is_test) @compileError("restore decoder fault probe is test-only");
        return reclaim(self, std.math.maxInt(u64));
    }

    fn idleStep(self: *Cache) ?u64 {
        const runtime = self.runtime orelse return null;
        const io = runtime.io() orelse return null;
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        if (self.decoder == null) return null;
        const elapsed = nowMs(io) -| self.last_used_ms;
        if (elapsed >= idle_ms) {
            if (self.active_pages != 0) return 100;
            self.clearLocked();
            return null;
        }
        return idle_ms - elapsed;
    }
};

fn nowMs(io: std.Io) u64 {
    return @intCast(@max(0, @divTrunc(std.Io.Clock.awake.now(io).nanoseconds, std.time.ns_per_ms)));
}

test "restore decoder key binds scope artifact descriptor namespace and path" {
    const original: Key = .{
        .scope = @splat(1),
        .artifact = @splat(2),
        .descriptor = @splat(3),
        .namespace = .{ .table_id = 4, .shard_id = 5, .range_id = 6 },
        .path = "/published/source",
    };
    try std.testing.expect(original.eql(original));
    var changed = original;
    changed.scope[0] ^= 1;
    try std.testing.expect(!original.eql(changed));
    changed = original;
    changed.artifact[0] ^= 1;
    try std.testing.expect(!original.eql(changed));
    changed = original;
    changed.descriptor[0] ^= 1;
    try std.testing.expect(!original.eql(changed));
    changed = original;
    changed.namespace.range_id += 1;
    try std.testing.expect(!original.eql(changed));
    changed = original;
    changed.path = "/published/other";
    try std.testing.expect(!original.eql(changed));
}

test "restore decoder registered owner slots stay bounded across idle and terminal" {
    const baseline = residentDecoderCount();
    try std.testing.expect(baseline <= max_resident_decoders);
    const available: usize = @intCast(max_resident_decoders - baseline);
    var caches: [max_resident_decoders]Cache = undefined;
    for (&caches) |*cache| cache.* = .{};
    var registered: usize = 0;
    defer {
        for (caches[0..registered]) |*cache| cache.retire(std.testing.io);
    }
    while (registered < available) : (registered += 1) {
        try std.testing.expect(reserveSlot());
        const cache = &caches[registered];
        cache.slot_owned = true;
        cache.io = std.testing.io;
        // Model a parked registration after its idle decoder was closed.
        // No callback context may be allocated for owner 17.
        cache.reclaimer = 1;
        cache.clearLocked();
        try std.testing.expect(cache.slot_owned);
    }
    try std.testing.expectEqual(max_resident_decoders, residentDecoderCount());
    try std.testing.expect(!reserveSlot());
    for (caches[0..registered]) |*cache| cache.retire(std.testing.io);
    try std.testing.expectEqual(baseline, residentDecoderCount());
}

test "restore decoder stable owner without resource manager uses no resident slot" {
    const baseline = residentDecoderCount();
    var cache: Cache = .{};
    var runtime: Runtime = undefined;
    var decoder: DB = undefined;
    const key: Key = .{
        .scope = @splat(1),
        .artifact = @splat(2),
        .descriptor = @splat(3),
        .namespace = .{ .table_id = 4, .shard_id = 5, .range_id = 6 },
        .path = "/published/source",
    };
    try std.testing.expect(!(try cache.installLocked(std.testing.allocator, std.testing.io, &runtime, null, key, &decoder, true)));
    try std.testing.expectEqual(baseline, residentDecoderCount());
    try std.testing.expect(!cache.slot_owned);
    try std.testing.expect(cache.idle_handle == null);
    try std.testing.expectEqual(@as(u64, 0), cache.reclaimer);
    try std.testing.expect(cache.decoder == null);
}
