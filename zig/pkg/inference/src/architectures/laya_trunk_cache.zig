// Copyright 2026 Antfly, Inc.
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

//! Session-owned cache of tree-packed Laya trunk keys and values
//! (models/laya/LAYA.md, "State cache"). A trunk never attends to a branch,
//! so its per-layer keys and values depend only on its tokens and the model.
//! A later request about the same state encodes only its question branches.
//!
//! Entries are host memory on CPU and retained device tensors on Metal, f16 by
//! default (ANTFLY_LAYA_TRUNK_CACHE_DTYPE=f32 keeps them exact). They are
//! bounded by `limit_bytes` and evicted least recently used; entries in use
//! are pinned and never evicted. The bound defaults to
//! ANTFLY_LAYA_TRUNK_CACHE_MB (256 MiB); 0 disables caching. Trunks shorter
//! than `min_tokens` are not cached. With admission configured, every
//! entry holds a KV lease on the model's admission controller.
//!
//! `lookup` reserves an entry's bytes and admission lease before the caller
//! encodes anything, so concurrent misses never exceed the budget, and it
//! lets only one caller fill a given key: later misses for that key wait for
//! the fill instead of duplicating it. An entry that cannot fit is bypassed
//! without encoding it for the cache.
const std = @import("std");
const platform = @import("antfly_platform");
const memory = @import("../runtime/tier/memory.zig");

pub const Precision = enum { f16, f32 };

pub const default_limit_mb = 256;
/// Shorter trunks cost less to re-encode than the cached path's fixed
/// overhead (measured on Metal and CPU with the released checkpoint).
pub const default_min_tokens = 96;

/// Per-layer trunk keys (after RoPE for the encoder) and values, `[T, H]`
/// each, for every encoder layer followed by every decision-head layer, in
/// slot order `[layer][keys, values]`. Host entries store `host16` or
/// `host32`; device entries own backend tensors through `device` (released
/// by `device_deinit`).
pub const Entry = struct {
    key: [32]u8,
    tokens: usize,
    hidden: usize,
    layers: usize,
    precision: Precision,
    host16: []f16 = &.{},
    host32: []f32 = &.{},
    device: ?*anyopaque = null,
    device_deinit: ?*const fn (?*anyopaque, std.mem.Allocator) void = null,
    lease: ?memory.AdmissionLease = null,
    pins: usize = 0,
    last_used: u64 = 0,
    /// A `fill` entry whose publication failed: still in use by its filler,
    /// so it stays reserved and leased until its last `release`.
    orphaned: bool = false,

    fn span(self: *const Entry, slot: usize) [2]usize {
        const n = self.tokens * self.hidden;
        return .{ slot * n, n };
    }
    /// Copy one host slot (`2 * layer` for keys, `+ 1` for values) in.
    pub fn store(self: *Entry, slot: usize, values: []const f32) void {
        const at = self.span(slot);
        switch (self.precision) {
            .f16 => for (self.host16[at[0]..][0..at[1]], values) |*dst, v| {
                dst.* = @floatCast(v);
            },
            .f32 => @memcpy(self.host32[at[0]..][0..at[1]], values),
        }
    }
    /// Copy one host slot out as f32.
    pub fn load(self: *const Entry, slot: usize, out: []f32) void {
        const at = self.span(slot);
        switch (self.precision) {
            .f16 => for (out, self.host16[at[0]..][0..at[1]]) |*dst, v| {
                dst.* = v;
            },
            .f32 => @memcpy(out, self.host32[at[0]..][0..at[1]]),
        }
    }
    pub fn bytes(self: *const Entry) usize {
        const width: usize = if (self.precision == .f16) 2 else 4;
        return 2 * self.layers * self.tokens * self.hidden * width;
    }
};

/// The admission controller, domain, and limits of the owning session.
pub const Admission = struct {
    controller: *memory.AdmissionController,
    backend_class: memory.BackendClass,
    limits: memory.Limits,
    /// Entries live in backend (device) memory rather than host memory.
    device: bool,
};

pub const Stats = struct { hits: u64 = 0, misses: u64 = 0, entries: usize = 0, bytes: usize = 0, evictions: u64 = 0, refusals: u64 = 0, waits: u64 = 0 };

/// What `lookup` hands back. `hit` and `fill` entries are pinned: `release`
/// them when done. A `fill` entry must be filled and then `publish`ed, or
/// `abandon`ed on failure. `bypass` means: do not use the cache.
pub const Lookup = union(enum) { hit: *Entry, fill: *Entry, bypass };

pub const Cache = struct {
    allocator: std.mem.Allocator,
    limit_bytes: usize,
    min_tokens: usize = default_min_tokens,
    precision: Precision = .f16,
    admission: ?Admission = null,
    mutex: std.atomic.Mutex = .unlocked,
    entries: std.ArrayListUnmanaged(*Entry) = .empty,
    /// Reserved entries being filled; their bytes count in `reserved_bytes`.
    pending: std.ArrayListUnmanaged(*Entry) = .empty,
    reserved_bytes: usize = 0,
    clock: u64 = 0,
    stats: Stats = .{},

    pub fn init(allocator: std.mem.Allocator, limit_bytes: usize) Cache {
        return .{ .allocator = allocator, .limit_bytes = limit_bytes };
    }

    pub fn fromEnvironment(allocator: std.mem.Allocator) Cache {
        const mb = platform.env.getenvUsize("ANTFLY_LAYA_TRUNK_CACHE_MB") orelse default_limit_mb;
        var cache = init(allocator, std.math.mul(usize, mb, 1024 * 1024) catch std.math.maxInt(usize));
        if (platform.env.getenv("ANTFLY_LAYA_TRUNK_CACHE_DTYPE")) |dtype| {
            if (std.mem.eql(u8, dtype, "f32")) cache.precision = .f32;
        }
        return cache;
    }

    pub fn deinit(self: *Cache) void {
        std.debug.assert(self.pending.items.len == 0);
        for (self.entries.items) |entry| self.destroy(entry);
        self.entries.deinit(self.allocator);
        self.pending.deinit(self.allocator);
    }

    /// Precision of entries created from now on; entries already cached or
    /// being filled keep theirs.
    pub fn setPrecision(self: *Cache, precision: Precision) void {
        platform.sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        self.precision = precision;
    }

    /// Charge future entries to the session's admission controller.
    pub fn configureAdmission(self: *Cache, admission: Admission) void {
        platform.sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        self.admission = admission;
    }

    fn destroy(self: *Cache, entry: *Entry) void {
        if (entry.lease) |*lease| lease.release();
        if (entry.device_deinit) |free_device| free_device(entry.device, self.allocator);
        self.allocator.free(entry.host16);
        self.allocator.free(entry.host32);
        self.allocator.destroy(entry);
    }

    pub fn key(ids: []const i64, layers: usize, hidden: usize) [32]u8 {
        return keyAt(ids, layers, hidden, 0);
    }

    /// The key of a trunk encoded at logical positions `first_position..`
    /// (`packing.question_first` moves it); its keys carry those RoPE angles.
    pub fn keyAt(ids: []const i64, layers: usize, hidden: usize, first_position: usize) [32]u8 {
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("antfly-laya-trunk/v2");
        hash.update(std.mem.asBytes(&layers));
        hash.update(std.mem.asBytes(&hidden));
        hash.update(std.mem.asBytes(&first_position));
        hash.update(std.mem.sliceAsBytes(ids));
        return hash.finalResult();
    }

    /// Pin and return a cached trunk, or null on a miss.
    pub fn acquire(self: *Cache, k: [32]u8) ?*Entry {
        platform.sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        if (self.hitLocked(k)) |entry| return entry;
        self.stats.misses += 1;
        return null;
    }

    /// Whether `k` is cached, without pinning it or counting a hit. The
    /// answer can go stale at once; it only guides scheduling.
    pub fn contains(self: *Cache, k: [32]u8) bool {
        platform.sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        for (self.entries.items) |entry| if (std.mem.eql(u8, &entry.key, &k)) return true;
        return false;
    }

    fn hitLocked(self: *Cache, k: [32]u8) ?*Entry {
        for (self.entries.items) |entry| if (std.mem.eql(u8, &entry.key, &k)) {
            entry.pins += 1;
            self.clock += 1;
            entry.last_used = self.clock;
            self.stats.hits += 1;
            return entry;
        };
        return null;
    }

    fn pendingLocked(self: *Cache, k: [32]u8) bool {
        // An orphaned entry will never be published, so nobody waits on it.
        for (self.pending.items) |entry| if (!entry.orphaned and std.mem.eql(u8, &entry.key, &k)) return true;
        return false;
    }

    /// A cached trunk (`hit`), or a reserved entry for the caller to fill
    /// (`fill`), or `bypass` when the entry cannot fit the budget or the
    /// admission controller. Reservation happens before any encoding. A miss
    /// on a key another caller is filling waits for that fill.
    pub fn lookup(self: *Cache, k: [32]u8, tokens: usize, layers: usize, hidden: usize, host: bool) !Lookup {
        const count = try std.math.mul(usize, try std.math.mul(usize, 2 * layers, tokens), hidden);
        while (true) {
            platform.sync.lockYielding(&self.mutex);
            // Read the precision once, under the lock: the reservation, the
            // entry, and its host slice must all agree on it.
            const precision = self.precision;
            const size = std.math.mul(usize, count, if (precision == .f16) @as(usize, 2) else 4) catch |err| {
                self.mutex.unlock();
                return err;
            };
            if (self.hitLocked(k)) |entry| {
                self.mutex.unlock();
                return .{ .hit = entry };
            }
            if (self.pendingLocked(k)) {
                self.stats.waits += 1;
                self.mutex.unlock();
                platform.time.yieldBriefly();
                continue;
            }
            self.stats.misses += 1;
            const reserved = self.reserveLocked(size) catch |err| {
                self.mutex.unlock();
                return err;
            };
            const lease = reserved orelse {
                self.stats.refusals += 1;
                self.mutex.unlock();
                return .bypass;
            };
            const entry = self.allocator.create(Entry) catch |err| {
                dropReservation(lease);
                self.mutex.unlock();
                return err;
            };
            entry.* = .{ .key = k, .tokens = tokens, .hidden = hidden, .layers = layers, .precision = precision, .pins = 1, .lease = lease.lease };
            self.pending.append(self.allocator, entry) catch |err| {
                self.allocator.destroy(entry);
                dropReservation(lease);
                self.mutex.unlock();
                return err;
            };
            self.reserved_bytes += size;
            self.mutex.unlock();
            // Host storage is allocated outside the lock; its bytes are
            // already reserved.
            const host_count = if (host) count else 0;
            const allocated = switch (precision) {
                .f16 => if (self.allocator.alloc(f16, host_count)) |v| blk: {
                    entry.host16 = v;
                    break :blk true;
                } else |_| false,
                .f32 => if (self.allocator.alloc(f32, host_count)) |v| blk: {
                    entry.host32 = v;
                    break :blk true;
                } else |_| false,
            };
            if (!allocated) {
                self.abandon(entry);
                return error.OutOfMemory;
            }
            return .{ .fill = entry };
        }
    }

    const Reserved = struct { lease: ?memory.AdmissionLease };

    /// Make room for `size` bytes under the byte budget and the admission
    /// controller, evicting least recently used unpinned entries. Null when
    /// it cannot fit.
    fn reserveLocked(self: *Cache, size: usize) !?Reserved {
        if (size > self.limit_bytes) return null;
        while (self.stats.bytes + self.reserved_bytes + size > self.limit_bytes) {
            if (!self.evictOne()) return null;
        }
        const admission = self.admission orelse return .{ .lease = null };
        const amounts: memory.AdmissionAmounts = if (admission.device) .{ .backend_kv_bytes = size } else .{ .host_kv_bytes = size };
        // Under memory pressure, give back older entries before refusing.
        while (true) {
            if (admission.controller.tryAcquire(admission.backend_class, admission.limits, amounts, true)) |lease| {
                return .{ .lease = lease };
            } else |_| {
                if (!self.evictOne()) return null;
            }
        }
    }

    fn dropReservation(reserved: Reserved) void {
        if (reserved.lease) |lease| {
            var owned = lease;
            owned.release();
        }
    }

    fn removePendingLocked(self: *Cache, entry: *Entry) void {
        const index = std.mem.indexOfScalar(*Entry, self.pending.items, entry).?;
        _ = self.pending.swapRemove(index);
        self.reserved_bytes -= entry.bytes();
    }

    /// Unpin an entry from `lookup` (either kind) or `acquire`. An entry that
    /// is no longer cached is freed on its last release.
    pub fn release(self: *Cache, entry: *Entry) void {
        platform.sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        entry.pins -= 1;
        if (entry.pins != 0) return;
        if (entry.orphaned) {
            // Its reservation and lease end with its last use.
            self.removePendingLocked(entry);
            self.destroy(entry);
        } else if (std.mem.indexOfScalar(*Entry, self.entries.items, entry) == null and
            std.mem.indexOfScalar(*Entry, self.pending.items, entry) == null) self.destroy(entry);
    }

    /// Give up a `fill` entry (the fill failed): drop its reservation and
    /// lease and free it. Waiters retry and one of them fills instead. The
    /// caller must not `release` it afterwards.
    pub fn abandon(self: *Cache, entry: *Entry) void {
        platform.sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        self.removePendingLocked(entry);
        entry.pins -= 1;
        std.debug.assert(entry.pins == 0);
        self.destroy(entry);
    }

    /// Evict the least recently used unpinned entry; false when none is.
    fn evictOne(self: *Cache) bool {
        var victim: ?usize = null;
        for (self.entries.items, 0..) |candidate, i| {
            if (candidate.pins != 0) continue;
            if (victim == null or candidate.last_used < self.entries.items[victim.?].last_used) victim = i;
        }
        const index = victim orelse return false;
        const evicted = self.entries.swapRemove(index);
        self.stats.bytes -= evicted.bytes();
        self.stats.evictions += 1;
        self.destroy(evicted);
        return true;
    }

    /// Publish a filled `fill` entry. Its bytes and lease were reserved by
    /// `lookup`, so it always fits; if the list cannot grow, the entry is
    /// used once and freed on release instead. The caller keeps its pin and
    /// must `release` it.
    pub fn publish(self: *Cache, entry: *Entry) void {
        platform.sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        self.entries.ensureUnusedCapacity(self.allocator, 1) catch {
            // The caller still reads the entry, so it stays charged (reserved
            // bytes and lease) until its last `release` frees it.
            entry.orphaned = true;
            return;
        };
        self.removePendingLocked(entry);
        self.entries.appendAssumeCapacity(entry);
        self.clock += 1;
        entry.last_used = self.clock;
        self.stats.bytes += entry.bytes();
        self.stats.entries = self.entries.items.len;
    }

    pub fn snapshot(self: *Cache) Stats {
        platform.sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        var out = self.stats;
        out.entries = self.entries.items.len;
        return out;
    }
};

/// Test helper: `lookup` that must reserve, then fill with `value` and publish.
fn fillFor(cache: *Cache, k: [32]u8, tokens: usize, layers: usize, hidden: usize, value: f16) !*Entry {
    const entry = switch (try cache.lookup(k, tokens, layers, hidden, true)) {
        .fill => |e| e,
        else => return error.TestUnexpectedResult,
    };
    @memset(entry.host16, value);
    cache.publish(entry);
    return entry;
}

test "laya trunk cache pins, evicts least recently used, and bypasses what cannot fit" {
    const a = std.testing.allocator;
    var cache = Cache.init(a, 2 * 2 * 4 * 8 * @sizeOf(f16));
    defer cache.deinit();
    const k1 = Cache.key(&.{ 1, 2, 3, 4 }, 1, 8);
    const k2 = Cache.key(&.{ 1, 2, 3, 5 }, 1, 8);
    const k3 = Cache.key(&.{ 9, 9, 9, 9 }, 1, 8);
    try std.testing.expect(!std.mem.eql(u8, &k1, &k2));
    try std.testing.expect(cache.acquire(k1) == null);
    for ([_][32]u8{ k1, k2 }) |k| cache.release(try fillFor(&cache, k, 4, 1, 8, 1));
    try std.testing.expectEqual(@as(usize, 2), cache.snapshot().entries);
    // A second lookup of a cached key is a hit, not a fill.
    switch (try cache.lookup(k1, 4, 1, 8, true)) {
        .hit => |e| cache.release(e),
        else => return error.TestUnexpectedResult,
    }
    // k1 is pinned, so reserving k3 must evict k2 even though k1 is older.
    const pinned = cache.acquire(k1).?;
    cache.release(try fillFor(&cache, k3, 4, 1, 8, 1));
    try std.testing.expect(cache.acquire(k2) == null);
    try std.testing.expectEqual(@as(u64, 1), cache.snapshot().evictions);
    cache.release(pinned);
    // An entry larger than the whole budget is bypassed before any encoding.
    try std.testing.expect((try cache.lookup(Cache.key(&.{7}, 3, 64), 4, 3, 64, false)) == .bypass);
    try std.testing.expectEqual(@as(usize, 2), cache.snapshot().entries);
    // An abandoned fill frees its reservation, so the key can be filled again.
    const k4 = Cache.key(&.{4}, 1, 8);
    switch (try cache.lookup(k4, 4, 1, 8, true)) {
        .fill => |e| cache.abandon(e),
        else => return error.TestUnexpectedResult,
    }
    try std.testing.expectEqual(@as(usize, 0), cache.reserved_bytes);
    cache.release(try fillFor(&cache, k4, 4, 1, 8, 1));
}

test "laya trunk cache stores f16 and f32 slots and charges admission before filling" {
    const a = std.testing.allocator;
    for ([_]Precision{ .f16, .f32 }) |precision| {
        var cache = Cache.init(a, 1 << 20);
        defer cache.deinit();
        cache.setPrecision(precision);
        const entry = switch (try cache.lookup(Cache.key(&.{1}, 1, 4), 2, 1, 4, true)) {
            .fill => |e| e,
            else => return error.TestUnexpectedResult,
        };
        const values = [_]f32{ 0.5, -1.25, 3.0e-3, 1000.0, 1, 2, 3, 4 };
        entry.store(1, &values);
        var out: [8]f32 = undefined;
        entry.load(1, &out);
        for (values, out) |want, got| try std.testing.expectApproxEqRel(want, got, @as(f32, if (precision == .f16) 1e-3 else 1e-7));
        try std.testing.expectEqual(@as(usize, 2 * 1 * 2 * 4 * (if (precision == .f16) @as(usize, 2) else 4)), entry.bytes());
        cache.publish(entry);
        cache.release(entry);
    }
    // A controller with no room refuses the reservation: nothing is filled.
    var controller: memory.AdmissionController = .{};
    var cache = Cache.init(a, 1 << 20);
    defer cache.deinit();
    cache.configureAdmission(.{ .controller = &controller, .backend_class = .cpu, .limits = .{ .host_limit_bytes = 16 }, .device = false });
    try std.testing.expect((try cache.lookup(Cache.key(&.{2}, 1, 4), 2, 1, 4, true)) == .bypass);
    try std.testing.expectEqual(@as(usize, 0), cache.snapshot().entries);
    try std.testing.expectEqual(@as(u64, 1), cache.snapshot().refusals);
    // With room, the lease is taken at reservation, held while cached, and
    // released on eviction.
    cache.configureAdmission(.{ .controller = &controller, .backend_class = .cpu, .limits = .{ .host_limit_bytes = 1 << 20 }, .device = false });
    const kept = switch (try cache.lookup(Cache.key(&.{3}, 1, 4), 2, 1, 4, true)) {
        .fill => |e| e,
        else => return error.TestUnexpectedResult,
    };
    try std.testing.expectEqual(kept.bytes(), controller.snapshot().host_kv_bytes);
    cache.publish(kept);
    cache.release(kept);
    try std.testing.expectEqual(kept.bytes(), controller.snapshot().host_kv_bytes);
    cache.deinit();
    cache = Cache.init(a, 1 << 20);
    try std.testing.expectEqual(@as(usize, 0), controller.snapshot().host_kv_bytes);
}

test "laya trunk cache keeps an entry it could not publish charged until its last release" {
    var controller: memory.AdmissionController = .{};
    // lookup allocates the entry, grows `pending`, then the host slice;
    // publish's growth of `entries` is the fourth allocation.
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 3 });
    var cache = Cache.init(failing.allocator(), 1 << 20);
    defer cache.deinit();
    cache.configureAdmission(.{ .controller = &controller, .backend_class = .cpu, .limits = .{ .host_limit_bytes = 1 << 20 }, .device = false });
    const k = Cache.key(&.{8}, 1, 4);
    const entry = switch (try cache.lookup(k, 2, 1, 4, true)) {
        .fill => |e| e,
        else => return error.TestUnexpectedResult,
    };
    cache.publish(entry);
    try std.testing.expect(entry.orphaned);
    try std.testing.expectEqual(@as(usize, 0), cache.snapshot().entries);
    // Still in use by its filler: its bytes and lease stay charged.
    try std.testing.expectEqual(entry.bytes(), cache.reserved_bytes);
    try std.testing.expectEqual(entry.bytes(), controller.snapshot().host_kv_bytes);
    // Nobody waits on an entry that will never be published.
    platform.sync.lockYielding(&cache.mutex);
    try std.testing.expect(!cache.pendingLocked(k));
    cache.mutex.unlock();
    cache.release(entry);
    try std.testing.expectEqual(@as(usize, 0), cache.reserved_bytes);
    try std.testing.expectEqual(@as(usize, 0), controller.snapshot().host_kv_bytes);
}

test "laya trunk cache keeps concurrent misses inside the budget and fills each key once" {
    const a = std.testing.allocator;
    const tokens = 4;
    const layers = 1;
    const hidden = 8;
    const entry_bytes = 2 * layers * tokens * hidden * @sizeOf(f16);
    // Room for exactly one entry.
    var cache = Cache.init(a, entry_bytes);
    defer cache.deinit();
    const Worker = struct {
        cache: *Cache,
        key: [32]u8,
        fills: *std.atomic.Value(u32),
        hits: *std.atomic.Value(u32),
        bypasses: *std.atomic.Value(u32),
        over_budget: *std.atomic.Value(bool),
        failed: *std.atomic.Value(bool),
        fn run(self: @This()) void {
            const result = self.cache.lookup(self.key, tokens, layers, hidden, true) catch {
                self.failed.store(true, .seq_cst);
                return;
            };
            switch (result) {
                .fill => |entry| {
                    _ = self.fills.fetchAdd(1, .seq_cst);
                    // Encoding takes a while; others must wait, not refill.
                    platform.sync.lockYielding(&self.cache.mutex);
                    if (self.cache.stats.bytes + self.cache.reserved_bytes > self.cache.limit_bytes) self.over_budget.store(true, .seq_cst);
                    self.cache.mutex.unlock();
                    for (0..200) |_| platform.time.yieldBriefly();
                    @memset(entry.host16, 3);
                    self.cache.publish(entry);
                    self.cache.release(entry);
                },
                .hit => |entry| {
                    _ = self.hits.fetchAdd(1, .seq_cst);
                    if (entry.host16[0] != 3) self.failed.store(true, .seq_cst);
                    self.cache.release(entry);
                },
                .bypass => _ = self.bypasses.fetchAdd(1, .seq_cst),
            }
        }
    };
    var fills = std.atomic.Value(u32).init(0);
    var hits = std.atomic.Value(u32).init(0);
    var bypasses = std.atomic.Value(u32).init(0);
    var over_budget = std.atomic.Value(bool).init(false);
    var failed = std.atomic.Value(bool).init(false);
    const shared = Cache.key(&.{ 5, 5, 5 }, layers, hidden);
    var threads: [8]std.Thread = undefined;
    for (&threads, 0..) |*thread, i| {
        // Six threads miss on one key; two on keys of their own.
        const k = if (i < 6) shared else Cache.key(&.{@intCast(i)}, layers, hidden);
        thread.* = try std.Thread.spawn(.{}, Worker.run, .{Worker{ .cache = &cache, .key = k, .fills = &fills, .hits = &hits, .bypasses = &bypasses, .over_budget = &over_budget, .failed = &failed }});
    }
    for (threads) |thread| thread.join();
    try std.testing.expect(!failed.load(.seq_cst));
    try std.testing.expect(!over_budget.load(.seq_cst));
    try std.testing.expectEqual(@as(u32, 8), fills.load(.seq_cst) + hits.load(.seq_cst) + bypasses.load(.seq_cst));
    const stats = cache.snapshot();
    try std.testing.expect(stats.bytes <= entry_bytes);
    try std.testing.expectEqual(@as(usize, 0), cache.reserved_bytes);
    // The shared key is filled once unless it was evicted and refilled
    // between waves; never more fills than distinct keys plus evictions.
    try std.testing.expect(fills.load(.seq_cst) <= 3 + stats.evictions);
}
