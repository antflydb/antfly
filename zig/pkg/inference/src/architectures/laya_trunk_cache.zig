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
//! published entry holds a KV lease on the model's admission controller.
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

pub const Stats = struct { hits: u64 = 0, misses: u64 = 0, entries: usize = 0, bytes: usize = 0, evictions: u64 = 0, refusals: u64 = 0 };

pub const Cache = struct {
    allocator: std.mem.Allocator,
    limit_bytes: usize,
    min_tokens: usize = default_min_tokens,
    precision: Precision = .f16,
    admission: ?Admission = null,
    mutex: std.atomic.Mutex = .unlocked,
    entries: std.ArrayListUnmanaged(*Entry) = .empty,
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
        for (self.entries.items) |entry| self.destroy(entry);
        self.entries.deinit(self.allocator);
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
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("antfly-laya-trunk/v1");
        hash.update(std.mem.asBytes(&layers));
        hash.update(std.mem.asBytes(&hidden));
        hash.update(std.mem.sliceAsBytes(ids));
        return hash.finalResult();
    }

    /// Pin and return a cached trunk, or null on a miss.
    pub fn acquire(self: *Cache, k: [32]u8) ?*Entry {
        platform.sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        for (self.entries.items) |entry| if (std.mem.eql(u8, &entry.key, &k)) {
            entry.pins += 1;
            self.clock += 1;
            entry.last_used = self.clock;
            self.stats.hits += 1;
            return entry;
        };
        self.stats.misses += 1;
        return null;
    }

    pub fn release(self: *Cache, entry: *Entry) void {
        platform.sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        entry.pins -= 1;
        if (entry.pins == 0 and std.mem.indexOfScalar(*Entry, self.entries.items, entry) == null) self.destroy(entry);
    }

    /// Allocate an unpublished, pinned entry for the caller to fill. Device
    /// entries (`host = false`) get no host storage.
    pub fn create(self: *Cache, k: [32]u8, tokens: usize, layers: usize, hidden: usize, host: bool) !*Entry {
        const count = if (host) try std.math.mul(usize, try std.math.mul(usize, 2 * layers, tokens), hidden) else 0;
        const entry = try self.allocator.create(Entry);
        errdefer self.allocator.destroy(entry);
        entry.* = .{ .key = k, .tokens = tokens, .hidden = hidden, .layers = layers, .precision = self.precision, .pins = 1 };
        switch (self.precision) {
            .f16 => entry.host16 = try self.allocator.alloc(f16, count),
            .f32 => entry.host32 = try self.allocator.alloc(f32, count),
        }
        return entry;
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

    /// Publish a filled entry when it fits the budget, evicting least recently
    /// used unpinned entries. The caller keeps its pin either way and must
    /// `release` it; an unpublished entry is freed on its last release.
    pub fn publish(self: *Cache, entry: *Entry) void {
        platform.sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        if (entry.bytes() > self.limit_bytes) return;
        for (self.entries.items) |existing| if (std.mem.eql(u8, &existing.key, &entry.key)) return;
        while (self.stats.bytes + entry.bytes() > self.limit_bytes) {
            if (!self.evictOne()) return;
        }
        if (self.admission) |admission| {
            const amounts: memory.AdmissionAmounts = if (admission.device) .{ .backend_kv_bytes = entry.bytes() } else .{ .host_kv_bytes = entry.bytes() };
            // Under memory pressure, give back older entries before refusing.
            while (true) {
                if (admission.controller.tryAcquire(admission.backend_class, admission.limits, amounts, true)) |lease| {
                    entry.lease = lease;
                    break;
                } else |_| {
                    if (!self.evictOne()) {
                        self.stats.refusals += 1;
                        return;
                    }
                }
            }
        }
        self.entries.append(self.allocator, entry) catch return;
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

test "laya trunk cache pins, evicts least recently used, and frees unpublished entries" {
    const a = std.testing.allocator;
    var cache = Cache.init(a, 2 * 2 * 4 * 8 * @sizeOf(f16));
    defer cache.deinit();
    const k1 = Cache.key(&.{ 1, 2, 3, 4 }, 1, 8);
    const k2 = Cache.key(&.{ 1, 2, 3, 5 }, 1, 8);
    const k3 = Cache.key(&.{ 9, 9, 9, 9 }, 1, 8);
    try std.testing.expect(!std.mem.eql(u8, &k1, &k2));
    try std.testing.expect(cache.acquire(k1) == null);
    for ([_][32]u8{ k1, k2 }) |k| {
        const entry = try cache.create(k, 4, 1, 8, true);
        @memset(entry.host16, 1);
        cache.publish(entry);
        cache.release(entry);
    }
    try std.testing.expectEqual(@as(usize, 2), cache.snapshot().entries);
    // k1 is pinned, so publishing k3 must evict k2 even though k1 is older.
    const pinned = cache.acquire(k1).?;
    const third = try cache.create(k3, 4, 1, 8, true);
    cache.publish(third);
    cache.release(third);
    try std.testing.expect(cache.acquire(k2) == null);
    try std.testing.expectEqual(@as(u64, 1), cache.snapshot().evictions);
    cache.release(pinned);
    // An entry larger than the whole budget is never published but still freed.
    const huge = try cache.create(Cache.key(&.{7}, 3, 64), 4, 3, 64, false);
    cache.publish(huge);
    cache.release(huge);
    try std.testing.expectEqual(@as(usize, 2), cache.snapshot().entries);
}

test "laya trunk cache stores f16 and f32 slots and charges admission" {
    const a = std.testing.allocator;
    for ([_]Precision{ .f16, .f32 }) |precision| {
        var cache = Cache.init(a, 1 << 20);
        defer cache.deinit();
        cache.precision = precision;
        const entry = try cache.create(Cache.key(&.{1}, 1, 4), 2, 1, 4, true);
        const values = [_]f32{ 0.5, -1.25, 3.0e-3, 1000.0, 1, 2, 3, 4 };
        entry.store(1, &values);
        var out: [8]f32 = undefined;
        entry.load(1, &out);
        for (values, out) |want, got| try std.testing.expectApproxEqRel(want, got, @as(f32, if (precision == .f16) 1e-3 else 1e-7));
        try std.testing.expectEqual(@as(usize, 2 * 1 * 2 * 4 * (if (precision == .f16) @as(usize, 2) else 4)), entry.bytes());
        cache.publish(entry);
        cache.release(entry);
    }
    // A controller with no room refuses the entry, which is then not cached.
    var controller: memory.AdmissionController = .{};
    var cache = Cache.init(a, 1 << 20);
    defer cache.deinit();
    cache.configureAdmission(.{ .controller = &controller, .backend_class = .cpu, .limits = .{ .host_limit_bytes = 16 }, .device = false });
    const entry = try cache.create(Cache.key(&.{2}, 1, 4), 2, 1, 4, true);
    cache.publish(entry);
    cache.release(entry);
    try std.testing.expectEqual(@as(usize, 0), cache.snapshot().entries);
    try std.testing.expectEqual(@as(u64, 1), cache.snapshot().refusals);
    // With room, the lease is held while cached and released on eviction.
    cache.configureAdmission(.{ .controller = &controller, .backend_class = .cpu, .limits = .{ .host_limit_bytes = 1 << 20 }, .device = false });
    const kept = try cache.create(Cache.key(&.{3}, 1, 4), 2, 1, 4, true);
    cache.publish(kept);
    cache.release(kept);
    try std.testing.expectEqual(kept.bytes(), controller.snapshot().host_kv_bytes);
    cache.deinit();
    cache = Cache.init(a, 1 << 20);
    try std.testing.expectEqual(@as(usize, 0), controller.snapshot().host_kv_bytes);
}
