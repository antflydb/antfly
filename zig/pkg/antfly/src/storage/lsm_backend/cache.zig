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

const std = @import("std");
const platform_sync = @import("antfly_platform").sync;
const builtin = @import("builtin");
const platform = @import("antfly_platform");

const Allocator = std.mem.Allocator;
const lsm_table_file = @import("../lsm/table_file.zig");
const resource_manager_mod = @import("../resource_manager.zig");
const state_mod = @import("state.zig");

const State = state_mod.State;
const TableIndex = lsm_table_file.TableIndex;
const CounterU64 = platform.atomic.Value(u64);

pub const DefaultCacheSizeBytes: usize = 256 * 1024 * 1024;
pub const DefaultTableBlockSize: usize = 32 * 1024;
const default_shard_count: usize = if (builtin.os.tag == .freestanding) 1 else 16;

pub const Kind = enum {
    run_state,
    run_table_raw,
    run_table_index,
    run_table_block,
    run_table_physical_block,
};

pub const KindStats = struct {
    hits: u64 = 0,
    misses: u64 = 0,
    inserts: u64 = 0,
    transient_serves: u64 = 0,
    policy_bypasses: u64 = 0,
    evictions: u64 = 0,
    invalidations: u64 = 0,
    waits: u64 = 0,
    used_bytes: usize = 0,
    peak_used_bytes: usize = 0,
};

pub const Stats = struct {
    used_bytes: usize = 0,
    peak_used_bytes: usize = 0,
    data_block_used_bytes: usize = 0,
    data_block_peak_used_bytes: usize = 0,
    entry_count: usize = 0,
    run_state: KindStats = .{},
    run_table_raw: KindStats = .{},
    run_table_index: KindStats = .{},
    run_table_block: KindStats = .{},
    run_table_physical_block: KindStats = .{},
};

pub const Cache = struct {
    const Key = struct {
        kind: Kind,
        run_id: u64,
        generation: u64,
        path: []const u8,
        block_offset: u64 = 0,
        block_len: u32 = 0,
    };

    const KeyContext = struct {
        pub fn hash(_: @This(), key: anytype) u64 {
            return hashKey(key.path, key.run_id, key.generation, key.kind, key.block_offset, key.block_len);
        }

        pub fn eql(_: @This(), a: anytype, b: Key) bool {
            return a.kind == b.kind and
                a.run_id == b.run_id and
                a.generation == b.generation and
                a.block_offset == b.block_offset and
                a.block_len == b.block_len and
                std.mem.eql(u8, a.path, b.path);
        }
    };

    const EntryMap = std.HashMapUnmanaged(Key, *Entry, KeyContext, 80);
    const PendingMap = std.HashMapUnmanaged(Key, PendingLoad, KeyContext, 80);
    const priority_count: usize = 4;

    const Entry = struct {
        kind: Kind,
        run_id: u64,
        generation: u64,
        path: []u8,
        block_offset: u64 = 0,
        block_len: u32 = 0,
        value: Value,
        byte_cost: usize,
        ref_count: usize,
        transient_ref_count: std.atomic.Value(usize) = .init(0),
        last_access: u64,
        invalidated: bool = false,
        lru_prev: ?*Entry = null,
        lru_next: ?*Entry = null,

        fn key(self: *const Entry) Key {
            return .{
                .kind = self.kind,
                .run_id = self.run_id,
                .generation = self.generation,
                .path = self.path,
                .block_offset = self.block_offset,
                .block_len = self.block_len,
            };
        }

        fn deinit(self: *Entry, allocator: Allocator) void {
            allocator.free(self.path);
            self.value.deinit(allocator);
            self.* = undefined;
        }
    };

    const Value = union(Kind) {
        run_state: State,
        run_table_raw: []u8,
        run_table_index: TableIndex,
        run_table_block: []u8,
        run_table_physical_block: []u8,

        fn deinit(self: *Value, allocator: Allocator) void {
            switch (self.*) {
                .run_state => |*state| state.deinit(allocator),
                .run_table_raw => |raw| allocator.free(raw),
                .run_table_index => |*index| index.deinit(allocator),
                .run_table_block => |raw| allocator.free(raw),
                .run_table_physical_block => |raw| allocator.free(raw),
            }
            self.* = undefined;
        }
    };

    const Shard = struct {
        mutex: std.atomic.Mutex = .unlocked,
        entries: EntryMap = .empty,
        lru_heads: [priority_count]?*Entry = @as([priority_count]?*Entry, @splat(null)),
        lru_tails: [priority_count]?*Entry = @as([priority_count]?*Entry, @splat(null)),
        pending_sync: PendingSync = .{},
        pending_loads: PendingMap = .empty,
    };

    const PendingLoad = struct {};

    const AtomicKindStats = struct {
        hits: CounterU64 = .init(0),
        misses: CounterU64 = .init(0),
        inserts: CounterU64 = .init(0),
        transient_serves: CounterU64 = .init(0),
        policy_bypasses: CounterU64 = .init(0),
        evictions: CounterU64 = .init(0),
        invalidations: CounterU64 = .init(0),
        waits: CounterU64 = .init(0),

        fn snapshot(self: *const AtomicKindStats) KindStats {
            return .{
                .hits = self.hits.load(.monotonic),
                .misses = self.misses.load(.monotonic),
                .inserts = self.inserts.load(.monotonic),
                .transient_serves = self.transient_serves.load(.monotonic),
                .policy_bypasses = self.policy_bypasses.load(.monotonic),
                .evictions = self.evictions.load(.monotonic),
                .invalidations = self.invalidations.load(.monotonic),
                .waits = self.waits.load(.monotonic),
            };
        }
    };

    const AtomicStats = struct {
        run_state: AtomicKindStats = .{},
        run_table_raw: AtomicKindStats = .{},
        run_table_index: AtomicKindStats = .{},
        run_table_block: AtomicKindStats = .{},
        run_table_physical_block: AtomicKindStats = .{},

        fn byKind(self: *AtomicStats, kind: Kind) *AtomicKindStats {
            return switch (kind) {
                .run_state => &self.run_state,
                .run_table_raw => &self.run_table_raw,
                .run_table_index => &self.run_table_index,
                .run_table_block => &self.run_table_block,
                .run_table_physical_block => &self.run_table_physical_block,
            };
        }

        fn snapshot(
            self: *const AtomicStats,
            used_bytes: usize,
            peak_used_bytes: usize,
            data_block_used_bytes: usize,
            data_block_peak_used_bytes: usize,
            entry_count: usize,
            kind_bytes: [@typeInfo(Kind).@"enum".field_names.len]usize,
            kind_peak_bytes: [@typeInfo(Kind).@"enum".field_names.len]usize,
        ) Stats {
            var run_state = self.run_state.snapshot();
            var run_table_raw = self.run_table_raw.snapshot();
            var run_table_index = self.run_table_index.snapshot();
            var run_table_block = self.run_table_block.snapshot();
            var run_table_physical_block = self.run_table_physical_block.snapshot();
            run_state.used_bytes = kind_bytes[@backingInt(Kind.run_state)];
            run_table_raw.used_bytes = kind_bytes[@backingInt(Kind.run_table_raw)];
            run_table_index.used_bytes = kind_bytes[@backingInt(Kind.run_table_index)];
            run_table_block.used_bytes = kind_bytes[@backingInt(Kind.run_table_block)];
            run_table_physical_block.used_bytes = kind_bytes[@backingInt(Kind.run_table_physical_block)];
            run_state.peak_used_bytes = kind_peak_bytes[@backingInt(Kind.run_state)];
            run_table_raw.peak_used_bytes = kind_peak_bytes[@backingInt(Kind.run_table_raw)];
            run_table_index.peak_used_bytes = kind_peak_bytes[@backingInt(Kind.run_table_index)];
            run_table_block.peak_used_bytes = kind_peak_bytes[@backingInt(Kind.run_table_block)];
            run_table_physical_block.peak_used_bytes = kind_peak_bytes[@backingInt(Kind.run_table_physical_block)];
            return .{
                .used_bytes = used_bytes,
                .peak_used_bytes = peak_used_bytes,
                .data_block_used_bytes = data_block_used_bytes,
                .data_block_peak_used_bytes = data_block_peak_used_bytes,
                .entry_count = entry_count,
                .run_state = run_state,
                .run_table_raw = run_table_raw,
                .run_table_index = run_table_index,
                .run_table_block = run_table_block,
                .run_table_physical_block = run_table_physical_block,
            };
        }
    };

    allocator: Allocator,
    max_bytes: usize,
    shards: []Shard,
    used_bytes: std.atomic.Value(usize) = .init(0),
    peak_used_bytes: std.atomic.Value(usize) = .init(0),
    data_block_used_bytes: std.atomic.Value(usize) = .init(0),
    data_block_peak_used_bytes: std.atomic.Value(usize) = .init(0),
    entry_count: std.atomic.Value(usize) = .init(0),
    kind_bytes: [@typeInfo(Kind).@"enum".field_names.len]std.atomic.Value(usize) = .{
        .init(0),
        .init(0),
        .init(0),
        .init(0),
        .init(0),
    },
    kind_peak_bytes: [@typeInfo(Kind).@"enum".field_names.len]std.atomic.Value(usize) = .{
        .init(0),
        .init(0),
        .init(0),
        .init(0),
        .init(0),
    },
    access_clock: CounterU64 = .init(0),
    evict_cursor: std.atomic.Value(usize) = .init(0),
    pressure_target_bytes: std.atomic.Value(usize) = .init(0),
    evict_mutex: std.atomic.Mutex = .unlocked,
    resource_accounting_mutex: std.atomic.Mutex = .unlocked,
    resource_manager: ?*resource_manager_mod.ResourceManager = null,
    reclaimer_identity: u64 = 0,
    resource_accounted_bytes: u64 = 0,
    stats: AtomicStats = .{},

    pub fn init(allocator: Allocator, max_bytes: usize) Cache {
        const shards = allocator.alloc(Shard, default_shard_count) catch @panic("OOM");
        @memset(shards, .{});
        return .{
            .allocator = allocator,
            .max_bytes = max_bytes,
            .shards = shards,
        };
    }

    pub fn deinit(self: *Cache) void {
        if (self.resource_manager) |manager| {
            manager.unregisterReclaimer(self.reclaimer_identity);
            self.reclaimer_identity = 0;
        }
        self.releaseResourceUsage();
        for (self.shards) |*shard| {
            for (0..priority_count) |priority| {
                var current = shard.lru_heads[priority];
                while (current) |entry| {
                    const next = entry.lru_next;
                    entry.deinit(self.allocator);
                    self.allocator.destroy(entry);
                    current = next;
                }
            }
            shard.entries.deinit(self.allocator);
            shard.pending_sync.lock();
            var pending_it = shard.pending_loads.iterator();
            while (pending_it.next()) |pending| {
                self.allocator.free(pending.key_ptr.path);
            }
            shard.pending_loads.deinit(self.allocator);
            shard.pending_sync.unlock();
        }
        self.allocator.free(self.shards);
        self.* = undefined;
    }

    pub fn attachResourceManager(self: *Cache, resource_manager: *resource_manager_mod.ResourceManager) void {
        const locked = lockAtomic(&self.resource_accounting_mutex);
        if (self.resource_manager == resource_manager and self.reclaimer_identity != 0) {
            if (locked) self.resource_accounting_mutex.unlock();
            return;
        }
        const old_manager = self.resource_manager;
        const old_reclaimer = self.reclaimer_identity;
        const current_bytes: u64 = @intCast(self.currentBytes());
        if (old_manager == null or old_manager.? != resource_manager) {
            if (old_manager) |manager| {
                manager.observeUsage(.lsm_block_table_cache, &self.resource_accounted_bytes, 0);
            } else {
                self.resource_accounted_bytes = 0;
            }
            self.resource_manager = resource_manager;
            resource_manager.observeUsage(.lsm_block_table_cache, &self.resource_accounted_bytes, current_bytes);
        }
        self.reclaimer_identity = 0;
        if (locked) self.resource_accounting_mutex.unlock();

        if (old_manager) |manager| manager.unregisterReclaimer(old_reclaimer);
        const identity = resource_manager.registerReclaimer(
            .lsm_block_table_cache,
            self,
            reclaimForResourceManager,
        ) catch return;
        const store_locked = lockAtomic(&self.resource_accounting_mutex);
        if (self.resource_manager == resource_manager and self.reclaimer_identity == 0) {
            self.reclaimer_identity = identity;
            if (store_locked) self.resource_accounting_mutex.unlock();
        } else {
            if (store_locked) self.resource_accounting_mutex.unlock();
            resource_manager.unregisterReclaimer(identity);
        }
    }

    fn reclaimForResourceManager(context: *anyopaque, target_bytes: u64) u64 {
        const self: *Cache = @ptrCast(@alignCast(context));
        if (target_bytes == 0) return 0;
        const before = self.currentBytes();
        while (before -| self.currentBytes() < target_bytes) {
            if (!self.evictOne()) break;
        }
        return @intCast(before -| self.currentBytes());
    }

    pub fn snapshotStats(self: *const Cache) Stats {
        var by_kind: [@typeInfo(Kind).@"enum".field_names.len]usize = undefined;
        var peak_by_kind: [@typeInfo(Kind).@"enum".field_names.len]usize = undefined;
        inline for (0..by_kind.len) |i| by_kind[i] = self.kind_bytes[i].load(.monotonic);
        inline for (0..peak_by_kind.len) |i| {
            peak_by_kind[i] = @max(by_kind[i], self.kind_peak_bytes[i].load(.monotonic));
        }
        const used_bytes = self.currentBytes();
        const data_block_used_bytes = self.data_block_used_bytes.load(.monotonic);
        return self.stats.snapshot(
            used_bytes,
            @max(used_bytes, self.peak_used_bytes.load(.monotonic)),
            data_block_used_bytes,
            @max(data_block_used_bytes, self.data_block_peak_used_bytes.load(.monotonic)),
            self.entryCount(),
            by_kind,
            peak_by_kind,
        );
    }

    pub fn valueAllocator(self: *const Cache) Allocator {
        return self.allocator;
    }

    pub fn retainRunState(self: *Cache, path: []const u8, run_id: u64, generation: u64) ?Handle {
        return self.retain(path, run_id, generation, .run_state);
    }

    pub fn retainRunTableRaw(self: *Cache, path: []const u8, run_id: u64, generation: u64) ?Handle {
        return self.retain(path, run_id, generation, .run_table_raw);
    }

    pub fn retainRunTableIndex(self: *Cache, path: []const u8, run_id: u64, generation: u64) ?Handle {
        return self.retain(path, run_id, generation, .run_table_index);
    }

    pub fn retainRunTableBlock(self: *Cache, path: []const u8, run_id: u64, generation: u64, block_offset: u64, block_len: u32) ?Handle {
        return self.retainWithBlock(path, run_id, generation, .run_table_block, block_offset, block_len);
    }

    pub fn retainRunTablePhysicalBlock(self: *Cache, path: []const u8, run_id: u64, generation: u64, block_offset: u64, block_len: u32) ?Handle {
        return self.retainWithBlock(path, run_id, generation, .run_table_physical_block, block_offset, block_len);
    }

    pub fn putRunState(self: *Cache, path: []const u8, run_id: u64, generation: u64, state: State) !Handle {
        errdefer {
            var cleanup = state;
            cleanup.deinit(self.allocator);
        }
        return try self.put(path, run_id, generation, .{ .run_state = state }, estimateStateCost(path, &state));
    }

    pub fn putRunTableRaw(self: *Cache, path: []const u8, run_id: u64, generation: u64, raw: []u8) !Handle {
        errdefer self.allocator.free(raw);
        return try self.put(path, run_id, generation, .{ .run_table_raw = raw }, estimateRawTableCost(path, raw));
    }

    pub fn putRunTableIndex(self: *Cache, path: []const u8, run_id: u64, generation: u64, index: TableIndex) !Handle {
        errdefer {
            var cleanup = index;
            cleanup.deinit(self.allocator);
        }
        return try self.put(path, run_id, generation, .{ .run_table_index = index }, estimateTableIndexCost(path, &index));
    }

    pub fn putRunTableBlock(self: *Cache, path: []const u8, run_id: u64, generation: u64, block_offset: u64, block_len: u32, block: []u8) !Handle {
        errdefer self.allocator.free(block);
        return try self.putWithBlock(
            path,
            run_id,
            generation,
            .{ .run_table_block = block },
            estimateTableBlockCost(path, block),
            block_offset,
            block_len,
            false,
        );
    }

    pub fn putTransientRunTableBlock(self: *Cache, path: []const u8, run_id: u64, generation: u64, block_offset: u64, block_len: u32, block: []u8) !Handle {
        errdefer self.allocator.free(block);
        return try self.putWithBlock(
            path,
            run_id,
            generation,
            .{ .run_table_block = block },
            estimateTableBlockCost(path, block),
            block_offset,
            block_len,
            true,
        );
    }

    pub fn putRunTablePhysicalBlock(self: *Cache, path: []const u8, run_id: u64, generation: u64, block_offset: u64, block_len: u32, block: []u8) !Handle {
        errdefer self.allocator.free(block);
        return try self.putWithBlock(
            path,
            run_id,
            generation,
            .{ .run_table_physical_block = block },
            estimateTableBlockCost(path, block),
            block_offset,
            block_len,
            false,
        );
    }

    pub fn putTransientRunTablePhysicalBlock(self: *Cache, path: []const u8, run_id: u64, generation: u64, block_offset: u64, block_len: u32, block: []u8) !Handle {
        errdefer self.allocator.free(block);
        return try self.putWithBlock(
            path,
            run_id,
            generation,
            .{ .run_table_physical_block = block },
            estimateTableBlockCost(path, block),
            block_offset,
            block_len,
            true,
        );
    }

    pub fn beginLoad(self: *Cache, path: []const u8, run_id: u64, generation: u64, kind: Kind) !void {
        return try self.beginLoadWithBlock(path, run_id, generation, kind, 0, 0);
    }

    pub fn beginLoadWithBlock(self: *Cache, path: []const u8, run_id: u64, generation: u64, kind: Kind, block_offset: u64, block_len: u32) !void {
        const key = makeKey(path, run_id, generation, kind, block_offset, block_len);
        const shard = self.shardForKey(key);
        while (true) {
            shard.pending_sync.lock();
            defer shard.pending_sync.unlock();

            if (shard.pending_loads.getPtrAdapted(key, KeyContext{}) != null) {
                self.bumpWait(kind);
                shard.pending_sync.wait();
                continue;
            }

            const gop = try shard.pending_loads.getOrPutContextAdapted(self.allocator, key, KeyContext{}, KeyContext{});
            if (!gop.found_existing) {
                gop.key_ptr.* = try copyKey(self.allocator, key);
                gop.value_ptr.* = .{};
                return;
            }
        }
    }

    pub fn finishLoad(self: *Cache, path: []const u8, run_id: u64, generation: u64, kind: Kind) void {
        self.finishLoadWithBlock(path, run_id, generation, kind, 0, 0);
    }

    pub fn finishLoadWithBlock(self: *Cache, path: []const u8, run_id: u64, generation: u64, kind: Kind, block_offset: u64, block_len: u32) void {
        const key = makeKey(path, run_id, generation, kind, block_offset, block_len);
        const shard = self.shardForKey(key);
        shard.pending_sync.lock();
        defer shard.pending_sync.unlock();

        if (shard.pending_loads.fetchRemoveAdapted(key, KeyContext{})) |removed| {
            self.allocator.free(removed.key.path);
            shard.pending_sync.broadcast();
        }
    }

    pub fn invalidatePath(self: *Cache, path: []const u8) void {
        for (self.shards) |*shard| {
            const locked = lockAtomic(&shard.mutex);

            for (0..priority_count) |priority| {
                var current = shard.lru_heads[priority];
                while (current) |entry| {
                    const next = entry.lru_next;
                    if (std.mem.eql(u8, entry.path, path)) {
                        entry.invalidated = true;
                        self.bumpInvalidation(entry.kind);
                        if (entry.ref_count == 0) self.removeEntryLocked(shard, entry);
                    }
                    current = next;
                }
            }
            if (locked) shard.mutex.unlock();
        }
    }

    pub fn invalidatePrefix(self: *Cache, prefix: []const u8) void {
        for (self.shards) |*shard| {
            const locked = lockAtomic(&shard.mutex);

            for (0..priority_count) |priority| {
                var current = shard.lru_heads[priority];
                while (current) |entry| {
                    const next = entry.lru_next;
                    if (std.mem.startsWith(u8, entry.path, prefix)) {
                        entry.invalidated = true;
                        self.bumpInvalidation(entry.kind);
                        if (entry.ref_count == 0) self.removeEntryLocked(shard, entry);
                    }
                    current = next;
                }
            }
            if (locked) shard.mutex.unlock();
        }
    }

    pub fn currentBytes(self: *const Cache) usize {
        return self.used_bytes.load(.monotonic);
    }

    pub fn entryCount(self: *const Cache) usize {
        return self.entry_count.load(.monotonic);
    }

    fn pendingLoadCountForTests(self: *Cache) usize {
        var count: usize = 0;
        for (self.shards) |*shard| {
            shard.pending_sync.lock();
            count += shard.pending_loads.count();
            shard.pending_sync.unlock();
        }
        return count;
    }

    fn retain(self: *Cache, path: []const u8, run_id: u64, generation: u64, kind: Kind) ?Handle {
        return self.retainWithBlock(path, run_id, generation, kind, 0, 0);
    }

    fn retainWithBlock(self: *Cache, path: []const u8, run_id: u64, generation: u64, kind: Kind, block_offset: u64, block_len: u32) ?Handle {
        const key = makeKey(path, run_id, generation, kind, block_offset, block_len);
        const shard = self.shardForKey(key);
        const locked = lockAtomic(&shard.mutex);
        defer if (locked) shard.mutex.unlock();

        if (self.findEntryLocked(shard, key)) |entry| {
            entry.ref_count += 1;
            entry.last_access = self.nextAccess();
            self.touchEntryLocked(shard, entry);
            self.bumpHit(kind);
            return .{
                .cache = self,
                .allocator = self.allocator,
                .entry = entry,
                .kind = kind,
            };
        }

        self.bumpMiss(kind);
        return null;
    }

    fn retainEntry(self: *Cache, entry: *Entry) void {
        const shard = self.shardForKey(entry.key());
        const locked = lockAtomic(&shard.mutex);
        defer if (locked) shard.mutex.unlock();

        std.debug.assert(entry.ref_count > 0);
        entry.ref_count += 1;
        entry.last_access = self.nextAccess();
        self.touchEntryLocked(shard, entry);
    }

    fn put(self: *Cache, path: []const u8, run_id: u64, generation: u64, value: Value, byte_cost: usize) !Handle {
        return try self.putWithBlock(path, run_id, generation, value, byte_cost, 0, 0, false);
    }

    fn putWithBlock(self: *Cache, path: []const u8, run_id: u64, generation: u64, value: Value, byte_cost: usize, block_offset: u64, block_len: u32, force_transient: bool) !Handle {
        const kind = std.meta.activeTag(value);
        const key = makeKey(path, run_id, generation, kind, block_offset, block_len);
        const owned_path = try self.allocator.dupe(u8, path);
        errdefer self.allocator.free(owned_path);

        const entry = try self.allocator.create(Entry);
        errdefer self.allocator.destroy(entry);
        entry.* = .{
            .kind = kind,
            .run_id = run_id,
            .generation = generation,
            .path = owned_path,
            .block_offset = block_offset,
            .block_len = block_len,
            .value = value,
            .byte_cost = byte_cost,
            .ref_count = 1,
            .last_access = 0,
        };

        // Retention is optional. Reserve its aggregate budget before making
        // the entry visible; when the budget cannot be reclaimed, ownership
        // stays in a transient handle so the read still succeeds.
        const retention_admitted = !force_transient and byte_cost <= self.effectiveMaxBytes() and self.admitResourceGrowth(byte_cost);
        var reservation_active = retention_admitted;
        errdefer if (reservation_active) self.releaseResourceBytes(byte_cost);

        const shard = self.shardForKey(key);
        const locked = lockAtomic(&shard.mutex);
        errdefer if (locked) shard.mutex.unlock();
        if (self.findEntryLocked(shard, key)) |existing| {
            existing.ref_count += 1;
            existing.last_access = self.nextAccess();
            self.touchEntryLocked(shard, existing);
            if (reservation_active) {
                self.releaseResourceBytes(byte_cost);
                reservation_active = false;
            }
            entry.deinit(self.allocator);
            self.allocator.destroy(entry);
            if (locked) shard.mutex.unlock();
            return .{
                .cache = self,
                .allocator = self.allocator,
                .entry = existing,
                .kind = kind,
            };
        }

        if (!retention_admitted) {
            entry.ref_count = 0;
            entry.transient_ref_count.store(1, .release);
            self.bumpTransientServe(kind);
            if (force_transient) self.bumpPolicyBypass(kind);
            if (locked) shard.mutex.unlock();
            return .{
                .cache = null,
                .allocator = self.allocator,
                .entry = entry,
                .kind = kind,
            };
        }

        entry.last_access = self.nextAccess();
        const gop = try shard.entries.getOrPutContextAdapted(self.allocator, key, KeyContext{}, KeyContext{});
        if (gop.found_existing) {
            const existing = gop.value_ptr.*;
            existing.ref_count += 1;
            existing.last_access = self.nextAccess();
            self.touchEntryLocked(shard, existing);
            self.releaseResourceBytes(byte_cost);
            reservation_active = false;
            entry.deinit(self.allocator);
            self.allocator.destroy(entry);
            if (locked) shard.mutex.unlock();
            return .{
                .cache = self,
                .allocator = self.allocator,
                .entry = existing,
                .kind = kind,
            };
        }
        gop.key_ptr.* = entry.key();
        gop.value_ptr.* = entry;
        self.linkEntryLocked(shard, entry);
        const used_bytes = self.used_bytes.fetchAdd(byte_cost, .monotonic) + byte_cost;
        updateAtomicMax(&self.peak_used_bytes, used_bytes);
        const kind_index = @backingInt(kind);
        const kind_bytes = self.kind_bytes[kind_index].fetchAdd(byte_cost, .monotonic) + byte_cost;
        updateAtomicMax(&self.kind_peak_bytes[kind_index], kind_bytes);
        if (isDataBlockKind(kind)) {
            const data_block_bytes = self.data_block_used_bytes.fetchAdd(byte_cost, .monotonic) + byte_cost;
            updateAtomicMax(&self.data_block_peak_used_bytes, data_block_bytes);
        }
        _ = self.entry_count.fetchAdd(1, .monotonic);
        self.bumpInsert(kind);
        reservation_active = false;
        if (locked) shard.mutex.unlock();
        self.refreshResourcePressure();
        self.evictToBudget();
        return .{
            .cache = self,
            .allocator = self.allocator,
            .entry = entry,
            .kind = kind,
        };
    }

    fn release(self: *Cache, entry: *Entry) void {
        const shard = self.shardForKey(entry.key());
        const locked = lockAtomic(&shard.mutex);

        std.debug.assert(entry.ref_count > 0);
        entry.ref_count -= 1;
        entry.last_access = self.nextAccess();
        self.touchEntryLocked(shard, entry);

        if (entry.ref_count == 0 and entry.invalidated) {
            self.removeEntryLocked(shard, entry);
            if (locked) shard.mutex.unlock();
            return;
        }
        if (locked) shard.mutex.unlock();
        self.evictToBudget();
    }

    fn findEntryLocked(self: *const Cache, shard: *const Shard, key: Key) ?*Entry {
        _ = self;
        const entry = shard.entries.getAdapted(key, KeyContext{}) orelse return null;
        if (entry.invalidated) return null;
        return entry;
    }

    fn shardFor(self: *Cache, path: []const u8, run_id: u64, generation: u64, kind: Kind) *Shard {
        return self.shardForKey(makeKey(path, run_id, generation, kind, 0, 0));
    }

    fn shardForBlock(self: *Cache, path: []const u8, run_id: u64, generation: u64, kind: Kind, block_offset: u64, block_len: u32) *Shard {
        return self.shardForKey(makeKey(path, run_id, generation, kind, block_offset, block_len));
    }

    fn shardForKey(self: *Cache, key: Key) *Shard {
        const hash = hashKey(key.path, key.run_id, key.generation, key.kind, key.block_offset, key.block_len);
        return &self.shards[@intCast(hash % self.shards.len)];
    }

    fn evictToBudget(self: *Cache) void {
        const locked = lockAtomic(&self.evict_mutex);
        defer if (locked) self.evict_mutex.unlock();

        while (self.currentBytes() > self.effectiveMaxBytes() and self.evictOne()) {}
    }

    fn effectiveMaxBytes(self: *Cache) usize {
        const pressure_target = self.pressure_target_bytes.load(.monotonic);
        if (pressure_target == 0) return self.max_bytes;
        return @min(self.max_bytes, pressure_target);
    }

    fn evictOne(self: *Cache) bool {
        const start = self.evict_cursor.fetchAdd(1, .monotonic);
        for (0..priority_count) |priority| {
            for (0..self.shards.len) |offset| {
                const shard = &self.shards[(start + offset) % self.shards.len];
                const locked = lockAtomic(&shard.mutex);

                var current = shard.lru_heads[priority];
                while (current) |entry| {
                    if (entry.ref_count == 0) {
                        self.removeEntryLocked(shard, entry);
                        if (locked) shard.mutex.unlock();
                        return true;
                    }
                    current = entry.lru_next;
                }
                if (locked) shard.mutex.unlock();
            }
        }
        return false;
    }

    fn removeEntryLocked(self: *Cache, shard: *Shard, entry: *Entry) void {
        _ = shard.entries.fetchRemoveAdapted(entry.key(), KeyContext{}) orelse unreachable;
        std.debug.assert(entry.ref_count == 0);
        self.unlinkEntryLocked(shard, entry);
        _ = self.used_bytes.fetchSub(entry.byte_cost, .monotonic);
        _ = self.kind_bytes[@backingInt(entry.kind)].fetchSub(entry.byte_cost, .monotonic);
        if (isDataBlockKind(entry.kind)) {
            _ = self.data_block_used_bytes.fetchSub(entry.byte_cost, .monotonic);
        }
        _ = self.entry_count.fetchSub(1, .monotonic);
        self.bumpEviction(entry.kind);
        const byte_cost = entry.byte_cost;
        entry.deinit(self.allocator);
        self.allocator.destroy(entry);
        self.releaseResourceBytes(byte_cost);
    }

    fn isDataBlockKind(kind: Kind) bool {
        return kind == .run_table_block or kind == .run_table_physical_block;
    }

    fn updateAtomicMax(counter: *std.atomic.Value(usize), value: usize) void {
        var observed = counter.load(.monotonic);
        while (observed < value) {
            if (counter.cmpxchgWeak(observed, value, .monotonic, .monotonic)) |actual| {
                observed = actual;
                continue;
            }
            return;
        }
    }

    fn nextAccess(self: *Cache) u64 {
        return self.access_clock.fetchAdd(1, .monotonic) + 1;
    }

    fn admitResourceGrowth(self: *Cache, bytes: usize) bool {
        if (bytes == 0 or self.resource_manager == null) return true;
        while (true) {
            const locked = lockAtomic(&self.resource_accounting_mutex);
            const manager = self.resource_manager orelse {
                if (locked) self.resource_accounting_mutex.unlock();
                return true;
            };
            const next = std.math.add(u64, self.resource_accounted_bytes, @as(u64, @intCast(bytes))) catch {
                if (locked) self.resource_accounting_mutex.unlock();
                return false;
            };
            manager.adjustUsage(.lsm_block_table_cache, &self.resource_accounted_bytes, next) catch {
                if (locked) self.resource_accounting_mutex.unlock();
                // ResourceManager deliberately does not call the requester's
                // own reclaimer. Shed an unpinned LSM entry here, then retry
                // once the exact released bytes have been published.
                if (!self.evictOne()) return false;
                continue;
            };
            if (locked) self.resource_accounting_mutex.unlock();
            return true;
        }
    }

    fn releaseResourceBytes(self: *Cache, bytes: usize) void {
        if (bytes == 0) return;
        const locked = lockAtomic(&self.resource_accounting_mutex);
        defer if (locked) self.resource_accounting_mutex.unlock();
        const manager = self.resource_manager orelse return;
        const amount: u64 = @intCast(bytes);
        if (amount > self.resource_accounted_bytes) {
            manager.recordAccountingError();
            return;
        }
        manager.adjustUsage(
            .lsm_block_table_cache,
            &self.resource_accounted_bytes,
            self.resource_accounted_bytes - amount,
        ) catch return;
        self.refreshPressureTarget(manager);
    }

    fn refreshResourcePressure(self: *Cache) void {
        const manager = self.resource_manager orelse return;
        const locked = lockAtomic(&self.resource_accounting_mutex);
        defer if (locked) self.resource_accounting_mutex.unlock();
        self.refreshPressureTarget(manager);
    }

    fn releaseResourceUsage(self: *Cache) void {
        const manager = self.resource_manager orelse return;
        const locked = lockAtomic(&self.resource_accounting_mutex);
        defer if (locked) self.resource_accounting_mutex.unlock();
        manager.observeUsage(.lsm_block_table_cache, &self.resource_accounted_bytes, 0);
        self.resource_manager = null;
        self.pressure_target_bytes.store(0, .monotonic);
    }

    fn refreshPressureTarget(self: *Cache, manager: *resource_manager_mod.ResourceManager) void {
        const stats = manager.sliceStats(.lsm_block_table_cache);
        const action = switch (stats.pressure) {
            .normal => {
                self.pressure_target_bytes.store(0, .monotonic);
                return;
            },
            .soft => stats.soft_action,
            .hard => stats.hard_action,
        };
        if (action != .shrink_cache) {
            self.pressure_target_bytes.store(0, .monotonic);
            return;
        }
        const target = if (stats.soft_limit_bytes > 0) stats.soft_limit_bytes else stats.hard_limit_bytes;
        self.pressure_target_bytes.store(clampU64ToUsize(target), .monotonic);
    }

    fn linkEntryLocked(self: *Cache, shard: *Shard, entry: *Entry) void {
        _ = self;
        const priority = evictionPriority(entry.kind);
        entry.lru_prev = shard.lru_tails[priority];
        entry.lru_next = null;
        if (entry.lru_prev) |prev| {
            prev.lru_next = entry;
        } else {
            shard.lru_heads[priority] = entry;
        }
        shard.lru_tails[priority] = entry;
    }

    fn unlinkEntryLocked(self: *Cache, shard: *Shard, entry: *Entry) void {
        _ = self;
        const priority = evictionPriority(entry.kind);
        if (entry.lru_prev) |prev| {
            prev.lru_next = entry.lru_next;
        } else {
            shard.lru_heads[priority] = entry.lru_next;
        }
        if (entry.lru_next) |next| {
            next.lru_prev = entry.lru_prev;
        } else {
            shard.lru_tails[priority] = entry.lru_prev;
        }
        entry.lru_prev = null;
        entry.lru_next = null;
    }

    fn touchEntryLocked(self: *Cache, shard: *Shard, entry: *Entry) void {
        const priority = evictionPriority(entry.kind);
        if (shard.lru_tails[priority] == entry) return;
        self.unlinkEntryLocked(shard, entry);
        self.linkEntryLocked(shard, entry);
    }

    fn bumpHit(self: *Cache, kind: Kind) void {
        _ = self.stats.byKind(kind).hits.fetchAdd(1, .monotonic);
    }

    fn bumpMiss(self: *Cache, kind: Kind) void {
        _ = self.stats.byKind(kind).misses.fetchAdd(1, .monotonic);
    }

    fn bumpInsert(self: *Cache, kind: Kind) void {
        _ = self.stats.byKind(kind).inserts.fetchAdd(1, .monotonic);
    }

    fn bumpTransientServe(self: *Cache, kind: Kind) void {
        _ = self.stats.byKind(kind).transient_serves.fetchAdd(1, .monotonic);
    }

    fn bumpPolicyBypass(self: *Cache, kind: Kind) void {
        _ = self.stats.byKind(kind).policy_bypasses.fetchAdd(1, .monotonic);
    }

    fn bumpEviction(self: *Cache, kind: Kind) void {
        _ = self.stats.byKind(kind).evictions.fetchAdd(1, .monotonic);
    }

    fn bumpInvalidation(self: *Cache, kind: Kind) void {
        _ = self.stats.byKind(kind).invalidations.fetchAdd(1, .monotonic);
    }

    fn bumpWait(self: *Cache, kind: Kind) void {
        _ = self.stats.byKind(kind).waits.fetchAdd(1, .monotonic);
    }
};

const supports_waitable_pending = builtin.os.tag != .freestanding and
    builtin.os.tag != .wasi and
    !builtin.single_threaded and
    builtin.link_libc and
    @hasDecl(std.c, "pthread_cond_wait");

const PendingSync = if (supports_waitable_pending)
    struct {
        mutex: std.c.pthread_mutex_t = std.c.PTHREAD_MUTEX_INITIALIZER,
        cond: std.c.pthread_cond_t = std.c.PTHREAD_COND_INITIALIZER,

        fn lock(self: *@This()) void {
            if (std.c.pthread_mutex_lock(&self.mutex) != .SUCCESS) unreachable;
        }

        fn unlock(self: *@This()) void {
            if (std.c.pthread_mutex_unlock(&self.mutex) != .SUCCESS) unreachable;
        }

        fn wait(self: *@This()) void {
            if (std.c.pthread_cond_wait(&self.cond, &self.mutex) != .SUCCESS) unreachable;
        }

        fn broadcast(self: *@This()) void {
            if (std.c.pthread_cond_broadcast(&self.cond) != .SUCCESS) unreachable;
        }
    }
else
    struct {
        mutex: std.atomic.Mutex = .unlocked,

        fn lock(self: *@This()) void {
            _ = lockAtomic(&self.mutex);
        }

        fn unlock(self: *@This()) void {
            self.mutex.unlock();
        }

        fn wait(self: *@This()) void {
            self.unlock();
            if (!builtin.single_threaded) sleepNs(50_000);
            self.lock();
        }

        fn broadcast(_: *@This()) void {}
    };

pub const Handle = struct {
    cache: ?*Cache,
    allocator: Allocator,
    entry: *Cache.Entry,
    kind: Kind,

    pub fn retain(self: *const Handle) Handle {
        if (self.cache) |cache| {
            cache.retainEntry(self.entry);
        } else {
            _ = self.entry.transient_ref_count.fetchAdd(1, .acq_rel);
        }
        return .{
            .cache = self.cache,
            .allocator = self.allocator,
            .entry = self.entry,
            .kind = self.kind,
        };
    }

    pub fn release(self: *Handle) void {
        if (self.cache) |cache| {
            cache.release(self.entry);
        } else if (self.entry.transient_ref_count.fetchSub(1, .acq_rel) == 1) {
            self.entry.deinit(self.allocator);
            self.allocator.destroy(self.entry);
        }
        self.* = undefined;
    }

    pub fn isRetained(self: *const Handle) bool {
        return self.cache != null;
    }

    pub fn runState(self: *const Handle) *const State {
        std.debug.assert(self.kind == .run_state);
        return &self.entry.value.run_state;
    }

    pub fn runTableRaw(self: *const Handle) []u8 {
        std.debug.assert(self.kind == .run_table_raw);
        return self.entry.value.run_table_raw;
    }

    pub fn runTableIndex(self: *const Handle) *const TableIndex {
        std.debug.assert(self.kind == .run_table_index);
        return &self.entry.value.run_table_index;
    }

    pub fn runTableBlock(self: *const Handle) []const u8 {
        std.debug.assert(self.kind == .run_table_block);
        return self.entry.value.run_table_block;
    }

    pub fn runTablePhysicalBlock(self: *const Handle) []const u8 {
        std.debug.assert(self.kind == .run_table_physical_block);
        return self.entry.value.run_table_physical_block;
    }
};

pub fn estimateStateCost(path: []const u8, state: *const State) usize {
    var total = path.len + @sizeOf(State);
    total += state.entries.items.len * @sizeOf(state_mod.OwnedEntry);
    for (state.entries.items) |entry| {
        if (entry.namespace_name) |name| total += name.len;
        total += entry.key.len;
        total += entry.value.len;
    }
    return total;
}

pub fn estimateRawTableCost(path: []const u8, raw: []const u8) usize {
    return path.len + raw.len;
}

pub fn estimateTableIndexCost(path: []const u8, index: *const TableIndex) usize {
    var total = path.len +
        @sizeOf(TableIndex) +
        index.entry_offsets.len * @sizeOf(u32) +
        index.block_entry_offsets.len * @sizeOf(u16) +
        index.filter.bytes.len;
    if (index.prefix_filter) |filter| total += filter.bytes.len;
    total += index.blocks.len * @sizeOf(TableIndex.BlockMeta);
    for (index.blocks) |block| {
        if (block.largest_namespace_name) |name| total += name.len;
        total += block.largest_key.len;
        if (block.filter) |filter| total += filter.bytes.len;
        if (block.prefix_filter) |filter| total += filter.bytes.len;
        total += block.hash_slots.len * @sizeOf(u32);
    }
    return total;
}

pub fn estimateTableBlockCost(path: []const u8, block: []const u8) usize {
    return path.len + block.len;
}

fn hashKey(path: []const u8, run_id: u64, generation: u64, kind: Kind, block_offset: u64, block_len: u32) u64 {
    var hasher = std.hash.Wyhash.init(0x15410f4dbdb67d1d);
    hasher.update(path);
    hasher.update(std.mem.asBytes(&run_id));
    hasher.update(std.mem.asBytes(&generation));
    const tag: u8 = @backingInt(kind);
    hasher.update(&.{tag});
    hasher.update(std.mem.asBytes(&block_offset));
    hasher.update(std.mem.asBytes(&block_len));
    return hasher.final();
}

fn evictionPriority(kind: Kind) u8 {
    return switch (kind) {
        .run_table_raw => 0,
        .run_table_block => 0,
        .run_table_physical_block => 1,
        .run_state => 2,
        .run_table_index => 3,
    };
}

fn makeKey(path: []const u8, run_id: u64, generation: u64, kind: Kind, block_offset: u64, block_len: u32) Cache.Key {
    return .{
        .kind = kind,
        .run_id = run_id,
        .generation = generation,
        .path = path,
        .block_offset = block_offset,
        .block_len = block_len,
    };
}

fn copyKey(allocator: Allocator, key: Cache.Key) !Cache.Key {
    return .{
        .kind = key.kind,
        .run_id = key.run_id,
        .generation = key.generation,
        .path = try allocator.dupe(u8, key.path),
        .block_offset = key.block_offset,
        .block_len = key.block_len,
    };
}

fn lockAtomic(mutex: *std.atomic.Mutex) bool {
    if (builtin.os.tag == .freestanding) return false;
    platform_sync.lockYielding(mutex);
    return true;
}

fn clampU64ToUsize(value: u64) usize {
    return if (value > std.math.maxInt(usize)) std.math.maxInt(usize) else @intCast(value);
}

fn sleepNs(ns: u64) void {
    if (ns == 0) return;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Clock.Duration.sleep(.{
        .clock = .awake,
        .raw = .fromNanoseconds(@intCast(ns)),
    }, io_impl.io()) catch {};
}

test "cache retains and releases run state" {
    const allocator = std.testing.allocator;
    var cache = Cache.init(allocator, 1024 * 1024);
    defer cache.deinit();

    var state: State = .{};
    try state.upsert(allocator, .{ .name = "ns" }, "key", "value", false);

    var inserted = try cache.putRunState("run-1", 1, 1, state);
    defer inserted.release();

    try std.testing.expectEqual(@as(usize, 1), cache.entryCount());
    try std.testing.expect(cache.currentBytes() > 0);
    try std.testing.expectEqualStrings("value", try inserted.runState().get(.{ .name = "ns" }, "key"));

    var retained = cache.retainRunState("run-1", 1, 1) orelse return error.ExpectedCacheHit;
    defer retained.release();
    try std.testing.expectEqualStrings("value", try retained.runState().get(.{ .name = "ns" }, "key"));

    const stats = cache.snapshotStats();
    try std.testing.expectEqual(@as(u64, 1), stats.run_state.hits);
}

test "cache serves oversized entries transiently" {
    const allocator = std.testing.allocator;
    var cache = Cache.init(allocator, 1);
    defer cache.deinit();

    var first_state: State = .{};
    try first_state.upsert(allocator, .{ .name = "ns" }, "a", "value-a", false);
    var first = try cache.putRunState("run-1", 1, 1, first_state);
    try std.testing.expect(!first.isRetained());
    try std.testing.expectEqualStrings("value-a", try first.runState().get(.{ .name = "ns" }, "a"));
    first.release();

    try std.testing.expectEqual(@as(usize, 0), cache.currentBytes());
    try std.testing.expect(cache.retainRunState("run-1", 1, 1) == null);
}

test "cache policy serves table blocks transiently without consuming capacity" {
    const allocator = std.testing.allocator;
    var cache = Cache.init(allocator, 1024 * 1024);
    defer cache.deinit();

    const block = try allocator.dupe(u8, "decoded vector block");
    var handle = try cache.putTransientRunTableBlock("run-vector", 7, 3, 128, @intCast(block.len), block);
    defer handle.release();

    try std.testing.expectEqualStrings("decoded vector block", handle.runTableBlock());
    try std.testing.expectEqual(@as(usize, 0), cache.entryCount());
    try std.testing.expectEqual(@as(usize, 0), cache.currentBytes());
    try std.testing.expect(cache.retainRunTableBlock("run-vector", 7, 3, 128, @intCast(block.len)) == null);
    const stats = cache.snapshotStats().run_table_block;
    try std.testing.expectEqual(@as(u64, 1), stats.transient_serves);
    try std.testing.expectEqual(@as(u64, 1), stats.policy_bypasses);
    try std.testing.expectEqual(@as(u64, 0), stats.inserts);
}

test "cache preserves simultaneous data block residency high water after eviction" {
    const allocator = std.testing.allocator;
    var cache = Cache.init(allocator, 1024 * 1024);
    defer cache.deinit();

    const decoded = try allocator.dupe(u8, "decoded block");
    var decoded_handle = try cache.putRunTableBlock("run-peak", 1, 1, 0, @intCast(decoded.len), decoded);
    const after_decoded = cache.snapshotStats();
    try std.testing.expectEqual(after_decoded.data_block_used_bytes, after_decoded.data_block_peak_used_bytes);

    const physical = try allocator.dupe(u8, "physical block");
    var physical_handle = try cache.putRunTablePhysicalBlock("run-peak", 1, 1, 64, @intCast(physical.len), physical);
    const simultaneous = cache.snapshotStats();
    try std.testing.expectEqual(simultaneous.data_block_used_bytes, simultaneous.data_block_peak_used_bytes);
    try std.testing.expect(simultaneous.data_block_peak_used_bytes > after_decoded.data_block_peak_used_bytes);

    decoded_handle.release();
    physical_handle.release();
    cache.invalidatePath("run-peak");
    const after_eviction = cache.snapshotStats();
    try std.testing.expectEqual(@as(usize, 0), after_eviction.data_block_used_bytes);
    try std.testing.expectEqual(simultaneous.data_block_peak_used_bytes, after_eviction.data_block_peak_used_bytes);
}

test "cache snapshot clamps concurrently sampled peaks to current residency" {
    var cache = Cache.init(std.testing.allocator, 1024 * 1024);
    defer cache.deinit();

    // Model the small publication window between the current-byte increment
    // and the corresponding atomic-max update.
    cache.used_bytes.store(1024, .monotonic);
    cache.peak_used_bytes.store(512, .monotonic);
    cache.data_block_used_bytes.store(768, .monotonic);
    cache.data_block_peak_used_bytes.store(256, .monotonic);
    cache.kind_bytes[@backingInt(Kind.run_table_block)].store(768, .monotonic);
    cache.kind_peak_bytes[@backingInt(Kind.run_table_block)].store(256, .monotonic);
    defer {
        cache.used_bytes.store(0, .monotonic);
        cache.peak_used_bytes.store(0, .monotonic);
        cache.data_block_used_bytes.store(0, .monotonic);
        cache.data_block_peak_used_bytes.store(0, .monotonic);
        cache.kind_bytes[@backingInt(Kind.run_table_block)].store(0, .monotonic);
        cache.kind_peak_bytes[@backingInt(Kind.run_table_block)].store(0, .monotonic);
    }

    const stats = cache.snapshotStats();
    try std.testing.expectEqual(stats.used_bytes, stats.peak_used_bytes);
    try std.testing.expectEqual(stats.data_block_used_bytes, stats.data_block_peak_used_bytes);
    try std.testing.expectEqual(stats.run_table_block.used_bytes, stats.run_table_block.peak_used_bytes);
}

test "cache pending load waiter survives finish removal" {
    const allocator = std.testing.allocator;
    var cache = Cache.init(allocator, 1024 * 1024);
    defer cache.deinit();

    try cache.beginLoad("run-1", 1, 1, .run_table_index);

    const Waiter = struct {
        err: ?anyerror = null,

        fn run(self: *@This(), cache_ptr: *Cache) void {
            cache_ptr.beginLoad("run-1", 1, 1, .run_table_index) catch |err| {
                self.err = err;
                return;
            };
            cache_ptr.finishLoad("run-1", 1, 1, .run_table_index);
        }
    };

    var waiter = Waiter{};
    const thread = try std.Thread.spawn(.{}, Waiter.run, .{ &waiter, &cache });
    sleepNs(10 * std.time.ns_per_ms);
    cache.finishLoad("run-1", 1, 1, .run_table_index);
    thread.join();

    if (waiter.err) |err| return err;
    try std.testing.expectEqual(@as(usize, 0), cache.pendingLoadCountForTests());
    try std.testing.expect(cache.snapshotStats().run_table_index.waits > 0);
}

test "cache accounts table index prefix bloom filters" {
    const allocator = std.testing.allocator;
    const path = "run-prefix-index";
    const entries = [_]lsm_table_file.Entry{
        .{ .namespace_name = "docs", .key = "tenant-a:001", .value = "a" },
        .{ .namespace_name = "docs", .key = "tenant-a:002", .value = "b" },
        .{ .namespace_name = "docs", .key = "tenant-c:001", .value = "c" },
    };

    const encoded = try lsm_table_file.encodeAlloc(allocator, &entries);
    defer allocator.free(encoded);

    var index = try lsm_table_file.decodeIndexAlloc(allocator, encoded);
    var index_owned = true;
    errdefer if (index_owned) index.deinit(allocator);
    const table_prefix_filter = index.prefix_filter orelse return error.ExpectedPrefixFilter;
    try std.testing.expect(table_prefix_filter.bytes.len > 0);
    var block_prefix_filter_bytes: usize = 0;
    for (index.blocks) |block| {
        if (block.prefix_filter) |filter| block_prefix_filter_bytes += filter.bytes.len;
    }
    try std.testing.expect(block_prefix_filter_bytes > 0);

    const expected_cost = estimateTableIndexCost(path, &index);
    var cache = Cache.init(allocator, 1024 * 1024);
    defer cache.deinit();

    var handle = try cache.putRunTableIndex(path, 1, 1, index);
    index_owned = false;
    defer handle.release();

    const stats = cache.snapshotStats();
    try std.testing.expectEqual(expected_cost, cache.currentBytes());
    try std.testing.expectEqual(expected_cost, stats.run_table_index.used_bytes);
    try std.testing.expect(expected_cost >= path.len + table_prefix_filter.bytes.len + block_prefix_filter_bytes);
}

test "cache reports shared byte usage to resource manager" {
    const allocator = std.testing.allocator;
    var budgets = resource_manager_mod.Options.defaultBudgets();
    budgets[@backingInt(resource_manager_mod.Slice.lsm_block_table_cache)] = .{
        .soft_limit_bytes = 1,
        .hard_limit_bytes = 1024 * 1024,
    };
    var resource_manager = resource_manager_mod.ResourceManager.init(.{ .budgets = budgets });

    var cache = Cache.init(allocator, 1024 * 1024);
    cache.attachResourceManager(&resource_manager);
    defer cache.deinit();

    var state: State = .{};
    try state.upsert(allocator, .{ .name = "ns" }, "key", "value", false);
    var inserted = try cache.putRunState("run-1", 1, 1, state);
    try std.testing.expect(inserted.isRetained());

    var stats = resource_manager.snapshot();
    try std.testing.expect(stats.slices[@backingInt(resource_manager_mod.Slice.lsm_block_table_cache)].used_bytes > 0);
    try std.testing.expect(stats.slices[@backingInt(resource_manager_mod.Slice.lsm_block_table_cache)].soft_limit_events > 0);

    inserted.release();
    cache.invalidatePath("run-1");
    stats = resource_manager.snapshot();
    try std.testing.expectEqual(@as(u64, 0), stats.slices[@backingInt(resource_manager_mod.Slice.lsm_block_table_cache)].used_bytes);
}

test "cache falls back to a transient handle when retention exceeds the resource envelope" {
    const allocator = std.testing.allocator;
    var budgets = resource_manager_mod.Options.defaultBudgets();
    budgets[@backingInt(resource_manager_mod.Slice.lsm_block_table_cache)] = .{
        .soft_limit_bytes = 1,
        .hard_limit_bytes = 2,
    };
    var resource_manager = resource_manager_mod.ResourceManager.init(.{ .budgets = budgets });

    var cache = Cache.init(allocator, 1024 * 1024);
    cache.attachResourceManager(&resource_manager);
    defer cache.deinit();

    var state: State = .{};
    try state.upsert(allocator, .{ .name = "ns" }, "key", "value", false);
    var handle = try cache.putRunState("run-transient", 1, 1, state);
    try std.testing.expect(!handle.isRetained());
    try std.testing.expectEqualStrings("value", try handle.runState().get(.{ .name = "ns" }, "key"));
    try std.testing.expectEqual(@as(usize, 0), cache.currentBytes());
    try std.testing.expectEqual(@as(u64, 0), resource_manager.sliceStats(.lsm_block_table_cache).used_bytes);
    try std.testing.expect(resource_manager.sliceStats(.lsm_block_table_cache).hard_limit_rejections > 0);
    try std.testing.expectEqual(@as(u64, 1), cache.snapshotStats().run_state.transient_serves);
    handle.release();
}

test "cache transfers existing usage when resource manager changes" {
    const allocator = std.testing.allocator;
    var first_manager = resource_manager_mod.ResourceManager.init(.{});
    defer first_manager.deinit(std.testing.allocator);
    var second_manager = resource_manager_mod.ResourceManager.init(.{});
    defer second_manager.deinit(std.testing.allocator);

    var cache = Cache.init(allocator, 1024 * 1024);
    cache.attachResourceManager(&first_manager);
    defer cache.deinit();

    var state: State = .{};
    try state.upsert(allocator, .{ .name = "ns" }, "key", "value", false);
    var inserted = try cache.putRunState("run-transfer", 1, 1, state);
    inserted.release();
    const bytes: u64 = @intCast(cache.currentBytes());
    try std.testing.expect(bytes > 0);

    cache.attachResourceManager(&second_manager);
    try std.testing.expectEqual(
        @as(u64, 0),
        first_manager.sliceStats(.lsm_block_table_cache).used_bytes,
    );
    try std.testing.expectEqual(
        bytes,
        second_manager.sliceStats(.lsm_block_table_cache).used_bytes,
    );
}

test "shared LSM cache yields to foreground aggregate admission" {
    const allocator = std.testing.allocator;
    var resource_manager = resource_manager_mod.ResourceManager.init(.{
        .memory_budget = .{ .soft_limit_bytes = 900, .hard_limit_bytes = 1024 },
    });
    defer resource_manager.deinit(std.testing.allocator);
    var cache = Cache.init(allocator, 1024 * 1024);
    cache.attachResourceManager(&resource_manager);
    defer cache.deinit();

    var state: State = .{};
    try state.upsert(allocator, .{ .name = "ns" }, "key", "value", false);
    var inserted = try cache.putRunState("run-reclaim", 1, 1, state);
    inserted.release();
    try std.testing.expect(cache.currentBytes() > 24);

    var foreground = try resource_manager.reserve(.dense_apply_working_set, 1000);
    defer foreground.release();
    try std.testing.expectEqual(@as(usize, 0), cache.currentBytes());
    try std.testing.expectEqual(@as(u64, 1000), resource_manager.snapshot().memory.used_bytes);
}

test "cache shrinks against resource manager pressure target" {
    const allocator = std.testing.allocator;
    var budgets = resource_manager_mod.Options.defaultBudgets();
    budgets[@backingInt(resource_manager_mod.Slice.lsm_block_table_cache)] = .{
        .soft_limit_bytes = 1,
        .hard_limit_bytes = 1024 * 1024,
    };
    var resource_manager = resource_manager_mod.ResourceManager.init(.{ .budgets = budgets });

    var cache = Cache.init(allocator, 1024 * 1024);
    cache.attachResourceManager(&resource_manager);
    defer cache.deinit();

    var state: State = .{};
    try state.upsert(allocator, .{ .name = "ns" }, "key", "value", false);
    var inserted = try cache.putRunState("run-1", 1, 1, state);

    try std.testing.expect(cache.currentBytes() > 1);
    try std.testing.expectEqual(@as(usize, 1), cache.entryCount());

    inserted.release();
    try std.testing.expectEqual(@as(usize, 0), cache.entryCount());
    try std.testing.expectEqual(@as(usize, 0), cache.currentBytes());
}

test "cache invalidates path while pinned" {
    const allocator = std.testing.allocator;
    var cache = Cache.init(allocator, 1024 * 1024);
    defer cache.deinit();

    var state: State = .{};
    try state.upsert(allocator, .{ .name = "ns" }, "key", "value", false);
    var handle = try cache.putRunState("run-1", 1, 1, state);

    cache.invalidatePath("run-1");
    try std.testing.expect(cache.retainRunState("run-1", 1, 1) == null);
    try std.testing.expectEqual(@as(usize, 1), cache.entryCount());

    handle.release();
    try std.testing.expectEqual(@as(usize, 0), cache.entryCount());
}

test "cache invalidates path prefix while pinned" {
    const allocator = std.testing.allocator;
    var cache = Cache.init(allocator, 1024 * 1024);
    defer cache.deinit();

    var first_state: State = .{};
    try first_state.upsert(allocator, .{ .name = "ns" }, "key-a", "value-a", false);
    var first = try cache.putRunState("/tmp/group-1/table-db/runs/1.tbl", 1, 1, first_state);

    var second_state: State = .{};
    try second_state.upsert(allocator, .{ .name = "ns" }, "key-b", "value-b", false);
    var second = try cache.putRunState("/tmp/group-1/table-db/runs/2.tbl", 2, 1, second_state);

    cache.invalidatePrefix("/tmp/group-1/table-db");
    try std.testing.expect(cache.retainRunState("/tmp/group-1/table-db/runs/1.tbl", 1, 1) == null);
    try std.testing.expect(cache.retainRunState("/tmp/group-1/table-db/runs/2.tbl", 2, 1) == null);
    try std.testing.expectEqual(@as(usize, 2), cache.entryCount());

    first.release();
    second.release();
    try std.testing.expectEqual(@as(usize, 0), cache.entryCount());
}

test "cache distinguishes reused paths by run id" {
    const allocator = std.testing.allocator;
    var cache = Cache.init(allocator, 1024 * 1024);
    defer cache.deinit();

    var first_state: State = .{};
    try first_state.upsert(allocator, .{ .name = "ns" }, "key", "first", false);
    var first = try cache.putRunState("same-path", 1, 1, first_state);
    defer first.release();

    var second_state: State = .{};
    try second_state.upsert(allocator, .{ .name = "ns" }, "key", "second", false);
    var second = try cache.putRunState("same-path", 2, 2, second_state);
    defer second.release();

    var retained_first = cache.retainRunState("same-path", 1, 1) orelse return error.ExpectedCacheHit;
    defer retained_first.release();
    try std.testing.expectEqualStrings("first", try retained_first.runState().get(.{ .name = "ns" }, "key"));

    var retained_second = cache.retainRunState("same-path", 2, 2) orelse return error.ExpectedCacheHit;
    defer retained_second.release();
    try std.testing.expectEqualStrings("second", try retained_second.runState().get(.{ .name = "ns" }, "key"));
}

test "cache invalidates ownership move prefix without reviving pinned generations" {
    const allocator = std.testing.allocator;
    var cache = Cache.init(allocator, 1024 * 1024);
    defer cache.deinit();

    var old_state: State = .{};
    try old_state.upsert(allocator, .{ .name = "ns" }, "doc", "old-owner", false);
    var old_handle = try cache.putRunState("/tmp/group-9/table-1/range-91/run.tbl", 91, 7, old_state);

    var new_state: State = .{};
    try new_state.upsert(allocator, .{ .name = "ns" }, "doc", "new-owner", false);
    var new_handle = try cache.putRunState("/tmp/group-10/table-1/range-92/run.tbl", 92, 8, new_state);
    defer new_handle.release();

    cache.invalidatePrefix("/tmp/group-9/table-1");
    try std.testing.expect(cache.retainRunState("/tmp/group-9/table-1/range-91/run.tbl", 91, 7) == null);
    try std.testing.expect(cache.retainRunState("/tmp/group-9/table-1/range-91/run.tbl", 91, 8) == null);

    var retained_new = cache.retainRunState("/tmp/group-10/table-1/range-92/run.tbl", 92, 8) orelse return error.ExpectedCacheHit;
    defer retained_new.release();
    try std.testing.expectEqualStrings("new-owner", try retained_new.runState().get(.{ .name = "ns" }, "doc"));

    try std.testing.expectEqual(@as(usize, 2), cache.entryCount());
    old_handle.release();
    try std.testing.expectEqual(@as(usize, 1), cache.entryCount());
}
