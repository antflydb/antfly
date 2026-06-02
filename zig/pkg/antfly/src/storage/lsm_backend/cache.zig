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
    evictions: u64 = 0,
    invalidations: u64 = 0,
    waits: u64 = 0,
    used_bytes: usize = 0,
};

pub const Stats = struct {
    used_bytes: usize = 0,
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
        lru_heads: [priority_count]?*Entry = [_]?*Entry{null} ** priority_count,
        lru_tails: [priority_count]?*Entry = [_]?*Entry{null} ** priority_count,
        pending_sync: PendingSync = .{},
        pending_loads: PendingMap = .empty,
    };

    const PendingLoad = struct {};

    const AtomicKindStats = struct {
        hits: CounterU64 = .init(0),
        misses: CounterU64 = .init(0),
        inserts: CounterU64 = .init(0),
        evictions: CounterU64 = .init(0),
        invalidations: CounterU64 = .init(0),
        waits: CounterU64 = .init(0),

        fn snapshot(self: *const AtomicKindStats) KindStats {
            return .{
                .hits = self.hits.load(.monotonic),
                .misses = self.misses.load(.monotonic),
                .inserts = self.inserts.load(.monotonic),
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

        fn snapshot(self: *const AtomicStats, used_bytes: usize, entry_count: usize, kind_bytes: [@typeInfo(Kind).@"enum".fields.len]usize) Stats {
            var run_state = self.run_state.snapshot();
            var run_table_raw = self.run_table_raw.snapshot();
            var run_table_index = self.run_table_index.snapshot();
            var run_table_block = self.run_table_block.snapshot();
            var run_table_physical_block = self.run_table_physical_block.snapshot();
            run_state.used_bytes = kind_bytes[@intFromEnum(Kind.run_state)];
            run_table_raw.used_bytes = kind_bytes[@intFromEnum(Kind.run_table_raw)];
            run_table_index.used_bytes = kind_bytes[@intFromEnum(Kind.run_table_index)];
            run_table_block.used_bytes = kind_bytes[@intFromEnum(Kind.run_table_block)];
            run_table_physical_block.used_bytes = kind_bytes[@intFromEnum(Kind.run_table_physical_block)];
            return .{
                .used_bytes = used_bytes,
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
    entry_count: std.atomic.Value(usize) = .init(0),
    kind_bytes: [@typeInfo(Kind).@"enum".fields.len]std.atomic.Value(usize) = .{
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
        defer if (locked) self.resource_accounting_mutex.unlock();
        self.resource_manager = resource_manager;
        resource_manager.observeUsage(.lsm_block_table_cache, &self.resource_accounted_bytes, @intCast(self.currentBytes()));
    }

    pub fn snapshotStats(self: *const Cache) Stats {
        var by_kind: [@typeInfo(Kind).@"enum".fields.len]usize = undefined;
        inline for (0..by_kind.len) |i| by_kind[i] = self.kind_bytes[i].load(.monotonic);
        return self.stats.snapshot(self.currentBytes(), self.entryCount(), by_kind);
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
        return try self.putWithBlock(path, run_id, generation, value, byte_cost, 0, 0);
    }

    fn putWithBlock(self: *Cache, path: []const u8, run_id: u64, generation: u64, value: Value, byte_cost: usize, block_offset: u64, block_len: u32) !Handle {
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

        const shard = self.shardForKey(key);
        const locked = lockAtomic(&shard.mutex);
        errdefer if (locked) shard.mutex.unlock();
        if (self.findEntryLocked(shard, key)) |existing| {
            existing.ref_count += 1;
            existing.last_access = self.nextAccess();
            self.touchEntryLocked(shard, existing);
            entry.deinit(self.allocator);
            self.allocator.destroy(entry);
            if (locked) shard.mutex.unlock();
            return .{
                .cache = self,
                .entry = existing,
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
            entry.deinit(self.allocator);
            self.allocator.destroy(entry);
            if (locked) shard.mutex.unlock();
            return .{
                .cache = self,
                .entry = existing,
                .kind = kind,
            };
        }
        gop.key_ptr.* = entry.key();
        gop.value_ptr.* = entry;
        self.linkEntryLocked(shard, entry);
        _ = self.used_bytes.fetchAdd(byte_cost, .monotonic);
        _ = self.kind_bytes[@intFromEnum(kind)].fetchAdd(byte_cost, .monotonic);
        _ = self.entry_count.fetchAdd(1, .monotonic);
        self.bumpInsert(kind);
        if (locked) shard.mutex.unlock();
        self.refreshResourceUsage();
        self.evictToBudget();
        return .{
            .cache = self,
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
        _ = self.kind_bytes[@intFromEnum(entry.kind)].fetchSub(entry.byte_cost, .monotonic);
        _ = self.entry_count.fetchSub(1, .monotonic);
        self.bumpEviction(entry.kind);
        entry.deinit(self.allocator);
        self.allocator.destroy(entry);
        self.refreshResourceUsage();
    }

    fn nextAccess(self: *Cache) u64 {
        return self.access_clock.fetchAdd(1, .monotonic) + 1;
    }

    fn refreshResourceUsage(self: *Cache) void {
        const manager = self.resource_manager orelse return;
        const locked = lockAtomic(&self.resource_accounting_mutex);
        defer if (locked) self.resource_accounting_mutex.unlock();
        manager.observeUsage(.lsm_block_table_cache, &self.resource_accounted_bytes, @intCast(self.currentBytes()));
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

const supports_waitable_pending = builtin.os.tag != .freestanding and builtin.link_libc and @hasDecl(std.c, "pthread_cond_wait");

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
    cache: *Cache,
    entry: *Cache.Entry,
    kind: Kind,

    pub fn retain(self: *const Handle) Handle {
        self.cache.retainEntry(self.entry);
        return .{
            .cache = self.cache,
            .entry = self.entry,
            .kind = self.kind,
        };
    }

    pub fn release(self: *Handle) void {
        self.cache.release(self.entry);
        self.* = undefined;
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
        index.filter.bytes.len;
    total += index.blocks.len * @sizeOf(TableIndex.BlockMeta);
    for (index.blocks) |block| {
        if (block.largest_namespace_name) |name| total += name.len;
        total += block.largest_key.len;
        if (block.filter) |filter| total += filter.bytes.len;
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
    const tag: u8 = @intFromEnum(kind);
    hasher.update(&.{tag});
    hasher.update(std.mem.asBytes(&block_offset));
    hasher.update(std.mem.asBytes(&block_len));
    return hasher.final();
}

fn evictionPriority(kind: Kind) u8 {
    return switch (kind) {
        .run_table_raw => 0,
        .run_table_physical_block => 1,
        .run_table_block => 1,
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
    while (!mutex.tryLock()) std.atomic.spinLoopHint();
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

test "cache evicts unpinned entries by budget" {
    const allocator = std.testing.allocator;
    var cache = Cache.init(allocator, 1);
    defer cache.deinit();

    var first_state: State = .{};
    try first_state.upsert(allocator, .{ .name = "ns" }, "a", "value-a", false);
    var first = try cache.putRunState("run-1", 1, 1, first_state);
    first.release();

    var second_state: State = .{};
    try second_state.upsert(allocator, .{ .name = "ns" }, "b", "value-b", false);
    var second = try cache.putRunState("run-2", 2, 2, second_state);
    defer second.release();

    try std.testing.expect(cache.currentBytes() > 1);
    try std.testing.expect(cache.retainRunState("run-1", 1, 1) == null);
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

test "cache reports shared byte usage to resource manager" {
    const allocator = std.testing.allocator;
    var budgets = resource_manager_mod.Options.defaultBudgets();
    budgets[@intFromEnum(resource_manager_mod.Slice.lsm_block_table_cache)] = .{
        .soft_limit_bytes = 1,
        .hard_limit_bytes = 2,
    };
    var resource_manager = resource_manager_mod.ResourceManager.init(.{ .budgets = budgets });

    var cache = Cache.init(allocator, 1024 * 1024);
    cache.attachResourceManager(&resource_manager);
    defer cache.deinit();

    var state: State = .{};
    try state.upsert(allocator, .{ .name = "ns" }, "key", "value", false);
    var inserted = try cache.putRunState("run-1", 1, 1, state);

    var stats = resource_manager.snapshot();
    try std.testing.expect(stats.slices[@intFromEnum(resource_manager_mod.Slice.lsm_block_table_cache)].used_bytes > 0);
    try std.testing.expect(stats.slices[@intFromEnum(resource_manager_mod.Slice.lsm_block_table_cache)].soft_limit_events > 0);
    try std.testing.expect(stats.slices[@intFromEnum(resource_manager_mod.Slice.lsm_block_table_cache)].hard_limit_rejections > 0);

    inserted.release();
    cache.invalidatePath("run-1");
    stats = resource_manager.snapshot();
    try std.testing.expectEqual(@as(u64, 0), stats.slices[@intFromEnum(resource_manager_mod.Slice.lsm_block_table_cache)].used_bytes);
}

test "cache shrinks against resource manager pressure target" {
    const allocator = std.testing.allocator;
    var budgets = resource_manager_mod.Options.defaultBudgets();
    budgets[@intFromEnum(resource_manager_mod.Slice.lsm_block_table_cache)] = .{
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
