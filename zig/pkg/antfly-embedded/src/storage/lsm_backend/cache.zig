// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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

const std = @import("std");
const read_admission = @import("../../common/read_admission.zig");
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
    // Both legacy registrations and owned claims have a fixed resident table.
    // Its actual bytes are part of Shard, admitted before shard allocation.
    const pending_capacity: usize = 64;
    const PendingMap = struct {
        const Pair = struct { used: bool = false, key: Key = undefined, value: PendingLoad = undefined };
        const Item = struct { key_ptr: *Key, value_ptr: *PendingLoad };
        const Result = struct { found_existing: bool, key_ptr: *Key, value_ptr: *PendingLoad };
        const Removed = struct { key: Key, value: PendingLoad };
        pairs: [pending_capacity]Pair = @splat(.{}),
        size: usize = 0,
        pub const empty: @This() = .{};
        fn find(self: *@This(), key: Key) ?*Pair {
            for (&self.pairs) |*pair| if (pair.used and (KeyContext{}).eql(key, pair.key)) return pair;
            return null;
        }
        pub fn getPtrAdapted(self: *@This(), key: Key, _: KeyContext) ?*PendingLoad {
            const pair = self.find(key) orelse return null;
            return &pair.value;
        }
        pub fn getAdapted(self: *@This(), key: Key, context: KeyContext) ?PendingLoad {
            return if (self.getPtrAdapted(key, context)) |value| value.* else null;
        }
        pub fn getOrPutContextAdapted(self: *@This(), _: Allocator, key: Key, _: KeyContext, _: KeyContext) !Result {
            if (self.find(key)) |pair| return .{ .found_existing = true, .key_ptr = &pair.key, .value_ptr = &pair.value };
            for (&self.pairs) |*pair| {
                if (!pair.used) {
                    pair.used = true;
                    self.size += 1;
                    // The caller publishes both fields under the pending mutex
                    // without any intervening fallible operation.
                    return .{ .found_existing = false, .key_ptr = &pair.key, .value_ptr = &pair.value };
                }
            }
            return error.ResourceTemporarilyUnavailable;
        }
        pub fn fetchRemoveAdapted(self: *@This(), key: Key, _: KeyContext) ?Removed {
            const pair = self.find(key) orelse return null;
            const removed = Removed{ .key = pair.key, .value = pair.value };
            pair.used = false;
            self.size -= 1;
            return removed;
        }
        pub fn count(self: *const @This()) usize {
            return self.size;
        }
        pub fn deinit(self: *@This(), _: Allocator) void {
            self.* = .empty;
        }
        const Iterator = struct {
            map: *PendingMap,
            index: usize = 0,
            pub fn next(self: *@This()) ?Item {
                while (self.index < pending_capacity) {
                    const pair = &self.map.pairs[self.index];
                    self.index += 1;
                    if (pair.used) return .{ .key_ptr = &pair.key, .value_ptr = &pair.value };
                }
                return null;
            }
        };
        pub fn iterator(self: *@This()) Iterator {
            return .{ .map = self };
        }
    };
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
        deferred_ref_releases: std.atomic.Value(usize) = .init(0),
        cleanup_shard: ?*Shard = null,
        transient_ref_count: std.atomic.Value(usize) = .init(0),
        transient_accounting_cache: ?*Cache = null,
        transient_cleanup: ?*TransientCleanup = null,
        last_access: u64,
        // First attempt after eight hits, then one probe per 256 hits. The
        // sentinel elects one in-flight promoter even across the retry window.
        point_promotion_hits: std.atomic.Value(u16) = .init(7),
        invalidated: bool = false,
        lru_prev: ?*Entry = null,
        lru_next: ?*Entry = null,
        idle_prev: ?*Entry = null,
        idle_next: ?*Entry = null,
        idle_linked: bool = false,
        // Per-path list within the owning shard, so invalidatePath visits only
        // the entries for that path instead of scanning every LRU list.
        path_prev: ?*Entry = null,
        path_next: ?*Entry = null,

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

        pub fn deinit(self: *Entry, allocator: Allocator) void {
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

        pub fn deinit(self: *Value, allocator: Allocator) void {
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

    const load_cleanup_slots: usize = 32;
    const LoadCleanup = struct {
        state: std.atomic.Value(enum(u8) { free, reserved, pending }) = .init(.free),
        key: ?Key = null,
        identity: u64 = 0,
        accounted_bytes: u64 = 0,
        retirement_debt: u64 = 0,
        construction_credit: ?resource_manager_mod.Reservation = null,
    };
    const transient_cleanup_slots: usize = 32;
    const TransientCleanup = struct {
        state: std.atomic.Value(enum(u8) { free, reserved, pending, collecting }) = .init(.free),
        cache: *Cache = undefined,
        entry: ?*Entry = null,
    };
    const Shard = struct {
        mutex: std.atomic.Mutex = .unlocked,
        entries: EntryMap = .empty,
        // Head of each path's entry list. Keys borrow the head entry's path.
        path_heads: std.StringHashMapUnmanaged(*Entry) = .empty,
        lru_heads: [priority_count]?*Entry = @as([priority_count]?*Entry, @splat(null)),
        lru_tails: [priority_count]?*Entry = @as([priority_count]?*Entry, @splat(null)),
        idle_heads: [priority_count]?*Entry = @as([priority_count]?*Entry, @splat(null)),
        idle_tails: [priority_count]?*Entry = @as([priority_count]?*Entry, @splat(null)),
        pending_sync: PendingSync = .{},
        pending_loads: PendingMap = .empty,
        load_cleanup: [load_cleanup_slots]LoadCleanup = @splat(.{}),
        transient_cleanup: [transient_cleanup_slots]TransientCleanup = @splat(.{}),
        deferred_pin_releases: std.atomic.Value(usize) = .init(0),
        pin_cleanup_cursor: ?*Entry = null,
        pin_cleanup_priority: usize = 0,
    };

    const PendingLoad = struct { identity: u64 };

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
    deferred_pin_releases: std.atomic.Value(usize) = .init(0),
    access_clock: CounterU64 = .init(0),
    next_load_identity: CounterU64 = .init(1),
    evict_cursor: std.atomic.Value(usize) = .init(0),
    pressure_target_bytes: std.atomic.Value(usize) = .init(0),
    evict_mutex: std.atomic.Mutex = .unlocked,
    resource_accounting_mutex: std.atomic.Mutex = .unlocked,
    // Key admission waits never retain a pending-map mutex. Collectors only
    // try this lock while holding that mutex, preserving one-way lock order.
    load_key_accounting_mutex: std.atomic.Mutex = .unlocked,
    resource_manager: ?*resource_manager_mod.ResourceManager = null,
    reclaimer_identity: u64 = 0,
    resource_accounted_bytes: u64 = 0,
    /// Physically retired bytes whose ledger debit is still owned by this cache.
    /// Protected by resource_accounting_mutex; remains charged until reconciled.
    resource_retirement_debt: u64 = 0,
    /// Physically retired pin bytes transferred without a ledger lock.
    /// Cache lifetime and the existing charge authorize this resident mailbox.
    transferred_retirement_debt: std.atomic.Value(u64) = .init(0),
    charged_transient_entries: std.atomic.Value(usize) = .init(0),
    pending_transient_entries: std.atomic.Value(usize) = .init(0),
    transient_cleanup_cursor: usize = 0,
    stats: AtomicStats = .{},

    /// Fixed backing storage allocated even while no entry is retained.
    /// Startup owners reserve this separately from per-entry cache estimates.
    pub fn fixedAllocationBytes() usize {
        return default_shard_count * @sizeOf(Shard);
    }

    pub fn init(allocator: Allocator, max_bytes: usize) Cache {
        return initFallible(allocator, max_bytes) catch @panic("OOM");
    }

    pub fn initFallible(allocator: Allocator, max_bytes: usize) Allocator.Error!Cache {
        const shards = try allocator.alloc(Shard, default_shard_count);
        @memset(shards, .{});
        return .{
            .allocator = allocator,
            .max_bytes = max_bytes,
            .shards = shards,
        };
    }

    pub fn deinit(self: *Cache) void {
        self.finishTransientCleanupForTeardown();
        std.debug.assert(self.charged_transient_entries.load(.acquire) == 0);
        if (self.resource_manager) |manager| {
            manager.unregisterReclaimer(self.reclaimer_identity);
            self.reclaimer_identity = 0;
        }
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
            shard.path_heads.deinit(self.allocator);
            shard.pending_sync.lock();
            self.finishLoadCleanupForTeardownLocked(shard);
            for (&shard.load_cleanup) |*slot| std.debug.assert(slot.state.load(.acquire) == .free);
            var pending_it = shard.pending_loads.iterator();
            while (pending_it.next()) |pending| {
                self.allocator.free(pending.key_ptr.path);
            }
            shard.pending_loads.deinit(self.allocator);
            shard.pending_sync.unlock();
        }
        // Physical entries and pending ownership are gone before charged
        // capacity becomes available to another owner, including retained debt.
        self.releaseResourceUsage();
        self.allocator.free(self.shards);
        self.* = undefined;
    }

    pub fn attachResourceManager(self: *Cache, resource_manager: *resource_manager_mod.ResourceManager) void {
        const locked = lockAtomic(&self.resource_accounting_mutex);
        if (self.resource_manager == resource_manager and self.reclaimer_identity != 0) {
            if (locked) self.resource_accounting_mutex.unlock();
            return;
        }
        self.finishTransientCleanupForTeardown();
        std.debug.assert(self.charged_transient_entries.load(.acquire) == 0);
        // Manager replacement is quiescent for loader claims. Retire all
        // transferred physical keys and their old charges before switching.
        for (self.shards) |*shard| {
            shard.pending_sync.lock();
            self.finishLoadCleanupForTeardownLocked(shard);
            shard.pending_sync.unlock();
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
            self.resource_retirement_debt = 0;
            self.transferred_retirement_debt.store(0, .release);
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
        // Reclaim runs on another owner's admission path. It must never wait
        // behind cache accounting: that owner may already hold a shard lock
        // while publishing a release, and a blocking inverse acquisition
        // would turn optional cache retention into process-wide backpressure.
        // A busy cache simply declines this pass; the requester can use its
        // bounded transient path or a later background reclaim pass.
        if (!self.resource_accounting_mutex.tryLock()) return 0;
        defer self.resource_accounting_mutex.unlock();
        const manager = self.resource_manager orelse return 0;
        const before = self.resource_accounted_bytes;
        var inspections: usize = reclaim_inspection_limit;
        var retirements: usize = 0;
        self.tryCollectTransientCleanup(&inspections, &retirements);
        self.adoptTransferredRetirementDebtAccountingLocked();
        if (manager.tryFinishRetirementDebt(.lsm_block_table_cache, &self.resource_accounted_bytes, &self.resource_retirement_debt) != .released) return 0;
        if (!self.tryCollectDeferredPinsAccountingLocked(target_bytes, before, &inspections, &retirements)) return before -| self.resource_accounted_bytes;
        while (before -| self.resource_accounted_bytes < target_bytes and retirements < reclaim_retirement_limit and inspections != 0) {
            if (!self.tryEvictOneAccountingLocked(&inspections)) break;
            retirements += 1;
            // Another owner still holds the released charge. Do not discard
            // more useful entries or claim unreconciled bytes as capacity.
            if (self.resource_retirement_debt != 0) break;
        }
        return before -| self.resource_accounted_bytes;
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

    pub fn retainRunTablePhysicalBlockWithAdmission(self: *Cache, path: []const u8, run_id: u64, generation: u64, block_offset: u64, block_len: u32, control: ?read_admission.Port) !?Handle {
        return self.retainWithBlockControlled(path, run_id, generation, .run_table_physical_block, block_offset, block_len, control);
    }

    pub fn retainRunTableRawWithAdmission(self: *Cache, path: []const u8, run_id: u64, generation: u64, control: ?read_admission.Port) !?Handle {
        return self.retainWithBlockControlled(path, run_id, generation, .run_table_raw, 0, 0, control);
    }
    pub fn retainRunTableIndexWithAdmission(self: *Cache, path: []const u8, run_id: u64, generation: u64, control: ?read_admission.Port) !?Handle {
        return self.retainWithBlockControlled(path, run_id, generation, .run_table_index, 0, 0, control);
    }
    pub fn retainRunTableBlockWithAdmission(self: *Cache, path: []const u8, run_id: u64, generation: u64, block_offset: u64, block_len: u32, control: ?read_admission.Port) !?Handle {
        return self.retainWithBlockControlled(path, run_id, generation, .run_table_block, block_offset, block_len, control);
    }

    pub fn putRunState(self: *Cache, path: []const u8, run_id: u64, generation: u64, state: State) !Handle {
        return try self.put(path, run_id, generation, .{ .run_state = state }, estimateStateCost(path, &state));
    }

    pub fn putRunTableRaw(self: *Cache, path: []const u8, run_id: u64, generation: u64, raw: []u8) !Handle {
        return try self.put(path, run_id, generation, .{ .run_table_raw = raw }, estimateRawTableCost(path, raw));
    }

    pub fn putRunTableIndex(self: *Cache, path: []const u8, run_id: u64, generation: u64, index: TableIndex) !Handle {
        return try self.put(path, run_id, generation, .{ .run_table_index = index }, estimateTableIndexCost(path, &index));
    }

    pub fn putRunTableBlock(self: *Cache, path: []const u8, run_id: u64, generation: u64, block_offset: u64, block_len: u32, block: []u8) !Handle {
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

    /// Consume a decoded buffer and transfer its construction admission into
    /// cache retention when both owners use the same ResourceManager.
    pub fn putRunTableBlockWithCredit(self: *Cache, path: []const u8, run_id: u64, generation: u64, block_offset: u64, block_len: u32, block: []u8, credit: ?*resource_manager_mod.Reservation) !Handle {
        return self.putWithBlockCredit(path, run_id, generation, .{ .run_table_block = block }, estimateTableBlockCost(path, block), block_offset, block_len, false, credit);
    }

    pub fn putRunTableBlockWithCreditAdmission(self: *Cache, path: []const u8, run_id: u64, generation: u64, block_offset: u64, block_len: u32, block: []u8, credit: ?*resource_manager_mod.Reservation, control: read_admission.Port) !Handle {
        return self.putWithBlockReservedCredit(path, run_id, generation, .{ .run_table_block = block }, estimateTableBlockCost(path, block), block_offset, block_len, false, control, false, credit);
    }

    pub fn putTransientRunTableBlock(self: *Cache, path: []const u8, run_id: u64, generation: u64, block_offset: u64, block_len: u32, block: []u8) !Handle {
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

    /// Admit the immutable raw file and publication metadata before input
    /// allocation. The successful read transfers the same reservation to the
    /// cache constructor; failures retire physical input before charged debt.
    pub fn readRunTableRawWithAdmission(self: *Cache, storage: @import("storage_io.zig").Storage, path: []const u8, run_id: u64, generation: u64, max_bytes: usize, control: ?read_admission.Port) !Handle {
        const budget = control orelse {
            const raw = try storage.readFileAlloc(self.allocator, path, max_bytes);
            return self.putRunTableRawWithAdmission(path, run_id, generation, raw, null);
        };
        try budget.checkpoint();
        const size = try storage.fileSizeWithAdmission(path, budget);
        try budget.checkpoint();
        if (size > max_bytes) return error.StreamTooLong;
        const input_bytes = std.math.cast(usize, size) orelse return error.StreamTooLong;
        const metadata = std.math.add(usize, @sizeOf(Entry), path.len) catch return error.ResourceBudgetExceeded;
        const byte_cost = std.math.add(usize, metadata, input_bytes) catch return error.ResourceBudgetExceeded;
        if (!try self.admitResourceGrowthWithAdmission(byte_cost, budget)) return error.ResourceBudgetExceeded;
        var reservation_active = true;
        errdefer if (reservation_active) self.transferRetiredResourceBytes(byte_cost);
        try budget.checkpoint();
        // The immutable file must still fit the admitted size. A changed file
        // cannot obtain extra bytes or silently publish a different snapshot.
        const raw = try storage.readFileAllocWithAdmission(self.allocator, path, input_bytes, budget);
        var raw_owned = true;
        errdefer if (raw_owned) self.allocator.free(raw);
        if (raw.len != input_bytes) return error.EndOfStream;
        try budget.checkpoint();
        raw_owned = false;
        reservation_active = false;
        const handle = try self.putWithBlockReserved(path, run_id, generation, .{ .run_table_raw = raw }, byte_cost, 0, 0, false, budget, true);
        return self.finishPublicationWithAdmission(handle, budget);
    }

    pub fn putRunTableRawWithAdmission(self: *Cache, path: []const u8, run_id: u64, generation: u64, raw: []u8, control: ?read_admission.Port) !Handle {
        const handle = try self.putWithBlockControlled(path, run_id, generation, .{ .run_table_raw = raw }, estimateRawTableCost(path, raw), 0, 0, false, control);
        return self.finishPublicationWithAdmission(handle, control);
    }

    /// Move-only synchronous decode owner. The path and cache must outlive
    /// publication or deinit; no foreground admission callback is retained.
    pub const DecodedIndex = struct {
        cache: ?*Cache,
        path: []const u8,
        index: TableIndex,
        byte_cost: usize,
        prepaid: bool,

        pub fn deinit(self: *DecodedIndex) void {
            const cache = self.cache orelse return;
            self.cache = null;
            self.index.deinit(cache.allocator);
            if (self.prepaid) cache.transferRetiredResourceBytes(self.byte_cost);
        }

        pub fn publish(self: *DecodedIndex, run_id: u64, generation: u64, control: ?read_admission.Port) !Handle {
            const cache = self.cache orelse return error.InvalidArgument;
            if (self.prepaid and control == null) return error.InvalidArgument;
            self.cache = null;
            const handle = try cache.putWithBlockReserved(self.path, run_id, generation, .{ .run_table_index = self.index }, self.byte_cost, 0, 0, false, control, self.prepaid);
            return cache.finishPublicationWithAdmission(handle, control);
        }
    };

    /// Each decoder allocation/growth is admitted before the underlying heap.
    /// Decoder frees retire physical bytes first and transfer debt without
    /// checking a failed scope. Successful slices use the original allocator;
    /// they never retain this stack allocator or its borrowed callback.
    const DecodeAllocator = struct {
        cache: *Cache,
        backing: ?Allocator = null,
        control: read_admission.Port,
        live_bytes: usize = 0,
        failure: ?anyerror = null,

        fn reserve(self: *DecodeAllocator, bytes: usize) bool {
            if (self.failure != null) return false;
            if (bytes > std.math.maxInt(usize) - self.live_bytes) {
                self.failure = error.ResourceBudgetExceeded;
                return false;
            }
            const admitted = self.cache.admitResourceGrowthWithAdmission(bytes, self.control) catch |err| {
                self.failure = err;
                return false;
            };
            if (!admitted) {
                self.failure = error.ResourceBudgetExceeded;
                return false;
            }
            self.live_bytes += bytes;
            return true;
        }
        fn retire(self: *DecodeAllocator, bytes: usize) void {
            std.debug.assert(bytes <= self.live_bytes);
            self.live_bytes -= bytes;
            self.cache.transferRetiredResourceBytes(bytes);
        }
        fn allocator(self: *DecodeAllocator) Allocator {
            return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
        }
        fn alloc(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
            const self: *DecodeAllocator = @ptrCast(@alignCast(raw));
            if (!self.reserve(len)) return null;
            return (self.backing orelse self.cache.allocator).rawAlloc(len, alignment, ret_addr) orelse {
                self.retire(len);
                return null;
            };
        }
        fn resize(raw: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) bool {
            const self: *DecodeAllocator = @ptrCast(@alignCast(raw));
            const growth = new_len -| memory.len;
            if (!self.reserve(growth)) return false;
            if (!(self.backing orelse self.cache.allocator).rawResize(memory, alignment, new_len, ret_addr)) {
                self.retire(growth);
                return false;
            }
            self.retire(memory.len -| new_len);
            return true;
        }
        fn remap(raw: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) ?[*]u8 {
            const self: *DecodeAllocator = @ptrCast(@alignCast(raw));
            const growth = new_len -| memory.len;
            if (!self.reserve(growth)) return null;
            const result = (self.backing orelse self.cache.allocator).rawRemap(memory, alignment, new_len, ret_addr) orelse {
                self.retire(growth);
                return null;
            };
            self.retire(memory.len -| new_len);
            return result;
        }
        fn free(raw: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
            const self: *DecodeAllocator = @ptrCast(@alignCast(raw));
            (self.backing orelse self.cache.allocator).rawFree(memory, alignment, ret_addr);
            self.retire(memory.len);
        }
    };

    pub fn decodeRunTableIndexWithAdmission(self: *Cache, path: []const u8, raw: []const u8, control: ?read_admission.Port) !DecodedIndex {
        const budget = control orelse {
            const index = try lsm_table_file.decodeIndexAlloc(self.allocator, raw);
            return .{ .cache = self, .path = path, .index = index, .byte_cost = estimateTableIndexCost(path, &index), .prepaid = false };
        };
        const metadata = std.math.add(usize, @sizeOf(Entry), path.len) catch return error.ResourceBudgetExceeded;
        if (!try self.admitResourceGrowthWithAdmission(metadata, budget)) return error.ResourceBudgetExceeded;
        errdefer self.transferRetiredResourceBytes(metadata);
        var owner = DecodeAllocator{ .cache = self, .control = budget };
        errdefer std.debug.assert(owner.live_bytes == 0);
        var index = lsm_table_file.decodeIndexAllocWithAdmission(owner.allocator(), raw, budget) catch |err| return owner.failure orelse err;
        errdefer index.deinit(owner.allocator());
        // A swallowed allocation denial cannot authorize publication or a new
        // attempt under fresh controls. Preserve its original typed cause.
        if (owner.failure) |err| return err;
        try budget.checkpoint();
        const byte_cost = std.math.add(usize, metadata, owner.live_bytes) catch return error.ResourceBudgetExceeded;
        std.debug.assert(byte_cost == estimateTableIndexCost(path, &index));
        return .{ .cache = self, .path = path, .index = index, .byte_cost = byte_cost, .prepaid = true };
    }

    /// Footer input, metadata input and decoded slices share one synchronous
    /// admitted allocator. Temporary input retires before decoded ownership
    /// transfers; neither storage nor the published index retains this scope.
    pub fn loadRunTableIndexWithAdmission(self: *Cache, storage: @import("storage_io.zig").Storage, path: []const u8, control: ?read_admission.Port) !DecodedIndex {
        const repository = @import("repository.zig");
        const budget = control orelse {
            const index = try repository.loadRunTableIndexAllocWithStorage(storage, self.allocator, path);
            return .{ .cache = self, .path = path, .index = index, .byte_cost = estimateTableIndexCost(path, &index), .prepaid = false };
        };
        const metadata = std.math.add(usize, @sizeOf(Entry), path.len) catch return error.ResourceBudgetExceeded;
        if (!try self.admitResourceGrowthWithAdmission(metadata, budget)) return error.ResourceBudgetExceeded;
        errdefer self.transferRetiredResourceBytes(metadata);
        var owner = DecodeAllocator{ .cache = self, .control = budget };
        errdefer std.debug.assert(owner.live_bytes == 0);
        var index = repository.loadRunTableIndexAllocWithStorageAndAdmission(storage, owner.allocator(), path, budget) catch |err| return owner.failure orelse err;
        errdefer index.deinit(owner.allocator());
        if (owner.failure) |err| return err;
        try budget.checkpoint();
        const byte_cost = std.math.add(usize, metadata, owner.live_bytes) catch return error.ResourceBudgetExceeded;
        std.debug.assert(byte_cost == estimateTableIndexCost(path, &index));
        return .{ .cache = self, .path = path, .index = index, .byte_cost = byte_cost, .prepaid = true };
    }

    /// Backend-owned materialized table. Cache, original allocator and all
    /// physical slices stay alive through deinit; no decoder scope is retained.
    pub const DecodedTable = struct {
        cache: ?*Cache,
        allocator: Allocator,
        table: lsm_table_file.BorrowedDecoded,
        byte_cost: usize,

        pub fn deinit(self: *DecodedTable) void {
            const cache = self.cache orelse return;
            self.cache = null;
            self.table.deinit(self.allocator);
            cache.transferRetiredResourceBytes(self.byte_cost);
        }
    };

    pub fn copyAndDecodeRunTableWithAdmission(self: *Cache, allocator: Allocator, raw: []const u8, control: read_admission.Port) !DecodedTable {
        var owner = DecodeAllocator{ .cache = self, .backing = allocator, .control = control };
        errdefer std.debug.assert(owner.live_bytes == 0);
        const admitted = owner.allocator();
        const copy = admitted.dupe(u8, raw) catch |err| return owner.failure orelse err;
        var copy_owned = true;
        errdefer if (copy_owned) admitted.free(copy);
        // Failure leaves the input with this owner. Success consumes it,
        // including physical replacement by the decompressed logical body.
        var table = lsm_table_file.decodeBorrowedOwnedAllocWithAdmission(admitted, copy, control) catch |err| return owner.failure orelse err;
        copy_owned = false;
        errdefer table.deinit(admitted);
        if (owner.failure) |err| return err;
        try control.checkpoint();
        return .{ .cache = self, .allocator = allocator, .table = table, .byte_cost = owner.live_bytes };
    }

    pub fn putRunTableIndexWithAdmission(self: *Cache, path: []const u8, run_id: u64, generation: u64, index: TableIndex, control: ?read_admission.Port) !Handle {
        const handle = try self.putWithBlockControlled(path, run_id, generation, .{ .run_table_index = index }, estimateTableIndexCost(path, &index), 0, 0, false, control);
        return self.finishPublicationWithAdmission(handle, control);
    }
    /// Move-only decoded block and its prepaid metadata/buffer charge. The
    /// path/cache outlive publication or deinit; no read scope is retained.
    pub const DecodedBlock = struct {
        cache: ?*Cache,
        path: []const u8,
        bytes: []u8,
        byte_cost: usize,
        offset: u64,
        physical_len: u32,
        prepaid: bool,

        pub fn deinit(self: *DecodedBlock) void {
            const cache = self.cache orelse return;
            self.cache = null;
            cache.allocator.free(self.bytes);
            if (self.prepaid) cache.transferRetiredResourceBytes(self.byte_cost);
        }

        pub fn publish(self: *DecodedBlock, run_id: u64, generation: u64, retain_block: bool, control: ?read_admission.Port) !Handle {
            const cache = self.cache orelse return error.InvalidArgument;
            if (self.prepaid and control == null) return error.InvalidArgument;
            self.cache = null;
            const handle = try cache.putWithBlockReserved(self.path, run_id, generation, .{ .run_table_block = self.bytes }, self.byte_cost, self.offset, self.physical_len, !retain_block, control, self.prepaid);
            return cache.finishPublicationWithAdmission(handle, control);
        }
    };

    /// Reader is synchronous and consumes the supplied admitted allocator for
    /// input. Decoding and temporary decompression buffers use that allocator
    /// too; the physical input retires before output ownership transfers.
    pub fn loadRunTableBlockWithAdmission(self: *Cache, reader: anytype, path: []const u8, offset: u64, physical_len: u32, compression: lsm_table_file.BlockCompression, logical_len: u32, checksum: u32, control: ?read_admission.Port) !DecodedBlock {
        const budget = control orelse {
            const payload = try reader.readFileRangeAlloc(self.allocator, path, offset, physical_len);
            defer self.allocator.free(payload);
            const bytes = try lsm_table_file.decodeBlockPayloadAlloc(self.allocator, compression, payload, logical_len, checksum);
            return .{ .cache = self, .path = path, .bytes = bytes, .byte_cost = estimateRawTableCost(path, bytes), .offset = offset, .physical_len = physical_len, .prepaid = false };
        };
        const metadata = std.math.add(usize, @sizeOf(Entry), path.len) catch return error.ResourceBudgetExceeded;
        if (!try self.admitResourceGrowthWithAdmission(metadata, budget)) return error.ResourceBudgetExceeded;
        errdefer self.transferRetiredResourceBytes(metadata);
        var owner = DecodeAllocator{ .cache = self, .control = budget };
        errdefer std.debug.assert(owner.live_bytes == 0);
        const admitted = owner.allocator();
        const bytes = blk: {
            try budget.checkpoint();
            const payload = reader.readFileRangeAlloc(admitted, path, offset, physical_len) catch |err| return owner.failure orelse err;
            defer admitted.free(payload);
            try budget.checkpoint();
            if (payload.len != physical_len) return error.EndOfStream;
            const decoded = lsm_table_file.decodeBlockPayloadAllocWithAdmission(admitted, compression, payload, logical_len, checksum, budget) catch |err| return owner.failure orelse err;
            errdefer admitted.free(decoded);
            if (owner.failure) |err| return err;
            try budget.checkpoint();
            break :blk decoded;
        };
        errdefer admitted.free(bytes);
        const byte_cost = std.math.add(usize, metadata, owner.live_bytes) catch return error.ResourceBudgetExceeded;
        std.debug.assert(byte_cost == estimateRawTableCost(path, bytes));
        return .{ .cache = self, .path = path, .bytes = bytes, .byte_cost = byte_cost, .offset = offset, .physical_len = physical_len, .prepaid = true };
    }

    pub fn putRunTableBlockWithAdmission(self: *Cache, path: []const u8, run_id: u64, generation: u64, block_offset: u64, block_len: u32, block: []u8, control: ?read_admission.Port) !Handle {
        const handle = try self.putWithBlockControlled(path, run_id, generation, .{ .run_table_block = block }, estimateRawTableCost(path, block), block_offset, block_len, false, control);
        return self.finishPublicationWithAdmission(handle, control);
    }
    pub fn putTransientRunTableBlockWithAdmission(self: *Cache, path: []const u8, run_id: u64, generation: u64, block_offset: u64, block_len: u32, block: []u8, control: ?read_admission.Port) !Handle {
        const handle = try self.putWithBlockControlled(path, run_id, generation, .{ .run_table_block = block }, estimateRawTableCost(path, block), block_offset, block_len, true, control);
        return self.finishPublicationWithAdmission(handle, control);
    }

    pub fn putRunTablePhysicalBlockWithAdmission(self: *Cache, path: []const u8, run_id: u64, generation: u64, block_offset: u64, block_len: u32, block: []u8, control: ?read_admission.Port) !Handle {
        const handle = try self.putWithBlockControlled(path, run_id, generation, .{ .run_table_physical_block = block }, estimateRawTableCost(path, block), block_offset, block_len, false, control);
        return self.finishPublicationWithAdmission(handle, control);
    }
    pub fn putTransientRunTablePhysicalBlockWithAdmission(self: *Cache, path: []const u8, run_id: u64, generation: u64, block_offset: u64, block_len: u32, block: []u8, control: ?read_admission.Port) !Handle {
        const handle = try self.putWithBlockControlled(path, run_id, generation, .{ .run_table_physical_block = block }, estimateRawTableCost(path, block), block_offset, block_len, true, control);
        return self.finishPublicationWithAdmission(handle, control);
    }

    /// The input has already been consumed. A failed maintenance admission
    /// releases only this handle's pin, preserving the cache's owned value.
    fn finishPublicationWithAdmission(self: *Cache, handle: Handle, control: ?read_admission.Port) !Handle {
        const budget = control orelse return handle;
        var owned = handle;
        errdefer owned.releaseForAdmissionFailure();
        try budget.checkpoint();
        if (owned.cache != null) {
            try self.refreshResourcePressureWithAdmission(budget);
            try self.evictToBudgetWithAdmission(budget);
        }
        try budget.checkpoint();
        return owned;
    }

    pub fn beginLoad(self: *Cache, path: []const u8, run_id: u64, generation: u64, kind: Kind) !void {
        return self.beginLoadWithBlock(path, run_id, generation, kind, 0, 0);
    }

    pub fn beginLoadWithBlock(self: *Cache, path: []const u8, run_id: u64, generation: u64, kind: Kind, block_offset: u64, block_len: u32) !void {
        _ = try self.beginLoadIdentity(makeKey(path, run_id, generation, kind, block_offset, block_len), null);
    }

    pub fn beginLoadWithAdmission(self: *Cache, path: []const u8, run_id: u64, generation: u64, kind: Kind, control: read_admission.Port) !void {
        return self.beginLoadWithBlockAdmission(path, run_id, generation, kind, 0, 0, control);
    }

    pub fn beginLoadWithBlockAdmission(self: *Cache, path: []const u8, run_id: u64, generation: u64, kind: Kind, block_offset: u64, block_len: u32, control: read_admission.Port) !void {
        _ = try self.beginLoadIdentity(makeKey(path, run_id, generation, kind, block_offset, block_len), control);
    }

    /// Reserve the owned cleanup key before publishing a claim. The returned
    /// token survives request/snapshot path destruction and retains no budget.
    pub fn beginLoadClaim(self: *Cache, path: []const u8, run_id: u64, generation: u64, kind: Kind, block_offset: u64, block_len: u32, control: ?read_admission.Port) !LoadClaim {
        if (control) |budget| try budget.checkpoint();
        const key_bytes = std.math.mul(usize, path.len, 2) catch return error.ResourceBudgetExceeded;
        if (self.resource_manager != null) {
            if (control) |budget| try self.drainLoadCleanupWithAdmission(budget);
        }
        const borrowed_key = makeKey(path, run_id, generation, kind, block_offset, block_len);
        const shard = self.shardForKey(borrowed_key);
        const slot = blk: {
            if (control) |budget| try lockPendingWithAdmission(&shard.pending_sync, budget) else shard.pending_sync.lock();
            defer shard.pending_sync.unlock();
            self.drainLoadCleanupLocked(shard);
            if (shard.pending_loads.count() == pending_capacity) return error.ResourceTemporarilyUnavailable;
            for (&shard.load_cleanup) |*candidate| {
                if (candidate.state.load(.acquire) == .free) {
                    candidate.state.store(.reserved, .release);
                    break :blk candidate;
                }
            }
            return error.ResourceTemporarilyUnavailable;
        };
        // Even a failed preallocation owns its published credit until a cache
        // pass releases it. Error unwinding performs no ledger or heap cleanup.
        errdefer slot.state.store(.pending, .release);
        try self.admitLoadKeyBytes(slot, key_bytes, control);
        slot.identity = 0;
        const key = try copyKey(self.allocator, borrowed_key);
        slot.key = key;
        const identity = try self.beginLoadIdentity(key, control);
        slot.identity = identity;
        return .{ .cache = self, .key = key, .identity = identity, .cleanup = slot };
    }

    fn admitLoadKeyBytes(self: *Cache, slot: *LoadCleanup, bytes: usize, control: ?read_admission.Port) !void {
        if (control) |budget| try @import("../../common/bounded_mutex.zig").lockAtomic(&self.load_key_accounting_mutex, budget) else _ = lockAtomic(&self.load_key_accounting_mutex);
        defer self.load_key_accounting_mutex.unlock();
        std.debug.assert(slot.accounted_bytes == 0 and slot.retirement_debt == 0 and slot.key == null and slot.construction_credit == null);
        const manager = self.resource_manager orelse return;
        if (control) |budget|
            try manager.adjustUsageWithAdmission(.lsm_block_table_cache, &slot.accounted_bytes, @intCast(bytes), budget)
        else
            try manager.adjustUsage(.lsm_block_table_cache, &slot.accounted_bytes, @intCast(bytes));
    }

    const LoadKeyRetirement = struct {
        cache: *Cache,
        shard: *Shard,
        slot: *LoadCleanup,
        pub fn retire(self: @This()) void {
            const key = self.slot.key orelse return;
            if (self.shard.pending_loads.getPtrAdapted(key, KeyContext{})) |pending| {
                if (pending.identity == self.slot.identity) {
                    const removed = self.shard.pending_loads.fetchRemoveAdapted(key, KeyContext{}).?;
                    self.cache.allocator.free(removed.key.path);
                    self.shard.pending_sync.broadcast();
                }
            }
            self.cache.allocator.free(key.path);
            self.slot.key = null;
        }
    };

    // Fixed, nonwaiting ledger attempts. Busy parents keep the charge in a
    // cache-owned slot after physical freeing; no progress callback runs here.
    fn drainLoadCleanupLocked(self: *Cache, shard: *Shard) void {
        if (!self.load_key_accounting_mutex.tryLock()) return;
        defer self.load_key_accounting_mutex.unlock();
        for (&shard.load_cleanup) |*slot| {
            if (slot.state.load(.acquire) != .pending) continue;
            if (tryFinishLoadCredit(slot) != .released) continue;
            const retirement = LoadKeyRetirement{ .cache = self, .shard = shard, .slot = slot };
            if (self.resource_manager) |manager| {
                if (slot.accounted_bytes > slot.retirement_debt) {
                    if (manager.tryRetireUsageWithRetainedDebt(.lsm_block_table_cache, &slot.accounted_bytes, &slot.retirement_debt, slot.accounted_bytes - slot.retirement_debt, retirement) != .retired) continue;
                } else if (slot.key != null) {
                    retirement.retire();
                }
                if (manager.tryFinishRetirementDebt(.lsm_block_table_cache, &slot.accounted_bytes, &slot.retirement_debt) != .released) continue;
            } else retirement.retire();
            slot.state.store(.free, .release);
        }
    }

    // Quiescent owner teardown/manager replacement permits mandatory legacy
    // accounting. Physical keys disappear before any parent capacity returns.
    fn finishLoadCleanupForTeardownLocked(self: *Cache, shard: *Shard) void {
        _ = lockAtomic(&self.load_key_accounting_mutex);
        defer self.load_key_accounting_mutex.unlock();
        for (&shard.load_cleanup) |*slot| {
            std.debug.assert(slot.state.load(.acquire) != .reserved);
            if (slot.state.load(.acquire) != .pending) continue;
            if (slot.construction_credit) |*credit| credit.release();
            slot.construction_credit = null;
            (LoadKeyRetirement{ .cache = self, .shard = shard, .slot = slot }).retire();
            if (self.resource_manager) |manager| manager.observeUsage(.lsm_block_table_cache, &slot.accounted_bytes, 0);
            slot.retirement_debt = 0;
            slot.state.store(.free, .release);
        }
    }

    /// Retire cache-owned loader cleanup using this independent finite scope.
    /// Failure leaves every unprocessed slot owned by the cache until another
    /// admitted pass or quiescent teardown, without resurrecting foreground IO.
    pub fn drainLoadCleanupWithAdmission(self: *Cache, control: read_admission.Port) !void {
        for (self.shards) |*shard| {
            while (true) {
                try lockPendingWithAdmission(&shard.pending_sync, control);
                self.drainLoadCleanupLocked(shard);
                shard.pending_sync.unlock();
                // Parent waits run after releasing the pending mutex. A busy
                // child ledger also retries only outside physical-owner locks.
                try @import("../../common/bounded_mutex.zig").lockAtomic(&self.load_key_accounting_mutex, control);
                var physical_pending = false;
                {
                    defer self.load_key_accounting_mutex.unlock();
                    for (&shard.load_cleanup) |*slot| {
                        if (slot.state.load(.acquire) != .pending) continue;
                        switch (tryFinishLoadCredit(slot)) {
                            .released => {},
                            .busy => {
                                physical_pending = true;
                                continue;
                            },
                            .unsupported => return error.ExecutionWaitUnsupported,
                            .mismatch => return error.ResourceAccountingMismatch,
                        }
                        if (slot.key != null) {
                            physical_pending = true;
                            continue;
                        }
                        if (self.resource_manager) |manager| {
                            if (slot.retirement_debt != 0) {
                                const next = slot.accounted_bytes - slot.retirement_debt;
                                manager.adjustUsageWithAdmission(.lsm_block_table_cache, &slot.accounted_bytes, next, control) catch |err| {
                                    // If an admission implementation reports
                                    // failure after committing the release, clear
                                    // matching debt before preserving its cause.
                                    if (slot.accounted_bytes == next) {
                                        slot.retirement_debt = 0;
                                        if (next == 0) slot.state.store(.free, .release);
                                    }
                                    return err;
                                };
                                slot.retirement_debt = 0;
                            }
                        }
                        if (slot.accounted_bytes == 0) slot.state.store(.free, .release);
                    }
                }
                if (!physical_pending) break;
                try control.waitForProgress();
            }
        }
    }

    fn tryFinishLoadCredit(slot: *LoadCleanup) resource_manager_mod.ResourceManager.MemoryParent.ReleaseResult {
        if (slot.construction_credit) |*credit| {
            const result = credit.manager.tryReleaseReservation(credit);
            if (result != .released) return result;
            slot.construction_credit = null;
        }
        return .released;
    }

    fn nextLoadIdentity(self: *Cache) !u64 {
        var observed = self.next_load_identity.load(.monotonic);
        while (observed != std.math.maxInt(u64)) {
            if (self.next_load_identity.cmpxchgWeak(observed, observed + 1, .monotonic, .monotonic)) |actual| {
                observed = actual;
            } else return observed;
        }
        return error.CacheLoadIdentityExhausted;
    }

    fn beginLoadIdentity(self: *Cache, key: Key, control: ?read_admission.Port) !u64 {
        const shard = self.shardForKey(key);
        while (true) {
            {
                if (control) |budget| try lockPendingWithAdmission(&shard.pending_sync, budget) else shard.pending_sync.lock();
                defer shard.pending_sync.unlock();
                self.drainLoadCleanupLocked(shard);
                if (shard.pending_loads.getPtrAdapted(key, KeyContext{}) == null) {
                    if (shard.pending_loads.count() == pending_capacity) return error.ResourceTemporarilyUnavailable;
                    const owned_key = try copyKey(self.allocator, key);
                    errdefer self.allocator.free(owned_key.path);
                    if (control) |budget| try budget.checkpoint();
                    const identity = try self.nextLoadIdentity();
                    const gop = try shard.pending_loads.getOrPutContextAdapted(self.allocator, key, KeyContext{}, KeyContext{});
                    std.debug.assert(!gop.found_existing);
                    gop.key_ptr.* = owned_key;
                    gop.value_ptr.* = .{ .identity = identity };
                    return identity;
                }
                self.bumpWait(key.kind);
                if (control == null) {
                    shard.pending_sync.wait();
                    continue;
                }
            }
            // Progress can complete another claim; never borrow it under the
            // pending mutex or replace the original wait authority.
            try control.?.waitForProgress();
        }
    }

    /// Optional work never waits for a loader or the pending-map mutex.
    /// Its independent key and charge survive a stopped caller. Cleanup is an
    /// infallible transfer into fixed cache storage, as for mandatory loads.
    pub fn tryBeginLoadClaim(self: *Cache, path: []const u8, run_id: u64, generation: u64, kind: Kind, block_offset: u64, block_len: u32, control: ?read_admission.Port) !?LoadClaim {
        if (control) |budget| try budget.checkpoint();
        const key_bytes = std.math.mul(usize, path.len, 2) catch return error.ResourceBudgetExceeded;
        const borrowed_key = makeKey(path, run_id, generation, kind, block_offset, block_len);
        const shard = self.shardForKey(borrowed_key);
        const slot = blk: {
            if (!shard.pending_sync.tryLock()) return null;
            defer shard.pending_sync.unlock();
            self.drainLoadCleanupLocked(shard);
            if (shard.pending_loads.getPtrAdapted(borrowed_key, KeyContext{}) != null or shard.pending_loads.count() == pending_capacity) return null;
            for (&shard.load_cleanup) |*candidate| {
                if (candidate.state.load(.acquire) == .free) {
                    candidate.state.store(.reserved, .release);
                    break :blk candidate;
                }
            }
            return null;
        };
        var transferred = false;
        defer if (!transferred) slot.state.store(.pending, .release);
        // Never wait for accounting while holding the pending-map lock.
        try self.admitLoadKeyBytes(slot, key_bytes, control);
        slot.identity = 0;
        const key = try copyKey(self.allocator, borrowed_key);
        slot.key = key;
        if (control) |budget| try budget.checkpoint();
        if (!shard.pending_sync.tryLock()) return null;
        defer shard.pending_sync.unlock();
        // Another loader may have won while we admitted the owned key.
        if (shard.pending_loads.getPtrAdapted(key, KeyContext{}) != null or shard.pending_loads.count() == pending_capacity) return null;
        try self.publishPendingLoadLocked(shard, key);
        const identity = shard.pending_loads.getPtrAdapted(key, KeyContext{}).?.identity;
        slot.identity = identity;
        transferred = true;
        // Publication commits ownership; no fallible checkpoint may follow.
        return .{ .cache = self, .key = key, .identity = identity, .cleanup = slot };
    }

    /// Legacy optional registration; callers must finish its borrowed key.
    pub fn tryBeginLoadWithBlock(self: *Cache, path: []const u8, run_id: u64, generation: u64, kind: Kind, block_offset: u64, block_len: u32) !bool {
        const key = makeKey(path, run_id, generation, kind, block_offset, block_len);
        const shard = self.shardForKey(key);
        if (!shard.pending_sync.tryLock()) return false;
        defer shard.pending_sync.unlock();
        self.drainLoadCleanupLocked(shard);
        if (shard.pending_loads.getPtrAdapted(key, KeyContext{}) != null) return false;
        if (shard.pending_loads.count() == pending_capacity) return false;
        try self.publishPendingLoadLocked(shard, key);
        return true;
    }

    fn publishPendingLoadLocked(self: *Cache, shard: *Shard, key: Key) !void {
        // Own the key before publishing a map slot. OOM leaves no pending owner.
        const owned_key = try copyKey(self.allocator, key);
        errdefer self.allocator.free(owned_key.path);
        const identity = try self.nextLoadIdentity();
        const gop = try shard.pending_loads.getOrPutContextAdapted(self.allocator, key, KeyContext{}, KeyContext{});
        std.debug.assert(!gop.found_existing);
        gop.key_ptr.* = owned_key;
        gop.value_ptr.* = .{ .identity = identity };
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

            var current = shard.path_heads.get(path);
            while (current) |entry| {
                const next = entry.path_next;
                entry.invalidated = true;
                self.bumpInvalidation(entry.kind);
                if (entry.ref_count == 0) self.removeEntryLocked(shard, entry);
                current = next;
            }
            if (locked) shard.mutex.unlock();
        }
    }

    /// Capacity was reserved by the caller before the entry became visible.
    fn linkPathLocked(shard: *Shard, entry: *Entry) void {
        const gop = shard.path_heads.getOrPutAssumeCapacity(entry.path);
        if (gop.found_existing) {
            const head = gop.value_ptr.*;
            head.path_prev = entry;
            entry.path_next = head;
        }
        gop.key_ptr.* = entry.path;
        gop.value_ptr.* = entry;
    }

    fn unlinkPathLocked(shard: *Shard, entry: *Entry) void {
        if (entry.path_next) |next| next.path_prev = entry.path_prev;
        if (entry.path_prev) |prev| {
            prev.path_next = entry.path_next;
        } else if (entry.path_next) |next| {
            // The map key borrows the head's path; repoint it before the old
            // head (and its path) is freed.
            const kv = shard.path_heads.getEntry(entry.path) orelse unreachable;
            kv.key_ptr.* = next.path;
            kv.value_ptr.* = next;
        } else {
            _ = shard.path_heads.remove(entry.path);
        }
        entry.path_prev = null;
        entry.path_next = null;
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
            self.drainLoadCleanupLocked(shard);
            count += shard.pending_loads.count();
            shard.pending_sync.unlock();
        }
        return count;
    }

    fn retain(self: *Cache, path: []const u8, run_id: u64, generation: u64, kind: Kind) ?Handle {
        return self.retainWithBlock(path, run_id, generation, kind, 0, 0);
    }

    fn retainWithBlock(self: *Cache, path: []const u8, run_id: u64, generation: u64, kind: Kind, block_offset: u64, block_len: u32) ?Handle {
        return self.retainWithBlockControlled(path, run_id, generation, kind, block_offset, block_len, null) catch unreachable;
    }

    fn retainWithBlockControlled(self: *Cache, path: []const u8, run_id: u64, generation: u64, kind: Kind, block_offset: u64, block_len: u32, control: ?read_admission.Port) !?Handle {
        if (self.deferred_pin_releases.load(.acquire) != 0) {
            if (control) |budget| try self.drainDeferredPinsWithAdmission(budget);
        }
        if (control) |budget| try budget.checkpoint();
        const key = makeKey(path, run_id, generation, kind, block_offset, block_len);
        const shard = self.shardForKey(key);
        const locked = if (control) |budget| blk: {
            try @import("../../common/bounded_mutex.zig").lockAtomic(&shard.mutex, budget);
            break :blk true;
        } else lockAtomic(&shard.mutex);
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
        return self.putWithBlockControlled(path, run_id, generation, value, byte_cost, block_offset, block_len, force_transient, null);
    }

    fn putWithBlockControlled(self: *Cache, path: []const u8, run_id: u64, generation: u64, value: Value, byte_cost: usize, block_offset: u64, block_len: u32, force_transient: bool, control: ?read_admission.Port) !Handle {
        return self.putWithBlockReserved(path, run_id, generation, value, byte_cost, block_offset, block_len, force_transient, control, false);
    }

    fn putWithBlockReserved(self: *Cache, path: []const u8, run_id: u64, generation: u64, value: Value, byte_cost: usize, block_offset: u64, block_len: u32, force_transient: bool, control: ?read_admission.Port, prepaid: bool) !Handle {
        return self.putWithBlockReservedCredit(path, run_id, generation, value, byte_cost, block_offset, block_len, force_transient, control, prepaid, null);
    }

    fn putWithBlockCredit(self: *Cache, path: []const u8, run_id: u64, generation: u64, value: Value, byte_cost: usize, block_offset: u64, block_len: u32, force_transient: bool, credit: ?*resource_manager_mod.Reservation) !Handle {
        return self.putWithBlockReservedCredit(path, run_id, generation, value, byte_cost, block_offset, block_len, force_transient, null, false, credit);
    }

    fn putWithBlockReservedCredit(self: *Cache, path: []const u8, run_id: u64, generation: u64, value: Value, byte_cost: usize, block_offset: u64, block_len: u32, force_transient: bool, control: ?read_admission.Port, prepaid: bool, credit: ?*resource_manager_mod.Reservation) !Handle {
        // Consume the value on every outcome. Descriptor errdefers run first;
        // destroy the input before transferring charged retirement debt, with
        // no further call into a failed foreground scope.
        var reservation_active = prepaid;
        var transient_cleanup: ?*TransientCleanup = null;
        errdefer {
            var cleanup = value;
            cleanup.deinit(self.allocator);
            if (reservation_active) {
                if (control != null) self.transferRetiredResourceBytes(byte_cost) else self.releaseResourceBytes(byte_cost);
            }
            if (transient_cleanup) |slot| slot.state.store(.free, .release);
        }
        if (control) |budget| try budget.checkpoint();
        const kind = std.meta.activeTag(value);
        const key = makeKey(path, run_id, generation, kind, block_offset, block_len);
        // Admit the complete estimate before allocating entry/path metadata.
        // Retention stays optional; managed admitted transients still require
        // their physical bytes to fit the node memory envelope.
        const retention_admitted = !force_transient and byte_cost <= self.effectiveMaxBytes() and (prepaid or (if (credit) |construction| (if (control) |budget| try self.admitResourceGrowthWithCreditAdmission(byte_cost, construction, budget) else self.admitResourceGrowthWithCredit(byte_cost, construction)) else if (control) |budget| try self.admitResourceGrowthWithAdmission(byte_cost, budget) else self.admitResourceGrowth(byte_cost)));
        reservation_active = prepaid or retention_admitted;
        // Optional promotion already owns construction credit. Rejecting
        // retention consumes the buffer; never charge it again as a transient.
        if (!retention_admitted and control != null and credit != null) return error.ResourceBudgetExceeded;
        if (!retention_admitted) {
            if (control) |budget| transient_cleanup = try self.reserveTransientCleanup(key, budget);
        }
        const transient_charged = !retention_admitted and control != null and self.resource_manager != null;
        if (transient_charged) {
            if (!prepaid and !try self.admitResourceGrowthWithAdmission(byte_cost, control.?)) return error.ResourceBudgetExceeded;
            reservation_active = true;
        }
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
        entry.cleanup_shard = shard;
        const locked = if (control) |budget| blk: {
            try @import("../../common/bounded_mutex.zig").lockAtomic(&shard.mutex, budget);
            break :blk true;
        } else lockAtomic(&shard.mutex);
        errdefer if (locked) shard.mutex.unlock();
        if (self.findEntryLocked(shard, key)) |existing| {
            existing.ref_count += 1;
            existing.last_access = self.nextAccess();
            self.touchEntryLocked(shard, existing);
            entry.deinit(self.allocator);
            self.allocator.destroy(entry);
            if (transient_cleanup) |slot| slot.state.store(.free, .release);
            if (reservation_active) {
                if (control != null) self.transferRetiredResourceBytes(byte_cost) else self.releaseResourceBytes(byte_cost);
                reservation_active = false;
            }
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
            if (transient_cleanup) |slot| {
                slot.entry = entry;
                entry.transient_cleanup = slot;
            }
            if (transient_charged) {
                entry.transient_accounting_cache = self;
                _ = self.charged_transient_entries.fetchAdd(1, .release);
            }
            reservation_active = false;
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

        if (control) |budget| try budget.checkpoint();
        entry.last_access = self.nextAccess();
        try shard.path_heads.ensureUnusedCapacity(self.allocator, 1);
        const gop = try shard.entries.getOrPutContextAdapted(self.allocator, key, KeyContext{}, KeyContext{});
        if (gop.found_existing) {
            const existing = gop.value_ptr.*;
            existing.ref_count += 1;
            existing.last_access = self.nextAccess();
            self.touchEntryLocked(shard, existing);
            entry.deinit(self.allocator);
            self.allocator.destroy(entry);
            if (control != null) self.transferRetiredResourceBytes(byte_cost) else self.releaseResourceBytes(byte_cost);
            reservation_active = false;
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
        linkPathLocked(shard, entry);
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
        if (control == null) {
            self.refreshResourcePressure();
            self.evictToBudget();
        }
        return .{
            .cache = self,
            .allocator = self.allocator,
            .entry = entry,
            .kind = kind,
        };
    }

    fn reserveTransientCleanup(self: *Cache, key: Key, control: read_admission.Port) !*TransientCleanup {
        if (self.pending_transient_entries.load(.acquire) != 0) try self.drainTransientCleanupWithAdmission(control);
        try control.checkpoint();
        const shard = self.shardForKey(key);
        for (&shard.transient_cleanup) |*slot| {
            if (slot.state.cmpxchgStrong(.free, .reserved, .acq_rel, .acquire) != null) continue;
            std.debug.assert(slot.entry == null);
            slot.cache = self;
            return slot;
        }
        return error.ResourceBudgetExceeded;
    }

    fn collectTransientSlot(self: *Cache, slot: *TransientCleanup) bool {
        if (slot.state.cmpxchgStrong(.pending, .collecting, .acq_rel, .acquire) != null) return false;
        const entry = slot.entry.?;
        std.debug.assert(entry.transient_ref_count.load(.acquire) == 0);
        const accounting_cache = entry.transient_accounting_cache;
        const bytes = entry.byte_cost;
        entry.deinit(self.allocator);
        self.allocator.destroy(entry);
        if (accounting_cache) |cache| {
            cache.transferRetiredResourceBytes(bytes);
            _ = cache.charged_transient_entries.fetchSub(1, .release);
        }
        slot.entry = null;
        _ = self.pending_transient_entries.fetchSub(1, .release);
        slot.state.store(.free, .release);
        return true;
    }

    /// A fresh scope owns collection; returning handles never invoke it.
    /// Allocator destruction remains an indivisible native operation.
    pub fn drainTransientCleanupWithAdmission(self: *Cache, control: read_admission.Port) !void {
        try control.checkpoint();
        while (self.pending_transient_entries.load(.acquire) != 0) {
            for (self.shards) |*shard| {
                try control.checkpoint();
                for (&shard.transient_cleanup) |*slot| {
                    if (slot.state.load(.acquire) != .pending) continue;
                    try control.checkpoint();
                    _ = self.collectTransientSlot(slot);
                }
            }
            if (self.pending_transient_entries.load(.acquire) != 0) try control.waitForProgress();
        }
        try self.finishRetirementsWithAdmission(control);
    }

    // Shares the global inspection/retirement limits. Accounting owns the
    // cursor; the per-slot CAS owns physical destruction.
    fn tryCollectTransientCleanup(self: *Cache, inspections: *usize, retirements: *usize) void {
        const count = self.shards.len * transient_cleanup_slots;
        while (self.pending_transient_entries.load(.acquire) != 0 and inspections.* != 0 and retirements.* < reclaim_retirement_limit) {
            const index = self.transient_cleanup_cursor % count;
            self.transient_cleanup_cursor = (index + 1) % count;
            inspections.* -= 1;
            const slot = &self.shards[index / transient_cleanup_slots].transient_cleanup[index % transient_cleanup_slots];
            if (self.collectTransientSlot(slot)) retirements.* += 1;
        }
    }

    // Teardown/replacement is quiescent for every published physical handle.
    fn finishTransientCleanupForTeardown(self: *Cache) void {
        for (self.shards) |*shard| {
            for (&shard.transient_cleanup) |*slot| {
                _ = self.collectTransientSlot(slot);
                std.debug.assert(slot.state.load(.acquire) == .free and slot.entry == null);
            }
        }
        std.debug.assert(self.pending_transient_entries.load(.acquire) == 0);
    }

    fn transferPinToCache(self: *Cache, entry: *Entry) void {
        const shard = entry.cleanup_shard.?;
        // Announce before final entry publication. A collector can observe an
        // in-progress producer and retry only through its independent scope.
        _ = self.deferred_pin_releases.fetchAdd(1, .release);
        _ = shard.deferred_pin_releases.fetchAdd(1, .release);
        // Final entry access: a collector may retire and destroy it now.
        _ = entry.deferred_ref_releases.fetchAdd(1, .release);
    }

    fn nextPinCleanupEntryLocked(shard: *Shard) ?*Entry {
        var entry = shard.pin_cleanup_cursor;
        if (entry == null) {
            for (0..priority_count) |_| {
                entry = shard.lru_heads[shard.pin_cleanup_priority];
                if (entry != null) break;
                shard.pin_cleanup_priority = (shard.pin_cleanup_priority + 1) % priority_count;
            }
        }
        const current = entry orelse return null;
        shard.pin_cleanup_cursor = current.lru_next;
        if (shard.pin_cleanup_cursor == null) shard.pin_cleanup_priority = (shard.pin_cleanup_priority + 1) % priority_count;
        return current;
    }

    fn consumeDeferredPinsLocked(self: *Cache, shard: *Shard, entry: *Entry) usize {
        const released = entry.deferred_ref_releases.swap(0, .acq_rel);
        if (released == 0) return 0;
        std.debug.assert(released <= entry.ref_count);
        entry.ref_count -= released;
        _ = shard.deferred_pin_releases.fetchSub(released, .acq_rel);
        _ = self.deferred_pin_releases.fetchSub(released, .acq_rel);
        entry.last_access = self.nextAccess();
        self.touchEntryLocked(shard, entry);
        return released;
    }

    fn restoreDeferredPinsLocked(self: *Cache, shard: *Shard, entry: *Entry, released: usize) void {
        entry.ref_count += released;
        self.touchEntryLocked(shard, entry);
        _ = self.deferred_pin_releases.fetchAdd(released, .release);
        _ = shard.deferred_pin_releases.fetchAdd(released, .release);
        _ = entry.deferred_ref_releases.fetchAdd(released, .release);
    }

    // Called under cache accounting only. Share the same global inspection/
    // retirement budgets with idle eviction; every physical/ledger attempt is
    // nonwaiting. Incomplete publication remains for the next finite pass.
    fn tryCollectDeferredPinsAccountingLocked(self: *Cache, target_bytes: u64, before: u64, inspections: *usize, retirements: *usize) bool {
        if (self.deferred_pin_releases.load(.acquire) == 0) return true;
        const manager = self.resource_manager orelse return false;
        for (self.shards) |*shard| {
            if (shard.deferred_pin_releases.load(.acquire) == 0) continue;
            if (inspections.* == 0 or retirements.* == reclaim_retirement_limit or before -| self.resource_accounted_bytes >= target_bytes) return true;
            if (!shard.mutex.tryLock()) continue;
            defer shard.mutex.unlock();
            while (shard.deferred_pin_releases.load(.acquire) != 0 and inspections.* != 0 and retirements.* < reclaim_retirement_limit and before -| self.resource_accounted_bytes < target_bytes) {
                const entry = nextPinCleanupEntryLocked(shard) orelse return false;
                inspections.* -= 1;
                const released = self.consumeDeferredPinsLocked(shard, entry);
                if (released == 0 or entry.ref_count != 0) continue;
                const result = manager.tryRetireUsageWithRetainedDebt(.lsm_block_table_cache, &self.resource_accounted_bytes, &self.resource_retirement_debt, @intCast(entry.byte_cost), EntryRetirement{ .cache = self, .shard = shard, .entry = entry });
                if (result != .retired) {
                    self.restoreDeferredPinsLocked(shard, entry, released);
                    return false;
                }
                retirements.* += 1;
                // Physical destruction may have left parent-owned debt. Report
                // only reconciled bytes and stop before discarding other data.
                if (self.resource_retirement_debt != 0) return false;
            }
        }
        return true;
    }

    /// Consume cache-owned pins under independent finite authority. The cursor
    /// advances across at most 64 entries per shard lock, surviving expiration
    /// and removal without retaining a request scope or an unowned entry pointer.
    pub fn drainDeferredPinsWithAdmission(self: *Cache, control: read_admission.Port) !void {
        try control.checkpoint();
        while (self.deferred_pin_releases.load(.acquire) != 0) {
            for (self.shards) |*shard| {
                if (shard.deferred_pin_releases.load(.acquire) == 0) continue;
                {
                    try @import("../../common/bounded_mutex.zig").lockAtomic(&shard.mutex, control);
                    defer shard.mutex.unlock();
                    var inspected: usize = 0;
                    while (shard.deferred_pin_releases.load(.acquire) != 0 and inspected < 64) : (inspected += 1) {
                        try control.checkpoint();
                        const current = nextPinCleanupEntryLocked(shard) orelse return error.ResourceAccountingMismatch;
                        const released = self.consumeDeferredPinsLocked(shard, current);
                        if (released == 0) continue;
                        if (current.ref_count == 0 and current.invalidated) {
                            self.removeEntryWithAdmission(shard, current, control) catch |err| {
                                // Admission failure precedes physical destruction.
                                // Restore authoritative deferred pin ownership so
                                // a later independent pass cannot miss this value.
                                self.restoreDeferredPinsLocked(shard, current, released);
                                return err;
                            };
                        }
                    }
                }
                // Parent reconciliation and fair progress never borrow the
                // shard mutex. Failed collection leaves all physical ownership
                // or already published retirement debt in the cache.
                try self.finishRetirementsWithAdmission(control);
            }
            if (self.deferred_pin_releases.load(.acquire) != 0) try control.waitForProgress();
        }
        try self.finishRetirementsWithAdmission(control);
    }

    fn release(self: *Cache, entry: *Entry) void {
        self.releasePin(entry, true);
    }
    fn releasePin(self: *Cache, entry: *Entry, run_maintenance: bool) void {
        const shard = self.shardForKey(entry.key());
        const locked = lockAtomic(&shard.mutex);

        std.debug.assert(entry.ref_count > 0);
        entry.ref_count -= 1;
        entry.last_access = self.nextAccess();
        self.touchEntryLocked(shard, entry);

        if (entry.ref_count == 0 and entry.invalidated) {
            if (run_maintenance) {
                self.removeEntryLocked(shard, entry);
            } else {
                // Failed foreground admission must not wait on the accounting
                // ledger or parent after mandatory physical pin retirement.
                const bytes = self.detachEntryLocked(shard, entry);
                self.transferRetiredResourceBytes(bytes);
            }
            if (locked) shard.mutex.unlock();
            return;
        }
        if (locked) shard.mutex.unlock();
        if (run_maintenance) self.evictToBudget();
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

    fn evictToBudgetWithAdmission(self: *Cache, control: read_admission.Port) !void {
        try self.finishRetirementsWithAdmission(control);
        try @import("../../common/bounded_mutex.zig").lockAtomic(&self.evict_mutex, control);
        defer self.evict_mutex.unlock();
        while (self.currentBytes() > self.effectiveMaxBytes()) {
            try control.checkpoint();
            if (!try self.evictOneWithAdmission(control)) break;
        }
    }

    /// Atomic, advisory signal only; ownership admission remains authoritative.
    pub fn resultPinsUnderPressure(self: *const Cache) bool {
        return self.pressure_target_bytes.load(.monotonic) != 0 or self.currentBytes() >= self.effectiveMaxBytes();
    }

    fn effectiveMaxBytes(self: *const Cache) usize {
        const pressure_target = self.pressure_target_bytes.load(.monotonic);
        if (pressure_target == 0) return self.max_bytes;
        return @min(self.max_bytes, pressure_target);
    }

    fn evictOne(self: *Cache) bool {
        return self.evictOneControlled(null) catch unreachable;
    }
    fn evictOneWithAdmission(self: *Cache, control: read_admission.Port) !bool {
        return self.evictOneControlled(control);
    }
    fn evictOneControlled(self: *Cache, control: ?read_admission.Port) !bool {
        if (control) |budget| try self.finishRetirementsWithAdmission(budget);
        const start = self.evict_cursor.fetchAdd(1, .monotonic);
        for (0..priority_count) |priority| {
            for (0..self.shards.len) |offset| {
                const shard = &self.shards[(start + offset) % self.shards.len];
                const locked = if (control) |budget| blk: {
                    try @import("../../common/bounded_mutex.zig").lockAtomic(&shard.mutex, budget);
                    break :blk true;
                } else lockAtomic(&shard.mutex);

                var owns_shard = locked;
                errdefer if (owns_shard) shard.mutex.unlock();
                var current = shard.idle_heads[priority];
                while (current) |entry| {
                    if (control) |budget| try budget.checkpoint();
                    std.debug.assert(entry.ref_count == 0);
                    if (entry.ref_count == 0) {
                        if (control) |budget| try self.removeEntryWithAdmission(shard, entry, budget) else self.removeEntryLocked(shard, entry);
                        if (locked) shard.mutex.unlock();
                        owns_shard = false;
                        // The value and ledger are already retired together.
                        // Refresh optional pressure policy outside the shard,
                        // retaining the original admission for its own waits.
                        if (control) |budget| {
                            // Physical-owner locks are released before any
                            // scoped wait to finish retained parent-ledger debt.
                            try self.finishRetirementsWithAdmission(budget);
                            try self.refreshResourcePressureWithAdmission(budget);
                        }
                        return true;
                    }
                    current = entry.idle_next;
                }
                if (locked) shard.mutex.unlock();
            }
        }
        return false;
    }

    const reclaim_inspection_limit = 64;
    const reclaim_retirement_limit = 8;
    const EntryRetirement = struct {
        cache: *Cache,
        shard: *Shard,
        entry: *Entry,
        pub fn retire(self: @This()) void {
            _ = self.cache.detachEntryLocked(self.shard, self.entry);
        }
    };

    fn tryEvictOneAccountingLocked(self: *Cache, inspections: *usize) bool {
        const start = self.evict_cursor.fetchAdd(1, .monotonic);
        for (0..priority_count) |priority| {
            for (0..self.shards.len) |offset| {
                const shard = &self.shards[(start + offset) % self.shards.len];
                if (!shard.mutex.tryLock()) continue;

                var current = shard.idle_heads[priority];
                while (current) |entry| {
                    if (inspections.* == 0) {
                        shard.mutex.unlock();
                        return false;
                    }
                    inspections.* -= 1;
                    std.debug.assert(entry.ref_count == 0);
                    if (entry.ref_count == 0) {
                        const manager = self.resource_manager orelse {
                            shard.mutex.unlock();
                            return false;
                        };
                        const result = manager.tryRetireUsageWithRetainedDebt(.lsm_block_table_cache, &self.resource_accounted_bytes, &self.resource_retirement_debt, @intCast(entry.byte_cost), EntryRetirement{ .cache = self, .shard = shard, .entry = entry });
                        shard.mutex.unlock();
                        return result == .retired;
                    }
                    current = entry.idle_next;
                }
                shard.mutex.unlock();
            }
        }
        return false;
    }

    /// Caller owns the shard and selected an unpinned entry. Before destruction
    /// validate its charge and retain debt in this cache. Later scoped failure
    /// preserves that debt even though physical detachment is already complete.
    fn removeEntryWithAdmission(self: *Cache, shard: *Shard, entry: *Entry, control: read_admission.Port) !void {
        if (self.resource_manager) |manager| {
            try @import("../../common/bounded_mutex.zig").lockAtomic(&self.resource_accounting_mutex, control);
            defer self.resource_accounting_mutex.unlock();
            const amount: u64 = @intCast(entry.byte_cost);
            if (amount > self.resource_accounted_bytes) return error.ResourceAccountingMismatch;
            try manager.retireUsageWithRetainedDebtWithAdmission(.lsm_block_table_cache, &self.resource_accounted_bytes, &self.resource_retirement_debt, amount, control, EntryRetirement{ .cache = self, .shard = shard, .entry = entry });
            // If a parent was busy, the exact charge stays in cache-owned debt.
            // evictOne releases the shard before attempting scoped completion.
        } else {
            try control.checkpoint();
            _ = self.detachEntryLocked(shard, entry);
        }
    }

    fn removeEntryLocked(self: *Cache, shard: *Shard, entry: *Entry) void {
        const byte_cost = self.detachEntryLocked(shard, entry);
        self.releaseResourceBytes(byte_cost);
    }

    fn detachEntryLocked(self: *Cache, shard: *Shard, entry: *Entry) usize {
        if (shard.pin_cleanup_cursor == entry) {
            shard.pin_cleanup_cursor = entry.lru_next;
            if (shard.pin_cleanup_cursor == null) shard.pin_cleanup_priority = (evictionPriority(entry.kind) + 1) % priority_count;
        }
        _ = shard.entries.fetchRemoveAdapted(entry.key(), KeyContext{}) orelse unreachable;
        std.debug.assert(entry.ref_count == 0);
        self.unlinkEntryLocked(shard, entry);
        unlinkPathLocked(shard, entry);
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
        return byte_cost;
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

    fn admitResourceGrowthWithAdmission(self: *Cache, bytes: usize, control: read_admission.Port) !bool {
        if (self.pending_transient_entries.load(.acquire) != 0) try self.drainTransientCleanupWithAdmission(control);
        if (self.deferred_pin_releases.load(.acquire) != 0) {
            try self.drainDeferredPinsWithAdmission(control);
        }
        try control.checkpoint();
        if (bytes == 0 or self.resource_manager == null) return true;
        while (true) {
            try @import("../../common/bounded_mutex.zig").lockAtomic(&self.resource_accounting_mutex, control);
            const manager = self.resource_manager orelse {
                self.resource_accounting_mutex.unlock();
                return true;
            };
            self.finishRetirementsAccountingLocked(control) catch |err| {
                self.resource_accounting_mutex.unlock();
                return err;
            };
            const next = std.math.add(u64, self.resource_accounted_bytes, @as(u64, @intCast(bytes))) catch {
                self.resource_accounting_mutex.unlock();
                return false;
            };
            manager.adjustUsageWithAdmission(.lsm_block_table_cache, &self.resource_accounted_bytes, next, control) catch |err| {
                self.resource_accounting_mutex.unlock();
                if (err != error.ResourceBudgetExceeded) return err;
                if (!try self.evictOneWithAdmission(control)) return false;
                continue;
            };
            self.resource_accounting_mutex.unlock();
            // Caller owns this reservation now; any later failed admission
            // must release it through the existing exactly-once cleanup path.
            return true;
        }
    }

    /// The cache outlives every outstanding retirement. A failed scope leaves
    /// its debt here for another owner-supplied scope or mandatory deinit.
    /// This method does not activate an independent completion task.
    pub fn finishRetirementsWithAdmission(self: *Cache, control: read_admission.Port) !void {
        try @import("../../common/bounded_mutex.zig").lockAtomic(&self.resource_accounting_mutex, control);
        defer self.resource_accounting_mutex.unlock();
        try self.finishRetirementsAccountingLocked(control);
    }

    fn transferRetiredResourceBytes(self: *Cache, bytes: usize) void {
        if (bytes == 0 or self.resource_manager == null) return;
        const amount: u64 = @intCast(bytes);
        // No accesses after publication: an independent cache pass may adopt
        // and reconcile this debt immediately. The manager is quiescent during
        // replacement and the cache outlives every outstanding retirement.
        _ = self.transferred_retirement_debt.fetchAdd(amount, .release);
    }

    fn adoptTransferredRetirementDebtAccountingLocked(self: *Cache) void {
        const incoming = self.transferred_retirement_debt.swap(0, .acq_rel);
        // Each transfer consumed a unique charged physical owner, so their
        // total is bounded by the observer's already admitted contribution.
        self.resource_retirement_debt = std.math.add(u64, self.resource_retirement_debt, incoming) catch unreachable;
    }

    fn finishRetirementsAccountingLocked(self: *Cache, control: read_admission.Port) !void {
        self.adoptTransferredRetirementDebtAccountingLocked();
        const debt = self.resource_retirement_debt;
        if (debt == 0) return;
        const manager = self.resource_manager orelse return error.ResourceAccountingMismatch;
        if (debt > self.resource_accounted_bytes) return error.ResourceAccountingMismatch;
        try manager.adjustUsageWithAdmission(.lsm_block_table_cache, &self.resource_accounted_bytes, self.resource_accounted_bytes - debt, control);
        // The exact ledger debit has committed; no fallible checkpoint follows.
        self.resource_retirement_debt = 0;
    }

    fn admitResourceGrowthWithCredit(self: *Cache, bytes: usize, credit: *resource_manager_mod.Reservation) bool {
        const locked = lockAtomic(&self.resource_accounting_mutex);
        const manager = self.resource_manager;
        if (manager == null or manager.? != credit.manager) {
            if (locked) self.resource_accounting_mutex.unlock();
            return self.admitResourceGrowth(bytes);
        }
        defer if (locked) self.resource_accounting_mutex.unlock();
        // Optional promotion never evicts other entries to rescue a failed
        // transfer. The caller retains construction credit until transient free.
        manager.?.adoptReservationUsage(credit, .lsm_block_table_cache, &self.resource_accounted_bytes, bytes) catch return false;
        return true;
    }

    fn admitResourceGrowthWithCreditAdmission(self: *Cache, bytes: usize, credit: *resource_manager_mod.Reservation, control: read_admission.Port) !bool {
        try @import("../../common/bounded_mutex.zig").lockAtomic(&self.resource_accounting_mutex, control);
        defer self.resource_accounting_mutex.unlock();
        const manager = self.resource_manager orelse return error.ResourceAccountingMismatch;
        if (manager != credit.manager) return error.ResourceAccountingMismatch;
        manager.adoptReservationUsageWithAdmission(credit, .lsm_block_table_cache, &self.resource_accounted_bytes, bytes, control) catch |err| return switch (err) {
            error.ResourceBudgetExceeded, error.OutOfMemory => false,
            else => err,
        };
        return true;
    }

    fn releaseResourceBytes(self: *Cache, bytes: usize) void {
        if (bytes == 0) return;
        const locked = lockAtomic(&self.resource_accounting_mutex);
        defer if (locked) self.resource_accounting_mutex.unlock();
        self.releaseResourceBytesAccountingLocked(bytes);
    }

    // Caller owns resource_accounting_mutex. Keeping the ResourceManager
    // reconciliation in one helper makes normal eviction and pin rollback
    // exactly-once accounting owners. Optional reclaim uses retained debt.
    fn releaseResourceBytesAccountingLocked(self: *Cache, bytes: usize) void {
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

    fn refreshResourcePressureWithAdmission(self: *Cache, control: read_admission.Port) !void {
        const manager = self.resource_manager orelse return;
        try @import("../../common/bounded_mutex.zig").lockAtomic(&self.resource_accounting_mutex, control);
        defer self.resource_accounting_mutex.unlock();
        const stats = try manager.sliceStatsWithAdmission(.lsm_block_table_cache, control);
        self.refreshPressureTargetFromStats(stats);
    }

    fn releaseResourceUsage(self: *Cache) void {
        const manager = self.resource_manager orelse return;
        const locked = lockAtomic(&self.resource_accounting_mutex);
        defer if (locked) self.resource_accounting_mutex.unlock();
        manager.observeUsage(.lsm_block_table_cache, &self.resource_accounted_bytes, 0);
        self.resource_retirement_debt = 0;
        self.transferred_retirement_debt.store(0, .release);
        self.resource_manager = null;
        self.pressure_target_bytes.store(0, .monotonic);
    }

    fn refreshPressureTarget(self: *Cache, manager: *resource_manager_mod.ResourceManager) void {
        const stats = manager.sliceStats(.lsm_block_table_cache);
        self.refreshPressureTargetFromStats(stats);
    }
    fn refreshPressureTargetFromStats(self: *Cache, stats: resource_manager_mod.SliceStats) void {
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
        const priority = evictionPriority(entry.kind);
        entry.lru_prev = shard.lru_tails[priority];
        entry.lru_next = null;
        if (entry.lru_prev) |prev| {
            prev.lru_next = entry;
        } else {
            shard.lru_heads[priority] = entry;
        }
        shard.lru_tails[priority] = entry;
        if (entry.ref_count == 0) self.linkIdleEntryLocked(shard, entry);
    }

    fn unlinkEntryLocked(self: *Cache, shard: *Shard, entry: *Entry) void {
        self.unlinkIdleEntryLocked(shard, entry);
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

    // The idle LRU is the unpinned subset of the ordinary access LRU. Every
    // pin transition and detach updates both lists under the same shard lock;
    // no allocation or unlocked continuation pointer is needed for reclaim.
    fn linkIdleEntryLocked(_: *Cache, shard: *Shard, entry: *Entry) void {
        std.debug.assert(entry.ref_count == 0 and !entry.idle_linked);
        const priority = evictionPriority(entry.kind);
        entry.idle_prev = shard.idle_tails[priority];
        entry.idle_next = null;
        if (entry.idle_prev) |previous| previous.idle_next = entry else shard.idle_heads[priority] = entry;
        shard.idle_tails[priority] = entry;
        entry.idle_linked = true;
    }

    fn unlinkIdleEntryLocked(_: *Cache, shard: *Shard, entry: *Entry) void {
        if (!entry.idle_linked) return;
        const priority = evictionPriority(entry.kind);
        if (entry.idle_prev) |previous| previous.idle_next = entry.idle_next else shard.idle_heads[priority] = entry.idle_next;
        if (entry.idle_next) |next| next.idle_prev = entry.idle_prev else shard.idle_tails[priority] = entry.idle_prev;
        entry.idle_prev = null;
        entry.idle_next = null;
        entry.idle_linked = false;
    }

    fn touchEntryLocked(self: *Cache, shard: *Shard, entry: *Entry) void {
        const priority = evictionPriority(entry.kind);
        if (shard.lru_tails[priority] == entry) {
            self.unlinkIdleEntryLocked(shard, entry);
            if (entry.ref_count == 0) self.linkIdleEntryLocked(shard, entry);
            return;
        }
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
    @hasDecl(std.c, "pthread_mutex_lock");

fn lockPendingWithAdmission(sync: *PendingSync, control: read_admission.Port) !void {
    while (true) {
        try control.checkpoint();
        if (sync.tryLock()) {
            errdefer sync.unlock();
            try control.checkpoint();
            return;
        }
        try control.waitForProgress();
    }
}

const PendingSync = if (supports_waitable_pending)
    struct {
        mutex: std.c.pthread_mutex_t = std.c.PTHREAD_MUTEX_INITIALIZER,

        fn tryLock(self: *@This()) bool {
            return switch (std.c.pthread_mutex_trylock(&self.mutex)) {
                .SUCCESS => true,
                .BUSY => false,
                else => unreachable,
            };
        }

        fn lock(self: *@This()) void {
            if (std.c.pthread_mutex_lock(&self.mutex) != .SUCCESS) unreachable;
        }

        fn unlock(self: *@This()) void {
            if (std.c.pthread_mutex_unlock(&self.mutex) != .SUCCESS) unreachable;
        }

        fn wait(self: *@This()) void {
            // Cache-owned transfer publishes without taking this mutex. A
            // legacy waiter therefore polls with a finite sleep, so publication
            // racing enrollment cannot strand it without another load.
            // Relative nanosleep is independent of wall-clock corrections.
            // Release the mutex across the wait; this path has no borrowed Io
            // and must neither allocate an executor nor retain a cache lock.
            self.unlock();
            platform.time.sleepNs(5 * std.time.ns_per_ms);
            self.lock();
        }

        // Polling also observes atomic cleanup transfers that cannot signal.
        fn broadcast(_: *@This()) void {}
    }
else
    struct {
        mutex: std.atomic.Mutex = .unlocked,

        fn tryLock(self: *@This()) bool {
            return self.mutex.tryLock();
        }

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

/// Owns exactly one loader registration and its independent cleanup key.
/// The cache/allocator must outlive this token and all physical loader work.
/// Retire or transfer the token before destroying its owning completion task.
pub fn qualifyOptionalLoadClaim(control: read_admission.Port) !void {
    if (!builtin.is_test) @compileError("qualification requires a test build");
    const a = std.testing.allocator;
    var parent = resource_manager_mod.ResourceManager.init(.{ .identity_allocator = a });
    defer parent.deinit(a);
    var child = resource_manager_mod.ResourceManager.init(.{ .identity_allocator = a });
    defer child.deinit(a);
    try child.installMemoryParent(parent.memoryParentPort());
    var cache = Cache.init(a, 1024);
    defer cache.deinit();
    cache.attachResourceManager(&child);
    const path = try a.dupe(u8, "optional-promotion-owned-path");
    defer a.free(path);
    const key = makeKey(path, 1, 1, .run_table_block, 8, 32);
    const shard = cache.shardForKey(key);
    // Optional acquisition cannot spend a loader/pending-lock wait.
    shard.pending_sync.lock();
    const busy = cache.tryBeginLoadClaim(path, 1, 1, .run_table_block, 8, 32, control);
    shard.pending_sync.unlock();
    try std.testing.expect((try busy) == null);
    var claim = (try cache.tryBeginLoadClaim(path, 1, 1, .run_table_block, 8, 32, control)) orelse return error.TestUnexpectedResult;
    defer claim.transferToCache();
    try std.testing.expect((try cache.tryBeginLoadClaim(path, 1, 1, .run_table_block, 8, 32, control)) == null);
    const bytes: u64 = path.len * 2;
    try std.testing.expectEqual(bytes, child.snapshot().memory.used_bytes);
    try std.testing.expectEqual(bytes, parent.snapshot().memory.used_bytes);
    // Destruction of the request's path cannot invalidate the cleanup key.
    @memset(path, 'x');
    // Cleanup must complete its ownership transfer even while both ledgers
    // and the pending lock are unavailable and the caller has stopped.
    shard.pending_sync.lock();
    std.debug.assert(child.mutex.tryLock());
    std.debug.assert(parent.mutex.tryLock());
    claim.transferToCache();
    claim.transferToCache();
    parent.mutex.unlock();
    child.mutex.unlock();
    shard.pending_sync.unlock();
    try std.testing.expect(claim.isFinished());
    var next = (try cache.tryBeginLoadClaim("optional-promotion-owned-path", 1, 1, .run_table_block, 8, 32, control)) orelse return error.TestUnexpectedResult;
    defer next.transferToCache();
    try std.testing.expectEqual(bytes, child.snapshot().memory.used_bytes);
    next.transferToCache();
    try cache.drainLoadCleanupWithAdmission(control);
    try std.testing.expectEqual(@as(usize, 0), shard.pending_loads.count());
    try std.testing.expectEqual(@as(u64, 0), child.snapshot().memory.used_bytes);
    try std.testing.expectEqual(@as(u64, 0), parent.snapshot().memory.used_bytes);
}

pub fn qualifyOptionalLoadClaimAllocation() !void {
    if (!builtin.is_test) @compileError("qualification requires a test build");
    const a = std.testing.allocator;
    for (0..2) |failed_copy| {
        var parent = resource_manager_mod.ResourceManager.init(.{ .identity_allocator = a });
        defer parent.deinit(a);
        var child = resource_manager_mod.ResourceManager.init(.{ .identity_allocator = a });
        defer child.deinit(a);
        try child.installMemoryParent(parent.memoryParentPort());
        var failing = std.testing.FailingAllocator.init(a, .{});
        var cache = Cache.init(failing.allocator(), 1024);
        defer cache.deinit();
        cache.attachResourceManager(&child);
        failing.fail_index = failing.alloc_index + failed_copy;
        const path = "optional-promotion-allocation";
        try std.testing.expectError(error.OutOfMemory, cache.tryBeginLoadClaim(path, 1, 1, .run_table_block, 8, 32, null));
        failing.fail_index = std.math.maxInt(usize);
        const shard = cache.shardForKey(makeKey(path, 1, 1, .run_table_block, 8, 32));
        try std.testing.expectEqual(@as(usize, 0), shard.pending_loads.count());
        // Both failures keep admitted key bytes owned until an independent
        // pass; the next load must recover that slot and the exact charge.
        try std.testing.expectEqual(@as(u64, path.len * 2), child.snapshot().memory.used_bytes);
        var claim = (try cache.tryBeginLoadClaim(path, 1, 1, .run_table_block, 8, 32, null)) orelse return error.TestUnexpectedResult;
        defer claim.transferToCache();
        try std.testing.expectEqual(@as(u64, path.len * 2), parent.snapshot().memory.used_bytes);
        claim.transferToCache();
        shard.pending_sync.lock();
        cache.drainLoadCleanupLocked(shard);
        shard.pending_sync.unlock();
        try std.testing.expectEqual(@as(u64, 0), parent.snapshot().memory.used_bytes);
        try std.testing.expectEqual(@as(u64, 0), child.snapshot().memory.used_bytes);
        try std.testing.expectEqual(@as(usize, 0), shard.pending_loads.count());
    }
}

pub fn qualifyPromotionCreditContention(control: read_admission.Port, stage: usize) !void {
    if (!builtin.is_test) @compileError("qualification requires a test build");
    const a = std.testing.allocator;
    var parent = resource_manager_mod.ResourceManager.init(.{ .identity_allocator = a });
    defer parent.deinit(a);
    var child = resource_manager_mod.ResourceManager.init(.{ .identity_allocator = a });
    defer child.deinit(a);
    try child.installMemoryParent(parent.memoryParentPort());
    defer {
        std.debug.assert(parent.snapshot().memory.used_bytes == 0);
        std.debug.assert(child.snapshot().memory.used_bytes == 0);
        resource_manager_mod.qualifyNoLiveReservations(&child) catch unreachable;
    }
    var cache = Cache.init(a, 4096);
    defer cache.deinit();
    cache.attachResourceManager(&child);
    const Probe = struct {
        control: read_admission.Port,
        child: *resource_manager_mod.ResourceManager,
        cache: *Cache,
        shard: *Cache.Shard,
        held: ?*std.atomic.Mutex = null,
        waits: usize = 0,
        pub fn checkpoint(self: *@This()) !void {
            try self.control.checkpoint();
        }
        pub fn waitForProgress(self: *@This()) !void {
            try std.testing.expect(self.shard.pending_sync.tryLock());
            self.shard.pending_sync.unlock();
            if (self.held != &self.child.mutex) {
                try std.testing.expect(self.child.mutex.tryLock());
                self.child.mutex.unlock();
            }
            self.waits += 1;
            try self.control.waitForProgress();
            self.unlock();
        }
        fn unlock(self: *@This()) void {
            if (self.held) |mutex| mutex.unlock();
            self.held = null;
        }
        fn hold(self: *@This(), mutex: *std.atomic.Mutex) void {
            std.debug.assert(mutex.tryLock());
            self.held = mutex;
        }
    };
    const path = "promotion-credit-contention";
    var probe = Probe{ .control = control, .child = &child, .cache = &cache, .shard = cache.shardForKey(makeKey(path, 1, 1, .run_table_block, 8, 128)) };
    defer probe.unlock();
    var adapter = read_admission.Adapter(*Probe){ .budget = &probe };
    const payload = adapter.payload();
    const port = read_admission.Port.init(&payload);
    var claim = (try cache.tryBeginLoadClaim(path, 1, 1, .run_table_block, 8, 128, port)) orelse return error.TestUnexpectedResult;
    defer claim.transferToCache();
    if (stage == 0) probe.hold(&child.mutex);
    if (stage == 1) probe.hold(&parent.mutex);
    const credit = try claim.reserveConstructionCredit(&child, 128, port);
    if (stage == 2) probe.hold(&cache.resource_accounting_mutex);
    if (stage == 3) probe.hold(&child.mutex);
    if (stage == 4) probe.hold(&parent.mutex);
    var promoted = try cache.putRunTableBlockWithCreditAdmission(path, 1, 1, 8, 128, try a.dupe(u8, &@as([128]u8, @splat('x'))), credit, port);
    defer promoted.releaseWithAdmission(port);
    try std.testing.expectEqual(@as(usize, 1), probe.waits);
    try std.testing.expectEqual(@as(u64, 0), credit.bytes);
    try std.testing.expectEqual(child.snapshot().memory.used_bytes, parent.snapshot().memory.used_bytes);
}

pub fn qualifyPromotionCreditCleanup(control: read_admission.Port, deny_retention: bool) !void {
    if (!builtin.is_test) @compileError("qualification requires a test build");
    const a = std.testing.allocator;
    var parent = resource_manager_mod.ResourceManager.init(.{ .identity_allocator = a });
    defer parent.deinit(a);
    var child = resource_manager_mod.ResourceManager.init(.{ .identity_allocator = a });
    defer child.deinit(a);
    try child.installMemoryParent(parent.memoryParentPort());
    var cache = Cache.init(a, if (deny_retention) 64 else 4096);
    defer cache.deinit();
    cache.attachResourceManager(&child);
    const path = "promotion-credit-cleanup";
    var claim = (try cache.tryBeginLoadClaim(path, 1, 1, .run_table_block, 8, 128, control)) orelse return error.TestUnexpectedResult;
    defer claim.transferToCache();
    const credit = try claim.reserveConstructionCredit(&child, 128, control);
    if (deny_retention) {
        try std.testing.expectError(error.ResourceBudgetExceeded, cache.putRunTableBlockWithCreditAdmission(path, 1, 1, 8, 128, try a.dupe(u8, &@as([128]u8, @splat('x'))), credit, control));
        try std.testing.expectEqual(@as(u64, 128), credit.bytes);
        try std.testing.expectEqual(@as(u64, 128 + path.len * 2), parent.snapshot().memory.peak_bytes);
        try std.testing.expectEqual(@as(usize, 0), cache.charged_transient_entries.load(.acquire));
    }
    const slot = claim.cleanup;
    const shard = cache.shardForKey(claim.key.?);
    shard.pending_sync.lock();
    std.debug.assert(child.mutex.tryLock());
    std.debug.assert(parent.mutex.tryLock());
    claim.transferToCache();
    parent.mutex.unlock();
    child.mutex.unlock();
    // A busy parent leaves the reservation fully charged in cache storage.
    std.debug.assert(parent.mutex.tryLock());
    cache.drainLoadCleanupLocked(shard);
    parent.mutex.unlock();
    shard.pending_sync.unlock();
    try std.testing.expect(slot.construction_credit != null);
    try std.testing.expectEqual(@as(u64, 128), slot.construction_credit.?.bytes);
    try cache.drainLoadCleanupWithAdmission(control);
    try std.testing.expect(slot.construction_credit == null);
    try std.testing.expectEqual(@as(u64, 0), parent.snapshot().memory.used_bytes);
    try std.testing.expectEqual(@as(u64, 0), child.snapshot().memory.used_bytes);
    try resource_manager_mod.qualifyNoLiveReservations(&child);
}

pub fn qualifyControlledPromotionElection(control: read_admission.Port) !void {
    if (!builtin.is_test) @compileError("qualification requires a test build");
    const a = std.testing.allocator;
    var cache = Cache.init(a, 1024 * 1024);
    defer cache.deinit();
    var physical = try cache.putRunTablePhysicalBlock("promotion-election", 1, 1, 8, 128, try a.dupe(u8, &@as([128]u8, @splat('x'))));
    defer physical.releaseWithAdmission(control);
    physical.entry.point_promotion_hits.store(0, .monotonic);
    try std.testing.expect(try physical.claimPointBlockPromotionWithAdmission(128, control));
    defer physical.finishPointBlockPromotion();
    try std.testing.expect(!(try physical.claimPointBlockPromotionWithAdmission(128, control)));
    try control.checkpoint();
}

/// Owns exactly one loader registration and its independent cleanup key.
/// The cache/allocator must outlive this token and all physical loader work.
/// Retire or transfer the token before destroying its owning completion task.
pub const LoadClaim = struct {
    cache: *Cache,
    key: ?Cache.Key,
    identity: u64,
    cleanup: *Cache.LoadCleanup,

    /// The manager outlives the cache-owned cleanup slot. Commit the credit
    /// directly to that slot before decoding; no borrowed scope is retained.
    pub fn reserveConstructionCredit(self: *LoadClaim, manager: *resource_manager_mod.ResourceManager, bytes: u64, control: ?read_admission.Port) !*resource_manager_mod.Reservation {
        if (self.key == null or self.cleanup.construction_credit != null) return error.InvalidArgument;
        self.cleanup.construction_credit = if (control) |budget| try manager.reserveWithoutReclaimWithAdmission(.lsm_read_working_set, bytes, budget) else try manager.reserveWithoutReclaim(.lsm_read_working_set, bytes);
        if (self.cleanup.construction_credit) |*credit| return credit;
        unreachable;
    }

    pub fn isFinished(self: *const LoadClaim) bool {
        return self.key == null;
    }

    fn finishLocked(self: *LoadClaim, shard: *Cache.Shard, _: Cache.Key) void {
        self.transferToCache();
        self.cache.drainLoadCleanupLocked(shard);
    }

    pub fn finishWithAdmission(self: *LoadClaim, control: read_admission.Port) !void {
        const key = self.key orelse return;
        const shard = self.cache.shardForKey(key);
        try lockPendingWithAdmission(&shard.pending_sync, control);
        defer shard.pending_sync.unlock();
        self.finishLocked(shard, key);
    }

    /// Infallible ownership transfer to the cache's preauthorized fixed slot.
    /// Call only after joining physical loader work. There is no lock, heap
    /// allocation, allocator free, scope callback or wake syscall on this path.
    /// The cache and its charged shard storage outlive the retired read root.
    pub fn transferToCache(self: *LoadClaim) void {
        if (self.key == null) return;
        self.key = null;
        // Final access: a concurrent admitted pass may now free/reuse this slot.
        self.cleanup.state.store(.pending, .release);
    }

    /// Legacy blocking acquisition followed by transfer and a nonwaiting
    /// retirement attempt. Busy accounting stays owned by the cache.
    pub fn finishBlocking(self: *LoadClaim) void {
        const key = self.key orelse return;
        const shard = self.cache.shardForKey(key);
        shard.pending_sync.lock();
        defer shard.pending_sync.unlock();
        self.finishLocked(shard, key);
    }
};

/// Movable cleanup ownership. The cache, manager and allocator must outlive
/// this token. A failed admission leaves its remaining pin or accounting debt
/// here; callers must finish it or transfer it to an already retained owner.
/// No callback or request deadline is retained by the token.
pub const PendingRelease = struct {
    handle: ?Handle,
    accounting_cache: ?*Cache = null,
    accounting_bytes: usize = 0,

    pub fn isFinished(self: *const PendingRelease) bool {
        return self.handle == null and self.accounting_bytes == 0;
    }

    pub fn finishWithAdmission(self: *PendingRelease, control: read_admission.Port) !void {
        if (self.handle) |owned| {
            if (owned.cache) |cache| {
                const shard = cache.shardForKey(owned.entry.key());
                {
                    try @import("../../common/bounded_mutex.zig").lockAtomic(&shard.mutex, control);
                    defer shard.mutex.unlock();
                    std.debug.assert(owned.entry.ref_count > 0);
                    owned.entry.ref_count -= 1;
                    owned.entry.last_access = cache.nextAccess();
                    cache.touchEntryLocked(shard, owned.entry);
                    self.handle = null;
                    if (owned.entry.ref_count == 0 and owned.entry.invalidated) {
                        // Destruction consumes value ownership. Keep the still
                        // charged bytes in this token before any fallible wait.
                        self.accounting_cache = cache;
                        self.accounting_bytes = cache.detachEntryLocked(shard, owned.entry);
                    }
                }
            } else {
                var transient = owned;
                transient.release();
                self.handle = null;
            }
        }
        if (self.accounting_bytes == 0) return;
        const cache = self.accounting_cache.?;
        if (cache.resource_manager) |manager| {
            try @import("../../common/bounded_mutex.zig").lockAtomic(&cache.resource_accounting_mutex, control);
            defer cache.resource_accounting_mutex.unlock();
            const amount: u64 = @intCast(self.accounting_bytes);
            if (amount > cache.resource_accounted_bytes) return error.ResourceAccountingMismatch;
            try manager.adjustUsageWithAdmission(.lsm_block_table_cache, &cache.resource_accounted_bytes, cache.resource_accounted_bytes - amount, control);
            // No fallible checkpoint after ledger transfer, and no optional
            // pressure refresh or eviction under mandatory cleanup ownership.
        }
        self.accounting_bytes = 0;
        self.accounting_cache = null;
    }

    /// Transfer physically retired accounting to the cache without a mutex,
    /// allocator call, scope callback or parent wait. A remaining physical pin
    /// stays in this token and returns false; the caller must still own/join it.
    pub fn transferRetiredAccountingToCache(self: *PendingRelease) bool {
        if (self.handle != null) return false;
        const cache = self.accounting_cache orelse {
            std.debug.assert(self.accounting_bytes == 0);
            return true;
        };
        const bytes = self.accounting_bytes;
        self.accounting_cache = null;
        self.accounting_bytes = 0;
        cache.transferRetiredResourceBytes(bytes);
        return true;
    }

    /// Retained pins and completed accounting transfer to the cache. Transient
    /// values still require synchronous allocator cleanup; this does not prove
    /// that destruction of every possible token is bounded.
    pub fn finishBlocking(self: *PendingRelease) void {
        if (self.handle) |owned| {
            var value = owned;
            value.releaseForAdmissionFailure();
            self.handle = null;
        }
        const transferred = self.transferRetiredAccountingToCache();
        std.debug.assert(transferred);
    }
};

pub const Handle = struct {
    cache: ?*Cache,
    allocator: Allocator,
    entry: *Cache.Entry,
    kind: Kind,

    /// Move this pin into cleanup ownership before attempting a fallible wait.
    pub fn takeReleaseOwnership(self: *Handle) PendingRelease {
        const result = PendingRelease{ .handle = self.* };
        self.* = undefined;
        return result;
    }

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

    fn releaseTransient(self: *Handle) void {
        if (self.entry.transient_ref_count.fetchSub(1, .acq_rel) != 1) return;
        if (self.entry.transient_cleanup) |slot| {
            _ = slot.cache.pending_transient_entries.fetchAdd(1, .release);
            // Final publication: collection can now destroy the physical entry.
            slot.state.store(.pending, .release);
            return;
        }
        const accounting_cache = self.entry.transient_accounting_cache;
        const bytes = self.entry.byte_cost;
        self.entry.deinit(self.allocator);
        self.allocator.destroy(self.entry);
        if (accounting_cache) |cache| {
            // Publish debt only after physical destruction. Busy ledgers leave
            // the charge cache-owned; returning never waits on accounting.
            cache.transferRetiredResourceBytes(bytes);
            _ = cache.charged_transient_entries.fetchSub(1, .release);
        }
    }

    pub fn release(self: *Handle) void {
        if (self.cache) |cache| {
            cache.release(self.entry);
        } else self.releaseTransient();
        self.* = undefined;
    }

    /// Returning a pin from an admitted read must not wait on cache or ledger
    /// locks, even when the read succeeded. The port selects ownership transfer;
    /// it is never checked or retained. A fresh admission or pressure reclaimer
    /// subsequently collects the charged pin. Admitted transients use reserved physical cleanup slots. Uncontrolled
    /// callers retain their existing release behavior.
    pub fn releaseWithAdmission(self: *Handle, control: ?read_admission.Port) void {
        if (control != null) self.releaseToCache() else self.release();
    }

    /// Transfer a retained pin without locks, allocations/frees, hashing,
    /// scope callbacks or wake syscalls. The charged cache entry remains alive
    /// until independent collection consumes this pin. Admitted transients transfer via a
    /// reserved physical cleanup slot; uncontrolled transients still require
    /// synchronous destruction.
    pub fn releaseToCache(self: *Handle) void {
        if (self.cache) |cache| {
            cache.transferPinToCache(self.entry);
        } else self.releaseTransient();
        self.* = undefined;
    }

    pub fn releaseForAdmissionFailure(self: *Handle) void {
        self.releaseToCache();
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

    /// Elect one promoter without allocating a separate hot-block directory.
    /// Transient/policy-bypassed blocks and oversized expansions stay direct.
    pub fn claimPointBlockPromotion(self: *const Handle, decoded_bytes: usize) bool {
        return self.claimPointBlockPromotionWithAdmission(decoded_bytes, null) catch unreachable;
    }

    pub fn claimPointBlockPromotionWithAdmission(self: *const Handle, decoded_bytes: usize, control: ?read_admission.Port) !bool {
        if (control) |budget| try budget.checkpoint();
        const cache = self.cache orelse return false;
        if (self.kind != .run_table_physical_block or decoded_bytes > 64 * 1024 or
            decoded_bytes +| self.entry.path.len > cache.effectiveMaxBytes() / 16) return false;
        const promoting = std.math.maxInt(u16);
        var remaining = self.entry.point_promotion_hits.load(.monotonic);
        while (remaining != promoting) {
            if (control) |budget| try budget.checkpoint();
            const next = if (remaining == 0) promoting else remaining - 1;
            if (self.entry.point_promotion_hits.cmpxchgWeak(remaining, next, .monotonic, .monotonic)) |observed| {
                remaining = observed;
            } else return remaining == 0;
        }
        return false;
    }

    pub fn finishPointBlockPromotion(self: *const Handle) void {
        std.debug.assert(self.entry.point_promotion_hits.load(.monotonic) == std.math.maxInt(u16));
        self.entry.point_promotion_hits.store(255, .monotonic);
    }

    pub fn runTablePhysicalBlock(self: *const Handle) []const u8 {
        std.debug.assert(self.kind == .run_table_physical_block);
        return self.entry.value.run_table_physical_block;
    }
};

// Entry embeds the value header; count that allocation once, plus its
// independently allocated path/payload. Saturation can never authorize a
// wrapped, deceptively small cache reservation.
pub fn estimateStateCost(path: []const u8, state: *const State) usize {
    var total = @sizeOf(Cache.Entry) +| path.len;
    total +|= clampU64ToUsize(state.estimatedMemoryBytes());
    if (state.arena_owner != null) total +|= @sizeOf(std.heap.ArenaAllocator);
    return total;
}

pub fn estimateRawTableCost(path: []const u8, raw: []const u8) usize {
    return @sizeOf(Cache.Entry) +| path.len +| raw.len;
}

pub fn estimateTableIndexCost(path: []const u8, index: *const TableIndex) usize {
    var total = @sizeOf(Cache.Entry) +| path.len +|
        (index.entry_offsets.len *| @sizeOf(u32)) +|
        (index.block_entry_offsets.len *| @sizeOf(u16)) +|
        index.filter.bytes.len;
    if (index.prefix_filter) |filter| total +|= filter.bytes.len;
    total +|= index.blocks.len *| @sizeOf(TableIndex.BlockMeta);
    for (index.blocks) |block| {
        if (block.smallest_namespace_name) |name| total +|= name.len;
        if (block.smallest_key) |key| total +|= key.len;
        if (block.largest_namespace_name) |name| total +|= name.len;
        total +|= block.largest_key.len;
        if (block.filter) |filter| total +|= filter.bytes.len;
        if (block.prefix_filter) |filter| total +|= filter.bytes.len;
        total +|= block.hash_slots.len *| @sizeOf(u32);
    }
    return total;
}

pub fn estimateTableBlockCost(path: []const u8, block: []const u8) usize {
    return estimateRawTableCost(path, block);
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
    {
        var thread = try std.testing.io.concurrent(Waiter.run, .{ &waiter, &cache });
        defer thread.await(std.testing.io);
        defer cache.finishLoad("run-1", 1, 1, .run_table_index);
        // beginLoad increments waits under pending_sync before releasing the
        // mutex in wait(). finishLoad takes that same mutex, so observing the
        // counter guarantees removal follows waiter enrollment, even if the
        // worker starts late. A scheduling delay cannot prove that ordering.
        while (cache.snapshotStats().run_table_index.waits == 0)
            platform.time.yieldBriefly();
    }

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

test "shared LSM resource reclaimer never waits for active accounting" {
    const allocator = std.testing.allocator;
    var resource_manager = resource_manager_mod.ResourceManager.init(.{});
    defer resource_manager.deinit(std.testing.allocator);
    var cache = Cache.init(allocator, 1024 * 1024);
    cache.attachResourceManager(&resource_manager);
    defer cache.deinit();

    var state: State = .{};
    try state.upsert(allocator, .{ .name = "ns" }, "key", "value", false);
    var inserted = try cache.putRunState("run-nonblocking-reclaim", 1, 1, state);
    inserted.release();
    const bytes: u64 = @intCast(cache.currentBytes());
    try std.testing.expect(bytes > 0);

    try std.testing.expect(cache.resource_accounting_mutex.tryLock());
    {
        defer cache.resource_accounting_mutex.unlock();
        try std.testing.expectEqual(@as(u64, 0), Cache.reclaimForResourceManager(&cache, bytes));
    }

    try std.testing.expectEqual(bytes, Cache.reclaimForResourceManager(&cache, bytes));
    try std.testing.expectEqual(@as(u64, 0), resource_manager.sliceStats(.lsm_block_table_cache).used_bytes);
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

test "cache read admission retains the original deadline while another loader owns capacity" {
    var cache = Cache.init(std.testing.allocator, 1024 * 1024);
    defer cache.deinit();
    const Budget = struct {
        cache: *Cache,
        mode: enum { release, expire, cancel },
        now_ns: u64 = 7 * std.time.ns_per_s,
        deadline_ns: u64 = 8 * std.time.ns_per_s,
        canceled: bool = false,
        waits: usize = 0,
        pub fn checkpoint(self: *@This()) !void {
            if (self.canceled) return error.Canceled;
            if (self.now_ns >= self.deadline_ns) return error.CatalogRoutingSnapshotTimeout;
        }
        pub fn waitForProgress(self: *@This()) !void {
            self.waits += 1;
            switch (self.mode) {
                .release => self.cache.finishLoad("held-index", 9, 7, .run_table_index),
                .expire => self.now_ns = self.deadline_ns,
                .cancel => self.canceled = true,
            }
        }
    };
    inline for (.{ .release, .expire, .cancel }) |mode| {
        try cache.beginLoad("held-index", 9, 7, .run_table_index);
        var budget = Budget{ .cache = &cache, .mode = mode };
        var adapter = read_admission.Adapter(*Budget){ .budget = &budget };
        const payload = adapter.payload();
        const result = cache.beginLoadWithAdmission("held-index", 9, 7, .run_table_index, read_admission.Port.init(&payload));
        if (mode == .release) try result else try std.testing.expectError(if (mode == .expire) error.CatalogRoutingSnapshotTimeout else error.Canceled, result);
        try std.testing.expectEqual(@as(usize, 1), budget.waits);
        try std.testing.expectEqual(@as(usize, 1), cache.pendingLoadCountForTests());
        cache.finishLoad("held-index", 9, 7, .run_table_index);
        try std.testing.expectEqual(@as(usize, 0), cache.pendingLoadCountForTests());
    }
}

test "cache read admission uses original progress to acquire the pending mutex" {
    var cache = Cache.init(std.testing.allocator, 1024 * 1024);
    defer cache.deinit();
    const shard = cache.shardForKey(makeKey("pending-mutex", 1, 1, .run_table_index, 0, 0));
    const Budget = struct {
        sync: *PendingSync,
        waits: usize = 0,
        pub fn checkpoint(_: *@This()) !void {}
        pub fn waitForProgress(self: *@This()) !void {
            self.waits += 1;
            self.sync.unlock();
        }
    };
    var budget = Budget{ .sync = &shard.pending_sync };
    var adapter = read_admission.Adapter(*Budget){ .budget = &budget };
    const payload = adapter.payload();
    shard.pending_sync.lock();
    try cache.beginLoadWithAdmission("pending-mutex", 1, 1, .run_table_index, read_admission.Port.init(&payload));
    defer cache.finishLoad("pending-mutex", 1, 1, .run_table_index);
    try std.testing.expectEqual(@as(usize, 1), budget.waits);
    try std.testing.expect(shard.pending_sync.tryLock());
    shard.pending_sync.unlock();
}

test "cache read admission releases a newly acquired pending mutex when canceled" {
    var cache = Cache.init(std.testing.allocator, 1024 * 1024);
    defer cache.deinit();
    const Budget = struct {
        checks: usize = 0,
        pub fn checkpoint(self: *@This()) !void {
            self.checks += 1;
            if (self.checks == 2) return error.Canceled;
        }
        pub fn waitForProgress(_: *@This()) !void {
            return error.TestUnexpectedResult;
        }
    };
    var budget = Budget{};
    var adapter = read_admission.Adapter(*Budget){ .budget = &budget };
    const payload = adapter.payload();
    try std.testing.expectError(error.Canceled, cache.beginLoadWithAdmission("racing-cancel", 1, 1, .run_table_index, read_admission.Port.init(&payload)));
    try std.testing.expectEqual(@as(usize, 0), cache.pendingLoadCountForTests());
}

test "cache read admission never releases another lookup owner's shard on cancellation" {
    var cache = Cache.init(std.testing.allocator, 1024 * 1024);
    defer cache.deinit();
    var installed = try cache.putRunTableRaw("held-lookup", 3, 4, try std.testing.allocator.dupe(u8, "raw"));
    defer installed.release();
    const shard = cache.shardForKey(makeKey("held-lookup", 3, 4, .run_table_raw, 0, 0));
    const Budget = struct {
        canceled: bool = false,
        waits: usize = 0,
        pub fn checkpoint(self: *@This()) !void {
            if (self.canceled) return error.Canceled;
        }
        pub fn waitForProgress(self: *@This()) !void {
            self.waits += 1;
            self.canceled = true;
        }
    };
    var budget = Budget{};
    var adapter = read_admission.Adapter(*Budget){ .budget = &budget };
    const payload = adapter.payload();
    try std.testing.expect(shard.mutex.tryLock());
    defer shard.mutex.unlock();
    try std.testing.expectError(error.Canceled, cache.retainRunTableRawWithAdmission("held-lookup", 3, 4, read_admission.Port.init(&payload)));
    try std.testing.expectEqual(@as(usize, 1), budget.waits);
    try std.testing.expect(!shard.mutex.tryLock());
    try std.testing.expectEqual(@as(usize, 1), installed.entry.ref_count);
}

test "cache read admission resumes cache lookup through the original progress callback" {
    var cache = Cache.init(std.testing.allocator, 1024 * 1024);
    defer cache.deinit();
    var installed = try cache.putRunTableRaw("resumed-lookup", 3, 4, try std.testing.allocator.dupe(u8, "raw"));
    defer installed.release();
    const shard = cache.shardForKey(makeKey("resumed-lookup", 3, 4, .run_table_raw, 0, 0));
    const Budget = struct {
        mutex: *std.atomic.Mutex,
        waits: usize = 0,
        pub fn checkpoint(_: *@This()) !void {}
        pub fn waitForProgress(self: *@This()) !void {
            self.waits += 1;
            self.mutex.unlock();
        }
    };
    var budget = Budget{ .mutex = &shard.mutex };
    var adapter = read_admission.Adapter(*Budget){ .budget = &budget };
    const payload = adapter.payload();
    try std.testing.expect(shard.mutex.tryLock());
    var retained = (try cache.retainRunTableRawWithAdmission("resumed-lookup", 3, 4, read_admission.Port.init(&payload))).?;
    try std.testing.expectEqual(@as(usize, 1), budget.waits);
    try std.testing.expectEqualStrings("raw", retained.runTableRaw());
    try std.testing.expectEqual(@as(usize, 2), installed.entry.ref_count);
    retained.release();
    try std.testing.expectEqual(@as(usize, 1), installed.entry.ref_count);
}

test "cache read admission key allocation failure never publishes a partial pending entry" {
    const Fixture = struct {
        pub fn checkpoint(_: *@This()) !void {}
        pub fn waitForProgress(_: *@This()) !void {
            return error.TestUnexpectedResult;
        }
        fn run(allocator: Allocator, controlled: bool) !void {
            var cache = try Cache.initFallible(allocator, 1024 * 1024);
            defer cache.deinit();
            var budget = @This(){};
            var adapter = read_admission.Adapter(*@This()){ .budget = &budget };
            const payload = adapter.payload();
            const result = if (controlled) cache.beginLoadWithAdmission("allocation-failure", 1, 1, .run_table_index, read_admission.Port.init(&payload)) else cache.beginLoad("allocation-failure", 1, 1, .run_table_index);
            result catch |err| {
                try std.testing.expectEqual(@as(usize, 0), cache.pendingLoadCountForTests());
                return err;
            };
            try std.testing.expectEqual(@as(usize, 1), cache.pendingLoadCountForTests());
            cache.finishLoad("allocation-failure", 1, 1, .run_table_index);
            try std.testing.expectEqual(@as(usize, 0), cache.pendingLoadCountForTests());
        }
    };
    inline for (.{ false, true }) |controlled| try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{controlled});
}

test "cache read admission rejects publication without releasing a held resource ledger" {
    var manager = resource_manager_mod.ResourceManager.init(.{});
    defer manager.deinit(std.testing.allocator);
    var cache = Cache.init(std.testing.allocator, 1024 * 1024);
    cache.attachResourceManager(&manager);
    defer cache.deinit();
    const Budget = struct {
        canceled: bool = false,
        waits: usize = 0,
        pub fn checkpoint(self: *@This()) !void {
            if (self.canceled) return error.Canceled;
        }
        pub fn waitForProgress(self: *@This()) !void {
            self.waits += 1;
            self.canceled = true;
        }
    };
    var budget = Budget{};
    var adapter = read_admission.Adapter(*Budget){ .budget = &budget };
    const payload = adapter.payload();
    try std.testing.expect(cache.resource_accounting_mutex.tryLock());
    const result = cache.putRunTableBlockWithAdmission("held-ledger", 1, 1, 9, 3, try std.testing.allocator.dupe(u8, "raw"), read_admission.Port.init(&payload));
    try std.testing.expect(!cache.resource_accounting_mutex.tryLock());
    cache.resource_accounting_mutex.unlock();
    try std.testing.expectError(error.Canceled, result);
    try std.testing.expectEqual(@as(usize, 1), budget.waits);
    try std.testing.expectEqual(@as(usize, 0), cache.entryCount());
    try std.testing.expectEqual(@as(u64, 0), cache.resource_accounted_bytes);
    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.lsm_block_table_cache).used_bytes);
}

test "cache read admission retains shared manager mutex ownership on publication cancellation" {
    var manager = resource_manager_mod.ResourceManager.init(.{});
    defer manager.deinit(std.testing.allocator);
    var cache = Cache.init(std.testing.allocator, 1024 * 1024);
    cache.attachResourceManager(&manager);
    defer cache.deinit();
    const Budget = struct {
        canceled: bool = false,
        waits: usize = 0,
        pub fn checkpoint(self: *@This()) !void {
            if (self.canceled) return error.GenerationPublicationChanged;
        }
        pub fn waitForProgress(self: *@This()) !void {
            self.waits += 1;
            self.canceled = true;
        }
    };
    var budget = Budget{};
    var adapter = read_admission.Adapter(*Budget){ .budget = &budget };
    const payload = adapter.payload();
    try std.testing.expect(manager.mutex.tryLock());
    const result = cache.putRunTableBlockWithAdmission("held-manager", 1, 1, 9, 3, try std.testing.allocator.dupe(u8, "raw"), read_admission.Port.init(&payload));
    try std.testing.expect(!manager.mutex.tryLock());
    manager.mutex.unlock();
    try std.testing.expectError(error.GenerationPublicationChanged, result);
    try std.testing.expectEqual(@as(usize, 1), budget.waits);
    try std.testing.expect(cache.resource_accounting_mutex.tryLock());
    cache.resource_accounting_mutex.unlock();
    try std.testing.expectEqual(@as(usize, 0), cache.entryCount());
    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.lsm_block_table_cache).used_bytes);
}

test "cache read admission rolls back admitted bytes when destination shard admission fails" {
    var manager = resource_manager_mod.ResourceManager.init(.{});
    defer manager.deinit(std.testing.allocator);
    var cache = Cache.init(std.testing.allocator, 1024 * 1024);
    cache.attachResourceManager(&manager);
    defer cache.deinit();
    const shard = cache.shardForKey(makeKey("held-publication", 1, 1, .run_table_block, 9, 3));
    const Budget = struct {
        canceled: bool = false,
        pub fn checkpoint(self: *@This()) !void {
            if (self.canceled) return error.Canceled;
        }
        pub fn waitForProgress(self: *@This()) !void {
            self.canceled = true;
        }
    };
    var budget = Budget{};
    var adapter = read_admission.Adapter(*Budget){ .budget = &budget };
    const payload = adapter.payload();
    try std.testing.expect(shard.mutex.tryLock());
    const result = cache.putRunTableBlockWithAdmission("held-publication", 1, 1, 9, 3, try std.testing.allocator.dupe(u8, "raw"), read_admission.Port.init(&payload));
    try std.testing.expect(!shard.mutex.tryLock());
    shard.mutex.unlock();
    try std.testing.expectError(error.Canceled, result);
    try std.testing.expectEqual(cache.transferred_retirement_debt.load(.acquire), cache.resource_accounted_bytes);
    try drainDeferredPinsForTest(&cache);
    try std.testing.expectEqual(@as(u64, 0), cache.resource_accounted_bytes);
    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.lsm_block_table_cache).used_bytes);
    try std.testing.expectEqual(@as(usize, 0), cache.currentBytes());
    try std.testing.expectEqual(@as(usize, 0), cache.entryCount());
}

test "cache read admission preserves shared ledger accounting after original progress resumes" {
    var manager = resource_manager_mod.ResourceManager.init(.{});
    defer manager.deinit(std.testing.allocator);
    var cache = Cache.init(std.testing.allocator, 1024 * 1024);
    cache.attachResourceManager(&manager);
    defer cache.deinit();
    const Budget = struct {
        mutex: *std.atomic.Mutex,
        waits: usize = 0,
        pub fn checkpoint(_: *@This()) !void {}
        pub fn waitForProgress(self: *@This()) !void {
            self.waits += 1;
            self.mutex.unlock();
        }
    };
    var budget = Budget{ .mutex = &manager.mutex };
    var adapter = read_admission.Adapter(*Budget){ .budget = &budget };
    const payload = adapter.payload();
    try std.testing.expect(manager.mutex.tryLock());
    var published = try cache.putRunTableBlockWithAdmission("resumed-publication", 1, 1, 9, 3, try std.testing.allocator.dupe(u8, "raw"), read_admission.Port.init(&payload));
    defer published.release();
    try std.testing.expectEqual(@as(usize, 1), budget.waits);
    try std.testing.expectEqualStrings("raw", published.runTableBlock());
    try std.testing.expectEqual(@as(u64, @intCast(cache.currentBytes())), manager.sliceStats(.lsm_block_table_cache).used_bytes);
}

test "cache read admission preserves reclaimer registry ownership when the original scope expires" {
    var manager = resource_manager_mod.ResourceManager.init(.{ .memory_budget = .{ .soft_limit_bytes = 1, .hard_limit_bytes = 1 } });
    defer manager.deinit(std.testing.allocator);
    var cache = Cache.init(std.testing.allocator, 1024 * 1024);
    cache.attachResourceManager(&manager);
    defer cache.deinit();
    const Budget = struct {
        expired: bool = false,
        waits: usize = 0,
        pub fn checkpoint(self: *@This()) !void {
            if (self.expired) return error.CatalogRoutingSnapshotTimeout;
        }
        pub fn waitForProgress(self: *@This()) !void {
            self.waits += 1;
            self.expired = true;
        }
    };
    var budget = Budget{};
    var adapter = read_admission.Adapter(*Budget){ .budget = &budget };
    const payload = adapter.payload();
    try std.testing.expect(manager.reclaimer_mutex.tryLock());
    const result = cache.putRunTableBlockWithAdmission("held-registry", 1, 1, 9, 3, try std.testing.allocator.dupe(u8, "raw"), read_admission.Port.init(&payload));
    try std.testing.expect(!manager.reclaimer_mutex.tryLock());
    manager.reclaimer_mutex.unlock();
    try std.testing.expectError(error.CatalogRoutingSnapshotTimeout, result);
    try std.testing.expectEqual(@as(usize, 1), budget.waits);
    try std.testing.expectEqual(@as(u64, 0), cache.resource_accounted_bytes);
    try std.testing.expectEqual(@as(usize, 0), cache.entryCount());
}

test "cache read admission publication checkpoint failures keep exactly one value and ledger owner" {
    const Budget = struct {
        checks: usize = 0,
        fail_at: usize,
        pub fn checkpoint(self: *@This()) !void {
            self.checks += 1;
            if (self.checks == self.fail_at) return error.Canceled;
        }
        pub fn waitForProgress(_: *@This()) !void {
            return error.TestUnexpectedResult;
        }
    };
    var completed = false;
    for (1..128) |fail_at| {
        var manager = resource_manager_mod.ResourceManager.init(.{});
        defer manager.deinit(std.testing.allocator);
        var cache = Cache.init(std.testing.allocator, 1024 * 1024);
        cache.attachResourceManager(&manager);
        defer cache.deinit();
        var budget = Budget{ .fail_at = fail_at };
        var adapter = read_admission.Adapter(*Budget){ .budget = &budget };
        const payload = adapter.payload();
        var published = cache.putRunTableBlockWithAdmission("checkpoint-publication", 1, 1, 9, 3, try std.testing.allocator.dupe(u8, "raw"), read_admission.Port.init(&payload)) catch |err| {
            try std.testing.expectEqual(error.Canceled, err);
            try std.testing.expectEqual(@as(u64, @intCast(cache.currentBytes())) + cache.transferred_retirement_debt.load(.acquire), cache.resource_accounted_bytes);
            try std.testing.expectEqual(cache.resource_accounted_bytes, manager.sliceStats(.lsm_block_table_cache).used_bytes);
            const failed_checks = budget.checks;
            try drainDeferredPinsForTest(&cache);
            try std.testing.expectEqual(failed_checks, budget.checks);
            if (cache.entryCount() == 0) {
                try std.testing.expectEqual(@as(u64, 0), cache.resource_accounted_bytes);
            } else {
                try std.testing.expectEqual(@as(usize, 1), cache.entryCount());
                try std.testing.expectEqual(@as(usize, 0), cache.deferred_pin_releases.load(.acquire));
                var retained = cache.retainRunTableBlock("checkpoint-publication", 1, 1, 9, 3).?;
                try std.testing.expectEqual(@as(usize, 1), retained.entry.ref_count);
                try std.testing.expectEqualStrings("raw", retained.runTableBlock());
                retained.releaseForAdmissionFailure();
            }
            continue;
        };
        published.release();
        completed = true;
        break;
    }
    try std.testing.expect(completed);
}

test "cache read admission publication allocation failures leave no value or ledger owner" {
    const Fixture = struct {
        pub fn checkpoint(_: *@This()) !void {}
        pub fn waitForProgress(_: *@This()) !void {
            return error.TestUnexpectedResult;
        }
        fn run(allocator: Allocator) !void {
            var manager = resource_manager_mod.ResourceManager.init(.{ .identity_allocator = allocator });
            defer manager.deinit(allocator);
            var cache = try Cache.initFallible(allocator, 1024 * 1024);
            // Use the real ledger without optional reclaimer registration so
            // every injected failure belongs to mandatory publication work.
            cache.resource_manager = &manager;
            defer cache.deinit();
            var budget = @This(){};
            var adapter = read_admission.Adapter(*@This()){ .budget = &budget };
            const payload = adapter.payload();
            var published = cache.putRunTableBlockWithAdmission("allocation-publication", 1, 1, 9, 3, try allocator.dupe(u8, "raw"), read_admission.Port.init(&payload)) catch |err| {
                try std.testing.expectEqual(@as(usize, 0), cache.entryCount());
                try std.testing.expectEqual(@as(usize, 0), cache.currentBytes());
                try std.testing.expectEqual(cache.transferred_retirement_debt.load(.acquire), cache.resource_accounted_bytes);
                try drainDeferredPinsForTest(&cache);
                try std.testing.expectEqual(@as(u64, 0), cache.resource_accounted_bytes);
                try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.lsm_block_table_cache).used_bytes);
                return err;
            };
            defer published.release();
            try std.testing.expectEqualStrings("raw", published.runTableBlock());
            try std.testing.expectEqual(@as(u64, @intCast(cache.currentBytes())), manager.sliceStats(.lsm_block_table_cache).used_bytes);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "cache read admission post-publication waits preserve mutex value and ledger ownership" {
    inline for (.{ .accounting, .manager, .eviction }) |phase| {
        inline for (.{ .cancel, .release }) |mode| {
            var manager = resource_manager_mod.ResourceManager.init(.{});
            defer manager.deinit(std.testing.allocator);
            var cache = Cache.init(std.testing.allocator, 1024 * 1024);
            cache.attachResourceManager(&manager);
            defer cache.deinit();
            const Budget = struct {
                cache: *Cache,
                mutex: *std.atomic.Mutex,
                held: bool = false,
                armed: bool = false,
                expired: bool = false,
                waits: usize = 0,
                release: bool,
                pub fn checkpoint(self: *@This()) !void {
                    if (self.expired) return error.CatalogRoutingSnapshotTimeout;
                    if (!self.armed and self.cache.entryCount() != 0) {
                        if (!self.mutex.tryLock()) return error.TestUnexpectedResult;
                        self.held = true;
                        self.armed = true;
                    }
                }
                pub fn waitForProgress(self: *@This()) !void {
                    self.waits += 1;
                    if (self.release) {
                        self.mutex.unlock();
                        self.held = false;
                    } else self.expired = true;
                }
            };
            var budget = Budget{ .cache = &cache, .mutex = switch (phase) {
                .accounting => &cache.resource_accounting_mutex,
                .manager => &manager.mutex,
                .eviction => &cache.evict_mutex,
                else => unreachable,
            }, .release = mode == .release };
            defer if (budget.held) budget.mutex.unlock();
            var adapter = read_admission.Adapter(*Budget){ .budget = &budget };
            const payload = adapter.payload();
            const result = cache.putRunTableBlockWithAdmission("post-publication", 1, 1, 9, 3, try std.testing.allocator.dupe(u8, "raw"), read_admission.Port.init(&payload));
            if (mode == .cancel) {
                try std.testing.expectError(error.CatalogRoutingSnapshotTimeout, result);
                try std.testing.expect(!budget.mutex.tryLock());
                budget.mutex.unlock();
                budget.held = false;
                try std.testing.expect(cache.deferred_pin_releases.load(.acquire) != 0);
                try drainDeferredPinsForTest(&cache);
                var retained = cache.retainRunTableBlock("post-publication", 1, 1, 9, 3).?;
                try std.testing.expectEqual(@as(usize, 1), retained.entry.ref_count);
                try std.testing.expectEqualStrings("raw", retained.runTableBlock());
                retained.release();
            } else {
                var owned = try result;
                try std.testing.expectEqualStrings("raw", owned.runTableBlock());
                owned.release();
            }
            try std.testing.expectEqual(@as(usize, 1), budget.waits);
            try std.testing.expectEqual(@as(usize, 1), cache.entryCount());
            try std.testing.expectEqual(@as(u64, @intCast(cache.currentBytes())), cache.resource_accounted_bytes);
            try std.testing.expectEqual(cache.resource_accounted_bytes, manager.sliceStats(.lsm_block_table_cache).used_bytes);
        }
    }
}

test "cache read admission failed duplicate publication releases only its acquired pin" {
    var manager = resource_manager_mod.ResourceManager.init(.{});
    defer manager.deinit(std.testing.allocator);
    var cache = Cache.init(std.testing.allocator, 1024 * 1024);
    cache.attachResourceManager(&manager);
    defer cache.deinit();
    var installed = try cache.putRunTableBlock("duplicate-publication", 1, 1, 9, 3, try std.testing.allocator.dupe(u8, "old"));
    defer installed.release();
    const before = cache.resource_accounted_bytes;
    const Budget = struct {
        entry: *Cache.Entry,
        pub fn checkpoint(self: *@This()) !void {
            if (self.entry.ref_count > 1) return error.GenerationPublicationChanged;
        }
        pub fn waitForProgress(_: *@This()) !void {
            return error.TestUnexpectedResult;
        }
    };
    var budget = Budget{ .entry = installed.entry };
    var adapter = read_admission.Adapter(*Budget){ .budget = &budget };
    const payload = adapter.payload();
    try std.testing.expectError(error.GenerationPublicationChanged, cache.putRunTableBlockWithAdmission("duplicate-publication", 1, 1, 9, 3, try std.testing.allocator.dupe(u8, "new"), read_admission.Port.init(&payload)));
    try drainDeferredPinsForTest(&cache);
    try std.testing.expectEqual(@as(usize, 1), installed.entry.ref_count);
    try std.testing.expectEqualStrings("old", installed.runTableBlock());
    try std.testing.expectEqual(before, cache.resource_accounted_bytes);
    try std.testing.expectEqual(before, manager.sliceStats(.lsm_block_table_cache).used_bytes);
}

test "cache read admission canceled transient publication frees the consumed value once" {
    var cache = Cache.init(std.testing.allocator, 1024 * 1024);
    defer cache.deinit();
    const Budget = struct {
        cache: *Cache,
        pub fn checkpoint(self: *@This()) !void {
            if (self.cache.snapshotStats().run_table_block.transient_serves != 0) return error.Canceled;
        }
        pub fn waitForProgress(_: *@This()) !void {
            return error.TestUnexpectedResult;
        }
    };
    var budget = Budget{ .cache = &cache };
    var adapter = read_admission.Adapter(*Budget){ .budget = &budget };
    const payload = adapter.payload();
    try std.testing.expectError(error.Canceled, cache.putTransientRunTableBlockWithAdmission("transient-publication", 1, 1, 9, 3, try std.testing.allocator.dupe(u8, "raw"), read_admission.Port.init(&payload)));
    try std.testing.expectEqual(@as(usize, 0), cache.entryCount());
    try std.testing.expectEqual(@as(usize, 0), cache.currentBytes());
}

test "cache read admission canceled index publication retains its owned decoded index" {
    var cache = Cache.init(std.testing.allocator, 1024 * 1024);
    defer cache.deinit();
    const entries = [_]lsm_table_file.Entry{.{ .namespace_name = "docs", .key = "doc:a", .value = "raw" }};
    const encoded = try lsm_table_file.encodeAlloc(std.testing.allocator, &entries);
    defer std.testing.allocator.free(encoded);
    const index = try lsm_table_file.decodeIndexAlloc(std.testing.allocator, encoded);
    const Budget = struct {
        cache: *Cache,
        pub fn checkpoint(self: *@This()) !void {
            if (self.cache.entryCount() != 0) return error.Canceled;
        }
        pub fn waitForProgress(_: *@This()) !void {
            return error.TestUnexpectedResult;
        }
    };
    var budget = Budget{ .cache = &cache };
    var adapter = read_admission.Adapter(*Budget){ .budget = &budget };
    const payload = adapter.payload();
    try std.testing.expectError(error.Canceled, cache.putRunTableIndexWithAdmission("index-publication", 1, 1, index, read_admission.Port.init(&payload)));
    try drainDeferredPinsForTest(&cache);
    var retained = cache.retainRunTableIndex("index-publication", 1, 1).?;
    defer retained.release();
    try std.testing.expectEqual(@as(usize, 1), retained.entry.ref_count);
    try std.testing.expectEqual(@as(usize, 1), retained.runTableIndex().entryCount());
}

test "cache read admission canceled invalidated publication retires value and ledger ownership" {
    var manager = resource_manager_mod.ResourceManager.init(.{});
    defer manager.deinit(std.testing.allocator);
    var cache = Cache.init(std.testing.allocator, 1024 * 1024);
    cache.attachResourceManager(&manager);
    defer cache.deinit();
    const Budget = struct {
        cache: *Cache,
        invalidated: bool = false,
        checks: usize = 0,
        pub fn checkpoint(self: *@This()) !void {
            self.checks += 1;
            if (self.invalidated) return error.Canceled;
            if (self.cache.entryCount() != 0) {
                self.cache.invalidatePath("invalidated-publication");
                self.invalidated = true;
                return error.Canceled;
            }
        }
        pub fn waitForProgress(_: *@This()) !void {
            return error.TestUnexpectedResult;
        }
    };
    var budget = Budget{ .cache = &cache };
    var adapter = read_admission.Adapter(*Budget){ .budget = &budget };
    const payload = adapter.payload();
    try std.testing.expectError(error.Canceled, cache.putRunTableBlockWithAdmission("invalidated-publication", 1, 1, 9, 3, try std.testing.allocator.dupe(u8, "raw"), read_admission.Port.init(&payload)));
    try std.testing.expect(budget.invalidated);
    try std.testing.expectEqual(@as(usize, 1), cache.entryCount());
    try std.testing.expectEqual(@as(usize, 1), cache.deferred_pin_releases.load(.acquire));
    const retained = cache.resource_accounted_bytes;
    try std.testing.expect(retained > 0);
    try std.testing.expectEqual(retained, manager.sliceStats(.lsm_block_table_cache).used_bytes);
    try std.testing.expectError(error.Canceled, cache.drainDeferredPinsWithAdmission(read_admission.Port.init(&payload)));
    try std.testing.expectEqual(retained, cache.resource_accounted_bytes);
    const original_checks = budget.checks;
    try drainDeferredPinsForTest(&cache);
    try std.testing.expectEqual(original_checks, budget.checks);
    try std.testing.expectEqual(@as(usize, 0), cache.entryCount());
    try std.testing.expectEqual(@as(usize, 0), cache.currentBytes());
    try std.testing.expectEqual(@as(usize, 0), cache.deferred_pin_releases.load(.acquire));
    try std.testing.expectEqual(@as(u64, 0), cache.resource_accounted_bytes);
    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.lsm_block_table_cache).used_bytes);
    try std.testing.expect(cache.retainRunTableBlock("invalidated-publication", 1, 1, 9, 3) == null);
}

test "cache read admission cleanup ownership survives canceled shard accounting and manager waits" {
    inline for (.{ .shard, .accounting, .manager }) |phase| {
        var manager = resource_manager_mod.ResourceManager.init(.{});
        defer manager.deinit(std.testing.allocator);
        var cache = Cache.init(std.testing.allocator, 1024 * 1024);
        cache.attachResourceManager(&manager);
        defer cache.deinit();
        var unrelated = try cache.putRunTableBlock("unrelated-cleanup", 2, 1, 0, 3, try std.testing.allocator.dupe(u8, "old"));
        defer unrelated.release();
        const unrelated_cost = unrelated.entry.byte_cost;
        var handle = try cache.putRunTableBlock("pending-cleanup", 1, 1, 0, 3, try std.testing.allocator.dupe(u8, "raw"));
        const original_bytes = cache.currentBytes();
        const shard = cache.shardForKey(handle.entry.key());
        cache.invalidatePath("pending-cleanup");
        var cleanup = handle.takeReleaseOwnership();
        defer cleanup.finishBlocking();
        const held = switch (phase) {
            .shard => &shard.mutex,
            .accounting => &cache.resource_accounting_mutex,
            .manager => &manager.mutex,
            else => unreachable,
        };
        const Budget = struct {
            expired: bool = false,
            waits: usize = 0,
            pub fn checkpoint(self: *@This()) !void {
                if (self.expired) return error.CatalogRoutingSnapshotTimeout;
            }
            pub fn waitForProgress(self: *@This()) !void {
                self.waits += 1;
                self.expired = true;
            }
        };
        var request = Budget{};
        var request_adapter = read_admission.Adapter(*Budget){ .budget = &request };
        const request_payload = request_adapter.payload();
        try std.testing.expect(held.tryLock());
        const result = cleanup.finishWithAdmission(read_admission.Port.init(&request_payload));
        try std.testing.expect(!held.tryLock());
        held.unlock();
        try std.testing.expectError(error.CatalogRoutingSnapshotTimeout, result);
        try std.testing.expectEqual(@as(usize, 1), request.waits);
        try std.testing.expect(!cleanup.isFinished());
        try std.testing.expectEqual(@as(u64, @intCast(original_bytes)), cache.resource_accounted_bytes);
        try std.testing.expectEqual(@as(u64, @intCast(original_bytes)), manager.sliceStats(.lsm_block_table_cache).used_bytes);
        if (phase == .shard) {
            try std.testing.expectEqual(@as(usize, 2), cache.entryCount());
            try std.testing.expect(cleanup.handle != null);
            try std.testing.expectEqual(@as(usize, 1), cleanup.handle.?.entry.ref_count);
        } else {
            try std.testing.expectEqual(@as(usize, 1), cache.entryCount());
            try std.testing.expect(cleanup.handle == null);
            try std.testing.expectEqual(original_bytes - unrelated_cost, cleanup.accounting_bytes);
        }
        // A different owner supplies its own authority. The token retained no
        // borrow of the failed foreground request and spends no new pin.
        var completion = Budget{};
        var completion_adapter = read_admission.Adapter(*Budget){ .budget = &completion };
        const completion_payload = completion_adapter.payload();
        try cleanup.finishWithAdmission(read_admission.Port.init(&completion_payload));
        try std.testing.expect(cleanup.isFinished());
        try std.testing.expectEqual(@as(usize, 0), completion.waits);
        try std.testing.expectEqual(@as(usize, 1), cache.entryCount());
        try std.testing.expectEqual(unrelated_cost, cache.currentBytes());
        try std.testing.expectEqual(@as(u64, @intCast(unrelated_cost)), cache.resource_accounted_bytes);
        try std.testing.expectEqual(@as(u64, @intCast(unrelated_cost)), manager.sliceStats(.lsm_block_table_cache).used_bytes);
        try std.testing.expectEqualStrings("old", unrelated.runTableBlock());
        // Completed cleanup cannot debit an unrelated owner's bytes on retry.
        try cleanup.finishWithAdmission(read_admission.Port.init(&request_payload));
        cleanup.finishBlocking();
        try std.testing.expectEqual(@as(u64, @intCast(unrelated_cost)), cache.resource_accounted_bytes);
    }
}

test "cache read admission cleanup ownership releases one duplicate pin without eviction" {
    var cache = Cache.init(std.testing.allocator, 1024 * 1024);
    defer cache.deinit();
    var original = try cache.putRunTableBlock("duplicate-cleanup", 1, 1, 0, 3, try std.testing.allocator.dupe(u8, "raw"));
    defer original.release();
    var duplicate = original.retain();
    var cleanup = duplicate.takeReleaseOwnership();
    defer cleanup.finishBlocking();
    const Budget = struct {
        pub fn checkpoint(_: *@This()) !void {}
        pub fn waitForProgress(_: *@This()) !void {
            return error.TestUnexpectedResult;
        }
    };
    var budget = Budget{};
    var adapter = read_admission.Adapter(*Budget){ .budget = &budget };
    const payload = adapter.payload();
    try std.testing.expect(cache.evict_mutex.tryLock());
    const result = cleanup.finishWithAdmission(read_admission.Port.init(&payload));
    try std.testing.expect(!cache.evict_mutex.tryLock());
    cache.evict_mutex.unlock();
    try result;
    try std.testing.expect(cleanup.isFinished());
    try std.testing.expectEqual(@as(usize, 1), original.entry.ref_count);
    try std.testing.expectEqualStrings("raw", original.runTableBlock());
}

test "cache read admission transient cleanup ownership frees exactly once without wait authority" {
    var cache = Cache.init(std.testing.allocator, 1);
    defer cache.deinit();
    var handle = try cache.putRunTableBlock("transient-cleanup", 1, 1, 0, 3, try std.testing.allocator.dupe(u8, "raw"));
    try std.testing.expect(!handle.isRetained());
    var cleanup = handle.takeReleaseOwnership();
    defer cleanup.finishBlocking();
    const Budget = struct {
        pub fn checkpoint(_: *@This()) !void {
            return error.Canceled;
        }
        pub fn waitForProgress(_: *@This()) !void {
            return error.TestUnexpectedResult;
        }
    };
    var budget = Budget{};
    var adapter = read_admission.Adapter(*Budget){ .budget = &budget };
    const payload = adapter.payload();
    try cleanup.finishWithAdmission(read_admission.Port.init(&payload));
    try cleanup.finishWithAdmission(read_admission.Port.init(&payload));
    try std.testing.expect(cleanup.isFinished());
    try std.testing.expectEqual(@as(usize, 0), cache.entryCount());
}

test "cache read admission loader cleanup retains its owned key after original path and scope expire" {
    var cache = Cache.init(std.testing.allocator, 1024);
    defer cache.deinit();
    const path = try std.testing.allocator.dupe(u8, "owned-loader-key");
    var claim = cache.beginLoadClaim(path, 1, 1, .run_table_block, 7, 3, null) catch |err| {
        std.testing.allocator.free(path);
        return err;
    };
    std.testing.allocator.free(path);
    defer claim.finishBlocking();
    const shard = cache.shardForKey(claim.key.?);
    const Budget = struct {
        expired: bool = false,
        waits: usize = 0,
        pub fn checkpoint(self: *@This()) !void {
            if (self.expired) return error.CatalogRoutingSnapshotTimeout;
        }
        pub fn waitForProgress(self: *@This()) !void {
            self.waits += 1;
            self.expired = true;
        }
    };
    var request = Budget{};
    var request_adapter = read_admission.Adapter(*Budget){ .budget = &request };
    const request_payload = request_adapter.payload();
    try std.testing.expect(shard.pending_sync.tryLock());
    const result = claim.finishWithAdmission(read_admission.Port.init(&request_payload));
    try std.testing.expect(!shard.pending_sync.tryLock());
    shard.pending_sync.unlock();
    try std.testing.expectError(error.CatalogRoutingSnapshotTimeout, result);
    try std.testing.expect(!claim.isFinished());
    try std.testing.expectEqual(@as(usize, 1), request.waits);
    try std.testing.expectEqual(claim.identity, shard.pending_loads.getAdapted(claim.key.?, Cache.KeyContext{}).?.identity);
    var completion = Budget{};
    var completion_adapter = read_admission.Adapter(*Budget){ .budget = &completion };
    const completion_payload = completion_adapter.payload();
    try claim.finishWithAdmission(read_admission.Port.init(&completion_payload));
    try std.testing.expect(claim.isFinished());
    try std.testing.expectEqual(@as(usize, 0), shard.pending_loads.count());
    try claim.finishWithAdmission(read_admission.Port.init(&request_payload));
    var later = try cache.beginLoadClaim("owned-loader-key", 1, 1, .run_table_block, 7, 3, null);
    defer later.finishBlocking();
}

test "cache read admission stale loader cleanup preserves a replacement claim" {
    var cache = Cache.init(std.testing.allocator, 1024);
    defer cache.deinit();
    var old = try cache.beginLoadClaim("replacement-loader", 1, 1, .run_table_index, 0, 0, null);
    defer old.finishBlocking();
    cache.finishLoad("replacement-loader", 1, 1, .run_table_index);
    var replacement = try cache.beginLoadClaim("replacement-loader", 1, 1, .run_table_index, 0, 0, null);
    defer replacement.finishBlocking();
    try std.testing.expect(old.identity != replacement.identity);
    const Budget = struct {
        pub fn checkpoint(_: *@This()) !void {}
        pub fn waitForProgress(_: *@This()) !void {
            return error.Canceled;
        }
    };
    var budget = Budget{};
    var adapter = read_admission.Adapter(*Budget){ .budget = &budget };
    const payload = adapter.payload();
    try old.finishWithAdmission(read_admission.Port.init(&payload));
    try std.testing.expect(old.isFinished());
    const shard = cache.shardForKey(replacement.key.?);
    try std.testing.expectEqual(replacement.identity, shard.pending_loads.getAdapted(replacement.key.?, Cache.KeyContext{}).?.identity);
    try std.testing.expectError(error.Canceled, cache.beginLoadWithAdmission("replacement-loader", 1, 1, .run_table_index, read_admission.Port.init(&payload)));
    try std.testing.expectEqual(replacement.identity, shard.pending_loads.getAdapted(replacement.key.?, Cache.KeyContext{}).?.identity);
}

test "cache read admission owned loader allocation failures never strand a registration" {
    const Fixture = struct {
        fn run(allocator: Allocator) !void {
            var cache = try Cache.initFallible(allocator, 1024);
            defer cache.deinit();
            var claim = cache.beginLoadClaim("allocation-loader", 1, 1, .run_table_index, 0, 0, null) catch |err| {
                const key = makeKey("allocation-loader", 1, 1, .run_table_index, 0, 0);
                try std.testing.expectEqual(@as(usize, 0), cache.shardForKey(key).pending_loads.count());
                return err;
            };
            defer claim.finishBlocking();
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "cache read admission loader identity exhaustion cannot wrap into another owner" {
    var cache = Cache.init(std.testing.allocator, 1024);
    defer cache.deinit();
    cache.next_load_identity.store(std.math.maxInt(u64) - 1, .monotonic);
    var claim = try cache.beginLoadClaim("last-loader", 1, 1, .run_table_index, 0, 0, null);
    defer claim.finishBlocking();
    try std.testing.expectEqual(std.math.maxInt(u64) - 1, claim.identity);
    try std.testing.expectError(error.CacheLoadIdentityExhausted, cache.beginLoadClaim("exhausted-loader", 2, 1, .run_table_index, 0, 0, null));
    const key = makeKey("exhausted-loader", 2, 1, .run_table_index, 0, 0);
    try std.testing.expect(cache.shardForKey(key).pending_loads.getAdapted(key, Cache.KeyContext{}) == null);
    try std.testing.expectEqual(std.math.maxInt(u64), cache.next_load_identity.load(.monotonic));
}

test "cache read admission eviction ledger waits preserve value and charge until transfer" {
    inline for (.{ .accounting, .manager }) |phase| {
        inline for (.{ .cancel, .release }) |mode| {
            var manager = resource_manager_mod.ResourceManager.init(.{});
            defer manager.deinit(std.testing.allocator);
            var cache = Cache.init(std.testing.allocator, 1024 * 1024);
            cache.attachResourceManager(&manager);
            defer cache.deinit();
            var pinned = try cache.putRunTableBlock("eviction-pinned", 2, 1, 0, 3, try std.testing.allocator.dupe(u8, "old"));
            defer pinned.release();
            const pinned_cost = pinned.entry.byte_cost;
            var candidate = try cache.putRunTableBlock("eviction-candidate", 1, 1, 0, 3, try std.testing.allocator.dupe(u8, "raw"));
            candidate.releaseForAdmissionFailure();
            try drainDeferredPinsForTest(&cache);
            const original_bytes = cache.currentBytes();
            const held = switch (phase) {
                .accounting => &cache.resource_accounting_mutex,
                .manager => &manager.mutex,
                else => unreachable,
            };
            const Budget = struct {
                mutex: *std.atomic.Mutex,
                canceled: bool = false,
                waits: usize = 0,
                pub fn checkpoint(self: *@This()) !void {
                    if (self.canceled) return error.CatalogRoutingSnapshotTimeout;
                }
                pub fn waitForProgress(self: *@This()) !void {
                    self.waits += 1;
                    if (mode == .cancel) self.canceled = true else self.mutex.unlock();
                }
            };
            var budget = Budget{ .mutex = held };
            var adapter = read_admission.Adapter(*Budget){ .budget = &budget };
            const payload = adapter.payload();
            try std.testing.expect(held.tryLock());
            const result = cache.evictOneWithAdmission(read_admission.Port.init(&payload));
            if (mode == .cancel) {
                try std.testing.expect(!held.tryLock());
                held.unlock();
                try std.testing.expectError(error.CatalogRoutingSnapshotTimeout, result);
                try std.testing.expectEqual(@as(usize, 2), cache.entryCount());
                try std.testing.expectEqual(original_bytes, cache.currentBytes());
                var retained = cache.retainRunTableBlock("eviction-candidate", 1, 1, 0, 3).?;
                try std.testing.expectEqualStrings("raw", retained.runTableBlock());
                retained.releaseForAdmissionFailure();
                try drainDeferredPinsForTest(&cache);
            } else {
                try std.testing.expect(try result);
                try std.testing.expectEqual(@as(usize, 1), cache.entryCount());
                try std.testing.expectEqual(pinned_cost, cache.currentBytes());
                try std.testing.expect(cache.retainRunTableBlock("eviction-candidate", 1, 1, 0, 3) == null);
            }
            try std.testing.expectEqual(@as(usize, 1), budget.waits);
            try std.testing.expectEqual(@as(u64, @intCast(cache.currentBytes())), cache.resource_accounted_bytes);
            try std.testing.expectEqual(cache.resource_accounted_bytes, manager.sliceStats(.lsm_block_table_cache).used_bytes);
            try std.testing.expectEqualStrings("old", pinned.runTableBlock());
        }
    }
}

test "cache read admission post-eviction cancellation preserves ledger and a new shard holder" {
    var manager = resource_manager_mod.ResourceManager.init(.{});
    defer manager.deinit(std.testing.allocator);
    var cache = Cache.init(std.testing.allocator, 1024 * 1024);
    cache.attachResourceManager(&manager);
    defer cache.deinit();
    var pinned = try cache.putRunTableBlock("after-eviction-pinned", 2, 1, 0, 3, try std.testing.allocator.dupe(u8, "old"));
    defer pinned.release();
    const pinned_cost = pinned.entry.byte_cost;
    var candidate = try cache.putRunTableBlock("after-eviction-candidate", 1, 1, 0, 3, try std.testing.allocator.dupe(u8, "raw"));
    const shard = cache.shardForKey(candidate.entry.key());
    candidate.releaseForAdmissionFailure();
    try drainDeferredPinsForTest(&cache);
    const Budget = struct {
        cache: *Cache,
        shard: *std.atomic.Mutex,
        acquired: bool = false,
        pub fn checkpoint(self: *@This()) !void {
            if (self.cache.entryCount() == 1) {
                if (!self.acquired) {
                    try std.testing.expect(self.shard.tryLock());
                    self.acquired = true;
                }
                return error.Canceled;
            }
        }
        pub fn waitForProgress(_: *@This()) !void {
            return error.TestUnexpectedResult;
        }
    };
    var budget = Budget{ .cache = &cache, .shard = &shard.mutex };
    var adapter = read_admission.Adapter(*Budget){ .budget = &budget };
    const payload = adapter.payload();
    const result = cache.evictOneWithAdmission(read_admission.Port.init(&payload));
    if (budget.acquired) {
        try std.testing.expect(!shard.mutex.tryLock());
        shard.mutex.unlock();
    }
    try std.testing.expectError(error.Canceled, result);
    try std.testing.expect(budget.acquired);
    try std.testing.expectEqual(pinned_cost, cache.currentBytes());
    try std.testing.expectEqual(@as(u64, @intCast(pinned_cost)), cache.resource_accounted_bytes);
    try std.testing.expectEqual(cache.resource_accounted_bytes, manager.sliceStats(.lsm_block_table_cache).used_bytes);
    try std.testing.expect(cache.retainRunTableBlock("after-eviction-candidate", 1, 1, 0, 3) == null);
    try std.testing.expectEqualStrings("old", pinned.runTableBlock());
}

test "cache read admission physical retirement precedes charge release and rejects invalid owners" {
    inline for (.{ .success, .stale, .growth, .canceled }) |scenario| {
        var manager = resource_manager_mod.ResourceManager.init(.{});
        defer manager.deinit(std.testing.allocator);
        var current: u64 = 0;
        try manager.adjustUsage(.lsm_block_table_cache, &current, 60);
        defer manager.adjustUsage(.lsm_block_table_cache, &current, 0) catch unreachable;
        var retired = false;
        const Retirement = struct {
            manager: *resource_manager_mod.ResourceManager,
            current: *u64,
            retired: *bool,
            pub fn retire(self: @This()) void {
                std.debug.assert(!self.manager.mutex.tryLock());
                std.debug.assert(self.manager.memory.used_bytes == 60);
                std.debug.assert(self.current.* == 60);
                self.retired.* = true;
            }
        };
        const Budget = struct {
            canceled: bool = false,
            pub fn checkpoint(self: *@This()) !void {
                if (self.canceled) return error.Canceled;
            }
            pub fn waitForProgress(self: *@This()) !void {
                self.canceled = true;
            }
        };
        var budget = Budget{};
        var adapter = read_admission.Adapter(*Budget){ .budget = &budget };
        const payload = adapter.payload();
        if (scenario == .stale) current += 1;
        if (scenario == .canceled) try std.testing.expect(manager.mutex.tryLock());
        const result = manager.retireUsageWithAdmission(.lsm_block_table_cache, &current, if (scenario == .growth) 61 else 30, read_admission.Port.init(&payload), Retirement{ .manager = &manager, .current = &current, .retired = &retired });
        if (scenario == .canceled) {
            try std.testing.expect(!manager.mutex.tryLock());
            manager.mutex.unlock();
            try std.testing.expectError(error.Canceled, result);
        } else if (scenario == .growth) {
            try std.testing.expectError(error.InvalidArgument, result);
        } else if (scenario == .stale) {
            try std.testing.expectError(error.ResourceAccountingMismatch, result);
            current -= 1;
        } else try result;
        try std.testing.expectEqual(scenario == .success, retired);
        try std.testing.expectEqual(@as(u64, if (scenario == .success) 30 else 60), current);
        try std.testing.expectEqual(current, manager.sliceStats(.lsm_block_table_cache).used_bytes);
    }
}

test "cache read admission parent chain preserves original scope and all ledgers under contention" {
    inline for (.{ .cancel, .release }) |mode| {
        var root = resource_manager_mod.ResourceManager.init(.{});
        defer root.deinit(std.testing.allocator);
        var middle = resource_manager_mod.ResourceManager.init(.{});
        defer middle.deinit(std.testing.allocator);
        try middle.installMemoryParent(root.memoryParentPort());
        var child = resource_manager_mod.ResourceManager.init(.{});
        defer child.deinit(std.testing.allocator);
        try child.installMemoryParent(middle.memoryParentPort());
        {
            var cache = Cache.init(std.testing.allocator, 1024 * 1024);
            cache.attachResourceManager(&child);
            defer cache.deinit();
            const Budget = struct {
                root: *resource_manager_mod.ResourceManager,
                middle: *resource_manager_mod.ResourceManager,
                child: *resource_manager_mod.ResourceManager,
                expired: bool = false,
                waits: usize = 0,
                pub fn checkpoint(self: *@This()) !void {
                    if (self.expired) return error.CatalogRoutingSnapshotTimeout;
                }
                pub fn waitForProgress(self: *@This()) !void {
                    self.waits += 1;
                    try std.testing.expect(self.child.mutex.tryLock());
                    self.child.mutex.unlock();
                    try std.testing.expect(self.middle.mutex.tryLock());
                    self.middle.mutex.unlock();
                    try std.testing.expectEqual(@as(u64, 0), self.child.memory.used_bytes);
                    try std.testing.expectEqual(@as(u64, 0), self.middle.memory.used_bytes);
                    if (mode == .cancel) self.expired = true else self.root.mutex.unlock();
                }
            };
            var budget = Budget{ .root = &root, .middle = &middle, .child = &child };
            var adapter = read_admission.Adapter(*Budget){ .budget = &budget };
            const payload = adapter.payload();
            try std.testing.expect(root.mutex.tryLock());
            const result = cache.putRunTableBlockWithAdmission("parent-chain", 1, 1, 0, 3, try std.testing.allocator.dupe(u8, "raw"), read_admission.Port.init(&payload));
            if (mode == .cancel) {
                try std.testing.expect(!root.mutex.tryLock());
                root.mutex.unlock();
                try std.testing.expectError(error.CatalogRoutingSnapshotTimeout, result);
                try std.testing.expectEqual(@as(usize, 0), cache.entryCount());
                try std.testing.expectEqual(@as(usize, 0), child.observer_identities.map.count());
            } else {
                var handle = try result;
                defer handle.release();
                try std.testing.expectEqualStrings("raw", handle.runTableBlock());
            }
            try std.testing.expectEqual(@as(usize, 1), budget.waits);
            try std.testing.expectEqual(@as(u64, @intCast(cache.currentBytes())), child.memory.used_bytes);
            try std.testing.expectEqual(child.memory.used_bytes, middle.memory.used_bytes);
            try std.testing.expectEqual(child.memory.used_bytes, root.memory.used_bytes);
        }
        try std.testing.expectEqual(@as(u64, 0), child.memory.used_bytes);
        try std.testing.expectEqual(@as(u64, 0), middle.memory.used_bytes);
        try std.testing.expectEqual(@as(u64, 0), root.memory.used_bytes);
    }
}

test "cache read admission parent nonblocking capability rejects fallback without partial ownership" {
    var root = resource_manager_mod.ResourceManager.init(.{});
    defer root.deinit(std.testing.allocator);
    var child = resource_manager_mod.ResourceManager.init(.{});
    defer child.deinit(std.testing.allocator);
    try child.installMemoryParent(root.memoryParentPort());
    const Trap = struct {
        fn reserve(_: *anyopaque, _: u64) callconv(.c) bool {
            @panic("controlled parent admission used legacy reservation");
        }
    };
    child.memory_parent.?.try_reserve_fn = null;
    child.memory_parent.?.reserve_fn = Trap.reserve;
    const Budget = struct {
        pub fn checkpoint(_: *@This()) !void {}
        pub fn waitForProgress(_: *@This()) !void {
            return error.TestUnexpectedResult;
        }
    };
    var budget = Budget{};
    var adapter = read_admission.Adapter(*Budget){ .budget = &budget };
    const payload = adapter.payload();
    var current: u64 = 0;
    try std.testing.expectError(error.ExecutionWaitUnsupported, child.adjustUsageWithAdmission(.lsm_block_table_cache, &current, 10, read_admission.Port.init(&payload)));
    try std.testing.expectEqual(@as(u64, 0), current);
    try std.testing.expectEqual(@as(u64, 0), child.memory.used_bytes);
    try std.testing.expectEqual(@as(u64, 0), root.memory.used_bytes);
    try std.testing.expectEqual(@as(usize, 0), child.observer_identities.map.count());
}

test "cache read admission nonblocking parent chain denial never reserves ancestor bytes" {
    var root = resource_manager_mod.ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = 100 } });
    defer root.deinit(std.testing.allocator);
    var middle = resource_manager_mod.ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = 50 } });
    defer middle.deinit(std.testing.allocator);
    try middle.installMemoryParent(root.memoryParentPort());
    var child = resource_manager_mod.ResourceManager.init(.{});
    defer child.deinit(std.testing.allocator);
    try child.installMemoryParent(middle.memoryParentPort());
    try std.testing.expectEqual(resource_manager_mod.ResourceManager.MemoryParent.ReserveResult.denied, child.tryReserveExternal(60));
    try std.testing.expectEqual(@as(u64, 0), child.memory.used_bytes);
    try std.testing.expectEqual(@as(u64, 0), middle.memory.used_bytes);
    try std.testing.expectEqual(@as(u64, 0), root.memory.used_bytes);
    try std.testing.expectEqual(resource_manager_mod.ResourceManager.MemoryParent.ReserveResult.reserved, child.tryReserveExternal(40));
    try std.testing.expectEqual(@as(u64, 40), root.memory.used_bytes);
    child.releaseExternal(40);
    try std.testing.expectEqual(@as(u64, 0), root.memory.used_bytes);
}

test "cache read admission malformed parent layout fails before a held child ledger" {
    var root = resource_manager_mod.ResourceManager.init(.{});
    defer root.deinit(std.testing.allocator);
    var child = resource_manager_mod.ResourceManager.init(.{});
    defer child.deinit(std.testing.allocator);
    var parent = root.memoryParentPort();
    parent.struct_size -= 8;
    try std.testing.expect(child.mutex.tryLock());
    const result = child.installMemoryParent(parent);
    try std.testing.expect(!child.mutex.tryLock());
    child.mutex.unlock();
    try std.testing.expectError(error.UnsupportedVersion, result);
    parent = root.memoryParentPort();
    parent.version = 0;
    try std.testing.expectError(error.UnsupportedVersion, child.installMemoryParent(parent));
    try std.testing.expect(child.memory_parent == null);
}

test "cache read admission parent pressure snapshots wait outside the child ledger under original scope" {
    inline for (.{ .cancel, .release }) |mode| {
        var root = resource_manager_mod.ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = 1 } });
        defer root.deinit(std.testing.allocator);
        try std.testing.expect(root.reserveExternal(1));
        defer root.releaseExternal(1);
        var child = resource_manager_mod.ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = 1 } });
        defer child.deinit(std.testing.allocator);
        try child.installMemoryParent(root.memoryParentPort());
        const Trap = struct {
            fn snapshot(_: *anyopaque, _: *@import("runtime_memory_abi").ParentSnapshot) callconv(.c) void {
                @panic("controlled parent pressure used the legacy snapshot callback");
            }
        };
        child.memory_parent.?.snapshot_fn = Trap.snapshot;
        const Budget = struct {
            root: *resource_manager_mod.ResourceManager,
            child: *resource_manager_mod.ResourceManager,
            expired: bool = false,
            waits: usize = 0,
            pub fn checkpoint(self: *@This()) !void {
                if (self.expired) return error.CatalogRoutingSnapshotTimeout;
            }
            pub fn waitForProgress(self: *@This()) !void {
                self.waits += 1;
                try std.testing.expect(self.child.mutex.tryLock());
                self.child.mutex.unlock();
                try std.testing.expect(self.child.reclaimer_mutex.tryLock());
                self.child.reclaimer_mutex.unlock();
                if (mode == .cancel) self.expired = true else self.root.mutex.unlock();
            }
        };
        var budget = Budget{ .root = &root, .child = &child };
        var adapter = read_admission.Adapter(*Budget){ .budget = &budget };
        const payload = adapter.payload();
        var current: u64 = 0;
        try std.testing.expect(root.mutex.tryLock());
        const result = child.adjustUsageWithAdmission(.lsm_block_table_cache, &current, 10, read_admission.Port.init(&payload));
        if (mode == .cancel) {
            try std.testing.expect(!root.mutex.tryLock());
            root.mutex.unlock();
            try std.testing.expectError(error.CatalogRoutingSnapshotTimeout, result);
        } else try std.testing.expectError(error.ResourceBudgetExceeded, result);
        try std.testing.expectEqual(@as(usize, 1), budget.waits);
        try std.testing.expectEqual(@as(u64, 0), current);
        try std.testing.expectEqual(@as(u64, 0), child.memory.used_bytes);
        try std.testing.expectEqual(@as(u64, 1), root.memory.used_bytes);
        try std.testing.expectEqual(@as(usize, 0), child.observer_identities.map.count());
    }
}

test "cache read admission missing parent pressure capability fails closed without legacy fallback" {
    var root = resource_manager_mod.ResourceManager.init(.{});
    defer root.deinit(std.testing.allocator);
    var child = resource_manager_mod.ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = 1 } });
    defer child.deinit(std.testing.allocator);
    try child.installMemoryParent(root.memoryParentPort());
    const Trap = struct {
        fn snapshot(_: *anyopaque, _: *@import("runtime_memory_abi").ParentSnapshot) callconv(.c) void {
            @panic("missing nonblocking snapshot invoked legacy fallback");
        }
    };
    child.memory_parent.?.try_snapshot_fn = null;
    child.memory_parent.?.snapshot_fn = Trap.snapshot;
    const Budget = struct {
        pub fn checkpoint(_: *@This()) !void {}
        pub fn waitForProgress(_: *@This()) !void {
            return error.TestUnexpectedResult;
        }
    };
    var budget = Budget{};
    var adapter = read_admission.Adapter(*Budget){ .budget = &budget };
    const payload = adapter.payload();
    var current: u64 = 0;
    try std.testing.expectError(error.ExecutionWaitUnsupported, child.adjustUsageWithAdmission(.lsm_block_table_cache, &current, 10, read_admission.Port.init(&payload)));
    try std.testing.expectEqual(@as(u64, 0), current);
    try std.testing.expectEqual(@as(u64, 0), root.memory.used_bytes);
    try std.testing.expectEqual(@as(usize, 0), child.observer_identities.map.count());
}

test "cache read admission nonblocking parent snapshots preserve unpublished output and reject older layout" {
    var root = resource_manager_mod.ResourceManager.init(.{ .memory_budget = .{ .soft_limit_bytes = 5, .hard_limit_bytes = 10 } });
    defer root.deinit(std.testing.allocator);
    try std.testing.expect(root.reserveExternal(7));
    defer root.releaseExternal(7);
    var parent = root.memoryParentPort();
    var snapshot: @import("runtime_memory_abi").ParentSnapshot = .{ .used_bytes = 55, .soft_limit_bytes = 66, .hard_limit_bytes = 77 };
    try std.testing.expect(root.mutex.tryLock());
    const result = parent.try_snapshot_fn.?(parent.ptr, &snapshot);
    try std.testing.expect(!root.mutex.tryLock());
    root.mutex.unlock();
    try std.testing.expectEqual(resource_manager_mod.ResourceManager.MemoryParent.SnapshotResult.busy, result);
    try std.testing.expectEqual(@as(u64, 55), snapshot.used_bytes);
    try std.testing.expectEqual(@as(u64, 66), snapshot.soft_limit_bytes);
    try std.testing.expectEqual(@as(u64, 77), snapshot.hard_limit_bytes);
    try std.testing.expectEqual(resource_manager_mod.ResourceManager.MemoryParent.SnapshotResult.ready, parent.try_snapshot_fn.?(parent.ptr, &snapshot));
    try std.testing.expectEqual(@as(u64, 7), snapshot.used_bytes);
    try std.testing.expectEqual(@as(u64, 5), snapshot.soft_limit_bytes);
    try std.testing.expectEqual(@as(u64, 10), snapshot.hard_limit_bytes);
    var child = resource_manager_mod.ResourceManager.init(.{});
    defer child.deinit(std.testing.allocator);
    parent.version = 1;
    try std.testing.expectError(error.UnsupportedVersion, child.installMemoryParent(parent));
}

test "cache read admission parent reclaimer registry contention spends original scope outside child locks" {
    inline for (.{ .cancel, .release }) |mode| {
        var root = resource_manager_mod.ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = 1 } });
        defer root.deinit(std.testing.allocator);
        try std.testing.expect(root.reserveExternal(1));
        defer root.releaseExternal(1);
        var child = resource_manager_mod.ResourceManager.init(.{});
        defer child.deinit(std.testing.allocator);
        try child.installMemoryParent(root.memoryParentPort());
        const Budget = struct {
            root: *resource_manager_mod.ResourceManager,
            child: *resource_manager_mod.ResourceManager,
            expired: bool = false,
            waits: usize = 0,
            pub fn checkpoint(self: *@This()) !void {
                if (self.expired) return error.CatalogRoutingSnapshotTimeout;
            }
            pub fn waitForProgress(self: *@This()) !void {
                self.waits += 1;
                try std.testing.expect(self.child.mutex.tryLock());
                self.child.mutex.unlock();
                try std.testing.expect(self.child.reclaimer_mutex.tryLock());
                self.child.reclaimer_mutex.unlock();
                try std.testing.expect(self.root.mutex.tryLock());
                self.root.mutex.unlock();
                if (mode == .cancel) self.expired = true else self.root.reclaimer_mutex.unlock();
            }
        };
        var budget = Budget{ .root = &root, .child = &child };
        var adapter = read_admission.Adapter(*Budget){ .budget = &budget };
        const payload = adapter.payload();
        var current: u64 = 0;
        try std.testing.expect(root.reclaimer_mutex.tryLock());
        const result = child.adjustUsageWithAdmission(.lsm_block_table_cache, &current, 10, read_admission.Port.init(&payload));
        if (mode == .cancel) {
            try std.testing.expect(!root.reclaimer_mutex.tryLock());
            root.reclaimer_mutex.unlock();
            try std.testing.expectError(error.CatalogRoutingSnapshotTimeout, result);
        } else try std.testing.expectError(error.ResourceBudgetExceeded, result);
        try std.testing.expectEqual(@as(usize, 1), budget.waits);
        try std.testing.expectEqual(@as(u64, 0), current);
        try std.testing.expectEqual(@as(u64, 1), root.memory.used_bytes);
    }
}

test "cache read admission checked parent reclaim preserves scope failures and releases real sibling cache capacity" {
    inline for (.{ .cancel, .reclaim }) |mode| {
        var root = resource_manager_mod.ResourceManager.init(.{});
        defer root.deinit(std.testing.allocator);
        var cache = Cache.init(std.testing.allocator, 1024 * 1024);
        cache.attachResourceManager(&root);
        defer cache.deinit();
        var handle = try cache.putRunTableBlock("parent-reclaim-cache", 1, 1, 0, 3, try std.testing.allocator.dupe(u8, "raw"));
        handle.releaseForAdmissionFailure();
        try drainDeferredPinsForTest(&cache);
        const initial_bytes = cache.currentBytes();
        root.memory.budget.hard_limit_bytes = @intCast(initial_bytes);
        var child = resource_manager_mod.ResourceManager.init(.{});
        defer child.deinit(std.testing.allocator);
        try child.installMemoryParent(root.memoryParentPort());
        const Budget = struct {
            root: *resource_manager_mod.ResourceManager,
            pub fn checkpoint(self: *@This()) !void {
                if (mode == .cancel and self.root.reclaim_requests.load(.monotonic) != 0) return error.CatalogRoutingSnapshotTimeout;
            }
            pub fn waitForProgress(_: *@This()) !void {
                return error.TestUnexpectedResult;
            }
        };
        var budget = Budget{ .root = &root };
        var adapter = read_admission.Adapter(*Budget){ .budget = &budget };
        const payload = adapter.payload();
        var current: u64 = 0;
        defer child.adjustUsage(.lsm_block_table_cache, &current, 0) catch unreachable;
        const result = child.adjustUsageWithAdmission(.lsm_block_table_cache, &current, 10, read_admission.Port.init(&payload));
        if (mode == .cancel) {
            try std.testing.expectError(error.CatalogRoutingSnapshotTimeout, result);
            try std.testing.expectEqual(@as(u64, 0), current);
            try std.testing.expectEqual(initial_bytes, cache.currentBytes());
            var retained = cache.retainRunTableBlock("parent-reclaim-cache", 1, 1, 0, 3).?;
            try std.testing.expectEqualStrings("raw", retained.runTableBlock());
            retained.releaseForAdmissionFailure();
            try drainDeferredPinsForTest(&cache);
        } else {
            try result;
            try std.testing.expectEqual(@as(u64, 10), current);
            try std.testing.expectEqual(@as(usize, 0), cache.currentBytes());
            try std.testing.expectEqual(@as(u64, 10), root.memory.used_bytes);
            try std.testing.expectEqual(@as(u64, 0), cache.resource_accounted_bytes);
        }
        for (root.reclaimers.items) |slot| try std.testing.expectEqual(@as(usize, 0), slot.in_flight.load(.acquire));
    }
}

test "cache read admission parent reclaim rejects missing and malformed checked capability without fallback" {
    inline for (.{ .missing, .malformed }) |mode| {
        var root = resource_manager_mod.ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = 1 } });
        defer root.deinit(std.testing.allocator);
        try std.testing.expect(root.reserveExternal(1));
        defer root.releaseExternal(1);
        var child = resource_manager_mod.ResourceManager.init(.{});
        defer child.deinit(std.testing.allocator);
        try child.installMemoryParent(root.memoryParentPort());
        const Trap = struct {
            fn legacy(_: *anyopaque, _: u64) callconv(.c) u64 {
                @panic("controlled parent reclaimed through legacy fallback");
            }
            fn checked(_: *anyopaque, _: u64, _: read_admission.Port) !u64 {
                @panic("malformed parent reclaim payload was invoked");
            }
        };
        const reclaim_port = @import("../../common/parent_reclaim_admission.zig");
        const malformed = reclaim_port.Payload{ .vtable_contract = .of(u8), .vtable = &.{ .reclaim = Trap.checked } };
        child.memory_parent.?.reclaim_fn = Trap.legacy;
        child.memory_parent.?.reclaim_admission = if (mode == .missing) null else &malformed;
        const Budget = struct {
            pub fn checkpoint(_: *@This()) !void {}
            pub fn waitForProgress(_: *@This()) !void {
                return error.TestUnexpectedResult;
            }
        };
        var budget = Budget{};
        var adapter = read_admission.Adapter(*Budget){ .budget = &budget };
        const payload = adapter.payload();
        var current: u64 = 0;
        const result = child.adjustUsageWithAdmission(.lsm_block_table_cache, &current, 10, read_admission.Port.init(&payload));
        try std.testing.expectError(if (mode == .missing) error.ExecutionWaitUnsupported else error.UnsupportedVersion, result);
        try std.testing.expectEqual(@as(u64, 0), current);
        try std.testing.expectEqual(@as(u64, 0), child.memory.used_bytes);
        try std.testing.expectEqual(@as(u64, 1), root.memory.used_bytes);
    }
}

extern fn antfly_parent_reclaim_fixture_payload() callconv(.c) *const anyopaque;

test "cache read admission parent reclaim crosses independently compiled error domains with original scope" {
    const port = @import("../../common/parent_reclaim_admission.zig");
    const Budget = struct {
        calls: usize = 0,
        fail: bool = false,
        pub fn checkpoint(self: *@This()) !void {
            self.calls += 1;
            if (self.fail) return error.GenerationPublicationChanged;
        }
        pub fn waitForProgress(_: *@This()) !void {
            return error.TestUnexpectedResult;
        }
    };
    var budget = Budget{};
    var adapter = read_admission.Adapter(*Budget){ .budget = &budget };
    const payload = adapter.payload();
    const foreign = antfly_parent_reclaim_fixture_payload();
    try std.testing.expectEqual(@as(u64, 8), try port.reclaim(foreign, &budget, 7, read_admission.Port.init(&payload)));
    try std.testing.expectEqual(@as(usize, 1), budget.calls);
    budget.fail = true;
    try std.testing.expectError(error.GenerationPublicationChanged, port.reclaim(foreign, &budget, 7, read_admission.Port.init(&payload)));
    try std.testing.expectEqual(@as(usize, 2), budget.calls);
}

test "cache read admission reclamation lease finishes while registry is held" {
    const Fixture = struct {
        manager: *resource_manager_mod.ResourceManager,
        held: std.atomic.Value(bool) = .init(false),
        done: std.atomic.Value(bool) = .init(false),
        rescued: std.atomic.Value(bool) = .init(false),
        fn reclaim(raw: *anyopaque, target: u64) u64 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            while (!self.manager.reclaimer_mutex.tryLock()) std.atomic.spinLoopHint();
            self.held.store(true, .release);
            return target;
        }
        pub fn checkpoint(self: *@This()) !void {
            if (self.held.load(.acquire)) return error.CatalogRoutingSnapshotTimeout;
        }
        pub fn waitForProgress(_: *@This()) !void {
            return error.TestUnexpectedResult;
        }
        fn rescue(self: *@This()) void {
            while (!self.held.load(.acquire)) {
                if (self.done.load(.acquire)) return;
                std.Io.sleep(std.testing.io, .fromMilliseconds(1), .awake) catch {};
            }
            for (0..100) |_| {
                if (self.done.load(.acquire)) break;
                std.Io.sleep(std.testing.io, .fromMilliseconds(1), .awake) catch {};
            }
            if (!self.done.load(.acquire)) self.rescued.store(true, .release);
            self.manager.reclaimer_mutex.unlock();
        }
    };
    var manager = resource_manager_mod.ResourceManager.init(.{
        .identity_allocator = std.testing.allocator,
        .memory_budget = .{ .hard_limit_bytes = 100 },
    });
    defer manager.deinit(std.testing.allocator);
    var fixture = Fixture{ .manager = &manager };
    const identity = try manager.registerReclaimer(.hbc_node_metadata_cache, &fixture, Fixture.reclaim);
    defer manager.unregisterReclaimer(identity);
    var accounted: u64 = 0;
    manager.observeUsage(.hbc_node_metadata_cache, &accounted, 100);
    defer manager.observeUsage(.hbc_node_metadata_cache, &accounted, 0);
    var rescue = try std.testing.io.concurrent(Fixture.rescue, .{&fixture});
    defer {
        fixture.done.store(true, .release);
        rescue.await(std.testing.io);
    }
    // The callback returns with the registry still locked. Its mandatory lease
    // decrement must finish without waiting for the rescue thread to unlock it.
    var adapter = read_admission.Adapter(*Fixture){ .budget = &fixture };
    const payload = adapter.payload();
    const parent = manager.memoryParentPort();
    const reclaimed = @import("../../common/parent_reclaim_admission.zig").reclaim(
        parent.reclaim_admission.?,
        parent.ptr,
        10,
        read_admission.Port.init(&payload),
    );
    fixture.done.store(true, .release);
    try std.testing.expectError(error.CatalogRoutingSnapshotTimeout, reclaimed);
    try std.testing.expect(!fixture.rescued.load(.acquire));
    try std.testing.expectEqual(@as(u32, 0), manager.reclaimers.items[0].in_flight.load(.acquire));
}

test "cache read admission active reclamation lease survives registry growth" {
    const Fixture = struct {
        manager: *resource_manager_mod.ResourceManager,
        identities: [257]u64 = @splat(0),
        failed: bool = false,
        fn passive(_: *anyopaque, _: u64) u64 {
            return 0;
        }
        fn reclaim(raw: *anyopaque, target: u64) u64 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            for (&self.identities) |*identity| {
                identity.* = self.manager.registerReclaimer(.hbc_node_metadata_cache, self, passive) catch {
                    self.failed = true;
                    return 0;
                };
            }
            return target;
        }
    };
    var manager = resource_manager_mod.ResourceManager.init(.{
        .identity_allocator = std.testing.allocator,
        .memory_budget = .{ .hard_limit_bytes = 100 },
    });
    defer manager.deinit(std.testing.allocator);
    var fixture = Fixture{ .manager = &manager };
    const identity = try manager.registerReclaimer(.hbc_node_metadata_cache, &fixture, Fixture.reclaim);
    defer {
        for (fixture.identities) |registered| manager.unregisterReclaimer(registered);
        manager.unregisterReclaimer(identity);
    }
    const original_slot = manager.reclaimers.items[0];
    var accounted: u64 = 0;
    manager.observeUsage(.hbc_node_metadata_cache, &accounted, 100);
    defer manager.observeUsage(.hbc_node_metadata_cache, &accounted, 0);
    try std.testing.expectEqual(@as(u64, 10), manager.reclaimForAllocation(.dense_apply_working_set, 10));
    try std.testing.expect(!fixture.failed);
    try std.testing.expectEqual(@as(usize, 258), manager.reclaimers.items.len);
    try std.testing.expect(original_slot == manager.reclaimers.items[0]);
    for (manager.reclaimers.items) |slot| try std.testing.expectEqual(@as(u32, 0), slot.in_flight.load(.acquire));
}

test "cache read admission reclamation chunk allocation failure retires all owners" {
    const Fixture = struct {
        fn passive(_: *anyopaque, _: u64) u64 {
            return 0;
        }
        fn run(allocator: std.mem.Allocator) !void {
            var manager = resource_manager_mod.ResourceManager.init(.{ .identity_allocator = allocator });
            defer manager.deinit(allocator);
            var context: u8 = 0;
            var identities: [65]u64 = @splat(0);
            defer for (identities) |identity| manager.unregisterReclaimer(identity);
            for (&identities) |*identity| identity.* = try manager.registerReclaimer(.hbc_node_metadata_cache, &context, passive);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "cache read admission unregister waits for callback lifetime before slot reuse" {
    const Fixture = struct {
        manager: *resource_manager_mod.ResourceManager,
        identity: u64 = 0,
        active: std.atomic.Value(bool) = .init(false),
        release: std.atomic.Value(bool) = .init(false),
        retired: std.atomic.Value(bool) = .init(false),
        completed: u64 = 0,
        observed: u64 = 0,
        fn reclaim(raw: *anyopaque, target: u64) u64 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.active.store(true, .release);
            while (!self.release.load(.acquire)) std.Io.sleep(std.testing.io, .fromMilliseconds(1), .awake) catch {};
            self.completed = 42;
            return target;
        }
        fn run(self: *@This()) u64 {
            return self.manager.reclaimForAllocation(.dense_apply_working_set, 10);
        }
        fn retire(self: *@This()) void {
            self.manager.unregisterReclaimer(self.identity);
            self.observed = self.completed;
            self.retired.store(true, .release);
        }
    };
    var manager = resource_manager_mod.ResourceManager.init(.{
        .identity_allocator = std.testing.allocator,
        .memory_budget = .{ .hard_limit_bytes = 100 },
    });
    defer manager.deinit(std.testing.allocator);
    var fixture = Fixture{ .manager = &manager };
    fixture.identity = try manager.registerReclaimer(.hbc_node_metadata_cache, &fixture, Fixture.reclaim);
    defer manager.unregisterReclaimer(fixture.identity);
    const original_slot = manager.reclaimers.items[0];
    var accounted: u64 = 0;
    manager.observeUsage(.hbc_node_metadata_cache, &accounted, 100);
    defer manager.observeUsage(.hbc_node_metadata_cache, &accounted, 0);
    var worker = try std.testing.io.concurrent(Fixture.run, .{&fixture});
    defer {
        fixture.release.store(true, .release);
        _ = worker.await(std.testing.io);
    }
    while (!fixture.active.load(.acquire)) std.Io.sleep(std.testing.io, .fromMilliseconds(1), .awake) catch {};
    var retire = try std.testing.io.concurrent(Fixture.retire, .{&fixture});
    defer {
        fixture.release.store(true, .release);
        retire.await(std.testing.io);
    }
    // Observe retirement under its owning lock, before releasing the callback.
    while (true) {
        if (manager.reclaimer_mutex.tryLock()) {
            const retiring = original_slot.retiring;
            manager.reclaimer_mutex.unlock();
            if (retiring) break;
        }
        std.Io.sleep(std.testing.io, .fromMilliseconds(1), .awake) catch {};
    }
    try std.testing.expect(!fixture.retired.load(.acquire));
    fixture.release.store(true, .release);
    try std.testing.expectEqual(@as(u64, 10), worker.await(std.testing.io));
    retire.await(std.testing.io);
    try std.testing.expectEqual(@as(u64, 42), fixture.observed);
    const replacement = try manager.registerReclaimer(.hbc_node_metadata_cache, &fixture, Fixture.reclaim);
    defer manager.unregisterReclaimer(replacement);
    try std.testing.expect(replacement != fixture.identity);
    try std.testing.expect(original_slot == manager.reclaimers.items[0]);
    try std.testing.expectEqual(@as(u32, 0), original_slot.in_flight.load(.acquire));
}

test "cache read admission index decode cancels every metadata phase without leaking ownership" {
    const table = @import("../lsm/table_file.zig");
    var keys: [600][4]u8 = undefined;
    var entries: [600]table.Entry = undefined;
    const value: [1024]u8 = @splat('x');
    for (&entries, &keys, 0..) |*entry, *key, i| {
        std.mem.writeInt(u32, key, @intCast(i), .big);
        entry.* = .{ .namespace_name = "ns", .key = key, .value = &value, .tombstone = false };
    }
    const raw = try table.encodeAlloc(std.testing.allocator, &entries);
    defer std.testing.allocator.free(raw);
    const footer = try table.decodeFooter(raw);
    const metadata = raw[footer.metadata_offset..][0..footer.metadata_len];
    const Budget = struct {
        calls: usize = 0,
        fail_at: usize = 0,
        pub fn checkpoint(self: *@This()) !void {
            self.calls += 1;
            if (self.calls == self.fail_at) return error.CatalogRoutingSnapshotTimeout;
        }
        pub fn waitForProgress(_: *@This()) !void {
            return error.TestUnexpectedResult;
        }
    };
    inline for (.{ false, true }) |footer_only| {
        var budget = Budget{};
        var adapter = read_admission.Adapter(*Budget){ .budget = &budget };
        const payload = adapter.payload();
        const control = read_admission.Port.init(&payload);
        var index = if (footer_only)
            try table.decodeIndexFromFooterAllocWithAdmission(std.testing.allocator, footer, metadata, control)
        else
            try table.decodeIndexAllocWithAdmission(std.testing.allocator, raw, control);
        defer index.deinit(std.testing.allocator);
        var legacy = try table.decodeIndexAlloc(std.testing.allocator, raw);
        defer legacy.deinit(std.testing.allocator);
        try std.testing.expectEqualSlices(u16, legacy.block_entry_offsets, index.block_entry_offsets);
        try std.testing.expectEqual(legacy.blocks.len, index.blocks.len);
        try std.testing.expectEqual(@as(usize, entries.len), index.entryCount());
        const checkpoints = budget.calls;
        try std.testing.expect(checkpoints > 20);
        for (1..checkpoints + 1) |fail_at| {
            budget = .{ .fail_at = fail_at };
            const result = if (footer_only)
                table.decodeIndexFromFooterAllocWithAdmission(std.testing.allocator, footer, metadata, control)
            else
                table.decodeIndexAllocWithAdmission(std.testing.allocator, raw, control);
            try std.testing.expectError(error.CatalogRoutingSnapshotTimeout, result);
            try std.testing.expectEqual(fail_at, budget.calls);
        }
    }
}

test "cache read admission index decode allocation failures release partial metadata" {
    const table = @import("../lsm/table_file.zig");
    const raw = try table.encodeAlloc(std.testing.allocator, &.{
        .{ .namespace_name = "ns", .key = "a", .value = "value", .tombstone = false },
        .{ .namespace_name = "ns", .key = "b", .value = "value", .tombstone = false },
    });
    defer std.testing.allocator.free(raw);
    const Fixture = struct {
        pub fn checkpoint(_: *@This()) !void {}
        pub fn waitForProgress(_: *@This()) !void {
            return error.TestUnexpectedResult;
        }
        fn run(allocator: std.mem.Allocator, bytes: []const u8) !void {
            var budget = @This(){};
            var adapter = read_admission.Adapter(*@This()){ .budget = &budget };
            const payload = adapter.payload();
            var index = try table.decodeIndexAllocWithAdmission(allocator, bytes, read_admission.Port.init(&payload));
            defer index.deinit(allocator);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{raw});
}

test "cache read admission parent release preserves cleanup debt through ancestor contention" {
    inline for (.{ .middle, .root }) |ancestor| inline for (.{ false, true }) |cancel| {
        var root = resource_manager_mod.ResourceManager.init(.{});
        defer root.deinit(std.testing.allocator);
        var middle = resource_manager_mod.ResourceManager.init(.{});
        defer middle.deinit(std.testing.allocator);
        try middle.installMemoryParent(root.memoryParentPort());
        var child = resource_manager_mod.ResourceManager.init(.{});
        defer child.deinit(std.testing.allocator);
        try child.installMemoryParent(middle.memoryParentPort());
        var cache = Cache.init(std.testing.allocator, 1024 * 1024);
        cache.attachResourceManager(&child);
        defer cache.deinit();
        var other = try cache.putRunTableBlock("release-unrelated", 1, 1, 0, 3, try std.testing.allocator.dupe(u8, "old"));
        defer other.release();
        const unrelated_cost = other.entry.byte_cost;
        var handle = try cache.putRunTableBlock("release-pending", 2, 1, 0, 3, try std.testing.allocator.dupe(u8, "raw"));
        const original_bytes = child.memory.used_bytes;
        cache.invalidatePath("release-pending");
        var cleanup = handle.takeReleaseOwnership();
        defer cleanup.finishBlocking();
        const held = if (ancestor == .root) &root.mutex else &middle.mutex;
        const Budget = struct {
            child: *resource_manager_mod.ResourceManager,
            held: *std.atomic.Mutex,
            expired: bool = false,
            waits: usize = 0,
            pub fn checkpoint(self: *@This()) !void {
                if (self.expired) return error.CatalogRoutingSnapshotTimeout;
            }
            pub fn waitForProgress(self: *@This()) !void {
                self.waits += 1;
                // No parent contention may retain the child ledger while the
                // caller runs its own original-budget progress callback.
                try std.testing.expect(self.child.mutex.tryLock());
                self.child.mutex.unlock();
                if (cancel) self.expired = true else self.held.unlock();
            }
        };
        var budget = Budget{ .child = &child, .held = held };
        var adapter = read_admission.Adapter(*Budget){ .budget = &budget };
        const payload = adapter.payload();
        try std.testing.expect(held.tryLock());
        const result = cleanup.finishWithAdmission(read_admission.Port.init(&payload));
        try std.testing.expectEqual(@as(usize, 1), budget.waits);
        if (cancel) {
            try std.testing.expect(!held.tryLock());
            held.unlock();
            try std.testing.expectError(error.CatalogRoutingSnapshotTimeout, result);
            try std.testing.expect(!cleanup.isFinished());
            try std.testing.expect(cleanup.handle == null);
            try std.testing.expectEqual(@as(usize, @intCast(original_bytes - unrelated_cost)), cleanup.accounting_bytes);
            try std.testing.expectEqual(original_bytes, cache.resource_accounted_bytes);
            try std.testing.expectEqual(original_bytes, child.memory.used_bytes);
            try std.testing.expectEqual(original_bytes, middle.memory.used_bytes);
            try std.testing.expectEqual(original_bytes, root.memory.used_bytes);
            // Physical memory is already gone, but debt must remain owned and
            // unavailable until another independently supplied scope retires it.
            try std.testing.expectEqual(unrelated_cost, cache.currentBytes());
            var completion = Budget{ .child = &child, .held = held };
            var completion_adapter = read_admission.Adapter(*Budget){ .budget = &completion };
            const completion_payload = completion_adapter.payload();
            try cleanup.finishWithAdmission(read_admission.Port.init(&completion_payload));
            try std.testing.expectEqual(@as(usize, 0), completion.waits);
        } else try result;
        try std.testing.expect(cleanup.isFinished());
        try std.testing.expectEqual(@as(u64, @intCast(unrelated_cost)), cache.resource_accounted_bytes);
        try std.testing.expectEqual(@as(u64, @intCast(unrelated_cost)), child.memory.used_bytes);
        try std.testing.expectEqual(child.memory.used_bytes, middle.memory.used_bytes);
        try std.testing.expectEqual(child.memory.used_bytes, root.memory.used_bytes);
        try std.testing.expectEqualStrings("old", other.runTableBlock());
        try cleanup.finishWithAdmission(read_admission.Port.init(&payload));
        try std.testing.expectEqual(child.memory.used_bytes, root.memory.used_bytes);
    };
}

test "cache read admission nonblocking parent release fails closed without partial debit" {
    var root = resource_manager_mod.ResourceManager.init(.{});
    defer root.deinit(std.testing.allocator);
    var middle = resource_manager_mod.ResourceManager.init(.{});
    defer middle.deinit(std.testing.allocator);
    try middle.installMemoryParent(root.memoryParentPort());
    var child = resource_manager_mod.ResourceManager.init(.{});
    defer child.deinit(std.testing.allocator);
    try child.installMemoryParent(middle.memoryParentPort());
    const Release = resource_manager_mod.ResourceManager.MemoryParent.ReleaseResult;
    try std.testing.expect(child.reserveExternal(40));
    defer child.releaseExternal(40);
    try std.testing.expectEqual(Release.mismatch, child.tryReleaseExternal(41));
    try std.testing.expectEqual(Release.busy, blk: {
        try std.testing.expect(root.mutex.tryLock());
        defer root.mutex.unlock();
        break :blk child.tryReleaseExternal(10);
    });
    const original = middle.memory_parent.?;
    defer middle.memory_parent = original;
    const Trap = struct {
        fn release(_: *anyopaque, _: u64) callconv(.c) void {
            @panic("controlled parent release used legacy blocking release");
        }
    };
    middle.memory_parent.?.release_fn = Trap.release;
    middle.memory_parent.?.try_release_fn = null;
    try std.testing.expectEqual(Release.unsupported, child.tryReleaseExternal(10));
    middle.memory_parent = original;
    middle.memory_parent.?.version -= 1;
    try std.testing.expectEqual(Release.unsupported, child.tryReleaseExternal(10));
    middle.memory_parent = original;
    middle.memory_parent.?.struct_size -= 8;
    try std.testing.expectEqual(Release.unsupported, child.tryReleaseExternal(10));
    try std.testing.expectEqual(@as(u64, 40), child.memory.external_bytes);
    try std.testing.expectEqual(@as(u64, 40), middle.memory.external_bytes);
    try std.testing.expectEqual(@as(u64, 40), root.memory.external_bytes);
    try std.testing.expectEqual(@as(u64, 40), child.memory.used_bytes);
    try std.testing.expectEqual(child.memory.used_bytes, middle.memory.used_bytes);
    try std.testing.expectEqual(child.memory.used_bytes, root.memory.used_bytes);
}

test "cache read admission missing parent release capability retains observer debt" {
    var root = resource_manager_mod.ResourceManager.init(.{});
    defer root.deinit(std.testing.allocator);
    var child = resource_manager_mod.ResourceManager.init(.{});
    defer child.deinit(std.testing.allocator);
    try child.installMemoryParent(root.memoryParentPort());
    var current: u64 = 0;
    try child.adjustUsage(.lsm_block_table_cache, &current, 30);
    defer child.adjustUsage(.lsm_block_table_cache, &current, 0) catch unreachable;
    const original = child.memory_parent.?;
    defer child.memory_parent = original;
    const Trap = struct {
        fn release(_: *anyopaque, _: u64) callconv(.c) void {
            @panic("controlled observer release used legacy blocking release");
        }
    };
    child.memory_parent.?.release_fn = Trap.release;
    child.memory_parent.?.try_release_fn = null;
    const Budget = struct {
        pub fn checkpoint(_: *@This()) !void {}
        pub fn waitForProgress(_: *@This()) !void {
            return error.TestUnexpectedResult;
        }
    };
    var budget = Budget{};
    var adapter = read_admission.Adapter(*Budget){ .budget = &budget };
    const payload = adapter.payload();
    try std.testing.expectError(error.ExecutionWaitUnsupported, child.adjustUsageWithAdmission(.lsm_block_table_cache, &current, 0, read_admission.Port.init(&payload)));
    try std.testing.expectEqual(@as(u64, 30), current);
    try std.testing.expectEqual(@as(u64, 30), child.memory.used_bytes);
    try std.testing.expectEqual(@as(u64, 30), root.memory.used_bytes);
    try std.testing.expectEqual(@as(usize, 1), child.observer_identities.map.count());
}

test "cache read admission eviction retains physical retirement debt across parent cancellation" {
    inline for (.{ false, true }) |cancel| {
        var root = resource_manager_mod.ResourceManager.init(.{});
        defer root.deinit(std.testing.allocator);
        var child = resource_manager_mod.ResourceManager.init(.{});
        defer child.deinit(std.testing.allocator);
        try child.installMemoryParent(root.memoryParentPort());
        var cache = Cache.init(std.testing.allocator, 1024 * 1024);
        cache.attachResourceManager(&child);
        defer cache.deinit();
        var kept = try cache.putRunTableBlock("debt-kept", 1, 1, 0, 3, try std.testing.allocator.dupe(u8, "old"));
        defer kept.release();
        const kept_cost = kept.entry.byte_cost;
        var evicted = try cache.putRunTableBlock("debt-evicted", 2, 1, 0, 3, try std.testing.allocator.dupe(u8, "raw"));
        const evicted_cost = evicted.entry.byte_cost;
        const evicted_shard = cache.shardForKey(evicted.entry.key());
        evicted.release();
        const before = child.memory.used_bytes;
        const Budget = struct {
            root: *resource_manager_mod.ResourceManager,
            child: *resource_manager_mod.ResourceManager,
            cache: *Cache,
            shard: *std.atomic.Mutex,
            before: u64,
            retired: usize,
            expired: bool = false,
            waits: usize = 0,
            pub fn checkpoint(self: *@This()) !void {
                if (self.expired) return error.CatalogRoutingSnapshotTimeout;
            }
            pub fn waitForProgress(self: *@This()) !void {
                self.waits += 1;
                try std.testing.expect(self.shard.tryLock());
                self.shard.unlock();
                try std.testing.expect(self.child.mutex.tryLock());
                self.child.mutex.unlock();
                try std.testing.expectEqual(@as(usize, 1), self.cache.entryCount());
                try std.testing.expectEqual(@as(u64, @intCast(self.retired)), self.cache.resource_retirement_debt);
                try std.testing.expectEqual(self.before, self.child.memory.used_bytes);
                try std.testing.expectEqual(self.before, self.root.memory.used_bytes);
                if (cancel) self.expired = true else self.root.mutex.unlock();
            }
        };
        var budget = Budget{ .root = &root, .child = &child, .cache = &cache, .shard = &evicted_shard.mutex, .before = before, .retired = evicted_cost };
        var adapter = read_admission.Adapter(*Budget){ .budget = &budget };
        const payload = adapter.payload();
        try std.testing.expect(root.mutex.tryLock());
        const result = cache.evictOneWithAdmission(read_admission.Port.init(&payload));
        try std.testing.expectEqual(@as(usize, 1), budget.waits);
        if (cancel) {
            try std.testing.expect(!root.mutex.tryLock());
            root.mutex.unlock();
            try std.testing.expectError(error.CatalogRoutingSnapshotTimeout, result);
            try std.testing.expectEqual(before, cache.resource_accounted_bytes);
            try std.testing.expectEqual(@as(u64, @intCast(evicted_cost)), cache.resource_retirement_debt);
            // A later scope retires the already-destroyed entry's exact charge.
            var completion = budget;
            completion.expired = false;
            completion.waits = 0;
            var completion_adapter = read_admission.Adapter(*Budget){ .budget = &completion };
            const completion_payload = completion_adapter.payload();
            try cache.finishRetirementsWithAdmission(read_admission.Port.init(&completion_payload));
            try std.testing.expectEqual(@as(usize, 0), completion.waits);
        } else try std.testing.expect(try result);
        try std.testing.expectEqual(@as(u64, 0), cache.resource_retirement_debt);
        try std.testing.expectEqual(@as(u64, @intCast(kept_cost)), cache.resource_accounted_bytes);
        try std.testing.expectEqual(cache.resource_accounted_bytes, child.memory.used_bytes);
        try std.testing.expectEqual(child.memory.used_bytes, root.memory.used_bytes);
        try std.testing.expectEqualStrings("old", kept.runTableBlock());
        try std.testing.expect(cache.retainRunTableBlock("debt-evicted", 2, 1, 0, 3) == null);
        if (cancel) try std.testing.expectError(error.CatalogRoutingSnapshotTimeout, cache.finishRetirementsWithAdmission(read_admission.Port.init(&payload)));
        // A healthy repeated finish cannot debit any unrelated bytes.
        budget.expired = false;
        try cache.finishRetirementsWithAdmission(read_admission.Port.init(&payload));
        try std.testing.expectEqual(@as(u64, @intCast(kept_cost)), root.memory.used_bytes);
    }
}

test "cache read admission retirement debt rejects stale and excessive destruction" {
    var manager = resource_manager_mod.ResourceManager.init(.{});
    defer manager.deinit(std.testing.allocator);
    var current: u64 = 0;
    var debt: u64 = 0;
    try manager.adjustUsage(.lsm_block_table_cache, &current, 60);
    defer manager.adjustUsage(.lsm_block_table_cache, &current, 0) catch unreachable;
    const Retirement = struct {
        count: *usize,
        pub fn retire(self: @This()) void {
            self.count.* += 1;
        }
    };
    const Budget = struct {
        pub fn checkpoint(_: *@This()) !void {}
        pub fn waitForProgress(_: *@This()) !void {
            return error.TestUnexpectedResult;
        }
    };
    var budget = Budget{};
    var adapter = read_admission.Adapter(*Budget){ .budget = &budget };
    const payload = adapter.payload();
    var count: usize = 0;
    const retirement = Retirement{ .count = &count };
    try manager.retireUsageToDebtWithAdmission(.lsm_block_table_cache, &current, &debt, 20, read_admission.Port.init(&payload), retirement);
    try manager.retireUsageToDebtWithAdmission(.lsm_block_table_cache, &current, &debt, 20, read_admission.Port.init(&payload), retirement);
    try std.testing.expectEqual(@as(usize, 2), count);
    try std.testing.expectEqual(@as(u64, 40), debt);
    try std.testing.expectEqual(@as(u64, 60), current);
    try std.testing.expectError(error.ResourceAccountingMismatch, manager.retireUsageToDebtWithAdmission(.lsm_block_table_cache, &current, &debt, 21, read_admission.Port.init(&payload), retirement));
    current += 1;
    const stale = manager.retireUsageToDebtWithAdmission(.lsm_block_table_cache, &current, &debt, 1, read_admission.Port.init(&payload), retirement);
    current -= 1;
    try std.testing.expectError(error.ResourceAccountingMismatch, stale);
    try std.testing.expectEqual(@as(usize, 2), count);
    try std.testing.expectEqual(@as(u64, 40), debt);
    try std.testing.expectEqual(@as(u64, 60), manager.memory.used_bytes);
    try manager.adjustUsageWithAdmission(.lsm_block_table_cache, &current, current - debt, read_admission.Port.init(&payload));
    debt = 0;
    try std.testing.expectEqual(@as(u64, 20), manager.memory.used_bytes);
}

test "cache read admission missing release capability leaves eviction debt owned until retry" {
    var root = resource_manager_mod.ResourceManager.init(.{});
    defer root.deinit(std.testing.allocator);
    var child = resource_manager_mod.ResourceManager.init(.{});
    defer child.deinit(std.testing.allocator);
    try child.installMemoryParent(root.memoryParentPort());
    var cache = Cache.init(std.testing.allocator, 1024 * 1024);
    cache.attachResourceManager(&child);
    defer cache.deinit();
    var handle = try cache.putRunTableBlock("unsupported-retirement", 1, 1, 0, 3, try std.testing.allocator.dupe(u8, "raw"));
    const charge = child.memory.used_bytes;
    handle.release();
    const original = child.memory_parent.?;
    defer child.memory_parent = original;
    const Trap = struct {
        fn release(_: *anyopaque, _: u64) callconv(.c) void {
            @panic("controlled cache eviction called blocking parent release");
        }
    };
    child.memory_parent.?.release_fn = Trap.release;
    child.memory_parent.?.try_release_fn = null;
    const Budget = struct {
        pub fn checkpoint(_: *@This()) !void {}
        pub fn waitForProgress(_: *@This()) !void {
            return error.TestUnexpectedResult;
        }
    };
    var budget = Budget{};
    var adapter = read_admission.Adapter(*Budget){ .budget = &budget };
    const payload = adapter.payload();
    try std.testing.expectError(error.ExecutionWaitUnsupported, cache.evictOneWithAdmission(read_admission.Port.init(&payload)));
    try std.testing.expectEqual(@as(usize, 0), cache.entryCount());
    try std.testing.expectEqual(@as(usize, 0), cache.currentBytes());
    try std.testing.expectEqual(charge, cache.resource_retirement_debt);
    try std.testing.expectEqual(charge, cache.resource_accounted_bytes);
    try std.testing.expectEqual(charge, child.memory.used_bytes);
    try std.testing.expectEqual(charge, root.memory.used_bytes);
    child.memory_parent = original;
    // The ordinary controlled eviction entry point also retries retained debt.
    try std.testing.expect(!try cache.evictOneWithAdmission(read_admission.Port.init(&payload)));
    try std.testing.expectEqual(@as(u64, 0), cache.resource_retirement_debt);
    try std.testing.expectEqual(@as(u64, 0), child.memory.used_bytes);
    try std.testing.expectEqual(@as(u64, 0), root.memory.used_bytes);
}

test "cache read admission deinit destroys physical buffers before releasing charged capacity" {
    var manager = resource_manager_mod.ResourceManager.init(.{});
    defer manager.deinit(std.testing.allocator);
    const Tracking = struct {
        watched: ?[*]u8 = null,
        freed: bool = false,
        early_release: bool = false,
        manager: *resource_manager_mod.ResourceManager,
        fn alloc(_: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
            return std.testing.allocator.rawAlloc(len, alignment, ra);
        }
        fn free(raw: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ra: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.watched == memory.ptr) {
                self.freed = true;
                self.early_release = self.manager.memory.used_bytes == 0;
            }
            std.testing.allocator.rawFree(memory, alignment, ra);
        }
        fn allocator(self: *@This()) Allocator {
            return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = Allocator.noResize, .remap = Allocator.noRemap, .free = free } };
        }
    };
    var tracking = Tracking{ .manager = &manager };
    const allocator = tracking.allocator();
    var cache = Cache.init(allocator, 1024 * 1024);
    cache.attachResourceManager(&manager);
    const bytes = try allocator.dupe(u8, "raw");
    tracking.watched = bytes.ptr;
    var handle = try cache.putRunTableBlock("deinit-order", 1, 1, 0, 3, bytes);
    handle.release();
    try std.testing.expect(manager.memory.used_bytes != 0);
    cache.deinit();
    try std.testing.expect(tracking.freed);
    try std.testing.expect(!tracking.early_release);
    try std.testing.expectEqual(@as(u64, 0), manager.memory.used_bytes);
}

test "cache read admission optional reclaimer preserves entries behind busy or stale ledgers" {
    inline for (.{ .busy, .stale }) |mode| {
        var manager = resource_manager_mod.ResourceManager.init(.{});
        defer manager.deinit(std.testing.allocator);
        var cache = Cache.init(std.testing.allocator, 1024 * 1024);
        cache.attachResourceManager(&manager);
        defer cache.deinit();
        var handle = try cache.putRunTableBlock("reclaimer-ledger", 1, 1, 0, 3, try std.testing.allocator.dupe(u8, "raw"));
        handle.release();
        const before = cache.resource_accounted_bytes;
        if (mode == .busy) try std.testing.expect(manager.mutex.tryLock()) else cache.resource_accounted_bytes += 1;
        const reclaimed = Cache.reclaimForResourceManager(&cache, before);
        if (mode == .busy) {
            try std.testing.expect(!manager.mutex.tryLock());
            manager.mutex.unlock();
        } else cache.resource_accounted_bytes -= 1;
        try std.testing.expectEqual(@as(u64, 0), reclaimed);
        try std.testing.expectEqual(@as(usize, 1), cache.entryCount());
        try std.testing.expectEqual(@as(u64, 0), cache.resource_retirement_debt);
        try std.testing.expectEqual(before, manager.memory.used_bytes);
        var retained = cache.retainRunTableBlock("reclaimer-ledger", 1, 1, 0, 3).?;
        try std.testing.expectEqualStrings("raw", retained.runTableBlock());
        retained.release();
        try std.testing.expectEqual(before, Cache.reclaimForResourceManager(&cache, before));
        try std.testing.expectEqual(@as(usize, 0), cache.entryCount());
        try std.testing.expectEqual(@as(u64, 0), manager.memory.used_bytes);
    }
}

test "cache read admission optional reclaimer credits only published capacity under parent contention" {
    inline for (.{ .busy, .unsupported }) |mode| {
        var root = resource_manager_mod.ResourceManager.init(.{});
        defer root.deinit(std.testing.allocator);
        var child = resource_manager_mod.ResourceManager.init(.{});
        defer child.deinit(std.testing.allocator);
        try child.installMemoryParent(root.memoryParentPort());
        var cache = Cache.init(std.testing.allocator, 1024 * 1024);
        cache.attachResourceManager(&child);
        defer cache.deinit();
        var kept = try cache.putRunTableBlock("reclaimer-kept", 1, 1, 0, 3, try std.testing.allocator.dupe(u8, "old"));
        defer kept.release();
        const kept_cost = kept.entry.byte_cost;
        var handle = try cache.putRunTableBlock("reclaimer-parent", 2, 1, 0, 3, try std.testing.allocator.dupe(u8, "raw"));
        const released_cost = handle.entry.byte_cost;
        handle.release();
        const before = cache.resource_accounted_bytes;
        const original = child.memory_parent.?;
        defer child.memory_parent = original;
        const Trap = struct {
            fn release(_: *anyopaque, _: u64) callconv(.c) void {
                @panic("optional cache reclaim called blocking parent release");
            }
        };
        child.memory_parent.?.release_fn = Trap.release;
        if (mode == .busy) try std.testing.expect(root.mutex.tryLock()) else child.memory_parent.?.try_release_fn = null;
        try std.testing.expectEqual(@as(u64, 0), Cache.reclaimForResourceManager(&cache, before));
        try std.testing.expectEqual(@as(usize, 1), cache.entryCount());
        try std.testing.expectEqual(@as(usize, kept_cost), cache.currentBytes());
        try std.testing.expectEqual(@as(u64, @intCast(released_cost)), cache.resource_retirement_debt);
        try std.testing.expectEqual(before, child.memory.used_bytes);
        try std.testing.expectEqual(before, root.memory.used_bytes);
        // Repeated attempts cannot report debt as released capacity or destroy
        // the other owner's pinned entry while the same parent is unavailable.
        try std.testing.expectEqual(@as(u64, 0), Cache.reclaimForResourceManager(&cache, before));
        if (mode == .busy) {
            try std.testing.expect(!root.mutex.tryLock());
            root.mutex.unlock();
        }
        child.memory_parent = original;
        try std.testing.expectEqual(@as(u64, @intCast(released_cost)), Cache.reclaimForResourceManager(&cache, before));
        try std.testing.expectEqual(@as(u64, 0), cache.resource_retirement_debt);
        try std.testing.expectEqual(@as(u64, @intCast(kept_cost)), child.memory.used_bytes);
        try std.testing.expectEqual(child.memory.used_bytes, root.memory.used_bytes);
        try std.testing.expectEqualStrings("old", kept.runTableBlock());
        try std.testing.expectEqual(@as(u64, 0), Cache.reclaimForResourceManager(&cache, before));
    }
}

test "cache read admission optional reclaimer limits retirements while publishing exact capacity" {
    var manager = resource_manager_mod.ResourceManager.init(.{});
    defer manager.deinit(std.testing.allocator);
    var cache = Cache.init(std.testing.allocator, 16 * 1024 * 1024);
    cache.attachResourceManager(&manager);
    defer cache.deinit();
    const count = 2 * Cache.reclaim_retirement_limit + 3;
    for (0..count) |index| {
        var path: [64]u8 = undefined;
        const name = try std.fmt.bufPrint(&path, "reclaimer-burst-{d}", .{index});
        var handle = try cache.putRunTableBlock(name, 1, 1, 0, 3, try std.testing.allocator.dupe(u8, "raw"));
        handle.release();
    }
    const original = manager.memory.used_bytes;
    var total: u64 = 0;
    while (cache.entryCount() != 0) {
        const before_count = cache.entryCount();
        const before_charge = manager.memory.used_bytes;
        const released = Cache.reclaimForResourceManager(&cache, original);
        try std.testing.expect(released != 0);
        try std.testing.expect(before_count - cache.entryCount() <= Cache.reclaim_retirement_limit);
        try std.testing.expectEqual(before_charge - manager.memory.used_bytes, released);
        try std.testing.expectEqual(@as(u64, @intCast(cache.currentBytes())), manager.memory.used_bytes);
        total += released;
    }
    try std.testing.expectEqual(original, total);
    try std.testing.expectEqual(@as(u64, 0), manager.memory.used_bytes);
    try std.testing.expectEqual(@as(u64, 0), cache.resource_retirement_debt);
}

test "cache read admission optional reclaimer limits inspection of a pinned shard chain" {
    var manager = resource_manager_mod.ResourceManager.init(.{});
    defer manager.deinit(std.testing.allocator);
    var cache = Cache.init(std.testing.allocator, 16 * 1024 * 1024);
    cache.attachResourceManager(&manager);
    defer cache.deinit();
    var handles: [Cache.reclaim_inspection_limit + 1]Handle = undefined;
    var initialized: usize = 0;
    defer for (handles[0..initialized]) |*handle| handle.release();
    var candidate: usize = 0;
    while (initialized < handles.len) : (candidate += 1) {
        var path: [64]u8 = undefined;
        const name = try std.fmt.bufPrint(&path, "reclaimer-pinned-{d}", .{candidate});
        if (cache.shardForBlock(name, 1, 1, .run_table_block, 0, 3) != &cache.shards[0]) continue;
        handles[initialized] = try cache.putRunTableBlock(name, 1, 1, 0, 3, try std.testing.allocator.dupe(u8, "raw"));
        initialized += 1;
    }
    const charge = manager.memory.used_bytes;
    try std.testing.expect(cache.resource_accounting_mutex.tryLock());
    var inspections: usize = Cache.reclaim_inspection_limit;
    const removed = cache.tryEvictOneAccountingLocked(&inspections);
    cache.resource_accounting_mutex.unlock();
    try std.testing.expect(!removed);
    try std.testing.expectEqual(@as(usize, Cache.reclaim_inspection_limit), inspections);
    try std.testing.expectEqual(handles.len, cache.entryCount());
    try std.testing.expectEqual(charge, manager.memory.used_bytes);
    try std.testing.expectEqual(@as(u64, 0), Cache.reclaimForResourceManager(&cache, charge));
    // Place an idle candidate beyond a pinned prefix larger than the old
    // scan budget. Its direct idle link must make it reachable in one attempt.
    var idle_path_buffer: [64]u8 = undefined;
    const idle_path = while (true) : (candidate += 1) {
        const path = try std.fmt.bufPrint(&idle_path_buffer, "reclaimer-pinned-{d}", .{candidate});
        if (cache.shardForBlock(path, 1, 1, .run_table_block, 0, 3) == &cache.shards[0]) break path;
    };
    var idle = try cache.putRunTableBlock(idle_path, 1, 1, 0, 3, try std.testing.allocator.dupe(u8, "raw"));
    const idle_cost = idle.entry.byte_cost;
    idle.releaseForAdmissionFailure();
    try drainDeferredPinsForTest(&cache);
    try std.testing.expectEqual(@as(u64, @intCast(idle_cost)), Cache.reclaimForResourceManager(&cache, @intCast(idle_cost)));
    try std.testing.expectEqual(handles.len, cache.entryCount());
    try std.testing.expectEqual(charge, manager.memory.used_bytes);
    try std.testing.expect(cache.retainRunTableBlock(idle_path, 1, 1, 0, 3) == null);
    for (handles[0..initialized]) |handle| try std.testing.expectEqualStrings("raw", handle.runTableBlock());
}

test "cache read admission idle LRU preserves ordering through duplicate pins cleanup and invalidation" {
    inline for (.{ false, true }) |pending_cleanup| {
        var manager = resource_manager_mod.ResourceManager.init(.{});
        defer manager.deinit(std.testing.allocator);
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        var cache = Cache.init(std.testing.allocator, 1024 * 1024);
        cache.attachResourceManager(&manager);
        defer cache.deinit();
        var handles: [3]Handle = undefined;
        var owned = [_]bool{ false, false, false };
        defer for (&handles, owned) |*handle, live| {
            if (live) handle.release();
        };
        var names: [3][]const u8 = undefined;
        var entries: [3]*Cache.Entry = undefined;
        var candidate: usize = 0;
        for (0..3) |index| {
            while (true) : (candidate += 1) {
                const path = try std.fmt.allocPrint(arena.allocator(), "idle-order-{d}", .{candidate});
                if (cache.shardForBlock(path, 1, 1, .run_table_block, 0, 3) != &cache.shards[0]) continue;
                names[index] = path;
                handles[index] = try cache.putRunTableBlock(path, 1, 1, 0, 3, try std.testing.allocator.dupe(u8, "raw"));
                entries[index] = handles[index].entry;
                owned[index] = true;
                candidate += 1;
                break;
            }
        }
        const shard = &cache.shards[0];
        const priority = evictionPriority(.run_table_block);
        try std.testing.expect(shard.idle_heads[priority] == null);
        handles[0].release();
        owned[0] = false;
        handles[1].release();
        owned[1] = false;
        try std.testing.expect(shard.idle_heads[priority] == entries[0]);
        try std.testing.expect(shard.idle_tails[priority] == entries[1]);
        var repinned = cache.retainRunTableBlock(names[0], 1, 1, 0, 3).?;
        var duplicate = repinned.retain();
        repinned.release();
        try std.testing.expect(!entries[0].idle_linked);
        try std.testing.expect(shard.idle_heads[priority] == entries[1]);
        if (pending_cleanup) {
            var cleanup = duplicate.takeReleaseOwnership();
            defer cleanup.finishBlocking();
            const Budget = struct {
                pub fn checkpoint(_: *@This()) !void {}
                pub fn waitForProgress(_: *@This()) !void {
                    return error.TestUnexpectedResult;
                }
            };
            var budget = Budget{};
            var adapter = read_admission.Adapter(*Budget){ .budget = &budget };
            const payload = adapter.payload();
            try cleanup.finishWithAdmission(read_admission.Port.init(&payload));
        } else {
            duplicate.releaseForAdmissionFailure();
            try drainDeferredPinsForTest(&cache);
        }
        try std.testing.expect(shard.idle_heads[priority] == entries[1]);
        try std.testing.expect(shard.idle_tails[priority] == entries[0]);
        cache.invalidatePath(names[1]);
        try std.testing.expect(shard.idle_heads[priority] == entries[0]);
        try std.testing.expect(entries[0].idle_prev == null and entries[0].idle_next == null);
        handles[2].release();
        owned[2] = false;
        try std.testing.expect(shard.idle_tails[priority] == entries[2]);
        const oldest_cost = entries[0].byte_cost;
        try std.testing.expectEqual(@as(u64, @intCast(oldest_cost)), Cache.reclaimForResourceManager(&cache, @intCast(oldest_cost)));
        try std.testing.expect(shard.idle_heads[priority] == entries[2]);
        try std.testing.expect(shard.idle_tails[priority] == entries[2]);
        try std.testing.expect(entries[2].idle_prev == null and entries[2].idle_next == null);
        try std.testing.expect(cache.retainRunTableBlock(names[0], 1, 1, 0, 3) == null);
        var retained = cache.retainRunTableBlock(names[2], 1, 1, 0, 3).?;
        defer retained.release();
        try std.testing.expectEqualStrings("raw", retained.runTableBlock());
        try std.testing.expect(shard.idle_heads[priority] == null and shard.idle_tails[priority] == null);
    }
}

test "cache read admission entry metadata prevents payload-only retention under a host ceiling" {
    const path = "metadata-budget";
    var manager = resource_manager_mod.ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = path.len + 3 } });
    defer manager.deinit(std.testing.allocator);
    var cache = Cache.init(std.testing.allocator, 1024 * 1024);
    cache.attachResourceManager(&manager);
    defer cache.deinit();
    var handle = try cache.putRunTableBlock(path, 1, 1, 0, 3, try std.testing.allocator.dupe(u8, "raw"));
    defer handle.release();
    try std.testing.expect(!handle.isRetained());
    try std.testing.expectEqualStrings("raw", handle.runTableBlock());
    try std.testing.expectEqual(@as(usize, @sizeOf(Cache.Entry) + path.len + 3), handle.entry.byte_cost);
    try std.testing.expectEqual(@as(usize, 0), cache.entryCount());
    try std.testing.expectEqual(@as(u64, 0), manager.memory.used_bytes);
}

test "cache read admission state charge includes spare array capacity and ordered snapshot ownership" {
    inline for (.{ false, true }) |ordered| {
        var manager = resource_manager_mod.ResourceManager.init(.{});
        defer manager.deinit(std.testing.allocator);
        var cache = Cache.init(std.testing.allocator, 1024 * 1024);
        cache.attachResourceManager(&manager);
        defer cache.deinit();
        var mutable = state_mod.ActiveMemTable{};
        defer mutable.deinit(std.testing.allocator);
        var state = State{};
        var state_owned = true;
        defer if (state_owned) state.deinit(std.testing.allocator);
        if (ordered) {
            try mutable.upsert(std.testing.allocator, .{ .name = "ns" }, "key", "value", false);
            state = try mutable.snapshot(std.testing.allocator);
            try std.testing.expect(state.ordered_root != null);
            try std.testing.expectEqual(@as(usize, 0), state.entries.items.len);
        } else {
            try state.upsert(std.testing.allocator, .{ .name = "ns" }, "key", "value", false);
            try state.entries.ensureTotalCapacity(std.testing.allocator, 64);
            try std.testing.expect(state.entries.capacity > state.entries.items.len);
        }
        state.freezeMemoryAccounting();
        const retained_bytes = state.estimatedMemoryBytes();
        if (!ordered) try std.testing.expect(retained_bytes >= state.entries.capacity * @sizeOf(state_mod.OwnedEntry));
        const cost = @sizeOf(Cache.Entry) + "state-metadata".len + clampU64ToUsize(retained_bytes);
        // putRunState consumes the snapshot on both success and failure.
        state_owned = false;
        var handle = try cache.putRunState("state-metadata", 1, 1, state);
        defer handle.release();
        try std.testing.expect(handle.isRetained());
        try std.testing.expectEqual(cost, handle.entry.byte_cost);
        try std.testing.expectEqual(@as(u64, @intCast(cost)), manager.memory.used_bytes);
        try std.testing.expectEqualStrings("value", try handle.runState().get(.{ .name = "ns" }, "key"));
    }
}

test "cache read admission metadata cost arithmetic saturates instead of wrapping capacity" {
    const byte: u8 = 0;
    const huge = @as([*]const u8, @ptrCast(&byte))[0..std.math.maxInt(usize)];
    try std.testing.expectEqual(std.math.maxInt(usize), estimateRawTableCost("path", huge));
    try std.testing.expectEqual(std.math.maxInt(usize), estimateTableBlockCost("path", huge));
    var index = std.mem.zeroes(TableIndex);
    var offset: u32 = 0;
    index.entry_offsets = @as([*]u32, @ptrCast(&offset))[0..std.math.maxInt(usize)];
    try std.testing.expectEqual(std.math.maxInt(usize), estimateTableIndexCost("path", &index));
}

test "cache read admission index metadata charges both owned block boundary keys" {
    var manager = resource_manager_mod.ResourceManager.init(.{});
    defer manager.deinit(std.testing.allocator);
    var cache = Cache.init(std.testing.allocator, 1024 * 1024);
    cache.attachResourceManager(&manager);
    defer cache.deinit();
    var index = std.mem.zeroes(TableIndex);
    var index_owned = true;
    defer if (index_owned) index.deinit(std.testing.allocator);
    index.blocks = try std.testing.allocator.alloc(TableIndex.BlockMeta, 1);
    index.blocks[0] = .{ .relative_offset = 0, .len = 1, .first_entry_index = 0, .entry_count = 1, .largest_key = &.{} };
    index.blocks[0].smallest_namespace_name = try std.testing.allocator.dupe(u8, "first-ns");
    index.blocks[0].smallest_key = try std.testing.allocator.dupe(u8, "first-key");
    index.blocks[0].largest_namespace_name = try std.testing.allocator.dupe(u8, "last-ns");
    index.blocks[0].largest_key = try std.testing.allocator.dupe(u8, "last-key");
    const expected = @sizeOf(Cache.Entry) + "index-metadata".len + @sizeOf(TableIndex.BlockMeta) +
        "first-ns".len + "first-key".len + "last-ns".len + "last-key".len;
    // The cache consumes every owned field, including on failed publication.
    index_owned = false;
    var handle = try cache.putRunTableIndex("index-metadata", 1, 1, index);
    defer handle.release();
    try std.testing.expect(handle.isRetained());
    try std.testing.expectEqual(@as(usize, expected), handle.entry.byte_cost);
    try std.testing.expectEqual(@as(u64, expected), manager.memory.used_bytes);
    try std.testing.expectEqualStrings("first-key", handle.runTableIndex().blocks[0].smallest_key.?);
    try std.testing.expectEqualStrings("last-key", handle.runTableIndex().blocks[0].largest_key);
}

test "cache read admission loader transfers ownership without locks allocation frees or scope callbacks" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    var cache = Cache.init(failing.allocator(), 1024);
    defer cache.deinit();
    var claim = try cache.beginLoadClaim("transferred-loader", 1, 1, .run_table_index, 0, 0, null);
    defer claim.finishBlocking();
    const shard = cache.shardForKey(claim.key.?);
    const identity = claim.identity;
    const key = claim.key.?;
    const allocations = failing.allocations;
    const frees = failing.deallocations;
    failing.fail_index = failing.alloc_index;
    {
        shard.pending_sync.lock();
        defer shard.pending_sync.unlock();
        claim.transferToCache();
        claim.transferToCache();
        try std.testing.expect(claim.isFinished());
        try std.testing.expectEqual(identity, shard.pending_loads.getAdapted(key, Cache.KeyContext{}).?.identity);
        try std.testing.expectEqual(@as(usize, allocations), failing.allocations);
        try std.testing.expectEqual(@as(usize, frees), failing.deallocations);
        try std.testing.expect(!failing.has_induced_failure);
    }
    const Budget = struct {
        expired: bool = true,
        pub fn checkpoint(self: *@This()) !void {
            if (self.expired) return error.CatalogRoutingSnapshotTimeout;
        }
        pub fn waitForProgress(_: *@This()) !void {
            return error.TestUnexpectedResult;
        }
    };
    var budget = Budget{};
    var adapter = read_admission.Adapter(*Budget){ .budget = &budget };
    const payload = adapter.payload();
    try std.testing.expectError(error.CatalogRoutingSnapshotTimeout, cache.drainLoadCleanupWithAdmission(read_admission.Port.init(&payload)));
    try std.testing.expectEqual(@as(usize, 1), shard.pending_loads.count());
    budget.expired = false;
    try cache.drainLoadCleanupWithAdmission(read_admission.Port.init(&payload));
    try std.testing.expectEqual(@as(usize, 0), shard.pending_loads.count());
    try std.testing.expectEqual(frees + 2, failing.deallocations);
}

test "cache read admission loader cleanup slots reject overload before publication and recover after transfer" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    var cache = Cache.init(failing.allocator(), 1024);
    defer cache.deinit();
    const path = "bounded-loader-slots";
    const target = cache.shardForKey(makeKey(path, 1, 1, .run_table_index, 0, 0));
    var claims: [Cache.load_cleanup_slots]LoadClaim = undefined;
    var count: usize = 0;
    defer for (claims[0..count]) |*claim| claim.finishBlocking();
    var next_id: u64 = 1;
    for (&claims) |*claim| {
        while (cache.shardForKey(makeKey(path, next_id, 1, .run_table_index, 0, 0)) != target) next_id += 1;
        claim.* = try cache.beginLoadClaim(path, next_id, 1, .run_table_index, 0, 0, null);
        count += 1;
        next_id += 1;
    }
    while (cache.shardForKey(makeKey(path, next_id, 1, .run_table_index, 0, 0)) != target) next_id += 1;
    failing.fail_index = failing.alloc_index;
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, cache.beginLoadClaim(path, next_id, 1, .run_table_index, 0, 0, null));
    try std.testing.expect(!failing.has_induced_failure);
    try std.testing.expectEqual(Cache.load_cleanup_slots, target.pending_loads.count());
    failing.fail_index = std.math.maxInt(usize);
    claims[0].transferToCache();
    var replacement = try cache.beginLoadClaim(path, next_id, 1, .run_table_index, 0, 0, null);
    defer replacement.finishBlocking();
    try std.testing.expectEqual(Cache.load_cleanup_slots, target.pending_loads.count());
}

test "cache read admission stale transferred loader leaves its replacement owned" {
    var cache = Cache.init(std.testing.allocator, 1024);
    defer cache.deinit();
    var old = try cache.beginLoadClaim("transferred-replacement", 1, 1, .run_table_index, 0, 0, null);
    defer old.finishBlocking();
    cache.finishLoad("transferred-replacement", 1, 1, .run_table_index);
    var replacement = try cache.beginLoadClaim("transferred-replacement", 1, 1, .run_table_index, 0, 0, null);
    defer replacement.finishBlocking();
    const shard = cache.shardForKey(replacement.key.?);
    old.transferToCache();
    shard.pending_sync.lock();
    cache.drainLoadCleanupLocked(shard);
    shard.pending_sync.unlock();
    try std.testing.expectEqual(replacement.identity, shard.pending_loads.getAdapted(replacement.key.?, Cache.KeyContext{}).?.identity);
    try std.testing.expectEqual(@as(usize, 1), shard.pending_loads.count());
}

test "cache read admission legacy duplicate waiter observes transferred cleanup without a wake syscall" {
    if (builtin.single_threaded or !supports_waitable_pending) return error.SkipZigTest;
    const Fixture = struct {
        cache: *Cache,
        io: std.Io,
        done: std.Io.Event = .unset,
        rescued: std.atomic.Value(bool) = .init(false),
        fn wait(self: *@This()) !void {
            try self.cache.beginLoad("legacy-transfer", 1, 1, .run_table_index);
            self.cache.finishLoad("legacy-transfer", 1, 1, .run_table_index);
            self.done.set(self.io);
        }
        fn rescue(self: *@This()) void {
            self.done.waitTimeout(self.io, .{ .duration = .{ .raw = .fromMilliseconds(250), .clock = .awake } }) catch {
                self.rescued.store(true, .release);
                self.cache.finishLoad("legacy-transfer", 1, 1, .run_table_index);
            };
        }
    };
    var threaded = std.Io.Threaded.init(std.testing.allocator, .{ .concurrent_limit = .limited(2) });
    defer threaded.deinit();
    const io = threaded.io();
    var cache = Cache.init(std.testing.allocator, 1024);
    defer cache.deinit();
    var claim = try cache.beginLoadClaim("legacy-transfer", 1, 1, .run_table_index, 0, 0, null);
    defer claim.finishBlocking();
    var fixture = Fixture{ .cache = &cache, .io = io };
    var waiter = try io.concurrent(Fixture.wait, .{&fixture});
    // The watchdog removes a stranded map registration so a regression fails
    // instead of hanging the suite. A valid transfer must complete without it.
    var rescue = io.concurrent(Fixture.rescue, .{&fixture}) catch |err| {
        claim.finishBlocking();
        _ = waiter.await(io) catch {};
        return err;
    };
    defer rescue.await(io);
    var waiter_joined = false;
    defer if (!waiter_joined) {
        _ = waiter.await(io) catch {};
    };
    var observed = false;
    for (0..100) |_| {
        if (cache.stats.run_table_index.waits.load(.acquire) != 0) {
            observed = true;
            break;
        }
        try io.sleep(.fromMilliseconds(1), .awake);
    }
    claim.transferToCache();
    const result = waiter.await(io);
    waiter_joined = true;
    try result;
    try std.testing.expect(observed);
    try std.testing.expect(!fixture.rescued.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), cache.pendingLoadCountForTests());
}

test "cache read admission fixed pending registry admits legacy metadata before key allocation" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    var cache = Cache.init(failing.allocator(), 1024);
    defer cache.deinit();
    const path = "fixed-pending-registry";
    const target = cache.shardForKey(makeKey(path, 1, 1, .run_table_index, 0, 0));
    var ids: [Cache.pending_capacity]u64 = undefined;
    var count: usize = 0;
    defer for (ids[0..count]) |id| cache.finishLoad(path, id, 1, .run_table_index);
    var next_id: u64 = 1;
    const baseline = failing.allocations;
    for (&ids) |*id| {
        while (cache.shardForKey(makeKey(path, next_id, 1, .run_table_index, 0, 0)) != target) next_id += 1;
        id.* = next_id;
        try cache.beginLoad(path, id.*, 1, .run_table_index);
        count += 1;
        next_id += 1;
    }
    // Exactly one path allocation per registration; the registry allocates no
    // buckets, header or growth scratch, even through every occupancy level.
    try std.testing.expectEqual(baseline + Cache.pending_capacity, failing.allocations);
    while (cache.shardForKey(makeKey(path, next_id, 1, .run_table_index, 0, 0)) != target) next_id += 1;
    failing.fail_index = failing.alloc_index;
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, cache.beginLoad(path, next_id, 1, .run_table_index));
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, cache.beginLoadClaim(path, next_id, 1, .run_table_index, 0, 0, null));
    try std.testing.expect(!failing.has_induced_failure);
    try std.testing.expectEqual(Cache.pending_capacity, target.pending_loads.count());
    const retired = ids[0];
    cache.finishLoad(path, retired, 1, .run_table_index);
    ids[0] = next_id;
    failing.fail_index = std.math.maxInt(usize);
    try cache.beginLoad(path, next_id, 1, .run_table_index);
    // Finishing an already-removed identity/path cannot remove the new entry.
    cache.finishLoad(path, retired, 1, .run_table_index);
    try std.testing.expectEqual(Cache.pending_capacity, target.pending_loads.count());
    try std.testing.expect(target.pending_loads.getAdapted(makeKey(path, next_id, 1, .run_table_index, 0, 0), Cache.KeyContext{}) != null);
}

test "cache read admission owned loader keys reject byte quota before allocation and retain OOM credit" {
    const path = "charged-loader-key";
    const bytes: u64 = 2 * path.len;
    const Budget = struct {
        expired: bool = false,
        pub fn checkpoint(self: *@This()) !void {
            if (self.expired) return error.CatalogRoutingSnapshotTimeout;
        }
        pub fn waitForProgress(_: *@This()) !void {
            return error.TestUnexpectedResult;
        }
    };
    for ([_]bool{ false, true }) |fits| {
        var manager = resource_manager_mod.ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = if (fits) bytes else bytes - 1 }, .identity_allocator = std.testing.allocator });
        defer manager.deinit(std.testing.allocator);
        var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
        var cache = Cache.init(failing.allocator(), 1024);
        defer cache.deinit();
        cache.attachResourceManager(&manager);
        failing.fail_index = failing.alloc_index;
        var budget = Budget{};
        var adapter = read_admission.Adapter(*Budget){ .budget = &budget };
        const payload = adapter.payload();
        const port = read_admission.Port.init(&payload);
        try std.testing.expectError(if (fits) error.OutOfMemory else error.ResourceBudgetExceeded, cache.beginLoadClaim(path, 1, 1, .run_table_index, 0, 0, port));
        try std.testing.expectEqual(fits, failing.has_induced_failure);
        try std.testing.expectEqual(if (fits) bytes else @as(u64, 0), manager.snapshot().memory.used_bytes);
        budget.expired = true;
        try std.testing.expectError(error.CatalogRoutingSnapshotTimeout, cache.drainLoadCleanupWithAdmission(port));
        try std.testing.expectEqual(if (fits) bytes else @as(u64, 0), manager.snapshot().memory.used_bytes);
        budget.expired = false;
        try cache.drainLoadCleanupWithAdmission(port);
        try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
    }
}

test "cache read admission owned loader key retirement keeps parent debt through expired cleanup" {
    const path = "parent-key-debt";
    const bytes: u64 = 2 * path.len;
    var parent = resource_manager_mod.ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = bytes }, .identity_allocator = std.testing.allocator });
    defer parent.deinit(std.testing.allocator);
    var child = resource_manager_mod.ResourceManager.init(.{ .identity_allocator = std.testing.allocator });
    defer child.deinit(std.testing.allocator);
    try child.installMemoryParent(parent.memoryParentPort());
    var cache = Cache.init(std.testing.allocator, 1024);
    defer cache.deinit();
    cache.attachResourceManager(&child);
    const Budget = struct {
        shard: ?*Cache.Shard = null,
        parent: *resource_manager_mod.ResourceManager,
        held: bool = false,
        expired: bool = false,
        waits: usize = 0,
        pub fn checkpoint(self: *@This()) !void {
            if (self.expired) return error.CatalogRoutingSnapshotTimeout;
        }
        pub fn waitForProgress(self: *@This()) !void {
            try std.testing.expect(self.shard.?.pending_sync.tryLock());
            self.shard.?.pending_sync.unlock();
            self.waits += 1;
            try std.testing.expect(self.held);
            self.parent.mutex.unlock();
            self.held = false;
        }
    };
    var budget = Budget{ .parent = &parent };
    defer if (budget.held) parent.mutex.unlock();
    var adapter = read_admission.Adapter(*Budget){ .budget = &budget };
    const payload = adapter.payload();
    const port = read_admission.Port.init(&payload);
    var claim = try cache.beginLoadClaim(path, 1, 1, .run_table_index, 0, 0, port);
    defer claim.finishBlocking();
    const slot = claim.cleanup;
    const shard = cache.shardForKey(claim.key.?);
    budget.shard = shard;
    try std.testing.expectEqual(bytes, child.snapshot().memory.used_bytes);
    try std.testing.expectEqual(bytes, parent.snapshot().memory.used_bytes);
    try std.testing.expect(parent.mutex.tryLock());
    budget.held = true;
    claim.transferToCache();
    shard.pending_sync.lock();
    cache.drainLoadCleanupLocked(shard);
    shard.pending_sync.unlock();
    try std.testing.expect(slot.key == null);
    try std.testing.expectEqual(bytes, slot.accounted_bytes);
    try std.testing.expectEqual(bytes, slot.retirement_debt);
    try std.testing.expectEqual(@as(usize, 0), shard.pending_loads.count());
    budget.expired = true;
    try std.testing.expectError(error.CatalogRoutingSnapshotTimeout, cache.drainLoadCleanupWithAdmission(port));
    try std.testing.expectEqual(bytes, slot.accounted_bytes);
    budget.expired = false;
    try cache.drainLoadCleanupWithAdmission(port);
    try std.testing.expectEqual(@as(usize, 1), budget.waits);
    try std.testing.expectEqual(@as(u64, 0), child.snapshot().memory.used_bytes);
    try std.testing.expectEqual(@as(u64, 0), parent.snapshot().memory.used_bytes);
    try std.testing.expectEqual(@as(u64, 0), slot.retirement_debt);
}

test "cache read admission key quota parent contention spends original scope outside pending mutex" {
    var parent = resource_manager_mod.ResourceManager.init(.{ .identity_allocator = std.testing.allocator });
    defer parent.deinit(std.testing.allocator);
    var child = resource_manager_mod.ResourceManager.init(.{ .identity_allocator = std.testing.allocator });
    defer child.deinit(std.testing.allocator);
    try child.installMemoryParent(parent.memoryParentPort());
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    var cache = Cache.init(failing.allocator(), 1024);
    defer cache.deinit();
    cache.attachResourceManager(&child);
    const Budget = struct {
        cache: *Cache,
        parent: *resource_manager_mod.ResourceManager,
        expired: bool = false,
        held: bool = true,
        waits: usize = 0,
        pub fn checkpoint(self: *@This()) !void {
            if (self.expired) return error.GenerationPublicationChanged;
        }
        pub fn waitForProgress(self: *@This()) !void {
            for (self.cache.shards) |*shard| {
                try std.testing.expect(shard.pending_sync.tryLock());
                shard.pending_sync.unlock();
            }
            self.waits += 1;
            self.parent.mutex.unlock();
            self.held = false;
            self.expired = true;
        }
    };
    try std.testing.expect(parent.mutex.tryLock());
    var budget = Budget{ .cache = &cache, .parent = &parent };
    defer if (budget.held) parent.mutex.unlock();
    var adapter = read_admission.Adapter(*Budget){ .budget = &budget };
    const payload = adapter.payload();
    failing.fail_index = failing.alloc_index;
    try std.testing.expectError(error.GenerationPublicationChanged, cache.beginLoadClaim("held-key-parent", 1, 1, .run_table_index, 0, 0, read_admission.Port.init(&payload)));
    try std.testing.expectEqual(@as(usize, 1), budget.waits);
    try std.testing.expect(!failing.has_induced_failure);
    try std.testing.expectEqual(@as(u64, 0), parent.snapshot().memory.used_bytes);
    try std.testing.expectEqual(@as(u64, 0), child.snapshot().memory.used_bytes);
}

test "cache read admission fresh shard can reuse completed loader key quota" {
    var first_path: [16]u8 = @splat('a');
    var second_path = first_path;
    const bytes: u64 = 2 * first_path.len;
    var manager = resource_manager_mod.ResourceManager.init(.{
        .memory_budget = .{ .hard_limit_bytes = bytes },
        .identity_allocator = std.testing.allocator,
    });
    defer manager.deinit(std.testing.allocator);
    var cache = Cache.init(std.testing.allocator, 1024);
    defer cache.deinit();
    cache.attachResourceManager(&manager);
    const first_shard = cache.shardForKey(makeKey(&first_path, 1, 1, .run_table_index, 0, 0));
    for (0..256) |suffix| {
        second_path[second_path.len - 1] = @intCast(suffix);
        if (cache.shardForKey(makeKey(&second_path, 1, 1, .run_table_index, 0, 0)) != first_shard) break;
    }
    try std.testing.expect(cache.shardForKey(makeKey(&second_path, 1, 1, .run_table_index, 0, 0)) != first_shard);
    const Budget = struct {
        pub fn checkpoint(_: *@This()) !void {}
        pub fn waitForProgress(_: *@This()) !void {
            return error.TestUnexpectedResult;
        }
    };
    var budget = Budget{};
    var adapter = read_admission.Adapter(*Budget){ .budget = &budget };
    const payload = adapter.payload();
    const port = read_admission.Port.init(&payload);
    var first = try cache.beginLoadClaim(&first_path, 1, 1, .run_table_index, 0, 0, port);
    defer first.finishBlocking();
    const retired = first.cleanup;
    first.transferToCache();
    try std.testing.expectEqual(bytes, manager.snapshot().memory.used_bytes);
    // No explicit collection: cold admission in the other shard must drain
    // the old owner before attempting its byte reservation.
    var second = try cache.beginLoadClaim(&second_path, 1, 1, .run_table_index, 0, 0, port);
    defer second.finishBlocking();
    try std.testing.expectEqual(@as(@TypeOf(retired.state.load(.acquire)), .free), retired.state.load(.acquire));
    try std.testing.expectEqual(@as(u64, 0), retired.accounted_bytes);
    try std.testing.expectEqual(@as(usize, 0), first_shard.pending_loads.count());
    try std.testing.expectEqual(bytes, manager.snapshot().memory.used_bytes);
    second.transferToCache();
    try cache.drainLoadCleanupWithAdmission(port);
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
}

test "cache read admission failed pin retirement transfers parent debt without accounting locks" {
    var parent = resource_manager_mod.ResourceManager.init(.{ .identity_allocator = std.testing.allocator });
    defer parent.deinit(std.testing.allocator);
    var child = resource_manager_mod.ResourceManager.init(.{ .identity_allocator = std.testing.allocator });
    defer child.deinit(std.testing.allocator);
    try child.installMemoryParent(parent.memoryParentPort());
    var cache = Cache.init(std.testing.allocator, 1024 * 1024);
    defer cache.deinit();
    cache.attachResourceManager(&child);
    var handle = try cache.putRunTableBlock("transferred-pin", 1, 1, 0, 3, try std.testing.allocator.dupe(u8, "raw"));
    const bytes: u64 = @intCast(handle.entry.byte_cost);
    cache.invalidatePath("transferred-pin");
    var token = handle.takeReleaseOwnership();
    defer token.finishBlocking();
    const Foreground = struct {
        pub fn checkpoint(_: *@This()) !void {}
        pub fn waitForProgress(_: *@This()) !void {
            return error.CatalogRoutingSnapshotTimeout;
        }
    };
    var foreground = Foreground{};
    var foreground_adapter = read_admission.Adapter(*Foreground){ .budget = &foreground };
    const foreground_payload = foreground_adapter.payload();
    try std.testing.expect(cache.resource_accounting_mutex.tryLock());
    var accounting_held = true;
    defer if (accounting_held) cache.resource_accounting_mutex.unlock();
    try std.testing.expectError(error.CatalogRoutingSnapshotTimeout, token.finishWithAdmission(read_admission.Port.init(&foreground_payload)));
    try std.testing.expect(child.mutex.tryLock());
    var child_held = true;
    defer if (child_held) child.mutex.unlock();
    try std.testing.expect(parent.mutex.tryLock());
    var parent_held = true;
    defer if (parent_held) parent.mutex.unlock();
    // Once physical work completed, the token transfers its remaining charge
    // while every accounting lock is held. No accounting wait is permitted.
    try std.testing.expect(token.transferRetiredAccountingToCache());
    try std.testing.expectEqual(@as(usize, 0), cache.entryCount());
    try std.testing.expectEqual(bytes, cache.transferred_retirement_debt.load(.acquire));
    try std.testing.expectEqual(bytes, cache.resource_accounted_bytes);
    child.mutex.unlock();
    child_held = false;
    cache.resource_accounting_mutex.unlock();
    accounting_held = false;
    const Budget = struct {
        parent: *resource_manager_mod.ResourceManager,
        held: *bool,
        expired: bool = false,
        waits: usize = 0,
        pub fn checkpoint(self: *@This()) !void {
            if (self.expired) return error.CatalogRoutingSnapshotTimeout;
        }
        pub fn waitForProgress(self: *@This()) !void {
            self.waits += 1;
            try std.testing.expect(self.held.*);
            self.parent.mutex.unlock();
            self.held.* = false;
        }
    };
    var budget = Budget{ .parent = &parent, .held = &parent_held, .expired = true };
    var adapter = read_admission.Adapter(*Budget){ .budget = &budget };
    const payload = adapter.payload();
    const port = read_admission.Port.init(&payload);
    try std.testing.expectError(error.CatalogRoutingSnapshotTimeout, cache.finishRetirementsWithAdmission(port));
    try std.testing.expectEqual(bytes, cache.resource_accounted_bytes);
    budget.expired = false;
    try cache.finishRetirementsWithAdmission(port);
    try std.testing.expectEqual(@as(usize, 1), budget.waits);
    try std.testing.expectEqual(@as(u64, 0), child.snapshot().memory.used_bytes);
    try std.testing.expectEqual(@as(u64, 0), parent.snapshot().memory.used_bytes);
    try std.testing.expectEqual(@as(u64, 0), cache.resource_retirement_debt);
    try std.testing.expectEqual(@as(u64, 0), cache.transferred_retirement_debt.load(.acquire));
    try cache.finishRetirementsWithAdmission(port);
}

test "cache read admission expired retired pin token transfers charge and refuses a live pin" {
    var manager = resource_manager_mod.ResourceManager.init(.{ .identity_allocator = std.testing.allocator });
    defer manager.deinit(std.testing.allocator);
    var cache = Cache.init(std.testing.allocator, 1024 * 1024);
    defer cache.deinit();
    cache.attachResourceManager(&manager);
    var handle = try cache.putRunTableBlock("token-pin-debt", 1, 1, 0, 3, try std.testing.allocator.dupe(u8, "raw"));
    const bytes: u64 = @intCast(handle.entry.byte_cost);
    cache.invalidatePath("token-pin-debt");
    var token = handle.takeReleaseOwnership();
    defer token.finishBlocking();
    try std.testing.expect(!token.transferRetiredAccountingToCache());
    try std.testing.expect(token.handle != null);
    const Budget = struct {
        expired: bool = false,
        pub fn checkpoint(self: *@This()) !void {
            if (self.expired) return error.GenerationPublicationChanged;
        }
        pub fn waitForProgress(self: *@This()) !void {
            self.expired = true;
        }
    };
    var budget = Budget{};
    var adapter = read_admission.Adapter(*Budget){ .budget = &budget };
    const payload = adapter.payload();
    const port = read_admission.Port.init(&payload);
    try std.testing.expect(cache.resource_accounting_mutex.tryLock());
    try std.testing.expectError(error.GenerationPublicationChanged, token.finishWithAdmission(port));
    try std.testing.expectEqual(@as(usize, 0), cache.entryCount());
    try std.testing.expect(token.transferRetiredAccountingToCache());
    try std.testing.expect(token.isFinished());
    try std.testing.expect(token.transferRetiredAccountingToCache());
    try std.testing.expectEqual(bytes, cache.transferred_retirement_debt.load(.acquire));
    cache.resource_accounting_mutex.unlock();
    try std.testing.expectEqual(bytes, manager.snapshot().memory.used_bytes);
    budget.expired = false;
    try cache.finishRetirementsWithAdmission(port);
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
}

test "cache read admission transferred pin debt reclaim returns only reconciled parent bytes" {
    var parent = resource_manager_mod.ResourceManager.init(.{ .identity_allocator = std.testing.allocator });
    defer parent.deinit(std.testing.allocator);
    var child = resource_manager_mod.ResourceManager.init(.{ .identity_allocator = std.testing.allocator });
    defer child.deinit(std.testing.allocator);
    try child.installMemoryParent(parent.memoryParentPort());
    var cache = Cache.init(std.testing.allocator, 1024 * 1024);
    defer cache.deinit();
    cache.attachResourceManager(&child);
    var useful = try cache.putRunTableBlock("useful-transfer", 2, 1, 0, 3, try std.testing.allocator.dupe(u8, "old"));
    useful.release();
    const live_bytes = cache.currentBytes();
    var first = try cache.putRunTableBlock("retired-transfer-a", 1, 1, 0, 3, try std.testing.allocator.dupe(u8, "raw"));
    var second = try cache.putRunTableBlock("retired-transfer-b", 3, 1, 0, 3, try std.testing.allocator.dupe(u8, "raw"));
    const bytes: u64 = @intCast(first.entry.byte_cost + second.entry.byte_cost);
    cache.invalidatePath("retired-transfer-a");
    cache.invalidatePath("retired-transfer-b");
    try std.testing.expect(parent.mutex.tryLock());
    var held = true;
    defer if (held) parent.mutex.unlock();
    var first_token = first.takeReleaseOwnership();
    defer first_token.finishBlocking();
    var second_token = second.takeReleaseOwnership();
    defer second_token.finishBlocking();
    const Foreground = struct {
        pub fn checkpoint(_: *@This()) !void {}
        pub fn waitForProgress(_: *@This()) !void {
            return error.CatalogRoutingSnapshotTimeout;
        }
    };
    var foreground = Foreground{};
    var adapter = read_admission.Adapter(*Foreground){ .budget = &foreground };
    const payload = adapter.payload();
    try std.testing.expect(cache.resource_accounting_mutex.tryLock());
    var accounting_held = true;
    defer if (accounting_held) cache.resource_accounting_mutex.unlock();
    try std.testing.expectError(error.CatalogRoutingSnapshotTimeout, first_token.finishWithAdmission(read_admission.Port.init(&payload)));
    try std.testing.expectError(error.CatalogRoutingSnapshotTimeout, second_token.finishWithAdmission(read_admission.Port.init(&payload)));
    try std.testing.expect(first_token.transferRetiredAccountingToCache());
    try std.testing.expect(second_token.transferRetiredAccountingToCache());
    cache.resource_accounting_mutex.unlock();
    accounting_held = false;
    try std.testing.expectEqual(bytes, cache.transferred_retirement_debt.load(.acquire));
    try std.testing.expectEqual(@as(u64, 0), Cache.reclaimForResourceManager(&cache, bytes));
    try std.testing.expectEqual(bytes, cache.resource_retirement_debt);
    try std.testing.expectEqual(@as(usize, 1), cache.entryCount());
    try std.testing.expectEqual(live_bytes, cache.currentBytes());
    parent.mutex.unlock();
    held = false;
    try std.testing.expectEqual(bytes, Cache.reclaimForResourceManager(&cache, bytes));
    try std.testing.expectEqual(@as(u64, @intCast(live_bytes)), child.snapshot().memory.used_bytes);
    try std.testing.expectEqual(@as(u64, @intCast(live_bytes)), parent.snapshot().memory.used_bytes);
    var still_useful = cache.retainRunTableBlock("useful-transfer", 2, 1, 0, 3).?;
    defer still_useful.release();
    try std.testing.expectEqualStrings("old", still_useful.runTableBlock());
}

test "cache read admission failed retained pin transfers under held shard and all accounting locks" {
    var parent = resource_manager_mod.ResourceManager.init(.{ .identity_allocator = std.testing.allocator });
    defer parent.deinit(std.testing.allocator);
    var child = resource_manager_mod.ResourceManager.init(.{ .identity_allocator = std.testing.allocator });
    defer child.deinit(std.testing.allocator);
    try child.installMemoryParent(parent.memoryParentPort());
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    const allocator = failing.allocator();
    var cache = Cache.init(allocator, 1024 * 1024);
    defer cache.deinit();
    cache.attachResourceManager(&child);
    var handle = try cache.putRunTableBlock("deferred-physical-pin", 1, 1, 0, 3, try allocator.dupe(u8, "raw"));
    const entry = handle.entry;
    const shard = entry.cleanup_shard.?;
    const bytes: u64 = @intCast(entry.byte_cost);
    cache.invalidatePath("deferred-physical-pin");
    const allocated = failing.allocated_bytes;
    const freed = failing.freed_bytes;
    failing.fail_index = failing.alloc_index;
    try std.testing.expect(shard.mutex.tryLock());
    try std.testing.expect(cache.resource_accounting_mutex.tryLock());
    var accounting_held = true;
    defer if (accounting_held) cache.resource_accounting_mutex.unlock();
    try std.testing.expect(child.mutex.tryLock());
    var child_held = true;
    defer if (child_held) child.mutex.unlock();
    try std.testing.expect(parent.mutex.tryLock());
    var shard_held = true;
    var parent_held = true;
    defer if (shard_held) shard.mutex.unlock();
    defer if (parent_held) parent.mutex.unlock();
    handle.releaseForAdmissionFailure();
    try std.testing.expectEqual(allocated, failing.allocated_bytes);
    try std.testing.expectEqual(freed, failing.freed_bytes);
    try std.testing.expect(!failing.has_induced_failure);
    try std.testing.expectEqual(@as(usize, 1), entry.ref_count);
    try std.testing.expectEqual(@as(usize, 1), entry.deferred_ref_releases.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), cache.deferred_pin_releases.load(.acquire));
    try std.testing.expectEqualStrings("raw", entry.value.run_table_block);
    child.mutex.unlock();
    child_held = false;
    cache.resource_accounting_mutex.unlock();
    accounting_held = false;
    const Budget = struct {
        shard: *Cache.Shard,
        parent: *resource_manager_mod.ResourceManager,
        shard_held: *bool,
        parent_held: *bool,
        expired: bool = true,
        waits: usize = 0,
        pub fn checkpoint(self: *@This()) !void {
            if (self.expired) return error.CatalogRoutingSnapshotTimeout;
        }
        pub fn waitForProgress(self: *@This()) !void {
            self.waits += 1;
            if (self.shard_held.*) {
                self.shard.mutex.unlock();
                self.shard_held.* = false;
            } else {
                try std.testing.expect(self.parent_held.*);
                try std.testing.expect(self.shard.mutex.tryLock());
                self.shard.mutex.unlock();
                self.parent.mutex.unlock();
                self.parent_held.* = false;
            }
        }
    };
    var budget = Budget{ .shard = shard, .parent = &parent, .shard_held = &shard_held, .parent_held = &parent_held };
    var adapter = read_admission.Adapter(*Budget){ .budget = &budget };
    const payload = adapter.payload();
    const port = read_admission.Port.init(&payload);
    try std.testing.expectError(error.CatalogRoutingSnapshotTimeout, cache.drainDeferredPinsWithAdmission(port));
    try std.testing.expectEqual(bytes, cache.resource_accounted_bytes);
    try std.testing.expectEqual(@as(usize, 1), cache.entryCount());
    budget.expired = false;
    try cache.drainDeferredPinsWithAdmission(port);
    try std.testing.expectEqual(@as(usize, 2), budget.waits);
    try std.testing.expectEqual(@as(usize, 0), cache.entryCount());
    try std.testing.expectEqual(@as(usize, 0), cache.deferred_pin_releases.load(.acquire));
    try std.testing.expectEqual(@as(u64, 0), child.snapshot().memory.used_bytes);
    try std.testing.expectEqual(@as(u64, 0), parent.snapshot().memory.used_bytes);
}

test "cache read admission deferred pin cursor survives bounded expiry and entry removal" {
    var cache = Cache.init(std.testing.allocator, 1024 * 1024);
    defer cache.deinit();
    const path = "deferred-cursor-pin";
    var handles: [70]Handle = undefined;
    var live: [70]bool = @splat(false);
    defer for (&handles, live) |*handle, active| {
        if (active) handle.release();
    };
    const shard = cache.shardForKey(makeKey(path, 1, 1, .run_table_block, 0, 3));
    var next_id: u64 = 1;
    for (&handles, 0..) |*handle, i| {
        while (cache.shardForKey(makeKey(path, next_id, 1, .run_table_block, 0, 3)) != shard) next_id += 1;
        handle.* = try cache.putRunTableBlock(path, next_id, 1, 0, 3, try std.testing.allocator.dupe(u8, "raw"));
        live[i] = true;
        next_id += 1;
    }
    handles[69].releaseForAdmissionFailure();
    live[69] = false;
    const Budget = struct {
        cache: *Cache,
        reject_wait: bool,
        checks: usize = 0,
        pub fn checkpoint(self: *@This()) !void {
            self.checks += 1;
        }
        pub fn waitForProgress(self: *@This()) !void {
            for (self.cache.shards) |*part| {
                try std.testing.expect(part.mutex.tryLock());
                part.mutex.unlock();
            }
            if (self.reject_wait) return error.GenerationPublicationChanged;
        }
    };
    var original = Budget{ .cache = &cache, .reject_wait = true };
    var original_adapter = read_admission.Adapter(*Budget){ .budget = &original };
    const original_payload = original_adapter.payload();
    try std.testing.expectError(error.GenerationPublicationChanged, cache.drainDeferredPinsWithAdmission(read_admission.Port.init(&original_payload)));
    try std.testing.expect(original.checks >= 64);
    try std.testing.expect(shard.pin_cleanup_cursor == handles[64].entry);
    cache.invalidatePath(path);
    handles[64].release();
    live[64] = false;
    const original_checks = original.checks;
    var cleanup = Budget{ .cache = &cache, .reject_wait = false };
    var cleanup_adapter = read_admission.Adapter(*Budget){ .budget = &cleanup };
    const cleanup_payload = cleanup_adapter.payload();
    try cache.drainDeferredPinsWithAdmission(read_admission.Port.init(&cleanup_payload));
    try std.testing.expectEqual(original_checks, original.checks);
    try std.testing.expectEqual(@as(usize, 0), cache.deferred_pin_releases.load(.acquire));
    try std.testing.expectEqual(@as(usize, 68), cache.entryCount());
}

fn drainDeferredPinsForTest(cache: *Cache) !void {
    const Budget = struct {
        cache: *Cache,
        checks: usize = 0,
        pub fn checkpoint(self: *@This()) !void {
            self.checks += 1;
            if (self.checks > 4096) return error.TestCleanupTimeout;
        }
        pub fn waitForProgress(self: *@This()) !void {
            for (self.cache.shards) |*shard| {
                try std.testing.expect(shard.mutex.tryLock());
                shard.mutex.unlock();
            }
            try self.checkpoint();
        }
    };
    var budget = Budget{ .cache = cache };
    var adapter = read_admission.Adapter(*Budget){ .budget = &budget };
    const payload = adapter.payload();
    try cache.drainDeferredPinsWithAdmission(read_admission.Port.init(&payload));
}

test "cache read admission deferred pin publication window retries through unlocked finite scope" {
    var manager = resource_manager_mod.ResourceManager.init(.{ .identity_allocator = std.testing.allocator });
    defer manager.deinit(std.testing.allocator);
    var cache = Cache.init(std.testing.allocator, 1024 * 1024);
    defer cache.deinit();
    cache.attachResourceManager(&manager);
    var handle = try cache.putRunTableBlock("publishing-pin", 1, 1, 0, 3, try std.testing.allocator.dupe(u8, "raw"));
    const entry = handle.entry;
    const shard = entry.cleanup_shard.?;
    const bytes = cache.resource_accounted_bytes;
    cache.invalidatePath("publishing-pin");
    // Pause one producer after announcing its pin but before final entry
    // publication. The old physical reference remains authoritative here.
    _ = cache.deferred_pin_releases.fetchAdd(1, .release);
    _ = shard.deferred_pin_releases.fetchAdd(1, .release);
    handle = undefined;
    const Budget = struct {
        cache: *Cache,
        entry: *Cache.Entry,
        bytes: u64,
        waits: usize = 0,
        checks: usize = 0,
        published: bool = false,
        pub fn checkpoint(self: *@This()) !void {
            self.checks += 1;
            if (self.checks > 256) return error.TestCleanupTimeout;
        }
        pub fn waitForProgress(self: *@This()) !void {
            try std.testing.expect(!self.published);
            for (self.cache.shards) |*part| {
                try std.testing.expect(part.mutex.tryLock());
                part.mutex.unlock();
            }
            try std.testing.expectEqual(self.bytes, self.cache.resource_accounted_bytes);
            try std.testing.expectEqual(@as(usize, 1), self.entry.ref_count);
            try std.testing.expectEqualStrings("raw", self.entry.value.run_table_block);
            self.waits += 1;
            self.published = true;
            // Final physical entry access by this paused producer.
            _ = self.entry.deferred_ref_releases.fetchAdd(1, .release);
        }
    };
    var budget = Budget{ .cache = &cache, .entry = entry, .bytes = bytes };
    var adapter = read_admission.Adapter(*Budget){ .budget = &budget };
    const payload = adapter.payload();
    try cache.drainDeferredPinsWithAdmission(read_admission.Port.init(&payload));
    try std.testing.expectEqual(@as(usize, 1), budget.waits);
    try std.testing.expectEqual(@as(usize, 0), cache.deferred_pin_releases.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), shard.deferred_pin_releases.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), cache.entryCount());
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
}

test "cache read admission deferred invalidated pin survives ledger failure and independent retry" {
    var manager = resource_manager_mod.ResourceManager.init(.{ .identity_allocator = std.testing.allocator });
    defer manager.deinit(std.testing.allocator);
    var cache = Cache.init(std.testing.allocator, 1024 * 1024);
    defer cache.deinit();
    cache.attachResourceManager(&manager);
    var handle = try cache.putRunTableBlock("deferred-ledger-pin", 1, 1, 0, 3, try std.testing.allocator.dupe(u8, "raw"));
    const entry = handle.entry;
    const shard = entry.cleanup_shard.?;
    const bytes = cache.resource_accounted_bytes;
    cache.invalidatePath("deferred-ledger-pin");
    handle.releaseForAdmissionFailure();
    try std.testing.expect(manager.mutex.tryLock());
    var held = true;
    defer if (held) manager.mutex.unlock();
    const Budget = struct {
        expired: bool = false,
        pub fn checkpoint(self: *@This()) !void {
            if (self.expired) return error.GenerationPublicationChanged;
        }
        pub fn waitForProgress(self: *@This()) !void {
            self.expired = true;
        }
    };
    var original = Budget{};
    var original_adapter = read_admission.Adapter(*Budget){ .budget = &original };
    const original_payload = original_adapter.payload();
    try std.testing.expectError(error.GenerationPublicationChanged, cache.drainDeferredPinsWithAdmission(read_admission.Port.init(&original_payload)));
    try std.testing.expectEqual(@as(usize, 1), cache.entryCount());
    try std.testing.expectEqual(@as(usize, 1), cache.deferred_pin_releases.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), shard.deferred_pin_releases.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), entry.deferred_ref_releases.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), entry.ref_count);
    try std.testing.expect(!entry.idle_linked);
    try std.testing.expectEqual(bytes, cache.resource_accounted_bytes);
    try std.testing.expectEqualStrings("raw", entry.value.run_table_block);
    manager.mutex.unlock();
    held = false;
    try std.testing.expectError(error.GenerationPublicationChanged, cache.drainDeferredPinsWithAdmission(read_admission.Port.init(&original_payload)));
    try drainDeferredPinsForTest(&cache);
    try std.testing.expectEqual(@as(usize, 0), cache.entryCount());
    try std.testing.expectEqual(@as(usize, 0), cache.deferred_pin_releases.load(.acquire));
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
}

test "cache read admission sibling budget recovers transferred pins before another cache lookup" {
    const path = "sibling-deferred-pin";
    const bytes: u64 = @intCast(estimateTableBlockCost(path, "raw"));
    var root = resource_manager_mod.ResourceManager.init(.{
        .memory_budget = .{ .hard_limit_bytes = bytes },
        .identity_allocator = std.testing.allocator,
    });
    defer root.deinit(std.testing.allocator);
    var sibling = resource_manager_mod.ResourceManager.init(.{ .identity_allocator = std.testing.allocator });
    defer sibling.deinit(std.testing.allocator);
    try sibling.installMemoryParent(root.memoryParentPort());
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    var cache = Cache.init(failing.allocator(), 1024 * 1024);
    defer cache.deinit();
    cache.attachResourceManager(&root);
    var handle = try cache.putRunTableBlock(path, 1, 1, 0, 3, try failing.allocator().dupe(u8, "raw"));
    try std.testing.expect(handle.isRetained());
    try std.testing.expectEqual(bytes, root.snapshot().memory.used_bytes);
    handle.releaseForAdmissionFailure();
    failing.fail_index = failing.alloc_index;
    const allocated = failing.allocated_bytes;
    const Budget = struct {
        checks: usize = 0,
        pub fn checkpoint(self: *@This()) !void {
            self.checks += 1;
        }
        pub fn waitForProgress(_: *@This()) !void {
            return error.TestUnexpectedResult;
        }
    };
    var budget = Budget{};
    var adapter = read_admission.Adapter(*Budget){ .budget = &budget };
    const payload = adapter.payload();
    var current: u64 = 0;
    defer sibling.observeUsage(.lsm_block_table_cache, &current, 0);
    // No new cache lookup/collector call: ancestor admission must recover the
    // old pin through the real registered bounded cache reclaimer.
    try sibling.adjustUsageWithAdmission(.lsm_block_table_cache, &current, 1, read_admission.Port.init(&payload));
    try std.testing.expect(budget.checks > 0);
    try std.testing.expectEqual(@as(u64, 1), current);
    try std.testing.expectEqual(@as(u64, 1), sibling.snapshot().memory.used_bytes);
    try std.testing.expectEqual(@as(u64, 1), root.snapshot().memory.used_bytes);
    try std.testing.expectEqual(@as(usize, 0), cache.entryCount());
    try std.testing.expectEqual(@as(usize, 0), cache.deferred_pin_releases.load(.acquire));
    try std.testing.expectEqual(allocated, failing.allocated_bytes);
    try std.testing.expect(!failing.has_induced_failure);
}

test "cache read admission deferred pin reclaim skips held locks and reports exact parent capacity" {
    inline for (.{ .cache_accounting, .shard, .manager, .parent }) |phase| {
        var parent = resource_manager_mod.ResourceManager.init(.{ .identity_allocator = std.testing.allocator });
        defer parent.deinit(std.testing.allocator);
        var child = resource_manager_mod.ResourceManager.init(.{ .identity_allocator = std.testing.allocator });
        defer child.deinit(std.testing.allocator);
        try child.installMemoryParent(parent.memoryParentPort());
        var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
        var cache = Cache.init(failing.allocator(), 1024 * 1024);
        defer cache.deinit();
        cache.attachResourceManager(&child);
        var handle = try cache.putRunTableBlock("nonwaiting-deferred-pin", 1, 1, 0, 3, try failing.allocator().dupe(u8, "raw"));
        const entry = handle.entry;
        const shard = entry.cleanup_shard.?;
        const bytes: u64 = @intCast(entry.byte_cost);
        cache.invalidatePath("nonwaiting-deferred-pin");
        handle.releaseForAdmissionFailure();
        const allocated = failing.allocated_bytes;
        failing.fail_index = failing.alloc_index;
        const mutex = switch (phase) {
            .cache_accounting => &cache.resource_accounting_mutex,
            .shard => &shard.mutex,
            .manager => &child.mutex,
            .parent => &parent.mutex,
            else => unreachable,
        };
        try std.testing.expect(mutex.tryLock());
        var held = true;
        defer if (held) mutex.unlock();
        try std.testing.expectEqual(@as(u64, 0), Cache.reclaimForResourceManager(&cache, bytes));
        try std.testing.expectEqual(bytes, cache.resource_accounted_bytes);
        try std.testing.expectEqual(bytes, child.memory.used_bytes);
        try std.testing.expectEqual(bytes, parent.memory.used_bytes);
        if (phase == .parent) {
            try std.testing.expectEqual(@as(usize, 0), cache.entryCount());
            try std.testing.expectEqual(@as(usize, 0), cache.deferred_pin_releases.load(.acquire));
            try std.testing.expectEqual(bytes, cache.resource_retirement_debt);
        } else {
            try std.testing.expectEqual(@as(usize, 1), cache.entryCount());
            try std.testing.expectEqual(@as(usize, 1), cache.deferred_pin_releases.load(.acquire));
            try std.testing.expectEqual(@as(usize, 1), entry.ref_count);
            try std.testing.expectEqual(@as(usize, 1), entry.deferred_ref_releases.load(.acquire));
            try std.testing.expect(!entry.idle_linked);
            try std.testing.expectEqualStrings("raw", entry.value.run_table_block);
        }
        mutex.unlock();
        held = false;
        try std.testing.expectEqual(bytes, Cache.reclaimForResourceManager(&cache, bytes));
        try std.testing.expectEqual(@as(usize, 0), cache.entryCount());
        try std.testing.expectEqual(@as(usize, 0), cache.deferred_pin_releases.load(.acquire));
        try std.testing.expectEqual(@as(u64, 0), child.snapshot().memory.used_bytes);
        try std.testing.expectEqual(@as(u64, 0), parent.snapshot().memory.used_bytes);
        try std.testing.expectEqual(allocated, failing.allocated_bytes);
        try std.testing.expect(!failing.has_induced_failure);
    }
}

test "cache read admission deferred pin reclaim shares the global eight retirement limit" {
    var manager = resource_manager_mod.ResourceManager.init(.{ .identity_allocator = std.testing.allocator });
    defer manager.deinit(std.testing.allocator);
    var cache = Cache.init(std.testing.allocator, 1024 * 1024);
    defer cache.deinit();
    cache.attachResourceManager(&manager);
    for (0..12) |i| {
        var handle = try cache.putRunTableBlock("deferred-reclaim-burst", i, 1, 0, 3, try std.testing.allocator.dupe(u8, "raw"));
        handle.releaseForAdmissionFailure();
    }
    try std.testing.expectEqual(@as(usize, 12), cache.deferred_pin_releases.load(.acquire));
    var passes: usize = 0;
    while (cache.entryCount() != 0) {
        const count = cache.entryCount();
        const bytes = manager.snapshot().memory.used_bytes;
        const reclaimed = Cache.reclaimForResourceManager(&cache, bytes);
        try std.testing.expect(reclaimed > 0);
        try std.testing.expect(count - cache.entryCount() <= Cache.reclaim_retirement_limit);
        try std.testing.expectEqual(bytes - manager.snapshot().memory.used_bytes, reclaimed);
        try std.testing.expectEqual(cache.entryCount(), cache.deferred_pin_releases.load(.acquire));
        passes += 1;
        try std.testing.expect(passes <= 12);
    }
    try std.testing.expectEqual(@as(usize, 2), passes);
    try std.testing.expectEqual(@as(u64, 0), cache.resource_accounted_bytes);
}

test "cache read admission deferred reclaim advances past a pinned prefix across bounded passes" {
    var manager = resource_manager_mod.ResourceManager.init(.{ .identity_allocator = std.testing.allocator });
    defer manager.deinit(std.testing.allocator);
    var cache = Cache.init(std.testing.allocator, 1024 * 1024);
    defer cache.deinit();
    cache.attachResourceManager(&manager);
    const path = "deferred-reclaim-prefix";
    var handles: [70]Handle = undefined;
    var live: [70]bool = @splat(false);
    defer for (&handles, live) |*handle, active| {
        if (active) handle.release();
    };
    const shard = cache.shardForKey(makeKey(path, 1, 1, .run_table_block, 0, 3));
    var next_id: u64 = 1;
    for (&handles, 0..) |*handle, i| {
        while (cache.shardForKey(makeKey(path, next_id, 1, .run_table_block, 0, 3)) != shard) next_id += 1;
        handle.* = try cache.putRunTableBlock(path, next_id, 1, 0, 3, try std.testing.allocator.dupe(u8, "raw"));
        live[i] = true;
        next_id += 1;
    }
    const bytes: u64 = @intCast(handles[69].entry.byte_cost);
    handles[69].releaseForAdmissionFailure();
    live[69] = false;
    try std.testing.expectEqual(@as(u64, 0), Cache.reclaimForResourceManager(&cache, bytes));
    try std.testing.expectEqual(@as(usize, 70), cache.entryCount());
    try std.testing.expect(shard.pin_cleanup_cursor == handles[64].entry);
    try std.testing.expectEqual(bytes, Cache.reclaimForResourceManager(&cache, bytes));
    try std.testing.expectEqual(@as(usize, 69), cache.entryCount());
    try std.testing.expectEqual(@as(usize, 0), cache.deferred_pin_releases.load(.acquire));
    for (handles[0..69]) |handle| try std.testing.expectEqualStrings("raw", handle.runTableBlock());
}

test "cache read admission transient policy and oversized fallback cannot bypass hard parent quota" {
    const path = "hard-transient-quota";
    const cost: u64 = @intCast(estimateTableBlockCost(path, "raw"));
    var parent = resource_manager_mod.ResourceManager.init(.{ .identity_allocator = std.testing.allocator, .memory_budget = .{ .hard_limit_bytes = cost - 1 } });
    defer parent.deinit(std.testing.allocator);
    var child = resource_manager_mod.ResourceManager.init(.{ .identity_allocator = std.testing.allocator });
    defer child.deinit(std.testing.allocator);
    try child.installMemoryParent(parent.memoryParentPort());
    var cache = Cache.init(std.testing.allocator, 1);
    defer cache.deinit();
    cache.attachResourceManager(&child);
    const Budget = struct {
        pub fn checkpoint(_: *@This()) !void {}
        pub fn waitForProgress(_: *@This()) !void {
            return error.TestUnexpectedResult;
        }
    };
    var budget = Budget{};
    var adapter = read_admission.Adapter(*Budget){ .budget = &budget };
    const payload = adapter.payload();
    const port = read_admission.Port.init(&payload);
    try std.testing.expectError(error.ResourceBudgetExceeded, cache.putTransientRunTableBlockWithAdmission(path, 1, 1, 0, 3, try std.testing.allocator.dupe(u8, "raw"), port));
    try std.testing.expectError(error.ResourceBudgetExceeded, cache.putRunTableBlockWithAdmission(path, 1, 1, 0, 3, try std.testing.allocator.dupe(u8, "raw"), port));
    try std.testing.expectEqual(@as(u64, 0), parent.snapshot().memory.used_bytes);
    try std.testing.expectEqual(@as(u64, 0), child.snapshot().memory.used_bytes);
    try std.testing.expectEqual(@as(u64, 0), cache.resource_accounted_bytes);
    try std.testing.expectEqual(@as(usize, 0), cache.charged_transient_entries.load(.acquire));
}

test "cache read admission charged transient duplicates retire physically once and transfer debt under held ledgers" {
    var parent = resource_manager_mod.ResourceManager.init(.{ .identity_allocator = std.testing.allocator });
    defer parent.deinit(std.testing.allocator);
    var child = resource_manager_mod.ResourceManager.init(.{ .identity_allocator = std.testing.allocator });
    defer child.deinit(std.testing.allocator);
    try child.installMemoryParent(parent.memoryParentPort());
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    const allocator = failing.allocator();
    var cache = Cache.init(allocator, 1);
    defer cache.deinit();
    cache.attachResourceManager(&child);
    const Budget = struct {
        checks: usize = 0,
        expired: bool = false,
        pub fn checkpoint(self: *@This()) !void {
            self.checks += 1;
            if (self.expired) return error.Canceled;
        }
        pub fn waitForProgress(_: *@This()) !void {
            return error.TestUnexpectedResult;
        }
    };
    var budget = Budget{};
    var adapter = read_admission.Adapter(*Budget){ .budget = &budget };
    const payload = adapter.payload();
    const port = read_admission.Port.init(&payload);
    var handle = try cache.putTransientRunTableBlockWithAdmission("charged-transient", 1, 1, 0, 3, try allocator.dupe(u8, "raw"), port);
    var duplicate = handle.retain();
    const bytes: u64 = @intCast(handle.entry.byte_cost);
    try std.testing.expect(!handle.isRetained());
    try std.testing.expectEqual(@as(usize, 0), cache.entryCount());
    try std.testing.expectEqual(@as(usize, 0), cache.currentBytes());
    try std.testing.expectEqual(@as(usize, 1), cache.charged_transient_entries.load(.acquire));
    try std.testing.expectEqual(bytes, parent.snapshot().memory.used_bytes);
    const freed = failing.freed_bytes;
    handle.releaseWithAdmission(port);
    try std.testing.expectEqual(freed, failing.freed_bytes);
    try std.testing.expectEqualStrings("raw", duplicate.runTableBlock());
    const checks = budget.checks;
    budget.expired = true;
    failing.fail_index = failing.alloc_index;
    try std.testing.expect(cache.resource_accounting_mutex.tryLock());
    var accounting_held = true;
    defer if (accounting_held) cache.resource_accounting_mutex.unlock();
    try std.testing.expect(child.mutex.tryLock());
    var child_held = true;
    defer if (child_held) child.mutex.unlock();
    try std.testing.expect(parent.mutex.tryLock());
    var parent_held = true;
    defer if (parent_held) parent.mutex.unlock();
    duplicate.releaseWithAdmission(port);
    try std.testing.expectEqual(freed, failing.freed_bytes);
    try std.testing.expect(!failing.has_induced_failure);
    try std.testing.expectEqual(checks, budget.checks);
    try std.testing.expectEqual(@as(usize, 1), cache.charged_transient_entries.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), cache.pending_transient_entries.load(.acquire));
    try std.testing.expectEqual(@as(u64, 0), cache.transferred_retirement_debt.load(.acquire));
    try std.testing.expectEqual(bytes, cache.resource_accounted_bytes);
    child.mutex.unlock();
    child_held = false;
    cache.resource_accounting_mutex.unlock();
    accounting_held = false;
    for (cache.shards, 0..) |*shard, shard_index| {
        for (&shard.transient_cleanup, 0..) |*slot, slot_index| {
            if (slot.state.load(.acquire) == .pending) cache.transient_cleanup_cursor = shard_index * Cache.transient_cleanup_slots + slot_index;
        }
    }
    try std.testing.expectEqual(@as(u64, 0), Cache.reclaimForResourceManager(&cache, bytes));
    try std.testing.expect(failing.freed_bytes > freed);
    try std.testing.expectEqual(@as(usize, 0), cache.charged_transient_entries.load(.acquire));
    parent.mutex.unlock();
    parent_held = false;
    try std.testing.expectEqual(bytes, Cache.reclaimForResourceManager(&cache, bytes));
    try std.testing.expectEqual(@as(u64, 0), parent.snapshot().memory.used_bytes);
    try std.testing.expectEqual(@as(u64, 0), child.snapshot().memory.used_bytes);
    try std.testing.expectEqual(@as(u64, 0), Cache.reclaimForResourceManager(&cache, bytes));
}

test "cache read admission canceled charged transient publication keeps retired bytes owned until independent cleanup" {
    var manager = resource_manager_mod.ResourceManager.init(.{ .identity_allocator = std.testing.allocator });
    defer manager.deinit(std.testing.allocator);
    var cache = Cache.init(std.testing.allocator, 1);
    defer cache.deinit();
    cache.attachResourceManager(&manager);
    const Budget = struct {
        cache: *Cache,
        expired: bool = false,
        pub fn checkpoint(self: *@This()) !void {
            if (self.cache.snapshotStats().run_table_block.transient_serves != 0) self.expired = true;
            if (self.expired) return error.GenerationPublicationChanged;
        }
        pub fn waitForProgress(_: *@This()) !void {
            return error.TestUnexpectedResult;
        }
    };
    var budget = Budget{ .cache = &cache };
    var adapter = read_admission.Adapter(*Budget){ .budget = &budget };
    const payload = adapter.payload();
    const path = "canceled-charged-transient";
    const cost: u64 = @intCast(estimateTableBlockCost(path, "raw"));
    try std.testing.expectError(error.GenerationPublicationChanged, cache.putTransientRunTableBlockWithAdmission(path, 1, 1, 0, 3, try std.testing.allocator.dupe(u8, "raw"), read_admission.Port.init(&payload)));
    try std.testing.expectEqual(@as(usize, 1), cache.charged_transient_entries.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), cache.pending_transient_entries.load(.acquire));
    try std.testing.expectEqual(@as(u64, 0), cache.transferred_retirement_debt.load(.acquire));
    try std.testing.expectEqual(cost, manager.snapshot().memory.used_bytes);
    var reclaimed: u64 = 0;
    for (0..cache.shards.len) |_| {
        reclaimed += Cache.reclaimForResourceManager(&cache, cost);
        if (reclaimed == cost) break;
    }
    try std.testing.expectEqual(cost, reclaimed);
    try std.testing.expectEqual(@as(usize, 0), cache.charged_transient_entries.load(.acquire));
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
    try std.testing.expect(budget.expired);
}

test "cache read admission duplicate publication retires discarded bytes before capacity and transfers debt behind held ledgers" {
    inline for (.{ .cancel, .release }) |mode| {
        var parent = resource_manager_mod.ResourceManager.init(.{ .identity_allocator = std.testing.allocator });
        defer parent.deinit(std.testing.allocator);
        var child = resource_manager_mod.ResourceManager.init(.{ .identity_allocator = std.testing.allocator });
        defer child.deinit(std.testing.allocator);
        try child.installMemoryParent(parent.memoryParentPort());
        var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
        const allocator = failing.allocator();
        var cache = Cache.init(allocator, 1024 * 1024);
        defer cache.deinit();
        cache.attachResourceManager(&child);
        const path = "duplicate-owned-retirement";
        var installed = try cache.putRunTableBlock(path, 1, 1, 0, 3, try allocator.dupe(u8, "old"));
        defer installed.release();
        const bytes: u64 = @intCast(installed.entry.byte_cost);
        const Budget = struct {
            cache: *Cache,
            child: *resource_manager_mod.ResourceManager,
            parent: *resource_manager_mod.ResourceManager,
            failing: *std.testing.FailingAllocator,
            entry: *Cache.Entry,
            before: u64,
            armed: bool = false,
            held: bool = false,
            waits: usize = 0,
            fn unlock(self: *@This()) void {
                if (!self.held) return;
                self.parent.mutex.unlock();
                self.child.mutex.unlock();
                self.cache.resource_accounting_mutex.unlock();
                self.held = false;
            }
            pub fn checkpoint(self: *@This()) !void {
                if (!self.armed and self.cache.resource_accounted_bytes > self.before) {
                    try std.testing.expect(self.cache.resource_accounting_mutex.tryLock());
                    try std.testing.expect(self.child.mutex.tryLock());
                    try std.testing.expect(self.parent.mutex.tryLock());
                    self.held = true;
                    self.armed = true;
                    self.failing.fail_index = self.failing.alloc_index;
                }
                if (mode == .cancel and self.entry.ref_count > 1) return error.GenerationPublicationChanged;
            }
            pub fn waitForProgress(self: *@This()) !void {
                self.waits += 1;
                try std.testing.expect(self.held);
                // Publication has unlocked every shard before its remaining
                // foreground pressure check can wait for these ledgers.
                for (self.cache.shards) |*shard| {
                    try std.testing.expect(shard.mutex.tryLock());
                    shard.mutex.unlock();
                }
                try std.testing.expectEqual(self.before, self.cache.transferred_retirement_debt.load(.acquire));
                self.unlock();
            }
        };
        var budget = Budget{ .cache = &cache, .child = &child, .parent = &parent, .failing = &failing, .entry = installed.entry, .before = bytes };
        defer budget.unlock();
        var adapter = read_admission.Adapter(*Budget){ .budget = &budget };
        const payload = adapter.payload();
        const port = read_admission.Port.init(&payload);
        const input = try allocator.dupe(u8, "new");
        const freed = failing.freed_bytes;
        const result = cache.putRunTableBlockWithAdmission(path, 1, 1, 0, 3, input, port);
        if (mode == .cancel) {
            try std.testing.expectError(error.GenerationPublicationChanged, result);
            try std.testing.expect(budget.held);
            try std.testing.expectEqual(@as(usize, 0), budget.waits);
            try std.testing.expectEqual(bytes, cache.transferred_retirement_debt.load(.acquire));
            try std.testing.expectEqual(bytes * 2, cache.resource_accounted_bytes);
        } else {
            var returned = try result;
            try std.testing.expectEqualStrings("old", returned.runTableBlock());
            returned.releaseWithAdmission(port);
            try std.testing.expectEqual(@as(usize, 1), budget.waits);
        }
        budget.unlock();
        try std.testing.expect(failing.freed_bytes > freed);
        try std.testing.expect(!failing.has_induced_failure);
        try std.testing.expectEqualStrings("old", installed.runTableBlock());
        try drainDeferredPinsForTest(&cache);
        try std.testing.expectEqual(@as(usize, 1), installed.entry.ref_count);
        try std.testing.expectEqual(@as(usize, 1), cache.entryCount());
        try std.testing.expectEqual(bytes, cache.resource_accounted_bytes);
        try std.testing.expectEqual(bytes, child.snapshot().memory.used_bytes);
        try std.testing.expectEqual(bytes, parent.snapshot().memory.used_bytes);
        try std.testing.expectEqual(@as(u64, 0), cache.transferred_retirement_debt.load(.acquire));
    }
}

test "cache read admission publication rollback owns physical input before debt and never borrows a failed scope" {
    inline for (.{ .quota, .path_oom, .entry_oom, .map_oom, .cancel }) |mode| {
        const path = "owned-publication-rollback";
        const bytes: u64 = @intCast(estimateTableBlockCost(path, "raw"));
        var parent = resource_manager_mod.ResourceManager.init(.{
            .identity_allocator = std.testing.allocator,
            .memory_budget = .{ .hard_limit_bytes = if (mode == .quota) bytes - 1 else 0 },
        });
        defer parent.deinit(std.testing.allocator);
        var child = resource_manager_mod.ResourceManager.init(.{ .identity_allocator = std.testing.allocator });
        defer child.deinit(std.testing.allocator);
        try child.installMemoryParent(parent.memoryParentPort());
        var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
        const allocator = failing.allocator();
        var cache = Cache.init(allocator, 1024 * 1024);
        defer cache.deinit();
        cache.attachResourceManager(&child);
        const Budget = struct {
            cache: *Cache,
            child: *resource_manager_mod.ResourceManager,
            parent: *resource_manager_mod.ResourceManager,
            checks: usize = 0,
            expired: bool = false,
            accounting_held: bool = false,
            child_held: bool = false,
            parent_held: bool = false,
            fn unlock(self: *@This()) void {
                if (self.parent_held) self.parent.mutex.unlock();
                if (self.child_held) self.child.mutex.unlock();
                if (self.accounting_held) self.cache.resource_accounting_mutex.unlock();
                self.parent_held = false;
                self.child_held = false;
                self.accounting_held = false;
            }
            pub fn checkpoint(self: *@This()) !void {
                self.checks += 1;
                if (self.expired) return error.GenerationPublicationChanged;
                if (mode == .cancel and self.cache.resource_accounted_bytes != 0) {
                    self.accounting_held = self.cache.resource_accounting_mutex.tryLock();
                    try std.testing.expect(self.accounting_held);
                    self.child_held = self.child.mutex.tryLock();
                    try std.testing.expect(self.child_held);
                    self.parent_held = self.parent.mutex.tryLock();
                    try std.testing.expect(self.parent_held);
                    self.expired = true;
                    return error.GenerationPublicationChanged;
                }
            }
            pub fn waitForProgress(_: *@This()) !void {
                return error.TestUnexpectedResult;
            }
        };
        var budget = Budget{ .cache = &cache, .child = &child, .parent = &parent };
        defer budget.unlock();
        var adapter = read_admission.Adapter(*Budget){ .budget = &budget };
        const payload = adapter.payload();
        const input = try allocator.dupe(u8, "raw");
        const freed = failing.freed_bytes;
        const allocations = failing.alloc_index;
        if (mode == .quota or mode == .path_oom) failing.fail_index = allocations;
        if (mode == .entry_oom) failing.fail_index = allocations + 1;
        if (mode == .map_oom) failing.fail_index = allocations + 2;
        const result = cache.putRunTableBlockWithAdmission(path, 1, 1, 0, 3, input, read_admission.Port.init(&payload));
        try std.testing.expectError(if (mode == .quota) error.ResourceBudgetExceeded else if (mode == .cancel) error.GenerationPublicationChanged else error.OutOfMemory, result);
        try std.testing.expect(failing.freed_bytes >= freed + 3);
        try std.testing.expectEqual(mode != .quota and mode != .cancel, failing.has_induced_failure);
        if (mode == .quota) try std.testing.expectEqual(allocations, failing.alloc_index);
        const failed_checks = budget.checks;
        try std.testing.expectEqual(@as(usize, 0), cache.entryCount());
        try std.testing.expectEqual(@as(usize, 0), cache.currentBytes());
        const debt: u64 = if (mode == .quota) 0 else bytes;
        try std.testing.expectEqual(debt, cache.resource_accounted_bytes);
        try std.testing.expectEqual(debt, cache.transferred_retirement_debt.load(.acquire));
        if (mode == .cancel) {
            try std.testing.expect(budget.accounting_held and budget.child_held and budget.parent_held);
            try std.testing.expectEqual(@as(u64, 0), Cache.reclaimForResourceManager(&cache, bytes));
        }
        budget.unlock();
        try std.testing.expectEqual(debt, child.snapshot().memory.used_bytes);
        try std.testing.expectEqual(debt, parent.snapshot().memory.used_bytes);
        try drainDeferredPinsForTest(&cache);
        try std.testing.expectEqual(failed_checks, budget.checks);
        try std.testing.expectEqual(@as(u64, 0), cache.resource_accounted_bytes);
        try std.testing.expectEqual(@as(u64, 0), child.snapshot().memory.used_bytes);
        try std.testing.expectEqual(@as(u64, 0), parent.snapshot().memory.used_bytes);
    }
}

test "cache read admission transient physical cleanup slots bound publication and preserve ownership through finite expiry" {
    const path = "fixed-transient-cleanup";
    const bytes: u64 = @intCast(estimateTableBlockCost(path, "raw"));
    var parent = resource_manager_mod.ResourceManager.init(.{ .identity_allocator = std.testing.allocator, .memory_budget = .{ .hard_limit_bytes = bytes * Cache.transient_cleanup_slots } });
    defer parent.deinit(std.testing.allocator);
    var child = resource_manager_mod.ResourceManager.init(.{ .identity_allocator = std.testing.allocator });
    defer child.deinit(std.testing.allocator);
    try child.installMemoryParent(parent.memoryParentPort());
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    const allocator = failing.allocator();
    var cache = Cache.init(allocator, 1);
    defer cache.deinit();
    cache.attachResourceManager(&child);
    var run_id: u64 = 1;
    while (cache.shardForKey(makeKey(path, run_id, 1, .run_table_block, 0, 3)) != &cache.shards[0]) run_id += 1;
    const Budget = struct {
        checks: usize = 0,
        fail_at: usize = std.math.maxInt(usize),
        pub fn checkpoint(self: *@This()) !void {
            self.checks += 1;
            if (self.checks >= self.fail_at) return error.GenerationPublicationChanged;
        }
        pub fn waitForProgress(_: *@This()) !void {
            return error.TestUnexpectedResult;
        }
    };
    var budget = Budget{};
    var adapter = read_admission.Adapter(*Budget){ .budget = &budget };
    const payload = adapter.payload();
    const port = read_admission.Port.init(&payload);
    var handles: [Cache.transient_cleanup_slots]Handle = undefined;
    var live: [Cache.transient_cleanup_slots]bool = @splat(false);
    defer for (&handles, live) |*handle, active| {
        if (active) handle.release();
    };
    for (&handles, 0..) |*handle, i| {
        handle.* = try cache.putTransientRunTableBlockWithAdmission(path, run_id, 1, 0, 3, try allocator.dupe(u8, "raw"), port);
        live[i] = true;
    }
    const overflow_input = try allocator.dupe(u8, "raw");
    const allocations = failing.alloc_index;
    failing.fail_index = allocations;
    try std.testing.expectError(error.ResourceBudgetExceeded, cache.putTransientRunTableBlockWithAdmission(path, run_id, 1, 0, 3, overflow_input, port));
    try std.testing.expectEqual(allocations, failing.alloc_index);
    try std.testing.expect(!failing.has_induced_failure);
    const freed = failing.freed_bytes;
    const checks = budget.checks;
    budget.fail_at = checks;
    try std.testing.expect(cache.shards[0].mutex.tryLock());
    var shard_held = true;
    defer if (shard_held) cache.shards[0].mutex.unlock();
    try std.testing.expect(cache.resource_accounting_mutex.tryLock());
    var accounting_held = true;
    defer if (accounting_held) cache.resource_accounting_mutex.unlock();
    try std.testing.expect(child.mutex.tryLock());
    var child_held = true;
    defer if (child_held) child.mutex.unlock();
    try std.testing.expect(parent.mutex.tryLock());
    var parent_held = true;
    defer if (parent_held) parent.mutex.unlock();
    for (&handles, 0..) |*handle, i| {
        handle.releaseWithAdmission(port);
        live[i] = false;
    }
    try std.testing.expectEqual(checks, budget.checks);
    try std.testing.expectEqual(freed, failing.freed_bytes);
    try std.testing.expectEqual(@as(usize, Cache.transient_cleanup_slots), cache.pending_transient_entries.load(.acquire));
    try std.testing.expectEqual(@as(usize, Cache.transient_cleanup_slots), cache.charged_transient_entries.load(.acquire));
    try std.testing.expectEqual(@as(u64, 0), cache.transferred_retirement_debt.load(.acquire));
    try std.testing.expectEqualStrings("raw", cache.shards[0].transient_cleanup[0].entry.?.value.run_table_block);
    parent.mutex.unlock();
    parent_held = false;
    child.mutex.unlock();
    child_held = false;
    cache.resource_accounting_mutex.unlock();
    accounting_held = false;
    cache.shards[0].mutex.unlock();
    shard_held = false;
    // Scope expiry after exactly two physical retirements preserves all other
    // slots and keeps even the retired contribution charged until cleanup.
    var partial = Budget{ .fail_at = 5 };
    var partial_adapter = read_admission.Adapter(*Budget){ .budget = &partial };
    const partial_payload = partial_adapter.payload();
    try std.testing.expectError(error.GenerationPublicationChanged, cache.drainTransientCleanupWithAdmission(read_admission.Port.init(&partial_payload)));
    try std.testing.expectEqual(@as(usize, Cache.transient_cleanup_slots - 2), cache.pending_transient_entries.load(.acquire));
    try std.testing.expectEqual(bytes * 2, cache.transferred_retirement_debt.load(.acquire));
    try std.testing.expectEqual(bytes * Cache.transient_cleanup_slots, cache.resource_accounted_bytes);
    try std.testing.expectEqual(checks, budget.checks);
    var passes: usize = 0;
    while (cache.pending_transient_entries.load(.acquire) != 0) {
        const before = cache.charged_transient_entries.load(.acquire);
        const charged = cache.resource_accounted_bytes;
        const reclaimed = Cache.reclaimForResourceManager(&cache, charged);
        const after = cache.charged_transient_entries.load(.acquire);
        try std.testing.expect(before - after <= Cache.reclaim_retirement_limit);
        try std.testing.expectEqual(charged - cache.resource_accounted_bytes, reclaimed);
        passes += 1;
        try std.testing.expect(passes <= 4);
    }
    try std.testing.expectEqual(@as(usize, 4), passes);
    failing.fail_index = std.math.maxInt(usize);
    var retry_budget = Budget{};
    var retry_adapter = read_admission.Adapter(*Budget){ .budget = &retry_budget };
    const retry_payload = retry_adapter.payload();
    const retry_port = read_admission.Port.init(&retry_payload);
    var retry = try cache.putTransientRunTableBlockWithAdmission(path, run_id, 1, 0, 3, try allocator.dupe(u8, "raw"), retry_port);
    try std.testing.expectEqualStrings("raw", retry.runTableBlock());
    retry.releaseWithAdmission(retry_port);
    try cache.drainTransientCleanupWithAdmission(retry_port);
    try std.testing.expectEqual(@as(u64, 0), child.snapshot().memory.used_bytes);
    try std.testing.expectEqual(@as(u64, 0), parent.snapshot().memory.used_bytes);
    try std.testing.expectEqual(checks, budget.checks);
    try std.testing.expect(!failing.has_induced_failure);
}

test "cache read admission transient pressure reclaim advances a bounded cursor to a distant slot" {
    var manager = resource_manager_mod.ResourceManager.init(.{ .identity_allocator = std.testing.allocator });
    defer manager.deinit(std.testing.allocator);
    var cache = Cache.init(std.testing.allocator, 1);
    defer cache.deinit();
    cache.attachResourceManager(&manager);
    const path = "distant-transient-cleanup";
    var run_id: u64 = 1;
    while (cache.shardForKey(makeKey(path, run_id, 1, .run_table_block, 0, 3)) != &cache.shards[cache.shards.len - 1]) run_id += 1;
    const Budget = struct {
        checks: usize = 0,
        pub fn checkpoint(self: *@This()) !void {
            self.checks += 1;
        }
        pub fn waitForProgress(_: *@This()) !void {
            return error.TestUnexpectedResult;
        }
    };
    var budget = Budget{};
    var adapter = read_admission.Adapter(*Budget){ .budget = &budget };
    const payload = adapter.payload();
    var handle = try cache.putTransientRunTableBlockWithAdmission(path, run_id, 1, 0, 3, try std.testing.allocator.dupe(u8, "raw"), read_admission.Port.init(&payload));
    const bytes: u64 = @intCast(handle.entry.byte_cost);
    handle.releaseToCache();
    const checks = budget.checks;
    try std.testing.expectEqual(@as(u64, 0), Cache.reclaimForResourceManager(&cache, bytes));
    try std.testing.expectEqual(Cache.reclaim_inspection_limit, cache.transient_cleanup_cursor);
    try std.testing.expectEqual(@as(usize, 1), cache.pending_transient_entries.load(.acquire));
    try std.testing.expectEqual(bytes, manager.snapshot().memory.used_bytes);
    var reclaimed: u64 = 0;
    for (0..cache.shards.len) |_| {
        reclaimed += Cache.reclaimForResourceManager(&cache, bytes);
        if (reclaimed == bytes) break;
    }
    try std.testing.expectEqual(bytes, reclaimed);
    try std.testing.expectEqual(@as(usize, 0), cache.pending_transient_entries.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), cache.charged_transient_entries.load(.acquire));
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
    try std.testing.expectEqual(checks, budget.checks);
}

test "cache read admission transient cleanup publication window waits only through independent unlocked scope" {
    var manager = resource_manager_mod.ResourceManager.init(.{ .identity_allocator = std.testing.allocator });
    defer manager.deinit(std.testing.allocator);
    var cache = Cache.init(std.testing.allocator, 1);
    defer cache.deinit();
    cache.attachResourceManager(&manager);
    const Budget = struct {
        cache: *Cache,
        manager: *resource_manager_mod.ResourceManager,
        slot: ?*Cache.TransientCleanup = null,
        expired: bool = false,
        checks: usize = 0,
        waits: usize = 0,
        pub fn checkpoint(self: *@This()) !void {
            self.checks += 1;
            if (self.expired) return error.GenerationPublicationChanged;
        }
        pub fn waitForProgress(self: *@This()) !void {
            self.waits += 1;
            try std.testing.expectEqual(@as(usize, 1), self.waits);
            for (self.cache.shards) |*shard| {
                try std.testing.expect(shard.mutex.tryLock());
                shard.mutex.unlock();
            }
            try std.testing.expect(self.cache.resource_accounting_mutex.tryLock());
            self.cache.resource_accounting_mutex.unlock();
            try std.testing.expect(self.manager.mutex.tryLock());
            self.manager.mutex.unlock();
            const slot = self.slot.?;
            try std.testing.expect(slot.state.load(.acquire) == .reserved);
            try std.testing.expectEqualStrings("raw", slot.entry.?.value.run_table_block);
            try std.testing.expect(self.manager.snapshot().memory.used_bytes != 0);
            slot.state.store(.pending, .release);
        }
    };
    var original = Budget{ .cache = &cache, .manager = &manager };
    var adapter = read_admission.Adapter(*Budget){ .budget = &original };
    const payload = adapter.payload();
    var handle = try cache.putTransientRunTableBlockWithAdmission("transient-publication-window", 1, 1, 0, 3, try std.testing.allocator.dupe(u8, "raw"), read_admission.Port.init(&payload));
    const slot = handle.entry.transient_cleanup.?;
    const bytes: u64 = @intCast(handle.entry.byte_cost);
    // Stop the last return between announcing its pending owner and the final
    // slot publication. The reserved slot, rather than a stack handle, owns it.
    handle.entry.transient_ref_count.store(0, .release);
    _ = cache.pending_transient_entries.fetchAdd(1, .release);
    handle = undefined;
    defer if (slot.state.load(.acquire) == .reserved) slot.state.store(.pending, .release);
    original.expired = true;
    try std.testing.expectError(error.GenerationPublicationChanged, cache.drainTransientCleanupWithAdmission(read_admission.Port.init(&payload)));
    const checks = original.checks;
    try std.testing.expectEqual(@as(usize, 0), original.waits);
    try std.testing.expectEqual(bytes, manager.snapshot().memory.used_bytes);
    var fresh = Budget{ .cache = &cache, .manager = &manager, .slot = slot };
    var fresh_adapter = read_admission.Adapter(*Budget){ .budget = &fresh };
    const fresh_payload = fresh_adapter.payload();
    try cache.drainTransientCleanupWithAdmission(read_admission.Port.init(&fresh_payload));
    try std.testing.expectEqual(@as(usize, 1), fresh.waits);
    try std.testing.expect(slot.state.load(.acquire) == .free and slot.entry == null);
    try std.testing.expectEqual(@as(usize, 0), cache.pending_transient_entries.load(.acquire));
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
    try std.testing.expectEqual(checks, original.checks);
}

test "cache read admission raw input reserves before allocation and transfers exact publication ownership" {
    const storage_io = @import("storage_io.zig");
    const Mode = enum { success, quota, cancel_before_read, cancel_after_read, read_error, changed_size };
    const path = "preadmitted-raw-input";
    const cost: u64 = @intCast(estimateRawTableCost(path, "raw"));
    const Fixture = struct {
        mode: Mode,
        cache: *Cache,
        manager: *resource_manager_mod.ResourceManager,
        expected_charge: u64,
        canceled: bool = false,
        reads: usize = 0,
        allocations: usize = 0,
        pub fn checkpoint(self: *@This()) !void {
            if (self.mode == .cancel_before_read and self.cache.resource_accounted_bytes != 0 and self.reads == 0) self.canceled = true;
            if (self.canceled) return error.Canceled;
        }
        pub fn waitForProgress(_: *@This()) !void {
            return error.TestUnexpectedResult;
        }
        fn size(_: *anyopaque, _: []const u8) !u64 {
            return 3;
        }
        fn read(raw: *anyopaque, alloc: std.mem.Allocator, _: []const u8, maximum: usize) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.reads += 1;
            try std.testing.expectEqual(@as(usize, 3), maximum);
            try std.testing.expectEqual(self.expected_charge, self.manager.snapshot().memory.used_bytes);
            if (self.mode == .read_error) return error.InputOutput;
            const bytes = try alloc.dupe(u8, if (self.mode == .changed_size) "ra" else "raw");
            self.allocations += 1;
            if (self.mode == .cancel_after_read) self.canceled = true;
            return bytes;
        }
    };
    for (std.enums.values(Mode)) |mode| {
        var manager = resource_manager_mod.ResourceManager.init(.{ .identity_allocator = std.testing.allocator, .memory_budget = .{ .hard_limit_bytes = if (mode == .quota) cost - 1 else 0 } });
        defer manager.deinit(std.testing.allocator);
        var cache = Cache.init(std.testing.allocator, 1024 * 1024);
        cache.attachResourceManager(&manager);
        var cache_live = true;
        defer if (cache_live) cache.deinit();
        var fixture = Fixture{ .mode = mode, .cache = &cache, .manager = &manager, .expected_charge = cost };
        var adapter = read_admission.Adapter(*Fixture){ .budget = &fixture };
        const payload = adapter.payload();
        const control = read_admission.Port.init(&payload);
        // Only these two callbacks are reached by this physical input fixture.
        var vtable: storage_io.Storage.VTable = undefined;
        // Leave unreachable required callbacks undefined, but initialize every
        // optional callback so compatibility reads use their intended fallback.
        inline for (comptime std.meta.fieldNames(storage_io.Storage.VTable)) |name| {
            if (@typeInfo(@FieldType(storage_io.Storage.VTable, name)) == .optional) @field(vtable, name) = null;
        }
        vtable.file_size = Fixture.size;
        vtable.read_file_alloc = Fixture.read;
        const storage = storage_io.Storage{ .ptr = &fixture, .vtable = &vtable };
        const result = cache.readRunTableRawWithAdmission(storage, path, 1, 1, 100, control);
        if (mode == .success) {
            var handle = try result;
            defer handle.release();
            try std.testing.expectEqualStrings("raw", handle.runTableRaw());
            try std.testing.expectEqual(cost, manager.snapshot().memory.used_bytes);
            fixture.expected_charge = 2 * cost;
            var duplicate = try cache.readRunTableRawWithAdmission(storage, path, 1, 1, 100, control);
            defer duplicate.release();
            try std.testing.expect(duplicate.entry == handle.entry);
            try cache.finishRetirementsWithAdmission(control);
            try std.testing.expectEqual(cost, manager.snapshot().memory.used_bytes);
            try std.testing.expectEqual(@as(usize, 2), fixture.reads);
            try std.testing.expectEqual(@as(usize, 2), fixture.allocations);
        } else {
            try std.testing.expectError(switch (mode) {
                .quota => error.ResourceBudgetExceeded,
                .cancel_before_read, .cancel_after_read => error.Canceled,
                .read_error => error.InputOutput,
                .changed_size => error.EndOfStream,
                .success => unreachable,
            }, result);
            try std.testing.expectEqual(@as(usize, if (mode == .quota or mode == .cancel_before_read) 0 else 1), fixture.reads);
            try std.testing.expectEqual(@as(usize, if (mode == .cancel_after_read or mode == .changed_size) 1 else 0), fixture.allocations);
            try std.testing.expectEqual(@as(usize, 0), cache.entryCount());
            if (mode != .quota) {
                // The failed scope is not borrowed for rollback/reconciliation.
                fixture.canceled = false;
                fixture.mode = .success;
                try cache.finishRetirementsWithAdmission(control);
            }
            try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
        }
        cache.deinit();
        cache_live = false;
        try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
    }
}

test "cache read admission decoded index allocation preserves quota expiry abandonment and publication ownership" {
    const path = "preadmitted-index";
    const entries = [_]lsm_table_file.Entry{
        .{ .namespace_name = "ns", .key = "a", .value = "first" },
        .{ .namespace_name = "ns", .key = "b", .value = "second" },
    };
    const raw = try lsm_table_file.encodeAlloc(std.testing.allocator, &entries);
    defer std.testing.allocator.free(raw);
    const metadata: u64 = @intCast(@sizeOf(Cache.Entry) + path.len);
    const Mode = enum { success, quota, partial_expiry, abandon, missing_scope, publication_expiry };
    const Budget = struct {
        cache: *Cache,
        metadata: u64,
        mode: Mode,
        expired: bool = false,
        checks: usize = 0,
        pub fn checkpoint(self: *@This()) !void {
            self.checks += 1;
            // Same-thread observation avoids entering the manager lock from
            // a checkpoint called while its admission mutation is in progress.
            if (self.mode == .partial_expiry and self.cache.resource_accounted_bytes > self.metadata) self.expired = true;
            if (self.expired) return error.CatalogRoutingSnapshotTimeout;
        }
        pub fn waitForProgress(_: *@This()) !void {
            return error.TestUnexpectedResult;
        }
    };
    for (std.enums.values(Mode)) |mode| {
        var manager = resource_manager_mod.ResourceManager.init(.{ .identity_allocator = std.testing.allocator, .memory_budget = .{ .hard_limit_bytes = if (mode == .quota) metadata else 0 } });
        defer manager.deinit(std.testing.allocator);
        var cache = Cache.init(std.testing.allocator, 1024 * 1024);
        cache.attachResourceManager(&manager);
        var cache_live = true;
        defer if (cache_live) cache.deinit();
        var budget = Budget{ .cache = &cache, .metadata = metadata, .mode = mode };
        var adapter = read_admission.Adapter(*Budget){ .budget = &budget };
        const payload = adapter.payload();
        const control = read_admission.Port.init(&payload);
        const result = cache.decodeRunTableIndexWithAdmission(path, raw, control);
        if (mode == .quota or mode == .partial_expiry) {
            try std.testing.expectError(if (mode == .quota) error.ResourceBudgetExceeded else error.CatalogRoutingSnapshotTimeout, result);
            try std.testing.expectEqual(@as(usize, 0), cache.entryCount());
        } else {
            var decoded = try result;
            defer decoded.deinit();
            const cost: u64 = @intCast(decoded.byte_cost);
            try std.testing.expect(cost > metadata);
            try std.testing.expectEqual(cost, manager.snapshot().memory.used_bytes);
            try std.testing.expectEqual(@as(usize, 2), decoded.index.entryCount());
            if (mode == .abandon) {
                const checks = budget.checks;
                decoded.deinit();
                decoded.deinit();
                try std.testing.expectEqual(checks, budget.checks);
                try std.testing.expectEqual(cost, manager.snapshot().memory.used_bytes);
            } else if (mode == .missing_scope) {
                try std.testing.expectError(error.InvalidArgument, decoded.publish(1, 1, null));
                try std.testing.expect(decoded.cache == &cache);
                try std.testing.expectEqual(cost, manager.snapshot().memory.used_bytes);
                const checks = budget.checks;
                decoded.deinit();
                try std.testing.expectEqual(checks, budget.checks);
            } else if (mode == .publication_expiry) {
                budget.expired = true;
                try std.testing.expectError(error.CatalogRoutingSnapshotTimeout, decoded.publish(1, 1, control));
                const checks = budget.checks;
                decoded.deinit();
                try std.testing.expectEqual(checks, budget.checks);
                try std.testing.expectEqual(@as(usize, 0), cache.entryCount());
            } else {
                var handle = try decoded.publish(1, 1, control);
                defer handle.release();
                try std.testing.expectEqual(cost, manager.snapshot().memory.used_bytes);
                var another = try cache.decodeRunTableIndexWithAdmission(path, raw, control);
                defer another.deinit();
                var duplicate = try another.publish(1, 1, control);
                defer duplicate.release();
                try std.testing.expect(duplicate.entry == handle.entry);
                try cache.finishRetirementsWithAdmission(control);
                try std.testing.expectEqual(cost, manager.snapshot().memory.used_bytes);
                try std.testing.expectEqual(@as(usize, 2), duplicate.runTableIndex().entryCount());
            }
        }
        budget.expired = false;
        budget.mode = .success;
        try cache.finishRetirementsWithAdmission(control);
        cache.deinit();
        cache_live = false;
        try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
    }
}

test "cache read admission decoded index allocation failures retire every physical partial before capacity" {
    const entries = [_]lsm_table_file.Entry{
        .{ .namespace_name = "ns", .key = "a", .value = "first" },
        .{ .namespace_name = "ns", .key = "b", .value = "second" },
    };
    const raw = try lsm_table_file.encodeAlloc(std.testing.allocator, &entries);
    defer std.testing.allocator.free(raw);
    const Fixture = struct {
        fn run(alloc: Allocator, encoded: []const u8) !void {
            var manager = resource_manager_mod.ResourceManager.init(.{ .identity_allocator = std.testing.allocator });
            defer manager.deinit(std.testing.allocator);
            var cache = try Cache.initFallible(alloc, 1024 * 1024);
            cache.attachResourceManager(&manager);
            defer cache.deinit();
            const Budget = struct {
                pub fn checkpoint(_: *@This()) !void {}
                pub fn waitForProgress(_: *@This()) !void {
                    return error.TestUnexpectedResult;
                }
            };
            var budget = Budget{};
            var adapter = read_admission.Adapter(*Budget){ .budget = &budget };
            const payload = adapter.payload();
            const control = read_admission.Port.init(&payload);
            var decoded = cache.decodeRunTableIndexWithAdmission("index-allocation-sweep", encoded, control) catch |err| {
                try cache.finishRetirementsWithAdmission(control);
                try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
                return err;
            };
            defer decoded.deinit();
            var handle = decoded.publish(1, 1, control) catch |err| {
                try cache.finishRetirementsWithAdmission(control);
                try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
                return err;
            };
            defer handle.release();
            try std.testing.expectEqual(@as(usize, 2), handle.runTableIndex().entryCount());
            try std.testing.expectEqual(@as(u64, @intCast(handle.entry.byte_cost)), manager.snapshot().memory.used_bytes);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{raw});
}

test "cache read admission compressed table copy keeps original allocator and charged ownership through expiry" {
    var keys: [64][8]u8 = undefined;
    var entries: [64]lsm_table_file.Entry = undefined;
    const value = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    for (&entries, 0..) |*entry, i| entry.* = .{ .namespace_name = "ns", .key = try std.fmt.bufPrint(&keys[i], "{d:0>4}", .{i}), .value = value };
    const raw = try lsm_table_file.encodeAlloc(std.testing.allocator, &entries);
    defer std.testing.allocator.free(raw);
    var source_index = try lsm_table_file.decodeIndexAlloc(std.testing.allocator, raw);
    defer source_index.deinit(std.testing.allocator);
    try std.testing.expect(lsm_table_file.indexHasCompressedBlocks(&source_index));
    const Mode = enum { success, quota, partial_expiry };
    const Budget = struct {
        cache: *Cache,
        input_bytes: usize,
        mode: Mode,
        expired: bool = false,
        checks: usize = 0,
        pub fn checkpoint(self: *@This()) !void {
            self.checks += 1;
            if (self.mode == .partial_expiry and self.cache.resource_accounted_bytes > self.input_bytes) self.expired = true;
            if (self.expired) return error.CatalogRoutingSnapshotTimeout;
        }
        pub fn waitForProgress(_: *@This()) !void {
            return error.TestUnexpectedResult;
        }
    };
    for (std.enums.values(Mode)) |mode| {
        var manager = resource_manager_mod.ResourceManager.init(.{ .identity_allocator = std.testing.allocator, .memory_budget = .{ .hard_limit_bytes = if (mode == .quota) raw.len - 1 else 0 } });
        defer manager.deinit(std.testing.allocator);
        var cache = Cache.init(std.testing.allocator, 1024 * 1024);
        cache.attachResourceManager(&manager);
        defer cache.deinit();
        var budget = Budget{ .cache = &cache, .input_bytes = raw.len, .mode = mode };
        var adapter = read_admission.Adapter(*Budget){ .budget = &budget };
        const payload = adapter.payload();
        const control = read_admission.Port.init(&payload);
        const result = cache.copyAndDecodeRunTableWithAdmission(std.testing.allocator, raw, control);
        if (mode == .success) {
            var loaded = try result;
            defer loaded.deinit();
            try std.testing.expect(loaded.allocator.ptr == std.testing.allocator.ptr and loaded.allocator.vtable == std.testing.allocator.vtable);
            try std.testing.expectEqual(@as(usize, 64), loaded.table.entryCount());
            for (entries, 0..) |expected, i| {
                const found = try loaded.table.entryAt(i);
                try std.testing.expectEqualStrings(expected.key, found.key);
                try std.testing.expectEqualStrings(expected.value, found.value);
            }
            try cache.finishRetirementsWithAdmission(control);
            try std.testing.expectEqual(@as(u64, @intCast(loaded.byte_cost)), manager.snapshot().memory.used_bytes);
            const charge = manager.snapshot().memory.used_bytes;
            const checks = budget.checks;
            budget.expired = true;
            loaded.deinit();
            loaded.deinit();
            try std.testing.expectEqual(checks, budget.checks);
            try std.testing.expectEqual(charge, manager.snapshot().memory.used_bytes);
        } else {
            try std.testing.expectError(if (mode == .quota) error.ResourceBudgetExceeded else error.CatalogRoutingSnapshotTimeout, result);
            try std.testing.expectEqual(@as(usize, 0), cache.entryCount());
        }
        budget.expired = false;
        budget.mode = .success;
        try cache.finishRetirementsWithAdmission(control);
        try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
    }
}

test "cache read admission compressed table decode allocation failures retire original backing memory and debt" {
    var keys: [32][8]u8 = undefined;
    var entries: [32]lsm_table_file.Entry = undefined;
    const value = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    for (&entries, 0..) |*entry, i| entry.* = .{ .namespace_name = "ns", .key = try std.fmt.bufPrint(&keys[i], "{d:0>4}", .{i}), .value = value };
    const raw = try lsm_table_file.encodeAlloc(std.testing.allocator, &entries);
    defer std.testing.allocator.free(raw);
    const Fixture = struct {
        fn run(backing: Allocator, encoded: []const u8) !void {
            var manager = resource_manager_mod.ResourceManager.init(.{ .identity_allocator = std.testing.allocator });
            defer manager.deinit(std.testing.allocator);
            var cache = Cache.init(std.testing.allocator, 1024 * 1024);
            cache.attachResourceManager(&manager);
            defer cache.deinit();
            const Budget = struct {
                pub fn checkpoint(_: *@This()) !void {}
                pub fn waitForProgress(_: *@This()) !void {
                    return error.TestUnexpectedResult;
                }
            };
            var budget = Budget{};
            var adapter = read_admission.Adapter(*Budget){ .budget = &budget };
            const payload = adapter.payload();
            const control = read_admission.Port.init(&payload);
            var loaded = cache.copyAndDecodeRunTableWithAdmission(backing, encoded, control) catch |err| {
                try cache.finishRetirementsWithAdmission(control);
                try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
                return err;
            };
            defer loaded.deinit();
            try std.testing.expectEqual(@as(usize, 32), loaded.table.entryCount());
            loaded.deinit();
            try cache.finishRetirementsWithAdmission(control);
            try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{raw});
}

test "cache read admission footer index input decode and publication share prepaid ownership" {
    const storage_io = @import("storage_io.zig");
    const path = "admitted-footer-index";
    const entries = [_]lsm_table_file.Entry{
        .{ .namespace_name = "ns", .key = "a", .value = "first" },
        .{ .namespace_name = "ns", .key = "b", .value = "second" },
    };
    const encoded = try lsm_table_file.encodeAlloc(std.testing.allocator, &entries);
    defer std.testing.allocator.free(encoded);
    const metadata = @sizeOf(Cache.Entry) + path.len;
    const Mode = enum { success, quota, metadata_expiry, read_error, invalid_footer };
    const Fixture = struct {
        encoded: []const u8,
        manager: *resource_manager_mod.ResourceManager,
        mode: Mode,
        reads: usize = 0,
        allocated: usize = 0,
        expired: bool = false,
        pub fn checkpoint(self: *@This()) !void {
            if (self.expired) return error.CatalogRoutingSnapshotTimeout;
        }
        pub fn waitForProgress(_: *@This()) !void {
            return error.TestUnexpectedResult;
        }
        fn size(raw: *anyopaque, _: []const u8) !u64 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return self.encoded.len;
        }
        fn range(raw: *anyopaque, alloc: Allocator, _: []const u8, offset: u64, len: usize) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.reads += 1;
            try std.testing.expect(self.manager.snapshot().memory.used_bytes >= metadata);
            if (self.mode == .read_error) return error.InputOutput;
            const start: usize = @intCast(offset);
            const bytes = try alloc.dupe(u8, self.encoded[start..][0..len]);
            self.allocated += 1;
            if (self.mode == .invalid_footer) bytes[0] ^= 0xff;
            if (self.mode == .metadata_expiry and self.reads == 2) self.expired = true;
            return bytes;
        }
    };
    for (std.enums.values(Mode)) |mode| {
        var manager = resource_manager_mod.ResourceManager.init(.{ .identity_allocator = std.testing.allocator, .memory_budget = .{ .hard_limit_bytes = if (mode == .quota) metadata + lsm_table_file.footer_len - 1 else 0 } });
        defer manager.deinit(std.testing.allocator);
        var cache = Cache.init(std.testing.allocator, 1024 * 1024);
        cache.attachResourceManager(&manager);
        defer cache.deinit();
        var fixture = Fixture{ .encoded = encoded, .manager = &manager, .mode = mode };
        var adapter = read_admission.Adapter(*Fixture){ .budget = &fixture };
        const payload = adapter.payload();
        const control = read_admission.Port.init(&payload);
        var vtable: storage_io.Storage.VTable = undefined;
        // Leave unreachable required callbacks undefined, but initialize every
        // optional callback so compatibility reads use their intended fallback.
        inline for (comptime std.meta.fieldNames(storage_io.Storage.VTable)) |name| {
            if (@typeInfo(@FieldType(storage_io.Storage.VTable, name)) == .optional) @field(vtable, name) = null;
        }
        vtable.file_size = Fixture.size;
        vtable.read_file_range_alloc = Fixture.range;
        vtable.read_file_trailer_alloc = null;
        const storage = storage_io.Storage{ .ptr = &fixture, .vtable = &vtable };
        const result = cache.loadRunTableIndexWithAdmission(storage, path, control);
        if (mode == .success) {
            var loaded = try result;
            defer loaded.deinit();
            try std.testing.expectEqual(@as(usize, 2), loaded.index.entryCount());
            try cache.finishRetirementsWithAdmission(control);
            try std.testing.expectEqual(@as(u64, @intCast(loaded.byte_cost)), manager.snapshot().memory.used_bytes);
            var handle = try loaded.publish(1, 1, control);
            defer handle.release();
            try std.testing.expectEqual(@as(usize, 2), handle.runTableIndex().entryCount());
            try std.testing.expectEqual(@as(u64, @intCast(handle.entry.byte_cost)), manager.snapshot().memory.used_bytes);
            try std.testing.expectEqual(@as(usize, 2), fixture.reads);
        } else {
            try std.testing.expectError(switch (mode) {
                .quota => error.ResourceBudgetExceeded,
                .metadata_expiry => error.CatalogRoutingSnapshotTimeout,
                .read_error => error.InputOutput,
                .invalid_footer => error.UnsupportedVersion,
                .success => unreachable,
            }, result);
            try std.testing.expectEqual(@as(usize, if (mode == .metadata_expiry) 2 else 1), fixture.reads);
            if (mode == .quota or mode == .read_error) try std.testing.expectEqual(@as(usize, 0), fixture.allocated);
            fixture.expired = false;
            fixture.mode = .success;
            try cache.finishRetirementsWithAdmission(control);
            try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
            try std.testing.expectEqual(@as(usize, 0), cache.entryCount());
        }
    }
}

test "cache read admission footer index allocation failures retire input and decoded partials" {
    const storage_io = @import("storage_io.zig");
    const entries = [_]lsm_table_file.Entry{
        .{ .namespace_name = "ns", .key = "a", .value = "first" },
        .{ .namespace_name = "ns", .key = "b", .value = "second" },
    };
    const encoded = try lsm_table_file.encodeAlloc(std.testing.allocator, &entries);
    defer std.testing.allocator.free(encoded);
    const Fixture = struct {
        encoded: []const u8,
        pub fn checkpoint(_: *@This()) !void {}
        pub fn waitForProgress(_: *@This()) !void {
            return error.TestUnexpectedResult;
        }
        fn size(raw: *anyopaque, _: []const u8) !u64 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return self.encoded.len;
        }
        fn range(raw: *anyopaque, alloc: Allocator, _: []const u8, offset: u64, len: usize) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const start: usize = @intCast(offset);
            return alloc.dupe(u8, self.encoded[start..][0..len]);
        }
        fn run(alloc: Allocator, bytes: []const u8) !void {
            var manager = resource_manager_mod.ResourceManager.init(.{ .identity_allocator = std.testing.allocator });
            defer manager.deinit(std.testing.allocator);
            var cache = try Cache.initFallible(alloc, 1024 * 1024);
            cache.attachResourceManager(&manager);
            defer cache.deinit();
            var fixture = @This(){ .encoded = bytes };
            var adapter = read_admission.Adapter(*@This()){ .budget = &fixture };
            const payload = adapter.payload();
            const control = read_admission.Port.init(&payload);
            var vtable: storage_io.Storage.VTable = undefined;
            // Leave unreachable required callbacks undefined, but initialize every
            // optional callback so compatibility reads use their intended fallback.
            inline for (comptime std.meta.fieldNames(storage_io.Storage.VTable)) |name| {
                if (@typeInfo(@FieldType(storage_io.Storage.VTable, name)) == .optional) @field(vtable, name) = null;
            }
            vtable.file_size = size;
            vtable.read_file_range_alloc = range;
            vtable.read_file_trailer_alloc = null;
            const storage = storage_io.Storage{ .ptr = &fixture, .vtable = &vtable };
            var loaded = cache.loadRunTableIndexWithAdmission(storage, "footer-failure-sweep", control) catch |err| {
                try cache.finishRetirementsWithAdmission(control);
                try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
                return err;
            };
            defer loaded.deinit();
            var handle = loaded.publish(1, 1, control) catch |err| {
                try cache.finishRetirementsWithAdmission(control);
                try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
                return err;
            };
            defer handle.release();
            try std.testing.expectEqual(@as(usize, 2), handle.runTableIndex().entryCount());
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{encoded});
}

test "cache read admission compressed block input decode and transient publication preserve ownership" {
    var keys: [64][8]u8 = undefined;
    var entries: [64]lsm_table_file.Entry = undefined;
    const value = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    for (&entries, 0..) |*entry, i| entry.* = .{ .namespace_name = "ns", .key = try std.fmt.bufPrint(&keys[i], "{d:0>4}", .{i}), .value = value };
    const encoded = try lsm_table_file.encodeAlloc(std.testing.allocator, &entries);
    defer std.testing.allocator.free(encoded);
    var index = try lsm_table_file.decodeIndexAlloc(std.testing.allocator, encoded);
    defer index.deinit(std.testing.allocator);
    const block = index.blocks[0];
    try std.testing.expect(block.compression != .none);
    const start = index.entry_data_start + block.physicalRelativeOffset();
    const input = encoded[start..][0..block.physicalLen()];
    const expected = try lsm_table_file.decodeBlockPayloadAlloc(std.testing.allocator, block.compression, input, block.len, block.checksum);
    defer std.testing.allocator.free(expected);
    const path = "admitted-compressed-block";
    const metadata = @sizeOf(Cache.Entry) + path.len;
    const Mode = enum { retained, transient, input_quota, decode_quota, input_expiry, decode_expiry, checksum, short_read, read_error, abandon, publication_expiry, missing_scope };
    const Fixture = struct {
        cache: *Cache,
        input: []const u8,
        mode: Mode,
        expired: bool = false,
        reads: usize = 0,
        allocations: usize = 0,
        checks: usize = 0,
        pub fn checkpoint(self: *@This()) !void {
            self.checks += 1;
            if (self.mode == .decode_expiry and self.cache.resource_accounted_bytes > metadata + self.input.len) self.expired = true;
            if (self.expired) return error.CatalogRoutingSnapshotTimeout;
        }
        pub fn waitForProgress(_: *@This()) !void {
            return error.TestUnexpectedResult;
        }
        pub fn readFileRangeAlloc(self: *@This(), alloc: Allocator, _: []const u8, offset: u64, len: usize) ![]u8 {
            self.reads += 1;
            try std.testing.expectEqual(@as(u64, 17), offset);
            try std.testing.expectEqual(self.input.len, len);
            if (self.mode == .read_error) return error.InputOutput;
            const bytes = try alloc.dupe(u8, if (self.mode == .short_read) self.input[0 .. self.input.len - 1] else self.input);
            self.allocations += 1;
            if (self.mode == .input_expiry) self.expired = true;
            return bytes;
        }
    };
    for (std.enums.values(Mode)) |mode| {
        const hard_limit: u64 = switch (mode) {
            .input_quota => metadata + input.len - 1,
            .decode_quota => metadata + input.len + expected.len - 1,
            else => 0,
        };
        var manager = resource_manager_mod.ResourceManager.init(.{ .identity_allocator = std.testing.allocator, .memory_budget = .{ .hard_limit_bytes = hard_limit } });
        defer manager.deinit(std.testing.allocator);
        var cache = Cache.init(std.testing.allocator, 1024 * 1024);
        cache.attachResourceManager(&manager);
        defer cache.deinit();
        var fixture = Fixture{ .cache = &cache, .input = input, .mode = mode };
        var adapter = read_admission.Adapter(*Fixture){ .budget = &fixture };
        const payload = adapter.payload();
        const control = read_admission.Port.init(&payload);
        const result = cache.loadRunTableBlockWithAdmission(&fixture, path, 17, block.physicalLen(), block.compression, block.len, if (mode == .checksum) block.checksum ^ 1 else block.checksum, control);
        switch (mode) {
            .retained, .transient, .abandon, .publication_expiry, .missing_scope => {
                var loaded = try result;
                defer loaded.deinit();
                try std.testing.expectEqualSlices(u8, expected, loaded.bytes);
                try cache.finishRetirementsWithAdmission(control);
                try std.testing.expectEqual(@as(u64, @intCast(loaded.byte_cost)), manager.snapshot().memory.used_bytes);
                if (mode == .missing_scope) {
                    try std.testing.expectError(error.InvalidArgument, loaded.publish(1, 1, true, null));
                    try std.testing.expect(loaded.cache == &cache);
                }
                if (mode == .abandon or mode == .missing_scope) {
                    fixture.expired = true;
                    const checks = fixture.checks;
                    loaded.deinit();
                    loaded.deinit();
                    try std.testing.expectEqual(checks, fixture.checks);
                } else if (mode == .publication_expiry) {
                    fixture.expired = true;
                    try std.testing.expectError(error.CatalogRoutingSnapshotTimeout, loaded.publish(1, 1, true, control));
                } else {
                    var handle = try loaded.publish(1, 1, mode == .retained, control);
                    try std.testing.expectEqualSlices(u8, expected, handle.runTableBlock());
                    try std.testing.expectEqual(@as(u64, @intCast(handle.entry.byte_cost)), manager.snapshot().memory.used_bytes);
                    handle.releaseWithAdmission(control);
                }
            },
            else => {
                try std.testing.expectError(switch (mode) {
                    .input_quota, .decode_quota => error.ResourceBudgetExceeded,
                    .input_expiry, .decode_expiry => error.CatalogRoutingSnapshotTimeout,
                    .checksum => error.TableBlockChecksumMismatch,
                    .short_read => error.EndOfStream,
                    .read_error => error.InputOutput,
                    else => unreachable,
                }, result);
                try std.testing.expectEqual(@as(usize, 1), fixture.reads);
                if (mode == .input_quota or mode == .read_error) try std.testing.expectEqual(@as(usize, 0), fixture.allocations);
            },
        }
        fixture.expired = false;
        fixture.mode = .retained;
        try cache.drainTransientCleanupWithAdmission(control);
        try cache.finishRetirementsWithAdmission(control);
        if (mode != .retained) try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
    }
}

test "cache read admission compressed block allocation failures retire every physical input and decoded partial" {
    var keys: [32][8]u8 = undefined;
    var entries: [32]lsm_table_file.Entry = undefined;
    const value = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    for (&entries, 0..) |*entry, i| entry.* = .{ .namespace_name = "ns", .key = try std.fmt.bufPrint(&keys[i], "{d:0>4}", .{i}), .value = value };
    const encoded = try lsm_table_file.encodeAlloc(std.testing.allocator, &entries);
    defer std.testing.allocator.free(encoded);
    var index = try lsm_table_file.decodeIndexAlloc(std.testing.allocator, encoded);
    defer index.deinit(std.testing.allocator);
    const block = index.blocks[0];
    try std.testing.expect(block.compression != .none);
    const start = index.entry_data_start + block.physicalRelativeOffset();
    const input = encoded[start..][0..block.physicalLen()];
    const Fixture = struct {
        input: []const u8,
        pub fn checkpoint(_: *@This()) !void {}
        pub fn waitForProgress(_: *@This()) !void {
            return error.TestUnexpectedResult;
        }
        pub fn readFileRangeAlloc(self: *@This(), alloc: Allocator, _: []const u8, _: u64, _: usize) ![]u8 {
            return alloc.dupe(u8, self.input);
        }
        fn run(alloc: Allocator, bytes: []const u8, meta: lsm_table_file.TableIndex.BlockMeta) !void {
            var manager = resource_manager_mod.ResourceManager.init(.{ .identity_allocator = std.testing.allocator });
            defer manager.deinit(std.testing.allocator);
            var cache = try Cache.initFallible(alloc, 1024 * 1024);
            cache.attachResourceManager(&manager);
            defer cache.deinit();
            var fixture = @This(){ .input = bytes };
            var adapter = read_admission.Adapter(*@This()){ .budget = &fixture };
            const payload = adapter.payload();
            const control = read_admission.Port.init(&payload);
            var loaded = cache.loadRunTableBlockWithAdmission(&fixture, "compressed-block-failure-sweep", 17, meta.physicalLen(), meta.compression, meta.len, meta.checksum, control) catch |err| {
                try cache.finishRetirementsWithAdmission(control);
                try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
                return err;
            };
            defer loaded.deinit();
            var handle = loaded.publish(1, 1, true, control) catch |err| {
                try cache.finishRetirementsWithAdmission(control);
                try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
                return err;
            };
            defer handle.release();
            try std.testing.expectEqual(@as(usize, meta.len), handle.runTableBlock().len);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{ input, block });
}

test "cache read admission every block codec preserves bytes and original deadline at every checkpoint" {
    const snappy = @import("../../encoding/snappy.zig");
    const hash = @import("antfly_hash").Crc32;
    var keys: [32][96]u8 = undefined;
    var entries: [32]lsm_table_file.Entry = undefined;
    const value = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    for (&entries, 0..) |*entry, i| entry.* = .{ .namespace_name = "ns", .key = try std.fmt.bufPrint(&keys[i], "shared-prefix-for-decoder-qualification/long-identical-key-components/{d:0>4}", .{i}), .value = value };
    const encoded = try lsm_table_file.encodeAlloc(std.testing.allocator, &entries);
    defer std.testing.allocator.free(encoded);
    var index = try lsm_table_file.decodeIndexAlloc(std.testing.allocator, encoded);
    defer index.deinit(std.testing.allocator);
    const block = index.blocks[0];
    const start = index.entry_data_start + block.physicalRelativeOffset();
    const physical = encoded[start..][0..block.physicalLen()];
    const logical = try lsm_table_file.decodeBlockPayloadAlloc(std.testing.allocator, block.compression, physical, block.len, block.checksum);
    defer std.testing.allocator.free(logical);
    const prefix = switch (block.compression) {
        .prefix_snappy => try snappy.decode(std.testing.allocator, physical),
        .prefix => try std.testing.allocator.dupe(u8, physical),
        else => return error.TestUnexpectedResult,
    };
    defer std.testing.allocator.free(prefix);
    const compressed_logical = try snappy.encode(std.testing.allocator, logical);
    defer std.testing.allocator.free(compressed_logical);
    const compressed_prefix = try snappy.encode(std.testing.allocator, prefix);
    defer std.testing.allocator.free(compressed_prefix);
    const Budget = struct {
        calls: usize = 0,
        fail_at: usize = 0,
        pub fn checkpoint(self: *@This()) !void {
            self.calls += 1;
            if (self.calls == self.fail_at) return error.CatalogRoutingSnapshotTimeout;
        }
        pub fn waitForProgress(_: *@This()) !void {
            return error.TestUnexpectedResult;
        }
    };
    for (std.enums.values(lsm_table_file.BlockCompression)) |codec| {
        const input = switch (codec) {
            .none => logical,
            .snappy => compressed_logical,
            .prefix => prefix,
            .prefix_snappy => compressed_prefix,
        };
        var budget = Budget{};
        var adapter = read_admission.Adapter(*Budget){ .budget = &budget };
        const payload = adapter.payload();
        const control = read_admission.Port.init(&payload);
        const decoded = try lsm_table_file.decodeBlockPayloadAllocWithAdmission(std.testing.allocator, codec, input, logical.len, hash.hash(input), control);
        defer std.testing.allocator.free(decoded);
        try std.testing.expectEqualSlices(u8, logical, decoded);
        const total_checks = budget.calls;
        try std.testing.expect(total_checks >= 3);
        for (1..total_checks + 1) |fail_at| {
            budget = .{ .fail_at = fail_at };
            try std.testing.expectError(error.CatalogRoutingSnapshotTimeout, lsm_table_file.decodeBlockPayloadAllocWithAdmission(std.testing.allocator, codec, input, logical.len, hash.hash(input), control));
            try std.testing.expectEqual(fail_at, budget.calls);
        }
        budget = .{};
        try std.testing.expectError(error.TableBlockChecksumMismatch, lsm_table_file.decodeBlockPayloadAllocWithAdmission(std.testing.allocator, codec, input, logical.len, hash.hash(input) ^ 1, control));
    }
}

test "cache read admission raw block crc and copy checkpoint broad literals" {
    const hash = @import("antfly_hash").Crc32;
    const bytes = try std.testing.allocator.alloc(u8, 256 * 1024);
    defer std.testing.allocator.free(bytes);
    for (bytes, 0..) |*byte, i| byte.* = @truncate(i);
    const Budget = struct {
        calls: usize = 0,
        fail_at: usize = 0,
        pub fn checkpoint(self: *@This()) !void {
            self.calls += 1;
            if (self.calls == self.fail_at) return error.CatalogRoutingSnapshotTimeout;
        }
        pub fn waitForProgress(_: *@This()) !void {
            return error.TestUnexpectedResult;
        }
    };
    var budget = Budget{};
    var adapter = read_admission.Adapter(*Budget){ .budget = &budget };
    const payload = adapter.payload();
    const control = read_admission.Port.init(&payload);
    const decoded = try lsm_table_file.decodeBlockPayloadAllocWithAdmission(std.testing.allocator, .none, bytes, bytes.len, hash.hash(bytes), control);
    defer std.testing.allocator.free(decoded);
    try std.testing.expectEqualSlices(u8, bytes, decoded);
    try std.testing.expectEqual(@as(usize, 10), budget.calls);
    for (1..11) |fail_at| {
        budget = .{ .fail_at = fail_at };
        try std.testing.expectError(error.CatalogRoutingSnapshotTimeout, lsm_table_file.decodeBlockPayloadAllocWithAdmission(std.testing.allocator, .none, bytes, bytes.len, hash.hash(bytes), control));
        try std.testing.expectEqual(fail_at, budget.calls);
    }
}

test "cache pending load allocation failure leaves no published entry" {
    const Fixture = struct {
        fn run(a: Allocator) !void {
            var cache = try Cache.initFallible(a, 1024);
            defer cache.deinit();
            cache.beginLoad("run-1", 1, 1, .run_table_index) catch |err| {
                try std.testing.expectEqual(@as(usize, 0), cache.pendingLoadCountForTests());
                return err;
            };
            cache.finishLoad("run-1", 1, 1, .run_table_index);
            try std.testing.expectEqual(@as(usize, 0), cache.pendingLoadCountForTests());
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "lsm cache hot prefix promotion elects one concurrent owner and bounds expansion" {
    if (@import("builtin").single_threaded) return error.SkipZigTest;
    const a = std.testing.allocator;
    var cache = Cache.init(a, 1024 * 1024);
    defer cache.deinit();
    var handle = try cache.putRunTablePhysicalBlock("promotion", 1, 1, 0, 1, try a.dupe(u8, "x"));
    defer handle.release();
    try std.testing.expect(!handle.claimPointBlockPromotion(64 * 1024 + 1));
    cache.max_bytes = 1024;
    try std.testing.expect(!handle.claimPointBlockPromotion(4096));
    cache.max_bytes = 1024 * 1024;
    const Worker = struct {
        handle: *const Handle,
        winners: *std.atomic.Value(usize),
        fn run(self: @This()) void {
            for (0..64) |_| if (self.handle.claimPointBlockPromotion(4096)) {
                _ = self.winners.fetchAdd(1, .monotonic);
            };
        }
    };
    var winners = std.atomic.Value(usize).init(0);
    var threads: [4]std.Thread = undefined;
    var spawned: usize = 0;
    defer for (threads[0..spawned]) |thread| thread.join();
    for (&threads) |*thread| {
        thread.* = try std.Thread.spawn(.{}, Worker.run, .{Worker{ .handle = &handle, .winners = &winners }});
        spawned += 1;
    }
    for (threads[0..spawned]) |thread| thread.join();
    spawned = 0;
    try std.testing.expectEqual(@as(usize, 1), winners.load(.monotonic));
    try std.testing.expect(!handle.claimPointBlockPromotion(4096));
    handle.finishPointBlockPromotion();
    for (0..255) |_| try std.testing.expect(!handle.claimPointBlockPromotion(4096));
    try std.testing.expect(handle.claimPointBlockPromotion(4096));
    handle.finishPointBlockPromotion();
    var transient = try cache.putTransientRunTablePhysicalBlock("transient", 2, 1, 0, 1, try a.dupe(u8, "x"));
    defer transient.release();
    for (0..16) |_| try std.testing.expect(!transient.claimPointBlockPromotion(4096));
}

test "lsm cache optional load gate skips busy owners and mutexes" {
    const a = std.testing.allocator;
    var cache = try Cache.initFallible(a, 1024 * 1024);
    defer cache.deinit();
    try std.testing.expect(try cache.tryBeginLoadWithBlock("optional", 1, 1, .run_table_block, 10, 20));
    try std.testing.expect(!try cache.tryBeginLoadWithBlock("optional", 1, 1, .run_table_block, 10, 20));
    cache.finishLoadWithBlock("optional", 1, 1, .run_table_block, 10, 20);
    const shard = cache.shardForKey(makeKey("optional", 1, 1, .run_table_block, 10, 20));
    shard.pending_sync.lock();
    const busy = cache.tryBeginLoadWithBlock("optional", 1, 1, .run_table_block, 10, 20);
    shard.pending_sync.unlock();
    try std.testing.expect(!try busy);
    try std.testing.expectEqual(@as(u64, 0), cache.snapshotStats().run_table_block.waits);
    const Fixture = struct {
        fn run(alloc: Allocator) !void {
            var c = try Cache.initFallible(alloc, 1024 * 1024);
            defer c.deinit();
            const started = c.tryBeginLoadWithBlock("optional", 1, 1, .run_table_block, 10, 20) catch |err| {
                for (c.shards) |*part| try std.testing.expectEqual(@as(usize, 0), part.pending_loads.count());
                return err;
            };
            try std.testing.expect(started);
            c.finishLoadWithBlock("optional", 1, 1, .run_table_block, 10, 20);
        }
    };
    try std.testing.checkAllAllocationFailures(a, Fixture.run, .{});
}

test "lsm cache construction handoff cleans duplicate winners and allocation failures" {
    const a = std.testing.allocator;
    const Fixture = struct {
        fn run(alloc: Allocator) !void {
            var manager = resource_manager_mod.ResourceManager.init(.{ .identity_allocator = std.testing.allocator });
            defer manager.deinit(std.testing.allocator);
            defer std.debug.assert(manager.snapshot().memory.used_bytes == 0 and manager.snapshot().memory.accounting_errors == 0);
            var cache = try Cache.initFallible(alloc, 1024 * 1024);
            defer cache.deinit();
            cache.attachResourceManager(&manager);
            for (0..2) |_| {
                var credit = try manager.reserveWithoutReclaim(.lsm_read_working_set, 1024);
                defer credit.release();
                const bytes = try alloc.alloc(u8, 1024);
                @memset(bytes, 17);
                var handle = try cache.putRunTableBlockWithCredit("handoff", 1, 0, 0, 1024, bytes, &credit);
                defer handle.release();
                try std.testing.expect(handle.isRetained());
                try std.testing.expectEqual(@as(u64, 0), credit.bytes);
                try std.testing.expectEqual(@as(u8, 17), handle.runTableBlock()[0]);
                try std.testing.expectEqual(@as(u64, @intCast(cache.currentBytes())), manager.sliceStats(.lsm_block_table_cache).used_bytes);
            }
            try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.lsm_read_working_set).used_bytes);
        }
    };
    try std.testing.checkAllAllocationFailures(a, Fixture.run, .{});
}
