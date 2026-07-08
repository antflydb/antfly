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

const std = @import("std");
const block = @import("block.zig");
const manager_mod = @import("manager.zig");
const pool_mod = @import("pool.zig");
const storage_runtime_mod = @import("storage_runtime.zig");

pub const Config = struct {
    enabled: bool = false,
    max_bytes: usize = 512 * 1024 * 1024,
    min_tokens: usize = 64,
    ttl_ms: u64 = 300_000,
};

pub const Stats = struct {
    hits: u64 = 0,
    misses: u64 = 0,
    evictions: u64 = 0,
    cached_tokens: u64 = 0,
    live_entries: usize = 0,
    live_bytes: usize = 0,
};

const Entry = struct {
    namespace: []u8,
    tokens: []i64,
    blocks: []block.KvBlockId,
    storage_blocks: []block.KvBlockId,
    estimated_bytes: usize,
    expires_at_ms: i64,
    last_used: u64,
};

pub const AttachedPrefix = struct {
    sequence_id: manager_mod.SequenceId,
    token_count: usize,
};

pub const StorageEnsureResult = struct {
    storage: *storage_runtime_mod.KvStorageRuntime,
    created: bool,
};

pub const PromptPrefixCache = struct {
    allocator: std.mem.Allocator,
    config: Config = .{},
    manager: manager_mod.KvManager,
    storage: ?storage_runtime_mod.KvStorageRuntime = null,
    pool_id: ?block.KvPoolId = null,
    pool_config: ?pool_mod.KvPoolConfig = null,
    entries: std.ArrayListUnmanaged(Entry) = .empty,
    estimated_bytes: usize = 0,
    tick: u64 = 0,
    hits: u64 = 0,
    misses: u64 = 0,
    evictions: u64 = 0,

    pub fn init(allocator: std.mem.Allocator) PromptPrefixCache {
        return .{
            .allocator = allocator,
            .manager = manager_mod.KvManager.init(allocator),
        };
    }

    pub fn deinit(self: *PromptPrefixCache) void {
        self.clearEntries();
        self.entries.deinit(self.allocator);
        if (self.storage) |*storage| storage.deinit();
        self.manager.deinit();
    }

    pub fn configure(self: *PromptPrefixCache, config: Config) void {
        self.config = config;
        self.evictToBudget();
    }

    pub fn ensurePool(self: *PromptPrefixCache, config: pool_mod.KvPoolConfig) !?block.KvPoolId {
        if (!self.config.enabled) return null;
        if (self.pool_config) |existing| {
            if (!existing.compatible(config)) return null;
            return self.pool_id;
        }
        const id = try self.manager.addPool(config);
        self.pool_id = id;
        self.pool_config = config;
        return id;
    }

    pub fn ensureStorage(self: *PromptPrefixCache, config: pool_mod.KvPoolConfig) !?StorageEnsureResult {
        _ = (try self.ensurePool(config)) orelse return null;
        if (self.pool_config) |existing| {
            if (!existing.compatible(config)) return null;
        }
        if (self.storage) |*storage| {
            return .{ .storage = storage, .created = false };
        }
        self.storage = try storage_runtime_mod.KvStorageRuntime.init(self.allocator, config);
        return .{ .storage = &self.storage.?, .created = true };
    }

    pub fn managerPtr(self: *PromptPrefixCache) *manager_mod.KvManager {
        return &self.manager;
    }

    pub fn storagePtr(self: *PromptPrefixCache) ?*storage_runtime_mod.KvStorageRuntime {
        if (self.storage) |*storage| return storage;
        return null;
    }

    pub fn pageSize(self: *const PromptPrefixCache) ?usize {
        const cfg = self.pool_config orelse return null;
        return cfg.page_size_tokens;
    }

    pub fn attachLongestPrefix(
        self: *PromptPrefixCache,
        namespace: []const u8,
        prompt_tokens: []const i64,
        max_prefix_tokens: usize,
    ) !?AttachedPrefix {
        if (!self.config.enabled or self.pool_id == null) return null;
        const page_size = self.pageSize() orelse return null;
        const limit = (max_prefix_tokens / page_size) * page_size;
        if (limit < page_size) {
            self.misses += 1;
            return null;
        }

        self.expireOld();
        var best_idx: ?usize = null;
        var best_tokens: usize = 0;
        for (self.entries.items, 0..) |entry, idx| {
            if (!std.mem.eql(u8, entry.namespace, namespace)) continue;
            const matched = commonPrefixTokens(entry.tokens, prompt_tokens);
            const usable = @min((matched / page_size) * page_size, limit);
            if (usable > best_tokens) {
                best_idx = idx;
                best_tokens = usable;
            }
        }
        const idx = best_idx orelse {
            self.misses += 1;
            return null;
        };
        if (best_tokens == 0) {
            self.misses += 1;
            return null;
        }

        const block_count = best_tokens / page_size;
        const sequence_id = try self.manager.attachSequenceWithRetainedBlocks(self.pool_id.?, self.entries.items[idx].blocks[0..block_count], best_tokens);
        errdefer self.manager.releaseSequence(sequence_id) catch {};
        if (self.storage) |*storage| {
            if (self.entries.items[idx].storage_blocks.len < block_count) return error.InvalidPagedKvState;
            const storage_sequence_id = try storage.attachSequenceWithRetainedBlocks(storage.poolId(), self.entries.items[idx].storage_blocks[0..block_count], best_tokens);
            errdefer storage.releaseSequence(storage_sequence_id) catch {};
            if (storage_sequence_id != sequence_id) return error.InvalidPagedKvState;
        }

        self.tick += 1;
        self.entries.items[idx].last_used = self.tick;
        self.hits += 1;
        return .{
            .sequence_id = sequence_id,
            .token_count = best_tokens,
        };
    }

    pub fn storeFromSequence(
        self: *PromptPrefixCache,
        namespace: []const u8,
        prompt_tokens: []const i64,
        sequence_id: manager_mod.SequenceId,
    ) !void {
        if (!self.config.enabled or self.pool_id == null) return;
        const page_size = self.pageSize() orelse return;
        const cacheable_tokens = (prompt_tokens.len / page_size) * page_size;
        if (cacheable_tokens < self.config.min_tokens) return;

        const tokens = prompt_tokens[0..cacheable_tokens];
        if (self.findExact(namespace, tokens)) |idx| {
            self.tick += 1;
            self.entries.items[idx].last_used = self.tick;
            return;
        }

        var blocks: std.ArrayListUnmanaged(block.KvBlockId) = .empty;
        defer blocks.deinit(self.allocator);
        try self.manager.retainSequencePrefixBlocks(sequence_id, cacheable_tokens, &blocks);
        errdefer self.manager.releaseRetainedBlocks(self.pool_id.?, blocks.items);

        var storage_blocks: std.ArrayListUnmanaged(block.KvBlockId) = .empty;
        defer storage_blocks.deinit(self.allocator);
        errdefer if (self.storage) |*storage| storage.releaseRetainedBlocks(storage_blocks.items);
        if (self.storage) |*storage| {
            try storage.retainSequencePrefixBlocks(sequence_id, cacheable_tokens, &storage_blocks);
        }

        const owned_namespace = try self.allocator.dupe(u8, namespace);
        errdefer self.allocator.free(owned_namespace);
        const owned_tokens = try self.allocator.dupe(i64, tokens);
        errdefer self.allocator.free(owned_tokens);
        const owned_blocks = try blocks.toOwnedSlice(self.allocator);
        errdefer self.manager.releaseRetainedBlocks(self.pool_id.?, owned_blocks);
        errdefer self.allocator.free(owned_blocks);
        const owned_storage_blocks = try storage_blocks.toOwnedSlice(self.allocator);
        errdefer if (self.storage) |*storage| storage.releaseRetainedBlocks(owned_storage_blocks);
        errdefer if (owned_storage_blocks.len > 0) self.allocator.free(owned_storage_blocks);

        self.tick += 1;
        const bytes = self.estimateBytes(cacheable_tokens);
        try self.entries.append(self.allocator, .{
            .namespace = owned_namespace,
            .tokens = owned_tokens,
            .blocks = owned_blocks,
            .storage_blocks = owned_storage_blocks,
            .estimated_bytes = bytes,
            .expires_at_ms = nowMs() + @as(i64, @intCast(self.config.ttl_ms)),
            .last_used = self.tick,
        });
        self.estimated_bytes += bytes;
        self.evictToBudget();
    }

    pub fn stats(self: *const PromptPrefixCache) Stats {
        var cached_tokens: u64 = 0;
        for (self.entries.items) |entry| cached_tokens += @intCast(entry.tokens.len);
        return .{
            .hits = self.hits,
            .misses = self.misses,
            .evictions = self.evictions,
            .cached_tokens = cached_tokens,
            .live_entries = self.entries.items.len,
            .live_bytes = self.estimated_bytes,
        };
    }

    fn findExact(self: *const PromptPrefixCache, namespace: []const u8, tokens: []const i64) ?usize {
        for (self.entries.items, 0..) |entry, idx| {
            if (std.mem.eql(u8, entry.namespace, namespace) and std.mem.eql(i64, entry.tokens, tokens)) return idx;
        }
        return null;
    }

    fn estimateBytes(self: *const PromptPrefixCache, token_count: usize) usize {
        const cfg = self.pool_config orelse return token_count * @sizeOf(i64);
        return token_count * cfg.num_layers_packed * cfg.bytesPerTokenPair();
    }

    fn expireOld(self: *PromptPrefixCache) void {
        const now = nowMs();
        var idx: usize = 0;
        while (idx < self.entries.items.len) {
            if (now > self.entries.items[idx].expires_at_ms) {
                self.removeEntry(idx);
                continue;
            }
            idx += 1;
        }
    }

    fn evictToBudget(self: *PromptPrefixCache) void {
        // ponytail: O(n) LRU scan, replace with an indexed queue if cache sizes get large.
        while (self.estimated_bytes > self.config.max_bytes and self.entries.items.len > 0) {
            var victim: usize = 0;
            for (self.entries.items, 0..) |entry, idx| {
                if (entry.last_used < self.entries.items[victim].last_used) victim = idx;
            }
            self.removeEntry(victim);
        }
    }

    fn clearEntries(self: *PromptPrefixCache) void {
        var idx: usize = self.entries.items.len;
        while (idx > 0) {
            idx -= 1;
            self.removeEntry(idx);
        }
    }

    fn removeEntry(self: *PromptPrefixCache, idx: usize) void {
        const entry = self.entries.items[idx];
        if (self.pool_id) |pool_id| self.manager.releaseRetainedBlocks(pool_id, entry.blocks);
        if (self.storage) |*storage| storage.releaseRetainedBlocks(entry.storage_blocks);
        self.allocator.free(entry.namespace);
        self.allocator.free(entry.tokens);
        self.allocator.free(entry.blocks);
        if (entry.storage_blocks.len > 0) self.allocator.free(entry.storage_blocks);
        self.estimated_bytes -|= entry.estimated_bytes;
        _ = self.entries.swapRemove(idx);
        self.evictions += 1;
    }
};

fn commonPrefixTokens(a: []const i64, b: []const i64) usize {
    const limit = @min(a.len, b.len);
    var idx: usize = 0;
    while (idx < limit and a[idx] == b[idx]) : (idx += 1) {}
    return idx;
}

fn nowMs() i64 {
    var ts: std.posix.timespec = undefined;
    switch (std.posix.errno(std.posix.system.clock_gettime(.MONOTONIC, &ts))) {
        .SUCCESS => return @intCast((@as(i128, ts.sec) * std.time.ms_per_s) + @divTrunc(ts.nsec, std.time.ns_per_ms)),
        else => return 0,
    }
}

test "prompt cache attaches longest retained prefix" {
    const allocator = std.testing.allocator;
    var cache = PromptPrefixCache.init(allocator);
    defer cache.deinit();
    cache.configure(.{ .enabled = true, .min_tokens = 2, .max_bytes = 1 << 20 });

    const pool_id = (try cache.ensurePool(.{
        .backend = .native,
        .dtype = .f32,
        .page_size_tokens = 2,
        .num_layers_packed = 1,
        .num_kv_heads = 1,
        .head_dim = 2,
    })).?;
    const source_id = try cache.manager.attachSequence(pool_id);
    try cache.manager.appendTokens(source_id, 4);
    try cache.storeFromSequence("agent", &.{ 1, 2, 3, 4 }, source_id);

    const hit = (try cache.attachLongestPrefix("agent", &.{ 1, 2, 3, 9 }, 2)).?;
    try std.testing.expectEqual(@as(usize, 2), hit.token_count);
    try cache.manager.releaseSequence(hit.sequence_id);

    const stats_value = cache.stats();
    try std.testing.expectEqual(@as(u64, 1), stats_value.hits);
}

test "prompt cache evicts retained blocks by budget" {
    const allocator = std.testing.allocator;
    var cache = PromptPrefixCache.init(allocator);
    defer cache.deinit();
    cache.configure(.{ .enabled = true, .min_tokens = 2, .max_bytes = 1 });

    const pool_id = (try cache.ensurePool(.{
        .backend = .native,
        .dtype = .f32,
        .page_size_tokens = 2,
        .num_layers_packed = 1,
        .num_kv_heads = 1,
        .head_dim = 2,
    })).?;
    const source_id = try cache.manager.attachSequence(pool_id);
    try cache.manager.appendTokens(source_id, 2);
    try cache.storeFromSequence("", &.{ 1, 2 }, source_id);

    try std.testing.expectEqual(@as(usize, 0), cache.stats().live_entries);
}

test "prompt cache attaches longest retained prefix with storage runtime" {
    const allocator = std.testing.allocator;
    var cache = PromptPrefixCache.init(allocator);
    defer cache.deinit();
    cache.configure(.{ .enabled = true, .min_tokens = 2, .max_bytes = 1 << 20 });

    const ensured = (try cache.ensureStorage(.{
        .backend = .metal,
        .dtype = .f32,
        .page_size_tokens = 2,
        .num_layers_packed = 1,
        .num_kv_heads = 1,
        .head_dim = 2,
    })).?;
    const storage = ensured.storage;
    const pool_id = cache.pool_id.?;

    const source_id = try cache.manager.attachSequence(pool_id);
    try cache.manager.appendTokens(source_id, 4);
    const storage_source_id = try storage.attachSequence(storage.poolId());
    try std.testing.expectEqual(source_id, storage_source_id);
    try storage.appendTokens(storage_source_id, 4);

    try cache.storeFromSequence("metal", &.{ 1, 2, 3, 4 }, source_id);
    const hit = (try cache.attachLongestPrefix("metal", &.{ 1, 2, 3, 9 }, 2)).?;
    try std.testing.expectEqual(@as(usize, 2), hit.token_count);
    try std.testing.expectEqual(@as(?usize, 2), storage.tokenCount(hit.sequence_id));

    try cache.manager.releaseSequence(hit.sequence_id);
    try storage.releaseSequence(hit.sequence_id);
}
