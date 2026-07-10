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

pub const max_namespace_bytes: usize = 256;

pub const Mode = enum {
    simple,
    block_hash,
};

pub const Config = struct {
    enabled: bool = false,
    mode: Mode = .simple,
    max_bytes: usize = 512 * 1024 * 1024,
    min_tokens: usize = 64,
    ttl_ms: u64 = 300_000,
    resource_usage_observer: ?ResourceUsageObserver = null,
};

pub const ResourceUsageObserver = struct {
    context: *anyopaque,
    update: *const fn (context: *anyopaque, current: *u64, next: u64) void,
};

pub const Stats = struct {
    hits: u64 = 0,
    misses: u64 = 0,
    evictions: u64 = 0,
    cached_tokens: u64 = 0,
    live_entries: usize = 0,
    live_bytes: usize = 0,
    block_hash_hits: u64 = 0,
    block_hash_misses: u64 = 0,
    block_hash_evictions: u64 = 0,
    block_hash_cached_blocks: usize = 0,
    block_hash_collision_guards: u64 = 0,
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

const Hash = [std.crypto.hash.sha2.Sha256.digest_length]u8;

fn spinLock(m: *std.atomic.Mutex) void {
    while (!m.tryLock()) {
        std.atomic.spinLoopHint();
    }
}

const BlockHashEntry = struct {
    hash: Hash,
    tokens: []i64,
    block_id: block.KvBlockId,
    storage_block_id: ?block.KvBlockId,
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
    /// Serializes cache mutation (attach/store/configure/evict) against
    /// concurrent stats() reads from the metrics endpoint.
    mutex: std.atomic.Mutex = .unlocked,
    config: Config = .{},
    manager: manager_mod.KvManager,
    storage: ?storage_runtime_mod.KvStorageRuntime = null,
    pool_id: ?block.KvPoolId = null,
    pool_config: ?pool_mod.KvPoolConfig = null,
    entries: std.ArrayListUnmanaged(Entry) = .empty,
    block_hash_entries: std.ArrayListUnmanaged(BlockHashEntry) = .empty,
    block_hash_index: std.AutoHashMapUnmanaged(Hash, usize) = .empty,
    estimated_bytes: usize = 0,
    tick: u64 = 0,
    hits: u64 = 0,
    misses: u64 = 0,
    evictions: u64 = 0,
    block_hash_hits: u64 = 0,
    block_hash_misses: u64 = 0,
    block_hash_evictions: u64 = 0,
    block_hash_collision_guards: u64 = 0,
    resource_accounted_bytes: u64 = 0,

    pub fn init(allocator: std.mem.Allocator) PromptPrefixCache {
        return .{
            .allocator = allocator,
            .manager = manager_mod.KvManager.init(allocator),
        };
    }

    pub fn deinit(self: *PromptPrefixCache) void {
        self.clearEntries();
        self.entries.deinit(self.allocator);
        self.block_hash_entries.deinit(self.allocator);
        self.block_hash_index.deinit(self.allocator);
        if (self.storage) |*storage| storage.deinit();
        self.manager.deinit();
    }

    pub fn configure(self: *PromptPrefixCache, config: Config) void {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        if (self.config.resource_usage_observer) |old_observer| {
            const same_observer = if (config.resource_usage_observer) |new_observer|
                old_observer.context == new_observer.context and old_observer.update == new_observer.update
            else
                false;
            if (!same_observer) old_observer.update(old_observer.context, &self.resource_accounted_bytes, 0);
        }
        self.config = config;
        self.evictToBudget();
        self.updateResourceUsage();
    }

    pub fn ensurePool(self: *PromptPrefixCache, config: pool_mod.KvPoolConfig) !?block.KvPoolId {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        return self.ensurePoolLocked(config);
    }

    fn ensurePoolLocked(self: *PromptPrefixCache, config: pool_mod.KvPoolConfig) !?block.KvPoolId {
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
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        _ = (try self.ensurePoolLocked(config)) orelse return null;
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
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        if (!self.config.enabled or self.pool_id == null) return null;
        if (self.config.mode == .block_hash) {
            return try self.attachLongestBlockHashPrefix(namespace, prompt_tokens, max_prefix_tokens);
        }
        if (namespace.len > max_namespace_bytes) {
            self.misses += 1;
            return null;
        }
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
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        if (!self.config.enabled or self.pool_id == null) return;
        if (self.config.mode == .block_hash) {
            return try self.storeBlockHashFromSequence(namespace, prompt_tokens, sequence_id);
        }
        if (namespace.len > max_namespace_bytes) return;
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
        errdefer {
            self.manager.releaseRetainedBlocks(self.pool_id.?, owned_blocks);
            self.allocator.free(owned_blocks);
        }
        const owned_storage_blocks = try storage_blocks.toOwnedSlice(self.allocator);
        errdefer {
            if (self.storage) |*storage| storage.releaseRetainedBlocks(owned_storage_blocks);
            if (owned_storage_blocks.len > 0) self.allocator.free(owned_storage_blocks);
        }

        self.tick += 1;
        const bytes = self.estimateBytes(namespace.len, cacheable_tokens, owned_blocks.len, owned_storage_blocks.len);
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
        self.updateResourceUsage();
        self.evictToBudget();
    }

    pub fn stats(self: *PromptPrefixCache) Stats {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        var cached_tokens: u64 = 0;
        for (self.entries.items) |entry| cached_tokens += @intCast(entry.tokens.len);
        for (self.block_hash_entries.items) |entry| cached_tokens += @intCast(entry.tokens.len);
        return .{
            .hits = self.hits,
            .misses = self.misses,
            .evictions = self.evictions,
            .cached_tokens = cached_tokens,
            .live_entries = self.entries.items.len + self.block_hash_entries.items.len,
            .live_bytes = self.estimated_bytes,
            .block_hash_hits = self.block_hash_hits,
            .block_hash_misses = self.block_hash_misses,
            .block_hash_evictions = self.block_hash_evictions,
            .block_hash_cached_blocks = self.block_hash_entries.items.len,
            .block_hash_collision_guards = self.block_hash_collision_guards,
        };
    }

    fn attachLongestBlockHashPrefix(
        self: *PromptPrefixCache,
        namespace: []const u8,
        prompt_tokens: []const i64,
        max_prefix_tokens: usize,
    ) !?AttachedPrefix {
        if (namespace.len > max_namespace_bytes) {
            self.misses += 1;
            self.block_hash_misses += 1;
            return null;
        }
        const page_size = self.pageSize() orelse return null;
        const limit = (max_prefix_tokens / page_size) * page_size;
        if (limit < page_size) {
            self.misses += 1;
            self.block_hash_misses += 1;
            return null;
        }

        self.expireOld();
        var blocks: std.ArrayListUnmanaged(block.KvBlockId) = .empty;
        defer blocks.deinit(self.allocator);
        var storage_blocks: std.ArrayListUnmanaged(block.KvBlockId) = .empty;
        defer storage_blocks.deinit(self.allocator);

        var previous_hash: Hash = zeroHash();
        var matched_tokens: usize = 0;
        while (matched_tokens + page_size <= limit and matched_tokens + page_size <= prompt_tokens.len) {
            const token_block = prompt_tokens[matched_tokens .. matched_tokens + page_size];
            const hash = blockHash(self.pool_config.?, namespace, previous_hash, token_block);
            const entry_idx = self.block_hash_index.get(hash) orelse break;
            const entry = &self.block_hash_entries.items[entry_idx];
            if (!std.mem.eql(i64, entry.tokens, token_block)) {
                self.block_hash_collision_guards += 1;
                break;
            }
            try blocks.append(self.allocator, entry.block_id);
            if (self.storage != null) {
                try storage_blocks.append(self.allocator, entry.storage_block_id orelse return error.InvalidPagedKvState);
            }
            self.tick += 1;
            entry.last_used = self.tick;
            previous_hash = hash;
            matched_tokens += page_size;
        }

        if (matched_tokens == 0) {
            self.misses += 1;
            self.block_hash_misses += 1;
            return null;
        }

        const sequence_id = try self.manager.attachSequenceWithRetainedBlocks(self.pool_id.?, blocks.items, matched_tokens);
        errdefer self.manager.releaseSequence(sequence_id) catch {};
        if (self.storage) |*storage| {
            const storage_sequence_id = try storage.attachSequenceWithRetainedBlocks(storage.poolId(), storage_blocks.items, matched_tokens);
            errdefer storage.releaseSequence(storage_sequence_id) catch {};
            if (storage_sequence_id != sequence_id) return error.InvalidPagedKvState;
        }

        self.hits += 1;
        self.block_hash_hits += 1;
        return .{
            .sequence_id = sequence_id,
            .token_count = matched_tokens,
        };
    }

    fn storeBlockHashFromSequence(
        self: *PromptPrefixCache,
        namespace: []const u8,
        prompt_tokens: []const i64,
        sequence_id: manager_mod.SequenceId,
    ) !void {
        if (namespace.len > max_namespace_bytes) return;
        const page_size = self.pageSize() orelse return;
        const cacheable_tokens = (prompt_tokens.len / page_size) * page_size;
        if (cacheable_tokens < self.config.min_tokens) return;

        var blocks: std.ArrayListUnmanaged(block.KvBlockId) = .empty;
        defer blocks.deinit(self.allocator);
        try self.manager.retainSequencePrefixBlocks(sequence_id, cacheable_tokens, &blocks);

        var storage_blocks: std.ArrayListUnmanaged(block.KvBlockId) = .empty;
        defer storage_blocks.deinit(self.allocator);
        var next_retained_idx: usize = 0;
        errdefer self.releaseRetainedBlockHashTail(blocks.items, storage_blocks.items, next_retained_idx);
        if (self.storage) |*storage| {
            try storage.retainSequencePrefixBlocks(sequence_id, cacheable_tokens, &storage_blocks);
            if (storage_blocks.items.len != blocks.items.len) return error.InvalidPagedKvState;
        }

        var previous_hash: Hash = zeroHash();
        while (next_retained_idx < blocks.items.len) {
            const idx = next_retained_idx;
            next_retained_idx += 1;
            const start = idx * page_size;
            const token_block = prompt_tokens[start .. start + page_size];
            const hash = blockHash(self.pool_config.?, namespace, previous_hash, token_block);
            previous_hash = hash;
            const storage_block_id: ?block.KvBlockId = if (self.storage != null) storage_blocks.items[idx] else null;
            var current_done = false;
            errdefer if (!current_done) self.releaseRetainedBlockHashBlock(blocks.items, storage_blocks.items, idx);

            if (self.block_hash_index.get(hash)) |entry_idx| {
                self.tick += 1;
                self.block_hash_entries.items[entry_idx].last_used = self.tick;
                if (!std.mem.eql(i64, self.block_hash_entries.items[entry_idx].tokens, token_block)) {
                    self.block_hash_collision_guards += 1;
                    self.releaseRetainedBlockHashBlock(blocks.items, storage_blocks.items, idx);
                    current_done = true;
                    self.releaseRetainedBlockHashTail(blocks.items, storage_blocks.items, next_retained_idx);
                    next_retained_idx = blocks.items.len;
                    break;
                }
                self.releaseRetainedBlockHashBlock(blocks.items, storage_blocks.items, idx);
                current_done = true;
                continue;
            }

            try self.insertBlockHashEntry(hash, token_block, blocks.items[idx], storage_block_id);
            current_done = true;
        }
        self.evictToBudget();
    }

    fn insertBlockHashEntry(
        self: *PromptPrefixCache,
        hash: Hash,
        token_block: []const i64,
        block_id: block.KvBlockId,
        storage_block_id: ?block.KvBlockId,
    ) !void {
        const owned_tokens = try self.allocator.dupe(i64, token_block);
        errdefer self.allocator.free(owned_tokens);
        const bytes = self.estimateBytes(0, token_block.len, 1, if (storage_block_id == null) 0 else 1);
        try self.block_hash_index.put(self.allocator, hash, self.block_hash_entries.items.len);
        errdefer _ = self.block_hash_index.remove(hash);
        self.tick += 1;
        try self.block_hash_entries.append(self.allocator, .{
            .hash = hash,
            .tokens = owned_tokens,
            .block_id = block_id,
            .storage_block_id = storage_block_id,
            .estimated_bytes = bytes,
            .expires_at_ms = nowMs() + @as(i64, @intCast(self.config.ttl_ms)),
            .last_used = self.tick,
        });
        self.estimated_bytes += bytes;
        self.updateResourceUsage();
    }

    fn releaseRetainedBlockHashBlock(
        self: *PromptPrefixCache,
        blocks: []const block.KvBlockId,
        storage_blocks: []const block.KvBlockId,
        idx: usize,
    ) void {
        if (self.pool_id) |pool_id| self.manager.releaseRetainedBlocks(pool_id, blocks[idx .. idx + 1]);
        if (self.storage) |*storage| {
            if (idx < storage_blocks.len) storage.releaseRetainedBlocks(storage_blocks[idx .. idx + 1]);
        }
    }

    fn releaseRetainedBlockHashTail(
        self: *PromptPrefixCache,
        blocks: []const block.KvBlockId,
        storage_blocks: []const block.KvBlockId,
        start: usize,
    ) void {
        if (start >= blocks.len) return;
        if (self.pool_id) |pool_id| self.manager.releaseRetainedBlocks(pool_id, blocks[start..]);
        if (self.storage) |*storage| {
            if (start < storage_blocks.len) storage.releaseRetainedBlocks(storage_blocks[start..]);
        }
    }

    fn findExact(self: *const PromptPrefixCache, namespace: []const u8, tokens: []const i64) ?usize {
        for (self.entries.items, 0..) |entry, idx| {
            if (std.mem.eql(u8, entry.namespace, namespace) and std.mem.eql(i64, entry.tokens, tokens)) return idx;
        }
        return null;
    }

    fn estimateBytes(self: *const PromptPrefixCache, namespace_len: usize, token_count: usize, block_count: usize, storage_block_count: usize) usize {
        const metadata_bytes =
            @sizeOf(Entry) +
            namespace_len +
            token_count * @sizeOf(i64) +
            (block_count + storage_block_count) * @sizeOf(block.KvBlockId);
        const cfg = self.pool_config orelse return metadata_bytes;
        return metadata_bytes + token_count * cfg.num_layers_packed * cfg.bytesPerTokenPair();
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
        idx = 0;
        while (idx < self.block_hash_entries.items.len) {
            if (now > self.block_hash_entries.items[idx].expires_at_ms) {
                self.removeBlockHashEntry(idx);
                continue;
            }
            idx += 1;
        }
    }

    fn evictToBudget(self: *PromptPrefixCache) void {
        // ponytail: O(n) LRU scan, replace with an indexed queue if cache sizes get large.
        while (self.estimated_bytes > self.config.max_bytes and (self.entries.items.len + self.block_hash_entries.items.len) > 0) {
            var simple_victim: ?usize = null;
            for (self.entries.items, 0..) |entry, idx| {
                if (simple_victim == null or entry.last_used < self.entries.items[simple_victim.?].last_used) simple_victim = idx;
            }
            var block_victim: ?usize = null;
            for (self.block_hash_entries.items, 0..) |entry, idx| {
                if (block_victim == null or entry.last_used < self.block_hash_entries.items[block_victim.?].last_used) block_victim = idx;
            }
            if (simple_victim) |simple_idx| {
                if (block_victim) |block_idx| {
                    if (self.entries.items[simple_idx].last_used <= self.block_hash_entries.items[block_idx].last_used) {
                        self.removeEntry(simple_idx);
                    } else {
                        self.removeBlockHashEntry(block_idx);
                    }
                } else {
                    self.removeEntry(simple_idx);
                }
            } else if (block_victim) |block_idx| {
                self.removeBlockHashEntry(block_idx);
            } else {
                break;
            }
        }
    }

    fn updateResourceUsage(self: *PromptPrefixCache) void {
        const observer = self.config.resource_usage_observer orelse return;
        observer.update(observer.context, &self.resource_accounted_bytes, @intCast(self.estimated_bytes));
    }

    fn clearEntries(self: *PromptPrefixCache) void {
        var idx: usize = self.entries.items.len;
        while (idx > 0) {
            idx -= 1;
            self.removeEntry(idx);
        }
        idx = self.block_hash_entries.items.len;
        while (idx > 0) {
            idx -= 1;
            self.removeBlockHashEntry(idx);
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
        self.updateResourceUsage();
        _ = self.entries.swapRemove(idx);
        self.evictions += 1;
    }

    fn removeBlockHashEntry(self: *PromptPrefixCache, idx: usize) void {
        const entry = self.block_hash_entries.items[idx];
        if (self.pool_id) |pool_id| self.manager.releaseRetainedBlocks(pool_id, &.{entry.block_id});
        if (entry.storage_block_id) |storage_block| {
            if (self.storage) |*storage| storage.releaseRetainedBlocks(&.{storage_block});
        }
        self.allocator.free(entry.tokens);
        self.estimated_bytes -|= entry.estimated_bytes;
        self.updateResourceUsage();
        _ = self.block_hash_index.remove(entry.hash);
        const last_idx = self.block_hash_entries.items.len - 1;
        _ = self.block_hash_entries.swapRemove(idx);
        if (idx != last_idx) {
            const moved_hash = self.block_hash_entries.items[idx].hash;
            if (self.block_hash_index.getPtr(moved_hash)) |mapped_idx| mapped_idx.* = idx;
        }
        self.evictions += 1;
        self.block_hash_evictions += 1;
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

fn zeroHash() Hash {
    return [_]u8{0} ** std.crypto.hash.sha2.Sha256.digest_length;
}

fn blockHash(config: pool_mod.KvPoolConfig, namespace: []const u8, previous_hash: Hash, tokens: []const i64) Hash {
    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    hasher.update(&previous_hash);
    updateHashU64(&hasher, @intFromEnum(config.backend));
    updateHashU64(&hasher, @intFromEnum(config.dtype));
    updateHashU64(&hasher, config.page_size_tokens);
    updateHashU64(&hasher, config.num_layers_packed);
    updateHashU64(&hasher, config.num_kv_heads);
    updateHashU64(&hasher, config.head_dim);
    updateHashOptionalU32(&hasher, config.key_values_per_token);
    updateHashOptionalU32(&hasher, config.value_values_per_token);
    updateHashOptionalU32(&hasher, config.sliding_window_size);
    updateHashU64(&hasher, @intFromBool(config.store_cpu_bytes));
    updateHashU64(&hasher, namespace.len);
    hasher.update(namespace);
    updateHashU64(&hasher, tokens.len);
    for (tokens) |token| updateHashI64(&hasher, token);
    var out: Hash = undefined;
    hasher.final(&out);
    return out;
}

fn updateHashOptionalU32(hasher: *std.crypto.hash.sha2.Sha256, value: ?u32) void {
    updateHashU64(hasher, if (value) |v| @as(u64, v) else std.math.maxInt(u64));
}

fn updateHashI64(hasher: *std.crypto.hash.sha2.Sha256, value: i64) void {
    var buf: [8]u8 = undefined;
    std.mem.writeInt(i64, &buf, value, .little);
    hasher.update(&buf);
}

fn updateHashU64(hasher: *std.crypto.hash.sha2.Sha256, value: anytype) void {
    var buf: [8]u8 = undefined;
    std.mem.writeInt(u64, &buf, @intCast(value), .little);
    hasher.update(&buf);
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

test "prompt cache reports retained bytes to resource observer" {
    const UsageProbe = struct {
        updates: usize = 0,
        last: u64 = 0,
        peak: u64 = 0,

        fn update(context: *anyopaque, current: *u64, next: u64) void {
            const probe: *@This() = @ptrCast(@alignCast(context));
            current.* = next;
            probe.last = next;
            probe.peak = @max(probe.peak, next);
            probe.updates += 1;
        }
    };

    const allocator = std.testing.allocator;
    var probe = UsageProbe{};
    var cache = PromptPrefixCache.init(allocator);
    defer cache.deinit();
    cache.configure(.{
        .enabled = true,
        .min_tokens = 2,
        .max_bytes = 1,
        .resource_usage_observer = .{
            .context = &probe,
            .update = UsageProbe.update,
        },
    });

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

    try std.testing.expect(probe.peak > 0);
    try std.testing.expectEqual(@as(u64, 0), probe.last);
    try std.testing.expect(probe.updates >= 2);
}

test "prompt cache budgets metadata bytes" {
    const allocator = std.testing.allocator;
    const pool_config = pool_mod.KvPoolConfig{
        .backend = .native,
        .dtype = .f32,
        .page_size_tokens = 2,
        .num_layers_packed = 1,
        .num_kv_heads = 1,
        .head_dim = 2,
    };
    var cache = PromptPrefixCache.init(allocator);
    defer cache.deinit();
    cache.configure(.{
        .enabled = true,
        .min_tokens = 2,
        .max_bytes = 2 * pool_config.num_layers_packed * pool_config.bytesPerTokenPair(),
    });

    const pool_id = (try cache.ensurePool(pool_config)).?;
    const source_id = try cache.manager.attachSequence(pool_id);
    try cache.manager.appendTokens(source_id, 2);
    try cache.storeFromSequence("agent", &.{ 1, 2 }, source_id);

    try std.testing.expectEqual(@as(usize, 0), cache.stats().live_entries);
}

test "prompt cache skips oversized namespace" {
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
    try cache.manager.appendTokens(source_id, 2);

    var namespace: [max_namespace_bytes + 1]u8 = undefined;
    @memset(&namespace, 'x');
    try cache.storeFromSequence(namespace[0..], &.{ 1, 2 }, source_id);
    try std.testing.expectEqual(@as(usize, 0), cache.stats().live_entries);
    try std.testing.expect((try cache.attachLongestPrefix(namespace[0..], &.{ 1, 2 }, 2)) == null);
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

test "prompt cache block hash isolates namespaces" {
    const allocator = std.testing.allocator;
    var cache = PromptPrefixCache.init(allocator);
    defer cache.deinit();
    cache.configure(.{ .enabled = true, .mode = .block_hash, .min_tokens = 2, .max_bytes = 1 << 20 });

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
    try cache.storeFromSequence("tenant-a", &.{ 1, 2, 3, 4 }, source_id);

    try std.testing.expect((try cache.attachLongestPrefix("tenant-b", &.{ 1, 2, 3, 4 }, 2)) == null);
    const hit = (try cache.attachLongestPrefix("tenant-a", &.{ 1, 2, 3, 9 }, 2)).?;
    try std.testing.expectEqual(@as(usize, 2), hit.token_count);
    try cache.manager.releaseSequence(hit.sequence_id);
}

test "prompt cache block hash walks chained blocks" {
    const allocator = std.testing.allocator;
    var cache = PromptPrefixCache.init(allocator);
    defer cache.deinit();
    cache.configure(.{ .enabled = true, .mode = .block_hash, .min_tokens = 2, .max_bytes = 1 << 20 });

    const pool_id = (try cache.ensurePool(.{
        .backend = .native,
        .dtype = .f32,
        .page_size_tokens = 2,
        .num_layers_packed = 1,
        .num_kv_heads = 1,
        .head_dim = 2,
    })).?;
    const source_id = try cache.manager.attachSequence(pool_id);
    try cache.manager.appendTokens(source_id, 6);
    try cache.storeFromSequence("agent", &.{ 1, 2, 3, 4, 5, 6 }, source_id);

    const hit = (try cache.attachLongestPrefix("agent", &.{ 1, 2, 3, 4, 7, 8 }, 4)).?;
    try std.testing.expectEqual(@as(usize, 4), hit.token_count);
    try cache.manager.releaseSequence(hit.sequence_id);

    const stats_value = cache.stats();
    try std.testing.expectEqual(@as(usize, 3), stats_value.block_hash_cached_blocks);
    try std.testing.expectEqual(@as(u64, 1), stats_value.block_hash_hits);
}

test "prompt cache block hash evicts retained blocks by budget" {
    const allocator = std.testing.allocator;
    var cache = PromptPrefixCache.init(allocator);
    defer cache.deinit();
    cache.configure(.{ .enabled = true, .mode = .block_hash, .min_tokens = 2, .max_bytes = 1 });

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

    try std.testing.expectEqual(@as(usize, 0), cache.stats().block_hash_cached_blocks);
}
