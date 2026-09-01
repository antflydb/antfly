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
pub const bytes_per_mebibyte: usize = 1024 * 1024;
/// Largest whole-MiB cache target that can be represented as bytes.
pub const max_config_bytes_mb: usize = std.math.maxInt(usize) / bytes_per_mebibyte;
/// Expiry timestamps use signed milliseconds.
pub const max_config_ttl_ms: u64 = @intCast(std.math.maxInt(i64));

pub const Mode = enum {
    /// Whole-prefix entries with linear scan matching. O(entries) lookup and
    /// eviction; only suitable for small caches or debugging.
    simple,
    /// Hash-addressed KV blocks with chained per-page hashes. O(1) lookup per
    /// block; production default.
    block_hash,
    /// Page-aligned compressed radix tree. Shared prefixes own KV blocks once,
    /// branch at the first mismatching page, and use leaf-only LRU eviction.
    /// Explicitly opt in; block_hash remains the compatibility default.
    radix,
};

pub const Config = struct {
    enabled: bool = false,
    mode: Mode = .block_hash,
    /// Eviction target for estimated logical cache bytes, not an allocator/RSS cap.
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
    radix_hits: u64 = 0,
    radix_misses: u64 = 0,
    radix_evictions: u64 = 0,
    radix_cached_nodes: usize = 0,
    radix_collision_guards: u64 = 0,
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

const RadixNodeId = u32;

const RadixNode = struct {
    active: bool = false,
    parent: ?RadixNodeId = null,
    namespace: []u8 = &.{},
    /// Compressed, page-aligned edge segment.
    tokens: []i64 = &.{},
    blocks: []block.KvBlockId = &.{},
    storage_blocks: []block.KvBlockId = &.{},
    children: std.ArrayListUnmanaged(RadixNodeId) = .empty,
    edge_hash: Hash = @as([std.crypto.hash.sha2.Sha256.digest_length]u8, @splat(0)),
    estimated_bytes: usize = 0,
    expires_at_ms: i64 = 0,
    last_used: u64 = 0,
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
    /// A node-wide rebalance may discover this cache while another request is
    /// actively mutating its KV manager. Foreign requests may publish the next
    /// budget here, but only this model's serialized generation path may apply
    /// it and evict retained blocks.
    pending_config: ?Config = null,
    manager: manager_mod.KvManager,
    storage: ?storage_runtime_mod.KvStorageRuntime = null,
    pool_id: ?block.KvPoolId = null,
    pool_config: ?pool_mod.KvPoolConfig = null,
    /// Set while ModelManager has admitted this cache into the node-wide
    /// budget but the first request has not finished creating its KV pool.
    /// Counting this state closes the gap between rebalance and ensurePool.
    activation_pending: bool = false,
    entries: std.ArrayListUnmanaged(Entry) = .empty,
    block_hash_entries: std.ArrayListUnmanaged(BlockHashEntry) = .empty,
    block_hash_index: std.AutoHashMapUnmanaged(Hash, usize) = .empty,
    radix_nodes: std.ArrayListUnmanaged(RadixNode) = .empty,
    radix_free_nodes: std.ArrayListUnmanaged(RadixNodeId) = .empty,
    radix_edge_index: std.AutoHashMapUnmanaged(Hash, RadixNodeId) = .empty,
    radix_live_nodes: usize = 0,
    estimated_bytes: usize = 0,
    tick: u64 = 0,
    hits: u64 = 0,
    misses: u64 = 0,
    evictions: u64 = 0,
    block_hash_hits: u64 = 0,
    block_hash_misses: u64 = 0,
    block_hash_evictions: u64 = 0,
    block_hash_collision_guards: u64 = 0,
    radix_hits: u64 = 0,
    radix_misses: u64 = 0,
    radix_evictions: u64 = 0,
    radix_collision_guards: u64 = 0,
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
        self.radix_nodes.deinit(self.allocator);
        self.radix_free_nodes.deinit(self.allocator);
        self.radix_edge_index.deinit(self.allocator);
        if (self.storage) |*storage| storage.deinit();
        self.manager.deinit();
    }

    pub fn configure(self: *PromptPrefixCache, config: Config) void {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        self.pending_config = null;
        self.configureLocked(config);
    }

    /// Queue a configuration change without touching the shared KV pool. The
    /// owning model applies it at its next request boundary via configure().
    pub fn scheduleConfigure(self: *PromptPrefixCache, config: Config) void {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        self.pending_config = config;
        // Publish a tighter cap immediately so owner-side stores cannot grow
        // the cache beyond the node rebalance target. Existing entries remain
        // untouched until the owning generation lane applies the full config.
        self.config.max_bytes = @min(self.config.max_bytes, config.max_bytes);
    }

    fn configureLocked(self: *PromptPrefixCache, config: Config) void {
        if (self.config.resource_usage_observer) |old_observer| {
            const same_observer = if (config.resource_usage_observer) |new_observer|
                old_observer.context == new_observer.context and old_observer.update == new_observer.update
            else
                false;
            if (!same_observer) old_observer.update(old_observer.context, &self.resource_accounted_bytes, 0);
        }
        if (self.config.mode != config.mode) self.clearEntries();
        self.config = config;
        if (!config.enabled) self.activation_pending = false;
        self.evictToBudget();
        self.updateResourceUsage();
    }

    pub fn detachResourceUsageObserver(self: *PromptPrefixCache) void {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        if (self.config.resource_usage_observer) |observer| {
            observer.update(observer.context, &self.resource_accounted_bytes, 0);
        }
        self.config.resource_usage_observer = null;
        if (self.pending_config) |*pending| pending.resource_usage_observer = null;
        self.resource_accounted_bytes = 0;
    }

    /// Reserve this cache's place in node-wide accounting before any
    /// potentially failing pool allocation runs outside ModelManager's lock.
    pub fn reserveActivation(self: *PromptPrefixCache) void {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        if (self.pool_id == null) self.activation_pending = true;
    }

    /// Roll back a failed first activation. Returns whether a pending
    /// reservation was removed; an already-created pool remains active.
    pub fn cancelPendingActivation(self: *PromptPrefixCache) bool {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        if (!self.activation_pending) return false;
        self.activation_pending = false;
        self.pending_config = null;
        self.config.enabled = false;
        return true;
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
        self.activation_pending = false;
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
        switch (self.config.mode) {
            .block_hash => return try self.attachLongestBlockHashPrefix(namespace, prompt_tokens, max_prefix_tokens),
            .radix => return try self.attachLongestRadixPrefix(namespace, prompt_tokens, max_prefix_tokens),
            .simple => {},
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
        self.entries.items[idx].expires_at_ms = self.refreshedExpiryMs();
        self.hits += 1;
        return .{
            .sequence_id = sequence_id,
            .token_count = best_tokens,
        };
    }

    /// Release a prefix sequence returned by attachLongestPrefix when the
    /// caller cannot seed it into its decode state. Optional cache failures
    /// must not leak the sequence's retained host or device blocks.
    pub fn releaseAttachedPrefix(self: *PromptPrefixCache, attached: AttachedPrefix) void {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        self.manager.releaseSequence(attached.sequence_id) catch |err| {
            std.log.warn("failed to release attached prompt-cache sequence: {s}", .{@errorName(err)});
        };
        if (self.storage) |*storage| storage.releaseSequence(attached.sequence_id) catch |err| {
            std.log.warn("failed to release attached prompt-cache storage: {s}", .{@errorName(err)});
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
        switch (self.config.mode) {
            .block_hash => return try self.storeBlockHashFromSequence(namespace, prompt_tokens, sequence_id),
            .radix => return try self.storeRadixFromSequence(namespace, prompt_tokens, sequence_id),
            .simple => {},
        }
        if (namespace.len > max_namespace_bytes) return;
        const page_size = self.pageSize() orelse return;
        const cacheable_tokens = (prompt_tokens.len / page_size) * page_size;
        if (cacheable_tokens < self.config.min_tokens) return;

        const tokens = prompt_tokens[0..cacheable_tokens];
        if (self.findExact(namespace, tokens)) |idx| {
            self.tick += 1;
            self.entries.items[idx].last_used = self.tick;
            self.entries.items[idx].expires_at_ms = self.refreshedExpiryMs();
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
            .expires_at_ms = self.refreshedExpiryMs(),
            .last_used = self.tick,
        });
        self.estimated_bytes += bytes;
        self.updateResourceUsage();
        self.evictToBudget();
    }

    /// Retain a materialized prompt-plus-generation prefix in the radix tree.
    /// Callers must exclude the final sampled token: autoregressive decoding
    /// has allocated its logical slot, but does not write that token's KV until
    /// the next forward pass. Keeping this entry point radix-only prevents an
    /// opt-in continuation policy from changing the established cache modes.
    pub fn storeGeneratedPrefixFromSequence(
        self: *PromptPrefixCache,
        namespace: []const u8,
        materialized_tokens: []const i64,
        sequence_id: manager_mod.SequenceId,
    ) !void {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        if (!self.config.enabled or self.config.mode != .radix or self.pool_id == null) return;
        try self.storeRadixFromSequence(namespace, materialized_tokens, sequence_id);
    }

    pub fn stats(self: *PromptPrefixCache) Stats {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        var cached_tokens: u64 = 0;
        for (self.entries.items) |entry| cached_tokens += @intCast(entry.tokens.len);
        for (self.block_hash_entries.items) |entry| cached_tokens += @intCast(entry.tokens.len);
        for (self.radix_nodes.items) |node| {
            if (node.active) cached_tokens += @intCast(node.tokens.len);
        }
        return .{
            .hits = self.hits,
            .misses = self.misses,
            .evictions = self.evictions,
            .cached_tokens = cached_tokens,
            .live_entries = self.entries.items.len + self.block_hash_entries.items.len + self.radix_live_nodes,
            .live_bytes = self.estimated_bytes,
            .block_hash_hits = self.block_hash_hits,
            .block_hash_misses = self.block_hash_misses,
            .block_hash_evictions = self.block_hash_evictions,
            .block_hash_cached_blocks = self.block_hash_entries.items.len,
            .block_hash_collision_guards = self.block_hash_collision_guards,
            .radix_hits = self.radix_hits,
            .radix_misses = self.radix_misses,
            .radix_evictions = self.radix_evictions,
            .radix_cached_nodes = self.radix_live_nodes,
            .radix_collision_guards = self.radix_collision_guards,
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
            entry.expires_at_ms = self.refreshedExpiryMs();
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
                self.block_hash_entries.items[entry_idx].expires_at_ms = self.refreshedExpiryMs();
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
            .expires_at_ms = self.refreshedExpiryMs(),
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

    fn radixNode(self: *PromptPrefixCache, id: RadixNodeId) ?*RadixNode {
        const idx: usize = @intCast(id);
        if (idx >= self.radix_nodes.items.len) return null;
        const node = &self.radix_nodes.items[idx];
        if (!node.active) return null;
        return node;
    }

    fn touchRadixNode(self: *PromptPrefixCache, id: RadixNodeId) void {
        const node = self.radixNode(id) orelse return;
        self.tick += 1;
        node.last_used = self.tick;
        node.expires_at_ms = self.refreshedExpiryMs();
    }

    fn findRadixChild(
        self: *PromptPrefixCache,
        parent: ?RadixNodeId,
        namespace: []const u8,
        first_page: []const i64,
    ) ?RadixNodeId {
        const config = self.pool_config orelse return null;
        const hash = radixEdgeHash(config, namespace, parent, first_page);
        const id = self.radix_edge_index.get(hash) orelse return null;
        const node = self.radixNode(id) orelse {
            self.radix_collision_guards += 1;
            return null;
        };
        if (node.parent != parent or
            !std.mem.eql(u8, node.namespace, namespace) or
            node.tokens.len < first_page.len or
            !std.mem.eql(i64, node.tokens[0..first_page.len], first_page))
        {
            self.radix_collision_guards += 1;
            return null;
        }
        return id;
    }

    fn installRadixNode(self: *PromptPrefixCache, node: RadixNode) !RadixNodeId {
        if (self.radix_edge_index.get(node.edge_hash) != null) {
            self.radix_collision_guards += 1;
            return error.InvalidPagedKvState;
        }
        try self.radix_edge_index.ensureUnusedCapacity(self.allocator, 1);
        const id: RadixNodeId = if (self.radix_free_nodes.pop()) |free_id| blk: {
            self.radix_nodes.items[@intCast(free_id)] = node;
            break :blk free_id;
        } else blk: {
            if (self.radix_nodes.items.len > std.math.maxInt(RadixNodeId)) return error.KvCapacityTooSmall;
            try self.radix_nodes.ensureUnusedCapacity(self.allocator, 1);
            const new_id: RadixNodeId = @intCast(self.radix_nodes.items.len);
            self.radix_nodes.appendAssumeCapacity(node);
            break :blk new_id;
        };
        self.radix_edge_index.putAssumeCapacity(node.edge_hash, id);
        self.radix_live_nodes += 1;
        self.estimated_bytes += node.estimated_bytes;
        self.updateResourceUsage();
        return id;
    }

    fn addRadixLeaf(
        self: *PromptPrefixCache,
        parent: ?RadixNodeId,
        namespace: []const u8,
        tokens: []const i64,
        blocks: []const block.KvBlockId,
        storage_blocks: []const block.KvBlockId,
    ) !RadixNodeId {
        const page_size = self.pageSize() orelse return error.InvalidPagedKvState;
        if (tokens.len == 0 or tokens.len % page_size != 0 or blocks.len != tokens.len / page_size) return error.InvalidPagedKvState;
        if (self.storage != null and storage_blocks.len != blocks.len) return error.InvalidPagedKvState;
        if (self.storage == null and storage_blocks.len != 0) return error.InvalidPagedKvState;

        if (parent) |parent_id| {
            const parent_node = self.radixNode(parent_id) orelse return error.InvalidPagedKvState;
            try parent_node.children.ensureUnusedCapacity(self.allocator, 1);
        }
        const owned_namespace = try self.allocator.dupe(u8, namespace);
        errdefer self.allocator.free(owned_namespace);
        const owned_tokens = try self.allocator.dupe(i64, tokens);
        errdefer self.allocator.free(owned_tokens);
        const owned_blocks = try self.allocator.dupe(block.KvBlockId, blocks);
        errdefer self.allocator.free(owned_blocks);
        const owned_storage_blocks = try self.allocator.dupe(block.KvBlockId, storage_blocks);
        errdefer if (owned_storage_blocks.len > 0) self.allocator.free(owned_storage_blocks);

        self.tick += 1;
        const id = try self.installRadixNode(.{
            .active = true,
            .parent = parent,
            .namespace = owned_namespace,
            .tokens = owned_tokens,
            .blocks = owned_blocks,
            .storage_blocks = owned_storage_blocks,
            .edge_hash = radixEdgeHash(self.pool_config.?, namespace, parent, tokens[0..page_size]),
            .estimated_bytes = self.estimateRadixNodeBytes(namespace.len, tokens.len, blocks.len, storage_blocks.len),
            .expires_at_ms = self.refreshedExpiryMs(),
            .last_used = self.tick,
        });
        if (parent) |parent_id| self.radixNode(parent_id).?.children.appendAssumeCapacity(id);
        return id;
    }

    /// Split a compressed edge at a page boundary. KV references move between
    /// the two nodes; no retain/release occurs, so sharing remains exact.
    fn splitRadixNode(self: *PromptPrefixCache, child_id: RadixNodeId, prefix_tokens: usize) !RadixNodeId {
        const page_size = self.pageSize() orelse return error.InvalidPagedKvState;
        const child_idx: usize = @intCast(child_id);
        if (child_idx >= self.radix_nodes.items.len) return error.InvalidPagedKvState;
        const current = &self.radix_nodes.items[child_idx];
        if (!current.active or prefix_tokens == 0 or prefix_tokens >= current.tokens.len or prefix_tokens % page_size != 0) return error.InvalidPagedKvState;
        const prefix_blocks_len = prefix_tokens / page_size;
        const old_parent = current.parent;
        const old_hash = current.edge_hash;

        const prefix_namespace = try self.allocator.dupe(u8, current.namespace);
        errdefer self.allocator.free(prefix_namespace);
        const prefix_token_slice = try self.allocator.dupe(i64, current.tokens[0..prefix_tokens]);
        errdefer self.allocator.free(prefix_token_slice);
        const prefix_blocks = try self.allocator.dupe(block.KvBlockId, current.blocks[0..prefix_blocks_len]);
        errdefer self.allocator.free(prefix_blocks);
        const prefix_storage = try self.allocator.dupe(
            block.KvBlockId,
            if (current.storage_blocks.len > 0) current.storage_blocks[0..prefix_blocks_len] else &.{},
        );
        errdefer if (prefix_storage.len > 0) self.allocator.free(prefix_storage);
        const suffix_tokens = try self.allocator.dupe(i64, current.tokens[prefix_tokens..]);
        errdefer self.allocator.free(suffix_tokens);
        const suffix_blocks = try self.allocator.dupe(block.KvBlockId, current.blocks[prefix_blocks_len..]);
        errdefer self.allocator.free(suffix_blocks);
        const suffix_storage = try self.allocator.dupe(
            block.KvBlockId,
            if (current.storage_blocks.len > 0) current.storage_blocks[prefix_blocks_len..] else &.{},
        );
        errdefer if (suffix_storage.len > 0) self.allocator.free(suffix_storage);

        var prefix_children: std.ArrayListUnmanaged(RadixNodeId) = .empty;
        errdefer prefix_children.deinit(self.allocator);
        try prefix_children.append(self.allocator, child_id);

        const prefix_id: RadixNodeId = if (self.radix_free_nodes.items.len > 0)
            self.radix_free_nodes.items[self.radix_free_nodes.items.len - 1]
        else blk: {
            if (self.radix_nodes.items.len > std.math.maxInt(RadixNodeId)) return error.KvCapacityTooSmall;
            break :blk @intCast(self.radix_nodes.items.len);
        };
        const child_new_hash = radixEdgeHash(self.pool_config.?, current.namespace, prefix_id, suffix_tokens[0..page_size]);
        if (self.radix_edge_index.get(child_new_hash) != null) {
            self.radix_collision_guards += 1;
            return error.InvalidPagedKvState;
        }
        try self.radix_edge_index.ensureUnusedCapacity(self.allocator, 1);
        if (self.radix_free_nodes.items.len == 0) try self.radix_nodes.ensureUnusedCapacity(self.allocator, 1);

        const current_after_reserve = &self.radix_nodes.items[child_idx];
        const old_child_bytes = current_after_reserve.estimated_bytes;
        const old_tokens = current_after_reserve.tokens;
        const old_blocks = current_after_reserve.blocks;
        const old_storage = current_after_reserve.storage_blocks;
        const namespace_len = current_after_reserve.namespace.len;
        const expires_at_ms = current_after_reserve.expires_at_ms;
        const last_used = current_after_reserve.last_used;

        const prefix_node = RadixNode{
            .active = true,
            .parent = old_parent,
            .namespace = prefix_namespace,
            .tokens = prefix_token_slice,
            .blocks = prefix_blocks,
            .storage_blocks = prefix_storage,
            .children = prefix_children,
            .edge_hash = old_hash,
            .estimated_bytes = self.estimateRadixNodeBytes(namespace_len, prefix_tokens, prefix_blocks.len, prefix_storage.len),
            .expires_at_ms = expires_at_ms,
            .last_used = last_used,
        };

        if (self.radix_free_nodes.items.len > 0) {
            const reused = self.radix_free_nodes.pop().?;
            std.debug.assert(reused == prefix_id);
            self.radix_nodes.items[@intCast(prefix_id)] = prefix_node;
        } else {
            self.radix_nodes.appendAssumeCapacity(prefix_node);
        }
        const child = &self.radix_nodes.items[child_idx];
        child.parent = prefix_id;
        child.tokens = suffix_tokens;
        child.blocks = suffix_blocks;
        child.storage_blocks = suffix_storage;
        child.edge_hash = child_new_hash;
        child.estimated_bytes = self.estimateRadixNodeBytes(namespace_len, suffix_tokens.len, suffix_blocks.len, suffix_storage.len);

        if (old_parent) |parent_id| {
            const parent_node = self.radixNode(parent_id) orelse unreachable;
            for (parent_node.children.items) |*candidate| {
                if (candidate.* == child_id) {
                    candidate.* = prefix_id;
                    break;
                }
            }
        }
        const old_mapping = self.radix_edge_index.getPtr(old_hash) orelse unreachable;
        std.debug.assert(old_mapping.* == child_id);
        old_mapping.* = prefix_id;
        self.radix_edge_index.putAssumeCapacity(child_new_hash, child_id);

        self.allocator.free(old_tokens);
        self.allocator.free(old_blocks);
        if (old_storage.len > 0) self.allocator.free(old_storage);
        self.radix_live_nodes += 1;
        self.estimated_bytes -|= old_child_bytes;
        self.estimated_bytes += prefix_node.estimated_bytes + child.estimated_bytes;
        self.updateResourceUsage();
        return prefix_id;
    }

    fn releaseRadixRetainedRange(
        self: *PromptPrefixCache,
        blocks: []const block.KvBlockId,
        storage_blocks: []const block.KvBlockId,
        start: usize,
        end: usize,
    ) void {
        if (start >= end) return;
        if (self.pool_id) |pool_id| self.manager.releaseRetainedBlocks(pool_id, blocks[start..end]);
        if (self.storage) |*storage| storage.releaseRetainedBlocks(storage_blocks[start..end]);
    }

    fn attachLongestRadixPrefix(
        self: *PromptPrefixCache,
        namespace: []const u8,
        prompt_tokens: []const i64,
        max_prefix_tokens: usize,
    ) !?AttachedPrefix {
        if (namespace.len > max_namespace_bytes) {
            self.misses += 1;
            self.radix_misses += 1;
            return null;
        }
        const page_size = self.pageSize() orelse return null;
        const limit = @min((max_prefix_tokens / page_size) * page_size, (prompt_tokens.len / page_size) * page_size);
        if (limit < page_size) {
            self.misses += 1;
            self.radix_misses += 1;
            return null;
        }

        self.expireOld();
        var blocks: std.ArrayListUnmanaged(block.KvBlockId) = .empty;
        defer blocks.deinit(self.allocator);
        var storage_blocks: std.ArrayListUnmanaged(block.KvBlockId) = .empty;
        defer storage_blocks.deinit(self.allocator);
        var parent: ?RadixNodeId = null;
        var matched_tokens: usize = 0;
        while (matched_tokens + page_size <= limit) {
            const child_id = self.findRadixChild(parent, namespace, prompt_tokens[matched_tokens .. matched_tokens + page_size]) orelse break;
            const node = self.radixNode(child_id) orelse break;
            const candidate_len = @min(node.tokens.len, limit - matched_tokens);
            const matched_edge = (commonPrefixTokens(node.tokens[0..candidate_len], prompt_tokens[matched_tokens .. matched_tokens + candidate_len]) / page_size) * page_size;
            if (matched_edge == 0) break;
            const matched_blocks = matched_edge / page_size;
            try blocks.appendSlice(self.allocator, node.blocks[0..matched_blocks]);
            if (self.storage != null) try storage_blocks.appendSlice(self.allocator, node.storage_blocks[0..matched_blocks]);
            self.touchRadixNode(child_id);
            matched_tokens += matched_edge;
            if (matched_edge < node.tokens.len) break;
            parent = child_id;
        }

        if (matched_tokens == 0) {
            self.misses += 1;
            self.radix_misses += 1;
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
        self.radix_hits += 1;
        return .{ .sequence_id = sequence_id, .token_count = matched_tokens };
    }

    fn storeRadixFromSequence(
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
        if (self.storage) |*storage| {
            storage.retainSequencePrefixBlocks(sequence_id, cacheable_tokens, &storage_blocks) catch |err| {
                self.manager.releaseRetainedBlocks(self.pool_id.?, blocks.items);
                return err;
            };
            if (storage_blocks.items.len != blocks.items.len) {
                self.manager.releaseRetainedBlocks(self.pool_id.?, blocks.items);
                storage.releaseRetainedBlocks(storage_blocks.items);
                return error.InvalidPagedKvState;
            }
        }

        var retained_cursor: usize = 0;
        errdefer self.releaseRadixRetainedRange(blocks.items, storage_blocks.items, retained_cursor, blocks.items.len);
        var parent: ?RadixNodeId = null;
        var token_cursor: usize = 0;
        while (token_cursor + page_size <= cacheable_tokens) {
            const child_id = self.findRadixChild(parent, namespace, prompt_tokens[token_cursor .. token_cursor + page_size]) orelse break;
            const node = self.radixNode(child_id) orelse break;
            const remaining = cacheable_tokens - token_cursor;
            const candidate_len = @min(node.tokens.len, remaining);
            const matched_edge = (commonPrefixTokens(node.tokens[0..candidate_len], prompt_tokens[token_cursor .. token_cursor + candidate_len]) / page_size) * page_size;
            if (matched_edge == 0) break;
            const matched_blocks = matched_edge / page_size;
            self.releaseRadixRetainedRange(blocks.items, storage_blocks.items, retained_cursor, retained_cursor + matched_blocks);
            retained_cursor += matched_blocks;
            token_cursor += matched_edge;
            if (matched_edge < node.tokens.len) {
                parent = try self.splitRadixNode(child_id, matched_edge);
                self.touchRadixNode(parent.?);
                break;
            }
            self.touchRadixNode(child_id);
            parent = child_id;
        }

        if (token_cursor < cacheable_tokens) {
            _ = try self.addRadixLeaf(
                parent,
                namespace,
                prompt_tokens[token_cursor..cacheable_tokens],
                blocks.items[retained_cursor..],
                if (self.storage != null) storage_blocks.items[retained_cursor..] else &.{},
            );
            retained_cursor = blocks.items.len;
        }
        self.evictToBudget();
    }

    /// Idle TTL: hits refresh expiry, so only entries unused for ttl_ms expire.
    fn refreshedExpiryMs(self: *const PromptPrefixCache) i64 {
        const ttl_ms = std.math.cast(i64, self.config.ttl_ms) orelse std.math.maxInt(i64);
        return std.math.add(i64, nowMs(), ttl_ms) catch std.math.maxInt(i64);
    }

    pub fn isActive(self: *PromptPrefixCache) bool {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        return self.config.enabled and self.pool_id != null;
    }

    /// Includes admitted first-use activations whose pool allocation has not
    /// completed yet. ModelManager uses this for race-free budget splitting.
    pub fn isParticipating(self: *PromptPrefixCache) bool {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        return self.config.enabled and
            (self.activation_pending or self.pool_id != null);
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
        const pool_id = self.pool_id orelse return metadata_bytes;
        const manager_pool = self.manager.getPool(pool_id) orelse return metadata_bytes;
        var kv_bytes: usize = if (manager_pool.config.store_cpu_bytes)
            block_count * manager_pool.bytesPerBlock()
        else
            0;
        if (self.storage) |*storage| {
            const storage_pool = &storage.storage;
            if (storage_pool.config.store_cpu_bytes) {
                kv_bytes += storage_block_count * storage_pool.bytesPerBlock();
            }
            if (storage.device_write_hook != null) {
                // Device formats and allocator capacity vary by backend. Use
                // f32 K+V per logical block as the accounting estimate.
                kv_bytes += storage_block_count *
                    @as(usize, storage_pool.config.num_layers_packed) *
                    @as(usize, storage_pool.config.page_size_tokens) *
                    (storage_pool.config.keyValuesPerToken() + storage_pool.config.valueValuesPerToken()) *
                    @sizeOf(f32);
            }
        }
        return metadata_bytes + kv_bytes;
    }

    fn estimateRadixNodeBytes(self: *const PromptPrefixCache, namespace_len: usize, token_count: usize, block_count: usize, storage_block_count: usize) usize {
        const base = self.estimateBytes(namespace_len, token_count, block_count, storage_block_count);
        // estimateBytes accounts for Entry metadata. Replace that with the
        // radix node plus the parent-to-child edge owned by this node. The
        // ArrayList may reserve more capacity internally, so this remains a
        // conservative logical estimate rather than allocator/RSS accounting.
        return base -| @sizeOf(Entry) + @sizeOf(RadixNode) + @sizeOf(RadixNodeId);
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
        while (true) {
            var expired_leaf: ?RadixNodeId = null;
            for (self.radix_nodes.items, 0..) |node, node_idx| {
                if (!node.active or node.children.items.len != 0 or now <= node.expires_at_ms) continue;
                if (expired_leaf == null or node.last_used < self.radix_nodes.items[@intCast(expired_leaf.?)].last_used) {
                    expired_leaf = @intCast(node_idx);
                }
            }
            const leaf = expired_leaf orelse break;
            self.removeRadixLeaf(leaf);
        }
    }

    fn evictToBudget(self: *PromptPrefixCache) void {
        // ponytail: O(n) LRU scan, replace with an indexed queue if cache sizes get large.
        while (self.estimated_bytes > self.config.max_bytes and (self.entries.items.len + self.block_hash_entries.items.len + self.radix_live_nodes) > 0) {
            var simple_victim: ?usize = null;
            for (self.entries.items, 0..) |entry, idx| {
                if (simple_victim == null or entry.last_used < self.entries.items[simple_victim.?].last_used) simple_victim = idx;
            }
            var block_victim: ?usize = null;
            for (self.block_hash_entries.items, 0..) |entry, idx| {
                if (block_victim == null or entry.last_used < self.block_hash_entries.items[block_victim.?].last_used) block_victim = idx;
            }
            var radix_victim: ?RadixNodeId = null;
            for (self.radix_nodes.items, 0..) |node, node_idx| {
                if (!node.active or node.children.items.len != 0) continue;
                if (radix_victim == null or node.last_used < self.radix_nodes.items[@intCast(radix_victim.?)].last_used) {
                    radix_victim = @intCast(node_idx);
                }
            }
            var victim_kind: enum { simple, block_hash, radix } = undefined;
            var victim_tick: u64 = std.math.maxInt(u64);
            if (simple_victim) |simple_idx| {
                victim_kind = .simple;
                victim_tick = self.entries.items[simple_idx].last_used;
            }
            if (block_victim) |block_idx| {
                if (self.block_hash_entries.items[block_idx].last_used < victim_tick) {
                    victim_kind = .block_hash;
                    victim_tick = self.block_hash_entries.items[block_idx].last_used;
                }
            }
            if (radix_victim) |radix_id| {
                if (self.radix_nodes.items[@intCast(radix_id)].last_used < victim_tick) {
                    victim_kind = .radix;
                    victim_tick = self.radix_nodes.items[@intCast(radix_id)].last_used;
                }
            }
            if (victim_tick == std.math.maxInt(u64)) {
                break;
            }
            switch (victim_kind) {
                .simple => self.removeEntry(simple_victim.?),
                .block_hash => self.removeBlockHashEntry(block_victim.?),
                .radix => self.removeRadixLeaf(radix_victim.?),
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
        while (self.radix_live_nodes > 0) {
            var removed = false;
            for (self.radix_nodes.items, 0..) |node, node_idx| {
                if (!node.active or node.children.items.len != 0) continue;
                self.removeRadixLeaf(@intCast(node_idx));
                removed = true;
                break;
            }
            if (!removed) break;
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

    fn removeRadixLeaf(self: *PromptPrefixCache, id: RadixNodeId) void {
        const idx: usize = @intCast(id);
        if (idx >= self.radix_nodes.items.len) return;
        const node = &self.radix_nodes.items[idx];
        if (!node.active or node.children.items.len != 0) return;
        if (node.parent) |parent_id| {
            if (self.radixNode(parent_id)) |parent| {
                var child_idx: usize = 0;
                while (child_idx < parent.children.items.len) : (child_idx += 1) {
                    if (parent.children.items[child_idx] == id) {
                        _ = parent.children.swapRemove(child_idx);
                        break;
                    }
                }
            }
        }
        _ = self.radix_edge_index.remove(node.edge_hash);
        if (self.pool_id) |pool_id| self.manager.releaseRetainedBlocks(pool_id, node.blocks);
        if (self.storage) |*storage| storage.releaseRetainedBlocks(node.storage_blocks);
        self.allocator.free(node.namespace);
        self.allocator.free(node.tokens);
        self.allocator.free(node.blocks);
        if (node.storage_blocks.len > 0) self.allocator.free(node.storage_blocks);
        node.children.deinit(self.allocator);
        self.estimated_bytes -|= node.estimated_bytes;
        node.* = .{};
        self.radix_free_nodes.append(self.allocator, id) catch {};
        self.radix_live_nodes -|= 1;
        self.evictions += 1;
        self.radix_evictions += 1;
        self.updateResourceUsage();
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
    return @as([std.crypto.hash.sha2.Sha256.digest_length]u8, @splat(0));
}

fn blockHash(config: pool_mod.KvPoolConfig, namespace: []const u8, previous_hash: Hash, tokens: []const i64) Hash {
    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    hasher.update(&previous_hash);
    updateHashU64(&hasher, @backingInt(config.backend));
    updateHashU64(&hasher, @backingInt(config.dtype));
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

fn radixEdgeHash(config: pool_mod.KvPoolConfig, namespace: []const u8, parent: ?RadixNodeId, first_page: []const i64) Hash {
    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    hasher.update("antfly-radix-edge-v1");
    updateHashU64(&hasher, if (parent) |id| @as(u64, id) else std.math.maxInt(u64));
    updateHashU64(&hasher, @backingInt(config.backend));
    updateHashU64(&hasher, @backingInt(config.dtype));
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
    updateHashU64(&hasher, first_page.len);
    for (first_page) |token| updateHashI64(&hasher, token);
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
    cache.configure(.{ .enabled = true, .mode = .simple, .min_tokens = 2, .max_bytes = 1 << 20 });

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

test "prompt cache refreshes idle ttl on hit" {
    const allocator = std.testing.allocator;
    var cache = PromptPrefixCache.init(allocator);
    defer cache.deinit();
    cache.configure(.{ .enabled = true, .mode = .simple, .min_tokens = 2, .max_bytes = 1 << 20 });

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

    // Age the entry toward expiry without pushing it into the past (so expireOld
    // keeps it), then confirm a hit pushes expiry forward by ~ttl_ms.
    const aged_expiry = nowMs() + 1;
    cache.entries.items[0].expires_at_ms = aged_expiry;
    const hit = (try cache.attachLongestPrefix("agent", &.{ 1, 2, 3, 9 }, 2)).?;
    try cache.manager.releaseSequence(hit.sequence_id);
    try std.testing.expect(cache.entries.items[0].expires_at_ms > aged_expiry);
}

test "prompt cache expiry saturates after timestamp overflow" {
    var cache = PromptPrefixCache.init(std.testing.allocator);
    defer cache.deinit();
    cache.configure(.{
        .enabled = true,
        .ttl_ms = max_config_ttl_ms,
    });

    try std.testing.expectEqual(std.math.maxInt(i64), cache.refreshedExpiryMs());
}

test "prompt cache evicts retained blocks by budget" {
    const allocator = std.testing.allocator;
    var cache = PromptPrefixCache.init(allocator);
    defer cache.deinit();
    cache.configure(.{ .enabled = true, .mode = .simple, .min_tokens = 2, .max_bytes = 1 });

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

test "prompt cache defers foreign rebalance eviction until owner configure" {
    const allocator = std.testing.allocator;
    var cache = PromptPrefixCache.init(allocator);
    defer cache.deinit();
    cache.configure(.{ .enabled = true, .mode = .simple, .min_tokens = 2, .max_bytes = 1 << 20 });

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
    try std.testing.expectEqual(@as(usize, 1), cache.stats().live_entries);

    cache.scheduleConfigure(.{ .enabled = true, .mode = .simple, .min_tokens = 2, .max_bytes = 1 });
    try std.testing.expectEqual(@as(usize, 1), cache.config.max_bytes);
    try std.testing.expectEqual(@as(usize, 1), cache.pending_config.?.max_bytes);
    try std.testing.expectEqual(@as(usize, 1), cache.stats().live_entries);

    cache.configure(cache.pending_config.?);
    try std.testing.expect(cache.pending_config == null);
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
        .mode = .simple,
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
        .mode = .simple,
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
    cache.configure(.{ .enabled = true, .mode = .simple, .min_tokens = 2, .max_bytes = 1 << 20 });

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
    cache.configure(.{ .enabled = true, .mode = .simple, .min_tokens = 2, .max_bytes = 1 << 20 });

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

test "prompt cache estimates GPU host and device KV copies" {
    const TestHook = struct {
        fn write(
            _: *anyopaque,
            _: storage_runtime_mod.KvSuffixWrite,
            _: storage_runtime_mod.DeviceKvRef,
            _: storage_runtime_mod.DeviceKvRef,
        ) anyerror!void {}

        fn deinit(_: *anyopaque, _: std.mem.Allocator) void {}

        const vtable: storage_runtime_mod.DeviceWriteHook.VTable = .{
            .writeLayerKvSuffix = write,
            .deinit = deinit,
        };
    };

    const allocator = std.testing.allocator;
    const pool_config = pool_mod.KvPoolConfig{
        .backend = .metal,
        .dtype = .f32,
        .page_size_tokens = 2,
        .num_layers_packed = 1,
        .num_kv_heads = 1,
        .head_dim = 2,
    };
    var cache = PromptPrefixCache.init(allocator);
    defer cache.deinit();
    cache.configure(.{ .enabled = true, .mode = .simple, .min_tokens = 2, .max_bytes = 1 << 20 });

    const storage = (try cache.ensureStorage(pool_config)).?.storage;
    var hook_context: u8 = 0;
    storage.setDeviceWriteHook(.{ .ctx = &hook_context, .vtable = &TestHook.vtable });
    const pool_id = cache.pool_id.?;
    const sequence_id = try cache.manager.attachSequence(pool_id);
    try cache.manager.appendTokens(sequence_id, 2);
    const storage_sequence_id = try storage.attachSequence(storage.poolId());
    try std.testing.expectEqual(sequence_id, storage_sequence_id);
    try storage.appendTokens(storage_sequence_id, 2);
    try cache.storeFromSequence("gpu", &.{ 1, 2 }, sequence_id);

    const metadata_bytes = @sizeOf(Entry) + "gpu".len + 2 * @sizeOf(i64) + 2 * @sizeOf(block.KvBlockId);
    const block_bytes = @as(usize, pool_config.num_layers_packed) *
        @as(usize, pool_config.page_size_tokens) *
        pool_config.bytesPerTokenPair();
    const expected_bytes = metadata_bytes + 3 * block_bytes;
    try std.testing.expectEqual(expected_bytes, cache.stats().live_bytes);

    var tighter = cache.config;
    tighter.max_bytes = metadata_bytes + 2 * block_bytes;
    cache.configure(tighter);
    try std.testing.expectEqual(@as(usize, 0), cache.stats().live_entries);
    try std.testing.expectEqual(@as(usize, 0), cache.stats().live_bytes);
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

test "prompt cache radix splits compressed edges and reuses both branches" {
    const allocator = std.testing.allocator;
    var cache = PromptPrefixCache.init(allocator);
    defer cache.deinit();
    cache.configure(.{ .enabled = true, .mode = .radix, .min_tokens = 2, .max_bytes = 1 << 20 });

    const pool_id = (try cache.ensurePool(.{
        .backend = .native,
        .dtype = .f32,
        .page_size_tokens = 2,
        .num_layers_packed = 1,
        .num_kv_heads = 1,
        .head_dim = 2,
    })).?;
    const first_id = try cache.manager.attachSequence(pool_id);
    try cache.manager.appendTokens(first_id, 8);
    try cache.storeFromSequence("agent", &.{ 1, 2, 3, 4, 5, 6, 7, 8 }, first_id);
    try std.testing.expectEqual(@as(usize, 1), cache.stats().radix_cached_nodes);

    const second_id = try cache.manager.attachSequence(pool_id);
    try cache.manager.appendTokens(second_id, 8);
    try cache.storeFromSequence("agent", &.{ 1, 2, 3, 4, 9, 10, 11, 12 }, second_id);

    const after_split = cache.stats();
    try std.testing.expectEqual(@as(usize, 3), after_split.radix_cached_nodes);
    // Four shared tokens plus two four-token suffixes: the radix tree owns the
    // common KV pages once rather than retaining both complete prompts.
    try std.testing.expectEqual(@as(u64, 12), after_split.cached_tokens);

    const first_hit = (try cache.attachLongestPrefix("agent", &.{ 1, 2, 3, 4, 5, 6, 7, 8 }, 8)).?;
    try std.testing.expectEqual(@as(usize, 8), first_hit.token_count);
    try cache.manager.releaseSequence(first_hit.sequence_id);
    const second_hit = (try cache.attachLongestPrefix("agent", &.{ 1, 2, 3, 4, 9, 10, 99, 100 }, 8)).?;
    try std.testing.expectEqual(@as(usize, 6), second_hit.token_count);
    try cache.manager.releaseSequence(second_hit.sequence_id);
    try std.testing.expectEqual(@as(u64, 2), cache.stats().radix_hits);
}

test "prompt cache radix strict extension reuses every stored page" {
    const allocator = std.testing.allocator;
    var cache = PromptPrefixCache.init(allocator);
    defer cache.deinit();
    cache.configure(.{ .enabled = true, .mode = .radix, .min_tokens = 2, .max_bytes = 1 << 20 });

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

    const continuation = [_]i64{ 1, 2, 3, 4, 5, 6, 7, 8 };
    const hit = (try cache.attachLongestPrefix("agent", &continuation, continuation.len - 2)).?;
    try std.testing.expectEqual(@as(usize, 6), hit.token_count);
    cache.releaseAttachedPrefix(hit);

    try std.testing.expect((try cache.attachLongestPrefix("tenant-b", &continuation, continuation.len - 2)) == null);
    try std.testing.expectEqual(@as(u64, 1), cache.stats().radix_hits);
    try std.testing.expectEqual(@as(u64, 1), cache.stats().radix_misses);
}

test "prompt cache radix exact restorage and attached-prefix cleanup preserve refcounts" {
    const allocator = std.testing.allocator;
    var cache = PromptPrefixCache.init(allocator);
    defer cache.deinit();
    cache.configure(.{ .enabled = true, .mode = .radix, .min_tokens = 2, .max_bytes = 1 << 20 });

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
    const prompt = [_]i64{ 1, 2, 3, 4 };
    try cache.storeFromSequence("agent", &prompt, source_id);

    const pool = cache.manager.getPool(pool_id).?;
    try std.testing.expectEqual(@as(u32, 2), pool.blockInfo(0).?.refcount);
    try std.testing.expectEqual(@as(u32, 2), pool.blockInfo(1).?.refcount);
    try cache.storeFromSequence("agent", &prompt, source_id);
    try std.testing.expectEqual(@as(usize, 1), cache.stats().radix_cached_nodes);
    try std.testing.expectEqual(@as(u32, 2), pool.blockInfo(0).?.refcount);
    try std.testing.expectEqual(@as(u32, 2), pool.blockInfo(1).?.refcount);

    const hit = (try cache.attachLongestPrefix("agent", &prompt, prompt.len)).?;
    try std.testing.expectEqual(@as(u32, 3), pool.blockInfo(0).?.refcount);
    try std.testing.expectEqual(@as(u32, 3), pool.blockInfo(1).?.refcount);
    cache.releaseAttachedPrefix(hit);
    try std.testing.expectEqual(@as(u32, 2), pool.blockInfo(0).?.refcount);
    try std.testing.expectEqual(@as(u32, 2), pool.blockInfo(1).?.refcount);
}

test "prompt cache radix can split an existing edge at a stored strict prefix" {
    const allocator = std.testing.allocator;
    var cache = PromptPrefixCache.init(allocator);
    defer cache.deinit();
    cache.configure(.{ .enabled = true, .mode = .radix, .min_tokens = 2, .max_bytes = 1 << 20 });

    const pool_id = (try cache.ensurePool(.{
        .backend = .native,
        .dtype = .f32,
        .page_size_tokens = 2,
        .num_layers_packed = 1,
        .num_kv_heads = 1,
        .head_dim = 2,
    })).?;
    const long_id = try cache.manager.attachSequence(pool_id);
    try cache.manager.appendTokens(long_id, 8);
    try cache.storeFromSequence("agent", &.{ 1, 2, 3, 4, 5, 6, 7, 8 }, long_id);

    const short_id = try cache.manager.attachSequence(pool_id);
    try cache.manager.appendTokens(short_id, 4);
    try cache.storeFromSequence("agent", &.{ 1, 2, 3, 4 }, short_id);
    try std.testing.expectEqual(@as(usize, 2), cache.stats().radix_cached_nodes);
    try std.testing.expectEqual(@as(u64, 8), cache.stats().cached_tokens);

    const shared = (try cache.attachLongestPrefix("agent", &.{ 1, 2, 3, 4, 9, 10 }, 6)).?;
    try std.testing.expectEqual(@as(usize, 4), shared.token_count);
    cache.releaseAttachedPrefix(shared);
    const full = (try cache.attachLongestPrefix("agent", &.{ 1, 2, 3, 4, 5, 6, 7, 8 }, 8)).?;
    try std.testing.expectEqual(@as(usize, 8), full.token_count);
    cache.releaseAttachedPrefix(full);
}

test "prompt cache radix leaf eviction preserves shared ancestors" {
    const allocator = std.testing.allocator;
    var cache = PromptPrefixCache.init(allocator);
    defer cache.deinit();
    cache.configure(.{ .enabled = true, .mode = .radix, .min_tokens = 2, .max_bytes = 1 << 20 });

    const pool_id = (try cache.ensurePool(.{
        .backend = .native,
        .dtype = .f32,
        .page_size_tokens = 2,
        .num_layers_packed = 1,
        .num_kv_heads = 1,
        .head_dim = 2,
    })).?;
    const first_id = try cache.manager.attachSequence(pool_id);
    try cache.manager.appendTokens(first_id, 6);
    try cache.storeFromSequence("agent", &.{ 1, 2, 3, 4, 5, 6 }, first_id);
    const second_id = try cache.manager.attachSequence(pool_id);
    try cache.manager.appendTokens(second_id, 6);
    try cache.storeFromSequence("agent", &.{ 1, 2, 3, 4, 9, 10 }, second_id);

    // Refresh the first branch so the second suffix is the leaf LRU.
    const warm = (try cache.attachLongestPrefix("agent", &.{ 1, 2, 3, 4, 5, 6 }, 6)).?;
    try cache.manager.releaseSequence(warm.sequence_id);
    var constrained = cache.config;
    constrained.max_bytes = cache.stats().live_bytes - 1;
    cache.configure(constrained);

    try std.testing.expectEqual(@as(usize, 2), cache.stats().radix_cached_nodes);
    const evicted_branch = (try cache.attachLongestPrefix("agent", &.{ 1, 2, 3, 4, 9, 10 }, 6)).?;
    try std.testing.expectEqual(@as(usize, 4), evicted_branch.token_count);
    try cache.manager.releaseSequence(evicted_branch.sequence_id);
    const surviving_branch = (try cache.attachLongestPrefix("agent", &.{ 1, 2, 3, 4, 5, 6 }, 6)).?;
    try std.testing.expectEqual(@as(usize, 6), surviving_branch.token_count);
    try cache.manager.releaseSequence(surviving_branch.sequence_id);
}

test "prompt cache radix attaches cache-owned storage after source release" {
    const allocator = std.testing.allocator;
    var cache = PromptPrefixCache.init(allocator);
    defer cache.deinit();
    cache.configure(.{ .enabled = true, .mode = .radix, .min_tokens = 2, .max_bytes = 1 << 20 });

    const storage = (try cache.ensureStorage(.{
        .backend = .metal,
        .dtype = .f32,
        .page_size_tokens = 2,
        .num_layers_packed = 1,
        .num_kv_heads = 1,
        .head_dim = 2,
    })).?.storage;
    const source_id = try cache.manager.attachSequence(cache.pool_id.?);
    try cache.manager.appendTokens(source_id, 4);
    const storage_source_id = try storage.attachSequence(storage.poolId());
    try std.testing.expectEqual(source_id, storage_source_id);
    try storage.appendTokens(storage_source_id, 4);
    try cache.storeFromSequence("metal", &.{ 1, 2, 3, 4 }, source_id);
    try cache.manager.releaseSequence(source_id);
    try storage.releaseSequence(storage_source_id);

    const hit = (try cache.attachLongestPrefix("metal", &.{ 1, 2, 3, 4 }, 4)).?;
    try std.testing.expectEqual(@as(?usize, 4), storage.tokenCount(hit.sequence_id));
    cache.releaseAttachedPrefix(hit);
    try std.testing.expectEqual(@as(?usize, null), storage.tokenCount(hit.sequence_id));
}

test "prompt cache radix generated-prefix store is mode gated and page aligned" {
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
    try cache.manager.appendTokens(source_id, 5);

    try cache.storeGeneratedPrefixFromSequence("agent", &.{ 1, 2, 3, 4, 5 }, source_id);
    try std.testing.expectEqual(@as(usize, 0), cache.stats().live_entries);

    var radix_config = cache.config;
    radix_config.mode = .radix;
    cache.configure(radix_config);
    try cache.storeGeneratedPrefixFromSequence("agent", &.{ 1, 2, 3, 4, 5 }, source_id);
    try std.testing.expectEqual(@as(u64, 4), cache.stats().cached_tokens);
    const hit = (try cache.attachLongestPrefix("agent", &.{ 1, 2, 3, 4, 9, 10 }, 6)).?;
    try std.testing.expectEqual(@as(usize, 4), hit.token_count);
    try cache.manager.releaseSequence(hit.sequence_id);
}

test "prompt cache radix hash collision guard fails closed" {
    const allocator = std.testing.allocator;
    var cache = PromptPrefixCache.init(allocator);
    defer cache.deinit();
    cache.configure(.{ .enabled = true, .mode = .radix, .min_tokens = 2, .max_bytes = 1 << 20 });

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
    try cache.storeFromSequence("agent", &.{ 1, 2 }, source_id);

    const config = cache.pool_config.?;
    const real_hash = radixEdgeHash(config, "agent", null, &.{ 1, 2 });
    const real_id = cache.radix_edge_index.get(real_hash).?;
    const poisoned_hash = radixEdgeHash(config, "agent", null, &.{ 9, 10 });
    try cache.radix_edge_index.put(allocator, poisoned_hash, real_id);
    defer _ = cache.radix_edge_index.remove(poisoned_hash);

    try std.testing.expect((try cache.attachLongestPrefix("agent", &.{ 9, 10 }, 2)) == null);
    const stats_value = cache.stats();
    try std.testing.expectEqual(@as(u64, 1), stats_value.radix_collision_guards);
    try std.testing.expectEqual(@as(u64, 1), stats_value.radix_misses);
}

test "prompt cache mode changes clear unreachable entries" {
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
    try cache.manager.appendTokens(source_id, 2);
    try cache.storeFromSequence("agent", &.{ 1, 2 }, source_id);
    try std.testing.expectEqual(@as(usize, 1), cache.stats().live_entries);
    var radix_config = cache.config;
    radix_config.mode = .radix;
    cache.configure(radix_config);
    try std.testing.expectEqual(@as(usize, 0), cache.stats().live_entries);
    try std.testing.expectEqual(@as(usize, 0), cache.stats().live_bytes);
}
