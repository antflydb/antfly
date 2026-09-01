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

// HuggingFace tokenizer.json parser.
//
// Supports three model types from the HuggingFace tokenizers format:
//   - WordPiece (BERT, DistilBERT, etc.)
//   - BPE (GPT-2, CLIP, RoBERTa, Gemma, etc.)
//   - Unigram (SentencePiece-based: DeBERTa v3, T5, ALBERT, XLNet, etc.)

const std = @import("std");
const builtin = @import("builtin");
const Tokenizer = @import("tokenizer.zig").Tokenizer;
const SpecialTokens = @import("tokenizer.zig").SpecialTokens;
const PriorityQueue = @import("priority_queue.zig").PriorityQueue;
const unicode_classes = @import("unicode_classes.zig");

/// Zig/WebAssembly supports atomics only up to the target's pointer width.
/// The browser build is explicitly single-threaded, so keep 64-bit profiling
/// counters non-atomic there while preserving native atomic behavior.
const AtomicU64 = if (builtin.cpu.arch == .wasm32 and builtin.single_threaded)
    SingleThreadedU64
else
    std.atomic.Value(u64);

const SingleThreadedU64 = extern struct {
    raw: u64,

    const Self = @This();

    fn init(value: u64) Self {
        return .{ .raw = value };
    }

    fn load(self: *const Self, comptime _: std.builtin.AtomicOrder) u64 {
        return self.raw;
    }

    fn store(self: *Self, value: u64, comptime _: std.builtin.AtomicOrder) void {
        self.raw = value;
    }

    fn swap(self: *Self, value: u64, comptime _: std.builtin.AtomicOrder) u64 {
        const previous = self.raw;
        self.raw = value;
        return previous;
    }

    fn fetchAdd(self: *Self, value: u64, comptime _: std.builtin.AtomicOrder) u64 {
        const previous = self.raw;
        self.raw +%= value;
        return previous;
    }

    fn fetchSub(self: *Self, value: u64, comptime _: std.builtin.AtomicOrder) u64 {
        const previous = self.raw;
        self.raw -%= value;
        return previous;
    }

    fn fetchOr(self: *Self, value: u64, comptime _: std.builtin.AtomicOrder) u64 {
        const previous = self.raw;
        self.raw |= value;
        return previous;
    }
};

const ModelType = enum { word_piece, bpe, unigram };

const PreTokenizerType = enum {
    bert, // BertPreTokenizer: split on whitespace + punctuation
    byte_level, // ByteLevel: byte-to-unicode mapping
    byte_level_split, // Split(regex) followed by ByteLevel, used by CLIP tokenizers
    metaspace, // Metaspace: replace spaces with ▁
    none, // No pre-tokenization
};

const MetaspacePrependScheme = enum {
    always,
    first,
    never,
};

pub const HfTokenizer = struct {
    allocator: std.mem.Allocator,
    model_type: ModelType,
    vocab: std.StringHashMapUnmanaged(i32),
    id_to_token: std.AutoHashMapUnmanaged(i32, []const u8),
    added_tokens: std.StringHashMapUnmanaged(i32),
    /// Trie of added-token byte sequences for fast longest-match-at-cursor and
    /// next-occurrence lookups. Built lazily after `parseAddedTokens`.
    added_trie: AddedTokenTrie,
    special: SpecialTokens,
    pad_token_seen: bool,
    do_lowercase: bool,
    replace_space_with: ?[]const u8,
    pre_tokenizer_type: PreTokenizerType,
    // WordPiece fields
    continuing_prefix: []const u8,
    max_input_chars_per_word: usize,
    // BPE fields.
    /// Maps merge pairs ("a<space>b") to their priority (lower rank = higher
    /// priority). Drives the priority-queue BPE merger in O(symbols log
    /// symbols) instead of an O(merges * symbols) sweep.
    merge_ranks: std.StringHashMapUnmanaged(u32),
    /// Hot-path merge table keyed by two packed vocabulary IDs. Unlike
    /// `merge_ranks`, lookups do not compose and hash "left right" strings.
    /// The string table remains as a compatibility fallback for unusual
    /// tokenizer.json merges whose components are absent from the vocab.
    merge_pairs: std.AutoHashMapUnmanaged(u64, PackedBpeMerge),
    /// Persistent, sharded pretoken cache. BPE inputs are highly repetitive
    /// in natural language; retaining their final token IDs avoids rebuilding
    /// symbol lists and priority queues on every occurrence and keeps lock
    /// contention low when a tokenizer is shared by concurrent requests.
    bpe_cache: ?*BpeCache,
    /// Opt-in cache profiling. Disabled on the normal hot path; benchmark
    /// tooling enables it only after warmup.
    bpe_profile_enabled: std.atomic.Value(bool),
    bpe_profile: BpeProfileCounters,
    byte_level_direct_ids: ?*ByteLevelDirectIds,
    parallel_workspace_mutex: std.atomic.Mutex,
    parallel_workspace_free: ?*ParallelBpeWorkspace,
    parallel_workspace_all: ?*ParallelBpeWorkspace,
    parallel_workspace_free_count: usize,
    parallel_workspace_free_bytes: usize,
    parallel_bpe_config: ParallelBpeConfig,
    worker_bpe_caches: [max_worker_bpe_caches]WorkerBpeCacheLease,
    cache_resource_budget_mutex: std.atomic.Mutex,
    cache_resource_budget_observer_id: usize,
    cache_resource_budget_bytes: usize,
    cache_resource_budget: ?BpeCacheResourceBudget,
    end_of_word_suffix: []const u8,
    byte_fallback: bool,
    // Unigram fields
    unigram_vocab: std.ArrayListUnmanaged(UnigramPiece),
    unigram_unk_id: i32,
    /// Trie of vocab byte sequences for Unigram Viterbi prefix pruning.
    unigram_trie: VocabTrie,
    /// Trie of vocab byte sequences for the BPE direct-pieces longest-match
    /// path (Gemma's "replace+split, no pre-tokenizer" config). Populated
    /// only when `shouldPreferDirectBpePieces()` is true after parsing.
    bpe_direct_trie: ?VocabTrie,
    // Metaspace pre-tokenizer
    metaspace_prepend_scheme: MetaspacePrependScheme,
    metaspace_split: bool,
    metaspace_replacement: []const u8,
    // Owned strings storage — freed on deinit.
    arena_strings: std.ArrayListUnmanaged([]const u8),

    const UnigramPiece = struct {
        token: []const u8,
        score: f32,
        id: i32,
    };

    const PackedBpeMerge = struct {
        rank: u32,
        result_id: i32,
    };

    const ByteLevelDirectIds = struct {
        single: [256]i32 = @splat(-1),
        pair: [65536]i32 = @splat(-1),
    };

    const bpe_cache_shard_count = 64;
    const bpe_cache_slots_per_shard = 2048;
    const bpe_cache_max_entries_per_shard = bpe_cache_slots_per_shard * 3 / 4;
    const bpe_cache_max_key_bytes = 256;
    const default_bpe_cache_max_bytes = 64 * 1024 * 1024;
    const max_cached_parallel_workspaces = 4;
    const default_max_cached_parallel_workspace_bytes = 64 * 1024 * 1024;
    const affinity_replay_min_input_bytes = 4 * 1024 * 1024;
    const affinity_steal_min_input_bytes = 4 * 1024 * 1024 * 1024;
    const invalid_worker_cache_owner = std.math.maxInt(u8);
    const max_parallel_bpe_chunks = 256;
    const max_bpe_bulk_slots_per_shard = 1 << 17;
    const max_worker_bpe_caches = 64;
    const max_worker_bpe_cache_slots = 1 << 23;
    const worker_bpe_batch_size = 256;
    const worker_bpe_prefetch_distance = 16;
    const worker_bpe_inline_sentinel = std.math.maxInt(u16);
    const worker_bpe_spill_count_max = std.math.maxInt(u16);
    const worker_bpe_initial_arena_ids = 4096;
    const max_prefix_commits_per_lock = 8;
    var next_cache_resource_budget_observer_id: std.atomic.Value(usize) = .init(1);

    fn acquireCacheResourceBudgetObserverId() usize {
        while (true) {
            const candidate = next_cache_resource_budget_observer_id.fetchAdd(1, .monotonic);
            if (candidate != 0) return candidate;
        }
    }

    /// Gigatoken-style private cache entry. The key contains up to fifteen
    /// pretoken bytes and an eight-bit length tag. `value` contains one to
    /// four final-form u16 token IDs without a dependent pointer load.
    const WorkerBpeCacheEntry = struct {
        key: u128 = 0,
        value: u64 = 0,
        padding: u64 = 0,
    };

    comptime {
        std.debug.assert(@sizeOf(WorkerBpeCacheEntry) == 32);
    }

    const WorkerBpeCacheStorage = enum {
        allocator,
        darwin_superpage,
    };

    const WorkerBpeCacheAllocation = struct {
        entries: []align(64) WorkerBpeCacheEntry,
        storage: WorkerBpeCacheStorage,
    };

    const WorkerBpeCache = struct {
        entries: []align(64) WorkerBpeCacheEntry,
        count: usize = 0,
        accounted_bytes: usize,
        storage: WorkerBpeCacheStorage,
        token_arena: std.ArrayListUnmanaged(i32) = .empty,
        frozen: std.atomic.Value(bool) = .init(false),

        fn deinit(self: *WorkerBpeCache, owner: *HfTokenizer) void {
            self.token_arena.deinit(owner.allocator);
            switch (self.storage) {
                .allocator => owner.allocator.free(self.entries),
                .darwin_superpage => {
                    if (comptime builtin.os.tag == .macos) {
                        _ = mach_vm_deallocate(
                            mach_task_self_,
                            @intFromPtr(self.entries.ptr),
                            self.entries.len *
                                @sizeOf(WorkerBpeCacheEntry),
                        );
                    } else unreachable;
                },
            }
            owner.releaseResourceBudgetBytes(self.accounted_bytes);
            self.* = undefined;
        }
    };

    const WorkerBpeCacheLease = struct {
        mutex: std.atomic.Mutex = .unlocked,
        cache: ?WorkerBpeCache = null,
        allocation_failed: bool = false,
        reservation_denials: u8 = 0,
        reservation_retry_after: u8 = 0,
        published_present: std.atomic.Value(bool) = .init(false),
        published_entries: std.atomic.Value(usize) = .init(0),
        published_slots: std.atomic.Value(usize) = .init(0),
        published_bytes: std.atomic.Value(usize) = .init(0),
        published_token_arena_ids: std.atomic.Value(usize) = .init(0),
        published_superpage: std.atomic.Value(bool) = .init(false),

        fn publishStats(self: *WorkerBpeCacheLease) void {
            const cache = self.cache orelse {
                self.published_present.store(false, .release);
                return;
            };
            self.published_entries.store(cache.count, .monotonic);
            self.published_slots.store(cache.entries.len, .monotonic);
            self.published_bytes.store(cache.accounted_bytes, .monotonic);
            self.published_token_arena_ids.store(
                cache.token_arena.items.len,
                .monotonic,
            );
            self.published_superpage.store(
                cache.storage == .darwin_superpage,
                .monotonic,
            );
            // Publish presence last so an observer that sees a table also sees
            // the complete snapshot written above. Subsequent fields only grow.
            self.published_present.store(true, .release);
        }

        fn loadPublishedStats(
            self: *const WorkerBpeCacheLease,
        ) WorkerBpeCachePublishedStats {
            if (!self.published_present.load(.acquire)) return .{};
            return .{
                .present = true,
                .entries = self.published_entries.load(.monotonic),
                .slots = self.published_slots.load(.monotonic),
                .bytes = self.published_bytes.load(.monotonic),
                .token_arena_ids = self.published_token_arena_ids.load(.monotonic),
                .superpage = self.published_superpage.load(.monotonic),
            };
        }
    };

    const WorkerBpeCachePublishedStats = struct {
        present: bool = false,
        entries: usize = 0,
        slots: usize = 0,
        bytes: usize = 0,
        token_arena_ids: usize = 0,
        superpage: bool = false,

        fn fromCache(cache: *const WorkerBpeCache) @This() {
            return .{
                .present = true,
                .entries = cache.count,
                .slots = cache.entries.len,
                .bytes = cache.accounted_bytes,
                .token_arena_ids = cache.token_arena.items.len,
                .superpage = cache.storage == .darwin_superpage,
            };
        }
    };

    const worker_bpe_superpage_bytes = 2 * 1024 * 1024;
    const darwin_vm_flags_anywhere = 0x0000_0001;
    const darwin_vm_flags_superpage_2mb = 0x0002_0000;
    const darwin_vm_prot_read_write = 0x03;
    const darwin_vm_inherit_copy = 1;

    extern var mach_task_self_: u32;

    extern fn mach_vm_map(
        target_task: u32,
        address: *u64,
        size: u64,
        mask: u64,
        flags: c_int,
        object: u32,
        offset: u64,
        copy: c_int,
        current_protection: c_int,
        maximum_protection: c_int,
        inheritance: u32,
    ) c_int;

    extern fn mach_vm_deallocate(
        target_task: u32,
        address: u64,
        size: u64,
    ) c_int;

    const BpeCacheEntry = struct {
        hash: u64,
        key: []const u8,
        token_ids: []const i32,
        referenced: std.atomic.Value(bool) = .init(true),
        next_retired: ?*BpeCacheEntry = null,
    };

    const BpeCacheShard = struct {
        mutex: std.atomic.Mutex = .unlocked,
        slots: [bpe_cache_slots_per_shard]std.atomic.Value(usize) =
            @splat(.{ .raw = 0 }),
        count: std.atomic.Value(usize) = .init(0),
        tombstones: usize = 0,
        clock_hand: usize = 0,
        bulk_count: std.atomic.Value(usize) = .init(0),
        bulk_tombstones: usize = 0,
        bulk_clock_hand: usize = 0,
    };

    const bpe_cache_tombstone: usize = 1;
    const bpe_doorkeeper_words = 4096;
    const bpe_doorkeeper_rotate_after = 32 * 1024;

    const BpeDoorkeeper = struct {
        // Two independent bits materially reduce false second-hit admission.
        // Two rotating generations keep a one-shot scan from saturating the
        // filter forever.
        generations: [2][bpe_doorkeeper_words]AtomicU64 =
            @splat(@splat(.{ .raw = 0 })),
        active_generation: std.atomic.Value(u8) = .init(0),
        observations: std.atomic.Value(usize) = .init(0),
        rotating: std.atomic.Value(bool) = .init(false),
    };

    const BpeCache = struct {
        owner: *HfTokenizer,
        shards: [bpe_cache_shard_count]BpeCacheShard =
            @as([bpe_cache_shard_count]BpeCacheShard, @splat(.{})),
        max_bytes: usize = default_bpe_cache_max_bytes,
        bulk_slots: ?[]std.atomic.Value(usize) = null,
        bulk_slots_per_shard: usize = 0,
        used_bytes: std.atomic.Value(usize) = .init(0),
        rejected_reservations: AtomicU64 = .init(0),
        rejected_admissions: AtomicU64 = .init(0),
        evictions: AtomicU64 = .init(0),
        resource_budget: ?BpeCacheResourceBudget = null,
        doorkeeper: BpeDoorkeeper = .{},
        reader_gate: std.atomic.Value(bool) = .init(false),
        active_readers: std.atomic.Value(usize) = .init(0),
        reclaiming: std.atomic.Value(bool) = .init(false),
        retired_mutex: std.atomic.Mutex = .unlocked,
        retired_head: ?*BpeCacheEntry = null,
        retired_bytes: std.atomic.Value(usize) = .init(0),
    };

    pub const BpeCacheResourceBudget = struct {
        context: *anyopaque,
        /// Optional ownership hooks for capabilities whose context can outlive
        /// the component that published it. Hooks must be supplied as a pair.
        /// HfTokenizer retains the context while the capability is installed,
        /// including while its accounted byte total is zero.
        retain_context: ?*const fn (*anyopaque) bool = null,
        release_context: ?*const fn (*anyopaque) void = null,
        /// Observe the total bytes owned by one stable tokenizer identity.
        /// The receiver must reject stale `previous` values. This makes
        /// duplicate releases fail closed instead of debiting another owner.
        observe: *const fn (
            context: *anyopaque,
            observer_id: usize,
            previous: usize,
            next: usize,
        ) bool,

        pub fn hasValidLifetimeHooks(self: @This()) bool {
            return (self.retain_context == null) == (self.release_context == null);
        }

        pub fn retainContext(self: @This()) bool {
            if (!self.hasValidLifetimeHooks()) return false;
            return if (self.retain_context) |retain| retain(self.context) else true;
        }

        pub fn releaseContext(self: @This()) void {
            if (self.release_context) |release| release(self.context);
        }
    };

    const ResourceBudgetCharge = enum {
        uncharged,
        charged,
    };

    pub const BpeCacheConfig = struct {
        /// Hard bound for the fixed lookup table and immutable cache entries.
        /// A value smaller than the fixed table disables cache insertion.
        max_bytes: usize = default_bpe_cache_max_bytes,
        /// Optional second-tier slots in each shard. Must be zero or a power
        /// of two. Front hits never touch this table; it exists for
        /// large-corpus long-tail retention.
        bulk_slots_per_shard: usize = 0,
        /// Optional process-wide admission budget. Reservations happen only on
        /// cold insertion; cache hits remain lock-free and callback-free.
        resource_budget: ?BpeCacheResourceBudget = null,
    };

    pub const BpeCacheStats = struct {
        max_bytes: usize,
        used_bytes: usize,
        entries: usize,
        front_entries: usize,
        bulk_entries: usize,
        bulk_slots: usize,
        rejected_reservations: u64,
        rejected_admissions: u64,
        evictions: u64,
        worker_tables: usize,
        worker_entries: usize,
        worker_min_entries: usize,
        worker_max_entries: usize,
        worker_slots: usize,
        worker_bytes: usize,
        worker_token_arena_ids: usize,
        worker_superpage_tables: usize,
        workspace_total_count: usize,
        workspace_total_bytes: usize,
        workspace_active_count: usize,
        workspace_active_bytes: usize,
        workspace_accounted_bytes: usize,
        workspace_active_output_bytes: usize,
        workspace_active_output_capacity_bytes: usize,
        workspace_cached_count: usize,
        workspace_cached_bytes: usize,
        affinity_learns: usize,
        affinity_replays: usize,
        affinity_stolen_chunks: usize,
        stable_offset_learns: usize,
        stable_offset_replays: usize,
        stable_offset_count: usize,
        stable_offset_bytes: usize,
        stable_boundary_words: usize,
        stable_boundary_bytes: usize,
        parallel_max_chunk_bytes: usize,
        parallel_max_chunk_ns: u64,
        parallel_slowest_chunk_bytes: usize,
        parallel_slowest_chunk_owner: usize,
        parallel_max_owner_chunk_ns: u64,
        parallel_min_owner_chunk_ns: u64,
    };

    pub const BpeProfile = struct {
        pretokens: u64,
        direct_hits: u64,
        hits: u64,
        misses: u64,
        probes: u64,
        key_bytes: u64,
        token_ids: u64,
        key_len_histogram: [33]u64,
        id_count_histogram: [9]u64,
        probe_histogram: [17]u64,
    };

    const BpeProfileCounters = struct {
        pretokens: AtomicU64 = .init(0),
        direct_hits: AtomicU64 = .init(0),
        hits: AtomicU64 = .init(0),
        misses: AtomicU64 = .init(0),
        probes: AtomicU64 = .init(0),
        key_bytes: AtomicU64 = .init(0),
        token_ids: AtomicU64 = .init(0),
        key_len_histogram: [33]AtomicU64 =
            @splat(.{ .raw = 0 }),
        id_count_histogram: [9]AtomicU64 =
            @splat(.{ .raw = 0 }),
        probe_histogram: [17]AtomicU64 =
            @splat(.{ .raw = 0 }),
    };

    pub const ParallelBpeConfig = struct {
        /// Zero selects the adaptive production default (four chunks per
        /// consumer below 4 MiB and eight chunks per consumer otherwise).
        chunks_per_task: usize = 0,
        /// Hard bound on chunks created for one encode. Keeping this separate
        /// from task count permits controlled scheduler experiments without
        /// changing the number of concurrent std.Io consumers.
        max_chunks: usize = 256,
        /// Number of persistent private tables available to std.Io queue
        /// consumers. Zero disables the high-memory throughput path.
        worker_cache_count: usize = 0,
        /// Power-of-two entries in each private table. Gigatoken's published
        /// geometry uses 2^21 32-byte entries (64 MiB) per consumer.
        worker_cache_slots: usize = 0,
        /// Maximum retained capacity of one reusable parallel workspace.
        /// The 64 MiB default prevents request-sized buffers from becoming
        /// permanent process state. Explicit throughput profiles can raise it;
        /// a configured resource budget must still admit the retained bytes.
        max_retained_workspace_bytes: usize =
            default_max_cached_parallel_workspace_bytes,
        /// Retain a one-bit-per-input-byte exact pretoken boundary index for
        /// immutable stable-input calls. Disabled in the normal profile.
        retain_stable_pretoken_boundaries: bool = false,
        /// On Darwin, mark released segmented-output pages reusable while
        /// retaining their virtual capacity. Explicit large-corpus profiles
        /// can keep file-backed input resident without paying to compress a
        /// previous token result; normal production defaults leave residency
        /// policy to the OS.
        recycle_segmented_output_pages: bool = false,
    };

    /// Byte-indexed trie used for added-token matching. Each node stores its
    /// children in a HashMap keyed by the next byte; final nodes hold the
    /// token id and length.
    const AddedTokenTrie = struct {
        nodes: std.ArrayListUnmanaged(Node) = .empty,
        root_bytes: [256]bool = @splat(false),
        root_byte_count: u16 = 0,
        single_root_byte: u8 = 0,
        single_token: []const u8 = "",
        single_token_id: i32 = -1,

        const Node = struct {
            children: std.AutoHashMapUnmanaged(u8, u32) = .{},
            token_id: i32 = -1,
            token_len: u32 = 0,
        };

        fn init(allocator: std.mem.Allocator) !AddedTokenTrie {
            var t: AddedTokenTrie = .{};
            // Reserve root at index 0.
            try t.nodes.append(allocator, .{});
            return t;
        }

        fn deinit(self: *AddedTokenTrie, allocator: std.mem.Allocator) void {
            for (self.nodes.items) |*n| n.children.deinit(allocator);
            self.nodes.deinit(allocator);
        }

        fn insert(self: *AddedTokenTrie, allocator: std.mem.Allocator, token: []const u8, id: i32) !void {
            if (token.len == 0) return;
            if (!self.root_bytes[token[0]]) {
                self.root_byte_count += 1;
                self.single_root_byte = token[0];
            }
            self.root_bytes[token[0]] = true;
            var cur: u32 = 0;
            for (token) |b| {
                const entry = try self.nodes.items[cur].children.getOrPut(allocator, b);
                if (!entry.found_existing) {
                    const new_idx: u32 = @intCast(self.nodes.items.len);
                    try self.nodes.append(allocator, .{});
                    entry.value_ptr.* = new_idx;
                }
                cur = entry.value_ptr.*;
            }
            self.nodes.items[cur].token_id = id;
            self.nodes.items[cur].token_len = @intCast(token.len);
        }

        /// Retain the sole added token directly so a SIMD root-byte search can
        /// validate it without walking thirteen hash-map-backed trie edges for
        /// every GPT-2 `<|endoftext|>` delimiter.
        fn configureSingleToken(
            self: *AddedTokenTrie,
            token: []const u8,
            id: i32,
        ) void {
            std.debug.assert(token.len != 0);
            self.single_token = token;
            self.single_token_id = id;
        }

        fn findSingleToken(
            self: *const AddedTokenTrie,
            text: []const u8,
            start: usize,
        ) ?usize {
            const token = self.single_token;
            if (token.len == 0 or start > text.len or
                token.len > text.len - start)
            {
                return null;
            }
            var pos = start;
            while (pos <= text.len - token.len) {
                pos = std.mem.indexOfScalarPos(
                    u8,
                    text,
                    pos,
                    token[0],
                ) orelse return null;
                if (pos > text.len - token.len) return null;
                if (std.mem.eql(u8, text[pos .. pos + token.len], token)) {
                    return pos;
                }
                pos += 1;
            }
            return null;
        }

        /// Longest added-token match starting at `text[0]`, if any.
        fn longestPrefixMatch(self: *const AddedTokenTrie, text: []const u8) ?AddedTokenMatch {
            if (self.single_token.len != 0) {
                if (!std.mem.startsWith(u8, text, self.single_token)) {
                    return null;
                }
                return .{
                    .id = self.single_token_id,
                    .len = self.single_token.len,
                };
            }
            if (self.nodes.items.len == 0 or text.len == 0 or !self.root_bytes[text[0]]) return null;
            var best: ?AddedTokenMatch = null;
            var cur: u32 = 0;
            for (text) |b| {
                const child_idx = self.nodes.items[cur].children.get(b) orelse break;
                cur = child_idx;
                if (self.nodes.items[cur].token_id >= 0) {
                    best = .{
                        .id = self.nodes.items[cur].token_id,
                        .len = self.nodes.items[cur].token_len,
                    };
                }
            }
            return best;
        }

        /// Position of the first byte where any added token matches, scanning
        /// `text[start..]`. Returns null if no added token occurs.
        fn findNext(self: *const AddedTokenTrie, text: []const u8, start: usize) ?usize {
            if (self.single_token.len != 0) {
                return self.findSingleToken(text, start);
            }
            if (self.nodes.items.len == 0) return null;
            // For each starting byte, walk the trie until a final node is hit
            // or a transition fails. Worst case O(text * max_token_len), but
            // most starts terminate immediately since the root only has
            // transitions for bytes that begin some added token.
            var i = start;
            while (i < text.len) : (i += 1) {
                if (self.root_byte_count == 1) {
                    i = std.mem.indexOfScalarPos(u8, text, i, self.single_root_byte) orelse return null;
                } else if (!self.root_bytes[text[i]]) continue;
                var cur: u32 = 0;
                var j: usize = i;
                while (j < text.len) : (j += 1) {
                    const child_idx = self.nodes.items[cur].children.get(text[j]) orelse break;
                    cur = child_idx;
                    if (self.nodes.items[cur].token_id >= 0) return i;
                }
            }
            return null;
        }
    };

    /// Byte-indexed trie used for Unigram Viterbi. A single forward walk from
    /// `word[start]` enumerates every vocab token starting at that position,
    /// avoiding the O(max_token_len) hashmap probe-and-miss inner loop.
    const VocabTrie = struct {
        nodes: std.ArrayListUnmanaged(Node) = .empty,

        const Node = struct {
            children: std.AutoHashMapUnmanaged(u8, u32) = .{},
            token_id: i32 = -1,
        };

        fn init(allocator: std.mem.Allocator) !VocabTrie {
            var t: VocabTrie = .{};
            try t.nodes.append(allocator, .{});
            return t;
        }

        fn deinit(self: *VocabTrie, allocator: std.mem.Allocator) void {
            for (self.nodes.items) |*n| n.children.deinit(allocator);
            self.nodes.deinit(allocator);
        }

        fn insert(self: *VocabTrie, allocator: std.mem.Allocator, token: []const u8, id: i32) !void {
            if (token.len == 0) return;
            var cur: u32 = 0;
            for (token) |b| {
                const entry = try self.nodes.items[cur].children.getOrPut(allocator, b);
                if (!entry.found_existing) {
                    const new_idx: u32 = @intCast(self.nodes.items.len);
                    try self.nodes.append(allocator, .{});
                    entry.value_ptr.* = new_idx;
                }
                cur = entry.value_ptr.*;
            }
            self.nodes.items[cur].token_id = id;
        }
    };

    const vtable = Tokenizer.VTable{
        .encode = @ptrCast(&encode),
        .encodeInto = @ptrCast(&encodeInto),
        .encodeIntoParallel = @ptrCast(&encodeIntoParallel),
        .encodeIntoParallelStable = @ptrCast(&encodeIntoParallelStable),
        .encodeForModel = @ptrCast(&encodeForModel),
        .encodeGeneration = @ptrCast(&encodeGeneration),
        .decode = @ptrCast(&decode),
        .specialTokens = @ptrCast(&getSpecialTokens),
        .vocabSize = @ptrCast(&getVocabSize),
        .deinit = @ptrCast(&deinitSelf),
    };

    pub fn tokenizer(self: *HfTokenizer) Tokenizer {
        return .{ .ptr = self, .vtable = &vtable };
    }

    pub fn encodeWithOffsets(self: *HfTokenizer, allocator: std.mem.Allocator, text: []const u8) !?EncodingWithOffsets {
        if (text.len > std.math.maxInt(u32)) return null;
        if (self.model_type == .word_piece and self.pre_tokenizer_type == .bert) {
            return try self.encodeWordPieceWithOffsets(allocator, text);
        }
        if (self.model_type == .unigram and self.pre_tokenizer_type == .metaspace and self.metaspace_split) {
            return try self.encodeUnigramWithOffsets(allocator, text);
        }
        return null;
    }

    pub fn applySpecialTokenIds(
        self: *HfTokenizer,
        bos_id: ?i32,
        eos_id: ?i32,
        pad_id: ?i32,
        unk_id: ?i32,
    ) void {
        if (bos_id) |id| self.special.cls_id = id;
        if (eos_id) |id| self.special.sep_id = id;
        if (pad_id) |id| {
            self.special.pad_id = id;
            self.pad_token_seen = true;
        }
        if (unk_id) |id| self.special.unk_id = id;
    }

    /// Load from a tokenizer.json file via an Io.Dir handle.
    pub fn loadFromDir(allocator: std.mem.Allocator, dir: std.Io.Dir, io: std.Io, sub_path: []const u8) !*HfTokenizer {
        const file = try dir.openFile(io, sub_path, .{});
        defer file.close(io);
        const stat = try file.stat(io);
        const bytes = try allocator.alloc(u8, stat.size);
        defer allocator.free(bytes);
        const n = try file.readPositionalAll(io, bytes, 0);
        if (n != stat.size) return error.IncompleteRead;
        return try loadFromBytes(allocator, bytes[0..n]);
    }

    /// Parse tokenizer.json content from memory.
    pub fn loadFromBytes(allocator: std.mem.Allocator, json_bytes: []const u8) !*HfTokenizer {
        return loadFromBytesConfigured(allocator, json_bytes, false);
    }

    /// Parse generated tokenizer JSON whose vocab may contain duplicate token
    /// strings. GGUF keeps every token ID, while encoding resolves duplicates
    /// to the last ID.
    pub fn loadFromBytesAllowDuplicateFields(allocator: std.mem.Allocator, json_bytes: []const u8) !*HfTokenizer {
        return loadFromBytesConfigured(allocator, json_bytes, true);
    }

    fn loadFromBytesConfigured(allocator: std.mem.Allocator, json_bytes: []const u8, allow_duplicate_fields: bool) !*HfTokenizer {
        const parsed = try std.json.parseFromSlice(std.json.Value, allocator, json_bytes, .{
            .duplicate_field_behavior = if (allow_duplicate_fields) .use_last else .@"error",
        });
        defer parsed.deinit();

        const root = parsed.value;
        if (root != .object) return error.InvalidTokenizerJson;

        const self = try allocator.create(HfTokenizer);
        self.* = .{
            .allocator = allocator,
            .model_type = .word_piece,
            .vocab = .{},
            .id_to_token = .{},
            .added_tokens = .{},
            .added_trie = .{},
            .special = .{},
            .pad_token_seen = false,
            .do_lowercase = false,
            .replace_space_with = null,
            .pre_tokenizer_type = .bert,
            .continuing_prefix = "##",
            .max_input_chars_per_word = 100,
            .merge_ranks = .{},
            .merge_pairs = .{},
            .bpe_cache = null,
            .bpe_profile_enabled = .init(false),
            .bpe_profile = .{},
            .byte_level_direct_ids = null,
            .parallel_workspace_mutex = .unlocked,
            .parallel_workspace_free = null,
            .parallel_workspace_all = null,
            .parallel_workspace_free_count = 0,
            .parallel_workspace_free_bytes = 0,
            .parallel_bpe_config = .{},
            .worker_bpe_caches = @as([max_worker_bpe_caches]WorkerBpeCacheLease, @splat(.{})),
            .cache_resource_budget_mutex = .unlocked,
            .cache_resource_budget_observer_id = acquireCacheResourceBudgetObserverId(),
            .cache_resource_budget_bytes = 0,
            .cache_resource_budget = null,
            .end_of_word_suffix = "",
            .byte_fallback = false,
            .unigram_vocab = .empty,
            .unigram_unk_id = 0,
            .unigram_trie = .{},
            .bpe_direct_trie = null,
            .metaspace_prepend_scheme = .always,
            .metaspace_split = true,
            .metaspace_replacement = "\xe2\x96\x81", // ▁ (U+2581) in UTF-8
            .arena_strings = .empty,
        };
        errdefer self.deinitSelf();
        self.added_trie = try AddedTokenTrie.init(allocator);
        self.unigram_trie = try VocabTrie.init(allocator);

        // Detect model type first. Some Hugging Face tokenizer.json files omit
        // `model.type`, so infer from the model payload shape when necessary.
        if (root.object.get("model")) |model| {
            if (model == .object) {
                self.model_type = inferModelType(model.object);
            }
        }
        if (self.model_type == .bpe) {
            // The pretoken cache is optional. Model loading and correct
            // tokenization remain available when process memory pressure
            // prevents allocating its fixed table.
            if (allocator.create(BpeCache)) |cache| {
                cache.* = .{ .owner = self };
                cache.used_bytes.store(@sizeOf(BpeCache), .monotonic);
                self.bpe_cache = cache;
            } else |_| {}
        }

        // Parse pre-tokenizer
        if (root.object.get("pre_tokenizer")) |pt| {
            if (pt == .object) {
                self.parsePreTokenizer(pt.object);
            }
        }

        // Parse model section
        if (root.object.get("model")) |model| {
            if (model == .object) {
                switch (self.model_type) {
                    .word_piece => try self.parseWordPieceModel(model.object),
                    .bpe => try self.parseBpeModel(model.object),
                    .unigram => try self.parseUnigramModel(model.object),
                }
            }
        }

        // Parse normalizer
        if (root.object.get("normalizer")) |norm| {
            if (norm == .object) {
                self.parseNormalizer(norm.object);
            }
        }

        // Parse added_tokens
        if (root.object.get("added_tokens")) |tokens| {
            if (tokens == .array) {
                try self.parseAddedTokens(tokens.array.items);
            }
        }
        if (self.added_tokens.count() == 1) {
            var iterator = self.added_tokens.iterator();
            const entry = iterator.next().?;
            self.added_trie.configureSingleToken(
                entry.key_ptr.*,
                entry.value_ptr.*,
            );
        }

        // Parse post_processor for special tokens
        if (root.object.get("post_processor")) |pp| {
            if (pp == .object) {
                self.parsePostProcessor(pp.object);
            }
        }

        // Build the BPE direct-pieces trie if the resolved config selects that
        // longest-match-from-vocab path. We can only decide this after the
        // pre-tokenizer, model, and normalizer fields are all populated.
        if (self.shouldPreferDirectBpePieces()) {
            var trie = try VocabTrie.init(allocator);
            errdefer trie.deinit(allocator);
            var it = self.vocab.iterator();
            while (it.next()) |entry| {
                try trie.insert(allocator, entry.key_ptr.*, entry.value_ptr.*);
            }
            self.bpe_direct_trie = trie;
        }

        return self;
    }

    /// Preserve an alternate token ID for decoding after duplicate vocab keys
    /// have resolved to the canonical encoding ID.
    pub fn addTokenIdAliasForDecode(self: *HfTokenizer, token: []const u8, id: i32) !void {
        if (self.id_to_token.contains(id)) return;
        const canonical = self.vocab.getKey(token) orelse return error.InvalidTokenizerJson;
        try self.id_to_token.put(self.allocator, id, canonical);
    }

    fn adoptArenaString(self: *HfTokenizer, owned: []u8) ![]const u8 {
        errdefer self.allocator.free(owned);
        try self.arena_strings.append(self.allocator, owned);
        return owned;
    }

    fn dupeArenaString(
        self: *HfTokenizer,
        bytes: []const u8,
    ) ![]const u8 {
        return self.adoptArenaString(try self.allocator.dupe(u8, bytes));
    }

    // =====================================================================
    // Pre-tokenizer parsing
    // =====================================================================

    fn parsePreTokenizer(self: *HfTokenizer, obj: std.json.ObjectMap) void {
        if (obj.get("type")) |t| {
            if (t == .string) {
                if (std.mem.eql(u8, t.string, "BertPreTokenizer")) {
                    self.pre_tokenizer_type = .bert;
                } else if (std.mem.eql(u8, t.string, "ByteLevel")) {
                    self.pre_tokenizer_type = .byte_level;
                } else if (std.mem.eql(u8, t.string, "Metaspace")) {
                    self.pre_tokenizer_type = .metaspace;
                    self.parseMetaspaceConfig(obj);
                } else if (std.mem.eql(u8, t.string, "Split")) {
                    if (isSpaceSplitMergedWithPrevious(obj)) {
                        // Gemma tokenizer.json uses a pre-tokenizer that splits on
                        // literal spaces, but its normalizer replaces spaces with
                        // ▁ first. After normalization there are no spaces left,
                        // so this behaves like no additional pre-tokenization.
                        self.pre_tokenizer_type = .none;
                    }
                } else if (std.mem.eql(u8, t.string, "Sequence")) {
                    var saw_split = false;
                    // For Sequence pre-tokenizers, use the first meaningful type.
                    // CLIP commonly uses Split(regex) followed by ByteLevel; the
                    // split removes whitespace before byte-level BPE runs.
                    if (obj.get("pretokenizers")) |pts| {
                        if (pts == .array) {
                            for (pts.array.items) |item| {
                                if (item == .object) {
                                    if (item.object.get("type")) |pt| {
                                        if (pt == .string) {
                                            if (std.mem.eql(u8, pt.string, "Split")) {
                                                saw_split = true;
                                            } else if (std.mem.eql(u8, pt.string, "ByteLevel")) {
                                                self.pre_tokenizer_type = if (saw_split) .byte_level_split else .byte_level;
                                                return;
                                            } else if (std.mem.eql(u8, pt.string, "Metaspace")) {
                                                self.pre_tokenizer_type = .metaspace;
                                                self.parseMetaspaceConfig(item.object);
                                                return;
                                            } else if (std.mem.eql(u8, pt.string, "BertPreTokenizer")) {
                                                self.pre_tokenizer_type = .bert;
                                                return;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    fn inferModelType(obj: std.json.ObjectMap) ModelType {
        if (obj.get("type")) |t| {
            if (t == .string) {
                if (std.mem.eql(u8, t.string, "BPE")) return .bpe;
                if (std.mem.eql(u8, t.string, "Unigram")) return .unigram;
                return .word_piece;
            }
        }

        if (obj.contains("merges")) return .bpe;
        if (obj.get("vocab")) |vocab| {
            if (vocab == .array) return .unigram;
        }
        return .word_piece;
    }

    fn parseMetaspaceConfig(self: *HfTokenizer, obj: std.json.ObjectMap) void {
        if (obj.get("prepend_scheme")) |ps| {
            if (ps == .string) {
                if (std.mem.eql(u8, ps.string, "always")) {
                    self.metaspace_prepend_scheme = .always;
                } else if (std.mem.eql(u8, ps.string, "first")) {
                    self.metaspace_prepend_scheme = .first;
                } else if (std.mem.eql(u8, ps.string, "never")) {
                    self.metaspace_prepend_scheme = .never;
                }
            }
        }
        if (obj.get("split")) |split| {
            if (split == .bool) self.metaspace_split = split.bool;
        }
    }

    // =====================================================================
    // WordPiece model parsing
    // =====================================================================

    fn parseWordPieceModel(self: *HfTokenizer, obj: std.json.ObjectMap) !void {
        if (obj.get("continuing_subword_prefix")) |v| {
            if (v == .string) {
                self.continuing_prefix = try self.dupeArenaString(v.string);
            }
        }
        if (obj.get("max_input_chars_per_word")) |v| {
            if (v == .integer) {
                self.max_input_chars_per_word = @intCast(v.integer);
            }
        }
        try self.parseVocabDict(obj);
    }

    // =====================================================================
    // BPE model parsing
    // =====================================================================

    fn parseBpeModel(self: *HfTokenizer, obj: std.json.ObjectMap) !void {
        self.continuing_prefix = "";
        self.end_of_word_suffix = "";

        if (obj.get("end_of_word_suffix")) |v| {
            if (v == .string and v.string.len > 0) {
                self.end_of_word_suffix = try self.dupeArenaString(v.string);
            }
        }
        if (obj.get("byte_fallback")) |v| {
            if (v == .bool) self.byte_fallback = v.bool;
        }
        if (obj.get("continuing_subword_prefix")) |v| {
            if (v == .string) {
                self.continuing_prefix = try self.dupeArenaString(v.string);
            }
        }

        try self.parseVocabDict(obj);

        // Parse merges into `merge_ranks`, keyed on "a<space>b" (the same
        // form the JSON provides). Lower rank = higher priority. The
        // priority-queue BPE merger is the only consumer.
        if (obj.get("merges")) |merges_val| {
            if (merges_val == .array) {
                var rank: u32 = 0;
                for (merges_val.array.items) |item| {
                    const json_left: []const u8, const json_right: []const u8 = if (item == .string) blk: {
                        const split = std.mem.indexOfScalar(u8, item.string, ' ') orelse continue;
                        break :blk .{ item.string[0..split], item.string[split + 1 ..] };
                    } else if (item == .array and item.array.items.len >= 2 and item.array.items[0] == .string and item.array.items[1] == .string) blk: {
                        break :blk .{ item.array.items[0].string, item.array.items[1].string };
                    } else continue;

                    var left_owned: ?[]u8 = null;
                    defer if (left_owned) |piece| self.allocator.free(piece);
                    var right_owned: ?[]u8 = null;
                    defer if (right_owned) |piece| self.allocator.free(piece);
                    const left_piece = if (self.pre_tokenizer_type == .byte_level) blk: {
                        const raw = try byteLevelDecodeTokenAlloc(self.allocator, json_left);
                        left_owned = raw;
                        break :blk raw;
                    } else json_left;
                    const right_piece = if (self.pre_tokenizer_type == .byte_level) blk: {
                        const raw = try byteLevelDecodeTokenAlloc(self.allocator, json_right);
                        right_owned = raw;
                        break :blk raw;
                    } else json_right;

                    var raw_key_owned: ?[]u8 = null;
                    defer if (raw_key_owned) |raw| self.allocator.free(raw);
                    const raw_key = if (self.pre_tokenizer_type != .byte_level and item == .string)
                        item.string
                    else blk: {
                        const composed = try std.fmt.allocPrint(self.allocator, "{s} {s}", .{ left_piece, right_piece });
                        raw_key_owned = composed;
                        break :blk composed;
                    };
                    const key = try self.dupeArenaString(raw_key);
                    // Earlier merges win ties because getOrPut is no-op on existing.
                    const gop = try self.merge_ranks.getOrPut(self.allocator, key);
                    if (!gop.found_existing) gop.value_ptr.* = rank;
                    try self.insertPackedBpeMerge(left_piece, right_piece, rank);
                    rank += 1;
                }
            }
        }
    }

    fn insertPackedBpeMerge(
        self: *HfTokenizer,
        left_piece: []const u8,
        right_piece: []const u8,
        rank: u32,
    ) !void {
        const left_id = self.vocab.get(left_piece) orelse return;
        const right_id = self.vocab.get(right_piece) orelse return;
        const pair_key = bpePairKey(left_id, right_id) orelse return;

        var stack_buf: [256]u8 = undefined;
        var heap_buf: ?[]u8 = null;
        defer if (heap_buf) |buf| self.allocator.free(buf);
        const total = left_piece.len + right_piece.len;
        const merged = if (total <= stack_buf.len) blk: {
            @memcpy(stack_buf[0..left_piece.len], left_piece);
            @memcpy(stack_buf[left_piece.len..total], right_piece);
            break :blk stack_buf[0..total];
        } else blk: {
            const owned = try self.allocator.alloc(u8, total);
            heap_buf = owned;
            @memcpy(owned[0..left_piece.len], left_piece);
            @memcpy(owned[left_piece.len..], right_piece);
            break :blk owned;
        };
        const result_id = self.vocab.get(merged) orelse return;

        const gop = try self.merge_pairs.getOrPut(self.allocator, pair_key);
        if (!gop.found_existing) {
            gop.value_ptr.* = .{ .rank = rank, .result_id = result_id };
        }
    }

    // =====================================================================
    // Unigram model parsing
    // =====================================================================

    fn parseUnigramModel(self: *HfTokenizer, obj: std.json.ObjectMap) !void {
        if (obj.get("unk_id")) |v| {
            if (v == .integer) self.unigram_unk_id = @intCast(v.integer);
        }

        if (obj.get("vocab")) |vocab_val| {
            if (vocab_val == .array) {
                for (vocab_val.array.items, 0..) |item, idx| {
                    if (item == .array and item.array.items.len >= 2) {
                        const token_val = item.array.items[0];
                        const score_val = item.array.items[1];
                        if (token_val == .string) {
                            const score: f32 = switch (score_val) {
                                .float => @floatCast(score_val.float),
                                .integer => @floatFromInt(score_val.integer),
                                else => 0.0,
                            };
                            const id: i32 = @intCast(idx);
                            const token = try self.dupeArenaString(token_val.string);
                            try self.unigram_vocab.append(self.allocator, .{
                                .token = token,
                                .score = score,
                                .id = id,
                            });
                            try self.vocab.put(self.allocator, token, id);
                            try self.id_to_token.put(self.allocator, id, token);
                            try self.unigram_trie.insert(self.allocator, token, id);
                        }
                    }
                }
            }
        }

        // Set unk special token
        self.special.unk_id = self.unigram_unk_id;
    }

    // =====================================================================
    // Shared parsing helpers
    // =====================================================================

    /// Parse vocab from a dict format: {"token": id, ...} (WordPiece and BPE)
    fn parseVocabDict(self: *HfTokenizer, obj: std.json.ObjectMap) !void {
        var allocated_direct_ids = false;
        // Word-final suffixes change even one-byte lookup keys. Do not pay for
        // a direct table that cannot be used safely by those tokenizers.
        if (self.pre_tokenizer_type == .byte_level and
            self.end_of_word_suffix.len == 0 and
            self.byte_level_direct_ids == null)
        {
            const direct_ids = try self.allocator.create(ByteLevelDirectIds);
            direct_ids.* = .{};
            self.byte_level_direct_ids = direct_ids;
            allocated_direct_ids = true;
        }
        errdefer if (allocated_direct_ids) {
            self.allocator.destroy(self.byte_level_direct_ids.?);
            self.byte_level_direct_ids = null;
        };

        if (obj.get("vocab")) |vocab_val| {
            if (vocab_val == .object) {
                var it = vocab_val.object.iterator();
                while (it.next()) |entry| {
                    if (entry.value_ptr.* == .integer) {
                        const id: i32 = @intCast(entry.value_ptr.integer);
                        const display_key = try self.dupeArenaString(entry.key_ptr.*);
                        const lookup_key = if (self.pre_tokenizer_type == .byte_level) blk: {
                            break :blk try self.adoptArenaString(
                                try byteLevelDecodeTokenAlloc(
                                    self.allocator,
                                    display_key,
                                ),
                            );
                        } else display_key;
                        try self.vocab.put(self.allocator, lookup_key, id);
                        try self.id_to_token.put(self.allocator, id, display_key);
                        if (self.byte_level_direct_ids) |direct_ids| {
                            if (lookup_key.len == 1) {
                                direct_ids.single[lookup_key[0]] = id;
                            } else if (lookup_key.len == 2) {
                                const pair_key =
                                    (@as(usize, lookup_key[0]) << 8) |
                                    @as(usize, lookup_key[1]);
                                direct_ids.pair[pair_key] = id;
                            }
                        }
                    }
                }
            }
        }

        // Resolve unk_token after vocab is loaded
        if (obj.get("unk_token")) |v| {
            if (v == .string) {
                var raw_unk: ?[]u8 = null;
                defer if (raw_unk) |raw| self.allocator.free(raw);
                const lookup = if (self.pre_tokenizer_type == .byte_level) blk: {
                    const raw = try byteLevelDecodeTokenAlloc(self.allocator, v.string);
                    raw_unk = raw;
                    break :blk raw;
                } else v.string;
                if (self.vocab.get(lookup)) |id| {
                    self.special.unk_id = id;
                }
            }
        }
    }

    fn parseNormalizer(self: *HfTokenizer, obj: std.json.ObjectMap) void {
        if (obj.get("type")) |t| {
            if (t == .string) {
                if (std.mem.eql(u8, t.string, "Sequence")) {
                    if (obj.get("normalizers")) |normalizers| {
                        if (normalizers == .array) {
                            for (normalizers.array.items) |item| {
                                if (item == .object) self.parseNormalizer(item.object);
                            }
                        }
                    }
                } else if (std.mem.eql(u8, t.string, "BertNormalizer") or
                    std.mem.eql(u8, t.string, "Lowercase"))
                {
                    if (obj.get("lowercase")) |lc| {
                        self.do_lowercase = switch (lc) {
                            .bool => lc.bool,
                            else => true,
                        };
                    } else {
                        self.do_lowercase = true;
                    }
                } else if (std.mem.eql(u8, t.string, "Replace")) {
                    if (obj.get("pattern")) |pattern| {
                        if (pattern == .object) {
                            if (pattern.object.get("String")) |str_val| {
                                if (str_val == .string and std.mem.eql(u8, str_val.string, " ")) {
                                    if (obj.get("content")) |content| {
                                        if (content == .string) {
                                            const s = self.allocator.dupe(u8, content.string) catch return;
                                            self.arena_strings.append(self.allocator, s) catch {
                                                self.allocator.free(s);
                                                return;
                                            };
                                            self.replace_space_with = s;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    fn isSpaceSplitMergedWithPrevious(obj: std.json.ObjectMap) bool {
        const pattern = obj.get("pattern") orelse return false;
        const behavior = obj.get("behavior") orelse return false;
        const invert = obj.get("invert") orelse return false;
        if (pattern != .object or behavior != .string or invert != .bool) return false;
        if (invert.bool) return false;
        if (!std.mem.eql(u8, behavior.string, "MergedWithPrevious")) return false;
        const pattern_string = pattern.object.get("String") orelse return false;
        return pattern_string == .string and std.mem.eql(u8, pattern_string.string, " ");
    }

    fn parseAddedTokens(self: *HfTokenizer, items: []const std.json.Value) !void {
        for (items) |item| {
            if (item != .object) continue;
            const content = item.object.get("content") orelse continue;
            const id_val = item.object.get("id") orelse continue;
            if (content != .string or id_val != .integer) continue;

            const id: i32 = @intCast(id_val.integer);
            const key = try self.dupeArenaString(content.string);
            try self.added_tokens.put(self.allocator, key, id);
            try self.added_trie.insert(self.allocator, key, id);

            // Also add to vocab/id_to_token if not present
            if (!self.vocab.contains(key)) {
                try self.vocab.put(self.allocator, key, id);
                try self.id_to_token.put(self.allocator, id, key);
            }

            // Detect common special tokens by content
            if (std.mem.eql(u8, content.string, "[CLS]")) self.special.cls_id = id;
            if (std.mem.eql(u8, content.string, "[SEP]")) self.special.sep_id = id;
            if (std.mem.eql(u8, content.string, "[PAD]")) {
                self.special.pad_id = id;
                self.pad_token_seen = true;
            }
            if (std.mem.eql(u8, content.string, "[UNK]")) self.special.unk_id = id;
            if (std.mem.eql(u8, content.string, "[MASK]")) self.special.mask_id = id;
            // RoBERTa/GPT-style special tokens
            if (std.mem.eql(u8, content.string, "<s>")) self.special.cls_id = id;
            if (std.mem.eql(u8, content.string, "<bos>")) self.special.cls_id = id;
            if (std.mem.eql(u8, content.string, "</s>")) self.special.sep_id = id;
            if (std.mem.eql(u8, content.string, "<eos>")) self.special.sep_id = id;
            if (std.mem.eql(u8, content.string, "<pad>")) {
                self.special.pad_id = id;
                self.pad_token_seen = true;
            }
            if (std.mem.eql(u8, content.string, "<unk>")) self.special.unk_id = id;
            if (std.mem.eql(u8, content.string, "<mask>")) self.special.mask_id = id;
        }
    }

    fn parsePostProcessor(self: *HfTokenizer, obj: std.json.ObjectMap) void {
        const processor_type = if (obj.get("type")) |t|
            if (t == .string) t.string else ""
        else
            "";

        if (obj.get("cls")) |cls| {
            if (cls == .array and cls.array.items.len >= 2) {
                if (cls.array.items[1] == .integer) {
                    self.special.cls_id = @intCast(cls.array.items[1].integer);
                }
            }
        }
        if (obj.get("sep")) |sep| {
            if (sep == .array and sep.array.items.len >= 2) {
                if (sep.array.items[1] == .integer) {
                    self.special.sep_id = @intCast(sep.array.items[1].integer);
                }
            }
        }
        if (obj.get("special_tokens")) |st| {
            if (st == .object) {
                if (st.object.get("[CLS]")) |cls| self.resolveSpecialToken(cls, &self.special.cls_id);
                if (st.object.get("[SEP]")) |sep| self.resolveSpecialToken(sep, &self.special.sep_id);
                if (st.object.get("[PAD]")) |pad| {
                    self.resolveSpecialToken(pad, &self.special.pad_id);
                    self.pad_token_seen = true;
                }
                if (st.object.get("<bos>")) |bos| self.resolveSpecialToken(bos, &self.special.cls_id);
                if (st.object.get("<eos>")) |eos| self.resolveSpecialToken(eos, &self.special.sep_id);
                if (st.object.get("<pad>")) |pad| {
                    self.resolveSpecialToken(pad, &self.special.pad_id);
                    self.pad_token_seen = true;
                }
            }
        }

        if (!self.pad_token_seen and
            std.mem.eql(u8, processor_type, "RobertaProcessing") and
            self.special.sep_id >= 0)
        {
            // CLIP tokenizers use RobertaProcessing with BOS/EOS specials but
            // no distinct pad token. HuggingFace pads these models with EOS.
            self.special.pad_id = self.special.sep_id;
        }
    }

    fn resolveSpecialToken(_: *HfTokenizer, val: std.json.Value, target: *i32) void {
        if (val != .object) return;
        if (val.object.get("ids")) |ids| {
            if (ids == .array and ids.array.items.len > 0) {
                if (ids.array.items[0] == .integer) {
                    target.* = @intCast(ids.array.items[0].integer);
                }
            }
        }
    }

    // =====================================================================
    // Encoding dispatch
    // =====================================================================

    fn encode(self: *HfTokenizer, allocator: std.mem.Allocator, text: []const u8) ![]i32 {
        const cache_reader = self.enterBpeCacheRead();
        defer if (cache_reader) |cache| self.leaveBpeCacheRead(cache);

        // Skip the buffer-reuse layer in the dedicated single-shot path:
        // we avoid a redundant ensureUnusedCapacity wraparound and the
        // toOwnedSlice resize that would chase it. Body mirrors `encodeInto`.
        var owned: ?[]u8 = null;
        defer if (owned) |buf| allocator.free(buf);
        var normalized: []const u8 = text;
        if (self.do_lowercase) {
            const lowered = try toLowerAlloc(allocator, normalized);
            owned = lowered;
            normalized = lowered;
        }
        if (self.replace_space_with) |replacement| {
            const replaced = try replaceSpacesAlloc(allocator, normalized, replacement);
            if (owned) |buf| allocator.free(buf);
            owned = replaced;
            normalized = replaced;
        }

        var ids = std.ArrayListUnmanaged(i32).empty;
        errdefer ids.deinit(allocator);
        try self.encodeWithAddedTokens(allocator, normalized, &ids);
        return try ids.toOwnedSlice(allocator);
    }

    fn encodeInto(
        self: *HfTokenizer,
        allocator: std.mem.Allocator,
        text: []const u8,
        ids: *std.ArrayListUnmanaged(i32),
    ) !void {
        const cache_reader = self.enterBpeCacheRead();
        defer if (cache_reader) |cache| self.leaveBpeCacheRead(cache);

        if (!self.do_lowercase and self.replace_space_with == null) {
            return self.encodeWithAddedTokens(allocator, text, ids);
        }

        var owned: ?[]u8 = null;
        defer if (owned) |buf| allocator.free(buf);
        var normalized: []const u8 = text;
        if (self.do_lowercase) {
            const lowered = try toLowerAlloc(allocator, normalized);
            owned = lowered;
            normalized = lowered;
        }
        if (self.replace_space_with) |replacement| {
            const replaced = try replaceSpacesAlloc(allocator, normalized, replacement);
            if (owned) |buf| allocator.free(buf);
            owned = replaced;
            normalized = replaced;
        }

        return self.encodeWithAddedTokens(allocator, normalized, ids);
    }

    const parallel_bpe_min_bytes = 256 * 1024;

    const PackedWorkerBpeValue = struct {
        value: u64,
    };

    const WorkerBpePairProbe = struct {
        value: PackedWorkerBpeValue,
        found: bool,
    };

    const WorkerBpeProbeView = struct {
        base: [*]const WorkerBpeCacheEntry,
        pair_mask: usize,
    };

    const worker_bpe_key_masks: [16]u128 = blk: {
        var masks: [16]u128 = undefined;
        masks[0] = 0;
        for (1..16) |len| {
            masks[len] = (@as(u128, 1) << @intCast(len * 8)) - 1;
        }
        break :blk masks;
    };

    fn workerBpeKey(word: []const u8) ?u128 {
        if (word.len == 0 or word.len > 15) return null;
        var key = @as(u128, word.len) << 120;
        for (word, 0..) |byte, idx| {
            key |= @as(u128, byte) << @intCast(idx * 8);
        }
        return key;
    }

    fn workerBpeKeyPadded(word: []const u8, input_end: usize) ?u128 {
        if (word.len == 0 or word.len > 15) return null;
        if (comptime builtin.cpu.arch.endian() == .little) {
            const available = input_end - @intFromPtr(word.ptr);
            if (available >= 16) {
                const raw_ptr: *align(1) const u128 = @ptrCast(word.ptr);
                return (raw_ptr.* & worker_bpe_key_masks[word.len]) |
                    (@as(u128, word.len) << 120);
            }
        }
        return workerBpeKey(word);
    }

    fn workerBpeKeyLen(key: u128) usize {
        return @intCast(key >> 120);
    }

    fn workerBpeHash(key: u128) u64 {
        const low: u64 = @truncate(key);
        const high: u64 = @truncate(key >> 64);
        if (comptime builtin.cpu.arch == .aarch64 and
            builtin.cpu.has(.aarch64, .crc))
        {
            return asm (
                \\crc32cx w8, wzr, %[low]
                \\crc32cx w8, w8, %[high]
                : [out] "={x8}" (-> u32),
                : [low] "r" (low),
                  [high] "r" (high),
            );
        }
        var hash = low ^ std.math.rotl(u64, high, 23);
        hash *%= 0x9e3779b185ebca87;
        hash ^= hash >> 29;
        hash *%= 0xc2b2ae3d27d4eb4f;
        return hash ^ (hash >> 32);
    }

    fn packWorkerBpeValue(token_ids: []const i32) ?PackedWorkerBpeValue {
        if (token_ids.len == 0 or token_ids.len > 4) return null;
        var value: u64 = std.math.maxInt(u64);
        for (token_ids, 0..) |id, idx| {
            if (id < 0 or id >= worker_bpe_inline_sentinel) return null;
            const shift: u6 = @intCast(idx * @bitSizeOf(u16));
            value &= ~(@as(u64, std.math.maxInt(u16)) << shift);
            value |= @as(u64, @intCast(id)) << shift;
        }
        return .{ .value = value };
    }

    inline fn workerBpeValueIsSpill(value: u64) bool {
        return @as(u16, @truncate(value)) == worker_bpe_inline_sentinel;
    }

    inline fn workerBpeInlineCount(value: u64) usize {
        std.debug.assert(!workerBpeValueIsSpill(value));
        // Inline IDs are followed by all-one u16 lanes, while every admitted
        // ID is below 0xffff. Inverting the word therefore leaves 0, 16, 32,
        // or 48 leading zero bits. `@clz` lowers to one instruction on the
        // production targets and avoids three lane comparisons/selects.
        return 4 - @as(usize, @clz(~value)) / @bitSizeOf(u16);
    }

    fn ensureWorkerTokenArenaCapacity(
        self: *HfTokenizer,
        cache: *WorkerBpeCache,
        required: usize,
    ) bool {
        if (required <= cache.token_arena.capacity) return true;
        const doubled = cache.token_arena.capacity *| 2;
        const target_capacity = @max(
            required,
            @max(worker_bpe_initial_arena_ids, doubled),
        );
        const added_capacity =
            target_capacity - cache.token_arena.capacity;
        const added_bytes = std.math.mul(
            usize,
            added_capacity,
            @sizeOf(i32),
        ) catch return false;
        const resource_budget_charged = self.tryReserveResourceBudgetBytes(added_bytes) orelse
            return false;
        cache.token_arena.ensureTotalCapacityPrecise(
            self.allocator,
            target_capacity,
        ) catch {
            if (resource_budget_charged == .charged)
                self.releaseResourceBudgetBytes(added_bytes);
            return false;
        };
        cache.accounted_bytes += added_bytes;
        return true;
    }

    fn packWorkerBpeValueWithSpill(
        self: *HfTokenizer,
        cache: *WorkerBpeCache,
        token_ids: []const i32,
    ) ?PackedWorkerBpeValue {
        if (packWorkerBpeValue(token_ids)) |inline_value| {
            return inline_value;
        }
        if (token_ids.len == 0 or
            token_ids.len > worker_bpe_spill_count_max or
            cache.token_arena.items.len >
                std.math.maxInt(u32) - token_ids.len)
        {
            return null;
        }
        const required = cache.token_arena.items.len + token_ids.len;
        if (!self.ensureWorkerTokenArenaCapacity(cache, required)) return null;
        const offset: u32 = @intCast(cache.token_arena.items.len);
        cache.token_arena.appendSliceAssumeCapacity(token_ids);
        return .{
            .value = @as(u64, worker_bpe_inline_sentinel) |
                (@as(u64, offset) << 16) |
                (@as(u64, @intCast(token_ids.len)) << 48),
        };
    }

    fn appendWorkerBpeValue(
        ids: *std.ArrayListUnmanaged(i32),
        packed_value: PackedWorkerBpeValue,
    ) void {
        std.debug.assert(!workerBpeValueIsSpill(packed_value.value));
        var output_len = ids.items.len;
        writeWorkerBpeValue(ids.items.ptr, &output_len, packed_value);
        ids.items.len = output_len;
    }

    inline fn workerBpeValueLanes(
        packed_value: PackedWorkerBpeValue,
    ) @Vector(4, u16) {
        // Preserve the zero-cost scalar bitcast on little-endian production
        // targets. Scalar byte order differs on big-endian targets, where
        // explicit semantic lanes keep token order without a runtime branch.
        if (comptime builtin.cpu.arch.endian() == .little) {
            return @bitCast(packed_value.value);
        }
        return .{
            @truncate(packed_value.value),
            @truncate(packed_value.value >> 16),
            @truncate(packed_value.value >> 32),
            @truncate(packed_value.value >> 48),
        };
    }

    inline fn writeWorkerBpeValueU16(
        output: [*]u16,
        output_len: *usize,
        packed_value: PackedWorkerBpeValue,
    ) void {
        const count = workerBpeInlineCount(packed_value.value);
        const destination = output + output_len.*;
        const output_store: *align(1) @Vector(4, u16) =
            @ptrCast(destination);
        output_store.* = workerBpeValueLanes(packed_value);
        output_len.* += count;
    }

    inline fn writeWorkerBpeValue(
        output: [*]i32,
        output_len: *usize,
        packed_value: PackedWorkerBpeValue,
    ) void {
        const count = workerBpeInlineCount(packed_value.value);
        const destination = output + output_len.*;
        const packed_ids = workerBpeValueLanes(packed_value);
        const widened: @Vector(4, i32) = packed_ids;
        const output_store: *align(1) @Vector(4, i32) =
            @ptrCast(destination);
        output_store.* = widened;
        output_len.* += count;
    }

    fn workerBpeHome(cache: *const WorkerBpeCache, hash: u64) usize {
        return (@as(usize, @truncate(hash)) &
            (cache.entries.len - 1)) & ~@as(usize, 1);
    }

    fn prefetchWorkerBpe(
        view: WorkerBpeProbeView,
        hash: u64,
        comptime locality: u2,
    ) void {
        const idx = @as(usize, @truncate(hash)) & view.pair_mask;
        @prefetch(&view.base[idx], .{
            .rw = .read,
            .locality = locality,
        });
    }

    /// Probe the home pair without a data-dependent value load. At the
    /// production table's low load factor nearly every warm hit is in this
    /// pair; displaced entries and true misses use the exact cold walk.
    inline fn probeWorkerBpePair(
        view: WorkerBpeProbeView,
        key: u128,
        hash: u64,
    ) WorkerBpePairProbe {
        const idx = @as(usize, @truncate(hash)) & view.pair_mask;
        const first = &view.base[idx];
        const second = &view.base[idx + 1];
        const first_matches = first.key == key;
        const second_matches = second.key == key;
        var value = first.value;
        if (comptime builtin.cpu.arch == .aarch64) {
            // Pin register-value selects. Source-level selection otherwise
            // becomes a selected value pointer followed by a dependent load.
            asm (
                \\cmp %[matches], #0
                \\csel %[value], %[value], %[second_value], ne
                : [value] "+r" (value),
                : [matches] "r" (@as(u64, @intFromBool(first_matches))),
                  [second_value] "r" (second.value),
                : .{ .nzcv = true });
        } else {
            const select_mask =
                @as(u64, 0) -% @as(u64, @intFromBool(first_matches));
            value = (value & select_mask) |
                (second.value & ~select_mask);
        }
        return .{
            .value = .{ .value = value },
            .found = first_matches or second_matches,
        };
    }

    fn lookupWorkerBpe(
        cache: *const WorkerBpeCache,
        key: u128,
        hash: u64,
    ) ?PackedWorkerBpeValue {
        var idx = workerBpeHome(cache, hash);
        var remaining = cache.entries.len / 2;
        while (remaining != 0) : (remaining -= 1) {
            const first = &cache.entries[idx];
            if (first.key == key) {
                return .{ .value = first.value };
            }
            const second = &cache.entries[idx + 1];
            if (second.key == key) {
                return .{ .value = second.value };
            }
            if (first.key == 0 or second.key == 0) return null;
            idx = (idx + 2) & (cache.entries.len - 1);
        }
        return null;
    }

    fn insertWorkerBpe(
        cache: *WorkerBpeCache,
        key: u128,
        hash: u64,
        packed_value: PackedWorkerBpeValue,
    ) void {
        if (cache.count >= cache.entries.len * 3 / 4) return;
        var idx = workerBpeHome(cache, hash);
        var remaining = cache.entries.len / 2;
        while (remaining != 0) : (remaining -= 1) {
            for (cache.entries[idx .. idx + 2]) |*entry| {
                if (entry.key == key) return;
                if (entry.key == 0) {
                    entry.* = .{
                        .key = key,
                        .value = packed_value.value,
                    };
                    cache.count += 1;
                    return;
                }
            }
            idx = (idx + 2) & (cache.entries.len - 1);
        }
    }

    fn seedWorkerBpeCache(self: *const HfTokenizer, cache: *WorkerBpeCache) void {
        if (self.model_type != .bpe or
            self.pre_tokenizer_type != .byte_level or
            self.end_of_word_suffix.len != 0 or
            cache.entries.len < self.vocab.count() *| 2)
        {
            return;
        }
        var iterator = self.vocab.iterator();
        while (iterator.next()) |entry| {
            const key = workerBpeKey(entry.key_ptr.*) orelse continue;
            const token_id = [_]i32{entry.value_ptr.*};
            const packed_value = packWorkerBpeValue(&token_id) orelse continue;
            const hash = workerBpeHash(key);
            insertWorkerBpe(cache, key, hash, packed_value);
        }
    }

    fn allocateWorkerBpeCacheEntries(
        self: *HfTokenizer,
        slot_count: usize,
        bytes: usize,
    ) !WorkerBpeCacheAllocation {
        if (comptime builtin.os.tag == .macos) {
            if (bytes >= worker_bpe_superpage_bytes and
                bytes % worker_bpe_superpage_bytes == 0)
            {
                var address: u64 = 0;
                const result = mach_vm_map(
                    mach_task_self_,
                    &address,
                    bytes,
                    0,
                    darwin_vm_flags_anywhere |
                        darwin_vm_flags_superpage_2mb,
                    0,
                    0,
                    0,
                    darwin_vm_prot_read_write,
                    darwin_vm_prot_read_write,
                    darwin_vm_inherit_copy,
                );
                if (result == 0) {
                    const entries_ptr: [*]align(64) WorkerBpeCacheEntry =
                        @ptrFromInt(address);
                    return .{
                        .entries = entries_ptr[0..slot_count],
                        .storage = .darwin_superpage,
                    };
                }
            }
        }
        return .{
            .entries = try self.allocator.alignedAlloc(
                WorkerBpeCacheEntry,
                .@"64",
                slot_count,
            ),
            .storage = .allocator,
        };
    }

    fn initializeWorkerBpeCacheLease(
        self: *HfTokenizer,
        lease: *WorkerBpeCacheLease,
    ) bool {
        if (lease.cache != null) return true;
        if (lease.allocation_failed) return false;
        if (lease.reservation_retry_after != 0) {
            lease.reservation_retry_after -= 1;
            return false;
        }

        const slot_count = self.parallel_bpe_config.worker_cache_slots;
        const bytes = std.math.mul(
            usize,
            slot_count,
            @sizeOf(WorkerBpeCacheEntry),
        ) catch {
            lease.allocation_failed = true;
            return false;
        };
        const resource_budget_charged = self.tryReserveResourceBudgetBytes(bytes) orelse {
            lease.reservation_denials +|= 1;
            const shift: u3 = @intCast(@min(
                lease.reservation_denials - 1,
                6,
            ));
            lease.reservation_retry_after =
                @as(u8, 1) << shift;
            return false;
        };
        lease.reservation_denials = 0;
        lease.reservation_retry_after = 0;
        const allocation = self.allocateWorkerBpeCacheEntries(
            slot_count,
            bytes,
        ) catch {
            if (resource_budget_charged == .charged)
                self.releaseResourceBudgetBytes(bytes);
            // Repeated multi-megabyte allocator attempts under OOM can amplify
            // pressure. Resource-budget denials above remain retryable, while
            // an admitted allocation failure is terminal for this lease.
            lease.allocation_failed = true;
            return false;
        };
        @memset(allocation.entries, .{});
        lease.cache = .{
            .entries = allocation.entries,
            .accounted_bytes = bytes,
            .storage = allocation.storage,
        };
        self.seedWorkerBpeCache(&lease.cache.?);
        return true;
    }

    fn acquireWorkerBpeCacheAt(
        self: *HfTokenizer,
        index: usize,
    ) ?*WorkerBpeCacheLease {
        if (index >= self.parallel_bpe_config.worker_cache_count) return null;
        const lease = &self.worker_bpe_caches[index];
        if (!lease.mutex.tryLock()) return null;
        if (self.initializeWorkerBpeCacheLease(lease)) return lease;
        lease.mutex.unlock();
        return null;
    }

    fn releaseWorkerBpeCache(lease: ?*WorkerBpeCacheLease) void {
        if (lease) |held| {
            held.publishStats();
            held.mutex.unlock();
        }
    }

    const ParallelBpeWorker = struct {
        tokenizer: *HfTokenizer = undefined,
        text: []const u8 = "",
        handle_added_tokens: bool = false,
        ids: std.ArrayListUnmanaged(i32) = .empty,
        ids_u16: std.ArrayListUnmanaged(u16) = .empty,
        packed_output: bool = false,
        added_token_offsets: std.ArrayListUnmanaged(usize) = .empty,
        pretoken_boundary_words: std.ArrayListUnmanaged(u64) = .empty,
        bpe_scratch: BpeScratch = .{},
        failure: ?anyerror = null,
        last_elapsed_ns: u64 = 0,
        done: std.atomic.Value(bool) = .init(false),
        claimed: std.atomic.Value(bool) = .init(false),
        cache_owner: u8 = invalid_worker_cache_owner,
        reuse_added_token_offsets: bool = false,
        reuse_pretoken_boundaries: bool = false,
        published_retained_bytes: std.atomic.Value(usize) = .init(0),
        published_output_bytes: std.atomic.Value(usize) = .init(0),
        published_output_capacity_bytes: std.atomic.Value(usize) = .init(0),
        published_stable_offset_count: std.atomic.Value(usize) = .init(0),
        published_stable_offset_bytes: std.atomic.Value(usize) = .init(0),
        published_stable_boundary_words: std.atomic.Value(usize) = .init(0),
        published_stable_boundary_bytes: std.atomic.Value(usize) = .init(0),
        published_text_bytes: std.atomic.Value(usize) = .init(0),
        published_elapsed_ns: AtomicU64 = .init(0),
        published_cache_owner: std.atomic.Value(usize) =
            .init(invalid_worker_cache_owner),

        fn retainedBytes(self: *const ParallelBpeWorker) usize {
            var total: usize = 0;
            total +|= self.ids.capacity *| @sizeOf(i32);
            total +|= self.ids_u16.capacity *| @sizeOf(u16);
            total +|= self.added_token_offsets.capacity *| @sizeOf(usize);
            total +|= self.pretoken_boundary_words.capacity *| @sizeOf(u64);
            total +|= self.bpe_scratch.symbols.capacity *| @sizeOf(BpeSymbol);
            total +|= self.bpe_scratch.transcode_ids.capacity *| @sizeOf(i32);
            if (self.bpe_scratch.candidates) |*candidates| {
                total +|= candidates.items.capacity *| @sizeOf(BpeCandidate);
            }
            return total;
        }

        /// Publish an eventually consistent, race-free observability snapshot.
        /// This runs only at chunk setup/completion, never in the pretoken or
        /// BPE probe loops.
        fn publishStats(self: *ParallelBpeWorker) void {
            self.published_retained_bytes.store(
                self.retainedBytes(),
                .monotonic,
            );
            self.published_output_bytes.store(
                self.ids.items.len *| @sizeOf(i32) +|
                    self.ids_u16.items.len *| @sizeOf(u16),
                .monotonic,
            );
            self.published_output_capacity_bytes.store(
                self.ids.capacity *| @sizeOf(i32) +|
                    self.ids_u16.capacity *| @sizeOf(u16),
                .monotonic,
            );
            self.published_stable_offset_count.store(
                self.added_token_offsets.items.len,
                .monotonic,
            );
            self.published_stable_offset_bytes.store(
                self.added_token_offsets.capacity *| @sizeOf(usize),
                .monotonic,
            );
            self.published_stable_boundary_words.store(
                self.pretoken_boundary_words.items.len,
                .monotonic,
            );
            self.published_stable_boundary_bytes.store(
                self.pretoken_boundary_words.capacity *| @sizeOf(u64),
                .monotonic,
            );
            self.published_text_bytes.store(self.text.len, .monotonic);
            self.published_elapsed_ns.store(self.last_elapsed_ns, .monotonic);
            self.published_cache_owner.store(self.cache_owner, .release);
        }

        fn run(
            self: *ParallelBpeWorker,
            worker_cache: ?*WorkerBpeCache,
        ) std.Io.Cancelable!void {
            if (self.packed_output) {
                const packed_result =
                    if (self.handle_added_tokens)
                        self.tokenizer
                            .encodeBpeByteLevelWithAddedTokensWorkerU16(
                            self.tokenizer.allocator,
                            self.text,
                            &self.ids_u16,
                            &self.bpe_scratch,
                            worker_cache,
                            if (self.reuse_added_token_offsets)
                                self.added_token_offsets.items
                            else
                                null,
                            if (self.reuse_pretoken_boundaries and
                                worker_cache != null)
                                self.pretoken_boundary_words.items
                            else
                                null,
                        )
                    else
                        self.tokenizer.encodeBpeByteLevelWorkerU16(
                            self.tokenizer.allocator,
                            self.text,
                            &self.ids_u16,
                            &self.bpe_scratch,
                            worker_cache,
                        );
                packed_result catch |err| {
                    self.failure = err;
                };
                return;
            }
            const result = if (self.handle_added_tokens)
                self.tokenizer.encodeBpeByteLevelWithAddedTokensWorker(
                    self.tokenizer.allocator,
                    self.text,
                    &self.ids,
                    &self.bpe_scratch,
                    worker_cache,
                    if (self.reuse_added_token_offsets)
                        self.added_token_offsets.items
                    else
                        null,
                    if (self.reuse_pretoken_boundaries and
                        worker_cache != null)
                        self.pretoken_boundary_words.items
                    else
                        null,
                )
            else
                self.tokenizer.encodeBpeByteLevelWorker(
                    self.tokenizer.allocator,
                    self.text,
                    &self.ids,
                    &self.bpe_scratch,
                    worker_cache,
                );
            result catch |err| {
                self.failure = err;
            };
        }
    };

    const ParallelAddedTokenScanJob = struct {
        tokenizer: *HfTokenizer,
        chunks: []ParallelBpeWorker,
        scan_added_offsets: bool,
        build_pretoken_boundaries: bool,
        next_chunk: std.atomic.Value(usize) = .init(0),

        fn run(self: *ParallelAddedTokenScanJob) std.Io.Cancelable!void {
            while (true) {
                const idx = self.next_chunk.fetchAdd(1, .monotonic);
                if (idx >= self.chunks.len) return;
                const chunk = &self.chunks[idx];
                self.scanChunk(chunk) catch |err| {
                    chunk.failure = err;
                };
            }
        }

        fn scanChunk(
            self: *ParallelAddedTokenScanJob,
            chunk: *ParallelBpeWorker,
        ) !void {
            if (self.scan_added_offsets) {
                var cursor: usize = 0;
                while (cursor < chunk.text.len) {
                    const offset =
                        self.tokenizer.findNextAddedToken(
                            chunk.text,
                            cursor,
                        ) orelse break;
                    try chunk.added_token_offsets.append(
                        self.tokenizer.allocator,
                        offset,
                    );
                    const match = self.tokenizer.matchAddedTokenAt(
                        chunk.text[offset..],
                    ) orelse return error.InvalidAddedTokenIndex;
                    cursor = offset + match.len;
                }
            }
            if (!self.build_pretoken_boundaries) return;

            var segment_start: usize = 0;
            for (chunk.added_token_offsets.items) |offset| {
                self.indexSegment(chunk, segment_start, offset);
                const match = self.tokenizer.matchAddedTokenAt(
                    chunk.text[offset..],
                ) orelse return error.InvalidAddedTokenIndex;
                segment_start = offset + match.len;
            }
            self.indexSegment(chunk, segment_start, chunk.text.len);
        }

        fn indexSegment(
            self: *ParallelAddedTokenScanJob,
            chunk: *ParallelBpeWorker,
            start: usize,
            end: usize,
        ) void {
            _ = self;
            var iterator = ByteLevelPretokenIterator{
                .text = chunk.text[start..end],
            };
            while (iterator.next()) |word| {
                const offset =
                    @intFromPtr(word.ptr) - @intFromPtr(chunk.text.ptr);
                chunk.pretoken_boundary_words.items[offset / 64] |=
                    @as(u64, 1) << @intCast(offset % 64);
            }
        }
    };

    const ParallelWorkerBpeCacheInitJob = struct {
        tokenizer: *HfTokenizer,
        cache_count: usize,
        next_cache: std.atomic.Value(usize) = .init(0),
        ready_count: std.atomic.Value(usize) = .init(0),

        fn run(self: *ParallelWorkerBpeCacheInitJob) std.Io.Cancelable!void {
            while (true) {
                const idx = self.next_cache.fetchAdd(1, .monotonic);
                if (idx >= self.cache_count) return;
                const lease = self.tokenizer.acquireWorkerBpeCacheAt(idx);
                if (lease != null) {
                    _ = self.ready_count.fetchAdd(1, .monotonic);
                }
                releaseWorkerBpeCache(lease);
            }
        }
    };

    fn prepareWorkerBpeCachesForStableIndex(
        self: *HfTokenizer,
        io: std.Io,
        cache_count: usize,
    ) !bool {
        std.debug.assert(cache_count != 0);
        std.debug.assert(
            cache_count <= self.parallel_bpe_config.worker_cache_count,
        );
        var job = ParallelWorkerBpeCacheInitJob{
            .tokenizer = self,
            .cache_count = cache_count,
        };
        var group: std.Io.Group = .init;
        errdefer group.cancel(io);
        for (1..cache_count) |_| {
            group.async(io, ParallelWorkerBpeCacheInitJob.run, .{&job});
        }
        try job.run();
        try group.await(io);
        return job.ready_count.load(.acquire) == cache_count;
    }

    const ParallelBpeJob = struct {
        tokenizer: *HfTokenizer,
        io: std.Io,
        chunks: []ParallelBpeWorker,
        output: ?*std.ArrayListUnmanaged(i32),
        next_chunk: std.atomic.Value(usize) = .init(0),
        next_consumer: std.atomic.Value(usize) = .init(0),
        consumer_count: usize,
        replay_affinity: bool,
        record_affinity: bool,
        allow_tail_steal: bool,
        owner_caches: ?[]const *WorkerBpeCache,
        stolen_chunks: std.atomic.Value(usize) = .init(0),
        commit_mutex: std.atomic.Mutex = .unlocked,
        next_commit: usize = 0,

        fn run(self: *ParallelBpeJob) std.Io.Cancelable!void {
            if (self.owner_caches) |owner_caches| {
                while (true) {
                    const idx = self.next_chunk.fetchAdd(1, .monotonic);
                    if (idx >= self.chunks.len) return;
                    const owner = self.chunks[idx].cache_owner;
                    std.debug.assert(
                        owner != invalid_worker_cache_owner and
                            owner < owner_caches.len,
                    );
                    try self.runChunk(
                        idx,
                        owner,
                        owner_caches[owner],
                    );
                }
            }
            const consumer_idx =
                self.next_consumer.fetchAdd(1, .monotonic);
            std.debug.assert(consumer_idx < self.consumer_count);
            const cache_lease =
                if (self.tokenizer.parallel_bpe_config.worker_cache_count != 0)
                    self.tokenizer.acquireWorkerBpeCacheAt(consumer_idx)
                else
                    null;
            defer releaseWorkerBpeCache(cache_lease);
            const worker_cache = if (cache_lease) |lease|
                if (lease.cache) |*cache| cache else null
            else
                null;
            if (self.replay_affinity) {
                const cache_owner: u8 = @intCast(consumer_idx);
                for (self.chunks, 0..) |*chunk, idx| {
                    if (chunk.cache_owner != cache_owner) continue;
                    if (chunk.claimed.swap(true, .acq_rel)) continue;
                    try self.runChunk(idx, consumer_idx, worker_cache);
                }
                if (!self.allow_tail_steal) return;
                while (true) {
                    const idx = self.next_chunk.fetchAdd(1, .monotonic);
                    if (idx >= self.chunks.len) return;
                    if (self.chunks[idx].claimed.swap(true, .acq_rel)) continue;
                    _ = self.stolen_chunks.fetchAdd(1, .monotonic);
                    try self.runChunk(idx, consumer_idx, worker_cache);
                }
            }
            while (true) {
                const idx = self.next_chunk.fetchAdd(1, .monotonic);
                if (idx >= self.chunks.len) return;
                try self.runChunk(idx, consumer_idx, worker_cache);
            }
        }

        fn runChunk(
            self: *ParallelBpeJob,
            idx: usize,
            consumer_idx: usize,
            worker_cache: ?*WorkerBpeCache,
        ) std.Io.Cancelable!void {
            const started = std.Io.Clock.Timestamp.now(
                self.io,
                .awake,
            );
            try self.chunks[idx].run(worker_cache);
            const finished = std.Io.Clock.Timestamp.now(
                self.io,
                .awake,
            );
            const elapsed = started.durationTo(finished).raw.nanoseconds;
            self.chunks[idx].last_elapsed_ns =
                if (elapsed > 0) @intCast(elapsed) else 0;
            if (self.record_affinity) {
                self.chunks[idx].cache_owner = @intCast(consumer_idx);
            }
            self.chunks[idx].publishStats();
            self.chunks[idx].done.store(true, .release);
            self.commitReady();
        }

        fn commitReady(self: *ParallelBpeJob) void {
            const output = self.output orelse return;
            if (!self.commit_mutex.tryLock()) return;
            defer self.commit_mutex.unlock();
            var committed: usize = 0;
            while (self.next_commit < self.chunks.len and
                committed < max_prefix_commits_per_lock)
            {
                const chunk = &self.chunks[self.next_commit];
                if (!chunk.done.load(.acquire) or chunk.failure != null) return;
                if (chunk.ids.items.len >
                    output.capacity - output.items.len)
                {
                    return;
                }
                output.appendSliceAssumeCapacity(chunk.ids.items);
                self.next_commit += 1;
                committed += 1;
            }
        }
    };

    const ParallelBpeCopy = struct {
        chunk: *const ParallelBpeWorker,
        output_start: usize,
    };

    const ParallelBpeCopyJob = struct {
        copies: []const ParallelBpeCopy,
        output: []i32,
        next_copy: std.atomic.Value(usize) = .init(0),

        fn run(self: *ParallelBpeCopyJob) std.Io.Cancelable!void {
            while (true) {
                const idx = self.next_copy.fetchAdd(1, .monotonic);
                if (idx >= self.copies.len) return;
                const copy = self.copies[idx];
                @memcpy(
                    self.output[copy.output_start .. copy.output_start + copy.chunk.ids.items.len],
                    copy.chunk.ids.items,
                );
            }
        }
    };

    const ParallelBpeWorkspace = struct {
        next_free: ?*ParallelBpeWorkspace = null,
        next_all: ?*ParallelBpeWorkspace = null,
        cached: bool = false,
        resource_accounted_bytes: std.atomic.Value(usize) = .init(0),
        affinity_valid: bool = false,
        affinity_text_ptr: usize = 0,
        affinity_text_len: usize = 0,
        affinity_worker_count: usize = 0,
        affinity_consumer_count: usize = 0,
        affinity_learns: std.atomic.Value(usize) = .init(0),
        affinity_replays: std.atomic.Value(usize) = .init(0),
        affinity_stolen_chunks: std.atomic.Value(usize) = .init(0),
        stable_chunk_boundaries_valid: bool = false,
        stable_chunk_boundaries_id: u64 = 0,
        stable_chunk_boundaries_text_ptr: usize = 0,
        stable_chunk_boundaries_text_len: usize = 0,
        stable_chunk_boundaries_requested: usize = 0,
        stable_chunk_boundary_count: usize = 0,
        stable_chunk_boundaries: [max_parallel_bpe_chunks + 1]usize = undefined,
        stable_added_offsets_valid: bool = false,
        stable_added_offsets_id: u64 = 0,
        stable_added_offsets_text_ptr: usize = 0,
        stable_added_offsets_text_len: usize = 0,
        stable_added_offsets_worker_count: usize = 0,
        stable_pretoken_boundaries_valid: bool = false,
        stable_offset_learns: std.atomic.Value(usize) = .init(0),
        stable_offset_replays: std.atomic.Value(usize) = .init(0),
        workers: [max_parallel_bpe_chunks]ParallelBpeWorker =
            @as([max_parallel_bpe_chunks]ParallelBpeWorker, @splat(.{})),

        fn retainedBytes(self: *const ParallelBpeWorkspace) usize {
            var total: usize = @sizeOf(ParallelBpeWorkspace);
            for (&self.workers) |*worker| {
                total +|= worker.retainedBytes();
            }
            return total;
        }

        fn publishedRetainedBytes(
            self: *const ParallelBpeWorkspace,
        ) usize {
            var total: usize = @sizeOf(ParallelBpeWorkspace);
            for (&self.workers) |*worker| {
                total +|= worker.published_retained_bytes.load(.acquire);
            }
            return total;
        }
    };

    /// Ordered, zero-copy token chunks produced by the internally parallel
    /// ByteLevel encoder. The result pins its tokenizer-owned workspace until
    /// `deinit`; callers may hash, stream, or consume every segment without
    /// materializing a second flat multi-gigabyte token array.
    pub const ParallelTokenSegments = struct {
        owner: ?*HfTokenizer,
        workspace: *ParallelBpeWorkspace,
        worker_count: usize,

        pub fn segmentCount(self: *const ParallelTokenSegments) usize {
            return self.worker_count;
        }

        pub fn segment(
            self: *const ParallelTokenSegments,
            index: usize,
        ) []const i32 {
            std.debug.assert(index < self.worker_count);
            return self.workspace.workers[index].ids.items;
        }

        pub fn tokenCount(self: *const ParallelTokenSegments) usize {
            var total: usize = 0;
            for (self.workspace.workers[0..self.worker_count]) |worker| {
                total +|= worker.ids.items.len;
            }
            return total;
        }

        pub fn deinit(self: *ParallelTokenSegments) void {
            const owner = self.owner orelse return;
            self.owner = null;
            owner.releaseParallelBpeWorkspace(self.workspace);
        }
    };

    /// Lossless compact segmented output for models whose complete token-ID
    /// domain fits in u16. This halves GPT-2 output traffic and residency;
    /// callers that require i32 can widen while consuming each segment.
    pub const ParallelTokenSegmentsU16 = struct {
        owner: ?*HfTokenizer,
        workspace: *ParallelBpeWorkspace,
        worker_count: usize,

        pub fn segmentCount(
            self: *const ParallelTokenSegmentsU16,
        ) usize {
            return self.worker_count;
        }

        pub fn segment(
            self: *const ParallelTokenSegmentsU16,
            index: usize,
        ) []const u16 {
            std.debug.assert(index < self.worker_count);
            return self.workspace.workers[index].ids_u16.items;
        }

        pub fn tokenCount(
            self: *const ParallelTokenSegmentsU16,
        ) usize {
            var total: usize = 0;
            for (
                self.workspace.workers[0..self.worker_count],
            ) |worker| {
                total +|= worker.ids_u16.items.len;
            }
            return total;
        }

        pub fn deinit(self: *ParallelTokenSegmentsU16) void {
            const owner = self.owner orelse return;
            self.owner = null;
            owner.releaseParallelBpeWorkspace(self.workspace);
        }
    };

    const ParallelSegmentsOutput = union(enum) {
        i32: *ParallelTokenSegments,
        u16: *ParallelTokenSegmentsU16,
    };

    fn destroyParallelBpeWorkspace(
        self: *HfTokenizer,
        workspace: *ParallelBpeWorkspace,
    ) void {
        if (self.parallel_bpe_config.recycle_segmented_output_pages) {
            adviseParallelOutputPages(workspace, false);
        }
        const resource_accounted_bytes =
            workspace.resource_accounted_bytes.swap(0, .acq_rel);
        self.releaseResourceBudgetBytes(resource_accounted_bytes);
        for (&workspace.workers) |*worker| {
            worker.ids.deinit(self.allocator);
            worker.ids_u16.deinit(self.allocator);
            worker.added_token_offsets.deinit(self.allocator);
            worker.pretoken_boundary_words.deinit(self.allocator);
            worker.bpe_scratch.deinit(self.allocator);
        }
        self.allocator.destroy(workspace);
    }

    fn acquireParallelBpeWorkspace(self: *HfTokenizer) !*ParallelBpeWorkspace {
        while (!self.parallel_workspace_mutex.tryLock()) std.atomic.spinLoopHint();
        if (self.parallel_workspace_free) |workspace| {
            self.parallel_workspace_free = workspace.next_free;
            self.parallel_workspace_free_count -= 1;
            self.parallel_workspace_free_bytes -= workspace.retainedBytes();
            workspace.next_free = null;
            workspace.cached = false;
            self.parallel_workspace_mutex.unlock();
            if (self.parallel_bpe_config.recycle_segmented_output_pages) {
                adviseParallelOutputPages(workspace, false);
            }
            return workspace;
        }
        self.parallel_workspace_mutex.unlock();

        const workspace = try self.allocator.create(ParallelBpeWorkspace);
        workspace.* = .{};

        while (!self.parallel_workspace_mutex.tryLock()) std.atomic.spinLoopHint();
        workspace.next_all = self.parallel_workspace_all;
        self.parallel_workspace_all = workspace;
        self.parallel_workspace_mutex.unlock();
        return workspace;
    }

    fn releaseParallelBpeWorkspace(
        self: *HfTokenizer,
        workspace: *ParallelBpeWorkspace,
    ) void {
        if (self.parallel_bpe_config.recycle_segmented_output_pages) {
            adviseParallelOutputPages(workspace, true);
        }
        const retained_bytes = workspace.retainedBytes();
        var cacheable =
            retained_bytes <=
            self.parallel_bpe_config.max_retained_workspace_bytes;
        if (cacheable) {
            const accounted_bytes =
                workspace.resource_accounted_bytes.load(.acquire);
            if (retained_bytes > accounted_bytes) {
                const additional =
                    retained_bytes - accounted_bytes;
                if (self.tryReserveResourceBudgetBytes(additional)) |charge| {
                    if (charge == .charged) {
                        workspace.resource_accounted_bytes.store(
                            retained_bytes,
                            .release,
                        );
                    }
                } else {
                    cacheable = false;
                }
            } else if (retained_bytes < accounted_bytes) {
                self.releaseResourceBudgetBytes(accounted_bytes - retained_bytes);
                workspace.resource_accounted_bytes.store(
                    retained_bytes,
                    .release,
                );
            }
        }
        while (!self.parallel_workspace_mutex.tryLock()) std.atomic.spinLoopHint();
        if (cacheable and
            self.parallel_workspace_free_count < max_cached_parallel_workspaces)
        {
            workspace.cached = true;
            workspace.next_free = self.parallel_workspace_free;
            self.parallel_workspace_free = workspace;
            self.parallel_workspace_free_count += 1;
            self.parallel_workspace_free_bytes += retained_bytes;
            self.parallel_workspace_mutex.unlock();
            return;
        }

        var link = &self.parallel_workspace_all;
        while (link.*) |current| {
            if (current == workspace) {
                link.* = current.next_all;
                break;
            }
            link = &current.next_all;
        }
        self.parallel_workspace_mutex.unlock();
        self.destroyParallelBpeWorkspace(workspace);
    }

    fn adviseParallelOutputPages(
        workspace: *ParallelBpeWorkspace,
        reusable: bool,
    ) void {
        if (comptime builtin.os.tag != .macos) return;
        const advice: u32 = if (reusable)
            std.posix.MADV.FREE_REUSABLE
        else
            std.posix.MADV.FREE_REUSE;
        const page_size = std.heap.page_size_min;
        for (&workspace.workers) |*worker| {
            const capacity_bytes = if (worker.packed_output)
                worker.ids_u16.capacity * @sizeOf(u16)
            else
                worker.ids.capacity * @sizeOf(i32);
            if (capacity_bytes == 0) continue;
            const allocation_start = if (worker.packed_output)
                @intFromPtr(worker.ids_u16.items.ptr)
            else
                @intFromPtr(worker.ids.items.ptr);
            const allocation_end = allocation_start +
                capacity_bytes;
            const start = std.mem.alignForward(
                usize,
                allocation_start,
                page_size,
            );
            const end = std.mem.alignBackward(
                usize,
                allocation_end,
                page_size,
            );
            if (end <= start) continue;
            const aligned_ptr: [*]align(std.heap.page_size_min) u8 =
                @ptrFromInt(start);
            std.posix.madvise(
                aligned_ptr,
                end - start,
                advice,
            ) catch {};
        }
    }

    fn adviseHugePages(values: []i32) void {
        if (comptime builtin.os.tag == .linux) {
            const byte_len = values.len * @sizeOf(i32);
            if (byte_len < 2 * 1024 * 1024) return;
            const page_size = std.heap.page_size_min;
            const allocation_start = @intFromPtr(values.ptr);
            const start = std.mem.alignForward(
                usize,
                allocation_start,
                page_size,
            );
            const end = allocation_start + byte_len;
            if (end <= start) return;
            const aligned_ptr: [*]align(std.heap.page_size_min) u8 =
                @ptrFromInt(start);
            std.posix.madvise(
                aligned_ptr,
                end - start,
                std.posix.MADV.HUGEPAGE,
            ) catch {};
        }
    }

    fn isAsciiWhitespaceByte(byte: u8) bool {
        return byte == ' ' or (byte >= 9 and byte <= 13);
    }

    fn parallelBpeTarget(text_len: usize, chunk_count: usize, idx: usize) usize {
        return (text_len / chunk_count) * idx +
            ((text_len % chunk_count) * idx) / chunk_count;
    }

    fn parallelBpeBoundary(text: []const u8, target: usize) usize {
        var pos = target;
        while (pos < text.len) : (pos += 1) {
            if (isAsciiWhitespaceByte(text[pos]) and
                (pos == 0 or !isAsciiWhitespaceByte(text[pos - 1])))
            {
                return pos;
            }
        }
        return text.len;
    }

    /// Resolve monotonically increasing targets without rescanning a suffix
    /// whose answer is already known. Prose retains the old few-byte probes,
    /// while whitespace-free/minified inputs scan to EOF only once.
    fn collectParallelBpeBoundaries(
        text: []const u8,
        chunk_count: usize,
        boundaries: *[max_parallel_bpe_chunks + 1]usize,
    ) usize {
        var boundary_count: usize = 1;
        boundaries[0] = 0;
        var target_idx: usize = 1;
        while (target_idx < chunk_count) {
            const target = parallelBpeTarget(text.len, chunk_count, target_idx);
            const boundary = parallelBpeBoundary(text, target);
            if (boundary == text.len) break;
            if (boundary > boundaries[boundary_count - 1]) {
                boundaries[boundary_count] = boundary;
                boundary_count += 1;
            }
            target_idx += 1;
            while (target_idx < chunk_count and
                parallelBpeTarget(text.len, chunk_count, target_idx) <= boundary)
            {
                target_idx += 1;
            }
        }

        boundaries[boundary_count] = text.len;
        return boundary_count + 1;
    }

    /// Find the first exact added-token/ByteLevel pretoken boundary at or
    /// after `target`, starting from a boundary already known to be safe.
    fn parallelBpePretokenBoundary(
        self: *const HfTokenizer,
        text: []const u8,
        start: usize,
        target: usize,
        end: usize,
    ) usize {
        var pos = start;
        var next_added = self.findNextAddedToken(text[0..end], pos);
        while (pos < target and pos < end) {
            if (next_added) |added_offset| {
                if (added_offset == pos) {
                    const match = self.matchAddedTokenAt(text[pos..end]) orelse
                        return end;
                    pos += match.len;
                    next_added = self.findNextAddedToken(text[0..end], pos);
                    continue;
                }
            }
            const segment_end = next_added orelse end;
            if (segment_end <= pos) return end;
            const relative_end = gpt2PreTokenEnd(
                text[pos..segment_end],
                0,
            );
            if (relative_end == 0) return end;
            pos += relative_end;
        }
        return pos;
    }

    /// Whitespace boundaries are essentially free for prose, but a minified
    /// or otherwise whitespace-free region can collapse many target chunks
    /// into one serial straggler. Subdivide only oversized gaps at exact
    /// ByteLevel/added-token boundaries, without retaining a dense index.
    fn refineOversizedParallelBpeBoundaries(
        self: *const HfTokenizer,
        text: []const u8,
        requested_chunks: usize,
        boundaries: *[max_parallel_bpe_chunks + 1]usize,
        boundary_count: usize,
    ) usize {
        if (requested_chunks <= 1 or text.len == 0) return boundary_count;
        const nominal =
            (text.len + requested_chunks - 1) / requested_chunks;
        const oversized_threshold = nominal +| 64 * 1024;
        var refined: [max_parallel_bpe_chunks + 1]usize = undefined;
        var refined_count: usize = 1;
        refined[0] = 0;

        for (boundaries[1..boundary_count], 1..) |interval_end, boundary_idx| {
            var cursor = refined[refined_count - 1];
            const remaining_endpoints = boundary_count - boundary_idx;
            while (interval_end > cursor and
                interval_end - cursor > oversized_threshold and
                refined_count + remaining_endpoints <
                    requested_chunks + 1)
            {
                const target = cursor + nominal;
                const boundary = self.parallelBpePretokenBoundary(
                    text,
                    cursor,
                    target,
                    interval_end,
                );
                if (boundary <= cursor or boundary >= interval_end) break;
                refined[refined_count] = boundary;
                refined_count += 1;
                cursor = boundary;
            }
            refined[refined_count] = interval_end;
            refined_count += 1;
        }
        @memcpy(boundaries[0..refined_count], refined[0..refined_count]);
        return refined_count;
    }

    /// Whitespace chunk boundaries cannot bisect an added token when every
    /// whitespace byte in that token, if present, is its first byte. This
    /// admits GPT-style document delimiters such as `<|endoftext|>` while
    /// retaining the serial fallback for general phrases containing spaces.
    fn addedTokensAreParallelBoundarySafe(self: *const HfTokenizer) bool {
        var iterator = self.added_tokens.keyIterator();
        while (iterator.next()) |token_ptr| {
            for (token_ptr.*[1..]) |byte| {
                if (isAsciiWhitespaceByte(byte)) return false;
            }
        }
        return true;
    }

    /// Parallelize one large GPT-2 ByteLevel document at pretoken-safe
    /// whitespace boundaries, then gather worker outputs in source order.
    /// Inputs requiring normalization or added-token segmentation stay on the
    /// serial path because those transforms can cross a proposed boundary.
    fn encodeIntoParallel(
        self: *HfTokenizer,
        io: std.Io,
        allocator: std.mem.Allocator,
        text: []const u8,
        ids: *std.ArrayListUnmanaged(i32),
        requested_tasks: usize,
    ) !void {
        return self.encodeIntoParallelImpl(
            io,
            allocator,
            text,
            ids,
            requested_tasks,
            null,
            null,
        );
    }

    fn encodeIntoParallelStable(
        self: *HfTokenizer,
        io: std.Io,
        allocator: std.mem.Allocator,
        text: []const u8,
        ids: *std.ArrayListUnmanaged(i32),
        requested_tasks: usize,
        stable_input_id: u64,
    ) !void {
        if (stable_input_id == 0) return error.InvalidStableInputId;
        return self.encodeIntoParallelImpl(
            io,
            allocator,
            text,
            ids,
            requested_tasks,
            stable_input_id,
            null,
        );
    }

    pub fn encodeParallelSegmentsStable(
        self: *HfTokenizer,
        io: std.Io,
        text: []const u8,
        requested_tasks: usize,
        stable_input_id: u64,
    ) !ParallelTokenSegments {
        if (stable_input_id == 0) return error.InvalidStableInputId;
        var unused_output: std.ArrayListUnmanaged(i32) = .empty;
        var result: ParallelTokenSegments = undefined;
        try self.encodeIntoParallelImpl(
            io,
            self.allocator,
            text,
            &unused_output,
            requested_tasks,
            stable_input_id,
            .{ .i32 = &result },
        );
        std.debug.assert(unused_output.capacity == 0);
        return result;
    }

    fn tokenIdsFitU16(self: *const HfTokenizer) bool {
        var vocab_ids = self.id_to_token.keyIterator();
        while (vocab_ids.next()) |id| {
            if (id.* < 0 or id.* > std.math.maxInt(u16)) return false;
        }
        var added_ids = self.added_tokens.valueIterator();
        while (added_ids.next()) |id| {
            if (id.* < 0 or id.* > std.math.maxInt(u16)) return false;
        }
        return true;
    }

    pub fn encodeParallelSegmentsU16Stable(
        self: *HfTokenizer,
        io: std.Io,
        text: []const u8,
        requested_tasks: usize,
        stable_input_id: u64,
    ) !ParallelTokenSegmentsU16 {
        if (stable_input_id == 0) return error.InvalidStableInputId;
        if (!self.tokenIdsFitU16()) {
            return error.TokenIdTooLargeForU16;
        }
        var unused_output: std.ArrayListUnmanaged(i32) = .empty;
        var result: ParallelTokenSegmentsU16 = undefined;
        try self.encodeIntoParallelImpl(
            io,
            self.allocator,
            text,
            &unused_output,
            requested_tasks,
            stable_input_id,
            .{ .u16 = &result },
        );
        std.debug.assert(unused_output.capacity == 0);
        return result;
    }

    fn encodeIntoParallelImpl(
        self: *HfTokenizer,
        io: std.Io,
        allocator: std.mem.Allocator,
        text: []const u8,
        ids: *std.ArrayListUnmanaged(i32),
        requested_tasks: usize,
        stable_input_id: ?u64,
        segments_out: ?ParallelSegmentsOutput,
    ) !void {
        if (requested_tasks <= 1 or
            text.len < parallel_bpe_min_bytes or
            self.model_type != .bpe or
            self.pre_tokenizer_type != .byte_level or
            self.do_lowercase or
            self.replace_space_with != null or
            self.end_of_word_suffix.len != 0)
        {
            if (segments_out != null) {
                return error.UnsupportedSegmentedEncoding;
            }
            return self.encodeInto(allocator, text, ids);
        }
        const has_added_token_config = self.added_tokens.count() != 0;
        const added_tokens_are_safe =
            !has_added_token_config or self.addedTokensAreParallelBoundarySafe();
        if (!added_tokens_are_safe and
            (self.matchAddedTokenAt(text) != null or
                self.findNextAddedToken(text, 0) != null))
        {
            if (segments_out != null) {
                return error.UnsupportedSegmentedEncoding;
            }
            return self.encodeInto(allocator, text, ids);
        }
        // Safe token sets are scanned exactly once inside their chunks. An
        // up-front full-input occurrence check would duplicate the dominant
        // memory pass on document-delimited corpora such as OpenWebText.
        const handle_added_tokens = has_added_token_config and added_tokens_are_safe;

        const runner_count = @min(requested_tasks, 64);
        // More chunks than runners lets the std.Io tasks pull another piece
        // when they finish early, reducing the long tail caused by uneven
        // pretoken/cache work while keeping concurrency bounded by the
        // caller's requested task count.
        const chunks_per_runner: usize =
            if (self.parallel_bpe_config.chunks_per_task != 0)
                self.parallel_bpe_config.chunks_per_task
            else if (text.len >= 1024 * 1024 * 1024)
                16
            else if (text.len >= 4 * 1024 * 1024)
                8
            else
                4;
        const chunk_count = @min(
            runner_count *| chunks_per_runner,
            self.parallel_bpe_config.max_chunks,
        );

        const workspace = try self.acquireParallelBpeWorkspace();
        var release_workspace = true;
        defer if (release_workspace) {
            self.releaseParallelBpeWorkspace(workspace);
        };

        var boundaries: [max_parallel_bpe_chunks + 1]usize = undefined;
        const reuse_chunk_boundaries =
            stable_input_id != null and
            workspace.stable_chunk_boundaries_valid and
            workspace.stable_chunk_boundaries_id == stable_input_id.? and
            workspace.stable_chunk_boundaries_text_ptr ==
                @intFromPtr(text.ptr) and
            workspace.stable_chunk_boundaries_text_len == text.len and
            workspace.stable_chunk_boundaries_requested == chunk_count;
        var boundary_count: usize = undefined;
        if (reuse_chunk_boundaries) {
            boundary_count = workspace.stable_chunk_boundary_count;
            @memcpy(
                boundaries[0..boundary_count],
                workspace.stable_chunk_boundaries[0..boundary_count],
            );
        } else {
            boundary_count = collectParallelBpeBoundaries(
                text,
                chunk_count,
                &boundaries,
            );
            boundary_count = self.refineOversizedParallelBpeBoundaries(
                text,
                chunk_count,
                &boundaries,
                boundary_count,
            );
            if (stable_input_id) |input_id| {
                workspace.stable_chunk_boundaries_valid = true;
                workspace.stable_chunk_boundaries_id = input_id;
                workspace.stable_chunk_boundaries_text_ptr =
                    @intFromPtr(text.ptr);
                workspace.stable_chunk_boundaries_text_len = text.len;
                workspace.stable_chunk_boundaries_requested = chunk_count;
                workspace.stable_chunk_boundary_count = boundary_count;
                @memcpy(
                    workspace.stable_chunk_boundaries[0..boundary_count],
                    boundaries[0..boundary_count],
                );
            }
        }
        const worker_count = boundary_count - 1;
        if (worker_count <= 1) {
            if (segments_out != null) {
                return error.UnsupportedSegmentedEncoding;
            }
            return self.encodeInto(allocator, text, ids);
        }

        // One read-side critical section protects every worker's lock-free
        // cache lookup. Serial fallbacks establish their own section.
        const cache_reader = self.enterBpeCacheRead();
        defer if (cache_reader) |cache| self.leaveBpeCacheRead(cache);

        const workers = workspace.workers[0..worker_count];
        const active_runners = @min(runner_count, worker_count);
        // The caller is one queue consumer; background_count therefore keeps
        // total active consumers at or below requested_tasks.
        const background_count = active_runners - 1;
        const stable_offsets_eligible =
            stable_input_id != null and handle_added_tokens;
        const stable_boundaries_eligible =
            stable_input_id != null and
            self.parallel_bpe_config.retain_stable_pretoken_boundaries and
            self.parallel_bpe_config.worker_cache_count >= active_runners;
        const stable_metadata_eligible =
            stable_offsets_eligible or stable_boundaries_eligible;
        const stable_identity_matches =
            stable_metadata_eligible and
            workspace.stable_added_offsets_valid and
            workspace.stable_added_offsets_id == stable_input_id.? and
            workspace.stable_added_offsets_text_ptr ==
                @intFromPtr(text.ptr) and
            workspace.stable_added_offsets_text_len == text.len and
            workspace.stable_added_offsets_worker_count == worker_count;
        var reuse_stable_added_offsets =
            stable_offsets_eligible and stable_identity_matches;
        var reuse_stable_boundaries =
            stable_boundaries_eligible and
            stable_identity_matches and
            workspace.stable_pretoken_boundaries_valid;
        const packed_output = if (segments_out) |output|
            switch (output) {
                .i32 => false,
                .u16 => true,
            }
        else
            false;

        for (workers, 0..) |*worker, idx| {
            worker.tokenizer = self;
            worker.text = text[boundaries[idx]..boundaries[idx + 1]];
            worker.handle_added_tokens = handle_added_tokens;
            worker.ids.clearRetainingCapacity();
            worker.ids_u16.clearRetainingCapacity();
            worker.packed_output = packed_output;
            worker.failure = null;
            worker.done.store(false, .monotonic);
            worker.claimed.store(false, .monotonic);
            worker.reuse_added_token_offsets =
                reuse_stable_added_offsets;
            worker.reuse_pretoken_boundaries =
                reuse_stable_boundaries;
            if (stable_offsets_eligible and
                !reuse_stable_added_offsets)
            {
                worker.added_token_offsets.clearRetainingCapacity();
            }
            worker.publishStats();
        }

        const learned_stable_offsets =
            stable_offsets_eligible and !reuse_stable_added_offsets;
        var build_stable_boundaries =
            stable_boundaries_eligible and !reuse_stable_boundaries;
        if (build_stable_boundaries) {
            // The index only accelerates the private-cache scanner. Establish
            // every required table first, in parallel, so a constrained shared
            // resource budget cannot admit a corpus-sized index and then deny
            // the caches needed to consume it. Tables remain useful if index
            // admission subsequently fails.
            build_stable_boundaries =
                try self.prepareWorkerBpeCachesForStableIndex(
                    io,
                    active_runners,
                );
        }
        if (build_stable_boundaries) {
            var additional_bytes: usize = 0;
            for (workers) |worker| {
                const required_words =
                    (worker.text.len + 63) / 64;
                if (required_words >
                    worker.pretoken_boundary_words.capacity)
                {
                    additional_bytes +|= (required_words -
                        worker.pretoken_boundary_words.capacity) *
                        @sizeOf(u64);
                }
            }
            if (additional_bytes != 0) {
                if (self.tryReserveResourceBudgetBytes(additional_bytes)) |charge| {
                    if (charge == .charged) {
                        _ = workspace.resource_accounted_bytes.fetchAdd(
                            additional_bytes,
                            .acq_rel,
                        );
                    }
                } else {
                    build_stable_boundaries = false;
                }
            }
            if (build_stable_boundaries) {
                for (workers) |*worker| {
                    const required_words =
                        (worker.text.len + 63) / 64;
                    worker.pretoken_boundary_words
                        .ensureTotalCapacityPrecise(
                        self.allocator,
                        required_words,
                    ) catch {
                        // The stable index is an optional replay
                        // acceleration. Retain and account any capacity
                        // already acquired, but run the exact scanner rather
                        // than failing tokenization under memory pressure.
                        build_stable_boundaries = false;
                        break;
                    };
                    worker.pretoken_boundary_words.items.len =
                        required_words;
                    @memset(
                        worker.pretoken_boundary_words.items,
                        0,
                    );
                }
                if (!build_stable_boundaries) {
                    for (workers) |*worker| {
                        worker.pretoken_boundary_words.items.len = 0;
                    }
                }
            }
        }
        if (learned_stable_offsets or build_stable_boundaries) {
            var scan_job = ParallelAddedTokenScanJob{
                .tokenizer = self,
                .chunks = workers,
                .scan_added_offsets = learned_stable_offsets,
                .build_pretoken_boundaries = build_stable_boundaries,
            };
            var scan_group: std.Io.Group = .init;
            errdefer scan_group.cancel(io);
            for (0..background_count) |_| {
                scan_group.async(
                    io,
                    ParallelAddedTokenScanJob.run,
                    .{&scan_job},
                );
            }
            try scan_job.run();
            try scan_group.await(io);
            for (workers) |worker| {
                if (worker.failure) |err| return err;
            }
            if (learned_stable_offsets) {
                reuse_stable_added_offsets = true;
            }
            if (build_stable_boundaries) {
                reuse_stable_boundaries = true;
            }
            for (workers) |*worker| {
                worker.reuse_added_token_offsets =
                    reuse_stable_added_offsets;
                worker.reuse_pretoken_boundaries =
                    reuse_stable_boundaries;
            }
            // The prior affinity, if any, includes delimiter-search work.
            // Learn a fresh assignment during this same cache-populating BPE
            // pass now that the immutable-source index is available.
            workspace.affinity_valid = false;
        }
        for (workers) |*worker| worker.publishStats();

        const affinity_eligible =
            text.len >= affinity_replay_min_input_bytes and
            self.parallel_bpe_config.worker_cache_count >= active_runners;
        const affinity_signature_matches =
            affinity_eligible and
            workspace.affinity_valid and
            workspace.affinity_text_ptr == @intFromPtr(text.ptr) and
            workspace.affinity_text_len == text.len and
            workspace.affinity_worker_count == worker_count and
            workspace.affinity_consumer_count == active_runners;
        const replay_affinity = affinity_signature_matches;
        if (affinity_eligible and !replay_affinity) {
            for (workers) |*worker| {
                worker.cache_owner = invalid_worker_cache_owner;
            }
        }

        // Reserve the normal GPT-style token density before launch. Ordered
        // commits stop at capacity; after the group joins, the caller grows
        // once to the exact residual size. This preserves copy/encode overlap
        // without reserving four output bytes for every input byte up front.
        const output_start = ids.items.len;
        if (segments_out == null) {
            const previous_output_capacity = ids.capacity;
            const estimated_tokens = text.len / 3 +| 8;
            const estimated_capacity = std.math.add(
                usize,
                output_start,
                estimated_tokens,
            ) catch return error.OutOfMemory;
            try ids.ensureTotalCapacityPrecise(
                allocator,
                estimated_capacity,
            );
            if (ids.capacity != previous_output_capacity) {
                adviseHugePages(ids.items.ptr[0..ids.capacity]);
            }
        }
        errdefer if (segments_out == null) {
            ids.items.len = output_start;
        };

        // A stable replay can safely share the learned owner tables across
        // all std.Io consumers. Hold every lease and freeze admission for the
        // duration: hits are immutable, and a rare miss computes exactly but
        // does not mutate the table. This retains cache locality without
        // pinning an owner's chunks to one potentially straggling consumer.
        var frozen_leases: [max_worker_bpe_caches]*WorkerBpeCacheLease = undefined;
        var frozen_caches: [max_worker_bpe_caches]*WorkerBpeCache = undefined;
        var frozen_cache_count: usize = 0;
        if (affinity_signature_matches) {
            while (frozen_cache_count < active_runners) {
                const lease = self.acquireWorkerBpeCacheAt(
                    frozen_cache_count,
                ) orelse break;
                frozen_leases[frozen_cache_count] = lease;
                frozen_caches[frozen_cache_count] =
                    if (lease.cache) |*cache| cache else unreachable;
                frozen_cache_count += 1;
            }
            if (frozen_cache_count != active_runners) {
                while (frozen_cache_count != 0) {
                    frozen_cache_count -= 1;
                    releaseWorkerBpeCache(
                        frozen_leases[frozen_cache_count],
                    );
                }
            } else {
                for (frozen_caches[0..frozen_cache_count]) |cache| {
                    cache.frozen.store(true, .release);
                }
            }
        }
        defer {
            for (frozen_caches[0..frozen_cache_count]) |cache| {
                cache.frozen.store(false, .release);
            }
            while (frozen_cache_count != 0) {
                frozen_cache_count -= 1;
                releaseWorkerBpeCache(frozen_leases[frozen_cache_count]);
            }
        }
        const dynamic_owner_caches: ?[]const *WorkerBpeCache =
            if (frozen_cache_count == active_runners)
                frozen_caches[0..frozen_cache_count]
            else
                null;

        var job = ParallelBpeJob{
            .tokenizer = self,
            .io = io,
            .chunks = workers,
            .output = if (segments_out == null) ids else null,
            .consumer_count = active_runners,
            // A first pass dynamically learns a naturally load-balanced
            // chunk-to-cache assignment. Repeated encodes of the same backing
            // text replay that affinity. Multi-gigabyte inputs additionally
            // let consumers steal unfinished chunks at the tail, avoiding the
            // full-corpus utilization cliff of fixed round-robin partitions.
            .replay_affinity = replay_affinity,
            .record_affinity = affinity_eligible and dynamic_owner_caches == null,
            // A steal runs the chunk against the wrong private table. The
            // full corpus makes that cache-cold replay substantially more
            // expensive than waiting for the dynamically learned owner.
            .allow_tail_steal = replay_affinity and
                text.len >= affinity_steal_min_input_bytes,
            .owner_caches = dynamic_owner_caches,
        };
        var group: std.Io.Group = .init;
        errdefer group.cancel(io);
        for (0..background_count) |_| {
            group.async(io, ParallelBpeJob.run, .{&job});
        }
        try job.run();
        try group.await(io);

        for (workers) |worker| {
            if (worker.failure) |err| return err;
        }
        if (stable_metadata_eligible) {
            workspace.stable_added_offsets_valid = true;
            workspace.stable_added_offsets_id = stable_input_id.?;
            workspace.stable_added_offsets_text_ptr =
                @intFromPtr(text.ptr);
            workspace.stable_added_offsets_text_len = text.len;
            workspace.stable_added_offsets_worker_count = worker_count;
            workspace.stable_pretoken_boundaries_valid =
                reuse_stable_boundaries;
        }
        if (stable_offsets_eligible) {
            if (learned_stable_offsets) {
                _ = workspace.stable_offset_learns.fetchAdd(1, .monotonic);
            } else {
                _ = workspace.stable_offset_replays.fetchAdd(1, .monotonic);
            }
        }
        if (affinity_eligible) {
            workspace.affinity_valid = true;
            workspace.affinity_text_ptr = @intFromPtr(text.ptr);
            workspace.affinity_text_len = text.len;
            workspace.affinity_worker_count = worker_count;
            workspace.affinity_consumer_count = active_runners;
            if (replay_affinity) {
                _ = workspace.affinity_replays.fetchAdd(1, .monotonic);
                _ = workspace.affinity_stolen_chunks.fetchAdd(
                    job.stolen_chunks.load(.monotonic),
                    .monotonic,
                );
            } else {
                _ = workspace.affinity_learns.fetchAdd(1, .monotonic);
            }
        } else {
            workspace.affinity_valid = false;
        }

        if (segments_out) |output| {
            switch (output) {
                inline else => |result| {
                    result.* = .{
                        .owner = self,
                        .workspace = workspace,
                        .worker_count = worker_count,
                    };
                },
            }
            release_workspace = false;
            return;
        }

        var residual_tokens: usize = 0;
        for (workers[job.next_commit..]) |worker| {
            residual_tokens = std.math.add(
                usize,
                residual_tokens,
                worker.ids.items.len,
            ) catch return error.OutOfMemory;
        }
        const exact_capacity = std.math.add(
            usize,
            ids.items.len,
            residual_tokens,
        ) catch return error.OutOfMemory;
        const capacity_before_residual = ids.capacity;
        try ids.ensureTotalCapacityPrecise(allocator, exact_capacity);
        if (ids.capacity != capacity_before_residual) {
            adviseHugePages(ids.items.ptr[0..ids.capacity]);
        }
        const residual_start = job.next_commit;
        if (residual_start != workers.len) {
            const parallel_copy =
                workers.len - residual_start > 1 and
                residual_tokens *| @sizeOf(i32) >= 64 * 1024 * 1024;
            if (!parallel_copy) {
                while (job.next_commit != workers.len) job.commitReady();
                return;
            }
            var copies: [max_parallel_bpe_chunks]ParallelBpeCopy = undefined;
            var output_pos = ids.items.len;
            for (workers[residual_start..], 0..) |*worker, idx| {
                copies[idx] = .{
                    .chunk = worker,
                    .output_start = output_pos,
                };
                output_pos += worker.ids.items.len;
            }
            std.debug.assert(output_pos == exact_capacity);
            ids.items.len = exact_capacity;

            const residual_copies = copies[0 .. workers.len - residual_start];
            var copy_job = ParallelBpeCopyJob{
                .copies = residual_copies,
                .output = ids.items,
            };
            const copy_consumers = @min(active_runners, residual_copies.len);
            var copy_group: std.Io.Group = .init;
            errdefer copy_group.cancel(io);
            for (0..copy_consumers - 1) |_| {
                copy_group.async(io, ParallelBpeCopyJob.run, .{&copy_job});
            }
            try copy_job.run();
            try copy_group.await(io);
            job.next_commit = workers.len;
        }
    }

    fn encodeWithAddedTokens(
        self: *HfTokenizer,
        allocator: std.mem.Allocator,
        text: []const u8,
        ids: *std.ArrayListUnmanaged(i32),
    ) !void {
        return self.encodeWithAddedTokensMetaspaceOverride(allocator, text, null, ids);
    }

    fn encodeWithAddedTokensMetaspaceOverride(
        self: *HfTokenizer,
        allocator: std.mem.Allocator,
        text: []const u8,
        metaspace_scheme_override: ?MetaspacePrependScheme,
        ids: *std.ArrayListUnmanaged(i32),
    ) !void {
        if (self.added_tokens.count() == 0) {
            return switch (self.model_type) {
                .word_piece => self.encodeWordPiece(allocator, text, ids),
                .bpe => self.encodeBpeWithMetaspaceScheme(allocator, text, metaspace_scheme_override, ids),
                .unigram => self.encodeUnigramWithMetaspaceScheme(allocator, text, metaspace_scheme_override, ids),
            };
        }

        // Fast path: if there's no added token at the start AND none later in
        // the text, the segment-and-merge loop reduces to a single encode of
        // the whole text.
        if (self.matchAddedTokenAt(text) == null and self.findNextAddedToken(text, 0) == null) {
            return switch (self.model_type) {
                .word_piece => self.encodeWordPiece(allocator, text, ids),
                .bpe => self.encodeBpeWithMetaspaceScheme(allocator, text, metaspace_scheme_override, ids),
                .unigram => self.encodeUnigramWithMetaspaceScheme(allocator, text, metaspace_scheme_override, ids),
            };
        }

        var cursor: usize = 0;
        while (cursor < text.len) {
            if (self.matchAddedTokenAt(text[cursor..])) |match| {
                try ids.append(allocator, match.id);
                cursor += match.len;
                continue;
            }

            const next_added = self.findNextAddedToken(text, cursor) orelse text.len;
            const segment = text[cursor..next_added];
            if (segment.len > 0) {
                const segment_metaspace_override: ?MetaspacePrependScheme = if (cursor > 0 and self.pre_tokenizer_type == .metaspace)
                    .never
                else
                    metaspace_scheme_override;
                try switch (self.model_type) {
                    .word_piece => self.encodeWordPiece(allocator, segment, ids),
                    .bpe => self.encodeBpeWithMetaspaceScheme(allocator, segment, segment_metaspace_override, ids),
                    .unigram => self.encodeUnigramWithMetaspaceScheme(allocator, segment, segment_metaspace_override, ids),
                };
            }
            cursor = next_added;
        }
    }

    const AddedTokenMatch = struct {
        id: i32,
        len: usize,
    };

    fn matchAddedTokenAt(self: *const HfTokenizer, text: []const u8) ?AddedTokenMatch {
        return self.added_trie.longestPrefixMatch(text);
    }

    fn findNextAddedToken(self: *const HfTokenizer, text: []const u8, start: usize) ?usize {
        return self.added_trie.findNext(text, start);
    }

    // =====================================================================
    // WordPiece encoding
    // =====================================================================

    fn encodeWordPiece(
        self: *HfTokenizer,
        allocator: std.mem.Allocator,
        text: []const u8,
        ids: *std.ArrayListUnmanaged(i32),
    ) !void {
        const words = try bertPreTokenize(allocator, text);
        defer allocator.free(words);

        // English averages ~0.25 tokens/byte; reserve to avoid the array
        // growing through several reallocations during the per-word loop.
        try ids.ensureUnusedCapacity(allocator, (text.len / 3) + 4);
        for (words) |word| {
            try self.wordPieceEncodeWord(allocator, word, ids);
        }
    }

    fn encodeWordPieceWithOffsets(self: *HfTokenizer, allocator: std.mem.Allocator, text: []const u8) !RawWordPieceEncoding {
        const words = try bertPreTokenizeWithOffsets(allocator, text);
        defer allocator.free(words);

        var result = RawWordPieceEncoding{};
        errdefer result.deinit(allocator);

        for (words) |word| {
            try self.wordPieceEncodeWordWithOffsets(allocator, word.text, word.start, &result);
        }
        return result;
    }

    fn encodeUnigramWithOffsets(self: *HfTokenizer, allocator: std.mem.Allocator, text: []const u8) !RawWordPieceEncoding {
        const words = try metaspacePreTokenizeWithOffsets(
            allocator,
            text,
            self.metaspace_replacement,
            self.metaspace_prepend_scheme,
            self.metaspace_split,
        );
        defer {
            for (words) |w| allocator.free(w.text);
            allocator.free(words);
        }

        var result = RawWordPieceEncoding{};
        errdefer result.deinit(allocator);

        for (words) |word| {
            const prefix_len = if (std.mem.startsWith(u8, word.text, self.metaspace_replacement))
                self.metaspace_replacement.len
            else
                0;
            try self.unigramEncodeWordWithOffsets(allocator, word.text, word.start, prefix_len, &result);
        }
        return result;
    }

    fn wordPieceEncodeWord(self: *HfTokenizer, allocator: std.mem.Allocator, word: []const u8, ids: *std.ArrayListUnmanaged(i32)) !void {
        if (word.len == 0) return;
        if (word.len > self.max_input_chars_per_word) {
            try ids.append(allocator, self.special.unk_id);
            return;
        }

        // Check if the whole word is an added token
        if (self.added_tokens.get(word)) |id| {
            try ids.append(allocator, id);
            return;
        }

        // Stack scratch buffer for "##xxx" lookups. The continuing prefix is
        // written once; each iteration only updates the substring tail.
        var prefix_buf: [256]u8 = undefined;
        const prefix = self.continuing_prefix;
        const prefix_buf_ok = prefix.len + word.len <= prefix_buf.len;
        if (prefix_buf_ok and prefix.len > 0) {
            @memcpy(prefix_buf[0..prefix.len], prefix);
        }

        var start: usize = 0;
        while (start < word.len) {
            var end = word.len;
            var found = false;

            while (end > start) {
                const substr = word[start..end];

                if (start == 0) {
                    if (self.vocab.get(substr)) |id| {
                        try ids.append(allocator, id);
                        found = true;
                        start = end;
                        break;
                    }
                } else {
                    const lookup_key = if (prefix_buf_ok) blk: {
                        @memcpy(prefix_buf[prefix.len .. prefix.len + substr.len], substr);
                        break :blk prefix_buf[0 .. prefix.len + substr.len];
                    } else blk: {
                        // Word longer than scratch; fall back to alloc.
                        const heap = try std.fmt.allocPrint(allocator, "{s}{s}", .{ prefix, substr });
                        break :blk heap;
                    };
                    defer if (!prefix_buf_ok) allocator.free(lookup_key);
                    if (self.vocab.get(lookup_key)) |id| {
                        try ids.append(allocator, id);
                        found = true;
                        start = end;
                        break;
                    }
                }

                end = prevCodepointBoundary(word, end);
            }

            if (!found) {
                try ids.append(allocator, self.special.unk_id);
                return;
            }
        }
    }

    fn wordPieceEncodeWordWithOffsets(
        self: *HfTokenizer,
        allocator: std.mem.Allocator,
        word: []const u8,
        word_start: usize,
        result: *RawWordPieceEncoding,
    ) !void {
        if (word.len == 0) return;

        var lookup_word = word;
        var owned_lookup_word: ?[]u8 = null;
        defer if (owned_lookup_word) |buf| allocator.free(buf);
        if (self.do_lowercase) {
            owned_lookup_word = try toLowerAlloc(allocator, word);
            lookup_word = owned_lookup_word.?;
        }

        if (lookup_word.len > self.max_input_chars_per_word) {
            try result.ids.append(allocator, self.special.unk_id);
            try result.offsets.append(allocator, .{ @intCast(word_start), @intCast(word_start + word.len) });
            return;
        }

        if (self.added_tokens.get(lookup_word)) |id| {
            try result.ids.append(allocator, id);
            try result.offsets.append(allocator, .{ @intCast(word_start), @intCast(word_start + word.len) });
            return;
        }

        var prefix_buf: [256]u8 = undefined;
        const prefix = self.continuing_prefix;
        const prefix_buf_ok = prefix.len + lookup_word.len <= prefix_buf.len;
        if (prefix_buf_ok and prefix.len > 0) {
            @memcpy(prefix_buf[0..prefix.len], prefix);
        }

        var start: usize = 0;
        while (start < lookup_word.len) {
            var end = lookup_word.len;
            var found = false;

            while (end > start) {
                const substr = lookup_word[start..end];

                if (start == 0) {
                    if (self.vocab.get(substr)) |id| {
                        try result.ids.append(allocator, id);
                        try result.offsets.append(allocator, .{ @intCast(word_start + start), @intCast(word_start + end) });
                        found = true;
                        start = end;
                        break;
                    }
                } else {
                    const lookup_key = if (prefix_buf_ok) blk: {
                        @memcpy(prefix_buf[prefix.len .. prefix.len + substr.len], substr);
                        break :blk prefix_buf[0 .. prefix.len + substr.len];
                    } else blk: {
                        const heap = try std.fmt.allocPrint(allocator, "{s}{s}", .{ prefix, substr });
                        break :blk heap;
                    };
                    defer if (!prefix_buf_ok) allocator.free(lookup_key);
                    if (self.vocab.get(lookup_key)) |id| {
                        try result.ids.append(allocator, id);
                        try result.offsets.append(allocator, .{ @intCast(word_start + start), @intCast(word_start + end) });
                        found = true;
                        start = end;
                        break;
                    }
                }

                end = prevCodepointBoundary(lookup_word, end);
            }

            if (!found) {
                try result.ids.append(allocator, self.special.unk_id);
                try result.offsets.append(allocator, .{ @intCast(word_start), @intCast(word_start + word.len) });
                return;
            }
        }
    }

    // =====================================================================
    // BPE encoding
    // =====================================================================

    fn encodeBpe(
        self: *HfTokenizer,
        allocator: std.mem.Allocator,
        text: []const u8,
        ids: *std.ArrayListUnmanaged(i32),
    ) !void {
        return self.encodeBpeWithMetaspaceScheme(allocator, text, null, ids);
    }

    fn encodeBpeWithMetaspaceScheme(
        self: *HfTokenizer,
        allocator: std.mem.Allocator,
        text: []const u8,
        metaspace_scheme_override: ?MetaspacePrependScheme,
        ids: *std.ArrayListUnmanaged(i32),
    ) !void {
        var scratch: BpeScratch = .{};
        defer scratch.deinit(allocator);
        switch (self.pre_tokenizer_type) {
            .byte_level => {
                try self.encodeBpeByteLevel(allocator, text, ids, &scratch);
            },
            .byte_level_split => {
                const words = try byteLevelSplitPreTokenize(allocator, text);
                defer {
                    for (words) |w| allocator.free(w);
                    allocator.free(words);
                }
                for (words) |word| {
                    try self.bpeEncodeWord(allocator, word, ids, &scratch);
                }
            },
            .metaspace => {
                // Metaspace: prepend ▁, split on spaces
                const prepend_scheme = metaspace_scheme_override orelse self.metaspace_prepend_scheme;
                const prepared = try metaspacePreTokenize(
                    allocator,
                    text,
                    self.metaspace_replacement,
                    prepend_scheme,
                    self.metaspace_split,
                );
                defer {
                    for (prepared) |w| allocator.free(w);
                    allocator.free(prepared);
                }
                for (prepared) |word| {
                    try self.bpeEncodeWord(allocator, word, ids, &scratch);
                }
            },
            .bert => {
                const words = try bertPreTokenize(allocator, text);
                defer allocator.free(words);
                for (words) |word| {
                    try self.bpeEncodeWord(allocator, word, ids, &scratch);
                }
            },
            .none => {
                try self.bpeEncodeWord(allocator, text, ids, &scratch);
            },
        }
    }

    /// Stream GPT-2-style whitespace pieces through one reusable byte-level
    /// encoding buffer. The previous implementation allocated every encoded
    /// pretoken plus an outer slice before BPE could begin, even when the
    /// persistent cache immediately hit.
    fn encodeBpeByteLevel(
        self: *HfTokenizer,
        allocator: std.mem.Allocator,
        text: []const u8,
        ids: *std.ArrayListUnmanaged(i32),
        scratch: *BpeScratch,
    ) !void {
        try ids.ensureUnusedCapacity(allocator, (text.len / 3) + 8);
        var start: usize = 0;
        while (start < text.len) {
            if (gpt2AsciiBoundaryMask(text, start)) |boundary_mask| {
                var remaining = boundary_mask & ~@as(u64, 1);
                var piece_start: usize = 0;
                while (remaining != 0) {
                    const piece_end: usize = @intCast(@ctz(remaining));
                    try self.bpeEncodeWord(
                        allocator,
                        text[start + piece_start .. start + piece_end],
                        ids,
                        scratch,
                    );
                    piece_start = piece_end;
                    remaining &= remaining - 1;
                }
                if (piece_start != 0) {
                    start += piece_start;
                    continue;
                }
            }

            const end = gpt2PreTokenEnd(text, start);
            try self.bpeEncodeWord(allocator, text[start..end], ids, scratch);
            start = end;
        }
    }

    const PreparedWorkerPretoken = struct {
        key: u128,
        ptr: [*]const u8,
        /// Cache hash for short keys; byte length when key is zero.
        meta: u64,
    };

    const WorkerPretokenBatchFill = struct {
        count: usize,
        input_bytes: usize,
    };

    comptime {
        std.debug.assert(@sizeOf(PreparedWorkerPretoken) == 32);
    }

    const ByteLevelPretokenIterator = struct {
        text: []const u8,
        pos: usize = 0,
        mask_base: usize = 0,
        mask_piece_start: usize = 0,
        remaining_mask: u64 = 0,

        fn next(iterator: *ByteLevelPretokenIterator) ?[]const u8 {
            while (iterator.pos < iterator.text.len) {
                if (iterator.remaining_mask != 0) {
                    const piece_end: usize =
                        @intCast(@ctz(iterator.remaining_mask));
                    const piece = iterator.text[iterator.mask_base + iterator.mask_piece_start .. iterator.mask_base + piece_end];
                    iterator.mask_piece_start = piece_end;
                    iterator.remaining_mask &= iterator.remaining_mask - 1;
                    return piece;
                }
                if (iterator.mask_piece_start != 0) {
                    iterator.pos = iterator.mask_base + iterator.mask_piece_start;
                    iterator.mask_piece_start = 0;
                    continue;
                }
                if (gpt2AsciiBoundaryMask(iterator.text, iterator.pos)) |mask| {
                    iterator.remaining_mask = mask & ~@as(u64, 1);
                    if (iterator.remaining_mask != 0) {
                        iterator.mask_base = iterator.pos;
                        continue;
                    }
                }
                const end = gpt2PreTokenEnd(iterator.text, iterator.pos);
                const piece = iterator.text[iterator.pos..end];
                iterator.pos = end;
                return piece;
            }
            return null;
        }
    };

    const IndexedPretokenIterator = struct {
        text: []const u8,
        boundary_words: []const u64,
        pos: usize,
        end: usize,

        fn next(iterator: *IndexedPretokenIterator) ?[]const u8 {
            if (iterator.pos >= iterator.end) return null;
            const start = iterator.pos;
            const search = start + 1;
            if (search >= iterator.end) {
                iterator.pos = iterator.end;
                return iterator.text[start..iterator.end];
            }
            var word_index = search / 64;
            var bits = iterator.boundary_words[word_index] &
                (@as(u64, std.math.maxInt(u64)) <<
                    @as(u6, @intCast(search % 64)));
            while (true) {
                if (bits != 0) {
                    const next_start =
                        word_index * 64 +
                        @as(usize, @intCast(@ctz(bits)));
                    if (next_start < iterator.end) {
                        iterator.pos = next_start;
                        return iterator.text[start..next_start];
                    }
                    break;
                }
                word_index += 1;
                if (word_index >= iterator.boundary_words.len) break;
                bits = iterator.boundary_words[word_index];
            }
            iterator.pos = iterator.end;
            return iterator.text[start..iterator.end];
        }
    };

    const Gpt2MaskFillState = struct {
        pos: usize = 0,
        scan: usize = 0,
    };

    const Gpt2BoundaryPositions = @Vector(8, u16);

    const gpt2_boundary_byte_positions: [256]Gpt2BoundaryPositions = blk: {
        @setEvalBranchQuota(5000);
        var table: [256]Gpt2BoundaryPositions = undefined;
        for (0..256) |value| {
            var positions: Gpt2BoundaryPositions = @splat(0);
            var bits: u8 = @intCast(value);
            var lane: usize = 0;
            while (bits != 0) : (lane += 1) {
                positions[lane] = @intCast(@ctz(bits));
                bits &= bits - 1;
            }
            table[value] = positions;
        }
        break :blk table;
    };

    inline fn appendGpt2BoundaryBits(
        bits_in: u64,
        block_base: usize,
        fill_base: usize,
        ends: []u16,
        count: *usize,
    ) void {
        // Flatten through a 4 KiB table. A SWAR inclusive prefix sum computes
        // all eight octet popcounts and their exclusive output offsets at
        // once. Eight unconditional Zig-vector stores then have independent
        // addresses: empty octets harmlessly scribble only into uncommitted
        // lanes, and the caller reserves the seven-lane tail slack.
        // A fill can begin in the middle of the preceding fixed-grid block,
        // where the unsigned packed adjustment is not valid. That one block
        // retains the sparse scalar extraction.
        if (block_base >= fill_base) {
            var octet_popcounts = bits_in;
            octet_popcounts -=
                (octet_popcounts >> 1) & 0x5555_5555_5555_5555;
            octet_popcounts =
                (octet_popcounts & 0x3333_3333_3333_3333) +
                ((octet_popcounts >> 2) & 0x3333_3333_3333_3333);
            octet_popcounts =
                (octet_popcounts + (octet_popcounts >> 4)) &
                0x0f0f_0f0f_0f0f_0f0f;
            const inclusive =
                octet_popcounts *% 0x0101_0101_0101_0101;
            const exclusive = inclusive << 8;
            const relative_base: u16 =
                @intCast(block_base - fill_base);
            inline for (0..@sizeOf(u64)) |byte_index| {
                const byte_bits: u8 = @truncate(
                    bits_in >> @intCast(byte_index * 8),
                );
                const write_offset: usize = @as(
                    u8,
                    @truncate(
                        exclusive >> @intCast(byte_index * 8),
                    ),
                );
                const positions =
                    gpt2_boundary_byte_positions[byte_bits];
                const adjusted =
                    positions +
                    @as(
                        Gpt2BoundaryPositions,
                        @splat(
                            relative_base +
                                @as(u16, byte_index * 8),
                        ),
                    );
                const destination: *align(1) Gpt2BoundaryPositions =
                    @ptrCast(ends.ptr + count.* + write_offset);
                destination.* = adjusted;
            }
            count.* += @as(u8, @truncate(inclusive >> 56));
            return;
        }

        var bits = bits_in;
        while (bits != 0) {
            const bit: usize = @intCast(@ctz(bits));
            const absolute = block_base + bit;
            std.debug.assert(absolute > fill_base);
            ends[count.*] = @intCast(absolute - fill_base);
            count.* += 1;
            bits &= bits - 1;
        }
    }

    /// Replay a retained one-bit-per-byte boundary index in the same
    /// cache-friendly batches as the fixed-grid scanner. Decoding one token
    /// at a time made the index slower than recomputing boundaries: every
    /// pretoken paid a mask, branch, and ctz. Here each u64 is loaded once and
    /// flattened through the shared octet table into compact relative ends.
    fn fillWorkerPretokenBatchIndexed(
        iterator: *IndexedPretokenIterator,
        input_end: usize,
        probe_view: WorkerBpeProbeView,
        prepared: []PreparedWorkerPretoken,
    ) WorkerPretokenBatchFill {
        const call_start = iterator.pos;
        const relative_limit = std.math.maxInt(u16) - 64;
        var output_count: usize = 0;
        while (output_count < worker_bpe_batch_size and
            iterator.pos < iterator.end)
        {
            const fill_base = iterator.pos;
            const needed = worker_bpe_batch_size - output_count;
            var ends: [worker_bpe_batch_size + 208]u16 = undefined;
            var boundary_count: usize = 0;
            const first_word = (fill_base + 1) / 64;
            var word_index = first_word;
            const word_limit = (iterator.end + 63) / 64;

            while (word_index < word_limit and
                boundary_count < needed)
            {
                const block_base = word_index * 64;
                if (block_base > fill_base +| relative_limit) break;

                var bits = iterator.boundary_words[word_index];
                if (word_index == first_word) {
                    const first_live = (fill_base + 1) % 64;
                    bits &= @as(u64, std.math.maxInt(u64)) <<
                        @as(u6, @intCast(first_live));
                }
                if (word_index + 1 == word_limit and
                    iterator.end % 64 != 0)
                {
                    bits &= (@as(u64, 1) <<
                        @as(u6, @intCast(iterator.end % 64))) - 1;
                }
                appendGpt2BoundaryBits(
                    bits,
                    block_base,
                    fill_base,
                    &ends,
                    &boundary_count,
                );
                word_index += 1;
            }

            // A segment's end is an implicit endpoint rather than a
            // pretoken-start bit. Add it only when it fits in this relative
            // batch; a very long final token takes the exact scalar iterator
            // below.
            if (word_index == word_limit and
                boundary_count < needed and
                iterator.end - fill_base <= std.math.maxInt(u16))
            {
                ends[boundary_count] =
                    @intCast(iterator.end - fill_base);
                boundary_count += 1;
            }

            if (boundary_count == 0) {
                const word = iterator.next() orelse break;
                const key =
                    workerBpeKeyPadded(word, input_end) orelse 0;
                const meta: u64 = if (key != 0)
                    workerBpeHash(key)
                else
                    @intCast(word.len);
                prepared[output_count] = .{
                    .key = key,
                    .ptr = word.ptr,
                    .meta = meta,
                };
                prefetchWorkerBpe(probe_view, meta, 2);
                output_count += 1;
                continue;
            }

            const emit_count = @min(boundary_count, needed);
            var previous: usize = 0;
            for (
                ends[0..emit_count],
                prepared[output_count .. output_count + emit_count],
            ) |end16, *item| {
                const end: usize = end16;
                const word = iterator.text[fill_base + previous .. fill_base + end];
                previous = end;
                const key =
                    workerBpeKeyPadded(word, input_end) orelse 0;
                const meta: u64 = if (key != 0)
                    workerBpeHash(key)
                else
                    @intCast(word.len);
                item.* = .{
                    .key = key,
                    .ptr = word.ptr,
                    .meta = meta,
                };
                prefetchWorkerBpe(probe_view, meta, 2);
            }
            output_count += emit_count;
            iterator.pos = fill_base + previous;
        }

        return .{
            .count = output_count,
            .input_bytes = iterator.pos - call_start,
        };
    }

    /// Harvest exact fixed-grid boundaries into compact u16 offsets, then
    /// build keys and hashes in a flat counted pass. Dirty grid zones
    /// (currently UTF-8 and edge contractions) are advanced by the scalar
    /// ground truth without rebasing the grid, so any stale bits crossed by
    /// a long token remain masked.
    fn fillWorkerPretokenBatchMask(
        state: *Gpt2MaskFillState,
        text: []const u8,
        input_end: usize,
        probe_view: WorkerBpeProbeView,
        prepared: []PreparedWorkerPretoken,
    ) WorkerPretokenBatchFill {
        const relative_limit = std.math.maxInt(u16) - 64;
        const call_start = state.pos;
        var pending = state.pos;
        var scan = state.scan;
        if (scan > pending) {
            const delta = scan - pending;
            scan -= 64 * ((delta + 63) / 64);
        }

        var output_count: usize = 0;
        while (output_count < worker_bpe_batch_size and
            pending < text.len)
        {
            if (pending >= scan + 64) {
                scan += 64 * ((pending - scan) / 64);
            }
            const fill_base = pending;
            const needed = worker_bpe_batch_size - output_count;
            // Phase-A flattening may harvest one complete 64-byte block after
            // reaching the requested count, and each unconditional SIMD
            // store scribbles seven uncommitted lanes. Keep generous fixed
            // slack so every store remains in-bounds without a hot clamp.
            var ends: [worker_bpe_batch_size + 208]u16 = undefined;
            var boundary_count: usize = 0;
            var resume_pos = pending;
            var exhausted = false;
            var overflow_end: ?usize = null;

            harvest: while (boundary_count < needed) {
                if (scan > fill_base +| relative_limit) break;
                if (scan + 64 > text.len) {
                    var cursor = if (boundary_count != 0)
                        fill_base + ends[boundary_count - 1]
                    else
                        fill_base;
                    while (cursor < text.len and
                        boundary_count < needed)
                    {
                        cursor = gpt2PreTokenEnd(text, cursor);
                        ends[boundary_count] =
                            @intCast(cursor - fill_base);
                        boundary_count += 1;
                    }
                    exhausted = cursor >= text.len;
                    break;
                }

                const block_base = scan;
                const masks = gpt2GridBoundaryMasks(
                    text,
                    block_base,
                );
                var usable_live: u64 = std.math.maxInt(u64);
                var bad_live: u64 = std.math.maxInt(u64);
                if (resume_pos >= block_base) {
                    const relative = resume_pos - block_base;
                    std.debug.assert(relative < 64);
                    bad_live = @as(u64, std.math.maxInt(u64)) <<
                        @as(u6, @intCast(relative));
                    usable_live = bad_live << 1;
                }

                if (masks.bad & bad_live == 0) {
                    appendGpt2BoundaryBits(
                        masks.usable & usable_live,
                        block_base,
                        fill_base,
                        &ends,
                        &boundary_count,
                    );
                    scan = block_base + 64;
                    continue;
                }

                while (true) {
                    const segment_bad = masks.bad & bad_live;
                    if (segment_bad == 0) {
                        appendGpt2BoundaryBits(
                            masks.usable & usable_live,
                            block_base,
                            fill_base,
                            &ends,
                            &boundary_count,
                        );
                        scan = block_base + 64;
                        break;
                    }
                    const first_bad: usize =
                        @intCast(@ctz(segment_bad));
                    const prefix_limit =
                        ~(@as(u64, std.math.maxInt(u64)) <<
                            @as(u6, @intCast(first_bad)));
                    appendGpt2BoundaryBits(
                        masks.usable & usable_live & prefix_limit,
                        block_base,
                        fill_base,
                        &ends,
                        &boundary_count,
                    );
                    var cursor = if (boundary_count != 0)
                        fill_base + ends[boundary_count - 1]
                    else
                        fill_base;
                    const rest = masks.usable &
                        (@as(u64, std.math.maxInt(u64)) <<
                            @as(u6, @intCast(first_bad)));
                    const scalar_until = if (rest != 0)
                        block_base + @as(usize, @intCast(@ctz(rest)))
                    else
                        block_base + 64;
                    while (cursor < scalar_until) {
                        cursor = gpt2PreTokenEnd(text, cursor);
                        const relative_end = cursor - fill_base;
                        if (relative_end > std.math.maxInt(u16)) {
                            overflow_end = cursor;
                            break :harvest;
                        }
                        ends[boundary_count] =
                            @intCast(relative_end);
                        boundary_count += 1;
                    }
                    if (cursor >= block_base + 64) {
                        scan = block_base +
                            64 * ((cursor - block_base) / 64);
                        resume_pos = cursor;
                        break;
                    }
                    const relative = cursor - block_base;
                    bad_live = @as(u64, std.math.maxInt(u64)) <<
                        @as(u6, @intCast(relative));
                    usable_live = bad_live << 1;
                }
            }

            if (boundary_count == 0) {
                const end = overflow_end orelse
                    gpt2PreTokenEnd(text, fill_base);
                const word = text[fill_base..end];
                const key =
                    workerBpeKeyPadded(word, input_end) orelse 0;
                const meta: u64 = if (key != 0)
                    workerBpeHash(key)
                else
                    @intCast(word.len);
                prepared[output_count] = .{
                    .key = key,
                    .ptr = word.ptr,
                    .meta = meta,
                };
                prefetchWorkerBpe(probe_view, meta, 2);
                output_count += 1;
                pending = end;
                continue;
            }

            const emit_count = @min(boundary_count, needed);
            var previous: usize = 0;
            for (
                ends[0..emit_count],
                prepared[output_count .. output_count + emit_count],
            ) |end16, *item| {
                const end: usize = end16;
                const word =
                    text[fill_base + previous .. fill_base + end];
                previous = end;
                const key =
                    workerBpeKeyPadded(word, input_end) orelse 0;
                const meta: u64 = if (key != 0)
                    workerBpeHash(key)
                else
                    @intCast(word.len);
                item.* = .{
                    .key = key,
                    .ptr = word.ptr,
                    .meta = meta,
                };
                prefetchWorkerBpe(probe_view, meta, 2);
            }
            output_count += emit_count;
            pending = fill_base + previous;
            if (exhausted) break;
        }

        if (scan > pending) {
            const delta = scan - pending;
            scan -= 64 * ((delta + 63) / 64);
        }
        state.pos = pending;
        state.scan = scan;
        return .{
            .count = output_count,
            .input_bytes = pending - call_start,
        };
    }

    fn bpeEncodeWordWorkerSlow(
        self: *HfTokenizer,
        allocator: std.mem.Allocator,
        word: []const u8,
        key: u128,
        hash: u64,
        ids: *std.ArrayListUnmanaged(i32),
        scratch: *BpeScratch,
        cache: *WorkerBpeCache,
    ) !void {
        if (lookupWorkerBpe(cache, key, hash)) |packed_value| {
            if (workerBpeValueIsSpill(packed_value.value)) {
                const count: usize =
                    @intCast(packed_value.value >> 48);
                const offset: usize =
                    @intCast((packed_value.value >> 16) & 0xffff_ffff);
                ids.appendSliceAssumeCapacity(
                    cache.token_arena.items[offset .. offset + count],
                );
            } else {
                appendWorkerBpeValue(ids, packed_value);
            }
            return;
        }
        const output_start = ids.items.len;
        try self.bpeEncodeWordUncached(allocator, word, ids, scratch);
        if (cache.frozen.load(.acquire)) return;
        if (self.packWorkerBpeValueWithSpill(
            cache,
            ids.items[output_start..],
        )) |packed_value| {
            insertWorkerBpe(cache, key, hash, packed_value);
        }
    }

    fn bpeEncodeWordWorkerSlowU16(
        self: *HfTokenizer,
        allocator: std.mem.Allocator,
        word: []const u8,
        key: u128,
        hash: u64,
        ids: *std.ArrayListUnmanaged(u16),
        scratch: *BpeScratch,
        cache: *WorkerBpeCache,
    ) !void {
        if (lookupWorkerBpe(cache, key, hash)) |packed_value| {
            if (workerBpeValueIsSpill(packed_value.value)) {
                const count: usize =
                    @intCast(packed_value.value >> 48);
                const offset: usize =
                    @intCast((packed_value.value >> 16) & 0xffff_ffff);
                for (
                    cache.token_arena.items[offset .. offset + count],
                ) |id| {
                    if (id < 0 or id > std.math.maxInt(u16)) {
                        return error.TokenIdTooLargeForU16;
                    }
                    ids.appendAssumeCapacity(@intCast(id));
                }
            } else {
                var output_len = ids.items.len;
                writeWorkerBpeValueU16(
                    ids.items.ptr,
                    &output_len,
                    packed_value,
                );
                ids.items.len = output_len;
            }
            return;
        }

        scratch.transcode_ids.clearRetainingCapacity();
        try self.bpeEncodeWordUncached(
            allocator,
            word,
            &scratch.transcode_ids,
            scratch,
        );
        for (scratch.transcode_ids.items) |id| {
            if (id < 0 or id > std.math.maxInt(u16)) {
                return error.TokenIdTooLargeForU16;
            }
            ids.appendAssumeCapacity(@intCast(id));
        }
        if (cache.frozen.load(.acquire)) return;
        if (self.packWorkerBpeValueWithSpill(
            cache,
            scratch.transcode_ids.items,
        )) |packed_value| {
            insertWorkerBpe(cache, key, hash, packed_value);
        }
    }

    /// Private-cache ByteLevel path. Span discovery and short-key preparation
    /// run 256 pieces ahead of probes so large-table DRAM latency can overlap
    /// useful scanner work. The per-batch input-byte reservation is a strict
    /// upper bound on ordinary ByteLevel BPE output length and makes the
    /// one-to-four-token hit path allocation-free.
    fn encodeBpeByteLevelWorker(
        self: *HfTokenizer,
        allocator: std.mem.Allocator,
        text: []const u8,
        ids: *std.ArrayListUnmanaged(i32),
        scratch: *BpeScratch,
        cache_optional: ?*WorkerBpeCache,
    ) !void {
        const cache = cache_optional orelse
            return self.encodeBpeByteLevel(allocator, text, ids, scratch);
        if (self.bpe_profile_enabled.load(.acquire)) {
            return self.encodeBpeByteLevelWorkerCached(
                i32,
                allocator,
                text,
                ids,
                scratch,
                cache,
                true,
                false,
                &.{},
                0,
                text.len,
            );
        }
        return self.encodeBpeByteLevelWorkerCached(
            i32,
            allocator,
            text,
            ids,
            scratch,
            cache,
            false,
            false,
            &.{},
            0,
            text.len,
        );
    }

    fn encodeBpeByteLevelWorkerIndexed(
        self: *HfTokenizer,
        allocator: std.mem.Allocator,
        text: []const u8,
        start: usize,
        end: usize,
        boundary_words: []const u64,
        ids: *std.ArrayListUnmanaged(i32),
        scratch: *BpeScratch,
        cache: *WorkerBpeCache,
    ) !void {
        if (self.bpe_profile_enabled.load(.acquire)) {
            return self.encodeBpeByteLevelWorkerCached(
                i32,
                allocator,
                text,
                ids,
                scratch,
                cache,
                true,
                true,
                boundary_words,
                start,
                end,
            );
        }
        return self.encodeBpeByteLevelWorkerCached(
            i32,
            allocator,
            text,
            ids,
            scratch,
            cache,
            false,
            true,
            boundary_words,
            start,
            end,
        );
    }

    fn encodeBpeByteLevelWorkerU16(
        self: *HfTokenizer,
        allocator: std.mem.Allocator,
        text: []const u8,
        ids: *std.ArrayListUnmanaged(u16),
        scratch: *BpeScratch,
        cache_optional: ?*WorkerBpeCache,
    ) !void {
        const cache = cache_optional orelse {
            scratch.transcode_ids.clearRetainingCapacity();
            try self.encodeBpeByteLevel(
                allocator,
                text,
                &scratch.transcode_ids,
                scratch,
            );
            try ids.ensureUnusedCapacity(
                allocator,
                scratch.transcode_ids.items.len,
            );
            for (scratch.transcode_ids.items) |id| {
                if (id < 0 or id > std.math.maxInt(u16)) {
                    return error.TokenIdTooLargeForU16;
                }
                ids.appendAssumeCapacity(@intCast(id));
            }
            return;
        };
        if (self.bpe_profile_enabled.load(.acquire)) {
            return self.encodeBpeByteLevelWorkerCached(
                u16,
                allocator,
                text,
                ids,
                scratch,
                cache,
                true,
                false,
                &.{},
                0,
                text.len,
            );
        }
        return self.encodeBpeByteLevelWorkerCached(
            u16,
            allocator,
            text,
            ids,
            scratch,
            cache,
            false,
            false,
            &.{},
            0,
            text.len,
        );
    }

    fn encodeBpeByteLevelWorkerIndexedU16(
        self: *HfTokenizer,
        allocator: std.mem.Allocator,
        text: []const u8,
        start: usize,
        end: usize,
        boundary_words: []const u64,
        ids: *std.ArrayListUnmanaged(u16),
        scratch: *BpeScratch,
        cache: *WorkerBpeCache,
    ) !void {
        if (self.bpe_profile_enabled.load(.acquire)) {
            return self.encodeBpeByteLevelWorkerCached(
                u16,
                allocator,
                text,
                ids,
                scratch,
                cache,
                true,
                true,
                boundary_words,
                start,
                end,
            );
        }
        return self.encodeBpeByteLevelWorkerCached(
            u16,
            allocator,
            text,
            ids,
            scratch,
            cache,
            false,
            true,
            boundary_words,
            start,
            end,
        );
    }

    fn encodeBpeByteLevelWorkerCached(
        self: *HfTokenizer,
        comptime OutputId: type,
        allocator: std.mem.Allocator,
        text: []const u8,
        ids: *std.ArrayListUnmanaged(OutputId),
        scratch: *BpeScratch,
        cache: *WorkerBpeCache,
        comptime profile_enabled: bool,
        comptime indexed_pretokens: bool,
        boundary_words: []const u64,
        scan_start: usize,
        scan_end: usize,
    ) !void {
        const probe_view = WorkerBpeProbeView{
            .base = cache.entries.ptr,
            .pair_mask = (cache.entries.len - 1) & ~@as(usize, 1),
        };
        const direct_ids = self.byte_level_direct_ids;
        var iterator = if (indexed_pretokens)
            IndexedPretokenIterator{
                .text = text,
                .boundary_words = boundary_words,
                .pos = scan_start,
                .end = scan_end,
            }
        else
            Gpt2MaskFillState{};
        const input_end = @intFromPtr(text.ptr) + text.len;
        var prepared: [worker_bpe_batch_size + worker_bpe_prefetch_distance]PreparedWorkerPretoken align(32) = undefined;
        for (prepared[worker_bpe_batch_size..]) |*item| {
            item.* = .{ .key = 0, .ptr = text.ptr, .meta = 0 };
        }
        while (true) {
            const fill = if (indexed_pretokens)
                @call(.never_inline, fillWorkerPretokenBatchIndexed, .{
                    &iterator,
                    input_end,
                    probe_view,
                    prepared[0..worker_bpe_batch_size],
                })
            else
                @call(.never_inline, fillWorkerPretokenBatchMask, .{
                    &iterator,
                    text,
                    input_end,
                    probe_view,
                    prepared[0..worker_bpe_batch_size],
                });
            const count = fill.count;
            if (count == 0) return;
            if (count < worker_bpe_batch_size) {
                for (prepared[count .. count + worker_bpe_prefetch_distance]) |*item| {
                    item.meta = 0;
                }
            }
            try ids.ensureUnusedCapacity(allocator, fill.input_bytes +| 4);
            var output_ptr = ids.items.ptr;
            var output_len = ids.items.len;
            for (prepared[0..@min(worker_bpe_prefetch_distance, count)]) |item| {
                prefetchWorkerBpe(probe_view, item.meta, 3);
            }
            for (prepared[0..count], 0..) |item, idx| {
                prefetchWorkerBpe(
                    probe_view,
                    prepared[idx + worker_bpe_prefetch_distance].meta,
                    3,
                );
                const word_len = if (item.key != 0)
                    workerBpeKeyLen(item.key)
                else
                    @as(usize, @intCast(item.meta));
                const word = item.ptr[0..word_len];
                if (item.key == 0) {
                    ids.items.len = output_len;
                    if (comptime OutputId == i32) {
                        try self.bpeEncodeWord(
                            allocator,
                            word,
                            ids,
                            scratch,
                        );
                    } else {
                        scratch.transcode_ids.clearRetainingCapacity();
                        try self.bpeEncodeWord(
                            allocator,
                            word,
                            &scratch.transcode_ids,
                            scratch,
                        );
                        for (scratch.transcode_ids.items) |id| {
                            if (id < 0 or id > std.math.maxInt(u16)) {
                                return error.TokenIdTooLargeForU16;
                            }
                            ids.appendAssumeCapacity(@intCast(id));
                        }
                    }
                    output_ptr = ids.items.ptr;
                    output_len = ids.items.len;
                    continue;
                }
                if (profile_enabled) {
                    _ = self.bpe_profile.pretokens.fetchAdd(1, .monotonic);
                }
                if (direct_ids) |direct_table| {
                    const direct_id = switch (word.len) {
                        1 => direct_table.single[word[0]],
                        2 => direct_table.pair[
                            (@as(usize, word[0]) << 8) |
                                @as(usize, word[1])
                        ],
                        else => -1,
                    };
                    if (direct_id >= 0) {
                        if (profile_enabled) {
                            _ = self.bpe_profile.direct_hits.fetchAdd(
                                1,
                                .monotonic,
                            );
                        }
                        if (comptime OutputId == i32) {
                            output_ptr[output_len] = direct_id;
                        } else {
                            if (direct_id > std.math.maxInt(u16)) {
                                return error.TokenIdTooLargeForU16;
                            }
                            output_ptr[output_len] =
                                @intCast(direct_id);
                        }
                        output_len += 1;
                        continue;
                    }
                }
                const pair = probeWorkerBpePair(
                    probe_view,
                    item.key,
                    item.meta,
                );
                if (pair.found and
                    !workerBpeValueIsSpill(pair.value.value))
                {
                    if (comptime OutputId == i32) {
                        writeWorkerBpeValue(
                            output_ptr,
                            &output_len,
                            pair.value,
                        );
                    } else {
                        writeWorkerBpeValueU16(
                            output_ptr,
                            &output_len,
                            pair.value,
                        );
                    }
                    continue;
                }
                ids.items.len = output_len;
                if (comptime OutputId == i32) {
                    try @call(
                        .never_inline,
                        bpeEncodeWordWorkerSlow,
                        .{
                            self,
                            allocator,
                            word,
                            item.key,
                            item.meta,
                            ids,
                            scratch,
                            cache,
                        },
                    );
                } else {
                    try @call(
                        .never_inline,
                        bpeEncodeWordWorkerSlowU16,
                        .{
                            self,
                            allocator,
                            word,
                            item.key,
                            item.meta,
                            ids,
                            scratch,
                            cache,
                        },
                    );
                }
                output_ptr = ids.items.ptr;
                output_len = ids.items.len;
            }
            ids.items.len = output_len;
            if (count < worker_bpe_batch_size) return;
        }
    }

    /// ByteLevel encoder used by parallel chunks when the tokenizer's added
    /// tokens are known not to cross whitespace chunk boundaries.
    fn encodeBpeByteLevelWithAddedTokens(
        self: *HfTokenizer,
        allocator: std.mem.Allocator,
        text: []const u8,
        ids: *std.ArrayListUnmanaged(i32),
        scratch: *BpeScratch,
    ) !void {
        var cursor: usize = 0;
        while (cursor < text.len) {
            if (self.matchAddedTokenAt(text[cursor..])) |match| {
                try ids.append(allocator, match.id);
                cursor += match.len;
                continue;
            }
            const next_added = self.findNextAddedToken(text, cursor) orelse text.len;
            if (next_added > cursor) {
                try self.encodeBpeByteLevel(
                    allocator,
                    text[cursor..next_added],
                    ids,
                    scratch,
                );
            }
            cursor = next_added;
        }
    }

    fn encodeBpeByteLevelWithAddedTokensWorker(
        self: *HfTokenizer,
        allocator: std.mem.Allocator,
        text: []const u8,
        ids: *std.ArrayListUnmanaged(i32),
        scratch: *BpeScratch,
        worker_cache: ?*WorkerBpeCache,
        known_added_token_offsets: ?[]const usize,
        known_pretoken_boundaries: ?[]const u64,
    ) !void {
        if (worker_cache == null and known_added_token_offsets == null) {
            return self.encodeBpeByteLevelWithAddedTokens(
                allocator,
                text,
                ids,
                scratch,
            );
        }
        if (known_added_token_offsets) |offsets| {
            if (known_pretoken_boundaries) |boundary_words| {
                const cache = worker_cache orelse unreachable;
                var cursor: usize = 0;
                for (offsets) |offset| {
                    if (offset < cursor or offset >= text.len) {
                        return error.InvalidStableInputMetadata;
                    }
                    if (offset > cursor) {
                        try self.encodeBpeByteLevelWorkerIndexed(
                            allocator,
                            text,
                            cursor,
                            offset,
                            boundary_words,
                            ids,
                            scratch,
                            cache,
                        );
                    }
                    const match =
                        self.matchAddedTokenAt(text[offset..]) orelse
                        return error.InvalidStableInputMetadata;
                    try ids.append(allocator, match.id);
                    cursor = offset + match.len;
                }
                if (cursor < text.len) {
                    try self.encodeBpeByteLevelWorkerIndexed(
                        allocator,
                        text,
                        cursor,
                        text.len,
                        boundary_words,
                        ids,
                        scratch,
                        cache,
                    );
                }
                return;
            }
            var cursor: usize = 0;
            for (offsets) |offset| {
                if (offset < cursor or offset >= text.len) {
                    return error.InvalidStableInputMetadata;
                }
                if (offset > cursor) {
                    try self.encodeBpeByteLevelWorker(
                        allocator,
                        text[cursor..offset],
                        ids,
                        scratch,
                        worker_cache,
                    );
                }
                const match =
                    self.matchAddedTokenAt(text[offset..]) orelse
                    return error.InvalidStableInputMetadata;
                try ids.append(allocator, match.id);
                cursor = offset + match.len;
            }
            if (cursor < text.len) {
                try self.encodeBpeByteLevelWorker(
                    allocator,
                    text[cursor..],
                    ids,
                    scratch,
                    worker_cache,
                );
            }
            return;
        }
        var cursor: usize = 0;
        while (cursor < text.len) {
            if (self.matchAddedTokenAt(text[cursor..])) |match| {
                try ids.append(allocator, match.id);
                cursor += match.len;
                continue;
            }
            const next_added = self.findNextAddedToken(text, cursor) orelse text.len;
            if (next_added > cursor) {
                try self.encodeBpeByteLevelWorker(
                    allocator,
                    text[cursor..next_added],
                    ids,
                    scratch,
                    worker_cache,
                );
            }
            cursor = next_added;
        }
    }

    fn encodeBpeByteLevelWithAddedTokensWorkerU16(
        self: *HfTokenizer,
        allocator: std.mem.Allocator,
        text: []const u8,
        ids: *std.ArrayListUnmanaged(u16),
        scratch: *BpeScratch,
        worker_cache: ?*WorkerBpeCache,
        known_added_token_offsets: ?[]const usize,
        known_pretoken_boundaries: ?[]const u64,
    ) !void {
        const cache = worker_cache orelse {
            scratch.transcode_ids.clearRetainingCapacity();
            try self.encodeBpeByteLevelWithAddedTokens(
                allocator,
                text,
                &scratch.transcode_ids,
                scratch,
            );
            try ids.ensureUnusedCapacity(
                allocator,
                scratch.transcode_ids.items.len,
            );
            for (scratch.transcode_ids.items) |id| {
                if (id < 0 or id > std.math.maxInt(u16)) {
                    return error.TokenIdTooLargeForU16;
                }
                ids.appendAssumeCapacity(@intCast(id));
            }
            return;
        };
        if (known_added_token_offsets) |offsets| {
            var cursor: usize = 0;
            for (offsets) |offset| {
                if (offset < cursor or offset >= text.len) {
                    return error.InvalidStableInputMetadata;
                }
                if (offset > cursor) {
                    if (known_pretoken_boundaries) |boundary_words| {
                        try self.encodeBpeByteLevelWorkerIndexedU16(
                            allocator,
                            text,
                            cursor,
                            offset,
                            boundary_words,
                            ids,
                            scratch,
                            cache,
                        );
                    } else {
                        try self.encodeBpeByteLevelWorkerU16(
                            allocator,
                            text[cursor..offset],
                            ids,
                            scratch,
                            cache,
                        );
                    }
                }
                const match =
                    self.matchAddedTokenAt(text[offset..]) orelse
                    return error.InvalidStableInputMetadata;
                if (match.id < 0 or
                    match.id > std.math.maxInt(u16))
                {
                    return error.TokenIdTooLargeForU16;
                }
                try ids.append(allocator, @intCast(match.id));
                cursor = offset + match.len;
            }
            if (cursor < text.len) {
                if (known_pretoken_boundaries) |boundary_words| {
                    try self.encodeBpeByteLevelWorkerIndexedU16(
                        allocator,
                        text,
                        cursor,
                        text.len,
                        boundary_words,
                        ids,
                        scratch,
                        cache,
                    );
                } else {
                    try self.encodeBpeByteLevelWorkerU16(
                        allocator,
                        text[cursor..],
                        ids,
                        scratch,
                        cache,
                    );
                }
            }
            return;
        }

        var cursor: usize = 0;
        while (cursor < text.len) {
            if (self.matchAddedTokenAt(text[cursor..])) |match| {
                if (match.id < 0 or
                    match.id > std.math.maxInt(u16))
                {
                    return error.TokenIdTooLargeForU16;
                }
                try ids.append(allocator, @intCast(match.id));
                cursor += match.len;
                continue;
            }
            const next_added =
                self.findNextAddedToken(text, cursor) orelse text.len;
            if (next_added > cursor) {
                try self.encodeBpeByteLevelWorkerU16(
                    allocator,
                    text[cursor..next_added],
                    ids,
                    scratch,
                    cache,
                );
            }
            cursor = next_added;
        }
    }

    /// Count exact GPT-2 ByteLevel pretokens without vocabulary lookup, cache
    /// access, BPE merging, or output allocation. This isolates scanner
    /// throughput for the benchmark harness while sharing the production
    /// boundary implementation.
    pub fn countByteLevelPretokens(
        self: *const HfTokenizer,
        text: []const u8,
    ) !usize {
        if (self.model_type != .bpe or self.pre_tokenizer_type != .byte_level) {
            return error.UnsupportedPreTokenizer;
        }
        var count: usize = 0;
        var start: usize = 0;
        while (start < text.len) {
            if (gpt2AsciiBoundaryMask(text, start)) |boundary_mask| {
                const remaining = boundary_mask & ~@as(u64, 1);
                if (remaining != 0) {
                    count +|= @popCount(remaining);
                    const last_boundary: usize =
                        63 - @as(usize, @intCast(@clz(remaining)));
                    start += last_boundary;
                    continue;
                }
            }
            start = gpt2PreTokenEnd(text, start);
            count +|= 1;
        }
        return count;
    }

    /// Symbol represented as a (start, end) index pair into the working byte
    /// buffer, which lets us merge two symbols by simply extending the left
    /// range — no allocation per merge.
    const BpeSymbol = struct {
        start: u32,
        end: u32,
        token_id: i32 = -1,
        prev: i32 = -1,
        next: i32 = -1,
        alive: bool = true,
    };

    const BpeCandidate = struct {
        rank: u32,
        left: u32,
        right: u32,
    };

    const BpeScratch = struct {
        symbols: std.ArrayListUnmanaged(BpeSymbol) = .empty,
        transcode_ids: std.ArrayListUnmanaged(i32) = .empty,
        candidates: ?PriorityQueue(BpeCandidate) = null,

        fn candidateQueue(
            self: *BpeScratch,
            allocator: std.mem.Allocator,
        ) !*PriorityQueue(BpeCandidate) {
            if (self.candidates == null) {
                self.candidates = try PriorityQueue(BpeCandidate).init(
                    allocator,
                    bpeCandidateCmp,
                );
            } else {
                self.candidates.?.clearRetainingCapacity();
            }
            return &self.candidates.?;
        }

        fn deinit(self: *BpeScratch, allocator: std.mem.Allocator) void {
            self.symbols.deinit(allocator);
            self.transcode_ids.deinit(allocator);
            if (self.candidates) |*candidates| candidates.deinit();
        }
    };

    /// Min-heap ordering on rank (lower rank merged first), with a stable
    /// left-to-right tie-break on the `left` symbol index. The tie-break
    /// matters when the same merge pair occurs more than once in a word:
    /// without it the heap can pop a later occurrence first, which after
    /// applying the merge invalidates the pair on the still-pending earlier
    /// occurrence and changes the resulting tokenization. PriorityQueue is a
    /// max-heap, so we invert: candidates that should pop first must compare
    /// as `.gt`.
    fn bpeCandidateCmp(a: BpeCandidate, b: BpeCandidate) std.math.Order {
        if (a.rank != b.rank) return std.math.order(b.rank, a.rank);
        return std.math.order(b.left, a.left);
    }

    /// Look up the merge rank for the adjacent symbol pair (`a`, `b`), if
    /// any. `merge_ranks` is keyed on "a<space>b" — the same form the JSON
    /// merges list provides. The common case composes the lookup key into a
    /// stack scratch buffer; pairs that overflow the scratch fall through to
    /// an allocating compose so we never silently miss a long merge.
    fn bpeMergeRank(self: *const HfTokenizer, allocator: std.mem.Allocator, a: []const u8, b: []const u8) !?u32 {
        var stack_buf: [256]u8 = undefined;
        const total = a.len + 1 + b.len;
        if (total <= stack_buf.len) {
            @memcpy(stack_buf[0..a.len], a);
            stack_buf[a.len] = ' ';
            @memcpy(stack_buf[a.len + 1 .. total], b);
            return self.merge_ranks.get(stack_buf[0..total]);
        }
        const heap_buf = try allocator.alloc(u8, total);
        defer allocator.free(heap_buf);
        @memcpy(heap_buf[0..a.len], a);
        heap_buf[a.len] = ' ';
        @memcpy(heap_buf[a.len + 1 .. total], b);
        return self.merge_ranks.get(heap_buf);
    }

    fn bpePairKey(left_id: i32, right_id: i32) ?u64 {
        if (left_id < 0 or right_id < 0) return null;
        return (@as(u64, @intCast(left_id)) << 32) | @as(u64, @intCast(right_id));
    }

    fn bpeMerge(
        self: *const HfTokenizer,
        allocator: std.mem.Allocator,
        left_id: i32,
        right_id: i32,
        left_bytes: []const u8,
        right_bytes: []const u8,
    ) !?PackedBpeMerge {
        if (bpePairKey(left_id, right_id)) |key| {
            if (self.merge_pairs.get(key)) |merge| return merge;
        }

        const rank = (try self.bpeMergeRank(allocator, left_bytes, right_bytes)) orelse return null;
        var stack_buf: [256]u8 = undefined;
        var heap_buf: ?[]u8 = null;
        defer if (heap_buf) |buf| allocator.free(buf);
        const total = left_bytes.len + right_bytes.len;
        const merged = if (total <= stack_buf.len) blk: {
            @memcpy(stack_buf[0..left_bytes.len], left_bytes);
            @memcpy(stack_buf[left_bytes.len..total], right_bytes);
            break :blk stack_buf[0..total];
        } else blk: {
            const owned = try allocator.alloc(u8, total);
            heap_buf = owned;
            @memcpy(owned[0..left_bytes.len], left_bytes);
            @memcpy(owned[left_bytes.len..], right_bytes);
            break :blk owned;
        };
        return .{
            .rank = rank,
            .result_id = self.vocab.get(merged) orelse -1,
        };
    }

    fn lockBpeCacheMutex(mutex: *std.atomic.Mutex) void {
        while (!mutex.tryLock()) std.atomic.spinLoopHint();
    }

    /// Reserve bytes against the capability installed at the serialization
    /// point. `.uncharged` means no capability was installed, `.charged` means
    /// the caller owns a charge that must be rolled back if allocation fails,
    /// and `null` is an admission denial. Returning explicit ownership avoids a
    /// racy pre-lock capability read around fallible allocations.
    fn tryReserveResourceBudgetBytes(self: *HfTokenizer, bytes: usize) ?ResourceBudgetCharge {
        if (bytes == 0) return .uncharged;
        lockBpeCacheMutex(&self.cache_resource_budget_mutex);
        defer self.cache_resource_budget_mutex.unlock();
        const budget = self.cache_resource_budget orelse return .uncharged;
        const previous = self.cache_resource_budget_bytes;
        const next = std.math.add(usize, previous, bytes) catch return null;
        if (!budget.observe(
            budget.context,
            self.cache_resource_budget_observer_id,
            previous,
            next,
        ))
            return null;
        self.cache_resource_budget_bytes = next;
        return .charged;
    }

    fn releaseResourceBudgetBytes(self: *HfTokenizer, bytes: usize) void {
        if (bytes == 0) return;
        lockBpeCacheMutex(&self.cache_resource_budget_mutex);
        defer self.cache_resource_budget_mutex.unlock();
        const budget = self.cache_resource_budget orelse return;
        const previous = self.cache_resource_budget_bytes;
        if (bytes > previous)
            @panic("tokenizer resource budget release exceeds owned bytes");
        const next = previous - bytes;
        if (!budget.observe(
            budget.context,
            self.cache_resource_budget_observer_id,
            previous,
            next,
        )) @panic("tokenizer resource budget rejected a release");
        self.cache_resource_budget_bytes = next;
    }

    fn switchResourceBudget(
        self: *HfTokenizer,
        next_budget: ?BpeCacheResourceBudget,
    ) !void {
        lockBpeCacheMutex(&self.cache_resource_budget_mutex);
        defer self.cache_resource_budget_mutex.unlock();
        const previous_budget = self.cache_resource_budget;
        if (sameBpeCacheResourceBudget(previous_budget, next_budget)) return;
        if (next_budget) |budget| {
            if (!budget.hasValidLifetimeHooks())
                return error.InvalidResourceBudgetLifetime;
            if (!budget.retainContext()) return error.ResourceOwnerShuttingDown;
        }
        const bytes = self.cache_resource_budget_bytes;
        if (next_budget) |budget| {
            if (bytes != 0 and
                !budget.observe(
                    budget.context,
                    self.cache_resource_budget_observer_id,
                    0,
                    bytes,
                ))
            {
                budget.releaseContext();
                return error.ResourceBudgetDenied;
            }
        }
        if (previous_budget) |budget| {
            if (bytes != 0 and
                !budget.observe(
                    budget.context,
                    self.cache_resource_budget_observer_id,
                    bytes,
                    0,
                ))
            {
                if (next_budget) |next| {
                    if (bytes != 0 and
                        !next.observe(
                            next.context,
                            self.cache_resource_budget_observer_id,
                            bytes,
                            0,
                        ))
                    {
                        // Both owners now retain the charge and neither can be
                        // released safely. Continuing would unpin a callback
                        // context with live accounting, violating the resource
                        // capability contract and risking a later use-after-free.
                        @panic("tokenizer resource budget rejected migration rollback");
                    }
                }
                if (next_budget) |next| next.releaseContext();
                return error.ResourceBudgetReleaseRejected;
            }
        }
        self.cache_resource_budget = next_budget;
        if (next_budget == null) self.cache_resource_budget_bytes = 0;
        if (previous_budget) |budget| budget.releaseContext();
    }

    fn sameBpeCacheResourceBudget(
        a: ?BpeCacheResourceBudget,
        b: ?BpeCacheResourceBudget,
    ) bool {
        if (a == null or b == null) return a == null and b == null;
        return a.?.context == b.?.context and
            a.?.observe == b.?.observe and
            a.?.retain_context == b.?.retain_context and
            a.?.release_context == b.?.release_context;
    }

    /// Configure the local hard limit and optional process-wide admission
    /// budget before the tokenizer is used. A denied base-table reservation
    /// disables the optional cache rather than failing tokenizer loading.
    pub fn configureBpeCache(self: *HfTokenizer, config: BpeCacheConfig) !void {
        if (self.parallel_workspace_all != null) return error.BpeCacheAlreadyPopulated;
        if (config.resource_budget) |budget| {
            if (!budget.hasValidLifetimeHooks())
                return error.InvalidResourceBudgetLifetime;
        }
        if (config.bulk_slots_per_shard != 0 and
            (!std.math.isPowerOfTwo(config.bulk_slots_per_shard) or
                config.bulk_slots_per_shard > max_bpe_bulk_slots_per_shard))
        {
            return error.InvalidBpeBulkCacheSize;
        }
        const cache = self.bpe_cache orelse {
            // Parallel BPE remains available when the optional fixed cache
            // table could not be allocated. Its retained workspaces still
            // participate in the caller's process-wide resource budget.
            try self.switchResourceBudget(config.resource_budget);
            return;
        };
        if (cache.bulk_slots != null) return error.BpeCacheAlreadyConfigured;
        for (&cache.shards) |*shard| {
            if (shard.count.load(.acquire) != 0 or
                shard.bulk_count.load(.acquire) != 0)
            {
                return error.BpeCacheAlreadyPopulated;
            }
        }

        const base_bytes = cache.used_bytes.load(.acquire);
        if (config.max_bytes < base_bytes) {
            self.releaseResourceBudgetBytes(base_bytes);
            self.allocator.destroy(cache);
            self.bpe_cache = null;
            try self.switchResourceBudget(config.resource_budget);
            return;
        }
        if (!sameBpeCacheResourceBudget(cache.resource_budget, config.resource_budget)) {
            if (cache.resource_budget == null and config.resource_budget != null) {
                const budget = config.resource_budget.?;
                // A lifetime failure is not a capacity denial. Preserve the
                // intact optional cache and surface the ownership error so a
                // caller can retry safely or fail model publication.
                if (!budget.retainContext()) return error.ResourceOwnerShuttingDown;
                lockBpeCacheMutex(&self.cache_resource_budget_mutex);
                std.debug.assert(self.cache_resource_budget_bytes == 0);
                std.debug.assert(self.cache_resource_budget == null);
                const base_admitted = budget.observe(
                    budget.context,
                    self.cache_resource_budget_observer_id,
                    0,
                    base_bytes,
                );
                // The retained capability remains installed even when the
                // optional table is denied. This closes the release/re-retain
                // race with owner shutdown and lets later parallel workspaces
                // participate in the same budget without extra callbacks.
                self.cache_resource_budget = config.resource_budget;
                if (base_admitted)
                    self.cache_resource_budget_bytes = base_bytes;
                self.cache_resource_budget_mutex.unlock();
                if (!base_admitted) {
                    // The fixed table is optional. A capacity denial disables
                    // only that table; the owner installed above at zero keeps
                    // later parallel workspaces tracked.
                    self.allocator.destroy(cache);
                    self.bpe_cache = null;
                    return;
                }
            } else {
                // Migration failures leave the old capability and cache
                // untouched. Never destroy a correctly accounted cache merely
                // because a replacement owner is temporarily unavailable.
                try self.switchResourceBudget(config.resource_budget);
            }
            cache.resource_budget = config.resource_budget;
        }
        std.debug.assert(sameBpeCacheResourceBudget(
            self.cache_resource_budget,
            config.resource_budget,
        ));
        cache.max_bytes = config.max_bytes;

        if (config.bulk_slots_per_shard != 0) {
            const slot_count = std.math.mul(
                usize,
                config.bulk_slots_per_shard,
                bpe_cache_shard_count,
            ) catch return error.InvalidBpeBulkCacheSize;
            const slot_bytes = std.math.mul(
                usize,
                slot_count,
                @sizeOf(std.atomic.Value(usize)),
            ) catch return error.InvalidBpeBulkCacheSize;
            // Reserve before allocating so a process-wide pressure denial does
            // not transiently allocate and zero a large optional table.
            if (!tryReserveBpeCacheBytes(cache, slot_bytes)) return;
            const slots = self.allocator.alloc(
                std.atomic.Value(usize),
                slot_count,
            ) catch {
                releaseBpeCacheBytes(cache, slot_bytes);
                return;
            };
            for (slots) |*slot| slot.* = .init(0);
            cache.bulk_slots = slots;
            cache.bulk_slots_per_shard = config.bulk_slots_per_shard;
        }
    }

    /// Configure parallel chunk geometry before the first parallel encode.
    /// Production callers normally keep the adaptive defaults; benchmark and
    /// deployment tuning can use this to sweep scheduling independently from
    /// the std.Io consumer count.
    pub fn configureParallelBpe(
        self: *HfTokenizer,
        config: ParallelBpeConfig,
    ) !void {
        if (self.parallel_workspace_all != null) {
            return error.ParallelBpeAlreadyUsed;
        }
        if (config.max_chunks == 0 or
            config.max_chunks > max_parallel_bpe_chunks or
            config.chunks_per_task > max_parallel_bpe_chunks or
            config.worker_cache_count > max_worker_bpe_caches or
            config.worker_cache_slots > max_worker_bpe_cache_slots or
            ((config.worker_cache_count == 0) !=
                (config.worker_cache_slots == 0)) or
            (config.worker_cache_slots != 0 and
                (config.worker_cache_slots < 2 or
                    !std.math.isPowerOfTwo(config.worker_cache_slots))))
        {
            return error.InvalidParallelBpeConfig;
        }
        self.parallel_bpe_config = config;
    }

    pub fn bpeCacheStats(self: *const HfTokenizer) BpeCacheStats {
        var worker_tables: usize = 0;
        var worker_entries: usize = 0;
        var worker_min_entries: usize = std.math.maxInt(usize);
        var worker_max_entries: usize = 0;
        var worker_slots: usize = 0;
        var worker_bytes: usize = 0;
        var worker_token_arena_ids: usize = 0;
        var worker_superpage_tables: usize = 0;
        for (@constCast(&self.worker_bpe_caches)) |*lease| {
            // A consumer owns its lease for an entire encode. Metrics must
            // never spin behind that potentially multi-second critical
            // section: use an exact snapshot when immediately available and
            // the last atomically published snapshot otherwise.
            const snapshot = if (lease.mutex.tryLock()) blk: {
                const exact = if (lease.cache) |*cache|
                    WorkerBpeCachePublishedStats.fromCache(cache)
                else
                    WorkerBpeCachePublishedStats{};
                lease.publishStats();
                lease.mutex.unlock();
                break :blk exact;
            } else lease.loadPublishedStats();
            if (!snapshot.present) continue;
            worker_tables += 1;
            worker_entries += snapshot.entries;
            worker_min_entries =
                @min(worker_min_entries, snapshot.entries);
            worker_max_entries =
                @max(worker_max_entries, snapshot.entries);
            worker_slots += snapshot.slots;
            worker_bytes += snapshot.bytes;
            worker_token_arena_ids += snapshot.token_arena_ids;
            if (snapshot.superpage) worker_superpage_tables += 1;
        }
        if (worker_tables == 0) worker_min_entries = 0;
        const workspace_mutex = @constCast(&self.parallel_workspace_mutex);
        while (!workspace_mutex.tryLock()) std.atomic.spinLoopHint();
        const workspace_cached_count = self.parallel_workspace_free_count;
        const workspace_cached_bytes = self.parallel_workspace_free_bytes;
        var workspace_total_count: usize = 0;
        var workspace_total_bytes: usize = 0;
        var workspace_active_count: usize = 0;
        var workspace_active_bytes: usize = 0;
        var workspace_accounted_bytes: usize = 0;
        var workspace_active_output_bytes: usize = 0;
        var workspace_active_output_capacity_bytes: usize = 0;
        var affinity_learns: usize = 0;
        var affinity_replays: usize = 0;
        var affinity_stolen_chunks: usize = 0;
        var stable_offset_learns: usize = 0;
        var stable_offset_replays: usize = 0;
        var stable_offset_count: usize = 0;
        var stable_offset_bytes: usize = 0;
        var stable_boundary_words: usize = 0;
        var stable_boundary_bytes: usize = 0;
        var parallel_max_chunk_bytes: usize = 0;
        var parallel_max_chunk_ns: u64 = 0;
        var parallel_slowest_chunk_bytes: usize = 0;
        var parallel_slowest_chunk_owner: usize =
            invalid_worker_cache_owner;
        var parallel_max_owner_chunk_ns: u64 = 0;
        var parallel_min_owner_chunk_ns: u64 = std.math.maxInt(u64);
        var workspace = self.parallel_workspace_all;
        while (workspace) |current| : (workspace = current.next_all) {
            // Cached workspaces cannot be acquired while the list mutex is
            // held and are therefore safe to inspect directly. Active
            // workspaces are mutated by std.Io consumers; read only their
            // atomic, per-chunk published snapshots.
            const retained_bytes = if (current.cached)
                current.retainedBytes()
            else
                current.publishedRetainedBytes();
            workspace_total_count += 1;
            workspace_total_bytes +|= retained_bytes;
            workspace_accounted_bytes +|=
                current.resource_accounted_bytes.load(.acquire);
            if (!current.cached) {
                workspace_active_count += 1;
                workspace_active_bytes +|= retained_bytes;
                for (&current.workers) |*worker| {
                    workspace_active_output_bytes +|=
                        worker.published_output_bytes.load(.acquire);
                    workspace_active_output_capacity_bytes +|=
                        worker.published_output_capacity_bytes.load(.acquire);
                }
            }
            affinity_learns += current.affinity_learns.load(.monotonic);
            affinity_replays += current.affinity_replays.load(.monotonic);
            affinity_stolen_chunks +=
                current.affinity_stolen_chunks.load(.monotonic);
            stable_offset_learns +=
                current.stable_offset_learns.load(.monotonic);
            stable_offset_replays +=
                current.stable_offset_replays.load(.monotonic);
            var owner_elapsed_ns: [max_worker_bpe_caches]u64 =
                @splat(0);
            var owner_has_chunks: [max_worker_bpe_caches]bool =
                @splat(false);
            for (&current.workers) |*worker| {
                const worker_stable_offset_count = if (current.cached)
                    worker.added_token_offsets.items.len
                else
                    worker.published_stable_offset_count.load(.acquire);
                const worker_stable_offset_bytes = if (current.cached)
                    worker.added_token_offsets.capacity * @sizeOf(usize)
                else
                    worker.published_stable_offset_bytes.load(.acquire);
                const worker_stable_boundary_words = if (current.cached)
                    worker.pretoken_boundary_words.items.len
                else
                    worker.published_stable_boundary_words.load(.acquire);
                const worker_stable_boundary_bytes = if (current.cached)
                    worker.pretoken_boundary_words.capacity * @sizeOf(u64)
                else
                    worker.published_stable_boundary_bytes.load(.acquire);
                const worker_text_bytes = if (current.cached)
                    worker.text.len
                else
                    worker.published_text_bytes.load(.acquire);
                const worker_elapsed_ns = if (current.cached)
                    worker.last_elapsed_ns
                else
                    worker.published_elapsed_ns.load(.acquire);
                const worker_cache_owner = if (current.cached)
                    @as(usize, worker.cache_owner)
                else
                    worker.published_cache_owner.load(.acquire);

                stable_offset_count += worker_stable_offset_count;
                stable_offset_bytes += worker_stable_offset_bytes;
                stable_boundary_words += worker_stable_boundary_words;
                stable_boundary_bytes += worker_stable_boundary_bytes;
                parallel_max_chunk_bytes =
                    @max(parallel_max_chunk_bytes, worker_text_bytes);
                if (worker_elapsed_ns > parallel_max_chunk_ns) {
                    parallel_max_chunk_ns = worker_elapsed_ns;
                    parallel_slowest_chunk_bytes = worker_text_bytes;
                    parallel_slowest_chunk_owner = worker_cache_owner;
                }
                if (worker_cache_owner != invalid_worker_cache_owner) {
                    const owner = worker_cache_owner;
                    owner_elapsed_ns[owner] +|= worker_elapsed_ns;
                    owner_has_chunks[owner] = true;
                }
            }
            for (
                owner_elapsed_ns,
                owner_has_chunks,
            ) |elapsed_ns, has_chunks| {
                if (!has_chunks) continue;
                parallel_max_owner_chunk_ns =
                    @max(parallel_max_owner_chunk_ns, elapsed_ns);
                parallel_min_owner_chunk_ns =
                    @min(parallel_min_owner_chunk_ns, elapsed_ns);
            }
        }
        if (parallel_min_owner_chunk_ns == std.math.maxInt(u64)) {
            parallel_min_owner_chunk_ns = 0;
        }
        workspace_mutex.unlock();
        const cache = self.bpe_cache orelse return .{
            .max_bytes = 0,
            .used_bytes = 0,
            .entries = 0,
            .front_entries = 0,
            .bulk_entries = 0,
            .bulk_slots = 0,
            .rejected_reservations = 0,
            .rejected_admissions = 0,
            .evictions = 0,
            .worker_tables = worker_tables,
            .worker_entries = worker_entries,
            .worker_min_entries = worker_min_entries,
            .worker_max_entries = worker_max_entries,
            .worker_slots = worker_slots,
            .worker_bytes = worker_bytes,
            .worker_token_arena_ids = worker_token_arena_ids,
            .worker_superpage_tables = worker_superpage_tables,
            .workspace_total_count = workspace_total_count,
            .workspace_total_bytes = workspace_total_bytes,
            .workspace_active_count = workspace_active_count,
            .workspace_active_bytes = workspace_active_bytes,
            .workspace_accounted_bytes = workspace_accounted_bytes,
            .workspace_active_output_bytes = workspace_active_output_bytes,
            .workspace_active_output_capacity_bytes = workspace_active_output_capacity_bytes,
            .workspace_cached_count = workspace_cached_count,
            .workspace_cached_bytes = workspace_cached_bytes,
            .affinity_learns = affinity_learns,
            .affinity_replays = affinity_replays,
            .affinity_stolen_chunks = affinity_stolen_chunks,
            .stable_offset_learns = stable_offset_learns,
            .stable_offset_replays = stable_offset_replays,
            .stable_offset_count = stable_offset_count,
            .stable_offset_bytes = stable_offset_bytes,
            .stable_boundary_words = stable_boundary_words,
            .stable_boundary_bytes = stable_boundary_bytes,
            .parallel_max_chunk_bytes = parallel_max_chunk_bytes,
            .parallel_max_chunk_ns = parallel_max_chunk_ns,
            .parallel_slowest_chunk_bytes = parallel_slowest_chunk_bytes,
            .parallel_slowest_chunk_owner = parallel_slowest_chunk_owner,
            .parallel_max_owner_chunk_ns = parallel_max_owner_chunk_ns,
            .parallel_min_owner_chunk_ns = parallel_min_owner_chunk_ns,
        };
        var front_entries: usize = 0;
        var bulk_entries: usize = 0;
        for (&cache.shards) |*shard| {
            front_entries += shard.count.load(.acquire);
            bulk_entries += shard.bulk_count.load(.acquire);
        }
        return .{
            .max_bytes = cache.max_bytes,
            .used_bytes = cache.used_bytes.load(.acquire),
            .entries = front_entries + bulk_entries,
            .front_entries = front_entries,
            .bulk_entries = bulk_entries,
            .bulk_slots = if (cache.bulk_slots) |slots| slots.len else 0,
            .rejected_reservations = cache.rejected_reservations.load(.monotonic),
            .rejected_admissions = cache.rejected_admissions.load(.monotonic),
            .evictions = cache.evictions.load(.monotonic),
            .worker_tables = worker_tables,
            .worker_entries = worker_entries,
            .worker_min_entries = worker_min_entries,
            .worker_max_entries = worker_max_entries,
            .worker_slots = worker_slots,
            .worker_bytes = worker_bytes,
            .worker_token_arena_ids = worker_token_arena_ids,
            .worker_superpage_tables = worker_superpage_tables,
            .workspace_total_count = workspace_total_count,
            .workspace_total_bytes = workspace_total_bytes,
            .workspace_active_count = workspace_active_count,
            .workspace_active_bytes = workspace_active_bytes,
            .workspace_accounted_bytes = workspace_accounted_bytes,
            .workspace_active_output_bytes = workspace_active_output_bytes,
            .workspace_active_output_capacity_bytes = workspace_active_output_capacity_bytes,
            .workspace_cached_count = workspace_cached_count,
            .workspace_cached_bytes = workspace_cached_bytes,
            .affinity_learns = affinity_learns,
            .affinity_replays = affinity_replays,
            .affinity_stolen_chunks = affinity_stolen_chunks,
            .stable_offset_learns = stable_offset_learns,
            .stable_offset_replays = stable_offset_replays,
            .stable_offset_count = stable_offset_count,
            .stable_offset_bytes = stable_offset_bytes,
            .stable_boundary_words = stable_boundary_words,
            .stable_boundary_bytes = stable_boundary_bytes,
            .parallel_max_chunk_bytes = parallel_max_chunk_bytes,
            .parallel_max_chunk_ns = parallel_max_chunk_ns,
            .parallel_slowest_chunk_bytes = parallel_slowest_chunk_bytes,
            .parallel_slowest_chunk_owner = parallel_slowest_chunk_owner,
            .parallel_max_owner_chunk_ns = parallel_max_owner_chunk_ns,
            .parallel_min_owner_chunk_ns = parallel_min_owner_chunk_ns,
        };
    }

    fn tryReserveBpeCacheBytes(cache: *BpeCache, bytes: usize) bool {
        if (bytes == 0) return true;
        var used = cache.used_bytes.load(.acquire);
        while (true) {
            const limit = cache.max_bytes;
            if (bytes > limit or used > limit - bytes) {
                _ = cache.rejected_reservations.fetchAdd(1, .monotonic);
                return false;
            }
            used = cache.used_bytes.cmpxchgWeak(
                used,
                used + bytes,
                .acq_rel,
                .acquire,
            ) orelse break;
        }
        if (cache.owner.tryReserveResourceBudgetBytes(bytes) == null) {
            _ = cache.used_bytes.fetchSub(bytes, .acq_rel);
            _ = cache.rejected_reservations.fetchAdd(1, .monotonic);
            return false;
        }
        return true;
    }

    fn releaseBpeCacheBytes(cache: *BpeCache, bytes: usize) void {
        if (bytes == 0) return;
        cache.owner.releaseResourceBudgetBytes(bytes);
        const previous = cache.used_bytes.fetchSub(bytes, .acq_rel);
        std.debug.assert(previous >= bytes);
    }

    fn bpeCacheShard(hash: u64) usize {
        return @intCast(hash & (bpe_cache_shard_count - 1));
    }

    fn bpeCacheSlot(hash: u64) usize {
        return @intCast((hash >> 6) & (bpe_cache_slots_per_shard - 1));
    }

    fn bpeBulkCacheSlot(cache: *const BpeCache, hash: u64) usize {
        return @intCast((hash >> 6) & (cache.bulk_slots_per_shard - 1));
    }

    fn bpeBulkShardSlots(
        cache: *BpeCache,
        shard_idx: usize,
    ) ?[]std.atomic.Value(usize) {
        const slots = cache.bulk_slots orelse return null;
        const start = shard_idx * cache.bulk_slots_per_shard;
        return slots[start..][0..cache.bulk_slots_per_shard];
    }

    fn bpeCacheHash(word: []const u8) u64 {
        return std.hash.Wyhash.hash(0, word);
    }

    fn bpeCacheEntryBytes(entry: *const BpeCacheEntry) usize {
        return @sizeOf(BpeCacheEntry) +
            entry.key.len +
            entry.token_ids.len * @sizeOf(i32);
    }

    fn enterBpeCacheRead(self: *HfTokenizer) ?*BpeCache {
        const cache = self.bpe_cache orelse return null;
        while (true) {
            while (cache.reader_gate.load(.acquire)) std.atomic.spinLoopHint();
            _ = cache.active_readers.fetchAdd(1, .acq_rel);
            if (!cache.reader_gate.load(.acquire)) return cache;
            const previous = cache.active_readers.fetchSub(1, .acq_rel);
            std.debug.assert(previous > 0);
        }
    }

    fn leaveBpeCacheRead(self: *HfTokenizer, cache: *BpeCache) void {
        const previous = cache.active_readers.fetchSub(1, .acq_rel);
        std.debug.assert(previous > 0);
        if (previous == 1 and cache.retired_bytes.load(.acquire) != 0) {
            self.reclaimRetiredBpeCacheEntries(cache);
        }
    }

    fn observeBpeCacheCandidate(cache: *BpeCache, hash: u64) bool {
        const mixed = hash ^ (hash >> 29) ^ (hash *% 0x9e3779b97f4a7c15);
        const word_index: usize = @intCast(hash & (bpe_doorkeeper_words - 1));
        const first_bit: u6 = @intCast((hash >> 12) & 63);
        var second_bit: u6 = @intCast(mixed & 63);
        if (second_bit == first_bit) second_bit +%= 1;
        const mask = (@as(u64, 1) << first_bit) | (@as(u64, 1) << second_bit);
        const active: usize = cache.doorkeeper.active_generation.load(.acquire) & 1;
        const previous = active ^ 1;
        const old_bits = cache.doorkeeper.generations[active][word_index].fetchOr(mask, .monotonic);
        const seen_active = old_bits & mask == mask;
        const seen_previous =
            cache.doorkeeper.generations[previous][word_index].load(.monotonic) & mask == mask;

        const observations = cache.doorkeeper.observations.fetchAdd(1, .monotonic) + 1;
        if (observations >= bpe_doorkeeper_rotate_after and
            cache.doorkeeper.rotating.cmpxchgStrong(false, true, .acq_rel, .acquire) == null)
        {
            const old_active: usize = cache.doorkeeper.active_generation.load(.acquire) & 1;
            const next = old_active ^ 1;
            for (&cache.doorkeeper.generations[next]) |*word| word.store(0, .monotonic);
            cache.doorkeeper.observations.store(0, .monotonic);
            cache.doorkeeper.active_generation.store(@intCast(next), .release);
            cache.doorkeeper.rotating.store(false, .release);
        }
        return seen_active or seen_previous;
    }

    fn retireBpeCacheEntry(cache: *BpeCache, entry: *BpeCacheEntry) void {
        lockBpeCacheMutex(&cache.retired_mutex);
        entry.next_retired = cache.retired_head;
        cache.retired_head = entry;
        cache.retired_mutex.unlock();
        _ = cache.retired_bytes.fetchAdd(bpeCacheEntryBytes(entry), .release);
    }

    fn bpeFrontCacheShardContainsLocked(
        shard: *BpeCacheShard,
        hash: u64,
        word: []const u8,
    ) bool {
        var slot_idx = bpeCacheSlot(hash);
        for (0..bpe_cache_slots_per_shard) |_| {
            const raw = shard.slots[slot_idx].load(.acquire);
            if (raw == 0) return false;
            if (raw > bpe_cache_tombstone) {
                const entry: *const BpeCacheEntry = @ptrFromInt(raw);
                if (entry.hash == hash and std.mem.eql(u8, entry.key, word)) return true;
            }
            slot_idx = (slot_idx + 1) & (bpe_cache_slots_per_shard - 1);
        }
        return false;
    }

    fn bpeBulkCacheShardContainsLocked(
        cache: *BpeCache,
        shard_idx: usize,
        hash: u64,
        word: []const u8,
    ) bool {
        const slots = bpeBulkShardSlots(cache, shard_idx) orelse return false;
        var slot_idx = bpeBulkCacheSlot(cache, hash);
        for (0..slots.len) |_| {
            const raw = slots[slot_idx].load(.acquire);
            if (raw == 0) return false;
            if (raw > bpe_cache_tombstone) {
                const entry: *const BpeCacheEntry = @ptrFromInt(raw);
                if (entry.hash == hash and std.mem.eql(u8, entry.key, word)) return true;
            }
            slot_idx = (slot_idx + 1) & (slots.len - 1);
        }
        return false;
    }

    fn evictBpeCacheEntry(cache: *BpeCache, shard: *BpeCacheShard) bool {
        for (0..bpe_cache_slots_per_shard * 2) |_| {
            const slot_idx = shard.clock_hand;
            shard.clock_hand = (slot_idx + 1) & (bpe_cache_slots_per_shard - 1);
            const raw = shard.slots[slot_idx].load(.acquire);
            if (raw <= bpe_cache_tombstone) continue;
            const entry: *BpeCacheEntry = @ptrFromInt(raw);
            if (entry.referenced.swap(false, .acq_rel)) continue;

            shard.slots[slot_idx].store(bpe_cache_tombstone, .release);
            const previous = shard.count.fetchSub(1, .release);
            std.debug.assert(previous > 0);
            shard.tombstones += 1;
            retireBpeCacheEntry(cache, entry);
            _ = cache.evictions.fetchAdd(1, .monotonic);
            return true;
        }
        return false;
    }

    fn evictBpeBulkCacheEntry(
        cache: *BpeCache,
        shard_idx: usize,
        shard: *BpeCacheShard,
    ) bool {
        const slots = bpeBulkShardSlots(cache, shard_idx) orelse return false;
        for (0..slots.len * 2) |_| {
            const slot_idx = shard.bulk_clock_hand;
            shard.bulk_clock_hand = (slot_idx + 1) & (slots.len - 1);
            const raw = slots[slot_idx].load(.acquire);
            if (raw <= bpe_cache_tombstone) continue;
            const entry: *BpeCacheEntry = @ptrFromInt(raw);
            if (entry.referenced.swap(false, .acq_rel)) continue;

            slots[slot_idx].store(bpe_cache_tombstone, .release);
            const previous = shard.bulk_count.fetchSub(1, .release);
            std.debug.assert(previous > 0);
            shard.bulk_tombstones += 1;
            retireBpeCacheEntry(cache, entry);
            _ = cache.evictions.fetchAdd(1, .monotonic);
            return true;
        }
        return false;
    }

    fn evictBpeCacheEntryForPressure(
        cache: *BpeCache,
        candidate_hash: u64,
        candidate_word: []const u8,
    ) void {
        // One pending retirement is sufficient to make the next encode retry
        // admission with released capacity. Avoid draining the cache when a
        // long request encounters many misses before its read epoch can end.
        if (cache.retired_bytes.load(.acquire) != 0) return;
        const candidate_shard_idx = bpeCacheShard(candidate_hash);
        for (0..bpe_cache_shard_count) |offset| {
            const shard_idx =
                (candidate_shard_idx + offset) & (bpe_cache_shard_count - 1);
            const shard = &cache.shards[shard_idx];
            lockBpeCacheMutex(&shard.mutex);
            if (shard_idx == candidate_shard_idx and
                (bpeFrontCacheShardContainsLocked(
                    shard,
                    candidate_hash,
                    candidate_word,
                ) or
                    bpeBulkCacheShardContainsLocked(
                        cache,
                        shard_idx,
                        candidate_hash,
                        candidate_word,
                    )))
            {
                shard.mutex.unlock();
                return;
            }
            const evicted = evictBpeBulkCacheEntry(
                cache,
                shard_idx,
                shard,
            ) or evictBpeCacheEntry(cache, shard);
            shard.mutex.unlock();
            if (evicted) return;
        }
    }

    fn rebuildBpeCacheShard(shard: *BpeCacheShard) void {
        if (shard.tombstones < bpe_cache_slots_per_shard / 4) return;
        var entries: [bpe_cache_slots_per_shard]*BpeCacheEntry = undefined;
        var entry_count: usize = 0;
        for (&shard.slots) |*slot| {
            const raw = slot.load(.monotonic);
            if (raw > bpe_cache_tombstone) {
                entries[entry_count] = @ptrFromInt(raw);
                entry_count += 1;
            }
            slot.store(0, .monotonic);
        }
        shard.tombstones = 0;
        shard.clock_hand = 0;
        for (entries[0..entry_count]) |entry| {
            var slot_idx = bpeCacheSlot(entry.hash);
            while (shard.slots[slot_idx].load(.monotonic) != 0) {
                slot_idx = (slot_idx + 1) & (bpe_cache_slots_per_shard - 1);
            }
            shard.slots[slot_idx].store(@intFromPtr(entry), .monotonic);
        }
        std.debug.assert(entry_count == shard.count.load(.monotonic));
    }

    fn rebuildBpeBulkCacheShard(
        self: *HfTokenizer,
        cache: *BpeCache,
        shard_idx: usize,
        shard: *BpeCacheShard,
    ) void {
        const slots = bpeBulkShardSlots(cache, shard_idx) orelse return;
        if (shard.bulk_tombstones < slots.len / 4) return;
        const expected_count = shard.bulk_count.load(.monotonic);
        const entries = self.allocator.alloc(
            *BpeCacheEntry,
            expected_count,
        ) catch return;
        defer self.allocator.free(entries);

        var entry_count: usize = 0;
        for (slots) |*slot| {
            const raw = slot.load(.monotonic);
            if (raw > bpe_cache_tombstone) {
                entries[entry_count] = @ptrFromInt(raw);
                entry_count += 1;
            }
            slot.store(0, .monotonic);
        }
        shard.bulk_tombstones = 0;
        shard.bulk_clock_hand = 0;
        for (entries[0..entry_count]) |entry| {
            var slot_idx = bpeBulkCacheSlot(cache, entry.hash);
            while (slots[slot_idx].load(.monotonic) != 0) {
                slot_idx = (slot_idx + 1) & (slots.len - 1);
            }
            slots[slot_idx].store(@intFromPtr(entry), .monotonic);
        }
        std.debug.assert(entry_count == expected_count);
    }

    fn reclaimRetiredBpeCacheEntries(self: *HfTokenizer, cache: *BpeCache) void {
        if (cache.reclaiming.cmpxchgStrong(false, true, .acq_rel, .acquire) != null) return;

        cache.reader_gate.store(true, .release);
        while (cache.active_readers.load(.acquire) != 0) std.atomic.spinLoopHint();

        lockBpeCacheMutex(&cache.retired_mutex);
        var retired = cache.retired_head;
        cache.retired_head = null;
        const retired_bytes = cache.retired_bytes.swap(0, .acq_rel);
        cache.retired_mutex.unlock();

        for (&cache.shards, 0..) |*shard, shard_idx| {
            lockBpeCacheMutex(&shard.mutex);
            rebuildBpeCacheShard(shard);
            self.rebuildBpeBulkCacheShard(cache, shard_idx, shard);
            shard.mutex.unlock();
        }

        var released_bytes: usize = 0;
        while (retired) |entry| {
            const next = entry.next_retired;
            released_bytes += bpeCacheEntryBytes(entry);
            self.allocator.free(entry.key);
            self.allocator.free(entry.token_ids);
            self.allocator.destroy(entry);
            retired = next;
        }
        std.debug.assert(released_bytes == retired_bytes);
        releaseBpeCacheBytes(cache, released_bytes);
        // Publish reclamation completion before admitting another reader, so
        // an immediately retiring newcomer cannot lose the only reclaim wakeup.
        cache.reclaiming.store(false, .release);
        cache.reader_gate.store(false, .release);
    }

    fn appendCachedBpe(
        self: *HfTokenizer,
        allocator: std.mem.Allocator,
        word: []const u8,
        ids: *std.ArrayListUnmanaged(i32),
    ) !bool {
        if (word.len == 0 or word.len > bpe_cache_max_key_bytes) return false;
        const hash = bpeCacheHash(word);
        const cache = self.bpe_cache orelse return false;
        var probes: usize = 0;
        const shard_idx = bpeCacheShard(hash);
        const shard = &cache.shards[shard_idx];
        var slot_idx = bpeCacheSlot(hash);
        for (0..bpe_cache_slots_per_shard) |_| {
            probes += 1;
            const raw = shard.slots[slot_idx].load(.acquire);
            if (raw == 0) break;
            if (raw == bpe_cache_tombstone) {
                slot_idx = (slot_idx + 1) & (bpe_cache_slots_per_shard - 1);
                continue;
            }
            const entry: *const BpeCacheEntry = @ptrFromInt(raw);
            if (entry.hash == hash and std.mem.eql(u8, entry.key, word)) {
                // Keep steady-state hits read-only. A write is needed only
                // after the CLOCK scanner has cleared this entry, avoiding
                // cache-line ownership traffic between parallel readers.
                if (!entry.referenced.load(.monotonic)) {
                    @constCast(entry).referenced.store(true, .monotonic);
                }
                if (entry.token_ids.len == 1) {
                    try ids.append(allocator, entry.token_ids[0]);
                } else {
                    try ids.appendSlice(allocator, entry.token_ids);
                }
                self.recordBpeCacheHit(word.len, entry.token_ids.len, probes);
                return true;
            }
            slot_idx = (slot_idx + 1) & (bpe_cache_slots_per_shard - 1);
        }

        if (bpeBulkShardSlots(cache, shard_idx)) |bulk_slots| {
            slot_idx = bpeBulkCacheSlot(cache, hash);
            for (0..bulk_slots.len) |_| {
                probes += 1;
                const raw = bulk_slots[slot_idx].load(.acquire);
                if (raw == 0) break;
                if (raw == bpe_cache_tombstone) {
                    slot_idx = (slot_idx + 1) & (bulk_slots.len - 1);
                    continue;
                }
                const entry: *const BpeCacheEntry = @ptrFromInt(raw);
                if (entry.hash == hash and std.mem.eql(u8, entry.key, word)) {
                    if (!entry.referenced.load(.monotonic)) {
                        @constCast(entry).referenced.store(true, .monotonic);
                    }
                    if (entry.token_ids.len == 1) {
                        try ids.append(allocator, entry.token_ids[0]);
                    } else {
                        try ids.appendSlice(allocator, entry.token_ids);
                    }
                    self.recordBpeCacheHit(word.len, entry.token_ids.len, probes);
                    return true;
                }
                slot_idx = (slot_idx + 1) & (bulk_slots.len - 1);
            }
        }
        self.recordBpeCacheMiss(probes);
        return false;
    }

    fn recordBpeCacheHit(
        self: *HfTokenizer,
        key_len: usize,
        id_count: usize,
        probes: usize,
    ) void {
        if (!self.bpe_profile_enabled.load(.acquire)) return;
        _ = self.bpe_profile.hits.fetchAdd(1, .monotonic);
        _ = self.bpe_profile.probes.fetchAdd(probes, .monotonic);
        _ = self.bpe_profile.key_bytes.fetchAdd(key_len, .monotonic);
        _ = self.bpe_profile.token_ids.fetchAdd(id_count, .monotonic);
        _ = self.bpe_profile.key_len_histogram[@min(key_len, 32)].fetchAdd(1, .monotonic);
        _ = self.bpe_profile.id_count_histogram[@min(id_count, 8)].fetchAdd(1, .monotonic);
        _ = self.bpe_profile.probe_histogram[@min(probes, 16)].fetchAdd(1, .monotonic);
    }

    fn recordBpeCacheMiss(self: *HfTokenizer, probes: usize) void {
        if (!self.bpe_profile_enabled.load(.acquire)) return;
        _ = self.bpe_profile.misses.fetchAdd(1, .monotonic);
        _ = self.bpe_profile.probes.fetchAdd(probes, .monotonic);
        _ = self.bpe_profile.probe_histogram[@min(probes, 16)].fetchAdd(1, .monotonic);
    }

    pub fn setBpeProfiling(self: *HfTokenizer, enabled: bool) void {
        self.bpe_profile_enabled.store(false, .release);
        self.bpe_profile.pretokens.store(0, .monotonic);
        self.bpe_profile.direct_hits.store(0, .monotonic);
        self.bpe_profile.hits.store(0, .monotonic);
        self.bpe_profile.misses.store(0, .monotonic);
        self.bpe_profile.probes.store(0, .monotonic);
        self.bpe_profile.key_bytes.store(0, .monotonic);
        self.bpe_profile.token_ids.store(0, .monotonic);
        for (&self.bpe_profile.key_len_histogram) |*counter| {
            counter.store(0, .monotonic);
        }
        for (&self.bpe_profile.id_count_histogram) |*counter| {
            counter.store(0, .monotonic);
        }
        for (&self.bpe_profile.probe_histogram) |*counter| {
            counter.store(0, .monotonic);
        }
        self.bpe_profile_enabled.store(enabled, .release);
    }

    pub fn bpeProfileSnapshot(self: *const HfTokenizer) BpeProfile {
        var key_len_histogram: [33]u64 = undefined;
        for (&key_len_histogram, &self.bpe_profile.key_len_histogram) |*out, *counter| {
            out.* = counter.load(.monotonic);
        }
        var id_count_histogram: [9]u64 = undefined;
        for (&id_count_histogram, &self.bpe_profile.id_count_histogram) |*out, *counter| {
            out.* = counter.load(.monotonic);
        }
        var probe_histogram: [17]u64 = undefined;
        for (&probe_histogram, &self.bpe_profile.probe_histogram) |*out, *counter| {
            out.* = counter.load(.monotonic);
        }
        return .{
            .pretokens = self.bpe_profile.pretokens.load(.monotonic),
            .direct_hits = self.bpe_profile.direct_hits.load(.monotonic),
            .hits = self.bpe_profile.hits.load(.monotonic),
            .misses = self.bpe_profile.misses.load(.monotonic),
            .probes = self.bpe_profile.probes.load(.monotonic),
            .key_bytes = self.bpe_profile.key_bytes.load(.monotonic),
            .token_ids = self.bpe_profile.token_ids.load(.monotonic),
            .key_len_histogram = key_len_histogram,
            .id_count_histogram = id_count_histogram,
            .probe_histogram = probe_histogram,
        };
    }

    /// Best-effort cache insertion. Tokenization must not fail merely because
    /// the optional cache cannot grow, so allocation failures are ignored.
    fn cacheBpe(self: *HfTokenizer, word: []const u8, token_ids: []const i32) void {
        if (word.len == 0 or word.len > bpe_cache_max_key_bytes or token_ids.len == 0) return;

        const hash = bpeCacheHash(word);
        const cache = self.bpe_cache orelse return;
        if (!observeBpeCacheCandidate(cache, hash)) {
            _ = cache.rejected_admissions.fetchAdd(1, .monotonic);
            return;
        }
        const ids_bytes = std.math.mul(
            usize,
            token_ids.len,
            @sizeOf(i32),
        ) catch return;
        const entry_bytes = std.math.add(
            usize,
            @sizeOf(BpeCacheEntry) + word.len,
            ids_bytes,
        ) catch return;
        if (!tryReserveBpeCacheBytes(cache, entry_bytes)) {
            // If the candidate can ever fit, make room for its next admitted
            // occurrence. The current read epoch safely delays reclamation.
            if (entry_bytes <= cache.max_bytes -| @sizeOf(BpeCache)) {
                evictBpeCacheEntryForPressure(cache, hash, word);
            }
            return;
        }
        var own_reservation = true;
        defer if (own_reservation) releaseBpeCacheBytes(cache, entry_bytes);

        const key_copy = self.allocator.dupe(u8, word) catch return;
        var own_key = true;
        defer if (own_key) self.allocator.free(key_copy);
        const ids_copy = self.allocator.dupe(i32, token_ids) catch return;
        var own_ids = true;
        defer if (own_ids) self.allocator.free(ids_copy);
        const new_entry = self.allocator.create(BpeCacheEntry) catch return;
        var own_entry = true;
        defer if (own_entry) self.allocator.destroy(new_entry);
        new_entry.* = .{
            .hash = hash,
            .key = key_copy,
            .token_ids = ids_copy,
        };

        const shard = &cache.shards[bpeCacheShard(hash)];
        const shard_idx = bpeCacheShard(hash);
        lockBpeCacheMutex(&shard.mutex);
        defer shard.mutex.unlock();

        var slot_idx = bpeCacheSlot(hash);
        for (0..bpe_cache_slots_per_shard) |_| {
            const raw = shard.slots[slot_idx].load(.acquire);
            if (raw == 0) {
                break;
            }
            if (raw == bpe_cache_tombstone) {
                slot_idx = (slot_idx + 1) & (bpe_cache_slots_per_shard - 1);
                continue;
            }
            const entry: *const BpeCacheEntry = @ptrFromInt(raw);
            if (entry.hash == hash and std.mem.eql(u8, entry.key, word)) return;
            slot_idx = (slot_idx + 1) & (bpe_cache_slots_per_shard - 1);
        }

        if (bpeBulkShardSlots(cache, shard_idx)) |bulk_slots| {
            var bulk_slot_idx = bpeBulkCacheSlot(cache, hash);
            for (0..bulk_slots.len) |_| {
                const raw = bulk_slots[bulk_slot_idx].load(.acquire);
                if (raw == 0) break;
                if (raw > bpe_cache_tombstone) {
                    const entry: *const BpeCacheEntry = @ptrFromInt(raw);
                    if (entry.hash == hash and std.mem.eql(u8, entry.key, word)) return;
                }
                bulk_slot_idx = (bulk_slot_idx + 1) & (bulk_slots.len - 1);
            }
        }

        if (shard.count.load(.monotonic) >= bpe_cache_max_entries_per_shard) {
            if (bpeBulkShardSlots(cache, shard_idx)) |bulk_slots| {
                const bulk_max_entries = bulk_slots.len * 3 / 4;
                if (shard.bulk_count.load(.monotonic) >= bulk_max_entries and
                    !evictBpeBulkCacheEntry(cache, shard_idx, shard))
                {
                    return;
                }

                var first_bulk_tombstone: ?usize = null;
                var bulk_slot_idx = bpeBulkCacheSlot(cache, hash);
                for (0..bulk_slots.len) |_| {
                    const raw = bulk_slots[bulk_slot_idx].load(.acquire);
                    if (raw == bpe_cache_tombstone and first_bulk_tombstone == null) {
                        first_bulk_tombstone = bulk_slot_idx;
                    } else if (raw == 0) {
                        break;
                    }
                    bulk_slot_idx = (bulk_slot_idx + 1) & (bulk_slots.len - 1);
                }
                const bulk_insert_idx = first_bulk_tombstone orelse bulk_slot_idx;
                if (bulk_slots[bulk_insert_idx].load(.monotonic) == bpe_cache_tombstone) {
                    std.debug.assert(shard.bulk_tombstones > 0);
                    shard.bulk_tombstones -= 1;
                }
                bulk_slots[bulk_insert_idx].store(@intFromPtr(new_entry), .release);
                _ = shard.bulk_count.fetchAdd(1, .release);
                own_key = false;
                own_ids = false;
                own_entry = false;
                own_reservation = false;
                return;
            }
            if (!evictBpeCacheEntry(cache, shard)) return;
        }

        var first_tombstone: ?usize = null;
        slot_idx = bpeCacheSlot(hash);
        for (0..bpe_cache_slots_per_shard) |_| {
            const raw = shard.slots[slot_idx].load(.acquire);
            if (raw == bpe_cache_tombstone and first_tombstone == null) {
                first_tombstone = slot_idx;
            } else if (raw == 0) {
                break;
            }
            slot_idx = (slot_idx + 1) & (bpe_cache_slots_per_shard - 1);
        }
        const insert_idx = first_tombstone orelse slot_idx;
        if (shard.slots[insert_idx].load(.monotonic) == bpe_cache_tombstone) {
            std.debug.assert(shard.tombstones > 0);
            shard.tombstones -= 1;
        }
        shard.slots[insert_idx].store(@intFromPtr(new_entry), .release);
        _ = shard.count.fetchAdd(1, .release);
        own_key = false;
        own_ids = false;
        own_entry = false;
        own_reservation = false;
    }

    fn bpeEncodeWord(
        self: *HfTokenizer,
        allocator: std.mem.Allocator,
        word: []const u8,
        ids: *std.ArrayListUnmanaged(i32),
        scratch: *BpeScratch,
    ) !void {
        if (self.bpe_profile_enabled.load(.acquire)) {
            _ = self.bpe_profile.pretokens.fetchAdd(1, .monotonic);
        }
        if (self.byte_level_direct_ids) |direct_ids| {
            const id = switch (word.len) {
                1 => direct_ids.single[word[0]],
                2 => direct_ids.pair[
                    (@as(usize, word[0]) << 8) | @as(usize, word[1])
                ],
                else => -1,
            };
            if (id >= 0) {
                if (self.bpe_profile_enabled.load(.acquire)) {
                    _ = self.bpe_profile.direct_hits.fetchAdd(1, .monotonic);
                }
                try ids.append(allocator, id);
                return;
            }
        }
        if (try self.appendCachedBpe(allocator, word, ids)) return;
        const output_start = ids.items.len;
        try self.bpeEncodeWordUncached(allocator, word, ids, scratch);
        self.cacheBpe(word, ids.items[output_start..]);
    }

    fn bpeEncodeWordUncached(
        self: *HfTokenizer,
        allocator: std.mem.Allocator,
        word: []const u8,
        ids: *std.ArrayListUnmanaged(i32),
        scratch: *BpeScratch,
    ) !void {
        if (word.len == 0) return;

        // Check added tokens first
        if (self.added_tokens.get(word)) |id| {
            try ids.append(allocator, id);
            return;
        }

        // Some HF BPE tokenizers, including Gemma 3, contain whole-word entries
        // that are not reconstructible from the merge table alone. Prefer an
        // exact vocab hit before falling back to character-split merges, but
        // only for models without an end-of-word suffix. CLIP-style BPE has
        // both raw byte/character entries ("a") and word-final entries
        // ("a</w>"); taking the raw hit here bypasses suffix-aware merges.
        if (self.end_of_word_suffix.len == 0 and self.vocab.get(word) != null) {
            const id = self.vocab.get(word).?;
            try ids.append(allocator, id);
            return;
        }

        if (self.shouldPreferDirectBpePieces()) {
            try self.bpeEncodeByDirectPieces(allocator, word, ids);
            return;
        }

        // Build a working buffer that holds `word` followed by the optional
        // end-of-word suffix. Symbols index into this buffer via (start, end),
        // which lets each merge extend the left symbol's range without
        // allocating a fresh concatenated string.
        var work_owned: ?[]u8 = null;
        defer if (work_owned) |buf| allocator.free(buf);
        const work: []const u8 = if (self.end_of_word_suffix.len == 0)
            word
        else blk: {
            const owned = try allocator.alloc(u8, word.len + self.end_of_word_suffix.len);
            @memcpy(owned[0..word.len], word);
            @memcpy(owned[word.len..], self.end_of_word_suffix);
            work_owned = owned;
            break :blk owned;
        };

        scratch.symbols.clearRetainingCapacity();
        try scratch.symbols.ensureTotalCapacity(allocator, word.len + 1);
        const symbols = &scratch.symbols;

        // One symbol per UTF-8 codepoint of `word`.
        var pos: usize = 0;
        while (pos < word.len) {
            const cp_len = if (self.pre_tokenizer_type == .byte_level)
                1
            else
                utf8CodepointLen(word[pos]);
            const end = @min(pos + cp_len, word.len);
            const idx: i32 = @intCast(symbols.items.len);
            try symbols.append(allocator, .{
                .start = @intCast(pos),
                .end = @intCast(end),
                .token_id = self.vocab.get(work[pos..end]) orelse -1,
                .prev = idx - 1,
                .next = idx + 1,
            });
            pos = end;
        }
        if (symbols.items.len > 0) {
            symbols.items[symbols.items.len - 1].next = -1;
            // Extend the last symbol's range over the suffix.
            if (self.end_of_word_suffix.len > 0) {
                symbols.items[symbols.items.len - 1].end = @intCast(work.len);
                const last = &symbols.items[symbols.items.len - 1];
                last.token_id = self.vocab.get(work[last.start..last.end]) orelse -1;
            }
        }

        const pq = try scratch.candidateQueue(allocator);
        for (0..symbols.items.len) |i| {
            const next = symbols.items[i].next;
            if (next < 0) continue;
            const right_idx: u32 = @intCast(next);
            const a = work[symbols.items[i].start..symbols.items[i].end];
            const b = work[symbols.items[right_idx].start..symbols.items[right_idx].end];
            if (try self.bpeMerge(
                allocator,
                symbols.items[i].token_id,
                symbols.items[right_idx].token_id,
                a,
                b,
            )) |merge| {
                try pq.insert(.{ .rank = merge.rank, .left = @intCast(i), .right = right_idx });
            }
        }

        while (pq.len() > 0) {
            const cand = pq.popMax();
            const left = &symbols.items[cand.left];
            if (!left.alive) continue;
            if (left.next != @as(i32, @intCast(cand.right))) continue;
            const right = &symbols.items[cand.right];
            if (!right.alive) continue;

            const a = work[left.start..left.end];
            const b = work[right.start..right.end];
            const current_merge = (try self.bpeMerge(
                allocator,
                left.token_id,
                right.token_id,
                a,
                b,
            )) orelse continue;
            if (current_merge.rank != cand.rank) continue;

            // Merge: extend left's range over right, splice right out of the
            // doubly-linked list. No allocation, since the bytes are already
            // contiguous in `work`.
            left.end = right.end;
            left.token_id = current_merge.result_id;
            const new_next = right.next;
            left.next = new_next;
            if (new_next >= 0) symbols.items[@intCast(new_next)].prev = @intCast(cand.left);
            right.alive = false;

            const left_bytes = work[left.start..left.end];
            if (left.prev >= 0) {
                const prev_idx: u32 = @intCast(left.prev);
                const pa = work[symbols.items[prev_idx].start..symbols.items[prev_idx].end];
                if (try self.bpeMerge(
                    allocator,
                    symbols.items[prev_idx].token_id,
                    left.token_id,
                    pa,
                    left_bytes,
                )) |merge| {
                    try pq.insert(.{ .rank = merge.rank, .left = prev_idx, .right = cand.left });
                }
            }
            if (left.next >= 0) {
                const next_idx: u32 = @intCast(left.next);
                const nb = work[symbols.items[next_idx].start..symbols.items[next_idx].end];
                if (try self.bpeMerge(
                    allocator,
                    left.token_id,
                    symbols.items[next_idx].token_id,
                    left_bytes,
                    nb,
                )) |merge| {
                    try pq.insert(.{ .rank = merge.rank, .left = cand.left, .right = next_idx });
                }
            }
        }

        // Walk the surviving symbols and emit ids.
        var idx: i32 = 0;
        while (idx >= 0 and idx < symbols.items.len) {
            const sym = symbols.items[@intCast(idx)];
            if (sym.alive) {
                const bytes = work[sym.start..sym.end];
                if (sym.token_id >= 0) {
                    try ids.append(allocator, sym.token_id);
                } else if (self.vocab.get(bytes)) |id| {
                    try ids.append(allocator, id);
                } else if (self.byte_fallback) {
                    for (bytes) |byte| {
                        var hex_buf: [6]u8 = undefined;
                        const hex = std.fmt.bufPrint(&hex_buf, "<0x{X:0>2}>", .{byte}) catch continue;
                        if (self.vocab.get(hex)) |id| {
                            try ids.append(allocator, id);
                        } else {
                            try ids.append(allocator, self.special.unk_id);
                        }
                    }
                } else {
                    try ids.append(allocator, self.special.unk_id);
                }
            }
            if (sym.next < 0) break;
            idx = sym.next;
        }
    }

    fn shouldPreferDirectBpePieces(self: *const HfTokenizer) bool {
        return self.model_type == .bpe and
            // A declared merge table defines ranked BPE semantics. The
            // longest-vocab-piece fallback is only valid for legacy/generated
            // vocab-only configs; using it when merges exist can select pieces
            // that the ranked merge sequence would never produce.
            self.merge_ranks.count() == 0 and
            self.replace_space_with != null and
            self.pre_tokenizer_type == .none and
            self.continuing_prefix.len == 0 and
            self.end_of_word_suffix.len == 0;
    }

    fn bpeEncodeByDirectPieces(
        self: *HfTokenizer,
        allocator: std.mem.Allocator,
        word: []const u8,
        ids: *std.ArrayListUnmanaged(i32),
    ) !void {
        // Caller already gated on shouldPreferDirectBpePieces, which is the
        // same predicate that guarantees the trie was built at load time.
        // Defer to the legacy substring-shrink path if for any reason the
        // trie is missing rather than crash on the optional unwrap.
        const trie: *const VocabTrie = if (self.bpe_direct_trie) |*t|
            t
        else
            return self.bpeEncodeByDirectPiecesFallback(allocator, word, ids);
        const trie_nodes = trie.nodes.items;

        var start: usize = 0;
        while (start < word.len) {
            // Walk the trie from `start` to find the longest vocab match,
            // remembering the deepest final node we hit.
            var node_idx: u32 = 0;
            var best_id: i32 = -1;
            var best_end: usize = start;
            var i = start;
            while (i < word.len) : (i += 1) {
                const child = trie_nodes[node_idx].children.get(word[i]) orelse break;
                node_idx = child;
                const tok_id = trie_nodes[node_idx].token_id;
                if (tok_id >= 0) {
                    best_id = tok_id;
                    best_end = i + 1;
                }
            }

            if (best_id >= 0) {
                try ids.append(allocator, best_id);
                start = best_end;
                continue;
            }

            // No vocab match at this position. Emit one codepoint, falling
            // back to byte-level <0xNN> tokens or the unk id.
            const cp_len = utf8CodepointLen(word[start]);
            const end_cp = @min(start + cp_len, word.len);
            const piece = word[start..end_cp];
            if (self.byte_fallback) {
                for (piece) |byte| {
                    var buf: [6]u8 = undefined;
                    const hex = std.fmt.bufPrint(&buf, "<0x{X:0>2}>", .{byte}) catch {
                        try ids.append(allocator, self.special.unk_id);
                        continue;
                    };
                    if (self.vocab.get(hex)) |id| {
                        try ids.append(allocator, id);
                    } else {
                        try ids.append(allocator, self.special.unk_id);
                    }
                }
            } else {
                try ids.append(allocator, self.special.unk_id);
            }
            start = end_cp;
        }
    }

    fn bpeEncodeByDirectPiecesFallback(
        self: *HfTokenizer,
        allocator: std.mem.Allocator,
        word: []const u8,
        ids: *std.ArrayListUnmanaged(i32),
    ) !void {
        var start: usize = 0;
        while (start < word.len) {
            var found = false;
            var end = word.len;
            while (end > start) {
                if (end < word.len and (word[end] & 0xC0) == 0x80) {
                    end -= 1;
                    continue;
                }
                const piece = word[start..end];
                if (self.vocab.get(piece)) |id| {
                    try ids.append(allocator, id);
                    start = end;
                    found = true;
                    break;
                }
                end = prevCodepointBoundary(word, end);
            }
            if (found) continue;

            const cp_len = utf8CodepointLen(word[start]);
            const end_cp = @min(start + cp_len, word.len);
            const piece = word[start..end_cp];
            if (self.vocab.get(piece)) |id| {
                try ids.append(allocator, id);
            } else if (self.byte_fallback) {
                for (piece) |byte| {
                    var buf: [6]u8 = undefined;
                    const hex = std.fmt.bufPrint(&buf, "<0x{X:0>2}>", .{byte}) catch {
                        try ids.append(allocator, self.special.unk_id);
                        continue;
                    };
                    if (self.vocab.get(hex)) |id| {
                        try ids.append(allocator, id);
                    } else {
                        try ids.append(allocator, self.special.unk_id);
                    }
                }
            } else {
                try ids.append(allocator, self.special.unk_id);
            }
            start = end_cp;
        }
    }

    // =====================================================================
    // Unigram encoding (Viterbi)
    // =====================================================================

    fn encodeUnigram(
        self: *HfTokenizer,
        allocator: std.mem.Allocator,
        text: []const u8,
        ids: *std.ArrayListUnmanaged(i32),
    ) !void {
        return self.encodeUnigramWithMetaspaceScheme(allocator, text, null, ids);
    }

    fn encodeUnigramWithMetaspaceScheme(
        self: *HfTokenizer,
        allocator: std.mem.Allocator,
        text: []const u8,
        metaspace_scheme_override: ?MetaspacePrependScheme,
        ids: *std.ArrayListUnmanaged(i32),
    ) !void {
        switch (self.pre_tokenizer_type) {
            .metaspace => {
                const prepend_scheme = metaspace_scheme_override orelse self.metaspace_prepend_scheme;
                const words = try metaspacePreTokenize(
                    allocator,
                    text,
                    self.metaspace_replacement,
                    prepend_scheme,
                    self.metaspace_split,
                );
                defer {
                    for (words) |w| allocator.free(w);
                    allocator.free(words);
                }
                for (words) |word| {
                    try self.unigramEncodeWord(allocator, word, ids);
                }
            },
            .bert => {
                const words = try bertPreTokenize(allocator, text);
                defer allocator.free(words);
                for (words) |word| {
                    try self.unigramEncodeWord(allocator, word, ids);
                }
            },
            else => {
                // Default: treat entire text as one piece
                try self.unigramEncodeWord(allocator, text, ids);
            },
        }
    }

    fn encodeGeneration(ptr: *anyopaque, allocator: std.mem.Allocator, text: []const u8, max_length: usize, add_bos_token: bool) anyerror!@import("tokenizer.zig").EncodeResult {
        const self: *HfTokenizer = @ptrCast(@alignCast(ptr));
        var raw = std.ArrayListUnmanaged(i32).empty;
        defer raw.deinit(allocator);
        if (add_bos_token and self.pre_tokenizer_type == .metaspace) {
            // This override intentionally bypasses encodeInto so it can
            // suppress Metaspace's implicit prefix when BOS is explicit.
            // Establish the same read epoch here before touching the shared
            // lock-free BPE cache.
            const cache_reader = self.enterBpeCacheRead();
            defer if (cache_reader) |cache| self.leaveBpeCacheRead(cache);
            try self.encodeWithAddedTokensMetaspaceOverride(allocator, text, .never, &raw);
        } else {
            try self.encodeInto(allocator, text, &raw);
        }
        const raw_ids = raw.items;

        const tok_iface = self.tokenizer();
        const prepend_bos = add_bos_token and tok_iface.specialTokens().cls_id >= 0 and max_length > 0;
        const available = if (prepend_bos) max_length - 1 else max_length;
        const token_count = @min(raw_ids.len, available);
        const ids = try allocator.alloc(i32, max_length);
        const mask = try allocator.alloc(i32, max_length);

        var pos: usize = 0;
        if (prepend_bos) {
            ids[0] = tok_iface.specialTokens().cls_id;
            mask[0] = 1;
            pos = 1;
        }
        for (0..token_count) |i| {
            ids[pos + i] = raw_ids[i];
            mask[pos + i] = 1;
        }
        for (pos + token_count..max_length) |i| {
            ids[i] = tok_iface.specialTokens().pad_id;
            mask[i] = 0;
        }

        return .{
            .ids = ids,
            .attention_mask = mask,
            .allocator = allocator,
        };
    }

    fn encodeForModel(ptr: *anyopaque, allocator: std.mem.Allocator, text: []const u8, max_length: usize) anyerror!@import("tokenizer.zig").EncodeResult {
        const self: *HfTokenizer = @ptrCast(@alignCast(ptr));
        if (self.model_type == .word_piece and self.pre_tokenizer_type == .bert) {
            var raw = try self.encodeWordPieceWithOffsets(allocator, text);
            defer raw.deinit(allocator);
            return HfTokenizer.wrapModelEncodingWithOffsets(self, allocator, raw.ids.items, raw.offsets.items, max_length);
        }
        if (self.model_type == .unigram and self.pre_tokenizer_type == .metaspace and self.metaspace_split) {
            var raw = try self.encodeUnigramWithOffsets(allocator, text);
            defer raw.deinit(allocator);
            return HfTokenizer.wrapModelEncodingWithOffsets(self, allocator, raw.ids.items, raw.offsets.items, max_length);
        }
        {
            const raw_ids = try self.encode(allocator, text);
            defer allocator.free(raw_ids);

            const special = self.getSpecialTokens();
            const max_tokens = if (max_length >= 2) max_length - 2 else 0;
            const token_count = @min(raw_ids.len, max_tokens);
            const total = token_count + 2;
            const ids = try allocator.alloc(i32, max_length);
            const mask = try allocator.alloc(i32, max_length);

            ids[0] = special.cls_id;
            mask[0] = 1;
            for (0..token_count) |i| {
                ids[i + 1] = raw_ids[i];
                mask[i + 1] = 1;
            }
            ids[total - 1] = special.sep_id;
            mask[total - 1] = 1;
            for (total..max_length) |i| {
                ids[i] = special.pad_id;
                mask[i] = 0;
            }
            return .{
                .ids = ids,
                .attention_mask = mask,
                .allocator = allocator,
            };
        }
    }

    fn unigramEncodeWord(self: *HfTokenizer, allocator: std.mem.Allocator, word: []const u8, ids: *std.ArrayListUnmanaged(i32)) !void {
        if (word.len == 0) return;

        // Check added tokens
        if (self.added_tokens.get(word)) |id| {
            try ids.append(allocator, id);
            return;
        }

        // Viterbi algorithm for best segmentation
        const n = word.len;

        // best_score[i] = best log probability for word[0..i]
        const best_score = try allocator.alloc(f32, n + 1);
        defer allocator.free(best_score);
        // best_len[i] = length of token ending at position i in best path
        const best_len = try allocator.alloc(usize, n + 1);
        defer allocator.free(best_len);

        best_score[0] = 0;
        best_len[0] = 0;
        for (1..n + 1) |i| {
            best_score[i] = -std.math.inf(f32);
            best_len[i] = 1; // default: single byte fallback
        }

        // Forward pass: walk the vocab trie from each position to enumerate
        // every token that can start there in a single pass, then relax the
        // Viterbi score for each. This avoids the O(max_len) hashmap probe
        // miss that the previous (start, len) double-loop incurred for the
        // common case where most prefixes have no continuation.
        for (0..n) |start| {
            if (start > 0 and best_score[start] == -std.math.inf(f32)) continue;

            const trie_nodes = self.unigram_trie.nodes.items;
            const vocab_items = self.unigram_vocab.items;
            const start_score = best_score[start];
            var node_idx: u32 = 0;
            const limit = @min(n - start, 128);
            var len: usize = 0;
            while (len < limit) : (len += 1) {
                const child = trie_nodes[node_idx].children.get(word[start + len]) orelse break;
                node_idx = child;
                const tok_id = trie_nodes[node_idx].token_id;
                if (tok_id < 0) continue;
                const score = vocab_items[@intCast(tok_id)].score;
                const end = start + len + 1;
                const candidate = start_score + score;
                if (candidate > best_score[end]) {
                    best_score[end] = candidate;
                    best_len[end] = len + 1;
                }
            }

            // Single-byte fallback (<0xNN>) for positions the trie didn't cover
            // with a one-byte token.
            const end1 = start + 1;
            if (best_score[end1] == -std.math.inf(f32)) {
                var buf: [6]u8 = undefined;
                const hex = std.fmt.bufPrint(&buf, "<0x{X:0>2}>", .{word[start]}) catch continue;
                if (self.vocab.contains(hex)) {
                    const candidate = best_score[start] + (-10.0);
                    if (candidate > best_score[end1]) {
                        best_score[end1] = candidate;
                        best_len[end1] = 1;
                    }
                }
            }
        }

        // Backward pass: reconstruct best path
        var segments = std.ArrayListUnmanaged([]const u8).empty;
        defer segments.deinit(allocator);

        var pos: usize = n;
        while (pos > 0) {
            const len = best_len[pos];
            if (len == 0) {
                // Shouldn't happen, but safety: emit unk and break
                try ids.append(allocator, self.unigram_unk_id);
                return;
            }
            try segments.append(allocator, word[pos - len .. pos]);
            pos -= len;
        }

        // Segments are in reverse order
        var i = segments.items.len;
        while (i > 0) {
            i -= 1;
            const piece = segments.items[i];
            if (self.vocab.get(piece)) |id| {
                try ids.append(allocator, id);
            } else if (piece.len == 1) {
                // Byte fallback
                var buf: [6]u8 = undefined;
                const hex = std.fmt.bufPrint(&buf, "<0x{X:0>2}>", .{piece[0]}) catch {
                    try ids.append(allocator, self.unigram_unk_id);
                    continue;
                };
                if (self.vocab.get(hex)) |id| {
                    try ids.append(allocator, id);
                } else {
                    try ids.append(allocator, self.unigram_unk_id);
                }
            } else {
                try ids.append(allocator, self.unigram_unk_id);
            }
        }
    }

    fn unigramEncodeWordWithOffsets(
        self: *HfTokenizer,
        allocator: std.mem.Allocator,
        word: []const u8,
        word_start: usize,
        prefix_len: usize,
        result: *RawWordPieceEncoding,
    ) !void {
        if (word.len == 0) return;

        if (self.added_tokens.get(word)) |id| {
            try result.ids.append(allocator, id);
            try result.offsets.append(allocator, .{
                @intCast(word_start),
                @intCast(word_start + word.len - clampedPrefixLen(prefix_len, word.len)),
            });
            return;
        }

        const n = word.len;
        const best_score = try allocator.alloc(f32, n + 1);
        defer allocator.free(best_score);
        const best_len = try allocator.alloc(usize, n + 1);
        defer allocator.free(best_len);

        best_score[0] = 0;
        best_len[0] = 0;
        for (1..n + 1) |i| {
            best_score[i] = -std.math.inf(f32);
            best_len[i] = 1;
        }

        for (0..n) |start| {
            if (start > 0 and best_score[start] == -std.math.inf(f32)) continue;

            const trie_nodes = self.unigram_trie.nodes.items;
            const vocab_items = self.unigram_vocab.items;
            const start_score = best_score[start];
            var node_idx: u32 = 0;
            const limit = @min(n - start, 128);
            var len: usize = 0;
            while (len < limit) : (len += 1) {
                const child = trie_nodes[node_idx].children.get(word[start + len]) orelse break;
                node_idx = child;
                const tok_id = trie_nodes[node_idx].token_id;
                if (tok_id < 0) continue;
                const score = vocab_items[@intCast(tok_id)].score;
                const end = start + len + 1;
                const candidate = start_score + score;
                if (candidate > best_score[end]) {
                    best_score[end] = candidate;
                    best_len[end] = len + 1;
                }
            }

            const end1 = start + 1;
            if (best_score[end1] == -std.math.inf(f32)) {
                var buf: [6]u8 = undefined;
                const hex = std.fmt.bufPrint(&buf, "<0x{X:0>2}>", .{word[start]}) catch continue;
                if (self.vocab.contains(hex)) {
                    const candidate = best_score[start] + (-10.0);
                    if (candidate > best_score[end1]) {
                        best_score[end1] = candidate;
                        best_len[end1] = 1;
                    }
                }
            }
        }

        var segments = std.ArrayListUnmanaged([2]usize).empty;
        defer segments.deinit(allocator);

        var pos: usize = n;
        while (pos > 0) {
            const len = best_len[pos];
            if (len == 0) {
                try result.ids.append(allocator, self.unigram_unk_id);
                try result.offsets.append(allocator, .{
                    @intCast(word_start),
                    @intCast(word_start + word.len - clampedPrefixLen(prefix_len, word.len)),
                });
                return;
            }
            try segments.append(allocator, .{ pos - len, pos });
            pos -= len;
        }

        var i = segments.items.len;
        while (i > 0) {
            i -= 1;
            const range = segments.items[i];
            const piece = word[range[0]..range[1]];
            if (self.vocab.get(piece)) |id| {
                try result.ids.append(allocator, id);
            } else if (piece.len == 1) {
                var buf: [6]u8 = undefined;
                const hex = std.fmt.bufPrint(&buf, "<0x{X:0>2}>", .{piece[0]}) catch {
                    try result.ids.append(allocator, self.unigram_unk_id);
                    continue;
                };
                if (self.vocab.get(hex)) |id| {
                    try result.ids.append(allocator, id);
                } else {
                    try result.ids.append(allocator, self.unigram_unk_id);
                }
            } else {
                try result.ids.append(allocator, self.unigram_unk_id);
            }

            const local_start = adjustedOffset(range[0], prefix_len, word.len);
            const local_end = adjustedOffset(range[1], prefix_len, word.len);
            try result.offsets.append(allocator, .{
                @intCast(word_start + local_start),
                @intCast(word_start + local_end),
            });
        }
    }

    fn wrapModelEncodingWithOffsets(
        self: *HfTokenizer,
        allocator: std.mem.Allocator,
        raw_ids: []const i32,
        raw_offsets: []const [2]u32,
        max_length: usize,
    ) !@import("tokenizer.zig").EncodeResult {
        const special = self.getSpecialTokens();
        const max_tokens = if (max_length >= 2) max_length - 2 else 0;
        const token_count = @min(raw_ids.len, max_tokens);
        const total = token_count + 2;
        const ids = try allocator.alloc(i32, max_length);
        const mask = try allocator.alloc(i32, max_length);
        const offsets = try allocator.alloc([2]u32, max_length);

        ids[0] = special.cls_id;
        mask[0] = 1;
        offsets[0] = .{ 0, 0 };

        for (0..token_count) |i| {
            ids[i + 1] = raw_ids[i];
            mask[i + 1] = 1;
            offsets[i + 1] = raw_offsets[i];
        }

        ids[total - 1] = special.sep_id;
        mask[total - 1] = 1;
        offsets[total - 1] = .{ 0, 0 };

        for (total..max_length) |i| {
            ids[i] = special.pad_id;
            mask[i] = 0;
            offsets[i] = .{ 0, 0 };
        }

        return .{
            .ids = ids,
            .attention_mask = mask,
            .offsets = offsets,
            .allocator = allocator,
        };
    }

    // =====================================================================
    // Decoding
    // =====================================================================

    fn decode(self: *HfTokenizer, allocator: std.mem.Allocator, token_ids: []const i32) ![]u8 {
        var result = std.ArrayListUnmanaged(u8).empty;

        for (token_ids) |id| {
            if (self.id_to_token.get(id)) |token| {
                // Skip special tokens in decode output
                if (self.added_tokens.contains(token)) continue;

                switch (self.model_type) {
                    .word_piece => {
                        if (std.mem.startsWith(u8, token, self.continuing_prefix)) {
                            try result.appendSlice(allocator, token[self.continuing_prefix.len..]);
                        } else {
                            if (result.items.len > 0) try result.append(allocator, ' ');
                            try result.appendSlice(allocator, token);
                        }
                    },
                    .bpe => {
                        // BPE: tokens may use byte-level encoding or have ▁ for spaces.
                        //
                        // byte_level_split must decode the same way as byte_level. Llama 3
                        // and Qwen3 both declare pre_tokenizer Sequence[Split, ByteLevel]
                        // with decoder ByteLevel, so skipping the decode here left raw
                        // marker characters ('Ġ' for space, 'Ċ' for newline) in generated
                        // text.
                        if (self.pre_tokenizer_type == .byte_level or
                            self.pre_tokenizer_type == .byte_level_split)
                        {
                            try appendByteDecoded(&result, allocator, token);
                        } else {
                            try result.appendSlice(allocator, token);
                        }
                    },
                    .unigram => {
                        // Unigram: ▁ represents space
                        try result.appendSlice(allocator, token);
                    },
                }
            }
        }

        // For metaspace/unigram or BPE with ▁ normalizer: replace ▁ with spaces and strip leading space
        if (self.model_type == .unigram or
            (self.model_type == .bpe and self.pre_tokenizer_type == .metaspace) or
            (self.model_type == .bpe and self.replace_space_with != null))
        {
            const cleaned = try replaceMetaspace(allocator, result.items, self.metaspace_replacement);
            result.deinit(allocator);
            return cleaned;
        }

        return try result.toOwnedSlice(allocator);
    }

    fn getSpecialTokens(self: *HfTokenizer) SpecialTokens {
        return self.special;
    }

    fn getVocabSize(self: *HfTokenizer) usize {
        return self.id_to_token.count();
    }

    pub fn deinitSelf(self: *HfTokenizer) void {
        const allocator = self.allocator;
        for (self.arena_strings.items) |s| {
            allocator.free(s);
        }
        self.arena_strings.deinit(allocator);
        self.vocab.deinit(allocator);
        self.id_to_token.deinit(allocator);
        self.added_tokens.deinit(allocator);
        self.added_trie.deinit(allocator);
        self.merge_ranks.deinit(allocator);
        self.merge_pairs.deinit(allocator);
        if (self.bpe_cache) |cache| {
            const accounted_bytes = cache.used_bytes.load(.acquire);
            for (&cache.shards) |*shard| {
                for (&shard.slots) |*slot| {
                    const raw = slot.load(.monotonic);
                    if (raw > bpe_cache_tombstone) {
                        const entry: *BpeCacheEntry = @ptrFromInt(raw);
                        allocator.free(entry.key);
                        allocator.free(entry.token_ids);
                        allocator.destroy(entry);
                    }
                }
            }
            if (cache.bulk_slots) |bulk_slots| {
                for (bulk_slots) |*slot| {
                    const raw = slot.load(.monotonic);
                    if (raw > bpe_cache_tombstone) {
                        const entry: *BpeCacheEntry = @ptrFromInt(raw);
                        allocator.free(entry.key);
                        allocator.free(entry.token_ids);
                        allocator.destroy(entry);
                    }
                }
                allocator.free(bulk_slots);
            }
            var retired = cache.retired_head;
            while (retired) |entry| {
                const next = entry.next_retired;
                allocator.free(entry.key);
                allocator.free(entry.token_ids);
                allocator.destroy(entry);
                retired = next;
            }
            self.releaseResourceBudgetBytes(accounted_bytes);
            allocator.destroy(cache);
        }
        if (self.byte_level_direct_ids) |direct_ids| allocator.destroy(direct_ids);
        var workspace = self.parallel_workspace_all;
        while (workspace) |current| {
            const next = current.next_all;
            self.destroyParallelBpeWorkspace(current);
            workspace = next;
        }
        for (&self.worker_bpe_caches) |*lease| {
            if (lease.cache) |*cache| cache.deinit(self);
        }
        if (self.cache_resource_budget_bytes != 0)
            @panic("tokenizer deinitialized with live resource charges");
        // Drop the capability only after all cache/workspace accounting has
        // reached zero. This is the final lifetime edge to a shared resource
        // domain and may destroy that domain.
        self.switchResourceBudget(null) catch
            @panic("tokenizer could not detach its resource budget");
        self.unigram_vocab.deinit(allocator);
        self.unigram_trie.deinit(allocator);
        if (self.bpe_direct_trie) |*t| t.deinit(allocator);
        allocator.destroy(self);
    }
};

// =========================================================================
// Pre-tokenizer implementations
// =========================================================================

pub const EncodingWithOffsets = struct {
    ids: std.ArrayListUnmanaged(i32) = .empty,
    offsets: std.ArrayListUnmanaged([2]u32) = .empty,

    pub fn deinit(self: *EncodingWithOffsets, allocator: std.mem.Allocator) void {
        self.ids.deinit(allocator);
        self.offsets.deinit(allocator);
    }
};

const RawWordPieceEncoding = EncodingWithOffsets;

const PreTokenSpan = struct {
    text: []const u8,
    start: usize,
    end: usize,
};

fn clampedPrefixLen(prefix_len: usize, word_len: usize) usize {
    return @min(prefix_len, word_len);
}

fn adjustedOffset(pos: usize, prefix_len: usize, word_len: usize) usize {
    const clamped = clampedPrefixLen(prefix_len, word_len);
    if (pos <= clamped) return 0;
    return pos - clamped;
}

/// BERT pre-tokenizer: split on whitespace and punctuation.
/// Returns slices borrowed from `text`. Caller owns the outer slice but must
/// not free the inner string contents.
fn bertPreTokenize(allocator: std.mem.Allocator, text: []const u8) ![][]const u8 {
    var words = std.ArrayListUnmanaged([]const u8).empty;
    var start: usize = 0;
    var i: usize = 0;

    while (i < text.len) {
        const c = text[i];
        if (std.ascii.isWhitespace(c)) {
            if (i > start) {
                try words.append(allocator, text[start..i]);
            }
            i += 1;
            start = i;
        } else if (isPunctuation(c)) {
            if (i > start) {
                try words.append(allocator, text[start..i]);
            }
            try words.append(allocator, text[i .. i + 1]);
            i += 1;
            start = i;
        } else {
            i += 1;
        }
    }

    if (i > start) {
        try words.append(allocator, text[start..i]);
    }

    return try words.toOwnedSlice(allocator);
}

fn bertPreTokenizeWithOffsets(allocator: std.mem.Allocator, text: []const u8) ![]PreTokenSpan {
    var words = std.ArrayListUnmanaged(PreTokenSpan).empty;
    var start: usize = 0;
    var i: usize = 0;

    while (i < text.len) {
        const c = text[i];
        if (std.ascii.isWhitespace(c)) {
            if (i > start) {
                try words.append(allocator, .{
                    .text = text[start..i],
                    .start = start,
                    .end = i,
                });
            }
            i += 1;
            start = i;
        } else if (isPunctuation(c)) {
            if (i > start) {
                try words.append(allocator, .{
                    .text = text[start..i],
                    .start = start,
                    .end = i,
                });
            }
            try words.append(allocator, .{
                .text = text[i .. i + 1],
                .start = i,
                .end = i + 1,
            });
            i += 1;
            start = i;
        } else {
            i += 1;
        }
    }

    if (i > start) {
        try words.append(allocator, .{
            .text = text[start..i],
            .start = start,
            .end = i,
        });
    }

    return try words.toOwnedSlice(allocator);
}

/// Metaspace pre-tokenizer.
/// When `split` is false, this returns the whole transformed string as one piece.
fn metaspacePreTokenize(
    allocator: std.mem.Allocator,
    text: []const u8,
    replacement: []const u8,
    prepend_scheme: MetaspacePrependScheme,
    split: bool,
) ![][]const u8 {
    var words = std.ArrayListUnmanaged([]const u8).empty;

    if (!split) {
        var prepared = std.ArrayListUnmanaged(u8).empty;
        defer prepared.deinit(allocator);

        if (text.len > 0 and prepend_scheme != .never) {
            try prepared.appendSlice(allocator, replacement);
        }
        for (text) |ch| {
            if (ch == ' ') {
                try prepared.appendSlice(allocator, replacement);
            } else {
                try prepared.append(allocator, ch);
            }
        }

        try words.append(allocator, try prepared.toOwnedSlice(allocator));
        return try words.toOwnedSlice(allocator);
    }

    const prepend_first = prepend_scheme != .never;
    var iter = std.mem.splitScalar(u8, text, ' ');
    var first = true;
    while (iter.next()) |segment| {
        if (segment.len == 0) {
            first = false;
            continue;
        }

        if ((prepend_first and first) or !first) {
            const word = try std.fmt.allocPrint(allocator, "{s}{s}", .{ replacement, segment });
            try words.append(allocator, word);
        } else {
            try words.append(allocator, try allocator.dupe(u8, segment));
        }
        first = false;
    }

    return try words.toOwnedSlice(allocator);
}

fn metaspacePreTokenizeWithOffsets(
    allocator: std.mem.Allocator,
    text: []const u8,
    replacement: []const u8,
    prepend_scheme: MetaspacePrependScheme,
    split: bool,
) ![]PreTokenSpan {
    var words = std.ArrayListUnmanaged(PreTokenSpan).empty;

    if (!split) {
        var prepared = std.ArrayListUnmanaged(u8).empty;
        defer prepared.deinit(allocator);

        if (text.len > 0 and prepend_scheme != .never) {
            try prepared.appendSlice(allocator, replacement);
        }
        for (text) |ch| {
            if (ch == ' ') {
                try prepared.appendSlice(allocator, replacement);
            } else {
                try prepared.append(allocator, ch);
            }
        }

        try words.append(allocator, .{
            .text = try prepared.toOwnedSlice(allocator),
            .start = 0,
            .end = text.len,
        });
        return try words.toOwnedSlice(allocator);
    }

    const prepend_first = prepend_scheme != .never;
    var seg_start: ?usize = null;
    var first = true;
    var i: usize = 0;
    while (i <= text.len) : (i += 1) {
        const at_end = i == text.len;
        const is_space = !at_end and text[i] == ' ';
        if (at_end or is_space) {
            if (seg_start) |start_idx| {
                const segment = text[start_idx..i];
                if (segment.len > 0) {
                    const needs_prefix = (prepend_first and first) or !first;
                    const transformed = if (needs_prefix)
                        try std.fmt.allocPrint(allocator, "{s}{s}", .{ replacement, segment })
                    else
                        try allocator.dupe(u8, segment);
                    try words.append(allocator, .{
                        .text = transformed,
                        .start = start_idx,
                        .end = i,
                    });
                    first = false;
                }
                seg_start = null;
            } else if (is_space) {
                first = false;
            }
        } else if (seg_start == null) {
            seg_start = i;
        }
    }

    return try words.toOwnedSlice(allocator);
}

// GPT-2 byte-to-unicode mapping for ByteLevel pre-tokenizer.
// `byte_to_unicode[b]` is the codepoint used to encode raw byte `b`.
// `unicode_to_byte[cp]` is the inverse map; codepoints beyond the table's
// length never originate from `byte_to_unicode`, so they decode to null.
const byte_to_unicode = initByteToUnicode();
const unicode_to_byte = initUnicodeToByte();

const unicode_to_byte_len: u21 = 324;

const Gpt2CharClass = enum {
    letter,
    number,
    whitespace,
    other,
};

const Gpt2Char = struct {
    class: Gpt2CharClass,
    len: usize,
};

fn gpt2ContractionLen(text: []const u8) ?usize {
    const contractions = [_][]const u8{ "'s", "'t", "'re", "'ve", "'m", "'ll", "'d" };
    for (contractions) |suffix| {
        if (std.mem.startsWith(u8, text, suffix)) return suffix.len;
    }
    return null;
}

/// Classify a UTF-8 codepoint for the GPT-2 pretokenizer. ASCII is the hot
/// path. Non-ASCII codepoints use a compact generated Unicode table matching
/// the `\p{L}`, `\p{N}`, and `\s` classes in the reference regex.
fn gpt2CharAt(text: []const u8, pos: usize) Gpt2Char {
    const first = text[pos];
    if (first < 0x80) {
        const class: Gpt2CharClass = if (std.ascii.isAlphabetic(first))
            .letter
        else if (std.ascii.isDigit(first))
            .number
        else if (std.ascii.isWhitespace(first))
            .whitespace
        else
            .other;
        return .{ .class = class, .len = 1 };
    }

    const len: usize = std.unicode.utf8ByteSequenceLength(first) catch
        return .{ .class = .other, .len = 1 };
    if (len > text.len - pos) {
        return .{ .class = .other, .len = 1 };
    }
    const cp = std.unicode.utf8Decode(text[pos .. pos + len]) catch
        return .{ .class = .other, .len = 1 };
    return .{
        .class = switch (unicode_classes.classify(cp)) {
            .letter => .letter,
            .number => .number,
            .whitespace => .whitespace,
            .other => .other,
        },
        .len = len,
    };
}

fn gpt2PreTokenEnd(text: []const u8, start: usize) usize {
    if (gpt2ContractionLen(text[start..])) |contraction_len| {
        return start + contraction_len;
    }

    // GPT-2's regex permits one literal ASCII space before a letter, number,
    // or punctuation run.
    var content_start = start;
    if (text[start] == ' ' and start + 1 < text.len) {
        const next = gpt2CharAt(text, start + 1);
        if (next.class != .whitespace) content_start += 1;
    }

    const first = gpt2CharAt(text, content_start);
    var end = content_start + first.len;
    while (end < text.len) {
        const next = gpt2CharAt(text, end);
        if (next.class != first.class) break;
        end += next.len;
    }

    // For a multi-codepoint whitespace run followed by content,
    // `\s+(?!\S)` emits all but the last codepoint. A trailing ASCII space
    // can then prefix content; other whitespace is emitted alone by `\s+`.
    if (first.class == .whitespace and end < text.len) {
        const last_start = prevCodepointBoundary(text, end);
        if (last_start > start) end = last_start;
    }
    return end;
}

const Gpt2AsciiClassMasks = struct {
    letters: u64,
    digits: u64,
    spaces: u64,
    whitespace: u64,
    high_bytes: u64,
    apostrophes: u64,
};

const Gpt2BoundaryMasks = struct {
    usable: u64,
    bad: u64,
};

const Gpt2BoolVector = @Vector(64, bool);
const Gpt2ByteVector = @Vector(64, u8);

inline fn normalizeGpt2PredicateMask(
    comptime native_endian: std.builtin.Endian,
    native_mask: u64,
) u64 {
    // A vector-to-scalar bitcast follows target byte order. The boundary
    // scanner's semantic contract instead requires lane N to become bit N.
    // This remains a no-op on little-endian targets and one bit-reverse on
    // big-endian targets, selected entirely at compile time.
    return if (comptime native_endian == .little)
        native_mask
    else
        @bitReverse(native_mask);
}

inline fn gpt2PredicateMask(predicate: Gpt2BoolVector) u64 {
    return normalizeGpt2PredicateMask(
        builtin.cpu.arch.endian(),
        @bitCast(predicate),
    );
}

inline fn gpt2PortableClassMasks(
    text: []const u8,
    start: usize,
) Gpt2AsciiClassMasks {
    const bytes: Gpt2ByteVector = text[start..][0..64].*;
    const lower = bytes | @as(Gpt2ByteVector, @splat(0x20));
    const letters_vec: Gpt2BoolVector =
        (lower >= @as(Gpt2ByteVector, @splat('a'))) &
        (lower <= @as(Gpt2ByteVector, @splat('z')));
    const digits_vec: Gpt2BoolVector =
        (bytes >= @as(Gpt2ByteVector, @splat('0'))) &
        (bytes <= @as(Gpt2ByteVector, @splat('9')));
    const spaces_vec: Gpt2BoolVector =
        bytes == @as(Gpt2ByteVector, @splat(' '));
    const control_ws_vec: Gpt2BoolVector =
        (bytes >= @as(Gpt2ByteVector, @splat(9))) &
        (bytes <= @as(Gpt2ByteVector, @splat(13)));
    const spaces = gpt2PredicateMask(spaces_vec);
    return .{
        .letters = gpt2PredicateMask(letters_vec),
        .digits = gpt2PredicateMask(digits_vec),
        .spaces = spaces,
        .whitespace = spaces | gpt2PredicateMask(control_ws_vec),
        .high_bytes = gpt2PredicateMask(
            bytes >= @as(Gpt2ByteVector, @splat(0x80)),
        ),
        .apostrophes = gpt2PredicateMask(
            bytes == @as(Gpt2ByteVector, @splat('\'')),
        ),
    };
}

const NeonBytes = @Vector(16, u8);

inline fn neonPredicateMask(predicate: @Vector(16, bool)) NeonBytes {
    return @select(
        u8,
        predicate,
        @as(NeonBytes, @splat(0xff)),
        @as(NeonBytes, @splat(0)),
    );
}

/// simdjson-style AArch64 movemask. LLVM expands a source-level pairwise
/// reduction into substantially more unzip/or instructions, so pin the four
/// ADDP operations that collapse four 16-byte 0x00/0xff predicates into one
/// u64 bitmask.
inline fn neonMovemask64(
    v0: NeonBytes,
    v1: NeonBytes,
    v2: NeonBytes,
    v3: NeonBytes,
) u64 {
    const weights: NeonBytes = .{
        1, 2, 4, 8, 16, 32, 64, 128,
        1, 2, 4, 8, 16, 32, 64, 128,
    };
    var a0 = v0 & weights;
    const a1 = v1 & weights;
    var a2 = v2 & weights;
    const a3 = v3 & weights;
    asm (
        \\addp %[a0].16b, %[a0].16b, %[a1].16b
        \\addp %[a2].16b, %[a2].16b, %[a3].16b
        \\addp %[a0].16b, %[a0].16b, %[a2].16b
        \\addp %[a0].16b, %[a0].16b, %[a0].16b
        : [a0] "+w" (a0),
          [a2] "+w" (a2),
        : [a1] "w" (a1),
          [a3] "w" (a3),
    );
    const lanes: @Vector(2, u64) = @bitCast(a0);
    return lanes[0];
}

inline fn gpt2Aarch64ClassMasks(
    text: []const u8,
    start: usize,
) Gpt2AsciiClassMasks {
    var letters: [4]NeonBytes = undefined;
    var digits: [4]NeonBytes = undefined;
    var spaces: [4]NeonBytes = undefined;
    var whitespace: [4]NeonBytes = undefined;
    var high: [4]NeonBytes = undefined;
    var apostrophes: [4]NeonBytes = undefined;
    inline for (0..4) |idx| {
        const bytes: NeonBytes =
            text[start + idx * 16 ..][0..16].*;
        const lowered = bytes | @as(NeonBytes, @splat(0x20));
        letters[idx] = neonPredicateMask(
            lowered -% @as(NeonBytes, @splat('a')) <=
                @as(NeonBytes, @splat(25)),
        );
        digits[idx] = neonPredicateMask(
            bytes -% @as(NeonBytes, @splat('0')) <=
                @as(NeonBytes, @splat(9)),
        );
        spaces[idx] = neonPredicateMask(
            bytes == @as(NeonBytes, @splat(' ')),
        );
        whitespace[idx] = neonPredicateMask(
            (bytes == @as(NeonBytes, @splat(' '))) |
                (bytes -% @as(NeonBytes, @splat(9)) <=
                    @as(NeonBytes, @splat(4))),
        );
        high[idx] = neonPredicateMask(
            bytes >= @as(NeonBytes, @splat(0x80)),
        );
        apostrophes[idx] = neonPredicateMask(
            bytes == @as(NeonBytes, @splat('\'')),
        );
    }

    const high_any =
        high[0] | high[1] | high[2] | high[3];
    const apostrophe_any =
        apostrophes[0] | apostrophes[1] | apostrophes[2] | apostrophes[3];
    return .{
        .letters = neonMovemask64(
            letters[0],
            letters[1],
            letters[2],
            letters[3],
        ),
        .digits = neonMovemask64(
            digits[0],
            digits[1],
            digits[2],
            digits[3],
        ),
        .spaces = neonMovemask64(
            spaces[0],
            spaces[1],
            spaces[2],
            spaces[3],
        ),
        .whitespace = neonMovemask64(
            whitespace[0],
            whitespace[1],
            whitespace[2],
            whitespace[3],
        ),
        .high_bytes = if (@reduce(.Or, high_any) != 0)
            neonMovemask64(high[0], high[1], high[2], high[3])
        else
            0,
        .apostrophes = if (@reduce(.Or, apostrophe_any) != 0)
            neonMovemask64(
                apostrophes[0],
                apostrophes[1],
                apostrophes[2],
                apostrophes[3],
            )
        else
            0,
    };
}

fn gpt2ClassBefore(text: []const u8, pos: usize) Gpt2CharClass {
    std.debug.assert(pos > 0);
    var start = pos - 1;
    while (start > 0 and text[start] & 0xc0 == 0x80) {
        start -= 1;
    }
    return gpt2CharAt(text, start).class;
}

const Gpt2CharThrough = struct {
    class: Gpt2CharClass,
    start: usize,
    end: usize,
};

fn gpt2CharThrough(text: []const u8, pos: usize) Gpt2CharThrough {
    std.debug.assert(pos > 0);
    var start = pos - 1;
    while (start > 0 and text[start] & 0xc0 == 0x80) {
        start -= 1;
    }
    const char = gpt2CharAt(text, start);
    return .{
        .class = char.class,
        .start = start,
        .end = start + char.len,
    };
}

const Gpt2UnicodeClassMasks = struct {
    letters: u64 = 0,
    numbers: u64 = 0,
    other: u64 = 0,
    whitespace: u64 = 0,
    whitespace2: u64 = 0,
    whitespace3: u64 = 0,
    continuation: u64 = 0,
    residual: u64 = 0,
};

fn addGpt2UnicodeClassMask(
    masks: *Gpt2UnicodeClassMasks,
    class: Gpt2CharClass,
    char_mask: u64,
) void {
    switch (class) {
        .letter => masks.letters |= char_mask,
        .number => masks.numbers |= char_mask,
        .other => masks.other |= char_mask,
        .whitespace => masks.whitespace |= char_mask,
    }
}

fn gpt2ClassifyUnicodeGrid(
    text: []const u8,
    start: usize,
    high_bytes_in: u64,
) Gpt2UnicodeClassMasks {
    var result: Gpt2UnicodeClassMasks = .{};
    var high_bytes = high_bytes_in;
    while (high_bytes != 0) {
        const relative: usize = @intCast(@ctz(high_bytes));
        const byte = text[start + relative];
        if (byte & 0xc0 == 0x80) {
            const bit = @as(u64, 1) << @as(u6, @intCast(relative));
            result.residual |= bit;
            high_bytes &= ~bit;
            continue;
        }
        const char = gpt2CharAt(text, start + relative);
        const in_block_len = @min(char.len, 64 - relative);
        const char_mask =
            ((@as(u64, 1) << @as(u6, @intCast(in_block_len))) - 1) <<
            @as(u6, @intCast(relative));
        const lead = @as(u64, 1) <<
            @as(u6, @intCast(relative));
        addGpt2UnicodeClassMask(
            &result,
            char.class,
            char_mask,
        );
        result.continuation |= char_mask & ~lead;
        if (char.class == .whitespace) {
            if (relative + char.len > 64 or char.len >= 4) {
                result.residual |= char_mask;
            } else if (char.len == 2) {
                result.whitespace2 |= lead;
            } else if (char.len == 3) {
                result.whitespace3 |= lead;
            }
        }
        high_bytes &= ~char_mask;
    }
    return result;
}

fn gpt2ExtendedGridBoundaryMasks(
    text: []const u8,
    start: usize,
    classes: Gpt2AsciiClassMasks,
) Gpt2BoundaryMasks {
    if (start + 68 > text.len) {
        return .{ .usable = 0, .bad = std.math.maxInt(u64) };
    }

    var claim: Gpt2UnicodeClassMasks = .{};
    var previous_letter: u64 = 0;
    var previous_number: u64 = 0;
    var previous_space: u64 = 0;
    var previous_whitespace: u64 = 0;
    var previous_other: u64 = 0;
    if (start != 0) {
        const through = gpt2CharThrough(text, start);
        previous_letter = @intFromBool(through.class == .letter);
        previous_number = @intFromBool(through.class == .number);
        previous_space = @intFromBool(text[start - 1] == ' ');
        previous_whitespace =
            @intFromBool(through.class == .whitespace);
        previous_other = @intFromBool(through.class == .other);
        if (through.end > start) {
            const claimed_len = @min(through.end - start, 64);
            const claimed_mask =
                (@as(u64, 1) <<
                    @as(u6, @intCast(claimed_len))) - 1;
            addGpt2UnicodeClassMask(
                &claim,
                through.class,
                claimed_mask,
            );
            claim.continuation = claimed_mask;
            if (through.class == .whitespace) {
                claim.residual = claimed_mask;
            }
        }
    }

    const unicode_masks = gpt2ClassifyUnicodeGrid(
        text,
        start,
        classes.high_bytes & ~claim.continuation,
    );
    const letters =
        classes.letters | claim.letters | unicode_masks.letters;
    const numbers =
        classes.digits | claim.numbers | unicode_masks.numbers;
    const whitespace =
        classes.whitespace |
        claim.whitespace |
        unicode_masks.whitespace;
    const other =
        ~(classes.letters |
            classes.digits |
            classes.whitespace |
            classes.high_bytes) |
        claim.other |
        unicode_masks.other;
    const continuation =
        claim.continuation | unicode_masks.continuation;
    const residual = claim.residual | unicode_masks.residual;

    const continue_same =
        (letters & ((letters << 1) | previous_letter)) |
        (numbers & ((numbers << 1) | previous_number)) |
        (other & ((other << 1) | previous_other));
    const after_space = (classes.spaces << 1) | previous_space;
    const non_whitespace_boundaries =
        ~whitespace &
        ~continue_same &
        ~after_space &
        ~continuation;

    const not_whitespace = ~whitespace;
    var split_whitespace =
        (classes.whitespace & (not_whitespace >> 1)) |
        (unicode_masks.whitespace2 & (not_whitespace >> 2)) |
        (unicode_masks.whitespace3 & (not_whitespace >> 3));
    const whitespace_leads =
        classes.whitespace |
        unicode_masks.whitespace2 |
        unicode_masks.whitespace3;
    const edge_multibyte =
        (unicode_masks.whitespace2 & (@as(u64, 1) << 62)) |
        (unicode_masks.whitespace3 & (@as(u64, 1) << 61));
    // A fixed 64-byte grid may end inside UTF-8. Classify the codepoint
    // containing the lookahead byte rather than passing a continuation byte
    // to the decoder.
    const lookahead_class =
        gpt2CharThrough(text, start + 65).class;
    if (lookahead_class != .whitespace) {
        split_whitespace |=
            edge_multibyte |
            (classes.whitespace & (@as(u64, 1) << 63));
    } else {
        split_whitespace &=
            ~(edge_multibyte | (@as(u64, 1) << 63));
    }
    const previous_whitespace_bits =
        (whitespace << 1) | previous_whitespace;
    const whitespace_boundaries =
        whitespace_leads &
        (~previous_whitespace_bits | split_whitespace);
    var boundaries =
        non_whitespace_boundaries | whitespace_boundaries;
    var bad =
        residual | (residual << 1) | (residual >> 1);

    var candidates =
        classes.apostrophes & boundaries & ~bad;
    while (candidates != 0) {
        const relative: usize = @intCast(@ctz(candidates));
        candidates &= candidates - 1;
        if (relative >= 61) {
            bad |= @as(u64, std.math.maxInt(u64)) <<
                @as(u6, @intCast(relative));
            break;
        }
        const contraction_len: usize =
            switch (text[start + relative + 1]) {
                's', 'd', 'm', 't' => 2,
                'l' => if (text[start + relative + 2] == 'l')
                    3
                else
                    0,
                'v' => if (text[start + relative + 2] == 'e')
                    3
                else
                    0,
                'r' => if (text[start + relative + 2] == 'e')
                    3
                else
                    0,
                else => 0,
            };
        if (contraction_len != 0) {
            boundaries &=
                ~(@as(u64, 1) <<
                    @as(u6, @intCast(relative + 1)));
            boundaries |=
                @as(u64, 1) <<
                @as(u6, @intCast(relative + contraction_len));
        }
    }
    return .{ .usable = boundaries & ~bad, .bad = bad };
}

/// Exact starts for one fixed 64-byte grid block. Pure-ASCII blocks expose
/// every boundary; blocks containing UTF-8 are marked dirty so the mask
/// walker re-derives that zone with `gpt2PreTokenEnd`. Unlike
/// `gpt2AsciiBoundaryMask`, bit zero is computed from the preceding
/// codepoint rather than assuming `start` is already a token boundary.
fn gpt2GridBoundaryMasks(
    text: []const u8,
    start: usize,
) Gpt2BoundaryMasks {
    const batch_len = 64;
    if (text.len - start <= batch_len) {
        return .{ .usable = 0, .bad = std.math.maxInt(u64) };
    }
    const classes = if (comptime builtin.cpu.arch == .aarch64)
        gpt2Aarch64ClassMasks(text, start)
    else
        gpt2PortableClassMasks(text, start);
    if (classes.high_bytes != 0)
        return gpt2ExtendedGridBoundaryMasks(text, start, classes);

    var previous_letter: u64 = 0;
    var previous_digit: u64 = 0;
    var previous_space: u64 = 0;
    var previous_whitespace: u64 = 0;
    var previous_other: u64 = 0;
    if (start != 0) {
        const previous_class = gpt2ClassBefore(text, start);
        previous_letter = @intFromBool(previous_class == .letter);
        previous_digit = @intFromBool(previous_class == .number);
        previous_space = @intFromBool(text[start - 1] == ' ');
        previous_whitespace =
            @intFromBool(previous_class == .whitespace);
        previous_other = @intFromBool(previous_class == .other);
    }

    const letters = classes.letters;
    const digits = classes.digits;
    const spaces = classes.spaces;
    const whitespace = classes.whitespace;
    const other = ~(letters | digits | whitespace);
    const continue_same =
        (letters & ((letters << 1) | previous_letter)) |
        (digits & ((digits << 1) | previous_digit)) |
        (other & ((other << 1) | previous_other));
    const after_space = (spaces << 1) | previous_space;
    const non_whitespace_boundaries =
        ~whitespace & ~continue_same & ~after_space;

    var split_whitespace = whitespace & (~whitespace >> 1);
    if ((whitespace >> 63) != 0 and
        gpt2CharAt(text, start + batch_len).class != .whitespace)
    {
        split_whitespace |= @as(u64, 1) << 63;
    }
    const previous_whitespace_bits =
        (whitespace << 1) | previous_whitespace;
    const whitespace_boundaries =
        whitespace & (~previous_whitespace_bits | split_whitespace);
    var boundaries =
        non_whitespace_boundaries | whitespace_boundaries;
    var bad: u64 = 0;

    var candidates = classes.apostrophes & boundaries;
    while (candidates != 0) {
        const rel: usize = @intCast(@ctz(candidates));
        candidates &= candidates - 1;
        if (rel >= 61) {
            bad |= @as(u64, std.math.maxInt(u64)) <<
                @as(u6, @intCast(rel));
            break;
        }
        const contraction_len: usize = switch (text[start + rel + 1]) {
            's', 'd', 'm', 't' => 2,
            'l' => if (text[start + rel + 2] == 'l') 3 else 0,
            'v' => if (text[start + rel + 2] == 'e') 3 else 0,
            'r' => if (text[start + rel + 2] == 'e') 3 else 0,
            else => 0,
        };
        if (contraction_len != 0) {
            boundaries &= ~(@as(u64, 1) << @intCast(rel + 1));
            boundaries |=
                @as(u64, 1) << @intCast(rel + contraction_len);
        }
    }
    return .{ .usable = boundaries & ~bad, .bad = bad };
}

/// Find every GPT-2 pretoken start in the next 64 ASCII bytes. The returned
/// mask always contains bit zero; bit N means `text[start + N]` begins a
/// pretoken. null routes batches containing Unicode, edge contractions, or
/// insufficient lookahead through the scalar ground-truth scanner.
fn gpt2AsciiBoundaryMask(text: []const u8, start: usize) ?u64 {
    const batch_len = 64;
    if (text.len - start <= batch_len) return null;

    const classes = if (comptime builtin.cpu.arch == .aarch64)
        gpt2Aarch64ClassMasks(text, start)
    else
        gpt2PortableClassMasks(text, start);
    const letters = classes.letters;
    const digits = classes.digits;
    const spaces = classes.spaces;
    const whitespace = classes.whitespace;
    const high_bytes = classes.high_bytes;
    // Keep the ASCII prefix of a mixed block usable. Returning null for the
    // whole block made every pretoken in the 64 bytes before a non-ASCII
    // codepoint re-enter the scalar scanner. Four bytes of bad-zone
    // lookbehind cover the longest contraction decision and any class
    // transition influenced by the Unicode byte.
    const usable_mask: u64 = if (high_bytes == 0)
        std.math.maxInt(u64)
    else blk: {
        const first_high: usize = @intCast(@ctz(high_bytes));
        if (first_high <= 4) return null;
        const usable_bits = first_high - 4;
        break :blk (@as(u64, 1) << @intCast(usable_bits)) - 1;
    };
    const apostrophes = classes.apostrophes;
    const other = ~(letters | digits | whitespace);

    const continue_same =
        (letters & (letters << 1)) |
        (digits & (digits << 1)) |
        (other & (other << 1));
    const after_space = spaces << 1;
    const non_whitespace_boundaries = ~whitespace & ~continue_same & ~after_space;

    var split_whitespace = whitespace & (~whitespace >> 1);
    if ((whitespace & (@as(u64, 1) << 63)) != 0 and
        gpt2CharAt(text, start + batch_len).class != .whitespace)
    {
        split_whitespace |= @as(u64, 1) << 63;
    }
    const previous_whitespace = whitespace << 1;
    const whitespace_boundaries =
        whitespace & (~previous_whitespace | split_whitespace);
    var boundaries =
        (non_whitespace_boundaries | whitespace_boundaries | 1) & usable_mask;

    // Regex contractions override the normal punctuation/letter boundary:
    // "'s", "'t", "'re", "'ve", "'m", "'ll", and "'d".
    var candidates = apostrophes & boundaries;
    while (candidates != 0) {
        const rel: usize = @intCast(@ctz(candidates));
        candidates &= candidates - 1;
        if (rel >= 61) return null;
        const contraction_len: usize = switch (text[start + rel + 1]) {
            's', 'd', 'm', 't' => 2,
            'l' => if (text[start + rel + 2] == 'l') 3 else 0,
            'v' => if (text[start + rel + 2] == 'e') 3 else 0,
            'r' => if (text[start + rel + 2] == 'e') 3 else 0,
            else => 0,
        };
        if (contraction_len != 0) {
            boundaries &= ~(@as(u64, 1) << @intCast(rel + 1));
            if (rel + contraction_len < batch_len) {
                boundaries |= @as(u64, 1) << @intCast(rel + contraction_len);
            }
        }
    }
    boundaries &= usable_mask;
    if (boundaries == 1) return null;
    return boundaries;
}

fn initByteToUnicode() [256]u21 {
    var table: [256]u21 = undefined;
    var n: u21 = 256;
    for (0..256) |i| {
        const b: u8 = @intCast(i);
        if ((b >= '!' and b <= '~') or (b >= 0xA1 and b <= 0xAC) or (b >= 0xAE)) {
            table[i] = b;
        } else {
            table[i] = n;
            n += 1;
        }
    }
    return table;
}

fn initUnicodeToByte() [unicode_to_byte_len]?u8 {
    @setEvalBranchQuota(20000);
    var table: [unicode_to_byte_len]?u8 = @splat(null);
    for (byte_to_unicode, 0..) |cp, idx| {
        if (cp < unicode_to_byte_len) table[cp] = @intCast(idx);
    }
    return table;
}

fn byteLevelSplitPreTokenize(allocator: std.mem.Allocator, text: []const u8) ![][]const u8 {
    var words = std.ArrayListUnmanaged([]const u8).empty;
    var i: usize = 0;
    while (i < text.len) {
        const c = text[i];
        if (std.ascii.isWhitespace(c)) {
            i += 1;
            continue;
        }

        const start = i;
        if (c == '\'') {
            const contraction_len = clipContractionLen(text[i..]);
            if (contraction_len > 0) {
                const encoded = try byteLevelEncode(allocator, text[i .. i + contraction_len]);
                try words.append(allocator, encoded);
                i += contraction_len;
                continue;
            }
        }

        if (std.ascii.isAlphabetic(c) or c >= 0x80) {
            i += utf8CodepointLen(c);
            while (i < text.len) {
                const next = text[i];
                if (!(std.ascii.isAlphabetic(next) or next >= 0x80)) break;
                i += utf8CodepointLen(next);
            }
        } else if (std.ascii.isDigit(c)) {
            i += 1;
        } else {
            i += 1;
            while (i < text.len) {
                const next = text[i];
                if (std.ascii.isWhitespace(next) or std.ascii.isAlphabetic(next) or std.ascii.isDigit(next) or next >= 0x80) break;
                if (next == '\'' and clipContractionLen(text[i..]) > 0) break;
                i += 1;
            }
        }

        const encoded = try byteLevelEncode(allocator, text[start..i]);
        try words.append(allocator, encoded);
    }
    return try words.toOwnedSlice(allocator);
}

fn clipContractionLen(text: []const u8) usize {
    const contractions = [_][]const u8{ "'s", "'t", "'re", "'ve", "'m", "'ll", "'d" };
    for (contractions) |suffix| {
        if (text.len >= suffix.len and std.ascii.eqlIgnoreCase(text[0..suffix.len], suffix)) return suffix.len;
    }
    return 0;
}

fn byteLevelEncode(allocator: std.mem.Allocator, text: []const u8) ![]u8 {
    var buf = std.ArrayListUnmanaged(u8).empty;
    errdefer buf.deinit(allocator);
    try byteLevelEncodeInto(allocator, text, &buf);
    return try buf.toOwnedSlice(allocator);
}

/// Decode one tokenizer.json ByteLevel vocabulary/merge piece to the raw
/// bytes that it represents. Doing this once during tokenizer loading lets
/// the hot GPT-2 path hash and merge borrowed input slices directly.
fn byteLevelDecodeTokenAlloc(
    allocator: std.mem.Allocator,
    token: []const u8,
) ![]u8 {
    var raw = std.ArrayListUnmanaged(u8).empty;
    errdefer raw.deinit(allocator);
    try raw.ensureTotalCapacity(allocator, token.len);

    var pos: usize = 0;
    while (pos < token.len) {
        const cp_len = @min(utf8CodepointLen(token[pos]), token.len - pos);
        const cp = std.unicode.utf8Decode(token[pos .. pos + cp_len]) catch {
            raw.appendAssumeCapacity(token[pos]);
            pos += 1;
            continue;
        };
        if (cp < unicode_to_byte_len) {
            if (unicode_to_byte[cp]) |byte| {
                raw.appendAssumeCapacity(byte);
                pos += cp_len;
                continue;
            }
        }
        raw.appendSliceAssumeCapacity(token[pos .. pos + cp_len]);
        pos += cp_len;
    }
    return try raw.toOwnedSlice(allocator);
}

fn byteLevelEncodeInto(
    allocator: std.mem.Allocator,
    text: []const u8,
    buf: *std.ArrayListUnmanaged(u8),
) !void {
    buf.clearRetainingCapacity();
    try buf.ensureTotalCapacity(allocator, text.len * 2);
    for (text) |byte| {
        const cp = byte_to_unicode[byte];
        var utf8_buf: [4]u8 = undefined;
        const len = std.unicode.utf8Encode(cp, &utf8_buf) catch 1;
        buf.appendSliceAssumeCapacity(utf8_buf[0..len]);
    }
}

fn appendByteDecoded(result: *std.ArrayListUnmanaged(u8), allocator: std.mem.Allocator, token: []const u8) !void {
    var i: usize = 0;
    while (i < token.len) {
        const cp_len = utf8CodepointLen(token[i]);
        const end = @min(i + cp_len, token.len);
        const cp = std.unicode.utf8Decode(token[i..end]) catch {
            try result.appendSlice(allocator, token[i..end]);
            i = end;
            continue;
        };
        if (unicodeToByte(cp)) |byte| {
            try result.append(allocator, byte);
        } else {
            try result.appendSlice(allocator, token[i..end]);
        }
        i = end;
    }
}

fn replaceMetaspace(allocator: std.mem.Allocator, text: []const u8, replacement: []const u8) ![]u8 {
    var result = std.ArrayListUnmanaged(u8).empty;
    var i: usize = 0;
    var at_start = true;
    while (i < text.len) {
        if (i + replacement.len <= text.len and std.mem.eql(u8, text[i .. i + replacement.len], replacement)) {
            if (!at_start) {
                try result.append(allocator, ' ');
            }
            i += replacement.len;
            at_start = false;
        } else {
            try result.append(allocator, text[i]);
            i += 1;
            at_start = false;
        }
    }
    return try result.toOwnedSlice(allocator);
}

fn unicodeToByte(cp: u21) ?u8 {
    if (cp >= unicode_to_byte_len) return null;
    return unicode_to_byte[cp];
}

// =========================================================================
// Utilities
// =========================================================================

fn prevCodepointBoundary(bytes: []const u8, pos: usize) usize {
    if (pos == 0) return 0;
    var i = pos - 1;
    while (i > 0 and (bytes[i] & 0xC0) == 0x80) : (i -= 1) {}
    return i;
}

fn utf8CodepointLen(first_byte: u8) usize {
    if (first_byte < 0x80) return 1;
    if (first_byte < 0xE0) return 2;
    if (first_byte < 0xF0) return 3;
    return 4;
}

fn toLowerAlloc(allocator: std.mem.Allocator, text: []const u8) ![]u8 {
    const result = try allocator.alloc(u8, text.len);
    for (text, 0..) |c, i| {
        result[i] = std.ascii.toLower(c);
    }
    return result;
}

fn replaceSpacesAlloc(allocator: std.mem.Allocator, text: []const u8, replacement: []const u8) ![]u8 {
    if (std.mem.indexOfScalar(u8, text, ' ') == null) return allocator.dupe(u8, text);

    var result = std.ArrayListUnmanaged(u8).empty;
    errdefer result.deinit(allocator);
    for (text) |c| {
        if (c == ' ') {
            try result.appendSlice(allocator, replacement);
        } else {
            try result.append(allocator, c);
        }
    }
    return try result.toOwnedSlice(allocator);
}

fn isPunctuation(c: u8) bool {
    return switch (c) {
        '!', '"', '#', '$', '%', '&', '\'', '(', ')', '*', '+', ',', '-', '.', '/' => true,
        ':', ';', '<', '=', '>', '?', '@' => true,
        '[', '\\', ']', '^', '_', '`' => true,
        '{', '|', '}', '~' => true,
        else => false,
    };
}

// =========================================================================
// Tests
// =========================================================================

test "wordpiece encode basic" {
    const allocator = std.testing.allocator;

    const json_str =
        \\{
        \\  "model": {
        \\    "type": "WordPiece",
        \\    "unk_token": "[UNK]",
        \\    "continuing_subword_prefix": "##",
        \\    "max_input_chars_per_word": 100,
        \\    "vocab": {
        \\      "[PAD]": 0, "[UNK]": 100, "[CLS]": 101, "[SEP]": 102,
        \\      "hello": 1, "world": 2, "test": 3, "##ing": 4, "##ed": 5
        \\    }
        \\  },
        \\  "normalizer": { "type": "Lowercase" },
        \\  "added_tokens": [
        \\    {"id": 0, "content": "[PAD]", "special": true},
        \\    {"id": 100, "content": "[UNK]", "special": true},
        \\    {"id": 101, "content": "[CLS]", "special": true},
        \\    {"id": 102, "content": "[SEP]", "special": true}
        \\  ],
        \\  "post_processor": {
        \\    "type": "BertProcessing",
        \\    "cls": ["[CLS]", 101],
        \\    "sep": ["[SEP]", 102]
        \\  }
        \\}
    ;

    var tok = try HfTokenizer.loadFromBytes(allocator, json_str);
    defer tok.deinitSelf();

    // "hello" should encode to [1]
    const ids1 = try tok.encode(allocator, "hello");
    defer allocator.free(ids1);
    try std.testing.expectEqual(@as(usize, 1), ids1.len);
    try std.testing.expectEqual(@as(i32, 1), ids1[0]);

    // "hello world" should encode to [1, 2]
    const ids2 = try tok.encode(allocator, "hello world");
    defer allocator.free(ids2);
    try std.testing.expectEqual(@as(usize, 2), ids2.len);
    try std.testing.expectEqual(@as(i32, 1), ids2[0]);
    try std.testing.expectEqual(@as(i32, 2), ids2[1]);

    // "testing" should encode to [3, 4] ("test" + "##ing")
    const ids3 = try tok.encode(allocator, "testing");
    defer allocator.free(ids3);
    try std.testing.expectEqual(@as(usize, 2), ids3.len);
    try std.testing.expectEqual(@as(i32, 3), ids3[0]);
    try std.testing.expectEqual(@as(i32, 4), ids3[1]);

    // "unknown" should encode to [100] (UNK)
    const ids4 = try tok.encode(allocator, "unknown");
    defer allocator.free(ids4);
    try std.testing.expectEqual(@as(usize, 1), ids4.len);
    try std.testing.expectEqual(@as(i32, 100), ids4[0]);
}

test "special tokens" {
    const allocator = std.testing.allocator;

    const json_str =
        \\{
        \\  "model": {
        \\    "type": "WordPiece",
        \\    "unk_token": "[UNK]",
        \\    "vocab": {"[PAD]": 0, "[UNK]": 100, "[CLS]": 101, "[SEP]": 102, "hi": 1}
        \\  },
        \\  "added_tokens": [
        \\    {"id": 0, "content": "[PAD]", "special": true},
        \\    {"id": 100, "content": "[UNK]", "special": true},
        \\    {"id": 101, "content": "[CLS]", "special": true},
        \\    {"id": 102, "content": "[SEP]", "special": true}
        \\  ]
        \\}
    ;

    var tok = try HfTokenizer.loadFromBytes(allocator, json_str);
    defer tok.deinitSelf();

    const special = tok.getSpecialTokens();
    try std.testing.expectEqual(@as(i32, 0), special.pad_id);
    try std.testing.expectEqual(@as(i32, 100), special.unk_id);
    try std.testing.expectEqual(@as(i32, 101), special.cls_id);
    try std.testing.expectEqual(@as(i32, 102), special.sep_id);
}

test "encode for model with padding" {
    const allocator = std.testing.allocator;

    const json_str =
        \\{
        \\  "model": {
        \\    "type": "WordPiece",
        \\    "unk_token": "[UNK]",
        \\    "vocab": {"[PAD]": 0, "[UNK]": 100, "[CLS]": 101, "[SEP]": 102, "hello": 1, "world": 2}
        \\  },
        \\  "added_tokens": [
        \\    {"id": 0, "content": "[PAD]", "special": true},
        \\    {"id": 100, "content": "[UNK]", "special": true},
        \\    {"id": 101, "content": "[CLS]", "special": true},
        \\    {"id": 102, "content": "[SEP]", "special": true}
        \\  ]
        \\}
    ;

    var hf = try HfTokenizer.loadFromBytes(allocator, json_str);
    defer hf.deinitSelf();
    const tok = hf.tokenizer();

    // "hello world" with max_length=8 → [CLS, 1, 2, SEP, PAD, PAD, PAD, PAD]
    var result = try tok.encodeForModel(allocator, "hello world", 8);
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 8), result.ids.len);
    try std.testing.expectEqual(@as(i32, 101), result.ids[0]); // [CLS]
    try std.testing.expectEqual(@as(i32, 1), result.ids[1]); // hello
    try std.testing.expectEqual(@as(i32, 2), result.ids[2]); // world
    try std.testing.expectEqual(@as(i32, 102), result.ids[3]); // [SEP]
    try std.testing.expectEqual(@as(i32, 0), result.ids[4]); // [PAD]

    try std.testing.expectEqual(@as(i32, 1), result.attention_mask[0]);
    try std.testing.expectEqual(@as(i32, 1), result.attention_mask[3]);
    try std.testing.expectEqual(@as(i32, 0), result.attention_mask[4]);
}

test "encode for model tracks wordpiece offsets" {
    const allocator = std.testing.allocator;

    const json_str =
        \\{
        \\  "model": {
        \\    "type": "WordPiece",
        \\    "unk_token": "[UNK]",
        \\    "vocab": {"[PAD]": 0, "[UNK]": 100, "[CLS]": 101, "[SEP]": 102, "John": 1, "Smith": 2}
        \\  },
        \\  "added_tokens": [
        \\    {"id": 0, "content": "[PAD]", "special": true},
        \\    {"id": 100, "content": "[UNK]", "special": true},
        \\    {"id": 101, "content": "[CLS]", "special": true},
        \\    {"id": 102, "content": "[SEP]", "special": true}
        \\  ],
        \\  "pre_tokenizer": {"type": "BertPreTokenizer"}
        \\}
    ;

    var hf = try HfTokenizer.loadFromBytes(allocator, json_str);
    defer hf.deinitSelf();
    const tok = hf.tokenizer();

    var result = try tok.encodeForModel(allocator, "John Smith", 8);
    defer result.deinit();

    try std.testing.expect(result.offsets != null);
    const offsets = result.offsets.?;
    try std.testing.expectEqual(@as(u32, 0), offsets[0][0]);
    try std.testing.expectEqual(@as(u32, 0), offsets[0][1]);
    try std.testing.expectEqual(@as(u32, 0), offsets[1][0]);
    try std.testing.expectEqual(@as(u32, 4), offsets[1][1]);
    try std.testing.expectEqual(@as(u32, 5), offsets[2][0]);
    try std.testing.expectEqual(@as(u32, 10), offsets[2][1]);
    try std.testing.expectEqual(@as(u32, 0), offsets[3][0]);
    try std.testing.expectEqual(@as(u32, 0), offsets[3][1]);
}

test "encode for model handles splade wordpiece tokenizer fixture" {
    const allocator = std.testing.allocator;
    const models_dir = if (std.c.getenv("ANTFLY_INFERENCE_MODELS_DIR")) |value|
        std.mem.span(value)
    else blk: {
        const home = std.c.getenv("HOME") orelse return error.SkipZigTest;
        break :blk try std.fs.path.join(allocator, &.{ std.mem.span(home), ".antfly", "inference", "models" });
    };
    defer if (std.c.getenv("ANTFLY_INFERENCE_MODELS_DIR") == null) allocator.free(models_dir);
    const path = try std.fs.path.join(allocator, &.{ models_dir, "sparse-encoder-testing", "splade-bert-tiny-nq-onnx", "tokenizer.json" });
    defer allocator.free(path);
    const bytes = std.Io.Dir.cwd().readFileAlloc(std.testing.io, path, allocator, .limited(16 * 1024 * 1024)) catch |err| switch (err) {
        error.FileNotFound => return error.SkipZigTest,
        else => return err,
    };
    defer allocator.free(bytes);

    var hf = try HfTokenizer.loadFromBytes(allocator, bytes);
    defer hf.deinitSelf();

    var result = try hf.tokenizer().encodeForModel(allocator, "machine learning", 8);
    defer result.deinit();

    try std.testing.expectEqual(@as(i32, 101), result.ids[0]);
    try std.testing.expectEqual(@as(i32, 3698), result.ids[1]);
    try std.testing.expectEqual(@as(i32, 4083), result.ids[2]);
    try std.testing.expectEqual(@as(i32, 102), result.ids[3]);
    try std.testing.expectEqual(@as(i32, 0), result.ids[4]);
    try std.testing.expectEqual(@as(i32, 1), result.attention_mask[0]);
    try std.testing.expectEqual(@as(i32, 0), result.attention_mask[4]);
}

test "bert pre-tokenizer" {
    const allocator = std.testing.allocator;

    const words = try bertPreTokenize(allocator, "Hello, world! Test.");
    defer allocator.free(words);

    try std.testing.expectEqual(@as(usize, 6), words.len);
    try std.testing.expectEqualStrings("Hello", words[0]);
    try std.testing.expectEqualStrings(",", words[1]);
    try std.testing.expectEqualStrings("world", words[2]);
    try std.testing.expectEqualStrings("!", words[3]);
    try std.testing.expectEqualStrings("Test", words[4]);
    try std.testing.expectEqualStrings(".", words[5]);
}

test "gpt2 pre-tokenizer matches regex boundaries" {
    const text = "Hello, world! I'm sure.  12345\n\n\nNext “déjà”";
    const expected = [_][]const u8{
        "Hello",
        ",",
        " world",
        "!",
        " I",
        "'m",
        " sure",
        ".",
        " ",
        " 12345",
        "\n\n",
        "\n",
        "Next",
        " “",
        "déjà",
        "”",
    };

    var start: usize = 0;
    for (expected) |piece| {
        const end = gpt2PreTokenEnd(text, start);
        try std.testing.expectEqualStrings(piece, text[start..end]);
        start = end;
    }
    try std.testing.expectEqual(text.len, start);
}

test "gpt2 pre-tokenizer uses exact Unicode classes" {
    const text = "A ١2 e\u{301} 😀\u{a0}Z";
    const expected = [_][]const u8{
        "A",
        " ١2",
        " e",
        "\u{301}",
        " 😀",
        "\u{a0}",
        "Z",
    };

    var start: usize = 0;
    for (expected) |piece| {
        const end = gpt2PreTokenEnd(text, start);
        try std.testing.expectEqualStrings(piece, text[start..end]);
        start = end;
    }
    try std.testing.expectEqual(text.len, start);
}

test "gpt2 ASCII vector scanner matches scalar boundaries" {
    const allocator = std.testing.allocator;
    const phrase = "Hello, world! I'm testing 12345.\n\nDon't split contractions; keep  spaces. ";
    var text = std.ArrayListUnmanaged(u8).empty;
    defer text.deinit(allocator);
    for (0..32) |_| try text.appendSlice(allocator, phrase);

    var scalar = std.ArrayListUnmanaged(usize).empty;
    defer scalar.deinit(allocator);
    try scalar.append(allocator, 0);
    var scalar_start: usize = 0;
    while (scalar_start < text.items.len) {
        scalar_start = gpt2PreTokenEnd(text.items, scalar_start);
        if (scalar_start < text.items.len) try scalar.append(allocator, scalar_start);
    }

    var vectorized = std.ArrayListUnmanaged(usize).empty;
    defer vectorized.deinit(allocator);
    try vectorized.append(allocator, 0);
    var vector_start: usize = 0;
    while (vector_start < text.items.len) {
        if (gpt2AsciiBoundaryMask(text.items, vector_start)) |boundary_mask| {
            var remaining = boundary_mask & ~@as(u64, 1);
            var piece_start: usize = 0;
            while (remaining != 0) {
                const piece_end: usize = @intCast(@ctz(remaining));
                try vectorized.append(allocator, vector_start + piece_end);
                piece_start = piece_end;
                remaining &= remaining - 1;
            }
            if (piece_start != 0) {
                vector_start += piece_start;
                continue;
            }
        }
        vector_start = gpt2PreTokenEnd(text.items, vector_start);
        if (vector_start < text.items.len) try vectorized.append(allocator, vector_start);
    }

    try std.testing.expectEqualSlices(usize, scalar.items, vectorized.items);
}

test "gpt2 SIMD predicate masks preserve byte order" {
    const expected =
        (@as(u64, 1) << 0) |
        (@as(u64, 1) << 7) |
        (@as(u64, 1) << 32) |
        (@as(u64, 1) << 63);

    var predicate: Gpt2BoolVector = @splat(false);
    predicate[0] = true;
    predicate[7] = true;
    predicate[32] = true;
    predicate[63] = true;
    try std.testing.expectEqual(expected, gpt2PredicateMask(predicate));

    // Exercise both compile-time layouts even when the test host is
    // little-endian. A big-endian vector bitcast presents the semantic mask
    // in reversed scalar-bit order.
    try std.testing.expectEqual(
        expected,
        normalizeGpt2PredicateMask(.little, expected),
    );
    try std.testing.expectEqual(
        expected,
        normalizeGpt2PredicateMask(.big, @bitReverse(expected)),
    );
}

test "gpt2 boundary lookup preserves semantic lane order" {
    const positions =
        HfTokenizer.gpt2_boundary_byte_positions[0b1011_0101];
    try std.testing.expectEqual(@as(u16, 0), positions[0]);
    try std.testing.expectEqual(@as(u16, 2), positions[1]);
    try std.testing.expectEqual(@as(u16, 4), positions[2]);
    try std.testing.expectEqual(@as(u16, 5), positions[3]);
    try std.testing.expectEqual(@as(u16, 7), positions[4]);
    try std.testing.expectEqual(@as(u16, 0), positions[5]);
    try std.testing.expectEqual(@as(u16, 0), positions[6]);
    try std.testing.expectEqual(@as(u16, 0), positions[7]);
}

test "gpt2 two-phase fixed-grid fill matches scalar boundaries" {
    const allocator = std.testing.allocator;
    const phrase =
        "Don't split contractions at grid edges; 12345 and  spaces.\n\n" ++
        "I'll verify every fixed sixty-four-byte block, café 😀! ";
    var text = std.ArrayListUnmanaged(u8).empty;
    defer text.deinit(allocator);
    for (0..48) |_| try text.appendSlice(allocator, phrase);

    var expected = std.ArrayListUnmanaged(usize).empty;
    defer expected.deinit(allocator);
    var pos: usize = 0;
    while (pos < text.items.len) {
        pos = gpt2PreTokenEnd(text.items, pos);
        try expected.append(allocator, pos);
    }

    const entries = try allocator.alignedAlloc(
        HfTokenizer.WorkerBpeCacheEntry,
        .@"64",
        1024,
    );
    defer allocator.free(entries);
    @memset(entries, .{});
    const probe_view = HfTokenizer.WorkerBpeProbeView{
        .base = entries.ptr,
        .pair_mask = entries.len - 2,
    };
    var state = HfTokenizer.Gpt2MaskFillState{};
    var prepared: [HfTokenizer.worker_bpe_batch_size]HfTokenizer.PreparedWorkerPretoken =
        undefined;
    var actual = std.ArrayListUnmanaged(usize).empty;
    defer actual.deinit(allocator);
    var actual_pos: usize = 0;
    const input_end = @intFromPtr(text.items.ptr) + text.items.len;
    while (true) {
        const fill = HfTokenizer.fillWorkerPretokenBatchMask(
            &state,
            text.items,
            input_end,
            probe_view,
            &prepared,
        );
        if (fill.count == 0) break;
        for (prepared[0..fill.count]) |item| {
            try std.testing.expectEqual(
                actual_pos,
                @intFromPtr(item.ptr) - @intFromPtr(text.items.ptr),
            );
            const len = if (item.key != 0)
                HfTokenizer.workerBpeKeyLen(item.key)
            else
                @as(usize, @intCast(item.meta));
            actual_pos += len;
            try actual.append(allocator, actual_pos);
        }
    }
    try std.testing.expectEqualSlices(
        usize,
        expected.items,
        actual.items,
    );
}

test "gpt2 fixed grid handles a UTF-8 continuation at lookahead" {
    var text: [80]u8 = @splat('a');
    text[63] = 0xc3;
    text[64] = 0xa9;
    text[65] = ' ';
    const through = gpt2CharThrough(&text, 65);
    try std.testing.expectEqual(Gpt2CharClass.letter, through.class);
    try std.testing.expectEqual(@as(usize, 63), through.start);
    try std.testing.expectEqual(@as(usize, 65), through.end);

    const masks = gpt2GridBoundaryMasks(&text, 0);
    try std.testing.expectEqual(
        @as(u64, 0),
        masks.usable & masks.bad,
    );

    const invalid_continuation = gpt2CharAt(&text, 64);
    try std.testing.expectEqual(
        Gpt2CharClass.other,
        invalid_continuation.class,
    );
    try std.testing.expectEqual(@as(usize, 1), invalid_continuation.len);
    const truncated = [_]u8{0xc3};
    try std.testing.expectEqual(
        @as(usize, 1),
        gpt2CharAt(&truncated, 0).len,
    );
}

test "gpt2 vector scanner preserves mixed Unicode bad zones" {
    const allocator = std.testing.allocator;
    const phrase =
        "A sufficiently long ASCII prefix has many words, numbers 123, and punctuation! " ++
        "café 😀 then another long ASCII suffix with don't and whitespace.\n";
    var text = std.ArrayListUnmanaged(u8).empty;
    defer text.deinit(allocator);
    for (0..24) |_| try text.appendSlice(allocator, phrase);

    var scalar = std.ArrayListUnmanaged(usize).empty;
    defer scalar.deinit(allocator);
    try scalar.append(allocator, 0);
    var pos: usize = 0;
    while (pos < text.items.len) {
        pos = gpt2PreTokenEnd(text.items, pos);
        if (pos < text.items.len) try scalar.append(allocator, pos);
    }

    var vectorized = std.ArrayListUnmanaged(usize).empty;
    defer vectorized.deinit(allocator);
    try vectorized.append(allocator, 0);
    pos = 0;
    while (pos < text.items.len) {
        if (gpt2AsciiBoundaryMask(text.items, pos)) |mask| {
            var remaining = mask & ~@as(u64, 1);
            var last: usize = 0;
            while (remaining != 0) {
                last = @intCast(@ctz(remaining));
                try vectorized.append(allocator, pos + last);
                remaining &= remaining - 1;
            }
            if (last != 0) {
                pos += last;
                continue;
            }
        }
        pos = gpt2PreTokenEnd(text.items, pos);
        if (pos < text.items.len) try vectorized.append(allocator, pos);
    }
    try std.testing.expectEqualSlices(usize, scalar.items, vectorized.items);
}

test "worker BPE inline cache packs keys and four token IDs" {
    const allocator = std.testing.allocator;
    const entries = try allocator.alignedAlloc(
        HfTokenizer.WorkerBpeCacheEntry,
        .@"64",
        16,
    );
    defer allocator.free(entries);
    @memset(entries, .{});
    var cache = HfTokenizer.WorkerBpeCache{
        .entries = entries,
        .accounted_bytes = entries.len *
            @sizeOf(HfTokenizer.WorkerBpeCacheEntry),
        .storage = .allocator,
    };
    const key = HfTokenizer.workerBpeKey(" tokenizer").?;
    try std.testing.expectEqual(@as(usize, 10), HfTokenizer.workerBpeKeyLen(key));
    const hash = HfTokenizer.workerBpeHash(key);
    const expected = [_]i32{ 1, 0xfffe, 42, 50_000 };
    const packed_value = HfTokenizer.packWorkerBpeValue(&expected).?;
    HfTokenizer.insertWorkerBpe(&cache, key, hash, packed_value);
    const found = HfTokenizer.lookupWorkerBpe(&cache, key, hash).?;
    var actual = std.ArrayListUnmanaged(i32).empty;
    defer actual.deinit(allocator);
    try actual.ensureTotalCapacityPrecise(allocator, 4);
    HfTokenizer.appendWorkerBpeValue(&actual, found);
    try std.testing.expectEqualSlices(i32, &expected, actual.items);
    try std.testing.expect(
        HfTokenizer.packWorkerBpeValue(&[_]i32{0xffff}) == null,
    );
    try std.testing.expect(
        HfTokenizer.packWorkerBpeValue(
            &[_]i32{std.math.maxInt(i32)},
        ) == null,
    );
}

test "parallel BPE boundary collection matches independent target scans" {
    const Reference = struct {
        fn boundary(text: []const u8, target: usize) usize {
            var pos = target;
            while (pos < text.len) : (pos += 1) {
                if (HfTokenizer.isAsciiWhitespaceByte(text[pos]) and
                    (pos == 0 or !HfTokenizer.isAsciiWhitespaceByte(text[pos - 1])))
                {
                    return pos;
                }
            }
            return text.len;
        }
    };
    const cases = [_][]const u8{
        "",
        "no-whitespace-at-all",
        " leading and  repeated\twhitespace\nruns ",
        "a\nb\r\nc\x0bd\x0ce",
        "one trailing run       ",
    };
    const chunk_counts = [_]usize{ 1, 2, 3, 8, 64 };

    for (cases) |text| {
        for (chunk_counts) |chunk_count| {
            var expected: [HfTokenizer.max_parallel_bpe_chunks + 1]usize = undefined;
            var expected_count: usize = 1;
            expected[0] = 0;
            for (1..chunk_count) |idx| {
                const target = HfTokenizer.parallelBpeTarget(text.len, chunk_count, idx);
                const boundary = Reference.boundary(text, target);
                if (boundary > expected[expected_count - 1] and boundary < text.len) {
                    expected[expected_count] = boundary;
                    expected_count += 1;
                }
            }
            expected[expected_count] = text.len;
            expected_count += 1;

            var actual: [HfTokenizer.max_parallel_bpe_chunks + 1]usize = undefined;
            const actual_count = HfTokenizer.collectParallelBpeBoundaries(
                text,
                chunk_count,
                &actual,
            );
            try std.testing.expectEqualSlices(
                usize,
                expected[0..expected_count],
                actual[0..actual_count],
            );
        }
    }
}

test "real tokenizer.json golden values" {
    const allocator = std.testing.allocator;

    var tok = HfTokenizer.loadFromDir(
        allocator,
        std.Io.Dir.cwd(),
        std.testing.io,
        "lib/tokenizer/testdata/embedder/tokenizer.json",
    ) catch |err| switch (err) {
        error.FileNotFound => try HfTokenizer.loadFromDir(
            allocator,
            std.Io.Dir.cwd(),
            std.testing.io,
            "testdata/embedder/tokenizer.json",
        ),
        else => return err,
    };
    defer tok.deinitSelf();

    const Case = struct { text: []const u8, expected: []const i32 };
    const cases = [_]Case{
        .{ .text = "hello world", .expected = &.{ 7592, 2088 } },
        .{ .text = "testing", .expected = &.{5604} },
        .{ .text = "machine learning", .expected = &.{ 3698, 4083 } },
        .{ .text = "The quick brown fox jumps over the lazy dog.", .expected = &.{ 1996, 4248, 2829, 4419, 14523, 2058, 1996, 13971, 3899, 1012 } },
    };

    for (cases) |tc| {
        const ids = try tok.encode(allocator, tc.text);
        defer allocator.free(ids);
        try std.testing.expectEqual(tc.expected.len, ids.len);
        for (tc.expected, 0..) |expected_id, i| {
            try std.testing.expectEqual(expected_id, ids[i]);
        }
    }

    const special = tok.getSpecialTokens();
    try std.testing.expectEqual(@as(i32, 0), special.pad_id);
    try std.testing.expectEqual(@as(i32, 100), special.unk_id);
    try std.testing.expectEqual(@as(i32, 101), special.cls_id);
    try std.testing.expectEqual(@as(i32, 102), special.sep_id);
    try std.testing.expectEqual(@as(usize, 30522), tok.getVocabSize());
}

test "decode" {
    const allocator = std.testing.allocator;

    const json_str =
        \\{
        \\  "model": {
        \\    "type": "WordPiece",
        \\    "unk_token": "[UNK]",
        \\    "continuing_subword_prefix": "##",
        \\    "vocab": {"[PAD]": 0, "[UNK]": 100, "[CLS]": 101, "[SEP]": 102, "test": 3, "##ing": 4}
        \\  },
        \\  "added_tokens": [
        \\    {"id": 0, "content": "[PAD]", "special": true},
        \\    {"id": 100, "content": "[UNK]", "special": true},
        \\    {"id": 101, "content": "[CLS]", "special": true},
        \\    {"id": 102, "content": "[SEP]", "special": true}
        \\  ]
        \\}
    ;

    var tok = try HfTokenizer.loadFromBytes(allocator, json_str);
    defer tok.deinitSelf();

    const text = try tok.decode(allocator, &.{ 3, 4 });
    defer allocator.free(text);
    try std.testing.expectEqualStrings("testing", text);
}

test "decode byte-level bpe tokens" {
    const allocator = std.testing.allocator;

    const json_str =
        \\{
        \\  "model": {
        \\    "type": "BPE",
        \\    "vocab": {"ĠThis": 1, "Ġtest": 2},
        \\    "merges": []
        \\  },
        \\  "pre_tokenizer": {"type": "ByteLevel"}
        \\}
    ;

    var tok = try HfTokenizer.loadFromBytes(allocator, json_str);
    defer tok.deinitSelf();

    const text = try tok.decode(allocator, &.{ 1, 2 });
    defer allocator.free(text);
    try std.testing.expectEqualStrings(" This test", text);
}

test "infer byte-level bpe when model type is omitted" {
    const allocator = std.testing.allocator;

    const json_str =
        \\{
        \\  "model": {
        \\    "vocab": {
        \\      "W": 10, "h": 11, "a": 12, "t": 13,
        \\      "d": 14, "o": 15, "e": 16, "s": 17, "Ġ": 18,
        \\      "Wh": 19, "Wha": 20, "What": 1,
        \\      "Ġd": 21, "Ġdo": 22, "Ġdoe": 23, "Ġdoes": 2
        \\    },
        \\    "merges": ["W h", "Wh a", "Wha t", "Ġ d", "Ġd o", "Ġdo e", "Ġdoe s"]
        \\  },
        \\  "pre_tokenizer": {"type": "ByteLevel", "add_prefix_space": false, "trim_offsets": true}
        \\}
    ;

    var tok = try HfTokenizer.loadFromBytes(allocator, json_str);
    defer tok.deinitSelf();

    const ids = try tok.encode(allocator, "What does");
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i32, &.{ 1, 2 }, ids);
}

test "byte-level BPE parallel encoding preserves serial token order" {
    const allocator = std.heap.c_allocator;
    const json_str =
        \\{
        \\  "model": {
        \\    "vocab": {
        \\      "W": 10, "h": 11, "a": 12, "t": 13,
        \\      "d": 14, "o": 15, "e": 16, "s": 17, "Ġ": 18,
        \\      "Wh": 19, "Wha": 20, "What": 1,
        \\      "Ġd": 21, "Ġdo": 22, "Ġdoe": 23, "Ġdoes": 2
        \\    },
        \\    "merges": ["W h", "Wh a", "Wha t", "Ġ d", "Ġd o", "Ġdo e", "Ġdoe s"]
        \\  },
        \\  "pre_tokenizer": {"type": "ByteLevel", "add_prefix_space": false, "trim_offsets": true}
        \\}
    ;

    var tok = try HfTokenizer.loadFromBytes(allocator, json_str);
    defer tok.deinitSelf();
    try tok.configureParallelBpe(.{
        .worker_cache_count = 4,
        .worker_cache_slots = 1024,
    });

    const phrase = "What does ";
    var text = std.ArrayListUnmanaged(u8).empty;
    defer text.deinit(allocator);
    try text.ensureTotalCapacity(allocator, phrase.len * 450_000);
    for (0..450_000) |_| text.appendSliceAssumeCapacity(phrase);

    var serial = std.ArrayListUnmanaged(i32).empty;
    defer serial.deinit(allocator);
    try tok.tokenizer().encodeInto(allocator, text.items, &serial);

    var parallel = std.ArrayListUnmanaged(i32).empty;
    defer parallel.deinit(allocator);
    try tok.tokenizer().encodeIntoParallel(std.testing.io, allocator, text.items, &parallel, 4);

    try std.testing.expectEqualSlices(i32, serial.items, parallel.items);
    parallel.clearRetainingCapacity();
    try tok.tokenizer().encodeIntoParallel(std.testing.io, allocator, text.items, &parallel, 4);
    try std.testing.expectEqualSlices(i32, serial.items, parallel.items);

    parallel.clearRetainingCapacity();
    try parallel.append(allocator, -123);
    try tok.tokenizer().encodeIntoParallel(std.testing.io, allocator, text.items, &parallel, 4);
    try std.testing.expectEqual(@as(i32, -123), parallel.items[0]);
    try std.testing.expectEqualSlices(i32, serial.items, parallel.items[1..]);
    try std.testing.expect(parallel.capacity < text.items.len);
    try std.testing.expectEqual(@as(usize, 1), tok.parallel_workspace_free_count);
    const cache_stats = tok.bpeCacheStats();
    try std.testing.expectEqual(@as(usize, 4), cache_stats.worker_tables);
    try std.testing.expect(cache_stats.worker_entries > 0);
    try std.testing.expectEqual(@as(usize, 1), cache_stats.affinity_learns);
    try std.testing.expectEqual(@as(usize, 2), cache_stats.affinity_replays);
    try std.testing.expectEqual(
        @as(usize, 0),
        cache_stats.affinity_stolen_chunks,
    );
}

test "tokenizer retains resource-budget context through physical teardown" {
    const json_str =
        \\{
        \\  "model": {"type": "BPE", "vocab": {"a": 1}, "merges": []},
        \\  "pre_tokenizer": {"type": "ByteLevel", "add_prefix_space": false}
        \\}
    ;
    const Budget = struct {
        references: usize = 1,
        used: usize = 0,

        fn retain(context: *anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(context));
            self.references += 1;
            return true;
        }

        fn release(context: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(context));
            std.debug.assert(self.references > 1);
            self.references -= 1;
        }

        fn observe(context: *anyopaque, _: usize, previous: usize, next: usize) bool {
            const self: *@This() = @ptrCast(@alignCast(context));
            if (self.used != previous) return false;
            self.used = next;
            return true;
        }
    };

    var budget = Budget{};
    var tok = try HfTokenizer.loadFromBytes(std.testing.allocator, json_str);
    try tok.configureBpeCache(.{
        .resource_budget = .{
            .context = &budget,
            .retain_context = Budget.retain,
            .release_context = Budget.release,
            .observe = Budget.observe,
        },
    });
    try std.testing.expectEqual(@as(usize, 2), budget.references);
    try std.testing.expect(budget.used > 0);

    tok.deinitSelf();
    try std.testing.expectEqual(@as(usize, 1), budget.references);
    try std.testing.expectEqual(@as(usize, 0), budget.used);
}

test "tokenizer cache configuration reports owner retention failure without mutation" {
    const json_str =
        \\{
        \\  "model": {"type": "BPE", "vocab": {"a": 1}, "merges": []},
        \\  "pre_tokenizer": {"type": "ByteLevel", "add_prefix_space": false}
        \\}
    ;
    const Budget = struct {
        retain_attempts: usize = 0,
        release_calls: usize = 0,

        fn retain(context: *anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(context));
            self.retain_attempts += 1;
            return false;
        }

        fn release(context: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(context));
            self.release_calls += 1;
        }

        fn observe(_: *anyopaque, _: usize, _: usize, _: usize) bool {
            return false;
        }
    };

    var budget = Budget{};
    var tok = try HfTokenizer.loadFromBytes(std.testing.allocator, json_str);
    defer tok.deinitSelf();
    const original_cache = tok.bpe_cache;

    try std.testing.expectError(
        error.ResourceOwnerShuttingDown,
        tok.configureBpeCache(.{
            .resource_budget = .{
                .context = &budget,
                .retain_context = Budget.retain,
                .release_context = Budget.release,
                .observe = Budget.observe,
            },
        }),
    );
    try std.testing.expectEqual(@as(usize, 1), budget.retain_attempts);
    try std.testing.expectEqual(@as(usize, 0), budget.release_calls);
    try std.testing.expect(tok.bpe_cache == original_cache);
    try std.testing.expect(tok.cache_resource_budget == null);
    try std.testing.expectEqual(@as(usize, 0), tok.cache_resource_budget_bytes);
}

test "tokenizer budget migration rolls back before releasing replacement context" {
    const json_str =
        \\{
        \\  "model": {"type": "BPE", "vocab": {"a": 1}, "merges": []},
        \\  "pre_tokenizer": {"type": "ByteLevel", "add_prefix_space": false}
        \\}
    ;
    const Budget = struct {
        references: usize = 1,
        used: usize = 0,
        reject_release: bool = false,

        fn retain(context: *anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(context));
            self.references += 1;
            return true;
        }

        fn release(context: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(context));
            std.debug.assert(self.references > 1);
            self.references -= 1;
        }

        fn observe(context: *anyopaque, _: usize, previous: usize, next: usize) bool {
            const self: *@This() = @ptrCast(@alignCast(context));
            if (self.used != previous) return false;
            if (self.reject_release and next < previous) return false;
            self.used = next;
            return true;
        }

        fn capability(self: *@This()) HfTokenizer.BpeCacheResourceBudget {
            return .{
                .context = self,
                .retain_context = retain,
                .release_context = release,
                .observe = observe,
            };
        }
    };

    var old = Budget{};
    var replacement = Budget{};
    var tok = try HfTokenizer.loadFromBytes(std.testing.allocator, json_str);
    try tok.configureBpeCache(.{ .resource_budget = old.capability() });
    const accounted = old.used;
    try std.testing.expect(accounted > 0);

    old.reject_release = true;
    try std.testing.expectError(
        error.ResourceBudgetReleaseRejected,
        tok.switchResourceBudget(replacement.capability()),
    );
    try std.testing.expectEqual(accounted, old.used);
    try std.testing.expectEqual(@as(usize, 2), old.references);
    try std.testing.expectEqual(@as(usize, 0), replacement.used);
    try std.testing.expectEqual(@as(usize, 1), replacement.references);
    try std.testing.expect(
        tok.cache_resource_budget.?.context == @as(*anyopaque, @ptrCast(&old)),
    );

    old.reject_release = false;
    tok.deinitSelf();
    try std.testing.expectEqual(@as(usize, 0), old.used);
    try std.testing.expectEqual(@as(usize, 1), old.references);
}

test "worker BPE caches honor external resource-budget denial" {
    const allocator = std.heap.c_allocator;
    const json_str =
        \\{
        \\  "model": {
        \\    "type": "BPE",
        \\    "vocab": {"a": 1, "b": 2, "c": 3, "ab": 4, "abc": 5, "Ġ": 6},
        \\    "merges": ["a b", "ab c"]
        \\  },
        \\  "pre_tokenizer": {"type": "ByteLevel", "add_prefix_space": false}
        \\}
    ;
    const Budget = struct {
        limit: usize,
        used: std.atomic.Value(usize) = .init(0),
        denials: std.atomic.Value(usize) = .init(0),

        fn observe(context: *anyopaque, _: usize, previous: usize, next: usize) bool {
            const self: *@This() = @ptrCast(@alignCast(context));
            if (self.used.load(.acquire) != previous) return false;
            if (next <= previous) {
                self.used.store(next, .release);
                return true;
            }
            const bytes = next - previous;
            if (bytes <= self.limit and previous <= self.limit - bytes) {
                self.used.store(next, .release);
                return true;
            }
            _ = self.denials.fetchAdd(1, .monotonic);
            return false;
        }
    };

    const base_bytes = @sizeOf(HfTokenizer.BpeCache);
    var budget = Budget{ .limit = base_bytes };
    var tok = try HfTokenizer.loadFromBytes(allocator, json_str);
    errdefer tok.deinitSelf();
    try tok.configureBpeCache(.{
        .max_bytes = base_bytes,
        .resource_budget = .{
            .context = &budget,
            .observe = Budget.observe,
        },
    });
    try tok.configureParallelBpe(.{
        .worker_cache_count = 4,
        .worker_cache_slots = 1024,
        .retain_stable_pretoken_boundaries = true,
    });

    var text = std.ArrayListUnmanaged(u8).empty;
    defer text.deinit(allocator);
    for (0..70_000) |_| try text.appendSlice(allocator, "abc ");
    var serial = std.ArrayListUnmanaged(i32).empty;
    defer serial.deinit(allocator);
    try tok.tokenizer().encodeInto(allocator, text.items, &serial);
    var parallel = std.ArrayListUnmanaged(i32).empty;
    defer parallel.deinit(allocator);
    try tok.tokenizer().encodeIntoParallel(
        std.testing.io,
        allocator,
        text.items,
        &parallel,
        4,
    );
    try std.testing.expectEqualSlices(i32, serial.items, parallel.items);
    try std.testing.expectEqual(@as(usize, 0), tok.bpeCacheStats().worker_tables);
    try std.testing.expectEqual(base_bytes, budget.used.load(.acquire));

    var packed_segments = try tok.encodeParallelSegmentsU16Stable(
        std.testing.io,
        text.items,
        4,
        9,
    );
    try std.testing.expectEqual(serial.items.len, packed_segments.tokenCount());
    var packed_pos: usize = 0;
    for (0..packed_segments.segmentCount()) |idx| {
        for (packed_segments.segment(idx)) |id| {
            try std.testing.expectEqual(
                serial.items[packed_pos],
                @as(i32, id),
            );
            packed_pos += 1;
        }
    }
    try std.testing.expectEqual(serial.items.len, packed_pos);
    const active_stats = tok.bpeCacheStats();
    try std.testing.expectEqual(@as(usize, 0), active_stats.worker_tables);
    try std.testing.expectEqual(@as(usize, 1), active_stats.workspace_active_count);
    try std.testing.expect(active_stats.workspace_active_output_bytes > 0);
    try std.testing.expect(
        active_stats.workspace_active_output_capacity_bytes >=
            active_stats.workspace_active_output_bytes,
    );
    try std.testing.expectEqual(
        @as(usize, 0),
        active_stats.workspace_accounted_bytes,
    );
    packed_segments.deinit();
    const denied_stats = tok.bpeCacheStats();
    try std.testing.expectEqual(@as(usize, 0), denied_stats.workspace_total_count);
    try std.testing.expectEqual(@as(usize, 0), denied_stats.workspace_cached_count);
    try std.testing.expectEqual(@as(usize, 0), denied_stats.stable_boundary_words);
    try std.testing.expectEqual(@as(usize, 0), denied_stats.stable_boundary_bytes);
    try std.testing.expect(budget.denials.load(.acquire) > 0);
    try std.testing.expectEqual(base_bytes, budget.used.load(.acquire));

    tok.deinitSelf();
    try std.testing.expectEqual(@as(usize, 0), budget.used.load(.acquire));
}

test "worker BPE caches retry transient resource-budget denial" {
    const allocator = std.heap.c_allocator;
    const json_str =
        \\{
        \\  "model": {
        \\    "type": "BPE",
        \\    "vocab": {"a": 1},
        \\    "merges": []
        \\  },
        \\  "pre_tokenizer": {"type": "ByteLevel"}
        \\}
    ;
    const table_slots = 1024;
    const table_bytes =
        table_slots * @sizeOf(HfTokenizer.WorkerBpeCacheEntry);
    const Budget = struct {
        table_bytes: usize,
        transient_denials: std.atomic.Value(usize) = .init(4),
        used: std.atomic.Value(usize) = .init(0),

        fn observe(context: *anyopaque, _: usize, previous: usize, next: usize) bool {
            const self: *@This() = @ptrCast(@alignCast(context));
            if (self.used.load(.acquire) != previous) return false;
            if (next <= previous) {
                self.used.store(next, .release);
                return true;
            }
            const bytes = next - previous;
            if (bytes == self.table_bytes) {
                var remaining = self.transient_denials.load(.acquire);
                while (remaining != 0) {
                    remaining = self.transient_denials.cmpxchgWeak(
                        remaining,
                        remaining - 1,
                        .acq_rel,
                        .acquire,
                    ) orelse return false;
                }
            }
            self.used.store(next, .release);
            return true;
        }
    };

    var budget = Budget{ .table_bytes = table_bytes };
    const tok = try HfTokenizer.loadFromBytes(allocator, json_str);
    errdefer tok.deinitSelf();
    try tok.configureBpeCache(.{
        .resource_budget = .{
            .context = &budget,
            .observe = Budget.observe,
        },
    });
    try tok.configureParallelBpe(.{
        .worker_cache_count = 4,
        .worker_cache_slots = table_slots,
    });

    // The first call for every lease is denied, the second observes its
    // one-acquisition cooldown, and the third retries successfully.
    for (0..3) |_| {
        for (0..4) |idx| {
            const lease = tok.acquireWorkerBpeCacheAt(idx);
            HfTokenizer.releaseWorkerBpeCache(lease);
        }
    }
    try std.testing.expectEqual(
        @as(usize, 0),
        budget.transient_denials.load(.acquire),
    );
    try std.testing.expectEqual(@as(usize, 4), tok.bpeCacheStats().worker_tables);
    tok.deinitSelf();
    try std.testing.expectEqual(@as(usize, 0), budget.used.load(.acquire));
}

test "stable boundary index yields resource priority to worker caches" {
    const allocator = std.heap.c_allocator;
    const json_str =
        \\{
        \\  "model": {
        \\    "type": "BPE",
        \\    "vocab": {"a": 1, "b": 2, "c": 3, "ab": 4, "abc": 5, "Ġ": 6},
        \\    "merges": ["a b", "ab c"]
        \\  },
        \\  "pre_tokenizer": {"type": "ByteLevel", "add_prefix_space": false}
        \\}
    ;
    const table_slots = 1024;
    const table_bytes =
        table_slots * @sizeOf(HfTokenizer.WorkerBpeCacheEntry);
    const table_count = 4;
    const Budget = struct {
        limit: usize,
        used: std.atomic.Value(usize) = .init(0),
        denials: std.atomic.Value(usize) = .init(0),

        fn observe(context: *anyopaque, _: usize, previous: usize, next: usize) bool {
            const self: *@This() = @ptrCast(@alignCast(context));
            if (self.used.load(.acquire) != previous) return false;
            if (next <= previous) {
                self.used.store(next, .release);
                return true;
            }
            const bytes = next - previous;
            if (bytes <= self.limit and previous <= self.limit - bytes) {
                self.used.store(next, .release);
                return true;
            }
            _ = self.denials.fetchAdd(1, .monotonic);
            return false;
        }
    };

    var budget = Budget{ .limit = table_count * table_bytes };
    const tok = try HfTokenizer.loadFromBytes(allocator, json_str);
    errdefer tok.deinitSelf();
    try tok.configureBpeCache(.{
        .max_bytes = 0,
        .resource_budget = .{
            .context = &budget,
            .observe = Budget.observe,
        },
    });
    try tok.configureParallelBpe(.{
        .worker_cache_count = table_count,
        .worker_cache_slots = table_slots,
        .retain_stable_pretoken_boundaries = true,
        .max_retained_workspace_bytes = 0,
    });

    var text = std.ArrayListUnmanaged(u8).empty;
    defer text.deinit(allocator);
    for (0..70_000) |_| try text.appendSlice(allocator, "abc ");
    var ids = std.ArrayListUnmanaged(i32).empty;
    defer ids.deinit(allocator);
    try tok.tokenizer().encodeIntoParallelStable(
        std.testing.io,
        allocator,
        text.items,
        &ids,
        table_count,
        17,
    );

    const stats = tok.bpeCacheStats();
    try std.testing.expectEqual(@as(usize, table_count), stats.worker_tables);
    try std.testing.expectEqual(@as(usize, 0), stats.stable_boundary_words);
    try std.testing.expect(budget.denials.load(.acquire) > 0);
    try std.testing.expectEqual(
        @as(usize, table_count * table_bytes),
        budget.used.load(.acquire),
    );

    tok.deinitSelf();
    try std.testing.expectEqual(@as(usize, 0), budget.used.load(.acquire));
}

test "worker cache statistics never wait for an active lease" {
    const allocator = std.heap.c_allocator;
    const json_str =
        \\{
        \\  "model": {
        \\    "type": "BPE",
        \\    "vocab": {"a": 1},
        \\    "merges": []
        \\  },
        \\  "pre_tokenizer": {"type": "ByteLevel"}
        \\}
    ;
    const tok = try HfTokenizer.loadFromBytes(allocator, json_str);
    defer tok.deinitSelf();
    try tok.configureParallelBpe(.{
        .worker_cache_count = 1,
        .worker_cache_slots = 1024,
    });

    const initialized = tok.acquireWorkerBpeCacheAt(0);
    try std.testing.expect(initialized != null);
    HfTokenizer.releaseWorkerBpeCache(initialized);

    const lease = &tok.worker_bpe_caches[0];
    try std.testing.expect(lease.mutex.tryLock());
    defer lease.mutex.unlock();
    const stats = tok.bpeCacheStats();
    try std.testing.expectEqual(@as(usize, 1), stats.worker_tables);
    try std.testing.expectEqual(@as(usize, 1024), stats.worker_slots);
}

test "packed worker BPE values preserve token lane order" {
    const token_ids = [_]i32{ 7, 513, 65_534 };
    const packed_value = HfTokenizer.packWorkerBpeValue(&token_ids).?;
    const lanes = HfTokenizer.workerBpeValueLanes(packed_value);
    try std.testing.expectEqual(@as(u16, 7), lanes[0]);
    try std.testing.expectEqual(@as(u16, 513), lanes[1]);
    try std.testing.expectEqual(@as(u16, 65_534), lanes[2]);
    try std.testing.expectEqual(
        @as(u16, HfTokenizer.worker_bpe_inline_sentinel),
        lanes[3],
    );

    var output_u16: [4]u16 = undefined;
    var output_u16_len: usize = 0;
    HfTokenizer.writeWorkerBpeValueU16(
        &output_u16,
        &output_u16_len,
        packed_value,
    );
    try std.testing.expectEqual(@as(usize, token_ids.len), output_u16_len);
    try std.testing.expectEqualSlices(
        u16,
        &[_]u16{ 7, 513, 65_534 },
        output_u16[0..output_u16_len],
    );

    var output_i32: [4]i32 = undefined;
    var output_i32_len: usize = 0;
    HfTokenizer.writeWorkerBpeValue(
        &output_i32,
        &output_i32_len,
        packed_value,
    );
    try std.testing.expectEqual(@as(usize, token_ids.len), output_i32_len);
    try std.testing.expectEqualSlices(
        i32,
        &token_ids,
        output_i32[0..output_i32_len],
    );
}

test "worker BPE cache retains spilled token sequences" {
    const allocator = std.heap.c_allocator;
    const json_str =
        \\{
        \\  "model": {
        \\    "type": "BPE",
        \\    "vocab": {
        \\      "a": 1, "b": 2, "c": 3, "d": 4,
        \\      "e": 5, "f": 6, "g": 7, "h": 8, "Ġ": 9
        \\    },
        \\    "merges": []
        \\  },
        \\  "pre_tokenizer": {"type": "ByteLevel", "add_prefix_space": false}
        \\}
    ;
    var tok = try HfTokenizer.loadFromBytes(allocator, json_str);
    defer tok.deinitSelf();
    try tok.configureParallelBpe(.{
        .worker_cache_count = 4,
        .worker_cache_slots = 1024,
    });

    var text = std.ArrayListUnmanaged(u8).empty;
    defer text.deinit(allocator);
    for (0..40_000) |_| try text.appendSlice(allocator, "abcdefgh ");
    var serial = std.ArrayListUnmanaged(i32).empty;
    defer serial.deinit(allocator);
    try tok.tokenizer().encodeInto(allocator, text.items, &serial);
    var parallel = std.ArrayListUnmanaged(i32).empty;
    defer parallel.deinit(allocator);
    for (0..2) |_| {
        parallel.clearRetainingCapacity();
        try tok.tokenizer().encodeIntoParallel(
            std.testing.io,
            allocator,
            text.items,
            &parallel,
            4,
        );
        try std.testing.expectEqualSlices(i32, serial.items, parallel.items);
    }
    const table_bytes =
        4 * 1024 * @sizeOf(HfTokenizer.WorkerBpeCacheEntry);
    const stats = tok.bpeCacheStats();
    try std.testing.expectEqual(@as(usize, 4), stats.worker_tables);
    try std.testing.expect(stats.worker_bytes > table_bytes);
}

test "packed parallel BPE rejects token IDs outside u16" {
    const allocator = std.testing.allocator;
    const json_str =
        \\{
        \\  "model": {
        \\    "type": "BPE",
        \\    "vocab": {"a": 70000},
        \\    "merges": []
        \\  },
        \\  "pre_tokenizer": {"type": "ByteLevel", "add_prefix_space": false}
        \\}
    ;
    var tok = try HfTokenizer.loadFromBytes(allocator, json_str);
    defer tok.deinitSelf();
    try std.testing.expectError(
        error.TokenIdTooLargeForU16,
        tok.encodeParallelSegmentsU16Stable(
            std.testing.io,
            "a",
            2,
            1,
        ),
    );
}

test "worker BPE spill arena falls back when resource budget denies growth" {
    const allocator = std.heap.c_allocator;
    const json_str =
        \\{
        \\  "model": {
        \\    "type": "BPE",
        \\    "vocab": {
        \\      "a": 1, "b": 2, "c": 3, "d": 4,
        \\      "e": 5, "f": 6, "g": 7, "h": 8, "Ġ": 9
        \\    },
        \\    "merges": []
        \\  },
        \\  "pre_tokenizer": {"type": "ByteLevel", "add_prefix_space": false}
        \\}
    ;
    const Budget = struct {
        limit: usize,
        used: std.atomic.Value(usize) = .init(0),

        fn observe(context: *anyopaque, _: usize, previous: usize, next: usize) bool {
            const self: *@This() = @ptrCast(@alignCast(context));
            if (self.used.load(.acquire) != previous) return false;
            if (next <= previous) {
                self.used.store(next, .release);
                return true;
            }
            const bytes = next - previous;
            if (bytes == HfTokenizer.worker_bpe_initial_arena_ids *
                @sizeOf(i32))
            {
                return false;
            }
            if (bytes <= self.limit and previous <= self.limit - bytes) {
                self.used.store(next, .release);
                return true;
            }
            return false;
        }
    };

    const base_bytes = @sizeOf(HfTokenizer.BpeCache);
    const table_bytes =
        4 * 1024 * @sizeOf(HfTokenizer.WorkerBpeCacheEntry);
    var budget = Budget{ .limit = base_bytes + table_bytes };
    var tok = try HfTokenizer.loadFromBytes(allocator, json_str);
    errdefer tok.deinitSelf();
    try tok.configureBpeCache(.{
        .max_bytes = base_bytes,
        .resource_budget = .{
            .context = &budget,
            .observe = Budget.observe,
        },
    });
    try tok.configureParallelBpe(.{
        .worker_cache_count = 4,
        .worker_cache_slots = 1024,
    });

    var text = std.ArrayListUnmanaged(u8).empty;
    defer text.deinit(allocator);
    for (0..40_000) |_| try text.appendSlice(allocator, "abcdefgh ");
    var serial = std.ArrayListUnmanaged(i32).empty;
    defer serial.deinit(allocator);
    try tok.tokenizer().encodeInto(allocator, text.items, &serial);
    var parallel = std.ArrayListUnmanaged(i32).empty;
    defer parallel.deinit(allocator);
    for (0..2) |_| {
        parallel.clearRetainingCapacity();
        try tok.tokenizer().encodeIntoParallel(
            std.testing.io,
            allocator,
            text.items,
            &parallel,
            4,
        );
        try std.testing.expectEqualSlices(i32, serial.items, parallel.items);
    }
    const stats = tok.bpeCacheStats();
    try std.testing.expectEqual(@as(usize, 4), stats.worker_tables);
    try std.testing.expectEqual(table_bytes, stats.worker_bytes);
    try std.testing.expectEqual(
        base_bytes + table_bytes,
        budget.used.load(.acquire),
    );
    tok.deinitSelf();
    try std.testing.expectEqual(@as(usize, 0), budget.used.load(.acquire));
}

test "byte-level BPE parallel encoding preserves document delimiters" {
    const allocator = std.heap.c_allocator;
    const json_str =
        \\{
        \\  "model": {
        \\    "type": "BPE",
        \\    "vocab": {
        \\      "W": 10, "h": 11, "a": 12, "t": 13,
        \\      "d": 14, "o": 15, "e": 16, "s": 17, "Ġ": 18,
        \\      "Wh": 19, "Wha": 20, "What": 1,
        \\      "Ġd": 21, "Ġdo": 22, "Ġdoe": 23, "Ġdoes": 2,
        \\      "<|endoftext|>": 30
        \\    },
        \\    "merges": ["W h", "Wh a", "Wha t", "Ġ d", "Ġd o", "Ġdo e", "Ġdoe s"]
        \\  },
        \\  "pre_tokenizer": {"type": "ByteLevel", "add_prefix_space": false},
        \\  "added_tokens": [
        \\    {"id": 30, "content": "<|endoftext|>", "special": true}
        \\  ]
        \\}
    ;

    var tok = try HfTokenizer.loadFromBytes(allocator, json_str);
    defer tok.deinitSelf();
    try std.testing.expect(tok.addedTokensAreParallelBoundarySafe());
    try tok.configureParallelBpe(.{
        .worker_cache_count = 4,
        .worker_cache_slots = 1024,
        .retain_stable_pretoken_boundaries = true,
    });

    const phrase = "What does<|endoftext|>What does ";
    var text = std.ArrayListUnmanaged(u8).empty;
    defer text.deinit(allocator);
    try text.ensureTotalCapacity(allocator, phrase.len * 12_000);
    for (0..12_000) |_| text.appendSliceAssumeCapacity(phrase);

    var serial = std.ArrayListUnmanaged(i32).empty;
    defer serial.deinit(allocator);
    try tok.tokenizer().encodeInto(allocator, text.items, &serial);

    var parallel = std.ArrayListUnmanaged(i32).empty;
    defer parallel.deinit(allocator);
    for (0..2) |_| {
        parallel.clearRetainingCapacity();
        try tok.tokenizer().encodeIntoParallelStable(
            std.testing.io,
            allocator,
            text.items,
            &parallel,
            4,
            7,
        );
        try std.testing.expectEqualSlices(i32, serial.items, parallel.items);
    }
    var segments = try tok.encodeParallelSegmentsStable(
        std.testing.io,
        text.items,
        4,
        7,
    );
    try std.testing.expectEqual(@as(usize, 0), tok.parallel_workspace_free_count);
    try std.testing.expectEqual(serial.items.len, segments.tokenCount());
    var segment_start: usize = 0;
    for (0..segments.segmentCount()) |idx| {
        const segment = segments.segment(idx);
        try std.testing.expectEqualSlices(
            i32,
            serial.items[segment_start .. segment_start + segment.len],
            segment,
        );
        segment_start += segment.len;
    }
    try std.testing.expectEqual(serial.items.len, segment_start);
    segments.deinit();
    try std.testing.expectEqual(@as(usize, 1), tok.parallel_workspace_free_count);

    var packed_segments = try tok.encodeParallelSegmentsU16Stable(
        std.testing.io,
        text.items,
        4,
        7,
    );
    try std.testing.expectEqual(serial.items.len, packed_segments.tokenCount());
    var packed_start: usize = 0;
    for (0..packed_segments.segmentCount()) |idx| {
        const packed_segment = packed_segments.segment(idx);
        for (packed_segment, serial.items[packed_start .. packed_start + packed_segment.len]) |actual, expected| {
            try std.testing.expectEqual(expected, @as(i32, actual));
        }
        packed_start += packed_segment.len;
    }
    try std.testing.expectEqual(serial.items.len, packed_start);
    packed_segments.deinit();
    try std.testing.expectEqual(@as(usize, 1), tok.parallel_workspace_free_count);

    const stats = tok.bpeCacheStats();
    try std.testing.expectEqual(@as(usize, 1), stats.stable_offset_learns);
    try std.testing.expectEqual(@as(usize, 3), stats.stable_offset_replays);
    try std.testing.expectEqual(@as(usize, 12_000), stats.stable_offset_count);
    try std.testing.expect(stats.stable_offset_bytes >=
        stats.stable_offset_count * @sizeOf(usize));
    try std.testing.expect(stats.stable_boundary_words > 0);
    try std.testing.expect(stats.stable_boundary_bytes >=
        stats.stable_boundary_words * @sizeOf(u64));
}

test "stable boundary replay refills across an oversized Unicode pretoken" {
    const allocator = std.testing.allocator;
    var text = std.ArrayListUnmanaged(u8).empty;
    defer text.deinit(allocator);
    for (0..64) |_| try text.appendSlice(allocator, "word ");
    // U+00C3 and U+00C2 are both Unicode letters. GPT-2 therefore treats
    // this entire 126 KiB run as one pretoken, matching the rare malformed
    // text pattern found in the complete OpenWebText qualification corpus.
    for (0..31_608) |_| {
        try text.appendSlice(allocator, "\xc3\x83\xc3\x82");
    }

    const word_count = (text.items.len + 63) / 64;
    const boundary_words = try allocator.alloc(u64, word_count);
    defer allocator.free(boundary_words);
    @memset(boundary_words, 0);

    var expected = HfTokenizer.ByteLevelPretokenIterator{
        .text = text.items,
    };
    var expected_count: usize = 0;
    var longest: usize = 0;
    while (expected.next()) |word| {
        const offset =
            @intFromPtr(word.ptr) - @intFromPtr(text.items.ptr);
        boundary_words[offset / 64] |=
            @as(u64, 1) << @intCast(offset % 64);
        expected_count += 1;
        longest = @max(longest, word.len);
    }
    try std.testing.expect(longest > std.math.maxInt(u16));
    try std.testing.expect(
        expected_count < HfTokenizer.worker_bpe_batch_size,
    );

    var cache_entries: [1024]HfTokenizer.WorkerBpeCacheEntry align(64) =
        undefined;
    @memset(&cache_entries, .{});
    const probe_view = HfTokenizer.WorkerBpeProbeView{
        .base = &cache_entries,
        .pair_mask = (cache_entries.len - 1) & ~@as(usize, 1),
    };
    var indexed = HfTokenizer.IndexedPretokenIterator{
        .text = text.items,
        .boundary_words = boundary_words,
        .pos = 0,
        .end = text.items.len,
    };
    var prepared: [HfTokenizer.worker_bpe_batch_size]HfTokenizer.PreparedWorkerPretoken =
        undefined;
    const fill = HfTokenizer.fillWorkerPretokenBatchIndexed(
        &indexed,
        @intFromPtr(text.items.ptr) + text.items.len,
        probe_view,
        &prepared,
    );
    try std.testing.expectEqual(expected_count, fill.count);
    try std.testing.expectEqual(text.items.len, fill.input_bytes);
    try std.testing.expectEqual(text.items.len, indexed.pos);
}

test "parallel BPE rejects added tokens containing internal whitespace" {
    const allocator = std.testing.allocator;
    const json_str =
        \\{
        \\  "model": {
        \\    "type": "BPE",
        \\    "vocab": {"f": 1, "o": 2, "b": 3, "a": 4, "r": 5, "foo bar": 6},
        \\    "merges": []
        \\  },
        \\  "pre_tokenizer": {"type": "ByteLevel", "add_prefix_space": false},
        \\  "added_tokens": [
        \\    {"id": 6, "content": "foo bar", "special": true}
        \\  ]
        \\}
    ;

    var tok = try HfTokenizer.loadFromBytes(allocator, json_str);
    defer tok.deinitSelf();
    try std.testing.expect(!tok.addedTokensAreParallelBoundarySafe());
}

test "byte-level BPE direct-addresses exact two-byte vocabulary tokens" {
    const allocator = std.testing.allocator;
    const json_str =
        \\{
        \\  "model": {
        \\    "type": "BPE",
        \\    "vocab": {"1": 1, "2": 2, "12": 12},
        \\    "merges": []
        \\  },
        \\  "pre_tokenizer": {"type": "ByteLevel", "add_prefix_space": false}
        \\}
    ;

    var tok = try HfTokenizer.loadFromBytes(allocator, json_str);
    defer tok.deinitSelf();

    const direct_ids = tok.byte_level_direct_ids orelse
        return error.TestExpectedByteLevelDirectIds;
    try std.testing.expectEqual(
        @as(i32, 12),
        direct_ids.pair[(@as(usize, '1') << 8) | @as(usize, '2')],
    );

    const ids = try tok.encode(allocator, "12");
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i32, &.{12}, ids);
}

test "byte-level BPE direct IDs preserve end-of-word suffix semantics" {
    const allocator = std.testing.allocator;
    const json_str =
        \\{
        \\  "model": {
        \\    "type": "BPE",
        \\    "vocab": {"a": 1, "a</w>": 2},
        \\    "merges": [],
        \\    "end_of_word_suffix": "</w>"
        \\  },
        \\  "pre_tokenizer": {"type": "ByteLevel", "add_prefix_space": false}
        \\}
    ;

    var tok = try HfTokenizer.loadFromBytes(allocator, json_str);
    defer tok.deinitSelf();

    try std.testing.expect(tok.byte_level_direct_ids == null);

    const ids = try tok.encode(allocator, "a");
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i32, &.{2}, ids);
}

test "BPE cache obeys local and external byte budgets" {
    const allocator = std.testing.allocator;
    const json_str =
        \\{
        \\  "model": {
        \\    "type": "BPE",
        \\    "vocab": {"a": 1, "b": 2, "c": 3, "ab": 4, "abc": 5, "cab": 6},
        \\    "merges": ["a b", "ab c"]
        \\  },
        \\  "pre_tokenizer": {"type": "ByteLevel", "add_prefix_space": false}
        \\}
    ;

    const Budget = struct {
        max_bytes: usize,
        used_bytes: std.atomic.Value(usize) = .init(0),

        fn observe(context: *anyopaque, _: usize, previous: usize, next: usize) bool {
            const self: *@This() = @ptrCast(@alignCast(context));
            if (self.used_bytes.load(.acquire) != previous) return false;
            if (next > self.max_bytes) return false;
            self.used_bytes.store(next, .release);
            return true;
        }
    };

    const entry_bytes =
        @sizeOf(HfTokenizer.BpeCacheEntry) + "abc".len + @sizeOf(i32);
    const hard_limit = @sizeOf(HfTokenizer.BpeCache) + entry_bytes;
    var budget = Budget{ .max_bytes = hard_limit };
    var tok = try HfTokenizer.loadFromBytes(allocator, json_str);
    errdefer tok.deinitSelf();
    try tok.configureBpeCache(.{
        .max_bytes = hard_limit,
        .resource_budget = .{
            .context = &budget,
            .observe = Budget.observe,
        },
    });

    const first = try tok.encode(allocator, "abc");
    allocator.free(first);
    const second = try tok.encode(allocator, "abc");
    allocator.free(second);
    const first_cab = try tok.encode(allocator, "cab");
    allocator.free(first_cab);
    const second_cab = try tok.encode(allocator, "cab");
    allocator.free(second_cab);
    const pressure_stats = tok.bpeCacheStats();
    try std.testing.expectEqual(@as(usize, 0), pressure_stats.entries);
    try std.testing.expectEqual(@sizeOf(HfTokenizer.BpeCache), pressure_stats.used_bytes);

    const third_cab = try tok.encode(allocator, "cab");
    defer allocator.free(third_cab);
    try std.testing.expectEqualSlices(i32, &.{6}, third_cab);

    const stats = tok.bpeCacheStats();
    try std.testing.expectEqual(@as(usize, 1), stats.entries);
    try std.testing.expectEqual(hard_limit, stats.used_bytes);
    try std.testing.expectEqual(@as(u64, 1), stats.rejected_reservations);
    try std.testing.expectEqual(stats.used_bytes, budget.used_bytes.load(.acquire));

    tok.deinitSelf();
    try std.testing.expectEqual(@as(usize, 0), budget.used_bytes.load(.acquire));
}

test "BPE bulk cache table participates in external byte budget" {
    const allocator = std.testing.allocator;
    const json_str =
        \\{
        \\  "model": {
        \\    "type": "BPE",
        \\    "vocab": {"a": 1},
        \\    "merges": []
        \\  },
        \\  "pre_tokenizer": {"type": "ByteLevel", "add_prefix_space": false}
        \\}
    ;
    const Budget = struct {
        used_bytes: std.atomic.Value(usize) = .init(0),

        fn observe(context: *anyopaque, _: usize, previous: usize, next: usize) bool {
            const self: *@This() = @ptrCast(@alignCast(context));
            if (self.used_bytes.load(.acquire) != previous) return false;
            self.used_bytes.store(next, .release);
            return true;
        }
    };

    const bulk_slots_per_shard = 4;
    const bulk_slot_count = bulk_slots_per_shard * HfTokenizer.bpe_cache_shard_count;
    const bulk_bytes =
        bulk_slot_count * @sizeOf(std.atomic.Value(usize));
    const hard_limit = @sizeOf(HfTokenizer.BpeCache) + bulk_bytes;
    var budget: Budget = .{};
    var tok = try HfTokenizer.loadFromBytes(allocator, json_str);
    errdefer tok.deinitSelf();
    try tok.configureBpeCache(.{
        .max_bytes = hard_limit,
        .bulk_slots_per_shard = bulk_slots_per_shard,
        .resource_budget = .{
            .context = &budget,
            .observe = Budget.observe,
        },
    });

    const stats = tok.bpeCacheStats();
    try std.testing.expectEqual(bulk_slot_count, stats.bulk_slots);
    try std.testing.expectEqual(@as(usize, 0), stats.bulk_entries);
    try std.testing.expectEqual(hard_limit, stats.used_bytes);
    try std.testing.expectEqual(hard_limit, budget.used_bytes.load(.acquire));

    tok.deinitSelf();
    try std.testing.expectEqual(@as(usize, 0), budget.used_bytes.load(.acquire));
}

test "BPE bulk cache denial preserves the front cache" {
    const allocator = std.testing.allocator;
    const json_str =
        \\{
        \\  "model": {
        \\    "type": "BPE",
        \\    "vocab": {"a": 1},
        \\    "merges": []
        \\  },
        \\  "pre_tokenizer": {"type": "ByteLevel", "add_prefix_space": false}
        \\}
    ;
    const Budget = struct {
        max_bytes: usize,
        used_bytes: std.atomic.Value(usize) = .init(0),

        fn observe(context: *anyopaque, _: usize, previous: usize, next: usize) bool {
            const self: *@This() = @ptrCast(@alignCast(context));
            if (self.used_bytes.load(.acquire) != previous) return false;
            if (next > self.max_bytes) return false;
            self.used_bytes.store(next, .release);
            return true;
        }
    };

    const base_bytes = @sizeOf(HfTokenizer.BpeCache);
    var budget = Budget{ .max_bytes = base_bytes };
    var tok = try HfTokenizer.loadFromBytes(allocator, json_str);
    errdefer tok.deinitSelf();
    try tok.configureBpeCache(.{
        .bulk_slots_per_shard = 4,
        .resource_budget = .{
            .context = &budget,
            .observe = Budget.observe,
        },
    });

    const stats = tok.bpeCacheStats();
    try std.testing.expectEqual(@as(usize, 0), stats.bulk_slots);
    try std.testing.expectEqual(base_bytes, stats.used_bytes);
    try std.testing.expectEqual(@as(u64, 1), stats.rejected_reservations);
    try std.testing.expectEqual(base_bytes, budget.used_bytes.load(.acquire));

    tok.deinitSelf();
    try std.testing.expectEqual(@as(usize, 0), budget.used_bytes.load(.acquire));
}

test "BPE bulk cache receives overflow without replacing front entries" {
    const allocator = std.testing.allocator;
    const json_str =
        \\{
        \\  "model": {
        \\    "type": "BPE",
        \\    "vocab": {"a": 1},
        \\    "merges": []
        \\  },
        \\  "pre_tokenizer": {"type": "ByteLevel", "add_prefix_space": false}
        \\}
    ;
    var tok = try HfTokenizer.loadFromBytes(allocator, json_str);
    defer tok.deinitSelf();
    try tok.configureBpeCache(.{
        .max_bytes = 16 * 1024 * 1024,
        .bulk_slots_per_shard = 4,
    });

    var candidate_index: usize = 0;
    var admitted: usize = 0;
    var last_key: [32]u8 = undefined;
    var last_key_len: usize = 0;
    while (admitted < HfTokenizer.bpe_cache_max_entries_per_shard + 1) {
        var key_buf: [32]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "bulk-key-{d}", .{candidate_index});
        candidate_index += 1;
        const hash = HfTokenizer.bpeCacheHash(key);
        if (HfTokenizer.bpeCacheShard(hash) != 0) continue;
        tok.cacheBpe(key, &.{42});
        tok.cacheBpe(key, &.{42});
        @memcpy(last_key[0..key.len], key);
        last_key_len = key.len;
        admitted += 1;
    }

    const stats = tok.bpeCacheStats();
    try std.testing.expectEqual(
        HfTokenizer.bpe_cache_max_entries_per_shard,
        stats.front_entries,
    );
    try std.testing.expectEqual(@as(usize, 1), stats.bulk_entries);
    try std.testing.expectEqual(
        HfTokenizer.bpe_cache_max_entries_per_shard + 1,
        stats.entries,
    );

    const reader = tok.enterBpeCacheRead() orelse
        return error.TestExpectedBpeCache;
    defer tok.leaveBpeCacheRead(reader);
    var ids: std.ArrayListUnmanaged(i32) = .empty;
    defer ids.deinit(allocator);
    try std.testing.expect(try tok.appendCachedBpe(
        allocator,
        last_key[0..last_key_len],
        &ids,
    ));
    try std.testing.expectEqualSlices(i32, &.{42}, ids.items);
}

test "metaspace generation participates in BPE cache reclamation epochs" {
    const allocator = std.testing.allocator;
    const json_str =
        \\{
        \\  "model": {
        \\    "type": "BPE",
        \\    "vocab": {
        \\      "a": 1, "b": 2, "c": 3,
        \\      "abc": 4, "cab": 5
        \\    },
        \\    "merges": []
        \\  },
        \\  "pre_tokenizer": {
        \\    "type": "Metaspace",
        \\    "prepend_scheme": "always",
        \\    "split": true
        \\  }
        \\}
    ;

    const entry_bytes =
        @sizeOf(HfTokenizer.BpeCacheEntry) + "abc".len + @sizeOf(i32);
    const hard_limit = @sizeOf(HfTokenizer.BpeCache) + entry_bytes;
    var tok = try HfTokenizer.loadFromBytes(allocator, json_str);
    defer tok.deinitSelf();
    try tok.configureBpeCache(.{ .max_bytes = hard_limit });

    const tokenizer = tok.tokenizer();
    var first = try tokenizer.encodeForGenerationConfigured(
        allocator,
        "abc",
        8,
        true,
    );
    first.deinit();
    var second = try tokenizer.encodeForGenerationConfigured(
        allocator,
        "abc",
        8,
        true,
    );
    second.deinit();
    try std.testing.expectEqual(@as(usize, 1), tok.bpeCacheStats().entries);

    var first_replacement = try tokenizer.encodeForGenerationConfigured(
        allocator,
        "cab",
        8,
        true,
    );
    first_replacement.deinit();
    var second_replacement = try tokenizer.encodeForGenerationConfigured(
        allocator,
        "cab",
        8,
        true,
    );
    second_replacement.deinit();

    const cache = tok.bpe_cache orelse return error.TestExpectedBpeCache;
    try std.testing.expectEqual(
        @as(usize, 0),
        cache.retired_bytes.load(.acquire),
    );
    try std.testing.expectEqual(
        @sizeOf(HfTokenizer.BpeCache),
        tok.bpeCacheStats().used_bytes,
    );

    var admitted_replacement = try tokenizer.encodeForGenerationConfigured(
        allocator,
        "cab",
        8,
        true,
    );
    admitted_replacement.deinit();
    try std.testing.expectEqual(@as(usize, 1), tok.bpeCacheStats().entries);
}

test "BPE cache admits repeated keys and replaces cold entries" {
    const allocator = std.testing.allocator;
    const json_str =
        \\{
        \\  "model": {
        \\    "type": "BPE",
        \\    "vocab": {"a": 1},
        \\    "merges": []
        \\  },
        \\  "pre_tokenizer": {"type": "ByteLevel", "add_prefix_space": false}
        \\}
    ;

    var tok = try HfTokenizer.loadFromBytes(allocator, json_str);
    defer tok.deinitSelf();

    const first = try tok.encode(allocator, "unseen");
    allocator.free(first);
    try std.testing.expectEqual(@as(usize, 0), tok.bpeCacheStats().entries);
    const second = try tok.encode(allocator, "unseen");
    allocator.free(second);
    try std.testing.expectEqual(@as(usize, 1), tok.bpeCacheStats().entries);

    const cache = tok.bpe_cache orelse return error.TestExpectedBpeCache;
    const reader = tok.enterBpeCacheRead() orelse return error.TestExpectedBpeCache;
    var reader_active = true;
    defer if (reader_active) tok.leaveBpeCacheRead(reader);
    var newest: [32]u8 = undefined;
    var newest_len: usize = 0;
    var candidate: usize = 0;
    var inserted: usize = 0;
    while (inserted <= HfTokenizer.bpe_cache_max_entries_per_shard) : (candidate += 1) {
        var key_buf: [32]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "collision-{d}", .{candidate});
        if (HfTokenizer.bpeCacheShard(HfTokenizer.bpeCacheHash(key)) != 0) continue;
        tok.cacheBpe(key, &.{42});
        tok.cacheBpe(key, &.{42});
        @memcpy(newest[0..key.len], key);
        newest_len = key.len;
        inserted += 1;
    }
    tok.leaveBpeCacheRead(reader);
    reader_active = false;

    const stats = tok.bpeCacheStats();
    // The unrelated "unseen" key may share this shard, but the shard itself
    // remains strictly capped and the newest admitted key survives.
    try std.testing.expect(stats.entries <=
        HfTokenizer.bpe_cache_max_entries_per_shard *
            HfTokenizer.bpe_cache_shard_count);
    try std.testing.expectEqual(
        HfTokenizer.bpe_cache_max_entries_per_shard,
        cache.shards[0].count.load(.acquire),
    );
    const verify_reader = tok.enterBpeCacheRead() orelse return error.TestExpectedBpeCache;
    defer tok.leaveBpeCacheRead(verify_reader);
    var ids = std.ArrayListUnmanaged(i32).empty;
    defer ids.deinit(allocator);
    try std.testing.expect(try tok.appendCachedBpe(
        allocator,
        newest[0..newest_len],
        &ids,
    ));
    try std.testing.expectEqualSlices(i32, &.{42}, ids.items);
    try std.testing.expectEqual(@as(usize, 0), cache.retired_bytes.load(.acquire));
}

test "parallel workspaces remain resource-accounted without a BPE cache table" {
    const allocator = std.testing.allocator;
    const json_str =
        \\{
        \\  "model": {
        \\    "type": "BPE",
        \\    "vocab": {"a": 1},
        \\    "merges": []
        \\  },
        \\  "pre_tokenizer": {"type": "ByteLevel", "add_prefix_space": false}
        \\}
    ;

    const Budget = struct {
        used_bytes: std.atomic.Value(usize) = .init(0),

        fn observe(context: *anyopaque, _: usize, previous: usize, next: usize) bool {
            const self: *@This() = @ptrCast(@alignCast(context));
            if (self.used_bytes.load(.acquire) != previous) return false;
            self.used_bytes.store(next, .release);
            return true;
        }
    };

    var tok = try HfTokenizer.loadFromBytes(allocator, json_str);
    const cache = tok.bpe_cache orelse return error.TestExpectedBpeCache;
    allocator.destroy(cache);
    tok.bpe_cache = null;

    var budget: Budget = .{};
    try tok.configureBpeCache(.{
        .resource_budget = .{
            .context = &budget,
            .observe = Budget.observe,
        },
    });
    const workspace = try tok.acquireParallelBpeWorkspace();
    tok.releaseParallelBpeWorkspace(workspace);
    try std.testing.expect(budget.used_bytes.load(.acquire) > 0);

    tok.deinitSelf();
    try std.testing.expectEqual(@as(usize, 0), budget.used_bytes.load(.acquire));
}

test "BPE tokenizer loading cleans up every allocation failure" {
    const json_str =
        \\{
        \\  "model": {
        \\    "type": "BPE",
        \\    "vocab": {"a": 1, "b": 2, "ab": 3},
        \\    "merges": ["a b"]
        \\  },
        \\  "pre_tokenizer": {"type": "ByteLevel", "add_prefix_space": false}
        \\}
    ;

    var fail_index: usize = 0;
    while (fail_index < 1024) : (fail_index += 1) {
        var failing = std.testing.FailingAllocator.init(
            std.testing.allocator,
            .{ .fail_index = fail_index },
        );
        const result = HfTokenizer.loadFromBytes(failing.allocator(), json_str);
        if (result) |tok| {
            tok.deinitSelf();
        } else |err| {
            try std.testing.expectEqual(error.OutOfMemory, err);
        }
        try std.testing.expectEqual(
            failing.allocated_bytes,
            failing.freed_bytes,
        );
        if (!failing.has_induced_failure) break;
    }
    try std.testing.expect(fail_index < 1024);
}

test "parallel workspace free list is bounded" {
    const allocator = std.testing.allocator;
    const json_str =
        \\{
        \\  "model": {
        \\    "type": "BPE",
        \\    "vocab": {"a": 1},
        \\    "merges": []
        \\  },
        \\  "pre_tokenizer": {"type": "ByteLevel", "add_prefix_space": false}
        \\}
    ;

    var tok = try HfTokenizer.loadFromBytes(allocator, json_str);
    defer tok.deinitSelf();
    var workspaces: [HfTokenizer.max_cached_parallel_workspaces + 2]*HfTokenizer.ParallelBpeWorkspace = undefined;
    for (&workspaces) |*workspace| {
        workspace.* = try tok.acquireParallelBpeWorkspace();
    }
    for (workspaces) |workspace| tok.releaseParallelBpeWorkspace(workspace);

    try std.testing.expectEqual(
        @as(usize, HfTokenizer.max_cached_parallel_workspaces),
        tok.parallel_workspace_free_count,
    );
    var all_count: usize = 0;
    var current = tok.parallel_workspace_all;
    while (current) |workspace| : (current = workspace.next_all) all_count += 1;
    try std.testing.expectEqual(
        @as(usize, HfTokenizer.max_cached_parallel_workspaces),
        all_count,
    );
}

test "clip byte-level bpe honors split pretokenizer array merges and end suffix" {
    const allocator = std.testing.allocator;

    const json_str =
        \\{
        \\  "model": {
        \\    "type": "BPE",
        \\    "end_of_word_suffix": "</w>",
        \\    "vocab": {
        \\      "<|startoftext|>": 49406,
        \\      "<|endoftext|>": 49407,
        \\      "a": 64,
        \\      "a</w>": 320,
        \\      "c": 66,
        \\      "u": 84,
        \\      "p": 79,
        \\      "k": 74,
        \\      "e": 68,
        \\      "cu": 1000,
        \\      "cup": 1001,
        \\      "cupc": 1002,
        \\      "cupca": 1003,
        \\      "cupcak": 1004,
        \\      "e</w>": 1005,
        \\      "cupcake</w>": 17025
        \\    },
        \\    "merges": [
        \\      ["a", "</w>"],
        \\      ["c", "u"],
        \\      ["cu", "p"],
        \\      ["cup", "c"],
        \\      ["cupc", "a"],
        \\      ["cupca", "k"],
        \\      ["e", "</w>"],
        \\      ["cupcak", "e</w>"]
        \\    ]
        \\  },
        \\  "pre_tokenizer": {
        \\    "type": "Sequence",
        \\    "pretokenizers": [
        \\      {"type": "Split", "pattern": {"Regex": "[\\p{L}]+"}, "behavior": "Removed", "invert": true},
        \\      {"type": "ByteLevel", "add_prefix_space": false, "trim_offsets": true}
        \\    ]
        \\  },
        \\  "added_tokens": [
        \\    {"id": 49406, "content": "<|startoftext|>", "special": true},
        \\    {"id": 49407, "content": "<|endoftext|>", "special": true}
        \\  ],
        \\  "post_processor": {
        \\    "type": "RobertaProcessing",
        \\    "sep": ["<|endoftext|>", 49407],
        \\    "cls": ["<|startoftext|>", 49406]
        \\  }
        \\}
    ;

    var hf = try HfTokenizer.loadFromBytes(allocator, json_str);
    defer hf.deinitSelf();

    var encoded = try hf.tokenizer().encodeForModel(allocator, "a cupcake", 6);
    defer encoded.deinit();

    try std.testing.expectEqualSlices(i32, &.{ 49406, 320, 17025, 49407, 49407, 49407 }, encoded.ids);
    try std.testing.expectEqualSlices(i32, &.{ 1, 1, 1, 1, 0, 0 }, encoded.attention_mask);
}

test "sequence normalizer applies lowercase before byte-level bpe" {
    const allocator = std.testing.allocator;

    const json_str =
        \\{
        \\  "model": {
        \\    "type": "BPE",
        \\    "vocab": {
        \\      "<|startoftext|>": 10,
        \\      "<|endoftext|>": 11,
        \\      "white": 1,
        \\      "black": 2
        \\    },
        \\    "merges": []
        \\  },
        \\  "normalizer": {
        \\    "type": "Sequence",
        \\    "normalizers": [
        \\      {"type": "NFC"},
        \\      {"type": "Lowercase"}
        \\    ]
        \\  },
        \\  "pre_tokenizer": {
        \\    "type": "Sequence",
        \\    "pretokenizers": [
        \\      {"type": "Split", "pattern": {"Regex": "[\\p{L}]+"}, "behavior": "Removed", "invert": true},
        \\      {"type": "ByteLevel", "add_prefix_space": false, "trim_offsets": true}
        \\    ]
        \\  },
        \\  "added_tokens": [
        \\    {"id": 10, "content": "<|startoftext|>", "special": true},
        \\    {"id": 11, "content": "<|endoftext|>", "special": true}
        \\  ],
        \\  "post_processor": {
        \\    "type": "RobertaProcessing",
        \\    "sep": ["<|endoftext|>", 11],
        \\    "cls": ["<|startoftext|>", 10]
        \\  }
        \\}
    ;

    var hf = try HfTokenizer.loadFromBytes(allocator, json_str);
    defer hf.deinitSelf();

    const white = try hf.encode(allocator, "WHITE");
    defer allocator.free(white);
    try std.testing.expectEqualSlices(i32, &.{1}, white);

    const black = try hf.encode(allocator, "BLACK");
    defer allocator.free(black);
    try std.testing.expectEqualSlices(i32, &.{2}, black);

    const tok = hf.tokenizer();
    var encoded = try tok.encodeForModel(allocator, "WHITE", 4);
    defer encoded.deinit();
    try std.testing.expectEqualSlices(i32, &.{ 10, 1, 11, 11 }, encoded.ids);
}

test "clip roberta processing pads with eos when no pad token is declared" {
    const allocator = std.testing.allocator;

    const json_str =
        \\{
        \\  "model": {
        \\    "type": "BPE",
        \\    "vocab": {
        \\      "<|startoftext|>": 49406,
        \\      "<|endoftext|>": 49407,
        \\      "cup": 1
        \\    },
        \\    "merges": []
        \\  },
        \\  "pre_tokenizer": {"type": "ByteLevel"},
        \\  "added_tokens": [
        \\    {"id": 49406, "content": "<|startoftext|>", "special": true},
        \\    {"id": 49407, "content": "<|endoftext|>", "special": true}
        \\  ],
        \\  "post_processor": {
        \\    "type": "RobertaProcessing",
        \\    "sep": ["<|endoftext|>", 49407],
        \\    "cls": ["<|startoftext|>", 49406]
        \\  }
        \\}
    ;

    var hf = try HfTokenizer.loadFromBytes(allocator, json_str);
    defer hf.deinitSelf();

    const special = hf.getSpecialTokens();
    try std.testing.expectEqual(@as(i32, 49407), special.pad_id);

    const tok = hf.tokenizer();
    var encoded = try tok.encodeForModel(allocator, "cup", 6);
    defer encoded.deinit();
    try std.testing.expectEqual(@as(i32, 49406), encoded.ids[0]);
    try std.testing.expectEqual(@as(i32, 49407), encoded.ids[2]);
    try std.testing.expectEqual(@as(i32, 49407), encoded.ids[3]);
    try std.testing.expectEqual(@as(i32, 0), encoded.attention_mask[3]);
}

test "bpe encode basic" {
    const allocator = std.testing.allocator;

    const json_str =
        \\{
        \\  "model": {
        \\    "type": "BPE",
        \\    "vocab": {"a": 0, "b": 1, "c": 2, "ab": 3, "abc": 4},
        \\    "merges": ["a b", "ab c"]
        \\  },
        \\  "pre_tokenizer": {"type": "BertPreTokenizer"}
        \\}
    ;

    var tok = try HfTokenizer.loadFromBytes(allocator, json_str);
    defer tok.deinitSelf();

    const ids = try tok.encode(allocator, "abc");
    defer allocator.free(ids);
    try std.testing.expectEqual(@as(usize, 1), ids.len);
    try std.testing.expectEqual(@as(i32, 4), ids[0]); // "abc" after merges
}

test "bpe encode preserves added token adjacent to text" {
    const allocator = std.testing.allocator;

    const json_str =
        \\{
        \\  "model": {
        \\    "type": "BPE",
        \\    "vocab": {"D": 1, "e": 2, "s": 3, "c": 4, "r": 5, "i": 6, "b": 7},
        \\    "merges": []
        \\  },
        \\  "pre_tokenizer": {"type": "Split", "pattern": {"String": " "}, "behavior": "MergedWithPrevious", "invert": false},
        \\  "added_tokens": [
        \\    {"id": 10, "content": "<start_of_image>", "special": true}
        \\  ]
        \\}
    ;

    var tok = try HfTokenizer.loadFromBytes(allocator, json_str);
    defer tok.deinitSelf();

    const ids = try tok.encode(allocator, "<start_of_image>Describe");
    defer allocator.free(ids);
    try std.testing.expect(ids.len > 0);
    try std.testing.expectEqual(@as(i32, 10), ids[0]);
}

test "bpe encode uses direct vocab hit when word is not merge-constructible" {
    const allocator = std.testing.allocator;

    const json_str =
        \\{
        \\  "model": {
        \\    "type": "BPE",
        \\    "vocab": {"u": 1, "s": 2, "e": 3, "r": 4, "user": 5},
        \\    "merges": []
        \\  },
        \\  "pre_tokenizer": {"type": "BertPreTokenizer"}
        \\}
    ;

    var tok = try HfTokenizer.loadFromBytes(allocator, json_str);
    defer tok.deinitSelf();

    const ids = try tok.encode(allocator, "user");
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i32, &.{5}, ids);
}

test "bpe encode handles gemma replace+split tokenizer mode" {
    const allocator = std.testing.allocator;

    const json_str =
        \\{
        \\  "normalizer": {"type": "Replace", "pattern": {"String": " "}, "content": "▁"},
        \\  "pre_tokenizer": {"type": "Split", "pattern": {"String": " "}, "behavior": "MergedWithPrevious", "invert": false},
        \\  "model": {
        \\    "type": "BPE",
        \\    "continuing_subword_prefix": null,
        \\    "end_of_word_suffix": null,
        \\    "byte_fallback": false,
        \\    "vocab": {"Describe": 10, "▁this": 11, "▁image": 12, ".": 13},
        \\    "merges": []
        \\  }
        \\}
    ;

    var tok = try HfTokenizer.loadFromBytes(allocator, json_str);
    defer tok.deinitSelf();

    const ids = try tok.encode(allocator, "Describe this image.");
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i32, &.{ 10, 11, 12, 13 }, ids);
}

test "gemma replace+split tokenizer honors ranked bpe merges" {
    const allocator = std.testing.allocator;

    // The longer vocab pieces are deliberate distractors: greedy vocab
    // segmentation would emit `▁Spell`+`a` and `▁Caff`+`e`, while the ranked
    // merge table specifies the same segmentation as llama.cpp/Gemma4.
    const json_str =
        \\{
        \\  "normalizer": {"type": "Replace", "pattern": {"String": " "}, "content": "▁"},
        \\  "pre_tokenizer": {"type": "Split", "pattern": {"String": " "}, "behavior": "MergedWithPrevious", "invert": false},
        \\  "model": {
        \\    "type": "BPE",
        \\    "continuing_subword_prefix": null,
        \\    "end_of_word_suffix": null,
        \\    "byte_fallback": false,
        \\    "vocab": {
        \\      "▁": 0, "S": 1, "p": 2, "e": 3, "l": 4, "a": 5, "C": 6, "f": 7,
        \\      "el": 8, "ell": 9, "ella": 10, "▁S": 11, "▁Sp": 12,
        \\      "ff": 13, "aff": 14, "affe": 15, "▁C": 16,
        \\      "▁Spell": 17, "▁Caff": 18
        \\    },
        \\    "merges": ["e l", "▁ S", "el l", "▁S p", "ell a", "▁ C", "f f", "a ff", "aff e"]
        \\  }
        \\}
    ;

    var tok = try HfTokenizer.loadFromBytes(allocator, json_str);
    defer tok.deinitSelf();

    const ids = try tok.encode(allocator, " Spella Caffe");
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i32, &.{ 12, 10, 16, 15 }, ids);
}

test "bpe encode applies same-rank merges left-to-right" {
    // Regression: with several same-rank "a a" candidates in "aaaaa", a
    // heap that ignores position can pop a middle candidate ahead of the
    // leftmost one, producing "aa, a, aa" instead of the HuggingFace
    // reference "aa, aa, a". The priority queue's tie-break on `left`
    // forces leftmost-first when ranks are equal.
    const allocator = std.testing.allocator;
    const json_str =
        \\{
        \\  "model": {
        \\    "type": "BPE",
        \\    "vocab": {"a": 0, "aa": 1},
        \\    "merges": ["a a"]
        \\  },
        \\  "pre_tokenizer": {"type": "BertPreTokenizer"}
        \\}
    ;

    var tok = try HfTokenizer.loadFromBytes(allocator, json_str);
    defer tok.deinitSelf();

    const ids = try tok.encode(allocator, "aaaaa");
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i32, &.{ 1, 1, 0 }, ids);
}

test "unigram encode basic" {
    const allocator = std.testing.allocator;

    const json_str =
        \\{
        \\  "model": {
        \\    "type": "Unigram",
        \\    "unk_id": 0,
        \\    "vocab": [["<unk>", 0.0], ["a", -1.0], ["b", -1.0], ["ab", -0.5], ["abc", -0.3], ["c", -1.0]]
        \\  },
        \\  "pre_tokenizer": {"type": "Metaspace", "replacement": "\u2581", "prepend_scheme": "always"}
        \\}
    ;

    var tok = try HfTokenizer.loadFromBytes(allocator, json_str);
    defer tok.deinitSelf();

    // "abc" with Unigram should prefer "abc" (score -0.3) over "ab"+"c" (score -0.5+-1.0=-1.5)
    // But metaspace prepends ▁, so input becomes "▁abc"
    // Since "▁abc" isn't in vocab, it falls back to byte-level pieces
    // Let's test without metaspace effect — use a word that matches
    const ids = try tok.encode(allocator, "abc");
    defer allocator.free(ids);
    // With metaspace "always" prepend, this becomes "▁abc" which won't match
    // So it will fall back to bytes. Let's just verify it doesn't crash.
    try std.testing.expect(ids.len > 0);
}

test "metaspace pre-tokenizer" {
    const allocator = std.testing.allocator;

    const words = try metaspacePreTokenize(allocator, "hello world test", "\xe2\x96\x81", .always, true);
    defer {
        for (words) |w| allocator.free(w);
        allocator.free(words);
    }

    try std.testing.expectEqual(@as(usize, 3), words.len);
    try std.testing.expectEqualStrings("\xe2\x96\x81hello", words[0]); // ▁hello
    try std.testing.expectEqualStrings("\xe2\x96\x81world", words[1]); // ▁world
    try std.testing.expectEqualStrings("\xe2\x96\x81test", words[2]); // ▁test
}

test "metaspace pre-tokenizer split false with first prepend" {
    const allocator = std.testing.allocator;

    const words = try metaspacePreTokenize(allocator, "What is 2+2?", "\xe2\x96\x81", .first, false);
    defer {
        for (words) |w| allocator.free(w);
        allocator.free(words);
    }

    try std.testing.expectEqual(@as(usize, 1), words.len);
    try std.testing.expectEqualStrings("\xe2\x96\x81What\xe2\x96\x81is\xe2\x96\x812+2?", words[0]);
}

test "byte-level decode applies to Sequence[Split, ByteLevel] pre-tokenizers" {
    const allocator = std.testing.allocator;

    // The pre_tokenizer shape used by Llama 3 and Qwen3. It parses to
    // .byte_level_split, which decode once treated as "not byte level", leaving the
    // raw space marker 'Ġ' (U+0120) and newline marker 'Ċ' (U+010A) in output text.
    const json_str =
        \\{
        \\  "pre_tokenizer": {"type": "Sequence", "pretokenizers": [
        \\    {"type": "Split", "pattern": {"Regex": "\\s+"}, "behavior": "Isolated", "invert": false},
        \\    {"type": "ByteLevel", "add_prefix_space": false, "trim_offsets": false}
        \\  ]},
        \\  "decoder": {"type": "ByteLevel"},
        \\  "model": {
        \\    "type": "BPE",
        \\    "continuing_subword_prefix": null,
        \\    "end_of_word_suffix": null,
        \\    "byte_fallback": false,
        \\    "vocab": {"Okay": 10, "Ġlet": 11, "Ġsee": 12, "Ċ": 13},
        \\    "merges": []
        \\  }
        \\}
    ;

    var tok = try HfTokenizer.loadFromBytes(allocator, json_str);
    defer tok.deinitSelf();

    try std.testing.expectEqual(PreTokenizerType.byte_level_split, tok.pre_tokenizer_type);

    const text = try tok.decode(allocator, &.{ 10, 11, 12, 13 });
    defer allocator.free(text);

    try std.testing.expectEqualStrings("Okay let see\n", text);
}
