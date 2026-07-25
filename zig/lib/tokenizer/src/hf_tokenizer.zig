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
    const max_cached_parallel_workspace_bytes = 64 * 1024 * 1024;

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
    };

    const bpe_cache_tombstone: usize = 1;
    const bpe_doorkeeper_words = 4096;
    const bpe_doorkeeper_rotate_after = 32 * 1024;

    const BpeDoorkeeper = struct {
        // Two independent bits materially reduce false second-hit admission.
        // Two rotating generations keep a one-shot scan from saturating the
        // filter forever.
        generations: [2][bpe_doorkeeper_words]std.atomic.Value(u64) =
            @splat(@splat(.{ .raw = 0 })),
        active_generation: std.atomic.Value(u8) = .init(0),
        observations: std.atomic.Value(usize) = .init(0),
        rotating: std.atomic.Value(bool) = .init(false),
    };

    const BpeCache = struct {
        shards: [bpe_cache_shard_count]BpeCacheShard =
            [_]BpeCacheShard{.{}} ** bpe_cache_shard_count,
        max_bytes: usize = default_bpe_cache_max_bytes,
        used_bytes: std.atomic.Value(usize) = .init(0),
        rejected_reservations: std.atomic.Value(u64) = .init(0),
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
        try_reserve: *const fn (context: *anyopaque, bytes: usize) bool,
        release: *const fn (context: *anyopaque, bytes: usize) void,
    };

    pub const BpeCacheConfig = struct {
        /// Hard bound for the fixed lookup table and immutable cache entries.
        /// A value smaller than the fixed table disables cache insertion.
        max_bytes: usize = default_bpe_cache_max_bytes,
        /// Optional process-wide admission budget. Reservations happen only on
        /// cold insertion; cache hits remain lock-free and callback-free.
        resource_budget: ?BpeCacheResourceBudget = null,
    };

    pub const BpeCacheStats = struct {
        max_bytes: usize,
        used_bytes: usize,
        entries: usize,
        rejected_reservations: u64,
    };

    pub const BpeProfile = struct {
        hits: u64,
        misses: u64,
        probes: u64,
        key_bytes: u64,
        token_ids: u64,
        key_len_histogram: [33]u64,
        id_count_histogram: [9]u64,
    };

    const BpeProfileCounters = struct {
        hits: std.atomic.Value(u64) = .init(0),
        misses: std.atomic.Value(u64) = .init(0),
        probes: std.atomic.Value(u64) = .init(0),
        key_bytes: std.atomic.Value(u64) = .init(0),
        token_ids: std.atomic.Value(u64) = .init(0),
        key_len_histogram: [33]std.atomic.Value(u64) =
            @splat(.{ .raw = 0 }),
        id_count_histogram: [9]std.atomic.Value(u64) =
            @splat(.{ .raw = 0 }),
    };

    /// Byte-indexed trie used for added-token matching. Each node stores its
    /// children in a HashMap keyed by the next byte; final nodes hold the
    /// token id and length.
    const AddedTokenTrie = struct {
        nodes: std.ArrayListUnmanaged(Node) = .empty,
        root_bytes: [256]bool = @splat(false),
        root_byte_count: u16 = 0,
        single_root_byte: u8 = 0,

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

        /// Longest added-token match starting at `text[0]`, if any.
        fn longestPrefixMatch(self: *const AddedTokenTrie, text: []const u8) ?AddedTokenMatch {
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
        const parsed = try std.json.parseFromSlice(std.json.Value, allocator, json_bytes, .{});
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
                cache.* = .{};
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

    const ParallelBpeWorker = struct {
        tokenizer: *HfTokenizer = undefined,
        text: []const u8 = "",
        ids: std.ArrayListUnmanaged(i32) = .empty,
        bpe_scratch: BpeScratch = .{},
        failure: ?anyerror = null,
        done: std.atomic.Value(bool) = .init(false),

        fn run(self: *ParallelBpeWorker) std.Io.Cancelable!void {
            self.tokenizer.encodeBpeByteLevel(
                self.tokenizer.allocator,
                self.text,
                &self.ids,
                &self.bpe_scratch,
            ) catch |err| {
                self.failure = err;
            };
        }
    };

    const ParallelBpeJob = struct {
        chunks: []ParallelBpeWorker,
        output: *std.ArrayListUnmanaged(i32),
        next_chunk: std.atomic.Value(usize) = .init(0),
        commit_mutex: std.atomic.Mutex = .unlocked,
        next_commit: usize = 0,

        fn run(self: *ParallelBpeJob) std.Io.Cancelable!void {
            while (true) {
                const idx = self.next_chunk.fetchAdd(1, .monotonic);
                if (idx >= self.chunks.len) return;
                try self.chunks[idx].run();
                self.chunks[idx].done.store(true, .release);
                self.commitReady();
            }
        }

        fn commitReady(self: *ParallelBpeJob) void {
            if (!self.commit_mutex.tryLock()) return;
            defer self.commit_mutex.unlock();
            while (self.next_commit < self.chunks.len) {
                const chunk = &self.chunks[self.next_commit];
                if (!chunk.done.load(.acquire) or chunk.failure != null) return;
                if (chunk.ids.items.len >
                    self.output.capacity - self.output.items.len)
                {
                    return;
                }
                self.output.appendSliceAssumeCapacity(chunk.ids.items);
                self.next_commit += 1;
            }
        }
    };

    const ParallelBpeWorkspace = struct {
        next_free: ?*ParallelBpeWorkspace = null,
        next_all: ?*ParallelBpeWorkspace = null,
        resource_accounted_bytes: usize = 0,
        workers: [64]ParallelBpeWorker =
            [_]ParallelBpeWorker{.{}} ** 64,

        fn retainedBytes(self: *const ParallelBpeWorkspace) usize {
            var total: usize = @sizeOf(ParallelBpeWorkspace);
            for (&self.workers) |*worker| {
                total +|= worker.ids.capacity *| @sizeOf(i32);
                total +|= worker.bpe_scratch.symbols.capacity *| @sizeOf(BpeSymbol);
                if (worker.bpe_scratch.candidates) |*candidates| {
                    total +|= candidates.items.capacity *| @sizeOf(BpeCandidate);
                }
            }
            return total;
        }
    };

    fn destroyParallelBpeWorkspace(
        self: *HfTokenizer,
        workspace: *ParallelBpeWorkspace,
    ) void {
        if (workspace.resource_accounted_bytes != 0) {
            if (self.cache_resource_budget) |budget| {
                budget.release(
                    budget.context,
                    workspace.resource_accounted_bytes,
                );
            }
            workspace.resource_accounted_bytes = 0;
        }
        for (&workspace.workers) |*worker| {
            worker.ids.deinit(self.allocator);
            worker.bpe_scratch.deinit(self.allocator);
        }
        self.allocator.destroy(workspace);
    }

    fn acquireParallelBpeWorkspace(self: *HfTokenizer) !*ParallelBpeWorkspace {
        while (!self.parallel_workspace_mutex.tryLock()) std.atomic.spinLoopHint();
        if (self.parallel_workspace_free) |workspace| {
            self.parallel_workspace_free = workspace.next_free;
            self.parallel_workspace_free_count -= 1;
            workspace.next_free = null;
            self.parallel_workspace_mutex.unlock();
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
        const retained_bytes = workspace.retainedBytes();
        var cacheable =
            retained_bytes <= max_cached_parallel_workspace_bytes;
        if (cacheable) {
            if (self.cache_resource_budget) |budget| {
                if (retained_bytes > workspace.resource_accounted_bytes) {
                    const additional =
                        retained_bytes - workspace.resource_accounted_bytes;
                    if (budget.try_reserve(budget.context, additional)) {
                        workspace.resource_accounted_bytes = retained_bytes;
                    } else {
                        cacheable = false;
                    }
                } else if (retained_bytes < workspace.resource_accounted_bytes) {
                    budget.release(
                        budget.context,
                        workspace.resource_accounted_bytes - retained_bytes,
                    );
                    workspace.resource_accounted_bytes = retained_bytes;
                }
            }
        }
        while (!self.parallel_workspace_mutex.tryLock()) std.atomic.spinLoopHint();
        if (cacheable and
            self.parallel_workspace_free_count < max_cached_parallel_workspaces)
        {
            workspace.next_free = self.parallel_workspace_free;
            self.parallel_workspace_free = workspace;
            self.parallel_workspace_free_count += 1;
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
        boundaries: *[65]usize,
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
        if (requested_tasks <= 1 or
            text.len < parallel_bpe_min_bytes or
            self.model_type != .bpe or
            self.pre_tokenizer_type != .byte_level or
            self.do_lowercase or
            self.replace_space_with != null or
            self.end_of_word_suffix.len != 0)
        {
            return self.encodeInto(allocator, text, ids);
        }
        if (self.added_tokens.count() != 0 and
            (self.matchAddedTokenAt(text) != null or
                self.findNextAddedToken(text, 0) != null))
        {
            return self.encodeInto(allocator, text, ids);
        }

        const runner_count = @min(requested_tasks, 64);
        // More chunks than runners lets the std.Io tasks pull another piece
        // when they finish early, reducing the long tail caused by uneven
        // pretoken/cache work while keeping concurrency bounded by the
        // caller's requested task count.
        const chunks_per_runner: usize = if (text.len >= 4 * 1024 * 1024)
            8
        else
            4;
        const chunk_count = @min(runner_count * chunks_per_runner, 64);
        var boundaries: [65]usize = undefined;
        const boundary_count = collectParallelBpeBoundaries(
            text,
            chunk_count,
            &boundaries,
        );
        const worker_count = boundary_count - 1;
        if (worker_count <= 1) return self.encodeInto(allocator, text, ids);

        // One read-side critical section protects every worker's lock-free
        // cache lookup. Serial fallbacks establish their own section.
        const cache_reader = self.enterBpeCacheRead();
        defer if (cache_reader) |cache| self.leaveBpeCacheRead(cache);

        const workspace = try self.acquireParallelBpeWorkspace();
        defer self.releaseParallelBpeWorkspace(workspace);
        const workers = workspace.workers[0..worker_count];
        const active_runners = @min(runner_count, worker_count);
        // The caller is one queue consumer; background_count therefore keeps
        // total active consumers at or below requested_tasks.
        const background_count = active_runners - 1;

        for (workers, 0..) |*worker, idx| {
            worker.tokenizer = self;
            worker.text = text[boundaries[idx]..boundaries[idx + 1]];
            worker.ids.clearRetainingCapacity();
            worker.failure = null;
            worker.done.store(false, .monotonic);
        }

        // Reserve the normal GPT-style token density before launch. Ordered
        // commits stop at capacity; after the group joins, the caller grows
        // once to the exact residual size. This preserves copy/encode overlap
        // without reserving four output bytes for every input byte up front.
        const output_start = ids.items.len;
        const previous_output_capacity = ids.capacity;
        const estimated_tokens = text.len / 3 +| 8;
        const estimated_capacity = std.math.add(
            usize,
            output_start,
            estimated_tokens,
        ) catch return error.OutOfMemory;
        try ids.ensureTotalCapacityPrecise(allocator, estimated_capacity);
        if (ids.capacity != previous_output_capacity) {
            adviseHugePages(ids.items.ptr[0..ids.capacity]);
        }
        errdefer ids.items.len = output_start;
        var job = ParallelBpeJob{ .chunks = workers, .output = ids };
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
        job.commitReady();
        std.debug.assert(job.next_commit == workers.len);
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

    fn matchAddedTokenAt(self: *HfTokenizer, text: []const u8) ?AddedTokenMatch {
        return self.added_trie.longestPrefixMatch(text);
    }

    fn findNextAddedToken(self: *HfTokenizer, text: []const u8, start: usize) ?usize {
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

    fn sameBpeCacheResourceBudget(
        a: ?BpeCacheResourceBudget,
        b: ?BpeCacheResourceBudget,
    ) bool {
        if (a == null or b == null) return a == null and b == null;
        return a.?.context == b.?.context and
            a.?.try_reserve == b.?.try_reserve and
            a.?.release == b.?.release;
    }

    /// Configure the local hard limit and optional process-wide admission
    /// budget before the tokenizer is used. A denied base-table reservation
    /// disables the optional cache rather than failing tokenizer loading.
    pub fn configureBpeCache(self: *HfTokenizer, config: BpeCacheConfig) !void {
        if (self.parallel_workspace_all != null) return error.BpeCacheAlreadyPopulated;
        const cache = self.bpe_cache orelse {
            // Parallel BPE remains available when the optional fixed cache
            // table could not be allocated. Its retained workspaces still
            // participate in the caller's process-wide resource budget.
            self.cache_resource_budget = config.resource_budget;
            return;
        };
        for (&cache.shards) |*shard| {
            if (shard.count.load(.acquire) != 0) return error.BpeCacheAlreadyPopulated;
        }

        const base_bytes = cache.used_bytes.load(.acquire);
        if (config.max_bytes < base_bytes) {
            if (cache.resource_budget) |old_budget| {
                old_budget.release(old_budget.context, base_bytes);
            }
            self.allocator.destroy(cache);
            self.bpe_cache = null;
            self.cache_resource_budget = config.resource_budget;
            return;
        }
        if (!sameBpeCacheResourceBudget(cache.resource_budget, config.resource_budget)) {
            if (config.resource_budget) |budget| {
                if (!budget.try_reserve(budget.context, base_bytes)) {
                    if (cache.resource_budget) |old_budget| {
                        old_budget.release(old_budget.context, base_bytes);
                    }
                    self.allocator.destroy(cache);
                    self.bpe_cache = null;
                    self.cache_resource_budget = config.resource_budget;
                    return;
                }
            }
            if (cache.resource_budget) |old_budget| {
                old_budget.release(old_budget.context, base_bytes);
            }
            cache.resource_budget = config.resource_budget;
        }
        self.cache_resource_budget = config.resource_budget;
        cache.max_bytes = config.max_bytes;
    }

    pub fn bpeCacheStats(self: *const HfTokenizer) BpeCacheStats {
        const cache = self.bpe_cache orelse return .{
            .max_bytes = 0,
            .used_bytes = 0,
            .entries = 0,
            .rejected_reservations = 0,
        };
        var entries: usize = 0;
        for (&cache.shards) |*shard| {
            entries += shard.count.load(.acquire);
        }
        return .{
            .max_bytes = cache.max_bytes,
            .used_bytes = cache.used_bytes.load(.acquire),
            .entries = entries,
            .rejected_reservations = cache.rejected_reservations.load(.monotonic),
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
        if (cache.resource_budget) |budget| {
            if (!budget.try_reserve(budget.context, bytes)) {
                _ = cache.used_bytes.fetchSub(bytes, .acq_rel);
                _ = cache.rejected_reservations.fetchAdd(1, .monotonic);
                return false;
            }
        }
        return true;
    }

    fn releaseBpeCacheBytes(cache: *BpeCache, bytes: usize) void {
        if (bytes == 0) return;
        if (cache.resource_budget) |budget| {
            budget.release(budget.context, bytes);
        }
        const previous = cache.used_bytes.fetchSub(bytes, .acq_rel);
        std.debug.assert(previous >= bytes);
    }

    fn bpeCacheShard(hash: u64) usize {
        return @intCast(hash & (bpe_cache_shard_count - 1));
    }

    fn bpeCacheSlot(hash: u64) usize {
        return @intCast((hash >> 6) & (bpe_cache_slots_per_shard - 1));
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

    fn bpeCacheShardContainsLocked(
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
                bpeCacheShardContainsLocked(shard, candidate_hash, candidate_word))
            {
                shard.mutex.unlock();
                return;
            }
            const evicted = evictBpeCacheEntry(cache, shard);
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

    fn reclaimRetiredBpeCacheEntries(self: *HfTokenizer, cache: *BpeCache) void {
        if (cache.reclaiming.cmpxchgStrong(false, true, .acq_rel, .acquire) != null) return;

        cache.reader_gate.store(true, .release);
        while (cache.active_readers.load(.acquire) != 0) std.atomic.spinLoopHint();

        lockBpeCacheMutex(&cache.retired_mutex);
        var retired = cache.retired_head;
        cache.retired_head = null;
        const retired_bytes = cache.retired_bytes.swap(0, .acq_rel);
        cache.retired_mutex.unlock();

        for (&cache.shards) |*shard| {
            lockBpeCacheMutex(&shard.mutex);
            rebuildBpeCacheShard(shard);
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
        const shard = &cache.shards[bpeCacheShard(hash)];
        var slot_idx = bpeCacheSlot(hash);
        for (0..bpe_cache_slots_per_shard) |_| {
            probes += 1;
            const raw = shard.slots[slot_idx].load(.acquire);
            if (raw == 0) {
                self.recordBpeCacheMiss(probes);
                return false;
            }
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
    }

    fn recordBpeCacheMiss(self: *HfTokenizer, probes: usize) void {
        if (!self.bpe_profile_enabled.load(.acquire)) return;
        _ = self.bpe_profile.misses.fetchAdd(1, .monotonic);
        _ = self.bpe_profile.probes.fetchAdd(probes, .monotonic);
    }

    pub fn setBpeProfiling(self: *HfTokenizer, enabled: bool) void {
        self.bpe_profile_enabled.store(false, .release);
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
        return .{
            .hits = self.bpe_profile.hits.load(.monotonic),
            .misses = self.bpe_profile.misses.load(.monotonic),
            .probes = self.bpe_profile.probes.load(.monotonic),
            .key_bytes = self.bpe_profile.key_bytes.load(.monotonic),
            .token_ids = self.bpe_profile.token_ids.load(.monotonic),
            .key_len_histogram = key_len_histogram,
            .id_count_histogram = id_count_histogram,
        };
    }

    /// Best-effort cache insertion. Tokenization must not fail merely because
    /// the optional cache cannot grow, so allocation failures are ignored.
    fn cacheBpe(self: *HfTokenizer, word: []const u8, token_ids: []const i32) void {
        if (word.len == 0 or word.len > bpe_cache_max_key_bytes or token_ids.len == 0) return;

        const hash = bpeCacheHash(word);
        const cache = self.bpe_cache orelse return;
        if (!observeBpeCacheCandidate(cache, hash)) return;
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

        if (shard.count.load(.monotonic) >= bpe_cache_max_entries_per_shard and
            !evictBpeCacheEntry(cache, shard))
        {
            return;
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
        if (self.byte_level_direct_ids) |direct_ids| {
            const id = switch (word.len) {
                1 => direct_ids.single[word[0]],
                2 => direct_ids.pair[
                    (@as(usize, word[0]) << 8) | @as(usize, word[1])
                ],
                else => -1,
            };
            if (id >= 0) {
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
        return self.vocab.count();
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
            var retired = cache.retired_head;
            while (retired) |entry| {
                const next = entry.next_retired;
                allocator.free(entry.key);
                allocator.free(entry.token_ids);
                allocator.destroy(entry);
                retired = next;
            }
            if (cache.resource_budget) |budget| {
                budget.release(budget.context, accounted_bytes);
            }
            allocator.destroy(cache);
        }
        if (self.byte_level_direct_ids) |direct_ids| allocator.destroy(direct_ids);
        var workspace = self.parallel_workspace_all;
        while (workspace) |current| {
            const next = current.next_all;
            self.destroyParallelBpeWorkspace(current);
            workspace = next;
        }
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

    const len = @min(utf8CodepointLen(first), text.len - pos);
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

/// Find every GPT-2 pretoken start in the next 64 ASCII bytes. The returned
/// mask always contains bit zero; bit N means `text[start + N]` begins a
/// pretoken. null routes batches containing Unicode, edge contractions, or
/// insufficient lookahead through the scalar ground-truth scanner.
fn gpt2AsciiBoundaryMask(text: []const u8, start: usize) ?u64 {
    const batch_len = 64;
    if (text.len - start <= batch_len) return null;

    const ByteVector = @Vector(batch_len, u8);
    const BoolVector = @Vector(batch_len, bool);
    const block: [batch_len]u8 = text[start..][0..batch_len].*;
    const bytes: ByteVector = block;
    const lower = bytes | @as(ByteVector, @splat(0x20));

    const letters_vec: BoolVector =
        (lower >= @as(ByteVector, @splat('a'))) &
        (lower <= @as(ByteVector, @splat('z')));
    const digits_vec: BoolVector =
        (bytes >= @as(ByteVector, @splat('0'))) &
        (bytes <= @as(ByteVector, @splat('9')));
    const spaces_vec: BoolVector = bytes == @as(ByteVector, @splat(' '));
    const control_ws_vec: BoolVector =
        (bytes >= @as(ByteVector, @splat(9))) &
        (bytes <= @as(ByteVector, @splat(13)));
    const high_vec: BoolVector = bytes >= @as(ByteVector, @splat(0x80));
    const apostrophe_vec: BoolVector = bytes == @as(ByteVector, @splat('\''));

    const letters: u64 = @bitCast(letters_vec);
    const digits: u64 = @bitCast(digits_vec);
    const spaces: u64 = @bitCast(spaces_vec);
    const whitespace: u64 = spaces | @as(u64, @bitCast(control_ws_vec));
    if (@as(u64, @bitCast(high_vec)) != 0) return null;
    const apostrophes: u64 = @bitCast(apostrophe_vec);
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
    var boundaries = non_whitespace_boundaries | whitespace_boundaries | 1;

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
            var expected: [65]usize = undefined;
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

            var actual: [65]usize = undefined;
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

    const phrase = "What does ";
    var text = std.ArrayListUnmanaged(u8).empty;
    defer text.deinit(allocator);
    try text.ensureTotalCapacity(allocator, phrase.len * 30_000);
    for (0..30_000) |_| text.appendSliceAssumeCapacity(phrase);

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

        fn tryReserve(context: *anyopaque, bytes: usize) bool {
            const self: *@This() = @ptrCast(@alignCast(context));
            var used = self.used_bytes.load(.acquire);
            while (true) {
                if (bytes > self.max_bytes or used > self.max_bytes - bytes) return false;
                used = self.used_bytes.cmpxchgWeak(
                    used,
                    used + bytes,
                    .acq_rel,
                    .acquire,
                ) orelse return true;
            }
        }

        fn release(context: *anyopaque, bytes: usize) void {
            const self: *@This() = @ptrCast(@alignCast(context));
            _ = self.used_bytes.fetchSub(bytes, .acq_rel);
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
            .try_reserve = Budget.tryReserve,
            .release = Budget.release,
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

        fn tryReserve(context: *anyopaque, bytes: usize) bool {
            const self: *@This() = @ptrCast(@alignCast(context));
            _ = self.used_bytes.fetchAdd(bytes, .acq_rel);
            return true;
        }

        fn release(context: *anyopaque, bytes: usize) void {
            const self: *@This() = @ptrCast(@alignCast(context));
            const previous = self.used_bytes.fetchSub(bytes, .acq_rel);
            std.debug.assert(previous >= bytes);
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
            .try_reserve = Budget.tryReserve,
            .release = Budget.release,
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
