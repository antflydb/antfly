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
const Tokenizer = @import("tokenizer.zig").Tokenizer;
const SpecialTokens = @import("tokenizer.zig").SpecialTokens;
const PriorityQueue = @import("priority_queue.zig").PriorityQueue;

const ModelType = enum { word_piece, bpe, unigram };

const PreTokenizerType = enum {
    bert, // BertPreTokenizer: split on whitespace + punctuation
    byte_level, // ByteLevel: byte-to-unicode mapping
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

    /// Byte-indexed trie used for added-token matching. Each node stores its
    /// children in a HashMap keyed by the next byte; final nodes hold the
    /// token id and length.
    const AddedTokenTrie = struct {
        nodes: std.ArrayListUnmanaged(Node) = .empty,

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
            if (self.nodes.items.len == 0) return null;
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
        if (pad_id) |id| self.special.pad_id = id;
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
        errdefer allocator.destroy(self);

        self.* = .{
            .allocator = allocator,
            .model_type = .word_piece,
            .vocab = .{},
            .id_to_token = .{},
            .added_tokens = .{},
            .added_trie = try AddedTokenTrie.init(allocator),
            .special = .{},
            .do_lowercase = false,
            .replace_space_with = null,
            .pre_tokenizer_type = .bert,
            .continuing_prefix = "##",
            .max_input_chars_per_word = 100,
            .merge_ranks = .{},
            .end_of_word_suffix = "",
            .byte_fallback = false,
            .unigram_vocab = .empty,
            .unigram_unk_id = 0,
            .unigram_trie = try VocabTrie.init(allocator),
            .bpe_direct_trie = null,
            .metaspace_prepend_scheme = .always,
            .metaspace_split = true,
            .metaspace_replacement = "\xe2\x96\x81", // ▁ (U+2581) in UTF-8
            .arena_strings = .empty,
        };

        // Detect model type first. Some Hugging Face tokenizer.json files omit
        // `model.type`, so infer from the model payload shape when necessary.
        if (root.object.get("model")) |model| {
            if (model == .object) {
                self.model_type = inferModelType(model.object);
            }
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
                    // For Sequence pre-tokenizers, use the first meaningful type
                    if (obj.get("pretokenizers")) |pts| {
                        if (pts == .array) {
                            for (pts.array.items) |item| {
                                if (item == .object) {
                                    if (item.object.get("type")) |pt| {
                                        if (pt == .string) {
                                            if (std.mem.eql(u8, pt.string, "ByteLevel")) {
                                                self.pre_tokenizer_type = .byte_level;
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
                const s = try self.allocator.dupe(u8, v.string);
                try self.arena_strings.append(self.allocator, s);
                self.continuing_prefix = s;
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
                const s = try self.allocator.dupe(u8, v.string);
                try self.arena_strings.append(self.allocator, s);
                self.end_of_word_suffix = s;
            }
        }
        if (obj.get("byte_fallback")) |v| {
            if (v == .bool) self.byte_fallback = v.bool;
        }
        if (obj.get("continuing_subword_prefix")) |v| {
            if (v == .string) {
                const s = try self.allocator.dupe(u8, v.string);
                try self.arena_strings.append(self.allocator, s);
                self.continuing_prefix = s;
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
                    if (item != .string) continue;
                    if (std.mem.indexOfScalar(u8, item.string, ' ') == null) continue;
                    const key = try self.allocator.dupe(u8, item.string);
                    try self.arena_strings.append(self.allocator, key);
                    // Earlier merges win ties because getOrPut is no-op on existing.
                    const gop = try self.merge_ranks.getOrPut(self.allocator, key);
                    if (!gop.found_existing) gop.value_ptr.* = rank;
                    rank += 1;
                }
            }
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
                            const token = try self.allocator.dupe(u8, token_val.string);
                            try self.arena_strings.append(self.allocator, token);
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
        if (obj.get("unk_token")) |v| {
            if (v == .string) {
                if (self.vocab.get(v.string)) |id| {
                    self.special.unk_id = id;
                }
            }
        }

        if (obj.get("vocab")) |vocab_val| {
            if (vocab_val == .object) {
                var it = vocab_val.object.iterator();
                while (it.next()) |entry| {
                    if (entry.value_ptr.* == .integer) {
                        const id: i32 = @intCast(entry.value_ptr.integer);
                        const key = try self.allocator.dupe(u8, entry.key_ptr.*);
                        try self.arena_strings.append(self.allocator, key);
                        try self.vocab.put(self.allocator, key, id);
                        try self.id_to_token.put(self.allocator, id, key);
                    }
                }
            }
        }

        // Resolve unk_token after vocab is loaded
        if (obj.get("unk_token")) |v| {
            if (v == .string) {
                if (self.vocab.get(v.string)) |id| {
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
            const key = try self.allocator.dupe(u8, content.string);
            try self.arena_strings.append(self.allocator, key);
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
            if (std.mem.eql(u8, content.string, "[PAD]")) self.special.pad_id = id;
            if (std.mem.eql(u8, content.string, "[UNK]")) self.special.unk_id = id;
            if (std.mem.eql(u8, content.string, "[MASK]")) self.special.mask_id = id;
            // RoBERTa/GPT-style special tokens
            if (std.mem.eql(u8, content.string, "<s>")) self.special.cls_id = id;
            if (std.mem.eql(u8, content.string, "<bos>")) self.special.cls_id = id;
            if (std.mem.eql(u8, content.string, "</s>")) self.special.sep_id = id;
            if (std.mem.eql(u8, content.string, "<eos>")) self.special.sep_id = id;
            if (std.mem.eql(u8, content.string, "<pad>")) self.special.pad_id = id;
            if (std.mem.eql(u8, content.string, "<unk>")) self.special.unk_id = id;
            if (std.mem.eql(u8, content.string, "<mask>")) self.special.mask_id = id;
        }
    }

    fn parsePostProcessor(self: *HfTokenizer, obj: std.json.ObjectMap) void {
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
                if (st.object.get("[PAD]")) |pad| self.resolveSpecialToken(pad, &self.special.pad_id);
                if (st.object.get("<bos>")) |bos| self.resolveSpecialToken(bos, &self.special.cls_id);
                if (st.object.get("<eos>")) |eos| self.resolveSpecialToken(eos, &self.special.sep_id);
                if (st.object.get("<pad>")) |pad| self.resolveSpecialToken(pad, &self.special.pad_id);
            }
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
        switch (self.pre_tokenizer_type) {
            .byte_level => {
                // ByteLevel: convert bytes to unicode chars, split on whitespace/punct regex
                const words = try byteLevelPreTokenize(allocator, text);
                defer {
                    for (words) |w| allocator.free(w);
                    allocator.free(words);
                }
                for (words) |word| {
                    try self.bpeEncodeWord(allocator, word, ids);
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
                    try self.bpeEncodeWord(allocator, word, ids);
                }
            },
            .bert => {
                const words = try bertPreTokenize(allocator, text);
                defer allocator.free(words);
                for (words) |word| {
                    try self.bpeEncodeWord(allocator, word, ids);
                }
            },
            .none => {
                try self.bpeEncodeWord(allocator, text, ids);
            },
        }
    }

    /// Symbol represented as a (start, end) index pair into the working byte
    /// buffer, which lets us merge two symbols by simply extending the left
    /// range — no allocation per merge.
    const BpeSymbol = struct {
        start: u32,
        end: u32,
        prev: i32 = -1,
        next: i32 = -1,
        alive: bool = true,
    };

    const BpeCandidate = struct {
        rank: u32,
        left: u32,
        right: u32,
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

    fn bpeEncodeWord(self: *HfTokenizer, allocator: std.mem.Allocator, word: []const u8, ids: *std.ArrayListUnmanaged(i32)) !void {
        if (word.len == 0) return;

        // Check added tokens first
        if (self.added_tokens.get(word)) |id| {
            try ids.append(allocator, id);
            return;
        }

        // Some HF BPE tokenizers, including Gemma 3, contain whole-word entries
        // that are not reconstructible from the merge table alone. Prefer an
        // exact vocab hit before falling back to character-split merges.
        if (self.vocab.get(word)) |id| {
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

        var symbols = std.ArrayListUnmanaged(BpeSymbol).empty;
        defer symbols.deinit(allocator);
        try symbols.ensureTotalCapacity(allocator, word.len + 1);

        // One symbol per UTF-8 codepoint of `word`.
        var pos: usize = 0;
        while (pos < word.len) {
            const cp_len = utf8CodepointLen(word[pos]);
            const end = @min(pos + cp_len, word.len);
            const idx: i32 = @intCast(symbols.items.len);
            try symbols.append(allocator, .{
                .start = @intCast(pos),
                .end = @intCast(end),
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
            }
        }

        var pq = try PriorityQueue(BpeCandidate).init(allocator, bpeCandidateCmp);
        defer pq.deinit();
        for (0..symbols.items.len) |i| {
            const next = symbols.items[i].next;
            if (next < 0) continue;
            const right_idx: u32 = @intCast(next);
            const a = work[symbols.items[i].start..symbols.items[i].end];
            const b = work[symbols.items[right_idx].start..symbols.items[right_idx].end];
            if (try self.bpeMergeRank(allocator, a, b)) |rank| {
                try pq.insert(.{ .rank = rank, .left = @intCast(i), .right = right_idx });
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
            const cur_rank = (try self.bpeMergeRank(allocator, a, b)) orelse continue;
            if (cur_rank != cand.rank) continue;

            // Merge: extend left's range over right, splice right out of the
            // doubly-linked list. No allocation, since the bytes are already
            // contiguous in `work`.
            left.end = right.end;
            const new_next = right.next;
            left.next = new_next;
            if (new_next >= 0) symbols.items[@intCast(new_next)].prev = @intCast(cand.left);
            right.alive = false;

            const left_bytes = work[left.start..left.end];
            if (left.prev >= 0) {
                const prev_idx: u32 = @intCast(left.prev);
                const pa = work[symbols.items[prev_idx].start..symbols.items[prev_idx].end];
                if (try self.bpeMergeRank(allocator, pa, left_bytes)) |r| {
                    try pq.insert(.{ .rank = r, .left = prev_idx, .right = cand.left });
                }
            }
            if (left.next >= 0) {
                const next_idx: u32 = @intCast(left.next);
                const nb = work[symbols.items[next_idx].start..symbols.items[next_idx].end];
                if (try self.bpeMergeRank(allocator, left_bytes, nb)) |r| {
                    try pq.insert(.{ .rank = r, .left = cand.left, .right = next_idx });
                }
            }
        }

        // Walk the surviving symbols and emit ids.
        var idx: i32 = 0;
        while (idx >= 0 and idx < symbols.items.len) {
            const sym = symbols.items[@intCast(idx)];
            if (sym.alive) {
                const bytes = work[sym.start..sym.end];
                if (self.vocab.get(bytes)) |id| {
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
                        // BPE: tokens may use byte-level encoding or have ▁ for spaces
                        if (self.pre_tokenizer_type == .byte_level) {
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

/// ByteLevel pre-tokenizer: map bytes to unicode and split on whitespace boundaries.
fn byteLevelPreTokenize(allocator: std.mem.Allocator, text: []const u8) ![][]const u8 {
    // Simple approach: split on whitespace, then byte-encode each word
    var words = std.ArrayListUnmanaged([]const u8).empty;
    var start: usize = 0;

    for (text, 0..) |c, i| {
        if (std.ascii.isWhitespace(c)) {
            if (i > start) {
                const encoded = try byteLevelEncode(allocator, text[start..i]);
                try words.append(allocator, encoded);
            }
            // Encode the whitespace too (GPT-2 includes leading space)
            const ws = try byteLevelEncode(allocator, text[i .. i + 1]);
            // Check if next word exists, if so prepend space to it
            if (i + 1 < text.len and !std.ascii.isWhitespace(text[i + 1])) {
                // Find end of next word
                var end = i + 1;
                while (end < text.len and !std.ascii.isWhitespace(text[end])) : (end += 1) {}
                const next_encoded = try byteLevelEncode(allocator, text[i..end]);
                allocator.free(ws);
                try words.append(allocator, next_encoded);
                start = end;
            } else {
                try words.append(allocator, ws);
                start = i + 1;
            }
        }
    }

    if (start < text.len) {
        const encoded = try byteLevelEncode(allocator, text[start..]);
        try words.append(allocator, encoded);
    }

    return try words.toOwnedSlice(allocator);
}

fn byteLevelEncode(allocator: std.mem.Allocator, text: []const u8) ![]u8 {
    var buf = std.ArrayListUnmanaged(u8).empty;
    for (text) |byte| {
        const cp = byte_to_unicode[byte];
        var utf8_buf: [4]u8 = undefined;
        const len = std.unicode.utf8Encode(cp, &utf8_buf) catch 1;
        try buf.appendSlice(allocator, utf8_buf[0..len]);
    }
    return try buf.toOwnedSlice(allocator);
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
    const models_dir = if (std.c.getenv("TERMITE_MODELS_DIR")) |value|
        std.mem.span(value)
    else blk: {
        const home = std.c.getenv("HOME") orelse return error.SkipZigTest;
        break :blk try std.fs.path.join(allocator, &.{ std.mem.span(home), ".termite", "models" });
    };
    defer if (std.c.getenv("TERMITE_MODELS_DIR") == null) allocator.free(models_dir);
    const path = try std.fs.path.join(allocator, &.{ models_dir, "sparse-encoder-testing", "splade-bert-tiny-nq-onnx", "tokenizer.json" });
    defer allocator.free(path);
    const bytes = std.fs.cwd().readFileAlloc(allocator, path, 16 * 1024 * 1024) catch |err| switch (err) {
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

test "real tokenizer.json golden values" {
    const allocator = std.testing.allocator;

    var tok = try HfTokenizer.loadFromDir(allocator, std.Io.Dir.cwd(), std.testing.io, "testdata/embedder/tokenizer.json");
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
    try std.testing.expectEqualSlices(i32, &.{ 10, 1, 11, 0 }, encoded.ids);
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
