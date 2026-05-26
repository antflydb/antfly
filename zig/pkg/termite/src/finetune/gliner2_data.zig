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
const compat = @import("../io/compat.zig");

pub const Entity = struct {
    text: []const u8,
    label: []const u8,
    start: usize,
    end: usize,
};

pub const Example = struct {
    text: []const u8,
    entities: []Entity,
};

pub const DatasetStats = struct {
    num_examples: usize = 0,
    avg_text_chars: f64 = 0,
    avg_entities: f64 = 0,
    unique_labels: usize = 0,
};

pub const TargetCoverageStats = struct {
    num_samples: usize = 0,
    total_entities: usize = 0,
    target_entities: usize = 0,
    samples_with_target: usize = 0,
    samples_without_target: usize = 0,
};

pub const SpanTargetSummary = struct {
    max_words: usize,
    max_span_width: usize,
    num_spans: usize,
    valid_spans: usize,
    positive_labels: usize,
};

pub const BatchShapeSummary = struct {
    batch_size: usize,
    max_length: usize,
    num_entity_types: usize,
    max_words_per_sample: usize,
    max_spans: usize,
    valid_spans: usize,
    positive_labels: usize,
    positive_rate_per_label: f64,
};

pub const DatasetSpanTargetSummary = struct {
    num_examples: usize,
    max_length: usize,
    max_span_width: usize,
    num_entity_types: usize,
    max_words_per_sample: usize,
    max_spans_per_sample: usize,
    valid_spans: usize,
    positive_labels: usize,
    positive_rate_per_label: f64,
};

pub const DatasetReadinessOptions = struct {
    min_examples: usize = 1,
    min_total_entities: usize = 1,
    min_unique_labels: usize = 1,
    min_target_entities: usize = 1,
    min_target_coverage_ratio: f64 = 0.0,
    require_all_examples_with_target: bool = false,
    min_positive_span_labels: usize = 1,
    min_positive_rate_per_label: f64 = 0.0,
};

pub const DatasetReadinessSummary = struct {
    stats: DatasetStats,
    coverage: TargetCoverageStats,
    batch_shape: BatchShapeSummary,
    span_targets: DatasetSpanTargetSummary,
    filtered_examples: usize,
    target_coverage_ratio: f64,
    passed: bool,
    failed_reasons: []const []const u8,
};

pub const LoadedExamples = struct {
    arena: std.heap.ArenaAllocator,
    dataset_root: []const u8,
    examples: []Example,

    pub fn deinit(self: *LoadedExamples) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

pub const Tokenizer = struct {
    const MetaspacePrependScheme = enum {
        always,
        first,
        never,
    };

    vocab: std.StringHashMapUnmanaged(i32) = .empty,
    vocab_size: i32 = 0,
    pad_id: i32 = 0,
    cls_id: i32 = 1,
    sep_id: i32 = 2,
    unk_id: i32 = 3,
    ent_id: i32 = 4,
    ent_sep_id: i32 = 5,
    p_token_id: i32 = 0,
    sep_text_token_id: i32 = 2,
    use_gliner2_hf_prompt: bool = false,
    hf_unigram_scores: []f32 = &.{},
    hf_metaspace_prepend_scheme: MetaspacePrependScheme = .always,
    hf_metaspace_split: bool = true,

    pub const EncodeIntoResult = struct {
        num_words: usize,
    };

    pub fn initDefault(allocator: std.mem.Allocator) !Tokenizer {
        var tok = Tokenizer{};
        try tok.vocab.put(allocator, try allocator.dupe(u8, "[PAD]"), 0);
        try tok.vocab.put(allocator, try allocator.dupe(u8, "[CLS]"), 1);
        try tok.vocab.put(allocator, try allocator.dupe(u8, "[SEP]"), 2);
        try tok.vocab.put(allocator, try allocator.dupe(u8, "[UNK]"), 3);
        try tok.vocab.put(allocator, try allocator.dupe(u8, "<<ENT>>"), 4);
        try tok.vocab.put(allocator, try allocator.dupe(u8, "<<SEP>>"), 5);
        const words = [_][]const u8{
            "the",   "a",         "an",     "is",      "are",  "was",     "were",      "be",        "of",           "in",       "for",     "on",
            "with",  "at",        "by",     "from",    "and",  "or",      "but",       "person",    "organization", "location", "product", "event",
            "other", "building",  "art",    "company", "city", "country", "ceo",       "president", "director",     "inc",      "corp",    "google",
            "apple", "microsoft", "amazon", "new",     "york", "san",     "francisco", "london",
        };
        for (words, 0..) |word, i| {
            try tok.vocab.put(allocator, try allocator.dupe(u8, word), @as(i32, @intCast(i + 6)));
        }
        tok.vocab_size = @intCast(tok.vocab.count());
        return tok;
    }

    pub fn initGLiNER2HF(allocator: std.mem.Allocator, model_input: []const u8) !Tokenizer {
        const tokenizer_path = try resolveTokenizerJsonPath(allocator, model_input);
        defer allocator.free(tokenizer_path);

        const raw = try compat.cwd().readFileAlloc(compat.io(), tokenizer_path, allocator, .limited(32 * 1024 * 1024));
        defer allocator.free(raw);

        var parsed = try std.json.parseFromSlice(std.json.Value, allocator, raw, .{});
        defer parsed.deinit();

        const root = parsed.value.object;
        const model = root.get("model") orelse return error.InvalidTokenizerJson;
        if (model != .object) return error.InvalidTokenizerJson;
        const model_type = model.object.get("type") orelse return error.InvalidTokenizerJson;
        if (model_type != .string or !std.mem.eql(u8, model_type.string, "Unigram")) return error.UnsupportedTokenizerModel;
        const vocab_value = model.object.get("vocab") orelse return error.InvalidTokenizerJson;
        if (vocab_value != .array) return error.InvalidTokenizerJson;

        var tok = Tokenizer{
            .use_gliner2_hf_prompt = true,
            .hf_unigram_scores = try allocator.alloc(f32, vocab_value.array.items.len),
        };
        errdefer tok.deinit(allocator);

        for (vocab_value.array.items, 0..) |entry, idx| {
            if (entry != .array or entry.array.items.len < 2) continue;
            const token_name_value = entry.array.items[0];
            const score_value = entry.array.items[1];
            if (token_name_value != .string) continue;
            const token_name = token_name_value.string;
            try tok.vocab.put(allocator, try allocator.dupe(u8, token_name), @intCast(idx));
            tok.hf_unigram_scores[idx] = switch (score_value) {
                .float => @floatCast(score_value.float),
                .integer => @floatFromInt(score_value.integer),
                else => 0,
            };
        }

        tok.vocab_size = @intCast(vocab_value.array.items.len);
        tok.pad_id = tok.vocab.get("[PAD]") orelse 0;
        tok.cls_id = tok.vocab.get("[CLS]") orelse 1;
        tok.sep_id = tok.vocab.get("[SEP]") orelse 2;
        tok.unk_id = tok.vocab.get("[UNK]") orelse 3;

        if (root.get("pre_tokenizer")) |pre| {
            if (pre == .object) tok.parseHFPreTokenizer(pre.object);
        }

        const added_tokens = root.get("added_tokens") orelse return error.InvalidTokenizerJson;
        if (added_tokens != .array) return error.InvalidTokenizerJson;
        for (added_tokens.array.items) |entry| {
            if (entry != .object) continue;
            const id_val = entry.object.get("id") orelse continue;
            const content_val = entry.object.get("content") orelse continue;
            if (id_val != .integer or content_val != .string) continue;
            const id: i32 = @intCast(id_val.integer);
            const content = content_val.string;
            if (std.mem.eql(u8, content, "[E]")) tok.ent_id = id;
            if (std.mem.eql(u8, content, "[P]")) tok.p_token_id = id;
            if (std.mem.eql(u8, content, "[SEP_TEXT]")) tok.sep_text_token_id = id;
        }
        if (tok.ent_id <= 0 or tok.p_token_id <= 0 or tok.sep_text_token_id <= 0) return error.InvalidTokenizerJson;
        tok.ent_sep_id = tok.sep_text_token_id;
        return tok;
    }

    pub fn deinit(self: *Tokenizer, allocator: std.mem.Allocator) void {
        var iter = self.vocab.iterator();
        while (iter.next()) |entry| allocator.free(entry.key_ptr.*);
        self.vocab.deinit(allocator);
        if (self.hf_unigram_scores.len > 0) allocator.free(self.hf_unigram_scores);
        self.* = undefined;
    }

    pub fn encodeInto(
        self: *const Tokenizer,
        allocator: std.mem.Allocator,
        text: []const u8,
        entity_types: []const []const u8,
        input_ids: []i32,
        attention_mask: []i32,
        words_mask: []i32,
        first_token_positions: []i32,
        e_token_positions: []i32,
        e_token_end_positions: []i32,
    ) EncodeIntoResult {
        @memset(input_ids, 0);
        @memset(attention_mask, 0);
        @memset(words_mask, 0);
        @memset(first_token_positions, 0);
        @memset(e_token_positions, -1);
        @memset(e_token_end_positions, -1);

        const max_length = input_ids.len;
        var pos: usize = 0;
        if (self.use_gliner2_hf_prompt) {
            if (pos < max_length) {
                input_ids[pos] = self.p_token_id;
                attention_mask[pos] = 1;
                pos += 1;
            }

            for (entity_types, 0..) |entity_type, i| {
                if (pos >= max_length - 1) break;
                const label_start = pos;
                const new_pos = self.encodeHFFragmentIntoAllocating(allocator, entity_type, input_ids, attention_mask, pos, max_length - 1) catch pos;
                if (new_pos >= max_length - 1) {
                    // Partial encode: zero out any tokens already written so the sequence
                    // boundary is clean and the model doesn't see a truncated entity label.
                    @memset(input_ids[label_start..new_pos], 0);
                    @memset(attention_mask[label_start..new_pos], 0);
                    break;
                }
                pos = new_pos;
                e_token_positions[i] = @intCast(label_start);
                e_token_end_positions[i] = @intCast(pos);
                input_ids[pos] = self.ent_id;
                attention_mask[pos] = 1;
                pos += 1;
            }

            if (pos < max_length - 1) {
                input_ids[pos] = self.sep_text_token_id;
                attention_mask[pos] = 1;
                pos += 1;
            }

            var num_words_hf: usize = 0;
            var words_hf = std.mem.tokenizeAny(u8, text, " \t\r\n");
            while (words_hf.next()) |word| {
                if (num_words_hf >= first_token_positions.len) break;
                if (pos >= max_length - 1) break;
                first_token_positions[num_words_hf] = @intCast(pos);
                const next_pos = self.encodeHFFragmentIntoAllocating(allocator, word, input_ids, attention_mask, pos, max_length - 1) catch pos;
                if (next_pos == pos) break;
                for (pos..next_pos) |token_pos| words_mask[token_pos] = @intCast(num_words_hf + 1);
                pos = next_pos;
                num_words_hf += 1;
            }
            return .{ .num_words = num_words_hf };
        }

        if (pos < max_length) {
            input_ids[pos] = self.cls_id;
            attention_mask[pos] = 1;
            pos += 1;
        }

        for (entity_types, 0..) |entity_type, i| {
            if (pos >= max_length - 1) break;
            e_token_positions[i] = @intCast(pos);
            input_ids[pos] = self.ent_id;
            attention_mask[pos] = 1;
            pos += 1;
            var type_words = std.mem.tokenizeAny(u8, entity_type, " \t\r\n");
            while (type_words.next()) |word| {
                if (pos >= max_length - 1) break;
                input_ids[pos] = self.tokenId(word);
                attention_mask[pos] = 1;
                pos += 1;
            }
            e_token_end_positions[i] = @intCast(pos);
            if (pos < max_length - 1) {
                input_ids[pos] = self.ent_sep_id;
                attention_mask[pos] = 1;
                pos += 1;
            }
        }

        var num_words: usize = 0;
        var words = std.mem.tokenizeAny(u8, text, " \t\r\n");
        while (words.next()) |word| {
            if (num_words >= first_token_positions.len) break;
            if (pos >= max_length - 1) break;
            first_token_positions[num_words] = @intCast(pos);
            input_ids[pos] = self.tokenId(word);
            attention_mask[pos] = 1;
            words_mask[pos] = @intCast(num_words + 1);
            pos += 1;
            num_words += 1;
        }

        if (pos < max_length) {
            input_ids[pos] = self.sep_id;
            attention_mask[pos] = 1;
        }
        return .{ .num_words = num_words };
    }

    fn tokenId(self: *const Tokenizer, raw: []const u8) i32 {
        var lower_buf: [128]u8 = undefined;
        const trimmed = std.mem.trim(u8, raw, ".,!?;:\"'()[]{}/-");
        if (trimmed.len == 0) return self.unk_id;
        if (self.use_gliner2_hf_prompt) {
            var meta_buf: [256]u8 = undefined;
            const needed = "▁".len + trimmed.len;
            if (needed <= meta_buf.len) {
                @memcpy(meta_buf[0.."▁".len], "▁");
                @memcpy(meta_buf["▁".len .. "▁".len + trimmed.len], trimmed);
                if (self.vocab.get(meta_buf[0 .. "▁".len + trimmed.len])) |id| return id;
            }
            if (self.vocab.get(trimmed)) |id| return id;
            return self.unk_id;
        }
        const n = @min(trimmed.len, lower_buf.len);
        for (trimmed[0..n], 0..) |ch, i| lower_buf[i] = std.ascii.toLower(ch);
        return self.vocab.get(lower_buf[0..n]) orelse self.unk_id;
    }

    fn parseHFPreTokenizer(self: *Tokenizer, obj: std.json.ObjectMap) void {
        if (obj.get("type")) |t| {
            if (t == .string and std.mem.eql(u8, t.string, "Metaspace")) {
                self.parseHFMetaspaceConfig(obj);
                return;
            }
            if (t == .string and std.mem.eql(u8, t.string, "Sequence")) {
                if (obj.get("pretokenizers")) |pts| {
                    if (pts == .array) {
                        for (pts.array.items) |item| {
                            if (item != .object) continue;
                            if (item.object.get("type")) |pt| {
                                if (pt == .string and std.mem.eql(u8, pt.string, "Metaspace")) {
                                    self.parseHFMetaspaceConfig(item.object);
                                    return;
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    fn parseHFMetaspaceConfig(self: *Tokenizer, obj: std.json.ObjectMap) void {
        if (obj.get("prepend_scheme")) |ps| {
            if (ps == .string) {
                if (std.mem.eql(u8, ps.string, "always")) self.hf_metaspace_prepend_scheme = .always;
                if (std.mem.eql(u8, ps.string, "first")) self.hf_metaspace_prepend_scheme = .first;
                if (std.mem.eql(u8, ps.string, "never")) self.hf_metaspace_prepend_scheme = .never;
            }
        }
        if (obj.get("split")) |split| {
            if (split == .bool) self.hf_metaspace_split = split.bool;
        }
    }

    fn encodeHFFragmentIntoAllocating(
        self: *const Tokenizer,
        allocator: std.mem.Allocator,
        text: []const u8,
        input_ids: []i32,
        attention_mask: []i32,
        pos: usize,
        limit: usize,
    ) !usize {
        return try self.encodeHFFragmentInto(allocator, text, input_ids, attention_mask, pos, limit);
    }

    fn encodeHFFragmentInto(
        self: *const Tokenizer,
        allocator: std.mem.Allocator,
        text: []const u8,
        input_ids: []i32,
        attention_mask: []i32,
        start_pos: usize,
        limit: usize,
    ) !usize {
        var pos = start_pos;
        const pieces = try self.metaspacePreTokenizeWithScheme(allocator, text, .never);
        defer {
            for (pieces) |piece| allocator.free(piece);
            allocator.free(pieces);
        }
        for (pieces) |piece| {
            const ids = try self.unigramEncodePieceAlloc(allocator, piece);
            defer allocator.free(ids);
            for (ids) |id| {
                if (pos >= limit) return pos;
                input_ids[pos] = id;
                attention_mask[pos] = 1;
                pos += 1;
            }
        }
        return pos;
    }

    fn metaspacePreTokenizeWithScheme(
        self: *const Tokenizer,
        allocator: std.mem.Allocator,
        text: []const u8,
        prepend_scheme: MetaspacePrependScheme,
    ) ![][]const u8 {
        var words = std.ArrayListUnmanaged([]const u8).empty;
        if (!self.hf_metaspace_split) {
            var prepared = std.ArrayListUnmanaged(u8).empty;
            defer prepared.deinit(allocator);
            if (text.len > 0 and prepend_scheme != .never) {
                try prepared.appendSlice(allocator, "▁");
            }
            for (text) |ch| {
                if (ch == ' ') {
                    try prepared.appendSlice(allocator, "▁");
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
                try words.append(allocator, try std.fmt.allocPrint(allocator, "▁{s}", .{segment}));
            } else {
                try words.append(allocator, try allocator.dupe(u8, segment));
            }
            first = false;
        }
        return try words.toOwnedSlice(allocator);
    }

    fn unigramEncodePieceAlloc(self: *const Tokenizer, allocator: std.mem.Allocator, piece: []const u8) ![]i32 {
        var ids = std.ArrayListUnmanaged(i32).empty;
        defer ids.deinit(allocator);
        const n = piece.len;
        if (n == 0) return try ids.toOwnedSlice(allocator);

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
            const max_len = @min(n - start, 128);
            for (1..max_len + 1) |len| {
                const end = start + len;
                const sub = piece[start..end];
                var score: f32 = -std.math.inf(f32);
                if (self.vocab.get(sub)) |id| {
                    const idx: usize = @intCast(id);
                    if (idx < self.hf_unigram_scores.len) score = self.hf_unigram_scores[idx];
                }
                if (score == -std.math.inf(f32)) continue;
                const candidate = best_score[start] + score;
                if (candidate > best_score[end]) {
                    best_score[end] = candidate;
                    best_len[end] = len;
                }
            }
        }

        var segments = std.ArrayListUnmanaged([]const u8).empty;
        defer segments.deinit(allocator);
        var pos: usize = n;
        while (pos > 0) {
            const len = best_len[pos];
            if (len == 0) {
                try ids.append(allocator, self.unk_id);
                return try ids.toOwnedSlice(allocator);
            }
            try segments.append(allocator, piece[pos - len .. pos]);
            pos -= len;
        }

        var i = segments.items.len;
        while (i > 0) {
            i -= 1;
            try ids.append(allocator, self.vocab.get(segments.items[i]) orelse self.unk_id);
        }
        return try ids.toOwnedSlice(allocator);
    }
};

pub const EncodedBatch = struct {
    allocator: std.mem.Allocator,
    owns_memory: bool = true,
    input_ids: []i32,
    attention_mask: []i32,
    words_mask: []i32,
    first_token_positions: []i32,
    word_lengths: []f32,
    word_has_digit: []f32,
    word_is_title: []f32,
    word_is_all_caps: []f32,
    span_indices: []i32,
    span_mask: []f32,
    span_labels: []f32,
    e_token_positions: []i32,
    e_token_end_positions: []i32,
    entity_type_kind: []i32,
    batch_size: usize,
    max_length: usize,
    max_words_per_sample: usize,
    max_spans: usize,
    num_entity_types: usize,

    pub fn deinit(self: *EncodedBatch) void {
        if (!self.owns_memory) {
            self.* = undefined;
            return;
        }
        self.allocator.free(self.input_ids);
        self.allocator.free(self.attention_mask);
        self.allocator.free(self.words_mask);
        self.allocator.free(self.first_token_positions);
        self.allocator.free(self.word_lengths);
        self.allocator.free(self.word_has_digit);
        self.allocator.free(self.word_is_title);
        self.allocator.free(self.word_is_all_caps);
        self.allocator.free(self.span_indices);
        self.allocator.free(self.span_mask);
        self.allocator.free(self.span_labels);
        self.allocator.free(self.e_token_positions);
        self.allocator.free(self.e_token_end_positions);
        self.allocator.free(self.entity_type_kind);
        self.* = undefined;
    }
};

pub const SpanPrediction = struct {
    sample_index: usize,
    span_index: usize,
    word_start: usize,
    word_end: usize,
    entity_type_index: usize,
    label: []const u8,
    score: f32,
};

pub const EntityPrediction = struct {
    sample_index: usize,
    span_index: usize,
    word_start: usize,
    word_end: usize,
    start: usize,
    end: usize,
    text: []const u8,
    entity_type_index: usize,
    label: []const u8,
    score: f32,
};

pub fn decodeSpanPredictionsAlloc(
    allocator: std.mem.Allocator,
    batch: *const EncodedBatch,
    entity_types: []const []const u8,
    span_scores: []const f32,
    threshold: f32,
) ![]SpanPrediction {
    if (entity_types.len != batch.num_entity_types) return error.EntityTypeCountMismatch;
    if (!std.math.isFinite(threshold)) return error.InvalidThreshold;
    const expected_scores = batch.batch_size * batch.max_spans * batch.num_entity_types;
    if (span_scores.len != expected_scores) return error.SpanScoreShapeMismatch;

    var out = std.ArrayListUnmanaged(SpanPrediction).empty;
    errdefer out.deinit(allocator);

    for (0..batch.batch_size) |sample_idx| {
        for (0..batch.max_spans) |span_idx| {
            const flat_span_idx = sample_idx * batch.max_spans + span_idx;
            if (batch.span_mask[flat_span_idx] <= 0.0) continue;

            const start_raw = batch.span_indices[flat_span_idx * 2];
            const end_raw = batch.span_indices[flat_span_idx * 2 + 1];
            if (start_raw < 0 or end_raw < 0) continue;

            for (0..batch.num_entity_types) |entity_type_idx| {
                const score_idx = flat_span_idx * batch.num_entity_types + entity_type_idx;
                const score = span_scores[score_idx];
                if (!std.math.isFinite(score) or score < threshold) continue;
                try out.append(allocator, .{
                    .sample_index = sample_idx,
                    .span_index = span_idx,
                    .word_start = @intCast(start_raw),
                    .word_end = @intCast(end_raw),
                    .entity_type_index = entity_type_idx,
                    .label = entity_types[entity_type_idx],
                    .score = score,
                });
            }
        }
    }

    return try out.toOwnedSlice(allocator);
}

pub fn tokenLogitsToSpanScoresAlloc(
    allocator: std.mem.Allocator,
    batch: *const EncodedBatch,
    token_logits: []const f32,
    num_classes: usize,
) ![]f32 {
    if (num_classes < batch.num_entity_types + 1) return error.EntityClassCountMismatch;
    const expected_logits = batch.batch_size * batch.max_length * num_classes;
    if (token_logits.len != expected_logits) return error.TokenLogitShapeMismatch;

    const span_scores = try allocator.alloc(f32, batch.batch_size * batch.max_spans * batch.num_entity_types);
    errdefer allocator.free(span_scores);
    @memset(span_scores, 0.0);

    for (0..batch.batch_size) |sample_idx| {
        const word_pos_offset = sample_idx * batch.max_words_per_sample;
        for (0..batch.max_spans) |span_idx| {
            const flat_span_idx = sample_idx * batch.max_spans + span_idx;
            if (batch.span_mask[flat_span_idx] <= 0.0) continue;

            const start_raw = batch.span_indices[flat_span_idx * 2];
            const end_raw = batch.span_indices[flat_span_idx * 2 + 1];
            if (start_raw < 0 or end_raw < 0) continue;
            const word_start: usize = @intCast(start_raw);
            const word_end: usize = @intCast(end_raw);
            if (word_start > word_end or word_end >= batch.max_words_per_sample) return error.InvalidSpanWordIndex;

            const word_count = word_end - word_start + 1;
            for (0..batch.num_entity_types) |entity_type_idx| {
                const class_idx = entity_type_idx + 1;
                var sum: f32 = 0.0;
                for (word_start..word_end + 1) |word_idx| {
                    const token_pos_raw = batch.first_token_positions[word_pos_offset + word_idx];
                    if (token_pos_raw < 0) return error.InvalidTokenPosition;
                    const token_pos: usize = @intCast(token_pos_raw);
                    if (token_pos >= batch.max_length) return error.InvalidTokenPosition;
                    const row = token_logits[(sample_idx * batch.max_length + token_pos) * num_classes ..][0..num_classes];
                    sum += softmaxClassProbability(row, class_idx);
                }
                span_scores[flat_span_idx * batch.num_entity_types + entity_type_idx] =
                    sum / @as(f32, @floatFromInt(word_count));
            }
        }
    }

    return span_scores;
}

pub fn decodeEntityPredictionsAlloc(
    allocator: std.mem.Allocator,
    batch: *const EncodedBatch,
    examples: []const Example,
    entity_types: []const []const u8,
    span_scores: []const f32,
    threshold: f32,
) ![]EntityPrediction {
    if (examples.len < batch.batch_size) return error.ExampleCountMismatch;
    if (entity_types.len != batch.num_entity_types) return error.EntityTypeCountMismatch;
    if (!std.math.isFinite(threshold)) return error.InvalidThreshold;
    const expected_scores = batch.batch_size * batch.max_spans * batch.num_entity_types;
    if (span_scores.len != expected_scores) return error.SpanScoreShapeMismatch;

    var out = std.ArrayListUnmanaged(EntityPrediction).empty;
    errdefer out.deinit(allocator);

    for (0..batch.batch_size) |sample_idx| {
        const word_boundaries = try getWordBoundaries(allocator, examples[sample_idx].text);
        defer allocator.free(word_boundaries);

        for (0..batch.max_spans) |span_idx| {
            const flat_span_idx = sample_idx * batch.max_spans + span_idx;
            if (batch.span_mask[flat_span_idx] <= 0.0) continue;

            const start_raw = batch.span_indices[flat_span_idx * 2];
            const end_raw = batch.span_indices[flat_span_idx * 2 + 1];
            if (start_raw < 0 or end_raw < 0) continue;

            const word_start: usize = @intCast(start_raw);
            const word_end: usize = @intCast(end_raw);
            if (word_start >= word_boundaries.len or word_end >= word_boundaries.len) continue;
            const char_start = word_boundaries[word_start][0];
            const char_end = word_boundaries[word_end][1];
            if (char_start > char_end or char_end > examples[sample_idx].text.len) continue;

            for (0..batch.num_entity_types) |entity_type_idx| {
                const score_idx = flat_span_idx * batch.num_entity_types + entity_type_idx;
                const score = span_scores[score_idx];
                if (!std.math.isFinite(score) or score < threshold) continue;
                try out.append(allocator, .{
                    .sample_index = sample_idx,
                    .span_index = span_idx,
                    .word_start = word_start,
                    .word_end = word_end,
                    .start = char_start,
                    .end = char_end,
                    .text = examples[sample_idx].text[char_start..char_end],
                    .entity_type_index = entity_type_idx,
                    .label = entity_types[entity_type_idx],
                    .score = score,
                });
            }
        }
    }

    return try out.toOwnedSlice(allocator);
}

fn softmaxClassProbability(logits: []const f32, class_idx: usize) f32 {
    std.debug.assert(class_idx < logits.len);
    var max_logit: f32 = -std.math.inf(f32);
    for (logits) |value| {
        if (value > max_logit) max_logit = value;
    }
    var denom: f32 = 0.0;
    for (logits) |value| {
        denom += @exp(value - max_logit);
    }
    if (denom <= 0 or !std.math.isFinite(denom)) return 0;
    return @exp(logits[class_idx] - max_logit) / denom;
}

pub const ReusableBatch = struct {
    allocator: std.mem.Allocator,
    input_ids: []i32,
    attention_mask: []i32,
    words_mask: []i32,
    first_token_positions: []i32,
    word_lengths: []f32,
    word_has_digit: []f32,
    word_is_title: []f32,
    word_is_all_caps: []f32,
    span_indices: []i32,
    span_mask: []f32,
    span_labels: []f32,
    e_token_positions: []i32,
    e_token_end_positions: []i32,
    entity_type_kind: []i32,
    batch_size: usize,
    max_length: usize,
    max_words_per_sample: usize,
    max_spans: usize,
    num_entity_types: usize,

    pub fn init(
        allocator: std.mem.Allocator,
        batch_size: usize,
        max_length: usize,
        max_span_width: usize,
        num_entity_types: usize,
    ) !ReusableBatch {
        const max_words_per_sample = computeMaxWordsPerSample(max_length, num_entity_types);
        const max_spans = max_words_per_sample * max_span_width;
        return .{
            .allocator = allocator,
            .input_ids = try allocator.alloc(i32, batch_size * max_length),
            .attention_mask = try allocator.alloc(i32, batch_size * max_length),
            .words_mask = try allocator.alloc(i32, batch_size * max_length),
            .first_token_positions = try allocator.alloc(i32, batch_size * max_words_per_sample),
            .word_lengths = try allocator.alloc(f32, batch_size * max_words_per_sample),
            .word_has_digit = try allocator.alloc(f32, batch_size * max_words_per_sample),
            .word_is_title = try allocator.alloc(f32, batch_size * max_words_per_sample),
            .word_is_all_caps = try allocator.alloc(f32, batch_size * max_words_per_sample),
            .span_indices = try allocator.alloc(i32, batch_size * max_spans * 2),
            .span_mask = try allocator.alloc(f32, batch_size * max_spans),
            .span_labels = try allocator.alloc(f32, batch_size * max_spans * num_entity_types),
            .e_token_positions = try allocator.alloc(i32, batch_size * num_entity_types),
            .e_token_end_positions = try allocator.alloc(i32, batch_size * num_entity_types),
            .entity_type_kind = try allocator.alloc(i32, batch_size * num_entity_types),
            .batch_size = batch_size,
            .max_length = max_length,
            .max_words_per_sample = max_words_per_sample,
            .max_spans = max_spans,
            .num_entity_types = num_entity_types,
        };
    }

    pub fn deinit(self: *ReusableBatch) void {
        self.allocator.free(self.input_ids);
        self.allocator.free(self.attention_mask);
        self.allocator.free(self.words_mask);
        self.allocator.free(self.first_token_positions);
        self.allocator.free(self.word_lengths);
        self.allocator.free(self.word_has_digit);
        self.allocator.free(self.word_is_title);
        self.allocator.free(self.word_is_all_caps);
        self.allocator.free(self.span_indices);
        self.allocator.free(self.span_mask);
        self.allocator.free(self.span_labels);
        self.allocator.free(self.e_token_positions);
        self.allocator.free(self.e_token_end_positions);
        self.allocator.free(self.entity_type_kind);
        self.* = undefined;
    }
};

pub fn loadExamples(allocator: std.mem.Allocator, path: []const u8, split: ?[]const u8) !LoadedExamples {
    var arena = std.heap.ArenaAllocator.init(allocator);
    errdefer arena.deinit();
    const arena_alloc = arena.allocator();

    var resolved = try resolveJsonlFiles(arena_alloc, path, split);
    defer resolved.deinit();

    var examples: std.ArrayListUnmanaged(Example) = .empty;
    defer examples.deinit(arena_alloc);
    for (resolved.paths) |resolved_path| {
        try loadExamplesFromFile(arena_alloc, resolved_path, &examples);
    }

    return .{
        .arena = arena,
        .dataset_root = try arena_alloc.dupe(u8, std.fs.path.dirname(resolved.base_dir) orelse resolved.base_dir),
        .examples = try examples.toOwnedSlice(arena_alloc),
    };
}

pub fn computeStats(allocator: std.mem.Allocator, examples: []const Example) !DatasetStats {
    var stats = DatasetStats{ .num_examples = examples.len };
    if (examples.len == 0) return stats;

    var total_chars: usize = 0;
    var total_entities: usize = 0;
    var labels = std.StringHashMapUnmanaged(void){};
    defer labels.deinit(allocator);

    for (examples) |ex| {
        total_chars += ex.text.len;
        total_entities += ex.entities.len;
        for (ex.entities) |ent| try labels.put(allocator, ent.label, {});
    }

    const n = @as(f64, @floatFromInt(examples.len));
    stats.avg_text_chars = @as(f64, @floatFromInt(total_chars)) / n;
    stats.avg_entities = @as(f64, @floatFromInt(total_entities)) / n;
    stats.unique_labels = labels.count();
    return stats;
}

pub fn buildLabelVocab(allocator: std.mem.Allocator, examples: []const Example, only_labels: ?[]const []const u8) ![][]const u8 {
    var labels = std.StringHashMapUnmanaged(void){};
    defer labels.deinit(allocator);
    for (examples) |ex| {
        for (ex.entities) |ent| {
            if (only_labels) |wanted| {
                if (indexOfLabel(wanted, ent.label) == null) continue;
            }
            try labels.put(allocator, ent.label, {});
        }
    }
    var out = try allocator.alloc([]const u8, labels.count());
    errdefer allocator.free(out);
    var it = labels.iterator();
    var idx: usize = 0;
    while (it.next()) |entry| : (idx += 1) out[idx] = try allocator.dupe(u8, entry.key_ptr.*);
    std.mem.sort([]const u8, out, {}, lessThanString);
    return out;
}

pub fn validateLabelClassCapacity(
    allocator: std.mem.Allocator,
    examples: []const Example,
    num_classes: usize,
) !usize {
    if (num_classes < 2) return error.InvalidNumClasses;
    const labels = try buildLabelVocab(allocator, examples, null);
    defer {
        for (labels) |label| allocator.free(label);
        allocator.free(labels);
    }
    if (labels.len + 1 > num_classes) return error.TooManyEntityTypes;
    return labels.len;
}

pub fn computeTargetCoverageStats(examples: []const Example, entity_types: []const []const u8) TargetCoverageStats {
    var stats = std.mem.zeroInit(TargetCoverageStats, .{ .num_samples = examples.len });
    for (examples) |ex| {
        var has_target = false;
        for (ex.entities) |ent| {
            stats.total_entities += 1;
            if (indexOfLabel(entity_types, ent.label) != null) {
                stats.target_entities += 1;
                has_target = true;
            }
        }
        if (has_target) stats.samples_with_target += 1 else stats.samples_without_target += 1;
    }
    return stats;
}

pub fn evaluateDatasetReadiness(
    allocator: std.mem.Allocator,
    examples: []const Example,
    entity_types: []const []const u8,
    max_length: usize,
    max_span_width: usize,
    batch_size: usize,
    options: DatasetReadinessOptions,
) !DatasetReadinessSummary {
    const stats = try computeStats(allocator, examples);
    const coverage = computeTargetCoverageStats(examples, entity_types);
    const filtered = try filterExamplesForEntityTypes(allocator, examples, entity_types, false);
    defer freeExamples(allocator, filtered);
    const batch_shape = try buildSimpleBatchShapeSummary(allocator, filtered, entity_types, max_length, max_span_width, batch_size);
    const span_targets = try summarizeSpanTargetsForExamples(allocator, filtered, entity_types, max_length, max_span_width);
    const target_coverage_ratio = if (coverage.total_entities == 0)
        0.0
    else
        @as(f64, @floatFromInt(coverage.target_entities)) / @as(f64, @floatFromInt(coverage.total_entities));

    var reasons = std.ArrayListUnmanaged([]const u8).empty;
    errdefer reasons.deinit(allocator);

    if (stats.num_examples < options.min_examples) try reasons.append(allocator, "min_examples");
    if (coverage.total_entities < options.min_total_entities) try reasons.append(allocator, "min_total_entities");
    if (stats.unique_labels < options.min_unique_labels) try reasons.append(allocator, "min_unique_labels");
    if (coverage.target_entities < options.min_target_entities) try reasons.append(allocator, "min_target_entities");
    if (target_coverage_ratio < options.min_target_coverage_ratio) try reasons.append(allocator, "min_target_coverage_ratio");
    if (options.require_all_examples_with_target and coverage.samples_without_target != 0) try reasons.append(allocator, "require_all_examples_with_target");
    if (span_targets.positive_labels < options.min_positive_span_labels) try reasons.append(allocator, "min_positive_span_labels");
    if (span_targets.positive_rate_per_label < options.min_positive_rate_per_label) try reasons.append(allocator, "min_positive_rate_per_label");

    return .{
        .stats = stats,
        .coverage = coverage,
        .batch_shape = batch_shape,
        .span_targets = span_targets,
        .filtered_examples = filtered.len,
        .target_coverage_ratio = target_coverage_ratio,
        .passed = reasons.items.len == 0,
        .failed_reasons = try reasons.toOwnedSlice(allocator),
    };
}

pub fn summarizeSpanTargetsForExamples(
    allocator: std.mem.Allocator,
    examples: []const Example,
    entity_types: []const []const u8,
    max_length: usize,
    max_span_width: usize,
) !DatasetSpanTargetSummary {
    const max_words_per_sample = computeMaxWordsPerSample(max_length, entity_types.len);
    const max_spans_per_sample = max_words_per_sample * max_span_width;
    var valid_spans: usize = 0;
    var positive_labels: usize = 0;

    for (examples) |ex| {
        const summary = try summarizeSpanTargets(allocator, ex, entity_types, max_span_width);
        valid_spans += summary.valid_spans;
        positive_labels += summary.positive_labels;
    }

    const denom = @as(f64, @floatFromInt(@max(@as(usize, 1), examples.len * max_spans_per_sample * entity_types.len)));
    return .{
        .num_examples = examples.len,
        .max_length = max_length,
        .max_span_width = max_span_width,
        .num_entity_types = entity_types.len,
        .max_words_per_sample = max_words_per_sample,
        .max_spans_per_sample = max_spans_per_sample,
        .valid_spans = valid_spans,
        .positive_labels = positive_labels,
        .positive_rate_per_label = @as(f64, @floatFromInt(positive_labels)) / denom,
    };
}

pub fn freeDatasetReadinessSummary(allocator: std.mem.Allocator, summary: *DatasetReadinessSummary) void {
    allocator.free(summary.failed_reasons);
    summary.* = undefined;
}

pub fn filterExamplesForEntityTypes(
    allocator: std.mem.Allocator,
    examples: []const Example,
    entity_types: []const []const u8,
    drop_no_target: bool,
) ![]Example {
    var out: std.ArrayListUnmanaged(Example) = .empty;
    errdefer {
        for (out.items) |item| allocator.free(item.entities);
        out.deinit(allocator);
    }

    for (examples) |ex| {
        var entities: std.ArrayListUnmanaged(Entity) = .empty;
        errdefer entities.deinit(allocator);
        for (ex.entities) |ent| {
            if (indexOfLabel(entity_types, ent.label) != null) try entities.append(allocator, ent);
        }
        if (drop_no_target and entities.items.len == 0) {
            entities.deinit(allocator);
            continue;
        }
        try out.append(allocator, .{
            .text = ex.text,
            .entities = try entities.toOwnedSlice(allocator),
        });
    }
    return try out.toOwnedSlice(allocator);
}

pub fn summarizeSpanTargets(
    allocator: std.mem.Allocator,
    ex: Example,
    label_vocab: []const []const u8,
    max_span_width: usize,
) !SpanTargetSummary {
    const word_boundaries = try getWordBoundaries(allocator, ex.text);
    defer allocator.free(word_boundaries);
    const max_words = word_boundaries.len;
    const num_spans = max_words * max_span_width;

    var valid_spans: usize = 0;
    var positive_labels: usize = 0;
    for (0..max_words) |start_word| {
        for (0..max_span_width) |width_idx| {
            const end_word = start_word + width_idx;
            if (end_word >= max_words) continue;
            valid_spans += 1;
            const span_start = word_boundaries[start_word][0];
            const span_end = word_boundaries[end_word][1];
            for (ex.entities) |ent| {
                if (ent.start == span_start and ent.end == span_end) {
                    if (indexOfLabel(label_vocab, ent.label) != null) positive_labels += 1;
                }
            }
        }
    }

    return .{
        .max_words = max_words,
        .max_span_width = max_span_width,
        .num_spans = num_spans,
        .valid_spans = valid_spans,
        .positive_labels = positive_labels,
    };
}

pub fn buildSimpleBatchShapeSummary(
    allocator: std.mem.Allocator,
    examples: []const Example,
    entity_types: []const []const u8,
    max_length: usize,
    max_span_width: usize,
    batch_size: usize,
) !BatchShapeSummary {
    const effective_batch = @min(batch_size, examples.len);
    const max_words_per_sample = computeMaxWordsPerSample(max_length, entity_types.len);
    const max_spans = max_words_per_sample * max_span_width;
    var valid_spans: usize = 0;
    var positive_labels: usize = 0;

    for (examples[0..effective_batch]) |ex| {
        const summary = try summarizeSpanTargets(allocator, ex, entity_types, max_span_width);
        valid_spans += summary.valid_spans;
        positive_labels += summary.positive_labels;
    }

    const denom = @as(f64, @floatFromInt(@max(@as(usize, 1), effective_batch * max_spans * entity_types.len)));
    return .{
        .batch_size = effective_batch,
        .max_length = max_length,
        .num_entity_types = entity_types.len,
        .max_words_per_sample = max_words_per_sample,
        .max_spans = max_spans,
        .valid_spans = valid_spans,
        .positive_labels = positive_labels,
        .positive_rate_per_label = @as(f64, @floatFromInt(positive_labels)) / denom,
    };
}

pub fn buildSimpleBatch(
    allocator: std.mem.Allocator,
    tokenizer: *const Tokenizer,
    examples: []const Example,
    entity_types: []const []const u8,
    max_length: usize,
    max_span_width: usize,
    batch_size: usize,
) !EncodedBatch {
    const effective_batch = @min(batch_size, examples.len);
    var workspace = try ReusableBatch.init(allocator, effective_batch, max_length, max_span_width, entity_types.len);
    errdefer workspace.deinit();
    var batch = try buildSimpleBatchInto(&workspace, tokenizer, examples, entity_types, max_span_width);
    batch.owns_memory = true;
    workspace = undefined;
    return batch;
}

pub fn buildSimpleBatchInto(
    workspace: *ReusableBatch,
    tokenizer: *const Tokenizer,
    examples: []const Example,
    entity_types: []const []const u8,
    max_span_width: usize,
) !EncodedBatch {
    const effective_batch = @min(workspace.batch_size, examples.len);
    if (entity_types.len != workspace.num_entity_types) return error.EntityTypeCountMismatch;
    if (workspace.max_spans != workspace.max_words_per_sample * max_span_width) return error.BatchShapeMismatch;

    const input_ids = workspace.input_ids[0 .. effective_batch * workspace.max_length];
    const attention_mask = workspace.attention_mask[0 .. effective_batch * workspace.max_length];
    const words_mask = workspace.words_mask[0 .. effective_batch * workspace.max_length];
    const first_token_positions = workspace.first_token_positions[0 .. effective_batch * workspace.max_words_per_sample];
    const word_lengths = workspace.word_lengths[0 .. effective_batch * workspace.max_words_per_sample];
    const word_has_digit = workspace.word_has_digit[0 .. effective_batch * workspace.max_words_per_sample];
    const word_is_title = workspace.word_is_title[0 .. effective_batch * workspace.max_words_per_sample];
    const word_is_all_caps = workspace.word_is_all_caps[0 .. effective_batch * workspace.max_words_per_sample];
    const span_indices = workspace.span_indices[0 .. effective_batch * workspace.max_spans * 2];
    const span_mask = workspace.span_mask[0 .. effective_batch * workspace.max_spans];
    const span_labels = workspace.span_labels[0 .. effective_batch * workspace.max_spans * workspace.num_entity_types];
    const e_token_positions = workspace.e_token_positions[0 .. effective_batch * workspace.num_entity_types];
    const e_token_end_positions = workspace.e_token_end_positions[0 .. effective_batch * workspace.num_entity_types];
    const entity_type_kind = workspace.entity_type_kind[0 .. effective_batch * workspace.num_entity_types];

    @memset(input_ids, 0);
    @memset(attention_mask, 0);
    @memset(words_mask, 0);
    @memset(first_token_positions, 0);
    @memset(word_lengths, 0);
    @memset(word_has_digit, 0);
    @memset(word_is_title, 0);
    @memset(word_is_all_caps, 0);
    @memset(span_indices, 0);
    @memset(span_mask, 0);
    @memset(span_labels, 0);
    @memset(e_token_positions, -1);
    @memset(e_token_end_positions, -1);
    @memset(entity_type_kind, 0);

    for (examples[0..effective_batch], 0..) |ex, b| {
        const input_offset = b * workspace.max_length;
        const ftp_offset = b * workspace.max_words_per_sample;
        const e_offset = b * workspace.num_entity_types;
        const encode_result = tokenizer.encodeInto(
            workspace.allocator,
            ex.text,
            entity_types,
            input_ids[input_offset .. input_offset + workspace.max_length],
            attention_mask[input_offset .. input_offset + workspace.max_length],
            words_mask[input_offset .. input_offset + workspace.max_length],
            first_token_positions[ftp_offset .. ftp_offset + workspace.max_words_per_sample],
            e_token_positions[e_offset .. e_offset + workspace.num_entity_types],
            e_token_end_positions[e_offset .. e_offset + workspace.num_entity_types],
        );
        fillWordSurfaceFeatures(
            ex.text,
            workspace.max_words_per_sample,
            word_lengths[ftp_offset .. ftp_offset + workspace.max_words_per_sample],
            word_has_digit[ftp_offset .. ftp_offset + workspace.max_words_per_sample],
            word_is_title[ftp_offset .. ftp_offset + workspace.max_words_per_sample],
            word_is_all_caps[ftp_offset .. ftp_offset + workspace.max_words_per_sample],
        );
        for (0..workspace.num_entity_types) |j| entity_type_kind[e_offset + j] = classifyEntityType(entity_types[j]);
        try fillSpanGrid(
            workspace.allocator,
            ex,
            entity_types,
            encode_result.num_words,
            workspace.max_words_per_sample,
            max_span_width,
            span_indices[b * workspace.max_spans * 2 .. (b + 1) * workspace.max_spans * 2],
            span_mask[b * workspace.max_spans .. (b + 1) * workspace.max_spans],
            span_labels[b * workspace.max_spans * workspace.num_entity_types .. (b + 1) * workspace.max_spans * workspace.num_entity_types],
        );
    }

    return .{
        .allocator = workspace.allocator,
        .owns_memory = false,
        .input_ids = input_ids,
        .attention_mask = attention_mask,
        .words_mask = words_mask,
        .first_token_positions = first_token_positions,
        .word_lengths = word_lengths,
        .word_has_digit = word_has_digit,
        .word_is_title = word_is_title,
        .word_is_all_caps = word_is_all_caps,
        .span_indices = span_indices,
        .span_mask = span_mask,
        .span_labels = span_labels,
        .e_token_positions = e_token_positions,
        .e_token_end_positions = e_token_end_positions,
        .entity_type_kind = entity_type_kind,
        .batch_size = effective_batch,
        .max_length = workspace.max_length,
        .max_words_per_sample = workspace.max_words_per_sample,
        .max_spans = workspace.max_spans,
        .num_entity_types = workspace.num_entity_types,
    };
}

pub fn freeExamples(allocator: std.mem.Allocator, examples: []Example) void {
    for (examples) |ex| allocator.free(ex.entities);
    allocator.free(examples);
}

const ResolvedFiles = struct {
    arena: std.heap.ArenaAllocator,
    base_dir: []const u8,
    paths: [][]const u8,

    fn deinit(self: *ResolvedFiles) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

fn resolveJsonlFiles(allocator: std.mem.Allocator, path: []const u8, split: ?[]const u8) !ResolvedFiles {
    var arena = std.heap.ArenaAllocator.init(allocator);
    errdefer arena.deinit();
    const arena_alloc = arena.allocator();

    if (std.mem.trim(u8, path, " \t\r\n").len == 0) return error.EmptyPath;
    const stat = try compat.cwd().statFile(compat.io(), path, .{});
    if (stat.kind == .file) {
        const one = try arena_alloc.alloc([]const u8, 1);
        one[0] = try arena_alloc.dupe(u8, path);
        return .{
            .arena = arena,
            .base_dir = try arena_alloc.dupe(u8, std.fs.path.dirname(path) orelse "."),
            .paths = one,
        };
    }
    if (stat.kind != .directory) return error.UnsupportedPathType;

    var dir = try compat.cwd().openDir(compat.io(), path, .{ .iterate = true });
    defer dir.close(compat.io());
    var iter = dir.iterate();
    var paths = std.ArrayListUnmanaged([]const u8).empty;
    defer paths.deinit(arena_alloc);
    while (try iter.next(compat.io())) |entry| {
        if (entry.kind != .file) continue;
        if (!std.mem.endsWith(u8, entry.name, ".jsonl")) continue;
        if (split) |want_split| {
            const prefix = try std.fmt.allocPrint(arena_alloc, "{s}-", .{want_split});
            if (!std.mem.startsWith(u8, entry.name, prefix)) continue;
        }
        try paths.append(arena_alloc, try std.fs.path.join(arena_alloc, &.{ path, entry.name }));
    }
    if (paths.items.len == 0) return error.NoJsonlFilesForSplit;
    std.mem.sort([]const u8, paths.items, {}, lessThanString);
    return .{
        .arena = arena,
        .base_dir = try arena_alloc.dupe(u8, path),
        .paths = try paths.toOwnedSlice(arena_alloc),
    };
}

fn loadExamplesFromFile(allocator: std.mem.Allocator, path: []const u8, out: *std.ArrayListUnmanaged(Example)) !void {
    const data = try compat.cwd().readFileAlloc(compat.io(), path, allocator, .limited(64 * 1024 * 1024));
    var lines = std.mem.tokenizeScalar(u8, data, '\n');
    while (lines.next()) |raw_line| {
        const line = std.mem.trim(u8, raw_line, " \t\r");
        if (line.len == 0) continue;
        const parsed = try std.json.parseFromSliceLeaky(Example, allocator, line, .{ .ignore_unknown_fields = true });
        if (std.mem.trim(u8, parsed.text, " \t\r\n").len == 0) return error.MissingText;
        try out.append(allocator, parsed);
    }
}

fn resolveTokenizerJsonPath(allocator: std.mem.Allocator, model_input: []const u8) ![]u8 {
    const stat = compat.cwd().statFile(compat.io(), model_input, .{}) catch return error.FileNotFound;
    if (stat.kind == .directory) return try std.fs.path.join(allocator, &.{ model_input, "tokenizer.json" });
    return error.InvalidArguments;
}

fn classifyEntityType(label: []const u8) i32 {
    if (std.mem.eql(u8, label, "person")) return 1;
    if (std.mem.eql(u8, label, "organization")) return 2;
    if (std.mem.eql(u8, label, "location")) return 3;
    return 0;
}

fn computeMaxWordsPerSample(max_length: usize, num_entity_types: usize) usize {
    const max_words_per_sample_base: isize = @intCast(max_length);
    const reserved = 3 + @as(isize, @intCast(num_entity_types * 3));
    return @as(usize, @intCast(@max(10, max_words_per_sample_base - reserved)));
}

fn fillWordSurfaceFeatures(
    text: []const u8,
    max_words: usize,
    word_lengths: []f32,
    word_has_digit: []f32,
    word_is_title: []f32,
    word_is_all_caps: []f32,
) void {
    var words = std.mem.tokenizeAny(u8, text, " \t\r\n");
    var idx: usize = 0;
    while (words.next()) |word| {
        if (idx >= max_words) break;
        word_lengths[idx] = @as(f32, @floatFromInt(@min(word.len, 32))) / 32.0;
        word_has_digit[idx] = if (containsDigit(word)) 1.0 else 0.0;
        word_is_title[idx] = if (isTitleCaseWord(word)) 1.0 else 0.0;
        word_is_all_caps[idx] = if (isAllCapsWord(word)) 1.0 else 0.0;
        idx += 1;
    }
}

fn containsDigit(word: []const u8) bool {
    for (word) |ch| if (std.ascii.isDigit(ch)) return true;
    return false;
}

fn isTitleCaseWord(word: []const u8) bool {
    if (word.len == 0) return false;
    if (!std.ascii.isAlphabetic(word[0]) or !std.ascii.isUpper(word[0])) return false;
    for (word[1..]) |ch| if (std.ascii.isAlphabetic(ch) and !std.ascii.isLower(ch)) return false;
    return true;
}

fn isAllCapsWord(word: []const u8) bool {
    var seen_alpha = false;
    for (word) |ch| {
        if (!std.ascii.isAlphabetic(ch)) continue;
        seen_alpha = true;
        if (!std.ascii.isUpper(ch)) return false;
    }
    return seen_alpha;
}

fn fillSpanGrid(
    allocator: std.mem.Allocator,
    ex: Example,
    entity_types: []const []const u8,
    num_words: usize,
    max_words: usize,
    max_span_width: usize,
    span_indices: []i32,
    span_mask: []f32,
    span_labels: []f32,
) !void {
    const num_entity_types = entity_types.len;
    const max_spans = max_words * max_span_width;
    const char_to_word = try buildCharToWordMap(allocator, ex.text);
    defer allocator.free(char_to_word);

    for (0..max_words) |start_word| {
        for (0..max_span_width) |w| {
            const span_idx = start_word * max_span_width + w;
            if (span_idx >= max_spans) continue;
            const end_word = start_word + w;
            if (start_word < num_words and end_word < num_words) {
                span_indices[span_idx * 2] = @intCast(start_word);
                span_indices[span_idx * 2 + 1] = @intCast(end_word);
                span_mask[span_idx] = 1.0;
                for (ex.entities) |ent| {
                    const span = getEntityWordSpan(ent, char_to_word);
                    if (span[0] == start_word and span[1] == end_word) {
                        if (indexOfLabel(entity_types, ent.label)) |label_idx| {
                            span_labels[span_idx * num_entity_types + label_idx] = 1.0;
                        }
                    }
                }
            }
        }
    }
}

fn buildCharToWordMap(allocator: std.mem.Allocator, text: []const u8) ![]i32 {
    const map = try allocator.alloc(i32, text.len);
    @memset(map, -1);
    var words = std.mem.tokenizeAny(u8, text, " \t\r\n");
    var search_start: usize = 0;
    var word_idx: usize = 0;
    while (words.next()) |word| : (word_idx += 1) {
        const idx = std.mem.indexOfPos(u8, text, search_start, word) orelse continue;
        const start = idx;
        const end = idx + word.len;
        for (start..@min(end, map.len)) |pos| map[pos] = @intCast(word_idx);
        search_start = end;
    }
    return map;
}

fn getEntityWordSpan(ent: Entity, char_to_word: []const i32) [2]usize {
    var start_word: i32 = -1;
    var end_word: i32 = -1;
    if (ent.start < char_to_word.len) start_word = char_to_word[ent.start];
    const end_char = if (ent.end > ent.start) ent.end - 1 else ent.start;
    if (end_char < char_to_word.len) end_word = char_to_word[end_char];
    return .{
        if (start_word >= 0) @intCast(start_word) else std.math.maxInt(usize),
        if (end_word >= 0) @intCast(end_word) else std.math.maxInt(usize),
    };
}

fn getWordBoundaries(allocator: std.mem.Allocator, text: []const u8) ![][2]usize {
    var out: std.ArrayListUnmanaged([2]usize) = .empty;
    defer out.deinit(allocator);
    var search_start: usize = 0;
    var words = std.mem.tokenizeAny(u8, text, " \t\r\n");
    while (words.next()) |word| {
        const idx = std.mem.indexOfPos(u8, text, search_start, word) orelse continue;
        const start = idx;
        const end = idx + word.len;
        try out.append(allocator, .{ start, end });
        search_start = end;
    }
    return try out.toOwnedSlice(allocator);
}

fn indexOfLabel(label_vocab: []const []const u8, label: []const u8) ?usize {
    for (label_vocab, 0..) |item, idx| if (std.mem.eql(u8, item, label)) return idx;
    return null;
}

fn lessThanString(_: void, a: []const u8, b: []const u8) bool {
    return std.mem.lessThan(u8, a, b);
}

test "load gliner2 examples and compute stats" {
    const allocator = std.testing.allocator;
    const root = try std.fmt.allocPrint(allocator, "/tmp/termite_gliner2_data_stats_test_{d}", .{std.posix.system.getpid()});
    defer allocator.free(root);
    compat.cwd().deleteTree(compat.io(), root) catch {};
    try compat.cwd().createDirPath(compat.io(), root);
    defer compat.cwd().deleteTree(compat.io(), root) catch {};

    const train_jsonl =
        \\{"text":"hello world","entities":[{"text":"world","label":"location","start":6,"end":11}]}
        \\{"text":"acme inc","entities":[{"text":"acme","label":"organization","start":0,"end":4}]}
        \\
    ;
    const path = try std.fs.path.join(allocator, &.{ root, "train-00000.jsonl" });
    defer allocator.free(path);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = path, .data = train_jsonl });

    var loaded = try loadExamples(allocator, root, "train");
    defer loaded.deinit();
    try std.testing.expectEqual(@as(usize, 2), loaded.examples.len);
    const stats = try computeStats(allocator, loaded.examples);
    try std.testing.expectEqual(@as(usize, 2), stats.unique_labels);
}

test "summarize gliner2 span targets" {
    const allocator = std.testing.allocator;
    var entities = [_]Entity{
        .{ .text = "world", .label = "location", .start = 6, .end = 11 },
        .{ .text = "acme", .label = "organization", .start = 17, .end = 21 },
    };
    const ex = Example{
        .text = "hello world from acme",
        .entities = entities[0..],
    };
    const label_vocab = [_][]const u8{ "location", "organization" };
    const summary = try summarizeSpanTargets(allocator, ex, label_vocab[0..], 3);
    try std.testing.expectEqual(@as(usize, 4), summary.max_words);
    try std.testing.expect(summary.positive_labels >= 2);
}

test "build simple gliner2 batch" {
    const allocator = std.testing.allocator;
    var tokenizer = try Tokenizer.initDefault(allocator);
    defer tokenizer.deinit(allocator);
    const entity_types = [_][]const u8{ "person", "organization", "location" };
    var entities = [_]Entity{
        .{ .text = "john", .label = "person", .start = 0, .end = 4 },
        .{ .text = "acme", .label = "organization", .start = 14, .end = 18 },
        .{ .text = "paris", .label = "location", .start = 22, .end = 27 },
    };
    const examples = [_]Example{
        .{
            .text = "john works at acme in paris",
            .entities = entities[0..],
        },
    };
    var batch = try buildSimpleBatch(allocator, &tokenizer, examples[0..], entity_types[0..], 64, 4, 1);
    defer batch.deinit();
    try std.testing.expectEqual(@as(usize, 1), batch.batch_size);
    try std.testing.expect(batch.input_ids.len == 64);
    try std.testing.expect(batch.span_labels.len == batch.max_spans * entity_types.len);
}

test "decode gliner2 span predictions from score grid" {
    const allocator = std.testing.allocator;
    var tokenizer = try Tokenizer.initDefault(allocator);
    defer tokenizer.deinit(allocator);
    const entity_types = [_][]const u8{ "person", "organization", "location" };
    var entities = [_]Entity{
        .{ .text = "john", .label = "person", .start = 0, .end = 4 },
        .{ .text = "acme", .label = "organization", .start = 14, .end = 18 },
        .{ .text = "paris", .label = "location", .start = 22, .end = 27 },
    };
    const examples = [_]Example{
        .{
            .text = "john works at acme in paris",
            .entities = entities[0..],
        },
    };
    var batch = try buildSimpleBatch(allocator, &tokenizer, examples[0..], entity_types[0..], 64, 4, 1);
    defer batch.deinit();

    const predictions = try decodeSpanPredictionsAlloc(allocator, &batch, entity_types[0..], batch.span_labels, 0.5);
    defer allocator.free(predictions);
    try std.testing.expectEqual(@as(usize, 3), predictions.len);
    try std.testing.expectEqual(@as(usize, 0), predictions[0].sample_index);
    try std.testing.expectEqual(@as(usize, 0), predictions[0].word_start);
    try std.testing.expectEqual(@as(usize, 0), predictions[0].word_end);
    try std.testing.expectEqual(@as(usize, 0), predictions[0].entity_type_index);
    try std.testing.expectEqualStrings("person", predictions[0].label);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), predictions[0].score, 1e-6);

    try std.testing.expectEqual(@as(usize, 3), predictions[1].word_start);
    try std.testing.expectEqual(@as(usize, 3), predictions[1].word_end);
    try std.testing.expectEqual(@as(usize, 1), predictions[1].entity_type_index);
    try std.testing.expectEqualStrings("organization", predictions[1].label);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), predictions[1].score, 1e-6);

    try std.testing.expectEqual(@as(usize, 5), predictions[2].word_start);
    try std.testing.expectEqual(@as(usize, 5), predictions[2].word_end);
    try std.testing.expectEqual(@as(usize, 2), predictions[2].entity_type_index);
    try std.testing.expectEqualStrings("location", predictions[2].label);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), predictions[2].score, 1e-6);

    const entity_predictions = try decodeEntityPredictionsAlloc(allocator, &batch, examples[0..], entity_types[0..], batch.span_labels, 0.5);
    defer allocator.free(entity_predictions);
    try std.testing.expectEqual(@as(usize, 3), entity_predictions.len);

    try std.testing.expectEqual(@as(usize, 0), entity_predictions[0].start);
    try std.testing.expectEqual(@as(usize, 4), entity_predictions[0].end);
    try std.testing.expectEqualStrings("john", entity_predictions[0].text);
    try std.testing.expectEqualStrings("person", entity_predictions[0].label);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), entity_predictions[0].score, 1e-6);

    try std.testing.expectEqual(@as(usize, 14), entity_predictions[1].start);
    try std.testing.expectEqual(@as(usize, 18), entity_predictions[1].end);
    try std.testing.expectEqualStrings("acme", entity_predictions[1].text);
    try std.testing.expectEqualStrings("organization", entity_predictions[1].label);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), entity_predictions[1].score, 1e-6);

    try std.testing.expectEqual(@as(usize, 22), entity_predictions[2].start);
    try std.testing.expectEqual(@as(usize, 27), entity_predictions[2].end);
    try std.testing.expectEqualStrings("paris", entity_predictions[2].text);
    try std.testing.expectEqualStrings("location", entity_predictions[2].label);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), entity_predictions[2].score, 1e-6);
}

test "decode gliner2 entity predictions from token logits" {
    const allocator = std.testing.allocator;
    var tokenizer = try Tokenizer.initDefault(allocator);
    defer tokenizer.deinit(allocator);
    const entity_types = [_][]const u8{ "person", "organization", "location" };
    var entities = [_]Entity{
        .{ .text = "john", .label = "person", .start = 0, .end = 4 },
        .{ .text = "acme", .label = "organization", .start = 14, .end = 18 },
    };
    const examples = [_]Example{
        .{
            .text = "john works at acme",
            .entities = entities[0..],
        },
    };
    var batch = try buildSimpleBatch(allocator, &tokenizer, examples[0..], entity_types[0..], 64, 4, 1);
    defer batch.deinit();

    const num_classes = entity_types.len + 1;
    const token_logits = try allocator.alloc(f32, batch.batch_size * batch.max_length * num_classes);
    defer allocator.free(token_logits);
    @memset(token_logits, -8.0);
    for (0..batch.batch_size * batch.max_length) |row_idx| {
        token_logits[row_idx * num_classes] = 8.0;
    }

    const john_token: usize = @intCast(batch.first_token_positions[0]);
    const acme_token: usize = @intCast(batch.first_token_positions[3]);
    token_logits[john_token * num_classes + 0] = -8.0;
    token_logits[john_token * num_classes + 1] = 8.0;
    token_logits[acme_token * num_classes + 0] = -8.0;
    token_logits[acme_token * num_classes + 2] = 8.0;

    const span_scores = try tokenLogitsToSpanScoresAlloc(allocator, &batch, token_logits, num_classes);
    defer allocator.free(span_scores);
    const entity_predictions = try decodeEntityPredictionsAlloc(allocator, &batch, examples[0..], entity_types[0..], span_scores, 0.99);
    defer allocator.free(entity_predictions);
    try std.testing.expectEqual(@as(usize, 2), entity_predictions.len);
    try std.testing.expectEqual(@as(usize, 0), entity_predictions[0].start);
    try std.testing.expectEqual(@as(usize, 4), entity_predictions[0].end);
    try std.testing.expectEqualStrings("john", entity_predictions[0].text);
    try std.testing.expectEqualStrings("person", entity_predictions[0].label);
    try std.testing.expect(entity_predictions[0].score > 0.99);

    try std.testing.expectEqual(@as(usize, 14), entity_predictions[1].start);
    try std.testing.expectEqual(@as(usize, 18), entity_predictions[1].end);
    try std.testing.expectEqualStrings("acme", entity_predictions[1].text);
    try std.testing.expectEqualStrings("organization", entity_predictions[1].label);
    try std.testing.expect(entity_predictions[1].score > 0.99);
}
