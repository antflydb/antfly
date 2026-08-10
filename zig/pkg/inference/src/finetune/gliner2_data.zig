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
const builtin = @import("builtin");
const compat = @import("../io/compat.zig");
const upstream_unicode = @import("gliner2_unicode_tables.zig");

pub const upstream_unicode_version = upstream_unicode.unicode_version;
pub const canonical_scoring_normalization = "unicode_nfc_collapsed_whitespace_casefold/v1";
pub const canonical_scoring_normalization_profile = "Unicode 15 NFC-inert/simple-casefold subset; unsupported values fail closed";

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

pub const UpstreamTaskKind = enum {
    entities,
    json_structures,
    relations,
    classifications,
};

/// Stable internal identity for one contextual schema field. Entity labels
/// remain user-facing for inference compatibility; every other task family
/// uses a kind-tagged, length-prefixed key so shared label names and dotted
/// task/field names cannot collide in the packed training vocabulary.
pub fn upstreamTaskFieldKey(
    allocator: std.mem.Allocator,
    kind: UpstreamTaskKind,
    task_name: []const u8,
    field_name: []const u8,
) ![]const u8 {
    if (kind == .entities) {
        if (std.mem.startsWith(u8, field_name, "@gliner2:")) return error.ReservedEntityLabelPrefix;
        return allocator.dupe(u8, field_name);
    }
    const tag: u8 = switch (kind) {
        .entities => unreachable,
        .json_structures => 'j',
        .relations => 'r',
        .classifications => 'c',
    };
    return std.fmt.allocPrint(allocator, "@gliner2:{c}:{d}:{s}:{d}:{s}", .{
        tag,
        task_name.len,
        task_name,
        field_name.len,
        field_name,
    });
}

pub const UpstreamField = struct {
    name: []const u8,
    value: []const u8,
    start: ?usize = null,
    end: ?usize = null,
    target_word_start: ?usize = null,
    target_word_end: ?usize = null,
    /// Document-order instance index this field value belongs to within its
    /// grouped structure task (0-based). Used by the per-instance structure
    /// loss to assign each gold span to the matching count-conditioned
    /// projection. Single-instance tasks leave this at 0.
    instance: usize = 0,
};

pub const UpstreamLabelDescription = struct {
    label: []const u8,
    description: []const u8,
};

pub const UpstreamClassificationExample = struct {
    input: []const u8,
    output: []const u8,
};

pub const UpstreamTask = struct {
    kind: UpstreamTaskKind,
    name: []const u8,
    /// Ordered schema field names, including fields with no gold value in a
    /// particular occurrence. Empty means derive them from `fields` for
    /// legacy/synthetic callers.
    schema_fields: []const []const u8 = &.{},
    labels: []const []const u8 = &.{},
    true_labels: []const []const u8 = &.{},
    /// Upstream classification decoding uses top-1 softmax for single-label
    /// tasks and thresholded sigmoid for multi-label tasks. Keep this schema
    /// bit alongside the labels so held-out native evaluation cannot silently
    /// apply the wrong decoder.
    multi_label: bool = false,
    prompt: ?[]const u8 = null,
    label_descriptions: []const UpstreamLabelDescription = &.{},
    examples: []const UpstreamClassificationExample = &.{},
    fields: []const UpstreamField = &.{},
    count: usize = 0,
};

pub const UpstreamRecord = struct {
    text: []const u8,
    tasks: []const UpstreamTask,
    prefix_tokens: []const []const u8 = &.{},
};

pub const UpstreamTaskStats = struct {
    num_records: usize = 0,
    entity_tasks: usize = 0,
    classification_tasks: usize = 0,
    json_structure_tasks: usize = 0,
    relation_tasks: usize = 0,
    classification_label_count: usize = 0,
    classification_true_label_count: usize = 0,
    span_field_annotations: usize = 0,
    non_entity_task_annotations: usize = 0,
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

pub const LoadedTrainingRecords = struct {
    arena: std.heap.ArenaAllocator,
    dataset_root: []const u8,
    records: []UpstreamRecord,

    pub fn deinit(self: *LoadedTrainingRecords) void {
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

    const HFNormalizerContract = enum {
        none,
        pinned_fastino_unicode15,
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
    c_token_id: i32 = 0,
    r_token_id: i32 = 0,
    l_token_id: i32 = 0,
    example_token_id: i32 = 0,
    output_token_id: i32 = 0,
    description_token_id: i32 = 0,
    sep_struct_token_id: i32 = 0,
    sep_text_token_id: i32 = 2,
    use_gliner2_hf_prompt: bool = false,
    hf_unigram_scores: []f32 = &.{},
    hf_metaspace_prepend_scheme: MetaspacePrependScheme = .always,
    hf_metaspace_split: bool = true,
    hf_normalizer_contract: HFNormalizerContract = .none,

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
        try tok.vocab.put(allocator, try allocator.dupe(u8, "[P]"), 6);
        try tok.vocab.put(allocator, try allocator.dupe(u8, "[C]"), 7);
        try tok.vocab.put(allocator, try allocator.dupe(u8, "[R]"), 8);
        try tok.vocab.put(allocator, try allocator.dupe(u8, "[L]"), 9);
        try tok.vocab.put(allocator, try allocator.dupe(u8, "[SEP_STRUCT]"), 10);
        try tok.vocab.put(allocator, try allocator.dupe(u8, "[SEP_TEXT]"), 11);
        tok.p_token_id = 6;
        tok.c_token_id = 7;
        tok.r_token_id = 8;
        tok.l_token_id = 9;
        tok.sep_struct_token_id = 10;
        tok.sep_text_token_id = 11;
        const words = [_][]const u8{
            "the",   "a",         "an",     "is",      "are",  "was",     "were",      "be",        "of",           "in",       "for",     "on",
            "with",  "at",        "by",     "from",    "and",  "or",      "but",       "person",    "organization", "location", "product", "event",
            "other", "building",  "art",    "company", "city", "country", "ceo",       "president", "director",     "inc",      "corp",    "google",
            "apple", "microsoft", "amazon", "new",     "york", "san",     "francisco", "london",
        };
        for (words, 0..) |word, i| {
            try tok.vocab.put(allocator, try allocator.dupe(u8, word), @as(i32, @intCast(i + 12)));
        }
        tok.example_token_id = @intCast(tok.vocab.count());
        try tok.vocab.put(allocator, try allocator.dupe(u8, "[EXAMPLE]"), tok.example_token_id);
        tok.output_token_id = @intCast(tok.vocab.count());
        try tok.vocab.put(allocator, try allocator.dupe(u8, "[OUTPUT]"), tok.output_token_id);
        tok.description_token_id = @intCast(tok.vocab.count());
        try tok.vocab.put(allocator, try allocator.dupe(u8, "[DESCRIPTION]"), tok.description_token_id);
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
        const normalizer_contract = try parseHFNormalizerContract(root);

        var tok = Tokenizer{
            .use_gliner2_hf_prompt = true,
            .hf_unigram_scores = try allocator.alloc(f32, vocab_value.array.items.len),
            .hf_normalizer_contract = normalizer_contract,
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
            if (std.mem.eql(u8, content, "[C]")) tok.c_token_id = id;
            if (std.mem.eql(u8, content, "[R]")) tok.r_token_id = id;
            if (std.mem.eql(u8, content, "[L]")) tok.l_token_id = id;
            if (std.mem.eql(u8, content, "[EXAMPLE]")) tok.example_token_id = id;
            if (std.mem.eql(u8, content, "[OUTPUT]")) tok.output_token_id = id;
            if (std.mem.eql(u8, content, "[DESCRIPTION]")) tok.description_token_id = id;
            if (std.mem.eql(u8, content, "[SEP_STRUCT]")) tok.sep_struct_token_id = id;
            if (std.mem.eql(u8, content, "[SEP_TEXT]")) tok.sep_text_token_id = id;
        }
        if (tok.ent_id <= 0 or tok.p_token_id <= 0 or tok.c_token_id <= 0 or tok.r_token_id <= 0 or tok.l_token_id <= 0 or tok.sep_struct_token_id <= 0 or tok.sep_text_token_id <= 0) return error.InvalidTokenizerJson;
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
    ) !EncodeIntoResult {
        @memset(input_ids, 0);
        @memset(attention_mask, 0);
        @memset(words_mask, 0);
        @memset(first_token_positions, 0);
        @memset(e_token_positions, -1);
        @memset(e_token_end_positions, -1);

        const max_length = input_ids.len;
        var pos: usize = 0;
        if (self.use_gliner2_hf_prompt) {
            pos = try self.encodeHFFragmentIntoAllocating(allocator, "(", input_ids, attention_mask, pos, max_length);
            if (pos < max_length) {
                input_ids[pos] = self.p_token_id;
                attention_mask[pos] = 1;
                pos += 1;
            }
            pos = try self.encodeHFFragmentIntoAllocating(allocator, "entities", input_ids, attention_mask, pos, max_length);
            pos = try self.encodeHFFragmentIntoAllocating(allocator, "(", input_ids, attention_mask, pos, max_length);

            for (entity_types, 0..) |entity_type, i| {
                if (pos >= max_length) break;
                input_ids[pos] = self.ent_id;
                attention_mask[pos] = 1;
                pos += 1;
                if (pos >= max_length) break;
                const label_start = pos - 1;
                const new_pos = try self.encodeHFFragmentIntoAllocating(allocator, entity_type, input_ids, attention_mask, pos, max_length);
                if (new_pos >= max_length and i + 1 < entity_types.len) {
                    break;
                }
                pos = new_pos;
                e_token_positions[i] = @intCast(label_start);
                e_token_end_positions[i] = @intCast(pos);
            }
            pos = try self.encodeHFFragmentIntoAllocating(allocator, ")", input_ids, attention_mask, pos, max_length);
            pos = try self.encodeHFFragmentIntoAllocating(allocator, ")", input_ids, attention_mask, pos, max_length);

            if (pos < max_length) {
                input_ids[pos] = self.sep_text_token_id;
                attention_mask[pos] = 1;
                pos += 1;
            }

            var num_words_hf: usize = 0;
            var words_hf = std.mem.tokenizeAny(u8, text, " \t\r\n");
            while (words_hf.next()) |word| {
                if (num_words_hf >= first_token_positions.len) break;
                if (pos >= max_length) break;
                first_token_positions[num_words_hf] = @intCast(pos);
                const lower_word = try std.ascii.allocLowerString(allocator, word);
                defer allocator.free(lower_word);
                const next_pos = try self.encodeHFFragmentIntoAllocating(allocator, lower_word, input_ids, attention_mask, pos, max_length);
                if (next_pos == pos) break;
                for (pos..next_pos) |token_pos| words_mask[token_pos] = @intCast(num_words_hf + 1);
                pos = next_pos;
                num_words_hf += 1;
            }
            const trimmed_text = std.mem.trim(u8, text, " \t\r\n");
            const needs_period = trimmed_text.len == 0 or !(trimmed_text[trimmed_text.len - 1] == '.' or trimmed_text[trimmed_text.len - 1] == '!' or trimmed_text[trimmed_text.len - 1] == '?');
            if (needs_period and num_words_hf < first_token_positions.len and pos < max_length) {
                first_token_positions[num_words_hf] = @intCast(pos);
                const next_pos = try self.encodeHFFragmentIntoAllocating(allocator, ".", input_ids, attention_mask, pos, max_length);
                if (next_pos != pos) {
                    for (pos..next_pos) |token_pos| words_mask[token_pos] = @intCast(num_words_hf + 1);
                    pos = next_pos;
                    num_words_hf += 1;
                }
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

    fn parseHFNormalizerContract(root: std.json.ObjectMap) !HFNormalizerContract {
        const normalizer = root.get("normalizer") orelse return error.UnsupportedTokenizerNormalizer;
        if (normalizer != .object or
            normalizer.object.count() != 2 or
            !jsonStringEquals(normalizer.object.get("type"), "Sequence"))
        {
            return error.UnsupportedTokenizerNormalizer;
        }
        const sequence = normalizer.object.get("normalizers") orelse return error.UnsupportedTokenizerNormalizer;
        if (sequence != .array or sequence.array.items.len != 3) return error.UnsupportedTokenizerNormalizer;

        const strip = sequence.array.items[0];
        if (strip != .object or
            strip.object.count() != 3 or
            !jsonStringEquals(strip.object.get("type"), "Strip") or
            !jsonBoolEquals(strip.object.get("strip_left"), true) or
            !jsonBoolEquals(strip.object.get("strip_right"), true))
        {
            return error.UnsupportedTokenizerNormalizer;
        }

        const precompiled = sequence.array.items[1];
        if (precompiled != .object or
            precompiled.object.count() != 2 or
            !jsonStringEquals(precompiled.object.get("type"), "Precompiled"))
        {
            return error.UnsupportedTokenizerNormalizer;
        }
        const charsmap = precompiled.object.get("precompiled_charsmap") orelse return error.UnsupportedTokenizerNormalizer;
        if (charsmap != .string) return error.UnsupportedTokenizerNormalizer;
        var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(charsmap.string, &digest, .{});
        if (!std.mem.eql(u8, &digest, &upstream_unicode.pinned_normalizer_charsmap_sha256)) {
            return error.UnsupportedTokenizerNormalizer;
        }

        const replace = sequence.array.items[2];
        if (replace != .object or
            replace.object.count() != 3 or
            !jsonStringEquals(replace.object.get("type"), "Replace") or
            !jsonStringEquals(replace.object.get("content"), " "))
        {
            return error.UnsupportedTokenizerNormalizer;
        }
        const pattern = replace.object.get("pattern") orelse return error.UnsupportedTokenizerNormalizer;
        if (pattern != .object or
            pattern.object.count() != 1 or
            !jsonStringEquals(pattern.object.get("Regex"), " {2,}"))
        {
            return error.UnsupportedTokenizerNormalizer;
        }
        return .pinned_fastino_unicode15;
    }

    fn jsonStringEquals(value: ?std.json.Value, expected: []const u8) bool {
        const actual = value orelse return false;
        return actual == .string and std.mem.eql(u8, actual.string, expected);
    }

    fn jsonBoolEquals(value: ?std.json.Value, expected: bool) bool {
        const actual = value orelse return false;
        return actual == .bool and actual.bool == expected;
    }

    /// Fail closed unless a fragment is provably unchanged by the exact
    /// tokenizer.json normalizer accepted at initialization.
    pub fn validateNormalizerInvariant(self: *const Tokenizer, text: []const u8) !void {
        switch (self.hf_normalizer_contract) {
            .none => return,
            .pinned_fastino_unicode15 => try validatePinnedFastinoNormalizerInvariant(text),
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
        return try self.encodeHFFragmentInto(allocator, text, input_ids, attention_mask, pos, limit, false);
    }

    fn encodeHFFragmentExactIntoAllocating(
        self: *const Tokenizer,
        allocator: std.mem.Allocator,
        text: []const u8,
        input_ids: []i32,
        attention_mask: []i32,
        pos: usize,
        limit: usize,
    ) !usize {
        return try self.encodeHFFragmentInto(allocator, text, input_ids, attention_mask, pos, limit, true);
    }

    fn encodeHFFragmentInto(
        self: *const Tokenizer,
        allocator: std.mem.Allocator,
        text: []const u8,
        input_ids: []i32,
        attention_mask: []i32,
        start_pos: usize,
        limit: usize,
        fail_on_truncation: bool,
    ) !usize {
        try self.validateNormalizerInvariant(text);
        var pos = start_pos;
        const pieces = try self.metaspacePreTokenizeWithScheme(allocator, text, .always);
        defer {
            for (pieces) |piece| allocator.free(piece);
            allocator.free(pieces);
        }
        for (pieces) |piece| {
            const ids = try self.unigramEncodePieceAlloc(allocator, piece);
            defer allocator.free(ids);
            for (ids) |id| {
                if (pos >= limit) {
                    if (fail_on_truncation) return error.SequenceTooLong;
                    return pos;
                }
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

/// Validate the deliberately conservative, concatenation-safe subset of the
/// exact Fastino tokenizer normalizer pinned by `initGLiNER2HF`.
pub fn validatePinnedFastinoNormalizerInvariant(text: []const u8) !void {
    if (text.len == 0) return;
    if (text[0] == ' ' or text[text.len - 1] == ' ') return error.UnsupportedTokenizerNormalization;

    var previous_was_space = false;
    var view = std.unicode.Utf8View.init(text) catch return error.InvalidUtf8;
    var iter = view.iterator();
    while (iter.nextCodepoint()) |cp| {
        if (cp == ' ') {
            if (previous_was_space) return error.UnsupportedTokenizerNormalization;
            previous_was_space = true;
            continue;
        }
        if (!upstream_unicode.isPinnedNormalizerInert(cp)) return error.UnsupportedTokenizerNormalization;
        previous_was_space = false;
    }
}

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
    text_word_counts: []i32 = &.{},
    schema_counts: []i32 = &.{},
    task_type_ids: []i32 = &.{},
    schema_special_positions: []i32 = &.{},
    schema_special_counts: []i32 = &.{},
    batch_size: usize,
    max_length: usize,
    max_words_per_sample: usize,
    max_spans: usize,
    num_entity_types: usize,
    max_schemas: usize = 0,
    max_schema_specials: usize = 0,
    uses_upstream_word_splitting: bool = false,

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
        if (self.text_word_counts.len > 0) self.allocator.free(self.text_word_counts);
        if (self.schema_counts.len > 0) self.allocator.free(self.schema_counts);
        if (self.task_type_ids.len > 0) self.allocator.free(self.task_type_ids);
        if (self.schema_special_positions.len > 0) self.allocator.free(self.schema_special_positions);
        if (self.schema_special_counts.len > 0) self.allocator.free(self.schema_special_counts);
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
        const word_boundaries = if (batch.uses_upstream_word_splitting)
            try getUpstreamWordBoundaries(allocator, examples[sample_idx].text)
        else
            try getWordBoundaries(allocator, examples[sample_idx].text);
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

pub fn loadTrainingRecords(allocator: std.mem.Allocator, path: []const u8, split: ?[]const u8) !LoadedTrainingRecords {
    var arena = std.heap.ArenaAllocator.init(allocator);
    errdefer arena.deinit();
    const arena_alloc = arena.allocator();

    var resolved = try resolveJsonlFiles(arena_alloc, path, split);
    defer resolved.deinit();

    var records: std.ArrayListUnmanaged(UpstreamRecord) = .empty;
    defer records.deinit(arena_alloc);
    for (resolved.paths) |resolved_path| {
        try loadTrainingRecordsFromFile(arena_alloc, resolved_path, &records);
    }

    return .{
        .arena = arena,
        .dataset_root = try arena_alloc.dupe(u8, std.fs.path.dirname(resolved.base_dir) orelse resolved.base_dir),
        .records = try records.toOwnedSlice(arena_alloc),
    };
}

pub fn computeUpstreamTaskStats(records: []const UpstreamRecord) UpstreamTaskStats {
    var stats = UpstreamTaskStats{ .num_records = records.len };
    for (records) |record| {
        for (record.tasks) |task| {
            switch (task.kind) {
                .entities => stats.entity_tasks += 1,
                .classifications => {
                    stats.classification_tasks += 1;
                    stats.classification_label_count += task.labels.len;
                    stats.classification_true_label_count += task.true_labels.len;
                    stats.non_entity_task_annotations += 1;
                },
                .json_structures => {
                    stats.json_structure_tasks += 1;
                    stats.non_entity_task_annotations += 1;
                },
                .relations => {
                    stats.relation_tasks += 1;
                    stats.non_entity_task_annotations += 1;
                },
            }
            stats.span_field_annotations += task.fields.len;
        }
    }
    return stats;
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

pub fn buildUpstreamTaskLabelVocab(allocator: std.mem.Allocator, records: []const UpstreamRecord, extra_labels: ?[]const []const u8) ![][]const u8 {
    var labels = std.StringHashMapUnmanaged(void){};
    defer labels.deinit(allocator);
    var ordered = std.ArrayListUnmanaged([]const u8).empty;
    defer ordered.deinit(allocator);
    var owned_temp = std.ArrayListUnmanaged([]const u8).empty;
    defer {
        for (owned_temp.items) |label| allocator.free(label);
        owned_temp.deinit(allocator);
    }

    if (extra_labels) |extras| {
        for (extras) |label| {
            const trimmed = std.mem.trim(u8, label, " \t\r\n");
            if (std.mem.startsWith(u8, trimmed, "@gliner2:")) return error.ReservedEntityLabelPrefix;
            if (trimmed.len > 0 and !labels.contains(trimmed)) {
                try labels.put(allocator, trimmed, {});
                try ordered.append(allocator, trimmed);
            }
        }
    }

    for (records) |record| {
        for (record.tasks) |task| {
            switch (task.kind) {
                .classifications => {
                    for (task.labels) |label| {
                        const trimmed = std.mem.trim(u8, label, " \t\r\n");
                        if (trimmed.len == 0) continue;
                        // Classification labels are contextual to their
                        // schema upstream. Qualify them so two tasks that both
                        // use e.g. "yes"/"no" retain distinct marker
                        // embeddings instead of overwriting one global slot.
                        const full = try upstreamTaskFieldKey(allocator, task.kind, task.name, trimmed);
                        if (labels.contains(full)) {
                            allocator.free(full);
                            continue;
                        }
                        try owned_temp.append(allocator, full);
                        try labels.put(allocator, full, {});
                        try ordered.append(allocator, full);
                    }
                },
                .entities => {
                    if (task.schema_fields.len > 0) {
                        for (task.schema_fields) |schema_field| {
                            const label = std.mem.trim(u8, schema_field, " \t\r\n");
                            if (label.len == 0) continue;
                            const full = try upstreamTaskFieldKey(allocator, task.kind, task.name, label);
                            if (labels.contains(full)) {
                                allocator.free(full);
                                continue;
                            }
                            try owned_temp.append(allocator, full);
                            try labels.put(allocator, full, {});
                            try ordered.append(allocator, full);
                        }
                    } else {
                        for (task.fields) |field| {
                            const label = std.mem.trim(u8, field.name, " \t\r\n");
                            if (label.len == 0) continue;
                            const full = try upstreamTaskFieldKey(allocator, task.kind, task.name, label);
                            if (labels.contains(full)) {
                                allocator.free(full);
                                continue;
                            }
                            try owned_temp.append(allocator, full);
                            try labels.put(allocator, full, {});
                            try ordered.append(allocator, full);
                        }
                    }
                },
                .json_structures, .relations => {
                    if (task.schema_fields.len > 0) {
                        for (task.schema_fields) |schema_field| {
                            const field_name = std.mem.trim(u8, schema_field, " \t\r\n");
                            if (field_name.len == 0) continue;
                            const full = try upstreamTaskFieldKey(allocator, task.kind, task.name, field_name);
                            if (labels.contains(full)) {
                                allocator.free(full);
                                continue;
                            }
                            try owned_temp.append(allocator, full);
                            try labels.put(allocator, full, {});
                            try ordered.append(allocator, full);
                        }
                        continue;
                    }
                    for (task.fields) |field| {
                        const field_name = std.mem.trim(u8, field.name, " \t\r\n");
                        if (field_name.len == 0) continue;
                        const full = try upstreamTaskFieldKey(allocator, task.kind, task.name, field_name);
                        if (labels.contains(full)) {
                            allocator.free(full);
                            continue;
                        }
                        try owned_temp.append(allocator, full);
                        try labels.put(allocator, full, {});
                        try ordered.append(allocator, full);
                    }
                },
            }
        }
    }

    var out = try allocator.alloc([]const u8, ordered.items.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |label| allocator.free(label);
        allocator.free(out);
    }
    for (ordered.items, 0..) |label, idx| {
        out[idx] = try allocator.dupe(u8, label);
        initialized += 1;
    }
    return out;
}

/// Entity labels are the only dataset-wide names needed by the exported
/// inference manifest. Classification and structured fields are contextual
/// inputs upstream, so keeping their union here would make training graph
/// width grow with dataset heterogeneity rather than with one sample.
pub fn buildUpstreamEntityLabelVocab(allocator: std.mem.Allocator, records: []const UpstreamRecord, extra_labels: ?[]const []const u8) ![][]const u8 {
    var seen = std.StringHashMapUnmanaged(void){};
    defer seen.deinit(allocator);
    var ordered = std.ArrayListUnmanaged([]const u8).empty;
    defer ordered.deinit(allocator);

    if (extra_labels) |extras| {
        for (extras) |raw| {
            const label = std.mem.trim(u8, raw, " \t\r\n");
            if (label.len == 0) continue;
            if (std.mem.startsWith(u8, label, "@gliner2:")) return error.ReservedEntityLabelPrefix;
            if (!seen.contains(label)) {
                try seen.put(allocator, label, {});
                try ordered.append(allocator, label);
            }
        }
    }
    for (records) |record| {
        for (record.tasks) |task| {
            if (task.kind != .entities) continue;
            if (task.schema_fields.len > 0) {
                for (task.schema_fields) |raw| {
                    const label = std.mem.trim(u8, raw, " \t\r\n");
                    if (label.len == 0) continue;
                    if (std.mem.startsWith(u8, label, "@gliner2:")) return error.ReservedEntityLabelPrefix;
                    if (!seen.contains(label)) {
                        try seen.put(allocator, label, {});
                        try ordered.append(allocator, label);
                    }
                }
            } else {
                for (task.fields) |field| {
                    const label = std.mem.trim(u8, field.name, " \t\r\n");
                    if (label.len == 0) continue;
                    if (std.mem.startsWith(u8, label, "@gliner2:")) return error.ReservedEntityLabelPrefix;
                    if (!seen.contains(label)) {
                        try seen.put(allocator, label, {});
                        try ordered.append(allocator, label);
                    }
                }
            }
        }
    }

    const out = try allocator.alloc([]const u8, ordered.items.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |label| allocator.free(label);
        allocator.free(out);
    }
    for (ordered.items, 0..) |label, idx| {
        out[idx] = try allocator.dupe(u8, label);
        initialized += 1;
    }
    return out;
}

/// Count positive entity annotations by raw manifest label. Contextual task
/// slots are deliberately ignored because their ordinal meaning is local to
/// each record.
pub fn countUpstreamEntityLabelPositives(
    allocator: std.mem.Allocator,
    records: []const UpstreamRecord,
    entity_labels: []const []const u8,
) ![]u64 {
    const counts = try allocator.alloc(u64, entity_labels.len);
    errdefer allocator.free(counts);
    @memset(counts, 0);

    var label_indices = std.StringHashMapUnmanaged(usize){};
    defer label_indices.deinit(allocator);
    for (entity_labels, 0..) |label, idx| {
        const entry = try label_indices.getOrPut(allocator, label);
        if (entry.found_existing) return error.DuplicateEntityLabel;
        entry.value_ptr.* = idx;
    }

    for (records) |record| {
        for (record.tasks) |task| {
            if (task.kind != .entities) continue;
            for (task.fields) |field| {
                const label = std.mem.trim(u8, field.name, " \t\r\n");
                if (label.len == 0) continue;
                const idx = label_indices.get(label) orelse return error.UnknownEntityLabel;
                counts[idx] += 1;
            }
        }
    }
    return counts;
}

pub fn maxUpstreamRecordLabelSlots(allocator: std.mem.Allocator, records: []const UpstreamRecord) !usize {
    var max_slots: usize = 0;
    for (records) |record| {
        const labels = try buildUpstreamTaskLabelVocab(allocator, &.{record}, null);
        defer {
            for (labels) |label| allocator.free(label);
            allocator.free(labels);
        }
        max_slots = @max(max_slots, labels.len);
    }
    return max_slots;
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

/// Measure the active token count using the same prompt/tokenizer path as
/// `buildSimpleBatch`, without allocating span grids or labels.
pub fn measureSimpleExampleEncodedLength(
    allocator: std.mem.Allocator,
    tokenizer: *const Tokenizer,
    example: Example,
    entity_types: []const []const u8,
    max_length: usize,
) !usize {
    return (try measureSimpleExampleEncoding(allocator, tokenizer, example, entity_types, max_length)).active_tokens;
}

/// Return the smallest fixed graph length that preserves both the observed
/// active tokens and the tokenizer's word-position capacity. The latter can
/// be larger when many short schema labels make the conservative workspace
/// reservation exceed the actual prompt length.
pub fn measureSimpleExampleRequiredLength(
    allocator: std.mem.Allocator,
    tokenizer: *const Tokenizer,
    example: Example,
    entity_types: []const []const u8,
    max_length: usize,
) !usize {
    const measured = try measureSimpleExampleEncoding(allocator, tokenizer, example, entity_types, max_length);
    const reserved = 3 + entity_types.len * 3;
    return @min(max_length, @max(measured.active_tokens, measured.num_words + reserved));
}

const SimpleEncodingMeasurement = struct {
    active_tokens: usize,
    num_words: usize,
};

fn measureSimpleExampleEncoding(
    allocator: std.mem.Allocator,
    tokenizer: *const Tokenizer,
    example: Example,
    entity_types: []const []const u8,
    max_length: usize,
) !SimpleEncodingMeasurement {
    if (max_length == 0) return error.InvalidMaxLength;
    const max_words = computeMaxWordsPerSample(max_length, entity_types.len);
    const input_ids = try allocator.alloc(i32, max_length);
    defer allocator.free(input_ids);
    const attention_mask = try allocator.alloc(i32, max_length);
    defer allocator.free(attention_mask);
    const words_mask = try allocator.alloc(i32, max_length);
    defer allocator.free(words_mask);
    const first_token_positions = try allocator.alloc(i32, max_words);
    defer allocator.free(first_token_positions);
    const e_token_positions = try allocator.alloc(i32, entity_types.len);
    defer allocator.free(e_token_positions);
    const e_token_end_positions = try allocator.alloc(i32, entity_types.len);
    defer allocator.free(e_token_end_positions);

    const encoded = try tokenizer.encodeInto(
        allocator,
        example.text,
        entity_types,
        input_ids,
        attention_mask,
        words_mask,
        first_token_positions,
        e_token_positions,
        e_token_end_positions,
    );
    var length: usize = 0;
    for (attention_mask) |active| length += @intFromBool(active != 0);
    return .{ .active_tokens = length, .num_words = encoded.num_words };
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
        const encode_result = try tokenizer.encodeInto(
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

/// Measure the active token count using the same schema/text encoder as the
/// upstream task batch builders, without allocating span grids or labels.
pub fn measureUpstreamRecordEncodedLength(
    allocator: std.mem.Allocator,
    tokenizer: *const Tokenizer,
    record: UpstreamRecord,
    max_length: usize,
) !usize {
    if (max_length == 0) return error.InvalidMaxLength;
    const max_schemas = @max(1, record.tasks.len);
    var max_schema_specials: usize = 1;
    for (record.tasks) |task| {
        const field_count = switch (task.kind) {
            .classifications => task.labels.len,
            .entities, .json_structures, .relations => if (task.schema_fields.len > 0)
                task.schema_fields.len
            else
                task.fields.len,
        };
        max_schema_specials = @max(max_schema_specials, field_count + 1);
    }
    const schema_special_capacity = try std.math.mul(usize, max_schemas, max_schema_specials);

    const input_ids = try allocator.alloc(i32, max_length);
    defer allocator.free(input_ids);
    const attention_mask = try allocator.alloc(i32, max_length);
    defer allocator.free(attention_mask);
    const words_mask = try allocator.alloc(i32, max_length);
    defer allocator.free(words_mask);
    const first_token_positions = try allocator.alloc(i32, max_length);
    defer allocator.free(first_token_positions);
    const task_type_ids = try allocator.alloc(i32, max_schemas);
    defer allocator.free(task_type_ids);
    const schema_special_positions = try allocator.alloc(i32, schema_special_capacity);
    defer allocator.free(schema_special_positions);
    const schema_special_counts = try allocator.alloc(i32, max_schemas);
    defer allocator.free(schema_special_counts);
    var no_entity_positions: [0]i32 = .{};

    @memset(input_ids, 0);
    @memset(attention_mask, 0);
    @memset(words_mask, 0);
    @memset(first_token_positions, -1);
    @memset(task_type_ids, 0);
    @memset(schema_special_positions, -1);
    @memset(schema_special_counts, 0);
    _ = try encodeUpstreamRecordInto(
        allocator,
        tokenizer,
        record,
        &.{},
        max_schemas,
        max_schema_specials,
        input_ids,
        attention_mask,
        words_mask,
        first_token_positions,
        &no_entity_positions,
        &no_entity_positions,
        task_type_ids,
        schema_special_positions,
        schema_special_counts,
    );
    var length: usize = 0;
    for (attention_mask) |active| length += @intFromBool(active != 0);
    return length;
}

pub fn buildUpstreamTaskBatch(
    allocator: std.mem.Allocator,
    tokenizer: *const Tokenizer,
    records: []const UpstreamRecord,
    entity_types: []const []const u8,
    max_length: usize,
    max_span_width: usize,
    batch_size: usize,
) !EncodedBatch {
    return buildUpstreamTaskBatchImpl(
        allocator,
        tokenizer,
        records,
        entity_types,
        entity_types.len,
        false,
        max_length,
        max_span_width,
        batch_size,
    );
}

/// Build one fixed-shape batch whose schema slots are local to each sample.
/// Slot 0 in two samples may name different fields, exactly as upstream's
/// per-sample schema embedding lists do; inactive tail slots stay masked.
pub fn buildUpstreamTaskBatchWithLocalSlots(
    allocator: std.mem.Allocator,
    tokenizer: *const Tokenizer,
    records: []const UpstreamRecord,
    num_slots: usize,
    max_length: usize,
    max_span_width: usize,
    batch_size: usize,
) !EncodedBatch {
    if (num_slots == 0) return error.InvalidEntityTypes;
    return buildUpstreamTaskBatchImpl(
        allocator,
        tokenizer,
        records,
        &.{},
        num_slots,
        true,
        max_length,
        max_span_width,
        batch_size,
    );
}

fn buildUpstreamTaskBatchImpl(
    allocator: std.mem.Allocator,
    tokenizer: *const Tokenizer,
    records: []const UpstreamRecord,
    shared_entity_types: []const []const u8,
    num_entity_slots: usize,
    local_slots: bool,
    max_length: usize,
    max_span_width: usize,
    batch_size: usize,
) !EncodedBatch {
    const effective_batch = @min(batch_size, records.len);
    const max_schemas = @max(1, maxTaskCount(records[0..effective_batch]));
    if (max_length == 0 or max_span_width == 0) return error.TooManySchemaTasks;
    const schema_row_capacity = std.math.mul(usize, max_length, max_span_width) catch
        return error.TooManySchemaTasks;
    if (max_schemas > schema_row_capacity) return error.TooManySchemaTasks;
    const max_words_per_sample = try upstreamBatchWordCapacity(
        records[0..effective_batch],
        max_schemas,
        max_length,
        max_span_width,
    );
    const max_spans = max_words_per_sample * max_span_width;
    const max_schema_specials = num_entity_slots + 1;

    var input_ids = try allocator.alloc(i32, effective_batch * max_length);
    errdefer allocator.free(input_ids);
    var attention_mask = try allocator.alloc(i32, effective_batch * max_length);
    errdefer allocator.free(attention_mask);
    var words_mask = try allocator.alloc(i32, effective_batch * max_length);
    errdefer allocator.free(words_mask);
    var first_token_positions = try allocator.alloc(i32, effective_batch * max_words_per_sample);
    errdefer allocator.free(first_token_positions);
    var word_lengths = try allocator.alloc(f32, effective_batch * max_words_per_sample);
    errdefer allocator.free(word_lengths);
    var word_has_digit = try allocator.alloc(f32, effective_batch * max_words_per_sample);
    errdefer allocator.free(word_has_digit);
    var word_is_title = try allocator.alloc(f32, effective_batch * max_words_per_sample);
    errdefer allocator.free(word_is_title);
    var word_is_all_caps = try allocator.alloc(f32, effective_batch * max_words_per_sample);
    errdefer allocator.free(word_is_all_caps);
    var span_indices = try allocator.alloc(i32, effective_batch * max_spans * 2);
    errdefer allocator.free(span_indices);
    var span_mask = try allocator.alloc(f32, effective_batch * max_spans);
    errdefer allocator.free(span_mask);
    var span_labels = try allocator.alloc(f32, effective_batch * max_spans * num_entity_slots);
    errdefer allocator.free(span_labels);
    var e_token_positions = try allocator.alloc(i32, effective_batch * num_entity_slots);
    errdefer allocator.free(e_token_positions);
    var e_token_end_positions = try allocator.alloc(i32, effective_batch * num_entity_slots);
    errdefer allocator.free(e_token_end_positions);
    var entity_type_kind = try allocator.alloc(i32, effective_batch * num_entity_slots);
    errdefer allocator.free(entity_type_kind);
    var text_word_counts = try allocator.alloc(i32, effective_batch);
    errdefer allocator.free(text_word_counts);
    var schema_counts = try allocator.alloc(i32, effective_batch);
    errdefer allocator.free(schema_counts);
    var task_type_ids = try allocator.alloc(i32, effective_batch * max_schemas);
    errdefer allocator.free(task_type_ids);
    var schema_special_positions = try allocator.alloc(i32, effective_batch * max_schemas * max_schema_specials);
    errdefer allocator.free(schema_special_positions);
    var schema_special_counts = try allocator.alloc(i32, effective_batch * max_schemas);
    errdefer allocator.free(schema_special_counts);

    @memset(input_ids, 0);
    @memset(attention_mask, 0);
    @memset(words_mask, 0);
    @memset(first_token_positions, -1);
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
    @memset(text_word_counts, 0);
    @memset(schema_counts, 0);
    @memset(task_type_ids, 0);
    @memset(schema_special_positions, -1);
    @memset(schema_special_counts, 0);

    for (records[0..effective_batch], 0..) |record, b| {
        const local_entity_types = if (local_slots)
            try buildUpstreamTaskLabelVocab(allocator, &.{record}, null)
        else
            null;
        defer if (local_entity_types) |labels| {
            for (labels) |label| allocator.free(label);
            allocator.free(labels);
        };
        const entity_types = local_entity_types orelse shared_entity_types;
        if (entity_types.len > num_entity_slots) return error.TooManySchemaSlots;
        const input_offset = b * max_length;
        const word_offset = b * max_words_per_sample;
        const span_offset = b * max_spans;
        const e_offset = b * num_entity_slots;
        const schema_offset = b * max_schemas;
        const schema_special_offset = b * max_schemas * max_schema_specials;

        const encode_result = try encodeUpstreamRecordInto(
            allocator,
            tokenizer,
            record,
            entity_types,
            max_schemas,
            max_schema_specials,
            input_ids[input_offset .. input_offset + max_length],
            attention_mask[input_offset .. input_offset + max_length],
            words_mask[input_offset .. input_offset + max_length],
            first_token_positions[word_offset .. word_offset + max_words_per_sample],
            e_token_positions[e_offset .. e_offset + num_entity_slots],
            e_token_end_positions[e_offset .. e_offset + num_entity_slots],
            task_type_ids[schema_offset .. schema_offset + max_schemas],
            schema_special_positions[schema_special_offset .. schema_special_offset + max_schemas * max_schema_specials],
            schema_special_counts[schema_offset .. schema_offset + max_schemas],
        );
        text_word_counts[b] = @intCast(encode_result.num_words);
        schema_counts[b] = @intCast(encode_result.num_schemas);

        try fillUpstreamWordSurfaceFeatures(
            allocator,
            record,
            encode_result.num_words,
            word_lengths[word_offset .. word_offset + max_words_per_sample],
            word_has_digit[word_offset .. word_offset + max_words_per_sample],
            word_is_title[word_offset .. word_offset + max_words_per_sample],
            word_is_all_caps[word_offset .. word_offset + max_words_per_sample],
        );
        try fillUpstreamSpanGrid(
            allocator,
            record,
            entity_types,
            num_entity_slots,
            encode_result.num_words,
            max_words_per_sample,
            max_span_width,
            span_indices[span_offset * 2 .. (span_offset + max_spans) * 2],
            span_mask[span_offset .. span_offset + max_spans],
            span_labels[span_offset * num_entity_slots .. (span_offset + max_spans) * num_entity_slots],
            entity_type_kind[e_offset .. e_offset + num_entity_slots],
        );
    }

    return .{
        .allocator = allocator,
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
        .text_word_counts = text_word_counts,
        .schema_counts = schema_counts,
        .task_type_ids = task_type_ids,
        .schema_special_positions = schema_special_positions,
        .schema_special_counts = schema_special_counts,
        .batch_size = effective_batch,
        .max_length = max_length,
        .max_words_per_sample = max_words_per_sample,
        .max_spans = max_spans,
        .num_entity_types = num_entity_slots,
        .max_schemas = max_schemas,
        .max_schema_specials = max_schema_specials,
        .uses_upstream_word_splitting = true,
    };
}

/// Fastino pads text-word routing and span work to the longest text in the
/// current batch, not to the encoder sequence ceiling. Keep a small bounded
/// set of graph signatures by rounding that count up to a power of two. Schema
/// objectives are packed into the first span rows, so their count supplies an
/// independent lower bound even for very short texts.
fn upstreamBatchWordCapacity(
    records: []const UpstreamRecord,
    max_schemas: usize,
    max_length: usize,
    max_span_width: usize,
) !usize {
    std.debug.assert(max_length > 0 and max_span_width > 0);
    var required_words = max_schemas / max_span_width + @intFromBool(max_schemas % max_span_width != 0);
    for (records) |record| {
        try validateUpstreamLowerableText(record.text);
        required_words = @max(required_words, upstreamRecordWordCount(record));
    }
    required_words = @max(required_words, 1);
    const bucket = std.math.ceilPowerOfTwo(usize, required_words) catch max_length;
    return @min(bucket, max_length);
}

fn upstreamRecordWordCount(record: UpstreamRecord) usize {
    var count = record.prefix_tokens.len;
    const needs_period = upstreamTextNeedsTerminalPeriod(record.text);
    var appended_period_joins_url = false;
    var text_idx: usize = 0;
    while (text_idx < record.text.len) {
        while (text_idx < record.text.len and isUpstreamWhitespaceAt(record.text, text_idx)) : (text_idx = nextCodepointEnd(record.text, text_idx)) {}
        if (text_idx >= record.text.len) break;
        const token_start = text_idx;
        text_idx = nextUpstreamTextTokenEnd(record.text, text_idx);
        appended_period_joins_url = needs_period and text_idx == record.text.len and
            (startsWithHttpUrl(record.text, token_start) or startsWithWwwUrl(record.text, token_start));
        count += 1;
    }
    return count + @intFromBool(needs_period and !appended_period_joins_url);
}

const UpstreamEncodeResult = struct {
    num_words: usize,
    num_schemas: usize,
};

fn encodeUpstreamRecordInto(
    allocator: std.mem.Allocator,
    tokenizer: *const Tokenizer,
    record: UpstreamRecord,
    entity_types: []const []const u8,
    max_schemas: usize,
    max_schema_specials: usize,
    input_ids: []i32,
    attention_mask: []i32,
    words_mask: []i32,
    first_token_positions: []i32,
    e_token_positions: []i32,
    e_token_end_positions: []i32,
    task_type_ids: []i32,
    schema_special_positions: []i32,
    schema_special_counts: []i32,
) !UpstreamEncodeResult {
    var appended_text: ?[]u8 = null;
    defer if (appended_text) |text| allocator.free(text);
    if (upstreamTextNeedsTerminalPeriod(record.text)) {
        appended_text = try std.mem.concat(allocator, u8, &.{ record.text, "." });
    }
    const effective_text: []const u8 = appended_text orelse record.text;
    try validateUpstreamLowerableText(effective_text);
    var pos: usize = 0;
    var schema_idx: usize = 0;
    for (record.tasks) |task| {
        if (schema_idx >= max_schemas) return error.BatchShapeMismatch;
        if (schema_idx > 0) try appendSpecialInputToken(tokenizer.sep_struct_token_id, input_ids, attention_mask, &pos);
        task_type_ids[schema_idx] = upstreamTaskTypeId(task.kind);
        try appendUpstreamSchema(
            allocator,
            tokenizer,
            task,
            entity_types,
            schema_idx,
            max_schema_specials,
            input_ids,
            attention_mask,
            &pos,
            e_token_positions,
            e_token_end_positions,
            schema_special_positions[schema_idx * max_schema_specials .. (schema_idx + 1) * max_schema_specials],
            &schema_special_counts[schema_idx],
        );
        schema_idx += 1;
    }
    try appendSpecialInputToken(tokenizer.sep_text_token_id, input_ids, attention_mask, &pos);

    var num_words: usize = 0;
    for (record.prefix_tokens) |token| {
        const appended = try appendUpstreamTextToken(
            allocator,
            tokenizer,
            token,
            null,
            input_ids,
            attention_mask,
            words_mask,
            first_token_positions,
            &pos,
            &num_words,
        );
        if (!appended) return error.UnencodableTextToken;
    }
    var text_idx: usize = 0;
    while (text_idx < effective_text.len) {
        while (text_idx < effective_text.len and isUpstreamWhitespaceAt(effective_text, text_idx)) : (text_idx = nextCodepointEnd(effective_text, text_idx)) {}
        if (text_idx >= effective_text.len) break;

        const token_start = text_idx;
        text_idx = nextUpstreamTextTokenEnd(effective_text, text_idx);
        const appended = try appendUpstreamTextToken(
            allocator,
            tokenizer,
            effective_text[token_start..text_idx],
            .{ .text = effective_text, .token_start = token_start },
            input_ids,
            attention_mask,
            words_mask,
            first_token_positions,
            &pos,
            &num_words,
        );
        if (!appended) return error.UnencodableTextToken;
    }
    return .{ .num_words = num_words, .num_schemas = schema_idx };
}

fn upstreamTextNeedsTerminalPeriod(text: []const u8) bool {
    return text.len == 0 or !(text[text.len - 1] == '.' or text[text.len - 1] == '!' or text[text.len - 1] == '?');
}

const UpstreamLowerContext = struct {
    text: []const u8,
    token_start: usize,
};

fn appendUpstreamTextToken(
    allocator: std.mem.Allocator,
    tokenizer: *const Tokenizer,
    token: []const u8,
    lower_context: ?UpstreamLowerContext,
    input_ids: []i32,
    attention_mask: []i32,
    words_mask: []i32,
    first_token_positions: []i32,
    pos: *usize,
    num_words: *usize,
) !bool {
    if (token.len == 0) return false;
    if (num_words.* >= first_token_positions.len or pos.* >= input_ids.len) return error.SequenceTooLong;
    first_token_positions[num_words.*] = @intCast(pos.*);
    const encoded_token = if (lower_context) |context| blk: {
        try validateUpstreamLowerableText(context.text);
        break :blk try lowerUpstreamTokenAlloc(allocator, token, context);
    } else blk: {
        try validateSupportedUpstreamSchemaFragment(token);
        break :blk try allocator.dupe(u8, token);
    };
    defer allocator.free(encoded_token);
    const next_pos = try tokenizer.encodeHFFragmentExactIntoAllocating(allocator, encoded_token, input_ids, attention_mask, pos.*, input_ids.len);
    if (next_pos == pos.*) return false;
    for (pos.*..next_pos) |token_pos| words_mask[token_pos] = @intCast(num_words.* + 1);
    pos.* = next_pos;
    num_words.* += 1;
    return true;
}

/// Python 3.12 has one lowercase expansion: U+0130 becomes U+0069 U+0307.
/// Two lowercased tokens cannot retain one-to-one byte boundaries into the
/// original UTF-8 text, so reject that scalar instead of corrupting supervision
/// and decoded spans. Every simple Unicode 15.0 lowercase mapping is supported.
fn validateUpstreamLowerableText(text: []const u8) !void {
    var view = std.unicode.Utf8View.init(text) catch return error.InvalidUtf8;
    var iter = view.iterator();
    while (iter.nextCodepoint()) |cp| {
        if (cp == 0x0130) return error.UnsupportedUnicodeLowerExpansion;
    }
}

/// The native Unigram path does not yet implement the tokenizer JSON's
/// precompiled Unicode normalizer. Preserve the existing exact schema subset
/// until that separate tokenizer gap is closed.
fn validateSupportedUpstreamSchemaFragment(fragment: []const u8) !void {
    var view = std.unicode.Utf8View.init(fragment) catch return error.InvalidUtf8;
    var iter = view.iterator();
    while (iter.nextCodepoint()) |cp| {
        if (cp < 0x80) continue;
        const supported_lower_latin1 = (cp >= 0x00df and cp <= 0x00f6) or (cp >= 0x00f8 and cp <= 0x00ff);
        if (!supported_lower_latin1) return error.UnsupportedUnicodePreprocessing;
    }
}

fn lowerUpstreamTokenAlloc(allocator: std.mem.Allocator, token: []const u8, context: UpstreamLowerContext) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(allocator);
    var token_pos: usize = 0;
    while (token_pos < token.len) {
        const cp = decodeCodepointAt(token, token_pos);
        if (cp == 0x0130) return error.UnsupportedUnicodeLowerExpansion;
        const absolute_pos = context.token_start + token_pos;
        const lowered = if (cp == 0x03a3 and isFinalSigma(context.text, absolute_pos)) @as(u21, 0x03c2) else upstream_unicode.simpleLower(cp);
        var encoded: [4]u8 = undefined;
        const len = try std.unicode.utf8Encode(lowered, &encoded);
        try out.appendSlice(allocator, encoded[0..len]);
        token_pos = nextCodepointEnd(token, token_pos);
    }
    return try out.toOwnedSlice(allocator);
}

fn isFinalSigma(text: []const u8, sigma_start: usize) bool {
    var before = sigma_start;
    var preceded_by_cased = false;
    while (before > 0) {
        before = previousCodepointStart(text, before);
        const cp = decodeCodepointAt(text, before);
        if (upstream_unicode.isCaseIgnorable(cp)) continue;
        preceded_by_cased = upstream_unicode.isCased(cp);
        break;
    }
    if (!preceded_by_cased) return false;

    var after = nextCodepointEnd(text, sigma_start);
    while (after < text.len) : (after = nextCodepointEnd(text, after)) {
        const cp = decodeCodepointAt(text, after);
        if (upstream_unicode.isCaseIgnorable(cp)) continue;
        return !upstream_unicode.isCased(cp);
    }
    return true;
}

test "upstream Unicode lowercase applies CPython final sigma context" {
    const allocator = std.testing.allocator;
    const cases = [_]struct { text: []const u8, token_start: usize, token_end: usize, expected: []const u8 }{
        .{ .text = "ΟΣ", .token_start = 0, .token_end = "ΟΣ".len, .expected = "ος" },
        .{ .text = "ΟΣΑ", .token_start = 0, .token_end = "ΟΣΑ".len, .expected = "οσα" },
        .{ .text = "A\u{0301}Σ", .token_start = "A\u{0301}".len, .token_end = "A\u{0301}Σ".len, .expected = "ς" },
        .{ .text = "A-Σ", .token_start = 0, .token_end = "A-Σ".len, .expected = "a-σ" },
        .{ .text = "Ⱥ", .token_start = 0, .token_end = "Ⱥ".len, .expected = "ⱥ" },
    };
    for (cases) |case| {
        const lowered = try lowerUpstreamTokenAlloc(
            allocator,
            case.text[case.token_start..case.token_end],
            .{ .text = case.text, .token_start = case.token_start },
        );
        defer allocator.free(lowered);
        try std.testing.expectEqualStrings(case.expected, lowered);
    }
}

fn nextUpstreamTextTokenEnd(text: []const u8, start: usize) usize {
    if (startsWithHttpUrl(text, start) or startsWithWwwUrl(text, start)) return scanUntilWhitespace(text, start);
    if (scanEmail(text, start)) |end| return end;
    if (text[start] == '@' and start + 1 < text.len and isRegexAsciiAlnumOrUnderscoreAt(text, start + 1)) {
        var idx = nextCodepointEnd(text, start + 1);
        while (idx < text.len and isRegexAsciiAlnumOrUnderscoreAt(text, idx)) : (idx = nextCodepointEnd(text, idx)) {}
        return idx;
    }
    if (isUpstreamWordAt(text, start)) return scanUpstreamWord(text, start);
    return nextCodepointEnd(text, start);
}

fn scanUpstreamWord(text: []const u8, start: usize) usize {
    var idx = start;
    while (idx < text.len and isUpstreamWordAt(text, idx)) : (idx = nextCodepointEnd(text, idx)) {}
    while (idx + 1 < text.len and (text[idx] == '-' or text[idx] == '_') and isUpstreamWordAt(text, idx + 1)) {
        idx += 1;
        while (idx < text.len and isUpstreamWordAt(text, idx)) : (idx = nextCodepointEnd(text, idx)) {}
    }
    return idx;
}

fn scanUntilWhitespace(text: []const u8, start: usize) usize {
    var idx = start;
    while (idx < text.len and !isUpstreamWhitespaceAt(text, idx)) : (idx = nextCodepointEnd(text, idx)) {}
    return idx;
}

/// Match `[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}` with the same
/// leftmost/greedy behavior as Python's case-insensitive branch.
fn scanEmail(text: []const u8, start: usize) ?usize {
    var idx = start;
    while (idx < text.len and isEmailLocalAt(text, idx)) : (idx = nextCodepointEnd(text, idx)) {}
    if (idx == start or idx >= text.len or text[idx] != '@') return null;
    idx += 1;
    const domain_start = idx;
    var separator_valid = false;
    var tld_letters: usize = 0;
    var best_end: ?usize = null;
    while (idx < text.len and isEmailDomainAt(text, idx)) {
        const cp = decodeCodepointAt(text, idx);
        const end = nextCodepointEnd(text, idx);
        if (cp == '.') {
            separator_valid = idx > domain_start;
            tld_letters = 0;
        } else if (separator_valid and isRegexAsciiLetter(cp)) {
            tld_letters += 1;
            if (tld_letters >= 2) best_end = end;
        } else {
            separator_valid = false;
            tld_letters = 0;
        }
        idx = end;
    }
    return best_end;
}

fn startsWithHttpUrl(text: []const u8, start: usize) bool {
    return startsWithRegexLiteralIgnoreCase(text, start, "http://") or startsWithRegexLiteralIgnoreCase(text, start, "https://");
}

fn startsWithWwwUrl(text: []const u8, start: usize) bool {
    return startsWithRegexLiteralIgnoreCase(text, start, "www.");
}

fn startsWithRegexLiteralIgnoreCase(text: []const u8, start: usize, literal: []const u8) bool {
    var pos = start;
    for (literal) |expected| {
        if (pos >= text.len) return false;
        const cp = decodeCodepointAt(text, pos);
        const folded = upstream_unicode.simpleLower(cp);
        if (folded != expected and !(expected == 'i' and cp == 0x0131) and !(expected == 's' and cp == 0x017f)) return false;
        pos = nextCodepointEnd(text, pos);
    }
    return true;
}

fn isEmailLocalAt(text: []const u8, start: usize) bool {
    const cp = decodeCodepointAt(text, start);
    return isRegexAsciiLetter(cp) or (cp >= '0' and cp <= '9') or cp == '.' or cp == '_' or cp == '%' or cp == '+' or cp == '-';
}

fn isEmailDomainAt(text: []const u8, start: usize) bool {
    const cp = decodeCodepointAt(text, start);
    return isRegexAsciiLetter(cp) or (cp >= '0' and cp <= '9') or cp == '.' or cp == '-';
}

fn isRegexAsciiAlnumOrUnderscoreAt(text: []const u8, start: usize) bool {
    const cp = decodeCodepointAt(text, start);
    return isRegexAsciiLetter(cp) or (cp >= '0' and cp <= '9') or cp == '_';
}

fn isRegexAsciiLetter(cp: u21) bool {
    const folded = upstream_unicode.simpleLower(cp);
    return (folded >= 'a' and folded <= 'z') or cp == 0x0131 or cp == 0x017f;
}

fn isUpstreamWordAt(text: []const u8, start: usize) bool {
    return upstream_unicode.isWord(decodeCodepointAt(text, start));
}

fn isUpstreamWhitespaceAt(text: []const u8, start: usize) bool {
    return upstream_unicode.isWhitespace(decodeCodepointAt(text, start));
}

fn decodeCodepointAt(text: []const u8, start: usize) u21 {
    const end = nextCodepointEnd(text, start);
    return std.unicode.utf8Decode(text[start..end]) catch unreachable;
}

fn nextCodepointEnd(text: []const u8, start: usize) usize {
    const len = std.unicode.utf8ByteSequenceLength(text[start]) catch unreachable;
    return @min(text.len, start + len);
}

fn previousCodepointStart(text: []const u8, end: usize) usize {
    var start = end - 1;
    while (start > 0 and (text[start] & 0xc0) == 0x80) start -= 1;
    return start;
}

fn appendUpstreamSchema(
    allocator: std.mem.Allocator,
    tokenizer: *const Tokenizer,
    task: UpstreamTask,
    entity_types: []const []const u8,
    schema_idx: usize,
    max_schema_specials: usize,
    input_ids: []i32,
    attention_mask: []i32,
    pos: *usize,
    e_token_positions: []i32,
    e_token_end_positions: []i32,
    schema_special_positions: []i32,
    schema_special_count: *i32,
) !void {
    _ = schema_idx;
    try appendInputFragment(allocator, tokenizer, "(", input_ids, attention_mask, pos);
    try appendSchemaSpecial(tokenizer.p_token_id, input_ids, attention_mask, pos, schema_special_positions, schema_special_count, max_schema_specials);
    if (task.prompt) |prompt| {
        if (prompt.len > 0) {
            const prompt_text = try std.fmt.allocPrint(allocator, "{s}: {s}", .{ task.name, prompt });
            defer allocator.free(prompt_text);
            try appendInputFragment(allocator, tokenizer, prompt_text, input_ids, attention_mask, pos);
        } else {
            try appendInputFragment(allocator, tokenizer, task.name, input_ids, attention_mask, pos);
        }
    } else {
        try appendInputFragment(allocator, tokenizer, task.name, input_ids, attention_mask, pos);
    }
    for (task.label_descriptions) |description| {
        if (tokenizer.description_token_id <= 0) return error.MissingSchemaConditioningTokens;
        try appendSpecialInputToken(tokenizer.description_token_id, input_ids, attention_mask, pos);
        const description_text = try std.fmt.allocPrint(allocator, "{s}: {s}", .{ description.label, description.description });
        defer allocator.free(description_text);
        try appendInputFragment(allocator, tokenizer, description_text, input_ids, attention_mask, pos);
    }
    for (task.examples) |example| {
        if (tokenizer.example_token_id <= 0 or tokenizer.output_token_id <= 0) return error.MissingSchemaConditioningTokens;
        try appendSpecialInputToken(tokenizer.example_token_id, input_ids, attention_mask, pos);
        try appendInputFragment(allocator, tokenizer, example.input, input_ids, attention_mask, pos);
        try appendSpecialInputToken(tokenizer.output_token_id, input_ids, attention_mask, pos);
        try appendInputFragment(allocator, tokenizer, example.output, input_ids, attention_mask, pos);
    }
    try appendInputFragment(allocator, tokenizer, "(", input_ids, attention_mask, pos);

    switch (task.kind) {
        .classifications => {
            for (task.labels) |label| {
                const marker_pos = pos.*;
                try appendSchemaSpecial(tokenizer.l_token_id, input_ids, attention_mask, pos, schema_special_positions, schema_special_count, max_schema_specials);
                const qualified_label = try upstreamTaskFieldKey(allocator, task.kind, task.name, label);
                defer allocator.free(qualified_label);
                if (indexOfLabel(entity_types, qualified_label)) |label_idx| {
                    e_token_positions[label_idx] = @intCast(marker_pos);
                    e_token_end_positions[label_idx] = @intCast(pos.*);
                }
                try appendInputFragment(allocator, tokenizer, label, input_ids, attention_mask, pos);
            }
        },
        .entities => {
            // Gold field values can repeat a field name (multiple mentions of one
            // type); the schema prompt lists each field name ONCE.
            var seen = std.ArrayListUnmanaged([]const u8).empty;
            defer seen.deinit(allocator);
            if (task.schema_fields.len > 0) {
                try seen.appendSlice(allocator, task.schema_fields);
            } else {
                for (task.fields) |field| {
                    if (sliceContainsString(seen.items, field.name)) continue;
                    try seen.append(allocator, field.name);
                }
            }
            for (seen.items) |field_name| {
                const marker_pos = pos.*;
                try appendSchemaSpecial(tokenizer.ent_id, input_ids, attention_mask, pos, schema_special_positions, schema_special_count, max_schema_specials);
                if (indexOfLabel(entity_types, field_name)) |label_idx| {
                    e_token_positions[label_idx] = @intCast(marker_pos);
                    e_token_end_positions[label_idx] = @intCast(pos.*);
                }
                try appendInputFragment(allocator, tokenizer, field_name, input_ids, attention_mask, pos);
            }
        },
        .json_structures, .relations => {
            const marker_id = if (task.kind == .json_structures) tokenizer.c_token_id else tokenizer.r_token_id;
            var schema_names = std.ArrayListUnmanaged([]const u8).empty;
            defer schema_names.deinit(allocator);
            if (task.schema_fields.len > 0) {
                try schema_names.appendSlice(allocator, task.schema_fields);
            } else {
                // Legacy/synthetic callers can omit schema_fields; derive the
                // prompt schema from first-seen gold field names.
                for (task.fields) |field| {
                    if (sliceContainsString(schema_names.items, field.name)) continue;
                    try schema_names.append(allocator, field.name);
                }
            }
            for (schema_names.items) |field_name| {
                const marker_pos = pos.*;
                try appendSchemaSpecial(marker_id, input_ids, attention_mask, pos, schema_special_positions, schema_special_count, max_schema_specials);
                const label = try upstreamTaskFieldKey(allocator, task.kind, task.name, field_name);
                defer allocator.free(label);
                if (indexOfLabel(entity_types, label)) |label_idx| {
                    e_token_positions[label_idx] = @intCast(marker_pos);
                    e_token_end_positions[label_idx] = @intCast(pos.*);
                }
                try appendInputFragment(allocator, tokenizer, field_name, input_ids, attention_mask, pos);
            }
        },
    }

    try appendInputFragment(allocator, tokenizer, ")", input_ids, attention_mask, pos);
    try appendInputFragment(allocator, tokenizer, ")", input_ids, attention_mask, pos);
}

fn appendSchemaSpecial(
    token_id: i32,
    input_ids: []i32,
    attention_mask: []i32,
    pos: *usize,
    schema_special_positions: []i32,
    schema_special_count: *i32,
    max_schema_specials: usize,
) !void {
    if (@as(usize, @intCast(@max(schema_special_count.*, 0))) >= max_schema_specials) return error.TooManySchemaSpecials;
    schema_special_positions[@intCast(schema_special_count.*)] = @intCast(pos.*);
    schema_special_count.* += 1;
    try appendSpecialInputToken(token_id, input_ids, attention_mask, pos);
}

fn appendSpecialInputToken(token_id: i32, input_ids: []i32, attention_mask: []i32, pos: *usize) !void {
    if (pos.* >= input_ids.len) return error.SequenceTooLong;
    input_ids[pos.*] = token_id;
    attention_mask[pos.*] = 1;
    pos.* += 1;
}

fn appendInputFragment(
    allocator: std.mem.Allocator,
    tokenizer: *const Tokenizer,
    fragment: []const u8,
    input_ids: []i32,
    attention_mask: []i32,
    pos: *usize,
) !void {
    // The pinned tokenizer normalizes every schema fragment too. Until the
    // native path carries that Unicode normalizer, apply the same fail-closed
    // subset used for document tokens rather than silently conditioning on
    // different token IDs.
    try validateSupportedUpstreamSchemaFragment(fragment);
    pos.* = try tokenizer.encodeHFFragmentExactIntoAllocating(allocator, fragment, input_ids, attention_mask, pos.*, input_ids.len);
}

fn maxTaskCount(records: []const UpstreamRecord) usize {
    var max_count: usize = 0;
    for (records) |record| max_count = @max(max_count, record.tasks.len);
    return max_count;
}

pub fn upstreamTaskTypeId(kind: UpstreamTaskKind) i32 {
    return switch (kind) {
        .entities => 1,
        .json_structures => 2,
        .relations => 3,
        .classifications => 4,
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

const max_jsonl_line_bytes = 64 * 1024 * 1024;

fn loadJsonlFile(
    comptime T: type,
    allocator: std.mem.Allocator,
    path: []const u8,
    out: *std.ArrayListUnmanaged(T),
    comptime appendValue: fn (std.mem.Allocator, std.json.Value, *std.ArrayListUnmanaged(T)) anyerror!void,
) !void {
    const io = compat.io();
    var file = try compat.cwd().openFile(io, path, .{});
    defer file.close(io);
    // Dataset values use the caller's long-lived arena; keep transient I/O storage outside it.
    const reader_buffer = try std.heap.page_allocator.alloc(u8, max_jsonl_line_bytes);
    defer std.heap.page_allocator.free(reader_buffer);
    var reader = file.readerStreaming(io, reader_buffer);
    var line_number: usize = 0;
    while (try reader.interface.takeDelimiter('\n')) |raw_line| {
        line_number += 1;
        const line = std.mem.trim(u8, raw_line, " \t\r");
        if (line.len == 0) continue;
        // takeDelimiter reuses its buffer, so parsed strings must be copied into the arena.
        const parsed = std.json.parseFromSliceLeaky(std.json.Value, allocator, line, .{ .allocate = .alloc_always }) catch |err| {
            if (!builtin.is_test) std.debug.print("GLiNER2 JSONL parse failed at {s}:{d}: {s}\n", .{ path, line_number, @errorName(err) });
            return err;
        };
        appendValue(allocator, parsed, out) catch |err| {
            if (!builtin.is_test) std.debug.print("GLiNER2 dataset record rejected at {s}:{d}: {s}\n", .{ path, line_number, @errorName(err) });
            return err;
        };
    }
}

fn loadExamplesFromFile(allocator: std.mem.Allocator, path: []const u8, out: *std.ArrayListUnmanaged(Example)) !void {
    try loadJsonlFile(Example, allocator, path, out, appendExampleFromJsonValue);
}

fn loadTrainingRecordsFromFile(allocator: std.mem.Allocator, path: []const u8, out: *std.ArrayListUnmanaged(UpstreamRecord)) !void {
    try loadJsonlFile(UpstreamRecord, allocator, path, out, appendTrainingRecordFromJsonValue);
}

fn appendExampleFromJsonValue(allocator: std.mem.Allocator, value: std.json.Value, out: *std.ArrayListUnmanaged(Example)) !void {
    if (value != .object) return error.InvalidGliner2Example;
    const obj = value.object;
    if (obj.get("text")) |text_value| {
        const text = try jsonString(text_value);
        if (std.mem.trim(u8, text, " \t\r\n").len == 0) return error.MissingText;
        var entities: std.ArrayListUnmanaged(Entity) = .empty;
        errdefer entities.deinit(allocator);
        if (obj.get("entities")) |entities_value| {
            try appendLegacyEntitiesFromJson(allocator, text, entities_value, &entities);
        }
        try out.append(allocator, .{
            .text = text,
            .entities = try entities.toOwnedSlice(allocator),
        });
        return;
    }

    const input_value = obj.get("input") orelse return error.MissingText;
    const output_value = obj.get("output") orelse return error.InvalidGliner2Example;
    const text = try jsonString(input_value);
    if (std.mem.trim(u8, text, " \t\r\n").len == 0) return error.MissingText;
    var entities: std.ArrayListUnmanaged(Entity) = .empty;
    errdefer entities.deinit(allocator);
    try appendUpstreamOutputEntities(allocator, text, output_value, &entities);
    try out.append(allocator, .{
        .text = text,
        .entities = try entities.toOwnedSlice(allocator),
    });
}

fn appendTrainingRecordFromJsonValue(
    allocator: std.mem.Allocator,
    value: std.json.Value,
    out: *std.ArrayListUnmanaged(UpstreamRecord),
) !void {
    if (value != .object) return error.InvalidGliner2Example;
    const obj = value.object;
    var tasks = std.ArrayListUnmanaged(UpstreamTask).empty;
    errdefer tasks.deinit(allocator);

    if (obj.get("text")) |text_value| {
        const text = try jsonString(text_value);
        if (std.mem.trim(u8, text, " \t\r\n").len == 0) return error.MissingText;
        if (obj.get("entities")) |entities_value| {
            try appendLegacyEntityTasks(allocator, text, entities_value, &tasks);
        }
        if (tasks.items.len == 0) return error.NoGliner2Tasks;
        try out.append(allocator, .{
            .text = text,
            .tasks = try tasks.toOwnedSlice(allocator),
        });
        return;
    }

    const text = try jsonString(obj.get("input") orelse return error.MissingText);
    if (std.mem.trim(u8, text, " \t\r\n").len == 0) return error.MissingText;
    const output_value = obj.get("output") orelse return error.InvalidGliner2Example;
    if (output_value != .object) return error.InvalidGliner2Example;
    var prefix_tokens = std.ArrayListUnmanaged([]const u8).empty;
    errdefer prefix_tokens.deinit(allocator);
    try appendUpstreamOutputTasks(allocator, text, output_value.object, &tasks, &prefix_tokens);
    if (tasks.items.len == 0) return error.NoGliner2Tasks;
    try out.append(allocator, .{
        .text = text,
        .tasks = try tasks.toOwnedSlice(allocator),
        .prefix_tokens = try prefix_tokens.toOwnedSlice(allocator),
    });
}

fn appendLegacyEntityTasks(
    allocator: std.mem.Allocator,
    text: []const u8,
    value: std.json.Value,
    tasks: *std.ArrayListUnmanaged(UpstreamTask),
) !void {
    if (value != .array) return error.InvalidGliner2Example;
    var fields = std.ArrayListUnmanaged(UpstreamField).empty;
    defer fields.deinit(allocator);
    const char_to_word = try buildCharToWordMap(allocator, text);
    defer allocator.free(char_to_word);
    for (value.array.items) |entity_value| {
        if (entity_value != .object) return error.InvalidGliner2Example;
        const obj = entity_value.object;
        const mention = try jsonString(obj.get("text") orelse return error.InvalidGliner2Example);
        const label = try jsonString(obj.get("label") orelse return error.InvalidGliner2Example);
        if (invalidSchemaIdentifier(label)) return error.InvalidEntitySchema;
        const start = try jsonUsize(obj.get("start") orelse return error.InvalidGliner2Example);
        const end = try jsonUsize(obj.get("end") orelse return error.InvalidGliner2Example);
        if (mention.len == 0 or end > text.len or start >= end) return error.AnnotationNotFound;
        // The legacy {text, entities} contract historically used ASCII-only
        // case-insensitive matching. Reject non-ASCII annotations instead of
        // silently applying the wrong comparison; Unicode-capable datasets
        // should use the upstream {input, output} task contract.
        if (containsNonAscii(text[start..end]) or containsNonAscii(mention)) {
            return error.UnsupportedLegacyUnicodeAnnotation;
        }
        if (!std.ascii.eqlIgnoreCase(text[start..end], mention)) return error.AnnotationNotFound;
        if (char_to_word[start] < 0 or char_to_word[end - 1] < 0 or
            (start > 0 and char_to_word[start - 1] == char_to_word[start]) or
            (end < char_to_word.len and char_to_word[end] == char_to_word[end - 1]))
        {
            return error.AnnotationNotTokenAligned;
        }
        try fields.append(allocator, .{
            .name = label,
            .value = text[start..end],
            .start = start,
            .end = end,
        });
    }
    if (fields.items.len > 0) {
        try tasks.append(allocator, .{
            .kind = .entities,
            .name = "entities",
            .fields = try fields.toOwnedSlice(allocator),
            .count = 1,
        });
    }
}

fn appendUpstreamOutputTasks(
    allocator: std.mem.Allocator,
    text: []const u8,
    output: std.json.ObjectMap,
    tasks: *std.ArrayListUnmanaged(UpstreamTask),
    prefix_tokens: *std.ArrayListUnmanaged([]const u8),
) !void {
    try validateUpstreamSchema(output);
    var entity_task: ?UpstreamTask = null;
    if (output.get("entities")) |entity_map| {
        if (entity_map != .object) return error.InvalidGliner2Example;
        var fields = std.ArrayListUnmanaged(UpstreamField).empty;
        errdefer fields.deinit(allocator);
        var schema_fields = std.ArrayListUnmanaged([]const u8).empty;
        defer schema_fields.deinit(allocator);
        var iter = entity_map.object.iterator();
        while (iter.next()) |entry| {
            const schema_field = entry.key_ptr.*;
            if (schema_field.len == 0 or std.mem.startsWith(u8, schema_field, "@gliner2:")) return error.InvalidEntitySchema;
            try schema_fields.append(allocator, schema_field);
            try appendTaskFieldsFromValue(allocator, text, schema_field, entry.value_ptr.*, null, &fields);
        }
        if (schema_fields.items.len > 0) {
            const owned_schema_fields = try schema_fields.toOwnedSlice(allocator);
            entity_task = .{
                .kind = .entities,
                .name = "entities",
                .schema_fields = owned_schema_fields,
                .label_descriptions = try parseLabelDescriptions(allocator, output.get("entity_descriptions"), owned_schema_fields),
                .fields = try fields.toOwnedSlice(allocator),
                .count = 1,
            };
        }
    }

    if (output.get("json_structures")) |structures_value| {
        if (structures_value != .array) return error.InvalidGliner2Example;
        // Upstream GLiNER2 emits ONE schema per distinct structure NAME, with the
        // count = number of instances and the gold field values aggregated across
        // instances (the schema prompt lists each field name once; see
        // appendUpstreamSchema's dedup). Group instances by name in first-seen order.
        const descriptions = if (output.get("json_descriptions")) |value| value.object else null;
        try appendGroupedStructureTasks(allocator, text, structures_value, .json_structures, descriptions, prefix_tokens, tasks);
    }

    if (entity_task) |task| try tasks.append(allocator, task);

    if (output.get("relations")) |relations_value| {
        if (relations_value != .array) return error.InvalidGliner2Example;
        try appendGroupedStructureTasks(allocator, text, relations_value, .relations, null, prefix_tokens, tasks);
    }

    if (output.get("classifications")) |classifications_value| {
        if (classifications_value != .array) return error.InvalidGliner2Example;
        for (classifications_value.array.items) |classification_value| {
            if (classification_value != .object) return error.InvalidGliner2Example;
            const cls = classification_value.object;
            const name = try jsonString(cls.get("task") orelse return error.InvalidGliner2Example);
            for (tasks.items) |existing| {
                if (existing.kind == .classifications and std.mem.eql(u8, existing.name, name)) {
                    return error.DuplicateClassificationTaskName;
                }
            }
            const labels = try jsonStringArray(allocator, cls.get("labels") orelse return error.InvalidGliner2Example);
            const true_labels = try jsonStringArray(allocator, cls.get("true_label") orelse return error.InvalidGliner2Example);
            const multi_label = if (cls.get("multi_label")) |value| switch (value) {
                .bool => |enabled| enabled or true_labels.len > 1,
                else => return error.InvalidClassificationMultiLabel,
            } else true_labels.len > 1;
            if (std.mem.trim(u8, name, " \t\r\n").len == 0 or labels.len == 0) return error.InvalidClassificationSchema;
            for (labels, 0..) |label, label_idx| {
                if (std.mem.trim(u8, label, " \t\r\n").len == 0) return error.InvalidClassificationSchema;
                for (labels[0..label_idx]) |previous| {
                    if (std.mem.eql(u8, previous, label)) return error.DuplicateClassificationLabel;
                }
            }
            for (true_labels, 0..) |true_label, true_idx| {
                if (!sliceContainsString(labels, true_label)) return error.UnknownClassificationTrueLabel;
                for (true_labels[0..true_idx]) |previous| {
                    if (std.mem.eql(u8, previous, true_label)) return error.DuplicateClassificationTrueLabel;
                }
            }
            try tasks.append(allocator, .{
                .kind = .classifications,
                .name = name,
                .labels = labels,
                .true_labels = true_labels,
                .multi_label = multi_label,
                .prompt = if (cls.get("prompt")) |value| value.string else null,
                .label_descriptions = try parseLabelDescriptions(allocator, cls.get("label_descriptions"), labels),
                .examples = try parseClassificationExamples(allocator, cls.get("examples"), labels),
                .count = true_labels.len,
            });
        }
    }
}

fn appendLegacyEntitiesFromJson(
    allocator: std.mem.Allocator,
    text: []const u8,
    value: std.json.Value,
    entities: *std.ArrayListUnmanaged(Entity),
) !void {
    if (value != .array) return error.InvalidGliner2Example;
    for (value.array.items) |entity_value| {
        if (entity_value != .object) return error.InvalidGliner2Example;
        const obj = entity_value.object;
        const entity_text = try jsonString(obj.get("text") orelse return error.InvalidGliner2Example);
        const label = try jsonString(obj.get("label") orelse return error.InvalidGliner2Example);
        if (invalidSchemaIdentifier(label)) return error.InvalidEntitySchema;
        const start = try jsonUsize(obj.get("start") orelse return error.InvalidGliner2Example);
        const end = try jsonUsize(obj.get("end") orelse return error.InvalidGliner2Example);
        if (entity_text.len == 0 or end > text.len or start >= end) return error.AnnotationNotFound;
        if (containsNonAscii(text[start..end]) or containsNonAscii(entity_text)) {
            return error.UnsupportedLegacyUnicodeAnnotation;
        }
        if (!std.ascii.eqlIgnoreCase(text[start..end], entity_text)) return error.AnnotationNotFound;
        try entities.append(allocator, .{
            .text = text[start..end],
            .label = label,
            .start = start,
            .end = end,
        });
    }
}

fn appendUpstreamOutputEntities(
    allocator: std.mem.Allocator,
    text: []const u8,
    output_value: std.json.Value,
    entities: *std.ArrayListUnmanaged(Entity),
) !void {
    if (output_value != .object) return error.InvalidGliner2Example;
    const output = output_value.object;
    try validateUpstreamSchema(output);

    if (output.get("entities")) |entity_map| {
        if (entity_map != .object) return error.InvalidGliner2Example;
        var iter = entity_map.object.iterator();
        while (iter.next()) |entry| {
            try appendMentionValueAsEntities(allocator, text, entry.key_ptr.*, entry.value_ptr.*, entities);
        }
    }

    if (output.get("json_structures")) |structures_value| {
        if (structures_value != .array) return error.InvalidGliner2Example;
        for (structures_value.array.items) |structure_value| {
            if (structure_value != .object) return error.InvalidGliner2Example;
            var struct_iter = structure_value.object.iterator();
            while (struct_iter.next()) |struct_entry| {
                if (struct_entry.value_ptr.* != .object) continue;
                var field_iter = struct_entry.value_ptr.object.iterator();
                while (field_iter.next()) |field_entry| {
                    const label = try std.fmt.allocPrint(allocator, "{s}.{s}", .{ struct_entry.key_ptr.*, field_entry.key_ptr.* });
                    try appendMentionValueAsEntities(allocator, text, label, field_entry.value_ptr.*, entities);
                }
            }
        }
    }

    if (output.get("relations")) |relations_value| {
        if (relations_value != .array) return error.InvalidGliner2Example;
        for (relations_value.array.items) |relation_value| {
            if (relation_value != .object) return error.InvalidGliner2Example;
            var relation_iter = relation_value.object.iterator();
            while (relation_iter.next()) |relation_entry| {
                if (relation_entry.value_ptr.* != .object) continue;
                var field_iter = relation_entry.value_ptr.object.iterator();
                while (field_iter.next()) |field_entry| {
                    const label = try std.fmt.allocPrint(allocator, "{s}.{s}", .{ relation_entry.key_ptr.*, field_entry.key_ptr.* });
                    try appendMentionValueAsEntities(allocator, text, label, field_entry.value_ptr.*, entities);
                }
            }
        }
    }
}

fn validateUpstreamSchema(output: std.json.ObjectMap) !void {
    if (output.get("entity_descriptions")) |descriptions| {
        if (!validDescriptionMap(descriptions)) return error.InvalidEntityDescriptions;
    }
    if (output.get("json_descriptions")) |descriptions| {
        if (!validJsonDescriptionMap(descriptions)) return error.InvalidJsonDescriptions;
    }

    if (output.get("entities")) |entities| if (entities == .object) {
        var iter = entities.object.iterator();
        while (iter.next()) |entry| {
            if (invalidSchemaIdentifier(entry.key_ptr.*) or std.mem.startsWith(u8, entry.key_ptr.*, "@gliner2:")) {
                return error.InvalidEntitySchema;
            }
        }
    };

    for ([_][]const u8{ "json_structures", "relations" }) |task_kind| {
        const structures = output.get(task_kind) orelse continue;
        if (structures != .array) continue;
        for (structures.array.items) |structure| {
            if (structure != .object) continue;
            var task_iter = structure.object.iterator();
            while (task_iter.next()) |task| {
                if (invalidSchemaIdentifier(task.key_ptr.*)) return error.InvalidStructureSchema;
                if (task.value_ptr.* != .object) continue;
                var field_iter = task.value_ptr.object.iterator();
                while (field_iter.next()) |field| {
                    if (invalidSchemaIdentifier(field.key_ptr.*)) return error.InvalidStructureSchema;
                    try rejectInvalidChoiceIdentifiers(field.value_ptr.*);
                }
            }
        }
    }

    const classifications = output.get("classifications") orelse return;
    if (classifications != .array) return;
    for (classifications.array.items) |classification| {
        if (classification != .object) continue;
        const cls = classification.object;
        if (cls.get("prompt")) |prompt| if (prompt != .string) return error.InvalidClassificationPrompt;
        if (cls.get("multi_label")) |multi_label| if (multi_label != .bool) return error.InvalidClassificationMultiLabel;
        if (cls.get("examples")) |examples| if (!validClassificationExamples(examples)) return error.InvalidClassificationExamples;
        if (cls.get("label_descriptions")) |descriptions| if (!validDescriptionMap(descriptions)) return error.InvalidClassificationLabelDescriptions;
        if (cls.get("labels")) |labels| if (labels == .array) {
            if (cls.get("label_descriptions")) |descriptions| if (descriptions == .object) {
                var description_iter = descriptions.object.iterator();
                while (description_iter.next()) |entry| {
                    if (!jsonStringArrayContains(labels.array.items, entry.key_ptr.*)) return error.UnknownLabelDescription;
                }
            };
            if (cls.get("examples")) |examples| if (examples == .array) {
                for (examples.array.items) |example| {
                    if (example == .array and example.array.items.len == 2 and example.array.items[1] == .string and
                        !jsonStringArrayContains(labels.array.items, example.array.items[1].string))
                    {
                        return error.UnknownClassificationExampleLabel;
                    }
                }
            };
        };
        if (cls.get("task")) |task| if (task == .string and invalidSchemaIdentifier(task.string)) {
            return error.InvalidClassificationSchema;
        };
        for ([_][]const u8{ "labels", "true_label" }) |label_key| {
            const labels = cls.get(label_key) orelse continue;
            if (labels != .array) continue;
            for (labels.array.items) |label| {
                if (label == .string and invalidSchemaIdentifier(label.string)) return error.InvalidClassificationSchema;
            }
        }
    }
}

fn jsonStringArrayContains(values: []const std.json.Value, needle: []const u8) bool {
    for (values) |value| {
        if (value == .string and std.mem.eql(u8, value.string, needle)) return true;
    }
    return false;
}

fn validDescriptionMap(value: std.json.Value) bool {
    if (value != .object) return false;
    var iter = value.object.iterator();
    while (iter.next()) |entry| {
        if (invalidSchemaIdentifier(entry.key_ptr.*) or entry.value_ptr.* != .string) return false;
    }
    return true;
}

fn validJsonDescriptionMap(value: std.json.Value) bool {
    if (value != .object) return false;
    var iter = value.object.iterator();
    while (iter.next()) |entry| {
        if (invalidSchemaIdentifier(entry.key_ptr.*) or !validDescriptionMap(entry.value_ptr.*)) return false;
    }
    return true;
}

fn validClassificationExamples(value: std.json.Value) bool {
    if (value != .array) return false;
    for (value.array.items) |example| {
        if (example != .array or example.array.items.len != 2 or
            example.array.items[0] != .string or example.array.items[1] != .string) return false;
    }
    return true;
}

fn parseLabelDescriptions(
    allocator: std.mem.Allocator,
    value: ?std.json.Value,
    labels: []const []const u8,
) ![]const UpstreamLabelDescription {
    const descriptions = value orelse return &.{};
    var out = std.ArrayListUnmanaged(UpstreamLabelDescription).empty;
    defer out.deinit(allocator);
    var iter = descriptions.object.iterator();
    while (iter.next()) |entry| {
        if (!sliceContainsString(labels, entry.key_ptr.*)) return error.UnknownLabelDescription;
        try out.append(allocator, .{
            .label = entry.key_ptr.*,
            .description = entry.value_ptr.string,
        });
    }
    return out.toOwnedSlice(allocator);
}

fn parseClassificationExamples(
    allocator: std.mem.Allocator,
    value: ?std.json.Value,
    labels: []const []const u8,
) ![]const UpstreamClassificationExample {
    const examples = value orelse return &.{};
    var out = std.ArrayListUnmanaged(UpstreamClassificationExample).empty;
    defer out.deinit(allocator);
    for (examples.array.items) |example| {
        const output = example.array.items[1].string;
        if (!sliceContainsString(labels, output)) return error.UnknownClassificationExampleLabel;
        try out.append(allocator, .{
            .input = example.array.items[0].string,
            .output = output,
        });
    }
    return out.toOwnedSlice(allocator);
}

fn invalidSchemaIdentifier(value: []const u8) bool {
    return value.len == 0 or std.ascii.isWhitespace(value[0]) or std.ascii.isWhitespace(value[value.len - 1]);
}

fn containsNonAscii(value: []const u8) bool {
    for (value) |byte| {
        if (byte >= 0x80) return true;
    }
    return false;
}

const CanonicalScoringIterator = struct {
    iterator: std.unicode.Utf8Iterator,
    pending_codepoint: ?u21 = null,
    pending_space: bool = false,
    emitted_value: bool = false,

    fn init(value: []const u8) !CanonicalScoringIterator {
        const view = std.unicode.Utf8View.init(value) catch return error.InvalidUtf8;
        return .{ .iterator = view.iterator() };
    }

    fn next(self: *CanonicalScoringIterator) !?u21 {
        if (self.pending_codepoint) |cp| {
            self.pending_codepoint = null;
            self.emitted_value = true;
            return cp;
        }
        while (self.iterator.nextCodepoint()) |cp| {
            if (upstream_unicode.isWhitespace(cp)) {
                if (self.emitted_value) self.pending_space = true;
                continue;
            }
            // The Python oracle first applies NFC and then full casefold. Zig
            // admits only concatenation-safe, normalization-inert scalars
            // whose casefold is exactly one pinned simple-lower scalar. This
            // is intentionally conservative and errors instead of silently
            // producing a different score for unsupported Unicode.
            if (!upstream_unicode.isPinnedNormalizerInert(cp)) return error.UnsupportedCanonicalNormalization;
            if (!upstream_unicode.isSimpleCasefold(cp)) return error.UnsupportedCanonicalCasefold;
            const folded = upstream_unicode.simpleLower(cp);
            if (self.pending_space) {
                self.pending_space = false;
                self.pending_codepoint = folded;
                return ' ';
            }
            self.emitted_value = true;
            return folded;
        }
        // A pending separator is trailing whitespace and is discarded.
        self.pending_space = false;
        return null;
    }
};

/// Compare exact-match atoms with the release scoring contract. The admitted
/// Zig profile is a fail-closed subset of Python's NFC + whitespace collapse
/// + casefold behavior; every accepted input has the same canonical value.
pub fn canonicalScoringValueEqual(lhs: []const u8, rhs: []const u8) !bool {
    var lhs_iter = try CanonicalScoringIterator.init(lhs);
    var rhs_iter = try CanonicalScoringIterator.init(rhs);
    var equal = true;
    while (true) {
        const lhs_cp = try lhs_iter.next();
        const rhs_cp = try rhs_iter.next();
        if (lhs_cp != rhs_cp) equal = false;
        if (lhs_cp == null and rhs_cp == null) return equal;
    }
}

fn rejectInvalidChoiceIdentifiers(value: std.json.Value) !void {
    if (value != .object) return;
    if (value.object.get("value")) |selected| switch (selected) {
        .string => |label| if (invalidSchemaIdentifier(label)) return error.InvalidChoiceSchema,
        .array => |labels| for (labels.items) |label| {
            if (label == .string and invalidSchemaIdentifier(label.string)) return error.InvalidChoiceSchema;
        },
        else => {},
    };
    const choices = value.object.get("choices") orelse return;
    if (choices != .array) return;
    for (choices.array.items) |choice| {
        if (choice == .string and invalidSchemaIdentifier(choice.string)) return error.InvalidChoiceSchema;
    }
}

fn appendMentionValueAsEntities(
    allocator: std.mem.Allocator,
    text: []const u8,
    label: []const u8,
    value: std.json.Value,
    entities: *std.ArrayListUnmanaged(Entity),
) !void {
    switch (value) {
        .string => try appendMentionAsEntity(allocator, text, label, value.string, entities),
        .array => for (value.array.items) |item| try appendMentionValueAsEntities(allocator, text, label, item, entities),
        .object => {
            if (value.object.get("value")) |choice_value| {
                try appendMentionValueAsEntities(allocator, text, label, choice_value, entities);
            }
        },
        else => {},
    }
}

const ChoiceFieldTarget = struct {
    value: []const u8,
    word_start: usize,
    word_end: usize,
};

fn appendJsonChoicePrefixTokens(
    allocator: std.mem.Allocator,
    parent: []const u8,
    fields_obj: std.json.ObjectMap,
    prefix_tokens: *std.ArrayListUnmanaged([]const u8),
    targets: *std.ArrayListUnmanaged(ChoiceFieldTarget),
) !void {
    var iter = fields_obj.iterator();
    var started = false;
    while (iter.next()) |entry| {
        const value = entry.value_ptr.*;
        if (value != .object) continue;
        const value_obj = value.object;
        const choice_value_raw = value_obj.get("value");
        const choices_value_raw = value_obj.get("choices");
        if (choice_value_raw == null and choices_value_raw == null) continue;
        if (choice_value_raw == null or choices_value_raw == null) return error.InvalidChoiceSchema;
        const choices_raw = choices_value_raw.?;
        if (choices_raw != .array or choices_raw.array.items.len == 0) return error.InvalidChoiceSchema;
        switch (choice_value_raw.?) {
            .string => |selected| {
                if (invalidSchemaIdentifier(selected)) return error.InvalidChoiceSchema;
                if (!try jsonArrayContainsString(choices_raw.array.items, selected)) return error.ChoiceValueNotInChoices;
            },
            .array => |items| for (items.items) |item| {
                if (item != .string) return error.InvalidChoiceSchema;
                if (invalidSchemaIdentifier(item.string)) return error.InvalidChoiceSchema;
                if (!try jsonArrayContainsString(choices_raw.array.items, item.string)) return error.ChoiceValueNotInChoices;
            },
            else => return error.InvalidChoiceSchema,
        }
        // Blank prefix tokens encode to zero words and would desync every
        // downstream span/surface word index, so fail closed on that schema.
        if (std.mem.indexOfNone(u8, parent, " \t\r\n") == null or
            std.mem.indexOfNone(u8, entry.key_ptr.*, " \t\r\n") == null) return error.InvalidChoiceSchema;
        if (!started) {
            try prefix_tokens.append(allocator, "(");
            try prefix_tokens.append(allocator, try std.fmt.allocPrint(allocator, "{s}:", .{parent}));
            started = true;
        } else {
            try prefix_tokens.append(allocator, ",");
        }
        try prefix_tokens.append(allocator, entry.key_ptr.*);
        try prefix_tokens.append(allocator, "(");
        var emitted_choice = false;
        for (choices_raw.array.items, 0..) |choice_item, choice_idx| {
            const choice = try jsonString(choice_item);
            // Blank choices likewise encode to zero words and cannot preserve
            // the upstream prefix-to-word mapping.
            if (invalidSchemaIdentifier(choice)) return error.InvalidChoiceSchema;
            for (choices_raw.array.items[0..choice_idx]) |previous| {
                if (std.mem.eql(u8, try jsonString(previous), choice)) return error.InvalidChoiceSchema;
            }
            if (emitted_choice) try prefix_tokens.append(allocator, "|");
            const word_idx = prefix_tokens.items.len;
            try prefix_tokens.append(allocator, choice);
            try targets.append(allocator, .{
                .value = choice,
                .word_start = word_idx,
                .word_end = word_idx,
            });
            emitted_choice = true;
        }
        try prefix_tokens.append(allocator, ")");
    }
    if (started) try prefix_tokens.append(allocator, ")");
}

fn jsonArrayContainsString(items: []const std.json.Value, needle: []const u8) !bool {
    for (items) |item| {
        if (std.mem.eql(u8, try jsonString(item), needle)) return true;
    }
    return false;
}

fn isChoiceFieldValue(value: std.json.Value) bool {
    return value == .object and value.object.get("value") != null and value.object.get("choices") != null;
}

/// Group `json_structures` / `relations` instances by their structure NAME into
/// one UpstreamTask per name: count = number of instances, gold field values
/// aggregated across all instances (the schema prompt dedups the field names).
/// Mirrors upstream GLiNER2, which emits one schema per distinct structure name.
fn sliceContainsString(items: []const []const u8, needle: []const u8) bool {
    for (items) |item| {
        if (std.mem.eql(u8, item, needle)) return true;
    }
    return false;
}

fn appendGroupedStructureTasks(
    allocator: std.mem.Allocator,
    text: []const u8,
    array_value: std.json.Value,
    kind: UpstreamTaskKind,
    descriptions: ?std.json.ObjectMap,
    prefix_tokens: *std.ArrayListUnmanaged([]const u8),
    tasks: *std.ArrayListUnmanaged(UpstreamTask),
) !void {
    var choice_targets = std.ArrayListUnmanaged(ChoiceFieldTarget).empty;
    defer choice_targets.deinit(allocator);
    var group_names = std.ArrayListUnmanaged([]const u8).empty;
    defer group_names.deinit(allocator);
    var group_occurrences = std.ArrayListUnmanaged(std.ArrayListUnmanaged(std.json.ObjectMap)).empty;
    defer {
        for (group_occurrences.items) |*occurrences| occurrences.deinit(allocator);
        group_occurrences.deinit(allocator);
    }

    for (array_value.array.items) |instance_value| {
        if (instance_value != .object) return error.InvalidGliner2Example;
        var inst_iter = instance_value.object.iterator();
        while (inst_iter.next()) |inst_entry| {
            if (inst_entry.value_ptr.* != .object) return error.InvalidStructureOccurrence;
            const name = inst_entry.key_ptr.*;
            if (std.mem.trim(u8, name, " \t\r\n").len == 0) return error.InvalidStructureSchema;
            if (kind == .json_structures) {
                // Upstream builds the classification-choice prefix from every
                // raw occurrence before grouping, deduplication, or empty-row
                // collapse. Preserve that original order and keep every choice
                // token position: selected values match all equal prefix tokens.
                try appendJsonChoicePrefixTokens(allocator, name, inst_entry.value_ptr.object, prefix_tokens, &choice_targets);
            }

            var gi: usize = group_names.items.len;
            for (group_names.items, 0..) |gn, idx| {
                if (std.mem.eql(u8, gn, name)) {
                    gi = idx;
                    break;
                }
            }
            if (gi == group_names.items.len) {
                try group_names.append(allocator, name);
                try group_occurrences.append(allocator, .empty);
            }
            try group_occurrences.items[gi].append(allocator, inst_entry.value_ptr.object);
        }
    }

    for (group_names.items, 0..) |name, idx| {
        const occurrences = group_occurrences.items[idx].items;
        if (occurrences.len == 0) continue;

        var schema_fields = std.ArrayListUnmanaged([]const u8).empty;
        defer schema_fields.deinit(allocator);
        if (kind == .relations) {
            var first_fields = occurrences[0].iterator();
            while (first_fields.next()) |entry| {
                if (std.mem.trim(u8, entry.key_ptr.*, " \t\r\n").len == 0) return error.InvalidStructureSchema;
                try schema_fields.append(allocator, entry.key_ptr.*);
            }
        } else {
            for (occurrences) |occurrence| {
                var field_iter = occurrence.iterator();
                while (field_iter.next()) |entry| {
                    if (std.mem.trim(u8, entry.key_ptr.*, " \t\r\n").len == 0) return error.InvalidStructureSchema;
                    if (!sliceContainsString(schema_fields.items, entry.key_ptr.*)) {
                        try schema_fields.append(allocator, entry.key_ptr.*);
                    }
                }
            }
        }
        // Upstream skips schemas whose sampled field set is empty.
        if (schema_fields.items.len == 0) continue;

        var choice_fields = std.ArrayListUnmanaged([]const u8).empty;
        defer choice_fields.deinit(allocator);
        if (kind == .json_structures) {
            for (occurrences) |occurrence| {
                var field_iter = occurrence.iterator();
                while (field_iter.next()) |entry| {
                    if (isChoiceFieldValue(entry.value_ptr.*) and !sliceContainsString(choice_fields.items, entry.key_ptr.*)) {
                        try choice_fields.append(allocator, entry.key_ptr.*);
                    }
                }
            }
        }

        var unique_occurrences = std.ArrayListUnmanaged(usize).empty;
        defer unique_occurrences.deinit(allocator);
        var seen_rows = std.StringHashMapUnmanaged(void){};
        defer seen_rows.deinit(allocator);
        var owned_row_keys = std.ArrayListUnmanaged([]const u8).empty;
        defer {
            for (owned_row_keys.items) |key| allocator.free(key);
            owned_row_keys.deinit(allocator);
        }
        var any_json_value = false;
        for (occurrences, 0..) |occurrence, occurrence_idx| {
            if (kind == .relations and !structureOccurrenceHasAllFields(occurrence, schema_fields.items)) continue;
            const row_key = try structureOccurrenceKey(allocator, occurrence, schema_fields.items);
            if (seen_rows.contains(row_key)) {
                allocator.free(row_key);
                continue;
            }
            try seen_rows.put(allocator, row_key, {});
            try owned_row_keys.append(allocator, row_key);
            try unique_occurrences.append(allocator, occurrence_idx);
            if (kind == .json_structures and structureOccurrenceHasAnyValue(occurrence, schema_fields.items)) {
                any_json_value = true;
            }
        }
        if (kind == .relations and unique_occurrences.items.len == 0) continue;
        if (kind == .json_structures and !any_json_value) unique_occurrences.clearRetainingCapacity();

        var fields = std.ArrayListUnmanaged(UpstreamField).empty;
        errdefer fields.deinit(allocator);
        for (unique_occurrences.items, 0..) |occurrence_idx, instance_idx| {
            const occurrence = occurrences[occurrence_idx];
            for (schema_fields.items) |field_name| {
                const value = occurrence.get(field_name) orelse continue;
                const fields_before = fields.items.len;
                try appendTaskFieldsFromValue(
                    allocator,
                    text,
                    field_name,
                    value,
                    if (kind == .json_structures and sliceContainsString(choice_fields.items, field_name)) choice_targets.items else null,
                    &fields,
                );
                for (fields.items[fields_before..]) |*field| field.instance = instance_idx;
            }
        }

        const owned_schema_fields = try schema_fields.toOwnedSlice(allocator);
        try tasks.append(allocator, .{
            .kind = kind,
            .name = name,
            .schema_fields = owned_schema_fields,
            .label_descriptions = try parseLabelDescriptions(
                allocator,
                if (descriptions) |description_map| description_map.get(name) else null,
                owned_schema_fields,
            ),
            .fields = try fields.toOwnedSlice(allocator),
            .count = unique_occurrences.items.len,
        });
    }
}

fn structureOccurrenceHasAllFields(occurrence: std.json.ObjectMap, schema_fields: []const []const u8) bool {
    for (schema_fields) |field| {
        if (!occurrence.contains(field)) return false;
    }
    return true;
}

fn normalizedStructureCell(value: std.json.Value) std.json.Value {
    if (value == .object) {
        if (value.object.get("value")) |nested| return nested;
    }
    return value;
}

fn structureOccurrenceHasAnyValue(occurrence: std.json.ObjectMap, schema_fields: []const []const u8) bool {
    for (schema_fields) |field| {
        const raw = occurrence.get(field) orelse continue;
        const value = normalizedStructureCell(raw);
        if (value == .null) continue;
        if (value == .string and value.string.len == 0) continue;
        return true;
    }
    return false;
}

fn structureOccurrenceKey(
    allocator: std.mem.Allocator,
    occurrence: std.json.ObjectMap,
    schema_fields: []const []const u8,
) ![]const u8 {
    var buffer: std.Io.Writer.Allocating = .init(allocator);
    errdefer buffer.deinit();
    try buffer.writer.writeByte('[');
    for (schema_fields, 0..) |field, idx| {
        if (idx > 0) try buffer.writer.writeByte(',');
        if (occurrence.get(field)) |raw| {
            try std.json.Stringify.value(normalizedStructureCell(raw), .{}, &buffer.writer);
        } else {
            try buffer.writer.writeAll("null");
        }
    }
    try buffer.writer.writeByte(']');
    return buffer.toOwnedSlice();
}

fn appendTaskFieldsFromValue(
    allocator: std.mem.Allocator,
    text: []const u8,
    field_name: []const u8,
    value: std.json.Value,
    choice_targets: ?[]const ChoiceFieldTarget,
    fields: *std.ArrayListUnmanaged(UpstreamField),
) !void {
    switch (value) {
        .string => try appendTaskFieldMention(allocator, text, field_name, value.string, choice_targets, fields),
        .array => for (value.array.items) |item| try appendTaskFieldsFromValue(allocator, text, field_name, item, choice_targets, fields),
        .object => {
            if (value.object.get("value")) |choice_value| {
                try appendTaskFieldsFromValue(allocator, text, field_name, choice_value, choice_targets, fields);
            } else return error.InvalidTaskFieldValue;
        },
        .null => {},
        else => return error.InvalidTaskFieldValue,
    }
}

fn appendTaskFieldMention(
    allocator: std.mem.Allocator,
    text: []const u8,
    field_name: []const u8,
    mention: []const u8,
    choice_targets: ?[]const ChoiceFieldTarget,
    fields: *std.ArrayListUnmanaged(UpstreamField),
) !void {
    try validateUpstreamLowerableText(text);
    try validateUpstreamLowerableText(mention);
    if (!hasUpstreamNonWhitespace(mention)) return;
    if (choice_targets) |targets| {
        for (targets) |target| {
            try validateUpstreamLowerableText(target.value);
            if (!upstreamLowerSliceEql(target.value, 0, target.value.len, mention, 0, mention.len)) continue;
            try fields.append(allocator, .{
                .name = field_name,
                .value = mention,
                .target_word_start = target.word_start,
                .target_word_end = target.word_end,
            });
        }
        return;
    }

    var search_start: usize = 0;
    var found = false;
    while (true) {
        while (search_start < text.len and isUpstreamWhitespaceAt(text, search_start)) search_start = nextCodepointEnd(text, search_start);
        if (search_start >= text.len) break;
        const candidate_start = search_start;

        var text_pos = candidate_start;
        var mention_pos: usize = 0;
        var match_end = candidate_start;
        var matched = true;
        while (true) {
            while (mention_pos < mention.len and isUpstreamWhitespaceAt(mention, mention_pos)) mention_pos = nextCodepointEnd(mention, mention_pos);
            if (mention_pos >= mention.len) break;
            while (text_pos < text.len and isUpstreamWhitespaceAt(text, text_pos)) text_pos = nextCodepointEnd(text, text_pos);
            if (text_pos >= text.len) {
                matched = false;
                break;
            }
            const mention_end = nextUpstreamTextTokenEnd(mention, mention_pos);
            const text_end = nextUpstreamTextTokenEnd(text, text_pos);
            if (!upstreamLowerSliceEql(mention, mention_pos, mention_end, text, text_pos, text_end)) {
                matched = false;
                break;
            }
            mention_pos = mention_end;
            text_pos = text_end;
            match_end = text_end;
        }
        if (matched and match_end > candidate_start) {
            try fields.append(allocator, .{
                .name = field_name,
                .value = text[candidate_start..match_end],
                .start = candidate_start,
                .end = match_end,
            });
            found = true;
        }
        search_start = nextUpstreamTextTokenEnd(text, candidate_start);
    }
    if (!found) return error.AnnotationNotFound;
}

fn appendMentionAsEntity(
    allocator: std.mem.Allocator,
    text: []const u8,
    label: []const u8,
    mention: []const u8,
    entities: *std.ArrayListUnmanaged(Entity),
) !void {
    try validateUpstreamLowerableText(text);
    try validateUpstreamLowerableText(mention);
    if (!hasUpstreamNonWhitespace(mention)) return;
    var search_start: usize = 0;
    var found = false;
    while (indexOfUpstreamLowerPos(text, mention, search_start)) |match| {
        try entities.append(allocator, .{
            .text = text[match.start..match.end],
            .label = label,
            .start = match.start,
            .end = match.end,
        });
        found = true;
        search_start = nextCodepointEnd(text, match.start);
    }
    if (!found) return error.AnnotationNotFound;
}

fn jsonString(value: std.json.Value) ![]const u8 {
    return switch (value) {
        .string => value.string,
        else => error.InvalidGliner2Example,
    };
}

fn jsonStringArray(allocator: std.mem.Allocator, value: std.json.Value) ![][]const u8 {
    if (value != .array) return error.InvalidGliner2Example;
    var out = std.ArrayListUnmanaged([]const u8).empty;
    errdefer out.deinit(allocator);
    for (value.array.items) |item| try out.append(allocator, try jsonString(item));
    return out.toOwnedSlice(allocator);
}

fn jsonUsize(value: std.json.Value) !usize {
    return switch (value) {
        .integer => |v| if (v >= 0) @as(usize, @intCast(v)) else error.InvalidGliner2Example,
        else => error.InvalidGliner2Example,
    };
}

const ByteMatch = struct { start: usize, end: usize };

fn indexOfUpstreamLowerPos(haystack: []const u8, needle: []const u8, start: usize) ?ByteMatch {
    if (needle.len == 0) return null;
    var i = start;
    while (i < haystack.len) : (i = nextCodepointEnd(haystack, i)) {
        var haystack_pos = i;
        var needle_pos: usize = 0;
        while (needle_pos < needle.len and haystack_pos < haystack.len) {
            if (lowerCodepointAt(haystack, haystack_pos) != lowerCodepointAt(needle, needle_pos)) break;
            haystack_pos = nextCodepointEnd(haystack, haystack_pos);
            needle_pos = nextCodepointEnd(needle, needle_pos);
        }
        if (needle_pos == needle.len) return .{ .start = i, .end = haystack_pos };
    }
    return null;
}

fn upstreamLowerSliceEql(a: []const u8, a_start: usize, a_end: usize, b: []const u8, b_start: usize, b_end: usize) bool {
    var a_pos = a_start;
    var b_pos = b_start;
    while (a_pos < a_end and b_pos < b_end) {
        if (lowerCodepointAt(a, a_pos) != lowerCodepointAt(b, b_pos)) return false;
        a_pos = nextCodepointEnd(a, a_pos);
        b_pos = nextCodepointEnd(b, b_pos);
    }
    return a_pos == a_end and b_pos == b_end;
}

fn lowerCodepointAt(text: []const u8, start: usize) u21 {
    const cp = decodeCodepointAt(text, start);
    return if (cp == 0x03a3 and isFinalSigma(text, start)) 0x03c2 else upstream_unicode.simpleLower(cp);
}

fn hasUpstreamNonWhitespace(text: []const u8) bool {
    var pos: usize = 0;
    while (pos < text.len) : (pos = nextCodepointEnd(text, pos)) {
        if (!isUpstreamWhitespaceAt(text, pos)) return true;
    }
    return false;
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

fn fillUpstreamWordSurfaceFeatures(
    allocator: std.mem.Allocator,
    record: UpstreamRecord,
    num_words: usize,
    word_lengths: []f32,
    word_has_digit: []f32,
    word_is_title: []f32,
    word_is_all_caps: []f32,
) !void {
    var idx: usize = 0;
    for (record.prefix_tokens) |token| {
        if (idx >= num_words or idx >= word_lengths.len) return;
        fillOneWordSurfaceFeature(token, idx, word_lengths, word_has_digit, word_is_title, word_is_all_caps);
        idx += 1;
    }
    var appended_text: ?[]u8 = null;
    defer if (appended_text) |text| allocator.free(text);
    if (upstreamTextNeedsTerminalPeriod(record.text)) {
        appended_text = try std.mem.concat(allocator, u8, &.{ record.text, "." });
    }
    const effective_text: []const u8 = appended_text orelse record.text;
    var text_idx: usize = 0;
    while (text_idx < effective_text.len) {
        while (text_idx < effective_text.len and isUpstreamWhitespaceAt(effective_text, text_idx)) : (text_idx = nextCodepointEnd(effective_text, text_idx)) {}
        if (text_idx >= effective_text.len) break;
        if (idx >= num_words or idx >= word_lengths.len) break;
        const end = nextUpstreamTextTokenEnd(effective_text, text_idx);
        fillOneWordSurfaceFeature(effective_text[text_idx..end], idx, word_lengths, word_has_digit, word_is_title, word_is_all_caps);
        text_idx = end;
        idx += 1;
    }
}

fn fillOneWordSurfaceFeature(
    word: []const u8,
    idx: usize,
    word_lengths: []f32,
    word_has_digit: []f32,
    word_is_title: []f32,
    word_is_all_caps: []f32,
) void {
    word_lengths[idx] = @as(f32, @floatFromInt(@min(word.len, 32))) / 32.0;
    word_has_digit[idx] = if (containsDigit(word)) 1.0 else 0.0;
    word_is_title[idx] = if (isTitleCaseWord(word)) 1.0 else 0.0;
    word_is_all_caps[idx] = if (isAllCapsWord(word)) 1.0 else 0.0;
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
    // Legacy path: whitespace word-index space, consistent with encodeInto /
    // first_token_positions / getWordBoundaries (NOT the punctuation-aware
    // buildCharToWordMap used by the upstream span grid).
    const char_to_word = try buildCharToWordMapWhitespace(allocator, ex.text);
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

fn fillUpstreamSpanGrid(
    allocator: std.mem.Allocator,
    record: UpstreamRecord,
    entity_types: []const []const u8,
    num_entity_slots: usize,
    num_words: usize,
    max_words: usize,
    max_span_width: usize,
    span_indices: []i32,
    span_mask: []f32,
    span_labels: []f32,
    entity_type_kind: []i32,
) !void {
    const num_entity_types = num_entity_slots;
    const max_spans = max_words * max_span_width;
    const char_to_word = try buildCharToWordMap(allocator, record.text);
    defer allocator.free(char_to_word);
    const prefix_word_count = record.prefix_tokens.len;

    for (0..max_words) |start_word| {
        for (0..max_span_width) |w| {
            const span_idx = start_word * max_span_width + w;
            if (span_idx >= max_spans) continue;
            const end_word = start_word + w;
            if (start_word < num_words and end_word < num_words) {
                span_indices[span_idx * 2] = @intCast(start_word);
                span_indices[span_idx * 2 + 1] = @intCast(end_word);
                span_mask[span_idx] = 1.0;
            }
        }
    }

    for (record.tasks) |task| {
        if (task.kind == .classifications) continue;
        const count_state: i32 = @intCast(@min(task.count, @as(usize, 19)) + 1);
        if (task.schema_fields.len > 0) {
            for (task.schema_fields) |schema_field| {
                const label = try upstreamTaskFieldKey(allocator, task.kind, task.name, schema_field);
                defer allocator.free(label);
                if (indexOfLabel(entity_types, label)) |idx| entity_type_kind[idx] = count_state;
            }
        }
        for (task.fields) |field| {
            const label_idx = if (task.kind == .entities)
                indexOfLabel(entity_types, field.name)
            else blk: {
                const label = try upstreamTaskFieldKey(allocator, task.kind, task.name, field.name);
                defer allocator.free(label);
                break :blk indexOfLabel(entity_types, label);
            };
            if (label_idx) |idx| {
                if (task.schema_fields.len == 0) entity_type_kind[idx] = count_state;
            }
            const span_idx = locateUpstreamFieldSpanIdx(field, char_to_word, prefix_word_count, max_words, max_span_width) orelse return error.AnnotationOutsideBatch;
            if (label_idx) |idx| {
                span_labels[span_idx * num_entity_types + idx] = 1.0;
            }
        }
    }
}

/// Map an upstream field's located mention to its flat span-grid index within
/// a sample (`start_word * max_span_width + (width - 1)`), or null if the field
/// has no locatable / in-bounds span. Shared by `fillUpstreamSpanGrid` and the
/// per-instance structure-loss target builder so both agree on the mapping.
pub fn locateUpstreamFieldSpanIdx(
    field: UpstreamField,
    char_to_word: []const i32,
    prefix_word_count: usize,
    max_words: usize,
    max_span_width: usize,
) ?usize {
    const start_word, const end_word = if (field.target_word_start != null and field.target_word_end != null)
        .{ field.target_word_start.?, field.target_word_end.? }
    else blk: {
        const start = field.start orelse return null;
        const end_exclusive = field.end orelse return null;
        if (start >= char_to_word.len or end_exclusive == 0 or end_exclusive > char_to_word.len) return null;
        const end = end_exclusive - 1;
        if (end >= char_to_word.len) return null;
        const start_word_raw = char_to_word[start];
        const end_word_raw = char_to_word[end];
        if (start_word_raw < 0 or end_word_raw < start_word_raw) return null;
        break :blk .{
            prefix_word_count + @as(usize, @intCast(start_word_raw)),
            prefix_word_count + @as(usize, @intCast(end_word_raw)),
        };
    };
    if (start_word >= max_words or end_word >= max_words) return null;
    const width = end_word - start_word + 1;
    if (width == 0 or width > max_span_width) return null;
    const span_idx = start_word * max_span_width + (width - 1);
    if (span_idx >= max_words * max_span_width) return null;
    return span_idx;
}

pub fn buildCharToWordMap(allocator: std.mem.Allocator, text: []const u8) ![]i32 {
    try validateUpstreamLowerableText(text);
    const map = try allocator.alloc(i32, text.len);
    @memset(map, -1);
    var text_idx: usize = 0;
    var word_idx: usize = 0;
    while (text_idx < text.len) : (word_idx += 1) {
        while (text_idx < text.len and isUpstreamWhitespaceAt(text, text_idx)) : (text_idx = nextCodepointEnd(text, text_idx)) {}
        if (text_idx >= text.len) break;
        const start = text_idx;
        const end = nextUpstreamTextTokenEnd(text, text_idx);
        for (start..@min(end, map.len)) |pos| map[pos] = @intCast(word_idx);
        text_idx = end;
    }
    return map;
}

/// Whitespace-only char->word map for the LEGACY (buildSimpleBatch) span path.
/// Its encode (`Tokenizer.encodeInto` -> `first_token_positions`), surface
/// features, and decode (`getWordBoundaries`) all split words on whitespace only.
/// `buildCharToWordMap` above splits on punctuation too (for the upstream path);
/// using it in the legacy path would give span start/end word indices in a
/// different word-index space than `first_token_positions`, silently shifting
/// gold labels onto the wrong words whenever punctuation is attached to a token.
fn buildCharToWordMapWhitespace(allocator: std.mem.Allocator, text: []const u8) ![]i32 {
    const map = try allocator.alloc(i32, text.len);
    @memset(map, -1);
    var text_idx: usize = 0;
    var word_idx: usize = 0;
    while (text_idx < text.len) : (word_idx += 1) {
        while (text_idx < text.len and std.ascii.isWhitespace(text[text_idx])) : (text_idx += 1) {}
        if (text_idx >= text.len) break;
        const start = text_idx;
        while (text_idx < text.len and !std.ascii.isWhitespace(text[text_idx])) : (text_idx += 1) {}
        for (start..@min(text_idx, map.len)) |pos| map[pos] = @intCast(word_idx);
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

fn getUpstreamWordBoundaries(allocator: std.mem.Allocator, text: []const u8) ![][2]usize {
    try validateUpstreamLowerableText(text);
    var out: std.ArrayListUnmanaged([2]usize) = .empty;
    defer out.deinit(allocator);
    var text_idx: usize = 0;
    while (text_idx < text.len) {
        while (text_idx < text.len and isUpstreamWhitespaceAt(text, text_idx)) : (text_idx = nextCodepointEnd(text, text_idx)) {}
        if (text_idx >= text.len) break;
        const end = nextUpstreamTextTokenEnd(text, text_idx);
        try out.append(allocator, .{ text_idx, end });
        text_idx = end;
    }
    return try out.toOwnedSlice(allocator);
}

/// Character boundaries for the same word splitter used by upstream task
/// encoding. Exposed for native full-task result decoding so prefix words can
/// be excluded without reimplementing tokenization rules in the evaluator.
pub fn upstreamWordBoundariesAlloc(allocator: std.mem.Allocator, text: []const u8) ![][2]usize {
    return getUpstreamWordBoundaries(allocator, text);
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

test "GLiNER2 HF prompt encoding propagates entity-label normalizer failures" {
    const allocator = std.testing.allocator;
    var tokenizer = Tokenizer{
        .use_gliner2_hf_prompt = true,
        .hf_normalizer_contract = .pinned_fastino_unicode15,
    };
    defer tokenizer.deinit(allocator);

    var input_ids: [64]i32 = undefined;
    var attention_mask: [64]i32 = undefined;
    var words_mask: [64]i32 = undefined;
    var first_token_positions: [16]i32 = undefined;
    var e_token_positions: [1]i32 = undefined;
    var e_token_end_positions: [1]i32 = undefined;
    const entity_types = [_][]const u8{"cafe\u{0301}"};

    try std.testing.expectError(
        error.UnsupportedTokenizerNormalization,
        tokenizer.encodeInto(
            allocator,
            "plain text",
            &entity_types,
            &input_ids,
            &attention_mask,
            &words_mask,
            &first_token_positions,
            &e_token_positions,
            &e_token_end_positions,
        ),
    );
}

test "GLiNER2 HF prompt encoding propagates text normalizer failures" {
    const allocator = std.testing.allocator;
    var tokenizer = Tokenizer{
        .use_gliner2_hf_prompt = true,
        .hf_normalizer_contract = .pinned_fastino_unicode15,
    };
    defer tokenizer.deinit(allocator);

    var input_ids: [64]i32 = undefined;
    var attention_mask: [64]i32 = undefined;
    var words_mask: [64]i32 = undefined;
    var first_token_positions: [16]i32 = undefined;
    var e_token_positions: [1]i32 = undefined;
    var e_token_end_positions: [1]i32 = undefined;
    const entity_types = [_][]const u8{"person"};

    try std.testing.expectError(
        error.UnsupportedTokenizerNormalization,
        tokenizer.encodeInto(
            allocator,
            "cafe\u{0301} remains visible",
            &entity_types,
            &input_ids,
            &attention_mask,
            &words_mask,
            &first_token_positions,
            &e_token_positions,
            &e_token_end_positions,
        ),
    );
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
