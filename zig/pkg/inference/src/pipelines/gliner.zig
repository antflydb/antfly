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

// GLiNER2 zero-shot NER pipeline.
//
// GLiNER models take user-specified entity labels at inference time and
// detect entity spans using a span-classification approach:
//
//   1. Build schema prefix: ( [P] entities ( [E] label1 [E] label2 ... ) ) [SEP_TEXT]
//   2. Tokenize text words individually, tracking word→sub-token mapping
//   3. Construct input tensors: input_ids, attention_mask, words_mask, span_idx
//   4. Run ONNX inference → [1, num_spans, num_labels] logits
//   5. Apply sigmoid, threshold, and flat NER deduplication
//
// Matches Go termite's lib/pipelines/gliner.go (GLiNER2 path).

const std = @import("std");
const backends = @import("../backends/backends.zig");
const Tokenizer = @import("termite_tokenizer").Tokenizer;
const Tensor = backends.Tensor;
const runtime = @import("../runtime/root.zig");

pub const Entity = @import("ner.zig").Entity;

pub const GlinerConfig = struct {
    max_width: u32 = 12,
    max_length: u32 = 512,
    threshold: f32 = 0.5,
    flat_ner: bool = true,
    default_labels: []const []const u8 = &.{},
    relation_labels: []const []const u8 = &.{},
    relation_threshold: f32 = 0.0,
    model_type: []const u8 = "",
    capabilities: []const []const u8 = &.{},
    // Special token IDs parsed from added_tokens.json
    token_p: i32 = 0, // [P]
    token_c: i32 = 0, // [C]
    token_e: i32 = 0, // [E]
    token_r: i32 = 0, // [R]
    token_sep_text: i32 = 0, // [SEP_TEXT]
    distributed: runtime.distributed.Config = .{},
};

pub const ClassificationConfig = struct {
    threshold: f32 = 0.0,
    multi_label: bool = false,
    top_k: usize = 0, // 0 follows Go parity: top-1 for single-label, all for multi-label
};

pub const ClassificationResult = struct {
    label: []const u8,
    score: f32,
};

pub const Relation = struct {
    head: Entity,
    tail: Entity,
    label: []const u8,
    score: f32,
    owned_head_label: ?[]const u8 = null,

    pub fn deinit(self: *Relation, allocator: std.mem.Allocator) void {
        allocator.free(self.head.text);
        allocator.free(self.tail.text);
        allocator.free(self.label);
        if (self.owned_head_label) |head_label| allocator.free(head_label);
    }
};

pub const GlinerPipeline = struct {
    allocator: std.mem.Allocator,
    session: backends.Session,
    tok: Tokenizer,
    config: GlinerConfig,

    pub fn usesDistributedMlx(self: *const GlinerPipeline) bool {
        return self.config.distributed.enabled and
            self.config.distributed.world_size > 1 and
            self.session.backend().usesGpuHostedSession();
    }

    pub fn usesTensorParallelMlx(self: *const GlinerPipeline) bool {
        return self.usesDistributedMlx() and self.config.distributed.mode == .tensor_parallel;
    }

    /// Recognize entities in a batch of texts using provided or default labels.
    pub fn recognizeBatch(
        self: *GlinerPipeline,
        texts: []const []const u8,
        labels: ?[]const []const u8,
    ) ![][]Entity {
        const use_labels = labels orelse self.config.default_labels;
        if (use_labels.len == 0) return error.NoLabelsProvided;
        return self.recognizeWithLabelTokenBatch(texts, use_labels, self.config.token_e, self.config.threshold, self.config.flat_ner);
    }

    pub fn extractRelationsBatch(
        self: *GlinerPipeline,
        texts: []const []const u8,
        entity_labels: ?[]const []const u8,
        relation_labels: ?[]const []const u8,
    ) !struct { entities: [][]Entity, relations: [][]Relation } {
        const alloc = self.allocator;
        if (!self.supportsRelationExtraction()) return error.RelationExtractionNotSupported;

        const use_entity_labels = entity_labels orelse self.config.default_labels;
        if (use_entity_labels.len == 0) return error.NoLabelsProvided;

        const all_entities = try self.recognizeWithLabelTokenBatch(texts, use_entity_labels, self.config.token_e, self.config.threshold, self.config.flat_ner);
        errdefer {
            for (all_entities) |entities| {
                for (entities) |entity| alloc.free(entity.text);
                alloc.free(entities);
            }
            alloc.free(all_entities);
        }

        const all_relations = try alloc.alloc([]Relation, texts.len);
        var initialized_relations: usize = 0;
        errdefer {
            for (all_relations[0..initialized_relations]) |relations| {
                for (relations) |*relation| relation.deinit(alloc);
                alloc.free(relations);
            }
            alloc.free(all_relations);
        }

        const use_relation_labels = relation_labels orelse self.config.relation_labels;
        if (use_relation_labels.len == 0) {
            for (all_relations) |*relations| {
                relations.* = try alloc.alloc(Relation, 0);
                initialized_relations += 1;
            }
            return .{ .entities = all_entities, .relations = all_relations };
        }

        var composite_labels = std.ArrayListUnmanaged([]const u8).empty;
        defer {
            for (composite_labels.items) |label| alloc.free(label);
            composite_labels.deinit(alloc);
        }
        try appendRelationCandidateLabels(alloc, &composite_labels, use_entity_labels, use_relation_labels);
        if (composite_labels.items.len == 0) {
            for (all_relations) |*relations| {
                relations.* = try alloc.alloc(Relation, 0);
                initialized_relations += 1;
            }
            return .{ .entities = all_entities, .relations = all_relations };
        }

        var rows_with_entities: usize = 0;
        for (all_entities) |entities| {
            if (entities.len >= 2) rows_with_entities += 1;
        }
        if (rows_with_entities == 0) {
            for (all_relations) |*relations| {
                relations.* = try alloc.alloc(Relation, 0);
                initialized_relations += 1;
            }
            return .{ .entities = all_entities, .relations = all_relations };
        }

        const relation_token = if (self.config.token_r != 0) self.config.token_r else self.config.token_e;
        const relation_threshold = if (self.config.relation_threshold > 0) self.config.relation_threshold else self.config.threshold;
        const active_texts = try alloc.alloc([]const u8, rows_with_entities);
        defer alloc.free(active_texts);
        const active_indices = try alloc.alloc(usize, rows_with_entities);
        defer alloc.free(active_indices);
        var active_len: usize = 0;
        for (all_entities, 0..) |entities, i| {
            if (entities.len < 2) continue;
            active_texts[active_len] = texts[i];
            active_indices[active_len] = i;
            active_len += 1;
        }

        const relation_heads = try self.recognizeWithLabelTokenBatch(active_texts[0..active_len], composite_labels.items, relation_token, relation_threshold, self.config.flat_ner);
        defer {
            for (relation_heads) |heads| {
                for (heads) |head| alloc.free(head.text);
                alloc.free(heads);
            }
            alloc.free(relation_heads);
        }

        for (all_entities, 0..) |entities, i| {
            if (entities.len >= 2) continue;
            all_relations[i] = try alloc.alloc(Relation, 0);
            initialized_relations += 1;
        }
        for (active_indices[0..active_len], 0..) |row_index, active_index| {
            all_relations[row_index] = try self.matchRelations(all_entities[row_index], relation_heads[active_index]);
            initialized_relations += 1;
        }

        return .{
            .entities = all_entities,
            .relations = all_relations,
        };
    }

    /// Classify texts against candidate labels using GLiNER2 span logits.
    pub fn classifyBatch(
        self: *GlinerPipeline,
        texts: []const []const u8,
        labels: []const []const u8,
        config: ClassificationConfig,
    ) ![][]ClassificationResult {
        const alloc = self.allocator;
        if (labels.len == 0) return error.NoLabelsProvided;

        const results = try alloc.alloc([]ClassificationResult, texts.len);
        var initialized: usize = 0;
        errdefer {
            for (results[0..initialized]) |r| alloc.free(r);
            alloc.free(results);
        }

        for (texts, 0..) |text, i| {
            results[i] = try self.classifySingle(text, labels, config);
            initialized += 1;
        }

        return results;
    }

    /// Recognize entities in a single text.
    fn recognize(self: *GlinerPipeline, text: []const u8, labels: []const []const u8) ![]Entity {
        return self.recognizeWithLabelToken(text, labels, self.config.token_e, self.config.threshold, self.config.flat_ner);
    }

    fn recognizeWithLabelToken(
        self: *GlinerPipeline,
        text: []const u8,
        labels: []const []const u8,
        label_token: i32,
        threshold: f32,
        flat_ner: bool,
    ) ![]Entity {
        const texts = [_][]const u8{text};
        const rows = try self.recognizeWithLabelTokenBatch(&texts, labels, label_token, threshold, flat_ner);
        defer self.allocator.free(rows);
        return rows[0];
    }

    const PreparedGlinerInput = struct {
        text: []const u8,
        input_ids: []i64,
        attention_mask: []i64,
        words_mask: []i64,
        span_idx: []i64,
        word_starts: []usize,
        word_ends: []usize,
        actual_num_words: usize,
        num_spans: usize,

        fn deinit(self: *PreparedGlinerInput, alloc: std.mem.Allocator) void {
            alloc.free(self.input_ids);
            alloc.free(self.attention_mask);
            alloc.free(self.words_mask);
            alloc.free(self.span_idx);
            alloc.free(self.word_starts);
            alloc.free(self.word_ends);
        }
    };

    fn recognizeWithLabelTokenBatch(
        self: *GlinerPipeline,
        texts: []const []const u8,
        labels: []const []const u8,
        label_token: i32,
        threshold: f32,
        flat_ner: bool,
    ) ![][]Entity {
        const alloc = self.allocator;
        const max_width: usize = self.config.max_width;

        if (self.config.token_p == 0 or label_token == 0 or self.config.token_sep_text == 0)
            return error.MissingSpecialTokenIds;

        const results = try alloc.alloc([]Entity, texts.len);
        @memset(results, &.{});
        var initialized_results: usize = 0;
        errdefer {
            for (results[0..initialized_results]) |entities| {
                for (entities) |entity| alloc.free(entity.text);
                alloc.free(entities);
            }
            alloc.free(results);
        }
        if (texts.len == 0) return results;

        var prepared = try alloc.alloc(PreparedGlinerInput, texts.len);
        var prepared_len: usize = 0;
        errdefer {
            for (prepared[0..prepared_len]) |*row| row.deinit(alloc);
            alloc.free(prepared);
        }

        var max_seq_len: usize = 0;
        var max_num_words: usize = 0;
        for (texts, 0..) |text, i| {
            prepared[i] = try self.prepareGlinerInput(text, labels, label_token);
            prepared_len += 1;
            max_seq_len = @max(max_seq_len, prepared[i].input_ids.len);
            max_num_words = @max(max_num_words, prepared[i].actual_num_words);
        }

        if (max_num_words == 0 or max_seq_len == 0) {
            for (results) |*row| {
                row.* = try alloc.alloc(Entity, 0);
                initialized_results += 1;
            }
            for (prepared[0..prepared_len]) |*row| row.deinit(alloc);
            alloc.free(prepared);
            return results;
        }

        const batch = texts.len;
        const batch_num_spans = max_num_words * max_width;
        const input_ids_buf = try alloc.alloc(i64, batch * max_seq_len);
        defer alloc.free(input_ids_buf);
        const attention_mask_buf = try alloc.alloc(i64, batch * max_seq_len);
        defer alloc.free(attention_mask_buf);
        const words_mask_buf = try alloc.alloc(i64, batch * max_seq_len);
        defer alloc.free(words_mask_buf);
        const span_idx_buf = try alloc.alloc(i64, batch * batch_num_spans * 2);
        defer alloc.free(span_idx_buf);
        @memset(input_ids_buf, 0);
        @memset(attention_mask_buf, 0);
        @memset(words_mask_buf, 0);
        @memset(span_idx_buf, 0);

        for (prepared[0..prepared_len], 0..) |row, b| {
            const input_off = b * max_seq_len;
            @memcpy(input_ids_buf[input_off..][0..row.input_ids.len], row.input_ids);
            @memcpy(attention_mask_buf[input_off..][0..row.attention_mask.len], row.attention_mask);
            @memcpy(words_mask_buf[input_off..][0..row.words_mask.len], row.words_mask);

            const span_off = b * batch_num_spans * 2;
            for (0..row.actual_num_words) |w| {
                for (0..max_width) |wi| {
                    const src = (w * max_width + wi) * 2;
                    const dst = span_off + (w * max_width + wi) * 2;
                    span_idx_buf[dst] = row.span_idx[src];
                    span_idx_buf[dst + 1] = row.span_idx[src + 1];
                }
            }
        }

        const shape_2d = [_]i64{ @intCast(batch), @intCast(max_seq_len) };
        var input_ids_tensor = try Tensor.initInt64(alloc, "input_ids", &shape_2d, input_ids_buf);
        defer input_ids_tensor.deinit();
        var attention_mask_tensor = try Tensor.initInt64(alloc, "attention_mask", &shape_2d, attention_mask_buf);
        defer attention_mask_tensor.deinit();
        var words_mask_tensor = try Tensor.initInt64(alloc, "words_mask", &shape_2d, words_mask_buf);
        defer words_mask_tensor.deinit();

        const span_shape = [_]i64{ @intCast(batch), @intCast(batch_num_spans), 2 };
        var span_idx_tensor = try Tensor.initInt64(alloc, "span_idx", &span_shape, span_idx_buf);
        defer span_idx_tensor.deinit();

        const outputs = try self.session.run(&.{
            input_ids_tensor,
            attention_mask_tensor,
            words_mask_tensor,
            span_idx_tensor,
        }, alloc);
        defer {
            for (outputs) |*o| o.deinit();
            alloc.free(outputs);
        }
        if (outputs.len == 0) return error.NoOutputTensors;

        const output = outputs[0];
        const logits = output.asFloat32();
        const output_shape = output.shape;

        var num_labels_dim: usize = labels.len;
        var output_num_words: usize = max_num_words;
        var output_max_width: usize = max_width;
        var row_stride: usize = batch_num_spans * num_labels_dim;
        if (output_shape.len >= 4) {
            output_num_words = @intCast(output_shape[1]);
            output_max_width = @intCast(output_shape[2]);
            num_labels_dim = @intCast(output_shape[3]);
            row_stride = output_num_words * output_max_width * num_labels_dim;
        } else if (output_shape.len == 3) {
            row_stride = @as(usize, @intCast(output_shape[1])) * @as(usize, @intCast(output_shape[2]));
            num_labels_dim = @intCast(output_shape[2]);
        }
        if (num_labels_dim > labels.len) num_labels_dim = labels.len;

        for (prepared[0..prepared_len], 0..) |row, i| {
            const row_start = @min(i * row_stride, logits.len);
            const row_end = @min(row_start + row_stride, logits.len);
            results[i] = try self.decodeEntitiesFromLogits(
                row,
                labels,
                logits[row_start..row_end],
                output_num_words,
                output_max_width,
                num_labels_dim,
                threshold,
                flat_ner,
            );
            initialized_results += 1;
        }

        for (prepared[0..prepared_len]) |*row| row.deinit(alloc);
        alloc.free(prepared);
        return results;
    }

    fn prepareGlinerInput(
        self: *GlinerPipeline,
        text: []const u8,
        labels: []const []const u8,
        label_token: i32,
    ) !PreparedGlinerInput {
        const alloc = self.allocator;
        const max_width: usize = self.config.max_width;
        const max_len: usize = self.config.max_length;

        // Split text into words with character offsets
        var words = std.ArrayListUnmanaged([]const u8).empty;
        defer words.deinit(alloc);
        var word_starts = std.ArrayListUnmanaged(usize).empty;
        errdefer word_starts.deinit(alloc);
        var word_ends = std.ArrayListUnmanaged(usize).empty;
        errdefer word_ends.deinit(alloc);

        try splitIntoWords(alloc, text, &words, &word_starts, &word_ends);
        const num_words = words.items.len;

        // Build schema tokens: ( [P] entities ( [E] label1 [E] label2 ... ) ) [SEP_TEXT]
        // Special tokens [P], [E], [SEP_TEXT] use their dedicated token IDs.
        // Regular text parts are tokenized normally.
        var schema_ids = std.ArrayListUnmanaged(i32).empty;
        defer schema_ids.deinit(alloc);

        // "("
        {
            const ids = try self.tok.encode(alloc, "(");
            defer alloc.free(ids);
            try schema_ids.appendSlice(alloc, ids);
        }
        // [P]
        try schema_ids.append(alloc, self.config.token_p);
        // "entities"
        {
            const ids = try self.tok.encode(alloc, "entities");
            defer alloc.free(ids);
            try schema_ids.appendSlice(alloc, ids);
        }
        // "("
        {
            const ids = try self.tok.encode(alloc, "(");
            defer alloc.free(ids);
            try schema_ids.appendSlice(alloc, ids);
        }

        for (labels) |label| {
            // [E] special token
            try schema_ids.append(alloc, label_token);

            // Label text (tokenized normally)
            const lbl_ids = try self.tok.encode(alloc, label);
            defer alloc.free(lbl_ids);
            try schema_ids.appendSlice(alloc, lbl_ids);
        }

        // ")"
        {
            const ids = try self.tok.encode(alloc, ")");
            defer alloc.free(ids);
            try schema_ids.appendSlice(alloc, ids);
        }
        // ")"
        {
            const ids = try self.tok.encode(alloc, ")");
            defer alloc.free(ids);
            try schema_ids.appendSlice(alloc, ids);
        }
        // [SEP_TEXT] special token
        try schema_ids.append(alloc, self.config.token_sep_text);

        const schema_len = schema_ids.items.len;

        // Tokenize each word individually (lowercased), tracking sub-token count per word
        var text_ids = std.ArrayListUnmanaged(i32).empty;
        defer text_ids.deinit(alloc);
        var word_token_counts = try alloc.alloc(usize, num_words);
        defer alloc.free(word_token_counts);

        for (words.items, 0..) |word, wi| {
            const lower = try toLower(alloc, word);
            defer alloc.free(lower);

            const ids = try self.tok.encode(alloc, lower);
            defer alloc.free(ids);

            word_token_counts[wi] = ids.len;
            try text_ids.appendSlice(alloc, ids);
        }

        // Total sequence length (clamp to max_len)
        var seq_len = schema_len + text_ids.items.len;
        if (seq_len > max_len) seq_len = max_len;

        // Build input tensors
        const input_ids_buf = try alloc.alloc(i64, seq_len);
        errdefer alloc.free(input_ids_buf);
        const attention_mask_buf = try alloc.alloc(i64, seq_len);
        errdefer alloc.free(attention_mask_buf);
        const words_mask_buf = try alloc.alloc(i64, seq_len);
        errdefer alloc.free(words_mask_buf);

        // Fill schema tokens
        for (0..@min(schema_len, seq_len)) |j| {
            input_ids_buf[j] = @intCast(schema_ids.items[j]);
            attention_mask_buf[j] = 1;
            words_mask_buf[j] = 0; // schema tokens get 0
        }

        // Fill text tokens with word IDs (1-indexed)
        var pos = schema_len;
        var actual_num_words: usize = 0;
        var token_offset: usize = 0;
        for (0..num_words) |wi| {
            const count = word_token_counts[wi];
            if (pos + count > seq_len) break;

            for (0..count) |ti| {
                if (pos >= seq_len) break;
                input_ids_buf[pos] = @intCast(text_ids.items[token_offset + ti]);
                attention_mask_buf[pos] = 1;
                words_mask_buf[pos] = @intCast(wi + 1); // 1-indexed word ID
                pos += 1;
            }
            token_offset += count;
            actual_num_words = wi + 1;
        }

        // Build span indices: all word-level spans up to max_width
        const num_spans = actual_num_words * max_width;
        const span_idx_buf = try alloc.alloc(i64, num_spans * 2);
        errdefer alloc.free(span_idx_buf);

        for (0..actual_num_words) |w| {
            for (0..max_width) |wi| {
                const span_i = w * max_width + wi;
                const end_word = w + wi;
                if (end_word < actual_num_words) {
                    span_idx_buf[span_i * 2] = @intCast(w);
                    span_idx_buf[span_i * 2 + 1] = @intCast(end_word);
                } else {
                    span_idx_buf[span_i * 2] = 0;
                    span_idx_buf[span_i * 2 + 1] = 0;
                }
            }
        }

        const word_starts_owned = try word_starts.toOwnedSlice(alloc);
        errdefer alloc.free(word_starts_owned);
        const word_ends_owned = try word_ends.toOwnedSlice(alloc);

        return .{
            .text = text,
            .input_ids = input_ids_buf,
            .attention_mask = attention_mask_buf,
            .words_mask = words_mask_buf,
            .span_idx = span_idx_buf,
            .word_starts = word_starts_owned,
            .word_ends = word_ends_owned,
            .actual_num_words = actual_num_words,
            .num_spans = num_spans,
        };
    }

    fn decodeEntitiesFromLogits(
        self: *GlinerPipeline,
        row: PreparedGlinerInput,
        labels: []const []const u8,
        logits: []const f32,
        output_num_words: usize,
        output_max_width: usize,
        num_labels_dim: usize,
        threshold: f32,
        flat_ner: bool,
    ) ![]Entity {
        const alloc = self.allocator;
        var entities = std.ArrayListUnmanaged(Entity).empty;
        errdefer {
            for (entities.items) |e| alloc.free(e.text);
            entities.deinit(alloc);
        }

        for (0..@min(row.actual_num_words, output_num_words)) |w| {
            for (0..output_max_width) |wi| {
                const end_word = w + wi;
                if (end_word >= row.actual_num_words) continue;

                // Flat index: w * max_width * num_labels + wi * num_labels + li
                const span_base = w * output_max_width * num_labels_dim + wi * num_labels_dim;

                for (0..num_labels_dim) |li| {
                    const logit_idx = span_base + li;
                    if (logit_idx >= logits.len) continue;

                    const score = sigmoid(logits[logit_idx]);
                    if (score >= threshold) {
                        const char_start = row.word_starts[w];
                        const char_end = row.word_ends[end_word];
                        const entity_text = try alloc.dupe(u8, row.text[char_start..char_end]);

                        try entities.append(alloc, .{
                            .text = entity_text,
                            .label = labels[li],
                            .start = char_start,
                            .end = char_end,
                            .score = score,
                        });
                    }
                }
            }
        }

        // Flat NER: remove overlapping entities (keep highest score)
        if (flat_ner and entities.items.len > 1) {
            std.mem.sort(Entity, entities.items, {}, struct {
                fn lessThan(_: void, a: Entity, b: Entity) bool {
                    return a.score > b.score;
                }
            }.lessThan);

            var keep = std.ArrayListUnmanaged(Entity).empty;
            defer keep.deinit(alloc);

            for (entities.items) |ent| {
                var overlaps = false;
                for (keep.items) |existing| {
                    if (ent.start < existing.end and ent.end > existing.start) {
                        overlaps = true;
                        break;
                    }
                }
                if (!overlaps) {
                    try keep.append(alloc, ent);
                } else {
                    alloc.free(ent.text);
                }
            }

            std.mem.sort(Entity, keep.items, {}, struct {
                fn lessThan(_: void, a: Entity, b: Entity) bool {
                    return if (a.start != b.start) a.start < b.start else a.end < b.end;
                }
            }.lessThan);

            entities.deinit(alloc);
            return try keep.toOwnedSlice(alloc);
        }

        std.mem.sort(Entity, entities.items, {}, struct {
            fn lessThan(_: void, a: Entity, b: Entity) bool {
                return if (a.start != b.start) a.start < b.start else a.end < b.end;
            }
        }.lessThan);

        return try entities.toOwnedSlice(alloc);
    }

    fn extractRelationsSingle(
        self: *GlinerPipeline,
        text: []const u8,
        entity_labels: []const []const u8,
        relation_labels: []const []const u8,
    ) !struct { entities: []Entity, relations: []Relation } {
        const alloc = self.allocator;
        const entities = try self.recognizeWithLabelToken(text, entity_labels, self.config.token_e, self.config.threshold, self.config.flat_ner);
        errdefer {
            for (entities) |entity| alloc.free(entity.text);
            alloc.free(entities);
        }

        if (entities.len < 2 or relation_labels.len == 0) {
            return .{
                .entities = entities,
                .relations = try alloc.alloc(Relation, 0),
            };
        }

        var composite_labels = std.ArrayListUnmanaged([]const u8).empty;
        defer {
            for (composite_labels.items) |label| alloc.free(label);
            composite_labels.deinit(alloc);
        }
        try appendRelationCandidateLabels(alloc, &composite_labels, entity_labels, relation_labels);
        if (composite_labels.items.len == 0) {
            return .{
                .entities = entities,
                .relations = try alloc.alloc(Relation, 0),
            };
        }

        const relation_token = if (self.config.token_r != 0) self.config.token_r else self.config.token_e;
        const relation_threshold = if (self.config.relation_threshold > 0) self.config.relation_threshold else self.config.threshold;
        const relation_heads = try self.recognizeWithLabelToken(text, composite_labels.items, relation_token, relation_threshold, self.config.flat_ner);
        defer {
            for (relation_heads) |head| alloc.free(head.text);
            alloc.free(relation_heads);
        }

        return .{
            .entities = entities,
            .relations = try self.matchRelations(entities, relation_heads),
        };
    }

    fn classifySingle(
        self: *GlinerPipeline,
        text: []const u8,
        labels: []const []const u8,
        config: ClassificationConfig,
    ) ![]ClassificationResult {
        const alloc = self.allocator;
        if (!self.supportsClassification()) return error.ClassificationNotSupported;
        if (text.len == 0) return try alloc.alloc(ClassificationResult, 0);

        const scores = try self.scoreLabels(text, labels);
        defer alloc.free(scores);

        var filtered = std.ArrayListUnmanaged(ClassificationResult).empty;
        errdefer filtered.deinit(alloc);

        for (labels, 0..) |label, i| {
            if (scores[i] < config.threshold) continue;
            try filtered.append(alloc, .{
                .label = label,
                .score = scores[i],
            });
        }

        std.mem.sort(ClassificationResult, filtered.items, {}, struct {
            fn lessThan(_: void, a: ClassificationResult, b: ClassificationResult) bool {
                return a.score > b.score;
            }
        }.lessThan);

        var keep_len = filtered.items.len;
        if (!config.multi_label and keep_len > 0) {
            keep_len = if (config.top_k > 0) @min(config.top_k, keep_len) else 1;
        } else if (config.top_k > 0) {
            keep_len = @min(config.top_k, keep_len);
        }
        if (keep_len != filtered.items.len) {
            filtered.shrinkRetainingCapacity(keep_len);
        }

        return try filtered.toOwnedSlice(alloc);
    }

    fn scoreLabels(self: *GlinerPipeline, text: []const u8, labels: []const []const u8) ![]f32 {
        const alloc = self.allocator;
        const max_width: usize = self.config.max_width;
        const max_len: usize = self.config.max_length;

        const label_token = if (self.config.token_c != 0) self.config.token_c else self.config.token_e;
        if (self.config.token_p == 0 or label_token == 0 or self.config.token_sep_text == 0)
            return error.MissingSpecialTokenIds;

        var words = std.ArrayListUnmanaged([]const u8).empty;
        defer words.deinit(alloc);
        var word_starts = std.ArrayListUnmanaged(usize).empty;
        defer word_starts.deinit(alloc);
        var word_ends = std.ArrayListUnmanaged(usize).empty;
        defer word_ends.deinit(alloc);

        try splitIntoWords(alloc, text, &words, &word_starts, &word_ends);
        const num_words = words.items.len;
        if (num_words == 0) return try alloc.alloc(f32, 0);

        var schema_ids = std.ArrayListUnmanaged(i32).empty;
        defer schema_ids.deinit(alloc);

        {
            const ids = try self.tok.encode(alloc, "(");
            defer alloc.free(ids);
            try schema_ids.appendSlice(alloc, ids);
        }
        try schema_ids.append(alloc, self.config.token_p);
        {
            const ids = try self.tok.encode(alloc, "entities");
            defer alloc.free(ids);
            try schema_ids.appendSlice(alloc, ids);
        }
        {
            const ids = try self.tok.encode(alloc, "(");
            defer alloc.free(ids);
            try schema_ids.appendSlice(alloc, ids);
        }
        for (labels) |label| {
            try schema_ids.append(alloc, label_token);
            const lbl_ids = try self.tok.encode(alloc, label);
            defer alloc.free(lbl_ids);
            try schema_ids.appendSlice(alloc, lbl_ids);
        }
        {
            const ids = try self.tok.encode(alloc, ")");
            defer alloc.free(ids);
            try schema_ids.appendSlice(alloc, ids);
        }
        {
            const ids = try self.tok.encode(alloc, ")");
            defer alloc.free(ids);
            try schema_ids.appendSlice(alloc, ids);
        }
        try schema_ids.append(alloc, self.config.token_sep_text);

        const schema_len = schema_ids.items.len;

        var text_ids = std.ArrayListUnmanaged(i32).empty;
        defer text_ids.deinit(alloc);
        const word_token_counts = try alloc.alloc(usize, num_words);
        defer alloc.free(word_token_counts);

        for (words.items, 0..) |word, wi| {
            const lower = try toLower(alloc, word);
            defer alloc.free(lower);

            const ids = try self.tok.encode(alloc, lower);
            defer alloc.free(ids);

            word_token_counts[wi] = ids.len;
            try text_ids.appendSlice(alloc, ids);
        }

        var seq_len = schema_len + text_ids.items.len;
        if (seq_len > max_len) seq_len = max_len;

        const input_ids_buf = try alloc.alloc(i64, seq_len);
        defer alloc.free(input_ids_buf);
        const attention_mask_buf = try alloc.alloc(i64, seq_len);
        defer alloc.free(attention_mask_buf);
        const words_mask_buf = try alloc.alloc(i64, seq_len);
        defer alloc.free(words_mask_buf);

        for (0..@min(schema_len, seq_len)) |j| {
            input_ids_buf[j] = @intCast(schema_ids.items[j]);
            attention_mask_buf[j] = 1;
            words_mask_buf[j] = 0;
        }

        var pos = schema_len;
        var actual_num_words: usize = 0;
        var token_offset: usize = 0;
        for (0..num_words) |wi| {
            const count = word_token_counts[wi];
            if (pos + count > seq_len) break;

            for (0..count) |ti| {
                if (pos >= seq_len) break;
                input_ids_buf[pos] = @intCast(text_ids.items[token_offset + ti]);
                attention_mask_buf[pos] = 1;
                words_mask_buf[pos] = @intCast(wi + 1);
                pos += 1;
            }
            token_offset += count;
            actual_num_words = wi + 1;
        }

        const num_spans = actual_num_words * max_width;
        if (num_spans == 0) {
            const empty_scores = try alloc.alloc(f32, labels.len);
            @memset(empty_scores, 0);
            return empty_scores;
        }

        const span_idx_buf = try alloc.alloc(i64, num_spans * 2);
        defer alloc.free(span_idx_buf);

        for (0..actual_num_words) |w| {
            for (0..max_width) |wi| {
                const span_i = w * max_width + wi;
                const end_word = w + wi;
                if (end_word < actual_num_words) {
                    span_idx_buf[span_i * 2] = @intCast(w);
                    span_idx_buf[span_i * 2 + 1] = @intCast(end_word);
                } else {
                    span_idx_buf[span_i * 2] = 0;
                    span_idx_buf[span_i * 2 + 1] = 0;
                }
            }
        }

        const seq: i64 = @intCast(seq_len);
        const shape_2d = [_]i64{ 1, seq };

        var input_ids_tensor = try Tensor.initInt64(alloc, "input_ids", &shape_2d, input_ids_buf);
        defer input_ids_tensor.deinit();
        var attention_mask_tensor = try Tensor.initInt64(alloc, "attention_mask", &shape_2d, attention_mask_buf);
        defer attention_mask_tensor.deinit();
        var words_mask_tensor = try Tensor.initInt64(alloc, "words_mask", &shape_2d, words_mask_buf);
        defer words_mask_tensor.deinit();

        const ns: i64 = @intCast(num_spans);
        const span_shape = [_]i64{ 1, ns, 2 };
        var span_idx_tensor = try Tensor.initInt64(alloc, "span_idx", &span_shape, span_idx_buf);
        defer span_idx_tensor.deinit();

        const outputs = try self.session.run(&.{
            input_ids_tensor,
            attention_mask_tensor,
            words_mask_tensor,
            span_idx_tensor,
        }, alloc);
        defer {
            for (outputs) |*o| o.deinit();
            alloc.free(outputs);
        }

        if (outputs.len == 0) return error.NoOutputTensors;

        const output = outputs[0];
        const logits = output.asFloat32();
        const output_shape = output.shape;

        var num_labels_dim: usize = labels.len;
        if (output_shape.len >= 4) {
            num_labels_dim = @intCast(output_shape[3]);
        } else if (output_shape.len == 3) {
            num_labels_dim = @intCast(output_shape[2]);
        }
        if (num_labels_dim > labels.len) num_labels_dim = labels.len;

        return scoreLabelsFromLogits(alloc, logits, num_labels_dim);
    }

    pub fn supportsClassification(self: *const GlinerPipeline) bool {
        if (std.mem.eql(u8, self.config.model_type, "gliner2")) return true;
        for (self.config.capabilities) |cap| {
            if (std.mem.eql(u8, cap, "classification")) return true;
        }
        return false;
    }

    pub fn supportsExtraction(self: *const GlinerPipeline) bool {
        if (std.mem.eql(u8, self.config.model_type, "gliner2")) return true;
        for (self.config.capabilities) |cap| {
            if (std.mem.eql(u8, cap, "extraction")) return true;
        }
        return false;
    }

    pub fn supportsRelationExtraction(self: *const GlinerPipeline) bool {
        if (std.mem.eql(u8, self.config.model_type, "gliner2")) return true;
        for (self.config.capabilities) |cap| {
            if (std.mem.eql(u8, cap, "relations")) return true;
        }
        return false;
    }

    fn matchRelations(self: *GlinerPipeline, entities: []const Entity, relation_heads: []const Entity) ![]Relation {
        const alloc = self.allocator;
        if (entities.len == 0 or relation_heads.len == 0) return try alloc.alloc(Relation, 0);

        var relations = std.ArrayListUnmanaged(Relation).empty;
        errdefer {
            for (relations.items) |*relation| relation.deinit(alloc);
            relations.deinit(alloc);
        }

        for (relation_heads) |head_span| {
            const candidate = parseRelationCandidateLabel(head_span.label) orelse continue;

            var head_entity: ?Entity = null;
            var exact_span_mismatched_label = false;
            for (entities) |entity| {
                if (entity.start == head_span.start and entity.end == head_span.end) {
                    if (std.ascii.eqlIgnoreCase(entity.label, candidate.head_label)) {
                        head_entity = entity;
                        break;
                    }
                    exact_span_mismatched_label = true;
                }
            }
            if (head_entity == null and exact_span_mismatched_label) continue;

            var best_tail: ?Entity = null;
            var best_distance: usize = std.math.maxInt(usize);
            for (entities) |entity| {
                if (candidate.tail_label) |tail_label| {
                    if (!std.ascii.eqlIgnoreCase(entity.label, tail_label)) continue;
                }
                if (overlapsSpan(head_span.start, head_span.end, entity.start, entity.end)) continue;
                const distance = charDistance(head_span.start, head_span.end, entity.start, entity.end);
                if (distance < best_distance) {
                    best_distance = distance;
                    best_tail = entity;
                }
            }
            if (best_tail == null) continue;

            var owned_head_label: ?[]const u8 = null;
            const head_copy = if (head_entity) |entity|
                try duplicateEntity(alloc, entity)
            else blk: {
                owned_head_label = try alloc.dupe(u8, candidate.head_label);
                break :blk Entity{
                    .text = try alloc.dupe(u8, head_span.text),
                    .label = owned_head_label.?,
                    .start = head_span.start,
                    .end = head_span.end,
                    .score = head_span.score,
                };
            };
            errdefer alloc.free(head_copy.text);
            errdefer if (owned_head_label) |owned_label| alloc.free(owned_label);

            const tail_copy = try duplicateEntity(alloc, best_tail.?);
            errdefer alloc.free(tail_copy.text);

            const relation_label_copy = try alloc.dupe(u8, candidate.relation_label);
            errdefer alloc.free(relation_label_copy);

            try relations.append(alloc, .{
                .head = head_copy,
                .tail = tail_copy,
                .label = relation_label_copy,
                .score = head_span.score,
                .owned_head_label = owned_head_label,
            });
        }

        return try relations.toOwnedSlice(alloc);
    }
};

fn appendRelationCandidateLabels(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged([]const u8),
    entity_labels: []const []const u8,
    relation_labels: []const []const u8,
) !void {
    for (relation_labels) |relation_label| {
        if (parseRelationCandidateLabel(relation_label)) |candidate| {
            if (candidate.tail_label) |tail_label| {
                if (!containsLabel(entity_labels, tail_label)) continue;
            }
            for (entity_labels) |entity_label| {
                if (!std.ascii.eqlIgnoreCase(entity_label, candidate.head_label)) continue;
                try appendOwnedRelationCandidateLabel(alloc, out, entity_label, candidate.relation_label, candidate.tail_label);
                break;
            }
            continue;
        }

        for (entity_labels) |entity_label| {
            try appendOwnedRelationCandidateLabel(alloc, out, entity_label, relation_label, null);
        }
    }
}

const RelationCandidateLabel = struct {
    head_label: []const u8,
    relation_label: []const u8,
    tail_label: ?[]const u8 = null,
};

fn parseRelationCandidateLabel(label: []const u8) ?RelationCandidateLabel {
    const first_sep = std.mem.indexOf(u8, label, "::") orelse return null;
    const head_label = label[0..first_sep];
    const rest = label[first_sep + 2 ..];
    if (head_label.len == 0 or rest.len == 0) return null;
    if (std.mem.indexOf(u8, rest, "::")) |second_sep| {
        const relation_label = rest[0..second_sep];
        const tail_label = rest[second_sep + 2 ..];
        if (relation_label.len == 0 or tail_label.len == 0) return null;
        return .{ .head_label = head_label, .relation_label = relation_label, .tail_label = tail_label };
    }
    return .{ .head_label = head_label, .relation_label = rest };
}

fn containsLabel(labels: []const []const u8, wanted: []const u8) bool {
    for (labels) |label| {
        if (std.ascii.eqlIgnoreCase(label, wanted)) return true;
    }
    return false;
}

fn appendOwnedRelationCandidateLabel(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged([]const u8),
    head_label: []const u8,
    relation_label: []const u8,
    tail_label: ?[]const u8,
) !void {
    const owned_label = if (tail_label) |tail|
        try std.fmt.allocPrint(alloc, "{s}::{s}::{s}", .{ head_label, relation_label, tail })
    else
        try std.fmt.allocPrint(alloc, "{s}::{s}", .{ head_label, relation_label });
    errdefer alloc.free(owned_label);
    try out.append(alloc, owned_label);
}

fn scoreLabelsFromLogits(alloc: std.mem.Allocator, logits: []const f32, num_labels: usize) ![]f32 {
    const scores = try alloc.alloc(f32, num_labels);
    if (num_labels == 0) return scores;
    @memset(scores, 0);

    for (0..num_labels) |li| {
        var max_logit: f32 = -1000;
        var idx = li;
        while (idx < logits.len) : (idx += num_labels) {
            if (logits[idx] > max_logit) max_logit = logits[idx];
        }
        scores[li] = sigmoid(max_logit);
    }

    return scores;
}

fn duplicateEntity(alloc: std.mem.Allocator, entity: Entity) !Entity {
    return .{
        .text = try alloc.dupe(u8, entity.text),
        .label = entity.label,
        .start = entity.start,
        .end = entity.end,
        .score = entity.score,
    };
}

fn charDistance(start1: usize, end1: usize, start2: usize, end2: usize) usize {
    if (end1 <= start2) return start2 - end1;
    return start1 - end2;
}

fn overlapsSpan(start1: usize, end1: usize, start2: usize, end2: usize) bool {
    return start1 < end2 and start2 < end1;
}

fn sigmoid(x: f32) f32 {
    return 1.0 / (1.0 + @exp(-x));
}

fn splitIntoWords(
    alloc: std.mem.Allocator,
    text: []const u8,
    words: *std.ArrayListUnmanaged([]const u8),
    starts: *std.ArrayListUnmanaged(usize),
    ends: *std.ArrayListUnmanaged(usize),
) !void {
    var word_start: ?usize = null;
    for (text, 0..) |c, i| {
        if (c == ' ' or c == '\t' or c == '\n' or c == '\r') {
            if (word_start) |ws| {
                try words.append(alloc, text[ws..i]);
                try starts.append(alloc, ws);
                try ends.append(alloc, i);
                word_start = null;
            }
        } else {
            if (word_start == null) word_start = i;
        }
    }
    if (word_start) |ws| {
        try words.append(alloc, text[ws..text.len]);
        try starts.append(alloc, ws);
        try ends.append(alloc, text.len);
    }
}

fn toLower(alloc: std.mem.Allocator, s: []const u8) ![]u8 {
    const buf = try alloc.alloc(u8, s.len);
    for (s, 0..) |c, i| {
        buf[i] = if (c >= 'A' and c <= 'Z') c + 32 else c;
    }
    return buf;
}

test "scoreLabelsFromLogits returns sigmoid of max logit per label" {
    const alloc = std.testing.allocator;
    const logits = [_]f32{
        0.1,  0.5,
        0.7,  -0.2,
        -0.3, 1.2,
    };

    const scores = try scoreLabelsFromLogits(alloc, &logits, 2);
    defer alloc.free(scores);

    try std.testing.expectApproxEqAbs(sigmoid(0.7), scores[0], 1e-6);
    try std.testing.expectApproxEqAbs(sigmoid(1.2), scores[1], 1e-6);
}

test "gliner supportsClassification checks model type and capabilities" {
    var pipeline = GlinerPipeline{
        .allocator = std.testing.allocator,
        .session = undefined,
        .tok = undefined,
        .config = .{ .model_type = "gliner2" },
    };
    try std.testing.expect(pipeline.supportsClassification());

    pipeline.config = .{ .capabilities = &.{"classification"} };
    try std.testing.expect(pipeline.supportsClassification());

    pipeline.config = .{};
    try std.testing.expect(!pipeline.supportsClassification());
}

test "gliner supportsExtraction checks model type and capabilities" {
    var pipeline = GlinerPipeline{
        .allocator = std.testing.allocator,
        .session = undefined,
        .tok = undefined,
        .config = .{ .model_type = "gliner2" },
    };
    try std.testing.expect(pipeline.supportsExtraction());

    pipeline.config = .{ .capabilities = &.{"extraction"} };
    try std.testing.expect(pipeline.supportsExtraction());

    pipeline.config = .{};
    try std.testing.expect(!pipeline.supportsExtraction());
}

test "gliner supportsRelationExtraction checks model type and capabilities" {
    var pipeline = GlinerPipeline{
        .allocator = std.testing.allocator,
        .session = undefined,
        .tok = undefined,
        .config = .{ .model_type = "gliner2" },
    };
    try std.testing.expect(pipeline.supportsRelationExtraction());

    pipeline.config = .{ .capabilities = &.{"relations"} };
    try std.testing.expect(pipeline.supportsRelationExtraction());

    pipeline.config = .{};
    try std.testing.expect(!pipeline.supportsRelationExtraction());
}

test "gliner relation matching keeps labels scores and nearest non-overlapping tail" {
    const allocator = std.testing.allocator;
    var pipeline = GlinerPipeline{
        .allocator = allocator,
        .session = undefined,
        .tok = undefined,
        .config = .{},
    };

    const entities = [_]Entity{
        .{ .text = "John Smith", .label = "person", .start = 0, .end = 10, .score = 0.91 },
        .{ .text = "Acme", .label = "organization", .start = 20, .end = 24, .score = 0.88 },
        .{ .text = "Paris", .label = "location", .start = 27, .end = 32, .score = 0.82 },
    };
    const relation_heads = [_]Entity{
        .{ .text = "John Smith", .label = "person::works_for", .start = 0, .end = 10, .score = 0.73 },
        .{ .text = "Acme", .label = "organization::located_in", .start = 20, .end = 24, .score = 0.64 },
    };

    const relations = try pipeline.matchRelations(&entities, &relation_heads);
    defer {
        for (relations) |*relation| relation.deinit(allocator);
        allocator.free(relations);
    }

    try std.testing.expectEqual(@as(usize, 2), relations.len);
    try std.testing.expectEqualStrings("John Smith", relations[0].head.text);
    try std.testing.expectEqualStrings("Acme", relations[0].tail.text);
    try std.testing.expectEqualStrings("works_for", relations[0].label);
    try std.testing.expectApproxEqAbs(@as(f32, 0.73), relations[0].score, 1e-6);
    try std.testing.expect(relations[0].owned_head_label == null);

    try std.testing.expectEqualStrings("Acme", relations[1].head.text);
    try std.testing.expectEqualStrings("Paris", relations[1].tail.text);
    try std.testing.expectEqualStrings("located_in", relations[1].label);
}

test "gliner relation matching preserves synthetic head label for unmatched head span" {
    const allocator = std.testing.allocator;
    var pipeline = GlinerPipeline{
        .allocator = allocator,
        .session = undefined,
        .tok = undefined,
        .config = .{},
    };

    const entities = [_]Entity{
        .{ .text = "John", .label = "person", .start = 0, .end = 4, .score = 0.91 },
        .{ .text = "Acme", .label = "organization", .start = 15, .end = 19, .score = 0.88 },
    };
    const relation_heads = [_]Entity{
        .{ .text = "employee", .label = "person::works_for", .start = 6, .end = 14, .score = 0.71 },
    };

    const relations = try pipeline.matchRelations(&entities, &relation_heads);
    defer {
        for (relations) |*relation| relation.deinit(allocator);
        allocator.free(relations);
    }

    try std.testing.expectEqual(@as(usize, 1), relations.len);
    try std.testing.expectEqualStrings("employee", relations[0].head.text);
    try std.testing.expectEqualStrings("person", relations[0].head.label);
    try std.testing.expectEqualStrings("person", relations[0].owned_head_label.?);
    try std.testing.expectEqualStrings("Acme", relations[0].tail.text);
    try std.testing.expectEqualStrings("works_for", relations[0].label);
}

test "gliner relation matching skips exact head span with mismatched entity label" {
    const allocator = std.testing.allocator;
    var pipeline = GlinerPipeline{
        .allocator = allocator,
        .tok = undefined,
        .session = undefined,
        .config = .{},
    };

    const entities = [_]Entity{
        .{ .text = "Boston", .label = "location", .start = 0, .end = 6, .score = 0.91 },
        .{ .text = "Acme", .label = "organization", .start = 20, .end = 24, .score = 0.88 },
    };
    const relation_heads = [_]Entity{
        .{ .text = "Boston", .label = "organization::located_in", .start = 0, .end = 6, .score = 0.73 },
    };

    const relations = try pipeline.matchRelations(&entities, &relation_heads);
    defer allocator.free(relations);

    try std.testing.expectEqual(@as(usize, 0), relations.len);
}

test "gliner relation matching honors qualified tail labels" {
    const allocator = std.testing.allocator;
    var pipeline = GlinerPipeline{
        .allocator = allocator,
        .tok = undefined,
        .session = undefined,
        .config = .{},
    };

    const entities = [_]Entity{
        .{ .text = "Alice", .label = "person", .start = 0, .end = 5, .score = 0.91 },
        .{ .text = "Bob", .label = "person", .start = 12, .end = 15, .score = 0.88 },
        .{ .text = "Acme", .label = "organization", .start = 30, .end = 34, .score = 0.87 },
    };
    const relation_heads = [_]Entity{
        .{ .text = "Alice", .label = "person::works_for::organization", .start = 0, .end = 5, .score = 0.73 },
    };

    const relations = try pipeline.matchRelations(&entities, &relation_heads);
    defer {
        for (relations) |*relation| relation.deinit(allocator);
        allocator.free(relations);
    }

    try std.testing.expectEqual(@as(usize, 1), relations.len);
    try std.testing.expectEqualStrings("Acme", relations[0].tail.text);
    try std.testing.expectEqualStrings("works_for", relations[0].label);
}

test "gliner relation candidate labels support qualified head labels" {
    const allocator = std.testing.allocator;
    var labels = std.ArrayListUnmanaged([]const u8).empty;
    defer {
        for (labels.items) |label| allocator.free(label);
        labels.deinit(allocator);
    }

    try appendRelationCandidateLabels(
        allocator,
        &labels,
        &.{ "person", "organization", "location" },
        &.{ "person::works_for::organization", "organization::located_in::location", "person::mentors::team", "founded" },
    );

    try std.testing.expectEqual(@as(usize, 5), labels.items.len);
    try std.testing.expectEqualStrings("person::works_for::organization", labels.items[0]);
    try std.testing.expectEqualStrings("organization::located_in::location", labels.items[1]);
    try std.testing.expectEqualStrings("person::founded", labels.items[2]);
    try std.testing.expectEqualStrings("organization::founded", labels.items[3]);
    try std.testing.expectEqualStrings("location::founded", labels.items[4]);
}

test "gliner distributed mlx helpers mirror pipeline/session state" {
    const allocator = std.testing.allocator;
    const FakeSession = struct {
        fn session() backends.Session {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .run = run,
                    .inputInfo = inputInfo,
                    .outputInfo = outputInfo,
                    .backend = backend,
                    .close = close,
                },
            };
        }

        fn run(_: *anyopaque, _: []const backends.Tensor, allocator_inner: std.mem.Allocator) anyerror![]backends.Tensor {
            return allocator_inner.alloc(backends.Tensor, 0);
        }

        fn inputInfo(_: *anyopaque) []const backends.TensorInfo {
            return &.{};
        }

        fn outputInfo(_: *anyopaque) []const backends.TensorInfo {
            return &.{};
        }

        fn backend(_: *anyopaque) backends.BackendType {
            return .mlx;
        }

        fn close(_: *anyopaque) void {}
    };

    var pipeline = GlinerPipeline{
        .allocator = allocator,
        .session = FakeSession.session(),
        .tok = undefined,
        .config = .{
            .model_type = "gliner2",
            .distributed = .{ .enabled = true, .mode = .tensor_parallel, .world_size = 2, .rank = 0, .local_rank = 0 },
        },
    };
    try std.testing.expect(pipeline.usesDistributedMlx());
    try std.testing.expect(pipeline.usesTensorParallelMlx());
}
