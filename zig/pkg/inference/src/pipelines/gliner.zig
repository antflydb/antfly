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
//   3. Construct input tensors for legacy words_mask models or official
//      text_positions/schema_positions ONNX exports
//   4. Run inference → span logits/scores
//   5. Apply sigmoid, threshold, and flat NER deduplication
//
// Matches Go inference's lib/pipelines/gliner.go (GLiNER2 path).

const std = @import("std");
const platform = @import("antfly_platform");
const backends = @import("../backends/backends.zig");
const session_mod = @import("../backends/session.zig");
const Tokenizer = @import("inference_tokenizer").Tokenizer;
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
    /// Optional caller-owned guard for backend state shared by pipelines made
    /// from the same loaded model. Preparation and decoding stay parallel;
    /// only stateful session execution is serialized.
    execution_lock: ?*std.atomic.Mutex = null,

    fn lockedSessionRun(
        self: *GlinerPipeline,
        permit: *session_mod.RunPermit,
        inputs: []const Tensor,
        alloc: std.mem.Allocator,
    ) ![]Tensor {
        if (self.execution_lock) |mutex| platform.sync.lockYielding(mutex);
        defer if (self.execution_lock) |mutex| mutex.unlock();
        return permit.run(inputs, alloc);
    }

    pub fn usesDistributedGpuHosted(self: *const GlinerPipeline) bool {
        return self.config.distributed.enabled and
            self.config.distributed.world_size > 1 and
            self.session.backend().usesGpuHostedSession();
    }

    pub fn usesTensorParallelGpuHosted(self: *const GlinerPipeline) bool {
        return self.usesDistributedGpuHosted() and self.config.distributed.mode == .tensor_parallel;
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

    /// Returns the exact encoder sequence length for an entities request,
    /// including GLiNER's schema/label prefix. Benchmarks use this to make a
    /// claimed 256-token row fail closed instead of relying on character or
    /// word-count approximations.
    pub fn entityEncoderTokenCount(
        self: *GlinerPipeline,
        text: []const u8,
        labels: []const []const u8,
    ) !usize {
        return self.encoderTokenCount(text, labels, self.config.token_e);
    }

    fn encoderTokenCount(
        self: *GlinerPipeline,
        text: []const u8,
        labels: []const []const u8,
        label_token: i32,
    ) !usize {
        var prepared = try self.prepareGlinerInput(text, labels, label_token);
        defer prepared.deinit(self.allocator);
        return prepared.input_ids.len;
    }

    /// Returns the largest exact classification row. Empty texts are handled
    /// without inference by `classifySingle` and therefore contribute zero.
    pub fn maxClassificationInputTokens(
        self: *GlinerPipeline,
        texts: []const []const u8,
        labels: []const []const u8,
    ) !usize {
        const label_token = if (self.config.token_c != 0) self.config.token_c else self.config.token_e;
        var result: usize = 0;
        for (texts) |text| {
            if (text.len == 0) continue;
            result = @max(result, try self.encoderTokenCount(text, labels, label_token));
        }
        return result;
    }

    /// Returns a conservative exact-token upper bound for every encoder row
    /// this extraction can execute. Relation extraction conditionally runs a
    /// second pass, so its composite-label row is included before inference.
    pub fn maxExtractionInputTokens(
        self: *GlinerPipeline,
        texts: []const []const u8,
        entity_labels: ?[]const []const u8,
        relation_labels: ?[]const []const u8,
    ) !usize {
        const use_entity_labels = entity_labels orelse self.config.default_labels;
        if (use_entity_labels.len == 0) return error.NoLabelsProvided;

        var result: usize = 0;
        for (texts) |text| {
            result = @max(result, try self.entityEncoderTokenCount(text, use_entity_labels));
        }

        const use_relation_labels = relation_labels orelse self.config.relation_labels;
        if (use_relation_labels.len == 0) return result;
        var composite_labels = std.ArrayListUnmanaged([]const u8).empty;
        defer {
            for (composite_labels.items) |label| self.allocator.free(label);
            composite_labels.deinit(self.allocator);
        }
        try appendRelationCandidateLabels(
            self.allocator,
            &composite_labels,
            use_entity_labels,
            use_relation_labels,
        );
        for (texts) |text| {
            result = @max(result, try self.entityEncoderTokenCount(text, composite_labels.items));
        }
        return result;
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
        var active_len: usize = 0;
        for (all_entities, 0..) |entities, i| {
            if (entities.len < 2) continue;
            active_texts[active_len] = texts[i];
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

        var active_index: usize = 0;
        for (all_entities, 0..) |entities, row_index| {
            if (entities.len < 2) {
                all_relations[row_index] = try alloc.alloc(Relation, 0);
                initialized_relations += 1;
                continue;
            }

            all_relations[row_index] = try self.matchRelations(entities, relation_heads[active_index]);
            active_index += 1;
            initialized_relations += 1;
            all_entities[row_index] = try includeRelationEndpoints(
                alloc,
                all_entities[row_index],
                all_relations[row_index],
                use_entity_labels,
            );
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

    const PreparedGlinerSchema = struct {
        ids: []i32,
        positions: []i64,

        fn deinit(self: *PreparedGlinerSchema, alloc: std.mem.Allocator) void {
            alloc.free(self.ids);
            alloc.free(self.positions);
        }
    };

    const PreparedGlinerInput = struct {
        text: []const u8,
        input_ids: []i64,
        attention_mask: []i64,
        words_mask: []i64,
        text_positions: []i64,
        schema_positions: []i64,
        span_idx: []i64,
        word_starts: []usize,
        word_ends: []usize,
        actual_num_words: usize,
        num_spans: usize,

        fn deinit(self: *PreparedGlinerInput, alloc: std.mem.Allocator) void {
            alloc.free(self.input_ids);
            alloc.free(self.attention_mask);
            alloc.free(self.words_mask);
            alloc.free(self.text_positions);
            alloc.free(self.schema_positions);
            alloc.free(self.span_idx);
            alloc.free(self.word_starts);
            alloc.free(self.word_ends);
        }
    };

    const GlinerOutputLayout = enum {
        word_major_logits,
        label_major_scores,
    };

    const BatchOutputLayout = struct {
        num_words: usize,
        max_width: usize,
        num_labels: usize,
        row_stride: usize,
    };

    fn validateBatchOutputLayout(
        output_shape: []const i64,
        logits_len: usize,
        batch: usize,
        expected_num_words: usize,
        expected_max_width: usize,
        requested_labels: usize,
    ) !BatchOutputLayout {
        if (output_shape.len != 3 and output_shape.len != 4) return error.UnexpectedOutputShape;
        const output_batch = nonNegativeDim(output_shape[0]) catch return error.UnexpectedOutputShape;
        if (output_batch != batch) return error.UnexpectedOutputShape;

        var num_words = expected_num_words;
        var max_width = expected_max_width;
        const output_labels: usize = if (output_shape.len == 4) labels: {
            num_words = nonNegativeDim(output_shape[1]) catch return error.UnexpectedOutputShape;
            max_width = nonNegativeDim(output_shape[2]) catch return error.UnexpectedOutputShape;
            break :labels nonNegativeDim(output_shape[3]) catch return error.UnexpectedOutputShape;
        } else labels: {
            const output_spans = nonNegativeDim(output_shape[1]) catch return error.UnexpectedOutputShape;
            const expected_spans = std.math.mul(usize, expected_num_words, expected_max_width) catch
                return error.UnexpectedOutputShape;
            if (output_spans != expected_spans) return error.UnexpectedOutputShape;
            break :labels nonNegativeDim(output_shape[2]) catch return error.UnexpectedOutputShape;
        };
        if (num_words == 0 or
            max_width == 0 or
            num_words != expected_num_words or
            max_width != expected_max_width or
            output_labels != requested_labels)
        {
            return error.UnexpectedOutputShape;
        }

        const row_stride = std.math.mul(
            usize,
            std.math.mul(usize, num_words, max_width) catch return error.UnexpectedOutputShape,
            output_labels,
        ) catch return error.UnexpectedOutputShape;
        const expected_logits = std.math.mul(usize, batch, row_stride) catch return error.UnexpectedOutputShape;
        if (logits_len != expected_logits) return error.UnexpectedOutputShape;

        return .{
            .num_words = num_words,
            .max_width = max_width,
            .num_labels = requested_labels,
            .row_stride = row_stride,
        };
    }

    fn recognizeWithLabelTokenBatch(
        self: *GlinerPipeline,
        texts: []const []const u8,
        labels: []const []const u8,
        label_token: i32,
        threshold: f32,
        flat_ner: bool,
    ) ![][]Entity {
        const profile_enabled = glinerPipelineProfileEnabled();
        const total_start_ns = glinerProfileStart(profile_enabled);
        const alloc = self.allocator;
        const max_width: usize = self.config.max_width;
        const cuda_preprocessing = self.session.backend() == .cuda;

        if (self.config.token_p == 0 or label_token == 0 or self.config.token_sep_text == 0)
            return error.MissingSpecialTokenIds;

        if (self.session.backend() == .onnx and texts.len > 1) {
            return self.recognizeWithLabelTokenBatchSerial(texts, labels, label_token, threshold, flat_ner);
        }

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

        var schema = if (cuda_preprocessing) try self.prepareGlinerSchema(labels, label_token) else null;
        defer if (schema) |*value| value.deinit(alloc);

        // Identical request rows are prepared and entity-decoded once on
        // every backend: the packed batch (and therefore the model forward)
        // is unchanged, so duplicate rows produce identical logits and their
        // results clone from the first occurrence. This also keeps
        // cross-backend benchmark comparisons on repeated-text batches
        // measuring the same work. Only the schema-reuse preparation path
        // below remains CUDA-specific.
        var prepared = try alloc.alloc(PreparedGlinerInput, texts.len);
        var prepared_len: usize = 0;
        errdefer {
            for (prepared[0..prepared_len]) |*row| row.deinit(alloc);
            alloc.free(prepared);
        }
        const row_to_prepared = try alloc.alloc(usize, texts.len);
        defer alloc.free(row_to_prepared);
        var prepared_by_text = std.StringHashMapUnmanaged(usize).empty;
        defer prepared_by_text.deinit(alloc);

        var max_seq_len: usize = 0;
        var max_num_words: usize = 0;
        const prepare_start_ns = glinerProfileStart(profile_enabled);
        for (texts, 0..) |text, i| {
            if (prepared_by_text.get(text)) |prepared_i| {
                row_to_prepared[i] = prepared_i;
                continue;
            }

            const prepared_i = prepared_len;
            prepared[prepared_i] = if (schema) |value|
                try self.prepareGlinerInputWithSchema(text, value, true)
            else
                try self.prepareGlinerInput(text, labels, label_token);
            prepared_len += 1;
            try prepared_by_text.put(alloc, text, prepared_i);
            row_to_prepared[i] = prepared_i;
            max_seq_len = @max(max_seq_len, prepared[prepared_i].input_ids.len);
            max_num_words = @max(max_num_words, prepared[prepared_i].actual_num_words);
        }
        const prepare_ms = glinerProfileElapsedMs(prepare_start_ns);

        if (max_num_words == 0 or max_seq_len == 0) {
            for (results) |*row| {
                row.* = try alloc.alloc(Entity, 0);
                initialized_results += 1;
            }
            for (prepared[0..prepared_len]) |*row| row.deinit(alloc);
            alloc.free(prepared);
            return results;
        }

        if (self.usesOfficialOnnxGlinerContract()) {
            const session_start_ns = glinerProfileStart(profile_enabled);
            for (row_to_prepared, 0..) |prepared_i, i| {
                const row = prepared[prepared_i];
                results[i] = try self.recognizeOfficialOnnxGlinerSingle(row, labels, threshold, flat_ner);
                initialized_results += 1;
            }
            const session_decode_ms = glinerProfileElapsedMs(session_start_ns);
            if (profile_enabled) {
                std.debug.print(
                    "gliner_pipeline_profile: official_onnx=1 batch={d} labels={d} seq_len={d} num_words={d} max_width={d} prepare_ms={d:.3} session_decode_ms={d:.3} total_ms={d:.3}\n",
                    .{
                        texts.len,
                        labels.len,
                        max_seq_len,
                        max_num_words,
                        max_width,
                        prepare_ms,
                        session_decode_ms,
                        glinerProfileElapsedMs(total_start_ns),
                    },
                );
            }

            for (prepared[0..prepared_len]) |*row| row.deinit(alloc);
            alloc.free(prepared);
            return results;
        }

        const batch = texts.len;
        const batch_num_spans = try checkedSizeMul(max_num_words, max_width);
        const batch_rows = try checkedSizeMul(batch, max_seq_len);
        const span_values = try checkedSizeMul(try checkedSizeMul(batch, batch_num_spans), 2);
        const input_elements = std.math.add(
            usize,
            std.math.mul(usize, batch_rows, 3) catch
                return error.ResourceLimitExceeded,
            span_values,
        ) catch return error.ResourceLimitExceeded;
        const input_bytes = std.math.mul(usize, input_elements, @sizeOf(i64)) catch
            return error.ResourceLimitExceeded;
        var run_permit = try self.session.admit(.{
            .batch = batch,
            .sequence = max_seq_len,
            .input_bytes = input_bytes,
            .host_preprocess_bytes = input_bytes,
        });
        defer run_permit.deinit();
        const pack_start_ns = glinerProfileStart(profile_enabled);
        const input_ids_buf = try alloc.alloc(i64, batch_rows);
        defer alloc.free(input_ids_buf);
        const attention_mask_buf = try alloc.alloc(i64, batch_rows);
        defer alloc.free(attention_mask_buf);
        const words_mask_buf = try alloc.alloc(i64, batch_rows);
        defer alloc.free(words_mask_buf);
        const span_idx_buf = try alloc.alloc(i64, span_values);
        defer alloc.free(span_idx_buf);
        @memset(input_ids_buf, 0);
        @memset(attention_mask_buf, 0);
        @memset(words_mask_buf, 0);
        @memset(span_idx_buf, 0);

        for (row_to_prepared, 0..) |prepared_i, b| {
            const row = prepared[prepared_i];
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

        const shape_2d = [_]i64{ try sizeToI64(batch), try sizeToI64(max_seq_len) };
        var input_ids_tensor = try Tensor.initInt64(alloc, "input_ids", &shape_2d, input_ids_buf);
        defer input_ids_tensor.deinit();
        var attention_mask_tensor = try Tensor.initInt64(alloc, "attention_mask", &shape_2d, attention_mask_buf);
        defer attention_mask_tensor.deinit();
        var words_mask_tensor = try Tensor.initInt64(alloc, "words_mask", &shape_2d, words_mask_buf);
        defer words_mask_tensor.deinit();

        const span_shape = [_]i64{ try sizeToI64(batch), try sizeToI64(batch_num_spans), 2 };
        var span_idx_tensor = try Tensor.initInt64(alloc, "span_idx", &span_shape, span_idx_buf);
        defer span_idx_tensor.deinit();
        const pack_ms = glinerProfileElapsedMs(pack_start_ns);

        const session_start_ns = glinerProfileStart(profile_enabled);
        const outputs = try self.lockedSessionRun(&run_permit, &.{
            input_ids_tensor,
            attention_mask_tensor,
            words_mask_tensor,
            span_idx_tensor,
        }, alloc);
        const session_ms = glinerProfileElapsedMs(session_start_ns);
        defer {
            for (outputs) |*o| o.deinit();
            alloc.free(outputs);
        }
        if (outputs.len == 0) return error.NoOutputTensors;

        const output = outputs[0];
        const logits = output.asFloat32();
        const output_shape = output.shape;

        const output_layout = try validateBatchOutputLayout(
            output_shape,
            logits.len,
            batch,
            max_num_words,
            max_width,
            labels.len,
        );

        const decode_start_ns = glinerProfileStart(profile_enabled);
        const first_result_by_prepared = try alloc.alloc(?usize, prepared_len);
        defer alloc.free(first_result_by_prepared);
        @memset(first_result_by_prepared, null);
        for (row_to_prepared, 0..) |prepared_i, i| {
            const row = prepared[prepared_i];
            results[i] = if (first_result_by_prepared[prepared_i] != null)
                try cloneEntities(alloc, results[first_result_by_prepared[prepared_i].?])
            else blk: {
                const row_start = try checkedSizeMul(i, output_layout.row_stride);
                const row_end = try checkedSizeAdd(row_start, output_layout.row_stride);
                break :blk try self.decodeEntitiesFromLogits(
                    row,
                    labels,
                    logits[row_start..row_end],
                    output_layout.num_words,
                    output_layout.max_width,
                    output_layout.num_labels,
                    threshold,
                    flat_ner,
                    .word_major_logits,
                );
            };
            if (first_result_by_prepared[prepared_i] == null) first_result_by_prepared[prepared_i] = i;
            initialized_results += 1;
        }
        const decode_ms = glinerProfileElapsedMs(decode_start_ns);
        if (profile_enabled) {
            std.debug.print(
                "gliner_pipeline_profile: batch={d} unique_texts={d} labels={d} seq_len={d} num_words={d} max_width={d} prepare_ms={d:.3} pack_ms={d:.3} session_run_ms={d:.3} decode_ms={d:.3} total_ms={d:.3}\n",
                .{
                    batch,
                    prepared_len,
                    labels.len,
                    max_seq_len,
                    max_num_words,
                    max_width,
                    prepare_ms,
                    pack_ms,
                    session_ms,
                    decode_ms,
                    glinerProfileElapsedMs(total_start_ns),
                },
            );
        }

        for (prepared[0..prepared_len]) |*row| row.deinit(alloc);
        alloc.free(prepared);
        return results;
    }

    fn recognizeWithLabelTokenBatchSerial(
        self: *GlinerPipeline,
        texts: []const []const u8,
        labels: []const []const u8,
        label_token: i32,
        threshold: f32,
        flat_ner: bool,
    ) anyerror![][]Entity {
        const alloc = self.allocator;
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

        for (texts, 0..) |_, i| {
            const single = try self.recognizeWithLabelTokenBatch(texts[i .. i + 1], labels, label_token, threshold, flat_ner);
            if (single.len != 1) {
                for (single) |entities| {
                    for (entities) |entity| alloc.free(entity.text);
                    alloc.free(entities);
                }
                alloc.free(single);
                return error.UnexpectedOutputShape;
            }
            results[i] = single[0];
            alloc.free(single);
            initialized_results += 1;
        }

        return results;
    }

    fn recognizeOfficialOnnxGlinerSingle(
        self: *GlinerPipeline,
        row: PreparedGlinerInput,
        labels: []const []const u8,
        threshold: f32,
        flat_ner: bool,
    ) ![]Entity {
        const alloc = self.allocator;
        const input_elements = std.math.add(
            usize,
            std.math.add(
                usize,
                row.input_ids.len,
                row.attention_mask.len,
            ) catch return error.ResourceLimitExceeded,
            std.math.add(
                usize,
                std.math.add(
                    usize,
                    row.text_positions.len,
                    row.schema_positions.len,
                ) catch return error.ResourceLimitExceeded,
                row.span_idx.len,
            ) catch return error.ResourceLimitExceeded,
        ) catch return error.ResourceLimitExceeded;
        const input_bytes = std.math.mul(usize, input_elements, @sizeOf(i64)) catch
            return error.ResourceLimitExceeded;
        var run_permit = try self.session.admit(.{
            .batch = 1,
            .sequence = row.input_ids.len,
            .input_bytes = input_bytes,
            .host_preprocess_bytes = input_bytes,
        });
        defer run_permit.deinit();
        const seq: i64 = @intCast(row.input_ids.len);
        const shape_2d = [_]i64{ 1, seq };
        var input_ids_tensor = try Tensor.initInt64(alloc, "input_ids", &shape_2d, row.input_ids);
        defer input_ids_tensor.deinit();
        var attention_mask_tensor = try Tensor.initInt64(alloc, "attention_mask", &shape_2d, row.attention_mask);
        defer attention_mask_tensor.deinit();

        const text_positions_shape = [_]i64{@intCast(row.text_positions.len)};
        var text_positions_tensor = try Tensor.initInt64(alloc, "text_positions", &text_positions_shape, row.text_positions);
        defer text_positions_tensor.deinit();

        const schema_positions_shape = [_]i64{@intCast(row.schema_positions.len)};
        var schema_positions_tensor = try Tensor.initInt64(alloc, "schema_positions", &schema_positions_shape, row.schema_positions);
        defer schema_positions_tensor.deinit();

        const span_shape = [_]i64{ 1, @intCast(row.num_spans), 2 };
        var span_idx_tensor = try Tensor.initInt64(alloc, "span_idx", &span_shape, row.span_idx);
        defer span_idx_tensor.deinit();

        const outputs = try self.lockedSessionRun(&run_permit, &.{
            input_ids_tensor,
            attention_mask_tensor,
            text_positions_tensor,
            schema_positions_tensor,
            span_idx_tensor,
        }, alloc);
        defer {
            for (outputs) |*o| o.deinit();
            alloc.free(outputs);
        }
        if (outputs.len == 0) return error.NoOutputTensors;

        const output = outputs[0];
        const scores = output.asFloat32();
        const output_shape = output.shape;
        if (output_shape.len != 4 and output_shape.len != 3) return error.UnexpectedOutputShape;

        const dim_offset: usize = if (output_shape.len == 4) 1 else 0;
        if (output_shape.len == 4 and (nonNegativeDim(output_shape[0]) catch return error.UnexpectedOutputShape) != 1) {
            return error.UnexpectedOutputShape;
        }
        const output_labels = nonNegativeDim(output_shape[dim_offset]) catch return error.UnexpectedOutputShape;
        const output_num_words = nonNegativeDim(output_shape[dim_offset + 1]) catch return error.UnexpectedOutputShape;
        const output_max_width = nonNegativeDim(output_shape[dim_offset + 2]) catch return error.UnexpectedOutputShape;
        if (output_labels < labels.len or output_num_words == 0 or output_max_width == 0) {
            return error.UnexpectedOutputShape;
        }
        const expected_scores = std.math.mul(
            usize,
            std.math.mul(usize, output_labels, output_num_words) catch return error.UnexpectedOutputShape,
            output_max_width,
        ) catch return error.UnexpectedOutputShape;
        if (scores.len != expected_scores) return error.UnexpectedOutputShape;

        return self.decodeEntitiesFromLogits(
            row,
            labels,
            scores,
            output_num_words,
            output_max_width,
            labels.len,
            threshold,
            flat_ner,
            .label_major_scores,
        );
    }

    fn prepareGlinerInput(
        self: *GlinerPipeline,
        text: []const u8,
        labels: []const []const u8,
        label_token: i32,
    ) !PreparedGlinerInput {
        var schema = try self.prepareGlinerSchema(labels, label_token);
        defer schema.deinit(self.allocator);
        return self.prepareGlinerInputWithSchema(text, schema, false);
    }

    fn prepareGlinerSchema(
        self: *GlinerPipeline,
        labels: []const []const u8,
        label_token: i32,
    ) !PreparedGlinerSchema {
        const alloc = self.allocator;
        var schema_ids = std.ArrayListUnmanaged(i32).empty;
        errdefer schema_ids.deinit(alloc);
        var schema_positions = std.ArrayListUnmanaged(i64).empty;
        errdefer schema_positions.deinit(alloc);

        // Use the caller-owned token buffers throughout this hot path. The
        // previous encode/append/free sequence allocated a fresh result for
        // every schema fragment, label, and word.
        try self.tok.encodeInto(alloc, "(", &schema_ids);
        // [P]
        try schema_positions.append(alloc, @intCast(schema_ids.items.len));
        try schema_ids.append(alloc, self.config.token_p);
        // "entities"
        try self.tok.encodeInto(alloc, "entities", &schema_ids);
        // "("
        try self.tok.encodeInto(alloc, "(", &schema_ids);

        for (labels) |label| {
            // [E] special token
            try schema_positions.append(alloc, @intCast(schema_ids.items.len));
            try schema_ids.append(alloc, label_token);

            // Label text (tokenized normally)
            try self.tok.encodeInto(alloc, label, &schema_ids);
        }

        try self.tok.encodeInto(alloc, ")", &schema_ids);
        try self.tok.encodeInto(alloc, ")", &schema_ids);
        // [SEP_TEXT] special token
        try schema_ids.append(alloc, self.config.token_sep_text);

        const ids_owned = try schema_ids.toOwnedSlice(alloc);
        errdefer alloc.free(ids_owned);
        const positions_owned = try schema_positions.toOwnedSlice(alloc);
        return .{ .ids = ids_owned, .positions = positions_owned };
    }

    fn prepareGlinerInputWithSchema(
        self: *GlinerPipeline,
        text: []const u8,
        schema: PreparedGlinerSchema,
        cache_word_tokens: bool,
    ) !PreparedGlinerInput {
        const alloc = self.allocator;
        const max_width: usize = self.config.max_width;
        const max_len: usize = self.config.max_length;

        var words = std.ArrayListUnmanaged([]const u8).empty;
        defer words.deinit(alloc);
        var word_starts = std.ArrayListUnmanaged(usize).empty;
        errdefer word_starts.deinit(alloc);
        var word_ends = std.ArrayListUnmanaged(usize).empty;
        errdefer word_ends.deinit(alloc);

        try splitIntoWords(alloc, text, &words, &word_starts, &word_ends);
        const num_words = words.items.len;
        const schema_len = schema.ids.len;

        // Tokenize each word individually (lowercased), tracking sub-token count per word
        var unique_word_ids = std.ArrayListUnmanaged(i32).empty;
        defer unique_word_ids.deinit(alloc);
        const TokenRange = struct { offset: usize, len: usize };
        var word_cache = std.HashMapUnmanaged([]const u8, TokenRange, AsciiCaselessStringContext, 80).empty;
        defer word_cache.deinit(alloc);
        var lowercase = std.ArrayListUnmanaged(u8).empty;
        defer lowercase.deinit(alloc);
        const word_token_ranges = try alloc.alloc(TokenRange, num_words);
        defer alloc.free(word_token_ranges);
        var text_token_count: usize = 0;

        for (words.items, 0..) |word, wi| {
            const token_range = (if (cache_word_tokens) word_cache.get(word) else null) orelse blk: {
                lowercase.clearRetainingCapacity();
                try lowercase.ensureUnusedCapacity(alloc, word.len);
                for (word) |c| lowercase.appendAssumeCapacity(std.ascii.toLower(c));
                const offset = unique_word_ids.items.len;
                try self.tok.encodeInto(alloc, lowercase.items, &unique_word_ids);
                const range = TokenRange{ .offset = offset, .len = unique_word_ids.items.len - offset };
                if (cache_word_tokens) try word_cache.put(alloc, word, range);
                break :blk range;
            };
            word_token_ranges[wi] = token_range;
            text_token_count = std.math.add(usize, text_token_count, token_range.len) catch
                return error.ResourceLimitExceeded;
        }

        // Total sequence length (clamp to max_len)
        var seq_len = std.math.add(usize, schema_len, text_token_count) catch
            return error.ResourceLimitExceeded;
        if (seq_len > max_len) seq_len = max_len;

        const input_ids_buf = try alloc.alloc(i64, seq_len);
        errdefer alloc.free(input_ids_buf);
        const attention_mask_buf = try alloc.alloc(i64, seq_len);
        errdefer alloc.free(attention_mask_buf);
        const words_mask_buf = try alloc.alloc(i64, seq_len);
        errdefer alloc.free(words_mask_buf);
        @memset(input_ids_buf, 0);
        @memset(attention_mask_buf, 0);
        @memset(words_mask_buf, 0);
        var text_positions = std.ArrayListUnmanaged(i64).empty;
        errdefer text_positions.deinit(alloc);

        for (0..@min(schema_len, seq_len)) |j| {
            input_ids_buf[j] = @intCast(schema.ids[j]);
            attention_mask_buf[j] = 1;
            words_mask_buf[j] = 0;
        }

        var pos = schema_len;
        var actual_num_words: usize = 0;
        for (0..num_words) |wi| {
            const token_range = word_token_ranges[wi];
            const count = token_range.len;
            if (pos > seq_len or count > seq_len - pos) break;

            try text_positions.append(alloc, @intCast(pos));
            for (0..count) |ti| {
                if (pos >= seq_len) break;
                input_ids_buf[pos] = @intCast(unique_word_ids.items[token_range.offset + ti]);
                attention_mask_buf[pos] = 1;
                words_mask_buf[pos] = @intCast(wi + 1);
                pos += 1;
            }
            actual_num_words = wi + 1;
        }

        const num_spans = std.math.mul(usize, actual_num_words, max_width) catch
            return error.ResourceLimitExceeded;
        const span_values = std.math.mul(usize, num_spans, 2) catch
            return error.ResourceLimitExceeded;
        const span_idx_buf = try alloc.alloc(i64, span_values);
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
        errdefer alloc.free(word_ends_owned);
        const text_positions_owned = try text_positions.toOwnedSlice(alloc);
        errdefer alloc.free(text_positions_owned);
        const schema_positions_owned = try alloc.dupe(i64, schema.positions);

        return .{
            .text = text,
            .input_ids = input_ids_buf,
            .attention_mask = attention_mask_buf,
            .words_mask = words_mask_buf,
            .text_positions = text_positions_owned,
            .schema_positions = schema_positions_owned,
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
        output_layout: GlinerOutputLayout,
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
                    const output_idx = switch (output_layout) {
                        .word_major_logits => span_base + li,
                        .label_major_scores => li * output_num_words * output_max_width + w * output_max_width + wi,
                    };
                    if (output_idx >= logits.len) continue;

                    const score = switch (output_layout) {
                        .word_major_logits => sigmoid(logits[output_idx]),
                        .label_major_scores => logits[output_idx],
                    };
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
            return try dedupEntitiesByLabelText(alloc, try keep.toOwnedSlice(alloc));
        }

        std.mem.sort(Entity, entities.items, {}, struct {
            fn lessThan(_: void, a: Entity, b: Entity) bool {
                return if (a.start != b.start) a.start < b.start else a.end < b.end;
            }
        }.lessThan);

        return try dedupEntitiesByLabelText(alloc, try entities.toOwnedSlice(alloc));
    }

    fn usesOfficialOnnxGlinerContract(self: *const GlinerPipeline) bool {
        return self.session.backend() == .onnx and
            self.hasSessionInput("text_positions") and
            self.hasSessionInput("schema_positions");
    }

    fn hasSessionInput(self: *const GlinerPipeline, name: []const u8) bool {
        for (self.session.inputInfo()) |input| {
            if (std.mem.eql(u8, input.name, name)) return true;
        }
        return false;
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

        const relations = try self.matchRelations(entities, relation_heads);
        errdefer {
            for (relations) |*relation| relation.deinit(alloc);
            alloc.free(relations);
        }
        return .{
            .entities = try includeRelationEndpoints(alloc, entities, relations, entity_labels),
            .relations = relations,
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
        if (num_words == 0) {
            const scores = try alloc.alloc(f32, labels.len);
            @memset(scores, 0);
            return scores;
        }

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
        const admitted_span_elements = std.math.mul(
            usize,
            std.math.mul(usize, num_words, max_width) catch
                return error.ResourceLimitExceeded,
            2,
        ) catch return error.ResourceLimitExceeded;
        const admitted_input_elements = std.math.add(
            usize,
            std.math.mul(usize, seq_len, 3) catch
                return error.ResourceLimitExceeded,
            admitted_span_elements,
        ) catch return error.ResourceLimitExceeded;
        const admitted_input_bytes = std.math.mul(
            usize,
            admitted_input_elements,
            @sizeOf(i64),
        ) catch return error.ResourceLimitExceeded;
        var run_permit = try self.session.admit(.{
            .batch = 1,
            .sequence = seq_len,
            .input_bytes = admitted_input_bytes,
            .host_preprocess_bytes = admitted_input_bytes,
        });
        defer run_permit.deinit();

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

        const outputs = try self.lockedSessionRun(&run_permit, &.{
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

        const output_layout = try validateBatchOutputLayout(
            output_shape,
            logits.len,
            1,
            actual_num_words,
            max_width,
            labels.len,
        );
        return scoreLabelsFromLogits(alloc, logits, output_layout.num_labels);
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

fn includeRelationEndpoints(
    alloc: std.mem.Allocator,
    entities: []Entity,
    relations: []const Relation,
    stable_labels: []const []const u8,
) ![]Entity {
    if (relations.len == 0) return entities;

    var out = std.ArrayListUnmanaged(Entity).empty;
    var borrowed_count: usize = 0;
    errdefer {
        for (out.items[borrowed_count..]) |entity| alloc.free(entity.text);
        out.deinit(alloc);
    }
    try out.appendSlice(alloc, entities);
    borrowed_count = entities.len;

    for (relations) |relation| {
        const endpoints = [_]Entity{ relation.head, relation.tail };
        for (endpoints) |endpoint| {
            if (containsEntity(out.items, endpoint)) continue;

            const stable_label = findStableLabel(stable_labels, endpoint.label) orelse
                return error.InvalidRelationEndpointLabel;
            const text = try alloc.dupe(u8, endpoint.text);
            out.append(alloc, .{
                .text = text,
                .label = stable_label,
                .start = endpoint.start,
                .end = endpoint.end,
                .score = endpoint.score,
            }) catch |err| {
                alloc.free(text);
                return err;
            };
        }
    }

    const reconciled = try out.toOwnedSlice(alloc);
    alloc.free(entities);
    return reconciled;
}

fn containsEntity(entities: []const Entity, needle: Entity) bool {
    for (entities) |entity| {
        if (entity.start == needle.start and entity.end == needle.end and
            std.mem.eql(u8, entity.label, needle.label) and
            std.mem.eql(u8, entity.text, needle.text)) return true;
    }
    return false;
}

fn findStableLabel(labels: []const []const u8, wanted: []const u8) ?[]const u8 {
    for (labels) |label| {
        if (std.mem.eql(u8, label, wanted)) return label;
    }
    return null;
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

fn dedupEntitiesByLabelText(alloc: std.mem.Allocator, input: []Entity) ![]Entity {
    if (input.len <= 1) return input;

    var keep = std.ArrayListUnmanaged(Entity).empty;
    errdefer {
        for (keep.items) |entity| alloc.free(entity.text);
        keep.deinit(alloc);
    }
    keep.ensureTotalCapacity(alloc, input.len) catch |err| {
        for (input) |entity| alloc.free(entity.text);
        alloc.free(input);
        return err;
    };

    for (input) |entity| {
        var kept_duplicate = false;
        for (keep.items) |*existing| {
            if (std.ascii.eqlIgnoreCase(entity.label, existing.label) and std.ascii.eqlIgnoreCase(entity.text, existing.text)) {
                kept_duplicate = true;
                if (entity.score > existing.score) {
                    alloc.free(existing.text);
                    existing.* = entity;
                } else {
                    alloc.free(entity.text);
                }
                break;
            }
        }
        if (!kept_duplicate) {
            keep.appendAssumeCapacity(entity);
        }
    }

    std.mem.sort(Entity, keep.items, {}, struct {
        fn lessThan(_: void, a: Entity, b: Entity) bool {
            return if (a.start != b.start) a.start < b.start else a.end < b.end;
        }
    }.lessThan);

    alloc.free(input);
    return try keep.toOwnedSlice(alloc);
}

fn glinerPipelineProfileEnabled() bool {
    return platform.env.getenvBool("TERMITE_GLINER_PROFILE") or
        platform.env.getenvBool("TERMITE_GLINER_PIPELINE_PROFILE");
}

fn glinerProfileNowNs() u128 {
    var ts: std.posix.timespec = undefined;
    return switch (std.posix.errno(std.posix.system.clock_gettime(.MONOTONIC, &ts))) {
        .SUCCESS => @intCast(@as(i128, ts.sec) * std.time.ns_per_s + ts.nsec),
        else => 0,
    };
}

fn glinerProfileStart(enabled: bool) u128 {
    return if (enabled) glinerProfileNowNs() else 0;
}

fn glinerProfileElapsedMs(start_ns: u128) f64 {
    if (start_ns == 0) return 0.0;
    const now = glinerProfileNowNs();
    if (now <= start_ns) return 0.0;
    return @as(f64, @floatFromInt(now - start_ns)) / 1.0e6;
}

fn splitIntoWords(
    alloc: std.mem.Allocator,
    text: []const u8,
    words: *std.ArrayListUnmanaged([]const u8),
    starts: *std.ArrayListUnmanaged(usize),
    ends: *std.ArrayListUnmanaged(usize),
) !void {
    var i: usize = 0;
    while (i < text.len) {
        if (isAsciiWhitespace(text[i])) {
            i += 1;
            continue;
        }

        const start = i;
        if (startsWithAsciiIgnoreCase(text[i..], "http://") or
            startsWithAsciiIgnoreCase(text[i..], "https://") or
            startsWithAsciiIgnoreCase(text[i..], "www."))
        {
            i = consumeUntilWhitespace(text, i);
        } else if (text[i] == '@' and i + 1 < text.len and isWordScalar(text[i + 1 ..])) {
            i += 1;
            while (i < text.len and isWordScalar(text[i..])) i += utf8ScalarLen(text[i..]);
        } else if (isWordScalar(text[i..])) {
            i += utf8ScalarLen(text[i..]);
            while (i < text.len) {
                if (isWordScalar(text[i..])) {
                    i += utf8ScalarLen(text[i..]);
                    continue;
                }
                if (text[i] == '-' and i + 1 < text.len and isWordScalar(text[i + 1 ..])) {
                    i += 1 + utf8ScalarLen(text[i + 1 ..]);
                    continue;
                }
                break;
            }
        } else {
            i += utf8ScalarLen(text[i..]);
        }

        try words.append(alloc, text[start..i]);
        try starts.append(alloc, start);
        try ends.append(alloc, i);
    }
}

fn isAsciiWhitespace(c: u8) bool {
    return c == ' ' or c == '\t' or c == '\n' or c == '\r' or c == 0x0b or c == 0x0c;
}

fn isAsciiWord(c: u8) bool {
    return std.ascii.isAlphanumeric(c) or c == '_';
}

fn isWordScalar(bytes: []const u8) bool {
    if (bytes.len == 0) return false;
    return isAsciiWord(bytes[0]) or bytes[0] >= 0x80;
}

fn consumeUntilWhitespace(text: []const u8, start: usize) usize {
    var i = start;
    while (i < text.len and !isAsciiWhitespace(text[i])) i += utf8ScalarLen(text[i..]);
    return i;
}

fn startsWithAsciiIgnoreCase(haystack: []const u8, needle: []const u8) bool {
    return haystack.len >= needle.len and std.ascii.eqlIgnoreCase(haystack[0..needle.len], needle);
}

fn utf8ScalarLen(bytes: []const u8) usize {
    if (bytes.len == 0) return 0;
    return @min(std.unicode.utf8ByteSequenceLength(bytes[0]) catch 1, bytes.len);
}

fn toLower(alloc: std.mem.Allocator, s: []const u8) ![]u8 {
    const buf = try alloc.alloc(u8, s.len);
    for (s, 0..) |c, i| {
        buf[i] = if (c >= 'A' and c <= 'Z') c + 32 else c;
    }
    return buf;
}

const AsciiCaselessStringContext = struct {
    pub fn hash(_: @This(), value: []const u8) u64 {
        var hash_value: u64 = 14695981039346656037;
        for (value) |c| {
            hash_value ^= std.ascii.toLower(c);
            hash_value *%= 1099511628211;
        }
        return hash_value;
    }

    pub fn eql(_: @This(), a: []const u8, b: []const u8) bool {
        return std.ascii.eqlIgnoreCase(a, b);
    }
};

fn checkedSizeMul(a: usize, b: usize) !usize {
    return std.math.mul(usize, a, b) catch error.ResourceLimitExceeded;
}

fn checkedSizeAdd(a: usize, b: usize) !usize {
    return std.math.add(usize, a, b) catch error.ResourceLimitExceeded;
}

fn sizeToI64(value: usize) !i64 {
    if (value > std.math.maxInt(i64)) return error.ResourceLimitExceeded;
    return @intCast(value);
}

fn nonNegativeDim(value: i64) !usize {
    if (value < 0) return error.UnexpectedOutputShape;
    return @intCast(value);
}

fn cloneEntities(alloc: std.mem.Allocator, entities: []const Entity) ![]Entity {
    const cloned = try alloc.alloc(Entity, entities.len);
    var initialized: usize = 0;
    errdefer {
        for (cloned[0..initialized]) |entity| alloc.free(entity.text);
        alloc.free(cloned);
    }
    for (entities, 0..) |entity, i| {
        cloned[i] = entity;
        cloned[i].text = try alloc.dupe(u8, entity.text);
        initialized += 1;
    }
    return cloned;
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

test "gliner batch output validation rejects incomplete backend results" {
    const layout = try GlinerPipeline.validateBatchOutputLayout(&.{ 2, 3, 4, 3 }, 72, 2, 3, 4, 3);
    try std.testing.expectEqual(@as(usize, 3), layout.num_words);
    try std.testing.expectEqual(@as(usize, 4), layout.max_width);
    try std.testing.expectEqual(@as(usize, 3), layout.num_labels);
    try std.testing.expectEqual(@as(usize, 36), layout.row_stride);

    try std.testing.expectError(
        error.UnexpectedOutputShape,
        GlinerPipeline.validateBatchOutputLayout(&.{ 1, 3, 4, 3 }, 36, 2, 3, 4, 3),
    );
    try std.testing.expectError(
        error.UnexpectedOutputShape,
        GlinerPipeline.validateBatchOutputLayout(&.{ 2, 3, 4, 3 }, 71, 2, 3, 4, 3),
    );
    try std.testing.expectError(
        error.UnexpectedOutputShape,
        GlinerPipeline.validateBatchOutputLayout(&.{ 2, 3, 4, 2 }, 48, 2, 3, 4, 3),
    );
    try std.testing.expectError(
        error.UnexpectedOutputShape,
        GlinerPipeline.validateBatchOutputLayout(&.{ 2, 3, 4, 4 }, 96, 2, 3, 4, 3),
    );
    try std.testing.expectError(
        error.UnexpectedOutputShape,
        GlinerPipeline.validateBatchOutputLayout(&.{ 2, 2, 4, 3 }, 48, 2, 3, 4, 3),
    );
    try std.testing.expectError(
        error.UnexpectedOutputShape,
        GlinerPipeline.validateBatchOutputLayout(&.{ 2, 3, 2, 3 }, 36, 2, 3, 4, 3),
    );
    try std.testing.expectError(
        error.UnexpectedOutputShape,
        GlinerPipeline.validateBatchOutputLayout(&.{ 2, 11, 5 }, 110, 2, 3, 4, 3),
    );
}

test "gliner whitespace classification returns one zero score per label" {
    var pipeline = GlinerPipeline{
        .allocator = std.testing.allocator,
        .session = undefined,
        .tok = undefined,
        .config = .{ .token_p = 1, .token_e = 2, .token_sep_text = 3 },
    };
    const scores = try pipeline.scoreLabels(" \t\n", &.{ "person", "organization" });
    defer std.testing.allocator.free(scores);
    try std.testing.expectEqualSlices(f32, &.{ 0, 0 }, scores);
}

test "gliner splitIntoWords separates punctuation like Python processor" {
    const alloc = std.testing.allocator;
    const text = "Apple Inc. hired @alice in San Francisco.";
    var words = std.ArrayListUnmanaged([]const u8).empty;
    defer words.deinit(alloc);
    var starts = std.ArrayListUnmanaged(usize).empty;
    defer starts.deinit(alloc);
    var ends = std.ArrayListUnmanaged(usize).empty;
    defer ends.deinit(alloc);

    try splitIntoWords(alloc, text, &words, &starts, &ends);

    const expected = [_][]const u8{ "Apple", "Inc", ".", "hired", "@alice", "in", "San", "Francisco", "." };
    try std.testing.expectEqual(expected.len, words.items.len);
    for (expected, 0..) |want, i| {
        try std.testing.expectEqualStrings(want, words.items[i]);
        try std.testing.expectEqualStrings(want, text[starts.items[i]..ends.items[i]]);
    }
}

test "gliner splitIntoWords keeps utf8 word text intact" {
    const alloc = std.testing.allocator;
    const text = "José García joined München Labs in São Paulo.";
    var words = std.ArrayListUnmanaged([]const u8).empty;
    defer words.deinit(alloc);
    var starts = std.ArrayListUnmanaged(usize).empty;
    defer starts.deinit(alloc);
    var ends = std.ArrayListUnmanaged(usize).empty;
    defer ends.deinit(alloc);

    try splitIntoWords(alloc, text, &words, &starts, &ends);

    const expected = [_][]const u8{ "José", "García", "joined", "München", "Labs", "in", "São", "Paulo", "." };
    try std.testing.expectEqual(expected.len, words.items.len);
    for (expected, 0..) |want, i| {
        try std.testing.expectEqualStrings(want, words.items[i]);
        try std.testing.expectEqualStrings(want, text[starts.items[i]..ends.items[i]]);
    }
}

test "gliner dedups repeated entity text per label" {
    const alloc = std.testing.allocator;
    var entities = try alloc.alloc(Entity, 4);
    entities[0] = .{ .text = try alloc.dupe(u8, "Apple Inc."), .label = "organization", .start = 0, .end = 10, .score = 1.0 };
    entities[1] = .{ .text = try alloc.dupe(u8, "Apple Inc."), .label = "organization", .start = 20, .end = 30, .score = 1.1 };
    entities[2] = .{ .text = try alloc.dupe(u8, "Apple Inc."), .label = "location", .start = 40, .end = 50, .score = 0.8 };
    entities[3] = .{ .text = try alloc.dupe(u8, "Cupertino"), .label = "location", .start = 60, .end = 69, .score = 0.7 };

    const deduped = try dedupEntitiesByLabelText(alloc, entities);
    defer {
        for (deduped) |entity| alloc.free(entity.text);
        alloc.free(deduped);
    }

    try std.testing.expectEqual(@as(usize, 3), deduped.len);
    try std.testing.expectEqual(@as(usize, 20), deduped[0].start);
    try std.testing.expectEqualStrings("organization", deduped[0].label);
    try std.testing.expectEqualStrings("location", deduped[1].label);
    try std.testing.expectEqualStrings("Cupertino", deduped[2].text);
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

test "gliner relation results include synthetic endpoint entities" {
    const allocator = std.testing.allocator;
    const labels = [_][]const u8{ "person", "organization" };

    const entities = try allocator.alloc(Entity, 2);
    var initialized_entities: usize = 0;
    errdefer {
        for (entities[0..initialized_entities]) |entity| allocator.free(entity.text);
        allocator.free(entities);
    }
    entities[0] = .{
        .text = try allocator.dupe(u8, "John"),
        .label = labels[0],
        .start = 0,
        .end = 4,
        .score = 0.91,
    };
    initialized_entities += 1;
    entities[1] = .{
        .text = try allocator.dupe(u8, "Acme"),
        .label = labels[1],
        .start = 15,
        .end = 19,
        .score = 0.88,
    };
    initialized_entities += 1;

    const relations = [_]Relation{.{
        .head = .{
            .text = "employee",
            .label = "person",
            .start = 6,
            .end = 14,
            .score = 0.71,
        },
        .tail = .{
            .text = "Acme",
            .label = "organization",
            .start = 15,
            .end = 19,
            .score = 0.88,
        },
        .label = "works_for",
        .score = 0.71,
    }};

    const reconciled = try includeRelationEndpoints(allocator, entities, &relations, &labels);
    defer {
        for (reconciled) |entity| allocator.free(entity.text);
        allocator.free(reconciled);
    }

    try std.testing.expectEqual(@as(usize, 3), reconciled.len);
    try std.testing.expect(containsEntity(reconciled, relations[0].head));
    try std.testing.expect(containsEntity(reconciled, relations[0].tail));
    try std.testing.expectEqualStrings("employee", reconciled[2].text);
    try std.testing.expectEqualStrings("person", reconciled[2].label);
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

test "gliner distributed GPU-hosted helpers mirror pipeline/session state" {
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
            return .metal;
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
    try std.testing.expect(pipeline.usesDistributedGpuHosted());
    try std.testing.expect(pipeline.usesTensorParallelGpuHosted());
}
