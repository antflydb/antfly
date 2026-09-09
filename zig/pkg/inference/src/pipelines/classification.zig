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

// Zero-shot text classification via Natural Language Inference (NLI).
//
// For each (text, label) pair, constructs a hypothesis from a template
// (e.g. "This example is {label}."), tokenizes as [CLS] text [SEP] hypothesis [SEP],
// and runs through an NLI cross-encoder. The entailment logit gives the
// relevance score for that label. Scores are normalized across labels
// (softmax for single-label, sigmoid for multi-label).

const std = @import("std");
const backends = @import("../backends/backends.zig");
const tokenizer_mod = @import("inference_tokenizer");
const Tokenizer = tokenizer_mod.Tokenizer;
const Tensor = backends.Tensor;
const runtime = @import("../runtime/root.zig");
const session_mod = @import("../backends/session.zig");
const InferenceExecutionControl = @import("../execution_control.zig").InferenceExecutionControl;

pub const ClassificationConfig = struct {
    max_length: usize = 512,
    hypothesis_template: []const u8 = "This example is {}.",
    multi_label: bool = false,
    entailment_index: ?usize = null, // index of entailment class; auto-detect if null
    /// Dynamic text encoders should execute only through the longest active
    /// premise/hypothesis pair instead of materializing max_length padding.
    trim_padding_to_batch_max: bool = true,
    distributed: runtime.distributed.Config = .{},
};

pub const ClassificationResult = struct {
    label: []const u8,
    score: f32,
};

/// Tokenized NLI rows shared by executor-contract validation, usage
/// accounting, and model execution. `labels` remain caller-owned, matching the
/// borrowed labels in `ClassificationResult`; every encoded row, the label
/// semantics snapshot, and the hypothesis-template snapshot are owned by this
/// value and released by `deinit`.
pub const PreparedClassificationInputs = struct {
    allocator: std.mem.Allocator,
    labels: []const []const u8,
    /// Exact owned copy of the labels used to build `encoded`. Results retain
    /// the public API's borrowed labels, while this snapshot prevents changed
    /// contents or ordering from silently re-attributing logits.
    label_semantics: [][]u8,
    encoded: []tokenizer_mod.EncodeResult,
    text_count: usize,
    max_length: usize,
    owner_session_ptr: *anyopaque,
    owner_session_vtable: *const backends.Session.VTable,
    owner_tokenizer_ptr: *anyopaque,
    owner_tokenizer_vtable: *const Tokenizer.VTable,
    hypothesis_template: []u8,
    multi_label: bool,
    entailment_index: ?usize,
    trim_padding_to_batch_max: bool,
    preprocess_permit: ?session_mod.RunPermit = null,
    run_permit: ?session_mod.RunPermit = null,
    max_input_tokens_per_item: usize = 0,
    prompt_tokens: usize = 0,

    pub fn deinit(self: *@This()) void {
        if (self.preprocess_permit) |*permit| permit.deinit();
        if (self.run_permit) |*permit| permit.deinit();
        for (self.encoded) |*item| item.deinit();
        self.allocator.free(self.encoded);
        for (self.label_semantics) |label| self.allocator.free(label);
        self.allocator.free(self.label_semantics);
        self.allocator.free(self.hypothesis_template);
        self.* = undefined;
    }

    fn labelsMatchSemantics(self: *const @This()) bool {
        if (self.labels.len != self.label_semantics.len) return false;
        for (self.labels, self.label_semantics) |label, semantic_label| {
            if (!std.mem.eql(u8, label, semantic_label)) return false;
        }
        return true;
    }
};

pub const ClassificationPipeline = struct {
    allocator: std.mem.Allocator,
    session: backends.Session,
    tok: Tokenizer,
    config: ClassificationConfig,
    execution_control: ?InferenceExecutionControl = null,

    pub fn usesDistributedGpuHosted(self: *const ClassificationPipeline) bool {
        return self.config.distributed.enabled and
            self.config.distributed.world_size > 1 and
            self.session.backend().usesGpuHostedSession();
    }

    pub fn usesTensorParallelGpuHosted(self: *const ClassificationPipeline) bool {
        return self.usesDistributedGpuHosted() and self.config.distributed.mode == .tensor_parallel;
    }

    pub fn init(
        allocator: std.mem.Allocator,
        session: backends.Session,
        tok: Tokenizer,
        config: ClassificationConfig,
    ) ClassificationPipeline {
        return .{
            .allocator = allocator,
            .session = session,
            .tok = tok,
            .config = config,
        };
    }

    /// Classify a single text against candidate labels.
    /// Returns results sorted by score descending. Caller owns the returned slice.
    pub fn classify(
        self: *ClassificationPipeline,
        text: []const u8,
        labels: []const []const u8,
    ) ![]ClassificationResult {
        const batch = try self.classifyBatch(&.{text}, labels);
        defer self.allocator.free(batch);
        return batch[0];
    }

    /// Classify a batch of texts. Returns [num_texts][]ClassificationResult.
    pub fn classifyBatch(
        self: *ClassificationPipeline,
        texts: []const []const u8,
        labels: []const []const u8,
    ) ![][]ClassificationResult {
        if (self.execution_control) |control| try control.update(.tokenizing, 0, @intCast(texts.len));
        var prepared = try self.prepareInputs(texts, labels);
        defer prepared.deinit();
        return self.classifyPrepared(&prepared);
    }

    pub fn prepareInputs(
        self: *ClassificationPipeline,
        texts: []const []const u8,
        labels: []const []const u8,
    ) !PreparedClassificationInputs {
        return self.prepareInputsInternal(texts, labels, true);
    }

    fn prepareInputsInternal(
        self: *ClassificationPipeline,
        texts: []const []const u8,
        labels: []const []const u8,
        admit: bool,
    ) !PreparedClassificationInputs {
        const alloc = self.allocator;
        const total_pairs = std.math.mul(usize, texts.len, labels.len) catch
            return error.ClassificationBatchTooLarge;
        const max_preprocess_tokens = std.math.mul(usize, total_pairs, self.config.max_length) catch
            return error.ClassificationBatchTooLarge;
        var preprocess_permit: ?session_mod.RunPermit = if (admit and total_pairs != 0)
            try self.session.admitHostPreprocess(
                std.math.mul(usize, max_preprocess_tokens, 40) catch
                    return error.ResourceLimitExceeded,
            )
        else
            null;
        errdefer if (preprocess_permit) |*permit| permit.deinit();
        const encoded = try alloc.alloc(tokenizer_mod.EncodeResult, total_pairs);
        var encoded_count: usize = 0;
        errdefer {
            for (encoded[0..encoded_count]) |*item| item.deinit();
            alloc.free(encoded);
        }
        const hypothesis_template = try alloc.dupe(u8, self.config.hypothesis_template);
        var hypothesis_template_transferred = false;
        errdefer if (!hypothesis_template_transferred) alloc.free(hypothesis_template);
        const label_semantics = try alloc.alloc([]u8, labels.len);
        var label_semantics_count: usize = 0;
        var label_semantics_transferred = false;
        errdefer if (!label_semantics_transferred) {
            for (label_semantics[0..label_semantics_count]) |label| alloc.free(label);
            alloc.free(label_semantics);
        };
        for (labels, 0..) |label, index| {
            label_semantics[index] = try alloc.dupe(u8, label);
            label_semantics_count += 1;
        }

        var prepared = PreparedClassificationInputs{
            .allocator = alloc,
            .labels = labels,
            .label_semantics = label_semantics,
            .encoded = encoded,
            .text_count = texts.len,
            .max_length = self.config.max_length,
            .owner_session_ptr = self.session.ptr,
            .owner_session_vtable = self.session.vtable,
            .owner_tokenizer_ptr = self.tok.ptr,
            .owner_tokenizer_vtable = self.tok.vtable,
            .hypothesis_template = hypothesis_template,
            .multi_label = self.config.multi_label,
            .entailment_index = self.config.entailment_index,
            .trim_padding_to_batch_max = self.config.trim_padding_to_batch_max,
            .preprocess_permit = preprocess_permit,
        };
        hypothesis_template_transferred = true;
        label_semantics_transferred = true;
        preprocess_permit = null;
        errdefer {
            if (prepared.preprocess_permit) |*permit| permit.deinit();
            if (prepared.run_permit) |*permit| permit.deinit();
            for (prepared.label_semantics) |label| alloc.free(label);
            alloc.free(prepared.label_semantics);
            alloc.free(prepared.hypothesis_template);
        }
        if (total_pairs == 0) return prepared;

        const hypotheses = try alloc.alloc([]const u8, labels.len);
        var hypotheses_count: usize = 0;
        defer {
            for (hypotheses[0..hypotheses_count]) |hypothesis| alloc.free(hypothesis);
            alloc.free(hypotheses);
        }
        for (labels, 0..) |label, index| {
            hypotheses[index] = try self.formatHypothesis(label);
            hypotheses_count += 1;
        }

        for (texts, 0..) |text, text_index| {
            for (hypotheses, 0..) |hypothesis, label_index| {
                const pair_index = text_index * labels.len + label_index;
                encoded[pair_index] = try self.tok.encodeForPair(
                    alloc,
                    text,
                    hypothesis,
                    self.config.max_length,
                );
                encoded_count += 1;
                const active_tokens = activeTokenLength(encoded[pair_index].attention_mask);
                prepared.max_input_tokens_per_item = @max(
                    prepared.max_input_tokens_per_item,
                    active_tokens,
                );
                for (encoded[pair_index].attention_mask) |mask| {
                    if (mask != 0) {
                        prepared.prompt_tokens = std.math.add(
                            usize,
                            prepared.prompt_tokens,
                            1,
                        ) catch return error.ClassificationBatchTooLarge;
                    }
                }
            }
        }
        if (admit) {
            const fixed_len = hasFixedTextSequenceLength(self.session.inputInfo());
            const effective_len = if (self.config.trim_padding_to_batch_max and !fixed_len)
                @max(@as(usize, 1), prepared.max_input_tokens_per_item)
            else
                self.config.max_length;
            prepared.run_permit = try self.admitPreparedRows(total_pairs, effective_len);
        }
        return prepared;
    }

    /// Returns the largest exact non-padding model row in this request. Each
    /// row is a text paired with one fully formatted label hypothesis.
    pub fn maxInputTokensPerItem(
        self: *ClassificationPipeline,
        texts: []const []const u8,
        labels: []const []const u8,
    ) !usize {
        var prepared = try self.prepareInputsInternal(texts, labels, false);
        defer prepared.deinit();
        return prepared.max_input_tokens_per_item;
    }

    /// Classify a batch and report the exact number of non-padding tokens sent
    /// to the NLI model across every text-label hypothesis pair.
    pub fn classifyBatchWithPromptTokens(
        self: *ClassificationPipeline,
        texts: []const []const u8,
        labels: []const []const u8,
        prompt_tokens: *usize,
    ) ![][]ClassificationResult {
        var prepared = try self.prepareInputs(texts, labels);
        defer prepared.deinit();
        prompt_tokens.* = prepared.prompt_tokens;
        return self.classifyPrepared(&prepared);
    }

    pub fn classifyPrepared(
        self: *ClassificationPipeline,
        prepared: *PreparedClassificationInputs,
    ) ![][]ClassificationResult {
        const alloc = self.allocator;
        if (prepared.owner_session_ptr != self.session.ptr or
            prepared.owner_session_vtable != self.session.vtable or
            prepared.owner_tokenizer_ptr != self.tok.ptr or
            prepared.owner_tokenizer_vtable != self.tok.vtable or
            !std.mem.eql(u8, prepared.hypothesis_template, self.config.hypothesis_template) or
            !prepared.labelsMatchSemantics() or
            prepared.max_length != self.config.max_length or
            prepared.multi_label != self.config.multi_label or
            prepared.entailment_index != self.config.entailment_index or
            prepared.trim_padding_to_batch_max != self.config.trim_padding_to_batch_max)
            return error.InvalidPreparedClassificationInputs;
        const labels = prepared.labels;
        const total_pairs = std.math.mul(usize, prepared.text_count, labels.len) catch
            return error.InvalidPreparedClassificationInputs;
        if (total_pairs != prepared.encoded.len)
            return error.InvalidPreparedClassificationInputs;

        const results = try alloc.alloc([]ClassificationResult, prepared.text_count);
        var initialized: usize = 0;
        errdefer {
            for (results[0..initialized]) |r| alloc.free(r);
            alloc.free(results);
        }

        if (labels.len == 0) {
            for (results) |*row| {
                row.* = try alloc.alloc(ClassificationResult, 0);
                initialized += 1;
            }
            return results;
        }

        if (prepared.text_count == 0) return results;

        const max_len = prepared.max_length;
        const run_permit = if (prepared.run_permit) |*permit| permit else return error.InvalidPreparedClassificationInputs;

        const fixed_len = hasFixedTextSequenceLength(self.session.inputInfo());
        const trim_padding = self.config.trim_padding_to_batch_max and !fixed_len;
        var effective_len: usize = if (trim_padding) 1 else max_len;
        if (trim_padding) {
            for (prepared.encoded) |item| {
                effective_len = @max(effective_len, activeTokenLength(item.attention_mask));
            }
        }

        const input_len = std.math.mul(usize, total_pairs, effective_len) catch
            return error.ClassificationBatchTooLarge;
        const all_ids = try alloc.alloc(i32, input_len);
        defer alloc.free(all_ids);
        const all_mask = try alloc.alloc(i32, input_len);
        defer alloc.free(all_mask);

        for (0..prepared.text_count) |text_i| {
            for (0..labels.len) |label_i| {
                const pair_i = text_i * labels.len + label_i;
                const result = prepared.encoded[pair_i];
                if (result.ids.len < effective_len or result.attention_mask.len < effective_len)
                    return error.UnexpectedInputShape;
                @memcpy(
                    all_ids[pair_i * effective_len .. (pair_i + 1) * effective_len],
                    result.ids[0..effective_len],
                );
                @memcpy(
                    all_mask[pair_i * effective_len .. (pair_i + 1) * effective_len],
                    result.attention_mask[0..effective_len],
                );
            }
        }

        const ids_i64 = try alloc.alloc(i64, input_len);
        defer alloc.free(ids_i64);
        const mask_i64 = try alloc.alloc(i64, input_len);
        defer alloc.free(mask_i64);
        for (0..input_len) |j| {
            ids_i64[j] = @intCast(all_ids[j]);
            mask_i64[j] = @intCast(all_mask[j]);
        }

        const shape = [_]i64{ @intCast(total_pairs), @intCast(effective_len) };
        var input_ids_tensor = try Tensor.initInt64(alloc, "input_ids", &shape, ids_i64);
        defer input_ids_tensor.deinit();
        var attention_mask_tensor = try Tensor.initInt64(alloc, "attention_mask", &shape, mask_i64);
        defer attention_mask_tensor.deinit();

        var token_type_tensor: ?Tensor = null;
        defer if (token_type_tensor) |*t| t.deinit();

        const input_info = self.session.inputInfo();
        var needs_token_type = false;
        for (input_info) |info| {
            if (std.mem.eql(u8, info.name, "token_type_ids")) {
                needs_token_type = true;
                break;
            }
        }

        const inputs = if (needs_token_type) blk: {
            const type_ids = try alloc.alloc(i64, input_len);
            defer alloc.free(type_ids);
            for (0..total_pairs) |b| {
                var in_segment_b = false;
                var sep_count: usize = 0;
                for (0..effective_len) |s| {
                    const idx = b * effective_len + s;
                    if (all_mask[idx] == 0) {
                        type_ids[idx] = 0;
                    } else if (in_segment_b) {
                        type_ids[idx] = 1;
                    } else {
                        type_ids[idx] = 0;
                        if (all_ids[idx] == self.tok.specialTokens().sep_id) {
                            sep_count += 1;
                            if (sep_count == 1) in_segment_b = true;
                        }
                    }
                }
            }
            token_type_tensor = try Tensor.initInt64(alloc, "token_type_ids", &shape, type_ids);
            break :blk &[_]Tensor{ input_ids_tensor, attention_mask_tensor, token_type_tensor.? };
        } else &[_]Tensor{ input_ids_tensor, attention_mask_tensor };

        var outputs = try run_permit.runWithControl(inputs, alloc, self.execution_control);
        defer {
            for (outputs) |*o| o.deinit();
            alloc.free(outputs);
        }

        if (outputs.len == 0) return error.NoOutputTensors;

        const raw_scores = try self.extractEntailmentScores(&outputs[0], total_pairs);
        defer alloc.free(raw_scores);

        const scores = try alloc.alloc(f32, labels.len);
        defer alloc.free(scores);

        for (0..prepared.text_count) |text_i| {
            const raw_slice = raw_scores[text_i * labels.len .. (text_i + 1) * labels.len];
            if (self.config.multi_label) {
                for (raw_slice, 0..) |raw_score, label_i| {
                    scores[label_i] = sigmoid(raw_score);
                }
            } else {
                softmax(raw_slice, scores);
            }

            const row = try alloc.alloc(ClassificationResult, labels.len);
            for (labels, 0..) |label, label_i| {
                row[label_i] = .{
                    .label = label,
                    .score = scores[label_i],
                };
            }
            std.mem.sort(ClassificationResult, row, {}, struct {
                fn lessThan(_: void, a: ClassificationResult, b: ClassificationResult) bool {
                    return a.score > b.score;
                }
            }.lessThan);
            results[text_i] = row;
            initialized += 1;
        }

        return results;
    }

    fn admitPreparedRows(self: *ClassificationPipeline, total_pairs: usize, sequence: usize) !session_mod.RunPermit {
        const admitted_input_len = std.math.mul(usize, total_pairs, sequence) catch
            return error.ClassificationBatchTooLarge;
        return self.session.admit(.{
            .batch = total_pairs,
            .sequence = sequence,
            .input_bytes = std.math.mul(usize, admitted_input_len, 24) catch
                return error.ResourceLimitExceeded,
        });
    }

    fn formatHypothesis(self: *ClassificationPipeline, label: []const u8) ![]const u8 {
        const template = self.config.hypothesis_template;
        // Find "{}" placeholder
        if (std.mem.indexOf(u8, template, "{}")) |pos| {
            return std.fmt.allocPrint(self.allocator, "{s}{s}{s}", .{
                template[0..pos],
                label,
                template[pos + 2 ..],
            });
        }
        // No placeholder — just append label
        return std.fmt.allocPrint(self.allocator, "{s} {s}", .{ template, label });
    }

    /// Extract entailment logits from NLI model output.
    /// NLI models output [batch, num_classes]. The entailment index varies by model:
    ///   BART-MNLI: class 0=contradiction, 1=neutral, 2=entailment
    ///   mDeBERTa-MNLI: class 0=entailment, 1=neutral, 2=contradiction
    /// Use config.entailment_index to specify, or defaults to last class.
    fn extractEntailmentScores(self: *ClassificationPipeline, output: *const Tensor, batch: usize) ![]f32 {
        const data = output.asFloat32();
        const shape = output.shape;
        const scores = try self.allocator.alloc(f32, batch);

        if (shape.len == 2) {
            const num_classes: usize = @intCast(shape[1]);
            if (num_classes >= 3) {
                const ent_idx = self.config.entailment_index orelse (num_classes - 1);
                for (0..batch) |b| {
                    scores[b] = data[b * num_classes + ent_idx];
                }
            } else if (num_classes == 2) {
                // Some models only have [not_entail, entail]
                for (0..batch) |b| {
                    scores[b] = data[b * num_classes + 1];
                }
            } else {
                // Single logit
                for (0..batch) |b| {
                    scores[b] = data[b];
                }
            }
        } else if (shape.len == 1) {
            @memcpy(scores, data[0..batch]);
        } else {
            return error.UnexpectedOutputShape;
        }

        return scores;
    }
};

fn activeTokenLength(mask: []const i32) usize {
    var last_active: usize = 0;
    var found = false;
    for (mask, 0..) |value, idx| {
        if (value > 0) {
            last_active = idx;
            found = true;
        }
    }
    return if (found) last_active + 1 else @min(mask.len, 1);
}

fn hasFixedTextSequenceLength(input_info: []const backends.TensorInfo) bool {
    for (input_info) |info| {
        if (!std.mem.eql(u8, info.name, "input_ids")) continue;
        return info.shape.len >= 2 and info.shape[1] > 0;
    }
    return false;
}

fn sigmoid(x: f32) f32 {
    return 1.0 / (1.0 + @exp(-x));
}

fn softmax(input: []const f32, output: []f32) void {
    var max_val: f32 = -std.math.inf(f32);
    for (input) |v| {
        if (v > max_val) max_val = v;
    }
    var sum: f32 = 0.0;
    for (input, 0..) |v, i| {
        output[i] = @exp(v - max_val);
        sum += output[i];
    }
    if (sum > 0.0) {
        for (output) |*v| v.* /= sum;
    }
}

test "classifyBatch runs all text-label pairs in one session batch" {
    const allocator = std.testing.allocator;

    var fake_session = FakeClassificationSession{};
    var fake_tokenizer = FakeClassificationTokenizer{};
    var pipeline = ClassificationPipeline.init(allocator, fake_session.session(), fake_tokenizer.tokenizer(), .{
        .max_length = 8,
    });

    const texts = [_][]const u8{ "first", "second", "third" };
    const labels = [_][]const u8{ "negative", "positive" };
    var prepared = try pipeline.prepareInputs(&texts, &labels);
    defer prepared.deinit();
    try std.testing.expectEqual(@as(usize, 5), prepared.max_input_tokens_per_item);
    try std.testing.expectEqual(@as(usize, 30), prepared.prompt_tokens);
    const encode_calls = fake_tokenizer.encode_calls;

    const results = try pipeline.classifyPrepared(&prepared);
    defer {
        for (results) |row| allocator.free(row);
        allocator.free(results);
    }

    try std.testing.expectEqual(encode_calls, fake_tokenizer.encode_calls);
    try std.testing.expectEqual(@as(usize, 1), fake_session.run_count);
    try std.testing.expectEqual(@as(usize, 6), fake_session.last_batch);
    try std.testing.expectEqual(@as(usize, 5), fake_session.last_sequence);
    try std.testing.expectEqual(@as(usize, texts.len), results.len);
    for (results) |row| {
        try std.testing.expectEqual(@as(usize, labels.len), row.len);
        try std.testing.expectEqualStrings("positive", row[0].label);
    }
}

test "classification trims dynamic padding but preserves fixed input shapes" {
    const allocator = std.testing.allocator;

    var fake_session = FakeClassificationSession{ .fixed_sequence = true };
    var fake_tokenizer = FakeClassificationTokenizer{};
    var pipeline = ClassificationPipeline.init(
        allocator,
        fake_session.session(),
        fake_tokenizer.tokenizer(),
        .{ .max_length = 8 },
    );

    const results = try pipeline.classifyBatch(&.{"first"}, &.{"positive"});
    defer {
        for (results) |row| allocator.free(row);
        allocator.free(results);
    }

    try std.testing.expectEqual(@as(usize, 1), fake_session.run_count);
    try std.testing.expectEqual(@as(usize, 8), fake_session.last_sequence);
}

test "classification admission rejects before tokenization" {
    const memory = @import("../runtime/tier/memory.zig");
    var controller = memory.AdmissionController{};
    controller.configureForcedRunDenialsForTesting(1);

    var fake_session = FakeClassificationSession{};
    var admitted_session = fake_session.session();
    admitted_session.run_admission = .{
        .controller = &controller,
        .backend_class = .cpu,
        .limits = .{},
        .static_workspace_bytes = 1,
    };
    var fake_tokenizer = FakeClassificationTokenizer{};
    var pipeline = ClassificationPipeline.init(
        std.testing.allocator,
        admitted_session,
        fake_tokenizer.tokenizer(),
        .{ .max_length = 8 },
    );

    try std.testing.expectError(
        error.ResourceTemporarilyUnavailable,
        pipeline.classifyBatch(&.{"first"}, &.{"positive"}),
    );
    try std.testing.expectEqual(@as(usize, 0), fake_tokenizer.encode_calls);
}

test "prepared classification inputs are bound to their pipeline generation" {
    var first_session = FakeClassificationSession{};
    var second_session = FakeClassificationSession{};
    var tokenizer = FakeClassificationTokenizer{};
    var first = ClassificationPipeline.init(
        std.testing.allocator,
        first_session.session(),
        tokenizer.tokenizer(),
        .{ .max_length = 8 },
    );
    var second = ClassificationPipeline.init(
        std.testing.allocator,
        second_session.session(),
        tokenizer.tokenizer(),
        .{ .max_length = 8 },
    );
    var prepared = try first.prepareInputs(&.{"first"}, &.{"positive"});
    defer prepared.deinit();

    try std.testing.expectError(
        error.InvalidPreparedClassificationInputs,
        second.classifyPrepared(&prepared),
    );
    first.config.multi_label = true;
    try std.testing.expectError(
        error.InvalidPreparedClassificationInputs,
        first.classifyPrepared(&prepared),
    );
}

test "prepared classification inputs reject a different tokenizer on the same session" {
    var session_state = FakeClassificationSession{};
    var first_tokenizer = FakeClassificationTokenizer{};
    var second_tokenizer = FakeClassificationTokenizer{};
    var first = ClassificationPipeline.init(
        std.testing.allocator,
        session_state.session(),
        first_tokenizer.tokenizer(),
        .{ .max_length = 8 },
    );
    var second = ClassificationPipeline.init(
        std.testing.allocator,
        session_state.session(),
        second_tokenizer.tokenizer(),
        .{ .max_length = 8 },
    );
    var prepared = try first.prepareInputs(&.{"first"}, &.{"positive"});
    defer prepared.deinit();

    try std.testing.expectError(
        error.InvalidPreparedClassificationInputs,
        second.classifyPrepared(&prepared),
    );
}

test "prepared classification inputs reject in-place hypothesis template mutation" {
    var session_state = FakeClassificationSession{};
    var tokenizer_state = FakeClassificationTokenizer{};
    var template = [_]u8{ 'T', 'h', 'i', 's', ' ', 'i', 's', ' ', '{', '}', '.' };
    var pipeline = ClassificationPipeline.init(
        std.testing.allocator,
        session_state.session(),
        tokenizer_state.tokenizer(),
        .{
            .max_length = 8,
            .hypothesis_template = &template,
        },
    );
    var prepared = try pipeline.prepareInputs(&.{"first"}, &.{"positive"});
    defer prepared.deinit();

    template[0] = 't';
    try std.testing.expectError(
        error.InvalidPreparedClassificationInputs,
        pipeline.classifyPrepared(&prepared),
    );
}

test "prepared classification inputs reject label reorder and content mutation" {
    var session_state = FakeClassificationSession{};
    var tokenizer_state = FakeClassificationTokenizer{};
    var first_label = [_]u8{ 'f', 'i', 'r', 's', 't' };
    var second_label = [_]u8{ 'o', 't', 'h', 'e', 'r' };
    var labels = [_][]const u8{ &first_label, &second_label };
    var pipeline = ClassificationPipeline.init(
        std.testing.allocator,
        session_state.session(),
        tokenizer_state.tokenizer(),
        .{ .max_length = 8 },
    );
    var prepared = try pipeline.prepareInputs(&.{"text"}, &labels);
    defer prepared.deinit();

    std.mem.swap([]const u8, &labels[0], &labels[1]);
    try std.testing.expectError(
        error.InvalidPreparedClassificationInputs,
        pipeline.classifyPrepared(&prepared),
    );

    std.mem.swap([]const u8, &labels[0], &labels[1]);
    first_label[0] = 'F';
    try std.testing.expectError(
        error.InvalidPreparedClassificationInputs,
        pipeline.classifyPrepared(&prepared),
    );
}

const FakeClassificationSession = struct {
    run_count: usize = 0,
    last_batch: usize = 0,
    last_sequence: usize = 0,
    fixed_sequence: bool = false,

    fn session(self: *FakeClassificationSession) backends.Session {
        return .{
            .ptr = self,
            .vtable = &.{
                .run = run,
                .inputInfo = inputInfo,
                .outputInfo = outputInfo,
                .backend = backend,
                .close = close,
            },
        };
    }

    fn run(ptr: *anyopaque, inputs: []const Tensor, allocator: std.mem.Allocator) anyerror![]Tensor {
        const self: *FakeClassificationSession = @ptrCast(@alignCast(ptr));
        try std.testing.expectEqual(@as(usize, 2), inputs.len);
        try std.testing.expectEqual(@as(usize, 2), inputs[0].shape.len);
        const batch: usize = @intCast(inputs[0].shape[0]);
        const sequence: usize = @intCast(inputs[0].shape[1]);
        self.run_count += 1;
        self.last_batch = batch;
        self.last_sequence = sequence;

        const logits = try allocator.alloc(f32, batch * 3);
        defer allocator.free(logits);
        for (0..batch) |i| {
            logits[i * 3 + 0] = 0.0;
            logits[i * 3 + 1] = 0.0;
            logits[i * 3 + 2] = @floatFromInt(i);
        }

        const out = try allocator.alloc(Tensor, 1);
        out[0] = try Tensor.initFloat32(allocator, "logits", &.{ @intCast(batch), 3 }, logits);
        return out;
    }

    fn inputInfo(ptr: *anyopaque) []const backends.TensorInfo {
        const self: *FakeClassificationSession = @ptrCast(@alignCast(ptr));
        return if (self.fixed_sequence)
            &.{
                .{ .name = "input_ids", .dtype = .i64, .shape = &.{ -1, 8 } },
                .{ .name = "attention_mask", .dtype = .i64, .shape = &.{ -1, 8 } },
            }
        else
            &.{
                .{ .name = "input_ids", .dtype = .i64, .shape = &.{ -1, -1 } },
                .{ .name = "attention_mask", .dtype = .i64, .shape = &.{ -1, -1 } },
            };
    }

    fn outputInfo(_: *anyopaque) []const backends.TensorInfo {
        return &.{.{ .name = "logits", .dtype = .f32, .shape = &.{ -1, 3 } }};
    }

    fn backend(_: *anyopaque) backends.BackendType {
        return .native;
    }

    fn close(_: *anyopaque) void {}
};

const FakeClassificationTokenizer = struct {
    encode_calls: usize = 0,

    fn tokenizer(self: *FakeClassificationTokenizer) Tokenizer {
        return .{
            .ptr = self,
            .vtable = &.{
                .encode = encode,
                .encodeInto = encodeInto,
                .encodeForModel = encodeForModel,
                .encodeGeneration = encodeGeneration,
                .decode = decode,
                .specialTokens = specialTokens,
                .vocabSize = vocabSize,
                .deinit = deinit,
            },
        };
    }

    fn encode(ptr: *anyopaque, allocator: std.mem.Allocator, text: []const u8) anyerror![]i32 {
        const self: *FakeClassificationTokenizer = @ptrCast(@alignCast(ptr));
        self.encode_calls += 1;
        const ids = try allocator.alloc(i32, 1);
        ids[0] = if (text.len == 0) 1 else @as(i32, @intCast(text[0]));
        return ids;
    }

    fn encodeInto(ptr: *anyopaque, allocator: std.mem.Allocator, text: []const u8, out: *std.ArrayListUnmanaged(i32)) anyerror!void {
        const ids = try encode(ptr, allocator, text);
        defer allocator.free(ids);
        try out.appendSlice(allocator, ids);
    }

    fn encodeForModel(ptr: *anyopaque, allocator: std.mem.Allocator, text: []const u8, max_length: usize) anyerror!@import("inference_tokenizer").EncodeResult {
        const tok = Tokenizer{ .ptr = ptr, .vtable = &.{
            .encode = encode,
            .encodeInto = encodeInto,
            .encodeForModel = encodeForModel,
            .encodeGeneration = encodeGeneration,
            .decode = decode,
            .specialTokens = specialTokens,
            .vocabSize = vocabSize,
            .deinit = deinit,
        } };
        return tok.encodeForGenerationFallback(allocator, text, max_length, true);
    }

    fn encodeGeneration(ptr: *anyopaque, allocator: std.mem.Allocator, text: []const u8, max_length: usize, add_bos_token: bool) anyerror!@import("inference_tokenizer").EncodeResult {
        const tok = Tokenizer{ .ptr = ptr, .vtable = &.{
            .encode = encode,
            .encodeInto = encodeInto,
            .encodeForModel = encodeForModel,
            .encodeGeneration = encodeGeneration,
            .decode = decode,
            .specialTokens = specialTokens,
            .vocabSize = vocabSize,
            .deinit = deinit,
        } };
        return tok.encodeForGenerationFallback(allocator, text, max_length, add_bos_token);
    }

    fn decode(_: *anyopaque, allocator: std.mem.Allocator, _: []const i32) anyerror![]u8 {
        return allocator.dupe(u8, "");
    }

    fn specialTokens(_: *anyopaque) @import("inference_tokenizer").SpecialTokens {
        return .{ .cls_id = 101, .sep_id = 102, .pad_id = 0 };
    }

    fn vocabSize(_: *anyopaque) usize {
        return 256;
    }

    fn deinit(_: *anyopaque) void {}
};
