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
const Tokenizer = @import("termite_tokenizer").Tokenizer;
const Tensor = backends.Tensor;
const runtime = @import("../runtime/root.zig");

pub const ClassificationConfig = struct {
    max_length: usize = 512,
    hypothesis_template: []const u8 = "This example is {}.",
    multi_label: bool = false,
    entailment_index: ?usize = null, // index of entailment class; auto-detect if null
    distributed: runtime.distributed.Config = .{},
};

pub const ClassificationResult = struct {
    label: []const u8,
    score: f32,
};

pub const ClassificationPipeline = struct {
    allocator: std.mem.Allocator,
    session: backends.Session,
    tok: Tokenizer,
    config: ClassificationConfig,

    pub fn usesDistributedMlx(self: *const ClassificationPipeline) bool {
        return self.config.distributed.enabled and
            self.config.distributed.world_size > 1 and
            self.session.backend().usesGpuHostedSession();
    }

    pub fn usesTensorParallelMlx(self: *const ClassificationPipeline) bool {
        return self.usesDistributedMlx() and self.config.distributed.mode == .tensor_parallel;
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
        const alloc = self.allocator;
        const results = try alloc.alloc([]ClassificationResult, texts.len);
        var initialized: usize = 0;
        errdefer {
            for (results[0..initialized]) |r| alloc.free(r);
            alloc.free(results);
        }

        if (labels.len == 0) {
            for (texts, 0..) |_, i| {
                results[i] = try alloc.alloc(ClassificationResult, 0);
                initialized += 1;
            }
            return results;
        }

        if (texts.len == 0) return results;

        const max_len = self.config.max_length;
        const total_pairs = std.math.mul(usize, texts.len, labels.len) catch return error.ClassificationBatchTooLarge;
        const input_len = std.math.mul(usize, total_pairs, max_len) catch return error.ClassificationBatchTooLarge;

        var hypotheses = try alloc.alloc([]const u8, labels.len);
        defer {
            for (hypotheses) |h| alloc.free(h);
            alloc.free(hypotheses);
        }
        for (labels, 0..) |label, i| {
            hypotheses[i] = try self.formatHypothesis(label);
        }

        const all_ids = try alloc.alloc(i32, input_len);
        defer alloc.free(all_ids);
        const all_mask = try alloc.alloc(i32, input_len);
        defer alloc.free(all_mask);

        for (texts, 0..) |text, text_i| {
            for (hypotheses, 0..) |hyp, label_i| {
                const pair_i = text_i * labels.len + label_i;
                var result = try self.tok.encodeForPair(alloc, text, hyp, max_len);
                defer result.deinit();
                @memcpy(all_ids[pair_i * max_len .. (pair_i + 1) * max_len], result.ids);
                @memcpy(all_mask[pair_i * max_len .. (pair_i + 1) * max_len], result.attention_mask);
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

        const shape = [_]i64{ @intCast(total_pairs), @intCast(max_len) };
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
                for (0..max_len) |s| {
                    const idx = b * max_len + s;
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

        var outputs = try self.session.run(inputs, alloc);
        defer {
            for (outputs) |*o| o.deinit();
            alloc.free(outputs);
        }

        if (outputs.len == 0) return error.NoOutputTensors;

        const raw_scores = try self.extractEntailmentScores(&outputs[0], total_pairs);
        defer alloc.free(raw_scores);

        const scores = try alloc.alloc(f32, labels.len);
        defer alloc.free(scores);

        for (texts, 0..) |_, text_i| {
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
    const results = try pipeline.classifyBatch(&texts, &labels);
    defer {
        for (results) |row| allocator.free(row);
        allocator.free(results);
    }

    try std.testing.expectEqual(@as(usize, 1), fake_session.run_count);
    try std.testing.expectEqual(@as(usize, 6), fake_session.last_batch);
    try std.testing.expectEqual(@as(usize, texts.len), results.len);
    for (results) |row| {
        try std.testing.expectEqual(@as(usize, labels.len), row.len);
        try std.testing.expectEqualStrings("positive", row[0].label);
    }
}

const FakeClassificationSession = struct {
    run_count: usize = 0,
    last_batch: usize = 0,

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
        self.run_count += 1;
        self.last_batch = batch;

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

    fn inputInfo(_: *anyopaque) []const backends.TensorInfo {
        return &.{
            .{ .name = "input_ids", .dtype = .i64, .shape = &.{ -1, 8 } },
            .{ .name = "attention_mask", .dtype = .i64, .shape = &.{ -1, 8 } },
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

    fn encode(_: *anyopaque, allocator: std.mem.Allocator, text: []const u8) anyerror![]i32 {
        const ids = try allocator.alloc(i32, 1);
        ids[0] = if (text.len == 0) 1 else @as(i32, @intCast(text[0]));
        return ids;
    }

    fn encodeInto(ptr: *anyopaque, allocator: std.mem.Allocator, text: []const u8, out: *std.ArrayListUnmanaged(i32)) anyerror!void {
        const ids = try encode(ptr, allocator, text);
        defer allocator.free(ids);
        try out.appendSlice(allocator, ids);
    }

    fn encodeForModel(ptr: *anyopaque, allocator: std.mem.Allocator, text: []const u8, max_length: usize) anyerror!@import("termite_tokenizer").EncodeResult {
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

    fn encodeGeneration(ptr: *anyopaque, allocator: std.mem.Allocator, text: []const u8, max_length: usize, add_bos_token: bool) anyerror!@import("termite_tokenizer").EncodeResult {
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

    fn specialTokens(_: *anyopaque) @import("termite_tokenizer").SpecialTokens {
        return .{ .cls_id = 101, .sep_id = 102, .pad_id = 0 };
    }

    fn vocabSize(_: *anyopaque) usize {
        return 256;
    }

    fn deinit(_: *anyopaque) void {}
};
