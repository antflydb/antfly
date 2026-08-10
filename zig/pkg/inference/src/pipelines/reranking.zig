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

// Reranking pipeline: cross-encoder scoring of query-document pairs.
//
// Accepts a query and list of documents, returns relevance scores.
// Uses [CLS] query [SEP] document [SEP] tokenization for cross-encoders.

const std = @import("std");
const platform = @import("antfly_platform");
const backends = @import("../backends/backends.zig");
const tokenizer_mod = @import("inference_tokenizer");
const Tokenizer = tokenizer_mod.Tokenizer;
const Tensor = backends.Tensor;
const runtime = @import("../runtime/root.zig");
const session_mod = @import("../backends/session.zig");

pub const ScoringMode = enum {
    cross_encoder,
    late_interaction,
};

pub const SingleTextEncoding = enum {
    encoder,
    generation,
};

pub const RerankingConfig = struct {
    max_length: usize = 512,
    batch_size: usize = 32,
    mode: ScoringMode = .cross_encoder,
    single_text_encoding: SingleTextEncoding = .encoder,
    add_bos_token: bool = false,
    /// Dynamic text encoders should execute only through the longest active
    /// pair in the batch rather than paying for max_length padding.
    trim_padding_to_batch_max: bool = true,
    distributed: runtime.distributed.Config = .{},
};

pub const RankedResult = struct {
    index: usize,
    score: f32,
};

pub const RerankingPipeline = struct {
    allocator: std.mem.Allocator,
    session: backends.Session,
    tok: Tokenizer,
    config: RerankingConfig,
    /// Optional caller-owned gate for stateful backend execution. Tokenization
    /// remains parallel; only the session forward pass is serialized.
    execution_lock: ?*std.atomic.Mutex = null,

    pub fn init(
        allocator: std.mem.Allocator,
        session: backends.Session,
        tok: Tokenizer,
        config: RerankingConfig,
    ) RerankingPipeline {
        return .{
            .allocator = allocator,
            .session = session,
            .tok = tok,
            .config = config,
        };
    }

    pub fn usesDistributedGpuHosted(self: *const RerankingPipeline) bool {
        return self.config.distributed.enabled and
            self.config.distributed.world_size > 1 and
            self.session.backend().usesGpuHostedSession();
    }

    pub fn usesTensorParallelGpuHosted(self: *const RerankingPipeline) bool {
        return self.usesDistributedGpuHosted() and self.config.distributed.mode == .tensor_parallel;
    }

    /// Score query-document pairs using the configured reranker mode.
    /// Returns scores in the same order as documents.
    pub fn rerank(self: *RerankingPipeline, query: []const u8, documents: []const []const u8) ![]f32 {
        if (documents.len == 0) return try self.allocator.alloc(f32, 0);

        return switch (self.config.mode) {
            .cross_encoder => self.rerankCrossEncoder(query, documents),
            .late_interaction => self.rerankLateInteraction(query, documents),
        };
    }

    fn rerankCrossEncoder(self: *RerankingPipeline, query: []const u8, documents: []const []const u8) ![]f32 {
        const alloc = self.allocator;
        const max_len = self.config.max_length;
        const batch = documents.len;

        // Admission must cover tokenizer and packing buffers too. Reserve the
        // configured upper bound before allocating them; dynamic sessions may
        // still execute the shorter, batch-local sequence below.
        var run_permit = try self.admitTextRun(batch, max_len);
        defer run_permit.deinit();

        const encoded = try alloc.alloc(tokenizer_mod.EncodeResult, batch);
        defer alloc.free(encoded);
        var encoded_count: usize = 0;
        defer {
            for (encoded[0..encoded_count]) |*result| result.deinit();
        }

        const fixed_len = hasFixedTextSequenceLength(self.session.inputInfo());
        const trim_padding = self.config.trim_padding_to_batch_max and !fixed_len;
        var effective_len: usize = if (trim_padding) 1 else max_len;
        for (documents, 0..) |doc, i| {
            encoded[i] = try self.tok.encodeForPair(alloc, query, doc, max_len);
            encoded_count += 1;
            if (trim_padding) {
                effective_len = @max(effective_len, activeTokenLength(encoded[i].attention_mask));
            }
        }

        const element_count = std.math.mul(usize, batch, effective_len) catch
            return error.ResourceLimitExceeded;
        const all_ids = try alloc.alloc(i32, element_count);
        defer alloc.free(all_ids);
        const all_mask = try alloc.alloc(i32, element_count);
        defer alloc.free(all_mask);
        const all_type_ids = try alloc.alloc(i64, element_count);
        defer alloc.free(all_type_ids);

        for (encoded, 0..) |result, i| {
            if (result.ids.len < effective_len or result.attention_mask.len < effective_len)
                return error.UnexpectedInputShape;
            const row_ids = result.ids[0..effective_len];
            const row_mask = result.attention_mask[0..effective_len];
            @memcpy(all_ids[i * effective_len .. (i + 1) * effective_len], row_ids);
            @memcpy(all_mask[i * effective_len .. (i + 1) * effective_len], row_mask);
            self.buildCrossEncoderTokenTypes(
                all_type_ids[i * effective_len .. (i + 1) * effective_len],
                row_ids,
                row_mask,
            );
        }

        var run = try self.runTextEncoder(
            all_ids,
            all_mask,
            all_type_ids,
            batch,
            effective_len,
            true,
            &run_permit,
        );
        defer run.deinit();

        return try self.extractScores(try run.output(), batch);
    }

    fn rerankLateInteraction(self: *RerankingPipeline, query: []const u8, documents: []const []const u8) ![]f32 {
        const alloc = self.allocator;
        const max_len = self.config.max_length;
        const special = self.tok.specialTokens();
        const chunk_size = @max(@as(usize, 1), self.config.batch_size);

        var query_permit = try self.admitTextRun(1, max_len);
        defer query_permit.deinit();
        var query_encoded = try self.encodeSingleText(query);
        defer query_encoded.deinit();

        const query_type_ids = try alloc.alloc(i64, max_len);
        defer alloc.free(query_type_ids);
        @memset(query_type_ids, 0);

        var query_run = try self.runTextEncoder(
            query_encoded.ids,
            query_encoded.attention_mask,
            query_type_ids,
            1,
            max_len,
            false,
            &query_permit,
        );
        defer query_run.deinit();

        const query_output = try query_run.output();
        if (query_output.shape.len != 3) return error.UnexpectedOutputShape;
        const hidden: usize = @intCast(query_output.shape[2]);

        const scores = try alloc.alloc(f32, documents.len);

        var offset: usize = 0;
        while (offset < documents.len) {
            const chunk_len = @min(chunk_size, documents.len - offset);
            var doc_permit = try self.admitTextRun(chunk_len, max_len);
            defer doc_permit.deinit();
            const doc_ids = try alloc.alloc(i32, chunk_len * max_len);
            defer alloc.free(doc_ids);
            const doc_mask = try alloc.alloc(i32, chunk_len * max_len);
            defer alloc.free(doc_mask);
            const doc_type_ids = try alloc.alloc(i64, chunk_len * max_len);
            defer alloc.free(doc_type_ids);
            @memset(doc_type_ids, 0);

            for (documents[offset .. offset + chunk_len], 0..) |doc, local_idx| {
                var encoded = try self.encodeSingleText(doc);
                defer encoded.deinit();
                @memcpy(doc_ids[local_idx * max_len .. (local_idx + 1) * max_len], encoded.ids);
                @memcpy(doc_mask[local_idx * max_len .. (local_idx + 1) * max_len], encoded.attention_mask);
            }

            var doc_run = try self.runTextEncoder(
                doc_ids,
                doc_mask,
                doc_type_ids,
                chunk_len,
                max_len,
                false,
                &doc_permit,
            );
            defer doc_run.deinit();
            const doc_output = try doc_run.output();
            if (doc_output.shape.len != 3) return error.UnexpectedOutputShape;

            const query_hidden = query_output.asFloat32();
            const doc_hidden = doc_output.asFloat32();
            for (0..chunk_len) |local_idx| {
                scores[offset + local_idx] = lateInteractionScore(
                    query_hidden,
                    query_encoded.ids,
                    query_encoded.attention_mask,
                    doc_hidden[local_idx * max_len * hidden .. (local_idx + 1) * max_len * hidden],
                    doc_ids[local_idx * max_len .. (local_idx + 1) * max_len],
                    doc_mask[local_idx * max_len .. (local_idx + 1) * max_len],
                    hidden,
                    special,
                );
            }

            offset += chunk_len;
        }

        return scores;
    }

    /// Rerank and return results sorted by score descending.
    pub fn rerankSorted(self: *RerankingPipeline, query: []const u8, documents: []const []const u8) ![]RankedResult {
        const scores = try self.rerank(query, documents);
        defer self.allocator.free(scores);

        const results = try self.allocator.alloc(RankedResult, scores.len);
        for (scores, 0..) |score, i| {
            results[i] = .{ .index = i, .score = score };
        }

        // Sort by score descending
        std.mem.sort(RankedResult, results, {}, struct {
            fn lessThan(_: void, a: RankedResult, b: RankedResult) bool {
                return a.score > b.score;
            }
        }.lessThan);

        return results;
    }

    /// Extract relevance scores from model output.
    /// Cross-encoders output either [batch, 1] or [batch, num_labels].
    fn extractScores(self: *RerankingPipeline, output: *const Tensor, batch: usize) ![]f32 {
        const data = output.asFloat32();
        const shape = output.shape;

        if (shape.len == 2) {
            if (shape[0] < 0 or @as(usize, @intCast(shape[0])) != batch or shape[1] <= 0)
                return error.UnexpectedOutputShape;
            const num_labels: usize = @intCast(shape[1]);
            const expected = std.math.mul(usize, batch, num_labels) catch return error.UnexpectedOutputShape;
            if (data.len != expected) return error.UnexpectedOutputShape;
        } else if (shape.len == 1) {
            if (shape[0] < 0 or @as(usize, @intCast(shape[0])) != batch or data.len != batch)
                return error.UnexpectedOutputShape;
        } else if (shape.len == 3) {
            if (shape[0] < 0 or @as(usize, @intCast(shape[0])) != batch or shape[1] <= 0 or shape[2] <= 0)
                return error.UnexpectedOutputShape;
            const seq_len: usize = @intCast(shape[1]);
            const num_labels: usize = @intCast(shape[2]);
            const row = std.math.mul(usize, seq_len, num_labels) catch return error.UnexpectedOutputShape;
            const expected = std.math.mul(usize, batch, row) catch return error.UnexpectedOutputShape;
            if (data.len != expected) return error.UnexpectedOutputShape;
        } else {
            return error.UnexpectedOutputShape;
        }

        const scores = try self.allocator.alloc(f32, batch);

        if (shape.len == 2) {
            const num_labels: usize = @intCast(shape[1]);
            if (num_labels == 1) {
                // Single logit — apply sigmoid
                for (0..batch) |b| {
                    scores[b] = sigmoid(data[b]);
                }
            } else {
                // Multi-label — take softmax and use label 1 (relevant) score
                for (0..batch) |b| {
                    const offset = b * num_labels;
                    if (num_labels >= 2) {
                        // Softmax over labels, return P(relevant)
                        const logit_0 = data[offset];
                        const logit_1 = data[offset + 1];
                        const max_val = @max(logit_0, logit_1);
                        const exp_0 = @exp(logit_0 - max_val);
                        const exp_1 = @exp(logit_1 - max_val);
                        scores[b] = exp_1 / (exp_0 + exp_1);
                    } else {
                        scores[b] = sigmoid(data[offset]);
                    }
                }
            }
        } else if (shape.len == 1) {
            // [batch] — raw logits
            for (0..batch) |b| {
                scores[b] = sigmoid(data[b]);
            }
        } else if (shape.len == 3) {
            // 3D: [batch, seq, labels] — take [CLS] position
            const seq_len: usize = @intCast(shape[1]);
            const num_labels: usize = @intCast(shape[2]);
            for (0..batch) |b| {
                const offset = b * seq_len * num_labels; // [CLS] is position 0
                scores[b] = sigmoid(data[offset]);
            }
        }

        return scores;
    }

    const TextRun = struct {
        allocator: std.mem.Allocator,
        outputs: []Tensor,

        fn deinit(self: *TextRun) void {
            for (self.outputs) |*o| o.deinit();
            self.allocator.free(self.outputs);
        }

        fn output(self: *const TextRun) !*const Tensor {
            if (self.outputs.len == 0) return error.MissingModelOutput;
            return &self.outputs[0];
        }
    };

    fn runTextEncoder(
        self: *RerankingPipeline,
        all_ids: []const i32,
        all_mask: []const i32,
        token_type_ids: []const i64,
        batch: usize,
        max_len: usize,
        include_cross_segments: bool,
        permit: *session_mod.RunPermit,
    ) !TextRun {
        const alloc = self.allocator;
        const element_count = std.math.mul(usize, batch, max_len) catch
            return error.ResourceLimitExceeded;
        if (all_ids.len != element_count or all_mask.len != element_count or token_type_ids.len != element_count)
            return error.UnexpectedInputShape;
        const ids_i64 = try alloc.alloc(i64, element_count);
        defer alloc.free(ids_i64);
        const mask_i64 = try alloc.alloc(i64, element_count);
        defer alloc.free(mask_i64);

        for (0..element_count) |j| {
            ids_i64[j] = @intCast(all_ids[j]);
            mask_i64[j] = @intCast(all_mask[j]);
        }

        const shape = [_]i64{ @intCast(batch), @intCast(max_len) };
        var input_ids_tensor = try Tensor.initInt64(alloc, "input_ids", &shape, ids_i64);
        defer input_ids_tensor.deinit();
        var attention_mask_tensor = try Tensor.initInt64(alloc, "attention_mask", &shape, mask_i64);
        defer attention_mask_tensor.deinit();
        var token_type_tensor: ?Tensor = null;
        defer if (token_type_tensor) |*t| t.deinit();

        const input_info = self.session.inputInfo();
        var needs_attention_mask = false;
        var needs_token_type = false;
        for (input_info) |info| {
            if (std.mem.eql(u8, info.name, "attention_mask")) needs_attention_mask = true;
            if (std.mem.eql(u8, info.name, "token_type_ids")) needs_token_type = true;
        }

        const inputs = if (needs_token_type) blk: {
            if (!include_cross_segments) {
                // Late-interaction models should still receive a stable zero token_type_ids tensor
                // when the backend session expects it.
            }
            token_type_tensor = try Tensor.initInt64(alloc, "token_type_ids", &shape, token_type_ids);
            if (needs_attention_mask) {
                break :blk &[_]Tensor{ input_ids_tensor, attention_mask_tensor, token_type_tensor.? };
            }
            break :blk &[_]Tensor{ input_ids_tensor, token_type_tensor.? };
        } else if (needs_attention_mask) &[_]Tensor{ input_ids_tensor, attention_mask_tensor } else &[_]Tensor{input_ids_tensor};

        return .{
            .allocator = alloc,
            .outputs = try self.lockedSessionRun(permit, inputs, alloc),
        };
    }

    fn lockedSessionRun(
        self: *RerankingPipeline,
        permit: *session_mod.RunPermit,
        inputs: []const Tensor,
        allocator: std.mem.Allocator,
    ) ![]Tensor {
        if (self.execution_lock) |mutex| platform.sync.lockYielding(mutex);
        defer if (self.execution_lock) |mutex| mutex.unlock();
        return permit.run(inputs, allocator);
    }

    fn admitTextRun(
        self: *RerankingPipeline,
        batch: usize,
        sequence: usize,
    ) !session_mod.RunPermit {
        const tokens = std.math.mul(usize, batch, sequence) catch
            return error.ResourceLimitExceeded;
        return self.session.admit(.{
            .batch = batch,
            .sequence = sequence,
            .input_bytes = std.math.mul(usize, tokens, 24) catch
                return error.ResourceLimitExceeded,
            .host_preprocess_bytes = std.math.mul(usize, tokens, 32) catch
                return error.ResourceLimitExceeded,
        });
    }

    fn encodeSingleText(self: *RerankingPipeline, text: []const u8) !@import("inference_tokenizer").EncodeResult {
        return switch (self.config.single_text_encoding) {
            .encoder => self.tok.encodeForModel(self.allocator, text, self.config.max_length),
            .generation => self.tok.encodeForGenerationConfigured(self.allocator, text, self.config.max_length, self.config.add_bos_token),
        };
    }

    fn buildCrossEncoderTokenTypes(self: *RerankingPipeline, dst: []i64, ids: []const i32, attention_mask: []const i32) void {
        var in_segment_b = false;
        var sep_count: usize = 0;
        for (ids, attention_mask, 0..) |id, mask, idx| {
            if (mask == 0) {
                dst[idx] = 0;
            } else if (in_segment_b) {
                dst[idx] = 1;
            } else {
                dst[idx] = 0;
                if (id == self.tok.specialTokens().sep_id) {
                    sep_count += 1;
                    if (sep_count == 1) in_segment_b = true;
                }
            }
        }
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

test "cross encoder trims dynamic batches but preserves fixed input shapes" {
    const allocator = std.testing.allocator;
    var tokenizer_state = FakeRerankingTokenizer{};

    var dynamic_session = FakeRerankingSession{ .fixed_sequence = false };
    var dynamic_pipeline = RerankingPipeline.init(
        allocator,
        dynamic_session.session(),
        tokenizer_state.tokenizer(),
        .{ .max_length = 8 },
    );
    const documents = [_][]const u8{ "first", "second" };
    const dynamic_scores = try dynamic_pipeline.rerank("query", &documents);
    defer allocator.free(dynamic_scores);
    try std.testing.expectEqual(@as(usize, 2), dynamic_scores.len);
    try std.testing.expectEqual(@as(usize, 5), dynamic_session.last_sequence.load(.acquire));

    var fixed_session = FakeRerankingSession{ .fixed_sequence = true };
    var fixed_pipeline = RerankingPipeline.init(
        allocator,
        fixed_session.session(),
        tokenizer_state.tokenizer(),
        .{ .max_length = 8 },
    );
    const fixed_scores = try fixed_pipeline.rerank("query", &documents);
    defer allocator.free(fixed_scores);
    try std.testing.expectEqual(@as(usize, 8), fixed_session.last_sequence.load(.acquire));
}

test "cross encoder admission rejects before tokenization" {
    const memory = @import("../runtime/tier/memory.zig");
    var controller = memory.AdmissionController{};
    controller.configureForcedRunDenialsForTesting(1);

    var session_state = FakeRerankingSession{ .fixed_sequence = false };
    var admitted_session = session_state.session();
    admitted_session.run_admission = .{
        .controller = &controller,
        .backend_class = .cpu,
        .limits = .{},
        .static_workspace_bytes = 1,
    };
    var tokenizer_state = FakeRerankingTokenizer{};
    var pipeline = RerankingPipeline.init(
        std.testing.allocator,
        admitted_session,
        tokenizer_state.tokenizer(),
        .{ .max_length = 8 },
    );

    try std.testing.expectError(
        error.ResourceTemporarilyUnavailable,
        pipeline.rerank("query", &.{"document"}),
    );
    try std.testing.expectEqual(@as(usize, 0), tokenizer_state.encode_count.load(.acquire));
}

test "reranking execution gate blocks the session forward pass" {
    if (@import("builtin").single_threaded) return error.SkipZigTest;

    var gate: std.atomic.Mutex = .unlocked;
    try std.testing.expect(gate.tryLock());

    var session_state = FakeRerankingSession{ .fixed_sequence = false };
    var tokenizer_state = FakeRerankingTokenizer{};
    var pipeline = RerankingPipeline.init(
        std.heap.page_allocator,
        session_state.session(),
        tokenizer_state.tokenizer(),
        .{ .max_length = 8 },
    );
    pipeline.execution_lock = &gate;

    const Worker = struct {
        pipeline: *RerankingPipeline,
        started: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
        failed: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

        fn run(self: *@This()) void {
            self.started.store(true, .release);
            const scores = self.pipeline.rerank("query", &.{"document"}) catch {
                self.failed.store(true, .release);
                return;
            };
            self.pipeline.allocator.free(scores);
        }
    };

    var worker = Worker{ .pipeline = &pipeline };
    const thread = try std.Thread.spawn(.{}, Worker.run, .{&worker});
    while (!worker.started.load(.acquire)) std.Thread.yield() catch {};
    for (0..64) |_| std.Thread.yield() catch {};
    try std.testing.expectEqual(@as(usize, 0), session_state.run_count.load(.acquire));

    gate.unlock();
    thread.join();
    try std.testing.expect(!worker.failed.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), session_state.run_count.load(.acquire));
}

test "reranking score extraction rejects malformed output shapes" {
    var pipeline = RerankingPipeline{
        .allocator = std.testing.allocator,
        .session = undefined,
        .tok = undefined,
        .config = .{},
    };
    var output = try Tensor.initFloat32(std.testing.allocator, "logits", &.{ 1, 1 }, &.{0});
    defer output.deinit();
    try std.testing.expectError(error.UnexpectedOutputShape, pipeline.extractScores(&output, 2));
}

const FakeRerankingSession = struct {
    fixed_sequence: bool,
    run_count: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    last_sequence: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),

    fn session(self: *FakeRerankingSession) backends.Session {
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
        const self: *FakeRerankingSession = @ptrCast(@alignCast(ptr));
        const batch: usize = @intCast(inputs[0].shape[0]);
        const sequence: usize = @intCast(inputs[0].shape[1]);
        _ = self.run_count.fetchAdd(1, .acq_rel);
        self.last_sequence.store(sequence, .release);

        const logits = try allocator.alloc(f32, batch);
        defer allocator.free(logits);
        @memset(logits, 0.0);
        const out = try allocator.alloc(Tensor, 1);
        out[0] = try Tensor.initFloat32(allocator, "logits", &.{ @intCast(batch), 1 }, logits);
        return out;
    }

    fn inputInfo(ptr: *anyopaque) []const backends.TensorInfo {
        const self: *FakeRerankingSession = @ptrCast(@alignCast(ptr));
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
        return &.{.{ .name = "logits", .dtype = .f32, .shape = &.{ -1, 1 } }};
    }

    fn backend(_: *anyopaque) backends.BackendType {
        return .native;
    }

    fn close(_: *anyopaque) void {}
};

const FakeRerankingTokenizer = struct {
    encode_count: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),

    fn tokenizer(self: *FakeRerankingTokenizer) Tokenizer {
        return .{ .ptr = self, .vtable = vtable() };
    }

    fn vtable() *const Tokenizer.VTable {
        return &.{
            .encode = encode,
            .encodeInto = encodeInto,
            .encodeForModel = encodeForModel,
            .encodeGeneration = encodeGeneration,
            .decode = decode,
            .specialTokens = specialTokens,
            .vocabSize = vocabSize,
            .deinit = deinit,
        };
    }

    fn encode(ptr: *anyopaque, allocator: std.mem.Allocator, text: []const u8) anyerror![]i32 {
        const self: *FakeRerankingTokenizer = @ptrCast(@alignCast(ptr));
        _ = self.encode_count.fetchAdd(1, .acq_rel);
        const ids = try allocator.alloc(i32, 1);
        ids[0] = if (text.len == 0) 1 else @intCast(text[0]);
        return ids;
    }

    fn encodeInto(ptr: *anyopaque, allocator: std.mem.Allocator, text: []const u8, out: *std.ArrayListUnmanaged(i32)) anyerror!void {
        const ids = try encode(ptr, allocator, text);
        defer allocator.free(ids);
        try out.appendSlice(allocator, ids);
    }

    fn encodeForModel(ptr: *anyopaque, allocator: std.mem.Allocator, text: []const u8, max_length: usize) anyerror!tokenizer_mod.EncodeResult {
        const tok = Tokenizer{ .ptr = ptr, .vtable = vtable() };
        return tok.encodeForGenerationFallback(allocator, text, max_length, true);
    }

    fn encodeGeneration(ptr: *anyopaque, allocator: std.mem.Allocator, text: []const u8, max_length: usize, add_bos_token: bool) anyerror!tokenizer_mod.EncodeResult {
        const tok = Tokenizer{ .ptr = ptr, .vtable = vtable() };
        return tok.encodeForGenerationFallback(allocator, text, max_length, add_bos_token);
    }

    fn decode(_: *anyopaque, allocator: std.mem.Allocator, _: []const i32) anyerror![]u8 {
        return allocator.dupe(u8, "");
    }

    fn specialTokens(_: *anyopaque) tokenizer_mod.SpecialTokens {
        return .{ .cls_id = 101, .sep_id = 102, .pad_id = 0 };
    }

    fn vocabSize(_: *anyopaque) usize {
        return 256;
    }

    fn deinit(_: *anyopaque) void {}
};

test "reranking text run reports missing model output instead of panicking" {
    const run = RerankingPipeline.TextRun{
        .allocator = std.testing.allocator,
        .outputs = &.{},
    };

    try std.testing.expectError(error.MissingModelOutput, run.output());
}

pub fn lateInteractionScore(
    query_hidden: []const f32,
    query_ids: []const i32,
    query_mask: []const i32,
    doc_hidden: []const f32,
    doc_ids: []const i32,
    doc_mask: []const i32,
    hidden: usize,
    special: tokenizer_mod.SpecialTokens,
) f32 {
    var total: f32 = 0.0;
    const query_seq = query_ids.len;
    const doc_seq = doc_ids.len;

    for (0..query_seq) |q_idx| {
        if (!isInteractionToken(query_ids[q_idx], query_mask[q_idx], special)) continue;
        const q_vec = query_hidden[q_idx * hidden .. (q_idx + 1) * hidden];
        var best = -std.math.inf(f32);
        var found = false;
        for (0..doc_seq) |d_idx| {
            if (!isInteractionToken(doc_ids[d_idx], doc_mask[d_idx], special)) continue;
            const d_vec = doc_hidden[d_idx * hidden .. (d_idx + 1) * hidden];
            const sim = cosineSimilarity(q_vec, d_vec);
            if (!found or sim > best) {
                best = sim;
                found = true;
            }
        }
        if (found) total += best;
    }

    return total;
}

fn isInteractionToken(token_id: i32, mask: i32, special: tokenizer_mod.SpecialTokens) bool {
    if (mask == 0) return false;
    return token_id != special.cls_id and token_id != special.sep_id and token_id != special.pad_id;
}

fn cosineSimilarity(a: []const f32, b: []const f32) f32 {
    std.debug.assert(a.len == b.len);
    var dot: f32 = 0.0;
    var a_norm: f32 = 0.0;
    var b_norm: f32 = 0.0;
    for (a, b) |av, bv| {
        dot += av * bv;
        a_norm += av * av;
        b_norm += bv * bv;
    }
    if (a_norm <= 0.0 or b_norm <= 0.0) return 0.0;
    return dot / (@sqrt(a_norm) * @sqrt(b_norm));
}

test "late interaction maxsim ignores special tokens and sums query maxima" {
    const hidden: usize = 2;
    const special = tokenizer_mod.SpecialTokens{ .cls_id = 101, .sep_id = 102, .pad_id = 0, .unk_id = 100, .mask_id = 103 };
    const query_ids = [_]i32{ 101, 11, 12, 102, 0 };
    const query_mask = [_]i32{ 1, 1, 1, 1, 0 };
    const doc_ids = [_]i32{ 101, 22, 23, 102, 0 };
    const doc_mask = [_]i32{ 1, 1, 1, 1, 0 };
    const query_hidden = [_]f32{
        9.0, 9.0,
        1.0, 0.0,
        0.0, 1.0,
        9.0, 9.0,
        0.0, 0.0,
    };
    const doc_hidden = [_]f32{
        9.0, 9.0,
        1.0, 0.0,
        0.6, 0.8,
        9.0, 9.0,
        0.0, 0.0,
    };

    const score = lateInteractionScore(&query_hidden, &query_ids, &query_mask, &doc_hidden, &doc_ids, &doc_mask, hidden, special);
    try std.testing.expectApproxEqAbs(@as(f32, 1.8), score, 1e-4);
}

test "late interaction maxsim is insensitive to padded tail" {
    const hidden: usize = 2;
    const special = tokenizer_mod.SpecialTokens{};
    const query_ids = [_]i32{ 101, 11, 102, 0 };
    const query_mask = [_]i32{ 1, 1, 1, 0 };
    const doc_ids = [_]i32{ 101, 11, 102, 0 };
    const doc_mask = [_]i32{ 1, 1, 1, 0 };
    const query_hidden = [_]f32{ 5.0, 5.0, 1.0, 2.0, 5.0, 5.0, 99.0, 99.0 };
    const doc_hidden = [_]f32{ 5.0, 5.0, 1.0, 2.0, 5.0, 5.0, -99.0, -99.0 };

    const score = lateInteractionScore(&query_hidden, &query_ids, &query_mask, &doc_hidden, &doc_ids, &doc_mask, hidden, special);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), score, 1e-5);
}

test "reranking score extraction handles single-logit classifier output" {
    const allocator = std.testing.allocator;
    var pipeline: RerankingPipeline = undefined;
    pipeline.allocator = allocator;

    const shape = [_]i64{ 2, 1 };
    const logits = [_]f32{ 0.0, 2.0 };
    var output = try Tensor.initFloat32(allocator, "logits", &shape, &logits);
    defer output.deinit();

    const scores = try pipeline.extractScores(&output, 2);
    defer allocator.free(scores);

    try std.testing.expectApproxEqAbs(@as(f32, 0.5), scores[0], 1e-6);
    try std.testing.expectApproxEqAbs(sigmoid(2.0), scores[1], 1e-6);
}

test "reranking returns an empty score list for empty documents" {
    const allocator = std.testing.allocator;
    var pipeline: RerankingPipeline = undefined;
    pipeline.allocator = allocator;
    pipeline.config = .{};

    const scores = try pipeline.rerank("what is cuda", &.{});
    defer allocator.free(scores);
    try std.testing.expectEqual(@as(usize, 0), scores.len);
}

test "reranking score extraction handles two-label classifier output" {
    const allocator = std.testing.allocator;
    var pipeline: RerankingPipeline = undefined;
    pipeline.allocator = allocator;

    const shape = [_]i64{ 2, 2 };
    const logits = [_]f32{
        3.0,  1.0,
        -1.0, 2.0,
    };
    var output = try Tensor.initFloat32(allocator, "logits", &shape, &logits);
    defer output.deinit();

    const scores = try pipeline.extractScores(&output, 2);
    defer allocator.free(scores);

    try std.testing.expectApproxEqAbs(@as(f32, 0.11920292), scores[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.95257413), scores[1], 1e-6);
}

test "reranking score extraction handles legacy cls-sequence output" {
    const allocator = std.testing.allocator;
    var pipeline: RerankingPipeline = undefined;
    pipeline.allocator = allocator;

    const shape = [_]i64{ 2, 3, 1 };
    const logits = [_]f32{
        1.0,  99.0, 99.0,
        -2.0, 99.0, 99.0,
    };
    var output = try Tensor.initFloat32(allocator, "last_hidden_state", &shape, &logits);
    defer output.deinit();

    const scores = try pipeline.extractScores(&output, 2);
    defer allocator.free(scores);

    try std.testing.expectApproxEqAbs(sigmoid(1.0), scores[0], 1e-6);
    try std.testing.expectApproxEqAbs(sigmoid(-2.0), scores[1], 1e-6);
}

fn sigmoid(x: f32) f32 {
    return 1.0 / (1.0 + @exp(-x));
}
