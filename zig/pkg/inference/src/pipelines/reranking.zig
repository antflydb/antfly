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
const backends = @import("../backends/backends.zig");
const tokenizer_mod = @import("termite_tokenizer");
const Tokenizer = tokenizer_mod.Tokenizer;
const Tensor = backends.Tensor;
const runtime = @import("../runtime/root.zig");

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

    pub fn usesDistributedMlx(self: *const RerankingPipeline) bool {
        return self.config.distributed.enabled and
            self.config.distributed.world_size > 1 and
            self.session.backend().usesGpuHostedSession();
    }

    pub fn usesTensorParallelMlx(self: *const RerankingPipeline) bool {
        return self.usesDistributedMlx() and self.config.distributed.mode == .tensor_parallel;
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

        const all_ids = try alloc.alloc(i32, batch * max_len);
        defer alloc.free(all_ids);
        const all_mask = try alloc.alloc(i32, batch * max_len);
        defer alloc.free(all_mask);
        const all_type_ids = try alloc.alloc(i64, batch * max_len);
        defer alloc.free(all_type_ids);

        for (documents, 0..) |doc, i| {
            var result = try self.tok.encodeForPair(alloc, query, doc, max_len);
            defer result.deinit();

            @memcpy(all_ids[i * max_len .. (i + 1) * max_len], result.ids);
            @memcpy(all_mask[i * max_len .. (i + 1) * max_len], result.attention_mask);
            self.buildCrossEncoderTokenTypes(
                all_type_ids[i * max_len .. (i + 1) * max_len],
                result.ids,
                result.attention_mask,
            );
        }

        var run = try self.runTextEncoder(all_ids, all_mask, all_type_ids, batch, max_len, true);
        defer run.deinit();

        return try self.extractScores(run.output(), batch);
    }

    fn rerankLateInteraction(self: *RerankingPipeline, query: []const u8, documents: []const []const u8) ![]f32 {
        const alloc = self.allocator;
        const max_len = self.config.max_length;
        const special = self.tok.specialTokens();
        const chunk_size = @max(@as(usize, 1), self.config.batch_size);

        var query_encoded = try self.encodeSingleText(query);
        defer query_encoded.deinit();

        const query_type_ids = try alloc.alloc(i64, max_len);
        defer alloc.free(query_type_ids);
        @memset(query_type_ids, 0);

        var query_run = try self.runTextEncoder(query_encoded.ids, query_encoded.attention_mask, query_type_ids, 1, max_len, false);
        defer query_run.deinit();

        const query_output = query_run.output();
        if (query_output.shape.len != 3) return error.UnexpectedOutputShape;
        const hidden: usize = @intCast(query_output.shape[2]);

        const scores = try alloc.alloc(f32, documents.len);

        var offset: usize = 0;
        while (offset < documents.len) {
            const chunk_len = @min(chunk_size, documents.len - offset);
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

            var doc_run = try self.runTextEncoder(doc_ids, doc_mask, doc_type_ids, chunk_len, max_len, false);
            defer doc_run.deinit();
            const doc_output = doc_run.output();
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
        } else {
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

        fn output(self: *const TextRun) *const Tensor {
            if (self.outputs.len == 0) @panic("TextRun.output called without outputs");
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
    ) !TextRun {
        const alloc = self.allocator;
        const ids_i64 = try alloc.alloc(i64, batch * max_len);
        defer alloc.free(ids_i64);
        const mask_i64 = try alloc.alloc(i64, batch * max_len);
        defer alloc.free(mask_i64);

        for (0..batch * max_len) |j| {
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
            .outputs = try self.session.run(inputs, alloc),
        };
    }

    fn encodeSingleText(self: *RerankingPipeline, text: []const u8) !@import("termite_tokenizer").EncodeResult {
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

fn sigmoid(x: f32) f32 {
    return 1.0 / (1.0 + @exp(-x));
}
