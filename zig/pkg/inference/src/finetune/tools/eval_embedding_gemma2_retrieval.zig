// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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

//! Evaluate a frozen or merged EmbeddingGemma 2 model on authenticated shards.
//!
//! Embeddings are produced from the exact prepared token IDs. Candidate vectors
//! are retained, but scores are streamed through bounded CPU batches; the tool
//! never materializes the query-by-corpus score matrix.

const std = @import("std");
const internal = @import("inference_internal");
const data = internal.finetune.embedding_gemma2_data;
const job = internal.finetune.embedding_gemma2_job;
const backends = internal.backends;
// The shared facade keeps loader types owned by one module in standalone
// tools, command tests, and the unified CLI, including core-only builds.
const model_manager = internal.server.model_manager;

const EvalSplit = enum { validation, @"test" };
const Backend = enum { cuda, native };

const Options = struct {
    job_path: []const u8,
    model_dir: []const u8,
    split: EvalSplit,
    backend: Backend = .cuda,
    embed_batch: usize = 32,
    score_batch: usize = 4096,
};

const Document = struct { aliases: std.ArrayListUnmanaged([]const u8) = .empty, digest: []const u8, ids: []const i32 };
const Query = struct { id: []const u8, group_id: []const u8, ids: []const i32, relevant: []const []const u8 };

const Corpus = struct {
    arena: std.heap.ArenaAllocator,
    documents: std.ArrayListUnmanaged(Document) = .empty,
    queries: std.ArrayListUnmanaged(Query) = .empty,

    fn deinit(self: *Corpus) void {
        self.documents.deinit(self.arena.allocator());
        self.queries.deinit(self.arena.allocator());
        self.arena.deinit();
    }
};

const Aggregate = struct {
    queries: usize = 0,
    mrr_at_10: f64 = 0,
    ndcg_at_10: f64 = 0,
    recall_at_100: f64 = 0,
};
const PerQuery = struct {
    mrr_at_10: []f64,
    ndcg_at_10: []f64,
    recall_at_100: []f64,
};
const Evaluation = struct {
    aggregate: Aggregate,
    per_query: PerQuery,
    fn deinit(self: Evaluation, allocator: std.mem.Allocator) void {
        allocator.free(self.per_query.mrr_at_10);
        allocator.free(self.per_query.ndcg_at_10);
        allocator.free(self.per_query.recall_at_100);
    }
};

pub fn main(init: std.process.Init) !void {
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, init.gpa);
    defer args.deinit();
    _ = args.next();
    const job_path = args.next() orelse return usage();
    if (std.mem.eql(u8, job_path, "--help")) return help();
    const model_dir = args.next() orelse return usage();
    var opts = Options{ .job_path = job_path, .model_dir = model_dir, .split = .validation };
    while (args.next()) |arg| {
        const value = args.next() orelse return usage();
        if (std.mem.eql(u8, arg, "--split")) opts.split = std.meta.stringToEnum(EvalSplit, value) orelse return error.InvalidEvalSplit else if (std.mem.eql(u8, arg, "--backend")) opts.backend = std.meta.stringToEnum(Backend, value) orelse return error.InvalidBackend else if (std.mem.eql(u8, arg, "--embed-batch")) opts.embed_batch = try boundedPositive(value, 256) else if (std.mem.eql(u8, arg, "--score-batch")) opts.score_batch = try boundedPositive(value, 1_000_000) else return usage();
    }

    const config_bytes = try std.Io.Dir.cwd().readFileAlloc(init.io, opts.job_path, init.gpa, .limited(1024 * 1024));
    defer init.gpa.free(config_bytes);
    var parsed = try std.json.parseFromSlice(job.Config, init.gpa, config_bytes, .{ .allocate = .alloc_always });
    defer parsed.deinit();
    try job.validate(parsed.value);
    var budget = internal.runtime.bounded_allocator.BoundedAllocator{ .backing = init.gpa, .limit = parsed.value.max_host_bytes };
    const allocator = budget.allocator();

    var dataset = try data.Dataset.open(allocator, init.io, parsed.value.prepared_dir);
    defer dataset.deinit();
    try verifyDatasetIdentity(dataset, parsed.value);
    const dataset_sha256 = try data.digestHex(allocator, dataset.manifest_sha256);
    defer allocator.free(dataset_sha256);
    var corpus = try loadCorpus(allocator, &dataset, opts.split);
    defer corpus.deinit();

    const control = internal.execution_control.InferenceExecutionControl{ .native_call_policy = .in_process };
    var sessions = backends.SessionManager.initWithIo(allocator, init.io);
    sessions.preferred_backends = switch (opts.backend) {
        .cuda => &.{.cuda},
        .native => &.{.native},
    };
    sessions.required_backend = switch (opts.backend) {
        .cuda => .cuda,
        .native => .native,
    };
    var models = model_manager.ModelManager.init(allocator, sessions);
    defer models.deinit();
    var handle = try models.acquireFromDirWithControl(opts.model_dir, control);
    defer handle.release();
    const model = handle.get();
    var lease = model.acquireEmbeddingAssetLease(false);
    defer lease.release();
    model.lockEmbeddingAssets();
    var pipeline = pipeline: {
        defer model.unlockEmbeddingAssets();
        try model.ensurePrimaryEmbeddingAssetsLockedWithControl(true, false, control);
        break :pipeline model.embeddingPipelineLocked(allocator);
    };

    pipeline.execution_control = control;

    const document_vectors = try embedDocuments(allocator, &pipeline, corpus.documents.items, opts.embed_batch);
    defer freeVectors(allocator, document_vectors);
    const query_vectors = try embedQueries(allocator, &pipeline, corpus.queries.items, opts.embed_batch);
    defer freeVectors(allocator, query_vectors);
    if (document_vectors.len == 0 or query_vectors.len == 0) return error.EmptyEvaluationSplit;

    var result: std.Io.Writer.Allocating = .init(allocator);
    defer result.deinit();
    try result.writer.writeAll("{\"format\":\"antfly.embeddinggemma2.retrieval-eval/v1\",\"qualification_scope\":\"bounded-candidate-corpus\",\"production_quality_eligible\":false,");
    try std.json.Stringify.value("model_dir", .{}, &result.writer);
    try result.writer.writeByte(':');
    try std.json.Stringify.value(opts.model_dir, .{}, &result.writer);
    try result.writer.writeAll(",\"dataset_manifest_sha256\":");
    try std.json.Stringify.value(dataset_sha256, .{}, &result.writer);
    try result.writer.writeAll(",\"public_source_manifest_sha256\":");
    try std.json.Stringify.value(dataset.parsed.value.source_manifest_sha256, .{}, &result.writer);
    try result.writer.writeAll(",\"dataset_source_repo\":");
    try std.json.Stringify.value(dataset.parsed.value.source_repo, .{}, &result.writer);
    try result.writer.writeAll(",\"dataset_source_revision\":");
    try std.json.Stringify.value(dataset.parsed.value.source_revision, .{}, &result.writer);
    try result.writer.print(",\"backend\":\"{s}\",\"split\":\"{s}\",\"documents\":{d},\"query_ids\":[", .{ @tagName(opts.backend), @tagName(opts.split), corpus.documents.items.len });
    for (corpus.queries.items, 0..) |query, index| {
        if (index != 0) try result.writer.writeByte(',');
        try std.json.Stringify.value(query.id, .{}, &result.writer);
    }
    try result.writer.writeAll("],\"group_ids\":[");
    for (corpus.queries.items, 0..) |query, index| {
        if (index != 0) try result.writer.writeByte(',');
        try std.json.Stringify.value(query.group_id, .{}, &result.writer);
    }
    try result.writer.writeAll("],\"dimensions\":{");
    for (job.dimensions, 0..) |dimension, index| {
        if (index != 0) try result.writer.writeByte(',');
        const metrics = try evaluateDimension(allocator, corpus.documents.items, corpus.queries.items, document_vectors, query_vectors, dimension, opts.score_batch);
        defer metrics.deinit(allocator);
        try result.writer.print("\"{d}\":", .{dimension});
        try std.json.Stringify.value(metrics, .{}, &result.writer);
    }
    try result.writer.writeAll("}}\n");
    var output_buffer: [32 * 1024]u8 = undefined;
    var output = std.Io.File.stdout().writerStreaming(init.io, &output_buffer);
    try output.interface.writeAll(result.written());
    try output.interface.flush();
}

fn verifyDatasetIdentity(dataset: data.Dataset, config: job.Config) !void {
    if (!std.mem.eql(u8, dataset.parsed.value.tokenizer_sha256, job.tokenizer_sha256)) return error.TrainingTokenizerIdentityMismatch;
    if (dataset.parsed.value.query_max_tokens != config.query_max_tokens or
        dataset.parsed.value.document_max_tokens != config.document_max_tokens or
        dataset.parsed.value.max_negatives != config.max_negatives) return error.PreparedPreprocessingMismatch;
}

fn loadCorpus(backing: std.mem.Allocator, dataset: *const data.Dataset, wanted: EvalSplit) !Corpus {
    var corpus = Corpus{ .arena = std.heap.ArenaAllocator.init(backing) };
    errdefer corpus.deinit();
    const a = corpus.arena.allocator();
    var ids: std.StringHashMapUnmanaged(usize) = .empty;
    defer ids.deinit(a);
    var content: std.StringHashMapUnmanaged(usize) = .empty;
    defer content.deinit(a);
    var position: u64 = 0;
    while (position < dataset.parsed.value.records) : (position += 1) {
        const split = try dataset.split(position);
        if ((wanted == .validation and split != .validation) or (wanted == .@"test" and split != .@"test")) continue;
        var record = try dataset.read(a, position);
        defer record.deinit();
        const r = record.value;
        try appendDocument(a, &corpus.documents, &ids, &content, r.positive);
        for (r.negatives) |negative| try appendDocument(a, &corpus.documents, &ids, &content, negative);
        const relevant = try a.alloc([]const u8, r.known_positive_ids.len + 1);
        relevant[0] = try a.dupe(u8, r.positive.id);
        for (r.known_positive_ids, 0..) |id, i| relevant[i + 1] = try a.dupe(u8, id);
        try corpus.queries.append(a, .{ .id = try a.dupe(u8, r.query_id), .group_id = try a.dupe(u8, r.group_id), .ids = try a.dupe(i32, r.query_ids), .relevant = relevant });
    }
    if (corpus.queries.items.len == 0) return error.EmptyEvaluationSplit;
    return corpus;
}

fn appendDocument(a: std.mem.Allocator, documents: *std.ArrayListUnmanaged(Document), ids: *std.StringHashMapUnmanaged(usize), content: *std.StringHashMapUnmanaged(usize), document: data.EncodedDocument) !void {
    if (ids.get(document.id)) |index| {
        const prior = documents.items[index];
        if (!std.mem.eql(u8, prior.digest, document.content_sha256) or !std.mem.eql(i32, prior.ids, document.input_ids)) return error.ConflictingCandidateDocument;
        return;
    }
    if (content.get(document.content_sha256)) |index| {
        var prior = &documents.items[index];
        if (!std.mem.eql(i32, prior.ids, document.input_ids)) return error.ConflictingCandidateContent;
        const alias = try a.dupe(u8, document.id);
        try prior.aliases.append(a, alias);
        try ids.put(a, alias, index);
        return;
    }
    const id = try a.dupe(u8, document.id);
    const digest = try a.dupe(u8, document.content_sha256);
    var aliases: std.ArrayListUnmanaged([]const u8) = .empty;
    try aliases.append(a, id);
    const index = documents.items.len;
    try documents.append(a, .{ .aliases = aliases, .digest = digest, .ids = try a.dupe(i32, document.input_ids) });
    try ids.put(a, id, index);
    try content.put(a, digest, index);
}

fn embedDocuments(a: std.mem.Allocator, pipeline: anytype, documents: []const Document, batch_size: usize) ![][]f32 {
    const result = try a.alloc([]f32, documents.len);
    errdefer a.free(result);
    var completed: usize = 0;
    errdefer for (result[0..completed]) |vector| a.free(vector);
    while (completed < documents.len) {
        const count = @min(batch_size, documents.len - completed);
        const ids = try a.alloc([]const i32, count);
        defer a.free(ids);
        for (ids, 0..) |*slot, i| slot.* = documents[completed + i].ids;
        const vectors = try embedTokenRows(a, pipeline, ids);
        defer a.free(vectors);
        for (vectors, 0..) |vector, i| result[completed + i] = vector;
        completed += count;
    }
    return result;
}

fn embedQueries(a: std.mem.Allocator, pipeline: anytype, queries: []const Query, batch_size: usize) ![][]f32 {
    const result = try a.alloc([]f32, queries.len);
    errdefer a.free(result);
    var completed: usize = 0;
    errdefer for (result[0..completed]) |vector| a.free(vector);
    while (completed < queries.len) {
        const count = @min(batch_size, queries.len - completed);
        const ids = try a.alloc([]const i32, count);
        defer a.free(ids);
        for (ids, 0..) |*slot, i| slot.* = queries[completed + i].ids;
        const vectors = try embedTokenRows(a, pipeline, ids);
        defer a.free(vectors);
        for (vectors, 0..) |vector, i| result[completed + i] = vector;
        completed += count;
    }
    return result;
}

fn embedTokenRows(a: std.mem.Allocator, pipeline: anytype, rows: []const []const i32) ![][]f32 {
    var sequence: usize = 0;
    for (rows) |row| sequence = @max(sequence, row.len);
    const total = try std.math.mul(usize, rows.len, sequence);
    const ids = try a.alloc(i64, total);
    defer a.free(ids);
    const mask = try a.alloc(i64, total);
    defer a.free(mask);
    @memset(ids, 0);
    @memset(mask, 0);
    for (rows, 0..) |row, batch| for (row, 0..) |token, offset| {
        ids[batch * sequence + offset] = token;
        mask[batch * sequence + offset] = 1;
    };
    return pipeline.embedTokenized(ids, mask, rows.len, sequence);
}

fn evaluateDimension(a: std.mem.Allocator, documents: []const Document, queries: []const Query, document_vectors: []const []const f32, query_vectors: []const []const f32, dimension: usize, score_batch: usize) !Evaluation {
    var total = Aggregate{ .queries = queries.len };
    const mrr = try a.alloc(f64, queries.len);
    errdefer a.free(mrr);
    const ndcg = try a.alloc(f64, queries.len);
    errdefer a.free(ndcg);
    const recall = try a.alloc(f64, queries.len);
    errdefer a.free(recall);
    const per_query = PerQuery{
        .mrr_at_10 = mrr,
        .ndcg_at_10 = ndcg,
        .recall_at_100 = recall,
    };
    const doc_norms = try a.alloc(f64, documents.len);
    defer a.free(doc_norms);
    for (document_vectors, 0..) |vector, i| doc_norms[i] = try normPrefix(vector, dimension);
    for (queries, query_vectors, 0..) |query, query_vector, query_index| {
        const query_norm = try normPrefix(query_vector, dimension);
        var top: [100]struct { score: f64, index: usize } = undefined;
        for (&top) |*ranked| ranked.* = .{ .score = -std.math.inf(f64), .index = 0 };
        var start: usize = 0;
        while (start < documents.len) : (start += score_batch) {
            const end = @min(documents.len, start + score_batch);
            for (start..end) |index| {
                var dot: f64 = 0;
                for (query_vector[0..dimension], document_vectors[index][0..dimension]) |q, d| dot += @as(f64, q) * @as(f64, d);
                insertTop10(&top, .{ .score = dot / (query_norm * doc_norms[index]), .index = index });
            }
        }
        var relevant_total: usize = 0;
        for (documents) |document| if (isRelevant(query.relevant, document.aliases.items)) {
            relevant_total += 1;
        };
        if (relevant_total == 0) return error.QueryHasNoCandidatePositive;
        var found: usize = 0;
        var reciprocal_rank: f64 = 0;
        var dcg: f64 = 0;
        for (top[0..@min(documents.len, 100)], 0..) |ranked, rank| if (isRelevant(query.relevant, documents[ranked.index].aliases.items)) {
            found += 1;
            if (rank < 10) {
                if (reciprocal_rank == 0) reciprocal_rank = 1.0 / @as(f64, @floatFromInt(rank + 1));
                dcg += 1.0 / std.math.log2(@as(f64, @floatFromInt(rank + 2)));
            }
        };
        var ideal: f64 = 0;
        for (0..@min(relevant_total, 10)) |rank| ideal += 1.0 / std.math.log2(@as(f64, @floatFromInt(rank + 2)));
        per_query.mrr_at_10[query_index] = reciprocal_rank;
        per_query.ndcg_at_10[query_index] = dcg / ideal;
        per_query.recall_at_100[query_index] = @as(f64, @floatFromInt(found)) / @as(f64, @floatFromInt(relevant_total));
        total.mrr_at_10 += per_query.mrr_at_10[query_index];
        total.ndcg_at_10 += per_query.ndcg_at_10[query_index];
        total.recall_at_100 += per_query.recall_at_100[query_index];
    }
    const denominator: f64 = @floatFromInt(queries.len);
    total.mrr_at_10 /= denominator;
    total.ndcg_at_10 /= denominator;
    total.recall_at_100 /= denominator;
    return .{ .aggregate = total, .per_query = per_query };
}

fn insertTop10(top: anytype, item: @TypeOf(top.*[0])) void {
    if (item.score <= top.*[top.len - 1].score) return;
    var position: usize = top.len - 1;
    while (position > 0 and item.score > top.*[position - 1].score) : (position -= 1) top.*[position] = top.*[position - 1];
    top.*[position] = item;
}
fn contains(values: []const []const u8, needle: []const u8) bool {
    for (values) |value| if (std.mem.eql(u8, value, needle)) return true;
    return false;
}
fn isRelevant(relevant: []const []const u8, aliases: []const []const u8) bool {
    for (aliases) |alias| if (contains(relevant, alias)) return true;
    return false;
}
fn normPrefix(vector: []const f32, dimension: usize) !f64 {
    if (dimension == 0 or vector.len < dimension) return error.InvalidEmbeddingDimension;
    var sum: f64 = 0;
    for (vector[0..dimension]) |value| {
        if (!std.math.isFinite(value)) return error.NonFiniteEmbedding;
        sum += @as(f64, value) * @as(f64, value);
    }
    if (sum == 0) return error.ZeroEmbedding;
    return @sqrt(sum);
}
fn freeVectors(a: std.mem.Allocator, vectors: [][]f32) void {
    for (vectors) |vector| a.free(vector);
    a.free(vectors);
}
fn boundedPositive(value: []const u8, maximum: usize) !usize {
    const parsed = try std.fmt.parseUnsigned(usize, value, 10);
    if (parsed == 0 or parsed > maximum) return error.InvalidBatchSize;
    return parsed;
}
fn help() void {
    std.debug.print("usage: eval-embeddinggemma2-retrieval <job.json> <base-or-merged-model-dir> --split validation|test [--backend cuda|native] [--embed-batch 32] [--score-batch 4096]\n", .{});
}
fn usage() error{InvalidArguments} {
    help();
    return error.InvalidArguments;
}

test "top ten ranking remains ordered" {
    var top: [10]struct { score: f64, index: usize } = undefined;
    for (&top) |*ranked| ranked.* = .{ .score = -std.math.inf(f64), .index = 0 };
    insertTop10(&top, .{ .score = 2, .index = 2 });
    insertTop10(&top, .{ .score = 4, .index = 4 });
    insertTop10(&top, .{ .score = 3, .index = 3 });
    try std.testing.expectEqual(@as(usize, 4), top[0].index);
    try std.testing.expectEqual(@as(usize, 3), top[1].index);
    try std.testing.expectEqual(@as(usize, 2), top[2].index);
}

test "retrieval metrics use deduplicated relevance and prefix normalization" {
    var positive_aliases: std.ArrayListUnmanaged([]const u8) = .empty;
    defer positive_aliases.deinit(std.testing.allocator);
    try positive_aliases.append(std.testing.allocator, "positive");
    var negative_aliases: std.ArrayListUnmanaged([]const u8) = .empty;
    defer negative_aliases.deinit(std.testing.allocator);
    try negative_aliases.append(std.testing.allocator, "negative");
    const documents = [_]Document{
        .{ .aliases = positive_aliases, .digest = "a", .ids = &.{} },
        .{ .aliases = negative_aliases, .digest = "b", .ids = &.{} },
    };
    const queries = [_]Query{.{ .id = "query", .group_id = "group", .ids = &.{}, .relevant = &.{ "positive", "positive" } }};
    const positive = [_]f32{ 2, 0 };
    const negative = [_]f32{ 0, 9 };
    const query = [_]f32{ 7, 0 };
    const metrics = try evaluateDimension(
        std.testing.allocator,
        &documents,
        &queries,
        &.{ &positive, &negative },
        &.{&query},
        2,
        1,
    );
    defer metrics.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), metrics.aggregate.queries);
    try std.testing.expectApproxEqAbs(@as(f64, 1), metrics.aggregate.mrr_at_10, 1e-12);
    try std.testing.expectApproxEqAbs(@as(f64, 1), metrics.aggregate.ndcg_at_10, 1e-12);
    try std.testing.expectApproxEqAbs(@as(f64, 1), metrics.aggregate.recall_at_100, 1e-12);
}
