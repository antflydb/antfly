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

//! Provider-neutral cosine routing. A named embedder owns transport, credentials,
//! batching and quotas. Each batch deduplicates evidence and category examples.
const std = @import("std");
const d = @import("decisions.zig");
const contract = @import("antfly_decisions");
const scoring = contract.scoring;
const A = std.mem.Allocator;
const V = std.json.Value;

const Texts = struct {
    values: std.ArrayList([]const u8) = .empty,
    ids: std.StringHashMapUnmanaged(usize) = .{},
    bytes: usize = 0,
    fn add(self: *@This(), a: A, text: []const u8, limit: usize) !usize {
        if (self.ids.get(text)) |id| return id;
        if (text.len > limit -| self.bytes or self.values.items.len >= 4096) return error.DecisionLimitExceeded;
        const id = self.values.items.len;
        try self.values.append(a, text);
        try self.ids.put(a, text, id);
        self.bytes += text.len;
        return id;
    }
};
const Question = struct { categories: []const []const usize, queries: []const usize, hash: [64]u8 };

pub fn batch(backing: A, runtime: anytype, cfg: d.DeciderConfig, requests: []const d.Request) ![]V {
    var scratch = std.heap.ArenaAllocator.init(backing);
    defer scratch.deinit();
    const a = scratch.allocator();
    const name = cfg.embedder orelse return error.InvalidDeciderConfig;
    const embedder = try runtime.registry.getEmbedderConfig(name);
    const dimensions = (cfg.embedding_options orelse d.EmbeddingOptions{}).dimensions;
    if ((embedder.dimensions orelse embedder.dimension orelse return error.InvalidDeciderConfig) != dimensions or (embedder.sparse orelse false)) return error.InvalidDeciderConfig;
    const first = try contract.parse(a, try d.wireRequest(a, cfg, try requests[0].input.requireText(), requests[0].questions));
    var output_bound: usize = 0;
    for (first.inner.questions) |question| output_bound = try std.math.add(usize, output_bound, try std.math.mul(usize, question.labels.len + 1, 256));
    if (try std.math.mul(usize, output_bound, requests.len) > 1024 * 1024) return error.DecisionLimitExceeded;
    var texts = Texts{};
    const questions = try a.alloc(Question, first.inner.questions.len);
    for (first.inner.questions, first.policies, questions) |question, policy, *plan| {
        if (policy.options.calibration_id != null) return error.UnsupportedEmbeddingCalibration;
        if (policy.kind != .choice and policy.kind != .multi_choice) return error.UnsupportedDecisionKind;
        if (policy.kind == .multi_choice and policy.thresholds == null) return error.EmbeddingMultiLabelThresholdRequired;
        const categories = try a.alloc([]const usize, question.labels.len);
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("antfly-hosted-prototypes-v1");
        hash.update(try std.json.Stringify.valueAlloc(a, .{ .model = embedder.model, .provider = @tagName(embedder.provider), .dimensions = dimensions, .question = question, .url = embedder.url, .account_id = embedder.account_id, .project_id = embedder.project_id, .location = embedder.location, .region = embedder.region, .request_format = embedder.request_format, .model_identity = embedder.model_identity, .input_type = embedder.input_type, .document_input_type = embedder.document_input_type, .truncate = embedder.truncate }, .{}));
        for (question.descriptions, categories, 0..) |description, *indices, label_index| {
            const examples = if (question.examples.len == 0) &.{} else question.examples[label_index];
            const ids = try a.alloc(usize, @max(examples.len, 1));
            for (ids, 0..) |*id, index| id.* = try texts.add(a, if (examples.len == 0)
                try scoring.renderCategory(a, question.instructions, description)
            else
                try scoring.renderInput(a, question.instructions, examples[index]), cfg.max_input_tokens);
            indices.* = ids;
        }
        const queries = try a.alloc(usize, requests.len);
        for (requests, queries) |request, *id| id.* = try texts.add(a, try scoring.renderInput(a, question.instructions, try request.input.requireText()), cfg.max_input_tokens);
        plan.* = .{ .categories = categories, .queries = queries, .hash = std.fmt.bytesToHex(hash.finalResult(), .lower) };
    }
    // Bound vector materialization before dispatch, independently of JSON size.
    if (try std.math.mul(usize, try std.math.mul(usize, texts.values.items.len, dimensions), @sizeOf(f32)) > 32 * 1024 * 1024) return error.DecisionLimitExceeded;
    if (texts.bytes > cfg.max_input_tokens -| runtime.input_tokens) return error.DecisionLimitExceeded;
    const Executor = @import("../common/inference_tasks.zig").Executor;
    const executor = Executor{
        .view = .{ .providers = runtime.registry },
        .source_table = if (requests[0].source_table.len > 0) requests[0].source_table else runtime.source_table,
        .http = runtime.http,
        .context = runtime.context orelse .{ .io = runtime.io, .deadline_ns = null },
        .secret_store = runtime.secret_store,
        .provider_runtime = runtime.provider_runtime,
        .antfly_provider = runtime.antfly_provider,
        .inference_api_url = runtime.antfly_url,
        .inference_api_key = runtime.antfly_api_key,
    };
    const vectors = try executor.embed(a, name, texts.values.items, dimensions);
    defer @import("../storage/db/enrichment/embedder.zig").freeDenseEmbeddingBatch(a, vectors);
    const results = try a.alloc(V, requests.len);
    for (results) |*result| {
        result.* = d.jsonObject();
        try d.put(a, result, "model", .{ .string = embedder.model });
        try d.put(a, result, "answers", d.jsonObject());
        var usage = d.jsonObject();
        // Ordinary embedding interfaces do not expose token usage. The byte
        // upper bound is conservative admission accounting, explicitly marked.
        try d.put(a, &usage, "input_tokens", .{ .integer = 0 });
        try d.put(a, &usage, "output_tokens", .{ .integer = 0 });
        try d.put(a, &usage, "estimated", .{ .bool = true });
        try d.put(a, result, "usage", usage);
    }
    try results[0].object.getPtr("usage").?.object.put(a, "input_tokens", .{ .integer = @intCast(texts.bytes) });
    for (first.inner.questions, first.policies, questions) |question, policy, plan| {
        const prototypes = try a.alloc([]const f32, plan.categories.len);
        for (plan.categories, prototypes) |indices, *prototype| {
            const examples = try a.alloc([]const f32, indices.len);
            for (indices, examples) |id, *example| example.* = vectors[id];
            prototype.* = try scoring.centroid(a, examples, dimensions);
        }
        for (results, plan.queries) |*result, query| {
            var answer = d.jsonObject();
            try d.put(a, &answer, "type", .{ .string = @tagName(policy.kind) });
            try d.put(a, &answer, "decision_method", .{ .string = "embedding_similarity" });
            try d.put(a, &answer, "similarity_metric", .{ .string = "cosine" });
            try d.put(a, &answer, "prototype_set_hash", .{ .string = try a.dupe(u8, &plan.hash) });
            var similarities = d.jsonObject();
            const scores = try a.alloc(f64, prototypes.len);
            for (question.labels, prototypes, scores) |label, prototype, *score| {
                score.* = try scoring.cosine(vectors[query], prototype, dimensions);
                try d.put(a, &similarities, label, .{ .float = score.* });
            }
            try d.put(a, &answer, "similarities", similarities);
            if (policy.kind == .choice) {
                const selection = try scoring.select(scores, policy.options);
                try d.put(a, &answer, "choice", if (selection.selected) |id| .{ .string = question.labels[id] } else .null);
                try d.put(a, &answer, "margin", .{ .float = selection.margin });
                try d.put(a, &answer, "status", .{ .string = if (selection.selected != null) "selected" else "abstained" });
                if (selection.reason) |reason| try d.put(a, &answer, "abstention_reason", .{ .string = reason });
            } else {
                const selection = try scoring.selectMulti(a, scores, policy.thresholds.?, policy.options.min_margin);
                var labels: std.array_list.Managed(V) = .init(a);
                var thresholds = d.jsonObject();
                for (selection.indices) |id| try labels.append(.{ .string = question.labels[id] });
                for (question.labels, policy.thresholds.?) |label, threshold| try d.put(a, &thresholds, label, .{ .float = threshold });
                try d.put(a, &answer, "choices", .{ .array = labels });
                try d.put(a, &answer, "similarity_thresholds", thresholds);
                try d.put(a, &answer, "margin", .{ .float = selection.margin });
                try d.put(a, &answer, "status", .{ .string = selection.status });
                if (selection.reason) |reason| try d.put(a, &answer, "abstention_reason", .{ .string = reason });
            }
            try result.object.getPtr("answers").?.object.put(a, question.name, answer);
        }
    }
    for (results, requests) |*result, request| result.* = try d.normalizeResponseWithConfig(a, request.questions, result.*, cfg);
    const owned = try backing.alloc(V, results.len);
    for (results, owned) |result, *out| out.* = try std.json.parseFromSliceLeaky(V, backing, try std.json.Stringify.valueAlloc(a, result, .{}), .{ .allocate = .alloc_always });
    return owned;
}
