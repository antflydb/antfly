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
const Task = @import("../inference/managed_embedder.zig").EmbeddingTaskType;

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
const Question = struct { categories: []const []const usize, queries: []const usize, hash: [64]u8, prototypes: []const []const f32 = &.{} };

pub const PartialBatch = struct {
    values: []?V,
    failures: []?anyerror,
    input_tokens: u64 = 0,
};

const Plan = struct {
    task: Task,
    first: contract.Request,
    dimensions: u32,
    texts: Texts,
    questions: []Question,
    vector_bytes: usize,
};

/// One planner owns rendering, deduplication and physical vector bounds for
/// both admission and execution. Planning never loads a model or opens HTTP.
fn prepare(a: A, cfg: d.DeciderConfig, embedder: @import("antfly_embeddings").Config, requests: []const d.Request) !Plan {
    if (requests.len == 0) return error.InvalidDecisionSpecification;
    const dimensions = (cfg.embedding_options orelse d.EmbeddingOptions{}).dimensions;
    if ((embedder.dimensions orelse embedder.dimension orelse return error.InvalidDeciderConfig) != dimensions or (embedder.sparse orelse false)) return error.InvalidDeciderConfig;
    const first = try contract.parse(a, try d.wireRequest(a, cfg, try requests[0].input.requireText(), requests[0].questions));
    const task: Task = if (std.mem.eql(u8, first.policies[0].options.task_type, "CLUSTERING")) .clustering else .classification;
    var output_bound: usize = 0;
    for (first.inner.questions) |question| output_bound = try std.math.add(usize, output_bound, try std.math.mul(usize, question.labels.len + 1, 256));
    if (try std.math.mul(usize, output_bound, requests.len) > 1024 * 1024) return error.DecisionLimitExceeded;
    var texts = Texts{};
    const questions = try a.alloc(Question, first.inner.questions.len);
    for (first.inner.questions, first.policies, questions) |question, policy, *plan| {
        if (policy.options.dimensions != dimensions) return error.InvalidDeciderConfig;
        if (!std.mem.eql(u8, policy.options.task_type, task.canonical())) return error.InvalidDeciderConfig;
        if (policy.options.calibration_id != null) return error.UnsupportedEmbeddingCalibration;
        if (policy.kind != .choice and policy.kind != .multi_choice) return error.UnsupportedDecisionKind;
        if (policy.kind == .multi_choice and policy.thresholds == null) return error.EmbeddingMultiLabelThresholdRequired;
        const categories = try a.alloc([]const usize, question.labels.len);
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("antfly-hosted-prototypes-v1");
        hash.update(try std.json.Stringify.valueAlloc(a, .{ .model = embedder.model, .provider = @tagName(embedder.provider), .dimensions = dimensions, .task_type = task.canonical(), .renderer_version = scoring.renderer_version, .question = question, .url = embedder.url, .account_id = embedder.account_id, .gateway_id = embedder.gateway_id, .project_id = embedder.project_id, .location = embedder.location, .region = embedder.region, .request_format = embedder.request_format, .model_identity = embedder.model_identity, .input_type = embedder.input_type, .document_input_type = embedder.document_input_type, .truncate = embedder.truncate }, .{}));
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
    const vector_bytes = try std.math.mul(usize, try std.math.mul(usize, texts.values.items.len, dimensions), @sizeOf(f32));
    if (vector_bytes > 32 * 1024 * 1024) return error.DecisionLimitExceeded;
    return .{ .task = task, .first = first, .dimensions = dimensions, .texts = texts, .questions = questions, .vector_bytes = vector_bytes };
}

pub fn workingBytes(a: A, cfg: d.DeciderConfig, embedder: @import("antfly_embeddings").Config, requests: []const d.Request) !usize {
    const plan = try prepare(a, cfg, embedder, requests);
    var prototypes: usize = 0;
    var indexes: usize = 0;
    for (plan.questions) |question| {
        prototypes = try std.math.add(usize, prototypes, question.categories.len);
        indexes = try std.math.add(usize, indexes, question.queries.len);
        for (question.categories) |category| indexes = try std.math.add(usize, indexes, category.len);
    }
    const prototype_bytes = try std.math.mul(usize, try std.math.mul(usize, prototypes, plan.dimensions), @sizeOf(f32));
    const metadata = try std.math.mul(usize, indexes + prototypes, 4 * @sizeOf(usize));
    // Vector/centroid transfers and encoded/rendered text coexist transiently.
    const vectors = try std.math.mul(usize, try std.math.add(usize, plan.vector_bytes, prototype_bytes), 2);
    return std.math.add(usize, vectors, try std.math.add(usize, metadata, try std.math.mul(usize, plan.texts.bytes, 2)));
}

/// Shared embedding work is charged once even when individual scoring fails.
/// Only valid rows escape scratch storage; failures retain their original index.
pub fn batchPartial(backing: A, runtime: anytype, cfg: d.DeciderConfig, requests: []const d.Request) !PartialBatch {
    var scratch = std.heap.ArenaAllocator.init(backing);
    defer scratch.deinit();
    const a = scratch.allocator();
    const name = cfg.embedder orelse return error.InvalidDeciderConfig;
    const embedder = try runtime.registry.getEmbedderConfig(name);
    const plan = try prepare(a, cfg, embedder, requests);
    const dimensions = plan.dimensions;
    const first = plan.first;
    const texts = plan.texts;
    const questions = plan.questions;
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
    const partial = PartialBatch{ .values = try backing.alloc(?V, requests.len), .failures = try backing.alloc(?anyerror, requests.len), .input_tokens = texts.bytes };
    @memset(partial.values, null);
    @memset(partial.failures, null);
    // This estimate remains available when dispatch or shared prototype work
    // fails. Admission is conservative and never presents it as measured usage.
    const vectors = executor.embedForTask(a, name, texts.values.items, dimensions, plan.task) catch |err| {
        @memset(partial.failures, err);
        return partial;
    };
    defer @import("../storage/db/enrichment/embedder.zig").freeDenseEmbeddingBatch(a, vectors);
    preparePrototypes(a, questions, vectors, dimensions) catch |err| {
        @memset(partial.failures, err);
        return partial;
    };
    var charged = false;
    for (requests, partial.values, partial.failures, 0..) |request, *value, *failure, row_index| {
        executor.context.check() catch |err| {
            for (partial.failures[row_index..]) |*pending| pending.* = err;
            return partial;
        };
        // Keep validation/serialization scratch at the outer arena's tail.
        // Owned results copy into backing, so each row can reuse this workspace
        // without retaining another copy of the question schema per sibling.
        var row_arena = std.heap.ArenaAllocator.init(a);
        defer row_arena.deinit();
        const row_allocator = row_arena.allocator();
        const result = scoreRow(row_allocator, cfg, request, first, questions, vectors, dimensions, embedder.model, row_index) catch |err| {
            failure.* = err;
            continue;
        };
        if (!charged) {
            try result.object.getPtr("usage").?.object.put(row_allocator, "input_tokens", .{ .integer = @intCast(texts.bytes) });
            charged = true;
        }
        value.* = try std.json.parseFromSliceLeaky(V, backing, try std.json.Stringify.valueAlloc(row_allocator, result, .{}), .{ .allocate = .alloc_always });
    }
    return partial;
}

fn preparePrototypes(a: A, questions: []Question, vectors: []const []const f32, dimensions: usize) !void {
    for (questions) |*plan| {
        const prototypes = try a.alloc([]const f32, plan.categories.len);
        for (plan.categories, prototypes) |indices, *prototype| {
            const examples = try a.alloc([]const f32, indices.len);
            for (indices, examples) |id, *example| example.* = vectors[id];
            prototype.* = try scoring.centroid(a, examples, dimensions);
        }
        plan.prototypes = prototypes;
    }
}

fn scoreRow(a: A, cfg: d.DeciderConfig, request: d.Request, first: anytype, questions: []const Question, vectors: []const []const f32, dimensions: usize, model: []const u8, row_index: usize) !V {
    var result = d.jsonObject();
    try d.put(a, &result, "model", .{ .string = model });
    try d.put(a, &result, "answers", d.jsonObject());
    var usage = d.jsonObject();
    try d.put(a, &usage, "input_tokens", .{ .integer = 0 });
    try d.put(a, &usage, "output_tokens", .{ .integer = 0 });
    try d.put(a, &usage, "estimated", .{ .bool = true });
    try d.put(a, &result, "usage", usage);
    for (first.inner.questions, first.policies, questions) |question, policy, plan| {
        var answer = d.jsonObject();
        try d.put(a, &answer, "type", .{ .string = @tagName(policy.kind) });
        try d.put(a, &answer, "decision_method", .{ .string = "embedding_similarity" });
        try d.put(a, &answer, "similarity_metric", .{ .string = "cosine" });
        try d.put(a, &answer, "prototype_set_hash", .{ .string = try a.dupe(u8, &plan.hash) });
        var similarities = d.jsonObject();
        const scores = try a.alloc(f64, plan.prototypes.len);
        for (question.labels, plan.prototypes, scores) |label, prototype, *score| {
            score.* = try scoring.cosine(vectors[plan.queries[row_index]], prototype, dimensions);
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
    return d.normalizeResponseWithConfig(a, request.questions, result, cfg);
}
