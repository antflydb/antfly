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
const reranker_data = @import("reranker_data.zig");
const model_manager_mod = @import("../server/model_manager.zig");
const backends = @import("../backends/backends.zig");
const runtime = @import("../runtime/root.zig");

pub const EvalSummary = struct {
    examples_evaluated: usize,
    mse: f64,
    mae: f64,
    correlation: f64,
    spearman_correlation: f64,
    groups_evaluated: usize = 0,
    top1_accuracy: f64 = 0,
    ndcg_at_4: f64 = 0,
    mrr_at_4: f64 = 0,
};

pub const BackendChoice = enum {
    auto,
    native,
    metal,
};

pub const RuntimeEvalResult = struct {
    summary: EvalSummary,
    backend_selected: backends.BackendType,
    distributed: runtime.distributed.Config,
    uses_distributed_gpu_hosted: bool,
    uses_tensor_parallel_gpu_hosted: bool,
};

const RankPair = struct {
    index: usize,
    value: f64,
};

pub fn computeEvalSummary(
    allocator: std.mem.Allocator,
    examples: []const reranker_data.Example,
    predicted: []const f64,
) !EvalSummary {
    if (examples.len != predicted.len) return error.ShapeMismatch;
    if (examples.len == 0) {
        return .{
            .examples_evaluated = 0,
            .mse = 0,
            .mae = 0,
            .correlation = 0,
            .spearman_correlation = 0,
        };
    }

    var sum_sq: f64 = 0;
    var sum_abs: f64 = 0;
    var sum_pred: f64 = 0;
    var sum_target: f64 = 0;
    var sum_pred_sq: f64 = 0;
    var sum_target_sq: f64 = 0;
    var sum_prod: f64 = 0;
    const actual = try allocator.alloc(f64, examples.len);
    defer allocator.free(actual);

    for (examples, predicted, 0..) |example, pred, idx| {
        const target = @as(f64, example.score);
        actual[idx] = target;
        const diff = pred - target;
        sum_sq += diff * diff;
        sum_abs += @abs(diff);
        sum_pred += pred;
        sum_target += target;
        sum_pred_sq += pred * pred;
        sum_target_sq += target * target;
        sum_prod += pred * target;
    }

    const n = @as(f64, @floatFromInt(examples.len));
    const pred_var = (n * sum_pred_sq) - (sum_pred * sum_pred);
    const target_var = (n * sum_target_sq) - (sum_target * sum_target);
    const denom = @sqrt(@max(pred_var, 0) * @max(target_var, 0));
    const corr = if (denom == 0) 0 else ((n * sum_prod) - (sum_pred * sum_target)) / denom;
    const spearman = try computeSpearmanCorrelation(allocator, predicted, actual);
    const grouped = computeGroupedRankingMetrics(allocator, examples, predicted);

    return .{
        .examples_evaluated = examples.len,
        .mse = sum_sq / n,
        .mae = sum_abs / n,
        .correlation = corr,
        .spearman_correlation = spearman,
        .groups_evaluated = grouped.groups_evaluated,
        .top1_accuracy = grouped.top1_accuracy,
        .ndcg_at_4 = grouped.ndcg_at_4,
        .mrr_at_4 = grouped.mrr_at_4,
    };
}

pub fn evaluateExamplesRuntime(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    examples: []const reranker_data.Example,
    choice: BackendChoice,
    max_examples: usize,
) !RuntimeEvalResult {
    var session_manager = backends.SessionManager.init(allocator);
    configureBackendPreference(&session_manager, choice);

    var model_manager = model_manager_mod.ModelManager.init(allocator, session_manager);
    defer model_manager.deinit();

    const model = try model_manager.loadFromDir(model_dir);
    var pipeline = model.rerankingPipeline(allocator);
    const effective_examples = examples[0..@min(examples.len, max_examples)];
    const predicted = try allocator.alloc(f64, effective_examples.len);
    defer allocator.free(predicted);

    for (effective_examples, 0..) |example, idx| {
        const docs = [_][]const u8{example.document};
        const scores = try pipeline.rerank(example.query, &docs);
        defer allocator.free(scores);
        predicted[idx] = scores[0];
    }

    return .{
        .summary = try computeEvalSummary(allocator, effective_examples, predicted),
        .backend_selected = model.session.backend(),
        .distributed = runtime.distributed.configFromEnv(),
        .uses_distributed_gpu_hosted = pipeline.usesDistributedGpuHosted(),
        .uses_tensor_parallel_gpu_hosted = pipeline.usesTensorParallelGpuHosted(),
    };
}

fn computeAverageRanks(allocator: std.mem.Allocator, values: []const f64) ![]f64 {
    const pairs = try allocator.alloc(RankPair, values.len);
    defer allocator.free(pairs);
    for (values, 0..) |value, idx| {
        pairs[idx] = .{ .index = idx, .value = value };
    }
    std.sort.pdq(RankPair, pairs, {}, struct {
        fn lessThan(_: void, a: RankPair, b: RankPair) bool {
            return a.value < b.value;
        }
    }.lessThan);

    const ranks = try allocator.alloc(f64, values.len);
    var i: usize = 0;
    while (i < pairs.len) {
        var j = i + 1;
        while (j < pairs.len and pairs[j].value == pairs[i].value) : (j += 1) {}
        const avg_rank = (@as(f64, @floatFromInt(i + j + 1))) / 2.0;
        for (pairs[i..j]) |pair| {
            ranks[pair.index] = avg_rank;
        }
        i = j;
    }
    return ranks;
}

fn computeSpearmanCorrelation(allocator: std.mem.Allocator, predicted: []const f64, actual: []const f64) !f64 {
    if (predicted.len != actual.len or predicted.len == 0) return 0;
    const pred_ranks = try computeAverageRanks(allocator, predicted);
    defer allocator.free(pred_ranks);
    const actual_ranks = try computeAverageRanks(allocator, actual);
    defer allocator.free(actual_ranks);

    const n = @as(f64, @floatFromInt(predicted.len));
    var sum_d2: f64 = 0;
    for (pred_ranks, actual_ranks) |pred_rank, actual_rank| {
        const d = pred_rank - actual_rank;
        sum_d2 += d * d;
    }
    const denom = n * ((n * n) - 1.0);
    if (denom == 0) return 0;
    return 1.0 - ((6.0 * sum_d2) / denom);
}

fn computeGroupedRankingMetrics(
    allocator: std.mem.Allocator,
    examples: []const reranker_data.Example,
    predicted: []const f64,
) struct {
    groups_evaluated: usize,
    top1_accuracy: f64,
    ndcg_at_4: f64,
    mrr_at_4: f64,
} {
    var groups: usize = 0;
    var top1_sum: f64 = 0;
    var ndcg_sum: f64 = 0;
    var mrr_sum: f64 = 0;
    var group_start: usize = 0;
    while (group_start < examples.len) {
        var group_end = group_start + 1;
        while (group_end < examples.len and std.mem.eql(u8, examples[group_end].query, examples[group_start].query)) : (group_end += 1) {}
        if (group_end - group_start >= 2) {
            const metrics = computeOneGroupedRankingMetrics(allocator, examples[group_start..group_end], predicted[group_start..group_end], 4);
            groups += 1;
            top1_sum += metrics.top1_accuracy;
            ndcg_sum += metrics.ndcg_at_4;
            mrr_sum += metrics.mrr_at_4;
        }
        group_start = group_end;
    }
    if (groups == 0) return .{ .groups_evaluated = 0, .top1_accuracy = 0, .ndcg_at_4 = 0, .mrr_at_4 = 0 };
    const groups_f = @as(f64, @floatFromInt(groups));
    return .{
        .groups_evaluated = groups,
        .top1_accuracy = top1_sum / groups_f,
        .ndcg_at_4 = ndcg_sum / groups_f,
        .mrr_at_4 = mrr_sum / groups_f,
    };
}

fn computeOneGroupedRankingMetrics(
    allocator: std.mem.Allocator,
    examples: []const reranker_data.Example,
    predicted: []const f64,
    k: usize,
) struct {
    top1_accuracy: f64,
    ndcg_at_4: f64,
    mrr_at_4: f64,
} {
    const Scored = struct {
        pred: f64,
        score: f64,
    };
    const scored = allocator.alloc(Scored, examples.len) catch return .{ .top1_accuracy = 0, .ndcg_at_4 = 0, .mrr_at_4 = 0 };
    defer allocator.free(scored);
    var best_actual_idx: usize = 0;
    for (examples, predicted, 0..) |ex, pred, idx| {
        scored[idx] = .{ .pred = pred, .score = ex.score };
        if (ex.score > examples[best_actual_idx].score) best_actual_idx = idx;
    }
    std.sort.pdq(Scored, scored, {}, struct {
        fn lessThan(_: void, a: Scored, b: Scored) bool {
            if (a.pred == b.pred) return a.score > b.score;
            return a.pred > b.pred;
        }
    }.lessThan);
    const best_score = @as(f64, examples[best_actual_idx].score);
    const top1: f64 = if (scored.len > 0 and scored[0].score == best_score) 1.0 else 0.0;

    const ideal = allocator.alloc(f64, examples.len) catch return .{ .top1_accuracy = 0, .ndcg_at_4 = 0, .mrr_at_4 = 0 };
    defer allocator.free(ideal);
    for (examples, 0..) |ex, idx| ideal[idx] = ex.score;
    std.sort.pdq(f64, ideal, {}, std.sort.desc(f64));

    const limit = @min(k, scored.len);
    var dcg: f64 = 0;
    var idcg: f64 = 0;
    var mrr: f64 = 0;
    for (0..limit) |idx| {
        const discount = std.math.log2(@as(f64, @floatFromInt(idx)) + 2.0);
        const gain = std.math.pow(f64, 2.0, scored[idx].score) - 1.0;
        dcg += gain / discount;
        const ideal_gain = std.math.pow(f64, 2.0, ideal[idx]) - 1.0;
        idcg += ideal_gain / discount;
        if (mrr == 0 and scored[idx].score == best_score) mrr = 1.0 / (@as(f64, @floatFromInt(idx)) + 1.0);
    }
    return .{
        .top1_accuracy = top1,
        .ndcg_at_4 = if (idcg == 0) 0 else dcg / idcg,
        .mrr_at_4 = mrr,
    };
}

fn configureBackendPreference(session_manager: *backends.SessionManager, choice: BackendChoice) void {
    session_manager.preferred_backends = switch (choice) {
        .auto => &.{ backends.BackendType.metal, backends.BackendType.native },
        .native => &.{backends.BackendType.native},
        .metal => &.{backends.BackendType.metal},
    };
}

test "compute reranker eval summary and grouped metrics" {
    const allocator = std.testing.allocator;
    const examples = [_]reranker_data.Example{
        .{ .query = "q1", .document = "d1", .score = 1.0 },
        .{ .query = "q1", .document = "d2", .score = 0.0 },
        .{ .query = "q2", .document = "d3", .score = 0.2 },
        .{ .query = "q2", .document = "d4", .score = 0.8 },
    };
    const predicted = [_]f64{ 0.9, 0.1, 0.3, 0.7 };
    const summary = try computeEvalSummary(allocator, &examples, &predicted);
    try std.testing.expectEqual(@as(usize, 4), summary.examples_evaluated);
    try std.testing.expect(summary.mse >= 0);
    try std.testing.expect(summary.mae >= 0);
    try std.testing.expectApproxEqAbs(@as(f64, 1.0), summary.top1_accuracy, 1e-6);
    try std.testing.expectApproxEqAbs(@as(f64, 1.0), summary.mrr_at_4, 1e-6);
}
