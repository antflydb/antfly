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
const inference = @import("inference_internal");

const gliner2_data = inference.finetune.gliner2_data;
const gliner2_autodiff = inference.finetune.gliner2_real_autodiff;
const compat = inference.io.compat;
const adapter_eval = @import("eval_gliner2_autodiff_adapter.zig");

const Options = struct {
    model_dir: []const u8,
    adapter_dir: []const u8,
    eval_data: []const u8,
    entity_types_csv: []const u8,
    seq_len: usize = 32,
    max_span_width: usize = 4,
    backend: adapter_eval.EvalBackend = .native,
    compiled_required: bool = false,
    objective: gliner2_autodiff.GlinerObjective = .span_start,
    max_examples: usize = 0,
    min_prediction_score: f32 = 0.03,
    label_thresholds: []const LabelThreshold = &.{},
    label_score_biases: []const LabelScoreBias = &.{},
    nms_overlap_threshold: ?f64 = 0.0,
    max_predictions_per_example: ?usize = null,
    top_k_per_label: ?usize = null,
    best_span_per_label_start: bool = false,
    best_label_per_span_start: bool = false,
    require_entitylike_span: bool = false,
    sweep_thresholds: []const f32 = &.{},
    min_precision: ?f64 = null,
    min_recall: ?f64 = null,
    min_f1: ?f64 = null,
    out_path: ?[]const u8 = null,
    thresholds_out_path: ?[]const u8 = null,
    diagnostic_limit: usize = 50,
};

const LabelThreshold = struct {
    label: []const u8,
    threshold: f32,
};

const LabelScoreBias = adapter_eval.LabelScoreBias;

const LabelMetric = struct {
    label: []const u8,
    gold: usize = 0,
    predicted: usize = 0,
    correct: usize = 0,
    precision: f64 = 0.0,
    recall: f64 = 0.0,
    f1: f64 = 0.0,
};

const QualitySummary = struct {
    model_dir: []const u8,
    adapter_dir: []const u8,
    eval_data: []const u8,
    entity_types: []const []const u8,
    objective: []const u8,
    seq_len: usize,
    max_span_width: usize,
    example_count: usize,
    gold_entity_count: usize,
    predicted_entity_count: usize,
    correct_entity_count: usize,
    false_positive_count: usize,
    false_negative_count: usize,
    precision: f64,
    recall: f64,
    f1: f64,
    min_prediction_score: f32,
    label_thresholds: []const LabelThreshold,
    label_score_biases: []const LabelScoreBias,
    nms_overlap_threshold: ?f64,
    max_predictions_per_example: ?usize,
    top_k_per_label: ?usize,
    best_span_per_label_start: bool,
    best_label_per_span_start: bool,
    require_entitylike_span: bool,
    per_label_score_stats: []const LabelScoreStats,
    per_label: []const LabelMetric,
    threshold_sweep: []const ThresholdMetric,
    best_threshold: ?ThresholdMetric,
    best_per_label_thresholds: []const LabelThresholdMetric,
    recommended_label_thresholds_csv: []const u8,
    diagnostic_limit: usize,
    diagnostics: []const QualityDiagnostic,
    status: []const u8,
};

const LabelScoreStats = struct {
    label: []const u8,
    count: usize = 0,
    min: f32 = 0.0,
    max: f32 = 0.0,
    mean: f64 = 0.0,

    fn observe(self: *LabelScoreStats, score: f32) void {
        if (self.count == 0) {
            self.min = score;
            self.max = score;
            self.mean = score;
            self.count = 1;
            return;
        }
        self.min = @min(self.min, score);
        self.max = @max(self.max, score);
        self.mean = ((self.mean * @as(f64, @floatFromInt(self.count))) + score) / @as(f64, @floatFromInt(self.count + 1));
        self.count += 1;
    }
};

const ThresholdMetric = struct {
    min_prediction_score: f32,
    predicted_entity_count: usize,
    correct_entity_count: usize,
    precision: f64,
    recall: f64,
    f1: f64,
};

const LabelThresholdMetric = struct {
    label: []const u8,
    min_prediction_score: f32,
    predicted_entity_count: usize,
    correct_entity_count: usize,
    precision: f64,
    recall: f64,
    f1: f64,
};

const DiagnosticEntity = struct {
    text: []const u8,
    label: []const u8,
    start: usize,
    end: usize,
    score: ?f32 = null,
};

const QualityDiagnostic = struct {
    kind: []const u8,
    example_index: usize,
    example_text: []const u8,
    gold: ?DiagnosticEntity = null,
    prediction: ?DiagnosticEntity = null,
};

const DiagnosticContext = struct {
    items: *std.ArrayListUnmanaged(QualityDiagnostic),
    example_index: usize,
    limit: usize,

    fn append(self: *DiagnosticContext, allocator: std.mem.Allocator, diagnostic: QualityDiagnostic) !void {
        if (self.items.items.len >= self.limit) return;
        try self.items.append(allocator, diagnostic);
    }
};

const EvalAccumulator = struct {
    threshold: f32,
    label_metrics: []LabelMetric,
    gold_total: usize = 0,
    predicted_total: usize = 0,
    correct_total: usize = 0,

    fn init(allocator: std.mem.Allocator, threshold: f32, entity_types: []const []const u8) !EvalAccumulator {
        const label_metrics = try allocator.alloc(LabelMetric, entity_types.len);
        for (label_metrics, entity_types) |*metric, label| metric.* = .{ .label = label };
        return .{ .threshold = threshold, .label_metrics = label_metrics };
    }

    fn deinit(self: *EvalAccumulator, allocator: std.mem.Allocator) void {
        allocator.free(self.label_metrics);
        self.* = undefined;
    }

    fn addGold(self: *EvalAccumulator, label_idx: usize) void {
        self.gold_total += 1;
        self.label_metrics[label_idx].gold += 1;
    }

    fn addPrediction(self: *EvalAccumulator, label_idx: usize, correct: bool) void {
        self.predicted_total += 1;
        self.label_metrics[label_idx].predicted += 1;
        if (correct) {
            self.correct_total += 1;
            self.label_metrics[label_idx].correct += 1;
        }
    }

    fn finish(self: *EvalAccumulator) void {
        for (self.label_metrics) |*metric| finishMetric(metric);
    }

    fn precision(self: *const EvalAccumulator) f64 {
        return ratio(self.correct_total, self.predicted_total);
    }

    fn recall(self: *const EvalAccumulator) f64 {
        return ratio(self.correct_total, self.gold_total);
    }

    fn f1(self: *const EvalAccumulator) f64 {
        return f1Score(self.precision(), self.recall());
    }

    fn thresholdMetric(self: *const EvalAccumulator) ThresholdMetric {
        return .{
            .min_prediction_score = self.threshold,
            .predicted_entity_count = self.predicted_total,
            .correct_entity_count = self.correct_total,
            .precision = self.precision(),
            .recall = self.recall(),
            .f1 = self.f1(),
        };
    }
};

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    const opts = try parseOptions(init.minimal.args, allocator) orelse return;
    defer allocator.free(opts.sweep_thresholds);
    defer freeLabelThresholds(allocator, opts.label_thresholds);
    defer freeLabelScoreBiases(allocator, opts.label_score_biases);
    const entity_types = try parseCsv(allocator, opts.entity_types_csv);
    defer freeStringSlice(allocator, entity_types);
    try validateLabelThresholds(opts.label_thresholds, entity_types);
    try validateLabelScoreBiases(opts.label_score_biases, entity_types);

    var loaded = try gliner2_data.loadExamples(allocator, opts.eval_data, null);
    defer loaded.deinit();

    const limit = if (opts.max_examples == 0) loaded.examples.len else @min(opts.max_examples, loaded.examples.len);
    if (limit == 0) return error.NoEvalExamples;

    const decode_floor = minDecodeThreshold(opts);
    var main_accum = try EvalAccumulator.init(allocator, opts.min_prediction_score, entity_types);
    defer main_accum.deinit(allocator);
    const score_stats = try allocator.alloc(LabelScoreStats, entity_types.len);
    defer allocator.free(score_stats);
    for (score_stats, entity_types) |*stat, label| stat.* = .{ .label = label };
    var sweep_accums = try allocator.alloc(EvalAccumulator, opts.sweep_thresholds.len);
    defer allocator.free(sweep_accums);
    for (opts.sweep_thresholds, 0..) |threshold, idx| {
        sweep_accums[idx] = try EvalAccumulator.init(allocator, threshold, entity_types);
    }
    defer for (sweep_accums) |*accum| accum.deinit(allocator);

    var diagnostics = std.ArrayListUnmanaged(QualityDiagnostic).empty;
    defer diagnostics.deinit(allocator);

    for (loaded.examples[0..limit], 0..) |ex, example_index| {
        for (ex.entities) |ent| {
            if (indexOfLabel(entity_types, ent.label)) |label_idx| {
                main_accum.addGold(label_idx);
                for (sweep_accums) |*accum| accum.addGold(label_idx);
            }
        }

        var summary = try adapter_eval.evalSavedAdapterText(allocator, .{
            .model_dir = opts.model_dir,
            .adapter_dir = opts.adapter_dir,
            .text = ex.text,
            .entity_types_csv = opts.entity_types_csv,
            .seq_len = opts.seq_len,
            .max_span_width = opts.max_span_width,
            .backend = opts.backend,
            .compiled_required = opts.compiled_required,
            .objective_override = opts.objective,
            .prediction_threshold = decode_floor,
            .label_score_biases = opts.label_score_biases,
        });
        defer summary.deinit(allocator);

        observeScores(score_stats, entity_types, summary.predictions);
        var diagnostic_ctx = DiagnosticContext{
            .items = &diagnostics,
            .example_index = example_index,
            .limit = opts.diagnostic_limit,
        };
        try scoreExample(allocator, &main_accum, ex, entity_types, summary.predictions, opts, &diagnostic_ctx);
        for (sweep_accums) |*accum| try scoreExample(allocator, accum, ex, entity_types, summary.predictions, opts, null);
    }

    main_accum.finish();
    for (sweep_accums) |*accum| accum.finish();
    const precision = main_accum.precision();
    const recall = main_accum.recall();
    const f1 = main_accum.f1();

    const threshold_sweep = try allocator.alloc(ThresholdMetric, sweep_accums.len);
    defer allocator.free(threshold_sweep);
    for (sweep_accums, threshold_sweep) |accum, *metric| metric.* = accum.thresholdMetric();
    const best_threshold = bestThresholdMetric(threshold_sweep);
    const best_per_label_thresholds = try bestPerLabelThresholdMetrics(allocator, sweep_accums, entity_types);
    defer allocator.free(best_per_label_thresholds);
    const recommended_label_thresholds_csv = try formatLabelThresholdCsv(allocator, best_per_label_thresholds);
    defer allocator.free(recommended_label_thresholds_csv);
    const quality_gate_failure = try qualityGateFailure(precision, recall, f1, opts);

    const summary = QualitySummary{
        .model_dir = opts.model_dir,
        .adapter_dir = opts.adapter_dir,
        .eval_data = opts.eval_data,
        .entity_types = entity_types,
        .objective = objectiveName(opts.objective),
        .seq_len = opts.seq_len,
        .max_span_width = opts.max_span_width,
        .example_count = limit,
        .gold_entity_count = main_accum.gold_total,
        .predicted_entity_count = main_accum.predicted_total,
        .correct_entity_count = main_accum.correct_total,
        .false_positive_count = main_accum.predicted_total - main_accum.correct_total,
        .false_negative_count = main_accum.gold_total - main_accum.correct_total,
        .precision = precision,
        .recall = recall,
        .f1 = f1,
        .min_prediction_score = opts.min_prediction_score,
        .label_thresholds = opts.label_thresholds,
        .label_score_biases = opts.label_score_biases,
        .nms_overlap_threshold = opts.nms_overlap_threshold,
        .max_predictions_per_example = opts.max_predictions_per_example,
        .top_k_per_label = opts.top_k_per_label,
        .best_span_per_label_start = opts.best_span_per_label_start,
        .best_label_per_span_start = opts.best_label_per_span_start,
        .require_entitylike_span = opts.require_entitylike_span,
        .per_label_score_stats = score_stats,
        .per_label = main_accum.label_metrics,
        .threshold_sweep = threshold_sweep,
        .best_threshold = best_threshold,
        .best_per_label_thresholds = best_per_label_thresholds,
        .recommended_label_thresholds_csv = recommended_label_thresholds_csv,
        .diagnostic_limit = opts.diagnostic_limit,
        .diagnostics = diagnostics.items,
        .status = if (quality_gate_failure == null) "passed" else "failed",
    };

    if (opts.out_path) |out_path| try writeQualitySummary(init, out_path, summary);
    if (opts.thresholds_out_path) |thresholds_out_path| try writeTextFile(init, thresholds_out_path, recommended_label_thresholds_csv);

    const stdout = std.Io.File.stdout();
    var buf: [32768]u8 = undefined;
    var writer = stdout.writer(init.io, &buf);
    try std.json.Stringify.value(summary, .{ .whitespace = .indent_2 }, &writer.interface);
    try writer.interface.writeByte('\n');
    try writer.interface.flush();

    if (quality_gate_failure) |failure| return qualityGateFailureError(failure);
}

const QualityGateFailure = enum {
    precision,
    recall,
    f1,
};

fn qualityGateFailure(precision: f64, recall: f64, f1: f64, opts: Options) !?QualityGateFailure {
    if (opts.min_precision) |threshold| {
        if (!std.math.isFinite(threshold) or threshold < 0 or threshold > 1) return error.InvalidQualityThreshold;
        if (precision < threshold) return .precision;
    }
    if (opts.min_recall) |threshold| {
        if (!std.math.isFinite(threshold) or threshold < 0 or threshold > 1) return error.InvalidQualityThreshold;
        if (recall < threshold) return .recall;
    }
    if (opts.min_f1) |threshold| {
        if (!std.math.isFinite(threshold) or threshold < 0 or threshold > 1) return error.InvalidQualityThreshold;
        if (f1 < threshold) return .f1;
    }
    return null;
}

fn qualityGateFailureError(failure: QualityGateFailure) anyerror {
    return switch (failure) {
        .precision => error.EntityPrecisionBelowThreshold,
        .recall => error.EntityRecallBelowThreshold,
        .f1 => error.EntityF1BelowThreshold,
    };
}

fn writeQualitySummary(init: std.process.Init, out_path: []const u8, summary: QualitySummary) !void {
    var file = try compat.cwd().createFile(init.io, out_path, .{ .truncate = true });
    defer file.close(init.io);

    var buf: [32768]u8 = undefined;
    var writer = file.writer(init.io, &buf);
    try std.json.Stringify.value(summary, .{ .whitespace = .indent_2 }, &writer.interface);
    try writer.interface.writeByte('\n');
    try writer.end();
}

fn writeTextFile(init: std.process.Init, out_path: []const u8, text: []const u8) !void {
    var file = try compat.cwd().createFile(init.io, out_path, .{ .truncate = true });
    defer file.close(init.io);

    var buf: [4096]u8 = undefined;
    var writer = file.writer(init.io, &buf);
    try writer.interface.writeAll(text);
    try writer.interface.writeByte('\n');
    try writer.end();
}

fn bestThresholdMetric(metrics: []const ThresholdMetric) ?ThresholdMetric {
    if (metrics.len == 0) return null;
    var best = metrics[0];
    for (metrics[1..]) |metric| {
        if (metric.f1 > best.f1 or
            (metric.f1 == best.f1 and metric.precision > best.precision) or
            (metric.f1 == best.f1 and metric.precision == best.precision and metric.min_prediction_score > best.min_prediction_score))
        {
            best = metric;
        }
    }
    return best;
}

fn bestPerLabelThresholdMetrics(allocator: std.mem.Allocator, sweep_accums: []const EvalAccumulator, entity_types: []const []const u8) ![]LabelThresholdMetric {
    if (sweep_accums.len == 0) return try allocator.alloc(LabelThresholdMetric, 0);
    const out = try allocator.alloc(LabelThresholdMetric, entity_types.len);
    for (entity_types, 0..) |label, label_idx| {
        var best = labelThresholdMetric(label, sweep_accums[0].threshold, sweep_accums[0].label_metrics[label_idx]);
        for (sweep_accums[1..]) |accum| {
            const candidate = labelThresholdMetric(label, accum.threshold, accum.label_metrics[label_idx]);
            if (candidate.f1 > best.f1 or
                (candidate.f1 == best.f1 and candidate.precision > best.precision) or
                (candidate.f1 == best.f1 and candidate.precision == best.precision and candidate.min_prediction_score > best.min_prediction_score))
            {
                best = candidate;
            }
        }
        out[label_idx] = best;
    }
    return out;
}

fn labelThresholdMetric(label: []const u8, threshold: f32, metric: LabelMetric) LabelThresholdMetric {
    return .{
        .label = label,
        .min_prediction_score = threshold,
        .predicted_entity_count = metric.predicted,
        .correct_entity_count = metric.correct,
        .precision = metric.precision,
        .recall = metric.recall,
        .f1 = metric.f1,
    };
}

fn formatLabelThresholdCsv(allocator: std.mem.Allocator, metrics: []const LabelThresholdMetric) ![]const u8 {
    var buffer: std.Io.Writer.Allocating = .init(allocator);
    errdefer buffer.deinit();
    for (metrics, 0..) |metric, idx| {
        if (idx > 0) try buffer.writer.writeByte(',');
        try buffer.writer.print("{s}={d:.6}", .{ metric.label, metric.min_prediction_score });
    }
    return buffer.toOwnedSlice();
}

fn observeScores(score_stats: []LabelScoreStats, entity_types: []const []const u8, predictions: []const adapter_eval.TopEntity) void {
    for (predictions) |prediction| {
        if (indexOfLabel(entity_types, prediction.label)) |label_idx| score_stats[label_idx].observe(prediction.score);
    }
}

fn minDecodeThreshold(opts: Options) f32 {
    var threshold = opts.min_prediction_score;
    for (opts.sweep_thresholds) |candidate| threshold = @min(threshold, candidate);
    for (opts.label_thresholds) |entry| threshold = @min(threshold, entry.threshold);
    return threshold;
}

fn scoreExample(
    allocator: std.mem.Allocator,
    accum: *EvalAccumulator,
    ex: gliner2_data.Example,
    entity_types: []const []const u8,
    predictions: []const adapter_eval.TopEntity,
    opts: Options,
    diagnostic_ctx: ?*DiagnosticContext,
) !void {
    const selected = try selectPredictions(allocator, predictions, accum.threshold, opts);
    defer allocator.free(selected);
    var gold_matched = try allocator.alloc(bool, ex.entities.len);
    defer allocator.free(gold_matched);
    @memset(gold_matched, false);

    for (selected) |prediction_idx| {
        const prediction = predictions[prediction_idx];
        const pred_label_idx = indexOfLabel(entity_types, prediction.label) orelse continue;
        var correct = false;
        for (ex.entities, 0..) |ent, gold_idx| {
            if (gold_matched[gold_idx]) continue;
            if (std.mem.eql(u8, ent.label, prediction.label) and ent.start == prediction.start and ent.end == prediction.end) {
                gold_matched[gold_idx] = true;
                correct = true;
                break;
            }
        }
        accum.addPrediction(pred_label_idx, correct);
        if (!correct) {
            if (diagnostic_ctx) |ctx| try appendFalsePositiveDiagnostic(allocator, ctx, ex, prediction, entity_types[pred_label_idx]);
        }
    }

    if (diagnostic_ctx) |ctx| {
        for (ex.entities, 0..) |ent, gold_idx| {
            if (gold_matched[gold_idx]) continue;
            const label_idx = indexOfLabel(entity_types, ent.label) orelse continue;
            try appendFalseNegativeDiagnostic(allocator, ctx, ex, ent, entity_types[label_idx]);
        }
    }
}

fn appendFalsePositiveDiagnostic(
    allocator: std.mem.Allocator,
    ctx: *DiagnosticContext,
    ex: gliner2_data.Example,
    prediction: adapter_eval.TopEntity,
    label: []const u8,
) !void {
    try ctx.append(allocator, .{
        .kind = "false_positive",
        .example_index = ctx.example_index,
        .example_text = ex.text,
        .prediction = .{
            .text = spanText(ex.text, prediction.start, prediction.end),
            .label = label,
            .start = prediction.start,
            .end = prediction.end,
            .score = prediction.score,
        },
    });
}

fn appendFalseNegativeDiagnostic(
    allocator: std.mem.Allocator,
    ctx: *DiagnosticContext,
    ex: gliner2_data.Example,
    ent: gliner2_data.Entity,
    label: []const u8,
) !void {
    try ctx.append(allocator, .{
        .kind = "false_negative",
        .example_index = ctx.example_index,
        .example_text = ex.text,
        .gold = .{
            .text = if (ent.text.len > 0) ent.text else spanText(ex.text, ent.start, ent.end),
            .label = label,
            .start = ent.start,
            .end = ent.end,
        },
    });
}

fn spanText(text: []const u8, start: usize, end: usize) []const u8 {
    if (start > end or end > text.len) return "";
    return text[start..end];
}

fn selectPredictions(
    allocator: std.mem.Allocator,
    predictions: []const adapter_eval.TopEntity,
    threshold: f32,
    opts: Options,
) ![]usize {
    var candidates = std.ArrayListUnmanaged(usize).empty;
    defer candidates.deinit(allocator);
    for (predictions, 0..) |prediction, idx| {
        if (opts.require_entitylike_span and !adapter_eval.isEntityLikeSpanText(prediction.text)) continue;
        if (prediction.score >= thresholdForLabel(opts.label_thresholds, prediction.label, threshold)) try candidates.append(allocator, idx);
    }
    std.mem.sort(usize, candidates.items, predictions, predictionBetterThan);
    if (opts.nms_overlap_threshold == null and !opts.best_span_per_label_start and !opts.best_label_per_span_start and opts.top_k_per_label == null and opts.max_predictions_per_example == null) return candidates.toOwnedSlice(allocator);

    var selected = std.ArrayListUnmanaged(usize).empty;
    errdefer selected.deinit(allocator);
    var per_label_counts = std.StringHashMapUnmanaged(usize){};
    defer per_label_counts.deinit(allocator);
    for (candidates.items) |candidate_idx| {
        const candidate = predictions[candidate_idx];
        if (opts.max_predictions_per_example) |max_predictions| {
            if (selected.items.len >= max_predictions) break;
        }
        if (opts.top_k_per_label) |top_k| {
            if ((per_label_counts.get(candidate.label) orelse 0) >= top_k) continue;
        }
        var suppressed = false;
        for (selected.items) |selected_idx| {
            const existing = predictions[selected_idx];
            if (opts.best_label_per_span_start and candidate.start == existing.start) {
                suppressed = true;
                break;
            }
            if (!std.mem.eql(u8, candidate.label, existing.label)) continue;
            if (opts.best_span_per_label_start and candidate.start == existing.start) {
                suppressed = true;
                break;
            }
            if (opts.nms_overlap_threshold) |overlap_threshold| {
                if (spanOverlapRatio(candidate.start, candidate.end, existing.start, existing.end) > overlap_threshold) {
                    suppressed = true;
                    break;
                }
            }
        }
        if (!suppressed) {
            try selected.append(allocator, candidate_idx);
            if (opts.top_k_per_label != null) {
                const prior = per_label_counts.get(candidate.label) orelse 0;
                try per_label_counts.put(allocator, candidate.label, prior + 1);
            }
        }
    }
    return selected.toOwnedSlice(allocator);
}

fn thresholdForLabel(label_thresholds: []const LabelThreshold, label: []const u8, fallback: f32) f32 {
    for (label_thresholds) |entry| {
        if (std.mem.eql(u8, entry.label, label)) return entry.threshold;
    }
    return fallback;
}

fn predictionBetterThan(predictions: []const adapter_eval.TopEntity, lhs_idx: usize, rhs_idx: usize) bool {
    const lhs = predictions[lhs_idx];
    const rhs = predictions[rhs_idx];
    if (lhs.score != rhs.score) return lhs.score > rhs.score;
    const lhs_len = lhs.end - lhs.start;
    const rhs_len = rhs.end - rhs.start;
    if (lhs_len != rhs_len) return lhs_len < rhs_len;
    if (lhs.start != rhs.start) return lhs.start < rhs.start;
    return lhs.end < rhs.end;
}

fn spanOverlapRatio(a_start: usize, a_end: usize, b_start: usize, b_end: usize) f64 {
    if (a_end <= a_start or b_end <= b_start) return 0.0;
    const start = @max(a_start, b_start);
    const end = @min(a_end, b_end);
    if (end <= start) return 0.0;
    const intersection = end - start;
    const union_len = @max(a_end, b_end) - @min(a_start, b_start);
    if (union_len == 0) return 0.0;
    return @as(f64, @floatFromInt(intersection)) / @as(f64, @floatFromInt(union_len));
}

fn finishMetric(metric: *LabelMetric) void {
    metric.precision = ratio(metric.correct, metric.predicted);
    metric.recall = ratio(metric.correct, metric.gold);
    metric.f1 = f1Score(metric.precision, metric.recall);
}

fn ratio(num: usize, denom: usize) f64 {
    if (denom == 0) return 0.0;
    return @as(f64, @floatFromInt(num)) / @as(f64, @floatFromInt(denom));
}

fn f1Score(precision: f64, recall: f64) f64 {
    const denom = precision + recall;
    if (denom == 0.0) return 0.0;
    return 2.0 * precision * recall / denom;
}

fn parseOptions(args_in: std.process.Args, allocator: std.mem.Allocator) !?Options {
    var args = try std.process.Args.Iterator.initAllocator(args_in, allocator);
    defer args.deinit();
    _ = args.next();

    const model_dir = args.next() orelse return usageError();
    if (std.mem.eql(u8, model_dir, "--help") or std.mem.eql(u8, model_dir, "-h")) {
        printUsage();
        return null;
    }
    var opts = Options{
        .model_dir = model_dir,
        .adapter_dir = args.next() orelse return usageError(),
        .eval_data = args.next() orelse return usageError(),
        .entity_types_csv = args.next() orelse return usageError(),
    };
    var sweep_thresholds = std.ArrayListUnmanaged(f32).empty;
    errdefer sweep_thresholds.deinit(allocator);
    var label_thresholds = std.ArrayListUnmanaged(LabelThreshold).empty;
    errdefer {
        freeLabelThresholdLabels(allocator, label_thresholds.items);
        label_thresholds.deinit(allocator);
    }
    var label_score_biases = std.ArrayListUnmanaged(LabelScoreBias).empty;
    errdefer {
        freeLabelScoreBiasLabels(allocator, label_score_biases.items);
        label_score_biases.deinit(allocator);
    }

    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--seq-len")) {
            opts.seq_len = try parseUsize(args.next() orelse return usageError());
        } else if (std.mem.eql(u8, arg, "--max-span-width")) {
            opts.max_span_width = try parseUsize(args.next() orelse return usageError());
        } else if (std.mem.eql(u8, arg, "--backend")) {
            opts.backend = parseBackend(args.next() orelse return usageError()) orelse return error.InvalidBackend;
        } else if (std.mem.eql(u8, arg, "--compiled-required")) {
            opts.compiled_required = true;
        } else if (std.mem.eql(u8, arg, "--objective")) {
            opts.objective = try parseObjective(args.next() orelse return usageError());
        } else if (std.mem.eql(u8, arg, "--max-examples")) {
            opts.max_examples = try parseUsize(args.next() orelse return usageError());
        } else if (std.mem.eql(u8, arg, "--min-prediction-score")) {
            opts.min_prediction_score = try std.fmt.parseFloat(f32, args.next() orelse return usageError());
        } else if (std.mem.eql(u8, arg, "--label-thresholds")) {
            try parseLabelThresholdCsv(allocator, args.next() orelse return usageError(), &label_thresholds);
        } else if (std.mem.eql(u8, arg, "--label-score-biases")) {
            try parseLabelScoreBiasCsv(allocator, args.next() orelse return usageError(), &label_score_biases);
        } else if (std.mem.eql(u8, arg, "--sweep-thresholds")) {
            try parseThresholdCsv(allocator, args.next() orelse return usageError(), &sweep_thresholds);
        } else if (std.mem.eql(u8, arg, "--nms-overlap")) {
            opts.nms_overlap_threshold = try std.fmt.parseFloat(f64, args.next() orelse return usageError());
        } else if (std.mem.eql(u8, arg, "--no-nms")) {
            opts.nms_overlap_threshold = null;
        } else if (std.mem.eql(u8, arg, "--max-predictions-per-example")) {
            opts.max_predictions_per_example = try parseUsize(args.next() orelse return usageError());
        } else if (std.mem.eql(u8, arg, "--top-k-per-label")) {
            opts.top_k_per_label = try parseUsize(args.next() orelse return usageError());
        } else if (std.mem.eql(u8, arg, "--best-span-per-label-start")) {
            opts.best_span_per_label_start = true;
        } else if (std.mem.eql(u8, arg, "--best-label-per-span-start")) {
            opts.best_label_per_span_start = true;
        } else if (std.mem.eql(u8, arg, "--require-entitylike-span")) {
            opts.require_entitylike_span = true;
        } else if (std.mem.eql(u8, arg, "--min-precision")) {
            opts.min_precision = try std.fmt.parseFloat(f64, args.next() orelse return usageError());
        } else if (std.mem.eql(u8, arg, "--min-recall")) {
            opts.min_recall = try std.fmt.parseFloat(f64, args.next() orelse return usageError());
        } else if (std.mem.eql(u8, arg, "--min-f1")) {
            opts.min_f1 = try std.fmt.parseFloat(f64, args.next() orelse return usageError());
        } else if (std.mem.eql(u8, arg, "--out")) {
            opts.out_path = args.next() orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--thresholds-out")) {
            opts.thresholds_out_path = args.next() orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--diagnostic-limit")) {
            opts.diagnostic_limit = try parseUsize(args.next() orelse return usageError());
        } else if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) {
            printUsage();
            return null;
        } else {
            return usageError();
        }
    }
    if (!std.math.isFinite(opts.min_prediction_score) or opts.min_prediction_score < 0 or opts.min_prediction_score > 1) return error.InvalidQualityThreshold;
    if (opts.nms_overlap_threshold) |threshold| {
        if (!std.math.isFinite(threshold) or threshold < 0 or threshold > 1) return error.InvalidNmsOverlapThreshold;
    }
    opts.sweep_thresholds = try sweep_thresholds.toOwnedSlice(allocator);
    opts.label_thresholds = try label_thresholds.toOwnedSlice(allocator);
    opts.label_score_biases = try label_score_biases.toOwnedSlice(allocator);
    return opts;
}

fn parseUsize(value: []const u8) !usize {
    return std.fmt.parseUnsigned(usize, value, 10);
}

fn parseObjective(value: []const u8) !gliner2_autodiff.GlinerObjective {
    if (std.mem.eql(u8, value, "token")) return .token;
    if (std.mem.eql(u8, value, "span-start") or std.mem.eql(u8, value, "span_start")) return .span_start;
    return error.InvalidObjective;
}

fn parseBackend(value: []const u8) ?adapter_eval.EvalBackend {
    if (std.ascii.eqlIgnoreCase(value, "auto")) return .auto;
    if (std.ascii.eqlIgnoreCase(value, "metal")) return .metal;
    if (std.ascii.eqlIgnoreCase(value, "mlx")) return .mlx;
    if (std.ascii.eqlIgnoreCase(value, "native")) return .native;
    return null;
}

fn objectiveName(objective: gliner2_autodiff.GlinerObjective) []const u8 {
    return switch (objective) {
        .token => "token",
        .span_start => "span-start",
    };
}

fn parseCsv(allocator: std.mem.Allocator, value: []const u8) ![][]const u8 {
    var out = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (out.items) |item| allocator.free(item);
        out.deinit(allocator);
    }
    var iter = std.mem.splitScalar(u8, value, ',');
    while (iter.next()) |raw| {
        const item = std.mem.trim(u8, raw, " \t\r\n");
        if (item.len == 0) continue;
        try out.append(allocator, try allocator.dupe(u8, item));
    }
    if (out.items.len == 0) return error.EmptyCsv;
    return try out.toOwnedSlice(allocator);
}

fn parseThresholdCsv(allocator: std.mem.Allocator, value: []const u8, out: *std.ArrayListUnmanaged(f32)) !void {
    var iter = std.mem.splitScalar(u8, value, ',');
    while (iter.next()) |raw| {
        const item = std.mem.trim(u8, raw, " \t\r\n");
        if (item.len == 0) continue;
        const threshold = try std.fmt.parseFloat(f32, item);
        if (!std.math.isFinite(threshold) or threshold < 0 or threshold > 1) return error.InvalidQualityThreshold;
        try out.append(allocator, threshold);
    }
}

fn parseLabelThresholdCsv(allocator: std.mem.Allocator, value: []const u8, out: *std.ArrayListUnmanaged(LabelThreshold)) !void {
    var iter = std.mem.splitScalar(u8, value, ',');
    while (iter.next()) |raw| {
        const item = std.mem.trim(u8, raw, " \t\r\n");
        if (item.len == 0) continue;
        const eq_idx = std.mem.indexOfScalar(u8, item, '=') orelse return error.InvalidLabelThreshold;
        const label = std.mem.trim(u8, item[0..eq_idx], " \t\r\n");
        const threshold_text = std.mem.trim(u8, item[eq_idx + 1 ..], " \t\r\n");
        if (label.len == 0 or threshold_text.len == 0) return error.InvalidLabelThreshold;
        if (containsLabelThreshold(out.items, label)) return error.DuplicateLabelThreshold;
        const threshold = try std.fmt.parseFloat(f32, threshold_text);
        if (!std.math.isFinite(threshold) or threshold < 0 or threshold > 1) return error.InvalidQualityThreshold;
        try out.append(allocator, .{
            .label = try allocator.dupe(u8, label),
            .threshold = threshold,
        });
    }
}

fn parseLabelScoreBiasCsv(allocator: std.mem.Allocator, value: []const u8, out: *std.ArrayListUnmanaged(LabelScoreBias)) !void {
    var iter = std.mem.splitScalar(u8, value, ',');
    while (iter.next()) |raw| {
        const item = std.mem.trim(u8, raw, " \t\r\n");
        if (item.len == 0) continue;
        const eq_idx = std.mem.indexOfScalar(u8, item, '=') orelse return error.InvalidLabelScoreBias;
        const label = std.mem.trim(u8, item[0..eq_idx], " \t\r\n");
        const bias_text = std.mem.trim(u8, item[eq_idx + 1 ..], " \t\r\n");
        if (label.len == 0 or bias_text.len == 0) return error.InvalidLabelScoreBias;
        if (containsLabelScoreBias(out.items, label)) return error.DuplicateLabelScoreBias;
        const bias = try std.fmt.parseFloat(f32, bias_text);
        if (!std.math.isFinite(bias)) return error.InvalidLabelScoreBias;
        try out.append(allocator, .{
            .label = try allocator.dupe(u8, label),
            .bias = bias,
        });
    }
}

fn containsLabelThreshold(values: []const LabelThreshold, label: []const u8) bool {
    for (values) |entry| {
        if (std.mem.eql(u8, entry.label, label)) return true;
    }
    return false;
}

fn containsLabelScoreBias(values: []const LabelScoreBias, label: []const u8) bool {
    for (values) |entry| {
        if (std.mem.eql(u8, entry.label, label)) return true;
    }
    return false;
}

fn validateLabelThresholds(label_thresholds: []const LabelThreshold, entity_types: []const []const u8) !void {
    for (label_thresholds) |entry| {
        if (indexOfLabel(entity_types, entry.label) == null) return error.UnknownLabelThreshold;
    }
}

fn validateLabelScoreBiases(label_score_biases: []const LabelScoreBias, entity_types: []const []const u8) !void {
    for (label_score_biases) |entry| {
        if (indexOfLabel(entity_types, entry.label) == null) return error.UnknownLabelScoreBias;
    }
}

fn freeLabelThresholds(allocator: std.mem.Allocator, values: []const LabelThreshold) void {
    freeLabelThresholdLabels(allocator, values);
    allocator.free(values);
}

fn freeLabelThresholdLabels(allocator: std.mem.Allocator, values: []const LabelThreshold) void {
    for (values) |entry| allocator.free(entry.label);
}

fn freeLabelScoreBiases(allocator: std.mem.Allocator, values: []const LabelScoreBias) void {
    freeLabelScoreBiasLabels(allocator, values);
    allocator.free(values);
}

fn freeLabelScoreBiasLabels(allocator: std.mem.Allocator, values: []const LabelScoreBias) void {
    for (values) |entry| allocator.free(entry.label);
}

fn freeStringSlice(allocator: std.mem.Allocator, values: []const []const u8) void {
    for (values) |value| allocator.free(value);
    allocator.free(values);
}

fn indexOfLabel(labels: []const []const u8, label: []const u8) ?usize {
    for (labels, 0..) |candidate, idx| {
        if (std.mem.eql(u8, candidate, label)) return idx;
    }
    return null;
}

fn usageError() error{InvalidArguments} {
    printUsage();
    return error.InvalidArguments;
}

fn printUsage() void {
    std.debug.print(
        \\usage: eval-gliner2-autodiff-adapter-dataset <model_dir> <adapter_dir> <eval_jsonl_or_dir> <entity_types_csv> [options]
        \\
        \\Reports exact-match entity precision/recall/F1 for a saved GLiNER2 autodiff adapter.
        \\Evaluates all decoded entities above --min-prediction-score.
        \\
        \\options:
        \\  --seq-len N
        \\  --max-span-width N
        \\  --backend auto|metal|mlx|native
        \\  --compiled-required
        \\  --objective token|span-start
        \\  --max-examples N
        \\  --min-prediction-score FLOAT
        \\  --label-thresholds label=FLOAT[,label=FLOAT...]
        \\  --label-score-biases label=FLOAT[,label=FLOAT...]
        \\  --sweep-thresholds CSV
        \\  --nms-overlap FLOAT
        \\  --no-nms
        \\  --max-predictions-per-example N
        \\  --top-k-per-label N
        \\  --best-span-per-label-start
        \\  --best-label-per-span-start
        \\  --require-entitylike-span
        \\  --min-precision FLOAT
        \\  --min-recall FLOAT
        \\  --min-f1 FLOAT
        \\  --out PATH
        \\  --thresholds-out PATH
        \\  --diagnostic-limit N
        \\
    , .{});
}

test "dataset evaluator records false positive and false negative diagnostics" {
    const allocator = std.testing.allocator;
    const labels = [_][]const u8{"person"};
    var accum = try EvalAccumulator.init(allocator, 0.5, &labels);
    defer accum.deinit(allocator);
    accum.addGold(0);

    const entities = [_]gliner2_data.Entity{.{
        .text = "Alice",
        .label = "person",
        .start = 0,
        .end = 5,
    }};
    const ex = gliner2_data.Example{
        .text = "Alice met Bob",
        .entities = &entities,
    };
    const predictions = [_]adapter_eval.TopEntity{.{
        .text = "Bob",
        .label = "person",
        .start = 10,
        .end = 13,
        .score = 0.9,
    }};
    const opts = Options{
        .model_dir = "",
        .adapter_dir = "",
        .eval_data = "",
        .entity_types_csv = "person",
        .min_prediction_score = 0.5,
    };
    var diagnostics = std.ArrayListUnmanaged(QualityDiagnostic).empty;
    defer diagnostics.deinit(allocator);
    var diagnostic_ctx = DiagnosticContext{
        .items = &diagnostics,
        .example_index = 7,
        .limit = 8,
    };

    try scoreExample(allocator, &accum, ex, &labels, &predictions, opts, &diagnostic_ctx);

    try std.testing.expectEqual(@as(usize, 1), accum.predicted_total);
    try std.testing.expectEqual(@as(usize, 0), accum.correct_total);
    try std.testing.expectEqual(@as(usize, 2), diagnostics.items.len);
    try std.testing.expectEqualStrings("false_positive", diagnostics.items[0].kind);
    try std.testing.expectEqualStrings("Bob", diagnostics.items[0].prediction.?.text);
    try std.testing.expectEqualStrings("false_negative", diagnostics.items[1].kind);
    try std.testing.expectEqualStrings("Alice", diagnostics.items[1].gold.?.text);
}

test "dataset evaluator formats reusable per-label threshold csv" {
    const allocator = std.testing.allocator;
    const metrics = [_]LabelThresholdMetric{
        .{
            .label = "person",
            .min_prediction_score = 0.15,
            .predicted_entity_count = 2,
            .correct_entity_count = 1,
            .precision = 0.5,
            .recall = 0.25,
            .f1 = 0.333,
        },
        .{
            .label = "organization",
            .min_prediction_score = 0.25,
            .predicted_entity_count = 1,
            .correct_entity_count = 1,
            .precision = 1.0,
            .recall = 0.5,
            .f1 = 0.666,
        },
    };
    const csv = try formatLabelThresholdCsv(allocator, &metrics);
    defer allocator.free(csv);

    try std.testing.expectEqualStrings("person=0.150000,organization=0.250000", csv);
}
