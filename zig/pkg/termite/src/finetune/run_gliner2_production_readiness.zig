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
const termite = @import("termite_internal");

const gliner2 = termite.finetune.gliner2;
const gliner2_data = termite.finetune.gliner2_data;
const validation = termite.finetune.gliner2_run_validation;

const train_gliner2_autodiff = @import("train/train_gliner2_autodiff.zig");
const eval_gliner2_autodiff_adapter = @import("tools/eval_gliner2_autodiff_adapter.zig");
const eval_gliner2_autodiff_adapter_dataset = @import("tools/eval_gliner2_autodiff_adapter_dataset.zig");
const materialize_gliner2_lora = @import("tools/materialize_gliner2_lora.zig");

const CommandMain = *const fn (std.process.Init) anyerror!void;

const SemanticGolden = struct {
    text: []const u8,
    expect_text: []const u8,
    expect_label: []const u8,
    min_score: []const u8,
};

const Options = struct {
    model_dir: []const u8,
    train_data: []const u8,
    eval_data: []const u8,
    out_dir: []const u8,
    entity_types_csv: []const u8,

    epochs: []const u8 = "5",
    batch_size: []const u8 = "1",
    max_examples: []const u8 = "100",
    seq_len: []const u8 = "256",
    learning_rate: []const u8 = "1e-3",
    lora_rank: []const u8 = "16",
    lora_alpha: []const u8 = "32",
    objective: []const u8 = "span-start",
    max_span_width: []const u8 = "4",
    span_loss: []const u8 = "bce",
    span_positive_weight: []const u8 = "32",
    span_label_positive_weights: ?[]const u8 = null,
    span_negative_weight: []const u8 = "1",
    span_hard_negative_weight: []const u8 = "1",
    max_grad_norm: []const u8 = "1.0",
    grad_accum: []const u8 = "1",
    seed: []const u8 = "42",
    backend: []const u8 = "auto",
    compiled_required: bool = false,
    production_metal_gate: bool = false,
    production_mlx_gate: bool = false,
    num_classes_override: ?[]const u8 = null,

    min_train_examples: usize = 100,
    min_eval_examples: usize = 20,
    min_total_entities: usize = 100,
    min_unique_labels: usize = 3,
    min_target_coverage_ratio: f64 = 0.95,
    min_positive_span_labels: usize = 100,
    min_positive_rate_per_label: f64 = 0.0,

    min_steps: ?usize = 100,
    min_supervised_tokens: ?usize = 1000,
    min_entity_tokens: ?usize = 100,
    min_supervised_tokens_per_second: ?f64 = null,
    max_avg_step_wall_ms: ?f64 = null,
    max_total_execute_ms: ?f64 = null,
    max_peak_resident_bytes: ?usize = null,
    max_device_trainable_transfer_count: ?u64 = null,
    max_device_resident_transfer_count: ?u64 = null,
    min_device_trainable_bytes: ?usize = null,
    require_loss_decrease: bool = true,

    eval_text: ?[]const u8 = null,
    expect_text: ?[]const u8 = null,
    expect_label: ?[]const u8 = null,
    min_score: ?[]const u8 = null,
    semantic_goldens: [16]SemanticGolden = undefined,
    semantic_golden_count: usize = 0,
    skip_semantic_eval: bool = false,
    semantic_min_prediction_score: ?[]const u8 = null,
    semantic_label_thresholds: ?[]const u8 = null,
    semantic_label_score_biases: ?[]const u8 = null,
    semantic_nms_overlap: ?[]const u8 = null,
    semantic_max_predictions: ?[]const u8 = null,
    semantic_top_k_per_label: ?[]const u8 = null,
    semantic_best_span_per_label_start: bool = false,
    semantic_best_label_per_span_start: bool = false,
    semantic_require_entitylike_span: bool = false,
    quality_eval: bool = false,
    quality_max_examples: ?[]const u8 = null,
    quality_min_prediction_score: ?[]const u8 = null,
    quality_label_thresholds: ?[]const u8 = null,
    quality_label_score_biases: ?[]const u8 = null,
    quality_sweep_thresholds: ?[]const u8 = null,
    quality_nms_overlap: ?[]const u8 = null,
    quality_disable_nms: bool = false,
    quality_max_predictions_per_example: ?[]const u8 = null,
    quality_top_k_per_label: ?[]const u8 = null,
    quality_best_span_per_label_start: bool = false,
    quality_best_label_per_span_start: bool = false,
    quality_require_entitylike_span: bool = false,
    quality_diagnostic_limit: []const u8 = "50",
    min_entity_precision: ?[]const u8 = null,
    min_entity_recall: ?[]const u8 = null,
    min_entity_f1: ?[]const u8 = null,

    materialized_dir: ?[]const u8 = null,
    dry_run: bool = false,
};

const ReadinessGateSummary = struct {
    model_dir: []const u8,
    train_data: []const u8,
    eval_data: []const u8,
    out_dir: []const u8,
    entity_types: []const []const u8,
    objective: []const u8,
    num_classes: usize,
    train_dataset: gliner2_data.DatasetReadinessSummary,
    eval_dataset: gliner2_data.DatasetReadinessSummary,
    run_validation: validation.RunValidationSummary,
    lora_inspection: gliner2.LoRABundleInspectionSummary,
    semantic_eval_required: bool,
    quality_summary_path: ?[]const u8,
    quality_thresholds_path: ?[]const u8,
    materialized_dir: ?[]const u8,
    status: []const u8 = "passed",
};

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, allocator);
    defer args.deinit();
    _ = args.next();

    const opts = try parseOptions(&args) orelse return;
    try runReadiness(init, allocator, opts);
}

fn runReadiness(init: std.process.Init, allocator: std.mem.Allocator, opts: Options) !void {
    if (opts.dry_run) {
        try printDryRun(init, opts);
        return;
    }

    const entity_types = try parseCsvOwned(allocator, opts.entity_types_csv);
    defer freeStringList(allocator, entity_types);
    if (entity_types.len == 0) return error.NoEntityTypesProvided;

    var train_loaded = try gliner2_data.loadExamples(allocator, opts.train_data, null);
    defer train_loaded.deinit();
    var eval_loaded = try gliner2_data.loadExamples(allocator, opts.eval_data, null);
    defer eval_loaded.deinit();

    const seq_len = try std.fmt.parseUnsigned(usize, opts.seq_len, 10);
    const max_span_width = try std.fmt.parseUnsigned(usize, opts.max_span_width, 10);
    const batch_size = try std.fmt.parseUnsigned(usize, opts.batch_size, 10);

    var train_readiness = try gliner2_data.evaluateDatasetReadiness(
        allocator,
        train_loaded.examples,
        entity_types,
        seq_len,
        max_span_width,
        batch_size,
        .{
            .min_examples = opts.min_train_examples,
            .min_total_entities = opts.min_total_entities,
            .min_unique_labels = opts.min_unique_labels,
            .min_target_entities = opts.min_total_entities,
            .min_target_coverage_ratio = opts.min_target_coverage_ratio,
            .require_all_examples_with_target = false,
            .min_positive_span_labels = opts.min_positive_span_labels,
            .min_positive_rate_per_label = opts.min_positive_rate_per_label,
        },
    );
    errdefer gliner2_data.freeDatasetReadinessSummary(allocator, &train_readiness);
    if (!train_readiness.passed) return error.TrainDatasetReadinessFailed;

    var eval_readiness = try gliner2_data.evaluateDatasetReadiness(
        allocator,
        eval_loaded.examples,
        entity_types,
        seq_len,
        max_span_width,
        batch_size,
        .{
            .min_examples = opts.min_eval_examples,
            .min_total_entities = @min(opts.min_total_entities, opts.min_eval_examples),
            .min_unique_labels = opts.min_unique_labels,
            .min_target_entities = @min(opts.min_total_entities, opts.min_eval_examples),
            .min_target_coverage_ratio = opts.min_target_coverage_ratio,
            .require_all_examples_with_target = false,
            .min_positive_span_labels = @min(opts.min_positive_span_labels, opts.min_eval_examples),
            .min_positive_rate_per_label = opts.min_positive_rate_per_label,
        },
    );
    errdefer gliner2_data.freeDatasetReadinessSummary(allocator, &eval_readiness);
    if (!eval_readiness.passed) return error.EvalDatasetReadinessFailed;

    const num_classes = try resolveNumClasses(allocator, train_loaded.examples, entity_types, opts.num_classes_override);
    var num_classes_buf: [32]u8 = undefined;
    const num_classes_arg = try std.fmt.bufPrint(&num_classes_buf, "{d}", .{num_classes});

    const train_args = [_][]const u8{
        "--model-dir",                 opts.model_dir,
        "--train-data",                opts.train_data,
        "--out-dir",                   opts.out_dir,
        "--epochs",                    opts.epochs,
        "--batch-size",                opts.batch_size,
        "--max-examples",              opts.max_examples,
        "--seq-len",                   opts.seq_len,
        "--num-classes",               num_classes_arg,
        "--entity-types",              opts.entity_types_csv,
        "--learning-rate",             opts.learning_rate,
        "--lora-rank",                 opts.lora_rank,
        "--lora-alpha",                opts.lora_alpha,
        "--objective",                 opts.objective,
        "--max-span-width",            opts.max_span_width,
        "--span-loss",                 opts.span_loss,
        "--span-positive-weight",      opts.span_positive_weight,
        "--span-hard-negative-weight", opts.span_hard_negative_weight,
        "--max-grad-norm",             opts.max_grad_norm,
        "--grad-accum",                opts.grad_accum,
        "--seed",                      opts.seed,
        "--backend",                   opts.backend,
    };
    var train_args_list = std.ArrayListUnmanaged([]const u8).empty;
    defer train_args_list.deinit(allocator);
    try train_args_list.appendSlice(allocator, &train_args);
    if (opts.span_label_positive_weights) |value| try train_args_list.appendSlice(allocator, &.{ "--span-label-positive-weights", value });
    try train_args_list.appendSlice(allocator, &.{ "--span-negative-weight", opts.span_negative_weight });
    if (opts.compiled_required) try train_args_list.append(allocator, "--compiled-required");
    try runCommand(init, allocator, "train-gliner2-autodiff", train_gliner2_autodiff.main, train_args_list.items);

    const metal_required = std.mem.eql(u8, opts.backend, "metal");
    const mlx_required = std.mem.eql(u8, opts.backend, "mlx");
    const device_optimizer_required = metal_required or mlx_required;
    const min_device_trainable_bytes: ?usize = opts.min_device_trainable_bytes orelse if (device_optimizer_required) @as(usize, 1) else null;
    var run_summary = try validation.validateRun(allocator, opts.out_dir, .{
        .require_loss_decrease = opts.require_loss_decrease,
        .min_supervised_tokens_per_second = opts.min_supervised_tokens_per_second,
        .max_avg_step_wall_ms = opts.max_avg_step_wall_ms,
        .max_total_execute_ms = opts.max_total_execute_ms,
        .max_peak_resident_bytes = opts.max_peak_resident_bytes,
        .min_examples = opts.min_train_examples,
        .min_steps = opts.min_steps,
        .min_entity_labels = opts.min_unique_labels,
        .min_supervised_tokens = opts.min_supervised_tokens,
        .min_entity_tokens = opts.min_entity_tokens,
        .require_backend = if (metal_required)
            "Metal"
        else if (mlx_required)
            "MLX (Apple Silicon)"
        else
            null,
        .require_optimizer_backend = if (metal_required)
            "metal"
        else if (mlx_required)
            "mlx"
        else
            null,
        .max_device_trainable_transfer_count = opts.max_device_trainable_transfer_count orelse opts.max_device_resident_transfer_count,
        .max_device_resident_transfer_count = opts.max_device_resident_transfer_count,
        .min_device_trainable_bytes = min_device_trainable_bytes,
    });
    errdefer validation.freeRunValidationSummary(allocator, &run_summary);

    var lora_summary = try gliner2.inspectLoRABundle(allocator, opts.model_dir, opts.out_dir);
    errdefer gliner2.freeLoRABundleInspectionSummary(allocator, &lora_summary);
    if (lora_summary.resolved_tensor_count == 0) return error.NoPeftAdapterTensors;

    const semantic_required = !opts.skip_semantic_eval;
    if (semantic_required) {
        if (opts.semantic_golden_count > 0) {
            for (opts.semantic_goldens[0..opts.semantic_golden_count]) |golden| {
                try runSemanticEval(init, allocator, opts, golden);
            }
        } else {
            const eval_text = opts.eval_text orelse return error.MissingSemanticEvalText;
            const expect_label = opts.expect_label orelse return error.MissingSemanticExpectedLabel;
            const min_score = opts.min_score orelse return error.MissingSemanticMinScore;
            try runSemanticEval(init, allocator, opts, .{
                .text = eval_text,
                .expect_text = opts.expect_text orelse "",
                .expect_label = expect_label,
                .min_score = min_score,
            });
        }
    }

    var quality_summary_path: ?[]const u8 = null;
    var quality_thresholds_path: ?[]const u8 = null;
    defer if (quality_summary_path) |path| allocator.free(path);
    defer if (quality_thresholds_path) |path| allocator.free(path);
    if (opts.quality_eval) {
        quality_summary_path = try std.fs.path.join(allocator, &.{ opts.out_dir, "quality_summary.json" });
        quality_thresholds_path = try std.fs.path.join(allocator, &.{ opts.out_dir, "quality_thresholds.csv" });
        try runQualityEval(init, allocator, opts, quality_summary_path.?, quality_thresholds_path.?);
    }

    if (opts.materialized_dir) |materialized_dir| {
        const materialize_args = [_][]const u8{ opts.model_dir, opts.out_dir, materialized_dir };
        try runCommand(init, allocator, "materialize-gliner2-lora", materialize_gliner2_lora.main, &materialize_args);
    }

    const report = ReadinessGateSummary{
        .model_dir = opts.model_dir,
        .train_data = opts.train_data,
        .eval_data = opts.eval_data,
        .out_dir = opts.out_dir,
        .entity_types = entity_types,
        .objective = opts.objective,
        .num_classes = num_classes,
        .train_dataset = train_readiness,
        .eval_dataset = eval_readiness,
        .run_validation = run_summary,
        .lora_inspection = lora_summary,
        .semantic_eval_required = semantic_required,
        .quality_summary_path = quality_summary_path,
        .quality_thresholds_path = quality_thresholds_path,
        .materialized_dir = opts.materialized_dir,
    };
    try printJson(init, report);

    gliner2.freeLoRABundleInspectionSummary(allocator, &lora_summary);
    validation.freeRunValidationSummary(allocator, &run_summary);
    gliner2_data.freeDatasetReadinessSummary(allocator, &eval_readiness);
    gliner2_data.freeDatasetReadinessSummary(allocator, &train_readiness);
}

fn runQualityEval(
    init: std.process.Init,
    allocator: std.mem.Allocator,
    opts: Options,
    quality_summary_path: []const u8,
    quality_thresholds_path: []const u8,
) !void {
    var quality_args_list = std.ArrayListUnmanaged([]const u8).empty;
    defer quality_args_list.deinit(allocator);
    try quality_args_list.appendSlice(allocator, &.{
        opts.model_dir,
        opts.out_dir,
        opts.eval_data,
        opts.entity_types_csv,
        "--backend",
        opts.backend,
        "--seq-len",
        opts.seq_len,
        "--max-span-width",
        opts.max_span_width,
        "--objective",
        opts.objective,
        "--out",
        quality_summary_path,
        "--thresholds-out",
        quality_thresholds_path,
        "--diagnostic-limit",
        opts.quality_diagnostic_limit,
    });
    if (opts.quality_max_examples) |value| try quality_args_list.appendSlice(allocator, &.{ "--max-examples", value });
    if (opts.quality_min_prediction_score) |value| try quality_args_list.appendSlice(allocator, &.{ "--min-prediction-score", value });
    if (opts.quality_label_thresholds) |value| try quality_args_list.appendSlice(allocator, &.{ "--label-thresholds", value });
    if (opts.quality_label_score_biases) |value| try quality_args_list.appendSlice(allocator, &.{ "--label-score-biases", value });
    if (opts.quality_sweep_thresholds) |value| try quality_args_list.appendSlice(allocator, &.{ "--sweep-thresholds", value });
    if (opts.quality_nms_overlap) |value| try quality_args_list.appendSlice(allocator, &.{ "--nms-overlap", value });
    if (opts.quality_disable_nms) try quality_args_list.append(allocator, "--no-nms");
    if (opts.quality_max_predictions_per_example) |value| try quality_args_list.appendSlice(allocator, &.{ "--max-predictions-per-example", value });
    if (opts.quality_top_k_per_label) |value| try quality_args_list.appendSlice(allocator, &.{ "--top-k-per-label", value });
    if (opts.quality_best_span_per_label_start) try quality_args_list.append(allocator, "--best-span-per-label-start");
    if (opts.quality_best_label_per_span_start) try quality_args_list.append(allocator, "--best-label-per-span-start");
    if (opts.quality_require_entitylike_span) try quality_args_list.append(allocator, "--require-entitylike-span");
    if (opts.compiled_required) try quality_args_list.append(allocator, "--compiled-required");
    if (opts.min_entity_precision) |value| try quality_args_list.appendSlice(allocator, &.{ "--min-precision", value });
    if (opts.min_entity_recall) |value| try quality_args_list.appendSlice(allocator, &.{ "--min-recall", value });
    if (opts.min_entity_f1) |value| try quality_args_list.appendSlice(allocator, &.{ "--min-f1", value });
    try runCommand(init, allocator, "eval-gliner2-autodiff-adapter-dataset", eval_gliner2_autodiff_adapter_dataset.main, quality_args_list.items);
}

fn runSemanticEval(
    init: std.process.Init,
    allocator: std.mem.Allocator,
    opts: Options,
    golden: SemanticGolden,
) !void {
    var eval_args_list = std.ArrayListUnmanaged([]const u8).empty;
    defer eval_args_list.deinit(allocator);
    try eval_args_list.appendSlice(allocator, &.{
        opts.model_dir,
        opts.out_dir,
        golden.text,
        opts.entity_types_csv,
        "--backend",
        opts.backend,
        "--seq-len",
        opts.seq_len,
        "--max-span-width",
        opts.max_span_width,
        "--objective",
        opts.objective,
        "--expect-label",
        golden.expect_label,
        "--min-score",
        golden.min_score,
    });
    if (golden.expect_text.len > 0) {
        try eval_args_list.appendSlice(allocator, &.{ "--expect-text", golden.expect_text });
    }
    if (opts.semantic_min_prediction_score) |value| try eval_args_list.appendSlice(allocator, &.{ "--min-prediction-score", value });
    if (opts.semantic_label_thresholds) |value| try eval_args_list.appendSlice(allocator, &.{ "--label-thresholds", value });
    if (opts.semantic_label_score_biases) |value| try eval_args_list.appendSlice(allocator, &.{ "--label-score-biases", value });
    if (opts.semantic_nms_overlap) |value| try eval_args_list.appendSlice(allocator, &.{ "--nms-overlap", value });
    if (opts.semantic_max_predictions) |value| try eval_args_list.appendSlice(allocator, &.{ "--max-predictions", value });
    if (opts.semantic_top_k_per_label) |value| try eval_args_list.appendSlice(allocator, &.{ "--top-k-per-label", value });
    if (opts.semantic_best_span_per_label_start) try eval_args_list.append(allocator, "--best-span-per-label-start");
    if (opts.semantic_best_label_per_span_start) try eval_args_list.append(allocator, "--best-label-per-span-start");
    if (opts.semantic_require_entitylike_span) try eval_args_list.append(allocator, "--require-entitylike-span");
    if (opts.compiled_required) try eval_args_list.append(allocator, "--compiled-required");
    try runCommand(init, allocator, "eval-gliner2-autodiff-adapter", eval_gliner2_autodiff_adapter.main, eval_args_list.items);
}

fn resolveNumClasses(
    allocator: std.mem.Allocator,
    examples: []const gliner2_data.Example,
    entity_types: []const []const u8,
    override: ?[]const u8,
) !usize {
    if (override) |value| return try std.fmt.parseUnsigned(usize, value, 10);
    const vocab = try gliner2_data.buildLabelVocab(allocator, examples, null);
    defer {
        for (vocab) |label| allocator.free(label);
        allocator.free(vocab);
    }
    return @max(vocab.len, entity_types.len) + 1;
}

fn runCommand(init: std.process.Init, allocator: std.mem.Allocator, argv0: []const u8, main_fn: CommandMain, args: []const []const u8) !void {
    printCommand(argv0, args);

    var owned = try allocator.alloc([:0]u8, args.len + 1);
    defer {
        for (owned) |arg| allocator.free(arg);
        allocator.free(owned);
    }
    var vector = try allocator.alloc([*:0]const u8, args.len + 1);
    defer allocator.free(vector);

    owned[0] = try allocator.dupeZ(u8, argv0);
    vector[0] = owned[0].ptr;
    for (args, 0..) |arg, idx| {
        owned[idx + 1] = try allocator.dupeZ(u8, arg);
        vector[idx + 1] = owned[idx + 1].ptr;
    }

    var command_init = init;
    command_init.minimal.args = .{ .vector = vector };
    try main_fn(command_init);
}

fn printCommand(argv0: []const u8, args: []const []const u8) void {
    std.debug.print("+ {s}", .{argv0});
    for (args) |arg| std.debug.print(" {s}", .{arg});
    std.debug.print("\n", .{});
}

fn parseCsvOwned(allocator: std.mem.Allocator, csv: []const u8) ![][]const u8 {
    var out = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (out.items) |item| allocator.free(item);
        out.deinit(allocator);
    }
    var it = std.mem.splitScalar(u8, csv, ',');
    while (it.next()) |raw| {
        const trimmed = std.mem.trim(u8, raw, " \t\r\n");
        if (trimmed.len == 0) continue;
        try out.append(allocator, try allocator.dupe(u8, trimmed));
    }
    return out.toOwnedSlice(allocator);
}

fn freeStringList(allocator: std.mem.Allocator, values: []const []const u8) void {
    for (values) |value| allocator.free(value);
    allocator.free(values);
}

fn printDryRun(init: std.process.Init, opts: Options) !void {
    const stdout = std.Io.File.stdout();
    var buf: [4096]u8 = undefined;
    var writer = stdout.writer(init.io, &buf);
    try writer.interface.print(
        \\gliner2_production_readiness_dry_run: true
        \\model_dir: {s}
        \\train_data: {s}
        \\eval_data: {s}
        \\out_dir: {s}
        \\entity_types: {s}
        \\objective: {s}
        \\production_metal_gate: {}
        \\production_mlx_gate: {}
        \\backend: {s}
        \\compiled_required: {}
        \\max_examples: {s}
        \\seq_len: {s}
        \\batch_size: {s}
        \\span_loss: {s}
        \\span_positive_weight: {s}
        \\span_label_positive_weights: {?s}
        \\span_negative_weight: {s}
        \\span_hard_negative_weight: {s}
        \\max_avg_step_wall_ms: {?d}
        \\max_device_trainable_transfer_count: {?}
        \\max_device_resident_transfer_count: {?}
        \\min_device_trainable_bytes: {?}
        \\semantic_golden_count: {}
        \\quality_eval: {}
        \\quality_max_examples: {?s}
        \\quality_min_prediction_score: {?s}
        \\quality_diagnostic_limit: {s}
        \\min_entity_f1: {?s}
        \\semantic_eval_required: {}
        \\
    , .{
        opts.model_dir,
        opts.train_data,
        opts.eval_data,
        opts.out_dir,
        opts.entity_types_csv,
        opts.objective,
        opts.production_metal_gate,
        opts.production_mlx_gate,
        opts.backend,
        opts.compiled_required,
        opts.max_examples,
        opts.seq_len,
        opts.batch_size,
        opts.span_loss,
        opts.span_positive_weight,
        opts.span_label_positive_weights,
        opts.span_negative_weight,
        opts.span_hard_negative_weight,
        opts.max_avg_step_wall_ms,
        opts.max_device_trainable_transfer_count,
        opts.max_device_resident_transfer_count,
        opts.min_device_trainable_bytes,
        opts.semantic_golden_count,
        opts.quality_eval,
        opts.quality_max_examples,
        opts.quality_min_prediction_score,
        opts.quality_diagnostic_limit,
        opts.min_entity_f1,
        !opts.skip_semantic_eval,
    });
    try writer.interface.flush();
}

fn printJson(init: std.process.Init, value: anytype) !void {
    const stdout = std.Io.File.stdout();
    var buf: [32768]u8 = undefined;
    var writer = stdout.writer(init.io, &buf);
    try std.json.Stringify.value(value, .{ .whitespace = .indent_2 }, &writer.interface);
    try writer.interface.writeByte('\n');
    try writer.interface.flush();
}

fn parseOptions(args: *std.process.Args.Iterator) !?Options {
    const model_dir = args.next() orelse return usageError();
    if (std.mem.eql(u8, model_dir, "--help") or std.mem.eql(u8, model_dir, "-h")) {
        printUsage();
        return null;
    }

    var opts = Options{
        .model_dir = model_dir,
        .train_data = args.next() orelse return usageError(),
        .eval_data = args.next() orelse return usageError(),
        .out_dir = args.next() orelse return usageError(),
        .entity_types_csv = args.next() orelse return usageError(),
    };

    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--epochs")) {
            opts.epochs = args.next() orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--batch-size")) {
            opts.batch_size = args.next() orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--max-examples")) {
            opts.max_examples = args.next() orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--seq-len")) {
            opts.seq_len = args.next() orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--learning-rate") or std.mem.eql(u8, arg, "--lr")) {
            opts.learning_rate = args.next() orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--lora-rank")) {
            opts.lora_rank = args.next() orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--lora-alpha")) {
            opts.lora_alpha = args.next() orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--objective")) {
            opts.objective = args.next() orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--max-span-width")) {
            opts.max_span_width = args.next() orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--span-loss")) {
            opts.span_loss = args.next() orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--span-positive-weight")) {
            opts.span_positive_weight = args.next() orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--span-label-positive-weights")) {
            opts.span_label_positive_weights = args.next() orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--span-negative-weight")) {
            opts.span_negative_weight = args.next() orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--span-hard-negative-weight")) {
            opts.span_hard_negative_weight = args.next() orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--max-grad-norm")) {
            opts.max_grad_norm = args.next() orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--grad-accum")) {
            opts.grad_accum = args.next() orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--seed")) {
            opts.seed = args.next() orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--backend")) {
            opts.backend = args.next() orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--compiled-required")) {
            opts.compiled_required = true;
        } else if (std.mem.eql(u8, arg, "--production-metal-gate")) {
            applyProductionMetalGateDefaults(&opts);
        } else if (std.mem.eql(u8, arg, "--production-mlx-gate")) {
            applyProductionMlxGateDefaults(&opts);
        } else if (std.mem.eql(u8, arg, "--num-classes")) {
            opts.num_classes_override = args.next() orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--min-train-examples")) {
            opts.min_train_examples = try parseUsizeArg(args, arg);
        } else if (std.mem.eql(u8, arg, "--min-eval-examples")) {
            opts.min_eval_examples = try parseUsizeArg(args, arg);
        } else if (std.mem.eql(u8, arg, "--min-total-entities")) {
            opts.min_total_entities = try parseUsizeArg(args, arg);
        } else if (std.mem.eql(u8, arg, "--min-unique-labels")) {
            opts.min_unique_labels = try parseUsizeArg(args, arg);
        } else if (std.mem.eql(u8, arg, "--min-target-coverage-ratio")) {
            opts.min_target_coverage_ratio = try parseF64Arg(args, arg);
        } else if (std.mem.eql(u8, arg, "--min-positive-span-labels")) {
            opts.min_positive_span_labels = try parseUsizeArg(args, arg);
        } else if (std.mem.eql(u8, arg, "--min-positive-rate-per-label")) {
            opts.min_positive_rate_per_label = try parseF64Arg(args, arg);
        } else if (std.mem.eql(u8, arg, "--min-steps")) {
            opts.min_steps = try parseUsizeArg(args, arg);
        } else if (std.mem.eql(u8, arg, "--min-supervised-tokens")) {
            opts.min_supervised_tokens = try parseUsizeArg(args, arg);
        } else if (std.mem.eql(u8, arg, "--min-entity-tokens")) {
            opts.min_entity_tokens = try parseUsizeArg(args, arg);
        } else if (std.mem.eql(u8, arg, "--min-supervised-tokens-per-second")) {
            opts.min_supervised_tokens_per_second = try parseF64Arg(args, arg);
        } else if (std.mem.eql(u8, arg, "--max-avg-step-wall-ms")) {
            opts.max_avg_step_wall_ms = try parseF64Arg(args, arg);
        } else if (std.mem.eql(u8, arg, "--max-total-execute-ms")) {
            opts.max_total_execute_ms = try parseF64Arg(args, arg);
        } else if (std.mem.eql(u8, arg, "--max-peak-resident-bytes")) {
            opts.max_peak_resident_bytes = try parseUsizeArg(args, arg);
        } else if (std.mem.eql(u8, arg, "--max-device-trainable-transfer-count")) {
            opts.max_device_trainable_transfer_count = try parseU64Arg(args, arg);
        } else if (std.mem.eql(u8, arg, "--max-device-resident-transfer-count")) {
            opts.max_device_resident_transfer_count = try parseU64Arg(args, arg);
            if (opts.max_device_trainable_transfer_count == null) opts.max_device_trainable_transfer_count = opts.max_device_resident_transfer_count;
        } else if (std.mem.eql(u8, arg, "--min-device-trainable-bytes")) {
            opts.min_device_trainable_bytes = try parseUsizeArg(args, arg);
        } else if (std.mem.eql(u8, arg, "--allow-flat-loss")) {
            opts.require_loss_decrease = false;
        } else if (std.mem.eql(u8, arg, "--eval-text")) {
            opts.eval_text = args.next() orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--expect-text")) {
            opts.expect_text = args.next() orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--expect-label")) {
            opts.expect_label = args.next() orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--min-score")) {
            opts.min_score = args.next() orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--semantic-golden")) {
            if (opts.semantic_golden_count >= opts.semantic_goldens.len) return error.TooManySemanticGoldens;
            opts.semantic_goldens[opts.semantic_golden_count] = .{
                .text = args.next() orelse return usageError(),
                .expect_text = args.next() orelse return usageError(),
                .expect_label = args.next() orelse return usageError(),
                .min_score = args.next() orelse return usageError(),
            };
            opts.semantic_golden_count += 1;
        } else if (std.mem.eql(u8, arg, "--skip-semantic-eval")) {
            opts.skip_semantic_eval = true;
        } else if (std.mem.eql(u8, arg, "--semantic-min-prediction-score")) {
            opts.semantic_min_prediction_score = args.next() orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--semantic-label-thresholds")) {
            opts.semantic_label_thresholds = args.next() orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--semantic-label-score-biases")) {
            opts.semantic_label_score_biases = args.next() orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--semantic-nms-overlap")) {
            opts.semantic_nms_overlap = args.next() orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--semantic-max-predictions")) {
            opts.semantic_max_predictions = args.next() orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--semantic-top-k-per-label")) {
            opts.semantic_top_k_per_label = args.next() orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--semantic-best-span-per-label-start")) {
            opts.semantic_best_span_per_label_start = true;
        } else if (std.mem.eql(u8, arg, "--semantic-best-label-per-span-start")) {
            opts.semantic_best_label_per_span_start = true;
        } else if (std.mem.eql(u8, arg, "--semantic-require-entitylike-span")) {
            opts.semantic_require_entitylike_span = true;
        } else if (std.mem.eql(u8, arg, "--quality-eval")) {
            opts.quality_eval = true;
        } else if (std.mem.eql(u8, arg, "--quality-max-examples")) {
            opts.quality_max_examples = args.next() orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--quality-min-prediction-score")) {
            opts.quality_min_prediction_score = args.next() orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--quality-label-thresholds")) {
            opts.quality_label_thresholds = args.next() orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--quality-label-score-biases")) {
            opts.quality_label_score_biases = args.next() orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--quality-sweep-thresholds")) {
            opts.quality_sweep_thresholds = args.next() orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--quality-nms-overlap")) {
            opts.quality_nms_overlap = args.next() orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--quality-no-nms")) {
            opts.quality_disable_nms = true;
        } else if (std.mem.eql(u8, arg, "--quality-max-predictions-per-example")) {
            opts.quality_max_predictions_per_example = args.next() orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--quality-top-k-per-label")) {
            opts.quality_top_k_per_label = args.next() orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--quality-best-span-per-label-start")) {
            opts.quality_best_span_per_label_start = true;
        } else if (std.mem.eql(u8, arg, "--quality-best-label-per-span-start")) {
            opts.quality_best_label_per_span_start = true;
        } else if (std.mem.eql(u8, arg, "--quality-require-entitylike-span")) {
            opts.quality_require_entitylike_span = true;
        } else if (std.mem.eql(u8, arg, "--quality-diagnostic-limit")) {
            opts.quality_diagnostic_limit = args.next() orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--min-entity-precision")) {
            opts.min_entity_precision = args.next() orelse return usageError();
            opts.quality_eval = true;
        } else if (std.mem.eql(u8, arg, "--min-entity-recall")) {
            opts.min_entity_recall = args.next() orelse return usageError();
            opts.quality_eval = true;
        } else if (std.mem.eql(u8, arg, "--min-entity-f1")) {
            opts.min_entity_f1 = args.next() orelse return usageError();
            opts.quality_eval = true;
        } else if (std.mem.eql(u8, arg, "--materialized-dir")) {
            opts.materialized_dir = args.next() orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--dry-run")) {
            opts.dry_run = true;
        } else if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) {
            printUsage();
            return null;
        } else {
            return usageError();
        }
    }
    return opts;
}

fn applyProductionMetalGateDefaults(opts: *Options) void {
    opts.production_metal_gate = true;
    opts.epochs = "1";
    opts.batch_size = "1";
    opts.max_examples = "200";
    opts.seq_len = "32";
    opts.span_loss = "bce";
    opts.backend = "metal";
    opts.compiled_required = true;
    opts.min_train_examples = 200;
    opts.min_eval_examples = 200;
    opts.min_total_entities = 100;
    opts.min_unique_labels = 3;
    opts.min_positive_span_labels = 100;
    opts.min_steps = 200;
    opts.min_supervised_tokens = 1000;
    opts.min_entity_tokens = 100;
    opts.max_avg_step_wall_ms = 3000.0;
    opts.max_device_trainable_transfer_count = 0;
    opts.max_device_resident_transfer_count = 0;
    opts.min_device_trainable_bytes = 1;
    opts.require_loss_decrease = true;
    opts.skip_semantic_eval = false;
    opts.semantic_require_entitylike_span = true;
    opts.quality_eval = true;
    opts.quality_max_examples = "25";
    opts.quality_min_prediction_score = "0.03";
    opts.quality_sweep_thresholds = "0.03,0.05,0.07,0.10,0.15,0.20,0.25,0.30";
    opts.quality_nms_overlap = "0.0";
    opts.quality_max_predictions_per_example = "3";
    opts.quality_top_k_per_label = "1";
    opts.quality_best_span_per_label_start = true;
    opts.quality_require_entitylike_span = true;
    opts.min_entity_f1 = "0.15";
}

fn applyProductionMlxGateDefaults(opts: *Options) void {
    opts.production_mlx_gate = true;
    opts.epochs = "1";
    opts.batch_size = "1";
    opts.max_examples = "200";
    opts.seq_len = "32";
    opts.span_loss = "bce";
    opts.backend = "mlx";
    opts.compiled_required = true;
    opts.min_train_examples = 200;
    opts.min_eval_examples = 200;
    opts.min_total_entities = 100;
    opts.min_unique_labels = 3;
    opts.min_positive_span_labels = 100;
    opts.min_steps = 200;
    opts.min_supervised_tokens = 1000;
    opts.min_entity_tokens = 100;
    opts.max_avg_step_wall_ms = 10000.0;
    opts.max_device_trainable_transfer_count = 0;
    opts.max_device_resident_transfer_count = 0;
    opts.min_device_trainable_bytes = 1;
    opts.require_loss_decrease = true;
    opts.skip_semantic_eval = false;
    opts.semantic_require_entitylike_span = true;
    opts.quality_eval = true;
    opts.quality_max_examples = "25";
    opts.quality_min_prediction_score = "0.03";
    opts.quality_sweep_thresholds = "0.03,0.05,0.07,0.10,0.15,0.20,0.25,0.30";
    opts.quality_nms_overlap = "0.0";
    opts.quality_max_predictions_per_example = "3";
    opts.quality_top_k_per_label = "1";
    opts.quality_best_span_per_label_start = true;
    opts.quality_require_entitylike_span = true;
    opts.min_entity_f1 = "0.15";
}

fn parseUsizeArg(args: *std.process.Args.Iterator, name: []const u8) !usize {
    return std.fmt.parseUnsigned(usize, args.next() orelse {
        std.debug.print("error: missing value for {s}\n", .{name});
        return error.InvalidArguments;
    }, 10);
}

fn parseU64Arg(args: *std.process.Args.Iterator, name: []const u8) !u64 {
    return std.fmt.parseUnsigned(u64, args.next() orelse {
        std.debug.print("error: missing value for {s}\n", .{name});
        return error.InvalidArguments;
    }, 10);
}

fn parseF64Arg(args: *std.process.Args.Iterator, name: []const u8) !f64 {
    return std.fmt.parseFloat(f64, args.next() orelse {
        std.debug.print("error: missing value for {s}\n", .{name});
        return error.InvalidArguments;
    });
}

fn usageError() error{InvalidArguments} {
    printUsage();
    return error.InvalidArguments;
}

fn printUsage() void {
    std.debug.print(
        \\usage: gliner2-production-readiness <model_dir> <train_jsonl_or_dir> <eval_jsonl_or_dir> <out_dir> <entity_types_csv> [options]
        \\
        \\Runs the production-readiness gate:
        \\  dataset readiness -> train-gliner2-autodiff -> validate run artifacts -> inspect bundle -> semantic eval -> optional materialization.
        \\
        \\Required semantic options unless --skip-semantic-eval is set:
        \\  --eval-text TEXT
        \\  --expect-label LABEL
        \\  --min-score FLOAT
        \\  --semantic-golden TEXT EXPECT_TEXT EXPECT_LABEL MIN_SCORE
        \\      Repeatable stronger form. When present, these replace --eval-text.
        \\  --semantic-min-prediction-score FLOAT
        \\  --semantic-label-thresholds label=FLOAT[,label=FLOAT...]
        \\  --semantic-label-score-biases label=FLOAT[,label=FLOAT...]
        \\  --semantic-nms-overlap FLOAT
        \\  --semantic-max-predictions N
        \\  --semantic-top-k-per-label N
        \\  --semantic-best-span-per-label-start
        \\  --semantic-best-label-per-span-start
        \\  --semantic-require-entitylike-span
        \\  --quality-eval
        \\  --quality-max-examples N
        \\  --quality-min-prediction-score FLOAT
        \\  --quality-label-thresholds label=FLOAT[,label=FLOAT...]
        \\  --quality-label-score-biases label=FLOAT[,label=FLOAT...]
        \\  --quality-sweep-thresholds CSV
        \\  --quality-nms-overlap FLOAT
        \\  --quality-no-nms
        \\  --quality-max-predictions-per-example N
        \\  --quality-top-k-per-label N
        \\  --quality-best-span-per-label-start
        \\  --quality-best-label-per-span-start
        \\  --quality-require-entitylike-span
        \\  --quality-diagnostic-limit N
        \\  --min-entity-precision FLOAT
        \\  --min-entity-recall FLOAT
        \\  --min-entity-f1 FLOAT
        \\
        \\Common options:
        \\  --objective token|span-start      Training/eval objective (default: span-start)
        \\  --epochs N                       Training epochs (default: 5)
        \\  --max-examples N                 Training cap (default: 100)
        \\  --seq-len N                      Sequence length (default: 256)
        \\  --batch-size N                   Batch size (default: 1)
        \\  --learning-rate FLOAT            Learning rate (default: 1e-3)
        \\  --span-loss bce|mse              Span-start label loss (default: bce)
        \\  --span-positive-weight FLOAT     Positive span-label loss weight (default: 32)
        \\  --span-label-positive-weights CSV Per-label positive weights, e.g. person=32,organization=96
        \\  --span-negative-weight FLOAT     Negative span-label loss weight (default: 1)
        \\  --span-hard-negative-weight FLOAT Extra negative weight for spans overlapping gold entities (default: 1)
        \\  --backend auto|metal|mlx|native  Training backend (default: auto)
        \\  --compiled-required              Fail if requested compiled backend falls back
        \\  --production-metal-gate          Canonical 200-step resident Metal gate
        \\  --production-mlx-gate            Canonical 200-step strict MLX gate
        \\  --materialized-dir DIR           Also materialize merged model artifacts
        \\  --dry-run                        Print the gate shape without touching model/data files
        \\
        \\Production threshold options:
        \\  --min-train-examples N
        \\  --min-eval-examples N
        \\  --min-total-entities N
        \\  --min-unique-labels N
        \\  --min-target-coverage-ratio FLOAT
        \\  --min-positive-span-labels N
        \\  --min-steps N
        \\  --min-supervised-tokens N
        \\  --min-entity-tokens N
        \\  --min-supervised-tokens-per-second FLOAT
        \\  --max-avg-step-wall-ms FLOAT
        \\  --max-total-execute-ms FLOAT
        \\  --max-peak-resident-bytes N
        \\  --max-device-trainable-transfer-count N
        \\  --max-device-resident-transfer-count N
        \\  --min-device-trainable-bytes N
        \\
    , .{});
}

test "production MLX gate defaults require compiled resident optimizer" {
    var opts = Options{};
    applyProductionMlxGateDefaults(&opts);

    try std.testing.expect(opts.production_mlx_gate);
    try std.testing.expectEqualStrings("mlx", opts.backend);
    try std.testing.expect(opts.compiled_required);
    try std.testing.expectEqual(@as(usize, 200), opts.min_train_examples);
    try std.testing.expectEqual(@as(usize, 200), opts.min_steps);
    try std.testing.expectEqual(@as(?u64, 0), opts.max_device_resident_transfer_count);
    try std.testing.expectEqual(@as(?usize, 1), opts.min_device_trainable_bytes);
    try std.testing.expect(opts.semantic_require_entitylike_span);
    try std.testing.expect(opts.quality_eval);
    try std.testing.expectEqualStrings("25", opts.quality_max_examples.?);
    try std.testing.expectEqualStrings("0.15", opts.min_entity_f1.?);
    try std.testing.expect(opts.quality_require_entitylike_span);
}

test "production Metal gate defaults include shaped quality eval" {
    var opts = Options{};
    applyProductionMetalGateDefaults(&opts);

    try std.testing.expect(opts.production_metal_gate);
    try std.testing.expectEqualStrings("metal", opts.backend);
    try std.testing.expect(opts.compiled_required);
    try std.testing.expect(opts.quality_eval);
    try std.testing.expectEqualStrings("25", opts.quality_max_examples.?);
    try std.testing.expectEqualStrings("0.15", opts.min_entity_f1.?);
    try std.testing.expect(opts.semantic_require_entitylike_span);
    try std.testing.expect(opts.quality_require_entitylike_span);
}
