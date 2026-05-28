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
const compat = termite.io.compat;
const finetune = termite.finetune.layoutlmv3;
const document_data = termite.finetune.document_data;
const run_contract = termite.run.contract;
const artifact_writer = termite.run.artifact_writer;

const DatasetStats = struct {
    num_examples: usize,
    avg_tokens: f64,
    examples_with_cls: usize,
    examples_with_tok: usize,
    class_labels: usize,
    token_labels: usize,
};

const WorkflowSummary = struct {
    artifact_family_version: []const u8,
    task: []const u8,
    base_model_dir: []const u8,
    train_input: []const u8,
    val_input: []const u8,
    output_root: []const u8,
    bootstrap_dir: []const u8,
    trained_dir: []const u8,
    materialized_dir: []const u8,
    train_stats: DatasetStats,
    val_stats: DatasetStats,
    bootstrap: finetune.BootstrapSummary,
    initial_inspect: finetune.LoRABundleInspectionSummary,
    trained_inspect: finetune.LoRABundleInspectionSummary,
    materialize: finetune.MaterializeSummary,
    sequence: ?finetune.SequenceTrainEvalSummary = null,
    token: ?finetune.TokenTrainEvalSummary = null,
};

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, allocator);
    defer args.deinit();
    _ = args.next();

    const base_model_dir = args.next() orelse return usageError();
    const train_input = args.next() orelse return usageError();
    const val_input = args.next() orelse return usageError();
    const task = args.next() orelse return usageError();
    const output_root = args.next() orelse return usageError();
    const rank_arg = args.next() orelse "8";
    const alpha_arg = args.next() orelse "16";
    const max_train_examples_arg = args.next() orelse "32";
    const learning_rate_arg = args.next() orelse "0.001";
    const max_val_examples_arg = args.next() orelse "16";
    const epochs_arg = args.next() orelse "2";
    const layer_name = args.next();

    if (!std.mem.eql(u8, task, "sequence") and !std.mem.eql(u8, task, "token")) return usageError();

    const rank = try std.fmt.parseUnsigned(usize, rank_arg, 10);
    const alpha = try std.fmt.parseFloat(f32, alpha_arg);
    const max_train_examples = try std.fmt.parseUnsigned(usize, max_train_examples_arg, 10);
    const learning_rate = try std.fmt.parseFloat(f32, learning_rate_arg);
    const max_val_examples = try std.fmt.parseUnsigned(usize, max_val_examples_arg, 10);
    const epochs = try std.fmt.parseUnsigned(usize, epochs_arg, 10);

    const bootstrap_dir = try std.fs.path.join(allocator, &.{ output_root, "bootstrap" });
    defer allocator.free(bootstrap_dir);
    const trained_dir = try std.fs.path.join(allocator, &.{ output_root, "trained" });
    defer allocator.free(trained_dir);
    const materialized_dir = try std.fs.path.join(allocator, &.{ output_root, "materialized" });
    defer allocator.free(materialized_dir);
    const workflow_report_path = try std.fs.path.join(allocator, &.{ output_root, "smoke_workflow_report.json" });
    defer allocator.free(workflow_report_path);
    const training_config_path = try std.fs.path.join(allocator, &.{ output_root, "training_config.json" });
    defer allocator.free(training_config_path);
    const training_report_path = try std.fs.path.join(allocator, &.{ output_root, "training_report.json" });
    defer allocator.free(training_report_path);
    const run_status_path = try std.fs.path.join(allocator, &.{ output_root, "run_status.json" });
    defer allocator.free(run_status_path);
    try compat.cwd().createDirPath(compat.io(), output_root);

    try artifact_writer.writeJsonFile(allocator, training_config_path, .{
        .contract_version = run_contract.training_config_version,
        .artifact_family_version = finetune.artifact_family_version,
        .task = "layoutlmv3_lora_smoke_workflow",
        .run_plan = .{
            .workflow = "bounded_native_smoke",
            .head_task = task,
        },
        .inputs = .{
            .base_model_dir = base_model_dir,
            .train_input = train_input,
            .val_input = val_input,
        },
        .output_root = output_root,
        .bootstrap = .{
            .rank = rank,
            .alpha = alpha,
            .bootstrap_dir = bootstrap_dir,
        },
        .training = .{
            .max_train_examples = max_train_examples,
            .max_val_examples = max_val_examples,
            .learning_rate = learning_rate,
            .epochs = epochs,
            .layer_name = layer_name,
        },
    });
    try artifact_writer.writeJsonFile(allocator, run_status_path, .{
        .contract_version = run_contract.run_status_version,
        .status = "running",
        .task = "layoutlmv3_lora_smoke_workflow",
        .out_dir = output_root,
        .resume_from = @as(?[]const u8, null),
        .actions = @as(?[]const u8, null),
        .derived = .{
            .outcome_code = "running",
            .alerts = [_]u8{},
            .metric_summary = .{
                .head_task = task,
            },
        },
        .artifacts = .{
            .report = training_report_path,
            .best = trained_dir,
            .latest = trained_dir,
            .final = materialized_dir,
        },
    });
    errdefer artifact_writer.writeJsonFile(allocator, run_status_path, .{
        .contract_version = run_contract.run_status_version,
        .status = "failed",
        .task = "layoutlmv3_lora_smoke_workflow",
        .out_dir = output_root,
        .resume_from = @as(?[]const u8, null),
        .actions = @as(?[]const u8, null),
        .derived = .{
            .outcome_code = "failed",
            .alerts = [_]u8{},
            .metric_summary = .{
                .head_task = task,
            },
        },
        .artifacts = .{
            .report = training_report_path,
            .best = trained_dir,
            .latest = trained_dir,
            .final = materialized_dir,
        },
    }) catch {};

    var train_loaded = try document_data.loadExamples(allocator, train_input, "train");
    defer train_loaded.deinit();
    var val_loaded = try document_data.loadExamples(allocator, val_input, "val");
    defer val_loaded.deinit();

    const train_stats_raw = try document_data.computeStats(allocator, train_loaded.examples);
    const val_stats_raw = try document_data.computeStats(allocator, val_loaded.examples);

    var bootstrap = try finetune.bootstrapLoRABundle(allocator, base_model_dir, bootstrap_dir, .{
        .rank = rank,
        .alpha = alpha,
        .base_model_name_or_path = base_model_dir,
    });
    errdefer finetune.freeBootstrapSummary(allocator, &bootstrap);

    var initial_inspect = try finetune.inspectLoRABundle(allocator, base_model_dir, bootstrap_dir);
    errdefer finetune.freeLoRABundleInspectionSummary(allocator, &initial_inspect);

    var maybe_sequence: ?finetune.SequenceTrainEvalSummary = null;
    var maybe_token: ?finetune.TokenTrainEvalSummary = null;
    errdefer if (maybe_sequence) |*summary| finetune.freeSequenceTrainEvalSummary(allocator, summary);
    errdefer if (maybe_token) |*summary| finetune.freeTokenTrainEvalSummary(allocator, summary);

    if (std.mem.eql(u8, task, "sequence")) {
        const train_examples = try document_data.filterSequenceExamples(allocator, train_loaded.dataset_root, train_loaded.examples);
        defer document_data.freeSequenceExamples(allocator, train_examples);
        const val_examples = try document_data.filterSequenceExamples(allocator, val_loaded.dataset_root, val_loaded.examples);
        defer document_data.freeSequenceExamples(allocator, val_examples);
        const label_vocab = try buildCombinedSequenceLabelVocab(allocator, train_loaded.examples, val_loaded.examples);
        defer freeLabelVocab(allocator, label_vocab);
        try requireSequenceLabelVocab(label_vocab);

        var bundle = try finetune.loadLoRABundle(allocator, base_model_dir, bootstrap_dir);
        defer bundle.deinit();
        maybe_sequence = try finetune.trainEvalSequenceLoRABundle(allocator, &bundle, train_examples, val_examples, label_vocab, trained_dir, .{
            .max_train_examples = max_train_examples,
            .max_val_examples = max_val_examples,
            .epochs = epochs,
            .learning_rate = learning_rate,
            .layer_name = layer_name,
        });
    } else {
        const train_examples = try document_data.filterTokenExamples(allocator, train_loaded.examples);
        defer allocator.free(train_examples);
        const val_examples = try document_data.filterTokenExamples(allocator, val_loaded.examples);
        defer allocator.free(val_examples);
        const label_vocab = try buildCombinedTokenLabelVocab(allocator, train_loaded.examples, val_loaded.examples);
        defer freeLabelVocab(allocator, label_vocab);
        try requireTokenLabelVocab(label_vocab);

        var bundle = try finetune.loadLoRABundle(allocator, base_model_dir, bootstrap_dir);
        defer bundle.deinit();
        maybe_token = try finetune.trainEvalTokenLoRABundle(allocator, &bundle, train_examples, val_examples, label_vocab, trained_dir, .{
            .max_train_examples = max_train_examples,
            .max_val_examples = max_val_examples,
            .epochs = epochs,
            .learning_rate = learning_rate,
            .layer_name = layer_name,
        });
    }

    var trained_inspect = try finetune.inspectLoRABundle(allocator, base_model_dir, trained_dir);
    errdefer finetune.freeLoRABundleInspectionSummary(allocator, &trained_inspect);

    var materialize = try finetune.materializeMergedModel(allocator, base_model_dir, trained_dir, task, materialized_dir);
    errdefer finetune.freeMaterializeSummary(allocator, &materialize);

    var summary = WorkflowSummary{
        .artifact_family_version = try allocator.dupe(u8, finetune.artifact_family_version),
        .task = try allocator.dupe(u8, task),
        .base_model_dir = try allocator.dupe(u8, base_model_dir),
        .train_input = try allocator.dupe(u8, train_input),
        .val_input = try allocator.dupe(u8, val_input),
        .output_root = try allocator.dupe(u8, output_root),
        .bootstrap_dir = try allocator.dupe(u8, bootstrap_dir),
        .trained_dir = try allocator.dupe(u8, trained_dir),
        .materialized_dir = try allocator.dupe(u8, materialized_dir),
        .train_stats = .{
            .num_examples = train_stats_raw.num_examples,
            .avg_tokens = train_stats_raw.avg_tokens,
            .examples_with_cls = train_stats_raw.examples_with_cls,
            .examples_with_tok = train_stats_raw.examples_with_tok,
            .class_labels = train_stats_raw.class_labels,
            .token_labels = train_stats_raw.token_labels,
        },
        .val_stats = .{
            .num_examples = val_stats_raw.num_examples,
            .avg_tokens = val_stats_raw.avg_tokens,
            .examples_with_cls = val_stats_raw.examples_with_cls,
            .examples_with_tok = val_stats_raw.examples_with_tok,
            .class_labels = val_stats_raw.class_labels,
            .token_labels = val_stats_raw.token_labels,
        },
        .bootstrap = bootstrap,
        .initial_inspect = initial_inspect,
        .trained_inspect = trained_inspect,
        .materialize = materialize,
        .sequence = maybe_sequence,
        .token = maybe_token,
    };
    defer freeWorkflowSummary(allocator, &summary);

    try artifact_writer.writeJsonFile(allocator, workflow_report_path, summary);
    try artifact_writer.writeJsonFile(allocator, training_report_path, .{
        .contract_version = run_contract.training_report_version,
        .artifact_family_version = finetune.artifact_family_version,
        .task = "layoutlmv3_lora_smoke_workflow",
        .summary = summary,
    });
    try artifact_writer.writeJsonFile(allocator, run_status_path, .{
        .contract_version = run_contract.run_status_version,
        .status = "completed",
        .task = "layoutlmv3_lora_smoke_workflow",
        .out_dir = output_root,
        .resume_from = @as(?[]const u8, null),
        .actions = @as(?[]const u8, null),
        .derived = .{
            .outcome_code = "completed",
            .alerts = [_]u8{},
            .metric_summary = .{
                .head_task = task,
                .train_examples = summary.train_stats.num_examples,
                .val_examples = summary.val_stats.num_examples,
            },
        },
        .artifacts = .{
            .report = training_report_path,
            .best = trained_dir,
            .latest = trained_dir,
            .final = materialized_dir,
        },
    });

    const stdout = std.Io.File.stdout();
    var buf: [8192]u8 = undefined;
    var writer = stdout.writer(init.io, &buf);
    try std.json.Stringify.value(summary, .{ .whitespace = .indent_2 }, &writer.interface);
    try writer.interface.writeByte('\n');
    try writer.interface.flush();
}

fn freeWorkflowSummary(allocator: std.mem.Allocator, summary: *WorkflowSummary) void {
    allocator.free(summary.artifact_family_version);
    allocator.free(summary.task);
    allocator.free(summary.base_model_dir);
    allocator.free(summary.train_input);
    allocator.free(summary.val_input);
    allocator.free(summary.output_root);
    allocator.free(summary.bootstrap_dir);
    allocator.free(summary.trained_dir);
    allocator.free(summary.materialized_dir);
    finetune.freeBootstrapSummary(allocator, &summary.bootstrap);
    finetune.freeLoRABundleInspectionSummary(allocator, &summary.initial_inspect);
    finetune.freeLoRABundleInspectionSummary(allocator, &summary.trained_inspect);
    finetune.freeMaterializeSummary(allocator, &summary.materialize);
    if (summary.sequence) |*sequence| finetune.freeSequenceTrainEvalSummary(allocator, sequence);
    if (summary.token) |*token| finetune.freeTokenTrainEvalSummary(allocator, token);
    summary.* = undefined;
}

fn buildCombinedSequenceLabelVocab(
    allocator: std.mem.Allocator,
    train_examples: []const document_data.PageExample,
    val_examples: []const document_data.PageExample,
) ![][]const u8 {
    var labels = std.StringHashMapUnmanaged(void){};
    defer labels.deinit(allocator);
    for (train_examples) |ex| {
        if (ex.label) |label| {
            if (std.mem.trim(u8, label, " \t\r\n").len > 0) try labels.put(allocator, label, {});
        }
    }
    for (val_examples) |ex| {
        if (ex.label) |label| {
            if (std.mem.trim(u8, label, " \t\r\n").len > 0) try labels.put(allocator, label, {});
        }
    }
    return collectSortedKeys(allocator, &labels);
}

fn buildCombinedTokenLabelVocab(
    allocator: std.mem.Allocator,
    train_examples: []const document_data.PageExample,
    val_examples: []const document_data.PageExample,
) ![][]const u8 {
    var labels = std.StringHashMapUnmanaged(void){};
    defer labels.deinit(allocator);
    for (train_examples) |ex| {
        if (ex.token_labels) |token_labels| {
            for (token_labels) |label| {
                if (std.mem.trim(u8, label, " \t\r\n").len > 0) try labels.put(allocator, label, {});
            }
        }
    }
    for (val_examples) |ex| {
        if (ex.token_labels) |token_labels| {
            for (token_labels) |label| {
                if (std.mem.trim(u8, label, " \t\r\n").len > 0) try labels.put(allocator, label, {});
            }
        }
    }
    return collectSortedKeys(allocator, &labels);
}

fn collectSortedKeys(
    allocator: std.mem.Allocator,
    labels: *std.StringHashMapUnmanaged(void),
) ![][]const u8 {
    var keys = try allocator.alloc([]const u8, labels.count());
    errdefer allocator.free(keys);
    var it = labels.iterator();
    var idx: usize = 0;
    while (it.next()) |entry| : (idx += 1) {
        keys[idx] = try allocator.dupe(u8, entry.key_ptr.*);
    }
    std.mem.sort([]const u8, keys, {}, struct {
        fn lessThan(_: void, lhs: []const u8, rhs: []const u8) bool {
            return std.mem.lessThan(u8, lhs, rhs);
        }
    }.lessThan);
    return keys;
}

fn freeLabelVocab(allocator: std.mem.Allocator, vocab: []const []const u8) void {
    for (vocab) |label| allocator.free(label);
    allocator.free(vocab);
}

fn requireSequenceLabelVocab(label_vocab: []const []const u8) !void {
    if (label_vocab.len == 0) {
        std.debug.print("error: no non-empty sequence labels were found in the provided train/val inputs\n", .{});
        return error.EmptyLabelVocabulary;
    }
    if (label_vocab.len < 2) {
        std.debug.print("error: sequence training requires at least 2 distinct labels, found {d}\n", .{label_vocab.len});
        return error.InsufficientLabelVocabulary;
    }
}

fn requireTokenLabelVocab(label_vocab: []const []const u8) !void {
    if (label_vocab.len == 0) {
        std.debug.print("error: no non-empty token labels were found in the provided train/val inputs\n", .{});
        return error.EmptyLabelVocabulary;
    }
    if (label_vocab.len < 2) {
        std.debug.print("error: token training requires at least 2 distinct labels, found {d}\n", .{label_vocab.len});
        return error.InsufficientLabelVocabulary;
    }
}

fn usageError() error{InvalidArguments} {
    std.debug.print(
        \\usage: run-layoutlmv3-lora-smoke-workflow <base_model_dir> <train_jsonl_or_dir> <val_jsonl_or_dir> <sequence|token> <output_root> [rank] [alpha] [max_train_examples] [learning_rate] [max_val_examples] [epochs] [layer_name|@layoutlmv3_token_top1|@layoutlmv3_token_top3|@layoutlmv3_sequence_top3]
        \\example: run-layoutlmv3-lora-smoke-workflow /tmp/layoutlmv3_base /tmp/train.jsonl /tmp/val.jsonl sequence /tmp/layoutlmv3_smoke 8 16 32 0.001 16 2 @layoutlmv3_sequence_top3
        \\
    , .{});
    return error.InvalidArguments;
}
